"""
Paper auto trader:
1. Reads the latest 2-3 digest contexts saved from reports.
2. Builds a consensus verdict plus per-asset trade plans.
3. Confirms crypto trades with /signals market bias.
4. Opens and closes simulated trades in the backtest ledger.
"""

import asyncio
import json
import logging
import os
from datetime import datetime, timedelta

from config import (
    AUTOTRADE_CONTEXT_MAX_AGE_HOURS,
    AUTOTRADE_ENTRY_TOLERANCE_PCT,
    AUTOTRADE_FOLLOW_SIGNALS_WHEN_NEUTRAL,
    AUTOTRADE_INTERVAL_SEC,
    AUTOTRADE_NEUTRAL_MIN_BIAS_SCORE,
    AUTOTRADE_NEUTRAL_SL_PCT,
    AUTOTRADE_NEUTRAL_TP_PCT,
    AUTOTRADE_OPEN_SCORE_THRESHOLD,
    AUTOTRADE_RECENT_CONTEXT_LIMIT,
    AUTOTRADE_REVERSAL_SCORE_THRESHOLD,
    AUTOTRADE_SIGNAL_BIAS_CACHE_SEC,
    DATA_SOURCE_BINANCE_SIGNALS,
    FEATURE_AUTOTRADE,
    LOG_AUTOTRADE_SKIPS,
)
from database import (
    add_backtest_signal,
    append_trade_decision_log,
    close_backtest_signal,
    get_backtest_config,
    get_backtest_signals,
    get_backtest_stats,
    get_recent_daily_contexts,
    get_recent_trade_decisions,
    update_backtest_capital,
)
from session_manager import session_manager, SESSION_START_CAPITAL

logger = logging.getLogger(__name__)

INTERVAL_SECONDS = AUTOTRADE_INTERVAL_SEC
RECENT_CONTEXT_LIMIT = AUTOTRADE_RECENT_CONTEXT_LIMIT
CONTEXT_MAX_AGE_HOURS = AUTOTRADE_CONTEXT_MAX_AGE_HOURS
ENTRY_TOLERANCE_PCT = AUTOTRADE_ENTRY_TOLERANCE_PCT
OPEN_SCORE_THRESHOLD = AUTOTRADE_OPEN_SCORE_THRESHOLD
SIGNAL_FOLLOW_SCORE_THRESHOLD = 12.0  # Lower threshold for signal-follow mode (no digest)
REVERSAL_SCORE_THRESHOLD = AUTOTRADE_REVERSAL_SCORE_THRESHOLD
CRYPTO_SIGNAL_SYMBOLS = {"BTC", "ETH", "SOL", "BNB"}

_trade_lock = asyncio.Lock()

_signal_cache: dict = {}
_signal_cache_time: datetime | None = None
_signal_cache_meta: tuple[str, bool] | None = None


def _direction_to_int(direction: str) -> int:
    direction = (direction or "").upper()
    if direction in {"BUY", "LONG", "BULLISH"}:
        return 1
    if direction in {"SELL", "SHORT", "BEARISH"}:
        return -1
    return 0


def _int_to_trade_direction(score: int) -> str:
    if score > 0:
        return "BUY"
    if score < 0:
        return "SELL"
    return "NEUTRAL"


def _consensus_to_signal_verdict(verdict: str) -> dict | None:
    if verdict == "BUY":
        return {"verdict": "BULLISH"}
    if verdict == "SELL":
        return {"verdict": "BEARISH"}
    return None


def _parse_context_dt(raw: str) -> datetime | None:
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return None


def is_daily_context_fresh(context: dict | None) -> bool:
    """Whether the latest saved digest context is still recent enough for trading."""
    if not context:
        return False
    created = _parse_context_dt(context.get("created_at", ""))
    if not created:
        return False
    return (datetime.now() - created) < timedelta(hours=CONTEXT_MAX_AGE_HOURS)


def _infer_plan_direction(context: dict, symbol: str) -> str:
    entries = context.get("entries", {}) or {}
    targets = context.get("targets", {}) or {}
    stops = context.get("stop_losses", {}) or {}

    entry = float(entries.get(symbol) or 0)
    target = float(targets.get(symbol) or 0)
    stop = float(stops.get(symbol) or 0)

    if entry and target and stop:
        if stop < entry < target:
            return "BUY"
        if target < entry < stop:
            return "SELL"

    verdict = (context.get("verdict") or "").upper()
    if verdict in {"BUY", "SELL"}:
        return verdict
    return "NEUTRAL"


def build_digest_consensus(contexts: list[dict]) -> dict:
    """Aggregate the latest digest contexts into a tradeable consensus."""
    contexts = contexts[:RECENT_CONTEXT_LIMIT]
    weights = [3, 2, 1]
    verdict_score = 0
    raw_candidates: dict[tuple[str, str], dict] = {}
    context_rows = []

    for idx, context in enumerate(contexts):
        weight = weights[idx] if idx < len(weights) else 1
        verdict = (context.get("verdict") or "NEUTRAL").upper()
        verdict_score += _direction_to_int(verdict) * weight
        context_rows.append({
            "created_at": context.get("created_at", ""),
            "verdict": verdict,
            "symbols": sorted(set(context.get("symbols", []) or [])),
        })

        symbols = sorted(set(context.get("symbols", []) or []))
        symbols.extend(list((context.get("entries", {}) or {}).keys()))
        symbols.extend(list((context.get("targets", {}) or {}).keys()))
        symbols.extend(list((context.get("stop_losses", {}) or {}).keys()))
        symbols = sorted(set(symbols))

        for symbol in symbols:
            direction = _infer_plan_direction(context, symbol)
            if direction not in {"BUY", "SELL"}:
                continue

            entry = float((context.get("entries", {}) or {}).get(symbol) or 0)
            target = float((context.get("targets", {}) or {}).get(symbol) or 0)
            stop = float((context.get("stop_losses", {}) or {}).get(symbol) or 0)
            timeframe = (context.get("timeframes", {}) or {}).get(symbol) or "1w"

            key = (symbol, direction)
            bucket = raw_candidates.setdefault(key, {
                "symbol": symbol,
                "direction": direction,
                "support": 0,
                "weighted_support": 0,
                "entry_values": [],
                "target_values": [],
                "stop_values": [],
                "timeframes": [],
                "context_dates": [],
                "latest_created_at": context.get("created_at", ""),
                "latest_news_summary": context.get("news_summary", ""),
            })

            bucket["support"] += 1
            bucket["weighted_support"] += weight
            bucket["context_dates"].append(context.get("created_at", ""))
            if entry > 0:
                bucket["entry_values"].append(entry)
            if target > 0:
                bucket["target_values"].append(target)
            if stop > 0:
                bucket["stop_values"].append(stop)
            if timeframe:
                bucket["timeframes"].append(timeframe)

    consensus_verdict = "NEUTRAL"
    if verdict_score >= 2:
        consensus_verdict = "BUY"
    elif verdict_score <= -2:
        consensus_verdict = "SELL"

    required_support = 2 if len(contexts) >= 2 else 1
    candidates = []

    for plan in raw_candidates.values():
        if plan["support"] < required_support:
            continue

        digest_score = plan["weighted_support"] * 4.0
        if consensus_verdict in {"BUY", "SELL"}:
            digest_score += 4.0 if plan["direction"] == consensus_verdict else -6.0

        candidate = {
            "symbol": plan["symbol"],
            "direction": plan["direction"],
            "support": plan["support"],
            "weighted_support": plan["weighted_support"],
            "digest_score": round(digest_score, 2),
            "entry": round(sum(plan["entry_values"]) / len(plan["entry_values"]), 4) if plan["entry_values"] else 0.0,
            "target": round(sum(plan["target_values"]) / len(plan["target_values"]), 4) if plan["target_values"] else 0.0,
            "stop": round(sum(plan["stop_values"]) / len(plan["stop_values"]), 4) if plan["stop_values"] else 0.0,
            "timeframe": plan["timeframes"][0] if plan["timeframes"] else "1w",
            "context_dates": plan["context_dates"][:],
            "latest_created_at": plan["latest_created_at"],
            "news_summary": plan["latest_news_summary"],
        }
        candidates.append(candidate)

    candidates.sort(key=lambda item: (item["digest_score"], item["weighted_support"]), reverse=True)

    return {
        "consensus_verdict": consensus_verdict,
        "verdict_score": verdict_score,
        "contexts": context_rows,
        "candidates": candidates,
    }


def _markets_bundle_audit(bundle: dict) -> dict:
    v = bundle.get("verdict") or {}
    sigs = bundle.get("signals") or []
    return {
        "github_digest_verdict": v.get("verdict"),
        "signals": [
            {
                "symbol": s.get("symbol"),
                "direction": s.get("direction"),
                "confidence": s.get("confidence"),
            }
            for s in sigs[:10]
        ],
    }


def _bias_raw_from_bundle(markets_bundle: dict, crypto_symbols: list[str]) -> dict:
    full = markets_bundle.get("binance_data") or {}
    out = {}
    for sym in crypto_symbols:
        key = f"{sym}USDT"
        if key in full:
            out[key] = full[key]
    return out


async def _fetch_crypto_signal_bias(
    symbols: list[str],
    consensus_verdict: str,
    *,
    neutral_follow: bool = False,
    markets_bundle: dict | None = None,
) -> dict:
    global _signal_cache, _signal_cache_time, _signal_cache_meta

    crypto_symbols = [symbol for symbol in symbols if symbol in CRYPTO_SIGNAL_SYMBOLS]
    if not crypto_symbols:
        return {}

    if not DATA_SOURCE_BINANCE_SIGNALS:
        return {}

    now = datetime.now()
    meta_key = (consensus_verdict or "", neutral_follow)

    if markets_bundle is None:
        if (
            _signal_cache_time
            and (now - _signal_cache_time).total_seconds() < AUTOTRADE_SIGNAL_BIAS_CACHE_SEC
            and _signal_cache_meta == meta_key
        ):
            return {symbol: _signal_cache.get(symbol, {}) for symbol in crypto_symbols}

    try:
        from signals import build_signal_bias_map, fetch_binance_signals, fetch_verdict

        if markets_bundle is not None:
            raw = _bias_raw_from_bundle(markets_bundle, crypto_symbols)
            if not raw:
                raw = await fetch_binance_signals([f"{symbol}USDT" for symbol in crypto_symbols])
        else:
            raw = await fetch_binance_signals([f"{symbol}USDT" for symbol in crypto_symbols])

        sig_verdict = None
        if consensus_verdict in ("BUY", "SELL"):
            sig_verdict = _consensus_to_signal_verdict(consensus_verdict)
        elif neutral_follow:
            vr = None
            if markets_bundle is not None:
                vr = markets_bundle.get("verdict")
            if vr is None or not vr.get("verdict"):
                try:
                    repo = os.getenv("GITHUB_REPO", "borzenkovandrej07-alt/DIALECTIC_EDg")
                    vr = await fetch_verdict(repo)
                except Exception as e:
                    logger.debug("fetch_verdict for neutral_follow: %s", e)
                    vr = None
            if vr and vr.get("verdict") in ("BULLISH", "BEARISH"):
                sig_verdict = vr

        bias = build_signal_bias_map(raw, sig_verdict)
        if markets_bundle is None:
            _signal_cache = bias
            _signal_cache_time = now
            _signal_cache_meta = meta_key
        return {symbol: bias.get(symbol, {}) for symbol in crypto_symbols}
    except Exception as e:
        logger.warning(f"Signal bias fetch error: {e}")
        return {}


def _signal_follow_active(
    consensus_verdict: str,
    candidates: list,
) -> bool:
    return (
        AUTOTRADE_FOLLOW_SIGNALS_WHEN_NEUTRAL
        and DATA_SOURCE_BINANCE_SIGNALS
        and (consensus_verdict == "NEUTRAL" or not candidates)
    )


def _append_signal_follow_candidates(
    consensus: dict,
    prices: dict,
    signal_bias: dict,
    *,
    open_positions: list[dict] | None = None,
) -> dict:
    if not _signal_follow_active(
        consensus.get("consensus_verdict", "NEUTRAL"),
        consensus.get("candidates") or [],
    ):
        return consensus

    # Build set of assets we currently hold (open BUY positions)
    held_symbols = set()
    for pos in (open_positions or []):
        if (pos.get("direction") or "").upper() == "BUY":
            held_symbols.add(pos["symbol"])

    existing = {c["symbol"] for c in consensus.get("candidates", [])}
    add = []
    for symbol in sorted(CRYPTO_SIGNAL_SYMBOLS):
        if symbol in existing:
            continue
        b = signal_bias.get(symbol) or {}
        direction = (b.get("direction") or "NEUTRAL").upper()
        score = float(b.get("score") or 0.0)
        if direction not in ("LONG", "SHORT") or abs(score) < AUTOTRADE_NEUTRAL_MIN_BIAS_SCORE:
            continue
        price = float(prices.get(symbol) or 0)
        if price <= 0:
            continue

        if direction == "SHORT":
            # Price falling — BUY the dip
            trade_dir = "BUY"
            tp = AUTOTRADE_NEUTRAL_TP_PCT
            sl = AUTOTRADE_NEUTRAL_SL_PCT
            entry = price
            target = price * (1 + tp)
            stop = price * (1 - sl)
        else:
            # Price rising — SELL if we hold it
            if symbol not in held_symbols:
                logger.debug(f"LONG signal for {symbol} but not held — skipping")
                continue
            trade_dir = "SELL"
            tp = AUTOTRADE_NEUTRAL_TP_PCT
            sl = AUTOTRADE_NEUTRAL_SL_PCT
            entry = price
            target = price * (1 - tp)
            stop = price * (1 + sl)

        digest_score = 12.0 + min(abs(score), 35.0) * 0.35
        add.append({
            "symbol": symbol,
            "direction": trade_dir,
            "support": 0,
            "weighted_support": 0,
            "digest_score": round(digest_score, 2),
            "entry": round(entry, 6),
            "target": round(target, 6),
            "stop": round(stop, 6),
            "timeframe": "signal_follow",
            "context_dates": [],
            "latest_created_at": "",
            "news_summary": "",
            "signal_follow_only": True,
        })
    out = dict(consensus)
    out["candidates"] = list(consensus.get("candidates", [])) + add
    if add:
        out["signal_follow_augmented"] = len(add)
    return out


async def _export_backtest_snapshot():
    try:
        from github_export import _github_get, _github_put, BACKTEST_FILE
        from datetime import datetime

        signals = await get_backtest_signals()
        stats = await get_backtest_stats()
        config = await get_backtest_config()

        # Use session manager to format BACKTEST.md
        content = session_manager.format_backtest_md(signals, stats, config)

        _, sha = await _github_get(BACKTEST_FILE)
        await _github_put(
            BACKTEST_FILE, content, sha,
            f"📊 Update backtest {datetime.now().strftime('%Y-%m-%d %H:%M')} [skip ci]"
        )
        logger.info("✅ BACKTEST.md updated on GitHub")
    except Exception as e:
        logger.warning(f"Backtest export error: {e}")


async def fetch_current_prices(symbols: list[str]) -> dict:
    """Fetch current prices for crypto and stocks used in recent trade plans."""
    prices = {}
    symbols = sorted(set(symbols))
    if not symbols:
        return prices

    signal_bias = await _fetch_crypto_signal_bias(symbols, "NEUTRAL")
    for symbol, data in signal_bias.items():
        last_price = float(data.get("last_price") or 0.0)
        if last_price > 0:
            prices[symbol] = last_price

    missing = [symbol for symbol in symbols if symbol not in prices]
    if not missing:
        return prices

    try:
        from tracker import get_current_price

        results = await asyncio.gather(*(get_current_price(symbol) for symbol in missing), return_exceptions=True)
        for symbol, result in zip(missing, results):
            if isinstance(result, Exception) or result in (None, 0):
                continue
            try:
                prices[symbol] = float(result)
            except Exception:
                continue
    except Exception as e:
        logger.warning(f"Fallback price fetch error: {e}")

    return prices


def _score_candidate(candidate: dict, current_price: float, signal_bias: dict) -> dict:
    direction = candidate["direction"]
    entry = float(candidate.get("entry") or 0)
    stop = float(candidate.get("stop") or 0)
    target = float(candidate.get("target") or 0)
    signal = signal_bias.get(candidate["symbol"], {})

    proximity_score = 0.0
    blocked_reason = ""

    if entry > 0:
        delta = (current_price - entry) / entry
        if direction == "BUY":
            if stop and current_price <= stop:
                blocked_reason = "price_below_stop"
            elif target and current_price >= target:
                blocked_reason = "price_at_target"
            elif current_price <= entry * (1 + ENTRY_TOLERANCE_PCT):
                proximity_score = max(0.0, 6.0 - abs(delta) * 150)
            else:
                proximity_score = -6.0
        else:
            if stop and current_price >= stop:
                blocked_reason = "price_above_stop"
            elif target and current_price <= target:
                blocked_reason = "price_at_target"
            elif current_price >= entry * (1 - ENTRY_TOLERANCE_PCT):
                proximity_score = max(0.0, 6.0 - abs(delta) * 150)
            else:
                proximity_score = -6.0

    signal_score = 0.0
    signal_direction = signal.get("direction", "NEUTRAL")
    if candidate["symbol"] in CRYPTO_SIGNAL_SYMBOLS:
        raw_signal_score = float(signal.get("score") or 0.0)
        signal_score = raw_signal_score * 0.35
        if signal_direction == "NEUTRAL":
            signal_score -= 2.0
        elif (direction == "BUY" and signal_direction == "SHORT") or (direction == "SELL" and signal_direction == "LONG"):
            signal_score -= 5.0
    else:
        signal_score = -2.0

    total_score = float(candidate.get("digest_score") or 0.0) + proximity_score + signal_score
    # Use lower threshold for signal-follow mode
    threshold = SIGNAL_FOLLOW_SCORE_THRESHOLD if candidate.get("signal_follow_only") else OPEN_SCORE_THRESHOLD
    ready = not blocked_reason and total_score >= threshold

    scored = dict(candidate)
    scored.update({
        "current_price": current_price,
        "signal_direction": signal_direction,
        "signal_strength": float(signal.get("strength") or 0.0),
        "signal_score_component": round(signal_score, 2),
        "proximity_score": round(proximity_score, 2),
        "total_score": round(total_score, 2),
        "ready": ready,
        "blocked_reason": blocked_reason,
        "signal_reasons": signal.get("reasons", []),
    })
    return scored


def rank_trade_candidates(consensus: dict, prices: dict, signal_bias: dict) -> list[dict]:
    ranked = []
    for candidate in consensus.get("candidates", []):
        price = float(prices.get(candidate["symbol"]) or 0.0)
        if price <= 0:
            continue
        ranked.append(_score_candidate(candidate, price, signal_bias))

    ranked.sort(key=lambda item: item["total_score"], reverse=True)
    return ranked


def _parse_trade_meta(position: dict) -> dict:
    raw = position.get("trade_log") or ""
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception:
        return {}


async def _close_position_if_needed(position: dict, prices: dict, signal_bias: dict, consensus: dict) -> dict | None:
    symbol = position["symbol"]
    current_price = float(prices.get(symbol) or 0.0)
    if current_price <= 0:
        return None

    meta = _parse_trade_meta(position)
    direction = (position.get("direction") or "").upper()
    target = float(meta.get("target") or 0.0)
    stop = float(meta.get("stop") or 0.0)
    reason = ""

    if direction == "BUY":
        if target and current_price >= target:
            reason = "Target hit"
        elif stop and current_price <= stop:
            reason = "Stop loss hit"
    elif direction == "SELL":
        if target and current_price <= target:
            reason = "Target hit"
        elif stop and current_price >= stop:
            reason = "Stop loss hit"

    if not reason:
        signal = signal_bias.get(symbol, {})
        signal_score = float(signal.get("score") or 0.0)
        signal_direction = signal.get("direction", "NEUTRAL")
        if direction == "BUY" and signal_direction == "SHORT" and abs(signal_score) >= REVERSAL_SCORE_THRESHOLD:
            reason = "Signal reversal"
        elif direction == "SELL" and signal_direction == "LONG" and abs(signal_score) >= REVERSAL_SCORE_THRESHOLD:
            reason = "Signal reversal"

    if not reason:
        consensus_verdict = consensus.get("consensus_verdict", "NEUTRAL")
        if direction == "BUY" and consensus_verdict == "SELL":
            reason = "Digest consensus flipped bearish"
        elif direction == "SELL" and consensus_verdict == "BUY":
            reason = "Digest consensus flipped bullish"

    if not reason:
        return None

    result = await close_backtest_signal(position["id"], current_price, reason=reason)
    if not result:
        return None

    # Record in session manager
    session_manager.record_trade({
        "symbol": symbol,
        "direction": direction,
        "entry_price": float(position.get("entry_price") or 0.0),
        "exit_price": current_price,
        "pnl": float(result.get("pnl") or 0.0),
        "pnl_pct": float(result.get("pnl_pct") or 0.0),
        "reason": reason,
    })
    session_manager.update_capital(float(result.get("new_capital") or 0.0))

    return {
        "event": "closed",
        "symbol": symbol,
        "direction": direction,
        "entry_price": float(position.get("entry_price") or 0.0),
        "exit_price": current_price,
        "reason": reason,
        "pnl": float(result.get("pnl") or 0.0),
        "pnl_pct": float(result.get("pnl_pct") or 0.0),
        "capital": float(result.get("new_capital") or 0.0),
    }


async def _notify_admins(bot, admin_ids: list[int], event: dict):
    if not bot or not admin_ids:
        return

    if event["event"] == "opened":
        emoji = "🟢" if event["direction"] == "BUY" else "🔴"
        msg = (
            f"🎯 *AUTO TRADE OPEN*\n"
            f"{emoji} *{event['symbol']}* {event['direction']}\n"
            f"Вход: `${event['entry_price']:,.2f}`\n"
            f"План: `{event['support']} digest(s)` | Score `{event['score']}`\n"
            f"Сигнал: `{event['signal_direction']}`\n"
            f"Тейк: `${event['target']:,.2f}` | Стоп: `${event['stop']:,.2f}`\n"
            f"Баланс: `${event['capital']:,.2f}`"
        )
    else:
        emoji = "🟢" if event["pnl"] >= 0 else "🔴"
        msg = (
            f"🎯 *AUTO TRADE CLOSE*\n"
            f"{emoji} *{event['symbol']}* {event['direction']}\n"
            f"Выход: `${event['exit_price']:,.2f}`\n"
            f"PnL: `{event['pnl']:+,.2f}` ({event['pnl_pct']:+.2f}%)\n"
            f"Причина: {event['reason']}\n"
            f"Баланс: `${event['capital']:,.2f}`"
        )

    for admin_id in admin_ids:
        try:
            await bot.send_message(admin_id, msg, parse_mode="Markdown")
        except Exception:
            continue


def _scoring_legend() -> dict:
    return {
        "digest_context_weights_newest_first": [3, 2, 1],
        "digest_score": "weighted_support * 4 + (aligned consensus +4 | opposite -6)",
        "signal_crypto": "build_signal_bias_map.score * 0.35; NEUTRAL -2; direction clash -5",
        "signal_non_crypto": "-2 (plan from digest; weak external confirmation)",
        "proximity": "near planned entry within ENTRY_TOLERANCE_PCT else penalty",
        "open_total_score_min": OPEN_SCORE_THRESHOLD,
        "reversal_signal_abs_score_min": REVERSAL_SCORE_THRESHOLD,
    }


async def check_and_trade(bot, admin_ids: list[int]) -> list[dict]:
    """Run one paper-trading cycle with session management."""
    if not FEATURE_AUTOTRADE:
        return []
    async with _trade_lock:
        return await _check_and_trade_locked(bot, admin_ids)


async def _check_and_trade_locked(bot, admin_ids: list[int]) -> list[dict]:
    """Actual trading logic — always called under lock."""

    # Load session state from BACKTEST.md on first run
    if not session_manager._loaded:
        try:
            from github_export import _github_get, BACKTEST_FILE
            backtest_content, _ = await _github_get(BACKTEST_FILE)
            if backtest_content:
                session_manager._load_from_backtest(backtest_content)
        except Exception as e:
            logger.debug("Failed to load session state from GitHub: %s", e)

    # Check if current session should be closed
    if session_manager.should_close_session():
        closed_session = session_manager.close_session()
        # Reset capital for new session
        await update_backtest_capital(SESSION_START_CAPITAL)
        session_manager.update_capital(SESSION_START_CAPITAL)
        events.append({
            "event": "session_closed",
            "session_id": closed_session["session_id"],
            "pnl": closed_session["pnl"],
            "lesson": closed_session["lesson"],
        })
        logger.info(
            f"🏁 Session #{closed_session['session_id']} closed. "
            f"PnL: ${closed_session['pnl']:+.2f}. New session started."
        )
        # Export updated sessions to GitHub
        try:
            await _export_backtest_snapshot()
        except Exception:
            pass

    config = await get_backtest_config()
    if not config.get("enabled", 1):
        return events

    # Update session manager with current capital
    current_capital = config.get("capital", 100.0)
    session_manager.update_capital(current_capital)

    contexts = await get_recent_daily_contexts(limit=RECENT_CONTEXT_LIMIT, max_age_hours=None)

    open_positions = [row for row in await get_backtest_signals() if row.get("status") == "open"]

    # No digest at all — trade on market signals
    if not contexts:
        logger.info("No digest contexts — trading on market signals only")
        consensus = {
            "consensus_verdict": "NEUTRAL",
            "verdict_score": 0,
            "contexts": [],
            "candidates": [],
        }
    else:
        consensus = build_digest_consensus(contexts)
        # If digest has no candidates — fall back to signals
        if not consensus.get("candidates"):
            logger.info("Digest has no trade candidates — falling back to market signals")

    cv = consensus.get("consensus_verdict", "NEUTRAL")
    use_follow = _signal_follow_active(cv, consensus.get("candidates") or [])

    symbols = {candidate["symbol"] for candidate in consensus.get("candidates", [])}
    symbols.update(position["symbol"] for position in open_positions)
    if use_follow:
        symbols |= set(CRYPTO_SIGNAL_SYMBOLS)

    from signals import fetch_markets_bundle

    gh_repo = os.getenv("GITHUB_REPO", "borzenkovandrej07-alt/DIALECTIC_EDg")
    markets_bundle = await fetch_markets_bundle(gh_repo)

    prices = await fetch_current_prices(list(symbols))
    signal_bias = await _fetch_crypto_signal_bias(
        list(symbols),
        cv,
        neutral_follow=use_follow,
        markets_bundle=markets_bundle,
    )
    if use_follow:
        consensus = _append_signal_follow_candidates(consensus, prices, signal_bias, open_positions=open_positions)

    for position in open_positions:
        closed_event = await _close_position_if_needed(position, prices, signal_bias, consensus)
        if closed_event:
            events.append(closed_event)
            await _notify_admins(bot, admin_ids, closed_event)

    remaining_open = [row for row in await get_backtest_signals() if row.get("status") == "open"]
    if remaining_open:
        if events:
            await _export_backtest_snapshot()
        return events

    ranked = rank_trade_candidates(consensus, prices, signal_bias)
    if not ranked:
        if events:
            await _export_backtest_snapshot()
        return events

    best = ranked[0]
    if not best.get("ready"):
        if LOG_AUTOTRADE_SKIPS:
            await append_trade_decision_log(
                "autotrade_skip_not_ready",
                {
                    "reason": best.get("blocked_reason") or "below_open_threshold",
                    "threshold": OPEN_SCORE_THRESHOLD,
                    "best": {k: best.get(k) for k in (
                        "symbol", "direction", "total_score", "digest_score",
                        "proximity_score", "signal_score_component", "signal_direction",
                        "blocked_reason", "ready",
                    )},
                    "runner_up": ({k: ranked[1].get(k) for k in (
                        "symbol", "direction", "total_score", "signal_direction",
                    )} if len(ranked) > 1 else None),
                    "consensus_verdict": consensus.get("consensus_verdict"),
                    "signal_follow_active": use_follow,
                    "digest_contexts_used": consensus.get("contexts", []),
                    "signal_bias_excerpt": {
                        sym: {
                            "direction": signal_bias.get(sym, {}).get("direction"),
                            "score": signal_bias.get(sym, {}).get("score"),
                            "reasons": (signal_bias.get(sym, {}).get("reasons") or [])[:4],
                        }
                        for sym in sorted(set(signal_bias.keys()) | {best.get("symbol")})
                    },
                    "scoring_legend": _scoring_legend(),
                    "markets_panel_snapshot": _markets_bundle_audit(markets_bundle),
                },
            )
        if events:
            await _export_backtest_snapshot()
        return events

    decision_audit = {
        "action": "open_simulated",
        "why": "Highest-ranked candidate met proximity + total_score threshold",
        "chosen": {k: best.get(k) for k in (
            "symbol", "direction", "entry", "target", "stop", "support",
            "digest_score", "proximity_score", "signal_score_component", "total_score",
            "signal_direction", "signal_reasons", "current_price", "context_dates",
        )},
        "runner_up": ({k: ranked[1].get(k) for k in (
            "symbol", "direction", "total_score", "signal_direction", "digest_score",
        )} if len(ranked) > 1 else None),
        "digest_contexts_used": consensus.get("contexts", []),
        "consensus_verdict": consensus.get("consensus_verdict"),
        "verdict_score": consensus.get("verdict_score"),
        "signal_bias": {
            sym: {
                "direction": signal_bias.get(sym, {}).get("direction"),
                "score": signal_bias.get(sym, {}).get("score"),
                "strength": signal_bias.get(sym, {}).get("strength"),
                "reasons": (signal_bias.get(sym, {}).get("reasons") or [])[:6],
            }
            for sym in sorted(signal_bias.keys())
        },
        "scoring_legend": _scoring_legend(),
        "feature_flags": {
            "FEATURE_AUTOTRADE": FEATURE_AUTOTRADE,
            "DATA_SOURCE_BINANCE_SIGNALS": DATA_SOURCE_BINANCE_SIGNALS,
            "AUTOTRADE_FOLLOW_SIGNALS_WHEN_NEUTRAL": AUTOTRADE_FOLLOW_SIGNALS_WHEN_NEUTRAL,
            "signal_follow_cycle": use_follow,
        },
        "markets_panel_snapshot": _markets_bundle_audit(markets_bundle),
    }

    if use_follow:
        decision_audit["signal_follow_mode"] = True
    if best.get("signal_follow_only"):
        decision_audit["opened_from_signal_follow"] = True

    notes = (
        f"Signal-follow (market) | {best['symbol']} {best['direction']} | "
        f"sig {best['signal_direction']}"
        if best.get("signal_follow_only")
        else (
            f"Digest consensus {consensus.get('consensus_verdict')} | "
            f"support {best['support']} | signal {best['signal_direction']}"
        )
    )
    trade_meta = json.dumps({
        "target": best.get("target") or 0.0,
        "stop": best.get("stop") or 0.0,
        "entry_plan": best.get("entry") or 0.0,
        "support": best.get("support") or 0,
        "context_dates": best.get("context_dates") or [],
        "consensus_verdict": consensus.get("consensus_verdict", "NEUTRAL"),
        "signal_direction": best.get("signal_direction", "NEUTRAL"),
        "signal_reasons": best.get("signal_reasons", []),
        "decision_audit": decision_audit,
    }, ensure_ascii=False)

    result = await add_backtest_signal(
        symbol=best["symbol"],
        direction=best["direction"],
        entry_price=float(best["current_price"]),
        source="auto_trader",
        quantity_pct=session_manager.get_adaptive_params().get("quantity_pct", 1.0),
        notes=notes,
        trade_log=trade_meta,
    )

    if result.get("status") != "opened":
        await append_trade_decision_log(
            "autotrade_open_failed",
            {"decision_audit": decision_audit, "result": result},
            signal_id=None,
        )
        return events

    sid = result.get("signal_id")
    await append_trade_decision_log(
        "autotrade_opened",
        decision_audit,
        signal_id=sid,
    )
    logger.info(
        "autotrade_opened %s %s score=%s audit=%s",
        best["symbol"],
        best["direction"],
        best.get("total_score"),
        json.dumps(decision_audit, ensure_ascii=False)[:2000],
    )

    opened_event = {
        "event": "opened",
        "symbol": best["symbol"],
        "direction": best["direction"],
        "entry_price": float(best["current_price"]),
        "target": float(best.get("target") or 0.0),
        "stop": float(best.get("stop") or 0.0),
        "support": int(best.get("support") or 0),
        "score": float(best.get("total_score") or 0.0),
        "signal_direction": best.get("signal_direction", "NEUTRAL"),
        "capital": float(result.get("capital_after") or 0.0),
    }
    events.append(opened_event)
    await _notify_admins(bot, admin_ids, opened_event)
    await _export_backtest_snapshot()
    logger.info("Opened paper trade %s %s at %.2f", best["symbol"], best["direction"], best["current_price"])
    return events


async def run_signal_trader(bot, admin_ids: list[int]):
    """Run the paper autotrader forever."""
    logger.info("Auto trader started, interval=%s sec", INTERVAL_SECONDS)
    while True:
        try:
            await check_and_trade(bot, admin_ids)
        except Exception as e:
            logger.error(f"Auto trader error: {e}")
        await asyncio.sleep(INTERVAL_SECONDS)


async def get_signal_trader_status() -> dict:
    """Return a richer status payload for /signalstatus."""
    config = await get_backtest_config()
    stats = await get_backtest_stats()
    signals = await get_backtest_signals()
    open_positions = [row for row in signals if row.get("status") == "open"]
    contexts = await get_recent_daily_contexts(limit=RECENT_CONTEXT_LIMIT, max_age_hours=CONTEXT_MAX_AGE_HOURS)
    latest_context = contexts[0] if contexts else None
    consensus = build_digest_consensus(contexts) if contexts else {
        "consensus_verdict": "NEUTRAL",
        "verdict_score": 0,
        "contexts": [],
        "candidates": [],
    }

    cv_status = consensus.get("consensus_verdict", "NEUTRAL")
    use_follow_status = _signal_follow_active(cv_status, consensus.get("candidates") or [])

    symbols_set = {c["symbol"] for c in consensus.get("candidates", [])[:8]}
    if use_follow_status:
        symbols_set |= set(CRYPTO_SIGNAL_SYMBOLS)
    symbols = sorted(symbols_set)
    consensus_display = consensus

    candidate_rows = []
    if symbols:
        prices = await fetch_current_prices(symbols)
        signal_bias = await _fetch_crypto_signal_bias(symbols, cv_status, neutral_follow=use_follow_status)
        consensus_display = _append_signal_follow_candidates(consensus, prices, signal_bias, open_positions=open_positions)
        if consensus_display.get("candidates"):
            candidate_rows = rank_trade_candidates(consensus_display, prices, signal_bias)[:3]

    active_positions = []
    for position in open_positions:
        meta = _parse_trade_meta(position)
        active_positions.append({
            "symbol": position["symbol"],
            "direction": position["direction"],
            "entry_price": float(position.get("entry_price") or 0.0),
            "quantity": float(position.get("quantity") or 0.0),
            "target": float(meta.get("target") or 0.0),
            "stop": float(meta.get("stop") or 0.0),
            "support": int(meta.get("support") or 0),
        })

    digest_pv = (latest_context or {}).get("prompt_versions") or {}
    snap_time = (latest_context or {}).get("model_inputs_snapshot") or {}
    snap_ts = snap_time.get("generated_at_utc") if isinstance(snap_time, dict) else None

    recent_decisions = await get_recent_trade_decisions(4)

    adaptive_params = session_manager.get_adaptive_params()

    return {
        "enabled": config.get("enabled", 1),
        "capital": float(config.get("capital", 100.0) or 100.0),
        "total_trades": stats.get("total", 0),
        "total_pnl": float(stats.get("total_pnl", 0.0) or 0.0),
        "open_positions": len(open_positions),
        "active_positions": active_positions,
        "tracked_symbols": [candidate["symbol"] for candidate in consensus_display.get("candidates", [])[:8]]
        if symbols
        else [],
        "signal_follow_active": use_follow_status,
        "daily_context_fresh": is_daily_context_fresh(latest_context),
        "consensus_verdict": consensus.get("consensus_verdict", "NEUTRAL"),
        "recent_contexts": consensus.get("contexts", []),
        "top_candidates": candidate_rows,
        "latest_digest_prompt_versions": digest_pv,
        "latest_digest_snapshot_utc": snap_ts,
        "recent_decisions": recent_decisions,
        "autotrade_feature_on": FEATURE_AUTOTRADE,
        "binance_signals_enabled": DATA_SOURCE_BINANCE_SIGNALS,
        "session_id": session_manager.current_session.session_id,
        "session_start": session_manager.current_session.start_time,
        "session_pnl": round(session_manager.current_session.total_pnl, 2),
        "session_trades": len(session_manager.current_session.trades),
        "past_sessions": len(session_manager.past_sessions),
        "adaptive_params": adaptive_params,
    }
