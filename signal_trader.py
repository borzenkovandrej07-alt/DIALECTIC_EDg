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
        logger.warning(f"Binance signal bias fetch error: {e}")
        # Fallback: try multiple sources
        bias = {}
        try:
            from signals import fetch_binance_signals
            raw = await fetch_binance_signals([f"{symbol}USDT" for symbol in crypto_symbols])
            if raw:
                from signals import build_signal_bias_map
                bias = build_signal_bias_map(raw)
        except Exception as e2:
            logger.warning(f"Bybit/Spot fallback also failed: {e2}")

        # Final fallback: CoinGecko prices
        if not bias:
            try:
                import aiohttp
                ids = []
                for s in crypto_symbols:
                    cg_id = {"BTC": "bitcoin", "ETH": "ethereum", "BNB": "binancecoin", "SOL": "solana"}.get(s)
                    if cg_id:
                        ids.append(cg_id)
                if ids:
                    url = f"https://api.coingecko.com/api/v3/simple/price"
                    params = {"ids": ",".join(ids), "vs_currencies": "usd", "include_24hr_change": "true"}
                    async with aiohttp.ClientSession() as session:
                        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                            data = await resp.json()
                            for s in crypto_symbols:
                                cg_id = {"BTC": "bitcoin", "ETH": "ethereum", "BNB": "binancecoin", "SOL": "solana"}.get(s)
                                if cg_id and cg_id in data:
                                    change = data[cg_id].get("usd_24h_change", 0)
                                    price = data[cg_id].get("usd", 0)
                                    bias[s] = {
                                        "symbol": s,
                                        "score": -abs(change) if change < 0 else abs(change),
                                        "direction": "SHORT" if change < 0 else "LONG",
                                        "strength": abs(change),
                                        "reasons": [f"CoinGecko 24h {change:+.2f}%"],
                                        "last_price": price,
                                    }
            except Exception as e3:
                logger.warning(f"CoinGecko fallback also failed: {e3}")

        # Last resort: assume SHORT (price falling = buy opportunity)
        if not bias:
            for symbol in crypto_symbols:
                price = prices.get(symbol) or 0
                if price > 0:
                    bias[symbol] = {
                        "symbol": symbol,
                        "score": -15.0,
                        "direction": "SHORT",
                        "strength": 15.0,
                        "reasons": ["Fallback — price falling"],
                        "last_price": price,
                    }

        return {symbol: bias.get(symbol, {}) for symbol in crypto_symbols}


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
    entry_price = float(position.get("entry_price") or 0.0)
    reason = ""

    if direction == "BUY":
        if target and current_price >= target:
            reason = "Target hit — фиксация прибыли"
        elif stop and current_price <= stop:
            reason = "Stop loss hit"
    elif direction == "SELL":
        if target and current_price <= target:
            reason = "Target hit — фиксация прибыли"
        elif stop and current_price >= stop:
            reason = "Stop loss hit"

    if not reason:
        return None

    result = await close_backtest_signal(position["id"], current_price, reason=reason)
    if not result:
        return None

    session_manager.record_trade({
        "symbol": symbol,
        "direction": direction,
        "entry_price": entry_price,
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
        "entry_price": entry_price,
        "exit_price": current_price,
        "reason": reason,
        "pnl": float(result.get("pnl") or 0.0),
        "pnl_pct": float(result.get("pnl_pct") or 0.0),
        "capital": float(result.get("new_capital") or 0.0),
    }


async def _close_on_signal_reversal(position: dict, prices: dict, signal_bias: dict) -> dict | None:
    """
    Close position if market signal reversed against our direction.
    E.g., we bought (BUY), but now signal shows SHORT (price falling) — close and cut loss.
    """
    symbol = position["symbol"]
    if symbol not in CRYPTO_SIGNAL_SYMBOLS:
        return None
    
    current_price = float(prices.get(symbol) or 0.0)
    if current_price <= 0:
        return None
    
    meta = _parse_trade_meta(position)
    direction = (position.get("direction") or "").upper()
    signal_direction = (signal_bias.get(symbol, {}).get("direction") or "NEUTRAL").upper()
    
    reversal_threshold = REVERSAL_SCORE_THRESHOLD
    signal = signal_bias.get(symbol, {})
    signal_score = abs(float(signal.get("score") or 0.0))
    
    reason = ""
    
    if direction == "BUY" and signal_direction == "SHORT" and signal_score >= reversal_threshold:
        reason = f"Signal reversal: {signal_direction} (score={signal_score:.1f})"
    elif direction == "SELL" and signal_direction == "LONG" and signal_score >= reversal_threshold:
        reason = f"Signal reversal: {signal_direction} (score={signal_score:.1f})"
    
    if not reason:
        return None
    
    result = await close_backtest_signal(position["id"], current_price, reason=reason)
    if not result:
        return None
    
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
    elif event["event"] == "partial_closed":
        emoji = "🟢" if event.get("pnl", 0) >= 0 else "🔴"
        msg = (
            f"🎯 *ЧАСТИЧНАЯ ФИКСАЦИЯ*\n"
            f"{emoji} *{event['symbol']}* {event['direction']}\n"
            f"Вход: `${event['entry_price']:,.2f}` | Выход: `${event['exit_price']:,.2f}`\n"
            f"Закрыто: {event.get('quantity_closed', 0):.6f} шт | Осталось: {event.get('quantity_remaining', 0):.6f} шт\n"
            f"PnL: `{event.get('pnl', 0):+,.2f}` ({event.get('pnl_pct', 0):+.2f}%)\n"
            f"Причина: {event['reason']}\n"
            f"Баланс: `${event.get('capital', 0):,.2f}`"
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
    events = []

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
        await update_backtest_capital(SESSION_START_CAPITAL)
        session_manager.update_capital(SESSION_START_CAPITAL)
        events.append({
            "event": "session_closed",
            "session_id": closed_session["session_id"],
            "pnl": closed_session["pnl"],
            "lesson": closed_session["lesson"],
        })
        logger.info(f"Session #{closed_session['session_id']} closed. PnL: ${closed_session['pnl']:+.2f}")

    config = await get_backtest_config()
    if not config.get("enabled", 1):
        return events

    current_capital = config.get("capital", 100.0)
    session_manager.update_capital(current_capital)

    # Step 1: Get current open positions
    open_positions = [row for row in await get_backtest_signals() if row.get("status") == "open"]

    # Step 2: Build consensus and signals
    contexts = await get_recent_daily_contexts(limit=RECENT_CONTEXT_LIMIT, max_age_hours=None)
    if not contexts:
        consensus = {
            "consensus_verdict": "NEUTRAL",
            "verdict_score": 0,
            "contexts": [],
            "candidates": [],
        }
    else:
        consensus = build_digest_consensus(contexts)
        if not consensus.get("candidates"):
            consensus = {
                "consensus_verdict": "NEUTRAL",
                "verdict_score": 0,
                "contexts": [],
                "candidates": [],
            }

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
        list(symbols), cv, neutral_follow=use_follow, markets_bundle=markets_bundle,
    )
    if use_follow:
        consensus = _append_signal_follow_candidates(consensus, prices, signal_bias, open_positions=open_positions)

    # Step 3: Close positions that hit target/stop OR signal reversal
    for position in open_positions:
        closed_event = await _close_position_if_needed(position, prices, signal_bias, consensus)
        if closed_event:
            events.append(closed_event)
            await _notify_admins(bot, admin_ids, closed_event)
            continue
        
        signal_reversal_event = await _close_on_signal_reversal(position, prices, signal_bias)
        if signal_reversal_event:
            events.append(signal_reversal_event)
            await _notify_admins(bot, admin_ids, signal_reversal_event)

    # Refresh open positions after closes
    open_positions = [row for row in await get_backtest_signals() if row.get("status") == "open"]
    if len(open_positions) >= 5:
        if events:
            await _export_backtest_snapshot()
        return events

    # Step 4: Open new positions (up to 5 total)
    ranked = rank_trade_candidates(consensus, prices, signal_bias)
    held_symbols = {p["symbol"] for p in open_positions}

    for candidate in ranked:
        if len(open_positions) >= 5:
            break
        if not candidate.get("ready"):
            continue
        if candidate["symbol"] in held_symbols:
            continue

        support = candidate.get("support") or 0
        notes = f"Signal-follow | {candidate['symbol']} {candidate['direction']}"
        trade_meta = json.dumps({
            "target": candidate.get("target") or 0.0,
            "stop": candidate.get("stop") or 0.0,
            "entry_plan": candidate.get("entry") or 0.0,
            "support": support,
            "consensus_verdict": cv,
            "signal_direction": candidate.get("signal_direction", "NEUTRAL"),
        }, ensure_ascii=False)

        try:
            result = await add_backtest_signal(
                symbol=candidate["symbol"],
                direction=candidate["direction"],
                entry_price=float(candidate["current_price"]),
                source="auto_trader",
                quantity_pct=session_manager.get_adaptive_params().get("quantity_pct", 0.15),
                notes=notes,
                trade_log=trade_meta,
            )
            if result.get("status") == "opened":
                events.append({
                    "event": "opened",
                    "symbol": candidate["symbol"],
                    "direction": candidate["direction"],
                    "entry_price": float(candidate["current_price"]),
                    "target": float(candidate.get("target") or 0.0),
                    "stop": float(candidate.get("stop") or 0.0),
                    "support": support,
                    "score": float(candidate.get("total_score") or 0.0),
                    "signal_direction": candidate.get("signal_direction", "NEUTRAL"),
                    "capital": float(result.get("capital_after", 0.0)),
                })
                held_symbols.add(candidate["symbol"])
                open_positions.append(result)
                logger.info(f"Opened {candidate['symbol']} {candidate['direction']} at {candidate['current_price']}")
        except Exception as e:
            logger.error(f"Failed to open {candidate['symbol']}: {e}")
            continue

    # Send one summary notification if any positions were opened
    opened_events = [e for e in events if e.get("event") == "opened"]
    if opened_events and bot and admin_ids:
        lines = ["🎯 *НОВЫЕ ПОЗИЦИИ*\n"]
        for ev in opened_events:
            lines.append(f"{'🟢' if ev['direction'] == 'BUY' else '🔴'} *{ev['symbol']}* {ev['direction']}")
            lines.append(f"  Вход: ${ev['entry_price']:,.2f}")
            lines.append(f"  Тейк: ${ev['target']:,.2f} | Стоп: ${ev['stop']:,.2f}")
            lines.append(f"  Score: {ev['score']:.1f} | Сигнал: {ev.get('signal_direction', 'NEUTRAL')}")
            lines.append("")
        lines.append(f"💵 Баланс: ${opened_events[-1]['capital']:,.2f}")
        msg = "\n".join(lines)
        for admin_id in admin_ids:
            try:
                await bot.send_message(admin_id, msg, parse_mode="Markdown")
            except Exception:
                pass

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
    # Load fresh state from GitHub BACKTEST.md
    if not session_manager._loaded:
        try:
            from github_export import _github_get, BACKTEST_FILE
            backtest_content, _ = await _github_get(BACKTEST_FILE)
            if backtest_content:
                session_manager._load_from_backtest(backtest_content)
                # Also sync to local DB
                from database import update_backtest_capital
                await update_backtest_capital(session_manager.current_session.capital)
        except Exception as e:
            logger.debug("Failed to load session state from GitHub: %s")

    # Also load open positions from GitHub if local DB is empty
    try:
        from github_export import _github_get, BACKTEST_FILE
        backtest_content, _ = await _github_get(BACKTEST_FILE)
        if backtest_content:
            import re
            
            # Parse capital
            capital_match = re.search(r'Текущий:\s*\*\*\$([\d,\.]+)\*\*', backtest_content)
            github_capital = 100.0
            if capital_match:
                github_capital = float(capital_match.group(1).replace(',', ''))
                logger.info(f"GitHub capital: ${github_capital}")
            
            # Use GitHub capital as default, override local config
            if github_capital > 0:
                config["capital"] = github_capital
            
            # Parse open positions - format: - **BNB** BUY @ $584.95 (qty: 0.0256) — 2026-04-03
            open_section = re.search(r'## 🔵 Открытые позиции\n(.*?)(?=\n## |\Z)', backtest_content, re.DOTALL)
            if open_section:
                lines = open_section.group(1).strip().split('\n')
                for line in lines:
                    line = line.strip()
                    if not line.startswith('- **'):
                        continue
                    # **BNB** BUY @ $584.95 (qty: 0.0256) — 2026-04-03
                    match = re.search(r'\*\*(\w+)\*\*\s+(\w+)\s+@\$\s*([\d,\.]+)\s+\(qty:\s*([\d\.]+)\)', line)
                    if match:
                        symbol, direction, entry, qty = match.groups()
                        entry = float(entry.replace(',', ''))
                        qty = float(qty)
                        
                        # Try to find target/stop - not always present in BACKTEST.md
                        # Use sensible defaults if not found
                        target = entry * 1.04
                        stop = entry * 0.98
                        
                        trade_log = json.dumps({
                            "target": target, 
                            "stop": stop,
                            "entry_plan": entry,
                        }, ensure_ascii=False)
                        
                        signals.append({
                            "id": 0,
                            "symbol": symbol,
                            "direction": direction,
                            "entry_price": entry,
                            "quantity": qty,
                            "status": "open",
                            "trade_log": trade_log,
                        })
                        logger.info(f"Loaded open position from GitHub: {symbol} {direction} @ ${entry} qty={qty}")
    except Exception as e:
        logger.debug(f"Failed to load positions from GitHub: {e}")

    config = await get_backtest_config()
    stats = await get_backtest_stats()
    signals = await get_backtest_signals()
    
    # Load positions from GitHub BACKTEST.md
    try:
        from github_export import _github_get, BACKTEST_FILE
        backtest_content, _ = await _github_get(BACKTEST_FILE)
        logger.info(f"BACKTEST.md length: {len(backtest_content) if backtest_content else 0}")
        if backtest_content:
            import re
            
            # Parse capital
            cap_m = re.search(r'Текущий:\s*\*\*\$([\d,\.]+)\*\*', backtest_content)
            if cap_m:
                config["capital"] = float(cap_m.group(1).replace(',', ''))
                logger.info(f"Capital from GitHub: ${config['capital']}")
            
            # Find open positions section
            idx = backtest_content.find('Открытые позиции')
            logger.info(f"Index of 'Открытые позиции': {idx}")
            if idx != -1:
                section = backtest_content[idx:]
                next_header = section.find('\n## ', 10)
                if next_header != -1:
                    section = section[:next_header]
                
                logger.info(f"Section content: {repr(section[:500])}")
                
                signals = [] # Reset signals to use GitHub data
                for line in section.split('\n'):
                    if '**' in line and 'qty' in line:
                         m = re.search(r'\*\*(\w+)\*\*\s+(\w+)\s+@\s*\$\s*([\d,\.]+)\s+\(qty:\s*([\d\.]+)\)', line)
                         logger.info(f"Line: {repr(line)}, Match: {m}")
                         if m:
                             sym, dir, entry, qty = m.groups()
                             entry = float(entry.replace(',', ''))
                             qty = float(qty)
                             signals.append({
                                 "id": 0,
                                 "symbol": sym,
                                 "direction": dir,
                                 "entry_price": entry,
                                 "quantity": qty,
                                  "status": "open",
                                  "trade_log": json.dumps({"target": entry*1.04, "stop": entry*0.98}),
                              })
                             logger.info(f"Loaded: {sym} {dir} @ ${entry} qty={qty}")
                logger.info(f"Total from GitHub: {len(signals)}")
    except Exception as e:
        logger.warning(f"GitHub load error: {e}", exc_info=True)
    
    open_positions = [row for row in signals if row.get("status") == "open"]
    
    open_positions = [row for row in signals if row.get("status") == "open"]
    contexts = await get_recent_daily_contexts(limit=RECENT_CONTEXT_LIMIT, max_age_hours=None)

    if not contexts:
        try:
            from github_export import _github_get, DIGEST_CACHE_FILE
            digest_content, _ = await _github_get(DIGEST_CACHE_FILE)
            if digest_content:
                import re
                pattern = r'## 📊 (\d{2}\.\d{2}\.\d{4})'
                matches = list(re.finditer(pattern, digest_content))
                for match in matches:
                    date_str = match.group(1)
                    verdict = "NEUTRAL"
                    snippet_start = digest_content.find(f"## 📊 {date_str}")
                    if snippet_start != -1:
                        snippet = digest_content[snippet_start:snippet_start+800].upper()
                        if "БЫЧ" in snippet or "BUY" in snippet or "LONG" in snippet:
                            verdict = "BUY"
                        elif "МЕДВ" in snippet or "SELL" in snippet or "SHORT" in snippet:
                            verdict = "SELL"
                    contexts.append({
                        "created_at": date_str,
                        "verdict": verdict,
                        "symbols": [],
                    })
                logger.info(f"Loaded {len(contexts)} contexts from GitHub DIGEST_CACHE.md")
        except Exception as e:
            logger.debug(f"Failed to load from GitHub: {e}")
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

    # Debug: log all signals to find open position issue
    all_signals = await get_backtest_signals()
    open_check = [s for s in all_signals if s.get("status") == "open"]
    logger.info(f"DEBUG: all_signals={len(all_signals)}, open_check={len(open_check)}")
    for s in open_check:
        logger.info(f"DEBUG open: {s.get('symbol')} {s.get('direction')} qty={s.get('quantity')} status={s.get('status')}")

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
