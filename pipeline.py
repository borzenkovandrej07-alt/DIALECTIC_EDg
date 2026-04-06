"""
pipeline.py — Connects data → analysis → signals → backtest → metrics.

Core functions:
  run_pipeline()       — one full cycle
  run_full_evaluation() — run on historical data and get real results
"""

import asyncio
import json
import logging
import re
from datetime import datetime
from typing import Optional

from trading_signal import (
    Signal,
    parse_signals_from_daily_context,
    parse_signals_from_predictions,
    parse_signals_from_backtest,
    timeframe_to_hours,
    serialize_signals,
    deserialize_signals,
)
from core.decision_engine import DecisionEngine
from backtester import Backtester, BacktestResult, Candle, get_candles
from metrics import calculate_metrics, Metrics, save_results, load_results

logger = logging.getLogger(__name__)


async def run_pipeline(
    signals: list,
    decision_engine: Optional[DecisionEngine] = None,
    backtester: Optional[Backtester] = None,
    fetch_candles: bool = True,
) -> tuple[list[BacktestResult], Metrics]:
    """
    Run the full pipeline on a list of signals.

    1. Filter signals through DecisionEngine
    2. Fetch OHLC candles for each asset
    3. Run Backtester
    4. Calculate Metrics

    Returns (results, metrics).
    """
    if not signals:
        logger.warning("No signals to evaluate")
        return [], Metrics()

    # Step 1: Filter
    engine = decision_engine or DecisionEngine()
    filtered = engine.filter(signals)
    logger.info(f"Signals: {len(signals)} total → {len(filtered)} after filtering")

    if not filtered:
        return [], Metrics()

    # Step 2: Fetch candles
    candles_map: dict[str, list[Candle]] = {}
    if fetch_candles:
        tasks = []
        for sig in filtered:
            hours = timeframe_to_hours(sig.timeframe)
            tasks.append(get_candles(sig.asset, timeframe_hours=hours, limit=30))

        candle_results = await asyncio.gather(*tasks, return_exceptions=True)
        for sig, candles_result in zip(filtered, candle_results):
            if isinstance(candles_result, Exception):
                logger.warning(f"Failed to fetch candles for {sig.asset}: {candles_result}")
                continue
            if candles_result:
                candles_map[sig.asset] = candles_result

    # Step 3: Backtest
    bt = backtester or Backtester()
    bt.reset()
    results = bt.test_signals(filtered, candles_map)

    # Step 4: Metrics
    metrics = calculate_metrics(results)

    return results, metrics


async def _load_signals_from_digest_cache(limit: int = 20) -> list[Signal]:
    """
    Load historical signals from DIGEST_CACHE.md on GitHub.
    This lets us validate past analysis without waiting for new /daily.
    """
    signals = []
    try:
        from github_export import _github_get, DIGEST_CACHE_FILE
        content, _ = await _github_get(DIGEST_CACHE_FILE)
        if not content:
            return signals

        # Parse each digest section
        sections = re.split(r'## 📊 \d{2}\.\d{2}\.\d{4}', content)
        for section in sections[1:]:  # skip header
            # Extract verdict
            verdict = "NEUTRAL"
            if any(w in section.upper() for w in ["БЫЧ", "BUY", "LONG", "BULLISH"]):
                verdict = "BUY"
            elif any(w in section.upper() for w in ["МЕДВ", "SELL", "SHORT", "BEARISH"]):
                verdict = "SELL"

            # Extract trading plans
            entries = {}
            targets = {}
            stops = {}
            timeframes = {}

            for match in re.finditer(r'[-•]\s*(?:Актив|Asset)\s*:\s*(\w+)', section, re.IGNORECASE):
                sym = match.group(1).upper()
                block = section[match.end():match.end()+200]
                entry_m = re.search(r'(?:Вход|Entry)\s*:\s*\$?([\d,\.]+)', block, re.IGNORECASE)
                target_m = re.search(r'(?:Цель|Target|Тейк)\s*:\s*\$?([\d,\.]+)', block, re.IGNORECASE)
                stop_m = re.search(r'(?:Стоп|Stop)\s*:\s*\$?([\d,\.]+)', block, re.IGNORECASE)
                tf_m = re.search(r'(?:Горизонт|Horizon)\s*:\s*(\w+)', block, re.IGNORECASE)

                if entry_m:
                    entries[sym] = float(entry_m.group(1).replace(',', ''))
                if target_m:
                    targets[sym] = float(target_m.group(1).replace(',', ''))
                if stop_m:
                    stops[sym] = float(stop_m.group(1).replace(',', ''))
                if tf_m:
                    timeframes[sym] = tf_m.group(1)

            # Create signals
            for sym in entries:
                entry = entries.get(sym, 0)
                target = targets.get(sym, 0)
                stop = stops.get(sym, 0)
                tf = timeframes.get(sym, "1w")

                if entry <= 0 or target <= 0 or stop <= 0:
                    continue

                direction = "LONG" if (stop < entry < target) else "SHORT" if (target < entry < stop) else None
                if direction is None:
                    continue

                signals.append(Signal(
                    asset=sym,
                    direction=direction,
                    entry=entry,
                    target=target,
                    stop=stop,
                    timeframe=tf,
                    source="digest_cache",
                    timestamp=datetime.now().isoformat(),
                ))

        logger.info(f"Loaded {len(signals)} signals from DIGEST_CACHE.md")
    except Exception as e:
        logger.warning(f"Failed to load signals from DIGEST_CACHE.md: {e}")

    return signals[:limit]


async def run_full_evaluation(
    source: str = "daily_context",
    limit: int = 10,
    save_to_file: str = "results.json",
    decision_params: Optional[dict] = None,
) -> Metrics:
    """
    Run a full evaluation on historical or live data.

    1. Get signals from source (daily_context, predictions, backtest, or digest_cache)
    2. Filter and rank
    3. Backtest against real candles
    4. Calculate and save metrics
    5. Print summary
    """
    logger.info(f"🚀 Starting full evaluation (source={source}, limit={limit})")

    # Step 1: Get signals
    signals = await _load_signals(source, limit)
    if not signals:
        logger.warning("No signals found. Run /daily first to generate analysis.")
        return Metrics()

    logger.info(f"Loaded {len(signals)} signals")

    # Step 2: Decision engine
    params = decision_params or {}
    engine = DecisionEngine(
        min_confidence=params.get("min_confidence", 0.0),
        min_risk_reward=params.get("min_risk_reward", 1.0),
        allowed_assets=params.get("allowed_assets"),
        max_signals_per_asset=params.get("max_signals_per_asset", 1),
    )

    # Step 3: Run pipeline
    results, metrics = await run_pipeline(
        signals=signals,
        decision_engine=engine,
        fetch_candles=True,
    )

    # Step 4: Save results
    if save_to_file:
        save_results(results, metrics, save_to_file)

    # Step 5: Print summary
    print("\n" + metrics.summary())

    return metrics


async def _load_signals(source: str, limit: int) -> list[Signal]:
    """Load signals from the specified source."""
    signals = []

    if source == "daily_context":
        from database import get_recent_daily_contexts
        contexts = await get_recent_daily_contexts(limit=limit, max_age_hours=None)
        for ctx in contexts:
            signals.extend(parse_signals_from_daily_context(ctx))

    elif source == "predictions":
        from database import get_pending_predictions
        predictions = await get_pending_predictions()
        signals = parse_signals_from_predictions(predictions[:limit])

    elif source == "backtest":
        from database import get_backtest_signals
        all_signals = await get_backtest_signals()
        signals = parse_signals_from_backtest(all_signals[:limit])

    elif source == "digest_cache":
        signals = await _load_signals_from_digest_cache(limit)

    return signals


async def evaluate_latest_digest() -> Metrics:
    """
    Convenience function: evaluate the latest digest analysis.
    This is what you run after /daily to see how good the analysis was.
    """
    return await run_full_evaluation(
        source="daily_context",
        limit=5,
        save_to_file="results.json",
    )


async def evaluate_all_pending() -> Metrics:
    """
    Evaluate all pending predictions from the database.
    """
    return await run_full_evaluation(
        source="predictions",
        limit=100,
        save_to_file="results_pending.json",
    )


async def evaluate_digest_history(limit: int = 20) -> Metrics:
    """
    Evaluate historical signals from DIGEST_CACHE.md on GitHub.
    This validates past analysis without waiting for new /daily.
    """
    return await run_full_evaluation(
        source="digest_cache",
        limit=limit,
        save_to_file="results_history.json",
    )
