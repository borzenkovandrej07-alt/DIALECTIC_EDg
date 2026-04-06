"""Local JSON cache for OHLC candles (speed + offline-friendly)."""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any

from backtester import Candle, get_candles

logger = logging.getLogger(__name__)


def _serialize(candles: list[Candle]) -> list[dict[str, Any]]:
    out = []
    for c in candles:
        out.append({
            "timestamp": c.timestamp.isoformat(),
            "open": c.open,
            "high": c.high,
            "low": c.low,
            "close": c.close,
            "volume": c.volume,
        })
    return out


def _deserialize(rows: list[dict[str, Any]]) -> list[Candle]:
    from datetime import datetime

    candles: list[Candle] = []
    for r in rows:
        ts = r.get("timestamp", "")
        try:
            t = datetime.fromisoformat(ts)
        except Exception:
            continue
        candles.append(Candle(
            timestamp=t,
            open=float(r.get("open", 0)),
            high=float(r.get("high", 0)),
            low=float(r.get("low", 0)),
            close=float(r.get("close", 0)),
            volume=float(r.get("volume", 0) or 0),
        ))
    return candles


async def get_candles_cached(
    asset: str,
    *,
    timeframe_hours: int,
    limit: int,
    cache_dir: Path,
    ttl_seconds: int,
) -> list[Candle]:
    """
    Return candles from disk cache if fresh; otherwise fetch via backtester.get_candles.
    """
    key = f"{asset.upper()}_{timeframe_hours}_{limit}.json"
    path = cache_dir / key
    now = time.time()

    if path.exists():
        try:
            with open(path, encoding="utf-8") as f:
                payload = json.load(f)
            if now - float(payload.get("fetched_at", 0)) <= ttl_seconds:
                return _deserialize(payload.get("candles", []))
        except Exception as e:
            logger.debug("cache read failed %s: %s", path, e)

    candles = await get_candles(asset, timeframe_hours=timeframe_hours, limit=limit)
    try:
        payload = {"fetched_at": now, "candles": _serialize(candles)}
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f)
    except Exception as e:
        logger.warning("cache write failed %s: %s", path, e)

    return candles
