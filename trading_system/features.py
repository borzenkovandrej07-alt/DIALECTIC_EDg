"""
Feature layer: build_features(data) — numeric features from OHLC rows.

`data` can be a list of backtester.Candle or dicts with keys open/high/low/close/volume.
"""

from __future__ import annotations

import math
from statistics import mean, pstdev
from typing import Any, Mapping, Sequence


def _rows(data: Sequence[Any]) -> list[dict[str, float]]:
    rows: list[dict[str, float]] = []
    for c in data:
        if hasattr(c, "open"):
            rows.append({
                "open": float(c.open),
                "high": float(c.high),
                "low": float(c.low),
                "close": float(c.close),
                "volume": float(getattr(c, "volume", 0.0) or 0.0),
            })
        elif isinstance(c, Mapping):
            rows.append({
                "open": float(c.get("open", 0) or 0),
                "high": float(c.get("high", 0) or 0),
                "low": float(c.get("low", 0) or 0),
                "close": float(c.get("close", 0) or 0),
                "volume": float(c.get("volume", 0) or 0),
            })
    return rows


def build_features(data: Sequence[Any]) -> dict[str, Any]:
    """
    Build a compact feature dict for ML / rules / CLI display.

    Does not mutate inputs.
    """
    rows = _rows(data)
    n = len(rows)
    if n < 2:
        return {
            "n_candles": n,
            "error": "need_at_least_2_candles",
        }

    closes = [r["close"] for r in rows if r["close"] > 0]
    if len(closes) < 2:
        return {"n_candles": n, "error": "invalid_closes"}

    rets = []
    for i in range(1, len(closes)):
        if closes[i - 1] > 0:
            rets.append((closes[i] - closes[i - 1]) / closes[i - 1])

    last = rows[-1]
    sma_20 = mean(closes[-20:]) if len(closes) >= 20 else mean(closes)
    sma_50 = mean(closes[-50:]) if len(closes) >= 50 else mean(closes)

    vol = pstdev(rets[-50:]) * math.sqrt(252 * 24) if len(rets) >= 2 else 0.0  # scaled rough annualized proxy

    high_20 = max(r["high"] for r in rows[-20:])
    low_20 = min(r["low"] for r in rows[-20:])

    trend_score = 0.0
    if sma_20 > 0:
        trend_score = (closes[-1] - sma_20) / sma_20

    return {
        "n_candles": n,
        "last_close": closes[-1],
        "last_open": last["open"],
        "return_1": rets[-1] if rets else 0.0,
        "return_mean_20": mean(rets[-20:]) if len(rets) >= 2 else 0.0,
        "volatility_ret_std": pstdev(rets[-50:]) if len(rets) > 1 else 0.0,
        "volatility_annualized_proxy": float(vol),
        "sma_20": sma_20,
        "sma_50": sma_50,
        "dist_from_sma20_pct": float((closes[-1] / sma_20 - 1.0) * 100) if sma_20 else 0.0,
        "range_20_pct": float((high_20 - low_20) / low_20 * 100) if low_20 > 0 else 0.0,
        "trend_score_vs_sma20": float(trend_score),
        "volume_last": last["volume"],
    }
