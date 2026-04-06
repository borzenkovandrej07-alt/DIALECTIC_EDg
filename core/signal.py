"""
Конвертация нормализованных идей → trading_signal.Signal.

Использует существующий parse_signals_from_predictions там, где возможно.
"""

from __future__ import annotations

import logging
from typing import Any

from trading_signal import parse_signals_from_predictions

logger = logging.getLogger(__name__)


def convert_ideas_to_signals(
    ideas: list[dict[str, Any]],
    *,
    default_timeframe: str = "1w",
    source: str = "analysis_service",
) -> list[Signal]:
    """
    direction == neutral уже отфильтрован адаптером.

    Нет entry/target/stop → не попадёт в ideas (адаптер).
    """
    preds: list[dict[str, Any]] = []
    for idea in ideas:
        d = (idea.get("direction") or "").lower()
        if d == "neutral":
            continue

        direction = "LONG" if d == "long" else "SHORT" if d == "short" else None
        if direction is None:
            continue

        preds.append({
            "asset": idea.get("asset", "").upper(),
            "direction": direction,
            "entry_price": float(idea.get("entry") or 0),
            "target_price": float(idea.get("target") or 0),
            "stop_loss": float(idea.get("stop") or 0),
            "timeframe": (idea.get("timeframe") or default_timeframe).lower(),
            "created_at": idea.get("timestamp"),
            "confidence": float(idea.get("confidence") or 0.0),
        })

    signals = parse_signals_from_predictions(preds)
    for sig in signals:
        sig.source = source
    return signals
