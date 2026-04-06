"""
Adapter: structured predictions from tracker → normalized «ideas» for Signal conversion.

Не трогает analysis_service.py — только нормализует dict'ы после extract_predictions_from_report.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


def normalize_prediction_ideas(preds: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Приводит записи трекера к единому виду:

        asset, direction (long|short), entry, target, stop,
        confidence, timestamp (ISO), timeframe

    Пропускает neutral / без цен / невалидные числа.
    """
    out: list[dict[str, Any]] = []
    for p in preds or []:
        asset = (p.get("asset") or "").strip().upper()
        if not asset:
            continue

        raw_dir = (p.get("direction") or "").strip().upper()
        if raw_dir in ("NEUTRAL", "ВНЕ РЫНКА", "CASH", "НАБЛЮДАТЬ", "OBSERVE"):
            continue

        try:
            entry = float(p.get("entry_price") or 0)
            target = float(p.get("target_price") or 0)
            stop = float(p.get("stop_loss") or 0)
        except (TypeError, ValueError):
            continue

        if entry <= 0 or target <= 0 or stop <= 0:
            logger.debug("skip idea %s: missing entry/target/stop", asset)
            continue

        if raw_dir in ("LONG", "BUY", "BULLISH"):
            direction = "long"
        elif raw_dir in ("SHORT", "SELL", "BEARISH"):
            direction = "short"
        else:
            direction = "neutral"

        if direction == "neutral":
            continue

        if direction == "long" and not (stop < entry < target):
            continue
        if direction == "short" and not (target < entry < stop):
            continue

        ts = p.get("created_at") or p.get("timestamp")
        if not ts:
            ts = datetime.now().isoformat()

        conf = p.get("confidence")
        try:
            conf_f = float(conf) if conf is not None else 0.0
        except (TypeError, ValueError):
            conf_f = 0.0

        tf = (p.get("timeframe") or "1w").strip().lower()

        out.append({
            "asset": asset,
            "direction": direction,
            "entry": entry,
            "target": target,
            "stop": stop,
            "confidence": conf_f,
            "timestamp": ts,
            "timeframe": tf,
        })
    return out
