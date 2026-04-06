"""Расширенный JSON для CLI / tooling: confidence, duration, signal_timestamp."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


def _parse_dt(raw: str | None) -> datetime | None:
    if not raw:
        return None
    try:
        s = raw.replace("Z", "+00:00") if "Z" in raw else raw
        return datetime.fromisoformat(s)
    except Exception:
        return None


def _duration_seconds(signal_ts: str, exit_ts: str) -> float | None:
    a = _parse_dt(signal_ts)
    b = _parse_dt(exit_ts)
    if not a or not b:
        return None
    return max(0.0, (b - a).total_seconds())


def save_enriched_backtest_json(
    results: list[Any],
    metrics: Any,
    signals: list[Any],
    filepath: str,
) -> None:
    """Сохраняет metrics + results с полями confidence и trade_duration_seconds."""
    by_key: dict[tuple[str, float], dict[str, Any]] = {}
    for s in signals:
        try:
            k = (s.asset.upper(), round(float(s.entry), 6))
            by_key[k] = {
                "confidence": float(getattr(s, "confidence", 0) or 0),
                "signal_timestamp": getattr(s, "timestamp", "") or "",
            }
        except Exception:
            continue

    rows: list[dict[str, Any]] = []
    for r in results:
        d = r.to_dict() if hasattr(r, "to_dict") else dict(r)
        k = (
            str(d.get("asset", "")).upper(),
            round(float(d.get("entry") or 0), 6),
        )
        meta = by_key.get(k) or {}
        conf = meta.get("confidence")
        sig_ts = meta.get("signal_timestamp") or d.get("signal_timestamp")
        exit_ts = d.get("exit_timestamp")
        dur = _duration_seconds(sig_ts, exit_ts) if sig_ts and exit_ts else None

        d = dict(d)
        d["confidence"] = conf
        d["signal_timestamp"] = sig_ts
        if dur is not None:
            d["trade_duration_seconds"] = round(dur, 3)
        rows.append(d)

    payload = {
        "metrics": metrics.to_dict() if hasattr(metrics, "to_dict") else metrics,
        "results": rows,
        "generated_at": datetime.now().isoformat(),
    }
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    logger.info("Saved enriched results to %s", filepath)
