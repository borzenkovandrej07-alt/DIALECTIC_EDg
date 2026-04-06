"""
signal.py — Structured trading signals.

Each analysis becomes a Signal with:
  asset, direction, entry, target, stop, timeframe

No structure = no signal = cannot validate.
"""

import json
import logging
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class Signal:
    """A single, structured, backtestable trading signal."""
    asset: str
    direction: str          # "LONG" or "SHORT"
    entry: float
    target: float
    stop: float
    timeframe: str          # "1h", "4h", "1d", "1w", etc.
    source: str             # "digest", "signal_follow", "manual"
    timestamp: str          # ISO format
    confidence: float = 0.0  # 0-100, optional
    reason: str = ""

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "Signal":
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})

    def validate(self) -> bool:
        """Signal is valid only if it has all required fields with sensible values."""
        if not self.asset:
            return False
        if self.direction not in ("LONG", "SHORT"):
            return False
        if self.entry <= 0:
            return False
        if self.target <= 0:
            return False
        if self.stop <= 0:
            return False
        if self.direction == "LONG" and not (self.stop < self.entry < self.target):
            return False
        if self.direction == "SHORT" and not (self.target < self.entry < self.stop):
            return False
        return True


def parse_signals_from_daily_context(context: dict) -> list[Signal]:
    """
    Convert a daily_context record into structured Signals.
    Skips NEUTRAL verdicts and entries without target/stop.
    """
    signals = []
    verdict = (context.get("verdict") or "NEUTRAL").upper()
    if verdict == "NEUTRAL":
        return signals

    symbols = context.get("symbols", []) or []
    entries = context.get("entries", {}) or {}
    targets = context.get("targets", {}) or {}
    stops = context.get("stop_losses", {}) or {}
    timeframes = context.get("timeframes", {}) or {}

    for symbol in symbols:
        entry = float(entries.get(symbol) or 0)
        target = float(targets.get(symbol) or 0)
        stop = float(stops.get(symbol) or 0)
        timeframe = (timeframes.get(symbol) or "1w").lower()

        if entry <= 0 or target <= 0 or stop <= 0:
            logger.debug(f"Skipping {symbol}: missing entry/target/stop (e={entry}, t={target}, s={stop})")
            continue

        direction = "LONG" if (stop < entry < target) else "SHORT" if (target < entry < stop) else None
        if direction is None:
            logger.debug(f"Skipping {symbol}: invalid price structure (entry={entry}, target={target}, stop={stop})")
            continue

        signal = Signal(
            asset=symbol.upper(),
            direction=direction,
            entry=entry,
            target=target,
            stop=stop,
            timeframe=timeframe,
            source="digest",
            timestamp=context.get("created_at", datetime.now().isoformat()),
            reason=context.get("news_summary", "")[:200],
        )

        if signal.validate():
            signals.append(signal)
        else:
            logger.debug(f"Skipping {symbol}: validation failed")

    return signals


def parse_signals_from_predictions(predictions: list[dict]) -> list[Signal]:
    """
    Convert prediction records from the DB into structured Signals.
    """
    signals = []
    for pred in predictions:
        asset = (pred.get("asset") or "").upper()
        direction = (pred.get("direction") or "").upper()
        entry = float(pred.get("entry_price") or 0)
        target = float(pred.get("target_price") or 0)
        stop = float(pred.get("stop_loss") or 0)
        timeframe = (pred.get("timeframe") or "1d").lower()

        if entry <= 0 or target <= 0 or stop <= 0:
            continue

        direction = "LONG" if direction in ("LONG", "BUY", "BULLISH") else "SHORT" if direction in ("SHORT", "SELL", "BEARISH") else None
        if direction is None:
            continue

        conf = float(pred.get("confidence") or 0.0)

        signal = Signal(
            asset=asset,
            direction=direction,
            entry=entry,
            target=target,
            stop=stop,
            timeframe=timeframe,
            source="prediction",
            timestamp=pred.get("created_at", datetime.now().isoformat()),
            confidence=conf,
        )

        if signal.validate():
            signals.append(signal)

    return signals


def parse_signals_from_backtest(signals_data: list[dict]) -> list[Signal]:
    """
    Convert backtest_signals from DB into structured Signals.
    Used for re-evaluating past trades.
    """
    signals = []
    for row in signals_data:
        asset = (row.get("symbol") or "").upper()
        direction = (row.get("direction") or "").upper()
        entry = float(row.get("entry_price") or 0)

        trade_log = row.get("trade_log") or "{}"
        try:
            meta = json.loads(trade_log)
        except Exception:
            meta = {}

        target = float(meta.get("target") or 0)
        stop = float(meta.get("stop") or 0)

        if entry <= 0 or target <= 0 or stop <= 0:
            continue

        direction = "LONG" if direction in ("LONG", "BUY") else "SHORT" if direction in ("SHORT", "SELL") else None
        if direction is None:
            continue

        signal = Signal(
            asset=asset,
            direction=direction,
            entry=entry,
            target=target,
            stop=stop,
            timeframe="1d",
            source=row.get("signal_source", "backtest"),
            timestamp=row.get("created_at", datetime.now().isoformat()),
        )

        if signal.validate():
            signals.append(signal)

    return signals


def timeframe_to_hours(timeframe: str) -> int:
    """Convert timeframe string to hours."""
    mapping = {
        "1h": 1, "2h": 2, "4h": 4, "6h": 6, "8h": 8, "12h": 12,
        "1d": 24, "2d": 48, "3d": 72,
        "1w": 168, "2w": 336,
        "1m": 720,
    }
    tf = timeframe.lower().strip()
    return mapping.get(tf, 24)


def serialize_signals(signals: list[Signal]) -> str:
    """Serialize signals to JSON string for storage."""
    return json.dumps([s.to_dict() for s in signals], ensure_ascii=False, indent=2)


def deserialize_signals(data: str) -> list[Signal]:
    """Deserialize signals from JSON string."""
    if not data:
        return []
    try:
        items = json.loads(data)
        return [Signal.from_dict(item) for item in items]
    except Exception as e:
        logger.warning(f"Failed to deserialize signals: {e}")
        return []
