"""
decision_engine.py — Signal filtering and decision logic.

Filters signals based on:
  - Confidence threshold
  - Risk/reward ratio
  - Asset availability
  - Duplicate detection

Only high-quality signals proceed to backtesting.
"""

import logging
from typing import Optional

from trading_signal import Signal, timeframe_to_hours

logger = logging.getLogger(__name__)


class DecisionEngine:
    """Filters and ranks signals before backtesting."""

    def __init__(
        self,
        min_confidence: float = 0.0,
        min_risk_reward: float = 1.0,
        allowed_assets: Optional[set] = None,
        max_signals_per_asset: int = 1,
    ):
        self.min_confidence = min_confidence
        self.min_risk_reward = min_risk_reward
        self.allowed_assets = allowed_assets
        self.max_signals_per_asset = max_signals_per_asset
        self._asset_counts: dict[str, int] = {}

    def evaluate(self, signal: Signal) -> tuple[bool, str]:
        """
        Evaluate a single signal.
        Returns (accepted, reason).
        """
        if not signal.validate():
            return False, "invalid_signal"

        if signal.confidence < self.min_confidence:
            return False, f"confidence_too_low ({signal.confidence:.1f} < {self.min_confidence:.1f})"

        risk = abs(signal.entry - signal.stop)
        reward = abs(signal.target - signal.entry)
        if risk <= 0:
            return False, "zero_risk"

        rr = reward / risk
        if rr < self.min_risk_reward:
            return False, f"rr_too_low ({rr:.2f} < {self.min_risk_reward:.2f})"

        if self.allowed_assets and signal.asset not in self.allowed_assets:
            return False, f"asset_not_allowed ({signal.asset})"

        asset_count = self._asset_counts.get(signal.asset, 0)
        if asset_count >= self.max_signals_per_asset:
            return False, f"max_signals_for_asset ({asset_count}/{self.max_signals_per_asset})"

        return True, "accepted"

    def filter(self, signals: list[Signal]) -> list[Signal]:
        """Filter a list of signals, returning only accepted ones."""
        accepted = []
        for sig in signals:
            ok, reason = self.evaluate(sig)
            if ok:
                accepted.append(sig)
                self._asset_counts[sig.asset] = self._asset_counts.get(sig.asset, 0) + 1
            else:
                logger.debug(f"Signal rejected: {sig.asset} {sig.direction} — {reason}")
        return accepted

    def rank(self, signals: list[Signal]) -> list[Signal]:
        """Rank signals by risk/reward ratio (descending)."""
        def score(sig: Signal) -> float:
            risk = abs(sig.entry - sig.stop)
            reward = abs(sig.target - sig.entry)
            rr = reward / risk if risk > 0 else 0
            return rr + (sig.confidence / 100.0) * 0.5

        return sorted(signals, key=score, reverse=True)

    def reset(self):
        """Reset asset counters for a new evaluation cycle."""
        self._asset_counts.clear()


def calculate_risk_reward(signal: Signal) -> float:
    """Calculate risk/reward ratio for a signal."""
    risk = abs(signal.entry - signal.stop)
    reward = abs(signal.target - signal.entry)
    return reward / risk if risk > 0 else 0.0


def estimate_position_size(
    capital: float,
    signal: Signal,
    risk_per_trade_pct: float = 0.02,
) -> float:
    """
    Estimate position size based on risk management.
    risk_per_trade_pct = max % of capital to risk on one trade.
    """
    risk_amount = capital * risk_per_trade_pct
    risk_distance = abs(signal.entry - signal.stop)
    if risk_distance <= 0:
        return 0.0
    return risk_amount / risk_distance
