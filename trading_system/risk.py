"""Risk management: position sizing (%% risk) and risk/reward filters."""

from __future__ import annotations

from typing import Literal


def risk_reward_ratio(
    entry: float,
    target: float,
    stop: float,
    direction: Literal["LONG", "SHORT"],
) -> float:
    """Reward / risk in price units (not %%)."""
    if entry <= 0:
        return 0.0
    if direction == "LONG":
        reward = max(0.0, target - entry)
        risk = max(1e-12, entry - stop)
    else:
        reward = max(0.0, entry - target)
        risk = max(1e-12, stop - entry)
    return reward / risk


def passes_rr_filter(
    entry: float,
    target: float,
    stop: float,
    direction: Literal["LONG", "SHORT"],
    min_rr: float,
) -> bool:
    return risk_reward_ratio(entry, target, stop, direction) >= min_rr


def position_size_in_base_units(
    equity: float,
    risk_pct: float,
    entry: float,
    stop: float,
    direction: Literal["LONG", "SHORT"],
) -> float:
    """
    Position size in asset units (e.g. BTC) for a cash equity account,
    risking `risk_pct`%% of equity if stop is hit.

    risk_amount = equity * (risk_pct / 100)
    per_unit_risk = |entry - stop|
    size = risk_amount / per_unit_risk
    """
    if equity <= 0 or risk_pct <= 0 or entry <= 0:
        return 0.0
    if direction == "LONG":
        risk_per_unit = entry - stop
    else:
        risk_per_unit = stop - entry
    if risk_per_unit <= 0:
        return 0.0
    risk_amount = equity * (risk_pct / 100.0)
    return risk_amount / risk_per_unit


def notional_position_value(
    equity: float,
    risk_pct: float,
    entry: float,
    stop: float,
    direction: Literal["LONG", "SHORT"],
) -> float:
    """Approximate USD notional = size * entry."""
    s = position_size_in_base_units(equity, risk_pct, entry, stop, direction)
    return s * entry
