"""Equity curve, max drawdown, profit factor — complements metrics.py without editing it."""

from __future__ import annotations

from typing import Sequence


def equity_curve_from_returns(
    returns: Sequence[float],
    initial_capital: float = 100.0,
) -> list[float]:
    """Compounded equity from per-trade return fractions (e.g. 0.02 = +2%%)."""
    eq = [initial_capital]
    for r in returns:
        eq.append(eq[-1] * (1.0 + r))
    return eq


def equity_curve_from_pnl_pct(
    pnls_pct: Sequence[float],
    initial_capital: float = 100.0,
) -> list[float]:
    """`pnls_pct` as percent points (+2.5 means +2.5%%)."""
    returns = [p / 100.0 for p in pnls_pct]
    return equity_curve_from_returns(returns, initial_capital)


def max_drawdown(equity: Sequence[float]) -> float:
    """Max drawdown as a positive fraction of peak (e.g. 0.15 = 15%%)."""
    if not equity:
        return 0.0
    peak = equity[0]
    max_dd = 0.0
    for x in equity:
        if x > peak:
            peak = x
        dd = (peak - x) / peak if peak > 0 else 0.0
        max_dd = max(max_dd, dd)
    return max_dd


def profit_factor_from_pnls(pnls: Sequence[float]) -> float:
    """Gross profit / gross loss (absolute)."""
    gp = sum(p for p in pnls if p > 0)
    gl = abs(sum(p for p in pnls if p < 0))
    if gl <= 0:
        return float("inf") if gp > 0 else 0.0
    return gp / gl
