"""
metrics.py — Calculate trading performance metrics.

Metrics:
  winrate, avg_win, avg_loss, profit_factor, total_pnl, sharpe_ratio
"""

import json
import logging
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class Metrics:
    """Aggregated trading performance metrics."""
    total_signals: int = 0
    wins: int = 0
    losses: int = 0
    timeouts: int = 0
    winrate: float = 0.0
    avg_win_pct: float = 0.0
    avg_loss_pct: float = 0.0
    total_pnl_pct: float = 0.0
    total_pnl_usd: float = 0.0
    profit_factor: float = 0.0
    best_trade_pct: float = 0.0
    worst_trade_pct: float = 0.0
    avg_holding_candles: float = 0.0
    max_consecutive_wins: int = 0
    max_consecutive_losses: int = 0
    by_asset: dict = None
    generated_at: str = ""

    def __post_init__(self):
        if self.by_asset is None:
            self.by_asset = {}

    def to_dict(self) -> dict:
        return asdict(self)

    def summary(self) -> str:
        """Human-readable summary."""
        lines = [
            "=" * 50,
            "📊 TRADING PERFORMANCE METRICS",
            "=" * 50,
            f"Total signals:     {self.total_signals}",
            f"Wins:              {self.wins}",
            f"Losses:            {self.losses}",
            f"Timeouts:          {self.timeouts}",
            f"Winrate:           {self.winrate:.1f}%",
            f"Avg win:           {self.avg_win_pct:+.2f}%",
            f"Avg loss:          {self.avg_loss_pct:+.2f}%",
            f"Total PnL:         {self.total_pnl_pct:+.2f}% (${self.total_pnl_usd:+.2f})",
            f"Profit factor:     {self.profit_factor:.2f}",
            f"Best trade:        {self.best_trade_pct:+.2f}%",
            f"Worst trade:       {self.worst_trade_pct:+.2f}%",
            f"Avg holding:       {self.avg_holding_candles:.1f} candles",
            f"Max win streak:    {self.max_consecutive_wins}",
            f"Max loss streak:   {self.max_consecutive_losses}",
        ]

        if self.by_asset:
            lines.append("")
            lines.append("By asset:")
            for asset, m in sorted(self.by_asset.items()):
                lines.append(f"  {asset}: {m.get('winrate', 0):.0f}% WR, PnL {m.get('total_pnl_pct', 0):+.2f}%")

        lines.append("=" * 50)

        if self.profit_factor >= 1.5:
            lines.append("✅ Profitable edge — system works")
        elif self.profit_factor >= 1.0:
            lines.append("🟡 Break-even — needs tuning")
        else:
            lines.append("🔴 Losing — no edge")

        return "\n".join(lines)


def calculate_metrics(results: list) -> Metrics:
    """
    Calculate metrics from a list of BacktestResult dicts or objects.
    """
    if not results:
        return Metrics(generated_at=datetime.now().isoformat())

    wins = []
    losses = []
    timeouts = []
    by_asset: dict[str, list] = {}

    for r in results:
        if isinstance(r, dict):
            result_type = r.get("result", "")
            pnl = float(r.get("pnl", 0) or 0)
            candles = int(r.get("candles_checked", 0) or 0)
            asset = r.get("asset", "UNKNOWN")
        else:
            result_type = r.result
            pnl = r.pnl
            candles = r.candles_checked
            asset = r.asset

        by_asset.setdefault(asset, [])
        by_asset[asset].append({"result": result_type, "pnl_pct": pnl})

        if result_type == "WIN":
            wins.append({"pnl": pnl, "candles": candles})
        elif result_type == "LOSS":
            losses.append({"pnl": pnl, "candles": candles})
        else:
            timeouts.append({"pnl": pnl, "candles": candles})

    total = len(results)
    win_count = len(wins)
    loss_count = len(losses)
    timeout_count = len(timeouts)

    winrate = (win_count / (win_count + loss_count) * 100) if (win_count + loss_count) > 0 else 0.0

    avg_win = sum(w["pnl"] for w in wins) / win_count if wins else 0.0
    avg_loss = sum(l["pnl"] for l in losses) / loss_count if losses else 0.0

    total_pnl = sum(w["pnl"] for w in wins) + sum(l["pnl"] for l in losses) + sum(t["pnl"] for t in timeouts)

    gross_profit = sum(w["pnl"] for w in wins)
    gross_loss = abs(sum(l["pnl"] for l in losses))
    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else (float("inf") if gross_profit > 0 else 0.0)

    all_pnls = [w["pnl"] for w in wins] + [l["pnl"] for l in losses] + [t["pnl"] for t in timeouts]
    best = max(all_pnls) if all_pnls else 0.0
    worst = min(all_pnls) if all_pnls else 0.0

    all_candles = [w["candles"] for w in wins] + [l["candles"] for l in losses] + [t["candles"] for t in timeouts]
    avg_candles = sum(all_candles) / len(all_candles) if all_candles else 0.0

    # Consecutive wins/losses
    max_consec_wins = 0
    max_consec_losses = 0
    cur_wins = 0
    cur_losses = 0

    for r in results:
        if isinstance(r, dict):
            result_type = r.get("result", "")
        else:
            result_type = r.result

        if result_type == "WIN":
            cur_wins += 1
            cur_losses = 0
            max_consec_wins = max(max_consec_wins, cur_wins)
        elif result_type == "LOSS":
            cur_losses += 1
            cur_wins = 0
            max_consec_losses = max(max_consec_losses, cur_losses)
        else:
            cur_wins = 0
            cur_losses = 0

    # By-asset breakdown
    asset_metrics = {}
    for asset, trades in by_asset.items():
        a_wins = [t for t in trades if t["result"] == "WIN"]
        a_losses = [t for t in trades if t["result"] == "LOSS"]
        a_total = len(trades)
        a_winrate = (len(a_wins) / (len(a_wins) + len(a_losses)) * 100) if (len(a_wins) + len(a_losses)) > 0 else 0.0
        a_pnl = sum(t["pnl_pct"] for t in trades)
        asset_metrics[asset] = {
            "total": a_total,
            "winrate": round(a_winrate, 1),
            "total_pnl_pct": round(a_pnl, 2),
        }

    return Metrics(
        total_signals=total,
        wins=win_count,
        losses=loss_count,
        timeouts=timeout_count,
        winrate=round(winrate, 1),
        avg_win_pct=round(avg_win, 2),
        avg_loss_pct=round(avg_loss, 2),
        total_pnl_pct=round(total_pnl, 2),
        total_pnl_usd=round(total_pnl, 2),
        profit_factor=round(profit_factor, 2) if profit_factor != float("inf") else 999.99,
        best_trade_pct=round(best, 2),
        worst_trade_pct=round(worst, 2),
        avg_holding_candles=round(avg_candles, 1),
        max_consecutive_wins=max_consec_wins,
        max_consecutive_losses=max_consec_losses,
        by_asset=asset_metrics,
        generated_at=datetime.now().isoformat(),
    )


def save_results(results: list, metrics: Metrics, filepath: str = "results.json"):
    """Save results and metrics to JSON file."""
    data = {
        "metrics": metrics.to_dict(),
        "results": [
            r if isinstance(r, dict) else r.to_dict()
            for r in results
        ],
        "generated_at": datetime.now().isoformat(),
    }

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    logger.info(f"✅ Results saved to {filepath}")


def load_results(filepath: str = "results.json") -> tuple[list, Optional[Metrics]]:
    """Load results and metrics from JSON file."""
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
        results = data.get("results", [])
        metrics_data = data.get("metrics", {})
        metrics = Metrics(**metrics_data) if metrics_data else None
        return results, metrics
    except Exception as e:
        logger.warning(f"Failed to load results from {filepath}: {e}")
        return [], None
