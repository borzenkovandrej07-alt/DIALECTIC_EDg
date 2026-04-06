"""
CLI: `python main.py analyze BTC`, `backtest BTC`, `report`

Dispatched from main.py without changing bot startup when no subcommand.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
from datetime import datetime

from trading_system.batch_runner import map_parallel
from trading_system.config_loader import cache_dir_from_config, cli_results_path, load_trading_config
from trading_system.equity_metrics import equity_curve_from_pnl_pct, max_drawdown, profit_factor_from_pnls
from trading_system.features import build_features
from trading_system.market_cache import get_candles_cached
from trading_system.risk import passes_rr_filter, position_size_in_base_units, risk_reward_ratio

logger = logging.getLogger(__name__)


def _setup_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")


async def _analyze_one(
    symbol: str,
    cfg: dict,
) -> dict:
    from signals import fetch_markets_bundle

    cache_dir = cache_dir_from_config(cfg)
    tf = int(cfg.get("default_timeframe_hours", 168))
    lim = int(cfg.get("candle_limit", 120))
    ttl = int(cfg.get("cache_ttl_seconds", 3600))

    candles = await get_candles_cached(
        symbol.upper(),
        timeframe_hours=tf,
        limit=lim,
        cache_dir=cache_dir,
        ttl_seconds=ttl,
    )
    feats = build_features(candles)
    repo = os.getenv("GITHUB_REPO", "borzenkovandrej07-alt/DIALECTIC_EDg")
    bundle = await fetch_markets_bundle(repo)

    last = feats.get("last_close") or 0.0
    bt = cfg.get("backtest") or {}
    direction = str(bt.get("demo_direction", "LONG")).upper()
    if direction == "LONG":
        entry = last
        target = entry * (1 + float(bt.get("target_pct", 4)) / 100)
        stop = entry * (1 - float(bt.get("stop_pct", 2)) / 100)
    else:
        entry = last
        target = entry * (1 - float(bt.get("target_pct", 4)) / 100)
        stop = entry * (1 + float(bt.get("stop_pct", 2)) / 100)

    rr = risk_reward_ratio(entry, target, stop, direction)  # type: ignore[arg-type]
    rr_ok = passes_rr_filter(
        entry, target, stop, direction, float(cfg.get("min_risk_reward", 1.5))  # type: ignore[arg-type]
    )
    equity_demo = 10_000.0
    size = position_size_in_base_units(
        equity_demo, float(cfg.get("risk_per_trade_pct", 2.0)), entry, stop, direction  # type: ignore[arg-type]
    )

    return {
        "symbol": symbol.upper(),
        "candles": len(candles),
        "features": feats,
        "markets_verdict": (bundle.get("verdict") or {}).get("verdict"),
        "demo_rr": round(rr, 3),
        "passes_min_rr": rr_ok,
        "demo_position_size_units": round(size, 8) if last else 0,
        "notional_at_10k_equity": round(size * entry, 2) if last else 0,
    }


async def cmd_analyze(symbols: list[str]) -> int:
    cfg = load_trading_config()
    max_w = int(cfg.get("parallel_max_workers", 8))

    async def _one(sym: str):
        return await _analyze_one(sym, cfg)

    results = await map_parallel(list(dict.fromkeys(symbols)), _one, max_workers=max_w)
    for sym, res in results.items():
        if isinstance(res, Exception):
            print(f"[{sym}] ERROR: {res}", file=sys.stderr)
        else:
            print(json.dumps(res, ensure_ascii=False, indent=2))
    return 0


async def cmd_backtest(symbols: list[str]) -> int:
    """Реальные идеи: analysis_service → DecisionEngine → Backtester."""
    from core.decision_engine import DecisionEngine
    from core.results_export import save_enriched_backtest_json
    from metrics import calculate_metrics

    cfg = load_trading_config()
    out_path = cli_results_path(cfg)
    sym = [s.strip().upper() for s in symbols if s.strip()]

    print(
        "[cli] backtest: full analysis pipeline (agents) — может занять несколько минут…",
        flush=True,
    )
    engine = DecisionEngine()
    signals = await engine.run_pipeline(user_id=0, symbols_filter=sym)
    if not signals:
        print(
            "Нет сигналов после фильтров (в отчёте нет entry/target/stop по этим активам "
            "или не прошли confidence / R:R). Проверь дайджест / config.",
            file=sys.stderr,
        )
        return 1

    results = await engine.run_backtest(signals)
    if not results:
        print("Бэктест не вернул результатов (нет свечей?).", file=sys.stderr)
        return 1

    metrics = calculate_metrics(results)
    print(metrics.summary())
    save_enriched_backtest_json(results, metrics, signals, str(out_path))
    print(f"\nSaved (enriched): {out_path}")
    return 0


async def cmd_report_async() -> int:
    from database import get_backtest_config, get_backtest_signals, init_db

    await init_db()
    rows = await get_backtest_signals()
    cfg = await get_backtest_config()
    capital = float(cfg.get("capital", 100.0) or 100.0)

    closed = [r for r in rows if (r.get("status") or "").lower() == "closed"]
    closed.sort(key=lambda x: str(x.get("created_at") or ""))

    pnls_usd = [float(r.get("pnl") or 0) for r in closed]
    pnls_pct = [float(r.get("pnl_pct") or 0) for r in closed]

    eq = equity_curve_from_pnl_pct(pnls_pct, initial_capital=capital)
    mdd = max_drawdown(eq)
    pf = profit_factor_from_pnls(pnls_usd) if pnls_usd else 0.0

    wins = sum(1 for p in pnls_usd if p > 0)
    losses = sum(1 for p in pnls_usd if p < 0)

    print("=" * 52)
    print("PAPER / BACKTEST REPORT (database)")
    print("=" * 52)
    print(f"Capital (config): ${capital:,.2f}")
    print(f"Closed trades:    {len(closed)}")
    print(f"Wins / Losses:    {wins} / {losses}")
    print(f"Sum PnL (USD):    ${sum(pnls_usd):+,.2f}")
    print(f"Max drawdown:     {mdd * 100:.2f}%")
    print(f"Profit factor:    {pf:.2f}" if pf != float("inf") else "Profit factor:    inf")
    if eq:
        print(f"Final equity:     ${eq[-1]:,.2f}")
    print("=" * 52)
    return 0


def cmd_report() -> int:
    return asyncio.run(cmd_report_async())


def run_cli(argv: list[str] | None = None) -> int:
    _setup_logging()
    argv = argv if argv is not None else sys.argv[1:]
    parser = argparse.ArgumentParser(description="Dialectic Edge trading CLI")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_an = sub.add_parser("analyze", help="Features + market bundle + R/R demo (batch)")
    p_an.add_argument("symbols", nargs="+", help="e.g. BTC ETH")
    p_bt = sub.add_parser("backtest", help="Run demo backtest on historical candles (batch)")
    p_bt.add_argument("symbols", nargs="+")
    sub.add_parser("report", help="PnL / equity / drawdown from SQLite backtest_signals")

    args = parser.parse_args(argv)

    if args.cmd == "analyze":
        return asyncio.run(cmd_analyze(args.symbols))
    if args.cmd == "backtest":
        return asyncio.run(cmd_backtest(args.symbols))
    if args.cmd == "report":
        return cmd_report()
    return 1


if __name__ == "__main__":
    raise SystemExit(run_cli())
