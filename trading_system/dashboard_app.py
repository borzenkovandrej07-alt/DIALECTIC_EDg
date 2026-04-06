"""
Streamlit dashboard: signals, win/loss, PnL, equity curve.

Run from project root:
  streamlit run trading_system/dashboard_app.py
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import DB_PATH
from trading_system.config_loader import load_trading_config, cli_results_path
from trading_system.equity_metrics import equity_curve_from_pnl_pct, max_drawdown


@st.cache_data(ttl=30)
def load_trades() -> pd.DataFrame:
    try:
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql_query(
            "SELECT * FROM backtest_signals ORDER BY datetime(created_at) ASC",
            conn,
        )
        conn.close()
        return df
    except Exception as e:
        st.warning(f"DB read: {e}")
        return pd.DataFrame()


def main() -> None:
    st.set_page_config(page_title="Dialectic Edge — Trading", layout="wide")
    st.title("Dialectic Edge — Trading dashboard")
    cfg = load_trading_config()
    mode = cfg.get("mode", "paper")
    st.caption(f"Config mode: **{mode}** (backtest | paper) · DB: `{DB_PATH}`")

    df = load_trades()
    if df.empty:
        st.info("Нет данных в `backtest_signals`. Запусти paper trading или CLI backtest.")
        return

    closed = df[df["status"].str.lower() == "closed"].copy()
    open_ = df[df["status"].str.lower() == "open"].copy()

    c1, c2, c3, c4 = st.columns(4)
    wins = int((closed["pnl"] > 0).sum()) if "pnl" in closed.columns else 0
    losses = int((closed["pnl"] < 0).sum()) if "pnl" in closed.columns else 0
    total_pnl = float(closed["pnl"].sum()) if "pnl" in closed.columns else 0.0

    c1.metric("Open positions", len(open_))
    c2.metric("Closed trades", len(closed))
    c3.metric("Wins / Losses", f"{wins} / {losses}")
    c4.metric("Total PnL (USD)", f"${total_pnl:+,.2f}")

    st.subheader("Equity curve (closed trades, %% PnL compounded)")
    if not closed.empty and "pnl_pct" in closed.columns:
        capital = 100.0
        try:
            conn = sqlite3.connect(DB_PATH)
            cur = conn.execute("SELECT capital FROM backtest_config WHERE id = 1")
            row = cur.fetchone()
            conn.close()
            if row and row[0] is not None:
                capital = float(row[0])
        except Exception:
            pass
        pnls = [float(x) for x in closed["pnl_pct"].tolist()]
        eq = equity_curve_from_pnl_pct(pnls, initial_capital=capital)
        mdd = max_drawdown(eq)
        st.caption(f"Max drawdown (equity): **{mdd * 100:.2f}%**")
        chart = pd.DataFrame({"equity": eq})
        st.line_chart(chart, height=320)
    else:
        st.caption("Нет закрытых сделок с pnl_pct для кривой.")

    st.subheader("Recent signals")
    show = df.sort_values(by="created_at", ascending=False).head(40)
    st.dataframe(show, use_container_width=True)

    st.subheader("CLI last backtest file (optional)")
    p = cli_results_path(cfg)
    if p.exists():
        st.success(str(p))
        try:
            import json

            with open(p, encoding="utf-8") as f:
                payload = json.load(f)
            st.json({"metrics": payload.get("metrics"), "n_results": len(payload.get("results", []))})
        except Exception as e:
            st.warning(str(e))
    else:
        st.caption("Запусти: `python main.py backtest BTC` — появится JSON с метриками.")


if __name__ == "__main__":
    main()
