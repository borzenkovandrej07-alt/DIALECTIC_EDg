"""
streamlit_app.py — Дашборд Dialectic Edge.

Запуск: streamlit run streamlit_app.py
"""

import streamlit as st
import pandas as pd
import json
import os
from datetime import datetime

st.set_page_config(page_title="Dialectic Edge Dashboard", layout="wide")

st.title("🧠 Dialectic Edge — Trading Dashboard")
st.caption(f"Updated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")

# ─── Sidebar ─────────────────────────────────────────────────────────────────
st.sidebar.header("⚙️ Settings")
show_raw = st.sidebar.checkbox("Show Raw Data", value=False)

# ─── Load Data ───────────────────────────────────────────────────────────────
def load_json(path):
    if os.path.exists(path):
        with open(path, "r") as f:
            return json.load(f)
    return None

results = load_json("results.json")
backtest = load_json("config/trading_config.json")

# ─── Metrics Row ─────────────────────────────────────────────────────────────
if results and "metrics" in results:
    m = results["metrics"]
    
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Total PnL", f"{m.get('total_pnl_pct', 0):+.2f}%")
    col2.metric("Winrate", f"{m.get('winrate', 0):.1f}%")
    col3.metric("Profit Factor", f"{m.get('profit_factor', 0):.2f}")
    col4.metric("Trades", m.get('total_signals', 0))
    col5.metric("Best Trade", f"{m.get('best_trade_pct', 0):+.2f}%")

    # ─── Results Table ───────────────────────────────────────────────────────
    st.subheader("📋 Trade History")
    if results.get("results"):
        df = pd.DataFrame(results["results"])
        if not df.empty:
            # Color code
            def color_result(val):
                if val == "win": return "color: green"
                elif val == "loss": return "color: red"
                return "color: orange"
            
            st.dataframe(
                df.style.applymap(color_result, subset=["result"]),
                use_container_width=True,
            )
        else:
            st.info("No trades yet.")
    else:
        st.info("No results data available.")

    # ─── By Asset ────────────────────────────────────────────────────────────
    if m.get("by_asset"):
        st.subheader("📊 Performance by Asset")
        asset_df = pd.DataFrame(m["by_asset"]).T
        st.dataframe(asset_df, use_container_width=True)

else:
    st.info("📂 No results.json found. Run `/eval` or `python -m pipeline` first.")

# ─── Regime & Risk ───────────────────────────────────────────────────────────
st.subheader("🌍 System Status")
c1, c2 = st.columns(2)

with c1:
    st.markdown("""
    **Modules Active:**
    - ✅ Regime Detector
    - ✅ Dynamic Risk Manager
    - ✅ Multi-Timeframe Analyzer
    - ✅ Data Enricher
    - ✅ Whale Detector
    - ✅ Correlation Matrix
    - ✅ Event Defense
    """)

with c2:
    st.markdown("""
    **Data Sources:**
    - 📊 Binance (Prices, OHLCV, Funding, OI)
    - 📈 Yahoo Finance (Macro)
    - 😱 Alternative.me (Fear & Greed)
    - 📰 News APIs (Sentiment)
    """)

# ─── Raw Data ────────────────────────────────────────────────────────────────
if show_raw and results:
    st.subheader("🔍 Raw Data")
    st.json(results)
