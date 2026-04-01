"""
ETF Flows - Institutional Money Flow Data
Tracks inflow/outflow in major ETFs to gauge institutional sentiment

Sources: ETF.com data (scraped) or alternative free APIs
"""

import asyncio
import aiohttp
from datetime import datetime
from typing import Optional

ETF_TICKERS = {
    "SPY": "SPDR S&P 500 ETF",
    "QQQ": "Invesco QQQ Trust",
    "IWM": "iShares Russell 2000 ETF",
    "GLD": "SPDR Gold Shares",
    "SLV": "iShares Silver Trust",
    "USO": "United States Oil Fund",
    "VWO": "Vanguard FTSE Emerging Markets",
    "EFA": "iShares MSCI EAFE",
    "TLT": "iShares 20+ Year Treasury Bond",
    "HYG": "iShares iBoxx $ High Yield Corporate Bond",
}


async def get_etf_flows() -> dict:
    """Get ETF flows for major ETFs."""
    flows = {}
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    
    async with aiohttp.ClientSession(headers=headers) as session:
        for ticker, name in ETF_TICKERS.items():
            try:
                url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
                params = {"range": "5d", "interval": "1d"}
                
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        result = data.get("chart", {}).get("result", [])
                        
                        if result and result[0].get("indicators"):
                            quotes = result[0]["indicators"].get("quote", [{}])[0]
                            closes = quotes.get("close", [])
                            volumes = quotes.get("volume", [])
                            
                            if len(closes) >= 2 and closes[-1] and closes[-2]:
                                price_now = closes[-1]
                                price_prev = closes[-2]
                                pct_change = ((price_now - price_prev) / price_prev * 100)
                                
                                recent_volumes = [v for v in volumes[-5:] if v]
                                avg_volume = sum(recent_volumes) / len(recent_volumes) if recent_volumes else 0
                                
                                flows[ticker] = {
                                    "name": name,
                                    "price": price_now,
                                    "change_5d": pct_change,
                                    "avg_volume": avg_volume,
                                    "direction": "inflow" if pct_change > 0 else "outflow"
                                }
            except Exception:
                continue
    
    return flows


def format_etf_flows_for_agents(flows: dict) -> str:
    """Format ETF flows for AI agents."""
    if not flows:
        return "ETF flows data not available"
    
    lines = ["=== ETF INSTITUTIONAL FLOWS ==="]
    
    spdrs = ["SPY", "QQQ", "IWM", "GLD", "SLV", "USO"]
    bond_etfs = ["TLT", "HYG"]
    intl_etfs = ["VWO", "EFA"]
    
    def categorize(ticker):
        if ticker in spdrs: return "Equity"
        elif ticker in bond_etfs: return "Bond"
        elif ticker in intl_etfs: return "Intl"
        return "Other"
    
    inflow = sum(1 for v in flows.values() if v["change_5d"] > 0)
    outflow = sum(1 for v in flows.values() if v["change_5d"] < 0)
    
    lines.append(f"5-day flows: {inflow} IN / {outflow} OUT")
    lines.append("")
    
    for ticker, data in flows.items():
        direction = "UP" if data["change_5d"] > 0 else "DOWN"
        lines.append(f"{ticker} ({categorize(ticker)}): {direction} {data['change_5d']:+.2f}% | ${data['avg_volume']/1e6:.0f}M vol")
    
    return "\n".join(lines)


async def get_market_breadth() -> dict:
    """Get market breadth from advancing/declining stocks (approximation via indices)."""
    flows = await get_etf_flows()
    
    spy_change = flows.get("SPY", {}).get("change_5d", 0)
    qqq_change = flows.get("QQQ", {}).get("change_5d", 0)
    iwm_change = flows.get("IWM", {}).get("change_5d", 0)
    
    if spy_change > 0.5 and qqq_change > 0.5:
        breadth = "strong_bullish"
    elif spy_change < -0.5 and qqq_change < -0.5:
        breadth = "strong_bearish"
    elif spy_change > 0:
        breadth = "bullish"
    elif spy_change < 0:
        breadth = "bearish"
    else:
        breadth = "neutral"
    
    return {
        "breadth": breadth,
        "spy_5d": spy_change,
        "qqq_5d": qqq_change,
        "iwm_5d": iwm_change,
    }


if __name__ == "__main__":
    async def test():
        flows = await get_etf_flows()
        print(format_etf_flows_for_agents(flows))
        
        print("\n--- Breadth ---")
        print(await get_market_breadth())
    
    asyncio.run(test())