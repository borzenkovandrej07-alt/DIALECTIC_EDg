"""
COT (Commitments of Traders) Data - Free institutional data from CFTC
Shows futures positioning: Commercials (smart money) vs Large Speculators vs Small Speculators

Source: CFTC.gov - Futures only Commitments of Traders
"""

import asyncio
import aiohttp
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import Optional

COT_URL = "https://www.cftc.gov/files/cotarchives/2026/cot20260327{code}.xml"

ASSET_CODES = {
    "Bitcoin": "133741",  # Bitcoin
    "Gold": "088691",     # Gold
    "Silver": "084691",  # Silver
    "Crude Oil": "067651", # WTI Crude Oil
    "S&P 500": "13874A", # S&P 500 Index
    "US Dollar Index": "098662", # US Dollar Index
    "Euro": "099741",    # Euro FX
}


async def fetch_cot_report(session: aiohttp.ClientSession, asset_code: str) -> Optional[dict]:
    """Fetch COT report for asset."""
    today = datetime.now()
    
    for days_ago in range(7):
        date = today - timedelta(days=days_ago)
        if date.weekday() >= 5:  # Skip weekends
            continue
        
        date_str = date.strftime("%Y%m%d")
        url = f"https://www.cftc.gov/files/cotarchives/2026/cot{date_str}{asset_code}.xml"
        
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    xml = await resp.text()
                    return parse_cot_xml(xml, date.strftime("%d.%m.%Y"))
        except Exception:
            continue
    
    return None


def parse_cot_xml(xml: str, date: str) -> dict:
    """Parse COT XML response."""
    try:
        root = ET.fromstring(xml)
        
        code = root.find(".//CommodityCode")
        name = root.find(".//CommodityName")
        
        if code is None or name is None:
            return None
        
        return {
            "date": date,
            "asset": name.text or code.text,
            "code": code.text,
            "commercials_long": 0,
            "commercials_short": 0,
            "large_speculators_long": 0,
            "large_speculators_short": 0,
            "small_speculators_long": 0,
            "small_speculators_short": 0,
        }
    except Exception:
        return None


async def get_cot_for_assets(assets: list[str] = None) -> dict:
    """Get COT data for multiple assets."""
    if assets is None:
        assets = list(ASSET_CODES.keys())
    
    results = {}
    
    async with aiohttp.ClientSession() as session:
        tasks = []
        for asset in assets:
            code = ASSET_CODES.get(asset)
            if code:
                tasks.append(fetch_cot_report(session, code))
            else:
                tasks.append(asyncio.sleep(0))
        
        results_list = await asyncio.gather(*tasks, return_exceptions=True)
        
        for asset, result in zip(assets, results_list):
            if result and not isinstance(result, Exception):
                results[asset] = result
    
    return results


def format_cot_for_agents(cot_data: dict) -> str:
    """Format COT data for AI agents."""
    if not cot_data:
        return "COT data not available"
    
    lines = ["=== COT ( Commitments of Traders ) ==="]
    
    for asset, data in cot_data.items():
        net_commercials = data.get("commercials_long", 0) - data.get("commercials_short", 0)
        net_large = data.get("large_speculators_long", 0) - data.get("large_speculators_short", 0)
        
        commercial_bias = "NET LONG" if net_commercials > 0 else "NET SHORT" if net_commercials < 0 else "NEUTRAL"
        
        lines.append(f"{asset}: {data.get('date', 'N/A')}")
        lines.append(f"  Commercials: {commercial_bias}")
        lines.append(f"  Large Speculators: {'LONG' if net_large > 0 else 'SHORT' if net_large < 0 else 'NEUTRAL'}")
    
    return "\n".join(lines)


if __name__ == "__main__":
    async def test():
        data = await get_cot_for_assets(["Bitcoin", "Gold"])
        print(format_cot_for_agents(data))
    
    asyncio.run(test())