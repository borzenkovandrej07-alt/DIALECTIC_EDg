"""
data_enricher.py — Элитные данные для анализа.

Собирает данные, которые используют хедж-фонды:
1. Derivatives: Funding Rates, Open Interest, Liquidations (Binance API)
2. On-Chain: Exchange Netflow, MVRV (Glassnode free / альтернативы)
3. Macro: DXY, US10Y, SPX (Yahoo Finance)
4. Sentiment: Fear & Greed Index (Alternative.me)
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

import aiohttp

logger = logging.getLogger(__name__)

# ─── Binance Derivatives ────────────────────────────────────────────────────

BINANCE_FUTURES = "https://fapi.binance.com"
BINANCE_SPOT = "https://api.binance.com"


async def get_funding_rate(symbol: str = "BTCUSDT") -> Optional[float]:
    """Текущая ставка финансирования.
    Positive = лонгисты платят шортистам (бычий настрой/перегрев).
    Negative = шортисты платят лонгистам (медвежий настрой).
    """
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{BINANCE_FUTURES}/fapi/v1/premiumIndex",
                params={"symbol": symbol},
                timeout=aiohttp.ClientTimeout(total=5)
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return float(data.get("lastFundingRate", 0))
    except Exception as e:
        logger.debug(f"Funding rate error: {e}")
    return None


async def get_open_interest(symbol: str = "BTCUSDT") -> Optional[dict]:
    """Открытый интерес и его изменение."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{BINANCE_FUTURES}/fapi/v1/openInterest",
                params={"symbol": symbol},
                timeout=aiohttp.ClientTimeout(total=5)
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return {
                        "oi": float(data.get("openInterest", 0)),
                        "symbol": symbol,
                    }
    except Exception as e:
        logger.debug(f"Open interest error: {e}")
    return None


async def get_recent_liquidations(symbol: str = "BTCUSDT", limit: int = 10) -> list[dict]:
    """Последние крупные ликвидации."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{BINANCE_FUTURES}/fapi/v1/allForceOrders",
                params={"symbol": symbol, "limit": limit},
                timeout=aiohttp.ClientTimeout(total=5)
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return [
                        {
                            "price": float(d["price"]),
                            "qty": float(d["executedQty"]),
                            "side": d["side"],
                            "time": d["time"],
                        }
                        for d in data
                    ]
    except Exception as e:
        logger.debug(f"Liquidations error: {e}")
    return []


# ─── Macro Data ─────────────────────────────────────────────────────────────

YAHOO_CHART = "https://query1.finance.yahoo.com/v8/finance/chart"


async def get_macro_indicators() -> dict:
    """DXY, US10Y, SPX."""
    tickers = {
        "DXY": "DX-Y.NYB",
        "US10Y": "TNX",
        "SPX": "^GSPC",
        "VIX": "^VIX",
    }
    results = {}

    async with aiohttp.ClientSession() as session:
        for name, ticker in tickers.items():
            try:
                url = f"{YAHOO_CHART}/{ticker}"
                params = {"interval": "1d", "range": "2d"}
                headers = {"User-Agent": "Mozilla/5.0"}
                async with session.get(url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        result = data.get("chart", {}).get("result", [{}])[0]
                        meta = result.get("meta", {})
                        price = meta.get("regularMarketPrice")
                        prev = meta.get("previousClose") or meta.get("chartPreviousClose")
                        if price and prev:
                            change = ((price - prev) / prev) * 100
                            results[name] = {"price": price, "change_pct": change}
            except Exception as e:
                logger.debug(f"Macro {name} error: {e}")

    return results


# ─── Fear & Greed ───────────────────────────────────────────────────────────

FG_INDEX = "https://api.alternative.me/fng/?limit=1"


async def get_fear_greed_index() -> Optional[dict]:
    """Индекс страха и жадности."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(FG_INDEX, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    entry = data.get("data", [{}])[0]
                    return {
                        "value": int(entry.get("value", 50)),
                        "classification": entry.get("value_classification", "Neutral"),
                        "timestamp": entry.get("timestamp"),
                    }
    except Exception as e:
        logger.debug(f"Fear & Greed error: {e}")
    return None


# ─── Main Enricher ──────────────────────────────────────────────────────────

async def enrich_context(symbols: list[str] = None) -> dict:
    """
    Собрать все элитные данные в один контекст.
    """
    symbols = symbols or ["BTCUSDT", "ETHUSDT"]
    
    tasks = {
        "macro": get_macro_indicators(),
        "fear_greed": get_fear_greed_index(),
    }
    
    # Добавляем деривативы для каждого символа
    for sym in symbols:
        tasks[f"funding_{sym}"] = get_funding_rate(sym)
        tasks[f"oi_{sym}"] = get_open_interest(sym)
        tasks[f"liq_{sym}"] = get_recent_liquidations(sym, limit=5)

    results = await asyncio.gather(*tasks.values(), return_exceptions=True)
    
    context = {
        "macro": {},
        "fear_greed": {},
        "derivatives": {},
    }

    keys = list(tasks.keys())
    for key, res in zip(keys, results):
        if isinstance(res, Exception):
            continue
        
        if key == "macro":
            context["macro"] = res
        elif key == "fear_greed":
            context["fear_greed"] = res
        elif key.startswith("funding_"):
            sym = key.split("_")[1]
            context["derivatives"].setdefault(sym, {})["funding_rate"] = res
        elif key.startswith("oi_"):
            sym = key.split("_")[1]
            context["derivatives"].setdefault(sym, {})["open_interest"] = res
        elif key.startswith("liq_"):
            sym = key.split("_")[1]
            context["derivatives"].setdefault(sym, {})["liquidations"] = res

    return context


def format_enriched_context(context: dict) -> str:
    """Форматировать данные для вставки в промпт агентов."""
    lines = ["📊 ELITE MARKET DATA"]
    lines.append("=" * 40)

    # Macro
    macro = context.get("macro", {})
    if macro:
        lines.append("\n🌍 MACRO:")
        for name, data in macro.items():
            if data:
                ch = data.get("change_pct", 0)
                lines.append(f"• {name}: {data['price']:.2f} ({ch:+.2f}%)")

    # Fear & Greed
    fg = context.get("fear_greed", {})
    if fg:
        lines.append(f"\n😱 FEAR & GREED: {fg.get('value')} ({fg.get('classification')})")

    # Derivatives
    deriv = context.get("derivatives", {})
    if deriv:
        lines.append("\n📈 DERIVATIVES:")
        for sym, data in deriv.items():
            lines.append(f"\n  {sym}:")
            fr = data.get("funding_rate")
            if fr is not None:
                status = "⚠️ Overheated" if fr > 0.001 else "❄️ Negative" if fr < -0.001 else "✅ Normal"
                lines.append(f"  • Funding: {fr*100:.4f}% {status}")
            
            oi = data.get("open_interest")
            if oi:
                lines.append(f"  • Open Interest: {oi.get('oi', 0):,.0f}")
            
            liqs = data.get("liquidations", [])
            if liqs:
                long_liq = sum(l["qty"] for l in liqs if l["side"] == "BUY")
                short_liq = sum(l["qty"] for l in liqs if l["side"] == "SELL")
                lines.append(f"  • Recent Liqs: Longs {long_liq:.2f} vs Shorts {short_liq:.2f}")

    lines.append("=" * 40)
    return "\n".join(lines)
