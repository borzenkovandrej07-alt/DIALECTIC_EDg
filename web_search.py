"""
web_search.py — Реалтайм данные + реальный веб-поиск через Tavily.

Источники:
  BTC/ETH/SOL   → Binance + CoinGecko fallback
  S&P 500        → Yahoo ^GSPC (индекс, не ETF SPY)
  Nasdaq 100     → Yahoo ^NDX  (индекс, не ETF QQQ)
  VIX            → Yahoo ^VIX
  DXY            → Yahoo DX-Y.NYB
  Нефть WTI      → Yahoo CL=F + fallback BNO
  Золото         → Yahoo GC=F + XAUUSD=X fallback
  Макро          → FRED API (CPI пересчитывается в YoY %)
  Веб-новости    → Tavily API (реальный поиск, не DDG-пустышка)
  COT           → CFTC.gov (фьючерсное позиционирование)
  ETF Flows     → Yahoo (институциональные потоки)
"""

import asyncio
import logging
import os
import aiohttp
from datetime import datetime
from config import FRED_API_KEY

logger = logging.getLogger(__name__)

TIMEOUT       = aiohttp.ClientTimeout(total=15)
HEADERS       = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
TAVILY_API_KEY = os.getenv("TAVILY_API_KEY", "")

# ─── Константы ────────────────────────────────────────────────────────────────
CPI_BASE_YEAR_AGO    = 319.8   # CPI март 2025 по BLS → YoY ≈ 2.4%
FED_INFLATION_TARGET = 2.0

PRICE_SANITY = {
    "BTC":     (20_000,  250_000),
    "ETH":     (500,     30_000),
    "SOL":     (10,      3_000),
    "SPX":     (4_000,   12_000),
    "NDX":     (10_000,  35_000),
    "VIX":     (5,       90),
    "DXY":     (80,      130),
    "OIL_WTI": (30,      200),
    "GOLD":    (2_000,   8_000),
}

def _sane(key: str, price: float) -> bool:
    if not price or price <= 0:
        return False
    lo, hi = PRICE_SANITY.get(key, (0, 999_999_999))
    ok = lo <= price <= hi
    if not ok:
        logger.warning(f"[sanity] {key}: {price:.2f} вне [{lo}, {hi}]")
    return ok


# ─── Tavily — реальный веб-поиск ──────────────────────────────────────────────

async def search_tavily(query: str, max_results: int = 3) -> str:
    """
    Реальный поиск через Tavily API.
    Возвращает сниппеты новостей/аналитики для агентов.
    """
    if not TAVILY_API_KEY:
        return ""
    try:
        url = "https://api.tavily.com/search"
        payload = {
            "api_key":      TAVILY_API_KEY,
            "query":        query,
            "max_results":  max_results,
            "search_depth": "basic",
            "include_answer": True,
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload,
                                    timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status == 200:
                    data = await r.json()
                    parts = []
                    # Если есть прямой ответ — берём его
                    if data.get("answer"):
                        parts.append(f"[Ответ]: {data['answer'][:300]}")
                    # Плюс топ-3 результата
                    for res in data.get("results", [])[:max_results]:
                        title   = res.get("title", "")[:80]
                        content = res.get("content", "")[:200]
                        url_s   = res.get("url", "")
                        parts.append(f"• {title}\n  {content}\n  ({url_s})")
                    return "\n\n".join(parts)
    except Exception as e:
        logger.debug(f"Tavily '{query}': {e}")
    return ""


async def get_news_context(topics: list[str]) -> str:
    """
    Собирает свежие новости по списку тем через Tavily.
    Используется в run_full_analysis для обогащения контекста агентов.
    """
    if not TAVILY_API_KEY:
        return "⚠️ Tavily API не настроен — веб-поиск недоступен."

    queries = [
        "Fed interest rates inflation latest news today",
        "geopolitical risk oil markets today",
        "Bitcoin crypto market news today",
        "S&P 500 stock market outlook today",
    ]
    # Добавляем пользовательские темы
    for t in topics:
        if t:
            queries.append(f"{t} financial market impact today")

    tasks   = [search_tavily(q, max_results=2) for q in queries[:6]]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    sections = []
    for q, res in zip(queries, results):
        if isinstance(res, str) and res.strip():
            sections.append(f"=== {q} ===\n{res}")

    if not sections:
        return "Свежих новостей не найдено."

    return "=== АКТУАЛЬНЫЕ НОВОСТИ (Tavily) ===\n\n" + "\n\n".join(sections)


async def search_news_context(topic: str) -> str:
    """Для /analyze — поиск по конкретной теме."""
    result = await search_tavily(f"{topic} market financial impact analysis", max_results=4)
    return result if result else "Свежих новостей по теме не найдено."


# ─── Binance ──────────────────────────────────────────────────────────────────

async def _binance(session, symbol: str, key: str) -> dict | None:
    try:
        url = f"https://api.binance.com/api/v3/ticker/24hr?symbol={symbol}"
        async with session.get(url, timeout=TIMEOUT) as r:
            if r.status == 200:
                d      = await r.json()
                price  = float(d["lastPrice"])
                change = float(d["priceChangePercent"])
                if _sane(key, price):
                    return {"price": price, "change_24h": round(change, 3), "source": "Binance"}
    except Exception as e:
        logger.debug(f"Binance {symbol}: {e}")
    return None


async def _coingecko_crypto(session) -> dict:
    out = {}
    try:
        url    = "https://api.coingecko.com/api/v3/simple/price"
        params = {"ids": "bitcoin,ethereum,solana", "vs_currencies": "usd",
                  "include_24hr_change": "true"}
        async with session.get(url, params=params, timeout=TIMEOUT) as r:
            if r.status == 200:
                data = await r.json()
                for cg_id, key in [("bitcoin","BTC"),("ethereum","ETH"),("solana","SOL")]:
                    if cg_id in data:
                        p = float(data[cg_id].get("usd", 0))
                        c = float(data[cg_id].get("usd_24h_change", 0))
                        if _sane(key, p):
                            out[key] = {"price": p, "change_24h": round(c, 3),
                                        "source": "CoinGecko"}
    except Exception as e:
        logger.debug(f"CoinGecko crypto: {e}")
    return out


# ─── Yahoo Finance ────────────────────────────────────────────────────────────

async def _yahoo(session, ticker: str, key: str) -> dict | None:
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
        async with session.get(url, params={"interval": "1d", "range": "5d"},
                               timeout=TIMEOUT) as r:
            if r.status == 200:
                data   = await r.json()
                result = data["chart"]["result"][0]
                meta   = result["meta"]
                price  = float(meta.get("regularMarketPrice", 0))
                
                change = 0.0
                if result.get("indicators", {}).get("quote"):
                    quotes = result["indicators"]["quote"][0]
                    closes = quotes.get("close", [])
                    if len(closes) >= 2 and closes[-1] is not None and closes[-2] is not None:
                        prev_price = closes[-2]
                        change = ((price - prev_price) / prev_price * 100) if prev_price else 0.0
                    elif len(closes) >= 1 and closes[-1] is not None:
                        prev = float(meta.get("previousClose", price) or price)
                        if prev != price:
                            change = ((price - prev) / prev * 100) if prev else 0.0
                
                if _sane(key, price):
                    return {"price": round(price, 2),
                            "change_24h": round(change, 3),
                            "source": f"Yahoo ({ticker})"}
    except Exception as e:
        logger.debug(f"Yahoo {ticker}: {e}")
    return None


async def _gold(session) -> dict | None:
    """GC=F → XAUUSD=X → предупреждение."""
    for ticker in ["GC=F", "XAUUSD=X"]:
        r = await _yahoo(session, ticker, "GOLD")
        if r:
            logger.info(f"Золото {ticker}: ${r['price']:,.2f}")
            return r
    logger.warning("Золото: все источники недоступны")
    return None


async def _oil(session) -> dict | None:
    """CL=F → BNO → предупреждение."""
    for ticker, key in [("CL=F", "OIL_WTI"), ("BNO", "OIL_WTI")]:
        r = await _yahoo(session, ticker, key)
        if r:
            return r
    return None


# ─── FRED ─────────────────────────────────────────────────────────────────────

async def _fred(session, series_id: str) -> str:
    if not FRED_API_KEY or FRED_API_KEY in ("", "твой_ключ", "YOUR_KEY"):
        return "N/A"
    try:
        url    = "https://api.stlouisfed.org/fred/series/observations"
        params = {"series_id": series_id, "api_key": FRED_API_KEY,
                  "file_type": "json", "limit": 1, "sort_order": "desc"}
        async with session.get(url, params=params, timeout=TIMEOUT) as r:
            if r.status == 200:
                data = await r.json()
                val  = data["observations"][0]["value"]
                return val if val != "." else "N/A"
    except Exception as e:
        logger.debug(f"FRED {series_id}: {e}")
    return "N/A"


async def _fear_greed(session) -> dict:
    try:
        async with session.get("https://api.alternative.me/fng/?limit=2",
                               timeout=TIMEOUT) as r:
            if r.status == 200:
                d     = await r.json()
                items = d.get("data", [])
                if items:
                    cur  = items[0]
                    prev = int(items[1]["value"]) if len(items) > 1 else int(cur["value"])
                    val  = int(cur["value"])
                    return {"val": val, "status": cur["value_classification"],
                            "change": val - prev}
    except Exception as e:
        logger.debug(f"F&G: {e}")
    return {"val": "N/A", "status": "Unknown", "change": 0}


# ─── Агрегатор ────────────────────────────────────────────────────────────────

async def fetch_realtime_prices() -> dict:
    prices = {}
    async with aiohttp.ClientSession(headers=HEADERS) as session:
        (btc, eth, sol,
         spx, ndx, vix, dxy, oil, gold,
         fed_rate, cpi_raw, fng) = await asyncio.gather(
            _binance(session, "BTCUSDT", "BTC"),
            _binance(session, "ETHUSDT", "ETH"),
            _binance(session, "SOLUSDT", "SOL"),
            _yahoo(session, "^GSPC",    "SPX"),   # S&P 500 индекс
            _yahoo(session, "^NDX",     "NDX"),   # Nasdaq 100 индекс
            _yahoo(session, "^VIX",     "VIX"),
            _yahoo(session, "DX-Y.NYB", "DXY"),
            _oil(session),
            _gold(session),
            _fred(session, "FEDFUNDS"),
            _fred(session, "CPIAUCSL"),
            _fear_greed(session),
            return_exceptions=True,
        )

        # Крипта с fallback
        missing = []
        for key, val in [("BTC", btc), ("ETH", eth), ("SOL", sol)]:
            if val and not isinstance(val, Exception):
                prices[key] = val
            else:
                missing.append(key)
        if missing:
            cg = await _coingecko_crypto(session)
            for k in missing:
                if k in cg:
                    prices[k] = cg[k]

        for key, val in [("SPX", spx), ("NDX", ndx), ("VIX", vix),
                         ("DXY", dxy), ("OIL_WTI", oil), ("GOLD", gold)]:
            if val and not isinstance(val, Exception):
                prices[key] = val

        prices["MACRO"] = {
            "fed_rate": fed_rate if not isinstance(fed_rate, Exception) else "N/A",
            "cpi_raw":  cpi_raw  if not isinstance(cpi_raw, Exception)  else "N/A",
            "fng":      fng      if not isinstance(fng, Exception) else
                        {"val": "N/A", "status": "Unknown", "change": 0},
        }

    got     = [k for k in prices if k != "MACRO"]
    missing = [k for k in ["BTC","ETH","SPX","NDX","GOLD","OIL_WTI"] if k not in prices]
    logger.info(f"✅ Цены: {got}")
    if missing:
        logger.warning(f"❌ Не получены: {missing}")
    return prices


# ─── CPI → YoY % ──────────────────────────────────────────────────────────────

def _cpi_yoy(raw: str) -> str:
    try:
        v   = float(raw)
        yoy = (v - CPI_BASE_YEAR_AGO) / CPI_BASE_YEAR_AGO * 100
        gap = yoy - FED_INFLATION_TARGET
        g_s = f"+{gap:.1f}%" if gap > 0 else f"{gap:.1f}%"
        status = ("выше таргета" if gap > 1.0 else
                  "незначительно выше таргета" if gap > 0.3 else
                  "близко к таргету")
        return f"~{yoy:.1f}% YoY — {status} (таргет 2.0%, отклонение {g_s})"
    except (ValueError, TypeError):
        return "нет данных"


# ─── Форматирование для агентов ───────────────────────────────────────────────

def format_prices_for_agents(prices: dict) -> str:
    if not prices:
        return "Рыночные данные временно недоступны."

    now   = datetime.now().strftime("%d.%m.%Y %H:%M UTC")
    lines = [f"=== ВЕРИФИЦИРОВАННЫЕ РЫНОЧНЫЕ ДАННЫЕ ({now}) ==="]

    lines.append("\n[КРИПТОРЫНОК]")
    for k, label in [("BTC","Bitcoin"),("ETH","Ethereum"),("SOL","Solana")]:
        if k in prices:
            p  = prices[k]
            ch = p["change_24h"]
            lines.append(f"  {label} ({k}): ${p['price']:,.2f}  "
                         f"{'🟢' if ch>=0 else '🔴'} {ch:+.2f}%  [{p['source']}]")

    if "MACRO" in prices:
        m   = prices["MACRO"]
        fng = m.get("fng", {})
        fv, fs = fng.get("val","N/A"), fng.get("status","")
        fc = fng.get("change", 0)
        lines.append("\n[МАКРОЭКОНОМИКА США]")
        lines.append(f"  Ставка ФРС:   {m['fed_rate']}%  [FRED]")
        lines.append(f"  Инфляция CPI: {_cpi_yoy(m.get('cpi_raw','N/A'))}  [FRED]")
        lines.append(
            f"  Fear & Greed: {fv}/100 ({fs})  "
            f"{'🟢' if fc > 0 else '🔴' if fc < 0 else '➡️'} {fc:+d} за сутки  "
            f"[Источник: Alternative.me Crypto F&G — НЕ FRED]"
        )
        lines.append("  [!] CPI = индекс (~323), НЕ %. Инфляция = YoY (выше).")

    lines.append("\n[ФОНДОВЫЕ ИНДЕКСЫ]")
    for k, label in [("SPX","S&P 500"),("NDX","Nasdaq 100"),("VIX","VIX")]:
        if k in prices:
            p  = prices[k]
            ch = p["change_24h"]
            lines.append(f"  {label}: {p['price']:,.2f}  "
                         f"{'🟢' if ch>=0 else '🔴'} {ch:+.2f}%  [{p['source']}]")

    lines.append("\n[СЫРЬЁ И ВАЛЮТЫ]")
    for k, label, unit in [("OIL_WTI","Нефть WTI","$/барр"),
                            ("GOLD","Золото","$/унц"),
                            ("DXY","Индекс доллара","")]:
        if k in prices:
            p  = prices[k]
            ch = p["change_24h"]
            u  = f" {unit}" if unit else ""
            lines.append(f"  {label}: {p['price']:,.2f}{u}  "
                         f"{'🟢' if ch>=0 else '🔴'} {ch:+.2f}%  [{p['source']}]")

    lines.append("\n⚠️ ИНСТРУКЦИЯ: используй ТОЛЬКО эти цифры. "
                 "Если актива нет — пиши 'нет данных'.")
    return "\n".join(lines)


async def get_full_realtime_context() -> tuple[dict, str]:
    prices    = await fetch_realtime_prices()
    formatted = format_prices_for_agents(prices)
    
    try:
        from cot_data import format_cot_for_agents, get_cot_for_assets
        cot_data = await get_cot_for_assets(["Bitcoin", "Gold", "Crude Oil"])
        cot_formatted = format_cot_for_agents(cot_data)
        formatted += "\n\n" + cot_formatted
    except Exception as e:
        logger.warning(f"COT data error: {e}")
    
    try:
        from etf_flows import format_etf_flows_for_agents, get_market_breadth, get_etf_flows
        etf_data = await get_etf_flows()
        etf_formatted = format_etf_flows_for_agents(etf_data)
        formatted += "\n\n" + etf_formatted
        
        breadth = await get_market_breadth()
        prices["BREADTH"] = breadth
    except Exception as e:
        logger.warning(f"ETF flows error: {e}")
    
    return prices, formatted
