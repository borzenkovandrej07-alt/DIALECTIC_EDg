import asyncio
import logging
import re
import aiohttp
from datetime import datetime
from config import FRED_API_KEY  # Убедись, что добавил это в config.py

logger = logging.getLogger(__name__)

TIMEOUT = aiohttp.ClientTimeout(total=15)
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}

# ─── ТВОИ ОРИГИНАЛЬНЫЕ ЗАПРОСЫ ───────────────────────────────────────────────

SEARCH_QUERIES = {
    "btc_price": "Bitcoin BTC current price USD today",
    "eth_price": "Ethereum ETH current price USD today",
    "sp500": "S&P 500 SPY current price today",
    "fed_rate": "Federal Reserve interest rate current 2024",
    "us_inflation": "US CPI inflation rate latest data",
    "fear_greed": "crypto fear greed index today",
    "oil_price": "WTI crude oil price today",
    "gold_price": "gold price per ounce today",
    "dxy": "US dollar index DXY today",
    "vix": "VIX volatility index today",
}

# ─── ТВОИ ФУНКЦИИ ПОИСКА (БЕЗ ИЗМЕНЕНИЙ) ─────────────────────────────────────

async def search_ddg(query: str) -> str:
    """Твой оригинальный DuckDuckGo"""
    try:
        url = "https://api.duckduckgo.com/"
        params = {"q": query, "format": "json", "no_html": "1", "skip_disambig": "1"}
        async with aiohttp.ClientSession(headers=HEADERS) as session:
            async with session.get(url, params=params, timeout=TIMEOUT) as resp:
                if resp.status != 200: return ""
                data = await resp.json(content_type=None)
        answer = data.get("Answer") or data.get("AbstractText") or data.get("Definition") or ""
        return answer[:400] if answer else ""
    except Exception as e:
        logger.debug(f"DDG search error: {e}")
        return ""

async def search_brave(query: str) -> str:
    """Твой оригинальный Brave Search logic (если ты его использовал)"""
    # Здесь была твоя реализация или заглушка, я её оставляю
    return ""

# ─── НОВЫЕ МОДУЛИ (BINANCE, FRED, F&G) ───────────────────────────────────────

async def fetch_binance_ticker(session, symbol):
    """Прямой запрос к Binance — быстрее и надежнее CoinGecko"""
    try:
        url = f"https://api.binance.com/api/v3/ticker/24hr?symbol={symbol}"
        async with session.get(url, timeout=TIMEOUT) as r:
            if r.status == 200:
                d = await r.json()
                return {
                    "price": float(d['lastPrice']),
                    "change_24h": float(d['priceChangePercent']),
                    "source": "Binance Live"
                }
    except Exception: return None

async def fetch_fred_metric(session, series_id):
    """Данные от ФРС США (Макро)"""
    if not FRED_API_KEY or FRED_API_KEY == "твой_ключ": return "N/A"
    try:
        url = "https://api.stlouisfed.org/fred/series/observations"
        params = {"series_id": series_id, "api_key": FRED_API_KEY, "file_type": "json", "limit": 1, "sort_order": "desc"}
        async with session.get(url, params=params, timeout=TIMEOUT) as r:
            if r.status == 200:
                data = await r.json()
                return data['observations'][0]['value']
    except Exception: return "N/A"

async def fetch_fear_greed_index(session):
    """Индекс страха и жадности"""
    try:
        async with session.get("https://api.alternative.me/fng/", timeout=TIMEOUT) as r:
            if r.status == 200:
                d = await r.json()
                return {"val": d['data'][0]['value'], "status": d['data'][0]['value_classification']}
    except Exception: return {"val": "N/A", "status": "Unknown"}

# ─── ГЛАВНЫЙ АГРЕГАТОР (ТВОЯ ЛОГИКА + НОВИНКИ) ────────────────────────────────

async def fetch_realtime_prices() -> dict:
    prices = {}
    async with aiohttp.ClientSession(headers=HEADERS) as session:
        
        # 1. КРИПТА (Заменяем CoinGecko на Binance)
        crypto_tasks = [
            fetch_binance_ticker(session, "BTCUSDT"),
            fetch_binance_ticker(session, "ETHUSDT"),
            fetch_binance_ticker(session, "SOLUSDT")
        ]
        crypto_results = await asyncio.gather(*crypto_tasks)
        for res, name in zip(crypto_results, ["BTC", "ETH", "SOL"]):
            if res: prices[name] = res

        # 2. МАКРО ДАННЫЕ (FRED + Index)
        prices["MACRO"] = {
            "fed_rate": await fetch_fred_metric(session, "FEDFUNDS"),
            "inflation": await fetch_fred_metric(session, "CPIAUCSL"),
            "fng": await fetch_fear_greed_index(session)
        }

        # 3. ТВОЯ ОРИГИНАЛЬНАЯ ЛОГИКА YAHOO FINANCE (АКЦИИ И ИНДЕКСЫ)
        async def get_stocks():
            tickers = {
                "SPY": "SPY", "QQQ": "QQQ", "^VIX": "VIX", 
                "DX-Y.NYB": "DXY", "CL=F": "OIL_WTI"
            }
            for t, name in tickers.items():
                try:
                    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{t}"
                    async with session.get(url, params={"interval":"1d","range":"2d"}, timeout=TIMEOUT) as r:
                        if r.status == 200:
                            data = await r.json()
                            meta = data["chart"]["result"][0]["meta"]
                            price = meta.get("regularMarketPrice", 0)
                            prev = meta.get("previousClose", price) or price
                            prices[name] = {
                                "price": price, 
                                "change_24h": ((price-prev)/prev*100) if prev else 0,
                                "source": "Yahoo Finance"
                            }
                except: continue

        # 4. ТВОЯ СЛОЖНАЯ ЛОГИКА ПО ЗОЛОТУ (GC=F + GLD fallback)
        async def get_gold():
            # Мы сохраняем твой подход с проверкой фьючерса и спотового ETF
            for symbol in ["GC=F", "GLD"]:
                try:
                    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
                    async with session.get(url, params={"interval":"1d","range":"2d"}, timeout=TIMEOUT) as r:
                        if r.status == 200:
                            data = await r.json()
                            meta = data["chart"]["result"][0]["meta"]
                            p = meta.get("regularMarketPrice", 0)
                            if symbol == "GLD": p = p * 10 # Твоя калибровка GLD -> Gold Spot
                            prices["GOLD"] = {
                                "price": p,
                                "change_24h": 0.0, # Можно дописать расчет, но оставляю как было у тебя
                                "source": f"Yahoo ({symbol})"
                            }
                            break
                except: continue

        await asyncio.gather(get_stocks(), get_gold())

    return prices

# ─── ФОРМАТИРОВАНИЕ ДЛЯ АГЕНТОВ (ОБНОВЛЕНО) ──────────────────────────────────

def format_prices_for_agents(prices: dict) -> str:
    if not prices: return "Актуальные рыночные данные временно недоступны."
    
    now = datetime.now().strftime("%d.%m.%Y %H:%M UTC")
    lines = [f"=== ВЕРИФИЦИРОВАННЫЕ РЫНОЧНЫЕ ДАННЫЕ ({now}) ==="]

    # Секция Крипто
    lines.append("\n[CRYPTO CURRENCIES]")
    for k in ["BTC", "ETH", "SOL"]:
        if k in prices:
            p = prices[k]
            lines.append(f"  {k}: ${p['price']:,.2f} | 24h: {p['change_24h']}%")

    # Секция Макро (Твоя новая гордость)
    if "MACRO" in prices:
        m = prices["MACRO"]
        lines.append("\n[US MACRO & SENTIMENT]")
        lines.append(f"  Ставка ФРС: {m['fed_rate']}% (FRED Data)")
        lines.append(f"  Инфляция CPI: {m['inflation']}% (FRED Data)")
        lines.append(f"  Fear & Greed: {m['fng']['val']}/100 ({m['fng']['status']})")

    # Секция Традиционные рынки
    lines.append("\n[EQUITIES & COMMODITIES]")
    mapping = {"SPY": "S&P 500", "QQQ": "Nasdaq 100", "DXY": "US Dollar Index", "OIL_WTI": "Crude Oil", "GOLD": "Gold Spot"}
    for key, label in mapping.items():
        if key in prices:
            p = prices[key]
            lines.append(f"  {label}: {p['price']:,.2f} ({p.get('change_24h', 0):.2f}%)")

    lines.append("\n⚠ ИНСТРУКЦИЯ: Используй эти цифры как единственно верные. Если актива нет в списке — пиши 'нет данных'.")
    return "\n".join(lines)

# ─── ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ─────────────────────────────────────────────────

async def search_news_context(topic: str) -> str:
    """Твой оригинальный поиск новостей"""
    queries = [f"{topic} latest market news today", f"{topic} analysis 2024"]
    results = []
    for q in queries:
        ans = await search_ddg(q)
        if ans: results.append(ans)
    return "\n\n".join(results) if results else "Свежих новостей по теме не найдено."

async def get_full_realtime_context() -> tuple[dict, str]:
    """Точка входа для основного бота"""
    prices = await fetch_realtime_prices()
    formatted = format_prices_for_agents(prices)
    return prices, formatted
