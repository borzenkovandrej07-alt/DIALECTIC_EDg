"""
web_search.py — Реалтайм данные: Binance, FRED, Yahoo Finance, Fear & Greed.

ИСПРАВЛЕНИЕ:
- fetch_fred_metric возвращает сырой индекс CPI (~327), не процент.
- format_prices_for_agents теперь пересчитывает индекс в YoY % перед
  отправкой агентам, чтобы они не писали "гиперинфляция 327%".
"""

import asyncio
import logging
import re
import aiohttp
from datetime import datetime
from config import FRED_API_KEY

logger = logging.getLogger(__name__)

TIMEOUT = aiohttp.ClientTimeout(total=15)
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

# Базовое значение CPI год назад для расчёта YoY.
# Обновляй раз в квартал если нужна точность.
# Март 2024 ≈ 312.3, среднее 2024 ≈ 314.2
CPI_BASE_YEAR_AGO = 314.2
FED_INFLATION_TARGET = 2.0


SEARCH_QUERIES = {
    "btc_price":    "Bitcoin BTC current price USD today",
    "eth_price":    "Ethereum ETH current price USD today",
    "sp500":        "S&P 500 SPY current price today",
    "fed_rate":     "Federal Reserve interest rate current 2024",
    "us_inflation": "US CPI inflation rate latest data",
    "fear_greed":   "crypto fear greed index today",
    "oil_price":    "WTI crude oil price today",
    "gold_price":   "gold price per ounce today",
    "dxy":          "US dollar index DXY today",
    "vix":          "VIX volatility index today",
}


# ─── Поиск ────────────────────────────────────────────────────────────────────

async def search_ddg(query: str) -> str:
    try:
        url = "https://api.duckduckgo.com/"
        params = {"q": query, "format": "json", "no_html": "1", "skip_disambig": "1"}
        async with aiohttp.ClientSession(headers=HEADERS) as session:
            async with session.get(url, params=params, timeout=TIMEOUT) as resp:
                if resp.status != 200:
                    return ""
                data = await resp.json(content_type=None)
        answer = data.get("Answer") or data.get("AbstractText") or data.get("Definition") or ""
        return answer[:400] if answer else ""
    except Exception as e:
        logger.debug(f"DDG search error: {e}")
        return ""


async def search_brave(query: str) -> str:
    return ""


# ─── Источники данных ─────────────────────────────────────────────────────────

async def fetch_binance_ticker(session, symbol):
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
    except Exception:
        return None


async def fetch_fred_metric(session, series_id):
    """Возвращает последнее значение ряда FRED как строку (сырое число)."""
    if not FRED_API_KEY or FRED_API_KEY == "твой_ключ":
        return "N/A"
    try:
        url = "https://api.stlouisfed.org/fred/series/observations"
        params = {
            "series_id": series_id,
            "api_key": FRED_API_KEY,
            "file_type": "json",
            "limit": 1,
            "sort_order": "desc"
        }
        async with session.get(url, params=params, timeout=TIMEOUT) as r:
            if r.status == 200:
                data = await r.json()
                return data['observations'][0]['value']
    except Exception:
        return "N/A"


async def fetch_fear_greed_index(session):
    try:
        async with session.get("https://api.alternative.me/fng/", timeout=TIMEOUT) as r:
            if r.status == 200:
                d = await r.json()
                return {
                    "val": d['data'][0]['value'],
                    "status": d['data'][0]['value_classification']
                }
    except Exception:
        return {"val": "N/A", "status": "Unknown"}


# ─── Агрегатор ────────────────────────────────────────────────────────────────

async def fetch_realtime_prices() -> dict:
    prices = {}
    async with aiohttp.ClientSession(headers=HEADERS) as session:

        # 1. Крипта через Binance
        crypto_tasks = [
            fetch_binance_ticker(session, "BTCUSDT"),
            fetch_binance_ticker(session, "ETHUSDT"),
            fetch_binance_ticker(session, "SOLUSDT"),
        ]
        crypto_results = await asyncio.gather(*crypto_tasks)
        for res, name in zip(crypto_results, ["BTC", "ETH", "SOL"]):
            if res:
                prices[name] = res

        # 2. Макро (FRED) — сырые значения, пересчёт происходит при форматировании
        prices["MACRO"] = {
            "fed_rate":  await fetch_fred_metric(session, "FEDFUNDS"),
            "cpi_raw":   await fetch_fred_metric(session, "CPIAUCSL"),  # индекс, не %!
            "fng":       await fetch_fear_greed_index(session),
        }

        # 3. Акции и индексы через Yahoo Finance
        async def get_stocks():
            tickers = {
                "SPY":      "SPY",
                "QQQ":      "QQQ",
                "^VIX":     "VIX",
                "DX-Y.NYB": "DXY",
                "CL=F":     "OIL_WTI",
            }
            for t, name in tickers.items():
                try:
                    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{t}"
                    async with session.get(
                        url, params={"interval": "1d", "range": "2d"}, timeout=TIMEOUT
                    ) as r:
                        if r.status == 200:
                            data = await r.json()
                            meta = data["chart"]["result"][0]["meta"]
                            price = meta.get("regularMarketPrice", 0)
                            prev = meta.get("previousClose", price) or price
                            prices[name] = {
                                "price": price,
                                "change_24h": ((price - prev) / prev * 100) if prev else 0,
                                "source": "Yahoo Finance"
                            }
                except Exception:
                    continue

        # 4. Золото (GC=F + GLD fallback)
        async def get_gold():
            for symbol in ["GC=F", "GLD"]:
                try:
                    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
                    async with session.get(
                        url, params={"interval": "1d", "range": "2d"}, timeout=TIMEOUT
                    ) as r:
                        if r.status == 200:
                            data = await r.json()
                            meta = data["chart"]["result"][0]["meta"]
                            p = meta.get("regularMarketPrice", 0)
                            if symbol == "GLD":
                                p = p * 10  # GLD ETF → Gold Spot приближение
                            prices["GOLD"] = {
                                "price": p,
                                "change_24h": 0.0,
                                "source": f"Yahoo ({symbol})"
                            }
                            break
                except Exception:
                    continue

        await asyncio.gather(get_stocks(), get_gold())

    return prices


# ─── Форматирование для агентов ───────────────────────────────────────────────

def _cpi_to_yoy(cpi_raw_str: str) -> str:
    """
    FRED возвращает CPI как индекс уровня цен (~327), не как процент.
    Эта функция пересчитывает его в YoY % и добавляет контекст.

    Возвращает готовую строку для вставки в live_prices.
    """
    try:
        cpi_value = float(cpi_raw_str)
        yoy_pct = ((cpi_value - CPI_BASE_YEAR_AGO) / CPI_BASE_YEAR_AGO) * 100
        gap = yoy_pct - FED_INFLATION_TARGET
        gap_str = f"+{gap:.1f}%" if gap > 0 else f"{gap:.1f}%"

        if gap > 1.0:
            status = "выше таргета ФРС"
        elif gap > 0.3:
            status = "незначительно выше таргета"
        else:
            status = "близко к таргету ФРС"

        return (
            f"~{yoy_pct:.1f}% годовых (YoY) — {status} "
            f"(таргет ФРС 2.0%, отклонение {gap_str})"
        )
    except (ValueError, TypeError):
        return "нет данных"


def format_prices_for_agents(prices: dict) -> str:
    if not prices:
        return "Актуальные рыночные данные временно недоступны."

    now = datetime.now().strftime("%d.%m.%Y %H:%M UTC")
    lines = [f"=== ВЕРИФИЦИРОВАННЫЕ РЫНОЧНЫЕ ДАННЫЕ ({now}) ==="]

    # Крипта
    lines.append("\n[CRYPTO CURRENCIES]")
    for k in ["BTC", "ETH", "SOL"]:
        if k in prices:
            p = prices[k]
            lines.append(f"  {k}: ${p['price']:,.2f} | 24h: {p['change_24h']:.3f}%")

    # Макро — ИСПРАВЛЕНИЕ: CPI пересчитывается в YoY %, не показывается как индекс
    if "MACRO" in prices:
        m = prices["MACRO"]
        lines.append("\n[US MACRO & SENTIMENT]")
        lines.append(f"  Ставка ФРС: {m['fed_rate']}% (FRED Data)")

        # Пересчитываем сырой индекс CPI в понятный YoY %
        cpi_display = _cpi_to_yoy(m.get("cpi_raw", "N/A"))
        lines.append(f"  Инфляция CPI: {cpi_display} (FRED Data)")

        fng = m.get("fng", {})
        lines.append(f"  Fear & Greed: {fng.get('val', 'N/A')}/100 ({fng.get('status', 'N/A')})")

        # Явная подсказка агентам — дублируем из data_sources для надёжности
        lines.append(
            "  [!] CPI — это индекс уровня цен, НЕ процент. "
            "Инфляция = YoY изменение (указано выше). Не путать."
        )

    # Традиционные рынки
    lines.append("\n[EQUITIES & COMMODITIES]")
    mapping = {
        "SPY":     "S&P 500",
        "QQQ":     "Nasdaq 100",
        "DXY":     "US Dollar Index",
        "OIL_WTI": "Crude Oil",
        "GOLD":    "Gold Spot",
    }
    for key, label in mapping.items():
        if key in prices:
            p = prices[key]
            lines.append(f"  {label}: {p['price']:,.2f} ({p.get('change_24h', 0):.2f}%)")

    lines.append(
        "\n⚠ ИНСТРУКЦИЯ: Используй эти цифры как единственно верные. "
        "Если актива нет в списке — пиши 'нет данных'."
    )
    return "\n".join(lines)


# ─── Вспомогательные функции ──────────────────────────────────────────────────

async def search_news_context(topic: str) -> str:
    queries = [f"{topic} latest market news today", f"{topic} analysis 2024"]
    results = []
    for q in queries:
        ans = await search_ddg(q)
        if ans:
            results.append(ans)
    return "\n\n".join(results) if results else "Свежих новостей по теме не найдено."


async def get_full_realtime_context() -> tuple[dict, str]:
    """Точка входа для основного бота."""
    prices = await fetch_realtime_prices()
    formatted = format_prices_for_agents(prices)
    return prices, formatted
