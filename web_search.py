"""
web_search.py — Реальный веб-поиск для агентов.

Агенты больше не угадывают цифры — они их ищут.
Использует Gemini с Google Search grounding (бесплатно).
Fallback: DuckDuckGo instant answers (без ключа).
"""

import asyncio
import logging
import re
import aiohttp
from datetime import datetime

logger = logging.getLogger(__name__)
TIMEOUT = aiohttp.ClientTimeout(total=15)
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; DialecticEdge/3.0)"}


# ─── Быстрые запросы для верификации цифр ─────────────────────────────────────

SEARCH_QUERIES = {
    "btc_price":     "Bitcoin BTC current price USD today",
    "eth_price":     "Ethereum ETH current price USD today",
    "sp500":         "S&P 500 SPY current price today",
    "fed_rate":      "Federal Reserve interest rate current 2024",
    "us_inflation":  "US CPI inflation rate latest data",
    "fear_greed":    "crypto fear greed index today",
    "oil_price":     "WTI crude oil price today",
    "gold_price":    "gold price per ounce today",
    "dxy":           "US dollar index DXY today",
    "vix":           "VIX volatility index today",
}


async def search_ddg(query: str) -> str:
    """
    DuckDuckGo Instant Answer API — бесплатно, без ключа.
    Возвращает краткий ответ на конкретный вопрос.
    """
    try:
        url = "https://api.duckduckgo.com/"
        params = {
            "q": query,
            "format": "json",
            "no_html": "1",
            "skip_disambig": "1",
        }
        async with aiohttp.ClientSession(headers=HEADERS) as session:
            async with session.get(url, params=params, timeout=TIMEOUT) as resp:
                if resp.status != 200:
                    return ""
                data = await resp.json(content_type=None)

        # Пробуем разные поля ответа
        answer = (
            data.get("Answer") or
            data.get("AbstractText") or
            data.get("Definition") or
            ""
        )
        return answer[:300] if answer else ""

    except Exception as e:
        logger.debug(f"DDG search error: {e}")
        return ""


async def search_brave(query: str, api_key: str = "") -> str:
    """
    Brave Search API — бесплатный tier 2000 запросов/месяц.
    Опционально если пользователь добавит ключ.
    """
    if not api_key:
        return ""
    try:
        url = "https://api.search.brave.com/res/v1/web/search"
        headers = {**HEADERS, "Accept": "application/json", "X-Subscription-Token": api_key}
        params = {"q": query, "count": 3, "text_decorations": False}

        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, params=params, timeout=TIMEOUT) as resp:
                if resp.status != 200:
                    return ""
                data = await resp.json()

        results = data.get("web", {}).get("results", [])
        snippets = [r.get("description", "") for r in results[:3] if r.get("description")]
        return " | ".join(snippets)[:500]

    except Exception as e:
        logger.debug(f"Brave search error: {e}")
        return ""


async def fetch_realtime_prices() -> dict:
    """
    Собирает актуальные цены из нескольких источников параллельно.
    Возвращает словарь {актив: цена}.
    """
    prices = {}

    async with aiohttp.ClientSession(headers=HEADERS) as session:
        tasks = []

        # BTC + ETH через CoinGecko
        async def get_crypto():
            try:
                url = "https://api.coingecko.com/api/v3/simple/price"
                params = {
                    "ids": "bitcoin,ethereum,solana,binancecoin",
                    "vs_currencies": "usd",
                    "include_24hr_change": "true"
                }
                async with session.get(url, params=params, timeout=TIMEOUT) as r:
                    if r.status == 200:
                        data = await r.json()
                        for coin, vals in data.items():
                            name_map = {
                                "bitcoin": "BTC", "ethereum": "ETH",
                                "solana": "SOL", "binancecoin": "BNB"
                            }
                            key = name_map.get(coin, coin.upper())
                            prices[key] = {
                                "price": vals.get("usd", 0),
                                "change_24h": vals.get("usd_24h_change", 0),
                                "source": "CoinGecko (live)"
                            }
            except Exception as e:
                logger.debug(f"Crypto price error: {e}")

        # Золото через прямой API (metals-api бесплатно)
        async def get_gold():
            """
            Пробуем несколько источников по порядку.
            GLD ETF = ~1/10 цены золота, поэтому умножаем на 10 как последний резерв.
            Реальная цена золота сейчас ~$2900-3200 за унцию.
            """
            # Источник 1: GC=F фьючерс COMEX через Yahoo (нужны правильные заголовки)
            try:
                yf_headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Accept": "application/json",
                    "Referer": "https://finance.yahoo.com/",
                }
                url = "https://query2.finance.yahoo.com/v8/finance/chart/GC=F"
                async with session.get(url, params={"interval": "1d", "range": "5d"},
                                       headers=yf_headers, timeout=TIMEOUT) as r:
                    if r.status == 200:
                        data = await r.json()
                        meta = data["chart"]["result"][0]["meta"]
                        price = meta.get("regularMarketPrice", 0)
                        prev = meta.get("chartPreviousClose", price) or price
                        change = ((price - prev) / prev * 100) if prev else 0
                        # Золото $1800-$4000 — реальный диапазон
                        if 1800 < price < 4000:
                            prices["GOLD"] = {
                                "price": round(price, 2),
                                "change_24h": round(change, 2),
                                "source": "Yahoo Finance (live)"
                            }
                            logger.info(f"✅ Золото GC=F: ${price}")
                            return
                        else:
                            logger.warning(f"GC=F вернул подозрительную цену: ${price}")
            except Exception as e:
                logger.debug(f"GC=F error: {e}")

            # Источник 2: GLD ETF * 10 (приблизительно, ~1% погрешность)
            try:
                url = "https://query1.finance.yahoo.com/v8/finance/chart/GLD"
                async with session.get(url, params={"interval": "1d", "range": "2d"},
                                       timeout=TIMEOUT) as r:
                    if r.status == 200:
                        data = await r.json()
                        meta = data["chart"]["result"][0]["meta"]
                        gld_price = meta.get("regularMarketPrice", 0)
                        prev = meta.get("chartPreviousClose", gld_price) or gld_price
                        change = ((gld_price - prev) / prev * 100) if prev else 0
                        # GLD торгуется ~$270-320 (это ~$2700-3200 золота)
                        if 200 < gld_price < 400:
                            gold_approx = round(gld_price * 10, 2)
                            prices["GOLD"] = {
                                "price": gold_approx,
                                "change_24h": round(change, 2),
                                "source": "Yahoo Finance GLD (приблизительно)"
                            }
                            logger.info(f"✅ Золото через GLD: ${gld_price} × 10 = ${gold_approx}")
                            return
            except Exception as e:
                logger.debug(f"GLD error: {e}")

            logger.warning("❌ Цена золота недоступна из всех источников")

        # Акции через Yahoo Finance
        async def get_stocks():
            tickers = ["SPY", "QQQ", "^VIX", "DX-Y.NYB", "CL=F", "HG=F"]
            name_map = {
                "SPY": "SPY", "QQQ": "QQQ",
                "^VIX": "VIX", "DX-Y.NYB": "DXY",
                "CL=F": "OIL_WTI", "HG=F": "COPPER",
            }
            for ticker in tickers:
                try:
                    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
                    async with session.get(url, params={"interval": "1d", "range": "2d"},
                                          timeout=TIMEOUT) as r:
                        if r.status == 200:
                            data = await r.json()
                            meta = data["chart"]["result"][0]["meta"]
                            price = meta.get("regularMarketPrice", 0)
                            prev = meta.get("previousClose", price) or price
                            change = ((price - prev) / prev * 100) if prev else 0
                            key = name_map.get(ticker, ticker)
                            prices[key] = {
                                "price": price,
                                "change_24h": change,
                                "source": "Yahoo Finance (15min delay)"
                            }
                    await asyncio.sleep(0.15)
                except Exception:
                    continue

        await asyncio.gather(get_crypto(), get_stocks(), get_gold(), return_exceptions=True)

    return prices


def format_prices_for_agents(prices: dict) -> str:
    """Форматирует цены в читаемый текст для агентов."""
    if not prices:
        return "Актуальные цены недоступны."

    now = datetime.now().strftime("%d.%m.%Y %H:%M UTC")
    lines = [f"=== АКТУАЛЬНЫЕ ЦЕНЫ (получены {now}) ==="]
    lines.append("ВАЖНО: используй ТОЛЬКО эти цифры. Не называй другие цены.\n")

    sections = {
        "КРИПТА": ["BTC", "ETH", "SOL", "BNB"],
        "АКЦИИ/ETF": ["SPY", "QQQ", "GLD"],
        "МАКРО": ["VIX", "DXY"],
        "СЫРЬЁ": ["OIL_WTI", "GOLD"],
    }

    for section, keys in sections.items():
        section_lines = []
        for key in keys:
            if key in prices:
                p = prices[key]
                price = p["price"]
                change = p["change_24h"]
                source = p["source"]
                ch_str = f"+{change:.1f}%" if change >= 0 else f"{change:.1f}%"
                direction = "▲" if change >= 0 else "▼"

                # Форматирование числа
                if price > 10000:
                    price_str = f"${price:,.0f}"
                elif price > 100:
                    price_str = f"${price:,.2f}"
                else:
                    price_str = f"${price:.4f}"

                section_lines.append(
                    f"  {key}: {price_str} {direction} {ch_str} | Источник: {source}"
                )

        if section_lines:
            lines.append(f"[{section}]")
            lines.extend(section_lines)
            lines.append("")

    lines.append(
        "ИНСТРУКЦИЯ ДЛЯ АГЕНТОВ: Если актива нет в этом списке — "
        "не называй цену. Напиши 'цена не получена'."
    )

    return "\n".join(lines)


async def search_news_context(topic: str) -> str:
    """
    Ищет свежие новости по конкретной теме через DDG.
    Используется когда пользователь вводит /analyze [тема].
    """
    queries = [
        f"{topic} latest news today",
        f"{topic} market impact 2024",
    ]

    results = []
    for q in queries:
        answer = await search_ddg(q)
        if answer and len(answer) > 50:
            results.append(answer)
        await asyncio.sleep(0.5)

    if not results:
        return ""

    return f"=== ВЕБ-ПОИСК ПО ТЕМЕ ===\n" + "\n".join(results)


async def get_full_realtime_context() -> tuple[dict, str]:
    """
    Главная функция — получает всё что нужно агентам в реальном времени.
    Возвращает: (словарь цен, текст для агентов).
    """
    logger.info("🔍 Получаю актуальные цены в реальном времени...")
    prices = await fetch_realtime_prices()
    formatted = format_prices_for_agents(prices)
    logger.info(f"✅ Получено цен: {len(prices)}")
    return prices, formatted
