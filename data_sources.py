"""
data_sources.py — Расширенные источники данных для глубокого анализа.

Бесплатные источники:
1. GDELT — геополитические события в реальном времени
2. Fed Calendar — расписание заседаний ФРС и ЕЦБ
3. Fear & Greed Index — сентимент крипто и фондового рынка
4. Макро-данные — инфляция, ВВП, занятость (FRED API)
5. Commodity prices — нефть, золото, медь, газ
6. Whale Alert — крупные on-chain транзакции крипты
7. SEC Filings — инсайдерские покупки/продажи акций (легально, публично)
8. Earnings Calendar — отчётности компаний
9. Options Flow — необычная активность опционов
10. Social Sentiment — тренды в финансовых сообществах

v6.0: CPI Middleware — FRED отдаёт сырой индекс (~327),
      агенты теперь получают % YoY с контекстом таргета ФРС.
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional
import aiohttp
import json

logger = logging.getLogger(__name__)

TIMEOUT = aiohttp.ClientTimeout(total=12)
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; DialecticEdge/2.0)"}


# ─── 1. ГЕОПОЛИТИКА — GDELT ───────────────────────────────────────────────────

async def fetch_geopolitical_events() -> str:
    try:
        url = "https://api.gdeltproject.org/api/v2/doc/doc"
        params = {
            "query": "economy sanctions war trade geopolitics",
            "mode": "artlist",
            "maxrecords": 10,
            "format": "json",
            "timespan": "24h",
            "sort": "hybridrel",
        }
        async with aiohttp.ClientSession(headers=HEADERS) as session:
            async with session.get(url, params=params, timeout=TIMEOUT) as resp:
                if resp.status != 200:
                    return ""
                data = await resp.json(content_type=None)

        articles = data.get("articles", [])
        if not articles:
            return ""

        lines = ["🌍 *ГЕОПОЛИТИКА (GDELT):*"]
        for art in articles[:6]:
            title = art.get("title", "")[:120]
            source = art.get("domain", "")
            if title:
                lines.append(f"• {title} _({source})_")

        return "\n".join(lines)

    except Exception as e:
        logger.warning(f"GDELT error: {e}")
        return ""


# ─── 2. МАКРО — FRED API ─────────────────────────────────────────────────────
# v6.0: CPI Middleware — пересчитываем сырой индекс в % YoY
# Без этого агенты пишут "CPI 327 = стабильная экономика" — логическая ошибка

async def fetch_macro_indicators() -> str:
    try:
        indicators = {
            "FEDFUNDS": "Ставка ФРС %",
            "CPIAUCSL": "Инфляция CPI (США)",
            "UNRATE":   "Безработица США %",
            "DGS10":    "Доходность 10-лет US Treasury",
        }

        results = {}
        async with aiohttp.ClientSession(headers=HEADERS) as session:
            for series_id, name in list(indicators.items())[:4]:
                try:
                    url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"
                    async with session.get(url, timeout=TIMEOUT) as resp:
                        if resp.status == 200:
                            text = await resp.text()
                            lines = [l for l in text.strip().split("\n") if l]
                            if len(lines) >= 2:
                                last_line = lines[-1].split(",")
                                if len(last_line) == 2:
                                    date_str = last_line[0].strip()
                                    value = last_line[1].strip()
                                    if value and value != ".":
                                        results[name] = (float(value), date_str)
                    await asyncio.sleep(0.3)
                except Exception:
                    continue

        if not results:
            return ""

        lines = ["📊 *МАКРОЭКОНОМИКА (FRED/ФРС):*"]
        for name, (value, date) in results.items():

            # ── CPI Middleware ────────────────────────────────────────────────
            # FRED отдаёт CPI как индекс уровня цен (~327).
            # Это НЕ процент инфляции. Агенты обязаны получать YoY %.
            # Средний CPI 2023 года (база) = 304.7, 2024 = 314.2
            # Используем среднее значение год назад для расчёта YoY.
            if "CPI" in name or "Инфляция" in name:
                CPI_BASE_YEAR_AGO = 314.2   # средний CPI ~год назад
                yoy_pct = ((value - CPI_BASE_YEAR_AGO) / CPI_BASE_YEAR_AGO) * 100
                fed_target = 2.0
                gap = yoy_pct - fed_target
                gap_str = f"+{gap:.1f}%" if gap > 0 else f"{gap:.1f}%"
                if gap > 1.0:
                    status = "🔴 значительно выше таргета"
                elif gap > 0.3:
                    status = "🟠 выше таргета"
                else:
                    status = "🟢 близко к таргету"
                lines.append(
                    f"• Инфляция CPI США: индекс {value:.2f} → "
                    f"*~{yoy_pct:.1f}% годовых (YoY)* {status}\n"
                    f"  _(таргет ФРС: 2.0%, отклонение: {gap_str}, на {date})_"
                )
            else:
                lines.append(f"• {name}: *{value:.2f}* _(на {date})_")

        # Явная подсказка агентам чтобы не путали
        lines.append(
            "\n_📌 Агентам: CPI = индекс уровня цен (~327), "
            "инфляция = изменение YoY (указано выше в %). Не путать._"
        )
        return "\n".join(lines)

    except Exception as e:
        logger.warning(f"FRED error: {e}")
        return ""


# ─── 3. FEAR & GREED INDEX ────────────────────────────────────────────────────

async def fetch_fear_greed() -> str:
    results = []

    try:
        url = "https://api.alternative.me/fng/?limit=2&format=json"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=TIMEOUT) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    items = data.get("data", [])
                    if items:
                        current = items[0]
                        value = int(current.get("value", 0))
                        label = current.get("value_classification", "")
                        yesterday = int(items[1].get("value", 0)) if len(items) > 1 else value
                        change = value - yesterday

                        if value <= 25:
                            signal = "🔴 Экстремальный страх — исторически точка входа"
                        elif value <= 45:
                            signal = "🟠 Страх — рынок осторожен"
                        elif value <= 55:
                            signal = "🟡 Нейтрально"
                        elif value <= 75:
                            signal = "🟢 Жадность — осторожно"
                        else:
                            signal = "🔴 Экстремальная жадность — риск коррекции"

                        change_str = f"+{change}" if change > 0 else str(change)
                        results.append(
                            f"₿ Crypto Fear & Greed: *{value}/100* ({label}) "
                            f"{change_str} за сутки\n   {signal}"
                        )
    except Exception as e:
        logger.warning(f"Crypto F&G error: {e}")

    if not results:
        return ""

    return "😱 *ИНДЕКС СТРАХА И ЖАДНОСТИ:*\n" + "\n".join(results)


# ─── 4. COMMODITIES — СЫРЬЕВЫЕ ТОВАРЫ ────────────────────────────────────────

async def fetch_commodities() -> str:
    commodities = {
        "CL=F":     ("🛢️ Нефть WTI", "$/баррель"),
        "GC=F":     ("🥇 Золото", "$/унция"),
        "SI=F":     ("🥈 Серебро", "$/унция"),
        "HG=F":     ("🔶 Медь", "$/фунт"),
        "NG=F":     ("🔥 Газ", "$/MMBtu"),
        "ZW=F":     ("🌾 Пшеница", "$/бушель"),
        "DX-Y.NYB": ("💵 Индекс доллара", ""),
    }

    results = []
    gold_change = 0.0
    dollar_change = 0.0

    async with aiohttp.ClientSession(headers=HEADERS) as session:
        for ticker, (name, unit) in commodities.items():
            try:
                url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
                params = {"interval": "1d", "range": "2d"}
                async with session.get(url, params=params, timeout=TIMEOUT) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        meta = data["chart"]["result"][0]["meta"]
                        price = meta.get("regularMarketPrice", 0)
                        prev = meta.get("previousClose", price)
                        if price and prev:
                            change = ((price - prev) / prev) * 100
                            ch_emoji = "🟢" if change >= 0 else "🔴"
                            ch_str = f"+{change:.1f}%" if change >= 0 else f"{change:.1f}%"
                            results.append(
                                f"{name}: *{price:.2f}* {unit} {ch_emoji} {ch_str}"
                            )
                            if "Золото" in name:
                                gold_change = change
                            if "доллара" in name:
                                dollar_change = change
                await asyncio.sleep(0.2)
            except Exception:
                continue

    if not results:
        return ""

    interpretation = []

    # Dr. Copper
    copper_line = next((r for r in results if "Медь" in r), None)
    if copper_line and "🔴" in copper_line:
        interpretation.append("⚠️ _Медь падает → сигнал замедления мировой экономики_")
    elif copper_line and "🟢" in copper_line:
        interpretation.append("✅ _Медь растёт → сигнал роста промышленного спроса_")

    # Risk-off сигнал — важно для агентов чтобы не путали
    if gold_change > 0.5:
        interpretation.append(
            "⚠️ _Золото растёт = RISK-OFF сигнал: инвесторы уходят в защитные активы. "
            "Это медвежий сигнал для BTC и акций, не бычий._"
        )
    if dollar_change > 0.3 and gold_change > 0.3:
        interpretation.append(
            "🔴 _Золото + доллар растут одновременно = стагфляционный риск или геополитика. "
            "Давление на все рисковые активы (BTC, акции, крипта)._"
        )

    lines = ["🛢️ *СЫРЬЕВЫЕ ТОВАРЫ:*"] + results + interpretation
    return "\n".join(lines)


# ─── 5. ИНСАЙДЕРСКИЕ СДЕЛКИ SEC ──────────────────────────────────────────────

async def fetch_sec_insider_trades() -> str:
    try:
        url = "https://openinsider.com/screener"
        params = {
            "s": "", "o": "",
            "pl": "1000000", "ph": "",
            "yn": "1", "sortcol": "0",
            "cnt": "10", "action": "getdata",
        }

        async with aiohttp.ClientSession(headers=HEADERS) as session:
            async with session.get(url, params=params, timeout=TIMEOUT) as resp:
                if resp.status != 200:
                    return ""
                text = await resp.text()

        import re
        rows = re.findall(
            r'<td[^>]*>\s*([A-Z]{1,5})\s*</td>.*?'
            r'<td[^>]*>\s*(CEO|CFO|Director|President|COO|CTO)\s*</td>.*?'
            r'<td[^>]*>\s*\+?([\d,]+)\s*</td>',
            text, re.DOTALL
        )

        if not rows:
            return ""

        lines = ["🏛️ *ИНСАЙДЕРСКИЕ ПОКУПКИ (SEC Form 4):*"]
        lines.append("_Топ-менеджеры покупают акции своих компаний на личные деньги:_")

        seen = set()
        count = 0
        for ticker, role, shares in rows[:8]:
            if ticker not in seen and count < 5:
                seen.add(ticker)
                shares_fmt = shares.replace(",", "")
                try:
                    if int(shares_fmt) > 1000:
                        lines.append(f"• *{ticker}* — {role} купил {shares} акций")
                        count += 1
                except ValueError:
                    continue

        if count == 0:
            return ""

        lines.append("_⚠️ Инсайдерские покупки — сигнал уверенности, не гарантия роста_")
        return "\n".join(lines)

    except Exception as e:
        logger.warning(f"SEC insider error: {e}")
        return ""


# ─── 6. ЭКОНОМИЧЕСКИЙ КАЛЕНДАРЬ ───────────────────────────────────────────────

async def fetch_economic_calendar() -> str:
    try:
        import feedparser

        url = "https://www.investing.com/rss/news_14.rss"
        async with aiohttp.ClientSession(headers=HEADERS) as session:
            async with session.get(url, timeout=TIMEOUT) as resp:
                content = await resp.text()

        feed = feedparser.parse(content)

        keywords = [
            "fed", "fomc", "rate decision", "cpi", "inflation", "nfp",
            "jobs report", "gdp", "payroll", "ecb", "bank of england",
            "powell", "lagarde", "interest rate", "ставка", "заседание"
        ]

        important = []
        for entry in feed.entries[:20]:
            title = entry.get("title", "").lower()
            if any(kw in title for kw in keywords):
                important.append(entry.get("title", "")[:100])
            if len(important) >= 4:
                break

        if not important:
            return ""

        lines = ["📅 *ВАЖНЫЕ СОБЫТИЯ (Экономический календарь):*"]
        for event in important:
            lines.append(f"• {event}")

        return "\n".join(lines)

    except Exception as e:
        logger.warning(f"Economic calendar error: {e}")
        return ""


# ─── 7. ON-CHAIN МЕТРИКИ ──────────────────────────────────────────────────────

async def fetch_onchain_metrics() -> str:
    try:
        results = []

        async with aiohttp.ClientSession() as session:
            try:
                url = "https://blockchain.info/stats?format=json"
                async with session.get(url, timeout=TIMEOUT) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        n_tx = data.get("n_tx", 0)
                        mempool = data.get("mempool_size", 0)
                        hash_rate = data.get("hash_rate", 0)
                        results.append(f"• Транзакций BTC за 24ч: *{n_tx:,}*")
                        results.append(f"• Mempool (незакрытых): *{mempool:,}*")
                        if hash_rate:
                            results.append(f"• Hash Rate: *{hash_rate/1e9:.1f} EH/s*")
            except Exception:
                pass

            try:
                url = "https://api.etherscan.io/api?module=gastracker&action=gasoracle"
                async with session.get(url, timeout=TIMEOUT) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        safe_gas = data.get("result", {}).get("SafeGasPrice", "?")
                        results.append(f"• ETH Gas (safe): *{safe_gas} Gwei*")
            except Exception:
                pass

        if not results:
            return ""

        lines = ["⛓️ *ON-CHAIN МЕТРИКИ:*"] + results
        lines.append(
            "_⚠️ Агентам: ончейн-метрики (Hash Rate, транзакции) показывают "
            "активность сети, но НЕ перевешивают макро-факторы (инфляция, ставки ФРС)._"
        )
        return "\n".join(lines)

    except Exception as e:
        logger.warning(f"On-chain error: {e}")
        return ""


# ─── 8. ГЛОБАЛЬНЫЕ РЫНКИ ──────────────────────────────────────────────────────

async def fetch_global_markets() -> str:
    indices = {
        "^N225":  "🇯🇵 Nikkei 225",
        "^HSI":   "🇭🇰 Hang Seng",
        "^SSEC":  "🇨🇳 Shanghai",
        "^FTSE":  "🇬🇧 FTSE 100",
        "^GDAXI": "🇩🇪 DAX",
        "^FCHI":  "🇫🇷 CAC 40",
        "^RTS.ME":"🇷🇺 RTS (Россия)",
    }

    results = []
    async with aiohttp.ClientSession(headers=HEADERS) as session:
        for ticker, name in list(indices.items())[:5]:
            try:
                url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
                params = {"interval": "1d", "range": "2d"}
                async with session.get(url, params=params, timeout=TIMEOUT) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        meta = data["chart"]["result"][0]["meta"]
                        price = meta.get("regularMarketPrice", 0)
                        prev = meta.get("previousClose", price)
                        if price and prev:
                            change = ((price - prev) / prev) * 100
                            ch_emoji = "🟢" if change >= 0 else "🔴"
                            ch_str = f"+{change:.1f}%" if change >= 0 else f"{change:.1f}%"
                            results.append(f"{name}: {ch_emoji} {ch_str}")
                await asyncio.sleep(0.2)
            except Exception:
                continue

    if not results:
        return ""

    green = sum(1 for r in results if "🟢" in r)
    red = sum(1 for r in results if "🔴" in r)
    if green > red:
        sentiment = "🟢 _Глобальный риск-аппетит позитивный_"
    elif red > green:
        sentiment = "🔴 _Глобальное бегство от риска_"
    else:
        sentiment = "🟡 _Смешанный глобальный сентимент_"

    lines = ["🌐 *МИРОВЫЕ РЫНКИ:*"] + results + [sentiment]
    return "\n".join(lines)


# ─── 9. SOCIAL SENTIMENT — ТРЕНДЫ ────────────────────────────────────────────

async def fetch_trending_topics() -> str:
    results = []

    try:
        url = "https://api.coingecko.com/api/v3/search/trending"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=TIMEOUT) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    coins = data.get("coins", [])[:5]
                    if coins:
                        trending = [c["item"]["name"] for c in coins]
                        results.append(
                            "🔥 *Trending крипта (CoinGecko):* " +
                            " | ".join(trending)
                        )
    except Exception:
        pass

    return "\n".join(results) if results else ""


# ─── ГЛАВНАЯ ФУНКЦИЯ ──────────────────────────────────────────────────────────

async def fetch_full_context() -> str:
    logger.info("📡 Собираю расширенный контекст данных...")

    tasks = [
        fetch_geopolitical_events(),
        fetch_macro_indicators(),
        fetch_fear_greed(),
        fetch_commodities(),
        fetch_global_markets(),
        fetch_economic_calendar(),
        fetch_onchain_metrics(),
        fetch_sec_insider_trades(),
        fetch_trending_topics(),
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    sections = []
    labels = [
        "Геополитика", "Макро", "Сентимент", "Сырьё",
        "Мировые рынки", "Календарь", "On-chain",
        "Инсайдеры SEC", "Тренды"
    ]

    for label, result in zip(labels, results):
        if isinstance(result, str) and result.strip():
            sections.append(result)
        elif isinstance(result, Exception):
            logger.warning(f"{label}: {result}")

    if not sections:
        return "Расширенные данные временно недоступны."

    now = datetime.now().strftime("%d.%m.%Y %H:%M UTC")
    header = f"=== РАСШИРЕННЫЙ КОНТЕКСТ ({now}) ===\n"

    return header + "\n\n".join(sections)
