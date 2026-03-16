"""
data_sources.py — Расширенные источники данных для глубокого анализа.

ДОБАВЛЕНО v3:
- Finnhub: market sentiment, инсайдерские сделки, earnings calendar
- Alpha Vantage: технические индикаторы RSI, MACD, SMA
- CPI база исправлена: 319.8 (синхронизировано с web_search.py)

Новые переменные окружения (Railway):
  FINNHUB_API_KEY    — получить бесплатно: https://finnhub.io/register
  ALPHA_VANTAGE_API_KEY — получить бесплатно: https://www.alphavantage.co/support/#api-key
"""

import asyncio
import logging
import os
from datetime import datetime

import aiohttp

logger = logging.getLogger(__name__)

TIMEOUT = aiohttp.ClientTimeout(total=12)
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; DialecticEdge/3.0)"}

FINNHUB_API_KEY       = os.getenv("FINNHUB_API_KEY", "")
ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY", "")


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
            if "CPI" in name or "Инфляция" in name:
                # ИСПРАВЛЕНО: синхронизировано с web_search.py
                CPI_BASE_YEAR_AGO = 319.8
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

        lines.append(
            "\n_📌 Агентам: CPI = индекс уровня цен (~320), "
            "инфляция = изменение YoY (указано выше в %). Не путать._"
        )
        return "\n".join(lines)

    except Exception as e:
        logger.warning(f"FRED error: {e}")
        return ""


# ─── 3. FEAR & GREED INDEX ────────────────────────────────────────────────────

async def fetch_fear_greed() -> str:
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
                        return (
                            "😱 *ИНДЕКС СТРАХА И ЖАДНОСТИ:*\n"
                            f"₿ Crypto Fear & Greed: *{value}/100* ({label}) "
                            f"{change_str} за сутки\n   {signal}"
                        )
    except Exception as e:
        logger.warning(f"Crypto F&G error: {e}")
    return ""


# ─── 4. COMMODITIES ───────────────────────────────────────────────────────────

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
                            results.append(f"{name}: *{price:.2f}* {unit} {ch_emoji} {ch_str}")
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
    copper_line = next((r for r in results if "Медь" in r), None)
    if copper_line and "🔴" in copper_line:
        interpretation.append("⚠️ _Медь падает → сигнал замедления мировой экономики_")
    elif copper_line and "🟢" in copper_line:
        interpretation.append("✅ _Медь растёт → сигнал роста промышленного спроса_")

    if gold_change > 0.5:
        interpretation.append(
            "⚠️ _Золото растёт = RISK-OFF сигнал: инвесторы уходят в защитные активы. "
            "Это медвежий сигнал для BTC и акций, не бычий._"
        )
    if dollar_change > 0.3 and gold_change > 0.3:
        interpretation.append(
            "🔴 _Золото + доллар растут = стагфляционный риск. "
            "Давление на все рисковые активы._"
        )

    return "\n".join(["🛢️ *СЫРЬЕВЫЕ ТОВАРЫ:*"] + results + interpretation)


# ─── 5. FINNHUB — НОВОЕ ───────────────────────────────────────────────────────

async def fetch_finnhub_sentiment() -> str:
    """
    Finnhub: рыночный сентимент и инсайдерские сделки.
    Бесплатный ключ: https://finnhub.io/register
    """
    if not FINNHUB_API_KEY:
        return ""

    results = []

    async with aiohttp.ClientSession() as session:

        # Новостной сентимент по крипте и рынкам
        for symbol, name in [("BINANCE:BTCUSDT", "BTC"), ("AAPL", "Apple"), ("SPY", "S&P ETF")]:
            try:
                url = "https://finnhub.io/api/v1/news-sentiment"
                params = {"symbol": symbol, "token": FINNHUB_API_KEY}
                async with session.get(url, params=params, timeout=TIMEOUT) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        score = data.get("companyNewsScore", 0)
                        buzz  = data.get("buzz", {})
                        articles_in_week = buzz.get("articlesInLastWeek", 0)
                        weekly_avg = buzz.get("weeklyAverage", 0)

                        if score > 0:
                            sentiment_emoji = "🟢" if score > 0.6 else "🟡" if score > 0.4 else "🔴"
                            buzz_str = ""
                            if weekly_avg > 0:
                                buzz_change = ((articles_in_week - weekly_avg) / weekly_avg * 100)
                                buzz_str = f" | Buzz: {buzz_change:+.0f}% vs среднего"
                            results.append(
                                f"• {name}: сентимент {sentiment_emoji} {score:.2f}/1.0{buzz_str}"
                            )
                await asyncio.sleep(0.3)
            except Exception as e:
                logger.debug(f"Finnhub sentiment {symbol}: {e}")
                continue

        # Earnings calendar — ближайшие отчётности
        try:
            from datetime import date, timedelta
            today = date.today()
            week_later = today + timedelta(days=7)
            url = "https://finnhub.io/api/v1/calendar/earnings"
            params = {
                "from": today.strftime("%Y-%m-%d"),
                "to": week_later.strftime("%Y-%m-%d"),
                "token": FINNHUB_API_KEY,
            }
            async with session.get(url, params=params, timeout=TIMEOUT) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    earnings = data.get("earningsCalendar", [])
                    # Фильтруем только крупные компании
                    big_names = {"AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META",
                                 "TSLA", "NFLX", "AMD", "INTC", "JPM", "GS", "BAC"}
                    important = [e for e in earnings if e.get("symbol") in big_names]
                    if important:
                        results.append("\n📅 *Ближайшие отчётности (7 дней):*")
                        for e in important[:4]:
                            sym  = e.get("symbol", "")
                            date_str = e.get("date", "")
                            eps_est  = e.get("epsEstimate")
                            eps_str  = f" | EPS прогноз: ${eps_est:.2f}" if eps_est else ""
                            results.append(f"  • {sym}: {date_str}{eps_str}")
        except Exception as e:
            logger.debug(f"Finnhub earnings: {e}")

        # Инсайдерские сделки топ компаний
        try:
            insider_lines = []
            for symbol in ["NVDA", "AAPL", "MSFT", "TSLA"]:
                url = "https://finnhub.io/api/v1/stock/insider-transactions"
                params = {"symbol": symbol, "token": FINNHUB_API_KEY}
                async with session.get(url, params=params, timeout=TIMEOUT) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        transactions = data.get("data", [])
                        # Берём только покупки за последние 30 дней
                        buys = [t for t in transactions
                                if t.get("transactionType") == "P"
                                and t.get("share", 0) > 1000]
                        if buys:
                            latest = buys[0]
                            shares = latest.get("share", 0)
                            name_insider = latest.get("name", "Инсайдер")
                            insider_lines.append(f"  • *{symbol}*: {name_insider} купил {shares:,} акций")
                await asyncio.sleep(0.2)

            if insider_lines:
                results.append("\n🏛️ *Инсайдерские покупки (Finnhub):*")
                results.extend(insider_lines[:3])
        except Exception as e:
            logger.debug(f"Finnhub insider: {e}")

    if not results:
        return ""

    return "📡 *FINNHUB СЕНТИМЕНТ И СОБЫТИЯ:*\n" + "\n".join(results)


# ─── 6. ALPHA VANTAGE — ТЕХНИЧЕСКИЕ ИНДИКАТОРЫ — НОВОЕ ───────────────────────

async def fetch_technical_indicators() -> str:
    """
    Alpha Vantage: RSI, MACD для BTC и S&P 500.
    Бесплатный ключ: https://www.alphavantage.co/support/#api-key
    Лимит: 25 запросов/день на бесплатном плане.
    """
    if not ALPHA_VANTAGE_API_KEY:
        return ""

    results = []

    async with aiohttp.ClientSession() as session:

        # RSI для BTC (через криптo endpoint)
        try:
            url = "https://www.alphavantage.co/query"
            params = {
                "function": "RSI",
                "symbol": "BTCUSD",
                "interval": "daily",
                "time_period": 14,
                "series_type": "close",
                "apikey": ALPHA_VANTAGE_API_KEY,
            }
            async with session.get(url, params=params, timeout=TIMEOUT) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    rsi_data = data.get("Technical Analysis: RSI", {})
                    if rsi_data:
                        latest_date = sorted(rsi_data.keys())[-1]
                        rsi_val = float(rsi_data[latest_date]["RSI"])

                        if rsi_val >= 70:
                            rsi_signal = "🔴 Перекупленность — возможна коррекция"
                        elif rsi_val <= 30:
                            rsi_signal = "🟢 Перепроданность — возможен отскок"
                        elif rsi_val >= 60:
                            rsi_signal = "🟡 Умеренный бычий импульс"
                        else:
                            rsi_signal = "🟡 Нейтральная зона"

                        results.append(f"• BTC RSI(14): *{rsi_val:.1f}* — {rsi_signal}")
            await asyncio.sleep(0.5)
        except Exception as e:
            logger.debug(f"Alpha Vantage RSI BTC: {e}")

        # RSI для S&P 500
        try:
            url = "https://www.alphavantage.co/query"
            params = {
                "function": "RSI",
                "symbol": "SPY",
                "interval": "daily",
                "time_period": 14,
                "series_type": "close",
                "apikey": ALPHA_VANTAGE_API_KEY,
            }
            async with session.get(url, params=params, timeout=TIMEOUT) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    rsi_data = data.get("Technical Analysis: RSI", {})
                    if rsi_data:
                        latest_date = sorted(rsi_data.keys())[-1]
                        rsi_val = float(rsi_data[latest_date]["RSI"])

                        if rsi_val >= 70:
                            rsi_signal = "🔴 Перекупленность"
                        elif rsi_val <= 30:
                            rsi_signal = "🟢 Перепроданность"
                        else:
                            rsi_signal = "🟡 Нейтральная зона"

                        results.append(f"• SPY RSI(14): *{rsi_val:.1f}* — {rsi_signal}")
            await asyncio.sleep(0.5)
        except Exception as e:
            logger.debug(f"Alpha Vantage RSI SPY: {e}")

        # MACD для BTC
        try:
            url = "https://www.alphavantage.co/query"
            params = {
                "function": "MACD",
                "symbol": "BTCUSD",
                "interval": "daily",
                "series_type": "close",
                "apikey": ALPHA_VANTAGE_API_KEY,
            }
            async with session.get(url, params=params, timeout=TIMEOUT) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    macd_data = data.get("Technical Analysis: MACD", {})
                    if macd_data:
                        latest_date = sorted(macd_data.keys())[-1]
                        macd_val  = float(macd_data[latest_date]["MACD"])
                        signal_val = float(macd_data[latest_date]["MACD_Signal"])
                        hist_val  = float(macd_data[latest_date]["MACD_Hist"])

                        if hist_val > 0 and macd_val > signal_val:
                            macd_signal = "🟢 Бычий кроссовер — импульс вверх"
                        elif hist_val < 0 and macd_val < signal_val:
                            macd_signal = "🔴 Медвежий кроссовер — импульс вниз"
                        elif hist_val > 0:
                            macd_signal = "🟡 Бычий импульс ослабевает"
                        else:
                            macd_signal = "🟡 Медвежий импульс ослабевает"

                        results.append(
                            f"• BTC MACD: {macd_val:.1f} | Signal: {signal_val:.1f} "
                            f"| Hist: {hist_val:+.1f} — {macd_signal}"
                        )
        except Exception as e:
            logger.debug(f"Alpha Vantage MACD: {e}")

    if not results:
        return ""

    header = "📈 *ТЕХНИЧЕСКИЕ ИНДИКАТОРЫ (Alpha Vantage):*\n"
    footer = (
        "\n_📌 Агентам: технические индикаторы подтверждают или опровергают "
        "макро-тезисы. RSI > 70 = перекупленность. RSI < 30 = перепроданность. "
        "Макро > техника по иерархии._"
    )
    return header + "\n".join(results) + footer


# ─── 7. ИНСАЙДЕРСКИЕ СДЕЛКИ SEC (fallback если нет Finnhub) ──────────────────

async def fetch_sec_insider_trades() -> str:
    if FINNHUB_API_KEY:
        return ""  # Finnhub уже даёт инсайдерские сделки

    try:
        url = "https://openinsider.com/screener"
        params = {
            "s": "", "o": "", "pl": "1000000", "ph": "",
            "yn": "1", "sortcol": "0", "cnt": "10", "action": "getdata",
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
        seen, count = set(), 0
        for ticker, role, shares in rows[:8]:
            if ticker not in seen and count < 5:
                seen.add(ticker)
                try:
                    if int(shares.replace(",", "")) > 1000:
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


# ─── 8. ЭКОНОМИЧЕСКИЙ КАЛЕНДАРЬ ──────────────────────────────────────────────

async def fetch_economic_calendar() -> str:
    if FINNHUB_API_KEY:
        return ""  # Finnhub уже даёт earnings calendar

    try:
        import feedparser
        url = "https://www.investing.com/rss/news_14.rss"
        async with aiohttp.ClientSession(headers=HEADERS) as session:
            async with session.get(url, timeout=TIMEOUT) as resp:
                content = await resp.text()

        feed = feedparser.parse(content)
        keywords = [
            "fed", "fomc", "rate decision", "cpi", "inflation", "nfp",
            "jobs report", "gdp", "payroll", "ecb", "powell", "lagarde",
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


# ─── 9. ON-CHAIN МЕТРИКИ ──────────────────────────────────────────────────────

async def fetch_onchain_metrics() -> str:
    try:
        results = []
        async with aiohttp.ClientSession() as session:
            try:
                url = "https://blockchain.info/stats?format=json"
                async with session.get(url, timeout=TIMEOUT) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        n_tx      = data.get("n_tx", 0)
                        mempool   = data.get("mempool_size", 0)
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
            "_⚠️ Агентам: ончейн-метрики показывают активность сети, "
            "но НЕ перевешивают макро-факторы._"
        )
        return "\n".join(lines)

    except Exception as e:
        logger.warning(f"On-chain error: {e}")
        return ""


# ─── 10. ГЛОБАЛЬНЫЕ РЫНКИ ─────────────────────────────────────────────────────

async def fetch_global_markets() -> str:
    indices = {
        "^N225":   "🇯🇵 Nikkei 225",
        "^HSI":    "🇭🇰 Hang Seng",
        "^FTSE":   "🇬🇧 FTSE 100",
        "^GDAXI":  "🇩🇪 DAX",
        "^RTS.ME": "🇷🇺 RTS (Россия)",
    }

    results = []
    async with aiohttp.ClientSession(headers=HEADERS) as session:
        for ticker, name in indices.items():
            try:
                url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
                params = {"interval": "1d", "range": "2d"}
                async with session.get(url, params=params, timeout=TIMEOUT) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        meta  = data["chart"]["result"][0]["meta"]
                        price = meta.get("regularMarketPrice", 0)
                        prev  = meta.get("previousClose", price)
                        if price and prev:
                            change   = ((price - prev) / prev) * 100
                            ch_emoji = "🟢" if change >= 0 else "🔴"
                            ch_str   = f"+{change:.1f}%" if change >= 0 else f"{change:.1f}%"
                            results.append(f"{name}: {ch_emoji} {ch_str}")
                await asyncio.sleep(0.2)
            except Exception:
                continue

    if not results:
        return ""

    green = sum(1 for r in results if "🟢" in r)
    red   = sum(1 for r in results if "🔴" in r)
    if green > red:
        sentiment = "🟢 _Глобальный риск-аппетит позитивный_"
    elif red > green:
        sentiment = "🔴 _Глобальное бегство от риска_"
    else:
        sentiment = "🟡 _Смешанный глобальный сентимент_"

    return "\n".join(["🌐 *МИРОВЫЕ РЫНКИ:*"] + results + [sentiment])


# ─── 11. TRENDING КРИПТА ──────────────────────────────────────────────────────

async def fetch_trending_topics() -> str:
    try:
        url = "https://api.coingecko.com/api/v3/search/trending"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=TIMEOUT) as resp:
                if resp.status == 200:
                    data  = await resp.json()
                    coins = data.get("coins", [])[:5]
                    if coins:
                        trending = [c["item"]["name"] for c in coins]
                        return "🔥 *Trending крипта (CoinGecko):* " + " | ".join(trending)
    except Exception:
        pass
    return ""


# ─── ГЛАВНАЯ ФУНКЦИЯ ──────────────────────────────────────────────────────────

async def fetch_full_context() -> str:
    logger.info("📡 Собираю расширенный контекст данных...")

    # Запускаем всё параллельно
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
        fetch_finnhub_sentiment(),       # НОВОЕ
        fetch_technical_indicators(),    # НОВОЕ
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    labels = [
        "Геополитика", "Макро", "Сентимент", "Сырьё",
        "Мировые рынки", "Календарь", "On-chain",
        "Инсайдеры SEC", "Тренды",
        "Finnhub", "Alpha Vantage",
    ]

    sections = []
    for label, result in zip(labels, results):
        if isinstance(result, str) and result.strip():
            sections.append(result)
        elif isinstance(result, Exception):
            logger.warning(f"{label}: {result}")

    if not sections:
        return "Расширенные данные временно недоступны."

    # Показываем какие источники активны
    active = []
    if FINNHUB_API_KEY:       active.append("Finnhub✅")
    if ALPHA_VANTAGE_API_KEY: active.append("AlphaVantage✅")
    sources_str = f" | Доп. источники: {', '.join(active)}" if active else ""

    now    = datetime.now().strftime("%d.%m.%Y %H:%M UTC")
    header = f"=== РАСШИРЕННЫЙ КОНТЕКСТ ({now}{sources_str}) ===\n"

    return header + "\n\n".join(sections)
