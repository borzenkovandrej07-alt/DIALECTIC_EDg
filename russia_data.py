"""
russia_data.py v2.0 — Расширенные источники данных для РФ модуля.

Источники:
1. ЦБ РФ — ключевая ставка, курсы валют (USD/EUR/CNY/TRY)
2. Мосбиржа MOEX ISS — индекс IMOEX, топ акции, облигации
3. РБК RSS — экономика, бизнес, политика
4. Коммерсант RSS — деловые новости
5. Ведомости RSS — финансы и экономика
6. Интерфакс RSS — оперативные новости
7. Росстат — инфляция РФ, ВВП (публичные данные)
8. Минфин РФ — ОФЗ доходность
9. Нефть Urals — российский сорт (дисконт к Brent)
"""

import asyncio
import logging
import re
from datetime import datetime
import aiohttp

logger = logging.getLogger(__name__)
TIMEOUT = aiohttp.ClientTimeout(total=15)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept-Language": "ru-RU,ru;q=0.9",
}


# ─── 1. ЦБ РФ — курсы и ставка ───────────────────────────────────────────────

async def fetch_cbr_data() -> str:
    results = []

    try:
        url = "https://www.cbr.ru/scripts/XML_daily.asp"
        async with aiohttp.ClientSession(headers=HEADERS) as session:
            async with session.get(url, timeout=TIMEOUT) as resp:
                if resp.status == 200:
                    text = await resp.text(encoding="windows-1251")

                    currencies = {
                        "USD": "💵 Доллар",
                        "EUR": "💶 Евро",
                        "CNY": "🇨🇳 Юань",
                        "TRY": "🇹🇷 Лира",
                        "AMD": "🇦🇲 Драм",
                        "KZT": "🇰🇿 Тенге",
                    }

                    for code, name in currencies.items():
                        pattern = rf'<CharCode>{code}</CharCode>.*?<Nominal>(\d+)</Nominal>.*?<Value>([\d,]+)</Value>'
                        m = re.search(pattern, text, re.DOTALL)
                        if m:
                            nominal = int(m.group(1))
                            val = float(m.group(2).replace(",", "."))
                            if nominal > 1:
                                results.append(f"• {name}: *{val:.2f} ₽* (за {nominal})")
                            else:
                                results.append(f"• {name}: *{val:.2f} ₽*")

    except Exception as e:
        logger.warning(f"CBR курсы error: {e}")

    # Ключевая ставка
    try:
        url = "https://www.cbr.ru/hd_base/KeyRate/?UniDbQuery.Posted=True&UniDbQuery.From=01.01.2025&UniDbQuery.To=31.12.2026"
        async with aiohttp.ClientSession(headers=HEADERS) as session:
            async with session.get(url, timeout=TIMEOUT) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    rates = re.findall(r'(\d{2}\.\d{2}\.\d{4})</td>\s*<td[^>]*>([\d,]+)', text)
                    if rates:
                        date, rate = rates[-1]
                        rate_val = rate.replace(",", ".")
                        rate_f = float(rate_val)
                        if rate_f >= 20:
                            comment = "🔴 _исторически высокая — давит на бизнес и ипотеку_"
                        elif rate_f >= 16:
                            comment = "🟠 _высокая — кредиты дорогие_"
                        else:
                            comment = "🟡 _умеренная_"
                        results.append(f"• 🏦 Ключевая ставка ЦБ: *{rate_val}%* {comment} _(на {date})_")
    except Exception as e:
        logger.warning(f"CBR ставка error: {e}")

    if not results:
        return ""

    lines = ["🏦 *ЦБ РФ — КУРСЫ И СТАВКА:*"] + results
    lines.append("_📌 Агентам: высокая ставка ЦБ = дорогие кредиты, давление на бизнес и рынок акций_")
    return "\n".join(lines)


# ─── 2. Мосбиржа — индекс + акции + ОФЗ ─────────────────────────────────────

async def fetch_moex_data() -> str:
    results = []

    async with aiohttp.ClientSession(headers=HEADERS) as session:

        # IMOEX индекс
        try:
            url = "https://iss.moex.com/iss/engines/stock/markets/index/securities/IMOEX.json"
            async with session.get(url, timeout=TIMEOUT) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    cols = data["marketdata"]["columns"]
                    rows = data["marketdata"]["data"]
                    if rows:
                        d = dict(zip(cols, rows[0]))
                        last  = d.get("CURRENTVALUE") or d.get("LASTVALUE")
                        change = d.get("LASTCHANGEPRC") or 0
                        if last:
                            ch_emoji = "🟢" if change >= 0 else "🔴"
                            ch_str = f"+{change:.2f}%" if change >= 0 else f"{change:.2f}%"
                            results.append(f"• 📊 IMOEX: *{last:.2f}* {ch_emoji} {ch_str}")
        except Exception as e:
            logger.warning(f"MOEX IMOEX error: {e}")

        await asyncio.sleep(0.3)

        # Топ акции РФ
        top_tickers = [
            ("SBER",  "Сбер"),
            ("GAZP",  "Газпром"),
            ("LKOH",  "Лукойл"),
            ("YNDX",  "Яндекс"),
            ("NVTK",  "Новатэк"),
            ("ROSN",  "Роснефть"),
            ("GMKN",  "Норникель"),
            ("TCSG",  "ТКС/Тинькофф"),
        ]

        stock_lines = []
        for ticker, name in top_tickers:
            try:
                url = f"https://iss.moex.com/iss/engines/stock/markets/shares/securities/{ticker}.json"
                async with session.get(url, timeout=TIMEOUT) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        cols = data["marketdata"]["columns"]
                        rows = data["marketdata"]["data"]
                        if rows:
                            d = dict(zip(cols, rows[0]))
                            price  = d.get("LAST") or d.get("WAPRICE")
                            change = d.get("LASTTOPREVPRICE") or 0
                            if price:
                                ch_emoji = "🟢" if change >= 0 else "🔴"
                                ch_str = f"+{change:.1f}%" if change >= 0 else f"{change:.1f}%"
                                stock_lines.append(
                                    f"  {ticker} ({name}): *{price:.1f} ₽* {ch_emoji} {ch_str}"
                                )
                await asyncio.sleep(0.15)
            except Exception:
                continue

        if stock_lines:
            results.append("• 🏢 *Акции РФ:*\n" + "\n".join(stock_lines))

        # ОФЗ доходность (гособлигации)
        try:
            url = "https://iss.moex.com/iss/engines/bond/markets/govt/securities.json?securities=SU26238RMFS4"
            async with session.get(url, timeout=TIMEOUT) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    cols = data["marketdata"]["columns"]
                    rows = data["marketdata"]["data"]
                    if rows:
                        d = dict(zip(cols, rows[0]))
                        yield_val = d.get("YIELD")
                        if yield_val:
                            results.append(
                                f"• 📜 ОФЗ доходность (10 лет): *{yield_val:.2f}%*\n"
                                f"  _📌 Если ОФЗ > ключевой ставки — рынок ждёт снижения ставки_"
                            )
        except Exception as e:
            logger.warning(f"MOEX OFZ error: {e}")

    if not results:
        return ""

    return "📈 *МОСБИРЖА:*\n" + "\n".join(results)


# ─── 3. Нефть Urals (российский сорт) ────────────────────────────────────────

async def fetch_urals_oil() -> str:
    """
    Urals — российская нефть, продаётся с дисконтом к Brent.
    Критически важна для бюджета РФ.
    """
    try:
        # Минфин РФ публикует цену Urals для расчёта налогов
        url = "https://www.minfin.ru/ru/perfomance/oil_gas/estimates/"
        async with aiohttp.ClientSession(headers=HEADERS) as session:
            async with session.get(url, timeout=TIMEOUT) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    prices = re.findall(r'(\d{1,3}[.,]\d{1,2})\s*долл', text)
                    if prices:
                        price = prices[0].replace(",", ".")
                        return (
                            f"🛢️ *НЕФТЬ URALS (российский сорт):*\n"
                            f"• Цена Urals: *${price}/баррель*\n"
                            f"_📌 Каждые $10 изменения цены Urals = ~±1.5 трлн ₽ в бюджет РФ_"
                        )
    except Exception as e:
        logger.warning(f"Urals error: {e}")

    # Fallback — считаем через Brent с типичным дисконтом
    try:
        url = "https://query1.finance.yahoo.com/v8/finance/chart/BZ=F"
        params = {"interval": "1d", "range": "2d"}
        async with aiohttp.ClientSession(headers=HEADERS) as session:
            async with session.get(url, params=params, timeout=TIMEOUT) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    brent = data["chart"]["result"][0]["meta"].get("regularMarketPrice", 0)
                    if brent:
                        urals_est = brent - 12  # типичный дисконт Urals к Brent
                        return (
                            f"🛢️ *НЕФТЬ:*\n"
                            f"• Brent: *${brent:.1f}/баррель*\n"
                            f"• Urals (оценка): *~${urals_est:.1f}/баррель* (дисконт ~$12)\n"
                            f"_📌 Бюджет РФ 2025 рассчитан при Urals $69.7. "
                            f"{'Профицит' if urals_est > 69.7 else 'Дефицит'} бюджета вероятен._"
                        )
    except Exception as e:
        logger.warning(f"Brent fallback error: {e}")

    return ""


# ─── 4. Новости РФ — РБК + Коммерсант + Ведомости + Интерфакс ───────────────

async def fetch_russia_news() -> str:
    all_news = []

    rss_feeds = [
        ("РБК Экономика",    "https://rss.rbc.ru/finances/rss.rss"),
        ("РБК Бизнес",       "https://rss.rbc.ru/business/rss.rss"),
        ("РБК Политика",     "https://rss.rbc.ru/politics/rss.rss"),
        ("Коммерсант",       "https://www.kommersant.ru/RSS/main.xml"),
        ("Ведомости",        "https://www.vedomosti.ru/rss/news"),
        ("Интерфакс",        "https://www.interfax.ru/rss.asp"),
    ]

    # Расширенные ключевые слова — законы, налоги, бизнес, санкции
    keywords = [
        # Экономика
        "закон", "налог", "ставк", "цб", "рубл", "инфляц", "ввп",
        "бюджет", "дефицит", "профицит", "нефт", "газ", "экспорт",
        # Бизнес
        "бизнес", "предприниматель", "малый бизнес", "импорт", "льгот",
        "субсид", "кредит", "ипотек", "банкрот", "штраф", "проверк",
        # Политика влияющая на экономику
        "санкц", "минфин", "минэконом", "госдума", "правительств",
        "указ", "постановлени", "регулир", "лицензи",
        # Рынки
        "мосбирж", "акци", "облигац", "офз", "дивиденд", "ipo",
        # Отрасли
        "строительств", "недвижимост", "логистик", "торговл", "розниц",
        "аграр", "сельхоз", "it", "технолог",
    ]

    async with aiohttp.ClientSession(headers=HEADERS) as session:
        for source_name, url in rss_feeds:
            try:
                async with session.get(url, timeout=TIMEOUT) as resp:
                    if resp.status == 200:
                        text = await resp.text()

                        # Пробуем CDATA формат
                        titles = re.findall(r'<title><!\[CDATA\[(.*?)\]\]></title>', text)
                        if not titles:
                            titles = re.findall(r'<title>(.*?)</title>', text)

                        count = 0
                        for title in titles[1:15]:
                            title = title.strip()
                            title = re.sub(r'<[^>]+>', '', title)  # убираем HTML теги
                            if title and any(kw in title.lower() for kw in keywords):
                                all_news.append((source_name, title))
                                count += 1
                            if count >= 3:
                                break

                await asyncio.sleep(0.2)
            except Exception as e:
                logger.warning(f"RSS {source_name} error: {e}")
                continue

    if not all_news:
        return ""

    lines = ["📰 *НОВОСТИ РФ (РБК / Коммерсант / Ведомости / Интерфакс):*"]
    seen = set()
    for source, title in all_news[:10]:
        if title not in seen:
            seen.add(title)
            lines.append(f"• {title} _({source})_")

    return "\n".join(lines)


# ─── 5. Росстат — инфляция РФ ────────────────────────────────────────────────

async def fetch_rosstat_inflation() -> str:
    """Инфляция в России по данным Росстата."""
    try:
        # Росстат публикует еженедельные данные
        url = "https://rosstat.gov.ru/storage/mediabank/Ind_potreb_cen.htm"
        async with aiohttp.ClientSession(headers=HEADERS) as session:
            async with session.get(url, timeout=TIMEOUT) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    # Ищем последние данные по инфляции
                    pct = re.findall(r'(\d+[.,]\d+)\s*%', text)
                    if pct:
                        val = pct[0].replace(",", ".")
                        rf_target = 4.0
                        val_f = float(val)
                        gap = val_f - rf_target
                        gap_str = f"+{gap:.1f}%" if gap > 0 else f"{gap:.1f}%"
                        status = "🔴 выше таргета ЦБ" if gap > 1 else "🟢 близко к таргету"
                        return (
                            f"📊 *ИНФЛЯЦИЯ РФ (Росстат):*\n"
                            f"• Инфляция: *~{val}% годовых* {status}\n"
                            f"  _(таргет ЦБ РФ: 4%, отклонение: {gap_str})_\n"
                            f"_📌 Высокая инфляция = ЦБ держит высокую ставку = дорогие кредиты_"
                        )
    except Exception as e:
        logger.warning(f"Rosstat error: {e}")
    return ""



# ─── 5б. ОФЗ с Мосбиржи (бесплатно) ─────────────────────────────────────────

async def fetch_ofz_yields() -> str:
    """
    Получает доходности ключевых ОФЗ с Мосбиржи ISS API.
    Бесплатно, без авторизации.
    """
    # Ключевые ОФЗ для инвесторов
    OFZ_LIST = {
        "SU26238RMFS4": "ОФЗ 26238 (погаш. 2041)",
        "SU26234RMFS3": "ОФЗ 26234 (погаш. 2025)",
        "SU26229RMFS3": "ОФЗ 26229 (погаш. 2025)",
        "SU29025RMFS2": "ОФЗ 29025 флоатер (RUONIA+)",
    }
    results = []
    try:
        async with aiohttp.ClientSession() as session:
            for isin, name in OFZ_LIST.items():
                try:
                    url = (
                        f"https://iss.moex.com/iss/engines/stock/markets/bonds"
                        f"/securities/{isin}.json?iss.meta=off&iss.only=securities,marketdata"
                    )
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            # Парсим цену и доходность
                            sec = data.get("securities", {})
                            md = data.get("marketdata", {})
                            sec_data = sec.get("data", [])
                            md_data = md.get("data", [])
                            if sec_data and md_data:
                                # Ищем индексы нужных полей
                                sec_cols = [c.upper() for c in sec.get("columns", [])]
                                md_cols = [c.upper() for c in md.get("columns", [])]
                                price_idx = md_cols.index("LAST") if "LAST" in md_cols else None
                                yield_idx = md_cols.index("YIELD") if "YIELD" in md_cols else None
                                if price_idx is not None and md_data[0]:
                                    price = md_data[0][price_idx]
                                    yld = md_data[0][yield_idx] if yield_idx else None
                                    if price:
                                        yld_str = f", доходность {yld:.1f}%" if yld else ""
                                        results.append(f"• {name}: {price:.2f}% от номинала{yld_str}")
                except Exception:
                    pass
    except Exception as e:
        logger.warning(f"OFZ fetch error: {e}")

    if results:
        header = "📋 *ОФЗ НА МОСБИРЖЕ:*"
        footer = "_LQDT/SBMM (фонды денежного рынка) — доходность ~ключевая ставка ЦБ_"
        return header + "\n" + "\n".join(results) + "\n" + footer
    return ""


# ─── 5в. Бюджетный калькулятор РФ ────────────────────────────────────────────

def calc_budget_balance(urals_price: float, budget_price: float = 69.7) -> str:
    """
    Считает профицит/дефицит бюджета РФ на основе цены Urals.
    budget_price — цена нефти заложенная в бюджет РФ (по умолчанию $69.7)
    """
    daily_barrels = 3_500_000  # ~3.5 млн барр/день экспорт
    diff = urals_price - budget_price
    daily_usd = diff * daily_barrels
    annual_usd = daily_usd * 365
    annual_rub = annual_usd * 90  # примерный курс для расчёта

    if diff > 0:
        status = f"ПРОФИЦИТ +${daily_usd/1_000_000:.0f} млн/день (+{annual_rub/1_000_000_000_000:.1f} трлн₽/год)"
        tax_risk = "низкий — бюджет в плюсе, новые налоги маловероятны"
    elif diff > -10:
        status = f"БЛИЗКО К НУЛЮ (${daily_usd/1_000_000:.0f} млн/день)"
        tax_risk = "средний — бюджет на грани, следи за новостями"
    else:
        status = f"ДЕФИЦИТ ${abs(daily_usd)/1_000_000:.0f} млн/день"
        tax_risk = "высокий — возможны новые налоги или секвестр расходов"

    return (
        f"🛢️ *БЮДЖЕТ РФ КАЛЬКУЛЯТОР:*\n"
        f"• Цена Urals: ${urals_price:.1f}/барр | Бюджет заложен при: ${budget_price}/барр\n"
        f"• Баланс: {status}\n"
        f"• Налоговый риск для бизнеса: {tax_risk}\n"
        f"_Если Urals упадёт ниже $70 на 6+ мес → риск новых налогов_"
    )

# ─── 6. Главная функция ───────────────────────────────────────────────────────

async def fetch_russia_context() -> str:
    """Собирает все РФ данные параллельно."""
    logger.info("🇷🇺 Собираю расширенный контекст РФ...")

    results = await asyncio.gather(
        fetch_cbr_data(),
        fetch_moex_data(),
        fetch_urals_oil(),
        fetch_rosstat_inflation(),
        fetch_russia_news(),
        fetch_ofz_yields(),
        return_exceptions=True
    )

    sections = []
    labels = ["ЦБ РФ", "Мосбиржа", "Нефть Urals", "Инфляция РФ", "Новости РФ", "ОФЗ"]
    for label, result in zip(labels, results):
        if isinstance(result, str) and result.strip():
            sections.append(result)
        elif isinstance(result, Exception):
            logger.warning(f"Russia {label} error: {result}")

    # Добавляем бюджетный калькулятор если есть цена Urals
    urals_price = None
    for section in sections:
        import re
        m = re.search(r'Urals.*?\$(\d+\.?\d*)', section)
        if m:
            try:
                urals_price = float(m.group(1))
            except:
                pass
    if urals_price and urals_price > 30:
        budget_section = calc_budget_balance(urals_price)
        sections.append(budget_section)

    if not sections:
        return "Данные по РФ временно недоступны."

    now = datetime.now().strftime("%d.%m.%Y %H:%M")
    header = f"=== КОНТЕКСТ РФ ({now}) ===\n"
    return header + "\n\n".join(sections)
