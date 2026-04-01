"""
news_fetcher.py v2.0 — Сбор новостей через RSS.

АРХИТЕКТУРА v2.0:
- Глобальные рынки: BBC, Guardian, MarketWatch, CNBC, Yahoo, FT, CoinDesk, Cointelegraph
- Геополитика внешняя: Bloomberg, AP, ISW + Reuters/AFP через Google News proxy
- РФ внутренняя: ТАСС, Интерфакс + РБК/Коммерсант/Ведомости/РИА через Google News proxy
- Google News proxy решает проблему блокировки российских доменов на Railway Amsterdam
"""

import asyncio
import logging
import re
from datetime import datetime

import aiohttp
import feedparser

from config import MAX_NEWS_PER_FEED, MAX_TOTAL_NEWS, NEWS_API_KEY

logger = logging.getLogger(__name__)

# ── Глобальные рынки ──────────────────────────────────────────────────────────
RSS_FEEDS_GLOBAL = {
    "BBC Business":      "https://feeds.bbci.co.uk/news/business/rss.xml",
    "Guardian Business": "https://www.theguardian.com/business/rss",
    "Guardian World":    "https://www.theguardian.com/world/rss",
    "MarketWatch":       "https://feeds.marketwatch.com/marketwatch/topstories/",
    "CNBC Markets":      "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=15839135",
    "Yahoo Finance":     "https://finance.yahoo.com/news/rssindex",
    "Investing.com Eco": "https://www.investing.com/rss/news_14.rss",
    "FT Markets":        "https://www.ft.com/rss/home/uk",
    "CoinDesk":          "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "Cointelegraph":     "https://cointelegraph.com/rss",
}

# ── Геополитика — прямые RSS (работают с Railway Amsterdam) ──────────────────
RSS_FEEDS_GEO_DIRECT = {
    "Bloomberg Markets":  "https://feeds.bloomberg.com/markets/news.rss",
    "Bloomberg Politics": "https://feeds.bloomberg.com/politics/news.rss",
    "AP World":           "https://apnews.com/world-news.rss",
    "AP Business":        "https://apnews.com/business.rss",
    "ISW":                "https://www.understandingwar.org/feeds/all",
}

# ── Геополитика — Google News proxy (для заблокированных) ────────────────────
RSS_FEEDS_GEO_PROXY = {
    "Reuters World":   "https://news.google.com/rss/search?q=when:24h+allinurl:reuters.com+world&hl=en&gl=US&ceid=US:en",
    "Reuters Markets": "https://news.google.com/rss/search?q=when:24h+allinurl:reuters.com+markets&hl=en&gl=US&ceid=US:en",
    "AFP":             "https://news.google.com/rss/search?q=when:24h+AFP+world+news&hl=en&gl=US&ceid=US:en",
}

# ── РФ внутренняя — прямые RSS (подтверждено работают) ───────────────────────
RSS_FEEDS_RF_DIRECT = {
    "ТАСС":         "https://tass.ru/rss/v2.xml",
    "Интерфакс":    "https://www.interfax.ru/rss.asp",
    "Консультант+": "https://www.consultant.ru/rss/hotdocs.xml",
    "Гарант":       "https://www.garant.ru/files/rss/prime.xml",
}

# ── РФ внутренняя — Google News proxy (для заблокированных) ─────────────────
RSS_FEEDS_RF_PROXY = {
    "РБК":        "https://news.google.com/rss/search?q=when:24h+allinurl:rbc.ru&hl=ru&gl=RU&ceid=RU:ru",
    "Коммерсант": "https://news.google.com/rss/search?q=when:24h+allinurl:kommersant.ru&hl=ru&gl=RU&ceid=RU:ru",
    "Ведомости":  "https://news.google.com/rss/search?q=when:24h+allinurl:vedomosti.ru&hl=ru&gl=RU&ceid=RU:ru",
    "РИА":        "https://news.google.com/rss/search?q=when:24h+allinurl:ria.ru+экономика&hl=ru&gl=RU&ceid=RU:ru",
}

# ── Ключевые слова ────────────────────────────────────────────────────────────
RF_KEYWORDS = [
    "закон", "налог", "ставк", "цб", "рубл", "инфляц", "ввп",
    "бюджет", "дефицит", "профицит", "нефт", "газ", "экспорт",
    "бизнес", "предприниматель", "импорт", "льгот", "субсид",
    "кредит", "ипотек", "банкрот", "штраф", "санкц",
    "минфин", "госдума", "правительств", "указ",
    "мосбирж", "акци", "облигац", "офз", "дивиденд",
    "экономик", "торговл", "логистик", "недвижимост",
]

GEO_KEYWORDS = [
    "russia", "ukraine", "war", "sanctions", "nato", "conflict",
    "military", "missile", "ceasefire", "kremlin", "putin",
    "zelenskyy", "trump", "china", "iran", "energy", "oil", "gas",
    "россия", "украина", "война", "санкции", "нато", "фронт",
    "наступлен", "обстрел", "перемири",
]


class NewsItem:
    def __init__(self, title: str, summary: str, source: str,
                 link: str = "", published: str = "", category: str = "global"):
        self.title    = title.strip()
        self.summary  = self._clean(summary)[:400]
        self.source   = source
        self.link     = link
        self.published = published
        self.category = category  # global | geopolitics | rf_internal

    @staticmethod
    def _clean(text: str) -> str:
        text = re.sub(r"<[^>]+>", " ", text)
        text = re.sub(r"\s+", " ", text)
        return text.strip()

    def to_text(self) -> str:
        return f"[{self.source}] {self.title}. {self.summary}"

    def to_formatted(self) -> str:
        return f"• *{self.title}*\n  _{self.source}_ | {self.published}"


class NewsFetcher:
    """Асинхронный сборщик новостей с разделением на категории."""

    async def fetch_all(self) -> str:
        tasks = []

        # Глобальные рынки — без фильтрации
        for name, url in RSS_FEEDS_GLOBAL.items():
            tasks.append(self._fetch_rss(name, url, category="global"))

        # Геополитика — прямые, без фильтрации
        for name, url in RSS_FEEDS_GEO_DIRECT.items():
            tasks.append(self._fetch_rss(name, url, category="geopolitics"))

        # Геополитика — прокси, с фильтрацией
        for name, url in RSS_FEEDS_GEO_PROXY.items():
            tasks.append(self._fetch_rss_filtered(name, url, GEO_KEYWORDS, category="geopolitics"))

        # РФ внутренняя — прямые, с фильтрацией
        for name, url in RSS_FEEDS_RF_DIRECT.items():
            tasks.append(self._fetch_rss_filtered(name, url, RF_KEYWORDS, category="rf_internal"))

        # РФ внутренняя — прокси, с фильтрацией
        for name, url in RSS_FEEDS_RF_PROXY.items():
            tasks.append(self._fetch_rss_filtered(name, url, RF_KEYWORDS, category="rf_internal"))

        # NewsAPI если есть ключ
        if NEWS_API_KEY:
            tasks.append(self._fetch_newsapi())

        results = await asyncio.gather(*tasks, return_exceptions=True)

        all_items: list[NewsItem] = []
        for result in results:
            if isinstance(result, Exception):
                logger.warning(f"Feed error: {result}")
                continue
            if isinstance(result, list):
                all_items.extend(result)

        # Дедупликация
        seen = set()
        unique_items = []
        for item in all_items:
            key = item.title.lower()[:60]
            if key not in seen:
                seen.add(key)
                unique_items.append(item)

        unique_items = unique_items[:MAX_TOTAL_NEWS]

        if not unique_items:
            return "Новости не удалось загрузить. Используй /analyze [текст новости] для ручного ввода."

        return self._build_context(unique_items)

    async def _fetch_rss(self, source_name: str, url: str,
                         category: str = "global") -> list[NewsItem]:
        try:
            headers = {"User-Agent": "Mozilla/5.0 (compatible; DialecticEdgeBot/1.0)"}
            async with aiohttp.ClientSession(headers=headers) as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    content = await resp.text(errors="replace")

            feed  = feedparser.parse(content)
            items = []

            for entry in feed.entries[:MAX_NEWS_PER_FEED]:
                title     = entry.get("title", "")
                summary   = entry.get("summary", entry.get("description", ""))
                link      = entry.get("link", "")
                published = entry.get("published", "")
                if title:
                    items.append(NewsItem(
                        title=title, summary=summary, source=source_name,
                        link=link, published=published[:16] if published else "",
                        category=category,
                    ))

            logger.info(f"RSS {source_name}: получено {len(items)} новостей")
            return items

        except Exception as e:
            logger.warning(f"RSS {source_name}: {e}")
            return []

    async def _fetch_rss_filtered(self, source_name: str, url: str,
                                   keywords: list[str],
                                   category: str = "global") -> list[NewsItem]:
        try:
            headers = {"User-Agent": "Mozilla/5.0 (compatible; DialecticEdgeBot/1.0)"}
            async with aiohttp.ClientSession(headers=headers) as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    content = await resp.text(errors="replace")

            feed  = feedparser.parse(content)
            items = []
            count = 0

            for entry in feed.entries[:25]:
                title     = entry.get("title", "")
                summary   = entry.get("summary", entry.get("description", ""))
                link      = entry.get("link", "")
                published = entry.get("published", "")

                if not title:
                    continue

                text_lower = (title + " " + summary).lower()
                if not any(kw in text_lower for kw in keywords):
                    continue

                items.append(NewsItem(
                    title=title, summary=summary, source=source_name,
                    link=link, published=published[:16] if published else "",
                    category=category,
                ))
                count += 1
                if count >= MAX_NEWS_PER_FEED:
                    break

            logger.info(f"RSS {source_name}: получено {count} новостей")
            return items

        except Exception as e:
            logger.warning(f"RSS {source_name}: {e}")
            return []

    async def _fetch_newsapi(self) -> list[NewsItem]:
        try:
            params = {
                "apiKey": NEWS_API_KEY,
                "category": "business",
                "language": "en",
                "pageSize": MAX_NEWS_PER_FEED,
            }
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    "https://newsapi.org/v2/top-headlines",
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=15)
                ) as resp:
                    data = await resp.json()

            items = []
            for article in data.get("articles", []):
                title = article.get("title", "")
                desc  = article.get("description", "")
                src   = article.get("source", {}).get("name", "NewsAPI")
                if title and "[Removed]" not in title:
                    items.append(NewsItem(
                        title=title, summary=desc or "",
                        source=src, link=article.get("url", ""),
                        category="global",
                    ))

            logger.info(f"NewsAPI: получено {len(items)} новостей")
            return items

        except Exception as e:
            logger.warning(f"NewsAPI error: {e}")
            return []

    def _build_context(self, items: list[NewsItem]) -> str:
        now = datetime.now().strftime("%d %B %Y, %H:%M UTC")

        global_items = [i for i in items if i.category == "global"]
        geo_items    = [i for i in items if i.category == "geopolitics"]
        rf_items     = [i for i in items if i.category == "rf_internal"]

        lines = [
            f"=== НОВОСТИ И ГЕОПОЛИТИКА ({now}) ===",
            f"Источников: {len(set(i.source for i in items))} | Новостей: {len(items)}",
            "",
        ]

        # Глобальные рынки
        if global_items:
            lines.append("── ГЛОБАЛЬНЫЕ РЫНКИ ──")
            by_source: dict[str, list[NewsItem]] = {}
            for item in global_items:
                by_source.setdefault(item.source, []).append(item)
            for source, src_items in by_source.items():
                lines.append(f"--- {source} ---")
                for item in src_items:
                    lines.append(f"• {item.title}")
                    if item.summary:
                        lines.append(f"  {item.summary[:200]}")
            lines.append("")

        # Геополитика
        if geo_items:
            lines.append("── ГЕОПОЛИТИКА (Bloomberg / AP / Reuters / ISW / AFP) ──")
            for item in geo_items:
                lines.append(f"• [{item.source}] {item.title}")
                if item.summary:
                    lines.append(f"  {item.summary[:200]}")
            lines.append("")

        # РФ внутренняя
        if rf_items:
            lines.append("── РФ ВНУТРЕННЯЯ (ТАСС / Интерфакс / РБК / Коммерсант / Ведомости / РИА) ──")
            for item in rf_items:
                lines.append(f"• [{item.source}] {item.title}")
                if item.summary:
                    lines.append(f"  {item.summary[:200]}")
            lines.append("")

        return "\n".join(lines)
