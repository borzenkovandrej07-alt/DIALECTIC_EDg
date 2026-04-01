"""News Provider Implementation - Tavily Web Search"""

import asyncio
import logging
from dataclasses import dataclass
from typing import Optional, List, Dict, Any
from datetime import datetime

try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False

logger = logging.getLogger(__name__)

if not AIOHTTP_AVAILABLE:
    logger.warning("aiohttp not installed. News provider will not work.")


@dataclass
class NewsArticle:
    """Статья новости"""
    title: str
    content: str
    url: str
    source: str
    publish_date: datetime
    relevance_score: float = 0.0


class TavilyProvider:
    """Tavily реальный поиск новостей и веб"""

    def __init__(
        self,
        api_key: str,
        base_url: str = "https://api.tavily.com/search",
        timeout: int = 15,
    ):
        self.api_key = api_key
        self.base_url = base_url
        self.timeout = timeout
        self.session: Optional[aiohttp.ClientSession] = None

    async def initialize(self) -> None:
        """Инициализация сессии"""
        if not self.session:
            self.session = aiohttp.ClientSession()

    async def close(self) -> None:
        """Закрытие сессии"""
        if self.session:
            await self.session.close()

    async def search_news(
        self,
        query: str,
        include_answer: bool = True,
        max_results: int = 10,
        search_depth: str = "advanced",
    ) -> List[NewsArticle]:
        """
        Поиск новостей по запросу.

        Args:
            query: Поисковый запрос
            include_answer: Включить ответ Tavily AI
            max_results: Максимум результатов (1-100)
            search_depth: "basic" или "advanced"

        Returns:
            Список статей новостей
        """
        await self.initialize()

        payload = {
            "api_key": self.api_key,
            "query": query,
            "include_answer": include_answer,
            "max_results": max(1, min(max_results, 100)),
            "search_depth": search_depth,
        }

        try:
            async with self.session.post(
                self.base_url,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=self.timeout),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()

                    articles = []
                    for result in data.get("results", []):
                        try:
                            article = NewsArticle(
                                title=result.get("title", ""),
                                content=result.get("content", ""),
                                url=result.get("url", ""),
                                source=result.get("source", "Unknown"),
                                publish_date=self._parse_date(
                                    result.get("published_date", "")
                                ),
                                relevance_score=result.get("score", 0.0),
                            )
                            articles.append(article)
                        except Exception as e:
                            logger.warning(f"Failed to parse article: {e}")
                            continue

                    logger.info(
                        f"✅ Found {len(articles)} articles for: {query}"
                    )
                    return articles
                else:
                    logger.error(f"Tavily search error: {resp.status}")
                    return []

        except asyncio.TimeoutError:
            logger.warning(f"Tavily search timeout for: {query}")
            return []
        except Exception as e:
            logger.error(f"Tavily search failed: {e}")
            return []

    async def search_real_time(
        self,
        query: str,
        topic: str = "general",  # "news", "general", or "tweets"
        max_results: int = 10,
    ) -> List[NewsArticle]:
        """Реальное время поиск (работает только с Pro плана)"""
        payload = {
            "api_key": self.api_key,
            "query": query,
            "topic": topic,
            "max_results": max(1, min(max_results, 100)),
            "search_depth": "advanced",
        }

        await self.initialize()

        try:
            async with self.session.post(
                f"{self.base_url}",
                json=payload,
                timeout=aiohttp.ClientTimeout(total=self.timeout),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()

                    articles = []
                    for result in data.get("results", []):
                        try:
                            article = NewsArticle(
                                title=result.get("title", ""),
                                content=result.get("content", ""),
                                url=result.get("url", ""),
                                source=result.get("source", ""),
                                publish_date=self._parse_date(
                                    result.get("published_date", "")
                                ),
                                relevance_score=result.get("score", 0.0),
                            )
                            articles.append(article)
                        except Exception as e:
                            logger.warning(f"Failed to parse article: {e}")

                    return articles
                else:
                    return []

        except asyncio.TimeoutError:
            return []
        except Exception as e:
            logger.warning(f"Real-time search failed: {e}")
            return []

    def _parse_date(self, date_str: str) -> datetime:
        """Парсинг даты из разных форматов"""
        if not date_str:
            return datetime.now()

        try:
            # Сначала пробуем ISO формат
            return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        except ValueError:
            pass

        try:
            # Потом стандартный формат
            return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            pass

        # Если не удалось - возвращаем текущее время
        logger.warning(f"Could not parse date: {date_str}")
        return datetime.now()

    async def get_news_summary(
        self,
        query: str,
        max_results: int = 5,
    ) -> Dict[str, Any]:
        """Получить ответ + новости одновременно"""
        articles = await self.search_news(
            query, include_answer=True, max_results=max_results
        )

        await self.initialize()

        payload = {
            "api_key": self.api_key,
            "query": query,
            "include_answer": True,
            "max_results": max_results,
            "search_depth": "advanced",
        }

        try:
            async with self.session.post(
                self.base_url,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=self.timeout),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return {
                        "answer": data.get("answer", ""),
                        "articles": articles,
                        "query_processed": data.get("query"),
                    }
        except Exception as e:
            logger.warning(f"Summary fetch failed: {e}")

        return {
            "answer": "",
            "articles": articles,
            "query_processed": query,
        }


class NewsCache:
    """Кэш новостей чтобы не перезапрашивать одно и то же"""

    def __init__(self, max_age_seconds: int = 3600):  # 1 час
        self.cache: Dict[str, tuple] = {}  # {query: (timestamp, articles)}
        self.max_age = max_age_seconds

    async def get(self, query: str) -> Optional[List[NewsArticle]]:
        """Получить кэшированные новости"""
        if query not in self.cache:
            return None

        timestamp, articles = self.cache[query]
        age = datetime.now().timestamp() - timestamp

        if age > self.max_age:
            del self.cache[query]
            return None

        logger.debug(f"📰 Cache HIT: {query} ({age:.0f}s old)")
        return articles

    async def set(self, query: str, articles: List[NewsArticle]) -> None:
        """Сохранить новости в кэш"""
        self.cache[query] = (datetime.now().timestamp(), articles)
        logger.debug(f"📰 Cache SET: {query}")

    async def clear(self) -> None:
        """Очистить весь кэш"""
        self.cache.clear()
        logger.info("📰 Cache cleared")

    def __len__(self) -> int:
        return len(self.cache)
