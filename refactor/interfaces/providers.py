"""
Abstract interfaces and protocols for Dialectic Edge
Унифицированные интерфейсы для всех слоёв системы
"""

from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, List
from datetime import datetime
from enum import Enum


# ─── Cache Provider Interface ──────────────────────────────────────────────

class CacheProvider(ABC):
    """Unified cache interface (Redis, Memcached, or in-memory)"""
    
    @abstractmethod
    async def get(self, key: str) -> Optional[str]:
        """Получить значение из кэша"""
        pass
    
    @abstractmethod
    async def set(self, key: str, value: str, ttl_seconds: Optional[int] = None) -> bool:
        """Установить значение в кэш с TTL"""
        pass
    
    @abstractmethod
    async def delete(self, key: str) -> bool:
        """Удалить значение из кэша"""
        pass
    
    @abstractmethod
    async def exists(self, key: str) -> bool:
        """Проверить наличие ключа"""
        pass
    
    @abstractmethod
    async def ping(self) -> bool:
        """Проверить подключение"""
        pass


class RedisProvider(CacheProvider):
    """Redis cache implementation"""
    
    def __init__(self, redis_url: str):
        self.redis_url = redis_url
        self.client = None
    
    async def get(self, key: str) -> Optional[str]:
        # Implementation
        pass
    
    async def set(self, key: str, value: str, ttl_seconds: Optional[int] = None) -> bool:
        # Implementation
        pass
    
    async def delete(self, key: str) -> bool:
        # Implementation
        pass
    
    async def exists(self, key: str) -> bool:
        # Implementation
        pass
    
    async def ping(self) -> bool:
        # Implementation
        pass


class FileCache(CacheProvider):
    """File-based cache (JSON/pickle) for fallback"""
    
    def __init__(self, cache_dir: str):
        self.cache_dir = cache_dir
    
    async def get(self, key: str) -> Optional[str]:
        # Implementation
        pass
    
    async def set(self, key: str, value: str, ttl_seconds: Optional[int] = None) -> bool:
        # Implementation
        pass
    
    async def delete(self, key: str) -> bool:
        # Implementation
        pass
    
    async def exists(self, key: str) -> bool:
        # Implementation
        pass
    
    async def ping(self) -> bool:
        # Implementation
        pass


# ─── Database Provider Interface ───────────────────────────────────────────

class DatabaseProvider(ABC):
    """Unified database interface (SQLite, PostgreSQL, etc)"""
    
    @abstractmethod
    async def execute(self, query: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Выполнить SQL запрос"""
        pass
    
    @abstractmethod
    async def insert(self, table: str, data: Dict[str, Any]) -> int:
        """Вставить строку (возвращает ID)"""
        pass
    
    @abstractmethod
    async def update(self, table: str, where: Dict[str, Any], data: Dict[str, Any]) -> int:
        """Обновить строки"""
        pass
    
    @abstractmethod
    async def query(self, table: str, where: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """Запросить строки"""
        pass
    
    @abstractmethod
    async def delete(self, table: str, where: Dict[str, Any]) -> int:
        """Удалить строки"""
        pass
    
    @abstractmethod
    async def close(self):
        """Закрыть соединение"""
        pass


class SQLiteProvider(DatabaseProvider):
    """SQLite database implementation"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = None
    
    async def execute(self, query: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        # Implementation
        pass
    
    async def insert(self, table: str, data: Dict[str, Any]) -> int:
        # Implementation
        pass
    
    async def update(self, table: str, where: Dict[str, Any], data: Dict[str, Any]) -> int:
        # Implementation
        pass
    
    async def query(self, table: str, where: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        # Implementation
        pass
    
    async def delete(self, table: str, where: Dict[str, Any]) -> int:
        # Implementation
        pass
    
    async def close(self):
        # Implementation
        pass


# ─── Market Data Provider Interface ────────────────────────────────────────

class MarketDataProvider(ABC):
    """Unified market data interface (Binance, Yahoo Finance, etc)"""
    
    @abstractmethod
    async def get_price(self, symbol: str) -> Dict[str, Any]:
        """Получить текущую цену"""
        pass
    
    @abstractmethod
    async def get_ohlcv(self, symbol: str, timeframe: str) -> List[Dict[str, Any]]:
        """Получить свечи (Open, High, Low, Close, Volume)"""
        pass
    
    @abstractmethod
    async def get_order_book(self, symbol: str, limit: int = 20) -> Dict[str, Any]:
        """Получить стакан"""
        pass


class BinanceProvider(MarketDataProvider):
    """Binance market data implementation"""
    
    async def get_price(self, symbol: str) -> Dict[str, Any]:
        # Implementation
        pass
    
    async def get_ohlcv(self, symbol: str, timeframe: str) -> List[Dict[str, Any]]:
        # Implementation
        pass
    
    async def get_order_book(self, symbol: str, limit: int = 20) -> Dict[str, Any]:
        # Implementation
        pass


class YahooFinanceProvider(MarketDataProvider):
    """Yahoo Finance market data implementation"""
    
    async def get_price(self, symbol: str) -> Dict[str, Any]:
        # Implementation
        pass
    
    async def get_ohlcv(self, symbol: str, timeframe: str) -> List[Dict[str, Any]]:
        # Implementation
        pass
    
    async def get_order_book(self, symbol: str, limit: int = 20) -> Dict[str, Any]:
        # Implementation
        pass


# ─── News & Sentiment Provider Interface ───────────────────────────────────

class NewsProvider(ABC):
    """Unified news data interface"""
    
    @abstractmethod
    async def fetch_news(self, query: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Получить новости по поисковому запросу"""
        pass
    
    @abstractmethod
    async def get_headlines(self, market: str, limit: int = 5) -> List[Dict[str, Any]]:
        """Получить заголовки новостей для рынка"""
        pass


class TavilyProvider(NewsProvider):
    """Tavily web search implementation"""
    
    async def fetch_news(self, query: str, limit: int = 10) -> List[Dict[str, Any]]:
        # Implementation
        pass
    
    async def get_headlines(self, market: str, limit: int = 5) -> List[Dict[str, Any]]:
        # Implementation
        pass


# ─── AI Provider Interface ────────────────────────────────────────────────

class AIProvider(ABC):
    """Unified LLM interface"""
    
    @abstractmethod
    async def generate(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.7,
        max_tokens: int = 2000,
    ) -> str:
        """Генерировать текст"""
        pass
    
    @abstractmethod
    async def stream(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.7,
        max_tokens: int = 2000,
    ):
        """Streamed генерация"""
        pass


# ─── Report Storage Interface ──────────────────────────────────────────────

class ReportStorage(ABC):
    """Unified report persistence interface"""
    
    @abstractmethod
    async def save_report(self, report_id: str, report: Dict[str, Any]) -> bool:
        """Сохранить отчёт"""
        pass
    
    @abstractmethod
    async def get_report(self, report_id: str) -> Optional[Dict[str, Any]]:
        """Получить отчёт"""
        pass
    
    @abstractmethod
    async def list_reports(self, market: str, limit: int = 10) -> List[str]:
        """Список отчётов"""
        pass
    
    @abstractmethod
    async def delete_report(self, report_id: str) -> bool:
        """Удалить отчёт"""
        pass


# ─── Enum for Provider Types ──────────────────────────────────────────────

class ProviderType(str, Enum):
    """Типы провайдеров в системе"""
    REDIS = "redis"
    SQLITE = "sqlite"
    FILE = "file"
    BINANCE = "binance"
    YAHOO = "yahoo"
    TAVILY = "tavily"
    OPENROUTER = "openrouter"
    TOGETHER = "together"
    GROQ = "groq"
