"""Cache Provider Implementations - Redis with FileCache Fallback"""

import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Optional, Any, Dict

try:
    import aioredis
    AIOREDIS_AVAILABLE = True
except ImportError:
    AIOREDIS_AVAILABLE = False
    logger = logging.getLogger(__name__)
    logger.warning("aioredis not installed, Redis provider will not work")

logger = logging.getLogger(__name__)


class RedisProvider:
    """Redis-based кэш с TTL поддержкой"""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        password: Optional[str] = None,
        default_ttl: int = 3600,  # 1 час
    ):
        if not AIOREDIS_AVAILABLE:
            logger.warning("aioredis is not installed. Redis provider will not work.")
        
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.default_ttl = default_ttl
        self.redis: Optional[aioredis.Redis] = None

    async def connect(self) -> None:
        """Подключение к Redis"""
        try:
            self.redis = await aioredis.from_url(
                f"redis://{self.host}:{self.port}/{self.db}",
                password=self.password,
                encoding="utf8",
                decode_responses=True,
            )
            logger.info(f"✅ Redis connected: {self.host}:{self.port}")
        except Exception as e:
            logger.error(f"❌ Redis connection failed: {e}")
            self.redis = None

    async def close(self) -> None:
        """Закрытие подключения"""
        if self.redis:
            await self.redis.close()

    async def get(self, key: str) -> Optional[Any]:
        """Получение значения из кэша"""
        if not self.redis:
            return None

        try:
            value = await self.redis.get(key)
            if value:
                # Пробуем парсить JSON
                try:
                    return json.loads(value)
                except json.JSONDecodeError:
                    return value
            return None
        except Exception as e:
            logger.warning(f"Redis GET error: {e}")
            return None

    async def set(
        self, key: str, value: Any, ttl: Optional[int] = None
    ) -> bool:
        """Установка значения в кэш"""
        if not self.redis:
            return False

        try:
            # Сериализуем значение если это не строка
            if not isinstance(value, str):
                value = json.dumps(value)

            ttl = ttl or self.default_ttl
            await self.redis.setex(key, ttl, value)
            return True
        except Exception as e:
            logger.warning(f"Redis SET error: {e}")
            return False

    async def delete(self, key: str) -> bool:
        """Удаление ключа"""
        if not self.redis:
            return False

        try:
            result = await self.redis.delete(key)
            return result > 0
        except Exception as e:
            logger.warning(f"Redis DELETE error: {e}")
            return False

    async def exists(self, key: str) -> bool:
        """Проверка наличия ключа"""
        if not self.redis:
            return False

        try:
            result = await self.redis.exists(key)
            return result > 0
        except Exception as e:
            logger.warning(f"Redis EXISTS error: {e}")
            return False

    async def clear_pattern(self, pattern: str) -> int:
        """Удаление всех ключей по паттерну"""
        if not self.redis:
            return 0

        try:
            keys = await self.redis.keys(pattern)
            if keys:
                return await self.redis.delete(*keys)
            return 0
        except Exception as e:
            logger.warning(f"Redis CLEAR_PATTERN error: {e}")
            return 0

    async def health_check(self) -> bool:
        """Проверка здоровья Redis"""
        if not self.redis:
            return False

        try:
            await self.redis.ping()
            return True
        except Exception as e:
            logger.warning(f"Redis health check failed: {e}")
            return False


class FileCache:
    """Файловый кэш (JSON) - fallback для Redis"""

    def __init__(
        self,
        cache_dir: str = ".cache",
        default_ttl: int = 86400,  # 24 часа
    ):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        self.default_ttl = default_ttl
        self.metadata_file = self.cache_dir / ".metadata.json"

    def _get_cache_file(self, key: str) -> Path:
        """Получить путь файла кэша для ключа"""
        # Хэшируем ключ для безопасности
        safe_key = key.replace("/", "_").replace(":", "_")[:100]
        return self.cache_dir / f"{safe_key}.json"

    def _load_metadata(self) -> Dict[str, float]:
        """Загрузить метаданные TTL"""
        if self.metadata_file.exists():
            try:
                with open(self.metadata_file, "r") as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Metadata load error: {e}")
        return {}

    def _save_metadata(self, metadata: Dict[str, float]) -> None:
        """Сохранить метаданные TTL"""
        try:
            with open(self.metadata_file, "w") as f:
                json.dump(metadata, f)
        except Exception as e:
            logger.warning(f"Metadata save error: {e}")

    async def get(self, key: str) -> Optional[Any]:
        """Получение из файлового кэша"""
        cache_file = self._get_cache_file(key)

        if not cache_file.exists():
            return None

        try:
            metadata = self._load_metadata()
            expire_time = metadata.get(key, 0)

            # Проверяем TTL
            if time.time() > expire_time:
                cache_file.unlink()
                return None

            # Загружаем данные
            with open(cache_file, "r") as f:
                data = json.load(f)
            return data

        except Exception as e:
            logger.warning(f"FileCache GET error: {e}")
            return None

    async def set(
        self, key: str, value: Any, ttl: Optional[int] = None
    ) -> bool:
        """Установка в файловый кэш"""
        cache_file = self._get_cache_file(key)
        ttl = ttl or self.default_ttl

        try:
            # Сохраняем данные
            with open(cache_file, "w") as f:
                json.dump(value, f)

            # Обновляем метаданные
            metadata = self._load_metadata()
            metadata[key] = time.time() + ttl
            self._save_metadata(metadata)

            return True
        except Exception as e:
            logger.warning(f"FileCache SET error: {e}")
            return False

    async def delete(self, key: str) -> bool:
        """Удаление из файлового кэша"""
        cache_file = self._get_cache_file(key)

        try:
            if cache_file.exists():
                cache_file.unlink()

            metadata = self._load_metadata()
            if key in metadata:
                del metadata[key]
                self._save_metadata(metadata)

            return True
        except Exception as e:
            logger.warning(f"FileCache DELETE error: {e}")
            return False

    async def exists(self, key: str) -> bool:
        """Проверка наличия в кэше"""
        cache_file = self._get_cache_file(key)

        if not cache_file.exists():
            return False

        try:
            metadata = self._load_metadata()
            expire_time = metadata.get(key, 0)

            if time.time() > expire_time:
                cache_file.unlink()
                return False

            return True
        except Exception as e:
            logger.warning(f"FileCache EXISTS error: {e}")
            return False

    async def clear_expired(self) -> int:
        """Удаление истёкших записей"""
        cleaned = 0
        metadata = self._load_metadata()
        current_time = time.time()

        keys_to_delete = []
        for key, expire_time in metadata.items():
            if current_time > expire_time:
                cache_file = self._get_cache_file(key)
                if cache_file.exists():
                    try:
                        cache_file.unlink()
                        cleaned += 1
                        keys_to_delete.append(key)
                    except Exception as e:
                        logger.warning(f"File cleanup error: {e}")

        # Обновляем метаданные
        for key in keys_to_delete:
            del metadata[key]
        self._save_metadata(metadata)

        return cleaned


class CacheChain:
    """
    Двухуровневый кэш: Redis → FileCache.
    Redis для быстрого доступа, FileCache для persist между перезагрузками.
    """

    def __init__(
        self,
        redis: Optional[RedisProvider] = None,
        file_cache: Optional[FileCache] = None,
    ):
        self.redis = redis
        self.file_cache = file_cache or FileCache()

    async def get(self, key: str) -> Optional[Any]:
        """Получение с приоритетом Redis"""
        # Сначала Redis
        if self.redis:
            value = await self.redis.get(key)
            if value is not None:
                logger.debug(f"Cache HIT (Redis): {key}")
                return value

        # Потом FileCache
        if self.file_cache:
            value = await self.file_cache.get(key)
            if value is not None:
                logger.debug(f"Cache HIT (File): {key}")
                # Закэшируем в Redis для следующего раза
                if self.redis:
                    await self.redis.set(key, value)
                return value

        logger.debug(f"Cache MISS: {key}")
        return None

    async def set(
        self, key: str, value: Any, ttl: Optional[int] = None
    ) -> bool:
        """Установка в оба слоя"""
        results = []

        if self.redis:
            results.append(await self.redis.set(key, value, ttl))

        if self.file_cache:
            results.append(await self.file_cache.set(key, value, ttl))

        return any(results)

    async def delete(self, key: str) -> bool:
        """Удаление из обоих слоёв"""
        results = []

        if self.redis:
            results.append(await self.redis.delete(key))

        if self.file_cache:
            results.append(await self.file_cache.delete(key))

        return any(results)

    async def exists(self, key: str) -> bool:
        """Проверка в обоих слоях"""
        if self.redis and await self.redis.exists(key):
            return True

        if self.file_cache and await self.file_cache.exists(key):
            return True

        return False

    def __repr__(self) -> str:
        redis_status = "✅" if self.redis else "❌"
        file_status = "✅" if self.file_cache else "❌"
        return f"CacheChain(Redis{redis_status} → File{file_status})"
