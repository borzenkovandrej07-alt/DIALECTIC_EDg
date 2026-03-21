"""
Опциональное хранилище дебатов в Redis.
Если REDIS_URL задан (например, Railway Redis addon) — снимки переживают
рестарт контейнера и работают при нескольких воркерах.
"""
import logging
import os

logger = logging.getLogger(__name__)

REDIS_URL = os.getenv("REDIS_URL", "")
_redis = None

try:
    from config import DEBATE_SNAPSHOT_HOURS
except ImportError:
    DEBATE_SNAPSHOT_HOURS = 72

DEBATE_TTL_SEC = DEBATE_SNAPSHOT_HOURS * 3600


def _get_redis():
    global _redis
    if not REDIS_URL:
        return None
    if _redis is None:
        try:
            import redis.asyncio as redis
            _redis = redis.from_url(REDIS_URL, decode_responses=True)
        except Exception as e:
            logger.warning("Redis init: %s", e)
    return _redis


async def save_debate_redis(user_id: int, report: str) -> bool:
    r = _get_redis()
    if not r:
        return False
    try:
        await r.setex(f"debate:{user_id}", DEBATE_TTL_SEC, report)
        logger.info("Debate saved to Redis user=%s", user_id)
        return True
    except Exception as e:
        logger.warning("Redis save_debate: %s", e)
        return False


async def get_debate_redis(user_id: int) -> str | None:
    r = _get_redis()
    if not r:
        return None
    try:
        return await r.get(f"debate:{user_id}")
    except Exception as e:
        logger.warning("Redis get_debate: %s", e)
        return None


async def ping_redis() -> bool:
    """Проверка при старте бота (логи Railway)."""
    r = _get_redis()
    if not r:
        return False
    try:
        await r.ping()
        return True
    except Exception as e:
        logger.warning("Redis PING: %s", e)
        return False
