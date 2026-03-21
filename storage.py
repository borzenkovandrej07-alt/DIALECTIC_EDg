"""
storage.py — Простое кэширование отчётов в JSON-файл.
"""

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

from config import CACHE_FILE, CACHE_TTL_HOURS, DEBATE_SNAPSHOT_HOURS

logger = logging.getLogger(__name__)


class Storage:
    def __init__(self):
        self.cache_path = Path(CACHE_FILE)
        self._data: dict = self._load()

    def reload_from_disk(self):
        """Актуализировать данные из cache.json (другой воркер мог записать дебаты)."""
        self._data = self._load()

    def _load(self) -> dict:
        if self.cache_path.exists():
            try:
                with open(self.cache_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Cache load error: {e}")
        return {}

    def _save(self):
        try:
            with open(self.cache_path, "w", encoding="utf-8") as f:
                json.dump(self._data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"Cache save error: {e}")

    def cache_report(
        self,
        report: str,
        prices: dict | None = None,
        owner_user_id: int | None = None,
    ):
        """Кэширует последний отчёт и снимок цен (для графика при отдаче из кэша)."""
        entry = {
            "report": report,
            "timestamp": datetime.now().strftime("%d.%m.%Y %H:%M"),
            "expires": (datetime.now() + timedelta(hours=CACHE_TTL_HOURS)).isoformat(),
        }
        if prices is not None:
            entry["prices"] = prices
        self.reload_from_disk()
        self._data["last_report"] = entry
        if owner_user_id is not None:
            # Отдельно на пользователя — fallback для кнопки «дебаты» на том же воркере/диске
            self._data.setdefault("user_report_cache", {})
            self._data["user_report_cache"][str(owner_user_id)] = {
                "report": report,
                "expires": entry["expires"],
                "timestamp": entry["timestamp"],
            }
        self._save()
        logger.info("Отчёт кэширован")

    def get_cached_report(self) -> dict | None:
        """Возвращает кэшированный отчёт, если он ещё актуален."""
        cached = self._data.get("last_report")
        if not cached:
            return None
        
        expires = datetime.fromisoformat(cached.get("expires", "2000-01-01"))
        if datetime.now() > expires:
            logger.info("Кэш устарел")
            return None
        
        return cached

    def get_user_last_cached_report(self, user_id: int) -> str | None:
        """Последний полный отчёт, сохранённый при /daily для этого user_id (тот же TTL что last_report)."""
        self.reload_from_disk()
        bucket = self._data.get("user_report_cache")
        if not isinstance(bucket, dict):
            return None
        ent = bucket.get(str(user_id))
        if not isinstance(ent, dict):
            return None
        try:
            exp = datetime.fromisoformat(ent.get("expires", "2000-01-01"))
        except Exception:
            return None
        if datetime.now() > exp:
            return None
        r = ent.get("report")
        return r if isinstance(r, str) and r.strip() else None

    def _prune_expired_debate_snapshots(self):
        ud = self._data.get("user_debates")
        if not isinstance(ud, dict):
            return
        now = datetime.now()
        dead = []
        for k, v in list(ud.items()):
            if not isinstance(v, dict):
                dead.append(k)
                continue
            try:
                exp = datetime.fromisoformat(v.get("expires", "2000-01-01"))
            except Exception:
                dead.append(k)
                continue
            if now > exp:
                dead.append(k)
        for k in dead:
            ud.pop(k, None)

    def save_user_debate_snapshot(self, user_id: int, report: str):
        """Дублирует полный отчёт для кнопки дебатов (рядом с last_report в JSON)."""
        self.reload_from_disk()
        self._data.setdefault("user_debates", {})
        self._data["user_debates"][str(user_id)] = {
            "report": report,
            "expires": (datetime.now() + timedelta(hours=DEBATE_SNAPSHOT_HOURS)).isoformat(),
        }
        self._prune_expired_debate_snapshots()
        self._save()
        logger.info("Снимок дебатов сохранён в cache.json user=%s", user_id)

    def get_user_debate_snapshot(self, user_id: int) -> str | None:
        self.reload_from_disk()
        self._prune_expired_debate_snapshots()
        ud = self._data.get("user_debates", {}).get(str(user_id))
        if not isinstance(ud, dict):
            return None
        try:
            exp = datetime.fromisoformat(ud.get("expires", "2000-01-01"))
        except Exception:
            return None
        if datetime.now() > exp:
            return None
        r = ud.get("report")
        return r if isinstance(r, str) and r.strip() else None

    def clear_cache(self):
        self._data = {}
        self._save()
