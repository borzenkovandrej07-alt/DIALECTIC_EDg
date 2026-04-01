"""
Debate navigation backed by the current Redis/SQLite/file fallback chain.
"""

from __future__ import annotations

import logging
from typing import Optional

from aiogram.types import CallbackQuery, Message

from database import get_debate_session, save_debate_session
from debate_storage import get_debate_redis, save_debate_redis
from storage import Storage

from .utils import clean_markdown, debates_keyboard, hydrate_debate_from_report, split_message

logger = logging.getLogger(__name__)

debate_cache: dict[int, dict] = {}
_storage = Storage()


class DebateHandler:
    def __init__(self):
        self.cache = debate_cache

    async def store_debate(self, user_id: int, report: str, market: str) -> bool:
        debate_data = hydrate_debate_from_report(report)
        if not debate_data:
            return False

        payload = {
            **debate_data,
            "market": market,
            "total": len(debate_data.get("rounds", [])),
        }
        self.cache[user_id] = payload

        try:
            await save_debate_session(user_id, report)
        except Exception as exc:
            logger.warning("save_debate_session failed for %s: %s", user_id, exc)
        try:
            await save_debate_redis(user_id, report)
        except Exception as exc:
            logger.warning("save_debate_redis failed for %s: %s", user_id, exc)
        try:
            _storage.save_user_debate_snapshot(user_id, report)
        except Exception as exc:
            logger.warning("save_user_debate_snapshot failed for %s: %s", user_id, exc)
        return True

    async def get_debate(self, user_id: int) -> Optional[dict]:
        cache = self.cache.get(user_id)
        if cache:
            return cache

        report = await get_debate_redis(user_id)
        if not report:
            report = await get_debate_session(user_id)
        if not report:
            _storage.reload_from_disk()
            report = _storage.get_user_debate_snapshot(user_id)
        if not report:
            _storage.reload_from_disk()
            report = _storage.get_user_last_cached_report(user_id)
        if not report:
            cached = _storage.get_cached_report()
            report = cached.get("report") if cached else None
        if not report:
            return None

        hydrated = hydrate_debate_from_report(report)
        if not hydrated:
            return None
        hydrated["total"] = len(hydrated.get("rounds", []))
        self.cache[user_id] = hydrated
        return hydrated

    async def send_round(self, message: Message, user_id: int, round_idx: int) -> None:
        debate = await self.get_debate(user_id)
        if not debate:
            await message.answer("📭 Дебаты не найдены")
            return
        if round_idx < 0 or round_idx >= len(debate["rounds"]):
            await message.answer("❌ Раунд не существует")
            return

        round_text = clean_markdown(debate["rounds"][round_idx])
        chunks = split_message(round_text, max_len=3500)
        for idx, chunk in enumerate(chunks):
            if idx == 0:
                chunk = f"📖 Раунд {round_idx + 1}/{debate['total']}\n\n{chunk}"
            await message.answer(chunk)
        await message.answer(
            "Навигация:",
            reply_markup=debates_keyboard(user_id, round_idx, debate["total"]),
        )

    async def handle_debate_navigation(
        self,
        callback: CallbackQuery,
        user_id: int,
        round_idx: int,
    ) -> None:
        debate = await self.get_debate(user_id)
        if not debate:
            await callback.answer("❌ Дебаты не найдены", show_alert=True)
            return
        if round_idx < 0 or round_idx >= len(debate["rounds"]):
            await callback.answer("❌ Раунд не найден", show_alert=True)
            return

        round_text = clean_markdown(debate["rounds"][round_idx])
        body = f"📖 Раунд {round_idx + 1}/{debate['total']}\n\n{round_text[:3500]}"
        try:
            await callback.message.edit_text(
                body,
                reply_markup=debates_keyboard(user_id, round_idx, debate["total"]),
            )
            await callback.answer()
        except Exception as exc:
            logger.warning("debate navigation edit failed: %s", exc)
            await callback.answer("❌ Не удалось обновить сообщение", show_alert=True)


_debate_handler = DebateHandler()


def get_debate_handler() -> DebateHandler:
    return _debate_handler


async def store_and_link_debate(user_id: int, report: str, market: str) -> bool:
    return await get_debate_handler().store_debate(user_id, report, market)


async def show_debate_round(message: Message, user_id: int, round_idx: int = 0) -> None:
    await get_debate_handler().send_round(message, user_id, round_idx)


async def handle_debate_navigation_callback(
    callback: CallbackQuery,
    user_id: int,
    round_idx: int,
) -> None:
    await get_debate_handler().handle_debate_navigation(callback, user_id, round_idx)
