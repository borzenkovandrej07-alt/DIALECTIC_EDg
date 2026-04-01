"""
Admin commands backed by current production database statistics.
"""

from __future__ import annotations

import logging
import platform
import sys
from typing import List

from aiogram.types import Message

from config import ADMIN_IDS as CONFIG_ADMIN_IDS
from database import get_admin_stats, get_feedback_stats, get_track_record

logger = logging.getLogger(__name__)

ADMIN_IDS: set[int] = set(CONFIG_ADMIN_IDS)


def register_admin(admin_id: int) -> None:
    ADMIN_IDS.add(admin_id)


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


class AdminHandler:
    async def format_stats(self) -> str:
        stats = await get_admin_stats()
        feedback = await get_feedback_stats()
        track = await get_track_record()
        tr_stats = track["stats"]
        wins = tr_stats.get("wins") or 0
        losses = tr_stats.get("losses") or 0
        winrate = (wins / (wins + losses) * 100) if (wins + losses) else 0
        return (
            "<b>📊 Статистика бота</b>\n\n"
            f"<b>Пользователи:</b> {stats['total_users']} | активных 7д: {stats['active_week']}\n"
            f"<b>Подписчики:</b> {stats['subscribers']}\n"
            f"<b>Отчёты:</b> {stats['total_reports']}\n"
            f"<b>Фидбек:</b> +{feedback.get('positive', 0)} / -{feedback.get('negative', 0)}\n"
            f"<b>Track Record:</b> {tr_stats.get('total', 0)} прогнозов | {winrate:.0f}% winrate"
        )

    async def format_health_check(self) -> str:
        stats = await get_admin_stats()
        return (
            "<b>✅ Health Check</b>\n\n"
            f"<b>Users:</b> {stats['total_users']}\n"
            f"<b>Subscribers:</b> {stats['subscribers']}\n"
            f"<b>Reports:</b> {stats['total_reports']}\n"
            "<b>Status:</b> online"
        )

    def get_recent_logs(self) -> str:
        return (
            "<b>📋 Логи</b>\n\n"
            "Локальный refactor-слой не читает файл логов напрямую.\n"
            "Используй stdout/stderr Railway или лог-файл процесса."
        )

    def format_system_info(self) -> str:
        return (
            "<b>🖥️ Системная информация</b>\n\n"
            f"<b>Python:</b> {sys.version.split()[0]}\n"
            f"<b>Platform:</b> {platform.system()} {platform.release()}\n"
            f"<b>Admins loaded:</b> {len(ADMIN_IDS)}"
        )


_admin_handler = AdminHandler()


def get_admin_handler() -> AdminHandler:
    return _admin_handler


async def check_admin(message: Message) -> bool:
    if not is_admin(message.from_user.id):
        await message.answer("❌ Команда доступна только администраторам")
        return False
    return True


async def handle_stats_command(message: Message) -> None:
    if not await check_admin(message):
        return
    await message.answer(await get_admin_handler().format_stats(), parse_mode="HTML")


async def handle_health_command(message: Message) -> None:
    if not await check_admin(message):
        return
    await message.answer(await get_admin_handler().format_health_check(), parse_mode="HTML")


async def handle_logs_command(message: Message) -> None:
    if not await check_admin(message):
        return
    await message.answer(get_admin_handler().get_recent_logs(), parse_mode="HTML")


async def handle_sysinfo_command(message: Message) -> None:
    if not await check_admin(message):
        return
    await message.answer(get_admin_handler().format_system_info(), parse_mode="HTML")


def setup_admins(admin_list: List[int]) -> None:
    for admin_id in admin_list:
        register_admin(admin_id)
