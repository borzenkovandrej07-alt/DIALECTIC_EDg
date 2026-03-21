"""
learning.py — Агенты учатся на своих ошибках.
Безопасный модуль: если таблица predictions ещё не создана — просто молча пропускает.

ИСПРАВЛЕНО v2:
- DB_PATH исправлен с "dialectic.db" на "dialectic_edge.db" (совпадает с database.py)
"""
import aiosqlite
import logging
from datetime import datetime

from config import DB_PATH

logger = logging.getLogger(__name__)


def classify_error(pred: dict) -> str:
    """Классифицирует тип ошибки прогноза."""
    pnl = pred.get("pnl_pct")
    if pnl is None:
        return "unknown"

    if pnl < -10:
        return "macro_missed"

    direction = pred.get("direction", "")
    if direction == "LONG" and pnl < 0:
        return "false_signal"
    if direction == "SHORT" and pnl > 0:
        return "false_signal"

    if -5 < pnl < 0:
        return "late_entry"

    return "minor"


def generate_lesson(pred: dict, error_type: str) -> str:
    """Генерирует урок для агента на основе ошибки."""
    asset = pred.get("asset", "актив")

    lessons = {
        "macro_missed": (
            f"⚠️ УРОК по {asset}: Всегда проверяй макро-факторы (инфляция, ФРС, геополитика). "
            "Если макро против — не входи, даже если техника бычья."
        ),
        "false_signal": (
            f"⚠️ УРОК по {asset}: Требуй подтверждения от 2+ независимых индикаторов. "
            "Одиночный сигнал — высокий риск."
        ),
        "late_entry": (
            f"⚠️ УРОК по {asset}: Если цена уже прошла 50%+ до цели — жди отката. "
            "Лучше пропустить, чем ловить разворот."
        ),
        "correlation_error": (
            f"⚠️ УРОК: Рост золота/доллара = Risk-off сигнал = давление на крипту/акции. "
            "Не путай корреляции."
        ),
    }

    return lessons.get(error_type, "")


async def get_recent_lessons(days: int = 14) -> str:
    """
    Возвращает строку с уроками для вставки в промпт агента.
    Если таблицы predictions нет — возвращает пустую строку (без ошибок!).
    """
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            # Проверяем что таблица существует
            async with db.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='predictions'"
            ) as cursor:
                if not await cursor.fetchone():
                    logger.debug("Learning: таблица predictions ещё не создана")
                    return ""

            cursor = await db.execute("""
                SELECT asset, direction, pnl_pct, result, created_at
                FROM predictions
                WHERE result = 'loss'
                AND datetime(created_at) >= datetime('now', ? )
                AND pnl_pct IS NOT NULL
                ORDER BY pnl_pct ASC
                LIMIT 10
            """, (f"-{days} days",))

            losses = await cursor.fetchall()

            if not losses:
                return ""

            result = "\n\n🧠 НЕДАВНИЕ УРОКИ (учти в анализе):\n"
            for i, row in enumerate(losses[:5], 1):
                asset, direction, pnl, status, created = row
                pred = {"asset": asset, "direction": direction, "pnl_pct": pnl}

                error_type = classify_error(pred)
                lesson = generate_lesson(pred, error_type)

                if lesson:
                    result += f"{i}. {lesson}\n"

            logger.info(f"📚 Агенты получили {len(losses)} уроков за {days} дней")
            return result

    except Exception as e:
        logger.debug(f"Learning: ошибка чтения ({e})")
        return ""


async def analyze_errors_for_report(days: int = 30) -> dict:
    """
    Возвращает статистику ошибок для /trackrecord или админки.
    """
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute("""
                SELECT asset, direction, pnl_pct, result
                FROM predictions
                WHERE result = 'loss'
                AND datetime(created_at) >= datetime('now', ?)
            """, (f"-{days} days",))

            errors = await cursor.fetchall()

            stats = {}
            for asset, direction, pnl, status in errors:
                error_type = classify_error({"asset": asset, "direction": direction, "pnl_pct": pnl})
                key = error_type or "unknown"
                if key not in stats:
                    stats[key] = {"count": 0, "avg_pnl": 0, "assets": []}
                stats[key]["count"] += 1
                stats[key]["avg_pnl"] += pnl
                if asset not in stats[key]["assets"]:
                    stats[key]["assets"].append(asset)

            for s in stats.values():
                if s["count"] > 0:
                    s["avg_pnl"] /= s["count"]

            return stats

    except Exception as e:
        logger.debug(f"Error analysis: {e}")
        return {}


# ─── Текстовые графики (без matplotlib) ──────────────────────────────────────

def generate_confidence_chart(bull_score: float, bear_score: float, max_width: int = 30) -> str:
    if bull_score <= 0 and bear_score <= 0:
        return "📊 Баланс: нет данных"

    total = bull_score + bear_score
    if total == 0:
        total = 1

    bull_bars = int((bull_score / total) * max_width)

    bull_bar = "🟩" * bull_bars + "⬜" * (max_width - bull_bars)
    bear_bar = "⬜" * bull_bars + "🟥" * (max_width - bull_bars)

    return (
        f"📊 Баланс аргументов:\n"
        f"🐂 Bull: {bull_bar} {bull_score:.1f}\n"
        f"🐻 Bear: {bear_bar} {bear_score:.1f}\n"
        f"{'─' * (max_width + 10)}"
    )


def generate_pnl_chart(predictions: list, max_width: int = 40) -> str:
    if not predictions:
        return "📈 P&L: нет данных"

    lines = ["📈 P&L последних прогнозов:"]

    for pred in predictions[:10]:
        asset = pred.get("asset", "?")[:6]
        pnl = pred.get("pnl_pct", 0)
        result = pred.get("result", "")

        bar_len = min(abs(int(pnl * 2)), max_width)
        if pnl >= 0:
            bar = "🟩" * bar_len
            sign = "+"
        else:
            bar = "🟥" * bar_len
            sign = ""

        emoji = "✅" if result == "win" else "❌" if result == "loss" else "⏳"
        lines.append(f"{emoji} {asset}: {bar} {sign}{pnl:.1f}%")

    return "\n".join(lines)
