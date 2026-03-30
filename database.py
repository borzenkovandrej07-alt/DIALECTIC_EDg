"""
database.py — SQLite база данных.

ИСПРАВЛЕНО v2:
- DB_PATH теперь импортируется из config.py (единый источник правды)
  Раньше был захардкожен здесь и не совпадал с learning.py
"""

import aiosqlite
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional

# ИСПРАВЛЕНО: импортируем из config чтобы все модули использовали один путь
from config import DB_PATH

logger = logging.getLogger(__name__)


async def init_db():
    """Создаёт все таблицы при первом запуске."""
    async with aiosqlite.connect(DB_PATH) as db:

        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id     INTEGER PRIMARY KEY,
                username    TEXT,
                first_name  TEXT,
                tier        TEXT DEFAULT 'free',
                daily_sub   INTEGER DEFAULT 0,
                sub_time    TEXT DEFAULT '08:00',
                requests_today INTEGER DEFAULT 0,
                requests_total INTEGER DEFAULT 0,
                signals_sub INTEGER DEFAULT 0,
                last_active TEXT,
                created_at  TEXT DEFAULT (datetime('now'))
            )
        """)

        # Добавляем колонку signals_sub если её нет (для обновления с существующей БД)
        try:
            await db.execute("ALTER TABLE users ADD COLUMN signals_sub INTEGER DEFAULT 0")
        except:
            pass  # Колонка уже существует

        await db.execute("""
            CREATE TABLE IF NOT EXISTS predictions (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at   TEXT DEFAULT (datetime('now')),
                asset        TEXT NOT NULL,
                direction    TEXT NOT NULL,
                entry_price  REAL,
                target_price REAL,
                stop_loss    REAL,
                timeframe    TEXT,
                source_news  TEXT,
                result       TEXT DEFAULT 'pending',
                result_price REAL,
                result_at    TEXT,
                pnl_pct      REAL,
                prediction_type TEXT,
                forecast     TEXT,
                fact         TEXT,
                report_type  TEXT DEFAULT 'global'
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS feedback (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER,
                report_type TEXT,
                rating      INTEGER,
                comment     TEXT,
                created_at  TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS reports (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER,
                report_type TEXT,
                news_used   TEXT,
                summary     TEXT,
                created_at  TEXT DEFAULT (datetime('now'))
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS debate_sessions (
                user_id    INTEGER PRIMARY KEY,
                report     TEXT NOT NULL,
                updated_at TEXT DEFAULT (datetime('now'))
            )
        """)

        await db.commit()

    logger.info("✅ База данных инициализирована")


# ─── Пользователи ─────────────────────────────────────────────────────────────

async def upsert_user(user_id: int, username: str = "", first_name: str = ""):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO users (user_id, username, first_name, last_active)
            VALUES (?, ?, ?, datetime('now'))
            ON CONFLICT(user_id) DO UPDATE SET
                username = excluded.username,
                first_name = excluded.first_name,
                last_active = datetime('now')
        """, (user_id, username or "", first_name or ""))
        await db.commit()


async def get_user(user_id: int) -> Optional[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM users WHERE user_id = ?", (user_id,)
        ) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None


async def increment_requests(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            UPDATE users SET
                requests_today = requests_today + 1,
                requests_total = requests_total + 1,
                last_active = datetime('now')
            WHERE user_id = ?
        """, (user_id,))
        await db.commit()


async def reset_daily_counts():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET requests_today = 0")
        await db.commit()


async def save_debate_session(user_id: int, report: str):
    """Снимок отчёта для листания дебатов после рестарта / другого воркера."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO debate_sessions (user_id, report, updated_at)
            VALUES (?, ?, datetime('now'))
            ON CONFLICT(user_id) DO UPDATE SET
                report = excluded.report,
                updated_at = datetime('now')
        """, (user_id, report))
        await db.commit()
    logger.info("debate_sessions сохранён user_id=%s (%s симв.)", user_id, len(report or ""))


async def get_debate_session(user_id: int) -> Optional[str]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT report FROM debate_sessions WHERE user_id = ?",
            (user_id,),
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else None


async def get_daily_subscribers() -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM users WHERE daily_sub = 1"
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]


async def set_daily_sub(user_id: int, enabled: bool, time: str = "08:00"):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            UPDATE users SET daily_sub = ?, sub_time = ?
            WHERE user_id = ?
        """, (1 if enabled else 0, time, user_id))
        await db.commit()


async def get_signals_subscribers() -> list[dict]:
    """Возвращает пользователей с включёнными сигналами."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM users WHERE signals_sub = 1"
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]


async def set_signals_sub(user_id: int, enabled: bool):
    """Включить/выключить сигналы для пользователя."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE users SET signals_sub = ? WHERE user_id = ?",
            (1 if enabled else 0, user_id)
        )
        await db.commit()


async def get_user_signals_status(user_id: int) -> bool:
    """Проверить статус подписки на сигналы."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT signals_sub FROM users WHERE user_id = ?",
            (user_id,)
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] == 1 if row else False


# ─── Прогнозы / Track Record ──────────────────────────────────────────────────

async def save_prediction(
    asset: str,
    direction: str,
    entry_price: float,
    target_price: float,
    stop_loss: float,
    timeframe: str,
    source_news: str,
) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("""
            INSERT INTO predictions
                (asset, direction, entry_price, target_price, stop_loss, timeframe, source_news)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (asset, direction, entry_price, target_price, stop_loss, timeframe, source_news[:500]))
        await db.commit()
        return cursor.lastrowid


async def get_pending_predictions() -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT * FROM predictions
            WHERE result = 'pending'
            AND created_at < datetime('now', '-1 day')
            ORDER BY created_at DESC
        """) as cursor:
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]


async def update_prediction_result(
    pred_id: int,
    result: str,
    result_price: float,
    pnl_pct: float,
):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            UPDATE predictions SET
                result = ?,
                result_price = ?,
                result_at = datetime('now'),
                pnl_pct = ?
            WHERE id = ?
        """, (result, result_price, pnl_pct, pred_id))
        await db.commit()


async def import_forecasts_from_markdown():
    """Импорт прогнозов из локального FORECASTS.md в SQLite."""
    import re
    import os
    forecast_path = os.path.join(os.path.dirname(__file__), "FORECASTS.md")
    if not os.path.exists(forecast_path):
        logger.warning("FORECASTS.md не найден")
        return

    with open(forecast_path, "r", encoding="utf-8") as f:
        content = f.read()

    predictions = []
    table_match = re.search(r"\| № \| Дата \|.*?\n\|[-|]+\|.*?\n((?:\|.*?\n)+)", content, re.DOTALL)
    if not table_match:
        logger.warning("Таблица прогнозов не найдена в FORECASTS.md")
        return

    rows = table_match.group(1).strip().split("\n")
    for row in rows:
        parts = [p.strip() for p in row.split("|")[1:-1]]
        if len(parts) < 8:
            continue
        try:
            date_str = parts[1]
            pred_type = parts[2].strip()
            asset = parts[3].strip()
            forecast = parts[4].strip()
            fact = parts[5].strip()
            result_text = parts[6].strip().lower()
            accuracy_text = parts[7].strip().replace("%", "").replace("*", "")
            try:
                pnl_pct = float(accuracy_text)
            except:
                pnl_pct = 0.0
            if "неверно" in result_text:
                result = "loss"
            elif "осторожность" in result_text:
                result = "caution"
                pnl_pct = 100.0
            elif "верно" in result_text or "точ" in result_text:
                result = "win"
            else:
                result = "win"
            date_obj = datetime.strptime(date_str, "%d.%m.%Y")
            created_at = date_obj.strftime("%Y-%m-%d %H:%M:%S")
            
            if "russia" in pred_type.lower() or "edge" in pred_type.lower():
                report_type = "russia"
            else:
                report_type = "global"
                
            predictions.append({
                "created_at": created_at,
                "asset": asset,
                "direction": forecast,
                "result": result,
                "pnl_pct": pnl_pct,
                "prediction_type": pred_type,
                "forecast": forecast,
                "fact": fact,
                "report_type": report_type
            })
        except Exception as e:
            logger.debug(f"Ошибка парсинга строки: {e}")

    if predictions:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("DELETE FROM predictions WHERE created_at LIKE '2026-03-%'")
            await db.commit()
            for p in predictions:
                await db.execute("""
                    INSERT INTO predictions (created_at, asset, direction, entry_price, target_price, result, pnl_pct, prediction_type, forecast, fact, report_type)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    p["created_at"],
                    p["asset"],
                    p["direction"],
                    None,
                    None,
                    p["result"],
                    p["pnl_pct"],
                    p.get("prediction_type", ""),
                    p.get("forecast", ""),
                    p.get("fact", ""),
                    p.get("report_type", "global")
                ))
            await db.commit()
        logger.info(f"✅ Импортировано {len(predictions)} прогнозов из FORECASTS.md")
    else:
        logger.warning("Не удалось распарсить прогнозы из FORECASTS.md")


async def get_track_record(report_type: str = None) -> dict:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

        where_clause = ""
        if report_type:
            where_clause = f" AND report_type = '{report_type}'"

        async with db.execute(f"""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN result = 'win'  THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN result = 'loss' THEN 1 ELSE 0 END) as losses,
                SUM(CASE WHEN result = 'caution' THEN 1 ELSE 0 END) as cautions,
                SUM(CASE WHEN result = 'pending' THEN 1 ELSE 0 END) as pending,
                AVG(CASE WHEN result != 'pending' THEN pnl_pct END) as avg_pnl,
                MAX(pnl_pct) as best_call,
                MIN(pnl_pct) as worst_call
            FROM predictions
            WHERE result != 'expired'{where_clause}
        """) as cursor:
            stats = dict(await cursor.fetchone())

        async with db.execute(f"""
            SELECT asset, direction, entry_price, result, pnl_pct, created_at, prediction_type, forecast, fact
            FROM predictions
            WHERE result != 'pending'{where_clause}
            ORDER BY created_at DESC
            LIMIT 50
        """) as cursor:
            recent = [dict(r) for r in await cursor.fetchall()]

        async with db.execute(f"""
            SELECT asset,
                COUNT(*) as calls,
                SUM(CASE WHEN result='win' THEN 1 ELSE 0 END) as wins,
                AVG(pnl_pct) as avg_pnl
            FROM predictions
            WHERE result IN ('win','loss'){where_clause}
            GROUP BY asset
            HAVING calls >= 2
            ORDER BY avg_pnl DESC
            LIMIT 5
        """) as cursor:
            by_asset = [dict(r) for r in await cursor.fetchall()]

        return {"stats": stats, "recent": recent, "by_asset": by_asset}


# ─── Фидбек ───────────────────────────────────────────────────────────────────

async def save_feedback(user_id: int, report_type: str, rating: int, comment: str = ""):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO feedback (user_id, report_type, rating, comment)
            VALUES (?, ?, ?, ?)
        """, (user_id, report_type, rating, comment))
        await db.commit()


async def get_feedback_stats() -> dict:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN rating =  1 THEN 1 ELSE 0 END) as positive,
                SUM(CASE WHEN rating = -1 THEN 1 ELSE 0 END) as negative
            FROM feedback
        """) as cursor:
            row = await cursor.fetchone()
            return dict(row)


# ─── Отчёты ───────────────────────────────────────────────────────────────────

async def log_report(user_id: int, report_type: str, news_used: str, summary: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO reports (user_id, report_type, news_used, summary)
            VALUES (?, ?, ?, ?)
        """, (user_id, report_type, news_used[:1000], summary[:500]))
        await db.commit()


# ─── Статистика для админа ────────────────────────────────────────────────────

async def get_admin_stats() -> dict:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

        async with db.execute("SELECT COUNT(*) as total FROM users") as c:
            total_users = (await c.fetchone())["total"]

        async with db.execute("""
            SELECT COUNT(*) as active FROM users
            WHERE last_active > datetime('now', '-7 days')
        """) as c:
            active_week = (await c.fetchone())["active"]

        async with db.execute(
            "SELECT COUNT(*) as subs FROM users WHERE daily_sub = 1"
        ) as c:
            subscribers = (await c.fetchone())["subs"]

        async with db.execute("SELECT COUNT(*) as total FROM reports") as c:
            total_reports = (await c.fetchone())["total"]

        return {
            "total_users":   total_users,
            "active_week":   active_week,
            "subscribers":   subscribers,
            "total_reports": total_reports,
        }
