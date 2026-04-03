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
from config import DB_PATH, DIGEST_SNAPSHOT_MAX_CHARS

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
        
        await db.execute("""
            CREATE TABLE IF NOT EXISTS portfolio (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER NOT NULL,
                symbol      TEXT NOT NULL,
                amount      REAL NOT NULL,
                entry_price REAL NOT NULL,
                added_at    TEXT DEFAULT (datetime('now')),
                UNIQUE(user_id, symbol)
            )
        """)
        
        await db.execute("""
            CREATE TABLE IF NOT EXISTS backtest_signals (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at  TEXT DEFAULT (datetime('now')),
                symbol      TEXT NOT NULL,
                direction   TEXT NOT NULL,  -- BUY or SELL
                entry_price REAL,
                exit_price  REAL,
                status      TEXT DEFAULT 'open',  -- open, closed
                pnl         REAL DEFAULT 0,
                pnl_pct     REAL DEFAULT 0,
                signal_source TEXT,  -- daily, manual, etc
                notes       TEXT,
                quantity    REAL DEFAULT 0,  -- amount of asset
                trade_log   TEXT  -- JSON log of trade actions
            )
        """)
        
        # Backtest config table
        await db.execute("""
            CREATE TABLE IF NOT EXISTS backtest_config (
                id          INTEGER PRIMARY KEY CHECK (id = 1),
                capital     REAL DEFAULT 100.0,
                enabled     INTEGER DEFAULT 1,
                last_updated TEXT DEFAULT (datetime('now'))
            )
        """)
        
        # Initialize default config if not exists
        await db.execute("""
            INSERT OR IGNORE INTO backtest_config (id, capital, enabled) VALUES (1, 100.0, 1)
        """)
        
        # Daily context table - stores verdict and price levels for signal trading
        await db.execute("""
            CREATE TABLE IF NOT EXISTS daily_context (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at      TEXT DEFAULT (datetime('now')),
                verdict         TEXT,  -- BUY, SELL, NEUTRAL
                symbols         TEXT,  -- JSON list of symbols to trade
                entries         TEXT,  -- JSON dict of entry prices
                stop_losses     TEXT,  -- JSON dict of stop loss prices
                targets         TEXT,  -- JSON dict of target prices
                timeframes      TEXT,  -- JSON dict of timeframes
                news_summary    TEXT,  -- brief news context
                expires_at      TEXT,  -- when this context expires (default 24h)
                prompt_versions TEXT,  -- JSON: версии пайплайна/промптов
                model_inputs_snapshot TEXT  -- JSON: усечённый снимок входов модели
            )
        """)

        async def _add_column_if_missing(table: str, column: str, decl: str):
            async with db.execute(f"PRAGMA table_info({table})") as cur:
                cols = [row[1] for row in await cur.fetchall()]
            if column not in cols:
                await db.execute(f"ALTER TABLE {table} ADD COLUMN {decl}")

        await _add_column_if_missing("daily_context", "prompt_versions", "prompt_versions TEXT")
        await _add_column_if_missing("daily_context", "model_inputs_snapshot", "model_inputs_snapshot TEXT")

        await db.execute("""
            CREATE TABLE IF NOT EXISTS trade_decision_log (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at  TEXT DEFAULT (datetime('now')),
                cycle_type  TEXT NOT NULL,
                signal_id   INTEGER,
                payload     TEXT NOT NULL
            )
        """)

        await db.commit()

    logger.info("✅ База данных инициализирована")


# ─── Пользователи ─────────────────────────────────────────────────────────────

async def upsert_user(user_id: int, username: str = "", first_name: str = ""):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO users (user_id, username, first_name, last_active, signals_sub)
            VALUES (?, ?, ?, datetime('now'), 1)
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
        params = []
        if report_type:
            where_clause = " AND report_type = ?"
            params = [report_type]

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
        """, params) as cursor:
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


async def add_portfolio_position(user_id: int, symbol: str, amount: float, entry_price: float) -> bool:
    """Add or update portfolio position."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO portfolio (user_id, symbol, amount, entry_price)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(user_id, symbol) DO UPDATE SET
                amount = excluded.amount,
                entry_price = excluded.entry_price
        """, (user_id, symbol.upper(), amount, entry_price))
        await db.commit()
    return True


async def get_portfolio(user_id: int) -> list[dict]:
    """Get user portfolio."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT symbol, amount, entry_price, added_at
            FROM portfolio WHERE user_id = ?
            ORDER BY added_at DESC
        """, (user_id,)) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]


async def remove_portfolio_position(user_id: int, symbol: str) -> bool:
    """Remove position from portfolio."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM portfolio WHERE user_id = ? AND symbol = ?",
                        (user_id, symbol.upper()))
        await db.commit()
    return True


# ─── Backtest Signals ─────────────────────────────────────────────────────────────

async def add_backtest_signal(
    symbol: str,
    direction: str,
    entry_price: float,
    source: str = "daily",
    quantity_pct: float = 1.0,
    notes: str = "",
    trade_log: str = "",
) -> dict:
    """Open a paper trade without changing equity until the trade is closed."""
    config = await get_backtest_config()

    if not config.get("enabled", 1):
        return {"status": "disabled", "message": "Backtest is disabled"}

    if entry_price <= 0:
        return {"status": "invalid", "message": "Entry price must be positive"}

    direction = direction.upper()
    symbol = symbol.upper()
    quantity_pct = min(max(quantity_pct, 0.01), 1.0)
    quantity = 0.0
    position_cost = 0.0
    capital = 0.0  # Will be read from DB

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

        # Get current config FIRST
        async with db.execute("SELECT capital FROM backtest_config WHERE id = 1") as cursor:
            config_row = await cursor.fetchone()
            if config_row:
                capital = float(config_row.get("capital", 100.0) or 100.0)
            else:
                await db.execute("INSERT INTO backtest_config (capital, enabled) VALUES (100.0, 1)")
                capital = 100.0

        logger.info(f"add_backtest_signal: capital from config={capital}")

        # Check for ANY existing open position (not just same symbol)
        async with db.execute("""
            SELECT * FROM backtest_signals
            WHERE status = 'open'
            ORDER BY created_at DESC LIMIT 1
        """) as cursor:
            existing_open = await cursor.fetchone()

        if existing_open:
            logger.info(f"add_backtest_signal: already have open position {existing_open['symbol']}, skipping new position")
            return {
                "status": "position_exists",
                "symbol": existing_open["symbol"],
                "direction": existing_open["direction"],
                "entry_price": existing_open["entry_price"],
                "quantity": existing_open["quantity"] or 0.0,
                "capital_before": capital,
                "capital_after": capital,
            }

        # Calculate quantity and cost
        quantity = (capital * quantity_pct) / entry_price if entry_price > 0 else 0.0
        position_cost = quantity * entry_price

        logger.info(f"add_backtest_signal: qty={quantity}, cost={position_cost}")

        # Deduct position cost from capital
        new_capital = capital - position_cost
        logger.info(f"add_backtest_signal: new_capital after deduct={new_capital}")

        cursor = await db.execute("""
            INSERT INTO backtest_signals (
                symbol, direction, entry_price, status, signal_source, notes, quantity, trade_log
            )
            VALUES (?, ?, ?, 'open', ?, ?, ?, ?)
        """, (symbol, direction, entry_price, source, notes[:500], quantity, trade_log[:4000]))

        # Update capital
        await db.execute("UPDATE backtest_config SET capital = ?, last_updated = datetime('now') WHERE id = 1", (new_capital,))
        await db.commit()

    return {
        "status": "opened",
        "signal_id": cursor.lastrowid,
        "symbol": symbol,
        "direction": direction,
        "entry_price": entry_price,
        "quantity": quantity,
        "capital_before": capital,
        "capital_after": new_capital,
    }


async def close_backtest_signal(signal_id: int, exit_price: float, reason: str = "") -> dict | None:
    """Close a paper trade, realize PnL, and update account equity."""
    if exit_price <= 0:
        return None

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

        async with db.execute("SELECT * FROM backtest_signals WHERE id = ?", (signal_id,)) as cursor:
            signal = await cursor.fetchone()
            if not signal:
                return None

        async with db.execute("SELECT capital FROM backtest_config WHERE id = 1") as cursor:
            config_row = await cursor.fetchone()
            capital = float((config_row["capital"] if config_row else 100.0) or 100.0)

        if signal["status"] == "closed":
            return {
                "pnl": signal["pnl"] or 0.0,
                "pnl_pct": signal["pnl_pct"] or 0.0,
                "new_capital": capital,
            }

        entry_price = float(signal["entry_price"] or 0.0)
        direction = (signal["direction"] or "").upper()
        quantity = float(signal["quantity"] or 0.0)
        quantity = quantity if quantity > 0 else (capital / entry_price if entry_price > 0 else 0.0)

        if direction == "BUY":
            pnl_per_unit = exit_price - entry_price
        else:
            pnl_per_unit = entry_price - exit_price

        pnl_pct = (pnl_per_unit / entry_price * 100) if entry_price > 0 else 0.0
        pnl = pnl_per_unit * quantity
        new_capital = max(capital + pnl, 0.0)

        old_notes = signal["notes"] or ""
        final_notes = old_notes
        if reason:
            final_notes = f"{old_notes}\n{reason}".strip()

        await db.execute("""
            UPDATE backtest_signals
            SET status = 'closed', exit_price = ?, pnl = ?, pnl_pct = ?, notes = ?
            WHERE id = ?
        """, (exit_price, pnl, pnl_pct, final_notes[:500], signal_id))

        await db.execute("""
            UPDATE backtest_config SET capital = ?, last_updated = datetime('now') WHERE id = 1
        """, (new_capital,))
        await db.commit()

    return {
        "pnl": pnl,
        "pnl_pct": pnl_pct,
        "new_capital": new_capital,
        "quantity": quantity,
    }


async def get_backtest_signals() -> list[dict]:
    """Get all backtest signals."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT * FROM backtest_signals ORDER BY created_at DESC
        """) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]


async def get_backtest_stats() -> dict:
    """Get backtest statistics."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        
        async with db.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN pnl < 0 THEN 1 ELSE 0 END) as losses,
                SUM(pnl) as total_pnl,
                AVG(pnl_pct) as avg_pnl_pct
            FROM backtest_signals WHERE status = 'closed'
        """) as cursor:
            row = await cursor.fetchone()
            if not row:
                return {"total": 0, "wins": 0, "losses": 0, "total_pnl": 0.0, "avg_pnl_pct": 0.0}
            d = dict(row)
            return {
                "total": d.get("total") or 0,
                "wins": d.get("wins") or 0,
                "losses": d.get("losses") or 0,
                "total_pnl": d.get("total_pnl") or 0.0,
                "avg_pnl_pct": d.get("avg_pnl_pct") or 0.0
            }


# ─── Backtest Config ─────────────────────────────────────────────────────────────

async def get_backtest_config() -> dict:
    """Get backtest configuration (capital, enabled)."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM backtest_config WHERE id = 1") as cursor:
            row = await cursor.fetchone()
            if not row:
                return {"capital": 100.0, "enabled": 1}
            d = dict(row)
            d["capital"] = d.get("capital") or 100.0
            d["enabled"] = d.get("enabled") or 1
            return d


async def update_backtest_capital(new_capital: float) -> dict:
    """Update backtest capital."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            UPDATE backtest_config SET capital = ?, last_updated = datetime('now') WHERE id = 1
        """, (new_capital,))
        await db.commit()
    return await get_backtest_config()


async def set_backtest_enabled(enabled: bool) -> dict:
    """Enable or disable backtest."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            UPDATE backtest_config SET enabled = ?, last_updated = datetime('now') WHERE id = 1
        """, (1 if enabled else 0,))
        await db.commit()
    return await get_backtest_config()


# ─── Daily Context ─────────────────────────────────────────────────────────────

def _decode_daily_context_row(row) -> dict | None:
    import json

    if not row:
        return None

    data = dict(row)
    data["symbols"] = json.loads(data.get("symbols", "[]") or "[]")
    data["entries"] = json.loads(data.get("entries", "{}") or "{}")
    data["stop_losses"] = json.loads(data.get("stop_losses", "{}") or "{}")
    data["targets"] = json.loads(data.get("targets", "{}") or "{}")
    data["timeframes"] = json.loads(data.get("timeframes", "{}") or "{}")
    pv = data.get("prompt_versions") or "{}"
    try:
        data["prompt_versions"] = json.loads(pv) if isinstance(pv, str) else (pv or {})
    except Exception:
        data["prompt_versions"] = {}
    ms = data.get("model_inputs_snapshot") or "{}"
    try:
        data["model_inputs_snapshot"] = json.loads(ms) if isinstance(ms, str) else (ms or {})
    except Exception:
        data["model_inputs_snapshot"] = {}
    return data


async def save_daily_context(
    verdict: str,
    symbols: list,
    entries: dict,
    stop_losses: dict,
    targets: dict,
    timeframes: dict,
    news_summary: str = "",
    prompt_versions: dict | None = None,
    model_inputs_snapshot: dict | None = None,
) -> int:
    """Save daily context from /daily and keep recent history for consensus trading."""
    import json

    pv_json = json.dumps(prompt_versions or {}, ensure_ascii=False)[:8000]
    snap_json = json.dumps(model_inputs_snapshot or {}, ensure_ascii=False)[:DIGEST_SNAPSHOT_MAX_CHARS]

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("""
            INSERT INTO daily_context (
                verdict, symbols, entries, stop_losses, targets, timeframes, news_summary, expires_at,
                prompt_versions, model_inputs_snapshot
            ) VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now', '+72 hours'), ?, ?)
        """, (
            verdict,
            json.dumps(symbols),
            json.dumps(entries),
            json.dumps(stop_losses),
            json.dumps(targets),
            json.dumps(timeframes),
            news_summary[:500],
            pv_json,
            snap_json,
        ))

        await db.execute("""
            DELETE FROM daily_context
            WHERE id NOT IN (
                SELECT id FROM daily_context ORDER BY created_at DESC, id DESC LIMIT 30
            )
        """)
        await db.commit()
        return cursor.lastrowid


async def get_daily_context() -> dict | None:
    """Get the latest saved daily context for signal trading."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT * FROM daily_context ORDER BY created_at DESC, id DESC LIMIT 1
        """) as cursor:
            row = await cursor.fetchone()
            return _decode_daily_context_row(row)


async def get_recent_daily_contexts(limit: int = 3, max_age_hours: int | None = 72) -> list[dict]:
    """Get several recent daily contexts for digest-consensus trading."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

        if max_age_hours is None:
            query = """
                SELECT * FROM daily_context
                ORDER BY created_at DESC, id DESC
                LIMIT ?
            """
            params = (limit,)
        else:
            query = f"""
                SELECT * FROM daily_context
                WHERE created_at >= datetime('now', '-{int(max_age_hours)} hours')
                ORDER BY created_at DESC, id DESC
                LIMIT ?
            """
            params = (limit,)

        async with db.execute(query, params) as cursor:
            rows = await cursor.fetchall()
            return [_decode_daily_context_row(row) for row in rows if row]


async def append_trade_decision_log(cycle_type: str, payload: dict, signal_id: int | None = None) -> int:
    """Аудит решений автотрейда / сигналов (JSON payload)."""
    import json

    raw = json.dumps(payload, ensure_ascii=False)
    if len(raw) > 65000:
        raw = raw[:65000] + '"…[truncated]"}'

    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """
            INSERT INTO trade_decision_log (cycle_type, signal_id, payload)
            VALUES (?, ?, ?)
            """,
            (cycle_type, signal_id, raw),
        )
        await db.commit()
        return int(cur.lastrowid)


async def get_recent_trade_decisions(limit: int = 5) -> list[dict]:
    import json

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """
            SELECT id, created_at, cycle_type, signal_id, payload
            FROM trade_decision_log
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ) as cursor:
            rows = await cursor.fetchall()
            out = []
            for row in rows:
                item = dict(row)
                try:
                    item["payload"] = json.loads(item.get("payload") or "{}")
                except Exception:
                    item["payload"] = {}
                out.append(item)
            return out


# ─── Recent Predictions for Context ─────────────────────────────────────────────

async def get_recent_predictions(days: int = 5, limit: int = 10) -> list[dict]:
    """Get recent predictions for context in analysis."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(f"""
            SELECT * FROM predictions 
            WHERE created_at > datetime('now', '-{days} days')
            ORDER BY created_at DESC
            LIMIT ?
        """, (limit,)) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]


async def get_predictions_summary(days: int = 5) -> str:
    """Get formatted summary of recent predictions for AI context."""
    predictions = await get_recent_predictions(days=days, limit=20)
    
    if not predictions:
        return "Нет прошлых прогнозов за последние дни."
    
    lines = ["=== ПРОШЛЫЕ ПРОГНОЗЫ ==="]
    
    # Group by asset
    by_asset = {}
    for p in predictions:
        asset = p.get("asset", "UNKNOWN")
        if asset not in by_asset:
            by_asset[asset] = []
        by_asset[asset].append(p)
    
    for asset, preds in by_asset.items():
        lines.append(f"\n{asset}:")
        for p in preds[:3]:  # Max 3 per asset
            direction = p.get("direction", "")
            entry = p.get("entry_price") or 0
            target = p.get("target_price") or 0
            result = p.get("result", "pending")
            date = p.get("created_at", "")[:10]
            
            if result == "pending":
                lines.append(f"  {date}: {direction} вход=${entry:.0f} цель=${target:.0f} — в ожидании")
            elif result == "win":
                lines.append(f"  {date}: {direction} вход=${entry:.0f} цель=${target:.0f} — ✅ WIN")
            elif result == "loss":
                lines.append(f"  {date}: {direction} вход=${entry:.0f} цель=${target:.0f} — 🔴 LOSS")
            else:
                lines.append(f"  {date}: {direction} вход=${entry:.0f} цель=${target:.0f} — {result}")
    
    # Calculate accuracy
    closed = [p for p in predictions if p.get("result") in ("win", "loss")]
    wins = len([p for p in closed if p.get("result") == "win"])
    accuracy = (wins / len(closed) * 100) if closed else 0
    
    lines.append(f"\nТочность: {wins}/{len(closed)} = {accuracy:.0f}%")
    lines.append("=========================")
    
    return "\n".join(lines)
