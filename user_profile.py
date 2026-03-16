"""
user_profile.py — Персонализация под риск-профиль пользователя.

ИСПРАВЛЕНО v2:
- DB_PATH импортируется из config.py (единый источник правды)
"""

import logging
from config import DB_PATH
import aiosqlite

logger = logging.getLogger(__name__)


RISK_PROFILES = {
    "conservative": {
        "name": "🛡️ Консерватор",
        "desc": "Сохранение капитала важнее прибыли. Минимальный риск.",
        "max_position": 5,
        "max_total_risk": 15,
        "stop_loss": 5,
        "preferred_assets": ["GLD", "SPY", "Облигации", "Дивидендные акции"],
        "avoid": ["Крипта", "Маржинальная торговля", "Опционы"],
        "horizon": "1-4 недели",
        "agent_instruction": (
            "Пользователь — КОНСЕРВАТОР. Приоритет: сохранение капитала.\n"
            "- Максимум 5% портфеля на позицию\n"
            "- Стоп-лосс не более 5%\n"
            "- Фокус: золото, широкий рынок, дивидендные акции\n"
            "- Криптовалюту и маржу не рекомендуй\n"
            "- Если риск высокий — рекомендуй кэш/золото как защиту"
        ),
    },
    "moderate": {
        "name": "⚖️ Умеренный",
        "desc": "Баланс между ростом и защитой. Стандартный риск.",
        "max_position": 10,
        "max_total_risk": 40,
        "stop_loss": 10,
        "preferred_assets": ["SPY", "QQQ", "BTC", "ETH", "Секторные ETF"],
        "avoid": ["Маржинальная торговля x5+", "Мемкоины"],
        "horizon": "1-4 недели",
        "agent_instruction": (
            "Пользователь — УМЕРЕННЫЙ инвестор. Баланс риска и роста.\n"
            "- Максимум 10% портфеля на позицию\n"
            "- Стоп-лосс 7-10%\n"
            "- Крипта допустима до 20% портфеля (BTC/ETH)\n"
            "- Диверсификация обязательна\n"
            "- Маржу не рекомендуй"
        ),
    },
    "aggressive": {
        "name": "🚀 Агрессивный",
        "desc": "Максимальный потенциал прибыли. Готов к высоким рискам.",
        "max_position": 20,
        "max_total_risk": 80,
        "stop_loss": 15,
        "preferred_assets": ["BTC", "ETH", "Altcoins", "Акции роста", "Опционы"],
        "avoid": [],
        "horizon": "1д-2 недели",
        "agent_instruction": (
            "Пользователь — АГРЕССИВНЫЙ трейдер. Приоритет: максимальный рост.\n"
            "- До 20% портфеля на позицию при сильном сигнале\n"
            "- Стоп-лосс до 15%\n"
            "- Крипта и акции роста приоритетны\n"
            "- Можно рассматривать краткосрочные спекуляции\n"
            "- Всё равно обязательно указывай стоп-лосс"
        ),
    },
}

HORIZONS = {
    "scalp":    {"name": "⚡ Скальпинг",     "desc": "Внутри дня (часы)"},
    "swing":    {"name": "📈 Свинг",          "desc": "1-14 дней"},
    "position": {"name": "🏗️ Позиционный",   "desc": "2-8 недель"},
    "invest":   {"name": "💎 Инвестиции",     "desc": "3+ месяца"},
}

MARKETS = {
    "crypto":       "₿ Криптовалюта",
    "stocks":       "📈 Акции (США)",
    "forex":        "💱 Форекс",
    "commodities":  "🛢️ Сырьё",
    "all":          "🌍 Все рынки",
}


async def init_profiles_table():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS user_profiles (
                user_id     INTEGER PRIMARY KEY,
                risk        TEXT DEFAULT 'moderate',
                horizon     TEXT DEFAULT 'swing',
                markets     TEXT DEFAULT 'all',
                capital     TEXT DEFAULT 'unknown',
                updated_at  TEXT DEFAULT (datetime('now'))
            )
        """)
        await db.commit()


async def save_profile(user_id: int, risk: str, horizon: str,
                       markets: str, capital: str = "unknown"):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO user_profiles (user_id, risk, horizon, markets, capital, updated_at)
            VALUES (?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(user_id) DO UPDATE SET
                risk = excluded.risk,
                horizon = excluded.horizon,
                markets = excluded.markets,
                capital = excluded.capital,
                updated_at = datetime('now')
        """, (user_id, risk, horizon, markets, capital))
        await db.commit()


async def get_profile(user_id: int) -> dict:
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM user_profiles WHERE user_id = ?", (user_id,)
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    return dict(row)
    except Exception:
        pass
    return {"risk": "moderate", "horizon": "swing", "markets": "all", "capital": "unknown"}


def build_profile_instruction(profile: dict) -> str:
    risk    = profile.get("risk", "moderate")
    horizon = profile.get("horizon", "swing")
    markets = profile.get("markets", "all")

    risk_data    = RISK_PROFILES.get(risk, RISK_PROFILES["moderate"])
    horizon_data = HORIZONS.get(horizon, HORIZONS["swing"])
    market_name  = MARKETS.get(markets, "Все рынки")

    return (
        f"\n{'='*50}\n"
        f"ПРОФИЛЬ ПОЛЬЗОВАТЕЛЯ (адаптируй анализ под него):\n"
        f"Риск-профиль: {risk_data['name']}\n"
        f"Горизонт: {horizon_data['name']} ({horizon_data['desc']})\n"
        f"Рынки: {market_name}\n\n"
        f"{risk_data['agent_instruction']}\n"
        f"Горизонт рекомендаций: {horizon_data['desc']}\n"
        f"{'='*50}\n"
    )


def format_profile_card(profile: dict) -> str:
    risk    = profile.get("risk", "moderate")
    horizon = profile.get("horizon", "swing")
    markets = profile.get("markets", "all")

    risk_data    = RISK_PROFILES.get(risk, RISK_PROFILES["moderate"])
    horizon_data = HORIZONS.get(horizon, HORIZONS["swing"])
    market_name  = MARKETS.get(markets, "Все рынки")

    return (
        f"👤 *Твой профиль:*\n\n"
        f"📊 Риск: {risk_data['name']}\n"
        f"_{risk_data['desc']}_\n\n"
        f"⏱ Горизонт: {horizon_data['name']}\n"
        f"_{horizon_data['desc']}_\n\n"
        f"🌍 Рынки: {market_name}\n\n"
        f"🎯 Максимум на позицию: {risk_data['max_position']}% портфеля\n"
        f"🛑 Стоп-лосс: до {risk_data['stop_loss']}%\n\n"
        f"Изменить: /profile"
    )
