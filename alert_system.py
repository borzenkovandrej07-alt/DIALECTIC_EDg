"""
alert_system.py — Условные алерты на основе истории вердиктов.

Логика:
1. Парсим FORECASTS.md с GitHub (последние прогнозы)
2. Смотрим направление — бычий/медвежий/нейтральный
3. Если 3+ дней подряд одно направление — шлём алерт подписчикам
4. Алерт — это информация, НЕ торговый сигнал

Алерт выглядит так:
🐂 3 дня подряд — бычий анализ
BTC сейчас $69,400 | Поддержка: ~$67,500
Триггер для входа: пробой $70,500 вверх
⚠️ DYOR. Не финансовый совет.
"""

import asyncio
import logging
import re
import aiohttp
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

# Raw URL — без API ключа, бесплатно
FORECASTS_RAW = "https://raw.githubusercontent.com/{repo}/main/FORECASTS.md"

# Минимум дней подряд для алерта
MIN_STREAK = 3

# Раз в сколько часов максимум шлём алерт (защита от спама)
ALERT_COOLDOWN_HOURS = 24


def _parse_direction(asset: str, direction: str) -> Optional[str]:
    """Определяет направление по активу и направлению."""
    asset_lower = asset.lower() if asset else ""
    direction_upper = direction.upper() if direction else ""
    
    # Определяем направление
    if any(w in direction_upper for w in ["BULLISH", "🐂", "LONG", "РОСТ"]):
        return "BULLISH"
    if any(w in direction_upper for w in ["BEARISH", "🐻", "SHORT", "ПАДЕН"]):
        return "BEARISH"
    if any(w in direction_upper for w in ["NEUTRAL", "CASH", "НЕЙТРАЛЬН"]):
        return "NEUTRAL"
    
    return None


async def fetch_forecast_history(github_repo: str) -> list[dict]:
    """
    Читает FORECASTS.md и возвращает историю прогнозов:
    [{"date": "22.03.2026", "direction": "BULLISH", "asset": "BTC"}, ...]
    """
    url = FORECASTS_RAW.format(repo=github_repo)
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=10),
                headers={"User-Agent": "Mozilla/5.0"}
            ) as resp:
                if resp.status != 200:
                    logger.warning(f"FORECASTS fetch: status {resp.status}")
                    return []
                content = await resp.text()
    except Exception as e:
        logger.warning(f"FORECASTS fetch error: {e}")
        return []

    forecasts = []
    in_forecasts = False
    
    for line in content.split('\n'):
        if '## 📝 Все прогнозы' in line:
            in_forecasts = True
            continue
        if in_forecasts and line.strip().startswith('|') and '---' not in line:
            parts = [p.strip() for p in line.split('|')[1:-1]]
            if len(parts) >= 7:
                try:
                    num = parts[0]
                    date = parts[1]
                    asset = parts[3]
                    direction = parts[4]
                    result = parts[6]
                    
                    direction_type = _parse_direction(asset, direction)
                    
                    if direction_type and direction_type != "NEUTRAL":
                        forecasts.append({
                            "date": date,
                            "asset": asset,
                            "direction": direction_type,
                            "result": result,
                        })
                except:
                    pass
        elif in_forecasts and line.strip().startswith('##'):
            break

    logger.info(f"Прогнозов найдено в истории: {len(forecasts)}")
    return forecasts[:14]  # последние 14 дней


def analyze_streak(forecasts: list[dict]) -> dict:
    """
    Считает серию одного направления.
    Возвращает:
    {
        "streak": 3,
        "direction": "BULLISH",
        "alert": True,
        "assets": ["BTC", "ETH"],
        "dates": ["22.03", "21.03", "20.03"]
    }
    """
    if not forecasts:
        return {"streak": 0, "direction": None, "alert": False}

    first_direction = forecasts[0]["direction"]
    streak = 0
    assets = []
    dates = []

    for v in forecasts:
        if v["direction"] == first_direction:
            streak += 1
            assets.append(v["asset"])
            dates.append(v["date"][-5:])
        else:
            break

    return {
        "streak": streak,
        "direction": first_direction,
        "alert": streak >= MIN_STREAK,
        "assets": assets[:5],
        "dates": dates[:3],
    }


async def get_btc_price_now() -> Optional[float]:
    """Текущая цена BTC с Binance."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT",
                timeout=aiohttp.ClientTimeout(total=5)
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return float(data["price"])
    except Exception as e:
        logger.warning(f"Binance BTC price error: {e}")
    return None


def build_alert_text(streak_info: dict, btc_now: Optional[float]) -> str:
    """Формирует текст алерта. НЕ торговый сигнал — информация."""
    direction = streak_info["direction"]
    streak = streak_info["streak"]
    assets = streak_info["assets"]
    dates = streak_info["dates"]

    emoji = "🐂" if direction == "BULLISH" else "🐻"
    direction_ru = "бычий" if direction == "BULLISH" else "медвежий"
    action_hint = "рост" if direction == "BULLISH" else "снижение"

    lines = [
        f"{emoji} *DIALECTIC EDGE — СЕРИЯ СИГНАЛОВ*",
        "",
        f"*{streak} дней подряд* — анализ показывает *{direction_ru}* настрой",
        f"Период: {' → '.join(reversed(dates))}" if dates else "",
        f"Активы: {', '.join(assets[:4])}" if assets else "",
    ]

    if btc_now:
        lines.append("")
        lines.append(f"*BTC сейчас:* ${btc_now:,.0f}")

        support = round(btc_now * 0.97 / 100) * 100
        resistance = round(btc_now * 1.03 / 100) * 100
        
        if direction == "BULLISH":
            lines.extend([
                f"📊 *Уровни для наблюдения:*",
                f"Поддержка: ~${support:,}",
                f"Сопротивление: ~${resistance:,}",
                f"Сигнал к {action_hint}: пробой ${resistance:,} с объёмом",
            ])
        else:
            lines.extend([
                f"📊 *Уровни для наблюдения:*",
                f"Поддержка: ~${support:,}",
                f"Сопротивление: ~${resistance:,}",
                f"Сигнал к {action_hint}: пробой ${support:,} вниз",
            ])

    lines.extend([
        "",
        "⚠️ _Это информация о серии аналитических вердиктов, не торговый сигнал._",
        "_DYOR. Риск потери капитала существует всегда._",
        "",
        "🔄 _Запусти /daily для полного анализа_",
    ])

    return "\n".join([l for l in lines if l])


class AlertSystem:
    def __init__(self, bot, github_repo: str):
        self.bot = bot
        self.github_repo = github_repo
        self._last_alert_direction: Optional[str] = None
        self._last_alert_time: Optional[datetime] = None
        self._last_alert_streak: int = 0

    def _should_send(self, streak_info: dict) -> bool:
        """Проверяет нужно ли слать алерт (защита от спама)."""
        if not streak_info["alert"]:
            return False

        direction = streak_info["direction"]
        streak = streak_info["streak"]
        now = datetime.now()

        if direction != self._last_alert_direction:
            return True

        if streak > self._last_alert_streak:
            if self._last_alert_time is None:
                return True
            hours_passed = (now - self._last_alert_time).total_seconds() / 3600
            return hours_passed >= ALERT_COOLDOWN_HOURS

        return False

    async def check_and_alert(self, subscribers: list[dict]) -> int:
        """Проверяет серию вердиктов и отправляет алерт если нужно."""
        if not subscribers:
            return 0

        forecasts = await fetch_forecast_history(self.github_repo)
        if not forecasts:
            logger.info("Alert: история прогнозов пуста")
            return 0

        streak_info = analyze_streak(forecasts)
        logger.info(
            f"Alert check: серия {streak_info['streak']} × {streak_info['direction']}, "
            f"алерт={'да' if streak_info['alert'] else 'нет'}"
        )

        if not self._should_send(streak_info):
            return 0

        btc_now = await get_btc_price_now()
        alert_text = build_alert_text(streak_info, btc_now)

        sent = 0
        for user in subscribers:
            try:
                await self.bot.send_message(
                    user["user_id"],
                    alert_text,
                    parse_mode="Markdown"
                )
                sent += 1
                await asyncio.sleep(0.1)
            except Exception as e:
                logger.warning(f"Alert send error user {user['user_id']}: {e}")

        self._last_alert_direction = streak_info["direction"]
        self._last_alert_time = datetime.now()
        self._last_alert_streak = streak_info["streak"]

        logger.info(f"✅ Алертов отправлено: {sent}")
        return sent
