"""
signals.py — Сигналы на основе данных Binance и вердиктов Dialectic Edge.

Логика:
1. Получаем данные Binance API (топ-трейдеры лонг/шорт) — без ключа
2. Читаем вердикт из DIGEST_CACHE
3. Сравниваем — если совпадение 60%+ или 80%+ трейдеров в одну сторону = сигнал
4. Отправляем подписчикам через scheduler
"""

import asyncio
import logging
import re
import aiohttp
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

# Binance API endpoints (публичные, без ключа)
BINANCE_FUTURES_URL = "https://fapi.binance.com"

DIGEST_CACHE_URL = "https://raw.githubusercontent.com/{repo}/main/DIGEST_CACHE.md"

# Пороги для сигналов
TOP_TRADERS_THRESHOLD = 80  # 80% трейдеров в одну сторону = сигнал
VERDICT_MATCH_THRESHOLD = 60  # 60% совпадение с вердиктом = сигнал


async def fetch_binance_signals(symbols: list[str] = ["BTCUSDT", "ETHUSDT"]) -> dict:
    """Получает позиции топ-трейдеров с Binance."""
    results = {}
    
    async with aiohttp.ClientSession() as session:
        for symbol in symbols:
            try:
                # top Long/Short Account Ratio
                url = f"{BINANCE_FUTURES_URL}/fapi/v1/topLongShortAccountRatio"
                params = {"symbol": symbol, "period": "1h", "limit": 1}
                
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data:
                            item = data[0]
                            long_ratio = float(item.get("longAccount", 0)) * 100
                            short_ratio = float(item.get("shortAccount", 0)) * 100
                            results[symbol] = {
                                "long": round(long_ratio, 1),
                                "short": round(short_ratio, 1),
                                "dominant": "LONG" if long_ratio > short_ratio else "SHORT"
                            }
            except Exception as e:
                logger.warning(f"Binance API error for {symbol}: {e}")
    
    return results


async def fetch_verdict(github_repo: str) -> Optional[dict]:
    """Читает последний вердикт из DIGEST_CACHE."""
    url = DIGEST_CACHE_URL.format(repo=github_repo)
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    return None
                content = await resp.text()
    except Exception as e:
        logger.warning(f"DIGEST_CACHE fetch error: {e}")
        return None
    
    # Ищем вердикт
    verdict = None
    for line in content.split('\n'):
        line_upper = line.upper()
        if "ВЕРДИКТ" in line_upper or "VERDICT" in line_upper:
            if "БЫЧ" in line_upper or "BULL" in line_upper:
                verdict = "BULLISH"
            elif "МЕДВЕЖ" in line_upper or "BEAR" in line_upper:
                verdict = "BEARISH"
            elif "NEUTRAL" in line_upper or "CASH" in line_upper:
                verdict = "NEUTRAL"
            break
    
    return {"verdict": verdict, "content": content[:500]}


def analyze_signals(binance_data: dict, verdict: Optional[dict]) -> dict:
    """Анализирует данные и генерирует сигналы."""
    signals = []
    
    for symbol, data in binance_data.items():
        long_pct = data["long"]
        short_pct = data["short"]
        dominant = data["dominant"]
        
        # Сигнал 1: 80%+ трейдеров в одну сторону
        if long_pct >= TOP_TRADERS_THRESHOLD:
            signals.append({
                "type": "TOP_TRADERS",
                "symbol": symbol,
                "direction": "LONG",
                "confidence": long_pct,
                "reason": f"{long_pct}% трейдеров в лонге"
            })
        elif short_pct >= TOP_TRADERS_THRESHOLD:
            signals.append({
                "type": "TOP_TRADERS",
                "symbol": symbol,
                "direction": "SHORT",
                "confidence": short_pct,
                "reason": f"{short_pct}% трейдеров в шорте"
            })
        
        # Сигнал 2: Совпадение с вердиктом
        if verdict and verdict.get("verdict"):
            v = verdict["verdict"]
            
            if v == "BULLISH" and dominant == "LONG":
                match = min(long_pct, 80)
                if match >= VERDICT_MATCH_THRESHOLD:
                    signals.append({
                        "type": "VERDICT_MATCH",
                        "symbol": symbol,
                        "direction": "LONG",
                        "confidence": match,
                        "reason": f"Наш вердикт: БЫЧИЙ + {long_pct}% трейдеров в лонге"
                    })
            
            elif v == "BEARISH" and dominant == "SHORT":
                match = min(short_pct, 80)
                if match >= VERDICT_MATCH_THRESHOLD:
                    signals.append({
                        "type": "VERDICT_MATCH",
                        "symbol": symbol,
                        "direction": "SHORT",
                        "confidence": match,
                        "reason": f"Наш вердикт: МЕДВЕЖИЙ + {short_pct}% трейдеров в шорте"
                    })
    
    return signals


def build_signals_message(signals: list, binance_data: dict, verdict: Optional[dict]) -> str:
    """Формирует красивое сообщение с сигналами."""
    lines = [
        "📡 *COPYTRADE SIGNALS*",
        f"_{datetime.now().strftime('%d.%m %H:%M UTC')}_",
        "",
    ]
    
    # Текущие позиции топ-трейдеров
    lines.append("═" * 30)
    lines.append("🔥 ТОП-ТРЕЙДЕРЫ (Binance)")
    lines.append("═" * 30)
    
    for symbol, data in binance_data.items():
        name = symbol.replace("USDT", "")
        long = data["long"]
        short = data["short"]
        
        if long >= 60:
            bar = "🟢" * int(long/10)
        elif short >= 60:
            bar = "🔴" * int(short/10)
        else:
            bar = "⚪️" * 5
        
        lines.append(f"{name}:")
        lines.append(f"  🟢 Лонг: {long}%")
        lines.append(f"  🔴 Шорт: {short}%")
        lines.append(f"  {bar}")
        lines.append("")
    
    # Вердикт
    if verdict and verdict.get("verdict"):
        v = verdict["verdict"]
        emoji = "🐂" if v == "BULLISH" else "🐻" if v == "BEARISH" else "⚪️"
        lines.append("═" * 30)
        lines.append(f"{emoji} НАШ ВЕРДИКТ")
        lines.append("═" * 30)
        lines.append(v)
        lines.append("")
    
    # Сигналы
    if signals:
        lines.append("═" * 30)
        lines.append("🔔 СИГНАЛЫ")
        lines.append("═" * 30)
        
        for s in signals:
            emoji = "🟢" if s["direction"] == "LONG" else "🔴"
            conf = s["confidence"]
            conf_emoji = "✅" if conf >= 70 else "⚠️"
            
            lines.append(f"{emoji} {s['symbol']} → {s['direction']} {conf_emoji}{conf}%")
            lines.append(f"   {s['reason']}")
            lines.append("")
    else:
        lines.append("═" * 30)
        lines.append("⚪️ СИГНАЛОВ НЕТ")
        lines.append("═" * 30)
        lines.append("Нет явных сигналов сейчас.")
    
    lines.extend([
        "",
        "⚠️ _Это информация, не финансовый совет._",
        "_DYOR._"
    ])
    
    return "\n".join(lines)


class SignalsSystem:
    def __init__(self, bot, github_repo: str):
        self.bot = bot
        self.github_repo = github_repo
        self._last_signal_time: Optional[datetime] = None
    
    async def check_and_send_signals(self, subscribers: list[dict]) -> int:
        """Проверяет сигналы и отправляет подписчикам."""
        if not subscribers:
            return 0
        
        # Получаем данные
        binance_data = await fetch_binance_signals()
        verdict = await fetch_verdict(self.github_repo)
        
        if not binance_data:
            logger.warning("No Binance data received")
            return 0
        
        # Анализируем
        signals = analyze_signals(binance_data, verdict)
        
        # Формируем сообщение
        message = build_signals_message(signals, binance_data, verdict)
        
        # Отправляем
        sent = 0
        for user in subscribers:
            try:
                await self.bot.send_message(user["user_id"], message, parse_mode="Markdown")
                sent += 1
                await asyncio.sleep(0.1)
            except Exception as e:
                logger.warning(f"Signal send error user {user['user_id']}: {e}")
        
        self._last_signal_time = datetime.now()
        logger.info(f"✅ Signals sent: {sent}")
        return sent
