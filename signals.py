"""
signals.py — Сигналы на основе данных Binance/Bybit и вердиктов Dialectic Edge.

Логика:
1. Получаем данные Bybit (позиции трейдеров) если есть API ключ
2. Иначе используем публичный Binance API
3. Читаем вердикт из DIGEST_CACHE
4. Анализируем и генерируем сигналы
5. Отправляем подписчикам через scheduler
"""

import asyncio
import hashlib
import hmac
import logging
import os
import re
import time
import aiohttp
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

# API URLs
BINANCE_FUTURES_URL = "https://fapi.binance.com"
BYBIT_URL = "https://api.bybit.com"

DIGEST_CACHE_URL = "https://raw.githubusercontent.com/{repo}/main/DIGEST_CACHE.md"

# Пороги для сигналов
PRICE_CHANGE_THRESHOLD = 2.0
FUNDING_THRESHOLD = 0.0001
TOP_TRADERS_THRESHOLD = 60  # 60%+ трейдеров в одну сторону


def get_bybit_keys() -> tuple:
    """Получает API ключи Bybit из переменных окружения."""
    api_key = os.getenv("BYBIT_API_KEY", "")
    secret = os.getenv("BYBIT_SECRET_KEY", "")
    return api_key, secret


async def fetch_bybit_long_short_ratio(symbols: list[str] = ["BTCUSDT", "ETHUSDT"]) -> dict:
    """Получает данные позиций трейдеров с Bybit API."""
    api_key, secret = get_bybit_keys()
    results = {}
    
    if not api_key or not secret:
        logger.info("Bybit API ключи не найдены, используем Binance")
        return {}
    
    for symbol in symbols:
        try:
            # Bybit V5 API для account ratio
            endpoint = "/v5/market/account-ratio"
            params = {
                "category": "linear",
                "symbol": symbol,
                "interval": "15",  # 15 минут
                "limit": 1
            }
            
            # Генерируем подпись
            timestamp = str(int(time.time() * 1000))
            query_string = "&".join([f"{k}={v}" for k, v in params.items()])
            sign = hmac.new(
                secret.encode(),
                f"{timestamp}{api_key}{query_string}".encode(),
                hashlib.sha256
            ).hexdigest()
            
            headers = {
                "X-BAPI-API-KEY": api_key,
                "X-BAPI-SIGN": sign,
                "X-BAPI-SIGN-TYPE": "HmacSHA256",
                "X-BAPI-TIMESTAMP": timestamp,
            }
            
            async with aiohttp.ClientSession() as session:
                url = f"{BYBIT_URL}{endpoint}"
                async with session.get(url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get("retCode") == 0 and data.get("result", {}).get("list"):
                            item = data["result"]["list"][0]
                            long_ratio = float(item.get("longAccount", 0)) * 100
                            short_ratio = float(item.get("shortAccount", 0)) * 100
                            results[symbol] = {
                                "long": round(long_ratio, 1),
                                "short": round(short_ratio, 1),
                                "dominant": "LONG" if long_ratio > short_ratio else "SHORT"
                            }
                            logger.info(f"Bybit data for {symbol}: long={long_ratio}%, short={short_ratio}%")
                    else:
                        logger.warning(f"Bybit API error: {resp.status}")
                        
        except Exception as e:
            logger.warning(f"Bybit fetch error for {symbol}: {e}")
    
    return results


async def fetch_binance_signals(symbols: list[str] = ["BTCUSDT", "ETHUSDT"]) -> dict:
    """Получает данные: Bybit (если есть ключи) + Binance (fallback)."""
    results = {}
    
    # Сначала пробуем Bybit (позиции трейдеров)
    bybit_data = await fetch_bybit_long_short_ratio(symbols)
    
    async with aiohttp.ClientSession() as session:
        for symbol in symbols:
            try:
                # Всегда получаем данные Binance как baseline
                ticker_url = f"{BINANCE_FUTURES_URL}/fapi/v1/ticker/24hr"
                async with session.get(ticker_url, params={"symbol": symbol}, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    if resp.status == 200:
                        ticker = await resp.json()
                        price_change = float(ticker.get("priceChangePercent", 0))
                        
                        results[symbol] = {
                            "price_change": round(price_change, 2),
                            "volume": float(ticker.get("quoteVolume", 0)),
                            "last_price": float(ticker.get("lastPrice", 0)),
                        }
                
                # Funding rate
                funding_url = f"{BINANCE_FUTURES_URL}/fapi/v1/fundingRate"
                async with session.get(funding_url, params={"symbol": symbol, "limit": 1}, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data:
                            funding = float(data[0].get("fundingRate", 0))
                            results[symbol]["funding_rate"] = funding
                            results[symbol]["funding_direction"] = "LONG" if funding > 0 else "SHORT"
                
                # Если есть Bybit данные - мержим
                if symbol in bybit_data:
                    results[symbol]["long"] = bybit_data[symbol]["long"]
                    results[symbol]["short"] = bybit_data[symbol]["short"]
                    results[symbol]["dominant"] = bybit_data[symbol]["dominant"]
                    results[symbol]["has_traders_data"] = True
                    logger.info(f"Using Bybit data for {symbol}")
                        
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


def analyze_signals(binance_data: dict, verdict: Optional[dict]) -> list:
    """Анализирует данные и генерирует сигналы."""
    signals = []
    
    for symbol, data in binance_data.items():
        price_change = data.get("price_change", 0)
        funding = data.get("funding_rate", 0)
        funding_dir = data.get("funding_direction", "NEUTRAL")
        
        # Сигнал 1: Bybit позиции трейдеров (приоритет!)
        long_pct = data.get("long", 0)
        short_pct = data.get("short", 0)
        
        if long_pct >= TOP_TRADERS_THRESHOLD:
            signals.append({
                "type": "BYBIT_TRADERS",
                "symbol": symbol,
                "direction": "LONG",
                "confidence": long_pct,
                "reason": f"{long_pct}% трейдеров в лонге"
            })
        elif short_pct >= TOP_TRADERS_THRESHOLD:
            signals.append({
                "type": "BYBIT_TRADERS",
                "symbol": symbol,
                "direction": "SHORT",
                "confidence": short_pct,
                "reason": f"{short_pct}% трейдеров в шорте"
            })
        
        # Сигнал 2: Сильное изменение цены (fallback если нет Bybit)
        elif abs(price_change) >= PRICE_CHANGE_THRESHOLD:
            direction = "LONG" if price_change > 0 else "SHORT"
            confidence = min(abs(price_change) * 10, 95)
            signals.append({
                "type": "PRICE_MOVE",
                "symbol": symbol,
                "direction": direction,
                "confidence": round(confidence),
                "reason": f"{price_change:+.2f}% за 24ч"
            })
        
        # Сигнал 3: Funding rate
        if abs(funding) >= FUNDING_THRESHOLD:
            direction = "LONG" if funding > 0 else "SHORT"
            confidence = min(abs(funding) * 100000, 80)
            signals.append({
                "type": "FUNDING",
                "symbol": symbol,
                "direction": direction,
                "confidence": round(confidence),
                "reason": f"Funding: {funding*100:.4f}%"
            })
        
        # Сигнал 3: Совпадение с вердиктом
        if verdict and verdict.get("verdict"):
            v = verdict["verdict"]
            
            if v == "BULLISH" and price_change > 1:
                signals.append({
                    "type": "VERDICT_MATCH",
                    "symbol": symbol,
                    "direction": "LONG",
                    "confidence": 75,
                    "reason": "Наш вердикт: БЫЧИЙ + рост"
                })
            elif v == "BEARISH" and price_change < -1:
                signals.append({
                    "type": "VERDICT_MATCH",
                    "symbol": symbol,
                    "direction": "SHORT",
                    "confidence": 75,
                    "reason": "Наш вердикт: МЕДВЕЖИЙ + падение"
                })
            
    return signals


def build_signals_message(signals: list, binance_data: dict, verdict: Optional[dict]) -> str:
    """Формирует красивое сообщение с сигналами."""
    lines = [
        "📡 *MARKET SIGNALS*",
        f"_{datetime.now().strftime('%d.%m %H:%M UTC')}_",
        "",
    ]
    
    # Данные рынка (Bybit если есть, иначе Binance)
    has_bybit = any(data.get("has_traders_data") for data in binance_data.values()) if binance_data else False
    source = "Bybit" if has_bybit else "Binance"
    lines.append(f"📊 *ТРЕЙДЕРЫ ({source})*")
    
    if not binance_data:
        lines.append("Ситуация неопределена")
    else:
        for symbol, data in binance_data.items():
            name = symbol.replace("USDT", "")
            price_change = data.get("price_change", 0)
            funding = data.get("funding_rate", 0)
            long_pct = data.get("long", 0)
            short_pct = data.get("short", 0)
            
            # Если есть данные Bybit трейдеров
            if long_pct > 0 or short_pct > 0:
                dominant = "🟢" if long_pct > short_pct else "🔴"
                lines.append(f"{name}:")
                lines.append(f"  🔼 Лонг: {long_pct}%")
                lines.append(f"  🔽 Шорт: {short_pct}%")
                lines.append(f"  {dominant} Доминирование")
            else:
                # Fallback на цену
                emoji = "🟢" if price_change > 0 else "🔴" if price_change < 0 else "⚪️"
                change_str = f"{emoji} {price_change:+.2f}%"
                funding_str = f"Funding: {'🔼' if funding > 0 else '🔽'}{funding*100:.4f}%"
                
                lines.append(f"{name}:")
                lines.append(f"  {change_str}")
                lines.append(f"  {funding_str}")
            lines.append("")
    
    # Вердикт
    if verdict and verdict.get("verdict"):
        v = verdict["verdict"]
        emoji = "🐂" if v == "BULLISH" else "🐻" if v == "BEARISH" else "⚪️"
        lines.append(f"{emoji} *НАШ ВЕРДИКТ*")
        lines.append(v)
    else:
        lines.append("🎯 *НАШ ВЕРДИКТ*")
        lines.append("Ситуация неопределена")
    
    lines.append("")
    
    # Сигналы
    if signals:
        lines.append("🔔 *СИГНАЛЫ*")
        
        for s in signals:
            emoji = "🟢" if s["direction"] == "LONG" else "🔴"
            conf = s["confidence"]
            conf_emoji = "✅" if conf >= 70 else "⚠️"
            
            lines.append(f"{emoji} {s['symbol']} → {s['direction']} {conf_emoji}{conf}%")
            lines.append(f"   {s['reason']}")
            lines.append("")
    else:
        lines.append("⚪️ *СИГНАЛЫ*")
        lines.append("Ситуация неопределена")
    
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
