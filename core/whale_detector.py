"""
whale_detector.py — Мониторинг крупных сделок (Китов).

Использует Binance Recent Trades API для обнаружения аномальных объемов.
"""

from __future__ import annotations

import asyncio
import logging
from typing import List, Optional

import aiohttp

logger = logging.getLogger(__name__)

# Порог "Китовской" сделки в USDT
WHALE_THRESHOLD_USDT = 500_000  # 500k USD

class WhaleDetector:
    def __init__(self, threshold: float = WHALE_THRESHOLD_USDT):
        self.threshold = threshold
        self._recent_whales: list[dict] = []

    async def check_for_whales(self, symbol: str = "BTCUSDT") -> list[dict]:
        """
        Проверить последние сделки на наличие китов.
        Возвращает список крупных сделок.
        """
        try:
            url = f"https://api.binance.com/api/v3/trades"
            params = {"symbol": symbol, "limit": 500}  # Последние 500 сделок
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    if resp.status == 200:
                        trades = await resp.json()
                        whales = []
                        
                        for t in trades:
                            price = float(t["price"])
                            qty = float(t["qty"])
                            value = price * qty
                            
                            if value >= self.threshold:
                                whale_data = {
                                    "symbol": symbol,
                                    "price": price,
                                    "qty": qty,
                                    "value": value,
                                    "side": "BUY" if not t["isBuyerMaker"] else "SELL", # isBuyerMaker=True значит SELL (тейкер sell)
                                    "time": t["time"],
                                }
                                whales.append(whale_data)
                        
                        # Сохраняем для истории
                        if whales:
                            self._recent_whales.extend(whales)
                            # Храним только последние 100
                            self._recent_whales = self._recent_whales[-100:]
                            logger.warning(f"🐋 WHALE ALERT: {len(whales)} сделок по {symbol} на сумму > ${self.threshold/1000:.0f}k")
                        
                        return whales
        except Exception as e:
            logger.debug(f"Whale check error: {e}")
        return []

    def get_whale_sentiment(self, symbol: str) -> str:
        """
        Определить настроение китов по последним сделкам.
        """
        relevant = [w for w in self._recent_whales if w["symbol"] == symbol]
        if not relevant:
            return "NEUTRAL"
        
        # Берем последние 10 китовых сделок
        recent = relevant[-10:]
        buy_vol = sum(w["value"] for w in recent if w["side"] == "BUY")
        sell_vol = sum(w["value"] for w in recent if w["side"] == "SELL")
        
        if buy_vol > sell_vol * 1.5:
            return "BULLISH"
        elif sell_vol > buy_vol * 1.5:
            return "BEARISH"
        return "NEUTRAL"
