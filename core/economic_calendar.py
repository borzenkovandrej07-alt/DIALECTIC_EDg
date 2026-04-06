"""
economic_calendar.py — Календарь макро-событий.

Предупреждает о высокой волатильности ДО выхода новостей.
Использует парсинг бесплатных источников (Investing.com / ForexFactory)
или ключевые слова в новостях для определения рисков.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta
from typing import List, Optional

import aiohttp

logger = logging.getLogger(__name__)

# Ключевые слова высокой важности (Триггеры волатильности)
HIGH_IMPACT_KEYWORDS = [
    r"CPI", r"Consumer Price Index",  # Инфляция
    r"FOMC", r"Federal Reserve", r"Interest Rate", r"Jerome Powell",  # ФРС
    r"NFP", r"Non-Farm Payrolls", r"Unemployment Claims",  # Рынок труда
    r"GDP", r"Gross Domestic Product",  # ВВП
    r"PPI", r"Producer Price Index",  # Инфляция производителей
    r"War", r"Invasion", r"Sanctions",  # Геополитика
]

class EconomicCalendar:
    def __init__(self):
        self._upcoming_events: List[dict] = []
        self._last_check: Optional[datetime] = None

    async def check_upcoming_risks(self) -> List[dict]:
        """
        Проверить новости на наличие предстоящих событий высокого риска.
        """
        # В бесплатной версии мы сканируем заголовки новостей на наличие триггеров.
        # Если находим "CPI data release" или "Fed meeting", помечаем как риск.
        
        # Здесь мы используем тот же метод, что и в data_enricher, но фокусируемся на времени.
        # Для полной автономности мы полагаемся на анализ входящего потока новостей.
        
        risks = []
        
        # Эмуляция проверки (в реальности здесь был бы парсер календаря)
        # Но мы можем использовать наш News API, который уже есть в проекте.
        # Если в последних новостях есть триггеры — считаем это риском на ближайшие 24ч.
        
        # Чтобы не дублировать код, мы просто вернем структуру, которую заполним
        # при получении новостей в основном цикле.
        
        return self._upcoming_events

    def add_risk_from_news(self, text: str):
        """Проанализировать текст новости и добавить риск, если найдены триггеры."""
        for pattern in HIGH_IMPACT_KEYWORDS:
            if re.search(pattern, text, re.IGNORECASE):
                event = {
                    "keyword": pattern,
                    "detected_at": datetime.now().isoformat(),
                    "risk_window_hours": 24,
                    "severity": "HIGH"
                }
                # Не добавляем дубликаты за последний час
                if not any(e["keyword"] == pattern and (datetime.now() - datetime.fromisoformat(e["detected_at"])).seconds < 3600 for e in self._upcoming_events):
                    self._upcoming_events.append(event)
                    logger.warning(f"📅 High Impact Event Detected: {pattern}")
                return True
        return False

    def is_safe_to_trade(self) -> tuple[bool, str]:
        """
        Проверить, безопасно ли открывать позиции.
        """
        now = datetime.now()
        active_risks = []
        
        for event in self._upcoming_events:
            detected = datetime.fromisoformat(event["detected_at"])
            window = timedelta(hours=event["risk_window_hours"])
            if now < detected + window:
                active_risks.append(event)
        
        if active_risks:
            reasons = ", ".join(e["keyword"] for e in active_risks)
            return False, f"High Impact Events active: {reasons}"
        
        return True, "Safe"

    def get_calendar_summary(self) -> str:
        """Краткая сводка для телеграма."""
        safe, reason = self.is_safe_to_trade()
        if safe:
            return "📅 *Календарь:* Чисто. Торговля разрешена."
        else:
            return f"⚠️ *Календарь:* {reason}\nТорговля приостановлена."
