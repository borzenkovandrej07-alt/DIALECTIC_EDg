"""
event_defense.py — Защита от событий высокого риска.

Сканирует новости и контекст на наличие "Красных Флагов".
При обнаружении — переводит систему в режим DEFENSE.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import List, Optional

logger = logging.getLogger(__name__)


@dataclass
class RiskEvent:
    """Обнаруженное рискованное событие."""
    keyword: str
    severity: str  # LOW, MEDIUM, HIGH, CRITICAL
    context: str
    source: str = "news_scan"


# Триггеры высокого уровня
CRITICAL_TRIGGERS = [
    r"rate\s*hike", r"hiked\s*rates", r"fed\s*raises",  # ФРС
    r"ban\s*crypto", r"crypto\s*ban", r"illegal\s*bitcoin",  # Запреты
    r"exchange\s*hack", r"hack.*exchange", r"stolen.*million",  # Взломы
    r"war", r"military\s*action", r"invasion",  # Война
    r"delist", r"sec\s*sues", r"sec\s*charges",  # SEC
]

HIGH_TRIGGERS = [
    r"recession", r"economic\s*crisis", r"crash",  # Кризис
    r"sanctions", r"embargo",  # Санкции
    r"liquidation.*billion", r"liquidation.*million",  # Массовые ликвидации
    r"bankruptcy", r"insolvency",  # Банкротства
    r"rug\s*pull", r"scam", r"fraud",  # Мошенничество
]

MEDIUM_TRIGGERS = [
    r"volatility", r"volatile",  # Волатильность
    r"uncertainty", r"uncertain",  # Неопределенность
    r"sell.?off", r"dump", r"plunge",  # Распродажи
    r"inflation", r"cpi\s*rise",  # Инфляция
]


class EventDefense:
    """
    Система защиты от событий.
    """

    def __init__(self):
        self._active_events: List[RiskEvent] = []
        self._defense_mode = False

    @property
    def is_defense_mode(self) -> bool:
        return self._defense_mode

    def scan_text(self, text: str) -> List[RiskEvent]:
        """
        Просканировать текст на наличие красных флагов.
        """
        if not text:
            return []

        events = []
        text_lower = text.lower()

        # Проверяем каждый триггер
        for pattern in CRITICAL_TRIGGERS:
            if re.search(pattern, text_lower):
                events.append(RiskEvent(
                    keyword=pattern,
                    severity="CRITICAL",
                    context=self._extract_context(text, pattern),
                ))

        for pattern in HIGH_TRIGGERS:
            if re.search(pattern, text_lower):
                events.append(RiskEvent(
                    keyword=pattern,
                    severity="HIGH",
                    context=self._extract_context(text, pattern),
                ))

        for pattern in MEDIUM_TRIGGERS:
            if re.search(pattern, text_lower):
                events.append(RiskEvent(
                    keyword=pattern,
                    severity="MEDIUM",
                    context=self._extract_context(text, pattern),
                ))

        self._active_events = events
        self._update_defense_mode()
        return events

    def _update_defense_mode(self):
        """Обновить режим защиты на основе событий."""
        critical_count = sum(1 for e in self._active_events if e.severity == "CRITICAL")
        high_count = sum(1 for e in self._active_events if e.severity == "HIGH")

        # DEFENSE включается если:
        # - 1+ CRITICAL событие
        # - ИЛИ 2+ HIGH события
        self._defense_mode = (critical_count >= 1) or (high_count >= 2)

    def get_defense_recommendation(self) -> str:
        """Получить рекомендацию в режиме защиты."""
        if not self._defense_mode:
            return "NORMAL — Торговля в обычном режиме."

        critical = [e for e in self._active_events if e.severity == "CRITICAL"]
        high = [e for e in self._active_events if e.severity == "HIGH"]

        recs = ["⚠️ РЕЖИМ ЗАЩИТЫ АКТИВИРОВАН"]
        
        if critical:
            recs.append(f"• Критические риски: {len(critical)}")
            recs.append("• ЗАКРЫТЬ все рискованные позиции")
            recs.append("• НЕ открывать новые сделки")
            recs.append("• Рассмотреть хеджирование или кэш")
        
        if high:
            recs.append(f"• Высокие риски: {len(high)}")
            recs.append("• Уменьшить размер позиций на 50%")
            recs.append("• Ужесточить стоп-лоссы")

        return "\n".join(recs)

    @staticmethod
    def _extract_context(text: str, pattern: str) -> str:
        """Извлечь контекст вокруг найденного триггера."""
        match = re.search(pattern, text.lower())
        if not match:
            return ""
        
        start = max(0, match.start() - 50)
        end = min(len(text), match.end() + 50)
        return "..." + text[start:end] + "..."
