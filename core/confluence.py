"""
confluence.py — Агрегатор сигналов (Confluence Score).

Собирает все факторы (Режим, Киты, RSI, Макро, Тренды) в единую оценку 0-100.
Это превращает "черный ящик" в прозрачную систему с понятной логикой.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import List, Optional

logger = logging.getLogger(__name__)


@dataclass
class Factor:
    """Один фактор влияния."""
    name: str
    weight: float       # Важность фактора (0-1)
    score: float        # Оценка (-1 до 1)
    reason: str         # Почему такая оценка

    @property
    def weighted_score(self) -> float:
        return self.weight * self.score


@dataclass
class ConfluenceResult:
    """Итоговый результат анализа."""
    symbol: str
    total_score: float      # 0-100
    verdict: str            # STRONG BUY, BUY, NEUTRAL, SELL, STRONG SELL
    factors: List[Factor]   # Детализация
    summary: str            # Краткое объяснение для юзера

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "score": round(self.total_score, 1),
            "verdict": self.verdict,
            "summary": self.summary,
            "factors": [
                {"name": f.name, "score": round(f.score, 2), "reason": f.reason}
                for f in self.factors
            ],
        }


class ConfluenceEngine:
    """
    Считает итоговую оценку на основе доступных данных.
    """

    def calculate(
        self,
        symbol: str,
        regime: Optional[str] = None,
        regime_conf: float = 0.5,
        rsi: float = 50.0,
        whale_sentiment: str = "NEUTRAL",
        macro_dxy_trend: str = "NEUTRAL",  # UP/DOWN/NEUTRAL
        funding_rate: float = 0.0,
        trend_alignment: float = 0.5,       # 0-1 (Multi-TF alignment)
    ) -> ConfluenceResult:
        
        factors = []

        # 1. Режим рынка (Вес: 30%)
        regime_score = 0.0
        regime_reason = "Нейтральный фон"
        
        if regime == "UPTREND":
            regime_score = regime_conf
            regime_reason = f"Бычий тренд (уверенность {regime_conf:.0%})"
        elif regime == "DOWNTREND":
            regime_score = -regime_conf
            regime_reason = f"Медвежий тренд (уверенность {regime_conf:.0%})"
        elif regime == "HIGH_VOL":
            regime_score = -0.5
            regime_reason = "Аномальная волатильность"
            
        factors.append(Factor("Market Regime", 0.30, regime_score, regime_reason))

        # 2. RSI (Вес: 20%)
        # Низкий RSI (<30) = Хорошо для покупки (Score +1)
        # Высокий RSI (>70) = Плохо для покупки (Score -1)
        rsi_score = 0.0
        if rsi < 30:
            rsi_score = 1.0 - (rsi / 30) # 1.0 at 0, 0.0 at 30
            rsi_reason = f"Перепроданность ({rsi:.1f})"
        elif rsi > 70:
            rsi_score = (70 - rsi) / 30  # 0.0 at 70, -1.0 at 100
            rsi_reason = f"Перекупленность ({rsi:.1f})"
        else:
            rsi_score = 0.0
            rsi_reason = f"Норма ({rsi:.1f})"
            
        factors.append(Factor("RSI (14)", 0.20, rsi_score, rsi_reason))

        # 3. Киты (Вес: 20%)
        whale_score = 0.0
        whale_reason = "Активности нет"
        if whale_sentiment == "BULLISH":
            whale_score = 1.0
            whale_reason = "Киты покупают"
        elif whale_sentiment == "BEARISH":
            whale_score = -1.0
            whale_reason = "Киты продают"
            
        factors.append(Factor("Whale Flow", 0.20, whale_score, whale_reason))

        # 4. Макро (Вес: 15%)
        # DXY UP = Crypto DOWN (обычно)
        macro_score = 0.0
        macro_reason = "Нейтрально"
        if macro_dxy_trend == "UP":
            macro_score = -0.8
            macro_reason = "Доллар растет (давит на крипту)"
        elif macro_dxy_trend == "DOWN":
            macro_score = 0.8
            macro_reason = "Доллар падает (поддержка крипты)"
            
        factors.append(Factor("Macro (DXY)", 0.15, macro_score, macro_reason))

        # 5. Тренды (Вес: 15%)
        # Совпадение таймфреймов
        tf_score = (trend_alignment - 0.5) * 2 # Нормализуем 0-1 к -1...1
        tf_reason = f"Согласованность ТФ: {trend_alignment:.0%}"
        factors.append(Factor("Timeframes", 0.15, tf_score, tf_reason))

        # === ИТОГО ===
        raw_score = sum(f.weighted_score for f in factors) # от -1 до 1
        
        # Превращаем в 0-100
        final_score = int((raw_score + 1) * 50)
        final_score = max(0, min(100, final_score))

        # Вердикт
        if final_score >= 80:
            verdict = "STRONG BUY"
        elif final_score >= 60:
            verdict = "BUY"
        elif final_score <= 20:
            verdict = "STRONG SELL"
        elif final_score <= 40:
            verdict = "SELL"
        else:
            verdict = "NEUTRAL"

        # Саммари
        bullish_factors = [f for f in factors if f.score > 0.2]
        bearish_factors = [f for f in factors if f.score < -0.2]
        
        summary_parts = []
        if bullish_factors:
            summary_parts.append("✅ " + ", ".join(f.reason for f in bullish_factors))
        if bearish_factors:
            summary_parts.append("❌ " + ", ".join(f.reason for f in bearish_factors))
            
        summary = "\n".join(summary_parts) if summary_parts else "Рынок в равновесии"

        return ConfluenceResult(
            symbol=symbol,
            total_score=final_score,
            verdict=verdict,
            factors=factors,
            summary=summary,
        )
