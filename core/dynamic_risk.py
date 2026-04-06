"""
dynamic_risk.py — Профессиональный риск-менеджмент.

- ATR-based стопы (не фиксированные %, а по реальной волатильности)
- Kelly Criterion для размера позиции
- Max drawdown защита
- Correlation-adjusted sizing
- Position sizing по режиму рынка
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class RiskParams:
    """Параметры риска для одной сделки."""
    position_size_pct: float   # % капитала на сделку
    stop_loss_pct: float       # % стоп-лосс
    take_profit_pct: float     # % тейк-профит
    max_portfolio_risk: float  # Макс % капитала под риском
    kelly_fraction: float      # Доля Kelly (0.0-1.0)
    atr_stop_multiplier: float # ATR множитель для стопа
    max_drawdown_pct: float    # Остановка при просадке
    correlation_penalty: float # Штраф за коррелированные позиции


class DynamicRiskManager:
    """
    Профессиональный риск-менеджмент.
    
    Адаптирует размер позиции и стопы под:
    - Волатильность (ATR)
    - Режим рынка
    - Историю винрейта
    - Текущую просадку
    """

    def __init__(
        self,
        base_risk_pct: float = 0.02,       # 2% базовый риск
        kelly_fraction: float = 0.25,       # Quarter Kelly
        max_drawdown_pct: float = 0.25,     # Стоп при -25%
        atr_stop_multiplier: float = 2.0,   # 2x ATR стоп
        max_correlated_positions: int = 3,  # Макс коррелированных позиций
    ):
        self.base_risk_pct = base_risk_pct
        self.kelly_fraction = kelly_fraction
        self.max_drawdown_pct = max_drawdown_pct
        self.atr_stop_multiplier = atr_stop_multiplier
        self.max_correlated_positions = max_correlated_positions

        # История для Kelly
        self._wins = 0
        self._losses = 0
        self._avg_win = 0.0
        self._avg_loss = 0.0
        self._total_pnl = 0.0
        self._peak_capital = 100.0
        self._current_capital = 100.0

    def update_capital(self, capital: float):
        """Обновить текущий капитал."""
        self._current_capital = capital
        if capital > self._peak_capital:
            self._peak_capital = capital

    def record_trade(self, pnl_pct: float, is_win: bool):
        """Записать результат сделки для Kelly."""
        if is_win:
            self._wins += 1
            n = self._wins
            self._avg_win = self._avg_win + (pnl_pct - self._avg_win) / n
        else:
            self._losses += 1
            n = self._losses
            self._avg_loss = self._avg_loss + (abs(pnl_pct) - self._avg_loss) / n
        self._total_pnl += pnl_pct

    def kelly_percentage(self) -> float:
        """
        Kelly Criterion: f* = (bp - q) / b
        b = avg_win / avg_loss
        p = win_rate
        q = 1 - p
        """
        if self._wins + self._losses < 10:
            return self.base_risk_pct  # Недостаточно данных

        p = self._wins / (self._wins + self._losses)
        q = 1 - p

        if self._avg_loss == 0:
            return self.base_risk_pct

        b = self._avg_win / self._avg_loss
        kelly = (b * p - q) / b

        # Quarter Kelly для безопасности
        return max(0, kelly * self.kelly_fraction)

    def should_stop_trading(self) -> tuple[bool, str]:
        """Проверить, нужно ли остановить торговлю."""
        # Drawdown check
        if self._peak_capital > 0:
            drawdown = (self._peak_capital - self._current_capital) / self._peak_capital
            if drawdown >= self.max_drawdown_pct:
                return True, f"Max drawdown reached: {drawdown*100:.1f}%"

        # Losing streak check
        if self._losses > 0 and self._wins == 0 and self._losses >= 5:
            return True, f"5 consecutive losses"

        return False, ""

    def calculate_position_size(
        self,
        capital: float,
        entry_price: float,
        stop_price: float,
        atr: float = 0,
        regime: str = "NEUTRAL",
        correlation_count: int = 0,
    ) -> dict:
        """
        Рассчитать размер позиции.
        
        Returns:
            {
                "quantity": float,      # Сколько единиц купить
                "position_value": float, # $ стоимость
                "risk_amount": float,    # $ под риском
                "risk_pct": float,       # % капитала под риском
                "stop_price": float,     # Итоговый стоп
                "take_profit": float,    # Итоговый тейк
                "kelly_pct": float,      # Kelly рекомендация
            }
        """
        if entry_price <= 0:
            return {"error": "invalid_entry"}

        # 1. ATR-based стоп
        if atr > 0:
            atr_stop_distance = atr * self.atr_stop_multiplier
            stop_distance = max(atr_stop_distance, abs(entry_price - stop_price))
        else:
            stop_distance = abs(entry_price - stop_price)

        if stop_distance == 0:
            stop_distance = entry_price * 0.02  # Fallback 2%

        final_stop = entry_price - stop_distance

        # 2. Kelly-based размер
        kelly_pct = self.kelly_percentage()
        risk_pct = min(kelly_pct, self.base_risk_pct)

        # 3. Режим рынка
        regime_multipliers = {
            "UPTREND": 1.0,
            "DOWNTREND": 0.7,
            "SIDEWAYS": 0.4,
            "HIGH_VOL": 0.5,
        }
        regime_mult = regime_multipliers.get(regime, 0.7)
        risk_pct *= regime_mult

        # 4. Корреляционный штраф
        if correlation_count >= self.max_correlated_positions:
            risk_pct *= 0.3  # Сильное снижение
        elif correlation_count > 0:
            risk_pct *= (1 - correlation_count * 0.15)

        # 5. Drawdown adjustment
        if self._peak_capital > 0:
            drawdown = (self._peak_capital - self._current_capital) / self._peak_capital
            if drawdown > 0.1:
                risk_pct *= max(0.3, 1 - drawdown * 2)

        # 6. Финальный расчёт
        risk_amount = capital * risk_pct
        quantity = risk_amount / stop_distance
        position_value = quantity * entry_price

        # Тейк: минимум 1.5:1 R/R
        take_profit = entry_price + (stop_distance * 1.5)

        return {
            "quantity": round(quantity, 8),
            "position_value": round(position_value, 2),
            "risk_amount": round(risk_amount, 2),
            "risk_pct": round(risk_pct * 100, 2),
            "stop_price": round(final_stop, 4),
            "take_profit": round(take_profit, 4),
            "kelly_pct": round(kelly_pct * 100, 2),
            "regime_multiplier": regime_mult,
        }

    def get_risk_summary(self) -> dict:
        """Сводка текущего состояния риска."""
        drawdown = 0.0
        if self._peak_capital > 0:
            drawdown = (self._peak_capital - self._current_capital) / self._peak_capital

        total_trades = self._wins + self._losses
        win_rate = self._wins / total_trades if total_trades > 0 else 0

        return {
            "current_capital": round(self._current_capital, 2),
            "peak_capital": round(self._peak_capital, 2),
            "drawdown_pct": round(drawdown * 100, 2),
            "total_trades": total_trades,
            "wins": self._wins,
            "losses": self._losses,
            "win_rate": round(win_rate * 100, 1),
            "avg_win": round(self._avg_win, 2),
            "avg_loss": round(self._avg_loss, 2),
            "kelly_pct": round(self.kelly_percentage() * 100, 2),
            "total_pnl": round(self._total_pnl, 2),
            "should_stop": self.should_stop_trading()[0],
            "stop_reason": self.should_stop_trading()[1],
        }
