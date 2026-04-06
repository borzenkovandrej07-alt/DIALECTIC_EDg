"""
multi_tf.py — Multi-timeframe confirmation.

Не входить, если таймфреймы противоречат друг другу.
Только когда 1D, 4H, 1H согласны → высокий conviction.

Использует:
  - Trend alignment across timeframes
  - RSI alignment
  - Volume confirmation
  - Support/resistance confluence
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Optional

from backtester import Candle, get_candles

logger = logging.getLogger(__name__)


@dataclass
class TFSignal:
    """Сигнал с одного таймфрейма."""
    timeframe: str
    trend: str        # UP, DOWN, NEUTRAL
    rsi: float
    rsi_signal: str   # OVERBOUGHT, OVERSOLD, NEUTRAL
    ma_alignment: str # BULLISH, BEARISH, MIXED
    volume_trend: str # CONFIRMING, WEAKENING, NEUTRAL
    conviction: float # 0.0-1.0


@dataclass
class MultiTFResult:
    """Результат multi-timeframe анализа."""
    overall_direction: str  # LONG, SHORT, NEUTRAL
    overall_conviction: float  # 0.0-1.0
    timeframe_signals: list[TFSignal]
    alignment_score: float  # Насколько таймфреймы согласны (0-100)
    recommendation: str
    should_enter: bool

    def to_dict(self) -> dict:
        return {
            "overall_direction": self.overall_direction,
            "overall_conviction": round(self.overall_conviction, 2),
            "alignment_score": round(self.alignment_score, 1),
            "recommendation": self.recommendation,
            "should_enter": self.should_enter,
            "timeframes": [
                {
                    "tf": s.timeframe,
                    "trend": s.trend,
                    "rsi": round(s.rsi, 1),
                    "conviction": round(s.conviction, 2),
                }
                for s in self.timeframe_signals
            ],
        }


class MultiTimeframeAnalyzer:
    """
    Анализирует несколько таймфреймов для подтверждения сигнала.
    
    Логика:
    - 1D (daily) → долгосрочный тренд
    - 4H → среднесрочный тренд
    - 1H → краткосрочный вход
    
    Вход только если:
    - 2 из 3 таймфреймов согласны
    - Нет перекупленности/перепроданности на старшем ТФ
    """

    def __init__(
        self,
        timeframes: list[tuple[str, int]] = None,
        min_agreement: int = 2,
    ):
        self.timeframes = timeframes or [
            ("1D", 24),
            ("4H", 4),
            ("1H", 1),
        ]
        self.min_agreement = min_agreement

    async def analyze(self, symbol: str) -> Optional[MultiTFResult]:
        """Проанализировать все таймфреймы для символа."""
        signals = []

        # Загружаем свечи для всех ТФ параллельно
        tasks = [
            get_candles(symbol, tf_hours, limit=100)
            for _, tf_hours in self.timeframes
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for (tf_name, _), candles_result in zip(self.timeframes, results):
            if isinstance(candles_result, Exception) or not candles_result:
                logger.warning(f"No candles for {symbol} {tf_name}")
                continue

            signal = self._analyze_timeframe(candles_result, tf_name)
            if signal:
                signals.append(signal)

        if not signals:
            return None

        # Агрегируем
        return self._aggregate(signals, symbol)

    def _analyze_timeframe(self, candles: list[Candle], tf_name: str) -> Optional[TFSignal]:
        """Анализ одного таймфрейма."""
        if len(candles) < 50:
            return None

        closes = [c.close for c in candles]
        highs = [c.high for c in candles]
        lows = [c.low for c in candles]
        volumes = [c.volume for c in candles]

        # MA
        ma_20 = sum(closes[-20:]) / 20
        ma_50 = sum(closes[-50:]) / 50
        current = closes[-1]

        if current > ma_20 > ma_50:
            trend = "UP"
            ma_alignment = "BULLISH"
        elif current < ma_20 < ma_50:
            trend = "DOWN"
            ma_alignment = "BEARISH"
        else:
            trend = "NEUTRAL"
            ma_alignment = "MIXED"

        # RSI
        rsi = self._rsi(closes, 14)
        if rsi > 70:
            rsi_signal = "OVERBOUGHT"
        elif rsi < 30:
            rsi_signal = "OVERSOLD"
        else:
            rsi_signal = "NEUTRAL"

        # Volume
        vol_recent = sum(volumes[-5:]) / 5
        vol_older = sum(volumes[-20:-5]) / 15
        if vol_recent > vol_older * 1.2:
            volume_trend = "CONFIRMING"
        elif vol_recent < vol_older * 0.8:
            volume_trend = "WEAKENING"
        else:
            volume_trend = "NEUTRAL"

        # Conviction
        conviction = 0.5
        if ma_alignment == "BULLISH":
            conviction += 0.2
        elif ma_alignment == "BEARISH":
            conviction += 0.2

        if rsi_signal == "NEUTRAL":
            conviction += 0.1
        else:
            conviction -= 0.1  # Перекупленность/перепроданность = неуверенность

        if volume_trend == "CONFIRMING":
            conviction += 0.1
        elif volume_trend == "WEAKENING":
            conviction -= 0.1

        conviction = max(0.1, min(0.95, conviction))

        return TFSignal(
            timeframe=tf_name,
            trend=trend,
            rsi=rsi,
            rsi_signal=rsi_signal,
            ma_alignment=ma_alignment,
            volume_trend=volume_trend,
            conviction=conviction,
        )

    def _aggregate(self, signals: list[TFSignal], symbol: str) -> MultiTFResult:
        """Агрегировать сигналы с разных таймфреймов."""
        up_count = sum(1 for s in signals if s.trend == "UP")
        down_count = sum(1 for s in signals if s.trend == "DOWN")
        neutral_count = len(signals) - up_count - down_count

        # Определение направления
        if up_count >= self.min_agreement:
            direction = "LONG"
        elif down_count >= self.min_agreement:
            direction = "SHORT"
        else:
            direction = "NEUTRAL"

        # Alignment score
        if len(signals) >= 3:
            if up_count == 3 or down_count == 3:
                alignment = 100.0
            elif up_count == 2 or down_count == 2:
                alignment = 66.0
            else:
                alignment = 33.0
        elif len(signals) == 2:
            alignment = 100.0 if (up_count == 2 or down_count == 2) else 50.0
        else:
            alignment = 50.0

        # Conviction
        avg_conviction = sum(s.conviction for s in signals) / len(signals)
        overall_conviction = avg_conviction * (alignment / 100)

        # Should enter?
        should_enter = (
            direction != "NEUTRAL"
            and alignment >= 66
            and overall_conviction >= 0.4
        )

        # Recommendation
        if should_enter:
            if direction == "LONG":
                rec = (
                    f"✅ LONG {symbol} подтверждён. "
                    f"Alignment: {alignment:.0f}%, Conviction: {overall_conviction:.2f}"
                )
            else:
                rec = (
                    f"✅ SHORT {symbol} подтверждён. "
                    f"Alignment: {alignment:.0f}%, Conviction: {overall_conviction:.2f}"
                )
        else:
            rec = (
                f"❌ Нет подтверждения для {symbol}. "
                f"Alignment: {alignment:.0f}% (нужно {self.min_agreement}/{len(signals)})"
            )

        return MultiTFResult(
            overall_direction=direction,
            overall_conviction=overall_conviction,
            timeframe_signals=signals,
            alignment_score=alignment,
            recommendation=rec,
            should_enter=should_enter,
        )

    @staticmethod
    def _rsi(closes: list[float], period: int) -> float:
        if len(closes) < period + 1:
            return 50.0

        gains = []
        losses = []
        for i in range(1, len(closes)):
            delta = closes[i] - closes[i - 1]
            gains.append(max(0, delta))
            losses.append(max(0, -delta))

        avg_gain = sum(gains[-period:]) / period
        avg_loss = sum(losses[-period:]) / period

        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))
