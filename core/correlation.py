"""
correlation.py — Матрица корреляций активов.

Помогает избежать дублирования риска (например, не открывать BTC и ETH одновременно,
если они движутся синхронно).
"""

from __future__ import annotations

import logging
from typing import Dict, List, Optional

import numpy as np

from backtester import get_candles, Candle

logger = logging.getLogger(__name__)


class CorrelationMatrix:
    """
    Считает корреляцию доходностей между активами.
    """

    def __init__(self, threshold: float = 0.85):
        self.threshold = threshold

    async def calculate(self, symbols: List[str], timeframe_hours: int = 24, limit: int = 30) -> Dict[str, Dict[str, float]]:
        """
        Рассчитать корреляцию для списка символов.
        Возвращает матрицу {sym1: {sym2: corr, ...}, ...}
        """
        if len(symbols) < 2:
            return {}

        # 1. Загружаем свечи
        candles_map: Dict[str, List[Candle]] = {}
        tasks = [get_candles(sym, timeframe_hours, limit) for sym in symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for sym, res in zip(symbols, results):
            if not isinstance(res, Exception) and res:
                candles_map[sym] = res

        if len(candles_map) < 2:
            return {}

        # 2. Считаем доходности (returns)
        returns_matrix = []
        valid_symbols = []
        
        # Выравниваем длину (берем минимум)
        min_len = min(len(c) for c in candles_map.values())
        
        closes = []
        for sym in candles_map:
            c = candles_map[sym]
            # Берем последние min_len свечей
            prices = [x.close for x in c[-min_len:]]
            # Считаем % изменение
            rets = [(prices[i] - prices[i-1]) / prices[i-1] for i in range(1, len(prices))]
            closes.append(rets)
            valid_symbols.append(sym)

        if len(closes) < 2 or len(closes[0]) < 2:
            return {}

        # 3. Корреляция Пирсона
        corr_matrix = np.corrcoef(closes)

        # 4. Форматируем вывод
        result = {}
        for i, sym1 in enumerate(valid_symbols):
            result[sym1] = {}
            for j, sym2 in enumerate(valid_symbols):
                if sym1 == sym2:
                    result[sym1][sym2] = 1.0
                else:
                    val = float(corr_matrix[i, j])
                    result[sym1][sym2] = round(val, 3)

        return result

    def check_conflict(self, symbol: str, held_symbols: List[str], matrix: Dict[str, Dict[str, float]]) -> Optional[str]:
        """
        Проверить, есть ли конфликт (высокая корреляция) с уже открытыми позициями.
        Возвращает символ конфликтующего актива или None.
        """
        if symbol not in matrix:
            return None
            
        for held in held_symbols:
            if held in matrix[symbol]:
                corr = matrix[symbol][held]
                if corr > self.threshold:
                    return held
        return None

