"""Market Data Provider Implementations - Binance and Yahoo Finance"""

import asyncio
import logging
from dataclasses import dataclass
from typing import Optional, List, Dict, Any, AsyncGenerator
from datetime import datetime, timedelta

try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False

logger = logging.getLogger(__name__)

if not AIOHTTP_AVAILABLE:
    logger.warning("aiohttp not installed. Market data providers will not work.")


@dataclass
class OHLCV:
    """Open-High-Low-Close-Volume свеча"""
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass
class PriceData:
    """Текущая цена"""
    symbol: str
    price: float
    change_24h: float
    change_24h_percent: float
    high_24h: float
    low_24h: float
    volume_24h: float
    timestamp: datetime


class BinanceProvider:
    """Binance REST API провайдер"""

    def __init__(
        self,
        base_url: str = "https://api.binance.com/api/v3",
        timeout: int = 10,
    ):
        self.base_url = base_url
        self.timeout = timeout
        self.session: Optional[aiohttp.ClientSession] = None

    async def initialize(self) -> None:
        """Инициализация сессии"""
        if not self.session:
            self.session = aiohttp.ClientSession()

    async def close(self) -> None:
        """Закрытие сессии"""
        if self.session:
            await self.session.close()

    async def get_price(self, symbol: str) -> Optional[PriceData]:
        """Получение текущей цены"""
        await self.initialize()

        # Binance используется символы как BTCUSDT
        binance_symbol = f"{symbol.upper()}USDT"

        try:
            async with self.session.get(
                f"{self.base_url}/ticker/24hr",
                params={"symbol": binance_symbol},
                timeout=aiohttp.ClientTimeout(total=self.timeout),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return PriceData(
                        symbol=symbol,
                        price=float(data["lastPrice"]),
                        change_24h=float(data["priceChange"]),
                        change_24h_percent=float(data["priceChangePercent"]),
                        high_24h=float(data["highPrice"]),
                        low_24h=float(data["lowPrice"]),
                        volume_24h=float(data["volume"]),
                        timestamp=datetime.fromtimestamp(data["closeTime"] / 1000),
                    )
                else:
                    logger.warning(f"Binance price error: {resp.status}")
                    return None

        except asyncio.TimeoutError:
            logger.warning("Binance timeout")
            return None
        except Exception as e:
            logger.warning(f"Binance error: {e}")
            return None

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> List[OHLCV]:
        """Получение исторических свечей (OHLCV)"""
        await self.initialize()

        binance_symbol = f"{symbol.upper()}USDT"

        try:
            async with self.session.get(
                f"{self.base_url}/klines",
                params={
                    "symbol": binance_symbol,
                    "interval": interval,
                    "limit": limit,
                },
                timeout=aiohttp.ClientTimeout(total=self.timeout),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    candles = []
                    for kline in data:
                        candles.append(
                            OHLCV(
                                timestamp=datetime.fromtimestamp(kline[0] / 1000),
                                open=float(kline[1]),
                                high=float(kline[2]),
                                low=float(kline[3]),
                                close=float(kline[4]),
                                volume=float(kline[7]),
                            )
                        )
                    return candles
                else:
                    logger.warning(f"Binance OHLCV error: {resp.status}")
                    return []

        except asyncio.TimeoutError:
            logger.warning("Binance OHLCV timeout")
            return []
        except Exception as e:
            logger.warning(f"Binance OHLCV error: {e}")
            return []

    async def get_order_book(
        self, symbol: str, limit: int = 20
    ) -> Optional[Dict[str, Any]]:
        """Получение биржевого ордербука"""
        await self.initialize()

        binance_symbol = f"{symbol.upper()}USDT"

        try:
            async with self.session.get(
                f"{self.base_url}/depth",
                params={
                    "symbol": binance_symbol,
                    "limit": limit,
                },
                timeout=aiohttp.ClientTimeout(total=self.timeout),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return {
                        "bids": data["bids"],  # [price, quantity]
                        "asks": data["asks"],  # [price, quantity]
                        "timestamp": datetime.fromtimestamp(data["E"] / 1000),
                    }
                else:
                    logger.warning(f"Binance orderbook error: {resp.status}")
                    return None

        except asyncio.TimeoutError:
            logger.warning("Binance orderbook timeout")
            return None
        except Exception as e:
            logger.warning(f"Binance orderbook error: {e}")
            return None


class YahooFinanceProvider:
    """Yahoo Finance провайдер через неофициальный API"""

    def __init__(
        self,
        base_url: str = "https://query2.finance.yahoo.com/v10/finance",
        timeout: int = 10,
    ):
        self.base_url = base_url
        self.timeout = timeout
        self.session: Optional[aiohttp.ClientSession] = None

    async def initialize(self) -> None:
        """Инициализация сессии"""
        if not self.session:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }
            self.session = aiohttp.ClientSession(headers=headers)

    async def close(self) -> None:
        """Закрытие сессии"""
        if self.session:
            await self.session.close()

    async def get_price(self, symbol: str) -> Optional[PriceData]:
        """Получение текущей цены через Yahoo"""
        await self.initialize()

        try:
            async with self.session.get(
                f"{self.base_url}/quoteSummary/{symbol.upper()}",
                params={"modules": "price"},
                timeout=aiohttp.ClientTimeout(total=self.timeout),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("quoteSummary", {}).get("result"):
                        price_data = (
                            data["quoteSummary"]["result"][0]["price"]
                        )
                        return PriceData(
                            symbol=symbol,
                            price=price_data.get("regularMarketPrice", 0),
                            change_24h=price_data.get("regularMarketChange", 0),
                            change_24h_percent=(
                                price_data.get("regularMarketChangePercent", 0)
                            ),
                            high_24h=price_data.get(
                                "fiftyTwoWeekHigh", 0
                            ),
                            low_24h=price_data.get("fiftyTwoWeekLow", 0),
                            volume_24h=price_data.get(
                                "regularMarketVolume", 0
                            ),
                            timestamp=datetime.now(),
                        )
                else:
                    logger.warning(f"Yahoo Finance error: {resp.status}")
                    return None

        except asyncio.TimeoutError:
            logger.warning("Yahoo Finance timeout")
            return None
        except Exception as e:
            logger.warning(f"Yahoo Finance error: {e}")
            return None

    async def get_ohlcv(
        self,
        symbol: str,
        period: str = "1mo",
        interval: str = "1d",
    ) -> List[OHLCV]:
        """Получение исторических данных"""
        await self.initialize()

        try:
            async with self.session.get(
                f"{self.base_url}/chart/{symbol.upper()}",
                params={
                    "range": period,
                    "interval": interval,
                },
                timeout=aiohttp.ClientTimeout(total=self.timeout),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("chart", {}).get("result"):
                        result = data["chart"]["result"][0]
                        timestamps = result["timestamp"]
                        quotes = result["indicators"]["quote"][0]

                        candles = []
                        for i, ts in enumerate(timestamps):
                            candles.append(
                                OHLCV(
                                    timestamp=datetime.fromtimestamp(ts),
                                    open=quotes["open"][i] or 0,
                                    high=quotes["high"][i] or 0,
                                    low=quotes["low"][i] or 0,
                                    close=quotes["close"][i] or 0,
                                    volume=quotes["volume"][i] or 0,
                                )
                            )
                        return candles
                else:
                    logger.warning(f"Yahoo Finance OHLCV error: {resp.status}")
                    return []

        except asyncio.TimeoutError:
            logger.warning("Yahoo Finance OHLCV timeout")
            return []
        except Exception as e:
            logger.warning(f"Yahoo Finance OHLCV error: {e}")
            return []


class MarketDataChain:
    """Fallback цепь для рыночных данных: Binance → Yahoo Finance"""

    def __init__(
        self,
        binance: Optional[BinanceProvider] = None,
        yahoo: Optional[YahooFinanceProvider] = None,
    ):
        self.binance = binance or BinanceProvider()
        self.yahoo = yahoo or YahooFinanceProvider()

    async def get_price(self, symbol: str) -> Optional[PriceData]:
        """Получить цену с fallback"""
        # Сначала Binance
        try:
            price = await self.binance.get_price(symbol)
            if price:
                logger.info(f"✅ Price from Binance: {symbol}")
                return price
        except Exception as e:
            logger.warning(f"⚠️  Binance price failed: {e}")

        # Потом Yahoo
        try:
            price = await self.yahoo.get_price(symbol)
            if price:
                logger.info(f"✅ Price from Yahoo: {symbol}")
                return price
        except Exception as e:
            logger.warning(f"⚠️  Yahoo price failed: {e}")

        logger.error(f"❌ No price data for {symbol}")
        return None

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
    ) -> List[OHLCV]:
        """Получить OHLCV с fallback"""
        # Сначала Binance
        try:
            candles = await self.binance.get_ohlcv(symbol, interval)
            if candles:
                logger.info(f"✅ OHLCV from Binance: {symbol}")
                return candles
        except Exception as e:
            logger.warning(f"⚠️  Binance OHLCV failed: {e}")

        # Потом Yahoo
        try:
            candles = await self.yahoo.get_ohlcv(symbol, "1mo", interval)
            if candles:
                logger.info(f"✅ OHLCV from Yahoo: {symbol}")
                return candles
        except Exception as e:
            logger.warning(f"⚠️  Yahoo OHLCV failed: {e}")

        logger.error(f"❌ No OHLCV data for {symbol}")
        return []

    async def close_all(self) -> None:
        """Закрытие всех сессий"""
        await self.binance.close()
        await self.yahoo.close()

    def __repr__(self) -> str:
        return "MarketDataChain(Binance → Yahoo Finance)"
