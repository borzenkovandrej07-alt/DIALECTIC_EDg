"""
market_data.py — Получение рыночных данных через бесплатные API.
Использует: Yahoo Finance (yfinance), CoinGecko (без ключа).
"""

import asyncio
import logging
from datetime import datetime
from typing import Optional

import aiohttp

logger = logging.getLogger(__name__)

# Ключевые активы для мониторинга
WATCHLIST_STOCKS = ["SPY", "QQQ", "DXY", "GLD", "USO", "^VIX", "^TNX"]
WATCHLIST_CRYPTO = ["bitcoin", "ethereum", "solana", "binancecoin"]
WATCHLIST_TICKERS_YF = ["BTC-USD", "ETH-USD", "AAPL", "TSLA", "NVDA", "SPY", "QQQ"]


class MarketDataFetcher:
    """Асинхронный получатель рыночных данных из бесплатных источников."""

    async def fetch_snapshot(self) -> str:
        """Возвращает текстовый снапшот рынков для агентов."""
        tasks = [
            self._fetch_coingecko(),
            self._fetch_yahoo_finance(),
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        parts = []
        for r in results:
            if isinstance(r, str) and r:
                parts.append(r)
            elif isinstance(r, Exception):
                logger.warning(f"Market data error: {r}")
        
        if not parts:
            return "Рыночные данные недоступны (проверьте интернет-соединение)."
        
        return "\n\n".join(parts)

    # ── CoinGecko (бесплатно, без ключа) ──────────────────────────────────────

    async def _fetch_coingecko(self) -> str:
        try:
            url = "https://api.coingecko.com/api/v3/simple/price"
            params = {
                "ids": ",".join(WATCHLIST_CRYPTO),
                "vs_currencies": "usd",
                "include_24hr_change": "true",
                "include_market_cap": "true",
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        return ""
                    data = await resp.json()

            lines = ["📊 *КРИПТОРЫНОК (CoinGecko):*"]
            
            emoji_map = {
                "bitcoin": "₿",
                "ethereum": "Ξ",
                "solana": "◎",
                "binancecoin": "🔶",
            }
            
            for coin_id, prices in data.items():
                usd = prices.get("usd", 0)
                change = prices.get("usd_24h_change", 0)
                mcap = prices.get("usd_market_cap", 0)
                
                emoji = emoji_map.get(coin_id, "•")
                change_str = f"+{change:.1f}%" if change >= 0 else f"{change:.1f}%"
                change_emoji = "🟢" if change >= 0 else "🔴"
                mcap_str = f"${mcap/1e9:.1f}B" if mcap > 1e9 else f"${mcap/1e6:.0f}M"
                
                lines.append(
                    f"{emoji} {coin_id.capitalize()}: *${usd:,.0f}* "
                    f"{change_emoji} {change_str} | MCap: {mcap_str}"
                )
            
            return "\n".join(lines)

        except Exception as e:
            logger.warning(f"CoinGecko error: {e}")
            return ""

    # ── Yahoo Finance через неофициальный endpoint ────────────────────────────

    async def _fetch_yahoo_finance(self) -> str:
        """
        Использует Yahoo Finance chart API (без ключа).
        Получает последние цены для основных инструментов.
        """
        try:
            results = {}
            
            async with aiohttp.ClientSession() as session:
                for ticker in WATCHLIST_TICKERS_YF[:5]:  # берём 5 главных
                    try:
                        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
                        params = {"interval": "1d", "range": "2d"}
                        headers = {"User-Agent": "Mozilla/5.0"}
                        
                        async with session.get(url, params=params, headers=headers,
                                               timeout=aiohttp.ClientTimeout(total=8)) as resp:
                            if resp.status == 200:
                                data = await resp.json()
                                result = data.get("chart", {}).get("result", [{}])[0]
                                meta = result.get("meta", {})
                                
                                price = meta.get("regularMarketPrice", 0)
                                prev_close = meta.get("previousClose", 0) or meta.get("chartPreviousClose", 0)
                                
                                if price and prev_close:
                                    change_pct = ((price - prev_close) / prev_close) * 100
                                    results[ticker] = (price, change_pct)
                    except Exception:
                        continue
                    
                    await asyncio.sleep(0.2)  # небольшая пауза между запросами

            if not results:
                return ""
            
            lines = ["📈 *РЫНОК АКЦИЙ/ETF (Yahoo Finance):*"]
            
            icons = {
                "SPY": "🇺🇸", "QQQ": "💻", "BTC-USD": "₿",
                "ETH-USD": "Ξ", "AAPL": "🍎", "TSLA": "🚗",
                "NVDA": "🎮", "DXY": "💵",
            }
            
            for ticker, (price, change) in results.items():
                icon = icons.get(ticker, "•")
                change_str = f"+{change:.2f}%" if change >= 0 else f"{change:.2f}%"
                change_emoji = "🟢" if change >= 0 else "🔴"
                lines.append(f"{icon} {ticker}: *${price:,.2f}* {change_emoji} {change_str}")
            
            return "\n".join(lines)

        except Exception as e:
            logger.warning(f"Yahoo Finance error: {e}")
            return ""


async def fetch_ohlc_candles(
    asset: str,
    timeframe_hours: int = 24,
    limit: int = 120,
):
    """
    OHLC для бэктеста: единая точка входа → `backtester.get_candles` (Binance / fallback).
    """
    from backtester import get_candles

    return await get_candles(asset, timeframe_hours=timeframe_hours, limit=limit)
