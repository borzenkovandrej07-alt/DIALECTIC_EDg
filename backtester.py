"""
backtester.py — Validate signals against real OHLC candle data.

Rules:
  1. Price MUST reach entry first — if not, NO TRADE
  2. Only candles AFTER signal timestamp are checked
  3. Only candles within timeframe_hours window
  4. LONG: win if high >= target, loss if low <= stop
  5. SHORT: win if low <= target, loss if high >= stop
  6. If nothing happens within timeframe → TIMEOUT

Fees: 0.1% entry + 0.1% exit = 0.2% total
"""

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

from trading_signal import Signal, timeframe_to_hours

logger = logging.getLogger(__name__)

FEE_PCT = 0.001  # 0.1% per side


@dataclass
class Candle:
    """Single OHLC candle."""
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0


@dataclass
class BacktestResult:
    """Result of backtesting a single signal. Flat structure for JSON."""
    asset: str
    direction: str
    entry: float
    target: float
    stop: float
    timeframe: str
    result: str            # "WIN", "LOSS", "TIMEOUT", "NO_ENTRY"
    pnl: float             # net PnL % after fees
    exit_price: float
    exit_reason: str       # "target", "stop", "timeout", "no_entry"
    entry_hit: bool        # did price actually reach entry?
    candles_checked: int
    fees_pct: float
    signal_timestamp: str
    exit_timestamp: str

    def to_dict(self) -> dict:
        return {
            "asset": self.asset,
            "direction": self.direction.lower(),
            "entry": self.entry,
            "target": self.target,
            "stop": self.stop,
            "result": self.result.lower(),
            "pnl": round(self.pnl, 4),
            "exit_price": self.exit_price,
            "exit_reason": self.exit_reason,
            "entry_hit": self.entry_hit,
            "candles_checked": self.candles_checked,
            "fees_pct": self.fees_pct,
            "signal_timestamp": self.signal_timestamp,
            "exit_timestamp": self.exit_timestamp,
        }


class Backtester:
    """Backtest signals against OHLC candle data."""

    def __init__(self, fee_pct: float = FEE_PCT):
        self.fee_pct = fee_pct
        self.results: list[BacktestResult] = []

    def test_signal(
        self,
        signal: Signal,
        candles: list[Candle],
    ) -> BacktestResult:
        """
        Test a single signal against OHLC candles.
        """
        direction = signal.direction
        entry = signal.entry
        target = signal.target
        stop = signal.stop
        total_fees = self.fee_pct * 2

        # Parse signal timestamp
        try:
            signal_ts = datetime.fromisoformat(signal.timestamp.replace("Z", "+00:00")).replace(tzinfo=None)
        except Exception:
            signal_ts = datetime.now()

        # Calculate end of timeframe window
        hours = timeframe_to_hours(signal.timeframe)
        end_ts = signal_ts + timedelta(hours=hours)

        # Filter candles: only AFTER signal timestamp AND within timeframe window
        filtered = [c for c in candles if signal_ts <= c.timestamp <= end_ts]

        if not filtered:
            return BacktestResult(
                asset=signal.asset, direction=direction, entry=entry,
                target=target, stop=stop, timeframe=signal.timeframe,
                result="NO_ENTRY", pnl=0.0, exit_price=0.0,
                exit_reason="no_candles_after_signal", entry_hit=False,
                candles_checked=0, fees_pct=0.0,
                signal_timestamp=signal.timestamp, exit_timestamp="",
            )

        # Step 1: Find the candle where price reaches entry
        entry_candle_idx = None
        for i, candle in enumerate(filtered):
            if direction == "LONG" and candle.low <= entry:
                entry_candle_idx = i
                break
            elif direction == "SHORT" and candle.high >= entry:
                entry_candle_idx = i
                break

        if entry_candle_idx is None:
            return BacktestResult(
                asset=signal.asset, direction=direction, entry=entry,
                target=target, stop=stop, timeframe=signal.timeframe,
                result="NO_ENTRY", pnl=0.0, exit_price=0.0,
                exit_reason="entry_not_reached", entry_hit=False,
                candles_checked=len(filtered), fees_pct=0.0,
                signal_timestamp=signal.timestamp, exit_timestamp="",
            )

        # Step 2: From entry candle onwards, check target/stop
        post_entry_candles = filtered[entry_candle_idx:]
        entry_candle = post_entry_candles[0]

        for i, candle in enumerate(post_entry_candles):
            if direction == "LONG":
                if candle.high >= target:
                    pnl = ((target - entry) / entry) - total_fees
                    return BacktestResult(
                        asset=signal.asset, direction=direction, entry=entry,
                        target=target, stop=stop, timeframe=signal.timeframe,
                        result="WIN", pnl=round(pnl * 100, 4), exit_price=target,
                        exit_reason="target", entry_hit=True, candles_checked=i + 1,
                        fees_pct=round(total_fees * 100, 4),
                        signal_timestamp=signal.timestamp, exit_timestamp=candle.timestamp.isoformat(),
                    )
                if candle.low <= stop:
                    pnl = ((stop - entry) / entry) - total_fees
                    return BacktestResult(
                        asset=signal.asset, direction=direction, entry=entry,
                        target=target, stop=stop, timeframe=signal.timeframe,
                        result="LOSS", pnl=round(pnl * 100, 4), exit_price=stop,
                        exit_reason="stop", entry_hit=True, candles_checked=i + 1,
                        fees_pct=round(total_fees * 100, 4),
                        signal_timestamp=signal.timestamp, exit_timestamp=candle.timestamp.isoformat(),
                    )
            elif direction == "SHORT":
                if candle.low <= target:
                    pnl = ((entry - target) / entry) - total_fees
                    return BacktestResult(
                        asset=signal.asset, direction=direction, entry=entry,
                        target=target, stop=stop, timeframe=signal.timeframe,
                        result="WIN", pnl=round(pnl * 100, 4), exit_price=target,
                        exit_reason="target", entry_hit=True, candles_checked=i + 1,
                        fees_pct=round(total_fees * 100, 4),
                        signal_timestamp=signal.timestamp, exit_timestamp=candle.timestamp.isoformat(),
                    )
                if candle.high >= stop:
                    pnl = ((entry - stop) / entry) - total_fees
                    return BacktestResult(
                        asset=signal.asset, direction=direction, entry=entry,
                        target=target, stop=stop, timeframe=signal.timeframe,
                        result="LOSS", pnl=round(pnl * 100, 4), exit_price=stop,
                        exit_reason="stop", entry_hit=True, candles_checked=i + 1,
                        fees_pct=round(total_fees * 100, 4),
                        signal_timestamp=signal.timestamp, exit_timestamp=candle.timestamp.isoformat(),
                    )

        # Timeout
        last = post_entry_candles[-1]
        pnl = ((last.close - entry) / entry) - total_fees if direction == "LONG" else ((entry - last.close) / entry) - total_fees

        return BacktestResult(
            asset=signal.asset, direction=direction, entry=entry,
            target=target, stop=stop, timeframe=signal.timeframe,
            result="TIMEOUT", pnl=round(pnl * 100, 4), exit_price=last.close,
            exit_reason="timeout", entry_hit=True, candles_checked=len(post_entry_candles),
            fees_pct=round(total_fees * 100, 4),
            signal_timestamp=signal.timestamp, exit_timestamp=last.timestamp.isoformat(),
        )

    def test_signals(
        self,
        signals: list[Signal],
        candles_map: dict[str, list[Candle]],
    ) -> list[BacktestResult]:
        """Test multiple signals against their respective candle data."""
        results = []
        for signal in signals:
            candles = candles_map.get(signal.asset, [])
            if not candles:
                logger.warning(f"No candle data for {signal.asset}, skipping")
                continue
            result = self.test_signal(signal, candles)
            results.append(result)
            self.results.append(result)
            logger.info(
                f"Backtest: {signal.asset} {signal.direction} @ {signal.entry} | "
                f"Result: {result.result} | PnL: {result.pnl:+.2f}% | "
                f"Exit: {result.exit_reason} | Entry hit: {result.entry_hit}"
            )
        return results

    def get_results(self) -> list[BacktestResult]:
        return list(self.results)

    def reset(self):
        self.results.clear()


async def get_candles(
    asset: str,
    timeframe_hours: int = 24,
    limit: int = 60,
) -> list[Candle]:
    """Fetch OHLC candles for an asset from Binance API."""
    candles = []

    try:
        symbol = f"{asset}USDT"
        interval_map = {
            1: "1m", 2: "1m", 4: "5m", 6: "15m", 8: "15m",
            12: "1h", 24: "1d", 48: "1d", 72: "1d",
            168: "1d", 336: "1d", 720: "1d",
        }
        interval = interval_map.get(timeframe_hours, "1d")

        import aiohttp
        url = "https://api.binance.com/api/v3/klines"
        params = {"symbol": symbol, "interval": interval, "limit": limit}

        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    for k in data:
                        candles.append(Candle(
                            timestamp=datetime.fromtimestamp(k[0] / 1000),
                            open=float(k[1]),
                            high=float(k[2]),
                            low=float(k[3]),
                            close=float(k[4]),
                            volume=float(k[5]),
                        ))
                    logger.info(f"Fetched {len(candles)} candles for {symbol} from Binance")
                    return candles
    except Exception as e:
        logger.warning(f"Binance klines failed for {asset}: {e}")

    # Fallback: CoinGecko
    try:
        import aiohttp
        cg_ids = {"BTC": "bitcoin", "ETH": "ethereum", "BNB": "binancecoin", "SOL": "solana"}
        cg_id = cg_ids.get(asset.upper())
        if not cg_id:
            return candles

        days = max(timeframe_hours * limit // 24, 1)
        url = f"https://api.coingecko.com/api/v3/coins/{cg_id}/market_chart"
        params = {"vs_currency": "usd", "days": days, "interval": "daily"}

        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    prices = data.get("prices", [])
                    for p in prices:
                        ts = datetime.fromtimestamp(p[0] / 1000)
                        price = float(p[1])
                        candles.append(Candle(
                            timestamp=ts, open=price, high=price * 1.01,
                            low=price * 0.99, close=price,
                        ))
                    logger.info(f"Fetched {len(candles)} candles for {asset} from CoinGecko (approx)")
    except Exception as e:
        logger.warning(f"CoinGecko fallback failed for {asset}: {e}")

    return candles
