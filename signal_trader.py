"""
Signal Trader — проверяет сигналы каждую минуту и торгует по простой стратегии.
"""
import asyncio
import logging
import os
import re
from datetime import datetime
from typing import Optional

import aiosqlite
from config import DB_PATH, ADMIN_IDS
from database import (
    get_backtest_config,
    get_daily_context,
    add_backtest_signal,
    get_backtest_signals,
    get_backtest_stats,
)

logger = logging.getLogger(__name__)

INTERVAL_SECONDS = 60  # Проверка каждую минуту (для тестов)

TRADE_SYMBOLS = ["BTC", "ETH", "SOL"]  # Символы для торговли
TRADE_THRESHOLD = 0.02  # 2% движение для входа
TAKE_PROFIT = 0.03  # 3% тейк-профит
STOP_LOSS = 0.02  # 2% стоп-лосс

last_prices = {}  # Цены на последней проверке


async def fetch_current_prices(symbols: list[str]) -> dict:
    """Получить текущие цены через web_search."""
    from web_search import get_full_realtime_context
    
    try:
        context = await get_full_realtime_context()
        prices = {}
        
        # Parse prices from context
        for symbol in symbols:
            # Look for symbol in context
            pattern = rf"{symbol}[^a-zA-Z]*\$?([\d,.]+)"
            match = re.search(pattern, context, re.IGNORECASE)
            if match:
                price_str = match.group(1).replace(",", "")
                try:
                    prices[symbol] = float(price_str)
                except:
                    pass
        
        return prices
    except Exception as e:
        logger.warning(f"Failed to fetch prices: {e}")
        return {}


async def check_and_trade(bot, admin_ids: list) -> list[dict]:
    """
    Проверить цены и исполнить сделки.
    Стратегия: если цена упала на 2%+ → BUY, выросла на 2%+ → SELL.
    Закрываем: +3% (тейк-профит) или -2% (стоп-лосс).
    """
    global last_prices
    executed = []
    
    config = await get_backtest_config()
    if not config.get("enabled", 1):
        return executed
    
    prices = await fetch_current_prices(TRADE_SYMBOLS)
    if not prices:
        return executed
    
    signals = await get_backtest_signals()
    open_positions = {s["symbol"]: s for s in signals if s.get("status") == "open"}
    
    for symbol in TRADE_SYMBOLS:
        current_price = prices.get(symbol)
        if not current_price:
            continue
        
        last_price = last_prices.get(symbol)
        
        # Check for new trade signal (only if no open position)
        if symbol not in open_positions and last_price:
            change = (current_price - last_price) / last_price
            
            # Price dropped → BUY
            if change <= -TRADE_THRESHOLD:
                direction = "BUY"
                result = await add_backtest_signal(
                    symbol=symbol,
                    direction=direction,
                    entry_price=current_price,
                    source="auto_trader"
                )
                
                if result.get("status") == "opened":
                    executed.append({
                        "symbol": symbol,
                        "direction": direction,
                        "entry_price": current_price,
                        "change": change,
                        "capital": result.get("capital_after", 0)
                    })
                    
                    emoji = "🟢"
                    msg = f"🎯 *АВТОТРЕЙДЕР - ПОКУПКА*\n"
                    msg += "═" * 25 + "\n"
                    msg += f"{emoji} *{symbol}* 📈 ЛОНГ\n"
                    msg += f"Вход: ${current_price:,.2f}\n"
                    msg += f"Изменение: {change*100:+.2f}%\n"
                    msg += f"💵 Баланс: ${result.get('capital_after', 0):,.2f}\n"
                    msg += "═" * 25
                    
                    for admin_id in admin_ids:
                        try:
                            await bot.send_message(admin_id, msg, parse_mode="Markdown")
                        except:
                            pass
                    
                    logger.info(f"Auto trade: {symbol} BUY at {current_price}")
            
            # Price jumped → SELL
            elif change >= TRADE_THRESHOLD:
                direction = "SELL"
                result = await add_backtest_signal(
                    symbol=symbol,
                    direction=direction,
                    entry_price=current_price,
                    source="auto_trader"
                )
                
                if result.get("status") == "opened":
                    executed.append({
                        "symbol": symbol,
                        "direction": direction,
                        "entry_price": current_price,
                        "change": change,
                        "capital": result.get("capital_after", 0)
                    })
                    
                    emoji = "🔴"
                    msg = f"🎯 *АВТОТРЕЙДЕР - ПРОДАЖА*\n"
                    msg += "═" * 25 + "\n"
                    msg += f"{emoji} *{symbol}* 📉 ШОРТ\n"
                    msg += f"Вход: ${current_price:,.2f}\n"
                    msg += f"Изменение: {change*100:+.2f}%\n"
                    msg += f"💵 Баланс: ${result.get('capital_after', 0):,.2f}\n"
                    msg += "═" * 25
                    
                    for admin_id in admin_ids:
                        try:
                            await bot.send_message(admin_id, msg, parse_mode="Markdown")
                        except:
                            pass
                    
                    logger.info(f"Auto trade: {symbol} SELL at {current_price}")
        
        # Check open positions for take profit / stop loss
        elif symbol in open_positions:
            pos = open_positions[symbol]
            entry = pos.get("entry_price", 0)
            direction = pos.get("direction", "")
            
            if not entry:
                continue
            
            change_pct = (current_price - entry) / entry if direction == "BUY" else (entry - current_price) / entry
            
            # Check take profit
            if change_pct >= TAKE_PROFIT:
                from database import close_backtest_signal
                result = await close_backtest_signal(pos["id"], current_price)
                if result:
                    emoji = "🟢"
                    msg = f"🎯 *АВТОТРЕЙДЕР - ТЕЙК-ПРОФИТ*\n"
                    msg += "═" * 25 + "\n"
                    msg += f"{emoji} {symbol} {direction} ЗАКРЫТ\n"
                    msg += f"Вход: ${entry:,.2f}\n"
                    msg += f"Выход: ${current_price:,.2f}\n"
                    msg += f"Профит: {change_pct*100:+.2f}%\n"
                    msg += f"💵 Баланс: ${result.get('new_capital', 0):,.2f}\n"
                    msg += "═" * 25
                    
                    for admin_id in admin_ids:
                        try:
                            await bot.send_message(admin_id, msg, parse_mode="Markdown")
                        except:
                            pass
                    
                    logger.info(f"Auto close: {symbol} TP at {current_price}")
            
            # Check stop loss
            elif change_pct <= -STOP_LOSS:
                from database import close_backtest_signal
                result = await close_backtest_signal(pos["id"], current_price)
                if result:
                    emoji = "🔴"
                    msg = f"🎯 *АВТОТРЕЙДЕР - СТОП-ЛОСС*\n"
                    msg += "═" * 25 + "\n"
                    msg += f"{emoji} {symbol} {direction} ЗАКРЫТ\n"
                    msg += f"Вход: ${entry:,.2f}\n"
                    msg += f"Выход: ${current_price:,.2f}\n"
                    msg += f"Потери: {change_pct*100:.2f}%\n"
                    msg += f"💵 Баланс: ${result.get('new_capital', 0):,.2f}\n"
                    msg += "═" * 25
                    
                    for admin_id in admin_ids:
                        try:
                            await bot.send_message(admin_id, msg, parse_mode="Markdown")
                        except:
                            pass
                    
                    logger.info(f"Auto close: {symbol} SL at {current_price}")
    
    # Update last prices for next iteration
    last_prices = prices
    
    return executed


async def run_signal_trader(bot, admin_ids: list):
    """Запустить автотрейдера в бесконечном цикле."""
    logger.info(f"🚀 Auto Trader started (check every {INTERVAL_SECONDS} sec)")
    
    while True:
        try:
            await check_and_trade(bot, admin_ids)
        except Exception as e:
            logger.error(f"Auto trader error: {e}")
        
        await asyncio.sleep(INTERVAL_SECONDS)


async def get_signal_trader_status() -> dict:
    """Получить статус автотрейдера."""
    config = await get_backtest_config()
    stats = await get_backtest_stats()
    signals = await get_backtest_signals()
    open_positions = [s for s in signals if s.get("status") == "open"]
    
    return {
        "enabled": config.get("enabled", 1),
        "capital": config.get("capital", 100.0),
        "total_trades": stats.get("total", 0),
        "total_pnl": stats.get("total_pnl", 0),
        "open_positions": len(open_positions),
        "symbols": TRADE_SYMBOLS,
    }