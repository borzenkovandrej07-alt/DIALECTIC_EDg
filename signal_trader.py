"""
Signal Trader — проверяет сигналы каждые N минут и исполняет сделки по вердикту из /daily.
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

INTERVAL_MINUTES = 5  # Проверка каждые 5 минут


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
    Проверить сигналы и исполнить сделки на основе daily_context.
    Возвращает список выполненных сделок.
    """
    executed = []
    
    # Check if backtest is enabled
    config = await get_backtest_config()
    if not config.get("enabled", 1):
        logger.info("Backtest disabled, skipping trade check")
        return executed
    
    # Get daily context
    context = await get_daily_context()
    if not context:
        logger.info("No daily context found, skipping trade check")
        return executed
    
    verdict = context.get("verdict", "").upper()
    symbols = context.get("symbols", [])
    entries = context.get("entries", {})
    stop_losses = context.get("stop_losses", {})
    
    if not symbols or not verdict:
        logger.info("No symbols or verdict in context")
        return executed
    
    # Get current prices
    prices = await fetch_current_prices(symbols)
    
    # Check for each symbol
    for symbol in symbols:
        try:
            current_price = prices.get(symbol)
            entry_price = entries.get(symbol)
            stop_loss = stop_losses.get(symbol)
            
            if not current_price or not entry_price:
                continue
            
            # Check if we already have an open position for this symbol
            signals = await get_backtest_signals()
            open_positions = [s for s in signals if s.get("status") == "open" and s.get("symbol") == symbol]
            
            if open_positions:
                logger.debug(f"Position already open for {symbol}")
                continue
            
            # Determine if we should trade based on verdict
            # If verdict is BUY, we look for price near entry or lower (buy the dip)
            # If verdict is SELL, we look for price near entry or higher (sell the rip)
            
            trade_triggered = False
            trigger_reason = ""
            
            if verdict == "BUY":
                # Buy if price is at or below entry (within 2% tolerance)
                if current_price <= entry_price * 1.02:
                    trade_triggered = True
                    trigger_reason = f"Price {current_price} <= entry {entry_price}"
            
            elif verdict == "SELL":
                # Sell if price is at or above entry (within 2% tolerance)
                if current_price >= entry_price * 0.98:
                    trade_triggered = True
                    trigger_reason = f"Price {current_price} >= entry {entry_price}"
            
            if not trade_triggered:
                continue
            
            # Execute the trade
            direction = "BUY" if verdict == "BUY" else "SELL"
            result = await add_backtest_signal(
                symbol=symbol,
                direction=direction,
                entry_price=current_price,
                source="signal_trader"
            )
            
            if result.get("status") == "opened":
                trade = {
                    "symbol": symbol,
                    "direction": direction,
                    "entry_price": current_price,
                    "reason": trigger_reason,
                    "capital": result.get("capital_after", 0)
                }
                executed.append(trade)
                
                # Notify admins with nice format
                emoji = "🟢" if direction == "BUY" else "🔴"
                direction_text = "ПОКУПКА" if direction == "BUY" else "ПРОДАЖА"
                msg = f"🎯 *ТЕСТОВЫЙ ТРЕЙДЕР - СИГНАЛ*\n"
                msg += "═" * 25 + "\n"
                msg += f"{emoji} *{symbol}* {direction_text}\n"
                msg += f"Вход: ${current_price:,.2f}\n"
                msg += f"Вердикт: {verdict}\n"
                msg += f"Причина: {trigger_reason}\n"
                msg += f"💵 Баланс: ${result.get('capital_after', 0):,.2f}\n"
                msg += "═" * 25
                
                for admin_id in admin_ids:
                    try:
                        await bot.send_message(admin_id, msg, parse_mode="Markdown")
                    except Exception as e:
                        logger.warning(f"Failed to notify admin {admin_id}: {e}")
                
                logger.info(f"Executed trade: {symbol} {direction} @ {current_price}")
        
        except Exception as e:
            logger.warning(f"Error processing {symbol}: {e}")
    
    return executed


async def run_signal_trader(bot, admin_ids: list):
    """Запустить сигнал-трейдер в бесконечном цикле."""
    logger.info(f"🚀 Signal Trader started (check every {INTERVAL_MINUTES} min)")
    
    while True:
        try:
            await check_and_trade(bot, admin_ids)
        except Exception as e:
            logger.error(f"Signal trader error: {e}")
        
        await asyncio.sleep(INTERVAL_MINUTES * 60)


async def get_signal_trader_status() -> dict:
    """Получить статус сигнал-трейдера."""
    context = await get_daily_context()
    config = await get_backtest_config()
    stats = await get_backtest_stats()
    
    return {
        "enabled": config.get("enabled", 1),
        "capital": config.get("capital", 100.0),
        "daily_verdict": context.get("verdict") if context else None,
        "symbols": context.get("symbols", []) if context else [],
        "total_trades": stats.get("total", 0),
        "total_pnl": stats.get("total_pnl", 0),
    }