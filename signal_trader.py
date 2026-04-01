"""
Auto Trader — комбинированная стратегия:
1. Приоритет: /daily контекст (вердикт + точки входа)
2. Резерв: автотрейдер на 2% движение (если нет daily)
3. Дополнительно: Binance сигналы как подтверждение
"""
import asyncio
import logging
import os
import re
from datetime import datetime, timedelta
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

INTERVAL_SECONDS = 60  # Проверка каждую минуту

TRADE_SYMBOLS = ["BTC", "ETH", "SOL"]
TRADE_THRESHOLD = 0.02  # 2% движение для автотрейдера
TAKE_PROFIT = 0.03  # 3% тейк-профит
STOP_LOSS = 0.02  # 2% стоп-лосс
DAILY_CONTEXT_TTL_HOURS = 24  # Контекст /daily актуален 24 часа

last_prices = {}  # Цены на последней проверке
last_binace_check = None
binance_signals_cache = {}


async def get_binance_signals() -> dict:
    """Получить сигналы Binance для дополнительного подтверждения."""
    global last_binace_check, binance_signals_cache
    
    # Cache for 5 minutes
    if last_binace_check and (datetime.now() - last_binace_check).seconds < 300:
        return binance_signals_cache
    
    try:
        from signals import fetch_binance_signals
        data = await fetch_binance_signals()
        last_binace_check = datetime.now()
        binance_signals_cache = data or {}
        return binance_signals_cache
    except Exception as e:
        logger.warning(f"Binance signals fetch error: {e}")
        return {}


def is_daily_context_fresh(context: dict) -> bool:
    """Проверить, свежий ли контекст /daily."""
    if not context:
        return False
    created = context.get("created_at", "")
    if not created:
        return False
    try:
        dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
        return (datetime.now() - dt) < timedelta(hours=DAILY_CONTEXT_TTL_HOURS)
    except:
        return False


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
    Приоритет: 1) /daily контекст 2) автотрейдер 3) Binance сигналы (подтверждение)
    """
    global last_prices
    executed = []
    
    config = await get_backtest_config()
    if not config.get("enabled", 1):
        return executed
    
    prices = await fetch_current_prices(TRADE_SYMBOLS)
    if not prices:
        return executed
    
    # Получаем контекст /daily и Binance сигналы
    daily_context = await get_daily_context()
    binance_signals = await get_binance_signals()
    is_daily_fresh = is_daily_context_fresh(daily_context)
    
    signals = await get_backtest_signals()
    open_positions = {s["symbol"]: s for s in signals if s.get("status") == "open"}
    
    verdict = daily_context.get("verdict", "").upper() if daily_context else ""
    entries = daily_context.get("entries", {}) if daily_context else {}
    
    for symbol in TRADE_SYMBOLS:
        current_price = prices.get(symbol)
        if not current_price:
            continue
        
        last_price = last_prices.get(symbol)
        
        # === ПРИОРИТЕТ 1: /daily контекст ===
        if is_daily_fresh and verdict and symbol in entries:
            entry_price = entries.get(symbol)
            if not entry_price:
                continue
            
            # Уже есть позиция по этому символу?
            if symbol in open_positions:
                continue
            
            # Проверяем сигнал от /daily (с допуском 5% от точки входа)
            trade_triggered = False
            trigger_reason = ""
            
            if verdict == "BUY" and current_price <= entry_price * 1.05:
                trade_triggered = True
                trigger_reason = f"Вердикт BUY, цена {current_price} близко к входу {entry_price}"
            
            elif verdict == "SELL" and current_price >= entry_price * 0.95:
                trade_triggered = True
                trigger_reason = f"Вердикт SELL, цена {current_price} близко к входу {entry_price}"
            
            if trade_triggered:
                direction = "BUY" if verdict == "BUY" else "SELL"
                result = await add_backtest_signal(
                    symbol=symbol,
                    direction=direction,
                    entry_price=current_price,
                    source="daily_context"
                )
                
                if result.get("status") == "opened":
                    executed.append({
                        "symbol": symbol,
                        "direction": direction,
                        "entry_price": current_price,
                        "source": "daily_context",
                        "reason": trigger_reason,
                        "capital": result.get("capital_after", 0)
                    })
                    
                    # Проверяем Binance сигналы для подтверждения
                    binance_confirm = binance_signals.get(symbol, {}).get("direction", "")
                    binance_note = f" | Binance: {binance_confirm}" if binance_confirm else ""
                    
                    emoji = "🟢"
                    msg = f"🎯 *ПРИОРИТЕТ: /daily*\n"
                    msg += "═" * 25 + "\n"
                    msg += f"{emoji} *{symbol}* {'📈 ЛОНГ' if direction == 'BUY' else '📉 ШОРТ'}\n"
                    msg += f"Вход: ${current_price:,.2f}\n"
                    msg += f"Вердикт: {verdict}\n"
                    msg += f"Причина: {trigger_reason}{binance_note}\n"
                    msg += f"💵 Баланс: ${result.get('capital_after', 0):,.2f}\n"
                    msg += "═" * 25
                    
                    for admin_id in admin_ids:
                        try:
                            await bot.send_message(admin_id, msg, parse_mode="Markdown")
                        except:
                            pass
                    
                    logger.info(f"Trade from daily: {symbol} {direction} at {current_price}")
                    continue
        
        # === ПРИОРИТЕТ 2: Если нет daily контекста — автотрейдер по daily точке входа ===
        if not is_daily_fresh and symbol not in open_positions:
            # Используем цену из daily как базу
            if symbol in entries:
                base_price = entries.get(symbol)
                if base_price:
                    change_pct = (current_price - base_price) / base_price
                    
                    # BUY если цена упала на 3%+ от точки входа
                    if change_pct <= -0.03:
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
                                "source": "auto_trader",
                                "capital": result.get("capital_after", 0)
                            })
                            
                            emoji = "🟢"
                            msg = f"🎯 *АВТОТРЕЙДЕР*\n"
                            msg += "═" * 25 + "\n"
                            msg += f"{emoji} *{symbol}* 📈 ЛОНГ\n"
                            msg += f"Вход: ${current_price:,.2f}\n"
                            msg += f"От точки входа: {change_pct*100:+.1f}%\n"
                            msg += f"💵 Баланс: ${result.get('capital_after', 0):,.2f}\n"
                            msg += "═" * 25
                            
                            for admin_id in admin_ids:
                                try:
                                    await bot.send_message(admin_id, msg, parse_mode="Markdown")
                                except:
                                    pass
                            
                            logger.info(f"Auto trade fallback: {symbol} BUY at {current_price}")
                    
                    # SELL если цена выросла на 3%+ от точки входа
                    elif change_pct >= 0.03:
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
                                "source": "auto_trader",
                                "capital": result.get("capital_after", 0)
                            })
                            
                            emoji = "🔴"
                            msg = f"🎯 *АВТОТРЕЙДЕР*\n"
                            msg += "═" * 25 + "\n"
                            msg += f"{emoji} *{symbol}* 📉 ШОРТ\n"
                            msg += f"Вход: ${current_price:,.2f}\n"
                            msg += f"От точки входа: {change_pct*100:+.1f}%\n"
                            msg += f"💵 Баланс: ${result.get('capital_after', 0):,.2f}\n"
                            msg += "═" * 25
                            
                            for admin_id in admin_ids:
                                try:
                                    await bot.send_message(admin_id, msg, parse_mode="Markdown")
                                except:
                                    pass
                            
                            logger.info(f"Auto trade fallback: {symbol} SELL at {current_price}")
        
        # === Проверка открытых позиций: тейк-профит / стоп-лосс ===
        if symbol in open_positions:
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
    daily_ctx = await get_daily_context()
    daily_fresh = is_daily_context_fresh(daily_ctx)
    
    return {
        "enabled": config.get("enabled", 1),
        "capital": config.get("capital", 100.0),
        "total_trades": stats.get("total", 0),
        "total_pnl": stats.get("total_pnl", 0),
        "open_positions": len(open_positions),
        "symbols": TRADE_SYMBOLS,
        "daily_context_fresh": daily_fresh,
        "daily_verdict": daily_ctx.get("verdict") if daily_ctx else None,
        "daily_symbols": daily_ctx.get("symbols", []) if daily_ctx else [],
    }