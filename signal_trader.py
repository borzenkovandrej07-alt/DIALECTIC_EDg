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

INTERVAL_SECONDS = 300  # Проверка каждые 5 минут

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


def get_daily_entries(context: dict) -> dict:
    """Получить точки входа из daily контекста (любого возраста)."""
    if not context:
        return {}
    return context.get("entries", {}) or {}


async def fetch_current_prices(symbols: list[str]) -> dict:
    """Получить текущие цены через web_search."""
    from web_search import get_full_realtime_context
    
    try:
        result = await get_full_realtime_context()
        # Result is a tuple (prices_dict, formatted_string)
        if isinstance(result, tuple):
            _, context = result
        else:
            context = result
        
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
    Логика:
    - Если вердикт BUY + сигналы LONG → ПОКУПАЕМ СЕЙЧАС
    - Если вердикт SELL + сигналы SHORT → ПРОДАЕМ СЕЙЧАС
    - Закрываем: когда цена упала на 3%+ или противоположный сигнал
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
    entries = get_daily_entries(daily_context) if daily_context else {}
    verdict = daily_context.get("verdict", "").upper() if daily_context else ""
    
    signals = await get_backtest_signals()
    open_positions = {s["symbol"]: s for s in signals if s.get("status") == "open"}
    
    for symbol in TRADE_SYMBOLS:
        current_price = prices.get(symbol)
        if not current_price:
            continue
        
        # Проверяем Binance сигналы для этого символа
        binance_dir = binance_signals.get(symbol, {}).get("direction", "")
        
        # === ГЛАВНАЯ ЛОГИКА: Покупаем/продаём СЕЙЧАС если есть совпадение ===
        if symbol not in open_positions and verdict and entries:
            entry_price = entries.get(symbol)
            if not entry_price:
                continue
            
            # BUY если: вердикт BUY + (сигнал LONG или цена ниже точки входа)
            if verdict == "BUY" and (binance_dir == "LONG" or current_price <= entry_price):
                direction = "BUY"
                result = await add_backtest_signal(
                    symbol=symbol,
                    direction=direction,
                    entry_price=current_price,
                    source="daily_signal"
                )
                
                if result.get("status") == "opened":
                    executed.append({
                        "symbol": symbol,
                        "direction": direction,
                        "entry_price": current_price,
                        "source": "daily_signal",
                        "capital": result.get("capital_after", 0)
                    })
                    
                    binance_note = f" | Binance: {binance_dir}" if binance_dir else ""
                    emoji = "🟢"
                    msg = f"🎯 *СИГНАЛ BUY!*\n"
                    msg += "═" * 25 + "\n"
                    msg += f"{emoji} *{symbol}* 📈 ЛОНГ\n"
                    msg += f"Купили по: ${current_price:,.2f}\n"
                    msg += f"Точка входа: ${entry_price:,.0f}\n"
                    msg += f"Источник: {verdict}{binance_note}\n"
                    msg += f"💵 Баланс: ${result.get('capital_after', 0):,.2f}\n"
                    msg += "═" * 25
                    
                    for admin_id in admin_ids:
                        try:
                            await bot.send_message(admin_id, msg, parse_mode="Markdown")
                        except:
                            pass
                    
                    logger.info(f"Trade: {symbol} BUY at {current_price}")
                    continue
            
            # SELL если: вердикт SELL + (сигнал SHORT или цена выше точки входа)
            elif verdict == "SELL" and (binance_dir == "SHORT" or current_price >= entry_price):
                direction = "SELL"
                result = await add_backtest_signal(
                    symbol=symbol,
                    direction=direction,
                    entry_price=current_price,
                    source="daily_signal"
                )
                
                if result.get("status") == "opened":
                    executed.append({
                        "symbol": symbol,
                        "direction": direction,
                        "entry_price": current_price,
                        "source": "daily_signal",
                        "capital": result.get("capital_after", 0)
                    })
                    
                    binance_note = f" | Binance: {binance_dir}" if binance_dir else ""
                    emoji = "🔴"
                    msg = f"🎯 *СИГНАЛ SELL!*\n"
                    msg += "═" * 25 + "\n"
                    msg += f"{emoji} *{symbol}* 📉 ШОРТ\n"
                    msg += f"Продали по: ${current_price:,.2f}\n"
                    msg += f"Точка входа: ${entry_price:,.0f}\n"
                    msg += f"Источник: {verdict}{binance_note}\n"
                    msg += f"💵 Баланс: ${result.get('capital_after', 0):,.2f}\n"
                    msg += "═" * 25
                    
                    for admin_id in admin_ids:
                        try:
                            await bot.send_message(admin_id, msg, parse_mode="Markdown")
                        except:
                            pass
                    
                    logger.info(f"Trade: {symbol} SELL at {current_price}")
        
        # === Проверка открытых позиций: тейк-профит / стоп-лосс ===
        if symbol in open_positions:
            pos = open_positions[symbol]
            entry = pos.get("entry_price", 0)
            direction = pos.get("direction", "")
            
            if not entry:
                continue
            
            change_pct = (current_price - entry) / entry if direction == "BUY" else (entry - current_price) / entry
            
            # Check take profit (+3%)
            if change_pct >= TAKE_PROFIT:
                from database import close_backtest_signal
                result = await close_backtest_signal(pos["id"], current_price)
                if result:
                    emoji = "🟢"
                    msg = f"🎯 *ТЕЙК-ПРОФИТ!*\n"
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
                    
                    logger.info(f"Closed: {symbol} TP at {current_price}")
            
            # Check stop loss (-3%)
            elif change_pct <= -STOP_LOSS:
                from database import close_backtest_signal
                result = await close_backtest_signal(pos["id"], current_price)
                if result:
                    emoji = "🔴"
                    msg = f"🎯 *СТОП-ЛОСС!*\n"
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
                    
                    logger.info(f"Closed: {symbol} SL at {current_price}")
    
    # Update last prices for next iteration
    last_prices = prices
    
    return executed
    
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