"""
Portfolio handler for portfolio views, callbacks, and text-driven inputs.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from database import (
    add_portfolio_position,
    get_portfolio,
    remove_portfolio_position,
)
from web_search import get_full_realtime_context

logger = logging.getLogger(__name__)

# Transient multi-step input state:
# {user_id: {"symbol": str, "step": "amount" | "price", "amount": float}}
user_portfolio_state: dict[int, dict[str, object]] = {}

ASSET_OPTIONS = (
    ("BTC", "₿ Bitcoin", "BTC"),
    ("ETH", "Ξ Ethereum", "ETH"),
    ("SOL", "◎ Solana", "SOL"),
    ("GOLD", "🥇 Gold", "GOLD"),
)
ALLOWED_SYMBOLS = tuple(symbol for symbol, _, _ in ASSET_OPTIONS)
PRICE_SYMBOLS = {symbol: price_symbol for symbol, _, price_symbol in ASSET_OPTIONS}


@dataclass(slots=True)
class PortfolioTotals:
    cost: float = 0.0
    value: float = 0.0
    pnl: float = 0.0


def portfolio_keyboard(has_positions: bool = True) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text="➕ Добавить", callback_data="portfolio:add_select:")],
    ]
    if has_positions:
        buttons.append(
            [InlineKeyboardButton(text="🗑 Удалить", callback_data="portfolio:remove_select:")]
        )
    buttons.append(
        [InlineKeyboardButton(text="🔄 Обновить", callback_data="portfolio:refresh:")]
    )
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def select_crypto_keyboard() -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text=label, callback_data=f"portfolio:add_amount:{symbol}")]
        for symbol, label, _ in ASSET_OPTIONS
    ]
    buttons.append([InlineKeyboardButton(text="🔙 Назад", callback_data="portfolio:menu:")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def _build_remove_keyboard(positions: list[dict]) -> InlineKeyboardMarkup:
    buttons = [
        [
            InlineKeyboardButton(
                text=f"🗑 {position['symbol']} ({position['amount']})",
                callback_data=f"portfolio:confirm_remove:{position['symbol']}",
            )
        ]
        for position in positions
    ]
    buttons.append([InlineKeyboardButton(text="🔙 Назад", callback_data="portfolio:menu:")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def _parse_positive_number(text: str) -> Optional[float]:
    try:
        value = float(text.replace(",", "."))
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None


def _parse_portfolio_callback(data: Optional[str]) -> tuple[str, str]:
    parts = (data or "").split(":")
    action = parts[1] if len(parts) > 1 else ""
    symbol = parts[2] if len(parts) > 2 else ""
    return action, symbol


def _build_portfolio_text(positions: list[dict], prices: dict) -> str:
    lines = ["📊 ТВОЙ ПОРТФЕЛЬ", ""]
    totals = PortfolioTotals()

    for position in positions:
        symbol = position["symbol"]
        amount = position["amount"]
        entry_price = position["entry_price"]
        current_price = prices.get(PRICE_SYMBOLS.get(symbol, symbol), {}).get("price", 0)

        cost = amount * entry_price
        totals.cost += cost

        if current_price:
            value = amount * current_price
            pnl = value - cost
            pnl_pct = (pnl / cost * 100) if cost else 0
            totals.value += value
            totals.pnl += pnl
            emoji = "🟢" if pnl >= 0 else "🔴"
            lines.append(f"{symbol}: {amount} x ${current_price:,.0f} = ${value:,.0f}")
            lines.append(
                f"  Вход: ${entry_price:,.0f} | PnL: {emoji}${pnl:+,.0f} ({pnl_pct:+.1f}%)"
            )
            continue

        totals.value += cost
        lines.append(f"{symbol}: {amount} x $??? | Вход: ${entry_price:,.0f}")

    if not positions:
        lines.append("Портфель пуст")

    if totals.cost > 0:
        total_pnl_pct = totals.pnl / totals.cost * 100
        emoji = "🟢" if totals.pnl >= 0 else "🔴"
        lines.extend(
            [
                "",
                f"📈 Итого: ${totals.value:,.0f} | {emoji} ${totals.pnl:+,.0f} ({total_pnl_pct:+.1f}%)",
            ]
        )

    return "\n".join(lines)


async def _load_prices() -> dict:
    try:
        prices, _ = await get_full_realtime_context()
    except Exception as exc:
        logger.warning("portfolio price refresh failed: %s", exc, exc_info=True)
        return {}
    return prices


async def show_portfolio(event: Message | CallbackQuery) -> None:
    """Show portfolio for either a message or a callback query."""
    user_id = event.from_user.id
    positions = await get_portfolio(user_id)
    prices = await _load_prices()
    text = _build_portfolio_text(positions, prices)
    reply_markup = portfolio_keyboard(bool(positions))

    if isinstance(event, CallbackQuery):
        await event.message.answer(text, reply_markup=reply_markup)
        return

    await event.answer(text, reply_markup=reply_markup)


async def handle_portfolio_callback(callback: CallbackQuery) -> None:
    """Handle all portfolio:* callback actions."""
    user_id = callback.from_user.id
    action, symbol = _parse_portfolio_callback(callback.data)
    await callback.answer()

    if action == "add_select":
        await callback.message.edit_text(
            "Выбери актив:",
            reply_markup=select_crypto_keyboard(),
        )
        return

    if action == "add_amount":
        if symbol not in ALLOWED_SYMBOLS:
            await callback.message.edit_text("Неизвестный актив")
            return
        user_portfolio_state[user_id] = {"symbol": symbol, "step": "amount"}
        await callback.message.edit_text(
            f"Сколько {symbol} ты купил?\nВведи число, например 0.5"
        )
        return

    if action == "menu":
        await callback.message.delete()
        await show_portfolio(callback)
        return

    if action == "refresh":
        await callback.message.delete()
        await show_portfolio(callback)
        return

    if action == "remove_select":
        positions = await get_portfolio(user_id)
        if not positions:
            await callback.message.edit_text(
                "Нечего удалять!",
                reply_markup=portfolio_keyboard(False),
            )
            return
        await callback.message.edit_text(
            "Что удалить?",
            reply_markup=_build_remove_keyboard(positions),
        )
        return

    if action == "confirm_remove":
        await remove_portfolio_position(user_id, symbol)
        user_portfolio_state.pop(user_id, None)
        await callback.message.delete()
        await show_portfolio(callback)


async def handle_portfolio_text_input(message: Message) -> bool:
    """
    Handle text input for portfolio amount/entry steps.

    Returns True when the message has been consumed by the portfolio flow.
    """
    user_id = message.from_user.id
    state = user_portfolio_state.get(user_id)
    if not state:
        return False

    value = _parse_positive_number((message.text or "").strip())
    step = state.get("step")

    if step == "amount":
        if value is None:
            await message.answer("Введи число, например 0.5")
            return True
        state["amount"] = value
        state["step"] = "price"
        await message.answer(
            f"По какой цене купил {state['symbol']}?\nВведи цену, например 65000"
        )
        return True

    if step == "price":
        if value is None:
            await message.answer("Введи цену, например 65000")
            return True
        symbol = str(state["symbol"])
        amount = float(state["amount"])
        await add_portfolio_position(user_id, symbol, amount, value)
        user_portfolio_state.pop(user_id, None)
        await message.answer(f"✅ Добавлено: {symbol} | {amount} шт. | ${value:,.0f}")
        return True

    user_portfolio_state.pop(user_id, None)
    return False


async def cmd_add_portfolio(message: Message) -> None:
    """/add BTC 0.5 65000"""
    parts = (message.text or "").split()
    if len(parts) != 4:
        await message.answer(
            "❌ Неверный формат.\n\n"
            "Пример: /add BTC 0.5 65000\n"
            "Формат: /add СИМВОЛ КОЛИЧЕСТВО ЦЕНА_ВХОДА"
        )
        return

    symbol = parts[1].upper()
    amount = _parse_positive_number(parts[2])
    entry_price = _parse_positive_number(parts[3])

    if symbol not in ALLOWED_SYMBOLS:
        await message.answer(f"❌ Пока только: {', '.join(ALLOWED_SYMBOLS)}")
        return
    if amount is None or entry_price is None:
        await message.answer("❌ Введите положительные числа правильно.")
        return

    await add_portfolio_position(message.from_user.id, symbol, amount, entry_price)
    await message.answer(f"✅ Добавлено:\n{symbol} | {amount} шт. | Вход: ${entry_price:,.0f}")


async def cmd_remove_portfolio(message: Message) -> None:
    """/remove BTC"""
    parts = (message.text or "").split()
    if len(parts) != 2:
        await message.answer("Пример: /remove BTC")
        return

    symbol = parts[1].upper()
    if symbol not in ALLOWED_SYMBOLS:
        await message.answer(f"❌ Пока только: {', '.join(ALLOWED_SYMBOLS)}")
        return

    await remove_portfolio_position(message.from_user.id, symbol)
    user_portfolio_state.pop(message.from_user.id, None)
    await message.answer(f"✅ Удалено: {symbol}")
