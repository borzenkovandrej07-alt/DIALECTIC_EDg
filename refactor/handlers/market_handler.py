"""
Market handler wired to the current production analysis pipeline.
"""

from __future__ import annotations

import logging
import re
from typing import List, Optional, Tuple

from aiogram.types import Message

from analysis_service import run_full_analysis
from user_profile import get_profile

from ..models import Market
from .debate_handler import store_and_link_debate
from .utils import (
    build_short_report,
    clean_markdown,
    extract_signal_pct_and_stars,
    main_report_keyboard,
    parse_report_parts,
)

logger = logging.getLogger(__name__)

MARKET_PATTERNS = {
    Market.CRYPTO: r"^[A-Z]{2,10}$",
    Market.STOCKS: r"^[A-Z]{1,5}$",
    Market.FOREX: r"^[A-Z]{6}$",
    Market.RUSSIA: r"^[A-Z]{4}$",
}


def parse_market_command(text: str) -> Tuple[Optional[str], Optional[Market]]:
    parts = text.strip().split()
    if len(parts) < 2:
        return None, None

    symbol = parts[1].upper()
    market = None
    if len(parts) >= 3:
        market = {
            "crypto": Market.CRYPTO,
            "stocks": Market.STOCKS,
            "forex": Market.FOREX,
            "russia": Market.RUSSIA,
        }.get(parts[2].lower())

    if not market:
        for candidate, pattern in MARKET_PATTERNS.items():
            if re.match(pattern, symbol):
                market = candidate
                break

    return symbol, market or Market.CRYPTO


class MarketHandler:
    def validate_symbol(self, symbol: str, market: Market) -> bool:
        return bool(re.match(MARKET_PATTERNS.get(market, r"^[A-Z0-9]{1,10}$"), symbol))

    def get_market_display_name(self, symbol: str, market: Market) -> str:
        prefix = {
            Market.CRYPTO: "₿",
            Market.STOCKS: "📈",
            Market.FOREX: "🌍",
            Market.RUSSIA: "🇷🇺",
        }
        return f"{prefix.get(market, '')} {symbol}".strip()

    async def build_analysis_prompt(self, user_id: int, symbol: str, market: Market) -> str:
        profile = await get_profile(user_id)
        risk = profile.get("risk", "moderate")
        horizon = profile.get("horizon", "swing")
        market_name = {
            Market.CRYPTO: "криптовалюта",
            Market.STOCKS: "акции США",
            Market.FOREX: "форекс",
            Market.RUSSIA: "российский рынок",
        }.get(market, market.value)
        return (
            f"Сделай отдельный анализ актива {symbol}.\n"
            f"Рынок: {market_name}.\n"
            f"Профиль пользователя: риск={risk}, горизонт={horizon}.\n"
            f"Нужны тезис bull, тезис bear, проверка рисков и финальный торговый план по {symbol}."
        )

    async def send_analysis_report(
        self,
        message: Message,
        report: str,
        prices: dict,
        symbol: str,
        market: Market,
    ) -> None:
        parts = parse_report_parts(report)
        pct, stars = extract_signal_pct_and_stars(report)
        messages = build_short_report(parts, stars, pct)
        for idx, chunk in enumerate(messages):
            payload = clean_markdown(chunk)
            if idx == len(messages) - 1:
                await message.answer(
                    payload,
                    reply_markup=main_report_keyboard(
                        message.from_user.id,
                        has_debates=bool(parts.get("rounds")),
                    ),
                )
            else:
                await message.answer(payload)
        await store_and_link_debate(
            user_id=message.from_user.id,
            report=report,
            market=self.get_market_display_name(symbol, market),
        )
        logger.info("market report sent for %s to %s", symbol, message.from_user.id)


_market_handler = MarketHandler()


def get_market_handler() -> MarketHandler:
    return _market_handler


async def handle_market_command(message: Message, command_text: str) -> None:
    handler = get_market_handler()
    symbol, market = parse_market_command(command_text)
    if not symbol or not market:
        await message.answer(
            "❌ Формат: /market BTC | /market AAPL stocks | /market EURUSD forex | /market LKOH russia"
        )
        return
    if not handler.validate_symbol(symbol, market):
        await message.answer(f"❌ Некорректный символ: {symbol}")
        return

    display_name = handler.get_market_display_name(symbol, market)
    status = await message.answer(f"⏳ Анализирую {display_name}...")
    try:
        prompt = await handler.build_analysis_prompt(message.from_user.id, symbol, market)
        report, prices = await run_full_analysis(
            user_id=message.from_user.id,
            custom_news=prompt,
            custom_mode=True,
        )
        await status.delete()
        await handler.send_analysis_report(message, report, prices, symbol, market)
    except Exception as exc:
        logger.error("market command failed for %s: %s", symbol, exc, exc_info=True)
        try:
            await status.delete()
        except Exception:
            pass
        await message.answer(f"❌ Ошибка анализа {display_name}: {exc}")


def get_supported_markets() -> List[Market]:
    return list(Market)


def get_market_examples() -> dict:
    return {
        Market.CRYPTO: ["BTC", "ETH", "SOL", "XRP"],
        Market.STOCKS: ["AAPL", "TSLA", "MSFT", "GOOGL"],
        Market.FOREX: ["EURUSD", "GBPUSD", "USDJPY"],
        Market.RUSSIA: ["LKOH", "GAZP", "SBER", "GMKN"],
    }
