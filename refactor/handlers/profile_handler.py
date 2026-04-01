"""
Profile handler facade backed by the existing SQLite profile storage.
"""

from __future__ import annotations

import logging
from typing import Dict, Optional

from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from user_profile import format_profile_card, get_profile, save_profile

from ..models import Market, RiskProfile, TimeHorizon, UserProfile

logger = logging.getLogger(__name__)

_profile_cache: Dict[int, UserProfile] = {}


def _map_risk(value: str) -> RiskProfile:
    mapping = {
        "conservative": RiskProfile.CONSERVATIVE,
        "moderate": RiskProfile.MODERATE,
        "aggressive": RiskProfile.AGGRESSIVE,
    }
    return mapping.get(value, RiskProfile.MODERATE)


def _map_horizon(value: str) -> TimeHorizon:
    mapping = {
        "scalp": TimeHorizon.SHORT_TERM,
        "swing": TimeHorizon.MEDIUM_TERM,
        "position": TimeHorizon.LONG_TERM,
        "invest": TimeHorizon.LONG_TERM,
    }
    return mapping.get(value, TimeHorizon.MEDIUM_TERM)


def _map_markets(value: str) -> list[Market]:
    market = {
        "crypto": [Market.CRYPTO],
        "stocks": [Market.STOCKS],
        "forex": [Market.FOREX],
        "commodities": [Market.COMMODITIES],
        "all": [Market.CRYPTO, Market.STOCKS, Market.FOREX, Market.RUSSIA],
    }
    return market.get(value, [Market.CRYPTO])


def load_or_create_profile(user_id: int, username: Optional[str] = None) -> UserProfile:
    profile = _profile_cache.get(user_id)
    if profile:
        return profile

    profile = UserProfile(
        user_id=user_id,
        risk_profile=RiskProfile.MODERATE,
        time_horizon=TimeHorizon.MEDIUM_TERM,
        preferred_markets=[Market.CRYPTO],
        language="RU",
    )
    _profile_cache[user_id] = profile
    return profile


class ProfileHandler:
    def get_settings_keyboard(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="🛡️ Консерватор", callback_data="profile:risk:conservative"),
                    InlineKeyboardButton(text="⚖️ Умеренный", callback_data="profile:risk:moderate"),
                    InlineKeyboardButton(text="🚀 Агрессивный", callback_data="profile:risk:aggressive"),
                ],
                [
                    InlineKeyboardButton(text="⚡ Скальпинг", callback_data="profile:hz:scalp"),
                    InlineKeyboardButton(text="📈 Свинг", callback_data="profile:hz:swing"),
                    InlineKeyboardButton(text="💎 Инвест", callback_data="profile:hz:invest"),
                ],
                [
                    InlineKeyboardButton(text="₿ Крипта", callback_data="profile:mkt:crypto"),
                    InlineKeyboardButton(text="📈 Акции", callback_data="profile:mkt:stocks"),
                    InlineKeyboardButton(text="🌍 Всё", callback_data="profile:mkt:all"),
                ],
            ]
        )

    def get_risk_profile_keyboard(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="🛡️ Консервативный", callback_data="profile_risk_conservative")],
                [InlineKeyboardButton(text="⚖️ Умеренный", callback_data="profile_risk_moderate")],
                [InlineKeyboardButton(text="🚀 Агрессивный", callback_data="profile_risk_aggressive")],
            ]
        )

    def get_time_horizon_keyboard(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="⚡ Короткий", callback_data="profile_horizon_short")],
                [InlineKeyboardButton(text="🔄 Средний", callback_data="profile_horizon_medium")],
                [InlineKeyboardButton(text="📈 Длинный", callback_data="profile_horizon_long")],
            ]
        )

    def get_markets_keyboard(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="₿ Крипто", callback_data="profile_market_crypto")],
                [InlineKeyboardButton(text="📈 Акции", callback_data="profile_market_stocks")],
                [InlineKeyboardButton(text="🌍 Форекс", callback_data="profile_market_forex")],
                [InlineKeyboardButton(text="🇷🇺 РФ", callback_data="profile_market_russia")],
            ]
        )

    async def load_profile(self, user_id: int) -> UserProfile:
        raw = await get_profile(user_id)
        profile = UserProfile(
            user_id=user_id,
            risk_profile=_map_risk(raw.get("risk", "moderate")),
            time_horizon=_map_horizon(raw.get("horizon", "swing")),
            preferred_markets=_map_markets(raw.get("markets", "all")),
            language="RU",
        )
        _profile_cache[user_id] = profile
        return profile

    async def build_settings_message(self, user_id: int) -> str:
        raw = await get_profile(user_id)
        return (
            f"⚙️ *Настройка профиля*\n\n"
            f"{format_profile_card(raw)}\n\n"
            f"*Выбери параметры:*\n"
            f"_Строка 1_ - риск-профиль\n"
            f"_Строка 2_ - горизонт торговли\n"
            f"_Строка 3_ - рынки\n\n"
            f"Агенты адаптируют анализ под твои настройки."
        )

    async def update_from_callback(self, user_id: int, callback_data: str) -> tuple[dict, str]:
        _, param_type, value = callback_data.split(":")
        profile = await get_profile(user_id)

        if param_type == "risk":
            profile["risk"] = value
        elif param_type == "hz":
            profile["horizon"] = value
        elif param_type == "mkt":
            profile["markets"] = value
        else:
            raise ValueError(f"Unsupported profile callback: {callback_data}")

        await save_profile(
            user_id,
            profile.get("risk", "moderate"),
            profile.get("horizon", "swing"),
            profile.get("markets", "all"),
        )

        labels = {
            "conservative": "🛡️ Консерватор",
            "moderate": "⚖️ Умеренный",
            "aggressive": "🚀 Агрессивный",
            "scalp": "⚡ Скальпинг",
            "swing": "📈 Свинг",
            "invest": "💎 Инвестиции",
            "crypto": "₿ Крипта",
            "stocks": "📈 Акции",
            "all": "🌍 Все рынки",
        }
        return profile, labels.get(value, value)


_profile_handler = ProfileHandler()


def get_profile_handler() -> ProfileHandler:
    return _profile_handler


async def show_profile(message: Message, user_id: int) -> None:
    raw = await get_profile(user_id)
    await message.answer(format_profile_card(raw), parse_mode="Markdown")


async def show_profile_settings(message: Message, user_id: int) -> None:
    handler = get_profile_handler()
    await message.answer(
        await handler.build_settings_message(user_id),
        parse_mode="Markdown",
        reply_markup=handler.get_settings_keyboard(),
    )


async def handle_profile_callback(callback: CallbackQuery) -> None:
    profile, label = await get_profile_handler().update_from_callback(
        callback.from_user.id,
        callback.data or "",
    )
    await callback.answer(f"✅ Сохранено: {label}")
    await callback.message.edit_text(
        f"✅ *Профиль обновлён*\n\n{format_profile_card(profile)}\n\n"
        f"Следующий анализ будет адаптирован под тебя.",
        parse_mode="Markdown",
    )


async def show_risk_selection(message: Message) -> None:
    await message.answer(
        "Выбери уровень риска:",
        reply_markup=get_profile_handler().get_risk_profile_keyboard(),
    )


async def show_horizon_selection(message: Message) -> None:
    await message.answer(
        "Выбери горизонт анализа:",
        reply_markup=get_profile_handler().get_time_horizon_keyboard(),
    )


async def show_markets_selection(message: Message) -> None:
    await message.answer(
        "Выбери интересующие рынки:",
        reply_markup=get_profile_handler().get_markets_keyboard(),
    )
