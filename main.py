"""
Dialectic Edge v6.0 — UX апгрейд.
- Одно сообщение вместо 6 (краткая выжимка + Synth)
- Кнопка "📖 Полные дебаты" — листаешь раунды по одному
- Простой язык в выводах для обычных людей
"""

import asyncio
import logging
from datetime import datetime

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton
)

from config import BOT_TOKEN, ADMIN_IDS
from news_fetcher import NewsFetcher
from data_sources import fetch_full_context
from web_search import get_full_realtime_context, search_news_context
from meta_analyst import get_meta_context
from sentiment import analyze_and_filter, format_for_agents
from agents import DebateOrchestrator
from storage import Storage
from database import (
    init_db, upsert_user, get_user, increment_requests,
    get_daily_subscribers, set_daily_sub,
    get_track_record, save_feedback, get_feedback_stats,
    log_report, get_admin_stats
)
from tracker import check_pending_predictions, save_predictions_from_report
from scheduler import Scheduler
from user_profile import (
    init_profiles_table, save_profile, get_profile,
    build_profile_instruction, format_profile_card,
    RISK_PROFILES, HORIZONS, MARKETS
)
from weekly_report import build_weekly_report, send_weekly_reports
from russia_data import fetch_russia_context
from russia_agents import run_russia_analysis

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
fetcher = NewsFetcher()
storage = Storage()

FREE_DAILY_LIMIT = 5

scheduler: Scheduler = None

# Хранилище дебатов для листания по кнопкам
# {user_id: {"rounds": [...], "full_report": str}}
debate_cache: dict = {}

# Кэш РФ анализа (обновляется вместе с /daily)
russia_cache: dict = {}  # {"report": str, "timestamp": str}


# ─── Утилиты ──────────────────────────────────────────────────────────────────

def clean_markdown(text: str) -> str:
    lines = text.split("\n")
    clean_lines = []
    for line in lines:
        if line.count("*") % 2 != 0:
            line = line.replace("*", "")
        if line.count("_") % 2 != 0:
            line = line.replace("_", "")
        if line.count("`") % 2 != 0:
            line = line.replace("`", "")
        clean_lines.append(line)
    return "\n".join(clean_lines)


def split_message(text: str, max_len: int = 4000) -> list:
    text = clean_markdown(text)
    if len(text) <= max_len:
        return [text]
    chunks = []
    while len(text) > max_len:
        split_at = text.rfind("\n", 0, max_len)
        if split_at == -1:
            split_at = max_len
        chunks.append(text[:split_at])
        text = text[split_at:].lstrip("\n")
    if text:
        chunks.append(text)
    return chunks


async def check_limit(user_id: int) -> bool:
    user = await get_user(user_id)
    if not user:
        return True
    if user.get("tier") == "pro":
        return True
    return user.get("requests_today", 0) < FREE_DAILY_LIMIT


def feedback_keyboard(report_type: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="👍 Полезно", callback_data=f"fb:1:{report_type}"),
        InlineKeyboardButton(text="👎 Мимо",    callback_data=f"fb:-1:{report_type}"),
    ]])


def signal_to_stars(confidence) -> str:
    mapping = {"HIGH": 0.85, "MEDIUM": 0.55, "LOW": 0.25, "EXTREME": 0.95}
    if isinstance(confidence, str):
        confidence = mapping.get(confidence.upper(), 0.5)
    try:
        confidence = float(confidence)
    except (TypeError, ValueError):
        confidence = 0.5
    stars = max(1, min(5, round(confidence * 5)))
    return "⭐" * stars + "☆" * (5 - stars)


# ─── Парсинг отчёта на части ──────────────────────────────────────────────────

def parse_report_parts(report: str) -> dict:
    """
    Разбивает полный отчёт на:
    - header: шапка с датой и звёздами
    - rounds: список раундов дебатов [раунд1, раунд2, раунд3]
    - synthesis: итоговый синтез Synth
    - disclaimer: нижний дисклеймер
    """
    parts = {
        "header": "",
        "rounds": [],
        "synthesis": "",
        "disclaimer": "",
        "full": report
    }

    # Вытаскиваем дисклеймер
    disclaimer_marker = "─────────────────────────\n🤝 Честно о боте:"
    if disclaimer_marker in report:
        idx = report.find(disclaimer_marker)
        parts["disclaimer"] = report[idx:]
        report = report[:idx]

    # Вытаскиваем синтез
    synth_marker = "⚖️ *ИТОГОВЫЙ СИНТЕЗ И РЕКОМЕНДАЦИИ*"
    if synth_marker in report:
        idx = report.find(synth_marker)
        parts["synthesis"] = report[idx:].strip()
        report = report[:idx]

    # Вытаскиваем раунды
    round_markers = [
        "── Раунд 1:",
        "── Раунд 2:",
        "── Раунд 3:",
    ]

    debate_marker = "🗣 *ДЕБАТЫ АГЕНТОВ*"
    if debate_marker in report:
        debate_idx = report.find(debate_marker)
        parts["header"] = report[:debate_idx].strip()
        debate_section = report[debate_idx:]

        # Разбиваем на раунды
        current_round = ""
        current_round_num = 0
        for line in debate_section.split("\n"):
            is_round_header = any(m in line for m in round_markers)
            if is_round_header:
                if current_round.strip() and current_round_num > 0:
                    parts["rounds"].append(current_round.strip())
                current_round = line + "\n"
                current_round_num += 1
            else:
                current_round += line + "\n"

        if current_round.strip() and current_round_num > 0:
            parts["rounds"].append(current_round.strip())

        if not parts["rounds"]:
            parts["rounds"] = [debate_section]
    else:
        parts["header"] = report.strip()

    return parts


def build_short_report(parts: dict, stars: str, pct: int) -> str:
    """
    Строит короткое сообщение (1 TG-сообщение):
    - Шапка + звёзды
    - Краткие позиции Bull и Bear (по 2-3 строки)
    - Полный Synth
    - Дисклеймер
    """
    now = datetime.now().strftime("%d.%m.%Y %H:%M")

    # Пытаемся вытащить первые 2-3 строки Bull и Bear из раунда 1
    bull_summary = ""
    bear_summary = ""

    if parts["rounds"]:
        round1 = parts["rounds"][0]
        lines = round1.split("\n")

        # Ищем Bull
        in_bull = False
        bull_lines = []
        for line in lines:
            if "🐂 Bull" in line:
                in_bull = True
                continue
            if in_bull:
                if "🐻 Bear" in line:
                    break
                if line.strip() and not line.startswith("──"):
                    bull_lines.append(line.strip())
                if len(bull_lines) >= 3:
                    break

        # Ищем Bear
        in_bear = False
        bear_lines = []
        for line in lines:
            if "🐻 Bear" in line:
                in_bear = True
                continue
            if in_bear:
                if line.strip() and not line.startswith("──"):
                    bear_lines.append(line.strip())
                if len(bear_lines) >= 3:
                    break

        bull_summary = "\n".join(bull_lines[:3]) if bull_lines else "Позиция бычья"
        bear_summary = "\n".join(bear_lines[:3]) if bear_lines else "Позиция медвежья"

    short = (
        f"📊 *DIALECTIC EDGE — ЕЖЕДНЕВНЫЙ ДАЙДЖЕСТ*\n"
        f"🕐 _{now}_\n\n"
        f"💬 _4 AI-модели изучили рынок и поспорили. Вот что вышло:_\n\n"
        f"📶 *Уровень сигнала:* {stars} ({pct}% уверенности)\n"
        f"_Больше звёзд = данные чище и противоречивее_\n\n"
        f"{'─' * 30}\n\n"
        f"🐂 *Бычья позиция (кратко):*\n{bull_summary}\n\n"
        f"🐻 *Медвежья позиция (кратко):*\n{bear_summary}\n\n"
        f"{'─' * 30}\n\n"
        f"{parts['synthesis']}\n\n"
        f"{parts['disclaimer']}"
    )

    return clean_markdown(short)


def debates_keyboard(user_id: int, round_idx: int, total_rounds: int) -> InlineKeyboardMarkup:
    """Клавиатура для листания раундов дебатов."""
    buttons = []

    nav_row = []
    if round_idx > 0:
        nav_row.append(InlineKeyboardButton(
            text="◀️ Назад",
            callback_data=f"debate:{user_id}:{round_idx - 1}"
        ))
    nav_row.append(InlineKeyboardButton(
        text=f"📄 {round_idx + 1}/{total_rounds}",
        callback_data="debate:noop"
    ))
    if round_idx < total_rounds - 1:
        nav_row.append(InlineKeyboardButton(
            text="Вперёд ▶️",
            callback_data=f"debate:{user_id}:{round_idx + 1}"
        ))

    buttons.append(nav_row)
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def main_report_keyboard(user_id: int, has_debates: bool = True) -> InlineKeyboardMarkup:
    """Клавиатура под основным отчётом."""
    buttons = []
    if has_debates:
        buttons.append([
            InlineKeyboardButton(
                text="📖 Полные дебаты агентов",
                callback_data=f"debate:{user_id}:0"
            )
        ])
    buttons.append([
        InlineKeyboardButton(text="👍 Полезно", callback_data=f"fb:1:daily"),
        InlineKeyboardButton(text="👎 Мимо",    callback_data=f"fb:-1:daily"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


# ─── Обработчик листания дебатов ──────────────────────────────────────────────

@dp.callback_query(F.data.startswith("debate:"))
async def handle_debate_page(callback: CallbackQuery):
    parts = callback.data.split(":")
    if len(parts) < 3:
        await callback.answer()
        return

    _, user_id_str, action = parts[0], parts[1], parts[2]
    user_id = int(user_id_str)

    if action == "noop":
        await callback.answer()
        return

    round_idx = int(action)

    # Берём дебаты из кэша
    cache = debate_cache.get(user_id)
    if not cache:
        await callback.answer("❌ Дебаты устарели, запусти /daily заново")
        return

    rounds = cache["rounds"]
    if round_idx >= len(rounds):
        await callback.answer()
        return

    round_text = clean_markdown(rounds[round_idx])

    # Если текст слишком длинный — режем
    if len(round_text) > 4000:
        round_text = round_text[:3900] + "\n\n_...сокращено..._"

    kb = debates_keyboard(user_id, round_idx, len(rounds))

    try:
        await callback.message.edit_text(
            round_text,
            parse_mode="Markdown",
            reply_markup=kb
        )
    except Exception:
        # Если не получается edit — отправляем новое
        await callback.message.answer(
            round_text,
            parse_mode="Markdown",
            reply_markup=kb
        )

    await callback.answer()


# ─── /start ───────────────────────────────────────────────────────────────────

@dp.message(Command("start"))
async def cmd_start(message: Message):
    await upsert_user(
        message.from_user.id,
        message.from_user.username or "",
        message.from_user.first_name or ""
    )
    name = message.from_user.first_name or "трейдер"
    await message.answer(
        f"👋 Привет, *{name}*!\n\n"
        "🧠 *Dialectic Edge* — честный AI-аналитик рынков\n\n"
        "4 агента спорят используя *живые данные*:\n"
        "🐂 *Bull* — ищет возможности роста\n"
        "🐻 *Bear* — указывает риски\n"
        "🔍 *Verifier* — проверяет каждую цифру\n"
        "⚖️ *Synth* — итог адаптированный под тебя\n\n"
        "📋 *Команды:*\n"
        "• /profile — настрой риск-профиль (важно сделать первым)\n"
        "• /daily — дайджест рынков\n"
        "• /analyze [текст] — анализ новости\n"
        "• /trackrecord — история точности агентов\n"
        "• /weeklyreport — отчёт за неделю\n"
        "• /subscribe — авторассылка\n"
        "• /markets — текущие цены\n"
        "• /russia — анализ для российского рынка 🇷🇺\n\n"
        "⚠️ _Не финансовый совет. Будущее неизвестно никому._",
        parse_mode="Markdown"
    )


# ─── /profile ─────────────────────────────────────────────────────────────────

@dp.message(Command("profile"))
async def cmd_profile(message: Message):
    user_id = message.from_user.id
    await upsert_user(user_id)
    profile = await get_profile(user_id)

    risk_kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🛡️ Консерватор", callback_data="profile:risk:conservative"),
            InlineKeyboardButton(text="⚖️ Умеренный",   callback_data="profile:risk:moderate"),
            InlineKeyboardButton(text="🚀 Агрессивный", callback_data="profile:risk:aggressive"),
        ],
        [
            InlineKeyboardButton(text="⚡ Скальпинг", callback_data="profile:hz:scalp"),
            InlineKeyboardButton(text="📈 Свинг",     callback_data="profile:hz:swing"),
            InlineKeyboardButton(text="💎 Инвест",    callback_data="profile:hz:invest"),
        ],
        [
            InlineKeyboardButton(text="₿ Крипта",    callback_data="profile:mkt:crypto"),
            InlineKeyboardButton(text="📈 Акции",     callback_data="profile:mkt:stocks"),
            InlineKeyboardButton(text="🌍 Всё",       callback_data="profile:mkt:all"),
        ],
    ])

    await message.answer(
        f"⚙️ *Настройка профиля*\n\n"
        f"{format_profile_card(profile)}\n\n"
        f"*Выбери параметры:*\n"
        f"_Строка 1_ — риск-профиль\n"
        f"_Строка 2_ — горизонт торговли\n"
        f"_Строка 3_ — рынки\n\n"
        f"Агенты адаптируют анализ под твои настройки.",
        parse_mode="Markdown",
        reply_markup=risk_kb
    )


@dp.callback_query(F.data.startswith("profile:"))
async def handle_profile(callback: CallbackQuery):
    _, param_type, value = callback.data.split(":")
    user_id = callback.from_user.id
    profile = await get_profile(user_id)

    if param_type == "risk":
        profile["risk"] = value
    elif param_type == "hz":
        profile["horizon"] = value
    elif param_type == "mkt":
        profile["markets"] = value

    await save_profile(
        user_id,
        profile.get("risk", "moderate"),
        profile.get("horizon", "swing"),
        profile.get("markets", "all")
    )

    labels = {
        "conservative": "🛡️ Консерватор", "moderate": "⚖️ Умеренный",
        "aggressive": "🚀 Агрессивный",   "scalp": "⚡ Скальпинг",
        "swing": "📈 Свинг",              "invest": "💎 Инвестиции",
        "crypto": "₿ Крипта",             "stocks": "📈 Акции",
        "all": "🌍 Все рынки",
    }

    await callback.answer(f"✅ Сохранено: {labels.get(value, value)}")
    await callback.message.edit_text(
        f"✅ *Профиль обновлён*\n\n{format_profile_card(profile)}\n\n"
        f"Следующий анализ будет адаптирован под тебя.",
        parse_mode="Markdown"
    )


# ─── Ядро анализа ─────────────────────────────────────────────────────────────

async def run_full_analysis(
    user_id: int,
    custom_news: str = "",
    custom_mode: bool = False
) -> str:
    tasks = [
        fetcher.fetch_all(),
        fetch_full_context(),
        get_full_realtime_context(),
        get_profile(user_id),
        get_meta_context(),
    ]

    news, geo_context, (prices_dict, live_prices), profile, meta_context = await asyncio.gather(
        *tasks, return_exceptions=True
    )

    if isinstance(news, Exception):         news = ""
    if isinstance(geo_context, Exception):  geo_context = ""
    if isinstance(live_prices, Exception):  live_prices = ""
    if isinstance(profile, Exception):      profile = {"risk": "moderate", "horizon": "swing", "markets": "all"}
    if isinstance(meta_context, Exception): meta_context = ""

    profile_instruction = build_profile_instruction(profile)

    if custom_mode and custom_news:
        web_context = await search_news_context(custom_news)
        news_context = (
            f"ТЕМА АНАЛИЗА: {custom_news}\n\n"
            f"{web_context}\n\n{geo_context}\n\n{meta_context}"
        )
    else:
        news_context = (
            f"{geo_context}\n\n=== НОВОСТИ ===\n{news}\n\n{meta_context}"
        )

    sentiment_result, confidence_instruction = analyze_and_filter(
        news_context, str(live_prices)
    )
    sentiment_block = format_for_agents(sentiment_result, confidence_instruction)

    logger.info(
        f"Sentiment: {sentiment_result.label} | "
        f"Confidence: {sentiment_result.confidence} | "
        f"Score: {sentiment_result.score:+.2f}"
    )

    orchestrator = DebateOrchestrator()
    report = await orchestrator.run_debate(
        news_context=news_context,
        live_prices=live_prices,
        profile_instruction=profile_instruction + sentiment_block,
        custom_mode=custom_mode
    )

    # ── Уровень сигнала ───────────────────────────────────────────────────────
    _conf_raw = sentiment_result.confidence
    _conf_map = {"HIGH": 0.85, "MEDIUM": 0.55, "LOW": 0.25, "EXTREME": 0.95}
    if isinstance(_conf_raw, str):
        _conf_num = _conf_map.get(_conf_raw.upper(), 0.5)
    else:
        try:
            _conf_num = float(_conf_raw)
        except (TypeError, ValueError):
            _conf_num = 0.5

    stars = signal_to_stars(_conf_num)
    pct   = int(_conf_num * 100)

    separator = "─" * 30 + "\n"
    signal_line = (
        f"📶 *Уровень сигнала:* {stars} ({pct}% уверенности)\n"
        f"_Чем больше звёзд — тем чище и противоречивее данные для анализа_\n\n"
    )
    report = report.replace(separator, separator + signal_line, 1)

    # ── Сохраняем прогнозы ────────────────────────────────────────────────────
    source = custom_news[:300] if custom_mode else str(news)[:300]
    await save_predictions_from_report(report, source_news=source)
    await log_report(
        user_id,
        "analyze" if custom_mode else "daily",
        source,
        report[:500]
    )

    if not custom_mode:
        storage.cache_report(report)
        if scheduler is not None:
            asyncio.create_task(scheduler.export_now())

    return report


# ─── /daily ───────────────────────────────────────────────────────────────────

async def run_daily_analysis(user_id: int) -> str:
    return await run_full_analysis(user_id)


@dp.message(Command("daily"))
async def cmd_daily(message: Message):
    user_id = message.from_user.id
    await upsert_user(user_id, message.from_user.username or "")

    if not await check_limit(user_id):
        await message.answer(
            f"⛔ *Лимит* — {FREE_DAILY_LIMIT} запросов/день (free)\n"
            "Попробуй завтра или /subscribe для авторассылки.",
            parse_mode="Markdown"
        )
        return

    cached = storage.get_cached_report()
    if cached:
        report = cached['report']
        _conf_map = {"HIGH": 0.85, "MEDIUM": 0.55, "LOW": 0.25, "EXTREME": 0.95}
        parts = parse_report_parts(report)
        short = build_short_report(parts, "⭐⭐⭐⭐☆", 85)

        # Кэшируем раунды для листания
        debate_cache[user_id] = {"rounds": parts["rounds"], "full": report}

        chunks = split_message(short)
        for chunk in chunks[:-1]:
            await message.answer(chunk, parse_mode="Markdown")
        await message.answer(
            chunks[-1],
            parse_mode="Markdown",
            reply_markup=main_report_keyboard(user_id, has_debates=bool(parts["rounds"]))
        )
        await message.answer(
            f"📦 _Кэш от {cached['timestamp']}. Новый через 2ч._",
            parse_mode="Markdown"
        )
        return

    wait_msg = await message.answer(
        "⏳ *Запускаю анализ...*\n\n"
        "🔄 Живые цены → новости → геополитика → дебаты агентов\n"
        "_Займёт 2–5 минут..._",
        parse_mode="Markdown"
    )

    try:
        await increment_requests(user_id)
        report = await run_daily_analysis(user_id)
        await bot.delete_message(chat_id=message.chat.id, message_id=wait_msg.message_id)

        # Парсим отчёт на части
        parts = parse_report_parts(report)

        # Вычисляем звёзды из отчёта
        stars_line = ""
        pct_val = 85
        if "Уровень сигнала" in report:
            import re
            m = re.search(r"Уровень сигнала.*?(\d+)%", report)
            if m:
                pct_val = int(m.group(1))
        stars_str = signal_to_stars(pct_val / 100)

        # Кэшируем раунды для кнопки
        debate_cache[user_id] = {"rounds": parts["rounds"], "full": report}

        # Строим короткое сообщение
        short = build_short_report(parts, stars_str, pct_val)

        chunks = split_message(short)
        for chunk in chunks[:-1]:
            await message.answer(chunk, parse_mode="Markdown")
        await message.answer(
            chunks[-1],
            parse_mode="Markdown",
            reply_markup=main_report_keyboard(user_id, has_debates=bool(parts["rounds"]))
        )

    except Exception as e:
        logger.error(f"Daily error: {e}", exc_info=True)
        await bot.edit_message_text(
            f"❌ *Ошибка:* `{str(e)[:200]}`\n\n"
            "Проверь: API ключи, интернет, BOT_TOKEN.",
            chat_id=message.chat.id,
            message_id=wait_msg.message_id,
            parse_mode="Markdown"
        )


# ─── /analyze ─────────────────────────────────────────────────────────────────

@dp.message(Command("analyze"))
async def cmd_analyze(message: Message):
    user_id = message.from_user.id
    await upsert_user(user_id, message.from_user.username or "")

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        await message.answer(
            "❗ *Укажи новость для анализа*\n\n"
            "Примеры:\n"
            "`/analyze Fed снизил ставку до 4.25%`\n"
            "`/analyze Binance заморозила вывод в США`\n"
            "`/analyze Китай ограничил экспорт редкоземельных металлов`",
            parse_mode="Markdown"
        )
        return

    if not await check_limit(user_id):
        await message.answer(
            f"⛔ *Лимит* — {FREE_DAILY_LIMIT} запросов/день (free)",
            parse_mode="Markdown"
        )
        return

    user_news = parts[1].strip()
    wait_msg = await message.answer(
        f"🔍 *Анализирую:*\n_{user_news[:150]}_\n\n"
        "⏳ Ищу контекст + запускаю дебаты...",
        parse_mode="Markdown"
    )

    try:
        await increment_requests(user_id)
        report = await run_full_analysis(user_id, custom_news=user_news, custom_mode=True)
        await bot.delete_message(chat_id=message.chat.id, message_id=wait_msg.message_id)

        report_parts = parse_report_parts(report)
        debate_cache[user_id] = {"rounds": report_parts["rounds"], "full": report}

        pct_val = 85
        import re
        m = re.search(r"Уровень сигнала.*?(\d+)%", report)
        if m:
            pct_val = int(m.group(1))
        stars_str = signal_to_stars(pct_val / 100)

        short = build_short_report(report_parts, stars_str, pct_val)
        chunks = split_message(short)
        for chunk in chunks[:-1]:
            await message.answer(chunk, parse_mode="Markdown")
        await message.answer(
            chunks[-1],
            parse_mode="Markdown",
            reply_markup=main_report_keyboard(user_id, has_debates=bool(report_parts["rounds"]))
        )

    except Exception as e:
        logger.error(f"Analyze error: {e}", exc_info=True)
        await bot.edit_message_text(
            f"❌ *Ошибка:* `{str(e)[:200]}`",
            chat_id=message.chat.id,
            message_id=wait_msg.message_id,
            parse_mode="Markdown"
        )



# ─── /russia ──────────────────────────────────────────────────────────────────

@dp.message(Command("russia"))
async def cmd_russia(message: Message):
    user_id = message.from_user.id
    await upsert_user(user_id, message.from_user.username or "")

    if not await check_limit(user_id):
        await message.answer(
            f"⛔ *Лимит* — {FREE_DAILY_LIMIT} запросов/день (free)",
            parse_mode="Markdown"
        )
        return

    # Проверяем кэш РФ (живёт 2 часа как основной)
    import time
    now_ts = time.time()
    if russia_cache.get("report") and (now_ts - russia_cache.get("ts", 0)) < 7200:
        cached_ru = russia_cache["report"]
        for chunk in split_message(cached_ru):
            await message.answer(chunk, parse_mode="Markdown")
        await message.answer(
            f"📦 _Кэш от {russia_cache['timestamp']}. Новый через 2ч._",
            parse_mode="Markdown",
            reply_markup=feedback_keyboard("russia")
        )
        return

    # Нужен глобальный анализ как основа
    global_report = ""
    cached = storage.get_cached_report()
    if cached:
        global_report = cached["report"]
    else:
        global_report = "Глобальный анализ пока не готов. Запусти /daily сначала."

    # Если нет кэша /daily — предлагаем выбор
    if not storage.get_cached_report():
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="✅ Сначала запущу /daily",
                callback_data="russia_choice:daily"
            ),
            InlineKeyboardButton(
                text="🚀 Запустить сейчас",
                callback_data="russia_choice:now"
            ),
        ]])
        await message.answer(
            "💡 *Совет перед запуском /russia:*\n\n"
            "Глобальный дайджест (/daily) даёт агентам полный контекст рынков.\n"
            "Без него анализ будет работать только на РФ данных.\n\n"
            "*Что делаем?*",
            parse_mode="Markdown",
            reply_markup=kb
        )
        return

    wait_msg = await message.answer(
        "🇷🇺 *Запускаю анализ для России...*\n\n"
        "🔄 ЦБ РФ → Мосбиржа → РБК → Llama агенты → Mistral синтез\n"
        "_Займёт 1–3 минуты..._",
        parse_mode="Markdown"
    )

    try:
        await increment_requests(user_id)

        # Собираем РФ данные
        russia_context = await fetch_russia_context()

        # Запускаем диалектический анализ
        report = await run_russia_analysis(global_report, russia_context)

        # Кэшируем
        from datetime import datetime
        import time
        russia_cache["report"]    = report
        russia_cache["timestamp"] = datetime.now().strftime("%d.%m.%Y %H:%M")
        russia_cache["ts"]        = time.time()

        await bot.delete_message(chat_id=message.chat.id, message_id=wait_msg.message_id)

        for chunk in split_message(report):
            await message.answer(chunk, parse_mode="Markdown")

        await message.answer(
            "💬 *Был ли анализ полезным?*",
            parse_mode="Markdown",
            reply_markup=feedback_keyboard("russia")
        )

    except Exception as e:
        logger.error(f"Russia error: {e}", exc_info=True)
        await bot.edit_message_text(
            f"❌ *Ошибка:* `{str(e)[:200]}`",
            chat_id=message.chat.id,
            message_id=wait_msg.message_id,
            parse_mode="Markdown"
        )



# ─── Выбор перед /russia ──────────────────────────────────────────────────────

@dp.callback_query(F.data.startswith("russia_choice:"))
async def handle_russia_choice(callback: CallbackQuery):
    action = callback.data.split(":")[1]
    user_id = callback.from_user.id

    await callback.message.edit_reply_markup(reply_markup=None)

    if action == "daily":
        await callback.answer()
        await callback.message.answer(
            "✅ Отличный выбор! Запускай /daily — после него /russia выдаст максимум.",
            parse_mode="Markdown"
        )
        return

    # action == "now" — запускаем сразу
    await callback.answer("🚀 Запускаю!")

    wait_msg = await callback.message.answer(
        "🇷🇺 *Запускаю анализ для России...*\n\n"
        "🔄 ЦБ РФ → Мосбиржа → РБК → Llama агенты → Mistral синтез\n"
        "_Займёт 1–3 минуты..._",
        parse_mode="Markdown"
    )

    try:
        await increment_requests(user_id)
        global_report = "Глобальный анализ не запускался. Работаю только на данных РФ."
        russia_context = await fetch_russia_context()
        report = await run_russia_analysis(global_report, russia_context)

        import time
        russia_cache["report"]    = report
        russia_cache["timestamp"] = datetime.now().strftime("%d.%m.%Y %H:%M")
        russia_cache["ts"]        = time.time()

        await bot.delete_message(
            chat_id=callback.message.chat.id,
            message_id=wait_msg.message_id
        )

        for chunk in split_message(report):
            await callback.message.answer(chunk, parse_mode="Markdown")

        await callback.message.answer(
            "💬 *Был ли анализ полезным?*",
            parse_mode="Markdown",
            reply_markup=feedback_keyboard("russia")
        )

    except Exception as e:
        logger.error(f"Russia choice error: {e}", exc_info=True)
        await bot.edit_message_text(
            f"❌ *Ошибка:* `{str(e)[:200]}`",
            chat_id=callback.message.chat.id,
            message_id=wait_msg.message_id,
            parse_mode="Markdown"
        )


# ─── /markets ─────────────────────────────────────────────────────────────────

@dp.message(Command("markets"))
async def cmd_markets(message: Message):
    await upsert_user(message.from_user.id)
    wait_msg = await message.answer("⏳ Загружаю живые данные...")
    try:
        _, live_prices = await get_full_realtime_context()
        now = datetime.now().strftime("%d.%m.%Y %H:%M")
        safe_prices = clean_markdown(live_prices)
        await bot.edit_message_text(
            f"📊 *РЫНКИ — {now}*\n\n{safe_prices}",
            chat_id=message.chat.id,
            message_id=wait_msg.message_id,
            parse_mode="Markdown"
        )
    except Exception as e:
        await bot.edit_message_text(
            f"❌ Ошибка: {e}",
            chat_id=message.chat.id,
            message_id=wait_msg.message_id
        )


# ─── /trackrecord ─────────────────────────────────────────────────────────────

@dp.message(Command("trackrecord"))
async def cmd_trackrecord(message: Message):
    await upsert_user(message.from_user.id)
    try:
        data     = await get_track_record()
        stats    = data["stats"]
        recent   = data["recent"]
        by_asset = data["by_asset"]

        total   = stats.get("total") or 0
        wins    = stats.get("wins") or 0
        losses  = stats.get("losses") or 0
        pending = stats.get("pending") or 0
        avg_pnl = stats.get("avg_pnl") or 0
        best    = stats.get("best_call") or 0
        worst   = stats.get("worst_call") or 0

        if total == 0:
            await message.answer(
                "📊 *Track Record*\n\n"
                "_Прогнозы накапливаются. Запусти /daily — агенты начнут делать прогнозы._\n\n"
                "Через 1-2 недели активного использования здесь появится реальная статистика.",
                parse_mode="Markdown"
            )
            return

        finished  = wins + losses
        winrate   = (wins / finished * 100) if finished > 0 else 0
        wr_emoji  = "🟢" if winrate >= 55 else "🟡" if winrate >= 45 else "🔴"
        pnl_emoji = "🟢" if avg_pnl >= 0 else "🔴"

        lines = [
            "📊 *TRACK RECORD АГЕНТОВ*\n",
            f"*Всего прогнозов:* {total}",
            f"*Завершено:* {finished} | ⏳ Ждут: {pending}",
        ]

        if finished > 0:
            lines += [
                f"*Winrate:* {wr_emoji} *{winrate:.0f}%* ({wins}✅ / {losses}❌)",
                f"*Средний P&L:* {pnl_emoji} *{avg_pnl:+.1f}%*",
            ]
            if best:               lines.append(f"*Лучший:* 🚀 +{best:.1f}%")
            if worst and worst < 0: lines.append(f"*Худший:* 💥 {worst:.1f}%")

        if by_asset:
            lines.append("\n*🏆 Топ активов:*")
            for a in by_asset[:3]:
                wr = (a['wins'] / a['calls'] * 100) if a['calls'] else 0
                lines.append(
                    f"• {a['asset']}: {wr:.0f}% winrate "
                    f"({a['calls']} сигналов, avg {a['avg_pnl']:+.1f}%)"
                )

        if recent:
            lines.append("\n*📋 Последние сигналы:*")
            for r in recent[:5]:
                emoji = "✅" if r["result"] == "win" else "❌"
                pnl   = r.get("pnl_pct") or 0
                lines.append(
                    f"{emoji} {r['asset']} {r['direction']} "
                    f"→ *{pnl:+.1f}%* _{(r.get('created_at') or '')[:10]}_"
                )

        lines.append(
            "\n⚠️ _Прошлые результаты не гарантируют будущих. Не финансовый совет._"
        )
        await message.answer("\n".join(lines), parse_mode="Markdown")

    except Exception as e:
        logger.error(f"Trackrecord error: {e}", exc_info=True)
        await message.answer(f"❌ Ошибка: {e}")


# ─── /weeklyreport ────────────────────────────────────────────────────────────

@dp.message(Command("weeklyreport"))
async def cmd_weekly(message: Message):
    await upsert_user(message.from_user.id)
    wait_msg = await message.answer("⏳ Формирую отчёт за неделю...")
    try:
        report = await build_weekly_report()
        await bot.delete_message(chat_id=message.chat.id, message_id=wait_msg.message_id)
        await message.answer(report, parse_mode="Markdown")
    except Exception as e:
        await bot.edit_message_text(
            f"❌ Ошибка: {e}",
            chat_id=message.chat.id,
            message_id=wait_msg.message_id
        )


# ─── /subscribe ───────────────────────────────────────────────────────────────

@dp.message(Command("subscribe"))
async def cmd_subscribe(message: Message):
    user_id   = message.from_user.id
    await upsert_user(user_id)
    user      = await get_user(user_id)
    is_subbed = user.get("daily_sub", 0) if user else 0
    sub_time  = user.get("sub_time", "08:00") if user else "08:00"
    parts     = message.text.split()

    if len(parts) == 1:
        status = f"✅ Активна (каждый день в *{sub_time} UTC*)" if is_subbed else "❌ Отключена"
        await message.answer(
            f"📬 *Авторассылка*\nСтатус: {status}\n\n"
            f"• `/subscribe on` — включить в 08:00 UTC\n"
            f"• `/subscribe on 09:30` — своё время\n"
            f"• `/subscribe off` — отключить",
            parse_mode="Markdown"
        )
        return

    action   = parts[1].lower()
    time_str = parts[2] if len(parts) > 2 else "08:00"
    try:
        h, m = time_str.split(":")
        assert 0 <= int(h) <= 23 and 0 <= int(m) <= 59
        time_str = f"{int(h):02d}:{int(m):02d}"
    except Exception:
        await message.answer("❌ Формат: HH:MM, например `08:30`", parse_mode="Markdown")
        return

    if action == "on":
        await set_daily_sub(user_id, True, time_str)
        await message.answer(
            f"✅ *Подписка активна*\nКаждый день в *{time_str} UTC*\n\n"
            f"Отключить: `/subscribe off`",
            parse_mode="Markdown"
        )
    elif action == "off":
        await set_daily_sub(user_id, False)
        await message.answer("❌ *Подписка отключена*", parse_mode="Markdown")


# ─── /stats ───────────────────────────────────────────────────────────────────

@dp.message(Command("stats"))
async def cmd_stats(message: Message):
    user_id = message.from_user.id
    await upsert_user(user_id)
    user    = await get_user(user_id)
    profile = await get_profile(user_id)

    if not user:
        await message.answer("Ошибка загрузки.")
        return

    fb           = await get_feedback_stats()
    total_fb     = fb.get("total") or 0
    pos_fb       = fb.get("positive") or 0
    satisfaction = (pos_fb / total_fb * 100) if total_fb > 0 else 0

    risk_name    = RISK_PROFILES.get(profile.get("risk", "moderate"), {}).get("name", "⚖️ Умеренный")
    horizon_name = HORIZONS.get(profile.get("horizon", "swing"), {}).get("name", "📈 Свинг")

    tr      = await get_track_record()
    tr_s    = tr["stats"]
    tr_wins = tr_s.get("wins") or 0
    tr_loss = tr_s.get("losses") or 0
    tr_wr   = (tr_wins / (tr_wins + tr_loss) * 100) if (tr_wins + tr_loss) > 0 else 0

    await message.answer(
        f"📈 *Моя статистика*\n\n"
        f"*Tier:* {'👑 PRO' if user.get('tier')=='pro' else '🆓 Free'}\n"
        f"*Запросов сегодня:* {user.get('requests_today',0)}/{FREE_DAILY_LIMIT}\n"
        f"*Запросов всего:* {user.get('requests_total',0)}\n"
        f"*Профиль:* {risk_name} | {horizon_name}\n"
        f"*Подписка:* {'✅' if user.get('daily_sub') else '❌'}\n\n"
        f"*🎯 Track Record бота:*\n"
        f"Прогнозов: {tr_s.get('total',0)} | Winrate: {tr_wr:.0f}%\n\n"
        f"*Оценки пользователей:*\n"
        f"Оценок: {total_fb} | Позитивных: {satisfaction:.0f}%\n\n"
        f"• /trackrecord — полная история точности\n"
        f"• /weeklyreport — отчёт за неделю\n"
        f"• /profile — изменить профиль",
        parse_mode="Markdown"
    )


# ─── /help ────────────────────────────────────────────────────────────────────

@dp.message(Command("help"))
async def cmd_help(message: Message):
    await upsert_user(message.from_user.id)
    await message.answer(
        "📖 *Dialectic Edge v6.0*\n\n"
        "*Что нового в v6:*\n"
        "• Один отчёт вместо 6 сообщений\n"
        "• Кнопка 📖 Полные дебаты — листай раунды\n"
        "• Простой язык в выводах\n"
        "• Умный Risk/Reward — если риск высокий, бот честно скажет 'ВНЕ РЫНКА'\n\n"
        "*Команды:*\n"
        "• `/profile` — настрой риск-профиль первым\n"
        "• `/daily` — дайджест рынков\n"
        "• `/analyze [текст]` — анализ новости\n"
        "• `/markets` — живые цены\n"
        "• `/trackrecord` — история точности\n"
        "• `/weeklyreport` — отчёт за неделю\n"
        "• `/subscribe on 08:00` — авторассылка\n"
        "• `/russia` — анализ для российского рынка 🇷🇺\n"
        "• `/stats` — твоя статистика\n\n"
        "⚠️ _Не финансовый совет. Будущее неизвестно никому._",
        parse_mode="Markdown"
    )


# ─── /admin ───────────────────────────────────────────────────────────────────

@dp.message(Command("admin"))
async def cmd_admin(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    stats    = await get_admin_stats()
    fb       = await get_feedback_stats()
    tr       = await get_track_record()
    tr_stats = tr["stats"]
    wins     = tr_stats.get("wins") or 0
    losses   = tr_stats.get("losses") or 0
    winrate  = (wins / (wins + losses) * 100) if (wins + losses) > 0 else 0

    await message.answer(
        f"🔧 *ADMIN*\n\n"
        f"👥 Пользователи: {stats['total_users']} | Активных: {stats['active_week']}\n"
        f"📬 Подписчики: {stats['subscribers']}\n"
        f"📊 Запросов: {stats['total_reports']}\n\n"
        f"👍 Фидбек: {fb.get('positive',0)}+ / {fb.get('negative',0)}-\n\n"
        f"🎯 Track Record:\n"
        f"Прогнозов: {tr_stats.get('total',0)} | Winrate: {winrate:.0f}%\n"
        f"Avg P&L: {(tr_stats.get('avg_pnl') or 0):+.1f}%",
        parse_mode="Markdown"
    )


# ─── Фидбек ───────────────────────────────────────────────────────────────────

@dp.callback_query(F.data.startswith("fb:"))
async def handle_feedback(callback: CallbackQuery):
    _, rating_str, report_type = callback.data.split(":")
    await save_feedback(callback.from_user.id, report_type, int(rating_str))
    emoji = "🙏 Спасибо!" if int(rating_str) == 1 else "📝 Учтём!"
    await callback.answer(emoji)
    await callback.message.edit_reply_markup(reply_markup=None)


# ─── Запуск ───────────────────────────────────────────────────────────────────

async def main():
    global scheduler

    await init_db()
    await init_profiles_table()
    logger.info("🚀 Dialectic Edge v6.0 starting...")

    scheduler = Scheduler(
        bot=bot,
        send_daily_fn=run_daily_analysis,
        check_predictions_fn=check_pending_predictions
    )

    await asyncio.gather(
        dp.start_polling(bot),
        scheduler.start()
    )


if __name__ == "__main__":
    asyncio.run(main())
