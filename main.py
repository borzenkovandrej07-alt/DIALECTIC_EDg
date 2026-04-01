"""
Dialectic Edge v7.1 — UX + FinBERT async + РФ-график.
- Одно сообщение вместо 6 (краткая выжимка + Synth)
- Кнопка "📖 Полные дебаты" — листаешь раунды по одному
- Простой язык в выводах для обычных людей
"""

import re
import asyncio
import logging
import os
from datetime import datetime
from typing import Optional, Tuple

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import (
    Message, CallbackQuery, BufferedInputFile,
    InlineKeyboardMarkup, InlineKeyboardButton, BotCommand
)

from config import (
    BOT_TOKEN,
    ADMIN_IDS,
    CACHE_TTL_HOURS,
    REDIS_URL,
    CACHE_FILE,
    DB_PATH,
    USING_DATA_DIR,
    DEBATE_SNAPSHOT_HOURS,
)
from web_search import get_full_realtime_context
from report_sanitizer import sanitize_full_report
from chart_generator import generate_main_chart, generate_russia_chart
from storage import Storage
from analysis_service import run_full_analysis as analysis_service_run_full_analysis, _fetcher as news_fetcher
from data_sources import fetch_full_context
from meta_analyst import get_meta_context
from github_export import get_previous_digest, push_digest_cache
from sentiment import analyze_and_filter_async, format_for_agents
from user_profile import build_profile_instruction
from news_fetcher import NewsFetcher
from agents import DebateOrchestrator
from tracker import save_predictions_from_report
from database import log_report
from web_search import search_news_context
from database import (
    init_db, upsert_user, get_user, increment_requests,
    save_debate_session,
    set_daily_sub,
    get_track_record, save_feedback, get_feedback_stats,
    import_forecasts_from_markdown,
    get_signals_subscribers, set_signals_sub, get_user_signals_status,
    add_portfolio_position, get_portfolio, remove_portfolio_position,
    add_backtest_signal, close_backtest_signal, get_backtest_signals, get_backtest_stats,
    get_backtest_config, update_backtest_capital, set_backtest_enabled,
    save_daily_context, get_daily_context,
    get_predictions_summary,
)
from tracker import check_pending_predictions
from scheduler import Scheduler
from user_profile import (
    init_profiles_table, get_profile,
    RISK_PROFILES, HORIZONS,
    format_profile_card, save_profile
)
from weekly_report import build_weekly_report
from russia_data import fetch_russia_context, fetch_cbr_data
from russia_agents import run_russia_analysis
from debate_storage import ping_redis, save_debate_redis

# Phase 3 Handler Imports — Market, Debate, Profile, Admin
from refactor.handlers import (
    handle_market_command,
    store_and_link_debate,
    handle_debate_navigation_callback,
    show_profile_settings,
    handle_profile_callback,
    setup_admins,
    handle_stats_command,
    handle_health_command,
    handle_logs_command,
    handle_sysinfo_command,
)

# Phase 4 Provider Imports — AI, Cache, Database, Market Data, News, Storage
from refactor.handlers.utils import (
    build_short_report,
    clean_markdown,
    debate_plain_text,
    extract_signal_pct_and_stars,
    hydrate_debate_from_report,
    main_report_keyboard,
    parse_report_parts,
    split_message,
    strip_digest_summary_text,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

bot: Optional[Bot] = None
dp = Dispatcher()
storage = Storage()

FREE_DAILY_LIMIT = 5

scheduler: Scheduler = None

# Хранилище дебатов для листания по кнопкам
# {user_id: {"rounds": [...], "full_report": str}}

# Кэш РФ анализа (обновляется вместе с /daily)
russia_cache: dict = {}  # {"report": str, "timestamp": str, "sections": {...}, "ts": float}
debate_cache: dict = {}  # {user_id: {"rounds": [...], "full": str}}


def get_bot() -> Bot:
    global bot
    if bot is None:
        bot = Bot(token=BOT_TOKEN)
    return bot


# ─── Утилиты вынесены в refactor/handlers/utils.py ───────────────────────────────────


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


def extract_signal_pct_and_stars(report: str) -> tuple[int, str]:
    """
    Процент в отчёте — это шкала уверенности FinBERT в классификации тона новостей
    (маппинг HIGH/MEDIUM/LOW → 85/55/25), а не «уверенность в направлении рынка».
    """
    m = re.search(r"Уровень\s+сигнала[^\d(]*\((\d+)%", report, re.IGNORECASE)
    if not m:
        m = re.search(r"📶[^\n]{0,160}\((\d+)%", report)
    pct = int(m.group(1)) if m else 50
    pct = max(0, min(100, pct))
    return pct, signal_to_stars(pct / 100)


SIGNAL_PCT_EXPLAINED = (
    "Число % — уверенность FinBERT в тоне новостей "
    "(EXTREME≈95%, HIGH≈85%, MEDIUM≈55%, LOW≈25%), "
    "не прогноз «рынок пойдёт вверх/вниз». Звёзды — наглядная шкала той же метрики.\n"
    "Если ниже FinBERT = NEUTRAL/MIXED, процент — насколько модель уверена именно в этой метке тона, "
    "а не «сила бычьего/медвежьего тренда»."
)


# Маркеры должны совпадать с `DebateOrchestrator._format_report` в agents.py
# и со старыми отчётами в кэше.
_SYNTH_START_MARKERS = (
    "⚖️ *ВЕРДИКТ И ТОРГОВЫЙ ПЛАН*",
    "⚖️ ВЕРДИКТ И ТОРГОВЫЙ ПЛАН",
    "⚖️ *ИТОГОВЫЙ СИНТЕЗ И РЕКОМЕНДАЦИИ*",
    "⚖️ ИТОГОВЫЙ СИНТЕЗ И РЕКОМЕНДАЦИИ",
    "ИТОГОВЫЙ СИНТЕЗ",
)
_DEBATE_START_MARKERS = (
    "🗣 *ДЕБАТЫ АГЕНТОВ*",
    "🗣 *ХОД ДЕБАТОВ*",
    "🗣 ХОД ДЕБАТОВ",
    "🗣 ДЕБАТЫ АГЕНТОВ",
)
_ROUND_HEADER_RE = re.compile(r"──\s*Раунд\s+\d+")

# Где начинается блок дебатов (жёсткие строки + запасные варианты — модель/парсер могли слегка сменить разметку)
_DEBATE_START_RES = (
    re.compile(r"🗣\s*\*?\s*ХОД\s+ДЕБАТОВ", re.IGNORECASE),
    re.compile(r"🗣\s*\*?\s*ДЕБАТЫ\s+АГЕНТОВ", re.IGNORECASE),
    re.compile(r"\*?──\*?\s*Раунд\s+1\b"),
    re.compile(r"──\s*Раунд\s+1\b"),
    re.compile(r"🐂\s*Bull\s+Researcher"),
)


def find_debate_start_index(text: str) -> Optional[int]:
    """Индекс начала блока дебатов; None если не найден."""
    hit = _find_first_marker(text, _DEBATE_START_MARKERS)
    if hit:
        return hit[0]
    best: Optional[int] = None
    for rx in _DEBATE_START_RES:
        m = rx.search(text)
        if m and (best is None or m.start() < best):
            best = m.start()
    return best


def _find_first_marker(text: str, markers: Tuple[str, ...]) -> Optional[Tuple[int, str]]:
    best: Optional[Tuple[int, str]] = None
    for m in markers:
        i = text.find(m)
        if i != -1 and (best is None or i < best[0]):
            best = (i, m)
    return best


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

    # Вытаскиваем дисклеймер — пробуем несколько вариантов маркера
    for disc_marker in [
        "─────────────────────────\n🤝 Честно о боте:",
        "─────────────────────────\n🤝 *Честно о боте:*",
        "🤝 Честно о боте:",
        "🤝 *Честно о боте:*",
    ]:
        if disc_marker in report:
            idx = report.find(disc_marker)
            parts["disclaimer"] = report[idx:]
            report = report[:idx]
            break

    # Вытаскиваем синтез — пробуем несколько вариантов маркера (v7 отчёты + старые)
    synth_hit = _find_first_marker(report, _SYNTH_START_MARKERS)
    if synth_hit:
        idx, _ = synth_hit
        parts["synthesis"] = report[idx:].strip()
        report = report[:idx]

    # Вытаскиваем раунды
    round_markers_legacy = (
        "── Раунд 1:",
        "── Раунд 2:",
        "── Раунд 3:",
    )

    debate_idx = find_debate_start_index(report)
    if debate_idx is not None:
        parts["header"] = report[:debate_idx].strip()
        debate_section = report[debate_idx:]

        # Разбиваем на раунды
        current_round = ""
        current_round_num = 0
        for line in debate_section.split("\n"):
            is_round_header = bool(_ROUND_HEADER_RE.search(line)) or any(
                m in line for m in round_markers_legacy
            )
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


def hydrate_debate_from_report(full_report: str) -> dict | None:
    """
    rounds + full для листания дебатов. Если parse_report_parts не выделил раунды,
    берём целиком блок от 🗣 до ⚖️ ВЕРДИКТ (одна «страница» вместо пустого кэша).
    """
    if not full_report or not full_report.strip():
        return None
    parts = parse_report_parts(full_report)
    if parts.get("rounds"):
        return {"rounds": parts["rounds"], "full": parts.get("full", full_report)}
    start = find_debate_start_index(full_report)
    if start is None:
        return None
    tail = full_report[start:]
    synth_hit = _find_first_marker(tail, _SYNTH_START_MARKERS)
    if synth_hit:
        section = tail[: synth_hit[0]].strip()
    else:
        disc_snip = "\n\n─────────────────────────"
        di = tail.find(disc_snip)
        section = tail[:di].strip() if di != -1 else tail.strip()
    if len(section) < 80:
        return None
    return {"rounds": [section], "full": full_report}


def build_short_report(parts: dict, stars: str, pct: int) -> list:
    """
    Возвращает СПИСОК сообщений для отправки.
    Шапка с Bull/Bear кратко — первое сообщение.
    Затем ВЕСЬ синтез + дисклеймер режется на чанки по 2500 символов.
    """
    now = datetime.now().strftime("%d.%m.%Y %H:%M")

    # Вытаскиваем Bull и Bear кратко из раунда 1
    bull_summary = "Позиция бычья"
    bear_summary = "Позиция медвежья"

    if parts["rounds"]:
        round1 = parts["rounds"][0]
        lines = round1.split("\n")
        bull_lines, bear_lines = [], []
        in_bull = in_bear = False
        for line in lines:
            if _ROUND_HEADER_RE.search(line):
                in_bull = in_bear = False
                continue
            if "🐂 Bull" in line:
                in_bull, in_bear = True, False
                continue
            if "🐻 Bear" in line:
                in_bear, in_bull = True, False
                continue
            stripped = line.strip()
            if not stripped or stripped.startswith("──") or re.match(r"^[-_*═─]{3,}\s*$", stripped):
                continue
            if stripped.startswith("#") and len(stripped) < 80:
                continue
            if in_bull and len(bull_lines) < 3:
                bull_lines.append(stripped)
            elif in_bear and len(bear_lines) < 3:
                bear_lines.append(stripped)
        if bull_lines:
            bull_summary = strip_digest_summary_text("\n".join(bull_lines))
        if bear_lines:
            bear_summary = strip_digest_summary_text("\n".join(bear_lines))

    # Шапка — первое сообщение
    header = (
        f"📊 DIALECTIC EDGE — ЕЖЕДНЕВНЫЙ ДАЙДЖЕСТ\n"
        f"🕐 {now}\n\n"
        f"4 AI-модели изучили рынок и поспорили. Вот что вышло:\n\n"
        f"Уровень сигнала: {stars} ({pct}%)\n"
        f"{SIGNAL_PCT_EXPLAINED}\n\n"
        f"{'─' * 30}\n\n"
        f"🐂 Бычья позиция (кратко):\n{bull_summary}\n\n"
        f"🐻 Медвежья позиция (кратко):\n{bear_summary}\n\n"
        f"{'─' * 30}"
    )

    messages = [header]

    # Берём весь контент после шапки из полного отчёта
    # Ищем начало синтеза в полном отчёте
    full = parts.get("full", "")
    synth_hit = _find_first_marker(full, _SYNTH_START_MARKERS)
    synth_start = synth_hit[0] if synth_hit else -1

    if synth_start != -1:
        synth_and_rest = full[synth_start:]
    else:
        synth_and_rest = parts.get("synthesis", "") + "\n\n" + parts.get("disclaimer", "")

    logger.info(f"synth_and_rest size: {len(synth_and_rest)} chars")

    # Режем на чанки по 2500 символов
    chunks = split_message(synth_and_rest, max_len=2500)
    logger.info(f"Total chunks: {len(chunks)}, sizes: {[len(c) for c in chunks]}")
    for chunk in chunks:
        if chunk.strip():
            messages.append(chunk)

    logger.info(f"Total messages to send: {len(messages)}")
    return messages


async def send_debates_attachment(chat_id: int, rounds: list[str]) -> None:
    """
    Все раунды одним .txt в чат — не зависит от RAM/Redis/SQLite после редеплоя Railway.
    Пользователь всегда может открыть файл в истории сообщений.
    """
    if not rounds:
        return
    blocks: list[str] = []
    for i, r in enumerate(rounds, 1):
        blocks.append(f"{'═' * 12} Раунд {i} {'═' * 12}\n\n{debate_plain_text(r)}")
    body = "\n\n".join(blocks)
    raw = body.encode("utf-8")
    max_bytes = 48 * 1024 * 1024  # лимит Telegram ~50 MiB
    if len(raw) > max_bytes:
        raw = raw[:max_bytes]
        body = raw.decode("utf-8", errors="ignore") + "\n\n…файл обрезан по лимиту Telegram"
        raw = body.encode("utf-8")
    fn = f"dialectic_debates_{datetime.now().strftime('%Y-%m-%d_%H%M')}.txt"
    try:
        await bot.send_document(
            chat_id,
            document=BufferedInputFile(raw, filename=fn),
            caption=(
                "📖 Все раунды дебатов в файле — остаётся в этом чате даже если бот перезапустился."
            ),
        )
    except Exception as e:
        logger.warning("Не удалось отправить файл дебатов: %s", e)


async def send_digest_chart(
    chat_id: int,
    report: str,
    prices_dict: dict,
    stars_str: str,
    pct_val: int,
) -> None:
    try:
        buf = generate_main_chart(report, prices_dict or {}, stars_str, pct_val)
        if not buf:
            return
        raw = buf.getvalue() if hasattr(buf, "getvalue") else buf.read()
        await bot.send_photo(
            chat_id,
            photo=BufferedInputFile(raw, filename="dialectic_edge.png"),
        )
    except Exception as e:
        logger.warning("Карточка-график не отправлена: %s", e)


async def send_russia_chart_photo(chat_id: int, report: str) -> None:
    try:
        buf = generate_russia_chart(report)
        if not buf:
            return
        raw = buf.getvalue() if hasattr(buf, "getvalue") else buf.read()
        await bot.send_photo(
            chat_id,
            photo=BufferedInputFile(raw, filename="russia_edge.png"),
        )
    except Exception as e:
        logger.warning("Карточка /russia не отправлена: %s", e)


async def send_daily_digest_bundle(
    chat_id: int,
    user_id: int,
    report: str,
    prices_dict: dict,
) -> None:
    """Текст дайджеста + график (после первого блока) + клавиатура."""
    parts = parse_report_parts(report)
    pct_val, stars_str = extract_signal_pct_and_stars(report)
    hid = hydrate_debate_from_report(report)
    if hid:
        debate_cache[user_id] = hid
    else:
        debate_cache[user_id] = {"rounds": parts["rounds"], "full": report}
    try:
        await save_debate_session(user_id, report)
    except Exception as e:
        logger.warning("save_debate_session: %s", e)
    try:
        await save_debate_redis(user_id, report)
    except Exception as e:
        logger.warning("save_debate_redis: %s", e)
    try:
        storage.save_user_debate_snapshot(user_id, report)
    except Exception as e:
        logger.warning("save_user_debate_snapshot: %s", e)

    messages = build_short_report(parts, stars_str, pct_val)
    logger.info(f"Отправляю {len(messages)} сообщений. Размеры: {[len(m) for m in messages]}")
    for i, msg in enumerate(messages):
        logger.info(f"Отправляю чанк {i+1}/{len(messages)}, размер: {len(msg)}")
        await bot.send_message(chat_id, clean_markdown(msg), parse_mode="Markdown")
        if i == 0:
            await send_digest_chart(chat_id, report, prices_dict or {}, stars_str, pct_val)
        await asyncio.sleep(0.3)
    await bot.send_message(
        chat_id,
        "Полный анализ выше.\n"
        "📎 Сразу после этой кнопки придёт файл со всеми дебатами — он не пропадёт при рестарте бота.",
        reply_markup=main_report_keyboard(
            user_id, has_debates=bool(debate_cache.get(user_id, {}).get("rounds")),
        ),
    )
    rounds_out = debate_cache.get(user_id, {}).get("rounds") or []
    if rounds_out:
        await asyncio.sleep(0.25)
        await send_debates_attachment(chat_id, rounds_out)


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
    if parts[1] == "noop" or parts[2] == "noop":
        await callback.answer()
        return
    try:
        kb_uid = int(parts[1])
        round_idx = int(parts[2])
    except ValueError:
        await callback.answer()
        return
    if kb_uid != callback.from_user.id:
        await callback.answer("Кнопка не с твоего аккаунта", show_alert=True)
        return
    await handle_debate_navigation_callback(callback, callback.from_user.id, round_idx)


# ─── /start ───────────────────────────────────────────────────────────────────

@dp.message(Command("start"))
async def cmd_start(message: Message):
    await upsert_user(
        message.from_user.id,
        message.from_user.username or "",
        message.from_user.first_name or ""
    )
    name = message.from_user.first_name or "трейдер"
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Портфель", callback_data="portfolio:menu:")],
        [InlineKeyboardButton(text="📈 Профиль", callback_data="cmd:profile"), InlineKeyboardButton(text="📋 Дайджест", callback_data="cmd:daily")],
        [InlineKeyboardButton(text="💰 Статус", callback_data="cmd:status"), InlineKeyboardButton(text="📊 Трек-рекорд", callback_data="cmd:trackrecord")],
    ])
    
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
        "• /trackrecord — история точности (всё)\n"
        "• /trackrecordglobal — Global прогнозы\n"
        "• /trackrecordrussia — Россия Edge 🇷🇺\n"
        "• /weeklyreport — отчёт за неделю\n"
        "• /subscribe — авторассылка\n"
        "• /markets — текущие цены\n"
        "• /status — краткий статус (можно закрепить)\n"
        "• /portfolio — твой портфель\n\n"
        "⚠️ _Не финансовый совет. Будущее неизвестно никому._",
        reply_markup=kb,
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

async def legacy_run_full_analysis(
    user_id: int,
    custom_news: str = "",
    custom_mode: bool = False
) -> tuple[str, dict]:
    tasks = [
        news_fetcher.fetch_all(),
        fetch_full_context(),
        get_full_realtime_context(),
        get_profile(user_id),
        get_meta_context(),
        get_previous_digest(),
    ]

    news, geo_context, realtime_result, profile, meta_context, prev_digest = await asyncio.gather(
        *tasks, return_exceptions=True
    )

    if isinstance(prev_digest, Exception): prev_digest = ""

    if isinstance(realtime_result, Exception):
        prices_dict, live_prices = {}, ""
    elif isinstance(realtime_result, tuple) and len(realtime_result) == 2:
        prices_dict, live_prices = realtime_result
    else:
        prices_dict, live_prices = {}, ""

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

    # Добавляем прошлый прогноз для сравнения агентами
    if prev_digest and not custom_mode:
        news_context += f"\n\n{prev_digest}"
        logger.info("Прошлый анализ передан агентам для сравнения")

    sentiment_result, confidence_instruction = await analyze_and_filter_async(
        news_context, str(live_prices)
    )
    sentiment_block = format_for_agents(sentiment_result, confidence_instruction)

    logger.info(
        f"Sentiment: {sentiment_result.label} | "
        f"Confidence: {sentiment_result.confidence} | "
        f"Score: {sentiment_result.score:+.2f}"
    )

    prices_dict = dict(prices_dict) if prices_dict else {}
    prices_dict["SENTIMENT"] = {
        "score": sentiment_result.score,
        "label": sentiment_result.label,
        "confidence": sentiment_result.confidence,
    }

    orchestrator = DebateOrchestrator()
    report = await orchestrator.run_debate(
        news_context=news_context,
        live_prices=live_prices,
        profile_instruction=profile_instruction + sentiment_block,
        custom_mode=custom_mode
    )
    report, _san_lines = sanitize_full_report(report)
    if _san_lines:
        logger.info("Пост-фильтр полного отчёта: удалено строк: %s", _san_lines)

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
        f"📶 *Уровень сигнала:* {stars} ({pct}% — уверенность FinBERT в тоне новостей)\n"
        f"_Не направление рынка; расшифровка — в шапке дайджеста._\n\n"
    )
    report = report.replace(separator, separator + signal_line, 1)

    # ── Сохраняем прогнозы ────────────────────────────────────────────────────
    source = custom_news[:300] if custom_mode else str(news)[:300]
    await save_predictions_from_report(report, source_news=source, bot=get_bot(), admin_ids=ADMIN_IDS)
    await log_report(
        user_id,
        "analyze" if custom_mode else "daily",
        source,
        report[:500]
    )

    if not custom_mode:
        storage.cache_report(report, prices_dict, owner_user_id=user_id)
        if scheduler is not None:
            asyncio.create_task(scheduler.export_now())
        # Кэшируем дайджест на GitHub для отслеживания точности (п.6)
        try:
            date_str = datetime.now().strftime("%d.%m.%Y %H:%M")
            asyncio.create_task(push_digest_cache(report, date_str))
        except Exception as e:
            logger.warning(f"Digest cache error: {e}")

    return report, prices_dict


# ─── /daily ───────────────────────────────────────────────────────────────────

async def run_daily_analysis(user_id: int) -> str:
    report, _ = await analysis_service_run_full_analysis(user_id)
    return report


async def deliver_scheduled_daily(user_id: int) -> None:
    """Рассылка подписчикам: как /daily — сначала общий кэш (без токенов), иначе полный прогон."""
    try:
        cached = storage.get_cached_report()
        if cached:
            report = cached["report"]
            prices = cached.get("prices") or {}
            await send_daily_digest_bundle(user_id, user_id, report, prices)
            return
        report, prices = await analysis_service_run_full_analysis(user_id)
        await send_daily_digest_bundle(user_id, user_id, report, prices)
    except Exception as e:
        logger.warning("Рассылка дайджеста user %s: %s", user_id, e)


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

    text_parts = (message.text or "").split(maxsplit=1)
    force_fresh = (
        len(text_parts) > 1
        and text_parts[1].strip().lower() in ("force", "fresh", "новый", "new")
    )

    cached = None if force_fresh else storage.get_cached_report()
    if cached:
        report = cached["report"]
        prices = cached.get("prices") or {}
        await send_daily_digest_bundle(message.chat.id, user_id, report, prices)
        await message.answer(
            f"Кэш от {cached['timestamp']}. Повтор без AI до ~{CACHE_TTL_HOURS} ч. "
            f"Сброс: `/daily force`",
            parse_mode="Markdown",
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
        report, prices = await analysis_service_run_full_analysis(user_id)
        try:
            await bot.delete_message(chat_id=message.chat.id, message_id=wait_msg.message_id)
        except Exception:
            pass  # сообщение уже удалено или недоступно — не критично
        await send_daily_digest_bundle(message.chat.id, user_id, report, prices)

    except Exception as e:
        logger.error(f"Daily error: {e}", exc_info=True)
        try:
            await bot.edit_message_text(
                f"❌ *Ошибка:* `{str(e)[:200]}`\n\n"
                "Проверь: API ключи, интернет, BOT_TOKEN.",
                chat_id=message.chat.id,
                message_id=wait_msg.message_id,
                parse_mode="Markdown"
            )
        except Exception:
            await message.answer(
                f"❌ *Ошибка:* `{str(e)[:200]}`\n\nПроверь: API ключи, интернет, BOT_TOKEN.",
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
        report, prices = await analysis_service_run_full_analysis(
            user_id, custom_news=user_news, custom_mode=True
        )
        try:
            await bot.delete_message(chat_id=message.chat.id, message_id=wait_msg.message_id)
        except Exception:
            pass  # сообщение уже удалено — не критично
        await send_daily_digest_bundle(message.chat.id, user_id, report, prices)

    except Exception as e:
        logger.error(f"Analyze error: {e}", exc_info=True)
        try:
            await bot.edit_message_text(
                f"❌ *Ошибка:* `{str(e)[:200]}`",
                chat_id=message.chat.id,
                message_id=wait_msg.message_id,
                parse_mode="Markdown"
            )
        except Exception:
            await message.answer(f"❌ *Ошибка:* `{str(e)[:200]}`", parse_mode="Markdown")



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
        await send_russia_chart_photo(message.chat.id, cached_ru)
        for chunk in split_message(cached_ru):
            await message.answer(chunk, parse_mode="Markdown")
        await message.answer(
            f"📦 _Кэш от {russia_cache['timestamp']}. Новый через 2ч._",
            parse_mode="Markdown",
            reply_markup=feedback_keyboard("russia")
        )
        return

    # Нужен глобальный анализ как основа (last_report + fallback на отчёт этого user_id с /daily)
    cached = storage.get_cached_report()
    global_report = ""
    if cached and isinstance(cached.get("report"), str):
        global_report = cached["report"]
    if not global_report.strip():
        ur = storage.get_user_last_cached_report(user_id)
        if isinstance(ur, str) and ur.strip():
            global_report = ur

    # Если нет актуального дайджеста — предлагаем выбор
    if not global_report.strip():
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

        # Санитайзер для russia — убирает галлюцинации (ставки банков и тд)
        report, _san_lines_ru = sanitize_full_report(report)
        if _san_lines_ru:
            logger.info("Russia пост-фильтр: удалено строк: %d", _san_lines_ru)

        # Кэшируем
        from datetime import datetime
        import time
        russia_cache["report"]    = report
        russia_cache["timestamp"] = datetime.now().strftime("%d.%m.%Y %H:%M")
        russia_cache["ts"]        = time.time()

        try:
            await bot.delete_message(chat_id=message.chat.id, message_id=wait_msg.message_id)
        except Exception:
            pass  # сообщение уже удалено — не критично

        await send_russia_chart_photo(message.chat.id, report)
        
        # Парсим секции для навигации (пробуем разные разделители)
        opportunities = ""
        risks = ""
        synthesis = ""
        
        # Пробуем разные разделители
        for sep in ["─" * 30, "---", "___"]:
            sections = report.split(sep)
            if len(sections) >= 4:
                opportunities = sections[1].strip() if len(sections) > 1 else ""
                risks = sections[2].strip() if len(sections) > 2 else ""
                synthesis = sections[3].strip() if len(sections) > 3 else ""
                break
        
        # Если не получилось парсить — сохраняем весь отчёт
        if not opportunities and not risks:
            opportunities = "Раздел возможностей"
            risks = "Раздел рисков"
            synthesis = synthesis if synthesis else "Раздел итогов"
        
        # Сохраняем секции в кэш для навигации
        russia_cache["sections"] = {
            "opportunities": opportunities,
            "risks": risks,
            "synthesis": synthesis
        }
        
        # Клавиатура навигации
        nav_keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="🟢 Возможности", callback_data="russia_nav:opp"),
                InlineKeyboardButton(text="🔴 Риски", callback_data="russia_nav:risk"),
            ],
            [
                InlineKeyboardButton(text="⚖️ Итог", callback_data="russia_nav:synth"),
                InlineKeyboardButton(text="📊 Полный", callback_data="russia_nav:full"),
            ]
        ])
        
        for chunk in split_message(report):
            await message.answer(clean_markdown(chunk), parse_mode="Markdown")

        await message.answer(
            "💬 *Был ли анализ полезным?*",
            parse_mode="Markdown",
            reply_markup=feedback_keyboard("russia")
        )
        
        await message.answer(
            "📍 *Навигация по разделам:*",
            parse_mode="Markdown",
            reply_markup=nav_keyboard
        )

    except Exception as e:
        logger.error(f"Russia error: {e}", exc_info=True)
        try:
            await bot.edit_message_text(
                f"❌ *Ошибка:* `{str(e)[:200]}`",
                chat_id=message.chat.id,
                message_id=wait_msg.message_id,
                parse_mode="Markdown"
            )
        except Exception:
            await message.answer(f"❌ *Ошибка:* `{str(e)[:200]}`", parse_mode="Markdown")



# ─── Выбор перед /russia ──────────────────────────────────────────────────────

@dp.callback_query(F.data.startswith("russia_nav:"))
async def handle_russia_nav(callback: CallbackQuery):
    await callback.answer()
    data = callback.data.split(":")
    section = data[1] if len(data) > 1 else "full"
    
    # Проверяем есть ли кэш
    if not russia_cache.get("report"):
        await callback.message.answer(
            "⚠️ Нет сохранённого отчёта.\nЗапусти /russia сначала!",
            parse_mode="Markdown"
        )
        return
    
    sections = russia_cache.get("sections", {})
    full_report = russia_cache.get("report", "")
    
    text = ""
    if section == "opp":
        text = sections.get("opportunities", "Раздел не найден. Запусти /russia заново.")
    elif section == "risk":
        text = sections.get("risks", "Раздел не найден. Запусти /russia заново.")
    elif section == "synth":
        text = sections.get("synthesis", "Раздел не найден. Запусти /russia заново.")
    elif section == "full":
        text = full_report[:3500] if full_report else "Отчёт не найден. Запусти /russia заново."
    else:
        text = "Выбери раздел:"
    
    nav_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🟢 Возможности", callback_data="russia_nav:opp"),
            InlineKeyboardButton(text="🔴 Риски", callback_data="russia_nav:risk"),
        ],
        [
            InlineKeyboardButton(text="⚖️ Итог", callback_data="russia_nav:synth"),
            InlineKeyboardButton(text="📊 Полный", callback_data="russia_nav:full"),
        ]
    ])
    
    await callback.message.answer(
        f"📍 *Раздел:* {section.upper()}\n\n{text[:3500]}",
        parse_mode="Markdown",
        reply_markup=nav_keyboard
    )


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

        try:
            await bot.delete_message(
                chat_id=callback.message.chat.id,
                message_id=wait_msg.message_id
            )
        except Exception:
            pass  # сообщение уже удалено — не критично

        await send_russia_chart_photo(callback.message.chat.id, report)
        for chunk in split_message(report):
            await callback.message.answer(clean_markdown(chunk), parse_mode="Markdown")

        await callback.message.answer(
            "💬 *Был ли анализ полезным?*",
            parse_mode="Markdown",
            reply_markup=feedback_keyboard("russia")
        )

    except Exception as e:
        logger.error(f"Russia choice error: {e}", exc_info=True)
        try:
            await bot.edit_message_text(
                f"❌ *Ошибка:* `{str(e)[:200]}`",
                chat_id=callback.message.chat.id,
                message_id=wait_msg.message_id,
                parse_mode="Markdown"
            )
        except Exception:
            await callback.message.answer(f"❌ *Ошибка:* `{str(e)[:200]}`", parse_mode="Markdown")


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


@dp.message(Command("status"))
async def cmd_status(message: Message):
    await upsert_user(message.from_user.id)
    wait_msg = await message.answer("⏳ Загружаю...")
    try:
        prices, _ = await get_full_realtime_context()
        cbr_data = await fetch_cbr_data()
        
        now = datetime.now().strftime("%d.%m %H:%M UTC")
        
        lines = [
            f"📊 СТАТУС РЫНКОВ",
            f"_{now}_",
            ""
        ]
        
        # Крипта
        lines.append("💰 КРИПТА")
        for k, label, icon in [
            ("BTC", "Bitcoin", "₿"),
            ("ETH", "Ethereum", "Ξ"),
        ]:
            if k in prices:
                p = prices[k]
                price = p.get("price", 0)
                change = p.get("change_24h", 0)
                emoji = "🟢" if change >= 0 else "🔴"
                lines.append(f"{icon} {label}: ${price:,.0f} {emoji}{change:+.1f}%")
        
        # Валюты
        if cbr_data:
            lines.append("")
            lines.append("💵 ВАЛЮТЫ (ЦБ РФ)")
            for line in cbr_data.strip().split('\n')[:3]:
                if line.strip():
                    lines.append(line)
        
        # Фондовые
        lines.append("")
        lines.append("📈 ИНДЕКСЫ")
        for k, label in [("SPX", "S&P"), ("NDX", "Nasdaq"), ("VIX", "VIX")]:
            if k in prices:
                p = prices[k]
                price = p.get("price", 0)
                change = p.get("change_24h", 0)
                emoji = "🟢" if change >= 0 else "🔴"
                lines.append(f"{label}: {price:,.0f} {emoji}{change:+.1f}%")
        
        # Макро
        if "MACRO" in prices:
            m = prices["MACRO"]
            fng = m.get("fng", {})
            fv = fng.get("val", "N/A")
            fs = fng.get("status", "")
            lines.append("")
            lines.append(f"F&Greed: {fv}/100 ({fs})")
        
        lines.append("")
        lines.append("⚠️ Не финансовый совет")
        
        await bot.edit_message_text(
            "\n".join(lines),
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


@dp.message(Command("signals"))
async def cmd_signals(message: Message):
    from signals import fetch_binance_signals, fetch_verdict, analyze_signals, build_signals_message
    import os
    
    user_id = message.from_user.id
    await upsert_user(user_id)
    wait_msg = await message.answer("⏳ Загружаю сигналы...")
    
    try:
        github_repo = os.getenv("GITHUB_REPO", "borzenkovandrej07-alt/DIALECTIC_EDg")
        
        binance_data = await fetch_binance_signals()
        verdict = await fetch_verdict(github_repo)
        signals = analyze_signals(binance_data, verdict)
        msg = build_signals_message(signals, binance_data, verdict)
        
        is_enabled = await get_user_signals_status(user_id)
        
        if is_enabled:
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔕 Выключить сигналы", callback_data="signals:disable")],
                [InlineKeyboardButton(text="📡 Проверить сейчас", callback_data="signals:check")],
                [InlineKeyboardButton(text="📊 Бэктест", callback_data="signals:backtest")],
            ])
            status_text = "\n\n✅ *Сигналы включены* — буду присылать когда появится сигнал"
        else:
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔔 Включить сигналы", callback_data="signals:enable")],
                [InlineKeyboardButton(text="📡 Проверить сейчас", callback_data="signals:check")],
                [InlineKeyboardButton(text="📊 Бэктест", callback_data="signals:backtest")],
            ])
            status_text = "\n\n━━━━━━━━━━━━━━━━━━━━━\nНажми 'Включить сигналы' — бот будет сам присылать когда:\n• 80%+ трейдеров в одну сторону\n• или 60%+ совпадение с вердиктом"
        
        await bot.edit_message_text(
            msg + status_text,
            chat_id=message.chat.id,
            message_id=wait_msg.message_id,
            parse_mode="Markdown",
            reply_markup=keyboard
        )
    except Exception as e:
        await bot.edit_message_text(
            f"❌ Ошибка: {e}",
            chat_id=message.chat.id,
            message_id=wait_msg.message_id
        )


@dp.callback_query(F.data.startswith("signals:"))
async def cb_signals(callback: CallbackQuery):
    user_id = callback.from_user.id
    action = callback.data.split(":")[1] if ":" in callback.data else ""
    
    if action == "enable":
        await set_signals_sub(user_id, True)
        await callback.answer("✅ Сигналы включены! Буду присылать когда появится сигнал.", show_alert=True)
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔕 Выключить сигналы", callback_data="signals:disable")],
            [InlineKeyboardButton(text="📡 Проверить сейчас", callback_data="signals:check")],
            [InlineKeyboardButton(text="📊 Бэктест", callback_data="signals:backtest")],
        ])
        
        await callback.message.edit_text(
            callback.message.text + "\n\n✅ *Сигналы включены!*",
            parse_mode="Markdown",
            reply_markup=keyboard
        )
    
    elif action == "disable":
        await set_signals_sub(user_id, False)
        await callback.answer("🔕 Сигналы выключены.", show_alert=True)
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔔 Включить сигналы", callback_data="signals:enable")],
            [InlineKeyboardButton(text="📡 Проверить сейчас", callback_data="signals:check")],
            [InlineKeyboardButton(text="📊 Бэктест", callback_data="signals:backtest")],
        ])
        
        await callback.message.edit_text(
            callback.message.text.replace("✅ *Сигналы включены!*", ""),
            parse_mode="Markdown",
            reply_markup=keyboard
        )
    
    elif action == "check":
        await callback.answer("📡 Проверяю...")
        
        import os
        github_repo = os.getenv("GITHUB_REPO", "borzenkovandrej07-alt/DIALECTIC_EDg")
        from signals import fetch_binance_signals, fetch_verdict, analyze_signals, build_signals_message
        
        binance_data = await fetch_binance_signals()
        verdict = await fetch_verdict(github_repo)
        signals = analyze_signals(binance_data, verdict)
        msg = build_signals_message(signals, binance_data, verdict)
        
        is_enabled = await get_user_signals_status(user_id)
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔕 Выключить сигналы" if is_enabled else "🔔 Включить сигналы", 
                                 callback_data="signals:disable" if is_enabled else "signals:enable")],
            [InlineKeyboardButton(text="📡 Обновить", callback_data="signals:check")],
            [InlineKeyboardButton(text="📊 Бэктест", callback_data="signals:backtest")],
        ])
        
        status_text = "\n\n✅ *Сигналы включены*" if is_enabled else ""
        
        await callback.message.edit_text(
            msg + status_text,
            parse_mode="Markdown",
            reply_markup=keyboard
        )
    
    elif action == "backtest":
        signals_data = await get_backtest_signals()
        stats = await get_backtest_stats()
        
        total = stats.get("total", 0) or 0
        wins = stats.get("wins", 0) or 0
        losses = stats.get("losses", 0) or 0
        total_pnl = stats.get("total_pnl", 0) or 0
        avg_pnl = stats.get("avg_pnl_pct", 0) or 0
        win_rate = (wins / total * 100) if total > 0 else 0
        
        msg = f"📊 *БЭКТЕСТ РЕЗУЛЬТАТЫ*\n\n"
        msg += f"Всего сделок: {total}\n"
        msg += f"Win Rate: {win_rate:.1f}%\n"
        msg += f"Total PnL: ${total_pnl:+,.2f}\n"
        msg += f"Avg PnL: {avg_pnl:+.2f}%\n\n"
        msg += "Последние сделки:\n"
        
        for s in signals_data[:5]:
            symbol = s["symbol"]
            direction = s["direction"]
            pnl = s.get("pnl", 0) or 0
            emoji = "🟢" if pnl > 0 else "🔴" if pnl < 0 else "⚪"
            msg += f"{symbol} {direction} {emoji} ${pnl:+,.0f}\n"
        
        await callback.message.edit_text(msg, parse_mode="Markdown")
        await callback.message.edit_text(
            msg + status_text,
            parse_mode="Markdown",
            reply_markup=keyboard
        )
    else:
        await callback.answer()


# ─── /trackrecord ─────────────────────────────────────────────────────────────

@dp.message(Command("market"))
async def cmd_market(message: Message):
    user_id = message.from_user.id
    await upsert_user(user_id, message.from_user.username or "")
    if not await check_limit(user_id):
        await message.answer(
            f"в›” *Р›РёРјРёС‚* вЂ” {FREE_DAILY_LIMIT} Р·Р°РїСЂРѕСЃРѕРІ/РґРµРЅСЊ (free)",
            parse_mode="Markdown"
        )
        return
    await increment_requests(user_id)
    await handle_market_command(message, message.text or "/market")


async def _cmd_trackrecord(message: Message, report_type: str = None, title: str = "АГЕНТОВ", filter_type: str = "all"):
    await upsert_user(message.from_user.id)
    try:
        import aiohttp
        import re

        content = None
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    "https://raw.githubusercontent.com/borzenkovandrej07-alt/DIALECTIC_EDg/main/FORECASTS.md",
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    if resp.status == 200:
                        content = await resp.text()
        except Exception as e:
            logger.warning(f"Failed to fetch FORECASTS.md: {e}")
        
        if not content:
            await message.answer("📊 Не удалось загрузить FORECASTS.md")
            return

        russia_keywords = ["руб", "рф", "россия", "сбер", "газпром", "лукойл", "роснефть", "мосбирж", "рбк", "офз", "usd/rub", "нефть"]

        last_update_match = re.search(r'Последнее обновление:\s*(\d{2}\.\d{2}\.\d{4})', content)
        last_update = last_update_match.group(1) if last_update_match else "—"

        total = 0
        wins = 0
        cautions = 0
        losses = 0
        winrate = 0
        winrate_conservative = 0
        protection = 0
        period = ""

        total_match = re.search(r'Всего прогнозов.*?\|.*?(\d+)', content)
        if total_match:
            total = int(total_match.group(1))
        
        wins_match = re.search(r'✅ Верно.*?\|.*?(\d+)', content)
        if wins_match:
            wins = int(wins_match.group(1))
        
        cautions_match = re.search(r'⚠️ Правильная осторожность.*?\|.*?(\d+)', content)
        if cautions_match:
            cautions = int(cautions_match.group(1))
        
        losses_match = re.search(r'❌ Неверно.*?\|.*?(\d+)', content)
        if losses_match:
            losses = int(losses_match.group(1))
        
        winrate_match = re.search(r'Точность \(с осторожностью\).*?\*\*(\d+\.?\d*)%', content)
        if winrate_match:
            winrate = float(winrate_match.group(1))
        
        winrate_conservative_match = re.search(r'Точность \(только направление\).*?\*\*(\d+\.?\d*)%', content)
        if winrate_conservative_match:
            winrate_conservative = float(winrate_conservative_match.group(1))
        
        protection_match = re.search(r'Защита капитала.*?\*\*(\d+\.?\d*)%', content)
        if protection_match:
            protection = float(protection_match.group(1))
        
        period_match = re.search(r'Период.*?(\d{2}\.\d{2}\.\d{4}.*\d{2}\.\d{2}\.\d{4})', content)
        if period_match:
            period = period_match.group(1)

        categories = []
        in_categories = False
        for line in content.split('\n'):
            if '## 📋 Точность по категориям' in line:
                in_categories = True
                continue
            if in_categories and line.strip().startswith('|') and '---' not in line and 'Категория' not in line:
                parts = [p.strip() for p in line.split('|')[1:-1]]
                if len(parts) >= 3:
                    categories.append({"name": parts[0], "stats": parts[1], "accuracy": parts[2]})
            elif in_categories and (line.strip().startswith('##') or line.strip() == ''):
                if len(categories) > 0:
                    break

        predictions = []
        
        russia_keywords = ["руб", "рф", "россия", "сбер", "газпром", "лукойл", "роснефть", "мосбирж", "офз", "нефть", "росси"]
        
        in_forecasts = False
        for line in content.split('\n'):
            if '## 📝 Все прогнозы' in line:
                in_forecasts = True
                continue
            if in_forecasts and line.strip().startswith('|') and '---' not in line:
                if '№' in line or 'Дата' in line:
                    continue
                parts = [p.strip() for p in line.split('|')[1:-1]]
                if len(parts) >= 7:
                    try:
                        date = parts[1] if len(parts) > 1 else ""
                        pred_type = parts[2] if len(parts) > 2 else ""
                        asset = parts[3] if len(parts) > 3 else ""
                        forecast = parts[4] if len(parts) > 4 else ""
                        fact = parts[5] if len(parts) > 5 else ""
                        result = parts[6] if len(parts) > 6 else ""
                        
                        is_russia = "Russia" in pred_type or any(kw in asset.lower() for kw in russia_keywords)
                        
                        # Фильтрация по типу
                        if report_type == "global" and is_russia:
                            continue
                        if report_type == "russia" and not is_russia:
                            continue
                        
                        predictions.append({
                            "date": date,
                            "type": pred_type,
                            "asset": asset,
                            "forecast": forecast[:30],
                            "fact": fact[:30],
                            "result": result,
                            "is_russia": is_russia
                        })
                    except:
                        pass
            if in_forecasts and line.strip().startswith('##') and 'Все прогнозы' not in line:
                break
        
        # Парсим статы из таблицы
        total_match = re.search(r'Всего прогнозов.*?(\d+)', content)
        if total_match:
            total = int(total_match.group(1))
        
        wins_match = re.search(r'Прибыльных.*?(\d+)', content)
        if wins_match:
            wins = int(wins_match.group(1))
        
        losses_match = re.search(r'Убыточных.*?(\d+)', content)
        if losses_match:
            losses = int(losses_match.group(1))
        
        # Фильтрация по типу
        if filter_type and filter_type != "all":
            filtered = []
            for p in predictions:
                result = p["result"]
                if filter_type == "win" and ("Верно" in result or "✅" in result):
                    filtered.append(p)
                elif filter_type == "loss" and ("Неверно" in result or "❌" in result):
                    filtered.append(p)
                elif filter_type == "caution" and ("Осторожность" in result or "⚠️" in result):
                    filtered.append(p)
            predictions = filtered

        # Считаем статистику из отфильтрованных прогнозов
        wins = sum(1 for p in predictions if "Верно" in p["result"] or "✅" in p["result"])
        cautions = sum(1 for p in predictions if "Осторожность" in p["result"] or "⚠️" in p["result"])
        losses = sum(1 for p in predictions if "Неверно" in p["result"] or "❌" in p["result"])
        total = wins + cautions + losses

        if total == 0:
            await message.answer(
                "📊 TRACK RECORD\n\nПрогнозов не найдено с таким фильтром.",
                parse_mode="Markdown"
            )
            return

        icon = "🌍" if report_type == "global" else "🇷🇺" if report_type == "russia" else "📊"
        
        filter_label = ""
        if filter_type and filter_type != "all":
            filter_label = f" [{filter_type.upper()}]"
        
        def make_bar(value: int, total: int, length: int = 10) -> str:
            if total == 0:
                return "░" * length
            pct = value / total
            filled = int(pct * length)
            return "█" * filled + "░" * (length - filled)

        finished = total
        lines = [
            f"{icon} 📊 DIALECTIC EDGE — TRACK RECORD{filter_label}",
            f"_{period}_" if period else f"_{last_update}_",
            "",
            "═" * 40,
            "🎯 ОБЩАЯ СТАТИСТИКА",
            "═" * 40,
        ]

        if finished > 0:
            win_bar = make_bar(wins, finished)
            loss_bar = make_bar(losses, finished)
            caution_bar = make_bar(cautions, finished)
            lines.extend([
                f"✅ WIN   [{win_bar}] {wins}/{finished} ({wins*100//finished}%)",
                f"⚠️ CAUT  [{caution_bar}] {cautions}/{finished} ({cautions*100//finished}%)",
                f"❌ LOSS  [{loss_bar}] {losses}/{finished} ({losses*100//finished}%)",
            ])

        # Точность только из отфильтрованных
        if finished > 0:
            winrate_calc = wins / finished * 100
            wr_emoji = "🟢" if winrate_calc >= 55 else "🟡" if winrate_calc >= 45 else "🔴"
            lines.append(f"Точность: {wr_emoji} {winrate_calc:.1f}%")

        # Категории показываем только без фильтра
        if categories and (not filter_type or filter_type == "all"):
            lines.append("")
            lines.append("📈 КАТЕГОРИИ")
            for cat in categories[:6]:
                lines.append(f"  {cat['name']}: {cat['accuracy']}")

        lines.append("")
        lines.append("📝 ПРОГНОЗЫ")
        
        for p in predictions:
            date = p.get("date", "")[:8]
            asset = p.get("asset", "")[:15]
            forecast = p.get("forecast", "")[:30]
            result = p.get("result", "")
            fact = p.get("fact", "")[:30]
            
            if "Верно" in result:
                res_emoji = "✅"
            elif "Неверно" in result:
                res_emoji = "❌"
            elif "Осторожность" in result:
                res_emoji = "⚠️"
            else:
                res_emoji = "⏳"
            
            # Для LOSS/CAUTION показываем больше инфы
            if filter_type and filter_type != "all" and fact:
                lines.append(f"{res_emoji} {date} {asset}")
                lines.append(f"   Прогноз: {forecast}")
                lines.append(f"   Факт:    {fact}")
            else:
                lines.append(f"{res_emoji} {date} {asset:<15} {forecast:<30}")

        lines.append("")
        lines.append("⚠️ Прошлые результаты не гарантируют будущих.")

        keyboard_buttons = []
        
        type_label = {"global": "GLOBAL", "russia": "РОССИЯ", None: "ВСЕ"}.get(report_type, "ВСЕ")
        
        keyboard_buttons.append([
            InlineKeyboardButton(text="🌍 Global", callback_data=f"tr_type:global"),
            InlineKeyboardButton(text="🇷🇺 Россия", callback_data=f"tr_type:russia"),
            InlineKeyboardButton(text="📊 Все", callback_data=f"tr_type:all"),
        ])
        
        keyboard_buttons.append([
            InlineKeyboardButton(text="✅ WIN", callback_data=f"tr_filter:win:{type_label}"),
            InlineKeyboardButton(text="❌ LOSS", callback_data=f"tr_filter:loss:{type_label}"),
            InlineKeyboardButton(text="⚠️ CAUTION", callback_data=f"tr_filter:caution:{type_label}"),
            InlineKeyboardButton(text="📋 Все", callback_data=f"tr_filter:all:{type_label}"),
        ])

        keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
        
        full_text = "\n".join(lines)
        
        if len(full_text) > 4000:
            part1 = "\n".join(lines[:40])
            part2 = "\n".join(lines[40:])
            await message.answer(part1, parse_mode="Markdown")
            await message.answer(part2, parse_mode="Markdown", reply_markup=keyboard)
        else:
            await message.answer(full_text, parse_mode="Markdown", reply_markup=keyboard)

    except Exception as e:
        logger.error(f"Trackrecord error: {e}", exc_info=True)
        await message.answer(f"❌ Ошибка: {e}")


@dp.callback_query(F.data.startswith("tr_type:"))
async def cb_tr_type(callback: CallbackQuery):
    await callback.answer()
    data = callback.data.split(":")
    report_type = data[1] if len(data) > 1 and data[1] != "all" else None
    title = "GLOBAL" if report_type == "global" else "РОССИЯ" if report_type == "russia" else "АГЕНТОВ"
    await _cmd_trackrecord(callback.message, report_type=report_type, title=title)


@dp.callback_query(F.data.startswith("tr_filter:"))
async def cb_tr_filter(callback: CallbackQuery):
    await callback.answer()
    data = callback.data.split(":")
    if len(data) < 3:
        return
    
    filter_type = data[1]
    type_label = data[2]
    
    report_type = "global" if type_label == "GLOBAL" else "russia" if type_label == "РОССИЯ" else None
    
    await _cmd_trackrecord(callback.message, report_type=report_type, title=f"{type_label} ({filter_type.upper()})", filter_type=filter_type)


@dp.message(Command("trackrecord"))
async def cmd_trackrecord(message: Message):
    await _cmd_trackrecord(message, report_type=None, title="АГЕНТОВ (ВСЕ)")


@dp.message(Command("trackrecordglobal"))
async def cmd_trackrecord_global(message: Message):
    await _cmd_trackrecord(message, report_type="global", title="GLOBAL")


@dp.message(Command("trackrecordrussia"))
async def cmd_trackrecord_russia(message: Message):
    await _cmd_trackrecord(message, report_type="russia", title="РОССИЯ EDGE")


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
from datetime import datetime

@dp.message(Command("subscribe"))
async def cmd_subscribe(message: Message):
    user_id   = message.from_user.id
    await upsert_user(user_id)
    user      = await get_user(user_id)
    is_subbed = user.get("daily_sub", 0) if user else 0
    sub_time  = user.get("sub_time", "08:00") if user else "08:00"
    
    from datetime import datetime
    current_utc = datetime.utcnow().strftime("%H:%M UTC")

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🌅 06:00 UTC", callback_data="sub_time:06:00"),
            InlineKeyboardButton(text="🌅 08:00 UTC", callback_data="sub_time:08:00"),
        ],
        [
            InlineKeyboardButton(text="☀️ 10:00 UTC", callback_data="sub_time:10:00"),
            InlineKeyboardButton(text="☀️ 12:00 UTC", callback_data="sub_time:12:00"),
        ],
        [
            InlineKeyboardButton(text="💬 Своё время", callback_data="sub_time:custom"),
        ],
        [
            InlineKeyboardButton(text="❌ Отключить", callback_data="sub_time:off"),
        ]
    ])

    if is_subbed:
        status = f"✅ Активна в {sub_time} UTC"
    else:
        status = "❌ Отключена"

    await message.answer(
        f"📬 *Авторассылка*\n"
        f"Статус: {status}\n\n"
        f"⏰ Сейчас: {current_utc}\n\n"
        f"🌍 *Важно:* Бот работает по UTC.\n"
        f"Если тебе нужно 10:00 МСК → выбирай 07:00 UTC\n"
        f"Если нужно 10:00 (Минск/Алматы) → выбирай 07:00-08:00 UTC\n\n"
        f"Выбери время:",
        parse_mode="Markdown",
        reply_markup=keyboard
    )


@dp.callback_query(F.data.startswith("sub_time:"))
async def cb_subscribe(callback: CallbackQuery):
    await callback.answer()
    user_id = callback.from_user.id
    data = callback.data.split(":")
    
    if len(data) < 2:
        return
    
    action = data[1]
    
    if action == "off":
        await set_daily_sub(user_id, False)
        await callback.message.edit_text(
            "❌ *Подписка отключена*",
            parse_mode="Markdown"
        )
        return
    
    if action == "custom":
        await callback.message.edit_text(
            "💬 *Введи время в формате HH:MM*\n\n"
            "Например: `09:30`\n\n"
            "Напоминаю: бот работает по UTC!",
            parse_mode="Markdown"
        )
        return
    
    time_str = action
    await set_daily_sub(user_id, True, time_str)
    
    await callback.message.edit_text(
        f"✅ *Подписка активана*\n\n"
        f"📬 Ежедневно в *{time_str} UTC*\n\n"
        f"❌ Отключить: нажми кнопку ниже",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отключить подписку", callback_data="sub_time:off")]
        ])
    )


@dp.message(F.text & ~F.text.startswith("/"))
async def handle_text_input(message: Message):
    """Handle portfolio input OR time subscription."""
    user_id = message.from_user.id
    text = message.text.strip()
    
    # Check portfolio state first
    state = user_portfolio_state.get(user_id)
    if state:
        if state["step"] == "amount":
            try:
                amount = float(text.replace(",", "."))
                assert amount > 0
                state["amount"] = amount
                state["step"] = "price"
                await message.answer(f"По какой цене купил {state['symbol']}?\nВведи цену (например 65000)")
            except:
                await message.answer("Введи число, например 0.5")
            return
        elif state["step"] == "price":
            try:
                price = float(text.replace(",", "."))
                assert price > 0
                symbol = state["symbol"]
                amount = state["amount"]
                await add_portfolio_position(user_id, symbol, amount, price)
                await message.answer(f"✅ Добавлено: {symbol} | {amount} шт. | ${price:,.0f}")
                del user_portfolio_state[user_id]
            except:
                await message.answer("Введи цену, например 65000")
            return
    
    # Check time input (for subscription)
    user = await get_user(user_id)
    if not user:
        return
    
    if ":" in text and len(text) == 5:
        try:
            h, m = text.split(":")
            h, m = int(h), int(m)
            assert 0 <= h <= 23 and 0 <= m <= 59
            time_str = f"{h:02d}:{m:02d}"
            await set_daily_sub(user_id, True, time_str)
            await message.answer(f"✅ Подписка активана\n📬 Ежедневно в {time_str} UTC")
            return
        except:
            pass
    
    # If not portfolio and not time, do nothing
    user_id = message.from_user.id
    user = await get_user(user_id)
    
    if not user:
        return
    
    text = message.text.strip()
    
    if ":" not in text or len(text) != 5:
        await message.answer("❌ Формат: HH:MM (например 09:30)")
        return
    
    try:
        h, m = text.split(":")
        h, m = int(h), int(m)
        assert 0 <= h <= 23 and 0 <= m <= 59
    except:
        await message.answer("❌ Некорректное время. Пример: 09:30")
        return
    
    time_str = f"{h:02d}:{m:02d}"
    await set_daily_sub(user_id, True, time_str)
    
    await message.answer(
        f"✅ *Подписка активана*\n\n"
        f"📬 Ежедневно в *{time_str} UTC*",
        parse_mode="Markdown"
    )


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
        f"• /trackrecord — история точности (всё)\n"
        f"• /trackrecordglobal — Global\n"
        f"• /trackrecordrussia — Россия Edge 🇷🇺\n"
        f"• /weeklyreport — отчёт за неделю\n"
        f"• /profile — изменить профиль",
        parse_mode="Markdown"
    )


# ─── /help ────────────────────────────────────────────────────────────────────

@dp.message(Command("help"))
async def cmd_help(message: Message):
    await upsert_user(message.from_user.id)
    await message.answer(
        "📖 *Dialectic Edge v7.1*\n\n"
        "*Что нового в v6:*\n"
        "• Один отчёт вместо 6 сообщений\n"
        "• Кнопка 📖 Полные дебаты — листай раунды\n"
        "• Простой язык в выводах\n"
        "• Умный Risk/Reward — если риск высокий, бот честно скажет 'ВНЕ РЫНКА'\n\n"
        "*Команды:*\n"
        "• `/profile` — настрой риск-профиль первым\n"
        "• `/daily` — дайджест (из кэша до суток без токенов)\n"
        "• `/daily force` — принудительно новый AI-прогон\n"
        "• `/analyze [текст]` — анализ новости\n"
        "• `/markets` — живые цены\n"
        "• `/trackrecord` — история точности (всё)\n"
        "• `/trackrecordglobal` — Global\n"
        "• `/trackrecordrussia` — Россия Edge 🇷🇺\n"
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
    await handle_stats_command(message)
    return
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

@dp.message(Command("health"))
async def cmd_health(message: Message):
    await handle_health_command(message)


@dp.message(Command("logs"))
async def cmd_logs(message: Message):
    await handle_logs_command(message)


@dp.message(Command("sysinfo"))
async def cmd_sysinfo(message: Message):
    await handle_sysinfo_command(message)


@dp.callback_query(F.data.startswith("fb:"))
async def handle_feedback(callback: CallbackQuery):
    _, rating_str, report_type = callback.data.split(":")
    await save_feedback(callback.from_user.id, report_type, int(rating_str))
    emoji = "🙏 Спасибо!" if int(rating_str) == 1 else "📝 Учтём!"
    await callback.answer(emoji)
    await callback.message.edit_reply_markup(reply_markup=None)


@dp.callback_query(F.data == "cmd_trackrecordglobal")
async def cb_trackrecord_global(callback: CallbackQuery):
    await callback.answer()
    await _cmd_trackrecord(callback.message, report_type="global", title="GLOBAL")


@dp.callback_query(F.data == "cmd_trackrecordrussia")
async def cb_trackrecord_russia(callback: CallbackQuery):
    await callback.answer()
    await _cmd_trackrecord(callback.message, report_type="russia", title="РОССИЯ EDGE")


@dp.callback_query(F.data == "cmd_trackrecord")
async def cb_trackrecord_all(callback: CallbackQuery):
    await callback.answer()
    await _cmd_trackrecord(callback.message, report_type=None, title="АГЕНТОВ (ВСЕ)")


# ─── Запуск ───────────────────────────────────────────────────────────────────

async def set_bot_commands(bot: Bot):
    from aiogram.types import BotCommand
    commands = [
        BotCommand(command="start", description="Перезапуск бота"),
        BotCommand(command="help", description="Справка"),
        BotCommand(command="daily", description="Дайджест рынков"),
        BotCommand(command="trackrecordglobal", description="🌍 Global прогнозы"),
        BotCommand(command="trackrecordrussia", description="🇷🇺 Россия Edge"),
        BotCommand(command="trackrecord", description="📊 Вся статистика"),
        BotCommand(command="markets", description="Текущие цены"),
        BotCommand(command="status", description="Краткий статус"),
        BotCommand(command="signals", description="📡 Сигналы копитрейдинг"),
        BotCommand(command="russia", description="Анализ РФ 🇷🇺"),
        BotCommand(command="profile", description="Настройки профиля"),
        BotCommand(command="subscribe", description="Авторассылка"),
    ]
    await bot.set_my_commands(commands)


async def main():
    global scheduler
    global bot
    bot = get_bot()
    
    await set_bot_commands(bot)

    await init_db()
    await import_forecasts_from_markdown()
    await init_profiles_table()
    setup_admins(ADMIN_IDS)
    logger.info("🚀 Dialectic Edge v7.1 starting...")
    if int(os.getenv("RAILWAY_REPLICA_COUNT", "1") or "1") > 1:
        logger.warning(
            "Railway: у сервиса бота >1 реплики — aiogram polling даёт TelegramConflictError. "
            "Scale → 1 или один процесс с BOT_TOKEN."
        )
    logger.info(
        "Подсказка: TelegramConflictError = второй процесс с тем же BOT_TOKEN "
        "(лишняя реплика Railway / локальный запуск)."
    )
    if USING_DATA_DIR:
        logger.info(
            "Постоянное хранилище: SQLite=%s | cache.json=%s",
            DB_PATH,
            CACHE_FILE,
        )
    if REDIS_URL.strip():
        if await ping_redis():
            logger.info(
                "Redis OK — полные дебаты переживут рестарт (TTL ≈ %s ч.)",
                DEBATE_SNAPSHOT_HOURS,
            )
        else:
            logger.warning(
                "REDIS_URL задан, но соединение не удалось — проверь Redis-плагин и что "
                "переменная подцеплена к сервису бота (Variables → shared / Reference)."
            )
    else:
        logger.warning(
            "REDIS_URL нет — после редеплоя кнопка «Полные дебаты» может быть пустой. "
            "Railway: New → Template → Redis ИЛИ + Database → Redis, затем в сервисе бота "
            "Variables → New Variable → Reference → Redis → REDIS_URL."
        )

    scheduler = Scheduler(
        bot=bot,
        send_daily_fn=deliver_scheduled_daily,
        check_predictions_fn=check_pending_predictions
    )

    # Start signal trader in background
    from signal_trader import run_signal_trader
    signal_trader_task = asyncio.create_task(run_signal_trader(bot, ADMIN_IDS))

    await asyncio.gather(
        dp.start_polling(bot),
        scheduler.start()
    )


# ─── Портфельный трекер ─────────────────────────────────────────────────────────

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

user_portfolio_state = {}  # user_id: {"symbol": str, "step": str}


def portfolio_keyboard(has_positions: bool = True) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text="➕ Добавить", callback_data="portfolio:add_select:")],
    ]
    if has_positions:
        buttons.append([InlineKeyboardButton(text="🗑 Удалить", callback_data="portfolio:remove_select:")])
    buttons.append([InlineKeyboardButton(text="🔄 Обновить", callback_data="portfolio:refresh:")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def select_crypto_keyboard() -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text="₿ Bitcoin", callback_data="portfolio:add_amount:BTC")],
        [InlineKeyboardButton(text="Ξ Ethereum", callback_data="portfolio:add_amount:ETH")],
        [InlineKeyboardButton(text="◎ Solana", callback_data="portfolio:add_amount:SOL")],
        [InlineKeyboardButton(text="🥇 Gold", callback_data="portfolio:add_amount:GOLD")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="portfolio:menu:")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


async def show_portfolio(event):
    """Show portfolio - works with both Message and CallbackQuery."""
    user_id = event.from_user.id
    
    positions = await get_portfolio(user_id)
    print(f"DEBUG: user_id={user_id}, positions={positions}")
    
    prices, _ = await get_full_realtime_context()
    
    symbol_map = {"BTC": "BTC", "ETH": "ETH", "SOL": "SOL", "GOLD": "GOLD"}
    
    lines = ["📊 ТВОЙ ПОРТФЕЛЬ", ""]
    total_pnl = 0
    total_value = 0
    
    for pos in positions:
        symbol = pos["symbol"]
        amount = pos["amount"]
        entry = pos["entry_price"]
        
        price_key = symbol_map.get(symbol, symbol)
        current_price = prices.get(price_key, {}).get("price", 0)
        
        if current_price:
            value = amount * current_price
            cost = amount * entry
            pnl = value - cost
            pnl_pct = (pnl / cost * 100) if cost > 0 else 0
            total_pnl += pnl
            total_value += value
            emoji = "🟢" if pnl >= 0 else "🔴"
            lines.append(f"{symbol}: {amount} x ${current_price:,.0f} = ${value:,.0f}")
            lines.append(f"  Вход: ${entry:,.0f} | PnL: {emoji}${pnl:+,.0f} ({pnl_pct:+.1f}%)")
        else:
            cost = amount * entry
            total_value += cost
            lines.append(f"{symbol}: {amount} x $??? | Вход: ${entry:,.0f}")
    
    if not positions:
        lines.append("Портфель пуст")
    
    if total_value > 0:
        total_cost = total_value - total_pnl if total_pnl > 0 else total_value
        total_pnl_pct = (total_pnl / total_cost * 100) if total_cost > 0 else 0
        emoji = "🟢" if total_pnl >= 0 else "🔴"
        lines.extend(["", f"📈 Итого: ${total_value:,.0f} | {emoji} {total_pnl:+,.0f} ({total_pnl_pct:+.1f}%)"])
    
    if hasattr(event, 'message'):
        await event.message.answer("\n".join(lines), reply_markup=portfolio_keyboard(bool(positions)))
    else:
        await event.answer("\n".join(lines), reply_markup=portfolio_keyboard(bool(positions)))


@dp.message(Command("portfolio"))
async def cmd_portfolio(message: Message):
    await upsert_user(message.from_user.id)
    await show_portfolio(message)


@dp.callback_query(F.data.startswith("portfolio:"))
async def handle_portfolio_callback(callback: CallbackQuery):
    user_id = callback.from_user.id
    parts = callback.data.split(":")
    action = parts[1] if len(parts) > 1 else ""
    symbol = parts[2] if len(parts) > 2 else ""
    
    await callback.answer()
    
    if action == "add_select":
        await callback.message.edit_text("Выбери криптовалюту:", reply_markup=select_crypto_keyboard())
    
    elif action == "add_amount":
        user_portfolio_state[user_id] = {"symbol": symbol, "step": "amount"}
        await callback.message.edit_text(f"Сколько {symbol} ты купил?\nВведи число (например 0.5)")
    
    elif action == "menu":
        await callback.message.delete()
        await show_portfolio(callback)
    
    elif action == "refresh":
        await callback.message.edit_text("⏳ Обновляю...")
        await show_portfolio(callback)
    
    elif action.startswith("cmd:"):
        cmd = action.replace("cmd:", "")
        await callback.message.delete()
        if cmd == "profile":
            await cmd_profile(callback.message)
        elif cmd == "daily":
            await cmd_daily(callback.message)
        elif cmd == "status":
            await cmd_status(callback.message)
        elif cmd == "trackrecord":
            await cmd_trackrecord(callback.message)
    
    elif action == "remove_select":
        positions = await get_portfolio(user_id)
        if not positions:
            await callback.message.edit_text("Нечего удалять!", reply_markup=portfolio_keyboard(False))
        else:
            buttons = []
            for pos in positions:
                s = pos["symbol"]
                a = pos["amount"]
                buttons.append([InlineKeyboardButton(
                    text=f"🗑 {s} ({a})",
                    callback_data=portfolio_cb.new(action="confirm_remove", symbol=s)
                )])
            buttons.append([InlineKeyboardButton(text="🔙 Назад", callback_data=portfolio_cb.new(action="menu", symbol=""))])
            await callback.message.edit_text("Что удалить?", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
    
    elif action == "confirm_remove":
        await remove_portfolio_position(user_id, symbol)
        await callback.message.edit_text(f"✅ {symbol} удалён из портфеля")
        await asyncio.sleep(1)
        await callback.message.delete()
        await cmd_portfolio(callback.message)


@dp.message(Command("add"))
async def cmd_add_portfolio(message: Message):
    """Add position to portfolio."""
    user_id = message.from_user.id
    await upsert_user(user_id)
    
    parts = message.text.split()
    
    if len(parts) != 4:
        await message.answer(
            "❌ Неверный формат.\n\n"
            "Пример: /add BTC 0.5 65000\n"
            "Формат: /add СИМВОЛ КОЛИЧЕСТВО ЦЕНА_ВХОДА"
        )
        return
    
    try:
        symbol = parts[1].upper()
        amount = float(parts[2])
        entry_price = float(parts[3])
    except ValueError:
        await message.answer("❌ Введите числа правильно.")
        return
    
    allowed = ["BTC", "ETH", "SOL", "GOLD"]
    if symbol not in allowed:
        await message.answer(f"❌ Пока только: {', '.join(allowed)}")
        return
    
    await add_portfolio_position(user_id, symbol, amount, entry_price)
    
    await message.answer(
        f"✅ Добавлено:\n{symbol} | {amount} шт. | Вход: ${entry_price:,.0f}"
    )


@dp.message(Command("remove"))
async def cmd_remove_portfolio(message: Message):
    """Remove position from portfolio."""
    user_id = message.from_user.id
    await upsert_user(user_id)
    
    parts = message.text.split()
    
    if len(parts) != 2:
        await message.answer("Пример: /remove BTC")
        return
    
    symbol = parts[1].upper()
    
    await remove_portfolio_position(user_id, symbol)
    
    await message.answer(f"✅ Удалено: {symbol}")


# ─── Backtest ───────────────────────────────────────────────────────────────────

backtest_enabled = True  # Global toggle for backtest recording


def backtest_keyboard(enabled: bool) -> InlineKeyboardMarkup:
    buttons = []
    if enabled:
        buttons.append([InlineKeyboardButton(text="⏸ Остановить", callback_data="bt:toggle")])
    else:
        buttons.append([InlineKeyboardButton(text="▶️ Запустить", callback_data="bt:toggle")])
    buttons.append([InlineKeyboardButton(text="📋 История сделок", callback_data="bt:history")])
    buttons.append([InlineKeyboardButton(text="💰 Изменить баланс", callback_data="bt:capital")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


@dp.message(Command("backtest"))
async def cmd_backtest(message: Message):
    """Show backtest results with nice formatting and keyboard."""
    signals = await get_backtest_signals()
    stats = await get_backtest_stats()
    config = await get_backtest_config()
    
    total = stats.get("total", 0) or 0
    wins = stats.get("wins", 0) or 0
    losses = stats.get("losses", 0) or 0
    total_pnl = stats.get("total_pnl", 0) or 0
    avg_pnl = stats.get("avg_pnl_pct", 0) or 0
    
    win_rate = (wins / total * 100) if total > 0 else 0
    
    capital = config.get("capital", 100.0)
    enabled = config.get("enabled", 1)
    
    msg = "🤖 *ТЕСТОВЫЙ ТРЕЙДЕР*\n"
    msg += "═" * 25 + "\n"
    msg += f"Это бот который торгует по сигналам анализа.\n"
    msg += f"Начинает с виртуального баланса и фармит $$$\n\n"
    msg += f"💵 *Баланс:* `${capital:,.2f}`\n"
    msg += f"📊 *Всего сделок:* {total}\n"
    msg += f"🎯 *Win Rate:* {win_rate:.1f}%\n"
    msg += f"💰 *Total PnL:* `${total_pnl:+,.2f}`\n"
    msg += f"📈 *Avg PnL:* {avg_pnl:+.2f}%\n"
    msg += "═" * 25 + "\n"
    
    open_positions = [s for s in signals if s.get("status") == "open"]
    if open_positions:
        msg += "\n🔵 *Открытые позиции:*\n"
        for s in open_positions:
            symbol = s["symbol"]
            direction = s["direction"]
            entry = s.get("entry_price", 0)
            emoji = "🟢" if direction == "BUY" else "🔴"
            dir_text = "📈 ЛОНГ" if direction == "BUY" else "📉 ШОРТ"
            msg += f"  {emoji} {symbol} {dir_text} @ ${entry:,.2f}\n"
    else:
        msg += "\n📭 *Нет открытых позиций*\n"
    
    closed = [s for s in signals if s.get("status") == "closed"]
    if closed:
        msg += "\n📋 *Последние сделки:*\n"
        for s in closed[:5]:
            symbol = s["symbol"]
            direction = s["direction"]
            pnl = s.get("pnl", 0) or 0
            pnl_pct = s.get("pnl_pct", 0) or 0
            emoji = "🟢" if pnl > 0 else "🔴"
            dir_text = "📈" if direction == "BUY" else "📉"
            msg += f"  {emoji} {symbol} {dir_text} ${pnl:+,.2f} ({pnl_pct:+.1f}%)\n"
    
    status_text = "✅ Работает" if enabled else "❌ Остановлен"
    msg += "═" * 25 + "\n"
    msg += f"Статус: {status_text}"
    
    await message.answer(
        msg, 
        parse_mode="Markdown",
        reply_markup=backtest_keyboard(bool(enabled))
    )
    
    # Also export to GitHub
    try:
        from github_export import export_backtest_to_github
        await export_backtest_to_github(signals, stats, config)
    except Exception as e:
        logger.warning(f"Backtest GitHub export failed: {e}")


@dp.message(Command("backtest_toggle"))
async def cmd_backtest_toggle(message: Message):
    """Toggle backtest recording using database."""
    config = await get_backtest_config()
    enabled = not bool(config.get("enabled", 1))
    await set_backtest_enabled(enabled)
    status = "включён" if enabled else "выключен"
    await message.answer(f"🤖 Бэктест {status}")


@dp.callback_query(F.data.startswith("bt:"))
async def cb_backtest(callback: CallbackQuery):
    """Handle backtest keyboard buttons."""
    action = callback.data.split(":")[1]
    user_id = callback.from_user.id
    
    if action == "toggle":
        config = await get_backtest_config()
        enabled = not bool(config.get("enabled", 1))
        await set_backtest_enabled(enabled)
        status = "✅ Работает" if enabled else "❌ Остановлен"
        await callback.message.edit_text(
            callback.message.text.split("Статус: ")[0] + f"Статус: {status}",
            parse_mode="Markdown",
            reply_markup=backtest_keyboard(bool(enabled))
        )
        await callback.answer(f"Бэктест {status}")
    
    elif action == "history":
        signals = await get_backtest_signals()
        closed = [s for s in signals if s.get("status") == "closed"]
        
        if not closed:
            await callback.answer("Нет закрытых сделок", show_alert=True)
            return
        
        msg = "📋 *История сделок*\n"
        msg += "═" * 25 + "\n"
        
        wins = 0
        losses = 0
        for s in closed:
            symbol = s["symbol"]
            direction = s["direction"]
            pnl = s.get("pnl", 0) or 0
            pnl_pct = s.get("pnl_pct", 0) or 0
            date = s.get("created_at", "")[:10]
            emoji = "🟢" if pnl > 0 else "🔴"
            if pnl > 0:
                wins += 1
            else:
                losses += 1
            dir_text = "📈" if direction == "BUY" else "📉"
            msg += f"{date} {emoji} {symbol} {dir_text} ${pnl:+,.2f} ({pnl_pct:+.1f}%)\n"
        
        msg += "═" * 25 + "\n"
        msg += f"Всего: {len(closed)} | 🟢 {wins} | 🔴 {losses}"
        
        await callback.message.answer(msg, parse_mode="Markdown")
        await callback.answer()
    
    elif action == "capital":
        await callback.message.answer(
            "💰 *Изменить баланс*\n\n"
            "Введите новую сумму:\n"
            "/backtest_capital 500\n"
            "или просто число",
            parse_mode="Markdown"
        )
        await callback.answer()
    
    else:
        await callback.answer()


@dp.message(Command("backtest_capital"))
async def cmd_backtest_capital(message: Message):
    """Set backtest capital."""
    try:
        parts = message.text.split()
        if len(parts) < 2:
            await message.answer("Использование: /backtest_capital [сумма]\nПример: /backtest_capital 500")
            return
        new_capital = float(parts[1].replace(",", ""))
        if new_capital <= 0:
            await message.answer("Сумма должна быть больше 0")
            return
        config = await update_backtest_capital(new_capital)
        await message.answer(f"💵 Капитал изменён на ${config['capital']:,.2f}")
    except ValueError:
        await message.answer("Неверная сумма. Пример: /backtest_capital 500")


@dp.message(Command("backtest_clear"))
async def cmd_backtest_clear(message: Message):
    """Clear backtest signals and reset capital."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM backtest_signals")
        await db.execute("UPDATE backtest_config SET capital = 100.0, last_updated = datetime('now') WHERE id = 1")
        await db.commit()
    await message.answer("🗑 Бэктест очищен, капитал сброшен до $100")


if __name__ == "__main__":
    asyncio.run(main())


# ─── Signal Trader Status ─────────────────────────────────────────────────────────

from signal_trader import get_signal_trader_status
from database import get_daily_context


@dp.message(Command("signal_status"))
async def cmd_signal_status(message: Message):
    """Check signal trader status with entry prices."""
    status = await get_signal_trader_status()
    daily_ctx = await get_daily_context()
    
    msg = "📡 *СИГНАЛ ТРЕЙДЕР*\n"
    msg += "═" * 25 + "\n"
    msg += f"Статус: {'✅ Работает' if status['enabled'] else '❌ Остановлен'}\n"
    msg += f"💵 Баланс: ${status['capital']:,.2f}\n"
    
    if daily_ctx:
        verdict = daily_ctx.get("verdict", "NEUTRAL")
        entries = daily_ctx.get("entries", {})
        
        msg += f"\n🎯 *Вердикт:* {verdict}\n"
        
        if entries:
            msg += "\n📊 *Точки входа и сигналы:*\n"
            for symbol, price in entries.items():
                if price:
                    if verdict == "BUY":
                        msg += f"  • {symbol}: вход ${price:,.0f}\n"
                        msg += f"    → BUY когда цена ≤ ${price:,.0f}\n"
                        msg += f"    → (цена упала на 3%+ от входа = доп. сигнал)\n"
                    elif verdict == "SELL":
                        msg += f"  • {symbol}: вход ${price:,.0f}\n"
                        msg += f"    → SELL когда цена ≥ ${price:,.0f}\n"
                        msg += f"    → (цена выросла на 3%+ от входа = доп. сигнал)\n"
                    else:
                        msg += f"  • {symbol}: ${price:,.0f}\n"
        else:
            msg += "\n📭 Нет точек входа — сделай /daily\n"
    else:
        msg += "\n📭 Нет контекста — сделай /daily\n"
    
    msg += "\n" + "═" * 25 + "\n"
    msg += f"💰 Всего сделок: {status['total_trades']}\n"
    msg += f"📈 Total PnL: ${status['total_pnl']:+,.2f}\n"
    msg += "═" * 25
    
    await message.answer(msg, parse_mode="Markdown")
