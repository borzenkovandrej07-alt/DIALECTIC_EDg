"""
Dialectic Edge v6.0 — UX апгрейд.
- Одно сообщение вместо 6 (краткая выжимка + Synth)
- Кнопка "📖 Полные дебаты" — листаешь раунды по одному
- Простой язык в выводах для обычных людей
"""

import asyncio
import logging
import re
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
    InlineKeyboardMarkup, InlineKeyboardButton
)

from config import BOT_TOKEN, ADMIN_IDS, CACHE_TTL_HOURS, REDIS_URL
from news_fetcher import NewsFetcher
from data_sources import fetch_full_context
from web_search import get_full_realtime_context, search_news_context
from meta_analyst import get_meta_context
from sentiment import analyze_and_filter, format_for_agents
from agents import DebateOrchestrator
from report_sanitizer import sanitize_full_report
from chart_generator import generate_main_chart
from storage import Storage
from database import (
    init_db, upsert_user, get_user, increment_requests,
    get_daily_subscribers, set_daily_sub,
    get_track_record, save_feedback, get_feedback_stats,
    log_report, get_admin_stats,
    save_debate_session, get_debate_session,
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
from github_export import export_to_github, push_digest_cache
from github_export import get_previous_digest, push_digest_cache
from debate_storage import save_debate_redis, get_debate_redis

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


def debate_plain_text(text: str) -> str:
    """
    Текст раунда дебатов без parse_mode: ответы моделей часто ломают Telegram Markdown
    (незакрытые *, _, ссылки) → Bad Request: can't parse entities.
    """
    t = clean_markdown(text)
    t = re.sub(r"[*_`#]", "", t)
    t = re.sub(r"\[([^\]]+)\]\([^)]*\)", r"\1", t)
    t = re.sub(r"\n{3,}", "\n\n", t)
    return t.strip()


def split_message(text: str, max_len: int = 3800) -> list:
    # Агрессивно чистим весь markdown — убираем *, _, `, #
    import re
    text = re.sub(r'[*_`#]', '', text)
    # Убираем двойные пробелы и лишние пустые строки
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = text.strip()

    if len(text) <= max_len:
        return [text]
    chunks = []
    while len(text) > max_len:
        split_at = text.rfind("\n", 0, max_len)
        if split_at == -1 or split_at < max_len // 2:
            split_at = text.rfind(" ", 0, max_len)
        if split_at == -1:
            split_at = max_len
        chunks.append(text[:split_at].rstrip())
        text = text[split_at:].lstrip("\n ")
    if text.strip():
        chunks.append(text.strip())
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
            if not stripped or stripped.startswith("──"):
                continue
            if in_bull and len(bull_lines) < 3:
                bull_lines.append(stripped)
            elif in_bear and len(bear_lines) < 3:
                bear_lines.append(stripped)
        if bull_lines:
            bull_summary = "\n".join(bull_lines)
        if bear_lines:
            bear_summary = "\n".join(bear_lines)

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
        await bot.send_message(chat_id, msg)
        if i == 0:
            await send_digest_chart(chat_id, report, prices_dict or {}, stars_str, pct_val)
        await asyncio.sleep(0.3)
    await bot.send_message(
        chat_id,
        "Полный анализ выше",
        reply_markup=main_report_keyboard(
            user_id, has_debates=bool(debate_cache.get(user_id, {}).get("rounds")),
        ),
    )


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

    if action == "noop":
        await callback.answer()
        return

    try:
        kb_uid = int(user_id_str)
    except ValueError:
        await callback.answer()
        return

    if kb_uid != callback.from_user.id:
        await callback.answer("Кнопка не с твоего аккаунта", show_alert=True)
        return

    user_id = callback.from_user.id
    try:
        round_idx = int(action)
    except ValueError:
        await callback.answer()
        return

    cache = debate_cache.get(user_id)
    if not cache:
        report_redis = await get_debate_redis(user_id)
        cache = hydrate_debate_from_report(report_redis) if report_redis else None
        if cache:
            debate_cache[user_id] = cache
    if not cache:
        report_db = await get_debate_session(user_id)
        cache = hydrate_debate_from_report(report_db) if report_db else None
        if cache:
            debate_cache[user_id] = cache
    if not cache:
        storage.reload_from_disk()
        snap = storage.get_user_debate_snapshot(user_id)
        cache = hydrate_debate_from_report(snap) if snap else None
        if cache:
            debate_cache[user_id] = cache

    if not cache:
        storage.reload_from_disk()
        rep_user = storage.get_user_last_cached_report(user_id)
        cache = hydrate_debate_from_report(rep_user) if rep_user else None
        if cache:
            debate_cache[user_id] = cache
    if not cache:
        storage.reload_from_disk()
        cached = storage.get_cached_report()
        rep = cached.get("report") if cached else None
        cache = hydrate_debate_from_report(rep) if rep else None
        if cache:
            debate_cache[user_id] = cache

    if not cache:
        logger.warning(
            "debate hydrate miss user_id=%s — RAM пуст, Redis/SQLite/JSON недоступны с этого воркера "
            "(часто: два инстанса бота → TelegramConflict, или эфемерный диск без Redis). "
            "Railway: один реплика + REDIS_URL или постоянный volume для БД.",
            user_id,
        )
        await callback.answer(
            "❌ Дебаты не найдены (рестарт бота или второй инстанс). "
            "Задай REDIS_URL на Railway или /daily заново.",
            show_alert=True,
        )
        return

    rounds = cache["rounds"]
    if round_idx >= len(rounds):
        await callback.answer()
        return

    round_text = debate_plain_text(rounds[round_idx])

    if len(round_text) > 4000:
        round_text = round_text[:3900] + "\n\n(…сокращено)"

    kb = debates_keyboard(user_id, round_idx, len(rounds))

    try:
        await callback.message.edit_text(round_text, reply_markup=kb)
    except Exception as e:
        logger.warning("debate edit_text: %s", e)
        try:
            await callback.message.answer(round_text, reply_markup=kb)
        except Exception as e2:
            logger.error("debate answer fallback: %s", e2)

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
) -> tuple[str, dict]:
    tasks = [
        fetcher.fetch_all(),
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

    sentiment_result, confidence_instruction = analyze_and_filter(
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
    await save_predictions_from_report(report, source_news=source)
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
    report, _ = await run_full_analysis(user_id)
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
        report, prices = await run_full_analysis(user_id)
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
        report, prices = await run_full_analysis(user_id)
        await bot.delete_message(chat_id=message.chat.id, message_id=wait_msg.message_id)
        await send_daily_digest_bundle(message.chat.id, user_id, report, prices)

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
        report, prices = await run_full_analysis(
            user_id, custom_news=user_news, custom_mode=True
        )
        await bot.delete_message(chat_id=message.chat.id, message_id=wait_msg.message_id)
        await send_daily_digest_bundle(message.chat.id, user_id, report, prices)

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
        "• `/daily` — дайджест (из кэша до суток без токенов)\n"
        "• `/daily force` — принудительно новый AI-прогон\n"
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
    if not REDIS_URL.strip():
        logger.warning(
            "REDIS_URL не задан — кнопка «Полные дебаты» не переживет другой воркер/рестарт. "
            "Railway: Add-ons → Redis; при TelegramConflict выключи лишний деплой с тем же BOT_TOKEN."
        )

    scheduler = Scheduler(
        bot=bot,
        send_daily_fn=deliver_scheduled_daily,
        check_predictions_fn=check_pending_predictions
    )

    await asyncio.gather(
        dp.start_polling(bot),
        scheduler.start()
    )


if __name__ == "__main__":
    asyncio.run(main())
