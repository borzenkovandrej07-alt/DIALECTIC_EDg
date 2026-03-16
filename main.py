"""
main.py — Dialectic Edge v7.0

UX v7.0:
- Пользователь получает 1 красивое сообщение вместо 6
- Bull кратко / Bear кратко / Вердикт судьи / ПЛАН
- Фото-график (matplotlib) прикреплён к сообщению
- Кнопка "📖 Полные дебаты" — все раунды постранично
- Кнопка "🇷🇺 Анализ для России"
- Tavily веб-поиск обогащает контекст агентов

Исправления v7.0:
- Synth обязан занять позицию (не "наблюдаем")
- S&P 500 → ^GSPC (не SPY ETF)
- CPI база обновлена → ~2.4% YoY
- Графики через chart_generator.py
- Память ошибок через learning.py
"""

import asyncio
import logging
import re
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
    InlineKeyboardMarkup, InlineKeyboardButton,
    BufferedInputFile,
)

from config import BOT_TOKEN, ADMIN_IDS
from news_fetcher import NewsFetcher
from data_sources import fetch_full_context
from web_search import get_full_realtime_context, search_news_context, get_news_context
from meta_analyst import get_meta_context
from sentiment import analyze_and_filter_async, format_for_agents
from agents import DebateOrchestrator
from storage import Storage
from database import (
    init_db, upsert_user, get_user, increment_requests,
    get_daily_subscribers, set_daily_sub,
    get_track_record, save_feedback, get_feedback_stats,
    log_report, get_admin_stats,
)
from tracker import check_pending_predictions, save_predictions_from_report
from scheduler import Scheduler
from user_profile import (
    init_profiles_table, save_profile, get_profile,
    build_profile_instruction, format_profile_card,
    RISK_PROFILES, HORIZONS, MARKETS,
)
from weekly_report import build_weekly_report
from russia_data import fetch_russia_context
from russia_agents import run_russia_analysis
from github_export import export_to_github, push_digest_cache, get_previous_digest
from learning import get_recent_lessons
from chart_generator import generate_main_chart, generate_russia_chart, is_available as charts_ok

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

bot      = Bot(token=BOT_TOKEN)
dp       = Dispatcher()
fetcher  = NewsFetcher()
storage  = Storage()

FREE_DAILY_LIMIT = 5
scheduler: Scheduler = None

# Кэш для листания дебатов {user_id: {"rounds": [...], "full": str}}
debate_cache: dict = {}
# Кэш РФ анализа
russia_cache: dict = {}


# ─── Утилиты ──────────────────────────────────────────────────────────────────

def clean_md(text: str) -> str:
    """Чистит незакрытые markdown-теги."""
    lines = []
    for line in text.split("\n"):
        for ch in ("*", "_", "`"):
            if line.count(ch) % 2 != 0:
                line = line.replace(ch, "")
        lines.append(line)
    return "\n".join(lines)


def split_msg(text: str, max_len: int = 3800) -> list[str]:
    """Режет текст на куски <= max_len, убирает markdown."""
    text = re.sub(r"[*_`#]", "", text)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    if len(text) <= max_len:
        return [text]
    chunks = []
    while len(text) > max_len:
        idx = text.rfind("\n", 0, max_len)
        if idx < max_len // 2:
            idx = text.rfind(" ", 0, max_len)
        if idx == -1:
            idx = max_len
        chunks.append(text[:idx].rstrip())
        text = text[idx:].lstrip()
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


def signal_to_stars(confidence) -> str:
    mapping = {"HIGH": 0.85, "MEDIUM": 0.55, "LOW": 0.25, "EXTREME": 0.95}
    if isinstance(confidence, str):
        confidence = mapping.get(confidence.upper(), 0.5)
    try:
        confidence = float(confidence)
    except (TypeError, ValueError):
        confidence = 0.5
    stars = max(1, min(5, round(confidence * 5)))
    return "★" * stars + "☆" * (5 - stars)


def feedback_kb(report_type: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="👍 Полезно", callback_data=f"fb:1:{report_type}"),
        InlineKeyboardButton(text="👎 Мимо",    callback_data=f"fb:-1:{report_type}"),
    ]])


# ─── Парсинг отчёта ───────────────────────────────────────────────────────────

def parse_report(report: str) -> dict:
    """Разбивает полный отчёт на части для UX."""
    parts = {"rounds": [], "synthesis": "", "disclaimer": "", "full": report}

    # Дисклеймер — ищем ТОЛЬКО по тексту, без разделителя
    # (разделители встречаются внутри синтеза и не должны его обрезать)
    for marker in ["🤝 Честно о боте:", "🤝 *Честно о боте:*"]:
        if marker in report:
            idx = report.find(marker)
            # Берём разделитель перед дисклеймером если он есть
            sep_before = report.rfind("─────────────────────────", 0, idx)
            if sep_before != -1 and idx - sep_before < 5:
                idx = sep_before
            parts["disclaimer"] = report[idx:]
            report = report[:idx]
            break

    # Синтез
    for marker in ["⚖️ *ВЕРДИКТ И ТОРГОВЫЙ ПЛАН*", "⚖️ ВЕРДИКТ И ТОРГОВЫЙ ПЛАН",
                   "⚖️ *ИТОГОВЫЙ СИНТЕЗ", "⚖️ ИТОГОВЫЙ СИНТЕЗ"]:
        if marker in report:
            idx = report.find(marker)
            parts["synthesis"] = report[idx:].strip()
            report = report[:idx]
            break

    # Раунды
    debate_marker = "🗣 *ХОД ДЕБАТОВ*"
    round_markers = ["── Раунд 1 ──", "── Раунд 2 ──", "── Раунд 3 ──",
                     "── Раунд 4 ──", "── Раунд 5 ──"]
    if debate_marker in report:
        debate_section = report[report.find(debate_marker):]
        current = ""
        n = 0
        for line in debate_section.split("\n"):
            if any(m in line for m in round_markers):
                if current.strip() and n > 0:
                    parts["rounds"].append(current.strip())
                current = line + "\n"
                n += 1
            else:
                current += line + "\n"
        if current.strip() and n > 0:
            parts["rounds"].append(current.strip())
        if not parts["rounds"]:
            parts["rounds"] = [debate_section]

    return parts


def extract_short_position(round1: str, agent_emoji: str) -> str:
    """Извлекает первые 3-4 тезиса агента из раунда 1."""
    lines      = round1.split("\n")
    collecting = False
    result     = []
    for line in lines:
        if agent_emoji in line and ("Bull" in line or "Bear" in line):
            collecting = True
            continue
        if collecting:
            # Останавливаемся на другом агенте
            if any(e in line for e in ["🐂", "🐻", "🔍", "⚖️"]) and line.strip():
                break
            stripped = line.strip()
            if not stripped or stripped.startswith("──") or stripped.startswith("*──"):
                continue
            if len(result) < 4:
                result.append(stripped)
    return "\n".join(result) if result else "Анализируем данные..."


def extract_verdict(synthesis: str) -> str:
    """
    Извлекает ВЕРДИКТ СУДЬИ + ПРОСТЫМИ СЛОВАМИ из синтеза.
    Оба блока важны — вердикт даёт позицию, простыми словами даёт понимание.
    """
    parts = []

    # Блок 1: Вердикт судьи
    for m in ["🏆 ВЕРДИКТ СУДЬИ", "ВЕРДИКТ СУДЬИ"]:
        if m in synthesis:
            idx   = synthesis.find(m)
            chunk = synthesis[idx:idx + 600]
            # Берём до плана действий или честного итога
            for stop in ["💼 ПЛАН ДЕЙСТВИЙ", "⚠️ ЧЕСТНЫЙ ИТОГ",
                         "🗣 ПРОСТЫМИ СЛОВАМИ", "─────────────────"]:
                pos = chunk.find(stop, 10)
                if pos != -1:
                    chunk = chunk[:pos]
                    break
            parts.append(chunk.strip())
            break

    if parts:
        return "\n\n".join(parts)

    # Fallback
    return synthesis[:600].strip()


def extract_simple_words(synthesis: str) -> str:
    """Извлекает блок ПРОСТЫМИ СЛОВАМИ отдельно."""
    for m in ["🗣 ПРОСТЫМИ СЛОВАМИ", "ПРОСТЫМИ СЛОВАМИ"]:
        if m in synthesis:
            idx   = synthesis.find(m)
            chunk = synthesis[idx:idx + 1200]
            for stop in ["⚠️ Не является", "─────────────────────────",
                         "🏆 ВЕРДИКТ", "💎 ЖЁСТКИЙ"]:
                pos = chunk.find(stop, 10)
                if pos != -1:
                    chunk = chunk[:pos]
                    break
            return chunk.strip()
    return ""


def extract_plan(synthesis: str) -> str:
    """Извлекает торговый план из синтеза. Лимит 2500 — план бывает длинным."""
    for m in ["💼 ПЛАН ДЕЙСТВИЙ", "ПЛАН ДЕЙСТВИЙ"]:
        if m in synthesis:
            idx   = synthesis.find(m)
            chunk = synthesis[idx:idx + 2500]
            for stop in ["⚠️ ЧЕСТНЫЙ ИТОГ", "🗣 ПРОСТЫМИ СЛОВАМИ",
                         "🏆 ВЕРДИКТ"]:
                pos = chunk.find(stop, 20)
                if pos != -1:
                    chunk = chunk[:pos]
                    break
            return chunk.strip()
    return ""


# ─── Построение красивого короткого дайджеста ─────────────────────────────────

def build_digest(parts: dict, stars: str, pct: int) -> str:
    """
    Формирует ОДНО сообщение — главный дайджест для пользователя.
    Структура:
      Шапка (дата, сигнал)
      🐂 Bull — 3 тезиса
      🐻 Bear — 3 тезиса
      🏆 Вердикт судьи
      💼 Торговый план
      Дисклеймер капсом
    """
    now = datetime.now().strftime("%d.%m.%Y %H:%M")

    # Позиции из раунда 1
    bull_text = "Анализируем..."
    bear_text = "Анализируем..."
    if parts["rounds"]:
        r1        = parts["rounds"][0]
        bull_text = extract_short_position(r1, "🐂")
        bear_text = extract_short_position(r1, "🐻")

    # Вердикт, простыми словами и план из синтеза
    verdict      = extract_verdict(parts["synthesis"])      if parts["synthesis"] else ""
    simple_words = extract_simple_words(parts["synthesis"]) if parts["synthesis"] else ""
    plan         = extract_plan(parts["synthesis"])         if parts["synthesis"] else ""

    lines = [
        "📊 DIALECTIC EDGE — ЕЖЕДНЕВНЫЙ ДАЙДЖЕСТ",
        f"🕐 {now}",
        "",
        f"Уровень сигнала: {stars} ({pct}% уверенности)",
        "─" * 30,
        "",
        "🐂 БЫЧЬЯ ПОЗИЦИЯ:",
        bull_text,
        "",
        "🐻 МЕДВЕЖЬЯ ПОЗИЦИЯ:",
        bear_text,
        "",
        "─" * 30,
    ]

    if verdict:
        lines += ["", verdict, ""]

    if plan:
        lines += ["─" * 30, "", plan, ""]

    # Простыми словами — всегда в конце перед дисклеймером
    if simple_words:
        lines += ["─" * 30, "", simple_words, ""]

    lines += [
        "─" * 30,
        "",
        "⚠️ ЭТО КРАТКИЙ ДАЙДЖЕСТ.",
        "ПОЛНЫЕ ДЕБАТЫ АГЕНТОВ (все раунды, верификация,",
        "эффекты 2-го порядка) — НАЖМИ КНОПКУ НИЖЕ.",
        "",
        "Не является финансовым советом. AI-анализ. DYOR.",
    ]

    return "\n".join(str(l) for l in lines)


def split_digest(text: str) -> list[str]:
    """
    Режет дайджест на части по смысловым блокам,
    а не по символам — чтобы план не обрывался на полуслове.
    """
    # Сначала пробуем целиком — если влезает
    clean = re.sub(r"[*_`#]", "", text)
    clean = re.sub(r"\n{3,}", "\n\n", clean).strip()
    if len(clean) <= 4000:
        return [clean]

    # Разбиваем по смысловым разделителям
    parts  = []
    blocks = re.split(r"(─{10,})", clean)
    current = ""
    for block in blocks:
        if len(current) + len(block) > 3800 and current.strip():
            parts.append(current.strip())
            current = block
        else:
            current += block
    if current.strip():
        parts.append(current.strip())
    return parts if parts else [clean[:4000]]


# ─── Клавиатуры ───────────────────────────────────────────────────────────────

def main_kb(user_id: int, has_debates: bool = True) -> InlineKeyboardMarkup:
    """
    Кнопки под дайджестом.
    Дебаты — листаются по кнопке (не отправляются автоматом).
    """
    rows = []
    if has_debates:
        rows.append([InlineKeyboardButton(
            text="📖 Полные дебаты агентов",
            callback_data=f"debate:{user_id}:0",
        )])
    rows.append([
        InlineKeyboardButton(text="🇷🇺 Russia Edge", callback_data=f"russia_quick:{user_id}"),
        InlineKeyboardButton(text="🔄 Обновить",     callback_data=f"refresh:{user_id}"),
    ])
    rows.append([
        InlineKeyboardButton(text="👍 Полезно", callback_data="fb:1:daily"),
        InlineKeyboardButton(text="👎 Мимо",    callback_data="fb:-1:daily"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def debates_kb(user_id: int, idx: int, total: int) -> InlineKeyboardMarkup:
    nav = []
    if idx > 0:
        nav.append(InlineKeyboardButton(text="◀️",
                   callback_data=f"debate:{user_id}:{idx-1}"))
    nav.append(InlineKeyboardButton(text=f"📄 {idx+1}/{total}",
               callback_data="debate:noop"))
    if idx < total - 1:
        nav.append(InlineKeyboardButton(text="▶️",
                   callback_data=f"debate:{user_id}:{idx+1}"))
    return InlineKeyboardMarkup(inline_keyboard=[nav])


# ─── Ядро анализа ─────────────────────────────────────────────────────────────

async def run_full_analysis(user_id: int, custom_news: str = "",
                            custom_mode: bool = False) -> tuple[str, dict]:
    """Возвращает (report_text, prices_dict)."""
    tasks = [
        fetcher.fetch_all(),
        fetch_full_context(),
        get_full_realtime_context(),
        get_profile(user_id),
        get_meta_context(),
        get_previous_digest(),   # прошлый анализ для сравнения
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    news, geo, realtime_result, profile, meta, prev_digest = results
    if isinstance(prev_digest, Exception): prev_digest = ""

    # Безопасная распаковка realtime — если упало, не крашим всё
    if isinstance(realtime_result, Exception):
        logger.error(f"get_full_realtime_context error: {realtime_result}")
        prices, live_prices = {}, ""
    elif isinstance(realtime_result, tuple) and len(realtime_result) == 2:
        prices, live_prices = realtime_result
    else:
        prices, live_prices = {}, ""

    if isinstance(news, Exception):    news = ""
    if isinstance(geo, Exception):     geo = ""
    if isinstance(profile, Exception): profile = {"risk": "moderate", "horizon": "swing", "markets": "all"}
    if isinstance(meta, Exception):    meta = ""

    profile_instr = build_profile_instruction(profile)
    lessons = await get_recent_lessons(days=14)
    if lessons:
        profile_instr += lessons

    # Tavily веб-поиск — реальные новости в контекст
    topics = [custom_news] if custom_mode and custom_news else ["markets", "bitcoin", "fed"]
    tavily_news = await get_news_context(topics)

    if custom_mode and custom_news:
        web_ctx = await search_news_context(custom_news)
        news_ctx = (f"ТЕМА АНАЛИЗА: {custom_news}\n\n"
                    f"{web_ctx}\n\n{geo}\n\n{meta}\n\n{tavily_news}")
    else:
        news_ctx = (f"{geo}\n\n=== НОВОСТИ ===\n{news}\n\n{meta}\n\n{tavily_news}")

    # Добавляем прошлый анализ в контекст — агенты сравнивают с реальностью
    if prev_digest and not custom_mode:
        news_ctx += f"\n\n{prev_digest}"
        logger.info("📚 Прошлый анализ передан агентам для сравнения")

    sentiment_result, confidence_instr = await analyze_and_filter_async(news_ctx, str(live_prices))
    sentiment_block = format_for_agents(sentiment_result, confidence_instr)

    orchestrator = DebateOrchestrator()
    report = await orchestrator.run_debate(
        news_context=news_ctx,
        live_prices=live_prices,
        profile_instruction=profile_instr + sentiment_block,
        custom_mode=custom_mode,
    )

    # Уровень сигнала
    _conf_map = {"HIGH": 0.85, "MEDIUM": 0.55, "LOW": 0.25, "EXTREME": 0.95}
    c_raw = sentiment_result.confidence
    c_num = _conf_map.get(c_raw.upper(), 0.5) if isinstance(c_raw, str) else float(c_raw or 0.5)
    stars = signal_to_stars(c_num)
    pct   = int(c_num * 100)
    sep   = "─" * 30 + "\n"
    report = report.replace(sep,
        sep + f"📶 Уровень сигнала: {stars} ({pct}% уверенности)\n\n", 1)

    source = custom_news[:300] if custom_mode else str(news)[:300]
    await save_predictions_from_report(report, source_news=source)
    await log_report(user_id, "analyze" if custom_mode else "daily", source, report[:500])

    if not custom_mode:
        storage.cache_report(report)
        if scheduler:
            asyncio.create_task(scheduler.export_now())
        try:
            asyncio.create_task(
                push_digest_cache(report, datetime.now().strftime("%d.%m.%Y %H:%M"))
            )
        except Exception as e:
            logger.warning(f"Digest cache: {e}")

    return report, prices


async def run_daily_analysis(user_id: int) -> str:
    report, _ = await run_full_analysis(user_id)
    return report


# ─── Отправка дайджеста пользователю ─────────────────────────────────────────

async def send_digest(message: Message, report: str, prices: dict):
    """
    UX v7.2 — финальный:
    1. Фото-график (dashboard)
    2. Краткий дайджест (Bull/Bear/Вердикт/План)
    3. Дисклеймер отдельным сообщением
    4. Кнопки: [📖 Полные дебаты] [🇷🇺] [🔄] [👍] [👎]
       — по кнопке дебаты листаются постранично (◀️ 📄 1/3 ▶️)
    """
    parts = parse_report(report)
    user_id = message.from_user.id
    debate_cache[user_id] = {"rounds": parts["rounds"], "full": report}

    # Уровень сигнала
    pct_val = 55
    m = re.search(r"Уровень сигнала.*?(\d+)%", report)
    if m:
        pct_val = int(m.group(1))
    stars_str = signal_to_stars(pct_val / 100)

    # 1. График
    if charts_ok():
        try:
            logger.info(f"Генерирую график, prices keys: {list(prices.keys())}")
            chart_bytes = generate_main_chart(report, prices, stars_str, pct_val)
            if chart_bytes:
                await message.answer_photo(
                    photo=BufferedInputFile(chart_bytes.read(), filename="analysis.png"),
                    caption="📊 Dialectic Edge — Market Dashboard",
                )
                logger.info("✅ График отправлен")
            else:
                logger.warning("⚠️ generate_main_chart вернул None")
        except Exception as e:
            logger.error(f"Chart error: {e}", exc_info=True)
    else:
        logger.warning("matplotlib недоступен")

    # 2. Краткий дайджест (без дисклеймера внутри)
    digest = build_digest(parts, stars_str, pct_val)
    chunks = split_digest(digest)
    for chunk in chunks:
        await message.answer(chunk)
        await asyncio.sleep(0.2)

    # 3. Дисклеймер — отдельным сообщением
    disclaimer = parts.get("disclaimer", "")
    if not disclaimer:
        disclaimer = (
            "─────────────────────────\n"
            "🤝 Честно о боте:\n"
            "Это AI-анализ на основе публичных данных — не предсказание будущего.\n"
            "Рынок непредсказуем. Агенты могут ошибаться и иногда ошибаются.\n"
            "Используй как один из инструментов мышления, не как сигнал к действию.\n\n"
            "⚠️ Не является финансовым советом. DYOR. Торговля = риск потери капитала."
        )
    clean_disc = re.sub(r"[*_`#]", "", disclaimer).strip()
    await message.answer(clean_disc)

    # 4. Кнопки — последнее сообщение
    has_debates = bool(parts["rounds"])
    await message.answer(
        "👇 Действия:",
        reply_markup=main_kb(user_id, has_debates=has_debates),
    )


# ─── /daily ───────────────────────────────────────────────────────────────────

@dp.message(Command("daily"))
async def cmd_daily(message: Message):
    user_id = message.from_user.id
    await upsert_user(user_id, message.from_user.username or "")

    if not await check_limit(user_id):
        await message.answer(f"⛔ Лимит — {FREE_DAILY_LIMIT} запросов/день (free).\n"
                             "Попробуй завтра или /subscribe.")
        return

    # Кэш
    cached = storage.get_cached_report()
    if cached:
        parts = parse_report(cached["report"])
        debate_cache[user_id] = {"rounds": parts["rounds"], "full": cached["report"]}
        pct_val   = 55
        m = re.search(r"Уровень сигнала.*?(\d+)%", cached["report"])
        if m:
            pct_val = int(m.group(1))
        stars_str = signal_to_stars(pct_val / 100)
        digest    = build_digest(parts, stars_str, pct_val)
        chunks    = split_digest(digest)
        has_debates = bool(parts["rounds"])
        for chunk in chunks[:-1]:
            await message.answer(chunk)
        await message.answer(
            chunks[-1] + f"\n\n📦 Кэш от {cached['timestamp']}. Новый через 2ч.",
            reply_markup=main_kb(user_id, has_debates=has_debates),
        )
        return

    wait = await message.answer(
        "⏳ Запускаю анализ...\n"
        "🔄 Binance → FRED → Tavily новости → дебаты агентов\n"
        "Займёт 2–4 минуты..."
    )
    try:
        await increment_requests(user_id)
        report, prices = await run_full_analysis(user_id)
        await bot.delete_message(message.chat.id, wait.message_id)
        await send_digest(message, report, prices)
    except Exception as e:
        logger.error(f"Daily error: {e}", exc_info=True)
        await bot.edit_message_text(
            f"❌ Ошибка: {str(e)[:200]}\nПроверь API ключи.",
            chat_id=message.chat.id, message_id=wait.message_id,
        )


# ─── /analyze ─────────────────────────────────────────────────────────────────

@dp.message(Command("analyze"))
async def cmd_analyze(message: Message):
    user_id   = message.from_user.id
    await upsert_user(user_id, message.from_user.username or "")
    msg_parts = message.text.split(maxsplit=1)
    if len(msg_parts) < 2 or not msg_parts[1].strip():
        await message.answer(
            "❗ Укажи новость:\n"
            "/analyze Fed снизил ставку до 4%\n"
            "/analyze Binance заморозила вывод\n"
            "/analyze Китай ограничил экспорт металлов"
        )
        return
    if not await check_limit(user_id):
        await message.answer(f"⛔ Лимит — {FREE_DAILY_LIMIT} запросов/день.")
        return

    user_news = msg_parts[1].strip()
    wait = await message.answer(f"🔍 Анализирую: {user_news[:100]}\n⏳ 2–4 минуты...")
    try:
        await increment_requests(user_id)
        report, prices = await run_full_analysis(
            user_id, custom_news=user_news, custom_mode=True
        )
        await bot.delete_message(message.chat.id, wait.message_id)
        await send_digest(message, report, prices)
    except Exception as e:
        logger.error(f"Analyze error: {e}", exc_info=True)
        await bot.edit_message_text(
            f"❌ Ошибка: {str(e)[:200]}",
            chat_id=message.chat.id, message_id=wait.message_id,
        )


# ─── Листание дебатов ─────────────────────────────────────────────────────────

@dp.callback_query(F.data.startswith("debate:"))
async def cb_debate(callback: CallbackQuery):
    parts = callback.data.split(":")
    if len(parts) < 3 or parts[2] == "noop":
        await callback.answer()
        return
    user_id   = int(parts[1])
    round_idx = int(parts[2])
    cache     = debate_cache.get(user_id)
    if not cache:
        await callback.answer("❌ Дебаты устарели — запусти /daily заново")
        return
    rounds = cache["rounds"]
    if round_idx >= len(rounds):
        await callback.answer()
        return
    text = clean_md(rounds[round_idx])
    if len(text) > 4000:
        text = text[:3900] + "\n\n...сокращено..."
    try:
        await callback.message.edit_text(
            text, reply_markup=debates_kb(user_id, round_idx, len(rounds))
        )
    except Exception:
        await callback.message.answer(
            text, reply_markup=debates_kb(user_id, round_idx, len(rounds))
        )
    await callback.answer()


# ─── Обновление по кнопке ─────────────────────────────────────────────────────

@dp.callback_query(F.data.startswith("refresh:"))
async def cb_refresh(callback: CallbackQuery):
    user_id = int(callback.data.split(":")[1])
    await callback.answer("🔄 Запускаю обновлённый анализ...")
    storage.clear_cache()  # сбрасываем кэш чтобы пересчитать
    try:
        report, prices = await run_full_analysis(user_id)
        await send_digest(callback.message, report, prices)
    except Exception as e:
        await callback.message.answer(f"❌ Ошибка обновления: {str(e)[:100]}")


# ─── Russia Edge (кнопка и команда) ──────────────────────────────────────────

async def _send_russia(message: Message, user_id: int):
    import time
    now_ts = time.time()
    if russia_cache.get("report") and (now_ts - russia_cache.get("ts", 0)) < 7200:
        # Из кэша
        report = russia_cache["report"]
        if charts_ok():
            try:
                chart = generate_russia_chart(report)
                if chart:
                    await message.answer_photo(
                        BufferedInputFile(chart.read(), filename="russia.png"),
                        caption="🇷🇺 Russia Edge — Риски и возможности",
                    )
            except Exception as e:
                logger.warning(f"Russia chart: {e}")
        for chunk in split_msg(report):
            await message.answer(chunk)
        await message.answer(
            f"📦 Кэш от {russia_cache['timestamp']}. Новый через 2ч.",
            reply_markup=feedback_kb("russia"),
        )
        return

    global_report = ""
    cached = storage.get_cached_report()
    if cached:
        global_report = cached["report"]
    else:
        global_report = "Глобальный анализ не запущен. Работаю только на данных РФ."

    wait = await message.answer(
        "🇷🇺 Запускаю Russia Edge...\n"
        "🔄 ЦБ РФ → Мосбиржа → Llama агенты → Mistral синтез\n"
        "Займёт 1–3 минуты..."
    )
    try:
        await increment_requests(user_id)
        russia_ctx = await fetch_russia_context()
        report     = await run_russia_analysis(global_report, russia_ctx)

        import time
        russia_cache.update({
            "report":    report,
            "timestamp": datetime.now().strftime("%d.%m.%Y %H:%M"),
            "ts":        time.time(),
        })
        await bot.delete_message(message.chat.id, wait.message_id)

        if charts_ok():
            try:
                chart = generate_russia_chart(report)
                if chart:
                    await message.answer_photo(
                        BufferedInputFile(chart.read(), filename="russia.png"),
                        caption="🇷🇺 Russia Edge — Риски и возможности",
                    )
            except Exception as e:
                logger.warning(f"Russia chart: {e}")

        for chunk in split_msg(report):
            await message.answer(chunk)
        await message.answer("Был ли анализ полезным?",
                             reply_markup=feedback_kb("russia"))
    except Exception as e:
        logger.error(f"Russia error: {e}", exc_info=True)
        await bot.edit_message_text(
            f"❌ Ошибка: {str(e)[:200]}",
            chat_id=message.chat.id, message_id=wait.message_id,
        )


@dp.message(Command("russia"))
async def cmd_russia(message: Message):
    user_id = message.from_user.id
    await upsert_user(user_id, message.from_user.username or "")
    if not await check_limit(user_id):
        await message.answer(f"⛔ Лимит — {FREE_DAILY_LIMIT} запросов/день.")
        return
    await _send_russia(message, user_id)


@dp.callback_query(F.data.startswith("russia_quick:"))
async def cb_russia_quick(callback: CallbackQuery):
    user_id = int(callback.data.split(":")[1])
    await callback.answer("🇷🇺 Загружаю Russia Edge...")
    await _send_russia(callback.message, user_id)


# ─── /markets ─────────────────────────────────────────────────────────────────

@dp.message(Command("markets"))
async def cmd_markets(message: Message):
    await upsert_user(message.from_user.id)
    wait = await message.answer("⏳ Загружаю живые данные...")
    try:
        _, live_prices = await get_full_realtime_context()
        now = datetime.now().strftime("%d.%m.%Y %H:%M")
        await bot.edit_message_text(
            f"📊 РЫНКИ — {now}\n\n{live_prices}",
            chat_id=message.chat.id, message_id=wait.message_id,
        )
    except Exception as e:
        await bot.edit_message_text(f"❌ Ошибка: {e}",
                                    chat_id=message.chat.id,
                                    message_id=wait.message_id)


# ─── /start, /help ────────────────────────────────────────────────────────────

@dp.message(Command("start"))
async def cmd_start(message: Message):
    await upsert_user(
        message.from_user.id,
        message.from_user.username or "",
        message.from_user.first_name or "",
    )
    name = message.from_user.first_name or "трейдер"
    await message.answer(
        f"👋 Привет, {name}!\n\n"
        "🧠 Dialectic Edge v7.0 — честный AI-аналитик рынков\n\n"
        "4 агента спорят используя живые данные:\n"
        "🐂 Bull (Groq/Llama) — ищет возможности\n"
        "🐻 Bear (Mistral) — указывает риски\n"
        "🔍 Verifier — проверяет каждую цифру\n"
        "⚖️ Synth — итог с чётким вердиктом\n\n"
        "НОВОЕ в v7.0:\n"
        "• 1 сообщение вместо 6 — кратко и чётко\n"
        "• Графики рынка (matplotlib)\n"
        "• Реальный веб-поиск (Tavily)\n"
        "• Synth всегда даёт вердикт — не уклоняется\n\n"
        "Команды:\n"
        "/profile — настрой риск-профиль\n"
        "/daily — дайджест рынков\n"
        "/analyze [текст] — анализ новости\n"
        "/russia — анализ для РФ рынка\n"
        "/markets — живые цены\n"
        "/trackrecord — история точности\n"
        "/subscribe — авторассылка\n\n"
        "⚠️ Не финансовый совет."
    )


@dp.message(Command("help"))
async def cmd_help(message: Message):
    await upsert_user(message.from_user.id)
    await message.answer(
        "📖 Dialectic Edge v7.0\n\n"
        "/daily — дайджест рынков (график + краткий анализ + кнопки)\n"
        "/analyze [текст] — анализ конкретной новости\n"
        "/russia — анализ для российского рынка\n"
        "/markets — живые цены Binance/Yahoo/FRED\n"
        "/profile — риск-профиль (влияет на рекомендации)\n"
        "/trackrecord — история точности прогнозов\n"
        "/weeklyreport — отчёт за неделю\n"
        "/subscribe on 08:00 — авторассылка\n"
        "/stats — твоя статистика\n\n"
        "⚠️ Не финансовый совет. Будущее неизвестно никому."
    )


# ─── /profile ─────────────────────────────────────────────────────────────────

@dp.message(Command("profile"))
async def cmd_profile(message: Message):
    user_id = message.from_user.id
    await upsert_user(user_id)
    profile = await get_profile(user_id)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🛡️ Консерватор",  callback_data="profile:risk:conservative"),
         InlineKeyboardButton(text="⚖️ Умеренный",     callback_data="profile:risk:moderate"),
         InlineKeyboardButton(text="🚀 Агрессивный",   callback_data="profile:risk:aggressive")],
        [InlineKeyboardButton(text="⚡ Скальпинг",     callback_data="profile:hz:scalp"),
         InlineKeyboardButton(text="📈 Свинг",         callback_data="profile:hz:swing"),
         InlineKeyboardButton(text="💎 Инвест",        callback_data="profile:hz:invest")],
        [InlineKeyboardButton(text="₿ Крипта",         callback_data="profile:mkt:crypto"),
         InlineKeyboardButton(text="📈 Акции",         callback_data="profile:mkt:stocks"),
         InlineKeyboardButton(text="🌍 Всё",           callback_data="profile:mkt:all")],
    ])
    await message.answer(
        f"⚙️ Настройка профиля\n\n{format_profile_card(profile)}\n\n"
        "Выбери параметры ниже:",
        reply_markup=kb,
    )


@dp.callback_query(F.data.startswith("profile:"))
async def cb_profile(callback: CallbackQuery):
    _, param_type, value = callback.data.split(":")
    user_id = callback.from_user.id
    profile = await get_profile(user_id)
    if param_type == "risk":   profile["risk"]    = value
    elif param_type == "hz":   profile["horizon"] = value
    elif param_type == "mkt":  profile["markets"] = value
    await save_profile(user_id, profile.get("risk","moderate"),
                       profile.get("horizon","swing"), profile.get("markets","all"))
    labels = {
        "conservative":"🛡️ Консерватор","moderate":"⚖️ Умеренный",
        "aggressive":"🚀 Агрессивный","scalp":"⚡ Скальпинг",
        "swing":"📈 Свинг","invest":"💎 Инвестиции",
        "crypto":"₿ Крипта","stocks":"📈 Акции","all":"🌍 Все рынки",
    }
    await callback.answer(f"✅ {labels.get(value, value)}")
    await callback.message.edit_text(
        f"✅ Профиль обновлён\n\n{format_profile_card(profile)}\n\n"
        "Следующий анализ адаптирован под тебя."
    )


# ─── /trackrecord ─────────────────────────────────────────────────────────────

@dp.message(Command("trackrecord"))
async def cmd_trackrecord(message: Message):
    await upsert_user(message.from_user.id)
    try:
        data    = await get_track_record()
        stats   = data["stats"]
        recent  = data["recent"]
        by_asset= data["by_asset"]
        total   = stats.get("total") or 0
        if total == 0:
            await message.answer(
                "📊 Track Record\n\nПрогнозы накапливаются. "
                "Запусти /daily — через 1-2 недели появится статистика."
            )
            return
        wins      = stats.get("wins") or 0
        losses    = stats.get("losses") or 0
        finished  = wins + losses
        winrate   = wins / finished * 100 if finished else 0
        avg_pnl   = stats.get("avg_pnl") or 0
        lines = [
            "📊 TRACK RECORD АГЕНТОВ\n",
            f"Всего: {total} | Winrate: {'🟢' if winrate>=55 else '🔴'} {winrate:.0f}%",
            f"Средний P&L: {'🟢' if avg_pnl>=0 else '🔴'} {avg_pnl:+.1f}%",
        ]
        if by_asset:
            lines.append("\nТоп активов:")
            for a in by_asset[:3]:
                wr = a["wins"]/a["calls"]*100 if a["calls"] else 0
                lines.append(f"  {a['asset']}: {wr:.0f}% wr | avg {a['avg_pnl']:+.1f}%")
        if recent:
            lines.append("\nПоследние сигналы:")
            for r in recent[:5]:
                e = "✅" if r["result"]=="win" else "❌"
                lines.append(f"  {e} {r['asset']} {r['direction']} "
                             f"→ {(r.get('pnl_pct') or 0):+.1f}%")
        lines.append("\n⚠️ Прошлые результаты не гарантируют будущих.")
        await message.answer("\n".join(lines))
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")


# ─── /weeklyreport ────────────────────────────────────────────────────────────

@dp.message(Command("weeklyreport"))
async def cmd_weekly(message: Message):
    await upsert_user(message.from_user.id)
    wait = await message.answer("⏳ Формирую отчёт за неделю...")
    try:
        report = await build_weekly_report()
        await bot.delete_message(message.chat.id, wait.message_id)
        await message.answer(report)
    except Exception as e:
        await bot.edit_message_text(f"❌ Ошибка: {e}",
                                    chat_id=message.chat.id,
                                    message_id=wait.message_id)


# ─── /subscribe ───────────────────────────────────────────────────────────────

@dp.message(Command("subscribe"))
async def cmd_subscribe(message: Message):
    user_id = message.from_user.id
    await upsert_user(user_id)
    user      = await get_user(user_id)
    is_subbed = user.get("daily_sub", 0) if user else 0
    sub_time  = user.get("sub_time", "08:00") if user else "08:00"
    parts     = message.text.split()
    if len(parts) == 1:
        status = f"✅ Активна ({sub_time} UTC)" if is_subbed else "❌ Отключена"
        await message.answer(
            f"📬 Авторассылка\nСтатус: {status}\n\n"
            "/subscribe on — включить в 08:00 UTC\n"
            "/subscribe on 09:30 — своё время\n"
            "/subscribe off — отключить"
        )
        return
    action   = parts[1].lower()
    time_str = parts[2] if len(parts) > 2 else "08:00"
    try:
        h, m_  = time_str.split(":")
        assert 0 <= int(h) <= 23 and 0 <= int(m_) <= 59
        time_str = f"{int(h):02d}:{int(m_):02d}"
    except Exception:
        await message.answer("❌ Формат: HH:MM, например 08:30")
        return
    if action == "on":
        await set_daily_sub(user_id, True, time_str)
        await message.answer(f"✅ Подписка активна — каждый день в {time_str} UTC")
    elif action == "off":
        await set_daily_sub(user_id, False)
        await message.answer("❌ Подписка отключена")


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
    fb       = await get_feedback_stats()
    total_fb = fb.get("total") or 0
    pos_fb   = fb.get("positive") or 0
    sat      = pos_fb / total_fb * 100 if total_fb else 0
    tr       = await get_track_record()
    tr_s     = tr["stats"]
    wins_    = tr_s.get("wins") or 0
    loss_    = tr_s.get("losses") or 0
    wr_      = wins_/(wins_+loss_)*100 if (wins_+loss_) else 0
    r_name   = RISK_PROFILES.get(profile.get("risk","moderate"),{}).get("name","⚖️ Умеренный")
    h_name   = HORIZONS.get(profile.get("horizon","swing"),{}).get("name","📈 Свинг")
    await message.answer(
        f"📈 Моя статистика\n\n"
        f"Tier: {'👑 PRO' if user.get('tier')=='pro' else '🆓 Free'}\n"
        f"Запросов сегодня: {user.get('requests_today',0)}/{FREE_DAILY_LIMIT}\n"
        f"Профиль: {r_name} | {h_name}\n"
        f"Подписка: {'✅' if user.get('daily_sub') else '❌'}\n\n"
        f"Track Record бота: {tr_s.get('total',0)} прогнозов | Winrate: {wr_:.0f}%\n\n"
        f"Оценки: {total_fb} | Позитивных: {sat:.0f}%"
    )


# ─── /admin ───────────────────────────────────────────────────────────────────

@dp.message(Command("admin"))
async def cmd_admin(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    stats   = await get_admin_stats()
    fb      = await get_feedback_stats()
    tr      = await get_track_record()
    tr_s    = tr["stats"]
    wins_   = tr_s.get("wins") or 0
    loss_   = tr_s.get("losses") or 0
    wr_     = wins_/(wins_+loss_)*100 if (wins_+loss_) else 0
    await message.answer(
        f"🔧 ADMIN\n\n"
        f"Пользователей: {stats['total_users']} | Активных: {stats['active_week']}\n"
        f"Подписчиков: {stats['subscribers']}\n"
        f"Запросов: {stats['total_reports']}\n\n"
        f"Фидбек: {fb.get('positive',0)}+ / {fb.get('negative',0)}-\n\n"
        f"Track Record: {tr_s.get('total',0)} | Winrate: {wr_:.0f}%\n"
        f"Avg P&L: {(tr_s.get('avg_pnl') or 0):+.1f}%\n\n"
        f"Графики: {'✅ matplotlib' if charts_ok() else '❌ не установлен'}"
    )


# ─── Фидбек ───────────────────────────────────────────────────────────────────

@dp.callback_query(F.data.startswith("fb:"))
async def cb_feedback(callback: CallbackQuery):
    _, rating_str, report_type = callback.data.split(":")
    await save_feedback(callback.from_user.id, report_type, int(rating_str))
    await callback.answer("🙏 Спасибо!" if int(rating_str)==1 else "📝 Учтём!")
    await callback.message.edit_reply_markup(reply_markup=None)


# ─── Запуск ───────────────────────────────────────────────────────────────────

async def main():
    global scheduler
    await init_db()
    await init_profiles_table()
    logger.info("🚀 Dialectic Edge v7.0 starting...")
    if charts_ok():
        logger.info("✅ matplotlib — графики активны")
    else:
        logger.warning("⚠️ matplotlib не установлен — pip install matplotlib")

    scheduler = Scheduler(
        bot=bot,
        send_daily_fn=run_daily_analysis,
        check_predictions_fn=check_pending_predictions,
    )
    await asyncio.gather(dp.start_polling(bot), scheduler.start())


if __name__ == "__main__":
    asyncio.run(main())
