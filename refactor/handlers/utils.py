"""
Shared handler utilities backed by the current production report format.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import List, Optional, Tuple

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from core.digest_context import build_digest_context, format_digest_telegram_summary

logger = logging.getLogger(__name__)

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
_DEBATE_START_RES = (
    re.compile(r"🗣\s*\*?\s*ХОД\s+ДЕБАТОВ", re.IGNORECASE),
    re.compile(r"🗣\s*\*?\s*ДЕБАТЫ\s+АГЕНТОВ", re.IGNORECASE),
    re.compile(r"\*?──\*?\s*Раунд\s+1\b"),
    re.compile(r"──\s*Раунд\s+1\b"),
    re.compile(r"🐂\s*Bull\s+Researcher"),
)
_ROUND_HEADER_RE = re.compile(r"──\s*Раунд\s+\d+")
_DISCLAIMER_MARKERS = (
    "─────────────────────────\n🤝 Честно о боте:",
    "─────────────────────────\n🤝 *Честно о боте:*",
    "🤝 Честно о боте:",
    "🤝 *Честно о боте:*",
)
_SIGNAL_LABEL_MAP = {"HIGH": 0.85, "MEDIUM": 0.55, "LOW": 0.25, "EXTREME": 0.95}
SIGNAL_PCT_EXPLAINED = (
    "Число % показывает уверенность FinBERT в тоне новостей, а не гарантию движения рынка."
)


def clean_markdown(text: str) -> str:
    """Remove broken markdown fragments that often break Telegram rendering."""
    lines = text.split("\n")
    clean_lines: List[str] = []
    for line in lines:
        # Strip ### headers
        line = re.sub(r'^#{1,6}\s*', '', line)
        if line.count("*") % 2 != 0:
            line = line.replace("*", "")
        if line.count("_") % 2 != 0:
            line = line.replace("_", "")
        if line.count("`") % 2 != 0:
            line = line.replace("`", "")
        clean_lines.append(line)
    return "\n".join(clean_lines)


def debate_plain_text(text: str) -> str:
    """Strip simple markdown for debate attachments/plain display."""
    text = re.sub(r"\*\*(.*?)\*\*", r"\1", text)
    text = re.sub(r"\*(.*?)\*", r"\1", text)
    text = re.sub(r"_(.*?)_", r"\1", text)
    text = re.sub(r"`(.*?)`", r"\1", text)
    text = re.sub(r"\[(.*?)\]\((.*?)\)", r"\1", text)
    return text


def strip_digest_summary_text(text: str) -> str:
    if "📰 DIGEST" in text:
        idx = text.find("📰 DIGEST")
        if "---" in text[idx:]:
            sep_idx = text.find("---", idx)
            next_sep = text.find("---", sep_idx + 3)
            if next_sep > sep_idx:
                text = text[:sep_idx] + text[next_sep + 3 :]
    return text.strip()


def split_message(text: str, max_len: int = 3800) -> List[str]:
    """Split text into Telegram-safe chunks."""
    if len(text) <= max_len:
        return [text]

    parts: List[str] = []
    current = ""
    paragraphs = text.split("\n\n")

    for para in paragraphs:
        if len(current) + len(para) + 2 <= max_len:
            current += ("\n\n" if current else "") + para
            continue
        if current:
            parts.append(current)
            current = ""
        if len(para) <= max_len:
            current = para
            continue
        for i in range(0, len(para), max_len):
            parts.append(para[i : i + max_len])
    if current:
        parts.append(current)
    return parts


def signal_to_stars(confidence) -> str:
    if isinstance(confidence, str):
        confidence = _SIGNAL_LABEL_MAP.get(confidence.upper(), 0.5)
    try:
        confidence = float(confidence)
    except (TypeError, ValueError):
        confidence = 0.5
    stars = max(1, min(5, round(confidence * 5)))
    return "⭐" * stars + "☆" * (5 - stars)


def extract_signal_pct_and_stars(report: str) -> Tuple[int, str]:
    match = re.search(r"Уровень\s+сигнала[^\d(]*\((\d+)%", report, re.IGNORECASE)
    if not match:
        match = re.search(r"📶[^\n]{0,160}\((\d+)%", report)
    pct = int(match.group(1)) if match else 50
    pct = max(0, min(100, pct))
    return pct, signal_to_stars(pct / 100)


def _find_first_marker(text: str, markers: Tuple[str, ...]) -> Optional[Tuple[int, str]]:
    best: Optional[Tuple[int, str]] = None
    for marker in markers:
        idx = text.find(marker)
        if idx != -1 and (best is None or idx < best[0]):
            best = (idx, marker)
    return best


def find_debate_start_index(text: str) -> Optional[int]:
    hit = _find_first_marker(text, _DEBATE_START_MARKERS)
    if hit:
        return hit[0]
    best: Optional[int] = None
    for regex in _DEBATE_START_RES:
        match = regex.search(text)
        if match and (best is None or match.start() < best):
            best = match.start()
    return best


def parse_report_parts(report: str) -> dict:
    parts = {
        "header": "",
        "rounds": [],
        "synthesis": "",
        "disclaimer": "",
        "full": report,
    }

    working = report
    for disc_marker in _DISCLAIMER_MARKERS:
        if disc_marker in working:
            idx = working.find(disc_marker)
            parts["disclaimer"] = working[idx:]
            working = working[:idx]
            break

    synth_hit = _find_first_marker(working, _SYNTH_START_MARKERS)
    if synth_hit:
        idx, _ = synth_hit
        parts["synthesis"] = working[idx:].strip()
        working = working[:idx]

    round_markers_legacy = (
        "── Раунд 1:",
        "── Раунд 2:",
        "── Раунд 3:",
    )

    debate_idx = find_debate_start_index(working)
    if debate_idx is None:
        parts["header"] = working.strip()
        return parts

    parts["header"] = working[:debate_idx].strip()
    debate_section = working[debate_idx:]
    current_round = ""
    current_round_num = 0

    for line in debate_section.split("\n"):
        is_round_header = bool(_ROUND_HEADER_RE.search(line)) or any(
            marker in line for marker in round_markers_legacy
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

    if not parts["rounds"] and debate_section.strip():
        parts["rounds"] = [debate_section.strip()]

    return parts


def hydrate_debate_from_report(full_report: str) -> Optional[dict]:
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
    section = tail[: synth_hit[0]].strip() if synth_hit else tail.strip()
    if len(section) < 80:
        return None
    return {"rounds": [section], "full": full_report}


def build_short_report(parts: dict, stars: str, pct: int) -> List[str]:
    now = datetime.now().strftime("%d.%m.%Y %H:%M")
    bull_summary = "Позиция бычья"
    bear_summary = "Позиция медвежья"

    if parts["rounds"]:
        round1 = parts["rounds"][0]
        lines = round1.split("\n")
        bull_lines: List[str] = []
        bear_lines: List[str] = []
        in_bull = False
        in_bear = False
        for line in lines:
            if _ROUND_HEADER_RE.search(line):
                in_bull = False
                in_bear = False
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

    header = (
        f"📊 DIALECTIC EDGE\n"
        f"🕐 {now}\n\n"
        f"Уровень сигнала: {stars} ({pct}%)\n"
        f"{SIGNAL_PCT_EXPLAINED}\n\n"
        f"{'─' * 30}\n\n"
        f"🐂 Бычья позиция:\n{bull_summary}\n\n"
        f"🐻 Медвежья позиция:\n{bear_summary}\n\n"
        f"{'─' * 30}"
    )
    messages = [header]

    full = parts.get("full", "")
    synth_hit = _find_first_marker(full, _SYNTH_START_MARKERS)
    synth_and_rest = full[synth_hit[0] :] if synth_hit else (
        (parts.get("synthesis", "") + "\n\n" + parts.get("disclaimer", "")).strip()
    )
    for chunk in split_message(synth_and_rest, max_len=2500):
        if chunk.strip():
            messages.append(chunk)

    return messages


def debates_keyboard(user_id: int, round_idx: int, total_rounds: int) -> InlineKeyboardMarkup:
    row: List[InlineKeyboardButton] = []
    if round_idx > 0:
        row.append(
            InlineKeyboardButton(
                text="⬅️ Назад",
                callback_data=f"debate:{user_id}:{round_idx - 1}",
            )
        )
    row.append(
        InlineKeyboardButton(
            text=f"Раунд {round_idx + 1}/{total_rounds}",
            callback_data="debate:noop",
        )
    )
    if round_idx < total_rounds - 1:
        row.append(
            InlineKeyboardButton(
                text="Дальше ➡️",
                callback_data=f"debate:{user_id}:{round_idx + 1}",
            )
        )
    return InlineKeyboardMarkup(inline_keyboard=[row])


def main_report_keyboard(user_id: int, has_debates: bool = True) -> InlineKeyboardMarkup:
    buttons: List[List[InlineKeyboardButton]] = []
    if has_debates:
        buttons.append(
            [
                InlineKeyboardButton(
                    text="📖 Полные дебаты агентов",
                    callback_data=f"debate:{user_id}:0",
                )
            ]
        )
    buttons.append(
        [
            InlineKeyboardButton(text="👍 Полезно", callback_data="fb:1:daily"),
            InlineKeyboardButton(text="👎 Мимо", callback_data="fb:-1:daily"),
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=buttons)
