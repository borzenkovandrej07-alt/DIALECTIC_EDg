"""
Helpers for extracting a structured digest summary from full model reports.
"""

from __future__ import annotations

import re
from typing import Optional

VERDICT_LABELS = {
    "BUY": "Бычий",
    "SELL": "Медвежий",
    "NEUTRAL": "Нейтральный",
}

VERDICT_EMOJIS = {
    "BUY": "🟢",
    "SELL": "🔴",
    "NEUTRAL": "⚪️",
}

_SECTION_BREAK_RE = re.compile(r"^(?:[-_=]{3,}|[╔═]{3,}|[⚖🏆📊📋💬👀🚨⚡])")
_SYMBOL_RE = re.compile(r"\b([A-Z]{2,10}(?:=F)?)\b")


def _strip_markup(text: str) -> str:
    text = text or ""
    text = re.sub(r"\*\*(.*?)\*\*", r"\1", text)
    text = re.sub(r"\*(.*?)\*", r"\1", text)
    text = re.sub(r"_(.*?)_", r"\1", text)
    text = re.sub(r"`(.*?)`", r"\1", text)
    return text.strip()


def _clean_line(text: str) -> str:
    text = _strip_markup(text)
    text = re.sub(r"\s+", " ", text).strip(" •-—–\t")
    return text


def _parse_price(raw: str) -> Optional[float]:
    raw = (raw or "").strip()
    if not raw:
        return None
    raw = raw.replace("$", "").replace(",", "").replace(" ", "")
    raw = raw.replace("к", "K").replace("К", "K")
    if raw.upper().endswith("K"):
        try:
            return float(raw[:-1]) * 1000
        except ValueError:
            return None
    try:
        return float(raw)
    except ValueError:
        return None


def _format_price(value: Optional[float]) -> str:
    if value is None:
        return "—"
    if abs(value - round(value)) < 0.01:
        return f"${value:,.0f}"
    if abs(value) >= 100:
        return f"${value:,.2f}"
    return f"${value:,.4f}"


def _unique_lines(lines: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for line in lines:
        key = _clean_line(line)
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


def normalize_verdict(value: str | None) -> str:
    text = (value or "").upper()
    if any(token in text for token in ("БЫЧ", "BULL", "LONG", "BUY")):
        return "BUY"
    if any(token in text for token in ("МЕДВ", "BEAR", "SHORT", "SELL")):
        return "SELL"
    return "NEUTRAL"


def extract_verdict(report_text: str) -> str:
    clean = _strip_markup(report_text)

    for pattern in (
        r"ВЕРДИКТ СУДЬИ\s*[:：]\s*([^\n]+)",
        r"ВЕРДИКТ\s*[:：]\s*([^\n]+)",
    ):
        match = re.search(pattern, clean, re.IGNORECASE)
        if match:
            return normalize_verdict(match.group(1))

    synth_markers = (
        "ВЕРДИКТ И ТОРГОВЫЙ ПЛАН",
        "ИТОГОВЫЙ СИНТЕЗ",
    )
    upper = clean.upper()
    for marker in synth_markers:
        idx = upper.find(marker)
        if idx != -1:
            return normalize_verdict(clean[idx : idx + 1200])

    return "NEUTRAL"


def _extract_single_line(report_text: str, label: str) -> str:
    clean = _strip_markup(report_text)
    pattern = rf"{label}\s*[:：]\s*([^\n]+)"
    match = re.search(pattern, clean, re.IGNORECASE)
    return _clean_line(match.group(1)) if match else ""


def extract_plain_language(report_text: str) -> str:
    clean = _strip_markup(report_text)
    markers = (
        "ПРОСТЫМИ СЛОВАМИ",
        "💬 ПРОСТЫМИ СЛОВАМИ",
    )

    for marker in markers:
        idx = clean.upper().find(marker)
        if idx == -1:
            continue
        block = clean[idx + len(marker) : idx + len(marker) + 900]
        lines: list[str] = []
        for raw_line in block.splitlines():
            line = _clean_line(raw_line)
            if not line:
                if lines:
                    break
                continue
            if _SECTION_BREAK_RE.match(line) and lines:
                break
            if len(line) < 8:
                continue
            lines.append(line)
            if len(" ".join(lines)) >= 320:
                break
        if lines:
            return " ".join(lines)[:360]
    return ""


def _extract_symbol(label: str) -> str:
    cleaned = _clean_line(label)
    if not cleaned:
        return ""
    if "CASH" in cleaned.upper():
        return "CASH"
    paren_match = re.search(r"\(([A-Z]{1,10}(?:=F)?)\)", cleaned)
    if paren_match:
        return paren_match.group(1)
    match = _SYMBOL_RE.search(cleaned)
    if match:
        return match.group(1)
    return cleaned[:24]


def _normalize_direction(value: str) -> str:
    upper = _clean_line(value).upper()
    if any(token in upper for token in ("LONG", "BUY", "БЫЧ")):
        return "LONG"
    if any(token in upper for token in ("SHORT", "SELL", "МЕДВ", "ШОРТ")):
        return "SHORT"
    if "CASH" in upper or "ВНЕ РЫНКА" in upper:
        return "CASH"
    if "WATCH" in upper or "НАБЛЮД" in upper:
        return "WATCH"
    return upper[:20]


def _parse_pipe_plan_line(line: str) -> Optional[dict]:
    cleaned = _clean_line(line)
    if "|" not in cleaned:
        return None

    parts = [part.strip() for part in cleaned.split("|") if part.strip()]
    if len(parts) < 2:
        return None

    direction = _normalize_direction(parts[1])
    if direction not in {"LONG", "SHORT", "CASH", "WATCH"}:
        return None

    plan = {
        "label": parts[0],
        "symbol": _extract_symbol(parts[0]),
        "direction": direction,
        "entry": None,
        "stop": None,
        "target": None,
        "horizon": "",
        "trigger": "",
        "rr": "",
        "size": "",
    }

    for chunk in parts[2:]:
        chunk_clean = _clean_line(chunk)
        lower = chunk_clean.lower()
        if "вход" in lower or "entry" in lower:
            price_match = re.search(r"\$?([\d.,KkКк]+)", chunk_clean)
            if price_match:
                plan["entry"] = _parse_price(price_match.group(1))
        elif "стоп" in lower or "stop" in lower:
            price_match = re.search(r"\$?([\d.,KkКк]+)", chunk_clean)
            if price_match:
                plan["stop"] = _parse_price(price_match.group(1))
        elif "цель" in lower or "target" in lower or "тейк" in lower:
            price_match = re.search(r"\$?([\d.,KkКк]+)", chunk_clean)
            if price_match:
                plan["target"] = _parse_price(price_match.group(1))
        elif "горизонт" in lower or "horizon" in lower:
            plan["horizon"] = chunk_clean.split(":", 1)[-1].strip()
        elif "триггер" in lower or "trigger" in lower:
            plan["trigger"] = chunk_clean.split(":", 1)[-1].strip()
        elif "r/r" in lower:
            plan["rr"] = chunk_clean.replace("R/R", "").replace("r/r", "").strip(": ")
        elif "размер" in lower or "size" in lower:
            plan["size"] = chunk_clean.split(":", 1)[-1].strip()

    return plan


def _extract_pipe_plans(report_text: str) -> list[dict]:
    plans: list[dict] = []
    for raw_line in _strip_markup(report_text).splitlines():
        line = raw_line.strip()
        if "|" not in line:
            continue
        if not any(marker in line.upper() for marker in ("LONG", "SHORT", "CASH", "WATCH", "ВНЕ РЫНКА", "НАБЛЮД")):
            continue
        plan = _parse_pipe_plan_line(line)
        if plan:
            plans.append(plan)
    return plans


def _extract_block_plans(report_text: str) -> list[dict]:
    lines = [_clean_line(line) for line in _strip_markup(report_text).splitlines()]
    plans: list[dict] = []
    current: Optional[dict] = None

    for line in lines:
        if not line:
            if current and current.get("symbol"):
                plans.append(current)
            current = None
            continue

        lower = line.lower()
        if lower.startswith("актив:") or lower.startswith("asset:"):
            if current and current.get("symbol"):
                plans.append(current)
            label = line.split(":", 1)[1].strip()
            current = {
                "label": label,
                "symbol": _extract_symbol(label),
                "direction": "",
                "entry": None,
                "stop": None,
                "target": None,
                "horizon": "",
                "trigger": "",
                "rr": "",
                "size": "",
            }
            continue

        if not current:
            continue

        if lower.startswith("направление:") or lower.startswith("direction:"):
            current["direction"] = _normalize_direction(line.split(":", 1)[1])
        elif lower.startswith("вход:") or lower.startswith("entry:"):
            current["entry"] = _parse_price(line.split(":", 1)[1])
        elif lower.startswith("стоп:") or lower.startswith("stop:"):
            current["stop"] = _parse_price(line.split(":", 1)[1])
        elif lower.startswith("цель:") or lower.startswith("target:") or lower.startswith("тейк:"):
            current["target"] = _parse_price(line.split(":", 1)[1])
        elif lower.startswith("горизонт:") or lower.startswith("horizon:"):
            current["horizon"] = line.split(":", 1)[1].strip()
        elif lower.startswith("триггер:") or lower.startswith("trigger:"):
            current["trigger"] = line.split(":", 1)[1].strip()
        elif lower.startswith("r/r:"):
            current["rr"] = line.split(":", 1)[1].strip()
        elif lower.startswith("размер:") or lower.startswith("size:"):
            current["size"] = line.split(":", 1)[1].strip()

    if current and current.get("symbol"):
        plans.append(current)

    normalized: list[dict] = []
    for plan in plans:
        direction = _normalize_direction(plan.get("direction", ""))
        if direction not in {"LONG", "SHORT", "CASH", "WATCH"}:
            continue
        plan["direction"] = direction
        normalized.append(plan)
    return normalized


def extract_trade_plans(report_text: str) -> list[dict]:
    plans = _extract_pipe_plans(report_text) + _extract_block_plans(report_text)
    unique: list[dict] = []
    seen: set[tuple] = set()

    for plan in plans:
        key = (
            plan.get("symbol") or plan.get("label"),
            plan.get("direction"),
            round(plan.get("entry") or 0, 4),
            round(plan.get("target") or 0, 4),
            round(plan.get("stop") or 0, 4),
        )
        if key in seen:
            continue
        seen.add(key)
        unique.append(plan)

    return unique


def extract_monitoring_points(report_text: str) -> list[str]:
    points = [
        _extract_single_line(report_text, "Ключевой триггер для пересмотра"),
        _extract_single_line(report_text, "Следующий уровень для мониторинга"),
    ]

    clean = _strip_markup(report_text)
    trigger_lines = re.findall(r"(Триггер\s+(?:LONG|SHORT)[^.\n]{0,180})", clean, re.IGNORECASE)
    points.extend(trigger_lines[:3])
    return _unique_lines([point for point in points if point])


def _plan_line(plan: dict) -> str:
    direction = plan.get("direction", "")
    symbol = plan.get("symbol") or plan.get("label") or "Актив"
    chunks = [f"{symbol} {direction}".strip()]

    if plan.get("entry") is not None:
        chunks.append(f"вход {_format_price(plan['entry'])}")
    if plan.get("target") is not None:
        chunks.append(f"цель {_format_price(plan['target'])}")
    if plan.get("stop") is not None:
        chunks.append(f"стоп {_format_price(plan['stop'])}")
    if plan.get("horizon"):
        chunks.append(f"горизонт {plan['horizon']}")
    if plan.get("trigger"):
        chunks.append(f"триггер {plan['trigger']}")
    return " | ".join(chunks)


def build_digest_context(report_text: str, source_news: str = "") -> dict:
    verdict = extract_verdict(report_text)
    plans = extract_trade_plans(report_text)
    monitoring_points = extract_monitoring_points(report_text)
    verdict_reason = _extract_single_line(report_text, "Потому что")
    key_trigger = _extract_single_line(report_text, "Ключевой триггер для пересмотра")
    monitoring_level = _extract_single_line(report_text, "Следующий уровень для мониторинга")
    plain_language = extract_plain_language(report_text) or _clean_line(source_news)[:320]

    return {
        "verdict": verdict,
        "verdict_label": VERDICT_LABELS.get(verdict, VERDICT_LABELS["NEUTRAL"]),
        "verdict_emoji": VERDICT_EMOJIS.get(verdict, VERDICT_EMOJIS["NEUTRAL"]),
        "verdict_reason": verdict_reason,
        "key_trigger": key_trigger,
        "monitoring_level": monitoring_level,
        "monitoring_points": monitoring_points,
        "plain_language": plain_language,
        "plans": plans,
        "full_report": report_text or "",
    }


def format_digest_cache_summary(context: dict, max_plans: int = 4) -> str:
    lines = [f"Вердикт: {context.get('verdict_label', VERDICT_LABELS['NEUTRAL'])}"]

    reason = context.get("verdict_reason")
    if reason:
        lines.append(f"Почему: {reason}")

    plans = context.get("plans") or []
    if plans:
        lines.append("План:")
        for plan in plans[:max_plans]:
            lines.append(f"- {_plan_line(plan)}")
    else:
        lines.append("План: явной сделки нет, работаем только от триггеров наблюдения.")

    monitoring_points = context.get("monitoring_points") or []
    if monitoring_points:
        lines.append("Точки наблюдения:")
        for point in monitoring_points[:3]:
            lines.append(f"- {point}")

    plain_language = context.get("plain_language")
    if plain_language:
        lines.append(f"Простыми словами: {plain_language}")

    return "\n".join(lines)


def format_digest_telegram_summary(
    context: dict,
    *,
    stars: str,
    pct: int,
    timestamp: str,
    max_plans: int = 4,
) -> str:
    verdict = context.get("verdict", "NEUTRAL")
    verdict_label = context.get("verdict_label", VERDICT_LABELS["NEUTRAL"])
    verdict_emoji = context.get("verdict_emoji", VERDICT_EMOJIS["NEUTRAL"])

    lines = [
        "📊 DIALECTIC EDGE — ЕЖЕДНЕВНЫЙ ДАЙДЖЕСТ",
        f"🕒 {timestamp}",
        "",
        f"🎯 *Вердикт:* {verdict_emoji} *{verdict_label}*",
        f"📊 Сигнал: {stars} ({pct}%)",
    ]

    reason = context.get("verdict_reason")
    if reason:
        lines.extend(["", f"🧠 *Почему:* {reason}"])

    plans = context.get("plans") or []
    lines.append("")
    lines.append("📋 *Торговый план:*")
    if plans:
        for plan in plans[:max_plans]:
            lines.append(f"• {_plan_line(plan)}")
    else:
        lines.append("• Явной сделки нет, ждём подтверждения по триггерам ниже.")

    monitoring_points = context.get("monitoring_points") or []
    if monitoring_points:
        lines.append("")
        lines.append("👀 *Точки наблюдения:*")
        for point in monitoring_points[:3]:
            lines.append(f"• {point}")

    plain_language = context.get("plain_language")
    if plain_language:
        lines.extend(["", f"💬 *Простыми словами:* {plain_language}"])

    lines.extend(["", "📜 Полный raw-ответ модели и полные дебаты доступны кнопками ниже."])
    return "\n".join(lines)
