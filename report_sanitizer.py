"""
report_sanitizer.py — пост-фильтр ответов агентов.

УЛУЧШЕНО:
- Фильтр иероглифов (китайские/японские символы от Llama)
- Запрет конкретных ставок банков ("Тинькофф 21.5%", "Сбер 20%" и тд)
- Запрет исторических галлюцинаций
"""

from __future__ import annotations

import logging
import re
import unicodedata

logger = logging.getLogger(__name__)

# ─── Паттерны для удаления строк ────────────────────────────────────────────

_LINE_PATTERNS: list[re.Pattern[str]] = [
    # Исторические галлюцинации
    re.compile(r"исторически\b", re.IGNORECASE),
    re.compile(r"историческ(ая|ий|ое|ие)\s+(точк|сигнал|аналог|уровн)", re.IGNORECASE),
    re.compile(r"\bв\s+20(1\d|2[0-3])\s+году\b", re.IGNORECASE),
    re.compile(r"март[еу]?\s+2020", re.IGNORECASE),
    re.compile(r"в\s+марте\s+2020", re.IGNORECASE),
    re.compile(r"как\s+в\s+20\d{2}", re.IGNORECASE),
    re.compile(r"аналогично\s+прошл", re.IGNORECASE),
    re.compile(r"VIX\s+достиг.*\b80\b", re.IGNORECASE),
    re.compile(r"халвинг.*(ethereum|eth|эфир|эфира)", re.IGNORECASE),
    re.compile(r"(ethereum|eth|эфир).{0,40}халвинг", re.IGNORECASE),
    re.compile(r"EIP-4844", re.IGNORECASE),
    re.compile(r"Dencun|Прото-данкшард", re.IGNORECASE),
    re.compile(r"Fear\s*&\s*Greed.{0,120}историческ", re.IGNORECASE),
    re.compile(r"точк[ауы]\s+входа.{0,40}историческ", re.IGNORECASE),

    # Конкретные ставки банков — агент не знает актуальных данных
    re.compile(r"(Тинькофф|Тинкофф|T-Bank|ВТБ|Сбер|Альфа|Газпромбанк|Россельхоз)\s+\d+[.,]?\d*\s*%", re.IGNORECASE),
    re.compile(r"→\s+после\s+налога\s+~?\d+[.,]?\d*\s*%", re.IGNORECASE),

    # Иероглифы — Llama иногда мешает языки
    # (убираем строки содержащие CJK символы)
]

# Диапазоны CJK (китайский, японский, корейский)
_CJK_RANGES = [
    (0x4E00, 0x9FFF),   # CJK Unified Ideographs
    (0x3400, 0x4DBF),   # CJK Extension A
    (0x20000, 0x2A6DF), # CJK Extension B
    (0x2A700, 0x2B73F), # CJK Extension C
    (0xF900, 0xFAFF),   # CJK Compatibility
    (0x3000, 0x303F),   # CJK Symbols
    (0x3040, 0x309F),   # Hiragana
    (0x30A0, 0x30FF),   # Katakana
    (0xAC00, 0xD7AF),   # Korean Hangul
]


def _has_cjk(text: str) -> bool:
    """Проверяет наличие CJK символов в строке."""
    for char in text:
        cp = ord(char)
        for start, end in _CJK_RANGES:
            if start <= cp <= end:
                return True
    return False


def _remove_cjk_from_line(line: str) -> str:
    """
    Удаляет CJK символы из строки.
    Если строка состоит ТОЛЬКО из CJK — удаляем всю.
    Если CJK вкраплены — убираем только их.
    """
    if not _has_cjk(line):
        return line

    # Убираем CJK символы, оставляем остальное
    cleaned = ""
    for char in line:
        cp = ord(char)
        is_cjk = any(start <= cp <= end for start, end in _CJK_RANGES)
        if not is_cjk:
            cleaned += char

    # Если после очистки осталось мало смысла — убираем строку
    cleaned = cleaned.strip()
    if len(cleaned) < 5:
        return ""
    return cleaned


def sanitize_agent_output(text: str) -> tuple[str, int]:
    """Возвращает очищенный текст и число удалённых/изменённых строк."""
    if not text or not text.strip():
        return text, 0

    lines = text.split("\n")
    kept: list[str] = []
    removed = 0

    for line in lines:
        # 1. Проверяем паттерны для удаления
        if any(p.search(line) for p in _LINE_PATTERNS):
            removed += 1
            continue

        # 2. Обрабатываем CJK иероглифы
        if _has_cjk(line):
            cleaned_line = _remove_cjk_from_line(line)
            if cleaned_line != line:
                removed += 1
                if cleaned_line:  # если что-то осталось — сохраняем
                    kept.append(cleaned_line)
                # если пусто — строка удалена
                continue

        kept.append(line)

    out = "\n".join(kept)
    if removed:
        logger.info("report_sanitizer: удалено/исправлено %s строк(и)", removed)
    return out, removed


def sanitize_full_report(text: str) -> tuple[str, int]:
    """Тот же фильтр для целого отчёта."""
    return sanitize_agent_output(text)
