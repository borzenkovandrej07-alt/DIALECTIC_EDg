"""
cpi_config.py — Единая конфигурация CPI для всех модулей.

ПРОБЛЕМА была: data_sources.py и web_search.py использовали разные значения
CPI_BASE_YEAR_AGO (314.2 vs 319.8), из-за чего агенты получали разные цифры
инфляции из разных источников.

РЕШЕНИЕ: один файл — один источник правды.
Импортируй CPI_BASE_YEAR_AGO и FED_INFLATION_TARGET отсюда.

Обновляй CPI_BASE раз в год (берётся среднее CPI за год назад по BLS/FRED).
Текущее значение: март 2025 по BLS = 319.8
"""

# CPI индекс год назад (для расчёта YoY инфляции)
# Источник: BLS / FRED серия CPIAUCSL
# Обновлён: март 2025
CPI_BASE_YEAR_AGO = 319.8

# Таргет ФРС по инфляции
FED_INFLATION_TARGET = 2.0


def cpi_to_yoy(raw_cpi: float) -> dict:
    """
    Пересчитывает сырой CPI индекс (~323) в YoY % для агентов.
    Возвращает словарь с процентом, отклонением и статусом.

    Использование:
        result = cpi_to_yoy(323.5)
        # {"yoy": 1.2, "gap": -0.8, "status": "близко к таргету", "text": "~1.2% YoY ..."}
    """
    yoy = (raw_cpi - CPI_BASE_YEAR_AGO) / CPI_BASE_YEAR_AGO * 100
    gap = yoy - FED_INFLATION_TARGET
    gap_str = f"+{gap:.1f}%" if gap > 0 else f"{gap:.1f}%"

    if gap > 1.0:
        status = "🔴 значительно выше таргета"
    elif gap > 0.3:
        status = "🟠 выше таргета"
    elif gap > -0.3:
        status = "🟢 близко к таргету"
    else:
        status = "🟢 ниже таргета"

    text = (
        f"~{yoy:.1f}% YoY {status} "
        f"(таргет ФРС: {FED_INFLATION_TARGET}%, отклонение: {gap_str})"
    )

    return {
        "yoy":    round(yoy, 2),
        "gap":    round(gap, 2),
        "status": status,
        "text":   text,
    }
