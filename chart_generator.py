"""
chart_generator.py — Генерация графиков для Dialectic Edge.

Графики:
1. Bull/Bear баланс — горизонтальный бар
2. Сценарии — круговая диаграмма (базовый/бычий/медвежий %)
3. Мини-дашборд — ключевые метрики рынка

Используется в /daily и /analyze.
Возвращает BytesIO — готов к отправке через bot.send_photo().
"""

import io
import logging
import re
from datetime import datetime

logger = logging.getLogger(__name__)

# Пробуем импортировать matplotlib
try:
    import matplotlib
    matplotlib.use("Agg")  # без GUI, для сервера
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    from matplotlib.gridspec import GridSpec
    MATPLOTLIB_OK = True
except ImportError:
    MATPLOTLIB_OK = False
    logger.warning("matplotlib не установлен — графики недоступны")


# ─── Цветовая схема (тёмная, профессиональная) ────────────────────────────────

COLORS = {
    "bg":       "#0D1117",   # фон
    "surface":  "#161B22",   # карточки
    "border":   "#30363D",   # границы
    "bull":     "#3FB950",   # зелёный — бычий
    "bear":     "#F85149",   # красный — медвежий
    "neutral":  "#8B949E",   # серый — нейтральный
    "gold":     "#D4A520",   # золотой — акценты
    "text":     "#C9D1D9",   # основной текст
    "subtext":  "#8B949E",   # второстепенный текст
    "blue":     "#58A6FF",   # синий
}


def _setup_dark_style():
    """Применяет тёмную тему ко всем графикам."""
    plt.rcParams.update({
        "figure.facecolor":  COLORS["bg"],
        "axes.facecolor":    COLORS["surface"],
        "axes.edgecolor":    COLORS["border"],
        "axes.labelcolor":   COLORS["text"],
        "xtick.color":       COLORS["subtext"],
        "ytick.color":       COLORS["subtext"],
        "text.color":        COLORS["text"],
        "grid.color":        COLORS["border"],
        "grid.alpha":        0.5,
        "font.family":       "DejaVu Sans",
        "font.size":         10,
    })


def _to_bytes(fig) -> io.BytesIO:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight",
                facecolor=COLORS["bg"])
    buf.seek(0)
    plt.close(fig)
    return buf


# ─── Парсинг отчёта ───────────────────────────────────────────────────────────

def _parse_scenarios(report: str) -> dict:
    """Извлекает % сценариев из текста отчёта."""
    scenarios = {"Базовый": 50, "Бычий": 25, "Медвежий": 25}
    patterns = [
        (r"БАЗОВЫЙ[^(]*\((\d+)%\)",     "Базовый"),
        (r"БЫЧИЙ[^(]*\((\d+)%\)",       "Бычий"),
        (r"МЕДВЕЖИЙ[^(]*\((\d+)%\)",    "Медвежий"),
        (r"базовый[^(]*\((\d+)%\)",     "Базовый"),
        (r"бычий[^(]*\((\d+)%\)",       "Бычий"),
        (r"медвежий[^(]*\((\d+)%\)",    "Медвежий"),
    ]
    for pattern, key in patterns:
        m = re.search(pattern, report, re.IGNORECASE)
        if m:
            scenarios[key] = int(m.group(1))
    return scenarios


def _parse_bull_bear_score(report: str) -> tuple[float, float]:
    """
    Грубая оценка веса аргументов Bull vs Bear из текста.
    Считаем количество подтверждённых аргументов каждой стороны.
    """
    bull_signals = [
        "бычий", "рост", "покупать", "long", "восстановлени",
        "позитивный", "сильный сигнал", "точка входа"
    ]
    bear_signals = [
        "медвежий", "падение", "продавать", "short", "риск",
        "давление", "коррекция", "стагфляци"
    ]
    text = report.lower()
    bull = sum(text.count(s) for s in bull_signals)
    bear = sum(text.count(s) for s in bear_signals)
    total = bull + bear or 1
    return round(bull / total * 100, 1), round(bear / total * 100, 1)


# ─── График 1: Главный дашборд ────────────────────────────────────────────────

def generate_main_chart(report: str, prices: dict, stars: str, pct: int) -> io.BytesIO | None:
    """
    Главный дашборд — отправляется пользователю вместе с кратким анализом.

    Содержит:
      - Bull/Bear баланс (горизонтальный бар)
      - Сценарии (пончик)
      - Ключевые цены (таблица)
      - Уровень сигнала
    """
    if not MATPLOTLIB_OK:
        logger.warning("matplotlib not available")
        return None

    # Защита от пустого prices
    if not prices:
        logger.warning("generate_main_chart: prices пустой — пропускаю")
        prices = {}

    try:
        _setup_dark_style()
        fig = plt.figure(figsize=(10, 6), facecolor=COLORS["bg"])
        gs  = GridSpec(2, 2, figure=fig, hspace=0.45, wspace=0.35,
                       left=0.08, right=0.95, top=0.88, bottom=0.08)

        now   = datetime.now().strftime("%d.%m.%Y %H:%M")
        bull_pct, bear_pct = _parse_bull_bear_score(report)
        scenarios           = _parse_scenarios(report)

        # ── Заголовок ─────────────────────────────────────────────────────────
        fig.text(0.5, 0.95, "DIALECTIC EDGE — MARKET ANALYSIS",
                 ha="center", va="top", fontsize=13, fontweight="bold",
                 color=COLORS["gold"])
        fig.text(0.5, 0.91, f"{now}   |   Сигнал: {stars} ({pct}%)",
                 ha="center", va="top", fontsize=9, color=COLORS["subtext"])

        # ── 1. Bull/Bear баланс ───────────────────────────────────────────────
        ax1 = fig.add_subplot(gs[0, 0])
        ax1.set_title("Баланс аргументов", color=COLORS["text"],
                      fontsize=10, pad=8)
        ax1.barh([""], [bull_pct], color=COLORS["bull"], height=0.5,
                 label=f"Bull {bull_pct:.0f}%")
        ax1.barh([""], [bear_pct], left=[bull_pct],
                 color=COLORS["bear"], height=0.5,
                 label=f"Bear {bear_pct:.0f}%")
        ax1.set_xlim(0, 100)
        ax1.set_xlabel("% аргументов", fontsize=8)
        ax1.axvline(50, color=COLORS["border"], linewidth=1, linestyle="--")
        ax1.text(bull_pct / 2, 0, f"{bull_pct:.0f}%",
                 ha="center", va="center", fontsize=9,
                 color="white", fontweight="bold")
        ax1.text(bull_pct + bear_pct / 2, 0, f"{bear_pct:.0f}%",
                 ha="center", va="center", fontsize=9,
                 color="white", fontweight="bold")
        ax1.set_yticks([])
        ax1.legend(loc="upper right", fontsize=7,
                   facecolor=COLORS["surface"], edgecolor=COLORS["border"],
                   labelcolor=COLORS["text"])

        # ── 2. Сценарии (пончик) ──────────────────────────────────────────────
        ax2 = fig.add_subplot(gs[0, 1])
        ax2.set_title("Вероятность сценариев", color=COLORS["text"],
                      fontsize=10, pad=8)
        labels = list(scenarios.keys())
        sizes  = list(scenarios.values())
        colors_pie = [COLORS["neutral"], COLORS["bull"], COLORS["bear"]]
        wedges, texts, autotexts = ax2.pie(
            sizes,
            labels=labels,
            colors=colors_pie,
            autopct="%1.0f%%",
            startangle=90,
            wedgeprops={"edgecolor": COLORS["bg"], "linewidth": 2},
            pctdistance=0.75,
            textprops={"color": COLORS["text"], "fontsize": 8},
        )
        for at in autotexts:
            at.set_color("white")
            at.set_fontsize(8)
            at.set_fontweight("bold")

        # ── 3. Цены ───────────────────────────────────────────────────────────
        ax3 = fig.add_subplot(gs[1, 0])
        ax3.set_title("Ключевые активы", color=COLORS["text"],
                      fontsize=10, pad=8)
        ax3.axis("off")

        rows = []
        labels_map = [
            ("BTC",     "Bitcoin",  "$", ","),
            ("ETH",     "Ethereum", "$", ","),
            ("SPX",     "S&P 500",  "",  ","),
            ("OIL_WTI", "Нефть WTI","$", ".2f"),
            ("GOLD",    "Золото",   "$", ","),
        ]
        for key, name, prefix, fmt in labels_map:
            if key in prices:
                p  = prices[key]
                pr = p["price"]
                ch = p["change_24h"]
                if fmt == ",":
                    p_str = f"{prefix}{pr:,.0f}"
                else:
                    p_str = f"{prefix}{pr:,.2f}"
                arrow = "▲" if ch >= 0 else "▼"
                color = COLORS["bull"] if ch >= 0 else COLORS["bear"]
                rows.append((name, p_str, f"{arrow}{abs(ch):.2f}%", color))

        y = 0.95
        ax3.text(0.0,  y, "Актив",  transform=ax3.transAxes,
                 fontsize=8, color=COLORS["subtext"], fontweight="bold")
        ax3.text(0.45, y, "Цена",   transform=ax3.transAxes,
                 fontsize=8, color=COLORS["subtext"], fontweight="bold")
        ax3.text(0.78, y, "24ч",    transform=ax3.transAxes,
                 fontsize=8, color=COLORS["subtext"], fontweight="bold")
        # axhline не поддерживает transform — рисуем линию через plot в осевых координатах
        ax3.plot([0, 1], [y - 0.04, y - 0.04], color=COLORS["border"],
                 linewidth=0.5, transform=ax3.transAxes, clip_on=False)

        for i, (name, price_str, chg_str, c) in enumerate(rows):
            yi = y - 0.16 - i * 0.16
            ax3.text(0.0,  yi, name,      transform=ax3.transAxes,
                     fontsize=8.5, color=COLORS["text"])
            ax3.text(0.45, yi, price_str, transform=ax3.transAxes,
                     fontsize=8.5, color=COLORS["text"])
            ax3.text(0.78, yi, chg_str,   transform=ax3.transAxes,
                     fontsize=8.5, color=c, fontweight="bold")

        # ── 4. Уровень сигнала + Fear&Greed ───────────────────────────────────
        ax4 = fig.add_subplot(gs[1, 1])
        ax4.set_title("Индикаторы", color=COLORS["text"], fontsize=10, pad=8)
        # Используем обычную систему координат данных (не transAxes) для barh
        ax4.set_xlim(0, 100)
        ax4.set_ylim(0, 3)
        ax4.axis("off")

        # Уровень сигнала (y=2.4)
        signal_color = (COLORS["bull"] if pct >= 60 else
                        COLORS["bear"] if pct <= 35 else
                        COLORS["gold"])
        ax4.barh([2.4], [pct],       height=0.3, color=signal_color, left=0)
        ax4.barh([2.4], [100 - pct], height=0.3, color=COLORS["border"], left=pct)
        ax4.text(0, 2.75, f"Уровень сигнала: {pct}%",
                 fontsize=8.5, color=COLORS["text"])

        # Fear & Greed (y=1.5)
        macro = prices.get("MACRO", {})
        fng   = macro.get("fng", {}) if isinstance(macro, dict) else {}
        fv    = fng.get("val", "N/A")
        fs    = fng.get("status", "")
        if isinstance(fv, int):
            fng_color = (COLORS["bear"] if fv <= 25 else
                         COLORS["bull"] if fv >= 60 else
                         COLORS["gold"])
            ax4.barh([1.5], [fv],       height=0.3, color=fng_color, left=0)
            ax4.barh([1.5], [100 - fv], height=0.3, color=COLORS["border"], left=fv)
            ax4.text(0, 1.85, f"Fear & Greed: {fv}/100 ({fs})",
                     fontsize=8.5, color=COLORS["text"])

        # VIX (y=0.6)
        if "VIX" in prices:
            vix_val   = prices["VIX"]["price"]
            vix_color = (COLORS["bear"] if vix_val > 30 else
                         COLORS["gold"] if vix_val > 20 else
                         COLORS["bull"])
            vix_label = ("Высокая волатильность" if vix_val > 30 else
                         "Умеренная" if vix_val > 20 else
                         "Низкая волатильность")
            ax4.text(0, 0.9, f"VIX: {vix_val:.2f} — {vix_label}",
                     fontsize=8.5, color=vix_color, fontweight="bold")

        # Дисклеймер внизу
        fig.text(0.5, 0.01,
                 "⚠️ Не является финансовым советом. AI-анализ. DYOR.",
                 ha="center", fontsize=7, color=COLORS["subtext"])

        return _to_bytes(fig)

    except Exception as e:
        logger.error(f"Chart error: {e}", exc_info=True)
        return None


# ─── График 2: Russia Edge ────────────────────────────────────────────────────

def generate_russia_chart(russia_report: str) -> io.BytesIO | None:
    """
    График для /russia — риски vs возможности.
    """
    if not MATPLOTLIB_OK:
        return None

    try:
        _setup_dark_style()
        fig, axes = plt.subplots(1, 2, figsize=(10, 4),
                                 facecolor=COLORS["bg"])
        fig.suptitle("🇷🇺 RUSSIA EDGE — Анализ рисков и возможностей",
                     color=COLORS["gold"], fontsize=12, fontweight="bold",
                     y=1.02)

        # Парсим возможности и риски
        opportunities = _parse_russia_items(russia_report, "🟢")
        risks         = _parse_russia_items(russia_report, "🔴")

        # Возможности
        ax1 = axes[0]
        ax1.set_title("Возможности", color=COLORS["bull"], fontsize=10, pad=8)
        if opportunities:
            names   = [o["name"][:22] for o in opportunities[:5]]
            ratings = [o["rating"] for o in opportunities[:5]]
            colors  = [COLORS["bull"] if r >= 3 else COLORS["gold"]
                       for r in ratings]
            bars = ax1.barh(range(len(names)), ratings, color=colors,
                            height=0.6)
            ax1.set_yticks(range(len(names)))
            ax1.set_yticklabels(names, fontsize=8)
            ax1.set_xlim(0, 3.5)
            ax1.set_xlabel("Уверенность", fontsize=8)
            ax1.set_xticks([1, 2, 3])
            ax1.set_xticklabels(["НИЗКАЯ", "СРЕДНЯЯ", "ВЫСОКАЯ"], fontsize=7)
            for bar, r in zip(bars, ratings):
                ax1.text(bar.get_width() + 0.05, bar.get_y() + bar.get_height()/2,
                         "★" * r, va="center", fontsize=8, color=COLORS["gold"])
        else:
            ax1.text(0.5, 0.5, "Данные\nне найдены",
                     ha="center", va="center", transform=ax1.transAxes,
                     color=COLORS["subtext"], fontsize=10)

        # Риски
        ax2 = axes[1]
        ax2.set_title("Риски", color=COLORS["bear"], fontsize=10, pad=8)
        if risks:
            names   = [r["name"][:22] for r in risks[:5]]
            ratings = [r["rating"] for r in risks[:5]]
            colors  = [COLORS["bear"] if rv >= 3 else COLORS["gold"]
                       for rv in ratings]
            bars = ax2.barh(range(len(names)), ratings, color=colors,
                            height=0.6)
            ax2.set_yticks(range(len(names)))
            ax2.set_yticklabels(names, fontsize=8)
            ax2.set_xlim(0, 3.5)
            ax2.set_xlabel("Вероятность", fontsize=8)
            ax2.set_xticks([1, 2, 3])
            ax2.set_xticklabels(["НИЗКАЯ", "СРЕДНЯЯ", "ВЫСОКАЯ"], fontsize=7)
            for bar, rv in zip(bars, ratings):
                ax2.text(bar.get_width() + 0.05, bar.get_y() + bar.get_height()/2,
                         "⚠️" if rv >= 3 else "!", va="center", fontsize=8,
                         color=COLORS["bear"])
        else:
            ax2.text(0.5, 0.5, "Данные\nне найдены",
                     ha="center", va="center", transform=ax2.transAxes,
                     color=COLORS["subtext"], fontsize=10)

        for ax in axes:
            ax.invert_yaxis()

        plt.tight_layout()
        return _to_bytes(fig)

    except Exception as e:
        logger.error(f"Russia chart error: {e}", exc_info=True)
        return None


def _parse_russia_items(text: str, marker: str) -> list[dict]:
    """
    Парсит возможности (🟢) или риски (🔴) из Russia Edge отчёта.
    marker = "🟢" для возможностей, "🔴" для рисков.
    Берём только блок между нужным маркером и следующим разделом.
    """
    import re as _re
    items = []
    rating_map = {"ВЫСОКАЯ": 3, "СРЕДНЯЯ": 2, "НИЗКАЯ": 1}

    # Находим нужный блок по маркеру
    start = text.find(marker)
    if start == -1:
        return items

    # Находим конец блока — следующий маркер другого типа или итог
    other_marker = "🔴" if marker == "🟢" else "🟢"
    end_markers = [other_marker, "🇷🇺 ИТОГ", "──────"]
    end = len(text)
    for em in end_markers:
        pos = text.find(em, start + 5)
        if pos != -1 and pos < end:
            end = pos

    block = text[start:end]
    lines = block.split("\n")
    current_name = None

    for line in lines:
        line = line.strip()
        if line.startswith("•") and len(line) > 5:
            raw_name = line.lstrip("• ").split("\n")[0][:50]
            raw_name = _re.sub(r"[*_`]", "", raw_name).strip()
            current_name = raw_name[:28]
        if current_name and ("Уверенность:" in line or "Вероятность:" in line):
            for key, val in rating_map.items():
                if key in line:
                    # Пропускаем низкую уверенность — слабые сигналы не нужны
                    if val >= 2:
                        items.append({"name": current_name, "rating": val})
                    current_name = None
                    break

    return items


# ─── Публичный интерфейс ──────────────────────────────────────────────────────

def is_available() -> bool:
    return MATPLOTLIB_OK
