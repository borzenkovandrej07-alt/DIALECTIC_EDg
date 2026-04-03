"""
github_export.py — Экспорт прогнозов + кэш дайджестов на GitHub.

Функции:
1. export_to_github() — FORECASTS.md с историей прогнозов
2. push_digest_cache() — DIGEST_CACHE.md: каждый дайджест кэшируется
3. get_previous_digest() — возвращает прошлый анализ для сравнения агентами

При новом дайджесте агенты видят прошлый вердикт и могут оценить
был ли он верным — это улучшает качество анализа со временем.
"""

import asyncio
import logging
import os
import re
from datetime import datetime

import aiohttp

from database import get_track_record, get_pending_predictions

logger = logging.getLogger(__name__)

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")
GITHUB_REPO  = os.getenv("GITHUB_REPO", "spermoeshka/DIALECTIC_EDg")
FORECASTS_FILE   = "FORECASTS.md"
DIGEST_CACHE_FILE = "DIGEST_CACHE.md"
BACKTEST_FILE = "BACKTEST.md"

TIMEOUT = aiohttp.ClientTimeout(total=15)


# ─── Утилиты GitHub API ───────────────────────────────────────────────────────

async def _github_get(path: str) -> tuple[str, str | None]:
    """Читает файл из GitHub. Возвращает (content, sha)."""
    if not GITHUB_TOKEN:
        return "", None
    url     = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{path}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}",
               "Accept": "application/vnd.github.v3+json"}
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(url, headers=headers, timeout=TIMEOUT) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    import base64
                    content = base64.b64decode(data["content"]).decode("utf-8")
                    return content, data.get("sha")
                elif resp.status == 404:
                    return "", None  # файл не существует — ок
    except Exception as e:
        logger.debug(f"GitHub GET {path}: {e}")
    return "", None


async def _github_put(path: str, content: str, sha: str | None, message: str) -> bool:
    """Записывает файл на GitHub. При 409 (SHA конфликт) — перечитывает SHA и повторяет."""
    if not GITHUB_TOKEN:
        logger.warning("GITHUB_TOKEN не задан — экспорт пропущен")
        return False
    import base64
    url     = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{path}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}",
               "Accept": "application/vnd.github.v3+json"}

    # Пробуем до 3 раз при конфликте SHA
    for attempt in range(3):
        # При повторе — перечитываем актуальный SHA
        if attempt > 0:
            _, sha = await _github_get(path)
            await asyncio.sleep(1 * attempt)

        payload = {
            "message": message,
            "content": base64.b64encode(content.encode()).decode(),
        }
        if sha:
            payload["sha"] = sha

        try:
            async with aiohttp.ClientSession() as s:
                async with s.put(url, json=payload, headers=headers,
                                 timeout=TIMEOUT) as resp:
                    if resp.status in (200, 201):
                        return True
                    elif resp.status == 409:
                        # SHA конфликт — перечитаем и повторим
                        logger.warning(f"GitHub PUT {path} → 409 SHA конфликт, попытка {attempt+1}/3")
                        continue
                    else:
                        err = await resp.text()
                        logger.error(f"GitHub PUT {path} → {resp.status}: {err[:200]}")
                        return False
        except Exception as e:
            logger.error(f"GitHub PUT {path}: {e}")
            return False

    logger.error(f"GitHub PUT {path}: все попытки исчерпаны")
    return False


# ─── FORECASTS.md — трек-рекорд прогнозов ────────────────────────────────────

async def generate_forecasts_md() -> str:
    data     = await get_track_record()
    stats    = data["stats"]
    recent   = data["recent"]
    by_asset = data["by_asset"]
    pending  = await get_pending_predictions()

    total    = stats.get("total") or 0
    wins     = stats.get("wins") or 0
    losses   = stats.get("losses") or 0
    avg_pnl  = stats.get("avg_pnl") or 0
    best     = stats.get("best_call") or 0
    worst    = stats.get("worst_call") or 0
    win_rate = (wins / (wins + losses) * 100) if (wins + losses) > 0 else 0
    now      = datetime.now().strftime("%d.%m.%Y %H:%M")

    lines = [
        "# 📊 Dialectic Edge — Track Record",
        "",
        f"> Последнее обновление: {now}",
        "> Автоматический трекинг точности прогнозов.",
        "> ⚠️ Не является финансовым советом. DYOR.",
        "",
        "---",
        "## 🎯 Общая статистика",
        "",
        "| Метрика | Значение |",
        "|---------|----------|",
        f"| Всего прогнозов | {total} |",
        f"| ✅ Прибыльных | {wins} |",
        f"| ❌ Убыточных | {losses} |",
        f"| ⏳ Открытых | {len(pending)} |",
        f"| 🎯 Точность | **{win_rate:.1f}%** |",
        f"| 📈 Средний P&L | {avg_pnl:+.1f}% |",
        f"| 🏆 Лучший сигнал | {best:+.1f}% |",
        f"| 💀 Худший сигнал | {worst:+.1f}% |",
        "",
        "---",
    ]

    if pending:
        lines += [
            "## ⏳ Открытые прогнозы",
            "",
            "| Актив | Направление | Вход | Цель | Стоп | Дата |",
            "|-------|-------------|------|------|------|------|",
        ]
        for p in pending:
            entry  = f"${p['entry_price']:,.0f}" if p['entry_price'] else "—"
            target = f"${p['target_price']:,.0f}" if p['target_price'] else "—"
            stop   = f"${p['stop_loss']:,.0f}" if p['stop_loss'] else "—"
            date   = p['created_at'][:10] if p['created_at'] else "—"
            lines.append(
                f"| {p['asset']} | {p['direction']} | {entry} | {target} | {stop} | {date} |"
            )
        lines += ["", "---"]

    if recent:
        lines += [
            "## 📋 Последние закрытые прогнозы",
            "",
            "| Дата | Актив | Направление | Вход | Результат | P&L |",
            "|------|-------|-------------|------|-----------|-----|",
        ]
        for r in recent:
            emoji = "✅" if r['result'] == 'win' else "❌"
            entry = f"${r['entry_price']:,.0f}" if r['entry_price'] else "—"
            pnl   = f"{r['pnl_pct']:+.1f}%" if r['pnl_pct'] is not None else "—"
            date  = r['created_at'][:10] if r['created_at'] else "—"
            lines.append(
                f"| {date} | {r['asset']} | {r['direction']} | {entry} | {emoji} {r['result'].upper()} | {pnl} |"
            )
        lines += ["", "---"]

    if by_asset:
        lines += [
            "## 🏆 Точность по активам",
            "",
            "| Актив | Сигналов | Побед | Точность | Средний P&L |",
            "|-------|----------|-------|----------|-------------|",
        ]
        for a in by_asset:
            wr  = (a['wins'] / a['calls'] * 100) if a['calls'] > 0 else 0
            avg = a['avg_pnl'] or 0
            lines.append(
                f"| {a['asset']} | {a['calls']} | {a['wins']} | {wr:.0f}% | {avg:+.1f}% |"
            )
        lines += ["", "---"]

    lines += [
        "## ℹ️ О проекте",
        "",
        "**Dialectic Edge** — мультиагентная система финансового анализа.",
        "4 AI-модели: Bull (Groq/Llama), Bear (Mistral), Verifier, Synth (Mistral Large).",
        "",
        "---",
        "*Прошлая точность не гарантирует будущих результатов.*",
    ]
    return "\n".join(lines)


async def export_to_github() -> bool:
    logger.info("📤 Экспорт прогнозов на GitHub...")
    content = await generate_forecasts_md()
    _, sha  = await _github_get(FORECASTS_FILE)
    success = await _github_put(
        FORECASTS_FILE, content, sha,
        f"📊 Update track record {datetime.now().strftime('%Y-%m-%d %H:%M')} [skip ci]"
    )
    if success:
        logger.info("✅ FORECASTS.md обновлён на GitHub")
    return success


# ─── DIGEST_CACHE.md — история дайджестов со сравнением ──────────────────────

def _extract_verdict(report: str) -> str:
    """Вытаскивает вердикт и ключевые цифры из отчёта для сравнения."""
    lines = []

    verdict_markers = [
        "⚖️ *ВЕРДИКТ И ТОРГОВЫЙ ПЛАН*",
        "⚖️ ВЕРДИКТ И ТОРГОВЫЙ ПЛАН",
        "⚖️ *ИТОГОВЫЙ СИНТЕЗ И РЕКОМЕНДАЦИИ*",
        "⚖️ ИТОГОВЫЙ СИНТЕЗ И РЕКОМЕНДАЦИИ",
        "🏆 ВЕРДИКТ СУДЬИ",
        "ВЕРДИКТ СУДЬИ",
    ]
    for m in verdict_markers:
        idx = report.find(m)
        if idx != -1:
            chunk = report[idx:idx+500].split("\n")[:8]
            lines.append("**ВЕРДИКТ:**")
            for l in chunk:
                stripped = l.strip()
                if stripped:
                    lines.append(stripped)
            break

    prices_found = []
    for pattern, label in [
        (r"BTC.*?\$?([\d,]+)", "BTC"),
        (r"ETH.*?\$?([\d,]+)", "ETH"),
        (r"S&P\s*500.*?([\d,]+)", "SPX"),
        (r"Нефть.*?\$?([\d.]+)", "Oil"),
        (r"Золото.*?\$?([\d,]+)", "Gold"),
        (r"Fear.*?Greed.*?(\d+)", "F&G"),
    ]:
        m = re.search(pattern, report[:3000])
        if m:
            prices_found.append(f"{label}={m.group(1)}")
    if prices_found:
        lines.append("**Цены:** " + ", ".join(prices_found))

    for marker in ["🗣 ПРОСТЫМИ СЛОВАМИ", "ПРОСТЫМИ СЛОВАМИ"]:
        idx = report.find(marker)
        if idx != -1:
            chunk = report[idx+len(marker):idx+len(marker)+400].strip()
            chunk = re.sub(r"[*_`#]", "", chunk).strip()
            lines.append("**Простыми словами:** " + chunk[:300])
            break

    return "\n".join(lines) if lines else report[:500]


def _extract_trading_plan(report: str) -> str:
    """Извлекает торговый план из отчёта для кэширования."""
    lines = []

    plan_markers = [
        "⚖️ *ВЕРДИКТ И ТОРГОВЫЙ ПЛАН*",
        "⚖️ ВЕРДИКТ И ТОРГОВЫЙ ПЛАН",
        "⚖️ *ИТОГОВЫЙ СИНТЕЗ И РЕКОМЕНДАЦИИ*",
        "⚖️ ИТОГОВЫЙ СИНТЕЗ И РЕКОМЕНДАЦИИ",
        "ТОРГОВЫЙ ПЛАН",
        "Торговый план",
        "TRADING PLAN",
        "ПЛАН СДЕЛОК",
        "Итоговый торговый план",
    ]
    plan_start = None
    for marker in plan_markers:
        idx = report.find(marker)
        if idx != -1 and (plan_start is None or idx < plan_start):
            plan_start = idx
            break

    if plan_start is None:
        return ""

    plan_section = report[plan_start:plan_start + 4000]
    for sep in ["\n---\n", "\n___\n", "\n═══", "\n🤝 ", "\n📊 "]:
        sep_idx = plan_section.find(sep)
        if sep_idx > 200:
            plan_section = plan_section[:sep_idx]
            break

    levels = []
    for line in plan_section.split("\n"):
        line = line.strip()
        if not line or len(line) < 8:
            continue
        if any(k in line for k in ["Триггер", "Trigger", "Вход", "Entry", "Выйти", "Выход", "Цель", "Target", "Стоп", "Stop", "шорт", "лонг", "BUY", "SELL", "LONG", "SHORT"]):
            if len(line) < 150:
                levels.append(line)
            else:
                levels.append(line[:150])
        if any(k in line for k in ["пробой", "уровень", "VIX", "RSI", "MACD"]):
            if len(line) < 150:
                levels.append(line)

    if levels:
        return "\n".join(levels[:25])

    return plan_section[:2000]


async def push_digest_cache(report: str, date_str: str, full_debates: str = "") -> bool:
    """
    Сохраняет дайджест в DIGEST_CACHE.md.
    Хранит последние 14 дайджестов.
    Каждый дайджест содержит:
    - дата и полный VERDICT (как есть сейчас)
    - ТОРГОВЫЙ ПЛАН (все точки входа/выхода)
    - Полный отчёт (всё что приходит пользователю в тг)
    - Все раунды дебатов (полностью)
    """
    if not GITHUB_TOKEN:
        return False

    current_content, sha = await _github_get(DIGEST_CACHE_FILE)

    verdict_block = _extract_verdict(report)
    trading_plan = _extract_trading_plan(report)

    new_entry_parts = [
        f"## 📊 {date_str}\n\n",
        verdict_block,
    ]

    if trading_plan:
        new_entry_parts.append(
            f"\n\n**Торговый план:**\n{trading_plan}"
        )

    new_entry_parts.append(
        f"\n\n<details><summary>📋 Полный отчёт (всё что видит пользователь)</summary>\n\n"
        f"{report}\n\n</details>"
    )

    if full_debates:
        new_entry_parts.append(
            f"\n\n<details><summary>🗣 Все раунды дебатов</summary>\n\n"
            f"{full_debates}\n\n</details>"
        )

    new_entry = "".join(new_entry_parts)

    entries = re.split(r"\n## 📊 ", current_content) if current_content else []
    entries = [e.strip() for e in entries if e.strip() and not e.startswith("#")]
    entries = entries[:13]
    entries.insert(0, new_entry)

    header = (
        "# 📚 Dialectic Edge — История дайджестов\n\n"
        "> Автоматический кэш для отслеживания точности прогнозов\n"
        "> Последние 14 дайджестов\n\n"
        "---\n\n"
    )
    full_content = header + "\n\n---\n\n## 📊 ".join(entries)

    success = await _github_put(
        DIGEST_CACHE_FILE, full_content, sha,
        f"📊 Digest {date_str} [skip ci]"
    )
    if success:
        logger.info("✅ Дайджест закэширован на GitHub")
    return success


async def get_previous_digest() -> str:
    """
    Возвращает предыдущий дайджест для передачи агентам.
    Агенты используют его чтобы сравнить свои прошлые прогнозы с реальностью.
    """
    if not GITHUB_TOKEN:
        return ""
    content, _ = await _github_get(DIGEST_CACHE_FILE)
    if not content:
        return ""

    # Берём второй по счёту дайджест (первый — текущий который только что добавили)
    entries = re.split(r"\n## 📊 ", content)
    entries = [e.strip() for e in entries if e.strip() and not e.startswith("#")]

    if len(entries) < 2:
        return ""

    prev = entries[1]  # предыдущий дайджест
    # Убираем блок с полным отчётом, оставляем только вердикт
    prev = re.sub(r"<details>.*?</details>", "", prev, flags=re.DOTALL).strip()

    return (
        "=== ПРОШЛЫЙ АНАЛИЗ (для сравнения и проверки точности) ===\n"
        f"{prev}\n"
        "=== ЗАДАЧА: если прошлый вердикт оказался неверным — объясни почему. "
        "Если верным — укажи это как подтверждение сигнала. ===\n"
    )


# ─── BACKTEST.md — история бэктеста ──────────────────────────────────────────────

async def export_backtest_to_github(signals: list[dict], stats: dict, config: dict = None) -> bool:
    """Экспортирует результаты бэктеста в BACKTEST.md на GitHub."""
    if not GITHUB_TOKEN:
        return False
    
    capital = config.get("capital", 100.0) if config else 100.0
    enabled = config.get("enabled", 1) if config else 1
    
    lines = [
        "# 📊 Dialectic Edge — Backtest Results",
        f"> Обновлено: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        "",
        "## 💵 Капитал",
        f"- Текущий: **${capital:,.2f}**",
        f"- Статус: **{'✅ Включён' if enabled else '❌ Выключен'}**",
        "",
        "## 📈 Статистика (закрытые сделки)",
        "",
    ]
    
    total = stats.get("total", 0) or 0
    wins = stats.get("wins", 0) or 0
    win_rate = (wins / total * 100) if total > 0 else 0
    total_pnl = stats.get("total_pnl") or 0
    avg_pnl_pct = stats.get("avg_pnl_pct") or 0
    
    lines.extend([
        f"- Всего сделок: **{total}**",
        f"- Win Rate: **{win_rate:.1f}%**",
        f"- Total PnL: **${total_pnl:+,.2f}**",
        f"- Avg PnL: **{avg_pnl_pct:+.2f}%**",
        "",
        "## 📋 История сделок",
        "",
    ])
    
    # Only show closed trades in table
    closed_signals = [s for s in signals if s.get("status") == "closed"]
    for s in closed_signals:
        date = s.get("created_at", "")[:10] or ""
        symbol = s.get("symbol", "") or ""
        direction = s.get("direction", "") or ""
        entry = s.get("entry_price") or 0
        exit_price = s.get("exit_price") or 0
        pnl = s.get("pnl") or 0
        pnl_pct = s.get("pnl_pct") or 0
        
        lines.append(f"| {date} | {symbol} | {direction} | ${entry:,.0f} | ${exit_price:,.0f} | ${pnl:+,.0f} | {pnl_pct:+.1f}% |")
    
    # Add open positions
    open_signals = [s for s in signals if s.get("status") == "open"]
    if open_signals:
        lines.extend(["", "## 🔵 Открытые позиции", ""])
        for s in open_signals:
            date = s.get("created_at", "")[:10] or ""
            symbol = s.get("symbol", "") or ""
            direction = s.get("direction", "") or ""
            entry = s.get("entry_price") or 0
            qty = s.get("quantity") or 0
            lines.append(f"- **{symbol}** {direction} @ ${entry:,.2f} (qty: {qty:.4f}) — {date}")
    
    content = "\n".join(lines)
    
    _, sha = await _github_get(BACKTEST_FILE)
    success = await _github_put(
        BACKTEST_FILE, content, sha,
        f"📊 Update backtest {datetime.now().strftime('%Y-%m-%d %H:%M')} [skip ci]"
    )
    
    if success:
        logger.info("✅ BACKTEST.md обновлён на GitHub")
    
    return success


if __name__ == "__main__":
    asyncio.run(export_to_github())
