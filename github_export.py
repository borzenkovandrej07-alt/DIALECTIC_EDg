"""
github_export.py — Экспорт прогнозов в текстовик для GitHub.

Создаёт FORECASTS.md — публичный файл который хранится в репо.
Каждые 2 недели обновляется автоматически через scheduler.py.

Как подключить к GitHub:
1. В репо создай файл FORECASTS.md (пустой)
2. Добавь в Railway переменные:
   GITHUB_TOKEN=ghp_xxxxxxx  (Settings → Developer → Personal access tokens)
   GITHUB_REPO=spermoeshka/DIALECTIC_EDg
3. Скрипт будет сам пушить обновления через GitHub API
"""

import asyncio
import logging
import os
from datetime import datetime

import aiohttp

from database import get_track_record, get_pending_predictions

logger = logging.getLogger(__name__)

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")
GITHUB_REPO  = os.getenv("GITHUB_REPO", "spermoeshka/DIALECTIC_EDg")
GITHUB_FILE  = "FORECASTS.md"


# ─── Генерация markdown ───────────────────────────────────────────────────────

async def generate_forecasts_md() -> str:
    """Генерирует красивый markdown с историей прогнозов."""
    data = await get_track_record()
    stats = data["stats"]
    recent = data["recent"]
    by_asset = data["by_asset"]
    pending = await get_pending_predictions()

    total    = stats.get("total") or 0
    wins     = stats.get("wins") or 0
    losses   = stats.get("losses") or 0
    avg_pnl  = stats.get("avg_pnl") or 0
    best     = stats.get("best_call") or 0
    worst    = stats.get("worst_call") or 0
    win_rate = (wins / (wins + losses) * 100) if (wins + losses) > 0 else 0

    now = datetime.now().strftime("%d.%m.%Y %H:%M")

    lines = [
        "# 📊 Dialectic Edge — Track Record",
        "",
        f"> Последнее обновление: {now}",
        "> Это автоматически обновляемый публичный трекинг точности прогнозов.",
        "> ⚠️ Не является финансовым советом. DYOR.",
        "",
        "---",
        "",
        "## 🎯 Общая статистика",
        "",
        f"| Метрика | Значение |",
        f"|---------|----------|",
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
        "",
    ]

    # Открытые прогнозы
    if pending:
        lines += [
            "## ⏳ Открытые прогнозы",
            "",
            "| Актив | Направление | Вход | Цель | Стоп | Таймфрейм | Дата |",
            "|-------|-------------|------|------|------|-----------|------|",
        ]
        for p in pending:
            entry  = f"${p['entry_price']:,.0f}" if p['entry_price'] else "—"
            target = f"${p['target_price']:,.0f}" if p['target_price'] else "—"
            stop   = f"${p['stop_loss']:,.0f}" if p['stop_loss'] else "—"
            date   = p['created_at'][:10] if p['created_at'] else "—"
            lines.append(
                f"| {p['asset']} | {p['direction']} | {entry} | {target} | {stop} | {p.get('timeframe','1w')} | {date} |"
            )
        lines += ["", "---", ""]

    # Последние результаты
    if recent:
        lines += [
            "## 📋 Последние закрытые прогнозы",
            "",
            "| Дата | Актив | Направление | Вход | Результат | P&L |",
            "|------|-------|-------------|------|-----------|-----|",
        ]
        for r in recent:
            emoji  = "✅" if r['result'] == 'win' else "❌"
            entry  = f"${r['entry_price']:,.0f}" if r['entry_price'] else "—"
            pnl    = f"{r['pnl_pct']:+.1f}%" if r['pnl_pct'] is not None else "—"
            date   = r['created_at'][:10] if r['created_at'] else "—"
            lines.append(
                f"| {date} | {r['asset']} | {r['direction']} | {entry} | {emoji} {r['result'].upper()} | {pnl} |"
            )
        lines += ["", "---", ""]

    # Статистика по активам
    if by_asset:
        lines += [
            "## 🏆 Точность по активам",
            "",
            "| Актив | Сигналов | Побед | Точность | Средний P&L |",
            "|-------|----------|-------|----------|-------------|",
        ]
        for a in by_asset:
            asset_win_rate = (a['wins'] / a['calls'] * 100) if a['calls'] > 0 else 0
            avg            = a['avg_pnl'] or 0
            lines.append(
                f"| {a['asset']} | {a['calls']} | {a['wins']} | {asset_win_rate:.0f}% | {avg:+.1f}% |"
            )
        lines += ["", "---", ""]

    lines += [
        "## ℹ️ О проекте",
        "",
        "**Dialectic Edge** — мультиагентная система финансового анализа.",
        "4 AI-модели спорят между собой: Bull (Groq/Llama), Bear (Mistral),",
        "Verifier (Mistral), Synth (Mistral Large).",
        "",
        "Telegram: @dialectic_edge",
        "",
        "---",
        "*Прошлая точность не гарантирует будущих результатов.*",
    ]

    return "\n".join(lines)


# ─── Пуш на GitHub ────────────────────────────────────────────────────────────

async def push_to_github(content: str) -> bool:
    """Обновляет FORECASTS.md в GitHub репо через API."""
    if not GITHUB_TOKEN:
        logger.warning("GITHUB_TOKEN не задан — экспорт пропущен")
        return False

    import base64
    api_url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_FILE}"
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    }

    async with aiohttp.ClientSession() as s:
        # Получаем SHA текущего файла (нужен для обновления)
        sha = None
        async with s.get(api_url, headers=headers) as resp:
            if resp.status == 200:
                data = await resp.json()
                sha = data.get("sha")

        # Пушим обновление
        payload = {
            "message": f"📊 Update track record {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            "content": base64.b64encode(content.encode()).decode(),
        }
        if sha:
            payload["sha"] = sha

        async with s.put(api_url, json=payload, headers=headers) as resp:
            if resp.status in (200, 201):
                logger.info(f"✅ FORECASTS.md обновлён на GitHub")
                return True
            else:
                err = await resp.text()
                logger.error(f"❌ GitHub push ошибка {resp.status}: {err[:200]}")
                return False


# ─── Главная функция ──────────────────────────────────────────────────────────

async def export_to_github() -> bool:
    """Генерирует markdown и пушит на GitHub."""
    logger.info("📤 Экспорт прогнозов на GitHub...")
    content = await generate_forecasts_md()
    success = await push_to_github(content)
    if success:
        logger.info("✅ Track record обновлён на GitHub")
    return success


if __name__ == "__main__":
    asyncio.run(export_to_github())


# ─── Кэш дайджестов на GitHub (п.6) ──────────────────────────────────────────

CACHE_FILE = "DIGEST_CACHE.md"

async def push_digest_cache(report: str, date_str: str) -> bool:
    """
    Сохраняет каждый дайджест в DIGEST_CACHE.md на GitHub.
    При новом дайджесте — сравнивает с предыдущим и добавляет оценку точности.
    """
    if not GITHUB_TOKEN:
        return False

    import base64
    api_url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{CACHE_FILE}"
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    }

    async with aiohttp.ClientSession() as s:
        # Читаем текущий кэш
        current_content = ""
        sha = None
        async with s.get(api_url, headers=headers) as resp:
            if resp.status == 200:
                data = await resp.json()
                sha = data.get("sha")
                import base64 as b64
                current_content = b64.b64decode(data["content"]).decode("utf-8")

        # Добавляем новый дайджест в начало
        separator = "\n\n---\n\n"
        new_entry = f"## 📊 Дайджест от {date_str}\n\n{report[:3000]}\n_(сокращено до 3000 символов)_"

        # Ограничиваем файл последними 10 дайджестами
        entries = current_content.split("---") if current_content else []
        entries = [e.strip() for e in entries if e.strip()]
        entries = entries[:9]  # оставляем последние 9
        entries.insert(0, new_entry)  # добавляем новый в начало

        header = "# 📚 Dialectic Edge — История дайджестов\n\n> Автоматический кэш для отслеживания точности прогнозов\n"
        full_content = header + separator.join(entries)

        payload = {
            "message": f"📊 Digest cache {date_str}",
            "content": base64.b64encode(full_content.encode()).decode(),
        }
        if sha:
            payload["sha"] = sha

        async with s.put(api_url, json=payload, headers=headers) as resp:
            if resp.status in (200, 201):
                logger.info("✅ Дайджест закэширован на GitHub")
                return True
            else:
                err = await resp.text()
                logger.error(f"❌ Digest cache push ошибка {resp.status}: {err[:100]}")
                return False


async def get_previous_digest() -> str:
    """Получает предыдущий дайджест из GitHub кэша для сравнения."""
    if not GITHUB_TOKEN:
        return ""

    import base64 as b64
    api_url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{CACHE_FILE}"
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    }

    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(api_url, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    content = b64.b64decode(data["content"]).decode("utf-8")
                    # Возвращаем второй дайджест (первый — текущий)
                    entries = content.split("---")
                    entries = [e.strip() for e in entries if "Дайджест от" in e]
                    if len(entries) >= 2:
                        return entries[1]
    except Exception as e:
        logger.warning(f"get_previous_digest error: {e}")
    return ""
