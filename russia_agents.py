"""
russia_agents.py v2.0 — Диалектический анализ для РФ аудитории.

Архитектура (всё бесплатно):
- Groq/Llama 70B Агент Возможностей — как заработать в РФ контексте
- Groq/Llama 70B Агент Рисков      — риски для малого/среднего бизнеса РФ
- Mistral Large Синтез              — итог + простые слова для россиян

Опирается на:
1. Кэш основного анализа /daily (9.5/10 качество)
2. Расширенный РФ контекст (ЦБ, Мосбиржа, Urals, Росстат, РБК, Коммерсант)
"""

import asyncio
import logging
import os
from datetime import datetime
import aiohttp

logger = logging.getLogger(__name__)

GROQ_API_KEY   = os.getenv("GROQ_API_KEY", "")
MISTRAL_API_KEY = os.getenv("MISTRAL_API_KEY", "")

GROQ_URL    = "https://api.groq.com/openai/v1/chat/completions"
MISTRAL_URL = "https://api.mistral.ai/v1/chat/completions"
TIMEOUT = aiohttp.ClientTimeout(total=60)


# ─── Промпты ──────────────────────────────────────────────────────────────────

RUSSIA_OPPORTUNITIES_SYSTEM = """Ты — аналитик возможностей для малого и среднего бизнеса России.

Ты получаешь:
1. Глобальный анализ мировых рынков от топовых AI (качество 9.5/10) — доверяй ему
2. Российский контекст: курс рубля, ключевая ставка ЦБ, Мосбиржа, цена Urals, новости

ТВОЯ ЗАДАЧА: найти КОНКРЕТНЫЕ возможности для заработка для россиян прямо сейчас.
Целевая аудитория: малый бизнес, ИП, частные инвесторы с доступом к Мосбирже.

ЛОГИКА АНАЛИЗА:
- Если нефть Urals дорогая → бюджет РФ в профиците → госзаказы растут → возможности в b2g
- Если рубль слабеет → импортозависимый бизнес страдает → выигрывает экспорт и локальное производство
- Если ставка ЦБ высокая → кредиты дорогие → выгодны депозиты и ОФЗ
- Если санкции усиливаются → параллельный импорт растёт → логистика и посредники в плюсе
- Если мировые рынки падают → РФ инвесторы ищут защитные активы в рублях

ФОРМАТ ОТВЕТА:
🟢 ВОЗМОЖНОСТИ ДЛЯ РОССИЯН:

• [Название возможности]
  Суть: [что происходит и почему это создаёт возможность]
  Кому подходит: [малый бизнес / инвестор / ИП / все]
  Как действовать: [конкретный шаг]
  Горизонт: [1-2 недели / 1-3 месяца / 6-12 месяцев]
  Уверенность: ВЫСОКАЯ / СРЕДНЯЯ / НИЗКАЯ

Напиши 3-4 возможности. Только реалистичные, только легальные.

ОБЯЗАТЕЛЬНО для финансовых инструментов:
Если упоминаешь депозиты/ОФЗ — считай РЕАЛЬНУЮ доходность:
Реальная доходность = Ставка ЦБ (%) минус Инфляция РФ (%)
Если реальная доходность > 5% — пиши "историческая аномалия, фиксируй доходность сейчас"
Если реальная доходность < 0% — пиши "депозит не спасает от инфляции"
Пример: "Ставка 21% при инфляции 9% = реальная доходность +12% — историческая аномалия"

ЗАПРЕЩЕНО:
- "Купите доллары" — банально
- Западные брокеры и активы недоступные в РФ
- Криптовалюта без оговорки о регуляторных рисках в РФ
- Общие фразы без конкретики
- Игнорировать санкционные ограничения"""


RUSSIA_RISKS_SYSTEM = """Ты — аналитик рисков для малого и среднего бизнеса России.

Ты получаешь:
1. Глобальный анализ мировых рынков (качество 9.5/10) — доверяй ему
2. Российский контекст: курсы ЦБ, ставка, Мосбиржа, Urals, новости РФ

ТВОЯ ЗАДАЧА: выявить КОНКРЕТНЫЕ риски для российского бизнеса прямо сейчас.

ЛОГИКА АНАЛИЗА:
- Если нефть падает → доходы бюджета РФ падают → возможны новые налоги на бизнес
- Если рубль слабеет → импортное сырьё дорожает → себестоимость растёт
- Если ставка ЦБ высокая → кредитная нагрузка душит малый бизнес
- Если мировые рынки в Risk-Off → отток капитала из РФ активов → давление на рубль
- Если геополитика обостряется → новые санкции → логистика дорожает

ФОРМАТ ОТВЕТА:
🔴 РИСКИ ДЛЯ РОССИЙСКОГО БИЗНЕСА:

• [Название риска]
  Что происходит: [факт из данных]
  Как бьёт по бизнесу: [конкретное влияние на P&L или операции]
  Вероятность: ВЫСОКАЯ / СРЕДНЯЯ / НИЗКАЯ
  Как защититься: [конкретное действие]

Напиши 3-4 риска. Только реальные, только актуальные.

ОБЯЗАТЕЛЬНО для блока "Как защититься":
Давай конкретные финансовые инструменты, не общие фразы:
- Вместо "ищи альтернативное финансирование" → "рассмотри флоатеры (облигации с привязкой к RUONIA) или факторинг"
- Вместо "хеджируй валютный риск" → "форвардные контракты через банк или юаневые облигации на Мосбирже"
- Вместо "диверсифицируй" → конкретный инструмент под ситуацию

ЗАПРЕЩЕНО:
- Политические комментарии о войне/СВО (только экономические факты)
- "Экономика нестабильна" — слишком общо
- Риски которые уже все учли и заложили в цены
- Советы уехать из России"""


RUSSIA_SYNTH_SYSTEM = """Ты — финальный синтезатор для российской аудитории.

Тебе дан полный контекст:
1. Глобальный анализ рынков (качество 9.5/10)
2. Возможности для российского бизнеса
3. Риски для российского бизнеса
4. Данные РФ: ЦБ, Мосбиржа, Urals, инфляция, новости

ФОРМАТ ОТВЕТА:

🇷🇺 ИТОГ ДЛЯ РОССИЯН

📊 ОБСТАНОВКА:
[2-3 предложения: что происходит в мире И в России прямо сейчас, 
как они связаны между собой]

⚖️ БАЛАНС РИСКОВ И ВОЗМОЖНОСТЕЙ:
[Возможностей больше / Рисков больше / Примерно поровну]
[Объясни почему — 1-2 предложения]

💡 ТОП-3 ДЕЙСТВИЯ ПРЯМО СЕЙЧАС:
1. [Конкретное действие для бизнеса или инвестора]
2. [Конкретное действие]
3. [Конкретное действие]

⚠️ ГЛАВНЫЙ РИСК НЕДЕЛИ:
[Один самый важный риск на который надо смотреть — конкретно]

📈 ДЛЯ ИНВЕСТОРОВ НА МОСБИРЖЕ:
[1-2 конкретных идеи с учётом текущих данных по акциям/ОФЗ]
[Только если есть данные — иначе напиши "недостаточно данных"]

🗣 ПРОСТЫМИ СЛОВАМИ:
[4-5 предложений без жаргона. Объясни как другу:
что сейчас происходит в российской экономике,
стоит ли что-то покупать/продавать,
на что обратить внимание на этой неделе.
Никаких терминов — только человеческий язык]

💎 ЖЁСТКИЙ ИТОГ (2 предложения максимум):
Самый выжатый вывод недели — что главное и что делать.
Формат: "[Ситуация одним словом]. [Конкретное действие]."
Пример: "Кэш — король. Пока ставка 21%, выгоднее держать деньги в ОФЗ или фондах денежного рынка (LQDT), чем открывать бизнес без господдержки."

⚠️ КРИТИЧЕСКИ ВАЖНО: НЕ ОБРЫВАЙ ТЕКСТ НА ПОЛУСЛОВЕ.
Если не хватает места — сокращай каждый блок, но все блоки должны быть завершены.

ЗАПРЕЩЕНО:
- Финансовый совет без дисклеймера
- Политические оценки
- Упоминать ARK Invest
- Западные брокеры недоступные в РФ
- R/R, DXY, Risk-off без объяснения простыми словами"""


# ─── Groq вызов ───────────────────────────────────────────────────────────────

async def call_groq_or_mistral(system: str, user_message: str) -> str:
    """Groq/Llama первый выбор, Mistral Small как fallback при rate limit."""

    # Сначала пробуем Groq
    if GROQ_API_KEY:
        headers = {
            "Authorization": f"Bearer {GROQ_API_KEY}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": "llama-3.3-70b-versatile",
            "messages": [
                {"role": "system", "content": system},
                {"role": "user",   "content": user_message},
            ],
            "temperature": 0.3,
            "max_tokens": 1500,
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    GROQ_URL, json=payload, headers=headers, timeout=TIMEOUT
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        logger.info("✅ Groq агент отработал")
                        return data["choices"][0]["message"]["content"]
                    elif resp.status == 429:
                        logger.warning("⚠️ Groq rate limit — переключаюсь на Mistral Small")
                    else:
                        logger.warning(f"Groq {resp.status} — переключаюсь на Mistral Small")
        except Exception as e:
            logger.warning(f"Groq недоступен ({e}) — переключаюсь на Mistral Small")

    # Fallback — Mistral Small (дешёвый, быстрый)
    if MISTRAL_API_KEY:
        headers = {
            "Authorization": f"Bearer {MISTRAL_API_KEY}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": "mistral-small-latest",
            "messages": [
                {"role": "system", "content": system},
                {"role": "user",   "content": user_message},
            ],
            "temperature": 0.3,
            "max_tokens": 1500,
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    MISTRAL_URL, json=payload, headers=headers, timeout=TIMEOUT
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        logger.info("✅ Mistral Small fallback отработал")
                        return data["choices"][0]["message"]["content"]
                    else:
                        error = await resp.text()
                        logger.error(f"Mistral Small error {resp.status}: {error[:200]}")
        except Exception as e:
            logger.error(f"Mistral Small exception: {e}")

    return "⚠️ Все провайдеры недоступны"


# Алиас для обратной совместимости
async def call_groq(system: str, user_message: str) -> str:
    return await call_groq_or_mistral(system, user_message)


# ─── Mistral синтез ───────────────────────────────────────────────────────────

async def call_mistral_synth(system: str, user_message: str) -> str:
    if not MISTRAL_API_KEY:
        return "⚠️ MISTRAL_API_KEY не настроен"

    headers = {
        "Authorization": f"Bearer {MISTRAL_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": "mistral-large-latest",
        "messages": [
            {"role": "system", "content": system},
            {"role": "user",   "content": user_message},
        ],
        "temperature": 0.3,
        "max_tokens": 2000,
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                MISTRAL_URL, json=payload, headers=headers, timeout=TIMEOUT
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data["choices"][0]["message"]["content"]
                else:
                    error = await resp.text()
                    logger.error(f"Mistral synth error {resp.status}: {error[:200]}")
                    return f"⚠️ Mistral ошибка {resp.status}"
    except Exception as e:
        logger.error(f"Mistral synth exception: {e}")
        return f"⚠️ Mistral недоступен: {str(e)[:100]}"


# ─── Главная функция ──────────────────────────────────────────────────────────

async def run_russia_analysis(global_report: str, russia_context: str) -> str:
    """
    Диалектический анализ для РФ аудитории.

    global_report  — кэш /daily анализа (9.5/10 качество)
    russia_context — ЦБ, Мосбиржа, Urals, Росстат, РБК, Коммерсант
    """
    logger.info("🇷🇺 Запускаю РФ анализ (Groq + Mistral)...")

    combined = f"""=== ГЛОБАЛЬНЫЙ АНАЛИЗ РЫНКОВ (топовые AI, качество 9.5/10) ===
{global_report[:3000]}

=== РОССИЙСКИЙ КОНТЕКСТ ===
{russia_context}"""

    # Параллельно два Groq агента (с паузой между ними чтобы не словить 429)
    logger.info("🦙 Запускаю Groq агентов...")

    opportunities = await call_groq(RUSSIA_OPPORTUNITIES_SYSTEM, combined)
    await asyncio.sleep(6)  # пауза между запросами Groq (rate limit protection)
    risks = await call_groq(RUSSIA_RISKS_SYSTEM, combined)

    logger.info("✅ Groq агенты завершили, запускаю Mistral синтез...")

    synth_input = f"""ГЛОБАЛЬНЫЙ АНАЛИЗ (резюме):
{global_report[:1500]}

РОССИЙСКИЙ КОНТЕКСТ:
{russia_context[:1500]}

АГЕНТ ВОЗМОЖНОСТЕЙ (Llama):
{opportunities}

АГЕНТ РИСКОВ (Llama):
{risks}

Собери финальный итог для российской аудитории."""

    synthesis = await call_mistral_synth(RUSSIA_SYNTH_SYSTEM, synth_input)
    logger.info("✅ РФ анализ завершён")

    now = datetime.now().strftime("%d.%m.%Y %H:%M")
    sep = "─" * 30

    report = f"""🇷🇺 *RUSSIA EDGE — АНАЛИЗ ДЛЯ РОССИЙСКОГО РЫНКА*
🕐 _{now}_

💬 _Глобальный анализ (Llama + Mistral Large) адаптирован для россиян_
🦙 _Агенты: Groq/Llama 70B × 2 + Mistral Large синтез_

{sep}

{opportunities}

{sep}

{risks}

{sep}

{synthesis}

{sep}
🤝 *Честно о модуле:*
_AI-анализ на основе публичных данных РФ и мировых рынков._
_Не является финансовым или юридическим советом._
_Законодательство РФ меняется — проверяй актуальность._

⚠️ _DYOR. Риск потери капитала существует всегда._"""

    return report
