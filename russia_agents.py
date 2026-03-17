"""
russia_agents.py — Диалектический анализ для российской аудитории.

ИСПРАВЛЕНО v2:
- В промпте RUSSIA_OPPORTUNITIES_SYSTEM: явный запрет на возможности
  с Уверенностью НИЗКАЯ — они не несут практической ценности и
  засоряют график (фильтруются в chart_generator но всё равно пишутся).

- Во всех трёх промптах: чёткое разграничение между
  инфляцией США (CPI FRED ~3-4% YoY) и инфляцией РФ (~9%).
"""

import asyncio
import logging
import os
from datetime import datetime
import aiohttp

logger = logging.getLogger(__name__)

GROQ_API_KEY    = os.getenv("GROQ_API_KEY", "")
GROQ_API_KEY_2  = os.getenv("GROQ_API_KEY_2", "")
MISTRAL_API_KEY = os.getenv("MISTRAL_API_KEY", "")

GROQ_URL    = "https://api.groq.com/openai/v1/chat/completions"
MISTRAL_URL = "https://api.mistral.ai/v1/chat/completions"
TIMEOUT     = aiohttp.ClientTimeout(total=60)


RUSSIA_OPPORTUNITIES_SYSTEM = """Ты — аналитик возможностей для малого и среднего бизнеса России.

Ты получаешь:
1. Глобальный анализ мировых рынков от топовых AI (качество 9.5/10) — доверяй ему
2. Российский контекст: курс рубля, ключевая ставка ЦБ, Мосбиржа, цена Urals, новости

ТВОЯ ЗАДАЧА: найти КОНКРЕТНЫЕ возможности для заработка для россиян прямо сейчас.
Целевая аудитория: малый бизнес, ИП, частные инвесторы с доступом к Мосбирже.

⚠️ КРИТИЧЕСКИ ВАЖНО — ДВЕ РАЗНЫЕ ИНФЛЯЦИИ:
- Инфляция США (CPI FRED) — ~3-4% годовых. К российскому бизнесу НЕ относится.
- Инфляция РФ (Росстат) — ~9% годовых. Только её используй для расчётов.
ЗАПРЕЩЕНО смешивать. Правильно: "Ставка 21% при инфляции РФ ~9% = реальная доходность +12%"

ЛОГИКА АНАЛИЗА:
- Если нефть Urals дорогая → бюджет в профиците → госзаказы растут
- Если рубль слабеет → экспорт выигрывает, импорт проигрывает
- Если ставка ЦБ высокая → депозиты и ОФЗ выгодны
- Если санкции усиливаются → логистика и параллельный импорт в плюсе

ФОРМАТ:
🟢 ВОЗМОЖНОСТИ ДЛЯ РОССИЯН:

• [Название]
  Суть: [что происходит и почему возможность]
  Кому подходит: [малый бизнес / инвестор / ИП / все]
  Как действовать: [конкретный шаг]
  Горизонт: [период]
  Уверенность: ВЫСОКАЯ / СРЕДНЯЯ

⚡ ЖЁСТКИЙ ФИЛЬТР УВЕРЕННОСТИ:
Напиши ТОЛЬКО 3-4 возможности.
ВКЛЮЧАЙ только Уверенность ВЫСОКАЯ или СРЕДНЯЯ.
ЗАПРЕЩЕНО писать возможности с Уверенностью НИЗКАЯ — они не несут практической ценности.
Если не можешь найти 3 возможности с ВЫСОКОЙ/СРЕДНЕЙ уверенностью — напиши 2, но качественных.

ОБЯЗАТЕЛЬНО для финансовых инструментов:
Реальная доходность = Ставка ЦБ РФ (%) минус Инфляция РФ (~9%)
Если > 5% → "историческая аномалия, фиксируй доходность сейчас"

ЗАПРЕЩЕНО:
- Возможности с Уверенностью НИЗКАЯ
- "Купите доллары" — банально
- Западные активы недоступные в РФ
- Криптовалюта без оговорки о регуляторных рисках
- Использовать инфляцию США (~4%) вместо инфляции РФ (~9%)

✅ ДОСТУПНЫЕ ИНСТРУМЕНТЫ:
Акции: GAZP, LKOH, ROSN, GMKN, ALRS, SBER, NVTK, MAGN, NLMK, CHMF
Облигации: ОФЗ, ОФЗ-ИН, флоатеры RUONIA, корпоративные
Фонды: LQDT, SBMM, AKMM
Валюта: юань (CNY) на Мосбирже, юаневые облигации
Инструменты: факторинг, лизинг, форварды через российские банки"""


RUSSIA_RISKS_SYSTEM = """Ты — аналитик рисков для малого и среднего бизнеса России.

Ты получаешь:
1. Глобальный анализ мировых рынков (качество 9.5/10)
2. Российский контекст: курсы ЦБ, ставка, Мосбиржа, Urals, новости РФ

⚠️ КРИТИЧЕСКИ ВАЖНО — ДВЕ РАЗНЫЕ ИНФЛЯЦИИ:
- Инфляция США (CPI FRED) — ~3-4%. Только для контекста про ФРС и глобальные рынки.
- Инфляция РФ (Росстат) — ~9%. Для всего про российский бизнес и потребителей.
ЗАПРЕЩЕНО: "инфляция 4.2%" в контексте РФ.

ЛОГИКА:
- Нефть падает → доходы бюджета РФ падают → новые налоги
- Рубль слабеет → импортное сырьё дорожает → себестоимость растёт
- Ставка ЦБ высокая → кредитная нагрузка душит МСБ
- Risk-Off в мире → отток из РФ активов → давление на рубль

ФОРМАТ:
🔴 РИСКИ ДЛЯ РОССИЙСКОГО БИЗНЕСА:

• [Название]
  Что происходит: [факт]
  Как бьёт по бизнесу: [конкретное влияние на P&L]
  Вероятность: ВЫСОКАЯ / СРЕДНЯЯ / НИЗКАЯ
  Как защититься: [конкретный инструмент]

Напиши 3-4 риска. Конкретные защитные инструменты:
- Вместо "хеджируй" → "форвардные контракты через банк или юаневые облигации на Мосбирже"
- Вместо "ищи финансирование" → "флоатеры (RUONIA) или факторинг"

ЗАПРЕЩЕНО:
- Политические комментарии о войне/СВО
- "Экономика нестабильна" — слишком общо
- "Инфляция 4.2%" в контексте РФ"""


RUSSIA_SYNTH_SYSTEM = """Ты — финальный синтезатор для российской аудитории.

⚠️ НЕ ПУТАЙ ДВЕ ИНФЛЯЦИИ:
- Инфляция США ≈ 3-4% — про ФРС и глобальные рынки.
- Инфляция РФ ≈ 9% — про Россию. ТОЛЬКО её для расчётов доходности ОФЗ.

⚡ БЛОК НЕФТЬ (обязателен если есть данные):
🛢️ НЕФТЬ:
"Urals $[цена] | Бюджет РФ при $69.7: [профицит/дефицит]
Каждые $10 изменения = ~±1.5 трлн ₽ в бюджет
Если Urals упадёт до $[X]: [последствия]"

ФОРМАТ:

🇷🇺 ИТОГ ДЛЯ РОССИЯН

📊 ОБСТАНОВКА:
[2-3 предложения: что в мире И в России, как связаны]

⚖️ БАЛАНС РИСКОВ И ВОЗМОЖНОСТЕЙ:
[Возможностей больше / Рисков больше / Поровну — объясни почему]

💡 ТОП-3 ДЕЙСТВИЯ ПРЯМО СЕЙЧАС:
1. [конкретное действие с инструментом]
2. [конкретное действие]
3. [конкретное действие]

⚠️ ГЛАВНЫЙ РИСК НЕДЕЛИ:
[Один конкретный риск с триггером и числом]

📈 ДЛЯ ИНВЕСТОРОВ НА МОСБИРЖЕ:
[1-2 конкретных идеи или "недостаточно данных"]

🗣 ПРОСТЫМИ СЛОВАМИ:
[4-5 предложений без жаргона — как другу объясняешь]

💎 ЖЁСТКИЙ ИТОГ:
[Ситуация одним словом]. [Конкретное действие.]
Пример: "Кэш — король. Пока ставка 21%, держать в ОФЗ/LQDT выгоднее чем рисковать."

⚠️ НЕ ОБРЫВАЙ ТЕКСТ. Если мало места — сокращай блоки, но завершай все.

ЗАПРЕЩЕНО:
- "ARK Invest", западные брокеры, недоступные в РФ активы
- Инфляцию США (~4%) как показатель для россиян
- Политические оценки"""


async def call_groq_or_mistral(system: str, user_message: str) -> str:
    """Groq первый, при 429 — следующий ключ, затем Mistral Small."""

    groq_keys = [k for k in [GROQ_API_KEY, GROQ_API_KEY_2] if k]

    for key in groq_keys:
        headers = {
            "Authorization": f"Bearer {key}",
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
                        logger.warning(f"⚠️ Groq ключ лимит — пробую следующий")
                        continue
                    else:
                        logger.warning(f"Groq {resp.status}")
        except Exception as e:
            logger.warning(f"Groq недоступен: {e}")

    # Fallback — Mistral Small
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
                        logger.info("✅ Mistral Small fallback")
                        return data["choices"][0]["message"]["content"]
                    else:
                        error = await resp.text()
                        logger.error(f"Mistral Small {resp.status}: {error[:200]}")
        except Exception as e:
            logger.error(f"Mistral Small: {e}")

    return "⚠️ Все провайдеры недоступны"


async def call_groq(system: str, user_message: str) -> str:
    return await call_groq_or_mistral(system, user_message)


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
                elif resp.status == 429:
                    logger.warning("Mistral Large 429 — пробую Small")
                    payload["model"] = "mistral-small-latest"
                    async with session.post(
                        MISTRAL_URL, json=payload, headers=headers, timeout=TIMEOUT
                    ) as resp2:
                        if resp2.status == 200:
                            data = await resp2.json()
                            return data["choices"][0]["message"]["content"]
                else:
                    error = await resp.text()
                    logger.error(f"Mistral synth {resp.status}: {error[:200]}")
                    return f"⚠️ Mistral ошибка {resp.status}"
    except Exception as e:
        logger.error(f"Mistral synth: {e}")
        return f"⚠️ Mistral недоступен: {str(e)[:100]}"


async def run_russia_analysis(global_report: str, russia_context: str) -> str:
    logger.info("🇷🇺 Запускаю РФ анализ...")

    inflation_note = (
        "\n⚠️ НАПОМИНАНИЕ:\n"
        "- Инфляция США (CPI) ≈ 3-4% — про ФРС, не про РФ.\n"
        "- Инфляция РФ (Росстат) ≈ 9% — для расчётов доходности ОФЗ.\n"
        "НЕ ПУТАЙ. Пример: 'ставка 21% − инфляция РФ 9% = доходность +12%'\n"
    )

    combined = f"""=== ГЛОБАЛЬНЫЙ АНАЛИЗ (качество 9.5/10) ===
{global_report[:3000]}

=== РОССИЙСКИЙ КОНТЕКСТ ===
{russia_context}
{inflation_note}"""

    logger.info("🦙 Запускаю Groq агентов...")
    opportunities = await call_groq(RUSSIA_OPPORTUNITIES_SYSTEM, combined)
    await asyncio.sleep(6)
    risks = await call_groq(RUSSIA_RISKS_SYSTEM, combined)

    logger.info("✅ Groq готов, запускаю Mistral синтез...")

    synth_input = f"""ГЛОБАЛЬНЫЙ АНАЛИЗ:
{global_report[:1500]}

РОССИЙСКИЙ КОНТЕКСТ:
{russia_context[:1500]}
{inflation_note}
ВОЗМОЖНОСТИ (Llama):
{opportunities}

РИСКИ (Llama):
{risks}

Собери финальный итог для российской аудитории."""

    synthesis = await call_mistral_synth(RUSSIA_SYNTH_SYSTEM, synth_input)
    logger.info("✅ РФ анализ завершён")

    now = datetime.now().strftime("%d.%m.%Y %H:%M")
    sep = "─" * 30

    return f"""🇷🇺 RUSSIA EDGE — АНАЛИЗ ДЛЯ РОССИЙСКОГО РЫНКА
🕐 {now}

💬 Глобальный анализ (Llama + Mistral Large) адаптирован для россиян
🦙 Агенты: Groq/Llama 70B × 2 + Mistral Large синтез

{sep}

{opportunities}

{sep}

{risks}

{sep}

{synthesis}

{sep}
🤝 Честно о модуле:
AI-анализ на основе публичных данных РФ и мировых рынков.
Не является финансовым или юридическим советом.
Законодательство РФ меняется — проверяй актуальность.

⚠️ DYOR. Риск потери капитала существует всегда."""
