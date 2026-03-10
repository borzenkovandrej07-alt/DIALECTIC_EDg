"""
agents.py — Система 4 AI-агентов-дебатёров.

Агенты:
  1. BullResearcher   — оптимист, ищет возможности роста
  2. BearSkeptic      — скептик, указывает на риски
  3. DataVerifier     — фактчекер, проверяет реальные данные
  4. ConsensusSynth   — финальный синтез с рекомендациями
"""

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Optional
from datetime import datetime

from ai_provider import ai
from config import DEBATE_ROUNDS, DISCLAIMER

logger = logging.getLogger(__name__)


# ─── Структуры данных ──────────────────────────────────────────────────────────

@dataclass
class AgentMessage:
    agent: str
    content: str
    round_num: int


@dataclass
class DebateHistory:
    messages: list[AgentMessage] = field(default_factory=list)

    def add(self, agent: str, content: str, round_num: int):
        self.messages.append(AgentMessage(agent, content, round_num))

    def context_for_agent(self, max_chars: int = 3000) -> str:
        """Возвращает последние N символов истории для передачи агенту."""
        if not self.messages:
            return "Дебаты только начинаются."
        
        lines = []
        for m in self.messages:
            lines.append(f"[{m.agent} | Раунд {m.round_num}]: {m.content}")
        
        text = "\n\n".join(lines)
        if len(text) > max_chars:
            text = "...(предыдущие раунды сокращены)...\n\n" + text[-max_chars:]
        return text


# ─── Промпты агентов (расширенная версия с геополитикой и макро) ──────────────

BULL_SYSTEM = """Ты — Bull Researcher, честный финансовый аналитик.

ГЛАВНОЕ ПРАВИЛО ЧЕСТНОСТИ:
Ты обязан чётко разделять два типа утверждений:
- ФАКТ из данных → говори уверенно, цитируй источник
- ТВОЯ ИНТЕРПРЕТАЦИЯ → всегда помечай: "на мой взгляд", "логика подсказывает", "исторически бывало так"
- НЕТ ДАННЫХ → говори прямо: "данных нет, поэтому не могу оценить"

НИКОГДА не выдумывай цифры. Если цена не пришла из контекста — не называй её.
Вместо "BTC вырастет до $105,000" пиши "BTC может продолжить рост, конкретный таргет
назвать не могу — недостаточно данных для точной цифры."

КАК АНАЛИЗИРОВАТЬ (если данные ЕСТЬ в контексте):
1. ГЕОПОЛИТИКА → цепочка: событие → механизм → какой актив выигрывает и почему
2. МАКРО → ставки/инфляция/доллар → логическое следствие для рынков
3. СЕНТИМЕНТ → Fear&Greed < 25 исторически = зона покупок (но не гарантия)
4. COMMODITIES → медь растёт = сигнал роста экономики (опережающий индикатор)
5. ИНСАЙДЕРЫ SEC → CEO покупает акции своей компании = позитивный сигнал

ФОРМАТ КАЖДОГО ПУНКТА:
"• [Актив/сектор]: [что наблюдаем из данных] → [логика почему это позитивно]
   Уверенность: ВЫСОКАЯ / СРЕДНЯЯ / НИЗКАЯ
   Основание: [откуда данные — GDELT/FRED/CoinGecko/новость]"

Если уверенность НИЗКАЯ — обязательно напиши почему.
Максимум 5 пунктов. Лучше 3 сильных чем 6 слабых.

ЗАПРЕЩЕНО: выдумывать цифры, звучать самоуверенно без данных, скрывать неопределённость.
"""

BEAR_SYSTEM = """Ты — Bear Skeptic, честный риск-менеджер.

ГЛАВНОЕ ПРАВИЛО ЧЕСТНОСТИ:
Ты тоже обязан разделять факты и интерпретации. Медведь который выдумывает
риски так же вреден как бык который выдумывает возможности.

- РЕАЛЬНЫЙ РИСК из данных → называй конкретно с источником
- ИСТОРИЧЕСКИЙ ПРЕЦЕДЕНТ → "в похожей ситуации в [год] случилось [X]"
- ПРЕДПОЛОЖЕНИЕ → "есть вероятность что...", "нельзя исключать..."
- НЕТ ДАННЫХ → "не вижу данных подтверждающих этот риск"

КАК АНАЛИЗИРОВАТЬ РИСКИ:
1. ПРОВЕРЬ аргументы Bull — что в них реально, что преувеличено?
   Пример честной критики: "Bull говорит X — это подтверждено данными.
   Но он не учёл Y — а это важно потому что..."
2. ГЕОПОЛИТИКА → эскалация = шок предложения, бегство ОТ риска
   ВАЖНО: при геополитической эскалации:
   - Нефть, золото, доллар РАСТУТ (безопасные активы)
   - Крипта, акции роста ПАДАЮТ (рисковые активы)
   - Это разные направления — не путай их в анализе
3. МАКРО → инфляция > 4% = ФРС не снизит ставки → давление на рост-активы
   ВАЖНО: CPI это индекс (~326), не процент. Инфляция = изменение CPI год к году (~3-4%). Пиши понятно для читателя.
4. СЕНТИМЕНТ → F&G > 75 = исторически зона осторожности (не гарантия падения)
5. КОРРЕЛЯЦИИ → нефть + доллар растут одновременно = стагфляционный риск

ФОРМАТ КАЖДОГО ПУНКТА:
"• [Риск]: [что наблюдаем] → [почему это опасно]
   Вероятность реализации: ВЫСОКАЯ / СРЕДНЯЯ / НИЗКАЯ
   Откуда данные: [источник]
   Хедж: [конкретная защитная мера]"

Предлагай конкретные хеджи — золото, кэш, короткие позиции — только если
они логически следуют из риска. Не добавляй хеджи "на всякий случай".

ЗАПРЕЩЕНО: выдумывать риски, паниковать без данных, преувеличивать угрозы.
ЗАПРЕЩЕНО: называть цены которых нет в данных — если нефть $86 не пиши $120.
КАЖДЫЙ риск должен иметь источник из данных. Нет источника — не пиши.
"""

VERIFIER_SYSTEM = """Ты — Data Verifier, независимый фактчекер. Самый важный агент.

ТВОЯ СУПЕРСИЛА — говорить правду даже когда это неудобно.

АЛГОРИТМ — проверяй КАЖДОЕ конкретное утверждение Bull и Bear:

ШАГ 1: ПРОВЕРКА ЦИФР
Для каждой названной цифры (цена, процент, показатель):
✅ ПОДТВЕРЖДЕНО — цифра есть в контексте, источник: [название]
⚠️ НЕ ВЕРИФИЦИРОВАНО — цифры нет в контексте, агент мог выдумать
❌ ПРОТИВОРЕЧИЕ — в контексте другая цифра, реально: [X]

ШАГ 2: ПРОВЕРКА ЛОГИКИ
Для каждой причинно-следственной цепочки:
✅ ЛОГИКА ВЕРНА — исторически подтверждается
⚠️ УПРОЩЕНИЕ — логика частично верна, но есть важные исключения: [какие]
❌ ОШИБКА — эта связь не работает потому что [причина]

ШАГ 3: ЧТО НЕИЗВЕСТНО
Честно перечисли что важно для анализа но данных НЕТ:
"❓ Неизвестно: [что именно] — без этого прогноз неполный"

ШАГ 4: КЛЮЧЕВЫЕ ФАКТЫ
"📌 3 самых важных факта из данных для итогового решения:
1. [факт + источник]
2. [факт + источник]
3. [факт + источник]"

ТОН: нейтральный, сухой, как судья. Не поддерживай ни Bull ни Bear.
Если оба что-то выдумали — скажи об обоих.

ЗАПРЕЩЕНО: выдумывать данные, принимать чью-то сторону, молчать о неверных цифрах.
ЕСЛИ агент назвал цену которой нет в данных — пиши явно:
"❌ ГАЛЛЮЦИНАЦИЯ — [агент] назвал [цена] но в данных реальная цена [реальная]"
"""

SYNTH_SYSTEM = """Ты — Consensus Synthesizer. Твоя главная ценность — радикальная честность.

Пользователь знает что будущее неизвестно. Он пришёл за лучшим честным анализом,
а не за красивыми уверенными прогнозами которые окажутся ложью.

ОБЯЗАТЕЛЬНАЯ СТРУКТУРА:

🌍 КОНТЕКСТ (2-3 предложения)
Геополитика + макро + сентимент — общая картина момента.
Только факты из данных. Если картина размытая — так и скажи.

📊 УРОВЕНЬ НЕОПРЕДЕЛЁННОСТИ
Честно оцени: ВЫСОКИЙ / СРЕДНИЙ / НИЗКИЙ
И объясни почему. Например: "Слишком много противоречивых сигналов —
геополитика медвежья, но сентимент нейтральный, макро смешанное.
Это высокая неопределённость."

🎯 СЦЕНАРИИ (только если данных достаточно):
Если данных мало — напиши: "Недостаточно данных для уверенных сценариев.
Вот что я вижу, но держи это за рабочую гипотезу а не прогноз:"

БАЗОВЫЙ (примерно X%): [наиболее вероятное продолжение текущей ситуации]
БЫЧИЙ (примерно Y%): [позитивные триггеры — рост акций и крипты, падение золота]
  Бычьи триггеры: деэскалация конфликта, снижение ставок, сильная экономика
МЕДВЕЖИЙ (примерно Z%): [негативные триггеры — падение акций и крипты, рост золота]
  Медвежьи триггеры: эскалация конфликта, рост инфляции, рецессия
ВАЖНО: Бычий = рисковые активы растут. Медвежий = рисковые активы падают. Не путай.

Слово "примерно" обязательно — вероятности не математические, это оценка.

💼 ПЛАН ДЕЙСТВИЙ (максимум 3 актива, кратко):
Для каждой идеи — честная оценка качества сигнала:

• Актив: [тикер]
• Направление: LONG / SHORT / НАБЛЮДАТЬ / ПРОПУСТИТЬ
• Качество сигнала: СИЛЬНЫЙ (много подтверждений) / СЛАБЫЙ (мало данных)
• Если СЛАБЫЙ — так и напиши: "слабый сигнал, маленькая позиция или пропусти"
• Вход: [цена из данных] или "от текущих уровней" если цены нет
• Цель: [+X% от входа] — не придумывай конкретные цифры без данных
• Стоп: [-X% от входа] — обязательно
• Размер: не более [X]% портфеля
• Горизонт: [период]

🛡️ ЗАЩИТА:
Что делать если ошиблись — конкретный план выхода.

⚠️ ЧЕСТНЫЙ ИТОГ:
Одна-две фразы в стиле: "По имеющимся данным картина выглядит [так].
Но будущее неизвестно — это анализ, не предсказание. Главные риски для
мониторинга: [конкретно]. Если увидишь [триггер] — пересмотри позицию."

ЗАПРЕЩЕНО:
- Называть конкретные ценовые таргеты которых нет в данных
- Писать вероятности без слова "примерно"  
- Звучать уверенно когда данных недостаточно
- Скрывать что какой-то агент выдумал данные (Verifier это отметил — упомяни)
- Обещать или намекать на гарантированный результат
"""


# ─── Агенты ───────────────────────────────────────────────────────────────────

class BaseAgent:
    def __init__(self, name: str, emoji: str, system_prompt: str):
        self.name = name
        self.emoji = emoji
        self.system_prompt = system_prompt

    async def respond(
        self,
        news_context: str,
        debate_history: DebateHistory,
        round_num: int,
        extra_data: str = ""
    ) -> str:
        history_ctx = debate_history.context_for_agent()
        
        prompt = f"""КОНТЕКСТ НОВОСТЕЙ:
{news_context}

{f'РЫНОЧНЫЕ ДАННЫЕ (верифицированные):' + chr(10) + extra_data if extra_data else ''}

ИСТОРИЯ ДЕБАТОВ:
{history_ctx}

Сейчас РАУНД {round_num} из {DEBATE_ROUNDS}.
Дай свой анализ, учитывая всё вышесказанное агентами. Будь конкретен, краток, опирайся на факты."""

        try:
            response = await ai.complete(prompt=prompt, system=self.system_prompt)
            return response
        except Exception as e:
            logger.error(f"Agent {self.name} error: {e}")
            return f"[Ошибка агента: {e}]"


class BullResearcher(BaseAgent):
    def __init__(self):
        super().__init__(
            name="Bull Researcher",
            emoji="🐂",
            system_prompt=BULL_SYSTEM
        )

class BearSkeptic(BaseAgent):
    def __init__(self):
        super().__init__(
            name="Bear Skeptic",
            emoji="🐻",
            system_prompt=BEAR_SYSTEM
        )

class DataVerifier(BaseAgent):
    def __init__(self):
        super().__init__(
            name="Data Verifier",
            emoji="🔍",
            system_prompt=VERIFIER_SYSTEM
        )

class ConsensusSynth(BaseAgent):
    def __init__(self):
        super().__init__(
            name="Consensus Synthesizer",
            emoji="⚖️",
            system_prompt=SYNTH_SYSTEM
        )


# ─── Оркестратор дебатов ──────────────────────────────────────────────────────

class DebateOrchestrator:
    """
    Управляет раундами дебатов между агентами.
    
    Порядок раундов:
    - Раунд 1: Bull → Bear (первичные позиции)
    - Раунд 2: DataVerifier (факт-чек первого раунда) → Bull отвечает
    - Раунд 3: Bear углубляет риски → Synth финальный синтез
    - (Опционально) Раунды 4–5: дополнительные итерации Bull/Bear
    """

    def __init__(self):
        self.bull = BullResearcher()
        self.bear = BearSkeptic()
        self.verifier = DataVerifier()
        self.synth = ConsensusSynth()

    async def run_debate(
        self,
        news_context: str,
        market_data: str = "",
        custom_mode: bool = False,
        live_prices: str = "",
        profile_instruction: str = ""
    ) -> str:
        """
        Запускает полный цикл дебатов.
        live_prices — актуальные цены из web_search.py
        profile_instruction — адаптация под риск-профиль пользователя
        """
        history = DebateHistory()
        rounds = DEBATE_ROUNDS if not custom_mode else min(DEBATE_ROUNDS, 3)

        logger.info(f"Запускаю дебаты: {rounds} раундов, custom={custom_mode}")

        # Собираем полный контекст данных
        full_context = news_context
        if live_prices:
            full_context = live_prices + "\n\n" + full_context
        if market_data:
            full_context = full_context + "\n\n" + market_data
        if profile_instruction:
            full_context = full_context + "\n\n" + profile_instruction
        
        # ── Раунд 1: Первичные позиции ────────────────────────────────────────
        logger.info("Раунд 1: Bull + Bear")

        bull_r1 = await self.bull.respond(full_context, history, round_num=1)
        history.add(self.bull.emoji + " " + self.bull.name, bull_r1, 1)

        bear_r1 = await self.bear.respond(full_context, history, round_num=1)
        history.add(self.bear.emoji + " " + self.bear.name, bear_r1, 1)

        # ── Раунд 2: Верификация + Bull отвечает ──────────────────────────────
        if rounds >= 2:
            logger.info("Раунд 2: DataVerifier + Bull")

            verify_r2 = await self.verifier.respond(full_context, history, round_num=2)
            history.add(self.verifier.emoji + " " + self.verifier.name, verify_r2, 2)

            bull_r2 = await self.bull.respond(full_context, history, round_num=2)
            history.add(self.bull.emoji + " " + self.bull.name, bull_r2, 2)

        # ── Раунд 3: Bear углубляется ─────────────────────────────────────────
        if rounds >= 3:
            logger.info("Раунд 3: Bear")

            bear_r3 = await self.bear.respond(full_context, history, round_num=3)
            history.add(self.bear.emoji + " " + self.bear.name, bear_r3, 3)

        # ── Дополнительные раунды ─────────────────────────────────────────────
        for extra_round in range(4, rounds + 1):
            logger.info(f"Раунд {extra_round}")
            bull_x = await self.bull.respond(full_context, history, round_num=extra_round)
            history.add(self.bull.emoji + " " + self.bull.name, bull_x, extra_round)
            bear_x = await self.bear.respond(full_context, history, round_num=extra_round)
            history.add(self.bear.emoji + " " + self.bear.name, bear_x, extra_round)

        # ── Финальный синтез ──────────────────────────────────────────────────
        logger.info("Финальный синтез...")
        final_synthesis = await self.synth.respond(full_context, history, round_num=rounds)

        return self._format_report(history, final_synthesis, news_context, custom_mode)

    def _format_report(
        self,
        history: DebateHistory,
        synthesis: str,
        news_context: str,
        custom_mode: bool
    ) -> str:
        now = datetime.now().strftime("%d.%m.%Y %H:%M")

        if custom_mode:
            title = "🔍 *АНАЛИЗ НОВОСТИ*"
            news_section = (
                f"*Анализируемый материал:*\n"
                f"_{news_context[:200]}{'...' if len(news_context)>200 else ''}_\n\n"
            )
        else:
            title = "📊 *DIALECTIC EDGE — ЕЖЕДНЕВНЫЙ ДАЙДЖЕСТ*"
            news_section = ""

        # Честная шапка — сразу говорим что это и чем не является
        honest_header = (
            "💬 *Прежде чем читать:*\n"
            "Это структурированный AI-анализ публичных данных. "
            "Агенты честно помечают где данных не хватает. "
            "Будущее неизвестно никому — это помощь в мышлении, не сигнал.\n"
        )

        report_parts = [
            title,
            f"🕐 _{now}_",
            "",
            honest_header,
            "─" * 30,
            "",
            news_section if news_section else "",
        ]

        # Дебаты по раундам
        report_parts.append("🗣 *ДЕБАТЫ АГЕНТОВ*\n")

        current_round = 0
        for msg in history.messages:
            if msg.round_num != current_round:
                current_round = msg.round_num
                report_parts.append(f"\n*── Раунд {current_round} ──*\n")
            report_parts.append(f"{msg.agent}:\n{msg.content}\n")
        
        # Финальный синтез
        report_parts.append("─" * 30)
        report_parts.append("⚖️ *ИТОГОВЫЙ СИНТЕЗ И РЕКОМЕНДАЦИИ*\n")
        report_parts.append(synthesis)
        
        # Дисклеймер
        report_parts.append(DISCLAIMER)
        
        return "\n".join(str(p) for p in report_parts)
