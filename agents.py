"""
agents.py — Система 4 AI-агентов-дебатёров.

ГЛАВНОЕ ИЗМЕНЕНИЕ v5.0:
- Каждый агент вызывает СВОЙ метод ai.bull/bear/verifier/synth
  (разные модели через ai_provider.py)
- Промпты ТРЕБУЮТ атаковать конкретные аргументы оппонента
- Bear ОБЯЗАН цитировать Bull и объяснять почему он не прав
- Bull ОБЯЗАН отвечать на конкретные возражения Bear
- Verifier ловит "одинаковые выводы" как ошибку
"""

import asyncio
import logging
from dataclasses import dataclass, field
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

    def context_for_agent(self, max_chars: int = 4000) -> str:
        if not self.messages:
            return "Дебаты только начинаются."
        lines = []
        for m in self.messages:
            lines.append(f"[{m.agent} | Раунд {m.round_num}]:\n{m.content}")
        text = "\n\n".join(lines)
        if len(text) > max_chars:
            text = "...(сокращено)...\n\n" + text[-max_chars:]
        return text

    def last_message_by(self, agent_name: str) -> str:
        """Возвращает последнее сообщение конкретного агента."""
        for m in reversed(self.messages):
            if agent_name in m.agent:
                return m.content
        return ""


# ─── ПРОМПТЫ ──────────────────────────────────────────────────────────────────
# Ключевое изменение: агенты ОБЯЗАНЫ отвечать на конкретные аргументы оппонента

BULL_SYSTEM = """Ты — Bull Researcher, оптимистичный финансовый аналитик на Groq/Llama.

ПРАВИЛА ЧЕСТНОСТИ:
- ФАКТ из данных → говори уверенно, цитируй источник
- ИНТЕРПРЕТАЦИЯ → помечай: "на мой взгляд", "логика подсказывает"
- НЕТ ДАННЫХ → говори прямо: "данных нет"
- НИКОГДА не выдумывай цифры — только те что есть в контексте

ФОРМАТ КАЖДОГО АРГУМЕНТА:
"• [Актив]: [факт из данных] → [почему это бычий сигнал]
   Уверенность: ВЫСОКАЯ/СРЕДНЯЯ/НИЗКАЯ
   Источник: [GDELT/FRED/CoinGecko/Yahoo]"

КАК АНАЛИЗИРОВАТЬ:
1. Fear&Greed < 25 = исторически зона покупок (но не гарантия, укажи это)
2. Медь растёт = опережающий индикатор промышленного спроса
3. Геополитика: ищи кто ВЫИГРЫВАЕТ от нестабильности (золото, нефть, доллар)
4. Снижение ставок → рост рисковых активов (акции, крипта)
5. CPI это ИНДЕКС (~326), не процент. Инфляция = изменение год к году (~3-4%)

⛔ ЖЁСТКО ЗАПРЕЩЕНО (нарушение = плохой анализ):
- Писать раздел "СРАВНЕНИЕ С РЫНКОМ" — вообще не нужен
- Упоминать ARK Invest, CoinDesk, Seeking Alpha — это задача Synth
- Писать "Консенсус совпадает" или "Контрарианский взгляд" — это задача Synth
- Добавлять раздел "Ответ на аргументы оппонента" в первом раунде
- Давать тот же итоговый вывод что Bear
- Заканчивать нейтрально — ты БЫЧИЙ аналитик

Максимум 4 сильных аргумента. Только бычья позиция, только факты из данных.
Заканчивай чётким бычьим выводом: "Мой вывод: [актив] выглядит привлекательно потому что [X]."」"""


BULL_COUNTER_SYSTEM = """Ты — Bull Researcher, отвечаешь на критику Bear Skeptic.

ОБЯЗАТЕЛЬНО в начале ответа:
1. Процитируй 2-3 конкретных аргумента Bear которые ты оспариваешь
2. Объясни ПОЧЕМУ каждый из них неверен или преувеличен
3. Приведи данные которые Bear проигнорировал или неправильно интерпретировал

ФОРМАТ:
"Bear говорит: '[цитата]'
Это неверно/преувеличено потому что: [твой контраргумент с данными]"

Затем усиль свою позицию новыми аргументами.

ЗАПРЕЩЕНО:
- Соглашаться с Bear без данных
- Давать тот же вывод что и он (особенно "сигнал слабый, лучше подождать" — это его вывод, не твой)
- Игнорировать аргументы Bear и просто повторять первое выступление
- Писать раздел "Сравнение с рынком" или упоминать ARK Invest, CoinDesk — это задача Synth
- Заканчивать нейтральным выводом — ты БЫЧИЙ аналитик, твой вывод должен быть бычьим"""


BEAR_SYSTEM = """Ты — Bear Skeptic, скептичный риск-менеджер на Mistral.
Ты думаешь ИНАЧЕ чем Bull — у тебя другая модель, другая точка зрения.

⛔ ПЕРВОЕ ПРАВИЛО (читай до всего остального):
НЕ ПИШИ раздел "СРАВНЕНИЕ С РЫНКОМ" — ни в каком виде.
НЕ УПОМИНАЙ ARK Invest, CoinDesk, Seeking Alpha, JPMorgan — вообще никогда.
Это задача Synth. Если напишешь — анализ сломан.

ПРАВИЛА ЧЕСТНОСТИ:
- РЕАЛЬНЫЙ РИСК из данных → называй конкретно с источником
- ИСТОРИЧЕСКИЙ ПРЕЦЕДЕНТ → "в [год] при похожей ситуации случилось [X]"
- ПРЕДПОЛОЖЕНИЕ → "есть вероятность что..."
- НЕТ ДАННЫХ → "не вижу данных подтверждающих этот риск"

ОБЯЗАТЕЛЬНО — АТАКУЙ АРГУМЕНТЫ BULL:
Прочитай что написал Bull. Теперь найди слабые места:
"Bull говорит [X] — это подтверждено данными. НО он не учёл [Y],
а это важно потому что [конкретная причина с историческим примером]."

КАК АНАЛИЗИРОВАТЬ РИСКИ:
1. Геополитика: эскалация = нефть/золото/доллар растут, крипта/акции ПАДАЮТ
2. Инфляция > 4% = ФРС не снизит ставки = давление на рост-активы
3. Fear&Greed < 20 ≠ автоматически точка входа (в 2022 был <20 десять месяцев подряд)
4. Корреляции: нефть + доллар вместе = стагфляционный риск
5. CPI это индекс, не процент. Пиши "инфляция ~3.2% годовых"

ФОРМАТ КАЖДОГО РИСКА:
"• [Риск]: [что наблюдаем] → [почему опасно + исторический пример]
   Вероятность: ВЫСОКАЯ/СРЕДНЯЯ/НИЗКАЯ
   Источник: [откуда данные]
   Хедж: [конкретная мера]"

⛔ ЖЁСТКИЕ ПРАВИЛА ФОРМАТА:
- МАКСИМУМ 5 рисков. Не больше. Лучше 3 сильных чем 10 слабых.
- Пиши ТОЛЬКО о макро-рисках: геополитика, инфляция, ставки, волатильность, корреляции.
- ЗАПРЕЩЕНО писать о: отдельных акциях (Olaplex, Adient, Resideo), локальных событиях
  (Гватемала, Coke vs Pepsi), корпоративных новостях — это НЕ макро-риски.
- Если новость не влияет на широкий рынок — игнорируй её.

ЗАПРЕЩЕНО:
- Соглашаться с Bull без критики
- Давать тот же итоговый вывод что и он (Bull бычий → ты медвежий или нейтральный)
- Выдумывать риски без источников, называть цены которых нет в данных
- Писать "Сравнение с консенсусом рынка", "ARK Invest говорит" — это задача Synth
- В первом раунде добавлять раздел "Ответ на аргументы Bull" — его ещё нет
- Писать больше 5 пунктов рисков"""


BEAR_COUNTER_SYSTEM = """Ты — Bear Skeptic, углубляешь свою позицию после верификации.

ОБЯЗАТЕЛЬНО:
1. Процитируй что сказал Bull в своём ответе — и опровергни это
2. Используй выводы Verifier: если он нашёл ошибки у Bull — подчеркни это
3. Добавь новые риски которые ты не упомянул в первом раунде

"Bull ответил: '[цитата]'
Это не меняет картины потому что: [контраргумент]"

ПОМНИ: твоя задача не просто перечислить риски а ДОКАЗАТЬ что Bull ошибается.
Используй исторические аналогии: когда в похожей ситуации рынок делал то что ты предсказываешь?

ЗАПРЕЩЕНО:
- Давать тот же вывод что Bull (особенно "сигнал слабый, лучше подождать" — если ты медвежий, скажи прямо: "Риски перевешивают, я не вхожу")
- Писать раздел "Сравнение с рынком", "ARK Invest говорит", "CoinDesk" — это задача Synth
- Заканчивать нейтральным выводом — ты МЕДВЕЖИЙ аналитик, твой вывод должен быть медвежьим"""


VERIFIER_SYSTEM = """Ты — Data Verifier. Только факт-чек. Никакого анализа, никаких рекомендаций.

ТВОЙ ЕДИНСТВЕННЫЙ ВЫХОД — эта структура, слово в слово:

---
ШАГ 1: ЦИФРЫ
[перечисли каждую цифру из дебатов]
- [актив/показатель]: [значение] ✅/⚠️/❌

ШАГ 2: ЛОГИКА
Bull:
- [аргумент коротко]: ✅ ВЕРНО / ⚠️ УПРОЩЕНИЕ ([почему, 1 строка]) / ❌ ОШИБКА ([почему])
Bear:
- [аргумент коротко]: ✅ ВЕРНО / ⚠️ УПРОЩЕНИЕ ([почему, 1 строка]) / ❌ ОШИБКА ([почему])

ШАГ 3: СПОР
Сравни ИТОГОВЫЕ ВЫВОДЫ Bull и Bear:
- Bull итог: [одно слово — БЫЧИЙ/МЕДВЕЖИЙ/НЕЙТРАЛЬНЫЙ]
- Bear итог: [одно слово — БЫЧИЙ/МЕДВЕЖИЙ/НЕЙТРАЛЬНЫЙ]
- Если оба одинаковые → "⚠️ ОДИНАКОВЫЕ ВЫВОДЫ — спора нет"
- Если разные → "✅ СПОР НАСТОЯЩИЙ — позиции противоположны"

ШАГ 4: НЕИЗВЕСТНО
❓ [что именно неизвестно, 1-2 строки]

ШАГ 5: ФАКТЫ ДЛЯ SYNTH
📌 3 самых важных подтверждённых факта:
1. [факт + источник]
2. [факт + источник]
3. [факт + источник]
---

⛔ АБСОЛЮТНО ЗАПРЕЩЕНО — нарушение = сломанный анализ:
- Писать "Ответ на аргументы оппонента" — ты НЕ агент дебатов
- Писать "Рекомендуем", "советуем", "следует" — ты НЕ даёшь советов
- Писать "Сравнение с рынком", "ARK Invest", "консенсус" — это задача Synth
- Писать "Исторический счёт" или "Контрарианский взгляд" — это задача Synth
- Выходить за рамки 5 шагов выше
- Добавлять любые разделы которых нет в структуре выше"""


SYNTH_SYSTEM = """Ты — Consensus Synthesizer на Mistral. Думай цепочками рассуждений.

Пользователь пришёл за честным анализом, не за красивым прогнозом.
Твоя задача: прочитать весь спор и синтезировать — кто прав и почему.

СНАЧАЛА ОЦЕНИ КАЧЕСТВО ДЕБАТОВ:
- Спорили ли агенты по-настоящему или пришли к одинаковому выводу?
- Если Verifier нашёл одинаковые выводы — скажи об этом прямо
- Если один агент игнорировал аргументы другого — отметь это

ОБЯЗАТЕЛЬНАЯ СТРУКТУРА:

🌍 КОНТЕКСТ (2-3 предложения)
Только факты из данных. Инфляция = % годовых, не CPI индекс.

📊 УРОВЕНЬ НЕОПРЕДЕЛЁННОСТИ: ВЫСОКИЙ / СРЕДНИЙ / НИЗКИЙ
Объясни почему в 1-2 предложениях.

⚔️ ИТОГ ДЕБАТОВ:
Обязательно цитируй конкретные аргументы из дебатов:
"Bull привёл аргумент: '[цитата]' — это убедительно потому что [X]"
"Bear привёл аргумент: '[цитата]' — это убедительно потому что [Y]"
Главное противоречие которое осталось неразрешённым: [одно предложение]

🌐 СРАВНЕНИЕ С КОНСЕНСУСОМ (только здесь, не у Bull/Bear):
- ✅ или ⚠️ или ❓ по каждому ключевому тезису
- Если рынок единодушен — это само по себе риск (contrarian signal)

🎯 СЦЕНАРИИ (только если данных достаточно):
БАЗОВЫЙ (примерно X%): [продолжение + триггеры]
БЫЧИЙ (примерно Y%):
  → рисковые активы РАСТУТ, защитные (золото) корректируются
  → триггеры: [конкретно]
МЕДВЕЖИЙ (примерно Z%):
  → рисковые активы ПАДАЮТ, защитные (золото, доллар) РАСТУТ
  → триггеры: [конкретно]

⛔ ЗАПРЕЩЕНО путать: "золото растёт" = МЕДВЕЖИЙ сигнал, не бычий.
⛔ ЗАПРЕЩЕНО писать раздел "СРАВНЕНИЕ С РЫНКОМ" дважды — он уже есть внутри структуры.
   Один раздел 🌐 СРАВНЕНИЕ С КОНСЕНСУСОМ — и всё. Больше не повторять.

💼 ПЛАН ДЕЙСТВИЙ (максимум 3 актива):
• Актив: [тикер]
• Направление: LONG / SHORT / НАБЛЮДАТЬ
• Качество сигнала: СИЛЬНЫЙ / СЛАБЫЙ
• Вход: [цена из данных]
• Цель: [+X% от входа]
• Стоп: [-X% от входа] ← ОБЯЗАТЕЛЬНО
• Размер: не более [X]% портфеля
• Горизонт: [период]

🛡️ ЗАЩИТА: что делать если ошиблись (1-2 конкретных триггера)

⚠️ ЧЕСТНЫЙ ИТОГ (1 абзац):
Если дебаты были слабыми — скажи прямо.
Насколько можно доверять этому анализу сегодня?

ЗАПРЕЩЕНО:
- Конкретные ценовые таргеты которых нет в данных
- Вероятности без слова "примерно"
- Писать "CPI 327.46" вместо "инфляция ~3.2% годовых"
- Звучать уверенно когда данных недостаточно
- Повторять целые абзацы из дебатов — только цитаты + твой анализ"""


# ─── Базовый агент ────────────────────────────────────────────────────────────

class BaseAgent:
    def __init__(self, name: str, emoji: str, system_prompt: str, ai_method: str):
        self.name = name
        self.emoji = emoji
        self.system_prompt = system_prompt
        self.ai_method = ai_method  # "bull", "bear", "verifier", "synth"

    async def respond(
        self,
        news_context: str,
        debate_history: DebateHistory,
        round_num: int,
        extra_instruction: str = ""
    ) -> str:
        history_ctx = debate_history.context_for_agent()

        prompt = f"""КОНТЕКСТ И ДАННЫЕ:
{news_context}

ИСТОРИЯ ДЕБАТОВ (читай внимательно — ты должен отвечать на аргументы оппонента):
{history_ctx}

{f'ДОПОЛНИТЕЛЬНАЯ ИНСТРУКЦИЯ:{chr(10)}{extra_instruction}' if extra_instruction else ''}

Сейчас РАУНД {round_num} из {DEBATE_ROUNDS}.
Будь конкретен. Опирайся только на факты из данных выше.
Обязательно отвечай на аргументы оппонента из истории дебатов."""

        try:
            # Вызываем СВОЙ метод агента (разные модели!)
            caller = getattr(ai, self.ai_method)
            response = await caller(prompt=prompt, system=self.system_prompt)
            return response
        except Exception as e:
            logger.error(f"Agent {self.name} ({self.ai_method}) error: {e}")
            return f"[Ошибка агента {self.name}: {e}]"


# ─── Конкретные агенты ────────────────────────────────────────────────────────

class BullResearcher(BaseAgent):
    def __init__(self):
        super().__init__(
            name="Bull Researcher",
            emoji="🐂",
            system_prompt=BULL_SYSTEM,
            ai_method="bull"  # → Groq/Llama 70B
        )

    async def respond_counter(self, news_context: str, history: DebateHistory, round_num: int) -> str:
        """Раунд контраргументов — использует агрессивный промпт."""
        bear_args = history.last_message_by("Bear")
        extra = (
            f"Аргументы Bear которые ты ОБЯЗАН атаковать:\n{bear_args[:1500]}"
            if bear_args else ""
        )
        # Временно меняем промпт на контратакующий
        self.system_prompt = BULL_COUNTER_SYSTEM
        result = await self.respond(news_context, history, round_num, extra)
        self.system_prompt = BULL_SYSTEM
        return result


class BearSkeptic(BaseAgent):
    def __init__(self):
        super().__init__(
            name="Bear Skeptic",
            emoji="🐻",
            system_prompt=BEAR_SYSTEM,
            ai_method="bear"  # → Mistral Small
        )

    async def respond_counter(self, news_context: str, history: DebateHistory, round_num: int) -> str:
        """Раунд углубления — атакует ответ Bull и использует выводы Verifier."""
        bull_counter = history.last_message_by("Bull")
        verifier_notes = history.last_message_by("Verifier")
        extra = ""
        if bull_counter:
            extra += f"Ответ Bull который ты ОБЯЗАН опровергнуть:\n{bull_counter[:1000]}\n\n"
        if verifier_notes:
            extra += f"Verifier нашёл эти проблемы — используй их против Bull:\n{verifier_notes[:800]}"
        self.system_prompt = BEAR_COUNTER_SYSTEM
        result = await self.respond(news_context, history, round_num, extra)
        self.system_prompt = BEAR_SYSTEM
        return result


class DataVerifier(BaseAgent):
    def __init__(self):
        super().__init__(
            name="Data Verifier",
            emoji="🔍",
            system_prompt=VERIFIER_SYSTEM,
            ai_method="verifier"  # → OpenRouter/Gemma 27B
        )


class ConsensusSynth(BaseAgent):
    def __init__(self):
        super().__init__(
            name="Consensus Synthesizer",
            emoji="⚖️",
            system_prompt=SYNTH_SYSTEM,
            ai_method="synth"  # → OpenRouter/DeepSeek R1
        )


# ─── Оркестратор дебатов ──────────────────────────────────────────────────────

class DebateOrchestrator:
    """
    Порядок раундов v5.0 — настоящий спор:

    Раунд 1: Bull → Bear (первичные позиции, независимо)
    Раунд 2: Verifier проверяет оба → Bull контратакует Bear
    Раунд 3: Bear углубляет и отвечает Bull → Synth финальный синтез
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
        history = DebateHistory()
        rounds = DEBATE_ROUNDS if not custom_mode else min(DEBATE_ROUNDS, 3)

        logger.info(f"Запускаю дебаты v5.0: {rounds} раундов")

        # Собираем полный контекст
        full_context = ""
        if live_prices:
            full_context += live_prices + "\n\n"
        full_context += news_context
        if market_data:
            full_context += "\n\n" + market_data
        if profile_instruction:
            full_context += "\n\n" + profile_instruction

        # ── Раунд 1: Независимые первичные позиции ────────────────────────────
        # ВАЖНО: Bull и Bear НЕ видят друг друга в первом раунде
        # Это гарантирует независимые позиции
        logger.info("Раунд 1: Bull и Bear независимо...")

        empty_history = DebateHistory()  # пустая история для первого раунда

        bull_r1, bear_r1 = await asyncio.gather(
            self.bull.respond(full_context, empty_history, round_num=1),
            self.bear.respond(full_context, empty_history, round_num=1)
        )

        history.add(f"{self.bull.emoji} {self.bull.name}", bull_r1, 1)
        history.add(f"{self.bear.emoji} {self.bear.name}", bear_r1, 1)

        # ── Раунд 2: Verifier проверяет → Bull контратакует ───────────────────
        if rounds >= 2:
            logger.info("Раунд 2: Verifier + Bull контратака...")

            verify_r2 = await self.verifier.respond(full_context, history, round_num=2)
            history.add(f"{self.verifier.emoji} {self.verifier.name}", verify_r2, 2)

            # Bull теперь видит Bear И Verifier → должен атаковать конкретно
            bull_r2 = await self.bull.respond_counter(full_context, history, round_num=2)
            history.add(f"{self.bull.emoji} {self.bull.name}", bull_r2, 2)

        # ── Раунд 3: Bear углубляется → Synth синтез ──────────────────────────
        if rounds >= 3:
            logger.info("Раунд 3: Bear контратака + Synth синтез...")

            # Bear видит всё: свою позицию, ответ Bull, выводы Verifier
            bear_r3 = await self.bear.respond_counter(full_context, history, round_num=3)
            history.add(f"{self.bear.emoji} {self.bear.name}", bear_r3, 3)

        # ── Дополнительные раунды ─────────────────────────────────────────────
        for extra_round in range(4, rounds + 1):
            logger.info(f"Раунд {extra_round}: дополнительный спор...")
            bull_x = await self.bull.respond_counter(full_context, history, extra_round)
            history.add(f"{self.bull.emoji} {self.bull.name}", bull_x, extra_round)
            bear_x = await self.bear.respond_counter(full_context, history, extra_round)
            history.add(f"{self.bear.emoji} {self.bear.name}", bear_x, extra_round)

        # ── Финальный синтез ──────────────────────────────────────────────────
        logger.info("Финальный синтез (DeepSeek R1)...")
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
                f"_{news_context[:200]}{'...' if len(news_context) > 200 else ''}_\n\n"
            )
        else:
            title = "📊 *DIALECTIC EDGE — ЕЖЕДНЕВНЫЙ ДАЙДЖЕСТ*"
            news_section = ""

        honest_header = (
            "💬 *Прежде чем читать:*\n"
            "Это структурированный AI-анализ. 4 разные модели спорят между собой.\n"
            "🐂 Bull = Groq/Llama | 🐻 Bear = Mistral | "
            "🔍 Verifier = Gemma | ⚖️ Synth = DeepSeek R1\n"
            "Будущее неизвестно — это помощь в мышлении, не сигнал.\n"
        )

        report_parts = [
            title,
            f"🕐 _{now}_",
            "",
            honest_header,
            "─" * 30,
            "",
        ]

        if news_section:
            report_parts.append(news_section)

        report_parts.append("🗣 *ДЕБАТЫ АГЕНТОВ*\n")

        current_round = 0
        for msg in history.messages:
            if msg.round_num != current_round:
                current_round = msg.round_num
                round_labels = {
                    1: "── Раунд 1: Первичные позиции ──",
                    2: "── Раунд 2: Верификация + Контратака ──",
                    3: "── Раунд 3: Углубление спора ──",
                }
                label = round_labels.get(current_round, f"── Раунд {current_round} ──")
                report_parts.append(f"\n*{label}*\n")
            report_parts.append(f"{msg.agent}:\n{msg.content}\n")

        report_parts.append("─" * 30)
        report_parts.append("⚖️ *ИТОГОВЫЙ СИНТЕЗ И РЕКОМЕНДАЦИИ*\n")
        report_parts.append(synthesis)
        report_parts.append(DISCLAIMER)

        return "\n".join(str(p) for p in report_parts)
