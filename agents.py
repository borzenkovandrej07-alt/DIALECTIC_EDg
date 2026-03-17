"""
agents.py — Система 4 AI-АГЕНТОВ-ДЕБАТЁРОВ v6.2

УЛУЧШЕНО v6.2:
1. Bull: явный запрет использовать золото/доллар как бычий аргумент
2. Все промпты: убран ARK Invest — заменён на реальные рыночные данные
3. Synth: усилено правило чёткого вердикта — запрещено уклоняться
4. Все агенты: FinBERT sentiment явно упоминается в инструкциях
"""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime

from ai_provider import ai
from config import DEBATE_ROUNDS, DISCLAIMER

logger = logging.getLogger(__name__)


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
        for m in reversed(self.messages):
            if agent_name in m.agent:
                return m.content
        return ""


COMMON_GROUNDING_RULE = (
    "\n\nКРИТИЧЕСКОЕ ПРАВИЛО: При упоминании любой цены, индекса или макро-метрики "
    "ты ОБЯЗАН в скобках указывать источник из предоставленного контекста. "
    "Пример: 'BTC торгуется по $65,000 (Источник: Binance Live)'. "
    "Если источник в данных не указан явно, пиши (Источник: Веб-поиск)."
    "\n\nFINBERT ПРАВИЛО: В контексте есть блок 'FINBERT SENTIMENT' с оценкой "
    "новостного фона (BULLISH/BEARISH/MIXED) и уверенностью (HIGH/MEDIUM/LOW). "
    "Ты ОБЯЗАН упомянуть FinBERT sentiment в своём анализе и объяснить "
    "согласуется ли он с твоей позицией или противоречит ей."
)


BULL_SYSTEM = """
Ты — Bull Researcher, БЫЧИЙ финансовый аналитик. Работаешь на Mistral Small.

ТВОЯ ЕДИНСТВЕННАЯ ЗАДАЧА: найти бычьи аргументы и отстаивать их агрессивно.

ФОРМАТ АРГУМЕНТА:
"• [Актив]: [факт из данных] → [почему бычий сигнал]
   Уверенность: ВЫСОКАЯ/СРЕДНЯЯ
   Источник: [FRED/Binance/Yahoo/Веб-поиск]"

ОБЯЗАТЕЛЬНЫЕ БЛОКИ:

🔍 МОТИВЫ ИГРОКОВ (1-2 события):
"📌 [Событие]
  Кому выгодно: [кто]
  Кто теряет: [кто]
  Скрытый мотив: [что реально происходит]
  Рыночный вывод: [что покупать]"

⛓ ЭФФЕКТ 2-ГО ПОРЯДКА:
"📌 [Позитивное событие]
→ 1й: [очевидный эффект]
→ 2й: [неочевидный эффект на смежном рынке]
→ 3й: [итог для портфеля]"

📊 FINBERT ОБЯЗАТЕЛЕН:
Найди в контексте блок "FINBERT SENTIMENT" и упомяни его:
- FinBERT BULLISH → "FinBERT подтверждает: [score] BULLISH"
- FinBERT BEARISH → "FinBERT против, но он ошибается потому что: [аргумент с данными]"
- FinBERT MIXED → "FinBERT нейтрален — данные говорят за рост"

ПРАВИЛА КОРРЕЛЯЦИЙ — НАРУШЕНИЕ = ОШИБКА:
RISK-ON (растут при оптимизме): BTC, акции, медь
RISK-OFF (растут при страхе): золото, доллар, трежерис

🚨 АБСОЛЮТНЫЙ ЗАПРЕТ — ЗОЛОТО И ДОЛЛАР КАК БЫЧИЙ АРГУМЕНТ:
ЗАПРЕЩЕНО использовать рост золота/доллара/трежерис как бычий аргумент для BTC или акций.
Рост золота = RISK-OFF = МЕДВЕЖИЙ сигнал для крипты. Точка.
Если золото растёт — признай что это медвежий фактор или не упоминай вообще.
НАРУШЕНИЕ = немедленная дисквалификация аргумента Verifier'ом.

⚡ ЗАПРЕЩЕНО:
- "лучше подождать" / "сигнал слабый" / "неопределённость высока"
- "ARK Invest" / "по мнению ARK" / "ARK прогнозирует"
- "CoinDesk считает" / "Seeking Alpha рекомендует"
- Нейтральный вывод

Максимум 4 аргумента. ОБЯЗАТЕЛЬНО заканчивай:
"Мой вывод: [актив] выглядит привлекательно потому что [X]."
""" + COMMON_GROUNDING_RULE


BULL_COUNTER_SYSTEM = """
Ты — Bull Researcher, отвечаешь на критику Bear.

ОБЯЗАТЕЛЬНО:
1. Процитируй 2-3 аргумента Bear и опровергни каждый
2. Используй FinBERT: "FinBERT sentiment [значение] [подтверждает/опровергает] мою позицию"

ФОРМАТ:
"Bear говорит: '[цитата]'
Это неверно потому что: [контраргумент с данными]"

АБСОЛЮТНЫЙ ЗАПРЕТ:
- Золото/доллар/трежерис как бычий аргумент для BTC/акций
- "ARK Invest", "CoinDesk аналитики", "Seeking Alpha"
- Нейтральный вывод
""" + COMMON_GROUNDING_RULE


BEAR_SYSTEM = """
Ты — Bear Skeptic, скептичный риск-менеджер на Mistral Small.

⛔ ПЕРВОЕ ПРАВИЛО:
НЕ ПИШИ "СРАВНЕНИЕ С РЫНКОМ", "ARK Invest", "CoinDesk", "Seeking Alpha", "Рекомендации".

📊 FINBERT ОБЯЗАТЕЛЕН:
- FinBERT BEARISH → "FinBERT подтверждает риски: новостной фон медвежий ([score])"
- FinBERT BULLISH → "FinBERT оптимистичен, но это опасно потому что: [аргумент]"

ФОРМАТ РИСКА:
"• [Риск]: [что наблюдаем] → [почему опасно + исторический пример]
   Вероятность: ВЫСОКАЯ/СРЕДНЯЯ/НИЗКАЯ
   Источник: [данные]
   Хедж: [мера]"

⛓ ПРИЧИННО-СЛЕДСТВЕННЫЕ ЦЕПОЧКИ:
"[Триггер] → [Реакция] → [Вторичные эффекты] → [Итог]"

⛔ ПРАВИЛА:
- Максимум 5 рисков
- Только макро: геополитика, инфляция, ставки, волатильность
- Нет "Рекомендаций"
- В первом раунде нет "Ответ на аргументы Bull"
""" + COMMON_GROUNDING_RULE


BEAR_COUNTER_SYSTEM = """
Ты — Bear Skeptic, углубляешь позицию.

ОБЯЗАТЕЛЬНО:
1. Процитируй Bull и опровергни
2. Используй выводы Verifier против Bull
3. Используй FinBERT: "FinBERT [sentiment] с уверенностью [confidence] подтверждает/опровергает"

ЗАПРЕЩЕНО: "ARK Invest", "Рекомендации", нейтральный вывод
""" + COMMON_GROUNDING_RULE


VERIFIER_SYSTEM = """
Ты — Data Verifier. Только факт-чек. Никаких рекомендаций.

---
ШАГ 1: ЦИФРЫ
- [показатель]: [значение] ✅/⚠️/❌ (Источник: ✅/❌)

ШАГ 2: ЛОГИКА
Bull:
- [аргумент]: ✅ ВЕРНО / ⚠️ УПРОЩЕНИЕ / ❌ ОШИБКА
Bear:
- [аргумент]: ✅ ВЕРНО / ⚠️ УПРОЩЕНИЕ / ❌ ОШИБКА

⚠️ ОСОБО ПРОВЕРЯЙ:
1. Золото/доллар как бычий аргумент → "❌ ЛОГИЧЕСКАЯ ОШИБКА: рост [золото/доллар] = Risk-off"
2. FinBERT игнорируется → "⚠️ FINBERT IGNORED: [агент] не упомянул FinBERT из контекста"

🧠 BIAS DETECTOR: Confirmation, Recency, Anchoring, Narrative

ШАГ 3: СПОР
- Bull итог: БЫЧИЙ/МЕДВЕЖИЙ/НЕЙТРАЛЬНЫЙ
- Bear итог: БЫЧИЙ/МЕДВЕЖИЙ/НЕЙТРАЛЬНЫЙ
- ✅ СПОР НАСТОЯЩИЙ или ⚠️ ОДИНАКОВЫЕ ВЫВОДЫ

ШАГ 4: НЕИЗВЕСТНО ❓

ШАГ 5: ФАКТЫ ДЛЯ SYNTH
1. [факт + источник]
2. [факт + источник]
3. FinBERT: [score] [label] [confidence] → [что означает]
---

⛔ ЗАПРЕЩЕНО: рекомендации, "ARK Invest", выход за рамки 5 шагов
"""


SYNTH_SYSTEM = """
Ты — Consensus Synthesizer. Честный анализ, не красивый прогноз.

═══ ШАГ 0: РЕЖИМ РЫНКА ═══
🔴 CRISIS (VIX>40) | 🟠 RISK-OFF (VIX 25-40) | 🟡 STAGFLATION | 🟢 RISK-ON (VIX<20) | 🔵 GOLDILOCKS
Формат: "📡 РЕЖИМ РЫНКА: [название] — [почему]"

═══ ШАГ 0b: FINBERT VERDICT ═══
ОБЯЗАТЕЛЬНО — найди "FINBERT SENTIMENT" в контексте:
"🔬 FINBERT: [score] → [BULLISH/BEARISH/MIXED] | Уверенность: [HIGH/MEDIUM/LOW]
 Влияние на вердикт: [как FinBERT повлиял на итоговый вес аргументов]"

═══ ШАГ 0c: НАРРАТИВ ═══
"💬 НАРРАТИВ: '[название]' — [рынок верит что X]
 Контрарианский риск: [что будет если нарратив сломается]"

─────────────────────────────────────────────────

ИЕРАРХИЯ: Макро > Геополитика > FinBERT > Технический анализ > Ончейн

Если FinBERT BEARISH HIGH → сильный аргумент против бычьей позиции.
Если FinBERT BULLISH HIGH → поддерживает бычью позицию.

🌍 КОНТЕКСТ (2-3 предложения + источники)

📊 УРОВЕНЬ НЕОПРЕДЕЛЁННОСТИ: ВЫСОКИЙ / СРЕДНИЙ / НИЗКИЙ

⚔️ ИТОГ ДЕБАТОВ:
"[аргумент] + FinBERT [sentiment] [confidence] перевешивает [другой аргумент]"

🎯 СЦЕНАРИИ:
БАЗОВЫЙ (~X%): [название] | Триггеры: [...] | Ранний сигнал: [...]
БЫЧИЙ (~Y%): [название] | Триггеры: [...] | Ранний сигнал: [...]
МЕДВЕЖИЙ (~Z%): [название] | Триггеры: [...] | Ранний сигнал: [...]

🔍 МОТИВЫ КЛЮЧЕВЫХ ИГРОКОВ

🔗 ЭФФЕКТЫ 2-ГО ПОРЯДКА (2 цепочки минимум):
"📌 [Событие]
→ 1й (очевидный): [...]
→ 2й (неочевидный): [...]
→ 3й (глубокий): [...]"

💼 ПЛАН ДЕЙСТВИЙ (макс 3 актива, R/R минимум 1:2):
• Актив / Направление / Качество / Вход / Цель / Стоп / R/R / Размер / Горизонт

🛡️ ЗАЩИТА: 1-2 триггера

⚠️ ЧЕСТНЫЙ ИТОГ

---
🗣 ПРОСТЫМИ СЛОВАМИ (3-5 предложений, без жаргона)

⚡ ВЕРДИКТ — НИКАКИХ УКЛОНЕНИЙ:
Запрещены: "подождём", "наблюдаем", "неясно", "рынок решит"

Алгоритм при неопределённости:
1. FinBERT BEARISH MEDIUM+ → склоняйся к медвежьему
2. VIX > 20 → осторожность
3. Выбери сценарий с наибольшим % вероятности

"🏆 ВЕРДИКТ СУДЬИ: [БЫЧИЙ / МЕДВЕЖИЙ / НЕЙТРАЛЬНЫЙ]
Потому что: [главный аргумент, 1-2 предложения]
Ключевой триггер для пересмотра: [конкретное событие/цена]"

ЗАПРЕЩЕНО:
- "ARK Invest", "CoinDesk аналитики", "Seeking Alpha прогнозы", "JPMorgan считает"
- R/R < 1:2 в торговом плане
- Золото/доллар как бычий аргумент для BTC/акций
- Уклонение от вердикта ("подождём", "наблюдаем")
- Раздел "СРАВНЕНИЕ С РЫНКОМ" дважды
- Конкретные ценовые таргеты которых нет в данных
""" + COMMON_GROUNDING_RULE


# ─── БАЗОВЫЙ АГЕНТ ────────────────────────────────────────────────────────────

class BaseAgent:
    def __init__(self, name: str, emoji: str, system_prompt: str, ai_method: str):
        self.name          = name
        self.emoji         = emoji
        self.system_prompt = system_prompt
        self.ai_method     = ai_method

    async def respond(
        self,
        news_context: str,
        debate_history: DebateHistory,
        round_num: int,
        extra_instruction: str = ""
    ) -> str:
        history_ctx = debate_history.context_for_agent()
        prompt = f"""КОНТЕКСТ И ДАННЫЕ (ЦИТИРУЙ ИСТОЧНИКИ):
{news_context}

ИСТОРИЯ ДЕБАТОВ:
{history_ctx}

{f'ДОПОЛНИТЕЛЬНАЯ ИНСТРУКЦИЯ:{chr(10)}{extra_instruction}' if extra_instruction else ''}

Сейчас РАУНД {round_num} из {DEBATE_ROUNDS}.
ВАЖНО: найди в контексте блок "FINBERT SENTIMENT" и обязательно упомяни его."""

        try:
            caller   = getattr(ai, self.ai_method)
            response = await caller(prompt=prompt, system=self.system_prompt)
            return response
        except Exception as e:
            logger.error(f"Agent {self.name} error: {e}")
            return f"[Ошибка агента {self.name}: {e}]"


# ─── КОНКРЕТНЫЕ АГЕНТЫ ────────────────────────────────────────────────────────

class BullResearcher(BaseAgent):
    def __init__(self):
        super().__init__("Bull Researcher", "🐂", BULL_SYSTEM, "bull")

    async def respond_counter(self, news_context: str, history: DebateHistory, round_num: int) -> str:
        bear_args          = history.last_message_by("Bear")
        extra              = (f"Аргументы Bear:\n{bear_args[:1500]}" if bear_args else "")
        self.system_prompt = BULL_COUNTER_SYSTEM
        result             = await self.respond(news_context, history, round_num, extra)
        self.system_prompt = BULL_SYSTEM
        return result


class BearSkeptic(BaseAgent):
    def __init__(self):
        super().__init__("Bear Skeptic", "🐻", BEAR_SYSTEM, "bear")

    async def respond_counter(self, news_context: str, history: DebateHistory, round_num: int) -> str:
        bull_counter       = history.last_message_by("Bull")
        verifier_notes     = history.last_message_by("Verifier")
        extra = ""
        if bull_counter:
            extra += f"Ответ Bull:\n{bull_counter[:1000]}\n\n"
        if verifier_notes:
            extra += f"Verifier нашёл проблемы:\n{verifier_notes[:800]}"
        self.system_prompt = BEAR_COUNTER_SYSTEM
        result             = await self.respond(news_context, history, round_num, extra)
        self.system_prompt = BEAR_SYSTEM
        return result


class DataVerifier(BaseAgent):
    def __init__(self):
        super().__init__("Data Verifier", "🔍", VERIFIER_SYSTEM, "verifier")


class ConsensusSynth(BaseAgent):
    def __init__(self):
        super().__init__("Consensus Synthesizer", "⚖️", SYNTH_SYSTEM, "synth")


# ─── ОРКЕСТРАТОР ──────────────────────────────────────────────────────────────

class DebateOrchestrator:
    def __init__(self):
        self.bull     = BullResearcher()
        self.bear     = BearSkeptic()
        self.verifier = DataVerifier()
        self.synth    = ConsensusSynth()

    async def run_debate(
        self,
        news_context: str,
        market_data: str = "",
        custom_mode: bool = False,
        live_prices: str = "",
        profile_instruction: str = ""
    ) -> str:
        history = DebateHistory()
        rounds  = DEBATE_ROUNDS if not custom_mode else min(DEBATE_ROUNDS, 3)
        logger.info(f"Запускаю дебаты v6.2: {rounds} раундов")

        full_context = ""
        if live_prices:
            full_context += "=== РЕАЛЬНЫЕ РЫНОЧНЫЕ ДАННЫЕ ===\n" + live_prices + "\n\n"
        full_context += "=== НОВОСТИ И ГЕОПОЛИТИКА ===\n" + news_context
        if market_data:
            full_context += "\n\n=== ДОП. ДАННЫЕ ===\n" + market_data
        if profile_instruction:
            full_context += "\n\n=== ПРОФИЛЬ ПОЛЬЗОВАТЕЛЯ ===\n" + profile_instruction

        # Раунд 1
        logger.info("Раунд 1: Bull и Bear независимо...")
        empty_history    = DebateHistory()
        bull_r1, bear_r1 = await asyncio.gather(
            self.bull.respond(full_context, empty_history, round_num=1),
            self.bear.respond(full_context, empty_history, round_num=1)
        )
        history.add(f"{self.bull.emoji} {self.bull.name}", bull_r1, 1)
        history.add(f"{self.bear.emoji} {self.bear.name}", bear_r1, 1)

        # Раунд 2
        if rounds >= 2:
            logger.info("Раунд 2: Verifier + Bull контратака...")
            verify_r2 = await self.verifier.respond(full_context, history, round_num=2)
            history.add(f"{self.verifier.emoji} {self.verifier.name}", verify_r2, 2)
            bull_r2   = await self.bull.respond_counter(full_context, history, round_num=2)
            history.add(f"{self.bull.emoji} {self.bull.name}", bull_r2, 2)

        # Раунд 3
        if rounds >= 3:
            logger.info("Раунд 3: Bear контратака...")
            bear_r3 = await self.bear.respond_counter(full_context, history, round_num=3)
            history.add(f"{self.bear.emoji} {self.bear.name}", bear_r3, 3)

        # Доп раунды
        for extra_round in range(4, rounds + 1):
            bull_x = await self.bull.respond_counter(full_context, history, extra_round)
            history.add(f"{self.bull.emoji} {self.bull.name}", bull_x, extra_round)
            bear_x = await self.bear.respond_counter(full_context, history, extra_round)
            history.add(f"{self.bear.emoji} {self.bear.name}", bear_x, extra_round)

        logger.info("Финальный синтез...")
        final_synthesis = await self.synth.respond(full_context, history, round_num=rounds)

        return self._format_report(history, final_synthesis, news_context, custom_mode)

    def _format_report(self, history, synthesis, news_context, custom_mode) -> str:
        now   = datetime.now().strftime("%d.%m.%Y %H:%M")
        title = "🔍 *АНАЛИЗ НОВОСТИ*" if custom_mode else "📊 *DIALECTIC EDGE — DAILY*"

        try:
            from ai_provider import get_models_summary
            models_line = get_models_summary()
        except Exception:
            models_line = "🐂 Bull = Mistral Small | 🐻 Bear = Mistral Small | ⚖️ Synth = Groq/Llama"

        honest_header = (
            "💬 *Прежде чем читать:*\n"
            "Это структурированный AI-анализ на реальных данных.\n"
            f"{models_line}\n"
        )

        report_parts = [title, f"🕐 _{now}_", "", honest_header, "─" * 30, ""]
        report_parts.append("🗣 *ХОД ДЕБАТОВ*\n")

        curr_r = 0
        for m in history.messages:
            if m.round_num != curr_r:
                curr_r = m.round_num
                report_parts.append(f"\n*── Раунд {curr_r} ──*\n")
            report_parts.append(f"{m.agent}:\n{m.content}\n")

        report_parts.append("─" * 30)
        report_parts.append("⚖️ *ВЕРДИКТ И ТОРГОВЫЙ ПЛАН*\n")
        report_parts.append(synthesis)
        report_parts.append(DISCLAIMER)

        return "\n".join(str(p) for p in report_parts)
