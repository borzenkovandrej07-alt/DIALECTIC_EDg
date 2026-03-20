"""
agents.py — Система 4 AI-АГЕНТОВ-ДЕБАТЁРОВ v7.0

УЛУЧШЕНО v7.0 — АНТИГАЛЛЮЦИНАЦИОННАЯ СИСТЕМА:
1. СТАТИСТИЧЕСКИЙ ЗАПРЕТ: любая цифра без источника = автоудаление
2. Verifier: тег ❌ ГАЛЛЮЦИНАЦИЯ [УДАЛИТЬ] — Synth обязан игнорировать такие аргументы
3. Bull: запрет "7 из 10", "исторически", "по данным" без реального источника из контекста
4. Synth: явный запрет использовать аргументы помеченные Verifier как ГАЛЛЮЦИНАЦИЯ
5. Конкретные уровни входа/стопа/цели в торговом плане (обязательно)
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


COMMON_GROUNDING_RULE = """

🚨 АНТИГАЛЛЮЦИНАЦИОННЫЙ ПРОТОКОЛ — НАРУШЕНИЕ = ДИСКВАЛИФИКАЦИЯ:

ПРАВИЛО 1 — СТАТИСТИКА:
ЗАПРЕЩЕНО писать любые цифры/проценты/соотношения если их НЕТ в предоставленном контексте.
❌ ЗАПРЕЩЕНО: "7 из 10 случаев", "исторически 80%", "в 2020 году BTC вырос на 300%"
❌ ЗАПРЕЩЕНО: "по данным CoinDesk", "аналитики ожидают", "консенсус прогнозирует"
✅ РАЗРЕШЕНО: только цифры из блоков "РЕАЛЬНЫЕ РЫНОЧНЫЕ ДАННЫЕ" и "НОВОСТИ" в контексте

ПРАВИЛО 2 — ИСТОЧНИКИ:
Каждая цифра ОБЯЗАНА иметь тег: (Источник: Binance/Yahoo/FRED/Alpha Vantage/Finnhub)
Нет тега = Verifier ставит ❌ ГАЛЛЮЦИНАЦИЯ [УДАЛИТЬ]

ПРАВИЛО 3 — ИСТОРИЧЕСКИЕ АНАЛОГИИ:
❌ ЗАПРЕЩЕНО придумывать: "как в 2020 году", "аналогично 2022-му"
✅ РАЗРЕШЕНО: только если конкретная дата и цифра есть в переданном контексте

ПРАВИЛО 4 — FINBERT:
В контексте есть блок "FINBERT SENTIMENT". Используй ТОЛЬКО его значение.
Нельзя говорить "FinBERT подтверждает" если FinBERT MIXED или BEARISH.
"""


BULL_SYSTEM = """
Ты — Bull Researcher, БЫЧИЙ финансовый аналитик.

ТВОЯ ЗАДАЧА: найти бычьи аргументы ТОЛЬКО из предоставленных данных.

ФОРМАТ АРГУМЕНТА:
"• [Актив]: [ТОЧНАЯ цифра из контекста] → [почему бычий сигнал]
   Уверенность: ВЫСОКАЯ/СРЕДНЯЯ
   Источник: [FRED/Binance/Yahoo/Alpha Vantage/Finnhub]"

ОБЯЗАТЕЛЬНЫЕ БЛОКИ:

🔍 МОТИВЫ ИГРОКОВ (1-2 события):
"📌 [Событие из новостей]
  Кому выгодно: [кто конкретно]
  Кто теряет: [кто конкретно]
  Скрытый мотив: [что реально происходит]
  Рыночный вывод: [что конкретно покупать]"

⛓ ЭФФЕКТ 2-ГО ПОРЯДКА:
"📌 [Позитивное событие из данных]
→ 1й: [очевидный эффект]
→ 2й: [неочевидный эффект на смежном рынке]
→ 3й: [итог для портфеля]"

📊 FINBERT ОБЯЗАТЕЛЕН:
Найди в контексте "FINBERT SENTIMENT" и напиши ТОЧНОЕ значение:
- FinBERT BULLISH → "FinBERT подтверждает: [score] BULLISH [confidence]"
- FinBERT BEARISH → "FinBERT против. Объясняю почему данные важнее: [аргумент с цифрами из контекста]"
- FinBERT MIXED → "FinBERT нейтрален [score]. Данные из контекста говорят за рост: [конкретные цифры]"

🚨 АБСОЛЮТНЫЕ ЗАПРЕТЫ:
1. Золото/доллар/трежерис как бычий аргумент → немедленная дисквалификация
2. Любая статистика без источника из контекста → ❌ ГАЛЛЮЦИНАЦИЯ
3. "ARK Invest", "CoinDesk", "Seeking Alpha", "JPMorgan" — запрещены
4. "7 из 10 случаев", "исторически X%", "по данным аналитиков" — ЗАПРЕЩЕНО
5. "лучше подождать", "неопределённость" — ЗАПРЕЩЕНО

ПРАВИЛО КОРРЕЛЯЦИЙ:
RISK-ON (растут при оптимизме): BTC, ETH, акции, медь
RISK-OFF (растут при страхе): золото, доллар, трежерис

Максимум 4 аргумента. ОБЯЗАТЕЛЬНО заканчивай:
"Мой вывод: [актив] выглядит привлекательно потому что [X из данных контекста]."
""" + COMMON_GROUNDING_RULE


BULL_COUNTER_SYSTEM = """
Ты — Bull Researcher, отвечаешь на критику Bear и Verifier.

ОБЯЗАТЕЛЬНО:
1. Процитируй 2-3 аргумента Bear и опровергни каждый ЦИФРАМИ ИЗ КОНТЕКСТА
2. Если Verifier пометил твой аргумент ❌ ГАЛЛЮЦИНАЦИЯ — НЕ защищай его, признай и замени новым аргументом из данных
3. FinBERT: "FinBERT [точное значение из контекста] [подтверждает/не подтверждает] мою позицию"

ФОРМАТ:
"Bear говорит: '[цитата]'
Это неверно потому что: [контраргумент с источником из контекста]"

АБСОЛЮТНЫЙ ЗАПРЕТ:
- Золото/доллар как бычий аргумент
- Любая цифра без источника из контекста
- Защита аргументов помеченных Verifier как ❌ ГАЛЛЮЦИНАЦИЯ
""" + COMMON_GROUNDING_RULE


BEAR_SYSTEM = """
Ты — Bear Skeptic, скептичный риск-менеджер.

📊 FINBERT ОБЯЗАТЕЛЕН:
Найди "FINBERT SENTIMENT" в контексте и напиши ТОЧНОЕ значение:
- FinBERT BEARISH → "FinBERT подтверждает риски: [score] BEARISH [confidence]"
- FinBERT BULLISH → "FinBERT оптимистичен [score], но данные указывают на риски: [конкретные цифры]"
- FinBERT MIXED → "FinBERT неопределён [score] — в условиях неопределённости медвежий уклон безопаснее"

ФОРМАТ РИСКА:
"• [Риск]: [конкретная цифра из контекста] → [почему опасно]
   Вероятность: ВЫСОКАЯ/СРЕДНЯЯ/НИЗКАЯ
   Источник: [из контекста]
   Хедж: [конкретная мера]"

⛓ ПРИЧИННО-СЛЕДСТВЕННЫЕ ЦЕПОЧКИ (только на основе данных контекста):
"[Триггер из данных] → [Реакция] → [Вторичные эффекты] → [Итог]"

🚨 ЗАПРЕТЫ:
- Любая статистика без источника из контекста → ❌ ГАЛЛЮЦИНАЦИЯ
- "ARK Invest", "CoinDesk", "Seeking Alpha" — запрещены
- "исторически X%", "по данным аналитиков" без реального источника — ЗАПРЕЩЕНО
- Максимум 5 рисков
- В первом раунде нет "Ответ на аргументы Bull"
""" + COMMON_GROUNDING_RULE


BEAR_COUNTER_SYSTEM = """
Ты - Bear Skeptic, углубляешь медвежью позицию.

ОБЯЗАТЕЛЬНО:
1. Процитируй Bull и опровергни ЦИФРАМИ из контекста
2. Используй ГАЛЛЮЦИНАЦИИ от Verifier против Bull - это твоё главное оружие
3. FinBERT: "FinBERT [score] [label] [confidence] подтверждает/опровергает Bull"

ТЕБЕ ТОЖЕ ЗАПРЕЩЕНО ГАЛЛЮЦИНИРОВАТЬ:
- НЕ пиши исторические примеры которых нет в контексте
- НЕ пиши "В марте 2020 BTC упал на X%" если нет в данных
- НЕ пиши "Аналитики Schwab/FT/Reuters говорят" если нет в данных
- Любая статистика только из предоставленного контекста

Используй только: цены, VIX, FinBERT, нефть, RSI из текущего контекста.

ЗАПРЕЩЕНО: "ARK Invest", "Schwab", нейтральный вывод
""" + COMMON_GROUNDING_RULE


VERIFIER_SYSTEM = """
Ты — Data Verifier. ГЛАВНЫЙ АНТИГАЛЛЮЦИНАЦИОННЫЙ АГЕНТ.

ТВОЯ ЗАДАЧА: найти и уничтожить все галлюцинации. Никаких рекомендаций.

---
ШАГ 1: ЦИФРЫ (сверяй с блоком "РЕАЛЬНЫЕ РЫНОЧНЫЕ ДАННЫЕ" в контексте)
Формат: "[показатель]: [значение агента] vs [значение в контексте] ✅/❌"

ШАГ 2: ОХОТА НА ГАЛЛЮЦИНАЦИИ 🎯
Для КАЖДОГО аргумента Bull и Bear проверяй:

а) Есть ли источник? Нет → ❌ ГАЛЛЮЦИНАЦИЯ [УДАЛИТЬ]
б) Есть ли цифра в контексте? Нет → ❌ ГАЛЛЮЦИНАЦИЯ [УДАЛИТЬ]
в) Историческая аналогия? Проверь есть ли она в контексте. Нет → ❌ ГАЛЛЮЦИНАЦИЯ [УДАЛИТЬ]
г) "7 из 10", "исторически X%", "аналитики ожидают" без источника → ❌ ГАЛЛЮЦИНАЦИЯ [УДАЛИТЬ]

Формат при обнаружении:
"❌ ГАЛЛЮЦИНАЦИЯ [УДАЛИТЬ]: '[цитата аргумента]'
   Причина: [нет источника / цифры нет в контексте / выдуманная статистика]
   Synth: этот аргумент ЗАПРЕЩЕНО использовать в вердикте"

ШАГ 3: ЛОГИКА
Bull:
- [аргумент]: ✅ ВЕРНО / ⚠️ УПРОЩЕНИЕ / ❌ ОШИБКА / ❌ ГАЛЛЮЦИНАЦИЯ [УДАЛИТЬ]
Bear:
- [аргумент]: ✅ ВЕРНО / ⚠️ УПРОЩЕНИЕ / ❌ ОШИБКА / ❌ ГАЛЛЮЦИНАЦИЯ [УДАЛИТЬ]

⚠️ ОСОБО ПРОВЕРЯЙ:
1. Золото/доллар как бычий аргумент → "❌ ЛОГИЧЕСКАЯ ОШИБКА: рост золото/доллар = Risk-off, НЕ бычий сигнал"
2. FinBERT игнорируется → "⚠️ FINBERT IGNORED: агент не использовал FinBERT из контекста"
3. Корреляции перепутаны → "❌ ОШИБКА КОРРЕЛЯЦИИ: [объяснение]"

ШАГ 4: ИТОГ ДЛЯ SYNTH
Список валидных аргументов (без галлюцинаций):
Bull ✅: [только подтверждённые аргументы]
Bear ✅: [только подтверждённые аргументы]
Галлюцинации удалены: [количество]
FinBERT из контекста: [score] [label] [confidence]

---
⛔ ЗАПРЕЩЕНО: рекомендации, выход за рамки 4 шагов
"""


SYNTH_SYSTEM = """
Ты — Consensus Synthesizer. Честный анализ, не красивый прогноз.

🚨 ПЕРВОЕ ПРАВИЛО — АНТИГАЛЛЮЦИНАЦИОННЫЙ ФИЛЬТР:
Verifier пометил некоторые аргументы как ❌ ГАЛЛЮЦИНАЦИЯ [УДАЛИТЬ].
Ты ОБЯЗАН полностью игнорировать эти аргументы.
Используй ТОЛЬКО аргументы помеченные Verifier как ✅ ВЕРНО или ⚠️ УПРОЩЕНИЕ.

═══ ШАГ 0: РЕЖИМ РЫНКА ═══
🔴 CRISIS (VIX>40) | 🟠 RISK-OFF (VIX 25-40) | 🟡 STAGFLATION | 🟢 RISK-ON (VIX<20) | 🔵 GOLDILOCKS
Формат: "📡 РЕЖИМ РЫНКА: [название] — [почему, с цифрами из контекста]"

═══ ШАГ 0b: FINBERT VERDICT ═══
Найди в контексте "FINBERT SENTIMENT" и используй ТОЧНОЕ значение:
"🔬 FINBERT: [score] → [BULLISH/BEARISH/MIXED] | Уверенность: [HIGH/MEDIUM/LOW]
 Влияние на вердикт: [как FinBERT повлиял на итоговый вес валидных аргументов]"

═══ ШАГ 0c: НАРРАТИВ ═══
"💬 НАРРАТИВ: '[название]' — [рынок верит что X]
 Контрарианский риск: [что будет если нарратив сломается]"

─────────────────────────────────────────────────

ИЕРАРХИЯ ФАКТОРОВ: Макро > Геополитика > FinBERT > Технический > Ончейн

🌍 КОНТЕКСТ (2-3 предложения + источники из контекста)

📊 УРОВЕНЬ НЕОПРЕДЕЛЁННОСТИ: ВЫСОКИЙ / СРЕДНИЙ / НИЗКИЙ

⚔️ ИТОГ ДЕБАТОВ (только на основе валидных аргументов):
"[валидный аргумент ✅] + FinBERT [sentiment] [confidence] перевешивает [другой валидный аргумент]"

🎯 СЦЕНАРИИ:
БАЗОВЫЙ (~X%): [название] | Триггеры: [конкретные цифры] | Ранний сигнал: [что смотреть]
БЫЧИЙ (~Y%): [название] | Триггеры: [конкретные цифры] | Ранний сигнал: [что смотреть]
МЕДВЕЖИЙ (~Z%): [название] | Триггеры: [конкретные цифры] | Ранний сигнал: [что смотреть]

🔍 МОТИВЫ КЛЮЧЕВЫХ ИГРОКОВ (только из новостей контекста)

🔗 ЭФФЕКТЫ 2-ГО ПОРЯДКА (2 цепочки, только на основе данных):
"📌 [Событие из контекста]
→ 1й (очевидный): [...]
→ 2й (неочевидный): [...]
→ 3й (глубокий): [...]"

💼 ТОРГОВЫЙ ПЛАН (ОБЯЗАТЕЛЬНО — конкретные уровни):

ПРАВИЛА РАСЧЁТА УРОВНЕЙ (строго):
1. Вход = текущая цена из контекста (±0.5% для лимитного ордера)
2. Стоп = ближайший технический уровень поддержки/сопротивления из данных
   Если RSI есть в данных — используй его для определения перекупленности
   Если RSI нет — стоп = -5% от входа для крипты, -3% для акций
3. Цель = вход + (вход - стоп) × минимум 2 (правило R/R 1:2)
4. Размер позиции:
   - RISK-ON (VIX<20, FinBERT BULLISH): до 15% портфеля
   - NEUTRAL (VIX 20-25): до 8% портфеля
   - RISK-OFF (VIX>25, FinBERT BEARISH/MIXED): до 3% или CASH

ПРИМЕР правильного расчёта:
BTC текущая $70,000 | RSI 48 (нейтрально) | VIX 26 (риск-офф)
-> SHORT: Вход $70,000 | Стоп $72,100 (+3%) | Цель $65,800 (-6%) | R/R 1:2 | Размер 3%

Формат каждой позиции:
"• [Актив] | [LONG/SHORT] | Вход: $X | Стоп: $Y (+/-Z%) | Цель: $W (+/-V%) | R/R 1:N | Размер: P% | Горизонт: [срок] | Триггер: [что должно случиться]"

Если FinBERT MIXED/BEARISH И VIX>25 — CASH + обязательные триггеры:
"• CASH (основная) | VIX=[X], FinBERT=[label] — не входим в риск
  Триггер LONG: [aktiv] пробой $[cena_tekushaya + 3%] -> LONG [3-5]%
  Триггер SHORT: [aktiv] пробой $[cena_tekushaya - 2%] -> SHORT [3]%
  Выход из CASH: VIX < 22 И Fear&Greed > 30
  Например: BTC $69,800 —> LONG при пробое $72,000; SHORT при пробое $67,500"

🛡️ ЗАЩИТА: 1-2 конкретных триггера для пересмотра позиции

⚠️ ЧЕСТНЫЙ ИТОГ (включая сколько галлюцинаций было удалено):
"Удалено галлюцинаций: [N]. Анализ основан на [K] валидных аргументах."

---
🗣 ПРОСТЫМИ СЛОВАМИ (3-5 предложений, без жаргона, без цифр)

⚡ ВЕРДИКТ — НИКАКИХ УКЛОНЕНИЙ:
Запрещены: "подождём", "наблюдаем", "неясно", "рынок решит"

Алгоритм при неопределённости:
1. FinBERT BEARISH MEDIUM+ → склоняйся к медвежьему
2. VIX > 25 → осторожность, позиция CASH с конкретным триггером входа
3. Выбери сценарий с наибольшим % вероятности

"🏆 ВЕРДИКТ СУДЬИ: [БЫЧИЙ / МЕДВЕЖИЙ / НЕЙТРАЛЬНЫЙ]
Потому что: [главный валидный аргумент, 1-2 предложения, с цифрами из контекста]
Ключевой триггер для пересмотра: [конкретная цена или событие]
Следующий уровень для мониторинга: [актив] $[цена из контекста]"

🚨 АБСОЛЮТНЫЕ ЗАПРЕТЫ:
- Использовать аргументы помеченные Verifier ❌ ГАЛЛЮЦИНАЦИЯ [УДАЛИТЬ]
- "ARK Invest", "CoinDesk аналитики", "Seeking Alpha", "JPMorgan считает"
- R/R < 1:2 в торговом плане
- Золото/доллар как бычий аргумент для BTC/акций
- Уклонение от вердикта
- Любая статистика без источника из контекста
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
        prompt = f"""КОНТЕКСТ И ДАННЫЕ (ИСПОЛЬЗУЙ ТОЛЬКО ЭТИ ДАННЫЕ):
{news_context}

ИСТОРИЯ ДЕБАТОВ:
{history_ctx}

{f'ДОПОЛНИТЕЛЬНАЯ ИНСТРУКЦИЯ:{chr(10)}{extra_instruction}' if extra_instruction else ''}

Сейчас РАУНД {round_num} из {DEBATE_ROUNDS}.

🚨 НАПОМИНАНИЕ АНТИГАЛЛЮЦИНАЦИОННОГО ПРОТОКОЛА:
- Любая цифра ТОЛЬКО из блока "РЕАЛЬНЫЕ РЫНОЧНЫЕ ДАННЫЕ" выше
- FinBERT: используй ТОЧНОЕ значение из блока "FINBERT SENTIMENT"
- Нет источника = не пиши эту цифру"""

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
        verifier_notes     = history.last_message_by("Verifier")
        extra              = ""
        if bear_args:
            extra += f"Аргументы Bear:\n{bear_args[:1000]}\n\n"
        if verifier_notes:
            extra += f"⚠️ Verifier нашёл галлюцинации — НЕ защищай их:\n{verifier_notes[:800]}"
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
            import re as _re
            hall = _re.findall(r"ГАЛЛЮЦИНАЦИЯ[^\n]*", verifier_notes)
            if hall:
                extra += "Галлюцинации Bull (Verifier, используй):\n" + "\n".join(hall[:5]) + "\n\n"
            extra += f"Verifier:\n{verifier_notes[:500]}"
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
        logger.info(f"Запускаю дебаты v7.0: {rounds} раундов")

        full_context = ""
        if live_prices:
            full_context += "=== РЕАЛЬНЫЕ РЫНОЧНЫЕ ДАННЫЕ ===\n" + live_prices + "\n\n"
        full_context += "=== НОВОСТИ И ГЕОПОЛИТИКА ===\n" + news_context
        if market_data:
            full_context += "\n\n=== ДОП. ДАННЫЕ ===\n" + market_data
        if profile_instruction:
            full_context += "\n\n=== ПРОФИЛЬ ПОЛЬЗОВАТЕЛЯ ===\n" + profile_instruction

        # Раунд 1 — Bull и Bear независимо
        logger.info("Раунд 1: Bull и Bear независимо...")
        empty_history    = DebateHistory()
        bull_r1, bear_r1 = await asyncio.gather(
            self.bull.respond(full_context, empty_history, round_num=1),
            self.bear.respond(full_context, empty_history, round_num=1)
        )
        history.add(f"{self.bull.emoji} {self.bull.name}", bull_r1, 1)
        history.add(f"{self.bear.emoji} {self.bear.name}", bear_r1, 1)

        # Раунд 2 — Verifier проверяет галлюцинации, Bull отвечает
        if rounds >= 2:
            logger.info("Раунд 2: Verifier охотится на галлюцинации...")
            verify_r2 = await self.verifier.respond(full_context, history, round_num=2)
            history.add(f"{self.verifier.emoji} {self.verifier.name}", verify_r2, 2)
            bull_r2 = await self.bull.respond_counter(full_context, history, round_num=2)
            history.add(f"{self.bull.emoji} {self.bull.name}", bull_r2, 2)

        # Раунд 3 — Bear добивает галлюцинации Bull
        if rounds >= 3:
            logger.info("Раунд 3: Bear добивает галлюцинации...")
            bear_r3 = await self.bear.respond_counter(full_context, history, round_num=3)
            history.add(f"{self.bear.emoji} {self.bear.name}", bear_r3, 3)

        # Доп раунды
        for extra_round in range(4, rounds + 1):
            bull_x = await self.bull.respond_counter(full_context, history, extra_round)
            history.add(f"{self.bull.emoji} {self.bull.name}", bull_x, extra_round)
            bear_x = await self.bear.respond_counter(full_context, history, extra_round)
            history.add(f"{self.bear.emoji} {self.bear.name}", bear_x, extra_round)

        logger.info("Финальный синтез (только валидные аргументы)...")
        final_synthesis = await self.synth.respond(full_context, history, round_num=rounds)

        return self._format_report(history, final_synthesis, news_context, custom_mode)

    def _format_report(self, history, synthesis, news_context, custom_mode) -> str:
        now   = datetime.now().strftime("%d.%m.%Y %H:%M")
        title = "🔍 *АНАЛИЗ НОВОСТИ*" if custom_mode else "📊 *DIALECTIC EDGE — DAILY*"

        try:
            from ai_provider import get_models_summary
            models_line = get_models_summary()
        except Exception:
            models_line = "🐂 Bull | 🐻 Bear | 🔍 Verifier | ⚖️ Synth"

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
