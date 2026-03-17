"""
sentiment.py — Sentiment scoring с FinBERT (Hugging Face) + keyword fallback.

ИСПРАВЛЕНО v4:
- ГЛАВНЫЙ БАГ: батч возвращал 1 результат → код добавлял его в results
  и возвращал results (len=1) вместо запуска одиночных запросов.
  Фикс: при плоском списке ставим флаг need_singles=True и НЕ добавляем в results.

- _finbert_single: одиночные параллельные запросы если батч не работает
- TIMEOUT = 45 сек (HF модель просыпается ~20-30 сек)
- _aggregate_finbert: EXTREME → HIGH → MEDIUM → LOW (правильный порядок)
- Порог MEDIUM снижен с 0.55 до 0.50
"""

import asyncio
import logging
import os
import re
from dataclasses import dataclass

import aiohttp

logger = logging.getLogger(__name__)
logger.info("🔬 sentiment.py v4 loaded — single requests enabled")

HF_TOKEN   = os.getenv("HF_TOKEN", "")
HF_API_URL = "https://router.huggingface.co/hf-inference/models/ProsusAI/finbert/pipeline/text-classification"
TIMEOUT    = aiohttp.ClientTimeout(total=45)

MAX_HEADLINES = 15


@dataclass
class SentimentResult:
    score: float
    label: str
    confidence: str
    bull_signals: int
    bear_signals: int
    summary: str
    source: str


RU_EN_MAP = {
    "рост": "growth", "растёт": "rises", "вырос": "surged",
    "повысился": "increased", "прибыль": "profit", "прорыв": "breakthrough",
    "максимум": "high", "одобрен": "approved", "купил": "bought",
    "покупка": "buying", "инвестиции": "investment", "партнёрство": "partnership",
    "снизил ставку": "rate cut", "смягчение": "easing", "халвинг": "halving",
    "институциональный": "institutional",
    "падение": "decline", "упал": "fell", "снизился": "decreased",
    "убыток": "loss", "банкротство": "bankruptcy", "запрет": "ban",
    "санкции": "sanctions", "арест": "arrest", "взлом": "hack",
    "кризис": "crisis", "инфляция": "inflation", "рецессия": "recession",
    "повысил ставку": "rate hike", "ужесточение": "tightening",
    "регуляция": "regulation", "обвал": "crash", "коллапс": "collapse",
    "война": "war", "эскалация": "escalation", "конфликт": "conflict",
    "нефть": "oil", "геополитика": "geopolitics",
    "ожидает": "expects", "возможно": "possibly", "вероятно": "likely",
    "неопределённость": "uncertainty", "может": "may", "если": "if",
    "рынок": "market", "акции": "stocks", "биткоин": "bitcoin",
    "индекс": "index", "ставка": "rate", "доллар": "dollar",
    "рубль": "ruble", "золото": "gold",
}


def _ru_to_en(text: str) -> str:
    result = text.lower()
    for ru, en in sorted(RU_EN_MAP.items(), key=lambda x: -len(x[0])):
        result = result.replace(ru, en)
    return result


def _extract_headlines(text: str) -> list[str]:
    headlines   = []
    short_lines = []
    long_lines  = []

    skip_prefixes = [
        "http", "источник:", "source:", "summary:", "уверенность",
        "вероятность", "хедж:", "📊", "🐂", "🐻", "⚠️",
        "источников:", "новостей:", "tavily", "===", "---",
    ]
    skip_words = [
        "summary", "источник", "вероятность", "хедж",
        "уверенность", "направление", "горизонт", "как действовать",
    ]

    for line in text.split("\n"):
        raw = line.strip()
        if not raw or len(raw) < 12:
            continue

        clean = re.sub(r"[*_`#\[\]()]", "", raw).strip()
        cl    = clean.lower()

        if any(cl.startswith(p.lower()) for p in skip_prefixes):
            continue
        if any(w in cl for w in skip_words):
            continue

        if raw.startswith("•") or raw.startswith("– ") or raw.startswith("- "):
            title = re.sub(r"^[•–\-]+\s*", "", clean).strip()
            if 15 <= len(title) <= 200:
                headlines.append(title)
        elif 15 <= len(clean) <= 120:
            short_lines.append(clean)
        elif 120 < len(clean) <= 300:
            long_lines.append(clean)

    result = []
    seen   = set()
    for line in headlines + short_lines + long_lines:
        key = line[:40].lower()
        if key not in seen:
            seen.add(key)
            result.append(line)
        if len(result) >= MAX_HEADLINES:
            break

    return result


async def _finbert_single(headline: str, session: aiohttp.ClientSession, headers: dict) -> dict | None:
    """Отправляет ОДИН заголовок в FinBERT. Надёжнее батча."""
    try:
        payload = {"inputs": headline}
        async with session.post(HF_API_URL, json=payload, headers=headers, timeout=TIMEOUT) as resp:
            if resp.status == 200:
                data = await resp.json()
                if isinstance(data, list) and len(data) > 0:
                    if isinstance(data[0], dict) and "label" in data[0]:
                        # Плоский список [{label,score}, ...] — один заголовок
                        return {d["label"].lower(): d["score"] for d in data}
                    elif isinstance(data[0], list):
                        # Вложенный список — берём первый элемент
                        return {d["label"].lower(): d["score"] for d in data[0]}
            return None
    except Exception:
        return None


async def _finbert_score(headlines: list[str]) -> list[dict] | None:
    if not HF_TOKEN:
        return None

    en_headlines = [_ru_to_en(h) for h in headlines]
    headers = {
        "Authorization": f"Bearer {HF_TOKEN}",
        "Content-Type": "application/json",
    }

    # Шаг 1: пробуем батч
    need_singles = False
    try:
        async with aiohttp.ClientSession() as session:
            payload = {"inputs": en_headlines}
            async with session.post(
                HF_API_URL, json=payload, headers=headers, timeout=TIMEOUT
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    logger.info(
                        f"FinBERT batch response: type={type(data).__name__}, "
                        f"len={len(data) if isinstance(data, list) else 0}, "
                        f"first={str(data[0])[:80] if isinstance(data, list) and data else 'empty'}"
                    )

                    if isinstance(data, list) and len(data) > 0:
                        first = data[0]

                        if isinstance(first, list):
                            # ✅ Батч работает: [[{label,score},...], ...]
                            results = []
                            for item in data:
                                scores = {d["label"].lower(): d["score"] for d in item}
                                results.append(scores)
                            logger.info(f"✅ FinBERT батч: {len(results)} заголовков")
                            return results

                        elif isinstance(first, dict) and "label" in first:
                            # ⚠️ ИСПРАВЛЕНО: HF вернул плоский список — ответ только на 1 заголовок
                            # НЕ добавляем в results — запускаем одиночные запросы
                            logger.warning(
                                "FinBERT батч → плоский список (1 заголовок) — "
                                "запускаю одиночные запросы для всех 15"
                            )
                            need_singles = True

                        else:
                            logger.warning(f"FinBERT неизвестный формат батча: {str(data)[:200]}")
                            need_singles = True

                elif resp.status == 503:
                    logger.warning("FinBERT: модель загружается (503)")
                    return None
                else:
                    err = await resp.text()
                    logger.warning(f"FinBERT API {resp.status}: {err[:100]}")
                    return None

    except asyncio.TimeoutError:
        logger.warning("FinBERT: timeout на батче, пробую одиночные запросы")
        need_singles = True
    except Exception as e:
        logger.warning(f"FinBERT батч error: {e}, пробую одиночные запросы")
        need_singles = True

    # Шаг 2: одиночные параллельные запросы (если батч не сработал)
    if need_singles:
        logger.info(f"FinBERT: отправляю {min(len(en_headlines), 10)} заголовков по одному...")
        async with aiohttp.ClientSession() as session:
            tasks = [
                _finbert_single(h, session, headers)
                for h in en_headlines[:10]  # макс 10 чтобы не спамить API
            ]
            raw_results = await asyncio.gather(*tasks)

        results = [r for r in raw_results if r is not None]
        logger.info(f"✅ FinBERT одиночные: {len(results)}/{min(len(en_headlines),10)} успешно")
        return results if results else None

    return None


def _aggregate_finbert(results: list[dict]) -> tuple[float, str, str]:
    if not results:
        return 0.0, "MIXED", "LOW"

    total_positive = 0.0
    total_negative = 0.0
    total_neutral  = 0.0
    total_weight   = 0.0

    for i, r in enumerate(results):
        weight = 1.0 / (1 + i * 0.1)
        total_positive += r.get("positive", 0) * weight
        total_negative += r.get("negative", 0) * weight
        total_neutral  += r.get("neutral",  0) * weight
        total_weight   += weight

    if total_weight == 0:
        return 0.0, "MIXED", "LOW"

    pos = total_positive / total_weight
    neg = total_negative / total_weight
    neu = total_neutral  / total_weight
    n   = len(results)
    score = pos - neg

    if pos > 0.5:   label = "BULLISH"
    elif neg > 0.5: label = "BEARISH"
    elif neu > 0.5: label = "NEUTRAL"
    else:           label = "MIXED"

    max_score = max(pos, neg, neu)

    # EXTREME сначала — иначе HIGH перехватывает
    if max_score > 0.85 and n >= 8:
        confidence = "EXTREME"
    elif max_score > 0.70 and n >= 5:
        confidence = "HIGH"
    elif max_score > 0.50 and n >= 3:
        confidence = "MEDIUM"
    else:
        confidence = "LOW"

    return round(score, 3), label, confidence


BULL_WORDS = [
    "рост", "растёт", "вырос", "повысился", "прибыль", "прорыв", "максимум",
    "одобрен", "одобрила", "купил", "покупка", "инвестиции", "партнёрство",
    "bullish", "surge", "rally", "high", "growth", "profit", "approved",
    "buy", "long", "upgrade", "beat", "exceeded", "record", "adoption",
    "халвинг", "etf", "институциональный", "снизил ставку", "смягчение",
]

BEAR_WORDS = [
    "падение", "упал", "снизился", "убыток", "банкротство", "запрет",
    "санкции", "арест", "взлом", "кризис", "инфляция", "рецессия",
    "bearish", "crash", "dump", "ban", "hack", "fraud", "loss", "sell",
    "short", "downgrade", "missed", "warning", "fear", "panic", "liquidation",
    "повысил ставку", "ужесточение", "регуляция", "обвал", "коллапс",
    "война", "эскалация", "геополитика",
]

NEUTRAL_WORDS = [
    "ожидает", "возможно", "вероятно", "неопределённость", "смешанный",
    "может", "если", "perhaps", "uncertain", "mixed", "wait", "hold",
]


def _keyword_score(text: str) -> tuple[float, str, str, int, int]:
    text_lower = text.lower()
    bull = sum(1 for w in BULL_WORDS if w in text_lower)
    bear = sum(1 for w in BEAR_WORDS if w in text_lower)
    neu  = sum(1 for w in NEUTRAL_WORDS if w in text_lower)
    total = bull + bear

    score = (bull - bear) / total if total > 0 else 0.0

    if score > 0.3:    label = "BULLISH"
    elif score < -0.3: label = "BEARISH"
    elif neu > 3:      label = "NEUTRAL"
    else:              label = "MIXED"

    imbalance = abs(bull - bear)
    if imbalance >= 5 and total >= 8:   confidence = "HIGH"
    elif imbalance >= 3 and total >= 4: confidence = "MEDIUM"
    else:                               confidence = "LOW"

    return round(score, 3), label, confidence, bull, bear


async def analyze_and_filter_async(
    news_text: str, market_data: str = ""
) -> tuple[SentimentResult, str]:
    combined  = news_text + (" " + market_data if market_data else "")
    headlines = _extract_headlines(combined)

    logger.info(f"FinBERT: извлечено {len(headlines)} заголовков")

    finbert_results = None
    if HF_TOKEN and headlines:
        finbert_results = await _finbert_score(headlines)

    if finbert_results:
        score, label, confidence = _aggregate_finbert(finbert_results)

        bull_signals = sum(1 for r in finbert_results if r.get("positive", 0) > 0.5)
        bear_signals = sum(1 for r in finbert_results if r.get("negative", 0) > 0.5)

        avg_pos = sum(r.get("positive", 0) for r in finbert_results) / len(finbert_results)
        avg_neg = sum(r.get("negative", 0) for r in finbert_results) / len(finbert_results)
        avg_neu = sum(r.get("neutral",  0) for r in finbert_results) / len(finbert_results)

        bar_bull = "█" * min(bull_signals, 10)
        bar_bear = "█" * min(bear_signals, 10)

        summary = (
            f"📊 FINBERT SENTIMENT: {score:+.3f} → {label}\n"
            f"Уверенность сигнала: {confidence} (FinBERT)\n"
            f"🐂 Бычьих заголовков: {bull_signals}/{len(finbert_results)} {bar_bull}\n"
            f"🐻 Медвежьих заголовков: {bear_signals}/{len(finbert_results)} {bar_bear}\n"
            f"📈 Avg positive: {avg_pos:.2f} | negative: {avg_neg:.2f} | neutral: {avg_neu:.2f}\n"
            f"🔬 Метод: FinBERT (ProsusAI) — обучен на финансовых текстах\n"
        )

        if confidence == "EXTREME":
            summary += f"🚨 ЭКСТРЕМАЛЬНЫЙ СИГНАЛ — рынок однозначно {label}.\n"
        elif confidence == "HIGH":
            summary += f"✅ Сигнал чёткий — учитывать {label} направление.\n"
        elif confidence == "MEDIUM":
            summary += "⚠️ Сигнал умеренный — анализировать внимательно.\n"
        else:
            summary += "❌ Сигнал слабый — высокая неопределённость.\n"

        result = SentimentResult(
            score=score, label=label, confidence=confidence,
            bull_signals=bull_signals, bear_signals=bear_signals,
            summary=summary, source="finbert",
        )

    else:
        score, label, confidence, bull, bear = _keyword_score(combined)

        bar_bull = "█" * min(bull, 10)
        bar_bear = "█" * min(bear, 10)

        summary = (
            f"📊 SENTIMENT SCORE: {score:+.2f} → {label}\n"
            f"Уверенность сигнала: {confidence} (keywords)\n"
            f"🐂 Бычьих сигналов: {bull} {bar_bull}\n"
            f"🐻 Медвежьих сигналов: {bear} {bar_bear}\n"
            f"⚠️ FinBERT недоступен — использован keyword-метод.\n"
        )

        if confidence == "HIGH":
            summary += f"✅ Сигнал чёткий — учитывать {label} направление.\n"
        elif confidence == "MEDIUM":
            summary += "⚠️ Сигнал умеренный — анализировать внимательно.\n"
        else:
            summary += "❌ Сигнал слабый — высокая неопределённость.\n"

        result = SentimentResult(
            score=score, label=label, confidence=confidence,
            bull_signals=bull, bear_signals=bear,
            summary=summary, source="keywords",
        )

    instruction = get_confidence_instruction(result.confidence)

    logger.info(
        f"Sentiment [{result.source}]: {result.label} ({result.score:+.3f}) | "
        f"Confidence: {result.confidence} | "
        f"Bull: {result.bull_signals} Bear: {result.bear_signals}"
    )

    return result, instruction


def analyze_and_filter(
    news_text: str, market_data: str = ""
) -> tuple[SentimentResult, str]:
    """Sync обёртка для обратной совместимости."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool:
                future = pool.submit(
                    asyncio.run,
                    analyze_and_filter_async(news_text, market_data)
                )
                return future.result(timeout=60)
        else:
            return loop.run_until_complete(
                analyze_and_filter_async(news_text, market_data)
            )
    except Exception as e:
        logger.error(f"analyze_and_filter error: {e}, falling back to keywords")
        score, label, confidence, bull, bear = _keyword_score(
            news_text + " " + market_data
        )
        result = SentimentResult(
            score=score, label=label, confidence=confidence,
            bull_signals=bull, bear_signals=bear,
            summary=f"📊 {label} ({score:+.2f}) — keyword fallback\n",
            source="keywords_emergency",
        )
        return result, get_confidence_instruction(confidence)


def format_for_agents(result: SentimentResult, instruction: str) -> str:
    return f"\n\n{result.summary}\n{instruction}\n"


CONFIDENCE_INSTRUCTIONS = {
    "EXTREME": """
🚨 РЕЖИМ: ЭКСТРЕМАЛЬНЫЙ СИГНАЛ
FinBERT зафиксировал однозначное направление с экстремальной уверенностью.
Это редкий сигнал — давай конкретные торговые рекомендации с чёткими входами и стопами.
Всё равно честно — проверяй данные и цитируй источники.
""",
    "HIGH": """
🟢 РЕЖИМ: СИЛЬНЫЙ СИГНАЛ
Sentiment score показывает чёткое направление с высокой уверенностью.
Можешь давать конкретные торговые рекомендации с точками входа и стопами.
Но всё равно честно — если агенты расходятся, скажи об этом.
""",
    "MEDIUM": """
🟡 РЕЖИМ: УМЕРЕННЫЙ СИГНАЛ
Sentiment неоднозначный. Можешь давать рекомендации но только с пометкой
"умеренный сигнал — маленькая позиция или жди подтверждения".
Акцент на сценариях и рисках, не на конкретных точках входа.
""",
    "LOW": """
🔴 РЕЖИМ: СЛАБЫЙ СИГНАЛ — НЕ ДАВАЙ ТОРГОВЫЕ РЕКОМЕНДАЦИИ
Данные противоречивы или их недостаточно для уверенного прогноза.
ЗАПРЕЩЕНО давать торговые рекомендации (LONG/SHORT/вход/стоп).
Вместо этого:
- Дай качественный анализ ситуации
- Опиши что нужно отслеживать
- Честно скажи: "Сигнал слабый — лучше подождать ясности"
""",
}


def get_confidence_instruction(confidence: str) -> str:
    return CONFIDENCE_INSTRUCTIONS.get(confidence, CONFIDENCE_INSTRUCTIONS["LOW"])
