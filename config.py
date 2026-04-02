"""
config.py — Центральная конфигурация Dialectic Edge.

ИСПРАВЛЕНО v2:
- FRED_API_KEY убран из кода в переменные окружения (был захардкожен и виден на GitHub)
- Добавлен DB_PATH — единый путь к БД для всех модулей
  (раньше learning.py использовал "dialectic.db" вместо "dialectic_edge.db")
"""

import os
from pathlib import Path

# ─── ПЕРСИСТЕНТНОЕ ХРАНИЛИЩЕ (Railway volume / любой VPS) ─────────────────────
# Без тома файлы живут в эфемерной ФС контейнера — после деплоя cache.json и БД пустые.
# Railway: том НЕ в Settings → Variables. Создание: Ctrl+K (⌘K на Mac) → набери "Volume"
#   ИЛИ правый клик по пустому месту на схеме проекта → Volume → привяжи к сервису бота,
#   укажи mount path (например /data). Тогда Railway сам выставит RAILWAY_VOLUME_MOUNT_PATH —
#   бот подхватит его без ручного DATA_DIR. Вручную: Variables → DATA_DIR=/data
# В один каталог кладём SQLite и cache.json (дебаты + last_report + user_debates).
_DATA_DIR = (
    os.getenv("DATA_DIR", "").strip()
    or os.getenv("RAILWAY_VOLUME_MOUNT_PATH", "").strip()
)
if _DATA_DIR:
    _data_root = Path(_DATA_DIR)
    _data_root.mkdir(parents=True, exist_ok=True)
    DB_PATH = str(_data_root / "dialectic_edge.db")
    CACHE_FILE = str(_data_root / "cache.json")
    USING_DATA_DIR = True
else:
    DB_PATH = os.getenv("DB_PATH", "dialectic_edge.db")
    CACHE_FILE = os.getenv("CACHE_FILE", "cache.json")
    USING_DATA_DIR = False

# ─── TELEGRAM ─────────────────────────────────────────────────────────────────
BOT_TOKEN = os.getenv("BOT_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN_HERE")

# ID администраторов
ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "0").split(",") if x.strip().isdigit()]

# Redis: снимки дебатов для кнопки «листать» — переживают рестарт и несколько воркеров.
# Railway: в проекте + New → Database → Redis. В СЕРВИСЕ БОТА: Variables → New Variable →
# Reference → Redis → REDIS_URL. URL в чат не шли — только в панели Railway.
REDIS_URL = os.getenv("REDIS_URL", "")

# ─── FRED API ─────────────────────────────────────────────────────────────────
# ИСПРАВЛЕНО: ключ перенесён в переменные окружения Railway
# Получить бесплатный ключ: https://fred.stlouisfed.org/docs/api/api_key.html
# В Railway: Settings → Variables → FRED_API_KEY=твой_ключ
FRED_API_KEY = os.getenv("FRED_API_KEY", "")

# ─── AI ПРОВАЙДЕР ─────────────────────────────────────────────────────────────
AI_PROVIDER = os.getenv("AI_PROVIDER", "gemini")

OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_MODEL    = os.getenv("OLLAMA_MODEL", "llama3.2")

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL   = os.getenv("GEMINI_MODEL", "gemini-1.5-flash")

OPENAI_COMPAT_BASE_URL = os.getenv("OPENAI_COMPAT_BASE_URL", "http://localhost:1234/v1")
OPENAI_COMPAT_API_KEY  = os.getenv("OPENAI_COMPAT_API_KEY", "lm-studio")
OPENAI_COMPAT_MODEL    = os.getenv("OPENAI_COMPAT_MODEL", "local-model")

# ─── ДЕБАТЫ ───────────────────────────────────────────────────────────────────
DEBATE_ROUNDS        = int(os.getenv("DEBATE_ROUNDS", "3"))
MAX_TOKENS_PER_AGENT = int(os.getenv("MAX_TOKENS", "1500"))
AGENT_TEMPERATURE    = float(os.getenv("AGENT_TEMP", "0.7"))
# Первый провайдер для Bull/Bear/Verifier/Synth (ai_provider.py):
#   AI_DEBATE_PRIMARY=mistral|groq|openrouter|together|gemini
# Модели: GROQ_MODEL, OPENROUTER_MODEL, TOGETHER_MODEL, MISTRAL_MODEL, MISTRAL_SYNTH_MODEL, GEMINI_MODEL
# Росстат на Railway: при SSLCertVerificationError — RUSSIA_ROSSTAT_INSECURE_SSL=1 (только этот URL, на свой риск)

# ─── НОВОСТИ ──────────────────────────────────────────────────────────────────
NEWS_API_KEY = os.getenv("NEWS_API_KEY", "")

# RSS: Reuters (feeds.reuters.com) часто недоступен из облаков — добавлены BBC, Guardian, MarketWatch и др.
RSS_FEEDS = {
    "BBC Business":       "https://feeds.bbci.co.uk/news/business/rss.xml",
    "Guardian Business":  "https://www.theguardian.com/business/rss",
    "Guardian World":     "https://www.theguardian.com/world/rss",
    "MarketWatch":        "https://feeds.marketwatch.com/marketwatch/topstories/",
    "CNBC Markets":       "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=15839135",
    "Yahoo Finance":      "https://finance.yahoo.com/news/rssindex",
    "Investing.com Eco":  "https://www.investing.com/rss/news_14.rss",
    "FT Markets":         "https://www.ft.com/rss/home/uk",
    "CoinDesk":           "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "Cointelegraph":      "https://cointelegraph.com/rss",
}

MAX_NEWS_PER_FEED = int(os.getenv("MAX_NEWS_PER_FEED", "3"))
MAX_TOTAL_NEWS    = int(os.getenv("MAX_TOTAL_NEWS", "20"))

# ─── ХРАНИЛИЩЕ (CACHE_FILE / DB_PATH заданы выше; при DATA_DIR — внутри тома) ─
# Повторный /daily отдаёт тот же отчёт без вызова AI, пока не истечёт TTL (экономия токенов).
# Раньше по умолчанию было 2 ч.; сутки — разумный баланс. Переопределение: CACHE_TTL_HOURS=6
CACHE_TTL_HOURS = int(os.getenv("CACHE_TTL_HOURS", "24"))
# Снимок дебатов для кнопки «листать раунды» (JSON + SQLite; переживает другой воркер при общем диске)
DEBATE_SNAPSHOT_HOURS = int(os.getenv("DEBATE_SNAPSHOT_HOURS", "72"))

# ─── ФОРМАТИРОВАНИЕ ───────────────────────────────────────────────────────────
DISCLAIMER = (
    "\n\n─────────────────────────\n"
    "🤝 *Честно о боте:*\n"
    "Это AI-анализ на основе публичных данных — не предсказание будущего.\n"
    "Рынок непредсказуем. Агенты могут ошибаться и иногда ошибаются.\n"
    "Где данных не хватало — агенты должны были это указать явно.\n"
    "Используй как один из инструментов мышления, не как сигнал к действию.\n\n"
    "⚠️ *Не является финансовым советом. DYOR. Торговля = риск потери капитала.*"
)

# ─── ФИЧЕФЛАГИ И ЛИМИТЫ (одно место; переопределение через env) ─────────────
# Глобально выключить цикл бумажного автотрейда (даже если backtest enabled в БД)
FEATURE_AUTOTRADE = os.getenv("FEATURE_AUTOTRADE", "1").strip().lower() in ("1", "true", "yes", "on")
# Источники данных для bias крипты в автотрейде
DATA_SOURCE_BINANCE_SIGNALS = os.getenv("DATA_SOURCE_BINANCE_SIGNALS", "1").strip().lower() in ("1", "true", "yes", "on")
# Писать в trade_decision_log каждый пропуск цикла (шумно; для отладки)
LOG_AUTOTRADE_SKIPS = os.getenv("LOG_AUTOTRADE_SKIPS", "0").strip().lower() in ("1", "true", "yes", "on")
# Параметры автотрейда (раньше были константами в signal_trader)
AUTOTRADE_INTERVAL_SEC = int(os.getenv("AUTOTRADE_INTERVAL_SEC", "300"))
AUTOTRADE_RECENT_CONTEXT_LIMIT = int(os.getenv("AUTOTRADE_RECENT_CONTEXT_LIMIT", "3"))
AUTOTRADE_CONTEXT_MAX_AGE_HOURS = int(os.getenv("AUTOTRADE_CONTEXT_MAX_AGE_HOURS", "72"))
AUTOTRADE_ENTRY_TOLERANCE_PCT = float(os.getenv("AUTOTRADE_ENTRY_TOLERANCE_PCT", "0.02"))
AUTOTRADE_OPEN_SCORE_THRESHOLD = float(os.getenv("AUTOTRADE_OPEN_SCORE_THRESHOLD", "18"))
AUTOTRADE_REVERSAL_SCORE_THRESHOLD = float(os.getenv("AUTOTRADE_REVERSAL_SCORE_THRESHOLD", "16"))
AUTOTRADE_SIGNAL_BIAS_CACHE_SEC = int(os.getenv("AUTOTRADE_SIGNAL_BIAS_CACHE_SEC", "300"))
# При NEUTRAL (или нет кандидатов из дайджеста) — добавлять кандидатов из /signals bias + рынок
AUTOTRADE_FOLLOW_SIGNALS_WHEN_NEUTRAL = os.getenv("AUTOTRADE_FOLLOW_SIGNALS_WHEN_NEUTRAL", "1").strip().lower() in (
    "1", "true", "yes", "on",
)
AUTOTRADE_NEUTRAL_MIN_BIAS_SCORE = float(os.getenv("AUTOTRADE_NEUTRAL_MIN_BIAS_SCORE", "8"))
AUTOTRADE_NEUTRAL_TP_PCT = float(os.getenv("AUTOTRADE_NEUTRAL_TP_PCT", "0.04"))
AUTOTRADE_NEUTRAL_SL_PCT = float(os.getenv("AUTOTRADE_NEUTRAL_SL_PCT", "0.02"))
# Ограничить размер JSON-снимка контекста модели в БД (символов)
DIGEST_SNAPSHOT_MAX_CHARS = int(os.getenv("DIGEST_SNAPSHOT_MAX_CHARS", "12000"))
