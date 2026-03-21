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
# Railway: New → Volume → Mount path, например /data → в Variables: DATA_DIR=/data
# В один каталог кладём и SQLite, и cache.json (дебаты + last_report + user_debates).
_DATA_DIR = os.getenv("DATA_DIR", "").strip()
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

# Redis (опционально): снимки дебатов для кнопки «листать» — переживают рестарт и несколько воркеров.
# Railway: Add-ons → Redis → REDIS_URL подтянется автоматически
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
