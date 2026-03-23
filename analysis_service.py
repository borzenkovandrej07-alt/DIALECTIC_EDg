"""
Analysis orchestration service used by handlers and Telegram commands.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Dict, Tuple

from agents import DebateOrchestrator
from data_sources import fetch_full_context
from database import log_report
from github_export import get_previous_digest, push_digest_cache
from meta_analyst import get_meta_context
from news_fetcher import NewsFetcher
from report_sanitizer import sanitize_full_report
from sentiment import analyze_and_filter_async, format_for_agents
from storage import Storage
from tracker import save_predictions_from_report
from user_profile import build_profile_instruction, get_profile
from web_search import get_full_realtime_context, search_news_context

logger = logging.getLogger(__name__)

_fetcher = NewsFetcher()
_storage = Storage()


async def run_full_analysis(
    user_id: int,
    custom_news: str = "",
    custom_mode: bool = False,
) -> Tuple[str, Dict]:
    """
    Run the current production analysis pipeline and return full report + prices.
    """
    tasks = [
        _fetcher.fetch_all(),
        fetch_full_context(),
        get_full_realtime_context(),
        get_profile(user_id),
        get_meta_context(),
        get_previous_digest(),
    ]
    news, geo_context, realtime_result, profile, meta_context, prev_digest = await asyncio.gather(
        *tasks, return_exceptions=True
    )

    if isinstance(news, Exception):
        logger.warning("news fetch failed: %s", news)
        news = ""
    if isinstance(geo_context, Exception):
        logger.warning("geo context failed: %s", geo_context)
        geo_context = ""
    if isinstance(profile, Exception):
        logger.warning("profile load failed: %s", profile)
        profile = {"risk": "moderate", "horizon": "swing", "markets": "all", "capital": "unknown"}
    if isinstance(meta_context, Exception):
        logger.warning("meta context failed: %s", meta_context)
        meta_context = ""
    if isinstance(prev_digest, Exception):
        logger.warning("previous digest failed: %s", prev_digest)
        prev_digest = ""

    if isinstance(realtime_result, Exception):
        logger.warning("realtime context failed: %s", realtime_result)
        prices_dict, live_prices = {}, ""
    elif isinstance(realtime_result, tuple) and len(realtime_result) == 2:
        prices_dict, live_prices = realtime_result
    else:
        prices_dict, live_prices = {}, ""

    profile_instruction = build_profile_instruction(profile)
    if custom_mode and custom_news:
        web_context = await search_news_context(custom_news)
        news_context = (
            f"ТЕМА АНАЛИЗА: {custom_news}\n\n"
            f"{web_context}\n\n{geo_context}\n\n{meta_context}"
        )
    else:
        news_context = f"{geo_context}\n\n=== НОВОСТИ ===\n{news}\n\n{meta_context}"

    if prev_digest and not custom_mode:
        news_context += f"\n\n{prev_digest}"

    sentiment_result, confidence_instruction = await analyze_and_filter_async(
        news_context,
        str(live_prices),
    )
    sentiment_block = format_for_agents(sentiment_result, confidence_instruction)
    prices_dict = dict(prices_dict) if prices_dict else {}
    prices_dict["SENTIMENT"] = {
        "score": sentiment_result.score,
        "label": sentiment_result.label,
        "confidence": sentiment_result.confidence,
    }

    report = await DebateOrchestrator().run_debate(
        news_context=news_context,
        live_prices=live_prices,
        profile_instruction=profile_instruction + sentiment_block,
        custom_mode=custom_mode,
    )
    report, removed_lines = sanitize_full_report(report)
    if removed_lines:
        logger.info("sanitizer removed %s lines", removed_lines)

    conf_raw = sentiment_result.confidence
    conf_map = {"HIGH": 0.85, "MEDIUM": 0.55, "LOW": 0.25, "EXTREME": 0.95}
    if isinstance(conf_raw, str):
        conf_num = conf_map.get(conf_raw.upper(), 0.5)
    else:
        try:
            conf_num = float(conf_raw)
        except (TypeError, ValueError):
            conf_num = 0.5

    stars = max(1, min(5, round(conf_num * 5)))
    pct = int(conf_num * 100)
    separator = "─" * 30 + "\n"
    signal_line = (
        f"📶 *Уровень сигнала:* {'⭐' * stars}{'☆' * (5 - stars)} "
        f"({pct}% — уверенность FinBERT в тоне новостей)\n"
        f"_Это не гарантированное направление рынка._\n\n"
    )
    report = report.replace(separator, separator + signal_line, 1)

    source = custom_news[:300] if custom_mode else str(news)[:300]
    await save_predictions_from_report(report, source_news=source)
    await log_report(
        user_id,
        "analyze" if custom_mode else "daily",
        source,
        report[:500],
    )

    if not custom_mode:
        _storage.cache_report(report, prices_dict, owner_user_id=user_id)
        try:
            date_str = datetime.now().strftime("%d.%m.%Y %H:%M")
            asyncio.create_task(push_digest_cache(report, date_str))
        except Exception as exc:
            logger.warning("digest cache push failed: %s", exc)

    return report, prices_dict
