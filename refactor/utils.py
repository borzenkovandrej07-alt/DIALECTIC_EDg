"""
Utility functions for working with refactored models
Практические помощники для работы с dataclasses
"""

from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
import json

from .models import (
    AnalysisContext, DebateRound, FinalReport, DebateHistory,
    MarketData, NewsContext, SentimentAnalysis, UserProfile,
    Prediction, DebateSession, AgentMessage
)


# ─── Context Building ──────────────────────────────────────────────────────

def build_analysis_context(
    market: str,
    price: float,
    change_24h: float,
    headlines: List[str],
    sentiments: List[float],
    user_profile: Optional[UserProfile] = None,
) -> AnalysisContext:
    """Construct AnalysisContext from raw data"""
    
    market_data = MarketData(
        symbol=market,
        price=price,
        change_24h=change_24h,
    )
    
    news_ctx = NewsContext(
        headlines=headlines,
        summaries=[],  # Will be populated separately
        sources=[],    # Will be populated separately
        sentiment_scores=sentiments,
        average_sentiment=sum(sentiments) / len(sentiments) if sentiments else 0.0,
    )
    
    sentiment = SentimentAnalysis(
        overall_score=news_ctx.average_sentiment,
        bull_indicators=[h for h, s in zip(headlines, sentiments) if s > 0.3],
        bear_indicators=[h for h, s in zip(headlines, sentiments) if s < -0.3],
        neutral_factors=[h for h, s in zip(headlines, sentiments) if -0.3 <= s <= 0.3],
        finbert_confidence=0.75,  # Default confidence
    )
    
    return AnalysisContext(
        market=market,
        market_data=market_data,
        news_context=news_ctx,
        sentiment=sentiment,
        user_profile=user_profile,
    )


# ─── Report Building ──────────────────────────────────────────────────────

def build_final_report(
    market: str,
    market_data: MarketData,
    debate_rounds: List[DebateRound],
    synthesis: str,
    recommendation: str,
    confidence: float,
    reasoning: str,
    models_used: Dict[str, str],
    debate_duration: float = 0.0,
) -> FinalReport:
    """Construct FinalReport from debate components"""
    
    total_tokens = sum(
        len(round.bull_argument.split()) + len(round.bear_argument.split())
        for round in debate_rounds
    )
    
    return FinalReport(
        market=market,
        market_data=market_data,
        all_rounds=debate_rounds,
        final_synthesis=synthesis,
        recommendation=recommendation,
        confidence=min(1.0, max(0.0, confidence)),  # Clamp to [0, 1]
        reasoning=reasoning,
        created_at=datetime.now(),
        debate_duration_seconds=debate_duration,
        token_count=total_tokens,
        models_used=models_used,
    )


def report_to_telegram_format(report: FinalReport, max_length: int = 4096) -> str:
    """Convert FinalReport to Telegram-safe HTML format"""
    
    parts = [
        f"📊 <b>{report.market}</b>",
        f"💰 Цена: ${report.market_data.price:,.2f}",
        f"📈 24h: {report.market_data.change_24h:+.2f}%",
        "",
        f"🎯 <b>Рекомендация: {report.recommendation}</b>",
        f"📊 Уверенность: {report.confidence:.0%}",
        "",
        f"💭 {report.reasoning}",
        "",
        f"✨ {report.final_synthesis}",
    ]
    
    text = "\n".join(parts)
    
    # Truncate if needed
    if len(text) > max_length:
        text = text[:max_length-10] + "\n...(сокращено)"
    
    return text


# ─── Debate History Utilities ──────────────────────────────────────────────

def enrich_debate_history_with_context(
    history: DebateHistory,
    user_profile: Optional[UserProfile] = None,
) -> str:
    """
    Create rich context string from debate history with user profile
    Полезно для передачи в верификатор/синтезератор
    """
    
    context_lines = [
        "📋 История дебатов:",
        history.context_for_agent(max_chars=3000),
    ]
    
    if user_profile:
        context_lines.append("\n" + user_profile.build_system_prompt_suffix())
    
    return "\n".join(context_lines)


def extract_round_arguments(
    history: DebateHistory,
    round_num: int,
) -> Dict[str, str]:
    """Extract specific round arguments"""
    
    messages_in_round = history.messages_by_round(round_num)
    
    result = {}
    for msg in messages_in_round:
        if msg.agent in ["Bull", "Bear", "Verifier", "Synth"]:
            result[msg.agent.lower()] = msg.content
    
    return result


# ─── Prediction Utilities ──────────────────────────────────────────────────

def create_prediction_from_report(
    report: FinalReport,
    report_id: str,
) -> Prediction:
    """Create Prediction from FinalReport"""
    
    return Prediction(
        report_id=report_id,
        symbol=report.market,
        recommendation=report.recommendation,
        entry_price=report.market_data.price,
        timestamp=report.created_at,
        metadata={
            "confidence": report.confidence,
            "reasoning": report.reasoning,
            "models": report.models_used,
        }
    )


def resolve_prediction(
    prediction: Prediction,
    current_price: float,
    threshold_percent: float = 2.0,
) -> None:
    """
    Resolve prediction against actual price movement
    Updates prediction in-place
    """
    
    if prediction.resolved:
        return  # Already resolved
    
    price_change_percent = ((current_price - prediction.entry_price) / prediction.entry_price) * 100
    
    # Determine direction
    if price_change_percent > threshold_percent:
        actual_direction = "UP"
    elif price_change_percent < -threshold_percent:
        actual_direction = "DOWN"
    else:
        actual_direction = "FLAT"
    
    # Check if correct
    is_correct = (
        (prediction.recommendation == "BULLISH" and actual_direction == "UP") or
        (prediction.recommendation == "BEARISH" and actual_direction == "DOWN") or
        (prediction.recommendation == "NEUTRAL" and actual_direction == "FLAT")
    )
    
    # Update prediction
    prediction.resolved = True
    prediction.resolved_at = datetime.now()
    prediction.actual_direction = actual_direction
    prediction.profit_loss = price_change_percent
    prediction.metadata["correct"] = is_correct


def calculate_prediction_accuracy(predictions: List[Prediction]) -> float:
    """Calculate accuracy from list of resolved predictions"""
    
    resolved = [p for p in predictions if p.resolved]
    if not resolved:
        return 0.0
    
    correct = sum(
        1 for p in resolved
        if p.metadata.get("correct", False)
    )
    
    return correct / len(resolved)


# ─── Session Utilities ─────────────────────────────────────────────────────

def create_debate_session(
    user_id: int,
    market: str,
    report: FinalReport,
    history: DebateHistory,
    session_id: Optional[str] = None,
    ttl_hours: int = 24,
) -> DebateSession:
    """Create DebateSession for storing debate state"""
    
    if session_id is None:
        from datetime import datetime
        session_id = f"debate_{user_id}_{market}_{int(datetime.now().timestamp())}"
    
    return DebateSession(
        session_id=session_id,
        user_id=user_id,
        market=market,
        report=report,
        debate_history=history,
        created_at=datetime.now(),
        expires_at=datetime.now() + timedelta(hours=ttl_hours),
        metadata={
            "ttl_hours": ttl_hours,
            "marker_count": len(history.messages),
        }
    )


# ─── Serialization Utilities ───────────────────────────────────────────────

def report_to_json(report: FinalReport) -> str:
    """Serialize FinalReport to JSON"""
    return json.dumps(report.to_dict(), ensure_ascii=False, indent=2)


def debate_history_to_json(history: DebateHistory) -> str:
    """Serialize DebateHistory to JSON"""
    
    messages = [
        {
            "agent": m.agent,
            "round": m.round_num,
            "content": m.content[:500],  # Truncate for readability
            "timestamp": m.timestamp.isoformat(),
            "model": m.model_used,
        }
        for m in history.messages
    ]
    
    return json.dumps(messages, ensure_ascii=False, indent=2)


# ─── Validation Utilities ─────────────────────────────────────────────────

def validate_report(report: FinalReport) -> tuple[bool, List[str]]:
    """Validate report completeness"""
    
    errors = []
    
    if not report.market:
        errors.append("Market symbol is required")
    
    if not report.all_rounds or len(report.all_rounds) == 0:
        errors.append("At least one debate round is required")
    
    if not report.final_synthesis or len(report.final_synthesis) < 50:
        errors.append("Final synthesis is too short")
    
    if report.recommendation not in ["BULLISH", "BEARISH", "NEUTRAL"]:
        errors.append(f"Invalid recommendation: {report.recommendation}")
    
    if not (0 <= report.confidence <= 1):
        errors.append("Confidence must be between 0 and 1")
    
    return len(errors) == 0, errors


def validate_context(ctx: AnalysisContext) -> tuple[bool, List[str]]:
    """Validate AnalysisContext completeness"""
    
    errors = []
    
    if not ctx.market:
        errors.append("Market symbol is required")
    
    if ctx.market_data.price <= 0:
        errors.append("Price must be positive")
    
    if not ctx.news_context.headlines:
        errors.append("At least one headline is required")
    
    return len(errors) == 0, errors
