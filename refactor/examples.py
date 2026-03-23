"""
Usage examples for refactored models
Примеры как использовать новые dataclasses в коде
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from datetime import datetime
from refactor import (
    AnalysisContext, DebateRound, FinalReport, DebateHistory,
    MarketData, NewsContext, SentimentAnalysis,
    UserProfile, RiskProfile, TimeHorizon, Market, Prediction
)
from refactor.utils import (
    build_analysis_context,
    build_final_report,
    create_prediction_from_report,
    resolve_prediction,
    calculate_prediction_accuracy,
    validate_report,
)


# ───────────────────────────────────────────────────────────────────────────


def example_1_build_context():
    """Example: Building AnalysisContext from market data"""
    
    print("📖 Example 1: Building AnalysisContext")
    print("-" * 50)
    
    # Create context with helper
    context = build_analysis_context(
        market="BTC",
        price=45000.0,
        change_24h=2.5,
        headlines=[
            "Bitcoin rallies above $45K on institutional buying",
            "Fed signals pause in rate hikes",
            "Crypto market cap reaches $2 trillion",
        ],
        sentiments=[0.8, 0.6, 0.7],  # Positive sentiments
    )
    
    print(f"✓ Created context for {context.market}")
    print(f"  Price: ${context.market_data.price:,.0f}")
    print(f"  Sentiment: {context.sentiment.overall_score:+.2f}")
    print(f"  Bull indicators: {len(context.sentiment.bull_indicators)}")
    print()


def example_2_create_user_profile():
    """Example: Creating personalized user profile"""
    
    print("📖 Example 2: User Profile with Personalization")
    print("-" * 50)
    
    profile = UserProfile(
        user_id=12345,
        risk_profile=RiskProfile.MODERATE,
        time_horizon=TimeHorizon.MEDIUM_TERM,
        preferred_markets=[Market.STOCKS, Market.CRYPTO],
        language="ru",
        custom_instructions="Focus on blue-chip stocks and Layer-1 crypto",
    )
    
    print(f"✓ Created profile for user {profile.user_id}")
    print(f"  Risk: {profile.risk_profile.value}")
    print(f"  Horizon: {profile.time_horizon.value}")
    print(f"  Markets: {[m.value for m in profile.preferred_markets]}")
    print()


def example_3_debate_history():
    """Example: Building debate history"""
    
    print("📖 Example 3: Debate History")
    print("-" * 50)
    
    history = DebateHistory()
    
    # Round 1
    history.add("Bull", "Bitcoin shows strong bullish momentum with break above key resistance", 1, "llama-3.3")
    history.add("Bear", "Overbought signals on RSI, correction likely coming", 1, "llama-3.3")
    
    # Round 2
    history.add("Bull", "Institution inflows confirm sustainability of rally", 2, "together")
    history.add("Bear", "Macro headwinds limit upside, $40K could be tested", 2, "together")
    
    print(f"✓ Created history with {len(history.messages)} messages")
    print(f"  Rounds: {max(m.round_num for m in history.messages)}")
    print(f"  Last Bull: {history.last_message_by('Bull')[:60]}...")
    print()


def example_4_create_report():
    """Example: Creating FinalReport from components"""
    
    print("📖 Example 4: FinalReport Creation")
    print("-" * 50)
    
    market_data = MarketData(symbol="BTC", price=45000.0, change_24h=2.5)
    
    rounds = [
        DebateRound(
            round_num=1,
            bull_argument="Strong tech momentum",
            bear_argument="Risky entry point",
            verifier_analysis="Mixed signals",
            synth_synthesis="Wait for confirmation",
        )
    ]
    
    report = build_final_report(
        market="BTC",
        market_data=market_data,
        debate_rounds=rounds,
        synthesis="Bitcoin shows bullish setup but with elevated risk",
        recommendation="BULLISH",
        confidence=0.75,
        reasoning="Strong institutional buying despite macro headwinds",
        models_used={"bull": "Llama-3.3", "bear": "Llama-3.3"},
        debate_duration=5.2,
    )
    
    print(f"✓ Created report for {report.market}")
    print(f"  Recommendation: {report.recommendation}")
    print(f"  Confidence: {report.confidence:.0%}")
    print(f"  Duration: {report.debate_duration_seconds:.1f}s")
    
    # Validate
    is_valid, errors = validate_report(report)
    print(f"  Valid: {'✓' if is_valid else '✗'}")
    if errors:
        for err in errors:
            print(f"    - {err}")
    print()


def example_5_prediction_tracking():
    """Example: Prediction creation and resolution"""
    
    print("📖 Example 5: Prediction Tracking")
    print("-" * 50)
    
    # Create report
    market_data = MarketData(symbol="BTC", price=45000.0, change_24h=2.5)
    report = FinalReport(
        market="BTC",
        market_data=market_data,
        all_rounds=[],
        final_synthesis="Bullish",
        recommendation="BULLISH",
        confidence=0.80,
        reasoning="Strong momentum",
    )
    
    # Create prediction
    pred = create_prediction_from_report(report, "report_001")
    print(f"✓ Created prediction: {pred.symbol} {pred.recommendation}")
    print(f"  Entry: ${pred.entry_price:,.0f}")
    print(f"  Resolved: {pred.resolved}")
    
    # Resolve after price movement
    resolve_prediction(pred, current_price=46500.0, threshold_percent=2.0)
    print(f"  After movement:")
    print(f"    - Direction: {pred.actual_direction}")
    print(f"    - P&L: {pred.profit_loss:+.2f}%")
    print(f"    - Correct: {pred.metadata.get('correct')}")
    print()


def example_6_portfolio_analytics():
    """Example: Portfolio prediction accuracy"""
    
    print("📖 Example 6: Portfolio Analytics")
    print("-" * 50)
    
    predictions = []
    
    # Create sample predictions
    for i, (recommend, actual_price, entry_price) in enumerate([
        ("BULLISH", 46500, 45000),   # Correct
        ("BEARISH", 42000, 44000),   # Correct
        ("BULLISH", 43500, 45000),   # Incorrect
    ]):
        pred = Prediction(
            report_id=f"rep_{i}",
            symbol="BTC",
            recommendation=recommend,
            entry_price=entry_price,
            resolved=True,
            actual_direction="UP" if actual_price > entry_price else "DOWN",
            profit_loss=((actual_price - entry_price) / entry_price) * 100,
        )
        resolve_prediction(pred, actual_price)
        predictions.append(pred)
    
    accuracy = calculate_prediction_accuracy(predictions)
    print(f"✓ Portfolio stats:")
    print(f"  Total predictions: {len(predictions)}")
    print(f"  Resolved: {sum(1 for p in predictions if p.resolved)}")
    print(f"  Accuracy: {accuracy:.0%}")
    print()


def example_7_report_formatting():
    """Example: Telegram formatting"""
    
    print("📖 Example 7: Report Formatting")
    print("-" * 50)
    
    from refactor.utils import report_to_telegram_format
    
    market_data = MarketData(symbol="BTC", price=45000.0, change_24h=2.5)
    report = FinalReport(
        market="BTC",
        market_data=market_data,
        all_rounds=[],
        final_synthesis="Strong bullish momentum with institutional buying",
        recommendation="BULLISH",
        confidence=0.85,
        reasoning="Tech setup + macro tailwinds + overnight strength",
    )
    
    telegram_text = report_to_telegram_format(report)
    print("✓ Telegram format generated:")
    print(telegram_text)
    print()


# ───────────────────────────────────────────────────────────────────────────


if __name__ == "__main__":
    print("\n" + "=" * 50)
    print("   📚 Refactored Models Usage Examples")
    print("=" * 50 + "\n")
    
    example_1_build_context()
    example_2_create_user_profile()
    example_3_debate_history()
    example_4_create_report()
    example_5_prediction_tracking()
    example_6_portfolio_analytics()
    example_7_report_formatting()
    
    print("=" * 50)
    print("   ✨ All examples completed!")
    print("=" * 50 + "\n")
