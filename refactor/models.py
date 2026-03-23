"""
Structured Models for Dialectic Edge v7.1
Унифицированные dataclasses для типизации и валидации данных.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, Any, List
from enum import Enum


# ─── Enums ─────────────────────────────────────────────────────────────────

class RiskProfile(str, Enum):
    """Профили риска для инвесторов"""
    CONSERVATIVE = "conservative"
    MODERATE = "moderate"
    AGGRESSIVE = "aggressive"


class TimeHorizon(str, Enum):
    """Инвестиционный горизонт"""
    SHORT_TERM = "short_term"  # < 1 года
    MEDIUM_TERM = "medium_term"  # 1-5 лет
    LONG_TERM = "long_term"  # > 5 лет


class Market(str, Enum):
    """Рынки для анализа"""
    STOCKS = "stocks"
    CRYPTO = "crypto"
    FOREX = "forex"
    COMMODITIES = "commodities"
    BONDS = "bonds"
    RUSSIA = "russia"


# ─── Agent Message & History ───────────────────────────────────────────────

@dataclass
class AgentMessage:
    """Сообщение от агента в дебатах"""
    agent: str  # "Bull", "Bear", "Verifier", "Synth"
    content: str
    round_num: int
    timestamp: datetime = field(default_factory=datetime.now)
    token_count: int = 0
    model_used: Optional[str] = None


@dataclass
class DebateHistory:
    """История одного раунда дебатов"""
    messages: List[AgentMessage] = field(default_factory=list)

    def add(self, agent: str, content: str, round_num: int, model_used: Optional[str] = None):
        """Добавить сообщение в историю"""
        msg = AgentMessage(agent, content, round_num, model_used=model_used)
        self.messages.append(msg)

    def context_for_agent(self, max_chars: int = 4000) -> str:
        """Получить контекст дебатов для агента"""
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
        """Получить последнее сообщение от агента"""
        for m in reversed(self.messages):
            if agent_name in m.agent:
                return m.content
        return ""

    def messages_by_round(self, round_num: int) -> List[AgentMessage]:
        """Получить все сообщения раунда"""
        return [m for m in self.messages if m.round_num == round_num]


# ─── Analysis Context ──────────────────────────────────────────────────────

@dataclass
class MarketData:
    """Рыночные данные для анализа"""
    symbol: str
    price: float
    change_24h: float  # %
    change_7d: Optional[float] = None
    change_30d: Optional[float] = None
    volume_24h: Optional[float] = None
    market_cap: Optional[float] = None
    timestamp: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class NewsContext:
    """Контекст новостей для анализа"""
    headlines: List[str]
    summaries: List[str]
    sources: List[str]
    sentiment_scores: List[float]  # -1 (negative) to 1 (positive)
    average_sentiment: float = 0.0
    timestamp: datetime = field(default_factory=datetime.now)
    raw_articles: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class SentimentAnalysis:
    """Результаты анализа тональности"""
    overall_score: float  # -1 to 1
    bull_indicators: List[str]
    bear_indicators: List[str]
    neutral_factors: List[str]
    finbert_confidence: float  # 0 to 1
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class AnalysisContext:
    """Унифицированный контекст для дебатов"""
    market: str  # "BTC", "S&P500", "GAZP", etc
    market_data: MarketData
    news_context: NewsContext
    sentiment: SentimentAnalysis
    meta_analysis: Optional[Dict[str, Any]] = None
    russia_specific: Optional[Dict[str, Any]] = None
    user_profile: Optional['UserProfile'] = None
    created_at: datetime = field(default_factory=datetime.now)
    
    def to_agent_prompt(self) -> str:
        """Конвертировать контекст в промпт для агента"""
        parts = [
            f"📊 Актив: {self.market}",
            f"💰 Цена: {self.market_data.price}",
            f"📈 24h: {self.market_data.change_24h:+.2f}%",
        ]
        
        if self.news_context.headlines:
            parts.append(f"\n📰 Новости ({len(self.news_context.headlines)}):")
            for h in self.news_context.headlines[:3]:
                parts.append(f"  • {h}")
        
        if self.sentiment:
            parts.append(f"\n🎯 Тональность: {self.sentiment.overall_score:+.2f}")
        
        return "\n".join(parts)


# ─── Debate & Report ───────────────────────────────────────────────────────

@dataclass
class DebateRound:
    """Один раунд дебатов"""
    round_num: int
    bull_argument: str
    bear_argument: str
    verifier_analysis: Optional[str] = None
    synth_synthesis: Optional[str] = None
    history_snapshot: Optional[DebateHistory] = None
    created_at: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        """Сериализировать раунд"""
        return {
            "round": self.round_num,
            "bull": self.bull_argument,
            "bear": self.bear_argument,
            "verifier": self.verifier_analysis,
            "synth": self.synth_synthesis,
            "timestamp": self.created_at.isoformat(),
        }


@dataclass
class FinalReport:
    """Финальный отчёт анализа"""
    market: str
    market_data: MarketData
    all_rounds: List[DebateRound]
    final_synthesis: str
    recommendation: str  # "BULLISH", "BEARISH", "NEUTRAL"
    confidence: float  # 0 to 1
    reasoning: str
    
    # Метаданные
    created_at: datetime = field(default_factory=datetime.now)
    debate_duration_seconds: float = 0.0
    token_count: int = 0
    models_used: Dict[str, str] = field(default_factory=dict)  # {"bull": "Llama-3.3", ...}
    
    # Опциональные
    user_profile: Optional['UserProfile'] = None
    russia_specific: Optional[str] = None
    charts: Dict[str, bytes] = field(default_factory=dict)  # {"main": b"...", "russia": b"..."}
    
    def to_dict(self) -> Dict[str, Any]:
        """Сериализировать в JSON"""
        return {
            "market": self.market,
            "price": self.market_data.price,
            "price_change_24h": self.market_data.change_24h,
            "rounds": [r.to_dict() for r in self.all_rounds],
            "synthesis": self.final_synthesis,
            "recommendation": self.recommendation,
            "confidence": self.confidence,
            "reasoning": self.reasoning,
            "timestamp": self.created_at.isoformat(),
            "duration_sec": self.debate_duration_seconds,
        }


# ─── User Profile ──────────────────────────────────────────────────────────

@dataclass
class UserProfile:
    """Профиль пользователя для персонализации"""
    user_id: int
    risk_profile: RiskProfile = RiskProfile.MODERATE
    time_horizon: TimeHorizon = TimeHorizon.MEDIUM_TERM
    preferred_markets: List[Market] = field(default_factory=lambda: [Market.STOCKS, Market.CRYPTO])
    language: str = "ru"
    
    # Статистика
    total_analyses: int = 0
    successful_predictions: int = 0
    failed_predictions: int = 0
    
    # Предпочтения
    receive_daily_digest: bool = True
    receive_weekly_report: bool = True
    prefer_short_summaries: bool = True
    
    # Метаданные
    created_at: datetime = field(default_factory=datetime.now)
    last_active: datetime = field(default_factory=datetime.now)
    custom_instructions: str = ""
    
    @property
    def accuracy_rate(self) -> float:
        """Точность предсказаний"""
        total = self.successful_predictions + self.failed_predictions
        if total == 0:
            return 0.0
        return self.successful_predictions / total
    
    def build_system_prompt_suffix(self) -> str:
        """Добавить в системный промпт персонализацию"""
        parts = [
            f"\n\n🎯 Профиль пользователя:",
            f"• Риск: {self.risk_profile.value}",
            f"• Горизонт: {self.time_horizon.value}",
        ]
        if self.custom_instructions:
            parts.append(f"• Особые указания: {self.custom_instructions}")
        return "\n".join(parts)


# ─── Tracking & Storage Types ──────────────────────────────────────────────

@dataclass
class Prediction:
    """Отслеживаемое предсказание"""
    report_id: str
    symbol: str
    recommendation: str  # "BULLISH", "BEARISH", "NEUTRAL"
    entry_price: float
    timestamp: datetime = field(default_factory=datetime.now)
    resolved: bool = False
    actual_direction: Optional[str] = None  # "UP", "DOWN"
    profit_loss: Optional[float] = None  # процент
    resolved_at: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DebateSession:
    """Session сохранённого дебата"""
    session_id: str
    user_id: int
    market: str
    report: FinalReport
    debate_history: DebateHistory
    created_at: datetime = field(default_factory=datetime.now)
    expires_at: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
