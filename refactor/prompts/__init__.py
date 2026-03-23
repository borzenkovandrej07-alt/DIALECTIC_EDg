"""
Prompts submodule - Market and Russia analysis prompts for Dialectic Edge

Market Analysis Prompts:
- COMMON_GROUNDING_RULE, ANTI_HALLUCINATION_RULE (base rules)
- BULL_SYSTEM, BULL_COUNTER_SYSTEM (bullish analyst)
- BEAR_SYSTEM, BEAR_COUNTER_SYSTEM (skeptical analyst)
- VERIFIER_SYSTEM (quality control)
- SYNTH_SYSTEM (synthesis & recommendations)

Russia-Specific Prompts:
- RUSSIA_OPPORTUNITIES_SYSTEM (SMB opportunities in Russian market)
- RUSSIA_RISKS_SYSTEM (SMB risks in Russian market)
- RUSSIA_SYNTH_SYSTEM (Russian market synthesis)
"""

from .market import (
    COMMON_GROUNDING_RULE,
    ANTI_HALLUCINATION_RULE,
    BULL_SYSTEM,
    BULL_COUNTER_SYSTEM,
    BEAR_SYSTEM,
    BEAR_COUNTER_SYSTEM,
    VERIFIER_SYSTEM,
    SYNTH_SYSTEM,
)

from .russia import (
    RUSSIA_OPPORTUNITIES_SYSTEM,
    RUSSIA_RISKS_SYSTEM,
    RUSSIA_SYNTH_SYSTEM,
)

__all__ = [
    # Base rules
    "COMMON_GROUNDING_RULE",
    "ANTI_HALLUCINATION_RULE",
    # Market prompts
    "BULL_SYSTEM",
    "BULL_COUNTER_SYSTEM",
    "BEAR_SYSTEM",
    "BEAR_COUNTER_SYSTEM",
    "VERIFIER_SYSTEM",
    "SYNTH_SYSTEM",
    # Russia prompts
    "RUSSIA_OPPORTUNITIES_SYSTEM",
    "RUSSIA_RISKS_SYSTEM",
    "RUSSIA_SYNTH_SYSTEM",
]
