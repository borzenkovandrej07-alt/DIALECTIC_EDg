"""
Централизованные версии промптов/пайплайна для аудита дайджестов.
Меняйте строки версий при существенных правках промптов или оркестратора.
"""

from __future__ import annotations

# Пайплайн генерации дайджеста (кодовые пути)
ANALYSIS_PIPELINE_ID = "analysis_service.run_full_analysis"
DEBATE_ORCHESTRATOR_ID = "agents.DebateOrchestrator"

# Логические версии (ручная метка; синхронизируйте с коммитом при релизе)
DIGEST_PIPELINE_VERSION = "2026-04-02.1"
SENTIMENT_MODULE_VERSION = "sentiment.analyze_and_filter_async/v1"
SANITIZER_VERSION = "report_sanitizer.sanitize_full_report/v1"


def get_digest_prompt_manifest() -> dict:
    """Снимок версий, сохраняемый вместе с daily_context."""
    return {
        "digest_pipeline_version": DIGEST_PIPELINE_VERSION,
        "analysis_pipeline": ANALYSIS_PIPELINE_ID,
        "debate_orchestrator": DEBATE_ORCHESTRATOR_ID,
        "sentiment": SENTIMENT_MODULE_VERSION,
        "sanitizer": SANITIZER_VERSION,
    }
