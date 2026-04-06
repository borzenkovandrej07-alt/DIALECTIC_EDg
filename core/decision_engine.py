"""core/decision_engine.py
Поток: analysis_service → ideas → Signal → RiskFilter → Backtester.

Использует существующие модули; demo-логика удалена из pipeline.
"""

from __future__ import annotations

import logging
from typing import Any, List, Optional

import analysis_service
from decision_engine import DecisionEngine as RiskFilterEngine
from market_data import MarketDataFetcher, fetch_ohlc_candles
from metrics import Metrics, calculate_metrics
from tracker import extract_predictions_from_report

from backtester import Backtester
from trading_signal import Signal, timeframe_to_hours

from .analysis_ideas_adapter import normalize_prediction_ideas
from .signal import convert_ideas_to_signals

logger = logging.getLogger(__name__)


def _load_risk_config() -> tuple[float, float, int]:
    """min_signal_confidence (0–100), min_risk_reward, candle_limit."""
    min_conf = 0.0
    min_rr = 1.0
    lim = 120
    try:
        from trading_system.config_loader import load_trading_config

        cfg = load_trading_config()
        min_conf = float(cfg.get("min_signal_confidence", 0.0))
        min_rr = float(cfg.get("min_risk_reward", 1.0))
        lim = int(cfg.get("candle_limit", 120))
    except Exception as e:
        logger.debug("risk config fallback: %s", e)
    return min_conf, min_rr, lim


class DecisionEngine:
    """Оркестратор: run_full_analysis → extract_predictions → ideas → signals → filter."""

    def __init__(
        self,
        data_provider: Any | None = None,
        analyst: Any | None = None,
        backtester: Backtester | None = None,
    ) -> None:
        self.market = data_provider or MarketDataFetcher()
        self.analyst = analyst or analysis_service.run_full_analysis
        self.backtester = backtester or Backtester()
        _mc, _mr, self.candle_limit = _load_risk_config()

    def filter(self, signals: List[Signal]) -> List[Signal]:
        """Совместимость с pipeline.py — делегирует RiskFilterEngine."""
        min_conf, min_rr, _ = _load_risk_config()
        rf = RiskFilterEngine(
            min_confidence=min_conf,
            min_risk_reward=min_rr,
            allowed_assets=None,
        )
        rf.reset()
        return rf.filter(signals)

    def rank(self, signals: List[Signal]) -> List[Signal]:
        min_conf, min_rr, _ = _load_risk_config()
        rf = RiskFilterEngine(
            min_confidence=min_conf,
            min_risk_reward=min_rr,
            allowed_assets=None,
        )
        rf.reset()
        return rf.rank(signals)

    async def run_pipeline(
        self,
        user_id: int = 0,
        custom_news: str = "",
        custom_mode: bool = False,
        symbols_filter: Optional[list[str]] = None,
    ) -> List[Signal]:
        """
        Запускает production-анализ (run_full_analysis), извлекает идеи из отчёта,
        конвертирует в Signal, фильтрует RiskFilterEngine.
        """
        min_conf, min_rr, _ = _load_risk_config()
        default_tf = "1w"
        try:
            from trading_system.config_loader import load_trading_config

            default_tf = str(
                (load_trading_config().get("backtest") or {}).get("timeframe_label") or "1w"
            )
        except Exception:
            pass

        try:
            try:
                await self.market.fetch_snapshot()
            except Exception:
                logger.debug("Market snapshot optional skip")

            report, _prices = await self.analyst(user_id, custom_news, custom_mode)
        except Exception as e:
            logger.error("Pipeline failed at analysis: %s", e)
            return []

        preds: list = []
        try:
            preds = extract_predictions_from_report(report) or []
        except Exception as e:
            logger.error("extract_predictions_from_report: %s", e)

        n_ideas_raw = len(preds)
        ideas = normalize_prediction_ideas(preds)
        n_ideas = len(ideas)

        signals = convert_ideas_to_signals(
            ideas,
            default_timeframe=default_tf,
            source="analysis_service",
        )
        n_sig = len(signals)

        rf = RiskFilterEngine(
            min_confidence=min_conf,
            min_risk_reward=min_rr,
            allowed_assets=None,
            max_signals_per_asset=3,
        )
        rf.reset()
        accepted = rf.filter(signals)
        ranked = rf.rank(accepted)
        n_filt = len(ranked)

        if symbols_filter:
            sf = {s.strip().upper() for s in symbols_filter if s}
            ranked = [s for s in ranked if s.asset.upper() in sf]

        n_out = len(ranked)

        print(
            f"[pipeline] ideas_from_report={n_ideas_raw} normalized_ideas={n_ideas} "
            f"signals={n_sig} after_risk_filter={n_filt} after_symbol_filter={n_out}",
            flush=True,
        )

        return ranked

    async def run_backtest(self, signals: List[Signal], user_id: int = 0) -> List[Any]:
        """Бэктест по свечам через market_data.fetch_ohlc_candles."""
        results = []
        bt = self.backtester
        _, _, lim = _load_risk_config()
        lim = getattr(self, "candle_limit", lim)

        for sig in signals:
            hours = timeframe_to_hours(sig.timeframe) if hasattr(sig, "timeframe") else 24
            candles = await fetch_ohlc_candles(sig.asset, timeframe_hours=hours, limit=lim)
            if not candles:
                logger.warning("No candles for %s", sig.asset)
                continue
            res = bt.test_signal(sig, candles)
            results.append(res)
        return results

    async def run_full_evaluation(
        self,
        user_id: int = 0,
        custom_news: str = "",
        custom_mode: bool = False,
    ) -> Metrics:
        signals = await self.run_pipeline(user_id, custom_news, custom_mode)
        if not signals:
            logger.info("No signals from pipeline")
            return calculate_metrics([])

        backtests = await self.run_backtest(signals, user_id)
        metrics = calculate_metrics(backtests)
        try:
            from metrics import save_results

            save_results(backtests, metrics, filepath="results.json")
        except Exception as e:
            logger.warning("save results.json: %s", e)
        return metrics

    def get_candles(self, asset: str, timeframe_hours: int) -> List[Any]:
        """Синхронная обёртка не используется в async-пайплайне; оставлена для совместимости."""
        import asyncio

        try:
            return asyncio.get_event_loop().run_until_complete(
                fetch_ohlc_candles(asset, timeframe_hours=timeframe_hours, limit=self.candle_limit)
            )
        except Exception:
            return []
