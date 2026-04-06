"""core/decision_engine.py
Centralized decision engine to orchestrate data flow:
- data -> analysis -> signals -> backtest -> metrics
- uses existing modules: market_data.py, analysis_service.py,
- tracker.py, trading_signal.py, backtester.py, metrics.py
"""

from __future__ import annotations

import logging
from typing import List, Dict, Any, Optional

from market_data import MarketDataFetcher
import analysis_service
from tracker import extract_predictions_from_report
from trading_signal import parse_signals_from_predictions, Signal, timeframe_to_hours
from backtester import Backtester, get_candles
from metrics import calculate_metrics, Metrics

logger = logging.getLogger(__name__)


class DecisionEngine:
    """Central orchestrator for data->analysis->signals->backtest->metrics.

    This class wires existing components without rewriting the underlying
    business logic and provides a single entry point for evaluating signals.
    """

    def __init__(
        self,
        data_provider: Optional[Any] = None,  # market_data.MarketDataFetcher
        analyst: Optional[Any] = None,         # analysis_service.run_full_analysis
        backtester: Optional[Backtester] = None,
    ) -> None:
        self.market = data_provider or MarketDataFetcher()
        # analysis service is expected to return (report_text, prices_dict)
        self.analyst = analyst or analysis_service.run_full_analysis
        self.backtester = backtester or Backtester()

    async def run_pipeline(self, user_id: int = 0, custom_news: str = "", custom_mode: bool = False) -> List[Signal]:
        """Fetch market snapshot, run analysis and convert to signals.

        Returns a list of validated Signal objects (LONG/SHORT) or empty list.
        """
        try:
            # 1) Get market snapshot (not strictly required for signals but keeps API stable)
            try:
                _ = await self.market.fetch_snapshot()
            except Exception:
                # snapshot may be optional for signals
                logger.debug("Market snapshot fetch failed, continuing without it")

            # 2) Run analysis to obtain ideas and a price context
            report, prices = await self.analyst(user_id, custom_news, custom_mode)
        except Exception as e:
            logger.error(f"Pipeline run failed during analysis step: {e}")
            return []

        # 3) Extract predictions from the report and convert to Signals
        try:
            preds = extract_predictions_from_report(report) or []
            signals = parse_signals_from_predictions(preds) if preds else []
        except Exception as e:
            logger.error(f"Failed to extract/parse signals from report: {e}")
            signals = []

        # Normalize directions and drop neutrals/incomplete
        clean: List[Signal] = []
        for sig in signals:
            d = (sig.direction or "").upper()
            if d in {"BUY", "LONG"}:
                sig.direction = "LONG"
            elif d in {"SELL", "SHORT"}:
                sig.direction = "SHORT"
            else:
                sig.direction = "NEUTRAL"
            # Basic validity check: entry/target/stop must exist and form a valid relation
            if sig.direction == "NEUTRAL":
                continue
            # estimate: in trading_signal we already have validate() that checks fields
            if hasattr(sig, "validate"):
                try:
                    if sig.validate():
                        clean.append(sig)
                except Exception:
                    continue
        return clean

    async def run_backtest(self, signals: List[Signal], user_id: int = 0) -> List[Any]:
        """Run backtests for a list of signals and return per-signal results."""
        results = []
        bt = self.backtester
        for sig in signals:
            hours = timeframe_to_hours(sig.timeframe) if hasattr(sig, "timeframe") else 24
            candles = await get_candles(sig.asset, hours)
            if not candles:
                continue
            res = bt.test_signal(sig, candles)
            results.append(res)
        return results

    async def run_full_evaluation(self, user_id: int = 0, custom_news: str = "", custom_mode: bool = False) -> Metrics:
        """Run pipeline and backtest, compute metrics and save results.json"""
        signals = await self.run_pipeline(user_id, custom_news, custom_mode)
        if not signals:
            logger.info("No signals produced by pipeline")
            metrics = calculate_metrics([])
            return metrics

        backtests = await self.run_backtest(signals, user_id)
        metrics = calculate_metrics(backtests)
        # Persist results as the central measure for external tooling
        try:
            from metrics import save_results
            save_results(backtests, metrics, save_to_file="results.json")
        except Exception as e:
            logger.warning(f"Failed to save results.json: {e}")
        return metrics

    def get_candles(self, asset: str, timeframe_hours: int) -> List[Any]:
        """Wrapper to get candles via market_data/backtester."""
        # Prefer backtester implementation if available
        try:
            return get_candles(asset, timeframe_hours)
        except Exception:
            return []
