"""SignalBot — event-driven runtime using EventBus.

Architecture
------------
Primary path  : WS kline_close → EventBus.publish(KlineCloseEvent) → _on_kline_close
Fallback path : emergency scan every ``emergency_fallback_seconds`` if no kline events
Support tasks : shortlist refresh, OI refresh, heartbeat, health telemetry

``SignalPipeline`` is the only analysis entry point.  ``SignalBot`` orchestrates
market data, WebSocket subscriptions, shortlist management, signal selection,
delivery, and tracking.
"""
from __future__ import annotations

import asyncio
import contextlib
import inspect
import logging
import os
import time
from collections import Counter
from dataclasses import replace
from datetime import datetime, timezone
from typing import Any, cast

from ..config import BotSettings
from ..core.events import BookTickerEvent, KlineCloseEvent, ReconnectEvent
from ..core.engine import StrategyDecision, StrategyRegistry
from ..features import min_required_bars, prepare_symbol
from ..filters import apply_global_filters
from ..market_data import BinanceFuturesMarketData, MarketDataUnavailable
from ..models import PreparedSymbol, Signal, SymbolFrames, UniverseSymbol, PipelineResult
from ..outcomes import build_prepared_feature_snapshot, extract_features_from_signal
from ..setup_base import SetupParams
from ..strategies import STRATEGY_CLASSES
from ..telemetry import TelemetryStore
from ..tracking import SignalTrackingEvent
from ..confluence import ConfluenceEngine
from .container import build_application_container
from .cycle_runner import CycleRunner
from .shortlist_service import ShortlistService

UTC = timezone.utc
LOG = logging.getLogger("bot.application.bot")


class SignalBot:
    """Event-driven signal bot runtime.

    Parameters
    ----------
    settings : BotSettings
        Runtime configuration.
    market_data : BinanceFuturesMarketData | None
        Market data client (created internally if ``None``).
    broadcaster : Any | None
        Telegram broadcaster (created internally if ``None``).
    telemetry : TelemetryStore | None
        Telemetry store (created internally if ``None``).
    """

    def __init__(
        self,
        settings: BotSettings,
        *,
        market_data: BinanceFuturesMarketData | None = None,
        broadcaster: Any | None = None,
        telemetry: TelemetryStore | None = None,
    ) -> None:
        self.settings = settings
        container = build_application_container(
            settings,
            market_data=market_data,
            broadcaster=broadcaster,
            telemetry=telemetry,
            register_strategies=self._register_strategies_to_registry,
        )
        self.client = container.client
        self._bus = container.bus
        self._ws_manager = container.ws_manager
        self.telegram = container.telegram
        self.delivery = container.delivery
        self.telemetry = container.telemetry
        self.alerts = container.alerts
        self._modern_repo = container.repository
        LOG.info("MemoryRepository initialized | db=%s", self._modern_repo._db_path)

        # Note: All persistence now uses MemoryRepository (SQLite)
        # Legacy JSON stores (memory.json, state.json, tracking.json) removed in modern architecture

        # ML Filter for live signal enhancement
        from bot.ml_filter import MLFilter
        self.ml_filter = MLFilter(settings)
        self.confluence = ConfluenceEngine(settings, ml_filter=self.ml_filter)

        # Market regime analyzer
        from bot.market_regime import MarketRegimeAnalyzer
        self.market_regime = MarketRegimeAnalyzer(settings)

        # Metrics collector
        from bot.metrics import BotMetricsCollector
        self.metrics = BotMetricsCollector(settings.runtime.metrics_port)
        disable_http_servers = os.getenv("BOT_DISABLE_HTTP_SERVERS", "0").strip().lower() in ("1", "true", "yes")
        if disable_http_servers:
            LOG.info("http servers disabled via BOT_DISABLE_HTTP_SERVERS=1 (metrics/dashboard not started)")
        else:
            self.metrics.start_server()

        # Dashboard
        from bot.dashboard import BotDashboard
        self.dashboard = BotDashboard(self, settings.runtime.dashboard_port)
        if not disable_http_servers:
            self.dashboard.start_server()

        # Tracking & ML - uses Modern MemoryRepository
        # Legacy stores removed - all data in SQLite
        self.tracker = container.tracker
        self.intelligence = container.intelligence

        # Modern SignalEngine — core/ architecture (replaces legacy SignalPipeline)
        self._modern_registry = container.registry
        self._modern_engine = container.engine
        LOG.info("SignalEngine initialized with %d strategies", len(self._modern_registry))

        # Track fire-and-forget tasks for graceful shutdown
        self._background_tasks: set[asyncio.Task[Any]] = set()

        # Async state
        self._shutdown = asyncio.Event()
        self._analysis_semaphore = asyncio.Semaphore(settings.runtime.analysis_concurrency)
        self._last_kline_event_ts: float = 0.0
        self._shortlist: list[UniverseSymbol] = []
        self._last_live_shortlist: list[UniverseSymbol] = []
        self._symbol_meta_by_symbol: dict[str, Any] = {}
        self._shortlist_source: str = "startup"
        self._shortlist_lock = asyncio.Lock()
        self._cycle_failure_streak = 0
        self._circuit_open_until: float = 0.0
        self.last_cycle_summary: dict[str, Any] = {}
        self._prepare_error_count: int = 0
        self._last_prepare_error: dict[str, Any] = {}
        self._diagnostic_trace_counts: dict[str, int] = {}

        # Intra-candle scan throttle — monotonic timestamp of last scan per symbol
        self._last_intra_scan: dict[str, float] = {}
        self._last_intra_mid: dict[str, float] = {}
        self._shortlist_service = ShortlistService(self)
        self._cycle_runner = CycleRunner(self)

        # Subscribe to EventBus events
        self._bus.subscribe(KlineCloseEvent, self._on_kline_close)  # type: ignore[arg-type]
        self._bus.subscribe(ReconnectEvent, self._on_reconnect)  # type: ignore[arg-type]
        self._bus.subscribe(BookTickerEvent, self._on_book_ticker)  # type: ignore[arg-type]
        LOG.info("EventBus subscriptions registered | handlers=3 (kline_close, reconnect, book_ticker)")

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def _noncritical_timeout_seconds(self) -> float:
        return max(self.settings.ws.rest_timeout_seconds * 2.0, 10.0)

    @property
    def _delivery_timeout_seconds(self) -> float:
        return max(self.settings.ws.rest_timeout_seconds * 8.0, 30.0)

    def _get_shortlist_service(self) -> ShortlistService:
        service = getattr(self, "_shortlist_service", None)
        if service is None:
            service = ShortlistService(self)
            self._shortlist_service = service
        return service

    def _get_cycle_runner(self) -> CycleRunner:
        runner = getattr(self, "_cycle_runner", None)
        if runner is None:
            runner = CycleRunner(self)
            self._cycle_runner = runner
        return runner

    def _decision_to_reject_row(self, *, symbol: str, decision: StrategyDecision) -> dict[str, Any]:
        row: dict[str, Any] = {
            "ts": datetime.now(UTC).isoformat(),
            "symbol": symbol,
            "setup_id": decision.setup_id,
            "direction": getattr(decision.signal, "direction", "n/a"),
            "stage": decision.stage,
            "reason": decision.reason_code,
            "reason_code": decision.reason_code,
            "decision_status": decision.status,
        }
        if decision.details:
            row["details"] = decision.details
        if decision.missing_fields:
            row["missing_fields"] = list(decision.missing_fields)
        if decision.invalid_fields:
            row["invalid_fields"] = list(decision.invalid_fields)
        if decision.error is not None:
            row["error"] = decision.error
        return row

    def _append_symbol_trace(self, *, symbol: str, row: dict[str, Any]) -> None:
        limit = int(getattr(getattr(self.settings, "runtime", None), "diagnostic_trace_limit_per_symbol", 20))
        if limit <= 0 or not hasattr(self.telemetry, "append_symbol_jsonl"):
            return
        count = self._diagnostic_trace_counts.get(symbol, 0)
        if count >= limit:
            return
        self._diagnostic_trace_counts[symbol] = count + 1
        self.telemetry.append_symbol_jsonl("analysis", symbol, "strategy_traces.jsonl", row)

    def _append_strategy_decision_telemetry(
        self,
        *,
        symbol: str,
        trigger: str,
        decision: StrategyDecision,
    ) -> None:
        row: dict[str, Any] = {
            "ts": datetime.now(UTC).isoformat(),
            "symbol": symbol,
            "trigger": trigger,
            "setup_id": decision.setup_id,
            "status": decision.status,
            "stage": decision.stage,
            "reason": decision.reason_code,
            "reason_code": decision.reason_code,
            "missing_fields": list(decision.missing_fields),
            "invalid_fields": list(decision.invalid_fields),
        }
        if decision.signal is not None:
            row["direction"] = decision.signal.direction
            row["score"] = round(decision.signal.score, 4)
        if decision.details:
            row["details"] = decision.details
        if decision.error is not None:
            row["error"] = decision.error
        self.telemetry.append_jsonl("strategy_decisions.jsonl", row)
        if decision.missing_fields or decision.invalid_fields:
            self.telemetry.append_jsonl("data_quality.jsonl", row)
        if decision.status in {"signal", "reject", "error"}:
            self._append_symbol_trace(symbol=symbol, row=row)

    # ------------------------------------------------------------------
    # Modern Engine Migration
    # ------------------------------------------------------------------

    def _register_strategies_to_registry(self, registry: StrategyRegistry) -> None:
        """Register concrete strategies directly with a provided registry."""
        enabled_count = 0
        for strategy_cls in STRATEGY_CLASSES:
            setup_id = strategy_cls.setup_id
            is_enabled = bool(getattr(self.settings.setups, setup_id, False))
            strategy = strategy_cls(SetupParams(enabled=is_enabled), self.settings)
            registry.register(strategy, enabled=is_enabled)
            if is_enabled:
                enabled_count += 1
            LOG.info("registered strategy %s (enabled=%s)", setup_id, is_enabled)

        LOG.info(
            "strategies registered | total=%d enabled=%d",
            len(STRATEGY_CLASSES),
            enabled_count,
        )

    async def _run_modern_analysis(
        self,
        item: UniverseSymbol,
        frames: SymbolFrames,
        trigger: str = "modern_engine",
        event_ts: datetime | None = None,
        ws_enrichments: dict[str, Any] | None = None,
    ) -> PipelineResult:
        """Run modern SignalEngine analysis for a symbol.
        
        Replaces legacy SignalPipeline.process_symbol().
        
        Returns:
            PipelineResult compatible with legacy pipeline output
        """
        event_ts = event_ts or datetime.now(UTC)
        candidates: list[Signal] = []
        rejected: list[dict[str, Any]] = []
        prepared: PreparedSymbol | None = None
        funnel: dict[str, Any] = {
            "shortlist_entered": True,
            "frame_rows": {},
            "frame_readiness": {},
            "detector_runs": 0,
            "post_filter_candidates": 0,
            "raw_hits": 0,
            "raw_hits_by_setup": {},
            "strategy_rejects_by_setup": {},
            "family_precheck_rejects": 0,
            "alignment_penalties": 0,
            "confirmation_rejects": 0,
            "filters_rejects": 0,
            "selected": 0,
            "delivered": 0,
        }
        
        LOG.info("%s: starting modern analysis | trigger=%s", item.symbol, trigger)
        item = self._refresh_universe_symbol_from_ws(item)

        minimums = min_required_bars(
            min_bars_15m=self.settings.filters.min_bars_15m,
            min_bars_1h=self.settings.filters.min_bars_1h,
            min_bars_4h=self.settings.filters.min_bars_4h,
        )
        rows_4h = frames.df_4h.height if frames.df_4h is not None else 0
        rows_5m = frames.df_5m.height if frames.df_5m is not None else 0
        rows_1h = frames.df_1h.height
        rows_15m = frames.df_15m.height
        funnel["frame_rows"] = {
            "15m": rows_15m,
            "1h": rows_1h,
            "5m": rows_5m,
            "4h": rows_4h,
        }
        funnel["frame_readiness"] = {
            "15m": rows_15m >= minimums["15m"],
            "1h": rows_1h >= minimums["1h"],
            "5m": rows_5m >= minimums["5m"],
            "4h": rows_4h >= minimums["4h"],
        }
        if rows_1h < minimums["1h"] or rows_15m < minimums["15m"]:
            missing_required = []
            if rows_15m < minimums["15m"]:
                missing_required.append("15m")
            if rows_1h < minimums["1h"]:
                missing_required.append("1h")
            rejected.append(
                {
                    "ts": datetime.now(UTC).isoformat(),
                    "symbol": item.symbol,
                    "setup_id": "data",
                    "direction": "n/a",
                    "stage": "data",
                    "reason": "insufficient_required_history",
                    "rows_1h": rows_1h,
                    "rows_15m": rows_15m,
                    "rows_5m": rows_5m,
                    "rows_4h": rows_4h,
                    "need_1h": minimums["1h"],
                    "need_15m": minimums["15m"],
                    "need_5m": minimums["5m"],
                    "need_4h": minimums["4h"],
                    "missing_required_frames": missing_required,
                }
            )
            LOG.warning(
                "%s: insufficient required history for analysis | 15m=%d/%d 1h=%d/%d optional_5m=%d/%d optional_4h=%d/%d",
                item.symbol,
                rows_15m,
                minimums["15m"],
                rows_1h,
                minimums["1h"],
                rows_5m,
                minimums["5m"],
                rows_4h,
                minimums["4h"],
            )
            return PipelineResult(
                symbol=item.symbol,
                trigger=trigger,
                event_ts=event_ts,
                raw_setups=0,
                candidates=candidates,
                rejected=rejected,
                status="insufficient_required_history",
                prepared=None,
                funnel=funnel,
            )

        try:
            # Build prepared symbol using modern prepare_symbol
            prepared = prepare_symbol(
                item,
                frames,
                minimums=minimums,
                settings=self.settings,
            )
            if prepared is not None and ws_enrichments:
                for key, value in ws_enrichments.items():
                    if hasattr(prepared, key):
                        setattr(prepared, key, value)
                # Debug: log enrichment status
                if ws_enrichments.get("mark_index_spread_bps") is not None:
                    LOG.debug("%s: enrichment mark_index_spread_bps=%.4f", item.symbol, ws_enrichments["mark_index_spread_bps"])
                else:
                    LOG.debug("%s: enrichment mark_index_spread_bps=None (ws_data_missing)", item.symbol)
            LOG.debug("%s: prepared symbol built | work_15m_rows=%s work_1h_rows=%s",
                      item.symbol,
                      prepared.work_15m.height if prepared is not None and prepared.work_15m is not None else 0,
                      prepared.work_1h.height if prepared is not None and prepared.work_1h is not None else 0)
        except Exception as exc:
            self._prepare_error_count += 1
            self._last_prepare_error = {
                "ts": datetime.now(UTC).isoformat(),
                "symbol": item.symbol,
                "stage": "prepare_symbol",
                "exception_type": type(exc).__name__,
                "error": str(exc),
            }
            funnel["prepare_error_stage"] = "prepare_symbol"
            funnel["prepare_error_exception_type"] = type(exc).__name__
            LOG.warning("%s: failed to build prepared symbol: %s", item.symbol, exc)
            return PipelineResult(
                symbol=item.symbol,
                trigger=trigger,
                event_ts=event_ts,
                raw_setups=0,
                candidates=candidates,
                rejected=rejected,
                error=str(exc),
                status="prepare_error",
                prepared=prepared,
                funnel=funnel,
            )
        
        # Run modern engine (replaces pipeline analysis)
        if prepared is None:
            LOG.warning("%s: prepared symbol is None", item.symbol)
            return PipelineResult(
                symbol=item.symbol,
                trigger=trigger,
                event_ts=event_ts,
                raw_setups=0,
                candidates=candidates,
                rejected=rejected,
                status="prepare_failed",
                prepared=None,
                funnel=funnel,
            )
        
        # Log engine stats before calculation
        engine_stats = self._modern_engine.get_engine_stats()
        LOG.debug("%s: engine stats | enabled_strategies=%d total=%d",
                  item.symbol, engine_stats.get('enabled_strategies', 0),
                  engine_stats.get('total_strategies', 0))
        self._diagnostic_trace_counts[item.symbol] = 0
        
        try:
            signal_results = await self._modern_engine.calculate_all(prepared)
            funnel["detector_runs"] = len(signal_results)
            LOG.debug("%s: engine calculated | results_count=%d", item.symbol, len(signal_results))
        except Exception as exc:
            LOG.warning("%s: modern engine calculation failed: %s", item.symbol, exc)
            return PipelineResult(
                symbol=item.symbol,
                trigger=trigger,
                event_ts=event_ts,
                raw_setups=0,
                candidates=candidates,
                rejected=rejected,
                error=str(exc),
                status="engine_error",
                prepared=prepared,
                funnel=funnel,
            )
        
        # Process results: convert SignalResult to Signal, then apply the
        # production hard-gate + confluence path before a signal can become a
        # runtime candidate.
        signals_found = 0
        signals_rejected_perf = 0
        signals_added = 0
        
        for result in signal_results:
            setup_id = result.setup_id or result.metadata.get("setup_id") or getattr(result.signal, "setup_id", "unknown")
            decision = result.decision
            if decision is None:
                decision = StrategyDecision.error_result(
                    setup_id=setup_id,
                    reason_code="runtime.missing_decision",
                    error=result.error or "missing strategy decision",
                    stage="engine",
                    details={"symbol": item.symbol},
                )
            self._append_strategy_decision_telemetry(
                symbol=item.symbol,
                trigger=trigger,
                decision=decision,
            )
            if decision.is_error or decision.is_skip or decision.is_reject:
                funnel["strategy_rejects_by_setup"][setup_id] = (
                    funnel["strategy_rejects_by_setup"].get(setup_id, 0) + 1
                )
                rejected.append(self._decision_to_reject_row(symbol=item.symbol, decision=decision))
                LOG.debug(
                    "%s: strategy produced no signal | setup=%s status=%s reason=%s",
                    item.symbol,
                    setup_id,
                    decision.status,
                    decision.reason_code,
                )
                continue

            signal = decision.signal or result.signal
            if signal is None:
                fallback_decision = StrategyDecision.reject(
                    setup_id=setup_id,
                    stage="strategy",
                    reason_code="runtime.signal_missing_after_hit",
                    details={"symbol": item.symbol},
                )
                funnel["strategy_rejects_by_setup"][setup_id] = (
                    funnel["strategy_rejects_by_setup"].get(setup_id, 0) + 1
                )
                rejected.append(self._decision_to_reject_row(symbol=item.symbol, decision=fallback_decision))
                continue

            setup_id = signal.setup_id
            metadata = self._strategy_metadata(setup_id)
            signal = self._apply_strategy_metadata(signal, metadata)

            precheck_ok, precheck_reason, precheck_details = self._check_family_precheck(
                signal,
                prepared,
                metadata,
            )
            if not precheck_ok:
                rejected.append({
                    "ts": datetime.now(UTC).isoformat(),
                    "symbol": item.symbol,
                    "setup_id": signal.setup_id,
                    "direction": signal.direction,
                    "stage": "family_precheck",
                    "reason": precheck_reason or "family_precheck_reject",
                    "details": precheck_details,
                })
                funnel["family_precheck_rejects"] += 1
                continue

            signal, alignment_details = self._apply_alignment_penalty(signal, prepared, metadata)
            if alignment_details.get("applied"):
                funnel["alignment_penalties"] += 1

            signals_found += 1
            funnel["raw_hits"] += 1
            funnel["raw_hits_by_setup"][signal.setup_id] = (
                funnel["raw_hits_by_setup"].get(signal.setup_id, 0) + 1
            )

            ltf_ok, ltf_reason, ltf_details = self._check_family_confirmation(signal, prepared, metadata)
            if not ltf_ok:
                rejected.append({
                    "ts": datetime.now(UTC).isoformat(),
                    "symbol": item.symbol,
                    "setup_id": signal.setup_id,
                    "direction": signal.direction,
                    "stage": "confirmation",
                    "reason": ltf_reason or "5m_confirmation_reject",
                    "details": ltf_details,
                })
                funnel["confirmation_rejects"] += 1
                continue
            
            # Check performance guard using modern repo
            score_adj = await self._modern_repo.get_setup_score_adjustment(signal.setup_id)
            if score_adj < -0.3:  # Suppressed due to poor performance
                rejected.append({
                    "ts": datetime.now(UTC).isoformat(),
                    "symbol": item.symbol,
                    "setup_id": signal.setup_id,
                    "direction": signal.direction,
                    "stage": "perf_guard",
                    "reason": "setup_underperforming",
                })
                signals_rejected_perf += 1
                continue

            passed, filtered_signal, filter_reason, scoring_result, filter_details = apply_global_filters(
                signal,
                prepared,
                self.settings,
                self.confluence,
            )
            if not passed:
                reject_row: dict[str, Any] = {
                    "ts": datetime.now(UTC).isoformat(),
                    "symbol": item.symbol,
                    "setup_id": signal.setup_id,
                    "direction": signal.direction,
                    "stage": "filters",
                    "reason": filter_reason or "filter_rejected",
                }
                if scoring_result is not None:
                    scoring_payload = scoring_result.to_dict()
                    scoring_payload["setup_id"] = signal.setup_id
                    reject_row["scoring"] = scoring_payload
                if filter_details:
                    reject_row["details"] = filter_details
                rejected.append(reject_row)
                funnel["filters_rejects"] += 1
                continue

            candidates.append(filtered_signal)
            signals_added += 1
            LOG.debug("%s: candidate signal | setup=%s dir=%s score=%.3f rr=%.2f",
                      item.symbol, filtered_signal.setup_id, filtered_signal.direction,
                      filtered_signal.score, filtered_signal.risk_reward or 0)
        
        LOG.info("%s: analysis complete | trigger=%s raw_strategies=%d signals_found=%d perf_rejected=%d candidates=%d",
                 item.symbol, trigger, len(signal_results), signals_found,
                 signals_rejected_perf, signals_added)
        funnel["post_filter_candidates"] = len(candidates)

        return PipelineResult(
            symbol=item.symbol,
            trigger=trigger,
            event_ts=event_ts,
            raw_setups=len(signal_results),
            candidates=candidates,
            rejected=rejected,
            status="no_setups" if len(signal_results) == 0 else "ok",
            prepared=prepared,
            funnel=funnel,
        )

    def _select_and_rank(
        self,
        all_candidates: dict[str, list[Signal]],
        max_signals: int,
    ) -> list[Signal]:
        """Select and rank signals from all candidates.
        
        Replaces legacy SignalPipeline.select_and_rank().
        
        Args:
            all_candidates: Dict of symbol -> list of candidate signals
            max_signals: Maximum signals to select per cycle
            
        Returns:
            List of selected signals, ranked by score
        """
        # Flatten all candidates
        flat_candidates: list[Signal] = []
        for symbol_candidates in all_candidates.values():
            flat_candidates.extend(symbol_candidates)
        
        if not flat_candidates:
            return []
        
        # Rank by score (descending), then by risk_reward (descending)
        ranked = sorted(
            flat_candidates,
            key=lambda s: (s.score, s.risk_reward or 0.0),
            reverse=True
        )
        
        # Select top N
        selected = ranked[:max_signals]
        
        LOG.debug(
            "select_and_rank | candidates=%d selected=%d",
            len(flat_candidates), len(selected)
        )
        
        return selected

    def _strategy_metadata(self, setup_id: str) -> Any | None:
        strategy = self._modern_registry.get(setup_id)
        return strategy.metadata if strategy is not None else None

    def _apply_strategy_metadata(
        self,
        signal: Signal,
        metadata: Any | None,
    ) -> Signal:
        if metadata is None:
            return signal
        return replace(
            signal,
            strategy_family=getattr(metadata, "family", signal.strategy_family),
            confirmation_profile=getattr(metadata, "confirmation_profile", signal.confirmation_profile),
        )

    @staticmethod
    def _frame_float(frame: Any, column: str) -> float | None:
        if frame is None or getattr(frame, "is_empty", lambda: True)():
            return None
        if column not in getattr(frame, "columns", []):
            return None
        try:
            value = frame.item(-1, column)
        except Exception:
            return None
        try:
            if value is None:
                return None
            numeric = float(value)
        except (TypeError, ValueError):
            return None
        return numeric if numeric == numeric and numeric not in (float("inf"), float("-inf")) else None

    def _directional_context(
        self,
        signal: Signal,
        prepared: PreparedSymbol,
    ) -> dict[str, Any]:
        work_5m = prepared.work_5m
        close_5m = self._frame_float(work_5m, "close")
        ema20_5m = self._frame_float(work_5m, "ema20")
        supertrend_5m = self._frame_float(work_5m, "supertrend_dir")
        delta_ratio_5m = self._frame_float(work_5m, "delta_ratio")
        taker_ratio = prepared.taker_ratio
        flow_proxy = None
        if prepared.agg_trade_delta_30s is not None:
            flow_proxy = float(prepared.agg_trade_delta_30s)
        elif taker_ratio is not None:
            flow_proxy = float(taker_ratio) - 1.0
        elif delta_ratio_5m is not None:
            flow_proxy = float(delta_ratio_5m) - 0.5

        premium_velocity = prepared.premium_slope_5m
        if premium_velocity is None:
            premium_velocity = prepared.mark_index_spread_bps
        depth_imbalance = prepared.depth_imbalance
        microprice_bias = prepared.microprice_bias
        depth_proxy = depth_imbalance if depth_imbalance is not None else microprice_bias
        if depth_proxy is None and prepared.spread_bps is not None and prepared.spread_bps > 0:
            depth_proxy = 0.0

        direction = signal.direction
        if direction == "long":
            trend_confirms = bool(
                close_5m is not None
                and ema20_5m is not None
                and close_5m >= ema20_5m
                and (supertrend_5m is None or supertrend_5m >= 0.0)
            )
            flow_confirms = bool(
                (flow_proxy is not None and flow_proxy >= 0.03)
                or (delta_ratio_5m is not None and delta_ratio_5m >= 0.53)
            )
            premium_confirms = bool(
                (premium_velocity is not None and premium_velocity >= 0.0)
                or (prepared.mark_index_spread_bps is not None and prepared.mark_index_spread_bps >= -4.0)
            )
            depth_confirms = bool(
                (depth_imbalance is not None and depth_imbalance >= 0.05)
                or (microprice_bias is not None and microprice_bias >= 0.0)
            )
            premium_exhaustion = bool(
                (prepared.premium_zscore_5m is not None and prepared.premium_zscore_5m <= -1.5)
                or (prepared.mark_index_spread_bps is not None and prepared.mark_index_spread_bps <= -8.0)
            )
            liquidation_exhaustion = bool(
                prepared.liquidation_score is not None and prepared.liquidation_score <= -0.35
            )
            crowd_exhaustion = bool(
                (prepared.global_ls_ratio is not None and prepared.global_ls_ratio <= 0.9)
                or (prepared.top_vs_global_ls_gap is not None and prepared.top_vs_global_ls_gap <= -0.1)
            )
            aggressor_reversal = bool(
                prepared.aggression_shift is not None and prepared.aggression_shift >= 0.03
            )
            regime_opposes = prepared.regime_1h_confirmed == "downtrend" or prepared.bias_1h == "downtrend"
            flow_opposes = bool(flow_proxy is not None and flow_proxy <= -0.03)
        else:
            trend_confirms = bool(
                close_5m is not None
                and ema20_5m is not None
                and close_5m <= ema20_5m
                and (supertrend_5m is None or supertrend_5m <= 0.0)
            )
            flow_confirms = bool(
                (flow_proxy is not None and flow_proxy <= -0.03)
                or (delta_ratio_5m is not None and delta_ratio_5m <= 0.47)
            )
            premium_confirms = bool(
                (premium_velocity is not None and premium_velocity <= 0.0)
                or (prepared.mark_index_spread_bps is not None and prepared.mark_index_spread_bps <= 4.0)
            )
            depth_confirms = bool(
                (depth_imbalance is not None and depth_imbalance <= -0.05)
                or (microprice_bias is not None and microprice_bias <= 0.0)
            )
            premium_exhaustion = bool(
                (prepared.premium_zscore_5m is not None and prepared.premium_zscore_5m >= 1.5)
                or (prepared.mark_index_spread_bps is not None and prepared.mark_index_spread_bps >= 8.0)
            )
            liquidation_exhaustion = bool(
                prepared.liquidation_score is not None and prepared.liquidation_score <= -0.35
            )
            crowd_exhaustion = bool(
                (prepared.global_ls_ratio is not None and prepared.global_ls_ratio >= 1.1)
                or (prepared.top_vs_global_ls_gap is not None and prepared.top_vs_global_ls_gap >= 0.1)
            )
            aggressor_reversal = bool(
                prepared.aggression_shift is not None and prepared.aggression_shift <= -0.03
            )
            regime_opposes = prepared.regime_1h_confirmed == "uptrend" or prepared.bias_1h == "uptrend"
            flow_opposes = bool(flow_proxy is not None and flow_proxy >= 0.03)

        exhaustion_hits = {
            "premium_extreme": premium_exhaustion,
            "liquidation_imbalance": liquidation_exhaustion,
            "crowd_stretch": crowd_exhaustion,
            "aggressor_reversal": aggressor_reversal,
        }
        return {
            "used": work_5m is not None and not work_5m.is_empty(),
            "close_5m": close_5m,
            "ema20_5m": ema20_5m,
            "supertrend_dir_5m": supertrend_5m,
            "delta_ratio_5m": delta_ratio_5m,
            "flow_proxy": flow_proxy,
            "mark_index_spread_bps": prepared.mark_index_spread_bps,
            "premium_zscore_5m": prepared.premium_zscore_5m,
            "premium_slope_5m": prepared.premium_slope_5m,
            "depth_imbalance": prepared.depth_imbalance,
            "microprice_bias": prepared.microprice_bias,
            "regime_1h": prepared.regime_1h_confirmed,
            "bias_1h": prepared.bias_1h,
            "trend_confirms": trend_confirms,
            "flow_confirms": flow_confirms,
            "premium_confirms": premium_confirms,
            "depth_confirms": depth_confirms,
            "regime_opposes": regime_opposes,
            "flow_opposes": flow_opposes,
            "exhaustion_hits": exhaustion_hits,
            "exhaustion_count": sum(1 for value in exhaustion_hits.values() if value),
        }

    def _check_family_precheck(
        self,
        signal: Signal,
        prepared: PreparedSymbol,
        metadata: Any | None,
    ) -> tuple[bool, str | None, dict[str, Any]]:
        details = self._directional_context(signal, prepared)
        family = getattr(metadata, "family", signal.strategy_family)
        profile = getattr(metadata, "confirmation_profile", signal.confirmation_profile)
        details["family"] = family
        details["confirmation_profile"] = profile
        strong_opposition = details["regime_opposes"] and details["flow_opposes"]
        if family in {"continuation", "breakout"} and strong_opposition and details["exhaustion_count"] == 0:
            return False, f"family_precheck_opposes_{signal.direction}", details
        if profile == "trend_follow" and details["flow_opposes"] and not details["trend_confirms"]:
            return False, f"flow_precheck_opposes_{signal.direction}", details
        return True, None, details

    def _apply_alignment_penalty(
        self,
        signal: Signal,
        prepared: PreparedSymbol,
        metadata: Any | None,
    ) -> tuple[Signal, dict[str, Any]]:
        family = getattr(metadata, "family", signal.strategy_family)
        profile = getattr(metadata, "confirmation_profile", signal.confirmation_profile)
        regime = prepared.regime_1h_confirmed
        bias = prepared.bias_1h
        direction = signal.direction
        if direction == "long":
            opposing_votes = int(regime == "downtrend") + int(bias == "downtrend")
        else:
            opposing_votes = int(regime == "uptrend") + int(bias == "uptrend")
        details = {
            "regime_1h": regime,
            "bias_1h": bias,
            "opposing_votes": opposing_votes,
            "applied": False,
            "family": family,
            "confirmation_profile": profile,
        }
        if opposing_votes == 0 or family == "reversal" or profile == "countertrend_exhaustion":
            return signal, details
        if signal.score <= 0.0:
            details["skipped_reason"] = "non_positive_score"
            return signal, details
        penalty_factor = 0.92 if opposing_votes == 1 else 0.85
        reasons = signal.reasons
        if "alignment_penalty" not in reasons:
            reasons = (*reasons, "alignment_penalty")
        adjusted_signal = replace(
            signal,
            score=round(max(signal.score * penalty_factor, 0.0), 4),
            reasons=reasons,
        )
        details["applied"] = True
        details["penalty_factor"] = penalty_factor
        return adjusted_signal, details

    def _check_family_confirmation(
        self,
        signal: Signal,
        prepared: PreparedSymbol,
        metadata: Any | None,
    ) -> tuple[bool, str | None, dict[str, Any]]:
        details = self._directional_context(signal, prepared)
        family = getattr(metadata, "family", signal.strategy_family)
        profile = getattr(metadata, "confirmation_profile", signal.confirmation_profile)
        details["family"] = family
        details["confirmation_profile"] = profile
        if (
            not details["used"]
            and details["flow_proxy"] is None
            and prepared.mark_index_spread_bps is None
            and prepared.depth_imbalance is None
            and prepared.microprice_bias is None
        ):
            details["fallback"] = "context_missing"
            strict_data_quality = bool(
                getattr(getattr(getattr(self, "settings", None), "runtime", None), "strict_data_quality", True)
            )
            if strict_data_quality and family in {"continuation", "breakout"}:
                return False, "data.fast_context_missing", details
            return True, None, details
        confirmation_votes = {
            "trend_5m": details["trend_confirms"],
            "flow_5m": details["flow_confirms"],
            "premium_slope": details["premium_confirms"],
            "depth_focus": details["depth_confirms"],
        }
        details["confirmation_votes"] = confirmation_votes
        details["confirmation_count"] = sum(1 for value in confirmation_votes.values() if value)

        if family == "reversal" or profile == "countertrend_exhaustion":
            if details["exhaustion_count"] > 0:
                return True, None, details
            if details["regime_opposes"] and details["flow_opposes"]:
                return False, f"reversal_unconfirmed_{signal.direction}", details
            return True, None, details

        if details["confirmation_count"] >= 2:
            return True, None, details
        if details["regime_opposes"] and details["flow_opposes"] and details["exhaustion_count"] == 0:
            return False, f"hard_context_opposes_{signal.direction}", details
        return False, f"5m_opposes_{signal.direction}", details

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def request_shutdown(self) -> None:
        self._shutdown.set()

    async def start(self) -> None:
        """Initial storage checks and WS bootstrap."""
        self._preflight_storage_check()

        # Initialize modern repository (SQLite)
        try:
            await self._modern_repo.initialize()
            LOG.info("modern repository initialized | SQLite ready")
        except Exception as exc:
            LOG.warning("modern repository init failed (non-fatal): %s", exc)

        try:
            startup_tracking_events = await self.tracker.review_open_signals(dry_run=False)
            if startup_tracking_events:
                LOG.info(
                    "startup tracking sweep closed open signals | events=%d",
                    len(startup_tracking_events),
                )
                await self._deliver_tracking(startup_tracking_events)
        except Exception as exc:
            LOG.warning("startup tracking sweep failed (non-fatal): %s", exc)

        # Get modern repository summary
        mem_summary = await self._modern_repo.summary()
        market_ctx = await self._modern_repo.get_market_context()
        await self._sync_ws_tracked_symbols()
        LOG.info(
            "runtime initialized | setups=%d shortlist_limit=%d "
            "memory_symbols=%d btc_bias=%s blacklisted=%s",
            len(self.settings.setups.enabled_setup_ids()),
            self.settings.universe.shortlist_limit,
            mem_summary.get("symbol_count", 0),
            market_ctx.get("btc_bias", "neutral"),
            mem_summary.get("blacklisted_symbols") or "none",
        )
        if self._ws_manager is not None:
            # Build full shortlist immediately instead of using only 4 pinned symbols
            try:
                shortlist_timeout_s = max(30.0, float(self.settings.ws.rest_timeout_seconds) * 2.5)
                shortlist = await asyncio.wait_for(
                    self._do_refresh_shortlist(),
                    timeout=shortlist_timeout_s,
                )
                symbols = [s.symbol for s in shortlist]
                LOG.info(
                    "starting ws_manager with shortlist | symbols=%d timeout=%.1fs",
                    len(symbols),
                    shortlist_timeout_s,
                )
            except asyncio.TimeoutError:
                LOG.warning(
                    "shortlist build timed out; using pinned | timeout=%.1fs pinned=%d",
                    shortlist_timeout_s,
                    len(self.settings.universe.pinned_symbols),
                )
                symbols = list(self.settings.universe.pinned_symbols)
            except Exception as exc:
                LOG.warning("shortlist build failed, using pinned | error=%s", exc)
                symbols = list(self.settings.universe.pinned_symbols)
             
            try:
                await self._ws_manager.start(symbols)
            except Exception as exc:
                LOG.info("ws_manager start failed (non-fatal, will use REST): %s", exc)

        # Preload historical frames in the background so `prepare_symbol` can
        # meet its required 15m/1h history and optional 5m/4h context. This is deliberately
        # lightweight (batch + delay) to avoid REST storms.
        if isinstance(self.client, BinanceFuturesMarketData):
            preload_task = asyncio.create_task(self._preload_shortlist_frames(), name="preload_frames")
            self._background_tasks.add(preload_task)
            preload_task.add_done_callback(self._background_tasks.discard)

    async def run_forever(self) -> None:
        """Main loop — EventBus-driven with emergency fallback."""
        bus_task = asyncio.create_task(self._bus.run(), name="event_bus")
        # Give EventBus a moment to start before WS events arrive
        await asyncio.sleep(0.1)
        LOG.info("event bus started and ready")

        background_tasks: list[asyncio.Task[None]] = [
            asyncio.create_task(self._refresh_shortlist_periodic(), name="shortlist_refresh"),
            asyncio.create_task(self._heartbeat_periodic(), name="heartbeat"),
            asyncio.create_task(self._health_telemetry_periodic(), name="health_telemetry"),
            asyncio.create_task(self._emergency_fallback_scan(), name="emergency_fallback"),
            asyncio.create_task(self._oi_refresh_periodic(), name="oi_refresh"),
            asyncio.create_task(self._tracking_review_periodic(), name="tracking_review"),
            asyncio.create_task(self._market_regime_periodic(), name="market_regime"),
        ]
        if self.intelligence is not None and self.settings.intelligence.enabled:
            background_tasks.append(
                asyncio.create_task(self._public_intelligence_periodic(), name="public_intelligence")
            )

        LOG.info(
            "event-driven mode active | emergency_fallback=%ss",
            self.settings.runtime.emergency_fallback_seconds,
        )

        try:
            await self._shutdown.wait()
        finally:
            bus_task.cancel()
            for t in background_tasks:
                t.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.gather(bus_task, *background_tasks, return_exceptions=True)
            # close() is called by CLI finally block; don't duplicate here

    async def close(self) -> None:
        """Graceful shutdown."""
        # Cancel and await all background tasks
        if self._background_tasks:
            for task in list(self._background_tasks):
                task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.gather(*self._background_tasks, return_exceptions=True)
            self._background_tasks.clear()

        if self._ws_manager is not None:
            await self._ws_manager.stop()
        # Modern repository auto-closes with connection
        try:
            await self.alerts.close()
        except Exception as exc:
            LOG.debug("alerts.close() failed (non-fatal): %s", exc)

        # Close external resources (best-effort).
        try:
            await self._modern_repo.close()
        except Exception as exc:
            LOG.debug("modern repo close failed (non-fatal): %s", exc)

        try:
            close_md = getattr(self.client, "close", None)
            if callable(close_md):
                result = close_md()
                if inspect.isawaitable(result):
                    await result
        except Exception as exc:
            LOG.debug("market data close failed (non-fatal): %s", exc)

        try:
            close_tg = getattr(self.telegram, "close", None)
            if callable(close_tg):
                result = close_tg()
                if inspect.isawaitable(result):
                    await result
        except Exception as exc:
            LOG.debug("telegram close failed (non-fatal): %s", exc)

    # ------------------------------------------------------------------
    # EventBus handlers
    # ------------------------------------------------------------------

    async def _on_kline_close(self, event: KlineCloseEvent) -> None:
        """Handle kline_close from EventBus — primary analysis path."""
        if event.interval != "15m":
            return  # only process 15m closes for signal detection

        self._last_kline_event_ts = asyncio.get_running_loop().time()
        symbol = event.symbol
        LOG.info("kline_close received | symbol=%s trigger=%s", symbol, event.trigger)

        async with self._shortlist_lock:
            shortlist = list(self._shortlist)

        # Review tracking for symbol
        tracking_events = await self.tracker.review_open_signals_for_symbol(symbol, dry_run=False)
        if tracking_events:
            await self._deliver_tracking(tracking_events)

        # Find symbol in shortlist
        item = next((row for row in shortlist if row.symbol == symbol), None)
        if item is None:
            LOG.debug("kline_close skipped | symbol=%s not in shortlist", symbol)
            return  # symbol not in shortlist, skip silently

        await self._get_cycle_runner().execute_symbol_cycle(
            symbol=symbol,
            item=item,
            interval=event.interval,
            trigger=event.trigger,
            event_ts=datetime.now(UTC),
            tracking_events=tracking_events,
            shortlist_size=len(shortlist),
        )

    async def _on_reconnect(self, event: ReconnectEvent) -> None:
        LOG.info("ws reconnected | reason=%s", event.reason)

    async def _on_book_ticker(self, event: BookTickerEvent) -> None:
        """Intra-candle scan trigger — fires at most once per throttle interval per symbol.

        bookTicker events arrive on every tick. We throttle (configurable via
        intra_candle_throttle_seconds) and fire a non-blocking analysis task so
        the event loop is never stalled. The cached frames from the last kline-close
        are still fresh enough for mid-candle signal detection.
        """
        symbol = event.symbol
        if not hasattr(self, "_last_intra_mid"):
            self._last_intra_mid = {}
        now = time.monotonic()
        throttle_seconds = float(
            getattr(
                getattr(getattr(self, "settings", None), "ws", None),
                "intra_candle_throttle_seconds",
                0.0,
            )
        )
        if now - self._last_intra_scan.get(symbol, 0.0) < throttle_seconds:
            return
        min_move_bps = float(
            getattr(
                getattr(getattr(self, "settings", None), "ws", None),
                "intra_candle_min_move_bps",
                0.0,
            )
        )
        if (
            min_move_bps > 0.0
            and event.bid is not None
            and event.ask is not None
            and event.bid > 0.0
            and event.ask > 0.0
        ):
            mid_now = (event.bid + event.ask) / 2.0
            mid_prev = self._last_intra_mid.get(symbol)
            if mid_prev is not None and mid_prev > 0.0:
                move_bps = abs(mid_now - mid_prev) / mid_prev * 10000.0
                if move_bps < min_move_bps:
                    return
            self._last_intra_mid[symbol] = mid_now
        self._last_intra_scan[symbol] = now

        async def _run() -> None:
            try:
                async with self._shortlist_lock:
                    shortlist = list(self._shortlist)
                item = next((row for row in shortlist if row.symbol == symbol), None)
                if item is None:
                    return

                event_ts = (
                    datetime.fromtimestamp(event.event_ts_ms / 1000.0, tz=UTC)
                    if event.event_ts_ms is not None and event.event_ts_ms > 0
                    else datetime.now(UTC)
                )
                ws_override: dict[str, Any] | None = None
                if (
                    event.bid is not None
                    and event.ask is not None
                    and event.bid > 0.0
                    and event.ask > 0.0
                    and event.ask >= event.bid
                ):
                    mid = (event.bid + event.ask) / 2.0
                    spread_bps = ((event.ask - event.bid) / mid) * 10000.0 if mid > 0.0 else None
                    ws_override = {
                        "bid_price": event.bid,
                        "ask_price": event.ask,
                        "spread_bps": spread_bps,
                    }
                await self._get_cycle_runner().execute_symbol_cycle(
                    symbol=symbol,
                    item=item,
                    interval="bookTicker",
                    trigger="intra_candle",
                    event_ts=event_ts,
                    shortlist_size=len(shortlist),
                    tracking_events=[],
                    ws_enrichments_override=ws_override,
                )

                LOG.debug(
                    "intra_candle scan complete | symbol=%s",
                    symbol,
                )
            except Exception as exc:
                LOG.debug("intra_candle scan failed for %s: %s", symbol, exc)

        task = asyncio.create_task(_run(), name=f"intra_candle:{symbol}")
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    # ------------------------------------------------------------------
    # Shared analysis logic — used by both kline_close and intra_candle paths
    # ------------------------------------------------------------------

    async def _select_and_deliver_for_symbol(
        self,
        symbol: str,
        result: PipelineResult,
    ) -> tuple[list[Signal], list[dict], list[Signal]]:
        """Run selection + cooldown + delivery for a single symbol's pipeline result.

        Returns (candidates, all_rejected, delivered).
        """
        candidates = result.candidates
        rejected: list[dict] = list(result.rejected)
        delivered: list[Signal] = []

        if candidates:
            selected = self._select_and_rank(
                {symbol: candidates},
                max_signals=self.settings.runtime.max_signals_per_cycle,
            )
            if result.funnel:
                result.funnel["selected"] = len(selected)
            prepared_by_tracking_id = (
                {item.tracking_id: result.prepared for item in selected}
                if result.prepared is not None
                else None
            )
            delivered, cooldown_rejected, delivery_status_counts = await self._select_and_deliver(
                selected,
                prepared_by_tracking_id=prepared_by_tracking_id,
            )
            if result.funnel:
                result.funnel["delivered"] = len(delivered)
                result.funnel["delivery_status_counts"] = dict(delivery_status_counts)
            rejected.extend(cooldown_rejected)

        return candidates, rejected, delivered

    # ------------------------------------------------------------------
    # Emergency fallback — full scan when no kline events
    # ------------------------------------------------------------------

    async def _tracking_review_periodic(self) -> None:
        """Review open signal tracking every 5 minutes, independent of kline events.

        Decoupled from the main analysis loop so TP/SL notifications are sent
        promptly even when the bot is idle or WS klines are disabled.
        """
        _interval = 300  # 5 minutes
        while not self._shutdown.is_set():
            await asyncio.sleep(_interval)
            if self._shutdown.is_set():
                break
            try:
                tracking_events = await self.tracker.review_open_signals(dry_run=False)
                if tracking_events:
                    await self._deliver_tracking(tracking_events)
            except Exception as exc:
                LOG.exception("tracking_review_periodic failed: %s", exc)

    async def _emergency_fallback_scan(self) -> None:
        """Run full shortlist scan if no kline events for a long time."""
        fallback_sec = self.settings.runtime.emergency_fallback_seconds
        while not self._shutdown.is_set():
            await asyncio.sleep(fallback_sec)
            if self._shutdown.is_set():
                break

            time_since_event = asyncio.get_running_loop().time() - self._last_kline_event_ts
            if time_since_event < fallback_sec:
                self.telemetry.append_jsonl(
                    "fallback_checks.jsonl",
                    {
                        "ts": datetime.now(UTC).isoformat(),
                        "trigger": "emergency_fallback",
                        "action": "skip",
                        "fallback_seconds": fallback_sec,
                        "time_since_last_kline_seconds": round(time_since_event, 1),
                    },
                )
                continue

            self.telemetry.append_jsonl(
                "fallback_checks.jsonl",
                {
                    "ts": datetime.now(UTC).isoformat(),
                    "trigger": "emergency_fallback",
                    "action": "run",
                    "fallback_seconds": fallback_sec,
                    "time_since_last_kline_seconds": round(time_since_event, 1),
                },
            )
            LOG.info(
                "emergency fallback: no kline events for %.0fs — running full scan",
                time_since_event,
            )
            try:
                await self._run_emergency_cycle()
            except Exception as exc:
                LOG.exception("emergency fallback cycle failed: %s", exc)

    async def _run_emergency_cycle(self) -> dict[str, Any]:
        """Full shortlist analysis — used for emergency fallback."""
        return await self._get_cycle_runner().run_emergency_cycle()

    # ------------------------------------------------------------------
    # Frame fetching & enrichments
    # ------------------------------------------------------------------

    async def _fetch_frames(self, item: UniverseSymbol) -> SymbolFrames | None:
        symbol = item.symbol
        minimums = min_required_bars(
            min_bars_15m=self.settings.filters.min_bars_15m,
            min_bars_1h=self.settings.filters.min_bars_1h,
            min_bars_4h=self.settings.filters.min_bars_4h,
        )

        ws_5m = None
        ws_15m = None
        ws_1h = None
        ws_bid = None
        ws_ask = None
        if self._ws_manager is not None:
            ws_frames = await self._ws_manager.get_symbol_frames(symbol)
            if ws_frames is not None:
                ws_5m = ws_frames.df_5m
                ws_15m = ws_frames.df_15m
                ws_1h = ws_frames.df_1h
                ws_bid = ws_frames.bid_price
                ws_ask = ws_frames.ask_price

        try:
            # 4h is macro-only and must never be a hard blocker for symbol analysis.
            if isinstance(self.client, BinanceFuturesMarketData):
                df_4h = await self.client.fetch_klines_cached(symbol, "4h", limit=240)
                if ws_1h is not None and ws_1h.height >= minimums["1h"]:
                    df_1h = ws_1h
                else:
                    df_1h = await self.client.fetch_klines_cached(symbol, "1h", limit=240)

                if ws_15m is not None and ws_15m.height >= minimums["15m"]:
                    df_15m = ws_15m
                else:
                    df_15m = await self.client.fetch_klines_cached(symbol, "15m", limit=240)
                if ws_5m is not None and ws_5m.height >= minimums["5m"]:
                    df_5m = ws_5m
                else:
                    df_5m = await self.client.fetch_klines_cached(symbol, "5m", limit=240)

                bid, ask = ws_bid, ws_ask
                if bid is None or ask is None:
                    bid, ask = await self.client.fetch_book_ticker(symbol)

                rows_4h = df_4h.height if df_4h is not None else 0
                rows_5m = df_5m.height if df_5m is not None else 0
                rows_1h = df_1h.height if df_1h is not None else 0
                rows_15m = df_15m.height if df_15m is not None else 0
                LOG.info(
                    "%s: frames merged | 15m=%d/%d 1h=%d/%d optional_5m=%d/%d optional_4h=%d/%d",
                    symbol,
                    rows_15m,
                    minimums["15m"],
                    rows_1h,
                    minimums["1h"],
                    rows_5m,
                    minimums["5m"],
                    rows_4h,
                    minimums["4h"],
                )
                if rows_1h < minimums["1h"] or rows_15m < minimums["15m"]:
                    LOG.warning(
                        "%s: insufficient required history | 15m=%d/%d 1h=%d/%d optional_5m=%d/%d optional_4h=%d/%d",
                        symbol,
                        rows_15m,
                        minimums["15m"],
                        rows_1h,
                        minimums["1h"],
                        rows_5m,
                        minimums["5m"],
                        rows_4h,
                        minimums["4h"],
                    )

                return SymbolFrames(
                    symbol=symbol,
                    df_1h=df_1h,
                    df_15m=df_15m,
                    bid_price=bid,
                    ask_price=ask,
                    df_5m=df_5m,
                    df_4h=df_4h,
                )

            LOG.info("%s: fetching frames from REST API (generic client)", symbol)
            frames = await cast(Any, self.client.fetch_symbol_frames(symbol))
            rows_4h = frames.df_4h.height if frames and frames.df_4h is not None else 0
            rows_5m = frames.df_5m.height if frames and frames.df_5m is not None else 0
            rows_1h = frames.df_1h.height if frames and frames.df_1h is not None else 0
            rows_15m = frames.df_15m.height if frames and frames.df_15m is not None else 0
            LOG.info("%s: REST frames fetched | 15m=%d 1h=%d optional_5m=%d optional_4h=%d", symbol, rows_15m, rows_1h, rows_5m, rows_4h)
            if frames and (rows_1h < minimums["1h"] or rows_15m < minimums["15m"]):
                LOG.warning(
                    "%s: REST frames insufficient required history | 15m=%d/%d 1h=%d/%d optional_5m=%d/%d optional_4h=%d/%d",
                    symbol,
                    rows_15m,
                    minimums["15m"],
                    rows_1h,
                    minimums["1h"],
                    rows_5m,
                    minimums["5m"],
                    rows_4h,
                    minimums["4h"],
                )
            return frames
        except (MarketDataUnavailable, Exception) as exc:
            LOG.warning("frame fetch failed for %s: %s", symbol, exc)
            return None

    async def _preload_shortlist_frames(self) -> None:
        """Preload historical klines for the current shortlist.

        Purpose: avoid `prepared symbol is None` caused by missing 1h/15m history.
        This is best-effort and throttled; failures are non-fatal.
        """
        await asyncio.sleep(1.0)  # allow startup + ws bootstrap to settle
        LOG.info("preload frames: starting...")
        if not isinstance(self.client, BinanceFuturesMarketData):
            LOG.info("preload frames: skipped - client is not BinanceFuturesMarketData")
            return
        async with self._shortlist_lock:
            shortlist = list(self._shortlist)
        if not shortlist:
            LOG.info("preload frames: skipped - shortlist is empty")
            return
        LOG.info("preload frames: loading data for %d symbols", len(shortlist))

        batch_size = int(self.settings.runtime.startup_batch_size)
        batch_delay = float(self.settings.runtime.startup_batch_delay_seconds)
        sem = asyncio.Semaphore(int(self.settings.runtime.max_concurrent_rest_requests))

        async def _preload_one(symbol: str) -> None:
            async with sem:
                try:
                    await self.client.fetch_klines_cached(symbol, "5m", limit=240)
                    await self.client.fetch_klines_cached(symbol, "1h", limit=240)
                    await self.client.fetch_klines_cached(symbol, "15m", limit=240)
                    await self.client.fetch_klines_cached(symbol, "4h", limit=240)
                except Exception as exc:
                    LOG.debug("preload frames failed (non-fatal) | symbol=%s err=%s", symbol, exc)

        processed = 0
        for i in range(0, len(shortlist), batch_size):
            batch = shortlist[i : i + batch_size]
            await asyncio.gather(*[_preload_one(item.symbol) for item in batch], return_exceptions=True)
            processed += len(batch)
            if i + batch_size < len(shortlist):
                await asyncio.sleep(batch_delay)
        LOG.info("preload frames completed | symbols=%d batches=%d", processed, (len(shortlist) + batch_size - 1) // batch_size)

    def _ws_cache_enrichments(self, symbol: str) -> dict[str, Any]:
        """Collect enrichments from in-memory caches — zero I/O."""
        enrichments: dict[str, Any] = {}
        context_ages: list[float] = []

        if self._ws_manager is not None:
            try:
                ticker = self._ws_manager.get_ticker_snapshot(symbol)
                ticker_age = self._ws_manager.get_ticker_age_seconds(symbol)
                if ticker:
                    ticker_price = float(ticker.get("last_price") or 0.0)
                    if ticker_price > 0:
                        enrichments["ticker_price"] = ticker_price
                    if ticker_age is not None:
                        enrichments["ticker_price_age_seconds"] = ticker_age
                        context_ages.append(ticker_age)
            except Exception:
                pass
            try:
                mp = self._ws_manager.get_mark_price_snapshot(symbol)
                mark_price_age = self._ws_manager.get_mark_price_age_seconds(symbol)
                if mp:
                    # Cache stores keys "mark_price" and "funding_rate" (set in _handle_mark_price)
                    mark_price = float(mp.get("mark_price") or 0.0)
                    funding_rate = mp.get("funding_rate")
                    if mark_price > 0:
                        enrichments["mark_price"] = mark_price
                    if funding_rate is not None:
                        enrichments["funding_rate"] = float(funding_rate)
                    if mark_price_age is not None:
                        enrichments["mark_price_age_seconds"] = mark_price_age
                        context_ages.append(mark_price_age)
            except Exception:
                pass
            try:
                book_age = self._ws_manager.get_book_ticker_age_seconds(symbol)
                if book_age is not None:
                    enrichments["book_ticker_age_seconds"] = book_age
                    context_ages.append(book_age)
            except Exception:
                pass
            try:
                liq = self._ws_manager.get_liquidation_sentiment(symbol, window_seconds=300)
                if liq is not None:
                    enrichments["liquidation_score"] = liq
            except Exception:
                pass

        if isinstance(self.client, BinanceFuturesMarketData):
            oi_chg = self.client.get_cached_oi_change(symbol)
            if oi_chg is not None:
                enrichments["oi_change_pct"] = oi_chg
            oi_slope_5m = self.client.get_cached_oi_change(symbol, period="5m")
            if oi_slope_5m is not None:
                enrichments["oi_slope_5m"] = oi_slope_5m
            ls = self.client.get_cached_ls_ratio(symbol)
            if ls is not None:
                enrichments["ls_ratio"] = ls
                enrichments["top_trader_position_ratio"] = ls
            ls_5m = self.client.get_cached_ls_ratio(symbol, period="5m")
            if ls_5m is not None:
                enrichments["top_trader_position_ratio"] = ls_5m
            taker = self.client.get_cached_taker_ratio(symbol)
            if taker is not None:
                enrichments["taker_ratio"] = taker
            taker_5m = self.client.get_cached_taker_ratio(symbol, period="5m")
            if taker_5m is not None:
                enrichments["agg_trade_delta_30s"] = taker_5m - 1.0
                if taker is not None:
                    enrichments["aggression_shift"] = taker_5m - taker
            funding_trend = self.client.get_cached_funding_trend(symbol)
            if funding_trend is not None:
                enrichments["funding_trend"] = funding_trend
            # Try REST cache first, then WebSocket fallback
            basis = self.client.get_cached_basis(symbol)
            if basis is not None:
                enrichments["basis_pct"] = basis
            
            # Check for mark price from WebSocket to update basis cache
            mp_data = self._ws_manager.get_mark_price_snapshot(symbol) if self._ws_manager else None
            ticker = self._ws_manager.get_ticker_snapshot(symbol) if self._ws_manager else None
            if mp_data and mp_data.get("mark_price"):
                mark_price = float(mp_data.get("mark_price", 0))
                # Use ticker last_price as index_price approximation (both are spot-based)
                index_price = None
                if ticker:
                    ticker_price = float(ticker.get("last_price") or 0)
                    if ticker_price > 0:
                        index_price = ticker_price
                # Try to update basis cache from WebSocket (zero I/O)
                ws_basis_stats = self.client.update_basis_from_websocket(
                    symbol, mark_price, index_price=index_price, period="5m"
                )
                if ws_basis_stats:
                    # Use WebSocket-calculated spread directly if available
                    if ws_basis_stats.get("mark_index_spread_bps") is not None:
                        enrichments["mark_index_spread_bps"] = ws_basis_stats["mark_index_spread_bps"]
                    if ws_basis_stats.get("latest_basis_pct") is not None:
                        enrichments["basis_pct"] = ws_basis_stats["latest_basis_pct"]
            
            global_ls = self.client.get_cached_global_ls_ratio(symbol)
            if global_ls is not None:
                enrichments["global_ls_ratio"] = global_ls
            global_ls_5m = self.client.get_cached_global_ls_ratio(symbol, period="5m")
            if global_ls_5m is not None:
                enrichments["global_ls_ratio"] = global_ls_5m
            top_ratio = enrichments.get("top_trader_position_ratio")
            global_ratio = enrichments.get("global_ls_ratio")
            if top_ratio is not None and global_ratio is not None:
                enrichments["top_vs_global_ls_gap"] = float(top_ratio) - float(global_ratio)
            basis_stats_5m = self.client.get_cached_basis_stats(symbol, period="5m")
            if basis_stats_5m is not None:
                enrichments["mark_index_spread_bps"] = basis_stats_5m.get("mark_index_spread_bps")
                enrichments["premium_slope_5m"] = basis_stats_5m.get("premium_slope_5m")
                enrichments["premium_zscore_5m"] = basis_stats_5m.get("premium_zscore_5m")
            
            # Add order book based metrics from WebSocket
            if self._ws_manager is not None:
                depth_imb = self._ws_manager.get_depth_imbalance(symbol)
                if depth_imb is not None:
                    enrichments["depth_imbalance"] = depth_imb
                micro_bias = self._ws_manager.get_microprice_bias(symbol)
                if micro_bias is not None:
                    enrichments["microprice_bias"] = micro_bias

        if context_ages:
            enrichments["context_snapshot_age_seconds"] = max(context_ages)
        enrichments.setdefault("data_source_mix", "futures_only")
        return enrichments

    def _refresh_universe_symbol_from_ws(self, item: UniverseSymbol) -> UniverseSymbol:
        if self._ws_manager is None:
            return item
        ticker = self._ws_manager.get_ticker_snapshot(item.symbol)
        ticker_age = self._ws_manager.get_ticker_age_seconds(item.symbol)
        if (
            not ticker
            or ticker_age is None
            or ticker_age > self.settings.ws.market_ticker_freshness_seconds
        ):
            return item

        next_last_price = item.last_price
        next_quote_volume = item.quote_volume
        next_price_change_pct = item.price_change_pct
        try:
            ticker_last_price = float(ticker.get("last_price") or 0.0)
        except (TypeError, ValueError):
            return item
        if ticker_last_price > 0:
            next_last_price = ticker_last_price
        try:
            ticker_quote_volume = float(ticker.get("quote_volume") or 0.0)
            if ticker_quote_volume > 0:
                next_quote_volume = ticker_quote_volume
        except (TypeError, ValueError):
            pass
        try:
            next_price_change_pct = float(
                ticker.get("price_change_percent") or item.price_change_pct
            )
        except (TypeError, ValueError):
            pass

        if (
            next_last_price == item.last_price
            and next_quote_volume == item.quote_volume
            and next_price_change_pct == item.price_change_pct
        ):
            return item
        return replace(
            item,
            last_price=next_last_price,
            quote_volume=next_quote_volume,
            price_change_pct=next_price_change_pct,
        )

    async def _ws_enrich(self, result: PipelineResult) -> None:
        """Post-pipeline REST enrichment (OI, L/S ratio)."""
        if result.prepared is None:
            return
        p = result.prepared
        try:
            p.oi_current = await self.client.fetch_open_interest(p.universe.symbol)
            p.oi_change_pct = await self.client.fetch_open_interest_change(p.universe.symbol, period="1h")
            p.oi_slope_5m = await self.client.fetch_open_interest_change(p.universe.symbol, period="5m")
        except Exception:
            pass
        try:
            p.ls_ratio = await self.client.fetch_long_short_ratio(p.universe.symbol, period="1h")
            p.top_trader_position_ratio = await self.client.fetch_long_short_ratio(p.universe.symbol, period="5m")
        except Exception:
            pass
        try:
            p.taker_ratio = await self.client.fetch_taker_ratio(p.universe.symbol, period="1h")
            taker_5m = await self.client.fetch_taker_ratio(p.universe.symbol, period="5m")
            if taker_5m is not None:
                p.agg_trade_delta_30s = taker_5m - 1.0
                if p.taker_ratio is not None:
                    p.aggression_shift = taker_5m - p.taker_ratio
        except Exception:
            pass
        try:
            p.global_ls_ratio = await self.client.fetch_global_ls_ratio(p.universe.symbol, period="5m")
            if p.top_trader_position_ratio is not None and p.global_ls_ratio is not None:
                p.top_vs_global_ls_gap = p.top_trader_position_ratio - p.global_ls_ratio
        except Exception:
            pass
        try:
            p.funding_trend = self.client.get_cached_funding_trend(p.universe.symbol)
        except Exception:
            pass
        try:
            p.basis_pct = await self.client.fetch_basis(p.universe.symbol, period="1h")
            await self.client.fetch_basis(p.universe.symbol, period="5m")
            basis_stats_5m = self.client.get_cached_basis_stats(p.universe.symbol, period="5m")
            if basis_stats_5m is not None:
                p.mark_index_spread_bps = cast(float | None, basis_stats_5m.get("mark_index_spread_bps"))
                p.premium_slope_5m = cast(float | None, basis_stats_5m.get("premium_slope_5m"))
                p.premium_zscore_5m = cast(float | None, basis_stats_5m.get("premium_zscore_5m"))
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Background OI + L/S refresh
    # ------------------------------------------------------------------

    async def _oi_refresh_periodic(self) -> None:
        """Pre-warm OI and L/S ratio caches every 15 minutes."""
        await asyncio.sleep(30)  # stagger after shortlist populates
        while not self._shutdown.is_set():
            async with self._shortlist_lock:
                shortlist = list(self._shortlist)

            if shortlist and isinstance(self.client, BinanceFuturesMarketData):
                # Staggered batch processing to prevent REST API flood
                batch_size = self.settings.runtime.startup_batch_size
                batch_delay = self.settings.runtime.startup_batch_delay_seconds
                rest_concurrency = max(
                    1,
                    int(self.settings.runtime.max_concurrent_rest_requests),
                )
                sem = asyncio.Semaphore(rest_concurrency)

                async def _fetch_one(symbol: str, limiter: asyncio.Semaphore) -> None:
                    async with limiter:
                        try:
                            await self.client.fetch_open_interest_change(symbol, period="1h")
                        except Exception:
                            pass
                        try:
                            await self.client.fetch_open_interest_change(symbol, period="5m")
                        except Exception:
                            pass
                        try:
                            await self.client.fetch_long_short_ratio(symbol, period="1h")
                        except Exception:
                            pass
                        try:
                            await self.client.fetch_long_short_ratio(symbol, period="5m")
                        except Exception:
                            pass
                        try:
                            await self.client.fetch_taker_ratio(symbol, period="1h")
                        except Exception:
                            pass
                        try:
                            await self.client.fetch_taker_ratio(symbol, period="5m")
                        except Exception:
                            pass
                        try:
                            await self.client.fetch_global_ls_ratio(symbol, period="1h")
                        except Exception:
                            pass
                        try:
                            await self.client.fetch_global_ls_ratio(symbol, period="5m")
                        except Exception:
                            pass
                        try:
                            await self.client.fetch_funding_rate_history(symbol)
                        except Exception:
                            pass
                        try:
                            await self.client.fetch_basis(symbol, period="1h")
                        except Exception:
                            pass
                        try:
                            await self.client.fetch_basis(symbol, period="5m")
                        except Exception:
                            pass

                # Process in batches with delay between batches
                processed = 0
                for i in range(0, len(shortlist), batch_size):
                    batch = shortlist[i:i + batch_size]
                    await asyncio.gather(
                        *[_fetch_one(item.symbol, sem) for item in batch],
                        return_exceptions=True,
                    )
                    processed += len(batch)
                    if i + batch_size < len(shortlist):
                        await asyncio.sleep(batch_delay)

                LOG.info(
                    "oi/ls cache refreshed | symbols=%d batches=%d rest_concurrency=%d",
                    processed,
                    (len(shortlist) + batch_size - 1) // batch_size,
                    rest_concurrency,
                )
                await self._update_memory_market_context(shortlist)

            try:
                await asyncio.wait_for(self._shutdown.wait(), timeout=900)
            except asyncio.TimeoutError:
                continue

    async def _market_regime_periodic(self) -> None:
        """Update market regime every 60 seconds."""
        await asyncio.sleep(10)  # Initial delay to let caches warm up
        while not self._shutdown.is_set():
            try:
                async with self._shortlist_lock:
                    shortlist = list(self._shortlist)
                if shortlist:
                    await self._update_memory_market_context(shortlist)
                    LOG.debug("market regime periodic update completed")
            except Exception as exc:
                LOG.debug("market regime periodic update failed: %s", exc)
            try:
                await asyncio.wait_for(self._shutdown.wait(), timeout=60)
            except asyncio.TimeoutError:
                continue

    async def _public_intelligence_periodic(self) -> None:
        await asyncio.sleep(45)
        while not self._shutdown.is_set():
            try:
                async with self._shortlist_lock:
                    shortlist = list(self._shortlist)
                if shortlist and self.intelligence is not None:
                    snapshot = await self.intelligence.collect([item.symbol for item in shortlist])
                    await self._update_memory_market_context(shortlist)
                    await self._apply_public_guardrails(snapshot)
                    LOG.info(
                        "public intelligence updated | barrier_long=%s barrier_short=%s macro=%s",
                        cast(dict[str, Any], snapshot.get("barrier") or {}).get("long_barrier_triggered"),
                        cast(dict[str, Any], snapshot.get("barrier") or {}).get("short_barrier_triggered"),
                        cast(dict[str, Any], snapshot.get("macro") or {}).get("risk_mode"),
                    )
            except Exception as exc:
                LOG.warning("public intelligence update failed: %s", exc, exc_info=True)
            try:
                await asyncio.wait_for(
                    self._shutdown.wait(),
                    timeout=max(60, int(self.settings.intelligence.refresh_interval_seconds)),
                )
            except asyncio.TimeoutError:
                continue

    async def _apply_public_guardrails(self, snapshot: dict[str, Any]) -> None:
        if self.intelligence is None:
            return
        open_rows = await self._modern_repo.get_active_signals(include_closed=False)
        if not open_rows:
            return

        barrier = cast(dict[str, Any], snapshot.get("barrier") or {})
        barrier_events: list[SignalTrackingEvent] = []
        closed_tracking_ids: set[str] = set()

        if bool(barrier.get("long_barrier_triggered")):
            long_ids = [
                str(row["tracking_id"])
                for row in open_rows
                if str(row.get("direction") or "").lower() == "long"
            ]
            if long_ids:
                note = (
                    f"tracked_signal_hard_barrier_long {barrier.get('strongest_symbol')} "
                    f"{barrier.get('strongest_move_pct')}pct/{barrier.get('window_minutes')}m"
                )
                barrier_events.extend(
                    await self.tracker.force_close_tracking_ids(
                        long_ids,
                        reason="emergency_exit",
                        occurred_at=datetime.now(UTC),
                        note=note,
                    )
                )
                closed_tracking_ids.update(long_ids)
                self.telemetry.append_jsonl(
                    "risk_actions.jsonl",
                    {
                        "ts": datetime.now(UTC).isoformat(),
                        "action": "emergency_exit",
                        "lifecycle_action": "analytical_hard_barrier_exit",
                        "tracking_semantics": "tracked_signal_lifecycle",
                        "runtime_mode": self.settings.intelligence.runtime_mode,
                        "source_policy": self.settings.intelligence.source_policy,
                        "exchange_execution": False,
                        "scope": "long",
                        "tracking_ids": long_ids,
                        "tracked_signal_ids": long_ids,
                        "barrier": barrier,
                    },
                )

        if bool(barrier.get("short_barrier_triggered")):
            short_ids = [
                str(row["tracking_id"])
                for row in open_rows
                if str(row.get("direction") or "").lower() == "short"
            ]
            if short_ids:
                note = (
                    f"tracked_signal_hard_barrier_short {barrier.get('strongest_symbol')} "
                    f"{barrier.get('strongest_move_pct')}pct/{barrier.get('window_minutes')}m"
                )
                barrier_events.extend(
                    await self.tracker.force_close_tracking_ids(
                        short_ids,
                        reason="emergency_exit",
                        occurred_at=datetime.now(UTC),
                        note=note,
                    )
                )
                closed_tracking_ids.update(short_ids)
                self.telemetry.append_jsonl(
                    "risk_actions.jsonl",
                    {
                        "ts": datetime.now(UTC).isoformat(),
                        "action": "emergency_exit",
                        "lifecycle_action": "analytical_hard_barrier_exit",
                        "tracking_semantics": "tracked_signal_lifecycle",
                        "runtime_mode": self.settings.intelligence.runtime_mode,
                        "source_policy": self.settings.intelligence.source_policy,
                        "exchange_execution": False,
                        "scope": "short",
                        "tracking_ids": short_ids,
                        "tracked_signal_ids": short_ids,
                        "barrier": barrier,
                    },
                )

        smart_exit_events: list[SignalTrackingEvent] = []
        if self.settings.intelligence.smart_exit_enabled:
            for row in open_rows:
                tracking_id = str(row.get("tracking_id") or "")
                if not tracking_id or tracking_id in closed_tracking_ids:
                    continue
                symbol = str(row.get("symbol") or "")
                direction = str(row.get("direction") or "")
                smart_exit = await self.intelligence.evaluate_smart_exit(symbol, direction)
                self.telemetry.append_jsonl(
                    "smart_exit_traces.jsonl",
                    {
                        "ts": datetime.now(UTC).isoformat(),
                        "tracking_id": tracking_id,
                        "symbol": symbol,
                        "direction": direction,
                        "tracking_semantics": "tracked_signal_lifecycle",
                        "exchange_execution": False,
                        **smart_exit,
                    },
                )
                if not bool(smart_exit.get("triggered")):
                    continue
                smart_exit_events.extend(
                    await self.tracker.force_close_tracking_ids(
                        [tracking_id],
                        reason="smart_exit",
                        occurred_at=datetime.now(UTC),
                        note=";".join(cast(list[str], smart_exit.get("reasons") or [])[:6]),
                    )
                )
                self.telemetry.append_jsonl(
                    "risk_actions.jsonl",
                    {
                        "ts": datetime.now(UTC).isoformat(),
                        "action": "smart_exit",
                        "lifecycle_action": "analytical_smart_exit",
                        "tracking_semantics": "tracked_signal_lifecycle",
                        "runtime_mode": self.settings.intelligence.runtime_mode,
                        "source_policy": self.settings.intelligence.source_policy,
                        "exchange_execution": False,
                        "tracking_id": tracking_id,
                        "symbol": symbol,
                        "direction": direction,
                        "confidence": smart_exit.get("confidence"),
                        "reasons": smart_exit.get("reasons"),
                    },
                )

        combined_events = barrier_events + smart_exit_events
        if combined_events:
            await self._deliver_tracking(combined_events)

    async def _update_memory_market_context(self, shortlist: list[UniverseSymbol]) -> None:
        try:
            if not isinstance(self.client, BinanceFuturesMarketData):
                return
            high_funding: list[str] = []
            low_funding: list[str] = []
            extreme_threshold = 0.0005
            funding_rates: dict[str, float] = {}

            for item in shortlist:
                cached = self.client._funding_rate_cache.get(item.symbol)
                if cached is None:
                    continue
                _, fr = cached
                funding_rates[item.symbol] = fr
                if fr >= extreme_threshold:
                    high_funding.append(item.symbol)
                elif fr <= -extreme_threshold:
                    low_funding.append(item.symbol)

            btc_bias = "neutral"
            eth_bias = "neutral"
            if self._ws_manager is not None:
                for sym, bias_attr in [("BTCUSDT", "btc_bias"), ("ETHUSDT", "eth_bias")]:
                    bias = self._compute_price_bias(sym)
                    if bias_attr == "btc_bias":
                        btc_bias = bias
                    else:
                        eth_bias = bias

            benchmark_context: dict[str, dict[str, Any]] = {}
            for sym, bias in [("BTCUSDT", btc_bias), ("ETHUSDT", eth_bias)]:
                payload: dict[str, Any] = {"bias": bias}
                payload["oi_change_pct"] = self.client.get_cached_oi_change(sym, period="1h")
                payload["basis_pct"] = self.client.get_cached_basis(sym, period="1h")
                basis_stats = self.client.get_cached_basis_stats(sym, period="5m")
                if basis_stats is not None:
                    payload["premium_slope_5m"] = basis_stats.get("premium_slope_5m")
                    payload["premium_zscore_5m"] = basis_stats.get("premium_zscore_5m")
                benchmark_context[sym] = payload

            # Update market regime analyzer with full ticker data
            ticker_data: list[dict[str, Any]] = []
            all_tickers = await self.client.fetch_ticker_24h()
            ticker_dict = {t.get("symbol"): t for t in all_tickers if isinstance(t, dict)}
            for item in shortlist:
                ticker = ticker_dict.get(item.symbol)
                if ticker:
                    ticker_data.append(ticker)

            # This will cache the result for 60 seconds
            regime_result = self.market_regime.analyze(
                ticker_data,
                funding_rates,
                benchmark_context=benchmark_context,
            )
            intelligence_snapshot = self.intelligence.latest_snapshot if self.intelligence is not None else None
            macro_risk_mode = (
                "disabled_binance_only"
                if self.settings.intelligence.source_policy == "binance_only"
                else "unknown"
            )
            if intelligence_snapshot:
                macro_snapshot = cast(dict[str, Any], intelligence_snapshot.get("macro") or {})
                macro_risk_mode = str(
                    macro_snapshot.get("risk_mode") or macro_risk_mode
                )
            await self._modern_repo.update_market_context(
                btc_bias,
                eth_bias,
                high_funding,
                low_funding,
                market_regime=regime_result.regime,
                market_regime_confirmed=True,
                macro_risk_mode=macro_risk_mode,
                intelligence_snapshot=intelligence_snapshot,
            )
            LOG.info(
                "market regime updated | regime=%s strength=%.2f btc=%s eth=%s",
                regime_result.regime,
                regime_result.strength,
                regime_result.btc_bias,
                regime_result.eth_bias,
            )

        except Exception as exc:
            LOG.warning("memory market context update failed: %s", exc, exc_info=True)

    def _compute_price_bias(self, symbol: str) -> str:
        """Compute price trend bias for *symbol* using 4h kline price change.

        Priority:
        1. WS 4h kline cache (last 2 closed candles → close-to-close pct)
        2. WS 24h ticker price_change_percent
        Falls back to "neutral" if no data is available yet (cold cache at startup).
        """
        if self._ws_manager is None:
            return "neutral"
        # 1. 4h kline cache (most accurate — actual price movement, not basis)
        klines = self._ws_manager.get_kline_cache(symbol, "4h")
        if klines and len(klines) >= 2:
            try:
                c1 = float(klines[-2]["close"])
                c2 = float(klines[-1]["close"])
                if c1 > 0 and c2 > 0:
                    pct = (c2 - c1) / c1
                    return "uptrend" if pct > 0.008 else ("downtrend" if pct < -0.008 else "neutral")
            except (KeyError, TypeError, ValueError):
                pass
        # 2. 24h ticker fallback (available before 4h kline cache warms up)
        ticker = self._ws_manager.get_ticker_snapshot(symbol)
        if ticker:
            try:
                pct_24h = float(ticker.get("price_change_percent") or 0.0) / 100.0
                return "uptrend" if pct_24h > 0.02 else ("downtrend" if pct_24h < -0.02 else "neutral")
            except (TypeError, ValueError):
                pass
        return "neutral"

    # ------------------------------------------------------------------
    # Shortlist management
    # ------------------------------------------------------------------

    async def _fetch_symbols_with_retry(self, max_retries: int = 1) -> list[Any]:
        """Fetch exchange symbols with timeout and retry logic."""
        return await self._get_shortlist_service().fetch_symbols_with_retry(max_retries=max_retries)

    def _extract_symbol_assets(self, symbol: str) -> tuple[str | None, str | None]:
        return self._get_shortlist_service().extract_symbol_assets(symbol)

    def _build_pinned_shortlist(self) -> list[UniverseSymbol]:
        return self._get_shortlist_service().build_pinned_shortlist()

    async def _build_live_shortlist(self) -> tuple[list[UniverseSymbol], dict[str, int]]:
        return await self._get_shortlist_service().build_live_shortlist()

    async def _sync_ws_tracked_symbols(self) -> None:
        if self._ws_manager is None:
            return
        try:
            rows = await self._modern_repo.get_active_signals()
            tracked_symbols = sorted(
                {
                    str(row.get("symbol", "")).strip().upper()
                    for row in rows
                    if str(row.get("symbol", "")).strip()
                }
            )
            await self._ws_manager.set_tracked_symbols(tracked_symbols)
        except Exception as exc:
            LOG.debug("tracked-symbol sync failed (non-fatal): %s", exc)

    async def _do_refresh_shortlist(self) -> list[UniverseSymbol]:
        return await self._get_shortlist_service().do_refresh_shortlist()

    async def _background_fetch_symbols(self) -> None:
        """Background task to fetch exchange symbols without blocking startup."""
        try:
            LOG.info("background fetch: attempting to get exchange symbols...")
            symbol_meta_list = await asyncio.wait_for(
                self.client.fetch_exchange_symbols(),
                timeout=30.0
            )
            self._symbol_meta_by_symbol = {
                str(getattr(row, "symbol", "")).strip().upper(): row for row in symbol_meta_list
            }
            LOG.info("background fetch: got %d exchange symbols", len(symbol_meta_list))
            # Could update shortlist here if needed, but pinned symbols are sufficient
        except Exception as exc:
            LOG.debug("background fetch: failed to get exchange symbols: %s", exc)

    async def _refresh_shortlist_periodic(self) -> None:
        await self._get_shortlist_service().refresh_shortlist_periodic()

    # ------------------------------------------------------------------
    # Delivery & tracking
    # ------------------------------------------------------------------

    async def _select_and_deliver(
        self,
        signals: list[Signal],
        *,
        prepared_by_tracking_id: dict[str, PreparedSymbol] | None = None,
    ) -> tuple[list[Signal], list[dict[str, Any]], Counter[str]]:
        if not signals:
            return [], [], Counter()

        ready_to_send: list[Signal] = []
        rejected_rows: list[dict[str, Any]] = []

        for signal in signals:
            # Check blacklist via modern repository
            is_blacklisted = await self._modern_repo.is_symbol_blacklisted(
                signal.symbol,
                max_sl_streak=self.settings.intelligence.max_consecutive_stop_losses,
                pause_hours=self.settings.intelligence.stop_loss_pause_hours,
            )
            if is_blacklisted:
                sl_streak = await self._modern_repo.get_consecutive_sl(signal.symbol)
                rejected_rows.append({
                    "ts": datetime.now(UTC).isoformat(),
                    "symbol": signal.symbol,
                    "setup_id": signal.setup_id,
                    "direction": signal.direction,
                    "stage": "memory",
                    "reason": "consecutive_sl_blacklist",
                    "tracking_semantics": "tracked_signal_lifecycle",
                    "runtime_mode": self.settings.intelligence.runtime_mode,
                    "consecutive_sl": sl_streak,
                    "max_consecutive_stop_losses": self.settings.intelligence.max_consecutive_stop_losses,
                    "pause_hours": self.settings.intelligence.stop_loss_pause_hours,
                })
                LOG.warning(
                    "tracked signal paused after consecutive losses | symbol=%s consecutive_sl=%d pause_hours=%d",
                    signal.symbol,
                    sl_streak,
                    self.settings.intelligence.stop_loss_pause_hours,
                )
                continue

            # Check existing active signals via modern repository
            active_signals = await self._modern_repo.get_active_signals(symbol=signal.symbol)
            existing = next(
                (r for r in active_signals if r.get("symbol") == signal.symbol and r.get("status") in ("pending", "active")),
                None,
            )
            if existing is not None:
                score_raw = existing.get("score") if isinstance(existing, dict) else getattr(existing, "score", None)
                if score_raw is not None and signal.score >= float(score_raw or 0.0) + 0.10:
                    closed = await self._close_superseded_signal(signal)
                    if closed:
                        await self._deliver_tracking(closed)
                    ready_to_send.append(signal)
                else:
                    rejected_rows.append({
                        "ts": datetime.now(UTC).isoformat(),
                        "symbol": signal.symbol,
                        "setup_id": signal.setup_id,
                        "direction": signal.direction,
                        "stage": "tracking",
                        "reason": "symbol_has_open_signal",
                        "existing_tracking_ref": existing.get("tracking_ref"),
                        "existing_direction": existing.get("direction"),
                        "existing_status": existing.get("status"),
                    })
                continue

            # Check cooldown via modern repository
            cooldown_key = f"{signal.setup_id}:{signal.symbol}"
            is_cooldown_active = await self._modern_repo.is_cooldown_active(
                cooldown_key, self.settings.filters.cooldown_minutes
            )
            if not is_cooldown_active:
                ready_to_send.append(signal)
                continue

            closed = await self._close_superseded_signal(signal)
            if closed:
                await self._deliver_tracking(closed)
                ready_to_send.append(signal)
            else:
                rejected_rows.append({
                    "ts": datetime.now(UTC).isoformat(),
                    "symbol": signal.symbol,
                    "setup_id": signal.setup_id,
                    "direction": signal.direction,
                    "stage": "cooldown",
                    "reason": "cooldown_active",
                })

        delivered: list[Signal] = []
        delivery_status_counts: Counter[str] = Counter()

        market_ctx = await self._modern_repo.get_market_context()
        btc_bias = market_ctx.get("btc_bias", "neutral")
        eth_bias = market_ctx.get("eth_bias", "neutral")

        for signal in ready_to_send:
            ok, results = await self._wait_noncritical(
                label=f"deliver {signal.symbol}/{signal.setup_id}",
                timeout=self._delivery_timeout_seconds,
                operation=self.delivery.deliver([signal], dry_run=False, btc_bias=btc_bias),
            )
            if not ok or not results:
                continue

            for item in results:
                self.telemetry.append_jsonl("delivery.jsonl", {
                    "ts": datetime.now(UTC).isoformat(),
                    "symbol": item.signal.symbol,
                    "setup_id": item.signal.setup_id,
                    "direction": item.signal.direction,
                    "tracking_id": item.signal.tracking_id,
                    "delivery_status": item.status,
                    "reason": item.reason,
                    "message_id": item.message_id,
                })
                delivery_status_counts[item.status] += 1
                if item.status != "sent":
                    continue

                delivered.append(item.signal)
                prepared = (
                    prepared_by_tracking_id.get(item.signal.tracking_id)
                    if prepared_by_tracking_id is not None
                    else None
                )
                self.tracker.set_signal_features(
                    item.signal.tracking_id,
                    extract_features_from_signal(
                        item.signal,
                        prepared_data=build_prepared_feature_snapshot(prepared),
                    ),
                )
                cooldown_key = f"{item.signal.setup_id}:{item.signal.symbol}"
                await self._modern_repo.set_cooldown(cooldown_key, datetime.now(UTC), item.signal.setup_id, item.signal.symbol, "signal")
                await self._wait_noncritical(
                    label=f"arm {item.signal.symbol}/{item.signal.setup_id}",
                    timeout=self._noncritical_timeout_seconds,
                    operation=self.tracker.arm_signals_with_messages(
                        [item.signal],
                        dry_run=False,
                        message_ids={item.signal.tracking_id: item.message_id},
                    ),
                )
                # Fire analytics companion message asynchronously (non-blocking)
                asyncio.create_task(
                    self.delivery.send_analytics_companion(
                        item.signal, btc_bias=btc_bias, eth_bias=eth_bias
                    ),
                    name=f"analytics:{item.signal.symbol}",
                )
                LOG.info(
                    "signal sent | symbol=%s setup=%s dir=%s score=%.3f rr=%.2f "
                    "oi_chg=%s ls_ratio=%s",
                    item.signal.symbol, item.signal.setup_id, item.signal.direction,
                    item.signal.score, item.signal.risk_reward,
                    f"{item.signal.oi_change_pct:.3f}" if item.signal.oi_change_pct is not None else "N/A",
                    f"{getattr(item.signal, 'ls_ratio', None):.2f}" if getattr(item.signal, "ls_ratio", None) is not None else "N/A",
                )

        try:
            await self.alerts.on_confirmed_signals(delivered, observed_at=datetime.now(UTC))
        except Exception as exc:
            LOG.debug("alerts.on_confirmed_signals failed: %s", exc)
        if delivered:
            await self._sync_ws_tracked_symbols()

        return delivered, rejected_rows, delivery_status_counts

    async def _close_superseded_signal(self, new_signal: Signal) -> list[SignalTrackingEvent] | None:
        try:
            return await self.tracker.supersede_open_signal(new_signal, dry_run=False)
        except Exception as exc:
            LOG.debug("supersede failed for %s: %s", new_signal.symbol, exc)
            return None

    async def _deliver_tracking(self, events: list[SignalTrackingEvent]) -> None:
        outcome_map = {
            "tp1_hit": "tp1",
            "tp2_hit": "tp2",
            "stop_loss": "loss",
            "expired": "expired",
            "smart_exit": "smart_exit",
            "emergency_exit": "emergency_exit",
            "ambiguous_exit": "ambiguous_exit",
            "superseded": "superseded",
        }
        for event in events:
            outcome = outcome_map.get(event.event_type)
            if outcome:
                tracked = event.tracked
                regime = getattr(tracked, "regime_4h_confirmed", None) or "neutral"
                await self._modern_repo.record_symbol_outcome(
                    tracked.symbol, tracked.setup_id, tracked.direction, regime, outcome,
                )
        await self._sync_ws_tracked_symbols()
        await self._wait_noncritical(
            label="tracking delivery",
            timeout=self._delivery_timeout_seconds,
            operation=self.delivery.deliver_tracking_updates(events, dry_run=False),
        )

    # ------------------------------------------------------------------
    # Heartbeat & health telemetry
    # ------------------------------------------------------------------

    async def _heartbeat_periodic(self) -> None:
        while not self._shutdown.is_set():
            await asyncio.sleep(300)
            if self._shutdown.is_set():
                break
            async with self._shortlist_lock:
                sl_size = len(self._shortlist)
            active_sigs = await self._modern_repo.get_active_signals()
            open_signals = len(active_sigs)
            ws_lag = 0
            ws_age = 0
            if self._ws_manager is not None:
                ws_lag = self._ws_manager._get_current_latency_ms() or 0
                ws_age = self._ws_manager._last_message_age_seconds() or 0
            mem_summary = await self._modern_repo.summary()
            blacklisted = mem_summary.get("blacklisted_symbols", [])
            market_ctx = await self._modern_repo.get_market_context()

            # Get market regime info if available
            regime_info = "n/a"
            if self.market_regime._last_result is not None:
                r = self.market_regime._last_result
                regime_info = f"{r.regime}:{r.strength:.1f}"

            LOG.info(
                "heartbeat | shortlist=%d open_signals=%d ws_lag_ms=%d ws_msg_age_s=%d "
                "market=%s btc_bias=%s memory_blacklist=%s",
                sl_size, open_signals, ws_lag, ws_age,
                regime_info,
                market_ctx.get("btc_bias", "neutral"),
                blacklisted if blacklisted else "none",
            )

            # Update Prometheus metrics
            if self.metrics._enabled:
                self.metrics.update_bot_state(sl_size, open_signals, len(blacklisted))
                self.metrics.record_ws_latency(ws_lag)
                self.metrics.record_ws_message_age(ws_age)
                if self._ws_manager is not None:
                    self.metrics.update_ws_streams(len(self._ws_manager._symbols))
                if self.market_regime._last_result is not None:
                    r = self.market_regime._last_result
                    self.metrics.update_market_regime(
                        r.regime, r.strength, r.altcoin_season_index
                    )

    async def _health_telemetry_periodic(self) -> None:
        while not self._shutdown.is_set():
            await asyncio.sleep(60)
            if self._shutdown.is_set():
                break
            row: dict[str, Any] = {
                "ts": datetime.now(UTC).isoformat(),
                "prepare_error_count": self._prepare_error_count,
            }
            if self._last_prepare_error:
                row["prepare_error_stage"] = self._last_prepare_error.get("stage")
                row["prepare_error_exception_type"] = self._last_prepare_error.get("exception_type")
            if self._ws_manager is not None:
                ws_snapshot = self._ws_manager.state_snapshot()
                row.update(ws_snapshot if isinstance(ws_snapshot, dict) else {})
            rest_snapshot_func = getattr(self.client, "state_snapshot", None)
            if callable(rest_snapshot_func):
                rest_snapshot = rest_snapshot_func()
                row.update(rest_snapshot if isinstance(rest_snapshot, dict) else {})
            self.telemetry.append_jsonl("health.jsonl", row)

    # ------------------------------------------------------------------
    # Misc
    # ------------------------------------------------------------------

    def _preflight_storage_check(self) -> None:
        self.settings.data_dir.mkdir(parents=True, exist_ok=True)
        self.settings.logs_dir.mkdir(parents=True, exist_ok=True)
        self.settings.telemetry_dir.mkdir(parents=True, exist_ok=True)
        self.settings.db_path.parent.mkdir(parents=True, exist_ok=True)

    async def _wait_noncritical(
        self, *, label: str, timeout: float, operation: Any
    ) -> tuple[bool, Any | None]:
        try:
            result = await asyncio.wait_for(operation, timeout=timeout)
        except asyncio.TimeoutError:
            LOG.warning("%s timed out after %.1fs; skipping", label, timeout)
            return False, None
        except Exception as exc:
            LOG.warning("%s failed (skipped): %s", label, exc)
            return False, None
        return True, result

    def _emit_telemetry_mismatch(
        self,
        *,
        symbol: str,
        trigger: str,
        mismatch_type: str,
        expected: dict[str, Any],
        actual: dict[str, Any],
    ) -> None:
        self.telemetry.append_jsonl(
            "telemetry_mismatch.jsonl",
            {
                "ts": datetime.now(UTC).isoformat(),
                "symbol": symbol,
                "trigger": trigger,
                "mismatch_type": mismatch_type,
                "expected": expected,
                "actual": actual,
            },
        )

    def _emit_cycle_log(
        self,
        *,
        symbol: str,
        interval: str,
        event_ts: datetime,
        shortlist_size: int,
        tracking_events: list[SignalTrackingEvent],
        result: PipelineResult,
        candidates: list[Signal],
        rejected: list[dict[str, Any]],
        delivered: list[Signal] | None = None,
    ) -> None:
        delivered_count = len(delivered or [])
        delivery_status_counts = (
            dict(result.funnel.get("delivery_status_counts", {}))
            if isinstance(result.funnel, dict)
            else {}
        )
        delivery_sent_count = int(delivery_status_counts.get("sent", delivered_count))
        cycle_row: dict[str, Any] = {
            "ts": datetime.now(UTC).isoformat(),
            "mode": {
                "kline_close": "event_driven",
                "intra_candle": "intra_candle",
                "emergency_fallback": "emergency_fallback",
            }.get(result.trigger, result.trigger),
            "trigger": result.trigger,
            "event_symbol": symbol,
            "event_interval": interval,
            "event_ts": event_ts.isoformat(),
            "shortlist_size": shortlist_size,
            "detector_runs": result.raw_setups,
            "post_filter_candidates": len(candidates),
            "selected_signals": delivered_count,
            "raw_setups": result.raw_setups,
            "candidate_count": len(candidates),
            "selected_count": delivered_count,
            "rejected_count": len(rejected),
            "shortlist_source": self._shortlist_source,
            "setup_counts": dict(Counter(s.setup_id for s in candidates)),
            "selected_setup_counts": dict(Counter(s.setup_id for s in (delivered or []))),
            "delivery_status_counts": delivery_status_counts,
            "tracking_events": [e.event_type for e in tracking_events],
            "dry_run": False,
            "status": result.status or ("ok" if not result.error else "error"),
            "prepare_error_count": self._prepare_error_count,
        }
        if result.funnel:
            cycle_row["funnel"] = result.funnel
            if result.funnel.get("prepare_error_stage") is not None:
                cycle_row["prepare_error_stage"] = result.funnel.get("prepare_error_stage")
            if result.funnel.get("prepare_error_exception_type") is not None:
                cycle_row["prepare_error_exception_type"] = result.funnel.get("prepare_error_exception_type")
        if result.error:
            cycle_row["error"] = result.error
        if self._ws_manager is not None:
            ws_snapshot = self._ws_manager.state_snapshot()
            cycle_row.update(ws_snapshot if isinstance(ws_snapshot, dict) else {})
        rest_snapshot_func = getattr(self.client, "state_snapshot", None)
        if callable(rest_snapshot_func):
            rest_snapshot = rest_snapshot_func()
            cycle_row.update(rest_snapshot if isinstance(rest_snapshot, dict) else {})

        self.telemetry.append_jsonl("cycles.jsonl", cycle_row)
        symbol_row: dict[str, Any] = {
            "ts": datetime.now(UTC).isoformat(),
            "symbol": symbol,
            "event_ts": event_ts.isoformat(),
            "status": result.status or ("ok" if not result.error else "error"),
            "error": result.error,
            "detector_runs": result.raw_setups,
            "post_filter_candidates": len(candidates),
            "selected_signals": delivered_count,
            "raw_setups": result.raw_setups,
            "candidates": len(candidates),
            "delivered": delivered_count,
            "rejected": len(rejected),
            "shortlist_source": self._shortlist_source,
            "delivery_status_counts": delivery_status_counts,
        }
        if result.prepared is not None:
            symbol_row.update(
                {
                    "work_rows_15m": int(result.prepared.work_15m.height) if result.prepared.work_15m is not None else 0,
                    "work_rows_1h": int(result.prepared.work_1h.height) if result.prepared.work_1h is not None else 0,
                    "work_rows_5m": int(result.prepared.work_5m.height) if result.prepared.work_5m is not None else 0,
                    "work_rows_4h": int(result.prepared.work_4h.height) if result.prepared.work_4h is not None else 0,
                    "spread_bps": result.prepared.spread_bps,
                    "bias_4h": result.prepared.bias_4h,
                    "bias_1h": result.prepared.bias_1h,
                    "market_regime": result.prepared.market_regime,
                    "context_snapshot_age_seconds": result.prepared.context_snapshot_age_seconds,
                    "data_source_mix": result.prepared.data_source_mix,
                    "mark_index_spread_bps": result.prepared.mark_index_spread_bps,
                    "premium_zscore_5m": result.prepared.premium_zscore_5m,
                    "premium_slope_5m": result.prepared.premium_slope_5m,
                    "oi_slope_5m": result.prepared.oi_slope_5m,
                    "top_vs_global_ls_gap": result.prepared.top_vs_global_ls_gap,
                }
            )
        if result.funnel:
            symbol_row["funnel"] = result.funnel
            if result.funnel.get("prepare_error_stage") is not None:
                symbol_row["prepare_error_stage"] = result.funnel.get("prepare_error_stage")
            if result.funnel.get("prepare_error_exception_type") is not None:
                symbol_row["prepare_error_exception_type"] = result.funnel.get("prepare_error_exception_type")
        self.telemetry.append_jsonl("symbol_analysis.jsonl", symbol_row)
        if delivery_sent_count != delivered_count:
            self._emit_telemetry_mismatch(
                symbol=symbol,
                trigger=result.trigger,
                mismatch_type="delivery_sent_vs_symbol_analysis",
                expected={"sent_delivery_rows": delivery_sent_count},
                actual={"symbol_analysis_delivered": delivered_count},
            )
        LOG.info(
            "cycle | symbol=%s detector_runs=%d candidates=%d delivered=%d rejected=%d status=%s",
            symbol, result.raw_setups, len(candidates),
            delivered_count, len(rejected), result.status or "ok",
        )
