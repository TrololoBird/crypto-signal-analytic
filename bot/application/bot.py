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
import html
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
from ..feature_flags import FeatureFlags
from ..features import min_required_bars, prepare_symbol
from ..filters import apply_global_filters
from ..market_data import BinanceFuturesMarketData, MarketDataUnavailable
from ..models import PreparedSymbol, Signal, SymbolFrames, UniverseSymbol, PipelineResult
from ..monitor_bot import HealthMonitor
from ..outcomes import build_prepared_feature_snapshot, extract_features_from_signal
from ..setup_base import SetupParams
from ..strategies import STRATEGY_CLASSES
from ..telemetry import TelemetryStore
from ..tracking import SignalTrackingEvent
from ..confluence import ConfluenceEngine
from .container import build_application_container
from .cycle_runner import CycleRunner
from .delivery_orchestrator import DeliveryOrchestrator
from .health_manager import HealthManager
from .market_context_updater import MarketContextUpdater
from .shortlist_service import ShortlistService
from .symbol_analyzer import SymbolAnalyzer

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
        self.feature_flags = FeatureFlags(settings)
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
        self._running: bool = False

        # Intra-candle scan throttle — monotonic timestamp of last scan per symbol
        self._last_intra_scan: dict[str, float] = {}
        self._last_intra_mid: dict[str, float] = {}
        self._shortlist_service = ShortlistService(self)
        self._cycle_runner = CycleRunner(self)
        self._symbol_analyzer = SymbolAnalyzer(self)
        self._delivery_orchestrator = DeliveryOrchestrator(self)
        self._health_manager = HealthManager(self)
        self._market_context_updater = MarketContextUpdater(self)
        self._health_monitor = HealthMonitor(
            interval_seconds=float(self.settings.runtime.heartbeat_seconds),
            check=self.health_check,
            publish=lambda payload: self.telemetry.append_jsonl("health_runtime.jsonl", payload),
            alert=self._alert_critical,
            alert_after_failures=3,
        )

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
        return await self._symbol_analyzer.run_modern_analysis(
            item,
            frames,
            trigger=trigger,
            event_ts=event_ts,
            ws_enrichments=ws_enrichments,
        )

    def _select_and_rank(
        self,
        all_candidates: dict[str, list[Signal]],
        max_signals: int,
    ) -> list[Signal]:
        return self._delivery_orchestrator.select_and_rank(all_candidates, max_signals)

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
                (depth_imbalance is not None and depth_imbalance >= 0.05)
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
        self._running = True

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
            asyncio.create_task(self._health_monitor.run(stop_event=self._shutdown), name="health_monitor"),
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
        self._running = False
        self._shutdown.set()

        # Cancel and await all background tasks
        if self._background_tasks:
            for task in list(self._background_tasks):
                task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.gather(*self._background_tasks, return_exceptions=True)
            self._background_tasks.clear()

        try:
            await self.tracker._persist_tracking_state()
        except Exception as exc:
            LOG.debug("tracker persist failed (non-fatal): %s", exc)

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

    async def health_check(self) -> dict[str, Any]:
        return await self._health_manager.health_check()

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
        return await self._symbol_analyzer.fetch_frames(item)

    async def _preload_shortlist_frames(self) -> None:
        await self._symbol_analyzer.preload_shortlist_frames()

    def _ws_cache_enrichments(self, symbol: str) -> dict[str, Any]:
        return self._symbol_analyzer.ws_cache_enrichments(symbol)

    def _refresh_universe_symbol_from_ws(self, item: UniverseSymbol) -> UniverseSymbol:
        return self._symbol_analyzer.refresh_universe_symbol_from_ws(item)

    async def _ws_enrich(self, result: PipelineResult) -> None:
        await self._symbol_analyzer.ws_enrich(result)

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
        await self._market_context_updater.market_regime_periodic()

    async def _public_intelligence_periodic(self) -> None:
        await self._market_context_updater.public_intelligence_periodic()

    async def _apply_public_guardrails(self, snapshot: dict[str, Any]) -> None:
        await self._market_context_updater.apply_public_guardrails(snapshot)

    async def _update_memory_market_context(self, shortlist: list[UniverseSymbol]) -> None:
        await self._market_context_updater.update_memory_market_context(shortlist)

    def _compute_price_bias(self, symbol: str) -> str:
        return self._market_context_updater.compute_price_bias(symbol)

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
        return await self._delivery_orchestrator.select_and_deliver(
            signals,
            prepared_by_tracking_id=prepared_by_tracking_id,
        )

    async def _close_superseded_signal(self, new_signal: Signal) -> list[SignalTrackingEvent] | None:
        return await self._delivery_orchestrator.close_superseded_signal(new_signal)

    async def _deliver_tracking(self, events: list[SignalTrackingEvent]) -> None:
        await self._delivery_orchestrator.deliver_tracking(events)

    # ------------------------------------------------------------------
    # Heartbeat & health telemetry
    # ------------------------------------------------------------------

    async def _heartbeat_periodic(self) -> None:
        await self._health_manager.heartbeat_periodic()

    async def _health_telemetry_periodic(self) -> None:
        await self._health_manager.health_telemetry_periodic()

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
            await self._alert_critical(exc, {"label": label, "timeout": timeout})
            return False, None
        return True, result

    async def _alert_critical(self, exc: Exception, context: dict[str, Any]) -> None:
        sender = getattr(self.telegram, "send_html", None)
        if not callable(sender):
            return
        try:
            await sender(
                "<b>🚨 CRITICAL ERROR</b>\n"
                f"<code>{html.escape(type(exc).__name__)}: {html.escape(str(exc))}</code>\n"
                f"<code>context={html.escape(str(context))}</code>"
            )
        except Exception:
            LOG.debug("critical alert dispatch failed", exc_info=True)

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
