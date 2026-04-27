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
from .fallback_runner import FallbackRunner
from .health_manager import HealthManager
from .intra_candle_scanner import IntraCandleScanner
from .kline_handler import KlineHandler
from .market_context_updater import MarketContextUpdater
from .oi_refresh_runner import OIRefreshRunner
from .shortlist_service import ShortlistService
from .symbol_analyzer import SymbolAnalyzer
from .telemetry_manager import TelemetryManager

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
        from bot.ml import MLFilter
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
        self._intra_candle_scanner = IntraCandleScanner(self)
        self._kline_handler = KlineHandler(self)
        self._telemetry_manager = TelemetryManager(self)
        self._fallback_runner = FallbackRunner(self)
        self._oi_refresh_runner = OIRefreshRunner(self)
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

    def _get_symbol_analyzer(self) -> SymbolAnalyzer:
        analyzer = getattr(self, "_symbol_analyzer", None)
        if analyzer is None:
            analyzer = SymbolAnalyzer(self)
            self._symbol_analyzer = analyzer
        return analyzer

    def _get_delivery_orchestrator(self) -> DeliveryOrchestrator:
        orchestrator = getattr(self, "_delivery_orchestrator", None)
        if orchestrator is None:
            orchestrator = DeliveryOrchestrator(self)
            self._delivery_orchestrator = orchestrator
        return orchestrator

    def _get_cycle_runner(self) -> CycleRunner:
        runner = getattr(self, "_cycle_runner", None)
        if runner is None:
            runner = CycleRunner(self)
            self._cycle_runner = runner
        return runner

    def _get_intra_candle_scanner(self) -> IntraCandleScanner:
        scanner = getattr(self, "_intra_candle_scanner", None)
        if scanner is None:
            scanner = IntraCandleScanner(self)
            self._intra_candle_scanner = scanner
        return scanner

    def _get_telemetry_manager(self) -> TelemetryManager:
        manager = getattr(self, "_telemetry_manager", None)
        if manager is None:
            manager = TelemetryManager(self)
            self._telemetry_manager = manager
        return manager

    def _get_kline_handler(self) -> KlineHandler:
        handler = getattr(self, "_kline_handler", None)
        if handler is None:
            handler = KlineHandler(self)
            self._kline_handler = handler
        return handler

    def _get_fallback_runner(self) -> FallbackRunner:
        runner = getattr(self, "_fallback_runner", None)
        if runner is None:
            runner = FallbackRunner(self)
            self._fallback_runner = runner
        return runner

    def _get_oi_refresh_runner(self) -> OIRefreshRunner:
        runner = getattr(self, "_oi_refresh_runner", None)
        if runner is None:
            runner = OIRefreshRunner(self)
            self._oi_refresh_runner = runner
        return runner

    def _decision_to_reject_row(self, *, symbol: str, decision: StrategyDecision) -> dict[str, Any]:
        return self._get_telemetry_manager().decision_to_reject_row(symbol=symbol, decision=decision)

    def _append_symbol_trace(self, *, symbol: str, row: dict[str, Any]) -> None:
        self._get_telemetry_manager().append_symbol_trace(symbol=symbol, row=row)

    def _append_strategy_decision_telemetry(
        self,
        *,
        symbol: str,
        trigger: str,
        decision: StrategyDecision,
    ) -> None:
        self._get_telemetry_manager().append_strategy_decision(
            symbol=symbol,
            trigger=trigger,
            decision=decision,
        )

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

    # Compatibility shims for existing tests/callers; logic lives in SymbolAnalyzer.
    def _directional_context(
        self,
        signal: Signal,
        prepared: PreparedSymbol,
    ) -> dict[str, Any]:
        return self._get_symbol_analyzer().directional_context(signal, prepared)

    def _check_family_precheck(
        self,
        signal: Signal,
        prepared: PreparedSymbol,
        metadata: Any | None,
    ) -> tuple[bool, str | None, dict[str, Any]]:
        return self._get_symbol_analyzer().check_family_precheck(signal, prepared, metadata)

    def _apply_alignment_penalty(
        self,
        signal: Signal,
        prepared: PreparedSymbol,
        metadata: Any | None,
    ) -> tuple[Signal, dict[str, Any]]:
        return self._get_symbol_analyzer().apply_alignment_penalty(signal, prepared, metadata)

    def _check_family_confirmation(
        self,
        signal: Signal,
        prepared: PreparedSymbol,
        metadata: Any | None,
    ) -> tuple[bool, str | None, dict[str, Any]]:
        return self._get_symbol_analyzer().check_family_confirmation(signal, prepared, metadata)

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
        """Delegate kline-close handling to KlineHandler."""
        await self._get_kline_handler().on_kline_close(event)

    async def _on_reconnect(self, event: ReconnectEvent) -> None:
        LOG.info("ws reconnected | reason=%s", event.reason)

    async def _on_book_ticker(self, event: BookTickerEvent) -> None:
        """Delegate intra-candle scan trigger handling to IntraCandleScanner."""
        await self._get_intra_candle_scanner().handle(event)

    # ------------------------------------------------------------------
    # Shared analysis logic — used by both kline_close and intra_candle paths
    # ------------------------------------------------------------------

    async def _select_and_deliver_for_symbol(
        self,
        symbol: str,
        result: PipelineResult,
    ) -> tuple[list[Signal], list[dict], list[Signal]]:
        return await self._get_kline_handler().select_and_deliver_for_symbol(symbol, result)

    # ------------------------------------------------------------------
    # Emergency fallback — full scan when no kline events
    # ------------------------------------------------------------------

    async def _tracking_review_periodic(self) -> None:
        """Delegate tracking-review loop to FallbackRunner."""
        await self._get_fallback_runner().tracking_review_periodic()

    async def _emergency_fallback_scan(self) -> None:
        """Delegate emergency-fallback loop to FallbackRunner."""
        await self._get_fallback_runner().emergency_fallback_scan()

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
        """Delegate OI/L-S refresh loop to OIRefreshRunner."""
        await self._get_oi_refresh_runner().run()

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
        return await self._get_delivery_orchestrator().select_and_deliver(
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
        self._get_telemetry_manager().emit_telemetry_mismatch(
            symbol=symbol,
            trigger=trigger,
            mismatch_type=mismatch_type,
            expected=expected,
            actual=actual,
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
        self._get_telemetry_manager().emit_cycle_log(
            symbol=symbol,
            interval=interval,
            event_ts=event_ts,
            shortlist_size=shortlist_size,
            tracking_events=tracking_events,
            result=result,
            candidates=candidates,
            rejected=rejected,
            delivered=delivered,
        )
