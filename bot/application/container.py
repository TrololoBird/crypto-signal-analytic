"""Dependency container for SignalBot runtime."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable

from ..alerts import AlertCoordinator
from ..config import BotSettings
from ..core.engine import SignalEngine, StrategyRegistry
from ..core.event_bus import EventBus
from ..core.memory import MemoryRepository
from ..delivery import SignalDelivery
from ..market_data import BinanceFuturesMarketData
from ..messaging import TelegramBroadcaster
from ..public_intelligence import PublicIntelligenceService
from ..telemetry import TelemetryStore
from ..tracking import SignalTracker
from ..ws_manager import FuturesWSManager

LOG = logging.getLogger("bot.application.container")


@dataclass(slots=True)
class ApplicationContainer:
    """Initialized runtime dependencies for ``SignalBot``."""

    client: BinanceFuturesMarketData
    bus: EventBus
    ws_manager: FuturesWSManager | None
    telemetry: TelemetryStore
    telegram: Any
    delivery: SignalDelivery
    alerts: AlertCoordinator
    repository: MemoryRepository
    tracker: SignalTracker
    intelligence: PublicIntelligenceService | None
    registry: StrategyRegistry
    engine: SignalEngine


def build_application_container(
    settings: BotSettings,
    *,
    register_strategies: Callable[[StrategyRegistry], None],
    market_data: BinanceFuturesMarketData | None = None,
    broadcaster: Any | None = None,
    telemetry: TelemetryStore | None = None,
) -> ApplicationContainer:
    """Build runtime dependencies for ``SignalBot``.

    ``register_strategies`` is provided by ``SignalBot`` to keep strategy wiring in one place.
    """

    client = market_data or BinanceFuturesMarketData(
        rest_timeout_seconds=settings.ws.rest_timeout_seconds,
    )

    bus = EventBus()
    ws_manager: FuturesWSManager | None = None
    if settings.ws.enabled and isinstance(client, BinanceFuturesMarketData):
        ws_manager = FuturesWSManager(client, settings.ws)
        ws_manager.set_event_bus(bus)
        if hasattr(client, "_ws"):
            client._ws = ws_manager
        LOG.info("ws_manager initialized | pinned_symbols=%d", len(settings.universe.pinned_symbols))

    tg = broadcaster or TelegramBroadcaster(settings.tg_token, settings.target_chat_id)
    delivery = SignalDelivery(
        tg, pending_expiry_minutes=settings.tracking.pending_expiry_minutes
    )
    telemetry_store = telemetry or TelemetryStore(settings.telemetry_dir)
    alerts = AlertCoordinator(
        settings=settings,
        broadcaster=tg,
        telemetry=telemetry_store,
    )

    repository = MemoryRepository(
        db_path=settings.db_path,
        data_dir=settings.data_dir / "parquet",
    )

    tracker = SignalTracker(
        settings,
        market_data=client,
        telemetry=telemetry_store,
        memory_repo=repository,
    )

    intelligence: PublicIntelligenceService | None = None
    if isinstance(client, BinanceFuturesMarketData):
        intelligence = PublicIntelligenceService(settings, client, telemetry_store)

    registry = StrategyRegistry()
    register_strategies(registry)
    engine = SignalEngine(registry, settings)

    return ApplicationContainer(
        client=client,
        bus=bus,
        ws_manager=ws_manager,
        telemetry=telemetry_store,
        telegram=tg,
        delivery=delivery,
        alerts=alerts,
        repository=repository,
        tracker=tracker,
        intelligence=intelligence,
        registry=registry,
        engine=engine,
    )
