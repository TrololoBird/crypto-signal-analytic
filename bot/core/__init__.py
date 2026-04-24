"""Core modules - EventBus, events, engine, memory, analyzer, diagnostics."""
from __future__ import annotations

from .event_bus import EventBus
from .events import AnyEvent, KlineCloseEvent, OIRefreshDueEvent, ReconnectEvent, ShortlistUpdatedEvent

# New architecture modules
from .engine import (
    StrategyRegistry,
    StrategyMetadata,
    AbstractStrategy,
    SignalResult,
    SignalEngine,
)
from .memory import (
    MemoryRepository,
    SignalRecord,
    OutcomeRecord,
    ParquetCache,
    TimeSeriesCache,
)
from .analyzer import (
    OutcomeTracker,
    PerformanceMetrics,
    WinRateCalculator,
    DailyReporter,
)
from .diagnostics import (
    MetricsExporter,
    BotMetrics,
    HealthChecker,
    HealthStatus,
    AlertManager,
    AlertSeverity,
)

__all__ = [
    # Events
    "EventBus",
    "AnyEvent",
    "KlineCloseEvent",
    "ShortlistUpdatedEvent",
    "ReconnectEvent",
    "OIRefreshDueEvent",
    # Engine
    "StrategyRegistry",
    "StrategyMetadata",
    "AbstractStrategy",
    "SignalResult",
    "SignalEngine",
    # Memory
    "MemoryRepository",
    "SignalRecord",
    "OutcomeRecord",
    "ParquetCache",
    "TimeSeriesCache",
    # Analyzer
    "OutcomeTracker",
    "PerformanceMetrics",
    "WinRateCalculator",
    "DailyReporter",
    # Diagnostics
    "MetricsExporter",
    "BotMetrics",
    "HealthChecker",
    "HealthStatus",
    "AlertManager",
    "AlertSeverity",
]
