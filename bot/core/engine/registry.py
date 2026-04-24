"""Strategy registry for pluggable strategy system."""

from __future__ import annotations

import logging
from typing import Any

from .base import AbstractStrategy, StrategyMetadata

LOG = logging.getLogger("bot.core.engine.registry")


class StrategyRegistry:
    """Registry for managing strategy plugins.
    
    Supports:
    - Dynamic strategy registration
    - Hot-reload of strategy parameters
    - Strategy filtering by tags/timeframes
    - Performance tracking per strategy
    """
    
    def __init__(self) -> None:
        self._strategies: dict[str, AbstractStrategy] = {}
        self._enabled: set[str] = set()
        self._performance: dict[str, dict[str, Any]] = {}
    
    def register(self, strategy: AbstractStrategy, enabled: bool = True) -> None:
        """Register a strategy instance.
        
        Args:
            strategy: Strategy instance to register
            enabled: Whether strategy is enabled by default
        """
        strategy_id = strategy.strategy_id
        
        if strategy_id in self._strategies:
            LOG.warning("Strategy %s already registered, replacing", strategy_id)
        
        self._strategies[strategy_id] = strategy
        
        if enabled:
            self._enabled.add(strategy_id)
        
        self._performance[strategy_id] = {
            "signals_generated": 0,
            "calculation_errors": 0,
            "avg_calculation_ms": 0.0,
        }
        
        LOG.info("Registered strategy: %s (enabled=%s)", strategy_id, enabled)
    
    def unregister(self, strategy_id: str) -> None:
        """Remove strategy from registry."""
        self._strategies.pop(strategy_id, None)
        self._enabled.discard(strategy_id)
        self._performance.pop(strategy_id, None)
        LOG.info("Unregistered strategy: %s", strategy_id)
    
    def get(self, strategy_id: str) -> AbstractStrategy | None:
        """Get strategy by ID."""
        return self._strategies.get(strategy_id)
    
    def get_enabled(self) -> list[AbstractStrategy]:
        """Get list of enabled strategies."""
        return [self._strategies[sid] for sid in self._enabled if sid in self._strategies]
    
    def list_all(self) -> list[StrategyMetadata]:
        """List metadata for all registered strategies."""
        return [s.metadata for s in self._strategies.values()]
    
    def list_enabled(self) -> list[StrategyMetadata]:
        """List metadata for enabled strategies."""
        return [
            self._strategies[sid].metadata 
            for sid in self._enabled 
            if sid in self._strategies
        ]
    
    def enable(self, strategy_id: str) -> bool:
        """Enable a strategy."""
        if strategy_id not in self._strategies:
            LOG.error("Cannot enable unknown strategy: %s", strategy_id)
            return False
        self._enabled.add(strategy_id)
        LOG.info("Enabled strategy: %s", strategy_id)
        return True
    
    def disable(self, strategy_id: str) -> bool:
        """Disable a strategy."""
        if strategy_id not in self._strategies:
            LOG.error("Cannot disable unknown strategy: %s", strategy_id)
            return False
        self._enabled.discard(strategy_id)
        LOG.info("Disabled strategy: %s", strategy_id)
        return True
    
    def is_enabled(self, strategy_id: str) -> bool:
        """Check if strategy is enabled."""
        return strategy_id in self._enabled
    
    def update_parameters(self, strategy_id: str, parameters: dict[str, Any]) -> bool:
        """Hot-update strategy parameters."""
        strategy = self._strategies.get(strategy_id)
        if strategy is None:
            LOG.error("Cannot update parameters for unknown strategy: %s", strategy_id)
            return False
        
        strategy.update_parameters(parameters)
        LOG.info("Updated parameters for %s: %s", strategy_id, parameters)
        return True
    
    def record_performance(
        self, 
        strategy_id: str, 
        calculation_ms: float,
        error: bool = False
    ) -> None:
        """Record performance metrics for a strategy."""
        if strategy_id not in self._performance:
            return
        
        perf = self._performance[strategy_id]
        if error:
            perf["calculation_errors"] += 1
        else:
            perf["signals_generated"] += 1
            # Running average
            old_avg = perf["avg_calculation_ms"]
            n = perf["signals_generated"]
            perf["avg_calculation_ms"] = (old_avg * (n - 1) + calculation_ms) / n
    
    def get_performance(self, strategy_id: str) -> dict[str, Any] | None:
        """Get performance metrics for a strategy."""
        return self._performance.get(strategy_id)
    
    def get_all_performance(self) -> dict[str, dict[str, Any]]:
        """Get performance metrics for all strategies."""
        return self._performance.copy()
    
    def filter_by_tags(self, tags: list[str]) -> list[AbstractStrategy]:
        """Get strategies matching all specified tags."""
        result = []
        for strategy in self._strategies.values():
            if all(tag in strategy.metadata.tags for tag in tags):
                result.append(strategy)
        return result
    
    def filter_by_timeframe(self, timeframe: str) -> list[AbstractStrategy]:
        """Get strategies supporting specific timeframe."""
        result = []
        for strategy in self._strategies.values():
            if timeframe in strategy.metadata.timeframes:
                result.append(strategy)
        return result
    
    def clear(self) -> None:
        """Clear all registered strategies."""
        self._strategies.clear()
        self._enabled.clear()
        self._performance.clear()
    
    def __len__(self) -> int:
        return len(self._strategies)
    
    def __contains__(self, strategy_id: str) -> bool:
        return strategy_id in self._strategies
