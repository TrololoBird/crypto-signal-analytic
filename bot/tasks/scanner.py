"""Symbol scanner task - periodically scans for trading opportunities."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from ..core.engine import SignalEngine, StrategyRegistry
from ..core.memory import MemoryRepository, SignalRecord
from ..core.diagnostics import BotMetrics
from ..models import Signal, PreparedSymbol

LOG = logging.getLogger("bot.tasks.scanner")


class SymbolScanner:
    """Scanner task that periodically checks symbols for signals.
    
    Coordinates with:
    - SignalEngine for strategy calculations
    - MemoryRepository for signal persistence
    - BotMetrics for telemetry
    """
    
    def __init__(
        self,
        engine: SignalEngine,
        registry: StrategyRegistry,
        repository: MemoryRepository,
        metrics: BotMetrics,
        symbols_provider: Any,  # Will be connected to ws_manager or data source
        min_score: float = 0.6,
    ):
        self._engine = engine
        self._registry = registry
        self._repo = repository
        self._metrics = metrics
        self._symbols = symbols_provider
        self._min_score = min_score
    
    async def scan_all(self) -> list[Signal]:
        """Scan all available symbols.
        
        Returns:
            List of signals generated
        """
        # Get symbols to scan (from provider)
        symbols = await self._get_symbols_to_scan()
        
        if not symbols:
            LOG.debug("No symbols to scan")
            return []
        
        LOG.info("Scanning %d symbols with %d strategies", 
                len(symbols), len(self._registry.get_enabled()))
        
        signals = []
        for symbol_data in symbols:
            try:
                signal = await self._scan_one(symbol_data)
                if signal:
                    signals.append(signal)
            except Exception as exc:
                LOG.error("Scan failed for %s: %s", getattr(symbol_data, 'symbol', '?'), exc)
        
        LOG.info("Scan complete: %d signals from %d symbols", len(signals), len(symbols))
        return signals
    
    async def _scan_one(self, prepared: PreparedSymbol) -> Signal | None:
        """Scan a single symbol."""
        # Run all strategies
        results = await self._engine.calculate_all(prepared)
        
        # Get best signal above threshold
        signals = self._engine.get_signals_above_threshold(results, self._min_score)
        
        if not signals:
            return None
        
        # Take highest scored signal
        best = signals[0]
        
        # Record metrics
        strategy_id = best.metadata.get("strategy", "unknown")
        self._metrics.record_signal(strategy_id, best.score)
        
        # Persist to memory
        await self._persist_signal(prepared, best, results)
        
        return best
    
    async def _get_symbols_to_scan(self) -> list[PreparedSymbol]:
        """Get list of symbols to scan.
        
        This should integrate with your data source (ws_manager, etc.)
        """
        # Placeholder - integrate with your symbol provider
        # Return list of PreparedSymbol objects
        return []
    
    async def _persist_signal(
        self,
        prepared: PreparedSymbol,
        signal: Signal,
        all_results: list[Any],
    ) -> None:
        """Persist signal to repository."""
        import uuid
        from datetime import datetime, timezone
        
        record = SignalRecord(
            signal_id=str(uuid.uuid4()),
            symbol=prepared.symbol,
            strategy_id=signal.metadata.get("strategy", "unknown"),
            direction=signal.side,
            entry_price=signal.entry,
            stop_loss=signal.sl,
            take_profit_1=signal.tp1,
            take_profit_2=signal.tp2,
            score=signal.score,
            created_at=datetime.now(timezone.utc),
            timeframe="1h",
            atr_pct=prepared.atr_pct or 0.0,
            spread_bps=prepared.spread_bps or 0.0,
            metadata={
                "all_strategies": [
                    {
                        "strategy": r.signal.metadata.get("strategy"),
                        "score": r.signal.score,
                        "confidence": r.confidence,
                    }
                    for r in all_results
                    if r.is_valid and r.signal
                ] if all_results else [],
            }
        )
        
        await self._repo.save_signal(record)
    
    async def run(self) -> None:
        """Execute one scan cycle."""
        await self.scan_all()
