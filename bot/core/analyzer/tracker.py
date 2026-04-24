"""Outcome tracker for post-signal analysis."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from ..memory.repository import MemoryRepository, SignalRecord, OutcomeRecord

LOG = logging.getLogger("bot.core.analyzer.tracker")


@dataclass
class PriceSnapshot:
    """Price data at a point in time."""
    price: float
    timestamp: datetime
    high: float | None = None
    low: float | None = None
    volume: float | None = None


class OutcomeTracker:
    """Tracks outcomes for generated signals.
    
    Updates signal outcomes at 1h, 4h, 24h intervals.
    Calculates MAE/MFE and hit rates for TP/SL.
    """
    
    # Time checkpoints for tracking (hours)
    CHECKPOINTS = [1, 4, 24]
    
    def __init__(self, repository: MemoryRepository):
        self._repo = repository
    
    async def update_outcomes(
        self, 
        signal_id: str,
        current_price: float,
        current_high: float | None = None,
        current_low: float | None = None,
    ) -> OutcomeRecord | None:
        """Update outcome for a signal with current market data.
        
        Args:
            signal_id: Signal to update
            current_price: Current market price
            current_high: Optional high since last check
            current_low: Optional low since last check
            
        Returns:
            Updated OutcomeRecord or None if signal not found
        """
        # Get signal
        signal = await self._repo.get_signal(signal_id)
        if signal is None:
            LOG.warning("Signal not found: %s", signal_id)
            return None
        
        # Get or create outcome
        outcome_id = f"outcome_{signal_id}"
        outcome = await self._repo.get_outcome(outcome_id)
        
        if outcome is None:
            outcome = OutcomeRecord(
                outcome_id=outcome_id,
                signal_id=signal_id,
                symbol=signal.symbol,
            )
        
        # Calculate time elapsed
        now = datetime.now(timezone.utc)
        elapsed_hours = (now - signal.created_at).total_seconds() / 3600
        
        # Update price checkpoints
        if elapsed_hours >= 1 and outcome.price_1h is None:
            outcome.price_1h = current_price
            outcome.pnl_1h = self._calculate_pnl(signal, current_price)
            
        if elapsed_hours >= 4 and outcome.price_4h is None:
            outcome.price_4h = current_price
            outcome.pnl_4h = self._calculate_pnl(signal, current_price)
            
        if elapsed_hours >= 24 and outcome.price_24h is None:
            outcome.price_24h = current_price
            outcome.pnl_24h = self._calculate_pnl(signal, current_price)
            outcome.closed_at = now
            outcome.result = self._classify_result(outcome, signal)
        
        # Update MAE/MFE
        pnl_pct = self._calculate_pnl(signal, current_price)
        if pnl_pct is not None:
            outcome.mfe = max(outcome.mfe, pnl_pct) if pnl_pct > 0 else outcome.mfe
            outcome.mae = min(outcome.mae, pnl_pct) if pnl_pct < 0 else outcome.mae
            
            if outcome.max_profit_pct < pnl_pct:
                outcome.max_profit_pct = pnl_pct
            if outcome.max_loss_pct > pnl_pct:
                outcome.max_loss_pct = pnl_pct
        
        # Check TP/SL hits
        self._check_targets(outcome, signal, current_price, current_high, current_low)
        
        # Update timestamp
        outcome.updated_at = now
        
        # Save
        await self._repo.save_outcome(outcome)
        
        return outcome
    
    def _calculate_pnl(
        self, 
        signal: SignalRecord, 
        current_price: float
    ) -> float | None:
        """Calculate PnL percentage from entry."""
        if signal.entry_price <= 0 or current_price <= 0:
            return None
        
        if signal.direction == "long":
            return (current_price - signal.entry_price) / signal.entry_price * 100
        else:  # short
            return (signal.entry_price - current_price) / signal.entry_price * 100
    
    def _check_targets(
        self,
        outcome: OutcomeRecord,
        signal: SignalRecord,
        price: float,
        high: float | None,
        low: float | None,
    ) -> None:
        """Check if TP or SL levels were hit."""
        if signal.direction == "long":
            # Check TP1 hit
            if not outcome.hit_tp1 and price >= signal.take_profit_1:
                outcome.hit_tp1 = True
                if outcome.time_to_tp1_min is None:
                    elapsed = (datetime.now(timezone.utc) - signal.created_at).total_seconds() / 60
                    outcome.time_to_tp1_min = int(elapsed)
            
            # Check TP2 hit
            if not outcome.hit_tp2 and price >= signal.take_profit_2:
                outcome.hit_tp2 = True
                if outcome.time_to_tp2_min is None:
                    elapsed = (datetime.now(timezone.utc) - signal.created_at).total_seconds() / 60
                    outcome.time_to_tp2_min = int(elapsed)
            
            # Check SL hit
            if not outcome.hit_sl and price <= signal.stop_loss:
                outcome.hit_sl = True
                if outcome.time_to_sl_min is None:
                    elapsed = (datetime.now(timezone.utc) - signal.created_at).total_seconds() / 60
                    outcome.time_to_sl_min = int(elapsed)
        
        else:  # short
            # Check TP1 hit
            if not outcome.hit_tp1 and price <= signal.take_profit_1:
                outcome.hit_tp1 = True
                if outcome.time_to_tp1_min is None:
                    elapsed = (datetime.now(timezone.utc) - signal.created_at).total_seconds() / 60
                    outcome.time_to_tp1_min = int(elapsed)
            
            # Check TP2 hit
            if not outcome.hit_tp2 and price <= signal.take_profit_2:
                outcome.hit_tp2 = True
                if outcome.time_to_tp2_min is None:
                    elapsed = (datetime.now(timezone.utc) - signal.created_at).total_seconds() / 60
                    outcome.time_to_tp2_min = int(elapsed)
            
            # Check SL hit
            if not outcome.hit_sl and price >= signal.stop_loss:
                outcome.hit_sl = True
                if outcome.time_to_sl_min is None:
                    elapsed = (datetime.now(timezone.utc) - signal.created_at).total_seconds() / 60
                    outcome.time_to_sl_min = int(elapsed)
    
    def _classify_result(
        self, 
        outcome: OutcomeRecord,
        signal: SignalRecord
    ) -> str:
        """Classify final outcome."""
        # Priority: SL hit = loss, TP hit = win
        if outcome.hit_sl:
            return "loss"
        
        if outcome.hit_tp1 or outcome.hit_tp2:
            return "win"
        
        # Check 24h PnL
        if outcome.pnl_24h is not None:
            if outcome.pnl_24h > 0.5:  # > 0.5% profit
                return "win"
            elif outcome.pnl_24h < -0.5:  # > 0.5% loss
                return "loss"
        
        return "breakeven"
    
    async def get_pending_signals(self, limit: int = 100) -> list[SignalRecord]:
        """Get signals without outcomes or with open outcomes."""
        return await self._repo.get_signals_without_outcome(limit=limit)
    
    async def batch_update(
        self,
        prices: dict[str, PriceSnapshot]
    ) -> list[OutcomeRecord]:
        """Batch update outcomes for multiple signals.
        
        Args:
            prices: Dict mapping symbol to PriceSnapshot
            
        Returns:
            List of updated OutcomeRecords
        """
        pending = await self.get_pending_signals(limit=200)
        
        updated = []
        for signal in pending:
            if signal.symbol in prices:
                snapshot = prices[signal.symbol]
                outcome = await self.update_outcomes(
                    signal.signal_id,
                    snapshot.price,
                    snapshot.high,
                    snapshot.low,
                )
                if outcome:
                    updated.append(outcome)
        
        LOG.info("Updated %d outcomes from batch", len(updated))
        return updated
