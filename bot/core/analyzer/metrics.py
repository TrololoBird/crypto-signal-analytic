"""Performance metrics calculation for strategy analysis."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

import polars as pl
import numpy as np

from ..memory.repository import MemoryRepository

LOG = logging.getLogger("bot.core.analyzer.metrics")


def _as_float(value: object, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    return default


@dataclass
class PerformanceMetrics:
    """Performance metrics for strategy or overall system."""
    
    # Basic counts
    total_signals: int = 0
    wins: int = 0
    losses: int = 0
    breakeven: int = 0
    
    # Ratios
    win_rate: float = 0.0
    loss_rate: float = 0.0
    profit_factor: float = 0.0
    
    # Returns
    avg_win_pct: float = 0.0
    avg_loss_pct: float = 0.0
    max_win_pct: float = 0.0
    max_loss_pct: float = 0.0
    
    # Risk metrics
    avg_mae: float = 0.0  # Max adverse excursion
    avg_mfe: float = 0.0  # Max favorable excursion
    avg_risk_reward: float = 0.0
    
    # Time metrics
    avg_time_to_tp1_min: float | None = None
    avg_time_to_sl_min: float | None = None
    
    # Sharpe-like ratio (simplified)
    sharpe_ratio: float = 0.0
    
    def to_dict(self) -> dict[str, Any]:
        return {
            "total_signals": self.total_signals,
            "wins": self.wins,
            "losses": self.losses,
            "breakeven": self.breakeven,
            "win_rate": round(self.win_rate, 4),
            "loss_rate": round(self.loss_rate, 4),
            "profit_factor": round(self.profit_factor, 4),
            "avg_win_pct": round(self.avg_win_pct, 4),
            "avg_loss_pct": round(self.avg_loss_pct, 4),
            "max_win_pct": round(self.max_win_pct, 4),
            "max_loss_pct": round(self.max_loss_pct, 4),
            "avg_mae": round(self.avg_mae, 4),
            "avg_mfe": round(self.avg_mfe, 4),
            "avg_risk_reward": round(self.avg_risk_reward, 4),
            "sharpe_ratio": round(self.sharpe_ratio, 4),
        }


class WinRateCalculator:
    """Calculate win rates and performance metrics."""
    
    def __init__(self, repository: MemoryRepository):
        self._repo = repository
    
    async def calculate_metrics(
        self,
        strategy_id: str | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        min_score: float = 0.0,
    ) -> PerformanceMetrics:
        """Calculate performance metrics.
        
        Args:
            strategy_id: Optional filter by strategy
            since: Start date for analysis
            until: End date for analysis
            min_score: Minimum signal score to include
            
        Returns:
            PerformanceMetrics with calculated values
        """
        # Default to last 30 days if not specified
        if since is None:
            since = datetime.utcnow() - timedelta(days=30)
        
        # Get signals with outcomes as DataFrame
        df = await self._repo.get_signals_for_analysis(since, min_score)
        
        if df.is_empty():
            return PerformanceMetrics()
        
        # Filter by strategy if specified
        if strategy_id:
            df = df.filter(pl.col("strategy_id") == strategy_id)
        
        if df.is_empty():
            return PerformanceMetrics()
        
        metrics = PerformanceMetrics()
        metrics.total_signals = df.height
        
        # Count results
        results = df["result"].value_counts()
        for row in results.iter_rows(named=True):
            result_type = row["result"]
            count = row["count"]
            
            if result_type == "win":
                metrics.wins = count
            elif result_type == "loss":
                metrics.losses = count
            else:
                metrics.breakeven = count
        
        # Calculate rates
        closed = metrics.wins + metrics.losses + metrics.breakeven
        if closed > 0:
            metrics.win_rate = metrics.wins / closed
            metrics.loss_rate = metrics.losses / closed
        
        # Calculate profit factor
        wins_df = df.filter(pl.col("result") == "win")
        losses_df = df.filter(pl.col("result") == "loss")
        
        if not wins_df.is_empty() and not losses_df.is_empty():
            total_wins = _as_float(wins_df["pnl_24h"].sum())
            total_losses = abs(_as_float(losses_df["pnl_24h"].sum()))
            
            if total_losses > 0:
                metrics.profit_factor = total_wins / total_losses
        
        # Average win/loss
        if not wins_df.is_empty():
            metrics.avg_win_pct = _as_float(wins_df["pnl_24h"].mean())
            metrics.max_win_pct = _as_float(wins_df["pnl_24h"].max())
        
        if not losses_df.is_empty():
            metrics.avg_loss_pct = _as_float(losses_df["pnl_24h"].mean())
            metrics.max_loss_pct = _as_float(losses_df["pnl_24h"].min())
        
        # MAE/MFE
        if "max_loss_pct" in df.columns:
            metrics.avg_mae = _as_float(df["max_loss_pct"].mean())
        if "max_profit_pct" in df.columns:
            metrics.avg_mfe = _as_float(df["max_profit_pct"].mean())
        
        # Simplified Sharpe (24h returns / std dev)
        pnl_col = df["pnl_24h"]
        if not pnl_col.is_null().all():
            returns = pnl_col.drop_nulls()
            if len(returns) > 1:
                mean_ret = _as_float(returns.mean())
                std_ret = _as_float(returns.std())
                if std_ret > 0:
                    metrics.sharpe_ratio = mean_ret / std_ret * np.sqrt(365)  # Annualized
        
        return metrics
    
    async def calculate_by_strategy(
        self,
        since: datetime | None = None,
    ) -> dict[str, PerformanceMetrics]:
        """Calculate metrics for each strategy."""
        if since is None:
            since = datetime.utcnow() - timedelta(days=30)
        
        df = await self._repo.get_signals_for_analysis(since)
        
        if df.is_empty():
            return {}
        
        strategies = df["strategy_id"].unique().to_list()
        
        results = {}
        for strategy_id in strategies:
            metrics = await self.calculate_metrics(strategy_id=strategy_id, since=since)
            results[strategy_id] = metrics
        
        return results
    
    async def get_winrate_trend(
        self,
        strategy_id: str | None = None,
        window_days: int = 7,
        periods: int = 4,
    ) -> list[dict[str, Any]]:
        """Get win rate trend over time.
        
        Args:
            strategy_id: Optional strategy filter
            window_days: Days per period
            periods: Number of periods
            
        Returns:
            List of period metrics
        """
        end = datetime.utcnow()
        results = []
        
        for i in range(periods):
            period_end = end - timedelta(days=i * window_days)
            period_start = period_end - timedelta(days=window_days)
            
            metrics = await self.calculate_metrics(
                strategy_id=strategy_id,
                since=period_start,
                until=period_end,
            )
            
            results.append({
                "period_start": period_start.isoformat(),
                "period_end": period_end.isoformat(),
                "metrics": metrics.to_dict(),
            })
        
        return list(reversed(results))
    
    def detect_degradation(
        self,
        current: PerformanceMetrics,
        baseline: PerformanceMetrics,
        winrate_threshold: float = 0.15,
        pf_threshold: float = 0.3,
    ) -> bool:
        """Detect if performance has degraded significantly.
        
        Returns True if degradation detected.
        """
        # Check win rate drop
        if baseline.win_rate > 0:
            winrate_drop = baseline.win_rate - current.win_rate
            if winrate_drop > winrate_threshold:
                LOG.warning(
                    "Win rate degradation detected: %.2f%% -> %.2f%%",
                    baseline.win_rate * 100,
                    current.win_rate * 100,
                )
                return True
        
        # Check profit factor drop
        if baseline.profit_factor > 0:
            pf_drop = baseline.profit_factor - current.profit_factor
            if pf_drop > pf_threshold:
                LOG.warning(
                    "Profit factor degradation: %.2f -> %.2f",
                    baseline.profit_factor,
                    current.profit_factor,
                )
                return True
        
        return False
