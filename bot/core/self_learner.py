"""Self-learning module for automated setup parameter optimization.

Uses Optuna for nightly optimization studies based on outcomes from aiosqlite.
Each setup's get_optimizable_params() provides the search space.

# WINDSURF_REVIEW: unified + self-learning + Optuna
"""
from __future__ import annotations

import asyncio
import importlib
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, cast

from ..learning import OutcomeStore, RegimeAwareParams, WalkForwardOptimizer

try:
    optuna = importlib.import_module("optuna")
    OPTUNA_AVAILABLE = True
except ImportError:
    optuna = None
    OPTUNA_AVAILABLE = False

if TYPE_CHECKING:
    from .engine import StrategyRegistry
    from ..config import BotSettings

LOG = logging.getLogger("bot.core.self_learner")


@dataclass
class OptimizationResult:
    """Result of a single setup parameter optimization."""
    setup_id: str
    params: dict[str, float]
    win_rate: float
    profit_factor: float
    total_trades: int
    optimized_at: datetime


class SelfLearner:
    """Nightly optimization engine for setup parameters using Optuna."""

    def __init__(
        self,
        setup_registry: StrategyRegistry,
        db_path: str = "data/bot/optimization.db",
        n_trials: int = 100,
    ):
        self.setup_registry = setup_registry
        self.db_path = db_path
        self.n_trials = n_trials
        self._study_cache: dict[str, Any | None] = {}
        self._running = False
        self._outcomes = OutcomeStore(db_path)
        self._walk_forward = WalkForwardOptimizer()

    async def run_nightly_study(self, settings: BotSettings) -> list[OptimizationResult]:
        """Run optimization study for all setups with sufficient outcome data.
        
        Returns list of OptimizationResult with optimized parameters.
        """
        if not OPTUNA_AVAILABLE:
            LOG.warning("Optuna not available, skipping optimization")
            return []

        results: list[OptimizationResult] = []
        
        for setup_instance in self.setup_registry.get_enabled():
            setup_id = setup_instance.strategy_id
            try:
                # Get optimizable params from setup
                default_params = cast(Any, setup_instance).get_optimizable_params()
                if not default_params:
                    continue

                # Get outcomes from database for this setup
                outcomes = await self._fetch_outcomes(setup_id)
                if len(outcomes) < 30:  # Need minimum data
                    LOG.info(f"Insufficient outcomes for {setup_id}: {len(outcomes)} trades")
                    continue

                # Run Optuna study
                regime = str(getattr(getattr(settings, "intelligence", None), "regime_detector", "legacy"))
                result = await self._optimize_setup(setup_id, default_params, outcomes, regime=regime)
                if result:
                    results.append(result)
                    
            except Exception as e:
                LOG.exception(f"Error optimizing {setup_id}: {e}")
                continue

        return results

    async def _fetch_outcomes(self, setup_id: str) -> list[dict[str, Any]]:
        """Fetch historical outcomes for a setup from aiosqlite."""
        return await self._outcomes.fetch_setup_outcomes(setup_id, limit=500)

    async def _optimize_setup(
        self,
        setup_id: str,
        default_params: dict[str, float],
        outcomes: list[dict[str, Any]],
        *,
        regime: str,
    ) -> OptimizationResult | None:
        """Run Optuna optimization for a single setup."""
        regime_bounds = RegimeAwareParams(regime=regime)
        
        def objective(trial: Any) -> float:
            """Optimization objective: maximize risk-adjusted return."""
            # Sample parameters from search space
            suggested_params = {}
            for param_name, default_value in default_params.items():
                if "score" in param_name or "threshold" in param_name or "penalty" in param_name:
                    # These are typically 0.0-1.0 or small ranges
                    low, high = regime_bounds.scale(param_name, float(default_value))
                    suggested_params[param_name] = trial.suggest_float(param_name, low, high)
                elif "min_rr" in param_name:
                    # Risk/reward ratio typically 1.0-3.0
                    low, high = regime_bounds.scale(param_name, float(default_value))
                    suggested_params[param_name] = trial.suggest_float(param_name, max(0.8, low), min(4.0, high))
                elif "bars" in param_name or "age" in param_name or "lookback" in param_name:
                    # Integer parameters
                    low, high = int(default_value * 0.5), int(default_value * 2.0)
                    suggested_params[param_name] = trial.suggest_int(param_name, max(1, low), max(2, high))
                else:
                    # Generic float parameter
                    low, high = regime_bounds.scale(param_name, float(default_value))
                    suggested_params[param_name] = trial.suggest_float(param_name, low, high)
            
            # Simulate performance with these parameters
            return self._simulate_performance(outcomes, suggested_params)

        # Create or load study
        study_name = f"{setup_id}_optimization"
        try:
            if optuna is None:
                return None
            study = cast(Any, optuna).create_study(
                study_name=study_name,
                storage=f"sqlite:///{self.db_path}",
                direction="maximize",
                load_if_exists=True,
            )
        except Exception as e:
            LOG.error(f"Failed to create study for {setup_id}: {e}")
            return None

        # Run optimization
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, study.optimize, objective, self.n_trials)
        except Exception as e:
            LOG.error(f"Optimization failed for {setup_id}: {e}")
            return None

        # Get best parameters
        best_params = study.best_params
        best_value = study.best_value

        # Calculate metrics
        wins = sum(1 for o in outcomes if o.get("outcome") == "win")
        losses = sum(1 for o in outcomes if o.get("outcome") == "loss")
        total = wins + losses
        win_rate = wins / total if total > 0 else 0.0

        # Calculate profit factor
        gross_profit = sum(o.get("pnl", 0) for o in outcomes if o.get("outcome") == "win")
        gross_loss = abs(sum(o.get("pnl", 0) for o in outcomes if o.get("outcome") == "loss"))
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else float('inf')

        LOG.info(
            f"Optimized {setup_id}: win_rate={win_rate:.2%}, "
            f"profit_factor={profit_factor:.2f}, score={best_value:.4f}"
        )

        return OptimizationResult(
            setup_id=setup_id,
            params=best_params,
            win_rate=win_rate,
            profit_factor=profit_factor,
            total_trades=total,
            optimized_at=datetime.now(timezone.utc),
        )

    def _simulate_performance(
        self,
        outcomes: list[dict[str, Any]],
        params: dict[str, float],
    ) -> float:
        """Simulate performance with walk-forward evaluation."""
        return self._walk_forward.evaluate(outcomes, params)

    async def update_setup_params(
        self,
        results: list[OptimizationResult],
        settings: BotSettings,
    ) -> None:
        """Update setup parameters in settings after optimization."""
        for result in results:
            try:
                # Update settings.filters.setups[setup_id]
                if not hasattr(settings.filters, 'setups'):
                    settings.filters.setups = {}
                
                settings.filters.setups[result.setup_id] = result.params
                
                LOG.info(
                    f"Updated {result.setup_id} params: {result.params} "
                    f"(win_rate={result.win_rate:.2%})"
                )
                
            except Exception as e:
                LOG.error(f"Failed to update params for {result.setup_id}: {e}")

    async def start_nightly_scheduler(self, settings: BotSettings) -> None:
        """Start background task for nightly optimization."""
        self._running = True
        
        while self._running:
            try:
                # Run at 00:00 UTC
                now = datetime.now(timezone.utc)
                next_run = now.replace(hour=0, minute=0, second=0, microsecond=0)
                if next_run <= now:
                    next_run = next_run.replace(day=next_run.day + 1)
                
                wait_seconds = (next_run - now).total_seconds()
                LOG.info(f"Next optimization run at {next_run} (in {wait_seconds/3600:.1f} hours)")
                
                await asyncio.sleep(wait_seconds)
                
                # Run optimization
                LOG.info("Starting nightly optimization study...")
                results = await self.run_nightly_study(settings)
                await self.update_setup_params(results, settings)
                LOG.info(f"Nightly optimization complete: {len(results)} setups optimized")
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                LOG.exception(f"Error in nightly scheduler: {e}")
                await asyncio.sleep(3600)  # Retry in 1 hour

    def stop(self) -> None:
        """Stop the nightly scheduler."""
        self._running = False


# Singleton instance
_self_learner: SelfLearner | None = None


def get_self_learner(
    setup_registry: StrategyRegistry | None = None,
    db_path: str = "data/bot/optimization.db",
) -> SelfLearner:
    """Get or create singleton SelfLearner instance."""
    global _self_learner
    if _self_learner is None:
        if setup_registry is None:
            raise ValueError("setup_registry required for initial creation")
        _self_learner = SelfLearner(setup_registry, db_path)
    return _self_learner
