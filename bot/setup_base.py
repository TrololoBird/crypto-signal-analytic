"""Base classes for all trading setup detectors."""
from __future__ import annotations

from abc import abstractmethod
from dataclasses import dataclass
from typing import Any

from .config import BotSettings
from .core.engine.base import AbstractStrategy, SignalResult, StrategyDecision, StrategyMetadata
from .models import PreparedSymbol, Signal
from .setups import (
    begin_strategy_decision_capture,
    finalize_strategy_decision,
    reset_strategy_decision_capture,
)


@dataclass(frozen=True)
class SetupParams:
    """Per-setup configuration parameters."""
    enabled: bool = True


class BaseSetup(AbstractStrategy):
    """Base setup class compatible with the modern signal engine."""

    setup_id: str  # class-level constant, defined by each subclass
    family: str = "continuation"
    confirmation_profile: str = "trend_follow"
    required_context: tuple[str, ...] = ()
    requires_oi: bool = False
    requires_funding: bool = False
    min_history_bars: int = 50

    def __init__(self, params: SetupParams | None = None, settings: BotSettings | None = None) -> None:
        super().__init__(settings)
        self.params = params or SetupParams()

    def is_enabled(self) -> bool:
        return self.params.enabled

    @property
    def metadata(self) -> StrategyMetadata:
        return StrategyMetadata(
            strategy_id=self.setup_id,
            name=self.setup_id.replace("_", " ").title(),
            description=f"{self.setup_id} setup",
            family=self.family,
            confirmation_profile=self.confirmation_profile,
            required_context=self.required_context,
            requires_oi=self.requires_oi,
            requires_funding=self.requires_funding,
            min_history_bars=self.min_history_bars,
        )

    @abstractmethod
    def detect(
        self,
        prepared: PreparedSymbol,
        settings: BotSettings,
    ) -> StrategyDecision | Signal | None:
        """Run detection logic."""
        ...

    @abstractmethod
    def get_optimizable_params(self, settings: "BotSettings | None" = None) -> dict[str, float]:
        """Return tunable parameters for self-learner Optuna optimization."""
        ...

    def calculate(self, prepared: PreparedSymbol) -> SignalResult:
        if self._settings is None:
            decision = StrategyDecision.error_result(
                setup_id=self.setup_id,
                reason_code="runtime.missing_settings",
                error=f"{self.setup_id} missing BotSettings",
                details={"symbol": prepared.symbol},
            )
            return SignalResult(
                setup_id=self.setup_id,
                signal=None,
                decision=decision,
                error=decision.error,
                metadata={"setup_id": self.setup_id},
            )
        runtime = getattr(self._settings, "runtime", None)
        strict_data_quality = bool(getattr(runtime, "strict_data_quality", True))
        token = begin_strategy_decision_capture(
            prepared=prepared,
            setup_id=self.setup_id,
            strict_data_quality=strict_data_quality,
        )
        try:
            outcome = self.detect(prepared, self._settings)
            decision = finalize_strategy_decision(
                prepared=prepared,
                setup_id=self.setup_id,
                outcome=outcome,
            )
        finally:
            reset_strategy_decision_capture(token)
        return SignalResult(
            setup_id=self.setup_id,
            signal=decision.signal,
            decision=decision,
            error=decision.error,
            metadata={"setup_id": self.setup_id},
        )

    def can_calculate(self, prepared: PreparedSymbol) -> bool:
        return self.is_enabled() and super().can_calculate(prepared)
