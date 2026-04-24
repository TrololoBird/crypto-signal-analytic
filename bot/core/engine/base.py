"""Abstract base class for all strategies."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

import polars as pl

from ...models import PreparedSymbol, Signal
from ...config import BotSettings


@dataclass(frozen=True, slots=True)
class StrategyMetadata:
    """Metadata for strategy registration."""
    
    strategy_id: str
    name: str
    description: str = ""
    version: str = "1.0.0"
    author: str = ""
    tags: list[str] = field(default_factory=list)
    timeframes: list[str] = field(default_factory=lambda: ["5m", "15m", "1h"])
    family: str = "continuation"
    confirmation_profile: str = "trend_follow"
    required_context: tuple[str, ...] = ()
    requires_oi: bool = False  # Requires open interest data
    requires_funding: bool = False  # Requires funding rate data
    min_history_bars: int = 50  # Minimum bars needed for calculation
    
    def to_dict(self) -> dict[str, Any]:
        return {
            "strategy_id": self.strategy_id,
            "name": self.name,
            "description": self.description,
            "version": self.version,
            "author": self.author,
            "tags": self.tags,
            "timeframes": self.timeframes,
            "family": self.family,
            "confirmation_profile": self.confirmation_profile,
            "required_context": list(self.required_context),
            "requires_oi": self.requires_oi,
            "requires_funding": self.requires_funding,
            "min_history_bars": self.min_history_bars,
        }


@dataclass(frozen=True, slots=True)
class StrategyDecision:
    """Normalized strategy output used across engine, bot, and telemetry."""

    setup_id: str
    status: str
    stage: str
    reason_code: str
    signal: Signal | None = None
    details: dict[str, Any] = field(default_factory=dict)
    missing_fields: tuple[str, ...] = ()
    invalid_fields: tuple[str, ...] = ()
    error: str | None = None

    @property
    def is_signal(self) -> bool:
        return self.status == "signal" and self.signal is not None and self.error is None

    @property
    def is_reject(self) -> bool:
        return self.status == "reject"

    @property
    def is_error(self) -> bool:
        return self.status == "error" or self.error is not None

    @property
    def is_skip(self) -> bool:
        return self.status == "skip"

    @classmethod
    def signal_hit(
        cls,
        *,
        setup_id: str,
        signal: Signal,
        stage: str = "strategy",
        reason_code: str = "pattern.raw_hit",
        details: dict[str, Any] | None = None,
    ) -> "StrategyDecision":
        return cls(
            setup_id=setup_id,
            status="signal",
            stage=stage,
            reason_code=reason_code,
            signal=signal,
            details=dict(details or {}),
        )

    @classmethod
    def reject(
        cls,
        *,
        setup_id: str,
        stage: str,
        reason_code: str,
        details: dict[str, Any] | None = None,
        missing_fields: tuple[str, ...] = (),
        invalid_fields: tuple[str, ...] = (),
    ) -> "StrategyDecision":
        return cls(
            setup_id=setup_id,
            status="reject",
            stage=stage,
            reason_code=reason_code,
            details=dict(details or {}),
            missing_fields=missing_fields,
            invalid_fields=invalid_fields,
        )

    @classmethod
    def skip(
        cls,
        *,
        setup_id: str,
        reason_code: str,
        details: dict[str, Any] | None = None,
        missing_fields: tuple[str, ...] = (),
        invalid_fields: tuple[str, ...] = (),
    ) -> "StrategyDecision":
        return cls(
            setup_id=setup_id,
            status="skip",
            stage="engine",
            reason_code=reason_code,
            details=dict(details or {}),
            missing_fields=missing_fields,
            invalid_fields=invalid_fields,
        )

    @classmethod
    def error_result(
        cls,
        *,
        setup_id: str,
        reason_code: str,
        error: str,
        stage: str = "runtime",
        details: dict[str, Any] | None = None,
    ) -> "StrategyDecision":
        return cls(
            setup_id=setup_id,
            status="error",
            stage=stage,
            reason_code=reason_code,
            details=dict(details or {}),
            error=error,
        )


@dataclass
class SignalResult:
    """Result from strategy calculation."""

    setup_id: str
    signal: Signal | None
    decision: StrategyDecision | None = None
    confidence: float = 0.0  # 0.0-1.0
    metadata: dict[str, Any] = field(default_factory=dict)
    calculation_time_ms: float = 0.0
    error: str | None = None

    def __post_init__(self) -> None:
        if "setup_id" not in self.metadata:
            self.metadata["setup_id"] = self.setup_id
        if self.decision is None:
            if self.error is not None:
                self.decision = StrategyDecision.error_result(
                    setup_id=self.setup_id,
                    reason_code="runtime.error",
                    error=self.error,
                    details=dict(self.metadata),
                )
            elif self.signal is not None:
                self.decision = StrategyDecision.signal_hit(
                    setup_id=self.setup_id,
                    signal=self.signal,
                    details=dict(self.metadata),
                )
            else:
                self.decision = StrategyDecision.reject(
                    setup_id=self.setup_id,
                    stage="strategy",
                    reason_code="pattern.no_raw_hit",
                    details=dict(self.metadata),
                )
        if self.error is None and self.decision is not None and self.decision.error is not None:
            self.error = self.decision.error

    @property
    def is_valid(self) -> bool:
        return self.decision is not None and self.decision.is_signal


class AbstractStrategy(ABC):
    """Abstract base class for all trading strategies.
    
    All strategies must inherit from this class and implement:
    - metadata property
    - calculate() method
    - can_calculate() method
    """
    
    def __init__(self, settings: BotSettings | None = None):
        self._settings = settings
        self._parameters: dict[str, Any] = {}
    
    @property
    @abstractmethod
    def metadata(self) -> StrategyMetadata:
        """Return strategy metadata for registration."""
        pass
    
    @abstractmethod
    def calculate(self, prepared: PreparedSymbol) -> SignalResult:
        """Calculate signal for given prepared symbol data.
        
        Args:
            prepared: Prepared symbol data with indicators
            
        Returns:
            SignalResult with signal or None if no setup
        """
        pass
    
    def can_calculate(self, prepared: PreparedSymbol) -> bool:
        """Check if strategy can calculate with available data.
        
        Override for custom validation (OI data, funding, etc.)
        """
        if prepared.work_1h is None or prepared.work_1h.is_empty():
            return False
            
        if prepared.work_1h.height < self.metadata.min_history_bars:
            return False
            
        if self.metadata.requires_oi and prepared.oi_current is None:
            return False
            
        if self.metadata.requires_funding and prepared.funding_rate is None:
            return False
            
        return True
    
    def update_parameters(self, parameters: dict[str, Any]) -> None:
        """Hot-update strategy parameters from optimizer."""
        self._parameters.update(parameters)
    
    def get_parameter(self, name: str, default: Any = None) -> Any:
        """Get parameter value with default."""
        return self._parameters.get(name, default)
    
    @property
    def strategy_id(self) -> str:
        return self.metadata.strategy_id
    
    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} id={self.strategy_id}>"
