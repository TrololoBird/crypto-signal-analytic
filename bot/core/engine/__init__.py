"""Core engine for pluggable strategy system."""

from .registry import StrategyRegistry, StrategyMetadata
from .base import AbstractStrategy, SignalResult, StrategyDecision
from .engine import SignalEngine

__all__ = [
    "StrategyRegistry",
    "StrategyMetadata",
    "AbstractStrategy",
    "SignalResult",
    "StrategyDecision",
    "SignalEngine",
]
