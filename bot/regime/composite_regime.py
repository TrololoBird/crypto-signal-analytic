from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import polars as pl

from .gmm_var import CentroidRegimeDetector
from .hmm_regime import RuleBasedRegimeDetector


@dataclass(frozen=True)
class RegimeResult:
    regime: str
    strength: float
    confidence: float


class CompositeRegimeAnalyzer:
    def __init__(self) -> None:
        self.rule_based = RuleBasedRegimeDetector()
        self.centroid = CentroidRegimeDetector()

    def analyze(
        self,
        ticker_data: list[dict[str, Any]],
        funding_rates: dict[str, float] | None,
        benchmark_context: dict[str, dict[str, Any]] | None,
    ) -> RegimeResult:
        benchmark_context = benchmark_context or {}
        btc = benchmark_context.get("BTCUSDT", {})
        returns = float(btc.get("basis_pct") or 0.0)
        vol = abs(float(btc.get("premium_slope_5m") or 0.0))
        funding = float(next(iter((funding_rates or {"x": 0.0}).values()))) if funding_rates else 0.0

        centroid_regime, centroid_conf = self.centroid.current_regime(
            {"returns": returns, "vol": vol, "funding_rate": funding}
        )
        rule_based_pred = self.rule_based.predict(
            self._build_rule_based_frame(benchmark_context=benchmark_context, returns=returns, vol=vol)
        )

        centroid_vote = self._map_centroid(centroid_regime)
        rule_based_vote = self._map_rule_based(rule_based_pred.regime)
        legacy_vote = self._legacy_vote(returns, vol, funding)

        vote_weights = {"centroid": 0.4, "rule_based": 0.4, "legacy": 0.2}
        weighted_scores: dict[str, float] = {"bull": 0.0, "bear": 0.0, "ranging": 0.0, "volatile": 0.0}
        weighted_scores[centroid_vote] += vote_weights["centroid"]
        weighted_scores[rule_based_vote] += vote_weights["rule_based"]
        weighted_scores[legacy_vote] += vote_weights["legacy"]

        regime = max(weighted_scores.items(), key=lambda item: item[1])[0]
        strength = max(0.45, min(0.9, weighted_scores[regime]))
        confidence = min(0.95, (centroid_conf * 0.5) + (rule_based_pred.confidence * 0.5))
        return RegimeResult(regime=regime, strength=strength, confidence=confidence)


    @property
    def gmm(self) -> CentroidRegimeDetector:
        """Backward-compatible alias for older tests/callers."""
        return self.centroid

    @property
    def hmm(self) -> RuleBasedRegimeDetector:
        """Backward-compatible alias for older tests/callers."""
        return self.rule_based

    @staticmethod
    def _build_rule_based_frame(
        *,
        benchmark_context: dict[str, dict[str, Any]],
        returns: float,
        vol: float,
    ) -> pl.DataFrame:
        btc = benchmark_context.get("BTCUSDT", {})
        history = btc.get("regime_frame_4h")
        if isinstance(history, pl.DataFrame) and not history.is_empty():
            required = {"log_returns", "realized_vol", "atr_pct"}
            if required.issubset(set(history.columns)):
                return history.select(sorted(required))

        return pl.DataFrame(
            {
                "log_returns": [returns],
                "realized_vol": [vol],
                "atr_pct": [abs(vol)],
            }
        )

    @staticmethod
    def _map_centroid(regime: str) -> str:
        if regime == "contagion":
            return "volatile"
        if regime == "calm_up":
            return "bull"
        if regime == "calm_down":
            return "bear"
        return "ranging"

    @staticmethod
    def _map_rule_based(regime: str) -> str:
        if regime == "high_vol_choppy":
            return "volatile"
        if regime == "low_vol_uptrend":
            return "bull"
        if regime == "low_vol_downtrend":
            return "bear"
        return "ranging"

    @staticmethod
    def _legacy_vote(returns: float, vol: float, funding: float) -> str:
        if vol >= 0.02:
            return "volatile"
        if returns > 0 and funding >= -0.0005:
            return "bull"
        if returns < 0 and funding <= 0.0005:
            return "bear"
        return "ranging"
