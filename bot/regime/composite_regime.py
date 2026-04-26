from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .gmm_var import GMMVARRegimeDetector
from .hmm_regime import HMMRegimeDetector


@dataclass(frozen=True)
class RegimeResult:
    regime: str
    strength: float
    confidence: float


class CompositeRegimeAnalyzer:
    def __init__(self) -> None:
        self.hmm = HMMRegimeDetector()
        self.gmm = GMMVARRegimeDetector()

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

        gmm_regime, gmm_conf = self.gmm.current_regime(
            {"returns": returns, "vol": vol, "funding_rate": funding}
        )
        if gmm_regime == "contagion":
            return RegimeResult(regime="volatile", strength=0.8, confidence=gmm_conf)
        if gmm_regime == "calm_up":
            return RegimeResult(regime="bull", strength=0.65, confidence=gmm_conf)
        if gmm_regime == "calm_down":
            return RegimeResult(regime="bear", strength=0.65, confidence=gmm_conf)
        return RegimeResult(regime="ranging", strength=0.45, confidence=gmm_conf)
