from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np


@dataclass(frozen=True)
class GMMVARPrediction:
    regime: str
    confidence: float


class GMMVARRegimeDetector:
    def __init__(self, n_regimes: int = 2) -> None:
        self.n_regimes = n_regimes
        self._centroid = np.zeros(3, dtype=float)

    def fit(self, ticker_data: list[dict[str, Any]]) -> None:
        features = []
        for row in ticker_data:
            change = float(row.get("price_change_percent") or 0.0)
            vol = float(row.get("quoteVolume") or row.get("quote_volume") or 0.0)
            funding = float(row.get("funding_rate") or 0.0)
            features.append([change, np.log1p(max(vol, 0.0)), funding])
        if features:
            self._centroid = np.array(features, dtype=float).mean(axis=0)

    def current_regime(self, features: dict[str, float]) -> tuple[str, float]:
        change = float(features.get("returns", 0.0))
        vol = float(features.get("vol", 0.0))
        funding = float(features.get("funding_rate", 0.0))
        vec = np.array([change, np.log1p(max(vol, 0.0)), funding], dtype=float)
        dist = float(np.linalg.norm(vec - self._centroid))
        confidence = max(0.0, min(0.99, 1.0 / (1.0 + dist)))
        if vol > 0.04:
            return "contagion", confidence
        if change > 0:
            return "calm_up", confidence
        if change < 0:
            return "calm_down", confidence
        return "neutral", confidence
