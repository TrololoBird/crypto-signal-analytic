from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np

try:  # optional dependencies
    from sklearn.mixture import GaussianMixture
except Exception:  # pragma: no cover
    GaussianMixture = None

try:
    from statsmodels.tsa.api import VAR
except Exception:  # pragma: no cover
    VAR = None


@dataclass(frozen=True)
class GMMVARPrediction:
    regime: str
    confidence: float


class CentroidRegimeDetector:
    """Rule/centroid-based detector used as a lightweight fallback."""

    def __init__(self, n_regimes: int = 2) -> None:
        self.n_regimes = max(2, n_regimes)
        self._centroid = np.zeros(3, dtype=float)
        self._gmm = None
        self._component_labels: dict[int, str] = {}
        self._var = None

    def fit(self, ticker_data: list[dict[str, Any]]) -> None:
        features = []
        for row in ticker_data:
            change = float(row.get("price_change_percent") or 0.0)
            vol = float(row.get("quoteVolume") or row.get("quote_volume") or 0.0)
            funding = float(row.get("funding_rate") or 0.0)
            features.append([change, np.log1p(max(vol, 0.0)), funding])
        if features:
            matrix = np.array(features, dtype=float)
            self._centroid = matrix.mean(axis=0)
            if GaussianMixture is not None and matrix.shape[0] >= self.n_regimes * 5:
                try:
                    gmm_model = GaussianMixture(n_components=self.n_regimes, random_state=42)
                    gmm_model.fit(matrix)
                    self._gmm = gmm_model
                    self._component_labels = self._label_components(gmm_model.means_)
                except Exception:
                    self._gmm = None
                    self._component_labels = {}
            if VAR is not None and matrix.shape[0] >= 30:
                try:
                    series = matrix[:, :2]  # returns + log-vol
                    self._var = VAR(series).fit(maxlags=2)
                except Exception:
                    self._var = None

    def current_regime(self, features: dict[str, float]) -> tuple[str, float]:
        change = float(features.get("returns", 0.0))
        vol = float(features.get("vol", 0.0))
        funding = float(features.get("funding_rate", 0.0))
        vec = np.array([change, np.log1p(max(vol, 0.0)), funding], dtype=float)
        if self._gmm is not None:
            try:
                probs = self._gmm.predict_proba([vec])[0]
                comp = int(np.argmax(probs))
                regime = self._component_labels.get(comp, "neutral")
                confidence = float(np.max(probs))
                regime = self._adjust_with_var(regime)
                return regime, max(0.0, min(0.99, confidence))
            except Exception:
                pass
        dist = float(np.linalg.norm(vec - self._centroid))
        confidence = max(0.0, min(0.99, 1.0 / (1.0 + dist)))
        if vol > 0.04:
            return "contagion", confidence
        if change > 0:
            return "calm_up", confidence
        if change < 0:
            return "calm_down", confidence
        return "neutral", confidence

    @staticmethod
    def _label_components(means: np.ndarray) -> dict[int, str]:
        labels: dict[int, str] = {}
        for idx, center in enumerate(means):
            ret = float(center[0])
            vol = float(center[1])
            if vol > np.percentile(means[:, 1], 70):
                labels[idx] = "contagion"
            elif ret > 0:
                labels[idx] = "calm_up"
            elif ret < 0:
                labels[idx] = "calm_down"
            else:
                labels[idx] = "neutral"
        return labels

    def _adjust_with_var(self, regime: str) -> str:
        if self._var is None:
            return regime
        try:
            forecast = self._var.forecast(self._var.endog[-self._var.k_ar :], steps=1)[0]
            next_ret = float(forecast[0])
            if regime == "neutral" and next_ret > 0:
                return "calm_up"
            if regime == "neutral" and next_ret < 0:
                return "calm_down"
            return regime
        except Exception:
            return regime


class GMMVARRegimeDetector(CentroidRegimeDetector):
    """Backward-compatible alias for older config/API names."""
