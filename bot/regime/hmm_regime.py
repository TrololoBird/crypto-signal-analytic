from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import numpy as np
from numpy.typing import NDArray
import polars as pl

try:  # optional dependency
    from hmmlearn.hmm import GaussianHMM
except Exception:  # pragma: no cover - optional
    GaussianHMM = None


@dataclass(frozen=True)
class HMMRegimePrediction:
    regime: str
    confidence: float


class RuleBasedRegimeDetector:
    """Rule-based market state detector (lightweight fallback path)."""

    def __init__(self, n_states: int = 3, features: tuple[str, ...] = ("log_returns", "realized_vol", "atr_pct")) -> None:
        self.n_states = n_states
        self.features = features
        self._last_prediction = HMMRegimePrediction(regime="ranging", confidence=0.0)
        self._hmm_model = None
        self._state_to_regime: dict[int, str] = {}

    def fit(self, df_4h: pl.DataFrame) -> None:
        if df_4h.is_empty():
            return
        matrix = self._feature_matrix(df_4h)
        if matrix.shape[0] >= max(20, self.n_states * 5) and GaussianHMM is not None:
            try:
                model = GaussianHMM(
                    n_components=self.n_states,
                    covariance_type="full",
                    n_iter=200,
                    random_state=42,
                )
                model.fit(matrix)
                states = model.predict(matrix)
                self._state_to_regime = self._infer_state_regimes(matrix, states)
                self._hmm_model = model
            except Exception:
                self._hmm_model = None
                self._state_to_regime = {}
        pred = self.predict(df_4h)
        self._last_prediction = pred

    def _series(self, df: pl.DataFrame, name: str) -> NDArray[np.float64]:
        if name not in df.columns:
            return np.array([])
        values: Iterable[float] = (float(v or 0.0) for v in df[name].to_list())
        return np.fromiter(values, dtype=float)

    def predict(self, df_4h: pl.DataFrame) -> HMMRegimePrediction:
        if df_4h.is_empty():
            return HMMRegimePrediction(regime="ranging", confidence=0.0)
        if self._hmm_model is not None:
            try:
                matrix = self._feature_matrix(df_4h)
                if matrix.shape[0] > 0:
                    states = self._hmm_model.predict(matrix)
                    state = int(states[-1])
                    probs = self._hmm_model.predict_proba(matrix)
                    confidence = float(probs[-1][state]) if probs.shape[0] else 0.5
                    regime = self._state_to_regime.get(state, "ranging")
                    return HMMRegimePrediction(regime=regime, confidence=max(0.0, min(0.99, confidence)))
            except Exception:
                pass

        return self._rule_predict(df_4h)

    def _rule_predict(self, df_4h: pl.DataFrame) -> HMMRegimePrediction:
        returns = self._series(df_4h, "log_returns")
        if returns.size == 0 and "close" in df_4h.columns and df_4h.height > 2:
            close = self._series(df_4h, "close")
            returns = np.diff(np.log(np.clip(close, 1e-9, None)))

        vol = self._series(df_4h, "realized_vol")
        if vol.size == 0:
            vol = np.abs(returns)

        ret = float(returns[-1]) if returns.size else 0.0
        vol_now = float(vol[-1]) if vol.size else 0.0
        vol_ref = float(np.nanmean(vol[-48:])) if vol.size else 0.0
        high_vol = vol_now > max(vol_ref * 1.25, 1e-6)

        if high_vol:
            return HMMRegimePrediction(regime="high_vol_choppy", confidence=min(0.95, 0.5 + vol_now))
        if ret > 0:
            return HMMRegimePrediction(regime="low_vol_uptrend", confidence=min(0.95, 0.55 + abs(ret) * 20))
        if ret < 0:
            return HMMRegimePrediction(regime="low_vol_downtrend", confidence=min(0.95, 0.55 + abs(ret) * 20))
        return HMMRegimePrediction(regime="ranging", confidence=0.5)

    def _feature_matrix(self, df_4h: pl.DataFrame) -> NDArray[np.float64]:
        matrix_cols: list[NDArray[np.float64]] = []
        for feature in self.features:
            col = self._series(df_4h, feature)
            if col.size == 0:
                col = np.zeros(df_4h.height, dtype=float)
            matrix_cols.append(col)
        matrix = np.vstack(matrix_cols).T if matrix_cols else np.empty((0, 0), dtype=float)
        matrix = np.nan_to_num(matrix, nan=0.0, posinf=0.0, neginf=0.0).astype(np.float64, copy=False)
        return matrix

    @staticmethod
    def _infer_state_regimes(matrix: np.ndarray, states: np.ndarray) -> dict[int, str]:
        mapping: dict[int, str] = {}
        if matrix.shape[1] == 0:
            return mapping
        for state in np.unique(states):
            subset = matrix[states == state]
            if subset.size == 0:
                continue
            mean_ret = float(np.mean(subset[:, 0]))
            mean_vol = float(np.mean(np.abs(subset[:, 1]))) if subset.shape[1] > 1 else 0.0
            if mean_vol > 0.02:
                mapping[int(state)] = "high_vol_choppy"
            elif mean_ret > 0:
                mapping[int(state)] = "low_vol_uptrend"
            elif mean_ret < 0:
                mapping[int(state)] = "low_vol_downtrend"
            else:
                mapping[int(state)] = "ranging"
        return mapping


class HMMRegimeDetector(RuleBasedRegimeDetector):
    """Backward-compatible alias kept for config compatibility."""
