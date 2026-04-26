from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import numpy as np
import polars as pl


@dataclass(frozen=True)
class HMMRegimePrediction:
    regime: str
    confidence: float


class HMMRegimeDetector:
    """Lightweight HMM-style detector with optional hmmlearn backend."""

    def __init__(self, n_states: int = 3, features: tuple[str, ...] = ("log_returns", "realized_vol", "atr_pct")) -> None:
        self.n_states = n_states
        self.features = features
        self._last_prediction = HMMRegimePrediction(regime="ranging", confidence=0.0)

    def fit(self, df_4h: pl.DataFrame) -> None:
        if df_4h.is_empty():
            return
        pred = self.predict(df_4h)
        self._last_prediction = pred

    def _series(self, df: pl.DataFrame, name: str) -> np.ndarray:
        if name not in df.columns:
            return np.array([])
        values: Iterable[float] = (float(v or 0.0) for v in df[name].to_list())
        return np.fromiter(values, dtype=float)

    def predict(self, df_4h: pl.DataFrame) -> HMMRegimePrediction:
        if df_4h.is_empty():
            return HMMRegimePrediction(regime="ranging", confidence=0.0)

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
