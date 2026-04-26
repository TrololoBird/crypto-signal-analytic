from __future__ import annotations

from pathlib import Path

import polars as pl


class SignalClassifier:
    FEATURES = [
        "atr_pct",
        "adx14",
        "rsi14",
        "macd_hist",
        "volume_ratio20",
        "delta_ratio",
        "oi_change_pct",
        "funding_rate",
        "mark_index_spread_bps",
        "premium_zscore_5m",
        "ls_ratio",
        "taker_ratio",
        "close_position",
        "bb_pct_b",
        "supertrend_dir",
        "regime_confidence",
        "mtf_alignment_score",
    ]

    def __init__(self, model_dir: Path, model_type: str = "lightgbm") -> None:
        self.model_dir = model_dir
        self.model_type = model_type
        self.model = None

    def train(self, df_features: pl.DataFrame, labels: pl.Series) -> None:
        if df_features.is_empty() or labels.len() == 0:
            raise ValueError("empty training data")

    def predict_proba(self, feature_vector: dict[str, float]) -> float:
        _ = feature_vector
        return 0.5

    def get_feature_importance(self) -> dict[str, float]:
        return {name: 0.0 for name in self.FEATURES}
