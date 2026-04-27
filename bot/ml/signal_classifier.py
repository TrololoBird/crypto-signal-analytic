from __future__ import annotations

import math
from pathlib import Path
from typing import Any

import polars as pl

try:
    import joblib
except ImportError:  # pragma: no cover - optional dependency fallback
    joblib = None


class _CentroidModel:
    """Baseline centroid-distance classifier (fallback when boosted/tree models unavailable)."""

    def __init__(self) -> None:
        self.mean_pos: list[float] = []
        self.mean_neg: list[float] = []
        self.feature_importances_: list[float] = []

    def fit(self, x: list[list[float]] | list[tuple[float, ...]], y: list[int]) -> None:
        rows = [list(row) for row in x]
        if not rows:
            return
        cols = len(rows[0])
        pos = [row for row, label in zip(rows, y, strict=False) if label == 1]
        neg = [row for row, label in zip(rows, y, strict=False) if label == 0]
        self.mean_pos = [sum(row[i] for row in pos) / max(len(pos), 1) for i in range(cols)]
        self.mean_neg = [sum(row[i] for row in neg) / max(len(neg), 1) for i in range(cols)]
        raw = [abs(self.mean_pos[i] - self.mean_neg[i]) for i in range(cols)]
        denom = sum(raw) or 1.0
        self.feature_importances_ = [v / denom for v in raw]

    def predict_proba(self, x: list[list[float]]) -> list[list[float]]:
        result: list[list[float]] = []
        for row in x:
            d_pos = sum((row[i] - self.mean_pos[i]) ** 2 for i in range(len(row)))
            d_neg = sum((row[i] - self.mean_neg[i]) ** 2 for i in range(len(row)))
            score = 1.0 / (1.0 + (d_pos / max(d_neg, 1e-9)))
            score = max(0.0, min(1.0, score))
            result.append([1.0 - score, score])
        return result


class _LinearFallbackModel:
    """Small dependency-free linear classifier fallback.

    Used when sklearn is unavailable for non-centroid model types.
    """

    def __init__(self) -> None:
        self.weights: list[float] = []
        self.bias: float = 0.0
        self.feature_importances_: list[float] = []

    def fit(self, x: list[list[float]] | list[tuple[float, ...]], y: list[int]) -> None:
        rows = [list(row) for row in x]
        if not rows:
            return
        cols = len(rows[0])
        pos = [row for row, label in zip(rows, y, strict=False) if label == 1]
        neg = [row for row, label in zip(rows, y, strict=False) if label == 0]
        mean_pos = [sum(row[i] for row in pos) / max(len(pos), 1) for i in range(cols)]
        mean_neg = [sum(row[i] for row in neg) / max(len(neg), 1) for i in range(cols)]

        self.weights = [mean_pos[i] - mean_neg[i] for i in range(cols)]
        midpoint = [(mean_pos[i] + mean_neg[i]) * 0.5 for i in range(cols)]
        self.bias = -sum(w * m for w, m in zip(self.weights, midpoint, strict=False))

        raw = [abs(w) for w in self.weights]
        denom = sum(raw) or 1.0
        self.feature_importances_ = [v / denom for v in raw]

    def predict_proba(self, x: list[list[float]]) -> list[list[float]]:
        if not self.weights:
            return [[0.5, 0.5] for _ in x]
        result: list[list[float]] = []
        for row in x:
            score = sum(w * v for w, v in zip(self.weights, row, strict=False)) + self.bias
            # numerically stable sigmoid
            if score >= 0:
                exp_term = math.exp(-score)
                p1 = 1.0 / (1.0 + exp_term)
            else:
                exp_term = math.exp(score)
                p1 = exp_term / (1.0 + exp_term)
            p1 = max(0.0, min(1.0, p1))
            result.append([1.0 - p1, p1])
        return result


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
        self.model: Any | None = None
        self._feature_names: list[str] = list(self.FEATURES)
        self.model_path = self.model_dir / f"{self.model_type}_signal_classifier.joblib"

    def train(self, df_features: pl.DataFrame, labels: pl.Series) -> None:
        if df_features.is_empty() or labels.len() == 0:
            raise ValueError("empty training data")
        frame = self._align_features(df_features)
        y = labels.cast(pl.Int8).to_list()
        if len(set(y)) < 2:
            raise ValueError("need at least 2 classes for training")

        model = self._build_model()
        try:
            if isinstance(model, _CentroidModel):
                model.fit(frame.rows(), y)
            else:
                model.fit(frame.to_numpy(), y)
        except Exception:
            model = _CentroidModel()
            model.fit(frame.rows(), y)
        self.model = model
        self.model_dir.mkdir(parents=True, exist_ok=True)
        payload = {"model": model, "features": self._feature_names}
        if joblib is not None:
            joblib.dump(payload, self.model_path)
        else:
            import pickle

            with self.model_path.open("wb") as handle:
                pickle.dump(payload, handle)

    def load(self) -> bool:
        if not self.model_path.exists():
            return False
        if joblib is not None:
            payload = joblib.load(self.model_path)
        else:
            import pickle

            with self.model_path.open("rb") as handle:
                payload = pickle.load(handle)
        self.model = payload.get("model")
        features = payload.get("features")
        if isinstance(features, list) and features:
            self._feature_names = [str(v) for v in features]
        return self.model is not None

    def model_kind(self) -> str:
        """Return normalized model kind for runtime guardrails/telemetry."""
        if self.model is None and not self.load():
            return "unloaded"
        if isinstance(self.model, _CentroidModel):
            return "centroid_baseline"
        return type(self.model).__name__.lower() if self.model is not None else "unloaded"

    def predict_proba(self, feature_vector: dict[str, float]) -> float:
        if self.model is None and not self.load():
            return 0.5
        vector = [float(feature_vector.get(name, 0.0)) for name in self._feature_names]
        if self.model is None:
            return 0.5
        pred = self.model.predict_proba([vector])[0][1]
        return float(max(0.0, min(1.0, pred)))

    def get_feature_importance(self) -> dict[str, float]:
        if self.model is None and not self.load():
            return {name: 0.0 for name in self._feature_names}
        importances = getattr(self.model, "feature_importances_", None)
        if importances is None:
            return {name: 0.0 for name in self._feature_names}
        return {
            name: float(value)
            for name, value in zip(self._feature_names, importances, strict=False)
        }

    def _align_features(self, df_features: pl.DataFrame) -> pl.DataFrame:
        frame = df_features
        for col in self._feature_names:
            if col not in frame.columns:
                frame = frame.with_columns([pl.lit(0.0).alias(col)])
        return frame.select(self._feature_names).cast(pl.Float64).fill_null(0.0)

    def _build_model(self) -> Any:
        model_key = self.model_type.strip().lower()
        if model_key in {"centroid", "fallback"}:
            return _CentroidModel()
        if model_key in {"rf", "random_forest"}:
            return self._build_model_fallback()
        if model_key in {"lightgbm", "lgbm"}:
            try:
                from lightgbm import LGBMClassifier

                return LGBMClassifier(
                    n_estimators=250,
                    learning_rate=0.05,
                    num_leaves=31,
                    subsample=0.9,
                    colsample_bytree=0.9,
                    random_state=42,
                )
            except Exception:
                return self._build_model_fallback()
        if model_key in {"xgboost", "xgb"}:
            try:
                from xgboost import XGBClassifier

                return XGBClassifier(
                    n_estimators=250,
                    learning_rate=0.05,
                    max_depth=4,
                    subsample=0.9,
                    colsample_bytree=0.9,
                    eval_metric="logloss",
                    random_state=42,
                    n_jobs=1,
                )
            except Exception:
                return self._build_model_fallback()
        return self._build_model_fallback()

    @staticmethod
    def _build_model_fallback() -> Any:
        try:
            from sklearn.ensemble import RandomForestClassifier

            return RandomForestClassifier(
                n_estimators=200,
                max_depth=8,
                min_samples_leaf=4,
                random_state=42,
                n_jobs=1,
            )
        except Exception:
            return _LinearFallbackModel()
