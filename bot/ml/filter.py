"""ML Filter module for live signal scoring enhancement.

Provides ML-based signal enhancement by loading trained models and applying
probability predictions. ML scoring is integrated into ConfluenceEngine.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import polars as pl

if TYPE_CHECKING:
    from ..config import BotSettings
    from ..models import PreparedSymbol, Signal

LOG = logging.getLogger("bot.ml.filter")


@dataclass(frozen=True)
class MLInferenceResult:
    """Result from ML model inference."""

    probability: float
    confidence: float
    feature_vector: dict[str, float]
    model_version: str
    error: str | None = None

    @property
    def is_confident(self) -> bool:
        """Check if prediction meets confidence threshold."""
        return self.confidence >= 0.5 and self.error is None


class MLFilter:
    """ML-based signal filter for live scoring enhancement.

    Loads trained models from disk and applies probability predictions
    as soft multipliers to scoring results when enabled.
    """

    def __init__(self, settings: BotSettings) -> None:
        self.settings = settings
        self.enabled = settings.ml.enabled and settings.ml.use_ml_in_live
        self.confidence_threshold = settings.ml.ml_confidence_threshold
        self.model_dir = settings.ml_dir / "models"
        self._model: Any | None = None
        self._model_metadata: dict[str, Any] | None = None
        self._feature_columns: list[str] | None = None
        self._classifier: Any | None = None

        if self.enabled:
            self._load_model()

    def _load_model(self) -> None:
        """Load the latest trained model from disk."""
        try:
            metadata: dict[str, Any] = {}
            # Try to load the latest model
            pattern = f"{self.settings.ml.model_type}_binary_*.joblib"
            model_files = sorted(self.model_dir.glob(pattern), reverse=True)

            if not model_files:
                # Fallback to lightweight signal classifier artifact
                from .signal_classifier import SignalClassifier

                classifier = SignalClassifier(self.model_dir, model_type=self.settings.ml.model_type)
                if classifier.load():
                    model_kind = classifier.model_kind()
                    if model_kind == "centroid_baseline":
                        LOG.warning(
                            "ML filter fallback resolved to centroid baseline model; "
                            "live ML scoring is disabled for safety"
                        )
                        self.enabled = False
                        return
                    self._classifier = classifier
                    self._model_metadata = {"trained_at": "signal_classifier_artifact", "model_kind": model_kind}
                    LOG.info("ML filter loaded SignalClassifier fallback from %s", classifier.model_path)
                    return
                LOG.warning("No trained model found in %s, ML filter disabled", self.model_dir)
                self.enabled = False
                return

            latest_file = model_files[0]

            # Check hash file for integrity
            hash_file = latest_file.with_suffix(".sha256")
            if hash_file.exists():
                import hashlib

                expected = hash_file.read_text(encoding="utf-8").strip()
                actual = hashlib.sha256(latest_file.read_bytes()).hexdigest()
                if expected != actual:
                    LOG.error("Model integrity check failed, ML filter disabled")
                    self.enabled = False
                    return

            # Load model
            import joblib

            data = joblib.load(latest_file)
            self._model = data["model"]
            self._model_metadata = data.get("metrics", {})
            metadata = self._model_metadata or {}

            # Try to load feature importance to determine feature columns
            feature_file = latest_file.parent / f"feature_importance_{latest_file.stem}.csv"
            if feature_file.exists():
                df = pl.read_csv(feature_file)
                self._feature_columns = df["feature"].to_list()
            else:
                # Fallback to extracting from model
                if hasattr(self._model, "feature_names_in_"):
                    self._feature_columns = list(cast(Any, self._model).feature_names_in_)
                else:
                    LOG.warning("Could not determine feature columns, ML may not work correctly")
                    self._feature_columns = None

            LOG.info(
                "ML model loaded | file=%s type=%s features=%s",
                latest_file.name,
                metadata.get("model_type", "unknown"),
                len(self._feature_columns) if self._feature_columns else "unknown",
            )

        except Exception as exc:
            LOG.error("Failed to load ML model: %s, ML filter disabled", exc)
            self.enabled = False

    def _extract_features(
        self, signal: "Signal", prepared: "PreparedSymbol"
    ) -> dict[str, float]:
        """Extract feature vector for ML inference.

        Column names MUST match dataset._outcome_to_row exactly so the model's
        feature alignment (self._feature_columns) maps correctly at inference time.
        """
        features: dict[str, float] = {}

        # Core signal features — names match dataset column names
        features["base_score"] = signal.score
        features["risk_reward"] = float(signal.risk_reward or 0.0)
        features["atr_pct_15m"] = signal.atr_pct or 0.0
        features["spread_bps"] = signal.spread_bps or 0.0
        features["stop_distance_pct"] = signal.stop_distance_pct or 0.0
        features["entry_mid"] = signal.entry_mid or 0.0
        features["quote_volume"] = float(signal.quote_volume or 0.0)
        features["delta_ratio"] = signal.orderflow_delta_ratio or 0.0

        # Market regime features
        if prepared.work_4h is not None and prepared.work_4h.height > 0:
            features["adx_4h"] = float(prepared.work_4h.item(-1, "adx14") or 0.0)
            features["rsi_4h"] = float(prepared.work_4h.item(-1, "rsi14") or 50.0)

        if prepared.work_1h.height > 0:
            features["adx_1h"] = float(prepared.work_1h.item(-1, "adx14") or 0.0)
            features["rsi_1h"] = float(prepared.work_1h.item(-1, "rsi14") or 50.0)
            ema20_1h = float(prepared.work_1h.item(-1, "ema20") or 0.0)
            ema50_1h = float(prepared.work_1h.item(-1, "ema50") or 0.0)
            ema200_1h = float(prepared.work_1h.item(-1, "ema200") or 0.0)
            features["ema20_above_ema50_1h"] = 1.0 if ema20_1h > ema50_1h > 0 else 0.0
            features["ema50_above_ema200_1h"] = 1.0 if ema50_1h > ema200_1h > 0 else 0.0

        if prepared.work_15m.height > 0:
            features["rsi_15m"] = float(prepared.work_15m.item(-1, "rsi14") or 50.0)
            features["volume_ratio_15m"] = float(prepared.work_15m.item(-1, "volume_ratio20") or 1.0)
            features["macd_histogram_15m"] = float(prepared.work_15m.item(-1, "macd_hist") or 0.0)
            ema20_15m = float(prepared.work_15m.item(-1, "ema20") or 0.0)
            ema50_15m = float(prepared.work_15m.item(-1, "ema50") or 0.0)
            ema200_15m = float(prepared.work_15m.item(-1, "ema200") or 0.0)
            features["ema20_above_ema50_15m"] = 1.0 if ema20_15m > ema50_15m > 0 else 0.0
            features["ema50_above_ema200_15m"] = 1.0 if ema50_15m > ema200_15m > 0 else 0.0

        # Bias one-hot — match dataset bias_4h_* columns
        bias = getattr(prepared, "bias_4h", None) or signal.bias_4h or "neutral"
        features["bias_4h_uptrend"] = 1.0 if bias == "uptrend" else 0.0
        features["bias_4h_downtrend"] = 1.0 if bias == "downtrend" else 0.0
        features["bias_4h_neutral"] = 1.0 if bias == "neutral" else 0.0

        # Direction one-hot — dataset has both direction_long and direction_short
        features["direction_long"] = 1.0 if signal.direction == "long" else 0.0
        features["direction_short"] = 1.0 if signal.direction == "short" else 0.0

        # Setup one-hot — match dataset setup_* columns (not integer setup_encoded)
        for sid in (
            "structure_pullback",
            "structure_break_retest",
            "wick_trap_reversal",
            "squeeze_setup",
            "ema_bounce",
            "fvg_setup",
            "order_block",
            "liquidity_sweep",
            "bos_choch",
            "hidden_divergence",
            "funding_reversal",
            "cvd_divergence",
            "session_killzone",
            "breaker_block",
            "turtle_soup",
        ):
            features[f"setup_{sid}"] = 1.0 if signal.setup_id == sid else 0.0

        # --- Advanced features (match build_prepared_feature_snapshot) ---
        # These are saved in outcomes but were previously missing from live inference.
        # They must be present so the trained model's feature set aligns at inference.
        if prepared.work_15m.height > 0:
            features["supertrend_dir_15m"] = float(prepared.work_15m.item(-1, "supertrend_dir") or 0.0)
            features["obv_above_ema_15m"] = float(prepared.work_15m.item(-1, "obv_above_ema") or 0.0)
            features["bb_pct_b_15m"] = float(prepared.work_15m.item(-1, "bb_pct_b") or 0.5)
            features["bb_width_15m"] = float(prepared.work_15m.item(-1, "bb_width") or 0.0)

        if prepared.work_1h.height > 0:
            features["supertrend_dir_1h"] = float(prepared.work_1h.item(-1, "supertrend_dir") or 0.0)

        # Market context features from PreparedSymbol
        features["funding_rate"] = float(getattr(prepared, "funding_rate", 0.0) or 0.0)
        features["oi_current"] = float(getattr(prepared, "oi_current", 0.0) or 0.0)
        features["oi_change_pct"] = float(getattr(prepared, "oi_change_pct", 0.0) or 0.0)
        features["ls_ratio"] = float(getattr(prepared, "ls_ratio", 1.0) or 1.0)
        features["liquidation_score"] = float(getattr(prepared, "liquidation_score", 0.0) or 0.0)
        features["market_regime_trending"] = 1.0 if getattr(prepared, "market_regime", "") == "trending" else 0.0
        features["market_regime_choppy"] = 1.0 if getattr(prepared, "market_regime", "") == "choppy" else 0.0
        features["market_regime_neutral"] = 1.0 if getattr(prepared, "market_regime", "") in ("neutral", "") else 0.0

        return features

    def predict(self, signal: "Signal", prepared: "PreparedSymbol") -> MLInferenceResult:
        """Run ML inference on a signal.

        Args:
            signal: The signal to evaluate
            prepared: The prepared symbol data

        Returns:
            MLInferenceResult with probability and confidence
        """
        if not self.enabled:
            return MLInferenceResult(
                probability=0.5,
                confidence=0.0,
                feature_vector={},
                model_version="disabled",
                error="ML not enabled or model not loaded",
            )

        try:
            metadata = self._model_metadata or {}
            # Extract features
            features = self._extract_features(signal, prepared)
            if self._model is None and self._classifier is not None:
                classifier_features = self._to_signal_classifier_features(features)
                probability = float(self._classifier.predict_proba(classifier_features))
                confidence = abs(probability - 0.5) * 2
                return MLInferenceResult(
                    probability=round(probability, 4),
                    confidence=round(confidence, 4),
                    feature_vector=features,
                    model_version=metadata.get("trained_at", "signal_classifier_artifact"),
                )

            if self._model is None:
                return MLInferenceResult(
                    probability=0.5,
                    confidence=0.0,
                    feature_vector={},
                    model_version="disabled",
                    error="ML not enabled or model not loaded",
                )

            model = cast(Any, self._model)

            # Align features to model expectations
            if self._feature_columns:
                missing = [col for col in self._feature_columns if col not in features]
                if missing:
                    LOG.warning(
                        "ML inference skipped for %s %s: %d missing feature(s): %s",
                        signal.symbol,
                        signal.setup_id,
                        len(missing),
                        missing[:5],
                    )
                    return MLInferenceResult(
                        probability=0.5,
                        confidence=0.0,
                        feature_vector=features,
                        model_version=metadata.get("trained_at", "unknown"),
                        error=f"missing_features:{','.join(missing[:3])}",
                    )
                aligned_features = {col: features[col] for col in self._feature_columns}
                input_df = pl.DataFrame([aligned_features])
            else:
                input_df = pl.DataFrame([features])

            # Run prediction (convert Polars to sklearn-friendly input).
            try:
                model_input = input_df.to_pandas(use_pyarrow_extension_array=False)
            except Exception:
                model_input = input_df.to_numpy()

            if hasattr(model, "predict_proba"):
                proba = model.predict_proba(model_input)[0]
                # Binary classification: probability of positive class
                probability = float(proba[1]) if len(proba) > 1 else float(proba[0])
            else:
                # Fallback to direct prediction
                pred = model.predict(model_input)[0]
                probability = float(pred) if pred > 0 else 0.0

            # Calculate confidence (distance from 0.5)
            confidence = abs(probability - 0.5) * 2  # Scale to 0-1

            return MLInferenceResult(
                probability=round(probability, 4),
                confidence=round(confidence, 4),
                feature_vector=features,
                model_version=metadata.get("trained_at", "unknown"),
            )

        except Exception as exc:
            LOG.debug("ML prediction failed for %s: %s", signal.symbol, exc)
            return MLInferenceResult(
                probability=0.5,
                confidence=0.0,
                feature_vector={},
                model_version="error",
                error=str(exc),
            )

    @staticmethod
    def _to_signal_classifier_features(features: dict[str, float]) -> dict[str, float]:
        mapped = dict(features)
        mapped.setdefault("atr_pct", float(features.get("atr_pct_15m", 0.0)))
        mapped.setdefault("adx14", float(features.get("adx_1h", 0.0)))
        mapped.setdefault("rsi14", float(features.get("rsi_15m", 50.0)))
        mapped.setdefault("macd_hist", float(features.get("macd_histogram_15m", 0.0)))
        mapped.setdefault("volume_ratio20", float(features.get("volume_ratio_15m", 1.0)))
        mapped.setdefault("delta_ratio", float(features.get("delta_ratio", 0.5)))
        mapped.setdefault("oi_change_pct", float(features.get("oi_change_pct", 0.0)))
        mapped.setdefault("funding_rate", float(features.get("funding_rate", 0.0)))
        mapped.setdefault("mark_index_spread_bps", float(features.get("mark_index_spread_bps", 0.0)))
        mapped.setdefault("premium_zscore_5m", float(features.get("premium_zscore_5m", 0.0)))
        mapped.setdefault("ls_ratio", float(features.get("ls_ratio", 1.0)))
        mapped.setdefault("taker_ratio", float(features.get("delta_ratio", 0.5)))
        mapped.setdefault("close_position", float(features.get("close_position", 0.5)))
        mapped.setdefault("bb_pct_b", float(features.get("bb_pct_b_15m", 0.5)))
        mapped.setdefault("supertrend_dir", float(features.get("supertrend_dir_15m", 0.0)))
        mapped.setdefault("regime_confidence", float(features.get("market_regime_trending", 0.0)))
        mapped.setdefault("mtf_alignment_score", float(features.get("ema20_above_ema50_1h", 0.0)))
        return mapped

    def get_status(self) -> dict[str, Any]:
        """Get current ML filter status for diagnostics."""
        metadata = self._model_metadata or {}
        return {
            "enabled": self.enabled,
            "model_loaded": self._model is not None,
            "signal_classifier_loaded": self._classifier is not None,
            "model_version": metadata.get("trained_at"),
            "feature_count": len(self._feature_columns) if self._feature_columns else 0,
            "confidence_threshold": self.confidence_threshold,
        }
