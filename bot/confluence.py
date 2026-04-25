"""ConfluenceEngine — unified signal quality scoring."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

from .config import BotSettings
from .models import PreparedSymbol, Signal
from .scoring import (
    ScoringResult,
    _crowd_position,
    _funding_contrarian,
    _mtf_alignment,
    _oi_momentum,
    _risk_reward_quality,
    _structure_clarity,
    _volume_quality,
)

if TYPE_CHECKING:
    from .ml_filter import MLFilter

LOG = logging.getLogger("bot.confluence")


@dataclass(frozen=True, slots=True)
class ComponentScore:
    """Score contribution from a single factor."""
    name: str
    raw: float
    weight: float
    contribution: float


@dataclass(frozen=True)
class ConfluenceResult:
    """Full quality assessment of a signal."""
    setup_prior: float
    components: tuple[ComponentScore, ...]
    final_score: float
    ml_probability: float | None = None
    ml_confidence: float | None = None
    ml_applied: bool = False

    @property
    def weighted_model_score(self) -> float:
        return sum(c.contribution for c in self.components)

    def to_scoring_result(self) -> ScoringResult:
        adjustments = {c.name: c.contribution for c in self.components}
        if self.ml_applied and self.ml_probability is not None:
            adjustments["ml_boost"] = self.ml_probability - 0.5
        return ScoringResult(
            base_score=self.setup_prior,
            adjustments=adjustments,
            final_score=self.final_score,  # type: ignore[attr-defined]
            setup_id="",
        )

    def to_dict(self) -> dict:
        return {
            "setup_prior": self.setup_prior,
            "components": [
                {"name": c.name, "raw": c.raw, "weight": c.weight, "contribution": c.contribution}
                for c in self.components
            ],
            "weighted_model_score": self.weighted_model_score,
            "final_score": self.final_score,
            "ml_probability": self.ml_probability,
            "ml_confidence": self.ml_confidence,
            "ml_applied": self.ml_applied,
        }


class ConfluenceEngine:
    """Single entry point for signal quality assessment.

    Usage::

        engine = ConfluenceEngine(settings, ml_filter=ml_filter)
        result = engine.score(signal, prepared)
    """

    def __init__(self, settings: BotSettings, ml_filter: "MLFilter | None" = None) -> None:
        self.settings = settings
        self._ml_filter = ml_filter

    def score(self, signal: Signal, prepared: PreparedSymbol) -> ConfluenceResult:
        cfg = self.settings.scoring
        components = self._compute_components(signal, prepared, cfg)
        model_score = sum(c.contribution for c in components)

        prior_w = max(0.0, min(cfg.setup_prior_weight, 1.0))
        blended = (signal.score * prior_w) + (model_score * (1.0 - prior_w))
        final = round(max(0.0, min(blended, 1.0)), 4)

        # ML enhancement (if enabled and confident)
        ml_probability: float | None = None
        ml_confidence: float | None = None
        ml_applied = False

        if self._ml_filter is not None and self._ml_filter.enabled:
            try:
                ml_result = self._ml_filter.predict(signal, prepared)
                if ml_result.error is None and ml_result.is_confident:
                    ml_probability = ml_result.probability
                    ml_confidence = ml_result.confidence
                    # Blend ML prediction into final score (40% ML, 60% base)
                    ml_weight = 0.4
                    final = round(
                        max(0.0, min(final * (1.0 - ml_weight) + ml_probability * ml_weight, 1.0)),
                        4
                    )
                    ml_applied = True
                    LOG.debug(
                        "ML applied | symbol=%s setup=%s ml_prob=%.3f confidence=%.3f final_score=%.3f",
                        signal.symbol, signal.setup_id, ml_probability, ml_confidence, final
                    )
            except Exception as exc:
                LOG.warning("ML prediction failed for %s: %s", signal.symbol, exc)

        return ConfluenceResult(
            setup_prior=signal.score,
            components=tuple(components),
            final_score=final,
            ml_probability=ml_probability,
            ml_confidence=ml_confidence,
            ml_applied=ml_applied,
        )

    def _compute_components(
        self,
        signal: Signal,
        prepared: PreparedSymbol,
        cfg,
    ) -> list[ComponentScore]:
        funding_weight = max(0.0, min(cfg.weight_crowd_position * 0.5, cfg.weight_crowd_position))
        crowd_weight = max(0.0, cfg.weight_crowd_position - funding_weight)
        specs = [
            ("mtf_alignment",     cfg.weight_mtf_alignment,    _mtf_alignment(prepared, signal)),
            ("volume_quality",    cfg.weight_volume_quality,   _volume_quality(prepared)),
            ("structure_clarity", cfg.weight_structure_clarity, _structure_clarity(prepared, signal)),
            ("risk_reward",       cfg.weight_risk_reward,      _risk_reward_quality(signal)),
            ("funding_score",     funding_weight,              _funding_contrarian(prepared, signal, self.settings)),
            ("crowd_position",    crowd_weight,                _crowd_position(prepared, signal, self.settings)),
            ("oi_momentum",       cfg.weight_oi_momentum,      _oi_momentum(prepared, signal)),
        ]
        return [
            ComponentScore(name=name, raw=round(raw, 4), weight=weight, contribution=round(weight * raw, 4))
            for name, weight, raw in specs
        ]
