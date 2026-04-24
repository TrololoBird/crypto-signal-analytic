"""Signal filtering pipeline."""

from __future__ import annotations

import math
import logging
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

import polars as pl

from .config import BotSettings
from .models import PreparedSymbol, Signal
from .scoring import ScoringResult

if TYPE_CHECKING:
    from .confluence import ConfluenceEngine


UTC = timezone.utc
LOGGER = logging.getLogger(__name__)


_ADX_POLICY_HARD_GATE = "hard_gate"
_ADX_POLICY_PENALTY = "score_penalty"

# setup_id -> ADX policy override (highest precedence)
_ADX_POLICY_BY_SETUP: dict[str, str] = {
    "wick_trap_reversal": _ADX_POLICY_PENALTY,
    "funding_reversal": _ADX_POLICY_PENALTY,
    "turtle_soup": _ADX_POLICY_PENALTY,
}

# strategy_family -> ADX policy fallback
_ADX_POLICY_BY_FAMILY: dict[str, str] = {
    "reversal": _ADX_POLICY_PENALTY,
    "trend_follow": _ADX_POLICY_HARD_GATE,
    "continuation": _ADX_POLICY_HARD_GATE,
}


def _resolve_adx_policy(signal: Signal) -> str:
    setup_policy = _ADX_POLICY_BY_SETUP.get(signal.setup_id)
    if setup_policy:
        return setup_policy
    family_policy = _ADX_POLICY_BY_FAMILY.get(signal.strategy_family)
    if family_policy:
        return family_policy
    if signal.confirmation_profile == "trend_follow":
        return _ADX_POLICY_HARD_GATE
    return _ADX_POLICY_PENALTY


def _frame_is_fresh(frame: pl.DataFrame, max_age: timedelta) -> bool:
    if frame.is_empty() or "close_time" not in frame.columns:
        return False
    try:
        last_close = frame["close_time"].item(-1)
        if isinstance(last_close, str):
            last_close = datetime.fromisoformat(last_close.replace("Z", "+00:00"))
        elif isinstance(last_close, datetime):
            pass
        elif isinstance(last_close, (int, float)):
            last_close = datetime.fromtimestamp(float(last_close), tz=UTC)
        else:
            LOGGER.debug(
                "Freshness degraded: unsupported close_time type %s",
                type(last_close).__name__,
            )
            return False

        if last_close.tzinfo is None:
            last_close = last_close.replace(tzinfo=UTC)
        else:
            last_close = last_close.astimezone(UTC)
    except Exception as exc:
        LOGGER.debug("Freshness degraded: failed to normalize close_time (%s)", exc)
        return False

    try:
        delta = datetime.now(UTC) - last_close
    except Exception as exc:
        LOGGER.debug("Freshness degraded: failed to compute freshness delta (%s)", exc)
        return False
    return delta <= max_age


def apply_global_filters(
    signal: Signal,
    prepared: PreparedSymbol,
    settings: BotSettings,
    confluence_engine: "ConfluenceEngine",
) -> tuple[bool, Signal, str | None, ScoringResult | None, dict[str, Any] | None]:
    """Apply hard gates, scoring, and optional ML enhancement.

    Pipeline order (strict):
      1. Data freshness gates (15m, 1h)
      2. Mark price deviation guard
      3. Spread gate
      4. ATR gate
      5. Stop distance gate
      6. Risk/Reward gate
      7. Scoring engine
      8. ML enhancement (if enabled and confident)
      9. Minimum score gate
    """
    passed = list(signal.passed_filters)

    base = replace(
        signal,
        quote_volume=prepared.universe.quote_volume,
        oi_change_pct=prepared.oi_change_pct,
        funding_rate=prepared.funding_rate,
        spread_bps=prepared.spread_bps,
    )

    def _reject(
        reason: str,
        updated_signal: Signal,
        scoring: ScoringResult | None = None,
        details: dict[str, Any] | None = None,
    ) -> tuple[bool, Signal, str | None, ScoringResult | None, dict[str, Any] | None]:
        return False, replace(updated_signal, passed_filters=tuple(passed)), reason, scoring, details

    # --- 1. Data freshness ---
    if not _frame_is_fresh(
        prepared.work_15m,
        timedelta(minutes=settings.filters.freshness_15m_minutes),
    ):
        return _reject("stale_15m", base)
    passed.append("fresh_15m")
    if not _frame_is_fresh(
        prepared.work_1h,
        timedelta(hours=settings.filters.freshness_1h_hours),
    ):
        return _reject("stale_1h", base)
    passed.append("fresh_1h")
    # --- 2. Mark price sanity ---
    if (
        prepared.mark_price is not None
        and prepared.mark_price > 0
        and prepared.ticker_price is not None
        and prepared.ticker_price > 0
    ):
        deviation = abs(prepared.mark_price - prepared.ticker_price) / prepared.ticker_price
        mark_price_details = {
            "mark_price": prepared.mark_price,
            "comparison_price": prepared.ticker_price,
            "comparison_source": "ws_ticker",
            "comparison_age_seconds": prepared.ticker_price_age_seconds,
            "mark_price_age_seconds": prepared.mark_price_age_seconds,
            "deviation_pct": deviation,
        }
        if deviation > settings.filters.max_mark_price_deviation_pct:
            return _reject("mark_price_deviation", base, details=mark_price_details)
    passed.append("mark_price_ok")

    # --- 3. Spread ---
    if prepared.spread_bps is None:
        return _reject("spread_unavailable", base)
    if prepared.spread_bps > settings.filters.max_spread_bps:
        return _reject("spread_too_wide", base)
    passed.append("spread_ok")

    # --- 4. ATR ---
    atr_pct_raw = prepared.work_15m.item(-1, "atr_pct")
    if atr_pct_raw is None or (isinstance(atr_pct_raw, float) and math.isnan(atr_pct_raw)):
        return _reject("atr_nan", replace(base, atr_pct=0.0))
    atr_pct = float(atr_pct_raw)
    if atr_pct < settings.filters.min_atr_pct:
        return _reject("atr_too_low", replace(base, atr_pct=atr_pct))
    if atr_pct > settings.filters.max_atr_pct:
        return _reject("atr_too_high", replace(base, atr_pct=atr_pct))
    passed.append("atr_ok")

    # --- 4b. ADX policy (setup/family aware) ---
    adx_1h = 0.0
    if not prepared.work_1h.is_empty():
        adx_1h = float(prepared.work_1h.item(-1, "adx14") or 0.0)
    setup_overrides = settings.filters.setups.get(signal.setup_id, {})
    min_adx_1h = float(setup_overrides.get("min_adx_1h", settings.filters.min_adx_1h))
    adx_penalty_factor = float(setup_overrides.get("adx_penalty_factor", 0.85))
    adx_policy = _resolve_adx_policy(signal)
    adx_penalty_applied = False
    if adx_1h > 0.0 and adx_1h < min_adx_1h:
        if adx_policy == _ADX_POLICY_HARD_GATE:
            details = {
                "adx_policy": adx_policy,
                "adx_1h": adx_1h,
                "min_adx_1h": min_adx_1h,
                "setup_id": signal.setup_id,
                "strategy_family": signal.strategy_family,
            }
            return _reject("regime_not_suitable", replace(base, atr_pct=atr_pct), details=details)
        adx_penalty_applied = True
        passed.append("adx_1h_penalized")
    else:
        passed.append("adx_1h_ok")

    # 4h ranging no longer hard-blocks breakout strategies — a ranging 4h already
    # lowers the MTF alignment score (0.5 instead of 1.0), which reduces confidence
    # appropriately. Hard-blocking caused too many missed setups on symbols where 4h
    # is transitional but 1h clearly shows direction.
    passed.append("regime_ok")

    # Compute delta_ratio from 15m candles (CVD proxy)
    delta_ratio: float | None = None
    if not prepared.work_15m.is_empty() and "delta_ratio" in prepared.work_15m.columns:
        raw_delta = prepared.work_15m.item(-1, "delta_ratio")
        if raw_delta is not None:
            delta_ratio = float(raw_delta)

    updated = replace(
        signal,
        spread_bps=prepared.spread_bps,
        atr_pct=atr_pct,
        quote_volume=prepared.universe.quote_volume,
        oi_change_pct=prepared.oi_change_pct,
        funding_rate=prepared.funding_rate,
        orderflow_delta_ratio=delta_ratio,
        passed_filters=tuple(passed),
    )

    # --- 5. Stop distance ---
    if updated.stop_distance_pct < settings.tracking.min_stop_distance_pct:
        return _reject("stop_too_tight", updated)
    if updated.stop_distance_pct > settings.tracking.max_stop_distance_pct:
        return _reject("stop_too_wide", updated)
    updated = replace(updated, passed_filters=tuple([*updated.passed_filters, "stop_ok"]))

    # --- 6. Risk / Reward ---
    if updated.risk_reward < settings.filters.min_risk_reward:
        return _reject("risk_reward_too_low", updated)
    updated = replace(updated, passed_filters=tuple([*updated.passed_filters, "rr_ok"]))

    # --- 7. Scoring + ML (ConfluenceEngine — unified path) ---
    scoring_result: ScoringResult | None = None
    if settings.scoring.enabled:
        confluence_result = confluence_engine.score(updated, prepared)
        updated = replace(updated, score=confluence_result.final_score)
        scoring_result = confluence_result.to_scoring_result()
        passed = list(updated.passed_filters)
        passed.append("scoring_applied")
        if confluence_result.ml_probability is not None:
            passed.append("ml_applied")
        updated = replace(updated, passed_filters=tuple(passed))

    if adx_penalty_applied:
        pre_penalty_score = updated.score
        adjusted_score = pre_penalty_score * adx_penalty_factor
        updated = replace(updated, score=adjusted_score)
        penalty_delta = round(adjusted_score - pre_penalty_score, 6)
        if scoring_result is not None:
            scoring_result = replace(
                scoring_result,
                final_score=adjusted_score,
                adjustments={
                    **scoring_result.adjustments,
                    "adx_policy_penalty": penalty_delta,
                },
            )
        updated = replace(updated, passed_filters=tuple([*updated.passed_filters, "adx_penalty_applied"]))

    # --- 9. Minimum score gate (final gate after ALL adjustments) ---
    if settings.filters.min_score > 0.0 and updated.score < settings.filters.min_score:
        return _reject("score_too_low", updated, scoring_result)

    return True, updated, None, scoring_result, None
