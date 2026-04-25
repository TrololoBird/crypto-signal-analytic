"""Trading setup utilities and signal builders."""
from __future__ import annotations

import logging
import math
import re
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from typing import Any

import polars as pl

from ..core.engine.base import StrategyDecision
from ..features import _swing_points  # shared swing detection helper
from ..models import PreparedSymbol, Signal
from .utils import (
    apply_graded_penalty,
    build_structural_targets,
    get_dynamic_params,
    normalize_trade_levels,
    validate_rr_or_penalty,
)

LOG = logging.getLogger("bot.setups")

__all__ = [
    "apply_graded_penalty",
    "begin_strategy_decision_capture",
    "build_structural_targets",
    "finalize_strategy_decision",
    "get_dynamic_params",
    "normalize_trade_levels",
    "reset_strategy_decision_capture",
    "validate_rr_or_penalty",
    "_build_signal",
    "_compute_dynamic_score",
    "_last_swing_prices",
    "_pullback_levels",
    "_reject",
]


_ALLOWED_REASON_PREFIXES = {"data", "indicator", "pattern", "targets", "context", "filter", "runtime", "delivery"}
_CURRENT_DECISION_CAPTURE: ContextVar["_DecisionCapture | None"] = ContextVar(
    "bot_setups_current_decision_capture",
    default=None,
)


@dataclass(slots=True)
class _DecisionCapture:
    setup_id: str
    prepared: PreparedSymbol
    strict_data_quality: bool
    recorded: list[StrategyDecision] = field(default_factory=list)


def begin_strategy_decision_capture(
    *,
    prepared: PreparedSymbol,
    setup_id: str,
    strict_data_quality: bool,
) -> Token:
    return _CURRENT_DECISION_CAPTURE.set(
        _DecisionCapture(
            setup_id=setup_id,
            prepared=prepared,
            strict_data_quality=bool(strict_data_quality),
        )
    )


def reset_strategy_decision_capture(token: Token) -> None:
    _CURRENT_DECISION_CAPTURE.reset(token)


def finalize_strategy_decision(
    *,
    prepared: PreparedSymbol,
    setup_id: str,
    outcome: StrategyDecision | Signal | None,
) -> StrategyDecision:
    capture = _CURRENT_DECISION_CAPTURE.get()
    if isinstance(outcome, StrategyDecision):
        return outcome
    if isinstance(outcome, Signal):
        return StrategyDecision.signal_hit(
            setup_id=setup_id,
            signal=outcome,
            details={"symbol": prepared.symbol},
        )
    if capture is not None and capture.recorded:
        return capture.recorded[-1]
    return StrategyDecision.reject(
        setup_id=setup_id,
        stage="strategy",
        reason_code="pattern.no_raw_hit",
        details={"symbol": prepared.symbol, "fallback": "silent_none"},
    )


def _capture_decision(decision: StrategyDecision) -> StrategyDecision:
    capture = _CURRENT_DECISION_CAPTURE.get()
    if capture is not None:
        capture.recorded.append(decision)
    return decision


def _sanitize_reason_slug(reason: str) -> str:
    normalized = re.sub(r"[^a-z0-9_]+", "_", str(reason or "").strip().lower())
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    return normalized or "unspecified"


def _infer_reason_category(slug: str) -> str:
    if slug.startswith("data_") or "missing" in slug or "nan" in slug:
        return "data"
    if slug.startswith("runtime_") or "exception" in slug or "timeout" in slug or "error" in slug:
        return "runtime"
    if (
        "target" in slug
        or slug.startswith("tp")
        or slug.startswith("risk_")
        or slug.endswith("_stop")
        or "stop_" in slug
    ):
        return "targets"
    if (
        "context" in slug
        or "bias" in slug
        or "confirmation" in slug
        or "killzone" in slug
        or "opposes" in slug
        or "momentum" in slug
    ):
        return "context"
    if (
        "atr" in slug
        or "adx" in slug
        or "rsi" in slug
        or "ema" in slug
        or "bb_" in slug
        or "supertrend" in slug
        or "delta" in slug
        or "funding" in slug
        or "oi_" in slug
        or "volume_ratio" in slug
    ):
        return "indicator"
    if "cooldown" in slug or "filter" in slug or "score" in slug or "spread" in slug:
        return "filter"
    if "delivery" in slug or "telegram" in slug:
        return "delivery"
    return "pattern"


def _category_stage(category: str) -> str:
    if category == "filter":
        return "filters"
    if category in {"runtime", "delivery"}:
        return category
    if category == "data":
        return "data_quality"
    return "strategy"


def _normalize_reason(reason: str, *, stage: str | None = None) -> tuple[str, str]:
    raw = str(reason or "").strip()
    if "." in raw:
        prefix, _, suffix = raw.partition(".")
        if prefix in _ALLOWED_REASON_PREFIXES:
            return stage or _category_stage(prefix), f"{prefix}.{_sanitize_reason_slug(suffix)}"
    slug = _sanitize_reason_slug(raw)
    category = _infer_reason_category(slug)
    return stage or _category_stage(category), f"{category}.{slug}"


def _sanitize_details(values: dict[str, object]) -> tuple[dict[str, Any], tuple[str, ...], tuple[str, ...]]:
    details: dict[str, Any] = {}
    missing_fields: list[str] = []
    invalid_fields: list[str] = []
    for key, value in values.items():
        field_name = str(key)
        if value is None:
            missing_fields.append(field_name)
            continue
        if isinstance(value, bool):
            details[field_name] = value
            continue
        if isinstance(value, (int, float)):
            numeric = float(value)
            if not math.isfinite(numeric):
                invalid_fields.append(field_name)
                continue
            details[field_name] = numeric
            continue
        details[field_name] = value
    return details, tuple(sorted(set(missing_fields))), tuple(sorted(set(invalid_fields)))


def _frame_metric(frame: pl.DataFrame | None, column: str) -> tuple[float | None, tuple[str, ...], tuple[str, ...]]:
    if frame is None or frame.is_empty() or column not in frame.columns:
        return None, (column,), ()
    value = frame.item(-1, column)
    if value is None:
        return None, (column,), ()
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None, (), (column,)
    if not math.isfinite(numeric):
        return None, (), (column,)
    return numeric, (), ()


def _attr_metric(prepared: PreparedSymbol, field_name: str) -> tuple[Any | None, tuple[str, ...], tuple[str, ...]]:
    value = getattr(prepared, field_name, None)
    if value is None:
        return None, (field_name,), ()
    if isinstance(value, bool):
        return value, (), ()
    if isinstance(value, (int, float)):
        numeric = float(value)
        if not math.isfinite(numeric):
            return None, (), (field_name,)
        return numeric, (), ()
    return value, (), ()


def _decision_snapshot(prepared: PreparedSymbol) -> tuple[dict[str, Any], tuple[str, ...], tuple[str, ...]]:
    snapshot: dict[str, Any] = {}
    missing_fields: list[str] = []
    invalid_fields: list[str] = []
    field_specs = [
        ("atr14", lambda: _frame_metric(prepared.work_15m, "atr14")),
        ("rsi14", lambda: _frame_metric(prepared.work_15m, "rsi14")),
        ("volume_ratio20", lambda: _frame_metric(prepared.work_15m, "volume_ratio20")),
        ("adx14", lambda: _frame_metric(prepared.work_1h, "adx14")),
        ("spread_bps", lambda: _attr_metric(prepared, "spread_bps")),
        ("funding_rate", lambda: _attr_metric(prepared, "funding_rate")),
        ("oi_change_pct", lambda: _attr_metric(prepared, "oi_change_pct")),
        ("ls_ratio", lambda: _attr_metric(prepared, "ls_ratio")),
        ("mark_price", lambda: _attr_metric(prepared, "mark_price")),
    ]
    for key, getter in field_specs:
        value, missing, invalid = getter()
        if value is not None:
            snapshot[key] = value
        missing_fields.extend(missing)
        invalid_fields.extend(invalid)
    for attr_name in ("bias_4h", "bias_1h", "market_regime", "structure_1h", "regime_1h_confirmed"):
        value = getattr(prepared, attr_name, None)
        if value not in (None, ""):
            snapshot[attr_name] = value
        else:
            missing_fields.append(attr_name)
    return snapshot, tuple(sorted(set(missing_fields))), tuple(sorted(set(invalid_fields)))


def _merge_field_names(*groups: tuple[str, ...] | list[str] | None) -> tuple[str, ...]:
    merged: set[str] = set()
    for group in groups:
        if not group:
            continue
        merged.update(str(item) for item in group if str(item))
    return tuple(sorted(merged))


def _finite_or_none(frame: pl.DataFrame | None, column: str) -> float | None:
    value, _, invalid = _frame_metric(frame, column)
    if invalid:
        return None
    return value


def _build_signal(
    *,
    prepared: PreparedSymbol,
    setup_id: str,
    direction: str,
    score: float,
    reasons: list[str],
    stop: float,
    tp1: float,
    tp2: float,
    price_anchor: float,
    atr: float,
    timeframe: str = "15m",
    strategy_family: str = "continuation",
    entry_pad_atr_mult: float = 0.08,
) -> Signal | None:
    if not math.isfinite(float(atr)) or atr <= 0.0:
        _reject(
            prepared,
            setup_id,
            "indicator.atr_invalid",
            atr=atr,
            price_anchor=price_anchor,
        )
        return None
    normalized_levels = normalize_trade_levels(direction, price_anchor, stop, tp1, tp2)
    if normalized_levels is None:
        _reject(
            prepared,
            setup_id,
            "targets.target_integrity_failed",
            direction=direction,
            price_anchor=price_anchor,
            stop=stop,
            tp1=tp1,
            tp2=tp2,
        )
        return None
    normalized_stop, normalized_tp1, normalized_tp2, single_target_mode, integrity_status = normalized_levels
    entry_pad = max(atr * entry_pad_atr_mult, price_anchor * 0.0005)
    entry_low = price_anchor - entry_pad
    entry_high = price_anchor + entry_pad
    entry_mid = (entry_low + entry_high) / 2

    volume_ratio = _finite_or_none(prepared.work_15m, "volume_ratio20")
    adx_1h = _finite_or_none(prepared.work_1h, "adx14")
    premium_zscore_5m = _finite_or_none(prepared.work_5m, "premium_zscore_5m")
    premium_slope_5m = _finite_or_none(prepared.work_5m, "premium_slope_5m")
    ls_ratio = _finite_or_none(prepared.work_5m, "ls_ratio")

    risk = abs(entry_mid - normalized_stop)
    reward = abs(normalized_tp1 - entry_mid)
    risk_reward = reward / risk if risk > 0 else None

    trend_direction = getattr(prepared, "bias_1h", None)
    trend_score = getattr(prepared, "trend_score_1h", None)

    return Signal(
        symbol=prepared.symbol,
        setup_id=setup_id,
        direction=direction,
        score=score,
        timeframe=str(timeframe or "15m"),
        entry_low=min(entry_low, entry_high),
        entry_high=max(entry_low, entry_high),
        stop=normalized_stop,
        take_profit_1=normalized_tp1,
        take_profit_2=normalized_tp2,
        reasons=tuple(reasons),
        strategy_family=str(strategy_family or "continuation"),
        bias_4h=prepared.bias_4h,
        quote_volume=prepared.universe.quote_volume,
        mark_price=prepared.mark_price,
        volume_ratio=volume_ratio,
        target_integrity_status=integrity_status,
        single_target_mode=single_target_mode,
        adx_1h=adx_1h,
        risk_reward=risk_reward,
        trend_direction=trend_direction,
        trend_score=trend_score,
        premium_zscore_5m=premium_zscore_5m,
        premium_slope_5m=premium_slope_5m,
        ls_ratio=ls_ratio,
    )


def _compute_dynamic_score(
    *,
    direction: str,
    base_score: float,
    vol_ratio: float = 1.0,
    rsi: float = 50.0,
    structure_clarity: float = 0.5,
) -> float:
    score = base_score
    score += min(vol_ratio / 3.0, 1.0) * 0.05
    if direction == "long":
        rsi_health = 1.0 - abs(rsi - 55.0) / 45.0
    else:
        rsi_health = 1.0 - abs(rsi - 45.0) / 45.0
    score += max(0.0, rsi_health) * 0.05
    score += structure_clarity * 0.05
    return round(max(0.35, min(score, 0.90)), 4)


def _reject(
    prepared: PreparedSymbol,
    detector: str,
    reason: str,
    *,
    stage: str | None = None,
    missing_fields: tuple[str, ...] | list[str] | None = None,
    invalid_fields: tuple[str, ...] | list[str] | None = None,
    **values: object,
) -> None:
    normalized_stage, reason_code = _normalize_reason(reason, stage=stage)
    details, detail_missing, detail_invalid = _sanitize_details(values)
    snapshot, snapshot_missing, snapshot_invalid = _decision_snapshot(prepared)
    if snapshot:
        details = {**details, "snapshot": snapshot}
    merged_missing = _merge_field_names(missing_fields, detail_missing, snapshot_missing)
    merged_invalid = _merge_field_names(invalid_fields, detail_invalid, snapshot_invalid)
    decision = StrategyDecision.reject(
        setup_id=detector,
        stage=normalized_stage,
        reason_code=reason_code,
        details=details,
        missing_fields=merged_missing,
        invalid_fields=merged_invalid,
    )
    _capture_decision(decision)
    if details:
        detail_text = " ".join(f"{key}={value}" for key, value in details.items() if key != "snapshot")
        LOG.debug("%s: %s rejected | reason=%s %s", prepared.symbol, detector, reason_code, detail_text)
    else:
        LOG.debug("%s: %s rejected | reason=%s", prepared.symbol, detector, reason_code)


def _last_swing_prices(
    work: pl.DataFrame, n: int = 3
) -> tuple[float | None, float | None]:
    """Return (last_swing_high_price, last_swing_low_price) from work frame."""
    sh, sl = _swing_points(work, n=n, include_unconfirmed_tail=True)
    sh_prices = work.filter(sh)["high"] if sh is not None else None
    sl_prices = work.filter(sl)["low"] if sl is not None else None
    last_high = float(sh_prices[-1]) if sh_prices is not None and sh_prices.len() > 0 else None
    last_low = float(sl_prices[-1]) if sl_prices is not None and sl_prices.len() > 0 else None
    return last_high, last_low


def _pullback_levels(prepared: PreparedSymbol, direction: str) -> list[tuple[str, float]]:
    levels: list[tuple[str, float]] = []
    if prepared.work_1h.is_empty():
        return levels
    ema20_1h = float(prepared.work_1h.item(-1, "ema20") or 0.0)
    if ema20_1h > 0.0:
        levels.append(("ema20_1h", ema20_1h))
    if prepared.poc_1h and prepared.poc_1h > 0.0:
        levels.append(("poc_1h", float(prepared.poc_1h)))
    last_sh, last_sl = _last_swing_prices(prepared.work_1h)
    if direction == "long" and last_sl and last_sl > 0.0:
        levels.append(("swing_low_1h", float(last_sl)))
    if direction == "short" and last_sh and last_sh > 0.0:
        levels.append(("swing_high_1h", float(last_sh)))
    return levels
