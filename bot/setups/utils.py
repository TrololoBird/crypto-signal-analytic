"""Unified utilities for setup SL/TP calculation and graded scoring.

# WINDSURF_REVIEW: unified + vectorized + 1H context + graded
"""
from __future__ import annotations

from dataclasses import replace
import math
from typing import TYPE_CHECKING

import polars as pl

if TYPE_CHECKING:
    from ..models import PreparedSymbol, Signal


def _safe_float(value: object, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    return default


def _is_finite_positive(value: object) -> bool:
    if isinstance(value, bool):
        return False
    if not isinstance(value, (int, float)):
        return False
    return math.isfinite(float(value)) and float(value) > 0.0


def _price_tolerance(price_anchor: float) -> float:
    return max(abs(float(price_anchor)) * 1e-8, 1e-8)


def normalize_target_pair(
    direction: str,
    price_anchor: float,
    tp1: float,
    tp2: float,
) -> tuple[float, float, bool, str] | None:
    """Normalize TP ordering and detect single-target semantics."""
    if not _is_finite_positive(price_anchor):
        return None
    if not _is_finite_positive(tp1) or not _is_finite_positive(tp2):
        return None

    target_a = float(tp1)
    target_b = float(tp2)
    tolerance = _price_tolerance(price_anchor)

    if direction == "long":
        ordered_tp1, ordered_tp2 = sorted((target_a, target_b))
        if ordered_tp1 <= price_anchor + tolerance or ordered_tp2 <= price_anchor + tolerance:
            return None
    elif direction == "short":
        ordered_tp1, ordered_tp2 = sorted((target_a, target_b), reverse=True)
        if ordered_tp1 >= price_anchor - tolerance or ordered_tp2 >= price_anchor - tolerance:
            return None
    else:
        return None

    normalized = not (
        math.isclose(target_a, ordered_tp1, abs_tol=tolerance, rel_tol=0.0)
        and math.isclose(target_b, ordered_tp2, abs_tol=tolerance, rel_tol=0.0)
    )
    single_target_mode = math.isclose(ordered_tp1, ordered_tp2, abs_tol=tolerance, rel_tol=0.0)
    if single_target_mode:
        ordered_tp1 = ordered_tp2

    if normalized and single_target_mode:
        status = "normalized_single_target"
    elif normalized:
        status = "normalized"
    elif single_target_mode:
        status = "single_target"
    else:
        status = "valid"
    return ordered_tp1, ordered_tp2, single_target_mode, status


def normalize_trade_levels(
    direction: str,
    price_anchor: float,
    stop: float,
    tp1: float,
    tp2: float,
) -> tuple[float, float, float, bool, str] | None:
    """Validate mirrored target invariant and normalize target ordering."""
    if not _is_finite_positive(price_anchor) or not _is_finite_positive(stop):
        return None

    normalized_targets = normalize_target_pair(direction, price_anchor, tp1, tp2)
    if normalized_targets is None:
        return None
    normalized_tp1, normalized_tp2, single_target_mode, status = normalized_targets
    tolerance = _price_tolerance(price_anchor)
    normalized_stop = float(stop)

    if direction == "long":
        if normalized_stop >= price_anchor - tolerance:
            return None
    elif direction == "short":
        if normalized_stop <= price_anchor + tolerance:
            return None
    else:
        return None

    return normalized_stop, normalized_tp1, normalized_tp2, single_target_mode, status


def select_structural_target(
    work: pl.DataFrame,
    *,
    mask: pl.Series | None,
    column: str,
    price_anchor: float,
    direction: str,
) -> float | None:
    if mask is None or work.is_empty() or column not in work.columns or mask.sum() <= 0:
        return None

    prices = work.filter(mask)[column]
    best_price: float | None = None
    best_distance: float | None = None
    for raw_price in prices.to_list():
        price = _safe_float(raw_price, default=float("nan"))
        if math.isnan(price) or price <= 0.0:
            continue
        if direction == "long" and price <= price_anchor:
            continue
        if direction == "short" and price >= price_anchor:
            continue
        distance = abs(price - price_anchor)
        if best_distance is None or distance <= best_distance:
            best_price = price
            best_distance = distance
    return best_price


def build_structural_targets(
    direction: str,
    price_anchor: float,
    stop_basis: float,
    atr: float,
    work_1h: pl.DataFrame,
    work_4h: pl.DataFrame | None = None,
    min_rr: float = 1.5,
    sh_mask: pl.Series | None = None,
    sl_mask: pl.Series | None = None,
    breakout_bar_idx: int | None = None,
    broken_level: float | None = None,
) -> tuple[float, float | None, float | None]:
    """Calculate unified structural SL/TP targets.
    
    Args:
        direction: 'long' or 'short'
        price_anchor: Entry price reference
        stop_basis: Base level for SL calculation
        atr: Current ATR for buffer calculation
        work_1h: 1H timeframe data for TP calculation
        work_4h: Optional 4H timeframe for extended targets
        min_rr: Minimum risk/reward ratio
        sh_mask: Swing high boolean mask (for long TP1)
        sl_mask: Swing low boolean mask (for short TP1)
        breakout_bar_idx: Index of breakout bar (for TP2 measured move)
        broken_level: Level that was broken (for TP2 projection)
    
    Returns:
        Tuple of (stop, tp1, tp2) where tp1/tp2 may be None
    """
    if direction == "long":
        # SL: below stop_basis + 0.2×ATR noise buffer
        stop = stop_basis - atr * 0.2
        
        # TP1: next 1h resistance (swing high above entry)
        tp1 = None
        if sh_mask is not None and sh_mask.sum() > 0:
            tp1 = select_structural_target(
                work_1h,
                mask=sh_mask,
                column="high",
                price_anchor=price_anchor,
                direction="long",
            )
        
        # TP2: measured move = prior range projected from breakout point
        tp2 = None
        if breakout_bar_idx is not None and breakout_bar_idx > 0 and broken_level is not None:
            high_before = _safe_float(work_1h["high"].slice(0, breakout_bar_idx).max())
            low_before = _safe_float(work_1h["low"].slice(0, breakout_bar_idx).min())
            range_before = float(
                high_before - low_before
            )
            tp2 = broken_level + range_before
        elif work_4h is not None and not work_4h.is_empty():
            # Fallback to 4H resistance if no measured move
            last_4h_high = float(work_4h["high"][-1])
            if last_4h_high > price_anchor * 1.02:  # At least 2% above
                tp2 = last_4h_high
    else:
        # SL: above stop_basis + 0.2×ATR noise buffer
        stop = stop_basis + atr * 0.2
        
        # TP1: next 1h support (swing low below entry)
        tp1 = None
        if sl_mask is not None and sl_mask.sum() > 0:
            tp1 = select_structural_target(
                work_1h,
                mask=sl_mask,
                column="low",
                price_anchor=price_anchor,
                direction="short",
            )
        
        # TP2: measured move downward from breakout point
        tp2 = None
        if breakout_bar_idx is not None and breakout_bar_idx > 0 and broken_level is not None:
            high_before = _safe_float(work_1h["high"].slice(0, breakout_bar_idx).max())
            low_before = _safe_float(work_1h["low"].slice(0, breakout_bar_idx).min())
            range_before = float(
                high_before - low_before
            )
            tp2 = broken_level - range_before
        elif work_4h is not None and not work_4h.is_empty():
            # Fallback to 4H support if no measured move
            last_4h_low = float(work_4h["low"][-1])
            if last_4h_low < price_anchor * 0.98:  # At least 2% below
                tp2 = last_4h_low
    
    # Fallback: if tp2 is None, use tp1 as tp2
    if tp2 is None:
        tp2 = tp1
    if tp1 is not None and tp2 is not None:
        normalized_targets = normalize_target_pair(direction, price_anchor, tp1, tp2)
        if normalized_targets is None:
            tp1 = None
            tp2 = None
        else:
            tp1, tp2, _, _ = normalized_targets

    return stop, tp1, tp2


def validate_rr_or_penalty(
    price_anchor: float,
    stop: float,
    tp1: float | None,
    min_rr: float = 1.5,
) -> tuple[bool, float | None]:
    """Validate risk/reward ratio and return (is_valid, adjusted_tp1).
    
    Returns:
        Tuple of (is_valid, tp1_or_none). If RR < min_rr, returns (False, None).
        Caller should apply penalty to score instead of rejecting signal.
    """
    risk = abs(price_anchor - stop)
    if risk <= 0:
        return False, None
    
    if tp1 is None:
        return False, None
    
    reward = abs(tp1 - price_anchor)
    rr = reward / risk if risk > 0 else 0
    
    if rr < min_rr:
        # Return invalid - caller should apply penalty, not reject
        return False, tp1
    
    return True, tp1


def apply_graded_penalty(
    signal: "Signal",
    condition: bool,
    penalty: float = 0.75,
    reason: str = "",
) -> "Signal":
    """Apply graded penalty to signal score if condition is True.
    
    Instead of rejecting signal completely, reduce score by penalty factor.
    This allows more signals to pass through with lower confidence.
    
    Args:
        signal: Signal to potentially penalize
        condition: If True, apply penalty
        penalty: Multiplier for score (0.75 = reduce by 25%)
        reason: Reason for penalty (added to signal reasons)
    
    Returns:
        Modified signal with updated score and reasons
    """
    if not condition or signal.score <= 0:
        return signal

    reasons = signal.reasons
    if reason and reason not in reasons:
        reasons = (*reasons, reason)
    return replace(signal, score=signal.score * penalty, reasons=reasons)


def get_dynamic_params(prepared: "PreparedSymbol", setup_id: str) -> dict[str, float]:
    """Get dynamic parameters from prepared symbol for specific setup.
    
    This retrieves setup-specific configuration from settings or falls back
    to defaults. Used for making base_score and thresholds configurable.
    
    Args:
        prepared: Prepared symbol with attached settings
        setup_id: Setup identifier (e.g., 'ema_bounce', 'fvg')
    
    Returns:
        Dictionary of dynamic parameters
    """
    # Try to get from prepared.settings if available
    settings = prepared.settings
    if settings is None:
        return {}
    
    # Get setup-specific filters from settings
    filters = settings.filters
    if filters is None:
        return {}
    
    # Try to get setup-specific config
    setups_config = filters.setups
    if isinstance(setups_config, dict) and setup_id in setups_config:
        return setups_config.get(setup_id, {})
    
    return {}
