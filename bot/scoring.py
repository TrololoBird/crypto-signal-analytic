"""Simplified structure-based scoring engine."""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from .config import BotSettings
from .features import _swing_points
from .models import PreparedSymbol, Signal


@dataclass(frozen=True, slots=True)
class ScoringResult:
    base_score: float
    adjustments: dict[str, float]
    final_score: float
    setup_id: str = ""
    ml_multiplier: float | None = None

    @property
    def total_adjustment(self) -> float:
        return self.final_score - self.base_score

    def to_dict(self) -> dict:
        return {
            "base_score": self.base_score,
            "adjustments": self.adjustments,
            "final_score": self.final_score,
            "setup_id": self.setup_id,
            "ml_multiplier": self.ml_multiplier,
        }


def _directional_alignment(value: str, direction: str) -> float:
    target = "uptrend" if direction == "long" else "downtrend"
    opposite = "downtrend" if direction == "long" else "uptrend"
    if value == target:
        return 1.0
    if value == opposite:
        return 0.0
    return 0.5


def _mtf_alignment(prepared: PreparedSymbol, signal: Signal) -> float:
    score_4h = _directional_alignment(prepared.regime_4h_confirmed, signal.direction)
    score_1h = _directional_alignment(prepared.structure_1h, signal.direction)
    # Weighted average: 1h structure is primary confirmation (70%), 4h bias is context (30%).
    # For 15m-triggered signals the 1h trend is the direct context — it should dominate.
    # 4h is useful but should reduce confidence, not veto.
    return max(0.0, min(score_1h * 0.70 + score_4h * 0.30, 1.0))


def _volume_quality(prepared: PreparedSymbol) -> float:
    if prepared.work_15m.is_empty():
        return 0.0
    ratio = float(prepared.work_15m.item(-1, "volume_ratio20") or 0.0)
    return max(0.0, min(ratio / 2.5, 1.0))


def _nearest_structure_level(prepared: PreparedSymbol, signal: Signal) -> float | None:
    levels: list[float] = []
    if not prepared.work_1h.is_empty():
        ema20 = float(prepared.work_1h.item(-1, "ema20") or 0.0)
        if ema20 > 0.0:
            levels.append(ema20)
    if prepared.poc_1h and prepared.poc_1h > 0.0:
        levels.append(prepared.poc_1h)
    sh_mask, sl_mask = _swing_points(prepared.work_1h, n=3)
    if signal.direction == "long":
        swing_levels = prepared.work_1h.filter(sl_mask)["low"].tail(3).to_list()
    else:
        swing_levels = prepared.work_1h.filter(sh_mask)["high"].tail(3).to_list()
    levels.extend(float(level) for level in swing_levels if float(level) > 0.0)
    if not levels:
        return None
    anchor = signal.entry_mid
    return min(levels, key=lambda level: abs(level - anchor))


def _structure_clarity(prepared: PreparedSymbol, signal: Signal) -> float:
    if prepared.work_1h.is_empty():
        return 0.0
    level = _nearest_structure_level(prepared, signal)
    if level is None or level <= 0.0:
        return 0.0
    atr = float(prepared.work_1h.item(-1, "atr14") or 0.0)
    zone_width = max(atr * 0.35, level * 0.0015)
    if zone_width <= 0.0:
        return 0.0
    touches = 0
    tail_df = prepared.work_1h.tail(24)
    for low, high in zip(tail_df["low"], tail_df["high"]):
        low_f = float(low or 0.0)
        high_f = float(high or 0.0)
        if low_f <= level + zone_width and high_f >= level - zone_width:
            touches += 1
    touch_score = min(touches / 4.0, 1.0)
    zone_width_pct = zone_width / level
    width_score = max(0.0, min(1.0 - (zone_width_pct / 0.02), 1.0))
    return max(0.0, min(touch_score * width_score, 1.0))


def _oi_momentum(prepared: PreparedSymbol, signal: Signal) -> float:
    """Score based on OI change + CVD proxy + basis (contango/backwardation).

    OI rising + CVD aligned with direction → high score (LONG_IN / SHORT_IN pattern).
    OI falling or CVD opposing direction → low score.
    Basis component: extreme contango penalises longs; backwardation penalises shorts.
    """
    # --- OI component ---
    oi_chg = prepared.oi_change_pct
    if oi_chg is None:
        oi_score = 0.5
    elif oi_chg >= 0.10:
        oi_score = 1.0
    elif oi_chg >= 0.05:
        oi_score = 0.75
    elif oi_chg >= 0.02:
        oi_score = 0.6
    elif oi_chg >= 0.0:
        oi_score = 0.5
    elif oi_chg >= -0.05:
        oi_score = 0.3
    else:
        oi_score = 0.1

    # --- CVD proxy component (delta_ratio from 15m candles) ---
    cvd_score = 0.5
    if not prepared.work_15m.is_empty() and "delta_ratio" in prepared.work_15m.columns:
        delta = float(prepared.work_15m.item(-1, "delta_ratio") or 0.5)
        # For LONG: buying pressure (delta_ratio > 0.5) is bullish
        # For SHORT: selling pressure (delta_ratio < 0.5) is bullish
        if signal.direction == "long":
            cvd_score = max(0.0, min((delta - 0.3) / 0.4, 1.0))
        else:
            cvd_score = max(0.0, min((0.7 - delta) / 0.4, 1.0))

    # --- Basis component (contango vs backwardation) ---
    # Extreme contango (> +0.10%) = futures richly priced → crowd is bullish/crowded
    #   → headwind for LONG, tailwind for SHORT
    # Deep backwardation (< -0.05%) = futures cheap → forced selling / capitulation
    #   → tailwind for LONG reversal, neutral for SHORT
    basis = getattr(prepared, "basis_pct", None)
    if basis is None:
        basis_score = 0.5
    else:
        if signal.direction == "long":
            if basis <= -0.05:
                basis_score = 0.85  # backwardation = capitulation, good for long reversal
            elif basis <= 0.05:
                basis_score = 0.5
            elif basis <= 0.15:
                basis_score = 0.35  # mild contango = crowded longs
            else:
                basis_score = 0.15  # extreme contango = very crowded
        else:
            if basis >= 0.10:
                basis_score = 0.85  # high contango = crowded longs, good for short
            elif basis >= 0.03:
                basis_score = 0.65
            elif basis >= -0.03:
                basis_score = 0.5
            else:
                basis_score = 0.35  # backwardation = capitulation already done, risky short

    return round((oi_score * 0.55 + cvd_score * 0.35 + basis_score * 0.10), 4)


def _risk_reward_quality(signal: Signal) -> float:
    rr = signal.risk_reward
    # Range starts at 1.9 (the filter minimum) so signals that just pass the gate
    # don't get near-zero scores.  RR 1.9 → 0.0, RR 4.0 → 1.0.
    return max(0.0, min((rr - 1.9) / 2.1, 1.0))


def _crowd_position(prepared: PreparedSymbol, signal: Signal, settings: BotSettings) -> float:
    """Score based on funding rate, L/S ratio, and taker buy/sell pressure (contrarian).

    Funding rate component (weight 0.50):
      Extreme funding against direction → crowd is wrong → high score
    L/S ratio component (weight 0.30):
      ls_ratio > 1.5 → crowd is very long → bullish for SHORT, bearish for LONG
      ls_ratio < 0.7 → crowd is very short → bearish for SHORT, bullish for LONG
    Taker ratio component (weight 0.20):
      taker_ratio > 1.3 → aggressive net buyers → confirms LONG, penalises SHORT
      taker_ratio < 0.7 → aggressive net sellers → confirms SHORT, penalises LONG
    """
    # --- Funding rate component ---
    funding = prepared.funding_rate
    if funding is None:
        funding_score = 0.5
    else:
        extreme = settings.scoring.funding_rate_extreme
        moderate = settings.scoring.funding_rate_moderate
        if signal.direction == "long":
            if funding <= -extreme:
                funding_score = 1.0
            elif funding <= -moderate:
                funding_score = 0.75
            elif funding >= extreme:
                funding_score = 0.0
            elif funding >= moderate:
                funding_score = 0.25
            else:
                funding_score = 0.5
        else:
            if funding >= extreme:
                funding_score = 1.0
            elif funding >= moderate:
                funding_score = 0.75
            elif funding <= -extreme:
                funding_score = 0.0
            elif funding <= -moderate:
                funding_score = 0.25
            else:
                funding_score = 0.5

    # --- L/S ratio component (top trader positioning) ---
    ls = prepared.ls_ratio
    if ls is None:
        ls_score = 0.5
    else:
        if signal.direction == "long":
            # Very long crowd (ls > 1.5) → contrarian bearish → penalise long
            if ls >= 2.0:
                ls_score = 0.10
            elif ls >= 1.5:
                ls_score = 0.30
            elif ls <= 0.5:
                ls_score = 1.0
            elif ls <= 0.7:
                ls_score = 0.85  # crowd short → long entry good
            else:
                ls_score = 0.5
        else:
            if ls <= 0.5:
                ls_score = 0.10  # crowd very short → contrarian bullish → penalise short
            elif ls <= 0.7:
                ls_score = 0.30
            elif ls >= 2.0:
                ls_score = 1.0
            elif ls >= 1.5:
                ls_score = 0.85  # crowd long → short entry good
            else:
                ls_score = 0.5

    # --- Taker ratio component (directional pressure from aggressor side) ---
    taker = getattr(prepared, "taker_ratio", None)
    if taker is None:
        taker_score = 0.5
    else:
        if signal.direction == "long":
            # Takers net buying → confirms long direction
            if taker >= 1.5:
                taker_score = 0.9
            elif taker >= 1.3:
                taker_score = 0.7
            elif taker <= 0.7:
                taker_score = 0.2  # takers selling → headwind for long
            elif taker <= 0.85:
                taker_score = 0.35
            else:
                taker_score = 0.5
        else:
            # Takers net selling → confirms short direction
            if taker <= 0.67:
                taker_score = 0.9
            elif taker <= 0.77:
                taker_score = 0.7
            elif taker >= 1.43:
                taker_score = 0.2  # takers buying → headwind for short
            elif taker >= 1.18:
                taker_score = 0.35
            else:
                taker_score = 0.5

    return round(funding_score * 0.50 + ls_score * 0.30 + taker_score * 0.20, 4)
