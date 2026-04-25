"""structure_pullback — pullback to structural level in confirmed trend.

# WINDSURF_REVIEW: unified + vectorized + 1H context + graded
"""
from __future__ import annotations

from typing import cast

import polars as pl

from ..config import BotSettings
from ..models import PreparedSymbol, Signal
from ..setup_base import BaseSetup
from ..setups import (
    _build_signal,
    _compute_dynamic_score,
    _pullback_levels,
    _reject,
)
from ..setups.utils import get_dynamic_params, select_structural_target


def _as_float(value: object, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    return default


class StructurePullbackSetup(BaseSetup):
    setup_id = "structure_pullback"
    family = "continuation"
    confirmation_profile = "trend_follow"
    required_context = ("futures_flow",)

    def get_optimizable_params(self, settings: BotSettings | None = None) -> dict[str, float]:
        """Tunable parameters for self-learner optimization."""
        defaults = {
            "base_score": 0.55,
            "bias_mismatch_penalty": 0.75,
            "tp_too_close_penalty": 0.75,
            "min_rr": 1.5,
            "min_trend_score": 0.40,
            "ema_proximity_pct": 0.995,
            "pullback_lookback": 12.0,
            "sl_buffer_atr": 0.5,
            "min_adx_1h": 15.0,
        }
        if settings is not None:
            filters = getattr(settings, 'filters', None)
            if filters:
                setups_config = getattr(filters, 'setups', {})
                if isinstance(setups_config, dict) and self.setup_id in setups_config:
                    return {**defaults, **setups_config.get(self.setup_id, {})}
        return defaults

    def detect(self, prepared: PreparedSymbol, settings: BotSettings) -> Signal | None:
        work_4h = prepared.work_4h
        work_1h = prepared.work_1h
        work_15m = prepared.work_15m

        dynamic_params = get_dynamic_params(prepared, self.setup_id)
        defaults = self.get_optimizable_params(settings)
        min_trend_score = dynamic_params.get("min_trend_score", defaults["min_trend_score"])
        ema_proximity_pct = dynamic_params.get("ema_proximity_pct", defaults["ema_proximity_pct"])
        pullback_lookback = int(dynamic_params.get("pullback_lookback", defaults["pullback_lookback"]))
        sl_buffer_atr = dynamic_params.get("sl_buffer_atr", defaults["sl_buffer_atr"])
        min_rr = dynamic_params.get("min_rr", defaults["min_rr"])

        if work_1h.height < 5 or work_15m.height < 5:
            _reject(prepared, "structure_pullback", "insufficient_bars")
            return None

        # Use 1H context for 15M signals (not 4H - too lagging for <4h trades)
        regime_1h = prepared.regime_1h_confirmed
        regime_4h = prepared.regime_4h_confirmed
        bias_1h = prepared.bias_1h
        structure = prepared.structure_1h

        if regime_1h in {"uptrend", "downtrend"} and structure in {"uptrend", "downtrend"} and structure != regime_1h:
            _reject(prepared, "structure_pullback", "structure_contradicts_regime",
                    regime=regime_1h, structure=structure)
            return None
        atr_1h = _as_float(work_1h.item(-1, "atr14"))
        ema20_1h = _as_float(work_1h.item(-1, "ema20"))
        close_1h = _as_float(work_1h.item(-1, "close"))
        if ema20_1h <= 0.0:
            _reject(prepared, "structure_pullback", "ema20_invalid", ema20_1h=ema20_1h)
            return None

        long_score = 0.0
        long_reasons: list[str] = []
        if regime_1h == "uptrend":
            long_score += 0.40
            long_reasons.append("1h_uptrend")
        elif bias_1h == "uptrend":
            long_score += 0.25
            long_reasons.append("1h_bias_uptrend")
        if structure == "uptrend":
            long_score += 0.30
            long_reasons.append("1h_structure_uptrend")
        elif structure == "ranging" and regime_1h == "uptrend":
            long_score += 0.15
            long_reasons.append("1h_ranging_in_1h_uptrend")
        if close_1h > ema20_1h:
            long_score += 0.20
            long_reasons.append("price_above_ema20")
        elif close_1h >= ema20_1h * float(ema_proximity_pct):
            long_score += 0.15
            long_reasons.append("price_near_ema20")

        short_score = 0.0
        short_reasons: list[str] = []
        if regime_1h == "downtrend":
            short_score += 0.40
            short_reasons.append("1h_downtrend")
        elif bias_1h == "downtrend":
            short_score += 0.25
            short_reasons.append("1h_bias_downtrend")
        if structure == "downtrend":
            short_score += 0.30
            short_reasons.append("1h_structure_downtrend")
        elif structure == "ranging" and regime_1h == "downtrend":
            short_score += 0.15
            short_reasons.append("1h_ranging_in_1h_downtrend")
        if close_1h < ema20_1h:
            short_score += 0.20
            short_reasons.append("price_below_ema20")
        elif close_1h <= ema20_1h * (2.0 - float(ema_proximity_pct)):
            short_score += 0.15
            short_reasons.append("price_near_ema20")

        if max(long_score, short_score) < float(min_trend_score) or abs(long_score - short_score) < 0.03:
            _reject(prepared, "structure_pullback", "trend_score_too_low",
                    long_score=round(long_score, 3), short_score=round(short_score, 3),
                    regime=regime_1h, structure=structure,
                    close=round(close_1h, 4), ema20=round(float(ema20_1h), 4))
            return None

        if long_score > short_score:
            direction = "long"
            trend_reasons = long_reasons
        else:
            direction = "short"
            trend_reasons = short_reasons

        adx_1h = _as_float(work_1h.item(-1, "adx14"))
        # Use per-setup min_adx from config if available, else fallback to global
        setup_params = self.get_optimizable_params(settings)
        min_adx = setup_params.get("min_adx_1h", settings.filters.min_adx_1h)
        if adx_1h > 0.0 and adx_1h < min_adx:
            _reject(prepared, "structure_pullback", "adx_too_low_1h", adx_1h=round(adx_1h, 2), min_adx=min_adx)
            return None

        prev_low = _as_float(work_15m.item(-2, "low"))
        prev_high = _as_float(work_15m.item(-2, "high"))
        trig_close = _as_float(work_15m.item(-1, "close"))
        atr = _as_float(work_15m.item(-1, "atr14"))
        if atr <= 0.0:
            _reject(prepared, "structure_pullback", "atr_non_positive", atr=atr)
            return None

        selected_level_name: str | None = None
        selected_level: float | None = None
        touch_tolerance = max(atr * 0.20, trig_close * 0.0015)
        recent_pullback = work_15m.tail(int(max(2, pullback_lookback)))
        for level_name, level in _pullback_levels(prepared, direction):
            if level <= 0.0:
                continue
            if direction == "long":
                local_low = _as_float(recent_pullback["low"].min(), prev_low)
                touched = min(prev_low, local_low) <= level + touch_tolerance
                bounced = trig_close > level
            else:
                local_high = _as_float(recent_pullback["high"].max(), prev_high)
                touched = max(prev_high, local_high) >= level - touch_tolerance
                bounced = trig_close < level
            if touched and bounced:
                if selected_level is None or abs(trig_close - level) < abs(trig_close - selected_level):
                    selected_level_name = level_name
                    selected_level = level

        if selected_level is None or selected_level_name is None:
            _reject(prepared, "structure_pullback", "no_valid_pullback_level")
            return None

        level = selected_level
        vol_ratio = _as_float(work_15m.item(-1, "volume_ratio20"), 1.0)
        if vol_ratio < 0.8:
            _reject(prepared, "structure_pullback", "volume_too_low", vol_ratio=vol_ratio)
            return None

        rsi = _as_float(work_15m.item(-1, "rsi14"), 50.0)
        if direction == "long" and not (25.0 <= rsi <= 80.0):
            _reject(prepared, "structure_pullback", "rsi_out_of_range", direction=direction, rsi=rsi)
            return None
        if direction == "short" and not (15.0 <= rsi <= 75.0):
            _reject(prepared, "structure_pullback", "rsi_out_of_range", direction=direction, rsi=rsi)
            return None

        bb_pct_b = work_15m.item(-1, "bb_pct_b")
        if bb_pct_b is not None:
            try:
                bb_pct_b = float(bb_pct_b)
            except (TypeError, ValueError):
                bb_pct_b = None
        if bb_pct_b is not None:
            if direction == "long" and bb_pct_b > 0.90:
                _reject(prepared, "structure_pullback", "bb_extreme_long", bb_pct_b=round(bb_pct_b, 4))
                return None
            if direction == "short" and bb_pct_b < 0.10:
                _reject(prepared, "structure_pullback", "bb_extreme_short", bb_pct_b=round(bb_pct_b, 4))
                return None

        reasons = [
            f"1h regime_confirmed={regime_1h}",
            f"1h structure={structure}",
            *trend_reasons,
            f"pullback to {selected_level_name}={level:.4f}",
            f"vol_ratio={vol_ratio:.2f}",
            f"rsi={rsi:.1f}",
        ]
        if regime_1h in {"uptrend", "downtrend"} and regime_4h in {"uptrend", "downtrend"} and regime_1h != regime_4h:
            reasons.append(f"macro_4h_conflict={regime_4h}")

        price_anchor = trig_close

        # --- Compute structural SL/TP ---
        from ..features import _swing_points as _sp
        work_15m_tail = work_15m.tail(10)
        _sh15, _sl15 = _sp(work_15m_tail, n=2)

        if direction == "long":
            # SL: below pullback swing low (last 3-5 15m bars) + 0.15×ATR noise buffer
            last_10_lows = work_15m_tail["low"]
            sl_candidates = last_10_lows.filter(_sl15) if _sl15 is not None else last_10_lows
            fallback_low = work_15m.tail(5)["low"].min()
            pullback_low = (
                _as_float(sl_candidates.min())
                if sl_candidates.len() > 0
                else _as_float(fallback_low)
            )
            stop = pullback_low - atr * float(sl_buffer_atr)

            # TP1: prior 1h swing high above entry (trend extreme before pullback)
            sh_1h_mask, sl_mask = _sp(work_1h, n=3, include_unconfirmed_tail=True)
            tp1 = select_structural_target(
                work_1h,
                mask=sh_1h_mask,
                column="high",
                price_anchor=price_anchor,
                direction="long",
            )

            # TP2: next 4h swing high beyond TP1
            if work_4h is not None and not work_4h.is_empty():
                sh_4h_mask, _ = _sp(work_4h, n=2)
                tp2 = select_structural_target(
                    work_4h,
                    mask=sh_4h_mask,
                    column="high",
                    price_anchor=price_anchor,
                    direction="long",
                )
            else:
                tp2 = None
        else:
            # SL: above pullback swing high + 0.4×ATR noise buffer
            last_10_highs = work_15m_tail["high"]
            sh_candidates = last_10_highs.filter(_sh15) if _sh15 is not None else last_10_highs
            fallback_high = work_15m.tail(5)["high"].max()
            pullback_high = (
                _as_float(sh_candidates.max())
                if sh_candidates.len() > 0
                else _as_float(fallback_high)
            )
            stop = pullback_high + atr * float(sl_buffer_atr)

            # TP1: prior 1h swing low below entry
            _, sl_1h_mask = _sp(work_1h, n=3, include_unconfirmed_tail=True)
            tp1 = select_structural_target(
                work_1h,
                mask=sl_1h_mask,
                column="low",
                price_anchor=price_anchor,
                direction="short",
            )

            # TP2: next 4h swing low beyond TP1
            if work_4h is not None and not work_4h.is_empty():
                _, sl_4h_mask = _sp(work_4h, n=2)
                tp2 = select_structural_target(
                    work_4h,
                    mask=sl_4h_mask,
                    column="low",
                    price_anchor=price_anchor,
                    direction="short",
                )
            else:
                tp2 = None

        # Validate: TP1 must be at least 1.5× risk distance, else reject setup
        risk = abs(price_anchor - stop)
        if risk <= 0:
            _reject(prepared, "structure_pullback", "invalid_stop", stop=stop)
            return None
        min_rr_cfg = float(min_rr)
        min_required = risk * min_rr_cfg
        if tp1 is None or abs(tp1 - price_anchor) < min_required:
            if direction == "long":
                tp1 = price_anchor + min_required
            else:
                tp1 = price_anchor - min_required
            reasons.append("tp1_rr_fallback")
        if tp2 is None:
            tp2 = tp1  # Use TP1 as TP2 if no extended target found

        score = _compute_dynamic_score(
            direction=direction,
            base_score=0.60, vol_ratio=vol_ratio, rsi=rsi, structure_clarity=0.6,
        )

        return _build_signal(
            prepared=prepared, setup_id="structure_pullback", direction=direction,
            score=score, timeframe="15m+1h", reasons=reasons,
            strategy_family=self.family, stop=stop, tp1=tp1, tp2=tp2,
            price_anchor=price_anchor, atr=atr,
        )
