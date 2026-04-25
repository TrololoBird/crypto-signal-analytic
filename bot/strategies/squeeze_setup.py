"""squeeze_setup — BB + Keltner Channel squeeze (Сквиз).

# WINDSURF_REVIEW: unified + vectorized + 1H context + graded
"""
from __future__ import annotations

import logging
from typing import cast

import polars as pl

from ..config import BotSettings
from ..models import PreparedSymbol, Signal
from ..setup_base import BaseSetup
from ..setups import _build_signal, _compute_dynamic_score, _reject
from ..setups.utils import get_dynamic_params


def _as_float(value: object, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    return default


def _bb_kc_squeeze_active(work_15m: pl.DataFrame) -> tuple[bool, str]:
    """Detect a genuine BB + Keltner Channel squeeze."""
    if work_15m.height < 30:
        return False, ""

    bb_pct_b = _as_float(work_15m.item(-1, "bb_pct_b"), 0.5)
    bb_width = _as_float(work_15m.item(-1, "bb_width"))
    kc_upper = _as_float(work_15m.item(-1, "kc_upper"))
    kc_lower = _as_float(work_15m.item(-1, "kc_lower"))
    close = float(work_15m.item(-1, "close"))

    if kc_upper <= 0 or kc_lower <= 0 or bb_width <= 0:
        return False, ""

    if work_15m.height >= 30:
        bb_width_history = work_15m["bb_width"].tail(30)
        width_q25 = _as_float(bb_width_history.quantile(0.25), 0.02)
        was_compressed = bb_width <= width_q25
    else:
        was_compressed = bb_width < 0.02

    breakout_up = close > kc_upper and bb_pct_b > 0.80
    breakout_down = close < kc_lower and bb_pct_b < 0.20

    if was_compressed and breakout_up:
        return True, "long"
    if was_compressed and breakout_down:
        return True, "short"

    return False, ""


class SqueezeSetup(BaseSetup):
    setup_id = "squeeze_setup"
    family = "breakout"
    confirmation_profile = "breakout_acceptance"
    required_context = ("futures_flow",)

    def get_optimizable_params(self, settings: BotSettings | None = None) -> dict[str, float]:
        """Tunable parameters for self-learner optimization."""
        defaults = {
            "base_score": 0.55,
            "min_bb_compression_width": 0.02,
            "bb_pct_b_threshold": 0.80,
            "bias_mismatch_penalty": 0.75,
            "min_rr": 1.5,
        }
        if settings is not None:
            filters = getattr(settings, 'filters', None)
            if filters:
                setups_config = getattr(filters, 'setups', {})
                if isinstance(setups_config, dict) and self.setup_id in setups_config:
                    return {**defaults, **setups_config.get(self.setup_id, {})}
        return defaults

    def detect(self, prepared: PreparedSymbol, settings: BotSettings) -> Signal | None:
        work_15m = prepared.work_15m

        dynamic_params = get_dynamic_params(prepared, self.setup_id)
        defaults = self.get_optimizable_params(settings)
        bb_squeeze_threshold = dynamic_params.get("bb_squeeze_threshold", 0.05)
        min_bb_compression_width = dynamic_params.get("min_bb_compression_width", defaults["min_bb_compression_width"])
        bb_pct_b_threshold = dynamic_params.get("bb_pct_b_threshold", defaults["bb_pct_b_threshold"])
        volume_threshold = dynamic_params.get("volume_threshold", 0.8)
        sl_buffer_atr = dynamic_params.get("sl_buffer_atr", 0.4)

        if work_15m.height < 30:
            _reject(prepared, "squeeze_setup", "insufficient_bars")
            return None

        is_squeeze, squeeze_dir = _bb_kc_squeeze_active(work_15m)
        if not is_squeeze:
            _reject(prepared, "squeeze_setup", "no_bb_kc_squeeze")
            return None

        funding = prepared.funding_rate
        liq_score = prepared.liquidation_score
        crowd_aligned = False
        crowd_reason = ""

        if funding is not None and abs(funding) >= 0.0003:
            if funding > 0 and squeeze_dir == "short":
                crowd_aligned = True
                crowd_reason = f"funding={funding:.4f} (longs crowded)"
            elif funding < 0 and squeeze_dir == "long":
                crowd_aligned = True
                crowd_reason = f"funding={funding:.4f} (shorts crowded)"

        if liq_score is not None and abs(liq_score) >= 0.25:
            if liq_score > 0 and squeeze_dir == "short":
                crowd_aligned = True
                crowd_reason = f"liq_score={liq_score:.3f} (long liq pressure)"
            elif liq_score < 0 and squeeze_dir == "long":
                crowd_aligned = True
                crowd_reason = f"liq_score={liq_score:.3f} (short liq pressure)"

        if not crowd_aligned:
            _reject(prepared, "squeeze_setup", "no_crowd_confirmation",
                    funding=funding, liquidation_score=liq_score)
            return None

        direction = squeeze_dir

        oi_chg = prepared.oi_change_pct
        if oi_chg is not None and oi_chg < -8.0:
            _reject(prepared, "squeeze_setup", "oi_falling_too_fast", oi_change_pct=oi_chg)
            return None

        atr = _as_float(work_15m.item(-1, "atr14"))
        if atr <= 0.0:
            _reject(prepared, "squeeze_setup", "atr_non_positive", atr=atr)
            return None
        vol_ratio = _as_float(work_15m.item(-1, "volume_ratio20"), 1.0)
        rsi = _as_float(work_15m.item(-1, "rsi14"), 50.0)

        if vol_ratio < 1.2:
            _reject(prepared, "squeeze_setup", "volume_too_low", vol_ratio=vol_ratio)
            return None

        if direction == "short" and rsi > 70.0:
            _reject(prepared, "squeeze_setup", "rsi_too_high", rsi=rsi)
            return None
        if direction == "long" and rsi < 30.0:
            _reject(prepared, "squeeze_setup", "rsi_too_low", rsi=rsi)
            return None

        reasons = [
            crowd_reason,
            f"bb_kc_squeeze breakout={direction}",
            f"vol_ratio={vol_ratio:.2f}",
            f"rsi={rsi:.1f}",
        ]

        price_anchor = _as_float(work_15m.item(-1, "close"))

        # --- Compute structural SL/TP ---
        pre_breakout = work_15m.slice(-11, 10)  # 10 bars before signal bar
        if pre_breakout.height < 3:
            pre_breakout = work_15m.slice(-6, 5)

        if direction == "long":
            # SL: below pre-breakout swing low + 0.4×ATR (was 0.15)
            stop = _as_float(pre_breakout["low"].min()) - atr * 0.4
            # TP1: first swing/fractal in breakout direction on 15m
            from ..features import _swing_points as _sp
            _sh_mask, sl_mask = _sp(work_15m, n=3)
            sh_prices = work_15m.filter(_sh_mask)["high"]
            tp1_candidates = sh_prices.filter(sh_prices > price_anchor)
            tp1 = _as_float(tp1_candidates[0]) if tp1_candidates.len() > 0 else None
            # TP2: squeeze range height projected from entry
            squeeze_range = _as_float(pre_breakout["high"].max()) - _as_float(pre_breakout["low"].min())
            tp2 = price_anchor + squeeze_range if squeeze_range > 0 else None
        else:
            # SL: above pre-breakout swing high + 0.4×ATR (was 0.15)
            stop = _as_float(pre_breakout["high"].max()) + atr * 0.4
            from ..features import _swing_points as _sp
            _, _sl15 = _sp(work_15m, n=2)
            sl_prices = work_15m.filter(_sl15)["low"]
            tp1_candidates = sl_prices.filter(sl_prices < price_anchor)
            tp1 = _as_float(tp1_candidates[-1]) if tp1_candidates.len() > 0 else None
            squeeze_range = _as_float(pre_breakout["high"].max()) - _as_float(pre_breakout["low"].min())
            tp2 = price_anchor - squeeze_range if squeeze_range > 0 else None

        # Validate: TP1 must be at least 1.5× risk distance, else reject
        risk = abs(price_anchor - stop)
        if risk <= 0:
            _reject(prepared, "squeeze_setup", "invalid_stop", stop=stop)
            return None
        if tp1 is None or abs(tp1 - price_anchor) < risk * 1.5:
            _reject(prepared, "squeeze_setup", "tp1_too_close_or_missing",
                    tp1=tp1, risk=risk, min_required=risk * 1.5)
            return None
        if tp2 is None:
            tp2 = tp1  # Use TP1 as TP2 if no extended target found

        score = _compute_dynamic_score(
            direction=direction,
            base_score=0.65, vol_ratio=vol_ratio, rsi=rsi, structure_clarity=0.5,
        )

        return _build_signal(
            prepared=prepared, setup_id="squeeze_setup", direction=direction,
            score=score, timeframe="15m", reasons=reasons,
            strategy_family=self.family, stop=stop, tp1=tp1, tp2=tp2,
            price_anchor=price_anchor, atr=atr,
        )
