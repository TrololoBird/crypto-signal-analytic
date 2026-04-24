"""structure_break_retest — structure break (переприор) + retest.

# WINDSURF_REVIEW: unified + vectorized + 1H context + graded
"""
from __future__ import annotations

from typing import cast

import polars as pl

from ..config import BotSettings
from ..config_loader import load_strategy_config, get_nested
from ..features import _swing_points
from ..models import PreparedSymbol, Signal
from ..setup_base import BaseSetup
from ..setups import _build_signal, _compute_dynamic_score, _reject
from ..setups.utils import (
    build_structural_targets,
    get_dynamic_params,
    validate_rr_or_penalty,
)


def _as_float(value: object, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    return default


class StructureBreakRetestSetup(BaseSetup):
    setup_id = "structure_break_retest"
    family = "breakout"
    confirmation_profile = "breakout_acceptance"
    required_context = ("futures_flow",)

    def get_optimizable_params(self, settings: BotSettings | None = None) -> dict[str, float]:
        """Tunable parameters for self-learner optimization."""
        defaults = {
            "base_score": 0.62,
            "min_vol_breakout": 1.3,
            "retest_atr_tol": 0.5,
            "breakout_threshold": 0.002,
            "bias_mismatch_penalty": 0.75,
            "tp_too_close_penalty": 0.75,
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
        setup_id = self.setup_id
        dynamic_params = get_dynamic_params(prepared, setup_id)
        defaults = self.get_optimizable_params(settings)
        
        # Load config-driven parameters
        config = load_strategy_config("structure_break_retest")
        swing_lookback = get_nested(config, "detection.swing_lookback", 10)
        retest_atr_mult = get_nested(config, "detection.retest_atr_mult", 0.3)
        sl_buffer_atr = get_nested(config, "risk_management.sl_buffer_atr", 0.5)

        work_1h = prepared.work_1h
        work_15m = prepared.work_15m

        if work_1h.height < 10 or work_15m.height < 5:
            _reject(prepared, setup_id, "insufficient_bars")
            return None

        atr_1h = _as_float(work_1h.item(-1, "atr14"))
        if atr_1h <= 0.0:
            _reject(prepared, setup_id, "atr_non_positive", atr_1h=atr_1h)
            return None

        # 1H context for 15M signals (not 4H - too lagging for <4h trades)
        regime_1h = prepared.regime_1h_confirmed
        bias_1h = prepared.bias_1h

        sh_mask, sl_mask = _swing_points(work_1h, n=3)
        if not sh_mask.any() and not sl_mask.any():
            _reject(prepared, setup_id, "no_swing_points")
            return None

        trig_close = _as_float(work_15m.item(-1, "close"))
        prev_close = _as_float(work_15m.item(-2, "close"))
        atr = _as_float(work_15m.item(-1, "atr14"))
        if atr <= 0.0:
            _reject(prepared, setup_id, "atr_non_positive_15m", atr=atr)
            return None

        direction: str | None = None
        broken_level: float | None = None
        breakout_bar_idx: int | None = None

        min_vol_breakout = dynamic_params.get("min_vol_breakout", defaults["min_vol_breakout"])
        retest_atr_tol = dynamic_params.get("retest_atr_tol", defaults["retest_atr_tol"])
        breakout_threshold = dynamic_params.get("breakout_threshold", defaults["breakout_threshold"])

        if regime_1h != "uptrend" and sh_mask.any():
            sh_positions = [idx for idx, is_swing in enumerate(sh_mask.to_list()) if is_swing]
            if len(sh_positions) > 0:
                last_sh_pos = sh_positions[-1]
                last_sh_price = float(work_1h["high"][last_sh_pos])
                lookback_start = max(last_sh_pos + 1, work_1h.height - 24)
                breakout_detected = False
                for i in range(lookback_start, work_1h.height):
                    bar_close = float(work_1h.item(i, "close"))
                    vol_ratio_bar = _as_float(work_1h.item(i, "volume_ratio20"), 1.0)
                    if bar_close > last_sh_price * (1 + breakout_threshold):
                        if vol_ratio_bar >= min_vol_breakout:
                            breakout_detected = True
                            broken_level = last_sh_price
                            breakout_bar_idx = i
                            break
                if breakout_detected and broken_level is not None:
                    retest_distance = abs(trig_close - broken_level)
                    if retest_distance < atr * retest_atr_tol and trig_close > broken_level * 1.001:
                        direction = "long"

        if regime_1h != "downtrend" and sl_mask.any() and direction is None:
            sl_positions = [idx for idx, is_swing in enumerate(sl_mask.to_list()) if is_swing]
            if len(sl_positions) > 0:
                last_sl_pos = sl_positions[-1]
                last_sl_price = float(work_1h["low"][last_sl_pos])
                lookback_start = max(last_sl_pos + 1, work_1h.height - 24)
                breakout_detected = False
                for i in range(lookback_start, work_1h.height):
                    bar_close = float(work_1h.item(i, "close"))
                    vol_ratio_bar = _as_float(work_1h.item(i, "volume_ratio20"), 1.0)
                    if bar_close < last_sl_price * (1 - breakout_threshold):
                        if vol_ratio_bar >= min_vol_breakout:
                            breakout_detected = True
                            broken_level = last_sl_price
                            breakout_bar_idx = i
                            break
                if breakout_detected and broken_level is not None:
                    retest_distance = abs(trig_close - broken_level)
                    if retest_distance < atr * retest_atr_tol and trig_close < broken_level * 0.999:
                        direction = "short"

        if direction is None or broken_level is None:
            _reject(prepared, setup_id, "no_breakout_detected", regime=regime_1h)
            return None
        broken_level_value = broken_level
        if breakout_bar_idx is None or breakout_bar_idx < work_1h.height - 4:
            _reject(prepared, setup_id, "stale_breakout_retest", breakout_bar_idx=breakout_bar_idx)
            return None

        vol_ratio = _as_float(work_1h.item(-1, "volume_ratio20"), 1.0)

        reasons = [
            f"1h broken level={broken_level_value:.4f}",
            f"direction={direction}",
            f"1h regime={regime_1h}",
            f"breakout_bar_idx={breakout_bar_idx}",
            f"retest confirmed vol_ratio={vol_ratio:.2f}",
        ]

        price_anchor = trig_close

        # --- Compute structural SL/TP via unified utility ---
        stop, tp1, tp2 = build_structural_targets(
            direction=direction,
            price_anchor=price_anchor,
            stop_basis=broken_level_value,
            atr=atr,
            work_1h=work_1h,
            min_rr=dynamic_params.get("min_rr", defaults["min_rr"]),
            sh_mask=sh_mask,
            sl_mask=sl_mask,
            breakout_bar_idx=breakout_bar_idx,
            broken_level=broken_level_value,
        )

        # Graded RR validation (penalty instead of reject)
        min_rr = dynamic_params.get("min_rr", defaults["min_rr"])
        is_valid_rr, _ = validate_rr_or_penalty(price_anchor, stop, tp1, min_rr)

        base_score = dynamic_params.get("base_score", defaults["base_score"])
        score = _compute_dynamic_score(
            direction=direction,
            base_score=base_score,
            vol_ratio=vol_ratio,
            structure_clarity=0.7,
        )

        # Graded scoring instead of reject for bias mismatch
        if direction == "long" and bias_1h == "downtrend":
            score *= dynamic_params.get("bias_mismatch_penalty", defaults["bias_mismatch_penalty"])
            reasons.append("bias_mismatch_penalty")
        if direction == "short" and bias_1h == "uptrend":
            score *= dynamic_params.get("bias_mismatch_penalty", defaults["bias_mismatch_penalty"])
            reasons.append("bias_mismatch_penalty")

        # TP too close penalty
        if not is_valid_rr and tp1 is not None:
            score *= dynamic_params.get("tp_too_close_penalty", defaults["tp_too_close_penalty"])
            reasons.append("tp_too_close_penalty")

        final_tp1 = tp1 if tp1 is not None else price_anchor
        final_tp2 = tp2 if tp2 is not None else final_tp1
        if final_tp1 <= 0.0 or final_tp2 <= 0.0:
            _reject(prepared, setup_id, "invalid_targets", tp1=tp1, tp2=tp2)
            return None

        return _build_signal(
            prepared=prepared, setup_id=setup_id, direction=direction,
            score=score, reasons=reasons, stop=stop, tp1=final_tp1, tp2=final_tp2,
            price_anchor=price_anchor, atr=atr,
        )
