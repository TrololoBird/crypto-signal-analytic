"""wick_trap_reversal — wick sweep of structural level then close back (Прокол).

# WINDSURF_REVIEW: unified + vectorized + 1H context + graded
"""
from __future__ import annotations

from datetime import datetime
from typing import cast

import polars as pl

from ..config import BotSettings
from ..features import _swing_points
from ..models import PreparedSymbol, Signal
from ..setup_base import BaseSetup
from ..setups import _build_signal, _compute_dynamic_score, _last_swing_prices, _reject
from ..setups.utils import get_dynamic_params


def _as_float(value: object, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    return default


class WickTrapReversalSetup(BaseSetup):
    setup_id = "wick_trap_reversal"
    family = "reversal"
    confirmation_profile = "countertrend_exhaustion"
    required_context = ("futures_flow",)

    def get_optimizable_params(self, settings: BotSettings | None = None) -> dict[str, float]:
        """Tunable parameters for self-learner optimization."""
        defaults = {
            "base_score": 0.55,
            "bias_mismatch_penalty": 0.75,
            "tp_too_close_penalty": 0.75,
            "min_rr": 1.5,
            "wick_atr_threshold": 0.3,
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

        work_1h = prepared.work_1h
        work_15m = prepared.work_15m

        if work_1h.height < 10 or work_15m.height < 8:
            _reject(prepared, setup_id, "insufficient_bars")
            return None

        atr = _as_float(work_15m.item(-1, "atr14"))
        if atr <= 0.0:
            _reject(prepared, setup_id, "atr_non_positive", atr=atr)
            return None

        # Config-driven parameters (from config_strategies.toml)
        wick_through_atr_mult = dynamic_params.get("wick_through_atr_mult", 0.3)
        closed_back_threshold = dynamic_params.get("closed_back_threshold", 0.0)
        
        sh_mask, sl_mask = _swing_points(work_1h, n=3)

        direction: str | None = None
        wick_bar_idx: int | None = None
        level: float | None = None

        def _recent_15m_positions_after(event_time: object) -> list[int]:
            positions: list[int] = []
            start_idx = max(0, work_15m.height - 12)
            for idx in range(start_idx, work_15m.height - 1):
                bar_time = work_15m.item(idx, "time")
                if isinstance(event_time, datetime) and isinstance(bar_time, datetime) and bar_time <= event_time:
                    continue
                positions.append(idx)
            return positions

        if sl_mask.any():
            # Get positions where sl_mask is True (swing low indices)
            sl_positions = [idx for idx, is_swing in enumerate(sl_mask.to_list()) if is_swing]
            for sl_pos in reversed(sl_positions):
                bars_ago = work_1h.height - 1 - sl_pos
                if 3 <= bars_ago <= 20:
                    candidate_level = float(work_1h["low"][sl_pos])
                    swing_time = work_1h.item(sl_pos, "time")
                    for k in _recent_15m_positions_after(swing_time):
                        bar_low = float(work_15m.item(k, "low"))
                        bar_close = float(work_15m.item(k, "close"))
                        wick_through = bar_low < candidate_level - atr * wick_through_atr_mult
                        closed_back = bar_close > candidate_level + closed_back_threshold
                        if wick_through and closed_back:
                            direction = "long"
                            wick_bar_idx = k
                            level = candidate_level
                            break
                    if direction is not None:
                        break

        if direction is None and sh_mask.any():
            # Get positions where sh_mask is True (swing high indices)
            sh_positions = [idx for idx, is_swing in enumerate(sh_mask.to_list()) if is_swing]
            for sh_pos in reversed(sh_positions):
                bars_ago = work_1h.height - 1 - sh_pos
                if 3 <= bars_ago <= 20:
                    candidate_level = float(work_1h["high"][sh_pos])
                    swing_time = work_1h.item(sh_pos, "time")
                    for k in _recent_15m_positions_after(swing_time):
                        bar_high = float(work_15m.item(k, "high"))
                        bar_close = float(work_15m.item(k, "close"))
                        wick_through = bar_high > candidate_level + atr * wick_through_atr_mult
                        closed_back = bar_close < candidate_level - closed_back_threshold
                        if wick_through and closed_back:
                            direction = "short"
                            wick_bar_idx = k
                            level = candidate_level
                            break
                    if direction is not None:
                        break

        if direction is None or level is None or wick_bar_idx is None:
            _reject(prepared, "wick_trap_reversal", "no_wick_trap_detected")
            return None

        vol_ratio = _as_float(work_15m.item(-1, "volume_ratio20"), 1.0)
        rsi = _as_float(work_15m.item(-1, "rsi14"), 50.0)
        st_15m = work_15m.item(-1, "supertrend_dir")
        if st_15m is not None:
            try:
                st_15m = float(st_15m)
            except (TypeError, ValueError):
                st_15m = None
        trig_high = float(work_15m.item(-1, "high"))
        trig_low = float(work_15m.item(-1, "low"))
        trig_close = float(work_15m.item(-1, "close"))
        candle_range = max(trig_high - trig_low, 0.0)
        close_strength_ok = False
        if candle_range > 0.0:
            if direction == "long":
                close_strength_ok = ((trig_close - trig_low) / candle_range) > 0.7
            else:
                close_strength_ok = ((trig_high - trig_close) / candle_range) > 0.7

        if st_15m is not None:
            if direction == "long" and st_15m < 0:
                _reject(prepared, "wick_trap_reversal", "supertrend_opposes_15m", st_dir_15m=st_15m)
                return None
            if direction == "short" and st_15m > 0:
                _reject(prepared, "wick_trap_reversal", "supertrend_opposes_15m", st_dir_15m=st_15m)
                return None

        if direction == "long":
            if trig_close <= level:
                _reject(prepared, "wick_trap_reversal", "trigger_close_below_level",
                        close=trig_close, level=level)
                return None
            if vol_ratio < 0.8 and not close_strength_ok:
                _reject(prepared, "wick_trap_reversal", "no_confirmation",
                        vol_ratio=vol_ratio, close_strength_ok=close_strength_ok)
                return None
        else:
            if trig_close >= level:
                _reject(prepared, "wick_trap_reversal", "trigger_close_above_level",
                        close=trig_close, level=level)
                return None
            if vol_ratio < 0.8 and not close_strength_ok:
                _reject(prepared, "wick_trap_reversal", "no_confirmation",
                        vol_ratio=vol_ratio, close_strength_ok=close_strength_ok)
                return None

        wick_bar_close = float(work_15m.item(wick_bar_idx, "close"))
        reasons = [
            f"wick_sweep level={level:.4f}",
            f"direction={direction}",
            f"wick_bar close={wick_bar_close:.4f}",
            f"vol_ratio={vol_ratio:.2f}",
            f"close_strength_ok={close_strength_ok}",
            f"rsi={rsi:.1f}",
        ]

        price_anchor = trig_close

        # --- Compute structural SL/TP ---
        if direction == "long":
            # SL: beyond wick extreme (absolute tip of sweep wick) + 0.5×ATR (was 0.1)
            wick_bar_low = float(work_15m.item(wick_bar_idx, "low"))
            stop = wick_bar_low - atr * 0.5
            # TP1: the swept level itself (price returns through what was pierced)
            tp1 = float(level)
            # TP2: next 1h swing beyond swept level
            last_sh, _ = _last_swing_prices(work_1h)
            tp2 = last_sh if (last_sh and last_sh > price_anchor) else None
        else:
            # SL: beyond wick extreme + 0.5×ATR (was 0.1)
            wick_bar_high = float(work_15m.item(wick_bar_idx, "high"))
            stop = wick_bar_high + atr * 0.5
            # TP1: the swept level
            tp1 = float(level)
            # TP2: next 1h swing beyond
            _, last_sl = _last_swing_prices(work_1h)
            tp2 = last_sl if (last_sl and last_sl < price_anchor) else None

        # Validate: TP1 must be at least 1.5× risk distance, else reject
        risk = abs(price_anchor - stop)
        if risk <= 0:
            _reject(prepared, "wick_trap_reversal", "invalid_stop", stop=stop)
            return None
        if tp1 is None or abs(tp1 - price_anchor) < risk * 1.5:
            _reject(prepared, "wick_trap_reversal", "tp1_too_close_or_missing",
                    tp1=tp1, risk=risk, min_required=risk * 1.5)
            return None
        if tp2 is None:
            tp2 = tp1  # Use TP1 as TP2 if no extended target found

        score = _compute_dynamic_score(
            direction=direction,
            base_score=0.60, vol_ratio=vol_ratio, rsi=rsi, structure_clarity=0.5,
        )

        return _build_signal(
            prepared=prepared, setup_id="wick_trap_reversal", direction=direction,
            score=score, timeframe="15m+1h", reasons=reasons,
            strategy_family=self.family, stop=stop, tp1=tp1, tp2=tp2,
            price_anchor=price_anchor, atr=atr,
        )


class WickTrapReversal(WickTrapReversalSetup):
    """Backward-compatible alias for legacy imports."""
