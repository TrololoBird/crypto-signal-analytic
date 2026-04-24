"""ema_bounce — simplified trend continuation setup.

# WINDSURF_REVIEW: unified + vectorized + 1H context + graded
"""
from __future__ import annotations

from typing import cast

import polars as pl

from ..config import BotSettings
from ..config_loader import load_strategy_config, get_nested
from ..models import PreparedSymbol, Signal
from ..setup_base import BaseSetup
from ..setups import _build_signal, _compute_dynamic_score, _reject
from ..setups.utils import (
    build_structural_targets,
    validate_rr_or_penalty,
    get_dynamic_params,
)


class EmaBounceSetup(BaseSetup):
    setup_id = "ema_bounce"
    family = "continuation"
    confirmation_profile = "trend_follow"
    required_context = ("futures_flow",)

    def get_optimizable_params(self, settings: BotSettings | None = None) -> dict[str, float]:
        """Tunable parameters for self-learner optimization.

        If settings provided, reads from config [bot.filters.setups].
        Otherwise returns hardcoded defaults.
        """
        defaults = {
            "base_score": 0.55,
            "min_adx_1h": 15.0,
            "vol_ratio_threshold": 1.0,
            "bias_mismatch_penalty": 0.75,
            "tp_too_close_penalty": 0.75,
            "min_rr": 1.5,
            "ema_touch_tolerance": 0.005,
        }

        if settings is not None:
            # Try to get from config
            filters = getattr(settings, 'filters', None)
            if filters:
                setups_config = getattr(filters, 'setups', {})
                if isinstance(setups_config, dict) and self.setup_id in setups_config:
                    config_params = setups_config.get(self.setup_id, {})
                    # Merge config with defaults (config takes precedence)
                    return {**defaults, **config_params}

        return defaults

    def detect(self, prepared: PreparedSymbol, settings: BotSettings) -> Signal | None:
        setup_id = self.setup_id
        dynamic_params = get_dynamic_params(prepared, setup_id)
        defaults = self.get_optimizable_params(settings)
        
        # Load config-driven parameters
        config = load_strategy_config("ema_bounce")
        ema_touch_tolerance_pct = get_nested(config, "detection.ema_touch_tolerance_pct", 0.008)
        bounce_threshold_pct = get_nested(config, "detection.bounce_threshold_pct", 0.005)
        min_adx = get_nested(config, "filters.min_adx", 18.0)
        
        work_1h = prepared.work_1h
        if work_1h.height < 3:
            _reject(prepared, setup_id, "insufficient_1h_bars")
            return None

        atr = float(work_1h.item(-1, "atr14") or 0.0)
        ema20 = float(work_1h.item(-1, "ema20") or 0.0)
        ema50 = float(work_1h.item(-1, "ema50") or 0.0)
        close = float(work_1h.item(-1, "close"))
        prev_close = float(work_1h.item(-2, "close"))

        if atr <= 0.0 or ema20 <= 0.0 or ema50 <= 0.0:
            _reject(prepared, setup_id, "invalid_indicator_state",
                    atr=atr, ema20=ema20, ema50=ema50)
            return None

        direction: str | None = None
        reasons: list[str] = []

        # 1H context for 15M signals (not 4H - too lagging for <4h trades)
        bias_1h = getattr(prepared, 'bias_1h', prepared.bias_4h)
        regime_1h = getattr(prepared, 'regime_1h_confirmed', prepared.regime_4h_confirmed)
        
        # Direction detection with graded scoring instead of reject
        signal_direction: str | None = None
        if bias_1h == "uptrend":
            touch_ema = (
                prev_close <= ema20 * (1.0 + float(ema_touch_tolerance_pct))
                or prev_close <= ema50 * (1.0 + float(ema_touch_tolerance_pct) * 2.0)
            )
            bounce = close > prev_close * (1.0 + float(bounce_threshold_pct)) and close > ema20
            if touch_ema and bounce:
                signal_direction = "long"
                reasons = ["ema_bounce_long", f"ema20_1h={ema20:.4f}", f"ema50_1h={ema50:.4f}"]
        elif bias_1h == "downtrend":
            touch_ema = (
                prev_close >= ema20 * (1.0 - float(ema_touch_tolerance_pct))
                or prev_close >= ema50 * (1.0 - float(ema_touch_tolerance_pct) * 2.0)
            )
            bounce = close < prev_close * (1.0 - float(bounce_threshold_pct)) and close < ema20
            if touch_ema and bounce:
                signal_direction = "short"
                reasons = ["ema_bounce_short", f"ema20_1h={ema20:.4f}", f"ema50_1h={ema50:.4f}"]

        if signal_direction is None:
            _reject(prepared, setup_id, "no_bounce_pattern", bias_1h=bias_1h)
            return None

        vol_ratio = float(work_1h.item(-1, "volume_ratio20") or 1.0)
        adx_1h = float(work_1h.item(-1, "adx14") or 0.0)
        if adx_1h > 0.0 and adx_1h < float(min_adx):
            _reject(prepared, setup_id, "adx_too_low", adx_1h=adx_1h, min_adx=min_adx)
            return None
        price_anchor = close

        # --- Compute structural SL/TP via unified utility ---
        from ..features import _swing_points as _sp
        sh_mask, sl_mask = _sp(work_1h, n=3)
        
        # Determine bounce EMA for SL basis
        if signal_direction == "long":
            bounce_ema = min(ema20, ema50) if prev_close <= ema50 * 1.01 else ema20
        else:
            bounce_ema = max(ema20, ema50) if prev_close >= ema50 * 0.99 else ema20
        
        stop, tp1, tp2 = build_structural_targets(
            direction=signal_direction,
            price_anchor=price_anchor,
            stop_basis=bounce_ema,
            atr=atr,
            work_1h=work_1h,
            min_rr=dynamic_params.get("min_rr", defaults["min_rr"]),
            sh_mask=sh_mask,
            sl_mask=sl_mask,
        )
        
        # Graded RR validation (penalty instead of reject)
        min_rr = dynamic_params.get("min_rr", defaults["min_rr"])
        is_valid_rr, _ = validate_rr_or_penalty(price_anchor, stop, tp1, min_rr)
        
        base_score = dynamic_params.get("base_score", defaults["base_score"])
        score = _compute_dynamic_score(
            direction=signal_direction,
            base_score=base_score,
            vol_ratio=vol_ratio,
            structure_clarity=0.3,
        )
        
        # Apply graded penalty for RR issues (not reject)
        if not is_valid_rr and tp1 is not None:
            score *= dynamic_params.get("tp_too_close_penalty", defaults["tp_too_close_penalty"])
            reasons.append("tp_too_close_penalty")
        
        if tp1 is None:
            _reject(prepared, setup_id, "tp1_missing", direction=signal_direction, price_anchor=price_anchor)
            return None

        # Fallback TP2
        if tp2 is None:
            tp2 = tp1

        return _build_signal(
            prepared=prepared, setup_id=setup_id, direction=signal_direction,
            score=score, timeframe="1h", reasons=reasons, stop=stop, tp1=tp1, tp2=tp2,
            price_anchor=close, atr=atr,
        )
