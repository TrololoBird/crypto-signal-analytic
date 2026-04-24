"""Fair Value Gap (FVG) setup detector.

Bullish FVG: high[i-2] < low[i]  (gap between candle i-2 top and candle i bottom)
Bearish FVG: low[i-2] > high[i]

Scans last 50 bars of work_15m; enters when price is currently inside a gap.

# WINDSURF_REVIEW: unified + vectorized + 1H context + graded
"""
from __future__ import annotations

import logging
import math

import polars as pl

from ..setup_base import BaseSetup
from ..config import BotSettings
from ..config_loader import load_strategy_config, get_nested
from ..models import PreparedSymbol, Signal
from ..setups import _build_signal, _compute_dynamic_score, _reject
from ..setups.utils import (
    build_structural_targets,
    validate_rr_or_penalty,
    apply_graded_penalty,
    get_dynamic_params,
)

LOG = logging.getLogger("bot.strategies.fvg")


class FVGSetup(BaseSetup):
    setup_id = "fvg_setup"
    family = "continuation"
    confirmation_profile = "trend_follow"
    required_context = ("futures_flow",)

    def get_optimizable_params(self, settings: BotSettings | None = None) -> dict[str, float]:
        """Tunable parameters for self-learner optimization."""
        defaults = {
            "base_score": 0.55,
            "min_gap_width_bps": 15.0,
            "min_volume_ratio": 1.10,
            "bias_mismatch_penalty": 0.75,
            "rsi_overbought": 70.0,
            "rsi_oversold": 30.0,
            "min_rr": 1.5,
            "tp_too_close_penalty": 0.8,
        }
        if settings is not None:
            filters = getattr(settings, 'filters', None)
            if filters:
                setups_config = getattr(filters, 'setups', {})
                if isinstance(setups_config, dict) and "fvg" in setups_config:
                    return {**defaults, **setups_config.get("fvg", {})}
        return defaults

    def detect(self, prepared: PreparedSymbol, settings: BotSettings) -> Signal | None:
        try:
            return self._detect(prepared, settings)
        except Exception as exc:
            LOG.exception("%s fvg_setup: unexpected error", prepared.symbol)
            _reject(
                prepared,
                self.setup_id,
                "runtime.unexpected_exception",
                stage="runtime",
                exception_type=type(exc).__name__,
            )
            return None

    def _detect(self, prepared: PreparedSymbol, settings: BotSettings) -> Signal | None:
        setup_id = self.setup_id
        dynamic_params = get_dynamic_params(prepared, setup_id)
        defaults = self.get_optimizable_params(settings)
        
        # Load config-driven parameters
        config = load_strategy_config("fvg")
        min_fvg_size_atr = get_nested(config, "detection.min_fvg_size_atr", 0.3)
        min_mitigation_pct = get_nested(config, "detection.min_mitigation_pct", 0.3)
        sl_buffer_atr = get_nested(config, "risk_management.sl_buffer_atr", 0.5)

        w = prepared.work_15m
        if w.height < 10:
            _reject(prepared, setup_id, "insufficient_15m_bars", bars=w.height)
            return None

        atr = float(w.item(-1, "atr14") or 0.0)
        if atr <= 0 or math.isnan(atr):
            _reject(prepared, setup_id, "atr_invalid", atr=atr)
            return None

        price = prepared.mark_price or prepared.universe.last_price
        if not price or price <= 0:
            _reject(prepared, setup_id, "price_missing")
            return None

        vol_ratio = float(w.item(-1, "volume_ratio20") or 1.0)
        rsi = float(w.item(-1, "rsi14") or 50.0)

        # Scan last 50 bars newest-first to find the most recent valid FVG.
        # An FVG between candle[i-2] and candle[i] is only valid once closed,
        # so we skip i = last index (current candle may still be forming).
        scan = w.tail(50) if w.height >= 50 else w
        highs = scan["high"].to_numpy()
        lows = scan["low"].to_numpy()
        n = len(scan)

        vol_ratios = scan["volume_ratio20"].to_numpy() if "volume_ratio20" in scan.columns else None

        min_gap_width_bps = dynamic_params.get("min_gap_width_bps", defaults["min_gap_width_bps"])
        min_volume_ratio = dynamic_params.get("min_volume_ratio", defaults["min_volume_ratio"])

        best_fvg = None
        # i goes newest-to-oldest; skip last candle (i = n-1) — use n-2 as newest
        for i in range(n - 2, 1, -1):
            # Bullish FVG: high[i-2] < low[i] — gap between 2 candles back and current
            gap_bot = highs[i - 2]
            gap_top = lows[i]
            if gap_bot < gap_top:
                width = gap_top - gap_bot
                if width / price >= (min_gap_width_bps / 10000) and gap_bot <= price <= gap_top:
                    # Require: the middle candle (i-1) or impulse candle (i) had elevated volume
                    vol_ok = True
                    if vol_ratios is not None:
                        candle_vol = float(vol_ratios[i - 1]) if not math.isnan(vol_ratios[i - 1]) else 1.0
                        vol_ok = candle_vol >= min_volume_ratio
                    if vol_ok:
                        best_fvg = ("long", gap_bot, gap_top)
                        break

            # Bearish FVG: low[i-2] > high[i]
            gap_top2 = lows[i - 2]
            gap_bot2 = highs[i]
            if gap_top2 > gap_bot2:
                width = gap_top2 - gap_bot2
                if width / price >= (min_gap_width_bps / 10000) and gap_bot2 <= price <= gap_top2:
                    vol_ok = True
                    if vol_ratios is not None:
                        candle_vol = float(vol_ratios[i - 1]) if not math.isnan(vol_ratios[i - 1]) else 1.0
                        vol_ok = candle_vol >= min_volume_ratio
                    if vol_ok:
                        best_fvg = ("short", gap_bot2, gap_top2)
                        break

        if best_fvg is None:
            _reject(prepared, setup_id, "no_fvg_detected")
            return None

        direction, fvg_low, fvg_high = best_fvg

        # Use 1H context for 15M signals (not 4H - too lagging for <4h trades)
        bias_1h = getattr(prepared, 'bias_1h', prepared.bias_4h)
        regime_1h = getattr(prepared, 'regime_1h_confirmed', prepared.regime_4h_confirmed)
        
        # Graded scoring instead of hard reject for bias mismatch
        base_score = dynamic_params.get("base_score", defaults["base_score"])
        score = _compute_dynamic_score(
            direction=direction,
            base_score=base_score,
            vol_ratio=vol_ratio,
            rsi=rsi,
        )

        if direction == "long" and bias_1h == "downtrend":
            score *= dynamic_params.get("bias_mismatch_penalty", defaults["bias_mismatch_penalty"])
        if direction == "short" and bias_1h == "uptrend":
            score *= dynamic_params.get("bias_mismatch_penalty", defaults["bias_mismatch_penalty"])

        # RSI extremes filter with graded penalty
        rsi_overbought = dynamic_params.get("rsi_overbought", defaults["rsi_overbought"])
        rsi_oversold = dynamic_params.get("rsi_oversold", defaults["rsi_oversold"])
        if direction == "long" and rsi > rsi_overbought:
            score *= 0.85  # Light penalty for overbought
        if direction == "short" and rsi < rsi_oversold:
            score *= 0.85  # Light penalty for oversold

        # 1h structure alignment with graded penalty
        structure_1h = prepared.structure_1h
        if direction == "long" and structure_1h == "downtrend":
            score *= dynamic_params.get("bias_mismatch_penalty", defaults["bias_mismatch_penalty"])
        if direction == "short" and structure_1h == "uptrend":
            score *= dynamic_params.get("bias_mismatch_penalty", defaults["bias_mismatch_penalty"])

        # --- Compute structural SL/TP ---
        fvg_mid = (fvg_low + fvg_high) / 2.0
        if direction == "long":
            # SL: beyond opposite side of FVG + 0.5×ATR (was 0.1)
            stop = fvg_low - atr * 0.5
            # TP1: FVG midpoint (50% fill)
            tp1 = fvg_mid if fvg_mid > price else fvg_high
            # TP2: full FVG fill (opposite boundary)
            tp2 = fvg_high
        else:
            # SL: beyond opposite side of FVG + 0.5×ATR (was 0.1)
            stop = fvg_high + atr * 0.5
            # TP1: FVG midpoint (50% fill)
            tp1 = fvg_mid if fvg_mid < price else fvg_low
            # TP2: full FVG fill
            tp2 = fvg_low

        # Graded RR validation instead of hard reject
        min_rr = dynamic_params.get("min_rr", defaults["min_rr"])
        is_valid_rr, _ = validate_rr_or_penalty(price, stop, tp1, min_rr)
        
        if not is_valid_rr and tp1 is not None:
            score *= dynamic_params.get("tp_too_close_penalty", defaults["tp_too_close_penalty"])

        if tp2 is None or abs(tp2 - price) <= abs(tp1 - price):
            tp2 = tp1  # Use TP1 as TP2 if no extended target found

        reasons = [
            f"FVG {direction}: gap [{fvg_low:.4f}-{fvg_high:.4f}]",
            f"price={price:.4f} inside gap | 1h_bias={bias_1h} 1h_struct={structure_1h}",
            f"vol_ratio={vol_ratio:.2f} rsi={rsi:.1f}",
        ]

        return _build_signal(
            prepared=prepared,
            setup_id=self.setup_id,
            direction=direction,
            score=score,
            reasons=reasons,
            stop=stop,
            tp1=tp1,
            tp2=tp2,
            price_anchor=price,
            atr=atr,
        )
