"""Break of Structure / Change of Character (BOS/CHoCH) setup detector.

Uses _swing_points to classify swing structure on work_15m.
Focuses on CHoCH signals (structure reversal) as entry triggers.

# WINDSURF_REVIEW: unified + vectorized + 1H context + graded
"""
from __future__ import annotations

import logging
import math
from typing import cast

import polars as pl

from ..setup_base import BaseSetup
from ..config import BotSettings
from ..config_loader import load_strategy_config, get_nested
from ..models import PreparedSymbol, Signal
from ..setups import _build_signal, _compute_dynamic_score, _reject
from ..features import _swing_points
from ..setups.utils import (
    build_structural_targets,
    validate_rr_or_penalty,
    get_dynamic_params,
)

LOG = logging.getLogger("bot.strategies.bos_choch")

_MIN_SWINGS = 6   # Need 3+ of each type for trend context


class BOSCHOCHSetup(BaseSetup):
    """BOS/CHoCH strategy detector for structural break signals."""

    setup_id = "bos_choch"
    family = "breakout"
    confirmation_profile = "breakout_acceptance"
    required_context = ("futures_flow",)

    def get_optimizable_params(self, settings: BotSettings | None = None) -> dict[str, float]:
        """Tunable parameters for self-learner optimization."""
        defaults = {
            "base_score": 0.55,
            "swing_lookback": 12,
            "breakout_threshold_atr": 0.4,
            "bias_mismatch_penalty": 0.75,
            "min_rr": 1.5,
            "min_swings": 6,
        }
        if settings is not None:
            filters = getattr(settings, 'filters', None)
            if filters:
                setups_config = getattr(filters, 'setups', {})
                if isinstance(setups_config, dict) and self.setup_id in setups_config:
                    return {**defaults, **setups_config.get(self.setup_id, {})}
        return defaults

    def detect(self, prepared: PreparedSymbol, settings: BotSettings) -> Signal | None:
        """Detect BOS/CHoCH signal for given symbol."""
        try:
            return self._detect(prepared, settings)
        except (ValueError, KeyError, IndexError) as e:
            LOG.exception("%s bos_choch: detection error: %s", prepared.symbol, e)
            _reject(
                prepared,
                self.setup_id,
                "runtime.unexpected_exception",
                stage="runtime",
                exception_type=type(e).__name__,
            )
            return None

    def _detect(self, prepared: PreparedSymbol, _settings: BotSettings) -> Signal | None:
        setup_id = self.setup_id
        dynamic_params = get_dynamic_params(prepared, setup_id)
        defaults = self.get_optimizable_params(_settings)
        
        # Load config-driven parameters
        config = load_strategy_config("bos_choch")
        bos_lookback = get_nested(config, "detection.bos_lookback", 20)
        choch_lookback = get_nested(config, "detection.choch_lookback", 15)
        sl_buffer_atr = get_nested(config, "risk_management.sl_buffer_atr", 0.5)

        w = prepared.work_15m
        if w.height < 30:
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

        # 1H context for 15M signals (not 4H - too lagging for <4h trades)
        bias_1h = getattr(prepared, 'bias_1h', prepared.bias_4h)

        sh_mask, sl_mask = _swing_points(w, n=3)
        sh_prices = w.filter(sh_mask)["high"]
        sl_prices = w.filter(sl_mask)["low"]

        # Need at least 3 of each to determine prior trend + break
        min_swings = int(dynamic_params.get("min_swings", defaults["min_swings"]))
        if sh_prices.len() < min_swings or sl_prices.len() < min_swings:
            _reject(
                prepared,
                setup_id,
                "insufficient_swing_points",
                swing_highs=sh_prices.len(),
                swing_lows=sl_prices.len(),
                min_swings=min_swings,
            )
            return None

        sh_vals = sh_prices.to_numpy()
        sl_vals = sl_prices.to_numpy()

        direction = None
        stop_price = None
        pivot_level = None

        # Bullish CHoCH:
        #   Prior downtrend confirmed: sh[-2] < sh[-3] (lower high) AND sl[-2] < sl[-3] (lower low)
        #   Structure break: sh[-1] > sh[-2] (first higher high = CHoCH)
        prior_downtrend = sh_vals[-2] < sh_vals[-3] and sl_vals[-2] < sl_vals[-3]
        bullish_break = sh_vals[-1] > sh_vals[-2]
        if prior_downtrend and bullish_break:
            direction = "long"
            # SL: beyond the BOS/CHoCH structural pivot (last swing low) + 0.2×ATR
            pivot_level = float(sl_vals[-1])
            stop_price = pivot_level - 0.2 * atr

        # Bearish CHoCH:
        #   Prior uptrend confirmed: sh[-2] > sh[-3] (higher high) AND sl[-2] > sl[-3] (higher low)
        #   Structure break: sl[-1] < sl[-2] (first lower low = CHoCH)
        else:
            prior_uptrend = sh_vals[-2] > sh_vals[-3] and sl_vals[-2] > sl_vals[-3]
            bearish_break = sl_vals[-1] < sl_vals[-2]
            if prior_uptrend and bearish_break:
                direction = "short"
                pivot_level = float(sh_vals[-1])
                stop_price = pivot_level + 0.2 * atr

        if direction is None or stop_price is None:
            _reject(prepared, setup_id, "no_choch_detected")
            return None

        # --- Compute structural SL/TP ---
        if direction == "long":
            # SL: beyond BOS/CHoCH structural pivot (last swing low) + 0.2×ATR
            pivot_level = float(sl_vals[-1])
            stop_price = pivot_level - 0.2 * atr
            risk = price - stop_price
            if risk <= 0:
                _reject(prepared, setup_id, "risk_non_positive_long", stop=stop_price, price=price)
                return None
            # TP1: last swing high before the structural break
            tp1 = float(sh_vals[-2]) if sh_vals[-2] > price else None
            # TP2: 4h swing target
            w4h = prepared.work_4h
            tp2 = None
            if w4h is not None and w4h.height > 5:
                sh4_mask, _ = _swing_points(w4h, n=2)
                sh4_prices = w4h.filter(sh4_mask)["high"]
                tp2_cands = sh4_prices.filter(sh4_prices > price)
                tp2 = float(tp2_cands[0]) if tp2_cands.len() > 0 else None
        else:
            # SL: beyond BOS/CHoCH structural pivot (last swing high) + 0.2×ATR
            pivot_level = float(sh_vals[-1])
            stop_price = pivot_level + 0.2 * atr
            risk = stop_price - price
            if risk <= 0:
                _reject(prepared, setup_id, "risk_non_positive_short", stop=stop_price, price=price)
                return None
            # TP1: last swing low before the structural break
            tp1 = float(sl_vals[-2]) if sl_vals[-2] < price else None
            # TP2: 4h swing target
            w4h = prepared.work_4h
            tp2 = None
            if w4h is not None and w4h.height > 5:
                _, sl4_mask = _swing_points(w4h, n=2)
                sl4_prices = w4h.filter(sl4_mask)["low"]
                tp2_cands = sl4_prices.filter(sl4_prices < price)
                tp2 = float(tp2_cands[-1]) if tp2_cands.len() > 0 else None

        # Validate: TP1 must be at least 1.5× risk distance, else reject
        if tp1 is None or abs(tp1 - price) < risk * 1.5:
            _reject(prepared, setup_id, "tp1_too_close_or_missing", tp1=tp1, risk=risk, price=price)
            return None  # Reject this CHoCH setup
        if tp2 is None:
            tp2 = tp1  # Use TP1 as TP2 if no extended target found

        vol_ratio = float(w.item(-1, "volume_ratio20") or 1.0)
        rsi = float(w.item(-1, "rsi14") or 50.0)
        score = _compute_dynamic_score(
            direction=direction,
            base_score=0.50,
            vol_ratio=vol_ratio,
            rsi=rsi,
        )

        reasons = [
            f"CHoCH {direction}: structure reversal",
            f"sh[-3]={sh_vals[-3]:.4f} sh[-2]={sh_vals[-2]:.4f} sh[-1]={sh_vals[-1]:.4f}",
            f"sl[-3]={sl_vals[-3]:.4f} sl[-2]={sl_vals[-2]:.4f} sl[-1]={sl_vals[-1]:.4f}",
        ]

        return _build_signal(
            prepared=prepared,
            setup_id=self.setup_id,
            direction=direction,
            score=score,
            timeframe="15m",
            reasons=reasons,
            stop=stop_price,
            tp1=tp1,
            tp2=tp2,
            price_anchor=price,
            atr=atr,
        )
