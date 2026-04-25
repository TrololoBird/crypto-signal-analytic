"""Turtle Soup / Judas Swing setup detector.

Detects false breakouts of 20-bar rolling high/low on work_1h.
Price breaks above/below the range but closes back inside = sweep setup.

# WINDSURF_REVIEW: unified + vectorized + 1H context + graded
"""
from __future__ import annotations

from typing import cast

import logging
import math
import polars as pl

from ..config import BotSettings
from ..features import _swing_points
from ..models import PreparedSymbol, Signal
from ..setup_base import BaseSetup
from ..setups import _build_signal, _compute_dynamic_score, _reject
from ..setups.utils import get_dynamic_params

LOG = logging.getLogger("bot.strategies.turtle_soup")


def _as_float(value: object, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    return default


class TurtleSoupSetup(BaseSetup):
    setup_id = "turtle_soup"
    family = "reversal"
    confirmation_profile = "countertrend_exhaustion"
    required_context = ("futures_flow",)

    def get_optimizable_params(self, settings: BotSettings | None = None) -> dict[str, float]:
        """Tunable parameters for self-learner optimization."""
        defaults = {
            "base_score": 0.52,
            "roll_bars": 20.0,
            "break_atr_mult": 0.1,
            "sl_buffer_atr": 0.5,
            "volume_threshold": 1.0,
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
        try:
            return self._detect(prepared, settings)
        except Exception:
            _reject(prepared, self.setup_id, "unexpected_exception")
            LOG.exception("%s turtle_soup: unexpected error", prepared.symbol)
            return None

    def _detect(self, prepared: PreparedSymbol, settings: BotSettings) -> Signal | None:
        setup_id = self.setup_id
        
        dynamic_params = get_dynamic_params(prepared, setup_id)
        defaults = self.get_optimizable_params(settings)
        roll_bars = max(5, int(dynamic_params.get("roll_bars", defaults["roll_bars"])))
        break_atr_mult = float(dynamic_params.get("break_atr_mult", defaults["break_atr_mult"]))
        sl_buffer_atr = float(dynamic_params.get("sl_buffer_atr", defaults["sl_buffer_atr"]))
        volume_threshold = float(dynamic_params.get("volume_threshold", defaults["volume_threshold"]))
        min_rr = float(dynamic_params.get("min_rr", defaults["min_rr"]))
        base_score = float(dynamic_params.get("base_score", defaults["base_score"]))
        
        w1h = prepared.work_1h
        if w1h.height < roll_bars + 3:
            _reject(prepared, setup_id, "insufficient_1h_bars", bars=w1h.height)
            return None

        atr = _as_float(w1h.item(-1, "atr14"))
        if atr <= 0 or math.isnan(atr):
            _reject(prepared, setup_id, "atr_invalid", atr=atr)
            return None

        price = prepared.mark_price or prepared.universe.last_price
        if not price or price <= 0:
            _reject(prepared, setup_id, "price_missing")
            return None

        # Rolling high/low over prior bars (excluding last bar = the sweep bar)
        roll_window = w1h.slice(-(roll_bars + 1), roll_bars)
        rolling_high = _as_float(roll_window["high"].max())
        rolling_low = _as_float(roll_window["low"].min())

        bar_high = _as_float(w1h.item(-1, "high"))
        bar_low = _as_float(w1h.item(-1, "low"))
        bar_close = _as_float(w1h.item(-1, "close"))
        direction = None
        wick_extreme = None

        # Long setup: price swept lows (bar.low < rolling_low - break_atr_mult*atr) but close back above
        if bar_low < rolling_low - break_atr_mult * atr and bar_close > rolling_low:
            direction = "long"
            wick_extreme = bar_low

        # Short setup: price swept highs (bar.high > rolling_high + break_atr_mult*atr) but close back below
        elif bar_high > rolling_high + break_atr_mult * atr and bar_close < rolling_high:
            direction = "short"
            wick_extreme = bar_high

        if direction is None or wick_extreme is None:
            _reject(prepared, setup_id, "no_false_breakout_detected")
            return None

        # Confirm on 15m: first bar closes in direction of reversal with volume > avg
        w15m = prepared.work_15m
        if w15m.height < 3:
            _reject(prepared, setup_id, "insufficient_15m_bars", bars=w15m.height)
            return None
        vol_ratio_15m = _as_float(w15m.item(-1, "volume_ratio20"), 1.0)
        bar15_open = _as_float(w15m.item(-1, "open"))
        bar15_close = _as_float(w15m.item(-1, "close"))

        if direction == "long" and not (bar15_close > bar15_open and vol_ratio_15m >= volume_threshold):
            _reject(prepared, setup_id, "15m_confirmation_missing_long", vol_ratio_15m=vol_ratio_15m)
            return None
        if direction == "short" and not (bar15_close < bar15_open and vol_ratio_15m >= volume_threshold):
            _reject(prepared, setup_id, "15m_confirmation_missing_short", vol_ratio_15m=vol_ratio_15m)
            return None

        # --- Compute structural SL/TP ---
        from ..features import _swing_points as _sp
        if direction == "long":
            # SL: beyond false breakout extreme + sl_buffer_atr×ATR
            stop = wick_extreme - sl_buffer_atr * atr
            risk = bar_close - stop
            if risk <= 0:
                _reject(prepared, setup_id, "risk_non_positive_long", stop=stop, close=bar_close)
                return None
            # TP1: the broken level itself (rolling low that was falsely broken)
            tp1 = rolling_low
            sh_mask, _ = _sp(w1h, n=3)
            sh_prices = w1h.filter(sh_mask)["high"]
            tp2_candidates = sh_prices.filter(sh_prices > bar_close)
            tp2 = _as_float(tp2_candidates[0]) if tp2_candidates.len() > 0 else None
        else:
            # SL: beyond false breakout extreme + sl_buffer_atr×ATR
            stop = wick_extreme + sl_buffer_atr * atr
            risk = stop - bar_close
            if risk <= 0:
                _reject(prepared, setup_id, "risk_non_positive_short", stop=stop, close=bar_close)
                return None
            # TP1: the broken level itself (rolling high that was falsely broken)
            tp1 = rolling_high
            _, sl_mask = _sp(w1h, n=3)
            sl_prices = w1h.filter(sl_mask)["low"]
            tp2_candidates = sl_prices.filter(sl_prices < bar_close)
            tp2 = _as_float(tp2_candidates[-1]) if tp2_candidates.len() > 0 else None

        # Validate: TP1 must be at least configured risk distance, else reject.
        if tp1 is None or abs(tp1 - bar_close) < risk * min_rr:
            _reject(prepared, setup_id, "tp1_too_close_or_missing", tp1=tp1, risk=risk)
            return None  # Reject this turtle soup setup
        if tp2 is None:
            tp2 = tp1  # Use TP1 as TP2 if no extended target found

        vol_ratio = float(w1h.item(-1, "volume_ratio20") or 1.0)
        rsi = float(w1h.item(-1, "rsi14") or 50.0)
        score = _compute_dynamic_score(
            direction=direction,
            base_score=base_score,
            vol_ratio=vol_ratio,
            rsi=rsi,
        )

        reasons = [
            f"Turtle soup {direction}: roll_high={rolling_high:.4f} roll_low={rolling_low:.4f}",
            f"wick_extreme={wick_extreme:.4f} close={bar_close:.4f}",
            f"15m vol_ratio={vol_ratio_15m:.2f}",
        ]

        return _build_signal(
            prepared=prepared,
            setup_id=self.setup_id,
            direction=direction,
            score=score,
            timeframe="15m+1h",
            reasons=reasons,
            stop=stop,
            tp1=tp1,
            tp2=tp2,
            price_anchor=bar_close,
            atr=atr,
        )
