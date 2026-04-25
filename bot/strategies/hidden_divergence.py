"""Hidden Divergence setup detector.

Hidden Bullish: price higher low + RSI lower low (continuation long)
Hidden Bearish: price lower high + RSI higher high (continuation short)

Uses swing points on work_1h for structure detection.
Requires 1H trend alignment (continuation signal).

# WINDSURF_REVIEW: unified + vectorized + 1H context + graded
"""
from __future__ import annotations

import logging
import math
from typing import cast

import polars as pl

from ..setup_base import BaseSetup
from ..config import BotSettings
from ..models import PreparedSymbol, Signal
from ..setups import _build_signal, _compute_dynamic_score, _reject
from ..features import _swing_points
from ..setups.utils import (
    build_structural_targets,
    validate_rr_or_penalty,
    get_dynamic_params,
)

LOG = logging.getLogger("bot.strategies.hidden_divergence")


class HiddenDivergenceSetup(BaseSetup):
    setup_id = "hidden_divergence"
    family = "continuation"
    confirmation_profile = "trend_follow"
    required_context = ("futures_flow",)

    def get_optimizable_params(self, settings: BotSettings | None = None) -> dict[str, float]:
        """Tunable parameters for self-learner optimization."""
        defaults = {
            "base_score": 0.50,
            "min_swings": 2.0,
            "bias_mismatch_penalty": 0.75,
            "tp_too_close_penalty": 0.75,
            "min_rr": 1.5,
            "rsi_divergence_threshold": 5.0,
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
        except Exception as exc:
            LOG.exception("%s hidden_divergence: unexpected error", prepared.symbol)
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
        
        rsi_divergence_lookback = int(dynamic_params.get("rsi_divergence_lookback", defaults["rsi_divergence_lookback"]))
        min_delta_threshold = dynamic_params.get("min_delta_threshold", defaults["min_delta_threshold"])
        sl_buffer_atr = dynamic_params.get("sl_buffer_atr", defaults["sl_buffer_atr"])

        w1h = prepared.work_1h
        if w1h.height < 20:
            _reject(prepared, setup_id, "insufficient_1h_bars", bars=w1h.height)
            return None

        atr = float(w1h.item(-1, "atr14") or 0.0)
        if atr <= 0 or math.isnan(atr):
            _reject(prepared, setup_id, "atr_invalid", atr=atr)
            return None

        price = prepared.mark_price or prepared.universe.last_price
        if not price or price <= 0:
            _reject(prepared, setup_id, "price_missing")
            return None

        w15m = prepared.work_15m
        if w15m.height < 3:
            _reject(prepared, setup_id, "insufficient_15m_bars", bars=w15m.height)
            return None
        vol_ratio_15m = float(w15m.item(-1, "volume_ratio20") or 1.0)
        if vol_ratio_15m < 1.1:
            _reject(prepared, setup_id, "volume_too_low", vol_ratio_15m=vol_ratio_15m)
            return None

        # 1H context for 15M signals (not 4H - too lagging for <4h trades)
        bias_1h = getattr(prepared, 'bias_1h', prepared.bias_4h)
        regime_1h = getattr(prepared, 'regime_1h_confirmed', prepared.regime_4h_confirmed)

        sh_mask, sl_mask = _swing_points(w1h, n=3, include_unconfirmed_tail=True)
        sh_prices = w1h.filter(sh_mask)["high"]
        sh_rsi = w1h.filter(sh_mask)["rsi14"] if "rsi14" in w1h.columns else None
        sl_prices = w1h.filter(sl_mask)["low"]
        sl_rsi = w1h.filter(sl_mask)["rsi14"] if "rsi14" in w1h.columns else None

        # Use 1H context for 15M signals (not 4H - too lagging for <4h trades)
        bias_1h = getattr(prepared, 'bias_1h', prepared.bias_4h)
        direction = None
        stop_price = None
        swing_ref = None

        # Hidden Bullish: price HL (sl[-1] > sl[-2]) + RSI LL (rsi_sl[-1] < rsi_sl[-2])
        impulse_size = None
        swing_ref = None
        if (bias_1h in ("uptrend", "neutral")
                and sl_prices.len() >= 2
                and sl_rsi is not None and sl_rsi.len() >= 2):
            sl_v = sl_prices.to_numpy()
            sl_r = sl_rsi.to_numpy()
            if sl_v[-1] > sl_v[-2] and sl_r[-1] < sl_r[-2]:
                direction = "long"
                swing_ref = float(sl_v[-1])
                # Compute last impulse wave size for Fib extensions
                if sh_prices.len() >= 1:
                    impulse_size = abs(float(sh_prices.to_numpy()[-1]) - float(sl_v[-1]))

        # Hidden Bearish: price LH (sh[-1] < sh[-2]) + RSI HH (rsi_sh[-1] > rsi_sh[-2])
        if direction is None and (bias_1h in ("downtrend", "neutral")
                and sh_prices.len() >= 2
                and sh_rsi is not None and sh_rsi.len() >= 2):
            sh_v = sh_prices.to_numpy()
            sh_r = sh_rsi.to_numpy()
            if sh_v[-1] < sh_v[-2] and sh_r[-1] > sh_r[-2]:
                direction = "short"
                swing_ref = float(sh_v[-1])
                if sl_prices.len() >= 1:
                    impulse_size = abs(float(sh_v[-1]) - float(sl_prices.to_numpy()[-1]))

        if direction is None or swing_ref is None:
            _reject(prepared, setup_id, "no_hidden_divergence_detected")
            return None

        # 4H trend must align for continuation
        if direction == "long" and bias_1h == "downtrend":
            _reject(prepared, setup_id, "context_bias_blocks_long", bias_1h=bias_1h)
            return None
        if direction == "short" and bias_1h == "uptrend":
            _reject(prepared, setup_id, "context_bias_blocks_short", bias_1h=bias_1h)
            return None

        # --- Compute structural SL/TP ---
        if direction == "long":
            # SL: beyond hidden divergence extreme (swing low) + 0.15×ATR
            stop_price = swing_ref - atr * 0.15
            risk = price - stop_price
            if risk <= 0:
                _reject(prepared, setup_id, "risk_non_positive_long", stop=stop_price, price=price)
                return None
            # TP1/TP2: Fibonacci 1.272× and 1.618× extension of last impulse wave
            if impulse_size and impulse_size > 0:
                tp1 = price + impulse_size * 1.272
                tp2 = price + impulse_size * 1.618
            else:
                tp1 = None
                tp2 = None
        else:
            # SL: beyond hidden divergence extreme (swing high) + 0.15×ATR
            stop_price = swing_ref + atr * 0.15
            risk = stop_price - price
            if risk <= 0:
                _reject(prepared, setup_id, "risk_non_positive_short", stop=stop_price, price=price)
                return None
            # TP1/TP2: Fibonacci extensions of last impulse wave
            if impulse_size and impulse_size > 0:
                tp1 = price - impulse_size * 1.272
                tp2 = price - impulse_size * 1.618
            else:
                tp1 = None
                tp2 = None

        # Validate: TP1 must be at least 1.5× risk distance, else reject
        if tp1 is None or abs(tp1 - price) < risk * 1.5:
            _reject(prepared, setup_id, "tp1_too_close_or_missing", tp1=tp1, risk=risk, price=price)
            return None  # Reject this hidden divergence setup
        if tp2 is None or abs(tp2 - price) <= abs(tp1 - price):
            tp2 = tp1  # Use TP1 as TP2 if no extended target found

        rsi = float(w1h.item(-1, "rsi14") or 50.0)
        vol_ratio = float(w1h.item(-1, "volume_ratio20") or 1.0)
        score = _compute_dynamic_score(
            direction=direction,
            base_score=0.48,
            vol_ratio=vol_ratio,
            rsi=rsi,
        )

        reasons = [
            f"Hidden div {direction}: swing_ref={swing_ref:.4f}",
            f"vol_ratio_15m={vol_ratio_15m:.2f} 4h={bias_1h}",
        ]

        return _build_signal(
            prepared=prepared,
            setup_id=self.setup_id,
            direction=direction,
            score=score,
            timeframe="15m+1h",
            reasons=reasons,
            strategy_family=self.family,
            stop=stop_price,
            tp1=tp1,
            tp2=tp2,
            price_anchor=price,
            atr=atr,
        )
