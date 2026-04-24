"""Extreme Funding Rate Reversal setup detector.

Triggers when funding rate is extreme (abs > 0.05%) and a reversal
candle pattern is confirmed on 15m with elevated volume.

# WINDSURF_REVIEW: unified + vectorized + 1H context + graded
"""
from __future__ import annotations

import logging
import math

from ..setup_base import BaseSetup
from ..config import BotSettings
from ..config_loader import load_strategy_config, get_nested
from ..models import PreparedSymbol, Signal
from ..setups import _build_signal, _compute_dynamic_score, _reject
from ..setups.utils import get_dynamic_params

LOG = logging.getLogger("bot.strategies.funding_reversal")

_FUNDING_THRESHOLD = 0.0005  # 0.05%


def _as_float(value: object, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    return default


class FundingReversalSetup(BaseSetup):
    setup_id = "funding_reversal"
    family = "reversal"
    confirmation_profile = "countertrend_exhaustion"
    required_context = ("futures_flow",)
    requires_funding = True

    def get_optimizable_params(self, settings: BotSettings | None = None) -> dict[str, float]:
        """Tunable parameters for self-learner optimization."""
        defaults = {
            "base_score": 0.52,
            "funding_threshold": 0.0005,
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
            LOG.exception("%s funding_reversal: unexpected error", prepared.symbol)
            return None

    def _detect(self, prepared: PreparedSymbol, settings: BotSettings) -> Signal | None:
        setup_id = self.setup_id
        
        # Load config-driven parameters
        config = load_strategy_config("funding_reversal")
        funding_threshold = get_nested(config, "funding.funding_threshold", 0.0005)
        funding_trend_bars = get_nested(config, "funding.funding_trend_bars", 3)
        min_delta_threshold = get_nested(config, "detection.min_delta_threshold", 0.1)
        sl_buffer_atr = get_nested(config, "risk_management.sl_buffer_atr", 0.5)
        
        if prepared.funding_rate is None:
            _reject(prepared, setup_id, "funding_rate_missing")
            return None
        fr = prepared.funding_rate
        if math.isnan(fr) or abs(fr) <= funding_threshold:
            _reject(prepared, setup_id, "funding_not_extreme", funding_rate=fr)
            return None

        # Require funding trend to avoid one-bar spikes that immediately mean-revert.
        # "rising" confirms longs are building (→ short reversal) or funding is
        # falling (accumulation squeeze → long reversal).  "flat" = not trending →
        # spike anomaly, skip.  None = no history yet → allow through.
        funding_trend = prepared.funding_trend
        if funding_trend == "flat":
            _reject(prepared, setup_id, "funding_trend_flat")
            return None
        if fr > _FUNDING_THRESHOLD and funding_trend == "falling":
            # Funding already unwinding on its own — not a setup
            _reject(prepared, setup_id, "funding_already_unwinding_short", funding_trend=funding_trend)
            return None
        if fr < -_FUNDING_THRESHOLD and funding_trend == "rising":
            # Negative funding already recovering — not a setup
            _reject(prepared, setup_id, "funding_already_unwinding_long", funding_trend=funding_trend)
            return None

        w = prepared.work_15m
        if w.height < 5:
            _reject(prepared, setup_id, "insufficient_15m_bars", bars=w.height)
            return None

        atr = _as_float(w.item(-1, "atr14"))
        if atr <= 0 or math.isnan(atr):
            _reject(prepared, setup_id, "atr_invalid", atr=atr)
            return None

        price = prepared.mark_price or prepared.universe.last_price
        if not price or price <= 0:
            _reject(prepared, setup_id, "price_missing")
            return None

        vol_ratio = _as_float(w.item(-1, "volume_ratio20"), 1.0)
        if vol_ratio < 1.2:
            _reject(prepared, setup_id, "volume_too_low", vol_ratio=vol_ratio)
            return None

        bar_open = _as_float(w.item(-1, "open"))
        bar_close = _as_float(w.item(-1, "close"))
        bar_high = _as_float(w.item(-1, "high"))
        bar_low = _as_float(w.item(-1, "low"))
        body = abs(bar_close - bar_open)

        if fr > _FUNDING_THRESHOLD:
            # Extreme longs → look for short reversal
            # Confirm: close < open, upper wick > body * 1.5
            upper_wick = bar_high - max(bar_open, bar_close)
            if not (bar_close < bar_open and body > 0 and upper_wick >= body * 1.5):
                _reject(prepared, setup_id, "reversal_candle_missing_short")
                return None
            direction = "short"
            # SL: beyond the extreme funding candle's wick tip + 0.1×ATR
            stop = bar_high + atr * 0.1
            risk = stop - price
            if risk <= 0:
                _reject(prepared, setup_id, "risk_non_positive_short", stop=stop, price=price)
                return None
            # TP1: funding mean-reversion level (mark price before funding spike = recent low)
            recent_lows = w["low"].slice(-10, 9)  # last 10 excluding current
            tp1 = _as_float(recent_lows.min()) if recent_lows.len() > 0 else None
            # TP2: prior structural level (1h swing low)
            from ..features import _swing_points as _sp
            w1h = prepared.work_1h
            tp2 = None
            if w1h.height > 5:
                sh_mask, sl_mask = _sp(w1h, n=3)
                sl_prices = w1h.filter(sl_mask)["low"]
                tp2_cands = sl_prices.filter(sl_prices < price)
                tp2 = _as_float(tp2_cands[-1]) if tp2_cands.len() > 0 else None
        else:
            # Extreme shorts → look for long reversal
            # Confirm: close > open, lower wick > body * 1.5
            lower_wick = min(bar_open, bar_close) - bar_low
            if not (bar_close > bar_open and body > 0 and lower_wick >= body * 1.5):
                _reject(prepared, setup_id, "reversal_candle_missing_long")
                return None
            direction = "long"
            # SL: beyond the extreme funding candle's wick tip + 0.1×ATR
            stop = bar_low - atr * 0.1
            risk = price - stop
            if risk <= 0:
                _reject(prepared, setup_id, "risk_non_positive_long", stop=stop, price=price)
                return None
            # TP1: funding mean-reversion level (mark price before funding spike = recent high)
            recent_highs = w["high"].slice(-10, 9)  # last 10 excluding current
            tp1 = _as_float(recent_highs.max()) if recent_highs.len() > 0 else None
            # TP2: prior structural level (1h swing high)
            from ..features import _swing_points as _sp
            w1h = prepared.work_1h
            tp2 = None
            if w1h.height > 5:
                sh_mask, _ = _sp(w1h, n=3)
                sh_prices = w1h.filter(sh_mask)["high"]
                tp2_cands = sh_prices.filter(sh_prices > price)
                tp2 = _as_float(tp2_cands[0]) if tp2_cands.len() > 0 else None

        # Validate: TP1 must be at least 1.5× risk distance, else reject
        if tp1 is None or abs(tp1 - price) < risk * 1.5:
            _reject(prepared, setup_id, "tp1_too_close_or_missing", tp1=tp1, risk=risk)
            return None  # Reject this funding reversal setup
        if tp2 is None:
            tp2 = tp1  # Use TP1 as TP2 if no extended target found

        rsi = _as_float(w.item(-1, "rsi14"), 50.0)
        score = _compute_dynamic_score(
            direction=direction,
            base_score=0.50,
            vol_ratio=vol_ratio,
            rsi=rsi,
        )

        reasons = [
            f"Funding reversal {direction}: fr={fr:.5f} trend={funding_trend or 'unknown'}",
            f"vol_ratio={vol_ratio:.2f} reversal_candle=yes",
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
            price_anchor=price,
            atr=atr,
        )
