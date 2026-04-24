"""Liquidity Sweep setup detector.

Detects sweep of equal highs/lows (liquidity pools) on work_1h.
Equal levels = 2+ peaks within 0.15% of each other in last 30 bars.
Sweep = recent bar's wick breaks the level but closes back inside.

# WINDSURF_REVIEW: unified + vectorized + 1H context + graded
"""
from __future__ import annotations

import logging
import math
from typing import cast

import polars as pl

from ..config import BotSettings
from ..models import PreparedSymbol, Signal
from ..setup_base import BaseSetup
from ..setups import _build_signal, _compute_dynamic_score, _reject
from ..setups.utils import get_dynamic_params

LOG = logging.getLogger("bot.strategies.liquidity_sweep")

_SCAN_BARS = 30
_EQUAL_TOL = 0.0015  # 0.15%


def _as_float(value: object, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    return default


class LiquiditySweepSetup(BaseSetup):
    setup_id = "liquidity_sweep"
    family = "reversal"
    confirmation_profile = "countertrend_exhaustion"
    required_context = ("futures_flow",)

    def get_optimizable_params(self, settings: BotSettings | None = None) -> dict[str, float]:
        """Tunable parameters for self-learner optimization."""
        defaults = {
            "base_score": 0.50,
            "equal_level_tol": 0.0015,
            "min_level_hits": 2,
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
        dynamic_params = get_dynamic_params(prepared, self.setup_id)
        defaults = self.get_optimizable_params(settings)
        sweep_atr_mult = dynamic_params.get("sweep_atr_mult", defaults["sweep_atr_mult"])
        reclaim_threshold = dynamic_params.get("reclaim_threshold", defaults["reclaim_threshold"])
        sl_buffer_atr = dynamic_params.get("sl_buffer_atr", defaults["sl_buffer_atr"])

        try:
            return self._detect(prepared, settings)
        except Exception as exc:
            LOG.exception("%s liquidity_sweep: unexpected error", prepared.symbol)
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

        w = prepared.work_1h
        if w.height < 10:
            _reject(prepared, setup_id, "insufficient_1h_bars", bars=w.height)
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

        scan = w.tail(_SCAN_BARS) if w.height >= _SCAN_BARS else w
        highs = scan["high"].to_numpy()
        lows = scan["low"].to_numpy()
        closes = scan["close"].to_numpy()
        n = len(scan)

        if n < 3:
            _reject(prepared, setup_id, "scan_window_insufficient", bars=n)
            return None

        sweep_bar_h = highs[-1]
        sweep_bar_l = lows[-1]
        sweep_bar_c = closes[-1]
        sweep_bar_o = float(scan["open"][-1])

        # --- Bearish sweep: wick into equal highs, close back below ---
        # Scan newest-to-oldest to find the most recent cluster of equal highs.
        prev_highs = highs[:-1]
        eq_high_level = None
        for ref in prev_highs[::-1]:
            matches = [h for h in prev_highs if abs(h - ref) / ref < _EQUAL_TOL]
            if len(matches) >= 2:
                eq_high_level = ref
                break

        if eq_high_level is not None:
            if sweep_bar_h > eq_high_level and sweep_bar_c < eq_high_level:
                if abs(price - sweep_bar_c) <= 0.3 * atr:
                    # SL: beyond swept liquidity level + 0.5×ATR
                    stop = sweep_bar_h + 0.5 * atr
                    risk = stop - price
                    if risk > 0:
                        # TP1: equilibrium price = origin of the sweep (midpoint of pre-sweep impulse)
                        impulse_low = _as_float(scan["low"].slice(-6, 5).min()) if n > 6 else sweep_bar_l
                        tp1 = min(eq_high_level, price - risk * 1.5)
                        if tp1 >= price:
                            _reject(prepared, setup_id, "tp1_invalid_short", tp1=tp1, price=price)
                            return None
                        # TP2: prior 1h structure on opposite side (swing low)
                        from ..features import _swing_points as _sp
                        _, sl_mask = _sp(w, n=3)
                        sl_prices = w.filter(sl_mask)["low"]
                        tp2_candidates = sl_prices.filter(sl_prices < price)
                        tp2 = _as_float(tp2_candidates[-1]) if tp2_candidates.len() > 0 else None
                        # Validate: TP1 must be at least 1.5× risk distance
                        if abs(tp1 - price) < risk * 1.5:
                            _reject(prepared, setup_id, "tp1_too_close_or_missing", tp1=tp1, risk=risk, price=price)
                            return None  # Reject this sweep setup
                        if tp2 is None or abs(tp2 - price) <= abs(tp1 - price):
                            tp2 = tp1  # Use TP1 as TP2 if no extended target found
                        vol_ratio = _as_float(w.item(-1, "volume_ratio20"), 1.0)
                        rsi = _as_float(w.item(-1, "rsi14"), 50.0)
                        score = _compute_dynamic_score(
                            direction="short",
                            base_score=0.52,
                            vol_ratio=vol_ratio,
                            rsi=rsi,
                        )
                        reasons = [
                            f"Liquidity sweep short: eq_high={eq_high_level:.4f}",
                            f"wick={sweep_bar_h:.4f} close={sweep_bar_c:.4f}",
                        ]
                        return _build_signal(
                            prepared=prepared,
                            setup_id=self.setup_id,
                            direction="short",
                            score=score,
                            timeframe="1h",
                            reasons=reasons,
                            stop=stop,
                            tp1=tp1,
                            tp2=tp2,
                            price_anchor=sweep_bar_c,
                            atr=atr,
                        )

        # --- Bullish sweep: wick into equal lows, close back above ---
        # Scan newest-to-oldest to find the most recent cluster of equal lows.
        prev_lows = lows[:-1]
        eq_low_level = None
        for ref in prev_lows[::-1]:
            matches = [l for l in prev_lows if abs(l - ref) / ref < _EQUAL_TOL]
            if len(matches) >= 2:
                eq_low_level = ref
                break

        if eq_low_level is not None:
            if sweep_bar_l < eq_low_level and sweep_bar_c > eq_low_level:
                if abs(price - sweep_bar_c) <= 0.3 * atr:
                    # SL: beyond swept liquidity level + 0.5×ATR
                    stop = sweep_bar_l - 0.5 * atr
                    risk = price - stop
                    if risk > 0:
                        # TP1: equilibrium price = midpoint of pre-sweep impulse
                        impulse_high = _as_float(scan["high"].slice(-6, 5).max()) if n > 6 else sweep_bar_h
                        tp1 = max(eq_low_level, price + risk * 1.5)
                        if tp1 <= price:
                            _reject(prepared, setup_id, "tp1_invalid_long", tp1=tp1, price=price)
                            return None
                        # TP2: prior 1h structure on opposite side (swing high)
                        from ..features import _swing_points as _sp
                        sh_mask, _ = _sp(w, n=3)
                        sh_prices = w.filter(sh_mask)["high"]
                        tp2_candidates = sh_prices.filter(sh_prices > price)
                        tp2 = _as_float(tp2_candidates[0]) if tp2_candidates.len() > 0 else None
                        # Validate: TP1 must be at least 1.5× risk distance
                        if abs(tp1 - price) < risk * 1.5:
                            _reject(prepared, setup_id, "tp1_too_close_or_missing", tp1=tp1, risk=risk, price=price)
                            return None  # Reject this sweep setup
                        if tp2 is None or abs(tp2 - price) <= abs(tp1 - price):
                            tp2 = tp1  # Use TP1 as TP2 if no extended target found
                        vol_ratio = _as_float(w.item(-1, "volume_ratio20"), 1.0)
                        rsi = _as_float(w.item(-1, "rsi14"), 50.0)
                        score = _compute_dynamic_score(
                            direction="long",
                            base_score=0.52,
                            vol_ratio=vol_ratio,
                            rsi=rsi,
                        )
                        reasons = [
                            f"Liquidity sweep long: eq_low={eq_low_level:.4f}",
                            f"wick={sweep_bar_l:.4f} close={sweep_bar_c:.4f}",
                        ]
                        return _build_signal(
                            prepared=prepared,
                            setup_id=self.setup_id,
                            direction="long",
                            score=score,
                            timeframe="1h",
                            reasons=reasons,
                            stop=stop,
                            tp1=tp1,
                            tp2=tp2,
                            price_anchor=sweep_bar_c,
                            atr=atr,
                        )

        _reject(prepared, setup_id, "no_liquidity_sweep_detected")
        return None
