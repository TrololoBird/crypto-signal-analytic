"""Order Block setup detector.

Finds the last bearish candle before a bullish impulse (long OB) or
last bullish candle before a bearish impulse (short OB) on work_1h.
Enters when current price is inside the OB zone.

# WINDSURF_REVIEW: unified + vectorized + 1H context + graded
"""
from __future__ import annotations

import logging
import math

from ..config import BotSettings
from ..models import PreparedSymbol, Signal
from ..setup_base import BaseSetup
from ..setups import _build_signal, _compute_dynamic_score, _reject
from ..setups.utils import (
    build_structural_targets,
    validate_rr_or_penalty,
    get_dynamic_params,
)

LOG = logging.getLogger("bot.strategies.order_block")

_MAX_OB_AGE = 30  # 1h bars


class OrderBlockSetup(BaseSetup):
    setup_id = "order_block"
    family = "continuation"
    confirmation_profile = "trend_follow"
    required_context = ("futures_flow",)

    def get_optimizable_params(self, settings: BotSettings | None = None) -> dict[str, float]:
        """Tunable parameters for self-learner optimization."""
        defaults = {
            "base_score": 0.52,
            "min_ob_impulse_atr": 1.5,
            "ob_max_age": 30.0,
            "bias_mismatch_penalty": 0.75,
            "tp_too_close_penalty": 0.75,
            "min_rr": 1.5,
            "rsi_overbought": 76.0,
            "rsi_oversold": 24.0,
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
            LOG.exception("%s order_block: unexpected error", prepared.symbol)
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

        w1h = prepared.work_1h
        if w1h.height < 10:
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

        closes = w1h["close"].to_numpy()
        opens = w1h["open"].to_numpy()
        highs = w1h["high"].to_numpy()
        lows = w1h["low"].to_numpy()
        n_bars = w1h.height

        min_ob_impulse_atr = dynamic_params.get("min_ob_impulse_atr", defaults["min_ob_impulse_atr"])
        ob_max_age = dynamic_params.get("ob_max_age", defaults["ob_max_age"])

        def _find_ob(direction: str):
            """Scan newest-to-oldest for most recent OB. Returns (ob_low, ob_high, age) or None."""
            impulse_dir = 1 if direction == "long" else -1
            for start in range(n_bars - 4, max(n_bars - int(ob_max_age) - 4, 1), -1):
                # Check 3+ consecutive impulse candles
                impulse_end = start + 3
                if impulse_end >= n_bars:
                    continue
                impulse_moves = []
                for k in range(start, min(start + 3, n_bars)):
                    move = closes[k] - opens[k]
                    impulse_moves.append(move * impulse_dir)
                if sum(1 for m in impulse_moves if m > 0) < 2:
                    continue
                total_move = closes[min(start + 2, n_bars - 1)] - opens[start]
                if abs(total_move) < min_ob_impulse_atr * atr:
                    continue
                ob_idx = start - 1
                if ob_idx < 0:
                    continue
                ob_candle_dir = closes[ob_idx] - opens[ob_idx]
                # For long OB: need bearish candle before bullish impulse
                if direction == "long" and ob_candle_dir >= 0:
                    continue
                # For short OB: need bullish candle before bearish impulse
                if direction == "short" and ob_candle_dir <= 0:
                    continue
                age = n_bars - 1 - ob_idx
                if age > ob_max_age:
                    continue
                return lows[ob_idx], highs[ob_idx], age
            return None

        signal = None
        for direction in ("long", "short"):
            result = _find_ob(direction)
            if result is None:
                continue
            ob_low, ob_high, age = result

            if not (ob_low <= price <= ob_high):
                continue

            # Use 1H context for 15M signals (not 4H - too lagging for <4h trades)
            bias_1h = getattr(prepared, 'bias_1h', prepared.bias_4h)
            
            # 1h structure must not oppose direction (graded penalty instead of reject)
            structure_1h = prepared.structure_1h
            
            # RSI extremes filter
            rsi_check = float(w1h.item(-1, "rsi14") or 50.0)

            # --- Compute structural SL/TP via unified utility ---
            from ..features import _swing_points as _sp
            sh_mask, sl_mask = _sp(w1h, n=3)
            
            if direction == "long":
                # SL: beyond OB zone boundary + 0.15×ATR (close below OB = invalidated)
                stop = ob_low - 0.5 * atr  # Increased from 0.15
                stop_basis = ob_low
            else:
                # SL: beyond OB zone boundary + 0.15×ATR
                stop = ob_high + 0.5 * atr  # Increased from 0.15
                stop_basis = ob_high
            
            stop_calc, tp1, tp2 = build_structural_targets(
                direction=direction,
                price_anchor=price,
                stop_basis=stop_basis,
                atr=atr,
                work_1h=w1h,
                min_rr=dynamic_params.get("min_rr", defaults["min_rr"]),
                sh_mask=sh_mask,
                sl_mask=sl_mask,
            )
            
            # Use calculated stop if valid, else fall back
            stop = stop_calc if stop_calc != stop_basis else stop
            risk = abs(price - stop)
            
            # Graded RR validation (penalty instead of reject)
            min_rr = dynamic_params.get("min_rr", defaults["min_rr"])
            is_valid_rr, _ = validate_rr_or_penalty(price, stop, tp1, min_rr)

            vol_ratio = float(w1h.item(-1, "volume_ratio20") or 1.0)
            rsi = float(w1h.item(-1, "rsi14") or 50.0)
            
            base_score = dynamic_params.get("base_score", defaults["base_score"])
            score = _compute_dynamic_score(
                direction=direction,
                base_score=base_score,
                vol_ratio=vol_ratio,
                rsi=rsi,
            )
            
            # Apply graded penalties
            if direction == "long" and bias_1h == "downtrend":
                score *= dynamic_params.get("bias_mismatch_penalty", defaults["bias_mismatch_penalty"])
            if direction == "short" and bias_1h == "uptrend":
                score *= dynamic_params.get("bias_mismatch_penalty", defaults["bias_mismatch_penalty"])
            
            if direction == "long" and structure_1h == "downtrend":
                score *= dynamic_params.get("bias_mismatch_penalty", defaults["bias_mismatch_penalty"])
            if direction == "short" and structure_1h == "uptrend":
                score *= dynamic_params.get("bias_mismatch_penalty", defaults["bias_mismatch_penalty"])
            
            # RSI extremes with graded penalty
            rsi_overbought = dynamic_params.get("rsi_overbought", defaults["rsi_overbought"])
            rsi_oversold = dynamic_params.get("rsi_oversold", defaults["rsi_oversold"])
            if direction == "long" and rsi_check > rsi_overbought:
                score *= 0.85
            if direction == "short" and rsi_check < rsi_oversold:
                score *= 0.85
            
            # TP too close penalty
            if not is_valid_rr and tp1 is not None:
                score *= dynamic_params.get("tp_too_close_penalty", defaults["tp_too_close_penalty"])
            
            if tp1 is None:
                _reject(prepared, setup_id, "tp1_missing", direction=direction, price=price)
                continue
            if tp2 is None:
                tp2 = tp1  # Use TP1 as TP2 if no extended target found

            reasons = [
                f"OB {direction}: zone [{ob_low:.4f}-{ob_high:.4f}]",
                f"age={age}bars price={price:.4f} | 1h_bias={bias_1h} 1h_struct={structure_1h}",
                f"rsi={rsi_check:.1f}",
            ]

            signal = _build_signal(
                prepared=prepared,
                setup_id=self.setup_id,
                direction=direction,
                score=score,
                timeframe="1h",
                reasons=reasons,
                stop=stop,
                tp1=tp1,
                tp2=tp2,
                price_anchor=price,
                atr=atr,
            )
            if signal:
                break
        if signal is None:
            _reject(prepared, setup_id, "no_order_block_detected")
        return signal
