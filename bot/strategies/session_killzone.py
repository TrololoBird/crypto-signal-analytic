"""Session Killzone setup detector.

Triggers during high-probability trading sessions (London Open, NY Open, Asia Range Break).
Requires directional momentum with volume confirmation during the killzone window.

# WINDSURF_REVIEW: unified + vectorized + 1H context + graded
"""
from __future__ import annotations

import logging
import math
from datetime import datetime, timezone
from typing import cast

import polars as pl

from ..config import BotSettings

from ..setup_base import BaseSetup
from ..models import PreparedSymbol, Signal
from ..setups import _build_signal, _compute_dynamic_score, _reject
from ..setups.utils import get_dynamic_params

LOG = logging.getLogger("bot.strategies.session_killzone")


def _as_float(value: object, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    return default

# (start_hour_utc, end_hour_utc)
_KILLZONES = [
    (7, 10),   # London Open
    (13, 16),  # NY Open
    (0, 3),    # Asia Range Break
]


def _in_killzone(hour: int) -> bool:
    for start, end in _KILLZONES:
        if start <= hour < end:
            return True
    return False


class SessionKillzoneSetup(BaseSetup):
    setup_id = "session_killzone"
    family = "breakout"
    confirmation_profile = "breakout_acceptance"
    required_context = ("futures_flow",)

    def get_optimizable_params(self, settings: BotSettings | None = None) -> dict[str, float]:
        """Tunable parameters for self-learner optimization."""
        defaults = {
            "base_score": 0.55,
            "min_volume_ratio": 1.2,
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
        setup_id = self.setup_id
        
        
        try:
            return self._detect(prepared, settings)
        except Exception:
            _reject(prepared, self.setup_id, "unexpected_exception")
            LOG.exception("%s session_killzone: unexpected error", prepared.symbol)
            return None

    def _detect(self, prepared: PreparedSymbol, settings: BotSettings) -> Signal | None:
        setup_id = self.setup_id
        last_bar_time = prepared.work_15m.item(-1, "time")
        now_utc = last_bar_time if isinstance(last_bar_time, datetime) else datetime.now(timezone.utc)
        if not _in_killzone(now_utc.hour):
            _reject(prepared, setup_id, "outside_killzone", hour=now_utc.hour)
            return None

        w = prepared.work_15m
        if w.height < 20:
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

        # ADX check on 1h
        w1h = prepared.work_1h
        if w1h.height < 3:
            _reject(prepared, setup_id, "insufficient_1h_bars", bars=w1h.height)
            return None
        adx_1h = _as_float(w1h.item(-1, "adx14"))
        if adx_1h < 18:
            _reject(prepared, setup_id, "adx_too_low", adx_1h=adx_1h)
            return None

        # Last 3 bars directional check with volume
        last3 = w.tail(3)
        opens = last3["open"].to_numpy()
        closes = last3["close"].to_numpy()
        if "volume_ratio20" not in last3.columns:
            _reject(prepared, setup_id, "volume_ratio_missing")
            return None
        vol_ratios = last3["volume_ratio20"].to_numpy()

        if any(math.isnan(v) for v in vol_ratios):
            _reject(prepared, setup_id, "volume_ratio_nan")
            return None
        avg_vol_ratio = float(sum(vol_ratios) / len(vol_ratios))
        if avg_vol_ratio < 1.15:
            _reject(prepared, setup_id, "average_volume_too_low", avg_vol_ratio=avg_vol_ratio)
            return None

        bullish_bars = sum(1 for o, c in zip(opens, closes) if c > o)
        bearish_bars = sum(1 for o, c in zip(opens, closes) if c < o)

        if bullish_bars >= 2:
            direction = "long"
        elif bearish_bars >= 2:
            direction = "short"
        else:
            _reject(prepared, setup_id, "directional_momentum_missing")
            return None

        # --- Structural SL: beyond session high/low (killzone boundary) + 0.15×ATR ---
        scan20 = w.tail(20)
        session_high = _as_float(scan20["high"].max())
        session_low = _as_float(scan20["low"].min())

        # TP targets: prior session's major levels and killzone range midpoint
        session_mid = (session_high + session_low) / 2.0

        # Look for prior session levels from 1h data
        from ..features import _swing_points as _sp
        w1h = prepared.work_1h

        if direction == "long":
            # SL: beyond session low (killzone boundary) + 0.15×ATR
            stop = session_low - atr * 0.15
            risk = price - stop
            if risk <= 0:
                _reject(prepared, setup_id, "risk_non_positive_long", stop=stop, price=price)
                return None
            # TP1: prior session's major level (previous 1h swing high or session high)
            tp1 = None
            if w1h.height > 5:
                sh_mask, sl_mask = _sp(w1h, n=3)
                sh_prices = w1h.filter(sh_mask)["high"]
                tp1_cands = sh_prices.filter(sh_prices > price)
                tp1 = float(tp1_cands[0]) if tp1_cands.len() > 0 else None
            # TP2: next killzone range midpoint (above)
            killzone_range = session_high - session_low
            tp2 = session_high + killzone_range * 0.5 if killzone_range > 0 else None
        else:
            # SL: beyond session high (killzone boundary) + 0.15×ATR
            stop = session_high + atr * 0.15
            risk = stop - price
            if risk <= 0:
                _reject(prepared, setup_id, "risk_non_positive_short", stop=stop, price=price)
                return None
            # TP1: prior session's major level (previous 1h swing low)
            tp1 = None
            if w1h.height > 5:
                _, sl_mask = _sp(w1h, n=3)
                sl_prices = w1h.filter(sl_mask)["low"]
                tp1_cands = sl_prices.filter(sl_prices < price)
                tp1 = float(tp1_cands[-1]) if tp1_cands.len() > 0 else None
            # TP2: next killzone range midpoint (below)
            killzone_range = session_high - session_low
            tp2 = session_low - killzone_range * 0.5 if killzone_range > 0 else None

        # Validate: TP1 must be at least 1.5× risk distance, else reject
        if tp1 is None or abs(tp1 - price) < risk * 1.5:
            _reject(prepared, setup_id, "tp1_too_close_or_missing", tp1=tp1, risk=risk)
            return None  # Reject this session killzone setup
        if tp2 is None:
            tp2 = tp1  # Use TP1 as TP2 if no extended target found

        rsi = float(w.item(-1, "rsi14") or 50.0)
        vol_ratio = float(w.item(-1, "volume_ratio20") or 1.0)
        score = _compute_dynamic_score(
            direction=direction,
            base_score=0.48,
            vol_ratio=vol_ratio,
            rsi=rsi,
        )

        session_name = "unknown"
        h = now_utc.hour
        if 7 <= h < 10:
            session_name = "London"
        elif 13 <= h < 16:
            session_name = "NY"
        elif 0 <= h < 3:
            session_name = "Asia"

        reasons = [
            f"Session killzone {direction}: {session_name} {now_utc.strftime('%H:%M')}UTC",
            f"adx1h={adx_1h:.1f} avg_vol={avg_vol_ratio:.2f}",
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
