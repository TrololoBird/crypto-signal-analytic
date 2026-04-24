"""Shared helpers for setup detectors.

Used by all strategy modules in ``bot/strategies/``.
No detect_* functions — those live in individual strategy files.
"""
from __future__ import annotations

import logging
from typing import Any

import polars as pl

from .config import BotSettings
from .features import _swing_points  # shared swing detection helper
from .models import PreparedSymbol, Signal

LOG = logging.getLogger("bot.setups")


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _build_signal(
    *,
    prepared: PreparedSymbol,
    setup_id: str,
    direction: str,
    score: float,
    reasons: list[str],
    stop: float,
    tp1: float,
    tp2: float,
    price_anchor: float,
    atr: float,
    timeframe: str = "15m",
    entry_pad_atr_mult: float = 0.08,
) -> Signal | None:
    if atr <= 0.0 or stop <= 0.0 or tp1 <= 0.0 or tp2 <= 0.0:
        return None
    if direction == "long":
        if stop >= price_anchor or tp1 <= price_anchor or tp2 <= price_anchor:
            return None
    elif direction == "short":
        if stop <= price_anchor or tp1 >= price_anchor or tp2 >= price_anchor:
            return None
    else:
        return None
    entry_pad = max(atr * entry_pad_atr_mult, price_anchor * 0.0005)
    entry_low = price_anchor - entry_pad
    entry_high = price_anchor + entry_pad
    volume_ratio: float | None = None
    if not prepared.work_15m.is_empty():
        try:
            volume_ratio = float(prepared.work_15m.item(-1, "volume_ratio20") or 0.0) or None
        except (TypeError, ValueError):
            pass
    return Signal(
        symbol=prepared.symbol,
        setup_id=setup_id,
        direction=direction,
        score=score,
        timeframe=str(timeframe or "15m"),
        entry_low=min(entry_low, entry_high),
        entry_high=max(entry_low, entry_high),
        stop=stop,
        take_profit_1=tp1,
        take_profit_2=tp2,
        reasons=tuple(reasons),
        bias_4h=prepared.bias_4h,
        quote_volume=prepared.universe.quote_volume,
        mark_price=prepared.mark_price,
        volume_ratio=volume_ratio,
    )


def _compute_dynamic_score(
    *,
    direction: str,
    base_score: float,
    vol_ratio: float = 1.0,
    rsi: float = 50.0,
    structure_clarity: float = 0.5,
) -> float:
    """Compute a quality-adjusted score — pattern confidence ONLY.

    Market context (MTF alignment, crowd, OI, risk/reward) is scored
    separately by ConfluenceEngine.  This function measures how cleanly
    the setup's own pattern was detected:

      - Volume quality: confirmation strength (+0.05 max)
      - RSI health: not at extremes that suggest exhaustion (+0.05 max)
      - Structure clarity: clean context, no competing levels (+0.05 max)

    Final score clamped to [0.35, 0.90].
    """
    score = base_score

    # Volume bonus (up to +0.05)
    score += min(vol_ratio / 3.0, 1.0) * 0.05

    # RSI health (up to +0.05)
    if direction == "long":
        rsi_health = 1.0 - abs(rsi - 55.0) / 45.0
    else:
        rsi_health = 1.0 - abs(rsi - 45.0) / 45.0
    score += max(0.0, rsi_health) * 0.05

    # Structure clarity (up to +0.05)
    score += structure_clarity * 0.05

    return round(max(0.35, min(score, 0.90)), 4)


def _last_swing_prices(
    work: pl.DataFrame, n: int = 3
) -> tuple[float | None, float | None]:
    """Return (last_swing_high_price, last_swing_low_price) from work frame."""
    sh, sl = _swing_points(work, n=n)
    sh_prices = work.filter(sh)["high"] if sh is not None else None
    sl_prices = work.filter(sl)["low"] if sl is not None else None
    last_high = float(sh_prices[-1]) if sh_prices is not None and sh_prices.len() > 0 else None
    last_low = float(sl_prices[-1]) if sl_prices is not None and sl_prices.len() > 0 else None
    return last_high, last_low


def _reject(prepared: PreparedSymbol, detector: str, reason: str, **values: object) -> None:
    if values:
        details = " ".join(f"{key}={value}" for key, value in values.items())
        LOG.debug("%s: %s rejected | reason=%s %s", prepared.symbol, detector, reason, details)
    else:
        LOG.debug("%s: %s rejected | reason=%s", prepared.symbol, detector, reason)


def _pullback_levels(prepared: PreparedSymbol, direction: str) -> list[tuple[str, float]]:
    levels: list[tuple[str, float]] = []
    if prepared.work_1h.is_empty():
        return levels
    ema20_1h = float(prepared.work_1h.item(-1, "ema20") or 0.0)
    if ema20_1h > 0.0:
        levels.append(("ema20_1h", ema20_1h))
    if prepared.poc_1h and prepared.poc_1h > 0.0:
        levels.append(("poc_1h", float(prepared.poc_1h)))
    last_sh, last_sl = _last_swing_prices(prepared.work_1h)
    if direction == "long" and last_sl and last_sl > 0.0:
        levels.append(("swing_low_1h", float(last_sl)))
    if direction == "short" and last_sh and last_sh > 0.0:
        levels.append(("swing_high_1h", float(last_sh)))
    return levels
