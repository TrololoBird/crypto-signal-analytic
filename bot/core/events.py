"""Typed event dataclasses for the EventBus.

All events are immutable (frozen msgspec.Struct) for safe concurrent dispatch.
"""
from __future__ import annotations

import msgspec


class KlineCloseEvent(msgspec.Struct, frozen=True):
    """Fired by FuturesWSManager when a kline of the configured interval closes."""

    symbol: str
    interval: str  # "15m", "1h", "4h"
    close_ts: int  # epoch ms (from k["T"])
    trigger: str = "kline_close"  # "kline_close" | "emergency_fallback"


class ShortlistUpdatedEvent(msgspec.Struct, frozen=True):
    """Fired after shortlist is rebuilt with new symbol universe."""

    symbols: tuple[str, ...]


class ReconnectEvent(msgspec.Struct, frozen=True):
    """Fired after WebSocket reconnects (not on first connect)."""

    reason: str = "reconnect"


class OIRefreshDueEvent(msgspec.Struct, frozen=True):
    """Fired by the OI refresh timer to trigger REST cache warm-up."""

    symbols: tuple[str, ...]


class BookTickerEvent(msgspec.Struct, frozen=True):
    """Fired by FuturesWSManager on every bookTicker (best bid/ask) update.

    These arrive on every tick — subscribers MUST throttle aggressively.
    """

    symbol: str
    bid: float | None
    ask: float | None


# Union type for dispatcher type routing
AnyEvent = KlineCloseEvent | ShortlistUpdatedEvent | ReconnectEvent | OIRefreshDueEvent | BookTickerEvent
