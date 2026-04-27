from __future__ import annotations

import asyncio
import logging
import time
from datetime import UTC, datetime
from typing import Any

from ..core.events import BookTickerEvent

LOG = logging.getLogger("bot.application.intra_candle_scanner")


class IntraCandleScanner:
    """Encapsulates throttled intra-candle analysis trigger logic."""

    def __init__(self, bot: Any) -> None:
        self._bot = bot

    async def handle(self, event: BookTickerEvent) -> None:
        symbol = event.symbol
        now = time.monotonic()
        throttle_seconds = float(
            getattr(
                getattr(getattr(self._bot, "settings", None), "ws", None),
                "intra_candle_throttle_seconds",
                0.0,
            )
        )
        if now - self._bot._last_intra_scan.get(symbol, 0.0) < throttle_seconds:
            return

        min_move_bps = float(
            getattr(
                getattr(getattr(self._bot, "settings", None), "ws", None),
                "intra_candle_min_move_bps",
                0.0,
            )
        )
        if (
            min_move_bps > 0.0
            and event.bid is not None
            and event.ask is not None
            and event.bid > 0.0
            and event.ask > 0.0
        ):
            mid_now = (event.bid + event.ask) / 2.0
            mid_prev = self._bot._last_intra_mid.get(symbol)
            if mid_prev is not None and mid_prev > 0.0:
                move_bps = abs(mid_now - mid_prev) / mid_prev * 10000.0
                if move_bps < min_move_bps:
                    return
            self._bot._last_intra_mid[symbol] = mid_now
        self._bot._last_intra_scan[symbol] = now

        async def _run() -> None:
            try:
                async with self._bot._shortlist_lock:
                    shortlist = list(self._bot._shortlist)
                item = next((row for row in shortlist if row.symbol == symbol), None)
                if item is None:
                    return

                event_ts = (
                    datetime.fromtimestamp(event.event_ts_ms / 1000.0, tz=UTC)
                    if event.event_ts_ms is not None and event.event_ts_ms > 0
                    else datetime.now(UTC)
                )
                ws_override: dict[str, Any] | None = None
                if (
                    event.bid is not None
                    and event.ask is not None
                    and event.bid > 0.0
                    and event.ask > 0.0
                    and event.ask >= event.bid
                ):
                    mid = (event.bid + event.ask) / 2.0
                    spread_bps = ((event.ask - event.bid) / mid) * 10000.0 if mid > 0.0 else None
                    ws_override = {
                        "bid_price": event.bid,
                        "ask_price": event.ask,
                        "spread_bps": spread_bps,
                    }
                await self._bot._get_cycle_runner().execute_symbol_cycle(
                    symbol=symbol,
                    item=item,
                    interval="bookTicker",
                    trigger="intra_candle",
                    event_ts=event_ts,
                    shortlist_size=len(shortlist),
                    tracking_events=[],
                    ws_enrichments_override=ws_override,
                )
                LOG.debug("intra_candle scan complete | symbol=%s", symbol)
            except Exception as exc:
                LOG.debug("intra_candle scan failed for %s: %s", symbol, exc)

        task = asyncio.create_task(_run(), name=f"intra_candle:{symbol}")
        self._bot._background_tasks.add(task)
        task.add_done_callback(self._bot._background_tasks.discard)
