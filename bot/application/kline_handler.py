from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import Any

from ..core.events import KlineCloseEvent
from ..models import PipelineResult, Signal

LOG = logging.getLogger("bot.application.kline_handler")


class KlineHandler:
    """Handles kline-close orchestration and per-symbol selection/delivery."""

    def __init__(self, bot: Any) -> None:
        self._bot = bot

    async def on_kline_close(self, event: KlineCloseEvent) -> None:
        if event.interval != "15m":
            return

        self._bot._last_kline_event_ts = asyncio.get_running_loop().time()
        symbol = event.symbol
        LOG.info("kline_close received | symbol=%s trigger=%s", symbol, event.trigger)

        async with self._bot._shortlist_lock:
            shortlist = list(self._bot._shortlist)

        tracking_events = await self._bot.tracker.review_open_signals_for_symbol(symbol, dry_run=False)
        if tracking_events:
            await self._bot._deliver_tracking(tracking_events)

        item = next((row for row in shortlist if row.symbol == symbol), None)
        if item is None:
            LOG.debug("kline_close skipped | symbol=%s not in shortlist", symbol)
            return

        await self._bot._get_cycle_runner().execute_symbol_cycle(
            symbol=symbol,
            item=item,
            interval=event.interval,
            trigger=event.trigger,
            event_ts=datetime.now(UTC),
            tracking_events=tracking_events,
            shortlist_size=len(shortlist),
        )

    async def select_and_deliver_for_symbol(
        self,
        symbol: str,
        result: PipelineResult,
    ) -> tuple[list[Signal], list[dict], list[Signal]]:
        candidates = result.candidates
        rejected: list[dict] = list(result.rejected)
        delivered: list[Signal] = []

        if candidates:
            selected = self._bot._select_and_rank(
                {symbol: candidates},
                max_signals=self._bot.settings.runtime.max_signals_per_cycle,
            )
            if result.funnel:
                result.funnel["selected"] = len(selected)
            prepared_by_tracking_id = (
                {item.tracking_id: result.prepared for item in selected}
                if result.prepared is not None
                else None
            )
            delivered, cooldown_rejected, delivery_status_counts = await self._bot._select_and_deliver(
                selected,
                prepared_by_tracking_id=prepared_by_tracking_id,
            )
            if result.funnel:
                result.funnel["delivered"] = len(delivered)
                result.funnel["delivery_status_counts"] = dict(delivery_status_counts)
            rejected.extend(cooldown_rejected)

        return candidates, rejected, delivered
