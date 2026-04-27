from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import Any

LOG = logging.getLogger("bot.application.fallback_runner")


class FallbackRunner:
    """Owns periodic tracking review and emergency fallback scan loops."""

    def __init__(self, bot: Any) -> None:
        self._bot = bot

    async def tracking_review_periodic(self) -> None:
        interval = 300  # 5 minutes
        while not self._bot._shutdown.is_set():
            await asyncio.sleep(interval)
            if self._bot._shutdown.is_set():
                break
            try:
                tracking_events = await self._bot.tracker.review_open_signals(dry_run=False)
                if tracking_events:
                    await self._bot._deliver_tracking(tracking_events)
            except Exception as exc:
                LOG.exception("tracking_review_periodic failed: %s", exc)

    async def emergency_fallback_scan(self) -> None:
        fallback_sec = self._bot.settings.runtime.emergency_fallback_seconds
        while not self._bot._shutdown.is_set():
            await asyncio.sleep(fallback_sec)
            if self._bot._shutdown.is_set():
                break

            time_since_event = asyncio.get_running_loop().time() - self._bot._last_kline_event_ts
            if time_since_event < fallback_sec:
                self._bot.telemetry.append_jsonl(
                    "fallback_checks.jsonl",
                    {
                        "ts": datetime.now(UTC).isoformat(),
                        "trigger": "emergency_fallback",
                        "action": "skip",
                        "fallback_seconds": fallback_sec,
                        "time_since_last_kline_seconds": round(time_since_event, 1),
                    },
                )
                continue

            self._bot.telemetry.append_jsonl(
                "fallback_checks.jsonl",
                {
                    "ts": datetime.now(UTC).isoformat(),
                    "trigger": "emergency_fallback",
                    "action": "run",
                    "fallback_seconds": fallback_sec,
                    "time_since_last_kline_seconds": round(time_since_event, 1),
                },
            )
            LOG.info(
                "emergency fallback: no kline events for %.0fs — running full scan",
                time_since_event,
            )
            try:
                await self._bot._run_emergency_cycle()
            except Exception as exc:
                LOG.warning("emergency fallback failed: %s", exc, exc_info=True)
