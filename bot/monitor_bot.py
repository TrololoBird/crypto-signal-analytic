from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from typing import Any

UTC = timezone.utc
LOG = logging.getLogger("bot.monitor_bot")


class HealthMonitor:
    """Runtime-integrated health monitor with repeated-failure alerting."""

    def __init__(
        self,
        *,
        interval_seconds: float,
        check: Callable[[], Awaitable[dict[str, Any]]],
        publish: Callable[[dict[str, Any]], None] | None = None,
        alert: Callable[[Exception, dict[str, Any]], Awaitable[None]] | None = None,
        alert_after_failures: int = 3,
    ) -> None:
        self._interval_seconds = max(5.0, float(interval_seconds))
        self._check = check
        self._publish = publish
        self._alert = alert
        self._alert_after_failures = max(1, int(alert_after_failures))
        self._failure_streak = 0

    async def run(self, *, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            try:
                payload = await self._check()
                payload["health_monitor_ts"] = datetime.now(UTC).isoformat()
                if self._publish is not None:
                    self._publish(payload)
                self._failure_streak = 0
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._failure_streak += 1
                LOG.warning(
                    "health monitor failure | streak=%d err=%s",
                    self._failure_streak,
                    exc,
                )
                if self._failure_streak >= self._alert_after_failures and self._alert is not None:
                    await self._alert(exc, {"component": "health_monitor", "failure_streak": self._failure_streak})
            await asyncio.sleep(self._interval_seconds)
