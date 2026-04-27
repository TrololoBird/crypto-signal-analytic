"""Health-check helpers extracted from ``bot.ws_manager``."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

LOG = logging.getLogger("bot.ws_manager")


async def evaluate_endpoint_health(manager: Any, ws: Any, endpoint: str) -> bool:
    """Evaluate endpoint-specific health checks.

    Returns True if a reconnect was triggered (ws.close called), else False.
    """
    if endpoint == "market":
        connected_at = manager._connected_at_by_endpoint.get(endpoint, 0.0)
        if connected_at > 0.0:
            recovery_age = time.monotonic() - connected_at
            if recovery_age >= manager._cfg.market_reconnect_grace_seconds:
                snapshot = manager.state_snapshot()
                if (
                    int(snapshot.get("fresh_tickers") or 0) == 0
                    or int(snapshot.get("fresh_mark_prices") or 0) == 0
                ):
                    LOG.warning(
                        "ws market recovery failed | endpoint=%s age=%.1fs fresh_tickers=%s fresh_mark_prices=%s - forcing reconnect",
                        endpoint,
                        recovery_age,
                        snapshot.get("fresh_tickers"),
                        snapshot.get("fresh_mark_prices"),
                    )
                    await ws.close()
                    return True

        stale_streams = manager._stale_kline_streams()
        if stale_streams:
            preview = stale_streams[:3]
            stale_symbols = list({s.split(":")[0] for s in stale_streams})
            LOG.warning(
                "ws stale kline data | endpoint=%s streams=%d sample=%s - backfilling (not reconnecting)",
                endpoint,
                len(stale_streams),
                preview,
            )
            asyncio.create_task(manager._backfill(stale_symbols))
        return False

    if endpoint == "public":
        fresh_books = sum(
            1
            for sym, ts in manager._book_update_times.items()
            if sym in manager._symbols
            and time.monotonic() - ts <= manager._cfg.market_ticker_freshness_seconds
        )
        if manager._symbols and manager._cfg.subscribe_book_ticker and fresh_books == 0:
            connected_at = manager._connected_at_by_endpoint.get(endpoint, 0.0)
            if (
                connected_at > 0.0
                and time.monotonic() - connected_at >= manager._cfg.market_reconnect_grace_seconds
            ):
                LOG.warning(
                    "ws public recovery failed | endpoint=%s fresh_book_tickers=0 - forcing reconnect",
                    endpoint,
                )
                await ws.close()
                return True
    return False
