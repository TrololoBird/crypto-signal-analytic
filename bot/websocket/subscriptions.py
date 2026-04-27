"""Subscription planning helpers extracted from ``bot.ws_manager``."""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from websockets import exceptions as ws_exceptions

LOG = logging.getLogger("bot.ws_manager")

def base_streams_for_symbols(manager: Any, symbols: list[str]) -> list[str]:
    streams: list[str] = []
    for symbol in symbols:
        sym = symbol.lower()
        for interval in manager._cfg.kline_intervals:
            streams.append(f"{sym}@kline_{interval}")
    return streams


def public_streams_for_symbols(manager: Any, symbols: list[str]) -> list[str]:
    if not manager._cfg.subscribe_book_ticker:
        return []
    return [f"{symbol.lower()}@bookTicker" for symbol in symbols]


def stream_endpoint_class(stream: str) -> str:
    if "@bookTicker" in stream or "@depth" in stream:
        return "public"
    return "market"


def tracked_agg_trade_streams(manager: Any, symbols: list[str]) -> list[str]:
    if not manager._should_subscribe_agg_trade():
        return []
    return [f"{symbol.lower()}@aggTrade" for symbol in symbols]


def global_streams(manager: Any) -> list[str]:
    if not manager._cfg.subscribe_market_streams:
        return []
    streams = [
        "!ticker@arr",
        "!markPrice@arr",
        "!forceOrder@arr",
    ]
    if not manager.is_ticker_cache_warm():
        streams.append("!miniTicker@arr")
    return streams


def recompute_intended_streams(manager: Any) -> None:
    public_streams = set(public_streams_for_symbols(manager, manager._symbols))
    market_streams = set(base_streams_for_symbols(manager, manager._symbols))
    market_streams.update(tracked_agg_trade_streams(manager, manager._tracked_symbols))
    if manager._symbols:
        market_streams.update(global_streams(manager))
    manager._intended_streams_by_endpoint["public"] = public_streams
    manager._intended_streams_by_endpoint["market"] = market_streams
    manager._intended_streams = set().union(public_streams, market_streams)


async def send_subscription_command(
    manager: Any,
    endpoint: str,
    method: str,
    streams: list[str],
) -> None:
    if not streams:
        return
    ws_conn = manager._ws_conns.get(endpoint)
    if ws_conn is None:
        return
    chunk_size = manager._cfg.subscribe_chunk_size
    delay_seconds = manager._cfg.subscribe_chunk_delay_ms / 1000.0
    for offset in range(0, len(streams), chunk_size):
        if manager._ws_conns.get(endpoint) is None:
            break
        chunk = streams[offset : offset + chunk_size]
        message = json.dumps({"method": method, "params": chunk, "id": manager._subscribe_id})
        manager._subscribe_id += 1
        try:
            await ws_conn.send(message)
            LOG.debug(
                "ws %s chunk | endpoint=%s offset=%d streams=%d",
                method,
                endpoint,
                offset,
                len(chunk),
            )
        except (ws_exceptions.ConnectionClosed, ConnectionError, OSError, AttributeError) as exc:
            LOG.debug("ws %s failed (non-fatal) | endpoint=%s error=%s", method, endpoint, exc)
            break
        if offset + chunk_size < len(streams):
            await asyncio.sleep(delay_seconds)


async def resubscribe_all(manager: Any, endpoint: str, ws: Any) -> None:
    streams = list(manager._intended_streams_by_endpoint.get(endpoint, set()))
    if not streams:
        return
    if len(streams) > 200:
        LOG.warning(
            "ws high stream count warning | endpoint=%s streams=%d symbols=%d - "
            "consider reducing shortlist_limit in config",
            endpoint,
            len(streams),
            len(manager._symbols),
        )
    previous_conn = manager._ws_conns.get(endpoint)
    manager._ws_conns[endpoint] = ws
    if endpoint == "market":
        manager._ws_conn = ws
    try:
        await send_subscription_command(manager, endpoint, "SUBSCRIBE", streams)
    finally:
        restored_conn = ws if manager._running else previous_conn
        manager._ws_conns[endpoint] = restored_conn
        if endpoint == "market":
            manager._ws_conn = restored_conn
    chunk_count = (len(streams) + manager._cfg.subscribe_chunk_size - 1) // manager._cfg.subscribe_chunk_size
    LOG.info(
        "ws resubscribe sent | endpoint=%s streams=%d chunks=%d",
        endpoint,
        len(streams),
        chunk_count,
    )
