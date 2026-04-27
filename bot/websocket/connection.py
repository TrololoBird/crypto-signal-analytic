"""Connection helpers extracted from ``bot.ws_manager``."""

from __future__ import annotations

import asyncio
import socket
import logging
import time
from typing import Any

LOG = logging.getLogger("bot.ws_manager")

def build_stream_url(manager: Any, endpoint: str) -> str:
    base = manager._cfg.endpoint_base_url(endpoint).rstrip("/")
    if base.endswith("/stream") or base.endswith("/ws"):
        return base
    return f"{base}/stream"


def get_ws_fallback_urls(manager: Any, endpoint: str) -> list[str]:
    """Return endpoint-specific websocket URL candidates."""
    return [build_stream_url(manager, endpoint)]


def get_ws_url_version(manager: Any, endpoint: str) -> str:
    base = manager._cfg.endpoint_base_url(endpoint)
    if "/public" in base:
        return "public"
    if "/market" in base:
        return "market"
    return "legacy"


def apply_tcp_keepalive(manager: Any, ws: Any) -> None:
    try:
        transport = getattr(ws, "transport", None)
        sock = transport.get_extra_info("socket") if transport is not None else None
        if sock is None:
            return
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        if hasattr(socket, "TCP_KEEPIDLE"):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 30)
        if hasattr(socket, "TCP_KEEPINTVL"):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 10)
        if hasattr(socket, "TCP_KEEPCNT"):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 5)
        LOG.debug("tcp keepalive applied")
    except (OSError, AttributeError) as exc:
        LOG.debug("tcp keepalive not applied: %s", exc)


def apply_connected_state(manager: Any, *, endpoint: str, ws: Any, url: str) -> None:
    """Apply connection state updates after a successful websocket connect."""
    manager._ws_conns[endpoint] = ws
    manager._connected_urls[endpoint] = url
    manager._connected_at_by_endpoint[endpoint] = time.monotonic()
    if endpoint == "market":
        manager._ws_conn = ws

    apply_tcp_keepalive(manager, ws)

    manager._last_message_ts_by_endpoint[endpoint] = 0.0
    manager._last_message_ts = 0.0
    manager._last_event_lag_ms = None
    manager._connected_endpoints[endpoint].set()
    manager._refresh_connected_event()

    manager._connect_counts[endpoint] += 1
    manager._connect_count += 1
    if manager._connect_counts[endpoint] > 1 and manager._reconnect_cb is not None:
        asyncio.create_task(manager._reconnect_cb())

    LOG.info(
        "ws connected | endpoint=%s url=%s streams=%d connect_count=%d endpoint_connect_count=%d",
        endpoint,
        url,
        len(manager._intended_streams_by_endpoint.get(endpoint, set())),
        manager._connect_count,
        manager._connect_counts[endpoint],
    )
    manager._last_reconnect_reason = f"{endpoint}:connected"
    manager._last_reconnect_reason_by_endpoint[endpoint] = "connected"
