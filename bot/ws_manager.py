"""WebSocket manager for Binance Futures market data.

Handles WebSocket connections, kline data streams, order book updates,
and aggregated trade data with automatic reconnection and backfill logic.
"""

from __future__ import annotations

import asyncio
import collections
import contextlib
import json
import logging
import random
import socket
import time
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

import polars as pl
import websockets
from websockets import exceptions as ws_exceptions

# Use orjson for faster JSON parsing if available
try:
    import orjson as _json
    _USE_ORJSON = True
except ImportError:
    import json as _json
    _USE_ORJSON = False

from .market_data import MarketDataUnavailable
from .models import AggTrade, AggTradeSnapshot, SymbolFrames

if TYPE_CHECKING:
    from .config import WSConfig
    from .core.event_bus import EventBus
    from .market_data import BinanceFuturesMarketData


UTC = timezone.utc
_BACKOFF_RESET_AFTER_SECONDS = 30.0
_PROACTIVE_RECONNECT_AFTER_SECONDS = 23 * 3600 + 50 * 60
_HEALTH_CHECK_INTERVAL_SECONDS = 30.0

# Binance WebSocket limits (from official docs)
_MAX_STREAMS_PER_CONNECTION = 300  # Conservative limit (docs say 1024, but 10 msg/sec limit)
_MAX_INCOMING_MSG_PER_SECOND = 8   # Below Binance's 10 msg/sec limit with safety margin
_MAX_SUBSCRIBE_MSG_PER_SECOND = 4  # JSON control messages limit is 5 msg/sec

_INTERVAL_SECONDS: dict[str, int] = {
    "1m": 60,
    "3m": 180,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "2h": 7200,
    "4h": 14400,
    "6h": 21600,
    "8h": 28800,
    "12h": 43200,
    "1d": 86400,
}
LOG = logging.getLogger("bot.ws_manager")
_HIGH_EVENT_LATENCY_MS = 5_000.0
_LATENCY_WARNING_INTERVAL_SECONDS = 60.0
_SHORT_DISCONNECT_BACKFILL_GRACE_SECONDS = 30.0
_LATENCY_WARNING_EVENTS = {"kline", "bookTicker", "aggTrade"}
_WS_PUBLIC = "public"
_WS_MARKET = "market"
_WS_ENDPOINTS = (_WS_PUBLIC, _WS_MARKET)


class RateLimiter:
    """Rate limiter for incoming WebSocket messages (Binance limit: 10 msg/sec)."""
    
    def __init__(self, max_per_second: int = _MAX_INCOMING_MSG_PER_SECOND) -> None:
        self.max_per_second = max_per_second
        self._timestamps: collections.deque = collections.deque(maxlen=max_per_second * 2)
        self._lock = asyncio.Lock()
    
    async def acquire(self) -> bool:
        """Try to acquire permission to process a message. Returns True if allowed."""
        async with self._lock:
            now = time.monotonic()
            # Remove timestamps older than 1 second
            while self._timestamps and now - self._timestamps[0] > 1.0:
                self._timestamps.popleft()
            
            if len(self._timestamps) < self.max_per_second:
                self._timestamps.append(now)
                return True
            return False
    
    async def wait_for_slot(self) -> None:
        """Wait until a slot is available."""
        while not await self.acquire():
            await asyncio.sleep(0.05)  # 50ms wait


class MessageBuffer:
    """Buffer for WebSocket messages with backpressure handling."""
    
    def __init__(self, maxsize: int = 10000) -> None:
        self._buffer: asyncio.Queue[Any] = asyncio.Queue(maxsize=maxsize)
        self._dropped_count = 0
        self._processed_count = 0
    
    async def put(self, msg: Any) -> bool:
        """Add message to buffer. Returns False if buffer is full (backpressure)."""
        try:
            self._buffer.put_nowait(msg)
            return True
        except asyncio.QueueFull:
            self._dropped_count += 1
            if self._dropped_count % 1000 == 1:
                LOG.warning("message buffer full | dropped=%d processed=%d", self._dropped_count, self._processed_count)
            return False
    
    async def get(self) -> Any | None:
        """Get message from buffer. Returns None if empty."""
        try:
            msg = self._buffer.get_nowait()
            self._processed_count += 1
            return msg
        except asyncio.QueueEmpty:
            return None
    
    def get_stats(self) -> dict[str, int]:
        """Return buffer statistics."""
        return {
            "size": self._buffer.qsize(),
            "maxsize": self._buffer.maxsize,
            "dropped": self._dropped_count,
            "processed": self._processed_count,
        }


def _ws_kline_to_row(k: dict) -> dict:
    return {
        "time": datetime.fromtimestamp(int(k["t"]) / 1000.0, tz=UTC),
        "open": float(k["o"]),
        "high": float(k["h"]),
        "low": float(k["l"]),
        "close": float(k["c"]),
        "volume": float(k["v"]),
        "close_time": datetime.fromtimestamp(int(k["T"]) / 1000.0, tz=UTC),
        "quote_volume": float(k["q"]),
        "num_trades": int(k["n"]),
        "taker_buy_base_volume": float(k["V"]),
        "taker_buy_quote_volume": float(k["Q"]),
    }


class FuturesWSManager:
    """Manages WebSocket connections to Binance Futures for real-time market data."""
    def __init__(
        self,
        rest_client: BinanceFuturesMarketData,
        config: WSConfig,
    ) -> None:
        self._rest = rest_client
        self._cfg = config
        self._symbols: list[str] = []
        self._tracked_symbols: list[str] = []
        self._lock = asyncio.Lock()
        self._data_lock = asyncio.Lock()
        self._backfill_sem = asyncio.Semaphore(5)

        self._klines: dict[str, dict[str, collections.deque]] = {}
        self._book: dict[str, tuple[float | None, float | None]] = {}
        self._agg_trades: dict[str, collections.deque] = {}

        # Global market stream caches (populated from !ticker@arr, !markPrice@arr, !forceOrder@arr)
        self._ticker_cache: dict[str, dict] = {}
        self._ticker_cache_ts: float = 0.0  # monotonic time of last ticker update
        self._mark_price_cache: dict[str, dict] = {}
        # Force order buffer: (timestamp_ms, symbol, side, qty, price)
        self._force_order_buffer: collections.deque = collections.deque(maxlen=500)

        self._stream_task: asyncio.Task | None = None
        self._stream_tasks: dict[str, asyncio.Task | None] = {endpoint: None for endpoint in _WS_ENDPOINTS}
        self._running = False
        self._connected = asyncio.Event()
        self._connected_endpoints: dict[str, asyncio.Event] = {
            endpoint: asyncio.Event() for endpoint in _WS_ENDPOINTS
        }
        self._ws_conn: Any | None = None
        self._ws_conns: dict[str, Any | None] = {endpoint: None for endpoint in _WS_ENDPOINTS}
        self._subscribe_id = 1
        self._intended_streams: set[str] = set()
        self._intended_streams_by_endpoint: dict[str, set[str]] = {
            endpoint: set() for endpoint in _WS_ENDPOINTS
        }
        self._last_message_ts = 0.0
        self._last_message_ts_by_endpoint: dict[str, float] = {
            endpoint: 0.0 for endpoint in _WS_ENDPOINTS
        }
        self._last_event_lag_ms: float | None = None
        self._short_lived_streak = 0
        self._last_reconnect_reason = "not_started"
        self._last_reconnect_reason_by_endpoint: dict[str, str] = {
            endpoint: "not_started" for endpoint in _WS_ENDPOINTS
        }
        self._connected_urls: dict[str, str | None] = {endpoint: None for endpoint in _WS_ENDPOINTS}
        self._connected_at_by_endpoint: dict[str, float] = {
            endpoint: 0.0 for endpoint in _WS_ENDPOINTS
        }
        self._subscription_errors: dict[str, Any | None] = {endpoint: None for endpoint in _WS_ENDPOINTS}
        self._subscription_ack_count: dict[str, int] = {endpoint: 0 for endpoint in _WS_ENDPOINTS}
        self._backfill_cooldowns: dict[str, float] = {}
        self._last_latency_warning_by_symbol: dict[str, float] = {}
        self._last_stale_warning_by_stream: dict[str, float] = {}
        self._last_short_disconnect_s: float | None = None
        # Debounce and throttling state for global streams
        self._ticker_update_times: dict[str, float] = {}  # symbol -> last update monotonic
        self._mark_price_update_times: dict[str, float] = {}  # symbol -> last update monotonic
        self._book_update_times: dict[str, float] = {}
        self._min_ticker_update_interval_ms: float = 100.0  # throttle duplicate updates
        self._shortlist_rebuild_lock = asyncio.Lock()
        self._last_shortlist_rebuild_ts: float = 0.0
        self._shortlist_rebuild_interval_seconds: float = 75.0  # 60-90s as requested
        self._last_shortlist: list[Any] = []
        self._last_shortlist_summary: dict[str, Any] = {}

        # Event-driven callbacks (fire-and-forget via asyncio.create_task)
        self._kline_close_cbs: dict[str, list] = {}  # interval -> [async cb(symbol, interval, close_ts_ms)]
        self._agg_trade_cbs: list = []               # [async cb(symbol, price, ts)]
        self._reconnect_cb: Any = None               # async cb() — fired on reconnect

        # P0: Rate limiting and message buffering (Binance limit: 10 msg/sec)
        self._incoming_rate_limiter = RateLimiter(max_per_second=_MAX_INCOMING_MSG_PER_SECOND)
        self._message_buffer = MessageBuffer(maxsize=2000)
        self._buffer_processor_task: asyncio.Task | None = None
        self._backfill_tasks: set[asyncio.Task[None]] = set()
        
        # P3: Per-stream latency monitoring
        self._stream_latency_ms: dict[str, collections.deque] = {}  # stream -> deque of last 10 latencies
        self._slow_streams: set[str] = set()  # streams with avg latency > 5000ms
        self._stream_last_message_ts: dict[str, float] = {}  # last message timestamp per stream
        self._connect_count: int = 0                 # incremented each successful connection
        self._connect_counts: dict[str, int] = {endpoint: 0 for endpoint in _WS_ENDPOINTS}

        # EventBus integration (optional — set via set_event_bus())
        self._event_bus: EventBus | None = None

    @staticmethod
    def _normalize_symbol_list(items: list[Any]) -> list[str]:
        normalized: list[str] = []
        for item in items or []:
            sym: Any = item
            if not isinstance(sym, str) and hasattr(sym, "symbol"):
                try:
                    sym = getattr(sym, "symbol")
                except Exception:
                    sym = item
            sym = str(sym).strip().upper()
            if not sym:
                continue
            normalized.append(sym)
        # preserve order while dropping duplicates
        return list(dict.fromkeys(normalized))

    def _should_subscribe_agg_trade(self) -> bool:
        return bool(self._cfg.subscribe_agg_trade)

    # ------------------------------------------------------------------
    # Event-driven callback registration
    # ------------------------------------------------------------------

    def register_kline_close(self, interval: str, cb: Any) -> None:
        """Register an async callback fired when a kline of *interval* closes.

        Callback signature: ``async def cb(symbol: str, interval: str, close_ts_ms: int) -> None``
        """
        self._kline_close_cbs.setdefault(interval, []).append(cb)

    def register_agg_trade(self, cb: Any) -> None:
        """Register an async callback fired on every aggTrade tick.

        Callback signature: ``async def cb(symbol: str, price: float, ts: datetime) -> None``
        """
        self._agg_trade_cbs.append(cb)

    def register_reconnect(self, cb: Any) -> None:
        """Register an async callback fired when the WS reconnects (not on first connect).

        Callback signature: ``async def cb() -> None``
        """
        self._reconnect_cb = cb

    def set_event_bus(self, bus: "EventBus") -> None:
        """Attach an EventBus.  When set, kline_close events are published to it
        in addition to (or instead of) the legacy callback system.

        Called by SignalBot.__init__ before ws_manager.start().
        """
        self._event_bus = bus
        LOG.info("EventBus attached to ws_manager")

    @staticmethod
    def _attach_task_logging(task: asyncio.Task, *, label: str) -> None:
        def _done(done: asyncio.Task) -> None:
            with contextlib.suppress(asyncio.CancelledError):
                exc = done.exception()
                if exc is not None:
                    LOG.exception("%s callback failed", label, exc_info=exc)

        task.add_done_callback(_done)

    async def start(self, symbols: list[str]) -> None:
        """Start the WebSocket manager with the given symbols.

        Args:
            symbols: List of trading symbols to subscribe to.
        """
        if self._running:
            LOG.debug("ws_manager already running, ignoring start() call")
            return
        async with self._lock:
            self._symbols = self._normalize_symbol_list(list(symbols))
            self._recompute_intended_streams()

        # Start WS first; do not block startup on full REST backfill.
        # Backfill is scheduled in the background to keep the bot responsive.
        self._running = True
        for endpoint in self._active_endpoint_classes():
            self._stream_tasks[endpoint] = asyncio.create_task(
                self._run_stream(endpoint), name=f"ws_manager_stream:{endpoint}"
            )
        self._stream_task = self._stream_tasks.get(_WS_MARKET) or self._stream_tasks.get(_WS_PUBLIC)
        # P1: Start message buffer processor
        self._buffer_processor_task = asyncio.create_task(
            self._process_buffered_messages(), name="ws_buffer_processor"
        )
        LOG.info(
            "ws_manager started | symbols=%d endpoints=%s",
            len(self._symbols),
            list(self._active_endpoint_classes()),
        )

        if self._symbols:
            task = asyncio.create_task(self._backfill(self._symbols), name="ws_backfill_initial")
            self._backfill_tasks.add(task)
            task.add_done_callback(self._backfill_tasks.discard)

    async def stop(self) -> None:
        """Stop the WebSocket manager and close all connections."""
        if not self._running:
            return  # Already stopped or never started
        self._running = False
        self._connected.clear()
        self._ws_conn = None
        for endpoint in _WS_ENDPOINTS:
            self._connected_endpoints[endpoint].clear()
            self._ws_conns[endpoint] = None
            self._connected_urls[endpoint] = None
            self._connected_at_by_endpoint[endpoint] = 0.0
        tasks_to_cancel = [
            task for task in self._stream_tasks.values()
            if task is not None and not task.done()
        ]
        for task in tasks_to_cancel:
            task.cancel()
        if tasks_to_cancel:
            await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
        self._stream_tasks = {endpoint: None for endpoint in _WS_ENDPOINTS}
        self._stream_task = None
        
        # P1: Stop buffer processor
        if self._buffer_processor_task and not self._buffer_processor_task.done():
            self._buffer_processor_task.cancel()
            try:
                await self._buffer_processor_task
            except asyncio.CancelledError:
                pass
        self._buffer_processor_task = None

        # Cancel any in-flight REST backfills (non-fatal, best-effort).
        if self._backfill_tasks:
            for task in list(self._backfill_tasks):
                task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.gather(*self._backfill_tasks, return_exceptions=True)
            self._backfill_tasks.clear()
        
        LOG.info("ws_manager stopped")

    async def wait_until_connected(self, timeout: float | None = None) -> bool:
        """Wait until the WebSocket connection is established.

        Args:
            timeout: Maximum time to wait in seconds. None means wait forever.

        Returns:
            True if connected, False if timeout occurred.
        """
        try:
            await asyncio.wait_for(self._connected.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            return False
        return True

    @property
    def is_running(self) -> bool:
        """Check if the WebSocket manager is currently running."""
        return self._running

    def _active_endpoint_classes(self) -> tuple[str, ...]:
        return tuple(
            endpoint
            for endpoint in _WS_ENDPOINTS
            if self._intended_streams_by_endpoint.get(endpoint)
        )

    def _refresh_connected_event(self) -> None:
        active = self._active_endpoint_classes()
        if active and all(self._connected_endpoints[endpoint].is_set() for endpoint in active):
            self._connected.set()
            return
        if not active:
            self._connected.clear()
            return
        self._connected.clear()

    def state_snapshot(self) -> dict[str, Any]:
        """Return a snapshot of the current WebSocket state.

        Returns:
            Dictionary with stream counts, cache health metrics, and reconnect info.
            Includes detailed cache metrics: ticker freshness, liquidation buffer size,
            and per-symbol freshness statistics.
        """
        warm = sum(1 for s in self._symbols if self.is_warm(s))
        total = len(self._symbols)
        now = time.monotonic()
        ticker_age = (
            round(now - self._ticker_cache_ts, 1)
            if self._ticker_cache_ts > 0
            else None
        )
        fresh_tickers = sum(
            1 for sym, ts in self._ticker_update_times.items()
            if now - ts <= self._cfg.market_ticker_freshness_seconds
        )
        fresh_mark_prices = sum(
            1 for sym, ts in self._mark_price_update_times.items()
            if now - ts <= self._cfg.market_ticker_freshness_seconds
        )
        fresh_book_tickers = sum(
            1 for sym, ts in self._book_update_times.items()
            if sym in self._symbols and now - ts <= self._cfg.market_ticker_freshness_seconds
        )
        fresh_klines_15m = sum(
            1 for sym in self._symbols
            if self._is_interval_fresh(sym, "15m")
        )
        connected_urls = {
            endpoint: url
            for endpoint, url in self._connected_urls.items()
            if url is not None
        }
        connected_endpoints = [
            endpoint for endpoint in _WS_ENDPOINTS
            if self._connected_endpoints[endpoint].is_set()
        ]
        return {
            "active_stream_count": len(self._intended_streams),
            "intended_stream_count": len(self._intended_streams),
            "warm_symbols": warm,
            "total_symbols": total,
            "reconnect_count": self._connect_count,
            "last_event_lag_ms": self._last_event_lag_ms,
            "avg_latency_ms": self._get_current_latency_ms(),
            "last_message_age_seconds": self._last_message_age_seconds(),
            "public_last_message_age_seconds": self._last_message_age_seconds(_WS_PUBLIC),
            "market_last_message_age_seconds": self._last_message_age_seconds(_WS_MARKET),
            "ticker_cache_age_seconds": ticker_age,
            "buffer_message_count": self._message_buffer.get_stats()["size"],
            "fresh_tickers": fresh_tickers,
            "fresh_mark_prices": fresh_mark_prices,
            "fresh_book_tickers": fresh_book_tickers,
            "fresh_klines_15m": fresh_klines_15m,
            "connect_count": self._connect_count,
            "public_connect_count": self._connect_counts[_WS_PUBLIC],
            "market_connect_count": self._connect_counts[_WS_MARKET],
            "mark_price_fresh_symbols": fresh_mark_prices,
            "liq_buffer_size": len(self._force_order_buffer),
            "stale_kline_stream_count": len(self._stale_kline_streams()),
            "connected_ws_url": connected_urls or None,
            "ws_endpoint_class": connected_endpoints or None,
            "public_subscription_ack_count": self._subscription_ack_count[_WS_PUBLIC],
            "market_subscription_ack_count": self._subscription_ack_count[_WS_MARKET],
            "public_subscription_error": self._subscription_errors[_WS_PUBLIC],
            "market_subscription_error": self._subscription_errors[_WS_MARKET],
            # Shortlist rebuild state
            "last_shortlist_rebuild_age_s": round(now - self._last_shortlist_rebuild_ts, 1) if self._last_shortlist_rebuild_ts > 0 else None,
        }

    def _get_current_latency_ms(self) -> float | None:
        """Calculate current WebSocket latency in milliseconds."""
        if not self._stream_latency_ms:
            return self._last_event_lag_ms
        all_latencies = []
        for latencies in self._stream_latency_ms.values():
            if latencies:
                all_latencies.append(sum(latencies) / len(latencies))
        if all_latencies:
            return round(sum(all_latencies) / len(all_latencies), 2)
        return self._last_event_lag_ms

    def _last_message_age_seconds(self, endpoint: str | None = None) -> float | None:
        """Get age of last received message in seconds."""
        now = time.monotonic()
        if endpoint is not None:
            last_message_ts = self._last_message_ts_by_endpoint.get(endpoint, 0.0)
            if last_message_ts == 0.0:
                return None
            return round(now - last_message_ts, 1)
        ages = [
            now - ts for ts in self._last_message_ts_by_endpoint.values()
            if ts > 0.0
        ]
        if ages:
            return round(min(ages), 1)
        if self._last_message_ts == 0.0:
            return None
        return round(now - self._last_message_ts, 1)

    async def subscribe(self, symbols: list[str]) -> None:
        """Subscribe to market data for the given symbols.

        Args:
            symbols: List of trading symbols to subscribe to.
        """
        async with self._lock:
            current = set(self._symbols)
            requested_symbols = self._normalize_symbol_list(list(symbols))
            requested_set = set(requested_symbols)
            new_symbols = [s for s in requested_symbols if s not in current]
            removed_symbols = [s for s in self._symbols if s not in requested_set]
            self._symbols = requested_symbols
            previous_by_endpoint = {
                endpoint: set(streams)
                for endpoint, streams in self._intended_streams_by_endpoint.items()
            }
            self._recompute_intended_streams()
            current_by_endpoint = {
                endpoint: set(streams)
                for endpoint, streams in self._intended_streams_by_endpoint.items()
            }
        if removed_symbols:
            async with self._data_lock:
                for symbol in removed_symbols:
                    self._klines.pop(symbol, None)
                    self._book.pop(symbol, None)
                    self._agg_trades.pop(symbol, None)
                    self._book_update_times.pop(symbol, None)

        if not new_symbols and not removed_symbols:
            return

        if self._running:
            for endpoint in _WS_ENDPOINTS:
                removed_streams = list(previous_by_endpoint[endpoint] - current_by_endpoint[endpoint])
                added_streams = list(current_by_endpoint[endpoint] - previous_by_endpoint[endpoint])
                if removed_streams:
                    await self._send_subscription_command(endpoint, "UNSUBSCRIBE", removed_streams)
                if added_streams:
                    stream_task = self._stream_tasks[endpoint]
                    if stream_task is None or stream_task.done():
                        self._stream_tasks[endpoint] = asyncio.create_task(
                            self._run_stream(endpoint),
                            name=f"ws_manager_stream:{endpoint}",
                        )
                        if endpoint == _WS_MARKET:
                            self._stream_task = self._stream_tasks[endpoint]
                    else:
                        await self._send_subscription_command(endpoint, "SUBSCRIBE", added_streams)
            LOG.info(
                "ws_manager subscription update | added=%d removed=%d total=%d endpoints=%s",
                len(new_symbols),
                len(removed_symbols),
                len(requested_symbols),
                list(self._active_endpoint_classes()),
            )
            if new_symbols:
                task = asyncio.create_task(
                    self._backfill(new_symbols),
                    name=f"ws_backfill_subscribe:{len(new_symbols)}",
                )
                self._backfill_tasks.add(task)
                task.add_done_callback(self._backfill_tasks.discard)
            return

        if new_symbols:
            task = asyncio.create_task(
                self._backfill(new_symbols),
                name=f"ws_backfill_subscribe:{len(new_symbols)}",
            )
            self._backfill_tasks.add(task)
            task.add_done_callback(self._backfill_tasks.discard)

    def _base_streams_for_symbols(self, symbols: list[str]) -> list[str]:
        streams: list[str] = []
        for symbol in symbols:
            sym = symbol.lower()
            for interval in self._cfg.kline_intervals:
                streams.append(f"{sym}@kline_{interval}")
        return streams

    def _public_streams_for_symbols(self, symbols: list[str]) -> list[str]:
        if not self._cfg.subscribe_book_ticker:
            return []
        return [f"{symbol.lower()}@bookTicker" for symbol in symbols]

    def _stream_endpoint_class(self, stream: str) -> str:
        if "@bookTicker" in stream or "@depth" in stream:
            return _WS_PUBLIC
        return _WS_MARKET

    def _recompute_intended_streams(self) -> None:
        public_streams = set(self._public_streams_for_symbols(self._symbols))
        market_streams = set(self._base_streams_for_symbols(self._symbols))
        market_streams.update(self._tracked_agg_trade_streams(self._tracked_symbols))
        if self._symbols:
            market_streams.update(self._global_streams())
        self._intended_streams_by_endpoint[_WS_PUBLIC] = public_streams
        self._intended_streams_by_endpoint[_WS_MARKET] = market_streams
        self._intended_streams = set().union(public_streams, market_streams)

    def _tracked_agg_trade_streams(self, symbols: list[str]) -> list[str]:
        if not self._should_subscribe_agg_trade():
            return []
        return [f"{symbol.lower()}@aggTrade" for symbol in symbols]

    async def set_tracked_symbols(self, symbols: list[str]) -> None:
        tracked_symbols = self._normalize_symbol_list(list(symbols))
        async with self._lock:
            current = set(self._tracked_symbols)
            requested = set(tracked_symbols)
            added = [s for s in tracked_symbols if s not in current]
            removed = [s for s in self._tracked_symbols if s not in requested]
            self._tracked_symbols = tracked_symbols
            previous_market_streams = set(self._intended_streams_by_endpoint[_WS_MARKET])
            self._recompute_intended_streams()
            current_market_streams = set(self._intended_streams_by_endpoint[_WS_MARKET])

        if not self._cfg.subscribe_agg_trade:
            return
        market_task = self._stream_tasks[_WS_MARKET]
        if self._running and (market_task is None or market_task.done()) and current_market_streams:
            self._stream_tasks[_WS_MARKET] = asyncio.create_task(
                self._run_stream(_WS_MARKET),
                name="ws_manager_stream:market",
            )
            self._stream_task = self._stream_tasks[_WS_MARKET]
        elif self._ws_conns[_WS_MARKET] is not None and self._running:
            await self._send_subscription_command(
                _WS_MARKET,
                "UNSUBSCRIBE",
                list(previous_market_streams - current_market_streams),
            )
            await self._send_subscription_command(
                _WS_MARKET,
                "SUBSCRIBE",
                list(current_market_streams - previous_market_streams),
            )
        if removed:
            async with self._data_lock:
                for symbol in removed:
                    self._agg_trades.pop(symbol, None)

        if tracked_symbols:
            LOG.info("ws tracked symbols updated | tracked=%d agg_trade=%s", len(tracked_symbols), self._cfg.subscribe_agg_trade)
        else:
            LOG.info("ws tracked symbols updated | tracked=0 aggTrade unsubscribed")

    async def _send_subscription_command(self, endpoint: str, method: str, streams: list[str]) -> None:
        if not streams:
            return
        ws_conn = self._ws_conns.get(endpoint)
        if ws_conn is None:
            return
        chunk_size = self._cfg.subscribe_chunk_size
        delay_seconds = self._cfg.subscribe_chunk_delay_ms / 1000.0
        for offset in range(0, len(streams), chunk_size):
            if self._ws_conns.get(endpoint) is None:
                break
            chunk = streams[offset : offset + chunk_size]
            message = json.dumps(
                {"method": method, "params": chunk, "id": self._subscribe_id}
            )
            self._subscribe_id += 1
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

    def _global_streams(self) -> list[str]:
        """Return the list of market-wide streams to subscribe if enabled."""
        if not self._cfg.subscribe_market_streams:
            return []
        streams = [
            "!ticker@arr",        # 24hr rolling tickers for all symbols (~1s updates)
            "!markPrice@arr",     # Mark price + funding rate for all symbols (3s)
            "!forceOrder@arr",    # All liquidation orders in real-time
        ]
        # Add miniTicker as lightweight fallback when ticker cache is cold
        if not self.is_ticker_cache_warm():
            streams.append("!miniTicker@arr")
        return streams

    async def _resubscribe_all(self, endpoint: str, ws: Any) -> None:
        streams = list(self._intended_streams_by_endpoint.get(endpoint, set()))
        if not streams:
            return

        # Pre-flight warning for high stream count
        if len(streams) > 200:
            LOG.warning(
                "ws high stream count warning | endpoint=%s streams=%d symbols=%d - "
                "consider reducing shortlist_limit in config",
                endpoint,
                len(streams),
                len(self._symbols),
            )
        previous_conn = self._ws_conns.get(endpoint)
        self._ws_conns[endpoint] = ws
        if endpoint == _WS_MARKET:
            self._ws_conn = ws
        try:
            await self._send_subscription_command(endpoint, "SUBSCRIBE", streams)
        finally:
            restored_conn = ws if self._running else previous_conn
            self._ws_conns[endpoint] = restored_conn
            if endpoint == _WS_MARKET:
                self._ws_conn = restored_conn
        chunk_count = (
            (len(streams) + self._cfg.subscribe_chunk_size - 1)
            // self._cfg.subscribe_chunk_size
        )
        LOG.info(
            "ws resubscribe sent | endpoint=%s streams=%d chunks=%d",
            endpoint,
            len(streams),
            chunk_count,
        )

    def _apply_tcp_keepalive(self, ws: Any) -> None:
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

    async def _health_monitor(self, ws: Any, endpoint: str) -> None:
        """Monitor WebSocket health and reconnect on message silence."""
        silence_limit = self._cfg.health_check_silence_seconds
        while True:
            await asyncio.sleep(_HEALTH_CHECK_INTERVAL_SECONDS)
            last_message_ts = self._last_message_ts_by_endpoint.get(endpoint, 0.0)
            if last_message_ts != 0.0:
                silence = time.monotonic() - last_message_ts
                if silence > silence_limit and self._intended_streams_by_endpoint.get(endpoint):
                    LOG.info(
                        "ws health: no message for %.0fs with %d streams - forcing reconnect | endpoint=%s",
                        silence,
                        len(self._intended_streams_by_endpoint.get(endpoint, set())),
                        endpoint,
                    )
                    await ws.close()
                    return
            if endpoint == _WS_MARKET:
                connected_at = self._connected_at_by_endpoint.get(endpoint, 0.0)
                if connected_at > 0.0:
                    recovery_age = time.monotonic() - connected_at
                    if recovery_age >= self._cfg.market_reconnect_grace_seconds:
                        snapshot = self.state_snapshot()
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
                            return
                stale_streams = self._stale_kline_streams()
                if stale_streams:
                    preview = stale_streams[:3]
                    stale_symbols = list({s.split(":")[0] for s in stale_streams})
                    LOG.warning(
                        "ws stale kline data | endpoint=%s streams=%d sample=%s - backfilling (not reconnecting)",
                        endpoint,
                        len(stale_streams),
                        preview,
                    )
                    # Backfill missing candles via REST instead of forcing a full
                    # WS reconnect.  Reconnecting triggers run_cycle() which blocks
                    # the event loop and causes even more lag / stale detections.
                    asyncio.create_task(self._backfill(stale_symbols))
            elif endpoint == _WS_PUBLIC:
                fresh_books = sum(
                    1
                    for sym, ts in self._book_update_times.items()
                    if sym in self._symbols and time.monotonic() - ts <= self._cfg.market_ticker_freshness_seconds
                )
                if self._symbols and self._cfg.subscribe_book_ticker and fresh_books == 0:
                    connected_at = self._connected_at_by_endpoint.get(endpoint, 0.0)
                    if connected_at > 0.0 and time.monotonic() - connected_at >= self._cfg.market_reconnect_grace_seconds:
                        LOG.warning(
                            "ws public recovery failed | endpoint=%s fresh_book_tickers=0 - forcing reconnect",
                            endpoint,
                        )
                        await ws.close()
                        return

    def _kline_close_age_seconds(self, symbol: str, interval: str) -> float | None:
        deq = self._klines.get(symbol, {}).get(interval)
        if not deq:
            return None
        close_time = deq[-1].get("close_time")
        if close_time is None:
            return None
        try:
            if isinstance(close_time, str):
                close_ts = datetime.fromisoformat(close_time.replace('Z', '+00:00'))
            else:
                close_ts = close_time
            return max(0.0, (datetime.now(UTC) - close_ts).total_seconds())
        except (ValueError, TypeError):
            return None

    def _stale_kline_streams(self) -> list[str]:
        stale: list[str] = []
        for symbol in self._symbols:
            for interval in self._cfg.kline_intervals:
                close_age = self._kline_close_age_seconds(symbol, interval)
                if close_age is None:
                    continue
                max_age = _INTERVAL_SECONDS.get(interval, 900) * 6
                if close_age > max_age:
                    stream_key = f"{symbol}:{interval}"
                    now = time.monotonic()
                    last_logged = self._last_stale_warning_by_stream.get(stream_key, 0.0)
                    if now - last_logged >= _HEALTH_CHECK_INTERVAL_SECONDS:
                        self._last_stale_warning_by_stream[stream_key] = now
                    stale.append(stream_key)
        return stale

    def _is_interval_fresh(self, symbol: str, interval: str) -> bool:
        close_age = self._kline_close_age_seconds(symbol, interval)
        if close_age is None:
            return False
        max_age = _INTERVAL_SECONDS.get(interval, 900) * 6
        return close_age <= max_age

    def _stale_symbols(self) -> list[str]:
        """Return list of symbols with stale (non-fresh) kline data."""
        return [
            symbol
            for symbol in self._symbols
            if any(
                not self._is_interval_fresh(symbol, interval)
                for interval in self._cfg.kline_intervals
            )
        ]

    def _should_backfill_after_disconnect(self, *, elapsed: float, stale_symbols: list[str]) -> bool:
        if not stale_symbols:
            return False
        self._last_short_disconnect_s = round(elapsed, 1) if elapsed < _SHORT_DISCONNECT_BACKFILL_GRACE_SECONDS else None
        if elapsed >= _SHORT_DISCONNECT_BACKFILL_GRACE_SECONDS:
            return True
        missing_cache = any(symbol not in self._klines for symbol in stale_symbols)
        return missing_cache

    async def _maybe_backfill_after_disconnect(self, *, elapsed: float, stale_symbols: list[str]) -> None:
        if not stale_symbols:
            return
        if not self._should_backfill_after_disconnect(elapsed=elapsed, stale_symbols=stale_symbols):
            LOG.info(
                "reconnect backfill skipped | short_disconnect=%.1fs stale_symbols=%d",
                elapsed,
                len(stale_symbols),
            )
            return
        LOG.info("reconnect backfill | stale_symbols=%d", len(stale_symbols))
        try:
            await self._backfill(stale_symbols)
        except (ConnectionError, TimeoutError, ValueError, RuntimeError) as backfill_exc:
            LOG.debug("reconnect backfill failed (non-fatal): %s", backfill_exc)

    def is_warm(self, symbol: str) -> bool:
        """Check if all required data is available and fresh for a symbol.

        Args:
            symbol: Trading symbol to check.

        Returns:
            True if symbol has all required fresh data.
        """
        klines = self._klines.get(symbol, {})
        for interval in self._cfg.kline_intervals:
            deq = klines.get(interval)
            if not deq:
                return False
            if not self._is_interval_fresh(symbol, interval):
                return False
        if self._cfg.subscribe_book_ticker and symbol not in self._book:
            return False
        return True

    async def get_symbol_frames(self, symbol: str) -> SymbolFrames | None:
        """Get structured market data frames for a symbol.

        Acquires _data_lock to prevent races with kline/book writers.

        Args:
            symbol: Trading symbol to retrieve data for.

        Returns:
            SymbolFrames object with kline data and book prices, or None if not warm.
        """
        async with self._data_lock:
            if not self.is_warm(symbol):
                return None
            klines = self._klines.get(symbol, {})
            interval_dfs: dict[str, pl.DataFrame] = {}
            for interval in self._cfg.kline_intervals:
                deq = klines.get(interval)
                if not deq:
                    return None
                interval_dfs[interval] = pl.DataFrame(list(deq))

            bid, ask = self._book.get(symbol, (None, None))
        return SymbolFrames(
            symbol=symbol,
            df_1h=interval_dfs.get("1h", pl.DataFrame()),
            df_15m=interval_dfs.get("15m", pl.DataFrame()),
            bid_price=bid,
            ask_price=ask,
            df_5m=interval_dfs.get("5m", pl.DataFrame()),
            df_4h=interval_dfs.get("4h", pl.DataFrame()),
        )

    async def get_book_ticker(self, symbol: str) -> tuple[float | None, float | None] | None:
        """Get current best bid/ask prices for a symbol.

        Acquires _data_lock to prevent races with book ticker writers.

        Args:
            symbol: Trading symbol to retrieve prices for.

        Returns:
            Tuple of (bid, ask) prices or None if unavailable.
        """
        async with self._data_lock:
            return self._book.get(symbol)

    def get_agg_trade_snapshot(self, symbol: str) -> AggTradeSnapshot | None:
        """Get aggregated trade statistics for a symbol over the configured window.

        Args:
            symbol: Trading symbol to retrieve trade snapshot for.

        Returns:
            AggTradeSnapshot with buy/sell statistics or None if no recent trades.
        """
        buf = self._agg_trades.get(symbol)
        if not buf:
            return None
        cutoff_ms = int(time.time() * 1000) - self._cfg.agg_trade_window_seconds * 1000
        buy_qty = sell_qty = 0.0
        count = 0
        for trade in buf:
            if trade.trade_time_ms < cutoff_ms:
                continue
            count += 1
            if trade.is_buyer_maker:
                sell_qty += trade.quantity
            else:
                buy_qty += trade.quantity
        if count == 0:
            return None
        total = buy_qty + sell_qty
        delta_ratio = (buy_qty - sell_qty) / total if total > 0 else None
        return AggTradeSnapshot(
            symbol=symbol,
            trade_count=count,
            buy_qty=buy_qty,
            sell_qty=sell_qty,
            delta_ratio=delta_ratio,
        )

    # ------------------------------------------------------------------ #
    # Global market stream accessors                                       #
    # ------------------------------------------------------------------ #

    def is_ticker_cache_warm(self) -> bool:
        """Return True if the !ticker@arr cache has been populated recently.

        Uses ``ws.market_ticker_freshness_seconds`` as the staleness limit.
        Falls back to True only if the cache has at least a handful of symbols
        so the shortlist can be meaningful.
        """
        if not self._ticker_cache:
            return False
        age = time.monotonic() - self._ticker_cache_ts
        return age <= self._cfg.market_ticker_freshness_seconds

    def get_stats(self) -> dict[str, Any]:
        """P3: Return WebSocket statistics for monitoring.
        
        Includes:
        - Buffer stats (dropped/processed messages)
        - Stream count
        - Slow streams (avg latency > 5000ms)
        - Per-stream average latency
        """
        stats: dict[str, Any] = {
            "streams_total": len(self._intended_streams),
            "streams_active": len(self._stream_last_message_ts),
            "slow_streams": list(self._slow_streams),
            "slow_streams_count": len(self._slow_streams),
            "buffer_stats": self._message_buffer.get_stats(),
        }
        
        # Calculate average latency per stream
        stream_latencies = {}
        for stream, latencies in self._stream_latency_ms.items():
            if latencies:
                avg_latency = sum(latencies) / len(latencies)
                stream_latencies[stream] = round(avg_latency, 2)
        
        if stream_latencies:
            stats["avg_latency_per_stream"] = stream_latencies
            # Overall average
            all_latencies = [sum(v)/len(v) for v in self._stream_latency_ms.values() if v]
            if all_latencies:
                stats["avg_latency_overall_ms"] = round(sum(all_latencies) / len(all_latencies), 2)
        
        return stats

    def get_ticker_snapshot(self, symbol: str) -> dict | None:
        """Return the latest 24hr ticker dict for *symbol*, or None."""
        return self._ticker_cache.get(symbol)

    def get_ticker_age_seconds(self, symbol: str) -> float | None:
        updated_at = self._ticker_update_times.get(symbol)
        if updated_at is None or updated_at <= 0.0:
            return None
        return round(time.monotonic() - updated_at, 3)

    def get_kline_cache(self, symbol: str, interval: str) -> list[dict] | None:
        """Return the most recent kline rows for *symbol*/*interval* as a list.

        Returns None if no data is cached yet. Rows are dicts with keys:
        time, open, high, low, close, volume, close_time, etc.
        """
        deq = self._klines.get(symbol, {}).get(interval)
        if not deq:
            return None
        return list(deq)

    def get_global_ticker_data(self) -> list[dict]:
        """Return all cached tickers in the format expected by build_shortlist.

        Each dict contains: symbol, quote_volume, price_change_percent,
        last_price (keys used by universe.build_shortlist).

        This method prefers full 24hr ticker data (!ticker@arr) but falls back
        to miniTicker (!miniTicker@arr) if available and full ticker is stale.
        """
        result: list[dict] = []
        now = time.monotonic()
        for symbol, t in self._ticker_cache.items():
            # Skip stale entries (older than freshness threshold)
            last_update = self._ticker_update_times.get(symbol, 0)
            if now - last_update > self._cfg.market_ticker_freshness_seconds:
                continue
            result.append(
                {
                    "symbol": symbol,
                    "quote_volume": t.get("quote_volume", 0.0),
                    "price_change_percent": t.get("price_change_percent", 0.0),
                    "last_price": t.get("last_price", 0.0),
                }
            )
        return result

    def get_mark_price_snapshot(self, symbol: str) -> dict | None:
        """Return the latest mark-price/funding dict for *symbol*, or None.

        Returned dict keys: ``mark_price`` (float), ``funding_rate`` (float),
        ``next_funding_time_ms`` (int).
        """
        return self._mark_price_cache.get(symbol)

    def get_mark_price_age_seconds(self, symbol: str) -> float | None:
        updated_at = self._mark_price_update_times.get(symbol)
        if updated_at is None or updated_at <= 0.0:
            return None
        return round(time.monotonic() - updated_at, 3)

    def get_book_snapshot(self, symbol: str) -> tuple[float | None, float | None]:
        """Return the latest (bid_price, ask_price) for *symbol*, or (None, None)."""
        return self._book.get(symbol, (None, None))

    def get_book_ticker_age_seconds(self, symbol: str) -> float | None:
        updated_at = self._book_update_times.get(symbol)
        if updated_at is None or updated_at <= 0.0:
            return None
        return round(time.monotonic() - updated_at, 3)

    def get_depth_imbalance(self, symbol: str) -> float | None:
        """Return a directional order-flow imbalance proxy in [-1, 1].

        Primary source is aggTrade ``delta_ratio`` already normalized to [-1, 1]:
        positive values imply net buyer pressure, negative imply seller pressure.
        Falls back to None when no directional proxy is available.
        """
        snapshot = self.get_agg_trade_snapshot(symbol)
        if snapshot is not None and snapshot.delta_ratio is not None:
            return round(max(-1.0, min(1.0, float(snapshot.delta_ratio))), 4)
        # Without level sizes we cannot compute true book imbalance from bids/asks only.
        return None

    def get_microprice_bias(self, symbol: str) -> float | None:
        """Calculate microprice bias from order book.

        Returns a signed bias proxy in [-1, 1], where positive means buy pressure
        and negative means sell pressure.

        We do not maintain full L2 sizes, so we approximate microprice pressure
        from recent aggTrade delta ratio when available.
        """
        bid, ask = self.get_book_snapshot(symbol)
        if bid is None or ask is None or bid <= 0 or ask <= 0:
            return None
        spread = ask - bid
        mid = (bid + ask) / 2.0
        if mid <= 0 or spread <= 0:
            return None
        snapshot = self.get_agg_trade_snapshot(symbol)
        if snapshot is None or snapshot.delta_ratio is None:
            return None
        return round(max(-1.0, min(1.0, float(snapshot.delta_ratio))), 4)

    def get_funding_sentiment(self) -> float | None:
        """Return the average funding rate across all tracked symbols.

        Positive → market is net-long / bullish crowding.
        Negative → market is net-short / bearish crowding.
        Returns None if the mark-price cache is empty.
        """
        rates = [
            v["funding_rate"]
            for v in self._mark_price_cache.values()
            if v.get("funding_rate") is not None
        ]
        if not rates:
            return None
        return sum(rates) / len(rates)

    def get_liquidation_sentiment(
        self,
        symbol: str | None = None,
        window_seconds: int = 60,
    ) -> float | None:
        """Return a liquidation sentiment score in [-1.0, +1.0].

        +1.0 → all recent liquidations were SHORT (buy-side squeeze, bullish).
        -1.0 → all recent liquidations were LONG (sell-side squeeze, bearish).
        Returns None if no liquidations in the window.

        Args:
            symbol: If given, filter to this symbol only.
            window_seconds: Look-back window in seconds.
        """
        cutoff_ms = int(time.time() * 1000) - window_seconds * 1000
        long_liq = short_liq = 0.0
        for entry in self._force_order_buffer:
            ts_ms, sym, side, qty, _price = entry
            if ts_ms < cutoff_ms:
                continue
            if symbol is not None and sym != symbol:
                continue
            if side == "BUY":  # liquidated SHORT → bullish (forced buy)
                short_liq += qty
            else:  # liquidated LONG → bearish (forced sell)
                long_liq += qty
        total = long_liq + short_liq
        if total == 0.0:
            return None
        # Positive = more short liquidations (bullish)
        return (short_liq - long_liq) / total

    async def rebuild_shortlist_on_demand(
        self,
        symbol_meta: list[Any],
        settings: Any,
    ) -> tuple[list[Any], dict[str, Any]]:
        """Rebuild shortlist using WS cache with timer-based throttling.

        This method is called from app.run_cycle() and implements the requested
        60-90 second rebuild interval to reduce load while keeping data fresh.

        Args:
            symbol_meta: List of exchange symbol metadata objects
            settings: Bot settings (for universe configuration)

        Returns:
            Tuple of (shortlist, summary_dict) compatible with universe.build_shortlist
        """
        now = time.monotonic()
        async with self._shortlist_rebuild_lock:
            time_since_last = now - self._last_shortlist_rebuild_ts
            if time_since_last < self._shortlist_rebuild_interval_seconds:
                LOG.debug("shortlist rebuild throttled | age=%.1fs < interval=%.1fs",
                         time_since_last, self._shortlist_rebuild_interval_seconds)
                return self._last_shortlist, self._last_shortlist_summary

            self._last_shortlist_rebuild_ts = now
            LOG.debug("shortlist rebuild triggered | age=%.1fs", time_since_last)

            # Import here to avoid circular dependency
            from .universe import build_shortlist

            if self.is_ticker_cache_warm():
                tickers = self.get_global_ticker_data()
                LOG.debug("shortlist from WS cache | symbols=%d", len(tickers))
            else:
                # Fall back to empty - caller should use REST
                LOG.debug("shortlist WS cache cold, signaling REST fallback needed")
                tickers = []

            shortlist, summary = build_shortlist(symbol_meta, tickers, settings)
            self._last_shortlist = shortlist
            self._last_shortlist_summary = summary
            return shortlist, summary

    def _should_throttle_ticker_update(self, symbol: str) -> bool:
        """Check if ticker update should be throttled (debounce rapid updates)."""
        now = time.monotonic()
        last_update = self._ticker_update_times.get(symbol, 0)
        elapsed_ms = (now - last_update) * 1000
        if elapsed_ms < self._min_ticker_update_interval_ms:
            # Log throttled ticker updates periodically (every 30s per symbol)
            last_logged = getattr(self, "_last_ticker_throttle_log", {}).get(symbol, 0.0)
            if now - last_logged >= 30.0:
                if not hasattr(self, "_last_ticker_throttle_log"):
                    self._last_ticker_throttle_log = {}
                self._last_ticker_throttle_log[symbol] = now
                LOG.debug(
                    "ticker throttled | symbol=%s elapsed=%.0fms min=%.0fms",
                    symbol, elapsed_ms, self._min_ticker_update_interval_ms,
                )
            return True
        self._ticker_update_times[symbol] = now
        return False

    def _should_throttle_mark_price_update(self, symbol: str) -> bool:
        """Check if mark price update should be throttled."""
        now = time.monotonic()
        last_update = self._mark_price_update_times.get(symbol, 0)
        elapsed_ms = (now - last_update) * 1000
        # Mark price updates every second, allow slightly faster
        if elapsed_ms < 50.0:  # 50ms throttle
            # Log throttled mark price updates periodically (every 30s per symbol)
            last_logged = getattr(self, "_last_markprice_throttle_log", {}).get(symbol, 0.0)
            if now - last_logged >= 30.0:
                if not hasattr(self, "_last_markprice_throttle_log"):
                    self._last_markprice_throttle_log = {}
                self._last_markprice_throttle_log[symbol] = now
                LOG.debug(
                    "mark_price throttled | symbol=%s elapsed=%.0fms min=50ms",
                    symbol, elapsed_ms,
                )
            return True
        self._mark_price_update_times[symbol] = now
        return False

    async def _backfill(self, symbols: list[str]) -> None:
        now = time.monotonic()
        filtered_symbols: list[str] = []
        for symbol in symbols:
            cooldown_until = self._backfill_cooldowns.get(symbol)
            if cooldown_until is not None and now < cooldown_until:
                LOG.warning(
                    "backfill cooldown active | symbol=%s remaining=%.1fs",
                    symbol,
                    cooldown_until - now,
                )
                continue
            filtered_symbols.append(symbol)
        tasks = [
            self._backfill_one(symbol, interval)
            for symbol in filtered_symbols
            for interval in self._cfg.kline_intervals
        ]
        if self._cfg.subscribe_book_ticker:
            tasks.extend(self._backfill_book_ticker(symbol) for symbol in filtered_symbols)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _backfill_one(self, symbol: str, interval: str) -> None:
        try:
            async with self._backfill_sem:
                df = await self._rest.fetch_klines(
                    symbol, interval, limit=self._cfg.kline_cache_size
                )
            if df.is_empty():
                return
            rows = df.to_dicts()
            deq: collections.deque = collections.deque(
                rows, maxlen=self._cfg.kline_cache_size
            )
            async with self._data_lock:
                if symbol not in self._klines:
                    self._klines[symbol] = {}
                self._klines[symbol][interval] = deq
            self._backfill_cooldowns.pop(symbol, None)
        except (ConnectionError, TimeoutError, ValueError, RuntimeError, MarketDataUnavailable) as exc:
            if self._cfg.backfill_failure_cooldown_seconds > 0:
                self._backfill_cooldowns[symbol] = (
                    time.monotonic() + self._cfg.backfill_failure_cooldown_seconds
                )
            LOG.debug("backfill failed | symbol=%s interval=%s: %s", symbol, interval, exc)

    async def _backfill_book_ticker(self, symbol: str) -> None:
        try:
            async with self._backfill_sem:
                bid, ask = await self._rest.fetch_book_ticker(symbol)
            async with self._data_lock:
                self._book[symbol] = (bid, ask)
            self._backfill_cooldowns.pop(symbol, None)
        except (ConnectionError, TimeoutError, ValueError, RuntimeError, MarketDataUnavailable) as exc:
            if self._cfg.backfill_failure_cooldown_seconds > 0:
                self._backfill_cooldowns[symbol] = (
                    time.monotonic() + self._cfg.backfill_failure_cooldown_seconds
                )
            LOG.debug("book ticker backfill failed | symbol=%s: %s", symbol, exc)

    def _get_ws_fallback_urls(self, endpoint: str) -> list[str]:
        """Return the WebSocket URL list for a single endpoint class.

        Cross-endpoint fallback is intentionally disabled so market streams
        cannot drift onto `/public` and public streams cannot drift onto `/market`.
        """
        return [self._build_stream_url(endpoint)]

    def _build_stream_url(self, endpoint: str) -> str:
        base = self._cfg.endpoint_base_url(endpoint).rstrip("/")
        if base.endswith("/stream") or base.endswith("/ws"):
            return base
        return f"{base}/stream"

    def _get_ws_url_version(self, endpoint: str) -> str:
        base = self._cfg.endpoint_base_url(endpoint)
        if "/public" in base:
            return "public"
        if "/market" in base:
            return "market"
        return "legacy"

    async def _run_stream(self, endpoint: str) -> None:
        delay = 1.0
        max_delay = self._cfg.reconnect_max_delay_seconds
        url = self._build_stream_url(endpoint)
        while self._running:
            connect_start = time.monotonic()
            backoff_reset = False
            reconnect_reason: str | None = None
            try:
                LOG.info(
                    "ws connecting | endpoint=%s url=%s streams=%d",
                    endpoint,
                    url,
                    len(self._intended_streams_by_endpoint.get(endpoint, set())),
                )
                ws = await asyncio.wait_for(
                    websockets.connect(
                        url,
                        ping_interval=20.0,
                        ping_timeout=20.0,
                        close_timeout=10.0,
                    ),
                    timeout=10.0,
                )
                LOG.info("ws connection established | endpoint=%s url=%s", endpoint, url)
                async with ws:
                    self._ws_conns[endpoint] = ws
                    self._connected_urls[endpoint] = url
                    self._connected_at_by_endpoint[endpoint] = time.monotonic()
                    if endpoint == _WS_MARKET:
                        self._ws_conn = ws
                    self._apply_tcp_keepalive(ws)
                    self._last_message_ts_by_endpoint[endpoint] = 0.0
                    self._last_message_ts = 0.0
                    self._last_event_lag_ms = None
                    self._connected_endpoints[endpoint].set()
                    self._refresh_connected_event()
                    self._connect_counts[endpoint] += 1
                    self._connect_count += 1
                    if self._connect_counts[endpoint] > 1 and self._reconnect_cb is not None:
                        asyncio.create_task(self._reconnect_cb())
                    LOG.info(
                        "ws connected | endpoint=%s url=%s streams=%d connect_count=%d endpoint_connect_count=%d",
                        endpoint,
                        url,
                        len(self._intended_streams_by_endpoint.get(endpoint, set())),
                        self._connect_count,
                        self._connect_counts[endpoint],
                    )
                    self._last_reconnect_reason = f"{endpoint}:connected"
                    self._last_reconnect_reason_by_endpoint[endpoint] = "connected"
                    await self._resubscribe_all(endpoint, ws)
                    stream_count = len(self._intended_streams_by_endpoint.get(endpoint, set()))
                    if stream_count > 120:
                        LOG.info(
                            "high stream count | endpoint=%s streams=%d shortlist=%d",
                            endpoint,
                            stream_count,
                            len(self._symbols),
                        )
                    health_task = asyncio.create_task(
                        self._health_monitor(ws, endpoint),
                        name=f"ws_manager_health:{endpoint}",
                    )
                    try:
                        async for raw in ws:
                            if not self._running:
                                return
                            elapsed = time.monotonic() - connect_start
                            if not backoff_reset and elapsed >= _BACKOFF_RESET_AFTER_SECONDS:
                                delay = 1.0
                                backoff_reset = True
                                self._short_lived_streak = 0
                            if elapsed >= _PROACTIVE_RECONNECT_AFTER_SECONDS:
                                reconnect_reason = "24h_proactive"
                                break
                            try:
                                msg = _json.loads(raw) if _USE_ORJSON else json.loads(raw)
                            except (json.JSONDecodeError, UnicodeDecodeError):
                                continue
                            await self._handle_message(msg, endpoint)
                    finally:
                        health_task.cancel()
                        try:
                            await health_task
                        except asyncio.CancelledError:
                            pass
                self._ws_conns[endpoint] = None
                self._connected_urls[endpoint] = None
                self._connected_at_by_endpoint[endpoint] = 0.0
                self._connected_endpoints[endpoint].clear()
                self._refresh_connected_event()
                if endpoint == _WS_MARKET:
                    self._ws_conn = None
                if reconnect_reason == "24h_proactive":
                    self._last_reconnect_reason = f"{endpoint}:{reconnect_reason}"
                    self._last_reconnect_reason_by_endpoint[endpoint] = reconnect_reason
                    LOG.info(
                        "ws proactive reconnect | endpoint=%s uptime=%.1fh",
                        endpoint,
                        (time.monotonic() - connect_start) / 3600,
                    )
                    delay = 1.0
                    self._short_lived_streak = 0
                    if endpoint == _WS_MARKET:
                        stale = self._stale_symbols()
                        await self._maybe_backfill_after_disconnect(
                            elapsed=time.monotonic() - connect_start,
                            stale_symbols=stale,
                        )
                    continue
                raise ConnectionError("stream closed without explicit close frame")
            except asyncio.CancelledError:
                self._ws_conns[endpoint] = None
                self._connected_urls[endpoint] = None
                self._connected_at_by_endpoint[endpoint] = 0.0
                self._connected_endpoints[endpoint].clear()
                self._refresh_connected_event()
                if endpoint == _WS_MARKET:
                    self._ws_conn = None
                return
            except (ws_exceptions.ConnectionClosed, ws_exceptions.InvalidStatus, ConnectionError, TimeoutError, OSError) as exc:
                self._ws_conns[endpoint] = None
                self._connected_urls[endpoint] = None
                self._connected_at_by_endpoint[endpoint] = 0.0
                self._connected_endpoints[endpoint].clear()
                self._refresh_connected_event()
                if endpoint == _WS_MARKET:
                    self._ws_conn = None
                if not self._running:
                    return

                elapsed = time.monotonic() - connect_start
                error_text = str(exc).lower()
                keepalive_timeout = "keepalive ping timeout" in error_text
                if elapsed < _BACKOFF_RESET_AFTER_SECONDS and not keepalive_timeout:
                    self._short_lived_streak += 1
                else:
                    self._short_lived_streak = 0
                close_detail = ""
                if isinstance(exc, ws_exceptions.ConnectionClosed):
                    _code = exc.rcvd.code if exc.rcvd else "none"
                    _reason = repr(exc.rcvd.reason) if exc.rcvd else ""
                    close_detail = f" code={_code} reason={_reason}"
                if keepalive_timeout:
                    min_delay = 1.0
                elif self._short_lived_streak >= 8:
                    min_delay = 300.0
                elif self._short_lived_streak >= 5:
                    min_delay = 30.0
                elif self._short_lived_streak >= 3:
                    min_delay = 5.0
                else:
                    min_delay = 1.0
                delay = max(delay, min_delay)
                delay += random.uniform(0.0, min(0.5, delay * 0.1))
                self._last_reconnect_reason = f"{endpoint}:{exc}"
                self._last_reconnect_reason_by_endpoint[endpoint] = str(exc)
                level = logging.WARNING if self._short_lived_streak >= 3 else logging.INFO
                LOG.log(
                    level,
                    "ws disconnected | endpoint=%s url=%s error=%s%s uptime=%.1fs retry_in=%.1fs streak=%d",
                    endpoint,
                    url,
                    exc,
                    close_detail,
                    elapsed,
                    delay,
                    self._short_lived_streak,
                )
                try:
                    await asyncio.sleep(delay)
                except asyncio.CancelledError:
                    return
                delay = min(delay * 2.0, max_delay)
                if endpoint == _WS_MARKET:
                    stale = self._stale_symbols()
                    await self._maybe_backfill_after_disconnect(
                        elapsed=elapsed,
                        stale_symbols=stale,
                    )
            except Exception as exc:
                LOG.error(
                    "ws unexpected error during connection | endpoint=%s error=%s (%s)",
                    endpoint,
                    exc,
                    type(exc).__name__,
                )
                self._ws_conns[endpoint] = None
                self._connected_urls[endpoint] = None
                self._connected_at_by_endpoint[endpoint] = 0.0
                self._connected_endpoints[endpoint].clear()
                self._refresh_connected_event()
                if endpoint == _WS_MARKET:
                    self._ws_conn = None
                if not self._running:
                    return
                delay = min(max(delay, 1.0) * 2.0, max_delay)
                try:
                    await asyncio.sleep(delay)
                except asyncio.CancelledError:
                    return

    def _handle_ws_response(self, msg: dict, endpoint: str) -> None:
        if msg.get("error"):
            self._subscription_errors[endpoint] = msg["error"]
            LOG.warning(
                "ws subscription error from Binance | endpoint=%s error=%s",
                endpoint,
                msg["error"],
            )
            return
        # Parse rate limits from response (Binance includes rateLimits in WS API responses)
        if "rateLimits" in msg:
            try:
                rate_limits = msg["rateLimits"]
                if isinstance(rate_limits, list):
                    for limit in rate_limits:
                        limit_type = limit.get("rateLimitType")
                        interval = limit.get("interval")
                        limit_val = limit.get("limit")
                        count = limit.get("count")
                        if limit_type and count is not None and limit_val is not None:
                            usage_pct = (count / limit_val) * 100 if limit_val > 0 else 0
                            if usage_pct > 80:
                                LOG.warning(
                                    "ws rate limit high usage | endpoint=%s type=%s interval=%s count=%d limit=%d usage=%.1f%%",
                                    endpoint, limit_type, interval, count, limit_val, usage_pct
                                )
            except (TypeError, ValueError, KeyError):
                pass
        if "result" not in msg:
            return
        result = msg["result"]
        if isinstance(result, list):
            self._subscription_ack_count[endpoint] += 1
            expected = len(self._intended_streams_by_endpoint.get(endpoint, set()))
            actual = len(result)
            if expected != actual:
                LOG.debug(
                    "ws subscriptions mismatch | endpoint=%s expected=%d actual=%d",
                    endpoint,
                    expected,
                    actual,
                )
            else:
                LOG.debug("ws subscriptions confirmed | endpoint=%s active=%d", endpoint, actual)
        else:
            self._subscription_ack_count[endpoint] += 1
            LOG.debug("ws command ack | endpoint=%s id=%s", endpoint, msg.get("id"))

    async def _dispatch_event(self, data: dict) -> None:
        """Dispatch a single market data event dict to the appropriate handler.

        kline/bookTicker/aggTrade handlers are async and acquire _data_lock to
        prevent races with subscribe() cleanup.  ticker/markPrice/forceOrder
        handlers are fine-grained and do not need the lock.
        """
        event_type = str(data.get("e", ""))
        symbol = str(data.get("s", "")).upper()
        event_time = data.get("E")
        if event_time is not None:
            try:
                self._last_event_lag_ms = max(0.0, (time.time() * 1000) - float(event_time))
                if (
                    symbol
                    and event_type in _LATENCY_WARNING_EVENTS
                    and self._last_event_lag_ms >= _HIGH_EVENT_LATENCY_MS
                ):
                    now = time.monotonic()
                    last_warn = self._last_latency_warning_by_symbol.get(symbol, 0.0)
                    if now - last_warn >= _LATENCY_WARNING_INTERVAL_SECONDS:
                        self._last_latency_warning_by_symbol[symbol] = now
                        LOG.debug(
                            "ws high latency | lag_ms=%.1f symbol=%s event=%s",
                            self._last_event_lag_ms,
                            symbol,
                            event_type,
                        )
            except (TypeError, ValueError):
                pass
        if event_type == "kline":
            if symbol:
                await self._handle_kline(symbol, data)
        elif event_type == "bookTicker":
            if symbol:
                await self._handle_book_ticker(symbol, data)
        elif event_type == "aggTrade":
            if symbol:
                await self._handle_agg_trade(symbol, data)
        elif event_type == "24hrTicker":
            if symbol:
                self._handle_ticker(symbol, data)
        elif event_type == "markPriceUpdate":
            if symbol:
                self._handle_mark_price(symbol, data)
        elif event_type == "miniTicker":
            if symbol:
                self._handle_mini_ticker(symbol, data)
        elif event_type == "forceOrder":
            self._handle_force_order(data)

    async def _handle_message(self, msg: dict, endpoint: str) -> None:
        self._last_message_ts = time.monotonic()
        self._last_message_ts_by_endpoint[endpoint] = self._last_message_ts
        if "result" in msg or "error" in msg:
            self._handle_ws_response(msg, endpoint)
            return

        buffered = await self._message_buffer.put(msg)
        if not buffered:
            # Backpressure fallback: process directly to avoid full data starvation.
            await self._process_message_internal(msg)
    
    async def _process_message_internal(self, msg: dict) -> None:
        """Internal message processing with latency tracking."""
        start_ts = time.monotonic()
        data = msg.get("data")
        stream = msg.get("stream", "unknown")
        
        # P3: Per-stream latency tracking
        if stream not in self._stream_latency_ms:
            self._stream_latency_ms[stream] = collections.deque(maxlen=10)
        
        # !ticker@arr and !markPrice@arr deliver a LIST of event objects.
        # Process all items to populate global caches, but only dispatch events
        # for symbols in our shortlist to avoid unnecessary processing.
        if isinstance(data, list):
            symbol_set = set(self._symbols)
            count = 0
            for item in data:
                if not isinstance(item, dict):
                    continue
                event_type = str(item.get("e", ""))
                sym = str(item.get("s", "")).upper()
                # forceOrder arrives as a wrapper; inner symbol is in item["o"]["s"]
                if event_type == "forceOrder":
                    await self._dispatch_event(item)
                elif event_type == "24hrTicker":
                    # Save ALL tickers to cache for shortlist building, but only dispatch for shortlist
                    self._handle_ticker(sym, item)
                    if sym in symbol_set:
                        await self._dispatch_event(item)
                elif event_type == "markPriceUpdate":
                    # Save ALL mark prices to cache
                    self._handle_mark_price(sym, item)
                    if sym in symbol_set:
                        await self._dispatch_event(item)
                elif sym and sym in symbol_set:
                    await self._dispatch_event(item)
                # Yield to event loop every 10 dispatched items so other tasks
                # can run and WebSocket messages continue to be received.
                count += 1
                if count % 10 == 0:
                    await asyncio.sleep(0)
            
            # Record latency for batch
            latency_ms = (time.monotonic() - start_ts) * 1000
            self._stream_latency_ms[stream].append(latency_ms)
            return
            
        # Per-symbol streams and !forceOrder@arr deliver a single dict
        if isinstance(data, dict):
            await self._dispatch_event(data)
            # Record latency
            latency_ms = (time.monotonic() - start_ts) * 1000
            self._stream_latency_ms[stream].append(latency_ms)
            
            # P3: Check if stream is slow (avg latency > 5000ms)
            if len(self._stream_latency_ms[stream]) >= 5:
                avg_latency = sum(self._stream_latency_ms[stream]) / len(self._stream_latency_ms[stream])
                if avg_latency > 5000 and stream not in self._slow_streams:
                    self._slow_streams.add(stream)
                    LOG.warning("slow stream detected | stream=%s avg_latency_ms=%.1f", stream, avg_latency)

    async def _process_buffered_messages(self) -> None:
        """P1: Background task to process buffered messages when rate limit allows."""
        while self._running:
            try:
                # Wait for a slot in rate limiter
                await self._incoming_rate_limiter.wait_for_slot()
                
                # Get message from buffer
                msg = await self._message_buffer.get()
                if msg is not None:
                    await self._process_message_internal(msg)
                else:
                    # Buffer empty, sleep briefly
                    await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                LOG.debug("buffer processor error: %s", exc)
                await asyncio.sleep(0.5)

    async def _handle_kline(self, symbol: str, data: dict) -> None:
        """Handle closed kline (candle) events.  Acquires _data_lock to prevent
        races with subscribe() cleanup of self._klines."""
        if self._symbols and symbol not in self._symbols:
            return
        k = data.get("k", {})
        if not k.get("x"):
            return
        interval = str(k.get("i", ""))
        LOG.info("kline closed | symbol=%s interval=%s", symbol, interval)
        if interval not in self._cfg.kline_intervals:
            return
        row = _ws_kline_to_row(k)
        async with self._data_lock:
            if symbol not in self._klines:
                self._klines[symbol] = {}
            if interval not in self._klines[symbol]:
                self._klines[symbol][interval] = collections.deque(
                    maxlen=self._cfg.kline_cache_size
                )
            deq = self._klines[symbol][interval]
            # Gap detection: fire before appending so backfill can repair the hole
            interval_secs = _INTERVAL_SECONDS.get(interval, 0)
            if interval_secs > 0 and deq:
                gap_secs = (row["time"] - deq[-1]["close_time"]).total_seconds()
                if gap_secs > interval_secs * 0.9:
                    missed = max(1, round(gap_secs / interval_secs) - 1)
                    LOG.error(
                        "kline gap detected | symbol=%s interval=%s "
                        "last_close=%s new_open=%s missed_candles=%d — triggering backfill",
                        symbol, interval,
                        deq[-1]["close_time"], row["time"], missed,
                    )
                    asyncio.create_task(
                        self._backfill([symbol]),
                        name=f"gap_backfill:{symbol}:{interval}",
                    )
            deq.append(row)

        # Fire candle-close callbacks (fire-and-forget; data already in deque)
        close_ts_ms = int(k.get("T", 0))
        cbs = self._kline_close_cbs.get(interval)
        if cbs:
            for _cb in cbs:
                task = asyncio.create_task(_cb(symbol, interval, close_ts_ms))
                self._attach_task_logging(task, label=f"kline_close:{symbol}:{interval}")

        # Publish to EventBus (primary path when SignalBot uses EventBus)
        if self._event_bus is not None:
            from .core.events import KlineCloseEvent
            self._event_bus.publish_nowait(
                KlineCloseEvent(symbol=symbol, interval=interval, close_ts=close_ts_ms)
            )
            LOG.info("kline published to EventBus | symbol=%s interval=%s", symbol, interval)
        else:
            LOG.warning("kline NOT published - EventBus is None | symbol=%s", symbol)

    async def _handle_book_ticker(self, symbol: str, data: dict) -> None:
        """Handle bookTicker events.  Acquires _data_lock to prevent races."""
        if self._symbols and symbol not in self._symbols:
            return
        try:
            bid = float(data["b"]) if data.get("b") is not None else None
            ask = float(data["a"]) if data.get("a") is not None else None
            event_ts_ms = int(data["E"]) if data.get("E") is not None else None
        except (KeyError, TypeError, ValueError):
            return
        async with self._data_lock:
            self._book[symbol] = (bid, ask)
            self._book_update_times[symbol] = time.monotonic()

        # Publish to EventBus so subscribers can trigger intra-candle scans.
        # These events are extremely frequent — subscribers MUST throttle.
        if self._event_bus is not None:
            from .core.events import BookTickerEvent
            self._event_bus.publish_nowait(
                BookTickerEvent(symbol=symbol, bid=bid, ask=ask, event_ts_ms=event_ts_ms)
            )

    async def _handle_agg_trade(self, symbol: str, data: dict) -> None:
        """Handle aggTrade events.  Acquires _data_lock to prevent races."""
        if self._symbols and symbol not in self._symbols:
            return
        try:
            trade = AggTrade(
                symbol=symbol,
                trade_id=int(data["a"]),
                price=float(data["p"]),
                quantity=float(data["q"]),
                trade_time_ms=int(data["T"]),
                is_buyer_maker=bool(data["m"]),
            )
        except (KeyError, TypeError, ValueError):
            return
        async with self._data_lock:
            if symbol not in self._agg_trades:
                self._agg_trades[symbol] = collections.deque(
                    maxlen=self._cfg.max_agg_trade_buffer
                )
            self._agg_trades[symbol].append(trade)

        # Fire trade callbacks (fire-and-forget; used for real-time TP/SL tracking)
        if self._agg_trade_cbs:
            trade_dt = datetime.fromtimestamp(trade.trade_time_ms / 1000.0, tz=timezone.utc)
        for _cb in self._agg_trade_cbs:
            task = asyncio.create_task(_cb(symbol, trade.price, trade_dt))
            self._attach_task_logging(task, label=f"agg_trade:{symbol}")

    def _handle_ticker(self, symbol: str, data: dict) -> None:
        """Handle 24hrTicker events from !ticker@arr.

        Binance Futures 24hr ticker fields:
          c = last price, q = quote volume (24h), P = price change %,
          p = price change abs, o = open price, h = high, l = low.
        """
        # Throttle rapid updates for same symbol
        if self._should_throttle_ticker_update(symbol):
            return
        try:
            self._ticker_cache[symbol] = {
                "symbol": symbol,
                "last_price": float(data.get("c") or 0.0),
                "quote_volume": float(data.get("q") or 0.0),
                "price_change_percent": float(data.get("P") or 0.0),
                "price_change": float(data.get("p") or 0.0),
                "open_price": float(data.get("o") or 0.0),
                "high_price": float(data.get("h") or 0.0),
                "low_price": float(data.get("l") or 0.0),
            }
            self._ticker_cache_ts = time.monotonic()
        except (TypeError, ValueError):
            pass

    def _handle_mini_ticker(self, symbol: str, data: dict) -> None:
        """Handle miniTicker events from !miniTicker@arr (lightweight fallback).

        miniTicker fields:
          c = last price, q = quote volume (24h), v = base volume,
          h = high, l = low, o = open.
        """
        # Only use miniTicker if full ticker is stale for this symbol
        now = time.monotonic()
        last_full_update = self._ticker_update_times.get(symbol, 0)
        if now - last_full_update < self._cfg.market_ticker_freshness_seconds:
            return  # Full ticker is fresh, skip miniTicker

        if self._should_throttle_ticker_update(symbol):
            return

        try:
            # Calculate price change % from open
            close_price = float(data.get("c") or 0.0)
            open_price = float(data.get("o") or 0.0)
            price_change_pct = 0.0
            if open_price > 0:
                price_change_pct = (close_price - open_price) / open_price * 100.0

            self._ticker_cache[symbol] = {
                "symbol": symbol,
                "last_price": close_price,
                "quote_volume": float(data.get("q") or 0.0),
                "price_change_percent": price_change_pct,
                "price_change": close_price - open_price,
                "open_price": open_price,
                "high_price": float(data.get("h") or 0.0),
                "low_price": float(data.get("l") or 0.0),
            }
            self._ticker_cache_ts = time.monotonic()
        except (TypeError, ValueError):
            pass

    def _handle_mark_price(self, symbol: str, data: dict) -> None:
        """Handle markPriceUpdate events from !markPrice@arr@1s.

        Binance Futures mark price fields:
          p = mark price, r = funding rate, T = next funding time ms,
          i = index price.
        """
        # Throttle very rapid updates
        if self._should_throttle_mark_price_update(symbol):
            return
        try:
            funding_str = data.get("r")
            funding_rate = float(funding_str) if funding_str not in (None, "", "0") else 0.0
            self._mark_price_cache[symbol] = {
                "symbol": symbol,
                "mark_price": float(data.get("p") or 0.0),
                "index_price": float(data.get("i") or 0.0),
                "funding_rate": funding_rate,
                "next_funding_time_ms": int(data.get("T") or 0),
            }
        except (TypeError, ValueError):
            pass

    def _handle_force_order(self, data: dict) -> None:
        """Handle forceOrder (liquidation) events from !forceOrder@arr.

        The inner order object ``o`` carries: s=symbol, S=side (BUY/SELL),
        q=original qty, ap=average fill price, T=trade time ms.
        Side BUY = a short position was liquidated (bullish pressure).

        Note (2026-04-10): Binance changed from sending 'latest' to 'largest'
        liquidation order within 1000ms window. Buffer logic remains unchanged
        as we process individual events as received.
        Side SELL = a long position was liquidated (bearish pressure).
        """
        try:
            order = data.get("o", {})
            symbol = str(order.get("s") or "").upper()
            side = str(order.get("S") or "").upper()  # BUY or SELL
            qty = float(order.get("q") or 0.0)
            price = float(order.get("ap") or order.get("p") or 0.0)
            ts_ms = int(order.get("T") or data.get("E") or (time.time() * 1000))
            if symbol and side in ("BUY", "SELL") and qty > 0:
                self._force_order_buffer.append((ts_ms, symbol, side, qty, price))
        except (TypeError, ValueError, KeyError):
            pass
