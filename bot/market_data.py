from __future__ import annotations

import asyncio
import logging
import math
import random
import time
from collections import deque
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, cast

import polars as pl
import aiohttp
try:
    from binance import AsyncClient
    from binance.exceptions import BinanceAPIException as BinanceNetworkError
except ImportError:
    AsyncClient = None  # type: ignore
    BinanceNetworkError = Exception  # type: ignore

from .models import AggTrade, AggTradeSnapshot, SymbolFrames, SymbolMeta

if TYPE_CHECKING:
    from .ws_manager import FuturesWSManager


UTC = timezone.utc
LOG = logging.getLogger("bot.market_data")
# Binance USD-M Futures request weight limit is 2400 per minute (see exchangeInfo -> rateLimits).
# Keep a client-side buffer to reduce 429 risk on shared IPs / bursts.
_REST_WEIGHT_SOFT_LIMIT = 1800
_REST_WEIGHT_HARD_LIMIT = 2200
_REST_WEIGHT_CRITICAL_LIMIT = 2350

_FAPI_BASE_URL = "https://fapi.binance.com"

# Global semaphore to prevent REST API flood during startup
_REST_GLOBAL_SEMAPHORE = asyncio.Semaphore(5)

# /futures/data/* request-based IP limit (not weight-based).
# Docs example (Open Interest Statistics): "IP rate limit 1000 requests/5min".
_FUTURES_DATA_IP_LIMIT_WINDOW_S = 300.0
_FUTURES_DATA_IP_LIMIT_MAX = 1000

# Cache TTL settings for graceful degradation (seconds)
_CACHE_TTL = {
    "klines_15m": 60,           # 1 minute - critical for trading
    "klines_1h": 300,           # 5 minutes
    "klines_4h": 900,           # 15 minutes
    "open_interest": 600,       # 10 minutes - non-critical
    "open_interest_change": 600,
    "long_short_ratio": 600,    # 10 minutes - non-critical
    "taker_ratio": 600,
    "global_ls_ratio": 600,
    "funding_rate": 300,        # 5 minutes
    "funding_history": 1800,    # 30 minutes
    "basis": 600,
    "book_ticker": 5,           # 5 seconds - use WS primarily
}

# Client-side weight estimates per operation (Binance Futures April 2026).
# klines: limit=240 → weight=5; all others default to 1 unless listed.
_ENDPOINT_WEIGHTS: dict[str, int] = {
    # Official docs: GET /fapi/v1/exchangeInfo weight=1
    "exchange_information": 1,
    "ticker24hr_price_change_statistics": 40,
    "symbol_order_book_ticker": 1,
    "compressed_aggregate_trades_list:snapshot": 2,
    "compressed_aggregate_trades_list:history": 2,
    "open_interest": 1,
    # /futures/data/openInterestHist has request weight=0; it is constrained by an IP request limit instead.
    "open_interest_statistics": 0,
    "top_trader_long_short_ratio_accounts": 1,
    "premium_index": 1,
}

_FUTURES_DATA_REQUEST_LIMITED_OPS: set[str] = {
    # /futures/data/*
    "open_interest_statistics",
    "top_trader_long_short_ratio_accounts",
}


class _SlidingWindowRateLimiter:
    """Sliding-window limiter for request-based quotas."""

    def __init__(self, *, max_requests: int, window_seconds: float) -> None:
        self._max_requests = max(1, int(max_requests))
        self._window_seconds = float(window_seconds)
        self._times: deque[float] = deque()
        self._lock = asyncio.Lock()

    async def acquire(self, *, label: str) -> None:
        async with self._lock:
            now = time.monotonic()
            cutoff = now - self._window_seconds
            while self._times and self._times[0] < cutoff:
                self._times.popleft()
            if len(self._times) >= self._max_requests:
                sleep_s = max(0.0, (self._times[0] + self._window_seconds) - now) + 0.05
                LOG.warning(
                    "futures-data request budget exhausted | sleeping=%.2fs label=%s used=%d limit=%d window=%.0fs",
                    sleep_s,
                    label,
                    len(self._times),
                    self._max_requests,
                    self._window_seconds,
                )
                await asyncio.sleep(sleep_s)
                now = time.monotonic()
                cutoff = now - self._window_seconds
                while self._times and self._times[0] < cutoff:
                    self._times.popleft()
            self._times.append(time.monotonic())


class MarketDataUnavailable(RuntimeError):
    def __init__(self, *, operation: str, detail: str, symbol: str | None = None) -> None:
        self.operation = operation
        self.detail = detail
        self.symbol = symbol
        scope = f" for {symbol}" if symbol else ""
        super().__init__(f"{operation}{scope} unavailable: {detail}")


def _timeframe_to_seconds(timeframe: str) -> int | None:
    mapping = {
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
    return mapping.get(timeframe)


def _ohlcv_frame_has_incomplete_tail(df: pl.DataFrame, timeframe: str) -> bool:
    if df.is_empty():
        return False
    timeframe_seconds = _timeframe_to_seconds(timeframe)
    if timeframe_seconds is None:
        return False
    last_open = df["time"].tail(1).item()
    return datetime.now(UTC) < last_open + timedelta(seconds=timeframe_seconds)


def _drop_incomplete_ohlcv_tail(df: pl.DataFrame, timeframe: str) -> pl.DataFrame:
    if df.is_empty():
        return df
    if _ohlcv_frame_has_incomplete_tail(df, timeframe):
        return df.head(df.height - 1)
    return df


def _klines_to_frame(rows: Any) -> pl.DataFrame:
    frame_rows: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, list) or len(row) < 11:
            continue
        frame_rows.append(
            {
                "time": datetime.fromtimestamp(int(row[0]) / 1000.0, tz=UTC),
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4]),
                "volume": float(row[5]),
                "close_time": datetime.fromtimestamp(int(row[6]) / 1000.0, tz=UTC),
                "quote_volume": float(row[7]),
                "num_trades": int(row[8]),
                "taker_buy_base_volume": float(row[9]),
                "taker_buy_quote_volume": float(row[10]),
            }
        )
    return pl.DataFrame(frame_rows)


def _unwrap_model(value: Any) -> Any:
    if hasattr(value, "actual_instance") and getattr(value, "actual_instance") is not None:
        return value.actual_instance
    return value


def _coerce_rest_row(item: Any) -> Mapping[str, Any]:
    row = _unwrap_model(item)
    if isinstance(row, Mapping):
        return row
    if hasattr(row, "model_dump"):
        dumped = row.model_dump()
        if isinstance(dumped, Mapping):
            return dumped
    raise TypeError(f"Unsupported REST row payload type: {type(item)!r}")


class BinanceFuturesMarketData:
    def __init__(
        self,
        *,
        ws_manager: FuturesWSManager | None = None,
        rest_timeout_seconds: float = 8.0,
    ) -> None:
        self._rest_timeout = rest_timeout_seconds
        # AsyncClient for public market data (no API keys needed)
        if AsyncClient is None:
            raise ImportError("binance library is required but not installed")
        self.client: Any = AsyncClient(
            api_key=None,
            api_secret=None,
            requests_params={"timeout": rest_timeout_seconds}
        )
        self._exchange_info_cache: tuple[float, list[SymbolMeta]] | None = None
        self._ticker_24h_cache: tuple[float, list[dict[str, float | str]]] | None = None
        self._funding_rate_cache: dict[str, tuple[float, float]] = {}
        self._open_interest_cache: dict[str, tuple[float, float]] = {}
        self._open_interest_change_cache: dict[tuple[str, str], tuple[float, float]] = {}
        self._long_short_ratio_cache: dict[tuple[str, str], tuple[float, float]] = {}
        self._taker_ratio_cache: dict[tuple[str, str], tuple[float, float]] = {}
        self._global_ls_ratio_cache: dict[tuple[str, str], tuple[float, float]] = {}
        self._funding_history_cache: dict[str, tuple[float, list[dict]]] = {}
        self._basis_cache: dict[tuple[str, str], tuple[float, float | None]] = {}
        self._basis_stats_cache: dict[tuple[str, str], tuple[float, dict[str, float | None]]] = {}
        self._ws: FuturesWSManager | None = ws_manager
        self._last_rest_weight_1m: int | None = None
        self._last_rest_response_time_ms: float | None = None
        self._rate_limit_pause_until = 0.0
        self._rate_limit_error_streak = 0
        self._weight_window_weight: int = 0
        self._weight_window_start: float = 0.0
        # Request-based limiter for /futures/data/*
        self._futures_data_limiter = _SlidingWindowRateLimiter(
            max_requests=_FUTURES_DATA_IP_LIMIT_MAX,
            window_seconds=_FUTURES_DATA_IP_LIMIT_WINDOW_S,
        )
        # Shared aiohttp session for manual REST endpoints (avoid per-call session churn)
        self._http_session: aiohttp.ClientSession | None = None
        # Klines cache to prevent REST stampedes on startup / reconnect backfills
        self._klines_cache: dict[tuple[str, str, int], tuple[float, pl.DataFrame]] = {}
        self._klines_locks: dict[tuple[str, str, int], asyncio.Lock] = {}
        # Circuit breaker state per operation
        self._circuit_failures: dict[str, int] = {}
        self._circuit_open_until: dict[str, float] = {}
        self._circuit_failure_threshold = 3
        self._circuit_open_duration_seconds = 30.0

    @staticmethod
    def _header_value(headers: Any, name: str) -> str | None:
        if not isinstance(headers, Mapping):
            return None
        needle = name.lower()
        for key, value in headers.items():
            if str(key).lower() == needle and value is not None:
                return str(value).strip()
        return None

    def _set_rate_limit_pause(self, seconds: float) -> None:
        if seconds <= 0:
            return
        self._rate_limit_pause_until = max(
            self._rate_limit_pause_until,
            time.monotonic() + seconds,
        )

    def _capture_retry_after(self, headers: Any) -> int | None:
        retry_after_raw = self._header_value(headers, "Retry-After")
        if retry_after_raw is None:
            return None
        try:
            retry_after = max(0, int(float(retry_after_raw)))
        except (TypeError, ValueError):
            return None
        if retry_after > 0:
            self._set_rate_limit_pause(retry_after)
        return retry_after

    @staticmethod
    def _calculate_backoff(attempt: int, *, base_delay: float = 1.0, cap: float = 60.0) -> float:
        delay = base_delay * (2 ** max(attempt, 0))
        jitter = random.uniform(0.5, 1.5)
        return min(delay * jitter, cap)

    def _is_circuit_open(self, operation: str) -> bool:
        """Check if circuit breaker is open for operation."""
        open_until = self._circuit_open_until.get(operation, 0.0)
        if time.monotonic() < open_until:
            return True
        return False

    def _record_circuit_failure(self, operation: str) -> None:
        """Record a failure and open circuit if threshold reached."""
        failures = self._circuit_failures.get(operation, 0) + 1
        self._circuit_failures[operation] = failures
        if failures >= self._circuit_failure_threshold:
            open_until = time.monotonic() + self._circuit_open_duration_seconds
            self._circuit_open_until[operation] = open_until
            LOG.warning(
                "circuit breaker opened | operation=%s failures=%d duration=%.0fs",
                operation, failures, self._circuit_open_duration_seconds
            )
            self._circuit_failures[operation] = 0

    def _record_circuit_success(self, operation: str) -> None:
        """Reset failure count on success."""
        if operation in self._circuit_failures:
            del self._circuit_failures[operation]

    def _is_cache_valid(self, cache_entry: tuple[float, Any] | None, ttl_seconds: int) -> bool:
        """Check if cache entry is still valid based on TTL."""
        if cache_entry is None:
            return False
        cached_at, _ = cache_entry
        return (time.monotonic() - cached_at) < ttl_seconds

    def _get_cached_or_none(self, cache: dict[str, tuple[float, Any]], key: str, ttl: int) -> Any | None:
        """Get cached value if valid, otherwise return None."""
        entry = cache.get(key)
        if self._is_cache_valid(entry, ttl):
            return entry[1] if entry else None
        return None

    def _estimate_weight(self, operation: str) -> int:
        """Return estimated request weight for client-side budget tracking."""
        if operation.startswith("kline_candlestick_data"):
            return 5  # limit=240 > 100
        return _ENDPOINT_WEIGHTS.get(operation, 1)

    def _track_weight(self, operation: str) -> None:
        """Accumulate client-side weight estimate; warn when approaching hard limit."""
        now = time.monotonic()
        if now - self._weight_window_start >= 60.0:
            self._weight_window_weight = 0
            self._weight_window_start = now
        self._weight_window_weight += self._estimate_weight(operation)
        if self._weight_window_weight >= _REST_WEIGHT_HARD_LIMIT:
            LOG.warning(
                "client-side weight budget at hard limit | estimated_1m=%d operation=%s",
                self._weight_window_weight,
                operation,
            )
        elif self._weight_window_weight >= _REST_WEIGHT_SOFT_LIMIT:
            LOG.info(
                "client-side weight budget elevated | estimated_1m=%d",
                self._weight_window_weight,
            )

    def _capture_response_metadata(self, response: Any, *, operation: str | None = None) -> None:
        headers = getattr(response, "headers", None)
        if not isinstance(headers, Mapping):
            return
        weight_raw = None if operation == "symbol_order_book_ticker" else self._header_value(headers, "x-mbx-used-weight-1m")
        response_time_raw = self._header_value(headers, "x-response-time")
        try:
            if weight_raw is not None:
                self._last_rest_weight_1m = int(weight_raw)
        except (TypeError, ValueError):
            self._last_rest_weight_1m = None
        try:
            if response_time_raw is not None:
                self._last_rest_response_time_ms = float(response_time_raw.rstrip("ms"))
        except (TypeError, ValueError):
            self._last_rest_response_time_ms = None
        retry_after = self._capture_retry_after(headers)
        if retry_after:
            LOG.warning("binance rest requested backoff | retry_after=%ss", retry_after)
        if self._last_rest_weight_1m is not None:
            if self._last_rest_weight_1m >= _REST_WEIGHT_CRITICAL_LIMIT:
                LOG.error(
                    "binance rest weight critical | used_weight_1m=%s - pausing 15s",
                    self._last_rest_weight_1m,
                )
                self._set_rate_limit_pause(15.0)
            elif self._last_rest_weight_1m >= _REST_WEIGHT_HARD_LIMIT:
                LOG.warning(
                    "binance rest weight hard limit | used_weight_1m=%s - applying 5s backoff",
                    self._last_rest_weight_1m,
                )
                self._set_rate_limit_pause(5.0)
            elif self._last_rest_weight_1m >= _REST_WEIGHT_SOFT_LIMIT:
                LOG.info(
                    "binance rest weight elevated | used_weight_1m=%s - applying 1s pacing",
                    self._last_rest_weight_1m,
                )
                self._set_rate_limit_pause(1.0)

    async def _call_rest(self, operation: str, func: Any, /, **kwargs: Any) -> Any:
        # Circuit breaker check
        if self._is_circuit_open(operation):
            raise MarketDataUnavailable(
                operation=operation,
                detail=f"circuit breaker open for {self._circuit_open_duration_seconds}s",
                symbol=kwargs.get("symbol"),
            )

        # /futures/data/* request-based limiter (independent of weight)
        if operation in _FUTURES_DATA_REQUEST_LIMITED_OPS:
            await self._futures_data_limiter.acquire(label=operation)

        pause_remaining = self._rate_limit_pause_until - time.monotonic()
        if pause_remaining > 0:
            LOG.debug(
                "rate-limit backoff | sleeping=%.1fs operation=%s",
                pause_remaining,
                operation,
            )
            await asyncio.sleep(pause_remaining)

        # Pre-flight weight guard: if adding this request would breach the soft
        # limit, sleep out the remainder of the current 60-second window first.
        now = time.monotonic()
        if now - self._weight_window_start >= 60.0:
            self._weight_window_weight = 0
            self._weight_window_start = now
        estimated = self._estimate_weight(operation)
        if self._weight_window_weight + estimated >= _REST_WEIGHT_SOFT_LIMIT:
            wait_secs = max(0.0, 60.0 - (now - self._weight_window_start)) + 1.0
            LOG.warning(
                "pre-flight weight guard | estimated_1m=%d threshold=%d sleeping=%.1fs operation=%s",
                self._weight_window_weight + estimated,
                _REST_WEIGHT_SOFT_LIMIT,
                wait_secs,
                operation,
            )
            await asyncio.sleep(wait_secs)
            self._weight_window_weight = 0
            self._weight_window_start = time.monotonic()

        try:
            async with _REST_GLOBAL_SEMAPHORE:
                result = await asyncio.wait_for(
                    func(**kwargs),
                    timeout=self._rest_timeout,
                )
            self._rate_limit_error_streak = 0
            self._capture_response_metadata(result, operation=operation)
            self._track_weight(operation)
            self._record_circuit_success(operation)
            return result
        except (asyncio.TimeoutError, TimeoutError, asyncio.CancelledError) as exc:
            if isinstance(exc, asyncio.CancelledError):
                task = asyncio.current_task()
                if task is None or task.cancelled():
                    raise
            symbol = kwargs.get("symbol")
            self._record_circuit_failure(operation)
            LOG.warning(
                "rest timeout | operation=%s symbol=%s timeout=%.1fs",
                operation,
                symbol,
                self._rest_timeout,
            )
            raise MarketDataUnavailable(
                operation=operation,
                detail=f"timeout after {self._rest_timeout}s",
                symbol=str(symbol) if symbol is not None else None,
            ) from exc
        except BinanceNetworkError as exc:
            symbol = kwargs.get("symbol")
            status_code = getattr(exc, "status_code", None) or getattr(exc, "status", None)
            headers = getattr(exc, "headers", None)
            if status_code == 418:
                # IP banned — enforce 30-minute minimum pause regardless of Retry-After header
                self._rate_limit_error_streak += 1
                self._capture_retry_after(headers)  # apply header value if present
                self._set_rate_limit_pause(1800)    # 30-min minimum always wins via max()
                LOG.critical(
                    "BINANCE IP BAN (418) | pause=1800s+ streak=%d operation=%s — "
                    "bot will pause until ban lifts",
                    self._rate_limit_error_streak,
                    operation,
                )
            elif status_code == 429:
                self._rate_limit_error_streak += 1
                retry_after = self._capture_retry_after(headers)
                if retry_after is None:
                    retry_after = int(
                        self._calculate_backoff(
                            self._rate_limit_error_streak - 1,
                            base_delay=2.0,
                            cap=120.0,
                        )
                    )
                    self._set_rate_limit_pause(retry_after)
                LOG.warning(
                    "binance rate limited (429) | retry_after=%ss streak=%d operation=%s",
                    retry_after,
                    self._rate_limit_error_streak,
                    operation,
                )
            else:
                self._rate_limit_error_streak = 0
                self._record_circuit_failure(operation)
            raise MarketDataUnavailable(
                operation=operation,
                detail=str(exc),
                symbol=str(symbol) if symbol is not None else None,
            ) from exc

    async def _call_public_http_json(
        self,
        operation: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        symbol: str | None = None,
    ) -> Any:
        """Call a public REST endpoint via aiohttp with the same circuit/rate-limit guards.

        Used for foundational endpoints where third-party SDK behavior is less predictable
        under cancellation / shutdown (stability-first path).
        """
        if self._is_circuit_open(operation):
            raise MarketDataUnavailable(
                operation=operation,
                detail=f"circuit breaker open for {self._circuit_open_duration_seconds}s",
                symbol=symbol,
            )

        pause_remaining = self._rate_limit_pause_until - time.monotonic()
        if pause_remaining > 0:
            LOG.debug(
                "rate-limit backoff | sleeping=%.1fs operation=%s",
                pause_remaining,
                operation,
            )
            await asyncio.sleep(pause_remaining)

        now = time.monotonic()
        if now - self._weight_window_start >= 60.0:
            self._weight_window_weight = 0
            self._weight_window_start = now
        estimated = self._estimate_weight(operation)
        if self._weight_window_weight + estimated >= _REST_WEIGHT_SOFT_LIMIT:
            wait_secs = max(0.0, 60.0 - (now - self._weight_window_start)) + 1.0
            LOG.warning(
                "pre-flight weight guard | estimated_1m=%d threshold=%d sleeping=%.1fs operation=%s",
                self._weight_window_weight + estimated,
                _REST_WEIGHT_SOFT_LIMIT,
                wait_secs,
                operation,
            )
            await asyncio.sleep(wait_secs)
            self._weight_window_weight = 0
            self._weight_window_start = time.monotonic()

        class _ResponseStub:
            __slots__ = ("headers",)

            def __init__(self, headers: Mapping[str, str]) -> None:
                self.headers = headers

        try:
            async with _REST_GLOBAL_SEMAPHORE:
                session = await self._get_http_session()
                async with session.get(url, params=params) as response:
                    headers = response.headers
                    status = int(response.status)
                    if status == 418:
                        self._rate_limit_error_streak += 1
                        retry_after = self._capture_retry_after(headers)
                        self._set_rate_limit_pause(1800.0)
                        LOG.critical(
                            "BINANCE IP BAN (418) | retry_after=%s pause=1800s+ streak=%d operation=%s",
                            retry_after,
                            self._rate_limit_error_streak,
                            operation,
                        )
                        self._record_circuit_failure(operation)
                        raise MarketDataUnavailable(operation=operation, detail="418 ip ban", symbol=symbol)
                    if status == 429:
                        self._rate_limit_error_streak += 1
                        retry_after = self._capture_retry_after(headers)
                        if retry_after is None:
                            retry_after = int(
                                self._calculate_backoff(
                                    self._rate_limit_error_streak - 1,
                                    base_delay=2.0,
                                    cap=120.0,
                                )
                            )
                        self._set_rate_limit_pause(float(retry_after))
                        LOG.warning(
                            "binance rate limited (429) | retry_after=%ss streak=%d operation=%s",
                            retry_after,
                            self._rate_limit_error_streak,
                            operation,
                        )
                        self._record_circuit_failure(operation)
                        raise MarketDataUnavailable(operation=operation, detail="429 rate limited", symbol=symbol)
                    if status < 200 or status >= 300:
                        text = await response.text()
                        detail = (text[:240].replace("\n", " ") if text else f"http={status}")
                        self._rate_limit_error_streak = 0
                        self._record_circuit_failure(operation)
                        raise MarketDataUnavailable(operation=operation, detail=detail, symbol=symbol)

                    payload = await response.json()

            self._rate_limit_error_streak = 0
            self._capture_response_metadata(_ResponseStub(headers), operation=operation)
            self._track_weight(operation)
            self._record_circuit_success(operation)
            return payload
        except (asyncio.TimeoutError, TimeoutError, asyncio.CancelledError) as exc:
            if isinstance(exc, asyncio.CancelledError):
                task = asyncio.current_task()
                if task is None or task.cancelled():
                    raise
            self._record_circuit_failure(operation)
            LOG.warning(
                "rest timeout | operation=%s symbol=%s timeout=%.1fs",
                operation,
                symbol,
                self._rest_timeout,
            )
            raise MarketDataUnavailable(
                operation=operation,
                detail=f"timeout after {self._rest_timeout}s",
                symbol=symbol,
            ) from exc
        except aiohttp.ClientError as exc:
            self._record_circuit_failure(operation)
            raise MarketDataUnavailable(
                operation=operation,
                detail=f"aiohttp:{exc.__class__.__name__}:{exc}",
                symbol=symbol,
            ) from exc

    async def _get_http_session(self) -> aiohttp.ClientSession:
        session = self._http_session
        if session is None or session.closed:
            timeout = aiohttp.ClientTimeout(total=self._rest_timeout)
            self._http_session = aiohttp.ClientSession(timeout=timeout)
        return cast(aiohttp.ClientSession, self._http_session)

    async def close(self) -> None:
        """Close AsyncClient session."""
        if self.client:
            await self.client.close_connection()
        if self._http_session is not None and not self._http_session.closed:
            await self._http_session.close()
            self._http_session = None

    def state_snapshot(self) -> dict[str, float | int | None]:
        now = time.monotonic()
        open_circuits = sum(1 for v in self._circuit_open_until.values() if now < v)
        return {
            "rest_weight_1m": self._last_rest_weight_1m,
            "rest_response_time_ms": self._last_rest_response_time_ms,
            "circuit_breakers_open": open_circuits,
            "circuit_failure_counts": sum(self._circuit_failures.values()),
        }

    async def preflight_check(self) -> None:
        await self.fetch_exchange_symbols()
        await self.fetch_ticker_24h()

    async def fetch_exchange_symbols(self) -> list[SymbolMeta]:
        now = time.monotonic()
        if self._exchange_info_cache is not None:
            cached_at, rows = self._exchange_info_cache
            if now - cached_at < 3600:
                return rows

        payload = await self._call_public_http_json(
            "exchange_information",
            f"{_FAPI_BASE_URL}/fapi/v1/exchangeInfo",
        )
        symbols = payload.get('symbols', []) if isinstance(payload, dict) else getattr(payload, 'symbols', [])
        rows = [
            SymbolMeta(
                symbol=str(item.get('symbol', '')) if isinstance(item, dict) else str(getattr(item, 'symbol', '')),
                base_asset=str(item.get('baseAsset', '')) if isinstance(item, dict) else str(getattr(item, 'base_asset', '')),
                quote_asset=str(item.get('quoteAsset', '')) if isinstance(item, dict) else str(getattr(item, 'quote_asset', '')),
                contract_type=str(item.get('contractType', '')) if isinstance(item, dict) else str(getattr(item, 'contract_type', '')),
                status=str(item.get('status', '')) if isinstance(item, dict) else str(getattr(item, 'status', '')),
                onboard_date_ms=int(item.get('onboardDate', 0) or 0) if isinstance(item, dict) else int(getattr(item, 'onboard_date', 0) or 0),
            )
            for item in symbols
        ]
        self._exchange_info_cache = (now, rows)
        return rows

    async def fetch_ticker_24h(self) -> list[dict[str, float | str]]:
        now = time.monotonic()
        if self._ticker_24h_cache is not None:
            cached_at, rows = self._ticker_24h_cache
            if now - cached_at < 300:  # 5 min cache
                return rows

        payload = await self._call_public_http_json(
            "ticker24hr_price_change_statistics",
            f"{_FAPI_BASE_URL}/fapi/v1/ticker/24hr",
        )
        rows: list[dict[str, float | str]] = []
        for item in payload if isinstance(payload, list) else []:
            # Handle both dict and object items
            if isinstance(item, dict):
                rows.append(
                    {
                        "symbol": str(item.get('symbol', '')),
                        "last_price": float(item.get('lastPrice', 0) or item.get('last_price', 0)),
                        "price_change_percent": float(item.get('priceChangePercent', 0) or item.get('price_change_percent', 0)),
                        "quote_volume": float(item.get('quoteVolume', 0) or item.get('quote_volume', 0)),
                    }
                )
            else:
                rows.append(
                    {
                        "symbol": str(getattr(item, 'symbol', '')),
                        "last_price": float(getattr(item, 'last_price', 0) or getattr(item, 'lastPrice', 0)),
                        "price_change_percent": float(getattr(item, 'price_change_percent', 0) or getattr(item, 'priceChangePercent', 0)),
                        "quote_volume": float(getattr(item, 'quote_volume', 0) or getattr(item, 'quoteVolume', 0)),
                    }
                )
        self._ticker_24h_cache = (now, rows)
        return rows

    async def _fetch_symbol_frames_rest(self, symbol: str) -> SymbolFrames:
        frame_4h, frame_1h, frame_15m, frame_5m, book_ticker = await asyncio.gather(
            self.fetch_klines_cached(symbol, "4h", limit=240),
            self.fetch_klines_cached(symbol, "1h", limit=240),
            self.fetch_klines_cached(symbol, "15m", limit=240),
            self.fetch_klines_cached(symbol, "5m", limit=240),
            self._fetch_book_ticker_rest(symbol),
        )
        bid, ask = book_ticker
        return SymbolFrames(
            symbol=symbol,
            df_1h=frame_1h,
            df_15m=frame_15m,
            bid_price=bid,
            ask_price=ask,
            df_5m=frame_5m,
            df_4h=frame_4h,
        )

    async def fetch_klines(self, symbol: str, interval: str, *, limit: int) -> pl.DataFrame:
        response = await self._call_rest(
            f"kline_candlestick_data:{interval}",
            self.client.futures_klines,
            symbol=symbol,
            interval=interval,
            limit=limit,
        )
        # Handle both dict and object responses
        if isinstance(response, dict):
            rows = response
        elif hasattr(response, 'data'):
            rows = response.data() if callable(response.data) else response.data
        else:
            rows = response
        frame = _drop_incomplete_ohlcv_tail(_klines_to_frame(rows), interval)
        return frame

    async def fetch_klines_cached(self, symbol: str, interval: str, *, limit: int) -> pl.DataFrame:
        """Fetch klines with a TTL cache to prevent REST stampedes."""
        key = (symbol, interval, int(limit))
        ttl = int(_CACHE_TTL.get(f"klines_{interval}", 60))
        now = time.monotonic()
        cached = self._klines_cache.get(key)
        if cached is not None and (now - cached[0]) < ttl:
            return cached[1]
        lock = self._klines_locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._klines_locks[key] = lock
        async with lock:
            now = time.monotonic()
            cached = self._klines_cache.get(key)
            if cached is not None and (now - cached[0]) < ttl:
                return cached[1]
            frame = await self.fetch_klines(symbol, interval, limit=limit)
            self._klines_cache[key] = (time.monotonic(), frame)
            return frame

    async def _fetch_book_ticker_rest(self, symbol: str) -> tuple[float | None, float | None]:
        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            try:
                response = await self._call_rest(
                    "symbol_order_book_ticker",
                    self.client.futures_orderbook_ticker,
                    symbol=symbol,
                )
                # Handle both dict and object responses
                if isinstance(response, dict):
                    payload = response
                elif hasattr(response, 'data'):
                    raw = response.data() if callable(response.data) else response.data
                    payload = _unwrap_model(raw)
                else:
                    payload = _unwrap_model(response)
                if isinstance(payload, Mapping):
                    bid_raw = payload.get("bidPrice") or payload.get("bid_price")
                    ask_raw = payload.get("askPrice") or payload.get("ask_price")
                else:
                    bid_raw = getattr(payload, "bid_price", None)
                    ask_raw = getattr(payload, "ask_price", None)
                bid = float(bid_raw) if bid_raw is not None else None
                ask = float(ask_raw) if ask_raw is not None else None
                return bid, ask
            except MarketDataUnavailable as exc:
                detail = (exc.detail or "").lower()
                if attempt < max_attempts and "timeout" in detail:
                    backoff = min(2.0, 0.5 * (2 ** (attempt - 1))) * random.uniform(0.9, 1.1)
                    LOG.warning(
                        "book ticker retry | symbol=%s attempt=%d/%d backoff=%.2fs detail=%s",
                        symbol,
                        attempt,
                        max_attempts,
                        backoff,
                        detail,
                    )
                    await asyncio.sleep(backoff)
                    continue
                LOG.warning(
                    "book ticker unavailable, returning None prices | symbol=%s detail=%s",
                    symbol,
                    detail,
                )
                return None, None
        return None, None

    async def _fetch_agg_trade_snapshot_rest(self, symbol: str, *, limit: int = 100) -> AggTradeSnapshot:
        response = await self._call_rest(
            "compressed_aggregate_trades_list:snapshot",
            self.client.futures_aggregate_trades,
            symbol=symbol,
            limit=limit,
        )
        # Handle both dict and object responses
        if isinstance(response, dict):
            payload = response
        elif hasattr(response, 'data'):
            payload = response.data() if callable(response.data) else response.data
        else:
            payload = response
        buy_qty = 0.0
        sell_qty = 0.0
        trade_count = 0
        payload_rows = cast(list[Any], payload)
        for item in payload_rows:
            row = _coerce_rest_row(item)
            qty = float(row.get("q") or 0.0)
            is_buyer_maker = bool(row.get("m"))
            trade_count += 1
            if is_buyer_maker:
                sell_qty += qty
            else:
                buy_qty += qty
        total_qty = buy_qty + sell_qty
        delta_ratio = None
        if total_qty > 0:
            delta_ratio = (buy_qty - sell_qty) / total_qty
        return AggTradeSnapshot(
            symbol=symbol,
            trade_count=trade_count,
            buy_qty=buy_qty,
            sell_qty=sell_qty,
            delta_ratio=delta_ratio,
        )

    async def fetch_symbol_frames(self, symbol: str) -> SymbolFrames:
        if self._ws is not None and self._ws.is_warm(symbol):
            frames = await self._ws.get_symbol_frames(symbol)
            if frames is not None:
                return frames
        return await self._fetch_symbol_frames_rest(symbol)

    async def fetch_book_ticker(self, symbol: str) -> tuple[float | None, float | None]:
        if self._ws is not None:
            cached = await self._ws.get_book_ticker(symbol)
            if cached is not None:
                return cached
        return await self._fetch_book_ticker_rest(symbol)

    async def fetch_agg_trade_snapshot(self, symbol: str, *, limit: int = 100) -> AggTradeSnapshot:
        if self._ws is not None:
            snapshot = self._ws.get_agg_trade_snapshot(symbol)
            if snapshot is not None:
                return snapshot
        return await self._fetch_agg_trade_snapshot_rest(symbol, limit=limit)

    async def fetch_agg_trades(
        self,
        symbol: str,
        *,
        start_time_ms: int,
        end_time_ms: int,
        page_limit: int,
        page_size: int,
    ) -> tuple[list[AggTrade], bool]:
        rows: list[AggTrade] = []
        pages = 0
        complete = True
        window_start_ms = max(int(start_time_ms), 0)
        final_end_ms = max(int(end_time_ms), 0)
        max_window_ms = 3_599_000  # Binance requires start/end span < 1 hour
        while pages < page_limit and window_start_ms <= final_end_ms:
            window_end_ms = min(window_start_ms + max_window_ms, final_end_ms)
            next_from_id: int | None = None
            while pages < page_limit:
                kwargs: dict[str, Any] = {"symbol": symbol, "limit": page_size}
                if next_from_id is None:
                    kwargs["start_time"] = window_start_ms
                    kwargs["end_time"] = window_end_ms
                else:
                    kwargs["from_id"] = next_from_id
                response = await self._call_rest(
                    "compressed_aggregate_trades_list:history",
                    self.client.futures_aggregate_trades,
                    **kwargs,
                )
                # Handle both dict and object responses
                if isinstance(response, dict):
                    payload = response
                elif hasattr(response, 'data'):
                    payload = response.data() if callable(response.data) else response.data
                else:
                    payload = response
                batch: list[AggTrade] = []
                payload_rows = cast(list[Any], payload)
                for item in payload_rows:
                    row = _coerce_rest_row(item)
                    trade_time_ms = int(row.get("T") or 0)
                    if trade_time_ms < start_time_ms:
                        continue
                    if trade_time_ms > end_time_ms:
                        continue
                    trade_id = int(row.get("a") or 0)
                    batch.append(
                        AggTrade(
                            symbol=symbol,
                            trade_id=trade_id,
                            price=float(row.get("p") or 0.0),
                            quantity=float(row.get("q") or 0.0),
                            trade_time_ms=trade_time_ms,
                            is_buyer_maker=bool(row.get("m")),
                        )
                    )
                if not payload_rows:
                    break
                rows.extend(batch)
                pages += 1
                last_row = _coerce_rest_row(payload_rows[-1])
                next_from_id = int(last_row.get("a") or 0) + 1
                last_time_ms = int(last_row.get("T") or 0)
                if len(payload_rows) < page_size or last_time_ms >= window_end_ms:
                    break
                await asyncio.sleep(0.05)
            if pages >= page_limit and window_end_ms < final_end_ms:
                complete = False
                break
            window_start_ms = window_end_ms + 1
        deduped: dict[int, AggTrade] = {}
        for item in rows:
            deduped[item.trade_id] = item
        sorted_rows = sorted(deduped.values(), key=lambda item: (item.trade_time_ms, item.trade_id))
        if sorted_rows and sorted_rows[-1].trade_time_ms < end_time_ms and pages >= page_limit:
            complete = False
        return sorted_rows, complete

    async def fetch_funding_rate(self, symbol: str) -> float | None:
        now = time.monotonic()
        cached = self._funding_rate_cache.get(symbol)
        if cached is not None and now - cached[0] < 300:
            return cached[1]

        try:
            session = await self._get_http_session()
            async with session.get(
                "https://fapi.binance.com/fapi/v1/premiumIndex",
                params={"symbol": symbol},
            ) as response:
                response.raise_for_status()
                self._track_weight("premium_index")
                payload = await response.json()
        except asyncio.TimeoutError as exc:
            raise MarketDataUnavailable(
                operation="premium_index",
                detail=f"timeout after {self._rest_timeout}s",
                symbol=symbol,
            ) from exc
        except (aiohttp.ClientError, ValueError) as exc:
            raise MarketDataUnavailable(
                operation="premium_index",
                detail=str(exc),
                symbol=symbol,
            ) from exc

        value_raw = payload.get("lastFundingRate") or payload.get("last_funding_rate")
        value = float(value_raw) if value_raw is not None else None
        if value is not None:
            self._funding_rate_cache[symbol] = (now, value)
        return value

    async def fetch_open_interest(self, symbol: str) -> float | None:
        now = time.monotonic()
        # Use extended TTL (10 min) for non-critical OI data
        cached = self._open_interest_cache.get(symbol)
        if self._is_cache_valid(cached, _CACHE_TTL["open_interest"]):
            return cached[1] if cached else None

        try:
            response = await self._call_rest(
                "open_interest",
                self.client.futures_open_interest,
                symbol=symbol,
            )
            # Handle both dict and object responses
            if isinstance(response, dict):
                raw_payload = response
            elif hasattr(response, 'data'):
                raw_payload = response.data() if callable(response.data) else response.data
            else:
                raw_payload = response
            payload = _unwrap_model(raw_payload)
            row = payload.model_dump() if hasattr(payload, "model_dump") else dict(payload)
            value_raw = row.get("open_interest") or row.get("openInterest")
            value = float(value_raw) if value_raw is not None else None
            if value is not None:
                self._open_interest_cache[symbol] = (now, value)
            return value
        except MarketDataUnavailable:
            # Graceful degradation: return stale cache if available
            if cached is not None:
                LOG.debug("OI graceful degradation | symbol=%s using stale cache", symbol)
                return cached[1]
            return None

    async def fetch_open_interest_change(self, symbol: str, *, period: str = "1h") -> float | None:
        cache_key = (symbol, period)
        now = time.monotonic()
        cached = self._open_interest_change_cache.get(cache_key)
        if self._is_cache_valid(cached, _CACHE_TTL["open_interest_change"]):
            return cached[1] if cached else None

        try:
            response = await self._call_rest(
                "open_interest_statistics",
                self.client.futures_open_interest_hist,
                symbol=symbol,
                period=period,
                limit=2,
            )
            # Handle both dict and object responses
            if isinstance(response, dict):
                raw_payload = response
            elif hasattr(response, 'data'):
                raw_payload = response.data() if callable(response.data) else response.data
            else:
                raw_payload = response
            payload = _unwrap_model(raw_payload)
            if not payload:
                return None
            rows = [
                item.model_dump() if hasattr(item, "model_dump") else dict(item)
                for item in payload
            ]
            rows.sort(key=lambda row: int(row.get("timestamp") or 0))
            if len(rows) < 2:
                return None
            prev_raw = rows[-2].get("sumOpenInterest") or rows[-2].get("sum_open_interest")
            curr_raw = rows[-1].get("sumOpenInterest") or rows[-1].get("sum_open_interest")
            prev = float(prev_raw) if prev_raw is not None else 0.0
            curr = float(curr_raw) if curr_raw is not None else 0.0
            if prev <= 0.0:
                return None
            change = (curr / prev) - 1.0
            self._open_interest_change_cache[cache_key] = (now, change)
            return change
        except MarketDataUnavailable:
            # Graceful degradation: return stale cache if available
            if cached is not None:
                LOG.debug("OI change graceful degradation | symbol=%s period=%s using stale cache", symbol, period)
                return cached[1]
            return None

    async def fetch_long_short_ratio(self, symbol: str, *, period: str = "1h") -> float | None:
        cache_key = (symbol, period)
        now = time.monotonic()
        cached = self._long_short_ratio_cache.get(cache_key)
        if self._is_cache_valid(cached, _CACHE_TTL["long_short_ratio"]):
            return cached[1] if cached else None

        try:
            response = await self._call_rest(
                "top_trader_long_short_ratio_accounts",
                self.client.futures_top_longshort_account_ratio,
                symbol=symbol,
                period=period,
                limit=1,
            )
            # Handle both dict and object responses
            if isinstance(response, dict):
                raw_payload = response
            elif hasattr(response, 'data'):
                raw_payload = response.data() if callable(response.data) else response.data
            else:
                raw_payload = response
            payload = _unwrap_model(raw_payload)
            if not payload:
                return None
            item = payload[0]
            row = item.model_dump() if hasattr(item, "model_dump") else dict(item)
            value_raw = row.get("longShortRatio") or row.get("long_short_ratio")
            value = float(value_raw) if value_raw is not None else None
            if value is not None:
                self._long_short_ratio_cache[cache_key] = (now, value)
            return value
        except MarketDataUnavailable:
            # Graceful degradation: return stale cache if available
            if cached is not None:
                LOG.debug("L/S ratio graceful degradation | symbol=%s period=%s using stale cache", symbol, period)
                return cached[1]
            return None

    # ------------------------------------------------------------------
    # Cache accessors — return cached values without making REST calls.
    # Used by the OI refresh background task to feed pre-fetched data
    # into the pipeline without adding per-event REST latency.
    # ------------------------------------------------------------------

    def get_cached_oi_change(self, symbol: str, period: str = "1h", max_age_s: float = 1800.0) -> float | None:
        """Return cached OI change pct if fresh, else None (no REST call)."""
        cached = self._open_interest_change_cache.get((symbol, period))
        if cached is None:
            return None
        cached_at, value = cached
        if time.monotonic() - cached_at > max_age_s:
            return None
        return value

    def get_cached_ls_ratio(self, symbol: str, period: str = "1h", max_age_s: float = 1800.0) -> float | None:
        """Return cached L/S ratio if fresh, else None (no REST call)."""
        cached = self._long_short_ratio_cache.get((symbol, period))
        if cached is None:
            return None
        cached_at, value = cached
        if time.monotonic() - cached_at > max_age_s:
            return None
        return value

    async def fetch_taker_ratio(self, symbol: str, *, period: str = "1h") -> float | None:
        """Fetch taker buy/sell volume ratio from /futures/data/takerlongshortRatio.

        Returns ratio > 1.0 means takers are net buyers (bullish aggression).
        Returns ratio < 1.0 means takers are net sellers (bearish aggression).
        Cached for 1200 seconds (matches OI refresh interval).
        """
        cache_key = (symbol, period)
        now = time.monotonic()
        cached = self._taker_ratio_cache.get(cache_key)
        if cached is not None and now - cached[0] < 1200:
            return cached[1]

        await self._futures_data_limiter.acquire(label="futures_data:takerlongshortRatio")
        try:
            session = await self._get_http_session()
            async with session.get(
                "https://fapi.binance.com/futures/data/takerlongshortRatio",
                params={"symbol": symbol, "period": period, "limit": 1},
            ) as response:
                response.raise_for_status()
                self._track_weight("taker_long_short_ratio")
                payload = await response.json()
        except asyncio.TimeoutError:
            return None
        except (aiohttp.ClientError, ValueError):
            return None

        if not payload:
            return None
        item = payload[0] if isinstance(payload, list) else payload
        raw = item.get("buySellRatio") or item.get("buy_sell_ratio")
        value = float(raw) if raw is not None else None
        if value is not None:
            self._taker_ratio_cache[cache_key] = (now, value)
        return value

    async def fetch_global_ls_ratio(self, symbol: str, *, period: str = "1h") -> float | None:
        """Fetch global long/short account ratio from /futures/data/globalLongShortAccountRatio.

        Unlike topLongShortAccountRatio (top traders only), this covers all accounts.
        ls_ratio > 1.0 means more accounts are long than short.
        Cached for 1200 seconds (matches OI refresh interval).
        """
        cache_key = (symbol, period)
        now = time.monotonic()
        cached = self._global_ls_ratio_cache.get(cache_key)
        if cached is not None and now - cached[0] < 1200:
            return cached[1]

        await self._futures_data_limiter.acquire(label="futures_data:globalLongShortAccountRatio")
        try:
            session = await self._get_http_session()
            async with session.get(
                "https://fapi.binance.com/futures/data/globalLongShortAccountRatio",
                params={"symbol": symbol, "period": period, "limit": 1},
            ) as response:
                response.raise_for_status()
                self._track_weight("global_long_short_account_ratio")
                payload = await response.json()
        except asyncio.TimeoutError:
            return None
        except (aiohttp.ClientError, ValueError):
            return None

        if not payload:
            return None
        item = payload[0] if isinstance(payload, list) else payload
        raw = item.get("longShortRatio") or item.get("long_short_ratio")
        value = float(raw) if raw is not None else None
        if value is not None:
            self._global_ls_ratio_cache[cache_key] = (now, value)
        return value

    def get_cached_taker_ratio(self, symbol: str, period: str = "1h", max_age_s: float = 1800.0) -> float | None:
        """Return cached taker buy/sell ratio if fresh, else None (no REST call)."""
        cached = self._taker_ratio_cache.get((symbol, period))
        if cached is None:
            return None
        cached_at, value = cached
        if time.monotonic() - cached_at > max_age_s:
            return None
        return value

    def get_cached_global_ls_ratio(self, symbol: str, period: str = "1h", max_age_s: float = 1800.0) -> float | None:
        """Return cached global L/S ratio if fresh, else None (no REST call)."""
        cached = self._global_ls_ratio_cache.get((symbol, period))
        if cached is None:
            return None
        cached_at, value = cached
        if time.monotonic() - cached_at > max_age_s:
            return None
        return value

    async def fetch_funding_rate_history(self, symbol: str, *, limit: int = 10) -> list[dict]:
        """Fetch last `limit` funding rate records from /fapi/v1/fundingRate.

        Returns list of {fundingTime: int ms, fundingRate: float, markPrice: float},
        sorted oldest-to-newest. Cached for 900 seconds.
        """
        now = time.monotonic()
        cached = self._funding_history_cache.get(symbol)
        if cached is not None and now - cached[0] < 900:
            return cached[1]

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    "https://fapi.binance.com/fapi/v1/fundingRate",
                    params={"symbol": symbol, "limit": limit},
                    timeout=aiohttp.ClientTimeout(total=self._rest_timeout),
                ) as response:
                    response.raise_for_status()
                    self._track_weight("funding_rate_history")
                    payload = await response.json()
        except asyncio.TimeoutError:
            return []
        except (aiohttp.ClientError, ValueError):
            return []

        if not isinstance(payload, list):
            return []

        rows = []
        for item in payload:
            try:
                rows.append({
                    "fundingTime": int(item.get("fundingTime") or 0),
                    "fundingRate": float(item.get("fundingRate") or 0.0),
                    "markPrice": float(item.get("markPrice") or 0.0),
                })
            except (TypeError, ValueError):
                continue

        rows.sort(key=lambda r: r["fundingTime"])
        self._funding_history_cache[symbol] = (now, rows)
        return rows

    def get_cached_funding_trend(self, symbol: str, max_age_s: float = 1800.0) -> str | None:
        """Derive funding trend from cached history — no REST call.

        Returns "rising", "falling", "flat", or None if no data.
        "rising"  = last 3+ records trending higher (crowd building longs)
        "falling" = last 3+ records trending lower
        "flat"    = no clear direction
        """
        cached = self._funding_history_cache.get(symbol)
        if cached is None:
            return None
        cached_at, rows = cached
        if time.monotonic() - cached_at > max_age_s:
            return None
        if len(rows) < 3:
            return None
        recent = [r["fundingRate"] for r in rows[-4:]]
        # Count directional steps
        ups = sum(1 for i in range(1, len(recent)) if recent[i] > recent[i - 1])
        downs = sum(1 for i in range(1, len(recent)) if recent[i] < recent[i - 1])
        steps = len(recent) - 1
        if ups >= steps * 0.75:
            return "rising"
        if downs >= steps * 0.75:
            return "falling"
        return "flat"

    async def fetch_basis(self, symbol: str, *, period: str = "1h", limit: int = 3) -> float | None:
        """Fetch most recent basis (futures - index price as %) from /futures/data/basis.

        Returns basis as a percentage (positive = contango, negative = backwardation).
        Cached for 900 seconds.
        """
        cache_key = (symbol, period)
        now = time.monotonic()
        cached = self._basis_cache.get(cache_key)
        if cached is not None and now - cached[0] < 900:
            return cached[1]

        await self._futures_data_limiter.acquire(label="futures_data:basis")
        try:
            session = await self._get_http_session()
            async with session.get(
                "https://fapi.binance.com/futures/data/basis",
                params={"pair": symbol, "contractType": "PERPETUAL", "period": period, "limit": limit},
            ) as response:
                response.raise_for_status()
                self._track_weight("basis")
                payload = await response.json()
        except asyncio.TimeoutError:
            return None
        except (aiohttp.ClientError, ValueError):
            return None

        if not isinstance(payload, list) or not payload:
            return None

        # Sort by timestamp, take the most recent
        payload.sort(key=lambda r: int(r.get("timestamp") or 0))
        basis_series: list[float] = []
        for row in payload:
            try:
                futures_price = float(row.get("futuresPrice") or 0.0)
                index_price = float(row.get("indexPrice") or 0.0)
            except (TypeError, ValueError):
                continue
            if index_price <= 0.0:
                continue
            basis_series.append((futures_price - index_price) / index_price * 100.0)
        if not basis_series:
            return None
        basis_pct = basis_series[-1]
        premium_slope = None
        if len(basis_series) >= 2:
            premium_slope = basis_series[-1] - basis_series[-2]
        premium_zscore = None
        if len(basis_series) >= 3:
            mean = sum(basis_series) / len(basis_series)
            variance = sum((value - mean) ** 2 for value in basis_series) / len(basis_series)
            std = math.sqrt(variance)
            if std > 0.0:
                premium_zscore = (basis_series[-1] - mean) / std

        self._basis_cache[cache_key] = (now, basis_pct)
        self._basis_stats_cache[cache_key] = (
            now,
            {
                "latest_basis_pct": basis_pct,
                "premium_slope_5m": premium_slope,
                "premium_zscore_5m": premium_zscore,
                "mark_index_spread_bps": basis_pct * 100.0,
            },
        )
        return basis_pct

    def get_cached_basis(self, symbol: str, period: str = "1h", max_age_s: float = 1800.0) -> float | None:
        """Return cached basis pct if fresh, else None (no REST call)."""
        cached = self._basis_cache.get((symbol, period))
        if cached is None:
            return None
        cached_at, value = cached
        if time.monotonic() - cached_at > max_age_s:
            return None
        return value

    def get_cached_basis_stats(
        self,
        symbol: str,
        period: str = "1h",
        max_age_s: float = 1800.0,
    ) -> dict[str, float | None] | None:
        cached = self._basis_stats_cache.get((symbol, period))
        if cached is None:
            return None
        cached_at, value = cached
        if time.monotonic() - cached_at > max_age_s:
            return None
        return dict(value)

    def update_basis_from_websocket(
        self,
        symbol: str,
        mark_price: float,
        index_price: float | None = None,
        period: str = "5m",
    ) -> dict[str, float | None] | None:
        """Update basis cache from WebSocket mark price data (zero I/O).

        If index_price is None, uses mark_price as fallback (spread = 0).
        Returns calculated stats dict or None if inputs invalid.
        """
        if mark_price <= 0.0:
            return None

        now = time.monotonic()
        cache_key = (symbol, period)

        if index_price is not None and index_price > 0.0:
            basis_pct = (mark_price - index_price) / index_price * 100.0
            mark_index_spread_bps = basis_pct * 100.0  # Convert to bps
        else:
            # No index price available - use cached basis or mark price
            cached = self._basis_cache.get(cache_key)
            if cached is not None:
                basis_pct = cached[1]  # Use existing basis
            else:
                basis_pct = 0.0
            mark_index_spread_bps = None  # Can't calculate without index

        # Update caches
        self._basis_cache[cache_key] = (now, basis_pct)

        # For stats, we need history for zscore/slope - use simple values
        stats = {
            "latest_basis_pct": basis_pct,
            "premium_slope_5m": None,  # Need history for slope
            "premium_zscore_5m": None,  # Need history for zscore
            "mark_index_spread_bps": mark_index_spread_bps,
        }
        self._basis_stats_cache[cache_key] = (now, stats)

        return stats
