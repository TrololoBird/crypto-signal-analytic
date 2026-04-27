"""Cache and stream-update helpers extracted from ``bot.ws_manager``."""

from __future__ import annotations

import asyncio
import collections
import logging
import time
from datetime import datetime, timezone
from typing import Any

from ..models import AggTrade
from .enrichment import depth_imbalance_from_delta_ratio, microprice_bias_from_book

LOG = logging.getLogger("bot.ws_manager")


def is_ticker_cache_warm(manager: Any) -> bool:
    if not manager._ticker_cache:
        return False
    age = time.monotonic() - manager._ticker_cache_ts
    return age <= manager._cfg.market_ticker_freshness_seconds


def get_stats(manager: Any) -> dict[str, Any]:
    stats: dict[str, Any] = {
        "streams_total": len(manager._intended_streams),
        "streams_active": len(manager._stream_last_message_ts),
        "slow_streams": list(manager._slow_streams),
        "slow_streams_count": len(manager._slow_streams),
        "buffer_stats": manager._message_buffer.get_stats(),
    }
    stream_latencies = {}
    for stream, latencies in manager._stream_latency_ms.items():
        if latencies:
            stream_latencies[stream] = round(sum(latencies) / len(latencies), 2)
    if stream_latencies:
        stats["avg_latency_per_stream"] = stream_latencies
        all_latencies = [sum(v) / len(v) for v in manager._stream_latency_ms.values() if v]
        if all_latencies:
            stats["avg_latency_overall_ms"] = round(sum(all_latencies) / len(all_latencies), 2)
    return stats


def get_global_ticker_data(manager: Any) -> list[dict]:
    result: list[dict] = []
    now = time.monotonic()
    for symbol, ticker in manager._ticker_cache.items():
        last_update = manager._ticker_update_times.get(symbol, 0.0)
        if now - last_update > manager._cfg.market_ticker_freshness_seconds:
            continue
        result.append(
            {
                "symbol": symbol,
                "quote_volume": ticker.get("quote_volume", 0.0),
                "price_change_percent": ticker.get("price_change_percent", 0.0),
                "last_price": ticker.get("last_price", 0.0),
            }
        )
    return result


def get_depth_imbalance(manager: Any, symbol: str) -> float | None:
    snapshot = manager.get_agg_trade_snapshot(symbol)
    if snapshot is not None:
        return depth_imbalance_from_delta_ratio(snapshot.delta_ratio)
    return None


def get_microprice_bias(manager: Any, symbol: str) -> float | None:
    bid, ask = manager.get_book_snapshot(symbol)
    if bid is None or ask is None or bid <= 0 or ask <= 0:
        return None
    snapshot = manager.get_agg_trade_snapshot(symbol)
    return microprice_bias_from_book(
        bid=bid,
        ask=ask,
        delta_ratio=None if snapshot is None else snapshot.delta_ratio,
    )


def get_funding_sentiment(manager: Any) -> float | None:
    rates = [
        value["funding_rate"]
        for value in manager._mark_price_cache.values()
        if value.get("funding_rate") is not None
    ]
    if not rates:
        return None
    return sum(rates) / len(rates)


def get_liquidation_sentiment(
    manager: Any,
    symbol: str | None = None,
    window_seconds: int = 60,
) -> float | None:
    cutoff_ms = int(time.time() * 1000) - window_seconds * 1000
    long_liq = short_liq = 0.0
    for ts_ms, sym, side, qty, _price in manager._force_order_buffer:
        if ts_ms < cutoff_ms:
            continue
        if symbol is not None and sym != symbol:
            continue
        if side == "BUY":
            short_liq += qty
        else:
            long_liq += qty
    total = long_liq + short_liq
    if total == 0.0:
        return None
    return (short_liq - long_liq) / total


def should_throttle_ticker_update(manager: Any, symbol: str) -> bool:
    now = time.monotonic()
    last_update = manager._ticker_update_times.get(symbol, 0.0)
    elapsed_ms = (now - last_update) * 1000
    if elapsed_ms < manager._min_ticker_update_interval_ms:
        last_logged = getattr(manager, "_last_ticker_throttle_log", {}).get(symbol, 0.0)
        if now - last_logged >= 30.0:
            if not hasattr(manager, "_last_ticker_throttle_log"):
                manager._last_ticker_throttle_log = {}
            manager._last_ticker_throttle_log[symbol] = now
            LOG.debug(
                "ticker throttled | symbol=%s elapsed=%.0fms min=%.0fms",
                symbol,
                elapsed_ms,
                manager._min_ticker_update_interval_ms,
            )
        return True
    manager._ticker_update_times[symbol] = now
    return False


def should_throttle_mark_price_update(manager: Any, symbol: str) -> bool:
    now = time.monotonic()
    last_update = manager._mark_price_update_times.get(symbol, 0.0)
    elapsed_ms = (now - last_update) * 1000
    if elapsed_ms < 50.0:
        last_logged = getattr(manager, "_last_markprice_throttle_log", {}).get(symbol, 0.0)
        if now - last_logged >= 30.0:
            if not hasattr(manager, "_last_markprice_throttle_log"):
                manager._last_markprice_throttle_log = {}
            manager._last_markprice_throttle_log[symbol] = now
            LOG.debug("mark_price throttled | symbol=%s elapsed=%.0fms min=50ms", symbol, elapsed_ms)
        return True
    manager._mark_price_update_times[symbol] = now
    return False


def handle_ticker(manager: Any, symbol: str, data: dict) -> None:
    if should_throttle_ticker_update(manager, symbol):
        return
    try:
        manager._ticker_cache[symbol] = {
            "symbol": symbol,
            "last_price": float(data.get("c") or 0.0),
            "quote_volume": float(data.get("q") or 0.0),
            "price_change_percent": float(data.get("P") or 0.0),
            "price_change": float(data.get("p") or 0.0),
            "open_price": float(data.get("o") or 0.0),
            "high_price": float(data.get("h") or 0.0),
            "low_price": float(data.get("l") or 0.0),
        }
        manager._ticker_cache_ts = time.monotonic()
    except (TypeError, ValueError):
        return


def handle_mini_ticker(manager: Any, symbol: str, data: dict) -> None:
    now = time.monotonic()
    last_full_update = manager._ticker_update_times.get(symbol, 0.0)
    if now - last_full_update < manager._cfg.market_ticker_freshness_seconds:
        return
    if should_throttle_ticker_update(manager, symbol):
        return
    try:
        close_price = float(data.get("c") or 0.0)
        open_price = float(data.get("o") or 0.0)
        price_change_pct = ((close_price - open_price) / open_price * 100.0) if open_price > 0 else 0.0
        manager._ticker_cache[symbol] = {
            "symbol": symbol,
            "last_price": close_price,
            "quote_volume": float(data.get("q") or 0.0),
            "price_change_percent": price_change_pct,
            "price_change": close_price - open_price,
            "open_price": open_price,
            "high_price": float(data.get("h") or 0.0),
            "low_price": float(data.get("l") or 0.0),
        }
        manager._ticker_cache_ts = time.monotonic()
    except (TypeError, ValueError):
        return


def handle_mark_price(manager: Any, symbol: str, data: dict) -> None:
    if should_throttle_mark_price_update(manager, symbol):
        return
    try:
        funding_str = data.get("r")
        funding_rate = float(funding_str) if funding_str not in (None, "", "0") else 0.0
        manager._mark_price_cache[symbol] = {
            "symbol": symbol,
            "mark_price": float(data.get("p") or 0.0),
            "index_price": float(data.get("i") or 0.0),
            "funding_rate": funding_rate,
            "next_funding_time_ms": int(data.get("T") or 0),
        }
    except (TypeError, ValueError):
        return


def handle_force_order(manager: Any, data: dict) -> None:
    try:
        order = data.get("o", {})
        symbol = str(order.get("s") or "").upper()
        side = str(order.get("S") or "").upper()
        qty = float(order.get("q") or 0.0)
        price = float(order.get("ap") or order.get("p") or 0.0)
        ts_ms = int(order.get("T") or data.get("E") or (time.time() * 1000))
        if symbol and side in ("BUY", "SELL") and qty > 0:
            manager._force_order_buffer.append((ts_ms, symbol, side, qty, price))
    except (TypeError, ValueError, KeyError):
        return


async def handle_book_ticker(manager: Any, symbol: str, data: dict) -> None:
    if manager._symbols and symbol not in manager._symbols:
        return
    try:
        bid = float(data["b"]) if data.get("b") is not None else None
        ask = float(data["a"]) if data.get("a") is not None else None
        event_ts_ms = int(data["E"]) if data.get("E") is not None else None
    except (KeyError, TypeError, ValueError):
        return

    async with manager._data_lock:
        manager._book[symbol] = (bid, ask)
        manager._book_update_times[symbol] = time.monotonic()

    if manager._event_bus is not None:
        from ..core.events import BookTickerEvent

        manager._event_bus.publish_nowait(
            BookTickerEvent(symbol=symbol, bid=bid, ask=ask, event_ts_ms=event_ts_ms)
        )


async def handle_agg_trade(manager: Any, symbol: str, data: dict) -> None:
    if manager._symbols and symbol not in manager._symbols:
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

    async with manager._data_lock:
        if symbol not in manager._agg_trades:
            manager._agg_trades[symbol] = collections.deque(maxlen=manager._cfg.max_agg_trade_buffer)
        manager._agg_trades[symbol].append(trade)

    if manager._agg_trade_cbs:
        trade_dt = datetime.fromtimestamp(trade.trade_time_ms / 1000.0, tz=timezone.utc)
        for callback in manager._agg_trade_cbs:
            task = asyncio.create_task(callback(symbol, trade.price, trade_dt))
            manager._attach_task_logging(task, label=f"agg_trade:{symbol}")
