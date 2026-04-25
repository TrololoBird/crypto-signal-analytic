from __future__ import annotations

import argparse
import asyncio
from typing import Sequence

from common import bootstrap_repo_path, configure_script_logging

bootstrap_repo_path()

from bot.config import load_settings
from bot.market_data import BinanceFuturesMarketData, MarketDataUnavailable
from bot.ws_manager import FuturesWSManager


LOG = configure_script_logging("scripts.live_check_binance_api")


async def _run(symbols: Sequence[str], warmup_seconds: float, reconnect_wait_seconds: float) -> None:
    settings = load_settings()
    client = BinanceFuturesMarketData(rest_timeout_seconds=settings.ws.rest_timeout_seconds)
    ws_manager = FuturesWSManager(client, settings.ws)
    try:
        exchange_symbols = await client.fetch_exchange_symbols()
        ticker_rows = await client.fetch_ticker_24h()
        book_bid, book_ask = await client.fetch_book_ticker(symbols[0])
        klines_15m = await client.fetch_klines_cached(symbols[0], "15m", limit=64)
        LOG.info(
            "rest_checks_ok",
            exchange_symbols=len(exchange_symbols),
            ticker_rows=len(ticker_rows),
            symbol=symbols[0],
            bid=book_bid,
            ask=book_ask,
            kline_rows=klines_15m.height,
        )

        await ws_manager.start(list(symbols))
        connected = await ws_manager.wait_until_connected(timeout=30.0)
        if not connected:
            raise RuntimeError("ws_manager failed to connect within 30 seconds")

        await asyncio.sleep(warmup_seconds)
        before = ws_manager.state_snapshot()
        LOG.info("ws_snapshot_before_reconnect", snapshot=before)
        if int(before.get("fresh_tickers") or 0) <= 0:
            raise RuntimeError(f"fresh_tickers did not warm up: {before}")
        if int(before.get("fresh_mark_prices") or 0) <= 0:
            raise RuntimeError(f"fresh_mark_prices did not warm up: {before}")
        if int(before.get("fresh_book_tickers") or 0) <= 0:
            raise RuntimeError(f"fresh_book_tickers did not warm up: {before}")

        market_ws = ws_manager._ws_conns.get("market")
        if market_ws is None:
            raise RuntimeError("market ws connection is missing before forced reconnect")
        await market_ws.close()
        await asyncio.sleep(reconnect_wait_seconds)

        after = ws_manager.state_snapshot()
        LOG.info("ws_snapshot_after_reconnect", snapshot=after)
        if int(after.get("market_connect_count") or 0) < 2:
            raise RuntimeError(f"market reconnect was not observed: {after}")
        if int(after.get("fresh_tickers") or 0) <= 0:
            raise RuntimeError(f"fresh_tickers were not restored after reconnect: {after}")
        if int(after.get("fresh_mark_prices") or 0) <= 0:
            raise RuntimeError(f"fresh_mark_prices were not restored after reconnect: {after}")
    finally:
        await ws_manager.stop()
        await client.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Live REST/WS Binance API smoke check")
    parser.add_argument("--symbols", nargs="+", default=["BTCUSDT", "ETHUSDT", "SOLUSDT"])
    parser.add_argument("--warmup-seconds", type=float, default=20.0)
    parser.add_argument("--reconnect-wait-seconds", type=float, default=20.0)
    args = parser.parse_args()
    try:
        asyncio.run(_run(args.symbols, args.warmup_seconds, args.reconnect_wait_seconds))
    except MarketDataUnavailable as exc:
        LOG.error("live_binance_api_unavailable", operation=exc.operation, detail=exc.detail, symbol=exc.symbol)
        raise SystemExit(2) from exc


if __name__ == "__main__":
    main()
