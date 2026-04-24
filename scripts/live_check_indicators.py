from __future__ import annotations

import argparse
import asyncio
import json
import logging
from pathlib import Path
from typing import Any

import structlog

from bot.config import load_settings
from bot.features import min_required_bars, prepare_symbol
from bot.market_data import BinanceFuturesMarketData
from bot.models import SymbolFrames, UniverseSymbol


LOG = structlog.get_logger("scripts.live_check_indicators")


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        force=True,
    )


def _symbols_from_run(run_id: str) -> list[str]:
    target = Path("data") / "bot" / "telemetry" / "runs" / run_id / "analysis" / "symbol_analysis.jsonl"
    if not target.exists():
        raise FileNotFoundError(target)
    symbols: list[str] = []
    seen: set[str] = set()
    for line in target.read_text(encoding="utf-8").splitlines():
        row = json.loads(line)
        symbol = str(row.get("symbol") or "").upper()
        if symbol and symbol not in seen:
            seen.add(symbol)
            symbols.append(symbol)
    return symbols


async def _build_universe_symbol(
    symbol: str,
    meta_map: dict[str, Any],
    ticker_map: dict[str, dict[str, Any]],
) -> UniverseSymbol:
    meta = meta_map[symbol]
    ticker = ticker_map.get(symbol, {})
    return UniverseSymbol(
        symbol=symbol,
        base_asset=meta.base_asset,
        quote_asset=meta.quote_asset,
        contract_type=meta.contract_type,
        status=meta.status,
        onboard_date_ms=meta.onboard_date_ms,
        quote_volume=float(ticker.get("quote_volume") or 0.0),
        price_change_pct=float(ticker.get("price_change_percent") or 0.0),
        last_price=float(ticker.get("last_price") or 0.0),
        shortlist_bucket="",
    )


async def _run(symbols: list[str], concurrency: int) -> None:
    settings = load_settings()
    minimums = min_required_bars(
        min_bars_15m=settings.filters.min_bars_15m,
        min_bars_1h=settings.filters.min_bars_1h,
        min_bars_4h=settings.filters.min_bars_4h,
    )
    client = BinanceFuturesMarketData(rest_timeout_seconds=settings.ws.rest_timeout_seconds)
    try:
        exchange_symbols = await client.fetch_exchange_symbols()
        ticker_rows = await client.fetch_ticker_24h()
        meta_map = {row.symbol: row for row in exchange_symbols}
        ticker_map = {
            str(row.get("symbol") or "").upper(): row
            for row in ticker_rows
            if isinstance(row, dict)
        }

        semaphore = asyncio.Semaphore(concurrency)
        failures: list[dict[str, Any]] = []
        successes = 0

        async def _check_symbol(symbol: str) -> None:
            nonlocal successes
            async with semaphore:
                if symbol not in meta_map:
                    failures.append({"symbol": symbol, "stage": "metadata", "error": "missing_symbol_meta"})
                    return
                item = await _build_universe_symbol(symbol, meta_map, ticker_map)
                df_4h = await client.fetch_klines_cached(symbol, "4h", limit=240)
                df_1h = await client.fetch_klines_cached(symbol, "1h", limit=240)
                df_15m = await client.fetch_klines_cached(symbol, "15m", limit=240)
                df_5m = await client.fetch_klines_cached(symbol, "5m", limit=240)
                bid_price, ask_price = await client.fetch_book_ticker(symbol)
                frames = SymbolFrames(
                    symbol=symbol,
                    df_1h=df_1h,
                    df_15m=df_15m,
                    bid_price=bid_price,
                    ask_price=ask_price,
                    df_5m=df_5m,
                    df_4h=df_4h,
                )
                try:
                    prepared = prepare_symbol(
                        item,
                        frames,
                        minimums=minimums,
                        settings=settings,
                    )
                except Exception as exc:
                    failures.append(
                        {
                            "symbol": symbol,
                            "stage": "prepare_symbol",
                            "error": str(exc),
                            "exception_type": type(exc).__name__,
                        }
                    )
                    return
                if prepared is None:
                    failures.append(
                        {
                            "symbol": symbol,
                            "stage": "prepare_symbol",
                            "error": "prepare_symbol returned None",
                        }
                    )
                    return
                successes += 1
                LOG.info(
                    "indicator_check_ok",
                    symbol=symbol,
                    atr_pct=prepared.atr_pct,
                    bias_4h=prepared.bias_4h,
                    bias_1h=prepared.bias_1h,
                    market_regime=prepared.market_regime,
                    work_rows_15m=prepared.work_15m.height,
                    work_rows_1h=prepared.work_1h.height,
                )

        await asyncio.gather(*[asyncio.create_task(_check_symbol(symbol)) for symbol in symbols])
        LOG.info(
            "indicator_check_summary",
            symbols=len(symbols),
            successes=successes,
            failures=len(failures),
        )
        if failures:
            LOG.error("indicator_check_failures", failures=failures[:20])
            raise RuntimeError(f"indicator/prepare checks failed for {len(failures)} symbols")
    finally:
        await client.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Live prepare_symbol + indicator verification")
    parser.add_argument("--symbols", nargs="*", default=[])
    parser.add_argument("--symbols-from-run", default="20260421_215817_70948")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--concurrency", type=int, default=3)
    args = parser.parse_args()

    _configure_logging()
    symbols = [str(symbol).upper() for symbol in args.symbols if str(symbol).strip()]
    if not symbols:
        symbols = _symbols_from_run(args.symbols_from_run)
    if args.limit > 0:
        symbols = symbols[: args.limit]
    asyncio.run(_run(symbols, args.concurrency))


if __name__ == "__main__":
    main()
