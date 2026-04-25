from __future__ import annotations

import argparse
import asyncio
import json
import logging
from collections import Counter
from pathlib import Path
import sys
from typing import Any

import structlog

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from bot.config import load_settings
from bot.core.engine import SignalEngine, StrategyRegistry
from bot.features import min_required_bars, prepare_symbol
from bot.market_data import BinanceFuturesMarketData, MarketDataUnavailable
from bot.models import SymbolFrames, UniverseSymbol
from bot.setup_base import SetupParams
from bot.strategies import STRATEGY_CLASSES


LOG = structlog.get_logger("scripts.live_check_strategies")


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        force=True,
    )


def _symbols_from_run(run_id: str) -> list[str]:
    target = Path("data") / "bot" / "telemetry" / "runs" / run_id / "analysis" / "symbol_analysis.jsonl"
    if not target.exists():
        LOG.warning("symbols_from_run_missing", run_id=run_id, path=str(target))
        return []
    symbols: list[str] = []
    seen: set[str] = set()
    for line in target.read_text(encoding="utf-8").splitlines():
        row = json.loads(line)
        symbol = str(row.get("symbol") or "").upper()
        if symbol and symbol not in seen:
            seen.add(symbol)
            symbols.append(symbol)
    return symbols


async def _build_prepared(
    client: BinanceFuturesMarketData,
    settings: Any,
    minimums: dict[str, int],
    meta_map: dict[str, Any],
    ticker_map: dict[str, dict[str, Any]],
    symbol: str,
):
    meta = meta_map.get(symbol)
    if meta is None:
        return None
    ticker = ticker_map.get(symbol, {})
    item = UniverseSymbol(
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
    frames = SymbolFrames(
        symbol=symbol,
        df_1h=await client.fetch_klines_cached(symbol, "1h", limit=240),
        df_15m=await client.fetch_klines_cached(symbol, "15m", limit=240),
        bid_price=None,
        ask_price=None,
        df_5m=await client.fetch_klines_cached(symbol, "5m", limit=240),
        df_4h=await client.fetch_klines_cached(symbol, "4h", limit=240),
    )
    return prepare_symbol(item, frames, minimums=minimums, settings=settings)


async def _run(symbols: list[str], concurrency: int) -> None:
    settings = load_settings()
    minimums = min_required_bars(
        min_bars_15m=settings.filters.min_bars_15m,
        min_bars_1h=settings.filters.min_bars_1h,
        min_bars_4h=settings.filters.min_bars_4h,
    )
    registry = StrategyRegistry()
    for strategy_class in STRATEGY_CLASSES:
        registry.register(strategy_class(SetupParams(enabled=True), settings))
    engine = SignalEngine(registry, settings)
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

        hits_by_setup: Counter[str] = Counter()
        errors_by_setup: Counter[str] = Counter()
        reject_reasons: Counter[str] = Counter()
        detector_runs = 0
        prepared_ok = 0
        failures: list[dict[str, Any]] = []
        semaphore = asyncio.Semaphore(concurrency)

        async def _analyze(symbol: str) -> None:
            nonlocal detector_runs, prepared_ok
            async with semaphore:
                prepared = await _build_prepared(
                    client,
                    settings,
                    minimums,
                    meta_map,
                    ticker_map,
                    symbol,
                )
                if prepared is None:
                    failures.append({"symbol": symbol, "stage": "prepare", "error": "prepare_symbol returned None"})
                    return
                prepared_ok += 1
                results = await engine.calculate_all(prepared)
                detector_runs += len(results)
                for result in results:
                    setup_id = str(result.setup_id or result.metadata.get("setup_id") or getattr(result.signal, "setup_id", "unknown"))
                    decision = result.decision
                    if decision is not None and decision.is_reject:
                        reject_reasons.update([decision.reason_code])
                    if result.error:
                        errors_by_setup.update([setup_id])
                    elif result.signal is not None:
                        hits_by_setup.update([result.signal.setup_id])

        await asyncio.gather(*[asyncio.create_task(_analyze(symbol)) for symbol in symbols])
        LOG.info(
            "strategy_surface_summary",
            symbols=len(symbols),
            prepared_ok=prepared_ok,
            detector_runs=detector_runs,
            strategy_hits=hits_by_setup.most_common(),
            strategy_errors=errors_by_setup.most_common(),
            strategy_reject_reasons=reject_reasons.most_common(15),
        )
        if failures:
            LOG.warning("strategy_prepare_failures", failures=failures[:20])
        if errors_by_setup:
            raise RuntimeError(f"strategy errors detected: {errors_by_setup.most_common()}")
    finally:
        await client.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Live strategy detector-surface review")
    parser.add_argument("--symbols", nargs="*", default=[])
    parser.add_argument("--symbols-from-run", default="20260421_215817_70948")
    parser.add_argument("--limit", type=int, default=12)
    parser.add_argument("--concurrency", type=int, default=2)
    args = parser.parse_args()

    _configure_logging()
    symbols = [str(symbol).upper() for symbol in args.symbols if str(symbol).strip()]
    if not symbols:
        symbols = _symbols_from_run(args.symbols_from_run)
    if not symbols:
        symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
        LOG.info("symbols_fallback_used", symbols=symbols)
    if args.limit > 0:
        symbols = symbols[: args.limit]
    try:
        asyncio.run(_run(symbols, args.concurrency))
    except MarketDataUnavailable as exc:
        LOG.error("live_strategies_unavailable", operation=exc.operation, detail=exc.detail, symbol=exc.symbol)
        raise SystemExit(2) from exc


if __name__ == "__main__":
    main()
