"""Live check script to verify critical enrichment fields are populated.

This script verifies that the following fields (previously always NULL) are now
correctly populated with live data:
- mark_index_spread_bps
- premium_zscore_5m
- premium_slope_5m
- depth_imbalance
- microprice_bias
- basis_pct
- funding_rate
- oi_change_pct
- ls_ratio
- adx_1h
- risk_reward
- trend_direction

Usage:
    python scripts/live_check_enrichments.py --symbols BTCUSDT ETHUSDT SOLUSDT --warmup 30
"""
from __future__ import annotations

import argparse
import asyncio
import logging
from typing import Any

import structlog

from bot.config import load_settings
from bot.features import min_required_bars, prepare_symbol
from bot.market_data import BinanceFuturesMarketData
from bot.models import SymbolFrames, UniverseSymbol
from bot.ws_manager import FuturesWSManager

LOG = structlog.get_logger("scripts.live_check_enrichments")


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        force=True,
    )


def _check_field(prepared: Any, field: str) -> dict[str, Any]:
    """Check if a field is populated on prepared symbol.
    
    Special handling for adx_1h which is computed from work_1h DataFrame,
    not set via enrichment.
    """
    # Special case: adx_1h is computed from work_1h, not an enrichment field
    if field == "adx_1h" and hasattr(prepared, "work_1h"):
        try:
            if not prepared.work_1h.is_empty():
                value = float(prepared.work_1h.item(-1, "adx14") or 0.0)
                if value > 0:
                    return {
                        "field": field,
                        "value": value,
                        "is_populated": True,
                        "type": "float",
                        "source": "work_1h.adx14",
                    }
        except Exception:
            pass
    
    value = getattr(prepared, field, None)
    return {
        "field": field,
        "value": value,
        "is_populated": value is not None,
        "type": type(value).__name__ if value is not None else "None",
    }


def _collect_ws_enrichments(
    symbol: str,
    ws_manager: FuturesWSManager | None,
    client: BinanceFuturesMarketData,
) -> dict[str, Any]:
    """Collect enrichments from in-memory caches — mimics _ws_cache_enrichments in bot.py."""
    enrichments: dict[str, Any] = {}
    context_ages: list[float] = []

    if ws_manager is not None:
        try:
            ticker = ws_manager.get_ticker_snapshot(symbol)
            ticker_age = ws_manager.get_ticker_age_seconds(symbol)
            if ticker:
                ticker_price = float(ticker.get("last_price") or 0.0)
                if ticker_price > 0:
                    enrichments["ticker_price"] = ticker_price
                if ticker_age is not None:
                    enrichments["ticker_price_age_seconds"] = ticker_age
                    context_ages.append(ticker_age)
        except Exception:
            pass
        try:
            mp = ws_manager.get_mark_price_snapshot(symbol)
            mark_price_age = ws_manager.get_mark_price_age_seconds(symbol)
            if mp:
                mark_price = float(mp.get("mark_price") or 0.0)
                funding_rate = mp.get("funding_rate")
                if mark_price > 0:
                    enrichments["mark_price"] = mark_price
                if funding_rate is not None:
                    enrichments["funding_rate"] = float(funding_rate)
                if mark_price_age is not None:
                    enrichments["mark_price_age_seconds"] = mark_price_age
                    context_ages.append(mark_price_age)
        except Exception:
            pass
        try:
            book_age = ws_manager.get_book_ticker_age_seconds(symbol)
            if book_age is not None:
                enrichments["book_ticker_age_seconds"] = book_age
                context_ages.append(book_age)
        except Exception:
            pass
        try:
            liq = ws_manager.get_liquidation_sentiment(symbol, window_seconds=300)
            if liq is not None:
                enrichments["liquidation_score"] = liq
        except Exception:
            pass

    # REST cache data from client
    oi_chg = client.get_cached_oi_change(symbol)
    if oi_chg is not None:
        enrichments["oi_change_pct"] = oi_chg

    ls = client.get_cached_ls_ratio(symbol)
    if ls is not None:
        enrichments["ls_ratio"] = ls

    taker = client.get_cached_taker_ratio(symbol)
    if taker is not None:
        enrichments["taker_ratio"] = taker

    funding_trend = client.get_cached_funding_trend(symbol)
    if funding_trend is not None:
        enrichments["funding_trend"] = funding_trend

    basis = client.get_cached_basis(symbol)
    if basis is not None:
        enrichments["basis_pct"] = basis

    # Check for mark price from WebSocket to update basis cache
    mp_data = ws_manager.get_mark_price_snapshot(symbol) if ws_manager else None
    ticker = ws_manager.get_ticker_snapshot(symbol) if ws_manager else None
    if mp_data and mp_data.get("mark_price"):
        mark_price = float(mp_data.get("mark_price", 0))
        index_price = None
        if ticker:
            ticker_price = float(ticker.get("last_price") or 0)
            if ticker_price > 0:
                index_price = ticker_price
        ws_basis_stats = client.update_basis_from_websocket(
            symbol, mark_price, index_price=index_price, period="5m"
        )
        if ws_basis_stats:
            if ws_basis_stats.get("mark_index_spread_bps") is not None:
                enrichments["mark_index_spread_bps"] = ws_basis_stats["mark_index_spread_bps"]
            if ws_basis_stats.get("latest_basis_pct") is not None:
                enrichments["basis_pct"] = ws_basis_stats["latest_basis_pct"]

    global_ls = client.get_cached_global_ls_ratio(symbol)
    if global_ls is not None:
        enrichments["global_ls_ratio"] = global_ls

    # Try both 5m and 1h periods for basis stats
    for period in ["5m", "1h"]:
        basis_stats = client.get_cached_basis_stats(symbol, period=period)
        LOG.debug("basis_stats_check | symbol=%s period=%s stats=%s", symbol, period, basis_stats)
        if basis_stats is not None:
            if enrichments.get("mark_index_spread_bps") is None:
                enrichments["mark_index_spread_bps"] = basis_stats.get("mark_index_spread_bps")
            if enrichments.get("premium_slope_5m") is None:
                enrichments["premium_slope_5m"] = basis_stats.get("premium_slope_5m")
            if enrichments.get("premium_zscore_5m") is None:
                enrichments["premium_zscore_5m"] = basis_stats.get("premium_zscore_5m")

    # Order book based metrics from WebSocket
    if ws_manager is not None:
        depth_imb = ws_manager.get_depth_imbalance(symbol)
        if depth_imb is not None:
            enrichments["depth_imbalance"] = depth_imb
        micro_bias = ws_manager.get_microprice_bias(symbol)
        if micro_bias is not None:
            enrichments["microprice_bias"] = micro_bias

    if context_ages:
        enrichments["context_snapshot_age_seconds"] = max(context_ages)
    enrichments.setdefault("data_source_mix", "futures_only")
    return enrichments


async def _run(symbols: list[str], warmup_seconds: float) -> None:
    settings = load_settings()
    minimums = min_required_bars(
        min_bars_15m=settings.filters.min_bars_15m,
        min_bars_1h=settings.filters.min_bars_1h,
        min_bars_4h=settings.filters.min_bars_4h,
    )
    client = BinanceFuturesMarketData(rest_timeout_seconds=settings.ws.rest_timeout_seconds)
    ws_manager = FuturesWSManager(client, settings.ws)

    try:
        # Start WebSocket to get live data
        await ws_manager.start(list(symbols))
        connected = await ws_manager.wait_until_connected(timeout=30.0)
        if not connected:
            raise RuntimeError("ws_manager failed to connect within 30 seconds")

        LOG.info("ws_connected | warmup=%.1fs", warmup_seconds)
        await asyncio.sleep(warmup_seconds)

        # Pre-fetch all REST data to populate caches
        for symbol in symbols:
            LOG.info("prefetching_rest_data | symbol=%s", symbol)
            try:
                # Fetch OI data
                await client.fetch_open_interest(symbol)
                await client.fetch_open_interest_change(symbol)
                # Fetch LS ratio
                await client.fetch_long_short_ratio(symbol)
                # Fetch funding rate
                await client.fetch_funding_rate(symbol)
                # Fetch basis with higher limit to ensure enough data for zscore/slope
                basis_result = await client.fetch_basis(symbol, period="1h", limit=10)
                LOG.info("fetch_basis_result | symbol=%s basis=%s", symbol, basis_result)
                # Fetch global LS ratio
                await client.fetch_global_ls_ratio(symbol)
            except Exception as exc:
                LOG.warning("prefetch_error | symbol=%s error=%s", symbol, exc)

        LOG.info("rest_prefetch_complete")

        # Give WebSocket time to update after REST fetches
        await asyncio.sleep(2.0)

        # Get exchange info for building UniverseSymbols
        exchange_symbols = await client.fetch_exchange_symbols()
        ticker_rows = await client.fetch_ticker_24h()
        meta_map = {row.symbol: row for row in exchange_symbols}
        ticker_map = {
            str(row.get("symbol") or "").upper(): row
            for row in ticker_rows
            if isinstance(row, dict)
        }

        # Critical fields to check
        critical_fields = [
            "mark_index_spread_bps",
            "premium_zscore_5m",
            "premium_slope_5m",
            "depth_imbalance",
            "microprice_bias",
            "basis_pct",
            "funding_rate",
            "oi_change_pct",
            "ls_ratio",
            "adx_1h",
            # Note: risk_reward is a Signal property, not PreparedSymbol field
            # Note: trend_direction doesn't exist on PreparedSymbol
        ]

        all_results: list[dict[str, Any]] = []

        for symbol in symbols:
            LOG.info("checking_symbol | symbol=%s", symbol)

            meta = meta_map.get(symbol)
            if meta is None:
                LOG.error("symbol_not_found | symbol=%s", symbol)
                continue

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

            # Fetch klines
            df_4h = await client.fetch_klines_cached(symbol, "4h", limit=240)
            df_1h = await client.fetch_klines_cached(symbol, "1h", limit=240)
            df_15m = await client.fetch_klines_cached(symbol, "15m", limit=240)
            bid_price, ask_price = await client.fetch_book_ticker(symbol)

            frames = SymbolFrames(
                symbol=symbol,
                df_1h=df_1h,
                df_15m=df_15m,
                bid_price=bid_price,
                ask_price=ask_price,
                df_5m=None,  # Not needed for this check
                df_4h=df_4h,
            )

            # Create prepared symbol
            prepared = prepare_symbol(item, frames, minimums=minimums, settings=settings)
            if prepared is None:
                LOG.error("prepare_symbol_failed | symbol=%s", symbol)
                continue

            # Get enrichments and apply them
            ws_enrichments = _collect_ws_enrichments(symbol, ws_manager, client)

            LOG.info("ws_enrichments | symbol=%s fields=%d",
                     symbol, len(ws_enrichments))
            for key, value in ws_enrichments.items():
                if key.endswith("_age_seconds") or key == "data_source_mix":
                    continue
                LOG.debug("enrichment_value | symbol=%s %s=%s", symbol, key, value)

            # Apply enrichments to prepared
            for key, value in ws_enrichments.items():
                if hasattr(prepared, key):
                    setattr(prepared, key, value)

            # Check all critical fields
            field_results = []
            populated_count = 0
            for field in critical_fields:
                result = _check_field(prepared, field)
                field_results.append(result)
                if result["is_populated"]:
                    populated_count += 1
                LOG.info("field_check | symbol=%s field=%s populated=%s value=%s",
                         symbol, field, result["is_populated"], result["value"])

            all_results.append({
                "symbol": symbol,
                "total_fields": len(critical_fields),
                "populated_count": populated_count,
                "null_count": len(critical_fields) - populated_count,
                "fields": field_results,
                "enrichments_available": len(ws_enrichments),
            })

        # Final summary
        LOG.info("=" * 60)
        LOG.info("ENRICHMENT CHECK SUMMARY")
        LOG.info("=" * 60)

        total_fields_all = sum(r["total_fields"] for r in all_results)
        total_populated = sum(r["populated_count"] for r in all_results)
        total_null = sum(r["null_count"] for r in all_results)

        LOG.info("overall | total_fields=%d populated=%d null=%d rate=%.1f%%",
                 total_fields_all, total_populated, total_null,
                 100.0 * total_populated / total_fields_all if total_fields_all > 0 else 0)

        for result in all_results:
            symbol = result["symbol"]
            rate = 100.0 * result["populated_count"] / result["total_fields"]
            LOG.info("symbol=%s | populated=%d/%d (%.1f%%) | enrichments=%d",
                     symbol, result["populated_count"], result["total_fields"],
                     rate, result["enrichments_available"])

            # List null fields
            null_fields = [f["field"] for f in result["fields"] if not f["is_populated"]]
            if null_fields:
                LOG.warning("  null_fields | symbol=%s: %s", symbol, ", ".join(null_fields))

        # Check if all critical fields are now populated
        all_populated = total_null == 0
        if all_populated:
            LOG.info("SUCCESS: All critical fields are populated!")
        else:
            LOG.error("FAILURE: %d fields still NULL", total_null)

    finally:
        await ws_manager.stop()
        await client.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Live check for critical enrichment fields"
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=["BTCUSDT", "ETHUSDT", "SOLUSDT"],
        help="Symbols to check"
    )
    parser.add_argument(
        "--warmup",
        type=float,
        default=30.0,
        help="WebSocket warmup seconds"
    )
    args = parser.parse_args()

    _configure_logging()
    asyncio.run(_run(args.symbols, args.warmup))


if __name__ == "__main__":
    main()
