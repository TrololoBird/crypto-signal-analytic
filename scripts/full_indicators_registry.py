"""
Full Indicators Registry Generator + Live Audit Script

This script:
1. Generates a complete registry of all indicators used across strategies, filters, and setups
2. Performs live API checks to verify all indicators are available and stable
3. Produces a comprehensive audit report

Usage:
    python -m scripts.full_indicators_registry --mode registry  # Generate registry only
    python -m scripts.full_indicators_registry --mode audit     # Full live audit
    python -m scripts.full_indicators_registry --symbols BTCUSDT ETHUSDT --warmup 30
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import structlog

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    force=True,
)
LOG = structlog.get_logger("scripts.full_indicators_registry")


# =============================================================================
# INDICATOR REGISTRY DEFINITIONS
# =============================================================================

@dataclass
class IndicatorDef:
    """Definition of a single indicator/field."""
    name: str
    category: str  # 'dataframe_column', 'prepared_field', 'enrichment', 'computed'
    source: str  # Where it's defined/used
    required_for: List[str]  # List of strategies/filters that use it
    data_type: str  # 'float', 'int', 'str', 'bool', 'dataframe'
    valid_range: Optional[Tuple[float, float]] = None
    nullable: bool = True
    notes: str = ""


# Complete registry based on code analysis
INDICATORS_REGISTRY: List[IndicatorDef] = [
    # =========================================================================
    # DataFrame Columns (work_1h, work_15m, work_5m, work_4h)
    # =========================================================================
    # Core OHLCV
    IndicatorDef("open", "dataframe_column", "klines", 
                 ["all"], "float", None, False, "Open price"),
    IndicatorDef("high", "dataframe_column", "klines",
                 ["all"], "float", None, False, "High price"),
    IndicatorDef("low", "dataframe_column", "klines",
                 ["all"], "float", None, False, "Low price"),
    IndicatorDef("close", "dataframe_column", "klines",
                 ["all"], "float", None, False, "Close price"),
    IndicatorDef("volume", "dataframe_column", "klines",
                 ["all"], "float", (0, None), False, "Volume"),
    IndicatorDef("close_time", "dataframe_column", "klines",
                 ["filters"], "datetime", None, False, "Bar close time"),
    
    # EMAs
    IndicatorDef("ema20", "dataframe_column", "features._ema",
                 ["structure_pullback", "ema_bounce", "filters"], "float", None, False, "EMA 20"),
    IndicatorDef("ema50", "dataframe_column", "features._ema",
                 ["ema_bounce", "hidden_divergence"], "float", None, False, "EMA 50"),
    IndicatorDef("ema200", "dataframe_column", "features._ema",
                 ["hidden_divergence", "fvg", "order_block"], "float", None, False, "EMA 200"),
    
    # Oscillators
    IndicatorDef("rsi14", "dataframe_column", "features._rsi",
                 ["structure_pullback", "session_killzone", "hidden_divergence", "cvd_divergence"], 
                 "float", (0, 100), False, "RSI 14"),
    IndicatorDef("adx14", "dataframe_column", "features._adx",
                 ["structure_pullback", "session_killzone", "filters", "ema_bounce"], 
                 "float", (0, 100), False, "ADX 14 (trend strength)"),
    
    # Volatility
    IndicatorDef("atr14", "dataframe_column", "features._atr",
                 ["structure_pullback", "ema_bounce", "wick_trap_reversal", "breaker_block",
                  "turtle_soup", "order_block", "squeeze_setup", "fvg", "bos_choch"], 
                 "float", (0, None), False, "ATR 14"),
    IndicatorDef("atr_pct", "dataframe_column", "features._atr",
                 ["filters", "confluence"], "float", (0, None), False, "ATR as % of price"),
    
    # Volume
    IndicatorDef("volume_ratio20", "dataframe_column", "features._volume_ratio",
                 ["structure_pullback", "ema_bounce", "session_killzone", "wick_trap_reversal",
                  "cvd_divergence", "liquidity_sweep", "order_block"], 
                 "float", (0, None), True, "Volume ratio vs 20-bar average"),
    
    # Trend/Structure
    IndicatorDef("supertrend_dir", "dataframe_column", "features._supertrend",
                 ["structure_pullback", "ema_bounce"], "int", (-1, 1), True, 
                 "SuperTrend direction: 1=uptrend, -1=downtrend"),
    
    # Bollinger Bands
    IndicatorDef("bb_pct_b", "dataframe_column", "features._bollinger_bands",
                 ["structure_pullback", "squeeze_setup", "breaker_block", "wick_trap_reversal"], 
                 "float", (0, 1), True, "%B position within bands"),
    IndicatorDef("bb_width", "dataframe_column", "features._bollinger_bands",
                 ["squeeze_setup"], "float", (0, None), True, "Band width"),
    
    # Keltner Channels
    IndicatorDef("kc_upper", "dataframe_column", "features._keltner_channels",
                 ["squeeze_setup"], "float", None, True, "KC upper band"),
    IndicatorDef("kc_lower", "dataframe_column", "features._keltner_channels",
                 ["squeeze_setup"], "float", None, True, "KC lower band"),
    IndicatorDef("kc_width", "dataframe_column", "features._keltner_channels",
                 ["squeeze_setup"], "float", (0, None), True, "KC width"),
    
    # VWAP
    IndicatorDef("vwap", "dataframe_column", "features._vwap",
                 ["cvd_divergence", "liquidity_sweep", "order_block"], "float", None, True, 
                 "Volume Weighted Average Price"),
    
    # Donchian Channels (actual column names in features.py)
    IndicatorDef("donchian_high20", "dataframe_column", "features._prepare_frame",
                 ["turtle_soup", "breaker_block"], "float", None, True, "Donchian 20-period high"),
    IndicatorDef("donchian_low20", "dataframe_column", "features._prepare_frame",
                 ["turtle_soup", "breaker_block"], "float", None, True, "Donchian 20-period low"),
    IndicatorDef("prev_donchian_high20", "dataframe_column", "features._prepare_frame",
                 ["turtle_soup", "breaker_block"], "float", None, True, "Previous Donchian high"),
    IndicatorDef("prev_donchian_low20", "dataframe_column", "features._prepare_frame",
                 ["turtle_soup", "breaker_block"], "float", None, True, "Previous Donchian low"),
    
    # MACD
    IndicatorDef("macd_line", "dataframe_column", "features._macd",
                 ["hidden_divergence", "session_killzone"], "float", None, True, "MACD line"),
    IndicatorDef("macd_signal", "dataframe_column", "features._macd",
                 ["hidden_divergence"], "float", None, True, "MACD signal"),
    IndicatorDef("macd_hist", "dataframe_column", "features._prepare_frame",
                 ["hidden_divergence", "session_killzone"], "float", None, True, "MACD histogram"),
    
    # CVD/Orderflow
    IndicatorDef("delta_ratio", "dataframe_column", "features._delta_ratio",
                 ["filters", "confluence"], "float", None, True, "Buy/sell delta ratio"),
    
    # Additional indicators from _add_advanced_indicators
    IndicatorDef("supertrend", "dataframe_column", "features._add_advanced_indicators",
                 [], "float", None, True, "SuperTrend value"),
    IndicatorDef("obv", "dataframe_column", "features._add_advanced_indicators",
                 [], "float", None, True, "On Balance Volume"),
    IndicatorDef("obv_ema20", "dataframe_column", "features._add_advanced_indicators",
                 [], "float", None, True, "OBV EMA 20"),
    IndicatorDef("cci20", "dataframe_column", "features._add_advanced_indicators",
                 [], "float", None, True, "CCI 20"),
    IndicatorDef("willr14", "dataframe_column", "features._add_advanced_indicators",
                 [], "float", (-100, 0), True, "Williams %R 14"),
    IndicatorDef("stoch_k14", "dataframe_column", "features._add_advanced_indicators",
                 [], "float", (0, 100), True, "Stochastic K 14"),
    IndicatorDef("stoch_d14", "dataframe_column", "features._add_advanced_indicators",
                 [], "float", (0, 100), True, "Stochastic D 14"),
    
    # CMF
    IndicatorDef("cmf20", "dataframe_column", "features._add_advanced_indicators",
                 ["cvd_divergence", "order_block", "bos_choch"], "float", (-1, 1), True, 
                 "Chaikin Money Flow"),
    
    # MFI
    IndicatorDef("mfi14", "dataframe_column", "features._mfi",
                 ["cvd_divergence"], "float", (0, 100), True, "Money Flow Index"),
    
    # =========================================================================
    # PreparedSymbol Fields (Direct Fields)
    # =========================================================================
    # Price data
    IndicatorDef("bid_price", "prepared_field", "SymbolFrames",
                 ["all"], "float", (0, None), True, "Best bid"),
    IndicatorDef("ask_price", "prepared_field", "SymbolFrames",
                 ["all"], "float", (0, None), True, "Best ask"),
    IndicatorDef("spread_bps", "prepared_field", "calculated",
                 ["filters", "scoring"], "float", (0, None), True, "Spread in basis points"),
    
    # Bias/Regime
    IndicatorDef("bias_1h", "prepared_field", "features._bias_1h",
                 ["structure_pullback", "ema_bounce", "scoring"], "str", None, False, 
                 "1H bias: bullish/bearish/neutral"),
    IndicatorDef("bias_4h", "prepared_field", "features._bias_4h",
                 ["filters", "scoring"], "str", None, False, "4H bias: bullish/bearish/neutral"),
    IndicatorDef("market_regime", "prepared_field", "features._market_regime",
                 ["scoring", "confluence"], "str", None, False, "Market regime: trending/neutral/choppy"),
    IndicatorDef("structure_1h", "prepared_field", "features._market_structure_1h",
                 ["structure_pullback", "confluence"], "str", None, False, 
                 "1H structure: uptrend/downtrend/ranging"),
    IndicatorDef("regime_4h_confirmed", "prepared_field", "features._regime_4h_confirmed",
                 ["structure_pullback", "confluence"], "str", None, False, "Confirmed 4H regime"),
    IndicatorDef("regime_1h_confirmed", "prepared_field", "features._regime_1h_confirmed",
                 ["structure_pullback", "confluence"], "str", None, False, "Confirmed 1H regime"),
    
    # Volume Profile
    IndicatorDef("poc_1h", "prepared_field", "features._volume_poc",
                 ["confluence"], "float", (0, None), True, "1H Point of Control"),
    IndicatorDef("poc_15m", "prepared_field", "features._volume_poc",
                 ["confluence"], "float", (0, None), True, "15M Point of Control"),
    
    # =========================================================================
    # WebSocket Enrichment Fields (Real-time Data)
    # =========================================================================
    IndicatorDef("mark_price", "enrichment", "WebSocket mark price stream",
                 ["filters", "calculations"], "float", (0, None), True, "Mark price from WS"),
    IndicatorDef("ticker_price", "enrichment", "WebSocket ticker stream",
                 ["filters"], "float", (0, None), True, "Last price from WS ticker"),
    IndicatorDef("funding_rate", "enrichment", "WebSocket mark price stream",
                 ["filters", "scoring", "funding_reversal"], "float", None, True, 
                 "Current funding rate"),
    IndicatorDef("oi_change_pct", "enrichment", "REST API OI data",
                 ["filters", "scoring"], "float", None, True, "Open Interest change %"),
    IndicatorDef("oi_slope_5m", "enrichment", "REST API OI data",
                 ["scoring"], "float", None, True, "OI slope over 5m"),
    IndicatorDef("ls_ratio", "enrichment", "REST API LS ratio",
                 ["filters", "scoring"], "float", (0, None), True, "Long/Short ratio"),
    IndicatorDef("global_ls_ratio", "enrichment", "REST API global LS",
                 ["confluence", "scoring"], "float", (0, None), True, "Global L/S ratio"),
    IndicatorDef("taker_ratio", "enrichment", "REST API taker data",
                 ["scoring"], "float", (0, None), True, "Taker buy/sell ratio"),
    IndicatorDef("liquidation_score", "enrichment", "WebSocket liquidation stream",
                 ["confluence", "scoring"], "float", (-1, 1), True, "Liquidation sentiment"),
    IndicatorDef("basis_pct", "enrichment", "REST API basis + WS mark/index",
                 ["filters", "scoring"], "float", None, True, "Futures-Index basis %"),
    IndicatorDef("mark_index_spread_bps", "enrichment", "REST API basis + WS",
                 ["confluence", "filters"], "float", None, True, "Mark-Index spread in bps"),
    IndicatorDef("premium_zscore_5m", "enrichment", "REST API basis history",
                 ["confluence", "scoring"], "float", None, True, "Premium z-score (5m)"),
    IndicatorDef("premium_slope_5m", "enrichment", "REST API basis history",
                 ["confluence", "scoring"], "float", None, True, "Premium slope (5m)"),
    IndicatorDef("depth_imbalance", "enrichment", "WebSocket book ticker",
                 ["confluence", "scoring"], "float", (-1, 1), True, "Order book imbalance"),
    IndicatorDef("microprice_bias", "enrichment", "WebSocket book ticker",
                 ["confluence", "scoring"], "float", None, True, "Microprice bias"),
    IndicatorDef("funding_trend", "enrichment", "REST API funding history",
                 ["scoring"], "str", None, True, "Funding trend: rising/falling/flat"),
    IndicatorDef("top_trader_position_ratio", "enrichment", "REST API LS ratio",
                 ["scoring"], "float", (0, None), True, "Top trader position ratio"),
    IndicatorDef("top_vs_global_ls_gap", "enrichment", "calculated from LS ratios",
                 ["scoring"], "float", None, True, "Gap between top and global LS"),
    IndicatorDef("agg_trade_delta_30s", "enrichment", "REST API taker 5m",
                 ["scoring"], "float", None, True, "Aggressive trade delta"),
    IndicatorDef("aggression_shift", "enrichment", "calculated from taker ratios",
                 ["scoring"], "float", None, True, "Shift in aggression"),
    
    # Age/Metadata
    IndicatorDef("mark_price_age_seconds", "enrichment", "WebSocket metadata",
                 ["diagnostics"], "float", (0, None), True, "Age of mark price data"),
    IndicatorDef("ticker_price_age_seconds", "enrichment", "WebSocket metadata",
                 ["diagnostics"], "float", (0, None), True, "Age of ticker data"),
    IndicatorDef("book_ticker_age_seconds", "enrichment", "WebSocket metadata",
                 ["diagnostics"], "float", (0, None), True, "Age of book ticker"),
    
    # =========================================================================
    # Computed/Derived Values (Not Direct Fields)
    # =========================================================================
    IndicatorDef("atr_pct", "computed", "PreparedSymbol property",
                 ["filters"], "float", (0, None), True, "ATR% computed from work_15m"),
    IndicatorDef("adx_1h", "computed", "Direct from work_1h.item(-1, 'adx14')",
                 ["filters", "strategy_rejection_logging"], "float", (0, 100), True, 
                 "ADX on 1H (computed)"),
    IndicatorDef("risk_reward", "computed", "Signal property",
                 ["rejection_logging"], "float", (0, None), True, "Risk/Reward ratio"),
]


def get_registry_by_category(category: str) -> List[IndicatorDef]:
    """Get all indicators of a specific category."""
    return [ind for ind in INDICATORS_REGISTRY if ind.category == category]


def get_registry_by_strategy(strategy: str) -> List[IndicatorDef]:
    """Get all indicators used by a specific strategy."""
    return [ind for ind in INDICATORS_REGISTRY if strategy in ind.required_for]


def export_registry_to_json(path: Path) -> None:
    """Export the registry to JSON."""
    data = [
        {
            "name": ind.name,
            "category": ind.category,
            "source": ind.source,
            "required_for": ind.required_for,
            "data_type": ind.data_type,
            "valid_range": ind.valid_range,
            "nullable": ind.nullable,
            "notes": ind.notes,
        }
        for ind in INDICATORS_REGISTRY
    ]
    
    output = {
        "generated_at": datetime.now().isoformat(),
        "total_indicators": len(INDICATORS_REGISTRY),
        "by_category": {
            "dataframe_column": len(get_registry_by_category("dataframe_column")),
            "prepared_field": len(get_registry_by_category("prepared_field")),
            "enrichment": len(get_registry_by_category("enrichment")),
            "computed": len(get_registry_by_category("computed")),
        },
        "indicators": data,
    }
    
    path.write_text(json.dumps(output, indent=2, default=str), encoding="utf-8")
    LOG.info("Registry exported", path=str(path), total=len(INDICATORS_REGISTRY))


# =============================================================================
# LIVE AUDIT IMPLEMENTATION
# =============================================================================

async def run_live_audit(
    symbols: List[str],
    warmup_seconds: float = 30.0,
    output_dir: Path = Path("scripts/audit_data"),
) -> Dict[str, Any]:
    """Run comprehensive live audit of all indicators."""
    from bot.config import load_settings
    from bot.features import min_required_bars, prepare_symbol
    from bot.market_data import BinanceFuturesMarketData
    from bot.models import SymbolFrames, UniverseSymbol
    from bot.ws_manager import FuturesWSManager
    
    settings = load_settings()
    minimums = min_required_bars(
        min_bars_15m=settings.filters.min_bars_15m,
        min_bars_1h=settings.filters.min_bars_1h,
        min_bars_4h=settings.filters.min_bars_4h,
    )
    
    client = BinanceFuturesMarketData(rest_timeout_seconds=settings.ws.rest_timeout_seconds)
    ws_manager = FuturesWSManager(client, settings.ws)
    
    results = {
        "started_at": datetime.now().isoformat(),
        "symbols": symbols,
        "indicators_checked": len(INDICATORS_REGISTRY),
        "symbol_results": {},
        "summary": {
            "total_ok": 0,
            "total_warnings": 0,
            "total_errors": 0,
        }
    }
    
    try:
        # Start WebSocket
        await ws_manager.start(list(symbols))
        connected = await ws_manager.wait_until_connected(timeout=30.0)
        if not connected:
            raise RuntimeError("WebSocket failed to connect")
        
        LOG.info("WebSocket connected, warming up...", warmup=warmup_seconds)
        await asyncio.sleep(warmup_seconds)
        
        # Pre-fetch REST data
        for symbol in symbols:
            LOG.info("Prefetching REST data", symbol=symbol)
            try:
                await client.fetch_open_interest(symbol)
                await client.fetch_open_interest_change(symbol)
                await client.fetch_long_short_ratio(symbol)
                await client.fetch_funding_rate(symbol)
                await client.fetch_basis(symbol, period="1h", limit=10)
                await client.fetch_global_ls_ratio(symbol)
            except Exception as exc:
                LOG.warning("Prefetch error", symbol=symbol, error=str(exc))
        
        await asyncio.sleep(2.0)
        
        # Get metadata
        exchange_symbols = await client.fetch_exchange_symbols()
        ticker_rows = await client.fetch_ticker_24h()
        meta_map = {row.symbol: row for row in exchange_symbols}
        ticker_map = {str(row.get("symbol") or "").upper(): row 
                      for row in ticker_rows if isinstance(row, dict)}
        
        # Audit each symbol
        for symbol in symbols:
            LOG.info("Auditing symbol", symbol=symbol)
            symbol_result = await _audit_symbol(
                symbol, meta_map, ticker_map, client, ws_manager, 
                settings, minimums
            )
            results["symbol_results"][symbol] = symbol_result
            
            # Update summary
            for ind_result in symbol_result["indicators"].values():
                if ind_result["status"] == "ok":
                    results["summary"]["total_ok"] += 1
                elif ind_result["status"] == "warning":
                    results["summary"]["total_warnings"] += 1
                else:
                    results["summary"]["total_errors"] += 1
        
        results["completed_at"] = datetime.now().isoformat()
        
        # Export results
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        results_path = output_dir / f"audit_results_{timestamp}.json"
        output_dir.mkdir(parents=True, exist_ok=True)
        results_path.write_text(json.dumps(results, indent=2, default=str), encoding="utf-8")
        LOG.info("Audit complete", results_path=str(results_path))
        
        return results
        
    finally:
        await ws_manager.stop()
        await client.close()


async def _audit_symbol(
    symbol: str,
    meta_map: Dict,
    ticker_map: Dict,
    client: BinanceFuturesMarketData,
    ws_manager: FuturesWSManager,
    settings: Any,
    minimums: Dict[str, int],
) -> Dict[str, Any]:
    """Audit all indicators for a single symbol."""
    from bot.models import SymbolFrames, UniverseSymbol
    
    result = {
        "symbol": symbol,
        "timestamp": datetime.now().isoformat(),
        "indicators": {},
    }
    
    meta = meta_map.get(symbol)
    if not meta:
        result["error"] = "Symbol metadata not found"
        return result
    
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
    
    # Fetch frames
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
        df_5m=None,
        df_4h=df_4h,
    )
    
    # Create prepared symbol
    from bot.features import prepare_symbol
    prepared = prepare_symbol(item, frames, minimums=minimums, settings=settings)
    
    if prepared is None:
        result["error"] = "prepare_symbol returned None"
        return result
    
    # Collect enrichments
    enrichments = _collect_enrichments(symbol, ws_manager, client)
    
    # Apply enrichments
    for key, value in enrichments.items():
        if hasattr(prepared, key):
            setattr(prepared, key, value)
    
    # Check each indicator
    for ind in INDICATORS_REGISTRY:
        check_result = _check_indicator(ind, prepared, enrichments)
        result["indicators"][ind.name] = check_result
    
    return result


def _collect_enrichments(symbol: str, ws_manager, client) -> Dict[str, Any]:
    """Collect all enrichment data."""
    enrichments: Dict[str, Any] = {}
    
    # WebSocket data
    if ws_manager is not None:
        try:
            ticker = ws_manager.get_ticker_snapshot(symbol)
            if ticker:
                enrichments["ticker_price"] = float(ticker.get("last_price") or 0.0)
            
            mp = ws_manager.get_mark_price_snapshot(symbol)
            if mp:
                enrichments["mark_price"] = float(mp.get("mark_price") or 0.0)
                if mp.get("funding_rate") is not None:
                    enrichments["funding_rate"] = float(mp["funding_rate"])
            
            liq = ws_manager.get_liquidation_sentiment(symbol, window_seconds=300)
            if liq is not None:
                enrichments["liquidation_score"] = liq
            
            depth_imb = ws_manager.get_depth_imbalance(symbol)
            if depth_imb is not None:
                enrichments["depth_imbalance"] = depth_imb
            
            micro_bias = ws_manager.get_microprice_bias(symbol)
            if micro_bias is not None:
                enrichments["microprice_bias"] = micro_bias
        except Exception:
            pass
    
    # REST cached data
    oi_chg = client.get_cached_oi_change(symbol)
    if oi_chg is not None:
        enrichments["oi_change_pct"] = oi_chg
    
    ls = client.get_cached_ls_ratio(symbol)
    if ls is not None:
        enrichments["ls_ratio"] = ls
    
    basis = client.get_cached_basis(symbol)
    if basis is not None:
        enrichments["basis_pct"] = basis
    
    for period in ["5m", "1h"]:
        basis_stats = client.get_cached_basis_stats(symbol, period=period)
        if basis_stats:
            for key in ["mark_index_spread_bps", "premium_slope_5m", "premium_zscore_5m"]:
                if basis_stats.get(key) is not None and enrichments.get(key) is None:
                    enrichments[key] = basis_stats[key]
    
    global_ls = client.get_cached_global_ls_ratio(symbol)
    if global_ls is not None:
        enrichments["global_ls_ratio"] = global_ls
    
    return enrichments


def _check_indicator(ind: IndicatorDef, prepared: Any, enrichments: Dict[str, Any]) -> Dict[str, Any]:
    """Check a single indicator and return status."""
    result = {
        "name": ind.name,
        "category": ind.category,
        "status": "unknown",
        "value": None,
        "error": None,
    }
    
    try:
        if ind.category == "dataframe_column":
            # Check in work_15m, work_1h, work_5m, work_4h
            value = None
            source_frame = None
            
            for frame_name in ["work_15m", "work_1h", "work_5m", "work_4h"]:
                frame = getattr(prepared, frame_name, None)
                if frame is not None and not frame.is_empty():
                    if ind.name in frame.columns:
                        try:
                            value = frame.item(-1, ind.name)
                            source_frame = frame_name
                            break
                        except Exception:
                            pass
            
            result["value"] = value
            result["source_frame"] = source_frame
            
            if value is None:
                if ind.nullable:
                    result["status"] = "warning"
                    result["error"] = "Column not found or null (but nullable)"
                else:
                    result["status"] = "error"
                    result["error"] = "Required column missing or null"
            else:
                # Check range
                if ind.valid_range and not _in_range(value, ind.valid_range):
                    result["status"] = "warning"
                    result["error"] = f"Value {value} outside valid range {ind.valid_range}"
                else:
                    result["status"] = "ok"
        
        elif ind.category == "prepared_field":
            value = getattr(prepared, ind.name, None)
            result["value"] = value
            
            if value is None:
                if ind.nullable:
                    result["status"] = "warning"
                else:
                    result["status"] = "error"
                    result["error"] = "Required field is null"
            else:
                result["status"] = "ok"
        
        elif ind.category == "enrichment":
            value = enrichments.get(ind.name)
            result["value"] = value
            
            if value is None:
                if ind.nullable:
                    result["status"] = "warning"
                else:
                    result["status"] = "error"
                    result["error"] = "Required enrichment missing"
            else:
                result["status"] = "ok"
        
        elif ind.category == "computed":
            # Special handling for computed properties
            if ind.name == "atr_pct":
                value = prepared.atr_pct
            elif ind.name == "adx_1h" and not prepared.work_1h.is_empty():
                value = float(prepared.work_1h.item(-1, "adx14") or 0.0)
            else:
                value = getattr(prepared, ind.name, None)
            
            result["value"] = value
            
            if value is None or (isinstance(value, float) and value == 0.0):
                result["status"] = "warning"
            else:
                result["status"] = "ok"
    
    except Exception as exc:
        result["status"] = "error"
        result["error"] = str(exc)
    
    return result


def _in_range(value: Any, range_def: Optional[Tuple[float, float]]) -> bool:
    """Check if value is within defined range."""
    if range_def is None:
        return True
    
    if not isinstance(value, (int, float)):
        return True
    
    min_val, max_val = range_def
    
    if min_val is not None and value < min_val:
        return False
    if max_val is not None and value > max_val:
        return False
    
    return True


def print_audit_summary(results: Dict[str, Any]) -> None:
    """Print human-readable audit summary."""
    print("\n" + "="*70)
    print("  FULL INDICATORS AUDIT SUMMARY")
    print("="*70)
    
    total = results.get("indicators_checked", 0)
    ok = results["summary"]["total_ok"]
    warnings = results["summary"]["total_warnings"]
    errors = results["summary"]["total_errors"]
    
    print(f"\nTotal indicators checked: {total}")
    print(f"  ✓ OK:        {ok} ({100*ok/total:.1f}%)")
    print(f"  ⚠ Warnings:  {warnings} ({100*warnings/total:.1f}%)")
    print(f"  ✗ Errors:    {errors} ({100*errors/total:.1f}%)")
    
    # Per-symbol breakdown
    print("\nPer-symbol results:")
    for symbol, symbol_result in results["symbol_results"].items():
        if "error" in symbol_result:
            print(f"  {symbol}: FAILED - {symbol_result['error']}")
            continue
        
        inds = symbol_result.get("indicators", {})
        s_ok = sum(1 for r in inds.values() if r["status"] == "ok")
        s_warn = sum(1 for r in inds.values() if r["status"] == "warning")
        s_err = sum(1 for r in inds.values() if r["status"] == "error")
        
        status = "✓" if s_err == 0 else "✗"
        print(f"  {status} {symbol}: {s_ok} OK, {s_warn} warnings, {s_err} errors")
    
    # List errors
    if errors > 0:
        print("\n" + "-"*70)
        print("  ERRORS DETAILED:")
        print("-"*70)
        for symbol, symbol_result in results["symbol_results"].items():
            if "indicators" not in symbol_result:
                continue
            for name, ind_result in symbol_result["indicators"].items():
                if ind_result["status"] == "error":
                    print(f"  {symbol}.{name}: {ind_result.get('error', 'Unknown error')}")
    
    print("\n" + "="*70)


def main():
    parser = argparse.ArgumentParser(description="Full Indicators Registry and Audit")
    parser.add_argument("--mode", choices=["registry", "audit", "both"], default="audit")
    parser.add_argument("--symbols", nargs="+", default=["BTCUSDT", "ETHUSDT", "SOLUSDT"])
    parser.add_argument("--warmup", type=float, default=30.0)
    parser.add_argument("--output-dir", default="scripts/audit_data")
    args = parser.parse_args()
    
    output_dir = Path(args.output_dir)
    
    if args.mode in ("registry", "both"):
        registry_path = output_dir / "indicators_registry.json"
        output_dir.mkdir(parents=True, exist_ok=True)
        export_registry_to_json(registry_path)
        print(f"Registry exported to: {registry_path}")
    
    if args.mode in ("audit", "both"):
        print(f"\nStarting live audit for symbols: {args.symbols}")
        print(f"WebSocket warmup: {args.warmup}s")
        print("This will take a few minutes...\n")
        
        results = asyncio.run(run_live_audit(
            symbols=args.symbols,
            warmup_seconds=args.warmup,
            output_dir=output_dir,
        ))
        
        print_audit_summary(results)


if __name__ == "__main__":
    main()
