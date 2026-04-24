from __future__ import annotations

import json
import math
import time
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Any, cast

import aiohttp
import structlog

from .config import BotSettings
from .features import _cached_prepare_frame, _swing_points, _to_polars
from .market_data import BinanceFuturesMarketData
from .telemetry import TelemetryStore

LOG = structlog.get_logger("bot.public_intelligence")
_OPTIONS_EXCHANGE_INFO_TTL_S = 3600.0
_MACRO_TTL_S = 900.0
_PLACEHOLDER_COLUMNS: frozenset[str] = frozenset(
    {
        "aroon_up14",
        "aroon_down14",
        "aroon_osc14",
        "stoch_k14",
        "stoch_d14",
        "stoch_h14",
        "cci20",
        "willr14",
        "mfi14",
        "cmf20",
        "uo",
        "fisher",
        "fisher_signal",
        "squeeze_hist",
        "squeeze_on",
        "squeeze_off",
        "squeeze_no",
        "chandelier_dir",
        "zscore30",
        "slope5",
    }
)


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(parsed) or math.isinf(parsed):
        return None
    return parsed


def _normalize_direction(direction: Any) -> str:
    raw = str(direction or "").strip().lower()
    return raw if raw in {"long", "short"} else "long"


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    return str(value)


def _dump_json(
    value: Any,
    *,
    indent: int | None = None,
    sort_keys: bool = False,
) -> str:
    return json.dumps(
        value,
        ensure_ascii=True,
        indent=indent,
        sort_keys=sort_keys,
        default=_json_default,
    )


class PublicIntelligenceService:
    """Public-only market intelligence snapshots for runtime and agent debugging."""

    def __init__(
        self,
        settings: BotSettings,
        market_data: BinanceFuturesMarketData,
        telemetry: TelemetryStore,
    ) -> None:
        self._settings = settings
        self._client = market_data
        self._telemetry = telemetry
        self._reports_dir = settings.data_dir / "session" / "reports"
        self._reports_dir.mkdir(parents=True, exist_ok=True)
        self._options_exchange_cache: dict[str, tuple[float, list[dict[str, Any]]]] = {}
        self._macro_cache: dict[str, tuple[float, dict[str, Any]]] = {}
        self._latest_snapshot: dict[str, Any] | None = None
        self._last_report_hour_key: str | None = None

    @property
    def latest_snapshot(self) -> dict[str, Any] | None:
        return self._latest_snapshot

    def _policy_snapshot(self) -> dict[str, str]:
        intelligence = self._settings.intelligence
        return {
            "runtime_mode": intelligence.runtime_mode,
            "source_policy": intelligence.source_policy,
            "smart_exit_mode": intelligence.smart_exit_mode,
            "gamma_semantics": intelligence.gamma_semantics,
        }

    def _macro_disabled_snapshot(self) -> dict[str, Any]:
        source_policy = self._settings.intelligence.source_policy
        return {
            "enabled": False,
            "available": False,
            "status": "disabled_by_source_policy",
            "reason": f"source_policy_{source_policy}",
            "risk_mode": "disabled_binance_only",
            "risk_off_votes": 0,
            "risk_on_votes": 0,
            "by_symbol": {},
            "confirmed_facts": [
                "external_macro_and_news_feeds_disabled_under_source_policy_binance_only"
            ],
        }

    async def collect(self, shortlist_symbols: Iterable[str]) -> dict[str, Any]:
        symbols = [str(item).strip().upper() for item in shortlist_symbols if str(item).strip()]
        symbols = list(dict.fromkeys(symbols))
        pinned_symbols = set(self._settings.universe.pinned_symbols)
        benchmark_symbols = list(
            dict.fromkeys(
                list(self._settings.intelligence.benchmark_symbols)
                + [symbol for symbol in symbols if symbol in pinned_symbols]
            )
        )

        policy = self._policy_snapshot()
        derivatives = await self._build_derivatives_snapshot(benchmark_symbols)
        options = await self._build_options_snapshot()
        macro = (
            self._macro_disabled_snapshot()
            if policy["source_policy"] == "binance_only"
            else await self._build_macro_snapshot()
        )
        barrier = await self._build_barrier_snapshot(benchmark_symbols)
        harmonic = await self._build_harmonic_snapshot(benchmark_symbols[:2] or ["BTCUSDT"])
        summary = self._build_summary(
            derivatives=derivatives,
            options=options,
            macro=macro,
            barrier=barrier,
        )
        macro_enabled = bool(cast(dict[str, Any], macro).get("enabled", True))

        snapshot: dict[str, Any] = {
            "ts": datetime.now(UTC).isoformat(),
            "runtime_mode": policy["runtime_mode"],
            "source_policy": policy["source_policy"],
            "smart_exit_mode": policy["smart_exit_mode"],
            "gamma_semantics": policy["gamma_semantics"],
            "policy": policy,
            "confirmed_facts": summary["confirmed_facts"],
            "inferences": summary["inferences"],
            "assumptions": summary["assumptions"],
            "uncertainty": summary["uncertainty"],
            "sources": {
                "binance_futures_public": True,
                "binance_options_public": True,
                "yahoo_macro_public": macro_enabled,
                "external_macro_public": macro_enabled,
                "external_news_public": False,
            },
            "benchmarks": benchmark_symbols,
            "derivatives": derivatives,
            "options": options,
            "macro": macro,
            "barrier": barrier,
            "harmonic": harmonic,
        }

        self._latest_snapshot = snapshot
        self._telemetry.append_jsonl("public_intelligence.jsonl", snapshot)
        self._write_latest_snapshot(snapshot)
        self._maybe_write_hourly_report(snapshot)
        return snapshot

    async def evaluate_smart_exit(self, symbol: str, direction: str) -> dict[str, Any]:
        direction = _normalize_direction(direction)
        feature_grid = await self._build_feature_grid(symbol)
        latest = self._latest_snapshot or {}
        macro = cast(dict[str, Any], latest.get("macro") or {})
        options = cast(dict[str, Any], latest.get("options") or {})
        derivatives = cast(dict[str, Any], latest.get("derivatives") or {})
        barrier = cast(dict[str, Any], latest.get("barrier") or {})

        if not feature_grid.get("available", False):
            return {
                "available": False,
                "triggered": False,
                "mode": self._settings.intelligence.smart_exit_mode,
                "symbol": symbol,
                "direction": direction,
                "runtime_mode": self._settings.intelligence.runtime_mode,
                "source_policy": self._settings.intelligence.source_policy,
                "semantics": "tracked_signal_analytical_exit",
                "feature_count": 0,
                "placeholder_feature_count": 0,
                "confidence": 0.0,
                "reasons": [],
                "uncertainty": ["feature_grid_unavailable"],
            }

        macro_enabled = bool(macro.get("enabled", True))
        features = cast(dict[str, float | None], feature_grid["features"])
        reasons: list[str] = []
        votes = 0
        max_votes = 0

        def _against_long(value: float | None, threshold: float, reason: str) -> None:
            nonlocal votes, max_votes
            if value is None:
                return
            max_votes += 1
            if value <= threshold:
                votes += 1
                reasons.append(reason)

        def _against_short(value: float | None, threshold: float, reason: str) -> None:
            nonlocal votes, max_votes
            if value is None:
                return
            max_votes += 1
            if value >= threshold:
                votes += 1
                reasons.append(reason)

        for interval in ("15m", "1h", "4h", "1d"):
            close = features.get(f"{interval}_close")
            ema20 = features.get(f"{interval}_ema20")
            ema50 = features.get(f"{interval}_ema50")
            ema200 = features.get(f"{interval}_ema200")
            rsi14 = features.get(f"{interval}_rsi14")
            macd_hist = features.get(f"{interval}_macd_hist")
            supertrend_dir = features.get(f"{interval}_supertrend_dir")

            if direction == "long":
                if close is not None and ema20 is not None:
                    _against_long(close - ema20, 0.0, f"{interval}_close_below_ema20")
                if ema20 is not None and ema50 is not None:
                    _against_long(ema20 - ema50, 0.0, f"{interval}_ema20_below_ema50")
                if ema50 is not None and ema200 is not None:
                    _against_long(ema50 - ema200, 0.0, f"{interval}_ema50_below_ema200")
                _against_long(rsi14, 45.0, f"{interval}_rsi_weak")
                _against_long(macd_hist, 0.0, f"{interval}_macd_negative")
                _against_long(supertrend_dir, 0.0, f"{interval}_supertrend_negative")
            else:
                if close is not None and ema20 is not None:
                    _against_short(close - ema20, 0.0, f"{interval}_close_above_ema20")
                if ema20 is not None and ema50 is not None:
                    _against_short(ema20 - ema50, 0.0, f"{interval}_ema20_above_ema50")
                if ema50 is not None and ema200 is not None:
                    _against_short(ema50 - ema200, 0.0, f"{interval}_ema50_above_ema200")
                _against_short(rsi14, 55.0, f"{interval}_rsi_strong_against_short")
                _against_short(macd_hist, 0.0, f"{interval}_macd_positive")
                _against_short(supertrend_dir, 0.0, f"{interval}_supertrend_positive")

        symbol_derivatives = cast(
            dict[str, Any],
            cast(dict[str, Any], derivatives.get("by_symbol") or {}).get(symbol) or {},
        )
        benchmark_options = cast(dict[str, Any], options.get("by_underlying") or {}).get("BTC", {})
        put_call_ratio = _safe_float(benchmark_options.get("put_call_oi_ratio"))
        basis_pct = _safe_float(symbol_derivatives.get("basis_pct"))
        taker_ratio = _safe_float(symbol_derivatives.get("taker_ratio"))
        oi_change_pct = _safe_float(symbol_derivatives.get("oi_change_pct"))
        macro_risk_mode = str(
            macro.get("risk_mode")
            or ("disabled_binance_only" if not macro_enabled else "normal")
        )

        if direction == "long":
            _against_long(taker_ratio, 0.97, "taker_flow_against_long")
            _against_long(basis_pct, 0.0, "basis_backwardation")
            _against_long(oi_change_pct, 0.0, "oi_contracting")
            _against_short(put_call_ratio, 1.05, "put_call_ratio_risk_off")
            if macro_enabled:
                max_votes += 1
                if macro_risk_mode == "risk_off":
                    votes += 1
                    reasons.append("macro_risk_off")
            max_votes += 1
            if bool(barrier.get("long_barrier_triggered")):
                votes += 1
                reasons.append("hard_barrier_active")
        else:
            _against_short(taker_ratio, 1.03, "taker_flow_against_short")
            _against_short(basis_pct, 0.0, "basis_contango_against_short")
            _against_short(oi_change_pct, 0.0, "oi_expanding_against_short")
            _against_long(put_call_ratio, 0.95, "put_call_ratio_risk_on")
            if macro_enabled:
                max_votes += 1
                if macro_risk_mode == "risk_on":
                    votes += 1
                    reasons.append("macro_risk_on")
            max_votes += 1
            if bool(barrier.get("short_barrier_triggered")):
                votes += 1
                reasons.append("hard_barrier_active")

        confidence = round(votes / max(max_votes, 1), 4)
        return {
            "available": True,
            "triggered": confidence >= float(self._settings.intelligence.smart_exit_threshold),
            "mode": self._settings.intelligence.smart_exit_mode,
            "symbol": symbol,
            "direction": direction,
            "runtime_mode": self._settings.intelligence.runtime_mode,
            "source_policy": self._settings.intelligence.source_policy,
            "semantics": "tracked_signal_analytical_exit",
            "feature_count": int(feature_grid["feature_count"]),
            "placeholder_feature_count": int(feature_grid["placeholder_feature_count"]),
            "confidence": confidence,
            "reasons": reasons[:16],
            "feature_grid": feature_grid,
            "uncertainty": [
                "smart_exit_is_rule_based_not_neural_model",
                "placeholder_indicator_columns_exist_when_optional_talib_stack_is_missing",
            ],
        }

    async def _build_derivatives_snapshot(self, benchmark_symbols: list[str]) -> dict[str, Any]:
        by_symbol: dict[str, Any] = {}
        confirmed_facts: list[str] = []
        for symbol in benchmark_symbols:
            funding_rate = await self._client.fetch_funding_rate(symbol)
            oi_current = await self._client.fetch_open_interest(symbol)
            oi_change_pct = await self._client.fetch_open_interest_change(symbol, period="1h")
            top_ls_ratio = await self._client.fetch_long_short_ratio(symbol, period="1h")
            global_ls_ratio = await self._client.fetch_global_ls_ratio(symbol, period="1h")
            taker_ratio = await self._client.fetch_taker_ratio(symbol, period="1h")
            basis_pct = await self._client.fetch_basis(symbol, period="1h")
            funding_history = await self._client.fetch_funding_rate_history(symbol, limit=4)
            funding_trend = self._client.get_cached_funding_trend(symbol)

            flow_bias = "neutral"
            if taker_ratio is not None and oi_change_pct is not None:
                if taker_ratio > 1.03 and oi_change_pct > 0:
                    flow_bias = "buyers_in_control"
                elif taker_ratio < 0.97 and oi_change_pct > 0:
                    flow_bias = "sellers_in_control"
                elif oi_change_pct < 0:
                    flow_bias = "position_unwind"

            by_symbol[symbol] = {
                "funding_rate": funding_rate,
                "oi_current": oi_current,
                "oi_change_pct": oi_change_pct,
                "top_trader_long_short_ratio": top_ls_ratio,
                "global_long_short_ratio": global_ls_ratio,
                "taker_ratio": taker_ratio,
                "basis_pct": basis_pct,
                "funding_trend": funding_trend,
                "flow_bias": flow_bias,
                "funding_history_points": len(funding_history),
            }
            if funding_rate is not None or oi_current is not None or taker_ratio is not None:
                confirmed_facts.append(f"{symbol}_public_futures_context_available")

        return {
            "by_symbol": by_symbol,
            "confirmed_facts": confirmed_facts,
        }

    async def _build_options_snapshot(self) -> dict[str, Any]:
        by_underlying: dict[str, Any] = {}
        confirmed_facts: list[str] = []
        inferences: list[str] = []
        assumptions: list[str] = []
        gamma_semantics = self._settings.intelligence.gamma_semantics

        for asset in self._settings.intelligence.option_underlyings:
            meta_rows = await self._fetch_options_exchange_info(asset)
            if not meta_rows:
                by_underlying[asset] = {
                    "available": False,
                    "reason": "no_public_option_symbols",
                    "gamma_semantics": gamma_semantics,
                }
                continue

            expiries = sorted(
                {
                    str(row["symbol"]).split("-")[1]
                    for row in meta_rows
                    if str(row.get("symbol"))
                }
            )
            selected_expiries = expiries[
                : max(1, int(self._settings.intelligence.options_expiry_count))
            ]
            oi_rows: list[dict[str, Any]] = []
            for expiry in selected_expiries:
                oi_rows.extend(await self._fetch_options_open_interest(asset, expiry))
            mark_rows = await self._fetch_options_mark_rows(f"{asset}USDT")

            meta_by_symbol = {str(row["symbol"]): row for row in meta_rows}
            mark_by_symbol = {str(row["symbol"]): row for row in mark_rows}
            call_oi_usd = 0.0
            put_oi_usd = 0.0
            call_oi_contracts = 0.0
            put_oi_contracts = 0.0
            weighted_iv_numerator = 0.0
            weighted_iv_denominator = 0.0
            gamma_balance_proxy = 0.0
            gamma_abs_proxy = 0.0

            for row in oi_rows:
                symbol = str(row.get("symbol") or "")
                if not symbol:
                    continue
                meta = meta_by_symbol.get(symbol, {})
                mark = mark_by_symbol.get(symbol, {})
                side = str(meta.get("side") or "").upper()
                oi_contracts = _safe_float(row.get("sumOpenInterest")) or 0.0
                oi_usd = _safe_float(row.get("sumOpenInterestUsd")) or 0.0
                gamma = _safe_float(mark.get("gamma")) or 0.0
                mark_iv = _safe_float(mark.get("markIV"))
                unit = _safe_float(meta.get("unit")) or 1.0

                if side == "CALL":
                    call_oi_usd += oi_usd
                    call_oi_contracts += oi_contracts
                    gamma_balance_proxy += gamma * oi_contracts * unit
                elif side == "PUT":
                    put_oi_usd += oi_usd
                    put_oi_contracts += oi_contracts
                    gamma_balance_proxy -= gamma * oi_contracts * unit
                gamma_abs_proxy += abs(gamma * oi_contracts * unit)

                if mark_iv is not None and oi_usd > 0.0:
                    weighted_iv_numerator += mark_iv * oi_usd
                    weighted_iv_denominator += oi_usd

            put_call_oi_ratio = None
            if call_oi_usd > 0.0:
                put_call_oi_ratio = round(put_oi_usd / call_oi_usd, 4)
            gamma_balance_ratio = 0.0
            if gamma_abs_proxy > 0.0:
                gamma_balance_ratio = gamma_balance_proxy / gamma_abs_proxy
            gamma_positioning_proxy = "flat_proxy"
            if gamma_balance_ratio >= 0.10:
                gamma_positioning_proxy = "positive_gamma_proxy"
            elif gamma_balance_ratio <= -0.10:
                gamma_positioning_proxy = "negative_gamma_proxy"

            by_underlying[asset] = {
                "available": True,
                "expiries_used": selected_expiries,
                "call_oi_contracts": round(call_oi_contracts, 4),
                "put_oi_contracts": round(put_oi_contracts, 4),
                "call_oi_usd": round(call_oi_usd, 2),
                "put_oi_usd": round(put_oi_usd, 2),
                "put_call_oi_ratio": put_call_oi_ratio,
                "weighted_mark_iv": round(weighted_iv_numerator / weighted_iv_denominator, 6)
                if weighted_iv_denominator > 0.0
                else None,
                "gamma_balance_proxy": round(gamma_balance_proxy, 8),
                "gamma_balance_ratio": round(gamma_balance_ratio, 6),
                "gamma_positioning_proxy": gamma_positioning_proxy,
                "gamma_semantics": gamma_semantics,
                "mark_rows": len(mark_rows),
                "oi_rows": len(oi_rows),
            }
            confirmed_facts.append(f"{asset}_public_options_mark_and_oi_available")
            inferences.append(
                f"{asset}_gamma_positioning_proxy_is_derived_from_public_gamma_and_oi"
            )
            assumptions.append(
                f"{asset}_gamma_proxy_is_sign_based_and_does_not_observe_real_dealer_inventory"
            )

        return {
            "by_underlying": by_underlying,
            "confirmed_facts": confirmed_facts,
            "inferences": inferences,
            "assumptions": assumptions,
        }

    async def _build_macro_snapshot(self) -> dict[str, Any]:
        by_symbol: dict[str, Any] = {}
        risk_off_votes = 0
        risk_on_votes = 0
        confirmed_facts: list[str] = []
        for symbol in self._settings.intelligence.macro_symbols:
            snapshot = await self._fetch_yahoo_chart_snapshot(symbol)
            by_symbol[symbol] = snapshot
            if bool(snapshot.get("available")):
                confirmed_facts.append(f"{symbol}_public_macro_quote_available")
            change_pct = _safe_float(snapshot.get("change_pct")) or 0.0
            if symbol == "^VIX":
                if change_pct >= 8.0:
                    risk_off_votes += 1
            elif symbol == "DX-Y.NYB":
                if change_pct >= 0.35:
                    risk_off_votes += 1
                elif change_pct <= -0.35:
                    risk_on_votes += 1
            elif symbol == "^TNX":
                if change_pct >= 1.0:
                    risk_off_votes += 1
            elif symbol == "^GSPC":
                if change_pct <= -0.75:
                    risk_off_votes += 1
                elif change_pct >= 0.75:
                    risk_on_votes += 1

        risk_mode = "normal"
        if risk_off_votes >= 2:
            risk_mode = "risk_off"
        elif risk_on_votes >= 2:
            risk_mode = "risk_on"

        available = any(bool(item.get("available")) for item in by_symbol.values())
        return {
            "enabled": True,
            "available": available,
            "status": "ok" if available else "unavailable",
            "provider": "yahoo_chart_public",
            "risk_mode": risk_mode,
            "risk_off_votes": risk_off_votes,
            "risk_on_votes": risk_on_votes,
            "by_symbol": by_symbol,
            "confirmed_facts": confirmed_facts,
        }

    async def _build_barrier_snapshot(self, benchmark_symbols: list[str]) -> dict[str, Any]:
        strongest_move_pct = 0.0
        strongest_symbol: str | None = None
        long_barrier_triggered = False
        short_barrier_triggered = False

        window_minutes = int(self._settings.intelligence.hard_barrier_window_minutes)
        limit = max(6, int(math.ceil(window_minutes / 5.0)) + 1)
        for symbol in benchmark_symbols[
            : max(1, int(self._settings.intelligence.barrier_symbol_count))
        ]:
            try:
                df = await self._client.fetch_klines_cached(symbol, "5m", limit=limit)
            except Exception:
                continue
            if df is None or df.height < 2:
                continue
            start_price = _safe_float(df.item(max(0, df.height - limit), "close"))
            end_price = _safe_float(df.item(-1, "close"))
            if start_price is None or end_price is None or start_price <= 0.0:
                continue
            move_pct = ((end_price / start_price) - 1.0) * 100.0
            if abs(move_pct) >= abs(strongest_move_pct):
                strongest_move_pct = move_pct
                strongest_symbol = symbol
            if move_pct <= -float(self._settings.intelligence.hard_barrier_pct):
                long_barrier_triggered = True
            if move_pct >= float(self._settings.intelligence.hard_barrier_pct):
                short_barrier_triggered = True

        return {
            "window_minutes": window_minutes,
            "threshold_pct": float(self._settings.intelligence.hard_barrier_pct),
            "long_barrier_triggered": long_barrier_triggered,
            "short_barrier_triggered": short_barrier_triggered,
            "strongest_symbol": strongest_symbol,
            "strongest_move_pct": round(strongest_move_pct, 4),
        }

    async def _build_harmonic_snapshot(self, symbols: list[str]) -> dict[str, Any]:
        by_symbol: dict[str, Any] = {}
        for symbol in symbols:
            try:
                df = await self._client.fetch_klines_cached(symbol, "1h", limit=260)
            except Exception:
                by_symbol[symbol] = {"available": False, "reason": "frame_fetch_failed"}
                continue
            if df is None or df.is_empty():
                by_symbol[symbol] = {"available": False, "reason": "no_frame_data"}
                continue
            work = _cached_prepare_frame(_to_polars(df), symbol=symbol, interval="1h")
            swing_high, swing_low = _swing_points(work, n=3)
            pivots: list[tuple[int, float, str]] = []
            for idx, row in enumerate(work.iter_rows(named=True)):
                ts_raw = row.get("close_time")
                ts_val = int(ts_raw.timestamp()) if isinstance(ts_raw, datetime) else idx
                if bool(swing_high[idx]):
                    pivots.append((ts_val, float(row["high"]), "high"))
                if bool(swing_low[idx]):
                    pivots.append((ts_val, float(row["low"]), "low"))
            pivots.sort(key=lambda item: item[0])
            if len(pivots) < 4:
                by_symbol[symbol] = {"available": False, "reason": "not_enough_pivots"}
                continue
            last_four = pivots[-4:]
            ab = last_four[1][1] - last_four[0][1]
            bc = last_four[2][1] - last_four[1][1]
            cd = last_four[3][1] - last_four[2][1]
            ratio_abcd = abs(cd) / abs(ab) if abs(ab) > 0 else None
            retracement_bc = abs(bc) / abs(ab) if abs(ab) > 0 else None
            deviation_pct = (
                abs((ratio_abcd or 0.0) - 1.0) * 100.0
                if ratio_abcd is not None
                else None
            )
            by_symbol[symbol] = {
                "available": True,
                "pattern": "abcd_like"
                if ratio_abcd is not None
                and retracement_bc is not None
                and 0.382 <= retracement_bc <= 0.886
                and abs(ratio_abcd - 1.0) <= 0.25
                else "none",
                "ratio_ab_cd": round(ratio_abcd, 4) if ratio_abcd is not None else None,
                "ratio_bc_ab": round(retracement_bc, 4) if retracement_bc is not None else None,
                "deviation_pct": round(deviation_pct, 4) if deviation_pct is not None else None,
            }
        return {
            "enabled": True,
            "mode": "debug_only_confluence",
            "standalone_strategy": False,
            "weight": "low",
            "by_symbol": by_symbol,
        }

    async def _build_feature_grid(self, symbol: str) -> dict[str, Any]:
        intervals = ("15m", "1h", "4h", "1d")
        interval_limits = {"15m": 260, "1h": 260, "4h": 260, "1d": 220}
        metrics = (
            "close",
            "ema20",
            "ema50",
            "ema200",
            "rsi14",
            "adx14",
            "atr_pct",
            "macd_hist",
            "volume_ratio20",
            "delta_ratio",
            "supertrend_dir",
            "bb_pct_b",
            "bb_width",
            "close_position",
            "vwap",
            "vwap_upper1",
            "vwap_lower1",
            "donchian_low20",
            "donchian_high20",
            "stoch_k14",
            "stoch_d14",
            "cci20",
            "willr14",
            "mfi14",
            "cmf20",
            "uo",
            "fisher",
            "fisher_signal",
            "squeeze_hist",
            "chandelier_dir",
            "zscore30",
            "slope5",
            "ichi_tenkan",
            "ichi_kijun",
            "ichi_senkou_a",
            "ichi_senkou_b",
        )
        features: dict[str, float | None] = {}
        placeholder_feature_count = 0

        for interval in intervals:
            try:
                frame = await self._client.fetch_klines_cached(
                    symbol,
                    interval,
                    limit=interval_limits[interval],
                )
            except Exception:
                continue
            if frame is None or frame.is_empty():
                continue
            work = _cached_prepare_frame(_to_polars(frame), symbol=symbol, interval=interval)
            if work.is_empty():
                continue
            last = work.row(-1, named=True)
            for metric in metrics:
                key = f"{interval}_{metric}"
                value = _safe_float(last.get(metric))
                features[key] = value
                if metric in _PLACEHOLDER_COLUMNS:
                    placeholder_feature_count += 1

        latest = self._latest_snapshot or {}
        derivatives_by_symbol = cast(
            dict[str, Any],
            cast(dict[str, Any], latest.get("derivatives") or {}).get("by_symbol") or {},
        )
        symbol_derivatives = cast(dict[str, Any], derivatives_by_symbol.get(symbol) or {})
        for key in (
            "funding_rate",
            "oi_current",
            "oi_change_pct",
            "taker_ratio",
            "basis_pct",
            "global_long_short_ratio",
            "top_trader_long_short_ratio",
        ):
            features[key] = _safe_float(symbol_derivatives.get(key))

        macro = cast(dict[str, Any], latest.get("macro") or {})
        features["macro_risk_off"] = 1.0 if str(macro.get("risk_mode") or "") == "risk_off" else 0.0
        features["macro_risk_on"] = 1.0 if str(macro.get("risk_mode") or "") == "risk_on" else 0.0
        options = cast(
            dict[str, Any],
            cast(dict[str, Any], latest.get("options") or {}).get("by_underlying") or {},
        )
        btc_options = cast(dict[str, Any], options.get("BTC") or {})
        features["btc_put_call_oi_ratio"] = _safe_float(btc_options.get("put_call_oi_ratio"))
        features["btc_gamma_balance_ratio"] = _safe_float(btc_options.get("gamma_balance_ratio"))

        return {
            "available": bool(features),
            "symbol": symbol,
            "feature_count": len(features),
            "placeholder_feature_count": placeholder_feature_count,
            "features": features,
        }

    def _build_summary(
        self,
        *,
        derivatives: dict[str, Any],
        options: dict[str, Any],
        macro: dict[str, Any],
        barrier: dict[str, Any],
    ) -> dict[str, list[str]]:
        confirmed_facts = list(cast(list[str], derivatives.get("confirmed_facts") or []))
        confirmed_facts.extend(cast(list[str], options.get("confirmed_facts") or []))
        confirmed_facts.extend(cast(list[str], macro.get("confirmed_facts") or []))
        if bool(barrier.get("long_barrier_triggered")) or bool(
            barrier.get("short_barrier_triggered")
        ):
            confirmed_facts.append("hard_barrier_threshold_breached_on_public_benchmark_data")

        inferences = list(cast(list[str], options.get("inferences") or []))
        macro_enabled = bool(macro.get("enabled", True))
        macro_risk_mode = str(
            macro.get("risk_mode")
            or ("disabled_binance_only" if not macro_enabled else "normal")
        )
        if macro_enabled and macro_risk_mode != "normal":
            inferences.append(f"macro_risk_mode_{macro_risk_mode}")

        assumptions = list(cast(list[str], options.get("assumptions") or []))
        assumptions.append("fund_flows_are_public_derivatives_flow_proxies_not_custody_or_etf_settlement_flows")

        uncertainty: list[str] = []
        if not any(
            item.endswith("_public_options_mark_and_oi_available")
            for item in confirmed_facts
        ):
            uncertainty.append("public_options_surface_not_available_for_requested_underlyings")
        uncertainty.append(
            "startup_snapshot_cannot_prove_live_binance_runtime_end_to_end_"
            "without_real_runtime_evidence"
        )
        return {
            "confirmed_facts": confirmed_facts,
            "inferences": inferences,
            "assumptions": assumptions,
            "uncertainty": uncertainty,
        }

    async def _fetch_options_exchange_info(self, underlying_asset: str) -> list[dict[str, Any]]:
        asset = str(underlying_asset or "").strip().upper()
        now = time.monotonic()
        cached = self._options_exchange_cache.get(asset)
        if cached is not None and now - cached[0] < _OPTIONS_EXCHANGE_INFO_TTL_S:
            return cached[1]

        payload = await self._fetch_json("https://eapi.binance.com/eapi/v1/exchangeInfo")
        option_symbols_raw = payload.get("optionSymbols") if isinstance(payload, dict) else None
        option_symbols = option_symbols_raw if isinstance(option_symbols_raw, list) else []
        rows = [
            item
            for item in option_symbols
            if isinstance(item, dict)
            and str(item.get("status") or "").upper() == "TRADING"
            and str(item.get("underlying") or "").upper() == f"{asset}USDT"
        ]
        self._options_exchange_cache[asset] = (now, rows)
        return rows

    async def _fetch_options_open_interest(
        self,
        underlying_asset: str,
        expiry_code: str,
    ) -> list[dict[str, Any]]:
        payload = await self._fetch_json(
            "https://eapi.binance.com/eapi/v1/openInterest",
            params={
                "underlyingAsset": str(underlying_asset or "").strip().upper(),
                "expiration": str(expiry_code or "").strip(),
            },
        )
        if isinstance(payload, list):
            return [row for row in payload if isinstance(row, dict)]
        return []

    async def _fetch_options_mark_rows(self, underlying: str) -> list[dict[str, Any]]:
        payload = await self._fetch_json(
            "https://eapi.binance.com/eapi/v1/mark",
            params={"underlying": str(underlying or "").strip().upper()},
        )
        if isinstance(payload, list):
            return [row for row in payload if isinstance(row, dict)]
        return []

    async def _fetch_yahoo_chart_snapshot(self, symbol: str) -> dict[str, Any]:
        now = time.monotonic()
        cached = self._macro_cache.get(symbol)
        if cached is not None and now - cached[0] < _MACRO_TTL_S:
            return cached[1]

        try:
            payload = await self._fetch_json(
                f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}",
                params={"interval": "1d", "range": "5d"},
                headers={
                    "Accept": "application/json, text/plain, */*",
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/135.0.0.0 Safari/537.36"
                    ),
                },
            )
        except aiohttp.ClientResponseError as exc:
            snapshot = {
                "available": False,
                "symbol": symbol,
                "provider": "yahoo_chart_public",
                "reason": f"http_{exc.status}",
            }
            self._macro_cache[symbol] = (now, snapshot)
            return snapshot
        except (aiohttp.ClientError, TimeoutError) as exc:
            snapshot = {
                "available": False,
                "symbol": symbol,
                "provider": "yahoo_chart_public",
                "reason": exc.__class__.__name__.lower(),
            }
            self._macro_cache[symbol] = (now, snapshot)
            return snapshot
        result_rows = (
            cast(dict[str, Any], payload.get("chart") or {}).get("result")
            if isinstance(payload, dict)
            else None
        )
        if not result_rows:
            snapshot = {
                "available": False,
                "symbol": symbol,
                "provider": "yahoo_chart_public",
                "reason": "empty_payload",
            }
            self._macro_cache[symbol] = (now, snapshot)
            return snapshot

        result = result_rows[0]
        meta = cast(dict[str, Any], result.get("meta") or {})
        indicators = cast(dict[str, Any], result.get("indicators") or {})
        quote_rows = indicators.get("quote") if isinstance(indicators, dict) else None
        close_values = (
            quote_rows[0].get("close")
            if quote_rows and isinstance(quote_rows[0], dict)
            else []
        )
        closes = [_safe_float(item) for item in close_values or []]
        closes = [item for item in closes if item is not None]
        current = _safe_float(meta.get("regularMarketPrice"))
        previous = _safe_float(meta.get("chartPreviousClose"))
        if previous is None and len(closes) >= 2:
            previous = closes[-2]
        change_pct = None
        if current is not None and previous not in (None, 0.0):
            change_pct = ((current / cast(float, previous)) - 1.0) * 100.0

        snapshot = {
            "available": True,
            "symbol": symbol,
            "provider": "yahoo_chart_public",
            "current": current,
            "previous_close": previous,
            "change_pct": round(change_pct, 4) if change_pct is not None else None,
            "market_time": meta.get("regularMarketTime"),
            "exchange": meta.get("fullExchangeName") or meta.get("exchangeName"),
            "instrument_type": meta.get("instrumentType"),
        }
        self._macro_cache[symbol] = (now, snapshot)
        return snapshot

    async def _fetch_json(
        self,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        params: dict[str, Any] | None = None,
    ) -> Any:
        session = await self._client._get_http_session()
        async with session.get(
            url,
            headers=headers,
            params=params,
            timeout=aiohttp.ClientTimeout(
                total=max(5.0, float(self._settings.ws.rest_timeout_seconds))
            ),
        ) as response:
            response.raise_for_status()
            return await response.json()

    def _write_latest_snapshot(self, snapshot: dict[str, Any]) -> None:
        latest_json = self._reports_dir / "latest_public_intelligence.json"
        latest_md = self._reports_dir / "latest_public_intelligence.md"
        latest_json.write_text(
            _dump_json(snapshot, indent=2),
            encoding="utf-8",
        )
        latest_md.write_text(self._render_markdown(snapshot), encoding="utf-8")

    def _maybe_write_hourly_report(self, snapshot: dict[str, Any]) -> None:
        if not bool(self._settings.intelligence.write_hourly_reports):
            return
        ts = datetime.fromisoformat(str(snapshot["ts"]))
        hour_key = ts.astimezone(UTC).strftime("%Y%m%d_%H")
        if hour_key == self._last_report_hour_key:
            return
        self._last_report_hour_key = hour_key
        stamp = ts.astimezone(UTC).strftime("%Y%m%d_%H0000")
        json_path = self._reports_dir / f"hourly_public_intelligence_{stamp}.json"
        md_path = self._reports_dir / f"hourly_public_intelligence_{stamp}.md"
        json_path.write_text(
            _dump_json(snapshot, indent=2),
            encoding="utf-8",
        )
        md_path.write_text(self._render_markdown(snapshot), encoding="utf-8")

    def _render_markdown(self, snapshot: dict[str, Any]) -> str:
        barrier = cast(dict[str, Any], snapshot.get("barrier") or {})
        macro = cast(dict[str, Any], snapshot.get("macro") or {})
        options = cast(dict[str, Any], snapshot.get("options") or {})
        derivatives = cast(dict[str, Any], snapshot.get("derivatives") or {})
        harmonic = cast(dict[str, Any], snapshot.get("harmonic") or {})
        policy = cast(dict[str, Any], snapshot.get("policy") or {})

        lines = [
            f"# Public Intelligence {snapshot['ts']}",
            "",
            "## Scope",
            "- Binance public futures/options endpoints only for this phase.",
            f"- Runtime mode: `{snapshot.get('runtime_mode') or policy.get('runtime_mode') or 'unknown'}`.",
            f"- Source policy: `{snapshot.get('source_policy') or policy.get('source_policy') or 'unknown'}`.",
            (
                f"- Smart exit mode: `{snapshot.get('smart_exit_mode') or policy.get('smart_exit_mode') or 'unknown'}` "
                "analytical exit only; no order placement."
            ),
            (
                f"- Gamma semantics: `{snapshot.get('gamma_semantics') or policy.get('gamma_semantics') or 'unknown'}`; "
                "proxy only and not observed dealer inventory."
            ),
            "- Output optimized for bot telemetry and AI-agent debugging.",
            "",
            "## Policy",
            f"- Policy snapshot: `{_dump_json(policy, sort_keys=True)}`",
            "",
            "## Confirmed Facts",
        ]
        for item in cast(list[str], snapshot.get("confirmed_facts") or []):
            lines.append(f"- {item}")
        lines.extend(["", "## Inferences"])
        for item in cast(list[str], snapshot.get("inferences") or []):
            lines.append(f"- {item}")
        lines.extend(["", "## Assumptions"])
        for item in cast(list[str], snapshot.get("assumptions") or []):
            lines.append(f"- {item}")
        lines.extend(["", "## Uncertainty"])
        for item in cast(list[str], snapshot.get("uncertainty") or []):
            lines.append(f"- {item}")
        lines.extend(
            [
                "",
                "## Barrier",
                f"- Barrier snapshot: `{_dump_json(barrier, sort_keys=True)}`",
                "",
                "## Macro / External",
                f"- Macro snapshot: `{_dump_json(macro, sort_keys=True)}`",
                "",
                "## Options",
                f"- Options snapshot: `{_dump_json(options, sort_keys=True)}`",
                "",
                "## Derivatives",
                f"- Derivatives snapshot: `{_dump_json(derivatives, sort_keys=True)}`",
                "",
                "## Harmonic (Debug Only)",
                f"- Harmonic snapshot: `{_dump_json(harmonic, sort_keys=True)}`",
                "",
                "## JSON",
                "```json",
                _dump_json(snapshot, indent=2),
                "```",
                "",
            ]
        )
        return "\n".join(lines)
