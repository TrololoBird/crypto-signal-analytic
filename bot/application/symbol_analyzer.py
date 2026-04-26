from __future__ import annotations

import asyncio
import logging
from dataclasses import replace
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast

from bot.core.engine import StrategyDecision
from bot.features import min_required_bars, prepare_symbol
from bot.filters import apply_global_filters
from bot.market_data import BinanceFuturesMarketData, MarketDataUnavailable
from bot.models import PipelineResult, PreparedSymbol, Signal, SymbolFrames, UniverseSymbol

if TYPE_CHECKING:
    from bot.application.bot import SignalBot


LOG = logging.getLogger("bot.application.bot")


class SymbolAnalyzer:
    def __init__(self, bot: SignalBot) -> None:
        self._bot = bot


    async def run_modern_analysis(
        self,
        item: UniverseSymbol,
        frames: SymbolFrames,
        trigger: str = "modern_engine",
        event_ts: datetime | None = None,
        ws_enrichments: dict[str, Any] | None = None,
    ) -> PipelineResult:
        """Run modern SignalEngine analysis for a symbol.

        Replaces legacy SignalPipeline.process_symbol().

        Returns:
            PipelineResult compatible with legacy pipeline output
        """
        event_ts = event_ts or datetime.now(UTC)
        candidates: list[Signal] = []
        rejected: list[dict[str, Any]] = []
        prepared: PreparedSymbol | None = None
        funnel: dict[str, Any] = {
            "shortlist_entered": True,
            "frame_rows": {},
            "frame_readiness": {},
            "detector_runs": 0,
            "post_filter_candidates": 0,
            "raw_hits": 0,
            "raw_hits_by_setup": {},
            "strategy_rejects_by_setup": {},
            "family_precheck_rejects": 0,
            "alignment_penalties": 0,
            "confirmation_rejects": 0,
            "filters_rejects": 0,
            "selected": 0,
            "delivered": 0,
        }

        LOG.info("%s: starting modern analysis | trigger=%s", item.symbol, trigger)
        item = self._bot._refresh_universe_symbol_from_ws(item)

        minimums = min_required_bars(
            min_bars_15m=self._bot.settings.filters.min_bars_15m,
            min_bars_1h=self._bot.settings.filters.min_bars_1h,
            min_bars_4h=self._bot.settings.filters.min_bars_4h,
        )
        rows_4h = frames.df_4h.height if frames.df_4h is not None else 0
        rows_5m = frames.df_5m.height if frames.df_5m is not None else 0
        rows_1h = frames.df_1h.height
        rows_15m = frames.df_15m.height
        funnel["frame_rows"] = {
            "15m": rows_15m,
            "1h": rows_1h,
            "5m": rows_5m,
            "4h": rows_4h,
        }
        funnel["frame_readiness"] = {
            "15m": rows_15m >= minimums["15m"],
            "1h": rows_1h >= minimums["1h"],
            "5m": rows_5m >= minimums["5m"],
            "4h": rows_4h >= minimums["4h"],
        }
        if rows_1h < minimums["1h"] or rows_15m < minimums["15m"]:
            missing_required = []
            if rows_15m < minimums["15m"]:
                missing_required.append("15m")
            if rows_1h < minimums["1h"]:
                missing_required.append("1h")
            rejected.append(
                {
                    "ts": datetime.now(UTC).isoformat(),
                    "symbol": item.symbol,
                    "setup_id": "data",
                    "direction": "n/a",
                    "stage": "data",
                    "reason": "insufficient_required_history",
                    "rows_1h": rows_1h,
                    "rows_15m": rows_15m,
                    "rows_5m": rows_5m,
                    "rows_4h": rows_4h,
                    "need_1h": minimums["1h"],
                    "need_15m": minimums["15m"],
                    "need_5m": minimums["5m"],
                    "need_4h": minimums["4h"],
                    "missing_required_frames": missing_required,
                }
            )
            LOG.warning(
                "%s: insufficient required history for analysis | 15m=%d/%d 1h=%d/%d optional_5m=%d/%d optional_4h=%d/%d",
                item.symbol,
                rows_15m,
                minimums["15m"],
                rows_1h,
                minimums["1h"],
                rows_5m,
                minimums["5m"],
                rows_4h,
                minimums["4h"],
            )
            return PipelineResult(
                symbol=item.symbol,
                trigger=trigger,
                event_ts=event_ts,
                raw_setups=0,
                candidates=candidates,
                rejected=rejected,
                status="insufficient_required_history",
                prepared=None,
                funnel=funnel,
            )

        try:
            # Build prepared symbol using modern prepare_symbol
            prepared = prepare_symbol(
                item,
                frames,
                minimums=minimums,
                settings=self._bot.settings,
            )
            if prepared is not None and ws_enrichments:
                for key, value in ws_enrichments.items():
                    if hasattr(prepared, key):
                        setattr(prepared, key, value)
                # Debug: log enrichment status
                if ws_enrichments.get("mark_index_spread_bps") is not None:
                    LOG.debug("%s: enrichment mark_index_spread_bps=%.4f", item.symbol, ws_enrichments["mark_index_spread_bps"])
                else:
                    LOG.debug("%s: enrichment mark_index_spread_bps=None (ws_data_missing)", item.symbol)
            LOG.debug("%s: prepared symbol built | work_15m_rows=%s work_1h_rows=%s",
                      item.symbol,
                      prepared.work_15m.height if prepared is not None and prepared.work_15m is not None else 0,
                      prepared.work_1h.height if prepared is not None and prepared.work_1h is not None else 0)
        except Exception as exc:
            self._bot._prepare_error_count += 1
            self._bot._last_prepare_error = {
                "ts": datetime.now(UTC).isoformat(),
                "symbol": item.symbol,
                "stage": "prepare_symbol",
                "exception_type": type(exc).__name__,
                "error": str(exc),
            }
            funnel["prepare_error_stage"] = "prepare_symbol"
            funnel["prepare_error_exception_type"] = type(exc).__name__
            LOG.warning("%s: failed to build prepared symbol: %s", item.symbol, exc)
            return PipelineResult(
                symbol=item.symbol,
                trigger=trigger,
                event_ts=event_ts,
                raw_setups=0,
                candidates=candidates,
                rejected=rejected,
                error=str(exc),
                status="prepare_error",
                prepared=prepared,
                funnel=funnel,
            )

        # Run modern engine (replaces pipeline analysis)
        if prepared is None:
            LOG.warning("%s: prepared symbol is None", item.symbol)
            return PipelineResult(
                symbol=item.symbol,
                trigger=trigger,
                event_ts=event_ts,
                raw_setups=0,
                candidates=candidates,
                rejected=rejected,
                status="prepare_failed",
                prepared=None,
                funnel=funnel,
            )

        # Log engine stats before calculation
        engine_stats = self._bot._modern_engine.get_engine_stats()
        LOG.debug("%s: engine stats | enabled_strategies=%d total=%d",
                  item.symbol, engine_stats.get('enabled_strategies', 0),
                  engine_stats.get('total_strategies', 0))
        self._bot._diagnostic_trace_counts[item.symbol] = 0

        try:
            signal_results = await self._bot._modern_engine.calculate_all(prepared)
            funnel["detector_runs"] = len(signal_results)
            LOG.debug("%s: engine calculated | results_count=%d", item.symbol, len(signal_results))
        except Exception as exc:
            LOG.warning("%s: modern engine calculation failed: %s", item.symbol, exc)
            return PipelineResult(
                symbol=item.symbol,
                trigger=trigger,
                event_ts=event_ts,
                raw_setups=0,
                candidates=candidates,
                rejected=rejected,
                error=str(exc),
                status="engine_error",
                prepared=prepared,
                funnel=funnel,
            )

        # Process results: convert SignalResult to Signal, then apply the
        # production hard-gate + confluence path before a signal can become a
        # runtime candidate.
        signals_found = 0
        signals_rejected_perf = 0
        signals_added = 0

        for result in signal_results:
            setup_id = result.setup_id or result.metadata.get("setup_id") or getattr(result.signal, "setup_id", "unknown")
            decision = result.decision
            if decision is None:
                decision = StrategyDecision.error_result(
                    setup_id=setup_id,
                    reason_code="runtime.missing_decision",
                    error=result.error or "missing strategy decision",
                    stage="engine",
                    details={"symbol": item.symbol},
                )
            self._bot._append_strategy_decision_telemetry(
                symbol=item.symbol,
                trigger=trigger,
                decision=decision,
            )
            if decision.is_error or decision.is_skip or decision.is_reject:
                funnel["strategy_rejects_by_setup"][setup_id] = (
                    funnel["strategy_rejects_by_setup"].get(setup_id, 0) + 1
                )
                rejected.append(self._bot._decision_to_reject_row(symbol=item.symbol, decision=decision))
                LOG.debug(
                    "%s: strategy produced no signal | setup=%s status=%s reason=%s",
                    item.symbol,
                    setup_id,
                    decision.status,
                    decision.reason_code,
                )
                continue

            signal = decision.signal or result.signal
            if signal is None:
                fallback_decision = StrategyDecision.reject(
                    setup_id=setup_id,
                    stage="strategy",
                    reason_code="runtime.signal_missing_after_hit",
                    details={"symbol": item.symbol},
                )
                funnel["strategy_rejects_by_setup"][setup_id] = (
                    funnel["strategy_rejects_by_setup"].get(setup_id, 0) + 1
                )
                rejected.append(self._bot._decision_to_reject_row(symbol=item.symbol, decision=fallback_decision))
                continue

            setup_id = signal.setup_id
            metadata = self._bot._strategy_metadata(setup_id)
            signal = self._bot._apply_strategy_metadata(signal, metadata)

            precheck_ok, precheck_reason, precheck_details = self._bot._check_family_precheck(
                signal,
                prepared,
                metadata,
            )
            if not precheck_ok:
                rejected.append({
                    "ts": datetime.now(UTC).isoformat(),
                    "symbol": item.symbol,
                    "setup_id": signal.setup_id,
                    "direction": signal.direction,
                    "stage": "family_precheck",
                    "reason": precheck_reason or "family_precheck_reject",
                    "details": precheck_details,
                })
                funnel["family_precheck_rejects"] += 1
                continue

            signal, alignment_details = self._bot._apply_alignment_penalty(signal, prepared, metadata)
            if alignment_details.get("applied"):
                funnel["alignment_penalties"] += 1

            signals_found += 1
            funnel["raw_hits"] += 1
            funnel["raw_hits_by_setup"][signal.setup_id] = (
                funnel["raw_hits_by_setup"].get(signal.setup_id, 0) + 1
            )

            ltf_ok, ltf_reason, ltf_details = self._bot._check_family_confirmation(signal, prepared, metadata)
            if not ltf_ok:
                rejected.append({
                    "ts": datetime.now(UTC).isoformat(),
                    "symbol": item.symbol,
                    "setup_id": signal.setup_id,
                    "direction": signal.direction,
                    "stage": "confirmation",
                    "reason": ltf_reason or "5m_confirmation_reject",
                    "details": ltf_details,
                })
                funnel["confirmation_rejects"] += 1
                continue
            
            # Check performance guard using modern repo
            score_adj = await self._bot._modern_repo.get_setup_score_adjustment(signal.setup_id)
            if score_adj < -0.3:  # Suppressed due to poor performance
                rejected.append({
                    "ts": datetime.now(UTC).isoformat(),
                    "symbol": item.symbol,
                    "setup_id": signal.setup_id,
                    "direction": signal.direction,
                    "stage": "perf_guard",
                    "reason": "setup_underperforming",
                })
                signals_rejected_perf += 1
                continue

            passed, filtered_signal, filter_reason, scoring_result, filter_details = apply_global_filters(
                signal,
                prepared,
                self._bot.settings,
                self._bot.confluence,
            )
            if not passed:
                reject_row: dict[str, Any] = {
                    "ts": datetime.now(UTC).isoformat(),
                    "symbol": item.symbol,
                    "setup_id": signal.setup_id,
                    "direction": signal.direction,
                    "stage": "filters",
                    "reason": filter_reason or "filter_rejected",
                }
                if scoring_result is not None:
                    scoring_payload = scoring_result.to_dict()
                    scoring_payload["setup_id"] = signal.setup_id
                    reject_row["scoring"] = scoring_payload
                if filter_details:
                    reject_row["details"] = filter_details
                rejected.append(reject_row)
                funnel["filters_rejects"] += 1
                continue

            candidates.append(filtered_signal)
            signals_added += 1
            LOG.debug("%s: candidate signal | setup=%s dir=%s score=%.3f rr=%.2f",
                      item.symbol, filtered_signal.setup_id, filtered_signal.direction,
                      filtered_signal.score, filtered_signal.risk_reward or 0)

        LOG.info("%s: analysis complete | trigger=%s raw_strategies=%d signals_found=%d perf_rejected=%d candidates=%d",
                 item.symbol, trigger, len(signal_results), signals_found,
                 signals_rejected_perf, signals_added)
        funnel["post_filter_candidates"] = len(candidates)

        return PipelineResult(
            symbol=item.symbol,
            trigger=trigger,
            event_ts=event_ts,
            raw_setups=len(signal_results),
            candidates=candidates,
            rejected=rejected,
            status="no_setups" if len(signal_results) == 0 else "ok",
            prepared=prepared,
            funnel=funnel,
        )


    async def fetch_frames(self, item: UniverseSymbol) -> SymbolFrames | None:
        symbol = item.symbol
        minimums = min_required_bars(
            min_bars_15m=self._bot.settings.filters.min_bars_15m,
            min_bars_1h=self._bot.settings.filters.min_bars_1h,
            min_bars_4h=self._bot.settings.filters.min_bars_4h,
        )

        ws_5m = ws_15m = ws_1h = None
        ws_bid = ws_ask = None
        if self._bot._ws_manager is not None:
            ws_frames = await self._bot._ws_manager.get_symbol_frames(symbol)
            if ws_frames is not None:
                ws_5m = ws_frames.df_5m
                ws_15m = ws_frames.df_15m
                ws_1h = ws_frames.df_1h
                ws_bid = ws_frames.bid_price
                ws_ask = ws_frames.ask_price

        try:
            if isinstance(self._bot.client, BinanceFuturesMarketData):
                df_4h = await self._bot.client.fetch_klines_cached(symbol, "4h", limit=240)
                df_1h = ws_1h if ws_1h is not None and ws_1h.height >= minimums["1h"] else await self._bot.client.fetch_klines_cached(symbol, "1h", limit=240)
                df_15m = ws_15m if ws_15m is not None and ws_15m.height >= minimums["15m"] else await self._bot.client.fetch_klines_cached(symbol, "15m", limit=240)
                df_5m = ws_5m if ws_5m is not None and ws_5m.height >= minimums["5m"] else await self._bot.client.fetch_klines_cached(symbol, "5m", limit=240)

                bid, ask = ws_bid, ws_ask
                if bid is None or ask is None:
                    bid, ask = await self._bot.client.fetch_book_ticker(symbol)

                return SymbolFrames(
                    symbol=symbol,
                    df_1h=df_1h,
                    df_15m=df_15m,
                    bid_price=bid,
                    ask_price=ask,
                    df_5m=df_5m,
                    df_4h=df_4h,
                )

            return await cast(Any, self._bot.client.fetch_symbol_frames(symbol))
        except (MarketDataUnavailable, Exception) as exc:
            LOG.warning("frame fetch failed for %s: %s", symbol, exc)
            return None

    async def preload_shortlist_frames(self) -> None:
        await asyncio.sleep(1.0)
        if not isinstance(self._bot.client, BinanceFuturesMarketData):
            return
        async with self._bot._shortlist_lock:
            shortlist = list(self._bot._shortlist)
        if not shortlist:
            return

        batch_size = int(self._bot.settings.runtime.startup_batch_size)
        batch_delay = float(self._bot.settings.runtime.startup_batch_delay_seconds)
        sem = asyncio.Semaphore(int(self._bot.settings.runtime.max_concurrent_rest_requests))

        async def _preload_one(symbol: str) -> None:
            async with sem:
                try:
                    await self._bot.client.fetch_klines_cached(symbol, "5m", limit=240)
                    await self._bot.client.fetch_klines_cached(symbol, "1h", limit=240)
                    await self._bot.client.fetch_klines_cached(symbol, "15m", limit=240)
                    await self._bot.client.fetch_klines_cached(symbol, "4h", limit=240)
                except Exception:
                    pass

        for i in range(0, len(shortlist), batch_size):
            batch = shortlist[i : i + batch_size]
            await asyncio.gather(*[_preload_one(item.symbol) for item in batch], return_exceptions=True)
            if i + batch_size < len(shortlist):
                await asyncio.sleep(batch_delay)

    def ws_cache_enrichments(self, symbol: str) -> dict[str, Any]:
        enrichments: dict[str, Any] = {}
        context_ages: list[float] = []
        if self._bot._ws_manager is not None:
            try:
                ticker = self._bot._ws_manager.get_ticker_snapshot(symbol)
                ticker_age = self._bot._ws_manager.get_ticker_age_seconds(symbol)
                if ticker:
                    ticker_price = float(ticker.get("last_price") or 0.0)
                    if ticker_price > 0:
                        enrichments["ticker_price"] = ticker_price
                    if ticker_age is not None:
                        enrichments["ticker_price_age_seconds"] = ticker_age
                        context_ages.append(ticker_age)
            except Exception:
                pass

        if isinstance(self._bot.client, BinanceFuturesMarketData):
            oi_chg = self._bot.client.get_cached_oi_change(symbol)
            if oi_chg is not None:
                enrichments["oi_change_pct"] = oi_chg
            ls = self._bot.client.get_cached_ls_ratio(symbol)
            if ls is not None:
                enrichments["ls_ratio"] = ls
            taker = self._bot.client.get_cached_taker_ratio(symbol)
            if taker is not None:
                enrichments["taker_ratio"] = taker

        if context_ages:
            enrichments["context_snapshot_age_seconds"] = max(context_ages)
        enrichments.setdefault("data_source_mix", "futures_only")
        return enrichments

    def refresh_universe_symbol_from_ws(self, item: UniverseSymbol) -> UniverseSymbol:
        if self._bot._ws_manager is None:
            return item
        ticker = self._bot._ws_manager.get_ticker_snapshot(item.symbol)
        ticker_age = self._bot._ws_manager.get_ticker_age_seconds(item.symbol)
        if (
            not ticker
            or ticker_age is None
            or ticker_age > self._bot.settings.ws.market_ticker_freshness_seconds
        ):
            return item

        next_last_price = item.last_price
        try:
            ticker_last_price = float(ticker.get("last_price") or 0.0)
        except (TypeError, ValueError):
            return item
        if ticker_last_price > 0:
            next_last_price = ticker_last_price

        if next_last_price == item.last_price:
            return item
        return replace(item, last_price=next_last_price)

    async def ws_enrich(self, result: PipelineResult) -> None:
        if result.prepared is None:
            return
        p = result.prepared
        try:
            p.oi_current = await self._bot.client.fetch_open_interest(p.universe.symbol)
            p.oi_change_pct = await self._bot.client.fetch_open_interest_change(
                p.universe.symbol,
                period="1h",
            )
            p.oi_slope_5m = await self._bot.client.fetch_open_interest_change(
                p.universe.symbol,
                period="5m",
            )
        except Exception:
            pass
