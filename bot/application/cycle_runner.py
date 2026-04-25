"""Signal cycle execution helpers for SignalBot."""
from __future__ import annotations

import asyncio
import logging
from collections import Counter
from datetime import datetime, timezone
from typing import Any

from ..models import PipelineResult, PreparedSymbol, Signal, UniverseSymbol

UTC = timezone.utc
LOG = logging.getLogger("bot.application.cycle_runner")


class CycleRunner:
    """Encapsulates signal-cycle execution while SignalBot keeps orchestration."""

    def __init__(self, bot: Any) -> None:
        self._bot = bot

    async def execute_symbol_cycle(
        self,
        *,
        symbol: str,
        item: UniverseSymbol,
        interval: str,
        trigger: str,
        event_ts: datetime,
        tracking_events: list[Any],
        shortlist_size: int,
        ws_enrichments_override: dict[str, Any] | None = None,
    ) -> None:
        bot = self._bot
        frames = await bot._fetch_frames(item)
        if frames is None:
            return

        ws_enrichments = dict(bot._ws_cache_enrichments(symbol))
        if ws_enrichments_override:
            ws_enrichments.update(ws_enrichments_override)

        async with bot._analysis_semaphore:
            result = await bot._run_modern_analysis(
                item,
                frames,
                trigger=trigger,
                event_ts=event_ts,
                ws_enrichments=ws_enrichments,
            )

        task = asyncio.create_task(bot._ws_enrich(result), name=f"ws_enrich:{symbol}")
        bot._background_tasks.add(task)
        task.add_done_callback(bot._background_tasks.discard)

        candidates, rejected, delivered = await bot._select_and_deliver_for_symbol(symbol, result)

        for row in rejected:
            bot.telemetry.append_jsonl("rejected.jsonl", row)
        for sig in candidates:
            bot.telemetry.append_jsonl(
                "candidates.jsonl",
                {"ts": datetime.now(UTC).isoformat(), **sig.to_log_row()},
            )
        for sig in delivered:
            bot.telemetry.append_jsonl(
                "selected.jsonl",
                {"ts": datetime.now(UTC).isoformat(), **sig.to_log_row()},
            )

        bot._emit_cycle_log(
            symbol=symbol,
            interval=interval,
            event_ts=event_ts,
            shortlist_size=shortlist_size,
            tracking_events=tracking_events,
            result=result,
            candidates=candidates,
            rejected=rejected,
            delivered=delivered,
        )

    async def run_emergency_cycle(self) -> dict[str, Any]:
        bot = self._bot
        tracking_events = await bot.tracker.review_open_signals(dry_run=False)
        if tracking_events:
            await bot._deliver_tracking(tracking_events)

        async with bot._shortlist_lock:
            shortlist = list(bot._shortlist)
        if not shortlist:
            shortlist = await bot._do_refresh_shortlist()

        semaphore = asyncio.Semaphore(bot.settings.runtime.analysis_concurrency)

        async def _analyze_one(item: UniverseSymbol) -> PipelineResult | None:
            async with semaphore:
                frames = await bot._fetch_frames(item)
                if frames is None:
                    return None
                ws_enrichments = bot._ws_cache_enrichments(item.symbol)
                result = await bot._run_modern_analysis(
                    item,
                    frames,
                    trigger="emergency_fallback",
                    ws_enrichments=ws_enrichments,
                )
                asyncio.create_task(bot._ws_enrich(result), name=f"ws_enrich:{item.symbol}")
                return result

        tasks = [asyncio.create_task(_analyze_one(item)) for item in shortlist]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        pipeline_results: list[PipelineResult] = []
        all_candidates: dict[str, list[Signal]] = {}
        all_rejected: list[dict[str, Any]] = []
        rejected_by_symbol: dict[str, list[dict[str, Any]]] = {}
        bias_counter: Counter[str] = Counter()

        for res in results:
            if res is None or isinstance(res, Exception) or not isinstance(res, PipelineResult):
                continue
            pipeline_results.append(res)
            rejected_by_symbol.setdefault(res.symbol, [])
            rejected_by_symbol[res.symbol].extend(res.rejected)
            all_rejected.extend(res.rejected)
            for row in res.rejected:
                bot.telemetry.append_jsonl("rejected.jsonl", row)
            if res.error:
                continue
            all_candidates[res.symbol] = res.candidates
            if res.prepared is not None:
                bias_counter[res.prepared.bias_4h] += 1

        prepared_by_tracking_id: dict[str, PreparedSymbol] = {}
        for res in pipeline_results:
            if res.prepared is None:
                continue
            for signal in res.candidates:
                prepared_by_tracking_id[signal.tracking_id] = res.prepared

        selected = bot._select_and_rank(
            all_candidates,
            max_signals=bot.settings.runtime.max_signals_per_cycle,
        )
        delivered, cooldown_rejected, delivery_status_counts = await bot._select_and_deliver(
            selected,
            prepared_by_tracking_id=prepared_by_tracking_id,
        )
        all_rejected.extend(cooldown_rejected)
        for row in cooldown_rejected:
            bot.telemetry.append_jsonl("rejected.jsonl", row)
            rejected_by_symbol.setdefault(str(row.get("symbol") or "unknown"), []).append(row)

        selected_by_symbol: dict[str, list[Signal]] = {}
        for signal in selected:
            selected_by_symbol.setdefault(signal.symbol, []).append(signal)

        delivered_by_symbol: dict[str, list[Signal]] = {}
        for signal in delivered:
            delivered_by_symbol.setdefault(signal.symbol, []).append(signal)

        delivery_status_counts_by_symbol: dict[str, Counter[str]] = {}
        for signal in delivered:
            counter = delivery_status_counts_by_symbol.setdefault(signal.symbol, Counter())
            counter["sent"] += 1
        for res in pipeline_results:
            if res.funnel:
                res.funnel["selected"] = len(selected_by_symbol.get(res.symbol, []))
                res.funnel["delivered"] = len(delivered_by_symbol.get(res.symbol, []))
                res.funnel["delivery_status_counts"] = dict(delivery_status_counts_by_symbol.get(res.symbol, Counter()))

        now_ts = datetime.now(UTC).isoformat()
        for candidates in all_candidates.values():
            for sig in candidates:
                bot.telemetry.append_jsonl(
                    "candidates.jsonl",
                    {"ts": now_ts, **sig.to_log_row()},
                )
        for sig in delivered:
            bot.telemetry.append_jsonl(
                "selected.jsonl",
                {"ts": now_ts, **sig.to_log_row()},
            )
        for res in pipeline_results:
            bot._emit_cycle_log(
                symbol=res.symbol,
                interval="emergency_fallback",
                event_ts=res.event_ts,
                shortlist_size=len(shortlist),
                tracking_events=[],
                result=res,
                candidates=res.candidates,
                rejected=rejected_by_symbol.get(res.symbol, []),
                delivered=delivered_by_symbol.get(res.symbol, []),
            )

        summary = {
            "shortlist_size": len(shortlist),
            "detector_runs": sum(r.raw_setups for r in pipeline_results),
            "post_filter_candidates": sum(len(r.candidates) for r in pipeline_results),
            "selected_signals": len(delivered),
            "raw_setups": sum(r.raw_setups for r in pipeline_results),
            "candidates": sum(len(r.candidates) for r in pipeline_results),
            "selected": len(delivered),
            "rejected": len(all_rejected),
            "bias": dict(bias_counter),
            "delivery_status_counts": dict(delivery_status_counts),
        }
        bot.last_cycle_summary = summary
        LOG.info("emergency cycle | %s", " ".join(f"{k}={v}" for k, v in summary.items()))
        return summary
