from __future__ import annotations

import logging
from collections import Counter
from datetime import UTC, datetime
from typing import Any

from ..core.engine import StrategyDecision
from ..models import PipelineResult, Signal
from ..tracking import SignalTrackingEvent

LOG = logging.getLogger("bot.application.telemetry_manager")


class TelemetryManager:
    """Centralizes telemetry row building and emission for SignalBot."""

    def __init__(self, bot: Any) -> None:
        self._bot = bot

    def decision_to_reject_row(self, *, symbol: str, decision: StrategyDecision) -> dict[str, Any]:
        row: dict[str, Any] = {
            "ts": datetime.now(UTC).isoformat(),
            "symbol": symbol,
            "setup_id": decision.setup_id,
            "direction": getattr(decision.signal, "direction", "n/a"),
            "stage": decision.stage,
            "reason": decision.reason_code,
            "reason_code": decision.reason_code,
            "decision_status": decision.status,
        }
        if decision.details:
            row["details"] = decision.details
        if decision.missing_fields:
            row["missing_fields"] = list(decision.missing_fields)
        if decision.invalid_fields:
            row["invalid_fields"] = list(decision.invalid_fields)
        if decision.error is not None:
            row["error"] = decision.error
        return row

    def append_symbol_trace(self, *, symbol: str, row: dict[str, Any]) -> None:
        limit = int(getattr(getattr(self._bot.settings, "runtime", None), "diagnostic_trace_limit_per_symbol", 20))
        if limit <= 0 or not hasattr(self._bot.telemetry, "append_symbol_jsonl"):
            return
        count = self._bot._diagnostic_trace_counts.get(symbol, 0)
        if count >= limit:
            return
        self._bot._diagnostic_trace_counts[symbol] = count + 1
        self._bot.telemetry.append_symbol_jsonl("analysis", symbol, "strategy_traces.jsonl", row)

    def append_strategy_decision(
        self,
        *,
        symbol: str,
        trigger: str,
        decision: StrategyDecision,
    ) -> None:
        row: dict[str, Any] = {
            "ts": datetime.now(UTC).isoformat(),
            "symbol": symbol,
            "trigger": trigger,
            "setup_id": decision.setup_id,
            "status": decision.status,
            "stage": decision.stage,
            "reason": decision.reason_code,
            "reason_code": decision.reason_code,
            "missing_fields": list(decision.missing_fields),
            "invalid_fields": list(decision.invalid_fields),
        }
        if decision.signal is not None:
            row["direction"] = decision.signal.direction
            row["score"] = round(decision.signal.score, 4)
        if decision.details:
            row["details"] = decision.details
        if decision.error is not None:
            row["error"] = decision.error
        self._bot.telemetry.append_jsonl("strategy_decisions.jsonl", row)
        if decision.missing_fields or decision.invalid_fields:
            self._bot.telemetry.append_jsonl("data_quality.jsonl", row)
        if decision.status in {"signal", "reject", "error"}:
            self.append_symbol_trace(symbol=symbol, row=row)

    def emit_telemetry_mismatch(
        self,
        *,
        symbol: str,
        trigger: str,
        mismatch_type: str,
        expected: dict[str, Any],
        actual: dict[str, Any],
    ) -> None:
        self._bot.telemetry.append_jsonl(
            "telemetry_mismatch.jsonl",
            {
                "ts": datetime.now(UTC).isoformat(),
                "symbol": symbol,
                "trigger": trigger,
                "mismatch_type": mismatch_type,
                "expected": expected,
                "actual": actual,
            },
        )

    def emit_cycle_log(
        self,
        *,
        symbol: str,
        interval: str,
        event_ts: datetime,
        shortlist_size: int,
        tracking_events: list[SignalTrackingEvent],
        result: PipelineResult,
        candidates: list[Signal],
        rejected: list[dict[str, Any]],
        delivered: list[Signal] | None = None,
    ) -> None:
        delivered_count = len(delivered or [])
        delivery_status_counts = (
            dict(result.funnel.get("delivery_status_counts", {}))
            if isinstance(result.funnel, dict)
            else {}
        )
        delivery_sent_count = int(delivery_status_counts.get("sent", delivered_count))
        cycle_row: dict[str, Any] = {
            "ts": datetime.now(UTC).isoformat(),
            "mode": {
                "kline_close": "event_driven",
                "intra_candle": "intra_candle",
                "emergency_fallback": "emergency_fallback",
            }.get(result.trigger, result.trigger),
            "trigger": result.trigger,
            "event_symbol": symbol,
            "event_interval": interval,
            "event_ts": event_ts.isoformat(),
            "shortlist_size": shortlist_size,
            "detector_runs": result.raw_setups,
            "post_filter_candidates": len(candidates),
            "selected_signals": delivered_count,
            "raw_setups": result.raw_setups,
            "candidate_count": len(candidates),
            "selected_count": delivered_count,
            "rejected_count": len(rejected),
            "shortlist_source": self._bot._shortlist_source,
            "setup_counts": dict(Counter(s.setup_id for s in candidates)),
            "selected_setup_counts": dict(Counter(s.setup_id for s in (delivered or []))),
            "delivery_status_counts": delivery_status_counts,
            "tracking_events": [e.event_type for e in tracking_events],
            "dry_run": False,
            "status": result.status or ("ok" if not result.error else "error"),
            "prepare_error_count": self._bot._prepare_error_count,
        }
        if result.funnel:
            cycle_row["funnel"] = result.funnel
            if result.funnel.get("prepare_error_stage") is not None:
                cycle_row["prepare_error_stage"] = result.funnel.get("prepare_error_stage")
            if result.funnel.get("prepare_error_exception_type") is not None:
                cycle_row["prepare_error_exception_type"] = result.funnel.get("prepare_error_exception_type")
        if result.error:
            cycle_row["error"] = result.error
        if self._bot._ws_manager is not None:
            ws_snapshot = self._bot._ws_manager.state_snapshot()
            cycle_row.update(ws_snapshot if isinstance(ws_snapshot, dict) else {})
        rest_snapshot_func = getattr(self._bot.client, "state_snapshot", None)
        if callable(rest_snapshot_func):
            rest_snapshot = rest_snapshot_func()
            cycle_row.update(rest_snapshot if isinstance(rest_snapshot, dict) else {})

        self._bot.telemetry.append_jsonl("cycles.jsonl", cycle_row)
        symbol_row: dict[str, Any] = {
            "ts": datetime.now(UTC).isoformat(),
            "symbol": symbol,
            "event_ts": event_ts.isoformat(),
            "status": result.status or ("ok" if not result.error else "error"),
            "error": result.error,
            "detector_runs": result.raw_setups,
            "post_filter_candidates": len(candidates),
            "selected_signals": delivered_count,
            "raw_setups": result.raw_setups,
            "candidates": len(candidates),
            "delivered": delivered_count,
            "rejected": len(rejected),
            "shortlist_source": self._bot._shortlist_source,
            "delivery_status_counts": delivery_status_counts,
        }
        if result.prepared is not None:
            symbol_row.update(
                {
                    "work_rows_15m": int(result.prepared.work_15m.height) if result.prepared.work_15m is not None else 0,
                    "work_rows_1h": int(result.prepared.work_1h.height) if result.prepared.work_1h is not None else 0,
                    "work_rows_5m": int(result.prepared.work_5m.height) if result.prepared.work_5m is not None else 0,
                    "work_rows_4h": int(result.prepared.work_4h.height) if result.prepared.work_4h is not None else 0,
                    "spread_bps": result.prepared.spread_bps,
                    "bias_4h": result.prepared.bias_4h,
                    "bias_1h": result.prepared.bias_1h,
                    "market_regime": result.prepared.market_regime,
                    "context_snapshot_age_seconds": result.prepared.context_snapshot_age_seconds,
                    "data_source_mix": result.prepared.data_source_mix,
                    "mark_index_spread_bps": result.prepared.mark_index_spread_bps,
                    "premium_zscore_5m": result.prepared.premium_zscore_5m,
                    "premium_slope_5m": result.prepared.premium_slope_5m,
                    "oi_slope_5m": result.prepared.oi_slope_5m,
                    "top_vs_global_ls_gap": result.prepared.top_vs_global_ls_gap,
                }
            )
        if result.funnel:
            symbol_row["funnel"] = result.funnel
            if result.funnel.get("prepare_error_stage") is not None:
                symbol_row["prepare_error_stage"] = result.funnel.get("prepare_error_stage")
            if result.funnel.get("prepare_error_exception_type") is not None:
                symbol_row["prepare_error_exception_type"] = result.funnel.get("prepare_error_exception_type")
        self._bot.telemetry.append_jsonl("symbol_analysis.jsonl", symbol_row)
        if delivery_sent_count != delivered_count:
            self.emit_telemetry_mismatch(
                symbol=symbol,
                trigger=result.trigger,
                mismatch_type="delivery_sent_vs_symbol_analysis",
                expected={"sent_delivery_rows": delivery_sent_count},
                actual={"symbol_analysis_delivered": delivered_count},
            )
        LOG.info(
            "cycle | symbol=%s detector_runs=%d candidates=%d delivered=%d rejected=%d status=%s",
            symbol, result.raw_setups, len(candidates),
            delivered_count, len(rejected), result.status or "ok",
        )
