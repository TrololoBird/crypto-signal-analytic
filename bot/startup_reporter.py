from __future__ import annotations

import asyncio
import html
import json
import os
import re
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .config import BotSettings, load_settings
from .core.memory import MemoryRepository
from .journal import build_config_suggestions
from .messaging import TelegramBroadcaster
from .tracked_signals import parse_state_dt


UTC = timezone.utc
MSK = timezone(timedelta(hours=3))
_SESSION_MARKER_RE = re.compile(r"BOT SESSION STARTED \| (?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} UTC)")


@dataclass(slots=True)
class StartupReportContext:
    repo_root: Path
    event: str
    report_ts: datetime
    bot_dir: Path
    logs_dir: Path
    telemetry_dir: Path
    reports_dir: Path
    latest_report_path: Path
    bot_log: Path
    cycles_file: Path
    shortlist_file: Path
    selected_file: Path
    rejected_file: Path
    symbol_analysis_file: Path
    delivery_file: Path
    strategy_decisions_file: Path
    data_quality_file: Path
    telemetry_mismatch_file: Path
    tracking_events_file: Path
    health_file: Path
    db_path: Path


@dataclass(slots=True)
class StartupReportResult:
    report_path: Path
    json_path: Path
    markdown: str
    telegram_message: str
    structured_summary: dict[str, Any]


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            raw = line.strip()
            if not raw:
                continue
            try:
                item = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if isinstance(item, dict):
                rows.append(item)
    return rows


def _read_text_tail(path: Path, *, lines: int = 40) -> str:
    if not path.exists():
        return ""
    content = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    if not content:
        return ""
    return "\n".join(content[-lines:])


def _load_runtime_policy(repo_root: Path) -> dict[str, Any]:
    config_path = repo_root / "config.toml"
    try:
        settings = load_settings(config_path)
    except Exception:
        return {
            "config_loaded": False,
            "runtime_mode": None,
            "source_policy": None,
            "smart_exit_mode": None,
            "gamma_semantics": None,
            "max_consecutive_stop_losses": None,
            "stop_loss_pause_hours": None,
        }
    intelligence = settings.intelligence
    return {
        "config_loaded": True,
        "runtime_mode": intelligence.runtime_mode,
        "source_policy": intelligence.source_policy,
        "smart_exit_mode": intelligence.smart_exit_mode,
        "gamma_semantics": intelligence.gamma_semantics,
        "max_consecutive_stop_losses": intelligence.max_consecutive_stop_losses,
        "stop_loss_pause_hours": intelligence.stop_loss_pause_hours,
    }


def _parse_dt(raw: str | None) -> datetime | None:
    if not raw:
        return None
    try:
        value = datetime.fromisoformat(str(raw))
    except ValueError:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _analysis_run_has_data(run_dir: Path) -> bool:
    analysis_dir = run_dir / "analysis"
    if not analysis_dir.exists():
        return False
    interesting_files = (
        analysis_dir / "strategy_decisions.jsonl",
        analysis_dir / "symbol_analysis.jsonl",
        analysis_dir / "rejected.jsonl",
        analysis_dir / "cycles.jsonl",
    )
    for path in interesting_files:
        if not path.exists():
            continue
        try:
            with path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    if line.strip():
                        return True
        except OSError:
            continue
    return False


def _load_context(repo_root: Path, event: str, report_ts: datetime) -> StartupReportContext:
    bot_dir = repo_root / "data" / "bot"
    reports_dir = bot_dir / "session" / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    # TelemetryStore writes to `telemetry/runs/<run_id>/analysis/*`. Startup report
    # should analyze the latest completed run, not a hard-coded legacy path.
    telemetry_root = bot_dir / "telemetry"
    runs_dir = telemetry_root / "runs"
    latest_run_dir: Path | None = None
    if runs_dir.exists():
        try:
            candidates = [p for p in runs_dir.iterdir() if p.is_dir()]
            candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
            for p in candidates:
                if _analysis_run_has_data(p):
                    latest_run_dir = p
                    break
            if latest_run_dir is None:
                for p in candidates:
                    if (p / "analysis").exists():
                        latest_run_dir = p
                        break
        except OSError:
            latest_run_dir = None

    analysis_dir = (latest_run_dir / "analysis") if latest_run_dir else (telemetry_root / "analysis")

    # Logs: runtime uses per-session `bot_*.log`. Prefer the newest file.
    logs_dir = bot_dir / "logs"
    bot_log = logs_dir / "bot.log"
    if logs_dir.exists():
        try:
            log_files = sorted(
                [p for p in logs_dir.glob("bot_*.log") if p.is_file()],
                key=lambda p: p.stat().st_mtime,
                reverse=True,
            )
            if log_files:
                bot_log = log_files[0]
        except OSError:
            pass

    return StartupReportContext(
        repo_root=repo_root,
        event=event,
        report_ts=report_ts,
        bot_dir=bot_dir,
        logs_dir=logs_dir,
        telemetry_dir=analysis_dir,
        reports_dir=reports_dir,
        latest_report_path=reports_dir / "latest_startup_report.md",
        bot_log=bot_log,
        cycles_file=analysis_dir / "cycles.jsonl",
        shortlist_file=analysis_dir / "shortlist.jsonl",
        selected_file=analysis_dir / "selected.jsonl",
        rejected_file=analysis_dir / "rejected.jsonl",
        symbol_analysis_file=analysis_dir / "symbol_analysis.jsonl",
        delivery_file=analysis_dir / "delivery.jsonl",
        strategy_decisions_file=analysis_dir / "strategy_decisions.jsonl",
        data_quality_file=analysis_dir / "data_quality.jsonl",
        telemetry_mismatch_file=analysis_dir / "telemetry_mismatch.jsonl",
        tracking_events_file=analysis_dir / "tracking_events.jsonl",
        health_file=analysis_dir / "health.jsonl",
        db_path=bot_dir / "bot.db",
    )


def _previous_session_start(bot_log: Path) -> datetime | None:
    if not bot_log.exists():
        return None
    latest: datetime | None = None
    for line in bot_log.read_text(encoding="utf-8", errors="ignore").splitlines():
        match = _SESSION_MARKER_RE.search(line)
        if not match:
            continue
        try:
            value = datetime.strptime(match.group("ts"), "%Y-%m-%d %H:%M:%S UTC").replace(tzinfo=UTC)
        except ValueError:
            continue
        latest = value
    return latest


def _filter_rows_since(rows: list[dict[str, Any]], since: datetime | None) -> list[dict[str, Any]]:
    if since is None:
        return rows
    filtered: list[dict[str, Any]] = []
    for row in rows:
        row_ts = _parse_dt(row.get("ts"))
        if row_ts is None or row_ts >= since:
            filtered.append(row)
    return filtered


def _load_sqlite_tracking_snapshot(db_path: Path, bot_dir: Path, since: datetime | None) -> dict[str, Any]:
    async def _read() -> dict[str, Any]:
        repo = MemoryRepository(db_path=db_path, data_dir=bot_dir / "parquet")
        await repo.initialize()
        try:
            last_days: int | None = None
            if since is not None:
                delta_days = max(1, int((datetime.now(UTC) - since).total_seconds() / 86400) + 1)
                last_days = delta_days
            outcomes = await repo.get_signal_outcomes(last_days=last_days)
            if since is not None:
                outcomes = [
                    row for row in outcomes
                    if (parse_state_dt(row.get("closed_at")) or parse_state_dt(row.get("created_at")) or since) >= since
                ]
            tracked_open = [
                row for row in await repo.get_active_signals(include_closed=False)
                if row.get("status") in ("pending", "active")
            ]
            cooldown_entries = await repo.get_cooldown_count()
            market_context = await repo.get_market_context()
            return {
                "outcomes": outcomes,
                "tracked_open": tracked_open,
                "cooldown_entries": cooldown_entries,
                "market_context": market_context,
            }
        finally:
            await repo.close()

    if not db_path.exists():
        return {
            "outcomes": [],
            "tracked_open": [],
            "cooldown_entries": 0,
            "market_context": {
                "btc_bias": "neutral",
                "eth_bias": "neutral",
                "high_funding_symbols": [],
                "low_funding_symbols": [],
            },
        }
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(_read())
    finally:
        loop.close()


def _collect_snapshot(context: StartupReportContext) -> dict[str, Any]:
    session_start = _previous_session_start(context.bot_log)
    cycles = _filter_rows_since(_read_jsonl(context.cycles_file), session_start)
    shortlist_rows = _filter_rows_since(_read_jsonl(context.shortlist_file), session_start)
    selected_rows = _filter_rows_since(_read_jsonl(context.selected_file), session_start)
    rejected_rows = _filter_rows_since(_read_jsonl(context.rejected_file), session_start)
    symbol_rows = _filter_rows_since(_read_jsonl(context.symbol_analysis_file), session_start)
    delivery_rows = _filter_rows_since(_read_jsonl(context.delivery_file), session_start)
    decision_rows = _filter_rows_since(_read_jsonl(context.strategy_decisions_file), session_start)
    data_quality_rows = _filter_rows_since(_read_jsonl(context.data_quality_file), session_start)
    telemetry_mismatch_rows = _filter_rows_since(_read_jsonl(context.telemetry_mismatch_file), session_start)
    tracking_rows = _filter_rows_since(_read_jsonl(context.tracking_events_file), session_start)
    health_rows = _filter_rows_since(_read_jsonl(context.health_file), session_start)
    sqlite_snapshot = _load_sqlite_tracking_snapshot(context.db_path, context.bot_dir, session_start)
    outcomes = sqlite_snapshot["outcomes"]

    latest_cycle = cycles[-1] if cycles else {}
    latest_shortlist = shortlist_rows[-1] if shortlist_rows else {}
    cycle_setup_counts = Counter()
    selected_setup_counts = Counter()
    rejection_reason_counts = Counter()
    rejection_stage_counts = Counter()
    delivery_status_counts = Counter()
    decision_status_counts = Counter()
    decision_reason_counts = Counter()
    decision_stage_counts = Counter()
    decision_reason_by_setup = Counter()
    strategy_hit_counts = Counter()
    data_quality_setup_counts = Counter()
    missing_field_counts = Counter()
    invalid_field_counts = Counter()
    telemetry_mismatch_counts = Counter()
    tracking_event_counts = Counter()
    symbol_status_counts = Counter()
    outcome_result_counts = Counter()
    outcome_setup_counts = Counter()
    outcome_quality_counts = Counter()
    runtime_errors: list[dict[str, Any]] = []

    for row in cycles:
        if isinstance(row.get("setup_counts"), dict):
            cycle_setup_counts.update({str(k): int(v) for k, v in row["setup_counts"].items()})
        if isinstance(row.get("selected_setup_counts"), dict):
            selected_setup_counts.update({str(k): int(v) for k, v in row["selected_setup_counts"].items()})
    for row in rejected_rows:
        rejection_reason_counts.update([str(row.get("reason") or "unknown")])
        rejection_stage_counts.update([str(row.get("stage") or "unknown")])
    for row in delivery_rows:
        delivery_status_counts.update([str(row.get("delivery_status") or "unknown")])
    for row in decision_rows:
        setup_id = str(row.get("setup_id") or "unknown")
        status = str(row.get("status") or "unknown")
        reason = str(row.get("reason_code") or row.get("reason") or "unknown")
        stage = str(row.get("stage") or "unknown")
        decision_status_counts.update([status])
        decision_reason_counts.update([reason])
        decision_stage_counts.update([stage])
        if status == "signal":
            strategy_hit_counts.update([setup_id])
        else:
            decision_reason_by_setup.update([(setup_id, stage, reason)])
    for row in data_quality_rows:
        setup_id = str(row.get("setup_id") or "unknown")
        data_quality_setup_counts.update([setup_id])
        missing_field_counts.update(
            str(field)
            for field in row.get("missing_fields", [])
            if isinstance(field, str)
        )
        invalid_field_counts.update(
            str(field)
            for field in row.get("invalid_fields", [])
            if isinstance(field, str)
        )
    for row in telemetry_mismatch_rows:
        telemetry_mismatch_counts.update([str(row.get("mismatch_type") or "unknown")])
    for row in tracking_rows:
        tracking_event_counts.update([str(row.get("event_type") or "unknown")])
    for row in symbol_rows:
        status = str(row.get("status") or "unknown")
        symbol_status_counts.update([status])
        if status == "error":
            runtime_errors.append(row)
    for row in outcomes:
        outcome_result_counts.update([str(row.get("result") or "unknown")])
        outcome_setup_counts.update([str(row.get("setup_id") or "unknown")])
        outcome_quality_counts.update([str(row.get("setup_quality") or "unknown")])

    detector_runs_total = sum(
        int(row.get("detector_runs") or row.get("raw_setups") or 0)
        for row in symbol_rows
    )
    zero_detector_symbols = sum(
        1
        for row in symbol_rows
        if str(row.get("status") or "") == "ok"
        and int(row.get("detector_runs") or row.get("raw_setups") or 0) == 0
    )
    post_filter_candidates_total = sum(
        int(row.get("post_filter_candidates") or row.get("candidates") or 0)
        for row in symbol_rows
    )
    zero_hit_setups = sorted(
        setup_id
        for setup_id in {
            str(row.get("setup_id") or "unknown")
            for row in decision_rows
            if str(row.get("setup_id") or "").strip()
        }
        if strategy_hit_counts.get(setup_id, 0) == 0
    )
    latest_cycle_ts = _parse_dt(latest_cycle.get("ts"))
    cycle_age_minutes = None
    if latest_cycle_ts is not None:
        cycle_age_minutes = round((context.report_ts - latest_cycle_ts).total_seconds() / 60.0, 1)

    tracked_open = sqlite_snapshot["tracked_open"]
    open_by_status = Counter(str(item.get("status") or "unknown") for item in tracked_open)
    open_by_setup = Counter(str(item.get("setup_id") or "unknown") for item in tracked_open)
    cooldown_entries = int(sqlite_snapshot["cooldown_entries"])
    market_context = sqlite_snapshot["market_context"]
    shortlist_payload = latest_shortlist if isinstance(latest_shortlist, dict) else {}
    shortlist_symbols: list[Any] = shortlist_payload["symbols"] if isinstance(shortlist_payload.get("symbols"), list) else []
    latest_health = health_rows[-1] if health_rows else {}

    return {
        "session_start": session_start,
        "cycles": cycles,
        "latest_cycle": latest_cycle,
        "latest_shortlist": latest_shortlist,
        "selected_rows": selected_rows,
        "rejected_rows": rejected_rows,
        "symbol_rows": symbol_rows,
        "delivery_rows": delivery_rows,
        "decision_rows": decision_rows,
        "data_quality_rows": data_quality_rows,
        "telemetry_mismatch_rows": telemetry_mismatch_rows,
        "tracking_rows": tracking_rows,
        "health_rows": health_rows,
        "latest_health": latest_health,
        "outcomes": outcomes,
        "cycle_count": len(cycles),
        "cycle_age_minutes": cycle_age_minutes,
        "cycle_setup_counts": cycle_setup_counts,
        "selected_setup_counts": selected_setup_counts,
        "rejection_reason_counts": rejection_reason_counts,
        "rejection_stage_counts": rejection_stage_counts,
        "delivery_status_counts": delivery_status_counts,
        "decision_status_counts": decision_status_counts,
        "decision_reason_counts": decision_reason_counts,
        "decision_stage_counts": decision_stage_counts,
        "decision_reason_by_setup": decision_reason_by_setup,
        "strategy_hit_counts": strategy_hit_counts,
        "zero_hit_setups": zero_hit_setups,
        "data_quality_setup_counts": data_quality_setup_counts,
        "missing_field_counts": missing_field_counts,
        "invalid_field_counts": invalid_field_counts,
        "telemetry_mismatch_counts": telemetry_mismatch_counts,
        "tracking_event_counts": tracking_event_counts,
        "symbol_status_counts": symbol_status_counts,
        "detector_runs_total": detector_runs_total,
        "zero_detector_symbols": zero_detector_symbols,
        "post_filter_candidates_total": post_filter_candidates_total,
        "runtime_errors": runtime_errors[:10],
        "outcome_result_counts": outcome_result_counts,
        "outcome_setup_counts": outcome_setup_counts,
        "outcome_quality_counts": outcome_quality_counts,
        "tracked_open": tracked_open,
        "open_by_status": open_by_status,
        "open_by_setup": open_by_setup,
        "cooldown_entries": cooldown_entries,
        "market_context": market_context,
        "shortlist_symbols": shortlist_symbols[:12],
        "bot_log_tail": _read_text_tail(context.bot_log, lines=40),
    }


def _top_counter(counter: Counter[str], *, limit: int = 5) -> list[tuple[str, int]]:
    return [(key, count) for key, count in counter.most_common(limit)]


def _counter_percentages(counter: Counter[str]) -> list[dict[str, Any]]:
    total = sum(counter.values())
    if total <= 0:
        return []
    rows: list[dict[str, Any]] = []
    for key, count in counter.most_common():
        rows.append(
            {
                "name": key,
                "count": count,
                "pct": round(count / total * 100.0, 2),
            }
        )
    return rows


def _derive_suspicious_modules(snapshot: dict[str, Any]) -> list[dict[str, str]]:
    modules: list[dict[str, str]] = []
    if snapshot["runtime_errors"]:
        modules.append(
            {
                "module": "bot.app",
                "reason": "symbol_analysis rows contain runtime errors from the previous session window",
            }
        )
    if snapshot["rejection_stage_counts"].get("global_filters", 0) > 0:
        modules.append(
            {
                "module": "bot.filters",
                "reason": "global_filters produced explicit rejections in persisted telemetry",
            }
        )
    if snapshot["rejection_stage_counts"].get("orderflow", 0) > 0:
        modules.append(
            {
                "module": "bot.filters",
                "reason": "orderflow confirmation vetoed or penalized candidates in the previous session window",
            }
        )
    if snapshot["cycle_count"] > 0 and snapshot["detector_runs_total"] == 0:
        modules.append(
            {
                "module": "bot.setups",
                "reason": "cycles existed but detector surface produced zero detector runs",
            }
        )
    elif snapshot["symbol_rows"]:
        zero_ratio = snapshot["zero_detector_symbols"] / max(len(snapshot["symbol_rows"]), 1)
        if zero_ratio >= 0.8:
            modules.append(
                {
                    "module": "bot.setups",
                    "reason": "most analyzed symbols ended with detector_runs=0",
                }
            )
    latest_cycle = snapshot["latest_cycle"]
    if latest_cycle.get("reconnect_reason") not in {None, "connected"}:
        modules.append(
            {
                "module": "bot.ws_manager",
                "reason": f"latest cycle recorded reconnect_reason={latest_cycle.get('reconnect_reason')}",
            }
        )
    if latest_cycle.get("rest_weight_1m") is not None and float(latest_cycle.get("rest_weight_1m") or 0) >= 1000:
        modules.append(
            {
                "module": "bot.market_data",
                "reason": "rest_weight_1m was high in the latest persisted cycle",
            }
        )
    if snapshot["delivery_status_counts"] and set(snapshot["delivery_status_counts"]) != {"sent"}:
        modules.append(
            {
                "module": "bot.delivery",
                "reason": "delivery telemetry contains non-sent statuses",
            }
        )
    if snapshot["open_by_status"]:
        if snapshot["open_by_status"].get("pending", 0) > 0 or snapshot["open_by_status"].get("active", 0) > 0:
            modules.append(
                {
                    "module": "bot.tracking",
                    "reason": "startup found persisted open tracked signals that still require lifecycle handling",
                }
            )
    deduped: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for row in modules:
        key = (row["module"], row["reason"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


def _derive_recommended_fixes(snapshot: dict[str, Any]) -> list[dict[str, str]]:
    fixes: list[dict[str, str]] = []
    if snapshot["cycle_count"] > 0 and snapshot["detector_runs_total"] == 0:
        fixes.append(
            {
                "priority": "high",
                "module": "bot.setups",
                "action": "Inspect detector thresholds and add per-detector miss telemetry for symbols with detector_runs=0.",
            }
        )
    elif snapshot["symbol_rows"]:
        zero_ratio = snapshot["zero_detector_symbols"] / max(len(snapshot["symbol_rows"]), 1)
        if zero_ratio >= 0.8:
            fixes.append(
                {
                    "priority": "high",
                    "module": "bot.setups",
                    "action": "Add detector-level miss counters to explain why most symbols produced no detector runs.",
                }
            )
    _top_rejection = snapshot["rejection_reason_counts"].most_common(1)
    if _top_rejection:
        top_reason = _top_rejection[0][0]
        fixes.append(
            {
                "priority": "medium",
                "module": "bot.filters",
                "action": f"Review the dominant rejection reason `{top_reason}` against intended thresholds and recent market regime.",
            }
        )
    if snapshot["runtime_errors"]:
        fixes.append(
            {
                "priority": "high",
                "module": "bot.app",
                "action": "Review symbol-analysis error rows and add targeted handling or telemetry before changing strategy logic.",
            }
        )
    latest_cycle = snapshot["latest_cycle"]
    if latest_cycle.get("rest_weight_1m") is not None and float(latest_cycle.get("rest_weight_1m") or 0) >= 1000:
        fixes.append(
            {
                "priority": "medium",
                "module": "bot.market_data",
                "action": "Reduce bursty REST usage or add adaptive backoff when Binance used weight approaches the minute limit.",
            }
        )
    if snapshot["delivery_status_counts"] and set(snapshot["delivery_status_counts"]) != {"sent"}:
        fixes.append(
            {
                "priority": "medium",
                "module": "bot.delivery",
                "action": "Audit non-sent delivery outcomes and confirm whether they were expected suppressions or actual transport failures.",
            }
        )
    if not fixes:
        fixes.append(
            {
                "priority": "low",
                "module": "bot.startup_reporter",
                "action": "No strong corrective action stands out from persisted telemetry alone; collect another full runtime session.",
            }
        )
    return fixes


def _market_context_is_fresh(snapshot: dict[str, Any]) -> bool:
    market_context = snapshot.get("market_context", {})
    if not isinstance(market_context, dict):
        return False
    updated_at = _parse_dt(market_context.get("updated_at"))
    if updated_at is None:
        return False
    session_start = snapshot.get("session_start")
    if isinstance(session_start, datetime) and updated_at < session_start:
        return False
    return True


def _live_market_context(snapshot: dict[str, Any]) -> dict[str, Any]:
    market_context = snapshot.get("market_context", {})
    if not isinstance(market_context, dict):
        return {}
    return market_context if _market_context_is_fresh(snapshot) else {}


def _live_intelligence_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    market_context = _live_market_context(snapshot)
    intelligence_snapshot = market_context.get("intelligence_snapshot", {})
    return intelligence_snapshot if isinstance(intelligence_snapshot, dict) else {}


def _build_current_market_state(snapshot: dict[str, Any]) -> dict[str, Any]:
    market_context = _live_market_context(snapshot)
    intelligence_snapshot = _live_intelligence_snapshot(snapshot)
    policy = intelligence_snapshot.get("policy", {}) if isinstance(intelligence_snapshot, dict) else {}
    high_funding = market_context.get("high_funding_symbols", [])
    low_funding = market_context.get("low_funding_symbols", [])
    funding_sentiment = "neutral"
    if len(high_funding) > len(low_funding):
        funding_sentiment = "long_heavy"
    elif len(low_funding) > len(high_funding):
        funding_sentiment = "short_heavy"
    market_regime = "unknown"
    if bool(market_context.get("market_regime_confirmed")):
        market_regime = str(market_context.get("market_regime") or "unknown")
    barrier = intelligence_snapshot.get("barrier") if isinstance(intelligence_snapshot, dict) else {}
    macro = intelligence_snapshot.get("macro") if isinstance(intelligence_snapshot, dict) else {}
    macro_risk_mode = (
        str(macro.get("risk_mode") or market_context.get("macro_risk_mode"))
        if isinstance(macro, dict)
        else None
    )
    if not macro_risk_mode:
        macro_risk_mode = (
            "disabled_binance_only"
            if policy.get("source_policy") == "binance_only"
            else "unknown"
        )
    return {
        "market_regime": market_regime,
        "btc_bias": market_context.get("btc_bias", "neutral"),
        "eth_bias": market_context.get("eth_bias", "neutral"),
        "funding_sentiment": funding_sentiment,
        "runtime_mode": policy.get("runtime_mode", "unknown"),
        "source_policy": policy.get("source_policy", "unknown"),
        "smart_exit_mode": policy.get("smart_exit_mode", "unknown"),
        "gamma_semantics": policy.get("gamma_semantics", "unknown"),
        "macro_risk_mode": macro_risk_mode,
        "macro_status": (
            str(macro.get("status") or "unknown")
            if isinstance(macro, dict)
            else "unknown"
        ),
        "context_updated_at": market_context.get("updated_at"),
        "high_funding_count": len(high_funding),
        "low_funding_count": len(low_funding),
        "public_intelligence_available": bool(
            intelligence_snapshot.get("ts")
        ) if isinstance(intelligence_snapshot, dict) else False,
        "hard_barrier_long": bool(barrier.get("long_barrier_triggered")) if isinstance(barrier, dict) else False,
        "hard_barrier_short": bool(barrier.get("short_barrier_triggered")) if isinstance(barrier, dict) else False,
    }


def _build_runtime_readiness(snapshot: dict[str, Any]) -> dict[str, Any]:
    symbol_rows = snapshot.get("symbol_rows", [])
    ready_15m = sum(1 for row in symbol_rows if int(row.get("work_rows_15m") or 0) > 0)
    ready_1h = sum(1 for row in symbol_rows if int(row.get("work_rows_1h") or 0) > 0)
    ready_5m = sum(1 for row in symbol_rows if int(row.get("work_rows_5m") or 0) > 0)
    ready_4h = sum(1 for row in symbol_rows if int(row.get("work_rows_4h") or 0) > 0)
    latest_health = snapshot.get("latest_health", {})
    latest_shortlist = snapshot.get("latest_shortlist", {})
    intelligence_snapshot = _live_intelligence_snapshot(snapshot)
    policy = intelligence_snapshot.get("policy", {}) if isinstance(intelligence_snapshot, dict) else {}
    macro = intelligence_snapshot.get("macro") if isinstance(intelligence_snapshot, dict) else {}
    return {
        "shortlist_source": latest_shortlist.get("source", "unknown"),
        "shortlist_size": latest_shortlist.get("size", latest_cycle.get("shortlist_size") if (latest_cycle := snapshot.get("latest_cycle", {})) else None),
        "shortlist_preview": snapshot.get("shortlist_symbols", []),
        "required_frame_readiness": {
            "15m_ready_symbols": ready_15m,
            "1h_ready_symbols": ready_1h,
            "5m_ready_symbols": ready_5m,
            "4h_macro_symbols": ready_4h,
        },
        "ws_health": {
            "active_stream_count": latest_health.get("active_stream_count", snapshot.get("latest_cycle", {}).get("active_stream_count")),
            "buffer_message_count": latest_health.get("buffer_message_count"),
            "reconnect_reason": latest_health.get("reconnect_reason", snapshot.get("latest_cycle", {}).get("reconnect_reason")),
            "ws_last_message_age_s": latest_health.get("ws_last_message_age_s"),
        },
        "public_intelligence_ts": intelligence_snapshot.get("ts") if isinstance(intelligence_snapshot, dict) else None,
        "intelligence_policy": {
            "runtime_mode": policy.get("runtime_mode"),
            "source_policy": policy.get("source_policy"),
            "smart_exit_mode": policy.get("smart_exit_mode"),
            "gamma_semantics": policy.get("gamma_semantics"),
        },
        "macro_status": (
            str(macro.get("status") or "unknown")
            if isinstance(macro, dict)
            else "unknown"
        ),
    }


def _build_structured_summary(context: StartupReportContext, snapshot: dict[str, Any]) -> dict[str, Any]:
    latest_cycle = snapshot["latest_cycle"]
    runtime_policy = _load_runtime_policy(context.repo_root)
    confirmed_facts: list[str] = []
    inferred_focus: list[str] = []
    project_state_notes = [
        "Live Binance runtime is not verified end-to-end by startup artifacts alone.",
        "Startup snapshot keeps market_regime=`unknown` unless persisted runtime context explicitly confirms it.",
        "Startup snapshot keeps public_intelligence_ts=`null` when persisted intelligence predates the analyzed session.",
        "Smart Levels / Nearby is not a separate module yet; current runtime exposes 5m/1h hooks and an explicit funnel instead.",
    ]

    if snapshot["session_start"] is None:
        confirmed_facts.append("No previous BOT SESSION STARTED marker was found in bot.log.")
    else:
        confirmed_facts.append(
            f"Previous session started at {snapshot['session_start'].astimezone(UTC).isoformat()}."
        )
    confirmed_facts.append(f"Observed cycles in previous session window: {snapshot['cycle_count']}.")
    confirmed_facts.append(f"Total detector runs: {snapshot['detector_runs_total']}.")
    confirmed_facts.append(f"Total post-filter candidates: {snapshot['post_filter_candidates_total']}.")
    confirmed_facts.append(f"Selected signals logged: {len(snapshot['selected_rows'])}.")
    confirmed_facts.append(f"Structured strategy decisions logged: {len(snapshot['decision_rows'])}.")
    confirmed_facts.append(f"Data-quality violations logged: {len(snapshot['data_quality_rows'])}.")
    confirmed_facts.append(f"Closed outcomes available: {len(snapshot['outcomes'])}.")
    confirmed_facts.append(f"Open tracked signals at startup: {len(snapshot['tracked_open'])}.")
    confirmed_facts.append(f"Cooldown entries persisted at startup: {snapshot['cooldown_entries']}.")
    if snapshot["zero_hit_setups"]:
        confirmed_facts.append(f"Zero-hit setups in the analyzed run: {', '.join(snapshot['zero_hit_setups'])}.")
    if snapshot["telemetry_mismatch_rows"]:
        confirmed_facts.append(f"Telemetry mismatch rows observed: {len(snapshot['telemetry_mismatch_rows'])}.")
    if runtime_policy.get("config_loaded"):
        confirmed_facts.append(
            "Configured runtime policy: "
            f"runtime_mode={runtime_policy['runtime_mode']} "
            f"source_policy={runtime_policy['source_policy']} "
            f"smart_exit_mode={runtime_policy['smart_exit_mode']} "
            f"gamma_semantics={runtime_policy['gamma_semantics']}."
        )
        confirmed_facts.append(
            "Configured loss-streak pause: "
            f"{runtime_policy['max_consecutive_stop_losses']} consecutive losses pause new tracked signals for "
            f"{runtime_policy['stop_loss_pause_hours']}h."
        )
        if runtime_policy["source_policy"] == "binance_only":
            project_state_notes.append(
                "External macro/news inputs are intentionally disabled under source_policy=`binance_only`."
            )
    if not _market_context_is_fresh(snapshot):
        confirmed_facts.append(
            "Persisted market_context predates the analyzed session and is not treated as live readiness."
        )

    if snapshot["cycle_count"] > 0 and snapshot["detector_runs_total"] == 0:
        inferred_focus.append("Detector surface produced no detector runs in the previous session window.")
    elif snapshot["symbol_rows"]:
        zero_ratio = snapshot["zero_detector_symbols"] / max(len(snapshot["symbol_rows"]), 1)
        if zero_ratio >= 0.8:
            inferred_focus.append(
                "Most analyzed symbols ended with detector_runs=0; detector conditions may be too strict or market regime mismatch dominated."
            )
    _top_focus = snapshot["rejection_reason_counts"].most_common(1)
    if _top_focus:
        top_reason, top_count = _top_focus[0]
        inferred_focus.append(f"Primary rejection pressure came from `{top_reason}` ({top_count} rows).")
    _top_decision_focus = snapshot["decision_reason_counts"].most_common(1)
    if _top_decision_focus:
        top_reason, top_count = _top_decision_focus[0]
        inferred_focus.append(f"Decision-level pressure was led by `{top_reason}` ({top_count} rows).")
    if snapshot["runtime_errors"]:
        inferred_focus.append("Runtime symbol-analysis errors were observed and should be reviewed before blaming strategies.")
    if snapshot["data_quality_rows"]:
        inferred_focus.append("Strict data-quality rows were persisted; missing/invalid inputs now appear as explicit evidence.")
    if snapshot["delivery_status_counts"] and set(snapshot["delivery_status_counts"]) != {"sent"}:
        inferred_focus.append("Delivery path had non-sent statuses in the previous session window.")
    if snapshot["telemetry_mismatch_rows"]:
        inferred_focus.append("Telemetry mismatch rows indicate file-level parity issues that should be resolved before tuning thresholds.")
    if not inferred_focus:
        inferred_focus.append("No single dominant failure mode was detected from persisted telemetry alone.")

    latest_cycle_summary = {
        "ts": latest_cycle.get("ts"),
        "shortlist_size": latest_cycle.get("shortlist_size"),
        "candidate_count": latest_cycle.get("candidate_count"),
        "selected_count": latest_cycle.get("selected_count"),
        "rejected_count": latest_cycle.get("rejected_count"),
        "active_stream_count": latest_cycle.get("active_stream_count"),
        "reconnect_reason": latest_cycle.get("reconnect_reason"),
        "rest_weight_1m": latest_cycle.get("rest_weight_1m"),
        "rest_response_time_ms": latest_cycle.get("rest_response_time_ms"),
    }
    suspicious_modules = _derive_suspicious_modules(snapshot)
    recommended_fixes = _derive_recommended_fixes(snapshot)
    current_market_state = _build_current_market_state(snapshot)
    current_runtime_readiness = _build_runtime_readiness(snapshot)
    if runtime_policy.get("config_loaded"):
        current_market_state.setdefault("runtime_mode", runtime_policy["runtime_mode"])
        current_market_state.setdefault("source_policy", runtime_policy["source_policy"])
        current_market_state.setdefault("smart_exit_mode", runtime_policy["smart_exit_mode"])
        current_market_state.setdefault("gamma_semantics", runtime_policy["gamma_semantics"])
        if current_market_state.get("runtime_mode") == "unknown":
            current_market_state["runtime_mode"] = runtime_policy["runtime_mode"]
        if current_market_state.get("source_policy") == "unknown":
            current_market_state["source_policy"] = runtime_policy["source_policy"]
        if current_market_state.get("smart_exit_mode") == "unknown":
            current_market_state["smart_exit_mode"] = runtime_policy["smart_exit_mode"]
        if current_market_state.get("gamma_semantics") == "unknown":
            current_market_state["gamma_semantics"] = runtime_policy["gamma_semantics"]
        if runtime_policy["source_policy"] == "binance_only":
            if current_market_state.get("macro_status") == "unknown":
                current_market_state["macro_status"] = "disabled_by_source_policy"
            if current_market_state.get("macro_risk_mode") == "unknown":
                current_market_state["macro_risk_mode"] = "disabled_binance_only"
            policy_block = current_runtime_readiness.get("intelligence_policy", {})
            if isinstance(policy_block, dict):
                if policy_block.get("runtime_mode") is None:
                    policy_block["runtime_mode"] = runtime_policy["runtime_mode"]
                if policy_block.get("source_policy") is None:
                    policy_block["source_policy"] = runtime_policy["source_policy"]
                if policy_block.get("smart_exit_mode") is None:
                    policy_block["smart_exit_mode"] = runtime_policy["smart_exit_mode"]
                if policy_block.get("gamma_semantics") is None:
                    policy_block["gamma_semantics"] = runtime_policy["gamma_semantics"]
            if current_runtime_readiness.get("macro_status") == "unknown":
                current_runtime_readiness["macro_status"] = "disabled_by_source_policy"
    return {
        "event": context.event,
        "generated_at_utc": context.report_ts.astimezone(UTC).isoformat(),
        "report_kind": "startup_snapshot",
        "confirmed_facts": confirmed_facts,
        "inferred_focus_areas": inferred_focus,
        "metrics": {
            "cycle_count": snapshot["cycle_count"],
            "cycle_age_minutes": snapshot["cycle_age_minutes"],
            "detector_runs_total": snapshot["detector_runs_total"],
            "post_filter_candidates_total": snapshot["post_filter_candidates_total"],
            "selected_total": len(snapshot["selected_rows"]),
            "rejected_total": len(snapshot["rejected_rows"]),
            "decision_rows_total": len(snapshot["decision_rows"]),
            "data_quality_rows_total": len(snapshot["data_quality_rows"]),
            "telemetry_mismatch_total": len(snapshot["telemetry_mismatch_rows"]),
            "outcomes_total": len(snapshot["outcomes"]),
            "open_tracked_total": len(snapshot["tracked_open"]),
            "cooldown_entries": snapshot["cooldown_entries"],
        },
        "top_rejection_reasons": _top_counter(snapshot["rejection_reason_counts"]),
        "top_rejection_stages": _top_counter(snapshot["rejection_stage_counts"]),
        "top_decision_reasons": _top_counter(snapshot["decision_reason_counts"]),
        "top_decision_stages": _top_counter(snapshot["decision_stage_counts"]),
        "percentages": {
            "cycle_setup_counts": _counter_percentages(snapshot["cycle_setup_counts"]),
            "selected_setup_counts": _counter_percentages(snapshot["selected_setup_counts"]),
            "outcome_setup_counts": _counter_percentages(snapshot["outcome_setup_counts"]),
            "outcome_result_counts": _counter_percentages(snapshot["outcome_result_counts"]),
            "outcome_quality_counts": _counter_percentages(snapshot["outcome_quality_counts"]),
            "rejection_reasons": _counter_percentages(snapshot["rejection_reason_counts"]),
            "rejection_stages": _counter_percentages(snapshot["rejection_stage_counts"]),
            "decision_reasons": _counter_percentages(snapshot["decision_reason_counts"]),
            "decision_stages": _counter_percentages(snapshot["decision_stage_counts"]),
            "delivery_statuses": _counter_percentages(snapshot["delivery_status_counts"]),
            "tracking_events": _counter_percentages(snapshot["tracking_event_counts"]),
        },
        "setup_counts": {
            "cycle_setup_counts": dict(snapshot["cycle_setup_counts"]),
            "selected_setup_counts": dict(snapshot["selected_setup_counts"]),
            "outcome_setup_counts": dict(snapshot["outcome_setup_counts"]),
        },
        "delivery_status_counts": dict(snapshot["delivery_status_counts"]),
        "decision_status_counts": dict(snapshot["decision_status_counts"]),
        "decision_reason_counts": dict(snapshot["decision_reason_counts"]),
        "decision_stage_counts": dict(snapshot["decision_stage_counts"]),
        "zero_hit_setups": snapshot["zero_hit_setups"],
        "data_quality": {
            "by_setup": dict(snapshot["data_quality_setup_counts"]),
            "missing_fields": dict(snapshot["missing_field_counts"]),
            "invalid_fields": dict(snapshot["invalid_field_counts"]),
        },
        "telemetry_mismatch_counts": dict(snapshot["telemetry_mismatch_counts"]),
        "tracking_event_counts": dict(snapshot["tracking_event_counts"]),
        "outcome_result_counts": dict(snapshot["outcome_result_counts"]),
        "outcome_quality_counts": dict(snapshot["outcome_quality_counts"]),
        "open_signal_counts": {
            "by_status": dict(snapshot["open_by_status"]),
            "by_setup": dict(snapshot["open_by_setup"]),
        },
        "latest_cycle_summary": latest_cycle_summary,
        "current_market_state": current_market_state,
        "current_runtime_readiness": current_runtime_readiness,
        "runtime_policy": runtime_policy,
        "runtime_errors": snapshot["runtime_errors"],
        "shortlist_preview": snapshot["shortlist_symbols"],
        "suspicious_modules": suspicious_modules,
        "recommended_fixes": recommended_fixes,
        "project_state_notes": project_state_notes,
    }


def _render_markdown(
    context: StartupReportContext,
    snapshot: dict[str, Any],
    summary: dict[str, Any],
) -> str:
    latest_cycle_summary = summary["latest_cycle_summary"]
    lines: list[str] = []
    lines.append(f"# Startup Report {context.report_ts.astimezone(MSK).strftime('%Y-%m-%d %H:%M:%S MSK')}")
    lines.append("")
    lines.append("## Purpose")
    lines.append("- Analyze the persisted evidence from the previous bot run before the new runtime starts.")
    lines.append("- Preserve an AI-readable handoff for debugging strategy, filters, tracking, delivery, and transport.")
    lines.append("")
    lines.append("## Data Sources")
    lines.append(f"- Bot log: `{context.bot_log}`")
    lines.append(f"- Telemetry dir: `{context.telemetry_dir}`")
    lines.append(f"- SQLite db: `{context.db_path}`")
    lines.append("")
    lines.append("## Confirmed Facts")
    for item in summary["confirmed_facts"]:
        lines.append(f"- {item}")
    lines.append("")
    lines.append("## Inferred Focus Areas")
    for item in summary["inferred_focus_areas"]:
        lines.append(f"- {item}")
    lines.append("")
    lines.append("## Project State Notes")
    for item in summary.get("project_state_notes", []):
        lines.append(f"- {item}")
    lines.append("")
    lines.append("## Recommended Fixes")
    for item in summary["recommended_fixes"]:
        lines.append(f"- [{item['priority']}] `{item['module']}`: {item['action']}")
    lines.append("")
    lines.append("## Suspicious Modules")
    for item in summary["suspicious_modules"]:
        lines.append(f"- `{item['module']}`: {item['reason']}")
    lines.append("")
    lines.append("## Previous Run Metrics")
    lines.append(f"- Runtime policy: `{json.dumps(summary.get('runtime_policy', {}), ensure_ascii=True, sort_keys=True)}`")
    lines.append(f"- Cycle count: `{summary['metrics']['cycle_count']}`")
    lines.append(f"- Cycle age minutes: `{summary['metrics']['cycle_age_minutes']}`")
    lines.append(f"- Detector runs total: `{summary['metrics']['detector_runs_total']}`")
    lines.append(f"- Post-filter candidates total: `{summary['metrics']['post_filter_candidates_total']}`")
    lines.append(f"- Selected total: `{summary['metrics']['selected_total']}`")
    lines.append(f"- Rejected total: `{summary['metrics']['rejected_total']}`")
    lines.append(f"- Strategy decision rows: `{summary['metrics']['decision_rows_total']}`")
    lines.append(f"- Data-quality rows: `{summary['metrics']['data_quality_rows_total']}`")
    lines.append(f"- Telemetry mismatches: `{summary['metrics']['telemetry_mismatch_total']}`")
    lines.append(f"- Outcomes total: `{summary['metrics']['outcomes_total']}`")
    lines.append(f"- Open tracked total: `{summary['metrics']['open_tracked_total']}`")
    lines.append(f"- Cooldown entries: `{summary['metrics']['cooldown_entries']}`")
    lines.append("")
    lines.append("## Strategy Analytics")
    lines.append(f"- Cycle setup counts: `{json.dumps(summary['setup_counts']['cycle_setup_counts'], ensure_ascii=True, sort_keys=True)}`")
    lines.append(f"- Selected setup counts: `{json.dumps(summary['setup_counts']['selected_setup_counts'], ensure_ascii=True, sort_keys=True)}`")
    lines.append(f"- Outcome setup counts: `{json.dumps(summary['setup_counts']['outcome_setup_counts'], ensure_ascii=True, sort_keys=True)}`")
    lines.append(f"- Outcome result counts: `{json.dumps(summary['outcome_result_counts'], ensure_ascii=True, sort_keys=True)}`")
    lines.append(f"- Outcome quality counts: `{json.dumps(summary['outcome_quality_counts'], ensure_ascii=True, sort_keys=True)}`")
    lines.append(f"- Cycle setup percentages: `{json.dumps(summary['percentages']['cycle_setup_counts'], ensure_ascii=True)}`")
    lines.append(f"- Selected setup percentages: `{json.dumps(summary['percentages']['selected_setup_counts'], ensure_ascii=True)}`")
    lines.append(f"- Outcome setup percentages: `{json.dumps(summary['percentages']['outcome_setup_counts'], ensure_ascii=True)}`")
    lines.append(f"- Outcome result percentages: `{json.dumps(summary['percentages']['outcome_result_counts'], ensure_ascii=True)}`")
    lines.append("")
    lines.append("## Filter Analytics")
    lines.append(f"- Top rejection reasons: `{json.dumps(summary['top_rejection_reasons'], ensure_ascii=True)}`")
    lines.append(f"- Top rejection stages: `{json.dumps(summary['top_rejection_stages'], ensure_ascii=True)}`")
    lines.append(f"- Top decision reasons: `{json.dumps(summary['top_decision_reasons'], ensure_ascii=True)}`")
    lines.append(f"- Top decision stages: `{json.dumps(summary['top_decision_stages'], ensure_ascii=True)}`")
    lines.append(f"- Rejection reason percentages: `{json.dumps(summary['percentages']['rejection_reasons'], ensure_ascii=True)}`")
    lines.append(f"- Rejection stage percentages: `{json.dumps(summary['percentages']['rejection_stages'], ensure_ascii=True)}`")
    lines.append(f"- Decision reason percentages: `{json.dumps(summary['percentages']['decision_reasons'], ensure_ascii=True)}`")
    lines.append(f"- Decision stage percentages: `{json.dumps(summary['percentages']['decision_stages'], ensure_ascii=True)}`")
    lines.append("")
    lines.append("## Runtime Analytics")
    lines.append(f"- Latest cycle summary: `{json.dumps(latest_cycle_summary, ensure_ascii=True, sort_keys=True)}`")
    lines.append(f"- Delivery status counts: `{json.dumps(summary['delivery_status_counts'], ensure_ascii=True, sort_keys=True)}`")
    lines.append(f"- Decision status counts: `{json.dumps(summary['decision_status_counts'], ensure_ascii=True, sort_keys=True)}`")
    lines.append(f"- Zero-hit setups: `{json.dumps(summary['zero_hit_setups'], ensure_ascii=True)}`")
    lines.append(f"- Data quality: `{json.dumps(summary['data_quality'], ensure_ascii=True, sort_keys=True)}`")
    lines.append(f"- Telemetry mismatch counts: `{json.dumps(summary['telemetry_mismatch_counts'], ensure_ascii=True, sort_keys=True)}`")
    lines.append(f"- Tracking event counts: `{json.dumps(summary['tracking_event_counts'], ensure_ascii=True, sort_keys=True)}`")
    lines.append(f"- Open signal counts: `{json.dumps(summary['open_signal_counts'], ensure_ascii=True, sort_keys=True)}`")
    lines.append(f"- Delivery status percentages: `{json.dumps(summary['percentages']['delivery_statuses'], ensure_ascii=True)}`")
    lines.append(f"- Tracking event percentages: `{json.dumps(summary['percentages']['tracking_events'], ensure_ascii=True)}`")
    lines.append("")
    lines.append("## Current Market State")
    lines.append(f"- Market state: `{json.dumps(summary['current_market_state'], ensure_ascii=True, sort_keys=True)}`")
    lines.append("")
    lines.append("## Current Runtime Readiness")
    lines.append(f"- Runtime readiness: `{json.dumps(summary['current_runtime_readiness'], ensure_ascii=True, sort_keys=True)}`")
    if summary["runtime_errors"]:
        lines.append("")
        lines.append("## Runtime Errors")
        for row in summary["runtime_errors"]:
            lines.append(f"- `{row.get('symbol')}` status=`{row.get('status')}` error=`{row.get('error')}`")
    lines.append("")
    lines.append("## Shortlist Preview")
    shortlist_preview = ", ".join(summary["shortlist_preview"]) if summary["shortlist_preview"] else "n/a"
    lines.append(f"- {shortlist_preview}")
    lines.append("")
    lines.append("## AI Handoff JSON")
    lines.append("```json")
    lines.append(json.dumps(summary, ensure_ascii=True, indent=2))
    lines.append("```")
    lines.append("")
    lines.append("## Bot Log Tail")
    lines.append("```text")
    lines.append(snapshot["bot_log_tail"] or "(empty)")
    lines.append("```")
    lines.append("")
    return "\n".join(lines)


def _build_telegram_message(summary: dict[str, Any]) -> str:
    latest_cycle = summary["latest_cycle_summary"]
    market_state = summary["current_market_state"]
    readiness = summary["current_runtime_readiness"]
    runtime_policy = summary.get("runtime_policy", {})
    ws_health = readiness["ws_health"]
    frame_readiness = readiness["required_frame_readiness"]
    confirmed = html.escape(summary["confirmed_facts"][0] if summary["confirmed_facts"] else "No confirmed facts.")
    focus = html.escape(summary["inferred_focus_areas"][0] if summary["inferred_focus_areas"] else "No dominant focus area.")
    fix = html.escape(summary["recommended_fixes"][0]["action"] if summary["recommended_fixes"] else "No specific action.")
    return "\n".join(
        [
            "<b>Startup Report</b>",
            f"Type: <code>{html.escape(str(summary['report_kind']))}</code>",
            f"Cycles: <code>{summary['metrics']['cycle_count']}</code> | Detector runs: <code>{summary['metrics']['detector_runs_total']}</code> | Selected: <code>{summary['metrics']['selected_total']}</code>",
            f"Outcomes: <code>{summary['metrics']['outcomes_total']}</code> | Open tracked: <code>{summary['metrics']['open_tracked_total']}</code>",
            f"Latest cycle: <code>{html.escape(str(latest_cycle.get('ts') or ''))}</code>",
            f"Market: regime=<code>{html.escape(str(market_state.get('market_regime') or 'unknown'))}</code> btc=<code>{html.escape(str(market_state.get('btc_bias') or 'neutral'))}</code> eth=<code>{html.escape(str(market_state.get('eth_bias') or 'neutral'))}</code>",
            (
                f"Policy: runtime=<code>{html.escape(str(runtime_policy.get('runtime_mode') or 'unknown'))}</code> "
                f"source=<code>{html.escape(str(runtime_policy.get('source_policy') or 'unknown'))}</code> "
                f"pause=<code>{html.escape(str(runtime_policy.get('max_consecutive_stop_losses') or 'n/a'))}"
                f"/{html.escape(str(runtime_policy.get('stop_loss_pause_hours') or 'n/a'))}h</code>"
            ),
            f"Shortlist: source=<code>{html.escape(str(readiness.get('shortlist_source') or 'unknown'))}</code> size=<code>{html.escape(str(readiness.get('shortlist_size') or 'n/a'))}</code>",
            f"WS: streams=<code>{html.escape(str(ws_health.get('active_stream_count') or 'n/a'))}</code> reconnect=<code>{html.escape(str(ws_health.get('reconnect_reason') or 'n/a'))}</code>",
            f"Frames: 15m=<code>{frame_readiness.get('15m_ready_symbols', 0)}</code> 1h=<code>{frame_readiness.get('1h_ready_symbols', 0)}</code> 5m=<code>{frame_readiness.get('5m_ready_symbols', 0)}</code>",
            f"Fact: {confirmed}",
            f"Focus: {focus}",
            f"Fix: {fix}",
        ]
    )


def _resolve_report_chat_id(settings: BotSettings) -> str:
    for env_name in ("STARTUP_REPORT_CHAT_ID", "WATCHDOG_CHAT_ID", "TARGET_CHAT_ID"):
        value = os.getenv(env_name, "").strip()
        if value:
            return value
    return settings.target_chat_id.strip()


async def _send_telegram_message(settings: BotSettings, text: str) -> bool:
    chat_id = _resolve_report_chat_id(settings)
    if not settings.tg_token.strip() or not chat_id:
        return False
    broadcaster = TelegramBroadcaster(settings.tg_token, chat_id)
    try:
        await broadcaster.send_html(text)
        return True
    finally:
        await broadcaster.close()


def generate_startup_report(
    repo_root: Path,
    *,
    event: str = "startup_previous_run_analysis",
    report_ts: datetime | None = None,
) -> StartupReportResult:
    report_ts = report_ts or datetime.now(UTC)
    context = _load_context(repo_root.resolve(), event, report_ts.astimezone(UTC))
    snapshot = _collect_snapshot(context)
    summary = _build_structured_summary(context, snapshot)
    markdown = _render_markdown(context, snapshot, summary)
    telegram_message = _build_telegram_message(summary)

    # Append config advisor suggestions if there are enough resolved outcomes.
    try:
        telemetry_root = context.bot_dir / "telemetry"
        suggestions = build_config_suggestions(telemetry_root)
        # Only include lines that contain real suggestions (skip "Not enough data" messages).
        real_suggestions = [s for s in suggestions if "[SUGGEST]" in s or "[ADVISOR]" in s]
        if real_suggestions and not any("Not enough" in s for s in suggestions[:2]):
            advisor_text = "\n\n<b>CONFIG ADVISOR</b>\n" + html.escape("\n".join(real_suggestions))
            telegram_message = telegram_message + advisor_text
    except Exception:
        pass

    stamp = context.report_ts.astimezone(MSK).strftime("%Y%m%d_%H%M%S")
    report_path = context.reports_dir / f"startup_report_{stamp}.md"
    json_path = context.reports_dir / f"startup_report_{stamp}.json"
    report_path.write_text(markdown, encoding="utf-8")
    context.latest_report_path.write_text(markdown, encoding="utf-8")
    json_path.write_text(json.dumps(summary, ensure_ascii=True, indent=2), encoding="utf-8")
    context.latest_report_path.with_suffix(".json").write_text(
        json.dumps(summary, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )
    return StartupReportResult(
        report_path=report_path,
        json_path=json_path,
        markdown=markdown,
        telegram_message=telegram_message,
        structured_summary=summary,
    )


async def generate_and_send_startup_report(
    repo_root: Path,
    *,
    event: str = "startup_previous_run_analysis",
    send_telegram: bool = True,
    config_path: str | Path = "config.toml",
) -> StartupReportResult:
    # Run sync report generation in thread pool to avoid blocking event loop
    result = await asyncio.to_thread(generate_startup_report, repo_root, event=event)
    if send_telegram:
        settings = load_settings(config_path)
        await _send_telegram_message(settings, result.telegram_message)
    return result


async def run_daily_summary_loop(
    repo_root: Path,
    *,
    stop_event: asyncio.Event,
    config_path: str | Path = "config.toml",
    send_telegram: bool = True,
    interval_hours: float = 24.0,
) -> None:
    """Periodic runtime summary loop (daily by default)."""
    interval_seconds = max(3600.0, float(interval_hours) * 3600.0)
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
            continue
        except asyncio.TimeoutError:
            pass
        try:
            await generate_and_send_startup_report(
                repo_root,
                event="daily_runtime_summary",
                send_telegram=send_telegram,
                config_path=config_path,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            # Keep loop alive in runtime regardless of report failure.
            continue
