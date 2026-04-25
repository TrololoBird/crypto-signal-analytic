from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable


def file_has_rows(path: Path) -> bool:
    if not path.exists():
        return False
    try:
        with path.open("r", encoding="utf-8") as handle:
            return any(line.strip() for line in handle)
    except OSError:
        return False


def find_latest_run_dir(
    telemetry_dir: Path,
    run_id: str | None = None,
    interesting_files: tuple[str, ...] = (
        "strategy_decisions.jsonl",
        "symbol_analysis.jsonl",
        "rejected.jsonl",
        "cycles.jsonl",
    ),
) -> Path | None:
    """Return the latest run directory that has non-empty analysis artifacts."""
    runs_dir = telemetry_dir / "runs"
    if not runs_dir.exists():
        return None

    if run_id:
        explicit = runs_dir / run_id
        return explicit if explicit.exists() else None

    run_dirs = sorted(
        (path for path in runs_dir.iterdir() if path.is_dir()),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )

    fallback: Path | None = None
    for run_dir in run_dirs:
        analysis_dir = run_dir / "analysis"
        if not analysis_dir.exists():
            continue
        fallback = fallback or run_dir
        if any(file_has_rows(analysis_dir / filename) for filename in interesting_files):
            return run_dir
    return fallback


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    """Read JSONL rows, skipping empty lines and malformed JSON."""
    if not path.exists():
        return []

    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            raw = line.strip()
            if not raw:
                continue
            try:
                row = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if isinstance(row, dict):
                rows.append(row)
    return rows


def aggregate_rejection_funnel(rows: Iterable[dict[str, Any]]) -> dict[str, Any]:
    stats: dict[str, Any] = {
        "by_stage": Counter(),
        "by_setup": Counter(),
        "by_reason": Counter(),
        "by_symbol_setup": Counter(),
        "detailed": defaultdict(list),
    }

    for row in rows:
        stage = str(row.get("stage") or "unknown")
        setup = str(row.get("setup_id") or "unknown")
        reason = str(row.get("reason") or "unknown")
        symbol = str(row.get("symbol") or "unknown")

        stats["by_stage"][stage] += 1
        stats["by_setup"][setup] += 1
        stats["by_reason"][reason] += 1
        stats["by_symbol_setup"][f"{symbol}:{setup}"] += 1

        if len(stats["detailed"][reason]) < 3:
            sample: dict[str, Any] = {
                "symbol": symbol,
                "setup": setup,
                "stage": stage,
            }
            if "adx_1h" in row:
                sample["adx_1h"] = row["adx_1h"]
            if "risk_reward" in row:
                sample["rr"] = row["risk_reward"]
            if "trend_direction" in row:
                sample["trend"] = row["trend_direction"]
            if "details" in row:
                sample["details"] = row["details"]
            stats["detailed"][reason].append(sample)

    return stats


def aggregate_symbol_funnel(rows: Iterable[dict[str, Any]]) -> dict[str, Any]:
    stats: dict[str, Any] = {
        "symbols_processed": 0,
        "symbols_with_raw_hits": 0,
        "total_raw_hits": 0,
        "total_candidates": 0,
        "total_delivered": 0,
        "rejection_reasons": Counter(),
    }

    for data in rows:
        funnel = data.get("funnel", {}) if isinstance(data.get("funnel"), dict) else {}

        stats["symbols_processed"] += 1
        raw_hits = int(funnel.get("raw_hits", 0) or 0)
        candidates = int(data.get("candidates", 0) or 0)

        if raw_hits > 0:
            stats["symbols_with_raw_hits"] += 1
            stats["total_raw_hits"] += raw_hits

        stats["total_candidates"] += candidates
        stats["total_delivered"] += int(data.get("delivered", 0) or 0)

        if raw_hits > 0 and candidates == 0:
            for key, value in funnel.items():
                if key == "alignment_penalties" and isinstance(value, int) and value > 0:
                    stats["rejection_reasons"]["alignment_penalties"] += value
                elif "rejects" in str(key) and isinstance(value, int) and value > 0:
                    stats["rejection_reasons"][str(key)] += value

    return stats


def aggregate_cycle_stats(rows: Iterable[dict[str, Any]]) -> dict[str, Any]:
    stats: dict[str, Any] = {
        "total_cycles": 0,
        "symbols_analyzed": set(),
        "total_detector_runs": 0,
        "total_candidates": 0,
        "total_delivered": 0,
        "health_checks": 0,
        "by_symbol": defaultdict(lambda: {"cycles": 0, "detectors": 0, "candidates": 0, "delivered": 0}),
    }

    for row in rows:
        symbol = row.get("symbol")
        if symbol:
            symbol_str = str(symbol)
            detector_runs = int(row.get("detector_runs", 0) or 0)
            candidates = int(row.get("candidates", 0) or 0)
            delivered = int(row.get("delivered", 0) or 0)

            stats["total_cycles"] += 1
            stats["symbols_analyzed"].add(symbol_str)
            stats["total_detector_runs"] += detector_runs
            stats["total_candidates"] += candidates
            stats["total_delivered"] += delivered

            symbol_stats = stats["by_symbol"][symbol_str]
            symbol_stats["cycles"] += 1
            symbol_stats["detectors"] += detector_runs
            symbol_stats["candidates"] += candidates
            symbol_stats["delivered"] += delivered
            continue

        stats["health_checks"] += 1
        funnel = row.get("funnel") if isinstance(row.get("funnel"), dict) else {}
        if funnel:
            stats["total_detector_runs"] += int(funnel.get("detector_runs", 0) or 0)
            stats["total_candidates"] += int(funnel.get("post_filter_candidates", 0) or 0)
            stats["total_delivered"] += int(funnel.get("delivered", 0) or 0)

    return stats


def parse_cycle_log_lines(lines: Iterable[str]) -> dict[str, Any]:
    """Parse cycle lines from text logs into deltas for monitor stats."""
    parsed: dict[str, Any] = {
        "cycles": 0,
        "symbols_processed": set(),
        "detector_runs_total": 0,
        "candidates_total": 0,
        "delivered_total": 0,
        "rejected_total": 0,
        "symbols_with_candidates": [],
        "last_signals": [],
        "errors": [],
    }

    for line in lines:
        symbol: str | None = None
        if "cycle | symbol=" in line:
            try:
                parts = line.split("|")
                for part in parts:
                    if "symbol=" in part:
                        symbol = part.split("symbol=")[1].split()[0]
                        parsed["symbols_processed"].add(symbol)
                    if "detector_runs=" in part:
                        parsed["detector_runs_total"] += int(part.split("detector_runs=")[1].split()[0])
                    if "candidates=" in part:
                        candidates = int(part.split("candidates=")[1].split()[0])
                        parsed["candidates_total"] += candidates
                        if candidates > 0:
                            parsed["symbols_with_candidates"].append({"symbol": symbol, "candidates": candidates})
                    if "delivered=" in part:
                        delivered = int(part.split("delivered=")[1].split()[0])
                        parsed["delivered_total"] += delivered
                        if delivered > 0:
                            parsed["last_signals"].append({"symbol": symbol, "delivered": delivered})
                    if "rejected=" in part:
                        parsed["rejected_total"] += int(part.split("rejected=")[1].split()[0])
                parsed["cycles"] += 1
            except (IndexError, ValueError):
                continue

        if "ERROR" in line or "error" in line.lower():
            parsed["errors"].append(line[:200])

    return parsed
