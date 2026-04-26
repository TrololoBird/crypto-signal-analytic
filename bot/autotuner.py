"""Offline auto-parameter tuner.

Reads completed trade outcomes from SQLite and computes empirical win-rate
curves to recommend threshold adjustments.

Usage (standalone):
    python -c "from bot.autotuner import compute_optimal_thresholds; print(compute_optimal_thresholds('data/bot'))"

Usage (CLI):
    python main.py --ml autotune [--apply]
"""

from __future__ import annotations

import json
import logging
import asyncio
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .core.memory import MemoryRepository

LOG = logging.getLogger("bot.autotuner")

UTC = timezone.utc

# Minimum sample threshold before any adjustments are recommended
MIN_OUTCOMES = 50

# Win-rate target for parameter threshold search
TARGET_WIN_RATE = 0.55

# Win reasons
WIN_REASONS = {"tp1_hit", "tp2_hit"}


def _load_outcomes(data_path: Path) -> list[dict[str, Any]]:
    """Load persisted tracked-signal outcomes from SQLite."""

    async def _read() -> list[dict[str, Any]]:
        bot_dir = data_path if data_path.name == "bot" else data_path / "bot"
        repo = MemoryRepository(bot_dir / "bot.db", bot_dir / "parquet")
        await repo.initialize()
        try:
            return await repo.get_signal_outcomes(last_days=None)
        finally:
            await repo.close()

    try:
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(_read())
        finally:
            loop.close()
    except Exception as exc:
        LOG.warning("autotuner: failed to load SQLite outcomes from %s: %s", data_path, exc)
        return []


def _is_win(record: dict[str, Any]) -> bool:
    close_reason = record.get("close_reason") or record.get("result") or ""
    return close_reason in WIN_REASONS


def _find_threshold(
    records: list[dict[str, Any]],
    field: str,
    n_bins: int = 10,
) -> float | None:
    """Return the bin lower-bound where win_rate first exceeds TARGET_WIN_RATE.

    Bins are computed over the observed range of *field*.  Returns None when
    there are too few records or no bin exceeds the target.
    """
    values: list[tuple[float, bool]] = []
    for rec in records:
        raw = rec.get(field)
        # Also look inside a nested "features" dict (outcomes schema)
        if raw is None and "features" in rec and isinstance(rec["features"], dict):
            raw = rec["features"].get(field)
        if raw is None:
            continue
        try:
            values.append((float(raw), _is_win(rec)))
        except (TypeError, ValueError):
            pass

    if len(values) < MIN_OUTCOMES:
        return None

    vals = [v for v, _ in values]
    lo, hi = min(vals), max(vals)
    if hi <= lo:
        return None

    step = (hi - lo) / n_bins
    for i in range(n_bins):
        bin_lo = lo + i * step
        bin_hi = bin_lo + step
        bucket = [(v, w) for v, w in values if bin_lo <= v < bin_hi]
        if len(bucket) < 5:
            continue
        win_rate = sum(w for _, w in bucket) / len(bucket)
        if win_rate >= TARGET_WIN_RATE:
            return round(bin_lo, 4)

    return None


def _setup_win_rates(records: list[dict[str, Any]]) -> dict[str, float]:
    counts: dict[str, int] = {}
    wins: dict[str, int] = {}
    for rec in records:
        sid = rec.get("setup_id") or ""
        if not sid:
            continue
        counts[sid] = counts.get(sid, 0) + 1
        if _is_win(rec):
            wins[sid] = wins.get(sid, 0) + 1
    return {
        sid: round(wins.get(sid, 0) / total, 4)
        for sid, total in counts.items()
        if total >= 10
    }


def compute_optimal_thresholds(data_path: str | Path) -> dict[str, Any]:
    """Read completed outcomes from SQLite and return recommended threshold adjustments.

    Returns a dict with:
      - min_score, min_risk_reward: recommended floor thresholds
      - setup_win_rates: per-setup empirical win rates
      - sample_count: total records processed
      - generated_at: ISO timestamp

    Returns ``{"skipped": "insufficient_data", "sample_count": N}`` when
    fewer than MIN_OUTCOMES (50) records are available.
    """
    data_path = Path(data_path)
    records = _load_outcomes(data_path)
    sample_count = len(records)

    if sample_count < MIN_OUTCOMES:
        LOG.info("autotuner: insufficient data (%d < %d required)", sample_count, MIN_OUTCOMES)
        return {"skipped": "insufficient_data", "sample_count": sample_count}

    LOG.info("autotuner: processing %d outcome records", sample_count)

    # --- Threshold recommendations ---
    min_score = _find_threshold(records, "score") or _find_threshold(records, "base_score")
    min_rr = _find_threshold(records, "risk_reward")

    result: dict[str, Any] = {
        "sample_count": sample_count,
        "generated_at": datetime.now(UTC).isoformat(),
        "setup_win_rates": _setup_win_rates(records),
    }
    if min_score is not None:
        result["min_score"] = min_score
    if min_rr is not None:
        result["min_risk_reward"] = min_rr

    # --- Persist recommended.toml ---
    try:
        tuning_dir = Path("data/bot/tuning")
        tuning_dir.mkdir(parents=True, exist_ok=True)
        toml_path = tuning_dir / "recommended.toml"
        lines = [
            "# Auto-generated by bot.autotuner — do not edit manually",
            f"# generated_at = {result['generated_at']}",
            f"# sample_count = {sample_count}",
            "",
            "[filters]",
        ]
        if min_score is not None:
            lines.append(f"min_score = {min_score}")
        if min_rr is not None:
            lines.append(f"min_risk_reward = {min_rr}")
        lines.append("")
        lines.append("[setup_win_rates]")
        for sid, wr in sorted(result["setup_win_rates"].items()):
            lines.append(f"{sid} = {wr}")
        toml_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        LOG.info("autotuner: wrote recommendations to %s", toml_path)
    except OSError as exc:
        LOG.warning("autotuner: could not write recommended.toml: %s", exc)

    return result
