from __future__ import annotations

from pathlib import Path

from bot.diagnostics.runtime_analysis import (
    aggregate_cycle_stats,
    aggregate_rejection_funnel,
    aggregate_symbol_funnel,
    parse_cycle_log_lines,
    read_jsonl,
)


FIXTURES_DIR = Path("tests/fixtures/runtime_analysis")


def test_read_jsonl_skips_invalid_rows() -> None:
    rows = read_jsonl(FIXTURES_DIR / "rejected.jsonl")

    assert len(rows) == 3
    assert rows[0]["symbol"] == "BTCUSDT"


def test_aggregate_rejection_funnel_from_fixture() -> None:
    rows = read_jsonl(FIXTURES_DIR / "rejected.jsonl")

    stats = aggregate_rejection_funnel(rows)

    assert stats["by_stage"]["filters"] == 2
    assert stats["by_setup"]["breakout"] == 2
    assert stats["by_reason"]["adx_low"] == 2
    assert len(stats["detailed"]["adx_low"]) == 2


def test_aggregate_symbol_funnel_from_fixture() -> None:
    rows = read_jsonl(FIXTURES_DIR / "symbol_analysis.jsonl")

    stats = aggregate_symbol_funnel(rows)

    assert stats["symbols_processed"] == 3
    assert stats["symbols_with_raw_hits"] == 2
    assert stats["total_raw_hits"] == 5
    assert stats["total_candidates"] == 2
    assert stats["total_delivered"] == 1
    assert stats["rejection_reasons"]["trend_rejects"] == 2
    assert stats["rejection_reasons"]["alignment_penalties"] == 1


def test_aggregate_cycle_stats_from_fixture() -> None:
    rows = read_jsonl(FIXTURES_DIR / "cycles.jsonl")

    stats = aggregate_cycle_stats(rows)

    assert stats["total_cycles"] == 2
    assert stats["health_checks"] == 1
    assert stats["total_detector_runs"] == 26
    assert stats["total_candidates"] == 4
    assert stats["total_delivered"] == 2
    assert stats["by_symbol"]["ETHUSDT"]["delivered"] == 1


def test_parse_cycle_log_lines() -> None:
    lines = [
        "2026-01-01 | INFO | cycle | symbol=BTCUSDT detector_runs=8 candidates=1 delivered=0 rejected=7",
        "2026-01-01 | ERROR | something bad happened",
    ]

    parsed = parse_cycle_log_lines(lines)

    assert parsed["cycles"] == 1
    assert parsed["detector_runs_total"] == 8
    assert parsed["candidates_total"] == 1
    assert parsed["rejected_total"] == 7
    assert parsed["symbols_with_candidates"][0]["symbol"] == "BTCUSDT"
    assert len(parsed["errors"]) == 1
