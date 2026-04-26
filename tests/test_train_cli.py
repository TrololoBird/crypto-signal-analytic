from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import polars as pl

from bot.ml import train as train_module


def test_train_cli_writes_report(tmp_path, monkeypatch, capsys) -> None:
    base = datetime(2025, 1, 1, tzinfo=UTC)
    data = pl.DataFrame(
        {
            "created_at": [base + timedelta(hours=i) for i in range(300)],
            "outcome": ["win" if i % 2 else "loss" for i in range(300)],
            "atr_pct": [0.8 + (i % 4) * 0.1 for i in range(300)],
            "adx14": [20 + (i % 6) for i in range(300)],
            "rsi14": [40 + (i % 20) for i in range(300)],
        }
    )
    input_path = tmp_path / "train.parquet"
    report_path = tmp_path / "report.json"
    data.write_parquet(input_path)

    monkeypatch.setattr(
        "sys.argv",
        [
            "train.py",
            "--start",
            data["created_at"].min().isoformat(),
            "--end",
            data["created_at"].max().isoformat(),
            "--input",
            str(input_path),
            "--window-days",
            "5",
            "--step-days",
            "2",
            "--model-dir",
            str(tmp_path / "models"),
            "--report",
            str(report_path),
        ],
    )
    train_module.main()
    out = capsys.readouterr().out.strip()
    payload = json.loads(out)
    assert "count" in payload
    assert "summary" in payload
    assert "quality_gate" in payload
    assert payload["quality_gate"]["passed"] is True
    assert payload["summary"]["f1"] >= 0.0
    assert report_path.exists()


def test_train_cli_fails_threshold_gate(tmp_path, monkeypatch) -> None:
    base = datetime(2025, 1, 1, tzinfo=UTC)
    data = pl.DataFrame(
        {
            "created_at": [base + timedelta(hours=i) for i in range(300)],
            "outcome": ["loss" for _ in range(300)],
            "atr_pct": [0.8 for _ in range(300)],
            "adx14": [20 for _ in range(300)],
            "rsi14": [45 for _ in range(300)],
        }
    )
    input_path = tmp_path / "train.parquet"
    data.write_parquet(input_path)

    monkeypatch.setattr(
        "sys.argv",
        [
            "train.py",
            "--start",
            data["created_at"].min().isoformat(),
            "--end",
            data["created_at"].max().isoformat(),
            "--input",
            str(input_path),
            "--window-days",
            "5",
            "--step-days",
            "2",
            "--model-dir",
            str(tmp_path / "models"),
            "--min-f1",
            "0.9",
        ],
    )
    try:
        train_module.main()
    except SystemExit as exc:
        assert exc.code == 2
    else:
        raise AssertionError("expected SystemExit threshold failure")


def test_train_cli_fails_min_windows_gate(tmp_path, monkeypatch) -> None:
    base = datetime(2025, 1, 1, tzinfo=UTC)
    data = pl.DataFrame(
        {
            "created_at": [base + timedelta(hours=i) for i in range(48)],
            "outcome": ["win" if i % 2 else "loss" for i in range(48)],
            "atr_pct": [0.9 for _ in range(48)],
            "adx14": [24 for _ in range(48)],
            "rsi14": [52 for _ in range(48)],
        }
    )
    input_path = tmp_path / "train.parquet"
    data.write_parquet(input_path)

    monkeypatch.setattr(
        "sys.argv",
        [
            "train.py",
            "--start",
            data["created_at"].min().isoformat(),
            "--end",
            data["created_at"].max().isoformat(),
            "--input",
            str(input_path),
            "--window-days",
            "5",
            "--step-days",
            "2",
            "--model-dir",
            str(tmp_path / "models"),
            "--min-windows",
            "1",
        ],
    )
    try:
        train_module.main()
    except SystemExit as exc:
        assert exc.code == 2
    else:
        raise AssertionError("expected SystemExit window-count failure")


def test_train_cli_rejects_invalid_metric_threshold(tmp_path, monkeypatch) -> None:
    base = datetime(2025, 1, 1, tzinfo=UTC)
    data = pl.DataFrame(
        {
            "created_at": [base + timedelta(hours=i) for i in range(300)],
            "outcome": ["win" if i % 2 else "loss" for i in range(300)],
            "atr_pct": [0.8 + (i % 4) * 0.1 for i in range(300)],
            "adx14": [20 + (i % 6) for i in range(300)],
            "rsi14": [40 + (i % 20) for i in range(300)],
        }
    )
    input_path = tmp_path / "train.parquet"
    data.write_parquet(input_path)

    monkeypatch.setattr(
        "sys.argv",
        [
            "train.py",
            "--start",
            data["created_at"].min().isoformat(),
            "--end",
            data["created_at"].max().isoformat(),
            "--input",
            str(input_path),
            "--window-days",
            "5",
            "--step-days",
            "2",
            "--model-dir",
            str(tmp_path / "models"),
            "--min-f1",
            "1.2",
        ],
    )
    try:
        train_module.main()
    except ValueError as exc:
        assert "between 0.0 and 1.0" in str(exc)
    else:
        raise AssertionError("expected ValueError for invalid threshold")


def test_aggregate_metrics_weighted_by_test_rows() -> None:
    report = train_module._aggregate_report_metrics(
        [
            {"accuracy": 1.0, "precision": 1.0, "recall": 1.0, "f1": 1.0, "test_rows": 100},
            {"accuracy": 0.0, "precision": 0.0, "recall": 0.0, "f1": 0.0, "test_rows": 1},
        ]
    )
    assert report["accuracy"] > 0.9
    assert report["f1"] > 0.9


def test_train_cli_reports_multiple_gate_failures(tmp_path, monkeypatch, capsys) -> None:
    base = datetime(2025, 1, 1, tzinfo=UTC)
    data = pl.DataFrame(
        {
            "created_at": [base + timedelta(hours=i) for i in range(300)],
            "outcome": ["loss" for _ in range(300)],
            "atr_pct": [0.8 for _ in range(300)],
            "adx14": [20 for _ in range(300)],
            "rsi14": [45 for _ in range(300)],
        }
    )
    input_path = tmp_path / "train.parquet"
    data.write_parquet(input_path)

    monkeypatch.setattr(
        "sys.argv",
        [
            "train.py",
            "--start",
            data["created_at"].min().isoformat(),
            "--end",
            data["created_at"].max().isoformat(),
            "--input",
            str(input_path),
            "--window-days",
            "5",
            "--step-days",
            "2",
            "--model-dir",
            str(tmp_path / "models"),
            "--min-precision",
            "0.6",
            "--min-recall",
            "0.6",
        ],
    )
    try:
        train_module.main()
    except SystemExit as exc:
        assert exc.code == 2
    else:
        raise AssertionError("expected SystemExit threshold failure")

    payload = json.loads(capsys.readouterr().out.strip())
    assert payload["quality_gate"]["passed"] is False
    assert len(payload["quality_gate"]["failures"]) >= 2
