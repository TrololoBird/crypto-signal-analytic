from __future__ import annotations

from datetime import UTC, datetime, timedelta

import polars as pl

from bot.ml.training_pipeline import MLTrainingPipeline


def _dataset(rows: int = 240) -> pl.DataFrame:
    base = datetime(2025, 1, 1, tzinfo=UTC)
    ts = [base + timedelta(hours=i) for i in range(rows)]
    close = [100.0 + (i * 0.08 if i < 120 else (240 - i) * 0.05) for i in range(rows)]
    outcome = ["win" if i % 3 != 0 else "loss" for i in range(rows)]
    return pl.DataFrame(
        {
            "created_at": ts,
            "close": close,
            "outcome": outcome,
            "atr_pct": [0.8 + (i % 10) * 0.01 for i in range(rows)],
            "adx14": [20.0 + (i % 7) for i in range(rows)],
            "rsi14": [45.0 + (i % 20) for i in range(rows)],
        }
    )


def test_walk_forward_train_produces_reports(tmp_path) -> None:
    data = _dataset()
    pipeline = MLTrainingPipeline(model_dir=tmp_path / "ml")
    reports = pipeline.walk_forward_train(
        start_date=data["created_at"].min(),
        end_date=data["created_at"].max(),
        window_days=3,
        step_days=1,
        dataset=data,
    )
    assert reports
    assert "accuracy" in reports[0]
