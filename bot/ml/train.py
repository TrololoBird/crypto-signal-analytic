from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

import polars as pl

from .signal_classifier import SignalClassifier
from .training_pipeline import MLTrainingPipeline


def main() -> None:
    parser = argparse.ArgumentParser(description="Walk-forward ML training")
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--input", required=True, help="Path to parquet/csv training dataset")
    parser.add_argument("--window-days", type=int, default=90)
    parser.add_argument("--step-days", type=int, default=30)
    parser.add_argument("--model-dir", default="data/bot/ml/models")
    parser.add_argument("--report", default="", help="Optional path to write JSON report")
    parser.add_argument(
        "--min-windows",
        type=int,
        default=0,
        help="Fail with non-zero exit code when evaluated windows are below this threshold",
    )
    parser.add_argument(
        "--min-accuracy",
        type=float,
        default=0.0,
        help="Fail with non-zero exit code when aggregate accuracy is below this threshold",
    )
    parser.add_argument(
        "--min-precision",
        type=float,
        default=0.0,
        help="Fail with non-zero exit code when aggregate precision is below this threshold",
    )
    parser.add_argument(
        "--min-recall",
        type=float,
        default=0.0,
        help="Fail with non-zero exit code when aggregate recall is below this threshold",
    )
    parser.add_argument(
        "--min-f1",
        type=float,
        default=0.0,
        help="Fail with non-zero exit code when aggregate F1 is below this threshold",
    )
    args = parser.parse_args()
    _validate_gate_args(
        min_windows=args.min_windows,
        min_accuracy=args.min_accuracy,
        min_precision=args.min_precision,
        min_recall=args.min_recall,
        min_f1=args.min_f1,
    )

    input_path = Path(args.input)
    if not input_path.exists():
        raise FileNotFoundError(f"training dataset not found: {input_path}")
    if input_path.suffix.lower() == ".parquet":
        dataset = pl.read_parquet(input_path)
    elif input_path.suffix.lower() in {".csv", ".txt"}:
        dataset = pl.read_csv(input_path)
    else:
        raise ValueError(f"unsupported input format: {input_path.suffix}")

    classifier = SignalClassifier(model_dir=Path(args.model_dir))
    pipeline = MLTrainingPipeline(classifier=classifier, model_dir=Path(args.model_dir))
    reports = pipeline.walk_forward_train(
        start_date=datetime.fromisoformat(args.start),
        end_date=datetime.fromisoformat(args.end),
        window_days=args.window_days,
        step_days=args.step_days,
        dataset=dataset,
    )
    payload = {
        "windows": reports,
        "count": len(reports),
        "summary": _aggregate_report_metrics(reports),
    }
    gate_failures = _evaluate_gate_failures(
        payload=payload,
        min_windows=args.min_windows,
        min_accuracy=args.min_accuracy,
        min_precision=args.min_precision,
        min_recall=args.min_recall,
        min_f1=args.min_f1,
    )
    payload["quality_gate"] = {"passed": len(gate_failures) == 0, "failures": gate_failures}
    if args.report:
        report_path = Path(args.report)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False))
    if gate_failures:
        print("; ".join(gate_failures), file=sys.stderr)
        raise SystemExit(2)


def _aggregate_report_metrics(reports: list[dict[str, float | int | str]]) -> dict[str, float]:
    if not reports:
        return {"accuracy": 0.0, "precision": 0.0, "recall": 0.0, "f1": 0.0}

    def _weighted_mean(name: str) -> float:
        weighted_sum = 0.0
        weight_total = 0.0
        for window in reports:
            weight = max(float(window.get("test_rows", 0)), 0.0)
            value = float(window.get(name, 0.0))
            weighted_sum += value * weight
            weight_total += weight
        if weight_total == 0.0:
            values = [float(window.get(name, 0.0)) for window in reports]
            return sum(values) / max(len(values), 1)
        return weighted_sum / weight_total

    return {
        "accuracy": _weighted_mean("accuracy"),
        "precision": _weighted_mean("precision"),
        "recall": _weighted_mean("recall"),
        "f1": _weighted_mean("f1"),
    }


def _evaluate_gate_failures(
    payload: dict[str, object],
    min_windows: int,
    min_accuracy: float,
    min_precision: float,
    min_recall: float,
    min_f1: float,
) -> list[str]:
    raw_count = payload.get("count", 0)
    count = int(raw_count) if isinstance(raw_count, (int, float, str)) else 0
    raw_summary = payload.get("summary", {})
    summary = raw_summary if isinstance(raw_summary, dict) else {}
    failures: list[str] = []

    if count < min_windows:
        failures.append(f"window count {count} is below min-windows {min_windows}")

    thresholds = {
        "accuracy": min_accuracy,
        "precision": min_precision,
        "recall": min_recall,
        "f1": min_f1,
    }
    for metric, threshold in thresholds.items():
        value = float(summary.get(metric, 0.0))
        if value < threshold:
            failures.append(f"aggregate {metric} {value:.4f} is below min-{metric} {threshold:.4f}")

    return failures


def _validate_gate_args(
    min_windows: int,
    min_accuracy: float,
    min_precision: float,
    min_recall: float,
    min_f1: float,
) -> None:
    if min_windows < 0:
        raise ValueError("--min-windows must be >= 0")
    for name, threshold in {
        "min-accuracy": min_accuracy,
        "min-precision": min_precision,
        "min-recall": min_recall,
        "min-f1": min_f1,
    }.items():
        if threshold < 0.0 or threshold > 1.0:
            raise ValueError(f"--{name} must be between 0.0 and 1.0")


if __name__ == "__main__":
    main()
