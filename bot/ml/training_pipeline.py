from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl

from .signal_classifier import SignalClassifier


@dataclass(frozen=True)
class WindowTrainingReport:
    train_start: str
    train_end: str
    test_end: str
    train_rows: int
    test_rows: int
    accuracy: float
    precision: float
    recall: float
    f1: float

    def to_dict(self) -> dict[str, float | int | str]:
        return asdict(self)


class MLTrainingPipeline:
    def __init__(self, classifier: SignalClassifier | None = None, model_dir: Path | None = None) -> None:
        self.classifier = classifier or SignalClassifier(model_dir=model_dir or Path("data/bot/ml/models"))

    def generate_labels(self, tracking_data: pl.DataFrame, horizon_bars: int = 48) -> pl.Series:
        if tracking_data.is_empty():
            return pl.Series("label", [], dtype=pl.Int8)
        if "outcome" in tracking_data.columns:
            return (
                tracking_data.get_column("outcome")
                .cast(pl.Utf8)
                .map_elements(lambda v: 1 if str(v).lower() == "win" else 0, return_dtype=pl.Int8)
                .alias("label")
            )
        if "pnl" in tracking_data.columns:
            return tracking_data.get_column("pnl").cast(pl.Float64).map_elements(
                lambda v: 1 if float(v or 0.0) > 0 else 0,
                return_dtype=pl.Int8,
            ).alias("label")
        if "close" in tracking_data.columns:
            future_ret = (
                (pl.col("close").shift(-horizon_bars) / pl.col("close")) - 1.0
            )
            frame = tracking_data.with_columns([future_ret.alias("future_ret")])
            return frame.get_column("future_ret").fill_null(0.0).map_elements(
                lambda v: 1 if float(v or 0.0) > 0 else 0,
                return_dtype=pl.Int8,
            ).alias("label")
        return pl.Series("label", [0] * tracking_data.height, dtype=pl.Int8)

    def walk_forward_train(
        self,
        start_date: datetime,
        end_date: datetime,
        window_days: int = 90,
        step_days: int = 30,
        dataset: pl.DataFrame | None = None,
    ) -> list[dict[str, float | int | str]]:
        if dataset is None or dataset.is_empty():
            return []
        ts_col = "created_at" if "created_at" in dataset.columns else ("ts" if "ts" in dataset.columns else "")
        if not ts_col:
            return []

        results: list[dict[str, float | int | str]] = []
        cursor = start_date
        step = timedelta(days=step_days)
        window = timedelta(days=window_days)

        while cursor + window < end_date:
            train_start = cursor
            train_end = cursor + window
            test_end = min(train_end + step, end_date)
            train_df = dataset.filter((pl.col(ts_col) >= train_start) & (pl.col(ts_col) < train_end))
            test_df = dataset.filter((pl.col(ts_col) >= train_end) & (pl.col(ts_col) < test_end))
            if train_df.height < 30 or test_df.height < 10:
                cursor += step
                continue

            labels = self.generate_labels(train_df)
            if labels.n_unique() < 2:
                cursor += step
                continue

            x_train = self._extract_features(train_df)
            self.classifier.train(x_train, labels)

            test_labels = self.generate_labels(test_df)
            x_test = self._extract_features(test_df)
            preds = [
                1 if self.classifier.predict_proba(row) >= 0.5 else 0
                for row in x_test.to_dicts()
            ]
            y_true = test_labels.to_list()
            metrics = self._classification_metrics(y_true, preds)
            report = WindowTrainingReport(
                train_start=train_start.isoformat(),
                train_end=train_end.isoformat(),
                test_end=test_end.isoformat(),
                train_rows=train_df.height,
                test_rows=test_df.height,
                accuracy=metrics["accuracy"],
                precision=metrics["precision"],
                recall=metrics["recall"],
                f1=metrics["f1"],
            )
            results.append(report.to_dict())
            cursor += step

        return results

    def _extract_features(self, frame: pl.DataFrame) -> pl.DataFrame:
        result = frame
        for name in SignalClassifier.FEATURES:
            if name not in result.columns:
                result = result.with_columns([pl.lit(0.0).alias(name)])
        return result.select(SignalClassifier.FEATURES).cast(pl.Float64).fill_null(0.0)

    @staticmethod
    def _classification_metrics(y_true: list[int], y_pred: list[int]) -> dict[str, float]:
        n = max(len(y_true), 1)
        correct = sum(1 for p, y in zip(y_pred, y_true, strict=False) if int(p) == int(y))
        tp = sum(1 for p, y in zip(y_pred, y_true, strict=False) if int(p) == 1 and int(y) == 1)
        fp = sum(1 for p, y in zip(y_pred, y_true, strict=False) if int(p) == 1 and int(y) == 0)
        fn = sum(1 for p, y in zip(y_pred, y_true, strict=False) if int(p) == 0 and int(y) == 1)
        precision = tp / max(tp + fp, 1)
        recall = tp / max(tp + fn, 1)
        f1 = 0.0 if (precision + recall) == 0 else (2 * precision * recall) / (precision + recall)
        return {
            "accuracy": correct / n,
            "precision": precision,
            "recall": recall,
            "f1": f1,
        }
