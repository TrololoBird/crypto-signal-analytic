from __future__ import annotations

from datetime import datetime

import polars as pl


class MLTrainingPipeline:
    def generate_labels(self, tracking_data: pl.DataFrame, horizon_bars: int = 48) -> pl.Series:
        _ = horizon_bars
        if tracking_data.is_empty():
            return pl.Series("label", [], dtype=pl.Int8)
        return pl.Series("label", [0] * tracking_data.height, dtype=pl.Int8)

    def walk_forward_train(
        self,
        start_date: datetime,
        end_date: datetime,
        window_days: int = 90,
        step_days: int = 30,
    ) -> list[dict[str, str]]:
        _ = (start_date, end_date, window_days, step_days)
        return []
