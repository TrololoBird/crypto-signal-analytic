from __future__ import annotations

import math

import numpy as np
import polars as pl


def ichimoku_lines(df: pl.DataFrame) -> tuple[pl.Series, pl.Series, pl.Series, pl.Series]:
    tenkan = (
        (df["high"].rolling_max(window_size=9) + df["low"].rolling_min(window_size=9)) / 2.0
    ).rename("tenkan")
    kijun = (
        (df["high"].rolling_max(window_size=26) + df["low"].rolling_min(window_size=26)) / 2.0
    ).rename("kijun")
    senkou_a = (((tenkan + kijun) / 2.0).shift(26)).rename("senkou_a")
    senkou_b = (
        ((df["high"].rolling_max(window_size=52) + df["low"].rolling_min(window_size=52)) / 2.0)
        .shift(26)
        .rename("senkou_b")
    )
    return tenkan, kijun, senkou_a, senkou_b


def weighted_moving_average(series: pl.Series, period: int, *, name: str) -> pl.Series:
    n = max(1, int(period))
    values = series.fill_null(strategy="forward").fill_null(0.0).to_numpy()
    out = np.full(values.shape[0], np.nan, dtype=float)
    weights = np.arange(1, n + 1, dtype=float)
    denom = float(weights.sum()) if weights.size else 1.0

    if values.size >= n:
        for i in range(n - 1, values.size):
            window = values[i - n + 1 : i + 1]
            out[i] = float((window * weights).sum() / denom)
    return pl.Series(name, out).fill_nan(0.0)


def hull_moving_average(close: pl.Series, period: int = 21, *, name: str = "hma21") -> pl.Series:
    period = max(2, int(period))
    half = max(1, period // 2)
    sqrt_n = max(1, int(math.sqrt(period)))

    wma_half = weighted_moving_average(close, half, name=f"{name}_half")
    wma_full = weighted_moving_average(close, period, name=f"{name}_full")
    raw = (2.0 * wma_half - wma_full).rename(f"{name}_raw")
    return weighted_moving_average(raw, sqrt_n, name=name)
