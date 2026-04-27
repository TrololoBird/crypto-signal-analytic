from __future__ import annotations

import polars as pl


def add_microstructure_features(df: pl.DataFrame) -> pl.DataFrame:
    """Add lightweight microstructure features from available L1/flow columns."""
    result = df
    if "delta_ratio" in result.columns:
        result = result.with_columns([
            ((pl.col("delta_ratio") - 0.5) * 2.0).clip(-1.0, 1.0).alias("signed_order_flow"),
        ])
    else:
        result = result.with_columns([pl.lit(0.0).alias("signed_order_flow")])

    if {"bid_qty", "ask_qty"}.issubset(result.columns):
        denom = (pl.col("bid_qty") + pl.col("ask_qty")).clip(lower_bound=1e-9)
        result = result.with_columns([
            ((pl.col("bid_qty") - pl.col("ask_qty")) / denom)
            .clip(-1.0, 1.0)
            .fill_nan(0.0)
            .alias("tob_imbalance"),
        ])
    else:
        result = result.with_columns([pl.lit(0.0).alias("tob_imbalance")])

    if {"bid_price", "ask_price", "bid_qty", "ask_qty", "close"}.issubset(result.columns):
        microprice = (
            (pl.col("ask_price") * pl.col("bid_qty")) + (pl.col("bid_price") * pl.col("ask_qty"))
        ) / (pl.col("bid_qty") + pl.col("ask_qty")).clip(lower_bound=1e-9)
        result = result.with_columns([
            (((microprice - pl.col("close")) / pl.col("close")).fill_nan(0.0) * 100.0)
            .clip(-2.0, 2.0)
            .alias("microprice_deviation_pct"),
        ])
    else:
        result = result.with_columns([pl.lit(0.0).alias("microprice_deviation_pct")])

    return result
