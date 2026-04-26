from __future__ import annotations

from datetime import UTC, datetime, timedelta

import polars as pl

from bot.features import _prepare_frame


def _ohlcv(rows: int = 260) -> pl.DataFrame:
    base = datetime(2026, 1, 1, tzinfo=UTC)
    times = [base + timedelta(minutes=15 * i) for i in range(rows)]
    closes = [100.0 + (i * 0.1) for i in range(rows)]
    return pl.DataFrame(
        {
            "open_time": times,
            "close_time": times,
            "open": [c - 0.2 for c in closes],
            "high": [c + 0.5 for c in closes],
            "low": [c - 0.5 for c in closes],
            "close": closes,
            "volume": [1000.0 + i for i in range(rows)],
            "quote_volume": [100000.0 + (10 * i) for i in range(rows)],
            "trades": [100 + i for i in range(rows)],
            "taker_buy_base_volume": [500.0 + (i * 0.1) for i in range(rows)],
            "taker_buy_quote_volume": [50000.0 + (i * 10) for i in range(rows)],
        }
    )


def test_prepare_frame_emits_expected_columns_without_nan() -> None:
    prepared = _prepare_frame(_ohlcv())

    assert prepared.height > 0
    for col in ["ema20", "ema50", "ema200", "atr_pct", "delta_ratio", "vwap_deviation_pct"]:
        assert col in prepared.columns
    assert prepared["ema200"].null_count() == 0
    assert prepared["atr_pct"].null_count() == 0
