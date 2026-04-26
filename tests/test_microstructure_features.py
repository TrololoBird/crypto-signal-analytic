from __future__ import annotations

from datetime import UTC, datetime, timedelta

import polars as pl

from bot.features import _prepare_frame


def _sample_ohlcv(rows: int = 260) -> pl.DataFrame:
    base = datetime(2026, 1, 1, tzinfo=UTC)
    ts = [base + timedelta(minutes=15 * i) for i in range(rows)]
    close = [100.0 + (i * 0.05) for i in range(rows)]
    return pl.DataFrame(
        {
            "open_time": ts,
            "open": close,
            "high": [c + 0.3 for c in close],
            "low": [c - 0.3 for c in close],
            "close": close,
            "volume": [1000.0 + (i % 10) * 30.0 for i in range(rows)],
            "quote_volume": [100000.0 + i * 25.0 for i in range(rows)],
            "taker_buy_base_volume": [520.0 + (i % 7) * 12.0 for i in range(rows)],
            "bid_price": [c - 0.02 for c in close],
            "ask_price": [c + 0.02 for c in close],
            "bid_qty": [50.0 + (i % 5) for i in range(rows)],
            "ask_qty": [48.0 + (i % 3) for i in range(rows)],
        }
    )


def test_prepare_frame_adds_microstructure_columns() -> None:
    frame = _prepare_frame(_sample_ohlcv())

    for col in ("signed_order_flow", "tob_imbalance", "microprice_deviation_pct"):
        assert col in frame.columns
        assert frame[col].null_count() == 0

    # Bounded columns should stay inside clipping range.
    assert frame["signed_order_flow"].min() >= -1.0
    assert frame["signed_order_flow"].max() <= 1.0
    assert frame["tob_imbalance"].min() >= -1.0
    assert frame["tob_imbalance"].max() <= 1.0
