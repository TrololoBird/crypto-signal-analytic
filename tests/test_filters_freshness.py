from __future__ import annotations

from datetime import datetime, timedelta, timezone

import polars as pl

from bot.filters import _frame_is_fresh


UTC = timezone.utc


def _frame_with_close_time(value: object) -> pl.DataFrame:
    return pl.DataFrame({"close_time": [value]})


def test_frame_is_fresh_with_aware_datetime() -> None:
    frame = _frame_with_close_time(datetime.now(UTC) - timedelta(minutes=1))
    assert _frame_is_fresh(frame, timedelta(minutes=5))


def test_frame_is_fresh_with_naive_datetime() -> None:
    frame = _frame_with_close_time(datetime.utcnow() - timedelta(minutes=1))
    assert _frame_is_fresh(frame, timedelta(minutes=5))


def test_frame_is_fresh_with_iso_string() -> None:
    close_time = (datetime.now(UTC) - timedelta(minutes=1)).isoformat().replace("+00:00", "Z")
    frame = _frame_with_close_time(close_time)
    assert _frame_is_fresh(frame, timedelta(minutes=5))


def test_frame_is_fresh_with_invalid_value() -> None:
    frame = _frame_with_close_time({"bad": "value"})
    assert not _frame_is_fresh(frame, timedelta(minutes=5))
