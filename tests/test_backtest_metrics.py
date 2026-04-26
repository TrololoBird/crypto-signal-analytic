from __future__ import annotations

from datetime import UTC, datetime, timedelta

import polars as pl

from bot.backtest.metrics import BacktestResult


def test_backtest_result_from_frames_computes_metrics() -> None:
    ts0 = datetime(2025, 1, 1, tzinfo=UTC)
    trades = pl.DataFrame({"ret": [0.1, -0.05, 0.03]})
    equity = pl.DataFrame(
        {
            "ts": [ts0 + timedelta(hours=i) for i in range(4)],
            "equity": [1.0, 1.1, 1.045, 1.07635],
        }
    )
    result = BacktestResult.from_frames(trades=trades, equity_curve=equity)
    assert result.total_return > 0
    assert 0.0 <= result.win_rate <= 1.0
    assert result.max_drawdown >= 0.0
    assert result.trade_count == 3
    assert result.expectancy != 0.0
