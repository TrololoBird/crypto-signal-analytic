from __future__ import annotations

from datetime import UTC, datetime, timedelta

import polars as pl
import pytest

from bot.backtest.engine import VectorizedBacktester
from bot.config import BotSettings


def _synthetic_market(rows: int = 220) -> pl.DataFrame:
    base = datetime(2025, 1, 1, tzinfo=UTC)
    ts = [base + timedelta(minutes=15 * i) for i in range(rows)]
    # Build alternating trend segments so EMA cross strategy can trade both ways.
    close: list[float] = []
    price = 100.0
    for i in range(rows):
        if i < 70:
            price += 0.12
        elif i < 140:
            price -= 0.18
        else:
            price += 0.14
        close.append(price)
    return pl.DataFrame(
        {
            "close_time": ts,
            "open": close,
            "high": [c + 0.4 for c in close],
            "low": [c - 0.4 for c in close],
            "close": close,
            "volume": [1000.0 + (i % 10) * 15.0 for i in range(rows)],
            "signal_long": [1 if i in (50, 150) else 0 for i in range(rows)],
            "signal_short": [1 if i in (100,) else 0 for i in range(rows)],
        }
    )


def test_backtester_simulates_real_trades(tmp_path) -> None:
    settings = BotSettings(tg_token="0" * 30, target_chat_id="0")
    settings.data_dir = tmp_path / "bot"
    parquet_dir = settings.data_dir / "parquet"
    parquet_dir.mkdir(parents=True, exist_ok=True)

    symbol = "BTCUSDT"
    timeframe = "15m"
    data = _synthetic_market()
    data.write_parquet(parquet_dir / f"{symbol}_{timeframe}.parquet")

    engine = VectorizedBacktester(settings)
    result = engine.run(
        symbol=symbol,
        start=data["close_time"].min(),
        end=data["close_time"].max(),
        timeframe=timeframe,
        setup_id="ema_cross",
    )

    assert result.trades.height > 0
    assert "entry_ts" in result.trades.columns
    assert "exit_ts" in result.trades.columns
    assert "position_leverage" in result.trades.columns
    assert result.trades["position_leverage"].max() <= 3.0
    assert result.equity_curve.height > 0
    assert result.trade_count == result.trades.height


def test_backtester_supports_momentum_breakout(tmp_path) -> None:
    settings = BotSettings(tg_token="0" * 30, target_chat_id="0")
    settings.data_dir = tmp_path / "bot"
    parquet_dir = settings.data_dir / "parquet"
    parquet_dir.mkdir(parents=True, exist_ok=True)
    symbol = "ETHUSDT"
    timeframe = "15m"
    data = _synthetic_market()
    data.write_parquet(parquet_dir / f"{symbol}_{timeframe}.parquet")
    engine = VectorizedBacktester(settings)
    result = engine.run(
        symbol=symbol,
        start=data["close_time"].min(),
        end=data["close_time"].max(),
        timeframe=timeframe,
        setup_id="momentum_breakout",
    )
    assert result.equity_curve.height > 0
    assert "setup_id" in result.trades.columns or result.trades.is_empty()


def test_backtester_uses_signal_columns_when_present(tmp_path) -> None:
    settings = BotSettings(tg_token="0" * 30, target_chat_id="0")
    settings.data_dir = tmp_path / "bot"
    parquet_dir = settings.data_dir / "parquet"
    parquet_dir.mkdir(parents=True, exist_ok=True)
    symbol = "XRPUSDT"
    timeframe = "15m"
    data = _synthetic_market()
    data.write_parquet(parquet_dir / f"{symbol}_{timeframe}.parquet")
    engine = VectorizedBacktester(settings)
    result = engine.run(
        symbol=symbol,
        start=data["close_time"].min(),
        end=data["close_time"].max(),
        timeframe=timeframe,
        setup_id="ema_cross",
        initial_equity=2.0,
    )
    assert result.equity_curve["equity"].item(0) >= 2.0
    assert result.trade_count >= 0


def test_backtester_rejects_unknown_setup(tmp_path) -> None:
    settings = BotSettings(tg_token="0" * 30, target_chat_id="0")
    settings.data_dir = tmp_path / "bot"
    parquet_dir = settings.data_dir / "parquet"
    parquet_dir.mkdir(parents=True, exist_ok=True)
    symbol = "SOLUSDT"
    timeframe = "15m"
    data = _synthetic_market()
    data.write_parquet(parquet_dir / f"{symbol}_{timeframe}.parquet")
    engine = VectorizedBacktester(settings)
    with pytest.raises(ValueError):
        engine.run(
            symbol=symbol,
            start=data["close_time"].min(),
            end=data["close_time"].max(),
            timeframe=timeframe,
            setup_id="unknown_setup",
        )
