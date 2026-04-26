from __future__ import annotations

from datetime import datetime

from bot.backtest.engine import VectorizedBacktester
from bot.config import BotSettings


def test_backtester_returns_empty_result_when_no_data() -> None:
    settings = BotSettings(tg_token="1" * 30, target_chat_id="123")
    backtester = VectorizedBacktester(settings)
    result = backtester.run(
        symbol="BTCUSDT",
        start=datetime(2025, 1, 1),
        end=datetime(2025, 2, 1),
    )
    assert result.total_return == 0.0
    assert result.equity_curve.height == 1
