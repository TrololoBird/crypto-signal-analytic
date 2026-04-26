from __future__ import annotations

from datetime import UTC, datetime

import polars as pl
import pytest

from bot.config import BotSettings
from bot.models import PreparedSymbol, UniverseSymbol
from bot.strategies import STRATEGY_CLASSES


def _prepared_symbol() -> PreparedSymbol:
    now = datetime.now(UTC)
    frame = pl.DataFrame(
        {
            "open_time": [now],
            "close_time": [now],
            "open": [100.0],
            "high": [101.0],
            "low": [99.0],
            "close": [100.0],
            "volume": [1000.0],
            "ema20": [100.0],
            "ema50": [100.0],
            "ema200": [100.0],
            "atr14": [1.0],
            "adx14": [30.0],
            "delta_ratio": [0.5],
            "volume_ratio20": [1.0],
        }
    )
    universe = UniverseSymbol(
        symbol="BTCUSDT",
        base_asset="BTC",
        quote_asset="USDT",
        contract_type="PERPETUAL",
        status="TRADING",
        onboard_date_ms=0,
        quote_volume=1_000_000,
        price_change_pct=0.0,
        last_price=100.0,
    )
    return PreparedSymbol(
        universe=universe,
        work_1h=frame,
        work_15m=frame,
        bid_price=99.9,
        ask_price=100.1,
        spread_bps=5.0,
    )


@pytest.mark.parametrize("strategy_cls", STRATEGY_CLASSES)
def test_strategy_metadata_and_calculate_contract(strategy_cls: type) -> None:
    settings = BotSettings(tg_token="1" * 30, target_chat_id="123")
    strategy = strategy_cls(settings=settings)
    result = strategy.calculate(_prepared_symbol())

    assert strategy.metadata.strategy_id
    assert isinstance(strategy.get_optimizable_params(settings), dict)
    assert result.setup_id == strategy.metadata.strategy_id
