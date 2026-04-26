from __future__ import annotations

from datetime import UTC, datetime, timedelta

import polars as pl

from bot.config import BotSettings
from bot.filters import apply_global_filters
from bot.models import PreparedSymbol, Signal, UniverseSymbol
from bot.scoring import ScoringResult


class _ConfluenceStub:
    class _Result:
        def __init__(self, score: float) -> None:
            self.final_score = score
            self.ml_probability = None

        def to_scoring_result(self) -> ScoringResult:
            return ScoringResult(base_score=0.7, adjustments={}, final_score=self.final_score)

    def score(self, signal: Signal, prepared: PreparedSymbol) -> _Result:
        return _ConfluenceStub._Result(score=signal.score)


def _settings() -> BotSettings:
    return BotSettings(tg_token="1" * 30, target_chat_id="123")


def _frame(minutes_ago: int, atr_pct: float = 1.0, adx14: float = 30.0) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "close_time": [datetime.now(UTC) - timedelta(minutes=minutes_ago)],
            "atr_pct": [atr_pct],
            "adx14": [adx14],
            "delta_ratio": [0.5],
            "volume_ratio20": [1.2],
            "low": [99.0],
            "high": [101.0],
            "ema20": [100.0],
            "atr14": [1.0],
        }
    )


def _prepared(minutes_ago_15m: int = 5, minutes_ago_1h: int = 30) -> PreparedSymbol:
    universe = UniverseSymbol(
        symbol="BTCUSDT",
        base_asset="BTC",
        quote_asset="USDT",
        contract_type="PERPETUAL",
        status="TRADING",
        onboard_date_ms=0,
        quote_volume=1_000_000.0,
        price_change_pct=0.0,
        last_price=100.0,
    )
    return PreparedSymbol(
        universe=universe,
        work_1h=_frame(minutes_ago_1h),
        work_15m=_frame(minutes_ago_15m),
        bid_price=99.9,
        ask_price=100.1,
        spread_bps=5.0,
        mark_price=100.0,
        ticker_price=100.0,
    )


def _signal() -> Signal:
    return Signal(
        symbol="BTCUSDT",
        setup_id="ema_bounce",
        direction="long",
        score=0.8,
        timeframe="15m",
        entry_low=100.0,
        entry_high=100.2,
        stop=99.0,
        take_profit_1=102.0,
        take_profit_2=103.0,
    )


def test_apply_global_filters_accepts_valid_signal() -> None:
    accepted, updated, reason, scoring, details = apply_global_filters(
        _signal(), _prepared(), _settings(), _ConfluenceStub()
    )

    assert accepted is True
    assert reason is None
    assert scoring is not None
    assert details is None
    assert "fresh_15m" in updated.passed_filters
    assert updated.atr_pct is not None


def test_apply_global_filters_rejects_stale_15m() -> None:
    accepted, _, reason, _, _ = apply_global_filters(
        _signal(), _prepared(minutes_ago_15m=120), _settings(), _ConfluenceStub()
    )

    assert accepted is False
    assert reason == "stale_15m"
