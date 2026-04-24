from __future__ import annotations

from datetime import datetime, timedelta, timezone

import polars as pl
import pytest

from bot.config import BotSettings
from bot.filters import apply_global_filters
from bot.models import PreparedSymbol, Signal, UniverseSymbol


UTC = timezone.utc


class _DummyConfluenceEngine:
    def score(self, signal: Signal, prepared: PreparedSymbol) -> None:  # pragma: no cover
        raise AssertionError("Scoring should be disabled in these tests")


def _build_prepared(adx_1h: float) -> PreparedSymbol:
    now = datetime.now(UTC)
    universe = UniverseSymbol(
        symbol="BTCUSDT",
        base_asset="BTC",
        quote_asset="USDT",
        contract_type="PERPETUAL",
        status="TRADING",
        onboard_date_ms=0,
        quote_volume=50_000_000.0,
        price_change_pct=0.5,
        last_price=100.0,
    )
    return PreparedSymbol(
        universe=universe,
        work_1h=pl.DataFrame(
            {
                "close_time": [now - timedelta(minutes=30)],
                "adx14": [adx_1h],
            }
        ),
        work_15m=pl.DataFrame(
            {
                "close_time": [now - timedelta(minutes=5)],
                "atr_pct": [1.2],
                "delta_ratio": [0.6],
            }
        ),
        bid_price=99.9,
        ask_price=100.1,
        spread_bps=2.0,
    )


def _base_signal(setup_id: str, strategy_family: str, confirmation_profile: str) -> Signal:
    return Signal(
        symbol="BTCUSDT",
        setup_id=setup_id,
        direction="long",
        score=1.0,
        timeframe="15m",
        entry_low=100.0,
        entry_high=100.0,
        stop=98.0,
        take_profit_1=102.0,
        take_profit_2=104.0,
        strategy_family=strategy_family,
        confirmation_profile=confirmation_profile,
    )


def test_trend_family_keeps_hard_reject_on_low_adx() -> None:
    settings = BotSettings(
        tg_token="",
        target_chat_id="",
        scoring={"enabled": False},
        filters={"min_adx_1h": 20.0, "min_score": 0.0},
    )

    passed, _signal, reason, _scoring, details = apply_global_filters(
        signal=_base_signal("structure_pullback", "continuation", "trend_follow"),
        prepared=_build_prepared(adx_1h=12.0),
        settings=settings,
        confluence_engine=_DummyConfluenceEngine(),
    )

    assert not passed
    assert reason == "regime_not_suitable"
    assert details is not None
    assert details["adx_policy"] == "hard_gate"


def test_reversal_family_uses_score_penalty_on_low_adx() -> None:
    settings = BotSettings(
        tg_token="",
        target_chat_id="",
        scoring={"enabled": False},
        filters={"min_adx_1h": 20.0, "min_score": 0.0},
    )

    passed, signal, reason, _scoring, _details = apply_global_filters(
        signal=_base_signal("wick_trap_reversal", "reversal", "reversal"),
        prepared=_build_prepared(adx_1h=12.0),
        settings=settings,
        confluence_engine=_DummyConfluenceEngine(),
    )

    assert passed
    assert reason is None
    assert signal.score == pytest.approx(0.85)
    assert "adx_1h_penalized" in signal.passed_filters
    assert "adx_penalty_applied" in signal.passed_filters
