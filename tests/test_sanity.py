from __future__ import annotations

import polars as pl
import pytest

from bot.features import _adx, _supertrend, _swing_points
from bot.market_regime import MarketRegimeAnalyzer
from bot.models import Signal
from bot.strategies import STRATEGY_CLASSES
from bot.analytics import StrategyAnalytics
from types import SimpleNamespace


def _frame(high: list[float], low: list[float], close: list[float]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "open": close,
            "high": high,
            "low": low,
            "close": close,
            "volume": [1000.0] * len(close),
        }
    )


def test_swing_points_with_unconfirmed_tail_marks_recent_bars() -> None:
    frame = _frame(
        high=[10.0, 11.0, 12.0, 11.0, 10.0, 13.0, 12.0],
        low=[9.0, 10.0, 11.0, 10.0, 9.0, 12.0, 11.0],
        close=[9.5, 10.5, 11.5, 10.5, 9.5, 12.5, 11.5],
    )
    sh, sl = _swing_points(frame, n=2, include_unconfirmed_tail=True)
    assert sh.len() == frame.height
    assert sl.len() == frame.height
    assert any(bool(v) for v in sh.tail(2).to_list()) or any(bool(v) for v in sl.tail(2).to_list())


def test_adx_no_inf_no_nan() -> None:
    frame = _frame(
        high=[10.0] * 30,
        low=[10.0] * 30,
        close=[10.0] * 30,
    )
    adx = _adx(frame, period=14)
    assert adx.is_nan().sum() == 0
    assert adx.is_infinite().sum() == 0


def test_supertrend_iterative_output_shape_and_domain() -> None:
    frame = _frame(
        high=[10, 11, 12, 11, 10, 9, 8, 9, 10, 11],
        low=[9, 10, 11, 10, 9, 8, 7, 8, 9, 10],
        close=[9.5, 10.5, 11.5, 10.5, 9.5, 8.5, 7.5, 8.5, 9.5, 10.5],
    )
    st, direction = _supertrend(frame, period=3, multiplier=3.0)
    assert st.len() == frame.height
    assert direction.len() == frame.height
    unique_dir = {float(v) for v in direction.unique().to_list()}
    assert unique_dir.issubset({-1.0, 1.0})


def test_signal_post_init_respects_explicit_risk_reward() -> None:
    signal = Signal(
        symbol="BTCUSDT",
        setup_id="sanity",
        direction="long",
        score=0.7,
        timeframe="1h",
        entry_low=99.0,
        entry_high=101.0,
        stop=90.0,
        take_profit_1=110.0,
        take_profit_2=120.0,
        risk_reward=5.0,
    )
    assert signal.risk_reward == 5.0


def test_strategy_registry_contains_extended_setups() -> None:
    strategy_ids = {cls().setup_id for cls in STRATEGY_CLASSES}
    assert "bos_choch" in strategy_ids
    assert "breaker_block" in strategy_ids
    assert "cvd_divergence" in strategy_ids
    assert "hidden_divergence" in strategy_ids
    assert "session_killzone" in strategy_ids
    assert "squeeze_setup" in strategy_ids


@pytest.mark.asyncio
async def test_strategy_analytics_report_shape() -> None:
    class RepoStub:
        async def get_setup_stats(self, *, last_days: int = 30):
            return [{"setup_id": "ema_bounce", "total": 2, "win_rate": 0.5, "avg_r_multiple": 0.2}]

        async def get_signal_outcomes(self, *, last_days: int = 30):
            return [
                {"setup_id": "ema_bounce", "pnl_r_multiple": 1.0},
                {"setup_id": "ema_bounce", "pnl_r_multiple": -0.6},
            ]

    report = await StrategyAnalytics(repo=RepoStub()).generate_report(days=14)  # type: ignore[arg-type]
    assert report["window_days"] == 14
    assert report["total_trades"] == 2
    assert len(report["setup_reports"]) == 1


def test_market_regime_exposes_extended_phase_fields() -> None:
    settings = SimpleNamespace()
    analyzer = MarketRegimeAnalyzer(settings=settings)  # type: ignore[arg-type]
    result = analyzer.analyze(
        ticker_data=[
            {"symbol": "BTCUSDT", "price_change_percent": 2.4},
            {"symbol": "ETHUSDT", "price_change_percent": 1.1},
            {"symbol": "SOLUSDT", "price_change_percent": 4.8},
            {"symbol": "XRPUSDT", "price_change_percent": 3.2},
        ],
        funding_rates={"BTCUSDT": 0.004, "ETHUSDT": 0.003},
        benchmark_context={
            "BTCUSDT": {"bias": "uptrend", "oi_change_pct": 0.06},
            "ETHUSDT": {"bias": "uptrend", "oi_change_pct": 0.04},
        },
    )
    payload = result.to_dict()
    assert payload["volatility_regime"] in {"expanding", "contracting", "stable"}
    assert payload["risk_on_off"] in {"risk_on", "risk_off", "neutral"}
    assert payload["btc_phase"] in {"accumulation", "markup", "distribution", "decline", "sideways"}
    assert 0.0 <= payload["confidence"] <= 1.0
