from __future__ import annotations

from types import SimpleNamespace

import polars as pl

from bot.config import BotSettings
from bot.confluence import ComponentScore, ConfluenceEngine
from bot.models import PreparedSymbol, Signal, UniverseSymbol


class _StubMLFilter:
    def __init__(self, *, enabled: bool, result: object | None = None) -> None:
        self.enabled = enabled
        self._result = result

    def predict(self, signal: Signal, prepared: PreparedSymbol) -> object:
        _ = (signal, prepared)
        if self._result is None:
            raise RuntimeError("predict not configured")
        return self._result


def _signal() -> Signal:
    return Signal(
        symbol="BTCUSDT",
        setup_id="ema_cross",
        direction="long",
        score=0.6,
        timeframe="15m",
        entry_low=100.0,
        entry_high=101.0,
        stop=98.0,
        take_profit_1=103.0,
        take_profit_2=104.0,
    )


def _prepared(*, regime: str = "neutral") -> PreparedSymbol:
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
    empty = pl.DataFrame()
    return PreparedSymbol(
        universe=universe,
        work_1h=empty,
        work_15m=empty,
        bid_price=None,
        ask_price=None,
        spread_bps=None,
        market_regime=regime,
    )


def _engine(settings: BotSettings, ml_filter) -> ConfluenceEngine:
    engine = ConfluenceEngine(settings, ml_filter=ml_filter)
    engine._compute_components = lambda *_args, **_kwargs: [ComponentScore("stub", 1.0, 0.3, 0.3)]  # type: ignore[method-assign]
    return engine


def test_confluence_sets_skip_reason_when_ml_filter_absent() -> None:
    settings = BotSettings(tg_token="0" * 30, target_chat_id="0")
    result = _engine(settings, ml_filter=None).score(_signal(), _prepared())
    assert result.ml_applied is False
    assert result.ml_skip_reason == "ml_filter_absent"


def test_confluence_sets_skip_reason_when_gate_blocks_ml() -> None:
    settings = BotSettings(tg_token="0" * 30, target_chat_id="0")
    ml_result = SimpleNamespace(error=None, is_confident=True, probability=0.8, confidence=0.9)
    result = _engine(settings, ml_filter=_StubMLFilter(enabled=True, result=ml_result)).score(
        _signal(),
        _prepared(regime="bull"),
    )
    assert result.ml_applied is False
    assert result.ml_skip_reason == "volatility_gate_blocked"


def test_confluence_applies_ml_and_clears_skip_reason() -> None:
    settings = BotSettings(tg_token="0" * 30, target_chat_id="0")
    ml_result = SimpleNamespace(error=None, is_confident=True, probability=0.9, confidence=0.95)
    result = _engine(settings, ml_filter=_StubMLFilter(enabled=True, result=ml_result)).score(
        _signal(),
        _prepared(regime="neutral"),
    )
    assert result.ml_applied is True
    assert result.ml_skip_reason is None
    assert result.to_dict()["ml_skip_reason"] is None
