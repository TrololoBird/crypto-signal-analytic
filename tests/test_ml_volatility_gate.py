from __future__ import annotations

from types import SimpleNamespace

from bot.config import BotSettings
from bot.confluence import ConfluenceEngine
from bot.ml.volatility_gate import VolatilityGate
from bot.models import Signal


class _MLResult:
    def __init__(self, probability: float, confidence: float = 1.0) -> None:
        self.probability = probability
        self.confidence = confidence
        self.error = None
        self.is_confident = True


class _MLFilter:
    enabled = True

    def __init__(self, probability: float) -> None:
        self._probability = probability

    def predict(self, signal, prepared):
        return _MLResult(probability=self._probability)


def _signal(score: float = 0.6) -> Signal:
    return Signal(
        symbol="BTCUSDT",
        setup_id="ema_bounce",
        direction="long",
        score=score,
        timeframe="15m",
        entry_low=100.0,
        entry_high=100.1,
        stop=99.0,
        take_profit_1=101.0,
        take_profit_2=102.0,
    )


def test_volatility_gate_rules() -> None:
    gate = VolatilityGate()
    assert gate.should_use_ml("ranging", 0.6) is True
    assert gate.should_use_ml("ranging", 0.4) is False
    assert gate.should_use_ml("bull", 0.9) is False


def test_confluence_ml_adjustment_is_capped_and_gated() -> None:
    settings = BotSettings(tg_token="1" * 30, target_chat_id="123")
    engine = ConfluenceEngine(settings, ml_filter=_MLFilter(probability=1.0))
    engine._compute_components = lambda signal, prepared, cfg: []  # type: ignore[method-assign]

    prepared_ranging = SimpleNamespace(market_regime="ranging")
    result_ranging = engine.score(_signal(0.6), prepared_ranging)
    assert result_ranging.ml_applied is True
    assert result_ranging.final_score == 0.54

    prepared_bull = SimpleNamespace(market_regime="bull")
    result_bull = engine.score(_signal(0.6), prepared_bull)
    assert result_bull.ml_applied is False
    assert result_bull.final_score == 0.39
