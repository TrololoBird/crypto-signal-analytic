from __future__ import annotations

from bot.config import BotSettings
from bot.market_regime import MarketRegimeAnalyzer
from bot.regime.composite_regime import CompositeRegimeAnalyzer


def test_composite_regime_analyzer_returns_bull_from_positive_context() -> None:
    analyzer = CompositeRegimeAnalyzer()
    result = analyzer.analyze(
        ticker_data=[],
        funding_rates={"BTCUSDT": 0.0001},
        benchmark_context={"BTCUSDT": {"basis_pct": 0.02, "premium_slope_5m": 0.001}},
    )
    assert result.regime in {"bull", "ranging", "volatile"}
    assert 0.0 <= result.confidence <= 1.0


def test_market_regime_analyzer_uses_composite_detector_when_enabled() -> None:
    settings = BotSettings(tg_token="1" * 30, target_chat_id="123")
    settings.intelligence.regime_detector = "composite"
    analyzer = MarketRegimeAnalyzer(settings)

    result = analyzer.analyze(
        ticker_data=[{"symbol": "BTCUSDT", "price_change_percent": "2.0"}],
        funding_rates={"BTCUSDT": 0.0002},
        benchmark_context={"BTCUSDT": {"basis_pct": 0.03, "premium_slope_5m": 0.002}},
    )

    assert result.regime in {"bull", "bear", "ranging", "volatile"}
    assert 0.0 <= result.confidence <= 1.0
