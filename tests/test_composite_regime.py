from __future__ import annotations

from types import SimpleNamespace

import pytest

from bot.regime.composite_regime import CompositeRegimeAnalyzer
from bot.regime.gmm_var import CentroidRegimeDetector, GMMVARRegimeDetector
from bot.regime.hmm_regime import HMMRegimeDetector, RuleBasedRegimeDetector
import polars as pl


def test_composite_regime_uses_hmm_and_gmm_votes(monkeypatch) -> None:
    analyzer = CompositeRegimeAnalyzer()
    monkeypatch.setattr(analyzer.gmm, "current_regime", lambda _features: ("calm_down", 0.8))
    monkeypatch.setattr(
        analyzer.hmm,
        "predict",
        lambda _df: SimpleNamespace(regime="low_vol_uptrend", confidence=0.9),
    )

    result = analyzer.analyze(
        ticker_data=[],
        funding_rates={"BTCUSDT": 0.0001},
        benchmark_context={"BTCUSDT": {"basis_pct": 0.002, "premium_slope_5m": 0.001}},
    )

    # gmm->bear (0.4) + hmm->bull (0.4) + legacy->bull (0.2) => bull
    assert result.regime == "bull"
    assert result.strength == pytest.approx(0.6)


def test_composite_regime_confidence_blends_models(monkeypatch) -> None:
    analyzer = CompositeRegimeAnalyzer()
    monkeypatch.setattr(analyzer.gmm, "current_regime", lambda _features: ("contagion", 0.7))
    monkeypatch.setattr(
        analyzer.hmm,
        "predict",
        lambda _df: SimpleNamespace(regime="high_vol_choppy", confidence=0.5),
    )

    result = analyzer.analyze(
        ticker_data=[],
        funding_rates={"BTCUSDT": 0.0},
        benchmark_context={"BTCUSDT": {"basis_pct": 0.0, "premium_slope_5m": 0.03}},
    )

    assert result.regime == "volatile"
    assert result.confidence == pytest.approx(0.6)


def test_regime_detector_aliases_remain_backward_compatible() -> None:
    assert issubclass(GMMVARRegimeDetector, CentroidRegimeDetector)
    assert issubclass(HMMRegimeDetector, RuleBasedRegimeDetector)


def test_rule_based_detector_fit_predict_does_not_fail() -> None:
    detector = RuleBasedRegimeDetector()
    df = pl.DataFrame(
        {
            "log_returns": [0.001 * ((i % 5) - 2) for i in range(80)],
            "realized_vol": [0.01 + 0.001 * (i % 7) for i in range(80)],
            "atr_pct": [0.8 + 0.01 * (i % 4) for i in range(80)],
        }
    )
    detector.fit(df)
    pred = detector.predict(df)
    assert pred.regime in {"high_vol_choppy", "low_vol_uptrend", "low_vol_downtrend", "ranging"}
    assert 0.0 <= pred.confidence <= 1.0


def test_centroid_detector_fit_predict_does_not_fail() -> None:
    detector = CentroidRegimeDetector(n_regimes=3)
    rows = [
        {"price_change_percent": float((i % 9) - 4), "quote_volume": float(1000 + i * 10), "funding_rate": 0.0001}
        for i in range(90)
    ]
    detector.fit(rows)
    regime, confidence = detector.current_regime({"returns": 0.5, "vol": 0.02, "funding_rate": 0.0001})
    assert regime in {"contagion", "calm_up", "calm_down", "neutral"}
    assert 0.0 <= confidence <= 1.0


def test_composite_regime_uses_history_frame_when_available(monkeypatch) -> None:
    analyzer = CompositeRegimeAnalyzer()
    captured = {}

    monkeypatch.setattr(analyzer.centroid, "current_regime", lambda _features: ("neutral", 0.4))

    def _fake_predict(df):
        captured["height"] = df.height
        return SimpleNamespace(regime="ranging", confidence=0.6)

    monkeypatch.setattr(analyzer.rule_based, "predict", _fake_predict)

    history = pl.DataFrame(
        {
            "log_returns": [0.001, -0.002, 0.003],
            "realized_vol": [0.01, 0.011, 0.012],
            "atr_pct": [0.01, 0.011, 0.012],
        }
    )

    analyzer.analyze(
        ticker_data=[],
        funding_rates={"BTCUSDT": 0.0},
        benchmark_context={"BTCUSDT": {"basis_pct": 0.0, "premium_slope_5m": 0.01, "regime_frame_4h": history}},
    )

    assert captured["height"] == 3
