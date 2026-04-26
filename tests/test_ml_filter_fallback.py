from __future__ import annotations

from types import SimpleNamespace

import polars as pl

from bot.config import BotSettings
from bot.ml.signal_classifier import SignalClassifier
from bot.ml import MLFilter


def _prepared_stub() -> SimpleNamespace:
    frame = pl.DataFrame(
        {
            "adx14": [25.0],
            "rsi14": [52.0],
            "ema20": [100.0],
            "ema50": [99.0],
            "ema200": [97.0],
            "volume_ratio20": [1.1],
            "macd_hist": [0.2],
            "supertrend_dir": [1.0],
            "obv_above_ema": [1.0],
            "bb_pct_b": [0.6],
            "bb_width": [0.04],
        }
    )
    return SimpleNamespace(
        work_4h=frame,
        work_1h=frame,
        work_15m=frame,
        bias_4h="uptrend",
        funding_rate=0.0002,
        oi_current=1000.0,
        oi_change_pct=0.03,
        ls_ratio=1.1,
        liquidation_score=0.0,
        market_regime="trending",
    )


def _signal_stub() -> SimpleNamespace:
    return SimpleNamespace(
        symbol="BTCUSDT",
        setup_id="ema_bounce",
        direction="long",
        score=0.74,
        risk_reward=1.9,
        atr_pct=0.8,
        spread_bps=4.0,
        stop_distance_pct=0.6,
        entry_mid=100.0,
        quote_volume=100000.0,
        orderflow_delta_ratio=0.57,
        bias_4h="uptrend",
    )


def test_ml_filter_uses_signal_classifier_fallback(tmp_path) -> None:
    settings = BotSettings(tg_token="0" * 30, target_chat_id="0")
    settings.data_dir = tmp_path / "bot"
    settings.ml.enabled = True
    settings.ml.use_ml_in_live = True
    settings.ml.model_type = "rf"
    model_dir = settings.ml_dir / "models"

    # train lightweight classifier artifact (fallback path)
    rows = 120
    features = pl.DataFrame(
        {name: [0.2 + (i / rows) for i in range(rows)] for name in SignalClassifier.FEATURES}
    )
    labels = pl.Series("label", [0 if i < (rows // 2) else 1 for i in range(rows)], dtype=pl.Int8)
    SignalClassifier(model_dir=model_dir, model_type="rf").train(features, labels)

    ml_filter = MLFilter(settings)
    result = ml_filter.predict(_signal_stub(), _prepared_stub())
    assert ml_filter.get_status()["signal_classifier_loaded"] is True
    assert 0.0 <= result.probability <= 1.0
