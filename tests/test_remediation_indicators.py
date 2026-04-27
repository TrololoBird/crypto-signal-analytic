from __future__ import annotations

import collections
import time
from types import SimpleNamespace
from unittest.mock import Mock

import polars as pl
import pytest

from test_remediation_regressions import (
    AggTrade,
    ConfluenceEngine,
    FuturesWSManager,
    WSConfig,
    _ichimoku_lines,
    _weighted_moving_average,
    make_prepared,
    make_signal,
)


def test_ws_depth_imbalance_uses_signed_delta_ratio() -> None:
    ws = FuturesWSManager(rest_client=Mock(), config=WSConfig())
    now_ms = int(time.time() * 1000)
    ws._agg_trades["BTCUSDT"] = collections.deque(
        [
            AggTrade("BTCUSDT", 1, 100.0, 4.0, now_ms, is_buyer_maker=False),  # buy
            AggTrade("BTCUSDT", 2, 100.1, 1.0, now_ms, is_buyer_maker=True),   # sell
        ],
        maxlen=32,
    )

    imbalance = ws.get_depth_imbalance("BTCUSDT")
    assert imbalance is not None
    assert imbalance == pytest.approx(0.6, rel=1e-6)


def test_ws_microprice_bias_keeps_signed_delta_ratio() -> None:
    ws = FuturesWSManager(rest_client=Mock(), config=WSConfig())
    now_ms = int(time.time() * 1000)
    ws._book["BTCUSDT"] = (100.0, 100.1)
    ws._agg_trades["BTCUSDT"] = collections.deque(
        [
            AggTrade("BTCUSDT", 3, 100.0, 1.0, now_ms, is_buyer_maker=False),  # buy
            AggTrade("BTCUSDT", 4, 100.1, 4.0, now_ms, is_buyer_maker=True),   # sell
        ],
        maxlen=32,
    )

    bias = ws.get_microprice_bias("BTCUSDT")
    assert bias is not None
    assert bias == pytest.approx(-0.6, rel=1e-6)


def test_weighted_moving_average_uses_linear_weights() -> None:
    series = pl.Series("close", [1.0, 2.0, 3.0], dtype=pl.Float64)
    wma3 = _weighted_moving_average(series, 3, name="wma3")
    assert wma3[2] == pytest.approx((1.0 * 1 + 2.0 * 2 + 3.0 * 3) / 6.0, rel=1e-6)


def test_ichimoku_senkou_is_forward_projected() -> None:
    highs = [float(i + 10) for i in range(80)]
    lows = [float(i) for i in range(80)]
    frame = pl.DataFrame({"high": highs, "low": lows})
    _, _, senkou_a, senkou_b = _ichimoku_lines(frame)
    # Forward projection offsets values into the future, leaving a leading null head.
    assert senkou_a.head(26).null_count() == 26
    assert senkou_b.head(26).null_count() == 26
    assert senkou_a.tail(26).null_count() == 0


def test_confluence_includes_explicit_funding_component() -> None:
    scoring_cfg = SimpleNamespace(
        weight_mtf_alignment=0.25,
        weight_volume_quality=0.2,
        weight_structure_clarity=0.2,
        weight_risk_reward=0.15,
        weight_crowd_position=0.1,
        weight_oi_momentum=0.1,
        setup_prior_weight=0.5,
        funding_rate_extreme=0.001,
        funding_rate_moderate=0.0005,
    )
    settings = SimpleNamespace(scoring=scoring_cfg)
    engine = ConfluenceEngine(settings)
    components = engine._compute_components(make_signal(), make_prepared(), scoring_cfg)
    names = [component.name for component in components]
    assert "funding_score" in names
