from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from bot.config import BotSettings
from bot.core.self_learner import SelfLearner
from bot.learning import RegimeAwareParams, WalkForwardOptimizer
from bot.learning.outcome_store import OutcomeStore
import polars as pl
import pytest


def _sample_outcomes(n: int = 60) -> list[dict[str, float | str]]:
    rows: list[dict[str, float | str]] = []
    for i in range(n):
        is_win = (i % 3) != 0
        rows.append(
            {
                "outcome": "win" if is_win else "loss",
                "score": 0.72 if is_win else 0.55,
                "entry": 100.0,
                "stop": 98.0,
                "tp1_hit": 103.0,
            }
        )
    return rows


def test_regime_aware_params_scales_by_regime() -> None:
    calm = RegimeAwareParams(regime="ranging")
    vol = RegimeAwareParams(regime="volatile")
    calm_low, calm_high = calm.scale("base_score", 0.6)
    vol_low, vol_high = vol.scale("base_score", 0.6)
    assert vol_low < calm_low
    assert vol_high > calm_high


def test_walk_forward_optimizer_returns_positive_for_profitable_distribution() -> None:
    optimizer = WalkForwardOptimizer(min_fold_size=15)
    score = optimizer.evaluate(_sample_outcomes(), {"base_score": 0.5})
    assert score > 0.0


def test_walk_forward_optimizer_optimize_returns_best_params() -> None:
    optimizer = WalkForwardOptimizer(min_fold_size=15)
    frame = pl.DataFrame(_sample_outcomes(90))
    best = optimizer.optimize(frame)
    assert "base_score" in best
    assert "min_rr" in best
    assert "_score" in best


def test_walk_forward_optimizer_honors_custom_search_space_keys() -> None:
    optimizer = WalkForwardOptimizer(min_fold_size=15)
    best = optimizer.optimize(
        _sample_outcomes(90),
        search_space={"score_threshold": [0.45, 0.55, 0.65]},
    )
    assert "score_threshold" in best


def test_regime_aware_params_roundtrip(tmp_path) -> None:
    db_path = tmp_path / "learning.db"
    params = RegimeAwareParams(regime="bull", db_path=db_path)
    params.set_params("ema_bounce", "bull", {"base_score": 0.62, "min_rr": 1.5})
    loaded = params.get_params("ema_bounce", "bull")
    assert loaded == {"base_score": 0.62, "min_rr": 1.5}


def test_outcome_store_get_outcomes_returns_polars_frame(tmp_path) -> None:
    db_path = tmp_path / "outcomes.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE setup_outcomes (
            setup_id TEXT,
            outcome TEXT,
            score REAL,
            created_at TEXT
        )
        """
    )
    now = datetime.now(UTC)
    conn.execute(
        "INSERT INTO setup_outcomes (setup_id, outcome, score, created_at) VALUES (?, ?, ?, ?)",
        ("ema_bounce", "win", 0.78, (now - timedelta(hours=1)).isoformat()),
    )
    conn.commit()
    conn.close()

    store = OutcomeStore(db_path)
    frame = store.get_outcomes("ema_bounce", now - timedelta(days=1), now + timedelta(minutes=1))
    assert frame.height == 1
    assert set(frame.columns) >= {"setup_id", "outcome", "score", "created_at"}


@pytest.mark.asyncio
async def test_self_learner_uses_walk_forward_fallback_when_optuna_missing(monkeypatch, tmp_path) -> None:
    class _Setup:
        strategy_id = "ema_bounce"

        @staticmethod
        def get_optimizable_params() -> dict[str, float]:
            return {"base_score": 0.55, "min_rr": 1.2}

    class _Registry:
        @staticmethod
        def get_enabled():
            return [_Setup()]

    learner = SelfLearner(_Registry(), db_path=str(tmp_path / "learner.db"), n_trials=5)
    settings = BotSettings(tg_token="1" * 30, target_chat_id="123")
    settings.intelligence = SimpleNamespace(regime_detector="composite")

    monkeypatch.setattr("bot.core.self_learner.OPTUNA_AVAILABLE", False)
    monkeypatch.setattr("bot.core.self_learner.optuna", None)
    async def _fake_fetch(_setup_id: str):
        return _sample_outcomes(90)

    monkeypatch.setattr(
        learner,
        "_fetch_outcomes",
        _fake_fetch,
    )

    results = await learner.run_nightly_study(settings)
    assert len(results) == 1
    assert results[0].setup_id == "ema_bounce"
    assert "base_score" in results[0].params

    stored = RegimeAwareParams(regime="composite", db_path=str(tmp_path / "learner.db")).get_params(
        "ema_bounce",
        "composite",
    )
    assert stored
