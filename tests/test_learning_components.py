from __future__ import annotations

from bot.learning import RegimeAwareParams, WalkForwardOptimizer


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
