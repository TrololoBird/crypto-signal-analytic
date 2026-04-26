from __future__ import annotations

import pytest

from bot.config import IntelligenceConfig


@pytest.mark.parametrize("value", ["legacy", "hmm", "gmm_var", "composite"])
def test_regime_detector_accepts_supported_values(value: str) -> None:
    cfg = IntelligenceConfig(regime_detector=value)
    assert cfg.regime_detector == value


def test_regime_detector_rejects_invalid_value() -> None:
    with pytest.raises(ValueError):
        IntelligenceConfig(regime_detector="unsupported")
