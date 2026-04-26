from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RegimeAwareParams:
    """Adjust parameter search bounds according to broad market regime."""

    regime: str

    def scale(self, param_name: str, default_value: float) -> tuple[float, float]:
        regime = self.regime.lower()
        if "volatile" in regime:
            width = 0.7
        elif "ranging" in regime:
            width = 0.4
        else:
            width = 0.5

        if "min_rr" in param_name:
            return (max(0.8, default_value * (1.0 - width)), min(4.0, default_value * (1.0 + width)))
        if any(token in param_name for token in ("score", "threshold", "penalty")):
            return (max(0.0, default_value * (1.0 - width)), min(1.0, default_value * (1.0 + width)))
        return (default_value * (1.0 - width), default_value * (1.0 + width))
