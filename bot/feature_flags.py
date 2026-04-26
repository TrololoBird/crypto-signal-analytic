"""Runtime feature flag helpers.

Current implementation is config-backed and async-friendly, so call-sites can
switch to DB/remote flags later without interface changes.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .config import BotSettings


@dataclass(slots=True)
class FeatureFlags:
    settings: BotSettings

    async def is_strategy_enabled(self, strategy_id: str) -> bool:
        if not strategy_id:
            return False
        return bool(getattr(self.settings.setups, strategy_id, False))

    async def get_strategy_params(self, strategy_id: str) -> dict[str, float]:
        setups = getattr(getattr(self.settings, "filters", None), "setups", {})
        if not isinstance(setups, dict):
            return {}
        params = setups.get(strategy_id, {})
        if not isinstance(params, dict):
            return {}
        out: dict[str, float] = {}
        for key, value in params.items():
            if isinstance(value, bool):
                out[str(key)] = float(value)
            elif isinstance(value, (int, float)):
                out[str(key)] = float(value)
        return out

    async def snapshot(self) -> dict[str, Any]:
        setup_ids = self.settings.setups.enabled_setup_ids()
        return {
            "enabled_strategy_count": len(setup_ids),
            "enabled_strategy_ids": list(setup_ids),
        }
