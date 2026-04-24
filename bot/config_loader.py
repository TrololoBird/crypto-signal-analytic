"""Legacy configuration loader for offline strategy tooling.

Runtime strategy detection now reads setup overrides from
`settings.filters.setups` populated at startup.
"""
from __future__ import annotations

import logging
import tomllib
from pathlib import Path
from typing import Any

LOG = logging.getLogger(__name__)


def load_strategy_config(strategy_name: str) -> dict[str, Any]:
    """Load TOML config for a specific strategy.
    
    Args:
        strategy_name: Name of the strategy (e.g., 'wick_trap_reversal')
        
    Returns:
        Dict with config sections (detection, risk_management, scoring)
    """
    config_dir = Path("config/strategies")
    config_path = config_dir / f"{strategy_name}.toml"
    
    if not config_path.exists():
        LOG.debug("No config file found for %s, using defaults", strategy_name)
        return {}
    
    try:
        with open(config_path, "rb") as f:
            config = tomllib.load(f)
        LOG.debug("Loaded config for %s: %s sections", strategy_name, list(config.keys()))
        return config
    except Exception as exc:
        LOG.warning("Failed to load config for %s: %s, using defaults", strategy_name, exc)
        return {}


def get_nested(config: dict, key: str, default: Any = None) -> Any:
    """Get value from config, supporting nested keys like 'detection.wick_mult'.
    
    Args:
        config: Config dict
        key: Key like 'wick_mult' or 'detection.wick_mult'
        default: Default value if key not found
    """
    if "." in key:
        parts = key.split(".")
        value = config
        for part in parts:
            if isinstance(value, dict) and part in value:
                value = value[part]
            else:
                return default
        return value
    
    # Flat lookup across all sections
    for section in config.values():
        if isinstance(section, dict) and key in section:
            return section[key]
    
    return default


def load_global_strategy_config() -> dict[str, Any]:
    """Load global strategy defaults from config_strategies.toml."""
    config_path = Path("config_strategies.toml")
    
    if not config_path.exists():
        return {}
    
    try:
        with open(config_path, "rb") as f:
            return tomllib.load(f)
    except Exception as exc:
        LOG.warning("Failed to load global strategy config: %s", exc)
        return {}
