from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .application.bot import SignalBot
    from .config import BotSettings

__all__ = ["SignalBot", "BotSettings", "load_settings"]


def __getattr__(name: str) -> Any:
    if name == "SignalBot":
        from .application.bot import SignalBot as _SignalBot

        return _SignalBot
    if name == "BotSettings":
        from .config import BotSettings as _BotSettings

        return _BotSettings
    if name == "load_settings":
        from .config import load_settings as _load_settings

        return _load_settings
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
