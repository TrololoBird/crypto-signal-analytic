"""Backward-compatible shim for legacy MLFilter import path.

Prefer importing from `bot.ml` or `bot.ml.filter`.
"""

from __future__ import annotations

from .ml.filter import MLFilter, MLInferenceResult

__all__ = ["MLFilter", "MLInferenceResult"]
