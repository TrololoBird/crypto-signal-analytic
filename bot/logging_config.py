"""Structured logging helpers for runtime correlation IDs and context.

This module augments stdlib logging with structlog contextvars.
"""

from __future__ import annotations

import logging

import structlog


def configure_structlog(level: int = logging.INFO) -> None:
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


def bind_log_context(**kwargs: object) -> None:
    structlog.contextvars.bind_contextvars(**kwargs)


def clear_log_context(*keys: str) -> None:
    if keys:
        structlog.contextvars.unbind_contextvars(*keys)
    else:
        structlog.contextvars.clear_contextvars()
