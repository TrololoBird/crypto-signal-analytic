"""In-memory asyncio.Queue EventBus with typed dispatch.

Design:
  - Single unbounded asyncio.Queue as the event queue.
  - Subscribers register handlers per event type.
  - Each handler is dispatched as an independent asyncio.Task so one slow
    handler does not block others or the dispatch loop.
  - Exceptions in handlers are logged and suppressed (never crash the bus).
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Callable, Coroutine

if TYPE_CHECKING:
    from .events import AnyEvent

LOG = logging.getLogger("bot.core.event_bus")

AsyncHandler = Callable[["AnyEvent"], Coroutine[Any, Any, None]]


class EventBus:
    """Asyncio-native in-memory event bus.

    Usage::

        bus = EventBus()
        bus.subscribe(KlineCloseEvent, my_handler)
        asyncio.create_task(bus.run())   # start dispatch loop
        await bus.publish(KlineCloseEvent(...))
    """

    def __init__(self) -> None:
        self._queue: asyncio.Queue[AnyEvent] = asyncio.Queue()
        self._subscribers: dict[type, list[AsyncHandler]] = defaultdict(list)
        self._running = False

    def subscribe(self, event_type: type, handler: AsyncHandler) -> None:
        """Register *handler* to be called when an event of *event_type* is published."""
        self._subscribers[event_type].append(handler)

    async def publish(self, event: AnyEvent) -> None:
        """Enqueue *event* for dispatch.  Never blocks (unbounded queue)."""
        await self._queue.put(event)

    def publish_nowait(self, event: AnyEvent) -> None:
        """Non-async enqueue.  Safe to call from sync contexts."""
        self._queue.put_nowait(event)

    async def run(self) -> None:
        """Dispatch loop — runs until cancelled."""
        self._running = True
        LOG.info("event bus dispatch loop started")
        try:
            while True:
                event = await self._queue.get()
                event_type = type(event).__name__
                handlers = self._subscribers.get(type(event), [])
                LOG.debug("event bus dispatching | type=%s handlers=%d", event_type, len(handlers))
                if not handlers:
                    LOG.debug("event bus no handlers for | type=%s", event_type)
                for handler in handlers:
                    task = asyncio.create_task(
                        self._safe_call(handler, event),
                        name=f"bus:{event_type}",
                    )
                    # Attach done-callback for unhandled exception logging
                    task.add_done_callback(self._task_done)
        except asyncio.CancelledError:
            LOG.debug("event bus dispatch loop stopped")
            self._running = False

    @staticmethod
    async def _safe_call(handler: AsyncHandler, event: AnyEvent) -> None:
        try:
            await handler(event)
        except Exception as exc:
            LOG.exception(
                "event handler %s raised on %s: %s",
                getattr(handler, "__qualname__", repr(handler)),
                type(event).__name__,
                exc,
            )

    @staticmethod
    def _task_done(task: asyncio.Task) -> None:
        with contextlib.suppress(asyncio.CancelledError):
            exc = task.exception()
            if exc is not None:
                LOG.error("unhandled exception in bus task %s: %s", task.get_name(), exc)
