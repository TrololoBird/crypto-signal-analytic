"""Async task scheduler for background operations."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Callable, Coroutine
from enum import Enum

LOG = logging.getLogger("bot.tasks.scheduler")


class TaskPriority(Enum):
    """Task priority levels."""
    HIGH = 1
    NORMAL = 2
    LOW = 3


@dataclass
class ScheduledTask:
    """Scheduled task definition."""
    name: str
    coro: Callable[[], Coroutine[Any, Any, Any]]
    interval_seconds: float
    priority: TaskPriority = TaskPriority.NORMAL
    last_run: datetime | None = None
    next_run: datetime | None = None
    error_count: int = 0
    max_errors: int = 5
    enabled: bool = True


class TaskScheduler:
    """Async task scheduler with priority support.
    
    Features:
    - Periodic task execution
    - Priority-based scheduling
    - Error tracking and backoff
    - Graceful shutdown
    """
    
    def __init__(self):
        self._tasks: dict[str, ScheduledTask] = {}
        self._running = False
        self._task_handles: dict[str, asyncio.Task] = {}
        self._shutdown_event = asyncio.Event()
    
    def register(
        self,
        name: str,
        coro: Callable[[], Coroutine[Any, Any, Any]],
        interval_seconds: float,
        priority: TaskPriority = TaskPriority.NORMAL,
        max_errors: int = 5,
    ) -> None:
        """Register a periodic task."""
        now = datetime.utcnow()
        
        self._tasks[name] = ScheduledTask(
            name=name,
            coro=coro,
            interval_seconds=interval_seconds,
            priority=priority,
            last_run=None,
            next_run=now,
            error_count=0,
            max_errors=max_errors,
            enabled=True,
        )
        
        LOG.info(
            "Registered task: %s (interval=%.1fs, priority=%s)",
            name, interval_seconds, priority.name
        )
    
    def unregister(self, name: str) -> None:
        """Unregister a task."""
        if name in self._tasks:
            del self._tasks[name]
            LOG.info("Unregistered task: %s", name)
    
    def enable(self, name: str) -> bool:
        """Enable a task."""
        if name not in self._tasks:
            return False
        self._tasks[name].enabled = True
        LOG.info("Enabled task: %s", name)
        return True
    
    def disable(self, name: str) -> bool:
        """Disable a task."""
        if name not in self._tasks:
            return False
        self._tasks[name].enabled = False
        LOG.info("Disabled task: %s", name)
        return True
    
    def run_once(self, name: str) -> asyncio.Task | None:
        """Manually trigger a task to run once."""
        if name not in self._tasks:
            return None
        
        task = self._tasks[name]
        return asyncio.create_task(
            self._execute_task(task),
            name=f"manual_{name}"
        )
    
    async def start(self) -> None:
        """Start the scheduler."""
        if self._running:
            LOG.warning("Scheduler already running")
            return
        
        self._running = True
        self._shutdown_event.clear()
        
        LOG.info("Starting task scheduler with %d tasks", len(self._tasks))
        
        # Start all enabled tasks
        for task in self._tasks.values():
            if task.enabled:
                self._schedule_task(task)
        
        # Wait for shutdown
        await self._shutdown_event.wait()
    
    async def stop(self) -> None:
        """Stop the scheduler gracefully."""
        if not self._running:
            return
        
        LOG.info("Stopping task scheduler...")
        self._running = False
        self._shutdown_event.set()
        
        # Cancel all running tasks
        for handle in list(self._task_handles.values()):
            handle.cancel()
        
        # Wait for cancellation
        if self._task_handles:
            await asyncio.gather(
                *self._task_handles.values(),
                return_exceptions=True
            )
        
        self._task_handles.clear()
        LOG.info("Task scheduler stopped")
    
    def _schedule_task(self, task: ScheduledTask) -> None:
        """Schedule next execution of a task."""
        if not self._running or not task.enabled:
            return
        
        # Calculate delay based on priority
        base_delay = task.interval_seconds
        if task.priority == TaskPriority.HIGH:
            delay = base_delay * 0.9  # Slight boost
        elif task.priority == TaskPriority.LOW:
            delay = base_delay * 1.1  # Slight delay
        else:
            delay = base_delay
        
        # Error backoff
        if task.error_count > 0:
            backoff = min(2 ** task.error_count, 300)  # Max 5 min backoff
            delay += backoff
            LOG.warning(
                "Task %s error backoff: +%ds (errors=%d)",
                task.name, backoff, task.error_count
            )
        
        # Create task
        handle = asyncio.create_task(
            self._run_with_delay(task, delay),
            name=f"scheduler_{task.name}"
        )
        
        self._task_handles[task.name] = handle
    
    async def _run_with_delay(self, task: ScheduledTask, delay: float) -> None:
        """Wait then execute task."""
        try:
            await asyncio.sleep(delay)
            
            if not self._running:
                return
            
            await self._execute_task(task)
            
            # Reschedule
            if self._running:
                self._schedule_task(task)
                
        except asyncio.CancelledError:
            LOG.debug("Task %s cancelled", task.name)
        except Exception as exc:
            LOG.error("Task %s scheduler error: %s", task.name, exc)
            task.error_count += 1
            
            if task.error_count < task.max_errors:
                # Retry
                self._schedule_task(task)
    
    async def _execute_task(self, task: ScheduledTask) -> None:
        """Execute a single task with error handling."""
        task.last_run = datetime.utcnow()
        
        try:
            LOG.debug("Executing task: %s", task.name)
            await task.coro()
            
            # Reset error count on success
            if task.error_count > 0:
                LOG.info("Task %s recovered after %d errors", task.name, task.error_count)
                task.error_count = 0
                
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            task.error_count += 1
            LOG.exception("Task %s failed (error %d/%d): %s", 
                         task.name, task.error_count, task.max_errors, exc)
            
            if task.error_count >= task.max_errors:
                LOG.error("Task %s disabled after %d errors", task.name, task.error_count)
                task.enabled = False
    
    def get_status(self) -> dict[str, Any]:
        """Get scheduler status."""
        return {
            "running": self._running,
            "tasks": {
                name: {
                    "enabled": task.enabled,
                    "interval_seconds": task.interval_seconds,
                    "priority": task.priority.name,
                    "last_run": task.last_run.isoformat() if task.last_run else None,
                    "error_count": task.error_count,
                    "max_errors": task.max_errors,
                }
                for name, task in self._tasks.items()
            },
            "active_handles": len(self._task_handles),
        }
