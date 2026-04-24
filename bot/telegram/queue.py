"""Async message queue for Telegram with rate limiting."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable

LOG = logging.getLogger("bot.telegram.queue")


class MessagePriority(Enum):
    """Message priority levels."""
    CRITICAL = 1  # Signal notifications
    HIGH = 2      # Alerts
    NORMAL = 3    # Reports
    LOW = 4       # Diagnostics


class MessageStatus(Enum):
    """Message processing status."""
    PENDING = "pending"
    SENDING = "sending"
    SENT = "sent"
    FAILED = "failed"
    DROPPED = "dropped"


@dataclass
class QueuedMessage:
    """Message in queue."""
    id: str
    text: str
    priority: MessagePriority
    chat_id: str
    reply_to: int | None = None
    parse_mode: str = "HTML"
    disable_web_page_preview: bool = True
    
    # Metadata
    created_at: datetime = field(default_factory=datetime.utcnow)
    status: MessageStatus = MessageStatus.PENDING
    retry_count: int = 0
    max_retries: int = 3
    error: str | None = None
    
    # Callbacks
    on_sent: Callable[[], None] | None = None
    on_failed: Callable[[str], None] | None = None


class TelegramQueue:
    """Async message queue for Telegram with rate limiting.
    
    Features:
    - Priority-based queueing
    - Per-chat rate limiting (max 20 messages/min per chat)
    - Automatic retry with backoff
    - Deduplication (based on message content hash)
    - Queue size limiting (drop oldest low-priority)
    """
    
    # Telegram limits
    MESSAGES_PER_MINUTE = 20  # Per chat limit
    MESSAGES_PER_SECOND = 1   # Global burst limit
    
    def __init__(
        self,
        max_size: int = 1000,
        dedup_window_seconds: float = 60.0,
    ):
        self._max_size = max_size
        self._dedup_window = dedup_window_seconds
        
        # Priority queues
        self._queues: dict[MessagePriority, asyncio.Queue[QueuedMessage]] = {
            MessagePriority.CRITICAL: asyncio.Queue(),
            MessagePriority.HIGH: asyncio.Queue(),
            MessagePriority.NORMAL: asyncio.Queue(),
            MessagePriority.LOW: asyncio.Queue(),
        }
        
        # Rate limiting state
        self._chat_timestamps: dict[str, list[datetime]] = {}  # chat -> timestamps
        self._last_sent_global: datetime | None = None
        
        # Deduplication
        self._recent_hashes: dict[str, datetime] = {}
        
        # Statistics
        self._stats = {
            "queued": 0,
            "sent": 0,
            "failed": 0,
            "dropped": 0,
            "deduped": 0,
        }
        
        self._running = False
        self._worker_task: asyncio.Task | None = None
    
    def enqueue(
        self,
        text: str,
        chat_id: str,
        priority: MessagePriority = MessagePriority.NORMAL,
        reply_to: int | None = None,
        **kwargs: Any,
    ) -> QueuedMessage | None:
        """Add message to queue.
        
        Returns:
            QueuedMessage or None if deduplicated or queue full
        """
        import hashlib
        
        # Check deduplication
        content_hash = hashlib.md5(text.encode()).hexdigest()[:16]
        now = datetime.utcnow()
        
        if content_hash in self._recent_hashes:
            last_seen = self._recent_hashes[content_hash]
            if (now - last_seen).total_seconds() < self._dedup_window:
                LOG.debug("Message deduplicated: %s...", text[:50])
                self._stats["deduped"] += 1
                return None
        
        self._recent_hashes[content_hash] = now
        
        # Clean old dedup entries
        self._cleanup_dedup(now)
        
        # Check queue size
        total_size = sum(q.qsize() for q in self._queues.values())
        if total_size >= self._max_size:
            # Try to drop oldest low-priority
            if not self._drop_oldest_low_priority():
                LOG.warning("Queue full, message dropped: %s...", text[:50])
                self._stats["dropped"] += 1
                return None
        
        # Create message
        msg = QueuedMessage(
            id=content_hash,
            text=text,
            priority=priority,
            chat_id=chat_id,
            reply_to=reply_to,
            **kwargs
        )
        
        # Add to appropriate queue
        self._queues[priority].put_nowait(msg)
        self._stats["queued"] += 1
        
        LOG.debug("Enqueued message [prio=%s]: %s...", priority.name, text[:50])
        return msg
    
    async def start(self, sender: Callable[[QueuedMessage], Any]) -> None:
        """Start queue processor."""
        if self._running:
            return
        
        self._running = True
        self._worker_task = asyncio.create_task(
            self._worker_loop(sender),
            name="telegram_queue_worker"
        )
        
        LOG.info("Telegram queue started")
    
    async def stop(self) -> None:
        """Stop queue processor."""
        if not self._running:
            return
        
        self._running = False
        
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
        
        LOG.info("Telegram queue stopped")
    
    async def _worker_loop(self, sender: Callable[[QueuedMessage], Any]) -> None:
        """Main worker loop."""
        while self._running:
            try:
                msg = await self._get_next_message()
                if msg is None:
                    await asyncio.sleep(0.1)
                    continue
                
                # Wait for rate limit
                await self._wait_rate_limit(msg.chat_id)
                
                # Send
                msg.status = MessageStatus.SENDING
                try:
                    await sender(msg)
                    msg.status = MessageStatus.SENT
                    self._stats["sent"] += 1
                    
                    if msg.on_sent:
                        msg.on_sent()
                    
                except Exception as exc:
                    msg.retry_count += 1
                    
                    if msg.retry_count <= msg.max_retries:
                        # Requeue with delay
                        LOG.warning("Send failed, retrying %d/%d: %s",
                                   msg.retry_count, msg.max_retries, exc)
                        await asyncio.sleep(2 ** msg.retry_count)
                        self._queues[msg.priority].put_nowait(msg)
                    else:
                        msg.status = MessageStatus.FAILED
                        msg.error = str(exc)
                        self._stats["failed"] += 1
                        
                        if msg.on_failed:
                            msg.on_failed(str(exc))
                
            except asyncio.CancelledError:
                break
            except Exception as exc:
                LOG.exception("Queue worker error: %s", exc)
                await asyncio.sleep(1)
    
    async def _get_next_message(self) -> QueuedMessage | None:
        """Get next message by priority."""
        # Try priorities in order
        for priority in [
            MessagePriority.CRITICAL,
            MessagePriority.HIGH,
            MessagePriority.NORMAL,
            MessagePriority.LOW,
        ]:
            queue = self._queues[priority]
            if not queue.empty():
                try:
                    return queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
        
        return None
    
    async def _wait_rate_limit(self, chat_id: str) -> None:
        """Wait if rate limited."""
        now = datetime.utcnow()
        
        # Check per-chat limit (20/min)
        if chat_id in self._chat_timestamps:
            timestamps = self._chat_timestamps[chat_id]
            # Remove old timestamps (> 60s)
            cutoff = now - timedelta(seconds=60)
            timestamps[:] = [t for t in timestamps if t > cutoff]
            
            if len(timestamps) >= self.MESSAGES_PER_MINUTE:
                # Wait until oldest expires
                wait_time = (timestamps[0] + timedelta(seconds=60) - now).total_seconds()
                if wait_time > 0:
                    LOG.debug("Rate limit hit for chat %s, waiting %.1fs", chat_id, wait_time)
                    await asyncio.sleep(wait_time)
        
        # Check global burst limit (1/sec)
        if self._last_sent_global:
            elapsed = (now - self._last_sent_global).total_seconds()
            if elapsed < 1.0:
                await asyncio.sleep(1.0 - elapsed)
        
        # Update timestamps
        if chat_id not in self._chat_timestamps:
            self._chat_timestamps[chat_id] = []
        self._chat_timestamps[chat_id].append(now)
        self._last_sent_global = now
    
    def _cleanup_dedup(self, now: datetime) -> None:
        """Clean old deduplication entries."""
        cutoff = now - timedelta(seconds=self._dedup_window)
        expired = [h for h, t in self._recent_hashes.items() if t < cutoff]
        for h in expired:
            del self._recent_hashes[h]
    
    def _drop_oldest_low_priority(self) -> bool:
        """Drop oldest low priority message if possible."""
        for priority in reversed(list(MessagePriority)):
            queue = self._queues[priority]
            if not queue.empty():
                try:
                    dropped = queue.get_nowait()
                    dropped.status = MessageStatus.DROPPED
                    self._stats["dropped"] += 1
                    LOG.debug("Dropped low priority message: %s", dropped.id)
                    return True
                except asyncio.QueueEmpty:
                    pass
        return False
    
    def get_stats(self) -> dict[str, Any]:
        """Get queue statistics."""
        return {
            **self._stats,
            "queue_sizes": {
                p.name: q.qsize() for p, q in self._queues.items()
            },
            "total_queued": sum(q.qsize() for q in self._queues.values()),
        }
    
    def clear(self) -> int:
        """Clear all queues. Returns number cleared."""
        cleared = 0
        for queue in self._queues.values():
            while not queue.empty():
                try:
                    queue.get_nowait()
                    cleared += 1
                except asyncio.QueueEmpty:
                    break
        return cleared
