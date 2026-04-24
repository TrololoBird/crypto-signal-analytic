"""Telegram sender using aiogram with retry logic."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

try:
    from aiogram import Bot
    from aiogram.exceptions import TelegramRetryAfter, TelegramAPIError
except ImportError:
    Bot = None  # type: ignore[assignment]
    TelegramRetryAfter = Exception
    TelegramAPIError = Exception

if TYPE_CHECKING:
    from aiogram import Bot as AiogramBot
else:
    AiogramBot = Any

from .queue import QueuedMessage, TelegramQueue
from ..core.diagnostics import BotMetrics

LOG = logging.getLogger("bot.telegram.sender")


class TelegramSender:
    """Async Telegram sender with aiogram.
    
    Features:
    - Retry with exponential backoff
    - Circuit breaker pattern
    - Integration with message queue
    - Metrics tracking
    """
    
    # Circuit breaker thresholds
    FAILURE_THRESHOLD = 5
    RECOVERY_TIMEOUT = 60  # seconds
    
    def __init__(
        self,
        bot_token: str,
        default_chat_id: str,
        metrics: BotMetrics | None = None,
    ):
        self._token = bot_token
        self._default_chat = default_chat_id
        self._metrics = metrics
        
        self._bot: AiogramBot | None = None
        self._queue: TelegramQueue | None = None
        
        # Circuit breaker state
        self._failures = 0
        self._circuit_open = False
        self._last_failure: float | None = None
    
    async def initialize(self) -> None:
        """Initialize bot and queue."""
        if Bot is None:
            raise RuntimeError("aiogram not installed. Install: pip install aiogram")
        
        self._bot = Bot(token=self._token)
        
        # Setup queue
        self._queue = TelegramQueue()
        await self._queue.start(self._send_with_retry)
        
        LOG.info("Telegram sender initialized")
    
    async def close(self) -> None:
        """Close bot and queue."""
        if self._queue:
            await self._queue.stop()
        
        if self._bot:
            await self._bot.session.close()
        
        LOG.info("Telegram sender closed")
    
    async def send_message(
        self,
        text: str,
        chat_id: str | None = None,
        priority: Any = None,  # MessagePriority
        reply_to: int | None = None,
        **kwargs: Any,
    ) -> bool:
        """Queue message for sending.
        
        Args:
            text: Message text (HTML format)
            chat_id: Target chat (defaults to default_chat_id)
            priority: MessagePriority level
            reply_to: Message ID to reply to
            
        Returns:
            True if queued successfully
        """
        if self._queue is None:
            LOG.error("Sender not initialized")
            return False
        
        if self._circuit_open:
            LOG.warning("Circuit breaker open, message dropped")
            if self._metrics:
                self._metrics.record_telegram_sent(error=True)
            return False
        
        target = chat_id or self._default_chat
        
        from .queue import MessagePriority
        prio = priority or MessagePriority.NORMAL
        
        msg = self._queue.enqueue(
            text=text,
            chat_id=target,
            priority=prio,
            reply_to=reply_to,
            **kwargs
        )
        
        return msg is not None
    
    async def send_signal_alert(
        self,
        symbol: str,
        direction: str,
        entry: float,
        sl: float,
        tp1: float,
        tp2: float,
        score: float,
        strategy: str,
    ) -> bool:
        """Send formatted signal alert."""
        emoji = "🟢" if direction == "long" else "🔴"
        
        text = f"""
<b>{emoji} {symbol} {direction.upper()}</b>

<b>Entry:</b> {entry:.8g}
<b>SL:</b> {sl:.8g}
<b>TP1:</b> {tp1:.8g}
<b>TP2:</b> {tp2:.8g}

<b>Score:</b> {score:.2f}
<b>Strategy:</b> {strategy}
""".strip()
        
        from .queue import MessagePriority
        return await self.send_message(
            text=text,
            priority=MessagePriority.CRITICAL,
        )
    
    async def _send_with_retry(self, msg: QueuedMessage) -> None:
        """Send message with retry logic."""
        if self._bot is None:
            raise RuntimeError("Bot not initialized")
        
        # Check circuit breaker
        if self._circuit_open:
            if self._last_failure:
                elapsed = asyncio.get_event_loop().time() - self._last_failure
                if elapsed < self.RECOVERY_TIMEOUT:
                    raise Exception("Circuit breaker open")
            # Try to close circuit
            self._circuit_open = False
            self._failures = 0
            LOG.info("Circuit breaker reset, attempting recovery")
        
        last_error: Exception | None = None
        
        for attempt in range(3):
            try:
                await self._bot.send_message(
                    chat_id=msg.chat_id,
                    text=msg.text,
                    parse_mode=msg.parse_mode,
                    disable_web_page_preview=msg.disable_web_page_preview,
                    reply_to_message_id=msg.reply_to,
                )
                
                # Success
                self._failures = 0
                if self._metrics:
                    self._metrics.record_telegram_sent(error=False)
                
                LOG.debug("Message sent to %s", msg.chat_id)
                return
                
            except TelegramRetryAfter as exc:
                # Rate limited by Telegram
                wait_time = max(1, int(getattr(exc, "retry_after", 1)))
                LOG.warning("Telegram rate limit, waiting %ds", wait_time)
                await asyncio.sleep(wait_time)
                last_error = exc
                
            except TelegramAPIError as exc:
                LOG.error("Telegram API error: %s", exc)
                last_error = exc
                self._record_failure()
                
                if attempt < 2:
                    wait = 2 ** attempt
                    LOG.info("Retrying in %ds (attempt %d/3)", wait, attempt + 1)
                    await asyncio.sleep(wait)
                
            except Exception as exc:
                LOG.exception("Send failed: %s", exc)
                last_error = exc
                self._record_failure()
                
                if attempt < 2:
                    wait = 2 ** attempt
                    await asyncio.sleep(wait)
        
        # All retries exhausted
        raise last_error or Exception("Send failed after retries")
    
    def _record_failure(self) -> None:
        """Record failure for circuit breaker."""
        self._failures += 1
        self._last_failure = asyncio.get_event_loop().time()
        
        if self._failures >= self.FAILURE_THRESHOLD:
            self._circuit_open = True
            LOG.error("Circuit breaker opened after %d failures", self._failures)
    
    def get_queue_stats(self) -> dict[str, Any]:
        """Get queue statistics."""
        if self._queue is None:
            return {"error": "not_initialized"}
        return self._queue.get_stats()
    
    def is_healthy(self) -> bool:
        """Check if sender is healthy."""
        return not self._circuit_open and self._queue is not None
