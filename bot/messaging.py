from __future__ import annotations

import asyncio
import html
import hashlib
import logging
import re
from dataclasses import dataclass
from collections import deque
from datetime import datetime, timedelta, timezone
from functools import wraps
from typing import Any, Protocol

import structlog

# aiogram for Telegram Bot API
try:
    from aiogram import Bot
    from aiogram.client.session.aiohttp import AiohttpSession
    try:
        from aiogram.exceptions import TelegramAPIError as AiogramAPIError
    except ImportError:
        from aiogram import exceptions as aiogram_exceptions
        AiogramAPIError = getattr(aiogram_exceptions, 'TelegramAPIError', Exception)
    _HAS_AIogram = True
except ImportError:
    _HAS_AIogram = False

# tenacity for retries
try:
    from tenacity import (
        retry,
        stop_after_attempt,
        wait_exponential,
        retry_if_exception_type,
        before_sleep_log,
    )
    HAS_TENACITY = True
except ImportError:
    HAS_TENACITY = False


LOG = structlog.get_logger("bot.messaging")
UTC = timezone.utc
NETWORK_RETRIES = 3
RETRY_DELAY_SECONDS = 1.5
TELEGRAM_DUPLICATE_WINDOW_SECONDS = 180
TELEGRAM_TEXT_LIMIT = 4000
TELEGRAM_CAPTION_LIMIT = 1024
TELEGRAM_LOG_PREVIEW_LIMIT = 500


# Fallback retry decorator for when tenacity is not installed
def _simple_retry(max_attempts: int = 3, exceptions: tuple[type[Exception], ...] = (Exception,)) -> Any:
    """Simple retry decorator as fallback when tenacity is not available."""
    def decorator(func: Any) -> Any:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_exc: Exception | None = None
            for attempt in range(max_attempts):
                try:
                    return await func(*args, **kwargs)
                except exceptions as exc:
                    last_exc = exc
                    if attempt < max_attempts - 1:
                        wait_time = RETRY_DELAY_SECONDS * (2 ** attempt)  # Exponential backoff
                        LOG.debug("retry %s/%s after %.1fs: %s", attempt + 1, max_attempts, wait_time, exc)
                        await asyncio.sleep(wait_time)
            raise last_exc or RuntimeError("Retry failed")
        return wrapper
    return decorator


class MessageBroadcaster(Protocol):
    async def preflight_check(self) -> None: ...
    async def send_html(self, text: str, *, reply_to_message_id: int | None = None) -> DeliveryResult: ...
    async def edit_html(self, message_id: int, text: str) -> None: ...
    async def send_photo(self, photo_bytes: bytes, caption: str, *, reply_to_message_id: int | None = None) -> None: ...
    async def close(self) -> None: ...


@dataclass(frozen=True, slots=True)
class DeliveryResult:
    status: str
    message_id: int | None = None
    reason: str | None = None


class TelegramBroadcaster:
    duplicate_window_seconds = TELEGRAM_DUPLICATE_WINDOW_SECONDS

    def __init__(self, token: str, target_chat_id: str) -> None:
        if not _HAS_AIogram:
            raise RuntimeError("aiogram not installed. Run: pip install aiogram>=3.27.0")
        
        self.token = token
        self.target_chat_id = target_chat_id
        session = AiohttpSession()
        self.bot = Bot(token=token, session=session)
        self._send_lock = asyncio.Lock()
        self._failure_count = 0
        self._circuit_state = "closed"
        self._circuit_reset_time: datetime | None = None
        self._recent_message_hashes: dict[str, datetime] = {}
        self._send_buffer: deque[str] = deque(maxlen=50)
        self._rate_limit_until: datetime | None = None

    async def preflight_check(self) -> None:
        """Verify bot token and chat access."""
        try:
            bot_info = await self.bot.get_me()
            LOG.info("telegram bot info", username=bot_info.username, id=bot_info.id)
            
            chat = await self.bot.get_chat(self.target_chat_id)
            LOG.info("telegram chat access confirmed", chat_id=chat.id, type=chat.type)
        except Exception as exc:
            raise RuntimeError(f"telegram preflight failed: {exc}") from exc

    async def send_html(self, text: str, *, reply_to_message_id: int | None = None) -> DeliveryResult:
        async with self._send_lock:
            await self._respect_rate_limit()
            if self._circuit_state == "open":
                if self._circuit_reset_time is not None and datetime.now(UTC) < self._circuit_reset_time:
                    self._send_buffer.append(text)
                    LOG.debug("telegram circuit breaker open; buffering message (%s buffered)", len(self._send_buffer))
                    return DeliveryResult(status="buffered_circuit_open", reason="circuit_open")
                self._circuit_state = "half_open"
            self._prune_recent_hashes()
            message_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
            if message_hash in self._recent_message_hashes:
                LOG.debug("suppressing duplicate telegram message within dedupe window")
                return DeliveryResult(status="suppressed_duplicate", reason="duplicate_within_window")
            try:
                sent_message_id = await self._send_immediate(
                    text,
                    message_hash=message_hash,
                    reply_to_message_id=reply_to_message_id,
                )
            except Exception as exc:
                return DeliveryResult(status="failed", reason=f"{exc.__class__.__name__}: {exc}")
            while self._send_buffer:
                buffered = self._send_buffer.popleft()
                buffered_hash = hashlib.sha256(buffered.encode("utf-8")).hexdigest()
                if buffered_hash in self._recent_message_hashes:
                    continue
                try:
                    await self._send_immediate(buffered, message_hash=buffered_hash, reply_to_message_id=None)
                except Exception:
                    self._send_buffer.appendleft(buffered)
                    break
            return DeliveryResult(status="sent", message_id=sent_message_id)

    async def edit_html(self, message_id: int, text: str) -> None:
        async with self._send_lock:
            await self._respect_rate_limit()
            if self._circuit_state == "open":
                if self._circuit_reset_time is not None and datetime.now(UTC) < self._circuit_reset_time:
                    LOG.debug("telegram circuit breaker open; skipping edit for message_id=%s", message_id)
                    return
                self._circuit_state = "half_open"
            try:
                await self._edit_immediate(message_id, text)
            except Exception as exc:
                self._record_send_failure(exc)
                raise
            self._failure_count = 0
            self._circuit_state = "closed"
            self._circuit_reset_time = None

    async def send_photo(self, photo_bytes: bytes, caption: str, *, reply_to_message_id: int | None = None) -> None:
        """Send photo using aiogram Bot with BufferedInputFile."""
        async with self._send_lock:
            await self._respect_rate_limit()
            html_caption, plain_caption = self._prepare_captions(caption)
            
            try:
                try:
                    from aiogram.types import BufferedInputFile
                except ImportError:
                    BufferedInputFile = None
                if BufferedInputFile is None:
                    raise RuntimeError("BufferedInputFile is unavailable")
                photo_file = BufferedInputFile(photo_bytes, filename="chart.png")
                await self.bot.send_photo(
                    chat_id=self.target_chat_id,
                    photo=photo_file,
                    caption=html_caption,
                    parse_mode="HTML",
                    reply_to_message_id=reply_to_message_id,
                )
            except Exception as exc:
                error_str = str(exc).lower()
                # Try plain text fallback if HTML parsing failed
                if "parse" in error_str or "html" in error_str or "caption" in error_str:
                    fallback_caption = self._plain_text_fallback(caption, exc) or plain_caption
                    try:
                        try:
                            from aiogram.types import BufferedInputFile
                        except ImportError:
                            BufferedInputFile = None
                        if BufferedInputFile is None:
                            raise RuntimeError("BufferedInputFile is unavailable")
                        photo_file = BufferedInputFile(photo_bytes, filename="chart.png")
                        await self.bot.send_photo(
                            chat_id=self.target_chat_id,
                            photo=photo_file,
                            caption=fallback_caption,
                            reply_to_message_id=reply_to_message_id,
                        )
                    except Exception as fallback_exc:
                        LOG.error("telegram photo send failed (fallback): %s", fallback_exc)
                        raise
                else:
                    LOG.error("telegram photo send failed: %s", exc)
                    raise

    def _prune_recent_hashes(self) -> None:
        now = datetime.now(UTC)
        self._recent_message_hashes = {
            key: sent_at
            for key, sent_at in self._recent_message_hashes.items()
            if (now - sent_at).total_seconds() < type(self).duplicate_window_seconds
        }

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((Exception,)),  # aiogram raises various exceptions
        before_sleep=before_sleep_log(LOG, logging.WARNING),
        reraise=True,
    ) if HAS_TENACITY else _simple_retry(3, (Exception,))
    async def _send_immediate(
        self,
        text: str,
        *,
        message_hash: str,
        reply_to_message_id: int | None,
    ) -> int | None:
        """Send message using aiogram Bot."""
        try:
            result = await self.bot.send_message(
                chat_id=self.target_chat_id,
                text=text,
                parse_mode="HTML",
                reply_to_message_id=reply_to_message_id,
                disable_web_page_preview=True,
            )
            self._record_send_success(message_hash)
            LOG.info("telegram message sent", chars=len(text), preview=_message_preview(text))
            return result.message_id
        except Exception as exc:
            # Try plain text fallback if HTML parsing failed
            error_str = str(exc).lower()
            if "parse" in error_str or "html" in error_str or "tag" in error_str:
                plain_text = self._plain_text_fallback(text, exc)
                if plain_text:
                    try:
                        result = await self.bot.send_message(
                            chat_id=self.target_chat_id,
                            text=plain_text,
                            reply_to_message_id=reply_to_message_id,
                            disable_web_page_preview=True,
                        )
                        self._record_send_success(message_hash)
                        LOG.info("telegram message sent (plain text)", chars=len(plain_text))
                        return result.message_id
                    except Exception as fallback_exc:
                        self._record_send_failure(fallback_exc)
                        raise
            self._record_send_failure(exc)
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((Exception,)),
        before_sleep=before_sleep_log(LOG, logging.WARNING),
        reraise=True,
    ) if HAS_TENACITY else _simple_retry(3, (Exception,))
    async def _edit_immediate(self, message_id: int, text: str) -> None:
        """Edit message using aiogram Bot."""
        try:
            await self.bot.edit_message_text(
                chat_id=self.target_chat_id,
                message_id=message_id,
                text=text,
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
        except Exception as exc:
            error_str = str(exc).lower()
            # Message not modified is OK
            if "not modified" in error_str or "message is not modified" in error_str:
                return
            # Try plain text fallback
            if "parse" in error_str or "html" in error_str:
                plain_text = self._plain_text_fallback(text, exc)
                if plain_text:
                    try:
                        await self.bot.edit_message_text(
                            chat_id=self.target_chat_id,
                            message_id=message_id,
                            text=plain_text,
                            disable_web_page_preview=True,
                        )
                        return
                    except Exception as fallback_exc:
                        if "not modified" in str(fallback_exc).lower():
                            return
                        raise
            raise

    def _build_payload(
        self,
        text: str,
        *,
        parse_mode: str | None,
        reply_to_message_id: int | None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "chat_id": self.target_chat_id,
            "text": text,
            "disable_web_page_preview": True,
        }
        if parse_mode:
            payload["parse_mode"] = parse_mode
        if reply_to_message_id is not None:
            payload["reply_parameters"] = {
                "message_id": reply_to_message_id,
                "allow_sending_without_reply": True,
            }
        return payload

    def _record_send_success(self, message_hash: str) -> None:
        self._failure_count = 0
        self._circuit_state = "closed"
        self._circuit_reset_time = None
        self._rate_limit_until = None
        self._recent_message_hashes[message_hash] = datetime.now(UTC)

    def _record_send_failure(self, exc: Exception) -> None:
        # Extract retry_after from aiogram exception or use fallback
        retry_after = None
        if hasattr(exc, 'retry_after'):
            retry_after = getattr(exc, 'retry_after', None)
        elif hasattr(exc, 'retry_after_seconds'):
            retry_after = getattr(exc, 'retry_after_seconds', None)
        
        if retry_after:
            self._rate_limit_until = datetime.now(UTC) + timedelta(seconds=retry_after)
            LOG.warning("telegram rate limited; pausing sends", seconds=retry_after)
        
        self._failure_count += 1
        LOG.error("telegram send failed", attempt=f"{self._failure_count}/5", error=str(exc))
        
        if self._circuit_state == "half_open" or self._failure_count >= 5:
            self._circuit_state = "open"
            self._circuit_reset_time = datetime.now(UTC) + timedelta(minutes=5)
            LOG.critical("telegram circuit breaker opened for 5 minutes")

    async def _respect_rate_limit(self) -> None:
        if self._rate_limit_until is None:
            return
        remaining = (self._rate_limit_until - datetime.now(UTC)).total_seconds()
        if remaining <= 0:
            self._rate_limit_until = None
            return
        LOG.info("telegram send throttled by retry_after | sleep=%.1fs", remaining)
        await asyncio.sleep(remaining)
        self._rate_limit_until = None

    @staticmethod
    def _plain_text_fallback(text: str, exc: Exception | None = None) -> str | None:
        """Convert HTML to plain text when Telegram rejects HTML parsing."""
        # Check if exception indicates recoverable HTML error
        if exc is not None:
            error_str = str(exc).lower()
            recoverable_fragments = (
                "can't parse entities",
                "unsupported start tag",
                "can't find end tag",
                "message is too long",
                "text is too long",
                "caption",
                "html",
            )
            if not any(fragment in error_str for fragment in recoverable_fragments):
                return None
        
        # Normalize HTML to plain text
        normalized = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
        normalized = re.sub(r"</p\s*>", "\n\n", normalized, flags=re.IGNORECASE)
        normalized = re.sub(r"<[^>]+>", "", normalized)
        normalized = html.unescape(normalized)
        normalized = re.sub(r"\n{3,}", "\n\n", normalized).strip()
        
        if not normalized:
            return None
        if len(normalized) > TELEGRAM_TEXT_LIMIT:
            normalized = normalized[: TELEGRAM_TEXT_LIMIT - 1].rstrip() + "…"
        return normalized

    @staticmethod
    def _prepare_captions(text: str) -> tuple[str, str]:
        """Prepare HTML and plain text versions of caption."""
        html_caption = text.strip()
        if len(html_caption) <= TELEGRAM_CAPTION_LIMIT:
            # Generate plain fallback for safety
            plain_caption = TelegramBroadcaster._plain_text_fallback(html_caption, None) or html_caption
            if len(plain_caption) > TELEGRAM_CAPTION_LIMIT:
                plain_caption = plain_caption[: TELEGRAM_CAPTION_LIMIT - 1].rstrip() + "…"
            return html_caption, plain_caption
        
        # For oversized captions, convert to plain text
        plain_caption = TelegramBroadcaster._plain_text_fallback(html_caption, None) or html_caption
        if len(plain_caption) > TELEGRAM_CAPTION_LIMIT:
            plain_caption = plain_caption[: TELEGRAM_CAPTION_LIMIT - 1].rstrip() + "…"
        return plain_caption, plain_caption

    async def close(self) -> None:
        """Close aiogram bot session."""
        if self.bot:
            await self.bot.session.close()
            LOG.info("telegram bot session closed")


def _message_preview(text: str, *, limit: int = TELEGRAM_LOG_PREVIEW_LIMIT) -> str:
    preview = " | ".join(part.strip() for part in text.splitlines() if part.strip())
    if len(preview) <= limit:
        return preview
    return preview[: limit - 1].rstrip() + "…"


def _extract_retry_after_seconds(description: str) -> int | None:
    match = re.search(r"retry after\s+(\d+)", str(description or ""), flags=re.IGNORECASE)
    if not match:
        return None
    try:
        value = int(match.group(1))
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None
