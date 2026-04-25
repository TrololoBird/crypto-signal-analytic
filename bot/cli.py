"""Live-only entry point — the bot always runs in full live mode.

No dry-run, no once, no self-check. To verify connectivity, read the logs
and telemetry. To test, run the bot and watch real data.
"""
from __future__ import annotations

import asyncio
import ctypes
import errno
import json
import logging
import logging.handlers
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
import re
import shutil

from . import BotSettings, SignalBot, load_settings
from .startup_reporter import generate_and_send_startup_report
from .telemetry import TelemetryStore

_LOGGER_STDERR_PREFIX_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}(?:,\d{3})?\s+\|"
)


def _is_preformatted_log_stderr(line: str) -> bool:
    return bool(_LOGGER_STDERR_PREFIX_RE.match(line.lstrip()))


def _run_doctor_check(settings: BotSettings) -> None:
    """Run lightweight doctor diagnostics and log the result."""
    try:
        report = {
            "pid_lock": str(settings.pid_file),
            "logs_dir": str(settings.logs_dir),
            "telemetry_dir": str(settings.telemetry_dir),
            "db_path": str(settings.db_path),
            "config_filters": {
                "min_atr_pct": settings.filters.min_atr_pct,
                "min_score": settings.filters.min_score,
                "freshness_15m_minutes": settings.filters.freshness_15m_minutes,
                "freshness_1h_hours": settings.filters.freshness_1h_hours,
            },
            "ws": {
                "base_url": settings.ws.base_url,
                "public_base_url": settings.ws.public_base_url,
                "market_base_url": settings.ws.market_base_url,
                "subscribe_book_ticker": settings.ws.subscribe_book_ticker,
            },
            "setups_enabled": {k: getattr(settings.setups, k) for k in ("structure_pullback", "structure_break_retest", "wick_trap_reversal", "squeeze_setup", "ema_bounce", "order_block", "breaker_block", "fvg_setup", "bos_choch", "liquidity_sweep", "turtle_soup", "cvd_divergence", "hidden_divergence", "funding_reversal", "session_killzone")},
        }
        logging.getLogger("bot.cli").info("DOCTOR OK | %s", json.dumps(report, default=str))
    except Exception as exc:
        logging.getLogger("bot.cli").warning("DOCTOR DEGRADED: %s", exc)


def _bootstrap_env_if_missing() -> None:
    env_path = Path(".env")
    if env_path.exists():
        return
    env_path.write_text(
        "# Required secrets - fill in your values\n"
        "TG_TOKEN=\n"
        "TARGET_CHAT_ID=\n",
        encoding="utf-8",
    )
    print("[INFO] .env not found - created with empty TG_TOKEN and TARGET_CHAT_ID")

def _rotate_session_log(log_path: Path, *, stamp: str) -> None:
    if not log_path.exists():
        return
    try:
        if log_path.stat().st_size <= 0:
            return
    except OSError:
        return
    target = log_path.with_name(f"{log_path.stem}_{stamp}{log_path.suffix}")
    counter = 1
    while target.exists():
        target = log_path.with_name(f"{log_path.stem}_{stamp}.{counter}{log_path.suffix}")
        counter += 1
    try:
        shutil.move(str(log_path), str(target))
    except OSError as exc:
        sys.stderr.write(f"[WARN] failed to rotate previous session log {log_path}: {exc}\n")


def configure_logging(settings: BotSettings, *, debug_mode: bool = False) -> None:
    session_dt = datetime.now(timezone.utc)
    session_stamp = session_dt.strftime("%Y%m%d_%H%M%S")
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    
    # Per-session log file with unique name
    log_path = settings.logs_dir / f"bot_{session_stamp}_{os.getpid()}.log"
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(
            logging.FileHandler(log_path, encoding="utf-8", mode="w")
        )
    except OSError as exc:
        sys.stderr.write(f"[WARN] file logging disabled for {log_path}: {exc}\n")
    
    # Use DEBUG level for full traces
    log_level = logging.DEBUG if debug_mode else getattr(logging, settings.log_level, logging.INFO)
    
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(funcName)s:%(lineno)d | %(message)s",
        handlers=handlers,
        force=True,
    )
    
    # Keep asyncio debug disabled to avoid slow-task spam (tracemalloc handles coroutine tracking)
    asyncio.get_event_loop().set_debug(False)
    
    # Reduce noise from external libraries but keep warnings
    logging.getLogger("websockets").setLevel(logging.INFO)
    logging.getLogger("hpack").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)  # Only real warnings, not debug spam
    
    # Log file location
    start_time = session_dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    logger = logging.getLogger("bot.cli")
    logger.info("=" * 80)
    logger.info("BOT SESSION STARTED | %s", start_time)
    logger.info("LOG FILE | %s", log_path)
    logger.info("DEBUG MODE | %s", debug_mode)
    logger.info("ASYNCIO DEBUG | %s", asyncio.get_event_loop().get_debug())
    logger.info("=" * 80)


def _pid_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        process_query_limited_information = 0x1000
        handle = ctypes.windll.kernel32.OpenProcess(process_query_limited_information, False, pid)
        if handle:
            ctypes.windll.kernel32.CloseHandle(handle)
            return True
        return ctypes.get_last_error() == 5
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _read_pid_value(pid_file: Path) -> int:
    try:
        return int(pid_file.read_text(encoding="utf-8").strip() or "0")
    except (OSError, ValueError):
        return 0


async def _acquire_pid_lock(pid_file: Path) -> None:
    """Acquire PID lock asynchronously without blocking event loop."""
    # Run blocking filesystem operations in thread pool
    await asyncio.to_thread(pid_file.parent.mkdir, parents=True, exist_ok=True)
    
    flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
    if hasattr(os, "O_BINARY"):
        flags |= os.O_BINARY
    retries = 0
    while True:
        try:
            # Try to create lock file atomically
            fd = await asyncio.to_thread(os.open, pid_file, flags)
            try:
                await asyncio.to_thread(
                    os.write, fd, str(os.getpid()).encode("ascii", errors="strict")
                )
            finally:
                await asyncio.to_thread(os.close, fd)
            return
        except FileExistsError:
            existing_pid = await asyncio.to_thread(_read_pid_value, pid_file)
            if existing_pid and existing_pid != os.getpid() and _pid_is_alive(existing_pid):
                raise SystemExit(f"another bot process is already running with pid {existing_pid}")
            # Race hardening for empty/initializing PID file:
            # never unlink immediately; first give other process enough time
            # to finish writing its PID.
            if existing_pid == 0:
                retries += 1
                try:
                    stat = await asyncio.to_thread(pid_file.stat)
                    age_s = max(0.0, time.time() - stat.st_mtime)
                except OSError:
                    age_s = 0.0
                if retries <= 50 or age_s < 10.0:
                    await asyncio.sleep(0.1)
                    continue
            try:
                await asyncio.to_thread(pid_file.unlink)
            except FileNotFoundError:
                continue
            except OSError as exc:
                raise SystemExit(f"failed to remove stale pid lock {pid_file}: {exc}") from exc
            # After successful unlink, retry the lock acquisition
            continue


def _release_pid_lock(pid_file: Path) -> None:
    try:
        if pid_file.exists():
            current = pid_file.read_text(encoding="utf-8").strip()
            if current == str(os.getpid()):
                pid_file.unlink()
    except OSError:
        logging.getLogger("bot.cli").warning("failed to release pid lock %s", pid_file)


def _setup_signal_handlers(bot: SignalBot) -> None:
    loop = asyncio.get_running_loop()

    def _request_shutdown() -> None:
        try:
            bot.request_shutdown()
        except Exception as exc:
            logging.getLogger("bot.cli").warning("failed to request shutdown: %s", repr(exc))

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _request_shutdown)
        except (NotImplementedError, TypeError) as exc:
            logging.getLogger("bot.cli").debug(
                "asyncio signal handler unavailable (%s), using signal.signal", repr(exc)
            )
            try:
                signal.signal(sig, lambda _signum, _frame: _request_shutdown())
            except (ValueError, NotImplementedError, OSError) as exc2:
                logging.getLogger("bot.cli").debug(
                    "signal.signal unavailable for %s (%s)", sig, repr(exc2)
                )
        except Exception as exc:
            logging.getLogger("bot.cli").warning("signal handler setup failed: %s", repr(exc))


async def _main() -> None:
    _bootstrap_env_if_missing()
    settings = load_settings("config.toml")
    settings.validate_for_runtime(require_telegram=True)

    try:
        await generate_and_send_startup_report(
            Path(".").resolve(),
            send_telegram=True,
            config_path="config.toml",
        )
    except Exception as exc:
        sys.stderr.write(f"[WARN] startup report failed: {exc}\n")

    debug_mode = os.getenv("DEBUG_BOT", "0") in ("1", "true", "yes")
    configure_logging(settings, debug_mode=debug_mode)
    
    # Capture all warnings as log entries
    logging.captureWarnings(True)
    
    # Redirect stderr to logger while preserving original output
    _orig_stderr = sys.stderr
    class _StderrToLog:
        def __init__(self, logger_name: str, orig) -> None:
            self.logger = logging.getLogger(logger_name)
            self._orig = orig
            self._buf = ""
        def write(self, msg: str) -> None:
            self._orig.write(msg)  # Always write to original stderr
            self._orig.flush()
            # Also log non-empty lines
            self._buf += msg
            if "\n" in self._buf:
                lines = self._buf.split("\n")
                for line in lines[:-1]:
                    if line.strip() and not _is_preformatted_log_stderr(line):
                        self.logger.warning("STDERR: %s", line)
                self._buf = lines[-1]
        def flush(self) -> None:
            self._orig.flush()
            if self._buf.strip():
                self.logger.warning("STDERR: %s", self._buf)
                self._buf = ""
    
    sys.stderr = _StderrToLog("stderr", _orig_stderr)  # type: ignore[assignment]
    
    # Hook to log unhandled exceptions
    def _log_exception(loop, context):
        msg = context.get("exception", context["message"])
        logging.getLogger("asyncio").exception("Unhandled exception: %s", msg, exc_info=msg if isinstance(msg, BaseException) else None)
    
    asyncio.get_running_loop().set_exception_handler(_log_exception)
    
    _run_doctor_check(settings)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S") + f"_{os.getpid()}"
    telemetry = TelemetryStore(settings.telemetry_dir, run_id=run_id)
    bot = SignalBot(settings, telemetry=telemetry)
    await _acquire_pid_lock(settings.pid_file)
    try:
        _setup_signal_handlers(bot)
        await bot.start()
        await bot.run_forever()
    finally:
        try:
            await bot.close()
        finally:
            _release_pid_lock(settings.pid_file)


def run() -> None:
    debug_mode = os.getenv("DEBUG_BOT", "0") in ("1", "true", "yes")
    
    if debug_mode:
        # Enable tracemalloc to track unawaited coroutines (without asyncio spam)
        import tracemalloc
        tracemalloc.start(25)
        sys.stderr.write("[DEBUG] tracemalloc enabled | logging level=DEBUG\n")
    
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        logging.getLogger("bot.cli").info("bot stopped by user")
    finally:
        if debug_mode:
            import tracemalloc
            current, peak = tracemalloc.get_traced_memory()
            logging.getLogger("bot.cli").debug("Memory: current=%.2fMB peak=%.2fMB", current/1024/1024, peak/1024/1024)
