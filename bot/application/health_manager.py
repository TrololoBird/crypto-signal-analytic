from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from bot.application.bot import SignalBot


LOG = logging.getLogger("bot.application.bot")


class HealthManager:
    def __init__(self, bot: SignalBot) -> None:
        self._bot = bot

    async def health_check(self) -> dict[str, Any]:
        ws_connected = bool(getattr(self._bot._ws_manager, "is_connected", lambda: False)())
        pending_outcomes = len(getattr(self._bot.tracker, "_pending_outcomes", []))
        try:
            active_rows = await self._bot._modern_repo.get_active_signals(include_closed=False)
            active_signals = len([r for r in active_rows if r.get("status") in ("pending", "active")])
        except Exception:
            active_signals = 0
        return {
            "status": "healthy" if self._bot._running else "degraded",
            "ws_connected": ws_connected,
            "active_signals": active_signals,
            "pending_outcomes": pending_outcomes,
            "shortlist_size": len(self._bot._shortlist),
            "last_kline_event_age_seconds": max(
                0.0,
                asyncio.get_running_loop().time() - self._bot._last_kline_event_ts,
            ),
            "feature_flags": await self._bot.feature_flags.snapshot(),
        }

    async def heartbeat_periodic(self) -> None:
        while not self._bot._shutdown.is_set():
            await asyncio.sleep(300)
            if self._bot._shutdown.is_set():
                break
            async with self._bot._shortlist_lock:
                sl_size = len(self._bot._shortlist)
            active_sigs = await self._bot._modern_repo.get_active_signals()
            open_signals = len(active_sigs)
            ws_lag = 0
            ws_age = 0
            if self._bot._ws_manager is not None:
                ws_lag = self._bot._ws_manager._get_current_latency_ms() or 0
                ws_age = self._bot._ws_manager._last_message_age_seconds() or 0
            mem_summary = await self._bot._modern_repo.summary()
            blacklisted = mem_summary.get("blacklisted_symbols", [])
            market_ctx = await self._bot._modern_repo.get_market_context()

            regime_info = "n/a"
            if self._bot.market_regime._last_result is not None:
                r = self._bot.market_regime._last_result
                regime_info = f"{r.regime}:{r.strength:.1f}"

            LOG.info(
                "heartbeat | shortlist=%d open_signals=%d ws_lag_ms=%d ws_msg_age_s=%d "
                "market=%s btc_bias=%s memory_blacklist=%s",
                sl_size,
                open_signals,
                ws_lag,
                ws_age,
                regime_info,
                market_ctx.get("btc_bias", "neutral"),
                blacklisted if blacklisted else "none",
            )

            if self._bot.metrics._enabled:
                self._bot.metrics.update_bot_state(sl_size, open_signals, len(blacklisted))
                self._bot.metrics.record_ws_latency(ws_lag)
                self._bot.metrics.record_ws_message_age(ws_age)
                if self._bot._ws_manager is not None:
                    self._bot.metrics.update_ws_streams(len(self._bot._ws_manager._symbols))
                if self._bot.market_regime._last_result is not None:
                    r = self._bot.market_regime._last_result
                    self._bot.metrics.update_market_regime(
                        r.regime,
                        r.strength,
                        r.altcoin_season_index,
                    )

    async def health_telemetry_periodic(self) -> None:
        while not self._bot._shutdown.is_set():
            await asyncio.sleep(60)
            if self._bot._shutdown.is_set():
                break
            row: dict[str, Any] = {
                "ts": datetime.now(UTC).isoformat(),
                "prepare_error_count": self._bot._prepare_error_count,
            }
            if self._bot._last_prepare_error:
                row["prepare_error_stage"] = self._bot._last_prepare_error.get("stage")
                row["prepare_error_exception_type"] = self._bot._last_prepare_error.get(
                    "exception_type"
                )
            if self._bot._ws_manager is not None:
                ws_snapshot = self._bot._ws_manager.state_snapshot()
                row.update(ws_snapshot if isinstance(ws_snapshot, dict) else {})
            rest_snapshot_func = getattr(self._bot.client, "state_snapshot", None)
            if callable(rest_snapshot_func):
                rest_snapshot = rest_snapshot_func()
                row.update(rest_snapshot if isinstance(rest_snapshot, dict) else {})
            self._bot.telemetry.append_jsonl("health.jsonl", row)
