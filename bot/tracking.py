"""Signal tracking module for monitoring signal lifecycle and outcomes."""
from __future__ import annotations

import asyncio
from bisect import bisect_right
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import logging
import os
from pathlib import Path
import tempfile
import typing
from typing import Any, Mapping

import polars as pl

from .config import BotSettings
from .market_data import MarketDataUnavailable
from .models import AggTrade, Signal
from .outcomes import SignalFeatures, create_outcome_from_tracked
from .telemetry import TelemetryStore
from .tracked_signals import TrackedSignalState, parse_state_dt

if typing.TYPE_CHECKING:
    from .core.memory import MemoryRepository


UTC = timezone.utc
LOG = logging.getLogger("bot.tracking")


@dataclass(frozen=True, slots=True)
class SignalTrackingEvent:
    """Event representing a state change in signal tracking."""

    event_type: str
    tracked: TrackedSignalState
    occurred_at: datetime
    event_price: float | None
    precision_mode: str
    note: str | None = None

    def to_log_row(self, *, stats: Mapping[str, float | int]) -> dict[str, Any]:
        """Convert event to loggable dictionary format."""
        return {
            "ts": self.occurred_at.astimezone(UTC).isoformat(),
            "event_type": self.event_type,
            "lifecycle_event": self.event_type,
            "tracking_semantics": "tracked_signal_lifecycle",
            "runtime_mode": "signal_only",
            "exchange_execution": False,
            "tracking_id": self.tracked.tracking_id,
            "tracking_ref": self.tracked.tracking_ref,
            "signal_key": self.tracked.signal_key,
            "symbol": self.tracked.symbol,
            "setup_id": self.tracked.setup_id,
            "direction": self.tracked.direction,
            "status": self.tracked.status,
            "close_reason": self.tracked.close_reason,
            "event_price": self.event_price,
            "precision_mode": self.precision_mode,
            "note": self.note,
            "entry_low": self.tracked.entry_low,
            "entry_high": self.tracked.entry_high,
            "stop": self.tracked.stop,
            "take_profit_1": self.tracked.take_profit_1,
            "take_profit_2": self.tracked.take_profit_2,
            "single_target_mode": self.tracked.single_target_mode,
            "target_integrity_status": self.tracked.target_integrity_status,
            "created_at": self.tracked.created_at,
            "activated_at": self.tracked.activated_at,
            "tp1_hit_at": self.tracked.tp1_hit_at,
            "closed_at": self.tracked.closed_at,
            "stats": stats,
        }


class SignalTracker:
    def __init__(
        self,
        settings: BotSettings,
        *,
        market_data: Any,
        telemetry: TelemetryStore,
        features_store: dict[str, SignalFeatures] | None = None,
        memory_repo: MemoryRepository,
    ) -> None:
        self.settings = settings
        self.market_data = market_data
        self.telemetry = telemetry
        self.memory_repo = memory_repo
        self._features_file: Path | None = getattr(settings, "features_store_file", None)
        if features_store is not None:
            self.features_store = features_store
        else:
            self.features_store = self._load_features_store()

        # Pending outcomes queue for batching I/O operations
        self._pending_outcomes: list[dict[str, Any]] = []
        self._pending_outcomes_lock = asyncio.Lock()
        self._pending_outcomes_flush_size = 10  # Flush when queue reaches this size

    async def _flush_pending_outcomes(self) -> None:
        """Flush pending outcomes to disk in batch."""
        if not self._pending_outcomes:
            return

        async with self._pending_outcomes_lock:
            outcomes_to_flush = self._pending_outcomes[:]
            self._pending_outcomes.clear()

        if outcomes_to_flush:
            try:
                await self.memory_repo.save_signal_outcomes_batch(outcomes_to_flush)
            except Exception as exc:
                LOG.warning("batch outcome flush failed: %s", exc)

    def _load_features_store(self) -> dict[str, SignalFeatures]:
        """Load persisted features from disk. Returns empty dict on any error."""
        if not self._features_file or not self._features_file.exists():
            return {}
        try:
            data = json.loads(self._features_file.read_text(encoding="utf-8"))
            result: dict[str, SignalFeatures] = {}
            for tid, fdict in data.items():
                try:
                    result[tid] = SignalFeatures(**fdict)
                except (ValueError, TypeError):
                    pass
            if result:
                LOG.info("features_store loaded | entries=%d", len(result))
            return result
        except Exception as exc:
            LOG.debug("features_store load failed: %s", exc)
            return {}

    def _persist_features_store(self) -> None:
        """Persist features to disk so they survive bot restarts."""
        if not self._features_file:
            return
        tmp_path: str | None = None
        try:
            self._features_file.parent.mkdir(parents=True, exist_ok=True)
            data = {tid: f.to_dict() for tid, f in self.features_store.items()}
            with tempfile.NamedTemporaryFile(
                "w",
                encoding="utf-8",
                dir=self._features_file.parent,
                prefix=f"{self._features_file.name}.",
                suffix=".tmp",
                delete=False,
            ) as handle:
                tmp_path = handle.name
                handle.write(json.dumps(data, indent=2))
                handle.flush()
            os.replace(tmp_path, self._features_file)
        except Exception as exc:
            LOG.debug("features_store persist failed: %s", exc)
            if tmp_path:
                try:
                    Path(tmp_path).unlink(missing_ok=True)
                except OSError:
                    pass

    def set_signal_features(self, tracking_id: str, features: SignalFeatures) -> None:
        """Сохраняет признаки сигнала для последующей записи в outcome."""
        self.features_store[tracking_id] = features
        self._persist_features_store()

    def _tracked_to_payload(self, tracked: TrackedSignalState) -> dict[str, Any]:
        fields = getattr(TrackedSignalState, "__struct_fields__", ())
        payload = {field: getattr(tracked, field, None) for field in fields}
        payload["reasons"] = list(tracked.reasons)
        return payload

    def _tracked_from_payload(self, payload: dict[str, Any]) -> TrackedSignalState:
        row = dict(payload)
        if "reasons" in row and isinstance(row["reasons"], list):
            row["reasons"] = tuple(row["reasons"])
        row.setdefault("initial_stop", row.get("stop"))
        row.setdefault("stop_price", row.get("stop"))
        row.setdefault("tp1_price", row.get("take_profit_1"))
        row.setdefault("tp2_price", row.get("take_profit_2"))
        row.setdefault("single_target_mode", False)
        row.setdefault("target_integrity_status", "unchecked")
        row["single_target_mode"] = bool(row.get("single_target_mode"))
        return TrackedSignalState(**row)

    async def _stats_snapshot(self) -> dict[str, int]:
        return await self.memory_repo.get_tracking_stats()

    async def _active_signals(self, *, symbol: str | None = None) -> list[TrackedSignalState]:
        rows = await self.memory_repo.get_active_signals(symbol=symbol)
        return [self._tracked_from_payload(row) for row in rows]

    async def _persist_tracking_state(self) -> None:
        # Tracking rows are persisted incrementally via MemoryRepository calls.
        # Here we only flush pending batched outcomes so callers that expect
        # an explicit "persist" step do not lose queued outcomes on shutdown.
        await self._flush_pending_outcomes()

    async def _arm_signal(
        self,
        signal: Signal,
        *,
        signal_message_id: int | None,
    ) -> TrackedSignalState:
        created_at = signal.created_at.astimezone(UTC)
        tracked = TrackedSignalState(
            tracking_id=signal.tracking_id,
            tracking_ref=signal.tracking_ref,
            signal_key=signal.signal_key,
            symbol=signal.symbol,
            setup_id=signal.setup_id,
            direction=signal.direction,
            timeframe=signal.timeframe,
            created_at=created_at.isoformat(),
            pending_expires_at=(created_at + timedelta(minutes=self.settings.tracking.pending_expiry_minutes)).isoformat(),
            active_expires_at=(created_at + timedelta(minutes=self.settings.tracking.active_expiry_minutes)).isoformat(),
            entry_low=signal.entry_low,
            entry_high=signal.entry_high,
            entry_mid=signal.entry_mid,
            initial_stop=signal.stop,
            stop=signal.stop,
            stop_price=signal.stop,
            take_profit_1=signal.take_profit_1,
            tp1_price=signal.take_profit_1,
            take_profit_2=signal.take_profit_2,
            tp2_price=signal.take_profit_2,
            single_target_mode=signal.single_target_mode,
            target_integrity_status=signal.target_integrity_status,
            score=signal.score,
            risk_reward=signal.risk_reward,
            reasons=signal.reasons,
            signal_message_id=signal_message_id,
            bias_4h=signal.bias_4h,
            quote_volume=signal.quote_volume,
            spread_bps=signal.spread_bps,
            atr_pct=signal.atr_pct,
            orderflow_delta_ratio=signal.orderflow_delta_ratio,
            status="pending",
        )
        await self.memory_repo.save_active_signal(self._tracked_to_payload(tracked))
        await self.memory_repo.increment_tracking_stats(signals_sent=1)
        return tracked

    async def _mark_activated(
        self,
        tracked: TrackedSignalState,
        *,
        activated_at: datetime,
        price: float,
        precision_mode: str,
    ) -> None:
        tracked.status = "active"
        tracked.activated_at = activated_at.astimezone(UTC).isoformat()
        tracked.activation_price = price
        tracked.last_checked_at = activated_at.astimezone(UTC).isoformat()
        tracked.last_price = price
        await self.memory_repo.save_active_signal(self._tracked_to_payload(tracked))
        await self.memory_repo.increment_tracking_stats(activated=1)

    async def _mark_tp1(
        self,
        tracked: TrackedSignalState,
        *,
        occurred_at: datetime,
        price: float,
        precision_mode: str,
        move_stop_to_break_even: bool,
    ) -> None:
        if tracked.tp1_hit_at is not None:
            return
        tracked.tp1_hit_at = occurred_at.astimezone(UTC).isoformat()
        tracked.tp1_price = price
        if move_stop_to_break_even:
            tracked.stop = tracked.entry_mid
            tracked.stop_price = tracked.entry_mid
        await self.memory_repo.save_active_signal(self._tracked_to_payload(tracked))
        await self.memory_repo.increment_tracking_stats(tp1_hit=1)

    async def _mark_checked(
        self,
        tracked: TrackedSignalState,
        *,
        checked_at: datetime,
        last_price: float | None,
        precision_mode: str,
    ) -> None:
        tracked.last_checked_at = checked_at.astimezone(UTC).isoformat()
        tracked.last_price = last_price
        await self.memory_repo.save_active_signal(self._tracked_to_payload(tracked))

    async def _close_signal(
        self,
        tracked: TrackedSignalState,
        *,
        reason: str,
        occurred_at: datetime,
        price: float | None,
        precision_mode: str,
    ) -> None:
        tracked.status = "closed"
        tracked.closed_at = occurred_at.astimezone(UTC).isoformat()
        tracked.close_reason = reason
        tracked.close_price = price
        if reason == "tp1_hit":
            if tracked.tp1_hit_at is None:
                tracked.tp1_hit_at = occurred_at.astimezone(UTC).isoformat()
            tracked.tp1_price = price if price is not None else tracked.take_profit_1
        elif reason == "tp2_hit":
            tracked.tp2_hit_at = occurred_at.astimezone(UTC).isoformat()
            tracked.tp2_price = price
        elif reason == "stop_loss" and price is not None:
            tracked.stop_price = price
        await self.memory_repo.save_active_signal(self._tracked_to_payload(tracked))

        deltas = {
            "tp1_hit": {"tp1_hit": 1},
            "tp2_hit": {"tp2_hit": 1},
            "stop_loss": {"stop_loss": 1},
            "expired": {"expired": 1},
            "ambiguous_exit": {"ambiguous_exit": 1},
        }.get(reason)
        if deltas:
            await self.memory_repo.increment_tracking_stats(**deltas)

    async def _record_setup_outcome(self, setup_id: str, event_type: str) -> None:
        await self.memory_repo.record_setup_outcome(setup_id, event_type)

    async def arm_signals(self, signals: list[Signal], *, dry_run: bool) -> None:
        await self.arm_signals_with_messages(signals, dry_run=dry_run, message_ids={})

    async def arm_signals_with_messages(
        self,
        signals: list[Signal],
        *,
        dry_run: bool,
        message_ids: dict[str, int | None],
    ) -> None:
        if dry_run or not self.settings.tracking.enabled or not signals:
            return
        now = datetime.now(UTC)
        for signal in signals:
            tracked = await self._arm_signal(
                signal,
                signal_message_id=message_ids.get(signal.tracking_id),
            )
            self.telemetry.append_jsonl(
                "tracking_events.jsonl",
                {
                    "ts": now.isoformat(),
                    "event_type": "armed",
                    "tracking_id": tracked.tracking_id,
                    "tracking_ref": tracked.tracking_ref,
                    "signal_key": tracked.signal_key,
                    "symbol": tracked.symbol,
                    "setup_id": tracked.setup_id,
                    "direction": tracked.direction,
                    "entry_low": tracked.entry_low,
                    "entry_high": tracked.entry_high,
                    "stop": tracked.stop,
                    "take_profit_1": tracked.take_profit_1,
                    "take_profit_2": tracked.take_profit_2,
                    "single_target_mode": tracked.single_target_mode,
                    "target_integrity_status": tracked.target_integrity_status,
                    "signal_message_id": tracked.signal_message_id,
                    "stats": await self._stats_snapshot(),
                },
            )
        await self._persist_tracking_state()

    async def review_open_signals(self, *, dry_run: bool) -> list[SignalTrackingEvent]:
        if dry_run or not self.settings.tracking.enabled:
            return []
        tracked_rows = await self._active_signals()
        if not tracked_rows:
            return []

        now = datetime.now(UTC)
        by_symbol: dict[str, list[TrackedSignalState]] = {}
        for tracked in tracked_rows:
            by_symbol.setdefault(tracked.symbol, []).append(tracked)

        events: list[SignalTrackingEvent] = []
        for symbol, symbol_rows in by_symbol.items():
            symbol_rows.sort(key=lambda item: item.created_at)
            try:
                symbol_events = await self._review_symbol(symbol, symbol_rows, now=now)
            except Exception:
                LOG.exception("tracking review failed for %s; using time fallback", symbol)
                symbol_events = await self._apply_time_fallback_rows(
                    symbol_rows,
                    now=now,
                    precision_mode="time_fallback",
                )
            events.extend(symbol_events)

        # Persist first to avoid re-emitting the same events after restart, but
        # never let a persistence failure suppress Telegram updates and telemetry.
        if tracked_rows:
            try:
                await self._persist_tracking_state()
            except (OSError, IOError):
                LOG.exception("tracking state persist failed (continuing without persistence)")
        for event in events:
            self.telemetry.append_jsonl(
                "tracking_events.jsonl",
                event.to_log_row(stats=await self._stats_snapshot()),
            )
        return events

    async def force_close_tracking_ids(
        self,
        tracking_ids: list[str],
        *,
        reason: str,
        occurred_at: datetime,
        note: str | None = None,
    ) -> list[SignalTrackingEvent]:
        if not tracking_ids:
            return []
        tracked_rows = await self._active_signals()
        if not tracked_rows:
            return []

        wanted = {str(item) for item in tracking_ids if str(item)}
        events: list[SignalTrackingEvent] = []
        for tracked in tracked_rows:
            if tracked.tracking_id not in wanted:
                continue
            price = tracked.last_price or tracked.close_price or tracked.activation_price or tracked.entry_mid
            events.append(
                await self._close_event(
                    tracked,
                    event_type=reason,
                    occurred_at=occurred_at,
                    price=price,
                    precision_mode="system",
                    note=note,
                )
            )

        if tracked_rows:
            try:
                await self._persist_tracking_state()
            except (OSError, IOError):
                LOG.exception("tracking state persist failed for forced close (continuing)")
        stats = await self._stats_snapshot()
        for event in events:
            self.telemetry.append_jsonl("tracking_events.jsonl", event.to_log_row(stats=stats))
        return events

    async def _review_symbol(
        self,
        symbol: str,
        tracked_rows: list[TrackedSignalState],
        *,
        now: datetime,
    ) -> list[SignalTrackingEvent]:
        oldest_check = now
        for tracked in tracked_rows:
            last_checked = parse_state_dt(tracked.last_checked_at) or parse_state_dt(tracked.created_at) or now
            if last_checked < oldest_check:
                oldest_check = last_checked
        start_time_ms = int(oldest_check.timestamp() * 1000)
        end_time_ms = int(now.timestamp() * 1000)

        try:
            trades, complete = await asyncio.wait_for(
                self.market_data.fetch_agg_trades(
                    symbol,
                    start_time_ms=start_time_ms,
                    end_time_ms=end_time_ms,
                    page_limit=self.settings.tracking.agg_trade_page_limit,
                    page_size=self.settings.tracking.agg_trade_page_size,
                ),
                timeout=self.settings.ws.rest_timeout_seconds,
            )
        except asyncio.TimeoutError:
            LOG.debug("agg trades timed out for %s; falling back to candles", symbol)
            trades = []
            complete = False
        except MarketDataUnavailable as exc:
            LOG.debug("agg trades unavailable for %s; falling back to candles: %s", symbol, exc)
            trades = []
            complete = False
        except (ValueError, TypeError):
            trades = []
            complete = False
        except Exception as exc:
            LOG.warning("agg trades review failed for %s; falling back to candles: %s", symbol, exc)
            trades = []
            complete = False

        if complete:
            events: list[SignalTrackingEvent] = []
            trade_times = [trade.trade_time for trade in trades]
            for tracked in tracked_rows:
                events.extend(await self._apply_trade_rows(tracked, trades, trade_times=trade_times, now=now))
            return events

        lookback_minutes = max(30, int((now - oldest_check).total_seconds() / 60.0) + 5)
        lookback_limit = min(max(lookback_minutes, 60), 1000)
        try:
            candles = await self.market_data.fetch_klines(symbol, "1m", limit=lookback_limit)
        except MarketDataUnavailable as exc:
            LOG.warning("tracking candles unavailable for %s; using time fallback: %s", symbol, exc)
            return await self._apply_time_fallback_rows(
                tracked_rows,
                now=now,
                precision_mode="time_fallback",
            )
        except Exception as exc:
            LOG.warning("tracking candle review failed for %s; using time fallback: %s", symbol, exc)
            return await self._apply_time_fallback_rows(
                tracked_rows,
                now=now,
                precision_mode="time_fallback",
            )
        events = []
        for tracked in tracked_rows:
            try:
                events.extend(await self._apply_candle_rows(tracked, candles, now=now))
            except Exception as exc:
                LOG.warning(
                    "tracking candle application failed for %s/%s; using time fallback: %s",
                    symbol,
                    tracked.tracking_id,
                    exc,
                )
                events.extend(
                    await self._apply_time_fallback(
                        tracked,
                        now=now,
                        precision_mode="time_fallback",
                    )
                )
        return events

    async def _apply_trade_rows(
        self,
        tracked: TrackedSignalState,
        trades: list[AggTrade],
        *,
        trade_times: list[datetime] | None = None,
        now: datetime,
    ) -> list[SignalTrackingEvent]:
        events: list[SignalTrackingEvent] = []
        last_checked = parse_state_dt(tracked.last_checked_at) or parse_state_dt(tracked.created_at) or now
        pending_expires_at = parse_state_dt(tracked.pending_expires_at) or now
        active_expires_at = parse_state_dt(tracked.active_expires_at) or now
        last_price = tracked.last_price

        relevant = trades
        if trades:
            ordered_times = trade_times or [trade.trade_time for trade in trades]
            start_idx = bisect_right(ordered_times, last_checked)
            relevant = trades[start_idx:]
        for trade in relevant:
            last_price = trade.price
            if tracked.activated_at is None:
                if trade.trade_time > pending_expires_at:
                    events.append(
                        await self._close_event(
                            tracked,
                            event_type="expired",
                            occurred_at=pending_expires_at,
                            price=last_price,
                            precision_mode="trade",
                        )
                    )
                    return events
                if _price_in_entry_zone(tracked, trade.price):
                    await self._mark_activated(
                        tracked,
                        activated_at=trade.trade_time,
                        price=trade.price,
                        precision_mode="trade",
                    )
                    events.append(
                        SignalTrackingEvent(
                            event_type="activated",
                            tracked=tracked,
                            occurred_at=trade.trade_time,
                            event_price=trade.price,
                            precision_mode="trade",
                        )
                    )
            if tracked.activated_at is None:
                continue
            if trade.trade_time > active_expires_at:
                events.append(
                    await self._close_event(
                        tracked,
                        event_type="expired",
                        occurred_at=active_expires_at,
                        price=last_price,
                        precision_mode="trade",
                    )
                )
                return events
            tick_events, closed = await self._apply_price_tick(
                tracked,
                price=trade.price,
                occurred_at=trade.trade_time,
                precision_mode="trade",
            )
            if tick_events:
                events.extend(tick_events)
            if closed:
                return events

        if tracked.activated_at is None and now > pending_expires_at:
            events.append(
                await self._close_event(
                    tracked,
                    event_type="expired",
                    occurred_at=pending_expires_at,
                    price=last_price,
                    precision_mode="trade",
                )
            )
            return events
        if tracked.activated_at is not None and now > active_expires_at:
            events.append(
                await self._close_event(
                    tracked,
                    event_type="expired",
                    occurred_at=active_expires_at,
                    price=last_price,
                    precision_mode="trade",
                )
            )
            return events
        await self._mark_checked(tracked, checked_at=now, last_price=last_price, precision_mode="trade")
        return events

    async def _apply_candle_rows(
        self,
        tracked: TrackedSignalState,
        candles: pl.DataFrame,
        *,
        now: datetime,
    ) -> list[SignalTrackingEvent]:
        events: list[SignalTrackingEvent] = []
        last_checked = parse_state_dt(tracked.last_checked_at) or parse_state_dt(tracked.created_at) or now
        pending_expires_at = parse_state_dt(tracked.pending_expires_at) or now
        active_expires_at = parse_state_dt(tracked.active_expires_at) or now
        last_price = tracked.last_price

        # Filter candles with close_time > last_checked using Polars
        relevant = candles.filter(pl.col("close_time") > last_checked)
        relevant = relevant.sort("close_time")
        last_processed_at: datetime | None = None
        for row in relevant.iter_rows(named=True):
            bar_close_time = row["close_time"]
            if isinstance(bar_close_time, str):
                bar_close_time = datetime.fromisoformat(bar_close_time.replace('Z', '+00:00'))
            bar_high = float(row["high"])
            bar_low = float(row["low"])
            bar_close = float(row["close"])
            last_processed_at = bar_close_time
            last_price = bar_close

            entry_touched = _bar_touches_entry(tracked, high=bar_high, low=bar_low)
            tp1_touched = _bar_hits_tp1(tracked, high=bar_high, low=bar_low)
            tp2_touched = _bar_hits_tp2(tracked, high=bar_high, low=bar_low)
            # Stop triggers immediately on price touch (as in real Binance futures trading)
            stop_touched = _bar_hits_stop(tracked, high=bar_high, low=bar_low)

            if tracked.activated_at is None:
                if bar_close_time > pending_expires_at:
                    events.append(
                        await self._close_event(
                            tracked,
                            event_type="expired",
                            occurred_at=pending_expires_at,
                            price=last_price,
                            precision_mode="candle",
                        )
                    )
                    return events
                if entry_touched and (tp1_touched or tp2_touched or stop_touched):
                    events.append(
                        await self._close_event(
                            tracked,
                            event_type="ambiguous_exit",
                            occurred_at=bar_close_time,
                            price=bar_close,
                            precision_mode="candle",
                            note="entry_and_exit_same_bar",
                        )
                    )
                    return events
                if entry_touched:
                    await self._mark_activated(
                        tracked,
                        activated_at=bar_close_time,
                        price=bar_close,
                        precision_mode="candle",
                    )
                    events.append(
                        SignalTrackingEvent(
                            event_type="activated",
                            tracked=tracked,
                            occurred_at=bar_close_time,
                            event_price=bar_close,
                            precision_mode="candle",
                        )
                    )
            if tracked.activated_at is None:
                continue
            if bar_close_time > active_expires_at:
                events.append(
                    await self._close_event(
                        tracked,
                        event_type="expired",
                        occurred_at=active_expires_at,
                        price=last_price,
                        precision_mode="candle",
                    )
                )
                return events
            if (tp2_touched and stop_touched) or (tracked.tp1_hit_at is None and tp1_touched and stop_touched):
                events.append(
                    await self._close_event(
                        tracked,
                        event_type="ambiguous_exit",
                        occurred_at=bar_close_time,
                        price=bar_close,
                        precision_mode="candle",
                        note="same_bar_conflict",
                    )
                )
                return events
            if tracked.single_target_mode and (tp1_touched or tp2_touched):
                events.append(
                    await self._close_event(
                        tracked,
                        event_type="tp1_hit",
                        occurred_at=bar_close_time,
                        price=tracked.take_profit_1,
                        precision_mode="candle",
                        note="single_target_close",
                    )
                )
                return events
            if tp2_touched:
                if tracked.tp1_hit_at is None:
                    await self._mark_tp1(
                        tracked,
                        occurred_at=bar_close_time,
                        price=tracked.take_profit_1,
                        precision_mode="candle",
                        move_stop_to_break_even=self.settings.tracking.move_stop_to_break_even_on_tp1,
                    )
                    events.append(
                        SignalTrackingEvent(
                            event_type="tp1_hit",
                            tracked=tracked,
                            occurred_at=bar_close_time,
                            event_price=tracked.take_profit_1,
                            precision_mode="candle",
                            note="tp2_implies_tp1",
                        )
                    )
                events.append(
                    await self._close_event(
                        tracked,
                        event_type="tp2_hit",
                        occurred_at=bar_close_time,
                        price=tracked.take_profit_2,
                        precision_mode="candle",
                    )
                )
                return events
            if stop_touched:
                events.append(
                    await self._close_event(
                        tracked,
                        event_type="stop_loss",
                        occurred_at=bar_close_time,
                        price=tracked.stop,
                        precision_mode="candle",
                    )
                )
                return events
            if tp1_touched and tracked.tp1_hit_at is None:
                await self._mark_tp1(
                    tracked,
                    occurred_at=bar_close_time,
                    price=tracked.take_profit_1,
                    precision_mode="candle",
                    move_stop_to_break_even=self.settings.tracking.move_stop_to_break_even_on_tp1,
                )
                events.append(
                    SignalTrackingEvent(
                        event_type="tp1_hit",
                        tracked=tracked,
                        occurred_at=bar_close_time,
                        event_price=tracked.take_profit_1,
                        precision_mode="candle",
                    )
                )

        if tracked.activated_at is None and now > pending_expires_at:
            events.append(
                await self._close_event(
                    tracked,
                    event_type="expired",
                    occurred_at=pending_expires_at,
                    price=last_price,
                    precision_mode="candle",
                )
            )
            return events
        if tracked.activated_at is not None and now > active_expires_at:
            events.append(
                await self._close_event(
                    tracked,
                    event_type="expired",
                    occurred_at=active_expires_at,
                    price=last_price,
                    precision_mode="candle",
                )
            )
            return events
        await self._mark_checked(
            tracked,
            checked_at=last_processed_at if last_processed_at is not None else now,
            last_price=last_price,
            precision_mode="candle",
        )
        return events

    async def _apply_time_fallback(
        self,
        tracked: TrackedSignalState,
        *,
        now: datetime,
        precision_mode: str,
    ) -> list[SignalTrackingEvent]:
        pending_expires_at = parse_state_dt(tracked.pending_expires_at) or now
        active_expires_at = parse_state_dt(tracked.active_expires_at) or now
        last_price = tracked.last_price or tracked.close_price or tracked.activation_price or tracked.entry_mid

        if tracked.activated_at is None and now > pending_expires_at:
            return [
                await self._close_event(
                    tracked,
                    event_type="expired",
                    occurred_at=pending_expires_at,
                    price=last_price,
                    precision_mode=precision_mode,
                    note="time_fallback_pending_expiry",
                )
            ]
        if tracked.activated_at is not None and now > active_expires_at:
            return [
                await self._close_event(
                    tracked,
                    event_type="expired",
                    occurred_at=active_expires_at,
                    price=last_price,
                    precision_mode=precision_mode,
                    note="time_fallback_active_expiry",
                )
            ]

        await self._mark_checked(
            tracked,
            checked_at=now,
            last_price=last_price,
            precision_mode=precision_mode,
        )
        return []

    async def _apply_time_fallback_rows(
        self,
        tracked_rows: list[TrackedSignalState],
        *,
        now: datetime,
        precision_mode: str,
    ) -> list[SignalTrackingEvent]:
        events: list[SignalTrackingEvent] = []
        for tracked in tracked_rows:
            events.extend(
                await self._apply_time_fallback(
                    tracked,
                    now=now,
                    precision_mode=precision_mode,
                )
            )
        return events

    async def _apply_price_tick(
        self,
        tracked: TrackedSignalState,
        *,
        price: float,
        occurred_at: datetime,
        precision_mode: str,
    ) -> tuple[list[SignalTrackingEvent], bool]:
        events: list[SignalTrackingEvent] = []
        if tracked.direction == "long":
            if price <= tracked.stop:
                events.append(
                    await self._close_event(
                        tracked,
                        event_type="stop_loss",
                        occurred_at=occurred_at,
                        price=price,
                        precision_mode=precision_mode,
                    )
                )
                return events, True
            if tracked.single_target_mode and price >= tracked.take_profit_1:
                events.append(
                    await self._close_event(
                        tracked,
                        event_type="tp1_hit",
                        occurred_at=occurred_at,
                        price=price,
                        precision_mode=precision_mode,
                        note="single_target_close",
                    )
                )
                return events, True
            if price >= tracked.take_profit_2:
                if tracked.tp1_hit_at is None:
                    await self._mark_tp1(
                        tracked,
                        occurred_at=occurred_at,
                        price=tracked.take_profit_1,
                        precision_mode=precision_mode,
                        move_stop_to_break_even=self.settings.tracking.move_stop_to_break_even_on_tp1,
                    )
                    events.append(
                        SignalTrackingEvent(
                            event_type="tp1_hit",
                            tracked=tracked,
                            occurred_at=occurred_at,
                            event_price=tracked.take_profit_1,
                            precision_mode=precision_mode,
                            note="tp2_implies_tp1",
                        )
                    )
                events.append(
                    await self._close_event(
                        tracked,
                        event_type="tp2_hit",
                        occurred_at=occurred_at,
                        price=price,
                        precision_mode=precision_mode,
                    )
                )
                return events, True
            if tracked.tp1_hit_at is None and price >= tracked.take_profit_1:
                await self._mark_tp1(
                    tracked,
                    occurred_at=occurred_at,
                    price=price,
                    precision_mode=precision_mode,
                    move_stop_to_break_even=self.settings.tracking.move_stop_to_break_even_on_tp1,
                )
                events.append(
                    SignalTrackingEvent(
                        event_type="tp1_hit",
                        tracked=tracked,
                        occurred_at=occurred_at,
                        event_price=price,
                        precision_mode=precision_mode,
                    )
                )
                return events, False
            return [], False

        if price >= tracked.stop:
            events.append(
                await self._close_event(
                    tracked,
                    event_type="stop_loss",
                    occurred_at=occurred_at,
                    price=price,
                    precision_mode=precision_mode,
                )
            )
            return events, True
        if tracked.single_target_mode and price <= tracked.take_profit_1:
            events.append(
                await self._close_event(
                    tracked,
                    event_type="tp1_hit",
                    occurred_at=occurred_at,
                    price=price,
                    precision_mode=precision_mode,
                    note="single_target_close",
                )
            )
            return events, True
        if price <= tracked.take_profit_2:
            if tracked.tp1_hit_at is None:
                await self._mark_tp1(
                    tracked,
                    occurred_at=occurred_at,
                    price=tracked.take_profit_1,
                    precision_mode=precision_mode,
                    move_stop_to_break_even=self.settings.tracking.move_stop_to_break_even_on_tp1,
                )
                events.append(
                    SignalTrackingEvent(
                        event_type="tp1_hit",
                        tracked=tracked,
                        occurred_at=occurred_at,
                        event_price=tracked.take_profit_1,
                        precision_mode=precision_mode,
                        note="tp2_implies_tp1",
                    )
                )
            events.append(
                await self._close_event(
                    tracked,
                    event_type="tp2_hit",
                    occurred_at=occurred_at,
                    price=price,
                    precision_mode=precision_mode,
                )
            )
            return events, True
        if tracked.tp1_hit_at is None and price <= tracked.take_profit_1:
            await self._mark_tp1(
                tracked,
                occurred_at=occurred_at,
                price=price,
                precision_mode=precision_mode,
                move_stop_to_break_even=self.settings.tracking.move_stop_to_break_even_on_tp1,
            )
            events.append(
                SignalTrackingEvent(
                    event_type="tp1_hit",
                    tracked=tracked,
                    occurred_at=occurred_at,
                    event_price=price,
                    precision_mode=precision_mode,
                )
            )
            return events, False
        return [], False

    async def _close_event(
        self,
        tracked: TrackedSignalState,
        *,
        event_type: str,
        occurred_at: datetime,
        price: float | None,
        precision_mode: str,
        note: str | None = None,
    ) -> SignalTrackingEvent:
        await self._close_signal(
            tracked,
            reason=event_type,
            occurred_at=occurred_at,
            price=price,
            precision_mode=precision_mode,
        )

        # Update adaptive scoring window for the closed setup.
        setup_outcome = (
            event_type
            if event_type in {"tp1_hit", "tp2_hit", "stop_loss", "expired", "ambiguous_exit", "smart_exit", "superseded"}
            else "ambiguous_exit"
        )
        try:
            await self._record_setup_outcome(tracked.setup_id, setup_outcome)
        except (OSError, IOError, ValueError):
            LOG.debug("record_outcome failed for %s (non-critical)", tracked.setup_id)

        # Сохраняем outcome в SQLite.
        # Use async batching for better I/O efficiency.
        # Fire-and-forget to not block the aggTrade realtime hot-path.
        try:
            task = asyncio.create_task(
                self._queue_outcome_for_batch(tracked, event_type),
                name=f"outcome_batch:{tracked.tracking_id}"
            )
            task.add_done_callback(lambda t: None)
        except RuntimeError:
            self._save_outcome(tracked, event_type)

        return SignalTrackingEvent(
            event_type=event_type,
            tracked=tracked,
            occurred_at=occurred_at,
            event_price=price,
            precision_mode=precision_mode,
            note=note,
        )

    def _save_outcome(self, tracked: TrackedSignalState, event_type: str) -> None:
        """Сохраняет outcome завершенного сигнала."""
        features = self.features_store.get(tracked.tracking_id)
        if not features:
            # Создаем базовые признаки из tracked данных
            features = SignalFeatures(
                base_score=tracked.score,
                llm_verdict=None,
                risk_reward=tracked.risk_reward,
                stop_distance_pct=getattr(tracked, "stop_distance_pct", 0.0),
                entry_mid=tracked.entry_mid,
                setup_id=tracked.setup_id,
                direction=tracked.direction,
                timeframe=tracked.timeframe,
                bias_4h=tracked.bias_4h,
            )

        entry_price = tracked.activation_price or tracked.entry_mid
        max_profit_pct = 0.0
        max_loss_pct = 0.0
        if entry_price and entry_price > 0.0:
            direction_sign = 1.0 if tracked.direction == "long" else -1.0
            if tracked.tp2_price is not None:
                max_profit_pct = direction_sign * (tracked.tp2_price - entry_price) / entry_price * 100.0
            elif tracked.tp1_price is not None:
                max_profit_pct = direction_sign * (tracked.tp1_price - entry_price) / entry_price * 100.0
            if tracked.stop_price is not None:
                max_loss_pct = direction_sign * (tracked.stop_price - entry_price) / entry_price * 100.0

        outcome = create_outcome_from_tracked(
            tracked,
            features,
            max_profit_pct=max_profit_pct,
            max_loss_pct=max_loss_pct,
        )
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            asyncio.run(self.memory_repo.save_signal_outcome(outcome.to_dict()))
        else:
            task = loop.create_task(self.memory_repo.save_signal_outcome(outcome.to_dict()))
            task.add_done_callback(
                lambda done: LOG.debug("save_signal_outcome failed: %s", done.exception())
                if not done.cancelled() and done.exception() is not None
                else None
            )

        # Очищаем features store
        self.features_store.pop(tracked.tracking_id, None)
        self._persist_features_store()

    async def _queue_outcome_for_batch(self, tracked: TrackedSignalState, event_type: str) -> None:
        """Queue outcome for batched I/O."""
        features = self.features_store.get(tracked.tracking_id)
        if not features:
            features = SignalFeatures(
                base_score=tracked.score,
                llm_verdict=None,
                risk_reward=tracked.risk_reward,
                stop_distance_pct=getattr(tracked, "stop_distance_pct", 0.0),
                entry_mid=tracked.entry_mid,
                setup_id=tracked.setup_id,
                direction=tracked.direction,
                timeframe=tracked.timeframe,
                bias_4h=tracked.bias_4h,
            )

        entry_price = tracked.activation_price or tracked.entry_mid
        max_profit_pct = 0.0
        max_loss_pct = 0.0
        if entry_price and entry_price > 0.0:
            direction_sign = 1.0 if tracked.direction == "long" else -1.0
            if tracked.tp2_price is not None:
                max_profit_pct = direction_sign * (tracked.tp2_price - entry_price) / entry_price * 100.0
            elif tracked.tp1_price is not None:
                max_profit_pct = direction_sign * (tracked.tp1_price - entry_price) / entry_price * 100.0
            if tracked.stop_price is not None:
                max_loss_pct = direction_sign * (tracked.stop_price - entry_price) / entry_price * 100.0

        from .outcomes import create_outcome_from_tracked
        outcome = create_outcome_from_tracked(tracked, features, max_profit_pct, max_loss_pct)

        async with self._pending_outcomes_lock:
            self._pending_outcomes.append(outcome.to_dict())
            should_flush = len(self._pending_outcomes) >= self._pending_outcomes_flush_size

        if should_flush:
            await self._flush_pending_outcomes()

        # Clean up features store immediately (cheap operation)
        self.features_store.pop(tracked.tracking_id, None)
        self._persist_features_store()

    async def supersede_open_signal(
        self,
        new_signal: Signal,
        *,
        dry_run: bool = False,
    ) -> list[SignalTrackingEvent] | None:
        """Close existing open signal when a new better one supersedes it.

        Args:
            new_signal: The new signal that supersedes the old one
            dry_run: If True, don't actually modify state

        Returns:
            List of tracking events if a signal was closed, None otherwise
        """
        if dry_run or not self.settings.tracking.enabled:
            return None

        # Find existing open signals for this symbol
        existing_signals = [
            r for r in await self._active_signals(symbol=new_signal.symbol)
            if r.activated_at is None
        ]

        if not existing_signals:
            return None

        events: list[SignalTrackingEvent] = []
        now = datetime.now(UTC)

        for existing in existing_signals:
            # Close the existing signal as superseded
            event = await self._close_event(
                existing,
                event_type="superseded",
                occurred_at=now,
                price=None,
                precision_mode="theoretical",
                note=f"Superseded by {new_signal.setup_id} (score: {new_signal.score:.2f})",
            )
            events.append(event)

        if events:
            try:
                await self._persist_tracking_state()
            except (OSError, IOError):
                LOG.exception("tracking state persist failed for supersede (continuing)")

            for event in events:
                self.telemetry.append_jsonl(
                    "tracking_events.jsonl",
                    event.to_log_row(stats=await self._stats_snapshot()),
                )

        return events if events else None

    # ------------------------------------------------------------------
    # Event-driven additions (Фаза 1 рефакторинга)
    # ------------------------------------------------------------------

    async def review_open_signals_for_symbol(
        self,
        symbol: str,
        *,
        dry_run: bool,
    ) -> list[SignalTrackingEvent]:
        """Per-symbol tracking review — called on each 15m candle close.

        Extracts the single-symbol branch from review_open_signals() so that
        the event-driven engine can review each symbol independently without
        waiting for the full global cycle.
        """
        if dry_run or not self.settings.tracking.enabled:
            return []
        tracked_rows = await self._active_signals(symbol=symbol)
        if not tracked_rows:
            return []
        tracked_rows.sort(key=lambda item: item.created_at)
        now = datetime.now(UTC)
        try:
            events = await self._review_symbol(symbol, tracked_rows, now=now)
        except Exception:
            LOG.exception("tracking review failed for %s; using time fallback", symbol)
            events = await self._apply_time_fallback_rows(
                tracked_rows,
                now=now,
                precision_mode="time_fallback",
            )
        if tracked_rows:
            try:
                await self._persist_tracking_state()
            except (OSError, IOError):
                LOG.exception("tracking state persist failed for %s (continuing without persistence)", symbol)
        for event in events:
            self.telemetry.append_jsonl(
                "tracking_events.jsonl",
                event.to_log_row(stats=await self._stats_snapshot()),
            )
        return events

    async def on_agg_trade(
        self,
        symbol: str,
        price: float,
        trade_dt: datetime,
    ) -> list[SignalTrackingEvent]:
        """Real-time TP/SL fast-path — called on every aggTrade tick.

        Only processes ACTIVE signals (already entered).  No REST I/O —
        pure in-memory price comparison via the existing _apply_price_tick().
        PENDING signals are still handled by review_open_signals_for_symbol()
        at each 15m candle close.
        """
        if not self.settings.tracking.enabled:
            return []
        active_rows = [
            r for r in await self._active_signals(symbol=symbol)
            if r.activated_at is not None
        ]
        if not active_rows:
            return []

        events: list[SignalTrackingEvent] = []
        for tracked in active_rows:
            tick_events, closed = await self._apply_price_tick(
                tracked,
                price=price,
                occurred_at=trade_dt,
                precision_mode="trade_realtime",
            )
            events.extend(tick_events)
            if closed:
                break  # signal closed — stop processing for this symbol

        if events:
            try:
                await self._persist_tracking_state()
            except (OSError, IOError):
                LOG.exception("tracking state persist failed for realtime trade %s (continuing without persistence)", symbol)
            for event in events:
                self.telemetry.append_jsonl(
                    "tracking_events.jsonl",
                    event.to_log_row(stats=await self._stats_snapshot()),
                )
        return events


def _price_in_entry_zone(tracked: TrackedSignalState, price: float) -> bool:
    return tracked.entry_low <= price <= tracked.entry_high


def _bar_touches_entry(tracked: TrackedSignalState, *, high: float, low: float) -> bool:
    return low <= tracked.entry_high and high >= tracked.entry_low


def _bar_hits_tp1(tracked: TrackedSignalState, *, high: float, low: float) -> bool:
    if tracked.direction == "long":
        return high >= tracked.take_profit_1
    return low <= tracked.take_profit_1


def _bar_hits_tp2(tracked: TrackedSignalState, *, high: float, low: float) -> bool:
    if tracked.direction == "long":
        return high >= tracked.take_profit_2
    return low <= tracked.take_profit_2


def _bar_hits_stop(tracked: TrackedSignalState, *, high: float, low: float) -> bool:
    """Stop hit detection (immediate trigger price hit).
    
    In real Binance futures trading, stop-limit orders trigger when
    price reaches the stop price, not waiting for candle close.
    """
    if tracked.direction == "long":
        return low <= tracked.stop
    return high >= tracked.stop
