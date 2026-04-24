from __future__ import annotations

import asyncio
import hashlib
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from .config import AlertConfig, BotSettings
from .messaging import DeliveryResult, MessageBroadcaster
from .models import PreparedSymbol, Signal
from .telemetry import TelemetryStore
LOG = logging.getLogger("bot.alerts")


@dataclass(frozen=True, slots=True)
class WatchCandidate:
    symbol: str
    setup_id: str
    direction: str
    level_name: str
    level_price: float
    interest_low: float
    interest_high: float
    entry_low: float
    entry_high: float
    invalidate_below: float | None
    invalidate_above: float | None
    created_at: datetime
    ref_id: str
    context: dict[str, object] = field(default_factory=dict)

    @property
    def key(self) -> str:
        return f"{self.symbol}|{self.direction}|{self.level_name}"


@dataclass(slots=True)
class WatchState:
    candidate: WatchCandidate
    status: str = "armed"  # armed -> watch_sent -> entry_sent
    watch_message_id: int | None = None
    watch_sent_at: datetime | None = None
    entry_sent_at: datetime | None = None
    entry_message_id: int | None = None
    entry_price: float | None = None
    last_price: float | None = None


def _fmt_price(value: float | None) -> str:
    if value is None:
        return "?"
    if value >= 1000:
        return f"{value:,.0f}"
    if value >= 1:
        return f"{value:,.4f}".rstrip("0").rstrip(".")
    return f"{value:.8f}".rstrip("0").rstrip(".")


def _fmt_pct(value: float) -> str:
    return f"{value * 100:.2f}%"


def _make_ref_id(*, symbol: str, setup_id: str, level_name: str, direction: str, ts: datetime) -> str:
    setup_short = {
        "structure_pullback": "PULL",
        "structure_break_retest": "BREAK",
        "wick_trap_reversal": "WICK",
        "squeeze_setup": "SQZ",
        "ema_bounce": "EMA",
    }.get(setup_id, str(setup_id)[:4].upper())
    base_asset = symbol.replace("USDT", "").replace("BUSD", "")
    ts_utc = ts.astimezone(timezone.utc)
    time_part = ts_utc.strftime("%m%d-%H%M")
    raw = f"{symbol}|{setup_id}|{direction}|{level_name}|{ts_utc.isoformat()}"
    hash_part = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:4].upper()
    return f"{base_asset}-{setup_short}-{time_part}-{hash_part}"


class AlertCoordinator:
    """3-stage alert funnel:

    - Level 1 (watch): price enters interest zone around a key level
    - Level 2 (entry): price enters tighter entry zone (optional)
    - Cancel: price invalidates the watch before confirmation

    Confirmed signals are delivered via the existing delivery pipeline and
    should call `on_confirmed_signals()` to clear watches.
    """

    def __init__(
        self,
        *,
        settings: BotSettings,
        broadcaster: MessageBroadcaster,
        telemetry: TelemetryStore,
    ) -> None:
        self._cfg: AlertConfig = settings.alerts
        self._broadcaster = broadcaster
        self._telemetry = telemetry
        self._lock = asyncio.Lock()

        self._candidates_by_symbol: dict[str, WatchCandidate] = {}
        self._active_by_symbol: dict[str, WatchState] = {}
        self._last_armed_key_by_symbol: dict[str, str] = {}

        self._watch_sent_times: list[datetime] = []
        self._last_watch_by_symbol: dict[str, datetime] = {}
        self._last_entry_by_symbol: dict[str, datetime] = {}
        self._closed = False

    async def close(self) -> None:
        """Gracefully shut down the alert coordinator.

        Releases any held lock and logs final telemetry for active watches.
        """
        if self._closed:
            return
        self._closed = True
        async with self._lock:
            active_count = len(self._active_by_symbol)
            if active_count > 0:
                LOG.info("alert coordinator closing | %d active watch(s) will be dropped", active_count)
            self._candidates_by_symbol.clear()
            self._active_by_symbol.clear()
            self._last_armed_key_by_symbol.clear()

    async def refresh_candidates(self, prepared: PreparedSymbol, *, observed_at: datetime) -> None:
        if not self._cfg.enabled:
            return
        if self._closed:
            return
        # Guard against accidental dict passage (PreparedFeatureSnapshot mix-up).
        if isinstance(prepared, dict):
            LOG.warning("refresh_candidates received dict instead of PreparedSymbol, skipping | symbol=%s", prepared.get("symbol", "unknown"))
            return
        # Idempotent fast-path: if the same candidate key is already armed and active,
        # skip re-processing. The watch is already telemetry-tracked.
        existing_candidate = self._candidates_by_symbol.get(prepared.symbol)
        if existing_candidate is not None:
            active = self._active_by_symbol.get(prepared.symbol)
            if active is not None and active.status in {"watch_sent", "watch_sending", "entry_sent", "entry_sending"}:
                # Already an active watch — no need to re-arm.
                return
        # Keep it intentionally narrow: only "pullback to a structural level" watches for now.
        candidates = self._pullback_watch_candidates(prepared, observed_at=observed_at)
        async with self._lock:
            if not candidates:
                self._candidates_by_symbol.pop(prepared.symbol, None)
                self._active_by_symbol.pop(prepared.symbol, None)
                return
            # Pick the closest candidate (function returns sorted by distance).
            chosen = candidates[0]
            active = self._active_by_symbol.get(prepared.symbol)
            if active is not None and active.candidate.key == chosen.key:
                # Keep the active watch immutable: do not rotate REF-ID / timestamps under it.
                self._candidates_by_symbol[prepared.symbol] = active.candidate
                return
            if active is not None and active.candidate.key != chosen.key:
                self._active_by_symbol.pop(prepared.symbol, None)
            self._candidates_by_symbol[prepared.symbol] = chosen
            last_key = self._last_armed_key_by_symbol.get(prepared.symbol)
            if last_key != chosen.key:
                self._last_armed_key_by_symbol[prepared.symbol] = chosen.key
                ctx = chosen.context or {}
                self._telemetry.append_jsonl(
                    "alerts.jsonl",
                    {
                        "ts": observed_at.isoformat(),
                        "event": "watch_armed",
                        "symbol": chosen.symbol,
                        "setup_id": chosen.setup_id,
                        "direction": chosen.direction,
                        "watch_key": chosen.key,
                        "ref_id": chosen.ref_id,
                        "ref": chosen.ref_id,
                        "level_name": chosen.level_name,
                        "level_price": chosen.level_price,
                        "interest_low": chosen.interest_low,
                        "interest_high": chosen.interest_high,
                        "entry_zone": [chosen.entry_low, chosen.entry_high],
                        "invalidation": chosen.invalidate_below
                        if chosen.invalidate_below is not None
                        else chosen.invalidate_above,
                        "context": dict(ctx),
                        "bias_4h": ctx.get("bias_4h", "unknown"),
                        "regime_4h_confirmed": ctx.get("regime_4h_confirmed", "unknown"),
                        "market_regime": ctx.get("market_regime", "unknown"),
                    },
                )

    async def on_confirmed_signals(self, signals: list[Signal], *, observed_at: datetime) -> None:
        if not self._cfg.enabled:
            return
        async with self._lock:
            for signal in signals:
                state = self._active_by_symbol.pop(signal.symbol, None)
                if state is None:
                    continue
                early_entry_advantage_bps = None
                if state.entry_price is not None and signal.entry_mid:
                    try:
                        early_entry_advantage_bps = round(
                            (float(state.entry_price) - float(signal.entry_mid)) / float(signal.entry_mid) * 10_000.0,
                            1,
                        )
                    except (TypeError, ValueError, ZeroDivisionError):
                        early_entry_advantage_bps = None
                # Enrich with context from the original watch candidate.
                ctx = state.candidate.context or {}
                self._telemetry.append_jsonl(
                    "alerts.jsonl",
                    {
                        "ts": observed_at.isoformat(),
                        "event": "watch_cleared_on_confirmed",
                        "symbol": signal.symbol,
                        "setup_id": signal.setup_id,
                        "direction": signal.direction,
                        "watch_key": state.candidate.key,
                        "ref_id": state.candidate.ref_id,
                        "ref": state.candidate.ref_id,
                        "confirmed_price": signal.entry_mid,
                        "early_entry_price": state.entry_price,
                        "early_entry_advantage_bps": early_entry_advantage_bps,
                        "signal_score": signal.score,
                        "signal_bias_4h": signal.bias_4h,
                        "watch_bias_4h": ctx.get("bias_4h", "unknown"),
                        "watch_regime_4h": ctx.get("regime_4h_confirmed", "unknown"),
                    },
                )

    async def on_tick(self, symbol: str, price: float, *, observed_at: datetime, dry_run: bool) -> None:
        if not self._cfg.enabled:
            return
        action: dict[str, object] | None = None

        async with self._lock:
            candidate = self._candidates_by_symbol.get(symbol)
            if candidate is None:
                return
            state = self._active_by_symbol.get(symbol)

            if state is None:
                if not (candidate.interest_low <= price <= candidate.interest_high):
                    return
                if not self._can_send_watch(symbol, observed_at):
                    self._telemetry.append_jsonl(
                        "alerts.jsonl",
                        {
                            "ts": observed_at.isoformat(),
                            "event": "watch_suppressed",
                            "symbol": symbol,
                            "reason": "rate_limited",
                            "watch_key": candidate.key,
                            "price": price,
                        },
                    )
                    return
                self._record_watch_sent(symbol, observed_at)
                self._active_by_symbol[symbol] = WatchState(
                    candidate=candidate,
                    status="watch_sending",
                    watch_sent_at=observed_at,
                    last_price=price,
                )
                action = {"type": "watch", "candidate": candidate, "price": price}
            else:
                state.last_price = price
                if state.watch_sent_at is not None:
                    expiry = timedelta(minutes=self._cfg.watch_expiry_minutes)
                    if observed_at - state.watch_sent_at >= expiry:
                        self._active_by_symbol.pop(symbol, None)
                        self._telemetry.append_jsonl(
                            "alerts.jsonl",
                            {
                                "ts": observed_at.isoformat(),
                                "event": "watch_expired",
                                "symbol": symbol,
                                "watch_key": state.candidate.key,
                                "ref_id": state.candidate.ref_id,
                                "ref": state.candidate.ref_id,
                                "age_s": (observed_at - state.watch_sent_at).total_seconds(),
                            },
                        )
                        return
                invalid_action = self._build_invalidation_action(state, price, observed_at=observed_at)
                if invalid_action is not None:
                    action = invalid_action
                elif self._cfg.enable_entry_zone and state.status == "watch_sent":
                    if candidate.entry_low <= price <= candidate.entry_high and self._can_send_entry(symbol, observed_at):
                        state.status = "entry_sending"
                        state.entry_sent_at = observed_at
                        self._last_entry_by_symbol[symbol] = observed_at
                        action = {
                            "type": "entry",
                            "candidate": candidate,
                            "price": price,
                            "reply_to_message_id": state.watch_message_id,
                        }

        if action is None:
            return
        action_type = str(action.get("type"))
        cand = action.get("candidate")
        if not isinstance(cand, WatchCandidate):
            return
        raw_action_price = action.get("price")
        current_price = float(raw_action_price) if isinstance(raw_action_price, (int, float)) else price
        if action_type == "watch":
            ref_id = cand.ref_id
            latency_ms = int(max(0.0, (observed_at - cand.created_at).total_seconds() * 1000.0))
            text = self._format_watch(cand, current_price=current_price, now=observed_at, ref_id=ref_id)
            message_id = await self._send(text, dry_run=dry_run, reply_to_message_id=None)
            async with self._lock:
                st = self._active_by_symbol.get(symbol)
                if st and st.status == "watch_sending" and st.candidate.key == cand.key:
                    st.status = "watch_sent"
                    st.watch_message_id = message_id
            self._telemetry.append_jsonl(
                "alerts.jsonl",
                {
                    "ts": observed_at.isoformat(),
                    "event": "watch_sent",
                    "symbol": symbol,
                    "setup_id": cand.setup_id,
                    "watch_key": cand.key,
                    "ref_id": ref_id,
                    "ref": ref_id,
                    "direction": cand.direction,
                    "level_name": cand.level_name,
                    "level_price": cand.level_price,
                    "interest_low": cand.interest_low,
                    "interest_high": cand.interest_high,
                    "entry_zone": [cand.entry_low, cand.entry_high],
                    "invalidation": cand.invalidate_below if cand.invalidate_below is not None else cand.invalidate_above,
                    "context": dict(cand.context or {}),
                    "price": current_price,
                    "latency_ms": latency_ms,
                    "message_id": message_id,
                },
            )
            return
        if action_type == "entry":
            ref_id = cand.ref_id
            reply_to = action.get("reply_to_message_id")
            reply_to_message_id = int(reply_to) if isinstance(reply_to, int) else None
            latency_ms = int(max(0.0, (observed_at - cand.created_at).total_seconds() * 1000.0))
            text = self._format_entry_zone(cand, current_price=current_price, now=observed_at, ref_id=ref_id)
            message_id = await self._send(text, dry_run=dry_run, reply_to_message_id=reply_to_message_id)
            async with self._lock:
                st = self._active_by_symbol.get(symbol)
                if st and st.status == "entry_sending" and st.candidate.key == cand.key:
                    st.status = "entry_sent"
                    st.entry_sent_at = observed_at
                    st.entry_message_id = message_id
                    st.entry_price = current_price
            self._telemetry.append_jsonl(
                "alerts.jsonl",
                {
                    "ts": observed_at.isoformat(),
                    "event": "entry_zone_sent",
                    "symbol": symbol,
                    "setup_id": cand.setup_id,
                    "watch_key": cand.key,
                    "ref_id": ref_id,
                    "ref": ref_id,
                    "trigger_price": current_price,
                    "latency_ms": latency_ms,
                    "message_id": message_id,
                    "reply_to_message_id": reply_to_message_id,
                },
            )
            return
        if action_type == "invalidate":
            reason = str(action.get("reason") or "invalidated")
            reply_to = action.get("reply_to_message_id")
            reply_to_message_id = int(reply_to) if isinstance(reply_to, int) else None
            ref_id = str(action.get("ref_id") or "")
            latency_ms = int(max(0.0, (observed_at - cand.created_at).total_seconds() * 1000.0))
            text = self._format_invalidated(
                cand,
                current_price=current_price,
                now=observed_at,
                reason=reason,
                ref_id=ref_id,
            )
            message_id = await self._send(text, dry_run=dry_run, reply_to_message_id=reply_to_message_id)
            self._telemetry.append_jsonl(
                "alerts.jsonl",
                {
                    "ts": observed_at.isoformat(),
                    "event": "watch_invalidated_sent",
                    "symbol": cand.symbol,
                    "setup_id": cand.setup_id,
                    "watch_key": cand.key,
                    "ref_id": ref_id,
                    "ref": ref_id,
                    "reason": reason,
                    "breach_price": current_price,
                    "latency_ms": latency_ms,
                    "message_id": message_id,
                    "reply_to_message_id": reply_to_message_id,
                },
            )
            return

    def _prune_watch_sent_times(self, now: datetime) -> None:
        cutoff = now - timedelta(hours=1)
        if not self._watch_sent_times:
            return
        self._watch_sent_times = [ts for ts in self._watch_sent_times if ts >= cutoff]

    def _can_send_watch(self, symbol: str, now: datetime) -> bool:
        self._prune_watch_sent_times(now)
        if self._cfg.max_watch_alerts_per_hour > 0 and len(self._watch_sent_times) >= self._cfg.max_watch_alerts_per_hour:
            return False
        last = self._last_watch_by_symbol.get(symbol)
        if last is None:
            return True
        return now - last >= timedelta(minutes=self._cfg.watch_symbol_cooldown_minutes)

    def _record_watch_sent(self, symbol: str, now: datetime) -> None:
        self._watch_sent_times.append(now)
        self._last_watch_by_symbol[symbol] = now

    def _can_send_entry(self, symbol: str, now: datetime) -> bool:
        last = self._last_entry_by_symbol.get(symbol)
        if last is None:
            return True
        return now - last >= timedelta(minutes=self._cfg.entry_symbol_cooldown_minutes)

    def _build_invalidation_action(
        self,
        state: WatchState,
        price: float,
        *,
        observed_at: datetime,
    ) -> dict[str, object] | None:
        cand = state.candidate
        invalid = False
        reason = None
        if cand.invalidate_below is not None and price < cand.invalidate_below:
            invalid = True
            reason = f"price_below_{_fmt_price(cand.invalidate_below)}"
        if cand.invalidate_above is not None and price > cand.invalidate_above:
            invalid = True
            reason = f"price_above_{_fmt_price(cand.invalidate_above)}"
        if not invalid:
            return None
        if state.watch_sent_at is None:
            self._active_by_symbol.pop(cand.symbol, None)
            return None
        min_age = timedelta(minutes=self._cfg.invalidated_min_age_minutes)
        if observed_at - state.watch_sent_at < min_age:
            # Too noisy: silently drop the watch.
            self._active_by_symbol.pop(cand.symbol, None)
            self._telemetry.append_jsonl(
                "alerts.jsonl",
                {
                    "ts": observed_at.isoformat(),
                    "event": "watch_invalidated_suppressed",
                    "symbol": cand.symbol,
                    "watch_key": cand.key,
                    "ref_id": state.candidate.ref_id,
                    "ref": state.candidate.ref_id,
                    "reason": reason,
                    "price": price,
                    "age_s": (observed_at - state.watch_sent_at).total_seconds(),
                },
            )
            return None
        reply_to = state.entry_message_id or state.watch_message_id
        ref_id = state.candidate.ref_id
        self._active_by_symbol.pop(cand.symbol, None)
        return {
            "type": "invalidate",
            "candidate": cand,
            "price": price,
            "reason": reason or "invalidated",
            "reply_to_message_id": reply_to,
            "ref_id": ref_id,
        }

    async def _send(self, text: str, *, dry_run: bool, reply_to_message_id: int | None) -> int | None:
        if dry_run:
            LOG.info("dry-run alert message\n%s", text)
            return None
        result: DeliveryResult = await self._broadcaster.send_html(text, reply_to_message_id=reply_to_message_id)
        if result.status != "sent":
            LOG.info("alert delivery failed | status=%s reason=%s", result.status, result.reason)
        return result.message_id

    def _pullback_watch_candidates(self, prepared: PreparedSymbol, *, observed_at: datetime) -> list[WatchCandidate]:
        """Build watch candidates using only already-prepared frames.

        This must stay cheap: it runs on each 15m refresh per symbol and is used
        only to arm tick-based gating later.
        """
        direction = None
        if prepared.regime_4h_confirmed in {"uptrend", "downtrend"}:
            direction = "long" if prepared.regime_4h_confirmed == "uptrend" else "short"
        elif prepared.bias_4h in {"uptrend", "downtrend"}:
            direction = "long" if prepared.bias_4h == "uptrend" else "short"
        if direction is None:
            return []

        if prepared.work_1h.is_empty():
            return []
        ema20_1h = float(prepared.work_1h.item(-1, "ema20") or 0.0)
        levels: list[tuple[str, float]] = []
        if ema20_1h > 0:
            levels.append(("ema20_1h", ema20_1h))
        if prepared.poc_1h and float(prepared.poc_1h) > 0:
            levels.append(("poc_1h", float(prepared.poc_1h)))
        if not levels:
            return []

        mid = None
        if prepared.mark_price and prepared.mark_price > 0:
            mid = float(prepared.mark_price)
        elif prepared.bid_price and prepared.ask_price and prepared.bid_price > 0 and prepared.ask_price > 0:
            mid = (float(prepared.bid_price) + float(prepared.ask_price)) / 2.0
        elif prepared.universe.last_price and prepared.universe.last_price > 0:
            mid = float(prepared.universe.last_price)
        if not mid or mid <= 0:
            return []

        interest_pct = float(self._cfg.watch_interest_pct)
        entry_pct = float(self._cfg.entry_zone_pct)
        invalidate_buffer_pct = float(self._cfg.invalidate_buffer_pct)
        context: dict[str, object] = {
            "bias_4h": prepared.bias_4h,
            "regime_4h_confirmed": prepared.regime_4h_confirmed,
            "structure_1h": prepared.structure_1h,
            "market_regime": prepared.market_regime,
            "funding_rate": prepared.funding_rate,
            "spread_bps": prepared.spread_bps,
        }
        if not prepared.work_15m.is_empty():
            context["rsi_15m"] = float(prepared.work_15m.item(-1, "rsi14") or 0.0)
            context["volume_ratio_15m"] = float(prepared.work_15m.item(-1, "volume_ratio20") or 0.0)
        candidates: list[WatchCandidate] = []
        for name, level in levels:
            if level <= 0:
                continue
            interest_low = level * (1.0 - interest_pct)
            interest_high = level * (1.0 + interest_pct)
            entry_low = level * (1.0 - entry_pct)
            entry_high = level * (1.0 + entry_pct)
            invalidate_below = None
            invalidate_above = None
            if direction == "long":
                invalidate_below = interest_low * (1.0 - invalidate_buffer_pct)
            else:
                invalidate_above = interest_high * (1.0 + invalidate_buffer_pct)
            candidates.append(
                WatchCandidate(
                    symbol=prepared.symbol,
                    setup_id="structure_pullback",
                    direction=direction,
                    level_name=name,
                    level_price=level,
                    interest_low=min(interest_low, interest_high),
                    interest_high=max(interest_low, interest_high),
                    entry_low=min(entry_low, entry_high),
                    entry_high=max(entry_low, entry_high),
                    invalidate_below=invalidate_below,
                    invalidate_above=invalidate_above,
                    created_at=observed_at,
                    ref_id=_make_ref_id(
                        symbol=prepared.symbol,
                        setup_id="structure_pullback",
                        level_name=name,
                        direction=direction,
                        ts=observed_at,
                    ),
                    context=dict(context),
                )
            )
        candidates.sort(key=lambda item: abs(mid - item.level_price))
        return candidates

    def _format_watch(self, cand: WatchCandidate, *, current_price: float, now: datetime, ref_id: str) -> str:
        direction = "LONG" if cand.direction == "long" else "SHORT"
        return "\n".join(
            [
                f"🔍 <b>SETUP WATCH</b>: <b>{cand.symbol}</b> <b>{direction}</b>",
                "",
                f"REF: <code>{ref_id}</code>",
                f"Level: <b>{cand.level_name}</b> @ <b>{_fmt_price(cand.level_price)}</b>",
                f"Interest zone ({_fmt_pct(self._cfg.watch_interest_pct)}): <b>{_fmt_price(cand.interest_low)}</b> – <b>{_fmt_price(cand.interest_high)}</b>",
                f"Now: <b>{_fmt_price(current_price)}</b>",
                "",
                "Подтверждение (Level 3) — на close 15m, если паттерн сохранится.",
                f"Время: {now.isoformat()}",
            ]
        )

    def _format_entry_zone(self, cand: WatchCandidate, *, current_price: float, now: datetime, ref_id: str) -> str:
        direction = "LONG" if cand.direction == "long" else "SHORT"
        return "\n".join(
            [
                f"🟦 <b>ENTRY ZONE</b>: <b>{cand.symbol}</b> <b>{direction}</b>",
                "",
                f"REF: <code>{ref_id}</code>",
                f"Zone ({_fmt_pct(self._cfg.entry_zone_pct)}): <b>{_fmt_price(cand.entry_low)}</b> – <b>{_fmt_price(cand.entry_high)}</b>",
                f"Now: <b>{_fmt_price(current_price)}</b>",
                "",
                "Важно: свеча 15m ещё может не закрыться — это раннее предупреждение.",
                f"Время: {now.isoformat()}",
            ]
        )

    def _format_invalidated(
        self,
        cand: WatchCandidate,
        *,
        current_price: float,
        now: datetime,
        reason: str,
        ref_id: str,
    ) -> str:
        direction = "LONG" if cand.direction == "long" else "SHORT"
        return "\n".join(
            [
                f"⛔ <b>SETUP INVALIDATED</b>: <b>{cand.symbol}</b> <b>{direction}</b>",
                "",
                f"REF: <code>{ref_id}</code>",
                f"Reason: <code>{reason}</code>",
                f"Now: <b>{_fmt_price(current_price)}</b>",
                f"Время: {now.isoformat()}",
            ]
        )
