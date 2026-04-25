from __future__ import annotations

from dataclasses import dataclass
import html
import logging
from datetime import datetime, timedelta, timezone
from typing import Protocol
from collections import OrderedDict

from .messaging import DeliveryResult
from .models import Signal
from .tracking import SignalTrackingEvent


LOG = logging.getLogger("bot.delivery")
UTC = timezone.utc
LOCAL_TZ = datetime.now().astimezone().tzinfo or UTC


class SignalBroadcaster(Protocol):
    async def preflight_check(self) -> None: ...
    async def send_html(self, text: str, *, reply_to_message_id: int | None = None) -> DeliveryResult: ...
    async def edit_html(self, message_id: int, text: str) -> None: ...
    async def close(self) -> None: ...


@dataclass(frozen=True, slots=True)
class DeliveredSignal:
    signal: Signal
    status: str = "sent"
    message_id: int | None = None
    reason: str | None = None


def _fmt_price(value: float | None) -> str:
    if value is None:
        return "n/a"
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return "n/a"
    if numeric >= 1_000.0:
        return f"{numeric:,.2f}"
    if numeric >= 1.0:
        return f"{numeric:,.4f}"
    return f"{numeric:.6f}"


def _tz_label(value: datetime) -> str:
    offset = value.utcoffset() or timedelta(0)
    sign = "+" if offset >= timedelta(0) else "-"
    total_minutes = abs(int(offset.total_seconds() // 60))
    hours, minutes = divmod(total_minutes, 60)
    return f"UTC{sign}{hours:02d}:{minutes:02d}"


def _fmt_dt(raw: str | datetime | None) -> str:
    if raw is None:
        return "n/a"
    value = raw if isinstance(raw, datetime) else datetime.fromisoformat(str(raw))
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    value = value.astimezone(LOCAL_TZ)
    return f"{value.strftime('%Y-%m-%d %H:%M')} {_tz_label(value)}"


def _direction_label(direction: str) -> str:
    return "LONG" if direction == "long" else "SHORT"


def _humanize_token(value: str) -> str:
    return html.escape(str(value or "").replace("_", " ").strip())


def _trigger_text(reasons: tuple[str, ...] | list[str]) -> str:
    parts = [_humanize_token(reason) for reason in reasons if str(reason).strip()]
    return " -> ".join(parts) if parts else "n/a"


def _tradingview_interval(timeframe: str) -> str:
    raw = str(timeframe or "").strip().lower()
    mapping = {
        "1m": "1",
        "3m": "3",
        "5m": "5",
        "15m": "15",
        "30m": "30",
        "45m": "45",
        "1h": "60",
        "2h": "120",
        "4h": "240",
        "1d": "1D",
    }
    if raw in mapping:
        return mapping[raw]

    for token in ("15m", "1h", "5m", "30m", "4h"):
        if token in raw:
            return mapping[token]
    return "15"


def tradingview_chart_url(symbol: str, timeframe: str) -> str:
    tv_symbol = f"BINANCE:{symbol}.P"
    interval = _tradingview_interval(timeframe)
    return f"https://www.tradingview.com/chart/?symbol={tv_symbol}&interval={interval}"


def _status_line_for_tracked(tracked: SignalTrackingEvent | object) -> str:
    state = tracked.tracked if isinstance(tracked, SignalTrackingEvent) else tracked
    pending_expires_at = getattr(state, "pending_expires_at", None)
    activated_at = getattr(state, "activated_at", None)
    activation_price = getattr(state, "activation_price", None)
    tp1_hit_at = getattr(state, "tp1_hit_at", None)
    moved_to_break_even_at = getattr(state, "moved_to_break_even_at", None)
    close_reason = getattr(state, "close_reason", None)
    closed_at = getattr(state, "closed_at", None)
    close_price = getattr(state, "close_price", None)
    single_target_mode = bool(getattr(state, "single_target_mode", False))
    entry_mid = getattr(state, "entry_mid", None)
    activation_price = getattr(state, "activation_price", None)
    if close_reason == "tp1_hit" and single_target_mode:
        return f"closed at TP on <code>{_fmt_dt(closed_at)}</code> at <code>{_fmt_price(close_price or getattr(state, 'take_profit_1'))}</code>"
    if close_reason == "tp2_hit":
        return f"closed at TP2 on <code>{_fmt_dt(closed_at)}</code> at <code>{_fmt_price(close_price or getattr(state, 'take_profit_2'))}</code>"
    if close_reason == "stop_loss":
        # If stop was moved to break-even (TP1) and then hit, show it explicitly.
        break_even = activation_price or entry_mid
        stop_px = close_price or getattr(state, "stop")
        is_break_even = False
        if moved_to_break_even_at and break_even and stop_px:
            try:
                is_break_even = abs(float(stop_px) - float(break_even)) <= (float(break_even) * 1e-6)
            except (TypeError, ValueError):
                is_break_even = False
        label = "stopped (break-even)" if is_break_even else "stopped"
        return f"{label} on <code>{_fmt_dt(closed_at)}</code> at <code>{_fmt_price(stop_px)}</code>"
    if close_reason == "smart_exit":
        exit_px = close_price or getattr(state, "last_price") or entry_mid
        return (
            "analytical smart-exit on "
            f"<code>{_fmt_dt(closed_at)}</code> at <code>{_fmt_price(exit_px)}</code>"
        )
    if close_reason == "emergency_exit":
        exit_px = close_price or getattr(state, "last_price") or entry_mid
        return (
            "analytical hard-barrier exit on "
            f"<code>{_fmt_dt(closed_at)}</code> at <code>{_fmt_price(exit_px)}</code>"
        )
    if close_reason == "expired":
        return f"expired on <code>{_fmt_dt(closed_at)}</code>"
    if close_reason == "ambiguous_exit":
        return f"ambiguous exit on <code>{_fmt_dt(closed_at)}</code>"
    if tp1_hit_at:
        if single_target_mode:
            return f"TP hit on <code>{_fmt_dt(tp1_hit_at)}</code>"
        if moved_to_break_even_at:
            return f"TP1 hit on <code>{_fmt_dt(tp1_hit_at)}</code>; stop moved to break-even"
        return f"TP1 hit on <code>{_fmt_dt(tp1_hit_at)}</code>; TP2 still open"
    if activated_at:
        return f"active since <code>{_fmt_dt(activated_at)}</code> at <code>{_fmt_price(activation_price or getattr(state, 'entry_mid'))}</code>"
    return f"waiting entry until <code>{_fmt_dt(pending_expires_at)}</code>"


def _confidence_label(score: float) -> str:
    if score > 0.75:
        return "🔥 Сильный"
    if score >= 0.68:
        return "💪 Средний"
    return "💡 Умеренный"


def _trailing_stop_instructions(stop_distance_pct: float) -> str:
    """Trailing stop hint based on stop distance."""
    trigger1 = round(stop_distance_pct * 2, 1)
    trigger2 = round(stop_distance_pct * 3, 1)
    return f"📌 Трейлинг: +{trigger1}% → SL в безубыток | +{trigger2}% → фиксировать 30%"


def _compute_tp3(entry_mid: float, take_profit_2: float, direction: str) -> float:
    """TP3 = entry + 2 × (TP2 - entry) for longs; mirror for shorts."""
    dist = abs(take_profit_2 - entry_mid)
    if direction == "long":
        return entry_mid + 2.0 * dist
    return entry_mid - 2.0 * dist


def _render_signal_card(
    *,
    symbol: str,
    direction: str,
    tracking_ref: str,
    timeframe: str,
    setup_id: str,
    entry_low: float,
    entry_high: float,
    stop: float,
    take_profit_1: float,
    take_profit_2: float,
    risk_reward: float,
    stop_distance_pct: float,
    reasons: tuple[str, ...] | list[str],
    status_line: str,
    score: float = 0.0,
    oi_change_pct: float | None = None,
    funding_rate: float | None = None,
    btc_bias: str | None = None,
    expiry_dt: datetime | None = None,
) -> str:
    entry_mid = (entry_low + entry_high) / 2.0
    scale = max(abs(entry_mid), abs(take_profit_1), abs(take_profit_2), 1.0)
    single_target_mode = abs(take_profit_2 - take_profit_1) <= (scale * 1e-6)
    tp3 = _compute_tp3(entry_mid, take_profit_2, direction)

    lines: list[str] = []

    # BTC warning header (when BTC trend conflicts with signal direction)
    if btc_bias == "downtrend" and direction == "long":
        lines.append("⚠️ BTC В НИСХОДЯЩЕМ ТРЕНДЕ — повышенный риск")
    elif btc_bias == "uptrend" and direction == "short":
        lines.append("⚠️ BTC В ВОСХОДЯЩЕМ ТРЕНДЕ — повышенный риск")

    lines += [
        f"<b>{html.escape(symbol)} {_direction_label(direction)}</b> <code>#{tracking_ref}</code>",
        f"{_confidence_label(score)} | <code>{html.escape(timeframe)} {_humanize_token(setup_id)}</code>",
        f"RR <code>{risk_reward:.2f}</code> | Risk <code>{stop_distance_pct:.2f}%</code>",
        "",
        f"<b>Entry</b> <code>{_fmt_price(entry_low)} – {_fmt_price(entry_high)}</code>",
        (
            f"<b>SL</b> <code>{_fmt_price(stop)}</code> | "
            f"<b>TP</b> <code>{_fmt_price(take_profit_1)}</code>"
            if single_target_mode
            else (
                f"<b>SL</b> <code>{_fmt_price(stop)}</code> | "
                f"<b>TP1</b> <code>{_fmt_price(take_profit_1)}</code> | "
                f"<b>TP2</b> <code>{_fmt_price(take_profit_2)}</code> | "
                f"<b>TP3</b> <code>{_fmt_price(tp3)}</code>"
            )
        ),
        _trailing_stop_instructions(stop_distance_pct),
    ]
    ctx = _market_context_line(oi_change_pct, funding_rate)
    if ctx:
        lines.append(f"<b>Market</b> <code>{html.escape(ctx)}</code>")
    trigger_text = _trigger_text(reasons)
    if trigger_text != "n/a":
        lines.append(f"<b>Trigger</b> {html.escape(trigger_text)}")
    lines.append(f'<b>Chart</b> <a href="{html.escape(tradingview_chart_url(symbol, timeframe), quote=True)}">TradingView</a>')
    if expiry_dt:
        lines.append(f"👁 Ожидать входа: до <code>{_fmt_dt(expiry_dt)}</code>")
    else:
        lines.append(f"<b>Status</b> {status_line}")
    return "\n".join(lines)


def _market_context_line(oi_change_pct: float | None, funding_rate: float | None) -> str | None:
    parts = []
    if oi_change_pct is not None:
        sign = "+" if oi_change_pct >= 0 else ""
        parts.append(f"OI {sign}{oi_change_pct * 100:.1f}%")
    if funding_rate is not None:
        sign = "+" if funding_rate >= 0 else ""
        parts.append(f"FR {sign}{funding_rate * 100:.4f}%")
    return " | ".join(parts) if parts else None


def format_signal_text(signal: Signal, *, pending_expiry_minutes: int, btc_bias: str | None = None) -> str:
    wait_until = signal.created_at.astimezone(UTC) + timedelta(minutes=pending_expiry_minutes)
    return _render_signal_card(
        symbol=signal.symbol,
        direction=signal.direction,
        tracking_ref=signal.tracking_ref,
        timeframe=signal.timeframe,
        setup_id=signal.setup_id,
        entry_low=signal.entry_low,
        entry_high=signal.entry_high,
        stop=signal.stop,
        take_profit_1=signal.take_profit_1,
        take_profit_2=signal.take_profit_2,
        risk_reward=signal.risk_reward,
        stop_distance_pct=signal.stop_distance_pct,
        reasons=signal.reasons,
        status_line=f"👁 Ожидать входа: до <code>{_fmt_dt(wait_until)}</code>",
        score=signal.score,
        oi_change_pct=signal.oi_change_pct,
        funding_rate=signal.funding_rate,
        btc_bias=btc_bias,
        expiry_dt=wait_until,
    )


def format_analytics_companion(signal: Signal, *, btc_bias: str | None = None, eth_bias: str | None = None) -> str:
    """Build actionable context companion — answers WHY this signal, WHY now, WHAT invalidates."""
    sym = html.escape(signal.symbol)
    direction = signal.direction
    lines = [f"📋 <b>{sym}</b> — почему сейчас"]

    # --- Trigger chain: what conditions aligned ---
    if signal.reasons:
        # Humanise the raw reason tokens into readable Russian phrases
        _reason_map = {
            "regime_confirmed": "4h тренд подтверждён",
            "structure_aligned": "1h структура совпадает",
            "price_above_ema20": "цена выше EMA20",
            "price_below_ema20": "цена ниже EMA20",
            "pullback_to_level": "откат к уровню",
            "volume_confirmed": "объём подтверждает",
            "bounce_confirmed": "отскок подтверждён",
            "break_confirmed": "пробой подтверждён",
            "retest_confirmed": "ретест уровня",
            "wick_pierced": "свеча прокола уровня",
            "wick_closed_back": "тело вернулось выше уровня",
            "squeeze_released": "сжатие BB/KC прорвалось",
            "adx_ok": "ADX подтверждает тренд",
            "rsi_ok": "RSI в рабочей зоне",
            "supertrend_aligned": "SuperTrend совпадает",
            "ob_held": "ордер-блок устоял",
            "fvg_touched": "FVG зона тронута",
            "bos_confirmed": "BOS подтверждён",
            "choch_confirmed": "CHoCH подтверждён",
            "sweep_confirmed": "ликвидность сметена",
            "funding_extreme": "фандинг в экстремуме",
            "cvd_divergence": "CVD-дивергенция",
        }
        readable = [_reason_map.get(r, r) for r in signal.reasons[:5]]
        lines.append("• Триггер: " + " → ".join(readable))

    # --- Volume quality ---
    if signal.volume_ratio is not None and signal.volume_ratio > 0:
        if signal.volume_ratio >= 1.8:
            vol_verdict = f"{signal.volume_ratio:.1f}× — сильное подтверждение"
        elif signal.volume_ratio >= 1.2:
            vol_verdict = f"{signal.volume_ratio:.1f}× — умеренное подтверждение"
        else:
            vol_verdict = f"{signal.volume_ratio:.1f}× — слабый объём, осторожно"
        lines.append(f"• Объём: <code>{vol_verdict}</code>")

    # --- Taker pressure (orderflow) ---
    if signal.orderflow_delta_ratio is not None:
        dr = signal.orderflow_delta_ratio
        if direction == "long":
            of_verdict = "покупатели агрессивны" if dr >= 0.55 else ("нейтральный поток" if dr >= 0.45 else "продавцы давят — риск")
        else:
            of_verdict = "продавцы агрессивны" if dr <= 0.45 else ("нейтральный поток" if dr <= 0.55 else "покупатели давят — риск")
        lines.append(f"• Ордерфлоу: <code>{dr:.2f}</code> → {of_verdict}")

    # --- Crowd positioning contrast (funding rate as contrarian signal) ---
    if signal.funding_rate is not None:
        fr_pct = signal.funding_rate * 100
        if direction == "long" and fr_pct <= -0.04:
            lines.append(f"• Фандинг: <code>{fr_pct:+.4f}%</code> — толпа в шортах → попутный ветер для лонга")
        elif direction == "short" and fr_pct >= 0.04:
            lines.append(f"• Фандинг: <code>{fr_pct:+.4f}%</code> — толпа в лонгах → попутный ветер для шорта")
        elif abs(fr_pct) >= 0.04:
            lines.append(f"• Фандинг: <code>{fr_pct:+.4f}%</code> — перекос против направления, учитывай")

    # --- BTC as market tailwind/headwind (15m momentum context) ---
    btc_map = {"uptrend": "растёт ↑", "downtrend": "падает ↓", "neutral": "нейтральный"}
    if btc_bias and btc_bias != "neutral":
        btc_label = btc_map.get(btc_bias, btc_bias)
        if (direction == "long" and btc_bias == "uptrend") or (direction == "short" and btc_bias == "downtrend"):
            lines.append(f"• BTC: {html.escape(btc_label)} — попутный ветер")
        else:
            lines.append(f"• BTC: {html.escape(btc_label)} — встречный ветер, будь готов к волатильности")

    # --- Invalidation condition ---
    stop_pct = signal.stop_distance_pct
    setup_invalidation = {
        "structure_pullback": "закрытие ниже минимума отката",
        "structure_break_retest": "закрытие обратно под пробитый уровень",
        "wick_trap_reversal": "цена ниже экстремума фитиля",
        "squeeze_setup": "возврат внутрь зоны сжатия KC",
        "ema_bounce": "закрытие тела под EMA",
        "order_block": "закрытие за пределы зоны OB",
        "breaker_block": "цена пробивает уровень BRB повторно",
        "fvg_setup": "полное закрытие гэпа",
        "bos_choch": "ретест и пробой структурного пивота",
        "liquidity_sweep": "продолжение движения за уровень ликвидности",
        "turtle_soup": "новый экстремум за ложный пробой",
        "cvd_divergence": "новый ценовой экстремум без CVD подтверждения",
        "hidden_divergence": "разворот основного тренда",
        "funding_reversal": "фандинг не нормализуется за 2 свечи",
        "session_killzone": "выход за границы сессионного диапазона",
    }.get(signal.setup_id, "пробой стопа")
    lines.append(f"• Аннулирование: {setup_invalidation} (стоп {stop_pct:.1f}% от входа)")

    return "\n".join(lines)


def format_tracked_signal_text(tracked: SignalTrackingEvent | object) -> str:
    state = tracked.tracked if isinstance(tracked, SignalTrackingEvent) else tracked
    entry_mid = getattr(state, "entry_mid")
    stop = getattr(state, "stop")
    risk = abs(entry_mid - stop)
    reward = abs(getattr(state, "take_profit_2") - entry_mid)
    risk_reward = (reward / risk) if risk > 0 else 0.0
    stop_distance_pct = abs(entry_mid - stop) / entry_mid * 100.0 if entry_mid > 0 else 0.0
    return _render_signal_card(
        symbol=getattr(state, "symbol"),
        direction=getattr(state, "direction"),
        tracking_ref=getattr(state, "tracking_ref"),
        timeframe=getattr(state, "timeframe"),
        setup_id=getattr(state, "setup_id"),
        entry_low=getattr(state, "entry_low"),
        entry_high=getattr(state, "entry_high"),
        stop=stop,
        take_profit_1=getattr(state, "take_profit_1"),
        take_profit_2=getattr(state, "take_profit_2"),
        risk_reward=risk_reward,
        stop_distance_pct=stop_distance_pct,
        reasons=getattr(state, "reasons", ()),
        score=float(getattr(state, "score", 0.0) or 0.0),
        status_line=_status_line_for_tracked(state),
    )


def format_tracking_event_text(event: SignalTrackingEvent) -> str:
    tracked = event.tracked
    single_target_mode = bool(getattr(tracked, "single_target_mode", False))
    event_titles = {
        "activated": "Tracking Activated",
        "tp1_hit": "TP Hit" if single_target_mode else "TP1 Hit",
        "tp2_hit": "TP2 Hit",
        "stop_loss": "Stop Loss",
        "smart_exit": "Analytical Exit (Smart)",
        "emergency_exit": "Analytical Exit (Hard Barrier)",
        "expired": "Tracking Expired",
        "ambiguous_exit": "Analytical Exit (Ambiguous)",
        "superseded": "Tracking Superseded",
    }
    if event.event_type == "stop_loss" and getattr(tracked, "moved_to_break_even_at", None):
        try:
            break_even = float(getattr(tracked, "activation_price", None) or tracked.entry_mid)
            stop_px = float(event.event_price or tracked.stop)
            if break_even > 0 and abs(stop_px - break_even) <= (break_even * 1e-6):
                event_titles["stop_loss"] = "Stop (Break-even)"
        except (TypeError, ValueError):
            pass
    lines = [
        f"<b>{html.escape(tracked.symbol)} {_direction_label(tracked.direction)}</b> <code>#{tracked.tracking_ref}</code> | <b>{event_titles.get(event.event_type, event.event_type)}</b>",
        (
            f"<b>Time</b> <code>{_fmt_dt(event.occurred_at)}</code> | "
            f"<b>Price</b> <code>{_fmt_price(event.event_price or tracked.last_price or tracked.entry_mid)}</code>"
        ),
        (
            f"<b>Plan</b> <code>Entry {_fmt_price(tracked.entry_low)}-{_fmt_price(tracked.entry_high)} | "
            + (
                f"SL {_fmt_price(tracked.stop)} | TP {_fmt_price(tracked.take_profit_1)}"
                if single_target_mode
                else f"SL {_fmt_price(tracked.stop)} | TP1 {_fmt_price(tracked.take_profit_1)} | TP2 {_fmt_price(tracked.take_profit_2)}"
            )
            + "</code>"
        ),
        f'<b>Chart</b> <a href="{html.escape(tradingview_chart_url(tracked.symbol, tracked.timeframe), quote=True)}">TradingView</a>',
    ]
    if event.note:
        lines.append(f"<b>Note</b> <code>{html.escape(event.note)}</code>")
    return "\n".join(lines)


class SignalDelivery:
    def __init__(
        self,
        broadcaster: SignalBroadcaster,
        *,
        pending_expiry_minutes: int,
        tracking_reply_freshness_minutes: int = 120,
    ) -> None:
        self.broadcaster = broadcaster
        self.pending_expiry_minutes = pending_expiry_minutes
        self.tracking_reply_freshness_minutes = tracking_reply_freshness_minutes

    async def preflight_check(self) -> None:
        await self.broadcaster.preflight_check()

    async def deliver(
        self,
        signals: list[Signal],
        *,
        dry_run: bool,
        btc_bias: str | None = None,
    ) -> list[DeliveredSignal]:
        delivered: list[DeliveredSignal] = []
        for signal in signals:
            text = format_signal_text(
                signal,
                pending_expiry_minutes=self.pending_expiry_minutes,
                btc_bias=btc_bias,
            )
            if dry_run:
                LOG.info("dry-run signal\n%s", text)
                delivered.append(DeliveredSignal(signal=signal, status="sent", message_id=None, reason="dry_run"))
                continue
            result = await self.broadcaster.send_html(text)
            if result.status == "sent":
                LOG.info("telegram signal sent\n%s", text)
            else:
                LOG.debug(
                    "telegram signal not delivered | status=%s reason=%s symbol=%s setup=%s",
                    result.status,
                    result.reason,
                    signal.symbol,
                    signal.setup_id,
                )
            delivered.append(
                DeliveredSignal(
                    signal=signal,
                    status=result.status,
                    message_id=result.message_id,
                    reason=result.reason,
                )
            )
        return delivered

    async def send_analytics_companion(
        self,
        signal: Signal,
        *,
        btc_bias: str | None = None,
        eth_bias: str | None = None,
    ) -> None:
        """Send a short analytics narrative as a follow-up message after a signal."""
        try:
            text = format_analytics_companion(signal, btc_bias=btc_bias, eth_bias=eth_bias)
            await self.broadcaster.send_html(text)
        except Exception as exc:
            LOG.debug("analytics companion send failed: %s", exc)

    async def deliver_tracking_updates(
        self,
        events: list[SignalTrackingEvent],
        *,
        dry_run: bool,
    ) -> None:
        event_batches = self._coalesce_tracking_events(events)
        for batch in event_batches:
            final_event = batch[-1]
            tracked_card = format_tracked_signal_text(final_event.tracked)
            edited = False
            if len(batch) > 1:
                LOG.info(
                    "coalesced tracking batch | tracking_ref=%s events=%s final=%s",
                    final_event.tracked.tracking_ref,
                    [item.event_type for item in batch],
                    final_event.event_type,
                )
            if not dry_run and final_event.tracked.signal_message_id:
                try:
                    await self.broadcaster.edit_html(final_event.tracked.signal_message_id, tracked_card)
                    LOG.info("telegram signal card edited\n%s", tracked_card)
                    edited = True
                except Exception:
                    LOG.exception("telegram signal card edit failed for %s", final_event.tracked.tracking_ref)
            elif dry_run:
                LOG.info("dry-run signal card edit\n%s", tracked_card)
                edited = True
            if not self._should_send_tracking_follow_up(final_event):
                continue
            if final_event.event_type == "activated" and edited:
                continue
            text = format_tracking_event_text(final_event)
            if dry_run:
                LOG.info("dry-run tracking update\n%s", text)
                continue
            reply_to_message_id = final_event.tracked.signal_message_id if final_event.tracked.signal_message_id else None
            result = await self.broadcaster.send_html(text, reply_to_message_id=reply_to_message_id)
            if result.status == "sent":
                LOG.info("telegram tracking update sent\n%s", text)
            else:
                LOG.debug(
                    "telegram tracking update not delivered | status=%s reason=%s tracking_ref=%s event=%s",
                    result.status,
                    result.reason,
                    final_event.tracked.tracking_ref,
                    final_event.event_type,
                )

    @staticmethod
    def _coalesce_tracking_events(events: list[SignalTrackingEvent]) -> list[list[SignalTrackingEvent]]:
        grouped: "OrderedDict[str, list[SignalTrackingEvent]]" = OrderedDict()
        for event in events:
            grouped.setdefault(event.tracked.tracking_id, []).append(event)
        return list(grouped.values())

    def _should_send_tracking_follow_up(self, event: SignalTrackingEvent) -> bool:
        if event.event_type == "activated":
            return False
        occurred_at = event.occurred_at.astimezone(UTC)
        max_age = timedelta(minutes=self.tracking_reply_freshness_minutes)
        if datetime.now(UTC) - occurred_at > max_age:
            LOG.info(
                "suppressing stale tracking follow-up | tracking_ref=%s event=%s age_minutes=%.1f",
                event.tracked.tracking_ref,
                event.event_type,
                (datetime.now(UTC) - occurred_at).total_seconds() / 60.0,
            )
            return False
        return True
