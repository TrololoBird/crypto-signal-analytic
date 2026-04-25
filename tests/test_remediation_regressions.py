from __future__ import annotations

import asyncio
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import polars as pl
import pytest

from bot.application.bot import SignalBot
from bot.cli import _is_preformatted_log_stderr
from bot.config import load_settings
from bot.core.engine import SignalEngine, StrategyRegistry
from bot.core.engine.base import StrategyDecision
from bot.core.events import BookTickerEvent
from bot.market_data import BinanceFuturesMarketData, MarketDataUnavailable
from bot.models import PipelineResult, PreparedSymbol, Signal, UniverseSymbol
from bot.setup_base import BaseSetup, SetupParams
from bot.strategies.ema_bounce import EmaBounceSetup
from bot.strategies.fvg import FVGSetup
from bot.strategies.structure_pullback import StructurePullbackSetup
from bot.setups import _build_signal
from bot.setups.utils import build_structural_targets
from bot.tracked_signals import TrackedSignalState
from bot.tracking import SignalTracker


class TelemetryStub:
    def __init__(self) -> None:
        self.rows: list[tuple[str, dict]] = []
        self.symbol_rows: list[tuple[str, str, dict]] = []

    def append_jsonl(self, filename: str, row: dict) -> None:
        self.rows.append((filename, row))

    def append_symbol_jsonl(self, bucket: str, symbol: str, relative_name: str, row: dict) -> None:
        self.symbol_rows.append((symbol, relative_name, row))


class DummyMemoryRepo:
    def __init__(self) -> None:
        self.active_rows: list[dict] = []
        self.saved_rows: list[dict] = []
        self.saved_outcomes: list[dict] = []
        self.setup_outcomes: list[tuple[str, str]] = []
        self.tracking_stats: dict[str, int] = {
            "signals_sent": 0,
            "activated": 0,
            "tp1_hit": 0,
            "tp2_hit": 0,
            "stop_loss": 0,
            "expired": 0,
            "ambiguous_exit": 0,
            "active": 0,
        }

    async def get_active_signals(
        self,
        symbol: str | None = None,
        status: str | None = None,
        include_closed: bool = False,
    ) -> list[dict]:
        rows = list(self.active_rows)
        if symbol is not None:
            rows = [row for row in rows if row.get("symbol") == symbol]
        if status is not None:
            rows = [row for row in rows if row.get("status") == status]
        if not include_closed and status is None:
            rows = [row for row in rows if row.get("status") in {"pending", "active"}]
        return [dict(row) for row in rows]

    async def save_active_signal(self, signal_data: dict) -> None:
        payload = dict(signal_data)
        self.saved_rows.append(payload)
        tracking_id = str(payload["tracking_id"])
        self.active_rows = [
            row for row in self.active_rows if row.get("tracking_id") != tracking_id
        ]
        self.active_rows.append(payload)

    async def increment_tracking_stats(self, **deltas: int) -> None:
        for key, value in deltas.items():
            self.tracking_stats[key] = self.tracking_stats.get(key, 0) + int(value)

    async def get_tracking_stats(self) -> dict[str, int]:
        return dict(self.tracking_stats)

    async def record_setup_outcome(self, setup_id: str, outcome: str) -> float:
        self.setup_outcomes.append((setup_id, outcome))
        return 0.0

    async def save_signal_outcomes_batch(self, outcomes_data: list[dict]) -> None:
        self.saved_outcomes.extend(outcomes_data)


def make_universe_symbol(symbol: str = "BTCUSDT", price: float = 100.0) -> UniverseSymbol:
    return UniverseSymbol(
        symbol=symbol,
        base_asset="BTC",
        quote_asset="USDT",
        contract_type="PERPETUAL",
        status="TRADING",
        onboard_date_ms=0,
        quote_volume=1_000_000.0,
        price_change_pct=1.0,
        last_price=price,
        shortlist_bucket="test",
    )


def make_indicator_frame(price: float = 100.0) -> pl.DataFrame:
    now = datetime.now(UTC)
    return pl.DataFrame(
        {
            "time": [now],
            "close_time": [now],
            "open": [price - 1.0],
            "high": [price + 1.0],
            "low": [price - 2.0],
            "close": [price],
            "volume": [1_000.0],
            "atr14": [2.0],
            "atr_pct": [2.0],
            "rsi14": [55.0],
            "adx14": [25.0],
            "volume_ratio20": [1.4],
            "macd_hist": [0.25],
            "ema20": [price - 1.0],
            "ema50": [price - 2.0],
            "ema200": [price - 5.0],
            "supertrend_dir": [1.0],
            "obv_above_ema": [1.0],
            "bb_pct_b": [0.62],
            "bb_width": [4.2],
        }
    )


def make_prepared(symbol: str = "BTCUSDT", price: float = 100.0) -> PreparedSymbol:
    return PreparedSymbol(
        universe=make_universe_symbol(symbol=symbol, price=price),
        work_1h=make_indicator_frame(price),
        work_15m=make_indicator_frame(price),
        work_4h=make_indicator_frame(price + 5.0),
        bid_price=price - 0.1,
        ask_price=price + 0.1,
        spread_bps=2.0,
        funding_rate=0.0008,
        oi_current=1_250_000.0,
        oi_change_pct=3.2,
        ls_ratio=1.18,
        liquidation_score=0.35,
        market_regime="trending",
    )


def make_runtime_settings(*, strict_data_quality: bool = True, throttle_seconds: float = 0.0):
    return SimpleNamespace(
        runtime=SimpleNamespace(
            strict_data_quality=strict_data_quality,
            diagnostic_trace_limit_per_symbol=20,
            max_signals_per_cycle=3,
        ),
        ws=SimpleNamespace(rest_timeout_seconds=0.1, intra_candle_throttle_seconds=throttle_seconds),
        intelligence=SimpleNamespace(
            max_consecutive_stop_losses=3,
            stop_loss_pause_hours=0,
            runtime_mode="signal_only",
        ),
        filters=SimpleNamespace(cooldown_minutes=30),
    )


def make_signal(symbol: str = "BTCUSDT", created_at: datetime | None = None) -> Signal:
    return Signal(
        symbol=symbol,
        setup_id="ema_bounce",
        direction="long",
        score=0.82,
        timeframe="15m",
        entry_low=99.5,
        entry_high=100.5,
        stop=97.5,
        take_profit_1=103.0,
        take_profit_2=105.0,
        created_at=created_at or datetime.now(UTC),
    )


def make_tracked_state(
    *,
    tracking_id: str = "BTCUSDT|ema_bounce|long|20260423T000000000000Z",
    direction: str = "long",
    status: str = "pending",
    created_at: datetime | None = None,
    pending_expires_at: datetime | None = None,
    active_expires_at: datetime | None = None,
    activated_at: datetime | None = None,
    stop: float = 97.5,
    tp1: float = 103.0,
    tp2: float = 105.0,
    single_target_mode: bool = False,
    target_integrity_status: str = "valid",
) -> TrackedSignalState:
    created = created_at or datetime.now(UTC)
    return TrackedSignalState(
        tracking_id=tracking_id,
        tracking_ref="ABC12345",
        signal_key="BTCUSDT|ema_bounce|long",
        symbol="BTCUSDT",
        setup_id="ema_bounce",
        direction=direction,
        timeframe="15m",
        created_at=created.isoformat(),
        pending_expires_at=(pending_expires_at or (created + timedelta(minutes=10))).isoformat(),
        active_expires_at=(active_expires_at or (created + timedelta(minutes=60))).isoformat(),
        entry_low=99.5,
        entry_high=100.5,
        entry_mid=100.0,
        initial_stop=stop,
        stop=stop,
        stop_price=stop,
        take_profit_1=tp1,
        tp1_price=tp1,
        take_profit_2=tp2,
        tp2_price=tp2,
        single_target_mode=single_target_mode,
        target_integrity_status=target_integrity_status,
        score=0.82,
        risk_reward=2.0,
        reasons=("test",),
        signal_message_id=321,
        bias_4h="uptrend",
        quote_volume=1_000_000.0,
        spread_bps=2.0,
        atr_pct=2.0,
        orderflow_delta_ratio=0.2,
        status=status,
        activated_at=activated_at.isoformat() if activated_at is not None else None,
    )


def make_tracker(market_data: object) -> tuple[SignalTracker, DummyMemoryRepo, TelemetryStub]:
    repo = DummyMemoryRepo()
    telemetry = TelemetryStub()
    settings = SimpleNamespace(
        tracking=SimpleNamespace(
            enabled=True,
            pending_expiry_minutes=10,
            active_expiry_minutes=60,
            agg_trade_page_limit=4,
            agg_trade_page_size=100,
            move_stop_to_break_even_on_tp1=False,
        ),
        ws=SimpleNamespace(rest_timeout_seconds=0.1),
        features_store_file=None,
    )
    tracker = SignalTracker(
        settings,
        market_data=market_data,
        telemetry=telemetry,
        memory_repo=repo,
    )
    tracker._queue_outcome_for_batch = AsyncMock(return_value=None)
    return tracker, repo, telemetry


@pytest.mark.asyncio
async def test_agg_trade_rest_paths_accept_dict_rows() -> None:
    market_data = BinanceFuturesMarketData.__new__(BinanceFuturesMarketData)
    market_data.client = SimpleNamespace(futures_aggregate_trades=object())

    async def fake_call_rest(*args, **kwargs):
        return [
            {"a": 11, "p": "100.5", "q": "2.0", "T": 1_000, "m": False},
            {"a": 12, "p": "100.0", "q": "1.0", "T": 1_050, "m": True},
        ]

    market_data._call_rest = fake_call_rest

    snapshot = await market_data._fetch_agg_trade_snapshot_rest("BTCUSDT", limit=2)
    trades, complete = await market_data.fetch_agg_trades(
        "BTCUSDT",
        start_time_ms=900,
        end_time_ms=1_100,
        page_limit=3,
        page_size=100,
    )

    assert snapshot.trade_count == 2
    assert snapshot.buy_qty == pytest.approx(2.0)
    assert snapshot.sell_qty == pytest.approx(1.0)
    assert [trade.trade_id for trade in trades] == [11, 12]
    assert complete is True


@pytest.mark.asyncio
async def test_tracking_expiry_falls_back_to_time_only_when_market_data_unavailable() -> None:
    class MarketDataStub:
        async def fetch_agg_trades(self, *args, **kwargs):
            raise MarketDataUnavailable(operation="agg", detail="offline", symbol="BTCUSDT")

        async def fetch_klines(self, *args, **kwargs):
            raise MarketDataUnavailable(operation="klines", detail="offline", symbol="BTCUSDT")

    tracker, repo, _ = make_tracker(MarketDataStub())
    now = datetime.now(UTC)
    tracked = make_tracked_state(
        created_at=now - timedelta(minutes=30),
        pending_expires_at=now - timedelta(minutes=5),
        active_expires_at=now + timedelta(minutes=30),
    )
    repo.active_rows = [tracker._tracked_to_payload(tracked)]

    events = await tracker.review_open_signals_for_symbol("BTCUSDT", dry_run=False)
    await asyncio.sleep(0)

    assert [event.event_type for event in events] == ["expired"]
    assert repo.active_rows[0]["status"] == "closed"
    assert repo.active_rows[0]["close_reason"] == "expired"


@pytest.mark.asyncio
async def test_short_price_tick_can_hit_tp2() -> None:
    tracker, _, _ = make_tracker(SimpleNamespace())
    tracked = make_tracked_state(
        direction="short",
        status="active",
        activated_at=datetime.now(UTC) - timedelta(minutes=1),
        stop=105.0,
        tp1=95.0,
        tp2=90.0,
    )

    events, closed = await tracker._apply_price_tick(
        tracked,
        price=89.0,
        occurred_at=datetime.now(UTC),
        precision_mode="trade",
    )
    await asyncio.sleep(0)

    assert closed is True
    assert [event.event_type for event in events] == ["tp1_hit", "tp2_hit"]
    assert tracked.close_reason == "tp2_hit"


@pytest.mark.asyncio
async def test_smart_exit_keeps_distinct_adaptive_outcome() -> None:
    tracker, repo, _ = make_tracker(SimpleNamespace())
    tracked = make_tracked_state(
        status="active",
        activated_at=datetime.now(UTC) - timedelta(minutes=2),
    )

    await tracker._close_event(
        tracked,
        event_type="smart_exit",
        occurred_at=datetime.now(UTC),
        price=101.0,
        precision_mode="system",
    )
    await asyncio.sleep(0)

    assert repo.setup_outcomes[-1] == ("ema_bounce", "smart_exit")


@pytest.mark.asyncio
async def test_select_and_deliver_uses_tracking_id_for_message_binding_and_feature_snapshot(
) -> None:
    signal = make_signal(created_at=datetime(2026, 4, 23, 0, 0, tzinfo=UTC))
    prepared = make_prepared(symbol=signal.symbol)
    feature_calls: list[tuple[str, object]] = []

    async def wait_noncritical(*, label: str, timeout: float, operation):
        return True, await operation

    tracker = SimpleNamespace(
        set_signal_features=lambda tracking_id, features: feature_calls.append(
            (tracking_id, features)
        ),
        arm_signals_with_messages=AsyncMock(return_value=None),
    )
    delivery_result = SimpleNamespace(signal=signal, status="sent", message_id=777, reason=None)
    bot = SignalBot.__new__(SignalBot)
    bot.settings = make_runtime_settings()
    bot._modern_repo = SimpleNamespace(
        is_symbol_blacklisted=AsyncMock(return_value=False),
        get_consecutive_sl=AsyncMock(return_value=0),
        get_active_signals=AsyncMock(return_value=[]),
        is_cooldown_active=AsyncMock(return_value=False),
        set_cooldown=AsyncMock(return_value=None),
        get_market_context=AsyncMock(return_value={}),
    )
    bot._wait_noncritical = wait_noncritical
    bot.delivery = SimpleNamespace(
        deliver=AsyncMock(return_value=[delivery_result]),
        send_analytics_companion=AsyncMock(return_value=None),
    )
    bot.tracker = tracker
    bot.telemetry = TelemetryStub()
    bot.alerts = SimpleNamespace(on_confirmed_signals=AsyncMock(return_value=None))
    bot._sync_ws_tracked_symbols = AsyncMock(return_value=None)
    bot._close_superseded_signal = AsyncMock(return_value=None)
    bot._deliver_tracking = AsyncMock(return_value=None)

    delivered, rejected, counts = await bot._select_and_deliver(
        [signal],
        prepared_by_tracking_id={signal.tracking_id: prepared},
    )

    assert delivered == [signal]
    assert rejected == []
    assert counts["sent"] == 1
    assert feature_calls
    assert feature_calls[0][0] == signal.tracking_id
    assert feature_calls[0][1].rsi_15m == pytest.approx(55.0)
    tracker.arm_signals_with_messages.assert_awaited_once()
    assert tracker.arm_signals_with_messages.await_args.kwargs["message_ids"] == {
        signal.tracking_id: 777
    }


@pytest.mark.asyncio
async def test_select_and_deliver_for_symbol_does_not_double_write_reject_telemetry() -> None:
    signal = make_signal(created_at=datetime(2026, 4, 23, 0, 1, tzinfo=UTC))
    bot = SignalBot.__new__(SignalBot)
    bot.settings = make_runtime_settings()
    bot.telemetry = TelemetryStub()
    bot._select_and_rank = Mock(return_value=[signal])
    bot._select_and_deliver = AsyncMock(
        return_value=([], [{"reason": "symbol_has_open_signal"}], Counter())
    )

    result = PipelineResult(
        symbol="BTCUSDT",
        trigger="kline_close",
        event_ts=datetime.now(UTC),
        raw_setups=1,
        candidates=[signal],
        rejected=[],
        prepared=None,
        funnel={},
    )

    candidates, rejected, delivered = await bot._select_and_deliver_for_symbol("BTCUSDT", result)

    assert candidates == [signal]
    assert delivered == []
    assert rejected == [{"reason": "symbol_has_open_signal"}]
    assert bot.telemetry.rows == []


@pytest.mark.asyncio
async def test_intra_candle_path_emits_cycle_log() -> None:
    signal = make_signal(created_at=datetime(2026, 4, 23, 0, 2, tzinfo=UTC))
    bot = SignalBot.__new__(SignalBot)
    bot.settings = make_runtime_settings()
    bot._last_intra_scan = {}
    bot._background_tasks = set()
    bot._shortlist_lock = asyncio.Lock()
    bot._shortlist = [make_universe_symbol()]
    bot._analysis_semaphore = asyncio.Semaphore(1)
    bot._fetch_frames = AsyncMock(return_value=SimpleNamespace())
    bot._ws_cache_enrichments = lambda symbol: {}
    bot._run_modern_analysis = AsyncMock(
        return_value=PipelineResult(
            symbol="BTCUSDT",
            trigger="intra_candle",
            event_ts=datetime.now(UTC),
            raw_setups=1,
            candidates=[signal],
            rejected=[],
            prepared=None,
            funnel={},
        )
    )
    bot._ws_enrich = AsyncMock(return_value=None)
    bot._select_and_deliver_for_symbol = AsyncMock(return_value=([signal], [], [signal]))
    bot._emit_cycle_log = Mock()
    bot.telemetry = TelemetryStub()

    await bot._on_book_ticker(BookTickerEvent(symbol="BTCUSDT", bid=None, ask=None))

    for _ in range(3):
        tasks = list(bot._background_tasks)
        if not tasks:
            break
        await asyncio.gather(*tasks)

    assert bot._emit_cycle_log.call_count == 1
    kwargs = bot._emit_cycle_log.call_args.kwargs
    assert kwargs["interval"] == "bookTicker"
    assert kwargs["result"].trigger == "intra_candle"
    assert kwargs["delivered"] == [signal]


def test_build_structural_targets_prefers_nearest_long_resistance() -> None:
    work_1h = pl.DataFrame(
        {
            "time": [
                datetime(2026, 4, 23, 0, 0, tzinfo=UTC),
                datetime(2026, 4, 23, 1, 0, tzinfo=UTC),
                datetime(2026, 4, 23, 2, 0, tzinfo=UTC),
            ],
            "high": [180.0, 120.0, 150.0],
            "low": [90.0, 95.0, 98.0],
        }
    )
    sh_mask = pl.Series("sh", [True, True, True], dtype=pl.Boolean)

    stop, tp1, tp2 = build_structural_targets(
        direction="long",
        price_anchor=100.0,
        stop_basis=95.0,
        atr=2.0,
        work_1h=work_1h,
        sh_mask=sh_mask,
    )

    assert stop == pytest.approx(94.6)
    assert tp1 == pytest.approx(120.0)
    assert tp2 == pytest.approx(120.0)


def test_build_pinned_shortlist_resolves_assets_from_meta_and_safe_quote_parsing(caplog: pytest.LogCaptureFixture) -> None:
    bot = SignalBot.__new__(SignalBot)
    bot.settings = SimpleNamespace(
        universe=SimpleNamespace(
            pinned_symbols=["BTCUSDT", "ETHUSDT", "1000PEPEUSDT", "DOGEXUSD", "BROKEN"],
            quote_asset="XUSD",
        )
    )
    bot.client = SimpleNamespace(_exchange_info_cache=None)
    bot._symbol_meta_by_symbol = {
        "BTCUSDT": SimpleNamespace(base_asset="BTC", quote_asset="USDT"),
        "ETHUSDT": SimpleNamespace(base_asset="ETH", quote_asset="USDT"),
        "1000PEPEUSDT": SimpleNamespace(base_asset="1000PEPE", quote_asset="USDT"),
    }

    with caplog.at_level("WARNING", logger="bot.application.bot"):
        shortlist = bot._build_pinned_shortlist()

    by_symbol = {row.symbol: row for row in shortlist}
    assert by_symbol["BTCUSDT"].base_asset == "BTC"
    assert by_symbol["BTCUSDT"].quote_asset == "USDT"
    assert by_symbol["ETHUSDT"].base_asset == "ETH"
    assert by_symbol["1000PEPEUSDT"].base_asset == "1000PEPE"
    assert by_symbol["DOGEXUSD"].base_asset == "DOGE"
    assert by_symbol["DOGEXUSD"].quote_asset == "XUSD"
    assert "BROKEN" not in by_symbol
    assert any("unresolved base/quote assets" in rec.message and "BROKEN" in rec.message for rec in caplog.records)


def test_build_signal_normalizes_swapped_targets() -> None:
    prepared = make_prepared(price=100.0)

    signal = _build_signal(
        prepared=prepared,
        setup_id="breaker_block",
        direction="long",
        score=0.77,
        reasons=["test"],
        stop=95.0,
        tp1=112.0,
        tp2=108.0,
        price_anchor=100.0,
        atr=2.0,
    )

    assert signal is not None
    assert signal.take_profit_1 == pytest.approx(108.0)
    assert signal.take_profit_2 == pytest.approx(112.0)
    assert signal.target_integrity_status == "normalized"
    assert signal.single_target_mode is False


def test_build_signal_reads_adx14_and_preserves_zero_metrics() -> None:
    prepared = make_prepared(price=100.0)
    prepared.work_1h = prepared.work_1h.with_columns(pl.lit(0.0).alias("adx14"))
    prepared.work_15m = prepared.work_15m.with_columns(pl.lit(0.0).alias("volume_ratio20"))
    prepared.work_5m = pl.DataFrame(
        {
            "time": [datetime.now(UTC)],
            "close_time": [datetime.now(UTC)],
            "premium_zscore_5m": [0.0],
            "premium_slope_5m": [0.0],
            "ls_ratio": [0.0],
        }
    )

    signal = _build_signal(
        prepared=prepared,
        setup_id="breaker_block",
        direction="long",
        score=0.7,
        reasons=["test"],
        stop=95.0,
        tp1=108.0,
        tp2=112.0,
        price_anchor=100.0,
        atr=2.0,
    )

    assert signal is not None
    assert signal.adx_1h == pytest.approx(0.0)
    assert signal.volume_ratio == pytest.approx(0.0)
    assert signal.premium_zscore_5m == pytest.approx(0.0)
    assert signal.premium_slope_5m == pytest.approx(0.0)
    assert signal.ls_ratio == pytest.approx(0.0)


def test_ema_bounce_emits_1h_timeframe() -> None:
    setup = EmaBounceSetup()
    settings = SimpleNamespace(filters=SimpleNamespace(setups={}))

    t0 = datetime.now(UTC) - timedelta(hours=2)
    prepared = make_prepared(price=101.0)
    prepared.bias_1h = "uptrend"
    prepared.regime_1h_confirmed = "uptrend"
    prepared.work_1h = pl.DataFrame(
        {
            "time": [t0, t0 + timedelta(hours=1), t0 + timedelta(hours=2)],
            "close_time": [t0, t0 + timedelta(hours=1), t0 + timedelta(hours=2)],
            "open": [99.0, 99.4, 100.2],
            "high": [100.5, 100.8, 102.0],
            "low": [98.8, 99.1, 100.0],
            "close": [99.8, 100.0, 101.2],
            "volume": [900.0, 950.0, 1100.0],
            "atr14": [1.2, 1.3, 1.4],
            "rsi14": [52.0, 54.0, 56.0],
            "volume_ratio20": [1.0, 1.1, 1.3],
            "ema20": [99.6, 100.2, 100.5],
            "ema50": [99.0, 99.4, 99.8],
            "adx14": [21.0, 22.0, 24.0],
        }
    )

    signal = setup.detect(prepared, settings)

    assert signal is not None
    assert signal.timeframe == "1h"


@pytest.mark.parametrize(
    ("min_adx", "expect_signal"),
    [
        (20.0, True),
        (30.0, False),
    ],
)
def test_ema_bounce_config_min_adx_changes_outcome(min_adx: float, expect_signal: bool) -> None:
    setup = EmaBounceSetup()
    settings = SimpleNamespace(filters=SimpleNamespace(setups={"ema_bounce": {"min_adx": min_adx}}))
    t0 = datetime.now(UTC) - timedelta(hours=2)
    prepared = make_prepared(price=101.0)
    prepared.bias_1h = "uptrend"
    prepared.regime_1h_confirmed = "uptrend"
    prepared.work_1h = pl.DataFrame(
        {
            "time": [t0, t0 + timedelta(hours=1), t0 + timedelta(hours=2)],
            "close_time": [t0, t0 + timedelta(hours=1), t0 + timedelta(hours=2)],
            "open": [98.5, 99.0, 99.6],
            "high": [99.8, 100.5, 101.2],
            "low": [98.2, 98.7, 99.4],
            "close": [99.2, 99.4, 100.1],
            "volume": [900.0, 950.0, 1100.0],
            "atr14": [1.2, 1.3, 1.4],
            "rsi14": [52.0, 54.0, 56.0],
            "volume_ratio20": [1.0, 1.1, 1.3],
            "ema20": [99.5, 100.0, 100.4],
            "ema50": [99.0, 99.3, 99.7],
            "adx14": [21.0, 22.0, 24.0],
        }
    )
    signal = setup.detect(prepared, settings)

    assert (signal is not None) is expect_signal


@pytest.mark.parametrize(
    ("min_trend_score", "expect_signal"),
    [
        (0.40, True),
        (0.95, False),
    ],
)
def test_structure_pullback_config_trend_threshold_changes_outcome(
    min_trend_score: float, expect_signal: bool
) -> None:
    setup = StructurePullbackSetup()
    settings = SimpleNamespace(
        filters=SimpleNamespace(
            min_adx_1h=18.0,
            setups={"structure_pullback": {"min_trend_score": min_trend_score}},
        )
    )
    prepared = make_prepared(price=106.0)
    prepared.bias_1h = "uptrend"
    prepared.regime_1h_confirmed = "uptrend"
    prepared.regime_4h_confirmed = "uptrend"
    prepared.structure_1h = "uptrend"
    now = datetime.now(UTC)
    prepared.work_1h = pl.DataFrame(
        {
            "time": [now - timedelta(hours=4), now - timedelta(hours=3), now - timedelta(hours=2), now - timedelta(hours=1), now],
            "close_time": [now - timedelta(hours=4), now - timedelta(hours=3), now - timedelta(hours=2), now - timedelta(hours=1), now],
            "open": [100.0, 101.0, 102.0, 103.0, 104.0],
            "high": [102.0, 104.0, 106.0, 109.0, 112.0],
            "low": [99.0, 100.0, 101.0, 102.0, 103.0],
            "close": [101.0, 103.0, 105.0, 107.0, 108.0],
            "volume": [900.0, 950.0, 1000.0, 1050.0, 1100.0],
            "atr14": [1.2, 1.3, 1.4, 1.5, 1.6],
            "rsi14": [50.0, 52.0, 54.0, 55.0, 56.0],
            "volume_ratio20": [1.0, 1.1, 1.2, 1.2, 1.3],
            "ema20": [100.5, 101.5, 102.5, 103.5, 104.5],
            "ema50": [99.0, 99.5, 100.0, 100.5, 101.0],
            "adx14": [24.0, 25.0, 26.0, 27.0, 28.0],
            "bb_pct_b": [0.50, 0.55, 0.60, 0.65, 0.70],
        }
    )
    prepared.work_15m = pl.DataFrame(
        {
            "time": [now - timedelta(minutes=60), now - timedelta(minutes=45), now - timedelta(minutes=30), now - timedelta(minutes=15), now],
            "close_time": [now - timedelta(minutes=60), now - timedelta(minutes=45), now - timedelta(minutes=30), now - timedelta(minutes=15), now],
            "open": [104.5, 104.2, 104.0, 103.8, 106.0],
            "high": [104.8, 104.3, 104.2, 104.0, 107.0],
            "low": [104.1, 103.9, 103.6, 103.2, 105.8],
            "close": [104.2, 104.0, 103.8, 103.5, 106.0],
            "volume": [800.0, 820.0, 850.0, 900.0, 1200.0],
            "atr14": [0.8, 0.8, 0.9, 1.0, 1.0],
            "rsi14": [48.0, 47.0, 45.0, 43.0, 55.0],
            "volume_ratio20": [0.9, 0.95, 1.0, 1.05, 1.2],
            "bb_pct_b": [0.40, 0.38, 0.36, 0.35, 0.60],
            "ema20": [104.0, 103.9, 103.8, 103.7, 103.9],
            "adx14": [20.0, 21.0, 22.0, 23.0, 24.0],
        }
    )
    prepared.work_4h = pl.DataFrame(
        {
            "time": [now - timedelta(hours=8), now - timedelta(hours=4), now],
            "close_time": [now - timedelta(hours=8), now - timedelta(hours=4), now],
            "open": [98.0, 102.0, 106.0],
            "high": [103.0, 109.0, 120.0],
            "low": [97.0, 101.0, 105.0],
            "close": [102.0, 106.0, 111.0],
            "volume": [5000.0, 5500.0, 6000.0],
            "atr14": [2.5, 2.6, 2.7],
            "rsi14": [52.0, 54.0, 56.0],
            "volume_ratio20": [1.0, 1.1, 1.2],
            "ema20": [100.0, 103.0, 106.0],
            "ema50": [95.0, 98.0, 101.0],
            "adx14": [24.0, 25.0, 26.0],
            "bb_pct_b": [0.55, 0.58, 0.62],
        }
    )
    signal = setup.detect(prepared, settings)

    assert (signal is not None) is expect_signal


@pytest.mark.parametrize(
    ("setup_overrides", "expect_signal"),
    [
        ({}, False),
        ({"min_adx_1h": 15.0}, True),
    ],
)
def test_structure_pullback_min_adx_uses_global_fallback_when_override_absent(
    setup_overrides: dict[str, float], expect_signal: bool
) -> None:
    setup = StructurePullbackSetup()
    settings = SimpleNamespace(
        filters=SimpleNamespace(
            min_adx_1h=18.0,
            setups={"structure_pullback": setup_overrides},
        )
    )
    prepared = make_prepared(price=106.0)
    prepared.bias_1h = "uptrend"
    prepared.regime_1h_confirmed = "uptrend"
    prepared.regime_4h_confirmed = "uptrend"
    prepared.structure_1h = "uptrend"
    now = datetime.now(UTC)
    prepared.work_1h = pl.DataFrame(
        {
            "time": [now - timedelta(hours=4), now - timedelta(hours=3), now - timedelta(hours=2), now - timedelta(hours=1), now],
            "close_time": [now - timedelta(hours=4), now - timedelta(hours=3), now - timedelta(hours=2), now - timedelta(hours=1), now],
            "open": [100.0, 101.0, 102.0, 103.0, 104.0],
            "high": [102.0, 104.0, 106.0, 109.0, 112.0],
            "low": [99.0, 100.0, 101.0, 102.0, 103.0],
            "close": [101.0, 103.0, 105.0, 107.0, 108.0],
            "volume": [900.0, 950.0, 1000.0, 1050.0, 1100.0],
            "atr14": [1.2, 1.3, 1.4, 1.5, 1.6],
            "rsi14": [50.0, 52.0, 54.0, 55.0, 56.0],
            "volume_ratio20": [1.0, 1.1, 1.2, 1.2, 1.3],
            "ema20": [100.5, 101.5, 102.5, 103.5, 104.5],
            "ema50": [99.0, 99.5, 100.0, 100.5, 101.0],
            "adx14": [24.0, 25.0, 26.0, 27.0, 17.0],
            "bb_pct_b": [0.50, 0.55, 0.60, 0.65, 0.70],
        }
    )
    prepared.work_15m = pl.DataFrame(
        {
            "time": [now - timedelta(minutes=60), now - timedelta(minutes=45), now - timedelta(minutes=30), now - timedelta(minutes=15), now],
            "close_time": [now - timedelta(minutes=60), now - timedelta(minutes=45), now - timedelta(minutes=30), now - timedelta(minutes=15), now],
            "open": [104.5, 104.2, 104.0, 103.8, 106.0],
            "high": [104.8, 104.3, 104.2, 104.0, 107.0],
            "low": [104.1, 103.9, 103.6, 103.2, 105.8],
            "close": [104.2, 104.0, 103.8, 103.5, 106.0],
            "volume": [800.0, 820.0, 850.0, 900.0, 1200.0],
            "atr14": [0.8, 0.8, 0.9, 1.0, 1.0],
            "rsi14": [48.0, 47.0, 45.0, 43.0, 55.0],
            "volume_ratio20": [0.9, 0.95, 1.0, 1.05, 1.2],
            "bb_pct_b": [0.40, 0.38, 0.36, 0.35, 0.60],
            "ema20": [104.0, 103.9, 103.8, 103.7, 103.9],
            "adx14": [20.0, 21.0, 22.0, 23.0, 24.0],
        }
    )
    prepared.work_4h = pl.DataFrame(
        {
            "time": [now - timedelta(hours=8), now - timedelta(hours=4), now],
            "close_time": [now - timedelta(hours=8), now - timedelta(hours=4), now],
            "open": [98.0, 102.0, 106.0],
            "high": [103.0, 109.0, 120.0],
            "low": [97.0, 101.0, 105.0],
            "close": [102.0, 106.0, 111.0],
            "volume": [5000.0, 5500.0, 6000.0],
            "atr14": [2.5, 2.6, 2.7],
            "rsi14": [52.0, 54.0, 56.0],
            "volume_ratio20": [1.0, 1.1, 1.2],
            "ema20": [100.0, 103.0, 106.0],
            "ema50": [95.0, 98.0, 101.0],
            "adx14": [24.0, 25.0, 26.0],
            "bb_pct_b": [0.55, 0.58, 0.62],
        }
    )
    signal = setup.detect(prepared, settings)

    assert (signal is not None) is expect_signal


@pytest.mark.parametrize(
    ("min_mitigation_pct", "expect_signal"),
    [
        (0.20, True),
        (0.70, False),
    ],
)
def test_fvg_config_mitigation_threshold_changes_outcome(
    min_mitigation_pct: float, expect_signal: bool
) -> None:
    setup = FVGSetup()
    settings = SimpleNamespace(
        filters=SimpleNamespace(
            setups={
                "fvg": {
                    "min_mitigation_pct": min_mitigation_pct,
                    "min_fvg_size_atr": 0.05,
                    "sl_buffer_atr": 0.5,
                }
            }
        )
    )
    now = datetime.now(UTC)
    prepared = make_prepared(price=100.0)
    prepared.bias_1h = "uptrend"
    prepared.regime_1h_confirmed = "uptrend"
    prepared.structure_1h = "uptrend"
    prepared.work_15m = pl.DataFrame(
        {
            "time": [now - timedelta(minutes=60), now - timedelta(minutes=45), now - timedelta(minutes=30), now - timedelta(minutes=15), now],
            "close_time": [now - timedelta(minutes=60), now - timedelta(minutes=45), now - timedelta(minutes=30), now - timedelta(minutes=15), now],
            "open": [98.2, 99.6, 99.8, 99.7, 100.0],
            "high": [98.6, 100.0, 100.0, 100.2, 100.3],
            "low": [98.0, 99.4, 99.9, 99.7, 99.9],
            "close": [98.4, 99.8, 100.0, 100.0, 100.0],
            "volume": [800.0, 950.0, 1100.0, 1050.0, 1000.0],
            "atr14": [1.0, 1.0, 1.0, 1.0, 1.0],
            "rsi14": [50.0, 52.0, 54.0, 55.0, 56.0],
            "volume_ratio20": [1.0, 1.1, 1.2, 1.1, 1.0],
            "ema20": [98.5, 99.0, 99.4, 99.7, 99.9],
            "ema50": [98.0, 98.5, 98.8, 99.0, 99.2],
            "adx14": [20.0, 21.0, 22.0, 23.0, 24.0],
        }
    )
    signal = setup.detect(prepared, settings)

    assert (signal is not None) is expect_signal


def test_load_settings_merges_legacy_strategy_overrides_once(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_file = tmp_path / "config.toml"
    config_file.write_text(
        """
[bot]
[bot.filters]
[bot.filters.setups]
ema_bounce = { min_adx = 24.0 }
""".strip()
    )
    legacy_dir = tmp_path / "config" / "strategies"
    legacy_dir.mkdir(parents=True)
    (legacy_dir / "ema_bounce.toml").write_text(
        """
[detection]
ema_touch_tolerance_pct = 0.01
[filters]
min_adx = 18.0
""".strip()
    )
    monkeypatch.setenv("TG_TOKEN", "")
    monkeypatch.setenv("TARGET_CHAT_ID", "")

    settings = load_settings(config_file)
    overrides = settings.filters.setups["ema_bounce"]

    assert overrides["ema_touch_tolerance_pct"] == pytest.approx(0.01)
    assert overrides["min_adx"] == pytest.approx(24.0)


@pytest.mark.asyncio
async def test_single_target_price_tick_closes_once_without_tp2_event() -> None:
    tracker, repo, _ = make_tracker(SimpleNamespace())
    tracked = make_tracked_state(
        status="active",
        activated_at=datetime.now(UTC) - timedelta(minutes=1),
        stop=97.5,
        tp1=105.0,
        tp2=105.0,
        single_target_mode=True,
        target_integrity_status="single_target",
    )

    events, closed = await tracker._apply_price_tick(
        tracked,
        price=105.0,
        occurred_at=datetime.now(UTC),
        precision_mode="trade",
    )
    await asyncio.sleep(0)

    assert closed is True
    assert [event.event_type for event in events] == ["tp1_hit"]
    assert tracked.close_reason == "tp1_hit"
    assert repo.tracking_stats["tp1_hit"] == 1
    assert repo.tracking_stats["tp2_hit"] == 0


def test_family_confirmation_rejects_missing_fast_context_when_strict() -> None:
    bot = SignalBot.__new__(SignalBot)
    bot.settings = make_runtime_settings(strict_data_quality=True)
    prepared = make_prepared()
    prepared.work_5m = None
    prepared.mark_index_spread_bps = None
    prepared.depth_imbalance = None
    prepared.microprice_bias = None
    signal = make_signal()

    ok, reason, details = bot._check_family_confirmation(signal, prepared, metadata=None)

    assert ok is False
    assert reason == "data.fast_context_missing"
    assert details["fallback"] == "context_missing"


@pytest.mark.asyncio
async def test_engine_skip_result_keeps_setup_id_and_reason_code() -> None:
    class HistoryHungrySetup(BaseSetup):
        setup_id = "history_hungry"
        min_history_bars = 10

        def get_optimizable_params(self, settings=None) -> dict[str, float]:
            return {}

        def detect(self, prepared: PreparedSymbol, settings):
            raise AssertionError("detect should not run when can_calculate is false")

    registry = StrategyRegistry()
    settings = make_runtime_settings()
    registry.register(HistoryHungrySetup(SetupParams(enabled=True), settings), enabled=True)
    engine = SignalEngine(registry, settings)

    results = await engine.calculate_all(make_prepared())

    assert len(results) == 1
    result = results[0]
    assert result.setup_id == "history_hungry"
    assert result.decision is not None
    assert result.decision.is_skip
    assert result.decision.reason_code == "data.work_1h_insufficient_history"


@pytest.mark.asyncio
async def test_parallel_strategy_rejections_keep_distinct_reason_codes() -> None:
    class RejectASetup(BaseSetup):
        setup_id = "reject_a"
        min_history_bars = 1

        def get_optimizable_params(self, settings=None) -> dict[str, float]:
            return {}

        def detect(self, prepared: PreparedSymbol, settings):
            from bot.setups import _reject

            _reject(prepared, self.setup_id, "atr_invalid", atr=float("nan"))
            return None

    class RejectBSetup(BaseSetup):
        setup_id = "reject_b"
        min_history_bars = 1

        def get_optimizable_params(self, settings=None) -> dict[str, float]:
            return {}

        def detect(self, prepared: PreparedSymbol, settings):
            from bot.setups import _reject

            _reject(prepared, self.setup_id, "price_missing")
            return None

    registry = StrategyRegistry()
    settings = make_runtime_settings()
    registry.register(RejectASetup(SetupParams(enabled=True), settings), enabled=True)
    registry.register(RejectBSetup(SetupParams(enabled=True), settings), enabled=True)
    engine = SignalEngine(registry, settings)

    results = await engine.calculate_all(make_prepared())
    reason_by_setup = {result.setup_id: result.decision.reason_code for result in results if result.decision is not None}

    assert reason_by_setup["reject_a"] == "indicator.atr_invalid"
    assert reason_by_setup["reject_b"] == "data.price_missing"


@pytest.mark.asyncio
async def test_strategy_exception_surfaces_runtime_error_decision() -> None:
    class BrokenSetup(BaseSetup):
        setup_id = "broken_setup"
        min_history_bars = 1

        def get_optimizable_params(self, settings=None) -> dict[str, float]:
            return {}

        def detect(self, prepared: PreparedSymbol, settings):
            raise ValueError("boom")

    registry = StrategyRegistry()
    settings = make_runtime_settings()
    registry.register(BrokenSetup(SetupParams(enabled=True), settings), enabled=True)
    engine = SignalEngine(registry, settings)

    results = await engine.calculate_all(make_prepared())

    assert len(results) == 1
    result = results[0]
    assert result.setup_id == "broken_setup"
    assert result.decision is not None
    assert result.decision.reason_code == "runtime.error"
    assert result.error == "boom"


def test_cli_stderr_prefilter_detects_logger_timestamp_prefix_for_any_year() -> None:
    assert _is_preformatted_log_stderr(
        "2026-04-23 10:11:12,345 | WARNING | stderr | write:1 | STDERR: loop warning"
    )
    assert _is_preformatted_log_stderr(
        "2027-01-01 00:00:00,001 | INFO    | bot.cli | run:42 | BOT SESSION STARTED"
    )
    assert not _is_preformatted_log_stderr("unstructured stderr noise from dependency")
