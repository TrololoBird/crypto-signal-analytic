from __future__ import annotations

import asyncio
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from test_remediation_regressions import (
    BookTickerEvent,
    PipelineResult,
    SignalBot,
    TelemetryStub,
    UTC,
    make_runtime_settings,
    make_signal,
    make_universe_symbol,
)


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


@pytest.mark.asyncio
async def test_intra_candle_uses_event_timestamp_when_available() -> None:
    signal = make_signal(created_at=datetime(2026, 4, 23, 0, 3, tzinfo=UTC))
    event_ts_ms = int(datetime(2026, 4, 23, 0, 4, tzinfo=UTC).timestamp() * 1000)
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

    await bot._on_book_ticker(BookTickerEvent(symbol="BTCUSDT", bid=100.0, ask=100.1, event_ts_ms=event_ts_ms))

    for _ in range(3):
        tasks = list(bot._background_tasks)
        if not tasks:
            break
        await asyncio.gather(*tasks)

    called_event_ts = bot._run_modern_analysis.await_args.kwargs["event_ts"]
    assert called_event_ts == datetime.fromtimestamp(event_ts_ms / 1000.0, tz=UTC)
    ws_enrichments = bot._run_modern_analysis.await_args.kwargs["ws_enrichments"]
    assert ws_enrichments["bid_price"] == pytest.approx(100.0)
    assert ws_enrichments["ask_price"] == pytest.approx(100.1)
    assert ws_enrichments["spread_bps"] == pytest.approx(((100.1 - 100.0) / 100.05) * 10000.0)


@pytest.mark.asyncio
async def test_intra_candle_skips_small_book_move_when_min_move_bps_configured() -> None:
    signal = make_signal(created_at=datetime(2026, 4, 23, 0, 5, tzinfo=UTC))
    bot = SignalBot.__new__(SignalBot)
    bot.settings = make_runtime_settings(min_move_bps=5.0)
    bot._last_intra_scan = {}
    bot._last_intra_mid = {}
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

    await bot._on_book_ticker(BookTickerEvent(symbol="BTCUSDT", bid=100.0, ask=100.1))
    for task in list(bot._background_tasks):
        await asyncio.gather(task)

    # ~0.5 bps move (< 5 bps threshold) should be ignored
    await bot._on_book_ticker(BookTickerEvent(symbol="BTCUSDT", bid=100.004, ask=100.104))
    for task in list(bot._background_tasks):
        await asyncio.gather(task)

    assert bot._run_modern_analysis.await_count == 1
