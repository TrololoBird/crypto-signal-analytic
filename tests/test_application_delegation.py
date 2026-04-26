from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from bot.application.bot import SignalBot
from bot.models import PipelineResult, SymbolFrames, UniverseSymbol


class _AsyncRecorder:
    def __init__(self, result):
        self.result = result
        self.calls: list[tuple[tuple, dict]] = []

    async def __call__(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return self.result


@pytest.mark.asyncio
async def test_run_modern_analysis_delegates_to_symbol_analyzer() -> None:
    bot = object.__new__(SignalBot)
    expected = PipelineResult(
        symbol="BTCUSDT",
        trigger="modern_engine",
        event_ts=datetime.now(UTC),
        raw_setups=0,
        candidates=[],
        rejected=[],
        prepared=None,
    )
    recorder = _AsyncRecorder(expected)
    bot._symbol_analyzer = SimpleNamespace(run_modern_analysis=recorder)

    item = UniverseSymbol(
        symbol="BTCUSDT",
        base_asset="BTC",
        quote_asset="USDT",
        contract_type="PERPETUAL",
        status="TRADING",
        onboard_date_ms=0,
        quote_volume=1_000_000.0,
        price_change_pct=0.0,
        last_price=100.0,
    )
    frames = SymbolFrames(symbol="BTCUSDT", df_1h=None, df_15m=None, bid_price=None, ask_price=None)  # type: ignore[arg-type]

    result = await bot._run_modern_analysis(item, frames, trigger="kline_close", ws_enrichments={"x": 1})

    assert result is expected
    assert len(recorder.calls) == 1
    args, kwargs = recorder.calls[0]
    assert args[0] is item
    assert args[1] is frames
    assert kwargs["trigger"] == "kline_close"
    assert kwargs["ws_enrichments"] == {"x": 1}


@pytest.mark.asyncio
async def test_delegated_periodic_methods_call_managers() -> None:
    bot = object.__new__(SignalBot)

    heartbeat = _AsyncRecorder(None)
    health_telemetry = _AsyncRecorder(None)
    market_regime = _AsyncRecorder(None)
    public_intel = _AsyncRecorder(None)

    bot._health_manager = SimpleNamespace(
        heartbeat_periodic=heartbeat,
        health_telemetry_periodic=health_telemetry,
        health_check=_AsyncRecorder({"status": "healthy"}),
    )
    bot._market_context_updater = SimpleNamespace(
        market_regime_periodic=market_regime,
        public_intelligence_periodic=public_intel,
        apply_public_guardrails=_AsyncRecorder(None),
        update_memory_market_context=_AsyncRecorder(None),
        compute_price_bias=lambda symbol: "neutral",
    )

    await bot._heartbeat_periodic()
    await bot._health_telemetry_periodic()
    await bot._market_regime_periodic()
    await bot._public_intelligence_periodic()

    assert len(heartbeat.calls) == 1
    assert len(health_telemetry.calls) == 1
    assert len(market_regime.calls) == 1
    assert len(public_intel.calls) == 1
