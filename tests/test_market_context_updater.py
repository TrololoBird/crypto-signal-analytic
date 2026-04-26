from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace

import pytest

from bot.application.market_context_updater import MarketContextUpdater
from bot.models import UniverseSymbol


@dataclass
class _RegimeResult:
    regime: str = "bull"
    strength: float = 0.7
    btc_bias: str = "uptrend"
    eth_bias: str = "uptrend"
    confidence: float = 0.8


class _Repo:
    async def update_market_context(self, *args, **kwargs):
        return None


class _Telemetry:
    def __init__(self) -> None:
        self.rows: list[tuple[str, dict]] = []

    def append_jsonl(self, name: str, row: dict) -> None:
        self.rows.append((name, row))


class _Client:
    def __init__(self) -> None:
        self._funding_rate_cache = {"BTCUSDT": (0, 0.0002)}

    def get_cached_oi_change(self, symbol: str, period: str = "1h"):
        return 0.01

    def get_cached_basis(self, symbol: str, period: str = "1h"):
        return 0.02

    def get_cached_basis_stats(self, symbol: str, period: str = "5m"):
        return {"premium_slope_5m": 0.001, "premium_zscore_5m": 0.2}

    async def fetch_ticker_24h(self):
        return [{"symbol": "BTCUSDT", "price_change_percent": "1.0"}]


@pytest.mark.asyncio
async def test_logs_regime_transition_once(monkeypatch: pytest.MonkeyPatch) -> None:
    from bot.application import market_context_updater as mcu_module

    class _DummyBinance(_Client):
        pass

    monkeypatch.setattr(mcu_module, "BinanceFuturesMarketData", _DummyBinance)

    bot = SimpleNamespace(
        client=_DummyBinance(),
        _ws_manager=None,
        market_regime=SimpleNamespace(analyze=lambda *args, **kwargs: _RegimeResult()),
        _modern_repo=_Repo(),
        telemetry=_Telemetry(),
        settings=SimpleNamespace(intelligence=SimpleNamespace(source_policy="binance_only", regime_detector="composite")),
        intelligence=None,
    )

    updater = MarketContextUpdater(bot)
    shortlist = [
        UniverseSymbol(
            symbol="BTCUSDT",
            base_asset="BTC",
            quote_asset="USDT",
            contract_type="PERPETUAL",
            status="TRADING",
            onboard_date_ms=0,
            quote_volume=1_000_000,
            price_change_pct=0.0,
            last_price=100.0,
        )
    ]

    await updater.update_memory_market_context(shortlist)
    await updater.update_memory_market_context(shortlist)

    transition_rows = [r for name, r in bot.telemetry.rows if name == "regime_transitions.jsonl"]
    assert len(transition_rows) == 1
    assert transition_rows[0]["new_regime"] == "bull"
