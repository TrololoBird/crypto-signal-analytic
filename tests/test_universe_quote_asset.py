from __future__ import annotations

from types import SimpleNamespace

import pytest

from bot.models import SymbolMeta
from bot.universe import build_shortlist


def _settings(*, quote_asset: str, pinned_symbols: tuple[str, ...] = ()) -> SimpleNamespace:
    return SimpleNamespace(
        universe=SimpleNamespace(
            quote_asset=quote_asset,
            pinned_symbols=pinned_symbols,
            min_listing_age_days=0,
            min_quote_volume_usd=0.0,
            dynamic_limit=20,
            shortlist_limit=20,
        ),
    )


def _meta(symbol: str, base_asset: str, quote_asset: str) -> SymbolMeta:
    return SymbolMeta(
        symbol=symbol,
        base_asset=base_asset,
        quote_asset=quote_asset,
        contract_type="PERPETUAL",
        status="TRADING",
        onboard_date_ms=0,
    )


@pytest.mark.parametrize(
    ("quote_asset", "symbol", "base_asset"),
    (("USDT", "BTCUSDT", "BTC"), ("USDC", "BTCUSDC", "BTC")),
)
def test_build_shortlist_accepts_usdt_and_usdc_when_quote_matches(
    quote_asset: str,
    symbol: str,
    base_asset: str,
) -> None:
    settings = _settings(quote_asset=quote_asset)
    shortlist, _summary = build_shortlist(
        [_meta(symbol, base_asset, quote_asset)],
        [
            {
                "symbol": symbol,
                "quote_volume": 1_000_000.0,
                "last_price": 100.0,
                "price_change_percent": 1.2,
            }
        ],
        settings,
    )

    assert [row.symbol for row in shortlist] == [symbol]


def test_build_shortlist_filters_by_meta_quote_asset() -> None:
    settings = _settings(quote_asset="USDC")
    shortlist, _summary = build_shortlist(
        [_meta("ETHUSDT", "ETH", "USDT")],
        [
            {
                "symbol": "ETHUSDT",
                "quote_volume": 1_000_000.0,
                "last_price": 2000.0,
                "price_change_percent": 2.0,
            }
        ],
        settings,
    )

    assert shortlist == []
