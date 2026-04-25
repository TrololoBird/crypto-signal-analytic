"""Shortlist refresh helpers for SignalBot."""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

from ..models import UniverseSymbol
from ..universe import build_shortlist

UTC = timezone.utc
LOG = logging.getLogger("bot.application.shortlist_service")


class ShortlistService:
    """Encapsulates shortlist build/refresh lifecycle for ``SignalBot``."""

    def __init__(self, bot: Any) -> None:
        self._bot = bot

    async def fetch_symbols_with_retry(self, *, max_retries: int = 1) -> list[Any]:
        for attempt in range(max_retries + 1):
            try:
                return await asyncio.wait_for(
                    self._bot.client.fetch_exchange_symbols(),
                    timeout=10.0,
                )
            except asyncio.TimeoutError:
                LOG.warning("fetch_exchange_symbols attempt %d/%d timed out", attempt + 1, max_retries + 1)
                if attempt < max_retries:
                    await asyncio.sleep(1.0)
                else:
                    raise
            except Exception as exc:
                LOG.warning("fetch_exchange_symbols attempt %d/%d failed: %s", attempt + 1, max_retries + 1, exc)
                if attempt < max_retries:
                    await asyncio.sleep(1.0)
                else:
                    raise
        return []

    def extract_symbol_assets(self, symbol: str) -> tuple[str | None, str | None]:
        bot = self._bot
        sym = str(symbol).strip().upper()
        meta = bot._symbol_meta_by_symbol.get(sym)
        if meta is None:
            exchange_cache = getattr(bot.client, "_exchange_info_cache", None)
            if exchange_cache is not None:
                _cached_at, rows = exchange_cache
                cache_map = {
                    str(getattr(row, "symbol", "")).strip().upper(): row
                    for row in rows
                }
                bot._symbol_meta_by_symbol.update(cache_map)
                meta = bot._symbol_meta_by_symbol.get(sym)
        if meta is not None:
            base = str(getattr(meta, "base_asset", "")).strip().upper()
            quote = str(getattr(meta, "quote_asset", "")).strip().upper()
            if base and quote:
                return base, quote

        configured_quote = str(bot.settings.universe.quote_asset).strip().upper()
        if configured_quote and sym.endswith(configured_quote):
            base = sym[: -len(configured_quote)]
            if base:
                return base, configured_quote
        return None, None

    def build_pinned_shortlist(self) -> list[UniverseSymbol]:
        bot = self._bot
        shortlist: list[UniverseSymbol] = []
        for raw_symbol in bot.settings.universe.pinned_symbols:
            symbol = str(raw_symbol).strip().upper()
            base_asset, quote_asset = self.extract_symbol_assets(symbol)
            if not base_asset or not quote_asset:
                LOG.warning(
                    "skipping pinned symbol due to unresolved base/quote assets | symbol=%s configured_quote_asset=%s",
                    symbol,
                    bot.settings.universe.quote_asset,
                )
                continue
            shortlist.append(
                UniverseSymbol(
                    symbol=symbol,
                    base_asset=base_asset,
                    quote_asset=quote_asset,
                    contract_type="PERPETUAL",
                    status="TRADING",
                    onboard_date_ms=0,
                    quote_volume=0.0,
                    price_change_pct=0.0,
                    last_price=0.0,
                    shortlist_bucket="pinned",
                )
            )
        return shortlist

    async def build_live_shortlist(self) -> tuple[list[UniverseSymbol], dict[str, int]]:
        bot = self._bot
        timeout_s = max(10.0, float(bot.settings.ws.rest_timeout_seconds) * 2.0)
        symbol_meta_list, tickers_24h = await asyncio.wait_for(
            asyncio.gather(
                self.fetch_symbols_with_retry(max_retries=1),
                bot.client.fetch_ticker_24h(),
            ),
            timeout=timeout_s,
        )
        bot._symbol_meta_by_symbol = {
            str(getattr(row, "symbol", "")).strip().upper(): row for row in symbol_meta_list
        }
        shortlist, summary = build_shortlist(symbol_meta_list, tickers_24h, bot.settings)
        return shortlist, summary

    async def do_refresh_shortlist(self) -> list[UniverseSymbol]:
        bot = self._bot
        LOG.info("refreshing shortlist...")

        source = "pinned_fallback"
        summary: dict[str, Any] = {}
        shortlist = self.build_pinned_shortlist()

        try:
            live_shortlist, live_summary = await self.build_live_shortlist()
            if live_shortlist:
                shortlist = live_shortlist
                summary = live_summary
                source = "live"
                bot._last_live_shortlist = list(live_shortlist)
            elif bot._last_live_shortlist:
                shortlist = list(bot._last_live_shortlist)
                source = "cached"
        except Exception as exc:
            if bot._last_live_shortlist:
                shortlist = list(bot._last_live_shortlist)
                source = "cached"
                LOG.warning("shortlist refresh failed, using cached shortlist: %s", exc)
            else:
                LOG.warning("shortlist refresh failed, using pinned fallback: %s", exc)

        async with bot._shortlist_lock:
            bot._shortlist = shortlist
        bot._shortlist_source = source

        bot.telemetry.append_jsonl(
            "shortlist.jsonl",
            {
                "ts": datetime.now(UTC).isoformat(),
                "source": source,
                "size": len(shortlist),
                "symbols": [item.symbol for item in shortlist[:20]],
                "eligible": summary.get("eligible"),
                "dynamic_pool": summary.get("dynamic_pool"),
                "pinned": summary.get("pinned"),
            },
        )

        LOG.info(
            "shortlist refresh complete | source=%s size=%d eligible=%s dynamic_pool=%s pinned=%s",
            source,
            len(shortlist),
            summary.get("eligible"),
            summary.get("dynamic_pool"),
            summary.get("pinned"),
        )
        return shortlist

    async def refresh_shortlist_periodic(self) -> None:
        bot = self._bot
        await asyncio.sleep(5)
        while not bot._shutdown.is_set():
            await self.do_refresh_shortlist()
            try:
                await asyncio.wait_for(
                    bot._shutdown.wait(),
                    timeout=bot.settings.runtime.shortlist_refresh_interval_seconds,
                )
            except asyncio.TimeoutError:
                continue
