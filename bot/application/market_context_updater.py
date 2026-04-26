from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast

from bot.market_data import BinanceFuturesMarketData
from bot.models import UniverseSymbol

if TYPE_CHECKING:
    from bot.application.bot import SignalBot


LOG = logging.getLogger("bot.application.bot")


class MarketContextUpdater:
    def __init__(self, bot: SignalBot) -> None:
        self._bot = bot
        self._last_regime: str | None = None

    async def market_regime_periodic(self) -> None:
        await asyncio.sleep(10)
        while not self._bot._shutdown.is_set():
            try:
                async with self._bot._shortlist_lock:
                    shortlist = list(self._bot._shortlist)
                if shortlist:
                    await self.update_memory_market_context(shortlist)
                    LOG.debug("market regime periodic update completed")
            except Exception as exc:
                LOG.debug("market regime periodic update failed: %s", exc)
            try:
                await asyncio.wait_for(self._bot._shutdown.wait(), timeout=60)
            except TimeoutError:
                continue

    async def public_intelligence_periodic(self) -> None:
        await asyncio.sleep(45)
        while not self._bot._shutdown.is_set():
            try:
                async with self._bot._shortlist_lock:
                    shortlist = list(self._bot._shortlist)
                if shortlist and self._bot.intelligence is not None:
                    snapshot = await self._bot.intelligence.collect([item.symbol for item in shortlist])
                    await self.update_memory_market_context(shortlist)
                    await self.apply_public_guardrails(snapshot)
                    LOG.info(
                        "public intelligence updated | barrier_long=%s barrier_short=%s macro=%s",
                        cast(dict[str, Any], snapshot.get("barrier") or {}).get(
                            "long_barrier_triggered"
                        ),
                        cast(dict[str, Any], snapshot.get("barrier") or {}).get(
                            "short_barrier_triggered"
                        ),
                        cast(dict[str, Any], snapshot.get("macro") or {}).get("risk_mode"),
                    )
            except Exception as exc:
                LOG.warning("public intelligence update failed: %s", exc, exc_info=True)
            try:
                await asyncio.wait_for(
                    self._bot._shutdown.wait(),
                    timeout=max(60, int(self._bot.settings.intelligence.refresh_interval_seconds)),
                )
            except TimeoutError:
                continue

    async def apply_public_guardrails(self, snapshot: dict[str, Any]) -> None:
        if self._bot.intelligence is None:
            return
        open_rows = await self._bot._modern_repo.get_active_signals(include_closed=False)
        if not open_rows:
            return

        barrier = cast(dict[str, Any], snapshot.get("barrier") or {})
        barrier_events = []
        closed_tracking_ids: set[str] = set()

        if bool(barrier.get("long_barrier_triggered")):
            long_ids = [
                str(row["tracking_id"])
                for row in open_rows
                if str(row.get("direction") or "").lower() == "long"
            ]
            if long_ids:
                note = (
                    f"tracked_signal_hard_barrier_long {barrier.get('strongest_symbol')} "
                    f"{barrier.get('strongest_move_pct')}pct/{barrier.get('window_minutes')}m"
                )
                barrier_events.extend(
                    await self._bot.tracker.force_close_tracking_ids(
                        long_ids,
                        reason="emergency_exit",
                        occurred_at=datetime.now(UTC),
                        note=note,
                    )
                )
                closed_tracking_ids.update(long_ids)

        if bool(barrier.get("short_barrier_triggered")):
            short_ids = [
                str(row["tracking_id"])
                for row in open_rows
                if str(row.get("direction") or "").lower() == "short"
            ]
            if short_ids:
                note = (
                    f"tracked_signal_hard_barrier_short {barrier.get('strongest_symbol')} "
                    f"{barrier.get('strongest_move_pct')}pct/{barrier.get('window_minutes')}m"
                )
                barrier_events.extend(
                    await self._bot.tracker.force_close_tracking_ids(
                        short_ids,
                        reason="emergency_exit",
                        occurred_at=datetime.now(UTC),
                        note=note,
                    )
                )
                closed_tracking_ids.update(short_ids)

        smart_exit_events = []
        if self._bot.settings.intelligence.smart_exit_enabled:
            for row in open_rows:
                tracking_id = str(row.get("tracking_id") or "")
                if not tracking_id or tracking_id in closed_tracking_ids:
                    continue
                symbol = str(row.get("symbol") or "")
                direction = str(row.get("direction") or "")
                smart_exit = await self._bot.intelligence.evaluate_smart_exit(symbol, direction)
                if not bool(smart_exit.get("triggered")):
                    continue
                smart_exit_events.extend(
                    await self._bot.tracker.force_close_tracking_ids(
                        [tracking_id],
                        reason="smart_exit",
                        occurred_at=datetime.now(UTC),
                        note=";".join(cast(list[str], smart_exit.get("reasons") or [])[:6]),
                    )
                )

        combined_events = barrier_events + smart_exit_events
        if combined_events:
            await self._bot._deliver_tracking(combined_events)

    async def update_memory_market_context(self, shortlist: list[UniverseSymbol]) -> None:
        try:
            if not isinstance(self._bot.client, BinanceFuturesMarketData):
                return
            high_funding: list[str] = []
            low_funding: list[str] = []
            extreme_threshold = 0.0005
            funding_rates: dict[str, float] = {}

            for item in shortlist:
                cached = self._bot.client._funding_rate_cache.get(item.symbol)
                if cached is None:
                    continue
                _, fr = cached
                funding_rates[item.symbol] = fr
                if fr >= extreme_threshold:
                    high_funding.append(item.symbol)
                elif fr <= -extreme_threshold:
                    low_funding.append(item.symbol)

            btc_bias = "neutral"
            eth_bias = "neutral"
            if self._bot._ws_manager is not None:
                for sym, bias_attr in [("BTCUSDT", "btc_bias"), ("ETHUSDT", "eth_bias")]:
                    bias = self.compute_price_bias(sym)
                    if bias_attr == "btc_bias":
                        btc_bias = bias
                    else:
                        eth_bias = bias

            benchmark_context: dict[str, dict[str, Any]] = {}
            for sym, bias in [("BTCUSDT", btc_bias), ("ETHUSDT", eth_bias)]:
                payload: dict[str, Any] = {"bias": bias}
                payload["oi_change_pct"] = self._bot.client.get_cached_oi_change(sym, period="1h")
                payload["basis_pct"] = self._bot.client.get_cached_basis(sym, period="1h")
                basis_stats = self._bot.client.get_cached_basis_stats(sym, period="5m")
                if basis_stats is not None:
                    payload["premium_slope_5m"] = basis_stats.get("premium_slope_5m")
                    payload["premium_zscore_5m"] = basis_stats.get("premium_zscore_5m")
                benchmark_context[sym] = payload

            ticker_data: list[dict[str, Any]] = []
            all_tickers = await self._bot.client.fetch_ticker_24h()
            ticker_dict = {t.get("symbol"): t for t in all_tickers if isinstance(t, dict)}
            for item in shortlist:
                ticker = ticker_dict.get(item.symbol)
                if ticker:
                    ticker_data.append(ticker)

            regime_result = self._bot.market_regime.analyze(
                ticker_data,
                funding_rates,
                benchmark_context=benchmark_context,
            )
            if self._last_regime != regime_result.regime:
                self._bot.telemetry.append_jsonl(
                    "regime_transitions.jsonl",
                    {
                        "ts": datetime.now(UTC).isoformat(),
                        "previous_regime": self._last_regime,
                        "new_regime": regime_result.regime,
                        "strength": float(regime_result.strength),
                        "confidence": float(getattr(regime_result, "confidence", 0.0) or 0.0),
                        "detector": str(
                            getattr(self._bot.settings.intelligence, "regime_detector", "legacy")
                        ),
                    },
                )
                self._last_regime = regime_result.regime
            intelligence_snapshot = (
                self._bot.intelligence.latest_snapshot if self._bot.intelligence is not None else None
            )
            macro_risk_mode = (
                "disabled_binance_only"
                if self._bot.settings.intelligence.source_policy == "binance_only"
                else "unknown"
            )
            if intelligence_snapshot:
                macro_snapshot = cast(dict[str, Any], intelligence_snapshot.get("macro") or {})
                macro_risk_mode = str(macro_snapshot.get("risk_mode") or macro_risk_mode)
            await self._bot._modern_repo.update_market_context(
                btc_bias,
                eth_bias,
                high_funding,
                low_funding,
                market_regime=regime_result.regime,
                market_regime_confirmed=True,
                macro_risk_mode=macro_risk_mode,
                intelligence_snapshot=intelligence_snapshot,
            )
            LOG.info(
                "market regime updated | regime=%s strength=%.2f btc=%s eth=%s",
                regime_result.regime,
                regime_result.strength,
                regime_result.btc_bias,
                regime_result.eth_bias,
            )

        except Exception as exc:
            LOG.warning("memory market context update failed: %s", exc, exc_info=True)

    def compute_price_bias(self, symbol: str) -> str:
        if self._bot._ws_manager is None:
            return "neutral"
        klines = self._bot._ws_manager.get_kline_cache(symbol, "4h")
        if klines and len(klines) >= 2:
            try:
                c1 = float(klines[-2]["close"])
                c2 = float(klines[-1]["close"])
                if c1 > 0 and c2 > 0:
                    pct = (c2 - c1) / c1
                    return "uptrend" if pct > 0.008 else ("downtrend" if pct < -0.008 else "neutral")
            except (KeyError, TypeError, ValueError):
                pass
        ticker = self._bot._ws_manager.get_ticker_snapshot(symbol)
        if ticker:
            try:
                pct_24h = float(ticker.get("price_change_percent") or 0.0) / 100.0
                return "uptrend" if pct_24h > 0.02 else ("downtrend" if pct_24h < -0.02 else "neutral")
            except (TypeError, ValueError):
                pass
        return "neutral"
