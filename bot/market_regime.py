"""Market regime detection and bull/bear index.

Analyzes market-wide conditions to determine overall trend direction
and strength. Used for signal filtering and market context in alerts.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import numpy as np
import polars as pl

from .regime.composite_regime import CompositeRegimeAnalyzer

if TYPE_CHECKING:
    from .config import BotSettings

LOG = logging.getLogger("bot.market_regime")


@dataclass(frozen=True)
class MarketRegimeResult:
    """Market regime analysis result."""

    regime: str  # "bull", "bear", "ranging", "volatile"
    strength: float  # 0.0 to 1.0
    btc_bias: str  # "bull", "bear", "neutral"
    eth_bias: str  # "bull", "bear", "neutral"
    dominance_24h: float  # BTC dominance change
    funding_sentiment: str  # "long_heavy", "short_heavy", "neutral"
    oi_momentum: str  # "rising", "falling", "stable"
    top_gainer_pct: float  # % of top 10 gainers
    top_loser_pct: float  # % of top 10 losers
    altcoin_season_index: float  # 0-100, higher = more alt activity
    volatility_regime: str = "stable"  # "expanding" | "contracting" | "stable"
    risk_on_off: str = "neutral"  # "risk_on" | "risk_off" | "neutral"
    btc_phase: str = "sideways"  # "accumulation" | "markup" | "distribution" | "decline" | "sideways"
    confidence: float = 0.0  # 0.0 to 1.0

    @property
    def is_bullish(self) -> bool:
        """Quick check if overall market is bullish."""
        return self.regime in ("bull",) and self.strength > 0.5

    @property
    def is_bearish(self) -> bool:
        """Quick check if overall market is bearish."""
        return self.regime in ("bear",) and self.strength > 0.5

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "regime": self.regime,
            "strength": round(self.strength, 3),
            "btc_bias": self.btc_bias,
            "eth_bias": self.eth_bias,
            "dominance_24h": round(self.dominance_24h, 3),
            "funding_sentiment": self.funding_sentiment,
            "oi_momentum": self.oi_momentum,
            "top_gainer_pct": round(self.top_gainer_pct, 2),
            "top_loser_pct": round(self.top_loser_pct, 2),
            "altcoin_season_index": round(self.altcoin_season_index, 1),
            "volatility_regime": self.volatility_regime,
            "risk_on_off": self.risk_on_off,
            "btc_phase": self.btc_phase,
            "confidence": round(self.confidence, 3),
        }


class MarketRegimeAnalyzer:
    """Analyzes overall market conditions and regime."""

    def __init__(self, settings: BotSettings) -> None:
        self.settings = settings
        self._last_result: MarketRegimeResult | None = None
        self._last_update_ts: float = 0.0
        self._cache_ttl_seconds = 60.0  # Recalculate every minute
        self._composite = CompositeRegimeAnalyzer()

    def analyze(
        self,
        ticker_data: list[dict[str, Any]],
        funding_rates: dict[str, float] | None = None,
        open_interest: dict[str, float] | None = None,
        benchmark_context: dict[str, dict[str, Any]] | None = None,
    ) -> MarketRegimeResult:
        """Analyze market regime from ticker and funding data.

        Args:
            ticker_data: List of 24h ticker data for all symbols
            funding_rates: Optional dict of symbol -> funding rate
            open_interest: Optional dict of symbol -> OI value
            benchmark_context: Optional BTC/ETH structure and derivatives context

        Returns:
            MarketRegimeResult with full analysis
        """
        import time

        now = time.monotonic()
        if (
            self._last_result is not None
            and now - self._last_update_ts < self._cache_ttl_seconds
        ):
            return self._last_result

        intelligence = getattr(self.settings, "intelligence", None)
        detector = getattr(intelligence, "regime_detector", "legacy")
        if detector in {"hmm", "gmm_var", "composite"}:
            composite = self._composite.analyze(
                ticker_data,
                funding_rates,
                benchmark_context,
            )
            mapped = MarketRegimeResult(
                regime=composite.regime,
                strength=composite.strength,
                btc_bias="neutral",
                eth_bias="neutral",
                dominance_24h=0.0,
                funding_sentiment="neutral",
                oi_momentum="stable",
                top_gainer_pct=0.0,
                top_loser_pct=0.0,
                altcoin_season_index=50.0,
                confidence=composite.confidence,
            )
            self._last_result = mapped
            self._last_update_ts = now
            return mapped

        result = self._calculate_regime(ticker_data, funding_rates, open_interest, benchmark_context)
        self._last_result = result
        self._last_update_ts = now
        return result

    def _calculate_regime(
        self,
        ticker_data: list[dict[str, Any]],
        funding_rates: dict[str, float] | None = None,
        open_interest: dict[str, float] | None = None,
        benchmark_context: dict[str, dict[str, Any]] | None = None,
    ) -> MarketRegimeResult:
        """Calculate market regime from raw data."""
        if not ticker_data:
            # Return neutral if no data
            return MarketRegimeResult(
                regime="ranging",
                strength=0.0,
                btc_bias="neutral",
                eth_bias="neutral",
                dominance_24h=0.0,
                funding_sentiment="neutral",
                oi_momentum="stable",
                top_gainer_pct=0.0,
                top_loser_pct=0.0,
                altcoin_season_index=50.0,
                volatility_regime="stable",
                risk_on_off="neutral",
                btc_phase="sideways",
                confidence=0.0,
            )

        # Find BTC and ETH
        btc_change = 0.0
        eth_change = 0.0
        all_changes: list[tuple[str, float]] = []

        for ticker in ticker_data:
            symbol = ticker.get("symbol", "")
            change = float(ticker.get("price_change_percent") or 0.0)
            all_changes.append((symbol, change))

            if symbol == "BTCUSDT":
                btc_change = change
            elif symbol == "ETHUSDT":
                eth_change = change

        benchmark_context = benchmark_context or {}
        btc_ctx = benchmark_context.get("BTCUSDT", {})
        eth_ctx = benchmark_context.get("ETHUSDT", {})

        # Prefer runtime structure-derived bias over raw 24h moves.
        btc_bias = str(btc_ctx.get("bias") or self._change_to_bias(btc_change))
        eth_bias = str(eth_ctx.get("bias") or self._change_to_bias(eth_change))

        # Sort by change for gainers/losers
        sorted_changes = sorted(all_changes, key=lambda x: x[1], reverse=True)
        top_10 = sorted_changes[:10]
        bottom_10 = sorted_changes[-10:]

        top_gainer_pct = sum(c for _, c in top_10) / len(top_10) if top_10 else 0.0
        top_loser_pct = sum(c for _, c in bottom_10) / len(bottom_10) if bottom_10 else 0.0

        # Calculate altcoin season index
        # Higher when alts outperform BTC
        if btc_change != 0:
            alt_perf = [
                c for s, c in all_changes if s not in ("BTCUSDT", "ETHUSDT")
            ]
            if alt_perf:
                avg_alt_change = sum(alt_perf) / len(alt_perf)
                # Index: 50 = neutral, >50 = alts leading, <50 = BTC leading
                altcoin_season_index = 50.0 + (avg_alt_change - btc_change) * 5.0
                altcoin_season_index = max(0.0, min(100.0, altcoin_season_index))
            else:
                altcoin_season_index = 50.0
        else:
            altcoin_season_index = 50.0

        def _context_score(payload: dict[str, Any]) -> float:
            score = 0.0
            bias = str(payload.get("bias") or "neutral")
            if bias in {"uptrend", "bull"}:
                score += 0.45
            elif bias in {"downtrend", "bear"}:
                score -= 0.45

            oi_change = float(payload.get("oi_change_pct") or 0.0)
            if oi_change > 0.0:
                score += min(oi_change * 4.0, 0.20)
            elif oi_change < 0.0:
                score += max(oi_change * 4.0, -0.20)

            basis_pct = payload.get("basis_pct")
            if basis_pct is not None:
                basis_val = float(basis_pct)
                score += max(min(basis_val / 0.25, 0.15), -0.15)

            premium_slope = payload.get("premium_slope_5m")
            if premium_slope is not None:
                score += max(min(float(premium_slope) / 0.10, 0.12), -0.12)
            return score

        btc_score = _context_score(btc_ctx)
        eth_score = _context_score(eth_ctx)
        avg_context_score = (btc_score + eth_score) / 2.0
        btc_bull = btc_bias in {"uptrend", "bull"}
        btc_bear = btc_bias in {"downtrend", "bear"}
        eth_bull = eth_bias in {"uptrend", "bull"}
        eth_bear = eth_bias in {"downtrend", "bear"}
        mixed_structure = (btc_bull and eth_bear) or (btc_bear and eth_bull)

        if btc_bull and eth_bull and avg_context_score > 0.20:
            regime = "bull"
            strength = min(1.0, 0.45 + abs(avg_context_score))
        elif btc_bear and eth_bear and avg_context_score < -0.20:
            regime = "bear"
            strength = min(1.0, 0.45 + abs(avg_context_score))
        elif mixed_structure or abs(btc_score - eth_score) > 0.35:
            regime = "volatile"
            strength = min(1.0, 0.35 + abs(btc_score - eth_score))
        else:
            regime = "ranging"
            strength = 0.30 + min(0.30, abs(avg_context_score))

        # Funding sentiment
        if funding_rates:
            avg_funding = sum(funding_rates.values()) / len(funding_rates)
            if avg_funding > 0.01:
                funding_sentiment = "long_heavy"
            elif avg_funding < -0.01:
                funding_sentiment = "short_heavy"
            else:
                funding_sentiment = "neutral"
        else:
            funding_sentiment = "neutral"

        oi_samples = []
        for payload in (btc_ctx, eth_ctx):
            raw_oi = payload.get("oi_change_pct")
            if raw_oi is None:
                continue
            try:
                oi_samples.append(float(raw_oi))
            except (TypeError, ValueError):
                continue
        if oi_samples:
            avg_oi = sum(oi_samples) / len(oi_samples)
            if avg_oi > 0.03:
                oi_momentum = "rising"
            elif avg_oi < -0.03:
                oi_momentum = "falling"
            else:
                oi_momentum = "stable"
        else:
            oi_momentum = "stable"

        # Dominance proxy: relative BTC leadership versus ETH over the same window.
        # Positive => BTC outperforms ETH, negative => ETH outperforms BTC.
        dominance_24h = btc_change - eth_change

        volatility_span = abs(top_gainer_pct - top_loser_pct)
        if volatility_span >= 8.0:
            volatility_regime = "expanding"
        elif volatility_span <= 3.0:
            volatility_regime = "contracting"
        else:
            volatility_regime = "stable"

        if altcoin_season_index >= 60 and dominance_24h < 0:
            risk_on_off = "risk_on"
        elif dominance_24h > 1.5 or (funding_sentiment == "long_heavy" and regime in {"bear", "volatile"}):
            risk_on_off = "risk_off"
        else:
            risk_on_off = "neutral"

        if btc_change > 1.5 and dominance_24h >= 0:
            btc_phase = "markup"
        elif btc_change < -1.5 and dominance_24h >= 0:
            btc_phase = "decline"
        elif dominance_24h < 0 and btc_change <= 0.5:
            btc_phase = "accumulation"
        elif dominance_24h < 0 and btc_change > 0.5:
            btc_phase = "distribution"
        else:
            btc_phase = "sideways"

        confidence = min(
            1.0,
            0.35
            + min(abs(avg_context_score), 0.35)
            + (0.15 if volatility_regime != "stable" else 0.0)
            + (0.15 if regime in {"bull", "bear"} else 0.0),
        )

        return MarketRegimeResult(
            regime=regime,
            strength=round(strength, 3),
            btc_bias=btc_bias,
            eth_bias=eth_bias,
            dominance_24h=round(dominance_24h, 3),
            funding_sentiment=funding_sentiment,
            oi_momentum=oi_momentum,
            top_gainer_pct=round(top_gainer_pct, 2),
            top_loser_pct=round(top_loser_pct, 2),
            altcoin_season_index=round(altcoin_season_index, 1),
            volatility_regime=volatility_regime,
            risk_on_off=risk_on_off,
            btc_phase=btc_phase,
            confidence=round(confidence, 3),
        )

    def _change_to_bias(self, change_pct: float) -> str:
        """Convert price change to bias string."""
        if change_pct > 2.0:
            return "bull"
        elif change_pct < -2.0:
            return "bear"
        return "neutral"

    def get_market_context_message(self) -> str:
        """Get formatted market context for alerts."""
        if self._last_result is None:
            return "🔄 Market analysis pending..."

        r = self._last_result

        # Emoji based on regime
        regime_emoji = {
            "bull": "🟢",
            "bear": "🔴",
            "ranging": "⚪",
            "volatile": "🟡",
        }.get(r.regime, "⚪")

        # Strength bars
        strength_bars = int(r.strength * 5) + 1
        strength_str = "█" * strength_bars + "░" * (5 - strength_bars)

        # BTC/ETH indicators
        btc_emoji = {"bull": "🟢", "bear": "🔴", "neutral": "⚪"}.get(r.btc_bias, "⚪")
        eth_emoji = {"bull": "🟢", "bear": "🔴", "neutral": "⚪"}.get(r.eth_bias, "⚪")

        lines = [
            f"{regime_emoji} <b>Market:</b> {r.regime.upper()} ({strength_str})",
            f"{btc_emoji} BTC: {r.btc_bias} | {eth_emoji} ETH: {r.eth_bias}",
            f"🧭 Vol: {r.volatility_regime} | Risk: {r.risk_on_off} | BTC phase: {r.btc_phase}",
        ]

        # Add funding context if interesting
        if r.funding_sentiment == "long_heavy":
            lines.append("⚠️ Funding: Long-heavy (potential squeeze)")
        elif r.funding_sentiment == "short_heavy":
            lines.append("🚀 Funding: Short-heavy (short squeeze potential)")

        # Add alt season indicator
        if r.altcoin_season_index > 60:
            lines.append(f"🌙 Alt Season Index: {r.altcoin_season_index:.0f}/100")
        elif r.altcoin_season_index < 40:
            lines.append(f"👑 BTC Dominance: {100 - r.altcoin_season_index:.0f}/100")

        return "\n".join(lines)
