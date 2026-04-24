"""Prometheus metrics collection for the signal bot.

Provides metrics endpoint for monitoring:
- Signal detection rates
- WebSocket latency
- Pipeline throughput
- Market regime
- Setup performance
"""

from __future__ import annotations

import logging
import time
from importlib import import_module
from dataclasses import dataclass
from typing import Any, cast

try:
    prometheus_client = import_module("prometheus_client")
    prometheus_core = import_module("prometheus_client.core")
    CollectorRegistry = cast(Any, prometheus_core.CollectorRegistry)
    REGISTRY = cast(Any, prometheus_client.REGISTRY)
    generate_latest = cast(Any, prometheus_client.generate_latest)
    _PromCounterClass = cast(Any, prometheus_client.Counter)
    _PromGaugeClass = cast(Any, prometheus_client.Gauge)
    _PromHistogramClass = cast(Any, prometheus_client.Histogram)
    _PromInfoClass = cast(Any, prometheus_client.Info)
    _prom_start_http_server = cast(Any, prometheus_client.start_http_server)
    HAS_PROMETHEUS = True
except ImportError:
    HAS_PROMETHEUS = False
    # Stubs for when prometheus_client is not installed
    class _PromCounterClass:  # type: ignore
        def __init__(self, *args: Any, **kwargs: Any) -> None: pass
        def inc(self, *args: Any, **kwargs: Any) -> None: pass
        def labels(self, *args: Any, **kwargs: Any) -> _PromCounterClass: return self
    class _PromGaugeClass:  # type: ignore
        def __init__(self, *args: Any, **kwargs: Any) -> None: pass
        def set(self, *args: Any, **kwargs: Any) -> None: pass
        def inc(self, *args: Any, **kwargs: Any) -> None: pass
        def dec(self, *args: Any, **kwargs: Any) -> None: pass
    class _PromHistogramClass:  # type: ignore
        def __init__(self, *args: Any, **kwargs: Any) -> None: pass
        def observe(self, *args: Any, **kwargs: Any) -> None: pass
    class _PromInfoClass:  # type: ignore
        def __init__(self, *args: Any, **kwargs: Any) -> None: pass
        def info(self, *args: Any, **kwargs: Any) -> None: pass
    def _prom_start_http_server(*args: Any, **kwargs: Any) -> None: pass
    def generate_latest(*args: Any, **kwargs: Any) -> bytes: return b""
    REGISTRY = None  # type: ignore

PromCounter = cast(Any, _PromCounterClass)
PromGauge = cast(Any, _PromGaugeClass)
PromHistogram = cast(Any, _PromHistogramClass)
PromInfo = cast(Any, _PromInfoClass)
prom_start_http_server = cast(Any, _prom_start_http_server)


LOG = logging.getLogger("bot.metrics")


@dataclass
class SignalMetrics:
    """Container for signal-related metrics snapshots."""
    total_detected: int
    total_delivered: int
    total_rejected: int
    by_setup: dict[str, dict[str, int]]
    avg_score: float
    avg_risk_reward: float


class BotMetricsCollector:
    """Collects and exposes Prometheus metrics for the signal bot."""

    def __init__(self, port: int = 9090) -> None:
        self.port = port
        self._enabled = HAS_PROMETHEUS

        if not self._enabled:
            LOG.warning("prometheus_client not installed, metrics disabled")
            return

        # Info
        self._info: Any = PromInfo("bot", "Signal bot information")
        self._info.info({"version": "2.0.0", "type": "signal_only"})

        # Signal counters
        self.signals_detected: Any = PromCounter(
            "bot_signals_detected_total",
            "Total signals detected",
            ["setup_id", "direction"]
        )
        self.signals_delivered: Any = PromCounter(
            "bot_signals_delivered_total",
            "Total signals delivered to Telegram",
            ["setup_id", "direction"]
        )
        self.signals_rejected: Any = PromCounter(
            "bot_signals_rejected_total",
            "Total signals rejected by filters",
            ["setup_id", "direction", "reason"]
        )

        # Signal tracking outcomes
        self.signal_outcomes: Any = PromCounter(
            "bot_signal_outcomes_total",
            "Signal outcome results",
            ["setup_id", "direction", "outcome"]  # outcome: tp1, tp2, sl, expired
        )

        # WebSocket metrics
        self.ws_latency_ms: Any = PromGauge(
            "bot_ws_latency_milliseconds",
            "WebSocket message latency"
        )
        self.ws_message_age_seconds: Any = PromGauge(
            "bot_ws_message_age_seconds",
            "Age of last WS message"
        )
        self.ws_streams_active: Any = PromGauge(
            "bot_ws_streams_active",
            "Number of active WebSocket streams"
        )
        self.ws_reconnects: Any = PromCounter(
            "bot_ws_reconnects_total",
            "Total WebSocket reconnections"
        )

        # Pipeline performance
        self.pipeline_duration_seconds: Any = PromHistogram(
            "bot_pipeline_duration_seconds",
            "Pipeline processing time",
            buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]
        )
        self.pipeline_symbols_processed: Any = PromCounter(
            "bot_pipeline_symbols_processed_total",
            "Total symbols processed"
        )

        # Market metrics
        self.market_regime: Any = PromGauge(
            "bot_market_regime",
            "Current market regime (0=bear, 1=ranging, 2=bull, 3=volatile)"
        )
        self.market_regime_strength: Any = PromGauge(
            "bot_market_regime_strength",
            "Market regime strength (0.0-1.0)"
        )
        self.altcoin_season_index: Any = PromGauge(
            "bot_altcoin_season_index",
            "Altcoin season index (0-100)"
        )

        # Bot state
        self.shortlist_size: Any = PromGauge(
            "bot_shortlist_size",
            "Number of symbols in shortlist"
        )
        self.open_signals: Any = PromGauge(
            "bot_open_signals",
            "Number of currently tracked signals"
        )
        self.memory_blacklist_size: Any = PromGauge(
            "bot_memory_blacklist_size",
            "Number of blacklisted symbols"
        )

        # Scoring metrics
        self.signal_score_histogram: Any = PromHistogram(
            "bot_signal_score",
            "Distribution of signal scores",
            buckets=[0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
        )
        self.signal_risk_reward: Any = PromHistogram(
            "bot_signal_risk_reward",
            "Distribution of risk/reward ratios",
            buckets=[1.0, 1.5, 2.0, 2.5, 3.0, 4.0, 5.0]
        )

        # ML metrics
        self.ml_predictions: Any = PromCounter(
            "bot_ml_predictions_total",
            "ML predictions made",
            ["confidence_bucket"]  # low, medium, high
        )
        self.ml_score_adjustment: Any = PromHistogram(
            "bot_ml_score_adjustment",
            "ML score adjustments applied",
            buckets=[-0.2, -0.1, 0, 0.1, 0.2]
        )

        LOG.info("metrics collector initialized on port %d", port)

    def start_server(self) -> None:
        """Start the Prometheus HTTP server."""
        if not self._enabled:
            LOG.debug("metrics server disabled (prometheus_client not installed)")
            return
        try:
            prom_start_http_server(self.port)
            LOG.info("metrics server started on port %d", self.port)
        except Exception as exc:
            LOG.error("failed to start metrics server: %s", exc)

    def record_signal_detected(
        self,
        setup_id: str,
        direction: str,
        score: float,
        risk_reward: float,
    ) -> None:
        """Record a detected signal."""
        if not self._enabled:
            return
        self.signals_detected.labels(setup_id=setup_id, direction=direction).inc()
        self.signal_score_histogram.observe(score)
        self.signal_risk_reward.observe(risk_reward)

    def record_signal_delivered(self, setup_id: str, direction: str) -> None:
        """Record a delivered signal."""
        if not self._enabled:
            return
        self.signals_delivered.labels(setup_id=setup_id, direction=direction).inc()

    def record_signal_rejected(
        self,
        setup_id: str,
        direction: str,
        reason: str,
    ) -> None:
        """Record a rejected signal."""
        if not self._enabled:
            return
        # Truncate reason to avoid label explosion
        short_reason = reason[:50] if len(reason) > 50 else reason
        self.signals_rejected.labels(
            setup_id=setup_id,
            direction=direction,
            reason=short_reason
        ).inc()

    def record_signal_outcome(
        self,
        setup_id: str,
        direction: str,
        outcome: str,
    ) -> None:
        """Record a signal outcome (tp1, tp2, sl, expired)."""
        if not self._enabled:
            return
        self.signal_outcomes.labels(
            setup_id=setup_id,
            direction=direction,
            outcome=outcome
        ).inc()

    def record_ws_latency(self, latency_ms: float) -> None:
        """Record WebSocket latency."""
        if not self._enabled:
            return
        self.ws_latency_ms.set(latency_ms)

    def record_ws_message_age(self, age_seconds: float) -> None:
        """Record WebSocket message age."""
        if not self._enabled:
            return
        self.ws_message_age_seconds.set(age_seconds)

    def record_ws_reconnect(self) -> None:
        """Record WebSocket reconnection."""
        if not self._enabled:
            return
        self.ws_reconnects.inc()

    def update_ws_streams(self, count: int) -> None:
        """Update active WebSocket streams count."""
        if not self._enabled:
            return
        self.ws_streams_active.set(count)

    def record_pipeline_duration(self, duration_seconds: float) -> None:
        """Record pipeline processing duration."""
        if not self._enabled:
            return
        self.pipeline_duration_seconds.observe(duration_seconds)

    def record_symbol_processed(self) -> None:
        """Record a symbol being processed."""
        if not self._enabled:
            return
        self.pipeline_symbols_processed.inc()

    def update_market_regime(
        self,
        regime: str,
        strength: float,
        altcoin_index: float,
    ) -> None:
        """Update market regime metrics."""
        if not self._enabled:
            return
        regime_map = {"bear": 0, "ranging": 1, "bull": 2, "volatile": 3}
        self.market_regime.set(regime_map.get(regime, 1))
        self.market_regime_strength.set(strength)
        self.altcoin_season_index.set(altcoin_index)

    def update_bot_state(
        self,
        shortlist_size: int,
        open_signals: int,
        blacklist_size: int,
    ) -> None:
        """Update bot state metrics."""
        if not self._enabled:
            return
        self.shortlist_size.set(shortlist_size)
        self.open_signals.set(open_signals)
        self.memory_blacklist_size.set(blacklist_size)

    def record_ml_prediction(self, confidence: float) -> None:
        """Record ML prediction confidence."""
        if not self._enabled:
            return
        if confidence < 0.5:
            bucket = "low"
        elif confidence < 0.7:
            bucket = "medium"
        else:
            bucket = "high"
        self.ml_predictions.labels(confidence_bucket=bucket).inc()

    def record_ml_adjustment(self, adjustment: float) -> None:
        """Record ML score adjustment."""
        if not self._enabled:
            return
        self.ml_score_adjustment.observe(adjustment)

    def get_metrics_text(self) -> bytes:
        """Get current metrics in Prometheus text format."""
        if not self._enabled or REGISTRY is None:
            return b"# prometheus_client not installed\n"
        return generate_latest(REGISTRY)
