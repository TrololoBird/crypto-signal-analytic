"""Prometheus-style metrics exporter for bot diagnostics."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any
from collections import deque

LOG = logging.getLogger("bot.core.diagnostics.metrics")


@dataclass
class Gauge:
    """Simple gauge metric."""
    name: str
    description: str
    value: float = 0.0
    labels: dict[str, str] = field(default_factory=dict)
    
    def set(self, value: float) -> None:
        self.value = value
    
    def inc(self, amount: float = 1.0) -> None:
        self.value += amount
    
    def dec(self, amount: float = 1.0) -> None:
        self.value -= amount


@dataclass
class Counter:
    """Simple counter metric (monotonically increasing)."""
    name: str
    description: str
    value: float = 0.0
    labels: dict[str, str] = field(default_factory=dict)
    
    def inc(self, amount: float = 1.0) -> None:
        self.value += amount


@dataclass
class Histogram:
    """Histogram metric with buckets."""
    name: str
    description: str
    buckets: list[float] = field(default_factory=lambda: [0.1, 0.5, 1.0, 2.5, 5.0, 10.0])
    counts: dict[float, int] = field(default_factory=dict)
    sum_val: float = 0.0
    count: int = 0
    
    def observe(self, value: float) -> None:
        self.sum_val += value
        self.count += 1
        
        for bucket in self.buckets:
            if value <= bucket:
                self.counts[bucket] = self.counts.get(bucket, 0) + 1


@dataclass
class TimeSeries:
    """Time series data point."""
    timestamp: float
    value: float


class TimeSeriesGauge:
    """Gauge with time-series history."""
    
    def __init__(self, name: str, description: str, max_history: int = 1000):
        self.name = name
        self.description = description
        self._current: float = 0.0
        self._history: deque[TimeSeries] = deque(maxlen=max_history)
    
    def set(self, value: float) -> None:
        self._current = value
        self._history.append(TimeSeries(time.time(), value))
    
    @property
    def value(self) -> float:
        return self._current
    
    def get_history(self, duration_seconds: float) -> list[TimeSeries]:
        """Get history for last N seconds."""
        cutoff = time.time() - duration_seconds
        return [ts for ts in self._history if ts.timestamp >= cutoff]


class BotMetrics:
    """Collection of bot metrics."""
    
    def __init__(self):
        # Signal metrics
        self.signals_generated = Counter("signals_total", "Total signals generated")
        self.signals_by_strategy: dict[str, Counter] = {}
        self.signal_score = Histogram("signal_score", "Signal confidence scores", buckets=[0.5, 0.6, 0.7, 0.8, 0.9])
        
        # Performance metrics
        self.winrate_24h = Gauge("winrate_24h", "24h rolling win rate")
        self.profit_factor = Gauge("profit_factor", "Current profit factor")
        self.sharpe_ratio = Gauge("sharpe_ratio", "Sharpe ratio")
        
        # Strategy performance
        self.strategy_calc_time = Histogram("strategy_calc_ms", "Strategy calculation time (ms)", buckets=[10, 50, 100, 250, 500, 1000])
        self.strategy_errors = Counter("strategy_errors_total", "Strategy calculation errors")
        
        # WebSocket metrics
        self.ws_reconnects = Counter("ws_reconnects_total", "WebSocket reconnect count")
        self.ws_messages = Counter("ws_messages_total", "WebSocket messages received")
        self.ws_latency_ms = Histogram("ws_latency_ms", "WebSocket message latency", buckets=[10, 50, 100, 250, 500])
        
        # Market data metrics
        self.rest_requests = Counter("rest_requests_total", "REST API requests")
        self.rest_errors = Counter("rest_errors_total", "REST API errors")
        self.rate_limit_hits = Counter("rate_limit_hits_total", "Rate limit hits")
        
        # System health
        self.memory_usage_mb = Gauge("memory_usage_mb", "Memory usage in MB")
        self.active_symbols = Gauge("active_symbols", "Number of active symbols")
        self.enabled_strategies = Gauge("enabled_strategies", "Number of enabled strategies")
        
        # Telegram metrics
        self.telegram_sent = Counter("telegram_sent_total", "Messages sent to Telegram")
        self.telegram_errors = Counter("telegram_errors_total", "Telegram send errors")
        self.telegram_queue_size = Gauge("telegram_queue_size", "Current Telegram queue size")
    
    def record_signal(self, strategy_id: str, score: float) -> None:
        """Record signal generation."""
        self.signals_generated.inc()
        self.signal_score.observe(score)
        
        # Track by strategy
        if strategy_id not in self.signals_by_strategy:
            self.signals_by_strategy[strategy_id] = Counter(
                f"signals_strategy_{strategy_id}",
                f"Signals for {strategy_id}"
            )
        self.signals_by_strategy[strategy_id].inc()
    
    def record_strategy_calc(self, duration_ms: float, error: bool = False) -> None:
        """Record strategy calculation."""
        self.strategy_calc_time.observe(duration_ms)
        if error:
            self.strategy_errors.inc()
    
    def record_ws_reconnect(self) -> None:
        """Record WebSocket reconnect."""
        self.ws_reconnects.inc()
    
    def record_ws_message(self, latency_ms: float) -> None:
        """Record WebSocket message."""
        self.ws_messages.inc()
        self.ws_latency_ms.observe(latency_ms)
    
    def record_rest_request(self, error: bool = False) -> None:
        """Record REST API request."""
        self.rest_requests.inc()
        if error:
            self.rest_errors.inc()
    
    def record_telegram_sent(self, error: bool = False) -> None:
        """Record Telegram message."""
        if error:
            self.telegram_errors.inc()
        else:
            self.telegram_sent.inc()
    
    def update_performance(self, winrate: float, pf: float, sharpe: float) -> None:
        """Update performance metrics."""
        self.winrate_24h.set(winrate)
        self.profit_factor.set(pf)
        self.sharpe_ratio.set(sharpe)
    
    def to_dict(self) -> dict[str, Any]:
        """Export all metrics as dictionary."""
        return {
            "signals_total": self.signals_generated.value,
            "winrate_24h": self.winrate_24h.value,
            "profit_factor": self.profit_factor.value,
            "sharpe_ratio": self.sharpe_ratio.value,
            "strategy_errors": self.strategy_errors.value,
            "ws_reconnects": self.ws_reconnects.value,
            "ws_messages": self.ws_messages.value,
            "rest_requests": self.rest_requests.value,
            "rest_errors": self.rest_errors.value,
            "rate_limit_hits": self.rate_limit_hits.value,
            "telegram_sent": self.telegram_sent.value,
            "telegram_errors": self.telegram_errors.value,
            "active_symbols": self.active_symbols.value,
            "enabled_strategies": self.enabled_strategies.value,
        }


class MetricsExporter:
    """Export metrics in Prometheus format."""
    
    def __init__(self, metrics: BotMetrics):
        self._metrics = metrics
    
    def export_prometheus(self) -> str:
        """Export metrics in Prometheus text format."""
        lines = []
        
        # Helper to format labels
        def format_labels(labels: dict[str, str]) -> str:
            if not labels:
                return ""
            pairs = [f'{k}="{v}"' for k, v in labels.items()]
            return "{" + ",".join(pairs) + "}"
        
        # Export all counters and gauges
        for name, metric in self._metrics.__dict__.items():
            if isinstance(metric, (Counter, Gauge)):
                lines.append(f"# HELP {metric.name} {metric.description}")
                lines.append(f"# TYPE {metric.name} {'counter' if isinstance(metric, Counter) else 'gauge'}")
                labels_str = format_labels(metric.labels)
                lines.append(f"{metric.name}{labels_str} {metric.value}")
        
        return "\n".join(lines)
    
    def export_json(self) -> dict[str, Any]:
        """Export metrics as JSON."""
        return self._metrics.to_dict()
