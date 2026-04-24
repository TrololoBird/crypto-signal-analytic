"""Daily reporter for signal performance."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

from .metrics import PerformanceMetrics, WinRateCalculator
from ..memory.repository import MemoryRepository

LOG = logging.getLogger("bot.core.analyzer.reporter")


class ReportFormat(Enum):
    """Report output format."""
    TEXT = "text"
    HTML = "html"
    MARKDOWN = "markdown"
    JSON = "json"


@dataclass
class DailyReport:
    """Daily performance report."""
    
    date: datetime
    overall_metrics: PerformanceMetrics
    strategy_metrics: dict[str, PerformanceMetrics]
    top_signals: list[dict[str, Any]]
    alerts: list[str]
    
    def to_text(self) -> str:
        """Generate text report."""
        lines = [
            f"📊 Daily Report - {self.date.strftime('%Y-%m-%d')}",
            "",
            "Overall Performance:",
            f"  Signals: {self.overall_metrics.total_signals}",
            f"  Win Rate: {self.overall_metrics.win_rate*100:.1f}%",
            f"  Profit Factor: {self.overall_metrics.profit_factor:.2f}",
            f"  Sharpe: {self.overall_metrics.sharpe_ratio:.2f}",
            "",
            "By Strategy:",
        ]
        
        for strategy_id, metrics in self.strategy_metrics.items():
            lines.append(f"  {strategy_id}:")
            lines.append(f"    Signals: {metrics.total_signals}")
            lines.append(f"    Win Rate: {metrics.win_rate*100:.1f}%")
            lines.append(f"    PF: {metrics.profit_factor:.2f}")
        
        if self.alerts:
            lines.extend(["", "⚠️ Alerts:", *self.alerts])
        
        return "\n".join(lines)
    
    def to_markdown(self) -> str:
        """Generate markdown report."""
        lines = [
            f"## 📊 Daily Report - {self.date.strftime('%Y-%m-%d')}",
            "",
            "### Overall Performance",
            "",
            "| Metric | Value |",
            "|--------|-------|",
            f"| Total Signals | {self.overall_metrics.total_signals} |",
            f"| Win Rate | {self.overall_metrics.win_rate*100:.1f}% |",
            f"| Profit Factor | {self.overall_metrics.profit_factor:.2f} |",
            f"| Sharpe Ratio | {self.overall_metrics.sharpe_ratio:.2f} |",
            f"| Avg Win | {self.overall_metrics.avg_win_pct:.2f}% |",
            f"| Avg Loss | {self.overall_metrics.avg_loss_pct:.2f}% |",
            "",
            "### By Strategy",
            "",
            "| Strategy | Signals | Win Rate | PF |",
            "|----------|---------|----------|-----|",
        ]
        
        for strategy_id, metrics in self.strategy_metrics.items():
            lines.append(
                f"| {strategy_id} | {metrics.total_signals} | "
                f"{metrics.win_rate*100:.1f}% | {metrics.profit_factor:.2f} |"
            )
        
        if self.alerts:
            lines.extend(["", "### ⚠️ Alerts", ""])
            for alert in self.alerts:
                lines.append(f"- {alert}")
        
        return "\n".join(lines)
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "date": self.date.isoformat(),
            "overall": self.overall_metrics.to_dict(),
            "strategies": {
                k: v.to_dict() for k, v in self.strategy_metrics.items()
            },
            "alerts": self.alerts,
        }


class DailyReporter:
    """Generate daily performance reports."""
    
    def __init__(
        self, 
        repository: MemoryRepository,
        calculator: WinRateCalculator,
    ):
        self._repo = repository
        self._calc = calculator
    
    async def generate(
        self,
        date: datetime | None = None,
        format: ReportFormat = ReportFormat.MARKDOWN,
    ) -> DailyReport:
        """Generate daily report.
        
        Args:
            date: Report date (defaults to today)
            format: Output format
            
        Returns:
            DailyReport with metrics and alerts
        """
        if date is None:
            date = datetime.utcnow()
        
        # Calculate day bounds
        day_start = date.replace(hour=0, minute=0, second=0, microsecond=0)
        day_end = day_start + timedelta(days=1)
        
        # Overall metrics for the day
        overall = await self._calc.calculate_metrics(
            since=day_start,
            until=day_end,
        )
        
        # Per-strategy metrics
        by_strategy = await self._calc.calculate_by_strategy(since=day_start)
        
        # Detect degradation alerts
        alerts = await self._check_alerts(by_strategy)
        
        report = DailyReport(
            date=date,
            overall_metrics=overall,
            strategy_metrics=by_strategy,
            top_signals=[],  # TODO: Add top performing signals
            alerts=alerts,
        )
        
        LOG.info("Generated daily report for %s", date.strftime("%Y-%m-%d"))
        return report
    
    async def _check_alerts(
        self,
        strategy_metrics: dict[str, PerformanceMetrics],
    ) -> list[str]:
        """Check for performance degradation alerts."""
        alerts = []
        
        for strategy_id, metrics in strategy_metrics.items():
            # Low win rate alert
            if metrics.total_signals >= 10 and metrics.win_rate < 0.4:
                alerts.append(
                    f"{strategy_id}: Low win rate ({metrics.win_rate*100:.1f}%)"
                )
            
            # Low profit factor alert
            if metrics.total_signals >= 10 and metrics.profit_factor < 1.0:
                alerts.append(
                    f"{strategy_id}: Negative expectancy (PF={metrics.profit_factor:.2f})"
                )
            
            # No signals alert
            if metrics.total_signals == 0:
                alerts.append(f"{strategy_id}: No signals generated")
        
        return alerts
    
    def format_report(
        self, 
        report: DailyReport, 
        format: ReportFormat = ReportFormat.MARKDOWN
    ) -> str:
        """Format report to specified output."""
        if format == ReportFormat.TEXT:
            return report.to_text()
        elif format == ReportFormat.MARKDOWN:
            return report.to_markdown()
        elif format == ReportFormat.JSON:
            import json
            return json.dumps(report.to_dict(), indent=2)
        else:
            raise ValueError(f"Unsupported format: {format}")
