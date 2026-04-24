"""Reporter task - generates periodic performance reports."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Callable

from ..core.analyzer import DailyReporter, ReportFormat
from ..core.diagnostics import AlertManager, AlertSeverity

LOG = logging.getLogger("bot.tasks.reporter")


class ReportTask:
    """Task that generates and sends periodic reports.
    
    Supports multiple output formats and destinations.
    """
    
    def __init__(
        self,
        reporter: DailyReporter,
        alert_manager: AlertManager,
        telegram_sender: Callable[[str], Any] | None = None,
    ):
        self._reporter = reporter
        self._alerts = alert_manager
        self._telegram = telegram_sender
    
    async def generate_daily(self) -> str | None:
        """Generate daily report.
        
        Returns:
            Formatted report text or None on error
        """
        try:
            report = await self._reporter.generate(
                date=datetime.utcnow(),
                format=ReportFormat.MARKDOWN,
            )
            
            # Check for degradation alerts
            if report.alerts:
                for alert_msg in report.alerts:
                    self._alerts.create_alert(
                        severity=AlertSeverity.WARNING,
                        component="daily_report",
                        message=alert_msg,
                    )
            
            # Format report
            formatted = self._reporter.format_report(report, ReportFormat.MARKDOWN)
            
            LOG.info("Generated daily report: %d signals, %.1f%% win rate",
                    report.overall_metrics.total_signals,
                    report.overall_metrics.win_rate * 100)
            
            return formatted
            
        except Exception as exc:
            LOG.error("Failed to generate daily report: %s", exc)
            self._alerts.create_alert(
                severity=AlertSeverity.WARNING,
                component="reporter",
                message=f"Daily report generation failed: {exc}",
            )
            return None
    
    async def send_report(self, report_text: str) -> bool:
        """Send report to configured destinations.
        
        Args:
            report_text: Formatted report
            
        Returns:
            True if sent successfully
        """
        success = True
        
        # Send via Telegram if configured
        if self._telegram:
            try:
                await self._telegram(report_text)
                LOG.info("Report sent via Telegram")
            except Exception as exc:
                LOG.error("Failed to send report via Telegram: %s", exc)
                success = False
        
        # Could add other channels here (email, Slack, etc.)
        
        return success
    
    async def run(self) -> None:
        """Execute one report cycle."""
        report = await self.generate_daily()
        
        if report and self._telegram:
            await self.send_report(report)
