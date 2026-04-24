"""Alert management for diagnostics."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable
from collections import deque

from .health import HealthStatus, ComponentHealth

LOG = logging.getLogger("bot.core.diagnostics.alerts")


class AlertSeverity(Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


@dataclass
class Alert:
    """Alert notification."""
    id: str
    severity: AlertSeverity
    component: str
    message: str
    timestamp: datetime
    details: dict[str, Any] | None = None
    acknowledged: bool = False


class AlertManager:
    """Manage alerts and notifications.
    
    Features:
    - Alert deduplication (same alert within cooldown period)
    - Alert throttling
    - Multiple notification channels
    - Alert history
    """
    
    def __init__(
        self,
        cooldown_seconds: float = 300.0,  # 5 min default cooldown
        max_history: int = 1000,
    ):
        self._cooldown = cooldown_seconds
        self._handlers: list[Callable[[Alert], None]] = []
        self._recent_alerts: dict[str, datetime] = {}  # Alert ID -> last sent
        self._history: deque[Alert] = deque(maxlen=max_history)
        self._alert_count = 0
    
    def add_handler(self, handler: Callable[[Alert], None]) -> None:
        """Add notification handler."""
        self._handlers.append(handler)
    
    def create_alert(
        self,
        severity: AlertSeverity,
        component: str,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> Alert | None:
        """Create alert if not in cooldown.
        
        Returns None if alert is suppressed (cooldown active).
        """
        # Generate alert ID from component + message hash
        alert_id = f"{component}:{hash(message) % 10000000}"
        
        # Check cooldown
        now = datetime.utcnow()
        last_sent = self._recent_alerts.get(alert_id)
        
        if last_sent and (now - last_sent).total_seconds() < self._cooldown:
            LOG.debug("Alert suppressed (cooldown): %s", alert_id)
            return None
        
        # Create alert
        alert = Alert(
            id=alert_id,
            severity=severity,
            component=component,
            message=message,
            timestamp=now,
            details=details,
        )
        
        # Update cooldown tracker
        self._recent_alerts[alert_id] = now
        
        # Add to history
        self._history.append(alert)
        self._alert_count += 1
        
        # Send to handlers
        for handler in self._handlers:
            try:
                if asyncio.iscoroutinefunction(handler):
                    asyncio.create_task(handler(alert))
                else:
                    handler(alert)
            except Exception as exc:
                LOG.error("Alert handler failed: %s", exc)
        
        LOG.info("Alert created: [%s] %s - %s", severity.value, component, message)
        return alert
    
    def from_health_check(self, check: ComponentHealth) -> Alert | None:
        """Create alert from health check result."""
        if check.status == HealthStatus.HEALTHY:
            return None
        
        severity = (
            AlertSeverity.CRITICAL 
            if check.status == HealthStatus.UNHEALTHY 
            else AlertSeverity.WARNING
        )
        
        return self.create_alert(
            severity=severity,
            component=check.name,
            message=check.message or f"Status: {check.status.value}",
            details=check.details,
        )
    
    def from_degradation(
        self,
        component: str,
        metric_name: str,
        old_value: float,
        new_value: float,
        threshold: float,
    ) -> Alert | None:
        """Create alert from metric degradation."""
        change = abs(old_value - new_value)
        
        severity = (
            AlertSeverity.CRITICAL 
            if change > threshold * 1.5 
            else AlertSeverity.WARNING
        )
        
        direction = "dropped" if new_value < old_value else "increased"
        
        return self.create_alert(
            severity=severity,
            component=component,
            message=f"{metric_name} {direction}: {old_value:.3f} -> {new_value:.3f}",
            details={
                "metric": metric_name,
                "old_value": old_value,
                "new_value": new_value,
                "threshold": threshold,
                "change": change,
            }
        )
    
    def get_recent(
        self, 
        since: datetime | None = None,
        severity: AlertSeverity | None = None,
    ) -> list[Alert]:
        """Get recent alerts."""
        alerts = list(self._history)
        
        if since:
            alerts = [a for a in alerts if a.timestamp >= since]
        
        if severity:
            alerts = [a for a in alerts if a.severity == severity]
        
        return sorted(alerts, key=lambda a: a.timestamp, reverse=True)
    
    def get_unacknowledged(self) -> list[Alert]:
        """Get unacknowledged alerts."""
        return [a for a in self._history if not a.acknowledged]
    
    def acknowledge(self, alert_id: str) -> bool:
        """Acknowledge an alert."""
        for alert in self._history:
            if alert.id == alert_id:
                alert.acknowledged = True
                return True
        return False
    
    def clear_history(self) -> None:
        """Clear alert history."""
        self._history.clear()
        self._recent_alerts.clear()
    
    def get_stats(self) -> dict[str, Any]:
        """Get alert statistics."""
        unack = len(self.get_unacknowledged())
        
        by_severity = {
            AlertSeverity.INFO: 0,
            AlertSeverity.WARNING: 0,
            AlertSeverity.CRITICAL: 0,
        }
        
        for alert in self._history:
            by_severity[alert.severity] += 1
        
        return {
            "total_alerts": self._alert_count,
            "in_history": len(self._history),
            "unacknowledged": unack,
            "by_severity": {
                k.value: v for k, v in by_severity.items()
            },
        }
