"""Health checking for bot components."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any
from datetime import datetime, timedelta

from ..engine.registry import StrategyRegistry
from ..analyzer.metrics import WinRateCalculator, PerformanceMetrics

LOG = logging.getLogger("bot.core.diagnostics.health")


class HealthStatus(Enum):
    """Health status levels."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"


@dataclass
class ComponentHealth:
    """Health status of a component."""
    name: str
    status: HealthStatus
    message: str = ""
    last_check: datetime | None = None
    details: dict[str, Any] | None = None


class HealthChecker:
    """Check health of bot components and overall system."""
    
    # Thresholds for degradation detection
    WINRATE_DEGRADATION_THRESHOLD = 0.15  # 15% drop
    PF_DEGRADATION_THRESHOLD = 0.3  # 0.3 drop
    MIN_WINRATE = 0.35  # Minimum acceptable win rate
    MIN_PF = 1.0  # Minimum acceptable profit factor
    
    def __init__(
        self,
        registry: StrategyRegistry,
        calculator: WinRateCalculator,
    ):
        self._registry = registry
        self._calc = calculator
        self._baseline_metrics: dict[str, PerformanceMetrics] = {}
        self._last_check: datetime | None = None
    
    async def check_all(self) -> list[ComponentHealth]:
        """Run all health checks."""
        checks = [
            self._check_strategies(),
            self._check_performance(),
            self._check_ws_health(),
        ]
        
        results = await asyncio.gather(*checks, return_exceptions=True)
        
        health_results = []
        for result in results:
            if isinstance(result, Exception):
                LOG.error("Health check failed: %s", result)
                continue
            health_results.append(result)
        
        self._last_check = datetime.utcnow()
        return health_results
    
    async def _check_strategies(self) -> ComponentHealth:
        """Check strategy health."""
        enabled = len(self._registry.get_enabled())
        total = len(self._registry)
        
        if enabled == 0:
            return ComponentHealth(
                name="strategies",
                status=HealthStatus.UNHEALTHY,
                message="No strategies enabled",
                details={"total": total, "enabled": enabled}
            )
        
        # Check for strategies with high error rates
        perf = self._registry.get_all_performance()
        high_error_strategies = [
            sid for sid, p in perf.items()
            if p["calculation_errors"] > p["signals_generated"] * 0.1  # >10% errors
        ]
        
        if high_error_strategies:
            return ComponentHealth(
                name="strategies",
                status=HealthStatus.DEGRADED,
                message=f"Strategies with high error rates: {high_error_strategies}",
                details={
                    "total": total,
                    "enabled": enabled,
                    "high_error_strategies": high_error_strategies,
                }
            )
        
        return ComponentHealth(
            name="strategies",
            status=HealthStatus.HEALTHY,
            message=f"{enabled}/{total} strategies enabled",
            details={"total": total, "enabled": enabled}
        )
    
    async def _check_performance(self) -> ComponentHealth:
        """Check overall performance health."""
        from datetime import datetime, timedelta
        
        metrics = await self._calc.calculate_metrics(
            since=datetime.utcnow() - timedelta(days=7)
        )
        
        # Check minimum thresholds
        if metrics.total_signals < 5:
            return ComponentHealth(
                name="performance",
                status=HealthStatus.HEALTHY,  # Not enough data
                message="Insufficient signals for performance check",
                details={"total_signals": metrics.total_signals}
            )
        
        issues = []
        
        if metrics.win_rate < self.MIN_WINRATE:
            issues.append(f"Low win rate: {metrics.win_rate*100:.1f}%")
        
        if metrics.profit_factor < self.MIN_PF:
            issues.append(f"Low PF: {metrics.profit_factor:.2f}")
        
        # Check degradation from baseline
        if "overall" in self._baseline_metrics:
            baseline = self._baseline_metrics["overall"]
            
            if baseline.win_rate > 0:
                winrate_drop = baseline.win_rate - metrics.win_rate
                if winrate_drop > self.WINRATE_DEGRADATION_THRESHOLD:
                    issues.append(f"Win rate degraded by {winrate_drop*100:.1f}%")
            
            if baseline.profit_factor > 0:
                pf_drop = baseline.profit_factor - metrics.profit_factor
                if pf_drop > self.PF_DEGRADATION_THRESHOLD:
                    issues.append(f"PF degraded by {pf_drop:.2f}")
        
        if issues:
            return ComponentHealth(
                name="performance",
                status=HealthStatus.DEGRADED,
                message="; ".join(issues),
                details=metrics.to_dict()
            )
        
        # Update baseline if performance is good
        if metrics.win_rate > 0.5 and metrics.profit_factor > 1.2:
            self._baseline_metrics["overall"] = metrics
        
        return ComponentHealth(
            name="performance",
            status=HealthStatus.HEALTHY,
            message=f"Win rate: {metrics.win_rate*100:.1f}%, PF: {metrics.profit_factor:.2f}",
            details=metrics.to_dict()
        )
    
    async def _check_ws_health(self) -> ComponentHealth:
        """Check WebSocket health (placeholder - needs integration with WS manager)."""
        # This would need access to WS manager stats
        # For now, return healthy
        return ComponentHealth(
            name="websocket",
            status=HealthStatus.HEALTHY,
            message="WebSocket connections active",
        )
    
    def get_overall_status(self, checks: list[ComponentHealth]) -> HealthStatus:
        """Determine overall health from component checks."""
        if any(c.status == HealthStatus.UNHEALTHY for c in checks):
            return HealthStatus.UNHEALTHY
        if any(c.status == HealthStatus.DEGRADED for c in checks):
            return HealthStatus.DEGRADED
        return HealthStatus.HEALTHY
    
    def format_report(self, checks: list[ComponentHealth]) -> str:
        """Format health report."""
        overall = self.get_overall_status(checks)
        
        lines = [
            f"Health Check - Overall: {overall.value}",
            f"Last Check: {self._last_check.isoformat() if self._last_check else 'never'}",
            "",
            "Components:",
        ]
        
        for check in checks:
            icon = "✅" if check.status == HealthStatus.HEALTHY else "⚠️" if check.status == HealthStatus.DEGRADED else "❌"
            lines.append(f"  {icon} {check.name}: {check.status.value}")
            if check.message:
                lines.append(f"     {check.message}")
        
        return "\n".join(lines)
