"""Strategy performance analytics built on persisted signal outcomes."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .core.memory.repository import MemoryRepository

UTC = timezone.utc


@dataclass(slots=True)
class StrategyAnalytics:
    repo: MemoryRepository

    async def generate_report(self, days: int = 30) -> dict[str, Any]:
        setup_rows = await self.repo.get_setup_stats(last_days=days)
        outcomes = await self.repo.get_signal_outcomes(last_days=days)
        by_setup: dict[str, list[dict[str, Any]]] = {}
        for row in outcomes:
            setup_id = str(row.get("setup_id") or "unknown")
            by_setup.setdefault(setup_id, []).append(row)

        setup_reports: list[dict[str, Any]] = []
        for setup in setup_rows:
            setup_id = str(setup.get("setup_id") or "unknown")
            rows = by_setup.get(setup_id, [])
            gross_profit = sum(max(float(r.get("pnl_r_multiple") or 0.0), 0.0) for r in rows)
            gross_loss = sum(abs(min(float(r.get("pnl_r_multiple") or 0.0), 0.0)) for r in rows)
            profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else None

            curve = 0.0
            peak = 0.0
            max_drawdown = 0.0
            for r in rows:
                curve += float(r.get("pnl_r_multiple") or 0.0)
                peak = max(peak, curve)
                max_drawdown = min(max_drawdown, curve - peak)

            expectancy = float(setup.get("avg_r_multiple") or 0.0)
            setup_reports.append(
                {
                    "setup_id": setup_id,
                    "trades": int(setup.get("total") or 0),
                    "win_rate": round(float(setup.get("win_rate") or 0.0), 4),
                    "expectancy_r": round(expectancy, 4),
                    "profit_factor": None if profit_factor is None else round(profit_factor, 4),
                    "max_drawdown_r": round(max_drawdown, 4),
                }
            )

        return {
            "generated_at": datetime.now(UTC).isoformat(),
            "window_days": int(days),
            "setup_reports": sorted(setup_reports, key=lambda r: r["setup_id"]),
            "total_trades": sum(int(r["trades"]) for r in setup_reports),
        }
