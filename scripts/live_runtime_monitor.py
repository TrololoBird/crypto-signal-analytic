"""Live Runtime Monitor for SignalBot

Tracks bot performance in real-time during live trading session.
Usage: python -m scripts.live_runtime_monitor --duration 3600
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import time
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import structlog

from bot.diagnostics.runtime_analysis import parse_cycle_log_lines

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    force=True,
)
LOG = structlog.get_logger("scripts.live_runtime_monitor")


class RuntimeMonitor:
    """Monitors bot runtime and collects statistics."""
    
    def __init__(self, log_dir: Path = Path("data/bot/logs")):
        self.log_dir = log_dir
        self.start_time = datetime.now()
        self.stats = {
            "cycles": 0,
            "symbols_processed": set(),
            "candidates_total": 0,
            "delivered_total": 0,
            "rejected_total": 0,
            "detector_runs_total": 0,
            "errors": [],
            "symbols_with_candidates": [],
            "last_signals": [],
        }
        self.running = True
        
    async def monitor(self, duration_seconds: float, poll_interval: float = 5.0):
        """Monitor bot for specified duration."""
        LOG.info("Starting runtime monitor", duration=duration_seconds, poll_interval=poll_interval)
        
        end_time = self.start_time + timedelta(seconds=duration_seconds)
        
        while self.running and datetime.now() < end_time:
            await self._collect_snapshot()
            await asyncio.sleep(poll_interval)
        
        await self._generate_report()
    
    async def _collect_snapshot(self):
        """Collect current bot state."""
        # Check for latest log files
        log_files = list(self.log_dir.glob("*.log"))
        if not log_files:
            return
        
        # Get most recent log
        latest_log = max(log_files, key=lambda p: p.stat().st_mtime)
        
        try:
            # Read last lines
            lines = latest_log.read_text(encoding="utf-8").splitlines()[-100:]
            self._parse_log_lines(lines)
        except Exception as exc:
            LOG.warning("Failed to read log", error=str(exc))
    
    def _parse_log_lines(self, lines: list[str]):
        """Parse log lines for statistics."""
        parsed = parse_cycle_log_lines(lines)
        self.stats["cycles"] += parsed["cycles"]
        self.stats["symbols_processed"].update(parsed["symbols_processed"])
        self.stats["detector_runs_total"] += parsed["detector_runs_total"]
        self.stats["candidates_total"] += parsed["candidates_total"]
        self.stats["delivered_total"] += parsed["delivered_total"]
        self.stats["rejected_total"] += parsed["rejected_total"]

        now = datetime.now().isoformat()
        for item in parsed["symbols_with_candidates"]:
            self.stats["symbols_with_candidates"].append(
                {"symbol": item["symbol"], "candidates": item["candidates"], "time": now}
            )

        for item in parsed["last_signals"]:
            self.stats["last_signals"].append(
                {"symbol": item["symbol"], "delivered": item["delivered"], "time": now}
            )

        for line in parsed["errors"]:
            if len(self.stats["errors"]) >= 50:
                break
            self.stats["errors"].append({"line": line, "time": now})
    
    async def _generate_report(self):
        """Generate final runtime report."""
        runtime = datetime.now() - self.start_time
        
        report = {
            "monitoring_session": {
                "started_at": self.start_time.isoformat(),
                "ended_at": datetime.now().isoformat(),
                "duration_seconds": runtime.total_seconds(),
            },
            "statistics": {
                "total_cycles": self.stats["cycles"],
                "unique_symbols_processed": len(self.stats["symbols_processed"]),
                "detector_runs_total": self.stats["detector_runs_total"],
                "candidates_total": self.stats["candidates_total"],
                "delivered_total": self.stats["delivered_total"],
                "rejected_total": self.stats["rejected_total"],
                "errors_count": len(self.stats["errors"]),
            },
            "performance": {
                "cycles_per_minute": self.stats["cycles"] / (runtime.total_seconds() / 60) if runtime.total_seconds() > 0 else 0,
                "candidates_per_cycle": self.stats["candidates_total"] / self.stats["cycles"] if self.stats["cycles"] > 0 else 0,
                "delivery_rate": self.stats["delivered_total"] / self.stats["candidates_total"] if self.stats["candidates_total"] > 0 else 0,
            },
            "signals": {
                "symbols_with_candidates": self.stats["symbols_with_candidates"][-20:],  # Last 20
                "last_signals": self.stats["last_signals"][-10:],  # Last 10
            },
            "errors_sample": self.stats["errors"][:10],  # First 10 errors
        }
        
        # Save report
        report_path = Path("scripts/audit_data/runtime_report_") / f"{self.start_time.strftime('%Y%m%d_%H%M%S')}.json"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
        
        # Print summary
        self._print_summary(report)
        
        return report
    
    def _print_summary(self, report: dict):
        """Print human-readable summary."""
        print("\n" + "="*70)
        print("  LIVE RUNTIME MONITORING COMPLETE")
        print("="*70)
        
        stats = report["statistics"]
        perf = report["performance"]
        
        print(f"\n⏱ Duration: {report['monitoring_session']['duration_seconds']:.0f} seconds")
        print(f"🔄 Total Cycles: {stats['total_cycles']}")
        print(f"📊 Symbols Processed: {stats['unique_symbols_processed']}")
        print(f"🔍 Detector Runs: {stats['detector_runs_total']}")
        print(f"✅ Candidates Found: {stats['candidates_total']}")
        print(f"📤 Signals Delivered: {stats['delivered_total']}")
        print(f"❌ Rejected: {stats['rejected_total']}")
        print(f"⚠️ Errors: {stats['errors_count']}")
        
        print("\n📈 Performance:")
        print(f"  Cycles/minute: {perf['cycles_per_minute']:.1f}")
        print(f"  Candidates/cycle: {perf['candidates_per_cycle']:.2f}")
        print(f"  Delivery rate: {perf['delivery_rate']*100:.1f}%")
        
        signals = report["signals"]["last_signals"]
        if signals:
            print("\n🔔 Last Signals:")
            for sig in signals[-5:]:
                print(f"  {sig['time'][-8:]} | {sig['symbol']}: {sig['delivered']} delivered")
        
        errors = report["errors_sample"]
        if errors:
            print(f"\n⚠️ Sample Errors ({len(errors)} shown):")
            for err in errors[:3]:
                print(f"  {err['line'][:80]}...")
        
        print(f"\n📄 Full report saved to: {report.get('report_path', 'N/A')}")
        print("="*70 + "\n")


def main():
    parser = argparse.ArgumentParser(description="Live Bot Runtime Monitor")
    parser.add_argument("--duration", type=float, default=300, help="Monitoring duration in seconds")
    parser.add_argument("--poll", type=float, default=5.0, help="Polling interval in seconds")
    args = parser.parse_args()
    
    monitor = RuntimeMonitor()
    
    try:
        asyncio.run(monitor.monitor(args.duration, args.poll))
    except KeyboardInterrupt:
        LOG.info("Monitoring interrupted by user")
        asyncio.run(monitor._generate_report())


if __name__ == "__main__":
    main()
