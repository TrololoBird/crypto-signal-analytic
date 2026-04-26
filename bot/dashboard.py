"""FastAPI dashboard for signal bot monitoring."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TYPE_CHECKING

UTC = timezone.utc

if TYPE_CHECKING:
    from fastapi import FastAPI

try:
    from fastapi import FastAPI as _FastAPI
    from fastapi.middleware.cors import CORSMiddleware as _CORSMiddleware
    from fastapi.responses import HTMLResponse as _HTMLResponse

    HAS_FASTAPI = True
except ImportError:
    HAS_FASTAPI = False

LOG = logging.getLogger("bot.dashboard")


class BotDashboard:
    """FastAPI dashboard bound to the current bot process."""

    def __init__(self, bot: Any, port: int = 8080) -> None:
        self.bot = bot
        self.port = port
        self._enabled = HAS_FASTAPI
        self.app: FastAPI | None = None

        if not self._enabled:
            LOG.warning("fastapi not installed, dashboard disabled")
            return

        app = _FastAPI(title="Signal Bot Dashboard", version="2.0.0")
        app.add_middleware(
            _CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
        self.app = app
        self._setup_routes()

    def _setup_routes(self) -> None:
        if not self.app:
            return

        @self.app.get("/", response_class=_HTMLResponse)
        async def root() -> str:
            return self._get_html_dashboard()

        @self.app.get("/api/status")
        async def status() -> dict[str, Any]:
            return await self._get_status()

        @self.app.get("/api/signals/active")
        async def active_signals() -> list[dict[str, Any]]:
            return await self._get_active_signals()

        @self.app.get("/api/signals/recent")
        async def recent_signals(limit: int = 20) -> list[dict[str, Any]]:
            return self._get_recent_signals(limit)

        @self.app.get("/api/market/regime")
        async def market_regime() -> dict[str, Any]:
            return self._get_market_regime()

        @self.app.get("/api/metrics")
        async def metrics() -> dict[str, Any]:
            return await self._get_metrics()

        @self.app.get("/api/health")
        async def health() -> dict[str, Any]:
            return await self.bot.health_check()

        @self.app.get("/api/analytics/report")
        async def analytics_report(days: int = 30) -> dict[str, Any]:
            from .analytics import StrategyAnalytics

            days = max(1, min(int(days), 365))
            reporter = StrategyAnalytics(repo=self.bot._modern_repo)
            return await reporter.generate_report(days=days)

    def _get_html_dashboard(self) -> str:
        return """<!DOCTYPE html>
<html>
<head>
    <title>Signal Bot Dashboard</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 0; padding: 20px; background: #f5f5f5; }
        .container { max-width: 1200px; margin: 0 auto; }
        h1 { color: #333; margin-bottom: 30px; }
        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; }
        .card { background: white; border-radius: 8px; padding: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .card h2 { margin-top: 0; font-size: 18px; color: #666; }
        .metric { display: flex; justify-content: space-between; padding: 10px 0; border-bottom: 1px solid #eee; }
        .metric:last-child { border-bottom: none; }
        .value { font-weight: bold; color: #333; }
        .neutral { color: #6b7280; }
        .status-online { color: #22c55e; }
        .status-offline { color: #ef4444; }
    </style>
    <script>
        async function fetchStatus() {
            const res = await fetch('/api/status');
            const data = await res.json();
            document.getElementById('status').textContent = data.running ? 'Online' : 'Offline';
            document.getElementById('status').className = data.running ? 'status-online' : 'status-offline';
            document.getElementById('shortlist').textContent = data.shortlist_size;
            document.getElementById('signals').textContent = data.open_signals;
            document.getElementById('ws-latency').textContent = data.ws_latency_ms + ' ms';
            document.getElementById('market').textContent = data.market_regime;
        }
        async function fetchSignals() {
            const res = await fetch('/api/signals/active');
            const data = await res.json();
            const el = document.getElementById('active-signals');
            if (data.length === 0) {
                el.innerHTML = '<p class="neutral">No active signals</p>';
                return;
            }
            el.innerHTML = data.map(s => `
                <div class="metric">
                    <span>${s.symbol} ${s.direction}</span>
                    <span class="value">${s.setup_id}</span>
                </div>
            `).join('');
        }
        setInterval(fetchStatus, 5000);
        setInterval(fetchSignals, 10000);
        fetchStatus();
        fetchSignals();
    </script>
</head>
<body>
    <div class="container">
        <h1>Signal Bot Dashboard</h1>
        <div class="grid">
            <div class="card">
                <h2>Bot Status</h2>
                <div class="metric"><span>Status</span><span id="status" class="status-offline">Loading...</span></div>
                <div class="metric"><span>Shortlist Size</span><span id="shortlist" class="value">-</span></div>
                <div class="metric"><span>Open Signals</span><span id="signals" class="value">-</span></div>
                <div class="metric"><span>WS Latency</span><span id="ws-latency" class="value">-</span></div>
                <div class="metric"><span>Market Regime</span><span id="market" class="value">-</span></div>
            </div>
            <div class="card">
                <h2>Active Signals</h2>
                <div id="active-signals"><p class="neutral">Loading...</p></div>
            </div>
        </div>
    </div>
</body>
</html>"""

    async def _get_status(self) -> dict[str, Any]:
        bot = self.bot
        regime = bot.market_regime._last_result
        active_signals = await bot._modern_repo.get_active_signals()

        ws_lag = 0
        if bot._ws_manager is not None:
            stats = bot._ws_manager.get_stats()
            ws_lag = stats.get("avg_latency_overall_ms", 0) or 0

        return {
            "running": not bot._shutdown.is_set(),
            "shortlist_size": len(bot._shortlist),
            "open_signals": len(active_signals),
            "ws_latency_ms": ws_lag,
            "market_regime": regime.regime if regime else "unknown",
            "market_strength": regime.strength if regime else 0.0,
            "timestamp": datetime.now(UTC).isoformat(),
        }

    async def _get_active_signals(self) -> list[dict[str, Any]]:
        signals = await self.bot._modern_repo.get_active_signals()
        return [
            {
                "symbol": sig.get("symbol"),
                "setup_id": sig.get("setup_id"),
                "direction": sig.get("direction"),
                "entry_price": sig.get("entry_price"),
                "stop_price": sig.get("stop_price"),
                "tp1_price": sig.get("tp1_price"),
                "tp2_price": sig.get("tp2_price"),
                "score": sig.get("score"),
                "risk_reward": sig.get("risk_reward"),
                "status": sig.get("status"),
                "tracking_id": sig.get("tracking_id"),
            }
            for sig in signals
        ]

    def _get_recent_signals(self, limit: int = 20) -> list[dict[str, Any]]:
        telemetry_dir = self.bot.settings.telemetry_dir
        signals: list[dict[str, Any]] = []

        candidates_file = self._latest_analysis_file(telemetry_dir, "candidates.jsonl")
        if candidates_file is None:
            return signals

        try:
            with candidates_file.open("r", encoding="utf-8") as handle:
                lines = handle.readlines()
            for line in reversed(lines[-limit:]):
                if line.strip():
                    signals.append(json.loads(line))
        except Exception as exc:
            LOG.debug("failed to read recent candidates: %s", exc)

        return signals

    def _get_market_regime(self) -> dict[str, Any]:
        regime = self.bot.market_regime._last_result
        if not regime:
            return {"error": "No market data available"}
        return regime.to_dict()

    async def _get_metrics(self) -> dict[str, Any]:
        bot = self.bot
        active_signals = await bot._modern_repo.get_active_signals()
        return {
            "shortlist_size": len(bot._shortlist),
            "open_signals": len(active_signals),
            "ws_streams": len(bot._ws_manager._symbols) if bot._ws_manager else 0,
            "market_regime": bot.market_regime._last_result.to_dict() if bot.market_regime._last_result else None,
            "engine": bot._modern_engine.get_engine_stats(),
        }

    def _latest_analysis_file(self, telemetry_dir: Path, filename: str) -> Path | None:
        runs_dir = telemetry_dir / "runs"
        if not runs_dir.exists():
            return None
        candidates = sorted(
            runs_dir.glob(f"*/analysis/{filename}"),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        return candidates[0] if candidates else None

    def start_server(self) -> None:
        if not self._enabled or not self.app:
            LOG.debug("dashboard server disabled (fastapi not installed)")
            return

        from threading import Thread

        def run_server() -> None:
            if self.app is None:
                return
            try:
                import uvicorn  # type: ignore
            except Exception as exc:
                LOG.warning("dashboard server failed to import uvicorn: %s", exc)
                return
            try:
                uvicorn.run(self.app, host="0.0.0.0", port=self.port, log_level="warning")
            except Exception as exc:
                LOG.warning("dashboard server crashed: %s", exc)

        thread = Thread(target=run_server, daemon=True)
        thread.start()
        LOG.info("dashboard server started on port %d", self.port)
