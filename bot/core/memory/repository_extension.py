"""Extension methods for MemoryRepository to replace BotMemory functionality."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any, cast

import aiosqlite


class MemoryRepositoryExtension:
    """Extension mixin for MemoryRepository with market context and stats."""
    _conn: aiosqlite.Connection | None

    def _require_conn(self) -> aiosqlite.Connection:
        conn = cast(aiosqlite.Connection | None, self._conn)
        if conn is None:
            raise RuntimeError("Repository not initialized")
        return conn

    async def _ensure_extended_tables(self) -> None:
        """Create additional tables for market context and stats."""
        conn = self._require_conn()

        # Market context table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS market_context (
                id INTEGER PRIMARY KEY,
                btc_bias TEXT DEFAULT 'neutral',
                eth_bias TEXT DEFAULT 'neutral',
                high_funding_symbols TEXT DEFAULT '[]',
                low_funding_symbols TEXT DEFAULT '[]',
                updated_at TEXT,
                market_regime TEXT DEFAULT 'unknown',
                market_regime_confirmed INTEGER DEFAULT 0,
                macro_risk_mode TEXT DEFAULT 'normal',
                intelligence_json TEXT DEFAULT '{}'
            )
        """)
        async with conn.execute("PRAGMA table_info(market_context)") as cursor:
            existing_columns = {str(row["name"]) for row in await cursor.fetchall()}
        for column_name, column_sql in (
            ("market_regime", "ALTER TABLE market_context ADD COLUMN market_regime TEXT DEFAULT 'unknown'"),
            (
                "market_regime_confirmed",
                "ALTER TABLE market_context ADD COLUMN market_regime_confirmed INTEGER DEFAULT 0",
            ),
            ("macro_risk_mode", "ALTER TABLE market_context ADD COLUMN macro_risk_mode TEXT DEFAULT 'normal'"),
            ("intelligence_json", "ALTER TABLE market_context ADD COLUMN intelligence_json TEXT DEFAULT '{}'"),
        ):
            if column_name not in existing_columns:
                await conn.execute(column_sql)
        
        # Symbol stats table (for blacklist)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS symbol_stats (
                symbol TEXT PRIMARY KEY,
                total_signals INTEGER DEFAULT 0,
                tp1_hits INTEGER DEFAULT 0,
                tp2_hits INTEGER DEFAULT 0,
                sl_hits INTEGER DEFAULT 0,
                consecutive_sl INTEGER DEFAULT 0,
                last_signal_ts TEXT
            )
        """)
        
        # Setup stats table (for score multiplier)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS setup_stats (
                setup_key TEXT PRIMARY KEY,
                setup_id TEXT,
                direction TEXT,
                regime TEXT,
                wins INTEGER DEFAULT 0,
                losses INTEGER DEFAULT 0
            )
        """)
        
        await conn.commit()

    async def update_market_context(
        self,
        btc_bias: str,
        eth_bias: str,
        high_funding_symbols: list[str],
        low_funding_symbols: list[str],
        *,
        market_regime: str = "unknown",
        market_regime_confirmed: bool = False,
        macro_risk_mode: str = "normal",
        intelligence_snapshot: dict[str, Any] | None = None,
    ) -> None:
        """Update market context in SQLite."""
        conn = self._require_conn()
        await self._ensure_extended_tables()

        await conn.execute("""
            INSERT OR REPLACE INTO market_context 
            (
                id,
                btc_bias,
                eth_bias,
                high_funding_symbols,
                low_funding_symbols,
                updated_at,
                market_regime,
                market_regime_confirmed,
                macro_risk_mode,
                intelligence_json
            )
            VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            btc_bias, eth_bias,
            json.dumps(high_funding_symbols),
            json.dumps(low_funding_symbols),
            datetime.now(timezone.utc).isoformat(),
            market_regime,
            1 if market_regime_confirmed else 0,
            macro_risk_mode,
            json.dumps(intelligence_snapshot or {}, ensure_ascii=True),
        ))
        await conn.commit()

    async def get_market_context(self) -> dict[str, Any]:
        """Get current market context."""
        conn = self._require_conn()
        await self._ensure_extended_tables()

        async with conn.execute(
            "SELECT * FROM market_context WHERE id = 1"
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                intelligence_snapshot: dict[str, Any] = {}
                raw_intelligence = row["intelligence_json"] if "intelligence_json" in row.keys() else None
                if raw_intelligence:
                    try:
                        intelligence_snapshot = json.loads(raw_intelligence)
                    except json.JSONDecodeError:
                        intelligence_snapshot = {}
                return {
                    "btc_bias": row["btc_bias"],
                    "eth_bias": row["eth_bias"],
                    "high_funding_symbols": json.loads(row["high_funding_symbols"]),
                    "low_funding_symbols": json.loads(row["low_funding_symbols"]),
                    "updated_at": row["updated_at"],
                    "market_regime": row["market_regime"] if "market_regime" in row.keys() else "unknown",
                    "market_regime_confirmed": bool(
                        row["market_regime_confirmed"]
                    ) if "market_regime_confirmed" in row.keys() else False,
                    "macro_risk_mode": row["macro_risk_mode"] if "macro_risk_mode" in row.keys() else "normal",
                    "intelligence_snapshot": intelligence_snapshot,
                }
            return {
                "btc_bias": "neutral",
                "eth_bias": "neutral",
                "high_funding_symbols": [],
                "low_funding_symbols": [],
                "market_regime": "unknown",
                "market_regime_confirmed": False,
                "macro_risk_mode": "normal",
                "intelligence_snapshot": {},
            }
    
    async def record_symbol_outcome(
        self,
        symbol: str,
        setup_id: str,
        direction: str,
        regime: str,
        outcome: str,
    ) -> None:
        """Record outcome for symbol/setup stats."""
        conn = self._require_conn()
        await self._ensure_extended_tables()

        # Update symbol stats
        await conn.execute("""
            INSERT INTO symbol_stats (symbol, total_signals, tp1_hits, tp2_hits, sl_hits, consecutive_sl, last_signal_ts)
            VALUES (?, 1, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol) DO UPDATE SET
                total_signals = total_signals + 1,
                tp1_hits = tp1_hits + ?,
                tp2_hits = tp2_hits + ?,
                sl_hits = sl_hits + ?,
                consecutive_sl = CASE WHEN ? = 'loss' THEN consecutive_sl + 1 ELSE 0 END,
                last_signal_ts = ?
        """, (
            symbol,
            1 if outcome in ("tp1", "tp2") else 0,
            1 if outcome == "tp2" else 0,
            1 if outcome == "loss" else 0,
            1 if outcome == "loss" else 0,
            datetime.now(timezone.utc).isoformat(),
            1 if outcome in ("tp1", "tp2") else 0,
            1 if outcome == "tp2" else 0,
            1 if outcome == "loss" else 0,
            outcome,
            datetime.now(timezone.utc).isoformat(),
        ))
        
        # Update setup stats
        key = f"{setup_id}|{direction}|{regime}"
        is_win = outcome in ("tp1", "tp2")
        is_loss = outcome == "loss"

        await conn.execute("""
            INSERT INTO setup_stats (setup_key, setup_id, direction, regime, wins, losses)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(setup_key) DO UPDATE SET
                wins = wins + ?,
                losses = losses + ?
        """, (key, setup_id, direction, regime, int(is_win), int(is_loss), int(is_win), int(is_loss)))

        await conn.commit()

    async def is_symbol_blacklisted(
        self,
        symbol: str,
        max_sl_streak: int = 3,
        pause_hours: int = 0,
    ) -> bool:
        """Check if symbol is temporarily paused due to consecutive SL hits."""
        conn = self._require_conn()
        await self._ensure_extended_tables()

        async with conn.execute(
            "SELECT consecutive_sl, last_signal_ts FROM symbol_stats WHERE symbol = ?",
            (symbol,)
        ) as cursor:
            row = await cursor.fetchone()
            if not row or row["consecutive_sl"] < max_sl_streak:
                return False
            if pause_hours <= 0:
                return True
            try:
                last_signal_ts = datetime.fromisoformat(str(row["last_signal_ts"]))
            except (TypeError, ValueError):
                return True
            if last_signal_ts.tzinfo is None:
                last_signal_ts = last_signal_ts.replace(tzinfo=timezone.utc)
            return (datetime.now(timezone.utc) - last_signal_ts) < timedelta(hours=pause_hours)

    async def get_consecutive_sl(self, symbol: str) -> int:
        """Get consecutive SL streak for symbol."""
        conn = self._require_conn()
        await self._ensure_extended_tables()

        async with conn.execute(
            "SELECT consecutive_sl FROM symbol_stats WHERE symbol = ?",
            (symbol,)
        ) as cursor:
            row = await cursor.fetchone()
            return row["consecutive_sl"] if row else 0
    
    async def get_setup_score_multiplier(
        self,
        setup_id: str,
        direction: str,
        regime: str,
        min_samples: int = 10,
    ) -> float:
        """Get score multiplier based on setup win rate."""
        conn = self._require_conn()
        await self._ensure_extended_tables()

        key = f"{setup_id}|{direction}|{regime}"
        async with conn.execute(
            "SELECT wins, losses FROM setup_stats WHERE setup_key = ?",
            (key,)
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                wins = row["wins"]
                losses = row["losses"]
                total = wins + losses
                if total >= min_samples:
                    win_rate = wins / total
                    if win_rate >= 0.65:
                        return 1.10
                    elif win_rate <= 0.40:
                        return 0.90
            return 1.0

    async def summary(self) -> dict[str, Any]:
        """Get summary for logging and Telegram."""
        conn = self._require_conn()
        await self._ensure_extended_tables()

        # Get blacklisted symbols
        async with conn.execute(
            "SELECT symbol FROM symbol_stats WHERE consecutive_sl >= 3"
        ) as cursor:
            blacklisted = [row["symbol"] for row in await cursor.fetchall()]

        # Get top setups by win rate
        async with conn.execute(
            "SELECT setup_id, wins, losses FROM setup_stats ORDER BY (wins + losses) DESC LIMIT 5"
        ) as cursor:
            top_setups = []
            for row in await cursor.fetchall():
                total = row["wins"] + row["losses"]
                win_rate = row["wins"] / total if total > 0 else 0
                top_setups.append({
                    "setup_id": row["setup_id"],
                    "win_rate": win_rate,
                    "samples": total,
                })
        
        return {
            "blacklisted_symbols": blacklisted,
            "top_setups": top_setups,
            "symbol_count": len(blacklisted),
        }
