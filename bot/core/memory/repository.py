"""Unified repository for signals and outcomes storage."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import aiosqlite
import polars as pl

from .repository_extension import MemoryRepositoryExtension

LOG = logging.getLogger("bot.core.memory.repository")


@dataclass
class SignalRecord:
    """Record of generated signal."""
    
    signal_id: str
    symbol: str
    strategy_id: str
    direction: str  # "long" or "short"
    entry_price: float
    stop_loss: float
    take_profit_1: float
    take_profit_2: float
    score: float
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    
    # Context at signal time
    timeframe: str = "1h"
    atr_pct: float = 0.0
    spread_bps: float = 0.0
    rsi_1h: float | None = None
    adx_1h: float | None = None
    volume_ratio: float | None = None
    funding_rate: float | None = None
    oi_change_pct: float | None = None
    
    # Metadata
    features: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        result = asdict(self)
        # Convert datetime to ISO string
        result["created_at"] = self.created_at.isoformat()
        return result
    
    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SignalRecord:
        """Create from dictionary."""
        # Parse ISO datetime
        if "created_at" in data and isinstance(data["created_at"], str):
            data["created_at"] = datetime.fromisoformat(data["created_at"])
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class OutcomeRecord:
    """Record of signal outcome after tracking."""
    
    outcome_id: str
    signal_id: str
    symbol: str
    
    # Price updates
    price_1h: float | None = None
    price_4h: float | None = None
    price_24h: float | None = None
    
    # PnL proxy (percentage from entry)
    pnl_1h: float | None = None
    pnl_4h: float | None = None
    pnl_24h: float | None = None
    
    # MAE/MFE tracking
    max_profit_pct: float = 0.0
    max_loss_pct: float = 0.0
    mae: float = 0.0  # Maximum adverse excursion
    mfe: float = 0.0  # Maximum favorable excursion
    
    # Result classification
    hit_tp1: bool = False
    hit_tp2: bool = False
    hit_sl: bool = False
    result: str = ""  # "win", "loss", "breakeven", "open"
    
    # Timestamps
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    closed_at: datetime | None = None
    
    # Analysis
    time_to_tp1_min: int | None = None
    time_to_tp2_min: int | None = None
    time_to_sl_min: int | None = None
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        result = asdict(self)
        # Convert datetimes to ISO strings
        if self.updated_at:
            result["updated_at"] = self.updated_at.isoformat()
        if self.closed_at:
            result["closed_at"] = self.closed_at.isoformat()
        return result
    
    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> OutcomeRecord:
        """Create from dictionary."""
        # Parse ISO datetimes
        if "updated_at" in data and isinstance(data["updated_at"], str):
            data["updated_at"] = datetime.fromisoformat(data["updated_at"])
        if "closed_at" in data and isinstance(data["closed_at"], str):
            data["closed_at"] = datetime.fromisoformat(data["closed_at"])
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


class MemoryRepository(MemoryRepositoryExtension):
    """Unified repository for signals and outcomes.
    
    Uses SQLite for metadata and Parquet for time-series data.
    Supports async operations and batch inserts.
    """
    
    def __init__(self, db_path: Path | str, data_dir: Path | str):
        self._db_path = Path(db_path)
        self._data_dir = Path(data_dir)
        self._data_dir.mkdir(parents=True, exist_ok=True)
        self._conn: aiosqlite.Connection | None = None
    
    async def initialize(self) -> None:
        """Initialize database tables."""
        self._conn = await aiosqlite.connect(self._db_path)
        self._conn.row_factory = aiosqlite.Row
        
        # Create tables
        await self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS signals (
                signal_id TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                strategy_id TEXT NOT NULL,
                direction TEXT NOT NULL,
                entry_price REAL NOT NULL,
                stop_loss REAL NOT NULL,
                take_profit_1 REAL NOT NULL,
                take_profit_2 REAL NOT NULL,
                score REAL NOT NULL,
                created_at TEXT NOT NULL,
                timeframe TEXT DEFAULT '1h',
                atr_pct REAL DEFAULT 0.0,
                spread_bps REAL DEFAULT 0.0,
                rsi_1h REAL,
                adx_1h REAL,
                volume_ratio REAL,
                funding_rate REAL,
                oi_change_pct REAL,
                features TEXT,  -- JSON
                metadata TEXT,  -- JSON
                outcome_id TEXT  -- Reference to outcome
            );
            
            CREATE INDEX IF NOT EXISTS idx_signals_symbol ON signals(symbol);
            CREATE INDEX IF NOT EXISTS idx_signals_strategy ON signals(strategy_id);
            CREATE INDEX IF NOT EXISTS idx_signals_created ON signals(created_at);
            
            CREATE TABLE IF NOT EXISTS outcomes (
                outcome_id TEXT PRIMARY KEY,
                signal_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                price_1h REAL,
                price_4h REAL,
                price_24h REAL,
                pnl_1h REAL,
                pnl_4h REAL,
                pnl_24h REAL,
                max_profit_pct REAL DEFAULT 0.0,
                max_loss_pct REAL DEFAULT 0.0,
                mae REAL DEFAULT 0.0,
                mfe REAL DEFAULT 0.0,
                hit_tp1 INTEGER DEFAULT 0,
                hit_tp2 INTEGER DEFAULT 0,
                hit_sl INTEGER DEFAULT 0,
                result TEXT DEFAULT '',
                updated_at TEXT NOT NULL,
                closed_at TEXT,
                time_to_tp1_min INTEGER,
                time_to_tp2_min INTEGER,
                time_to_sl_min INTEGER,
                FOREIGN KEY (signal_id) REFERENCES signals(signal_id)
            );
            
            CREATE INDEX IF NOT EXISTS idx_outcomes_symbol ON outcomes(symbol);
            CREATE INDEX IF NOT EXISTS idx_outcomes_signal ON outcomes(signal_id);
            CREATE INDEX IF NOT EXISTS idx_outcomes_result ON outcomes(result);
            
            CREATE TABLE IF NOT EXISTS config_versions (
                version_id TEXT PRIMARY KEY,
                config_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                is_active INTEGER DEFAULT 0
            );
            
            -- Cooldown tracking (replaces SignalCooldownStore JSON)
            CREATE TABLE IF NOT EXISTS cooldowns (
                cooldown_key TEXT PRIMARY KEY,
                last_sent_at TEXT NOT NULL,
                setup_id TEXT,
                symbol TEXT,
                cooldown_type TEXT DEFAULT 'signal_key'  -- 'signal_key' or 'symbol'
            );
            
            CREATE INDEX IF NOT EXISTS idx_cooldowns_symbol ON cooldowns(symbol);
            CREATE INDEX IF NOT EXISTS idx_cooldowns_setup ON cooldowns(setup_id);
            
            -- Setup adaptive scoring (replaces setup_score_adjustments JSON)
            CREATE TABLE IF NOT EXISTS setup_scores (
                setup_id TEXT PRIMARY KEY,
                score_adjustment REAL DEFAULT 0.0,
                outcome_window TEXT,  -- JSON array of last 20 outcomes
                updated_at TEXT NOT NULL
            );
            
            -- Active signal tracking (replaces SignalTrackingStore JSON)
            CREATE TABLE IF NOT EXISTS active_signals (
                tracking_id TEXT PRIMARY KEY,
                tracking_ref TEXT NOT NULL,
                signal_key TEXT NOT NULL,
                symbol TEXT NOT NULL,
                setup_id TEXT NOT NULL,
                direction TEXT NOT NULL,
                timeframe TEXT,
                created_at TEXT NOT NULL,
                pending_expires_at TEXT,
                active_expires_at TEXT,
                entry_low REAL,
                entry_high REAL,
                entry_mid REAL,
                initial_stop REAL,
                stop REAL,
                take_profit_1 REAL,
                take_profit_2 REAL,
                single_target_mode INTEGER DEFAULT 0,
                target_integrity_status TEXT DEFAULT 'unchecked',
                score REAL,
                risk_reward REAL,
                reasons TEXT,  -- JSON array
                signal_message_id INTEGER,
                bias_4h TEXT DEFAULT 'neutral',
                quote_volume REAL,
                spread_bps REAL,
                atr_pct REAL,
                orderflow_delta_ratio REAL,
                status TEXT DEFAULT 'pending',  -- pending, active, closed
                activated_at TEXT,
                activation_price REAL,
                tp1_hit_at TEXT,
                tp2_hit_at TEXT,
                stop_price REAL,
                tp1_price REAL,
                tp2_price REAL,
                last_checked_at TEXT,
                last_price REAL,
                closed_at TEXT,
                close_reason TEXT,
                close_price REAL
            );
            
            CREATE INDEX IF NOT EXISTS idx_active_signals_symbol ON active_signals(symbol);
            CREATE INDEX IF NOT EXISTS idx_active_signals_status ON active_signals(status);
            CREATE INDEX IF NOT EXISTS idx_active_signals_setup ON active_signals(setup_id);
            CREATE INDEX IF NOT EXISTS idx_active_signals_created ON active_signals(created_at);

            CREATE TABLE IF NOT EXISTS tracking_stats (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                signals_sent INTEGER DEFAULT 0,
                activated INTEGER DEFAULT 0,
                tp1_hit INTEGER DEFAULT 0,
                tp2_hit INTEGER DEFAULT 0,
                stop_loss INTEGER DEFAULT 0,
                expired INTEGER DEFAULT 0,
                ambiguous_exit INTEGER DEFAULT 0
            );

            INSERT OR IGNORE INTO tracking_stats (id) VALUES (1);

            CREATE TABLE IF NOT EXISTS signal_outcomes (
                tracking_id TEXT PRIMARY KEY,
                signal_id TEXT NOT NULL,
                tracking_ref TEXT NOT NULL,
                symbol TEXT NOT NULL,
                setup_id TEXT NOT NULL,
                direction TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                created_at TEXT NOT NULL,
                activated_at TEXT,
                closed_at TEXT,
                entry_price REAL,
                exit_price REAL,
                result TEXT NOT NULL,
                pnl_pct REAL DEFAULT 0.0,
                pnl_r_multiple REAL DEFAULT 0.0,
                max_profit_pct REAL DEFAULT 0.0,
                max_loss_pct REAL DEFAULT 0.0,
                mae REAL DEFAULT 0.0,
                mfe REAL DEFAULT 0.0,
                time_to_entry_min INTEGER DEFAULT 0,
                time_to_exit_min INTEGER DEFAULT 0,
                features TEXT,
                was_profitable INTEGER DEFAULT 0,
                llm_was_correct INTEGER,
                setup_quality TEXT DEFAULT 'neutral'
            );

            CREATE INDEX IF NOT EXISTS idx_signal_outcomes_symbol ON signal_outcomes(symbol);
            CREATE INDEX IF NOT EXISTS idx_signal_outcomes_setup ON signal_outcomes(setup_id);
            CREATE INDEX IF NOT EXISTS idx_signal_outcomes_result ON signal_outcomes(result);
            CREATE INDEX IF NOT EXISTS idx_signal_outcomes_closed_at ON signal_outcomes(closed_at);
        """)
        
        await self._ensure_table_columns(
            "active_signals",
            {
                "tp1_hit_at": "TEXT",
                "tp2_hit_at": "TEXT",
                "stop_price": "REAL",
                "tp1_price": "REAL",
                "tp2_price": "REAL",
                "last_checked_at": "TEXT",
                "last_price": "REAL",
                "single_target_mode": "INTEGER DEFAULT 0",
                "target_integrity_status": "TEXT DEFAULT 'unchecked'",
            },
        )
        await self._ensure_extended_tables()
        await self._conn.commit()
        LOG.info("Memory repository initialized at %s", self._db_path)

    async def _ensure_table_columns(self, table_name: str, columns: dict[str, str]) -> None:
        """Add missing columns for existing databases."""
        if not self._conn:
            raise RuntimeError("Repository not initialized")

        async with self._conn.execute(f"PRAGMA table_info({table_name})") as cursor:
            existing_rows = await cursor.fetchall()
        existing = {row["name"] for row in existing_rows}

        for column_name, column_type in columns.items():
            if column_name in existing:
                continue
            await self._conn.execute(
                f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}"
            )
    
    async def close(self) -> None:
        """Close database connection."""
        if self._conn:
            await self._conn.close()
            self._conn = None
    
    async def save_signal(self, record: SignalRecord) -> None:
        """Save signal record."""
        if not self._conn:
            raise RuntimeError("Repository not initialized")
        
        await self._conn.execute("""
            INSERT INTO signals (
                signal_id, symbol, strategy_id, direction, entry_price,
                stop_loss, take_profit_1, take_profit_2, score, created_at,
                timeframe, atr_pct, spread_bps, rsi_1h, adx_1h, volume_ratio,
                funding_rate, oi_change_pct, features, metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            record.signal_id, record.symbol, record.strategy_id, record.direction,
            record.entry_price, record.stop_loss, record.take_profit_1,
            record.take_profit_2, record.score, record.created_at.isoformat(),
            record.timeframe, record.atr_pct, record.spread_bps, record.rsi_1h,
            record.adx_1h, record.volume_ratio, record.funding_rate,
            record.oi_change_pct,
            json.dumps(record.features) if record.features else None,
            json.dumps(record.metadata) if record.metadata else None
        ))
        await self._conn.commit()
    
    async def save_outcome(self, record: OutcomeRecord) -> None:
        """Save outcome record."""
        if not self._conn:
            raise RuntimeError("Repository not initialized")
        
        await self._conn.execute("""
            INSERT INTO outcomes (
                outcome_id, signal_id, symbol, price_1h, price_4h, price_24h,
                pnl_1h, pnl_4h, pnl_24h, max_profit_pct, max_loss_pct, mae, mfe,
                hit_tp1, hit_tp2, hit_sl, result, updated_at, closed_at,
                time_to_tp1_min, time_to_tp2_min, time_to_sl_min
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(outcome_id) DO UPDATE SET
                price_1h = excluded.price_1h,
                price_4h = excluded.price_4h,
                price_24h = excluded.price_24h,
                pnl_1h = excluded.pnl_1h,
                pnl_4h = excluded.pnl_4h,
                pnl_24h = excluded.pnl_24h,
                max_profit_pct = excluded.max_profit_pct,
                max_loss_pct = excluded.max_loss_pct,
                mae = excluded.mae,
                mfe = excluded.mfe,
                hit_tp1 = excluded.hit_tp1,
                hit_tp2 = excluded.hit_tp2,
                hit_sl = excluded.hit_sl,
                result = excluded.result,
                updated_at = excluded.updated_at,
                closed_at = excluded.closed_at
        """, (
            record.outcome_id, record.signal_id, record.symbol,
            record.price_1h, record.price_4h, record.price_24h,
            record.pnl_1h, record.pnl_4h, record.pnl_24h,
            record.max_profit_pct, record.max_loss_pct, record.mae, record.mfe,
            int(record.hit_tp1), int(record.hit_tp2), int(record.hit_sl),
            record.result, record.updated_at.isoformat(),
            record.closed_at.isoformat() if record.closed_at else None,
            record.time_to_tp1_min, record.time_to_tp2_min, record.time_to_sl_min
        ))
        await self._conn.commit()
    
    async def get_signal(self, signal_id: str) -> SignalRecord | None:
        """Get signal by ID."""
        if not self._conn:
            raise RuntimeError("Repository not initialized")
        
        async with self._conn.execute(
            "SELECT * FROM signals WHERE signal_id = ?", (signal_id,)
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                return self._row_to_signal_record(row)
            return None
    
    async def get_outcome(self, outcome_id: str) -> OutcomeRecord | None:
        """Get outcome by ID."""
        if not self._conn:
            raise RuntimeError("Repository not initialized")
        
        async with self._conn.execute(
            "SELECT * FROM outcomes WHERE outcome_id = ?", (outcome_id,)
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                return self._row_to_outcome_record(row)
            return None
    
    async def get_outcome_by_signal(self, signal_id: str) -> OutcomeRecord | None:
        """Get outcome for a signal."""
        if not self._conn:
            raise RuntimeError("Repository not initialized")
        
        async with self._conn.execute(
            "SELECT * FROM outcomes WHERE signal_id = ?", (signal_id,)
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                return self._row_to_outcome_record(row)
            return None
    
    async def get_signals_without_outcome(
        self, 
        limit: int = 100
    ) -> list[SignalRecord]:
        """Get signals that don't have outcomes yet."""
        if not self._conn:
            raise RuntimeError("Repository not initialized")
        
        async with self._conn.execute("""
            SELECT s.* FROM signals s
            LEFT JOIN outcomes o ON s.signal_id = o.signal_id
            WHERE o.outcome_id IS NULL
            ORDER BY s.created_at DESC
            LIMIT ?
        """, (limit,)) as cursor:
            rows = await cursor.fetchall()
            return [self._row_to_signal_record(row) for row in rows]
    
    async def get_signals_by_strategy(
        self, 
        strategy_id: str,
        since: datetime | None = None,
        limit: int = 1000
    ) -> list[SignalRecord]:
        """Get signals for a strategy."""
        if not self._conn:
            raise RuntimeError("Repository not initialized")
        
        query = "SELECT * FROM signals WHERE strategy_id = ?"
        params: list[Any] = [strategy_id]
        
        if since:
            query += " AND created_at > ?"
            params.append(since.isoformat())
        
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        
        async with self._conn.execute(query, params) as cursor:
            rows = await cursor.fetchall()
            return [self._row_to_signal_record(row) for row in rows]
    
    async def get_signals_for_analysis(
        self,
        since: datetime,
        min_score: float = 0.0
    ) -> pl.DataFrame:
        """Get signals as Polars DataFrame for analysis."""
        if not self._conn:
            raise RuntimeError("Repository not initialized")
        
        async with self._conn.execute("""
            SELECT s.*, o.result, o.pnl_24h, o.max_profit_pct, o.max_loss_pct
            FROM signals s
            LEFT JOIN outcomes o ON s.signal_id = o.signal_id
            WHERE s.created_at > ? AND s.score >= ?
            ORDER BY s.created_at DESC
        """, (since.isoformat(), min_score)) as cursor:
            rows = await cursor.fetchall()
            
        if not rows:
            return pl.DataFrame()
        
        # Convert to dicts for Polars
        data = [dict(row) for row in rows]
        return pl.DataFrame(data)
    
    async def save_config_version(self, config_json: str) -> str:
        """Save config version."""
        if not self._conn:
            raise RuntimeError("Repository not initialized")
        
        version_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        
        await self._conn.execute("""
            INSERT INTO config_versions (version_id, config_json, created_at, is_active)
            VALUES (?, ?, ?, 1)
        """, (version_id, config_json, datetime.now(timezone.utc).isoformat()))
        
        # Deactivate previous versions
        await self._conn.execute("""
            UPDATE config_versions SET is_active = 0
            WHERE version_id != ?
        """, (version_id,))
        
        await self._conn.commit()
        return version_id
    
    async def get_active_config(self) -> str | None:
        """Get active config JSON."""
        if not self._conn:
            raise RuntimeError("Repository not initialized")
        
        async with self._conn.execute(
            "SELECT config_json FROM config_versions WHERE is_active = 1 LIMIT 1"
        ) as cursor:
            row = await cursor.fetchone()
            return row["config_json"] if row else None
    
    def _row_to_signal_record(self, row: aiosqlite.Row) -> SignalRecord:
        """Convert DB row to SignalRecord."""
        data = dict(row)
        # Parse JSON fields
        if data.get("features"):
            data["features"] = json.loads(data["features"])
        if data.get("metadata"):
            data["metadata"] = json.loads(data["metadata"])
        return SignalRecord.from_dict(data)
    
    def _row_to_outcome_record(self, row: aiosqlite.Row) -> OutcomeRecord:
        """Convert DB row to OutcomeRecord."""
        data = dict(row)
        # Convert integer booleans
        data["hit_tp1"] = bool(data.get("hit_tp1", 0))
        data["hit_tp2"] = bool(data.get("hit_tp2", 0))
        data["hit_sl"] = bool(data.get("hit_sl", 0))
        return OutcomeRecord.from_dict(data)
    
    # ------------------------------------------------------------------
    # Cooldown methods (replaces SignalCooldownStore)
    # ------------------------------------------------------------------
    
    async def get_cooldown(self, cooldown_key: str) -> datetime | None:
        """Get last sent time for a cooldown key."""
        if not self._conn:
            raise RuntimeError("Repository not initialized")
        
        async with self._conn.execute(
            "SELECT last_sent_at FROM cooldowns WHERE cooldown_key = ?",
            (cooldown_key,)
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                return datetime.fromisoformat(row["last_sent_at"])
            return None
    
    async def set_cooldown(
        self,
        cooldown_key: str,
        sent_at: datetime,
        setup_id: str | None = None,
        symbol: str | None = None,
        cooldown_type: str = "signal_key"
    ) -> None:
        """Set cooldown for a key."""
        if not self._conn:
            raise RuntimeError("Repository not initialized")
        
        await self._conn.execute("""
            INSERT INTO cooldowns (cooldown_key, last_sent_at, setup_id, symbol, cooldown_type)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(cooldown_key) DO UPDATE SET
                last_sent_at = excluded.last_sent_at,
                setup_id = excluded.setup_id,
                symbol = excluded.symbol,
                cooldown_type = excluded.cooldown_type
        """, (cooldown_key, sent_at.isoformat(), setup_id, symbol, cooldown_type))
        await self._conn.commit()
    
    async def is_cooldown_active(
        self,
        cooldown_key: str,
        cooldown_minutes: int,
        now: datetime | None = None
    ) -> bool:
        """Check if cooldown is still active."""
        if cooldown_minutes <= 0:
            return False
        
        last_sent = await self.get_cooldown(cooldown_key)
        if not last_sent:
            return False
        
        now = now or datetime.now(timezone.utc)
        return (now - last_sent) < timedelta(minutes=cooldown_minutes)
    
    # ------------------------------------------------------------------
    # Setup adaptive scoring (replaces setup_score_adjustments)
    # ------------------------------------------------------------------
    
    async def get_setup_score_adjustment(self, setup_id: str) -> float:
        """Get current score adjustment for a setup."""
        if not self._conn:
            raise RuntimeError("Repository not initialized")
        
        async with self._conn.execute(
            "SELECT score_adjustment FROM setup_scores WHERE setup_id = ?",
            (setup_id,)
        ) as cursor:
            row = await cursor.fetchone()
            return row["score_adjustment"] if row else 0.0
    
    async def record_setup_outcome(
        self,
        setup_id: str,
        outcome: str,
        window_size: int = 20,
        min_outcomes: int = 15,
        penalty: float = -0.05,
        bonus: float = 0.03,
        low_win_rate: float = 0.40,
        high_win_rate: float = 0.60
    ) -> float:
        """Record outcome and return new score adjustment.
        
        Replaces SignalCooldownStore.record_outcome()
        """
        if not self._conn:
            raise RuntimeError("Repository not initialized")
        
        # Get current window
        async with self._conn.execute(
            "SELECT outcome_window FROM setup_scores WHERE setup_id = ?",
            (setup_id,)
        ) as cursor:
            row = await cursor.fetchone()
            window: list[str] = []
            if row and row["outcome_window"]:
                window = json.loads(row["outcome_window"])
        
        # Add new outcome
        window.append(outcome)
        if len(window) > window_size:
            window = window[-window_size:]
        
        # Calculate adjustment
        win_reasons = {"tp1_hit", "tp2_hit"}
        adjustment = 0.0
        if len(window) >= min_outcomes:
            wins = sum(1 for r in window if r in win_reasons)
            win_rate = wins / len(window)
            if win_rate < low_win_rate:
                adjustment = penalty
            elif win_rate > high_win_rate:
                adjustment = bonus
        
        # Save
        await self._conn.execute("""
            INSERT INTO setup_scores (setup_id, score_adjustment, outcome_window, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(setup_id) DO UPDATE SET
                score_adjustment = excluded.score_adjustment,
                outcome_window = excluded.outcome_window,
                updated_at = excluded.updated_at
        """, (setup_id, adjustment, json.dumps(window), datetime.now(timezone.utc).isoformat()))
        await self._conn.commit()
        
        return adjustment
    
    # ------------------------------------------------------------------
    # Active signal tracking (replaces SignalTrackingStore)
    # ------------------------------------------------------------------
    
    async def save_active_signal(self, signal_data: dict[str, Any]) -> None:
        """Save or update active signal.
        
        signal_data must contain: tracking_id, tracking_ref, signal_key, symbol, setup_id, direction
        """
        if not self._conn:
            raise RuntimeError("Repository not initialized")
        
        # Build columns and values
        columns = [
            "tracking_id", "tracking_ref", "signal_key", "symbol", "setup_id",
            "direction", "timeframe", "created_at", "pending_expires_at", "active_expires_at",
            "entry_low", "entry_high", "entry_mid", "initial_stop", "stop",
            "take_profit_1", "take_profit_2", "single_target_mode", "target_integrity_status",
            "score", "risk_reward", "reasons",
            "signal_message_id", "bias_4h", "quote_volume", "spread_bps", "atr_pct",
            "orderflow_delta_ratio", "status", "activated_at", "activation_price",
            "tp1_hit_at", "tp2_hit_at", "stop_price", "tp1_price", "tp2_price",
            "last_checked_at", "last_price",
            "closed_at", "close_reason", "close_price"
        ]
        
        values = []
        for col in columns:
            val = signal_data.get(col)
            if col == "reasons" and isinstance(val, (list, tuple)):
                val = json.dumps(list(val))
            values.append(val)
        
        placeholders = ", ".join(["?"] * len(columns))
        updates = ", ".join([f"{col} = excluded.{col}" for col in columns if col != "tracking_id"])
        
        await self._conn.execute(f"""
            INSERT INTO active_signals ({', '.join(columns)})
            VALUES ({placeholders})
            ON CONFLICT(tracking_id) DO UPDATE SET {updates}
        """, values)
        await self._conn.commit()
    
    async def get_active_signals(
        self,
        symbol: str | None = None,
        status: str | None = None,
        include_closed: bool = False,
    ) -> list[dict[str, Any]]:
        """Get active signals with optional filtering."""
        if not self._conn:
            raise RuntimeError("Repository not initialized")
        
        query = "SELECT * FROM active_signals WHERE 1=1"
        params: list[Any] = []
        
        if not include_closed and status is None:
            query += " AND status IN ('pending', 'active')"
        if symbol:
            query += " AND symbol = ?"
            params.append(symbol)
        if status:
            query += " AND status = ?"
            params.append(status)
        
        query += " ORDER BY created_at DESC"
        
        async with self._conn.execute(query, params) as cursor:
            rows = await cursor.fetchall()
            result = []
            for row in rows:
                data = dict(row)
                if data.get("reasons"):
                    data["reasons"] = json.loads(data["reasons"])
                result.append(data)
            return result
    
    async def close_active_signal(
        self,
        tracking_id: str,
        close_reason: str,
        close_price: float | None = None,
        closed_at: datetime | None = None
    ) -> None:
        """Close an active signal."""
        if not self._conn:
            raise RuntimeError("Repository not initialized")
        
        closed_at = closed_at or datetime.now(timezone.utc)
        
        await self._conn.execute("""
            UPDATE active_signals
            SET status = 'closed',
                close_reason = ?,
                close_price = ?,
                closed_at = ?
            WHERE tracking_id = ?
        """, (close_reason, close_price, closed_at.isoformat(), tracking_id))
        await self._conn.commit()
    
    async def update_signal_status(
        self,
        tracking_id: str,
        status: str,
        activation_price: float | None = None,
        activated_at: datetime | None = None
    ) -> None:
        """Update signal status (e.g., pending -> active)."""
        if not self._conn:
            raise RuntimeError("Repository not initialized")
        
        if status == "active" and activated_at:
            await self._conn.execute("""
                UPDATE active_signals
                SET status = ?, activation_price = ?, activated_at = ?
                WHERE tracking_id = ?
            """, (status, activation_price, activated_at.isoformat(), tracking_id))
        else:
            await self._conn.execute(
                "UPDATE active_signals SET status = ? WHERE tracking_id = ?",
                (status, tracking_id)
            )
        await self._conn.commit()

    async def get_tracking_stats(self) -> dict[str, int]:
        """Return tracking lifecycle counters."""
        if not self._conn:
            raise RuntimeError("Repository not initialized")

        async with self._conn.execute(
            """
            SELECT signals_sent, activated, tp1_hit, tp2_hit, stop_loss, expired, ambiguous_exit
            FROM tracking_stats
            WHERE id = 1
            """
        ) as cursor:
            row = await cursor.fetchone()

        stats = dict(row) if row else {
            "signals_sent": 0,
            "activated": 0,
            "tp1_hit": 0,
            "tp2_hit": 0,
            "stop_loss": 0,
            "expired": 0,
            "ambiguous_exit": 0,
        }
        async with self._conn.execute(
            "SELECT COUNT(*) AS active_count FROM active_signals WHERE status IN ('pending', 'active')"
        ) as cursor:
            active_row = await cursor.fetchone()
        stats["active"] = int(active_row["active_count"]) if active_row else 0
        stats["ambiguous"] = int(stats.pop("ambiguous_exit", 0))
        return {key: int(value) for key, value in stats.items()}

    async def increment_tracking_stats(self, **deltas: int) -> None:
        """Increment one or more tracking counters."""
        if not self._conn:
            raise RuntimeError("Repository not initialized")

        allowed = {
            "signals_sent",
            "activated",
            "tp1_hit",
            "tp2_hit",
            "stop_loss",
            "expired",
            "ambiguous_exit",
        }
        updates: list[str] = []
        params: list[int] = []
        for key, delta in deltas.items():
            if key not in allowed or delta == 0:
                continue
            updates.append(f"{key} = {key} + ?")
            params.append(int(delta))

        if not updates:
            return

        params.append(1)
        await self._conn.execute(
            f"UPDATE tracking_stats SET {', '.join(updates)} WHERE id = ?",
            params,
        )
        await self._conn.commit()

    async def save_signal_outcome(self, outcome_data: dict[str, Any]) -> None:
        """Persist a completed tracked-signal outcome."""
        await self.save_signal_outcomes_batch([outcome_data])

    async def save_signal_outcomes_batch(self, outcomes_data: list[dict[str, Any]]) -> None:
        """Persist completed tracked-signal outcomes in batch."""
        if not self._conn:
            raise RuntimeError("Repository not initialized")
        if not outcomes_data:
            return

        query = """
            INSERT INTO signal_outcomes (
                tracking_id, signal_id, tracking_ref, symbol, setup_id, direction, timeframe,
                created_at, activated_at, closed_at, entry_price, exit_price, result,
                pnl_pct, pnl_r_multiple, max_profit_pct, max_loss_pct, mae, mfe,
                time_to_entry_min, time_to_exit_min, features, was_profitable,
                llm_was_correct, setup_quality
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(tracking_id) DO UPDATE SET
                signal_id = excluded.signal_id,
                tracking_ref = excluded.tracking_ref,
                symbol = excluded.symbol,
                setup_id = excluded.setup_id,
                direction = excluded.direction,
                timeframe = excluded.timeframe,
                created_at = excluded.created_at,
                activated_at = excluded.activated_at,
                closed_at = excluded.closed_at,
                entry_price = excluded.entry_price,
                exit_price = excluded.exit_price,
                result = excluded.result,
                pnl_pct = excluded.pnl_pct,
                pnl_r_multiple = excluded.pnl_r_multiple,
                max_profit_pct = excluded.max_profit_pct,
                max_loss_pct = excluded.max_loss_pct,
                mae = excluded.mae,
                mfe = excluded.mfe,
                time_to_entry_min = excluded.time_to_entry_min,
                time_to_exit_min = excluded.time_to_exit_min,
                features = excluded.features,
                was_profitable = excluded.was_profitable,
                llm_was_correct = excluded.llm_was_correct,
                setup_quality = excluded.setup_quality
        """
        rows: list[tuple[Any, ...]] = []
        for item in outcomes_data:
            llm_was_correct = item.get("llm_was_correct")
            rows.append((
                item["tracking_id"],
                item.get("signal_id", item["tracking_id"]),
                item["tracking_ref"],
                item["symbol"],
                item["setup_id"],
                item["direction"],
                item["timeframe"],
                item["created_at"],
                item.get("activated_at"),
                item.get("closed_at"),
                item.get("entry_price"),
                item.get("exit_price"),
                item.get("result", ""),
                item.get("pnl_pct", 0.0),
                item.get("pnl_r_multiple", 0.0),
                item.get("max_profit_pct", 0.0),
                item.get("max_loss_pct", 0.0),
                item.get("mae", 0.0),
                item.get("mfe", 0.0),
                item.get("time_to_entry_min", 0),
                item.get("time_to_exit_min", 0),
                json.dumps(item.get("features", {})),
                int(bool(item.get("was_profitable", False))),
                None if llm_was_correct is None else int(bool(llm_was_correct)),
                item.get("setup_quality", "neutral"),
            ))
        await self._conn.executemany(query, rows)
        await self._conn.commit()

    async def get_setup_stats(
        self,
        setup_id: str | None = None,
        *,
        last_days: int | None = 90,
    ) -> list[dict[str, Any]]:
        """Aggregate tracked-signal outcome performance by setup."""
        if not self._conn:
            raise RuntimeError("Repository not initialized")

        query = """
            SELECT
                setup_id,
                SUM(CASE WHEN was_profitable = 1 THEN 1 ELSE 0 END) AS wins,
                SUM(CASE WHEN result = 'stop_loss' THEN 1 ELSE 0 END) AS losses,
                COUNT(*) AS total,
                AVG(pnl_r_multiple) AS avg_r_multiple,
                AVG(pnl_pct) AS avg_pnl_pct
            FROM signal_outcomes
            WHERE 1 = 1
        """
        params: list[Any] = []
        if setup_id:
            query += " AND setup_id = ?"
            params.append(setup_id)
        if last_days is not None:
            since = datetime.now(timezone.utc) - timedelta(days=last_days)
            query += " AND COALESCE(closed_at, created_at) >= ?"
            params.append(since.isoformat())
        query += " GROUP BY setup_id ORDER BY total DESC, setup_id ASC"

        async with self._conn.execute(query, params) as cursor:
            rows = await cursor.fetchall()

        result: list[dict[str, Any]] = []
        for row in rows:
            total = int(row["total"] or 0)
            wins = int(row["wins"] or 0)
            losses = int(row["losses"] or 0)
            result.append({
                "setup_id": row["setup_id"],
                "wins": wins,
                "losses": losses,
                "total": total,
                "win_rate": (wins / total) if total > 0 else 0.0,
                "avg_r_multiple": float(row["avg_r_multiple"] or 0.0),
                "avg_pnl_pct": float(row["avg_pnl_pct"] or 0.0),
            })
        return result

    async def get_signal_outcomes(
        self,
        *,
        setup_id: str | None = None,
        symbol: str | None = None,
        result: str | None = None,
        last_days: int | None = 30,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """Return persisted tracked-signal outcomes."""
        if not self._conn:
            raise RuntimeError("Repository not initialized")

        query = "SELECT * FROM signal_outcomes WHERE 1 = 1"
        params: list[Any] = []
        if setup_id:
            query += " AND setup_id = ?"
            params.append(setup_id)
        if symbol:
            query += " AND symbol = ?"
            params.append(symbol)
        if result:
            query += " AND result = ?"
            params.append(result)
        if last_days is not None:
            since = datetime.now(timezone.utc) - timedelta(days=last_days)
            query += " AND COALESCE(closed_at, created_at) >= ?"
            params.append(since.isoformat())

        query += " ORDER BY COALESCE(closed_at, created_at) DESC"
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)

        async with self._conn.execute(query, params) as cursor:
            rows = await cursor.fetchall()

        result_rows: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            raw_features = item.get("features")
            if raw_features:
                try:
                    item["features"] = json.loads(raw_features)
                except json.JSONDecodeError:
                    item["features"] = {}
            else:
                item["features"] = {}
            item["was_profitable"] = bool(item.get("was_profitable", 0))
            llm_was_correct = item.get("llm_was_correct")
            item["llm_was_correct"] = None if llm_was_correct is None else bool(llm_was_correct)
            result_rows.append(item)
        return result_rows

    async def get_cooldown_count(self) -> int:
        """Return number of persisted cooldown entries."""
        if not self._conn:
            raise RuntimeError("Repository not initialized")
        async with self._conn.execute("SELECT COUNT(*) AS count FROM cooldowns") as cursor:
            row = await cursor.fetchone()
        return int(row["count"]) if row else 0
