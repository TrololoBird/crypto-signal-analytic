from __future__ import annotations

import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl

LOG = logging.getLogger("bot.learning.outcome_store")


class OutcomeStore:
    """Persistence adapter for historical setup outcomes used by optimizers."""

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = str(db_path)

    async def fetch_setup_outcomes(self, setup_id: str, *, limit: int = 500) -> list[dict[str, Any]]:
        try:
            import aiosqlite

            async with aiosqlite.connect(self.db_path) as db:
                cursor = await db.execute(
                    """SELECT * FROM setup_outcomes
                       WHERE setup_id = ? AND outcome IN ('win', 'loss', 'breakeven')
                       ORDER BY created_at DESC
                       LIMIT ?""",
                    (setup_id, int(limit)),
                )
                rows = await cursor.fetchall()
                columns = [desc[0] for desc in cursor.description or []]
                return [dict(zip(columns, row)) for row in rows]
        except Exception as exc:
            LOG.warning("failed to fetch outcomes for %s: %s", setup_id, exc)
            return []

    def get_outcomes(
        self,
        setup_id: str | None,
        start: datetime,
        end: datetime,
    ) -> pl.DataFrame:
        """Load outcomes from SQLite as Polars frame for training/analysis."""
        if end <= start:
            return pl.DataFrame()
        conn = sqlite3.connect(self.db_path)
        try:
            table = self._detect_outcomes_table(conn)
            if table is None:
                return pl.DataFrame()
            columns = self._table_columns(conn, table)
            ts_col = "created_at" if "created_at" in columns else ("ts" if "ts" in columns else None)
            if ts_col is None:
                return pl.DataFrame()

            query = (
                f"SELECT * FROM {table} "
                f"WHERE {ts_col} >= ? AND {ts_col} < ? "
            )
            params: list[Any] = [start.isoformat(), end.isoformat()]
            if setup_id is not None and "setup_id" in columns:
                query += "AND setup_id = ? "
                params.append(setup_id)
            query += f"ORDER BY {ts_col} ASC"
            cursor = conn.execute(query, tuple(params))
            rows = cursor.fetchall()
            names = [d[0] for d in cursor.description or []]
            if not rows or not names:
                return pl.DataFrame()
            return pl.from_dicts([dict(zip(names, row)) for row in rows])
        except Exception as exc:
            LOG.warning("failed to get outcomes: %s", exc)
            return pl.DataFrame()
        finally:
            conn.close()

    @staticmethod
    def _detect_outcomes_table(conn: sqlite3.Connection) -> str | None:
        candidates = ("setup_outcomes", "signal_outcomes")
        rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        names = {str(name) for (name,) in rows}
        for candidate in candidates:
            if candidate in names:
                return candidate
        return None

    @staticmethod
    def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        return {str(row[1]) for row in rows if len(row) > 1}
