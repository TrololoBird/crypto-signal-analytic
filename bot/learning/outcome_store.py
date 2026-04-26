from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

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
