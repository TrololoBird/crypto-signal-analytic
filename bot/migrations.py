"""SQLite schema migrations for runtime repository.

Lightweight forward-only migration registry.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence

import aiosqlite

LOG = logging.getLogger("bot.migrations")


MIGRATIONS: Sequence[tuple[int, str, str]] = (
    (
        1,
        "bootstrap_schema_version",
        """
        CREATE TABLE IF NOT EXISTS schema_version (
            version INTEGER PRIMARY KEY,
            applied_at TEXT DEFAULT (datetime('now')),
            description TEXT DEFAULT ''
        );
        """,
    ),
)


async def migrate_db(conn: aiosqlite.Connection) -> int:
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_version (
            version INTEGER PRIMARY KEY,
            applied_at TEXT DEFAULT (datetime('now')),
            description TEXT DEFAULT ''
        )
        """
    )
    async with conn.execute("SELECT COALESCE(MAX(version), 0) AS version FROM schema_version") as cursor:
        row = await cursor.fetchone()
    current = int(row[0]) if row and row[0] is not None else 0

    applied = 0
    for version, description, sql in MIGRATIONS:
        if version <= current:
            continue
        await conn.executescript(sql)
        await conn.execute(
            "INSERT OR REPLACE INTO schema_version (version, description) VALUES (?, ?)",
            (version, description),
        )
        await conn.commit()
        applied += 1
        LOG.info("db migration applied | version=%d description=%s", version, description)
    return applied
