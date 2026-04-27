from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path


@dataclass
class RegimeAwareParams:
    """Adjust bounds by regime and persist optimized params."""

    regime: str
    db_path: str | Path = "data/bot/optimization.db"

    def scale(self, param_name: str, default_value: float) -> tuple[float, float]:
        regime = self.regime.lower()
        if "volatile" in regime:
            width = 0.7
        elif "ranging" in regime:
            width = 0.4
        else:
            width = 0.5

        if "min_rr" in param_name:
            return (max(0.8, default_value * (1.0 - width)), min(4.0, default_value * (1.0 + width)))
        if any(token in param_name for token in ("score", "threshold", "penalty")):
            return (max(0.0, default_value * (1.0 - width)), min(1.0, default_value * (1.0 + width)))
        return (default_value * (1.0 - width), default_value * (1.0 + width))

    def get_params(self, setup_id: str, regime: str | None = None) -> dict[str, float]:
        """Load stored optimized params for setup/regime from SQLite."""
        effective_regime = (regime or self.regime or "unknown").lower()
        conn = sqlite3.connect(str(self.db_path))
        try:
            self._ensure_schema(conn)
            row = conn.execute(
                """
                SELECT params_json
                FROM regime_aware_params
                WHERE setup_id = ? AND regime = ?
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                (setup_id, effective_regime),
            ).fetchone()
            if not row or not row[0]:
                return {}
            parsed = json.loads(str(row[0]))
            if not isinstance(parsed, dict):
                return {}
            return {str(k): float(v) for k, v in parsed.items() if isinstance(v, (int, float))}
        finally:
            conn.close()

    def set_params(self, setup_id: str, regime: str, params: dict[str, float]) -> None:
        """Upsert optimized params for setup/regime into SQLite."""
        clean = {str(k): float(v) for k, v in params.items() if isinstance(v, (int, float))}
        if not clean:
            return
        conn = sqlite3.connect(str(self.db_path))
        try:
            self._ensure_schema(conn)
            conn.execute(
                """
                INSERT INTO regime_aware_params (setup_id, regime, params_json, updated_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(setup_id, regime) DO UPDATE SET
                    params_json = excluded.params_json,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (setup_id, regime.lower(), json.dumps(clean, sort_keys=True)),
            )
            conn.commit()
        finally:
            conn.close()

    @staticmethod
    def _ensure_schema(conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS regime_aware_params (
                setup_id TEXT NOT NULL,
                regime TEXT NOT NULL,
                params_json TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (setup_id, regime)
            )
            """
        )
