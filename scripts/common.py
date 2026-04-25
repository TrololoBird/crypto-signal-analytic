from __future__ import annotations

import json
import logging
import sys
from pathlib import Path
from typing import Iterable, Sequence

import structlog


_COMMON_LOG = structlog.get_logger("scripts.common")


def bootstrap_repo_path() -> Path:
    """Ensure repository root is on sys.path for direct script execution."""
    root_dir = Path(__file__).resolve().parents[1]
    if str(root_dir) not in sys.path:
        sys.path.insert(0, str(root_dir))
    return root_dir


def configure_script_logging(name: str) -> structlog.BoundLogger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        force=True,
    )
    return structlog.get_logger(name)


def load_symbols_from_run(run_id: str, telemetry_root: Path) -> list[str]:
    target = telemetry_root / "runs" / run_id / "analysis" / "symbol_analysis.jsonl"
    if not target.exists():
        _COMMON_LOG.warning("symbols_from_run_missing", run_id=run_id, path=str(target))
        return []

    symbols: list[str] = []
    seen: set[str] = set()
    for line in target.read_text(encoding="utf-8").splitlines():
        row = json.loads(line)
        symbol = str(row.get("symbol") or "").upper()
        if symbol and symbol not in seen:
            seen.add(symbol)
            symbols.append(symbol)
    return symbols


def _normalize_symbols(values: Iterable[object]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for value in values:
        symbol = str(value or "").strip().upper()
        if symbol and symbol not in seen:
            seen.add(symbol)
            normalized.append(symbol)
    return normalized


def resolve_symbols(
    args_symbols: Sequence[object],
    symbols_from_run: Sequence[object],
    fallback_symbols: Sequence[object],
) -> list[str]:
    explicit_symbols = _normalize_symbols(args_symbols)
    if explicit_symbols:
        return explicit_symbols

    run_symbols = _normalize_symbols(symbols_from_run)
    if run_symbols:
        return run_symbols

    return _normalize_symbols(fallback_symbols)
