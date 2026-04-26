#!/usr/bin/env python3
"""Локальная валидация окружения/конфига и базовой целостности фичей."""

from __future__ import annotations

import sys
from pathlib import Path

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def main() -> int:
    errors: list[str] = []

    # 1) Базовые папки
    for directory in ("data", "logs"):
        path = REPO_ROOT / directory
        if not path.exists():
            path.mkdir(parents=True, exist_ok=True)

    # 2) Импорт экспортов стратегий
    try:
        from bot.strategies import STRATEGY_CLASSES

        if not STRATEGY_CLASSES:
            errors.append("No strategies exported via bot.strategies.STRATEGY_CLASSES")
    except Exception as exc:  # pragma: no cover - defensive CLI script
        errors.append(f"Strategy import failed: {exc}")

    # 3) Проверка advanced indicators на константные placeholder-профили
    try:
        from bot.features import _add_advanced_indicators

        df = pl.DataFrame(
            {
                "open": [100.0 + i * 0.1 for i in range(120)],
                "high": [101.0 + i * 0.1 for i in range(120)],
                "low": [99.0 + i * 0.1 for i in range(120)],
                "close": [100.5 + i * 0.1 for i in range(120)],
                "volume": [1000.0 + i for i in range(120)],
            }
        )
        out = _add_advanced_indicators(df)
        must_vary = (
            "aroon_up14",
            "aroon_down14",
            "fisher",
            "fisher_signal",
            "squeeze_hist",
            "chandelier_dir",
        )
        for column in must_vary:
            if column not in out.columns:
                errors.append(f"Missing indicator column: {column}")
                continue
            values = out[column].drop_nulls()
            if values.len() == 0:
                errors.append(f"Indicator empty after drop_nulls: {column}")
                continue
            uniq = values.unique().len()
            if uniq <= 1:
                only_value = float(values[0])
                if only_value in {0.0, 50.0}:
                    errors.append(
                        f"Indicator appears placeholder-like constant ({only_value}): {column}"
                    )
    except Exception as exc:  # pragma: no cover - defensive CLI script
        errors.append(f"Indicator validation failed: {exc}")

    if errors:
        print("VALIDATION FAILED:")
        for error in errors:
            print(f"  ❌ {error}")
        return 1

    print("✅ All checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
