from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

from scripts.check_scripts_readme import _listed_scripts

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize(
    ("line", "expected"),
    [
        (
            (
                "| `live_smoke_bot.py` | Smoke test | active | --warmup | "
                "`python scripts/live_smoke_bot.py` |"
            ),
            {"live_smoke_bot.py"},
        ),
        (
            (
                "| live_smoke_bot.py | missing backticks | active | --warmup | "
                "`python scripts/live_smoke_bot.py` |"
            ),
            set(),
        ),
        (
            "| `README.md` | not a python script row | active | - | - |",
            set(),
        ),
        (
            "| malformed row without separators",
            set(),
        ),
    ],
)
def test_listed_scripts_parses_table_rows(line: str, expected: set[str]) -> None:
    assert _listed_scripts(line) == expected


def test_scripts_readme_inventory_is_synced() -> None:
    result = subprocess.run(
        [sys.executable, "scripts/check_scripts_readme.py"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, (
        "scripts/README.md is out of sync with scripts/*.py\n"
        f"stdout:\n{result.stdout}\n"
        f"stderr:\n{result.stderr}"
    )
