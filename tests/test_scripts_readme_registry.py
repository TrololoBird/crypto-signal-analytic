from __future__ import annotations

import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


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
