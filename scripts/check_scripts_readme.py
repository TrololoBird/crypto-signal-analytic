from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT / "scripts"
README_PATH = SCRIPTS_DIR / "README.md"


def _listed_scripts(readme_text: str) -> set[str]:
    listed: set[str] = set()
    for line in readme_text.splitlines():
        if not line.startswith("| `"):
            continue
        parts = [part.strip() for part in line.split("|")]
        if len(parts) < 3:
            continue
        script_cell = parts[1]
        if not (script_cell.startswith("`") and script_cell.endswith("`")):
            continue
        script_name = script_cell.strip("`")
        if script_name.endswith(".py"):
            listed.add(script_name)
    return listed


def main() -> int:
    if not README_PATH.exists():
        print(f"ERROR: missing {README_PATH.relative_to(ROOT).as_posix()}")
        return 1

    readme_text = README_PATH.read_text(encoding="utf-8")
    listed = _listed_scripts(readme_text)
    existing = {
        path.name
        for path in SCRIPTS_DIR.glob("*.py")
        if path.name != "__init__.py"
    }

    missing_in_readme = sorted(existing - listed)
    stale_in_readme = sorted(listed - existing)

    if missing_in_readme:
        print("ERROR: scripts missing in scripts/README.md:")
        for name in missing_in_readme:
            print(f"  - {name}")

    if stale_in_readme:
        print("ERROR: scripts listed in scripts/README.md but missing on disk:")
        for name in stale_in_readme:
            print(f"  - {name}")

    if missing_in_readme or stale_in_readme:
        return 1

    print("OK: scripts/README.md is in sync with scripts/*.py")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
