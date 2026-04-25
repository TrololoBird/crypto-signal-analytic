from __future__ import annotations

import textwrap


def main() -> None:
    print(
        textwrap.dedent(
            """
            [DEPRECATED] scripts/generate_audit_artifacts.py
            Этот скрипт будет удалён в следующем релизе.

            Используйте вместо него:
              - python scripts/runtime_audit.py --run-id <RUN_ID>
            """
        ).strip()
    )


if __name__ == "__main__":
    main()
