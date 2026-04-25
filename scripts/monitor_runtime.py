from __future__ import annotations

import textwrap


def main() -> None:
    print(
        textwrap.dedent(
            """
            [DEPRECATED] scripts/monitor_runtime.py
            Скрипт ориентирован на устаревший ручной/Windows workflow и будет удалён в следующем релизе.

            Используйте вместо него:
              - python -m scripts.live_runtime_monitor --duration 1800
              - python scripts/runtime_audit.py --run-id <RUN_ID>
            """
        ).strip()
    )


if __name__ == "__main__":
    main()
