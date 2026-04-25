from __future__ import annotations

import textwrap


def main() -> None:
    print(
        textwrap.dedent(
            """
            [DEPRECATED] scripts/full_indicators_registry.py
            Этот скрипт будет удалён в следующем релизе.

            Используйте вместо него:
              1) python scripts/live_check_indicators.py --symbols BTCUSDT ETHUSDT
              2) python scripts/runtime_audit.py --run-id <RUN_ID>
            """
        ).strip()
    )


if __name__ == "__main__":
    main()
