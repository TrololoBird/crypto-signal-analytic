#!/usr/bin/env python3
from __future__ import annotations

import textwrap


def main() -> None:
    print(
        textwrap.dedent(
            """
            [DEPRECATED] scripts/migrate_configs.py
            Этот one-off мигратор оставлен только для обратной совместимости и будет удалён в следующем релизе.

            Для новых изменений конфигурации используйте:
              - точечные правки config.toml / config.toml.example
              - targeted refactor в bot/config.py и стратегиях
            """
        ).strip()
    )


if __name__ == "__main__":
    main()
