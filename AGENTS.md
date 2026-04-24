# Crypto Signal Bot - Agent Routing

## Goal

Keep context small. Read the nearest `AGENTS.md` and only the files on the active call path.

## Confirmed Repo Facts

- Runtime entry: `main.py` -> `bot.cli.run()` -> `bot.application.bot.SignalBot`
- Main package: `bot/`
- Orchestration lives in `bot/application/bot.py`
- Core infra lives in `bot/core/`
- Strategies live in `bot/strategies/` and are exported via `bot/strategies/__init__.py`
- Shared setup helpers live in `bot/setups.py` and `bot/setups/`
- Config lives in `config.toml` and `config.toml.example`, parsed by `bot/config.py`
- Persistence lives in `bot/core/memory/repository.py`
- Current regression tests live in `tests/test_remediation_regressions.py`
- Manual/live checks live in `scripts/live_*.py`

## Global Rules

- Prefer `rg` and targeted reads. Do not read large files end-to-end unless you are editing them.
- Start from the module named in the task, then expand only through imports, callers, and direct contracts.
- Use Polars for new dataframe work. Do not introduce pandas-centric flows.
- Keep I/O async. Avoid blocking calls inside async code.
- Preserve the logging style already used in the touched module. Do not mix in a new logging stack unless the task is a deliberate refactor.
- Strategy parameters belong in `config.toml` under `[bot.filters.setups]`; enable flags belong under `[bot.setups]`.
- Keep `config.toml.example` aligned when config surface changes.
- Persistence changes should go through `MemoryRepository`, not ad hoc files or duplicate stores.
- Never hardcode secrets or paste real API keys/tokens.

## Routing

- `bot/*.py`: shared package-level modules such as config, models, features, market data, tracking, delivery
- `bot/application/`: runtime orchestration and event wiring
- `bot/core/`: event bus, engine, memory, diagnostics, analyzer, self-learner
- `bot/strategies/`: individual setup detectors
- `bot/setups/`: shared strategy helpers
- `bot/tasks/`: background jobs and schedulers
- `bot/telegram/`: Telegram queue/sender infrastructure
- `tests/`: regression coverage
- `scripts/`: manual/live validation scripts

## Required Work Pattern

1. Read the nearest local `AGENTS.md`.
2. Identify the exact entry point, contract, and affected callers.
3. Edit the smallest coherent set of files.
4. Verify with the narrowest relevant check: grep, import path review, targeted `pytest`, or a live script when the task actually requires external validation.
5. In summaries, separate confirmed facts from assumptions or unverified inferences.
