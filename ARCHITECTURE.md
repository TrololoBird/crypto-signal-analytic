# Crypto Signal Bot — Architecture (current state)

This document summarizes the **actual runtime path** and module boundaries used by the project.

## Runtime entry path

1. `main.py`
2. `bot.cli.run()`
3. `bot.application.bot.SignalBot`

`SignalBot` is the orchestrator that wires together market data, strategy execution, tracking, delivery, telemetry, and shutdown lifecycle.

## Core runtime components

- **Application orchestration:** `bot/application/bot.py`
  - startup/shutdown lifecycle
  - periodic tasks (shortlist refresh, OI/L/S cache refresh, tracking loops)
  - analysis cycle orchestration
- **Execution engine:** `bot/core/engine/`
  - strategy registry
  - setup dispatch
  - strategy decisions
- **Market I/O:** `bot/market_data.py`, `bot/ws_manager.py`
  - REST enrichment
  - websocket stream handling
- **Persistence / state:** `bot/core/memory/repository.py`
  - active/closed signals
  - cooldowns
  - stats/outcomes
- **Signal lifecycle:** `bot/tracking.py`, `bot/delivery.py`
  - activation / TP / SL transitions
  - message rendering and send flow

## Strategy layer

- Strategy implementations live in `bot/strategies/`.
- Shared setup helpers live in `bot/setups.py` and `bot/setups/`.
- Strategy enable flags: `config.toml` under `[bot.setups]`.
- Strategy parameter overrides: `config.toml` under `[bot.filters.setups]`.

## Config contracts

- Main config parser/models: `bot/config.py`
- Runtime config files:
  - `config.toml`
  - `config.toml.example` (must stay aligned with real config surface)

## Operational checks

- Regression tests: `tests/test_remediation_regressions.py` and adjacent `tests/`.
- Live/manual scripts: `scripts/live_*.py`.
- Script inventory sync check: `scripts/check_scripts_readme.py`.

## Known design constraints

- Keep market and network I/O async.
- New dataframe work should use Polars.
- Persistence changes should go through `MemoryRepository` instead of ad hoc stores.
