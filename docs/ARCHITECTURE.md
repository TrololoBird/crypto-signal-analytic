# Architecture

## Runtime flow

1. `main.py` launches CLI.
2. `bot.cli.run()` constructs `SignalBot`.
3. `SignalBot` orchestrates market data, strategy evaluation, confluence scoring, delivery, and tracking.

## Main modules

- `bot/application/`: runtime orchestration (`bot.py`, `symbol_analyzer.py`, delivery/health/context helpers).
- `bot/core/`: engine, event bus, memory repository, diagnostics.
- `bot/strategies/`: setup detectors registered in the modern strategy registry.
- `bot/setups/`: shared setup helpers and metadata.
- `bot/market_regime.py` + `bot/regime/`: market regime analyzers.
- `bot/confluence.py`: score blending and optional ML adjustment.
- `bot/learning/`: walk-forward optimization, regime-aware param bounds, and outcome store adapters.

## Data and state

- Config sources: `config.toml`, `config.toml.example`.
- Persistence: `MemoryRepository` (SQLite-backed).
- Telemetry: JSONL appenders in telemetry directory.

## Design boundaries

- Strategy logic should stay in strategies/setup helpers, not in orchestration.
- Persistence goes through repository abstractions.
- Async I/O boundaries are preserved in runtime modules.
