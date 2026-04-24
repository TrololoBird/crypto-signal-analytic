# Docs Drift Report

## Current Runtime Truth
- Entry path: `main.py -> bot.cli.run() -> bot.application.bot.SignalBot`.
- Runtime is single-process within the current repo tree.

## Adjustments Made
- `PRD_v7.0_Futures_Signal_Bot_Final_Codex.md` is now explicitly marked as historical and non-authoritative for the current runtime.
- `config.toml.example` now contains the `[bot.filters.setups]` table that the live config parser and strategies expect.

## Remaining Drift
- The PRD still contains large sections describing a multi-process/shared-memory/notifier architecture; treat those sections as design history only.
- No dedicated README or operator document currently replaces the historical PRD as a concise runtime overview.