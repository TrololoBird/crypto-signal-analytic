# Bot Package Agent

## Scope

Work in top-level modules directly under `bot/`.

## Key Files

- `config.py`: settings loading and config contracts
- `models.py`: shared dataclasses and signal/prepared-symbol contracts
- `features.py`: Polars feature preparation and derived market context
- `market_data.py`, `ws_manager.py`: external market I/O and stream handling
- `tracking.py`, `delivery.py`, `messaging.py`: signal lifecycle and notification plumbing
- `setups.py`: shared strategy helpers with broad blast radius

## Local Rules

- Treat top-level `bot/*.py` modules as shared boundaries. Trace callers and consumers before changing public shapes.
- Model or config changes require checking strategies, application flow, and tests in the same pass.
- Keep market I/O async and time-sensitive paths lightweight.
- Preserve the logging style already used in the touched module.
- Do not duplicate logic that already belongs in `bot/core/`, `bot/application/`, or `bot/setups.py`.
- Changes to `bot/setups.py` or `bot/models.py` usually affect multiple strategies; verify representative callers.

## Token Discipline

- Open the specific module first, then follow only direct imports and callers.
- For large files such as `market_data.py`, `ws_manager.py`, or `tracking.py`, read only the exact function or class region you need.

## Verification

- Prefer targeted regressions or import/call-site review proportional to the blast radius.
