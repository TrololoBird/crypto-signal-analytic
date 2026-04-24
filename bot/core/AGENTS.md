# Core Agent

## Scope

Work in `bot/core/`: `event_bus.py`, `events.py`, `engine/`, `memory/`, `analyzer/`, `diagnostics/`, `self_learner.py`.

## Start Here

- For event flow: `event_bus.py`, `events.py`, then producers/consumers found via `rg`
- For engine contracts: `engine/base.py`, `engine/engine.py`, `engine/registry.py`, then `bot/setup_base.py`
- For persistence: `memory/repository.py`, `memory/cache.py`, and repository call sites
- For diagnostics/reporting: the touched module plus its callers

## Local Rules

- Keep events typed. If an event shape changes, update both producers and consumers in the same change.
- Preserve async and non-blocking behavior. Cancellation, timeouts, and per-handler isolation matter here.
- Use `MemoryRepository` as the persistence boundary; do not create parallel state stores.
- Use Polars in memory/analyzer code paths when dataframe work is needed.
- Preserve the current stdlib `logging` style unless the task is an explicit logging refactor.
- If strategy engine contracts change, inspect `bot/setup_base.py`, `bot/strategies/__init__.py`, and `bot/application/bot.py`.
- If repository schemas or record shapes change, inspect `bot/application/bot.py`, `bot/tracking.py`, and any `rg` hits for the affected methods.
- Do not assume older bridge modules exist. Verify files from the current tree before referencing them.

## Token Discipline

- Do not read the whole `bot/core/` tree. Pick the sub-area first, then trace only the direct dependencies.
- Use `rg` on event names, repository methods, or class names before opening more files.

## Verification

- Prefer targeted regression checks for the touched path.
- If no direct test exists, verify by reviewing imports, callers, and the exact async/data contracts you changed.
