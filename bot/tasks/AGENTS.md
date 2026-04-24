# Tasks Agent

## Scope

Work in `bot/tasks/`: scheduler, scanner, reporter, tracker.

## Important Context

Some task modules are infrastructure scaffolds or partially integrated. Do not assume they are active in the main runtime. Verify call sites with `rg` before making architecture claims.

## Read Before Editing

- The touched task module
- Its direct dependencies in `bot/core/` or `bot/telegram/`
- Any actual callers found with `rg`

## Local Rules

- Keep tasks async and cancellation-safe.
- Preserve scheduler/reporter/tracker boundaries instead of collapsing them into one module.
- Avoid inventing background loops that duplicate behavior already owned by `bot/application/bot.py`.
- If a task depends on repository, engine, or telegram contracts, verify the live interfaces before editing.
- Mark placeholders as placeholders; do not document them as fully integrated behavior unless the code proves it.

## Token Discipline

- Start with usage search. If a task has no callers, treat it as isolated scaffolding until proven otherwise.
- Read only the touched task and the exact dependency modules it imports.

## Verification

- Prefer targeted tests for scheduler semantics or task contracts.
- If there are no tests for the touched task, verify by tracing callers and checking lifecycle correctness.
