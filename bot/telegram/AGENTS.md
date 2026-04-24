# Telegram Agent

## Scope

Work in `bot/telegram/`: queueing and sending infrastructure.

## Important Context

This package provides reusable Telegram infra. Do not assume it is the only messaging path in the repo; verify call sites before claiming a runtime integration path.

## Read Before Editing

- The touched file in `bot/telegram/`
- `bot/messaging.py` or `bot/telegram_bot.py` if the task is about end-to-end delivery
- Any queue/sender callers found with `rg`

## Local Rules

- Keep the flow async end-to-end.
- Preserve queue semantics: priority, deduplication, rate limiting, retry, and shutdown.
- Preserve sender semantics: circuit breaker, retry behavior, optional aiogram dependency.
- Do not add blocking sleeps or synchronous network calls.
- Keep message payload contracts explicit; avoid hidden kwargs coupling between queue and sender.

## Token Discipline

- For queue-only work, do not read unrelated bot runtime files.
- For sender-only work, inspect the queue contract and the exact caller path, not the whole project.

## Verification

- Verify queue/sender contracts together when either side changes.
- If integration is touched, also verify the actual caller path rather than assuming this package is active.
