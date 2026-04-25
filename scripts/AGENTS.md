# Scripts Agent

## Scope

Work in `scripts/`, which contains manual/live validation helpers.

## Local Rules

- Treat these scripts as operator tools, not production runtime code.
- Keep live scripts read-only and diagnostic unless the task explicitly requires something else.
- Make network or credential assumptions explicit before claiming a script can run successfully.
- If a script depends on current exchange behavior or external APIs, verify those claims when possible and record the exact script used.
- Do not move core business logic into `scripts/`; shared logic belongs under `bot/`.
- New helper modules/functions in `scripts/` are allowed only after checking for an existing equivalent in
  `scripts/common.py` and `bot/core/diagnostics/`. Reuse or extend existing helpers first; duplicate helpers are
  not allowed.

## Token Discipline

- Open only the specific script relevant to the task.
- Read runtime modules only when the script imports or exercises them directly.

## Verification

- State clearly whether a live script was actually run or only reviewed.
