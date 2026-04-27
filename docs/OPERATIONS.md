# Operations

## Local commands

- `make check` — full local checks.
- `make lint` — lint and type checks.
- `make test` — run tests.
- `make run` — start bot runtime.
- `make dry-run` — run without sending live deliveries.
- Targeted remediation suites:
  - `pytest -q tests/test_remediation_intra_candle.py`
  - `pytest -q tests/test_remediation_indicators.py`
  - `pytest -q tests/test_remediation_regressions.py`

## Recommended routine

1. Validate config values in `config.toml`.
2. Run `make check`.
3. Start with `make dry-run`.
4. Inspect telemetry logs (`telemetry/`) and repository state.
5. Enable live mode only after dry-run sanity checks.

## Runtime health signals

- heartbeat metrics (health manager)
- tracking queue/drain status
- memory repository summary
- data quality and strategy decision JSONL traces
- emergency fallback checks (`fallback_checks.jsonl`)
- cycle/symbol telemetry emitted via `TelemetryManager`

## ML training quality gates

- Use `python -m bot.ml.train` with `--report` to persist machine-readable walk-forward output.
- `summary` metrics are aggregated with `test_rows` weights, so larger validation windows have proportionally larger impact.
- Use gates for CI/non-interactive validation:
  - `--min-windows` (expected minimum number of evaluated windows),
  - `--min-accuracy`, `--min-precision`, `--min-recall`, `--min-f1` (all bounded in `[0.0, 1.0]`).
- CLI exits with code `2` when any enabled gate fails and includes failure reasons in `quality_gate.failures`.
- Runtime ML integration should import `MLFilter` from `bot.ml` (canonical path). The top-level module `bot.ml_filter` is kept only as a backward-compatible shim.
- If runtime fallback resolves to `centroid_baseline`, live ML scoring is auto-disabled as a safety guardrail.

## Incident checklist

1. Verify exchange connectivity and WS status.
2. Confirm fresh klines/market context.
3. Inspect reject reasons in telemetry.
4. Validate cooldown/blacklist status in repository.
5. Restart only after root cause is identified.
