# Test Agent

## Scope

Work in `tests/`, currently centered on `test_remediation_regressions.py`.

## Role

Tests here are regression-focused and mostly validate contract behavior with async stubs, mocks, and Polars frames.

## Local Rules

- Add or change tests only for concrete regressions, contracts, or behavior that the code actually guarantees.
- Keep tests focused and explicit; avoid broad snapshots or speculative coverage.
- Use lightweight builders/stubs when possible, following the existing test style.
- Default test runs should stay offline. Live/manual API checks belong in `scripts/live_*.py`.
- If an interface changes, update the relevant builders and assertions in the same change.

## Token Discipline

- Start from the failing or relevant test block, not the whole file.
- Mirror existing helpers before introducing new fixture layers.

## Verification

- Run the narrowest `pytest` selection that covers the changed behavior.
- If no test is added, explain why the behavior was verified another way.
