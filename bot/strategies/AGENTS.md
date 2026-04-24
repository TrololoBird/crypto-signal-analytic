# Strategies Agent

## Scope

Work in `bot/strategies/` for individual setup detectors.

## Read Before Editing

- The target strategy file
- `bot/setup_base.py`
- `bot/models.py`
- `bot/setups.py`
- `bot/setups/utils.py`
- Matching keys in `config.toml` and `config.toml.example`

## Strategy Contract

- Inherit from `BaseSetup`
- Keep a stable `setup_id`
- Implement `detect(prepared, settings) -> Signal | None`
- Implement `get_optimizable_params(settings | None) -> dict[str, float]`
- Return `bot.models.Signal`, not ad hoc dicts or tuples

## Local Rules

- `setup_id` must match the keys under `[bot.filters.setups]` and `[bot.setups]`.
- Prefer shared helpers over duplicating RR, SL/TP, scoring, or rejection logic.
- Guard frame height and required columns before `item(-1)`, `item(-2)`, or similar indexed access.
- Keep dataframe logic Polars-native.
- Put tunable thresholds in config; hardcode only true invariants.
- If you add or rename a strategy, update `bot/strategies/__init__.py`, config files, and any registry/caller references in the same change.
- Keep signal fields consistent with `bot/models.py` semantics: `direction`, `entry_low`, `entry_high`, `stop`, `take_profit_1`, `take_profit_2`.

## Token Discipline

- Compare against one or two similar strategies, not all 15.
- Read helpers first; many strategies share the same target-building and scoring patterns.

## Verification

- Check config key alignment and strategy exports after every strategy change.
- Run targeted regression coverage if the changed behavior is exercised by tests; otherwise verify the contract by tracing the call path into `bot/application/bot.py`.
