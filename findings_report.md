# Findings Report

## Confirmed Fixes
- Plain `pytest -q` bootstrap is normalized through `pyproject.toml` `pythonpath = ["."]`.
- The 5 concrete pyright errors were addressed in `bot/delivery.py`, `bot/public_intelligence.py`, and `bot/ws_manager.py`.
- `config.toml.example` now mirrors the live `[bot.filters.setups]` surface from `config.toml`.
- Optional runtime dependencies are classified in `requirements-optional.txt` instead of being implicit ambient state.

## Confirmed Remaining Risks
- `PreparedSymbol.spot_lead_return_1m` and `PreparedSymbol.spot_futures_spread_bps` are declared but not populated anywhere on the live path.
- `bot/features.py` still carries backward-compat neutral placeholder columns.
- `bot/market_regime.py` still mixes benchmark context with raw 24h ticker-change aggregates.
- Quarantine-first candidates remain in-tree because no deletion proof beyond static reachability was gathered for external consumers.

## Historical Telemetry Evidence
- HYPEUSDT long: stop=40.68290871 entry=41.09420690 tp1=43.17900000 tp2=42.35400000
- FETUSDT short: equal targets emitted tp1_hit and tp2_hit
