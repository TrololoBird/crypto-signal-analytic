# Strategies

## Overview

Strategies are implemented in `bot/strategies/` and registered in the modern registry.  
Each strategy produces a `StrategyDecision` that can be accepted, rejected, skipped, or errored.

## Pipeline

1. Frame preparation (`bot/features.py`)
2. Strategy detection (`bot/core/engine.py`)
3. Family precheck + confirmation (`bot/application/symbol_analyzer.py`)
4. Global filters (`bot/filters.py`)
5. Confluence scoring (`bot/confluence.py`)
6. Delivery + tracking (`bot/application/delivery_orchestrator.py`)

## Family model

- `continuation`
- `breakout`
- `reversal`

Family and confirmation profile metadata are attached per strategy and used by symbol-level context checks.

## Operational notes

- Keep strategy params in `[bot.filters.setups]` config scope.
- Keep setup enable flags in `[bot.setups]`.
- Add regression coverage when changing decision contracts or metadata.
