.PHONY: check lint test dry-run run

check:
	@echo "=== Compile check ==="
	@python -m compileall -q bot
	@echo "=== Import check ==="
	@python -c "from bot.application.bot import SignalBot; print('Imports OK')"
	@echo "=== Strategy export check ==="
	@python -c "from bot.strategies import STRATEGY_CLASSES; print(f'Strategies: {len(STRATEGY_CLASSES)}')"

lint:
	@ruff check bot/ || true
	@mypy bot/ --ignore-missing-imports || true

test:
	@pytest -q tests/ || true

dry-run:
	@python main.py --mode dry-run --config config.toml

run:
	@python main.py --config config.toml
