# Архитектура проекта

## Поток данных

```text
Binance REST/WS -> market_data/ws_manager -> application/bot.py
                                        -> features.py
                                        -> strategies/*.py
                                        -> filters/scoring/confluence
                                        -> tracking + delivery
```

## Ключевые контракты
- `BaseSetup.detect(prepared, settings) -> Signal | None | StrategyDecision`
- `PreparedSymbol` содержит `work_15m/work_1h/work_4h/work_5m` и market context
- `Signal` — основной контракт торгового сигнала

## Где менять что
- Индикаторы: `bot/features.py`
- Стратегии: `bot/strategies/`
- Фильтрация: `bot/filters.py`
- Доставка: `bot/delivery.py`
- Трекинг/исходы: `bot/tracking.py`, `bot/outcomes.py`, `bot/core/memory/repository.py`
