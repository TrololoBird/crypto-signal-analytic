# ML/AI Ready Configuration Guide

## Проблема
Все параметры были хардкодированы в стратегиях:
```python
# ❌ Было: хардкод
wick_through = bar_low < candidate_level - atr * 0.3
stop = wick_bar_low - atr * 0.5
```

## Решение
Централизованный конфиг `config_strategies.toml` + чтение через `dynamic_params`:
```python
# ✅ Стало: config-driven
wick_through_atr_mult = dynamic_params.get("wick_through_atr_mult", 0.3)
wick_through = bar_low < candidate_level - atr * wick_through_atr_mult
```

## Как работает

### 1. Стратегия читает параметры:
```python
def detect(self, prepared: PreparedSymbol, settings: BotSettings) -> Signal | None:
    setup_id = self.setup_id
    dynamic_params = get_dynamic_params(prepared, setup_id)
    
    # Чтение из config_strategies.toml с дефолтом
    wick_through_atr_mult = dynamic_params.get("wick_through_atr_mult", 0.3)
    sl_buffer_atr = dynamic_params.get("sl_buffer_atr", 0.5)
```

### 2. Конфигурируем в `config_strategies.toml`:
```toml
[bot.strategies.wick_trap_reversal]
wick_through_atr_mult = 0.3
sl_buffer_atr = 0.5
min_rr = 1.5
```

### 3. ML/AI может оптимизировать:
```python
# Автоматическая калибровка через self-learner
from bot.autotuner import SelfLearner

learner = SelfLearner()
# Оптимизирует параметры на основе исторических результатов
best_params = learner.optimize_strategy("wick_trap_reversal")
# Обновляет config_strategies.toml автоматически
```

## Шаблон миграции стратегии

### Шаг 1: Найти хардкод
```python
# Найти все магические числа
atr * 0.3
vol_ratio >= 1.0
score >= 0.50
```

### Шаг 2: Заменить на config-driven
```python
# Добавить в начало detect()
dynamic_params = get_dynamic_params(prepared, setup_id)
wick_mult = dynamic_params.get("wick_through_atr_mult", 0.3)
vol_threshold = dynamic_params.get("vol_ratio_threshold", 0.8)
min_score = dynamic_params.get("min_trend_score", 0.40)
```

### Шаг 3: Добавить в config_strategies.toml
```toml
[bot.strategies.strategy_name]
wick_through_atr_mult = 0.3
vol_ratio_threshold = 0.8
min_trend_score = 0.40
```

## Параметры для ML оптимизации

### Детекция (detection thresholds)
- `wick_through_atr_mult` — чувствительность wick
- `vol_ratio_threshold` — минимальный объём
- `min_trend_score` — порог тренда
- `break_atr_mult` — порог пробоя

### Risk Management
- `sl_buffer_atr` — размер стоп-лосса
- `min_rr` — минимальный risk/reward
- `tp_too_close_penalty` — штраф за близкий TP

### Скоринг
- `base_score` — базовый скор сигнала
- `bias_mismatch_penalty` — штраф несоответствия
- `*_weight` — веса различных факторов

## ML Pipeline

```python
# 1. Сбор данных
outcomes = repository.get_strategy_outcomes("wick_trap_reversal")

# 2. Feature engineering
features = {
    "wick_mult": current_params["wick_through_atr_mult"],
    "win_rate": calculate_win_rate(outcomes),
    "avg_profit": calculate_avg_profit(outcomes),
    "max_drawdown": calculate_drawdown(outcomes)
}

# 3. Optimization (Bayesian / Genetic / Gradient)
optimizer = BayesianOptimizer()
best_params = optimizer.optimize(
    objective=lambda p: sharpe_ratio(p, outcomes),
    bounds={"wick_mult": (0.1, 0.5), "sl_buffer": (0.3, 1.0)}
)

# 4. Update config
update_config("wick_trap_reversal", best_params)
```

## Статус миграции

| Стратегия | Хардкод удалён | Config-driven | ML Ready |
|-----------|----------------|---------------|----------|
| wick_trap_reversal | ✅ | ✅ | ✅ |
| Остальные 14 | ❌ | ❌ | ❌ |

## Следующие шаги
1. ✅ Создан config_strategies.toml
2. ✅ Шаблон миграции (wick_trap_reversal)
3. ⏳ Мигрировать остальные 14 стратегий
4. ⏳ Интеграция с SelfLearner
5. ⏳ Автоматическая калибровка
