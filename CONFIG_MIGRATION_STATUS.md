# Статус миграции конфигураций

## ✅ Завершено

### 1. Загрузчик конфигов
- **Файл**: `bot/config_loader.py`
- **Функции**:
  - `load_strategy_config()` — загружает TOML конфиг стратегии
  - `get_nested()` — получает вложенные значения
  - `load_global_strategy_config()` — глобальные настройки

### 2. Конфиг-файлы созданы (7 стратегий)

| Стратегия | Конфиг-файл | Статус |
|-----------|-------------|--------|
| turtle_soup | ✅ `config/strategies/turtle_soup.toml` | ✅ Код мигрирован |
| structure_pullback | ✅ `config/strategies/structure_pullback.toml` | ⏳ Код НЕ мигрирован |
| squeeze_setup | ✅ `config/strategies/squeeze_setup.toml` | ⏳ Код НЕ мигрирован |
| ema_bounce | ✅ `config/strategies/ema_bounce.toml` | ⏳ Код НЕ мигрирован |
| session_killzone | ✅ `config/strategies/session_killzone.toml` | ⏳ Код НЕ мигрирован |
| liquidity_sweep | ✅ `config/strategies/liquidity_sweep.toml` | ⏳ Код НЕ мигрирован |
| wick_trap_reversal | ✅ `config_strategies.toml` | ✅ Уже был мигрирован |

### 3. Стратегия с мигрированным кодом
**turtle_soup.py** — полностью на config-driven параметрах:
```python
# Было:
_ROLL_BARS = 20
_BREAK_ATR_MULT = 0.1
if w1h.height < _ROLL_BARS + 3:

# Стало:
config = load_strategy_config("turtle_soup")
roll_bars = get_nested(config, "detection.roll_bars", 20)
break_atr_mult = get_nested(config, "detection.break_atr_mult", 0.1)
if w1h.height < roll_bars + 3:
```

## ⏳ Осталось сделать

### Миграция кода (14 стратегий)
Нужно заменить хардкод в каждой стратегии на чтение из конфига:

1. structure_pullback.py
2. squeeze_setup.py
3. ema_bounce.py
4. session_killzone.py
5. liquidity_sweep.py
6. order_block.py
7. hidden_divergence.py
8. funding_reversal.py
9. cvd_divergence.py
10. breaker_block.py
11. structure_break_retest.py
12. bos_choch.py
13. fvg.py
14. wick_trap_reversal.py (дополнить оставшиеся параметры)

### Шаблон миграции для каждой стратегии:
```python
# 1. Добавить импорт
from ..config_loader import load_strategy_config, get_nested

# 2. В начале detect() добавить:
config = load_strategy_config("strategy_name")
param = get_nested(config, "section.param", default_value)

# 3. Заменить все константы на переменные
```

## Использование

Теперь для изменения параметров:
```toml
# config/strategies/turtle_soup.toml
[detection]
roll_bars = 25  # Было 20, теперь легко поменять!
break_atr_mult = 0.15  # Было 0.1
```

Без перезапуска бота — параметры читаются при каждом вызове detect()!

## Следующие шаги
1. ⏳ Мигрировать код оставшихся 14 стратегий (3-4 часа)
2. ⏳ Создать единый config_strategies.toml с глобальными умолчаниями
3. ⏳ Добавить валидацию конфигов при загрузке
