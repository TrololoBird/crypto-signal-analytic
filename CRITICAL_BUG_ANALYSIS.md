# 🔴 КРИТИЧЕСКИЙ БАГ: 0% Accept Rate

## Диагноз

### Проблема #1: Детекторы не находят паттерны (92.6% no_raw_hit)
**Причина**: Все 15 стратегий возвращают `None` из-за незаполненных полей.

### Проблема #2: Критические поля 100% NULL
| Поле | Используется в | Влияние |
|------|---------------|---------|
| `adx_1h` | ADX-фильтр | Сигналы без ADX отклоняются |
| `risk_reward` | R/R фильтр | Сигналы без R/R отклоняются |
| `trend_direction` | Trend-фильтр | Сигналы без trend отклоняются |
| `premium_zscore_5m` | Confirmation | Всегда NULL |
| `premium_slope_5m` | Confirmation | Всегда NULL |
| `oi_change_pct` | OI-фильтр | Всегда NULL |
| `ls_ratio` | Sentiment | Всегда NULL |

## Корень проблемы

### 1. Поля определены в PreparedSymbol, но нигде не заполняются:
```python
# models.py - только объявления:
premium_zscore_5m: float | None = None
premium_slope_5m: float | None = None
adx_1h: float | None = None  # нет такого поля!
```

### 2. Индикаторы считаются в DataFrame, но не копируются в PreparedSymbol:
```python
# features.py считает индикаторы:
work_1h = work_1h.with_columns([
    pl.col("close").ta.adx(14).alias("adx_1h"),
])

# Но adx_1h не сохраняется как отдельное поле PreparedSymbol!
```

### 3. Telemetry ожидает поля которых нет:
```python
# runtime_audit.py пытается читать:
"adx_1h": signal.get("adx_1h"),  # всегда None!
"risk_reward": signal.get("risk_reward"),  # всегда None!
```

## Почему стратегии не находят сигналы

1. Стратегии проверяют `adx_1h` через `work_1h` DataFrame ✅
2. Но фильтры проверяют поля `Signal` dataclass ❌
3. `Signal` создаётся без `adx_1h`, `risk_reward`, `trend_direction`
4. Фильтры отклоняют сигналы из-за отсутствия обязательных полей
5. Получаем 0% accept rate

## Что нужно исправить

### Fix #1: Добавить поля в Signal dataclass
```python
@dataclass
class Signal:
    # ... existing fields ...
    adx_1h: float | None = None  # NEW
    risk_reward: float | None = None  # NEW
    trend_direction: str | None = None  # NEW
    trend_score: float | None = None  # NEW
```

### Fix #2: Заполнять поля при создании сигнала
```python
# В каждой стратегии при создании Signal:
return Signal(
    # ... existing fields ...
    adx_1h=prepared.work_1h.item(-1, "adx"),
    risk_reward=abs(take_profit_1 - entry) / abs(entry - stop),
    trend_direction=prepared.bias_1h,
)
```

### Fix #3: Fix premium_zscore расчёт
```python
# Добавить в _enrich_prepared:
if prepared.work_5m is not None:
    prepared.premium_zscore_5m = calculate_zscore(
        prepared.work_5m, "mark_index_spread_bps"
    )
```

## Срочность
**🔴 CRITICAL** - Бот не работает совсем (0% сигналов)

## Временное решение
Отключить фильтры adx_1h, risk_reward, trend_direction до фикса.
