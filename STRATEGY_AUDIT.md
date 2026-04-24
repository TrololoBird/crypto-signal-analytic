# Полный аудит стратегий — Stop Loss Analysis

## Стратегии и их текущие ATR-множители для SL

| Стратегия | Файл | ATR Buffer | Статус |
|-----------|------|------------|--------|
| wick_trap_reversal | wick_trap_reversal.py | 0.5 ATR | ✅ ИСПРАВЛЕНО |
| turtle_soup | turtle_soup.py | 0.5 ATR | ✅ ИСПРАВЛЕНО |
| structure_pullback | structure_pullback.py | 0.4 ATR | ✅ ИСПРАВЛЕНО |
| squeeze_setup | squeeze_setup.py | 0.4 ATR | ✅ ИСПРАВЛЕНО |
| fvg | fvg.py | 0.1 ATR | ❌ НУЖНО 0.5 |
| liquidity_sweep | liquidity_sweep.py | 0.1 ATR | ❌ НУЖНО 0.5 |
| order_block | order_block.py | 0.15 ATR | ❌ НУЖНО 0.5 |
| session_killzone | session_killzone.py | 0.15 ATR | ❌ НУЖНО 0.5 |
| hidden_divergence | hidden_divergence.py | 0.15 ATR | ❌ НУЖНО 0.5 |
| funding_reversal | funding_reversal.py | 0.1 ATR | ❌ НУЖНО 0.5 |
| cvd_divergence | cvd_divergence.py | 0.15 ATR | ❌ НУЖНО 0.5 |
| breaker_block | breaker_block.py | 0.2 ATR | ❌ НУЖНО 0.5 |
| ema_bounce | ema_bounce.py | 0.2 ATR (через utils) | ❌ НУЖНО 0.4 |
| structure_break_retest | structure_break_retest.py | TBD | ❌ НУЖНО ПРОВЕРИТЬ |
| bos_choch | bos_choch.py | TBD | ❌ НУЖНО ПРОВЕРИТЬ |

## Shared Utilities
| Файл | ATR Buffer | Статус |
|------|------------|--------|
| setups/utils.py | 0.2 ATR | ❌ НУЖНО 0.4 |

## Целевые значения
- **0.5 ATR** для высоковолатильных стратегий (fvg, liquidity_sweep, funding_reversal)
- **0.4 ATR** для остальных стратегий (order_block, session_killzone, и т.д.)

## Таймфреймы (все стратегии)
- **work_1h** — основной для тренда и структуры
- **work_15m** — основной для сигналов
- **work_4h** — контекст (bias_4h)

## Индикаторы используемые в стратегиях
- EMA (20, 50, 200)
- RSI (14)
- ADX (14)
- ATR (14)
- VWAP
- Bollinger Bands
- Volume ratio
- Donchian channels
- MACD
- Supertrend
