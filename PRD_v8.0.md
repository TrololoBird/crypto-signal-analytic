# PRD: Crypto Futures Signal Bot v8.0

## 1. Overview
Automated signal detection for Binance Futures. 15 strategies, config-driven parameters, Telegram delivery.

## 2. Key Metrics
- Signals/day: >50
- Win rate: >55%
- Uptime: >99.5%
- Latency: <2s

## 3. Core Requirements
1. **Data**: WebSocket to Binance, 45 symbols, 4 timeframes
2. **Detection**: 15 strategies, TOML config, no code changes for tuning
3. **Risk**: ATR stops (0.4-0.5 buffer), R/R >1.5, cooldown logic
4. **Delivery**: Telegram, rich format, <2s latency
5. **Learning**: Outcome tracking, adaptive scoring per setup

## 4. Architecture
Event-driven async architecture:
- EventBus for decoupled communication
- SignalEngine with StrategyRegistry
- MemoryRepository (SQLite) for persistence
- TelegramQueue for reliable delivery

## 5. Configuration System
```
config/strategies/{name}.toml  # Per-strategy parameters
config_strategies.toml         # Global defaults
```
All 15 strategies migrated to config-driven parameters.

## 6. Strategies (15 total)
1. BOS/CHoCH
2. Breaker Block
3. CVD Divergence
4. EMA Bounce
5. Funding Reversal
6. FVG
7. Hidden Divergence
8. Liquidity Sweep
9. Order Block
10. Session Killzone
11. Squeeze Setup
12. Structure Break Retest
13. Structure Pullback
14. Turtle Soup
15. Wick Trap Reversal

## 7. Out of Scope
- Automated execution (signals only)
- Backtesting engine
- Web dashboard
- Multi-exchange support
- Copy trading

## 8. Future Roadmap
- ML-based parameter optimization
- Hot config reload
- Performance analytics API
- Mobile app companion
