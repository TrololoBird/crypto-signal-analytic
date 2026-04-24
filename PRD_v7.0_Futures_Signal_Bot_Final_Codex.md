> Status note (2026-04-23): this file is historical design context, not the
> authoritative runtime description for the current repo. The live code path is
> `main.py -> bot.cli.run() -> bot.application.bot.SignalBot`, and the
> multi-process/shared-memory/notifier architecture below is not implemented
> as-is in the current tree.

# PRD v7.0 — Quantitative Futures Signal Bot
## Binance USDⓈ-M Perpetual Futures | Multi-Process | Signal-Only | CPython 3.13+

**Версия:** 7.0 (Final, Codex Single-Pass)
**Дата:** 21 апреля 2026
**Статус:** Production-Ready Specification
**Архитектура:** Multi-Process (Collector / Brain / Notifier) + Shared Memory IPC
**Target Platform:** Windows 11 / Linux VPS
**Python:** CPython 3.13+ (verified compatible stack)
**Execution Model:** Single-pass implementation (no staged roadmap)

---

## 1. Executive Summary

### 1.1 Vision
Автономный quantitative signal-only бот для Binance USDⓈ-M Perpetual Futures, построенный на мультипроцессной архитектуре с zero-copy IPC, Polars-native аналитикой и структурным (Smart Money Concepts) подходом к генерации сигналов. Все данные — публичные. Авто-торговля отсутствует.

### 1.2 Core Principles
1. **Structure over Fixed R:R** — стопы и тейки строятся исключительно по структурным уровням (BOS, CHoCH, OB, FVG, liquidity), никаких фиксированных соотношений риск/прибыль.
2. **Polars-First** — весь data pipeline от ingestion до indicators использует Polars (Lazy API), numpy только для математических констант.
3. **Zero-Copy IPC** — shared memory на Windows через `multiprocessing.shared_memory`, сериализация через `msgspec` (вместо pickle).
4. **Official SDK Priority** — REST и WS через `binance-sdk-derivatives-trading-usds-futures` (>=9.1.0), fallback на raw `websockets` >=15.0.1 для новых endpoint'ов (/public, /market).
5. **Dual Data Source** — USD-M Futures (primary) + Spot (secondary для базиса и арбитражной фильтрации).

### 1.3 Success Metrics

| Метрика | Цель |
|---------|------|
| Win-rate сигналов (4h hold) | ≥ 60% |
| Средний confidence публикуемых сигналов | ≥ 72/100 |
| Latency (close → Telegram) | < 3 сек |
| Uptime | ≥ 99.5% |
| Сигналов в минуту | ≤ 3 |

---

## 2. Technical Stack (CPython 3.13+ Verified)

| Компонент | Библиотека | Версия | CPython 3.13 |
|-----------|-----------|--------|--------------|
| **Runtime** | CPython | 3.13+ | ✅ Target |
| **Binance REST** | binance-sdk-derivatives-trading-usds-futures | >=9.1.0 | ✅ |
| **Binance WS** | binance-connector | >=3.12.0 | ✅ |
| **Async HTTP** | aiohttp | >=3.11.0 | ✅ |
| **WS Client** | websockets | >=15.0.1,<16.0.0 | ✅ |
| **DataFrame** | polars | >=1.39.3 | ✅ |
| **TA Engine** | polars_ta | >=0.3.0 | ✅ Pure Polars, no numba |
| **Math** | numpy | >=1.26.4,<2.5 | ✅ 2.x supports 3.13 |
| **Telegram** | aiogram | >=3.27.0 | ✅ |
| **Config** | pydantic | >=2.8.0 | ✅ |
| **Env** | python-dotenv | >=1.0.0 | ✅ |
| **DB** | aiosqlite | >=0.22.1 | ✅ |
| **Logs** | structlog | >=24.1.0 | ✅ |
| **Retry** | tenacity | >=9.0.0 | ✅ |
| **Metrics** | prometheus-client | >=0.19.0 | ✅ |
| **Serialization** | msgspec | >=0.19.0 | ✅ Faster than pickle |
| **JSON** | orjson | >=3.9.0 | ✅ |
| **Testing** | pytest, pytest-asyncio, pytest-xdist | >=8.3, >=0.25, >=3.6 | ✅ |

**Removed from stack:**
- `pandas` / `pandas-ta` — replaced by Polars + `polars_ta`.
- `unicorn-binance-websocket-api` — overkill, official SDK sufficient.
- `ta-lib` / `numba` — C-extension hell, incompatible with some 3.13 builds.

---

## 3. Multi-Process Architecture

### 3.1 Process Map

```
ORCHESTRATOR (main.py)
  ├── COLLECTOR (asyncio)
  │     WS (UMFuturesWebsocketClient + raw fallback)
  │     REST (official SDK, two limiters)
  │     SharedMemory writer (OHLCV ring buffer)
  │     SQLite: market_data.db (WAL)
  │
  ├── BRAIN (asyncio + ProcessPoolExecutor(4))
  │     SharedMemory reader (zero-copy)
  │     deriv_queue reader (msgspec)
  │     Indicator workers (Polars-native)
  │     Strategy registry (15 strategies)
  │     Confidence + Structural Risk
  │     signal_queue writer (msgspec)
  │
  └── NOTIFIER (asyncio)
        signal_queue reader (msgspec)
        aiogram → Telegram (HTML)
        Retry queue (exponential backoff)
        SQLite: signals.db (WAL)
```

### 3.2 IPC Channels

| Канал | Технология | Данные | Частота |
|-------|-----------|--------|---------|
| OHLCV Buffer | `multiprocessing.shared_memory` | Ring buffer float64[6,45,500,8] | Real-time |
| Derivatives | `multiprocessing.Queue` + `msgspec` | Snapshot (OI, funding, L/S) | 1/5-15 min |
| Signals | `multiprocessing.Queue` + `msgspec` | ValidatedSignal | 1-2/min |
| Alerts | `multiprocessing.Queue` | Emergency strings | Rare |

**Why `msgspec` over `pickle`:** 5-10x faster serialization, strict schema validation, smaller payload.

---

## 4. Data Sources (Primary + Secondary)

### 4.1 Primary: Binance USDⓈ-M Futures

**WebSocket Streams:**
- Conn A (high-freq): 45 pairs × `kline_15m`, `kline_1h`, `bookTicker` + `!markPrice@arr`.
- Conn B (low-freq): 45 pairs × `kline_4h`, `kline_1d` + `!forceOrder@arr`.
- Limit: 1024 streams/conn, 10 incoming msg/sec/conn.
- Post-2026-04-23: use `/public` and `/market` paths (websockets>=15.0.1).

**REST Endpoints:**
| Endpoint | Weight | Limit | Notes |
|----------|--------|-------|-------|
| `GET /fapi/v1/klines` | 2 (limit≤500) | 2400 weight/min | Gap-fill, preload |
| `GET /fapi/v1/premiumIndex` | 5 | 2400 weight/min | Emergency backup |
| `GET /fapi/v1/openInterest` | **1** | 2400 weight/min | **Single symbol only, NO batch** |
| `GET /futures/data/openInterestHist` | 0 | **1000 req/5min** | **IP request limit, not weight** |
| `GET /futures/data/globalLongShortAccountRatio` | 0 | **1000 req/5min** | IP request limit |
| `GET /futures/data/topLongShortPositionRatio` | 0 | **1000 req/5min** | IP request limit |
| `GET /futures/data/takerlongshortRatio` | 0 | **1000 req/5min** | IP request limit |

### 4.2 Secondary: Binance Spot

- `GET /api/v3/avgPrice` — базис (futures premium/discount).
- `GET /api/v3/klines` — Spot OHLCV для корреляции и арбитража.
- **Usage:** Strategy C12 (Basis Arbitrage) + Premium Divergence filter.

### 4.3 Dual Rate Limiters

```python
# Limiter 1: Weight-based (/fapi/*)
weight_limiter = TokenBucket(capacity=2400, refill_per_sec=40)

# Limiter 2: Request-based (/futures/data/*)
request_limiter = TokenBucket(capacity=1000, refill_per_sec=3.33)  # per 300s
```

**OI Polling (sequential, no batch):**
- 45 symbols × 1 request = 45 req every 5 min.
- Weight: 9/min. Request: 45/5min = 4.5% of budget.

---

## 5. WebSocket Payload Specification (Verified)

### 5.1 Kline Stream (`<symbol>@kline_<tf>`)

Critical fields for parsing:
- `k.t` — open time (ms)
- `k.T` — close time (ms)
- `k.o`, `k.h`, `k.l`, `k.c` — OHLC (strings → cast to float)
- `k.v` — volume (base asset)
- `k.q` — quote volume
- `k.V` — taker buy base volume
- `k.Q` — taker buy quote volume
- `k.n` — number of trades
- **`k.x`** — **is this kline closed?** (bool). **Only process when `k.x == true`.**

### 5.2 Mark Price (`!markPrice@arr`)

- Default interval: **3 seconds** (not 1s).
- Fields:
  - `s` — symbol
  - `p` — markPrice
  - `i` — indexPrice
  - **`r`** — **lastFundingRate** (float)
  - `T` — nextFundingTime (ms)

### 5.3 Force Order (`!forceOrder@arr`)

- Snapshot: only the **largest liquidation per symbol per 1s** is pushed.
- Fields: `o.S` (symbol), `o.s` (side), `o.q` (qty), `o.p` (price), `o.z` (avgPrice).

---

## 6. Shared Memory Layout (Windows)

```python
class SharedMarketBuffer:
    MAX_PAIRS = 45
    MAX_TFS = 6        # 1m, 5m, 15m, 1h, 4h, 1d
    MAX_BARS = 500
    FIELDS = 8         # time, open, high, low, close, volume, taker_buy_base, taker_buy_quote
    
    # Layout:
    # [0:48MB]      data: float64[45, 6, 500, 8]
    # [48MB:48.1MB] head: uint32[45, 6] — ring buffer pointers
    # [48.1MB:48.2MB] metadata: symbol registry, last_update_ts, warm flags
```

**Ring Buffer Logic:**
- `head = (head + 1) % MAX_BARS` on write.
- Brain reads `min(head, required_bars)` bars.
- Lock: `multiprocessing.Lock` only on head increment (~50ns).
- Brain reads data without lock (read-only).

---

## 7. Database (Two Isolated SQLite DBs)

| БД | Процесс | Таблицы | Mode |
|----|---------|---------|------|
| `market_data.db` | Collector (писатель) | ohlcv, market_state, liquidations | WAL |
| `signals.db` | Notifier (писатель) | signals, deliveries, errors | WAL |

**No write contention.** Brain does not write to SQLite.

---

## 8. Таймфреймы

| ТФ | WS Stream | Назначение | Стратегии | Min Bars |
|----|-----------|------------|-----------|----------|
| **1m** | `@kline_1m` | Скальпинг, точность входа | F6 (Liquidation), C1 (Confluence) | 210 |
| **5m** | `@kline_5m` | Краткосрочный моментум | T5 (Supertrend), F3 (OI Flush) | 210 |
| **15m** | `@kline_15m` | Intraday setups | T1 (RSI), T3 (BB), C1, F6 | 210 |
| **1h** | `@kline_1h` | Основной сигнальный | T2, T7, F1, F4, F5, C2, C3 | 210 |
| **4h** | `@kline_4h` | Свинг | T4 (EMA Cross), F2 (OI Buildup), C2 | 210 |
| **1d** | `@kline_1d` | Структурный bias | HTF filter only | 50 |

**MTF Confluence Rule:**
- Сигнал на 15m/1h требует подтверждения 4h bias (Bullish/Bearish/Neutral).
- Сигнал на 4h требует подтверждения 1d bias.
- Brain принимает символ только если `warm=True` и `rows >= 210` для всех нужных ТФ.

---

## 9. Indicator Stack (Polars-Native, 70+)

| Категория | Индикаторы | Библиотека |
|-----------|-----------|------------|
| **Momentum** | RSI(6/14), StochRSI, Williams %R, CCI, ROC, UO | polars_ta |
| **Trend** | EMA(9/21/50/100/200), SMA, DEMA, TEMA, KAMA, MACD, TRIX, Supertrend | polars_ta |
| **Volatility** | BB(20,2/2.5/3), Keltner, Donchian, ATR(14), NATR | polars_ta + custom |
| **Volume** | OBV, VWAP, MFI, CMF, AD, Force Index, Klinger Volume | polars_ta |
| **Volume Profile** | POC, VAH, VAL (histogram over last N bars) | Custom Polars |
| **Breadth** | ADX(14), DMI(+/-DI), Elder Ray, Bull/Bear Power | polars_ta |
| **Pattern** | Doji, Engulfing, Hammer, Morning Star | polars_ta |
| **Futures Custom** | Funding Z-Score, Premium Deviation, OI Slope, CVD, Liquidation Density | Custom Polars/numpy |

**Custom Indicators (Polars Expressions):**
- **CVD**: `(taker_buy_base - (volume - taker_buy_base)).cum_sum()`
- **Volume Profile**: `pl.col('close').hist(bins=50, weights='volume')` → POC=argmax, VAH/VAL=percentiles.
- **Funding Z-Score**: `(current_funding - mean_90d) / std_90d`
- **OI Slope**: `oi.rolling_mean(24).diff()`
- **Liquidation Density**: rolling sum of `forceOrder` values over 15m window.

---

## 10. Strategy Portfolio (15 Strategies)

### 10.1 Structure-Based Core (Smart Money Concepts)

| ID | Стратегия | ТФ | Триггер | Структурный SL | Структурный TP | Фильтры |
|----|-----------|-----|---------|----------------|----------------|---------|
| **S1** | **BOS + OI Confirmation** | 1h, 4h | Break of Structure (close beyond previous high/low) + OI рост > 3% | За противоположной стороной BOS | Противоположный OB / предыдущий CHoCH | Volume > 1.5x SMA, Funding не против |
| **S2** | **Order Block Mitigation** | 1h, 4h | Price returns to bullish/bearish OB + Volume Imbalance (buy/sell delta > 60%) | За дальней границей OB | Ближайший FVG / BOS target | OI flat или растёт, Premium < 0.1% |
| **S3** | **FVG + CVD Divergence** | 15m, 1h | Fair Value Gap unfilled + CVD diverges from price (absorption) | За ближайшей границей FVG | Противоположный FVG / liquidity pool | Funding neutral, L/S Top Pos < 0.7 |
| **S4** | **Liquidity Sweep + OI Flush** | 5m, 15m | Price sweeps liquidity (takes out previous high/low) + immediate reversal + OI drops > 2% | За экстремумом sweep'а | Ближайший OB / 50% retracement | Ликвидации > $200K за 5m, CVD против |
| **S5** | **Breaker Block + Funding Filter** | 1h, 4h | Previous support becomes resistance (breaker) + price rejects + funding противоположен направлению | За хай/лоу breaker formation | Противоположный BOS | Funding имеет знак ПРОТИВ сигнала |

### 10.2 Futures Microstructure

| ID | Стратегия | ТФ | Триггер | Структурный SL | Структурный TP | Фильтры |
|----|-----------|-----|---------|----------------|----------------|---------|
| **F1** | **Smart Money Trap** | 1h, 4h | Top Position Ratio < 0.35 при Global Account Ratio > 2.5 + price at structural level (OB/FVG) | За структурным high/low | Противоположный liquidity pool | Дивергенция price/OI, funding против retail |
| **F2** | **OI Buildup Breakout** | 4h | OI вырос > 5% за 4h + BOS + CVD растёт в направлении пробоя | За broken structure (retest zone) | Следующий BOS / opposing OB | Объём > 1.5x SMA, funding не против |
| **F3** | **OI Flush Reversal** | 5m, 15m | Цена движется, OI падает > 3% за 15m + CVD против цены | За хай/лоу импульсной свечи | Ближайший OB / 50% range | Funding нейтральный |
| **F4** | **Premium Divergence** | 1h | Premium > 0.15% и расширяется + price at structural resistance/support | За structural extreme | Ближайший FVG fill | Mark diverges from index, funding flat |
| **F5** | **Liquidation Exhaustion** | 5m | Rolling ликвидации > $500K за 5 мин + отскок > 50% диапазона + OI падает | За хай/лоу каскада | Ближайший OB / POC | Нет новых открытий (OI ↓) |
| **F6** | **Funding Extremum (Filter Only)** | 1h | |r| > 0.05% за 30 мин до funding | — | — | **Не standalone стратегия.** Используется как фильтр (+confidence) для S1-S5. |

### 10.3 Composite & Multi-Timeframe

| ID | Стратегия | ТФ | Логика | Структурный SL | Структурный TP | Фильтры |
|----|-----------|-----|--------|----------------|----------------|---------|
| **C1** | **Confluence Scalp** | 15m | 3+ осциллятора (RSI, StochRSI, Williams) в экстремальной зоне + структурный уровень (OB/FVG) | За ближайшим structural high/low | Ближайший liquidity pool / BOS | Volume > 2x SMA |
| **C2** | **Structure + OI + Funding** | 4h | BOS на 4h + OI рост > 3% + funding нейтральный или противоположный новому тренду | За broken structure | Следующий HTF BOS | L/S Top Pos < 0.6 |
| **C3** | **HTF Bias + LTF Entry** | 1d + 1h/15m | 1d BOS bullish + 1h/15m return to OB/FVG + RSI < 30 | За границей LTF OB | 1d BOS target / opposing OB | 4h bias aligned with 1d |
| **C4** | **Basis Arbitrage Signal** | 1h, 4h | Spot vs Futures premium > 0.2% + futures at structural resistance (short) или discount + support (long) | За structural extreme | Fill of premium gap / opposing level | Использует Spot API |
| **C5** | **CVD Absorption + Delta** | 5m, 15m | Price makes new low, CVD makes higher low (absorption) + delta volume > 70% taker buy | За лоу свечи абсорбции | Ближайший OB / POC | OI flat или ↓ (нет новых шортов) |

---

## 11. Universal Filters (Applied to ALL Strategies)

| Фильтр | Условие | Действие |
|--------|---------|----------|
| **Time** | Первые 30 мин после funding (00:00, 08:00, 16:00 UTC) | Блок |
| **Liquidity** | Дневной объём < $100M ИЛИ OV < $50M | Блок |
| **Volume** | Текущий объём < 1.5x SMA20 | -10 confidence |
| **Funding** | |r| > 0.08% (если стратегия не F6) | -15 confidence |
| **OI** | OI 24h change < -10% (если не reversal setup) | Блок |
| **Premium** | |premium| > 0.15% (если не C4/F4) | -10 confidence |
| **L/S Extreme** | Top Position Ratio > 0.9 (для trend-стратегий) | -10 confidence |
| **Cooldown** | 4 часа на пару+направление+стратегию | Блок |
| **Confidence** | Final score < 65 | Блок |
| **Data Freshness** | Last kline older than 2×TF | Блок |

---

## 12. Structural Risk Management (No Fixed R:R)

**Принцип:** Все уровни риска строятся от структуры рынка, не от математического соотношения.

| Параметр | Методология |
|----------|-------------|
| **Stop Loss** | За структурным уровнем: recent BOS/CHoCH extreme, Order Block граница, FVG edge, Liquidity Sweep high/low. Минимум: ATR(14) × 1.5. |
| **Take Profit 1** | Ближайший структурный уровень: opposing OB, previous BOS/CHoCH, POC, FVG fill. |
| **Take Profit 2** | Следующий структурный уровень: HTF BOS target, major liquidity pool, opposing FVG. |
| **Leverage** | `min(10, 3 / ATR%)`. ATR% = ATR(14) / close × 100. |
| **Position Size** | Risk 1.5% от депозита ($10k default) = $150 / |entry − SL|. |
| **Liquidation Check** | Если `|entry − liq_price| / |entry − SL| < 2` → сигнал блокируется. |
| **Margin Type** | Isolated (рекомендация в сообщении). |

**Пример:**
- Entry: $67,450 (long at bullish OB)
- SL: $66,800 (за low of OB) — 0.96%
- TP1: $68,200 (previous BOS high) — 1.1%
- TP2: $69,000 (opposing FVG fill) — 2.3%
- Leverage: 5x (ATR% = 0.6%)

---

## 13. Confidence Engine (0–100)

```
Base (0–50):
  • Strategy quality: 15–30 (зависит от setup)
  • Confluence (+1 доп. индикатор подтверждения): +5 каждый, max +20

Market Context (0–30):
  • HTF Trend Alignment (1d/4h bias = direction): +10
  • Volume Confirmation (>1.5x SMA20): +5
  • OI Confirmation (рост при breakout, падение при reversal): +5
  • Funding/Premium не против сигнала: +5
  • Нет близкого funding (< 30 min): +5

Risk Profile (0–20):
  • Structural SL чёткий (не за горами): +5
  • Liquidation buffer > 2x SL distance: +5
  • TP1 — структурный уровень (не фиксированный %): +5
  • Пара в топ-20 по OV: +5

Порог публикации: >= 65
Anti-Spam: Cooldown 4ч, max 3 сигнала/мин.
```

---

## 14. Telegram Output Format (HTML)

```
⚡ BTCUSDT | 1H | LONG
Confidence: 78/100

Strategies: S2 (OB Mitigation) + F6 (Funding Filter)
HTF Bias: 4H Bullish (BOS confirmed)

📊 Entry: 67,450
🛑 SL: 66,800 (below OB low) | 0.96%
🎯 TP1: 68,200 (prev BOS high)
🎯 TP2: 69,000 (opposing FVG)

⚙️ Risk:
• Leverage: 5x (Isolated)
• Position: $150 risk (1.5% of $10k)
• Est. Liquidation: 54,100

📈 Context:
• Funding: 0.051% 🔴 (extreme, against shorts)
• OI 1h Δ: +2.1% (buildup)
• Premium: 0.008%
• L/S Top Pos: 0.38 (smart money short)
• CVD: Rising (absorption)

⏱ Valid: 4 hours
```

**Команды:**
- `/start` — uptime, memory, active pairs, last signal count.
- `/last` — последние 5 сигналов из signals.db.
- `/funding` — топ-5 пар по |funding|.
- `/status` — health всех 3 процессов.

---

## 15. Telemetry & Observability

### 15.1 Structured Logging (structlog)
Обязательные поля: `timestamp`, `pid`, `process_name`, `event_type`, `symbol`, `tf`, `latency_ms`, `run_id`.

### 15.2 Prometheus Metrics
- `bot_signals_total` (counter, labels: strategy, direction, status)
- `bot_signal_latency_seconds` (histogram)
- `bot_ws_messages_total` (counter, labels: stream, status)
- `bot_rest_requests_total` (counter, labels: endpoint, status)
- `bot_memory_bytes` (gauge, labels: process)
- `bot_cpu_seconds_total` (counter, labels: process)

### 15.3 JSONL Telemetry Files
- `cycles.jsonl` — цикл анализа Brain.
- `symbol_analysis.jsonl` — raw setups, candidates, rejections.
- `selected.jsonl` — прошедшие confidence.
- `delivery.jsonl` — Telegram dispatch outcomes.
- `health.jsonl` — heartbeat от всех процессов.

---

## 16. Single-Pass Implementation Plan (for Codex)

### Phase 0: Infrastructure
1. `config.py` — Pydantic settings, `.env` loading, strategy toggles.
2. `models.py` — msgspec structs: Candle, DerivativesSnapshot, SignalCandidate, ValidatedSignal, FeatureVector.
3. `orchestrator.py` — spawn(Collector, Brain, Notifier), monitor, SIGTERM handler.
4. `shared_memory.py` — SharedMarketBuffer (create, write, read, ring buffer, symbol registry).
5. `logging_config.py` — structlog setup, per-process log files (collector.log, brain.log, notifier.log).

### Phase 1: Data Layer (Collector)
6. `collector/rest_client.py` — Official SDK wrapper, two rate limiters (weight + request), tenacity retries.
7. `collector/ws_client.py` — UMFuturesWebsocketClient + raw websockets fallback, dual conn, k.x parsing, !markPrice@arr parsing (field `r`).
8. `collector/preloader.py` — REST klines preload for all 45 symbols (15m/1h/4h, limit=240), warm flag, persist to Parquet cache.
9. `collector/poller.py` — Sequential OI polling (45 symbols, every 5 min), L/S ratios (every 30 min), taker ratio (every 15 min).
10. `collector/db_writer.py` — aiosqlite writer for market_data.db (ohlcv, market_state, liquidations).
11. `collector/main.py` — asyncio event loop, start WS + REST + poller + db_writer.

### Phase 2: Analytics Layer (Brain)
12. `brain/shm_reader.py` — Read OHLCV from shared_memory into Polars DataFrames.
13. `brain/indicators.py` — Polars-native indicator functions (polars_ta + custom expressions).
14. `brain/indicator_workers.py` — ProcessPoolExecutor task: compute FeatureVector from mmap fd.
15. `brain/strategies/` — 15 strategy modules (S1-S5, F1-F6, C1-C5), each: `(FeatureVector, HTFBias) -> Optional[SignalCandidate]`.
16. `brain/confidence.py` — Scoring engine, cooldown manager, structural risk calculator.
17. `brain/mtf_confluence.py` — HTF bias resolver (reads 4h/1d from shared_memory).
18. `brain/main.py` — asyncio loop: read deriv_queue → dispatch compute jobs → strategies → confidence → signal_queue.

### Phase 3: Delivery Layer (Notifier)
19. `notifier/telegram_bot.py` — aiogram dispatcher, HTML formatter (Jinja2), command handlers.
20. `notifier/retry_queue.py` — Exponential backoff, max 5 attempts, dead-letter JSONL.
21. `notifier/db_writer.py` — aiosqlite writer for signals.db.
22. `notifier/main.py` — asyncio loop: read signal_queue → format → send → save receipt.

### Phase 4: Integration & Polish
23. `main.py` — Entry point: Orchestrator.start().
24. `tests/` — Unit tests for limiters, shared_memory, indicators, strategies (pytest + mocks).
25. `prometheus_exporter.py` — Optional HTTP endpoint for metrics scraping.

---

## 17. File Structure

```
bot/
├── main.py                      # Orchestrator entry point
├── config.py                    # Pydantic settings
├── models.py                    # msgspec structs
├── shared_memory.py             # SharedMarketBuffer
├── logging_config.py            # structlog setup
├── orchestrator.py              # Process spawn/monitor
├── collector/
│   ├── main.py
│   ├── rest_client.py
│   ├── ws_client.py
│   ├── preloader.py
│   ├── poller.py
│   └── db_writer.py
├── brain/
│   ├── main.py
│   ├── shm_reader.py
│   ├── indicators.py
│   ├── indicator_workers.py
│   ├── confidence.py
│   ├── mtf_confluence.py
│   └── strategies/
│       ├── __init__.py
│       ├── structure.py         # S1-S5
│       ├── microstructure.py    # F1-F6
│       └── composite.py         # C1-C5
├── notifier/
│   ├── main.py
│   ├── telegram_bot.py
│   ├── retry_queue.py
│   └── db_writer.py
├── telemetry/
│   └── prometheus_exporter.py
├── tests/
└── requirements.txt
```

---

## 18. Appendix: Critical API Facts

- **WS kline close:** `k.x == true` (not `is_closed`).
- **!markPrice@arr:** default 3s, field `r` = funding rate.
- **/fapi/v1/openInterest:** weight=1, single symbol, NO batch.
- **/futures/data/*:** IP limit 1000 requests / 5 minutes (not weight-based).
- **WS limits:** 1024 streams/conn, 10 msg/sec incoming, 24h connection lifetime.
- **WS paths (post-2026-04-23):** `/public` and `/market` (legacy `/stream` deprecated).
- **Official SDK:** `UMFuturesWebsocketClient` supports combined streams but is sync-threaded; wrap in asyncio thread executor.

---

*PRD v7.0 — финальная спецификация для Codex single-pass реализации. Все данные верифицированы по официальной документации Binance (april 2026).
