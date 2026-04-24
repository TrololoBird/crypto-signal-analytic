› A previous agent produced the plan below to accomplish the user's task. Implement the plan in a fresh context. Treat the plan as the source of user intent, re-read
  files as needed, and carry the work through implementation and verification.

  # План глубокой переработки signal funnel на публичных Binance данных

  ## Summary
  - `confirmed fact`: проблема системная, а не разовая. По двум последним полноценным сессиям бот дал `6` и `2` доставленных сигнала, при этом в обеих сессиях главным
  downstream-veto был `5m_opposes_long`, а не сетевые ошибки или отсутствие данных.
  - `confirmed fact`: последний запуск содержал явный дефект целевых уровней: `HYPEUSDT breaker_block long` был сохранён с `TP2 < TP1`; `FETUSDT` был сохранён с `TP1
  == TP2`, и трекер обработал это как искусственную двухступенчатую сделку.
  - `confirmed fact`: в проекте уже есть `polars_ta==0.5.17`, но `features.py` его не использует; часть индикаторных колонок сейчас являются нейтральными заглушками.
  - `confirmed fact`: текущий futures data layer уже забирает `openInterest`, `openInterestHist`, `funding`, `taker buy/sell`, `basis`, `top/global long-short`, но
  основной signal path использует это неполно и почти не использует в ранних стадиях отбора.
  - `inference`: бот слишком поздно применяет directional-context. Raw-detector’ы часто строят long-кандидаты, которые потом массово режутся 5m-фильтром. Значит,
  проблема не только в порогах, а в структуре конвейера.
  - `uncertainty`: число `50-80` сигналов в день на `15m` нельзя обосновать официальной документацией Binance или TA-библиотек. Поэтому acceptance должен строиться на
  replay и funnel-метриках, а не на жёсткой внешней “норме”.

  ## Web-Grounded Inputs
  - Futures raw candles: `/fapi/v1/klines`
    https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Kline-Candlestick-Data
  - Mark/index/premium candles: `/fapi/v1/markPriceKlines`, `/fapi/v1/indexPriceKlines`, `/fapi/v1/premiumIndexKlines`
    https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Mark-Price-Kline-Candlestick-Data
    https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Index-Price-Kline-Candlestick-Data
    https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Premium-Index-Kline-Data
  - Futures flow/context: `/fapi/v1/openInterest`, `/futures/data/openInterestHist`, `/futures/data/takerlongshortRatio`, `/futures/data/globalLongShortAccountRatio`,
  `/futures/data/topLongShortPositionRatio`, `/futures/data/basis`, `/fapi/v1/premiumIndex`
    https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Open-Interest
    https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Open-Interest-Statistics
    https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Taker-BuySell-Volume
    https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Long-Short-Ratio
    https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Top-Trader-Long-Short-Ratio
    https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Basis
    https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Mark-Price
  - Futures real-time streams: `!markPrice@arr@1s`, `<symbol>@aggTrade`, `!forceOrder@arr`, `<symbol>@depth10@500ms`
    https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Mark-Price-Stream-for-All-market
    https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Aggregate-Trade-Streams
    https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/All-Market-Liquidation-Order-Streams
    https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Partial-Book-Depth-Streams
  - Spot public market data for lead/lag and basis confirmation: `/api/v3/depth`, `/api/v3/aggTrades`, `/api/v3/klines`
    https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints
  - Indicator stack: `polars-ta` already available and TA-Lib exposes a large indicator/pattern set, but hot-path should use a curated subset, not all 200+ функций
    https://pypi.org/project/polars-ta/
    https://ta-lib.org/functions/

  ## Key Changes
  - Перестроить data layer вокруг двух скоростей:
    - `fast context` для `15m` сигналов: `5m` ряды из `openInterestHist`, `takerlongshortRatio`, `globalLongShortAccountRatio`, `topLongShortPositionRatio`, `basis`,
  `mark/index/premium` klines.
    - `slow context` для макро-смещения: текущие `1h`/`4h` свечи и funding history.
  - Добавить futures microstructure sidecar только для `focus set`, а не для всего shortlist:
    - `focus set = max 8` символов: tracked symbols + raw-hit symbols + top pre-ranked symbols.
    - для них подписывать `<symbol>@depth10@500ms` и `<symbol>@aggTrade`.
    - считать `depth_imbalance`, `microprice_bias`, `agg_trade_delta_30s`, `aggression_shift`.
    - не подписывать depth на все `45` символов, чтобы не убить WS budget.
  - Добавить spot-reference sidecar:
    - всегда держать `BTCUSDT`, `ETHUSDT` spot.
    - для `focus set` подключать spot только если существует парный spot symbol.
    - считать `spot_return_1m/5m`, `spot_depth_imbalance`, `spot_vs_futures_return_gap`, `spot_futures_spread_bps`.
  - Расширить `PreparedSymbol` новыми полями:
    - `mark_index_spread_bps`
    - `premium_zscore_5m`
    - `premium_slope_5m`
    - `oi_slope_5m`
    - `top_trader_position_ratio`
    - `global_ls_ratio`
    - `top_vs_global_ls_gap`
    - `depth_imbalance`
    - `microprice_bias`
    - `spot_lead_return_1m`
    - `spot_futures_spread_bps`
  - Расширить `StrategyMetadata`, а не плодить ad-hoc if/else:
    - `family`: `continuation`, `reversal`, `breakout`, `mean_reversion`
    - `confirmation_profile`: `trend_follow`, `countertrend_exhaustion`, `breakout_acceptance`
    - `required_context`: набор нужных слоёв (`futures_flow`, `spot_ref`, `depth_focus`)
  - Заменить текущий single-bar `_check_5m_confirmation` на family-aware confirmation:
    - continuation setup проходит, если выполнены минимум `2 из 4`: `5m trend`, `taker/agg delta`, `premium/mark-index slope`, `depth imbalance`.
    - reversal setup не режется только потому, что `1h` и `5m` против него; он проходит при наличии exhaustion signals: экстремальный premium, liquidation imbalance,
  crowd stretch (`top_vs_global_ls_gap`), резкий разворот aggressor flow.
    - hard reject оставлять только когда одновременно против сигнала идут `1h regime`, `5m flow`, и нет exhaustion pattern.
  - Перенести directional sanity раньше по funnel:
    - detector не должен генерировать long raw-hit, если continuation-profile уже видит подтверждённый short-flow на `5m`.
    - reversal detectors должны использовать контр-трендовый профиль явно, а не ломаться на общих veto.
  - Исправить target engine централизованно:
    - единый инвариант `long: stop < entry < TP1 <= TP2`, `short: stop > entry > TP1 >= TP2`.
    - автоматическая нормализация перепутанных TP.
    - single-target семантика без фальшивого `tp1_hit + tp2_hit`.
    - delivery/tracking/outcomes обязаны трактовать `TP1 == TP2` как одну цель.
  - Переписать feature engine на реальные Expr-реализации:
    - заменить ручные упрощённые/заглушечные расчёты для реально используемых индикаторов на `polars_ta`.
    - не тянуть весь TA-Lib в hot path.
    - оставить curated набор: точные `ADX`, `ATR`, `RSI`, `Supertrend`, `Stoch`, `MFI`, `CMF`, `CCI`, `OBV`, `ROC`, `realized vol`, `VWAP deviation`.
  - Заменить `market_regime.py`:
    - не использовать 24h ticker changes как основной market regime.
    - строить regime из `BTC/ETH 15m/1h/4h` структуры + `OI/funding/premium/basis` + spot/futures divergence.
    - этот слой должен снижать/повышать уверенность, а не blindly veto raw setups.

  ## Replay / Validation
  - Добавить offline replay harness на публичных Binance данных и на уже собранных telemetry runs.
  - Прогонять минимум последние две полноценные сессии как baseline:
    - `20260421_234355_31268`
    - `20260422_230643_23708`
    - `0` ложных dual-TP lifecycle cases для single-target сигналов.
    - доля post-raw reject с причиной `5m_opposes_long` должна заметно упасть, потому что long-bias должен отсеиваться раньше и умнее.
    - `confirmation_reject`
    - `filters_reject`
    - `selected`
    - `delivered`
    - `tracking_outcome`
  - Добавить integrity telemetry:
    - `target_integrity_status`
    - `single_target_mode`
    - `context_snapshot_age`
    - `data_source_mix = futures_only | futures+spot`

  - `assumption`: spot integration включаем сразу, но только для benchmark symbols и `focus set`, а не для полного рынка.
  - `assumption`: новый C-native dependency не обязателен; сначала используем уже установленный `polars_ta`. `TA-Lib` можно оставить как optional verification backend,
  а не как runtime hard dependency.
  - `uncertainty`: фиксированную цель `50-80` сигналов/день не использовать как единственный KPI. Для качества важнее replayed opportunity capture, conversion funnel и
  outcome quality.
  - `default`: depth stream брать `depth10@500ms`, а не `@100ms`, чтобы не перегрузить WS.