# Startup Report 2026-04-24 13:56:36 MSK

## Purpose
- Analyze the persisted evidence from the previous bot run before the new runtime starts.
- Preserve an AI-readable handoff for debugging strategy, filters, tracking, delivery, and transport.

## Data Sources
- Bot log: `C:\Users\undea\Documents\bot\data\bot\logs\bot_20260424_061154_100252.log`
- Telemetry dir: `C:\Users\undea\Documents\bot\data\bot\telemetry\runs\20260424_004735_93656\analysis`
- SQLite db: `C:\Users\undea\Documents\bot\data\bot\bot.db`

## Confirmed Facts
- Previous session started at 2026-04-24T06:11:54+00:00.
- Observed cycles in previous session window: 0.
- Total detector runs: 0.
- Total post-filter candidates: 0.
- Selected signals logged: 0.
- Structured strategy decisions logged: 0.
- Data-quality violations logged: 0.
- Closed outcomes available: 0.
- Open tracked signals at startup: 2.
- Cooldown entries persisted at startup: 7.
- Configured runtime policy: runtime_mode=signal_only source_policy=binance_only smart_exit_mode=heuristic_v1 gamma_semantics=proxy_only.
- Configured loss-streak pause: 3 consecutive losses pause new tracked signals for 5h.
- Persisted market_context predates the analyzed session and is not treated as live readiness.

## Inferred Focus Areas
- No single dominant failure mode was detected from persisted telemetry alone.

## Project State Notes
- Live Binance runtime is not verified end-to-end by startup artifacts alone.
- Startup snapshot keeps market_regime=`unknown` unless persisted runtime context explicitly confirms it.
- Startup snapshot keeps public_intelligence_ts=`null` when persisted intelligence predates the analyzed session.
- Smart Levels / Nearby is not a separate module yet; current runtime exposes 5m/1h hooks and an explicit funnel instead.
- External macro/news inputs are intentionally disabled under source_policy=`binance_only`.

## Recommended Fixes
- [low] `bot.startup_reporter`: No strong corrective action stands out from persisted telemetry alone; collect another full runtime session.

## Suspicious Modules
- `bot.tracking`: startup found persisted open tracked signals that still require lifecycle handling

## Previous Run Metrics
- Runtime policy: `{"config_loaded": true, "gamma_semantics": "proxy_only", "max_consecutive_stop_losses": 3, "runtime_mode": "signal_only", "smart_exit_mode": "heuristic_v1", "source_policy": "binance_only", "stop_loss_pause_hours": 5}`
- Cycle count: `0`
- Cycle age minutes: `None`
- Detector runs total: `0`
- Post-filter candidates total: `0`
- Selected total: `0`
- Rejected total: `0`
- Strategy decision rows: `0`
- Data-quality rows: `0`
- Telemetry mismatches: `0`
- Outcomes total: `0`
- Open tracked total: `2`
- Cooldown entries: `7`

## Strategy Analytics
- Cycle setup counts: `{}`
- Selected setup counts: `{}`
- Outcome setup counts: `{}`
- Outcome result counts: `{}`
- Outcome quality counts: `{}`
- Cycle setup percentages: `[]`
- Selected setup percentages: `[]`
- Outcome setup percentages: `[]`
- Outcome result percentages: `[]`

## Filter Analytics
- Top rejection reasons: `[]`
- Top rejection stages: `[]`
- Top decision reasons: `[]`
- Top decision stages: `[]`
- Rejection reason percentages: `[]`
- Rejection stage percentages: `[]`
- Decision reason percentages: `[]`
- Decision stage percentages: `[]`

## Runtime Analytics
- Latest cycle summary: `{"active_stream_count": null, "candidate_count": null, "reconnect_reason": null, "rejected_count": null, "rest_response_time_ms": null, "rest_weight_1m": null, "selected_count": null, "shortlist_size": null, "ts": null}`
- Delivery status counts: `{}`
- Decision status counts: `{}`
- Zero-hit setups: `[]`
- Data quality: `{"by_setup": {}, "invalid_fields": {}, "missing_fields": {}}`
- Telemetry mismatch counts: `{}`
- Tracking event counts: `{}`
- Open signal counts: `{"by_setup": {"breaker_block": 1, "liquidity_sweep": 1}, "by_status": {"active": 2}}`
- Delivery status percentages: `[]`
- Tracking event percentages: `[]`

## Current Market State
- Market state: `{"btc_bias": "neutral", "context_updated_at": null, "eth_bias": "neutral", "funding_sentiment": "neutral", "gamma_semantics": "proxy_only", "hard_barrier_long": false, "hard_barrier_short": false, "high_funding_count": 0, "low_funding_count": 0, "macro_risk_mode": "disabled_binance_only", "macro_status": "disabled_by_source_policy", "market_regime": "unknown", "public_intelligence_available": false, "runtime_mode": "signal_only", "smart_exit_mode": "heuristic_v1", "source_policy": "binance_only"}`

## Current Runtime Readiness
- Runtime readiness: `{"intelligence_policy": {"gamma_semantics": "proxy_only", "runtime_mode": "signal_only", "smart_exit_mode": "heuristic_v1", "source_policy": "binance_only"}, "macro_status": "disabled_by_source_policy", "public_intelligence_ts": null, "required_frame_readiness": {"15m_ready_symbols": 0, "1h_ready_symbols": 0, "4h_macro_symbols": 0, "5m_ready_symbols": 0}, "shortlist_preview": [], "shortlist_size": null, "shortlist_source": "unknown", "ws_health": {"active_stream_count": null, "buffer_message_count": null, "reconnect_reason": null, "ws_last_message_age_s": null}}`

## Shortlist Preview
- n/a

## AI Handoff JSON
```json
{
  "event": "startup_previous_run_analysis",
  "generated_at_utc": "2026-04-24T10:56:36.066375+00:00",
  "report_kind": "startup_snapshot",
  "confirmed_facts": [
    "Previous session started at 2026-04-24T06:11:54+00:00.",
    "Observed cycles in previous session window: 0.",
    "Total detector runs: 0.",
    "Total post-filter candidates: 0.",
    "Selected signals logged: 0.",
    "Structured strategy decisions logged: 0.",
    "Data-quality violations logged: 0.",
    "Closed outcomes available: 0.",
    "Open tracked signals at startup: 2.",
    "Cooldown entries persisted at startup: 7.",
    "Configured runtime policy: runtime_mode=signal_only source_policy=binance_only smart_exit_mode=heuristic_v1 gamma_semantics=proxy_only.",
    "Configured loss-streak pause: 3 consecutive losses pause new tracked signals for 5h.",
    "Persisted market_context predates the analyzed session and is not treated as live readiness."
  ],
  "inferred_focus_areas": [
    "No single dominant failure mode was detected from persisted telemetry alone."
  ],
  "metrics": {
    "cycle_count": 0,
    "cycle_age_minutes": null,
    "detector_runs_total": 0,
    "post_filter_candidates_total": 0,
    "selected_total": 0,
    "rejected_total": 0,
    "decision_rows_total": 0,
    "data_quality_rows_total": 0,
    "telemetry_mismatch_total": 0,
    "outcomes_total": 0,
    "open_tracked_total": 2,
    "cooldown_entries": 7
  },
  "top_rejection_reasons": [],
  "top_rejection_stages": [],
  "top_decision_reasons": [],
  "top_decision_stages": [],
  "percentages": {
    "cycle_setup_counts": [],
    "selected_setup_counts": [],
    "outcome_setup_counts": [],
    "outcome_result_counts": [],
    "outcome_quality_counts": [],
    "rejection_reasons": [],
    "rejection_stages": [],
    "decision_reasons": [],
    "decision_stages": [],
    "delivery_statuses": [],
    "tracking_events": []
  },
  "setup_counts": {
    "cycle_setup_counts": {},
    "selected_setup_counts": {},
    "outcome_setup_counts": {}
  },
  "delivery_status_counts": {},
  "decision_status_counts": {},
  "decision_reason_counts": {},
  "decision_stage_counts": {},
  "zero_hit_setups": [],
  "data_quality": {
    "by_setup": {},
    "missing_fields": {},
    "invalid_fields": {}
  },
  "telemetry_mismatch_counts": {},
  "tracking_event_counts": {},
  "outcome_result_counts": {},
  "outcome_quality_counts": {},
  "open_signal_counts": {
    "by_status": {
      "active": 2
    },
    "by_setup": {
      "breaker_block": 1,
      "liquidity_sweep": 1
    }
  },
  "latest_cycle_summary": {
    "ts": null,
    "shortlist_size": null,
    "candidate_count": null,
    "selected_count": null,
    "rejected_count": null,
    "active_stream_count": null,
    "reconnect_reason": null,
    "rest_weight_1m": null,
    "rest_response_time_ms": null
  },
  "current_market_state": {
    "market_regime": "unknown",
    "btc_bias": "neutral",
    "eth_bias": "neutral",
    "funding_sentiment": "neutral",
    "runtime_mode": "signal_only",
    "source_policy": "binance_only",
    "smart_exit_mode": "heuristic_v1",
    "gamma_semantics": "proxy_only",
    "macro_risk_mode": "disabled_binance_only",
    "macro_status": "disabled_by_source_policy",
    "context_updated_at": null,
    "high_funding_count": 0,
    "low_funding_count": 0,
    "public_intelligence_available": false,
    "hard_barrier_long": false,
    "hard_barrier_short": false
  },
  "current_runtime_readiness": {
    "shortlist_source": "unknown",
    "shortlist_size": null,
    "shortlist_preview": [],
    "required_frame_readiness": {
      "15m_ready_symbols": 0,
      "1h_ready_symbols": 0,
      "5m_ready_symbols": 0,
      "4h_macro_symbols": 0
    },
    "ws_health": {
      "active_stream_count": null,
      "buffer_message_count": null,
      "reconnect_reason": null,
      "ws_last_message_age_s": null
    },
    "public_intelligence_ts": null,
    "intelligence_policy": {
      "runtime_mode": "signal_only",
      "source_policy": "binance_only",
      "smart_exit_mode": "heuristic_v1",
      "gamma_semantics": "proxy_only"
    },
    "macro_status": "disabled_by_source_policy"
  },
  "runtime_policy": {
    "config_loaded": true,
    "runtime_mode": "signal_only",
    "source_policy": "binance_only",
    "smart_exit_mode": "heuristic_v1",
    "gamma_semantics": "proxy_only",
    "max_consecutive_stop_losses": 3,
    "stop_loss_pause_hours": 5
  },
  "runtime_errors": [],
  "shortlist_preview": [],
  "suspicious_modules": [
    {
      "module": "bot.tracking",
      "reason": "startup found persisted open tracked signals that still require lifecycle handling"
    }
  ],
  "recommended_fixes": [
    {
      "priority": "low",
      "module": "bot.startup_reporter",
      "action": "No strong corrective action stands out from persisted telemetry alone; collect another full runtime session."
    }
  ],
  "project_state_notes": [
    "Live Binance runtime is not verified end-to-end by startup artifacts alone.",
    "Startup snapshot keeps market_regime=`unknown` unless persisted runtime context explicitly confirms it.",
    "Startup snapshot keeps public_intelligence_ts=`null` when persisted intelligence predates the analyzed session.",
    "Smart Levels / Nearby is not a separate module yet; current runtime exposes 5m/1h hooks and an explicit funnel instead.",
    "External macro/news inputs are intentionally disabled under source_policy=`binance_only`."
  ]
}
```

## Bot Log Tail
```text
2026-04-24 09:11:54,990 | INFO    | bot.application.bot | __init__:91 | ws_manager initialized | pinned_symbols=4
2026-04-24 09:11:57,314 | INFO    | bot.application.bot | __init__:108 | MemoryRepository initialized | db=data\bot\bot.db
2026-04-24 09:11:57,557 | INFO    | bot.metrics | __init__:194 | metrics collector initialized on port 9090
2026-04-24 09:11:57,579 | INFO    | bot.metrics | start_server:203 | metrics server started on port 9090
2026-04-24 09:12:00,257 | INFO    | bot.dashboard | start_server:266 | dashboard server started on port 8080
2026-04-24 09:12:00,305 | INFO    | bot.tracking | _load_features_store:132 | features_store loaded | entries=2
2026-04-24 09:12:00,308 | INFO    | bot.core.engine.registry | register:51 | Registered strategy: structure_pullback (enabled=True)
2026-04-24 09:12:00,309 | INFO    | bot.application.bot | _register_strategies:207 | registered strategy structure_pullback (enabled=True)
2026-04-24 09:12:00,309 | INFO    | bot.core.engine.registry | register:51 | Registered strategy: structure_break_retest (enabled=True)
2026-04-24 09:12:00,310 | INFO    | bot.application.bot | _register_strategies:207 | registered strategy structure_break_retest (enabled=True)
2026-04-24 09:12:00,310 | INFO    | bot.core.engine.registry | register:51 | Registered strategy: wick_trap_reversal (enabled=True)
2026-04-24 09:12:00,310 | INFO    | bot.application.bot | _register_strategies:207 | registered strategy wick_trap_reversal (enabled=True)
2026-04-24 09:12:00,311 | INFO    | bot.core.engine.registry | register:51 | Registered strategy: squeeze_setup (enabled=True)
2026-04-24 09:12:00,313 | INFO    | bot.application.bot | _register_strategies:207 | registered strategy squeeze_setup (enabled=True)
2026-04-24 09:12:00,314 | INFO    | bot.core.engine.registry | register:51 | Registered strategy: ema_bounce (enabled=True)
2026-04-24 09:12:00,314 | INFO    | bot.application.bot | _register_strategies:207 | registered strategy ema_bounce (enabled=True)
2026-04-24 09:12:00,315 | INFO    | bot.core.engine.registry | register:51 | Registered strategy: fvg_setup (enabled=True)
2026-04-24 09:12:00,318 | INFO    | bot.application.bot | _register_strategies:207 | registered strategy fvg_setup (enabled=True)
2026-04-24 09:12:00,319 | INFO    | bot.core.engine.registry | register:51 | Registered strategy: order_block (enabled=True)
2026-04-24 09:12:00,319 | INFO    | bot.application.bot | _register_strategies:207 | registered strategy order_block (enabled=True)
2026-04-24 09:12:00,319 | INFO    | bot.core.engine.registry | register:51 | Registered strategy: liquidity_sweep (enabled=True)
2026-04-24 09:12:00,320 | INFO    | bot.application.bot | _register_strategies:207 | registered strategy liquidity_sweep (enabled=True)
2026-04-24 09:12:00,322 | INFO    | bot.core.engine.registry | register:51 | Registered strategy: bos_choch (enabled=True)
2026-04-24 09:12:00,323 | INFO    | bot.application.bot | _register_strategies:207 | registered strategy bos_choch (enabled=True)
2026-04-24 09:12:00,324 | INFO    | bot.core.engine.registry | register:51 | Registered strategy: hidden_divergence (enabled=True)
2026-04-24 09:12:00,337 | INFO    | bot.application.bot | _register_strategies:207 | registered strategy hidden_divergence (enabled=True)
2026-04-24 09:12:00,338 | INFO    | bot.core.engine.registry | register:51 | Registered strategy: funding_reversal (enabled=True)
2026-04-24 09:12:00,338 | INFO    | bot.application.bot | _register_strategies:207 | registered strategy funding_reversal (enabled=True)
2026-04-24 09:12:00,339 | INFO    | bot.core.engine.registry | register:51 | Registered strategy: cvd_divergence (enabled=True)
2026-04-24 09:12:00,339 | INFO    | bot.application.bot | _register_strategies:207 | registered strategy cvd_divergence (enabled=True)
2026-04-24 09:12:00,340 | INFO    | bot.core.engine.registry | register:51 | Registered strategy: session_killzone (enabled=True)
2026-04-24 09:12:00,341 | INFO    | bot.application.bot | _register_strategies:207 | registered strategy session_killzone (enabled=True)
2026-04-24 09:12:00,341 | INFO    | bot.core.engine.registry | register:51 | Registered strategy: breaker_block (enabled=True)
2026-04-24 09:12:00,343 | INFO    | bot.application.bot | _register_strategies:207 | registered strategy breaker_block (enabled=True)
2026-04-24 09:12:00,344 | INFO    | bot.core.engine.registry | register:51 | Registered strategy: turtle_soup (enabled=True)
2026-04-24 09:12:00,344 | INFO    | bot.application.bot | _register_strategies:207 | registered strategy turtle_soup (enabled=True)
2026-04-24 09:12:00,344 | INFO    | bot.application.bot | _register_strategies:209 | strategies registered | total=15 enabled=15
2026-04-24 09:12:00,348 | INFO    | bot.application.bot | __init__:153 | SignalEngine initialized with 15 strategies
2026-04-24 09:12:00,349 | INFO    | bot.application.bot | __init__:179 | EventBus subscriptions registered | handlers=3 (kline_close, reconnect, book_ticker)
2026-04-24 09:12:00,383 | WARNING | stderr | write:271 | STDERR: another bot process is already running with pid 93656
```
