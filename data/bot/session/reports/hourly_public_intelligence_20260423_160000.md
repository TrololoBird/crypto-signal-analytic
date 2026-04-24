# Public Intelligence 2026-04-23T16:43:48.881052+00:00

## Scope
- Binance public futures/options endpoints only for this phase.
- Runtime mode: `signal_only`.
- Source policy: `binance_only`.
- Smart exit mode: `heuristic_v1` analytical exit only; no order placement.
- Gamma semantics: `proxy_only`; proxy only and not observed dealer inventory.
- Output optimized for bot telemetry and AI-agent debugging.

## Policy
- Policy snapshot: `{"gamma_semantics": "proxy_only", "runtime_mode": "signal_only", "smart_exit_mode": "heuristic_v1", "source_policy": "binance_only"}`

## Confirmed Facts
- BTCUSDT_public_futures_context_available
- ETHUSDT_public_futures_context_available
- SOLUSDT_public_futures_context_available
- XRPUSDT_public_futures_context_available
- BTC_public_options_mark_and_oi_available
- ETH_public_options_mark_and_oi_available
- external_macro_and_news_feeds_disabled_under_source_policy_binance_only

## Inferences
- BTC_gamma_positioning_proxy_is_derived_from_public_gamma_and_oi
- ETH_gamma_positioning_proxy_is_derived_from_public_gamma_and_oi

## Assumptions
- BTC_gamma_proxy_is_sign_based_and_does_not_observe_real_dealer_inventory
- ETH_gamma_proxy_is_sign_based_and_does_not_observe_real_dealer_inventory
- fund_flows_are_public_derivatives_flow_proxies_not_custody_or_etf_settlement_flows

## Uncertainty
- startup_snapshot_cannot_prove_live_binance_runtime_end_to_end_without_real_runtime_evidence

## Barrier
- Barrier snapshot: `{"long_barrier_triggered": false, "short_barrier_triggered": false, "strongest_move_pct": -0.1308, "strongest_symbol": "ETHUSDT", "threshold_pct": 1.5, "window_minutes": 15}`

## Macro / External
- Macro snapshot: `{"available": false, "by_symbol": {}, "confirmed_facts": ["external_macro_and_news_feeds_disabled_under_source_policy_binance_only"], "enabled": false, "reason": "source_policy_binance_only", "risk_mode": "disabled_binance_only", "risk_off_votes": 0, "risk_on_votes": 0, "status": "disabled_by_source_policy"}`

## Options
- Options snapshot: `{"assumptions": ["BTC_gamma_proxy_is_sign_based_and_does_not_observe_real_dealer_inventory", "ETH_gamma_proxy_is_sign_based_and_does_not_observe_real_dealer_inventory"], "by_underlying": {"BTC": {"available": true, "call_oi_contracts": 3761.86, "call_oi_usd": 294610075.81, "expiries_used": ["260424", "260425"], "gamma_balance_proxy": 0.14271122, "gamma_balance_ratio": 0.33183, "gamma_positioning_proxy": "positive_gamma_proxy", "gamma_semantics": "proxy_only", "mark_rows": 1702, "oi_rows": 140, "put_call_oi_ratio": 1.3243, "put_oi_contracts": 4981.85, "put_oi_usd": 390153595.01, "weighted_mark_iv": 0.774565}, "ETH": {"available": true, "call_oi_contracts": 44778.58, "call_oi_usd": 104337600.79, "expiries_used": ["260424", "260425"], "gamma_balance_proxy": 1.91038348, "gamma_balance_ratio": 0.02164, "gamma_positioning_proxy": "flat_proxy", "gamma_semantics": "proxy_only", "mark_rows": 1702, "oi_rows": 108, "put_call_oi_ratio": 1.0818, "put_oi_contracts": 48440.25, "put_oi_usd": 112869578.86, "weighted_mark_iv": 1.227115}}, "confirmed_facts": ["BTC_public_options_mark_and_oi_available", "ETH_public_options_mark_and_oi_available"], "inferences": ["BTC_gamma_positioning_proxy_is_derived_from_public_gamma_and_oi", "ETH_gamma_positioning_proxy_is_derived_from_public_gamma_and_oi"]}`

## Derivatives
- Derivatives snapshot: `{"by_symbol": {"BTCUSDT": {"basis_pct": -0.05755192678575503, "flow_bias": "neutral", "funding_history_points": 4, "funding_rate": -7.866e-05, "funding_trend": "flat", "global_long_short_ratio": 0.6567, "oi_change_pct": 0.001435814287505277, "oi_current": 98750.02, "taker_ratio": 1.0258, "top_trader_long_short_ratio": 0.6827}, "ETHUSDT": {"basis_pct": -0.062275892455095266, "flow_bias": "position_unwind", "funding_history_points": 10, "funding_rate": -5.18e-05, "funding_trend": "flat", "global_long_short_ratio": 1.8703, "oi_change_pct": -0.007989504876573905, "oi_current": 2053942.346, "taker_ratio": 0.9262, "top_trader_long_short_ratio": 1.4673}, "SOLUSDT": {"basis_pct": -0.05583121106666316, "flow_bias": "buyers_in_control", "funding_history_points": 10, "funding_rate": 1.256e-05, "funding_trend": "flat", "global_long_short_ratio": 2.2584, "oi_change_pct": 0.00985826729930972, "oi_current": 9455656.45, "taker_ratio": 1.3455, "top_trader_long_short_ratio": 2.3333}, "XRPUSDT": {"basis_pct": -0.05860822787018106, "flow_bias": "position_unwind", "funding_history_points": 4, "funding_rate": -2.433e-05, "funding_trend": "flat", "global_long_short_ratio": 2.3715, "oi_change_pct": -0.006141561122239403, "oi_current": 274275301.6, "taker_ratio": 0.786, "top_trader_long_short_ratio": 2.4977}}, "confirmed_facts": ["BTCUSDT_public_futures_context_available", "ETHUSDT_public_futures_context_available", "SOLUSDT_public_futures_context_available", "XRPUSDT_public_futures_context_available"]}`

## Harmonic (Debug Only)
- Harmonic snapshot: `{"by_symbol": {"BTCUSDT": {"available": true, "deviation_pct": 11.2035, "pattern": "abcd_like", "ratio_ab_cd": 0.888, "ratio_bc_ab": 0.4423}, "ETHUSDT": {"available": true, "deviation_pct": 0.9946, "pattern": "abcd_like", "ratio_ab_cd": 1.0099, "ratio_bc_ab": 0.5202}}, "enabled": true, "mode": "debug_only_confluence", "standalone_strategy": false, "weight": "low"}`

## JSON
```json
{
  "ts": "2026-04-23T16:43:48.881052+00:00",
  "runtime_mode": "signal_only",
  "source_policy": "binance_only",
  "smart_exit_mode": "heuristic_v1",
  "gamma_semantics": "proxy_only",
  "policy": {
    "runtime_mode": "signal_only",
    "source_policy": "binance_only",
    "smart_exit_mode": "heuristic_v1",
    "gamma_semantics": "proxy_only"
  },
  "confirmed_facts": [
    "BTCUSDT_public_futures_context_available",
    "ETHUSDT_public_futures_context_available",
    "SOLUSDT_public_futures_context_available",
    "XRPUSDT_public_futures_context_available",
    "BTC_public_options_mark_and_oi_available",
    "ETH_public_options_mark_and_oi_available",
    "external_macro_and_news_feeds_disabled_under_source_policy_binance_only"
  ],
  "inferences": [
    "BTC_gamma_positioning_proxy_is_derived_from_public_gamma_and_oi",
    "ETH_gamma_positioning_proxy_is_derived_from_public_gamma_and_oi"
  ],
  "assumptions": [
    "BTC_gamma_proxy_is_sign_based_and_does_not_observe_real_dealer_inventory",
    "ETH_gamma_proxy_is_sign_based_and_does_not_observe_real_dealer_inventory",
    "fund_flows_are_public_derivatives_flow_proxies_not_custody_or_etf_settlement_flows"
  ],
  "uncertainty": [
    "startup_snapshot_cannot_prove_live_binance_runtime_end_to_end_without_real_runtime_evidence"
  ],
  "sources": {
    "binance_futures_public": true,
    "binance_options_public": true,
    "yahoo_macro_public": false,
    "external_macro_public": false,
    "external_news_public": false
  },
  "benchmarks": [
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "XRPUSDT"
  ],
  "derivatives": {
    "by_symbol": {
      "BTCUSDT": {
        "funding_rate": -7.866e-05,
        "oi_current": 98750.02,
        "oi_change_pct": 0.001435814287505277,
        "top_trader_long_short_ratio": 0.6827,
        "global_long_short_ratio": 0.6567,
        "taker_ratio": 1.0258,
        "basis_pct": -0.05755192678575503,
        "funding_trend": "flat",
        "flow_bias": "neutral",
        "funding_history_points": 4
      },
      "ETHUSDT": {
        "funding_rate": -5.18e-05,
        "oi_current": 2053942.346,
        "oi_change_pct": -0.007989504876573905,
        "top_trader_long_short_ratio": 1.4673,
        "global_long_short_ratio": 1.8703,
        "taker_ratio": 0.9262,
        "basis_pct": -0.062275892455095266,
        "funding_trend": "flat",
        "flow_bias": "position_unwind",
        "funding_history_points": 10
      },
      "SOLUSDT": {
        "funding_rate": 1.256e-05,
        "oi_current": 9455656.45,
        "oi_change_pct": 0.00985826729930972,
        "top_trader_long_short_ratio": 2.3333,
        "global_long_short_ratio": 2.2584,
        "taker_ratio": 1.3455,
        "basis_pct": -0.05583121106666316,
        "funding_trend": "flat",
        "flow_bias": "buyers_in_control",
        "funding_history_points": 10
      },
      "XRPUSDT": {
        "funding_rate": -2.433e-05,
        "oi_current": 274275301.6,
        "oi_change_pct": -0.006141561122239403,
        "top_trader_long_short_ratio": 2.4977,
        "global_long_short_ratio": 2.3715,
        "taker_ratio": 0.786,
        "basis_pct": -0.05860822787018106,
        "funding_trend": "flat",
        "flow_bias": "position_unwind",
        "funding_history_points": 4
      }
    },
    "confirmed_facts": [
      "BTCUSDT_public_futures_context_available",
      "ETHUSDT_public_futures_context_available",
      "SOLUSDT_public_futures_context_available",
      "XRPUSDT_public_futures_context_available"
    ]
  },
  "options": {
    "by_underlying": {
      "BTC": {
        "available": true,
        "expiries_used": [
          "260424",
          "260425"
        ],
        "call_oi_contracts": 3761.86,
        "put_oi_contracts": 4981.85,
        "call_oi_usd": 294610075.81,
        "put_oi_usd": 390153595.01,
        "put_call_oi_ratio": 1.3243,
        "weighted_mark_iv": 0.774565,
        "gamma_balance_proxy": 0.14271122,
        "gamma_balance_ratio": 0.33183,
        "gamma_positioning_proxy": "positive_gamma_proxy",
        "gamma_semantics": "proxy_only",
        "mark_rows": 1702,
        "oi_rows": 140
      },
      "ETH": {
        "available": true,
        "expiries_used": [
          "260424",
          "260425"
        ],
        "call_oi_contracts": 44778.58,
        "put_oi_contracts": 48440.25,
        "call_oi_usd": 104337600.79,
        "put_oi_usd": 112869578.86,
        "put_call_oi_ratio": 1.0818,
        "weighted_mark_iv": 1.227115,
        "gamma_balance_proxy": 1.91038348,
        "gamma_balance_ratio": 0.02164,
        "gamma_positioning_proxy": "flat_proxy",
        "gamma_semantics": "proxy_only",
        "mark_rows": 1702,
        "oi_rows": 108
      }
    },
    "confirmed_facts": [
      "BTC_public_options_mark_and_oi_available",
      "ETH_public_options_mark_and_oi_available"
    ],
    "inferences": [
      "BTC_gamma_positioning_proxy_is_derived_from_public_gamma_and_oi",
      "ETH_gamma_positioning_proxy_is_derived_from_public_gamma_and_oi"
    ],
    "assumptions": [
      "BTC_gamma_proxy_is_sign_based_and_does_not_observe_real_dealer_inventory",
      "ETH_gamma_proxy_is_sign_based_and_does_not_observe_real_dealer_inventory"
    ]
  },
  "macro": {
    "enabled": false,
    "available": false,
    "status": "disabled_by_source_policy",
    "reason": "source_policy_binance_only",
    "risk_mode": "disabled_binance_only",
    "risk_off_votes": 0,
    "risk_on_votes": 0,
    "by_symbol": {},
    "confirmed_facts": [
      "external_macro_and_news_feeds_disabled_under_source_policy_binance_only"
    ]
  },
  "barrier": {
    "window_minutes": 15,
    "threshold_pct": 1.5,
    "long_barrier_triggered": false,
    "short_barrier_triggered": false,
    "strongest_symbol": "ETHUSDT",
    "strongest_move_pct": -0.1308
  },
  "harmonic": {
    "enabled": true,
    "mode": "debug_only_confluence",
    "standalone_strategy": false,
    "weight": "low",
    "by_symbol": {
      "BTCUSDT": {
        "available": true,
        "pattern": "abcd_like",
        "ratio_ab_cd": 0.888,
        "ratio_bc_ab": 0.4423,
        "deviation_pct": 11.2035
      },
      "ETHUSDT": {
        "available": true,
        "pattern": "abcd_like",
        "ratio_ab_cd": 1.0099,
        "ratio_bc_ab": 0.5202,
        "deviation_pct": 0.9946
      }
    }
  }
}
```
