# Public Intelligence 2026-04-23T17:14:24.704057+00:00

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
- Barrier snapshot: `{"long_barrier_triggered": false, "short_barrier_triggered": false, "strongest_move_pct": -0.7549, "strongest_symbol": "BTCUSDT", "threshold_pct": 1.5, "window_minutes": 15}`

## Macro / External
- Macro snapshot: `{"available": false, "by_symbol": {}, "confirmed_facts": ["external_macro_and_news_feeds_disabled_under_source_policy_binance_only"], "enabled": false, "reason": "source_policy_binance_only", "risk_mode": "disabled_binance_only", "risk_off_votes": 0, "risk_on_votes": 0, "status": "disabled_by_source_policy"}`

## Options
- Options snapshot: `{"assumptions": ["BTC_gamma_proxy_is_sign_based_and_does_not_observe_real_dealer_inventory", "ETH_gamma_proxy_is_sign_based_and_does_not_observe_real_dealer_inventory"], "by_underlying": {"BTC": {"available": true, "call_oi_contracts": 3762.98, "call_oi_usd": 292751339.38, "expiries_used": ["260424", "260425"], "gamma_balance_proxy": 0.08308821, "gamma_balance_ratio": 0.200193, "gamma_positioning_proxy": "positive_gamma_proxy", "gamma_semantics": "proxy_only", "mark_rows": 1702, "oi_rows": 140, "put_call_oi_ratio": 1.3319, "put_oi_contracts": 5011.91, "put_oi_usd": 389915276.04, "weighted_mark_iv": 0.826221}, "ETH": {"available": true, "call_oi_contracts": 45803.06, "call_oi_usd": 106150284.89, "expiries_used": ["260424", "260425"], "gamma_balance_proxy": -5.46138922, "gamma_balance_ratio": -0.060831, "gamma_positioning_proxy": "flat_proxy", "gamma_semantics": "proxy_only", "mark_rows": 1702, "oi_rows": 108, "put_call_oi_ratio": 1.0653, "put_oi_contracts": 48794.86, "put_oi_usd": 113083904.94, "weighted_mark_iv": 1.211391}}, "confirmed_facts": ["BTC_public_options_mark_and_oi_available", "ETH_public_options_mark_and_oi_available"], "inferences": ["BTC_gamma_positioning_proxy_is_derived_from_public_gamma_and_oi", "ETH_gamma_positioning_proxy_is_derived_from_public_gamma_and_oi"]}`

## Derivatives
- Derivatives snapshot: `{"by_symbol": {"BTCUSDT": {"basis_pct": -0.0631401418940343, "flow_bias": "sellers_in_control", "funding_history_points": 4, "funding_rate": -8.978e-05, "funding_trend": "flat", "global_long_short_ratio": 0.6504, "oi_change_pct": 0.005964745314152253, "oi_current": 98741.875, "taker_ratio": 0.7914, "top_trader_long_short_ratio": 0.6773}, "ETHUSDT": {"basis_pct": -0.046685866800669294, "flow_bias": "position_unwind", "funding_history_points": 4, "funding_rate": -5.089e-05, "funding_trend": "flat", "global_long_short_ratio": 1.8571, "oi_change_pct": -0.001218465006974201, "oi_current": 2050913.399, "taker_ratio": 1.0343, "top_trader_long_short_ratio": 1.4582}, "SOLUSDT": {"basis_pct": -0.03761455658969606, "flow_bias": "sellers_in_control", "funding_history_points": 4, "funding_rate": -1.93e-06, "funding_trend": "flat", "global_long_short_ratio": 2.2733, "oi_change_pct": 0.0007201415935669253, "oi_current": 9462205.39, "taker_ratio": 0.7362, "top_trader_long_short_ratio": 2.3523}, "XRPUSDT": {"basis_pct": -0.04952888465234553, "flow_bias": "position_unwind", "funding_history_points": 4, "funding_rate": -3.212e-05, "funding_trend": "flat", "global_long_short_ratio": 2.3921, "oi_change_pct": -0.005107569070017637, "oi_current": 272824402.1, "taker_ratio": 0.9178, "top_trader_long_short_ratio": 2.5361}}, "confirmed_facts": ["BTCUSDT_public_futures_context_available", "ETHUSDT_public_futures_context_available", "SOLUSDT_public_futures_context_available", "XRPUSDT_public_futures_context_available"]}`

## Harmonic (Debug Only)
- Harmonic snapshot: `{"by_symbol": {"BTCUSDT": {"available": true, "deviation_pct": 11.2035, "pattern": "abcd_like", "ratio_ab_cd": 0.888, "ratio_bc_ab": 0.4423}, "ETHUSDT": {"available": true, "deviation_pct": 0.9946, "pattern": "abcd_like", "ratio_ab_cd": 1.0099, "ratio_bc_ab": 0.5202}}, "enabled": true, "mode": "debug_only_confluence", "standalone_strategy": false, "weight": "low"}`

## JSON
```json
{
  "ts": "2026-04-23T17:14:24.704057+00:00",
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
        "funding_rate": -8.978e-05,
        "oi_current": 98741.875,
        "oi_change_pct": 0.005964745314152253,
        "top_trader_long_short_ratio": 0.6773,
        "global_long_short_ratio": 0.6504,
        "taker_ratio": 0.7914,
        "basis_pct": -0.0631401418940343,
        "funding_trend": "flat",
        "flow_bias": "sellers_in_control",
        "funding_history_points": 4
      },
      "ETHUSDT": {
        "funding_rate": -5.089e-05,
        "oi_current": 2050913.399,
        "oi_change_pct": -0.001218465006974201,
        "top_trader_long_short_ratio": 1.4582,
        "global_long_short_ratio": 1.8571,
        "taker_ratio": 1.0343,
        "basis_pct": -0.046685866800669294,
        "funding_trend": "flat",
        "flow_bias": "position_unwind",
        "funding_history_points": 4
      },
      "SOLUSDT": {
        "funding_rate": -1.93e-06,
        "oi_current": 9462205.39,
        "oi_change_pct": 0.0007201415935669253,
        "top_trader_long_short_ratio": 2.3523,
        "global_long_short_ratio": 2.2733,
        "taker_ratio": 0.7362,
        "basis_pct": -0.03761455658969606,
        "funding_trend": "flat",
        "flow_bias": "sellers_in_control",
        "funding_history_points": 4
      },
      "XRPUSDT": {
        "funding_rate": -3.212e-05,
        "oi_current": 272824402.1,
        "oi_change_pct": -0.005107569070017637,
        "top_trader_long_short_ratio": 2.5361,
        "global_long_short_ratio": 2.3921,
        "taker_ratio": 0.9178,
        "basis_pct": -0.04952888465234553,
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
        "call_oi_contracts": 3762.98,
        "put_oi_contracts": 5011.91,
        "call_oi_usd": 292751339.38,
        "put_oi_usd": 389915276.04,
        "put_call_oi_ratio": 1.3319,
        "weighted_mark_iv": 0.826221,
        "gamma_balance_proxy": 0.08308821,
        "gamma_balance_ratio": 0.200193,
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
        "call_oi_contracts": 45803.06,
        "put_oi_contracts": 48794.86,
        "call_oi_usd": 106150284.89,
        "put_oi_usd": 113083904.94,
        "put_call_oi_ratio": 1.0653,
        "weighted_mark_iv": 1.211391,
        "gamma_balance_proxy": -5.46138922,
        "gamma_balance_ratio": -0.060831,
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
    "strongest_symbol": "BTCUSDT",
    "strongest_move_pct": -0.7549
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
