# Public Intelligence 2026-04-23T18:46:25.657776+00:00

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
- Barrier snapshot: `{"long_barrier_triggered": false, "short_barrier_triggered": false, "strongest_move_pct": 0.1379, "strongest_symbol": "BTCUSDT", "threshold_pct": 1.5, "window_minutes": 15}`

## Macro / External
- Macro snapshot: `{"available": false, "by_symbol": {}, "confirmed_facts": ["external_macro_and_news_feeds_disabled_under_source_policy_binance_only"], "enabled": false, "reason": "source_policy_binance_only", "risk_mode": "disabled_binance_only", "risk_off_votes": 0, "risk_on_votes": 0, "status": "disabled_by_source_policy"}`

## Options
- Options snapshot: `{"assumptions": ["BTC_gamma_proxy_is_sign_based_and_does_not_observe_real_dealer_inventory", "ETH_gamma_proxy_is_sign_based_and_does_not_observe_real_dealer_inventory"], "by_underlying": {"BTC": {"available": true, "call_oi_contracts": 3797.26, "call_oi_usd": 296336150.69, "expiries_used": ["260424", "260425"], "gamma_balance_proxy": 0.11734224, "gamma_balance_ratio": 0.26767, "gamma_positioning_proxy": "positive_gamma_proxy", "gamma_semantics": "proxy_only", "mark_rows": 1702, "oi_rows": 140, "put_call_oi_ratio": 1.3421, "put_oi_contracts": 5096.21, "put_oi_usd": 397704396.09, "weighted_mark_iv": 1.026209}, "ETH": {"available": true, "call_oi_contracts": 47631.81, "call_oi_usd": 110216545.77, "expiries_used": ["260424", "260425"], "gamma_balance_proxy": -0.52219679, "gamma_balance_ratio": -0.00559, "gamma_positioning_proxy": "flat_proxy", "gamma_semantics": "proxy_only", "mark_rows": 1702, "oi_rows": 108, "put_call_oi_ratio": 1.0399, "put_oi_contracts": 49530.76, "put_oi_usd": 114609584.29, "weighted_mark_iv": 1.259907}}, "confirmed_facts": ["BTC_public_options_mark_and_oi_available", "ETH_public_options_mark_and_oi_available"], "inferences": ["BTC_gamma_positioning_proxy_is_derived_from_public_gamma_and_oi", "ETH_gamma_positioning_proxy_is_derived_from_public_gamma_and_oi"]}`

## Derivatives
- Derivatives snapshot: `{"by_symbol": {"BTCUSDT": {"basis_pct": null, "flow_bias": "position_unwind", "funding_history_points": 4, "funding_rate": -0.00011063, "funding_trend": "flat", "global_long_short_ratio": 0.6598, "oi_change_pct": -0.008102941326435964, "oi_current": 98550.681, "taker_ratio": 0.9581, "top_trader_long_short_ratio": 0.6975}, "ETHUSDT": {"basis_pct": -0.04811762914746378, "flow_bias": "position_unwind", "funding_history_points": 10, "funding_rate": -6.485e-05, "funding_trend": "flat", "global_long_short_ratio": 1.9612, "oi_change_pct": -0.004409607218049283, "oi_current": 2046086.934, "taker_ratio": 0.8811, "top_trader_long_short_ratio": 1.5661}, "SOLUSDT": {"basis_pct": -0.06238825658051581, "flow_bias": "position_unwind", "funding_history_points": 4, "funding_rate": -3.623e-05, "funding_trend": "flat", "global_long_short_ratio": 2.34, "oi_change_pct": -0.007797195973070714, "oi_current": 9340305.26, "taker_ratio": 0.8666, "top_trader_long_short_ratio": 2.4412}, "XRPUSDT": {"basis_pct": null, "flow_bias": "sellers_in_control", "funding_history_points": 4, "funding_rate": -3.97e-06, "funding_trend": "flat", "global_long_short_ratio": 2.3025, "oi_change_pct": 0.013169807761092667, "oi_current": 277062893.9, "taker_ratio": 0.9078, "top_trader_long_short_ratio": 2.4807}}, "confirmed_facts": ["BTCUSDT_public_futures_context_available", "ETHUSDT_public_futures_context_available", "SOLUSDT_public_futures_context_available", "XRPUSDT_public_futures_context_available"]}`

## Harmonic (Debug Only)
- Harmonic snapshot: `{"by_symbol": {"BTCUSDT": {"available": true, "deviation_pct": 11.2035, "pattern": "abcd_like", "ratio_ab_cd": 0.888, "ratio_bc_ab": 0.4423}, "ETHUSDT": {"available": true, "deviation_pct": 0.9946, "pattern": "abcd_like", "ratio_ab_cd": 1.0099, "ratio_bc_ab": 0.5202}}, "enabled": true, "mode": "debug_only_confluence", "standalone_strategy": false, "weight": "low"}`

## JSON
```json
{
  "ts": "2026-04-23T18:46:25.657776+00:00",
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
        "funding_rate": -0.00011063,
        "oi_current": 98550.681,
        "oi_change_pct": -0.008102941326435964,
        "top_trader_long_short_ratio": 0.6975,
        "global_long_short_ratio": 0.6598,
        "taker_ratio": 0.9581,
        "basis_pct": null,
        "funding_trend": "flat",
        "flow_bias": "position_unwind",
        "funding_history_points": 4
      },
      "ETHUSDT": {
        "funding_rate": -6.485e-05,
        "oi_current": 2046086.934,
        "oi_change_pct": -0.004409607218049283,
        "top_trader_long_short_ratio": 1.5661,
        "global_long_short_ratio": 1.9612,
        "taker_ratio": 0.8811,
        "basis_pct": -0.04811762914746378,
        "funding_trend": "flat",
        "flow_bias": "position_unwind",
        "funding_history_points": 10
      },
      "SOLUSDT": {
        "funding_rate": -3.623e-05,
        "oi_current": 9340305.26,
        "oi_change_pct": -0.007797195973070714,
        "top_trader_long_short_ratio": 2.4412,
        "global_long_short_ratio": 2.34,
        "taker_ratio": 0.8666,
        "basis_pct": -0.06238825658051581,
        "funding_trend": "flat",
        "flow_bias": "position_unwind",
        "funding_history_points": 4
      },
      "XRPUSDT": {
        "funding_rate": -3.97e-06,
        "oi_current": 277062893.9,
        "oi_change_pct": 0.013169807761092667,
        "top_trader_long_short_ratio": 2.4807,
        "global_long_short_ratio": 2.3025,
        "taker_ratio": 0.9078,
        "basis_pct": null,
        "funding_trend": "flat",
        "flow_bias": "sellers_in_control",
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
        "call_oi_contracts": 3797.26,
        "put_oi_contracts": 5096.21,
        "call_oi_usd": 296336150.69,
        "put_oi_usd": 397704396.09,
        "put_call_oi_ratio": 1.3421,
        "weighted_mark_iv": 1.026209,
        "gamma_balance_proxy": 0.11734224,
        "gamma_balance_ratio": 0.26767,
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
        "call_oi_contracts": 47631.81,
        "put_oi_contracts": 49530.76,
        "call_oi_usd": 110216545.77,
        "put_oi_usd": 114609584.29,
        "put_call_oi_ratio": 1.0399,
        "weighted_mark_iv": 1.259907,
        "gamma_balance_proxy": -0.52219679,
        "gamma_balance_ratio": -0.00559,
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
    "strongest_move_pct": 0.1379
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
