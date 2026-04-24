# Public Intelligence 2026-04-23T19:47:13.515415+00:00

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
- Barrier snapshot: `{"long_barrier_triggered": false, "short_barrier_triggered": false, "strongest_move_pct": -0.1942, "strongest_symbol": "BTCUSDT", "threshold_pct": 1.5, "window_minutes": 15}`

## Macro / External
- Macro snapshot: `{"available": false, "by_symbol": {}, "confirmed_facts": ["external_macro_and_news_feeds_disabled_under_source_policy_binance_only"], "enabled": false, "reason": "source_policy_binance_only", "risk_mode": "disabled_binance_only", "risk_off_votes": 0, "risk_on_votes": 0, "status": "disabled_by_source_policy"}`

## Options
- Options snapshot: `{"assumptions": ["BTC_gamma_proxy_is_sign_based_and_does_not_observe_real_dealer_inventory", "ETH_gamma_proxy_is_sign_based_and_does_not_observe_real_dealer_inventory"], "by_underlying": {"BTC": {"available": true, "call_oi_contracts": 3809.22, "call_oi_usd": 295915946.65, "expiries_used": ["260424", "260425"], "gamma_balance_proxy": 0.08372616, "gamma_balance_ratio": 0.186006, "gamma_positioning_proxy": "positive_gamma_proxy", "gamma_semantics": "proxy_only", "mark_rows": 1702, "oi_rows": 140, "put_call_oi_ratio": 1.341, "put_oi_contracts": 5108.34, "put_oi_usd": 396836955.31, "weighted_mark_iv": 0.814024}, "ETH": {"available": true, "call_oi_contracts": 47339.28, "call_oi_usd": 109443816.48, "expiries_used": ["260424", "260425"], "gamma_balance_proxy": -2.49652708, "gamma_balance_ratio": -0.026895, "gamma_positioning_proxy": "flat_proxy", "gamma_semantics": "proxy_only", "mark_rows": 1702, "oi_rows": 108, "put_call_oi_ratio": 1.0539, "put_oi_contracts": 49890.89, "put_oi_usd": 115342889.99, "weighted_mark_iv": 1.253718}}, "confirmed_facts": ["BTC_public_options_mark_and_oi_available", "ETH_public_options_mark_and_oi_available"], "inferences": ["BTC_gamma_positioning_proxy_is_derived_from_public_gamma_and_oi", "ETH_gamma_positioning_proxy_is_derived_from_public_gamma_and_oi"]}`

## Derivatives
- Derivatives snapshot: `{"by_symbol": {"BTCUSDT": {"basis_pct": -0.06840945343427, "flow_bias": "buyers_in_control", "funding_history_points": 4, "funding_rate": -9.889e-05, "funding_trend": "flat", "global_long_short_ratio": 0.6504, "oi_change_pct": 0.0026680130055520834, "oi_current": 98475.929, "taker_ratio": 1.0587, "top_trader_long_short_ratio": 0.6883}, "ETHUSDT": {"basis_pct": -0.06372759411877808, "flow_bias": "sellers_in_control", "funding_history_points": 10, "funding_rate": -6.558e-05, "funding_trend": "flat", "global_long_short_ratio": 1.9464, "oi_change_pct": 0.0019073352056813153, "oi_current": 2046335.119, "taker_ratio": 0.8559, "top_trader_long_short_ratio": 1.5556}, "SOLUSDT": {"basis_pct": -0.06442646882487581, "flow_bias": "position_unwind", "funding_history_points": 10, "funding_rate": -6.09e-05, "funding_trend": "flat", "global_long_short_ratio": 2.3256, "oi_change_pct": -0.004347930451566562, "oi_current": 9383767.04, "taker_ratio": 1.0027, "top_trader_long_short_ratio": 2.4352}, "XRPUSDT": {"basis_pct": -0.036940203903734946, "flow_bias": "position_unwind", "funding_history_points": 10, "funding_rate": 1.978e-05, "funding_trend": "flat", "global_long_short_ratio": 2.3322, "oi_change_pct": -0.0018290574823704997, "oi_current": 276572739.0, "taker_ratio": 0.9149, "top_trader_long_short_ratio": 2.4953}}, "confirmed_facts": ["BTCUSDT_public_futures_context_available", "ETHUSDT_public_futures_context_available", "SOLUSDT_public_futures_context_available", "XRPUSDT_public_futures_context_available"]}`

## Harmonic (Debug Only)
- Harmonic snapshot: `{"by_symbol": {"BTCUSDT": {"available": true, "deviation_pct": 138.3144, "pattern": "none", "ratio_ab_cd": 2.3831, "ratio_bc_ab": 2.0074}, "ETHUSDT": {"available": true, "deviation_pct": 42.3521, "pattern": "none", "ratio_ab_cd": 1.4235, "ratio_bc_ab": 1.9416}}, "enabled": true, "mode": "debug_only_confluence", "standalone_strategy": false, "weight": "low"}`

## JSON
```json
{
  "ts": "2026-04-23T19:47:13.515415+00:00",
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
        "funding_rate": -9.889e-05,
        "oi_current": 98475.929,
        "oi_change_pct": 0.0026680130055520834,
        "top_trader_long_short_ratio": 0.6883,
        "global_long_short_ratio": 0.6504,
        "taker_ratio": 1.0587,
        "basis_pct": -0.06840945343427,
        "funding_trend": "flat",
        "flow_bias": "buyers_in_control",
        "funding_history_points": 4
      },
      "ETHUSDT": {
        "funding_rate": -6.558e-05,
        "oi_current": 2046335.119,
        "oi_change_pct": 0.0019073352056813153,
        "top_trader_long_short_ratio": 1.5556,
        "global_long_short_ratio": 1.9464,
        "taker_ratio": 0.8559,
        "basis_pct": -0.06372759411877808,
        "funding_trend": "flat",
        "flow_bias": "sellers_in_control",
        "funding_history_points": 10
      },
      "SOLUSDT": {
        "funding_rate": -6.09e-05,
        "oi_current": 9383767.04,
        "oi_change_pct": -0.004347930451566562,
        "top_trader_long_short_ratio": 2.4352,
        "global_long_short_ratio": 2.3256,
        "taker_ratio": 1.0027,
        "basis_pct": -0.06442646882487581,
        "funding_trend": "flat",
        "flow_bias": "position_unwind",
        "funding_history_points": 10
      },
      "XRPUSDT": {
        "funding_rate": 1.978e-05,
        "oi_current": 276572739.0,
        "oi_change_pct": -0.0018290574823704997,
        "top_trader_long_short_ratio": 2.4953,
        "global_long_short_ratio": 2.3322,
        "taker_ratio": 0.9149,
        "basis_pct": -0.036940203903734946,
        "funding_trend": "flat",
        "flow_bias": "position_unwind",
        "funding_history_points": 10
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
        "call_oi_contracts": 3809.22,
        "put_oi_contracts": 5108.34,
        "call_oi_usd": 295915946.65,
        "put_oi_usd": 396836955.31,
        "put_call_oi_ratio": 1.341,
        "weighted_mark_iv": 0.814024,
        "gamma_balance_proxy": 0.08372616,
        "gamma_balance_ratio": 0.186006,
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
        "call_oi_contracts": 47339.28,
        "put_oi_contracts": 49890.89,
        "call_oi_usd": 109443816.48,
        "put_oi_usd": 115342889.99,
        "put_call_oi_ratio": 1.0539,
        "weighted_mark_iv": 1.253718,
        "gamma_balance_proxy": -2.49652708,
        "gamma_balance_ratio": -0.026895,
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
    "strongest_move_pct": -0.1942
  },
  "harmonic": {
    "enabled": true,
    "mode": "debug_only_confluence",
    "standalone_strategy": false,
    "weight": "low",
    "by_symbol": {
      "BTCUSDT": {
        "available": true,
        "pattern": "none",
        "ratio_ab_cd": 2.3831,
        "ratio_bc_ab": 2.0074,
        "deviation_pct": 138.3144
      },
      "ETHUSDT": {
        "available": true,
        "pattern": "none",
        "ratio_ab_cd": 1.4235,
        "ratio_bc_ab": 1.9416,
        "deviation_pct": 42.3521
      }
    }
  }
}
```
