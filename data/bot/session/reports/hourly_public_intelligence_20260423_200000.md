# Public Intelligence 2026-04-23T20:41:05.792390+00:00

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
- Barrier snapshot: `{"long_barrier_triggered": false, "short_barrier_triggered": false, "strongest_move_pct": 0.26, "strongest_symbol": "BTCUSDT", "threshold_pct": 1.5, "window_minutes": 15}`

## Macro / External
- Macro snapshot: `{"available": false, "by_symbol": {}, "confirmed_facts": ["external_macro_and_news_feeds_disabled_under_source_policy_binance_only"], "enabled": false, "reason": "source_policy_binance_only", "risk_mode": "disabled_binance_only", "risk_off_votes": 0, "risk_on_votes": 0, "status": "disabled_by_source_policy"}`

## Options
- Options snapshot: `{"assumptions": ["BTC_gamma_proxy_is_sign_based_and_does_not_observe_real_dealer_inventory", "ETH_gamma_proxy_is_sign_based_and_does_not_observe_real_dealer_inventory"], "by_underlying": {"BTC": {"available": true, "call_oi_contracts": 3814.49, "call_oi_usd": 297287848.41, "expiries_used": ["260424", "260425"], "gamma_balance_proxy": 0.11306858, "gamma_balance_ratio": 0.245556, "gamma_positioning_proxy": "positive_gamma_proxy", "gamma_semantics": "proxy_only", "mark_rows": 1702, "oi_rows": 140, "put_call_oi_ratio": 1.345, "put_oi_contracts": 5130.64, "put_oi_usd": 399864943.39, "weighted_mark_iv": 0.814133}, "ETH": {"available": true, "call_oi_contracts": 47727.3, "call_oi_usd": 110912138.22, "expiries_used": ["260424", "260425"], "gamma_balance_proxy": 8.76591928, "gamma_balance_ratio": 0.088334, "gamma_positioning_proxy": "flat_proxy", "gamma_semantics": "proxy_only", "mark_rows": 1702, "oi_rows": 108, "put_call_oi_ratio": 1.0488, "put_oi_contracts": 50056.89, "put_oi_usd": 116325860.83, "weighted_mark_iv": 1.251214}}, "confirmed_facts": ["BTC_public_options_mark_and_oi_available", "ETH_public_options_mark_and_oi_available"], "inferences": ["BTC_gamma_positioning_proxy_is_derived_from_public_gamma_and_oi", "ETH_gamma_positioning_proxy_is_derived_from_public_gamma_and_oi"]}`

## Derivatives
- Derivatives snapshot: `{"by_symbol": {"BTCUSDT": {"basis_pct": -0.07903189634048499, "flow_bias": "buyers_in_control", "funding_history_points": 4, "funding_rate": -8.368e-05, "funding_trend": "flat", "global_long_short_ratio": 0.6598, "oi_change_pct": 0.002677660847091312, "oi_current": 98708.322, "taker_ratio": 1.1556, "top_trader_long_short_ratio": 0.6995}, "ETHUSDT": {"basis_pct": -0.07090025253369203, "flow_bias": "position_unwind", "funding_history_points": 4, "funding_rate": -5.716e-05, "funding_trend": "flat", "global_long_short_ratio": 1.9525, "oi_change_pct": -0.002049861109069684, "oi_current": 2037636.738, "taker_ratio": 0.9777, "top_trader_long_short_ratio": 1.5589}, "SOLUSDT": {"basis_pct": -0.08936717079803225, "flow_bias": "sellers_in_control", "funding_history_points": 4, "funding_rate": -6.588e-05, "funding_trend": "flat", "global_long_short_ratio": 2.3356, "oi_change_pct": 0.0024908706564295002, "oi_current": 9380523.92, "taker_ratio": 0.8182, "top_trader_long_short_ratio": 2.4352}, "XRPUSDT": {"basis_pct": -0.06296703607559875, "flow_bias": "neutral", "funding_history_points": 10, "funding_rate": 3.384e-05, "funding_trend": "flat", "global_long_short_ratio": 2.3411, "oi_change_pct": 0.0008453113434987447, "oi_current": 276107383.0, "taker_ratio": 1.0021, "top_trader_long_short_ratio": 2.5137}}, "confirmed_facts": ["BTCUSDT_public_futures_context_available", "ETHUSDT_public_futures_context_available", "SOLUSDT_public_futures_context_available", "XRPUSDT_public_futures_context_available"]}`

## Harmonic (Debug Only)
- Harmonic snapshot: `{"by_symbol": {"BTCUSDT": {"available": true, "deviation_pct": 138.3144, "pattern": "none", "ratio_ab_cd": 2.3831, "ratio_bc_ab": 2.0074}, "ETHUSDT": {"available": true, "deviation_pct": 42.3521, "pattern": "none", "ratio_ab_cd": 1.4235, "ratio_bc_ab": 1.9416}}, "enabled": true, "mode": "debug_only_confluence", "standalone_strategy": false, "weight": "low"}`

## JSON
```json
{
  "ts": "2026-04-23T20:41:05.792390+00:00",
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
        "funding_rate": -8.368e-05,
        "oi_current": 98708.322,
        "oi_change_pct": 0.002677660847091312,
        "top_trader_long_short_ratio": 0.6995,
        "global_long_short_ratio": 0.6598,
        "taker_ratio": 1.1556,
        "basis_pct": -0.07903189634048499,
        "funding_trend": "flat",
        "flow_bias": "buyers_in_control",
        "funding_history_points": 4
      },
      "ETHUSDT": {
        "funding_rate": -5.716e-05,
        "oi_current": 2037636.738,
        "oi_change_pct": -0.002049861109069684,
        "top_trader_long_short_ratio": 1.5589,
        "global_long_short_ratio": 1.9525,
        "taker_ratio": 0.9777,
        "basis_pct": -0.07090025253369203,
        "funding_trend": "flat",
        "flow_bias": "position_unwind",
        "funding_history_points": 4
      },
      "SOLUSDT": {
        "funding_rate": -6.588e-05,
        "oi_current": 9380523.92,
        "oi_change_pct": 0.0024908706564295002,
        "top_trader_long_short_ratio": 2.4352,
        "global_long_short_ratio": 2.3356,
        "taker_ratio": 0.8182,
        "basis_pct": -0.08936717079803225,
        "funding_trend": "flat",
        "flow_bias": "sellers_in_control",
        "funding_history_points": 4
      },
      "XRPUSDT": {
        "funding_rate": 3.384e-05,
        "oi_current": 276107383.0,
        "oi_change_pct": 0.0008453113434987447,
        "top_trader_long_short_ratio": 2.5137,
        "global_long_short_ratio": 2.3411,
        "taker_ratio": 1.0021,
        "basis_pct": -0.06296703607559875,
        "funding_trend": "flat",
        "flow_bias": "neutral",
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
        "call_oi_contracts": 3814.49,
        "put_oi_contracts": 5130.64,
        "call_oi_usd": 297287848.41,
        "put_oi_usd": 399864943.39,
        "put_call_oi_ratio": 1.345,
        "weighted_mark_iv": 0.814133,
        "gamma_balance_proxy": 0.11306858,
        "gamma_balance_ratio": 0.245556,
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
        "call_oi_contracts": 47727.3,
        "put_oi_contracts": 50056.89,
        "call_oi_usd": 110912138.22,
        "put_oi_usd": 116325860.83,
        "put_call_oi_ratio": 1.0488,
        "weighted_mark_iv": 1.251214,
        "gamma_balance_proxy": 8.76591928,
        "gamma_balance_ratio": 0.088334,
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
    "strongest_move_pct": 0.26
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
