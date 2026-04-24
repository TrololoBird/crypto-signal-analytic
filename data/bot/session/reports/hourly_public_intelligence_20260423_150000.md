# Public Intelligence 2026-04-23T15:48:53.313139+00:00

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
- Barrier snapshot: `{"long_barrier_triggered": false, "short_barrier_triggered": false, "strongest_move_pct": -0.197, "strongest_symbol": "ETHUSDT", "threshold_pct": 1.5, "window_minutes": 15}`

## Macro / External
- Macro snapshot: `{"available": false, "by_symbol": {}, "confirmed_facts": ["external_macro_and_news_feeds_disabled_under_source_policy_binance_only"], "enabled": false, "reason": "source_policy_binance_only", "risk_mode": "disabled_binance_only", "risk_off_votes": 0, "risk_on_votes": 0, "status": "disabled_by_source_policy"}`

## Options
- Options snapshot: `{"assumptions": ["BTC_gamma_proxy_is_sign_based_and_does_not_observe_real_dealer_inventory", "ETH_gamma_proxy_is_sign_based_and_does_not_observe_real_dealer_inventory"], "by_underlying": {"BTC": {"available": true, "call_oi_contracts": 3735.92, "call_oi_usd": 292656845.57, "expiries_used": ["260424", "260425"], "gamma_balance_proxy": 0.14047365, "gamma_balance_ratio": 0.345928, "gamma_positioning_proxy": "positive_gamma_proxy", "gamma_semantics": "proxy_only", "mark_rows": 1702, "oi_rows": 140, "put_call_oi_ratio": 1.3304, "put_oi_contracts": 4970.17, "put_oi_usd": 389342992.93, "weighted_mark_iv": 0.776649}, "ETH": {"available": true, "call_oi_contracts": 44724.31, "call_oi_usd": 104348950.32, "expiries_used": ["260424", "260425"], "gamma_balance_proxy": 2.210508, "gamma_balance_ratio": 0.025175, "gamma_positioning_proxy": "flat_proxy", "gamma_semantics": "proxy_only", "mark_rows": 1702, "oi_rows": 108, "put_call_oi_ratio": 1.0828, "put_oi_contracts": 48427.18, "put_oi_usd": 112988336.76, "weighted_mark_iv": 1.216511}}, "confirmed_facts": ["BTC_public_options_mark_and_oi_available", "ETH_public_options_mark_and_oi_available"], "inferences": ["BTC_gamma_positioning_proxy_is_derived_from_public_gamma_and_oi", "ETH_gamma_positioning_proxy_is_derived_from_public_gamma_and_oi"]}`

## Derivatives
- Derivatives snapshot: `{"by_symbol": {"BTCUSDT": {"basis_pct": -0.0682919344296999, "flow_bias": "buyers_in_control", "funding_history_points": 4, "funding_rate": -6.13e-05, "funding_trend": "flat", "global_long_short_ratio": 0.6801, "oi_change_pct": 0.0021351635827708826, "oi_current": 98176.823, "taker_ratio": 1.3629, "top_trader_long_short_ratio": 0.7091}, "ETHUSDT": {"basis_pct": null, "flow_bias": "position_unwind", "funding_history_points": 10, "funding_rate": -6.051e-05, "funding_trend": "flat", "global_long_short_ratio": 1.8539, "oi_change_pct": -0.00011718181238251635, "oi_current": 2056121.185, "taker_ratio": 1.2117, "top_trader_long_short_ratio": 1.4667}, "SOLUSDT": {"basis_pct": -0.057814339167356614, "flow_bias": "position_unwind", "funding_history_points": 10, "funding_rate": 1.617e-05, "funding_trend": "flat", "global_long_short_ratio": 2.2227, "oi_change_pct": -0.00010899901081984353, "oi_current": 9358546.99, "taker_ratio": 1.266, "top_trader_long_short_ratio": 2.319}, "XRPUSDT": {"basis_pct": -0.06374016984814397, "flow_bias": "position_unwind", "funding_history_points": 4, "funding_rate": -2.584e-05, "funding_trend": "falling", "global_long_short_ratio": 2.4483, "oi_change_pct": -0.004155565060868671, "oi_current": 275680639.9, "taker_ratio": 1.0547, "top_trader_long_short_ratio": 2.601}}, "confirmed_facts": ["BTCUSDT_public_futures_context_available", "ETHUSDT_public_futures_context_available", "SOLUSDT_public_futures_context_available", "XRPUSDT_public_futures_context_available"]}`

## Harmonic (Debug Only)
- Harmonic snapshot: `{"by_symbol": {"BTCUSDT": {"available": true, "deviation_pct": 11.2035, "pattern": "abcd_like", "ratio_ab_cd": 0.888, "ratio_bc_ab": 0.4423}, "ETHUSDT": {"available": true, "deviation_pct": 0.9946, "pattern": "abcd_like", "ratio_ab_cd": 1.0099, "ratio_bc_ab": 0.5202}}, "enabled": true, "mode": "debug_only_confluence", "standalone_strategy": false, "weight": "low"}`

## JSON
```json
{
  "ts": "2026-04-23T15:48:53.313139+00:00",
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
        "funding_rate": -6.13e-05,
        "oi_current": 98176.823,
        "oi_change_pct": 0.0021351635827708826,
        "top_trader_long_short_ratio": 0.7091,
        "global_long_short_ratio": 0.6801,
        "taker_ratio": 1.3629,
        "basis_pct": -0.0682919344296999,
        "funding_trend": "flat",
        "flow_bias": "buyers_in_control",
        "funding_history_points": 4
      },
      "ETHUSDT": {
        "funding_rate": -6.051e-05,
        "oi_current": 2056121.185,
        "oi_change_pct": -0.00011718181238251635,
        "top_trader_long_short_ratio": 1.4667,
        "global_long_short_ratio": 1.8539,
        "taker_ratio": 1.2117,
        "basis_pct": null,
        "funding_trend": "flat",
        "flow_bias": "position_unwind",
        "funding_history_points": 10
      },
      "SOLUSDT": {
        "funding_rate": 1.617e-05,
        "oi_current": 9358546.99,
        "oi_change_pct": -0.00010899901081984353,
        "top_trader_long_short_ratio": 2.319,
        "global_long_short_ratio": 2.2227,
        "taker_ratio": 1.266,
        "basis_pct": -0.057814339167356614,
        "funding_trend": "flat",
        "flow_bias": "position_unwind",
        "funding_history_points": 10
      },
      "XRPUSDT": {
        "funding_rate": -2.584e-05,
        "oi_current": 275680639.9,
        "oi_change_pct": -0.004155565060868671,
        "top_trader_long_short_ratio": 2.601,
        "global_long_short_ratio": 2.4483,
        "taker_ratio": 1.0547,
        "basis_pct": -0.06374016984814397,
        "funding_trend": "falling",
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
        "call_oi_contracts": 3735.92,
        "put_oi_contracts": 4970.17,
        "call_oi_usd": 292656845.57,
        "put_oi_usd": 389342992.93,
        "put_call_oi_ratio": 1.3304,
        "weighted_mark_iv": 0.776649,
        "gamma_balance_proxy": 0.14047365,
        "gamma_balance_ratio": 0.345928,
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
        "call_oi_contracts": 44724.31,
        "put_oi_contracts": 48427.18,
        "call_oi_usd": 104348950.32,
        "put_oi_usd": 112988336.76,
        "put_call_oi_ratio": 1.0828,
        "weighted_mark_iv": 1.216511,
        "gamma_balance_proxy": 2.210508,
        "gamma_balance_ratio": 0.025175,
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
    "strongest_move_pct": -0.197
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
