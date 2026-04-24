# Public Intelligence 2026-04-23T21:34:14.179029+00:00

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
- Barrier snapshot: `{"long_barrier_triggered": false, "short_barrier_triggered": false, "strongest_move_pct": 0.0916, "strongest_symbol": "BTCUSDT", "threshold_pct": 1.5, "window_minutes": 15}`

## Macro / External
- Macro snapshot: `{"available": false, "by_symbol": {}, "confirmed_facts": ["external_macro_and_news_feeds_disabled_under_source_policy_binance_only"], "enabled": false, "reason": "source_policy_binance_only", "risk_mode": "disabled_binance_only", "risk_off_votes": 0, "risk_on_votes": 0, "status": "disabled_by_source_policy"}`

## Options
- Options snapshot: `{"assumptions": ["BTC_gamma_proxy_is_sign_based_and_does_not_observe_real_dealer_inventory", "ETH_gamma_proxy_is_sign_based_and_does_not_observe_real_dealer_inventory"], "by_underlying": {"BTC": {"available": true, "call_oi_contracts": 3801.99, "call_oi_usd": 296456979.64, "expiries_used": ["260424", "260425"], "gamma_balance_proxy": 0.11381142, "gamma_balance_ratio": 0.243393, "gamma_positioning_proxy": "positive_gamma_proxy", "gamma_semantics": "proxy_only", "mark_rows": 1712, "oi_rows": 140, "put_call_oi_ratio": 1.3499, "put_oi_contracts": 5132.15, "put_oi_usd": 400175088.76, "weighted_mark_iv": 1.012648}, "ETH": {"available": true, "call_oi_contracts": 47771.25, "call_oi_usd": 111085033.47, "expiries_used": ["260424", "260425"], "gamma_balance_proxy": 8.301762, "gamma_balance_ratio": 0.085582, "gamma_positioning_proxy": "flat_proxy", "gamma_semantics": "proxy_only", "mark_rows": 1712, "oi_rows": 108, "put_call_oi_ratio": 1.0461, "put_oi_contracts": 49971.83, "put_oi_usd": 116202209.9, "weighted_mark_iv": 1.252813}}, "confirmed_facts": ["BTC_public_options_mark_and_oi_available", "ETH_public_options_mark_and_oi_available"], "inferences": ["BTC_gamma_positioning_proxy_is_derived_from_public_gamma_and_oi", "ETH_gamma_positioning_proxy_is_derived_from_public_gamma_and_oi"]}`

## Derivatives
- Derivatives snapshot: `{"by_symbol": {"BTCUSDT": {"basis_pct": -0.07067473676034207, "flow_bias": "position_unwind", "funding_history_points": 4, "funding_rate": -7.534e-05, "funding_trend": "flat", "global_long_short_ratio": 0.6644, "oi_change_pct": -0.0017518539944646472, "oi_current": 98530.476, "taker_ratio": 1.2195, "top_trader_long_short_ratio": 0.7027}, "ETHUSDT": {"basis_pct": -0.041760669081812, "flow_bias": "position_unwind", "funding_history_points": 10, "funding_rate": -4.052e-05, "funding_trend": "flat", "global_long_short_ratio": 1.936, "oi_change_pct": -0.009817214197950341, "oi_current": 2016564.667, "taker_ratio": 1.0031, "top_trader_long_short_ratio": 1.5342}, "SOLUSDT": {"basis_pct": -0.1068990144893596, "flow_bias": "position_unwind", "funding_history_points": 4, "funding_rate": -8.616e-05, "funding_trend": "flat", "global_long_short_ratio": 2.3311, "oi_change_pct": -0.004565817265223204, "oi_current": 9319857.37, "taker_ratio": 1.1717, "top_trader_long_short_ratio": 2.4305}, "XRPUSDT": {"basis_pct": -0.049605865356888115, "flow_bias": "position_unwind", "funding_history_points": 10, "funding_rate": 4.204e-05, "funding_trend": "flat", "global_long_short_ratio": 2.3501, "oi_change_pct": -0.007716232105766441, "oi_current": 274420511.4, "taker_ratio": 1.7975, "top_trader_long_short_ratio": 2.5423}}, "confirmed_facts": ["BTCUSDT_public_futures_context_available", "ETHUSDT_public_futures_context_available", "SOLUSDT_public_futures_context_available", "XRPUSDT_public_futures_context_available"]}`

## Harmonic (Debug Only)
- Harmonic snapshot: `{"by_symbol": {"BTCUSDT": {"available": true, "deviation_pct": 3.1903, "pattern": "none", "ratio_ab_cd": 0.9681, "ratio_bc_ab": 1.1872}, "ETHUSDT": {"available": true, "deviation_pct": 11.5756, "pattern": "abcd_like", "ratio_ab_cd": 1.1158, "ratio_bc_ab": 0.7332}}, "enabled": true, "mode": "debug_only_confluence", "standalone_strategy": false, "weight": "low"}`

## JSON
```json
{
  "ts": "2026-04-23T21:34:14.179029+00:00",
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
        "funding_rate": -7.534e-05,
        "oi_current": 98530.476,
        "oi_change_pct": -0.0017518539944646472,
        "top_trader_long_short_ratio": 0.7027,
        "global_long_short_ratio": 0.6644,
        "taker_ratio": 1.2195,
        "basis_pct": -0.07067473676034207,
        "funding_trend": "flat",
        "flow_bias": "position_unwind",
        "funding_history_points": 4
      },
      "ETHUSDT": {
        "funding_rate": -4.052e-05,
        "oi_current": 2016564.667,
        "oi_change_pct": -0.009817214197950341,
        "top_trader_long_short_ratio": 1.5342,
        "global_long_short_ratio": 1.936,
        "taker_ratio": 1.0031,
        "basis_pct": -0.041760669081812,
        "funding_trend": "flat",
        "flow_bias": "position_unwind",
        "funding_history_points": 10
      },
      "SOLUSDT": {
        "funding_rate": -8.616e-05,
        "oi_current": 9319857.37,
        "oi_change_pct": -0.004565817265223204,
        "top_trader_long_short_ratio": 2.4305,
        "global_long_short_ratio": 2.3311,
        "taker_ratio": 1.1717,
        "basis_pct": -0.1068990144893596,
        "funding_trend": "flat",
        "flow_bias": "position_unwind",
        "funding_history_points": 4
      },
      "XRPUSDT": {
        "funding_rate": 4.204e-05,
        "oi_current": 274420511.4,
        "oi_change_pct": -0.007716232105766441,
        "top_trader_long_short_ratio": 2.5423,
        "global_long_short_ratio": 2.3501,
        "taker_ratio": 1.7975,
        "basis_pct": -0.049605865356888115,
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
        "call_oi_contracts": 3801.99,
        "put_oi_contracts": 5132.15,
        "call_oi_usd": 296456979.64,
        "put_oi_usd": 400175088.76,
        "put_call_oi_ratio": 1.3499,
        "weighted_mark_iv": 1.012648,
        "gamma_balance_proxy": 0.11381142,
        "gamma_balance_ratio": 0.243393,
        "gamma_positioning_proxy": "positive_gamma_proxy",
        "gamma_semantics": "proxy_only",
        "mark_rows": 1712,
        "oi_rows": 140
      },
      "ETH": {
        "available": true,
        "expiries_used": [
          "260424",
          "260425"
        ],
        "call_oi_contracts": 47771.25,
        "put_oi_contracts": 49971.83,
        "call_oi_usd": 111085033.47,
        "put_oi_usd": 116202209.9,
        "put_call_oi_ratio": 1.0461,
        "weighted_mark_iv": 1.252813,
        "gamma_balance_proxy": 8.301762,
        "gamma_balance_ratio": 0.085582,
        "gamma_positioning_proxy": "flat_proxy",
        "gamma_semantics": "proxy_only",
        "mark_rows": 1712,
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
    "strongest_move_pct": 0.0916
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
        "ratio_ab_cd": 0.9681,
        "ratio_bc_ab": 1.1872,
        "deviation_pct": 3.1903
      },
      "ETHUSDT": {
        "available": true,
        "pattern": "abcd_like",
        "ratio_ab_cd": 1.1158,
        "ratio_bc_ab": 0.7332,
        "deviation_pct": 11.5756
      }
    }
  }
}
```
