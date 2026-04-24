# Public Intelligence 2026-04-24T00:53:05.234977+00:00

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
- Barrier snapshot: `{"long_barrier_triggered": false, "short_barrier_triggered": false, "strongest_move_pct": 0.0587, "strongest_symbol": "ETHUSDT", "threshold_pct": 1.5, "window_minutes": 15}`

## Macro / External
- Macro snapshot: `{"available": false, "by_symbol": {}, "confirmed_facts": ["external_macro_and_news_feeds_disabled_under_source_policy_binance_only"], "enabled": false, "reason": "source_policy_binance_only", "risk_mode": "disabled_binance_only", "risk_off_votes": 0, "risk_on_votes": 0, "status": "disabled_by_source_policy"}`

## Options
- Options snapshot: `{"assumptions": ["BTC_gamma_proxy_is_sign_based_and_does_not_observe_real_dealer_inventory", "ETH_gamma_proxy_is_sign_based_and_does_not_observe_real_dealer_inventory"], "by_underlying": {"BTC": {"available": true, "call_oi_contracts": 3891.97, "call_oi_usd": 305295756.89, "expiries_used": ["260424", "260425"], "gamma_balance_proxy": 0.19249842, "gamma_balance_ratio": 0.374936, "gamma_positioning_proxy": "positive_gamma_proxy", "gamma_semantics": "proxy_only", "mark_rows": 1712, "oi_rows": 140, "put_call_oi_ratio": 1.352, "put_oi_contracts": 5261.78, "put_oi_usd": 412747042.67, "weighted_mark_iv": 0.823747}, "ETH": {"available": true, "call_oi_contracts": 48576.96, "call_oi_usd": 113415915.93, "expiries_used": ["260424", "260425"], "gamma_balance_proxy": 14.31600559, "gamma_balance_ratio": 0.142177, "gamma_positioning_proxy": "positive_gamma_proxy", "gamma_semantics": "proxy_only", "mark_rows": 1712, "oi_rows": 108, "put_call_oi_ratio": 1.0537, "put_oi_contracts": 51187.28, "put_oi_usd": 119510406.69, "weighted_mark_iv": 1.248804}}, "confirmed_facts": ["BTC_public_options_mark_and_oi_available", "ETH_public_options_mark_and_oi_available"], "inferences": ["BTC_gamma_positioning_proxy_is_derived_from_public_gamma_and_oi", "ETH_gamma_positioning_proxy_is_derived_from_public_gamma_and_oi"]}`

## Derivatives
- Derivatives snapshot: `{"by_symbol": {"BTCUSDT": {"basis_pct": -0.04610475356536167, "flow_bias": "buyers_in_control", "funding_history_points": 4, "funding_rate": -3.468e-05, "funding_trend": "flat", "global_long_short_ratio": 0.6678, "oi_change_pct": 0.00045434628518070674, "oi_current": 99362.664, "taker_ratio": 1.0982, "top_trader_long_short_ratio": 0.7036}, "ETHUSDT": {"basis_pct": -0.06464055134541194, "flow_bias": "position_unwind", "funding_history_points": 4, "funding_rate": -4.085e-05, "funding_trend": "flat", "global_long_short_ratio": 1.9265, "oi_change_pct": -0.001149711630297312, "oi_current": 2022063.539, "taker_ratio": 0.7504, "top_trader_long_short_ratio": 1.5214}, "SOLUSDT": {"basis_pct": -0.06306997435372706, "flow_bias": "position_unwind", "funding_history_points": 4, "funding_rate": -5.806e-05, "funding_trend": "flat", "global_long_short_ratio": 2.283, "oi_change_pct": -0.003125921406704979, "oi_current": 9277389.34, "taker_ratio": 0.9618, "top_trader_long_short_ratio": 2.3852}, "XRPUSDT": {"basis_pct": -0.061064973301495754, "flow_bias": "position_unwind", "funding_history_points": 4, "funding_rate": 4.778e-05, "funding_trend": "flat", "global_long_short_ratio": 2.3467, "oi_change_pct": -0.0037777350199582616, "oi_current": 272918099.0, "taker_ratio": 1.1911, "top_trader_long_short_ratio": 2.5336}}, "confirmed_facts": ["BTCUSDT_public_futures_context_available", "ETHUSDT_public_futures_context_available", "SOLUSDT_public_futures_context_available", "XRPUSDT_public_futures_context_available"]}`

## Harmonic (Debug Only)
- Harmonic snapshot: `{"by_symbol": {"BTCUSDT": {"available": true, "deviation_pct": 3.1903, "pattern": "none", "ratio_ab_cd": 0.9681, "ratio_bc_ab": 1.1872}, "ETHUSDT": {"available": true, "deviation_pct": 11.5756, "pattern": "abcd_like", "ratio_ab_cd": 1.1158, "ratio_bc_ab": 0.7332}}, "enabled": true, "mode": "debug_only_confluence", "standalone_strategy": false, "weight": "low"}`

## JSON
```json
{
  "ts": "2026-04-24T00:53:05.234977+00:00",
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
        "funding_rate": -3.468e-05,
        "oi_current": 99362.664,
        "oi_change_pct": 0.00045434628518070674,
        "top_trader_long_short_ratio": 0.7036,
        "global_long_short_ratio": 0.6678,
        "taker_ratio": 1.0982,
        "basis_pct": -0.04610475356536167,
        "funding_trend": "flat",
        "flow_bias": "buyers_in_control",
        "funding_history_points": 4
      },
      "ETHUSDT": {
        "funding_rate": -4.085e-05,
        "oi_current": 2022063.539,
        "oi_change_pct": -0.001149711630297312,
        "top_trader_long_short_ratio": 1.5214,
        "global_long_short_ratio": 1.9265,
        "taker_ratio": 0.7504,
        "basis_pct": -0.06464055134541194,
        "funding_trend": "flat",
        "flow_bias": "position_unwind",
        "funding_history_points": 4
      },
      "SOLUSDT": {
        "funding_rate": -5.806e-05,
        "oi_current": 9277389.34,
        "oi_change_pct": -0.003125921406704979,
        "top_trader_long_short_ratio": 2.3852,
        "global_long_short_ratio": 2.283,
        "taker_ratio": 0.9618,
        "basis_pct": -0.06306997435372706,
        "funding_trend": "flat",
        "flow_bias": "position_unwind",
        "funding_history_points": 4
      },
      "XRPUSDT": {
        "funding_rate": 4.778e-05,
        "oi_current": 272918099.0,
        "oi_change_pct": -0.0037777350199582616,
        "top_trader_long_short_ratio": 2.5336,
        "global_long_short_ratio": 2.3467,
        "taker_ratio": 1.1911,
        "basis_pct": -0.061064973301495754,
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
        "call_oi_contracts": 3891.97,
        "put_oi_contracts": 5261.78,
        "call_oi_usd": 305295756.89,
        "put_oi_usd": 412747042.67,
        "put_call_oi_ratio": 1.352,
        "weighted_mark_iv": 0.823747,
        "gamma_balance_proxy": 0.19249842,
        "gamma_balance_ratio": 0.374936,
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
        "call_oi_contracts": 48576.96,
        "put_oi_contracts": 51187.28,
        "call_oi_usd": 113415915.93,
        "put_oi_usd": 119510406.69,
        "put_call_oi_ratio": 1.0537,
        "weighted_mark_iv": 1.248804,
        "gamma_balance_proxy": 14.31600559,
        "gamma_balance_ratio": 0.142177,
        "gamma_positioning_proxy": "positive_gamma_proxy",
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
    "strongest_symbol": "ETHUSDT",
    "strongest_move_pct": 0.0587
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
