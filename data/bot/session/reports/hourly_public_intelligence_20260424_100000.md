# Public Intelligence 2026-04-24T10:59:22.541566+00:00

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
- Barrier snapshot: `{"long_barrier_triggered": false, "short_barrier_triggered": false, "strongest_move_pct": 0.0977, "strongest_symbol": "ETHUSDT", "threshold_pct": 1.5, "window_minutes": 15}`

## Macro / External
- Macro snapshot: `{"available": false, "by_symbol": {}, "confirmed_facts": ["external_macro_and_news_feeds_disabled_under_source_policy_binance_only"], "enabled": false, "reason": "source_policy_binance_only", "risk_mode": "disabled_binance_only", "risk_off_votes": 0, "risk_on_votes": 0, "status": "disabled_by_source_policy"}`

## Options
- Options snapshot: `{"assumptions": ["BTC_gamma_proxy_is_sign_based_and_does_not_observe_real_dealer_inventory", "ETH_gamma_proxy_is_sign_based_and_does_not_observe_real_dealer_inventory"], "by_underlying": {"BTC": {"available": true, "call_oi_contracts": 551.6, "call_oi_usd": 42894729.0, "expiries_used": ["260425", "260426"], "gamma_balance_proxy": -0.00453685, "gamma_balance_ratio": -0.033827, "gamma_positioning_proxy": "flat_proxy", "gamma_semantics": "proxy_only", "mark_rows": 1420, "oi_rows": 58, "put_call_oi_ratio": 0.9126, "put_oi_contracts": 503.38, "put_oi_usd": 39144939.6, "weighted_mark_iv": 0.381078}, "ETH": {"available": true, "call_oi_contracts": 11935.2, "call_oi_usd": 27655712.52, "expiries_used": ["260425", "260426"], "gamma_balance_proxy": 19.3032125, "gamma_balance_ratio": 0.330586, "gamma_positioning_proxy": "positive_gamma_proxy", "gamma_semantics": "proxy_only", "mark_rows": 1420, "oi_rows": 62, "put_call_oi_ratio": 0.6109, "put_oi_contracts": 7291.2, "put_oi_usd": 16894843.08, "weighted_mark_iv": 0.534446}}, "confirmed_facts": ["BTC_public_options_mark_and_oi_available", "ETH_public_options_mark_and_oi_available"], "inferences": ["BTC_gamma_positioning_proxy_is_derived_from_public_gamma_and_oi", "ETH_gamma_positioning_proxy_is_derived_from_public_gamma_and_oi"]}`

## Derivatives
- Derivatives snapshot: `{"by_symbol": {"BTCUSDT": {"basis_pct": -0.05653869989819252, "flow_bias": "position_unwind", "funding_history_points": 4, "funding_rate": -2.778e-05, "funding_trend": "flat", "global_long_short_ratio": 0.7185, "oi_change_pct": -0.006293872793757926, "oi_current": 96128.556, "taker_ratio": 0.878, "top_trader_long_short_ratio": 0.7528}, "ETHUSDT": {"basis_pct": -0.059816896972618924, "flow_bias": "sellers_in_control", "funding_history_points": 4, "funding_rate": -0.00010014, "funding_trend": "flat", "global_long_short_ratio": 2.0294, "oi_change_pct": 0.00444207918564099, "oi_current": 2059025.214, "taker_ratio": 0.8812, "top_trader_long_short_ratio": 1.6151}, "SOLUSDT": {"basis_pct": -0.057843263118889446, "flow_bias": "sellers_in_control", "funding_history_points": 10, "funding_rate": -4.081e-05, "funding_trend": "flat", "global_long_short_ratio": 2.4083, "oi_change_pct": 0.006828744891365757, "oi_current": 9311919.07, "taker_ratio": 0.7422, "top_trader_long_short_ratio": 2.5791}, "XRPUSDT": {"basis_pct": -0.053424514449552624, "flow_bias": "sellers_in_control", "funding_history_points": 4, "funding_rate": 6.254e-05, "funding_trend": "rising", "global_long_short_ratio": 2.3434, "oi_change_pct": 0.0008741581382381014, "oi_current": 272531083.7, "taker_ratio": 0.8758, "top_trader_long_short_ratio": 2.5894}}, "confirmed_facts": ["BTCUSDT_public_futures_context_available", "ETHUSDT_public_futures_context_available", "SOLUSDT_public_futures_context_available", "XRPUSDT_public_futures_context_available"]}`

## Harmonic (Debug Only)
- Harmonic snapshot: `{"by_symbol": {"BTCUSDT": {"available": true, "deviation_pct": 34.5214, "pattern": "none", "ratio_ab_cd": 0.6548, "ratio_bc_ab": 0.9416}, "ETHUSDT": {"available": true, "deviation_pct": 34.5212, "pattern": "none", "ratio_ab_cd": 0.6548, "ratio_bc_ab": 0.9067}}, "enabled": true, "mode": "debug_only_confluence", "standalone_strategy": false, "weight": "low"}`

## JSON
```json
{
  "ts": "2026-04-24T10:59:22.541566+00:00",
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
        "funding_rate": -2.778e-05,
        "oi_current": 96128.556,
        "oi_change_pct": -0.006293872793757926,
        "top_trader_long_short_ratio": 0.7528,
        "global_long_short_ratio": 0.7185,
        "taker_ratio": 0.878,
        "basis_pct": -0.05653869989819252,
        "funding_trend": "flat",
        "flow_bias": "position_unwind",
        "funding_history_points": 4
      },
      "ETHUSDT": {
        "funding_rate": -0.00010014,
        "oi_current": 2059025.214,
        "oi_change_pct": 0.00444207918564099,
        "top_trader_long_short_ratio": 1.6151,
        "global_long_short_ratio": 2.0294,
        "taker_ratio": 0.8812,
        "basis_pct": -0.059816896972618924,
        "funding_trend": "flat",
        "flow_bias": "sellers_in_control",
        "funding_history_points": 4
      },
      "SOLUSDT": {
        "funding_rate": -4.081e-05,
        "oi_current": 9311919.07,
        "oi_change_pct": 0.006828744891365757,
        "top_trader_long_short_ratio": 2.5791,
        "global_long_short_ratio": 2.4083,
        "taker_ratio": 0.7422,
        "basis_pct": -0.057843263118889446,
        "funding_trend": "flat",
        "flow_bias": "sellers_in_control",
        "funding_history_points": 10
      },
      "XRPUSDT": {
        "funding_rate": 6.254e-05,
        "oi_current": 272531083.7,
        "oi_change_pct": 0.0008741581382381014,
        "top_trader_long_short_ratio": 2.5894,
        "global_long_short_ratio": 2.3434,
        "taker_ratio": 0.8758,
        "basis_pct": -0.053424514449552624,
        "funding_trend": "rising",
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
          "260425",
          "260426"
        ],
        "call_oi_contracts": 551.6,
        "put_oi_contracts": 503.38,
        "call_oi_usd": 42894729.0,
        "put_oi_usd": 39144939.6,
        "put_call_oi_ratio": 0.9126,
        "weighted_mark_iv": 0.381078,
        "gamma_balance_proxy": -0.00453685,
        "gamma_balance_ratio": -0.033827,
        "gamma_positioning_proxy": "flat_proxy",
        "gamma_semantics": "proxy_only",
        "mark_rows": 1420,
        "oi_rows": 58
      },
      "ETH": {
        "available": true,
        "expiries_used": [
          "260425",
          "260426"
        ],
        "call_oi_contracts": 11935.2,
        "put_oi_contracts": 7291.2,
        "call_oi_usd": 27655712.52,
        "put_oi_usd": 16894843.08,
        "put_call_oi_ratio": 0.6109,
        "weighted_mark_iv": 0.534446,
        "gamma_balance_proxy": 19.3032125,
        "gamma_balance_ratio": 0.330586,
        "gamma_positioning_proxy": "positive_gamma_proxy",
        "gamma_semantics": "proxy_only",
        "mark_rows": 1420,
        "oi_rows": 62
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
    "strongest_move_pct": 0.0977
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
        "ratio_ab_cd": 0.6548,
        "ratio_bc_ab": 0.9416,
        "deviation_pct": 34.5214
      },
      "ETHUSDT": {
        "available": true,
        "pattern": "none",
        "ratio_ab_cd": 0.6548,
        "ratio_bc_ab": 0.9067,
        "deviation_pct": 34.5212
      }
    }
  }
}
```
