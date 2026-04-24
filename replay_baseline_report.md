# Replay Baseline Report

Historical telemetry evidence only; this is not a post-change replay harness.

## 20260421_234355_31268
- tracking events: 7
- rejected events: 189274
- top reject reasons: no_raw_hit=174119, 5m_opposes_long=3130, atr_too_low=2727, 1h_alignment_opposes_short=2035, score_too_low=1972
- invariant violations: 0
- equal-target dual-close violations: 0

## 20260422_230643_23708
- tracking events: 6
- rejected events: 51268
- top reject reasons: no_raw_hit=47915, 5m_opposes_long=1582, 1h_alignment_opposes_short=513, risk_reward_too_low=358, atr_too_low=238
- invariant violations: 1
- equal-target dual-close violations: 1
- sample invariant violations:
  - HYPEUSDT long: stop=40.68290871 entry=41.09420690 tp1=43.17900000 tp2=42.35400000
- sample equal-target lifecycle violations:
  - FETUSDT short: equal targets emitted tp1_hit and tp2_hit

## Current-Code Interpretation
- Historical runs still contain pre-fix target-integrity defects.
- Current regression coverage in tests/test_remediation_regressions.py exercises swapped-target normalization and single-target close semantics.
