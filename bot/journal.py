"""Trader's journal — analytics over telemetry JSONL files.

Reads data/bot/telemetry/analysis/{selected,rejected,tracking_events}.jsonl
and returns a structured JournalReport. No writes, no network calls.

Also provides build_config_suggestions() which joins selected signals with their
outcomes (tp/sl/expired) and bins by key parameters to suggest config thresholds.
"""
from __future__ import annotations

import json
import logging
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

LOG = logging.getLogger("bot.journal")


@dataclass
class JournalReport:
    signals_sent: int = 0
    top_rejection_reasons: list[tuple[str, int]] = field(default_factory=list)
    setup_outcomes: dict[str, dict[str, int]] = field(default_factory=dict)
    # {setup_id: {event: count}} where event in tp1/tp2/sl/expired
    hourly_signal_counts: dict[int, int] = field(default_factory=dict)
    # {hour_of_day (0-23): count of signals sent}


def _iter_jsonl(path: Path):
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            yield json.loads(line)
        except json.JSONDecodeError:
            continue


def build_journal_report(telemetry_root: Path) -> JournalReport:
    """Build a JournalReport from JSONL files under telemetry_root.

    Supports both legacy layout:
      telemetry_root/analysis/*.jsonl
    and per-run layout:
      telemetry_root/runs/<run_id>/analysis/*.jsonl
    """
    analysis_dirs: list[Path] = []
    legacy_analysis = telemetry_root / "analysis"
    if legacy_analysis.exists():
        analysis_dirs.append(legacy_analysis)
    runs_dir = telemetry_root / "runs"
    if runs_dir.exists():
        for run_path in sorted(runs_dir.iterdir()):
            analysis_path = run_path / "analysis"
            if analysis_path.exists():
                analysis_dirs.append(analysis_path)
    report = JournalReport()

    # Signals sent
    hourly: Counter = Counter()
    signals_count = 0
    for analysis in analysis_dirs:
        for row in _iter_jsonl(analysis / "selected.jsonl"):
            signals_count += 1
            ts = row.get("ts", "")
            try:
                hour = int(ts[11:13])
                hourly[hour] += 1
            except (IndexError, ValueError):
                pass
    report.signals_sent = signals_count
    report.hourly_signal_counts = dict(sorted(hourly.items()))

    # Rejection reasons
    rejection_counter: Counter = Counter()
    for analysis in analysis_dirs:
        for row in _iter_jsonl(analysis / "rejected.jsonl"):
            reason = row.get("reason") or row.get("filter") or "unknown"
            rejection_counter[reason] += 1
    report.top_rejection_reasons = rejection_counter.most_common(10)

    # Tracking outcomes by setup_id
    outcomes: dict[str, Counter] = defaultdict(Counter)
    for analysis in analysis_dirs:
        for row in _iter_jsonl(analysis / "tracking_events.jsonl"):
            event = row.get("event") or row.get("type") or ""
            setup_id = row.get("setup_id") or "unknown"
            if event in ("tp1", "tp2", "sl", "expired"):
                outcomes[setup_id][event] += 1
    report.setup_outcomes = {k: dict(v) for k, v in outcomes.items()}

    return report


def _collect_analysis_dirs(telemetry_root: Path) -> list[Path]:
    dirs: list[Path] = []
    legacy = telemetry_root / "analysis"
    if legacy.exists():
        dirs.append(legacy)
    runs = telemetry_root / "runs"
    if runs.exists():
        for run_path in sorted(runs.iterdir()):
            ap = run_path / "analysis"
            if ap.exists():
                dirs.append(ap)
    return dirs


def build_config_suggestions(telemetry_root: Path) -> list[str]:
    """Analyse outcomes and suggest config parameter adjustments.

    Joins selected.jsonl signals with their tracking outcomes (tp1/tp2/sl/expired)
    using tracking_ref as the join key.  Bins by ATR%, score band, and RR band,
    then suggests thresholds where win rate diverges significantly across bins.

    Returns a list of suggestion lines ready to print.  Empty list = not enough data.
    """
    analysis_dirs = _collect_analysis_dirs(telemetry_root)

    # 1. Collect all selected signals keyed by tracking_ref
    signals: dict[str, dict[str, Any]] = {}
    for ad in analysis_dirs:
        for row in _iter_jsonl(ad / "selected.jsonl"):
            ref = row.get("tracking_ref") or row.get("signal", {}).get("tracking_ref")
            if not ref:
                continue
            sig = row.get("signal", row)
            signals[ref] = sig

    # 2. Collect terminal outcomes keyed by tracking_ref
    WIN_EVENTS = {"tp1", "tp2"}
    LOSS_EVENTS = {"sl"}
    outcomes: dict[str, str] = {}  # ref -> "win" | "loss" | "expired"
    for ad in analysis_dirs:
        for row in _iter_jsonl(ad / "tracking_events.jsonl"):
            event = row.get("event") or row.get("type") or ""
            ref = row.get("tracking_ref")
            if not ref or event not in WIN_EVENTS | LOSS_EVENTS | {"expired"}:
                continue
            if ref not in outcomes:
                if event in WIN_EVENTS:
                    outcomes[ref] = "win"
                elif event in LOSS_EVENTS:
                    outcomes[ref] = "loss"
                else:
                    outcomes[ref] = "expired"

    # Only signals with a terminal outcome (tp/sl/expired) are usable
    resolved = [(signals[r], outcomes[r]) for r in outcomes if r in signals]
    total_resolved = len(resolved)
    MIN_BIN = 5
    suggestions: list[str] = []

    if total_resolved < 10:
        suggestions.append(
            f"[SUGGEST] Not enough resolved outcomes yet ({total_resolved}). "
            "Need at least 10 to generate suggestions."
        )
        return suggestions

    suggestions.append(f"[ADVISOR] Analysed {total_resolved} resolved signals\n")

    def _win_rate(items: list[tuple[dict, str]]) -> tuple[int, int, int]:
        """Return (wins, losses, expired)."""
        w = sum(1 for _, o in items if o == "win")
        l = sum(1 for _, o in items if o == "loss")
        e = sum(1 for _, o in items if o == "expired")
        return w, l, e

    def _wr_str(w: int, l: int) -> str:
        total = w + l
        if total == 0:
            return "n/a"
        return f"{w / total * 100:.0f}%"

    # --- ATR % bins ---
    atr_bins: dict[str, list] = defaultdict(list)
    for sig, outcome in resolved:
        atr = sig.get("atr_pct") or sig.get("signal", {}).get("atr_pct")
        if atr is None:
            continue
        atr = float(atr)
        if atr < 0.5:
            atr_bins["<0.50"].append((sig, outcome))
        elif atr < 0.75:
            atr_bins["0.50-0.75"].append((sig, outcome))
        elif atr < 1.0:
            atr_bins["0.75-1.00"].append((sig, outcome))
        elif atr < 1.5:
            atr_bins["1.00-1.50"].append((sig, outcome))
        else:
            atr_bins[">1.50"].append((sig, outcome))

    atr_lines = []
    for label in ["<0.50", "0.50-0.75", "0.75-1.00", "1.00-1.50", ">1.50"]:
        items = atr_bins.get(label, [])
        if len(items) >= MIN_BIN:
            w, l, e = _win_rate(items)
            atr_lines.append(f"  ATR {label:>9}%  n={len(items):>3}  wr={_wr_str(w, l)}  (wins={w} sl={l} exp={e})")
    if len(atr_lines) >= 2:
        suggestions.append("[SUGGEST] min_atr_pct — win rate by ATR band:")
        suggestions.extend(atr_lines)
        # Find the threshold where win rate drops below 40%
        threshold_hint = None
        for label in ["<0.50", "0.50-0.75"]:
            items = atr_bins.get(label, [])
            if len(items) >= MIN_BIN:
                w, l, _ = _win_rate(items)
                total = w + l
                if total > 0 and w / total < 0.40:
                    threshold_hint = label
        if threshold_hint:
            suggestions.append(f"  → Low win rate in {threshold_hint} bin suggests raising min_atr_pct")
        suggestions.append("")

    # --- Score bins ---
    score_bins: dict[str, list] = defaultdict(list)
    for sig, outcome in resolved:
        score = sig.get("score") or sig.get("signal", {}).get("score")
        if score is None:
            continue
        score = float(score)
        if score < 0.65:
            score_bins["0.64-0.65"].append((sig, outcome))
        elif score < 0.68:
            score_bins["0.65-0.68"].append((sig, outcome))
        elif score < 0.72:
            score_bins["0.68-0.72"].append((sig, outcome))
        elif score < 0.78:
            score_bins["0.72-0.78"].append((sig, outcome))
        else:
            score_bins[">0.78"].append((sig, outcome))

    score_lines = []
    for label in ["0.64-0.65", "0.65-0.68", "0.68-0.72", "0.72-0.78", ">0.78"]:
        items = score_bins.get(label, [])
        if len(items) >= MIN_BIN:
            w, l, e = _win_rate(items)
            score_lines.append(f"  score {label:>9}  n={len(items):>3}  wr={_wr_str(w, l)}  (wins={w} sl={l} exp={e})")
    if len(score_lines) >= 2:
        suggestions.append("[SUGGEST] min_score — win rate by score band:")
        suggestions.extend(score_lines)
        # Suggest raising min_score if bottom band has poor win rate
        low_items = score_bins.get("0.64-0.65", []) + score_bins.get("0.65-0.68", [])
        high_items = score_bins.get("0.72-0.78", []) + score_bins.get(">0.78", [])
        if len(low_items) >= MIN_BIN and len(high_items) >= MIN_BIN:
            lw, ll, _ = _win_rate(low_items)
            hw, hl, _ = _win_rate(high_items)
            lt = lw + ll
            ht = hw + hl
            if lt > 0 and ht > 0 and (lw / lt) < (hw / ht) - 0.15:
                suggestions.append(f"  → Clear quality gap (low={_wr_str(lw, ll)} vs high={_wr_str(hw, hl)}) suggests raising min_score to 0.68+")
        suggestions.append("")

    # --- RR bins ---
    rr_bins: dict[str, list] = defaultdict(list)
    for sig, outcome in resolved:
        rr = sig.get("risk_reward") or sig.get("signal", {}).get("risk_reward")
        if rr is None:
            continue
        rr = float(rr)
        if rr < 2.5:
            rr_bins["1.9-2.5"].append((sig, outcome))
        elif rr < 3.5:
            rr_bins["2.5-3.5"].append((sig, outcome))
        elif rr < 5.0:
            rr_bins["3.5-5.0"].append((sig, outcome))
        else:
            rr_bins[">5.0"].append((sig, outcome))

    rr_lines = []
    for label in ["1.9-2.5", "2.5-3.5", "3.5-5.0", ">5.0"]:
        items = rr_bins.get(label, [])
        if len(items) >= MIN_BIN:
            w, l, e = _win_rate(items)
            rr_lines.append(f"  RR {label:>9}  n={len(items):>3}  wr={_wr_str(w, l)}  (wins={w} sl={l} exp={e})")
    if len(rr_lines) >= 2:
        suggestions.append("[SUGGEST] min_risk_reward — win rate by RR band:")
        suggestions.extend(rr_lines)
        suggestions.append("")

    # --- Setup performance with regime context ---
    setup_regime_bins: dict[str, list] = defaultdict(list)
    for sig, outcome in resolved:
        setup_id = sig.get("setup_id") or sig.get("signal", {}).get("setup_id") or "unknown"
        regime = sig.get("bias_4h") or sig.get("signal", {}).get("bias_4h") or "neutral"
        key = f"{setup_id} / {regime}"
        setup_regime_bins[key].append((sig, outcome))

    setup_lines = []
    for key, items in sorted(setup_regime_bins.items(), key=lambda x: -len(x[1])):
        if len(items) >= MIN_BIN:
            w, l, e = _win_rate(items)
            total = w + l
            wr_val = w / total if total > 0 else None
            flag = " ← LOW" if wr_val is not None and wr_val < 0.35 else ""
            setup_lines.append(f"  {key:<35}  n={len(items):>3}  wr={_wr_str(w, l)}{flag}")
    if setup_lines:
        suggestions.append("[SUGGEST] Setup performance by regime (low wr = consider disabling):")
        suggestions.extend(setup_lines)
        suggestions.append("")

    if len(suggestions) <= 2:
        suggestions.append("[ADVISOR] Bins too small for suggestions — keep accumulating outcomes.")

    return suggestions


def print_journal_report(report: JournalReport) -> None:
    """Print a human-readable journal report to stdout."""
    print("=" * 60)
    print("TRADER'S JOURNAL")
    print("=" * 60)
    print(f"\nSignals sent: {report.signals_sent}")

    print("\nTop rejection reasons:")
    if not report.top_rejection_reasons:
        print("  (no data)")
    for reason, count in report.top_rejection_reasons:
        print(f"  {reason:<35} {count:>5}")

    print("\nSetup outcomes (tp1 / tp2 / sl / expired):")
    if not report.setup_outcomes:
        print("  (no tracking data yet)")
    for setup_id, events in sorted(report.setup_outcomes.items()):
        tp1 = events.get("tp1", 0)
        tp2 = events.get("tp2", 0)
        sl = events.get("sl", 0)
        expired = events.get("expired", 0)
        total = tp1 + tp2 + sl
        wr = f"{(tp1 + tp2) / total * 100:.0f}%" if total > 0 else "n/a"
        print(f"  {setup_id:<30} tp1={tp1} tp2={tp2} sl={sl} exp={expired} wr={wr}")

    print("\nSignals by hour of day (UTC):")
    if report.hourly_signal_counts:
        for hour in range(24):
            count = report.hourly_signal_counts.get(hour, 0)
            bar = "#" * count
            print(f"  {hour:02d}h  {count:>3}  {bar}")
    else:
        print("  (no data)")
    print()

