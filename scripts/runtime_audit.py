"""
Deep Runtime Audit - Live analysis of bot signal generation pipeline.
Tracks: strategy hits, filter rejections, data preparation, setup performance.
"""
import argparse
import json
import sqlite3
import subprocess
import time
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

TELEMETRY_DIR = Path("data/bot/telemetry")
DB_PATH = Path("data/bot/bot.db")
PID_FILE = Path("data/bot/bot.pid")


def print_header(title: str) -> None:
    print(f"\n{'='*80}")
    print(f"  {title}")
    print(f"{'='*80}\n")


def _file_has_rows(path: Path) -> bool:
    if not path.exists():
        return False
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    return True
    except OSError:
        return False
    return False


def get_latest_run_dir(run_id: str | None = None) -> Path | None:
    """Find latest non-empty telemetry run directory, or use an explicit run id."""
    runs_dir = TELEMETRY_DIR / "runs"
    if not runs_dir.exists():
        return None
    if run_id:
        candidate = runs_dir / run_id
        return candidate if candidate.exists() else None
    run_dirs = sorted(runs_dir.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True)
    interesting_files = (
        "strategy_decisions.jsonl",
        "symbol_analysis.jsonl",
        "rejected.jsonl",
        "cycles.jsonl",
    )
    fallback: Path | None = None
    for run_dir in run_dirs:
        if not run_dir.is_dir():
            continue
        analysis_dir = run_dir / "analysis"
        if not analysis_dir.exists():
            continue
        fallback = fallback or run_dir
        if any(_file_has_rows(analysis_dir / filename) for filename in interesting_files):
            return run_dir
    return fallback


def analyze_rejection_funnel(run_dir: Path) -> dict:
    """Analyze rejection reasons by stage and setup"""
    rejected_file = run_dir / "analysis" / "rejected.jsonl"
    if not rejected_file.exists():
        return {"error": "No rejected.jsonl found"}
    
    stats = {
        "by_stage": Counter(),
        "by_setup": Counter(),
        "by_reason": Counter(),
        "by_symbol_setup": Counter(),
        "detailed": defaultdict(list)
    }
    
    try:
        with open(rejected_file, 'r', encoding='utf-8') as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    row = json.loads(line)
                    stage = row.get("stage", "unknown")
                    setup = row.get("setup_id", "unknown")
                    reason = row.get("reason", "unknown")
                    symbol = row.get("symbol", "unknown")
                    
                    stats["by_stage"][stage] += 1
                    stats["by_setup"][setup] += 1
                    stats["by_reason"][reason] += 1
                    stats["by_symbol_setup"][f"{symbol}:{setup}"] += 1
                    
                    # Store sample for each reason (max 3 per reason)
                    if len(stats["detailed"][reason]) < 3:
                        sample = {
                            "symbol": symbol,
                            "setup": setup,
                            "stage": stage
                        }
                        # Add optional fields if present
                        if "adx_1h" in row:
                            sample["adx_1h"] = row["adx_1h"]
                        if "risk_reward" in row:
                            sample["rr"] = row["risk_reward"]
                        if "trend_direction" in row:
                            sample["trend"] = row["trend_direction"]
                        if "details" in row:
                            sample["details"] = row["details"]
                        stats["detailed"][reason].append(sample)
                except json.JSONDecodeError:
                    continue
    except Exception as e:
        return {"error": str(e)}
    
    return stats


def analyze_symbol_funnel(run_dir: Path) -> dict:
    """Analyze symbol_analysis.jsonl for true funnel metrics"""
    analysis_file = run_dir / "analysis" / "symbol_analysis.jsonl"
    if not analysis_file.exists():
        return {"error": "No symbol_analysis.jsonl found"}
    
    stats = {
        "symbols_processed": 0,
        "symbols_with_raw_hits": 0,
        "total_raw_hits": 0,
        "total_candidates": 0,
        "total_delivered": 0,
        "rejection_reasons": Counter()
    }
    
    try:
        with open(analysis_file, 'r', encoding='utf-8') as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    data = json.loads(line)
                    funnel = data.get("funnel", {})
                    
                    stats["symbols_processed"] += 1
                    raw_hits = funnel.get("raw_hits", 0)
                    candidates = data.get("candidates", 0)
                    
                    if raw_hits > 0:
                        stats["symbols_with_raw_hits"] += 1
                        stats["total_raw_hits"] += raw_hits
                    
                    stats["total_candidates"] += candidates
                    stats["total_delivered"] += data.get("delivered", 0)
                    
                    # Count rejection reasons when raw hits exist but no candidates
                    if raw_hits > 0 and candidates == 0:
                        for key, val in funnel.items():
                            if "rejects" in key and isinstance(val, int) and val > 0:
                                stats["rejection_reasons"][key] += val
                            elif key == "alignment_penalties" and isinstance(val, int) and val > 0:
                                stats["rejection_reasons"]["alignment_penalties"] += val
                except json.JSONDecodeError:
                    continue
    except Exception as e:
        return {"error": str(e)}
    
    return stats


def analyze_strategy_decisions(run_dir: Path) -> dict:
    """Analyze structured strategy decisions for concrete reject reasons and zero-hit setups."""
    decisions_file = run_dir / "analysis" / "strategy_decisions.jsonl"
    if not decisions_file.exists():
        return {"error": "No strategy_decisions.jsonl found"}

    stats = {
        "by_status": Counter(),
        "by_stage": Counter(),
        "by_setup": Counter(),
        "by_reason": Counter(),
        "signal_hits_by_setup": Counter(),
        "rejects_by_setup_reason": Counter(),
        "runtime_failures": Counter(),
        "samples": defaultdict(list),
        "zero_hit_setups": [],
    }

    try:
        with decisions_file.open("r", encoding="utf-8") as handle:
            for line in handle:
                raw = line.strip()
                if not raw:
                    continue
                try:
                    row = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                setup = str(row.get("setup_id") or "unknown")
                status = str(row.get("status") or "unknown")
                stage = str(row.get("stage") or "unknown")
                reason = str(row.get("reason_code") or row.get("reason") or "unknown")
                stats["by_status"][status] += 1
                stats["by_stage"][stage] += 1
                stats["by_setup"][setup] += 1
                stats["by_reason"][reason] += 1
                if status == "signal":
                    stats["signal_hits_by_setup"][setup] += 1
                else:
                    stats["rejects_by_setup_reason"][(setup, stage, reason)] += 1
                    if reason.startswith("runtime."):
                        stats["runtime_failures"][reason] += 1
                    if len(stats["samples"][reason]) < 3:
                        stats["samples"][reason].append(
                            {
                                "symbol": row.get("symbol"),
                                "setup": setup,
                                "status": status,
                                "stage": stage,
                                "details": row.get("details"),
                            }
                        )
    except Exception as exc:
        return {"error": str(exc)}

    stats["zero_hit_setups"] = sorted(
        setup for setup in stats["by_setup"]
        if stats["signal_hits_by_setup"].get(setup, 0) == 0
    )
    return stats


def analyze_data_quality(run_dir: Path) -> dict:
    """Analyze strict data-quality violations emitted by the runtime."""
    path = run_dir / "analysis" / "data_quality.jsonl"
    if not path.exists():
        return {"error": "No data_quality.jsonl found"}

    stats = {
        "rows": 0,
        "by_setup": Counter(),
        "missing_fields": Counter(),
        "invalid_fields": Counter(),
        "strict_none_violations": 0,
    }
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                raw = line.strip()
                if not raw:
                    continue
                try:
                    row = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                stats["rows"] += 1
                setup = str(row.get("setup_id") or "unknown")
                stats["by_setup"][setup] += 1
                missing_fields = row.get("missing_fields") if isinstance(row.get("missing_fields"), list) else []
                invalid_fields = row.get("invalid_fields") if isinstance(row.get("invalid_fields"), list) else []
                if missing_fields or invalid_fields:
                    stats["strict_none_violations"] += 1
                stats["missing_fields"].update(str(field) for field in missing_fields)
                stats["invalid_fields"].update(str(field) for field in invalid_fields)
    except Exception as exc:
        return {"error": str(exc)}
    return stats


def analyze_null_fields(run_dir: Path) -> dict:
    """Analyze rejected.jsonl to find all fields that are None/null"""
    rejected_file = run_dir / "analysis" / "rejected.jsonl"
    if not rejected_file.exists():
        print("  No rejected.jsonl found")
        return {}
    
    null_stats = defaultdict(lambda: {"null_count": 0, "total_count": 0, "samples": []})
    fields_to_check = [
        "mark_index_spread_bps",
        "premium_zscore_5m", 
        "premium_slope_5m",
        "depth_imbalance",
        "microprice_bias",
        "basis_pct",
        "funding_rate",
        "oi_change_pct",
        "ls_ratio",
        "adx_1h",
        "risk_reward",
        "trend_direction"
    ]
    
    try:
        with open(rejected_file, 'r', encoding='utf-8') as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    row = json.loads(line)
                    for field in fields_to_check:
                        value = row.get(field)
                        null_stats[field]["total_count"] += 1
                        if value is None:
                            null_stats[field]["null_count"] += 1
                            if len(null_stats[field]["samples"]) < 3:
                                null_stats[field]["samples"].append({
                                    "symbol": row.get("symbol", "unknown"),
                                    "setup": row.get("strategy_id", row.get("setup_id", "unknown")),
                                    "reason": row.get("reason", "unknown")
                                })
                except json.JSONDecodeError:
                    continue
        
        # Print summary
        print("  Field Null Analysis (from rejected.jsonl):")
        print(f"  {'Field':<25} {'Null %':>8} {'Null/Total':>15}")
        print("  " + "-" * 55)
        
        for field in fields_to_check:
            stats = null_stats[field]
            if stats["total_count"] > 0:
                null_pct = stats["null_count"] / stats["total_count"] * 100
                ratio = f"{stats['null_count']}/{stats['total_count']}"
                print(f"  {field:<25} {null_pct:>7.1f}% {ratio:>15}")
                
                # Show samples if > 50% null
                if null_pct > 50 and stats["samples"]:
                    print(f"    -> Samples:", end="")
                    for s in stats["samples"][:2]:
                        print(f" {s['symbol']}/{s['setup']}", end="")
                    print()
        
        # Find fields that are 100% null (potential bugs)
        all_null = [f for f in fields_to_check if null_stats[f]["total_count"] > 0 and null_stats[f]["null_count"] == null_stats[f]["total_count"]]
        if all_null:
            print(f"\n  [CRITICAL] Fields always NULL: {', '.join(all_null)}")
            print("  -> These fields may indicate data pipeline bugs!")
            
    except Exception as e:
        print(f"  ERROR analyzing null fields: {e}")
    
    return dict(null_stats)


def analyze_cycles(run_dir: Path) -> dict:
    """Analyze cycle logs - includes global health and symbol-level records"""
    cycles_file = run_dir / "analysis" / "cycles.jsonl"
    if not cycles_file.exists():
        return {"error": "No cycles.jsonl found"}
    
    stats = {
        "total_cycles": 0,
        "symbols_analyzed": set(),
        "total_detector_runs": 0,
        "total_candidates": 0,
        "total_delivered": 0,
        "health_checks": 0,
        "by_symbol": defaultdict(lambda: {
            "cycles": 0, "detectors": 0, "candidates": 0, "delivered": 0
        })
    }
    
    try:
        with open(cycles_file, 'r', encoding='utf-8') as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    row = json.loads(line)
                    symbol = row.get("symbol")
                    
                    # Symbol-level cycle analysis
                    if symbol:
                        stats["total_cycles"] += 1
                        stats["symbols_analyzed"].add(symbol)
                        stats["total_detector_runs"] += row.get("detector_runs", 0)
                        stats["total_candidates"] += row.get("candidates", 0)
                        stats["total_delivered"] += row.get("delivered", 0)
                        
                        s = stats["by_symbol"][symbol]
                        s["cycles"] += 1
                        s["detectors"] += row.get("detector_runs", 0)
                        s["candidates"] += row.get("candidates", 0)
                        s["delivered"] += row.get("delivered", 0)
                    else:
                        # Global health check record
                        stats["health_checks"] += 1
                        # Extract funnel metrics if present
                        funnel = row.get("funnel", {})
                        if funnel:
                            stats["total_detector_runs"] += funnel.get("detector_runs", 0)
                            stats["total_candidates"] += funnel.get("post_filter_candidates", 0)
                            stats["total_delivered"] += funnel.get("delivered", 0)
                except json.JSONDecodeError:
                    continue
    except Exception as e:
        return {"error": str(e)}
    
    return stats


def analyze_database() -> dict:
    """Query SQLite for signal and rejection data"""
    if not DB_PATH.exists():
        return {"error": "Database not found"}
    
    stats = {}
    try:
        conn = sqlite3.connect(str(DB_PATH))
        cursor = conn.cursor()
        
        # Check all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        stats["tables"] = tables
        
        # Signals table
        if "signals" in tables:
            cursor.execute("SELECT strategy_id, direction, COUNT(*) FROM signals GROUP BY strategy_id, direction")
            stats["signals_by_setup"] = cursor.fetchall()
            cursor.execute("SELECT COUNT(*) FROM active_signals WHERE status='active'")
            stats["active_signals"] = cursor.fetchone()[0]
        
        # Rejected table (if exists)
        if "rejected" in tables:
            cursor.execute("SELECT stage, reason, COUNT(*) FROM rejected GROUP BY stage, reason ORDER BY COUNT(*) DESC")
            stats["rejections_by_stage_reason"] = cursor.fetchall()
        
        # Cooldowns
        if "cooldowns" in tables:
            cursor.execute("SELECT COUNT(*) FROM cooldowns")
            stats["active_cooldowns"] = cursor.fetchone()[0]
        
        conn.close()
    except Exception as e:
        return {"error": str(e)}
    
    return stats


def check_bot_process() -> dict:
    """Check if bot is running"""
    if not PID_FILE.exists():
        return {"running": False, "error": "No PID file"}
    
    try:
        pid = int(PID_FILE.read_text().strip())
        result = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV"],
            capture_output=True, text=True
        )
        is_running = str(pid) in result.stdout
        return {"running": is_running, "pid": pid}
    except Exception as e:
        return {"running": False, "error": str(e)}


def print_audit_report(
    rejection_stats: dict,
    cycle_stats: dict,
    db_stats: dict,
    process: dict,
    funnel_stats: dict,
    decision_stats: dict,
    data_quality_stats: dict,
    run_dir: Path,
):
    """Print comprehensive audit report"""
    print_header("RUNTIME AUDIT REPORT")
    print(f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"Bot Status: {'RUNNING' if process.get('running') else 'STOPPED'} (PID: {process.get('pid')})")
    
    # TRUE FUNNEL ANALYSIS (from symbol_analysis.jsonl)
    print_header("TRUE SIGNAL FUNNEL (from symbol_analysis.jsonl)")
    if "error" in funnel_stats:
        print(f"ERROR: {funnel_stats['error']}")
    else:
        print(f"Symbols Processed: {funnel_stats.get('symbols_processed', 0)}")
        print(f"Symbols with Raw Hits: {funnel_stats.get('symbols_with_raw_hits', 0)}")
        print(f"Total Raw Hits (detectors found): {funnel_stats.get('total_raw_hits', 0)}")
        print(f"Total Candidates (after filters): {funnel_stats.get('total_candidates', 0)}")
        print(f"Total Delivered: {funnel_stats.get('total_delivered', 0)}")
        
        if funnel_stats.get('total_raw_hits', 0) > 0:
            conversion = funnel_stats.get('total_candidates', 0) / funnel_stats.get('total_raw_hits', 1) * 100
            print(f"\nConversion: {conversion:.2f}% (raw hits to candidates)")
        
        print("\n--- Rejection Reasons (when raw hits exist) ---")
        for reason, count in funnel_stats.get("rejection_reasons", Counter()).most_common():
            print(f"  {reason}: {count}")
    
    # Cycle Summary
    print_header("CYCLE PERFORMANCE")
    if "error" in cycle_stats:
        print(f"ERROR: {cycle_stats['error']}")
    else:
        print(f"Health Checks: {cycle_stats.get('health_checks', 0)}")
        print(f"Symbol Cycles: {cycle_stats.get('total_cycles', 0)}")
        print(f"Symbols Analyzed: {len(cycle_stats.get('symbols_analyzed', set()))}")
        print(f"Total Detector Runs: {cycle_stats.get('total_detector_runs', 0)}")
        print(f"Total Candidates: {cycle_stats.get('total_candidates', 0)}")
        print(f"Total Delivered: {cycle_stats.get('total_delivered', 0)}")
        
        if cycle_stats.get('total_detector_runs', 0) > 0:
            hit_rate = cycle_stats.get('total_candidates', 0) / cycle_stats.get('total_detector_runs', 1) * 100
            print(f"\nHit Rate: {hit_rate:.2f}% (candidates / detector_runs)")
        
        # Top symbols by detector runs
        print("\n--- Top 10 Symbols by Activity ---")
        by_sym = sorted(
            cycle_stats.get('by_symbol', {}).items(),
            key=lambda x: x[1]['detectors'],
            reverse=True
        )[:10]
        for sym, data in by_sym:
            print(f"  {sym}: {data['cycles']} cycles, {data['detectors']} detectors, {data['candidates']} candidates, {data['delivered']} delivered")
    
    # Rejection Analysis
    print_header("REJECTION ANALYSIS (Why no signals?)")
    if "error" in rejection_stats:
        print(f"ERROR: {rejection_stats['error']}")
    else:
        print("\n--- By Stage (where in pipeline?) ---")
        for stage, count in rejection_stats.get("by_stage", Counter()).most_common():
            print(f"  {stage}: {count}")
        
        print("\n--- By Reason (why rejected?) ---")
        for reason, count in rejection_stats.get("by_reason", Counter()).most_common(10):
            print(f"  {reason}: {count}")
            # Show samples
            samples = rejection_stats.get("detailed", {}).get(reason, [])
            for s in samples[:2]:
                details = f"{s['symbol']} | {s['setup']} | stage={s['stage']}"
                if "adx_1h" in s:
                    details += f" | adx={s['adx_1h']:.1f}" if s['adx_1h'] else " | adx=N/A"
                if "rr" in s:
                    details += f" | rr={s['rr']:.2f}" if s['rr'] else " | rr=N/A"
                if "details" in s:
                    details += f" | details={s['details']}"
                print(f"    -> {details}")
        
        print("\n--- By Setup (which strategies fail?) ---")
        for setup, count in rejection_stats.get("by_setup", Counter()).most_common():
            print(f"  {setup}: {count} rejections")

    print_header("STRATEGY DECISIONS")
    if "error" in decision_stats:
        print(f"ERROR: {decision_stats['error']}")
    else:
        print("\n--- By Status ---")
        for status, count in decision_stats.get("by_status", Counter()).most_common():
            print(f"  {status}: {count}")

        print("\n--- Zero-Hit Setups ---")
        zero_hit_setups = decision_stats.get("zero_hit_setups", [])
        if zero_hit_setups:
            for setup in zero_hit_setups:
                print(f"  {setup}")
        else:
            print("  none")

        print("\n--- Top Setup / Stage / Reason ---")
        for (setup, stage, reason), count in decision_stats.get("rejects_by_setup_reason", Counter()).most_common(15):
            print(f"  {setup} | {stage} | {reason}: {count}")

        if decision_stats.get("runtime_failures"):
            print("\n--- Helper / Runtime Failures ---")
            for reason, count in decision_stats["runtime_failures"].most_common():
                print(f"  {reason}: {count}")

    print_header("DATA QUALITY")
    if "error" in data_quality_stats:
        print(f"ERROR: {data_quality_stats['error']}")
    else:
        print(f"Rows: {data_quality_stats.get('rows', 0)}")
        print(f"Strict-None Violations: {data_quality_stats.get('strict_none_violations', 0)}")

        print("\n--- Missing Fields ---")
        for field, count in data_quality_stats.get("missing_fields", Counter()).most_common(10):
            print(f"  {field}: {count}")

        print("\n--- Invalid Fields ---")
        for field, count in data_quality_stats.get("invalid_fields", Counter()).most_common(10):
            print(f"  {field}: {count}")
    
    # Database Summary
    print_header("DATABASE STATE")
    if "error" in db_stats:
        print(f"ERROR: {db_stats['error']}")
    else:
        print(f"Tables: {', '.join(db_stats.get('tables', []))}")
        print(f"Active Signals: {db_stats.get('active_signals', 'N/A')}")
        print(f"Active Cooldowns: {db_stats.get('active_cooldowns', 'N/A')}")
        
        if db_stats.get("signals_by_setup"):
            print("\n--- Signals by Strategy ---")
            for strategy, direction, count in db_stats["signals_by_setup"]:
                print(f"  {strategy} ({direction}): {count}")
        
        if db_stats.get("rejections_by_stage_reason"):
            print("\n--- DB Rejection Summary ---")
            for stage, reason, count in db_stats["rejections_by_stage_reason"][:10]:
                print(f"  {stage}/{reason}: {count}")
    
    # NULL FIELD ANALYSIS
    print_header("NULL FIELD ANALYSIS")
    analyze_null_fields(run_dir)
    
    # Diagnostics
    print_header("DIAGNOSTICS")
    total_rejections = sum(rejection_stats.get("by_reason", Counter()).values())
    if total_rejections > 0:
        no_raw_hit_pct = rejection_stats.get("by_reason", Counter()).get("no_raw_hit", 0) / total_rejections * 100
        if no_raw_hit_pct > 90:
            print(f"[WARNING] {no_raw_hit_pct:.1f}% rejections are 'no_raw_hit' - detectors not finding patterns")
            print("  -> Check strategy thresholds in config.toml [bot.filters.setups]")
        else:
            print(f"[OK] Rejection distribution looks healthy ({no_raw_hit_pct:.1f}% no_raw_hit)")
    else:
        print("[WARNING] No rejection data available")
    
    print(f"\n{'='*80}\n")


def main():
    parser = argparse.ArgumentParser(description="Audit persisted bot runtime telemetry")
    parser.add_argument("--run", default=None, help="explicit telemetry run id under data/bot/telemetry/runs")
    parser.add_argument("--watch", action="store_true", help="rerun audit every 30 seconds")
    args = parser.parse_args()

    print_header("BOT RUNTIME AUDIT - Starting")
    
    # Check bot status
    process = check_bot_process()
    if not process.get("running"):
        print(f"[WARNING] Bot not running: {process.get('error')}")
        print("Audit will analyze existing data...\n")
    else:
        print(f"[OK] Bot running with PID {process['pid']}")
    
    # Get latest telemetry
    run_dir = get_latest_run_dir(args.run)
    if not run_dir:
        print("[ERROR] No telemetry run directory found")
        print(f"Expected: {TELEMETRY_DIR}/runs/")
        sys.exit(1)
    
    print(f"[OK] Analyzing telemetry: {run_dir.name}")
    
    # Collect all data
    print("\nCollecting rejection data...")
    rejection_stats = analyze_rejection_funnel(run_dir)
    
    print("Collecting cycle data...")
    cycle_stats = analyze_cycles(run_dir)
    
    print("Collecting symbol funnel data...")
    funnel_stats = analyze_symbol_funnel(run_dir)

    print("Collecting strategy decision data...")
    decision_stats = analyze_strategy_decisions(run_dir)

    print("Collecting data quality data...")
    data_quality_stats = analyze_data_quality(run_dir)
    
    print("Collecting database data...")
    db_stats = analyze_database()
    
    # Print report
    print_audit_report(
        rejection_stats,
        cycle_stats,
        db_stats,
        process,
        funnel_stats,
        decision_stats,
        data_quality_stats,
        run_dir,
    )
    
    # Optional: continuous monitoring mode
    if args.watch:
        print("Entering watch mode (Ctrl+C to exit)...\n")
        try:
            while True:
                time.sleep(30)
                print(f"\n--- Update at {datetime.now(timezone.utc).strftime('%H:%M:%S')} ---")
                run_dir = get_latest_run_dir(args.run) or run_dir
                rejection_stats = analyze_rejection_funnel(run_dir)
                cycle_stats = analyze_cycles(run_dir)
                funnel_stats = analyze_symbol_funnel(run_dir)
                decision_stats = analyze_strategy_decisions(run_dir)
                data_quality_stats = analyze_data_quality(run_dir)
                db_stats = analyze_database()
                print_audit_report(
                    rejection_stats,
                    cycle_stats,
                    db_stats,
                    process,
                    funnel_stats,
                    decision_stats,
                    data_quality_stats,
                    run_dir,
                )
        except KeyboardInterrupt:
            print("\nExiting watch mode.")


if __name__ == "__main__":
    main()
