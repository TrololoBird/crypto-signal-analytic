"""
Runtime monitoring script for 30-minute bot test.
Tracks: process status, WebSocket events, telemetry, database, logs.
"""
import asyncio
import json
import sqlite3
import sys
import subprocess
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

RUNTIME_MINUTES = 30
CHECK_INTERVAL_SECONDS = 30

TELEMETRY_DIR = Path("data/bot/telemetry")
DB_PATH = Path("data/bot/bot.db")
PID_FILE = Path("data/bot/bot.pid")

def print_section(title: str) -> None:
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}\n")

def wait_for_bot():
    """Wait for user to start the bot manually"""
    print("=" * 70)
    print("INSTRUCTION: Start bot manually in another terminal:")
    print("  python main.py")
    print("=" * 70)
    print(f"\nWaiting for bot to appear at {PID_FILE}...")
    
    for i in range(120):  # Wait up to 2 minutes
        time.sleep(1)
        if PID_FILE.exists():
            try:
                pid = int(PID_FILE.read_text().strip())
                # Verify process exists
                result = subprocess.run(
                    ["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV"],
                    capture_output=True,
                    text=True
                )
                if str(pid) in result.stdout:
                    print(f"[OK] Bot detected with PID: {pid}")
                    return True
            except Exception:
                pass
        if i % 10 == 0:
            print(f"  ... still waiting ({i}s)")
    
    print("[FAIL] Bot did not start within 2 minutes")
    return False

def check_process_status():
    """Check if bot process is running"""
    if not PID_FILE.exists():
        return {"running": False, "pid": None, "error": "PID file not found"}
    
    try:
        pid = int(PID_FILE.read_text().strip())
        
        # Windows: check process via tasklist
        result = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV"],
            capture_output=True,
            text=True
        )
        
        if str(pid) in result.stdout:
            return {"running": True, "pid": pid}
        else:
            return {"running": False, "pid": pid, "error": "Process not found in tasklist"}
    except Exception as e:
        return {"running": False, "pid": None, "error": str(e)}

def check_telemetry():
    """Analyze telemetry files"""
    if not TELEMETRY_DIR.exists():
        return {"error": "Telemetry directory not found"}
    
    stats = {"files": {}, "recent_entries": []}
    
    # Search recursively for all jsonl files (they may be in runs/{run_id}/analysis/)
    for jsonl_file in TELEMETRY_DIR.rglob("*.jsonl"):
        try:
            content = jsonl_file.read_text(encoding='utf-8').strip()
            lines = [l for l in content.split('\n') if l.strip()]
            stats["files"][jsonl_file.name] = len(lines)
            
            # Last 3 entries from each file
            for line in lines[-3:]:
                try:
                    entry = json.loads(line)
                    stats["recent_entries"].append({
                        "file": jsonl_file.name,
                        "data": entry
                    })
                except json.JSONDecodeError:
                    pass
        except Exception as e:
            stats["files"][jsonl_file.name] = f"error: {e}"
    
    return stats

def check_database():
    """Check SQLite database"""
    if not DB_PATH.exists():
        return {"error": "Database not found"}
    
    try:
        conn = sqlite3.connect(str(DB_PATH))
        cursor = conn.cursor()
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        
        stats = {
            "tables": {},
            "total_size_mb": DB_PATH.stat().st_size / (1024*1024)
        }
        
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                stats["tables"][table] = count
            except:
                stats["tables"][table] = "error"
        
        conn.close()
        return stats
    except Exception as e:
        return {"error": str(e)}

LOGS_DIR = Path("data/bot/logs")

def check_logs():
    """Check log files for key events"""
    log_files = sorted(LOGS_DIR.glob("bot_*.log"), key=lambda p: p.stat().st_mtime, reverse=True)
    
    if not log_files:
        return {"error": "No log files found"}
    
    latest_log = log_files[0]
    
    try:
        content = latest_log.read_text(encoding='utf-8', errors='ignore')
        lines = content.split('\n')
        
        key_events = []
        error_count = 0
        warning_count = 0
        kline_count = 0
        signal_count = 0
        
        for line in lines:
            if 'ERROR' in line:
                error_count += 1
            elif 'WARNING' in line:
                warning_count += 1
            
            if 'kline_close received' in line.lower():
                kline_count += 1
            if any(k in line.lower() for k in ['signal generated', 'candidate signal', 'delivered signal']):
                signal_count += 1
            
            # Capture important events
            if any(k in line for k in [
                'BOT SESSION STARTED', 'ws connected', 'kline_close received',
                'strategies registered', 'modern repository', 'signal generated',
                'candidate signal', 'ERROR', 'crashed', 'failed'
            ]):
                if len(key_events) < 50:
                    key_events.append(line.strip()[:200])
        
        return {
            "file": str(latest_log),
            "total_lines": len(lines),
            "errors": error_count,
            "warnings": warning_count,
            "kline_events": kline_count,
            "signal_events": signal_count,
            "key_events": key_events[-15:]  # Last 15
        }
    except Exception as e:
        return {"error": str(e)}

async def monitor_loop():
    """Main monitoring loop"""
    start_time = datetime.now(timezone.utc)
    end_time = start_time + timedelta(minutes=RUNTIME_MINUTES)
    
    print_section(f"BOT RUNTIME MONITORING | Start: {start_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"Planned runtime: {RUNTIME_MINUTES} minutes")
    print(f"Check interval: {CHECK_INTERVAL_SECONDS} seconds")
    print(f"Expected end: {end_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"\nPID file: {PID_FILE}")
    print(f"Telemetry dir: {TELEMETRY_DIR}")
    print(f"Database: {DB_PATH}")
    
    # Wait for user to start bot
    if not wait_for_bot():
        print_section("BOT NOT DETECTED")
        return
    
    # Give bot time to initialize
    print("Giving bot 5 seconds to initialize...")
    time.sleep(5)
    
    iteration = 0
    check_count = (RUNTIME_MINUTES * 60) // CHECK_INTERVAL_SECONDS
    
    all_kline_events = 0
    all_signal_events = 0
    all_errors = 0
    
    while iteration < check_count:
        iteration += 1
        elapsed = iteration * CHECK_INTERVAL_SECONDS
        remaining = (check_count - iteration) * CHECK_INTERVAL_SECONDS
        
        print(f"\n{'-'*70}")
        print(f"Check {iteration}/{check_count} | Elapsed: {elapsed//60}m{elapsed%60}s | Remaining: {remaining//60}m{remaining%60}s")
        print(f"{'-'*70}")
        
        # 1. Process status
        status = check_process_status()
        if status["running"]:
            print(f"[OK PROCESS] PID {status['pid']} - RUNNING")
        else:
            print(f"[FAIL PROCESS] STOPPED - {status.get('error', 'unknown error')}")
            print_section("CRITICAL: Bot has stopped!")
            break
        
        # 2. Logs analysis
        logs = check_logs()
        if "error" not in logs:
            all_kline_events = logs['kline_events']
            all_signal_events = logs['signal_events']
            all_errors = logs['errors']
            
            print(f"[LOGS] File: {Path(logs['file']).name}")
            print(f"[LOGS] Lines: {logs['total_lines']}, Errors: {logs['errors']}, Warnings: {logs['warnings']}")
            print(f"[LOGS] Kline events: {logs['kline_events']}, Signal events: {logs['signal_events']}")
            
            if logs.get('key_events'):
                print("[LOGS] Recent key events:")
                for line in logs['key_events'][-5:]:
                    print(f"  -> {line[:120]}")
        else:
            print(f"[LOGS] Error: {logs['error']}")
        
        # 3. Telemetry
        telemetry = check_telemetry()
        if "error" not in telemetry:
            print(f"[TELEMETRY] Files: {len(telemetry['files'])}")
            for fname, count in list(telemetry['files'].items())[:5]:
                print(f"  -> {fname}: {count} entries")
        else:
            print(f"[TELEMETRY] {telemetry['error']}")
        
        # 4. Database
        db_stats = check_database()
        if "error" not in db_stats:
            print(f"[DATABASE] Size: {db_stats['total_size_mb']:.2f} MB")
            table_summary = ', '.join([f"{k}={v}" for k, v in list(db_stats['tables'].items())[:5]])
            print(f"[DATABASE] Tables: {table_summary}...")
        else:
            print(f"[DATABASE] {db_stats['error']}")
        
        await asyncio.sleep(CHECK_INTERVAL_SECONDS)
    
    # Final report
    print_section("FINAL REPORT - 30 MINUTE RUNTIME ANALYSIS")
    
    end_time = datetime.now(timezone.utc)
    actual_duration = (end_time - start_time).total_seconds() / 60
    
    print(f"Actual runtime: {actual_duration:.1f} minutes")
    print(f"End time: {end_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    
    # Final status
    final_status = check_process_status()
    print(f"\n{'-'*40}")
    print("PROCESS STATUS")
    print(f"{'-'*40}")
    print(f"Running: {'YES [OK]' if final_status['running'] else 'NO [FAIL]'}")
    print(f"PID: {final_status.get('pid', 'N/A')}")
    
    # Final telemetry
    final_telemetry = check_telemetry()
    print(f"\n{'-'*40}")
    print("TELEMETRY SUMMARY")
    print(f"{'-'*40}")
    if "error" not in final_telemetry:
        for fname, count in sorted(final_telemetry['files'].items()):
            print(f"  {fname}: {count} entries")
    else:
        print(f"  Error: {final_telemetry['error']}")
    
    # Final database
    final_db = check_database()
    print(f"\n{'-'*40}")
    print("DATABASE SUMMARY")
    print(f"{'-'*40}")
    if "error" not in final_db:
        print(f"  Size: {final_db['total_size_mb']:.2f} MB")
        for table, count in sorted(final_db['tables'].items()):
            print(f"  {table}: {count} rows")
    else:
        print(f"  Error: {final_db['error']}")
    
    # Final logs summary
    final_logs = check_logs()
    print(f"\n{'-'*40}")
    print("LOGS SUMMARY")
    print(f"{'-'*40}")
    if "error" not in final_logs:
        print(f"  Total lines: {final_logs['total_lines']}")
        print(f"  Errors: {final_logs['errors']}")
        print(f"  Warnings: {final_logs['warnings']}")
        print(f"  Kline events: {final_logs['kline_events']}")
        print(f"  Signal events: {final_logs['signal_events']}")
        
        if final_logs['key_events']:
            print("\n  Key events (chronological):")
            for line in final_logs['key_events']:
                print(f"    {line[:140]}")
    else:
        print(f"  Error: {final_logs['error']}")
    
    # Runtime health assessment
    print(f"\n{'='*70}")
    print("  RUNTIME HEALTH ASSESSMENT")
    print(f"{'='*70}")
    
    health_score = 100
    issues = []
    
    if all_errors > 10:
        health_score -= 20
        issues.append(f"High error count: {all_errors}")
    elif all_errors > 0:
        health_score -= 5
        issues.append(f"Some errors: {all_errors}")
    
    if all_kline_events == 0:
        health_score -= 30
        issues.append("No kline events received (WebSocket issue?)")
    elif all_kline_events < 10:
        health_score -= 10
        issues.append(f"Low kline activity: {all_kline_events}")
    
    if not final_status['running'] and actual_duration < RUNTIME_MINUTES - 2:
        health_score -= 40
        issues.append("Bot stopped prematurely")
    
    if health_score >= 90:
        status = "EXCELLENT [OK]"
    elif health_score >= 70:
        status = "GOOD [OK]"
    elif health_score >= 50:
        status = "FAIR [!]"
    else:
        status = "POOR [FAIL]"
    
    print(f"\n  Health Score: {health_score}/100 - {status}")
    
    if issues:
        print(f"\n  Issues detected:")
        for issue in issues:
            print(f"    - {issue}")
    else:
        print(f"\n  No major issues detected!")
    
    print(f"\n{'='*70}")
    print("  MONITORING COMPLETE")
    print(f"{'='*70}")
    
    # Stop bot if still running
    if final_status["running"]:
        print("\nStopping bot...")
        try:
            pid = final_status["pid"]
            subprocess.run(["taskkill", "/PID", str(pid), "/F"], capture_output=True)
            print("Bot stopped")
            time.sleep(1)
            # Remove PID file
            if PID_FILE.exists():
                PID_FILE.unlink()
        except Exception as e:
            print(f"Error stopping bot: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(monitor_loop())
    except KeyboardInterrupt:
        print("\n\nMonitoring interrupted by user")
        # Try to stop bot
        status = check_process_status()
        if status["running"]:
            print("Stopping bot...")
            try:
                subprocess.run(["taskkill", "/PID", str(status["pid"]), "/F"], capture_output=True)
            except:
                pass
        sys.exit(0)
