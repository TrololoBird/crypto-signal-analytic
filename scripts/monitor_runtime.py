from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Lightweight runtime monitor for bot process/artifacts")
    parser.add_argument("--pid-file", type=Path, default=Path("data/bot/bot.pid"), help="path to PID file")
    parser.add_argument("--db-path", type=Path, default=Path("data/bot/signals.db"), help="path to SQLite DB")
    parser.add_argument(
        "--telemetry-dir",
        type=Path,
        default=Path("data/bot/telemetry"),
        help="path to telemetry directory",
    )
    parser.add_argument("--watch", action="store_true", help="rerun checks continuously")
    parser.add_argument("--interval", type=float, default=30.0, help="watch interval in seconds")
    return parser.parse_args()


def read_pid(pid_file: Path) -> tuple[int | None, str]:
    if not pid_file.exists():
        return None, f"PID file not found: {pid_file}"

    try:
        raw = pid_file.read_text(encoding="utf-8").strip()
    except OSError as exc:
        return None, f"PID file read error ({pid_file}): {exc}"

    if not raw:
        return None, f"PID file is empty: {pid_file}"

    try:
        return int(raw), "ok"
    except ValueError:
        return None, f"PID file has invalid integer: {pid_file}"


def is_process_running(pid: int) -> tuple[bool, str]:
    try:
        import psutil  # type: ignore

        if not psutil.pid_exists(pid):
            return False, "psutil: pid does not exist"
        try:
            process = psutil.Process(pid)
            if process.status() == psutil.STATUS_ZOMBIE:
                return False, "psutil: zombie process"
        except psutil.Error as exc:
            return False, f"psutil: unable to inspect process ({exc})"
        return True, "psutil: process is alive"
    except ImportError:
        # Fallback for environments without psutil.
        pass

    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False, "kill(0): process does not exist"
    except PermissionError:
        # Process exists but we don't have rights.
        return True, "kill(0): process exists (permission denied)"
    except OSError as exc:
        return False, f"kill(0): OS error ({exc})"

    return True, "kill(0): process is alive"


def print_report(args: argparse.Namespace) -> None:
    pid, pid_status = read_pid(args.pid_file)

    print("=" * 72)
    print("RUNTIME MONITOR")
    print("=" * 72)
    print(f"Platform      : {sys.platform}")
    print(f"PID file      : {args.pid_file}")
    print(f"Database path : {args.db_path}")
    print(f"Telemetry dir : {args.telemetry_dir}")

    if pid is None:
        print(f"Process       : STOPPED ({pid_status})")
    else:
        running, reason = is_process_running(pid)
        state = "RUNNING" if running else "STOPPED"
        print(f"Process       : {state} (pid={pid}, {reason})")

    print(f"DB exists     : {'yes' if args.db_path.exists() else 'no'}")
    print(f"Telemetry dir : {'yes' if args.telemetry_dir.exists() else 'no'}")
    print("=" * 72)


def main() -> None:
    args = parse_args()
    print_report(args)

    if args.watch:
        try:
            while True:
                time.sleep(max(args.interval, 0.1))
                print_report(args)
        except KeyboardInterrupt:
            print("[INFO] monitor stopped by user")


if __name__ == "__main__":
    main()
