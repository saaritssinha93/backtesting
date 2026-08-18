from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from eqidv2_runtime_paths import RUNTIME_STATUS_DIR, runtime_dir


SESSION = "collect_filtered_stock_data"
IST = ZoneInfo("Asia/Kolkata")
DEFAULT_WORKSPACE = Path(
    r"C:\Users\Saarit\OneDrive\Desktop\Trading\Short_term_trading"
)
DEFAULT_SCRIPT = DEFAULT_WORKSPACE / "collect_filtered_stock_data.py"
DEFAULT_INPUT = DEFAULT_WORKSPACE / "filtered_fno_MIS_v2.py"
DEFAULT_OUTPUT = DEFAULT_WORKSPACE / "filtered_fno_MIS_v2_data_nse"
OUTPUT_ROOT = runtime_dir(SESSION)
STATUS_PATH = RUNTIME_STATUS_DIR / f"{SESSION}.status"
HEARTBEAT_PATH = RUNTIME_STATUS_DIR / f"{SESSION}.heartbeat"


def now_ist() -> dt.datetime:
    return dt.datetime.now(IST)


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(text, encoding="utf-8")
    os.replace(temporary, path)


def write_status(path: Path, status: str, **extra: Any) -> None:
    payload: dict[str, Any] = {
        "status": status,
        "session": SESSION,
        "pid": os.getpid(),
        "ts": now_ist().isoformat(),
    }
    payload.update(extra)
    atomic_write_text(path, json.dumps(payload, indent=2, ensure_ascii=True))


def summary_counts(path: Path) -> tuple[int, Counter[str]]:
    counts: Counter[str] = Counter()
    if not path.exists():
        return 0, counts
    total = 0
    with path.open("r", encoding="utf-8-sig", errors="replace", newline="") as handle:
        for row in csv.DictReader(handle):
            total += 1
            counts[str(row.get("status", "unknown") or "unknown").lower()] += 1
    return total, counts


def write_report(
    path: Path,
    *,
    status: str,
    started: dt.datetime,
    finished: dt.datetime,
    output: Path,
    exit_code: int,
) -> None:
    summary = output / "summary.csv"
    total, counts = summary_counts(summary)
    lines = [
        "# collect_filtered_stock_data",
        "",
        f"Session date: {started.date().isoformat()}",
        f"Status: {status}",
        f"Started: {started.isoformat()}",
        f"Finished: {finished.isoformat()}",
        f"Exit code: {exit_code}",
        f"Output folder: `{output}`",
        f"Summary: `{summary}`",
        f"Symbols in summary: {total}",
        f"Counts: {dict(counts)}",
        "Mode: NSE annual reports and shareholding refresh (`--skip-screener`).",
        "",
    ]
    atomic_write_text(path, "\n".join(lines))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Dashboard-aware weekday wrapper for collect_filtered_stock_data.py."
    )
    parser.add_argument("--python-exe", type=Path, default=Path(sys.executable))
    parser.add_argument("--script", type=Path, default=DEFAULT_SCRIPT)
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--heartbeat-seconds", type=float, default=15.0)
    parser.add_argument("--allow-weekend", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    started = now_ist()
    session_date = started.date().isoformat()
    if started.weekday() >= 5 and not args.allow_weekend:
        write_status(STATUS_PATH, "SKIPPED_WEEKEND", session_date_ist=session_date)
        write_status(HEARTBEAT_PATH, "SKIPPED_WEEKEND", session_date_ist=session_date)
        return 0
    if not args.script.is_file() or not args.input.is_file():
        missing = args.script if not args.script.is_file() else args.input
        write_status(STATUS_PATH, "FAILED", error=f"Missing file: {missing}")
        return 1

    session_dir = OUTPUT_ROOT / "sessions" / session_date
    latest_dir = OUTPUT_ROOT / "latest"
    session_dir.mkdir(parents=True, exist_ok=True)
    latest_dir.mkdir(parents=True, exist_ok=True)
    report = session_dir / "collect_filtered_stock_data.md"
    latest_report = latest_dir / "latest_collect_filtered_stock_data.md"
    command = [
        str(args.python_exe),
        "-u",
        str(args.script),
        "--input",
        str(args.input),
        "--output",
        str(args.output),
        "--skip-screener",
    ]
    running = {
        "session_date_ist": session_date,
        "started_at_ist": started.isoformat(),
        "script": str(args.script),
        "output": str(args.output),
        "message": "Refreshing NSE annual-report and shareholding records.",
    }
    write_status(STATUS_PATH, "RUNNING", **running)
    write_status(HEARTBEAT_PATH, "RUNNING", **running)
    print(f"[START] {' '.join(command)}", flush=True)

    try:
        process = subprocess.Popen(command, cwd=str(args.script.parent))
        while process.poll() is None:
            write_status(
                HEARTBEAT_PATH,
                "RUNNING",
                **running,
                elapsed_seconds=round((now_ist() - started).total_seconds(), 1),
            )
            time.sleep(max(1.0, args.heartbeat_seconds))
        exit_code = int(process.returncode or 0)
    except Exception as exc:
        exit_code = 1
        print(f"[ERROR] {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)

    finished = now_ist()
    status = "SUCCESS" if exit_code == 0 else "FAILED"
    write_report(
        report,
        status=status,
        started=started,
        finished=finished,
        output=args.output,
        exit_code=exit_code,
    )
    atomic_write_text(latest_report, report.read_text(encoding="utf-8"))
    total, counts = summary_counts(args.output / "summary.csv")
    final = {
        **running,
        "finished_at_ist": finished.isoformat(),
        "duration_seconds": round((finished - started).total_seconds(), 1),
        "exit_code": exit_code,
        "summary_rows": total,
        "summary_counts": dict(counts),
        "report": str(latest_report),
    }
    write_status(STATUS_PATH, status, **final)
    write_status(HEARTBEAT_PATH, status, **final)
    print(f"[{status}] exit={exit_code} rows={total} counts={dict(counts)}", flush=True)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
