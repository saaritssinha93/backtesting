"""Wait until the daily backtesting data job and verifier are ready.

The dashboard parity job is scheduled by wall-clock time, but the parquet build
can finish a few minutes later. This helper gates the parity run on actual data
readiness instead of producing a misleading zero/partial v11 comparison.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path
from typing import Any

import pandas as pd

from eqidv2_runtime_paths import runtime_dir


BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "logs"
VERIFY_DIR = runtime_dir("backtesting_result_v11", "latest")


def _today_ist() -> str:
    return pd.Timestamp.now(tz="Asia/Kolkata").strftime("%Y-%m-%d")


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return ""


def _data_job_status(day: str) -> tuple[str, str]:
    path = LOG_DIR / f"data_for_backtesting_{day}.log"
    if not path.exists():
        return "WAIT", f"waiting for {path.name}"
    text = _read_text(path)
    if not text:
        return "WAIT", f"{path.name} exists but is empty"
    start_at = text.rfind("START Data for backtesting parallel session")
    end_matches = list(re.finditer(r"END Data for backtesting parallel session \(exit=(\d+)\)", text))
    if not end_matches:
        return "WAIT", f"{path.name} has not reached END yet"
    m = end_matches[-1]
    if start_at > m.start():
        return "WAIT", f"{path.name} latest run has not reached END yet"
    exit_code = int(m.group(1))
    if exit_code == 0:
        return "PASS", f"{path.name} finished exit=0"
    return "FAIL", f"{path.name} finished exit={exit_code}"


def _verify_status(day: str, scope: str = "") -> tuple[str, str, dict[str, Any]]:
    path = VERIFY_DIR / f"data_verify_{day}.json"
    if not path.exists():
        return "WAIT", f"waiting for {path.name}", {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return "WAIT", f"{path.name} is not readable yet: {type(exc).__name__}", {}
    observed_scope = str(payload.get("scope", "all") or "all").strip().lower()
    expected_scope = str(scope or "").strip().lower()
    if expected_scope and observed_scope != expected_scope:
        return "WAIT", f"{path.name} scope={observed_scope}, waiting for scope={expected_scope}", payload
    exit_code = int(payload.get("overall_exit_code", 2))
    status = str(payload.get("overall_status", "")).upper() or {0: "PASS", 1: "WARN", 2: "FAIL"}.get(exit_code, "FAIL")
    if exit_code == 0:
        return "PASS", f"{path.name} PASS", payload
    if exit_code == 1:
        return "WARN", f"{path.name} WARN", payload
    return "FAIL", f"{path.name} {status} exit={exit_code}", payload


def wait(day: str, timeout_sec: int, poll_sec: int, scope: str = "") -> int:
    deadline = time.time() + max(timeout_sec, 1)
    last_line = ""
    while True:
        data_status, data_note = _data_job_status(day)
        verify_status, verify_note, payload = _verify_status(day, scope=scope)
        line = f"[data-ready] data={data_status} ({data_note}); verify={verify_status} ({verify_note})"
        if line != last_line:
            print(line, flush=True)
            last_line = line

        if data_status == "FAIL" or verify_status == "FAIL":
            return 2
        if data_status == "PASS" and verify_status == "PASS":
            run_at = payload.get("run_at_ist", "")
            print(f"[data-ready] ready for {day}; verifier_run_at={run_at}", flush=True)
            return 0
        if data_status == "PASS" and verify_status == "WARN":
            print("[data-ready] verifier WARN is not accepted for parity run", flush=True)
            return 1
        if time.time() >= deadline:
            print(f"[data-ready] timeout after {timeout_sec}s waiting for {day}", flush=True)
            return 2
        time.sleep(max(poll_sec, 1))


def main() -> int:
    ap = argparse.ArgumentParser(description="Wait for daily backtesting data readiness")
    ap.add_argument("--date", default=_today_ist(), help="YYYY-MM-DD, defaults to today IST")
    ap.add_argument("--timeout-sec", type=int, default=3600)
    ap.add_argument("--poll-sec", type=int, default=15)
    ap.add_argument("--scope", choices=("", "all", "fno"), default="")
    args = ap.parse_args()
    return wait(str(args.date), args.timeout_sec, args.poll_sec, scope=str(args.scope))


if __name__ == "__main__":
    raise SystemExit(main())
