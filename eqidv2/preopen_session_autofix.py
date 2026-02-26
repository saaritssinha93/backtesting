#!/usr/bin/env python3
"""Auto-fix loop for pre-open healthcheck failures (09:05-09:30 IST)."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, Iterable, List, Tuple
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")
BASE_DIR = Path(__file__).resolve().parent
BAT_DIR = BASE_DIR / "bat"
LOG_DIR = BASE_DIR / "logs"
HEALTHCHECK_SCRIPT = BASE_DIR / "preopen_session_healthcheck.py"
HEALTHCHECK_JSON = LOG_DIR / "preopen_session_healthcheck_latest.json"

TASK_TO_BAT: Dict[str, Path] = {
    "EQIDV2_log_dashboard_start_0855": BAT_DIR / "run_log_dashboard_public_link_scheduled.bat",
    "EQIDV2_eod_15mins_data_0900": BAT_DIR / "run_eqidv2_eod_scheduler_for_15mins_data.bat",
    "EQIDV2_live_combined_csv_v2_0900": BAT_DIR / "run_eqidv2_live_combined_analyser_csv_v2.bat",
    "EQIDV2_live_combined_csv_v3_0900": BAT_DIR / "run_eqidv2_live_combined_analyser_csv_v3.bat",
    "EQIDV2_live_combined_csv_v4_short_0900": BAT_DIR / "run_eqidv2_live_combined_analyser_csv_v4_short.bat",
    "EQIDV2_live_combined_csv_v4_long_0900": BAT_DIR / "run_eqidv2_live_combined_analyser_csv_v4_long.bat",
    "EQIDV2_live_combined_csv_v5_short_0900": BAT_DIR / "run_eqidv2_live_combined_analyser_csv_v5_short.bat",
    "EQIDV2_live_combined_csv_v5_long_0900": BAT_DIR / "run_eqidv2_live_combined_analyser_csv_v5_long.bat",
    "EQIDV2_avwap_paper_trade_v2_0900": BAT_DIR / "run_avwap_trade_execution_PAPER_TRADE_TRUE_v2.bat",
    "EQIDV2_avwap_paper_trade_v3_0900": BAT_DIR / "run_avwap_trade_execution_PAPER_TRADE_TRUE_v3.bat",
    "EQIDV2_avwap_paper_trade_v4_0900": BAT_DIR / "run_avwap_trade_execution_PAPER_TRADE_TRUE_v4.bat",
    "EQIDV2_avwap_paper_trade_v5_0900": BAT_DIR / "run_avwap_trade_execution_PAPER_TRADE_TRUE_v5.bat",
    "EQIDV2_authentication_v2_0900": BAT_DIR / "run_authentication_v2.bat",
}

FAIL_CHECK_TO_BAT: Dict[str, Path] = {
    "live_scanner_v2_log": BAT_DIR / "run_eqidv2_live_combined_analyser_csv_v2.bat",
    "live_scanner_v3_log": BAT_DIR / "run_eqidv2_live_combined_analyser_csv_v3.bat",
    "live_scanner_v4_short_log": BAT_DIR / "run_eqidv2_live_combined_analyser_csv_v4_short.bat",
    "live_scanner_v4_long_log": BAT_DIR / "run_eqidv2_live_combined_analyser_csv_v4_long.bat",
    "live_scanner_v5_short_log": BAT_DIR / "run_eqidv2_live_combined_analyser_csv_v5_short.bat",
    "live_scanner_v5_long_log": BAT_DIR / "run_eqidv2_live_combined_analyser_csv_v5_long.bat",
    "papertrade_v2_log": BAT_DIR / "run_avwap_trade_execution_PAPER_TRADE_TRUE_v2.bat",
    "papertrade_v3_log": BAT_DIR / "run_avwap_trade_execution_PAPER_TRADE_TRUE_v3.bat",
    "papertrade_v4_log": BAT_DIR / "run_avwap_trade_execution_PAPER_TRADE_TRUE_v4.bat",
    "papertrade_v5_log": BAT_DIR / "run_avwap_trade_execution_PAPER_TRADE_TRUE_v5.bat",
}


def _now_ist() -> dt.datetime:
    return dt.datetime.now(IST)


def _fmt_now() -> str:
    return _now_ist().strftime("%Y-%m-%d %H:%M:%S%z")


def _parse_hhmm(value: str) -> dt.time:
    s = str(value).strip()
    if len(s) != 5 or s[2] != ":":
        raise ValueError(f"invalid HH:MM time: {value}")
    hh = int(s[:2])
    mm = int(s[3:])
    return dt.time(hh, mm)


def _sleep_until(local_time: dt.time) -> None:
    now = _now_ist()
    target = dt.datetime.combine(now.date(), local_time, tzinfo=IST)
    if now >= target:
        return
    sleep_s = max(1, int((target - now).total_seconds()))
    print(f"[AUTOFIX] Waiting until {target.strftime('%Y-%m-%d %H:%M:%S%z')} ({sleep_s}s)", flush=True)
    time.sleep(sleep_s)


def _run_cmd(args: List[str], timeout_sec: int = 40) -> Tuple[int, str]:
    try:
        proc = subprocess.run(
            args,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_sec,
        )
        out = (proc.stdout or "") + (proc.stderr or "")
        return int(proc.returncode), out.strip()
    except Exception as exc:
        return 999, str(exc)


def _start_bat_detached(path: Path) -> Tuple[int, str]:
    if not path.exists():
        return 2, f"missing bat: {path}"
    cmd = f'start "" /MIN cmd /c call "{path}"'
    return _run_cmd(["cmd", "/c", cmd], timeout_sec=25)


def _run_bat_sync(path: Path) -> Tuple[int, str]:
    if not path.exists():
        return 2, f"missing bat: {path}"
    return _run_cmd(["cmd", "/c", str(path)], timeout_sec=90)


def _run_task_now(task_name: str) -> Tuple[int, str]:
    return _run_cmd(["schtasks", "/Run", "/TN", task_name], timeout_sec=40)


def _run_healthcheck(max_age_min: int) -> Tuple[int, str, List[dict]]:
    cmd = [
        sys.executable,
        "-u",
        str(HEALTHCHECK_SCRIPT),
        "--max-age-min",
        str(max_age_min),
        "--report-path",
        str(LOG_DIR / "preopen_session_healthcheck_latest.log"),
    ]
    code, out = _run_cmd(cmd, timeout_sec=120)
    failures: List[dict] = []
    if HEALTHCHECK_JSON.exists():
        try:
            payload = json.loads(HEALTHCHECK_JSON.read_text(encoding="utf-8"))
            checks = payload.get("checks", [])
            if isinstance(checks, list):
                failures = [c for c in checks if isinstance(c, dict) and str(c.get("status")) == "FAIL"]
        except Exception:
            failures = []
    return code, out, failures


def _iter_actions_for_fail(name: str) -> Iterable[Tuple[str, str, str]]:
    if name == "dashboard_local_http":
        yield ("sync_bat", "dashboard_restart", str(BAT_DIR / "run_log_dashboard_restart_keep_url.bat"))
        return

    if name.startswith("task_"):
        task_name = name[len("task_") :]
        yield ("task_run", f"task:{task_name}", task_name)
        bat = TASK_TO_BAT.get(task_name)
        if bat is not None:
            yield ("detached_bat", f"bat:{bat.name}", str(bat))
        return

    bat = FAIL_CHECK_TO_BAT.get(name)
    if bat is not None:
        yield ("detached_bat", f"bat:{bat.name}", str(bat))


def _apply_action(kind: str, payload: str) -> Tuple[int, str]:
    if kind == "task_run":
        return _run_task_now(payload)
    path = Path(payload)
    if kind == "sync_bat":
        return _run_bat_sync(path)
    if kind == "detached_bat":
        return _start_bat_detached(path)
    return 998, f"unknown action kind: {kind}"


def main() -> int:
    ap = argparse.ArgumentParser(description="Auto-fix EQIDV2 preopen healthcheck failures.")
    ap.add_argument("--start-time", default="09:05", help="IST start time (HH:MM).")
    ap.add_argument("--end-time", default="09:30", help="IST end time (HH:MM).")
    ap.add_argument("--retry-sec", type=int, default=45, help="Seconds between healthcheck cycles.")
    ap.add_argument("--max-age-min", type=int, default=35, help="Healthcheck max staleness threshold.")
    ap.add_argument(
        "--max-action-attempts",
        type=int,
        default=2,
        help="Maximum attempts per unique fix action during this run.",
    )
    ap.add_argument(
        "--max-cycles",
        type=int,
        default=0,
        help="Optional cap for cycles (0 means unlimited until end-time).",
    )
    args = ap.parse_args()

    start_t = _parse_hhmm(args.start_time)
    end_t = _parse_hhmm(args.end_time)
    if end_t <= start_t:
        print(f"[AUTOFIX] Invalid window: start={args.start_time} end={args.end_time}", flush=True)
        return 2

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    _sleep_until(start_t)

    attempts: Dict[str, int] = {}
    cycles = 0

    while True:
        now = _now_ist()
        if now.time() > end_t:
            print(f"[AUTOFIX] Window ended at {end_t.strftime('%H:%M')}. Last check time={_fmt_now()}", flush=True)
            break

        cycles += 1
        code, output, fails = _run_healthcheck(max_age_min=max(1, int(args.max_age_min)))
        print(f"[AUTOFIX] cycle={cycles} ts={_fmt_now()} healthcheck_exit={code} fail_count={len(fails)}", flush=True)
        if output:
            first_line = output.splitlines()[0]
            print(f"[AUTOFIX] healthcheck_head={first_line}", flush=True)

        if not fails:
            print("[AUTOFIX] All required checks PASS. Exiting success.", flush=True)
            return 0

        applied_any = False
        for fail in fails:
            name = str(fail.get("name", "")).strip()
            detail = str(fail.get("detail", "")).strip()
            print(f"[AUTOFIX] fail={name} | detail={detail}", flush=True)
            for kind, action_id, payload in _iter_actions_for_fail(name):
                n = attempts.get(action_id, 0)
                if n >= int(args.max_action_attempts):
                    print(f"[AUTOFIX] skip action={action_id} attempts={n} (limit reached)", flush=True)
                    continue
                attempts[action_id] = n + 1
                applied_any = True
                rc, out = _apply_action(kind, payload)
                print(
                    f"[AUTOFIX] action={action_id} kind={kind} attempt={attempts[action_id]} rc={rc}",
                    flush=True,
                )
                if out:
                    short_out = out.replace("\r", " ").replace("\n", " | ")
                    print(f"[AUTOFIX] action_out={short_out[:600]}", flush=True)

        if int(args.max_cycles) > 0 and cycles >= int(args.max_cycles):
            print(f"[AUTOFIX] max_cycles reached ({args.max_cycles}). Exiting.", flush=True)
            break

        wait_s = max(10, int(args.retry_sec))
        if not applied_any:
            # If nothing to fix was mapped, still keep polling until window end.
            wait_s = max(20, wait_s)
        time.sleep(wait_s)

    # Final check before exit.
    code, _output, fails = _run_healthcheck(max_age_min=max(1, int(args.max_age_min)))
    if code == 0 and not fails:
        print("[AUTOFIX] Final check PASS.", flush=True)
        return 0
    print(f"[AUTOFIX] Final check still failing (fail_count={len(fails)}).", flush=True)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
