#!/usr/bin/env python3
"""Pre-open health check for EQIDV2 live sessions (v1/v2/v3)."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional
from urllib.request import Request, urlopen

from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")
BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "logs"
LIVE_SIGNAL_DIR = BASE_DIR / "live_signals"


@dataclass
class CheckResult:
    name: str
    status: str  # PASS | WARN | FAIL
    detail: str


def now_ist() -> dt.datetime:
    return dt.datetime.now(IST)


def _fmt_ts(ts: dt.datetime) -> str:
    return ts.strftime("%Y-%m-%d %H:%M:%S%z")


def check_http(url: str, timeout_sec: float) -> CheckResult:
    name = "dashboard_local_http"
    try:
        req = Request(url, method="GET")
        with urlopen(req, timeout=timeout_sec) as resp:
            code = int(getattr(resp, "status", 0) or 0)
        if 200 <= code < 500:
            return CheckResult(name, "PASS", f"{url} responded HTTP {code}")
        return CheckResult(name, "FAIL", f"{url} responded HTTP {code}")
    except Exception as exc:
        return CheckResult(name, "FAIL", f"{url} not reachable: {exc}")


def check_file_recent(path: Path, max_age_min: int, required: bool, label: str) -> CheckResult:
    now = now_ist()
    if not path.exists():
        if required:
            return CheckResult(label, "FAIL", f"missing file: {path}")
        return CheckResult(label, "WARN", f"missing optional file: {path}")
    try:
        mtime = dt.datetime.fromtimestamp(path.stat().st_mtime, tz=IST)
    except OSError as exc:
        return CheckResult(label, "FAIL", f"unable to stat {path}: {exc}")

    age_min = max(0.0, (now - mtime).total_seconds() / 60.0)
    if age_min <= float(max_age_min):
        return CheckResult(label, "PASS", f"updated {age_min:.1f}m ago | {path.name}")
    if required:
        return CheckResult(label, "FAIL", f"stale ({age_min:.1f}m) | {path.name}")
    return CheckResult(label, "WARN", f"stale optional ({age_min:.1f}m) | {path.name}")


def check_today_file(pattern: str, required: bool, max_age_min: int, label: str) -> CheckResult:
    today = now_ist().strftime("%Y-%m-%d")
    path = LIVE_SIGNAL_DIR / pattern.format(today)
    return check_file_recent(path, max_age_min=max_age_min, required=required, label=label)


def _run_schtasks_query(task_name: str) -> Optional[str]:
    try:
        proc = subprocess.run(
            ["schtasks", "/Query", "/TN", task_name, "/FO", "LIST", "/V"],
            capture_output=True,
            text=True,
            check=False,
        )
    except Exception:
        return None
    if proc.returncode != 0:
        return None
    return proc.stdout or ""


def _extract_value(lines: List[str], prefix: str) -> str:
    prefix_l = prefix.lower()
    for ln in lines:
        s = ln.strip()
        if s.lower().startswith(prefix_l):
            _, _, tail = s.partition(":")
            return tail.strip()
    return ""


def check_task_ran_today(task_name: str) -> CheckResult:
    out = _run_schtasks_query(task_name)
    label = f"task_{task_name}"
    if not out:
        return CheckResult(label, "FAIL", "task not found or query failed")

    lines = out.splitlines()
    state = _extract_value(lines, "Scheduled Task State")
    status = _extract_value(lines, "Status")
    last_run = _extract_value(lines, "Last Run Time")

    if state.upper() != "ENABLED":
        return CheckResult(label, "FAIL", f"task not enabled (state={state or 'N/A'})")

    # Expected format in this environment: DD-MM-YYYY HH:MM:SS
    today_dmy = now_ist().strftime("%d-%m-%Y")
    if not last_run.startswith(today_dmy):
        return CheckResult(
            label,
            "FAIL",
            f"not run today | last_run={last_run or 'N/A'} | status={status or 'N/A'}",
        )

    return CheckResult(label, "PASS", f"ran today | status={status or 'N/A'} | last_run={last_run}")


def build_checks(max_age_min: int, include_optional_csv: bool) -> List[CheckResult]:
    checks: List[CheckResult] = []

    # Core reachability.
    checks.append(check_http("http://127.0.0.1:8787/", timeout_sec=8.0))

    # Scheduled tasks expected to have run by 09:05.
    preopen_tasks = [
        "EQIDV2_log_dashboard_start_0855",
        "EQIDV2_eod_15mins_data_0900",
        "EQIDV2_live_combined_csv_0900",
        "EQIDV2_live_combined_csv_v2_0900",
        "EQIDV2_live_combined_csv_v3_0900",
        "EQIDV2_avwap_paper_trade_0900",
        "EQIDV2_avwap_paper_trade_v2_0900",
        "EQIDV2_avwap_paper_trade_v3_0900",
        "EQIDV2_authentication_v2_0900",
    ]
    for task in preopen_tasks:
        checks.append(check_task_ran_today(task))

    # Scanner logs should be fresh.
    checks.append(
        check_file_recent(
            LOG_DIR / "eqidv2_live_combined_analyser_csv.log",
            max_age_min=max_age_min,
            required=True,
            label="live_scanner_v1_log",
        )
    )
    checks.append(
        check_file_recent(
            LOG_DIR / "eqidv2_live_combined_analyser_csv_v2.log",
            max_age_min=max_age_min,
            required=True,
            label="live_scanner_v2_log",
        )
    )
    checks.append(
        check_file_recent(
            LOG_DIR / "eqidv2_live_combined_analyser_csv_v3.log",
            max_age_min=max_age_min,
            required=True,
            label="live_scanner_v3_log",
        )
    )

    # Papertrade logs should be fresh.
    today = now_ist().strftime("%Y-%m-%d")
    checks.append(
        check_file_recent(
            LOG_DIR / f"avwap_trade_execution_PAPER_TRADE_TRUE_{today}.log",
            max_age_min=max_age_min,
            required=True,
            label="papertrade_v1_log",
        )
    )
    checks.append(
        check_file_recent(
            LOG_DIR / f"avwap_trade_execution_PAPER_TRADE_TRUE_v2_{today}.log",
            max_age_min=max_age_min,
            required=True,
            label="papertrade_v2_log",
        )
    )
    checks.append(
        check_file_recent(
            LOG_DIR / f"avwap_trade_execution_PAPER_TRADE_TRUE_v3_{today}.log",
            max_age_min=max_age_min,
            required=True,
            label="papertrade_v3_log",
        )
    )

    # Optional output file presence (can be empty early in session).
    if include_optional_csv:
        checks.append(
            check_today_file(
                "signals_{}.csv",
                required=False,
                max_age_min=max_age_min,
                label="live_entries_csv_v1",
            )
        )
        checks.append(
            check_today_file(
                "signals_{}_v2.csv",
                required=False,
                max_age_min=max_age_min,
                label="live_entries_csv_v2",
            )
        )
        checks.append(
            check_today_file(
                "signals_{}_v3.csv",
                required=False,
                max_age_min=max_age_min,
                label="live_entries_csv_v3",
            )
        )
        checks.append(
            check_today_file(
                "paper_trades_{}.csv",
                required=False,
                max_age_min=max_age_min,
                label="papertrade_csv_v1",
            )
        )
        checks.append(
            check_today_file(
                "paper_trades_{}_v2.csv",
                required=False,
                max_age_min=max_age_min,
                label="papertrade_csv_v2",
            )
        )
        checks.append(
            check_today_file(
                "paper_trades_{}_v3.csv",
                required=False,
                max_age_min=max_age_min,
                label="papertrade_csv_v3",
            )
        )

    return checks


def render_report(checks: List[CheckResult]) -> str:
    now = now_ist()
    total = len(checks)
    passed = sum(1 for c in checks if c.status == "PASS")
    warned = sum(1 for c in checks if c.status == "WARN")
    failed = sum(1 for c in checks if c.status == "FAIL")
    overall = "PASS" if failed == 0 else "FAIL"

    lines: List[str] = []
    lines.append(f"EQIDV2 PREOPEN HEALTHCHECK | {_fmt_ts(now)}")
    lines.append(f"overall={overall} | pass={passed} warn={warned} fail={failed} total={total}")
    lines.append("-" * 92)
    for c in checks:
        lines.append(f"{c.status:5s} | {c.name:34s} | {c.detail}")
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description="Run pre-open health checks for EQIDV2 sessions.")
    ap.add_argument(
        "--max-age-min",
        type=int,
        default=35,
        help="Max allowed log staleness in minutes for required runtime checks.",
    )
    ap.add_argument(
        "--skip-optional-csv",
        action="store_true",
        help="Skip optional CSV presence checks.",
    )
    ap.add_argument(
        "--report-path",
        default="",
        help="Optional explicit report output path. If omitted, writes date-stamped + latest file under logs/.",
    )
    args = ap.parse_args()

    checks = build_checks(
        max_age_min=max(1, int(args.max_age_min)),
        include_optional_csv=not bool(args.skip_optional_csv),
    )
    report = render_report(checks)
    print(report, flush=True)

    ts = now_ist()
    dated_name = f"preopen_session_healthcheck_{ts.strftime('%Y-%m-%d')}.log"
    dated_path = LOG_DIR / dated_name
    latest_path = LOG_DIR / "preopen_session_healthcheck_latest.log"
    explicit_path = Path(args.report_path).expanduser() if str(args.report_path).strip() else None
    payload_json = {
        "ts_ist": _fmt_ts(ts),
        "overall": "PASS" if all(c.status != "FAIL" for c in checks) else "FAIL",
        "checks": [c.__dict__ for c in checks],
    }
    json_path = LOG_DIR / "preopen_session_healthcheck_latest.json"

    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        dated_path.write_text(report + "\n", encoding="utf-8")
        latest_path.write_text(report + "\n", encoding="utf-8")
        json_path.write_text(json.dumps(payload_json, ensure_ascii=False, indent=2), encoding="utf-8")
        if explicit_path is not None:
            explicit_path.parent.mkdir(parents=True, exist_ok=True)
            explicit_path.write_text(report + "\n", encoding="utf-8")
    except OSError as exc:
        print(f"[WARN] Unable to persist report: {exc}", file=sys.stderr)

    return 0 if all(c.status != "FAIL" for c in checks) else 1


if __name__ == "__main__":
    raise SystemExit(main())

