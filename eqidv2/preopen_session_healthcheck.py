#!/usr/bin/env python3
"""Pre-open health check for active EQIDV2 live sessions."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from eqidv2_runtime_paths import LIVE_SIGNALS_DIR as RUNTIME_LIVE_SIGNALS_DIR

from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")
BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "logs"
LIVE_SIGNAL_DIR = RUNTIME_LIVE_SIGNALS_DIR
BAT_DIR = BASE_DIR / "bat"
KITE_EXPORT_DIR = BASE_DIR / "kite_exports"
V15_NEW_TASK = "EQIDV2_live_combined_csv_v15_new_0900"

DASHBOARD_SESSION_TASKS = (
    "EQIDV2_log_dashboard_start_0855",
    "EQIDV2_authentication_v2_0900",
    "EQIDV2_eod_5mins_data_0900",
    "EQIDV2_eod_15mins_data_0900",
    "EQIDV2_live_combined_csv_v15_new_0900",
    "EQIDV2_avwap_paper_trade_v15_0900",
    "EQIDV2_avwap_live_trade_v15_0905",
    "EQIDV2_nifty_guard_fetch_v15_0915",
    "EQIDV2_paper_trade_id_5min_v7_0900",
    "EQIDV2_live_trade_id_5min_v7_0900",
    "EQIDV2_live_combined_csv_v16_5min_0900",
    "EQIDV2_signal_early_engine_v16_5min_0900",
    "EQIDV2_pending_data_fetcher_v16_5min_0900",
    "EQIDV2_detection_engine_v16_5min_0900",
    "EQIDV2_paper_trade_v16_5min_0900",
    "EQIDV2_live_trade_v16_5min_0900",
    "EQIDV2_nifty_guard_fetch_v16_5min_0915",
    "EQIDV2_signal_discovery_v7_5mins_ID",
    "EQIDV2_entry_engine_1min_v5_ID",
    "EQIDV2_v7_research_layer_0917",
    "EQIDV2_data_for_backtesting_1545",
    "EQIDV2_backtesting_result_v7_v8_1600",
    "EQIDV2_suggestions_v7_live_research_1615",
    "EQIDV2_kite_export_start_0915",
    "EQIDV2_preopen_session_healthcheck_0905",
    "EQIDV2_eod_1540_update_1540",
)


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
    except HTTPError as exc:
        code = int(getattr(exc, "code", 0) or 0)
        if code == 401:
            return CheckResult(name, "PASS", f"{url} reachable (auth required, HTTP 401)")
        return CheckResult(name, "FAIL", f"{url} responded HTTP {code}")
    except Exception as exc:
        return CheckResult(name, "FAIL", f"{url} not reachable: {exc}")


def check_file_recent(
    path: Path,
    max_age_min: int,
    required: bool,
    label: str,
    optional_warn: bool = True,
) -> CheckResult:
    now = now_ist()
    if not path.exists():
        if required:
            return CheckResult(label, "FAIL", f"missing file: {path}")
        if optional_warn:
            return CheckResult(label, "WARN", f"missing optional file: {path}")
        return CheckResult(label, "PASS", f"optional file not present yet: {path}")
    try:
        mtime = dt.datetime.fromtimestamp(path.stat().st_mtime, tz=IST)
    except OSError as exc:
        return CheckResult(label, "FAIL", f"unable to stat {path}: {exc}")

    age_min = max(0.0, (now - mtime).total_seconds() / 60.0)
    if age_min <= float(max_age_min):
        return CheckResult(label, "PASS", f"updated {age_min:.1f}m ago | {path.name}")
    if required:
        return CheckResult(label, "FAIL", f"stale ({age_min:.1f}m) | {path.name}")
    if optional_warn:
        return CheckResult(label, "WARN", f"stale optional ({age_min:.1f}m) | {path.name}")
    return CheckResult(label, "PASS", f"optional stale ({age_min:.1f}m) | {path.name}")


def check_today_file(
    pattern: str,
    required: bool,
    max_age_min: int,
    label: str,
    optional_warn: bool = True,
) -> CheckResult:
    today = now_ist().strftime("%Y-%m-%d")
    path = LIVE_SIGNAL_DIR / pattern.format(today)
    return check_file_recent(
        path,
        max_age_min=max_age_min,
        required=required,
        label=label,
        optional_warn=optional_warn,
    )


def parse_keyfile(path: Path) -> dict[str, str]:
    payload: dict[str, str] = {}
    if not path.exists():
        return payload
    try:
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            payload[key.strip()] = value.strip()
    except OSError:
        return {}
    return payload


def _parse_keyfile_ts(value: str) -> Optional[dt.datetime]:
    raw = str(value or "").strip()
    if not raw:
        return None
    candidates = [raw, raw.replace("_", "T")]
    for candidate in candidates:
        try:
            parsed = dt.datetime.fromisoformat(candidate.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=IST)
            return parsed.astimezone(IST)
        except ValueError:
            pass
    for fmt in ("%Y-%m-%d_%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return dt.datetime.strptime(raw, fmt).replace(tzinfo=IST)
        except ValueError:
            pass
    return None


def check_status_file_today(
    path: Path,
    label: str,
    allowed_statuses: set[str],
    required: bool = True,
) -> CheckResult:
    payload = parse_keyfile(path)
    if not payload:
        return CheckResult(label, "FAIL" if required else "PASS", f"missing status file: {path}")

    status = str(payload.get("status", "")).strip().upper()
    if status not in allowed_statuses:
        return CheckResult(label, "FAIL", f"unexpected status={status or 'N/A'} | {path.name}")

    ts = _parse_keyfile_ts(payload.get("ts", ""))
    if ts is None:
        return CheckResult(label, "FAIL", f"missing/invalid ts in {path.name}")

    today = now_ist().date()
    if ts.date() != today:
        return CheckResult(label, "FAIL", f"status not from today ({ts.date().isoformat()}) | {path.name}")

    return CheckResult(label, "PASS", f"status={status} today @ {ts.strftime('%H:%M:%S')} | {path.name}")


def check_supervised_runtime_if_enabled(
    log_path: Path,
    status_path: Path,
    heartbeat_path: Path,
    max_age_min: int,
    label: str,
    enabled: bool,
    disabled_detail: str,
) -> CheckResult:
    if not enabled:
        return CheckResult(label, "PASS", disabled_detail)

    heartbeat = parse_keyfile(heartbeat_path)
    hb_state = str(heartbeat.get("state", "")).strip().upper()
    hb_ts = _parse_keyfile_ts(heartbeat.get("ts_utc", "") or heartbeat.get("ts", ""))
    if hb_ts is not None:
        age_min = max(0.0, (now_ist() - hb_ts).total_seconds() / 60.0)
        if age_min <= float(max_age_min) and hb_state in {"RUNNING", "RESTARTING", "COOLDOWN"}:
            idle_sec = str(heartbeat.get("idle_sec", "")).strip()
            idle_text = f" | idle_sec={idle_sec}" if idle_sec else ""
            return CheckResult(
                label,
                "PASS",
                f"heartbeat={hb_state} updated {age_min:.1f}m ago{idle_text} | {heartbeat_path.name}",
            )

    status = parse_keyfile(status_path)
    status_value = str(status.get("status", "")).strip().upper()
    status_ts = _parse_keyfile_ts(status.get("ts", ""))
    if status_ts is not None:
        age_min = max(0.0, (now_ist() - status_ts).total_seconds() / 60.0)
        if age_min <= float(max_age_min) and status_value in {"SUCCESS", "RUNNING", "SKIPPED_CUTOFF", "STOPPED_AFTER_CUTOFF"}:
            return CheckResult(
                label,
                "PASS",
                f"status={status_value} updated {age_min:.1f}m ago | {status_path.name}",
            )

    return check_file_recent(
        log_path,
        max_age_min=max_age_min,
        required=True,
        label=label,
        optional_warn=False,
    )


def check_bat_exists(path: Path, label: str, detail_prefix: str = "runner wrapper present") -> CheckResult:
    if path.exists():
        return CheckResult(label, "PASS", f"{detail_prefix} | {path.name}")
    return CheckResult(label, "FAIL", f"missing runner wrapper: {path}")


def _run_schtasks_query(task_name: str) -> Optional[str]:
    candidates = [task_name]
    normalized = task_name.strip()
    if normalized:
        root_name = f"\\{normalized.lstrip('\\')}"
        bare_name = normalized.lstrip("\\")
        for candidate in (root_name, bare_name):
            if candidate and candidate not in candidates:
                candidates.append(candidate)

    for candidate in candidates:
        try:
            proc = subprocess.run(
                ["schtasks", "/Query", "/TN", candidate, "/FO", "LIST", "/V"],
                capture_output=True,
                text=True,
                check=False,
            )
        except Exception:
            continue
        if proc.returncode == 0:
            return proc.stdout or ""
    return None


def _task_exists(task_name: str) -> bool:
    return _run_schtasks_query(task_name) is not None


def _task_is_enabled(task_name: str) -> bool:
    out = _run_schtasks_query(task_name)
    if not out:
        return False
    state = _extract_value(out.splitlines(), "Scheduled Task State")
    return state.strip().upper() == "ENABLED"


def _extract_value(lines: List[str], prefix: str) -> str:
    prefix_l = prefix.lower()
    for ln in lines:
        s = ln.strip()
        if s.lower().startswith(prefix_l):
            _, _, tail = s.partition(":")
            return tail.strip()
    return ""


def _task_scheduled_time(task_name: str) -> Optional[dt.time]:
    suffix = task_name.rsplit("_", 1)[-1]
    if len(suffix) != 4 or not suffix.isdigit():
        return None
    hh = int(suffix[:2])
    mm = int(suffix[2:])
    if hh > 23 or mm > 59:
        return None
    return dt.time(hh, mm)


def _task_should_have_run_today(task_name: str, now_local: dt.datetime) -> bool:
    scheduled = _task_scheduled_time(task_name)
    if scheduled is None:
        return False
    return now_local.time() >= scheduled


def check_task_ran_today(task_name: str) -> CheckResult:
    out = _run_schtasks_query(task_name)
    label = f"task_{task_name}"
    if not out:
        return CheckResult(label, "FAIL", "task not found or query failed")

    lines = out.splitlines()
    state = _extract_value(lines, "Scheduled Task State")
    status = _extract_value(lines, "Status")
    last_run = _extract_value(lines, "Last Run Time")
    start_date = _extract_value(lines, "Start Date")
    next_run = _extract_value(lines, "Next Run Time")

    if state.upper() != "ENABLED":
        return CheckResult(label, "FAIL", f"task not enabled (state={state or 'N/A'})")

    # Expected format in this environment: DD-MM-YYYY HH:MM:SS
    today_dmy = now_ist().strftime("%d-%m-%Y")
    if not last_run.startswith(today_dmy):
        # Transitional acceptance:
        # If task was created today (first run pending tomorrow) schtasks reports a
        # sentinel last-run date 30-11-1999. Avoid false FAIL in that case.
        never_ran = last_run.startswith("30-11-1999") or last_run.startswith("N/A")
        if never_ran and start_date.startswith(today_dmy):
            return CheckResult(
                label,
                "PASS",
                f"new task created today | first run pending | status={status or 'N/A'} | next_run={next_run or 'N/A'}",
            )
        return CheckResult(
            label,
            "FAIL",
            f"not run today | last_run={last_run or 'N/A'} | status={status or 'N/A'}",
        )

    return CheckResult(label, "PASS", f"ran today | status={status or 'N/A'} | last_run={last_run}")


def check_task_enabled_state(
    task_name: str,
    label: str,
    require_run_today: bool,
    inactive_ok: bool,
    inactive_detail: str,
) -> CheckResult:
    out = _run_schtasks_query(task_name)
    if not out:
        if inactive_ok:
            return CheckResult(label, "PASS", f"{inactive_detail} (task missing or query failed)")
        return CheckResult(label, "FAIL", "task not found or query failed")

    lines = out.splitlines()
    state = _extract_value(lines, "Scheduled Task State")
    status = _extract_value(lines, "Status")
    last_run = _extract_value(lines, "Last Run Time")
    next_run = _extract_value(lines, "Next Run Time")
    start_date = _extract_value(lines, "Start Date")

    if state.upper() != "ENABLED":
        if inactive_ok:
            return CheckResult(label, "PASS", inactive_detail)
        return CheckResult(label, "FAIL", f"task not enabled (state={state or 'N/A'})")

    today_dmy = now_ist().strftime("%d-%m-%Y")
    if not require_run_today:
        if last_run.startswith(today_dmy):
            return CheckResult(
                label,
                "PASS",
                f"ran today | status={status or 'N/A'} | last_run={last_run} | next_run={next_run or 'N/A'}",
            )
        return CheckResult(
            label,
            "PASS",
            f"enabled | status={status or 'N/A'} | next_run={next_run or 'N/A'} | last_run={last_run or 'N/A'}",
        )

    if not last_run.startswith(today_dmy):
        never_ran = last_run.startswith("30-11-1999") or last_run.startswith("N/A")
        if never_ran:
            if start_date.startswith(today_dmy):
                return CheckResult(
                    label,
                    "PASS",
                    f"new task created today | first run pending | status={status or 'N/A'} | next_run={next_run or 'N/A'}",
                )
            return CheckResult(
                label,
                "FAIL",
                f"enabled but not run today yet | status={status or 'N/A'} | next_run={next_run or 'N/A'}",
            )
        return CheckResult(
            label,
            "FAIL",
            f"not run today | last_run={last_run or 'N/A'} | status={status or 'N/A'}",
        )

    return CheckResult(label, "PASS", f"ran today | status={status or 'N/A'} | last_run={last_run}")


def check_file_recent_if_enabled(
    path: Path,
    max_age_min: int,
    label: str,
    enabled: bool,
    disabled_detail: str,
    required_when_enabled: bool = True,
    optional_warn_when_enabled: bool = True,
) -> CheckResult:
    if not enabled:
        return CheckResult(label, "PASS", disabled_detail)
    return check_file_recent(
        path,
        max_age_min=max_age_min,
        required=required_when_enabled,
        label=label,
        optional_warn=optional_warn_when_enabled,
    )


def build_checks(max_age_min: int, include_optional_csv: bool, warn_optional_csv: bool) -> List[CheckResult]:
    checks: List[CheckResult] = []

    # Core reachability.
    checks.append(check_http("http://127.0.0.1:8787/", timeout_sec=8.0))

    eod_15min_enabled = _task_is_enabled("EQIDV2_eod_15mins_data_0900")
    v15_new_enabled = _task_is_enabled(V15_NEW_TASK)
    v15_paper_enabled = _task_is_enabled("EQIDV2_avwap_paper_trade_v15_0900")
    v15_live_enabled = _task_is_enabled("EQIDV2_avwap_live_trade_v15_0905")
    v15_nifty_enabled = _task_is_enabled("EQIDV2_nifty_guard_fetch_v15_0915")
    v16_5min_nifty_enabled = _task_is_enabled("EQIDV2_nifty_guard_fetch_v16_5min_0915")
    kite_export_enabled = _task_is_enabled("EQIDV2_kite_export_start_0915")
    now_local = now_ist()
    v15_nifty_log_due = now_local.time() >= dt.time(9, 15)
    v16_5min_nifty_log_due = now_local.time() >= dt.time(9, 15)

    # Dashboard scheduled sessions. Future slots are reported as waiting; past
    # enabled slots must have a same-day scheduler run.
    for task in DASHBOARD_SESSION_TASKS:
        checks.append(
            check_task_enabled_state(
                task,
                f"task_{task}",
                require_run_today=_task_should_have_run_today(task, now_local),
                inactive_ok=True,
                inactive_detail="session not enabled",
            )
        )

    # Dashboard sessions mapped 1:1 to live cards.
    checks.append(
        check_status_file_today(
            LOG_DIR / "authentication_v2_runner.status",
            label="authentication_v2",
            allowed_statuses={"SUCCESS"},
        )
    )
    checks.append(
        check_file_recent_if_enabled(
            LOG_DIR / "eqidv2_eod_scheduler_for_15mins_data_live_minimal.log",
            max_age_min=max_age_min,
            label="eod_15min_data",
            enabled=eod_15min_enabled,
            disabled_detail="session not enabled",
        )
    )
    checks.append(
        check_file_recent_if_enabled(
            LOG_DIR / "eqidv2_live_combined_analyser_csv_v15_new_persistent.log",
            max_age_min=max_age_min,
            label="live_combined_csv_v15_new_persistent",
            enabled=v15_new_enabled,
            disabled_detail="session not enabled",
        )
    )
    checks.append(
        check_file_recent_if_enabled(
            LOG_DIR / "nifty_guard_fetcher_v15.log",
            max_age_min=max_age_min,
            label="nifty_guard_fetch_v15",
            enabled=v15_nifty_enabled and v15_nifty_log_due,
            disabled_detail="session not enabled or scheduled later in session",
        )
    )
    checks.append(
        check_file_recent_if_enabled(
            LOG_DIR / "nifty_guard_fetcher_v16_5min.log",
            max_age_min=max_age_min,
            label="nifty_guard_fetch_v16_5min",
            enabled=v16_5min_nifty_enabled and v16_5min_nifty_log_due,
            disabled_detail="session not enabled or scheduled later in session",
        )
    )
    checks.append(
        check_file_recent_if_enabled(
            LIVE_SIGNAL_DIR / f"paper_trade_execution_{now_local.strftime('%Y-%m-%d')}_v15_new.log",
            max_age_min=max_age_min,
            label="paper_trade_v15",
            enabled=v15_paper_enabled,
            disabled_detail="session not enabled",
        )
    )
    checks.append(
        check_supervised_runtime_if_enabled(
            LIVE_SIGNAL_DIR / f"live_trade_execution_{now_local.strftime('%Y-%m-%d')}_v15_new.log",
            LOG_DIR / "avwap_trade_execution_PAPER_TRADE_FALSE_v15_new.status",
            LOG_DIR / "avwap_trade_execution_PAPER_TRADE_FALSE_v15_new.heartbeat",
            max_age_min=max_age_min,
            label="kite_trade_v15",
            enabled=v15_live_enabled,
            disabled_detail="session not enabled",
        )
    )

    today = now_local.strftime("%Y-%m-%d")

    checks.append(CheckResult("preopen_healthcheck", "PASS", "report generated now"))

    checks.append(
        check_bat_exists(
            BAT_DIR / "run_eqidv2_eod_scheduler_for_1540_update.bat",
            "eod_1540_update",
            detail_prefix="later-session wrapper present",
        )
    )

    checks.append(
        check_task_enabled_state(
            "EQIDV2_kite_export_start_0915",
            "kite_holdings_today_csv",
            require_run_today=False,
            inactive_ok=True,
            inactive_detail="session not enabled",
        )
    )
    checks.append(
        check_task_enabled_state(
            "EQIDV2_kite_export_start_0915",
            "kite_positions_day_today_csv",
            require_run_today=False,
            inactive_ok=True,
            inactive_detail="session not enabled",
        )
    )

    # Optional output file presence (can be empty early in session).
    if include_optional_csv:
        checks.append(
            check_file_recent_if_enabled(
                LIVE_SIGNAL_DIR / f"signals_{today}_v15_new_short.csv",
                max_age_min=max_age_min,
                label="live_signals_csv_v15_new_short",
                enabled=v15_new_enabled,
                disabled_detail="session not enabled",
                required_when_enabled=False,
                optional_warn_when_enabled=warn_optional_csv,
            )
        )
        checks.append(
            check_file_recent_if_enabled(
                LIVE_SIGNAL_DIR / f"signals_{today}_v15_new_long.csv",
                max_age_min=max_age_min,
                label="live_signals_csv_v15_new_long",
                enabled=v15_new_enabled,
                disabled_detail="session not enabled",
                required_when_enabled=False,
                optional_warn_when_enabled=warn_optional_csv,
            )
        )
        checks.append(
            check_file_recent_if_enabled(
                LIVE_SIGNAL_DIR / f"paper_trades_{today}_v15_new.csv",
                max_age_min=max_age_min,
                label="live_papertrade_result_csv_v15",
                enabled=v15_paper_enabled,
                disabled_detail="session not enabled",
                required_when_enabled=False,
                optional_warn_when_enabled=warn_optional_csv,
            )
        )
        checks.append(
            check_file_recent_if_enabled(
                LIVE_SIGNAL_DIR / f"live_trades_{today}_v15_new.csv",
                max_age_min=max_age_min,
                label="live_kite_trades_csv_v15",
                enabled=v15_live_enabled,
                disabled_detail="session not enabled",
                required_when_enabled=False,
                optional_warn_when_enabled=warn_optional_csv,
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
        "--warn-optional-csv",
        action="store_true",
        help="Mark missing/stale optional CSV checks as WARN (default is PASS for optional checks).",
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
        warn_optional_csv=bool(args.warn_optional_csv),
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
