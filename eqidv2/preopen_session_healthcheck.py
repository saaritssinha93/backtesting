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
from typing import Callable, List, Optional, Tuple
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from eqidv2_runtime_paths import (
    LIVE_SIGNALS_DIR as RUNTIME_LIVE_SIGNALS_DIR,
    RUNTIME_ROOT,
    RUNTIME_STATUS_DIR,
)

from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")
BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "logs"
LIVE_SIGNAL_DIR = RUNTIME_LIVE_SIGNALS_DIR
BAT_DIR = BASE_DIR / "bat"
KITE_EXPORT_DIR = BASE_DIR / "kite_exports"
V15_NEW_TASK = "EQIDV2_live_combined_csv_v15_new_0900"
FNO_V6_EQUITY_1MIN_FEED_TASK = "EQIDV2_fno_v6_equity_1min_feed_0919"
FNO_LEGACY_PRODUCTION_TASK = "EQIDV2_fno_oi_fetch_5min_0905"
FNO_FAST_PRODUCTION_TASK = "EQIDV2_fno_oi_fetch_5min_fast_production_0905"
FNO_FAST_PRODUCTION_TRIAL_DATE = dt.date(2026, 9, 2)
FNO_FAST_PRODUCTION_START = dt.time(9, 5)
FNO_FAST_PRODUCTION_STARTUP_GRACE_END = dt.time(9, 6)
FNO_FAST_PRODUCTION_FIRST_SLOT = dt.time(9, 20)
FNO_FAST_PRODUCTION_STATUS = (
    LOG_DIR / "fno_oi_fetch_5min_fast_production.supervisor.status"
)
FNO_FAST_PRODUCTION_HEARTBEAT = (
    LOG_DIR / "fno_oi_fetch_5min_fast_production.supervisor.heartbeat"
)
FNO_FAST_PRODUCTION_FIRST_MARKER = (
    RUNTIME_ROOT / "fno_oi" / "slot_ready" / "slot_20260902_0920.json"
)
FNO_V10_V11_V12_PAPER_TASK = "EQIDV2_fno_v10_v11_v12_paper_0915"
FNO_V10_V11_V12_STATUS = (
    RUNTIME_ROOT / "fno_oi" / "multi_strategy_paper_v1" / "status.json"
)
FNO_V6_LIVE_KITE_TASK = "EQIDV2_fno_v6_live_kite_qty1_0915"
FNO_V8_COMBINED_PAPER_TASK = "EQIDV2_fno_v8_combined_paper_0915"
FNO_V8_COMBINED_PAPER_HEARTBEAT = (
    RUNTIME_STATUS_DIR / "fno_v8_combined_paper.heartbeat"
)
FNO_V8_STARTUP_GRACE_END = dt.time(9, 17)
FNO_V6_SCANNER_TASK = "EQIDV2_fno_v6_scanner_5min_0918"
FNO_V6_CUTOVER_DOWNSTREAM_TASKS = (
    FNO_V6_EQUITY_1MIN_FEED_TASK,
    "EQIDV2_fno_v6_confirmation_1min_0919",
    "EQIDV2_fno_v6_live_long_0920",
    "EQIDV2_fno_v6_live_short_0920",
    "EQIDV2_fno_v6_trade_logger_0920",
    "EQIDV2_fno_v6_net_result_0920",
    FNO_V6_LIVE_KITE_TASK,
)

DASHBOARD_SESSION_TASKS = (
    "EQIDV2_log_dashboard_start_0855",
    "EQIDV2_authentication_v2_0900",
    "EQIDV2_eod_5mins_data_0900",
    "EQIDV2_eod_15mins_data_0900",
    "EQIDV2_fno_oi_universe_0850",
    FNO_LEGACY_PRODUCTION_TASK,
    FNO_FAST_PRODUCTION_TASK,
    "EQIDV2_fno_oi_fetch_5min_fast_shadow_0906",
    "EQIDV2_fno_oi_feature_ranker_0915",
    "EQIDV2_fno_v6_scanner_5min_0918",
    "EQIDV2_fno_v6_equity_1min_feed_0919",
    "EQIDV2_fno_v6_confirmation_1min_0919",
    "EQIDV2_fno_v6_live_long_0920",
    "EQIDV2_fno_v6_live_short_0920",
    "EQIDV2_fno_v6_trade_logger_0920",
    "EQIDV2_fno_v6_net_result_0920",
    FNO_V6_LIVE_KITE_TASK,
    FNO_V10_V11_V12_PAPER_TASK,
    "EQIDV2_fno_oi_eod_qc_1540",
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
    "EQIDV2_backtesting_result_v11_1600",
    "EQIDV2_suggestions_v7_live_research_1615",
    "EQIDV2_kite_export_start_0915",
    "EQIDV2_eod_1540_update_1540",
)

# The confirmation consumer has no broker fallback.  Without this producer
# task every V6 confirmation slot must fail closed, so a missing/disabled task
# is a preopen failure rather than an optional inactive session.
REQUIRED_DASHBOARD_SESSION_TASKS = frozenset(
    {FNO_V6_EQUITY_1MIN_FEED_TASK}
)


@dataclass
class CheckResult:
    name: str
    status: str  # PASS | WARN | FAIL
    detail: str


def now_ist() -> dt.datetime:
    return dt.datetime.now(IST)


def check_v8_paper_cutover_activation(
    *,
    v8_task_enabled: bool,
    observed_at: dt.datetime,
    task_probe: Callable[[str], Tuple[bool, bool, bool, str, str]] | None = None,
    v8_task_state: Tuple[bool, bool, bool, str, str] | None = None,
) -> CheckResult:
    """Fail closed when Task Scheduler selects V8 but today's permit is invalid.

    The enabled V8 task is the persistent, scheduler-backed cutover-mode marker.
    While it is enabled, the deliberately disabled V6 one-minute feed must not
    be treated as an autofix target.  A missing/expired V8 permit remains a
    visible failure, but has no automatic start action.
    """

    label = "fno_v8_combined_paper_cutover_activation"
    probe = task_probe or _task_cutover_state
    v8_state = v8_task_state or probe(FNO_V8_COMBINED_PAPER_TASK)
    if not v8_task_enabled:
        exists, enabled, _running, state, _status = v8_state
        if exists and not enabled and state == "DISABLED":
            return CheckResult(label, "PASS", "V8 paper cutover mode not selected")
        _feed_exists, _feed_enabled, _feed_running, feed_state, _ = probe(
            FNO_V6_EQUITY_1MIN_FEED_TASK
        )
        return CheckResult(
            label,
            "FAIL",
            "V8 scheduler identity is missing/unavailable/ambiguous; automatic mode "
            "selection is blocked regardless of V6 feed state: "
            f"v8_state={state or 'UNKNOWN'}, v6_feed_state={feed_state or 'UNKNOWN'}",
        )
    scanner_exists, scanner_enabled, _scanner_running, scanner_state, _ = probe(
        FNO_V6_SCANNER_TASK
    )
    if not scanner_exists or not scanner_enabled:
        return CheckResult(
            label,
            "FAIL",
            f"V8 mode selected but shared V6 scanner is not enabled: {scanner_state or 'MISSING'}",
        )
    conflicts: list[str] = []
    for task_name in FNO_V6_CUTOVER_DOWNSTREAM_TASKS:
        exists, enabled, running, state, status = probe(task_name)
        if not exists or enabled or running:
            conflicts.append(
                f"{task_name}[state={state or 'MISSING'},status={status or 'N/A'}]"
            )
    if conflicts:
        return CheckResult(
            label,
            "FAIL",
            "V8 mode is not mutually exclusive with all V6 downstream tasks: "
            + ", ".join(conflicts),
        )
    try:
        import fno_v8_combined_paper_control as v8_control

        decision = v8_control.evaluate_activation(
            observed_at.astimezone(IST).date(),
            now=observed_at.astimezone(IST),
        )
    except Exception as exc:
        return CheckResult(
            label,
            "FAIL",
            f"V8 task enabled but activation validation errored: {type(exc).__name__}",
        )
    if not decision.allowed:
        return CheckResult(
            label,
            "FAIL",
            f"V8 task enabled but today's PAPER activation is blocked: {decision.reason}",
        )
    return CheckResult(
        label,
        "PASS",
        f"V8 task enabled with valid one-session PAPER permit {decision.permit_id}",
    )


def check_v8_paper_runtime_liveness(
    *,
    v8_task_enabled: bool,
    observed_at: dt.datetime,
    v8_task_state: Tuple[bool, bool, bool, str, str] | None = None,
    heartbeat_path: Path = FNO_V8_COMBINED_PAPER_HEARTBEAT,
    max_heartbeat_age_seconds: float = 120.0,
) -> CheckResult:
    """Expose a selected-but-dead V8 pipeline without any autofix action."""

    label = "fno_v8_combined_paper_runtime_liveness"
    if not v8_task_enabled:
        return CheckResult(label, "PASS", "V8 paper cutover mode not selected")
    if observed_at.astimezone(IST).time() < FNO_V8_STARTUP_GRACE_END:
        return CheckResult(label, "PASS", "V8 paper task is within its 09:15 startup grace")

    state = v8_task_state or _task_cutover_state(FNO_V8_COMBINED_PAPER_TASK)
    exists, enabled, running, scheduled_state, task_status = state
    if not exists or not enabled or not running:
        return CheckResult(
            label,
            "FAIL",
            "V8 mode selected but the 09:15 PAPER task is not running after grace: "
            f"state={scheduled_state or 'MISSING'},status={task_status or 'N/A'}",
        )

    heartbeat = parse_keyfile(Path(heartbeat_path))
    heartbeat_state = str(heartbeat.get("state", "")).strip().upper()
    heartbeat_ts = _parse_keyfile_ts(
        heartbeat.get("ts_utc", "") or heartbeat.get("ts", "")
    )
    if heartbeat_state != "RUNNING" or heartbeat_ts is None:
        return CheckResult(
            label,
            "FAIL",
            "V8 task is running but its PAPER heartbeat is missing/invalid: "
            f"state={heartbeat_state or 'N/A'}",
        )
    observed = observed_at.astimezone(IST)
    age_seconds = (observed - heartbeat_ts.astimezone(IST)).total_seconds()
    if heartbeat_ts.date() != observed.date() or age_seconds < -5.0 or age_seconds > float(
        max_heartbeat_age_seconds
    ):
        return CheckResult(
            label,
            "FAIL",
            "V8 task heartbeat is not a fresh same-session observation: "
            f"age_seconds={age_seconds:.1f}",
        )
    return CheckResult(
        label,
        "PASS",
        f"V8 PAPER task running; heartbeat age={max(0.0, age_seconds):.1f}s",
    )


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


def latest_nifty_v16_5min_log() -> Path:
    today = now_ist().strftime("%Y-%m-%d")
    patterns = (
        f"eqidv2_nifty_guard_fetcher_supervised_v16_5min_{today}*.log",
        "eqidv2_nifty_guard_fetcher_supervised_v16_5min_*.log",
        "nifty_guard_fetcher_v16_5min.log",
    )
    for pattern in patterns:
        try:
            matches = list(LOG_DIR.glob(pattern))
        except OSError:
            matches = []
        if matches:
            return max(matches, key=lambda p: p.stat().st_mtime)
    return LOG_DIR / f"eqidv2_nifty_guard_fetcher_supervised_v16_5min_{today}.log"


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


def check_task_trigger_not_market_hours(task_name: str) -> CheckResult:
    """Fail if the task is ENABLED and its actual trigger fires during market hours.

    The backtesting/data-moving task EQIDV2_data_for_backtesting_1545 once had a
    09:17:30 trigger which ran large file-moving and CPU-intensive work during the
    live session. This check catches the mis-configuration before market open.
    """
    label = f"task_trigger_not_market_hours_{task_name}"
    out = _run_schtasks_query(task_name)
    if not out:
        return CheckResult(label, "PASS", "task not found — no trigger to validate")
    lines = out.splitlines()
    state = _extract_value(lines, "Scheduled Task State")
    if state.strip().upper() != "ENABLED":
        return CheckResult(label, "PASS", f"task disabled — trigger not active (state={state})")
    # Look for Start Time: HH:MM:SS in schtasks /FO LIST /V output
    start_time_str = _extract_value(lines, "Start Time")
    if not start_time_str:
        return CheckResult(label, "WARN", "cannot read trigger time from schtasks output")
    try:
        trigger_time = dt.time.fromisoformat(start_time_str.split()[0])
    except ValueError:
        return CheckResult(label, "WARN", f"cannot parse trigger time: {start_time_str!r}")
    market_open = dt.time(9, 10)
    market_close = dt.time(15, 30)
    if market_open <= trigger_time <= market_close:
        return CheckResult(
            label,
            "FAIL",
            f"ENABLED task fires at {trigger_time} — INSIDE market hours {market_open}–{market_close}. "
            f"Run bat/fix_task_trigger_backtesting_1545.bat to correct.",
        )
    return CheckResult(label, "PASS", f"trigger at {trigger_time} is outside market hours")


def check_bat_exists(path: Path, label: str, detail_prefix: str = "runner wrapper present") -> CheckResult:
    if path.exists():
        return CheckResult(label, "PASS", f"{detail_prefix} | {path.name}")
    return CheckResult(label, "FAIL", f"missing runner wrapper: {path}")


def _run_schtasks_query(task_name: str) -> Optional[str]:
    candidates = [task_name]
    normalized = task_name.strip()
    if normalized:
        bare_name = normalized.lstrip("\\")
        root_name = "\\" + bare_name
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


def _task_cutover_state(task_name: str) -> Tuple[bool, bool, bool, str, str]:
    out = _run_schtasks_query(task_name)
    if not out:
        return False, False, False, "", ""
    lines = out.splitlines()
    state = _extract_value(lines, "Scheduled Task State").strip().upper()
    status = _extract_value(lines, "Status").strip().upper()
    return True, state == "ENABLED", status == "RUNNING", state, status


def _extract_value(lines: List[str], prefix: str) -> str:
    prefix_l = prefix.lower()
    for ln in lines:
        s = ln.strip()
        if s.lower().startswith(prefix_l):
            _, _, tail = s.partition(":")
            return tail.strip()
    return ""


def _task_scheduled_time(task_name: str) -> Optional[dt.time]:
    out = _run_schtasks_query(task_name)
    if out:
        actual = _extract_value(out.splitlines(), "Start Time")
        for fmt in ("%H:%M:%S", "%H:%M", "%I:%M:%S %p", "%I:%M %p"):
            try:
                return dt.datetime.strptime(actual, fmt).time()
            except ValueError:
                continue

    # Keep suffix parsing as a fallback for unavailable or localized schtasks
    # output. Some stable task IDs intentionally retain their former time suffix.
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
    next_run = _extract_value(lines, "Next Run Time")
    last_result = _extract_value(lines, "Last Result")

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

    if status.strip().upper() != "RUNNING" and last_result.strip() not in {"0", "0x0"}:
        return CheckResult(
            label,
            "FAIL",
            f"ran today but completed nonzero | last_result={last_result or 'N/A'} | status={status or 'N/A'}",
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
    last_result = _extract_value(lines, "Last Result")

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

    if status.strip().upper() != "RUNNING" and last_result.strip() not in {"0", "0x0"}:
        return CheckResult(
            label,
            "FAIL",
            f"ran today but completed nonzero | last_result={last_result or 'N/A'} | status={status or 'N/A'}",
        )

    return CheckResult(label, "PASS", f"ran today | status={status or 'N/A'} | last_run={last_run}")


def check_dashboard_session_task(
    task_name: str,
    observed_at: dt.datetime,
    v8_positively_disabled: bool,
) -> CheckResult:
    """Check one dashboard task without ever autofixing V6 behind V8/unknown mode."""

    if task_name in FNO_V6_CUTOVER_DOWNSTREAM_TASKS and not v8_positively_disabled:
        # This deliberately does not use the ``task_`` label namespace because
        # the autofix process maps every such failure to ``schtasks /Run`` and,
        # for the V6 feed, a direct BAT fallback.  V8 enabled *or an ambiguous
        # V8 scheduler query* must therefore suppress this check completely;
        # the separate cutover-coherence check remains the fail-closed alert.
        label = (
            "fno_v6_equity_1min_feed_autofix_suppressed"
            if task_name == FNO_V6_EQUITY_1MIN_FEED_TASK
            else f"fno_v6_downstream_autofix_suppressed_{task_name}"
        )
        return CheckResult(
            label,
            "PASS",
            "V6 downstream task/action checks suppressed until V8 is positively observed Disabled",
        )

    local_date = observed_at.astimezone(IST).date()
    selected_production_task = (
        FNO_FAST_PRODUCTION_TASK
        if local_date == FNO_FAST_PRODUCTION_TRIAL_DATE
        else FNO_LEGACY_PRODUCTION_TASK
    )
    required = (
        task_name in REQUIRED_DASHBOARD_SESSION_TASKS
        or task_name == selected_production_task
    )
    result = check_task_enabled_state(
        task_name,
        f"task_{task_name}",
        require_run_today=_task_should_have_run_today(task_name, observed_at),
        inactive_ok=not required,
        inactive_detail="session not enabled",
    )
    if task_name == FNO_V6_LIVE_KITE_TASK and result.status == "FAIL":
        # This task owns a real broker executor.  Report a scheduler problem,
        # but keep it outside the generic ``task_*`` autofix namespace so the
        # health loop never creates an extra live-execution start attempt.
        return CheckResult(
            "fno_v6_live_kite_manual_review",
            "FAIL",
            result.detail + " | automatic live-executor start suppressed",
        )
    return result


def check_fast_production_trial_runtime(
    observed_at: dt.datetime,
    *,
    status_path: Path = FNO_FAST_PRODUCTION_STATUS,
    heartbeat_path: Path = FNO_FAST_PRODUCTION_HEARTBEAT,
) -> CheckResult:
    """Require same-session supervisor evidence after the Sep-2 startup grace."""

    label = "fno_fast_production_trial_runtime"
    local = observed_at.astimezone(IST)
    if local.date() != FNO_FAST_PRODUCTION_TRIAL_DATE:
        return CheckResult(label, "PASS", "Sep-2 fast-production trial not active today")
    if local.time() < FNO_FAST_PRODUCTION_START:
        return CheckResult(label, "PASS", "fast-production start is scheduled for 09:05")
    if local.time() < FNO_FAST_PRODUCTION_STARTUP_GRACE_END:
        return CheckResult(label, "PASS", "fast-production task is within its 09:05 startup grace")

    status = parse_keyfile(status_path)
    status_value = str(status.get("status", "")).strip().upper()
    status_ts = _parse_keyfile_ts(status.get("ts", ""))
    status_is_today = status_ts is not None and status_ts.date() == local.date()
    terminal = {
        "FAILED", "ERROR", "CRASHED", "STOPPED", "BLOCKED",
        "COOLDOWN_EXHAUSTED", "SKIPPED_CUTOFF",
    }
    if status_is_today and status_value in terminal:
        return CheckResult(
            label,
            "FAIL",
            f"fast-production supervisor terminal status={status_value} @ {status_ts.strftime('%H:%M:%S')}",
        )
    if status_is_today and status_value in {
        "RUNNING", "RESTARTING", "COOLDOWN", "SUCCESS",
    }:
        return CheckResult(
            label,
            "PASS",
            f"fast-production supervisor status={status_value} @ {status_ts.strftime('%H:%M:%S')}",
        )

    heartbeat = parse_keyfile(heartbeat_path)
    heartbeat_state = str(
        heartbeat.get("state", "") or heartbeat.get("status", "")
    ).strip().upper()
    heartbeat_ts = _parse_keyfile_ts(
        heartbeat.get("ts_utc", "") or heartbeat.get("ts", "")
    )
    heartbeat_is_today = heartbeat_ts is not None and heartbeat_ts.date() == local.date()
    if heartbeat_is_today and heartbeat_state in {"RUNNING", "RESTARTING", "COOLDOWN"}:
        return CheckResult(
            label,
            "PASS",
            f"fast-production heartbeat={heartbeat_state} @ {heartbeat_ts.strftime('%H:%M:%S')}",
        )
    return CheckResult(
        label,
        "FAIL",
        "no same-session fast-production RUNNING/SUCCESS supervisor evidence after 09:06",
    )


def check_fast_production_trial_first_slot(
    observed_at: dt.datetime,
    *,
    marker_path: Path = FNO_FAST_PRODUCTION_FIRST_MARKER,
) -> CheckResult:
    """Keep the autofix monitor open until the first canonical stock marker passes."""

    label = "fno_fast_production_trial_first_slot"
    local = observed_at.astimezone(IST)
    if local.date() != FNO_FAST_PRODUCTION_TRIAL_DATE:
        return CheckResult(label, "PASS", "Sep-2 fast-production trial not active today")
    if local.time() < FNO_FAST_PRODUCTION_FIRST_SLOT:
        return CheckResult(
            label,
            "WARN",
            "acceptance pending: waiting for the first canonical 09:20 stock-futures marker",
        )
    try:
        payload = json.loads(marker_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return CheckResult(label, "FAIL", f"first canonical marker missing/invalid: {marker_path}")
    if not isinstance(payload, dict):
        return CheckResult(label, "FAIL", "first canonical marker is not a JSON object")

    expected_slot = "2026-09-02T09:20:00+05:30"
    required_truth = {
        "schema_version": "fno_oi_fetch_slot_v2",
        "slot_ist": expected_slot,
        "universe_date": "2026-09-02",
    }
    mismatches = [
        f"{key}={payload.get(key)!r}"
        for key, expected in required_truth.items()
        if payload.get(key) != expected
    ]
    if not bool(payload.get("stock_complete")):
        mismatches.append("stock_complete=false")
    if str(payload.get("stock_state", "")).strip().upper() != "SUCCESS":
        mismatches.append(f"stock_state={payload.get('stock_state')!r}")
    if mismatches:
        return CheckResult(label, "FAIL", "first marker failed quality gates: " + ", ".join(mismatches))

    published = _parse_keyfile_ts(str(payload.get("published_at_ist", "")))
    if published is None or published.date() != local.date():
        return CheckResult(label, "FAIL", "first marker has missing/stale published_at_ist")
    delay_seconds = (
        published
        - dt.datetime(2026, 9, 2, 9, 20, tzinfo=IST)
    ).total_seconds()
    if delay_seconds < 0 or delay_seconds > 60:
        return CheckResult(
            label,
            "FAIL",
            f"first marker stock-complete but late: publish_delay={delay_seconds:.1f}s (limit=60s)",
        )
    return CheckResult(
        label,
        "PASS",
        f"first canonical marker stock-complete in {delay_seconds:.1f}s",
    )


def check_v10_v11_v12_shared_runtime(
    observed_at: dt.datetime,
    *,
    enabled: bool,
    status_path: Path = FNO_V10_V11_V12_STATUS,
) -> CheckResult:
    """Reject a scheduler-successful but late/NOT_RUN shared paper session."""

    label = "fno_v10_v11_v12_shared_runtime"
    if not enabled:
        return CheckResult(label, "PASS", "shared V10/V11/V12 PAPER session not enabled")
    local = observed_at.astimezone(IST)
    if local.time() < dt.time(9, 17):
        return CheckResult(label, "PASS", "shared PAPER session is within startup grace")
    try:
        payload = json.loads(status_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return CheckResult(label, "FAIL", f"shared PAPER status missing/invalid: {status_path}")
    if not isinstance(payload, dict):
        return CheckResult(label, "FAIL", "shared PAPER status is not a JSON object")
    session_date = str(payload.get("session_date", "")).strip()
    status = str(payload.get("status", "")).strip().upper()
    phase = str(payload.get("phase", "")).strip().upper()
    if session_date != local.date().isoformat():
        return CheckResult(
            label,
            "FAIL",
            f"shared PAPER status is stale: session_date={session_date or 'N/A'}",
        )
    if status in {"NOT_RUN", "BLOCKED", "FAILED", "ERROR"}:
        return CheckResult(
            label,
            "FAIL",
            f"shared PAPER session status={status} phase={phase or 'N/A'}",
        )
    if status not in {"RUNNING", "COMPLETE", "DEGRADED"}:
        return CheckResult(
            label,
            "FAIL",
            f"shared PAPER session has unexpected status={status or 'N/A'}",
        )
    try:
        healthy_apps = int(payload.get("healthy_app_count", 0) or 0)
    except (TypeError, ValueError):
        healthy_apps = 0
    if healthy_apps < 7:
        return CheckResult(
            label,
            "FAIL",
            f"shared PAPER session has only {healthy_apps}/7 required healthy Kite apps",
        )
    return CheckResult(
        label,
        "PASS",
        f"shared PAPER session status={status} phase={phase or 'N/A'} apps={healthy_apps}/7+",
    )


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
    v10_v11_v12_paper_enabled = _task_is_enabled(FNO_V10_V11_V12_PAPER_TASK)
    v8_task_state = _task_cutover_state(FNO_V8_COMBINED_PAPER_TASK)
    v8_paper_enabled = bool(v8_task_state[0] and v8_task_state[1])
    v8_positively_disabled = bool(
        v8_task_state[0]
        and not v8_task_state[1]
        and v8_task_state[3] == "DISABLED"
    )
    now_local = now_ist()
    v15_nifty_log_due = now_local.time() >= dt.time(9, 15)
    v16_5min_nifty_log_due = now_local.time() >= dt.time(9, 15)

    # Dashboard scheduled sessions. Future slots are reported as waiting; past
    # enabled slots must have a same-day scheduler run.
    for task in DASHBOARD_SESSION_TASKS:
        checks.append(
            check_dashboard_session_task(
                task_name=task,
                observed_at=now_local,
                v8_positively_disabled=v8_positively_disabled,
            )
        )

    checks.append(check_fast_production_trial_runtime(now_local))
    checks.append(check_fast_production_trial_first_slot(now_local))
    checks.append(
        check_v10_v11_v12_shared_runtime(
            now_local,
            enabled=v10_v11_v12_paper_enabled,
        )
    )

    # This failure intentionally has no autofix mapping.  If V8 is selected
    # but today's two-key activation is absent/expired, stay fail-closed rather
    # than launching the disabled V6 feed and overlapping entry pipelines.
    checks.append(
        check_v8_paper_cutover_activation(
            v8_task_enabled=v8_paper_enabled,
            observed_at=now_local,
            v8_task_state=v8_task_state,
        )
    )
    checks.append(
        check_v8_paper_runtime_liveness(
            v8_task_enabled=v8_paper_enabled,
            observed_at=now_local,
            v8_task_state=v8_task_state,
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
            latest_nifty_v16_5min_log(),
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

    # Validate that the backtesting/data-moving task does not fire during market hours.
    # The task was once misconfigured with a 09:17:30 trigger instead of 15:45.
    checks.append(check_task_trigger_not_market_hours("EQIDV2_data_for_backtesting_1545"))

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
    overall = "FAIL" if failed else ("WAIT" if warned else "PASS")

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
    failed = any(c.status == "FAIL" for c in checks)
    warned = any(c.status == "WARN" for c in checks)
    payload_json = {
        "ts_ist": _fmt_ts(ts),
        "overall": "FAIL" if failed else ("WAIT" if warned else "PASS"),
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

    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
