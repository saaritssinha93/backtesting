"""Pre-market readiness checks for the V7 live-paper / V11 parity pipeline.

The default mode validates code, launchers, configuration, and scheduled-task
actions without requiring the workers to be running.  Use ``--require-running``
after 09:02 IST to additionally require fresh, conf-backed runtime manifests.
"""

from __future__ import annotations

import argparse
import importlib
import json
import os
import py_compile
import subprocess
import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Iterable

import pytz


IST = pytz.timezone("Asia/Kolkata")
ROOT = Path(__file__).resolve().parent
RUNTIME_ROOT = Path(os.getenv("EQIDV2_RUNTIME_ROOT", r"C:\TradingData\eqidv2"))
OUT_DIR = RUNTIME_ROOT / "runtime_status"
EXPECTED_CONF_MODULE = "final_setup_conf_v11_working"
EXPECTED_FEATURE_VERSION = "shared_immutable_slot_features_v1"
EXPECTED_ACTIVE_SETUPS = {
    "A_MOD_BREAK_C1_LOW",
    "A_PULLBACK_C2_THEN_BREAK_C2_LOW",
    "B_HUGE_RED_FAILED_BOUNCE",
    "C_OR_BREAKDOWN",
    "DOC5D_AVWAP_RECLAIM_LONG",
    "D_EMA20_REJECTION",
    "E_ORB_BREAKOUT_LONG",
    "G_HIGHER_HIGH_BREAK",
    "G_LOWER_LOW_BREAK",
    "L_DOUBLE_BOTTOM_VWAP",
    "S9_MIDDAY_LOSE",
}

TASK_ACTIONS = {
    r"\EQIDV2_signal_discovery_v7_5mins_ID": "run_conf_paper_signal_discovery.bat",
    r"\EQIDV2_entry_engine_1min_v5_ID": "run_conf_paper_entry_engine.bat",
    r"\EQIDV2_paper_trade_id_5min_v7_0900": "run_conf_paper_executor.bat",
}

LAUNCHER_REQUIREMENTS = {
    "bat/run_conf_paper_signal_discovery.bat": (
        "EQIDV2_USE_FINAL_SETUP_CONF=1",
        f"EQIDV2_FINAL_SETUP_CONF_MODULE={EXPECTED_CONF_MODULE}",
        f"EQIDV2_EXPECT_FINAL_SETUP_CONF_MODULE={EXPECTED_CONF_MODULE}",
        "EQIDV2_LAUNCHER_NAME=run_conf_paper_signal_discovery.bat",
    ),
    "bat/run_conf_paper_entry_engine.bat": (
        "EQIDV2_USE_FINAL_SETUP_CONF=1",
        f"EQIDV2_FINAL_SETUP_CONF_MODULE={EXPECTED_CONF_MODULE}",
        f"EQIDV2_EXPECT_FINAL_SETUP_CONF_MODULE={EXPECTED_CONF_MODULE}",
        "EQIDV2_LAUNCHER_NAME=run_conf_paper_entry_engine.bat",
        "EQIDV2_ENTRY_ENGINE_USE_SLOT_CANDIDATE_JSON=1",
        "EQIDV2_ENTRY_ENGINE_REQUIRE_SLOT_COMPLETE_MARKER=1",
        "EQIDV2_ENTRY_ENGINE_1MIN_V5_ID_CANDIDATE_WAIT_POLL_SEC=0.25",
        "EQIDV2_ENTRY_ENGINE_1MIN_V5_ID_POLL_SEC=0.25",
        "EQIDV2_ENTRY_ENGINE_RAW_FETCH_APP_COUNT=8",
        "EQIDV2_ENTRY_ENGINE_WRITE_SLA_SEC=10",
    ),
    "bat/run_conf_paper_executor.bat": (
        "EQIDV2_USE_FINAL_SETUP_CONF=1",
        f"EQIDV2_FINAL_SETUP_CONF_MODULE={EXPECTED_CONF_MODULE}",
        f"EQIDV2_EXPECT_FINAL_SETUP_CONF_MODULE={EXPECTED_CONF_MODULE}",
        "EQIDV2_LAUNCHER_NAME=run_conf_paper_executor.bat",
        "EQIDV2_MAX_CONCURRENT_TRADES=20",
        "EQIDV2_MAX_OPEN_POSITIONS=20",
        "EQIDV2_MAX_CAPITAL_DEPLOYED_RS=2000000",
    ),
    "bat/run_backtesting_result_v11_1600.bat": (
        "EQIDV2_USE_FINAL_SETUP_CONF=1",
        f"EQIDV2_FINAL_SETUP_CONF_MODULE={EXPECTED_CONF_MODULE}",
        f"EQIDV2_EXPECT_FINAL_SETUP_CONF_MODULE={EXPECTED_CONF_MODULE}",
        "EQIDV2_LAUNCHER_NAME=run_backtesting_result_v11_1600.bat",
    ),
}

PYTHON_SOURCES = (
    "eqidv2_runtime_manifest.py",
    "eqidv2_pre_momentum.py",
    "eqidv2_decision_funnel.py",
    "avwap_5min_ID_v7_candidate_scan.py",
    "eqidv2_signal_discovery_v7_5min_id_persistent.py",
    "eqidv2_entry_engine_1min_v5_id.py",
    "avwap_trade_execution_PAPER_TRADE_TRUE_id_5min_v7.py",
    "avwap_5min_ID_v11_backtesting.py",
    "backtesting_result_v11_daily.py",
    "log_dashboard_server.py",
)

RUNTIME_COMPONENTS = {
    "scanner": ("signal_discovery_v7_5mins_ID", "run_conf_paper_signal_discovery.bat"),
    "entry": ("entry_engine_1min_v5_ID", "run_conf_paper_entry_engine.bat"),
    "paper": ("paper_trade_executor_id_5min_v7", "run_conf_paper_executor.bat"),
}


@dataclass
class Check:
    name: str
    status: str
    detail: str


def _check(checks: list[Check], name: str, ok: bool, detail: str) -> None:
    checks.append(Check(name, "PASS" if ok else "FAIL", detail))


def _warn(checks: list[Check], name: str, detail: str) -> None:
    checks.append(Check(name, "WARN", detail))


def _task_action(task_name: str) -> tuple[str, str]:
    proc = subprocess.run(
        ["schtasks", "/Query", "/TN", task_name, "/XML"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=10,
        check=False,
    )
    if proc.returncode:
        return "", (proc.stderr or proc.stdout or f"exit={proc.returncode}").strip()
    try:
        root = ET.fromstring(proc.stdout.lstrip("\ufeff"))
        commands = [
            str(node.text or "").strip()
            for node in root.iter()
            if node.tag.rsplit("}", 1)[-1] == "Command"
        ]
        arguments = [
            str(node.text or "").strip()
            for node in root.iter()
            if node.tag.rsplit("}", 1)[-1] == "Arguments"
        ]
        action = " ".join(commands + arguments).strip()
        return action, ""
    except ET.ParseError as exc:
        return "", f"invalid task XML: {exc}"


def _validate_launchers(checks: list[Check]) -> None:
    for relative, required in LAUNCHER_REQUIREMENTS.items():
        path = ROOT / relative
        if not path.is_file():
            _check(checks, f"launcher:{Path(relative).name}", False, f"missing {path}")
            continue
        text = path.read_text(encoding="utf-8", errors="replace").casefold()
        missing = [token for token in required if token.casefold() not in text]
        _check(
            checks,
            f"launcher:{Path(relative).name}",
            not missing,
            "contract present" if not missing else f"missing tokens: {missing}",
        )


def _validate_sources(checks: list[Check]) -> None:
    failures: list[str] = []
    for relative in PYTHON_SOURCES:
        path = ROOT / relative
        try:
            py_compile.compile(str(path), doraise=True)
        except Exception as exc:  # pragma: no cover - exact compiler text varies
            failures.append(f"{relative}: {exc}")
    _check(
        checks,
        "python_compile",
        not failures,
        f"{len(PYTHON_SOURCES)} sources compile" if not failures else "; ".join(failures),
    )


def _validate_conf_and_features(checks: list[Check]) -> None:
    try:
        conf_module = importlib.import_module(EXPECTED_CONF_MODULE)
        setup_conf = getattr(conf_module, "FINAL_SETUP_CONF", None)
        actual_setups = set(setup_conf) if isinstance(setup_conf, dict) else set()
        _check(
            checks,
            "final_setup_conf",
            actual_setups == EXPECTED_ACTIVE_SETUPS,
            f"module={EXPECTED_CONF_MODULE}, setup_count={len(actual_setups)}, "
            f"missing={sorted(EXPECTED_ACTIVE_SETUPS - actual_setups)}, "
            f"unexpected={sorted(actual_setups - EXPECTED_ACTIVE_SETUPS)}",
        )
    except Exception as exc:
        _check(checks, "final_setup_conf", False, repr(exc))

    try:
        feature_module = importlib.import_module("eqidv2_pre_momentum")
        actual = str(getattr(feature_module, "FEATURE_VERSION", ""))
        _check(
            checks,
            "shared_feature_version",
            actual == EXPECTED_FEATURE_VERSION,
            f"resolved={actual or '<empty>'}",
        )
    except Exception as exc:
        _check(checks, "shared_feature_version", False, repr(exc))


def _validate_tasks(checks: list[Check]) -> None:
    for task_name, expected_bat in TASK_ACTIONS.items():
        action, error = _task_action(task_name)
        ok = bool(action) and expected_bat.casefold() in action.casefold()
        detail = error or f"action={action}"
        _check(checks, f"scheduled_task:{task_name.lstrip(chr(92))}", ok, detail)


def _validate_dashboard_routes(checks: list[Check]) -> None:
    path = ROOT / "log_dashboard_server.py"
    text = path.read_text(encoding="utf-8", errors="replace")
    missing = [
        f'"{card}": "{expected}"'
        for card, expected in (
            ("signal_discovery_v7_5min_id", "run_conf_paper_signal_discovery.bat"),
            ("entry_engine_1min_v5_id", "run_conf_paper_entry_engine.bat"),
            ("paper_trade_id_5min_v7", "run_conf_paper_executor.bat"),
        )
        if f'"{card}": "{expected}"' not in text
    ]
    _check(
        checks,
        "dashboard_restart_routes",
        not missing,
        "all V7 paper routes use conf launchers" if not missing else f"missing: {missing}",
    )


def _validate_runtime_manifests(checks: list[Check], require_running: bool) -> None:
    today = datetime.now(tz=IST).date().isoformat()
    for label, (component, expected_launcher) in RUNTIME_COMPONENTS.items():
        path = RUNTIME_ROOT / "runtime_manifests" / component / "latest.json"
        name = f"runtime_manifest:{label}"
        if not path.is_file():
            detail = f"not created yet: {path}"
            if require_running:
                _check(checks, name, False, detail)
            else:
                _warn(checks, name, detail)
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            created = str(payload.get("created_at_ist", ""))
            launcher = str(payload.get("launcher_name", ""))
            pid = int(payload.get("pid", 0) or 0)
            pid_alive = False
            if pid > 0:
                proc = subprocess.run(
                    ["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"],
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    timeout=5,
                    check=False,
                )
                pid_alive = (
                    proc.returncode == 0
                    and str(pid) in proc.stdout
                    and "No tasks are running" not in proc.stdout
                )
            contract = payload.get("final_setup_conf_contract", {}) or {}
            ok = (
                created[:10] == today
                and launcher.casefold() == expected_launcher.casefold()
                and contract.get("enabled") is True
                and contract.get("module") == EXPECTED_CONF_MODULE
                and set(contract.get("setup_keys", [])) == EXPECTED_ACTIVE_SETUPS
                and (pid_alive or not require_running)
            )
            detail = (
                f"created={created}, pid={pid}, pid_alive={pid_alive}, "
                f"launcher={launcher or '<empty>'}, "
                f"conf={contract.get('module')}, setups={contract.get('setup_count')}"
            )
            if ok:
                _check(checks, name, True, detail)
            elif require_running:
                _check(checks, name, False, detail)
            else:
                _warn(checks, name, detail)
        except Exception as exc:
            _check(checks, name, False, f"{path}: {exc}")


def _write_report(checks: Iterable[Check], require_running: bool) -> Path:
    checks_list = list(checks)
    now = datetime.now(tz=IST)
    counts = {
        status: sum(item.status == status for item in checks_list)
        for status in ("PASS", "WARN", "FAIL")
    }
    payload = {
        "schema_version": "v7_v11_parity_preflight_v1",
        "created_at_ist": now.isoformat(),
        "require_running": bool(require_running),
        "overall_state": "FAIL" if counts["FAIL"] else "READY",
        "counts": counts,
        "checks": [asdict(item) for item in checks_list],
    }
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    stamped = OUT_DIR / f"v7_v11_parity_preflight_{now:%Y%m%d_%H%M%S}.json"
    latest = OUT_DIR / "v7_v11_parity_preflight_latest.json"
    rendered = json.dumps(payload, indent=2, sort_keys=True)
    stamped.write_text(rendered, encoding="utf-8")
    latest.write_text(rendered, encoding="utf-8")
    return latest


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--require-running",
        action="store_true",
        help="Require today's live workers to have fresh conf-backed manifests.",
    )
    args = parser.parse_args()

    checks: list[Check] = []
    _validate_sources(checks)
    _validate_conf_and_features(checks)
    _validate_launchers(checks)
    _validate_tasks(checks)
    _validate_dashboard_routes(checks)
    _validate_runtime_manifests(checks, args.require_running)
    report_path = _write_report(checks, args.require_running)

    for item in checks:
        print(f"[{item.status}] {item.name}: {item.detail}")
    failures = sum(item.status == "FAIL" for item in checks)
    warnings = sum(item.status == "WARN" for item in checks)
    state = "FAIL" if failures else "READY"
    print(f"[{state}] failures={failures} warnings={warnings} report={report_path}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
