#!/usr/bin/env python3
"""Simple secured log dashboard for EQIDV2 scheduled jobs."""

from __future__ import annotations

import argparse
import base64
import csv
import datetime as dt
import json
import math
import os
import re
import subprocess
import time
from collections import deque
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Sequence, Set, Tuple
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo
from eqidv2_runtime_paths import (
    LIVE_SIGNALS_DIR as RUNTIME_LIVE_SIGNALS_DIR,
    RUNTIME_STATUS_DIR,
    runtime_dir,
)

BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "logs"


def _resolve_status_path(filename: str) -> Path:
    # v16_5min scripts now write runtime status off OneDrive; older scripts
    # still write under logs/. Prefer the off-OneDrive copy when present.
    off_onedrive = RUNTIME_STATUS_DIR / filename
    if off_onedrive.exists():
        return off_onedrive
    return LOG_DIR / filename
LIVE_SIGNAL_DIR = RUNTIME_LIVE_SIGNALS_DIR
SIGNAL_DISCOVERY_V7_ROOT = runtime_dir("signal_discovery_v7_5mins_ID")
SIGNAL_DISCOVERY_V7_LATEST_DIR = SIGNAL_DISCOVERY_V7_ROOT / "latest"
SIGNAL_DISCOVERY_V7_CSV_DIR = SIGNAL_DISCOVERY_V7_ROOT / "csv"
SLOT_READY_5M_DIR = runtime_dir("slot_ready_5m")
V7_RESEARCH_LAYER_ROOT = runtime_dir("live_research_v7_research_layer")
V7_RESEARCH_LAYER_LATEST_DIR = V7_RESEARCH_LAYER_ROOT / "latest"
DAILY_LIVE_V7_RESEARCH_ROOT = runtime_dir("daily_live_v7_research_session")
DAILY_LIVE_V7_RESEARCH_LATEST_DIR = DAILY_LIVE_V7_RESEARCH_ROOT / "latest"
V7_PRE_MOMENTUM_FILTER_ANALYST_ROOT = runtime_dir("v7_pre_momentum_filter_analyst")
V7_PRE_MOMENTUM_FILTER_ANALYST_LATEST_DIR = V7_PRE_MOMENTUM_FILTER_ANALYST_ROOT / "latest"
KITE_EXPORT_DIR = BASE_DIR / "kite_exports"
IST = ZoneInfo("Asia/Kolkata")
OPEN_LIVE_TRADES_STATE_PATTERN_V5 = "open_live_trades_state_{}_v5.json"
OPEN_PAPER_TRADES_STATE_PATTERN_V5 = "open_trades_state_{}_v5.json"
OPEN_LIVE_TRADES_STATE_PATTERN_V7_SWEEP = "open_live_trades_state_{}_v7_sweep.json"
OPEN_PAPER_TRADES_STATE_PATTERN_V7_SWEEP = "open_trades_state_{}_v7_sweep.json"
OPEN_LIVE_TRADES_STATE_PATTERN_V15 = "open_live_trades_state_{}_v15_new.json"
OPEN_PAPER_TRADES_STATE_PATTERN_V15 = "open_trades_state_{}_v15_new.json"
OPEN_LIVE_TRADES_STATE_PATTERN_V16_5MIN = "open_live_trades_state_{}_v16_5min.json"
OPEN_PAPER_TRADES_STATE_PATTERN_V16_5MIN = "open_trades_state_{}_v16_5min.json"
KILL_SWITCH_LIVE_FILE_V5 = LIVE_SIGNAL_DIR / "kill_switch_false_v5.json"
KILL_SWITCH_PAPER_FILE_V5 = LIVE_SIGNAL_DIR / "kill_switch_true_v5.json"
KILL_SWITCH_LIVE_FILE_V7_SWEEP = LIVE_SIGNAL_DIR / "kill_switch_false_v7_sweep.json"
KILL_SWITCH_PAPER_FILE_V7_SWEEP = LIVE_SIGNAL_DIR / "kill_switch_true_v7_sweep.json"
KILL_SWITCH_LIVE_FILE_V15 = LIVE_SIGNAL_DIR / "kill_switch_false_v15_new.json"
KILL_SWITCH_PAPER_FILE_V15 = LIVE_SIGNAL_DIR / "kill_switch_true_v15_new.json"
KILL_SWITCH_LIVE_FILE_V16_5MIN = LIVE_SIGNAL_DIR / "kill_switch_false_v16_5min.json"
KILL_SWITCH_PAPER_FILE_V16_5MIN = LIVE_SIGNAL_DIR / "kill_switch_true_v16_5min.json"
V15_SHORT_SHARD_IDS: Tuple[str, ...] = tuple(f"{idx:02d}" for idx in range(1, 11))
V15_LONG_SHARD_IDS: Tuple[str, ...] = tuple(f"{idx:02d}" for idx in range(1, 11))
HIDDEN_CARD_IDS = {
    "live_combined_csv_v5_unified",
    "live_combined_csv_v5_short",
    "live_combined_csv_v5_long",
    "live_combined_csv_v7_sweep_short",
    "live_combined_csv_v7_sweep_long",
    "live_signals_csv_v5_short",
    "live_signals_csv_v5_long",
    "live_signals_csv_v7_sweep_short",
    "live_signals_csv_v7_sweep_long",
    "paper_trade_v5",
    "paper_trade_v7_sweep",
    "live_papertrade_result_csv_v5",
    "live_papertrade_result_csv_v7_sweep",
    "kite_trade",
    "live_kite_trades_csv",
    "kite_trade_v7_sweep",
    "live_kite_trades_csv_v7_sweep",
    "live_signals_csv_v15_short",
    "live_signals_csv_v15_long",
    *{f"live_combined_csv_v15_short_s{shard_id}" for shard_id in V15_SHORT_SHARD_IDS},
    *{f"live_combined_csv_v15_long_s{shard_id}" for shard_id in V15_LONG_SHARD_IDS},
}

LOG_FILES: Dict[str, str] = {
    "authentication_v2": "authentication_v2_runner.log",
    "live_combined_csv_v5_unified": "eqidv2_live_combined_analyser_csv_v5_unified.log",
    "eod_5min_data": "eqidv2_eod_scheduler_for_5mins_data_live_minimal.log",
    "eod_15min_data": "eqidv2_eod_scheduler_for_15mins_data_live_minimal.log",
    "eod_1540_update": "eqidv2_eod_scheduler_for_1540_update.log",
    "live_combined_csv_v5_short": "eqidv2_live_combined_analyser_csv_v5_short.log",
    "live_combined_csv_v5_long": "eqidv2_live_combined_analyser_csv_v5_long.log",
    "live_combined_csv_v7_sweep_short": "eqidv2_live_combined_analyser_csv_v7_sweep_short.log",
    "live_combined_csv_v7_sweep_long": "eqidv2_live_combined_analyser_csv_v7_sweep_long.log",
    "nifty_guard_fetch_v15": "nifty_guard_fetcher_v15.log",
    "nifty_guard_fetch_v16_5min": "nifty_guard_fetcher_v16_5min.log",
    "live_combined_csv_v15_new_persistent": "eqidv2_live_combined_analyser_csv_v15_new_persistent.log",
    "live_combined_csv_id_5min_v7_persistent": "eqidv2_live_combined_analyser_csv_id_5min_v7_persistent.log",
    "live_combined_csv_v16_5min":      "eqidv2_live_combined_analyser_csv_v16_5min.log",
    "signal_discovery_v7_5min_id":     "signal_discovery_v7_5mins_ID/heartbeat/candidate_tickers.status.json",
    "entry_engine_1min_v5_id":          "entry_engine_1min_v5_ID/heartbeat/entry_engine.status.json",
    "v7_research_layer":                "live_research_v7_research_layer/latest/latest_summary.json",
    "daily_live_v7_research_session":   "daily_live_v7_research_session/latest/latest_daily_live_v7_research.md",
    "v7_pre_momentum_filter_analyst":    "v7_pre_momentum_filter_analyst/latest/latest_v7_pre_momentum_filter_analyst.md",
    "data_for_backtesting":             "data_for_backtesting_latest.log",
    "backtesting_result_v11":            "backtesting_result_v11_latest.log",
    "signal_early_engine_v16_5min":    "eqidv2_signal_early_engine_v16_5min.log",
    "pending_data_fetcher_v16_5min":   "eqidv2_pending_data_fetcher_v16_5min.log",
    "detection_engine_v16_5min":       "eqidv2_detection_engine_v16_5min.log",
}
LOG_IDS = tuple(LOG_FILES.keys()) + (
    "paper_trade_v5",
    "paper_trade_v7_sweep",
    "paper_trade_v15",
    "paper_trade_v16_5min",
    "paper_trade_id_5min_v7",
    "kite_trade",
    "kite_trade_v7_sweep",
    "kite_trade_v15",
    "kite_trade_v16_5min",
    "kite_trade_id_5min_v7",
    "preopen_healthcheck",
    "pending_signals_v16_5min",
    "detected_signals_v16_5min",
)

STATUS_FILES: Dict[str, str] = {
    "authentication_v2": "authentication_v2_runner.status",
    "live_combined_csv_v5_unified": "eqidv2_live_combined_analyser_csv_v5_unified.status",
    "eod_5min_data": "eqidv2_eod_scheduler_for_5mins_data_live_minimal.supervisor.status",
    "live_combined_csv_v5_short": "eqidv2_live_combined_analyser_csv_v5_short.status",
    "live_combined_csv_v5_long": "eqidv2_live_combined_analyser_csv_v5_long.status",
    "live_combined_csv_v7_sweep_short": "eqidv2_live_combined_analyser_csv_v7_sweep_short.status",
    "live_combined_csv_v7_sweep_long": "eqidv2_live_combined_analyser_csv_v7_sweep_long.status",
    "kite_trade_v7_sweep": "avwap_trade_execution_PAPER_TRADE_FALSE_v7_sweep.status",
    "kite_trade_v15": "avwap_trade_execution_PAPER_TRADE_FALSE_v15_new.status",
    "kite_trade_v16_5min": "avwap_trade_execution_PAPER_TRADE_FALSE_v16_5min.status",
    "kite_trade_id_5min_v7": "avwap_trade_execution_PAPER_TRADE_FALSE_id_5min_v7.status",
    "nifty_guard_fetch_v15": "nifty_guard_fetcher_v15.status",
    "nifty_guard_fetch_v16_5min": "nifty_guard_fetcher_v16_5min.status",
    "live_combined_csv_v15_new_persistent": "eqidv2_live_combined_analyser_csv_v15_new_persistent.status",
    "live_combined_csv_id_5min_v7_persistent": "eqidv2_live_combined_analyser_csv_id_5min_v7_persistent.status",
    "live_combined_csv_v16_5min":      "eqidv2_live_combined_analyser_csv_v16_5min.status",
    "signal_discovery_v7_5min_id":     "signal_discovery_v7_5mins_ID.status",
    "entry_engine_1min_v5_id":          "entry_engine_1min_v5_ID.status",
    "v7_research_layer":                "live_research_v7_research_layer.status",
    "daily_live_v7_research_session":   "daily_live_v7_research_session.status",
    "v7_pre_momentum_filter_analyst":    "v7_pre_momentum_filter_analyst.status",
    "signal_early_engine_v16_5min":    "eqidv2_signal_early_engine_v16_5min.status",
    "pending_data_fetcher_v16_5min":   "eqidv2_pending_data_fetcher_v16_5min.status",
    "detection_engine_v16_5min":       "eqidv2_detection_engine_v16_5min.status",
}

HEARTBEAT_FILES: Dict[str, str] = {
    "eod_5min_data": "eqidv2_eod_scheduler_for_5mins_data_live_minimal.supervisor.heartbeat",
    "kite_trade_v7_sweep": "avwap_trade_execution_PAPER_TRADE_FALSE_v7_sweep.heartbeat",
    "kite_trade_v15": "avwap_trade_execution_PAPER_TRADE_FALSE_v15_new.heartbeat",
    "kite_trade_v16_5min": "avwap_trade_execution_PAPER_TRADE_FALSE_v16_5min.heartbeat",
    "kite_trade_id_5min_v7": "avwap_trade_execution_PAPER_TRADE_FALSE_id_5min_v7.heartbeat",
    "live_combined_csv_v15_new_persistent": "eqidv2_live_combined_analyser_csv_v15_new_persistent.heartbeat",
    "live_combined_csv_id_5min_v7_persistent": "eqidv2_live_combined_analyser_csv_id_5min_v7_persistent.heartbeat",
    "live_combined_csv_v16_5min":      "eqidv2_live_combined_analyser_csv_v16_5min.heartbeat",
    "signal_discovery_v7_5min_id":     "signal_discovery_v7_5mins_ID.heartbeat",
    "entry_engine_1min_v5_id":          "entry_engine_1min_v5_ID.heartbeat",
    "v7_research_layer":                "live_research_v7_research_layer.heartbeat",
    "daily_live_v7_research_session":   "daily_live_v7_research_session.heartbeat",
    "v7_pre_momentum_filter_analyst":    "v7_pre_momentum_filter_analyst.heartbeat",
    "signal_early_engine_v16_5min":    "eqidv2_signal_early_engine_v16_5min.heartbeat",
    "pending_data_fetcher_v16_5min":   "eqidv2_pending_data_fetcher_v16_5min.heartbeat",
    "detection_engine_v16_5min":       "eqidv2_detection_engine_v16_5min.heartbeat",
}

CARD_TASK_NAMES: Dict[str, Tuple[str, ...]] = {
    "authentication_v2": ("\\EQIDV2_authentication_v2_0900",),
    "eod_5min_data": ("\\EQIDV2_eod_5mins_data_0900",),
    "eod_15min_data": ("\\EQIDV2_eod_15mins_data_0900",),
    "eod_1540_update": ("\\EQIDV2_eod_1540_update_1540",),
    "nifty_guard_fetch_v15": ("\\EQIDV2_nifty_guard_fetch_v15_0915",),
    "nifty_guard_fetch_v16_5min": ("\\EQIDV2_nifty_guard_fetch_v16_5min_0915",),
    "live_combined_csv_v15_new_persistent": ("\\EQIDV2_live_combined_csv_v15_new_0900",),
    "live_signals_csv_v15_new_short": ("\\EQIDV2_live_combined_csv_v15_new_0900",),
    "live_signals_csv_v15_new_long": ("\\EQIDV2_live_combined_csv_v15_new_0900",),
    "live_combined_csv_id_5min_v7_persistent": ("\\EQIDV2_live_combined_csv_id_5min_v7_0909",),
    "signal_discovery_v7_5min_id": ("\\EQIDV2_signal_discovery_v7_5mins_ID",),
    "candidate_tickers_v7_5min_id": ("\\EQIDV2_signal_discovery_v7_5mins_ID",),
    "entry_engine_1min_v5_id": ("\\EQIDV2_entry_engine_1min_v5_ID",),
    "v7_research_layer": (
        "\\EQIDV2_v7_research_layer_0917",
        "\\EQIDV2_suggestions_v7_live_research_1615",
    ),
    "daily_live_v7_research_session": ("\\EQIDV2_daily_live_v7_research_0917",),
    "v7_pre_momentum_filter_analyst": ("\\EQIDV2_v7_pre_momentum_filter_analyst_0917",),
    "live_signals_csv_id_5min_v7_short": ("\\EQIDV2_entry_engine_1min_v5_ID",),
    "live_signals_csv_id_5min_v7_long": ("\\EQIDV2_entry_engine_1min_v5_ID",),
    "paper_trade_id_5min_v7": ("\\EQIDV2_paper_trade_id_5min_v7_0900",),
    "live_papertrade_result_csv_id_5min_v7": ("\\EQIDV2_paper_trade_id_5min_v7_0900",),
    "kite_trade_id_5min_v7": ("\\EQIDV2_live_trade_id_5min_v7_0900",),
    "live_kite_trades_csv_id_5min_v7": ("\\EQIDV2_live_trade_id_5min_v7_0900",),
    "data_for_backtesting": ("\\EQIDV2_data_for_backtesting_1545",),
    "backtesting_result_v11": ("\\EQIDV2_backtesting_result_v11_1600",),
    "paper_trade_v15": ("\\EQIDV2_avwap_paper_trade_v15_0900",),
    "live_papertrade_result_csv_v15": ("\\EQIDV2_avwap_paper_trade_v15_0900",),
    "kite_trade_v15": ("\\EQIDV2_avwap_live_trade_v15_0905",),
    "live_kite_trades_csv_v15": ("\\EQIDV2_avwap_live_trade_v15_0905",),
    "live_combined_csv_v16_5min": ("\\EQIDV2_live_combined_csv_v16_5min_0900",),
    "live_signals_csv_v16_5min_short": ("\\EQIDV2_detection_engine_v16_5min_0900",),
    "live_signals_csv_v16_5min_long": ("\\EQIDV2_detection_engine_v16_5min_0900",),
    "paper_trade_v16_5min": ("\\EQIDV2_paper_trade_v16_5min_0900",),
    "live_papertrade_result_csv_v16_5min": ("\\EQIDV2_paper_trade_v16_5min_0900",),
    "kite_trade_v16_5min": ("\\EQIDV2_live_trade_v16_5min_0900",),
    "live_kite_trades_csv_v16_5min": ("\\EQIDV2_live_trade_v16_5min_0900",),
    "preopen_healthcheck": ("\\EQIDV2_preopen_session_healthcheck_0905",),
    "kite_holdings_today_csv": ("\\EQIDV2_kite_export_start_0915",),
    "kite_positions_day_today_csv": ("\\EQIDV2_kite_export_start_0915",),
    "signal_early_engine_v16_5min":  ("\\EQIDV2_signal_early_engine_v16_5min_0900",),
    "pending_signals_v16_5min":      ("\\EQIDV2_signal_early_engine_v16_5min_0900",),
    "pending_data_fetcher_v16_5min": ("\\EQIDV2_pending_data_fetcher_v16_5min_0900",),
    "detection_engine_v16_5min":     ("\\EQIDV2_detection_engine_v16_5min_0900",),
    "detected_signals_v16_5min":     ("\\EQIDV2_detection_engine_v16_5min_0900",),
}

_TASK_SNAPSHOT_CACHE: Dict[str, Dict[str, str]] = {}
_TASK_SNAPSHOT_CACHE_AT: Optional[dt.datetime] = None

RESTARTABLE_CARDS: Dict[str, str] = {
    "nifty_guard_fetch_v16_5min":    "run_nifty_guard_fetcher_v16_5min.bat",
    "eod_5min_data":                 "run_eqidv2_eod_scheduler_for_5mins_data_live_minimal.bat",
    "signal_early_engine_v16_5min":  "run_eqidv2_signal_early_engine_v16_5min.bat",
    "detection_engine_v16_5min":     "run_eqidv2_detection_engine_v16_5min.bat",
    "pending_data_fetcher_v16_5min": "run_eqidv2_pending_data_fetcher_v16_5min.bat",
    "entry_engine_1min_v5_id": "run_eqidv2_entry_engine_1min_v5_id.bat",
    "v7_pre_momentum_filter_analyst": "run_eqidv2_v7_pre_momentum_filter_analyst.bat",
    "kite_positions_day_today_csv":  "run_zerodha_kite_export_scheduler.bat",
    "kite_holdings_today_csv":       "run_zerodha_kite_export_scheduler.bat",
    "authentication_v2":             "run_authentication_v2.bat",
    "preopen_healthcheck":           "run_preopen_session_healthcheck.bat",
    "backtesting_result_v11":        "run_backtesting_result_v11_1600.bat",
}


def _run_cmd_silent(cmd: Sequence[str], timeout: float = 5.0) -> Tuple[int, str]:
    kwargs: Dict[str, Any] = {
        "capture_output": True,
        "text": True,
        "timeout": timeout,
    }
    if os.name == "nt":
        kwargs["creationflags"] = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    try:
        result = subprocess.run(list(cmd), **kwargs)
        combined = (result.stdout or "") + (result.stderr or "")
        return result.returncode, combined.strip()
    except subprocess.TimeoutExpired:
        return 124, "timeout"
    except FileNotFoundError as exc:
        return 127, str(exc)
    except Exception as exc:
        return 1, str(exc)


def _find_bat_process_pids(bat_basename: str) -> list[int]:
    if not bat_basename:
        return []
    # Escape single quotes for PowerShell literal
    safe = bat_basename.replace("'", "''")
    ps_cmd = (
        "Get-CimInstance Win32_Process -Filter \"Name='cmd.exe' OR "
        "Name='powershell.exe' OR Name='python.exe' OR Name='pythonw.exe' OR Name='conhost.exe'\" "
        f"| Where-Object {{ $_.CommandLine -and $_.CommandLine -like '*{safe}*' }} "
        "| Select-Object -ExpandProperty ProcessId"
    )
    rc, out = _run_cmd_silent(
        ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", ps_cmd],
        timeout=6.0,
    )
    if rc != 0 or not out:
        return []
    pids: list[int] = []
    for line in out.splitlines():
        token = line.strip()
        if token.isdigit():
            pids.append(int(token))
    return pids


def _parse_pid_value(value: object) -> Optional[int]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        pid = int(text)
    except (TypeError, ValueError):
        return None
    return pid if pid > 0 else None


def _find_process_pids_by_token(token: str) -> list[int]:
    if not token:
        return []
    safe = token.replace("'", "''")
    ps_cmd = (
        "Get-CimInstance Win32_Process -Filter \"Name='cmd.exe' OR "
        "Name='powershell.exe' OR Name='python.exe' OR Name='pythonw.exe' OR Name='conhost.exe'\" "
        f"| Where-Object {{ $_.CommandLine -and $_.CommandLine -like '*{safe}*' }} "
        "| Select-Object -ExpandProperty ProcessId"
    )
    rc, out = _run_cmd_silent(
        ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", ps_cmd],
        timeout=6.0,
    )
    if rc != 0 or not out:
        return []
    pids: list[int] = []
    for line in out.splitlines():
        item = line.strip()
        if item.isdigit():
            pids.append(int(item))
    return pids


def _list_alive_pids(pids: Sequence[int]) -> list[int]:
    clean = sorted({int(pid) for pid in pids if int(pid) > 0})
    if not clean:
        return []
    joined = ",".join(str(pid) for pid in clean)
    ps_cmd = (
        f"$ids=@({joined}); "
        "Get-Process -Id $ids -ErrorAction SilentlyContinue | "
        "Select-Object -ExpandProperty Id"
    )
    rc, out = _run_cmd_silent(
        ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", ps_cmd],
        timeout=5.0,
    )
    if rc != 0 or not out:
        return []
    alive: list[int] = []
    for line in out.splitlines():
        item = line.strip()
        if item.isdigit():
            alive.append(int(item))
    return alive


def _kill_pid_tree(pid: int, force: bool) -> Tuple[int, str]:
    cmd = ["taskkill"]
    if force:
        cmd.append("/F")
    cmd.extend(["/T", "/PID", str(pid)])
    return _run_cmd_silent(cmd, timeout=6.0)


def _wait_for_pids_exit(pids: Sequence[int], timeout: float) -> list[int]:
    deadline = time.time() + max(0.5, timeout)
    remaining = _list_alive_pids(pids)
    while remaining and time.time() < deadline:
        time.sleep(0.35)
        remaining = _list_alive_pids(remaining)
    return remaining


def _supervisor_status_path(card_id: str) -> Optional[Path]:
    filename = STATUS_FILES.get(card_id)
    if not filename:
        return None
    return LOG_DIR / filename


def _default_supervisor_spawn_path(card_id: str) -> Optional[Path]:
    filename = STATUS_FILES.get(card_id)
    if not filename:
        return None
    stem = filename[:-7] if filename.lower().endswith(".status") else Path(filename).stem
    return RUNTIME_STATUS_DIR / f"{stem}.supervisor.spawn"


def _read_restart_identity(card_id: str) -> Dict[str, str]:
    info: Dict[str, str] = {}
    filename = STATUS_FILES.get(card_id)
    if not filename:
        return info

    worker_status_path = _resolve_status_path(filename)
    supervisor_status_path = _supervisor_status_path(card_id)

    worker_status = parse_status_file(worker_status_path)
    if worker_status:
        for key, value in worker_status.items():
            info[f"worker_{key}"] = value

    supervisor_status = parse_status_file(supervisor_status_path) if supervisor_status_path else {}
    if supervisor_status:
        for key, value in supervisor_status.items():
            info[f"supervisor_{key}"] = value

    spawn_path: Optional[Path] = None
    raw_spawn_path = str(supervisor_status.get("spawn_record_file", "")).strip()
    if raw_spawn_path:
        try:
            spawn_path = Path(raw_spawn_path)
        except (TypeError, ValueError):
            spawn_path = None
    if spawn_path is None:
        spawn_path = _default_supervisor_spawn_path(card_id)
    if spawn_path is not None:
        spawn = parse_status_file(spawn_path)
        if spawn:
            for key, value in spawn.items():
                info[f"spawn_{key}"] = value

    info["card_id"] = card_id
    if supervisor_status_path is not None:
        info["supervisor_status_path"] = str(supervisor_status_path)
    info["worker_status_path"] = str(worker_status_path)
    if spawn_path is not None:
        info["spawn_path"] = str(spawn_path)
    return info


def _restart_identity_key(snapshot: Dict[str, str]) -> Tuple[str, ...]:
    return (
        str(snapshot.get("supervisor_run_id", "")).strip(),
        str(snapshot.get("spawn_run_id", "")).strip(),
        str(snapshot.get("supervisor_supervisor_pid", "")).strip(),
        str(snapshot.get("supervisor_launcher_pid", "")).strip(),
        str(snapshot.get("supervisor_worker_pid", "")).strip(),
        str(snapshot.get("supervisor_launcher_start_utc", "")).strip(),
        str(snapshot.get("supervisor_worker_start_utc", "")).strip(),
        str(snapshot.get("spawn_supervisor_pid", "")).strip(),
        str(snapshot.get("spawn_launcher_pid", "")).strip(),
        str(snapshot.get("spawn_worker_pid", "")).strip(),
        str(snapshot.get("spawn_launcher_start_utc", "")).strip(),
        str(snapshot.get("spawn_worker_start_utc", "")).strip(),
    )


def _wait_for_restart_identity_change(card_id: str, before: Dict[str, str], timeout: float = 20.0) -> Dict[str, str]:
    before_key = _restart_identity_key(before)
    deadline = time.time() + max(2.0, timeout)
    latest = before
    while time.time() < deadline:
        latest = _read_restart_identity(card_id)
        if _restart_identity_key(latest) != before_key:
            return latest
        time.sleep(0.5)
    return latest


def _collect_restart_candidate_pids(card_id: str, bat_basename: str) -> Tuple[list[int], Dict[str, str], Set[str]]:
    snapshot = _read_restart_identity(card_id)
    pids: Set[int] = set()
    for key in (
        "worker_pid",
        "supervisor_pid",
        "launcher_pid",
        "pid",
    ):
        for prefix in ("supervisor_", "spawn_", "worker_"):
            pid = _parse_pid_value(snapshot.get(f"{prefix}{key}", ""))
            if pid is not None:
                pids.add(pid)

    tokens: Set[str] = set()
    if bat_basename:
        tokens.add(Path(bat_basename).name)

    for raw in (
        snapshot.get("worker_script", ""),
        snapshot.get("supervisor_name", ""),
    ):
        value = str(raw or "").strip()
        if value:
            tokens.add(Path(value).name)

    for token in list(tokens):
        for pid in _find_process_pids_by_token(token):
            if pid > 0:
                pids.add(pid)

    return sorted(pids), snapshot, tokens


def _verify_restart_success(card_id: str, before: Dict[str, str], trace: list[str]) -> Optional[Dict[str, Any]]:
    after = _wait_for_restart_identity_change(card_id, before, timeout=20.0)
    if _restart_identity_key(after) == _restart_identity_key(before):
        trace.append("verify=no_change")
        return None

    new_worker_pid = (
        _parse_pid_value(after.get("supervisor_worker_pid"))
        or _parse_pid_value(after.get("spawn_worker_pid"))
        or _parse_pid_value(after.get("worker_pid"))
    )
    new_supervisor_pid = (
        _parse_pid_value(after.get("supervisor_supervisor_pid"))
        or _parse_pid_value(after.get("spawn_supervisor_pid"))
    )
    new_status = (
        str(after.get("worker_status", "")).strip()
        or str(after.get("supervisor_status", "")).strip()
        or str(after.get("spawn_state", "")).strip()
    )
    trace.append(
        "verify=changed"
        f"; new_supervisor_pid={new_supervisor_pid or ''}"
        f"; new_worker_pid={new_worker_pid or ''}"
        f"; new_status={new_status}"
    )
    return {
        "new_supervisor_pid": new_supervisor_pid,
        "new_worker_pid": new_worker_pid,
        "new_status": new_status,
        "after": after,
    }


def _restart_card_session(card_id: str) -> Dict[str, Any]:
    task_names = CARD_TASK_NAMES.get(card_id, ())
    bat_basename = RESTARTABLE_CARDS.get(card_id, "")
    if not task_names or not bat_basename:
        return {"ok": False, "message": "Session is not restartable."}
    task_name = task_names[0]
    trace: list[str] = []
    identity_before = _read_restart_identity(card_id)
    cutoff_hhmm = str(identity_before.get("supervisor_cutoff_hhmm", "")).strip()
    if cutoff_hhmm.isdigit():
        now_hhmm = dt.datetime.now(IST).strftime("%H%M")
        if now_hhmm >= cutoff_hhmm:
            return {
                "ok": False,
                "message": (
                    f"Restart blocked after cutoff ({cutoff_hhmm}). "
                    "A relaunch now would be skipped by this session's BAT/supervisor."
                ),
                "task_name": task_name,
            }

    def _run_and_verify(step_no: int, success_message: str, run_label: str) -> Optional[Dict[str, Any]]:
        run_rc, run_out = _run_cmd_silent(["schtasks", "/Run", "/TN", task_name], timeout=5.0)
        trace.append(f"{run_label}_rc={run_rc}")
        if run_out:
            trace.append(f"{run_label}_out={run_out[:240]}")
        verified = _verify_restart_success(card_id, identity_before, trace)
        if verified is not None:
            return {
                "ok": True,
                "step": step_no,
                "message": success_message,
                "task_name": task_name,
                "trace": " | ".join(trace),
                "new_supervisor_pid": verified["new_supervisor_pid"],
                "new_worker_pid": verified["new_worker_pid"],
                "new_status": verified["new_status"],
            }
        if run_rc != 0 and run_out:
            trace.append(f"{run_label}_verify_failed")
        return None

    # Step 1: graceful restart via Task Scheduler (/End then /Run)
    end_rc, _ = _run_cmd_silent(["schtasks", "/End", "/TN", task_name], timeout=5.0)
    trace.append(f"end_rc={end_rc}")
    time.sleep(1.0)
    verified = _run_and_verify(1, "Restarted via Task Scheduler and verified.", "run1")
    if verified is not None:
        return verified

    # Step 2: graceful taskkill on scheduler/supervisor tree (no /F), then /Run
    pids, _, tokens = _collect_restart_candidate_pids(card_id, bat_basename)
    for pid in pids:
        _kill_pid_tree(pid, force=False)
    remaining = _wait_for_pids_exit(pids, timeout=4.0)
    trace.append(f"graceful_pids={pids}")
    if tokens:
        trace.append(f"tokens={sorted(tokens)}")
    if remaining:
        trace.append(f"graceful_remaining={remaining}")
    time.sleep(1.2)
    verified = _run_and_verify(2, "Graceful stop + verified start succeeded.", "run2")
    if verified is not None:
        return verified

    # Step 3: force taskkill + /Run
    pids, _, _ = _collect_restart_candidate_pids(card_id, bat_basename)
    for pid in pids:
        _kill_pid_tree(pid, force=True)
    remaining = _wait_for_pids_exit(pids, timeout=6.0)
    trace.append(f"force_pids={pids}")
    if remaining:
        trace.append(f"force_remaining={remaining}")
    time.sleep(1.0)
    verified = _run_and_verify(3, "Force stop + verified start succeeded.", "run3")
    if verified is not None:
        return verified

    run_rc, run_out = _run_cmd_silent(["schtasks", "/Query", "/TN", task_name, "/FO", "LIST", "/V"], timeout=5.0)
    trace.append(f"query_rc={run_rc}")
    if run_out:
        trace.append(f"query_out={run_out[:240]}")
    err_msg = run_out or "restart verification failed"
    return {
        "ok": False,
        "step": 3,
        "message": f"Restart failed: {err_msg}",
        "task_name": task_name,
        "trace": " | ".join(trace),
    }


def _latest_matching_file(base_dir: Path, glob_pattern: str) -> Optional[Path]:
    try:
        candidates = list(base_dir.glob(glob_pattern))
    except OSError:
        return None
    if not candidates:
        return None
    try:
        return max(candidates, key=lambda p: p.stat().st_mtime)
    except OSError:
        return None


def resolve_log_target(name: str) -> Tuple[Path, str]:
    today_ist = dt.datetime.now(IST).date().isoformat()
    if name == "signal_discovery_v7_5min_id":
        path = SIGNAL_DISCOVERY_V7_ROOT / "heartbeat" / "candidate_tickers.status.json"
        return path, str(Path("signal_discovery_v7_5mins_ID") / "heartbeat" / path.name)

    if name == "entry_engine_1min_v5_id":
        path = runtime_dir("entry_engine_1min_v5_ID") / "heartbeat" / "entry_engine.status.json"
        return path, str(Path("entry_engine_1min_v5_ID") / "heartbeat" / path.name)

    if name == "v7_research_layer":
        path = V7_RESEARCH_LAYER_LATEST_DIR / "latest_summary.json"
        return path, str(Path("live_research_v7_research_layer") / "latest" / path.name)

    if name == "daily_live_v7_research_session":
        path = DAILY_LIVE_V7_RESEARCH_LATEST_DIR / "latest_daily_live_v7_research.md"
        return path, str(Path("daily_live_v7_research_session") / "latest" / path.name)

    if name == "v7_pre_momentum_filter_analyst":
        path = V7_PRE_MOMENTUM_FILTER_ANALYST_LATEST_DIR / "latest_v7_pre_momentum_filter_analyst.md"
        return path, str(Path("v7_pre_momentum_filter_analyst") / "latest" / path.name)

    if name in LOG_FILES:
        file_name = LOG_FILES[name]
        return LOG_DIR / file_name, file_name

    if name == "paper_trade_v5":
        runtime_name = f"paper_trade_execution_{today_ist}_v5.log"
        runtime_path = LIVE_SIGNAL_DIR / runtime_name
        if runtime_path.exists():
            return runtime_path, str(Path("live_signals") / runtime_name)
        latest_runtime = _latest_matching_file(LIVE_SIGNAL_DIR, "paper_trade_execution_*_v5.log")
        if latest_runtime is not None:
            return latest_runtime, str(Path("live_signals") / latest_runtime.name)
        today_name = f"avwap_trade_execution_PAPER_TRADE_TRUE_v5_{today_ist}.log"
        today_path = LOG_DIR / today_name
        if today_path.exists():
            return today_path, today_name
        latest = _latest_matching_file(LOG_DIR, "avwap_trade_execution_PAPER_TRADE_TRUE_v5_*.log")
        if latest is not None:
            return latest, latest.name
        legacy_name = "avwap_trade_execution_PAPER_TRADE_TRUE_v5.log"
        return LOG_DIR / legacy_name, legacy_name

    if name == "paper_trade_v7_sweep":
        runtime_name = f"paper_trade_execution_{today_ist}_v7_sweep.log"
        runtime_path = LIVE_SIGNAL_DIR / runtime_name
        if runtime_path.exists():
            return runtime_path, str(Path("live_signals") / runtime_name)
        latest_runtime = _latest_matching_file(LIVE_SIGNAL_DIR, "paper_trade_execution_*_v7_sweep.log")
        if latest_runtime is not None:
            return latest_runtime, str(Path("live_signals") / latest_runtime.name)
        today_name = f"avwap_trade_execution_PAPER_TRADE_TRUE_v7_sweep_{today_ist}.log"
        today_path = LOG_DIR / today_name
        if today_path.exists():
            return today_path, today_name
        latest = _latest_matching_file(LOG_DIR, "avwap_trade_execution_PAPER_TRADE_TRUE_v7_sweep_*.log")
        if latest is not None:
            return latest, latest.name
        legacy_name = "avwap_trade_execution_PAPER_TRADE_TRUE_v7_sweep.log"
        return LOG_DIR / legacy_name, legacy_name

    if name == "paper_trade_v15":
        runtime_name = f"paper_trade_execution_{today_ist}_v15_new.log"
        runtime_path = LIVE_SIGNAL_DIR / runtime_name
        if runtime_path.exists():
            return runtime_path, str(Path("live_signals") / runtime_name)
        latest_runtime = _latest_matching_file(LIVE_SIGNAL_DIR, "paper_trade_execution_*_v15_new.log")
        if latest_runtime is not None:
            return latest_runtime, str(Path("live_signals") / latest_runtime.name)
        today_name = f"avwap_trade_execution_PAPER_TRADE_TRUE_v15_new_{today_ist}.log"
        today_path = LOG_DIR / today_name
        if today_path.exists():
            return today_path, today_name
        latest = _latest_matching_file(LOG_DIR, "avwap_trade_execution_PAPER_TRADE_TRUE_v15_new_*.log")
        if latest is not None:
            return latest, latest.name
        legacy_name = "avwap_trade_execution_PAPER_TRADE_TRUE_v15_new.log"
        return LOG_DIR / legacy_name, legacy_name

    if name == "kite_trade":
        runtime_name = f"live_trade_execution_{today_ist}_v5.log"
        runtime_path = LIVE_SIGNAL_DIR / runtime_name
        if runtime_path.exists():
            return runtime_path, str(Path("live_signals") / runtime_name)
        latest_runtime = _latest_matching_file(LIVE_SIGNAL_DIR, "live_trade_execution_*_v5.log")
        if latest_runtime is not None:
            return latest_runtime, str(Path("live_signals") / latest_runtime.name)
        today_name = f"avwap_trade_execution_PAPER_TRADE_FALSE_{today_ist}.log"
        today_path = LOG_DIR / today_name
        if today_path.exists():
            return today_path, today_name
        latest = _latest_matching_file(LOG_DIR, "avwap_trade_execution_PAPER_TRADE_FALSE_*.log")
        if latest is not None:
            return latest, latest.name
        legacy_name = "avwap_trade_execution_PAPER_TRADE_FALSE.log"
        legacy_path = LOG_DIR / legacy_name
        if legacy_path.exists():
            return legacy_path, legacy_name
        signal_log_name = "live_trade_execution.log"
        signal_log_path = LIVE_SIGNAL_DIR / signal_log_name
        if signal_log_path.exists():
            return signal_log_path, str(Path("live_signals") / signal_log_name)
        return legacy_path, legacy_name

    if name == "kite_trade_v7_sweep":
        runtime_name = f"live_trade_execution_{today_ist}_v7_sweep.log"
        runtime_path = LIVE_SIGNAL_DIR / runtime_name
        if runtime_path.exists():
            return runtime_path, str(Path("live_signals") / runtime_name)
        latest_runtime = _latest_matching_file(LIVE_SIGNAL_DIR, "live_trade_execution_*_v7_sweep.log")
        if latest_runtime is not None:
            return latest_runtime, str(Path("live_signals") / latest_runtime.name)
        today_name = f"avwap_trade_execution_PAPER_TRADE_FALSE_v7_sweep_{today_ist}.log"
        today_path = LOG_DIR / today_name
        if today_path.exists():
            return today_path, today_name
        latest = _latest_matching_file(LOG_DIR, "avwap_trade_execution_PAPER_TRADE_FALSE_v7_sweep_*.log")
        if latest is not None:
            return latest, latest.name
        legacy_name = "avwap_trade_execution_PAPER_TRADE_FALSE_v7_sweep.log"
        return LOG_DIR / legacy_name, legacy_name

    if name == "kite_trade_v15":
        runtime_name = f"live_trade_execution_{today_ist}_v15_new.log"
        runtime_path = LIVE_SIGNAL_DIR / runtime_name
        if runtime_path.exists():
            return runtime_path, str(Path("live_signals") / runtime_name)
        latest_runtime = _latest_matching_file(LIVE_SIGNAL_DIR, "live_trade_execution_*_v15_new.log")
        if latest_runtime is not None:
            return latest_runtime, str(Path("live_signals") / latest_runtime.name)
        today_name = f"avwap_trade_execution_PAPER_TRADE_FALSE_v15_new_{today_ist}.log"
        today_path = LOG_DIR / today_name
        if today_path.exists():
            return today_path, today_name
        latest = _latest_matching_file(LOG_DIR, "avwap_trade_execution_PAPER_TRADE_FALSE_v15_new_*.log")
        if latest is not None:
            return latest, latest.name
        legacy_name = "avwap_trade_execution_PAPER_TRADE_FALSE_v15_new.log"
        return LOG_DIR / legacy_name, legacy_name

    if name == "paper_trade_v16_5min":
        runtime_name = f"paper_trade_execution_{today_ist}_v16_5min.log"
        runtime_path = LIVE_SIGNAL_DIR / runtime_name
        if runtime_path.exists():
            return runtime_path, str(Path("live_signals") / runtime_name)
        latest_runtime = _latest_matching_file(LIVE_SIGNAL_DIR, "paper_trade_execution_*_v16_5min.log")
        if latest_runtime is not None:
            return latest_runtime, str(Path("live_signals") / latest_runtime.name)
        today_name = f"avwap_trade_execution_PAPER_TRADE_TRUE_v16_5min_{today_ist}.log"
        today_path = LOG_DIR / today_name
        if today_path.exists():
            return today_path, today_name
        latest = _latest_matching_file(LOG_DIR, "avwap_trade_execution_PAPER_TRADE_TRUE_v16_5min_*.log")
        if latest is not None:
            return latest, latest.name
        legacy_name = "avwap_trade_execution_PAPER_TRADE_TRUE_v16_5min.log"
        return LOG_DIR / legacy_name, legacy_name

    if name == "paper_trade_id_5min_v7":
        runtime_name = f"paper_trade_execution_{today_ist}_id_5min_v7.log"
        runtime_path = LIVE_SIGNAL_DIR / runtime_name
        if runtime_path.exists():
            return runtime_path, str(Path("live_signals") / runtime_name)
        latest_runtime = _latest_matching_file(LIVE_SIGNAL_DIR, "paper_trade_execution_*_id_5min_v7.log")
        if latest_runtime is not None:
            return latest_runtime, str(Path("live_signals") / latest_runtime.name)
        today_name = f"avwap_trade_execution_PAPER_TRADE_TRUE_id_5min_v7_{today_ist}.log"
        today_path = LOG_DIR / today_name
        if today_path.exists():
            return today_path, today_name
        latest = _latest_matching_file(LOG_DIR, "avwap_trade_execution_PAPER_TRADE_TRUE_id_5min_v7_*.log")
        if latest is not None:
            return latest, latest.name
        legacy_name = "avwap_trade_execution_PAPER_TRADE_TRUE_id_5min_v7.log"
        return LOG_DIR / legacy_name, legacy_name

    if name == "kite_trade_v16_5min":
        runtime_name = f"live_trade_execution_{today_ist}_v16_5min.log"
        runtime_path = LIVE_SIGNAL_DIR / runtime_name
        if runtime_path.exists():
            return runtime_path, str(Path("live_signals") / runtime_name)
        latest_runtime = _latest_matching_file(LIVE_SIGNAL_DIR, "live_trade_execution_*_v16_5min.log")
        if latest_runtime is not None:
            return latest_runtime, str(Path("live_signals") / latest_runtime.name)
        today_name = f"avwap_trade_execution_PAPER_TRADE_FALSE_v16_5min_{today_ist}.log"
        today_path = LOG_DIR / today_name
        if today_path.exists():
            return today_path, today_name
        latest = _latest_matching_file(LOG_DIR, "avwap_trade_execution_PAPER_TRADE_FALSE_v16_5min_*.log")
        if latest is not None:
            return latest, latest.name
        legacy_name = "avwap_trade_execution_PAPER_TRADE_FALSE_v16_5min.log"
        return LOG_DIR / legacy_name, legacy_name

    if name == "kite_trade_id_5min_v7":
        runtime_name = f"live_trade_execution_{today_ist}_id_5min_v7.log"
        runtime_path = LIVE_SIGNAL_DIR / runtime_name
        if runtime_path.exists():
            return runtime_path, str(Path("live_signals") / runtime_name)
        latest_runtime = _latest_matching_file(LIVE_SIGNAL_DIR, "live_trade_execution_*_id_5min_v7.log")
        if latest_runtime is not None:
            return latest_runtime, str(Path("live_signals") / latest_runtime.name)
        today_name = f"avwap_trade_execution_PAPER_TRADE_FALSE_id_5min_v7_{today_ist}.log"
        today_path = LOG_DIR / today_name
        if today_path.exists():
            return today_path, today_name
        latest = _latest_matching_file(LOG_DIR, "avwap_trade_execution_PAPER_TRADE_FALSE_id_5min_v7_*.log")
        if latest is not None:
            return latest, latest.name
        legacy_name = "avwap_trade_execution_PAPER_TRADE_FALSE_id_5min_v7.log"
        return LOG_DIR / legacy_name, legacy_name

    if name == "preopen_healthcheck":
        today_name = f"preopen_session_healthcheck_{today_ist}.log"
        today_path = LOG_DIR / today_name
        if today_path.exists():
            return today_path, today_name
        latest_name = "preopen_session_healthcheck_latest.log"
        latest_path = LOG_DIR / latest_name
        if latest_path.exists():
            return latest_path, latest_name
        fallback = _latest_matching_file(LOG_DIR, "preopen_session_healthcheck_*.log")
        if fallback is not None:
            return fallback, fallback.name
        return latest_path, latest_name

    if name == "paper_trade_exec":
        today_name = f"paper_trade_execution_{today_ist}.log"
        today_path = LIVE_SIGNAL_DIR / today_name
        if today_path.exists():
            return today_path, str(Path("live_signals") / today_name)
        latest = _latest_matching_file(LIVE_SIGNAL_DIR, "paper_trade_execution_*.log")
        if latest is not None:
            return latest, str(Path("live_signals") / latest.name)
        legacy_name = "paper_trade_execution.log"
        return LIVE_SIGNAL_DIR / legacy_name, str(Path("live_signals") / legacy_name)

    if name == "pending_signals_v16_5min":
        today_name = f"pending_signals_{today_ist}_v16_5min.csv"
        today_path = LIVE_SIGNAL_DIR / today_name
        return today_path, str(Path("live_signals") / today_name)

    if name == "detected_signals_v16_5min":
        today_name = f"detected_signals_{today_ist}_v16_5min.csv"
        today_path = LIVE_SIGNAL_DIR / today_name
        return today_path, str(Path("live_signals") / today_name)

    raise KeyError(name)


def parse_status_file(path: Path) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not path.exists():
        return out
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
        if text.startswith("\ufeff"):
            text = text.lstrip("\ufeff")
        for line in text.splitlines():
            if "=" in line:
                k, v = line.split("=", 1)
                out[k.strip().lstrip("\ufeff")] = v.strip()
    except OSError:
        return {}
    return out


def _parse_status_datetime(raw: object, *, default_tz: dt.tzinfo = IST) -> Optional[dt.datetime]:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        parsed = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=default_tz)
        return parsed.astimezone(IST)
    except ValueError:
        pass
    for fmt in ("%Y-%m-%d_%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return dt.datetime.strptime(text, fmt).replace(tzinfo=default_tz)
        except ValueError:
            continue
    return None


def _append_status_note(status: Dict[str, str], note: str) -> Dict[str, str]:
    merged = dict(status or {})
    extra = str(note or "").strip()
    if not extra:
        return merged
    current = str(merged.get("derived_status", "")).strip()
    merged["derived_status"] = f"{current}; {extra}" if current else extra
    return merged


def infer_pid_session_provenance(card_id: str, status: Dict[str, str]) -> Dict[str, str]:
    merged = dict(status or {})
    candidate_keys = (
        ("start_ts_utc", "start_ts_utc"),
        ("worker_start_utc", "worker_start_utc"),
        ("launcher_start_utc", "launcher_start_utc"),
        ("start_ts", "start_ts"),
    )
    sources_to_check = [("status", merged)]
    supervisor_filename = STATUS_FILES.get(card_id, "")
    if supervisor_filename:
        supervisor_path = LOG_DIR / supervisor_filename
        supervisor_status = parse_status_file(supervisor_path)
        if supervisor_status:
            sources_to_check.append(("supervisor_status", supervisor_status))

    start_ist: Optional[dt.datetime] = None
    start_source = ""
    for scope, source_dict in sources_to_check:
        for key, source in candidate_keys:
            parsed = _parse_status_datetime(source_dict.get(key, ""))
            if parsed is not None:
                start_ist = parsed.astimezone(IST)
                start_source = f"{scope}:{source}"
                break
        if start_ist is not None:
            break

    if start_ist is None:
        return merged

    session_start = dt.datetime.combine(dt.datetime.now(IST).date(), dt.time(9, 0), tzinfo=IST)
    merged["pid_start_ist"] = start_ist.strftime("%Y-%m-%d %H:%M:%S%z")
    merged["pid_start_source"] = start_source
    merged["session_start_ist"] = session_start.strftime("%Y-%m-%d %H:%M:%S%z")

    if start_ist < session_start:
        merged["provenance_flag"] = "PID_PREDATES_SESSION"
        merged = _append_status_note(
            merged,
            (
                f"provenance=pid_predates_session"
                f" | pid_start={merged['pid_start_ist']}"
                f" | session_start={merged['session_start_ist']}"
            ),
        )
    return merged


def merge_runtime_status(status: Dict[str, str], heartbeat: Dict[str, str]) -> Dict[str, str]:
    merged = dict(status or {})
    if not heartbeat:
        return merged

    hb_state = str(heartbeat.get("state", "")).strip().upper()
    if hb_state:
        merged["heartbeat_state"] = hb_state
        merged["heartbeat_ts_utc"] = str(heartbeat.get("ts_utc", "")).strip()
        merged["heartbeat_idle_sec"] = str(heartbeat.get("idle_sec", "")).strip()
        merged["heartbeat_note"] = str(heartbeat.get("note", "")).strip()
        if hb_state in {"RUNNING", "RESTARTING", "COOLDOWN"}:
            merged["status"] = hb_state
        elif "status" not in merged:
            merged["status"] = hb_state
    return merged


def infer_scanner_runtime_status(key: str, path: Path, status: Dict[str, str]) -> Dict[str, str]:
    if not key.startswith("live_combined_csv_"):
        return dict(status or {})

    merged = dict(status or {})
    current = str(merged.get("status", "")).strip().upper()
    if current in {"RUNNING", "RESTARTING", "COOLDOWN"}:
        return merged
    if not path.exists():
        return merged
    try:
        mtime = dt.datetime.fromtimestamp(path.stat().st_mtime, tz=IST)
    except OSError:
        return merged
    now = dt.datetime.now(IST)
    age_min = max(0.0, (now - mtime).total_seconds() / 60.0)
    if mtime.date() == now.date() and age_min <= 20.0:
        merged["status"] = "RUNNING"
        merged["derived_status"] = f"log_fresh_{age_min:.1f}m"
    return merged


def _count_csv_data_rows(path: Path, side: Optional[str] = None) -> int:
    if not path.exists():
        return 0
    side_upper = str(side or "").strip().upper()
    count = 0
    try:
        with open(path, newline="", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if side_upper and str(row.get("side", "")).strip().upper() != side_upper:
                    continue
                count += 1
    except (OSError, csv.Error):
        return 0
    return count


def infer_v16_side_output_status(side_csv_path: Path, side: str, detected_csv_path: Path) -> Dict[str, str]:
    side_upper = str(side or "").strip().upper()
    side_rows = _count_csv_data_rows(side_csv_path)
    detected_rows = _count_csv_data_rows(detected_csv_path, side=side_upper)
    derived = f"side_rows={side_rows}; detected_rows={detected_rows}"

    if not side_csv_path.exists():
        return {
            "status": "MISSING_OUTPUT" if detected_rows > 0 else "WAITING_OUTPUT",
            "derived_status": derived,
        }

    if side_rows <= 0:
        return {
            "status": "EMPTY_OUTPUT",
            "derived_status": derived,
        }

    if not detected_csv_path.exists():
        return {
            "status": "STALE_OUTPUT",
            "derived_status": f"{derived}; reason=missing_detected_csv",
        }

    if detected_rows <= 0:
        return {
            "status": "STALE_OUTPUT",
            "derived_status": f"{derived}; reason=no_detected_rows",
        }

    if detected_rows > 0 and side_rows < detected_rows:
        return {
            "status": "STALE_OUTPUT",
            "derived_status": f"{derived}; reason=row_count_lag",
        }

    if detected_rows > 0 and detected_csv_path.exists():
        try:
            side_mtime = side_csv_path.stat().st_mtime
            detected_mtime = detected_csv_path.stat().st_mtime
        except OSError:
            side_mtime = 0.0
            detected_mtime = 0.0
        lag_sec = max(0.0, detected_mtime - side_mtime)
        if lag_sec > 2.0:
            return {
                "status": "STALE_OUTPUT",
                "derived_status": f"{derived}; reason=mtime_lag_{lag_sec:.1f}s",
            }

    return {"derived_status": derived}


def _parse_schtasks_verbose(text: str) -> Dict[str, Dict[str, str]]:
    tasks: Dict[str, Dict[str, str]] = {}
    if not text:
        return tasks
    blocks = re.split(r"(?:\r?\n){2,}", text)
    for block in blocks:
        if "TaskName:" not in block:
            continue
        record: Dict[str, str] = {}
        for raw_line in block.splitlines():
            line = raw_line.strip()
            if not line or ":" not in line:
                continue
            key, value = line.split(":", 1)
            record[key.strip()] = value.strip()
        task_name = str(record.get("TaskName", "")).strip()
        if task_name:
            tasks[task_name] = record
    return tasks


def load_task_scheduler_snapshot(force: bool = False) -> Dict[str, Dict[str, str]]:
    global _TASK_SNAPSHOT_CACHE_AT, _TASK_SNAPSHOT_CACHE
    now_utc = dt.datetime.now(dt.timezone.utc)
    if (
        not force
        and _TASK_SNAPSHOT_CACHE_AT is not None
        and (now_utc - _TASK_SNAPSHOT_CACHE_AT).total_seconds() < 10.0
    ):
        return dict(_TASK_SNAPSHOT_CACHE)

    tasks: Dict[str, Dict[str, str]] = {}
    try:
        completed = subprocess.run(
            ["schtasks", "/Query", "/FO", "LIST", "/V"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=20,
            check=False,
        )
        raw = "\n".join(part for part in (completed.stdout, completed.stderr) if part)
        parsed = _parse_schtasks_verbose(raw)
        tasks = {
            task_name: fields
            for task_name, fields in parsed.items()
            if str(task_name).strip().upper().startswith("\\EQIDV2")
        }
    except (OSError, subprocess.SubprocessError):
        tasks = {}

    _TASK_SNAPSHOT_CACHE = tasks
    _TASK_SNAPSHOT_CACHE_AT = now_utc
    return dict(tasks)


def apply_scheduler_status(card_id: str, status: Dict[str, str], task_snapshot: Dict[str, Dict[str, str]]) -> Dict[str, str]:
    merged = dict(status or {})
    task_names = CARD_TASK_NAMES.get(card_id, ())
    if not task_names:
        return merged

    records = [task_snapshot.get(task_name) for task_name in task_names if task_snapshot.get(task_name)]
    if not records:
        return merged

    def _upper(value: object) -> str:
        return str(value or "").strip().upper()

    scheduler_states = [_upper(rec.get("Scheduled Task State")) for rec in records]
    scheduler_statuses = [_upper(rec.get("Status")) for rec in records]
    next_runs = [
        str(rec.get("Next Run Time", "")).strip()
        for rec in records
        if str(rec.get("Next Run Time", "")).strip() and str(rec.get("Next Run Time", "")).strip().upper() != "N/A"
    ]

    all_disabled = bool(records) and all(
        state == "DISABLED" or status_val == "DISABLED"
        for state, status_val in zip(scheduler_states, scheduler_statuses)
    )
    any_enabled = any(state == "ENABLED" for state in scheduler_states)
    any_running = any(status_val == "RUNNING" for status_val in scheduler_statuses)

    if all_disabled:
        scheduler_status = "DISABLED"
        scheduler_state = "DISABLED"
    elif any_running:
        scheduler_status = "RUNNING"
        scheduler_state = "ENABLED" if any_enabled else ""
    elif any_enabled:
        scheduler_status = "SCHEDULED"
        scheduler_state = "ENABLED"
    else:
        scheduler_status = ""
        scheduler_state = ""

    if scheduler_state:
        merged["scheduler_state"] = scheduler_state
    if scheduler_status:
        merged["scheduler_status"] = scheduler_status
    if next_runs:
        merged["scheduler_next_run"] = min(next_runs)
    merged["scheduler_tasks"] = ", ".join(task_names)

    current = _upper(merged.get("status"))
    if scheduler_status == "DISABLED":
        merged["status"] = "DISABLED"
    elif not current and scheduler_status:
        merged["status"] = scheduler_status

    return merged


def tail_text(path: Path, lines: int = 80, max_bytes: int = 120_000) -> str:
    if not path.exists():
        return ""
    try:
        size = path.stat().st_size
        with path.open("rb") as f:
            if size > max_bytes:
                f.seek(size - max_bytes)
            chunk = f.read()
        text = chunk.decode("utf-8", errors="replace")
        return "\n".join(text.splitlines()[-lines:])
    except OSError as exc:
        return f"[ERROR reading log: {exc}]"


def _read_csv_tail_rows(path: Path, limit: int = 30) -> list[dict[str, str]]:
    if not path.exists():
        return []
    rows: deque[dict[str, str]] = deque(maxlen=max(1, int(limit)))
    try:
        with path.open("r", encoding="utf-8-sig", errors="replace", newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                if not row:
                    continue
                rows.append({str(k): ("" if v is None else str(v)) for k, v in row.items()})
    except (OSError, csv.Error):
        return []
    return list(rows)


def _pick_csv_value(row: dict[str, str], keys: Sequence[str]) -> str:
    for key in keys:
        val = str(row.get(key, "")).strip()
        if val:
            return val
    return ""


def _clip_text(value: str, width: int) -> str:
    s = str(value)
    if len(s) <= width:
        return s
    if width <= 1:
        return s[:width]
    return s[: width - 1] + "~"


def _extract_time_only(value: str) -> str:
    """
    Strip YYYY-MM-DD and trailing timezone offsets from common datetime strings,
    and keep only the time part.
    Examples:
      2026-02-24 11:05:18+0530 -> 11:05:18
      2026-02-24T11:05:18+05:30 -> 11:05:18
    """
    s = str(value or "").strip()
    if not s:
        return s
    m = re.match(r"^\d{4}-\d{2}-\d{2}[ T](.+)$", s)
    if m:
        s = m.group(1).strip()
    s = re.sub(r"\s*(?:Z|[+-]\d{2}:?\d{2})$", "", s).strip()
    return s


def _shift_iso_5min(value: str) -> str:
    """
    Display-only shift: add 5 minutes to a bar-anchored ISO/naive timestamp so
    bar-open stamps render as bar-close. Does NOT modify source data. Preserves
    input format (date separator and tz offset) when possible; falls back to
    naive HH:MM[:SS] arithmetic for bare time strings. Empty/unparsable -> original.
    """
    s = str(value or "").strip()
    if not s:
        return s
    # Full datetime: YYYY-MM-DD(T| )HH:MM(:SS)?(tz)?
    m = re.match(
        r"^(\d{4}-\d{2}-\d{2})([ T])(\d{2}):(\d{2})(:\d{2}(?:\.\d+)?)?([+\-]\d{2}:?\d{2}|Z)?$",
        s,
    )
    if m:
        date_s, sep, hh, mm, sec, tz = m.groups()
        total = int(hh) * 60 + int(mm) + 5
        nh = (total // 60) % 24
        nm = total % 60
        return f"{date_s}{sep}{nh:02d}:{nm:02d}{sec or ''}{tz or ''}"
    # Bare time: HH:MM(:SS)?
    m = re.match(r"^(\d{2}):(\d{2})(:\d{2}(?:\.\d+)?)?$", s)
    if m:
        hh, mm, sec = m.groups()
        total = int(hh) * 60 + int(mm) + 5
        nh = (total // 60) % 24
        nm = total % 60
        return f"{nh:02d}:{nm:02d}{sec or ''}"
    return s


_BAR_SLOT_RE_FULL = re.compile(
    r"\b(slot|target_slot|signal_bar)=(\d{4}-\d{2}-\d{2})([ T])(\d{2}):(\d{2})(:\d{2})?([+\-]\d{2}:?\d{2}|Z)?"
)
_BAR_SLOT_RE_HHMM = re.compile(r"\b(slot|target_slot|signal_bar)=(\d{2}):(\d{2})(?![:\d])")


def _shift_bar_slots_in_text(text: str) -> str:
    """
    Display-only shift on raw log tails: rewrites `slot=`, `target_slot=`, and
    `signal_bar=` tokens so bar-open stamps render as bar-close. Wallclock
    timestamps (which never carry these prefixes) are left untouched.
    """
    if not text:
        return text

    def _full(m: "re.Match[str]") -> str:
        key, date_s, sep, hh, mm, sec, tz = m.groups()
        total = int(hh) * 60 + int(mm) + 5
        nh = (total // 60) % 24
        nm = total % 60
        return f"{key}={date_s}{sep}{nh:02d}:{nm:02d}{sec or ''}{tz or ''}"

    def _hhmm(m: "re.Match[str]") -> str:
        key, hh, mm = m.group(1), int(m.group(2)), int(m.group(3))
        total = hh * 60 + mm + 5
        return f"{key}={(total // 60) % 24:02d}:{total % 60:02d}"

    text = _BAR_SLOT_RE_FULL.sub(_full, text)
    text = _BAR_SLOT_RE_HHMM.sub(_hhmm, text)
    return text


def _to_float_or_nan(value: str) -> float:
    s = str(value or "").strip()
    if not s:
        return float("nan")
    # tolerate display formats like Rs.+1,234.56
    s = s.replace(",", "")
    s = s.replace("Rs.", "").replace("RS.", "").replace("rs.", "")
    s = s.replace("%", "")
    try:
        return float(s)
    except ValueError:
        return float("nan")


def _fmt_indian_number(value: float, decimals: int = 2, signed: bool = False) -> str:
    if math.isnan(value):
        return "n/a"

    sign = ""
    if value < 0:
        sign = "-"
    elif signed and value > 0:
        sign = "+"

    abs_val = abs(value)
    if decimals <= 0:
        numeric = f"{abs_val:.0f}"
        int_part = numeric
        frac_part = ""
    else:
        numeric = f"{abs_val:.{decimals}f}"
        int_part, frac_part = numeric.split(".", 1)

    if len(int_part) > 3:
        head = int_part[:-3]
        tail = int_part[-3:]
        groups = []
        while len(head) > 2:
            groups.append(head[-2:])
            head = head[:-2]
        if head:
            groups.append(head)
        groups.reverse()
        int_part = ",".join(groups + [tail])

    if decimals <= 0:
        return f"{sign}{int_part}"
    return f"{sign}{int_part}.{frac_part}"


def _fmt_pct(value: float, signed: bool = True) -> str:
    return f"{_fmt_indian_number(value, decimals=2, signed=signed)}%"


def _fmt_rs(value: float) -> str:
    return f"Rs.{_fmt_indian_number(value, decimals=2, signed=True)}"


def _fmt_rs_plain(value: float) -> str:
    return f"Rs.{_fmt_indian_number(value, decimals=2, signed=False)}"


def _compute_holding_total_pnl_pct(row: dict[str, str]) -> str:
    qty = _to_float_or_nan(_pick_csv_value(row, ("quantity", "qty")))
    t1_qty = _to_float_or_nan(_pick_csv_value(row, ("t1_quantity",)))
    avg = _to_float_or_nan(_pick_csv_value(row, ("average_price", "avg_price", "price")))
    pnl = _to_float_or_nan(_pick_csv_value(row, ("pnl", "unrealised", "unrealized")))

    if math.isnan(avg) or math.isnan(pnl):
        return ""

    q = 0.0
    if not math.isnan(qty):
        q += float(qty)
    if not math.isnan(t1_qty):
        q += float(t1_qty)
    if q <= 0.0:
        return ""

    invested = q * float(avg)
    if invested <= 0.0:
        return ""

    return str((float(pnl) * 100.0) / invested)


def _format_csv_projection(
    path: Path,
    columns: Sequence[Tuple[str, Sequence[str]]],
    limit_rows: int = 25,
    time_only_cols: Optional[Set[str]] = None,
    time_shift_5min_cols: Optional[Set[str]] = None,
    sort_numeric_desc_by_keys: Optional[Sequence[str]] = None,
    total_numeric_by_keys: Optional[Sequence[str]] = None,
    total_numeric_label: str = "",
    total_numeric_first: bool = False,
    indian_numeric_cols: Optional[Set[str]] = None,
    indian_int_cols: Optional[Set[str]] = None,
    percent_cols: Optional[Set[str]] = None,
    signed_numeric_cols: Optional[Set[str]] = None,
    computed_cols: Optional[Dict[str, Callable[[dict[str, str]], str]]] = None,
) -> str:
    """
    Render a compact fixed-width table from selected CSV columns.
    Shows latest rows (oldest-to-newest order within the selected tail window).
    """
    if not path.exists():
        return ""

    rows_raw = _read_csv_tail_rows(path, limit=limit_rows)
    if not rows_raw:
        return "(no rows yet)"

    computed_cols = dict(computed_cols or {})

    if sort_numeric_desc_by_keys:
        def _pick_numeric_raw_for_sort(row: dict[str, str], keys: Sequence[str]) -> str:
            for key in keys:
                if key in computed_cols:
                    try:
                        val = str(computed_cols[key](row) or "").strip()
                    except Exception:
                        val = ""
                else:
                    val = _pick_csv_value(row, (key,))
                if val:
                    return val
            return ""

        def _sort_key(row: dict[str, str]) -> tuple[int, float]:
            raw = _pick_numeric_raw_for_sort(row, sort_numeric_desc_by_keys)
            num = _to_float_or_nan(raw)
            if not math.isnan(num):
                return (0, -float(num))
            return (1, 0.0)

        rows_raw = sorted(rows_raw, key=_sort_key)

    total_val = 0.0
    total_count = 0
    if total_numeric_by_keys:
        for row in rows_raw:
            raw = _pick_csv_value(row, total_numeric_by_keys)
            num = _to_float_or_nan(raw)
            if math.isnan(num):
                continue
            total_val += float(num)
            total_count += 1

    rows: list[dict[str, str]] = []
    time_only_cols = set(time_only_cols or set())
    time_shift_5min_cols = set(time_shift_5min_cols or set())
    indian_numeric_cols = set(indian_numeric_cols or set())
    indian_int_cols = set(indian_int_cols or set())
    percent_cols = set(percent_cols or set())
    signed_numeric_cols = set(signed_numeric_cols or set())
    for row in rows_raw:
        projected: dict[str, str] = {}
        for col_name, key_candidates in columns:
            if col_name in computed_cols:
                try:
                    val = str(computed_cols[col_name](row) or "")
                except Exception:
                    val = ""
            else:
                val = _pick_csv_value(row, key_candidates)
            if col_name in time_shift_5min_cols:
                val = _shift_iso_5min(val)
            if col_name in time_only_cols:
                val = _extract_time_only(val)
            elif col_name in percent_cols:
                num = _to_float_or_nan(val)
                if not math.isnan(num):
                    val = _fmt_pct(num, signed=(col_name in signed_numeric_cols))
            elif col_name in indian_int_cols:
                num = _to_float_or_nan(val)
                if not math.isnan(num):
                    val = _fmt_indian_number(num, decimals=0, signed=(col_name in signed_numeric_cols))
            elif col_name in indian_numeric_cols:
                num = _to_float_or_nan(val)
                if not math.isnan(num):
                    val = _fmt_indian_number(num, decimals=2, signed=(col_name in signed_numeric_cols))
            projected[col_name] = val
        rows.append(projected)

    widths: dict[str, int] = {}
    for col_name, _ in columns:
        max_len = max([len(col_name)] + [len(r[col_name]) for r in rows])
        widths[col_name] = min(max_len, 30)

    header = " | ".join(col_name.ljust(widths[col_name]) for col_name, _ in columns)
    sep = "-+-".join("-" * widths[col_name] for col_name, _ in columns)
    body = [
        " | ".join(
            _clip_text(r[col_name], widths[col_name]).ljust(widths[col_name])
            for col_name, _ in columns
        )
        for r in rows
    ]
    out_lines = [f"rows_shown={len(rows)} (latest)", header, sep] + body
    if total_numeric_by_keys and total_numeric_label:
        total_text = _fmt_rs(total_val) if total_count > 0 else "n/a"
        total_line = f"{total_numeric_label}={total_text}"
        if total_numeric_first:
            out_lines = [total_line] + out_lines
        else:
            out_lines.append(total_line)
    return "\n".join(out_lines)


def _fmt_price(value: float) -> str:
    if math.isnan(value):
        return ""
    return _fmt_indian_number(value, decimals=2, signed=False)


def _fmt_qty(value: float) -> str:
    if math.isnan(value):
        return ""
    return _fmt_indian_number(value, decimals=0, signed=False)


def _read_json_dict(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8", errors="replace"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _format_fixed_table(
    rows: Sequence[dict[str, str]],
    columns: Sequence[str],
    *,
    rows_meta: str,
    summary_lines: Sequence[str] = (),
    column_max_widths: Optional[dict[str, int]] = None,
) -> str:
    if not rows:
        return "\n".join([*summary_lines, rows_meta, "(no rows yet)"])

    widths: dict[str, int] = {}
    for col_name in columns:
        max_len = max([len(col_name)] + [len(str(r.get(col_name, ""))) for r in rows])
        max_width = int((column_max_widths or {}).get(col_name, 30))
        widths[col_name] = min(max_len, max_width)

    header = " | ".join(col_name.ljust(widths[col_name]) for col_name in columns)
    sep = "-+-".join("-" * widths[col_name] for col_name in columns)
    body = [
        " | ".join(
            _clip_text(str(row.get(col_name, "")), widths[col_name]).ljust(widths[col_name])
            for col_name in columns
        )
        for row in rows
    ]
    return "\n".join([*summary_lines, rows_meta, header, sep, *body])


def _parse_latest_live_pnl_line(log_path: Path) -> dict[str, Any]:
    text = tail_text(log_path, lines=250, max_bytes=300_000)
    out: dict[str, Any] = {"ticker_pnl": {}}
    for line in reversed(text.splitlines()):
        if "[LIVE.PNL]" not in line:
            continue
        out["raw_line"] = line
        ts_match = re.match(r"^(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})", line)
        if ts_match:
            out["log_time"] = ts_match.group(1)
        open_match = re.search(r"\[LIVE\.PNL\]\s+open=(\d+)", line)
        if open_match:
            out["open"] = int(open_match.group(1))

        tickers_match = re.search(r"\|\s*tickers=(.*?)\s*\|\s*unrealized=", line)
        ticker_pnl: dict[str, float] = {}
        if tickers_match:
            for part in tickers_match.group(1).split(","):
                if "=" not in part:
                    continue
                ticker, raw_val = part.rsplit("=", 1)
                ticker = ticker.strip().upper()
                pnl = _to_float_or_nan(raw_val)
                if ticker and not math.isnan(pnl):
                    ticker_pnl[ticker] = float(pnl)
        out["ticker_pnl"] = ticker_pnl

        for key in ("unrealized", "realized", "total", "deployed_margin"):
            match = re.search(rf"\|\s*{key}=([^|]+)", line)
            if match:
                val = _to_float_or_nan(match.group(1))
                if not math.isnan(val):
                    out[key] = float(val)
        return out
    return out


def _read_signal_setup_map(today_ist: str) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    for side in ("short", "long"):
        path = LIVE_SIGNAL_DIR / f"signals_{today_ist}_id_5min_v7_{side}.csv"
        for row in _read_csv_tail_rows(path, limit=5000):
            signal_id = str(row.get("signal_id", "")).strip()
            if signal_id:
                out[signal_id] = row
    return out


def _paper_open_pnl_from_state(row: dict[str, Any]) -> float:
    qty = _to_float_or_nan(str(row.get("quantity", "")))
    entry = _to_float_or_nan(str(row.get("entry_price", "")))
    ltp = _to_float_or_nan(str(row.get("last_ltp", "")))
    side = str(row.get("side", "")).upper().strip()
    if math.isnan(qty) or math.isnan(entry) or math.isnan(ltp) or ltp <= 0:
        return float("nan")
    if side == "SHORT":
        return (entry - ltp) * qty
    return (ltp - entry) * qty


def _paper_pnl_pct(pnl_rs: float, entry_price: float, qty: float) -> float:
    if math.isnan(pnl_rs) or math.isnan(entry_price) or math.isnan(qty):
        return float("nan")
    invested = entry_price * qty
    if invested <= 0:
        return float("nan")
    return pnl_rs * 100.0 / invested


def _v7_monitor_slot_label(value: object) -> str:
    s = str(value or "").strip()
    if not s:
        return ""
    m = re.search(r"\b\d{4}-\d{2}-\d{2}[ T](\d{2}):(\d{2})", s)
    if not m:
        m = re.search(r"\b(\d{2}):(\d{2})(?::\d{2})?", s)
    if not m:
        return ""
    hour = int(m.group(1))
    minute = int(m.group(2))
    return f"{hour:02d}:{(minute // 5) * 5:02d}"


def _v7_monitor_slot_from_path(path: Path, date_key: str) -> str:
    m = re.search(rf"_{re.escape(date_key)}_(\d{{2}})(\d{{2}})", path.stem)
    if not m:
        return ""
    return f"{int(m.group(1)):02d}:{(int(m.group(2)) // 5) * 5:02d}"


def _v7_monitor_slot_rows(path: Path, date_key: str, field: str, slots: dict[str, dict[str, Any]]) -> list[dict[str, str]]:
    slot = _v7_monitor_slot_from_path(path, date_key)
    rows = _read_csv_tail_rows(path, limit=20000)
    if slot:
        rec = slots.setdefault(slot, {"slot": slot})
        rec[field] = max(int(rec.get(field, 0) or 0), len(rows))
    return rows


def _v7_monitor_num(row: dict[str, str], keys: Sequence[str]) -> float:
    return _to_float_or_nan(_pick_csv_value(row, keys))


def _v7_monitor_add_avg(bucket: dict[str, Any], name: str, value: float) -> None:
    if math.isnan(value):
        return
    bucket[f"{name}_sum"] = float(bucket.get(f"{name}_sum", 0.0) or 0.0) + float(value)
    bucket[f"{name}_n"] = int(bucket.get(f"{name}_n", 0) or 0) + 1


def _v7_monitor_avg(bucket: dict[str, Any], name: str) -> float:
    count = int(bucket.get(f"{name}_n", 0) or 0)
    if count <= 0:
        return float("nan")
    return float(bucket.get(f"{name}_sum", 0.0) or 0.0) / count


def _v7_monitor_fmt_avg(bucket: dict[str, Any], name: str, decimals: int = 2, signed: bool = False) -> str:
    value = _v7_monitor_avg(bucket, name)
    if math.isnan(value):
        return ""
    return _fmt_indian_number(value, decimals=decimals, signed=signed)


def _v7_monitor_setup_key(row: dict[str, str]) -> tuple[str, str]:
    side = str(row.get("side", "")).upper().strip()
    setup = str(row.get("setup", "")).strip()
    return side, setup


def _v7_monitor_setup_bucket(
    setup_stats: dict[tuple[str, str], dict[str, Any]],
    row: dict[str, str],
) -> Optional[dict[str, Any]]:
    side, setup = _v7_monitor_setup_key(row)
    if not side and not setup:
        return None
    bucket = setup_stats.setdefault(
        (side, setup),
        {
            "side": side,
            "setup": setup,
            "raw": 0,
            "potential": 0,
            "entry_raw": 0,
            "v11_rej": 0,
            "pre_rej": 0,
            "pre_pass": 0,
            "entries": 0,
            "signals": 0,
            "paper": 0,
            "open": 0,
            "target": 0,
            "sl": 0,
            "eod": 0,
            "skips": 0,
            "pnl": 0.0,
            "pnl_n": 0,
        },
    )
    for avg_name, keys in (
        ("q", ("quality_score", "score")),
        ("rank", ("ranker_score",)),
        ("rs", ("rs_pct",)),
        ("vol", ("vol_ratio",)),
        ("atr", ("atr_pct",)),
        ("pre", ("pre_entry_momentum_score",)),
        ("adx", ("pre1_adx", "sig5_adx_calc", "adx")),
        ("rsi", ("pre1_rsi_dir", "sig5_rsi_dir", "rsi")),
    ):
        _v7_monitor_add_avg(bucket, avg_name, _v7_monitor_num(row, keys))
    return bucket


def _v7_monitor_add_setup_count(
    setup_stats: dict[tuple[str, str], dict[str, Any]],
    row: dict[str, str],
    field: str,
    qty: int = 1,
) -> None:
    bucket = _v7_monitor_setup_bucket(setup_stats, row)
    if bucket is None:
        return
    bucket[field] = int(bucket.get(field, 0) or 0) + qty


def _v7_monitor_outcome_bucket(outcome: object) -> str:
    text = str(outcome or "").upper().strip()
    if not text:
        return ""
    if "TARGET" in text or text == "TGT":
        return "target"
    if text == "SL" or "STOP" in text:
        return "sl"
    if "EOD" in text or "SQUARE" in text or text in {"CLOSED", "EXIT"}:
        return "eod"
    if "SKIP" in text:
        return "skips"
    if text == "OPEN":
        return "open"
    return ""


def _v7_monitor_parse_dt(value: object) -> Optional[dt.datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    parsed = _parse_status_datetime(text)
    if parsed is not None:
        return parsed
    cleaned = re.sub(r"([+-]\d{2})(\d{2})$", r"\1:\2", text)
    try:
        out = dt.datetime.fromisoformat(cleaned)
    except ValueError:
        return None
    if out.tzinfo is None:
        out = out.replace(tzinfo=IST)
    return out.astimezone(IST)


def _v7_monitor_fmt_duration(start: object, end: object, *, now: Optional[dt.datetime] = None) -> str:
    start_dt = _v7_monitor_parse_dt(start)
    end_dt = _v7_monitor_parse_dt(end) if end else None
    if start_dt is None:
        return ""
    if end_dt is None:
        end_dt = now or dt.datetime.now(IST)
    total_sec = max(0, int((end_dt - start_dt).total_seconds()))
    minutes = total_sec // 60
    seconds = total_sec % 60
    if minutes >= 60:
        return f"{minutes // 60}h{minutes % 60:02d}m"
    return f"{minutes}m{seconds:02d}s"


def _v7_monitor_sec_between(start: object, end: object) -> float:
    start_dt = _v7_monitor_parse_dt(start)
    end_dt = _v7_monitor_parse_dt(end)
    if start_dt is None or end_dt is None:
        return float("nan")
    return max(0.0, (end_dt - start_dt).total_seconds())


def _v7_monitor_fmt_sec(value: float) -> str:
    if math.isnan(value):
        return ""
    return _fmt_indian_number(float(value), decimals=2)


def _v7_monitor_slot_key_from_time(value: object) -> str:
    parsed = _v7_monitor_parse_dt(value)
    if parsed is None:
        return ""
    floored = parsed.replace(minute=(parsed.minute // 5) * 5, second=0, microsecond=0)
    return floored.strftime("%Y%m%d_%H%M")


def _v7_monitor_raw_fetch_lag_sec(slot_time: object, ticker: object) -> float:
    slot_dt = _v7_monitor_parse_dt(slot_time)
    slot_key = _v7_monitor_slot_key_from_time(slot_time)
    symbol = str(ticker or "").upper().strip()
    if slot_dt is None or not slot_key or not symbol:
        return float("nan")
    raw_path = runtime_dir("entry_engine_1min_v5_ID") / "slot_raw_1min" / slot_key / f"{symbol}_raw_1min.parquet"
    if not raw_path.exists():
        return float("nan")
    try:
        raw_mtime = dt.datetime.fromtimestamp(raw_path.stat().st_mtime, tz=IST)
    except OSError:
        return float("nan")
    return max(0.0, (raw_mtime - slot_dt).total_seconds())


def _v7_monitor_slot_dt(today_ist: str, slot: str) -> Optional[dt.datetime]:
    try:
        return dt.datetime.strptime(f"{today_ist} {slot}", "%Y-%m-%d %H:%M").replace(tzinfo=IST)
    except ValueError:
        return None


def _v7_monitor_mtime(path: Path) -> Optional[dt.datetime]:
    try:
        return dt.datetime.fromtimestamp(path.stat().st_mtime, tz=IST)
    except OSError:
        return None


def _v7_monitor_executor_events(log_path: Path, *, live: bool) -> tuple[dict[str, dict[str, str]], dict[str, str]]:
    events: dict[str, dict[str, str]] = {}
    failures: dict[str, str] = {}
    if not log_path.exists():
        return events, failures
    try:
        lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return events, failures
    for line in lines:
        ts_match = re.match(r"^(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}(?:,\d+)?)", line)
        sid_match = re.search(r"\bsignal_id=([A-Za-z0-9_-]+)", line)
        if sid_match is None:
            continue
        signal_id = sid_match.group(1).strip()
        ticker_match = re.search(r"\bticker=([A-Za-z0-9&._-]+)", line)
        ticker = ticker_match.group(1).upper() if ticker_match else ""
        event_match = "[LIVE][ENTRY.REBASE]" in line if live else "[ENTRY.NEW]" in line
        if event_match and ts_match:
            events[signal_id] = {
                "time": ts_match.group(1).replace(",", "."),
                "ticker": ticker,
                "line": line,
            }
        upper_line = line.upper()
        if any(token in upper_line for token in ("[ERROR", "[SKIP", "[REJECT", "FAILED")):
            failures[signal_id] = re.sub(r"^\S+\s+\S+\s+\|\s+\w+\s+\|\s*", "", line).strip()
    return events, failures


def _v7_monitor_find_signal_event(events: dict[str, dict[str, str]], signal_id: str) -> Optional[dict[str, str]]:
    signal_id = str(signal_id or "").strip()
    if not signal_id:
        return None
    if signal_id in events:
        return events[signal_id]
    for event_id, event in events.items():
        if signal_id.startswith(event_id) or event_id.startswith(signal_id):
            return event
    return None


def _v7_monitor_task_disabled(status: dict[str, str]) -> bool:
    values = (
        status.get("scheduler_status"),
        status.get("scheduler_state"),
        status.get("status"),
    )
    return any(str(value or "").strip().upper() == "DISABLED" for value in values)


def _v7_monitor_status_for(card_id: str, task_snapshot: Dict[str, Dict[str, str]]) -> Dict[str, str]:
    status = parse_status_file(_resolve_status_path(STATUS_FILES[card_id])) if card_id in STATUS_FILES else {}
    heartbeat = parse_status_file(_resolve_status_path(HEARTBEAT_FILES[card_id])) if card_id in HEARTBEAT_FILES else {}
    if heartbeat:
        status = merge_runtime_status(status, heartbeat)
    status = apply_scheduler_status(card_id, status, task_snapshot)
    status = infer_pid_session_provenance(card_id, status)
    return status


def _format_v7_live_5min_monitor(
    today_ist: str,
    task_snapshot: Dict[str, Dict[str, str]],
) -> tuple[str, Dict[str, str]]:
    date_key = today_ist.replace("-", "")
    now_ist = dt.datetime.now(IST)
    entry_root = runtime_dir("entry_engine_1min_v5_ID")
    entry_audit_dir = entry_root / "audit"
    latest_summary_path = entry_root / "latest" / "latest_summary.json"
    latest_summary = _read_json_dict(latest_summary_path)
    slots: dict[str, dict[str, Any]] = {}
    setup_stats: dict[tuple[str, str], dict[str, Any]] = {}
    detail_rows: list[dict[str, str]] = []

    def _slot_rec(slot: str) -> dict[str, Any]:
        rec = slots.setdefault(slot, {"slot": slot})
        for set_field in (
            "tickers",
            "scan_raw_tickers",
            "potential_tickers",
            "scan_v11_rej_tickers",
            "research_rej_tickers",
            "entry_raw_tickers",
            "entry_tickers",
            "signal_tickers",
            "signal_ids",
            "paper_tickers",
            "live_tickers",
        ):
            rec.setdefault(set_field, set())
        for field in (
            "scan_raw",
            "potential",
            "scan_v11_rej",
            "research_rej",
            "entry_raw",
            "v11_rej",
            "pre_rej",
            "pre_pass",
            "entries",
            "signals",
            "paper",
            "target",
            "sl",
            "eod",
            "skips",
            "open",
            "pnl",
            "pnl_n",
            "short_written",
            "long_written",
            "short_sig",
            "long_sig",
        ):
            rec.setdefault(field, 0)
        return rec

    def _add_scanner_rows(path: Path, field: str) -> int:
        rows = _read_csv_tail_rows(path, limit=50000)
        for row in rows:
            slot = _v7_monitor_slot_label(_pick_csv_value(row, ("scan_slot_ist", "signal_time_ist", "created_at_ist")))
            if not slot:
                continue
            rec = _slot_rec(slot)
            rec[field] = int(rec.get(field, 0) or 0) + 1
            ticker = str(row.get("ticker", "")).upper().strip()
            if ticker:
                rec["tickers"].add(ticker)
                rec[f"{field}_tickers"].add(ticker)
            setup_field = {
                "scan_raw": "raw",
                "potential": "potential",
                "scan_v11_rej": "v11_rej",
                "research_rej": "research_rej",
            }.get(field)
            if setup_field:
                _v7_monitor_add_setup_count(setup_stats, row, setup_field)
        return len(rows)

    raw_candidates_path = SIGNAL_DISCOVERY_V7_CSV_DIR / f"raw_candidate_tickers_{today_ist}.csv"
    candidate_path = SIGNAL_DISCOVERY_V7_CSV_DIR / f"candidate_tickers_{today_ist}.csv"
    scan_v11_rej_path = SIGNAL_DISCOVERY_V7_CSV_DIR / f"v11_overlay_rejected_candidate_tickers_{today_ist}.csv"
    research_rej_path = SIGNAL_DISCOVERY_V7_CSV_DIR / f"research_filter_rejected_candidate_tickers_{today_ist}.csv"
    raw_scan_total = _add_scanner_rows(raw_candidates_path, "scan_raw")
    potential_total = _add_scanner_rows(candidate_path, "potential")
    scan_v11_rej_total = _add_scanner_rows(scan_v11_rej_path, "scan_v11_rej")
    research_rej_total = _add_scanner_rows(research_rej_path, "research_rej")
    scanner_audit_paths = sorted(
        (SIGNAL_DISCOVERY_V7_ROOT / "audit").glob(f"candidate_tickers_audit_{today_ist}*.csv"),
        key=lambda path: path.stat().st_mtime if path.exists() else 0.0,
    )
    scanner_audit_rows: list[dict[str, str]] = []
    for scanner_audit_path in scanner_audit_paths:
        scanner_audit_rows.extend(_read_csv_tail_rows(scanner_audit_path, limit=5000))
    for row in scanner_audit_rows:
        slot = _v7_monitor_slot_label(row.get("slot_ist", ""))
        if not slot:
            continue
        rec = _slot_rec(slot)
        scan_sec = _to_float_or_nan(row.get("elapsed_sec", ""))
        scan_lag = _v7_monitor_sec_between(row.get("slot_ist", ""), row.get("created_at_ist", ""))
        if not math.isnan(scan_sec):
            rec["scan_sec"] = float(scan_sec)
        if not math.isnan(scan_lag):
            rec["scan_lag_sec"] = float(scan_lag)
        rec["scan_created"] = _extract_time_only(str(row.get("created_at_ist", "")))
        rec["scanner_audit_seen"] = True
        for source_key, target_key in (
            ("candidate_count", "candidate_count"),
            ("raw_candidate_count", "raw_candidate_count"),
            ("v11_tier123_live_scan_elapsed_sec", "tier123_scan_sec"),
        ):
            value = _to_float_or_nan(str(row.get(source_key, "")))
            if not math.isnan(value):
                rec[target_key] = int(value) if source_key != "v11_tier123_live_scan_elapsed_sec" else float(value)

    for marker_path in sorted(SLOT_READY_5M_DIR.glob(f"slot_{date_key}_*.json")):
        marker = _read_json_dict(marker_path)
        slot = _v7_monitor_slot_label(marker.get("slot_ist") or marker_path.stem)
        if not slot:
            path_match = re.search(r"_(\d{2})(\d{2})$", marker_path.stem)
            slot = f"{path_match.group(1)}:{path_match.group(2)}" if path_match else ""
        if not slot:
            continue
        rec = _slot_rec(slot)
        rec["fetch_marker_seen"] = True
        rec["fetch_complete"] = bool(marker.get("complete"))
        fetch_failed = _to_float_or_nan(str(marker.get("tickers_failed", "0")))
        verify_failed = _to_float_or_nan(str(marker.get("verification_failed_count", "0")))
        rec["fetch_failed"] = 0 if math.isnan(fetch_failed) else int(fetch_failed)
        rec["fetch_verify_failed"] = 0 if math.isnan(verify_failed) else int(verify_failed)
        fetch_lag = _v7_monitor_sec_between(marker.get("slot_ist", ""), marker.get("published_at_ist", ""))
        if not math.isnan(fetch_lag):
            rec["fetch_lag_sec"] = float(fetch_lag)
        duration_ms = _to_float_or_nan(str(marker.get("duration_ms", "")))
        if not math.isnan(duration_ms):
            rec["fetch_duration_sec"] = float(duration_ms) / 1000.0

    audit_jsonl = entry_audit_dir / f"entry_engine_audit_{today_ist}.jsonl"
    audit_slots = 0
    pre_version_seen = ""
    if audit_jsonl.exists():
        try:
            lines = audit_jsonl.read_text(encoding="utf-8", errors="replace").splitlines()
        except OSError:
            lines = []
        for line in lines:
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(payload, dict):
                continue
            slot = _v7_monitor_slot_label(payload.get("slot_ist") or payload.get("candidate_snapshot_slot_ist"))
            if not slot:
                continue
            audit_slots += 1
            rec = _slot_rec(slot)
            rec["entry_audit_seen"] = True
            rec["candidate_snapshot_ready"] = bool(payload.get("candidate_snapshot_ready"))
            rec["pre_momentum_gate_enabled"] = bool(payload.get("pre_momentum_gate_enabled"))
            rec["raw_fetch_failures"] = int(payload.get("raw_fetch_failures", 0) or 0)
            for source_key, target_key in (
                ("candidate_count", "candidate_count"),
                ("tickers_requested", "tickers_requested"),
                ("tickers_fetched", "tickers_fetched"),
                ("raw_entry_rows", "entry_raw"),
                ("v11_entry_rejected_rows", "v11_rej"),
                ("pre_momentum_rejected_rows", "pre_rej"),
                ("pre_momentum_filtered_entry_rows", "pre_pass"),
                ("selected_entry_rows", "entries"),
                ("entry_rows", "entry_rows"),
                ("short_written", "short_written"),
                ("long_written", "long_written"),
            ):
                value = _to_float_or_nan(str(payload.get(source_key, "")))
                if not math.isnan(value):
                    rec[target_key] = int(value)
            if not int(rec.get("entries", 0) or 0) and int(rec.get("entry_rows", 0) or 0):
                rec["entries"] = int(rec.get("entry_rows", 0) or 0)
            rec["signals"] = int(rec.get("short_written", 0) or 0) + int(rec.get("long_written", 0) or 0)
            for source_key, target_key in (
                ("elapsed_sec", "elapsed_sec"),
                ("candidate_wait_sec", "candidate_wait_sec"),
                ("candidate_load_elapsed_sec", "candidate_load_sec"),
                ("raw_fetch_elapsed_sec", "raw_fetch_sec"),
                ("entry_scan_elapsed_sec", "entry_scan_sec"),
                ("entry_reject_audit_elapsed_sec", "entry_reject_sec"),
                ("v11_entry_overlay_elapsed_sec", "v11_sec"),
                ("pre_momentum_gate_elapsed_sec", "pre_gate_sec"),
                ("entry_select_elapsed_sec", "select_sec"),
                ("audit_csv_write_elapsed_sec", "audit_csv_sec"),
                ("setup_exit_rules_write_elapsed_sec", "rules_csv_sec"),
                ("live_signal_csv_write_elapsed_sec", "live_csv_sec"),
                ("short_signal_write_elapsed_sec", "short_csv_sec"),
                ("long_signal_write_elapsed_sec", "long_csv_sec"),
            ):
                value = _to_float_or_nan(str(payload.get(source_key, "")))
                if not math.isnan(value):
                    rec[target_key] = float(value)
            rec["raw_fetch_mode"] = str(payload.get("raw_fetch_mode", rec.get("raw_fetch_mode", "")) or "")
            if payload.get("pre_momentum_version"):
                pre_version_seen = str(payload.get("pre_momentum_version", ""))
                rec["pre_version"] = pre_version_seen

    for path in sorted(entry_audit_dir.glob(f"entry_rows_raw_candidates_{date_key}_*.csv")):
        rows = _v7_monitor_slot_rows(path, date_key, "entry_raw", slots)
        slot = _v7_monitor_slot_from_path(path, date_key)
        rec = _slot_rec(slot) if slot else None
        for row in rows:
            _v7_monitor_add_setup_count(setup_stats, row, "entry_raw")
            ticker = str(row.get("ticker", "")).upper().strip()
            if rec is not None and ticker:
                rec["entry_raw_tickers"].add(ticker)
    for path in sorted(entry_audit_dir.glob(f"entry_rejected_v11_overlay_{date_key}_*.csv")):
        rows = _v7_monitor_slot_rows(path, date_key, "v11_rej", slots)
        for row in rows:
            _v7_monitor_add_setup_count(setup_stats, row, "v11_rej")
    for path in sorted(entry_audit_dir.glob(f"entry_rejected_pre_momentum_{date_key}_*.csv")):
        rows = _v7_monitor_slot_rows(path, date_key, "pre_rej", slots)
        for row in rows:
            _v7_monitor_add_setup_count(setup_stats, row, "pre_rej")
    for path in sorted(entry_audit_dir.glob(f"entry_rows_{date_key}_*.csv")):
        rows = _v7_monitor_slot_rows(path, date_key, "entries", slots)
        slot = _v7_monitor_slot_from_path(path, date_key)
        rec = _slot_rec(slot) if slot else None
        completed_at = _v7_monitor_mtime(path)
        if rec is not None and completed_at is not None:
            rec["entry_completed_at"] = completed_at
        for row in rows:
            _v7_monitor_add_setup_count(setup_stats, row, "entries")
            _v7_monitor_add_setup_count(setup_stats, row, "pre_pass")
            ticker = str(row.get("ticker", "")).upper().strip()
            if rec is not None and ticker:
                rec["entry_tickers"].add(ticker)
            signal_time = _pick_csv_value(row, ("signal_time_ist", "bar_time_ist"))
            raw_fetch_lag = _v7_monitor_raw_fetch_lag_sec(signal_time, row.get("ticker", ""))
            detail_rows.append(
                {
                    "state": "ENTRY",
                    "ticker": str(row.get("ticker", "")).upper(),
                    "side": str(row.get("side", "")).upper(),
                    "setup": str(row.get("setup", "")),
                    "signal": _extract_time_only(signal_time),
                    "entry": _fmt_price(_v7_monitor_num(row, ("entry_price", "v7_signal_entry_price"))),
                    "sl": _fmt_price(_v7_monitor_num(row, ("sl_price", "v7_signal_stop_price"))),
                    "target": _fmt_price(_v7_monitor_num(row, ("target_price", "v7_signal_target_price"))),
                    "qty": _fmt_qty(_v7_monitor_num(row, ("quantity",))),
                    "pnl": "",
                    "outcome": "",
                    "fetch_lag": _v7_monitor_fmt_sec(raw_fetch_lag),
                    "csv_lag": "",
                    "entry_lag": "",
                    "dur": "",
                }
            )

    signal_slot_counts: dict[str, int] = {}
    for side in ("short", "long"):
        signal_path = LIVE_SIGNAL_DIR / f"signals_{today_ist}_id_5min_v7_{side}.csv"
        for row in _read_csv_tail_rows(signal_path, limit=5000):
            slot = _v7_monitor_slot_label(
                _pick_csv_value(row, ("signal_datetime", "signal_entry_datetime_ist", "signal_bar_time_ist"))
            )
            if slot:
                signal_slot_counts[slot] = int(signal_slot_counts.get(slot, 0) or 0) + 1
                rec = _slot_rec(slot)
                if side == "short":
                    rec["short_sig"] = int(rec.get("short_sig", 0) or 0) + 1
                else:
                    rec["long_sig"] = int(rec.get("long_sig", 0) or 0) + 1
                signal_lag = _v7_monitor_sec_between(
                    _pick_csv_value(row, ("signal_datetime", "signal_entry_datetime_ist", "signal_bar_time_ist")),
                    _pick_csv_value(row, ("detected_time_ist", "logtime_ist", "received_time")),
                )
                if not math.isnan(signal_lag):
                    rec["sig_csv_lag_sec"] = max(float(rec.get("sig_csv_lag_sec", 0.0) or 0.0), float(signal_lag))
                signal_id = str(row.get("signal_id", "")).strip()
                ticker = str(row.get("ticker", "")).upper().strip()
                if signal_id:
                    rec["signal_ids"].add(signal_id)
                if ticker:
                    rec["signal_tickers"].add(ticker)
            _v7_monitor_add_setup_count(setup_stats, row, "signals")
            signal_time = _pick_csv_value(row, ("signal_datetime", "signal_entry_datetime_ist"))
            raw_fetch_lag = _v7_monitor_raw_fetch_lag_sec(signal_time, row.get("ticker", ""))
            csv_lag = _v7_monitor_sec_between(
                signal_time,
                _pick_csv_value(row, ("detected_time_ist", "logtime_ist", "received_time")),
            )
            detail_rows.append(
                {
                    "state": "SIGNAL",
                    "ticker": str(row.get("ticker", "")).upper(),
                    "side": str(row.get("side", "")).upper(),
                    "setup": str(row.get("setup", "")),
                    "signal": _extract_time_only(signal_time),
                    "entry": _fmt_price(_v7_monitor_num(row, ("entry_price",))),
                    "sl": _fmt_price(_v7_monitor_num(row, ("stop_price", "_stop_price"))),
                    "target": _fmt_price(_v7_monitor_num(row, ("target_price",))),
                    "qty": _fmt_qty(_v7_monitor_num(row, ("quantity",))),
                    "pnl": "",
                    "outcome": "",
                    "fetch_lag": _v7_monitor_fmt_sec(raw_fetch_lag),
                    "csv_lag": _v7_monitor_fmt_sec(csv_lag),
                    "entry_lag": "",
                    "dur": "",
                }
            )
    for slot, count in signal_slot_counts.items():
        rec = _slot_rec(slot)
        rec["signals"] = max(int(rec.get("signals", 0) or 0), count)

    def _add_trade_row(row: dict[str, str], state: str) -> None:
        signal_time = _pick_csv_value(row, ("signal_datetime", "signal_entry_datetime_ist", "signal_bar_time_ist"))
        slot = _v7_monitor_slot_label(signal_time or row.get("entry_time", ""))
        if slot:
            rec = _slot_rec(slot)
            rec["paper"] = int(rec.get("paper", 0) or 0) + 1
            entry_lag = _v7_monitor_sec_between(signal_time, row.get("entry_time", ""))
            if not math.isnan(entry_lag):
                rec["entry_lag_sec"] = max(float(rec.get("entry_lag_sec", 0.0) or 0.0), float(entry_lag))
        outcome_key = _v7_monitor_outcome_bucket(row.get("outcome", state))
        pnl = _v7_monitor_num(row, ("pnl_rs", "pnl"))
        if slot:
            rec = _slot_rec(slot)
            if outcome_key:
                rec[outcome_key] = int(rec.get(outcome_key, 0) or 0) + 1
            if not math.isnan(pnl):
                rec["pnl"] = float(rec.get("pnl", 0.0) or 0.0) + float(pnl)
                rec["pnl_n"] = int(rec.get("pnl_n", 0) or 0) + 1
        _v7_monitor_add_setup_count(setup_stats, row, "paper")
        setup_bucket = _v7_monitor_setup_bucket(setup_stats, row)
        if setup_bucket is not None:
            if outcome_key:
                setup_bucket[outcome_key] = int(setup_bucket.get(outcome_key, 0) or 0) + 1
            if not math.isnan(pnl):
                setup_bucket["pnl"] = float(setup_bucket.get("pnl", 0.0) or 0.0) + float(pnl)
                setup_bucket["pnl_n"] = int(setup_bucket.get("pnl_n", 0) or 0) + 1
        detail_rows.append(
            {
                "state": state,
                "ticker": str(row.get("ticker", "")).upper(),
                "side": str(row.get("side", "")).upper(),
                "setup": str(row.get("setup", "")),
                "signal": _extract_time_only(signal_time),
                "entry": _fmt_price(_v7_monitor_num(row, ("entry_price", "filled_price"))),
                "sl": _fmt_price(_v7_monitor_num(row, ("stop_price",))),
                "target": _fmt_price(_v7_monitor_num(row, ("target_price",))),
                "qty": _fmt_qty(_v7_monitor_num(row, ("quantity",))),
                "pnl": _fmt_rs(pnl) if not math.isnan(pnl) else "",
                "outcome": str(row.get("outcome", "") or state),
                "fetch_lag": _v7_monitor_fmt_sec(_v7_monitor_raw_fetch_lag_sec(signal_time, row.get("ticker", ""))),
                "csv_lag": "",
                "entry_lag": _v7_monitor_fmt_sec(_v7_monitor_sec_between(signal_time, row.get("entry_time", ""))),
                "dur": _v7_monitor_fmt_duration(row.get("entry_time", ""), row.get("exit_time", ""), now=now_ist),
            }
        )

    paper_path = LIVE_SIGNAL_DIR / f"paper_trades_{today_ist}_id_5min_v7.csv"
    for row in _read_csv_tail_rows(paper_path, limit=5000):
        _add_trade_row(row, str(row.get("outcome", "") or "PAPER").upper())

    live_trade_path = LIVE_SIGNAL_DIR / f"live_trades_{today_ist}_id_5min_v7.csv"
    for row in _read_csv_tail_rows(live_trade_path, limit=5000):
        _add_trade_row(row, f"LIVE_{str(row.get('outcome', '') or 'TRADE').upper()}")

    signal_rows = _read_signal_setup_map(today_ist)
    state_payload = _read_json_dict(LIVE_SIGNAL_DIR / f"open_trades_state_{today_ist}_id_5min_v7.json")
    latest_log_path = LIVE_SIGNAL_DIR / f"paper_trade_execution_{today_ist}_id_5min_v7.log"
    latest_pnl = _parse_latest_live_pnl_line(latest_log_path)
    ticker_pnl = latest_pnl.get("ticker_pnl", {}) if isinstance(latest_pnl.get("ticker_pnl", {}), dict) else {}
    for trade in [r for r in state_payload.get("open_trades", []) if isinstance(r, dict)]:
        signal_id = str(trade.get("signal_id", "")).strip()
        signal = signal_rows.get(signal_id, {})
        merged_row = {
            "ticker": str(trade.get("ticker", "") or signal.get("ticker", "")),
            "side": str(trade.get("side", "") or signal.get("side", "")),
            "setup": str(signal.get("setup", "")),
            "signal_datetime": str(signal.get("signal_datetime", "") or signal.get("signal_entry_datetime_ist", "")),
            "entry_time": str(trade.get("entry_time", "")),
            "entry_price": str(trade.get("entry_price", "")),
            "stop_price": str(trade.get("stop_price", "")),
            "target_price": str(trade.get("target_price", "")),
            "quantity": str(trade.get("quantity", "")),
            "outcome": "OPEN",
        }
        ticker = str(merged_row["ticker"]).upper().strip()
        pnl = _to_float_or_nan(str(ticker_pnl.get(ticker, ""))) if ticker else float("nan")
        if math.isnan(pnl):
            pnl = _paper_open_pnl_from_state(trade)
        if not math.isnan(pnl):
            merged_row["pnl_rs"] = str(pnl)
        _add_trade_row(merged_row, "OPEN")

    paper_log_path = LIVE_SIGNAL_DIR / f"paper_trade_execution_{today_ist}_id_5min_v7.log"
    live_log_path = LIVE_SIGNAL_DIR / f"live_trade_execution_{today_ist}_id_5min_v7.log"
    paper_events, paper_failures = _v7_monitor_executor_events(paper_log_path, live=False)
    live_events, live_failures = _v7_monitor_executor_events(live_log_path, live=True)
    for trade in [r for r in state_payload.get("open_trades", []) if isinstance(r, dict)]:
        signal_id = str(trade.get("signal_id", "")).strip()
        if signal_id and _v7_monitor_find_signal_event(paper_events, signal_id) is None:
            paper_events[signal_id] = {
                "time": str(trade.get("entry_time", "")),
                "ticker": str(trade.get("ticker", "")).upper(),
                "line": "open_trades_state",
            }

    paper_task_status = _v7_monitor_status_for("paper_trade_id_5min_v7", task_snapshot)
    live_task_status = _v7_monitor_status_for("kite_trade_id_5min_v7", task_snapshot)
    paper_disabled = _v7_monitor_task_disabled(paper_task_status)
    live_disabled = _v7_monitor_task_disabled(live_task_status)

    flow_rows: list[dict[str, str]] = []
    for slot in sorted(slots):
        rec = slots[slot]
        slot_dt = _v7_monitor_slot_dt(today_ist, slot)
        if slot_dt is None:
            continue
        entry_anchor = slot_dt + dt.timedelta(seconds=60)
        due_fetch = now_ist >= slot_dt + dt.timedelta(seconds=60)
        due_entry = now_ist >= entry_anchor + dt.timedelta(seconds=75)
        reasons: list[str] = []

        fetch_lag = _to_float_or_nan(str(rec.get("fetch_lag_sec", "")))
        fetch_ok = bool(rec.get("fetch_marker_seen")) and bool(rec.get("fetch_complete")) and not math.isnan(fetch_lag)
        if fetch_ok:
            fetch_state = "YES" if fetch_lag <= 60.0 else "LATE"
            if fetch_lag > 60.0:
                reasons.append(f"fetch_late={fetch_lag:.1f}s")
            if int(rec.get("fetch_failed", 0) or 0) or int(rec.get("fetch_verify_failed", 0) or 0):
                fetch_state = "NO"
                reasons.append(
                    f"fetch_fail={int(rec.get('fetch_failed', 0) or 0)},"
                    f"verify={int(rec.get('fetch_verify_failed', 0) or 0)}"
                )
        elif due_fetch:
            fetch_state = "NO"
            reasons.append("fetch_marker_missing")
        else:
            fetch_state = "WAIT"

        scan_lag = _to_float_or_nan(str(rec.get("scan_lag_sec", "")))
        scan_sec = _to_float_or_nan(str(rec.get("scan_sec", "")))
        scan_seen = bool(rec.get("scanner_audit_seen"))
        if scan_seen and not math.isnan(scan_lag):
            scan_state = "YES" if scan_lag <= 60.0 else "LATE"
            if scan_lag > 60.0:
                reasons.append(f"candidate_late={scan_lag:.1f}s")
        elif due_fetch:
            scan_state = "NO"
            reasons.append("candidate_snapshot_missing")
        else:
            scan_state = "WAIT"
        # Scanner and fetcher run on independent cadences (the scanner does not
        # gate on slot_ready_5m), so they routinely finish within a second or two
        # of each other and the scanner's audit row simply flushes first. That
        # marker-ordering skew is noise; only a meaningfully-early scan (the
        # scanner finished well before the fetch published, i.e. it likely read
        # pre-/partially-fetched data) is a real ordering risk worth a WARN.
        SCAN_BEFORE_FETCH_TOLERANCE_SEC = 10.0
        if fetch_ok and scan_seen and scan_lag + SCAN_BEFORE_FETCH_TOLERANCE_SEC < fetch_lag:
            reasons.append(f"scan_before_fetch={fetch_lag - scan_lag:.1f}s")

        candidate_count = int(rec.get("candidate_count", rec.get("potential", 0)) or 0)
        candidate_state = (
            f"YES({candidate_count})" if scan_seen and not math.isnan(scan_lag) and scan_lag <= 60.0
            else f"LATE({candidate_count})" if scan_seen
            else "NO"
        )

        entry_seen = bool(rec.get("entry_audit_seen"))
        completed_at = rec.get("entry_completed_at")
        entry_after_anchor = (
            max(0.0, (completed_at - entry_anchor).total_seconds())
            if isinstance(completed_at, dt.datetime)
            else float("nan")
        )
        requested = int(rec.get("tickers_requested", 0) or 0)
        fetched = int(rec.get("tickers_fetched", 0) or 0)
        entries = int(rec.get("entries", 0) or 0)
        signals = int(rec.get("signals", 0) or 0)
        pre_enabled_for_slot = bool(rec.get("pre_momentum_gate_enabled"))
        if entry_seen:
            engine_state = "YES" if candidate_count else "YES/NO_CAND"
            if not bool(rec.get("candidate_snapshot_ready")):
                engine_state = "NO"
                reasons.append("entry_snapshot_not_ready")
            if requested != fetched or int(rec.get("raw_fetch_failures", 0) or 0):
                engine_state = "NO"
                reasons.append(
                    f"1m_fetch={fetched}/{requested},fail={int(rec.get('raw_fetch_failures', 0) or 0)}"
                )
            if candidate_count and not pre_enabled_for_slot:
                engine_state = "NO"
                reasons.append("pre_momentum_disabled")
            if entries != signals:
                engine_state = "NO"
                reasons.append(f"entry_signal_mismatch={entries}/{signals}")
        elif due_entry:
            engine_state = "NO"
            reasons.append("entry_audit_missing")
        else:
            engine_state = "WAIT"

        engine_path = "/".join(
            (
                "L:Y" if entry_seen and bool(rec.get("candidate_snapshot_ready")) else "L:N",
                "F:Y" if entry_seen and requested == fetched and not int(rec.get("raw_fetch_failures", 0) or 0) else "F:N",
                "S:Y" if entry_seen else "S:N",
                "P:Y" if pre_enabled_for_slot else "P:N",
                "C:Y" if entry_seen and entries == signals else "C:N",
            )
        )

        signal_ids = sorted(str(value) for value in rec.get("signal_ids", set()))
        paper_event_list = [
            event for signal_id in signal_ids
            if (event := _v7_monitor_find_signal_event(paper_events, signal_id)) is not None
        ]
        live_event_list = [
            event for signal_id in signal_ids
            if (event := _v7_monitor_find_signal_event(live_events, signal_id)) is not None
        ]

        def _executor_delay(events: list[dict[str, str]]) -> float:
            values = [
                max(0.0, (_v7_monitor_parse_dt(event.get("time")) - entry_anchor).total_seconds())
                for event in events
                if _v7_monitor_parse_dt(event.get("time")) is not None
            ]
            return max(values) if values else float("nan")

        paper_delay = _executor_delay(paper_event_list)
        live_delay = _executor_delay(live_event_list)
        if not signal_ids:
            paper_state = "N/A"
            live_state = "OFF" if live_disabled else "N/A"
        elif paper_disabled:
            paper_state = "OFF"
            reasons.append("PAPER_TRADE_TRUE task disabled")
        elif len(paper_event_list) == len(signal_ids):
            paper_state = "YES"
        elif paper_event_list:
            paper_state = "PART"
            reasons.append(f"paper_partial={len(paper_event_list)}/{len(signal_ids)}")
        else:
            paper_state = "NO"
            paper_reason = next(
                (
                    reason for signal_id in signal_ids
                    for event_id, reason in paper_failures.items()
                    if signal_id.startswith(event_id) or event_id.startswith(signal_id)
                ),
                "",
            )
            reasons.append(paper_reason or "paper_signal_not_consumed")

        if signal_ids and live_disabled:
            live_state = "OFF"
        elif not signal_ids:
            live_state = "N/A"
        elif len(live_event_list) == len(signal_ids):
            live_state = "YES"
        elif live_event_list:
            live_state = "PART"
            reasons.append(f"live_partial={len(live_event_list)}/{len(signal_ids)}")
        else:
            live_state = "NO"
            live_reason = next(
                (
                    reason for signal_id in signal_ids
                    for event_id, reason in live_failures.items()
                    if signal_id.startswith(event_id) or event_id.startswith(signal_id)
                ),
                "",
            )
            reasons.append(live_reason or "live_signal_not_consumed")

        potential_tickers = sorted(rec.get("potential_tickers", set()))
        entry_tickers = sorted(rec.get("entry_tickers", set()))
        signal_tickers = sorted(rec.get("signal_tickers", set()))
        paper_tickers = sorted(
            {str(event.get("ticker", "")).upper() for event in paper_event_list if event.get("ticker")}
        )
        live_tickers = sorted(
            {str(event.get("ticker", "")).upper() for event in live_event_list if event.get("ticker")}
        )
        ticker_flow = (
            f"C:{','.join(potential_tickers) or '-'}>"
            f"E:{','.join(entry_tickers) or '-'}>"
            f"S:{','.join(signal_tickers) or '-'}>"
            f"P:{','.join(paper_tickers) or '-'}>"
            f"L:{','.join(live_tickers) or '-'}"
        )
        hard_failure = any(
            state in {"NO", "LATE", "PART"}
            for state in (fetch_state, scan_state, engine_state, paper_state, live_state)
            if state not in {"OFF", "N/A", "YES/NO_CAND"}
        )
        waiting = any(state == "WAIT" for state in (fetch_state, scan_state, engine_state))
        flow_state = (
            "WAIT"
            if waiting
            else "BLOCKED"
            if hard_failure
            else "WARN"
            if reasons
            else "PASS"
            if signal_ids
            else "IDLE"
        )
        flow_rows.append(
            {
                "slot": slot,
                "fetch5m": fetch_state,
                "fetch_s": _v7_monitor_fmt_sec(fetch_lag),
                "scan5m": scan_state,
                "scan_s": _v7_monitor_fmt_sec(scan_sec),
                "cand_print": candidate_state,
                "cand_s": _v7_monitor_fmt_sec(scan_lag),
                "engine1m": engine_state,
                "eng_s+1m": _v7_monitor_fmt_sec(entry_after_anchor),
                "L/F/S/P/C": engine_path,
                "entries/sig": f"{entries}/{signals}",
                "paper_T": paper_state,
                "paper_s+1m": _v7_monitor_fmt_sec(paper_delay),
                "paper_F": live_state,
                "live_s+1m": _v7_monitor_fmt_sec(live_delay),
                "flow": flow_state,
                "blocker/reason": "; ".join(dict.fromkeys(reasons)) or "-",
                "tickers C>E>S>P>L": ticker_flow,
            }
        )
    flow_rows = flow_rows[-24:]

    slot_rows: list[dict[str, str]] = []
    for slot in sorted(slots):
        rec = slots[slot]
        tickers = rec.get("tickers")
        ticker_count = len(tickers) if isinstance(tickers, set) else 0
        pnl = float(rec.get("pnl", 0.0) or 0.0)
        pnl_n = int(rec.get("pnl_n", 0) or 0)
        elapsed = _to_float_or_nan(str(rec.get("elapsed_sec", "")))
        raw_fetch = _to_float_or_nan(str(rec.get("raw_fetch_sec", "")))
        wait = _to_float_or_nan(str(rec.get("candidate_wait_sec", "")))
        load = _to_float_or_nan(str(rec.get("candidate_load_sec", "")))
        entry_scan = _to_float_or_nan(str(rec.get("entry_scan_sec", "")))
        audit_csv = _to_float_or_nan(str(rec.get("audit_csv_sec", "")))
        live_csv = _to_float_or_nan(str(rec.get("live_csv_sec", "")))
        scan_lag = _to_float_or_nan(str(rec.get("scan_lag_sec", "")))
        scan_sec = _to_float_or_nan(str(rec.get("scan_sec", "")))
        sig_lag = _to_float_or_nan(str(rec.get("sig_csv_lag_sec", "")))
        entry_lag = _to_float_or_nan(str(rec.get("entry_lag_sec", "")))
        slot_rows.append(
            {
                "slot": slot,
                "tickers": str(ticker_count or rec.get("tickers_requested", "") or ""),
                "scan_lag": _v7_monitor_fmt_sec(scan_lag),
                "scan_sec": _v7_monitor_fmt_sec(scan_sec),
                "scan_raw": str(int(rec.get("scan_raw", 0) or 0)),
                "pot": str(int(rec.get("potential", 0) or rec.get("candidate_count", 0) or 0)),
                "scan_v11rej": str(int(rec.get("scan_v11_rej", 0) or 0)),
                "res_rej": str(int(rec.get("research_rej", 0) or 0)),
                "entry_raw": str(int(rec.get("entry_raw", 0) or 0)),
                "v11_rej": str(int(rec.get("v11_rej", 0) or 0)),
                "pre_rej": str(int(rec.get("pre_rej", 0) or 0)),
                "pre_pass": str(int(rec.get("pre_pass", 0) or 0)),
                "entries": str(int(rec.get("entries", 0) or 0)),
                "sig": str(int(rec.get("signals", 0) or 0)),
                "short": str(int(rec.get("short_written", 0) or rec.get("short_sig", 0) or 0)),
                "long": str(int(rec.get("long_written", 0) or rec.get("long_sig", 0) or 0)),
                "paper": str(int(rec.get("paper", 0) or 0)),
                "tgt": str(int(rec.get("target", 0) or 0)),
                "sl": str(int(rec.get("sl", 0) or 0)),
                "eod": str(int(rec.get("eod", 0) or 0)),
                "open": str(int(rec.get("open", 0) or 0)),
                "pnl": _fmt_rs(pnl) if pnl_n else "",
                "entry_sec": _v7_monitor_fmt_sec(elapsed),
                "load": _v7_monitor_fmt_sec(load if not math.isnan(load) else wait),
                "fetch": _fmt_indian_number(raw_fetch, decimals=2) if not math.isnan(raw_fetch) else "",
                "cand_wait": _v7_monitor_fmt_sec(wait),
                "scan": _v7_monitor_fmt_sec(entry_scan),
                "audit_csv": _v7_monitor_fmt_sec(audit_csv),
                "live_csv": _v7_monitor_fmt_sec(live_csv),
                "sig_lag": _v7_monitor_fmt_sec(sig_lag),
                "entry_lag": _v7_monitor_fmt_sec(entry_lag),
            }
        )
    slot_rows = slot_rows[-24:]

    setup_rows: list[dict[str, str]] = []
    for bucket in setup_stats.values():
        pnl = float(bucket.get("pnl", 0.0) or 0.0)
        pnl_n = int(bucket.get("pnl_n", 0) or 0)
        setup_rows.append(
            {
                "side": str(bucket.get("side", "")),
                "setup": str(bucket.get("setup", "")),
                "raw": str(int(bucket.get("raw", 0) or 0)),
                "pot": str(int(bucket.get("potential", 0) or 0)),
                "entry_raw": str(int(bucket.get("entry_raw", 0) or 0)),
                "pre_rej": str(int(bucket.get("pre_rej", 0) or 0)),
                "pre_pass": str(int(bucket.get("pre_pass", 0) or 0)),
                "entries": str(int(bucket.get("entries", 0) or 0)),
                "sig": str(int(bucket.get("signals", 0) or 0)),
                "paper": str(int(bucket.get("paper", 0) or 0)),
                "T/SL/EOD": f"{int(bucket.get('target', 0) or 0)}/{int(bucket.get('sl', 0) or 0)}/{int(bucket.get('eod', 0) or 0)}",
                "pnl": _fmt_rs(pnl) if pnl_n else "",
                "q": _v7_monitor_fmt_avg(bucket, "q", decimals=1),
                "rank": _v7_monitor_fmt_avg(bucket, "rank", decimals=3),
                "rs": _v7_monitor_fmt_avg(bucket, "rs", decimals=2, signed=True),
                "vol": _v7_monitor_fmt_avg(bucket, "vol", decimals=2),
                "atr": _v7_monitor_fmt_avg(bucket, "atr", decimals=4),
                "pre": _v7_monitor_fmt_avg(bucket, "pre", decimals=1),
                "adx": _v7_monitor_fmt_avg(bucket, "adx", decimals=1),
                "rsi": _v7_monitor_fmt_avg(bucket, "rsi", decimals=1),
            }
        )
    setup_rows.sort(
        key=lambda r: (
            -_to_float_or_nan(r.get("entries", "0")),
            -_to_float_or_nan(r.get("sig", "0")),
            -_to_float_or_nan(r.get("pot", "0")),
            r.get("side", ""),
            r.get("setup", ""),
        )
    )
    setup_rows = setup_rows[:18]
    detail_rows = detail_rows[-30:]

    session_rows: list[dict[str, str]] = []
    for card_id, label in (
        ("signal_discovery_v7_5min_id", "scanner"),
        ("entry_engine_1min_v5_id", "entry"),
        ("live_combined_csv_id_5min_v7_persistent", "legacy_scan"),
        ("v7_research_layer", "research"),
        ("daily_live_v7_research_session", "daily_research"),
        ("v7_pre_momentum_filter_analyst", "pre_mom_analyst"),
        ("paper_trade_id_5min_v7", "paper"),
        ("kite_trade_id_5min_v7", "kite"),
    ):
        status = _v7_monitor_status_for(card_id, task_snapshot)
        started = (
            _parse_status_datetime(status.get("start_ts_utc"))
            or _parse_status_datetime(status.get("start_ts"))
            or _parse_status_datetime(status.get("worker_start_utc"))
        )
        runtime = _v7_monitor_fmt_duration(started.isoformat(), "", now=now_ist) if started else ""
        heartbeat_age = str(status.get("heartbeat_idle_sec", "")).strip()
        session_rows.append(
            {
                "session": label,
                "status": str(status.get("status") or status.get("scheduler_status") or ""),
                "phase": str(status.get("phase") or status.get("heartbeat_state") or ""),
                "pid": str(status.get("pid", "")),
                "runtime": runtime,
                "hb_idle": heartbeat_age,
                "slot": str(status.get("slot") or status.get("target_slot") or status.get("candidate_snapshot_slot_ist") or ""),
                "next": str(status.get("scheduler_next_run", "")),
            }
        )

    total_pnl = sum(float(rec.get("pnl", 0.0) or 0.0) for rec in slots.values())
    total_pnl_n = sum(int(rec.get("pnl_n", 0) or 0) for rec in slots.values())
    latest_slot = str(latest_summary.get("slot_ist") or latest_summary.get("candidate_snapshot_slot_ist") or "")
    latest_slot_label = _v7_monitor_slot_label(latest_slot)
    pre_version = str(
        latest_summary.get("pre_momentum_version")
        or latest_summary.get("pre_momentum_gate_version")
        or pre_version_seen
        or ""
    )
    pre_enabled = str(latest_summary.get("pre_momentum_gate_enabled", "")).strip()
    v11_enabled = str(latest_summary.get("v11_entry_overlay_enabled", "")).strip()
    status = {
        "status": "READY" if slots or latest_summary else "WAITING_OUTPUT",
        "session": "V7 ID 5min live monitor",
        "slot": latest_slot,
        "scan_raw": str(raw_scan_total),
        "potential": str(potential_total),
        "pre_momentum_gate": "ON" if pre_enabled.lower() == "true" else pre_enabled,
        "v11_entry_overlay": "ON" if v11_enabled.lower() == "true" else v11_enabled,
        "flow_status": flow_rows[-1]["flow"] if flow_rows else "WAIT",
    }

    summary_lines = [
        (
            f"V7 ID 5min live monitor | date={today_ist} | latest_slot={latest_slot_label or 'n/a'} | "
            f"scanner_raw={raw_scan_total} | potential={potential_total} | "
            f"scan_v11_rej={scan_v11_rej_total} | research_rej={research_rej_total}"
        ),
        (
            f"entry_audit_slots={audit_slots} | pre_momentum_gate={status['pre_momentum_gate'] or 'n/a'} | "
            f"pre_version={pre_version or 'n/a'} | v11_entry_overlay={status['v11_entry_overlay'] or 'n/a'} | "
            f"paper/live_pnl={_fmt_rs(total_pnl) if total_pnl_n else 'n/a'}"
        ),
        (
            "timing_sec: scan_lag=slot_to_candidate_publish | scan_sec=scanner_elapsed | "
            "entry_sec=1min_engine_total | load=candidate_load_or_wait | fetch=raw_1m_fetch | "
            "scan=entry_row_scan | audit_csv=entry_audit_print | live_csv=short_long_signal_print | "
            "sig_lag=signal_csv_write | entry_lag=paper/live_entry_after_signal"
        ),
    ]
    flow_table = _format_fixed_table(
        flow_rows,
        (
            "slot",
            "fetch5m",
            "fetch_s",
            "scan5m",
            "scan_s",
            "cand_print",
            "cand_s",
            "engine1m",
            "eng_s+1m",
            "L/F/S/P/C",
            "entries/sig",
            "paper_T",
            "paper_s+1m",
            "paper_F",
            "live_s+1m",
            "flow",
            "blocker/reason",
            "tickers C>E>S>P>L",
        ),
        rows_meta=f"brief end-to-end flow rows_shown={len(flow_rows)} (latest 24 slots)",
        summary_lines=(
            "SLA: fetch/candidate publish <=60s from 5m slot; engine/executors measured from slot+60s.",
            "L/F/S/P/C = candidate load / raw 1m fetch / entry scan / pre-momentum / signal CSV.",
            "OFF is intentional task disablement; N/A means no signal required that executor for the slot.",
        ),
        column_max_widths={
            "blocker/reason": 72,
            "tickers C>E>S>P>L": 110,
        },
    )
    slot_table = _format_fixed_table(
        slot_rows,
        (
            "slot",
            "tickers",
            "scan_lag",
            "scan_sec",
            "scan_raw",
            "pot",
            "scan_v11rej",
            "res_rej",
            "entry_raw",
            "v11_rej",
            "pre_rej",
            "pre_pass",
            "entries",
            "sig",
            "short",
            "long",
            "paper",
            "tgt",
            "sl",
            "eod",
            "open",
            "pnl",
            "entry_sec",
            "load",
            "fetch",
            "cand_wait",
            "scan",
            "audit_csv",
            "live_csv",
            "sig_lag",
            "entry_lag",
        ),
        rows_meta=f"5-min slot funnel rows_shown={len(slot_rows)} (latest 24 slots)",
        summary_lines=summary_lines,
    )
    setup_table = _format_fixed_table(
        setup_rows,
        (
            "side",
            "setup",
            "raw",
            "pot",
            "entry_raw",
            "pre_rej",
            "pre_pass",
            "entries",
            "sig",
            "paper",
            "T/SL/EOD",
            "pnl",
            "q",
            "rank",
            "rs",
            "vol",
            "atr",
            "pre",
            "adx",
            "rsi",
        ),
        rows_meta=f"setup/indicator rows_shown={len(setup_rows)} (top by entries, signals, potential)",
    )
    detail_table = _format_fixed_table(
        detail_rows,
        (
            "state",
            "ticker",
            "side",
            "setup",
            "signal",
            "entry",
            "sl",
            "target",
            "qty",
            "pnl",
            "outcome",
            "fetch_lag",
            "csv_lag",
            "entry_lag",
            "dur",
        ),
        rows_meta=f"latest entry/trade detail rows_shown={len(detail_rows)} (signals, entries, paper/live/open)",
    )
    session_table = _format_fixed_table(
        session_rows,
        ("session", "status", "phase", "pid", "runtime", "hb_idle", "slot", "next"),
        rows_meta=f"active session timing rows_shown={len(session_rows)}",
    )
    return "\n\n".join((flow_table, slot_table, setup_table, detail_table, session_table)), status


def _format_v7_id_papertrade_runner_view(log_path: Path, today_ist: str) -> str:
    state_path = LIVE_SIGNAL_DIR / f"open_trades_state_{today_ist}_id_5min_v7.json"
    paper_csv_path = LIVE_SIGNAL_DIR / f"paper_trades_{today_ist}_id_5min_v7.csv"
    summary_path = LIVE_SIGNAL_DIR / "paper_trade_summary_id_5min_v7.json"

    state_payload = _read_json_dict(state_path)
    open_trades_raw = state_payload.get("open_trades", [])
    open_trades = [r for r in open_trades_raw if isinstance(r, dict)]
    signal_rows = _read_signal_setup_map(today_ist)
    latest_pnl = _parse_latest_live_pnl_line(log_path)
    ticker_pnl = latest_pnl.get("ticker_pnl", {})
    if not isinstance(ticker_pnl, dict):
        ticker_pnl = {}

    rows: list[dict[str, str]] = []
    open_pnl_values: list[tuple[str, float]] = []
    for trade in open_trades:
        ticker = str(trade.get("ticker", "")).upper().strip()
        signal_id = str(trade.get("signal_id", "")).strip()
        signal = signal_rows.get(signal_id, {})
        qty = _to_float_or_nan(str(trade.get("quantity", "")))
        entry = _to_float_or_nan(str(trade.get("entry_price", "")))
        last_ltp = _to_float_or_nan(str(trade.get("last_ltp", "")))
        pnl = _to_float_or_nan(str(ticker_pnl.get(ticker, ""))) if ticker else float("nan")
        if math.isnan(pnl):
            pnl = _paper_open_pnl_from_state(trade)
        pnl_pct = _paper_pnl_pct(pnl, entry, qty)
        if ticker and not math.isnan(pnl):
            open_pnl_values.append((ticker, float(pnl)))
        rows.append(
            {
                "state": "OPEN",
                "ticker": ticker,
                "side": str(trade.get("side", "")).upper(),
                "setup": str(signal.get("setup", "")),
                "qty": _fmt_qty(qty),
                "entry": _fmt_price(entry),
                "ltp_exit": _fmt_price(last_ltp),
                "pnl_rs": _fmt_rs(pnl) if not math.isnan(pnl) else "",
                "pnl_pct": _fmt_pct(pnl_pct) if not math.isnan(pnl_pct) else "",
                "sl": _fmt_price(_to_float_or_nan(str(trade.get("stop_price", "")))),
                "tgt": _fmt_price(_to_float_or_nan(str(trade.get("target_price", "")))),
                "time": _extract_time_only(str(trade.get("entry_time", ""))),
                "update": _extract_time_only(str(trade.get("last_ltp_time", state_payload.get("updated_at", "")))),
            }
        )

    closed_rows = _read_csv_tail_rows(paper_csv_path, limit=20)
    closed_pnl_total = 0.0
    closed_count_for_pnl = 0
    wins = losses = skipped = 0
    for row in closed_rows:
        outcome = str(row.get("outcome", "")).upper().strip()
        pnl = _to_float_or_nan(row.get("pnl_rs", ""))
        if not math.isnan(pnl):
            closed_pnl_total += float(pnl)
            closed_count_for_pnl += 1
            if pnl > 0:
                wins += 1
            elif pnl < 0:
                losses += 1
        if "SKIP" in outcome:
            skipped += 1
        rows.append(
            {
                "state": outcome or "CLOSED",
                "ticker": str(row.get("ticker", "")).upper(),
                "side": str(row.get("side", "")).upper(),
                "setup": str(row.get("setup", "")),
                "qty": _fmt_qty(_to_float_or_nan(row.get("quantity", ""))),
                "entry": _fmt_price(_to_float_or_nan(row.get("entry_price", ""))),
                "ltp_exit": _fmt_price(_to_float_or_nan(row.get("exit_price", ""))),
                "pnl_rs": _fmt_rs(pnl) if not math.isnan(pnl) else "",
                "pnl_pct": _fmt_pct(_to_float_or_nan(row.get("pnl_pct", ""))) if row.get("pnl_pct") else "",
                "sl": _fmt_price(_to_float_or_nan(row.get("stop_price", ""))),
                "tgt": _fmt_price(_to_float_or_nan(row.get("target_price", ""))),
                "time": _extract_time_only(str(row.get("entry_time", ""))),
                "update": _extract_time_only(str(row.get("exit_time", ""))),
            }
        )

    def _state_rank(row: dict[str, str]) -> tuple[int, float, str]:
        state = row.get("state", "")
        pnl = _to_float_or_nan(row.get("pnl_rs", ""))
        pnl_sort = float(pnl) if not math.isnan(pnl) else 0.0
        return (0 if state == "OPEN" else 1, pnl_sort, row.get("ticker", ""))

    rows = sorted(rows, key=_state_rank)
    columns = ("state", "ticker", "side", "setup", "qty", "entry", "ltp_exit", "pnl_rs", "pnl_pct", "sl", "tgt", "time", "update")

    unrealized = latest_pnl.get("unrealized")
    if not isinstance(unrealized, (int, float)):
        unrealized = sum(v for _, v in open_pnl_values)
    realized = latest_pnl.get("realized")
    total = latest_pnl.get("total")
    deployed = latest_pnl.get("deployed_margin")
    summary_payload = _read_json_dict(summary_path)
    if not isinstance(realized, (int, float)):
        realized = _to_float_or_nan(str(summary_payload.get("total_pnl_rs", "")))
    if not isinstance(total, (int, float)) and isinstance(unrealized, (int, float)) and isinstance(realized, (int, float)):
        total = float(unrealized) + float(realized)

    best = max(open_pnl_values, key=lambda x: x[1], default=("", float("nan")))
    worst = min(open_pnl_values, key=lambda x: x[1], default=("", float("nan")))
    updated = str(latest_pnl.get("log_time") or state_payload.get("updated_at") or "").strip()
    open_count = latest_pnl.get("open", len(open_trades))
    summary_lines = [
        (
            f"V7 ID 5min Papertrade | updated={updated or 'n/a'} | "
            f"open={open_count} | realized={_fmt_rs(float(realized)) if isinstance(realized, (int, float)) and not math.isnan(float(realized)) else 'n/a'} | "
            f"unrealized={_fmt_rs(float(unrealized)) if isinstance(unrealized, (int, float)) and not math.isnan(float(unrealized)) else 'n/a'} | "
            f"total={_fmt_rs(float(total)) if isinstance(total, (int, float)) and not math.isnan(float(total)) else 'n/a'}"
        ),
        (
            f"deployed_margin={_fmt_rs_plain(float(deployed)) if isinstance(deployed, (int, float)) and not math.isnan(float(deployed)) else 'n/a'} | "
            f"closed_rows={len(closed_rows)} | wins={wins} | losses={losses} | skipped={skipped} | "
            f"closed_csv_pnl={_fmt_rs(closed_pnl_total) if closed_count_for_pnl else 'n/a'}"
        ),
    ]
    if open_pnl_values:
        summary_lines.append(
            f"worst_open={worst[0]} {_fmt_rs(worst[1])} | best_open={best[0]} {_fmt_rs(best[1])}"
        )

    return _format_fixed_table(
        rows,
        columns,
        rows_meta=f"rows_shown={len(rows)} (open first by PnL, then latest closed)",
        summary_lines=summary_lines,
    )


def _format_preopen_scheduled_sessions() -> str:
    """
    Render the Preopen Healthcheck card body from the latest JSON report,
    keeping active/scheduled session (task_*) checks visible. Each row is
    bucketed into:
      RAN_TODAY       - task ran today (latest run present)
      WAITING_RUN     - enabled, first run pending OR scheduled later in session
      NOT_RUN_TODAY   - enabled but no run today by expected time
      FAILED          - task FAIL with other reason
      DISABLED        - session intentionally off
      NOT_FOUND       - schtasks query failed / task missing
    """
    json_path = LOG_DIR / "preopen_session_healthcheck_latest.json"
    if not json_path.exists():
        return "(preopen healthcheck JSON not found yet - run run_preopen_session_healthcheck.bat)"
    try:
        payload = json.loads(json_path.read_text(encoding="utf-8", errors="replace"))
    except (OSError, json.JSONDecodeError) as exc:
        return f"(unable to read preopen healthcheck JSON: {exc})"

    ts_ist = str(payload.get("ts_ist", "") or "").strip()
    overall = str(payload.get("overall", "") or "").strip()
    checks = payload.get("checks", []) or []

    task_checks = [
        c for c in checks
        if isinstance(c, dict) and str(c.get("name", "")).startswith("task_")
    ]
    if not task_checks:
        return (
            f"(no active/scheduled session (task_*) checks in latest report | "
            f"ts={ts_ist} | overall={overall})"
        )

    def _bucket(status: str, detail_l: str) -> str:
        if status == "PASS":
            if "session not enabled" in detail_l:
                return "DISABLED"
            if "ran today" in detail_l:
                return "RAN_TODAY"
            if "first run pending" in detail_l:
                return "WAITING_RUN"
            if "enabled |" in detail_l or "scheduled later" in detail_l:
                return "WAITING_RUN"
            return "RAN_TODAY"
        # FAIL / WARN
        if "not found" in detail_l or "query failed" in detail_l:
            return "NOT_FOUND"
        if "not enabled" in detail_l:
            return "DISABLED"
        if "not run today" in detail_l or "not run today yet" in detail_l:
            return "NOT_RUN_TODAY"
        return "FAILED"

    bucket_order = {
        "FAILED": 0,
        "NOT_RUN_TODAY": 1,
        "NOT_FOUND": 2,
        "WAITING_RUN": 3,
        "RAN_TODAY": 4,
        "DISABLED": 5,
    }
    counts: Dict[str, int] = {}
    hidden_disabled = 0
    rows: list[tuple[int, str, str, str]] = []
    for check in task_checks:
        status = str(check.get("status", "") or "").upper()
        detail = str(check.get("detail", "") or "")
        name = str(check.get("name", "") or "")
        session = name[len("task_"):] if name.startswith("task_") else name
        b = _bucket(status, detail.lower())
        counts[b] = counts.get(b, 0) + 1
        if b == "DISABLED":
            hidden_disabled += 1
            continue
        rows.append((bucket_order.get(b, 99), b, session, detail))
    rows.sort(key=lambda r: (r[0], r[2].lower()))

    summary_parts = [f"{k}={counts.get(k, 0)}" for k in bucket_order if counts.get(k)]
    summary = " | ".join(summary_parts) if summary_parts else "no sessions"

    out: list[str] = []
    out.append(f"preopen_healthcheck | ts_ist={ts_ist} | overall={overall}")
    out.append(
        f"active_or_scheduled_sessions={len(rows)} | "
        f"total_task_checks={len(task_checks)} | hidden_disabled={hidden_disabled} | {summary}"
    )
    out.append(f"rows_shown={len(rows)} (active/scheduled only)")
    if not rows:
        out.append("(no active or scheduled sessions found in latest report)")
        return "\n".join(out)

    bucket_w = max(len(r[1]) for r in rows)
    session_w = max(len(r[2]) for r in rows)
    out.append(f"{'bucket':{bucket_w}s} | {'session':{session_w}s} | detail")
    out.append(f"{'-' * bucket_w}-+-{'-' * session_w}-+-{'-' * 3}")
    for _, bucket, session, detail in rows:
        out.append(f"{bucket:{bucket_w}s} | {session:{session_w}s} | {detail}")
    return "\n".join(out)


def _compute_holdings_summary(path: Path) -> tuple[float, float, float, float, float, float]:
    invested = 0.0
    current = 0.0
    day_pnl = 0.0
    prev_close_value = 0.0

    if not path.exists():
        return (
            float("nan"),
            float("nan"),
            float("nan"),
            float("nan"),
            float("nan"),
            float("nan"),
        )

    try:
        with path.open("r", encoding="utf-8-sig", errors="replace", newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                if not row:
                    continue
                qty = _to_float_or_nan(_pick_csv_value(row, ("quantity",)))
                t1_qty = _to_float_or_nan(_pick_csv_value(row, ("t1_quantity",)))
                avg = _to_float_or_nan(_pick_csv_value(row, ("average_price", "avg_price", "price")))
                ltp = _to_float_or_nan(_pick_csv_value(row, ("last_price", "ltp", "close_price")))
                close_price = _to_float_or_nan(_pick_csv_value(row, ("close_price",)))
                day_change = _to_float_or_nan(_pick_csv_value(row, ("day_change",)))

                q = 0.0
                if not math.isnan(qty):
                    q += float(qty)
                if not math.isnan(t1_qty):
                    q += float(t1_qty)

                if q == 0.0:
                    continue

                if not math.isnan(avg):
                    invested += q * float(avg)
                if not math.isnan(ltp):
                    current += q * float(ltp)
                if not math.isnan(day_change):
                    day_pnl += q * float(day_change)
                elif not math.isnan(ltp) and not math.isnan(close_price):
                    day_pnl += q * (float(ltp) - float(close_price))

                if not math.isnan(close_price):
                    prev_close_value += q * float(close_price)
                elif not math.isnan(ltp) and not math.isnan(day_change):
                    prev_close_value += q * (float(ltp) - float(day_change))
    except (OSError, csv.Error):
        return (
            float("nan"),
            float("nan"),
            float("nan"),
            float("nan"),
            float("nan"),
            float("nan"),
        )

    pnl = current - invested
    pnl_pct = (pnl * 100.0 / invested) if invested > 0 else float("nan")
    day_pnl_pct = (day_pnl * 100.0 / prev_close_value) if prev_close_value > 0 else float("nan")
    return invested, current, pnl, pnl_pct, day_pnl, day_pnl_pct


def _read_kite_snapshot_meta(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8", errors="replace"))
    except (OSError, json.JSONDecodeError):
        return {}
    return raw if isinstance(raw, dict) else {}


def _extract_funds_available(meta: dict[str, object]) -> float:
    raw = meta.get("funds_available")
    try:
        if raw is None:
            return float("nan")
        return float(raw)
    except (TypeError, ValueError):
        return float("nan")


def iso_mtime(path: Path) -> Optional[str]:
    if not path.exists():
        return None
    try:
        return dt.datetime.fromtimestamp(path.stat().st_mtime).isoformat(sep=" ", timespec="seconds")
    except OSError:
        return None


def _today_ist_str() -> str:
    return dt.datetime.now(IST).date().isoformat()


def _safe_int(value: object, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return int(default)


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _kill_switch_scope_paths(scope: str, today_ist: str) -> tuple[Path, Path]:
    if scope == "false_v5":
        return (
            LIVE_SIGNAL_DIR / OPEN_LIVE_TRADES_STATE_PATTERN_V5.format(today_ist),
            KILL_SWITCH_LIVE_FILE_V5,
        )
    if scope == "true_v5":
        return (
            LIVE_SIGNAL_DIR / OPEN_PAPER_TRADES_STATE_PATTERN_V5.format(today_ist),
            KILL_SWITCH_PAPER_FILE_V5,
        )
    if scope == "false_v7_sweep":
        return (
            LIVE_SIGNAL_DIR / OPEN_LIVE_TRADES_STATE_PATTERN_V7_SWEEP.format(today_ist),
            KILL_SWITCH_LIVE_FILE_V7_SWEEP,
        )
    if scope == "true_v7_sweep":
        return (
            LIVE_SIGNAL_DIR / OPEN_PAPER_TRADES_STATE_PATTERN_V7_SWEEP.format(today_ist),
            KILL_SWITCH_PAPER_FILE_V7_SWEEP,
        )
    if scope == "false_v15":
        return (
            LIVE_SIGNAL_DIR / OPEN_LIVE_TRADES_STATE_PATTERN_V15.format(today_ist),
            KILL_SWITCH_LIVE_FILE_V15,
        )
    if scope == "true_v15":
        return (
            LIVE_SIGNAL_DIR / OPEN_PAPER_TRADES_STATE_PATTERN_V15.format(today_ist),
            KILL_SWITCH_PAPER_FILE_V15,
        )
    if scope == "false_v16_5min":
        return (
            LIVE_SIGNAL_DIR / OPEN_LIVE_TRADES_STATE_PATTERN_V16_5MIN.format(today_ist),
            KILL_SWITCH_LIVE_FILE_V16_5MIN,
        )
    if scope == "true_v16_5min":
        return (
            LIVE_SIGNAL_DIR / OPEN_PAPER_TRADES_STATE_PATTERN_V16_5MIN.format(today_ist),
            KILL_SWITCH_PAPER_FILE_V16_5MIN,
        )
    raise ValueError(f"Unknown kill-switch scope: {scope}")


def _load_open_positions(state_path: Path, today_ist: str) -> list[dict[str, object]]:
    if not state_path.exists():
        return []
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8", errors="replace"))
    except (OSError, json.JSONDecodeError):
        return []
    if not isinstance(payload, dict):
        return []
    if str(payload.get("date", "")).strip() != today_ist:
        return []
    rows = payload.get("open_trades", [])
    if isinstance(rows, dict):
        rows = list(rows.values())
    if not isinstance(rows, list):
        return []

    out: list[dict[str, object]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        signal_id = str(row.get("signal_id", "")).strip()
        ticker = str(row.get("ticker", "")).strip().upper()
        if not signal_id or not ticker:
            continue
        out.append(
            {
                "signal_id": signal_id,
                "trade_id": str(row.get("trade_id", "")).strip(),
                "ticker": ticker,
                "side": str(row.get("side", "")).strip().upper(),
                "quantity": _safe_int(row.get("quantity", 0), 0),
                "entry_price": _safe_float(row.get("filled_price", row.get("entry_price", 0.0)), 0.0),
                "stop_price": _safe_float(row.get("stop_price", 0.0), 0.0),
                "target_price": _safe_float(row.get("target_price", 0.0), 0.0),
                "entry_time": str(row.get("entry_time", "")).strip(),
                "updated_at": str(row.get("updated_at", "")).strip(),
            }
        )

    out.sort(key=lambda r: (str(r.get("ticker", "")), str(r.get("signal_id", ""))))
    return out


def _atomic_write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + f".{os.getpid()}.{id(payload)}.tmp")
    with tmp_path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp_path, path)


# Column projection specs for two-stage V16 5min CSV cards.
_PENDING_SIGNALS_V16_COLS: list[Tuple[str, Sequence[str]]] = [
    ("added_at",     ("added_at",)),
    ("ticker",       ("ticker",)),
    ("side",         ("side",)),
    ("status",       ("status",)),
    ("setup",        ("setup",)),
    ("trigger_iso",  ("trigger_bar_iso",)),
    ("entry_ist",    ("signal_entry_datetime_ist",)),
    ("expires_at",   ("expires_at",)),
    ("entry_px",     ("entry_price",)),
    ("stop_px",      ("stop_price", "sl_price")),
    ("tgt_px",       ("target_price",)),
]
_DETECTED_SIGNALS_V16_COLS: list[Tuple[str, Sequence[str]]] = [
    ("detected_at",   ("detected_time",)),
    ("ticker",        ("ticker",)),
    ("side",          ("side",)),
    ("setup",         ("setup",)),
    ("entry_px",      ("entry_price",)),
    ("stop_px",       ("stop_price", "sl_price")),
    ("tgt_px",        ("target_price",)),
    ("lag_bar_sec",   ("lag_from_signal_bar_sec", "lag_from_signal_sec")),
    ("lag_entry_sec", ("lag_from_entry_slot_sec",)),
]
_LIVE_SIGNALS_V16_COLS: list[Tuple[str, Sequence[str]]] = [
    ("signal_datetime", ("signal_datetime", "signal_entry_datetime_ist", "signal_bar_time_ist", "created_ts_ist")),
    ("detected_time_ist", ("detected_time_ist",)),
    ("ticker", ("ticker",)),
    ("side", ("side",)),
    ("entry_px", ("entry_price",)),
    ("stop_px", ("stop_price", "sl_price", "_stop_price")),
    ("tgt_px", ("target_price",)),
    ("quantity", ("quantity",)),
]


def _normalize_cli_auth_value(value: str) -> str:
    text = (value or "").strip()
    text = text.replace('\\"', '"')
    if len(text) >= 2 and text[0] == text[-1] and text[0] in {'"', "'"}:
        text = text[1:-1].strip()
    return text


class LogDashboardHandler(BaseHTTPRequestHandler):
    server_version = "EQIDV2LogDashboard/1.0"

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)

        if parsed.path == "/favicon.ico":
            self.send_response(HTTPStatus.NO_CONTENT)
            self.send_header("Cache-Control", "public, max-age=86400")
            self.end_headers()
            return

        if not self._authorized(params):
            self._unauthorized()
            return

        if parsed.path == "/":
            self._send_html()
            return
        if parsed.path == "/api/snapshot":
            lines = self._int_param(params, "lines", 80, 20, 400)
            payload = self._snapshot(lines=lines)
            self._send_json(payload)
            return
        if parsed.path == "/api/log":
            name = (params.get("name") or [""])[0]
            if name not in LOG_IDS:
                self._send_json({"error": "unknown log name"}, status=HTTPStatus.BAD_REQUEST)
                return
            lines = self._int_param(params, "lines", 150, 20, 1000)
            file_path, _ = resolve_log_target(name)
            body = tail_text(file_path, lines=lines)
            self._send_text(body)
            return

        self.send_error(HTTPStatus.NOT_FOUND, "Not Found")

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)

        if not self._authorized(params):
            self._unauthorized()
            return

        if parsed.path == "/api/kill":
            self._handle_kill_switch()
            return
        if parsed.path == "/api/restart":
            self._handle_restart_session()
            return

        self.send_error(HTTPStatus.NOT_FOUND, "Not Found")

    def log_message(self, fmt: str, *args) -> None:
        # Keep stdout useful but concise.
        super().log_message(fmt, *args)

    def _authorized(self, params: Dict[str, list[str]]) -> bool:
        api_token = getattr(self.server, "api_token", "") or ""
        provided_token = _normalize_cli_auth_value((params.get("token") or [""])[0])
        if api_token and provided_token and provided_token == api_token:
            return True

        username = self.server.username
        password = self.server.password
        if not username or not password:
            return True

        raw = self.headers.get("Authorization", "")
        if not raw.startswith("Basic "):
            return False
        token = raw[6:].strip()
        try:
            decoded = base64.b64decode(token).decode("utf-8", errors="strict")
        except Exception:
            return False
        if ":" not in decoded:
            return False
        user, pwd = decoded.split(":", 1)
        return user == username and pwd == password

    def _unauthorized(self) -> None:
        self.send_response(HTTPStatus.UNAUTHORIZED)
        self.send_header("WWW-Authenticate", 'Basic realm="EQIDV2 Logs"')
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(b"Authentication required.")

    def _read_json_body(self) -> Optional[dict[str, Any]]:
        try:
            raw_len = int(self.headers.get("Content-Length", "0") or "0")
        except (TypeError, ValueError):
            raw_len = 0
        if raw_len <= 0:
            return None
        try:
            body = self.rfile.read(raw_len)
        except Exception:
            return None
        try:
            parsed = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return None
        return parsed if isinstance(parsed, dict) else None

    def _handle_restart_session(self) -> None:
        payload = self._read_json_body()
        if payload is None:
            self._send_json(
                {"ok": False, "message": "Invalid JSON payload."},
                status=HTTPStatus.BAD_REQUEST,
            )
            return
        card_id = str(payload.get("card_id", "")).strip()
        if not card_id or card_id not in RESTARTABLE_CARDS:
            self._send_json(
                {"ok": False, "message": "Unknown or non-restartable session."},
                status=HTTPStatus.BAD_REQUEST,
            )
            return
        result = _restart_card_session(card_id)
        self._send_json(result)

    def _handle_kill_switch(self) -> None:
        payload = self._read_json_body()
        if payload is None:
            self._send_json(
                {"ok": False, "message": "Invalid JSON payload."},
                status=HTTPStatus.BAD_REQUEST,
            )
            return

        if not bool(payload.get("confirm", False)):
            self._send_json(
                {"ok": False, "message": "Confirmation checkbox is required."},
                status=HTTPStatus.BAD_REQUEST,
            )
            return

        scope = str(payload.get("scope", "")).strip().lower()
        mode = str(payload.get("mode", "")).strip().lower()
        ticker = str(payload.get("ticker", "")).strip().upper()
        if scope not in {"false_v5", "true_v5", "false_v7_sweep", "true_v7_sweep", "false_v15", "true_v15"}:
            self._send_json(
                {
                    "ok": False,
                    "message": "Invalid scope. Use false_v5, true_v5, false_v7_sweep, true_v7_sweep, false_v15, or true_v15.",
                },
                status=HTTPStatus.BAD_REQUEST,
            )
            return
        if mode not in {"all", "ticker"}:
            self._send_json(
                {"ok": False, "message": "Invalid mode. Use all or ticker."},
                status=HTTPStatus.BAD_REQUEST,
            )
            return
        if mode == "ticker" and not ticker:
            self._send_json(
                {"ok": False, "message": "Ticker is required for ticker mode."},
                status=HTTPStatus.BAD_REQUEST,
            )
            return

        today_ist = _today_ist_str()
        try:
            state_path, command_path = _kill_switch_scope_paths(scope, today_ist)
        except ValueError:
            self._send_json(
                {"ok": False, "message": "Unknown scope."},
                status=HTTPStatus.BAD_REQUEST,
            )
            return

        positions = _load_open_positions(state_path, today_ist)
        if mode == "ticker":
            positions = [p for p in positions if str(p.get("ticker", "")) == ticker]

        target_signal_ids = sorted(
            {
                str(p.get("signal_id", "")).strip()
                for p in positions
                if str(p.get("signal_id", "")).strip()
            }
        )
        target_tickers = sorted(
            {
                str(p.get("ticker", "")).strip().upper()
                for p in positions
                if str(p.get("ticker", "")).strip()
            }
        )

        if not target_signal_ids:
            label = ticker if ticker else "all symbols"
            self._send_json(
                {
                    "ok": False,
                    "message": f"No active trades found for {label} in {scope}.",
                    "scope": scope,
                    "mode": mode,
                    "ticker": ticker,
                }
            )
            return

        now_ist = dt.datetime.now(IST)
        command_id = f"KS-{scope}-{now_ist.strftime('%H%M%S%f')}"
        command_payload: dict[str, object] = {
            "date": today_ist,
            "scope": scope,
            "command_id": command_id,
            "mode": mode,
            "ticker": ticker,
            "target_signal_ids": target_signal_ids,
            "target_tickers": target_tickers,
            "requested_at_ist": now_ist.strftime("%Y-%m-%d %H:%M:%S%z"),
            "source": "dashboard",
        }
        try:
            _atomic_write_json(command_path, command_payload)
        except OSError as exc:
            self._send_json(
                {"ok": False, "message": f"Failed to write kill-switch command: {exc}"},
                status=HTTPStatus.INTERNAL_SERVER_ERROR,
            )
            return

        target_label = ticker if (mode == "ticker" and ticker) else "ALL"
        self._send_json(
            {
                "ok": True,
                "message": (
                    f"Kill switch queued for {len(target_signal_ids)} active trade(s) "
                    f"({scope}, {target_label})."
                ),
                "scope": scope,
                "mode": mode,
                "ticker": ticker,
                "command_id": command_id,
                "target_signal_ids": target_signal_ids,
                "target_tickers": target_tickers,
            }
        )

    def _send_html(self) -> None:
        api_token_json = json.dumps(getattr(self.server, "api_token", "") or "")
        html = """<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>EQIDV2 Live Logs</title>
  <style>
    :root {
      --bg: #eef2f7;
      --bg-soft: #f8fafc;
      --surface: #ffffff;
      --surface-strong: #f1f5f9;
      --surface-muted: #e7edf5;
      --card: #ffffff;
      --line: #d8e0ea;
      --line-strong: #b7c4d4;
      --text: #172033;
      --muted: #64748b;
      --muted-strong: #475569;
      --ok: #059669;
      --bad: #dc2626;
      --warn: #d97706;
      --scheduled: #2563eb;
      --disabled: #94a3b8;
      --accent: #315a8c;
      --accent-2: #0f766e;
      --log-bg: #111827;
      --log-text: #e5e7eb;
      --log-head: #1f2937;
      --shadow: 0 12px 28px rgba(15, 23, 42, 0.1);
      --shadow-soft: 0 6px 18px rgba(15, 23, 42, 0.08);
      --radius: 8px;
    }

    body[data-theme="dark"] {
      --bg: #10141b;
      --bg-soft: #141a23;
      --surface: #171e28;
      --surface-strong: #202a36;
      --surface-muted: #1d2632;
      --card: #171e28;
      --line: #2f3a48;
      --line-strong: #465467;
      --text: #edf2f7;
      --muted: #9aa8ba;
      --muted-strong: #c2ccd8;
      --ok: #10b981;
      --bad: #ef4444;
      --warn: #f59e0b;
      --scheduled: #60a5fa;
      --disabled: #64748b;
      --accent: #7aa2d6;
      --accent-2: #2dd4bf;
      --log-bg: #0b1120;
      --log-text: #e5e7eb;
      --log-head: #162033;
      --shadow: 0 14px 30px rgba(0, 0, 0, 0.28);
      --shadow-soft: 0 8px 20px rgba(0, 0, 0, 0.2);
    }

    body[data-theme="dark"] .snapshot-value,
    body[data-theme="dark"] .health-pill strong,
    body[data-theme="dark"] .name,
    body[data-theme="dark"] .timeline-time,
    body[data-theme="dark"] .section-title,
    body[data-theme="dark"] h1 {
      color: #ffffff;
    }

    body[data-theme="dark"] .snapshot-note,
    body[data-theme="dark"] .compact-desc,
    body[data-theme="dark"] .meta,
    body[data-theme="dark"] .toolbar-note,
    body[data-theme="dark"] .sub,
    body[data-theme="dark"] .section-note,
    body[data-theme="dark"] .timeline-note,
    body[data-theme="dark"] .timeline-name,
    body[data-theme="dark"] .timeline-status,
    body[data-theme="dark"] .snapshot-label,
    body[data-theme="dark"] .kill-meta {
      color: #cbd5e1;
    }

    body[data-theme="dark"] .health-pill,
    body[data-theme="dark"] .snapshot-tile,
    body[data-theme="dark"] .filter-chip,
    body[data-theme="dark"] .theme-toggle,
    body[data-theme="dark"] .card-toggle,
    body[data-theme="dark"] .section-note,
    body[data-theme="dark"] .search-input {
      background: #1d2632;
      color: #d7e0ec;
    }

    body[data-theme="dark"] .search-input::placeholder {
      color: #94a3b8;
    }

    body[data-theme="dark"] .filter-chip.is-active,
    body[data-theme="dark"] .section-jump.is-active,
    body[data-theme="dark"] .card-toggle.is-active,
    body[data-theme="dark"] .pin-toggle.is-pinned {
      border-color: #60a5fa;
      background: #2563eb;
      color: #ffffff;
    }

    body[data-theme="dark"] .restart-btn,
    body[data-theme="dark"] .section-jump,
    body[data-theme="dark"] .theme-toggle,
    body[data-theme="dark"] .filter-chip,
    body[data-theme="dark"] .card-toggle {
      border-color: #4b5b70;
    }

    body[data-theme="dark"] .mini-badge {
      background: #202b38;
      color: #dbeafe;
      border-color: #3b4a5d;
    }

    body[data-theme="dark"] .mini-badge.ok {
      color: #6ee7b7;
      background: rgba(16, 185, 129, 0.16);
    }

    body[data-theme="dark"] .mini-badge.warn {
      color: #fcd34d;
      background: rgba(245, 158, 11, 0.16);
    }

    body[data-theme="dark"] .mini-badge.bad {
      color: #fca5a5;
      background: rgba(239, 68, 68, 0.16);
    }

    body[data-theme="dark"] .log-table td {
      color: #f8fafc;
      background: #111a2b;
    }

    body[data-theme="dark"] .log-table th {
      color: #ffffff;
    }

    body[data-theme="dark"] .th-sort-btn {
      color: #ffffff !important;
    }

    body[data-theme="dark"] .table-summary {
      color: #ffffff;
    }

    body[data-theme="dark"] .table-meta,
    body[data-theme="dark"] .sort-mark {
      color: #cbd5e1;
    }

    body[data-theme="dark"] pre {
      color: #f8fafc;
    }

    body[data-theme="dark"] pre.log-empty {
      color: #d1d9e6;
    }

    body[data-theme="dark"] .timeline-step {
      background: #171e28;
    }

    body[data-theme="dark"] .timeline-step.is-now {
      background: #243247;
      box-shadow: inset 0 0 0 1px #4b5b70;
    }

    body[data-theme="dark"] .timeline-step.is-now .timeline-time,
    body[data-theme="dark"] .timeline-step.is-now .timeline-name {
      color: #ffffff;
    }

    body[data-theme="dark"] .timeline-step.is-now .timeline-status {
      background: #111827;
      color: #f8fafc;
      border-color: #64748b;
    }

    body[data-theme="dark"] .card-head {
      background: linear-gradient(180deg, #1d2632, #171e28);
    }

    * { box-sizing: border-box; }

    html {
      overflow-x: hidden;
      min-width: 0;
    }

    body {
      margin: 0;
      min-height: 100vh;
      color: var(--text);
      font-family: "Inter", "Segoe UI", Arial, sans-serif;
      font-size: 14px;
      line-height: 1.45;
      overflow-x: hidden;
      -webkit-font-smoothing: antialiased;
      text-rendering: optimizeLegibility;
      background:
        linear-gradient(180deg, rgba(248, 250, 252, 0.98), rgba(238, 242, 247, 0.96)),
        var(--bg);
    }

    body[data-theme="dark"] {
      background: linear-gradient(180deg, #111827, #10141b 42%, #0f141c);
    }

    body::after {
      content: none;
    }

    header {
      position: sticky;
      top: 0;
      z-index: 20;
      padding: 14px 16px 12px;
      border-bottom: 1px solid var(--line);
      background: rgba(248, 250, 252, 0.94);
      backdrop-filter: blur(14px);
      box-shadow: 0 6px 22px rgba(15, 23, 42, 0.07);
    }

    body[data-theme="dark"] header {
      background: rgba(16, 20, 27, 0.94);
    }

    .topbar {
      display: grid;
      grid-template-columns: minmax(220px, 1fr) auto;
      gap: 16px;
      align-items: center;
      min-width: 0;
    }

    .title-block {
      min-width: 0;
    }

    h1 {
      margin: 0;
      font-size: 21px;
      font-weight: 800;
      letter-spacing: 0;
      line-height: 1.2;
    }

    .sub {
      margin-top: 4px;
      font-size: 12px;
      color: var(--muted);
      line-height: 1.35;
    }

    .top-actions {
      display: flex;
      gap: 9px;
      align-items: center;
      justify-content: flex-end;
      flex-wrap: wrap;
      min-width: 0;
    }

    .health-strip {
      display: flex;
      align-items: center;
      gap: 8px;
      margin-top: 10px;
      flex-wrap: wrap;
      max-width: 100%;
      overflow-x: auto;
      scrollbar-width: thin;
    }

    .health-pill {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      border: 1px solid var(--line);
      border-radius: var(--radius);
      padding: 5px 10px;
      background: var(--surface);
      color: var(--muted);
      font-size: 11px;
      font-weight: 700;
      white-space: nowrap;
    }

    .health-pill strong {
      color: var(--text);
      font-size: 12px;
    }

    .health-pill.ok {
      border-color: rgba(5, 150, 105, 0.28);
      background: #ecfdf5;
      color: #047857;
    }

    .health-pill.warn {
      border-color: rgba(217, 119, 6, 0.28);
      background: #fffbeb;
      color: #b45309;
    }

    .health-pill.bad {
      border-color: rgba(220, 38, 38, 0.28);
      background: #fef2f2;
      color: #b91c1c;
    }

    .health-pill.neutral {
      border-color: var(--line);
      background: var(--surface);
    }

    .toolbar {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      margin-top: 11px;
      flex-wrap: wrap;
      min-width: 0;
    }

    .mini-status-bar {
      position: sticky;
      top: 0;
      z-index: 19;
      display: flex;
      align-items: center;
      gap: 8px;
      max-width: 100%;
      overflow-x: auto;
      padding: 7px 16px;
      border-bottom: 1px solid var(--line);
      background: color-mix(in srgb, var(--surface) 94%, transparent);
      backdrop-filter: blur(12px);
      box-shadow: var(--shadow-soft);
      scrollbar-width: thin;
    }

    .mini-status-item {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      white-space: nowrap;
      border: 1px solid var(--line);
      border-radius: var(--radius);
      background: var(--surface);
      color: var(--muted-strong);
      font-size: 11px;
      font-weight: 750;
      padding: 4px 8px;
    }

    .mini-status-item strong {
      color: var(--text);
      font-size: 12px;
    }

    body[data-density="compact"] .snapshot-grid {
      grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
      gap: 7px;
    }

    body[data-density="compact"] .snapshot-tile {
      min-height: 52px;
      padding: 7px 9px;
    }

    body[data-density="compact"] .snapshot-value {
      font-size: 17px;
    }

    body[data-density="compact"] .wrap {
      grid-template-columns: repeat(auto-fit, minmax(245px, 1fr));
      gap: 8px;
      padding: 10px;
    }

    body[data-density="compact"] .card-head {
      padding: 7px 9px;
    }

    body[data-density="compact"] .name {
      font-size: 12px;
    }

    body[data-density="compact"] .mini-badge,
    body[data-density="compact"] .compact-desc,
    body[data-density="compact"] .card-toggle,
    body[data-density="compact"] .restart-btn {
      font-size: 10px;
    }

    body[data-density="compact"] pre,
    body[data-density="compact"] .table-shell {
      max-height: 140px;
      font-size: 10px;
    }

    body[data-density="focus"] .card:not(.is-bad):not(.is-warn) {
      display: none;
    }

    .toolbar-main {
      display: flex;
      align-items: center;
      gap: 8px;
      flex-wrap: wrap;
      flex: 1 1 auto;
      min-width: 0;
    }

    .toolbar-controls {
      display: flex;
      align-items: center;
      gap: 8px;
      flex-wrap: wrap;
    }

    .toolbar-note {
      font-size: 11px;
      color: var(--muted);
      border: 1px solid var(--line);
      border-radius: var(--radius);
      padding: 5px 10px;
      background: var(--surface);
    }

    .theme-toggle {
      min-width: 78px;
      border-color: var(--line);
      background: var(--surface);
      color: var(--muted-strong);
      box-shadow: var(--shadow-soft);
    }

    .theme-toggle:hover {
      border-color: var(--accent);
      background: var(--surface-strong);
    }

    .search-input {
      width: min(320px, 42vw);
      min-width: 180px;
      flex: 1 1 260px;
      max-width: 420px;
      border: 1px solid var(--line);
      border-radius: var(--radius);
      background: var(--surface);
      color: var(--text);
      padding: 8px 10px;
      font-size: 12px;
      outline: none;
      box-shadow: var(--shadow-soft);
    }

    .search-input:focus {
      border-color: rgba(37, 99, 235, 0.55);
      box-shadow: 0 0 0 3px rgba(37, 99, 235, 0.13);
    }

    .filter-bar {
      display: flex;
      align-items: center;
      gap: 7px;
      flex-wrap: wrap;
      min-width: 0;
    }

    .filter-chip {
      border: 1px solid var(--line);
      color: var(--muted-strong);
      background: var(--surface);
      box-shadow: none;
      border-radius: var(--radius);
      padding: 6px 10px;
      font-size: 11px;
      line-height: 1;
    }

    .filter-chip:hover {
      transform: none;
      box-shadow: none;
      border-color: rgba(37, 99, 235, 0.42);
      color: var(--text);
    }

    .filter-chip.is-active {
      border-color: rgba(37, 99, 235, 0.55);
      background: #eff6ff;
      color: #1d4ed8;
    }

    .section-nav {
      display: flex;
      align-items: center;
      gap: 7px;
      max-width: 100%;
      margin-top: 10px;
      overflow-x: auto;
      overscroll-behavior-x: contain;
      scrollbar-width: thin;
    }

    .section-jump {
      flex: 0 0 auto;
      display: inline-flex;
      align-items: center;
      gap: 6px;
      min-height: 30px;
      border: 1px solid var(--line);
      border-radius: var(--radius);
      background: var(--surface);
      color: var(--muted-strong);
      padding: 6px 10px;
      font-size: 11px;
      font-weight: 750;
      box-shadow: none;
      white-space: nowrap;
    }

    .section-jump:hover {
      transform: none;
      border-color: rgba(37, 99, 235, 0.42);
      background: var(--surface-strong);
      box-shadow: none;
    }

    .section-jump.is-active {
      border-color: rgba(37, 99, 235, 0.55);
      background: #eff6ff;
      color: #1d4ed8;
    }

    .section-jump strong {
      font-size: 11px;
      font-variant-numeric: tabular-nums;
    }

    .restart-all-btn {
      display: inline-flex;
      align-items: center;
      gap: 7px;
      border: 1px solid rgba(220, 38, 38, 0.26);
      background: color-mix(in srgb, var(--bad) 10%, var(--surface));
      color: var(--bad);
      font-size: 12px;
      font-weight: 700;
      letter-spacing: 0;
      padding: 8px 12px;
      border-radius: var(--radius);
      cursor: pointer;
      box-shadow: var(--shadow-soft);
      transition: transform 0.14s ease, border-color 0.14s ease, background 0.14s ease, box-shadow 0.14s ease;
      user-select: none;
    }

    .restart-all-btn:hover {
      border-color: rgba(220, 38, 38, 0.48);
      background: color-mix(in srgb, var(--bad) 16%, var(--surface));
      box-shadow: var(--shadow);
      transform: translateY(-1px);
    }

    .restart-all-btn:active {
      transform: translateY(0);
      box-shadow: 0 3px 8px rgba(12, 58, 90, 0.4);
    }

    .restart-all-btn:disabled {
      opacity: 0.72;
      cursor: wait;
      transform: none;
    }

    .restart-all-btn .restart-icon {
      display: inline-block;
      font-size: 14px;
      line-height: 1;
    }

    .restart-all-btn.is-busy .restart-icon {
      animation: restart-spin 0.9s linear infinite;
    }

    .restart-all-btn.is-ok {
      border-color: rgba(5, 150, 105, 0.45);
      background: color-mix(in srgb, var(--ok) 16%, var(--surface));
      color: var(--ok);
    }

    .restart-all-btn.is-err {
      border-color: rgba(220, 38, 38, 0.55);
      background: color-mix(in srgb, var(--bad) 16%, var(--surface));
      color: var(--bad);
    }

    button {
      border: 1px solid rgba(37, 99, 235, 0.32);
      color: #ffffff;
      font-weight: 700;
      background: var(--scheduled);
      padding: 8px 12px;
      border-radius: var(--radius);
      font-size: 13px;
      line-height: 1;
      min-height: 34px;
      cursor: pointer;
      transition: transform 0.14s ease, box-shadow 0.14s ease;
      box-shadow: var(--shadow-soft);
    }

    button:hover {
      transform: translateY(-1px);
      box-shadow: var(--shadow);
    }

    button:focus-visible,
    .search-input:focus-visible,
    select:focus-visible,
    input:focus-visible {
      outline: 0;
      box-shadow: 0 0 0 3px rgba(37, 99, 235, 0.16), var(--shadow-soft);
    }

    button:disabled {
      cursor: not-allowed;
      opacity: 0.62;
      transform: none;
      box-shadow: none;
    }

    .wrap {
      max-width: 1600px;
      width: 100%;
      margin: 0 auto;
      padding: 14px 16px 18px;
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(min(100%, 330px), 1fr));
      gap: 12px;
      min-width: 0;
    }

    .snapshot-grid {
      max-width: 1600px;
      width: 100%;
      margin: 12px auto 0;
      padding: 0 16px;
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 12px;
      min-width: 0;
    }

    .snapshot-tile {
      border: 1px solid var(--line);
      border-radius: var(--radius);
      background: var(--surface);
      padding: 11px 12px;
      box-shadow: var(--shadow-soft);
      min-height: 72px;
    }

    .snapshot-label {
      font-size: 11px;
      font-weight: 800;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      color: var(--muted);
    }

    .snapshot-value {
      margin-top: 4px;
      font-size: 20px;
      font-weight: 850;
      line-height: 1.1;
      color: var(--text);
    }

    .snapshot-note {
      margin-top: 4px;
      color: var(--muted);
      font-size: 11px;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }

    .snapshot-tile.is-ok { border-left: 3px solid color-mix(in srgb, var(--ok) 70%, var(--line)); }
    .snapshot-tile.is-warn { border-left: 3px solid color-mix(in srgb, var(--warn) 70%, var(--line)); }
    .snapshot-tile.is-bad { border-left: 3px solid color-mix(in srgb, var(--bad) 70%, var(--line)); }
    .snapshot-tile.is-info { border-left: 3px solid color-mix(in srgb, var(--scheduled) 55%, var(--line)); }

    .timeline-panel {
      width: 100%;
      max-width: 100%;
      margin-top: 12px;
      border: 1px solid rgba(69, 196, 255, 0.22);
      border-color: var(--line);
      border-radius: var(--radius);
      background: var(--surface);
      overflow: hidden;
      box-shadow: var(--shadow-soft);
    }

    .timeline-panel.is-collapsed .timeline-track {
      display: none;
    }

    .timeline-panel.is-collapsed .timeline-head {
      border-bottom: 0;
    }

    .timeline-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      padding: 9px 12px;
      border-bottom: 1px solid var(--line);
      min-width: 0;
    }

    .timeline-title {
      font-size: 11px;
      font-weight: 800;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      color: var(--text);
    }

    .timeline-note {
      font-size: 11px;
      color: var(--muted);
      white-space: nowrap;
      min-width: 0;
    }

    .timeline-track {
      display: grid;
      grid-auto-flow: column;
      grid-auto-columns: minmax(136px, 1fr);
      gap: 1px;
      overflow-x: auto;
      scrollbar-width: thin;
      background: var(--line);
      overscroll-behavior-x: contain;
    }

    .timeline-step {
      position: relative;
      min-height: 78px;
      padding: 11px 11px 10px 12px;
      background: var(--surface);
      border-top: 2px solid var(--line-strong);
    }

    .timeline-step.ok { border-top-color: var(--ok); }
    .timeline-step.warn { border-top-color: var(--warn); }
    .timeline-step.bad { border-top-color: var(--bad); }
    .timeline-step.disabled { opacity: 0.62; }
    .timeline-step.is-now {
      background: #eff6ff;
    }

    .timeline-time {
      font-size: 11px;
      font-weight: 800;
      color: #1d4ed8;
      white-space: nowrap;
    }

    .timeline-name {
      margin-top: 4px;
      font-size: 11px;
      font-weight: 700;
      color: var(--text);
      line-height: 1.25;
      overflow-wrap: anywhere;
    }

    .timeline-status {
      margin-top: 6px;
      display: inline-flex;
      max-width: 100%;
      border: 1px solid rgba(112, 145, 179, 0.25);
      border-radius: var(--radius);
      padding: 2px 7px;
      color: var(--muted);
      background: var(--surface-strong);
      font-size: 10px;
      font-weight: 800;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }

    .section-banner {
      grid-column: 1 / -1;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      position: relative;
      padding: 10px 12px 10px 14px;
      border: 1px solid transparent;
      border-radius: var(--radius);
      border-bottom-color: var(--line);
      background: transparent;
      box-shadow: none;
      scroll-margin-top: 176px;
    }

    .section-banner::before {
      content: "";
      position: absolute;
      left: 0;
      top: 8px;
      bottom: 8px;
      width: 3px;
      border-radius: 999px;
      background: var(--section-accent, var(--line-strong));
    }

    .section-banner.is-disabled {
      border-color: transparent;
      border-bottom-color: var(--line);
      background: transparent;
      --section-accent: var(--disabled);
    }

    .section-banner.market { --section-accent: #2563eb; }
    .section-banner.v7 { --section-accent: #059669; }
    .section-banner.research { --section-accent: #7c3aed; }
    .section-banner.v16 { --section-accent: #d97706; }
    .section-banner.admin { --section-accent: #64748b; }
    .section-banner.other { --section-accent: #0f766e; }
    .section-left {
      display: flex;
      flex-direction: column;
      gap: 3px;
      min-width: 0;
    }
    .section-action {
      flex: 0 0 auto;
      border-radius: 999px;
      padding: 5px 10px;
      font-size: 11px;
      line-height: 1;
      background: var(--surface);
      border-color: var(--line);
      color: var(--text);
      box-shadow: none;
    }
    .section-action:hover {
      box-shadow: 0 8px 16px rgba(1, 8, 15, 0.22);
    }

    .section-title {
      font-size: 12px;
      font-weight: 800;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      color: var(--text);
    }

    .section-note {
      font-size: 11px;
      color: var(--muted);
      white-space: normal;
      border: 1px solid var(--line);
      border-radius: var(--radius);
      background: var(--surface);
      padding: 2px 7px;
      max-width: 100%;
    }

    .card {
      position: relative;
      border: 1px solid var(--line);
      border-radius: var(--radius);
      background: var(--card);
      overflow: hidden;
      box-shadow: var(--shadow-soft);
      animation: cardIn 0.33s ease both;
      transition: transform 0.16s ease, border-color 0.16s ease, box-shadow 0.16s ease;
      min-height: 128px;
      min-width: 0;
    }

    .card:hover {
      transform: translateY(-2px);
      border-color: var(--line-strong);
      box-shadow: var(--shadow);
    }

    .card::before {
      content: "";
      position: absolute;
      inset: 0 auto 0 0;
      width: 3px;
      background: var(--line-strong);
    }

    .card.is-ok::before { background: var(--ok); }
    .card.is-warn::before { background: var(--warn); }
    .card.is-bad::before { background: var(--bad); }
    .card.is-fullscreen::before { background: var(--accent); }
    .card.is-disabled-compact {
      opacity: 0.72;
      box-shadow: none;
    }
    .card.is-disabled-compact:hover {
      transform: none;
    }
    .card.is-disabled-compact .card-head {
      border-bottom: 0;
    }
    .card.is-disabled-compact pre,
    .card.is-disabled-compact .table-shell,
    .card.is-disabled-compact .kill-shell {
      display: none;
    }
    .card.is-disabled-compact.is-fullscreen {
      opacity: 1;
    }
    .card.is-disabled-compact.is-fullscreen pre,
    .card.is-disabled-compact.is-fullscreen .table-shell,
    .card.is-disabled-compact.is-fullscreen .kill-shell {
      display: block;
    }
    .card.is-disabled-compact.is-fullscreen .card-head {
      border-bottom: 1px solid var(--line);
    }
    .card.is-log-hidden pre,
    .card.is-log-hidden .table-shell {
      display: none;
    }
    .card.is-log-hidden .card-head {
      border-bottom: 0;
    }

    .card.is-log-hidden::after {
      content: "Log collapsed";
      display: block;
      margin: -1px 12px 12px;
      padding: 7px 10px;
      border: 1px dashed var(--line);
      border-radius: var(--radius);
      background: var(--surface-strong);
      color: var(--muted);
      font-size: 11px;
      font-weight: 700;
    }

    .card.is-fullscreen.is-log-hidden pre,
    .card.is-fullscreen.is-log-hidden .table-shell {
      display: block;
    }

    .card.is-fullscreen.is-log-hidden::after {
      display: none;
    }

    .card.is-disabled-compact.is-log-hidden::after {
      display: none;
    }

    .card.is-expanded .card-head {
      border-bottom: 1px solid var(--line);
    }

    .card-head {
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
      gap: 10px;
      padding: 11px 12px;
      border-bottom: 1px solid var(--line);
      background: linear-gradient(180deg, var(--surface), var(--bg-soft));
    }

    .card-head-left {
      display: flex;
      flex-direction: column;
      align-items: flex-start;
      gap: 5px;
      min-width: 0;
      flex: 1 1 auto;
    }

    .name {
      font-size: 13px;
      font-weight: 800;
      letter-spacing: 0;
      max-width: 100%;
      overflow-wrap: anywhere;
      line-height: 1.25;
      display: -webkit-box;
      -webkit-line-clamp: 2;
      -webkit-box-orient: vertical;
      overflow: hidden;
    }

    .meta {
      font-size: 11px;
      color: var(--muted);
      line-height: 1.35;
      max-width: 100%;
      overflow-wrap: anywhere;
    }

    .compact-desc {
      width: 100%;
      color: var(--muted);
      font-size: 11px;
      line-height: 1.25;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }

    .mini-badges {
      display: flex;
      flex-wrap: wrap;
      gap: 5px;
      margin-top: 2px;
    }

    .mini-badge {
      display: inline-flex;
      align-items: center;
      min-height: 20px;
      border: 1px solid rgba(112, 145, 179, 0.28);
      border-radius: var(--radius);
      padding: 2px 7px;
      color: var(--muted);
      background: var(--surface-strong);
      font-size: 10px;
      font-weight: 700;
      white-space: nowrap;
    }

    .mini-badge.ok {
      border-color: rgba(5, 150, 105, 0.25);
      color: var(--ok);
      background: color-mix(in srgb, var(--ok) 11%, var(--surface));
    }

    .mini-badge.warn {
      border-color: rgba(217, 119, 6, 0.25);
      color: var(--warn);
      background: color-mix(in srgb, var(--warn) 12%, var(--surface));
    }

    .mini-badge.bad {
      border-color: rgba(220, 38, 38, 0.25);
      color: var(--bad);
      background: color-mix(in srgb, var(--bad) 11%, var(--surface));
    }

    .pill {
      font-size: 10px;
      font-weight: 700;
      border-radius: var(--radius);
      padding: 3px 8px;
      border: 1px solid var(--line);
      color: var(--muted);
      background: var(--surface-strong);
      white-space: nowrap;
    }

    .pill.ok {
      color: #ffffff;
      border-color: rgba(5, 150, 105, 0.35);
      background: var(--ok);
    }

    .pill.warn {
      color: #ffffff;
      border-color: rgba(217, 119, 6, 0.35);
      background: var(--warn);
    }

    .pill.info {
      color: #ffffff;
      border-color: color-mix(in srgb, var(--scheduled) 60%, var(--line));
      background: var(--scheduled);
    }

    .pill.muted {
      color: #ffffff;
      border-color: color-mix(in srgb, var(--disabled) 60%, var(--line));
      background: var(--disabled);
    }

    .pill.fail {
      color: #ffffff;
      border-color: rgba(220, 38, 38, 0.35);
      background: var(--bad);
    }

    .card-head-right {
      display: flex;
      align-items: flex-start;
      gap: 6px;
      flex-shrink: 0;
      flex-wrap: wrap;
      justify-content: flex-end;
      max-width: 148px;
    }

    .card-toggle {
      border: 1px solid var(--line-strong);
      color: var(--muted-strong);
      font-weight: 700;
      background: var(--surface);
      padding: 0 7px;
      border-radius: var(--radius);
      font-size: 11px;
      min-width: 30px;
      min-height: 28px;
      cursor: pointer;
      box-shadow: none;
      transition: border-color 0.14s ease, background 0.14s ease;
    }

    .card-toggle:hover {
      transform: none;
      box-shadow: none;
      border-color: var(--accent);
      background: color-mix(in srgb, var(--scheduled) 10%, var(--surface));
    }

    .card-toggle.is-active {
      border-color: var(--accent);
      background: color-mix(in srgb, var(--scheduled) 16%, var(--surface));
      color: var(--scheduled);
    }

    .log-toggle.is-hidden {
      border-color: rgba(112, 145, 179, 0.42);
      color: var(--muted);
      background: var(--surface-strong);
    }

    .pin-toggle.is-pinned {
      border-color: rgba(217, 119, 6, 0.35);
      background: color-mix(in srgb, var(--warn) 12%, var(--surface));
      color: var(--warn);
    }

    .restart-btn {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 0;
      border: 1px solid rgba(37, 99, 235, 0.25);
      background: color-mix(in srgb, var(--scheduled) 10%, var(--surface));
      color: var(--scheduled);
      font-size: 11px;
      font-weight: 750;
      letter-spacing: 0;
      min-width: 30px;
      min-height: 28px;
      padding: 0 7px;
      border-radius: var(--radius);
      cursor: pointer;
      box-shadow: none;
      transition: transform 0.14s ease, border-color 0.14s ease, background 0.14s ease, box-shadow 0.14s ease;
      user-select: none;
    }

    .restart-btn:hover {
      border-color: rgba(37, 99, 235, 0.45);
      background: color-mix(in srgb, var(--scheduled) 16%, var(--surface));
      box-shadow: var(--shadow-soft);
      transform: translateY(-1px);
    }

    .restart-btn:active {
      transform: translateY(0);
      box-shadow: 0 1px 2px rgba(0, 0, 0, 0.3);
    }

    .restart-btn:disabled {
      opacity: 0.72;
      cursor: wait;
      transform: none;
    }

    .restart-btn .restart-icon {
      display: inline-block;
      font-size: 13px;
      line-height: 1;
    }

    .restart-btn .restart-label {
      display: none;
    }

    .restart-btn.is-busy,
    .restart-btn.is-ok,
    .restart-btn.is-err {
      min-width: 82px;
      gap: 5px;
      padding: 0 8px;
    }

    .restart-btn.is-busy .restart-label,
    .restart-btn.is-ok .restart-label,
    .restart-btn.is-err .restart-label {
      display: inline;
    }

    .restart-btn.is-busy .restart-icon {
      animation: restart-spin 0.9s linear infinite;
    }

    .restart-btn.is-ok {
      border-color: rgba(5, 150, 105, 0.35);
      background: color-mix(in srgb, var(--ok) 16%, var(--surface));
      color: var(--ok);
    }

    .restart-btn.is-err {
      border-color: rgba(220, 38, 38, 0.35);
      background: color-mix(in srgb, var(--bad) 16%, var(--surface));
      color: var(--bad);
    }

    @keyframes restart-spin {
      from { transform: rotate(0deg); }
      to { transform: rotate(360deg); }
    }

    .kill-shell {
      margin: 8px 10px 4px;
      padding: 8px;
      border: 1px solid rgba(220, 38, 38, 0.25);
      border-radius: var(--radius);
      background: color-mix(in srgb, var(--bad) 8%, var(--surface));
      display: flex;
      flex-direction: column;
      gap: 6px;
    }

    .kill-row {
      display: flex;
      align-items: center;
      gap: 6px;
      flex-wrap: wrap;
    }

    .kill-confirm {
      font-size: 11px;
      color: var(--bad);
      display: inline-flex;
      align-items: center;
      gap: 6px;
      user-select: none;
    }

    .kill-meta {
      font-size: 10px;
      color: var(--muted);
    }

    .kill-ticker {
      min-width: 130px;
      max-width: 220px;
      border: 1px solid var(--line-strong);
      border-radius: var(--radius);
      padding: 4px 6px;
      background: var(--surface);
      color: var(--text);
      font-size: 12px;
    }

    .kill-btn {
      border-radius: var(--radius);
      font-size: 11px;
      padding: 5px 8px;
      box-shadow: none;
      transform: none !important;
    }

    .kill-btn.kill-one {
      border-color: rgba(217, 119, 6, 0.28);
      background: color-mix(in srgb, var(--warn) 12%, var(--surface));
      color: var(--warn);
    }

    .kill-btn.kill-all {
      border-color: rgba(220, 38, 38, 0.32);
      background: color-mix(in srgb, var(--bad) 14%, var(--surface));
      color: var(--bad);
    }

    .kill-btn:disabled,
    .kill-ticker:disabled {
      opacity: 0.55;
      cursor: not-allowed;
    }

    .kill-status {
      font-size: 10px;
      line-height: 1.25;
      min-height: 1.2em;
      color: var(--ok);
      overflow-wrap: anywhere;
    }

    .kill-status.err {
      color: var(--bad);
    }

    pre {
      margin: 0;
      padding: 10px 11px;
      white-space: pre;
      word-break: normal;
      font-size: 11px;
      line-height: 1.4;
      max-height: 210px;
      overflow-x: auto;
      overflow-y: auto;
      font-family: "Consolas", "Lucida Console", monospace;
      background: var(--log-bg);
      color: var(--log-text);
      tab-size: 4;
      scrollbar-gutter: stable both-edges;
      border-top: 1px solid color-mix(in srgb, var(--line) 55%, transparent);
    }

    pre.log-empty {
      color: #aab7c8;
      background:
        repeating-linear-gradient(
          -45deg,
          color-mix(in srgb, var(--log-bg) 94%, #ffffff),
          color-mix(in srgb, var(--log-bg) 94%, #ffffff) 10px,
          var(--log-bg) 10px,
          var(--log-bg) 20px
        );
      font-style: italic;
    }

    .empty-state {
      display: grid;
      place-items: center;
      min-height: 74px;
      color: #aab7c8;
      background:
        repeating-linear-gradient(
          -45deg,
          color-mix(in srgb, var(--log-bg) 94%, #ffffff),
          color-mix(in srgb, var(--log-bg) 94%, #ffffff) 10px,
          var(--log-bg) 10px,
          var(--log-bg) 20px
        );
      font-family: "Consolas", "Lucida Console", monospace;
      font-size: 11px;
      border-top: 1px solid color-mix(in srgb, var(--line) 55%, transparent);
    }

    .empty-state strong {
      color: var(--log-text);
      font-size: 12px;
    }

    .empty-state span {
      display: block;
      margin-top: 3px;
      color: #94a3b8;
      font-size: 10px;
      text-align: center;
    }

    .table-shell {
      margin: 0;
      padding: 8px;
      max-height: 210px;
      overflow-x: auto;
      overflow-y: auto;
      font-family: "Consolas", "Lucida Console", monospace;
      font-size: 10px;
      line-height: 1.25;
      background: var(--log-bg);
      color: var(--log-text);
      tab-size: 4;
      scrollbar-gutter: stable both-edges;
      border-top: 1px solid color-mix(in srgb, var(--line) 55%, transparent);
    }

    .table-summary,
    .table-meta {
      white-space: pre;
      overflow-wrap: anywhere;
    }

    .table-summary {
      display: grid;
      gap: 2px;
      color: #dbeafe;
      margin-bottom: 6px;
      padding: 6px 7px;
      border: 1px solid rgba(148, 163, 184, 0.24);
      border-radius: var(--radius);
      background: rgba(15, 23, 42, 0.72);
    }

    .table-meta {
      color: var(--muted);
      margin-bottom: 6px;
    }

    .log-table {
      border-collapse: collapse;
      width: max-content;
      min-width: max-content;
      table-layout: auto;
    }

    .log-table th,
    .log-table td {
      border: 1px solid var(--line);
      padding: 3px 6px;
      white-space: nowrap;
      text-align: left;
      background: color-mix(in srgb, var(--log-bg) 88%, #ffffff);
    }

    .log-table tbody tr:nth-child(even) td {
      background: color-mix(in srgb, var(--log-bg) 82%, #ffffff);
    }

    .log-table td.num {
      text-align: right;
      font-variant-numeric: tabular-nums;
    }

    .log-table td.pos {
      color: #86efac;
      font-weight: 700;
    }

    .log-table td.neg {
      color: #fca5a5;
      font-weight: 700;
    }

    .log-table thead th {
      position: sticky;
      top: 0;
      z-index: 1;
      background: var(--log-head);
      box-shadow: 0 1px 0 var(--line);
    }

    .th-sort-btn {
      display: inline-flex;
      align-items: center;
      gap: 2px;
      border: 0 !important;
      color: #e5e7eb !important;
      font-weight: 700;
      background: transparent !important;
      padding: 0 !important;
      border-radius: 0 !important;
      font-size: 11px !important;
      cursor: pointer;
      box-shadow: none !important;
    }

    .th-sort-btn:hover {
      transform: none !important;
      box-shadow: none !important;
      color: #93c5fd !important;
      text-decoration: underline;
    }

    .sort-mark {
      color: #94a3b8;
      font-size: 9px;
      min-width: 1.7em;
      text-align: right;
    }

    body.has-fullscreen {
      overflow: hidden;
    }

    body.has-fullscreen header {
      display: none;
    }

    body.has-fullscreen .wrap {
      max-width: none;
      padding: 0;
      display: block;
    }

    body.has-fullscreen .card {
      display: none;
    }

    body.has-fullscreen .card.is-fullscreen {
      display: block;
      position: fixed;
      inset: 8px;
      margin: 0;
      z-index: 999;
      border-radius: 12px;
      border-color: var(--line-strong);
      box-shadow: 0 18px 42px rgba(0, 0, 0, 0.55);
    }

    body.has-fullscreen .card.is-fullscreen .card-head {
      position: sticky;
      top: 0;
      z-index: 2;
      background: var(--surface);
    }

    body.has-fullscreen .card.is-fullscreen pre {
      max-height: calc(100vh - 92px);
      height: calc(100vh - 92px);
      font-size: 12px;
      padding: 12px;
    }

    body.has-fullscreen .card.is-fullscreen .table-shell {
      max-height: calc(100vh - 92px);
      height: calc(100vh - 92px);
      font-size: 12px;
      padding: 12px;
    }

    @keyframes cardIn {
      from { opacity: 0; transform: translateY(6px); }
      to { opacity: 1; transform: translateY(0); }
    }

    @media (max-width: 960px) {
      .topbar {
        grid-template-columns: 1fr;
        align-items: start;
      }

      .top-actions {
        justify-content: flex-start;
        width: 100%;
        max-width: 100%;
      }

      .toolbar {
        align-items: stretch;
      }

      .toolbar-main,
      .toolbar-controls {
        width: 100%;
      }
    }

    @media (max-width: 720px) {
      header {
        padding: 12px 10px 10px;
      }

      h1 {
        font-size: 18px;
      }

      .top-actions > button,
      .top-actions .theme-toggle,
      .top-actions .filter-chip {
        width: 100%;
        min-width: 0;
        justify-content: center;
        text-align: center;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
        min-height: 32px;
        padding: 7px 8px;
        font-size: 12px;
      }

      .top-actions {
        display: grid;
        grid-template-columns: repeat(3, minmax(0, 1fr));
        overflow: visible;
        gap: 6px;
      }

      .restart-all-btn {
        justify-content: center;
      }

      .health-strip,
      .filter-bar,
      .mini-status-bar {
        display: flex;
        flex-wrap: nowrap;
        width: 100%;
        max-width: 100%;
        overflow-x: auto;
        overscroll-behavior-x: contain;
        gap: 6px;
      }

      .mini-status-bar {
        position: static;
        padding: 8px 10px;
      }

      .toolbar-main {
        align-items: stretch;
      }

      .search-input {
        width: 100%;
        min-width: 0;
        max-width: none;
        flex-basis: 100%;
      }

      .filter-chip,
      .health-pill,
      .mini-status-item {
        width: auto;
        flex: 0 0 auto;
        justify-content: center;
        min-height: 28px;
        padding-left: 6px;
        padding-right: 6px;
      }

      .section-nav {
        margin-top: 8px;
      }

      .section-jump {
        min-height: 28px;
        padding: 5px 9px;
      }

      .timeline-head {
        flex-wrap: wrap;
        align-items: flex-start;
      }

      .timeline-note {
        white-space: normal;
      }

      .timeline-track {
        grid-auto-columns: minmax(134px, 64vw);
      }

      .snapshot-grid {
        grid-template-columns: repeat(2, minmax(0, 1fr));
        padding: 0 10px;
        gap: 8px;
      }

      .snapshot-tile {
        min-height: 74px;
        padding: 10px;
      }

      .wrap {
        grid-template-columns: minmax(0, 1fr);
        padding: 10px;
        gap: 10px;
      }

      .section-banner {
        flex-wrap: wrap;
        align-items: flex-start;
        padding: 10px 10px 10px 13px;
      }

      .card-head {
        padding: 10px;
      }

      .card-head-right {
        max-width: 112px;
      }

      .mini-badge {
        white-space: normal;
      }

      pre {
        max-height: 250px;
        font-size: 10.5px;
      }

      .table-shell {
        max-height: 250px;
      }
    }

    @media (max-width: 360px) {
      .snapshot-grid {
        grid-template-columns: 1fr;
      }

      .top-actions > button,
      .top-actions .theme-toggle,
      .top-actions .filter-chip {
        grid-column: 1 / -1;
      }

      .health-strip,
      .filter-bar,
      .mini-status-bar {
        gap: 5px;
      }
    }
  </style>
</head>
<body>
  <header>
    <div class="topbar">
      <div class="title-block">
        <h1>EQIDV2 Live Operations</h1>
        <div class="sub" id="info">loading...</div>
      </div>
      <div class="top-actions">
        <button id="refreshBtn" onclick="loadNow()" title="Refresh dashboard now">Refresh</button>
        <button type="button" class="theme-toggle" id="themeToggle" title="Switch dashboard theme">Theme</button>
        <button type="button" class="theme-toggle" id="timelineToggle" title="Show or hide timeline">Timeline</button>
        <button type="button" class="theme-toggle" id="densityToggle" title="Switch dashboard density">Comfort</button>
        <button type="button" class="filter-chip" id="problemsFirstBtn" title="Keep pinned and problem cards at the top">Problems First</button>
        <button type="button" class="restart-all-btn" id="restartAllBtn"
                title="Restart all managed sessions (sequential 3-step escalation per session)">
          <span class="restart-icon" aria-hidden="true">&#x21BB;</span>
          <span class="restart-all-label">Restart All</span>
        </button>
      </div>
    </div>
    <div class="health-strip" id="healthSummary"></div>
    <div class="toolbar">
      <div class="toolbar-main">
        <input class="search-input" id="cardSearch" type="search" placeholder="Search session, ticker, status, file..." autocomplete="off" />
        <div class="filter-bar" id="statusFilters"></div>
      </div>
      <div class="toolbar-controls">
        <div class="toolbar-note">Auto refresh 15s | 5-min monitor</div>
      </div>
    </div>
    <div class="section-nav" id="sectionNav"></div>
    <div class="timeline-panel" id="todayTimeline"></div>
  </header>
  <div class="mini-status-bar" id="miniStatusBar"></div>
  <div class="snapshot-grid" id="opsSnapshot"></div>
  <div class="wrap" id="cards"></div>

  <script>
    const LOG_ORDER = [
      "nifty_guard_fetch_v16_5min",
      "eod_5min_data",
      "live_combined_csv_id_5min_v7_persistent",
      "signal_discovery_v7_5min_id",
      "candidate_tickers_v7_5min_id",
      "entry_engine_1min_v5_id",
      "v7_live_5min_monitor",
      "v7_research_layer",
      "daily_live_v7_research_session",
      "v7_pre_momentum_filter_analyst",
      "live_signals_csv_id_5min_v7_short",
      "live_signals_csv_id_5min_v7_long",
      "live_papertrade_result_csv_id_5min_v7",
      "paper_trade_id_5min_v7",
      "live_kite_trades_csv_id_5min_v7",
      "kite_trade_id_5min_v7",
      "data_for_backtesting",
      "backtesting_result_v11",
      "signal_early_engine_v16_5min",
      "pending_signals_v16_5min",
      "pending_data_fetcher_v16_5min",
      "detection_engine_v16_5min",
      "detected_signals_v16_5min",
      "live_signals_csv_v16_5min_short",
      "live_signals_csv_v16_5min_long",
      "live_kite_trades_csv_v16_5min",
      "kite_trade_v16_5min",
      "paper_trade_v16_5min",
      "live_papertrade_result_csv_v16_5min",
      "live_combined_csv_v16_5min",
      "eod_15min_data",
      "nifty_guard_fetch_v15",
      "live_combined_csv_v15_new_persistent",
      "live_signals_csv_v15_new_short",
      "live_signals_csv_v15_new_long",
      "live_kite_trades_csv_v15",
      "kite_trade_v15",
      "paper_trade_v15",
      "live_papertrade_result_csv_v15",
      "kite_positions_day_today_csv",
      "kite_holdings_today_csv",
      "preopen_healthcheck",
      "authentication_v2",
      "eod_1540_update"
    ];
    const LOG_TITLES = {
      "signal_early_engine_v16_5min":  "V16 5min Signal Early Engine (SEE)",
      "pending_signals_v16_5min":      "V16 5min Pending Pool CSV",
      "pending_data_fetcher_v16_5min": "V16 5min Pending Data Fetcher",
      "detection_engine_v16_5min":     "V16 5min Detection Engine (Confirmation)",
      "detected_signals_v16_5min":     "V16 5min Detected Signals CSV",
      "eod_5min_data": "Live Data Fetch (5mins)",
      "eod_15min_data": "Live Data Fetch (15mins)",
      "live_combined_csv_v16_5min": "V16 5min Scanner (anti-exhaustion, 5min slots)",
      "live_signals_csv_v16_5min_short": "V16 5min Signals SHORT",
      "live_signals_csv_v16_5min_long": "V16 5min Signals LONG",
      "live_papertrade_result_csv_v16_5min": "V16 5min Papertrade Results",
      "live_kite_trades_csv_v16_5min": "V16 5min Live Kite Trades CSV",
      "paper_trade_v16_5min": "V16 5min Papertrade Runner Log",
      "kite_trade_v16_5min": "V16 5min Live Trade Runner Log",
      "live_combined_csv_v15_new_persistent": "Live combined (short+long) V15 new persistent scanner",
      "live_combined_csv_id_5min_v7_persistent": "Legacy V7 Scanner",
      "signal_discovery_v7_5min_id": "Signal discovery v7 5mins ID",
      "candidate_tickers_v7_5min_id": "Candidate tickers",
      "entry_engine_1min_v5_id": "Entry engine 1min v7 ID",
      "v7_live_5min_monitor": "V7 ID 5min Live Monitor",
      "v7_research_layer": "Suggestions v7 live research",
      "daily_live_v7_research_session": "Daily Live V7 Research",
      "v7_pre_momentum_filter_analyst": "v7 pre momentum filter analyst",
      "live_signals_csv_id_5min_v7_short": "Live Entries CSV ID 5mins v7 Short",
      "live_signals_csv_id_5min_v7_long": "Live Entries CSV ID 5mins v7 Long",
      "live_papertrade_result_csv_id_5min_v7": "V7 ID 5min Papertrade Results",
      "paper_trade_id_5min_v7": "V7 ID 5min Papertrade Runner Log",
      "live_kite_trades_csv_id_5min_v7": "V7 ID 5min Live Kite Trades CSV",
      "kite_trade_id_5min_v7": "V7 ID 5min Live Trade Runner Log",
      "data_for_backtesting": "Data for backtesting",
      "backtesting_result_v11": "Backtesting Result v11",
      "nifty_guard_fetch_v15": "NIFTY Fetch V15",
      "nifty_guard_fetch_v16_5min": "NIFTY Fetch 5min",
      "live_signals_csv_v15_new_short": "Live Entries CSV V15 Short New",
      "live_signals_csv_v15_new_long": "Live Entries CSV V15 Long",
      "live_papertrade_result_csv_v15": "Live Papertrade Result CSV V15",
      "live_kite_trades_csv_v15": "Live Kite Trades CSV V15",
      "kite_holdings_today_csv": "Kite Holdings (Today)",
      "kite_positions_day_today_csv": "Kite Positions (Daily, Today)",
      "authentication_v2": "Auth_V2",
      "paper_trade_v15": "Papertrade Runner View V15",
      "preopen_healthcheck": "Preopen Healthcheck",
      "kite_trade_v15": "Live Kite Trades Log V15",
      "eod_1540_update": "Live EOD Data Fetch"
    };
    const ACTIVE_GROUPS = [
      {
        key: "market",
        nav: "Market",
        title: "Market Data Readiness",
        accent: "market",
        ids: [
          "nifty_guard_fetch_v16_5min",
          "eod_5min_data"
        ]
      },
      {
        key: "v7",
        nav: "V7 Flow",
        title: "Core V7 Live Flow",
        accent: "v7",
        ids: [
          "signal_discovery_v7_5min_id",
          "candidate_tickers_v7_5min_id",
          "entry_engine_1min_v5_id",
          "v7_live_5min_monitor",
          "live_signals_csv_id_5min_v7_short",
          "live_signals_csv_id_5min_v7_long",
          "live_papertrade_result_csv_id_5min_v7",
          "paper_trade_id_5min_v7",
          "live_kite_trades_csv_id_5min_v7",
          "kite_trade_id_5min_v7"
        ]
      },
      {
        key: "backtesting",
        nav: "Backtesting",
        title: "Data & Backtesting",
        accent: "research",
        ids: [
          "data_for_backtesting",
          "backtesting_result_v11"
        ]
      },
      {
        key: "research",
        nav: "Research",
        title: "Research & Suggestions",
        accent: "research",
        ids: [
          "v7_research_layer",
          "daily_live_v7_research_session",
          "v7_pre_momentum_filter_analyst"
        ]
      },
      {
        key: "v16",
        nav: "V16",
        title: "V16 / Parallel Strategy",
        accent: "v16",
        ids: [
          "signal_early_engine_v16_5min",
          "pending_signals_v16_5min",
          "pending_data_fetcher_v16_5min",
          "detection_engine_v16_5min",
          "detected_signals_v16_5min",
          "live_signals_csv_v16_5min_short",
          "live_signals_csv_v16_5min_long",
          "live_kite_trades_csv_v16_5min",
          "kite_trade_v16_5min",
          "paper_trade_v16_5min",
          "live_papertrade_result_csv_v16_5min",
          "live_combined_csv_v16_5min"
        ]
      },
      {
        key: "admin",
        nav: "Admin",
        title: "Admin & Exports",
        accent: "admin",
        ids: [
          "eod_15min_data",
          "kite_positions_day_today_csv",
          "kite_holdings_today_csv",
          "preopen_healthcheck",
          "authentication_v2",
          "eod_1540_update"
        ]
      }
    ];
    const SESSION_TIMELINE = [
      { time: "09:00", id: "authentication_v2", label: "Auth" },
      { time: "09:00", id: "eod_5min_data", label: "Live Data Fetch 5min" },
      { time: "09:15", id: "nifty_guard_fetch_v16_5min", label: "NIFTY Fetch 5min" },
      { time: "09:17", id: "v7_research_layer", label: "V7 Research Layer" },
      { time: "09:17", id: "daily_live_v7_research_session", label: "Daily Live V7 Research" },
      { time: "09:17", id: "v7_pre_momentum_filter_analyst", label: "V7 Pre-Momentum Analyst" },
      { time: "09:20", id: "signal_discovery_v7_5min_id", label: "Signal Discovery" },
      { time: "09:21", id: "entry_engine_1min_v5_id", label: "Entry Engine" },
      { time: "09:22", id: "paper_trade_id_5min_v7", label: "Papertrade TRUE" },
      { time: "09:22", id: "kite_trade_id_5min_v7", label: "Live Trade FALSE" },
      { time: "15:45", id: "data_for_backtesting", label: "Data for Backtesting" },
      { time: "16:00", id: "backtesting_result_v11", label: "Backtesting Result v11" },
      { time: "16:15", id: "v7_research_layer", label: "Suggestions v7 Research" },
      { time: "17:00", id: "", label: "Dashboard Close" }
    ];
    const API_TOKEN = __API_TOKEN_JSON__;
    let FULLSCREEN_ID = "";
    let DISABLED_SECTION_MINIMIZED = localStorage.getItem("eqidv2_disabled_section_minimized") === "1";
    let ACTIVE_FILTER = localStorage.getItem("eqidv2_dashboard_filter") || "all";
    let SEARCH_QUERY = localStorage.getItem("eqidv2_dashboard_search") || "";
    let PROBLEMS_FIRST = localStorage.getItem("eqidv2_problems_first") === "1";
    let DASHBOARD_THEME = localStorage.getItem("eqidv2_dashboard_theme") || "light";
    let TIMELINE_COLLAPSED = localStorage.getItem("eqidv2_timeline_collapsed") === "1";
    let DASHBOARD_DENSITY = localStorage.getItem("eqidv2_dashboard_density") || "comfort";
    function readJsonLocalStorage(key, fallback) {
      try {
        const raw = localStorage.getItem(key);
        if (!raw) return fallback;
        const parsed = JSON.parse(raw);
        return parsed && typeof parsed === "object" ? parsed : fallback;
      } catch (_) {
        return fallback;
      }
    }
    localStorage.removeItem("eqidv2_log_hidden_by_card");
    let LOG_HIDDEN_BY_CARD = {};
    let LOG_EXPANDED_BY_CARD = readJsonLocalStorage("eqidv2_log_expanded_by_card", {});
    let PINNED_CARDS = readJsonLocalStorage("eqidv2_pinned_cards", {});
    const FILTERS = [
      { id: "all", label: "All" },
      { id: "problem", label: "Problem" },
      { id: "watch", label: "Watch" },
      { id: "v7", label: "V7" },
      { id: "v16", label: "V16" },
      { id: "paper", label: "Paper" },
      { id: "live", label: "Live" },
      { id: "research", label: "Research" },
      { id: "disabled", label: "Disabled" }
    ];
    const TABLE_SORT_STATE = {};
    const RESTARTABLE_CARDS = new Set([
      "nifty_guard_fetch_v16_5min",
      "eod_5min_data",
      "signal_early_engine_v16_5min",
      "detection_engine_v16_5min",
      "pending_data_fetcher_v16_5min",
      "kite_positions_day_today_csv",
      "kite_holdings_today_csv",
      "authentication_v2",
      "preopen_healthcheck"
    ]);
    const KILL_CARD_SCOPE = {
      "kite_trade_v16_5min": "false_v16_5min",
      "live_kite_trades_csv_v16_5min": "false_v16_5min",
      "kite_trade_id_5min_v7": "false_id_5min_v7",
      "live_kite_trades_csv_id_5min_v7": "false_id_5min_v7",
      "paper_trade_id_5min_v7": "true_id_5min_v7",
      "live_papertrade_result_csv_id_5min_v7": "true_id_5min_v7",
      "paper_trade_v16_5min": "true_v16_5min",
      "live_papertrade_result_csv_v16_5min": "true_v16_5min",
      "kite_trade_v15": "false_v15",
      "live_kite_trades_csv_v15": "false_v15",
      "paper_trade_v15": "true_v15",
      "live_papertrade_result_csv_v15": "true_v15"
    };

    function applyTheme() {
      const theme = DASHBOARD_THEME === "dark" ? "dark" : "light";
      document.body.setAttribute("data-theme", theme);
      const btn = document.getElementById("themeToggle");
      if (btn) btn.textContent = theme === "dark" ? "Light" : "Dark";
    }

    function applyDensity() {
      const allowed = new Set(["comfort", "compact", "focus"]);
      const density = allowed.has(DASHBOARD_DENSITY) ? DASHBOARD_DENSITY : "comfort";
      document.body.setAttribute("data-density", density);
      const btn = document.getElementById("densityToggle");
      if (btn) btn.textContent = density === "comfort" ? "Comfort" : (density === "compact" ? "Compact" : "Focus");
    }

    function esc(s) {
      return (s === null || s === undefined ? "" : String(s))
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/\"/g, '&quot;')
        .replace(/'/g, '&#39;');
    }

    function apiUrl(path) {
      if (!API_TOKEN) return path;
      const sep = path.includes('?') ? '&' : '?';
      return `${path}${sep}token=${encodeURIComponent(API_TOKEN)}`;
    }

    function displayName(id) {
      return LOG_TITLES[id] || id;
    }

    function normalizeText(s) {
      return String(s || "").toLowerCase();
    }

    function rowsShownFromTail(tail) {
      const match = String(tail || "").match(/rows_shown=(\\d+)/);
      return match ? Number(match[1] || 0) : 0;
    }

    function isPinned(cardId) {
      return !!PINNED_CARDS[cardId];
    }

    function setPinned(cardId, pinned) {
      if (!cardId) return;
      if (pinned) {
        PINNED_CARDS[cardId] = true;
      } else {
        delete PINNED_CARDS[cardId];
      }
      localStorage.setItem("eqidv2_pinned_cards", JSON.stringify(PINNED_CARDS));
    }

    function statusBadge(status) {
      const s = String(status || "").toUpperCase();
      if (!s) return '<span class="pill">UNKNOWN</span>';
      if (s === "SUCCESS" || s === "RUNNING") return `<span class="pill ok">${esc(s)}</span>`;
      if (s === "RESTARTING" || s === "COOLDOWN") return `<span class="pill warn">${esc(s)}</span>`;
      if (s === "WAITING_OUTPUT" || s === "EMPTY_OUTPUT" || s === "STALE_OUTPUT") return `<span class="pill warn">${esc(s)}</span>`;
      if (s === "MISSING_OUTPUT") return `<span class="pill fail">${esc(s)}</span>`;
      if (s === "SCHEDULED" || s === "READY" || s === "ENABLED") return `<span class="pill info">${esc(s)}</span>`;
      if (s === "DISABLED" || s === "SKIPPED_CUTOFF" || s === "STOPPED_AFTER_CUTOFF" || s === "STOPPED") return `<span class="pill muted">${esc(s)}</span>`;
      return `<span class="pill fail">${esc(s)}</span>`;
    }

    function statusBucket(status) {
      const s = String(status || "").toUpperCase();
      if (s === "DISABLED") return "disabled";
      if (s === "SUCCESS" || s === "RUNNING") return "ok";
      if (s === "RESTARTING" || s === "COOLDOWN") return "warn";
      if (s === "WAITING_OUTPUT" || s === "EMPTY_OUTPUT" || s === "STALE_OUTPUT") return "warn";
      if (s === "MISSING_OUTPUT") return "bad";
      if (s === "SCHEDULED" || s === "READY" || s === "ENABLED") return "scheduled";
      if (s === "SKIPPED_CUTOFF" || s === "STOPPED_AFTER_CUTOFF" || s === "STOPPED") return "scheduled";
      if (!s) return "unknown";
      return "bad";
    }

    function renderHealthSummary(items) {
      const counts = { ok: 0, scheduled: 0, warn: 0, bad: 0, disabled: 0, unknown: 0 };
      for (const item of (items || [])) {
        const bucket = statusBucket(item && item.status ? item.status.status : "");
        counts[bucket] = (counts[bucket] || 0) + 1;
      }
      const total = Object.values(counts).reduce((acc, n) => acc + n, 0);
      const pill = (cls, label, value) => (
        `<span class="health-pill ${cls}"><strong>${esc(String(value))}</strong>${esc(label)}</span>`
      );
      const html = [
        pill("neutral", "Total", total),
        pill("ok", "Healthy", counts.ok),
        pill("neutral", "Scheduled", counts.scheduled),
        pill("warn", "Watch", counts.warn),
        pill("bad", "Problem", counts.bad),
        pill("neutral", "Disabled", counts.disabled)
      ].join("");
      const el = document.getElementById("healthSummary");
      if (el) el.innerHTML = html;
    }

    function renderMiniStatus(items, serverTime) {
      const counts = { ok: 0, scheduled: 0, warn: 0, bad: 0, disabled: 0, unknown: 0 };
      let candidates = 0;
      let liveRows = 0;
      let paperRows = 0;
      for (const item of (items || [])) {
        const id = String((item && item.id) || "");
        const bucket = statusBucket(item && item.status ? item.status.status : "");
        counts[bucket] = (counts[bucket] || 0) + 1;
        if (id === "candidate_tickers_v7_5min_id" && item.status) {
          candidates = Number(item.status.total_candidates || 0) || rowsShownFromTail(item.tail);
        }
        if (id.includes("papertrade") || id.includes("paper_trade")) paperRows += rowsShownFromTail(item.tail);
        if (id.includes("kite_trades") || id.includes("live_kite") || id.includes("live_signals")) liveRows += rowsShownFromTail(item.tail);
      }
      const item = (label, value) => `<span class="mini-status-item"><strong>${esc(String(value))}</strong>${esc(label)}</span>`;
      const time = String(serverTime || "").split(" ").slice(-1)[0] || "-";
      const html = [
        item("Problems", counts.bad),
        item("Watch", counts.warn),
        item("Candidates", candidates),
        item("Live Rows", liveRows),
        item("Paper Rows", paperRows),
        item("Refresh", time)
      ].join("");
      const el = document.getElementById("miniStatusBar");
      if (el) el.innerHTML = html;
    }

    function renderOpsSnapshot(items) {
      const counts = { ok: 0, scheduled: 0, warn: 0, bad: 0, disabled: 0, unknown: 0 };
      let fresh = 0;
      let stale = 0;
      let candidates = 0;
      let paperRows = 0;
      let liveRows = 0;
      for (const item of (items || [])) {
        const id = String((item && item.id) || "");
        const bucket = statusBucket(item && item.status ? item.status.status : "");
        counts[bucket] = (counts[bucket] || 0) + 1;
        const age = formatAge(item && item.mtime ? item.mtime : "");
        if (age.cls === "ok") fresh += 1;
        if (age.cls === "bad" || age.cls === "warn") stale += 1;
        if (id === "candidate_tickers_v7_5min_id" && item.status) {
          candidates = Number(item.status.total_candidates || 0) || rowsShownFromTail(item.tail);
        }
        if (id.includes("papertrade") || id.includes("paper_trade")) {
          paperRows += rowsShownFromTail(item.tail);
        }
        if (id.includes("kite_trades") || id.includes("live_kite") || id.includes("live_signals")) {
          liveRows += rowsShownFromTail(item.tail);
        }
      }
      const tile = (cls, label, value, note) => `
        <div class="snapshot-tile ${cls}">
          <div class="snapshot-label">${esc(label)}</div>
          <div class="snapshot-value">${esc(String(value))}</div>
          <div class="snapshot-note">${esc(note || "")}</div>
        </div>
      `;
      const html = [
        tile(counts.bad ? "is-bad" : "is-ok", "Problems", counts.bad, counts.bad ? "Needs action" : "No hard failures"),
        tile(counts.warn ? "is-warn" : "is-ok", "Watch", counts.warn, "Stale, waiting, cooldown"),
        tile("is-info", "Candidates", candidates, "Signal discovery output"),
        tile("is-info", "Live Rows", liveRows, "Signals and Kite CSV rows"),
        tile("is-info", "Paper Rows", paperRows, "Papertrade result rows"),
        tile(stale > fresh ? "is-warn" : "is-ok", "Fresh Outputs", fresh, `${stale} older/missing`)
      ].join("");
      const el = document.getElementById("opsSnapshot");
      if (el) el.innerHTML = html;
    }

    function nowHHMM() {
      const d = new Date();
      const hh = String(d.getHours()).padStart(2, "0");
      const mm = String(d.getMinutes()).padStart(2, "0");
      return `${hh}:${mm}`;
    }

    function statusClassForBucket(bucket) {
      if (bucket === "ok") return "ok";
      if (bucket === "warn") return "warn";
      if (bucket === "bad") return "bad";
      if (bucket === "disabled") return "disabled";
      return "";
    }

    function renderTodayTimeline(itemsById) {
      const now = nowHHMM();
      let activeIdx = -1;
      for (let i = 0; i < SESSION_TIMELINE.length; i += 1) {
        if (SESSION_TIMELINE[i].time <= now) activeIdx = i;
      }
      const steps = SESSION_TIMELINE.map((step, idx) => {
        const item = step.id ? (itemsById[step.id] || { status: {} }) : { status: { status: "SCHEDULED" } };
        const rawStatus = step.id ? String((item.status && item.status.status) || "UNKNOWN").toUpperCase() : "SCHEDULED";
        const bucket = statusBucket(rawStatus);
        const cls = [statusClassForBucket(bucket), idx === activeIdx ? "is-now" : ""].filter(Boolean).join(" ");
        const label = step.label || (step.id ? displayName(step.id) : "Step");
        return `
          <div class="timeline-step ${cls}">
            <div class="timeline-time">${esc(step.time)}</div>
            <div class="timeline-name">${esc(label)}</div>
            <div class="timeline-status">${esc(rawStatus)}</div>
          </div>
        `;
      }).join("");
      const completed = Math.max(0, activeIdx + 1);
      const el = document.getElementById("todayTimeline");
      if (!el) return;
      el.classList.toggle("is-collapsed", TIMELINE_COLLAPSED);
      el.innerHTML = `
        <div class="timeline-head">
          <div class="timeline-title">Today Timeline</div>
          <div class="timeline-note">${esc(completed)} / ${esc(String(SESSION_TIMELINE.length))} steps reached</div>
        </div>
        <div class="timeline-track">${steps}</div>
      `;
    }

    function isLogHidden(cardId, item) {
      if (!cardId) return false;
      if (LOG_HIDDEN_BY_CARD[cardId]) return true;
      if (LOG_EXPANDED_BY_CARD[cardId]) return false;
      return false;
    }

    function setLogHidden(cardId, hidden) {
      if (!cardId) return;
      if (hidden) {
        LOG_HIDDEN_BY_CARD[cardId] = true;
        delete LOG_EXPANDED_BY_CARD[cardId];
      } else {
        delete LOG_HIDDEN_BY_CARD[cardId];
        LOG_EXPANDED_BY_CARD[cardId] = true;
      }
      localStorage.setItem("eqidv2_log_hidden_by_card", JSON.stringify(LOG_HIDDEN_BY_CARD));
      localStorage.setItem("eqidv2_log_expanded_by_card", JSON.stringify(LOG_EXPANDED_BY_CARD));
    }

    function sectionDomId(key) {
      return `section-${String(key || "other").replace(/[^a-z0-9_-]/gi, "-").toLowerCase()}`;
    }

    function renderSectionNav(items) {
      const el = document.getElementById("sectionNav");
      if (!el) return;
      const visible = (items || []).filter((it) => it && it.count > 0);
      if (!visible.length) {
        el.innerHTML = "";
        return;
      }
      el.innerHTML = visible.map((it, idx) => `
        <button type="button" class="section-jump${idx === 0 ? " is-active" : ""}" data-section-target="${esc(sectionDomId(it.key))}">
          ${esc(it.label)} <strong>${esc(String(it.count))}</strong>
        </button>
      `).join("");
    }

    function wireSectionNav() {
      const buttons = document.querySelectorAll("#sectionNav .section-jump");
      buttons.forEach((btn) => {
        btn.addEventListener("click", () => {
          const id = btn.getAttribute("data-section-target") || "";
          const target = id ? document.getElementById(id) : null;
          if (!target) return;
          buttons.forEach((b) => b.classList.remove("is-active"));
          btn.classList.add("is-active");
          target.scrollIntoView({ behavior: "smooth", block: "start" });
        });
      });
    }

    function cardMatchesFilter(id, item, requestedFilter) {
      const rawFilter = requestedFilter || ACTIVE_FILTER;
      const filter = FILTERS.some((f) => f.id === rawFilter) ? rawFilter : "all";
      const status = item && item.status ? item.status.status : "";
      const bucket = statusBucket(status);
      const title = `${id} ${displayName(id)}`.toLowerCase();
      const query = normalizeText(SEARCH_QUERY).trim();
      if (query) {
        const fileName = normalizeText(item && item.file_name ? item.file_name : "");
        const derived = normalizeText(item && item.status && item.status.derived_status ? item.status.derived_status : "");
        const haystack = `${title} ${fileName} ${normalizeText(status)} ${derived}`;
        const terms = query.split(/\\s+/).filter(Boolean);
        if (!terms.every((term) => haystack.includes(term))) return false;
      }
      if (DASHBOARD_DENSITY === "focus" && bucket !== "bad" && bucket !== "warn") return false;
      if (filter === "all") return true;
      if (filter === "problem") return bucket === "bad";
      if (filter === "watch") return bucket === "warn";
      if (filter === "disabled") return bucket === "disabled";
      if (filter === "v7") {
        return title.includes("v7") || title.includes("id_5min") || title.includes("5mins id");
      }
      if (filter === "v16") return title.includes("v16");
      if (filter === "paper") return title.includes("paper");
      if (filter === "live") return title.includes("live") || title.includes("kite");
      if (filter === "research") {
        return id === "v7_research_layer"
          || id === "daily_live_v7_research_session"
          || id === "v7_pre_momentum_filter_analyst"
          || id === "backtesting_result_v11"
          || id === "data_for_backtesting";
      }
      return true;
    }

    function renderStatusFilters(itemsById, orderedIds) {
      const counts = {};
      for (const filter of FILTERS) counts[filter.id] = 0;
      for (const id of orderedIds) {
        const item = itemsById[id] || { status: {} };
        for (const filter of FILTERS) {
          if (cardMatchesFilter(id, item, filter.id)) counts[filter.id] += 1;
        }
      }
      const el = document.getElementById("statusFilters");
      if (!el) return;
      el.innerHTML = FILTERS.map((filter) => {
        const active = filter.id === ACTIVE_FILTER ? " is-active" : "";
        return `<button type="button" class="filter-chip${active}" data-filter-id="${esc(filter.id)}">${esc(filter.label)} ${esc(String(counts[filter.id] || 0))}</button>`;
      }).join("");
    }

    function wireStatusFilters() {
      const buttons = document.querySelectorAll('#statusFilters .filter-chip[data-filter-id]');
      buttons.forEach((btn) => {
        btn.addEventListener('click', (ev) => {
          const filter = ev.currentTarget.getAttribute('data-filter-id') || "all";
          ACTIVE_FILTER = FILTERS.some((f) => f.id === filter) ? filter : "all";
          localStorage.setItem("eqidv2_dashboard_filter", ACTIVE_FILTER);
          loadNow();
        });
      });
    }

    function wireSearchControl() {
      const input = document.getElementById('cardSearch');
      if (!input || input.dataset.wired === '1') return;
      input.dataset.wired = '1';
      input.value = SEARCH_QUERY;
      input.addEventListener('input', () => {
        SEARCH_QUERY = String(input.value || "");
        localStorage.setItem("eqidv2_dashboard_search", SEARCH_QUERY);
        loadNow();
      });
    }

    function wireProblemsFirstControl() {
      const btn = document.getElementById('problemsFirstBtn');
      if (!btn) return;
      btn.classList.toggle('is-active', PROBLEMS_FIRST);
      if (btn.dataset.wired === '1') return;
      btn.dataset.wired = '1';
      btn.addEventListener('click', () => {
        PROBLEMS_FIRST = !PROBLEMS_FIRST;
        localStorage.setItem("eqidv2_problems_first", PROBLEMS_FIRST ? "1" : "0");
        btn.classList.toggle('is-active', PROBLEMS_FIRST);
        loadNow();
      });
    }

    function wireThemeControl() {
      applyTheme();
      const btn = document.getElementById('themeToggle');
      if (!btn || btn.dataset.wired === '1') return;
      btn.dataset.wired = '1';
      btn.addEventListener('click', () => {
        DASHBOARD_THEME = DASHBOARD_THEME === "dark" ? "light" : "dark";
        localStorage.setItem("eqidv2_dashboard_theme", DASHBOARD_THEME);
        applyTheme();
      });
    }

    function wireTimelineControl() {
      const btn = document.getElementById('timelineToggle');
      if (!btn) return;
      btn.textContent = TIMELINE_COLLAPSED ? "Timeline +" : "Timeline -";
      if (btn.dataset.wired === '1') return;
      btn.dataset.wired = '1';
      btn.addEventListener('click', () => {
        TIMELINE_COLLAPSED = !TIMELINE_COLLAPSED;
        localStorage.setItem("eqidv2_timeline_collapsed", TIMELINE_COLLAPSED ? "1" : "0");
        btn.textContent = TIMELINE_COLLAPSED ? "Timeline +" : "Timeline -";
        const panel = document.getElementById("todayTimeline");
        if (panel) panel.classList.toggle("is-collapsed", TIMELINE_COLLAPSED);
      });
    }

    function wireDensityControl() {
      applyDensity();
      const btn = document.getElementById('densityToggle');
      if (!btn || btn.dataset.wired === '1') return;
      btn.dataset.wired = '1';
      btn.addEventListener('click', () => {
        const order = ["comfort", "compact", "focus"];
        const idx = order.indexOf(DASHBOARD_DENSITY);
        DASHBOARD_DENSITY = order[(idx + 1 + order.length) % order.length];
        localStorage.setItem("eqidv2_dashboard_density", DASHBOARD_DENSITY);
        applyDensity();
      });
    }

    function parseLocalDate(raw) {
      const text = String(raw || "").trim();
      if (!text || text === "-") return null;
      const date = new Date(text.replace(" ", "T"));
      return Number.isFinite(date.getTime()) ? date : null;
    }

    function formatAge(rawMtime) {
      const date = parseLocalDate(rawMtime);
      if (!date) return { label: "output: none", cls: "bad" };
      const ageMin = Math.max(0, Math.round((Date.now() - date.getTime()) / 60000));
      if (ageMin < 1) return { label: "output: now", cls: "ok" };
      if (ageMin < 60) return { label: `output: ${ageMin}m`, cls: ageMin <= 20 ? "ok" : "warn" };
      const ageHr = Math.round(ageMin / 60);
      if (ageHr < 24) return { label: `output: ${ageHr}h`, cls: "warn" };
      const ageDay = Math.round(ageHr / 24);
      return { label: `output: ${ageDay}d`, cls: "bad" };
    }

    function compactNextRun(rawNextRun) {
      const text = String(rawNextRun || "").trim();
      if (!text) return "";
      const parts = text.split(/\\s+/);
      if (parts.length >= 2) return `${parts[0]} ${parts[1]}`;
      return text;
    }

    function compactFileName(rawFileName) {
      const text = String(rawFileName || "").trim();
      if (!text) return "-";
      const parts = text.split(/[\\\\/]/).filter(Boolean);
      return parts.length ? parts[parts.length - 1] : text;
    }

    function renderMiniBadges(item, mtime, size) {
      const age = formatAge(mtime);
      const badges = [`<span class="mini-badge ${age.cls}">${esc(age.label)}</span>`];
      badges.push(`<span class="mini-badge">size: ${esc(String(size || 0))}b</span>`);
      return `<div class="mini-badges">${badges.join("")}</div>`;
    }

    function cardStatusClass(status) {
      const s = String(status || "").toUpperCase();
      if (s === "SUCCESS" || s === "RUNNING") return "card is-ok";
      if (s === "RESTARTING" || s === "COOLDOWN") return "card is-warn";
      if (s === "WAITING_OUTPUT" || s === "EMPTY_OUTPUT" || s === "STALE_OUTPUT") return "card is-warn";
      if (s === "SCHEDULED" || s === "READY" || s === "ENABLED" || s === "DISABLED" || s === "STOPPED" || s === "STOPPED_AFTER_CUTOFF" || s === "SKIPPED_CUTOFF") return "card";
      if (s) return "card is-bad";
      return "card";
    }

    function killScopeForCard(cardId) {
      return KILL_CARD_SCOPE[cardId] || "";
    }

    function renderRestartButton(cardId) {
      if (!RESTARTABLE_CARDS.has(cardId)) return "";
      return `
        <button type="button" class="restart-btn" data-restart-id="${esc(cardId)}"
                title="Restart session (1: scheduler restart, 2: graceful stop, 3: force stop)"
                aria-label="Restart session">
          <span class="restart-icon" aria-hidden="true">&#x21BB;</span>
          <span class="restart-label">Restart</span>
        </button>
      `;
    }

    function renderPinButton(cardId) {
      const pinned = isPinned(cardId);
      const label = pinned ? "★" : "☆";
      const cls = pinned ? "card-toggle pin-toggle is-pinned" : "card-toggle pin-toggle";
      return `<button type="button" class="${cls}" data-pin-id="${esc(cardId)}" title="${pinned ? "Unpin" : "Pin"} card">${label}</button>`;
    }

    function uniqueSorted(arr) {
      const seen = new Set();
      const out = [];
      for (const raw of (arr || [])) {
        const v = String(raw || "").trim();
        if (!v || seen.has(v)) continue;
        seen.add(v);
        out.push(v);
      }
      out.sort((a, b) => a.localeCompare(b, undefined, { sensitivity: "base" }));
      return out;
    }

    function renderKillControls(cardId, killSnapshot) {
      const scope = killScopeForCard(cardId);
      if (!scope) return "";
      const scopeSnap = (killSnapshot && killSnapshot[scope]) ? killSnapshot[scope] : {};
      const positions = Array.isArray(scopeSnap.positions) ? scopeSnap.positions : [];
      const tickers = uniqueSorted(positions.map((p) => String((p && p.ticker) || "").toUpperCase()));
      const noActive = positions.length === 0;
      const options = tickers.length
        ? tickers.map((t) => `<option value="${esc(t)}">${esc(t)}</option>`).join("")
        : `<option value="">no active ticker</option>`;
      const lastCmd = (scopeSnap && scopeSnap.last_command) ? scopeSnap.last_command : {};
      const lastCmdId = String((lastCmd && lastCmd.command_id) || "").trim();
      const lastCmdTs = String((lastCmd && lastCmd.requested_at_ist) || "").trim();
      const lastCmdMeta = lastCmdId
        ? `last_cmd=${lastCmdId}${lastCmdTs ? ` @ ${lastCmdTs}` : ""}`
        : "last_cmd=none";
      return `
        <div class="kill-shell" data-kill-scope="${esc(scope)}" data-kill-card="${esc(cardId)}">
          <div class="kill-row">
            <label class="kill-confirm">
              <input type="checkbox" class="kill-confirm-box" />
              Confirm kill action
            </label>
            <span class="kill-meta">active=${positions.length} | ${esc(lastCmdMeta)}</span>
          </div>
          <div class="kill-row">
            <select class="kill-ticker" ${noActive ? "disabled" : ""}>${options}</select>
            <button type="button" class="kill-btn kill-one" ${tickers.length ? "" : "disabled"}>Kill Ticker</button>
            <button type="button" class="kill-btn kill-all" ${noActive ? "disabled" : ""}>Kill All</button>
          </div>
          <div class="kill-status"></div>
        </div>
      `;
    }

    function parseNumberish(value) {
      const s = String(value || "").trim();
      if (!s) return NaN;
      const cleaned = s
        .replace(/,/g, "")
        .replace(/^Rs\\./i, "")
        .replace(/%/g, "")
        .replace(/\\s+/g, "");
      const n = Number.parseFloat(cleaned);
      return Number.isFinite(n) ? n : NaN;
    }

    function isTickerColumn(header) {
      return String(header || "").trim().toLowerCase() === "ticker";
    }

    function tableCellClass(header, value) {
      const h = String(header || "").toLowerCase();
      const text = String(value ?? "").trim();
      const numeric = Number.isFinite(parseNumberish(text)) && !isTickerColumn(header);
      const cls = [];
      if (numeric) cls.push("num");
      if (numeric && (/^[+-]/.test(text) || /(pnl|chg|change|pct|%|return)/i.test(h))) {
        const n = parseNumberish(text);
        if (n > 0) cls.push("pos");
        if (n < 0) cls.push("neg");
      }
      return cls.join(" ");
    }

    function parseTabularTail(tailText) {
      const lines = String(tailText || "").split(/\\r?\\n/);
      const rowsMetaIdx = lines.findIndex((ln) => ln.startsWith("rows_shown="));
      if (rowsMetaIdx < 0 || rowsMetaIdx + 2 >= lines.length) return null;

      const headerLine = lines[rowsMetaIdx + 1] || "";
      const sepLine = lines[rowsMetaIdx + 2] || "";
      if (!headerLine.includes(" | ") || !sepLine.includes("-+-")) return null;

      const headers = headerLine.split(" | ").map((h) => h.trim());
      if (!headers.length) return null;

      const dataLines = lines.slice(rowsMetaIdx + 3).filter((ln) => String(ln || "").trim().length > 0);
      const rows = dataLines.map((ln) => {
        const parts = ln.split(" | ").map((p) => String(p || "").trim());
        while (parts.length < headers.length) parts.push("");
        if (parts.length > headers.length) return parts.slice(0, headers.length);
        return parts;
      });

      return {
        rowsMeta: lines[rowsMetaIdx] || "",
        summaryLines: lines.slice(0, rowsMetaIdx).filter((ln) => String(ln || "").trim().length > 0),
        headers,
        rows
      };
    }

    function sortMark(header, cardId, colIdx) {
      const st = TABLE_SORT_STATE[cardId];
      if (!st || st.colIdx !== colIdx) return "sort";
      if (isTickerColumn(header)) return st.dir === "asc" ? "A-Z" : "Z-A";
      return st.dir === "asc" ? "asc" : "desc";
    }

    function sortedRows(parsed, cardId) {
      const st = TABLE_SORT_STATE[cardId];
      const indexed = parsed.rows.map((cells, idx) => ({ cells, idx }));
      if (!st || st.colIdx < 0) return indexed.map((r) => r.cells);

      const colIdx = st.colIdx;
      const dirMul = st.dir === "asc" ? 1 : -1;
      const header = parsed.headers[colIdx] || "";

      indexed.sort((a, b) => {
        const av = String((a.cells[colIdx] ?? "")).trim();
        const bv = String((b.cells[colIdx] ?? "")).trim();
        const aEmpty = !av;
        const bEmpty = !bv;
        if (aEmpty && bEmpty) return a.idx - b.idx;
        if (aEmpty) return 1;
        if (bEmpty) return -1;

        let cmp = 0;
        if (!isTickerColumn(header)) {
          const an = parseNumberish(av);
          const bn = parseNumberish(bv);
          if (Number.isFinite(an) && Number.isFinite(bn)) {
            cmp = an - bn;
          }
        }
        if (cmp === 0) {
          cmp = av.localeCompare(bv, undefined, { numeric: true, sensitivity: "base" });
        }
        if (cmp === 0) return a.idx - b.idx;
        return cmp * dirMul;
      });

      return indexed.map((r) => r.cells);
    }

    function renderSortableTable(cardId, hostEl, parsed) {
      const rows = sortedRows(parsed, cardId);

      const summaryHtml = parsed.summaryLines.length
        ? `<div class="table-summary">${parsed.summaryLines.map((ln) => esc(ln)).join("<br>")}</div>`
        : "";
      const metaHtml = parsed.rowsMeta ? `<div class="table-meta">${esc(parsed.rowsMeta)}</div>` : "";
      const headHtml = parsed.headers.map((h, i) => `
        <th>
          <button type="button" class="th-sort-btn" data-col="${i}">
            <span>${esc(h)}</span>
            <span class="sort-mark">${esc(sortMark(h, cardId, i))}</span>
          </button>
        </th>
      `).join("");
      const bodyHtml = rows.map((cells) => `
        <tr>${parsed.headers.map((h, i) => `<td class="${esc(tableCellClass(h, cells[i]))}">${esc(cells[i] ?? "")}</td>`).join("")}</tr>
      `).join("");

      hostEl.innerHTML = `
        ${summaryHtml}
        ${metaHtml}
        <table class="log-table">
          <thead><tr>${headHtml}</tr></thead>
          <tbody>${bodyHtml}</tbody>
        </table>
      `;

      hostEl.querySelectorAll(".th-sort-btn").forEach((btn) => {
        btn.addEventListener("click", (ev) => {
          const colIdx = Number((ev.currentTarget && ev.currentTarget.getAttribute("data-col")) || "-1");
          if (!Number.isInteger(colIdx) || colIdx < 0) return;
          const prev = TABLE_SORT_STATE[cardId] || { colIdx: -1, dir: "asc" };
          const nextDir = prev.colIdx === colIdx && prev.dir === "asc" ? "desc" : "asc";
          TABLE_SORT_STATE[cardId] = { colIdx, dir: nextDir };
          renderSortableTable(cardId, hostEl, parsed);
        });
      });
    }

    function enhanceSortableTables() {
      const cards = document.querySelectorAll("#cards .card");
      cards.forEach((card) => {
        const cardId = card.getAttribute("data-id") || "";
        const preEl = card.querySelector("pre");
        if (!preEl) return;
        const parsed = parseTabularTail(preEl.textContent || "");
        if (!parsed) return;
        const host = document.createElement("div");
        host.className = "table-shell";
        preEl.replaceWith(host);
        renderSortableTable(cardId, host, parsed);
      });
    }

    function applyFullscreenState() {
      const cards = document.getElementById('cards');
      const all = cards.querySelectorAll('.card');
      all.forEach((card) => {
        const cardId = card.getAttribute('data-id') || "";
        card.classList.toggle('is-fullscreen', FULLSCREEN_ID && cardId === FULLSCREEN_ID);
      });
      document.body.classList.toggle('has-fullscreen', !!FULLSCREEN_ID);
    }

    function toggleFullscreen(id) {
      if (!id) return;
      FULLSCREEN_ID = (FULLSCREEN_ID === id) ? "" : id;
      applyFullscreenState();
    }

    function wireCardControls() {
      const buttons = document.querySelectorAll('#cards .card-toggle[data-toggle-id]');
      buttons.forEach((btn) => {
        btn.addEventListener('click', (ev) => {
          const id = ev.currentTarget.getAttribute('data-toggle-id') || "";
          toggleFullscreen(id);
        });
      });
    }

    function wirePinControls() {
      const buttons = document.querySelectorAll('#cards .pin-toggle[data-pin-id]');
      buttons.forEach((btn) => {
        btn.addEventListener('click', (ev) => {
          ev.stopPropagation();
          const id = ev.currentTarget.getAttribute('data-pin-id') || "";
          if (!id) return;
          setPinned(id, !isPinned(id));
          loadNow();
        });
      });
    }

    function wireLogToggleControls() {
      const buttons = document.querySelectorAll('#cards .log-toggle[data-log-id]');
      buttons.forEach((btn) => {
        btn.addEventListener('click', (ev) => {
          const id = ev.currentTarget.getAttribute('data-log-id') || "";
          if (!id) return;
          setLogHidden(id, !isLogHidden(id));
          loadNow();
        });
      });
    }

    function wireDisabledSectionControls() {
      const btn = document.getElementById('disabledSectionToggle');
      if (!btn || btn.dataset.wired === '1') return;
      btn.dataset.wired = '1';
      btn.addEventListener('click', () => {
        DISABLED_SECTION_MINIMIZED = !DISABLED_SECTION_MINIMIZED;
        localStorage.setItem("eqidv2_disabled_section_minimized", DISABLED_SECTION_MINIMIZED ? "1" : "0");
        loadNow();
      });
    }

    function wireRestartAllControl() {
      const btn = document.getElementById('restartAllBtn');
      if (!btn || btn.dataset.wired === '1') return;
      btn.dataset.wired = '1';
      const labelEl = btn.querySelector('.restart-all-label');
      const origLabel = labelEl ? labelEl.textContent : "Restart All";
      btn.addEventListener('click', async () => {
        if (btn.disabled) return;
        if (!window.confirm(`Restart all ${RESTARTABLE_CARDS.size} managed sessions?`)) return;
        btn.disabled = true;
        btn.classList.remove('is-ok', 'is-err');
        btn.classList.add('is-busy');
        const ids = Array.from(RESTARTABLE_CARDS);
        const total = ids.length;
        let done = 0;
        let failed = 0;
        const updateLabel = () => {
          if (labelEl) labelEl.textContent = `Restarting ${done}/${total}`;
        };
        updateLabel();
        const results = await Promise.all(ids.map(async (cardId) => {
          try {
            const res = await fetch(apiUrl('/api/restart'), {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              cache: 'no-store',
              body: JSON.stringify({ card_id: cardId })
            });
            const data = await res.json().catch(() => ({}));
            const ok = !!(res.ok && data && data.ok);
            if (!ok) failed += 1;
            return { cardId, ok, step: data && data.step, message: data && data.message };
          } catch (err) {
            failed += 1;
            return { cardId, ok: false, message: String((err && err.message) || err) };
          } finally {
            done += 1;
            updateLabel();
          }
        }));
        btn.classList.remove('is-busy');
        if (failed === 0) {
          btn.classList.add('is-ok');
          if (labelEl) labelEl.textContent = `OK ${total}/${total}`;
          btn.title = results.map(r => `${r.cardId}: step ${r.step}`).join(' | ');
        } else {
          btn.classList.add('is-err');
          if (labelEl) labelEl.textContent = `${total - failed}/${total} OK, ${failed} failed`;
          btn.title = results
            .filter(r => !r.ok)
            .map(r => `${r.cardId}: ${r.message || 'failed'}`)
            .join(' | ');
        }
        setTimeout(() => {
          btn.disabled = false;
          btn.classList.remove('is-ok', 'is-err');
          if (labelEl) labelEl.textContent = origLabel;
          btn.title = "Restart all managed sessions (sequential 3-step escalation per session)";
          loadNow();
        }, 3200);
      });
    }

    function wireRestartControls() {
      const buttons = document.querySelectorAll('#cards .restart-btn');
      buttons.forEach((btn) => {
        btn.addEventListener('click', async (ev) => {
          ev.stopPropagation();
          const target = ev.currentTarget;
          const cardId = target.getAttribute('data-restart-id') || "";
          if (!cardId || target.disabled) return;
          const sessionName = displayName(cardId);
          if (!window.confirm(`Restart session "${sessionName}"?\n\nEscalation: (1) scheduler restart, (2) graceful stop + start, (3) force stop + start.`)) return;
          const labelEl = target.querySelector('.restart-label');
          const origLabel = labelEl ? labelEl.textContent : "Restart";
          target.disabled = true;
          target.classList.remove('is-ok', 'is-err');
          target.classList.add('is-busy');
          if (labelEl) labelEl.textContent = "Restarting...";
          try {
            const res = await fetch(apiUrl('/api/restart'), {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              cache: 'no-store',
              body: JSON.stringify({ card_id: cardId })
            });
            const data = await res.json().catch(() => ({}));
            if (res.ok && data && data.ok) {
              target.classList.add('is-ok');
              const step = data.step || "?";
              if (labelEl) labelEl.textContent = `OK (step ${step})`;
            } else {
              target.classList.add('is-err');
              if (labelEl) labelEl.textContent = "Failed";
              const msg = (data && data.message) ? data.message : `HTTP ${res.status}`;
              target.title = `Restart failed: ${msg}`;
            }
          } catch (err) {
            target.classList.add('is-err');
            if (labelEl) labelEl.textContent = "Error";
            const msg = (err && err.message) ? err.message : String(err);
            target.title = `Restart error: ${msg}`;
          } finally {
            target.classList.remove('is-busy');
            setTimeout(() => {
              target.disabled = false;
              target.classList.remove('is-ok', 'is-err');
              if (labelEl) labelEl.textContent = origLabel;
              target.title = "Restart session (1: scheduler restart, 2: graceful stop, 3: force stop)";
              loadNow();
            }, 2400);
          }
        });
      });
    }

    function wireKillSwitchControls() {
      const roots = document.querySelectorAll('#cards .kill-shell');
      roots.forEach((root) => {
        const scope = root.getAttribute('data-kill-scope') || "";
        const confirmBox = root.querySelector('.kill-confirm-box');
        const tickerSel = root.querySelector('.kill-ticker');
        const btnTicker = root.querySelector('.kill-one');
        const btnAll = root.querySelector('.kill-all');
        const statusEl = root.querySelector('.kill-status');
        if (!scope || !confirmBox || !tickerSel || !btnTicker || !btnAll || !statusEl) return;

        const setStatus = (msg, isErr) => {
          statusEl.textContent = String(msg || "");
          statusEl.classList.toggle('err', !!isErr);
        };

        const setBusy = (busy) => {
          const on = !!busy;
          btnTicker.disabled = on || btnTicker.hasAttribute('data-init-disabled');
          btnAll.disabled = on || btnAll.hasAttribute('data-init-disabled');
        };
        if (btnTicker.disabled) btnTicker.setAttribute('data-init-disabled', '1');
        if (btnAll.disabled) btnAll.setAttribute('data-init-disabled', '1');

        async function fireKill(mode) {
          if (!confirmBox.checked) {
            setStatus("Tick confirmation checkbox first.", true);
            return;
          }
          const ticker = mode === "ticker" ? String(tickerSel.value || "").trim().toUpperCase() : "";
          if (mode === "ticker" && !ticker) {
            setStatus("Select ticker first.", true);
            return;
          }
          setBusy(true);
          setStatus("Submitting kill switch...");
          try {
            const res = await fetch(apiUrl('/api/kill'), {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              cache: 'no-store',
              body: JSON.stringify({
                scope,
                mode,
                ticker,
                confirm: true
              })
            });
            const data = await res.json().catch(() => ({}));
            if (!res.ok || !data || !data.ok) {
              const msg = (data && data.message) ? data.message : `HTTP ${res.status}`;
              setStatus(msg, true);
              return;
            }
            setStatus((data && data.message) ? data.message : "Kill switch queued.", false);
            confirmBox.checked = false;
            setTimeout(() => { loadNow(); }, 450);
          } catch (err) {
            const msg = (err && err.message) ? err.message : String(err);
            setStatus(`Kill request failed: ${msg}`, true);
          } finally {
            setBusy(false);
          }
        }

        btnTicker.addEventListener('click', () => fireKill("ticker"));
        btnAll.addEventListener('click', () => fireKill("all"));
      });
    }

    document.addEventListener('keydown', (ev) => {
      if (ev.key === "Escape" && FULLSCREEN_ID) {
        FULLSCREEN_ID = "";
        applyFullscreenState();
      }
    });

    async function loadNow() {
      try {
        const prevY = window.scrollY;
        const res = await fetch(apiUrl('/api/snapshot?lines=80'), { cache: 'no-store' });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        document.getElementById('info').textContent = `server ${data.server_time} | auto refresh every 15s`;
        renderHealthSummary(data.items || []);
        renderMiniStatus(data.items || [], data.server_time);
        renderOpsSnapshot(data.items || []);

        const byId = {};
        for (const item of data.items) byId[item.id] = item;
        renderTodayTimeline(byId);
        const killSnapshot = data.kill_switch || {};
        const orderedBase = LOG_ORDER.concat(Object.keys(byId).filter((id) => !LOG_ORDER.includes(id)));
        const ordered = orderedBase
          .map((id, idx) => {
            const it = byId[id] || { status: {} };
            const status = String((it.status && it.status.status) || "").toUpperCase();
            const disabled = status === "DISABLED";
            const bucket = statusBucket(status);
            return { id, idx, disabled, bucket, pinned: isPinned(id) };
          })
          .sort((a, b) => {
            if (a.pinned !== b.pinned) return a.pinned ? -1 : 1;
            if (a.disabled !== b.disabled) return a.disabled ? 1 : -1;
            if (PROBLEMS_FIRST) {
              const rank = { bad: 0, warn: 1, unknown: 2, scheduled: 3, ok: 4, disabled: 5 };
              const ar = rank[a.bucket] ?? 9;
              const br = rank[b.bucket] ?? 9;
              if (ar !== br) return ar - br;
            }
            return a.idx - b.idx;
          })
          .map((x) => x.id);

        const activeOrdered = ordered.filter((id) => {
          const it = byId[id] || { status: {} };
          const status = String((it.status && it.status.status) || "").toUpperCase();
          return status !== "DISABLED";
        });
        const disabledOrdered = ordered.filter((id) => !activeOrdered.includes(id));
        renderStatusFilters(byId, ordered);
        const visibleActiveOrdered = activeOrdered.filter((id) => cardMatchesFilter(id, byId[id] || { status: {} }));
        const visibleDisabledOrdered = disabledOrdered.filter((id) => cardMatchesFilter(id, byId[id] || { status: {} }));

        function renderSectionBanner(title, note, disabled, accent, sectionKey) {
          const cls = ["section-banner", disabled ? "is-disabled" : "", accent || ""].filter(Boolean).join(" ");
          const action = disabled
            ? `<button type="button" class="section-action" id="disabledSectionToggle">${DISABLED_SECTION_MINIMIZED ? "Show disabled" : "Hide disabled"}</button>`
            : "";
          return `
            <div class="${cls}" id="${esc(sectionDomId(sectionKey || title))}">
              <div class="section-left">
                <div class="section-title">${esc(title)}</div>
                <div class="section-note">${esc(note)}</div>
              </div>
              ${action}
            </div>
          `;
        }

        function renderCard(id, idx) {
          const it = byId[id] || {id,exists:false,tail:""};
          const status = it.status && it.status.status ? it.status.status : "";
          const statusUpper = String(status || "").toUpperCase();
          const isDisabled = statusUpper === "DISABLED";
          const mtime = it.mtime || "-";
          const size = it.size_bytes || 0;
          const nextRun = it.status && it.status.scheduler_next_run ? compactNextRun(it.status.scheduler_next_run) : "";
          const compactDesc = [
            `file: ${compactFileName(it.file_name || "-")}`,
            nextRun ? `next: ${nextRun}` : "",
            it.status && it.status.derived_status ? String(it.status.derived_status).split(";")[0].trim() : ""
          ].filter(Boolean).join(" | ");
          const miniBadges = renderMiniBadges(it, mtime, size);
          const cardCls = cardStatusClass(status);
          const isFs = FULLSCREEN_ID === id ? " is-fullscreen" : "";
          const disabledCompact = isDisabled && FULLSCREEN_ID !== id ? " is-disabled-compact" : "";
          const logHidden = isLogHidden(id, it);
          const logHiddenClass = logHidden ? " is-log-hidden" : "";
          const expandedClass = logHidden ? "" : " is-expanded";
          const toggleLabel = FULLSCREEN_ID === id ? "▣" : "□";
          const toggleCls = FULLSCREEN_ID === id ? "card-toggle is-active" : "card-toggle";
          const logToggleLabel = logHidden ? "▾" : "▴";
          const logToggleCls = logHidden ? "card-toggle log-toggle is-hidden" : "card-toggle log-toggle";
          const killControls = renderKillControls(it.id, killSnapshot);
          const logText = it.tail || (it.exists ? "(empty)" : "(log file not found yet)");
          const isEmptyLog = /\\((empty|no rows yet|log file not found yet)\\)/i.test(String(logText).trim());
          const emptyLabel = it.exists ? "No rows yet" : "Log file not found";
          const emptyHint = it.exists ? "Waiting for the next write" : "Waiting for this session to create output";
          return `
            <div class="${cardCls}${isFs}${disabledCompact}${logHiddenClass}${expandedClass}" data-id="${esc(id)}" style="animation-delay:${Math.min(idx * 0.05, 0.55)}s">
              <div class="card-head">
                <div class="card-head-left">
                  <div class="name">${esc(displayName(it.id))}</div>
                  ${miniBadges}
                  <div class="compact-desc" title="${esc(compactDesc)}">${esc(compactDesc)}</div>
                </div>
                <div class="card-head-right">
                  ${renderPinButton(it.id)}
                  <button type="button" class="${toggleCls}" data-toggle-id="${esc(id)}" title="${FULLSCREEN_ID === id ? "Exit fullscreen" : "Maximize"}">${toggleLabel}</button>
                  <button type="button" class="${logToggleCls}" data-log-id="${esc(id)}" title="${logHidden ? "Show log" : "Hide log"}">${logToggleLabel}</button>
                  ${renderRestartButton(it.id)}
                  <div>${statusBadge(status)}</div>
                </div>
              </div>
              ${killControls}
              ${isEmptyLog ? `<div class="empty-state"><div><strong>${esc(emptyLabel)}</strong><span>${esc(emptyHint)}</span></div></div>` : `<pre>${esc(logText)}</pre>`}
            </div>
          `;
        }

        const sections = [];
        const navItems = [];
        let renderIdx = 0;
        if (visibleActiveOrdered.length) {
          const used = new Set();
          for (const group of ACTIVE_GROUPS) {
            const groupIds = group.ids.filter((id) => visibleActiveOrdered.includes(id));
            if (!groupIds.length) continue;
            groupIds.forEach((id) => used.add(id));
            navItems.push({ key: group.key, label: group.nav || group.title, count: groupIds.length });
            sections.push(renderSectionBanner(group.title, `${groupIds.length} active/scheduled`, false, group.accent, group.key));
            sections.push(groupIds.map((id) => renderCard(id, renderIdx++)).join(''));
          }
          const otherActive = visibleActiveOrdered.filter((id) => !used.has(id));
          if (otherActive.length) {
            navItems.push({ key: "other", label: "Other", count: otherActive.length });
            sections.push(renderSectionBanner("Other Active / Scheduled", `${otherActive.length} cards`, false, "other", "other"));
            sections.push(otherActive.map((id) => renderCard(id, renderIdx++)).join(''));
          }
        }
        if (visibleDisabledOrdered.length) {
          const forceShowDisabled = ACTIVE_FILTER === "disabled";
          const note = (DISABLED_SECTION_MINIMIZED && !forceShowDisabled)
            ? `${visibleDisabledOrdered.length} card(s) hidden`
            : `${visibleDisabledOrdered.length} card(s)`;
          navItems.push({ key: "disabled", label: "Disabled", count: visibleDisabledOrdered.length });
          sections.push(renderSectionBanner("Disabled", note, true, "admin", "disabled"));
          if (forceShowDisabled || !DISABLED_SECTION_MINIMIZED) {
            sections.push(visibleDisabledOrdered.map((id) => renderCard(id, renderIdx++)).join(''));
          }
        }
        if (!sections.length) {
          sections.push(`
            <div class="section-banner">
              <div class="section-left">
                <div class="section-title">No matching sessions</div>
                <div class="section-note">Change the filter chip above to view more cards.</div>
              </div>
            </div>
          `);
        }

        const html = sections.join('');
        renderSectionNav(navItems);
        wireSectionNav();

        const cards = document.getElementById('cards');
        cards.innerHTML = html;
        wireCardControls();
        wirePinControls();
        wireLogToggleControls();
        wireStatusFilters();
        wireDisabledSectionControls();
        wireRestartControls();
        wireKillSwitchControls();
        enhanceSortableTables();
        applyFullscreenState();
        cards.querySelectorAll('pre').forEach((preEl) => {
          preEl.scrollTop = preEl.scrollHeight;
        });
        if (!FULLSCREEN_ID) window.scrollTo(0, prevY);
      } catch (err) {
        const msg = (err && err.message) ? err.message : String(err);
        document.getElementById('info').textContent = `load failed: ${msg}`;
        document.getElementById('cards').innerHTML = `
          <div class="card">
            <pre>Unable to load logs now. Tap Refresh Now.
If opened inside WhatsApp/Telegram in-app browser, open the same link in Safari/Chrome.</pre>
          </div>
        `;
      }
    }

    applyTheme();
    applyDensity();
    wireThemeControl();
    wireTimelineControl();
    wireDensityControl();
    wireSearchControl();
    wireProblemsFirstControl();
    wireRestartAllControl();
    loadNow();
    setInterval(loadNow, 15000);
  </script>
</body>
</html>"""
        html = html.replace("__API_TOKEN_JSON__", api_token_json)
        body = html.encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _snapshot(self, lines: int) -> Dict[str, object]:
        items = []
        task_snapshot = load_task_scheduler_snapshot()
        today_ist = dt.datetime.now(IST).date().isoformat()
        for key in LOG_IDS:
            path, file_name = resolve_log_target(key)
            status = parse_status_file(_resolve_status_path(STATUS_FILES[key])) if key in STATUS_FILES else {}
            heartbeat = parse_status_file(_resolve_status_path(HEARTBEAT_FILES[key])) if key in HEARTBEAT_FILES else {}
            if heartbeat:
                status = merge_runtime_status(status, heartbeat)
            status = infer_scanner_runtime_status(key, path, status)
            status = apply_scheduler_status(key, status, task_snapshot)
            status = infer_pid_session_provenance(key, status)
            try:
                size = path.stat().st_size if path.exists() else 0
            except OSError:
                size = 0
            if key == "pending_signals_v16_5min":
                tail = _format_csv_projection(
                    path, _PENDING_SIGNALS_V16_COLS,
                    limit_rows=5000,
                    time_only_cols={"added_at", "trigger_iso", "entry_ist", "expires_at"},
                )
            elif key == "detected_signals_v16_5min":
                tail = _format_csv_projection(
                    path, _DETECTED_SIGNALS_V16_COLS,
                    limit_rows=5000,
                    time_only_cols={"detected_at"},
                )
            elif key == "preopen_healthcheck":
                tail = _format_preopen_scheduled_sessions()
            elif key == "entry_engine_1min_v5_id":
                entry_rows = runtime_dir("entry_engine_1min_v5_ID") / "latest" / "latest_entry_engine_rows.csv"
                entry_cols: list[Tuple[str, Sequence[str]]] = [
                    ("entry_time", ("entry_time_ist",)),
                    ("ticker", ("ticker",)),
                    ("side", ("side",)),
                    ("setup", ("setup",)),
                    ("entry_price", ("entry_price",)),
                    ("sl_price", ("sl_price",)),
                    ("target_price", ("target_price",)),
                    ("sl_pct", ("sl_pct",)),
                    ("target_pct", ("target_pct",)),
                    ("exit_rule", ("exit_rule_source",)),
                    ("score", ("score",)),
                ]
                projected = _format_csv_projection(
                    entry_rows,
                    entry_cols,
                    limit_rows=5000,
                    time_only_cols={"entry_time"},
                    sort_numeric_desc_by_keys=("score",),
                )
                tail = projected if projected else tail_text(path, lines=lines)
            elif key == "v7_research_layer":
                report = V7_RESEARCH_LAYER_LATEST_DIR / "latest_multi_window_suggestions.md"
                projected = tail_text(report, lines=lines)
                tail = projected if projected else tail_text(path, lines=lines)
            elif key == "daily_live_v7_research_session":
                report = DAILY_LIVE_V7_RESEARCH_LATEST_DIR / "latest_daily_live_v7_research.md"
                projected = tail_text(report, lines=lines)
                tail = projected if projected else tail_text(path, lines=lines)
            elif key == "v7_pre_momentum_filter_analyst":
                report = V7_PRE_MOMENTUM_FILTER_ANALYST_LATEST_DIR / "latest_v7_pre_momentum_filter_analyst.md"
                projected = tail_text(report, lines=lines)
                tail = projected if projected else tail_text(path, lines=lines)
            elif key == "backtesting_result_v11":
                report = runtime_dir("backtesting_result_v11", "latest", "latest_backtesting_result_v11.md")
                projected = tail_text(report, lines=lines)
                tail = projected if projected else tail_text(path, lines=lines)
            elif key == "paper_trade_id_5min_v7":
                today_ist = dt.datetime.now(IST).date().isoformat()
                projected = _format_v7_id_papertrade_runner_view(path, today_ist)
                tail = projected if projected else tail_text(path, lines=lines)
            elif key in ("signal_early_engine_v16_5min", "pending_data_fetcher_v16_5min", "detection_engine_v16_5min"):
                tail = _shift_bar_slots_in_text(tail_text(path, lines=lines))
            else:
                tail = tail_text(path, lines=lines)
            items.append(
                {
                    "id": key,
                    "file_name": file_name,
                    "exists": path.exists(),
                    "mtime": iso_mtime(path),
                    "size_bytes": size,
                    "status": status,
                    "tail": tail,
                }
            )

        # Output session directly after "Signal discovery v7 5mins ID":
        # signal-candle candidate tickers only; no entry candle/entry price.
        candidate_latest_csv = SIGNAL_DISCOVERY_V7_LATEST_DIR / "latest_candidate_tickers.csv"
        candidate_latest_json = SIGNAL_DISCOVERY_V7_LATEST_DIR / "latest_candidate_tickers.json"
        try:
            candidate_size = candidate_latest_csv.stat().st_size if candidate_latest_csv.exists() else 0
        except OSError:
            candidate_size = 0
        candidate_status: Dict[str, str] = {}
        if candidate_latest_json.exists():
            try:
                payload = json.loads(candidate_latest_json.read_text(encoding="utf-8", errors="replace"))
                total_candidates = int(payload.get("total_candidates", 0) or 0)
                candidate_status = {
                    "status": "READY" if total_candidates > 0 else "EMPTY_OUTPUT",
                    "session": str(payload.get("session", "Signal discovery v7 5mins ID")),
                    "slot": str(payload.get("slot_ist", "")),
                    "total_candidates": str(total_candidates),
                    "long_candidates": str(payload.get("long_candidates", 0)),
                    "short_candidates": str(payload.get("short_candidates", 0)),
                    "json_file": str(Path("signal_discovery_v7_5mins_ID") / "latest" / candidate_latest_json.name),
                }
            except (OSError, json.JSONDecodeError, ValueError):
                candidate_status = {"status": "BAD_JSON"}
        elif candidate_latest_csv.exists():
            candidate_status = {"status": "READY", "derived_status": "latest_csv_exists_json_missing"}
        else:
            candidate_status = {"status": "MISSING_OUTPUT"}

        candidate_cols: list[Tuple[str, Sequence[str]]] = [
            ("signal_time", ("signal_time_ist",)),
            ("ticker", ("ticker",)),
            ("side", ("side",)),
            ("setup", ("setup",)),
            ("signal_close", ("signal_close",)),
            ("quality_score", ("quality_score",)),
            ("rs_pct", ("rs_pct",)),
            ("vol_ratio", ("vol_ratio",)),
            ("reason", ("reason",)),
            ("status", ("status",)),
        ]
        candidate_tail = _format_csv_projection(
            candidate_latest_csv,
            candidate_cols,
            limit_rows=5000,
            time_only_cols={"signal_time"},
            sort_numeric_desc_by_keys=("quality_score",),
        )
        items.append(
            {
                "id": "candidate_tickers_v7_5min_id",
                "file_name": str(Path("signal_discovery_v7_5mins_ID") / "latest" / candidate_latest_csv.name),
                "exists": candidate_latest_csv.exists(),
                "mtime": iso_mtime(candidate_latest_csv),
                "size_bytes": candidate_size,
                "status": candidate_status,
                "tail": candidate_tail,
            }
        )

        monitor_path = runtime_dir("entry_engine_1min_v5_ID") / "latest" / "latest_summary.json"
        monitor_tail, monitor_status = _format_v7_live_5min_monitor(today_ist, task_snapshot)
        try:
            monitor_size = monitor_path.stat().st_size if monitor_path.exists() else 0
        except OSError:
            monitor_size = 0
        items.append(
            {
                "id": "v7_live_5min_monitor",
                "file_name": str(Path("entry_engine_1min_v5_ID") / "latest" / monitor_path.name),
                "exists": monitor_path.exists(),
                "mtime": iso_mtime(monitor_path),
                "size_bytes": monitor_size,
                "status": monitor_status,
                "tail": monitor_tail,
            }
        )

        # Dynamic cards: today's live signal CSV(s) used by trade execution.
        entry_engine_status_for_live_csv = next(
            (
                dict(item.get("status") or {})
                for item in items
                if item.get("id") == "entry_engine_1min_v5_id"
            ),
            {},
        )
        live_entries_cols: list[Tuple[str, Sequence[str]]] = [
            ("signal_datetime", ("signal_datetime", "signal_entry_datetime_ist", "signal_bar_time_ist", "created_ts_ist")),
            ("detected_time_ist", ("detected_time_ist",)),
            ("ticker", ("ticker",)),
            ("side", ("side",)),
            ("entry_price", ("entry_price",)),
            ("target_price", ("target_price",)),
            ("stop_price", ("stop_price", "_stop_price")),
            ("quantity", ("quantity",)),
        ]

        # Dynamic card: today's live signal CSV V5 short.
        live_csv_name_v5_short = f"signals_{today_ist}_v5_short.csv"
        live_csv_path_v5_short = LIVE_SIGNAL_DIR / live_csv_name_v5_short
        try:
            live_size_v5_short = live_csv_path_v5_short.stat().st_size if live_csv_path_v5_short.exists() else 0
        except OSError:
            live_size_v5_short = 0
        live_entries_tail_v5_short = _format_csv_projection(
            live_csv_path_v5_short,
            live_entries_cols,
            # Show full intraday signal sheet instead of a tiny tail window.
            limit_rows=5000,
            time_only_cols={"signal_datetime", "detected_time_ist"},
        )
        items.append(
            {
                "id": "live_signals_csv_v5_short",
                "file_name": str(Path("live_signals") / live_csv_name_v5_short),
                "exists": live_csv_path_v5_short.exists(),
                "mtime": iso_mtime(live_csv_path_v5_short),
                "size_bytes": live_size_v5_short,
                "status": {},
                "tail": live_entries_tail_v5_short,
            }
        )

        # Dynamic card: today's live signal CSV V5 long.
        live_csv_name_v5_long = f"signals_{today_ist}_v5_long.csv"
        live_csv_path_v5_long = LIVE_SIGNAL_DIR / live_csv_name_v5_long
        try:
            live_size_v5_long = live_csv_path_v5_long.stat().st_size if live_csv_path_v5_long.exists() else 0
        except OSError:
            live_size_v5_long = 0
        live_entries_tail_v5_long = _format_csv_projection(
            live_csv_path_v5_long,
            live_entries_cols,
            # Show full intraday signal sheet instead of a tiny tail window.
            limit_rows=5000,
            time_only_cols={"signal_datetime", "detected_time_ist"},
        )
        items.append(
            {
                "id": "live_signals_csv_v5_long",
                "file_name": str(Path("live_signals") / live_csv_name_v5_long),
                "exists": live_csv_path_v5_long.exists(),
                "mtime": iso_mtime(live_csv_path_v5_long),
                "size_bytes": live_size_v5_long,
                "status": {},
                "tail": live_entries_tail_v5_long,
            }
        )

        # Dynamic card: today's live signal CSV V7 sweep short.
        live_csv_name_v7_sweep_short = f"signals_{today_ist}_v7_sweep_short.csv"
        live_csv_path_v7_sweep_short = LIVE_SIGNAL_DIR / live_csv_name_v7_sweep_short
        try:
            live_size_v7_sweep_short = (
                live_csv_path_v7_sweep_short.stat().st_size if live_csv_path_v7_sweep_short.exists() else 0
            )
        except OSError:
            live_size_v7_sweep_short = 0
        live_entries_tail_v7_sweep_short = _format_csv_projection(
            live_csv_path_v7_sweep_short,
            live_entries_cols,
            # Show full intraday signal sheet instead of a tiny tail window.
            limit_rows=5000,
            time_only_cols={"signal_datetime", "detected_time_ist"},
        )
        items.append(
            {
                "id": "live_signals_csv_v7_sweep_short",
                "file_name": str(Path("live_signals") / live_csv_name_v7_sweep_short),
                "exists": live_csv_path_v7_sweep_short.exists(),
                "mtime": iso_mtime(live_csv_path_v7_sweep_short),
                "size_bytes": live_size_v7_sweep_short,
                "status": {},
                "tail": live_entries_tail_v7_sweep_short,
            }
        )

        # Dynamic card: today's live signal CSV V7 sweep long.
        live_csv_name_v7_sweep_long = f"signals_{today_ist}_v7_sweep_long.csv"
        live_csv_path_v7_sweep_long = LIVE_SIGNAL_DIR / live_csv_name_v7_sweep_long
        try:
            live_size_v7_sweep_long = (
                live_csv_path_v7_sweep_long.stat().st_size if live_csv_path_v7_sweep_long.exists() else 0
            )
        except OSError:
            live_size_v7_sweep_long = 0
        live_entries_tail_v7_sweep_long = _format_csv_projection(
            live_csv_path_v7_sweep_long,
            live_entries_cols,
            # Show full intraday signal sheet instead of a tiny tail window.
            limit_rows=5000,
            time_only_cols={"signal_datetime", "detected_time_ist"},
        )
        items.append(
            {
                "id": "live_signals_csv_v7_sweep_long",
                "file_name": str(Path("live_signals") / live_csv_name_v7_sweep_long),
                "exists": live_csv_path_v7_sweep_long.exists(),
                "mtime": iso_mtime(live_csv_path_v7_sweep_long),
                "size_bytes": live_size_v7_sweep_long,
                "status": {},
                "tail": live_entries_tail_v7_sweep_long,
            }
        )

        # Dynamic card: today's live signal CSV V15_NEW short.
        live_csv_name_v15_new_short = f"signals_{today_ist}_v15_new_short.csv"
        live_csv_path_v15_new_short = LIVE_SIGNAL_DIR / live_csv_name_v15_new_short
        try:
            live_size_v15_new_short = (
                live_csv_path_v15_new_short.stat().st_size if live_csv_path_v15_new_short.exists() else 0
            )
        except OSError:
            live_size_v15_new_short = 0
        live_entries_tail_v15_new_short = _format_csv_projection(
            live_csv_path_v15_new_short,
            live_entries_cols,
            limit_rows=5000,
            time_only_cols={"signal_datetime", "detected_time_ist"},
        )
        items.append(
            {
                "id": "live_signals_csv_v15_new_short",
                "file_name": str(Path("live_signals") / live_csv_name_v15_new_short),
                "exists": live_csv_path_v15_new_short.exists(),
                "mtime": iso_mtime(live_csv_path_v15_new_short),
                "size_bytes": live_size_v15_new_short,
                "status": {},
                "tail": live_entries_tail_v15_new_short,
            }
        )

        # Dynamic card: today's live signal CSV V15_NEW long.
        live_csv_name_v15_new_long = f"signals_{today_ist}_v15_new_long.csv"
        live_csv_path_v15_new_long = LIVE_SIGNAL_DIR / live_csv_name_v15_new_long
        try:
            live_size_v15_new_long = (
                live_csv_path_v15_new_long.stat().st_size if live_csv_path_v15_new_long.exists() else 0
            )
        except OSError:
            live_size_v15_new_long = 0
        live_entries_tail_v15_new_long = _format_csv_projection(
            live_csv_path_v15_new_long,
            live_entries_cols,
            limit_rows=5000,
            time_only_cols={"signal_datetime", "detected_time_ist"},
        )
        items.append(
            {
                "id": "live_signals_csv_v15_new_long",
                "file_name": str(Path("live_signals") / live_csv_name_v15_new_long),
                "exists": live_csv_path_v15_new_long.exists(),
                "mtime": iso_mtime(live_csv_path_v15_new_long),
                "size_bytes": live_size_v15_new_long,
                "status": {},
                "tail": live_entries_tail_v15_new_long,
            }
        )

        # Dynamic card: today's live signal CSV ID 5min v7 short.
        live_csv_name_id_5min_v7_short = f"signals_{today_ist}_id_5min_v7_short.csv"
        live_csv_path_id_5min_v7_short = LIVE_SIGNAL_DIR / live_csv_name_id_5min_v7_short
        try:
            live_size_id_5min_v7_short = (
                live_csv_path_id_5min_v7_short.stat().st_size if live_csv_path_id_5min_v7_short.exists() else 0
            )
        except OSError:
            live_size_id_5min_v7_short = 0
        live_entries_tail_id_5min_v7_short = _format_csv_projection(
            live_csv_path_id_5min_v7_short,
            live_entries_cols,
            limit_rows=5000,
            time_only_cols={"signal_datetime", "detected_time_ist"},
        )
        items.append(
            {
                "id": "live_signals_csv_id_5min_v7_short",
                "file_name": str(Path("live_signals") / live_csv_name_id_5min_v7_short),
                "exists": live_csv_path_id_5min_v7_short.exists(),
                "mtime": iso_mtime(live_csv_path_id_5min_v7_short),
                "size_bytes": live_size_id_5min_v7_short,
                "status": dict(entry_engine_status_for_live_csv),
                "tail": live_entries_tail_id_5min_v7_short,
            }
        )

        # Dynamic card: today's live signal CSV ID 5min v7 long.
        live_csv_name_id_5min_v7_long = f"signals_{today_ist}_id_5min_v7_long.csv"
        live_csv_path_id_5min_v7_long = LIVE_SIGNAL_DIR / live_csv_name_id_5min_v7_long
        try:
            live_size_id_5min_v7_long = (
                live_csv_path_id_5min_v7_long.stat().st_size if live_csv_path_id_5min_v7_long.exists() else 0
            )
        except OSError:
            live_size_id_5min_v7_long = 0
        live_entries_tail_id_5min_v7_long = _format_csv_projection(
            live_csv_path_id_5min_v7_long,
            live_entries_cols,
            limit_rows=5000,
            time_only_cols={"signal_datetime", "detected_time_ist"},
        )
        items.append(
            {
                "id": "live_signals_csv_id_5min_v7_long",
                "file_name": str(Path("live_signals") / live_csv_name_id_5min_v7_long),
                "exists": live_csv_path_id_5min_v7_long.exists(),
                "mtime": iso_mtime(live_csv_path_id_5min_v7_long),
                "size_bytes": live_size_id_5min_v7_long,
                "status": dict(entry_engine_status_for_live_csv),
                "tail": live_entries_tail_id_5min_v7_long,
            }
        )

        detected_csv_path_v16_5min, _detected_csv_name_v16_5min = resolve_log_target(
            "detected_signals_v16_5min"
        )

        # Dynamic card: today's live signal CSV V16_5MIN short.
        live_csv_name_v16_5min_short = f"signals_{today_ist}_v16_5min_short.csv"
        live_csv_path_v16_5min_short = LIVE_SIGNAL_DIR / live_csv_name_v16_5min_short
        try:
            live_size_v16_5min_short = (
                live_csv_path_v16_5min_short.stat().st_size if live_csv_path_v16_5min_short.exists() else 0
            )
        except OSError:
            live_size_v16_5min_short = 0
        live_status_v16_5min_short = infer_v16_side_output_status(
            live_csv_path_v16_5min_short,
            "SHORT",
            detected_csv_path_v16_5min,
        )
        live_entries_tail_v16_5min_short = _format_csv_projection(
            live_csv_path_v16_5min_short,
            _LIVE_SIGNALS_V16_COLS,
            limit_rows=5000,
            time_only_cols={"signal_datetime", "detected_time_ist"},
            time_shift_5min_cols={"signal_datetime"},
        )
        items.append(
            {
                "id": "live_signals_csv_v16_5min_short",
                "file_name": str(Path("live_signals") / live_csv_name_v16_5min_short),
                "exists": live_csv_path_v16_5min_short.exists(),
                "mtime": iso_mtime(live_csv_path_v16_5min_short),
                "size_bytes": live_size_v16_5min_short,
                "status": live_status_v16_5min_short,
                "tail": live_entries_tail_v16_5min_short,
            }
        )

        # Dynamic card: today's live signal CSV V16_5MIN long.
        live_csv_name_v16_5min_long = f"signals_{today_ist}_v16_5min_long.csv"
        live_csv_path_v16_5min_long = LIVE_SIGNAL_DIR / live_csv_name_v16_5min_long
        try:
            live_size_v16_5min_long = (
                live_csv_path_v16_5min_long.stat().st_size if live_csv_path_v16_5min_long.exists() else 0
            )
        except OSError:
            live_size_v16_5min_long = 0
        live_status_v16_5min_long = infer_v16_side_output_status(
            live_csv_path_v16_5min_long,
            "LONG",
            detected_csv_path_v16_5min,
        )
        live_entries_tail_v16_5min_long = _format_csv_projection(
            live_csv_path_v16_5min_long,
            _LIVE_SIGNALS_V16_COLS,
            limit_rows=5000,
            time_only_cols={"signal_datetime", "detected_time_ist"},
            time_shift_5min_cols={"signal_datetime"},
        )
        items.append(
            {
                "id": "live_signals_csv_v16_5min_long",
                "file_name": str(Path("live_signals") / live_csv_name_v16_5min_long),
                "exists": live_csv_path_v16_5min_long.exists(),
                "mtime": iso_mtime(live_csv_path_v16_5min_long),
                "size_bytes": live_size_v16_5min_long,
                "status": live_status_v16_5min_long,
                "tail": live_entries_tail_v16_5min_long,
            }
        )

        # Dynamic cards: today's paper trade results CSV(s).
        paper_trade_cols: list[Tuple[str, Sequence[str]]] = [
            ("ticker", ("ticker",)),
            ("exit_time", ("exit_time",)),
            ("side", ("side",)),
            ("outcome", ("outcome",)),
            ("pnl_rs", ("pnl_rs",)),
            ("pnl_pct", ("pnl_pct",)),
        ]

        # Dynamic card: today's paper trade results CSV V5 unified.
        paper_trade_csv_name_v5 = f"paper_trades_{today_ist}_v5.csv"
        paper_trade_csv_path_v5 = LIVE_SIGNAL_DIR / paper_trade_csv_name_v5
        try:
            paper_trade_size_v5 = paper_trade_csv_path_v5.stat().st_size if paper_trade_csv_path_v5.exists() else 0
        except OSError:
            paper_trade_size_v5 = 0
        paper_trade_tail_v5 = _format_csv_projection(
            paper_trade_csv_path_v5,
            paper_trade_cols,
            limit_rows=max(5, min(40, lines // 2)),
            time_only_cols={"exit_time"},
        )
        items.append(
            {
                "id": "live_papertrade_result_csv_v5",
                "file_name": str(Path("live_signals") / paper_trade_csv_name_v5),
                "exists": paper_trade_csv_path_v5.exists(),
                "mtime": iso_mtime(paper_trade_csv_path_v5),
                "size_bytes": paper_trade_size_v5,
                "status": {},
                "tail": paper_trade_tail_v5,
            }
        )

        # Dynamic card: today's paper trade results CSV V7 sweep.
        paper_trade_csv_name_v7_sweep = f"paper_trades_{today_ist}_v7_sweep.csv"
        paper_trade_csv_path_v7_sweep = LIVE_SIGNAL_DIR / paper_trade_csv_name_v7_sweep
        try:
            paper_trade_size_v7_sweep = (
                paper_trade_csv_path_v7_sweep.stat().st_size if paper_trade_csv_path_v7_sweep.exists() else 0
            )
        except OSError:
            paper_trade_size_v7_sweep = 0
        paper_trade_tail_v7_sweep = _format_csv_projection(
            paper_trade_csv_path_v7_sweep,
            paper_trade_cols,
            limit_rows=max(5, min(40, lines // 2)),
            time_only_cols={"exit_time"},
        )
        items.append(
            {
                "id": "live_papertrade_result_csv_v7_sweep",
                "file_name": str(Path("live_signals") / paper_trade_csv_name_v7_sweep),
                "exists": paper_trade_csv_path_v7_sweep.exists(),
                "mtime": iso_mtime(paper_trade_csv_path_v7_sweep),
                "size_bytes": paper_trade_size_v7_sweep,
                "status": {},
                "tail": paper_trade_tail_v7_sweep,
            }
        )

        # Dynamic card: today's paper trade results CSV V15.
        paper_trade_csv_name_v15 = f"paper_trades_{today_ist}_v15_new.csv"
        paper_trade_csv_path_v15 = LIVE_SIGNAL_DIR / paper_trade_csv_name_v15
        try:
            paper_trade_size_v15 = (
                paper_trade_csv_path_v15.stat().st_size if paper_trade_csv_path_v15.exists() else 0
            )
        except OSError:
            paper_trade_size_v15 = 0
        paper_trade_tail_v15 = _format_csv_projection(
            paper_trade_csv_path_v15,
            paper_trade_cols,
            limit_rows=max(5, min(40, lines // 2)),
            time_only_cols={"exit_time"},
        )
        items.append(
            {
                "id": "live_papertrade_result_csv_v15",
                "file_name": str(Path("live_signals") / paper_trade_csv_name_v15),
                "exists": paper_trade_csv_path_v15.exists(),
                "mtime": iso_mtime(paper_trade_csv_path_v15),
                "size_bytes": paper_trade_size_v15,
                "status": {},
                "tail": paper_trade_tail_v15,
            }
        )

        # Dynamic card: today's paper trade results CSV V16_5MIN.
        paper_trade_csv_name_v16_5min = f"paper_trades_{today_ist}_v16_5min.csv"
        paper_trade_csv_path_v16_5min = LIVE_SIGNAL_DIR / paper_trade_csv_name_v16_5min
        try:
            paper_trade_size_v16_5min = (
                paper_trade_csv_path_v16_5min.stat().st_size if paper_trade_csv_path_v16_5min.exists() else 0
            )
        except OSError:
            paper_trade_size_v16_5min = 0
        paper_trade_tail_v16_5min = _format_csv_projection(
            paper_trade_csv_path_v16_5min,
            paper_trade_cols,
            limit_rows=max(5, min(40, lines // 2)),
            time_only_cols={"exit_time"},
        )
        items.append(
            {
                "id": "live_papertrade_result_csv_v16_5min",
                "file_name": str(Path("live_signals") / paper_trade_csv_name_v16_5min),
                "exists": paper_trade_csv_path_v16_5min.exists(),
                "mtime": iso_mtime(paper_trade_csv_path_v16_5min),
                "size_bytes": paper_trade_size_v16_5min,
                "status": {},
                "tail": paper_trade_tail_v16_5min,
            }
        )

        # Dynamic card: today's paper trade results CSV ID 5min v7.
        paper_trade_csv_name_id_5min_v7 = f"paper_trades_{today_ist}_id_5min_v7.csv"
        paper_trade_csv_path_id_5min_v7 = LIVE_SIGNAL_DIR / paper_trade_csv_name_id_5min_v7
        try:
            paper_trade_size_id_5min_v7 = (
                paper_trade_csv_path_id_5min_v7.stat().st_size
                if paper_trade_csv_path_id_5min_v7.exists()
                else 0
            )
        except OSError:
            paper_trade_size_id_5min_v7 = 0
        paper_trade_tail_id_5min_v7 = _format_csv_projection(
            paper_trade_csv_path_id_5min_v7,
            paper_trade_cols,
            limit_rows=max(5, min(40, lines // 2)),
            time_only_cols={"exit_time"},
        )
        items.append(
            {
                "id": "live_papertrade_result_csv_id_5min_v7",
                "file_name": str(Path("live_signals") / paper_trade_csv_name_id_5min_v7),
                "exists": paper_trade_csv_path_id_5min_v7.exists(),
                "mtime": iso_mtime(paper_trade_csv_path_id_5min_v7),
                "size_bytes": paper_trade_size_id_5min_v7,
                "status": {},
                "tail": paper_trade_tail_id_5min_v7,
            }
        )

        # Dynamic card: today's live Kite trades CSV.
        # V5 live executor writes live_trades_YYYY-MM-DD_v5.csv.
        live_kite_trade_csv_name_v5 = f"live_trades_{today_ist}_v5.csv"
        live_kite_trade_csv_name_legacy = f"live_trades_{today_ist}.csv"
        live_kite_trade_csv_path_v5 = LIVE_SIGNAL_DIR / live_kite_trade_csv_name_v5
        live_kite_trade_csv_path_legacy = LIVE_SIGNAL_DIR / live_kite_trade_csv_name_legacy
        if live_kite_trade_csv_path_v5.exists() or (not live_kite_trade_csv_path_legacy.exists()):
            live_kite_trade_csv_name = live_kite_trade_csv_name_v5
            live_kite_trade_csv_path = live_kite_trade_csv_path_v5
        else:
            live_kite_trade_csv_name = live_kite_trade_csv_name_legacy
            live_kite_trade_csv_path = live_kite_trade_csv_path_legacy
        try:
            live_kite_trade_size = (
                live_kite_trade_csv_path.stat().st_size if live_kite_trade_csv_path.exists() else 0
            )
        except OSError:
            live_kite_trade_size = 0
        live_kite_trade_cols: list[Tuple[str, Sequence[str]]] = [
            ("ticker", ("ticker",)),
            ("entry_time", ("entry_time",)),
            ("exit_time", ("exit_time",)),
            ("side", ("side",)),
            ("outcome", ("outcome",)),
            ("entry", ("filled_price", "entry_price")),
            ("exit", ("exit_price",)),
            ("pnl_rs", ("pnl_rs",)),
        ]
        live_kite_trade_tail = _format_csv_projection(
            live_kite_trade_csv_path,
            live_kite_trade_cols,
            # Show full intraday live-trade sheet instead of a small tail window.
            limit_rows=5000,
            time_only_cols={"entry_time", "exit_time"},
        )
        items.append(
            {
                "id": "live_kite_trades_csv",
                "file_name": str(Path("live_signals") / live_kite_trade_csv_name),
                "exists": live_kite_trade_csv_path.exists(),
                "mtime": iso_mtime(live_kite_trade_csv_path),
                "size_bytes": live_kite_trade_size,
                "status": {},
                "tail": live_kite_trade_tail,
            }
        )

        # Dynamic card: today's live Kite trades CSV V7 sweep.
        live_kite_trade_csv_name_v7_sweep = f"live_trades_{today_ist}_v7_sweep.csv"
        live_kite_trade_csv_path_v7_sweep = LIVE_SIGNAL_DIR / live_kite_trade_csv_name_v7_sweep
        try:
            live_kite_trade_size_v7_sweep = (
                live_kite_trade_csv_path_v7_sweep.stat().st_size if live_kite_trade_csv_path_v7_sweep.exists() else 0
            )
        except OSError:
            live_kite_trade_size_v7_sweep = 0
        live_kite_trade_tail_v7_sweep = _format_csv_projection(
            live_kite_trade_csv_path_v7_sweep,
            live_kite_trade_cols,
            # Show full intraday live-trade sheet instead of a small tail window.
            limit_rows=5000,
            time_only_cols={"entry_time", "exit_time"},
        )
        items.append(
            {
                "id": "live_kite_trades_csv_v7_sweep",
                "file_name": str(Path("live_signals") / live_kite_trade_csv_name_v7_sweep),
                "exists": live_kite_trade_csv_path_v7_sweep.exists(),
                "mtime": iso_mtime(live_kite_trade_csv_path_v7_sweep),
                "size_bytes": live_kite_trade_size_v7_sweep,
                "status": {},
                "tail": live_kite_trade_tail_v7_sweep,
            }
        )

        # Dynamic card: today's live Kite trades CSV V15.
        live_kite_trade_csv_name_v15 = f"live_trades_{today_ist}_v15_new.csv"
        live_kite_trade_csv_path_v15 = LIVE_SIGNAL_DIR / live_kite_trade_csv_name_v15
        try:
            live_kite_trade_size_v15 = (
                live_kite_trade_csv_path_v15.stat().st_size if live_kite_trade_csv_path_v15.exists() else 0
            )
        except OSError:
            live_kite_trade_size_v15 = 0
        live_kite_trade_tail_v15 = _format_csv_projection(
            live_kite_trade_csv_path_v15,
            live_kite_trade_cols,
            limit_rows=5000,
            time_only_cols={"entry_time", "exit_time"},
        )
        items.append(
            {
                "id": "live_kite_trades_csv_v15",
                "file_name": str(Path("live_signals") / live_kite_trade_csv_name_v15),
                "exists": live_kite_trade_csv_path_v15.exists(),
                "mtime": iso_mtime(live_kite_trade_csv_path_v15),
                "size_bytes": live_kite_trade_size_v15,
                "status": {},
                "tail": live_kite_trade_tail_v15,
            }
        )

        # Dynamic card: today's live Kite trades CSV V16_5MIN.
        live_kite_trade_csv_name_v16_5min = f"live_trades_{today_ist}_v16_5min.csv"
        live_kite_trade_csv_path_v16_5min = LIVE_SIGNAL_DIR / live_kite_trade_csv_name_v16_5min
        try:
            live_kite_trade_size_v16_5min = (
                live_kite_trade_csv_path_v16_5min.stat().st_size if live_kite_trade_csv_path_v16_5min.exists() else 0
            )
        except OSError:
            live_kite_trade_size_v16_5min = 0
        live_kite_trade_tail_v16_5min = _format_csv_projection(
            live_kite_trade_csv_path_v16_5min,
            live_kite_trade_cols,
            limit_rows=5000,
            time_only_cols={"entry_time", "exit_time"},
        )
        items.append(
            {
                "id": "live_kite_trades_csv_v16_5min",
                "file_name": str(Path("live_signals") / live_kite_trade_csv_name_v16_5min),
                "exists": live_kite_trade_csv_path_v16_5min.exists(),
                "mtime": iso_mtime(live_kite_trade_csv_path_v16_5min),
                "size_bytes": live_kite_trade_size_v16_5min,
                "status": {},
                "tail": live_kite_trade_tail_v16_5min,
            }
        )

        # Dynamic card: today's live Kite trades CSV ID 5min v7.
        live_kite_trade_csv_name_id_5min_v7 = f"live_trades_{today_ist}_id_5min_v7.csv"
        live_kite_trade_csv_path_id_5min_v7 = LIVE_SIGNAL_DIR / live_kite_trade_csv_name_id_5min_v7
        try:
            live_kite_trade_size_id_5min_v7 = (
                live_kite_trade_csv_path_id_5min_v7.stat().st_size
                if live_kite_trade_csv_path_id_5min_v7.exists()
                else 0
            )
        except OSError:
            live_kite_trade_size_id_5min_v7 = 0
        live_kite_trade_tail_id_5min_v7 = _format_csv_projection(
            live_kite_trade_csv_path_id_5min_v7,
            live_kite_trade_cols,
            limit_rows=5000,
            time_only_cols={"entry_time", "exit_time"},
        )
        items.append(
            {
                "id": "live_kite_trades_csv_id_5min_v7",
                "file_name": str(Path("live_signals") / live_kite_trade_csv_name_id_5min_v7),
                "exists": live_kite_trade_csv_path_id_5min_v7.exists(),
                "mtime": iso_mtime(live_kite_trade_csv_path_id_5min_v7),
                "size_bytes": live_kite_trade_size_id_5min_v7,
                "status": {},
                "tail": live_kite_trade_tail_id_5min_v7,
            }
        )

        # Dynamic cards: today's Kite holdings / day positions exported by zerodha_kite_export.py
        today_ymd = dt.datetime.now(IST).strftime("%Y%m%d")
        kite_meta = _read_kite_snapshot_meta(KITE_EXPORT_DIR / "kite_snapshot_meta.json")
        funds_available = _extract_funds_available(kite_meta)
        funds_available_text = _fmt_rs(funds_available) if not math.isnan(funds_available) else "n/a"

        holdings_candidates = [
            KITE_EXPORT_DIR / f"holdings_{today_ymd}.csv",
            KITE_EXPORT_DIR / "kite_holdings_today.csv",
        ]
        holdings_path = next((p for p in holdings_candidates if p.exists()), holdings_candidates[-1])
        try:
            holdings_size = holdings_path.stat().st_size if holdings_path.exists() else 0
        except OSError:
            holdings_size = 0
        holdings_cols: list[Tuple[str, Sequence[str]]] = [
            ("ticker", ("tradingsymbol", "symbol", "ticker")),
            ("exchange", ("exchange",)),
            ("qty", ("quantity", "qty")),
            ("avg_price", ("average_price", "avg_price")),
            ("last_price", ("last_price", "ltp")),
            ("pnl", ("pnl", "unrealised", "unrealized")),
            ("%total_pnl", ()),
            ("day_chg_pct", ("day_change_percentage", "day_change_pct")),
        ]
        holdings_tail = _format_csv_projection(
            holdings_path,
            holdings_cols,
            limit_rows=max(200, lines),
            sort_numeric_desc_by_keys=("%total_pnl", "pnl", "unrealised", "unrealized"),
            indian_numeric_cols={"avg_price", "last_price", "pnl"},
            indian_int_cols={"qty"},
            percent_cols={"%total_pnl", "day_chg_pct"},
            signed_numeric_cols={"pnl", "%total_pnl", "day_chg_pct"},
            computed_cols={"%total_pnl": _compute_holding_total_pnl_pct},
        )
        invested_amt, current_amt, total_pnl, total_pnl_pct, day_pnl, day_pnl_pct = _compute_holdings_summary(
            holdings_path
        )
        total_current_with_funds = (
            current_amt + funds_available
            if (not math.isnan(current_amt) and not math.isnan(funds_available))
            else float("nan")
        )
        total_invested_with_funds = (
            invested_amt + funds_available
            if (not math.isnan(invested_amt) and not math.isnan(funds_available))
            else float("nan")
        )
        holdings_summary_lines = [
            f"invested_amount={_fmt_rs(invested_amt) if not math.isnan(invested_amt) else 'n/a'}",
            f"current_amount={_fmt_rs(current_amt) if not math.isnan(current_amt) else 'n/a'}",
            f"total_pnl={_fmt_rs(total_pnl) if not math.isnan(total_pnl) else 'n/a'}",
            f"total_pnl_pct={_fmt_pct(total_pnl_pct) if not math.isnan(total_pnl_pct) else 'n/a'}",
            f"day_pnl={_fmt_rs(day_pnl) if not math.isnan(day_pnl) else 'n/a'}",
            f"day_pnl_pct={_fmt_pct(day_pnl_pct) if not math.isnan(day_pnl_pct) else 'n/a'}",
            f"funds_available={funds_available_text}",
            f"TOTAL(invested)={_fmt_rs(total_invested_with_funds) if not math.isnan(total_invested_with_funds) else 'n/a'}",
            f"TOTAL(current)={_fmt_rs(total_current_with_funds) if not math.isnan(total_current_with_funds) else 'n/a'}",
        ]
        holdings_tail = "\n".join(holdings_summary_lines + [holdings_tail])
        items.append(
            {
                "id": "kite_holdings_today_csv",
                "file_name": str(Path("kite_exports") / holdings_path.name),
                "exists": holdings_path.exists(),
                "mtime": iso_mtime(holdings_path),
                "size_bytes": holdings_size,
                "status": {},
                "tail": holdings_tail,
            }
        )

        positions_day_candidates = [
            KITE_EXPORT_DIR / f"positions_day_{today_ymd}.csv",
            KITE_EXPORT_DIR / "kite_positions_day_today.csv",
        ]
        positions_day_path = next((p for p in positions_day_candidates if p.exists()), positions_day_candidates[-1])
        try:
            positions_day_size = positions_day_path.stat().st_size if positions_day_path.exists() else 0
        except OSError:
            positions_day_size = 0
        positions_day_cols: list[Tuple[str, Sequence[str]]] = [
            ("ticker", ("tradingsymbol", "symbol", "ticker")),
            ("exchange", ("exchange",)),
            ("product", ("product",)),
            ("qty", ("quantity", "qty")),
            ("buy_qty", ("buy_quantity",)),
            ("sell_qty", ("sell_quantity",)),
            ("avg_price", ("average_price", "avg_price")),
            ("last_price", ("last_price", "ltp")),
            ("pnl", ("pnl", "unrealised", "unrealized")),
        ]
        positions_day_tail = _format_csv_projection(
            positions_day_path,
            positions_day_cols,
            limit_rows=max(200, lines),
            total_numeric_by_keys=("pnl", "unrealised", "unrealized"),
            total_numeric_label="total_pnl_ongoing",
            total_numeric_first=True,
            indian_numeric_cols={"avg_price", "last_price", "pnl"},
            indian_int_cols={"qty", "buy_qty", "sell_qty"},
            signed_numeric_cols={"pnl"},
        )
        positions_day_tail = "\n".join([f"funds_available={funds_available_text}", positions_day_tail])
        items.append(
            {
                "id": "kite_positions_day_today_csv",
                "file_name": str(Path("kite_exports") / positions_day_path.name),
                "exists": positions_day_path.exists(),
                "mtime": iso_mtime(positions_day_path),
                "size_bytes": positions_day_size,
                "status": {},
                "tail": positions_day_tail,
            }
        )

        kill_switch: dict[str, object] = {}
        for scope in ("false_v5", "true_v5", "false_v7_sweep", "true_v7_sweep", "false_v15", "true_v15", "false_v16_5min", "true_v16_5min"):
            state_path, command_path = _kill_switch_scope_paths(scope, today_ist)
            positions = _load_open_positions(state_path, today_ist)
            command_meta: dict[str, object] = {}
            if command_path.exists():
                try:
                    raw_cmd = json.loads(command_path.read_text(encoding="utf-8", errors="replace"))
                    if isinstance(raw_cmd, dict):
                        command_meta = {
                            "command_id": str(raw_cmd.get("command_id", "")).strip(),
                            "mode": str(raw_cmd.get("mode", "")).strip().lower(),
                            "ticker": str(raw_cmd.get("ticker", "")).strip().upper(),
                            "requested_at_ist": str(raw_cmd.get("requested_at_ist", "")).strip(),
                        }
                except (OSError, json.JSONDecodeError):
                    command_meta = {}

            kill_switch[scope] = {
                "state_file": str(Path("live_signals") / state_path.name),
                "state_mtime": iso_mtime(state_path),
                "positions": positions,
                "positions_count": len(positions),
                "command_file": str(Path("live_signals") / command_path.name),
                "command_mtime": iso_mtime(command_path),
                "last_command": command_meta,
            }

        for item in items:
            item["status"] = apply_scheduler_status(str(item.get("id", "")), item.get("status", {}), task_snapshot)

        items = [item for item in items if str(item.get("id", "")) not in HIDDEN_CARD_IDS]

        return {
            "server_time": dt.datetime.now().isoformat(sep=" ", timespec="seconds"),
            "log_dir": str(LOG_DIR),
            "items": items,
            "kill_switch": kill_switch,
        }

    def _send_json(self, payload: Dict[str, object], status: HTTPStatus = HTTPStatus.OK) -> None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _send_text(self, text: str, status: HTTPStatus = HTTPStatus.OK) -> None:
        data = text.encode("utf-8", errors="replace")
        self.send_response(status)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    @staticmethod
    def _int_param(params, name: str, default: int, lo: int, hi: int) -> int:
        try:
            raw = (params.get(name) or [str(default)])[0]
            num = int(raw)
        except (TypeError, ValueError):
            return default
        return max(lo, min(hi, num))


def main() -> int:
    parser = argparse.ArgumentParser(description="EQIDV2 log dashboard server")
    parser.add_argument("--host", default="127.0.0.1", help="Bind host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8787, help="Bind port (default: 8787)")
    parser.add_argument("--username", default=os.environ.get("LOG_DASH_USER", ""), help="Basic auth username")
    parser.add_argument("--password", default=os.environ.get("LOG_DASH_PASS", ""), help="Basic auth password")
    parser.add_argument("--api-token", default=os.environ.get("LOG_DASH_TOKEN", ""), help="Optional API token fallback")
    args = parser.parse_args()

    LOG_DIR.mkdir(parents=True, exist_ok=True)

    httpd = ThreadingHTTPServer((args.host, args.port), LogDashboardHandler)
    httpd.username = _normalize_cli_auth_value(args.username)
    httpd.password = _normalize_cli_auth_value(args.password)
    httpd.api_token = _normalize_cli_auth_value(args.api_token)

    mode = "NO AUTH"
    if args.username and args.password:
        mode = "BASIC AUTH ENABLED"
    if args.api_token:
        mode = mode + " + API TOKEN"
    print(f"[INFO] Serving EQIDV2 dashboard on http://{args.host}:{args.port} ({mode})")
    print(f"[INFO] Reading logs from: {LOG_DIR}")

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
