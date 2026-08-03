# -*- coding: utf-8 -*-
# Backup reference (2026-02-26):
# - c:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\backups_codex\20260226_180142\eqidv2_eod_scheduler_for_15mins_data.py
# - c:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\backups_codex\20260226_180142\run_eqidv2_eod_scheduler_for_15mins_data.bat
"""
avwap_trade_execution_PAPER_TRADE_TRUE.py Ã¢â‚¬â€ Paper Trade Executor (Simulation)
==============================================================================

Watches the daily signal CSV produced by avwap_live_signal_generator.py and
simulates trade execution locally. No real orders are placed.

For each new signal:
  1. Records the simulated entry at the signal's entry_price
  2. Tracks P&L against target and stop-loss using 5-second LTP polling
  3. Forces simulated close at 15:15 IST if neither target nor SL is hit
  4. Appends results to a daily paper trade log CSV

Output:
  - live_signals/paper_trades_YYYY-MM-DD_id_5min_v7.csv  (detailed trade log)
  - live_signals/paper_trade_summary_id_5min_v7.json     (running P&L summary)

Features:
  - Watchdog-based CSV monitoring for instant reaction
  - Concurrent trade simulation threads (one per active trade)
  - Signal deduplication via signal_id tracking
  - Graceful shutdown on Ctrl+C
  - Optional Kite LTP polling for realistic price simulation

Usage:
    python avwap_trade_execution_PAPER_TRADE_TRUE_v15.py
    python avwap_trade_execution_PAPER_TRADE_TRUE_v15.py --no-ltp
    python avwap_trade_execution_PAPER_TRADE_TRUE_v15.py --capital 500000
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
import threading
import time
import traceback
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, time as dt_time
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Set, Tuple

import socket as _socket

# Force IPv4 for all outbound connections (avoids Kite IP-whitelist failures on
# dynamic IPv6 addresses assigned by the ISP).
_orig_getaddrinfo = _socket.getaddrinfo
def _ipv4_only_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):  # noqa: A002
    return _orig_getaddrinfo(host, port, _socket.AF_INET, type, proto, flags)
_socket.getaddrinfo = _ipv4_only_getaddrinfo

import numpy as np
import pandas as pd
import pytz
from eqidv2_runtime_paths import LIVE_SIGNALS_DIR as RUNTIME_LIVE_SIGNALS_DIR, runtime_dir
from eqidv2_runtime_manifest import freeze_runtime_manifest
from nse_intraday_costs import CostConfig, intraday_equity_costs
import eqidv2_risk_brake as rb

try:
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler
except ImportError:
    print("ERROR: watchdog package required. Install with: pip install watchdog")
    sys.exit(1)

# ============================================================================
# CONSTANTS
# ============================================================================
IST = pytz.timezone("Asia/Kolkata")

SIGNAL_DIR = str(RUNTIME_LIVE_SIGNALS_DIR)
CANDIDATE_CSV_DIR = runtime_dir("signal_discovery_v7_5mins_ID", "csv")
SIGNAL_CSV_PATTERNS = ("signals_{}_id_5min_v7_short.csv", "signals_{}_id_5min_v7_long.csv")
PAPER_TRADE_LOG_PATTERN = "paper_trades_{}_id_5min_v7.csv"
PAPER_TRADE_EXEC_LOG_PATTERN = "paper_trade_execution_{}_id_5min_v7.log"
EXECUTED_SIGNALS_FILE = os.path.join(SIGNAL_DIR, "executed_signals_paper_id_5min_v7.json")
SUMMARY_FILE = os.path.join(SIGNAL_DIR, "paper_trade_summary_id_5min_v7.json")
OPEN_TRADES_STATE_PATTERN = "open_trades_state_{}_id_5min_v7.json"
KILL_SWITCH_COMMAND_FILE = os.path.join(SIGNAL_DIR, "kill_switch_true_id_5min_v7.json")

# Trading hours
MARKET_OPEN = dt_time(9, 15)
MARKET_CLOSE = dt_time(15, 30)
FORCED_CLOSE_TIME = dt_time(15, 20)  # aligned closer to backtest EOD; safe before broker auto-square-off


def _parse_hhmm_time(value: str, default: str) -> dt_time:
    raw = str(value or default).strip()
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            return datetime.strptime(raw, fmt).time()
        except ValueError:
            continue
    return datetime.strptime(default, "%H:%M").time()


ENTRY_WINDOW_START_RAW = os.getenv("EQIDV2_PAPER_V7_ENTRY_WINDOW_START", "09:30").strip()
ENTRY_WINDOW_END_RAW = os.getenv("EQIDV2_PAPER_V7_ENTRY_WINDOW_END", "14:30").strip()
ENTRY_WINDOW_START = _parse_hhmm_time(ENTRY_WINDOW_START_RAW, "09:30")
ENTRY_WINDOW_END = _parse_hhmm_time(ENTRY_WINDOW_END_RAW, "14:30")
ENTRY_SIGNAL_TO_ENTRY_LAG_MIN = int(os.getenv("EQIDV2_PAPER_V7_ENTRY_LAG_MIN", "1"))

# Simulation
POLL_INTERVAL_SEC = 5
LIVE_PNL_LOG_INTERVAL_SEC = int(os.getenv("LIVE_PNL_LOG_INTERVAL_SEC", "5"))
# P0-19 (paper mirrors live): when the final_setup_conf qualification is active
# (EQIDV2_USE_FINAL_SETUP_CONF set), paper defaults flip to LIVE risk values so the
# qualification run tests the same machine that will be switched on. With the flag
# off, research paper keeps its broad-coverage defaults. Explicit env vars still win.
_CONF_QUAL_MODE = str(os.getenv("EQIDV2_USE_FINAL_SETUP_CONF", "0")).strip().lower() in {"1", "true", "yes", "on"}
# 0 or negative means unlimited worker threads (no executor-side cap).
# V7 paper/research sessions need broad coverage across simultaneous setups.
MAX_CONCURRENT_TRADES = int(os.getenv(
    "EQIDV2_PAPER_V7_ID_5MIN_MAX_CONCURRENT_TRADES", "20" if _CONF_QUAL_MODE else "100"))

# ---------------------------------------------------------------------------
# v8 research parity
#
# The 1-minute entry engine writes stop/target from v6.SETUP_EXIT_RULES, which
# is what v8 research resolves with. Keep this empty so the executor honors
# those upstream prices without applying older CAND-E4 overrides.
# ---------------------------------------------------------------------------
CANDIDATE_E4_SL_TGT = {}


def _candE4_per_setup_sl_tgt(side, setup, entry_price, csv_stop, csv_target):
    """Return (stop_price, target_price, applied) overriding from CANDIDATE_E4_SL_TGT
    when (side, setup) is in the dict; else fall back to csv values."""
    try:
        ep = float(entry_price)
    except (TypeError, ValueError):
        return csv_stop, csv_target, False
    if ep <= 0:
        return csv_stop, csv_target, False
    s = str(side).strip().upper()
    su = str(setup).strip().upper()
    sl_tgt = CANDIDATE_E4_SL_TGT.get((s, su))
    if sl_tgt is None:
        return csv_stop, csv_target, False
    sl_frac = float(sl_tgt[0]) / 100.0
    tg_frac = float(sl_tgt[1]) / 100.0
    if s == "LONG":
        new_stop = ep * (1.0 - sl_frac)
        new_target = ep * (1.0 + tg_frac)
    else:
        new_stop = ep * (1.0 + sl_frac)
        new_target = ep * (1.0 - tg_frac)
    return new_stop, new_target, True


# ---------------------------------------------------------------------------
# CAND-E4 G1 (time-window) + G2 (daily side cap) governors
# (mirror of the FALSE executor; same defaults, separate counter file)
# ---------------------------------------------------------------------------
CANDE4_LONG_TIME_WINDOW_LO  = float(os.getenv("EQIDV2_CANDE4_LONG_WINDOW_LO",  "9.15"))
CANDE4_LONG_TIME_WINDOW_HI  = float(os.getenv("EQIDV2_CANDE4_LONG_WINDOW_HI",  "15.50"))
CANDE4_SHORT_TIME_WINDOW_LO = float(os.getenv("EQIDV2_CANDE4_SHORT_WINDOW_LO", "9.15"))
CANDE4_SHORT_TIME_WINDOW_HI = float(os.getenv("EQIDV2_CANDE4_SHORT_WINDOW_HI", "15.50"))
CANDE4_MAX_LONG_PER_DAY     = int(os.getenv("EQIDV2_CANDE4_MAX_LONG_PER_DAY",  "999999"))
CANDE4_MAX_SHORT_PER_DAY    = int(os.getenv("EQIDV2_CANDE4_MAX_SHORT_PER_DAY", "999999"))
CANDE4_G2_COUNTERS_FILE     = os.path.join(SIGNAL_DIR, "candE4_g2_counters_id_5min_v7_paper.json")

_candE4_g2_lock     = threading.Lock()
_candE4_g2_counters = {"date": "", "long": 0, "short": 0}


def _candE4_g1_time_window_check(side):
    s = str(side).strip().upper()
    if s == "LONG":
        lo, hi = CANDE4_LONG_TIME_WINDOW_LO, CANDE4_LONG_TIME_WINDOW_HI
    else:
        lo, hi = CANDE4_SHORT_TIME_WINDOW_LO, CANDE4_SHORT_TIME_WINDOW_HI
    now_ist = datetime.now(IST)
    h = now_ist.hour + now_ist.minute / 60.0 + now_ist.second / 3600.0
    if not (lo <= h <= hi):
        return (
            False,
            f"candE4_G1_time_window {s} {now_ist.strftime('%H:%M')} "
            f"outside [{lo:.2f}, {hi:.2f}]",
        )
    return True, "ok"


def _candE4_g2_load_from_disk(today_str):
    try:
        if os.path.exists(CANDE4_G2_COUNTERS_FILE):
            with open(CANDE4_G2_COUNTERS_FILE, "r", encoding="utf-8") as f:
                payload = json.load(f)
            if isinstance(payload, dict) and str(payload.get("date", "")) == today_str:
                return {
                    "date": today_str,
                    "long": int(payload.get("long", 0)),
                    "short": int(payload.get("short", 0)),
                }
    except Exception:
        pass
    return {"date": today_str, "long": 0, "short": 0}


def _candE4_g2_persist():
    try:
        # Use plain json write -- _atomic_write_json may not exist in this file
        tmp = CANDE4_G2_COUNTERS_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(dict(_candE4_g2_counters), f)
        os.replace(tmp, CANDE4_G2_COUNTERS_FILE)
    except Exception:
        pass


def _candE4_g2_check_and_increment(side):
    s = str(side).strip().upper()
    cap = CANDE4_MAX_LONG_PER_DAY if s == "LONG" else CANDE4_MAX_SHORT_PER_DAY
    side_key = "long" if s == "LONG" else "short"
    today_str = datetime.now(IST).strftime("%Y-%m-%d")
    with _candE4_g2_lock:
        if _candE4_g2_counters.get("date") != today_str:
            fresh = _candE4_g2_load_from_disk(today_str)
            _candE4_g2_counters.clear()
            _candE4_g2_counters.update(fresh)
        cur = int(_candE4_g2_counters.get(side_key, 0))
        if cur >= cap:
            return False, f"candE4_G2_daily_cap {s} reached ({cur}/{cap})"
        _candE4_g2_counters[side_key] = cur + 1
        _candE4_g2_persist()
    return True, "ok"
SLIPPAGE_PCT = 0.0005  # 5 bps realistic slippage on entry
# P2-10: exit slippage applied on SL fills and unscheduled closes (time stop,
# kill switch, forced close).  TARGET fills are limit orders — assume filled
# at limit.  0 = disabled (parity with old behaviour).
EXIT_SLIPPAGE_BPS = float(os.getenv("EQIDV2_PAPER_EXIT_SLIPPAGE_BPS", "5.0"))

# Max entry slip gate: if the live LTP (or signal_bar fallback) is more than this
# fraction above the model trigger price for a LONG, the signal is rejected rather
# than entered at a worsened price.  Set to 0.0 to disable.
# Background: B_HUGE_C1_CLOSE_RECLAIM_BREAK signals arrive ~35s after bar close.
# The bar's close is already 0.5–0.8% above the model trigger, leaving little
# room with the tighter tuned V16_5min stop. A cap of 0.003 (0.3%) rejects
# those chase entries.
LONG_MAX_ENTRY_SLIP_PCT = float(os.getenv("EQIDV2_LONG_MAX_ENTRY_SLIP_PCT", "0.003"))
# Same gate for SHORT (price must not be more than this BELOW model trigger).
SHORT_MAX_ENTRY_SLIP_PCT = float(os.getenv("EQIDV2_SHORT_MAX_ENTRY_SLIP_PCT", "0.003"))
ENTRY_RETRY_NEAR_ENTRY_ENABLE = str(os.getenv("EQIDV2_ENTRY_RETRY_NEAR_ENTRY_ENABLE", "1")).strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
ENTRY_RETRY_NEAR_ENTRY_PCT = float(os.getenv("EQIDV2_ENTRY_RETRY_NEAR_ENTRY_PCT", "0.003"))
ENTRY_RETRY_WAIT_SEC = int(os.getenv("EQIDV2_ENTRY_RETRY_WAIT_SEC", "300"))
ENTRY_RETRY_POLL_SEC = float(os.getenv("EQIDV2_ENTRY_RETRY_POLL_SEC", "2"))  # per-cycle LTP poll interval (spec: Section 10)
ENTRY_SLOT_MAX_WAIT_SEC = max(0, int(os.getenv("EQIDV2_ENTRY_SLOT_MAX_WAIT_SEC", "300")))
# Fix #20 (post-2026-04-21): stale-detection guard.
LATE_DETECTION_GUARD_ENABLE = str(os.getenv("EQIDV2_LATE_DETECTION_GUARD_ENABLE", "1")).strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
LATE_DETECTION_MAX_LAG_SEC = int(os.getenv("EQIDV2_LATE_DETECTION_MAX_LAG_SEC", "30"))

def _late_lag_threshold_for_setup(setup: Optional[str]) -> int:
    return LATE_DETECTION_MAX_LAG_SEC

_LATE_SKIPPED_LOCK = threading.Lock()
_late_skipped_count = 0




def _entry_price_within_retry_band(side: str, signal_entry_price: float, live_price: float) -> bool:
    if signal_entry_price <= 0 or live_price <= 0:
        return False
    band = max(0.0, float(ENTRY_RETRY_NEAR_ENTRY_PCT))
    if side == "LONG":
        return live_price <= signal_entry_price * (1.0 + band)
    return live_price >= signal_entry_price * (1.0 - band)


def _wait_for_near_entry_price(
    ticker: str,
    side: str,
    signal_entry_price: float,
    retry_until_ist: datetime,
    use_ltp: bool = True,
) -> Optional[float]:
    last_ltp: Optional[float] = None
    poll_sec = max(1.0, float(ENTRY_RETRY_POLL_SEC))
    while datetime.now(IST) <= retry_until_ist:
        ltp_now = get_ltp(ticker) if use_ltp else None
        if ltp_now is not None and ltp_now > 0:
            last_ltp = float(ltp_now)
            if _entry_price_within_retry_band(side, signal_entry_price, last_ltp):
                return last_ltp
        time.sleep(poll_sec)
    return last_ltp


def _entry_retry_deadline(
    signal: dict,
    trade_start_ist: datetime,
    forced_close_dt: datetime,
) -> datetime:
    # The writer's contract deadline is authoritative. Detection, polling, and
    # near-entry retries must never extend execution beyond this timestamp.
    contract_deadline = _parse_ist_signal_ts(signal.get("deadline_ist"))
    if contract_deadline is not None:
        return min(contract_deadline.to_pydatetime(), forced_close_dt)

    # Anchor to Stage-2 confirmation time first — this is the definitive moment
    # the signal was live in the two-stage pipeline (detected_time_ist/logtime_ist).
    # For same-cycle detections (received_time == detected_time_ist) the old
    # rebasing logic produced a deadline anchored to the stale model entry slot,
    # causing ENTRY_SKIPPED_STALE_SIGNAL on all early-session signals.
    base_ts = _parse_ist_signal_ts(
        signal.get("detected_time_ist") or signal.get("logtime_ist")
    )
    # Fall back to Stage-1 pending pool insertion time.
    if base_ts is None:
        base_ts = _parse_ist_signal_ts(signal.get("received_time"))
    # Final fallback: model entry slot (legacy rows without pipeline timestamps).
    if base_ts is None:
        base_ts = _parse_ist_signal_ts(
            signal.get("signal_entry_datetime_ist")
            or signal.get("signal_bar_time_ist")
            or signal.get("bar_time_ist")
            or signal.get("signal_datetime")
        )
    if base_ts is None:
        candidate = trade_start_ist + timedelta(seconds=max(1, ENTRY_RETRY_WAIT_SEC))
    else:
        candidate = base_ts.to_pydatetime() + timedelta(seconds=max(1, ENTRY_RETRY_WAIT_SEC))
    return min(candidate, forced_close_dt)


def _trade_started_after_entry_deadline(
    trade_start_ist: Optional[datetime],
    entry_retry_deadline: Optional[datetime],
) -> bool:
    if trade_start_ist is None or entry_retry_deadline is None:
        return False
    return trade_start_ist >= entry_retry_deadline


def _detection_lag_seconds(signal: dict) -> Optional[float]:
    """Seconds between the model entry slot and Stage 2 detection."""
    detected = _parse_ist_signal_ts(
        signal.get("detected_time_ist") or signal.get("logtime_ist")
    )
    entry_slot = _parse_ist_signal_ts(
        signal.get("signal_entry_datetime_ist")
        or signal.get("signal_bar_time_ist")
        or signal.get("bar_time_ist")
    )
    if detected is None or entry_slot is None:
        return None
    return (detected - entry_slot).total_seconds()


def _append_late_skipped_csv(signal: dict, lag_sec: float, threshold_sec: int) -> None:
    """Append one row to late_skipped_<date>_id_5min_v7_PAPER.csv and bump heartbeat counter."""
    global _late_skipped_count
    try:
        date_str = datetime.now(IST).strftime("%Y-%m-%d")
        path = Path(SIGNAL_DIR) / f"late_skipped_{date_str}_id_5min_v7_PAPER.csv"
        row = {
            "skipped_at_ist": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S%z"),
            "ticker":         str(signal.get("ticker", "")),
            "side":           str(signal.get("side", "")),
            "setup":          str(signal.get("setup", "")),
            "signal_id":      str(signal.get("signal_id", "")),
            "signal_bar":     str(signal.get("signal_time_ist") or signal.get("signal_bar_time_ist") or ""),
            "entry_slot":     str(signal.get("signal_entry_datetime_ist") or ""),
            "detected_time":  str(signal.get("detected_time_ist") or ""),
            "lag_sec":        "" if lag_sec is None else f"{lag_sec:.1f}",
            "threshold_sec":  str(threshold_sec),
        }
        with _LATE_SKIPPED_LOCK:
            new_file = not path.exists()
            with path.open("a", encoding="utf-8", newline="") as fh:
                writer = csv.DictWriter(fh, fieldnames=list(row.keys()))
                if new_file:
                    writer.writeheader()
                writer.writerow(row)
            _late_skipped_count += 1
    except Exception as exc:
        log.warning(f"[LATE_SKIP] CSV append failed: {exc}")


SHORT_STOP_PCT = float(
    os.getenv(
        "EQIDV7_ID_5MIN_SHORT_STOP_PCT",
        "0.0075",
    )
)
LONG_STOP_PCT = float(
    os.getenv(
        "EQIDV7_ID_5MIN_LONG_STOP_PCT",
        "0.0075",
    )
)
SHORT_TARGET_PCT = float(
    os.getenv(
        "EQIDV7_ID_5MIN_SHORT_TARGET_PCT",
        "0.0100",
    )
)
LONG_TARGET_PCT = float(
    os.getenv(
        "EQIDV7_ID_5MIN_LONG_TARGET_PCT",
        "0.0100",
    )
)
ENTRY_PRICE_SOURCE_CHOICES = ("signal_bar", "ltp_on_signal")
ENTRY_PRICE_SOURCE_DEFAULT = str(os.getenv("ENTRY_PRICE_SOURCE", "ltp_on_signal")).strip().lower()
if ENTRY_PRICE_SOURCE_DEFAULT not in ENTRY_PRICE_SOURCE_CHOICES:
    ENTRY_PRICE_SOURCE_DEFAULT = "signal_bar"

# Default capital
DEFAULT_START_CAPITAL = 1_000_000
# Fallback margin capital when the signal row omits quantity.
DEFAULT_POSITION_SIZE = float(
    os.getenv(
        "EQIDV7_ID_5MIN_DEFAULT_POSITION_SIZE_RS",
        os.getenv("EQIDV7_ID_5MIN_DEFAULT_POSITION_SIZE_RS", "10000"),
    )
)
INTRADAY_LEVERAGE = 5.0             # MIS leverage on Zerodha

# Exposure limits for the broad V7 paper/research runner.
RISK_LIMITS_ENABLED = str(
    os.getenv(
        "EQIDV2_PAPER_V7_ID_5MIN_ENABLE_RISK_LIMITS",
        os.getenv("EQIDV2_ENABLE_RISK_LIMITS", "1"),
    )
).strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
MAX_OPEN_POSITIONS = int(
    os.getenv(
        "EQIDV2_PAPER_V7_ID_5MIN_MAX_OPEN_POSITIONS",
        os.getenv("EQIDV2_MAX_OPEN_POSITIONS", "100"),
    )
)
MAX_CAPITAL_DEPLOYED_RS = float(
    os.getenv(
        "EQIDV2_PAPER_V7_ID_5MIN_MAX_CAPITAL_DEPLOYED_RS",
        os.getenv("EQIDV2_MAX_CAPITAL_DEPLOYED_RS", "2000000"),
    )
)
# P1-7: gross short notional cap (same env var as live for symmetric config).  0 = disabled.
MAX_GROSS_SHORT_NOTIONAL_RS = float(os.getenv("EQIDV2_MAX_GROSS_SHORT_NOTIONAL_RS", "1500000.0"))

# Research suggestions are promoted into PAPER_TRADE_TRUE first. These gates do
# not affect live/scanner signal creation; they only log paper skip rows so the
# next research session can prove or reject the idea.
RESEARCH_PAPER_GATES_ENABLED = str(os.getenv("EQIDV2_PAPER_V7_RESEARCH_GATES_ENABLED", "1")).strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
# P0.2: setups completely blocked from paper trading. Add comma-separated names to the env var to extend.
PAPER_BLOCKED_SETUPS: frozenset = frozenset(
    s.strip().upper()
    for s in os.getenv(
        "EQIDV2_PAPER_V7_BLOCKED_SETUPS",
        "T_TREND_DAY_EMA_STAIR_SHORT",
    ).split(",")
    if s.strip()
)
ANTI_CHASE_LONG_CLOSE_LOC_MIN = float(os.getenv("EQIDV2_PAPER_V7_ANTI_CHASE_LONG_CLOSE_LOC_MIN", "0.97"))
ANTI_CHASE_LONG_VWAP_DIST_ATR_MIN = float(os.getenv("EQIDV2_PAPER_V7_ANTI_CHASE_LONG_VWAP_DIST_ATR_MIN", "3.50"))
B_AVWAP_RECLAIM_MIN_RANKER_SCORE = float(os.getenv("EQIDV2_PAPER_V7_B_AVWAP_MIN_RANKER_SCORE", "0.65"))
# P0-19: brake ON by default + Rs10k limit (= live) when conf qualification is active.
DAILY_LOSS_BRAKE_ENABLED = str(os.getenv(
    "EQIDV2_PAPER_V7_DAILY_LOSS_BRAKE_ENABLED", "1" if _CONF_QUAL_MODE else "0")).strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
DAILY_LOSS_BRAKE_RS = abs(float(os.getenv(
    "EQIDV2_PAPER_V7_DAILY_LOSS_BRAKE_RS", "10000" if _CONF_QUAL_MODE else "7500")))
# rev-2 P0-18: MTM-aware brake (realized + open MTM, + per-day throttle + per-setup
# concurrency cap) via eqidv2_risk_brake. OBSERVE-mode logs the decision only (default
# ON during the conf qualification) — no behavior change. ACT-mode (EQIDV2_BRAKE_MTM_ACT)
# lets it actually block entries; flatten stays separately flag-gated in the module.
_MTM_BRAKE_OBSERVE = str(os.getenv(
    "EQIDV2_BRAKE_MTM_OBSERVE", "1" if _CONF_QUAL_MODE else "0")).strip().lower() in {"1", "true", "yes", "on"}
_MTM_BRAKE_ACT = str(os.getenv("EQIDV2_BRAKE_MTM_ACT", "0")).strip().lower() in {"1", "true", "yes", "on"}
C_OR_BREAKOUT_TIME_STOP_ENABLED = str(
    os.getenv("EQIDV2_PAPER_V7_C_OR_BREAKOUT_TIME_STOP_ENABLED", "1")
).strip().lower() in {"1", "true", "yes", "on"}
C_OR_BREAKOUT_TIME_STOP_MIN = max(0, int(float(os.getenv("EQIDV2_PAPER_V7_C_OR_BREAKOUT_TIME_STOP_MIN", "30"))))
C_OR_BREAKOUT_SESSION_CAP_ENABLED = str(
    os.getenv("EQIDV2_PAPER_V7_C_OR_BREAKOUT_SESSION_CAP_ENABLED", "1")
).strip().lower() in {"1", "true", "yes", "on"}
C_OR_BREAKOUT_SESSION_CAP = max(0, int(float(os.getenv("EQIDV2_PAPER_V7_C_OR_BREAKOUT_SESSION_CAP", "50"))))
C_OR_BREAKOUT_SESSION_CAP_COUNTER_FILE = os.path.join(
    SIGNAL_DIR,
    "c_or_breakout_session_cap_counter_id_5min_v7_paper.json",
)
RESEARCH_PAPER_GATE_VERSION = "v7_research_2026_06_04_pf_sl_eod"

# P1.2: once E_VWAP_LOSE_EARLY_SHORT reaches +0.5R profit, move SL to breakeven
# so a subsequent reversal cannot turn a winner into a full -1R loss (AWFIS pattern).
VWAP_EARLY_SHORT_BE_STOP_ENABLED = str(
    os.getenv("EQIDV2_PAPER_V7_VWAP_EARLY_SHORT_BE_STOP", "1")
).strip().lower() in {"1", "true", "yes", "on"}
VWAP_EARLY_SHORT_BE_STOP_R = float(os.getenv("EQIDV2_PAPER_V7_VWAP_EARLY_SHORT_BE_STOP_R", "0.5"))

_candidate_context_cache_lock = threading.Lock()
_candidate_context_cache: Dict[str, Tuple[float, Dict[str, dict]]] = {}
_c_or_session_cap_lock = threading.Lock()
_c_or_session_cap_counter: dict = {"date": "", "signal_ids": []}

# Leave unset so quantity is taken from the signal row when present.
_FORCE_ENTRY_QUANTITY_RAW = str(os.getenv("EQIDV7_ID_5MIN_FORCE_ENTRY_QUANTITY", "")).strip()
try:
    FORCE_ENTRY_QUANTITY: Optional[int] = (
        max(1, int(_FORCE_ENTRY_QUANTITY_RAW)) if _FORCE_ENTRY_QUANTITY_RAW else None
    )
except Exception:
    FORCE_ENTRY_QUANTITY = None

# Paper trade log columns
TRADE_LOG_COLUMNS = [
    "trade_id",
    "signal_id",
    "signal_datetime",
    "signal_entry_datetime_ist",
    "entry_time",
    "exit_time",
    "ticker",
    "side",
    "setup",
    "impulse_type",
    "quantity",
    "entry_price",
    "exit_price",
    "stop_price",
    "initial_stop_price",
    "target_price",
    "outcome",
    "gross_pnl",
    "total_cost",
    "net_pnl",
    "gross_pnl_rs",
    "gross_pnl_pct",
    "brokerage_rs",
    "stt_rs",
    "exch_txn_rs",
    "sebi_rs",
    "ipft_rs",
    "stamp_rs",
    "gst_rs",
    "total_cost_rs",
    "net_pnl_rs",
    "net_pnl_pct",
    "cost_bps_of_turnover",
    "cost_pct_of_entry",
    "cost_rates_as_of",
    "pnl_rs",
    "pnl_pct",
    "quality_score",
    "p_win",
    "confidence_multiplier",
]

# Signal CSV columns (must match signal generator output)
SIGNAL_COLUMNS = [
    "signal_id", "signal_datetime", "received_time", "ticker", "side",
    "setup", "impulse_type", "entry_price", "stop_price", "target_price",
    "quality_score", "atr_pct", "rsi", "adx", "quantity",
    "signal_entry_datetime_ist", "signal_bar_time_ist",
]

# Column name mapping: signal generator CSV name Ã¢â€ â€™ executor expected name
_SIGNAL_COL_MAP = {
    "entry":          "entry_price",
    "sl":             "stop_price",
    "target":         "target_price",
    "impulse":        "impulse_type",
    "created_ts_ist": "signal_datetime",
    "bar_time_ist":   "signal_bar_time_ist",
    "signal_bar_time_ist": "signal_bar_time_ist",
    "conf_mult":      "confidence_multiplier",
}

# ============================================================================
# LOGGING
# ============================================================================
def setup_logging() -> logging.Logger:
    logger = logging.getLogger("paper_trade_id_5min_v7")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s")

    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    os.makedirs(SIGNAL_DIR, exist_ok=True)
    today_str = datetime.now(IST).strftime("%Y-%m-%d")
    fh = logging.FileHandler(
        os.path.join(SIGNAL_DIR, PAPER_TRADE_EXEC_LOG_PATTERN.format(today_str)),
        mode="a",
        encoding="utf-8",
    )
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger


log = setup_logging()


# ============================================================================
# KITE SESSION (optional Ã¢â‚¬â€ for LTP simulation)
# ============================================================================
kite = None
_ltp_last_error_by_ticker: Dict[str, str] = {}
_ltp_error_lock = threading.Lock()
_ltp_cache_by_ticker: Dict[str, Tuple[float, float]] = {}
_ltp_cache_lock = threading.Lock()
_ltp_batch_refresh_lock = threading.Lock()
_ltp_last_batch_refresh_monotonic = 0.0
LTP_CACHE_TTL_SEC = float(os.getenv("EQIDV2_LTP_CACHE_TTL_SEC", "2.0"))
LTP_BATCH_MIN_INTERVAL_SEC = float(os.getenv("EQIDV2_LTP_BATCH_MIN_INTERVAL_SEC", "1.0"))


def _normalize_ticker_symbol(ticker: str) -> str:
    return str(ticker or "").strip().upper()


def _ltp_instrument_candidates(ticker: str) -> List[str]:
    raw = _normalize_ticker_symbol(ticker)
    if not raw:
        return []
    if ":" in raw:
        ex, sym = raw.split(":", 1)
        ex = ex.strip().upper()
        sym = sym.strip().upper()
        if not sym:
            return []
        out: List[str] = [f"{ex}:{sym}"]
        if ex != "NSE":
            out.append(f"NSE:{sym}")
        if ex != "BSE":
            out.append(f"BSE:{sym}")
        return out
    return [f"NSE:{raw}", f"BSE:{raw}"]


def _set_ltp_error(ticker: str, msg: str) -> None:
    key = _normalize_ticker_symbol(ticker)
    if not key:
        return
    with _ltp_error_lock:
        if msg:
            _ltp_last_error_by_ticker[key] = str(msg)
        else:
            _ltp_last_error_by_ticker.pop(key, None)


def get_last_ltp_error(ticker: str) -> str:
    key = _normalize_ticker_symbol(ticker)
    if not key:
        return ""
    with _ltp_error_lock:
        return str(_ltp_last_error_by_ticker.get(key, ""))


def _get_cached_ltp(ticker: str, max_age_sec: Optional[float] = None) -> Optional[float]:
    key = _normalize_ticker_symbol(ticker)
    if not key:
        return None
    max_age = max(0.1, float(max_age_sec if max_age_sec is not None else LTP_CACHE_TTL_SEC))
    now_mono = time.monotonic()
    with _ltp_cache_lock:
        row = _ltp_cache_by_ticker.get(key)
    if row is None:
        return None
    value, updated_mono = row
    if now_mono - updated_mono <= max_age and value > 0:
        return float(value)
    return None


def _cache_ltp_value(ticker: str, ltp: float) -> None:
    key = _normalize_ticker_symbol(ticker)
    if not key or ltp <= 0:
        return
    with _ltp_cache_lock:
        _ltp_cache_by_ticker[key] = (float(ltp), time.monotonic())


def _active_ltp_tickers(requested_ticker: str) -> List[str]:
    tickers: Set[str] = set()
    requested = _normalize_ticker_symbol(requested_ticker)
    if requested:
        tickers.add(requested)
    try:
        with active_positions_lock:
            for pos in active_positions.values():
                key = _normalize_ticker_symbol(str(pos.get("ticker", "")))
                if key:
                    tickers.add(key)
    except NameError:
        pass
    return sorted(tickers)

_kite_session_lock = threading.Lock()
_kite_last_refresh_monotonic = 0.0
KITE_AUTH_RETRY_COOLDOWN_SEC = 10.0

def _read_first_token(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        txt = f.read().strip()
    if not txt:
        raise RuntimeError(f"empty auth file: {path}")
    return txt.split()[0].strip()

def _kite_auth_profiles() -> List[Tuple[str, str, str]]:
    profiles: List[Tuple[str, str, str]] = []
    specs = [
        ("app1", "api_key.txt", "access_token.txt"),
        ("app2", "api_key2.txt", "access_token2.txt"),
        ("app3", "api_key3.txt", "access_token3.txt"),
        ("app4", "api_key4.txt", "access_token4.txt"),
        ("app5", "api_key5.txt", "access_token5.txt"),
        ("app6", "api_key6.txt", "access_token6.txt"),
        ("app7", "api_key7.txt", "access_token7.txt"),
        ("app8", "api_key8.txt", "access_token8.txt"),
    ]
    for profile_name, key_path, token_path in specs:
        if os.path.exists(key_path) and os.path.exists(token_path):
            profiles.append((profile_name, key_path, token_path))
    return profiles

def _is_kite_auth_error(exc: Exception) -> bool:
    msg = str(exc).strip().lower()
    return ("incorrect `api_key` or `access_token`" in msg) or ("tokenexception" in msg and "access_token" in msg)

def _setup_kite_session_impl(log_success: bool = True) -> bool:
    global kite
    from kiteconnect import KiteConnect

    last_error: Optional[Exception] = None
    for profile_name, key_path, token_path in _kite_auth_profiles():
        try:
            api_key = _read_first_token(key_path)
            access_token = _read_first_token(token_path)
            client = KiteConnect(api_key=api_key)
            client.set_access_token(access_token)
            client.profile()  # validates api_key + access_token pairing
            kite = client
            if log_success:
                log.info(
                    "Kite session established (%s profile: %s + %s).",
                    profile_name,
                    key_path,
                    token_path,
                )
            return True
        except Exception as e:
            last_error = e
            log.warning(
                "Kite auth profile '%s' failed (%s + %s): %s",
                profile_name,
                key_path,
                token_path,
                e,
            )

    kite = None
    if last_error is not None:
        log.warning("Kite session setup failed for all profiles: %s", last_error)
    else:
        log.warning("Kite session setup failed: no auth profile files found.")
    return False

def _refresh_kite_session(reason: str, force: bool = False) -> bool:
    global _kite_last_refresh_monotonic
    now_mono = time.monotonic()
    with _kite_session_lock:
        if (not force) and (now_mono - _kite_last_refresh_monotonic < KITE_AUTH_RETRY_COOLDOWN_SEC):
            return kite is not None
        _kite_last_refresh_monotonic = now_mono
        log.warning("[KITE.AUTH] Refreshing session due to: %s", reason)
        return _setup_kite_session_impl(log_success=True)


def setup_kite_session(reason: str = "startup", force: bool = True):
    """Set up Kite session for LTP polling. Non-fatal if it fails."""
    global kite
    try:
        ok = _refresh_kite_session(reason, force=force)
        if not ok:
            raise RuntimeError("all Kite auth profiles failed")
    except Exception as e:
        log.warning(f"Kite session not available right now: {e}")
        log.warning("LTP polling remains enabled; runtime will retry Kite session automatically.")
        kite = None

def _extract_ltp_from_payload(ticker: str, data: object, instruments: List[str]) -> Optional[float]:
    if isinstance(data, dict):
        for inst in instruments:
            row = data.get(inst)
            if not isinstance(row, dict):
                continue
            ltp = _safe_float(row.get("last_price", 0.0), 0.0)
            if ltp > 0:
                _set_ltp_error(ticker, "")
                return float(ltp)
    return None


def _refresh_ltp_batch(tickers: Sequence[str]) -> bool:
    global _ltp_last_batch_refresh_monotonic

    normalized = sorted({_normalize_ticker_symbol(t) for t in tickers if _normalize_ticker_symbol(t)})
    if not normalized:
        return False

    if kite is None:
        setup_kite_session(reason="ltp_batch_refresh", force=False)
        if kite is None:
            for t in normalized:
                _set_ltp_error(t, "kite_session_unavailable")
            return False

    now_mono = time.monotonic()
    if now_mono - _ltp_last_batch_refresh_monotonic < max(0.1, LTP_BATCH_MIN_INTERVAL_SEC):
        return False
    _ltp_last_batch_refresh_monotonic = now_mono

    ticker_to_instruments: Dict[str, List[str]] = {}
    instruments: List[str] = []
    seen_instruments: Set[str] = set()
    for ticker in normalized:
        candidates = _ltp_instrument_candidates(ticker)
        if not candidates:
            continue
        ticker_to_instruments[ticker] = candidates
        for inst in candidates:
            if inst not in seen_instruments:
                seen_instruments.add(inst)
                instruments.append(inst)

    if not instruments:
        return False

    def _apply_payload(payload: object) -> bool:
        any_hit = False
        for ticker, candidates in ticker_to_instruments.items():
            ltp = _extract_ltp_from_payload(ticker, payload, candidates)
            if ltp is not None:
                _cache_ltp_value(ticker, float(ltp))
                any_hit = True
        return any_hit

    try:
        data = kite.ltp(instruments if len(instruments) > 1 else instruments[0])
        return _apply_payload(data)
    except Exception as e:
        if _is_kite_auth_error(e) and _refresh_kite_session("ltp batch auth error", force=False) and kite is not None:
            try:
                data = kite.ltp(instruments if len(instruments) > 1 else instruments[0])
                return _apply_payload(data)
            except Exception as e2:
                for t in normalized:
                    _set_ltp_error(t, f"ltp_batch_error={e2}")
        else:
            for t in normalized:
                _set_ltp_error(t, f"ltp_batch_error={e}")
    return False


def get_ltp(ticker: str) -> Optional[float]:
    """Get last traded price from Kite with NSE/BSE fallback."""
    cached = _get_cached_ltp(ticker)
    if cached is not None:
        return cached

    batch_tickers = _active_ltp_tickers(ticker)
    if len(batch_tickers) >= 5:
        acquired = _ltp_batch_refresh_lock.acquire(timeout=max(0.2, LTP_CACHE_TTL_SEC))
        try:
            cached = _get_cached_ltp(ticker)
            if cached is not None:
                return cached
            if acquired:
                _refresh_ltp_batch(batch_tickers)
        finally:
            if acquired:
                _ltp_batch_refresh_lock.release()
        cached = _get_cached_ltp(ticker, max_age_sec=max(LTP_CACHE_TTL_SEC * 2.0, 1.0))
        if cached is not None:
            return cached
        return None

    if kite is None:
        setup_kite_session(reason=f"ltp_request ticker={ticker}", force=False)
        if kite is None:
            _set_ltp_error(ticker, "kite_session_unavailable")
            return None
    instruments = _ltp_instrument_candidates(ticker)
    if not instruments:
        return None

    try:
        data = kite.ltp(instruments if len(instruments) > 1 else instruments[0])
        ltp = _extract_ltp_from_payload(ticker, data, instruments)
        if ltp is not None:
            _cache_ltp_value(ticker, float(ltp))
            return ltp
    except Exception as e:
        if _is_kite_auth_error(e) and _refresh_kite_session(f"ltp auth error ticker={ticker}", force=False) and kite is not None:
            try:
                data = kite.ltp(instruments if len(instruments) > 1 else instruments[0])
                ltp = _extract_ltp_from_payload(ticker, data, instruments)
                if ltp is not None:
                    _cache_ltp_value(ticker, float(ltp))
                    return ltp
            except Exception as e2:
                _set_ltp_error(ticker, f"ltp_error={e2}")
        else:
            _set_ltp_error(ticker, f"ltp_error={e}")

    if kite is None:
        return None
    try:
        data_q = kite.quote(instruments)
        ltp = _extract_ltp_from_payload(ticker, data_q, instruments)
        if ltp is not None:
            _cache_ltp_value(ticker, float(ltp))
            return ltp
        _set_ltp_error(ticker, f"no_valid_last_price candidates={','.join(instruments)}")
    except Exception as e:
        if _is_kite_auth_error(e) and _refresh_kite_session(f"quote auth error ticker={ticker}", force=False) and kite is not None:
            try:
                data_q = kite.quote(instruments)
                ltp = _extract_ltp_from_payload(ticker, data_q, instruments)
                if ltp is not None:
                    _cache_ltp_value(ticker, float(ltp))
                    return ltp
            except Exception as e2:
                _set_ltp_error(ticker, f"quote_error={e2}")
        else:
            _set_ltp_error(ticker, f"quote_error={e}")
    return None


# ============================================================================
# EXECUTED SIGNALS TRACKING
# ============================================================================
def load_executed_signals() -> Set[str]:
    if os.path.exists(EXECUTED_SIGNALS_FILE):
        try:
            with open(EXECUTED_SIGNALS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                # Reset if from a different day
                if data.get("date") != datetime.now(IST).strftime("%Y-%m-%d"):
                    return set()
                return set(data.get("signals", []))
        except (json.JSONDecodeError, KeyError):
            # keep a copy for forensic debugging; do not hard-fail the engine
            try:
                bad = EXECUTED_SIGNALS_FILE + ".corrupt"
                os.replace(EXECUTED_SIGNALS_FILE, bad)
            except Exception:
                pass
            return set()
    return set()


def save_executed_signals(executed: Set[str]) -> None:
    payload = {
        "date": datetime.now(IST).strftime("%Y-%m-%d"),
        "signals": sorted(set(executed)),
    }
    _atomic_write_json(EXECUTED_SIGNALS_FILE, payload)


# ============================================================================
# TRADE SIMULATION
# ============================================================================
@dataclass
class PaperTrade:
    trade_id: str = ""
    signal_id: str = ""
    signal_datetime: str = ""
    signal_entry_datetime_ist: str = ""
    entry_time: str = ""
    exit_time: str = ""
    ticker: str = ""
    side: str = ""
    setup: str = ""
    impulse_type: str = ""
    quantity: int = 1
    entry_price: float = 0.0
    exit_price: float = 0.0
    stop_price: float = 0.0
    initial_stop_price: float = 0.0
    target_price: float = 0.0
    outcome: str = ""
    gross_pnl: float = 0.0
    total_cost: float = 0.0
    net_pnl: float = 0.0
    gross_pnl_rs: float = 0.0
    gross_pnl_pct: float = 0.0
    brokerage_rs: float = 0.0
    stt_rs: float = 0.0
    exch_txn_rs: float = 0.0
    sebi_rs: float = 0.0
    ipft_rs: float = 0.0
    stamp_rs: float = 0.0
    gst_rs: float = 0.0
    total_cost_rs: float = 0.0
    net_pnl_rs: float = 0.0
    net_pnl_pct: float = 0.0
    cost_bps_of_turnover: float = 0.0
    cost_pct_of_entry: float = 0.0
    cost_rates_as_of: str = ""
    pnl_rs: float = 0.0
    pnl_pct: float = 0.0
    quality_score: float = 0.0
    p_win: float = 0.0
    confidence_multiplier: float = 1.0


# Shared state
active_trades: Dict[str, threading.Thread] = {}
active_trades_lock = threading.Lock()
inflight_signals: Set[str] = set()
inflight_signals_lock = threading.Lock()
executed_lock = threading.Lock()
active_positions: Dict[str, dict] = {}  # signal_id -> open position state
active_positions_lock = threading.Lock()
opened_entry_ids: Set[str] = set()  # signal_ids that actually opened today; skips excluded
opened_entry_ids_lock = threading.Lock()
state_file_lock = threading.Lock()
kill_switch_cache_lock = threading.Lock()
kill_switch_cache_mtime: float = -1.0
kill_switch_cache_payload: Optional[dict] = None
daily_pnl: Dict[str, float] = {
    "total": 0.0,
    "gross_total": 0.0,
    "total_cost": 0.0,
    "wins": 0,
    "losses": 0,
    "trades": 0,
    "gross_profit": 0.0,
    "gross_loss": 0.0,
    "net_profit": 0.0,
    "net_loss": 0.0,
}
daily_pnl_lock = threading.Lock()
NSE_COST_CONFIG = CostConfig()

# Capital / position tracking (margin, not notional Ã¢â‚¬â€ accounts for MIS leverage)
capital_deployed: Dict[str, float] = {}   # signal_id Ã¢â€ â€™ margin blocked
capital_lock = threading.Lock()


def _fmt_rs(v: float) -> str:
    return f"Rs.{v:,.2f}"


def _fmt_rs_signed(v: float) -> str:
    return f"Rs.{v:+,.2f}"


def _calc_pnl(side: str, entry_price: float, exit_price: float, qty: int) -> Tuple[float, float]:
    if side.upper() == "SHORT":
        pnl_rs = (entry_price - exit_price) * qty
    else:
        pnl_rs = (exit_price - entry_price) * qty
    pnl_pct = (pnl_rs / (entry_price * qty) * 100) if (entry_price > 0 and qty > 0) else 0.0
    return float(pnl_rs), float(pnl_pct)


def _calc_costed_pnl(side: str, entry_price: float, exit_price: float, qty: int) -> dict:
    gross_pnl, gross_pct = _calc_pnl(side, entry_price, exit_price, qty)
    try:
        b = intraday_equity_costs(
            float(entry_price),
            float(exit_price),
            float(qty),
            str(side).upper(),
            NSE_COST_CONFIG,
        )
    except Exception:
        return {
            "gross_pnl_rs": float(gross_pnl),
            "gross_pnl_pct": float(gross_pct),
            "brokerage_rs": 0.0,
            "stt_rs": 0.0,
            "exch_txn_rs": 0.0,
            "sebi_rs": 0.0,
            "ipft_rs": 0.0,
            "stamp_rs": 0.0,
            "gst_rs": 0.0,
            "total_cost_rs": 0.0,
            "net_pnl_rs": float(gross_pnl),
            "net_pnl_pct": float(gross_pct),
            "cost_bps_of_turnover": 0.0,
            "cost_pct_of_entry": 0.0,
            "cost_rates_as_of": NSE_COST_CONFIG.rates_as_of,
        }
    entry_notional = float(entry_price) * float(qty)
    net_pct = (float(b.net_pnl) / entry_notional * 100.0) if entry_notional > 0 else 0.0
    return {
        "gross_pnl_rs": float(b.gross_pnl),
        "gross_pnl_pct": float(gross_pct),
        "brokerage_rs": float(b.brokerage),
        "stt_rs": float(b.stt),
        "exch_txn_rs": float(b.exch_txn),
        "sebi_rs": float(b.sebi),
        "ipft_rs": float(b.ipft),
        "stamp_rs": float(b.stamp),
        "gst_rs": float(b.gst),
        "total_cost_rs": float(b.total_cost),
        "net_pnl_rs": float(b.net_pnl),
        "net_pnl_pct": float(net_pct),
        "cost_bps_of_turnover": float(b.cost_bps_of_turnover),
        "cost_pct_of_entry": float(b.cost_pct_of_entry),
        "cost_rates_as_of": NSE_COST_CONFIG.rates_as_of,
    }


def _row_float_first(row: dict, keys: Sequence[str], default: float = 0.0) -> float:
    for key in keys:
        if key not in row:
            continue
        raw = row.get(key)
        if raw is None or str(raw).strip() == "":
            continue
        return _safe_float(raw, default)
    return float(default)


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _safe_int(value: object, default: int = 1) -> int:
    try:
        parsed = int(float(value))
        return parsed if parsed > 0 else int(default)
    except Exception:
        return int(default)


def _normalise_signal_ts_text(value: object) -> str:
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return str(value or "").strip()
    ts = pd.Timestamp(parsed)
    if ts.tzinfo is None:
        ts = ts.tz_localize(IST)
    else:
        ts = ts.tz_convert(IST)
    offset = ts.strftime("%z")
    return f"{ts.strftime('%Y-%m-%d %H:%M:%S')}{offset[:3]}:{offset[3:]}"


def _signal_context_key(row: dict) -> str:
    signal_time = (
        row.get("signal_entry_datetime_ist")
        or row.get("signal_bar_time_ist")
        or row.get("signal_datetime")
        or row.get("signal_time_ist")
        or ""
    )
    return "|".join(
        [
            str(row.get("ticker", "")).strip().upper(),
            str(row.get("side", "")).strip().upper(),
            str(row.get("setup", "")).strip(),
            _normalise_signal_ts_text(signal_time),
        ]
    )


def _signal_day(signal: dict) -> str:
    for key in ("signal_entry_datetime_ist", "signal_bar_time_ist", "signal_datetime", "signal_time_ist"):
        ts = pd.to_datetime(signal.get(key, ""), errors="coerce")
        if pd.notna(ts):
            stamp = pd.Timestamp(ts)
            if stamp.tzinfo is None:
                stamp = stamp.tz_localize(IST)
            else:
                stamp = stamp.tz_convert(IST)
            return stamp.strftime("%Y-%m-%d")
    return _today_ist_str()


def _candidate_context_for_day(day: str) -> Dict[str, dict]:
    path = Path(CANDIDATE_CSV_DIR) / f"candidate_tickers_{day}.csv"
    try:
        mtime = path.stat().st_mtime if path.exists() else -1.0
    except OSError:
        mtime = -1.0
    with _candidate_context_cache_lock:
        cached = _candidate_context_cache.get(day)
        if cached is not None and cached[0] == mtime:
            return cached[1]

    mapping: Dict[str, dict] = {}
    if mtime >= 0:
        try:
            df = pd.read_csv(path, low_memory=False)
        except Exception as exc:
            log.warning(f"[RESEARCH.GATE] candidate context read failed: {path} | {exc}")
            df = pd.DataFrame()
        if not df.empty:
            for _, row in df.iterrows():
                data = row.to_dict()
                key = _signal_context_key(data)
                if key.strip("|"):
                    mapping[key] = data

    with _candidate_context_cache_lock:
        _candidate_context_cache[day] = (mtime, mapping)
    return mapping


def _enriched_signal_context(signal: dict) -> dict:
    out = dict(signal)
    if not RESEARCH_PAPER_GATES_ENABLED:
        return out
    lookup = _candidate_context_for_day(_signal_day(signal))
    candidate = lookup.get(_signal_context_key(signal))
    if not candidate:
        return out
    for col in (
        "close_loc",
        "vwap_dist_atr",
        "ranker_score",
        "ranker_model",
        "vol_ratio",
        "rs_pct",
        "market_ret_pct",
        "regime",
        "research_shadow_status",
        "research_shadow_reason",
    ):
        if col in candidate and str(out.get(col, "")).strip() == "":
            out[col] = candidate.get(col)
    return out


def _research_paper_gate(signal: dict) -> Tuple[bool, str, str]:
    if not RESEARCH_PAPER_GATES_ENABLED:
        return False, "", ""
    ctx = _enriched_signal_context(signal)
    side = str(ctx.get("side", "")).strip().upper()
    setup = str(ctx.get("setup", "")).strip().upper()
    ticker = str(ctx.get("ticker", "")).strip().upper()
    close_loc = _safe_float(ctx.get("close_loc", ""), float("nan"))
    vwap_dist_atr = _safe_float(ctx.get("vwap_dist_atr", ""), float("nan"))
    ranker_score = _safe_float(ctx.get("ranker_score", ""), float("nan"))

    # P0.2: hard block list — these setups never paper-trade regardless of other gates
    if setup in PAPER_BLOCKED_SETUPS:
        return (
            True,
            "ENTRY_SKIPPED_RESEARCH_SETUP_BLOCKED",
            (
                f"[RESEARCH.GATE] Skipping {ticker} {side} {setup}: setup is in PAPER_BLOCKED_SETUPS "
                f"| version={RESEARCH_PAPER_GATE_VERSION}"
            ),
        )

    if (
        side == "LONG"
        and close_loc > ANTI_CHASE_LONG_CLOSE_LOC_MIN
        and vwap_dist_atr > ANTI_CHASE_LONG_VWAP_DIST_ATR_MIN
    ):
        return (
            True,
            "ENTRY_SKIPPED_RESEARCH_ANTI_CHASE_LONG",
            (
                f"[RESEARCH.GATE] Skipping {ticker} LONG {setup}: anti-chase paper gate "
                f"close_loc={close_loc:.3f} > {ANTI_CHASE_LONG_CLOSE_LOC_MIN:.3f} and "
                f"vwap_dist_atr={vwap_dist_atr:.3f} > {ANTI_CHASE_LONG_VWAP_DIST_ATR_MIN:.3f} "
                f"| version={RESEARCH_PAPER_GATE_VERSION}"
            ),
        )

    if (
        side == "LONG"
        and setup == "B_AVWAP_RECLAIM_REVERSAL"
        and np.isfinite(ranker_score)
        and ranker_score < B_AVWAP_RECLAIM_MIN_RANKER_SCORE
    ):
        return (
            True,
            "ENTRY_SKIPPED_RESEARCH_B_AVWAP_RANKER_GATE",
            (
                f"[RESEARCH.GATE] Skipping {ticker} LONG {setup}: paper ranker gate "
                f"ranker_score={ranker_score:.3f} < {B_AVWAP_RECLAIM_MIN_RANKER_SCORE:.3f} "
                f"| version={RESEARCH_PAPER_GATE_VERSION}"
            ),
        )

    return False, "", ""


def _mtm_brake_cfg() -> "rb.BrakeConfig":
    return rb.BrakeConfig.from_env(
        daily_default=DAILY_LOSS_BRAKE_RS if DAILY_LOSS_BRAKE_RS > 0 else 10000.0,
        per_trade_default=5000.0,
    )


def _mtm_brake_eval(signal: dict) -> Tuple[bool, str]:
    """rev-2 P0-18 MTM-aware brake. Returns (tripped, reason). In OBSERVE mode it
    only logs; the caller acts only when ACT mode is on. Fail-open on any error so a
    brake-eval bug can never block a paper trade."""
    if not _MTM_BRAKE_OBSERVE and not _MTM_BRAKE_ACT:
        return False, ""
    try:
        cfg = _mtm_brake_cfg()
        with daily_pnl_lock:
            realized = float(daily_pnl.get("total", 0.0))
        entries_today = _opened_entries_today_count()
        open_mtm = _unrealized_total_from_positions()
        setup = str(signal.get("setup", "")).strip().upper()
        with active_positions_lock:
            setup_open = sum(
                1 for p in active_positions.values()
                if str(p.get("setup", "")).strip().upper() == setup
            )
        allowed, reason = rb.entry_allowed(realized, open_mtm, entries_today, setup, setup_open, cfg)
        if not allowed:
            mode = "ACT" if _MTM_BRAKE_ACT else "OBSERVE"
            print(
                f"[RISK.BRAKE.MTM][{mode}] would block {signal.get('side')} "
                f"{signal.get('ticker')} {setup}: {reason} | "
                f"realized={_fmt_rs_signed(realized)} open_mtm={_fmt_rs_signed(open_mtm)} "
                f"day_total={_fmt_rs_signed(realized + open_mtm)}",
                flush=True,
            )
            return True, reason
        return False, ""
    except Exception:
        return False, ""


def _daily_loss_brake_gate(signal: dict) -> Tuple[bool, str, str]:
    # MTM-aware brake first (realized + open MTM, throttle, per-setup caps).
    # OBSERVE logs only; ACT actually blocks. Independent of the realized-only
    # brake below, which stays as the conservative fallback.
    mtm_tripped, mtm_reason = _mtm_brake_eval(signal)
    if mtm_tripped and _MTM_BRAKE_ACT:
        return True, "ENTRY_SKIPPED_MTM_BRAKE", f"[RISK.BRAKE.MTM] {mtm_reason}"

    if not DAILY_LOSS_BRAKE_ENABLED or DAILY_LOSS_BRAKE_RS <= 0:
        return False, "", ""
    with daily_pnl_lock:
        day_total = float(daily_pnl.get("total", 0.0))
    if day_total > -DAILY_LOSS_BRAKE_RS:
        return False, "", ""

    ticker = str(signal.get("ticker", "")).strip().upper()
    side = str(signal.get("side", "")).strip().upper()
    setup = str(signal.get("setup", "")).strip().upper()
    return (
        True,
        "ENTRY_SKIPPED_DAILY_LOSS_BRAKE",
        (
            f"[RISK.BRAKE] Skipping new entry {side} {ticker} {setup}: "
            f"daily paper PnL {_fmt_rs_signed(day_total)} <= "
            f"-{_fmt_rs(DAILY_LOSS_BRAKE_RS)} | version={RESEARCH_PAPER_GATE_VERSION}"
        ),
    )


def _c_or_session_cap_enabled_for_signal(signal: dict) -> bool:
    setup = str(signal.get("setup", "")).strip().upper()
    return bool(C_OR_BREAKOUT_SESSION_CAP_ENABLED and C_OR_BREAKOUT_SESSION_CAP > 0 and setup == "C_OR_BREAKOUT")


def _c_or_session_cap_load(today_str: str) -> dict:
    try:
        if os.path.exists(C_OR_BREAKOUT_SESSION_CAP_COUNTER_FILE):
            with open(C_OR_BREAKOUT_SESSION_CAP_COUNTER_FILE, "r", encoding="utf-8") as f:
                payload = json.load(f)
            if isinstance(payload, dict) and str(payload.get("date", "")) == today_str:
                raw_ids = payload.get("signal_ids", [])
                if isinstance(raw_ids, list):
                    signal_ids = [str(x).strip() for x in raw_ids if str(x).strip()]
                else:
                    signal_ids = []
                return {"date": today_str, "signal_ids": sorted(set(signal_ids))}
    except Exception as exc:
        log.warning(f"[C_OR.CAP] Failed to load session cap counter: {exc}")
    return {"date": today_str, "signal_ids": []}


def _c_or_session_cap_persist() -> None:
    try:
        tmp = C_OR_BREAKOUT_SESSION_CAP_COUNTER_FILE + ".tmp"
        payload = {
            "date": str(_c_or_session_cap_counter.get("date", "")),
            "signal_ids": sorted(set(str(x) for x in _c_or_session_cap_counter.get("signal_ids", []) if str(x))),
            "count": len(set(str(x) for x in _c_or_session_cap_counter.get("signal_ids", []) if str(x))),
            "cap": int(C_OR_BREAKOUT_SESSION_CAP),
            "updated_at_ist": datetime.now(IST).isoformat(),
        }
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, sort_keys=True)
        os.replace(tmp, C_OR_BREAKOUT_SESSION_CAP_COUNTER_FILE)
    except Exception as exc:
        log.warning(f"[C_OR.CAP] Failed to persist session cap counter: {exc}")


def _c_or_session_cap_prepare(today_str: Optional[str] = None) -> None:
    today = today_str or _today_ist_str()
    if _c_or_session_cap_counter.get("date") == today:
        return
    fresh = _c_or_session_cap_load(today)
    _c_or_session_cap_counter.clear()
    _c_or_session_cap_counter.update(fresh)


def _c_or_session_cap_count() -> int:
    return len(set(str(x) for x in _c_or_session_cap_counter.get("signal_ids", []) if str(x)))


def _c_or_breakout_session_cap_gate(signal: dict) -> Tuple[bool, str, str]:
    if not _c_or_session_cap_enabled_for_signal(signal):
        return False, "", ""
    today = _today_ist_str()
    with _c_or_session_cap_lock:
        _c_or_session_cap_prepare(today)
        count = _c_or_session_cap_count()
    if count < C_OR_BREAKOUT_SESSION_CAP:
        return False, "", ""

    ticker = str(signal.get("ticker", "")).strip().upper()
    side = str(signal.get("side", "")).strip().upper()
    return (
        True,
        "ENTRY_SKIPPED_C_OR_SATURATION_CAP",
        (
            f"[C_OR.CAP] Skipping new entry {side} {ticker} C_OR_BREAKOUT: "
            f"session accepted-entry cap reached ({count}/{C_OR_BREAKOUT_SESSION_CAP}) | "
            f"version={RESEARCH_PAPER_GATE_VERSION}"
        ),
    )


def _c_or_session_cap_increment(signal_id: str, signal: dict) -> Tuple[bool, str]:
    if not signal_id or not _c_or_session_cap_enabled_for_signal(signal):
        return True, "not_applicable"
    today = _today_ist_str()
    with _c_or_session_cap_lock:
        _c_or_session_cap_prepare(today)
        ids = set(str(x) for x in _c_or_session_cap_counter.get("signal_ids", []) if str(x))
        if signal_id in ids:
            return True, f"already_counted {len(ids)}/{C_OR_BREAKOUT_SESSION_CAP}"
        if len(ids) >= C_OR_BREAKOUT_SESSION_CAP:
            return False, f"C_OR_BREAKOUT session cap reached ({len(ids)}/{C_OR_BREAKOUT_SESSION_CAP})"
        ids.add(signal_id)
        _c_or_session_cap_counter["signal_ids"] = sorted(ids)
        _c_or_session_cap_persist()
        return True, f"counted {len(ids)}/{C_OR_BREAKOUT_SESSION_CAP}"


def _c_or_session_cap_decrement(signal_id: str) -> None:
    if not signal_id or not C_OR_BREAKOUT_SESSION_CAP_ENABLED:
        return
    today = _today_ist_str()
    with _c_or_session_cap_lock:
        _c_or_session_cap_prepare(today)
        ids = set(str(x) for x in _c_or_session_cap_counter.get("signal_ids", []) if str(x))
        if signal_id not in ids:
            return
        ids.discard(signal_id)
        _c_or_session_cap_counter["signal_ids"] = sorted(ids)
        _c_or_session_cap_persist()


def _record_pre_entry_skip(
    signal: dict,
    outcome_name: str,
    warning_message: str,
    use_ltp: bool,
    trade_start_ist: Optional[datetime] = None,
    release_capacity: bool = False,
    clear_active_trade: bool = False,
) -> bool:
    ticker = str(signal.get("ticker", "")).strip().upper()
    side = str(signal.get("side", "")).strip().upper()
    signal_id = str(signal.get("signal_id", "")).strip()
    if not ticker or side not in {"LONG", "SHORT"} or not signal_id:
        log.error(
            f"[SIM] invalid skip payload; skipping | signal_id={signal_id} "
            f"| ticker={ticker} | side={side} | outcome={outcome_name}"
        )
        if release_capacity:
            _release_capacity(signal_id)
        return False

    signal_entry_price = _safe_float(signal.get("entry_price", 0.0), 0.0)
    stop_price = _safe_float(signal.get("stop_price", 0.0), 0.0)
    target_price = _safe_float(signal.get("target_price", 0.0), 0.0)
    quantity = _safe_int(signal.get("quantity", 1), 1)
    setup = str(signal.get("setup", ""))
    impulse = str(signal.get("impulse_type", ""))
    stop_price, target_price, _ = _candE4_per_setup_sl_tgt(
        side,
        setup,
        signal_entry_price,
        stop_price,
        target_price,
    )

    entry_time_ist = trade_start_ist or datetime.now(IST)
    exit_time_ist = datetime.now(IST)
    trade_id_raw = str(signal.get("trade_id", "")).strip()
    trade_id = trade_id_raw or f"PT-{signal_id[:8]}-{entry_time_ist.strftime('%H%M%S')}"

    trade = PaperTrade(
        trade_id=trade_id,
        signal_id=signal_id,
        signal_datetime=str(signal.get("signal_datetime", "")),
        signal_entry_datetime_ist=str(signal.get("signal_entry_datetime_ist", "")),
        entry_time=entry_time_ist.strftime("%Y-%m-%d %H:%M:%S%z"),
        exit_time=exit_time_ist.strftime("%Y-%m-%d %H:%M:%S%z"),
        ticker=ticker,
        side=side,
        setup=setup,
        impulse_type=impulse,
        quantity=quantity,
        entry_price=round(signal_entry_price, 2),
        exit_price=round(signal_entry_price, 2),
        stop_price=round(stop_price, 2),
        target_price=round(target_price, 2),
        outcome=outcome_name,
        pnl_rs=0.0,
        pnl_pct=0.0,
        quality_score=_safe_float(signal.get("quality_score", 0), 0.0),
        p_win=_safe_float(signal.get("p_win", 0.0), 0.0),
        confidence_multiplier=_safe_float(signal.get("confidence_multiplier", 1.0), 1.0),
    )
    _log_trade(trade)

    with daily_pnl_lock:
        daily_pnl["total"] += 0.0
        daily_pnl["trades"] += 1
        day_total = float(daily_pnl["total"])
        day_wins = int(daily_pnl["wins"])
        day_losses = int(daily_pnl["losses"])
        _save_summary()

    log.warning(warning_message)
    log.info(
        f"[SIM] RESULT {side} {ticker} | {outcome_name} | "
        f"P&L: Rs.+0.00 (+0.00%) | Day total: Rs.{day_total:+,.2f} "
        f"({day_wins}W/{day_losses}L)"
    )

    if release_capacity:
        _release_capacity(signal_id)
    with active_positions_lock:
        active_positions.pop(signal_id, None)
    if clear_active_trade:
        with active_trades_lock:
            active_trades.pop(signal_id, None)
    _persist_open_trades_state()
    _log_live_pnl_snapshot(use_ltp, source=f"skip:{ticker}")
    return True


def _today_ist_str() -> str:
    return datetime.now(IST).strftime("%Y-%m-%d")


def _open_trades_state_path(today_str: Optional[str] = None) -> str:
    d = today_str or _today_ist_str()
    return os.path.join(SIGNAL_DIR, OPEN_TRADES_STATE_PATTERN.format(d))


def _atomic_write_json(path: str, payload: object) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp_path = f"{path}.{os.getpid()}.{threading.get_ident()}.tmp"
    last_err: Optional[Exception] = None
    for attempt in range(5):
        try:
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, path)
            return
        except PermissionError as e:
            last_err = e
            # Windows can briefly deny replace/create under concurrent writers.
            time.sleep(0.05 * (attempt + 1))
        finally:
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except Exception:
                pass
    if last_err is not None:
        raise last_err


def _persist_open_trades_state() -> None:
    with state_file_lock:
        with active_positions_lock:
            rows = [dict(v) for _, v in sorted(active_positions.items(), key=lambda kv: kv[0])]
        payload = {
            "date": _today_ist_str(),
            "open_trades": rows,
            "updated_at": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S"),
        }
        _atomic_write_json(_open_trades_state_path(), payload)


def _load_open_trades_state(today_str: Optional[str] = None) -> List[dict]:
    path = _open_trades_state_path(today_str=today_str)
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if str(data.get("date", "")) != (today_str or _today_ist_str()):
            return []
        rows = data.get("open_trades", [])
        if not isinstance(rows, list):
            return []
        out: List[dict] = []
        for row in rows:
            if isinstance(row, dict):
                out.append(dict(row))
        return out
    except Exception:
        return []


def _load_kill_switch_command_cached() -> Optional[dict]:
    global kill_switch_cache_mtime, kill_switch_cache_payload
    path = Path(KILL_SWITCH_COMMAND_FILE)
    try:
        mtime = path.stat().st_mtime if path.exists() else -1.0
    except OSError:
        mtime = -1.0

    with kill_switch_cache_lock:
        if mtime == kill_switch_cache_mtime:
            return dict(kill_switch_cache_payload) if isinstance(kill_switch_cache_payload, dict) else None

    payload: Optional[dict] = None
    if mtime >= 0:
        try:
            with path.open("r", encoding="utf-8") as f:
                raw = json.load(f)
            if isinstance(raw, dict):
                today = _today_ist_str()
                if str(raw.get("date", today)).strip() == today:
                    payload = dict(raw)
        except Exception:
            payload = None

    with kill_switch_cache_lock:
        kill_switch_cache_mtime = mtime
        kill_switch_cache_payload = dict(payload) if isinstance(payload, dict) else None
        return dict(kill_switch_cache_payload) if isinstance(kill_switch_cache_payload, dict) else None


def _get_kill_switch_for_trade(signal_id: str, ticker: str) -> Optional[dict]:
    cmd = _load_kill_switch_command_cached()
    if not cmd:
        return None

    target_ids_raw = cmd.get("target_signal_ids", [])
    target_ids = {
        str(x).strip()
        for x in target_ids_raw
        if str(x).strip()
    } if isinstance(target_ids_raw, list) else set()
    if target_ids:
        if signal_id in target_ids:
            return cmd
        return None

    mode = str(cmd.get("mode", "")).strip().lower()
    if mode == "all":
        return cmd
    if mode == "ticker":
        target_ticker = str(cmd.get("ticker", "")).strip().upper()
        if target_ticker and target_ticker == str(ticker).strip().upper():
            return cmd
    return None


def _release_capacity(signal_id: str) -> None:
    with capital_lock:
        capital_deployed.pop(signal_id, None)


def _unrealized_total_from_positions() -> float:
    with active_positions_lock:
        positions = [dict(p) for p in active_positions.values()]
    total = 0.0
    for pos in positions:
        side = str(pos.get("side", "LONG")).upper()
        qty = _safe_int(pos.get("quantity", 0), 0)
        entry_price = _safe_float(pos.get("entry_price", 0.0), 0.0)
        mark_price = _safe_float(pos.get("last_ltp", entry_price), entry_price)
        costed = _calc_costed_pnl(side, entry_price, mark_price, qty)
        total += float(costed["net_pnl_rs"])
    return float(total)


def _capital_snapshot() -> Tuple[int, float]:
    with capital_lock:
        return len(capital_deployed), float(sum(capital_deployed.values()))


def _daily_snapshot() -> Dict[str, float]:
    with daily_pnl_lock:
        return dict(daily_pnl)


def _opened_entries_today_count() -> int:
    with opened_entry_ids_lock:
        return len(opened_entry_ids)


def _mark_entry_opened(signal_id: str) -> None:
    sid = str(signal_id or "").strip()
    if not sid:
        return
    with opened_entry_ids_lock:
        opened_entry_ids.add(sid)


def _replace_opened_entry_ids(signal_ids: Set[str]) -> None:
    with opened_entry_ids_lock:
        opened_entry_ids.clear()
        opened_entry_ids.update(str(s).strip() for s in signal_ids if str(s).strip())


def _live_pnl_line(use_ltp: bool) -> str:
    """
    Build one-line live PnL snapshot:
    - per ticker unrealized PnL for open positions
    - unrealized, realized and combined totals
    """
    with active_positions_lock:
        positions = [dict(p) for p in active_positions.values()]

    if not positions:
        daily = _daily_snapshot()
        _, deployed_margin = _capital_snapshot()
        day_total = float(daily.get("total", 0.0))
        return (
            "[LIVE.PNL] open=0 | tickers=none | "
            f"unrealized={_fmt_rs_signed(0.0)} | "
            f"realized={_fmt_rs_signed(day_total)} | "
            f"total={_fmt_rs_signed(day_total)} | "
            f"deployed_margin={_fmt_rs(deployed_margin)}"
        )

    ticker_unrealized: Dict[str, float] = {}
    ltp_na = set()

    for pos in positions:
        ticker = str(pos.get("ticker", ""))
        side = str(pos.get("side", "LONG")).upper()
        qty = _safe_int(pos.get("quantity", 0), 0)
        entry_price = _safe_float(pos.get("entry_price", 0), 0.0)
        ltp = _safe_float(pos.get("last_ltp", 0.0), 0.0)
        if ltp <= 0:
            ltp = entry_price
            if ticker:
                ltp_na.add(ticker)

        costed = _calc_costed_pnl(side, entry_price, ltp, qty)
        ticker_unrealized[ticker] = ticker_unrealized.get(ticker, 0.0) + float(costed["net_pnl_rs"])

    unrealized_total = float(sum(ticker_unrealized.values()))
    daily = _daily_snapshot()
    realized_total = float(daily.get("total", 0.0))
    combined_total = realized_total + unrealized_total
    _, deployed_margin = _capital_snapshot()

    ticker_parts = [
        f"{ticker}={_fmt_rs_signed(pnl)}"
        for ticker, pnl in sorted(ticker_unrealized.items())
        if ticker
    ]
    ticker_text = ", ".join(ticker_parts) if ticker_parts else "none"

    line = (
        f"[LIVE.PNL] open={len(positions)} | tickers={ticker_text} | "
        f"unrealized={_fmt_rs_signed(unrealized_total)} | "
        f"realized={_fmt_rs_signed(realized_total)} | "
        f"total={_fmt_rs_signed(combined_total)} | "
        f"deployed_margin={_fmt_rs(deployed_margin)}"
    )
    if ltp_na:
        line += f" | ltp_na={','.join(sorted(ltp_na))}"

    return line


def _log_live_pnl_snapshot(use_ltp: bool, source: str = "") -> None:
    prefix = f"{source} | " if source else ""
    log.info(f"{prefix}{_live_pnl_line(use_ltp)}")


def _check_risk_limits(signal: dict) -> Optional[str]:
    """
    Check open positions and capital deployed.
    Returns a rejection reason string, or None if the trade is allowed.
    """
    if not RISK_LIMITS_ENABLED:
        return None

    entry_price = _safe_float(signal.get("entry_price", 0.0), 0.0)
    quantity = _safe_int(signal.get("quantity", 1), 1)
    if entry_price <= 0:
        return "invalid entry_price"
    if quantity <= 0:
        return "invalid quantity"

    margin = (entry_price * quantity) / INTRADAY_LEVERAGE

    with capital_lock:
        open_count = len(capital_deployed)
        total_deployed = sum(capital_deployed.values())

    if open_count >= MAX_OPEN_POSITIONS:
        return f"max open positions reached ({open_count}/{MAX_OPEN_POSITIONS})"

    if (total_deployed + margin) > MAX_CAPITAL_DEPLOYED_RS:
        return (
            f"margin limit exceeded (deployed Rs.{total_deployed:,.0f} + "
            f"Rs.{margin:,.0f} > Rs.{MAX_CAPITAL_DEPLOYED_RS:,})"
        )

    return None


def _reserve_capacity_for_signal(signal_id: str, signal: dict) -> Tuple[bool, str, float]:
    """
    Atomically check and reserve margin/slot so concurrent dispatches cannot
    breach open-position or deployed-margin limits.
    Returns (ok, reason, reserved_margin).
    """
    # CAND-E4 LIVE GATE (2026-05-04): skip rows where Detection Engine wrote
    # size_multiplier == 0 (Cand-E4 dropped setups: SHORT C_OR_BREAKDOWN +
    # SHORT D_EMA20_REJECTION). Default to 1.0 if column missing.
    size_mult_raw = signal.get("size_multiplier", 1.0)
    try:
        size_mult = float(size_mult_raw) if size_mult_raw not in (None, "") else 1.0
    except (TypeError, ValueError):
        size_mult = 1.0
    if size_mult <= 0.0:
        setup = str(signal.get("setup", "?"))
        side = str(signal.get("side", "?"))
        return False, f"candE4_size_multiplier=0 (setup={side} {setup})", 0.0

    # CAND-E4 G1: time-window governor (stateless).
    g1_side = str(signal.get("side", "")).strip().upper()
    g1_ok, g1_reason = _candE4_g1_time_window_check(g1_side)
    if not g1_ok:
        return False, g1_reason, 0.0

    entry_price = _safe_float(signal.get("entry_price", 0.0), 0.0)
    quantity = _safe_int(signal.get("quantity", 1), 1)
    if entry_price <= 0:
        return False, "invalid entry_price", 0.0
    if quantity <= 0:
        return False, "invalid quantity", 0.0

    margin = float((entry_price * quantity) / INTRADAY_LEVERAGE)

    with capital_lock:
        if signal_id in capital_deployed:
            return False, "already_reserved_or_open", 0.0

        if RISK_LIMITS_ENABLED:
            open_count = len(capital_deployed)
            total_deployed = float(sum(capital_deployed.values()))

            if open_count >= MAX_OPEN_POSITIONS:
                return False, f"max open positions reached ({open_count}/{MAX_OPEN_POSITIONS})", 0.0

            if (total_deployed + margin) > MAX_CAPITAL_DEPLOYED_RS:
                return (
                    False,
                    (
                        f"margin limit exceeded (deployed Rs.{total_deployed:,.0f} + "
                        f"Rs.{margin:,.0f} > Rs.{MAX_CAPITAL_DEPLOYED_RS:,})"
                    ),
                    0.0,
                )

            # P1-7: gross short notional cap
            if g1_side == "SHORT" and MAX_GROSS_SHORT_NOTIONAL_RS > 0:
                with active_positions_lock:
                    gross_short = sum(
                        float(pos.get("entry_price", 0)) * int(pos.get("quantity", 0))
                        for pos in active_positions.values()
                        if str(pos.get("side", "")).upper() == "SHORT"
                    )
                new_notional = float(entry_price * quantity)
                if (gross_short + new_notional) > MAX_GROSS_SHORT_NOTIONAL_RS:
                    return (
                        False,
                        f"gross short cap exceeded (open Rs.{gross_short:,.0f} + "
                        f"Rs.{new_notional:,.0f} > Rs.{MAX_GROSS_SHORT_NOTIONAL_RS:,})",
                        0.0,
                    )

        # CAND-E4 G2: daily side cap. Increment only after capacity/margin
        # checks pass so rejected signals do not consume quota.
        g2_ok, g2_reason = _candE4_g2_check_and_increment(g1_side)
        if not g2_ok:
            return False, g2_reason, 0.0

        capital_deployed[signal_id] = margin

    return True, "reserved", margin


def _apply_exit_slippage(price: float, side: str, outcome: str) -> float:
    """Worsen exit fill price for SL / unscheduled closes by EXIT_SLIPPAGE_BPS.

    SL fill for LONG: price slips down → lower fill.
    SL fill for SHORT: price slips up → higher fill.
    TARGET fills are limit orders — no slippage assumed.
    """
    if EXIT_SLIPPAGE_BPS <= 0 or outcome == "TARGET":
        return price
    slip = price * EXIT_SLIPPAGE_BPS / 10_000.0
    return round(price - slip if side == "LONG" else price + slip, 2)


def _is_market_open_now(now_ist: Optional[datetime] = None) -> bool:
    now = now_ist or datetime.now(IST)
    return MARKET_OPEN <= now.time() <= MARKET_CLOSE


def simulate_trade(
    signal: dict,
    use_ltp: bool = True,
    entry_price_source: str = "signal_bar",
    pre_reserved_margin: Optional[float] = None,
    resume_mode: bool = False,
) -> bool:
    """
    Simulate a single trade in a background thread.
    In resume_mode, the trade continues monitoring from persisted open state.
    Returns True when a trade lifecycle was completed and logged.
    """
    ticker = str(signal.get("ticker", "")).strip().upper()
    side = str(signal.get("side", "")).strip().upper()
    signal_id = str(signal.get("signal_id", "")).strip()
    if not ticker or side not in {"LONG", "SHORT"} or not signal_id:
        log.error(f"[SIM] invalid signal payload; skipping | signal_id={signal_id} | ticker={ticker} | side={side}")
        if pre_reserved_margin is not None:
            _release_capacity(signal_id)
            _c_or_session_cap_decrement(signal_id)
        return False

    signal_entry_price = _safe_float(signal.get("entry_price", 0.0), 0.0)
    stop_price = _safe_float(signal.get("stop_price", 0.0), 0.0)
    target_price = _safe_float(signal.get("target_price", 0.0), 0.0)
    quantity = _safe_int(signal.get("quantity", 1), 1)

    # CAND-E4 per-setup SL/TGT override (variable names differ from FALSE
    # executor: stop_price/target_price instead of signal_stop/signal_target)
    _setup_for_candE4 = str(signal.get("setup", "")).strip().upper()
    _csv_stop_pre, _csv_target_pre = stop_price, target_price
    stop_price, target_price, _candE4_applied = _candE4_per_setup_sl_tgt(
        side, _setup_for_candE4, signal_entry_price, stop_price, target_price,
    )
    if _candE4_applied:
        log.info(
            f"[CAND-E4_SLTGT] {ticker} {side} {_setup_for_candE4} "
            f"entry={signal_entry_price:.2f} "
            f"SL {_csv_stop_pre:.2f}->{stop_price:.2f} "
            f"TGT {_csv_target_pre:.2f}->{target_price:.2f}"
        )
    if signal_entry_price <= 0 or stop_price <= 0 or target_price <= 0 or quantity <= 0:
        log.error(
            f"[SIM] invalid numeric inputs; skipping | signal_id={signal_id[:12]} | "
            f"entry={signal_entry_price} sl={stop_price} tgt={target_price} qty={quantity}"
        )
        if pre_reserved_margin is not None:
            _release_capacity(signal_id)
            _c_or_session_cap_decrement(signal_id)
        return False

    signal_time = str(signal.get("signal_entry_datetime_ist") or signal.get("signal_datetime") or "")
    received_time = str(signal.get("received_time", ""))
    setup = str(signal.get("setup", ""))
    impulse = str(signal.get("impulse_type", ""))

    entry_time_ist = datetime.now(IST)
    entry_time_raw = str(signal.get("entry_time", "")).strip()
    if resume_mode and entry_time_raw:
        try:
            parsed = pd.to_datetime(entry_time_raw, errors="coerce")
            if not pd.isna(parsed):
                parsed_ts = pd.Timestamp(parsed)
                if parsed_ts.tzinfo is None:
                    parsed_ts = parsed_ts.tz_localize(IST)
                else:
                    parsed_ts = parsed_ts.tz_convert(IST)
                entry_time_ist = parsed_ts.to_pydatetime()
        except Exception:
            pass

    trade_id_raw = str(signal.get("trade_id", "")).strip()
    trade_id = trade_id_raw or f"PT-{signal_id[:8]}-{entry_time_ist.strftime('%H%M%S')}"
    today = datetime.now(IST).date()
    forced_close_dt = IST.localize(datetime.combine(today, FORCED_CLOSE_TIME))
    configured_forced_exit = str(signal.get("forced_exit_time", "")).strip()
    if configured_forced_exit:
        try:
            hh, mm = configured_forced_exit.split(":", 1)
            configured_close_dt = IST.localize(
                datetime.combine(today, dt_time(int(hh), int(mm)))
            )
            forced_close_dt = min(forced_close_dt, configured_close_dt)
        except Exception:
            log.warning(
                f"[EXIT.POLICY] invalid forced_exit_time={configured_forced_exit!r}; "
                f"using {FORCED_CLOSE_TIME} | signal_id={signal_id[:12]}"
            )
    trade_start_ist = entry_time_ist if not resume_mode else datetime.now(IST)
    entry_retry_deadline = _entry_retry_deadline(signal, trade_start_ist, forced_close_dt)

    def _finalize_pre_entry_skip(outcome_name: str, warning_message: str) -> bool:
        recorded = _record_pre_entry_skip(
            signal=signal,
            outcome_name=outcome_name,
            warning_message=warning_message,
            use_ltp=use_ltp,
            trade_start_ist=trade_start_ist,
            release_capacity=True,
            clear_active_trade=True,
        )
        _c_or_session_cap_decrement(signal_id)
        return recorded

    if not resume_mode:
        try:
            entry_slot_ts = _parse_ist_signal_ts(
                signal.get("signal_entry_datetime_ist")
                or signal.get("signal_bar_time_ist")
                or signal.get("bar_time_ist")
            )
            if entry_slot_ts is not None:
                entry_slot_dt = entry_slot_ts.to_pydatetime()
                now_ist = datetime.now(IST)
                wait_sec = (entry_slot_dt - now_ist).total_seconds()
                if wait_sec > ENTRY_SLOT_MAX_WAIT_SEC > 0:
                    return _finalize_pre_entry_skip(
                        "ENTRY_SKIPPED_SLOT_TOO_FAR",
                        (
                            f"[ENTRY.SLOT] Skipping {ticker} {side}: entry slot "
                            f"{entry_slot_dt.strftime('%H:%M:%S')} is {wait_sec:.0f}s away "
                            f"(> cap {ENTRY_SLOT_MAX_WAIT_SEC}s)."
                        ),
                    )
                if wait_sec > 0:
                    log.info(
                        f"[ENTRY.SLOT] {ticker} signal_id={signal_id[:12]}: "
                        f"sleeping {wait_sec:.1f}s until bar-open "
                        f"{entry_slot_dt.strftime('%H:%M:%S')}"
                    )
                    time.sleep(wait_sec)
                    entry_time_ist = datetime.now(IST)
                    trade_start_ist = entry_time_ist
                    if not trade_id_raw:
                        trade_id = f"PT-{signal_id[:8]}-{entry_time_ist.strftime('%H%M%S')}"
                    entry_retry_deadline = _entry_retry_deadline(
                        signal,
                        trade_start_ist,
                        forced_close_dt,
                    )
        except Exception as slot_err:
            log.warning(
                f"[ENTRY.SLOT] {ticker} signal_id={signal_id[:12]}: "
                f"slot-gate error: {slot_err}"
            )

    if not resume_mode and _trade_started_after_entry_deadline(trade_start_ist, entry_retry_deadline):
        return _finalize_pre_entry_skip(
            "ENTRY_SKIPPED_STALE_SIGNAL",
            (
                f"[STALE] Skipping {ticker} {side}: signal surfaced at "
                f"{trade_start_ist.strftime('%H:%M:%S')} after freshness deadline "
                f"{entry_retry_deadline.strftime('%H:%M:%S')}."
            ),
        )

    # Universal entry-slot freshness guard with a late_skipped CSV trail.
    if (not resume_mode) and LATE_DETECTION_GUARD_ENABLE and LATE_DETECTION_MAX_LAG_SEC > 0:
        _lag_sec = _detection_lag_seconds(signal)
        _setup_name = str(signal.get("setup", "")).upper().strip()
        _threshold = _late_lag_threshold_for_setup(_setup_name)
        if _lag_sec is None:
            _append_late_skipped_csv(signal, None, _threshold)
            return _finalize_pre_entry_skip(
                "ENTRY_SKIPPED_MALFORMED_TIMING",
                (
                    f"[MALFORMED] Skipping {ticker} {side} {_setup_name}: "
                    f"lag=None (missing or unparseable detected_time_ist) | "
                    f"signal_id={signal_id[:12]}"
                ),
            )
        if _lag_sec < 0:
            _append_late_skipped_csv(signal, _lag_sec, _threshold)
            return _finalize_pre_entry_skip(
                "ENTRY_SKIPPED_NEGATIVE_LAG",
                (
                    f"[NEGATIVE_LAG] Skipping {ticker} {side} {_setup_name}: "
                    f"lag={_lag_sec:.1f}s (signal appears future-dated) | "
                    f"signal_id={signal_id[:12]}"
                ),
            )
        if _lag_sec > _threshold:
            _append_late_skipped_csv(signal, _lag_sec, _threshold)
            return _finalize_pre_entry_skip(
                "ENTRY_SKIPPED_STALE_DETECTION",
                (
                    f"[STALE.DETECT] Skipping {ticker} {side} {_setup_name}: detected "
                    f"{_lag_sec:.0f}s after entry slot "
                    f"(universal threshold {_threshold}s) | "
                    f"signal_id={signal_id[:12]} | total_late_skipped_today={_late_skipped_count}"
                ),
            )

    # Select raw entry reference price:
    # - signal_bar: use signal CSV entry_price (15m logic)
    # - ltp_on_signal: use current LTP at dispatch time; fallback to signal_bar if unavailable
    raw_entry = signal_entry_price
    entry_source_used = "restored" if resume_mode else "signal_bar"
    if not resume_mode and entry_price_source == "ltp_on_signal":
        ltp_dispatch = get_ltp(ticker) if use_ltp else None
        if ltp_dispatch is not None and ltp_dispatch > 0:
            raw_entry = float(ltp_dispatch)
            entry_source_used = "ltp_on_signal"
        else:
            entry_source_used = "signal_bar_fallback"
            log.warning(
                f"[ENTRY.FALLBACK] ticker={ticker} | side={side} | signal_id={signal_id[:12]} | "
                f"reason=ltp_unavailable | fallback_entry={signal_entry_price:.2f}"
            )

    configured_trigger = _safe_float(signal.get("entry_trigger_price", 0.0), 0.0)
    configured_cancel = _safe_float(signal.get("entry_cancel_price", 0.0), 0.0)
    configured_valid_minutes = max(
        0, int(_safe_float(signal.get("entry_valid_minutes", 0), 0))
    )
    configured_gap_pct = max(
        0.0, _safe_float(signal.get("entry_max_gap_pct", 0.0), 0.0)
    )
    configured_breakout_entry = (
        not resume_mode
        and str(signal.get("entry_policy_model", "")).strip().lower()
        == "high_break_trigger"
        and configured_trigger > 0
        and configured_cancel > 0
        and configured_valid_minutes > 0
    )
    if configured_breakout_entry and use_ltp:
        signal_bar_ts = _parse_ist_signal_ts(
            signal.get("signal_time_ist")
            or signal.get("signal_bar_time_ist")
            or signal.get("bar_time_ist")
        )
        if signal_bar_ts is None:
            return _finalize_pre_entry_skip(
                "ENTRY_SKIPPED_MALFORMED_TRIGGER_WINDOW",
                f"[ENTRY.TRIGGER] Missing signal timestamp for {ticker} | signal_id={signal_id[:12]}",
            )
        trigger_deadline = min(
            signal_bar_ts.to_pydatetime()
            + timedelta(minutes=configured_valid_minutes + 1),
            forced_close_dt,
        )
        last_trigger_ltp: Optional[float] = None
        log.info(
            f"[ENTRY.TRIGGER] Armed {ticker} LONG trigger={configured_trigger:.2f} "
            f"cancel={configured_cancel:.2f} until={trigger_deadline.strftime('%H:%M:%S')} "
            f"| signal_id={signal_id[:12]}"
        )
        while datetime.now(IST) < trigger_deadline:
            trigger_ltp = get_ltp(ticker)
            if trigger_ltp is None or trigger_ltp <= 0:
                time.sleep(POLL_INTERVAL_SEC)
                continue
            last_trigger_ltp = float(trigger_ltp)
            # Cancel-first is the conservative live equivalent of the frozen
            # same-1m-bar ambiguity rule.
            if last_trigger_ltp <= configured_cancel:
                return _finalize_pre_entry_skip(
                    "ENTRY_CANCELLED_BEFORE_TRIGGER",
                    (
                        f"[ENTRY.TRIGGER] Cancelled {ticker}: ltp={last_trigger_ltp:.2f} "
                        f"<= cancel={configured_cancel:.2f} | signal_id={signal_id[:12]}"
                    ),
                )
            if last_trigger_ltp >= configured_trigger:
                gap_pct = (
                    (last_trigger_ltp / configured_trigger - 1.0) * 100.0
                )
                if configured_gap_pct > 0 and gap_pct > configured_gap_pct:
                    return _finalize_pre_entry_skip(
                        "ENTRY_SKIPPED_TRIGGER_GAP",
                        (
                            f"[ENTRY.TRIGGER] Skipped {ticker}: executable={last_trigger_ltp:.2f} "
                            f"is {gap_pct:.3f}% above trigger={configured_trigger:.2f} "
                            f"> {configured_gap_pct:.3f}% | signal_id={signal_id[:12]}"
                        ),
                    )
                raw_entry = last_trigger_ltp
                entry_source_used = "configured_high_break_trigger"
                entry_time_ist = datetime.now(IST)
                break
            time.sleep(POLL_INTERVAL_SEC)
        else:
            return _finalize_pre_entry_skip(
                "ENTRY_SKIPPED_TRIGGER_NOT_HIT",
                (
                    f"[ENTRY.TRIGGER] Not hit for {ticker} before "
                    f"{trigger_deadline.strftime('%H:%M:%S')} "
                    f"| last_ltp={last_trigger_ltp} | signal_id={signal_id[:12]}"
                ),
            )

    # --- Near-entry retry window + max entry slip gate ---
    if (not resume_mode) and not configured_breakout_entry and signal_entry_price > 0 and ENTRY_RETRY_NEAR_ENTRY_ENABLE and ENTRY_RETRY_WAIT_SEC > 0:
        if raw_entry > 0 and not _entry_price_within_retry_band(side, signal_entry_price, raw_entry):
            retry_until_ist = entry_retry_deadline
            log.info(
                f"[ENTRY.RETRY] Waiting for {ticker} {side} to return near entry "
                f"| signal_id={signal_id[:12]} | signal={signal_entry_price:.2f} "
                f"| ltp={raw_entry:.2f} | band={ENTRY_RETRY_NEAR_ENTRY_PCT*100:.2f}% "
                f"| until={retry_until_ist.strftime('%H:%M:%S')}"
            )
            waited_ltp = _wait_for_near_entry_price(
                ticker=ticker,
                side=side,
                signal_entry_price=signal_entry_price,
                retry_until_ist=retry_until_ist,
                use_ltp=use_ltp,
            )
            if waited_ltp is not None and waited_ltp > 0 and _entry_price_within_retry_band(side, signal_entry_price, float(waited_ltp)):
                raw_entry = float(waited_ltp)
                entry_source_used = "ltp_retry_near_signal"
                log.info(
                    f"[ENTRY.RETRY] Re-armed {ticker} {side} near entry "
                    f"| signal_id={signal_id[:12]} | signal={signal_entry_price:.2f} "
                    f"| ltp={raw_entry:.2f}"
                )
            else:
                last_ltp = float(waited_ltp) if waited_ltp else float(raw_entry)
                return _finalize_pre_entry_skip(
                    "ENTRY_SKIPPED_PRICE_NOT_NEAR",
                    (
                        f"[ENTRY.RETRY] Skipping {ticker} {side}: price did not return "
                        f"within {ENTRY_RETRY_NEAR_ENTRY_PCT*100:.2f}% of signal entry "
                        f"before freshness deadline {entry_retry_deadline.strftime('%H:%M:%S')} "
                        f"| signal_id={signal_id[:12]} | signal={signal_entry_price:.2f} "
                        f"| last_ltp={last_ltp:.2f}"
                    ),
                )

    # --- Max entry slip gate ---
    # Reject signals where the actual fill price has already deviated too far from
    # the model trigger.  For LONG: reject if raw_entry > signal_entry_price * (1 + cap).
    # For SHORT: reject if raw_entry < signal_entry_price * (1 - cap).
    # Only applied on fresh entries (not resumes) when a meaningful cap is set.
    if (not resume_mode) and signal_entry_price > 0:
        if side == "LONG" and LONG_MAX_ENTRY_SLIP_PCT > 0.0:
            slip = (raw_entry - signal_entry_price) / signal_entry_price
            if slip > LONG_MAX_ENTRY_SLIP_PCT:
                return _finalize_pre_entry_skip(
                    "ENTRY_SKIPPED_MAX_ENTRY_SLIP",
                    (
                        f"[SLIP.GATE] Skipping {ticker} LONG: live entry drifted beyond cap "
                        f"| signal_id={signal_id[:12]} | model_trigger={signal_entry_price:.2f} "
                        f"| raw_entry={raw_entry:.2f} | slip={slip*100:.2f}% "
                        f"> cap={LONG_MAX_ENTRY_SLIP_PCT*100:.2f}%"
                    ),
                )
        elif side == "SHORT" and SHORT_MAX_ENTRY_SLIP_PCT > 0.0:
            slip = (signal_entry_price - raw_entry) / signal_entry_price
            if slip > SHORT_MAX_ENTRY_SLIP_PCT:
                return _finalize_pre_entry_skip(
                    "ENTRY_SKIPPED_MAX_ENTRY_SLIP",
                    (
                        f"[SLIP.GATE] Skipping {ticker} SHORT: live entry drifted beyond cap "
                        f"| signal_id={signal_id[:12]} | model_trigger={signal_entry_price:.2f} "
                        f"| raw_entry={raw_entry:.2f} | slip={slip*100:.2f}% "
                        f"> cap={SHORT_MAX_ENTRY_SLIP_PCT*100:.2f}%"
                    ),
                )

    if resume_mode:
        entry_price = _safe_float(signal.get("entry_price_exec", signal.get("entry_price", raw_entry)), raw_entry)
    else:
        # Apply realistic slippage: worsen entry in the unfavourable direction
        if side == "LONG":
            entry_price = round(raw_entry * (1 + SLIPPAGE_PCT), 2)
        else:
            entry_price = round(raw_entry * (1 - SLIPPAGE_PCT), 2)

    # When entry is taken from live LTP, rebase SL/target to executed entry
    # so % distances remain consistent with signal design.
    if (
        (not resume_mode)
        and entry_source_used in {"ltp_on_signal", "configured_high_break_trigger"}
        and signal_entry_price > 0
    ):
        stop_mult = float(stop_price / signal_entry_price)
        target_mult = float(target_price / signal_entry_price)
        rebased_stop = round(entry_price * stop_mult, 2)
        rebased_target = round(entry_price * target_mult, 2)
        if rebased_stop > 0 and rebased_target > 0:
            old_stop = stop_price
            old_target = target_price
            stop_price = float(rebased_stop)
            target_price = float(rebased_target)
            log.info(
                f"[ENTRY.REBASE] ticker={ticker} | side={side} | signal_id={signal_id[:12]} | "
                f"src={entry_source_used} | signal_entry={signal_entry_price:.2f} | entry_exec={entry_price:.2f} | "
                f"sl:{old_stop:.2f}->{stop_price:.2f} | tgt:{old_target:.2f}->{target_price:.2f}"
            )

    invested = float(entry_price * quantity)
    margin = float(invested / INTRADAY_LEVERAGE)

    # Margin was pre-reserved in process_new_signals; reconcile to exact executed margin.
    with capital_lock:
        if pre_reserved_margin is None and signal_id not in capital_deployed:
            capital_deployed[signal_id] = margin
        else:
            capital_deployed[signal_id] = margin
        open_positions = len(capital_deployed)
        deployed_margin = float(sum(capital_deployed.values()))

    with active_positions_lock:
        active_positions[signal_id] = {
            "trade_id": trade_id,
            "signal_id": signal_id,
            "ticker": ticker,
            "side": side,
            "setup": str(signal.get("setup", "")).strip().upper(),  # P0-18: per-setup concurrency cap
            "quantity": quantity,
            "entry_price": float(entry_price),
            "stop_price": float(stop_price),
            "target_price": float(target_price),
            "entry_time": entry_time_ist.strftime("%Y-%m-%d %H:%M:%S%z"),
            "last_ltp": _safe_float(signal.get("last_ltp", 0.0), 0.0),
            "restored": bool(resume_mode),
            "entry_policy_model": str(signal.get("entry_policy_model", "")),
            "max_hold_minutes": signal.get("max_hold_minutes", ""),
            "forced_exit_time": str(signal.get("forced_exit_time", "")),
        }
    _mark_entry_opened(signal_id)
    _persist_open_trades_state()

    if resume_mode:
        log.info(
            f"[RESUME.OPEN] trade_id={trade_id} | signal_id={signal_id[:12]} | ticker={ticker} | side={side} | "
            f"entry={entry_price:.2f} | sl={stop_price:.2f} | tgt={target_price:.2f} | qty={quantity} | "
            f"margin={_fmt_rs(margin)} | open_positions={open_positions} | deployed_margin={_fmt_rs(deployed_margin)}"
        )
    else:
        log.info(
            f"[ENTRY.NEW] trade_id={trade_id} | signal_id={signal_id[:12]} | ticker={ticker} | side={side} | "
            f"signal_time={signal_time} | received_time={received_time} | setup={setup} | impulse={impulse} | "
            f"entry={entry_price:.2f} | sl={stop_price:.2f} | tgt={target_price:.2f} | qty={quantity} | "
            f"invested={_fmt_rs(invested)} | margin={_fmt_rs(margin)} | src={entry_source_used} | "
            f"open_positions={open_positions} | deployed_margin={_fmt_rs(deployed_margin)}"
    )
    _log_live_pnl_snapshot(use_ltp, source=f"entry:{ticker}")

    c_or_time_stop_dt: Optional[datetime] = None
    if (
        C_OR_BREAKOUT_TIME_STOP_ENABLED
        and C_OR_BREAKOUT_TIME_STOP_MIN > 0
        and _setup_for_candE4 == "C_OR_BREAKOUT"
    ):
        time_stop_start_ist = entry_time_ist if resume_mode else datetime.now(IST)
        candidate_dt = time_stop_start_ist + timedelta(minutes=C_OR_BREAKOUT_TIME_STOP_MIN)
        if candidate_dt < forced_close_dt:
            c_or_time_stop_dt = candidate_dt
            log.info(
                f"[TIME.STOP] Armed {side} {ticker} C_OR_BREAKOUT "
                f"{C_OR_BREAKOUT_TIME_STOP_MIN}m time stop at "
                f"{c_or_time_stop_dt.strftime('%H:%M:%S')} | ID={trade_id}"
            )

    exit_price = entry_price
    outcome = "MONITORING"
    last_valid_ltp: Optional[float] = _safe_float(signal.get("last_ltp", 0.0), 0.0) or None
    ltp_miss_count = 0
    last_kill_switch_command_id = ""
    # P1.2: breakeven stop state for E_VWAP_LOSE_EARLY_SHORT
    _be_stop_applies = (
        VWAP_EARLY_SHORT_BE_STOP_ENABLED
        and _setup_for_candE4 == "E_VWAP_LOSE_EARLY_SHORT"
    )
    _be_stop_armed = False
    _one_r = abs(entry_price - stop_price)
    _initial_stop_price = stop_price
    configured_max_hold_minutes = max(
        0.0, _safe_float(signal.get("max_hold_minutes", 0.0), 0.0)
    )
    configured_time_stop_dt = (
        entry_time_ist + timedelta(minutes=configured_max_hold_minutes)
        if configured_max_hold_minutes > 0 else None
    )

    while True:
        now_ist = datetime.now(IST)

        kill_cmd = _get_kill_switch_for_trade(signal_id, ticker)
        if kill_cmd:
            cmd_id = str(kill_cmd.get("command_id", "")).strip() or "NO_CMD_ID"
            if cmd_id != last_kill_switch_command_id:
                last_kill_switch_command_id = cmd_id
                ltp_now = get_ltp(ticker) if use_ltp else None
                if ltp_now is not None and ltp_now > 0:
                    last_valid_ltp = float(ltp_now)
                exit_price = float(last_valid_ltp) if last_valid_ltp is not None else entry_price
                outcome = "MANUAL_KILL_SWITCH"
                exit_price = _apply_exit_slippage(exit_price, side, outcome)
                req_ts = str(kill_cmd.get("requested_at_ist", "")).strip()
                log.warning(
                    f"[KILL] Kill switch exit for {side} {ticker} @ {exit_price:.2f} | "
                    f"command_id={cmd_id} | signal_id={signal_id[:12]}"
                    + (f" | requested_at={req_ts}" if req_ts else "")
                )
                break

        if now_ist >= forced_close_dt:
            ltp = get_ltp(ticker) if use_ltp else None
            if ltp is not None and ltp > 0:
                last_valid_ltp = float(ltp)
            exit_price = float(last_valid_ltp) if last_valid_ltp is not None else entry_price
            outcome = "EOD_CLOSE"
            exit_price = _apply_exit_slippage(exit_price, side, outcome)
            close_src = "last_ltp" if last_valid_ltp is not None else "entry_fallback"
            log.info(
                f"[SIM] FORCED CLOSE {side} {ticker} @ {exit_price} "
                f"(EOD forced close, src={close_src}) | ID={trade_id}"
            )
            break

        if configured_time_stop_dt is not None and now_ist >= configured_time_stop_dt:
            ltp = get_ltp(ticker) if use_ltp else None
            if ltp is not None and ltp > 0:
                last_valid_ltp = float(ltp)
            exit_price = float(last_valid_ltp) if last_valid_ltp is not None else entry_price
            outcome = "TIME"
            exit_price = _apply_exit_slippage(exit_price, side, outcome)
            log.info(
                f"[SIM] TIME EXIT {side} {ticker} @ {exit_price:.2f} "
                f"({configured_max_hold_minutes:g}m max hold) | ID={trade_id}"
            )
            break

        if use_ltp:
            ltp = get_ltp(ticker)
            if ltp is not None and ltp > 0:
                ltp = float(ltp)
                last_valid_ltp = ltp
                ltp_miss_count = 0
                with active_positions_lock:
                    if signal_id in active_positions:
                        active_positions[signal_id]["last_ltp"] = ltp
                        active_positions[signal_id]["last_ltp_time"] = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S%z")
            else:
                ltp = None
                ltp_miss_count += 1
        else:
            ltp = None

        if not use_ltp:
            exit_price = entry_price
            outcome = "NO_LTP_SIMULATED"
            log.info(
                f"[SIM] NO LTP mode (--no-ltp) - recording {side} {ticker} @ {entry_price} "
                f"as simulated entry | ID={trade_id}"
            )
            break

        if ltp is None:
            if c_or_time_stop_dt is not None and now_ist >= c_or_time_stop_dt:
                exit_price = float(last_valid_ltp) if last_valid_ltp is not None else entry_price
                outcome = "TIME_STOP_30M"
                exit_price = _apply_exit_slippage(exit_price, side, outcome)
                close_src = "last_ltp" if last_valid_ltp is not None else "entry_fallback"
                log.info(
                    f"[SIM] TIME STOP {side} {ticker} @ {exit_price:.2f} "
                    f"(C_OR_BREAKOUT {C_OR_BREAKOUT_TIME_STOP_MIN}m, src={close_src}) | ID={trade_id}"
                )
                break
            if ltp_miss_count == 1 or (ltp_miss_count % 12 == 0):
                last_err = get_last_ltp_error(ticker)
                log.warning(
                    f"[LTP.MISS] ticker={ticker} | side={side} | misses={ltp_miss_count} | "
                    f"action=retry_keep_open | ID={trade_id}"
                    + (f" | reason={last_err}" if last_err else "")
                )
            time.sleep(POLL_INTERVAL_SEC)
            continue

        # P1.2: arm breakeven stop once +0.5R profit is reached
        if _be_stop_applies and not _be_stop_armed and _one_r > 0:
            _be_trigger = (
                entry_price - VWAP_EARLY_SHORT_BE_STOP_R * _one_r
                if side == "SHORT"
                else entry_price + VWAP_EARLY_SHORT_BE_STOP_R * _one_r
            )
            _profit_reached = (side == "SHORT" and ltp <= _be_trigger) or (
                side == "LONG" and ltp >= _be_trigger
            )
            if _profit_reached:
                old_stop = stop_price
                stop_price = entry_price
                _be_stop_armed = True
                with active_positions_lock:
                    if signal_id in active_positions:
                        active_positions[signal_id]["stop_price"] = stop_price
                _persist_open_trades_state()
                log.info(
                    f"[BE.STOP] Breakeven armed {side} {ticker} {_setup_for_candE4} "
                    f"ltp={ltp:.2f} trigger={_be_trigger:.2f} "
                    f"SL {old_stop:.2f}→{stop_price:.2f} | ID={trade_id}"
                )

        if side == "SHORT":
            if ltp >= stop_price:
                exit_price = _apply_exit_slippage(stop_price, side, "SL")
                outcome = "SL"
                log.info(f"[SIM] SL HIT {side} {ticker} @ {exit_price} (LTP={ltp}) | ID={trade_id}")
                break
            if ltp <= target_price:
                exit_price = target_price
                outcome = "TARGET"
                log.info(f"[SIM] TARGET HIT {side} {ticker} @ {exit_price} (LTP={ltp}) | ID={trade_id}")
                break
        else:
            if ltp <= stop_price:
                exit_price = _apply_exit_slippage(stop_price, side, "SL")
                outcome = "SL"
                log.info(f"[SIM] SL HIT {side} {ticker} @ {exit_price} (LTP={ltp}) | ID={trade_id}")
                break
            if ltp >= target_price:
                exit_price = target_price
                outcome = "TARGET"
                log.info(f"[SIM] TARGET HIT {side} {ticker} @ {exit_price} (LTP={ltp}) | ID={trade_id}")
                break

        if c_or_time_stop_dt is not None and now_ist >= c_or_time_stop_dt:
            exit_price = float(last_valid_ltp) if last_valid_ltp is not None else entry_price
            outcome = "TIME_STOP_30M"
            exit_price = _apply_exit_slippage(exit_price, side, outcome)
            close_src = "last_ltp" if last_valid_ltp is not None else "entry_fallback"
            log.info(
                f"[SIM] TIME STOP {side} {ticker} @ {exit_price:.2f} "
                f"(C_OR_BREAKOUT {C_OR_BREAKOUT_TIME_STOP_MIN}m, src={close_src}) | ID={trade_id}"
            )
            break

        time.sleep(POLL_INTERVAL_SEC)

    exit_time_ist = datetime.now(IST)
    costed = _calc_costed_pnl(side, entry_price, float(exit_price), quantity)
    gross_pnl_rs = float(costed["gross_pnl_rs"])
    gross_pnl_pct = float(costed["gross_pnl_pct"])
    total_cost_rs = float(costed["total_cost_rs"])
    net_pnl_rs = float(costed["net_pnl_rs"])
    net_pnl_pct = float(costed["net_pnl_pct"])

    trade = PaperTrade(
        trade_id=trade_id,
        signal_id=signal_id,
        signal_datetime=str(signal.get("signal_datetime", "")),
        signal_entry_datetime_ist=str(signal.get("signal_entry_datetime_ist", "")),
        entry_time=entry_time_ist.strftime("%Y-%m-%d %H:%M:%S%z"),
        exit_time=exit_time_ist.strftime("%Y-%m-%d %H:%M:%S%z"),
        ticker=ticker,
        side=side,
        setup=setup,
        impulse_type=impulse,
        quantity=quantity,
        entry_price=round(entry_price, 2),
        exit_price=round(exit_price, 2),
        stop_price=round(stop_price, 2),
        initial_stop_price=round(_initial_stop_price, 2),
        target_price=round(target_price, 2),
        outcome=outcome,
        gross_pnl=round(gross_pnl_rs, 2),
        total_cost=round(total_cost_rs, 2),
        net_pnl=round(net_pnl_rs, 2),
        gross_pnl_rs=round(gross_pnl_rs, 2),
        gross_pnl_pct=round(gross_pnl_pct, 4),
        brokerage_rs=round(float(costed["brokerage_rs"]), 4),
        stt_rs=round(float(costed["stt_rs"]), 4),
        exch_txn_rs=round(float(costed["exch_txn_rs"]), 4),
        sebi_rs=round(float(costed["sebi_rs"]), 4),
        ipft_rs=round(float(costed["ipft_rs"]), 4),
        stamp_rs=round(float(costed["stamp_rs"]), 4),
        gst_rs=round(float(costed["gst_rs"]), 4),
        total_cost_rs=round(total_cost_rs, 2),
        net_pnl_rs=round(net_pnl_rs, 2),
        net_pnl_pct=round(net_pnl_pct, 4),
        cost_bps_of_turnover=round(float(costed["cost_bps_of_turnover"]), 4),
        cost_pct_of_entry=round(float(costed["cost_pct_of_entry"]), 4),
        cost_rates_as_of=str(costed["cost_rates_as_of"]),
        pnl_rs=round(net_pnl_rs, 2),
        pnl_pct=round(net_pnl_pct, 4),
        quality_score=_safe_float(signal.get("quality_score", 0), 0.0),
        p_win=_safe_float(signal.get("p_win", 0.0), 0.0),
        confidence_multiplier=_safe_float(signal.get("confidence_multiplier", 1.0), 1.0),
    )

    _log_trade(trade)

    with daily_pnl_lock:
        daily_pnl["total"] += net_pnl_rs
        daily_pnl["gross_total"] += gross_pnl_rs
        daily_pnl["total_cost"] += total_cost_rs
        daily_pnl["trades"] += 1
        if net_pnl_rs > 0:
            daily_pnl["wins"] += 1
            daily_pnl["net_profit"] += net_pnl_rs
        elif net_pnl_rs < 0:
            daily_pnl["losses"] += 1
            daily_pnl["net_loss"] += net_pnl_rs
        if gross_pnl_rs > 0:
            daily_pnl["gross_profit"] += gross_pnl_rs
        elif gross_pnl_rs < 0:
            daily_pnl["gross_loss"] += gross_pnl_rs
        day_total = float(daily_pnl["total"])
        day_wins = int(daily_pnl["wins"])
        day_losses = int(daily_pnl["losses"])
        _save_summary()

    log.info(
        f"[SIM] RESULT {side} {ticker} | {outcome} | "
        f"Net P&L: Rs.{net_pnl_rs:+,.2f} ({net_pnl_pct:+.2f}%) | "
        f"gross=Rs.{gross_pnl_rs:+,.2f} cost=Rs.{total_cost_rs:,.2f} | "
        f"Day net: Rs.{day_total:+,.2f} ({day_wins}W/{day_losses}L)"
    )

    _release_capacity(signal_id)
    with active_positions_lock:
        active_positions.pop(signal_id, None)
    _persist_open_trades_state()

    with active_trades_lock:
        active_trades.pop(signal_id, None)

    _log_live_pnl_snapshot(use_ltp, source=f"exit:{ticker}")
    return True


def _log_trade(trade: PaperTrade) -> None:
    """Append trade result to daily CSV."""
    today_str = datetime.now(IST).strftime("%Y-%m-%d")
    csv_path = os.path.join(SIGNAL_DIR, PAPER_TRADE_LOG_PATTERN.format(today_str))

    file_exists = os.path.exists(csv_path) and os.path.getsize(csv_path) > 0

    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=TRADE_LOG_COLUMNS, quoting=csv.QUOTE_ALL)
        if not file_exists:
            writer.writeheader()

        writer.writerow({
            "trade_id": trade.trade_id,
            "signal_id": trade.signal_id,
            "signal_datetime": trade.signal_datetime,
            "signal_entry_datetime_ist": trade.signal_entry_datetime_ist,
            "entry_time": trade.entry_time,
            "exit_time": trade.exit_time,
            "ticker": trade.ticker,
            "side": trade.side,
            "setup": trade.setup,
            "impulse_type": trade.impulse_type,
            "quantity": trade.quantity,
            "entry_price": trade.entry_price,
            "exit_price": trade.exit_price,
            "stop_price": trade.stop_price,
            "initial_stop_price": trade.initial_stop_price,
            "target_price": trade.target_price,
            "outcome": trade.outcome,
            "gross_pnl": trade.gross_pnl,
            "total_cost": trade.total_cost,
            "net_pnl": trade.net_pnl,
            "gross_pnl_rs": trade.gross_pnl_rs,
            "gross_pnl_pct": trade.gross_pnl_pct,
            "brokerage_rs": trade.brokerage_rs,
            "stt_rs": trade.stt_rs,
            "exch_txn_rs": trade.exch_txn_rs,
            "sebi_rs": trade.sebi_rs,
            "ipft_rs": trade.ipft_rs,
            "stamp_rs": trade.stamp_rs,
            "gst_rs": trade.gst_rs,
            "total_cost_rs": trade.total_cost_rs,
            "net_pnl_rs": trade.net_pnl_rs,
            "net_pnl_pct": trade.net_pnl_pct,
            "cost_bps_of_turnover": trade.cost_bps_of_turnover,
            "cost_pct_of_entry": trade.cost_pct_of_entry,
            "cost_rates_as_of": trade.cost_rates_as_of,
            "pnl_rs": trade.pnl_rs,
            "pnl_pct": trade.pnl_pct,
            "quality_score": trade.quality_score,
            "p_win": trade.p_win,
            "confidence_multiplier": trade.confidence_multiplier,
        })


def _save_summary() -> None:
    """Save running P&L summary to JSON."""
    try:
        wr = daily_pnl["wins"] / daily_pnl["trades"] * 100 if daily_pnl["trades"] > 0 else 0
        summary = {
            "date": datetime.now(IST).strftime("%Y-%m-%d"),
            "total_pnl_rs": round(daily_pnl["total"], 2),
            "pnl_basis": "NET_AFTER_NSE_ID_COSTS",
            "net_pnl_rs": round(daily_pnl["total"], 2),
            "gross_pnl_rs": round(daily_pnl.get("gross_total", 0.0), 2),
            "total_cost_rs": round(daily_pnl.get("total_cost", 0.0), 2),
            "cost_rates_as_of": NSE_COST_CONFIG.rates_as_of,
            "total_trades": daily_pnl["trades"],
            "wins": daily_pnl["wins"],
            "losses": daily_pnl["losses"],
            "gross_profit_rs": round(daily_pnl.get("gross_profit", 0.0), 2),
            "gross_loss_rs": round(daily_pnl.get("gross_loss", 0.0), 2),
            "net_profit_rs": round(daily_pnl.get("net_profit", 0.0), 2),
            "net_loss_rs": round(daily_pnl.get("net_loss", 0.0), 2),
            "win_rate_pct": round(wr, 2),
            "last_updated": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S"),
        }
        _atomic_write_json(SUMMARY_FILE, summary)
    except Exception:
        pass


# ============================================================================
# SIGNAL NORMALISATION
# ============================================================================
def _normalize_signal(raw: dict) -> dict:
    """
    Map signal-generator CSV column names to the names the executor expects.
    Preserve signal quantity/SL/target when present so simulation stays aligned
    with the scanner output. Backfill missing values from executor defaults.
    """
    sig = {}
    for k, v in raw.items():
        mapped = _SIGNAL_COL_MAP.get(k, k)
        sig[mapped] = v

    if not sig.get("signal_entry_datetime_ist"):
        sig["signal_entry_datetime_ist"] = (
            sig.get("signal_bar_time_ist")
            or sig.get("bar_time_ist")
            or sig.get("signal_datetime")
            or ""
        )

    if not sig.get("signal_datetime"):
        sig["signal_datetime"] = sig.get("signal_entry_datetime_ist", "")

    signal_quantity = _safe_int(sig.get("quantity", 0), 0)
    if FORCE_ENTRY_QUANTITY is not None:
        sig["quantity"] = max(1, int(FORCE_ENTRY_QUANTITY))
    elif signal_quantity > 0:
        sig["quantity"] = int(signal_quantity)
    else:
        entry = _safe_float(sig.get("entry_price", 0), 0.0)
        if entry > 0:
            notional = float(DEFAULT_POSITION_SIZE) * float(INTRADAY_LEVERAGE)
            sig["quantity"] = max(1, int(notional / entry))
        else:
            sig["quantity"] = 1

    # Keep signal SL/target when supplied; only backfill missing fields.
    side = str(sig.get("side", "")).strip().upper()
    entry = _safe_float(sig.get("entry_price", 0.0), 0.0)
    stop_price = _safe_float(sig.get("stop_price", 0.0), 0.0)
    target_price = _safe_float(sig.get("target_price", 0.0), 0.0)
    if entry > 0 and side in {"SHORT", "LONG"} and (stop_price <= 0 or target_price <= 0):
        if side == "SHORT":
            sig["stop_price"] = round(entry * (1.0 + SHORT_STOP_PCT), 2)
            sig["target_price"] = round(entry * (1.0 - SHORT_TARGET_PCT), 2)
        else:
            sig["stop_price"] = round(entry * (1.0 - LONG_STOP_PCT), 2)
            sig["target_price"] = round(entry * (1.0 + LONG_TARGET_PCT), 2)

    return sig


def _parse_ist_signal_ts(value: object) -> Optional[pd.Timestamp]:
    s = str(value or "").strip()
    if not s:
        return None
    try:
        ts = pd.to_datetime(s, errors="coerce")
        if pd.isna(ts):
            return None
        ts = pd.Timestamp(ts)
        if ts.tzinfo is None:
            ts = ts.tz_localize(IST)
        else:
            ts = ts.tz_convert(IST)
        return ts
    except Exception:
        return None


def _entry_window_reference_ts(signal: dict) -> Optional[pd.Timestamp]:
    for key in ("entry_time_ist", "entry_ts", "entry_datetime_ist", "entry_datetime"):
        ts = _parse_ist_signal_ts(signal.get(key, ""))
        if ts is not None:
            return ts.floor("min")

    entry_ts = _parse_ist_signal_ts(signal.get("signal_entry_datetime_ist", ""))
    bar_ts = None
    for key in ("signal_bar_time_ist", "bar_time_ist"):
        bar_ts = _parse_ist_signal_ts(signal.get(key, ""))
        if bar_ts is not None:
            break

    if entry_ts is not None:
        if bar_ts is not None and abs((entry_ts - bar_ts).total_seconds()) < 1:
            return bar_ts.floor("min") + pd.Timedelta(minutes=ENTRY_SIGNAL_TO_ENTRY_LAG_MIN)
        return entry_ts.floor("min")
    if bar_ts is not None:
        return bar_ts.floor("min") + pd.Timedelta(minutes=ENTRY_SIGNAL_TO_ENTRY_LAG_MIN)

    signal_ts = _parse_ist_signal_ts(signal.get("signal_datetime", ""))
    if signal_ts is not None:
        return signal_ts.floor("min") + pd.Timedelta(minutes=ENTRY_SIGNAL_TO_ENTRY_LAG_MIN)
    return None


def _entry_time_window_gate(signal: dict) -> Tuple[bool, str, str]:
    entry_ts = _entry_window_reference_ts(signal)
    if entry_ts is None:
        entry_ts = pd.Timestamp(datetime.now(IST)).floor("min")
        reference_source = "current_time_fallback"
    else:
        reference_source = "signal_entry_time"

    entry_t = entry_ts.time()
    if ENTRY_WINDOW_START <= entry_t <= ENTRY_WINDOW_END:
        return False, "", ""

    ticker = str(signal.get("ticker", "")).strip().upper()
    side = str(signal.get("side", "")).strip().upper()
    setup = str(signal.get("setup", "")).strip().upper()
    return (
        True,
        "ENTRY_SKIPPED_ENTRY_TIME_WINDOW",
        (
            f"[ENTRY.WINDOW] Skipping {side} {ticker} {setup}: "
            f"entry_time={entry_ts.strftime('%H:%M')} outside "
            f"{ENTRY_WINDOW_START_RAW}-{ENTRY_WINDOW_END_RAW} IST "
            f"| source={reference_source}"
        ),
    )


def _signal_ist_date(sig: dict) -> Optional[datetime.date]:
    for key in ("signal_entry_datetime_ist", "signal_bar_time_ist", "bar_time_ist", "signal_datetime"):
        ts = _parse_ist_signal_ts(sig.get(key, ""))
        if ts is not None:
            return ts.date()
    return None


def _filter_today_signals(signals: List[dict]) -> Tuple[List[dict], int]:
    today = datetime.now(IST).date()
    filtered: List[dict] = []
    dropped = 0
    for sig in signals:
        sig_date = _signal_ist_date(sig)
        if sig_date == today:
            filtered.append(sig)
        else:
            dropped += 1
    return filtered, dropped


# ============================================================================
# CSV SIGNAL READER
# ============================================================================
def read_signals_csv(csv_path: str) -> List[dict]:
    """Read signals from the daily CSV file."""
    if not os.path.exists(csv_path):
        return []
    try:
        df = pd.read_csv(
            csv_path,
            quotechar='"',
            quoting=csv.QUOTE_ALL,
            on_bad_lines="warn",
            engine="python",
        )
        if df.empty:
            return []
        signals = [_normalize_signal(row) for row in df.to_dict("records")]
        signals, dropped = _filter_today_signals(signals)
        if dropped > 0:
            log.warning(
                f"[CSV] Ignored {dropped} stale signal row(s) not matching today's IST date."
            )
        return signals
    except Exception as e:
        log.error(f"Error reading signals CSV: {e}")
        return []


def get_signal_csv_paths_for_today() -> List[str]:
    today_str = datetime.now(IST).strftime("%Y-%m-%d")
    return [os.path.join(SIGNAL_DIR, pattern.format(today_str)) for pattern in SIGNAL_CSV_PATTERNS]


def read_signals_csv_multi(csv_paths: Sequence[str]) -> List[dict]:
    """
    Read and merge signals from multiple CSV files (short + long direction).
    Dedupe by signal_id, keeping first-seen row.
    """
    merged: Dict[str, dict] = {}
    for csv_path in csv_paths:
        rows = read_signals_csv(csv_path)
        if not rows:
            continue
        for sig in rows:
            sid = str(sig.get("signal_id", "")).strip()
            if not sid:
                continue
            if sid not in merged:
                merged[sid] = sig

    out = list(merged.values())
    out.sort(
        key=lambda r: (
            str(
                r.get("signal_entry_datetime_ist")
                or r.get("signal_bar_time_ist")
                or r.get("signal_datetime")
                or ""
            ),
            str(r.get("ticker", "")),
            str(r.get("side", "")),
        )
    )
    return out


def _sanitize_today_paper_trade_csv() -> Tuple[int, int]:
    """
    Ensure today's paper trade CSV only has rows whose signal time is from today (IST).
    Returns (rows_before, rows_removed).
    """
    today_str = datetime.now(IST).strftime("%Y-%m-%d")
    today_date = datetime.now(IST).date()
    csv_path = os.path.join(SIGNAL_DIR, PAPER_TRADE_LOG_PATTERN.format(today_str))
    if not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
        return 0, 0

    try:
        df = pd.read_csv(
            csv_path,
            quotechar='"',
            quoting=csv.QUOTE_ALL,
            on_bad_lines="warn",
            engine="python",
        )
    except Exception:
        return 0, 0

    if df.empty:
        return 0, 0

    rows_before = int(len(df))
    keep_mask = []
    for _, row in df.iterrows():
        row_date = _signal_ist_date(row)
        keep_mask.append(row_date == today_date if row_date is not None else False)

    df_today = df[pd.Series(keep_mask, index=df.index)].copy()
    rows_removed = rows_before - int(len(df_today))
    if rows_removed <= 0:
        return rows_before, 0

    for c in TRADE_LOG_COLUMNS:
        if c not in df_today.columns:
            df_today[c] = ""
    df_today = df_today[TRADE_LOG_COLUMNS]
    df_today.to_csv(csv_path, index=False, quoting=csv.QUOTE_ALL)
    return rows_before, rows_removed


def _load_closed_ids_and_realized_summary(
    paper_csv_path: str,
    today_date: date,
) -> Tuple[Set[str], Set[str], Dict[str, float]]:
    closed_ids: Set[str] = set()
    opened_ids: Set[str] = set()
    realized_total = 0.0
    realized_trades = 0
    realized_wins = 0
    realized_losses = 0
    gross_total = 0.0
    total_cost = 0.0
    gross_profit = 0.0
    gross_loss = 0.0
    net_profit = 0.0
    net_loss = 0.0

    if os.path.exists(paper_csv_path) and os.path.getsize(paper_csv_path) > 0:
        try:
            df = pd.read_csv(
                paper_csv_path,
                quotechar='"',
                quoting=csv.QUOTE_ALL,
                on_bad_lines="warn",
                engine="python",
            )
            if not df.empty:
                for row in df.to_dict("records"):
                    row_date = _signal_ist_date(row)
                    if row_date is not None and row_date != today_date:
                        continue

                    sid = str(row.get("signal_id", "")).strip()
                    if sid:
                        closed_ids.add(sid)
                        outcome = str(row.get("outcome", "")).strip().upper()
                        if not outcome.startswith("ENTRY_SKIPPED"):
                            opened_ids.add(sid)

                    net_pnl_rs = _row_float_first(row, ("net_pnl_rs", "net_pnl", "pnl_rs"), 0.0)
                    gross_pnl_rs = _row_float_first(row, ("gross_pnl_rs", "gross_pnl", "pnl_rs"), net_pnl_rs)
                    cost_rs = _row_float_first(row, ("total_cost_rs", "total_cost"), max(0.0, gross_pnl_rs - net_pnl_rs))
                    realized_total += net_pnl_rs
                    gross_total += gross_pnl_rs
                    total_cost += cost_rs
                    realized_trades += 1
                    if net_pnl_rs > 0:
                        realized_wins += 1
                        net_profit += net_pnl_rs
                    elif net_pnl_rs < 0:
                        realized_losses += 1
                        net_loss += net_pnl_rs
                    if gross_pnl_rs > 0:
                        gross_profit += gross_pnl_rs
                    elif gross_pnl_rs < 0:
                        gross_loss += gross_pnl_rs
        except Exception as e:
            log.warning(f"[RESTORE] Could not parse paper trade CSV: {e}")

    return closed_ids, opened_ids, {
        "realized_total": float(realized_total),
        "realized_trades": float(realized_trades),
        "realized_wins": float(realized_wins),
        "realized_losses": float(realized_losses),
        "gross_total": float(gross_total),
        "total_cost": float(total_cost),
        "gross_profit": float(gross_profit),
        "gross_loss": float(gross_loss),
        "net_profit": float(net_profit),
        "net_loss": float(net_loss),
    }


def _restore_intraday_runtime_state(
    signal_csv_paths: Sequence[str],
    paper_csv_path: str,
    executed: Set[str],
) -> Tuple[Dict[str, float], List[dict]]:
    """
    Restore runtime state after a mid-session restart.
    Priority source for open positions is open_trades_state_YYYY-MM-DD.json.
    """
    today_str = datetime.now(IST).strftime("%Y-%m-%d")
    today_date = datetime.now(IST).date()

    # Build today's signal lookup by signal_id from both short + long direction CSVs
    signal_rows = read_signals_csv_multi(signal_csv_paths)
    signals_by_id: Dict[str, dict] = {}
    for sig in signal_rows:
        sid = str(sig.get("signal_id", "")).strip()
        if sid:
            signals_by_id[sid] = sig

    closed_ids, opened_ids, realized = _load_closed_ids_and_realized_summary(
        paper_csv_path=paper_csv_path,
        today_date=today_date,
    )
    _replace_opened_entry_ids(opened_ids)

    with daily_pnl_lock:
        daily_pnl["total"] = float(realized["realized_total"])
        daily_pnl["trades"] = int(realized["realized_trades"])
        daily_pnl["wins"] = int(realized["realized_wins"])
        daily_pnl["losses"] = int(realized["realized_losses"])
        daily_pnl["gross_total"] = float(realized["gross_total"])
        daily_pnl["total_cost"] = float(realized["total_cost"])
        daily_pnl["gross_profit"] = float(realized["gross_profit"])
        daily_pnl["gross_loss"] = float(realized["gross_loss"])
        daily_pnl["net_profit"] = float(realized["net_profit"])
        daily_pnl["net_loss"] = float(realized["net_loss"])
        _save_summary()

    restored_positions: Dict[str, dict] = {}
    resume_signals: List[dict] = []
    restored_from_state = 0
    reconstructed_from_signals = 0

    # Primary restore path: exact open-trade state persisted during runtime
    state_rows = _load_open_trades_state(today_str=today_str)
    seen_state_ids: Set[str] = set()
    for row in state_rows:
        sid = str(row.get("signal_id", "")).strip()
        if not sid or sid in seen_state_ids or sid in closed_ids:
            continue
        seen_state_ids.add(sid)

        base = signals_by_id.get(sid, {})
        ticker = str(row.get("ticker") or base.get("ticker") or "").strip().upper()
        side = str(row.get("side") or base.get("side") or "LONG").strip().upper()
        qty = _safe_int(row.get("quantity", base.get("quantity", 1)), 1)
        entry_exec = _safe_float(
            row.get("entry_price", row.get("entry_price_exec", base.get("entry_price", 0.0))),
            0.0,
        )
        stop_price = _safe_float(row.get("stop_price", base.get("stop_price", 0.0)), 0.0)
        target_price = _safe_float(row.get("target_price", base.get("target_price", 0.0)), 0.0)
        entry_time = str(
            row.get("entry_time")
            or base.get("signal_entry_datetime_ist")
            or base.get("signal_datetime")
            or ""
        ).strip()
        signal_datetime = str(
            base.get("signal_datetime")
            or row.get("signal_datetime")
            or entry_time
        ).strip()
        signal_entry_dt = str(
            base.get("signal_entry_datetime_ist")
            or base.get("signal_bar_time_ist")
            or row.get("signal_entry_datetime_ist")
            or signal_datetime
        ).strip()
        trade_id = str(row.get("trade_id", "")).strip() or f"RESTORE-{sid[:8]}"
        last_ltp = _safe_float(row.get("last_ltp", 0.0), 0.0)

        if (
            not sid
            or not ticker
            or side not in {"LONG", "SHORT"}
            or qty <= 0
            or entry_exec <= 0
            or stop_price <= 0
            or target_price <= 0
        ):
            continue

        restored_positions[sid] = {
            "trade_id": trade_id,
            "signal_id": sid,
            "ticker": ticker,
            "side": side,
            "setup": str(row.get("setup") or base.get("setup", "")).strip().upper(),
            "quantity": qty,
            "entry_price": float(entry_exec),
            "stop_price": float(stop_price),
            "target_price": float(target_price),
            "entry_time": entry_time,
            "last_ltp": float(last_ltp),
            "restored": True,
        }
        resume_signals.append(
            {
                "trade_id": trade_id,
                "signal_id": sid,
                "ticker": ticker,
                "side": side,
                "quantity": qty,
                "entry_price": _safe_float(base.get("entry_price", entry_exec), entry_exec),
                "entry_price_exec": float(entry_exec),
                "stop_price": float(stop_price),
                "target_price": float(target_price),
                "signal_datetime": signal_datetime,
                "signal_entry_datetime_ist": signal_entry_dt,
                "received_time": str(base.get("received_time", "")),
                "setup": str(base.get("setup", "")),
                "impulse_type": str(base.get("impulse_type", "")),
                "quality_score": _safe_float(base.get("quality_score", 0.0), 0.0),
                "p_win": _safe_float(base.get("p_win", 0.0), 0.0),
                "confidence_multiplier": _safe_float(base.get("confidence_multiplier", 1.0), 1.0),
                "entry_time": entry_time,
                "last_ltp": float(last_ltp),
                "pre_reserved_margin": float(entry_exec * qty / INTRADAY_LEVERAGE),
            }
        )
        restored_from_state += 1

    # Fallback restore path (for legacy runs before open-state persistence existed)
    # We only reconstruct when a signal was executed but not closed and no state row exists.
    for sid, sig in signals_by_id.items():
        if sid in closed_ids or sid in restored_positions or sid not in executed:
            continue

        ticker = str(sig.get("ticker", "")).strip().upper()
        side = str(sig.get("side", "LONG")).strip().upper()
        qty = _safe_int(sig.get("quantity", 1), 1)
        signal_entry = _safe_float(sig.get("entry_price", 0.0), 0.0)
        stop_price = _safe_float(sig.get("stop_price", 0.0), 0.0)
        target_price = _safe_float(sig.get("target_price", 0.0), 0.0)
        if (
            not sid
            or not ticker
            or side not in {"LONG", "SHORT"}
            or qty <= 0
            or signal_entry <= 0
            or stop_price <= 0
            or target_price <= 0
        ):
            continue

        if side == "LONG":
            entry_exec = round(signal_entry * (1 + SLIPPAGE_PCT), 2)
        else:
            entry_exec = round(signal_entry * (1 - SLIPPAGE_PCT), 2)

        entry_time = str(sig.get("signal_entry_datetime_ist") or sig.get("signal_datetime") or "").strip()
        trade_id = f"RECON-{sid[:8]}"
        restored_positions[sid] = {
            "trade_id": trade_id,
            "signal_id": sid,
            "ticker": ticker,
            "side": side,
            "setup": str(sig.get("setup", "")).strip().upper(),
            "quantity": qty,
            "entry_price": float(entry_exec),
            "stop_price": float(stop_price),
            "target_price": float(target_price),
            "entry_time": entry_time,
            "last_ltp": 0.0,
            "restored": True,
        }
        resume_signals.append(
            {
                "trade_id": trade_id,
                "signal_id": sid,
                "ticker": ticker,
                "side": side,
                "quantity": qty,
                "entry_price": float(signal_entry),
                "entry_price_exec": float(entry_exec),
                "stop_price": float(stop_price),
                "target_price": float(target_price),
                "signal_datetime": str(sig.get("signal_datetime", "")),
                "signal_entry_datetime_ist": str(sig.get("signal_entry_datetime_ist", "")),
                "received_time": str(sig.get("received_time", "")),
                "setup": str(sig.get("setup", "")),
                "impulse_type": str(sig.get("impulse_type", "")),
                "quality_score": _safe_float(sig.get("quality_score", 0.0), 0.0),
                "p_win": _safe_float(sig.get("p_win", 0.0), 0.0),
                "confidence_multiplier": _safe_float(sig.get("confidence_multiplier", 1.0), 1.0),
                "entry_time": entry_time,
                "last_ltp": 0.0,
                "pre_reserved_margin": float(entry_exec * qty / INTRADAY_LEVERAGE),
            }
        )
        reconstructed_from_signals += 1

    with active_positions_lock:
        active_positions.clear()
        active_positions.update(restored_positions)
    with opened_entry_ids_lock:
        opened_entry_ids.update(restored_positions.keys())

    with capital_lock:
        capital_deployed.clear()
        for sid, pos in restored_positions.items():
            margin = float(pos["entry_price"] * pos["quantity"] / INTRADAY_LEVERAGE)
            capital_deployed[sid] = margin

    # Keep closed signal_ids blocked for the full trading day so a restart
    # cannot re-dispatch older rows that are still present in today's signal CSVs.
    with executed_lock:
        executed.update(closed_ids)
        executed.update(restored_positions.keys())

    _persist_open_trades_state()
    _, deployed_margin = _capital_snapshot()
    stats = {
        "signals_today": float(len(signals_by_id)),
        "executed_loaded": float(len(executed)),
        "closed_today": float(len(closed_ids)),
        "open_restored": float(len(restored_positions)),
        "restored_exact": float(restored_from_state),
        "restored_reconstructed": float(reconstructed_from_signals),
        "realized_trades": float(realized["realized_trades"]),
        "realized_total": float(realized["realized_total"]),
        "deployed_margin": float(deployed_margin),
    }
    return stats, resume_signals


# ============================================================================
# SIGNAL PROCESSOR
# ============================================================================
def _launch_trade_thread(
    signal: dict,
    signal_id: str,
    executed: Set[str],
    use_ltp: bool,
    trade_semaphore: Optional[threading.Semaphore],
    entry_price_source: str,
    pre_reserved_margin: Optional[float] = None,
    resume_mode: bool = False,
) -> bool:
    with inflight_signals_lock:
        if signal_id in inflight_signals:
            return False
        inflight_signals.add(signal_id)

    def _run_sim(
        sig=signal,
        sid=signal_id,
        ultp=use_ltp,
        eps=entry_price_source,
        reserved=pre_reserved_margin,
        is_resume=resume_mode,
    ):
        ok = False
        acquired = False
        if trade_semaphore is not None:
            trade_semaphore.acquire()
            acquired = True
        try:
            ok = simulate_trade(
                sig,
                use_ltp=ultp,
                entry_price_source=eps,
                pre_reserved_margin=reserved,
                resume_mode=is_resume,
            )
        except Exception as e:
            log.error(f"Simulation error for {sig.get('ticker', '?')}: {e}")
            log.error(traceback.format_exc())
        finally:
            if acquired and trade_semaphore is not None:
                trade_semaphore.release()
            with inflight_signals_lock:
                inflight_signals.discard(sid)

            if not ok:
                _release_capacity(sid)
                with active_positions_lock:
                    active_positions.pop(sid, None)
                _persist_open_trades_state()
                with active_trades_lock:
                    active_trades.pop(sid, None)

                changed = False
                with executed_lock:
                    if sid in executed:
                        executed.discard(sid)
                        changed = True
                    executed_snapshot = set(executed)
                if changed:
                    save_executed_signals(executed_snapshot)
                    log.warning(
                        f"[RETRY] signal_id={sid[:12]} released after simulation failure; eligible for re-dispatch."
                    )

    t = threading.Thread(target=_run_sim, daemon=True, name=f"paper-trade-{signal_id[:8]}")
    with active_trades_lock:
        active_trades[signal_id] = t

    try:
        t.start()
        return True
    except Exception:
        with active_trades_lock:
            active_trades.pop(signal_id, None)
        with inflight_signals_lock:
            inflight_signals.discard(signal_id)
        return False


def process_new_signals(
    csv_paths: Sequence[str],
    executed: Set[str],
    use_ltp: bool,
    trade_semaphore: Optional[threading.Semaphore],
    entry_price_source: str = "signal_bar",
) -> Set[str]:
    """
    Read signal CSVs, find unprocessed signals, launch simulation threads.
    Returns updated executed signals set.
    """
    if not _is_market_open_now():
        return executed

    signals = read_signals_csv_multi(csv_paths)
    if not signals:
        return executed

    new_count = 0
    executed_changed = False
    for signal in signals:
        signal_id = str(signal.get("signal_id", "")).strip()
        if not signal_id:
            continue

        with executed_lock:
            if signal_id in executed:
                continue

        with inflight_signals_lock:
            if signal_id in inflight_signals:
                continue

        signal = _enriched_signal_context(signal)
        skip_by_entry_window, entry_window_outcome, entry_window_message = _entry_time_window_gate(signal)
        if skip_by_entry_window:
            if _record_pre_entry_skip(
                signal=signal,
                outcome_name=entry_window_outcome,
                warning_message=entry_window_message + f" | signal_id={signal_id[:12]}",
                use_ltp=use_ltp,
                trade_start_ist=datetime.now(IST),
                release_capacity=False,
                clear_active_trade=False,
            ):
                with executed_lock:
                    executed.add(signal_id)
                    executed_changed = True
            continue

        skip_by_research, skip_outcome, skip_message = _research_paper_gate(signal)
        if skip_by_research:
            if _record_pre_entry_skip(
                signal=signal,
                outcome_name=skip_outcome,
                warning_message=skip_message + f" | signal_id={signal_id[:12]}",
                use_ltp=use_ltp,
                trade_start_ist=datetime.now(IST),
                release_capacity=False,
                clear_active_trade=False,
            ):
                with executed_lock:
                    executed.add(signal_id)
                    executed_changed = True
            continue

        skip_by_daily_brake, brake_outcome, brake_message = _daily_loss_brake_gate(signal)
        if skip_by_daily_brake:
            if _record_pre_entry_skip(
                signal=signal,
                outcome_name=brake_outcome,
                warning_message=brake_message + f" | signal_id={signal_id[:12]}",
                use_ltp=use_ltp,
                trade_start_ist=datetime.now(IST),
                release_capacity=False,
                clear_active_trade=False,
            ):
                with executed_lock:
                    executed.add(signal_id)
                    executed_changed = True
            continue

        skip_by_c_or_cap, c_or_cap_outcome, c_or_cap_message = _c_or_breakout_session_cap_gate(signal)
        if skip_by_c_or_cap:
            if _record_pre_entry_skip(
                signal=signal,
                outcome_name=c_or_cap_outcome,
                warning_message=c_or_cap_message + f" | signal_id={signal_id[:12]}",
                use_ltp=use_ltp,
                trade_start_ist=datetime.now(IST),
                release_capacity=False,
                clear_active_trade=False,
            ):
                with executed_lock:
                    executed.add(signal_id)
                    executed_changed = True
            continue

        allowed, reason, reserved_margin = _reserve_capacity_for_signal(signal_id, signal)
        if not allowed:
            now_ist = datetime.now(IST)
            forced_close_dt = IST.localize(datetime.combine(now_ist.date(), FORCED_CLOSE_TIME))
            entry_retry_deadline = _entry_retry_deadline(signal, now_ist, forced_close_dt)
            if reason.startswith("max open positions reached") and _trade_started_after_entry_deadline(
                now_ist,
                entry_retry_deadline,
            ):
                if _record_pre_entry_skip(
                    signal=signal,
                    outcome_name="ENTRY_SKIPPED_CAPACITY_TIMEOUT",
                    warning_message=(
                        f"[CAPACITY] Skipping {signal.get('ticker', '?')} "
                        f"{signal.get('side', '?')}: open-position limit stayed full "
                        f"until freshness deadline {entry_retry_deadline.strftime('%H:%M:%S')} "
                        f"| reason={reason} | signal_id={signal_id[:12]}"
                    ),
                    use_ltp=use_ltp,
                    trade_start_ist=now_ist,
                    release_capacity=False,
                    clear_active_trade=False,
                ):
                    with executed_lock:
                        executed.add(signal_id)
                        executed_changed = True
                continue

            log.warning(
                f"[RISK] Rejecting {signal.get('side', '?')} "
                f"{signal.get('ticker', '?')}: {reason}"
            )
            continue

        cap_counted = False
        c_or_count_ok, c_or_count_reason = _c_or_session_cap_increment(signal_id, signal)
        if not c_or_count_ok:
            if _record_pre_entry_skip(
                signal=signal,
                outcome_name="ENTRY_SKIPPED_C_OR_SATURATION_CAP",
                warning_message=(
                    f"[C_OR.CAP] Skipping {signal.get('side', '?')} {signal.get('ticker', '?')} "
                    f"C_OR_BREAKOUT after capacity reservation: {c_or_count_reason} | "
                    f"signal_id={signal_id[:12]}"
                ),
                use_ltp=use_ltp,
                trade_start_ist=datetime.now(IST),
                release_capacity=True,
                clear_active_trade=False,
            ):
                with executed_lock:
                    executed.add(signal_id)
                    executed_changed = True
            continue
        cap_counted = _c_or_session_cap_enabled_for_signal(signal)

        # Mark executed only after reservation succeeds; if launch fails this is reverted.
        with executed_lock:
            executed.add(signal_id)
            executed_changed = True

        started = _launch_trade_thread(
            signal=signal,
            signal_id=signal_id,
            executed=executed,
            use_ltp=use_ltp,
            trade_semaphore=trade_semaphore,
            entry_price_source=entry_price_source,
            pre_reserved_margin=reserved_margin,
            resume_mode=False,
        )
        if not started:
            _release_capacity(signal_id)
            if cap_counted:
                _c_or_session_cap_decrement(signal_id)
            with executed_lock:
                executed.discard(signal_id)
                executed_changed = True
            log.error(f"[DISPATCH] Failed to launch simulation thread for signal_id={signal_id[:12]}")
            continue

        new_count += 1

        c_or_cap_log = (
            f" | c_or_cap={c_or_count_reason}"
            if c_or_count_reason != "not_applicable"
            else ""
        )
        log.info(
            f"[DISPATCH] Launched simulation for "
            f"{signal.get('side', '?')} {signal.get('ticker', '?')} "
            f"@ {signal.get('entry_price', '?')} | p_win={signal.get('p_win', '?')} | "
            f"reserved_margin={_fmt_rs(reserved_margin)}"
            f"{c_or_cap_log} | ID={signal_id[:12]}"
        )

    if executed_changed:
        with executed_lock:
            executed_snapshot = set(executed)
        save_executed_signals(executed_snapshot)

    if new_count > 0:
        with active_trades_lock:
            active_sim_count = len(active_trades)
        log.info(f"Processed {new_count} new signal(s). Active sims: {active_sim_count}")
        _log_live_pnl_snapshot(use_ltp, source="dispatch")

    return executed


def start_resumed_trade_monitors(
    resumed_signals: List[dict],
    executed: Set[str],
    use_ltp: bool,
    trade_semaphore: Optional[threading.Semaphore],
    entry_price_source: str = "signal_bar",
) -> int:
    if not resumed_signals:
        return 0

    started = 0
    for signal in resumed_signals:
        signal_id = str(signal.get("signal_id", "")).strip()
        if not signal_id:
            continue

        reserved_margin = _safe_float(signal.get("pre_reserved_margin", 0.0), 0.0)
        launched = _launch_trade_thread(
            signal=signal,
            signal_id=signal_id,
            executed=executed,
            use_ltp=use_ltp,
            trade_semaphore=trade_semaphore,
            entry_price_source=entry_price_source,
            pre_reserved_margin=reserved_margin if reserved_margin > 0 else None,
            resume_mode=True,
        )
        if not launched:
            _release_capacity(signal_id)
            with active_positions_lock:
                active_positions.pop(signal_id, None)
            _persist_open_trades_state()
            with executed_lock:
                executed.discard(signal_id)
                executed_snapshot = set(executed)
            save_executed_signals(executed_snapshot)
            log.error(f"[RESUME] Failed to launch monitor thread for signal_id={signal_id[:12]}")
            continue
        started += 1

    if started > 0:
        with executed_lock:
            executed_snapshot = set(executed)
        save_executed_signals(executed_snapshot)
    return started


# ============================================================================
# WATCHDOG FILE MONITOR
# ============================================================================
class SignalCSVHandler(FileSystemEventHandler):
    """Watches multiple signal CSV files for modifications and triggers processing."""

    def __init__(self, csv_paths: Sequence[str], callback, debounce_sec: float = 3.0):
        super().__init__()
        self.csv_paths = list(csv_paths)
        self.csv_filenames = {os.path.basename(p) for p in self.csv_paths}
        self.callback = callback
        self.debounce_sec = debounce_sec
        self._timer: Optional[threading.Timer] = None
        self._lock = threading.Lock()

    def on_modified(self, event):
        if event.is_directory:
            return
        if os.path.basename(event.src_path) in self.csv_filenames:
            self._debounce()

    def on_created(self, event):
        if event.is_directory:
            return
        if os.path.basename(event.src_path) in self.csv_filenames:
            self._debounce()

    def _debounce(self):
        with self._lock:
            if self._timer and self._timer.is_alive():
                self._timer.cancel()
            self._timer = threading.Timer(self.debounce_sec, self.callback)
            self._timer.start()


# ============================================================================
# MAIN
# ============================================================================
def main():
    parser = argparse.ArgumentParser(description="AVWAP Paper Trade Executor V7 ID 5min")
    parser.add_argument(
        "--no-ltp", action="store_true",
        help="Disable Kite LTP polling (record trades at signal prices)",
    )
    parser.add_argument(
        "--capital", type=float, default=DEFAULT_START_CAPITAL,
        help=f"Starting capital in Rs (default: {DEFAULT_START_CAPITAL})",
    )
    parser.add_argument(
        "--entry-price-source",
        choices=ENTRY_PRICE_SOURCE_CHOICES,
        default=ENTRY_PRICE_SOURCE_DEFAULT,
        help=(
            "Entry reference source: "
            "'signal_bar' uses CSV 15m entry_price, "
            "'ltp_on_signal' uses live LTP at dispatch "
            "(fallback to signal_bar when LTP is unavailable)."
        ),
    )
    parser.add_argument(
        "--max-trades",
        type=int,
        default=MAX_CONCURRENT_TRADES,
        help=(
            "Max concurrent simulation workers "
            f"(default: {MAX_CONCURRENT_TRADES}; 0 or less = unlimited)"
        ),
    )
    args = parser.parse_args()
    manifest_path, _ = freeze_runtime_manifest(
        "paper_trade_executor_id_5min_v7",
        runtime_root=Path(RUNTIME_LIVE_SIGNALS_DIR).parent,
        source_files=(Path(__file__), Path(rb.__file__)),
        resolved_config={
            "signal_dir": SIGNAL_DIR,
            "max_concurrent_trades": int(args.max_trades),
            "max_open_positions": int(MAX_OPEN_POSITIONS),
            "max_capital_deployed_rs": float(MAX_CAPITAL_DEPLOYED_RS),
            "daily_loss_brake_enabled": bool(DAILY_LOSS_BRAKE_ENABLED),
            "daily_loss_brake_rs": float(DAILY_LOSS_BRAKE_RS),
            "entry_window_start": ENTRY_WINDOW_START_RAW,
            "entry_window_end": ENTRY_WINDOW_END_RAW,
        },
    )

    use_ltp = not args.no_ltp

    log.info("=" * 65)
    log.info("AVWAP Paper Trade Executor V7 ID 5min -- PAPER_TRADE = TRUE")
    log.info(f"Frozen runtime manifest: {manifest_path}")
    log.info(f"  Mode            : SIMULATION (no real orders)")
    log.info(f"  LTP polling     : {'Enabled' if use_ltp else 'Disabled'}")
    log.info(f"  Entry source    : {args.entry_price_source}")
    log.info(
        f"  Entry window    : {ENTRY_WINDOW_START_RAW}-{ENTRY_WINDOW_END_RAW} IST "
        f"(signal-to-entry lag fallback={ENTRY_SIGNAL_TO_ENTRY_LAG_MIN}m)"
    )
    log.info(f"  Max concurrent  : {args.max_trades}")
    log.info(f"  Starting capital: Rs.{args.capital:,.0f}")
    log.info(f"  Signal dir      : {os.path.abspath(SIGNAL_DIR)}/")
    log.info(f"  Forced close at : {FORCED_CLOSE_TIME} IST")
    log.info(
        "  Entry sizing    : "
        "signal quantity preferred | "
        f"fallback margin=Rs.{DEFAULT_POSITION_SIZE:,.0f}/signal | "
        f"MIS={INTRADAY_LEVERAGE:.1f}x | "
        f"notional≈Rs.{DEFAULT_POSITION_SIZE * INTRADAY_LEVERAGE:,.0f}"
    )
    if FORCE_ENTRY_QUANTITY is not None:
        log.info(f"  Quantity override: FORCE_ENTRY_QUANTITY={FORCE_ENTRY_QUANTITY} (all NEW entries)")
    log.info(
        "  SL/Target policy: "
        "signal prices preferred | "
        f"SHORT=SL {SHORT_STOP_PCT*100:.2f}% / TGT {SHORT_TARGET_PCT*100:.2f}% | "
        f"LONG=SL {LONG_STOP_PCT*100:.2f}% / TGT {LONG_TARGET_PCT*100:.2f}% "
        "(fallback profile; rebased from actual fill)"
    )
    if RISK_LIMITS_ENABLED:
        log.info(
            f"  Risk limits     : ENABLED (max_open={MAX_OPEN_POSITIONS}, "
            f"max_margin=Rs.{MAX_CAPITAL_DEPLOYED_RS:,.0f})"
        )
    else:
        log.warning("  Risk limits     : DISABLED (no max-open / margin cap checks)")
    log.info(
        "  Research gates  : "
        f"{'ENABLED' if RESEARCH_PAPER_GATES_ENABLED else 'DISABLED'} | "
        f"anti_chase_long close_loc>{ANTI_CHASE_LONG_CLOSE_LOC_MIN:.2f}, "
        f"vwap_dist_atr>{ANTI_CHASE_LONG_VWAP_DIST_ATR_MIN:.2f} | "
        f"version={RESEARCH_PAPER_GATE_VERSION}"
    )
    log.info(
        "  V7 PF/SL guards : "
        f"daily_loss_brake={'ENABLED' if DAILY_LOSS_BRAKE_ENABLED else 'DISABLED'} "
        f"at -{_fmt_rs(DAILY_LOSS_BRAKE_RS)} | "
        f"C_OR_BREAKOUT_time_stop={'ENABLED' if C_OR_BREAKOUT_TIME_STOP_ENABLED else 'DISABLED'} "
        f"{C_OR_BREAKOUT_TIME_STOP_MIN}m | "
        f"C_OR_BREAKOUT_session_cap={'ENABLED' if C_OR_BREAKOUT_SESSION_CAP_ENABLED else 'DISABLED'} "
        f"{C_OR_BREAKOUT_SESSION_CAP}"
    )
    log.info("=" * 65)

    if args.entry_price_source == "ltp_on_signal" and not use_ltp:
        log.warning(
            "entry-price-source=ltp_on_signal with --no-ltp; using signal_bar fallback."
        )

    # Set up Kite for LTP if requested.
    # Do not disable use_ltp on startup auth failure; tokens may refresh later
    # and get_ltp() can recover automatically on-demand.
    if use_ltp:
        setup_kite_session(reason="startup", force=True)
        if kite is None:
            log.warning("LTP polling temporarily unavailable at startup (Kite session unavailable).")
            if args.entry_price_source == "ltp_on_signal":
                log.warning(
                    "entry-price-source=ltp_on_signal and Kite LTP is unavailable now; "
                    "fallback may be used until session recovers."
                )

    # Morning cleanup: keep today's paper-trade CSV strictly intraday.
    rows_before, rows_removed = _sanitize_today_paper_trade_csv()
    if rows_removed > 0:
        log.warning(
            f"[CSV] startup_cleanup removed stale rows from today's paper trade CSV: "
            f"{rows_removed}/{rows_before}"
        )

    # Load executed signals
    executed = load_executed_signals()
    log.info(f"Loaded {len(executed)} previously executed signals.")

    # Resolve today's signal CSVs (short + long direction)
    today_str = datetime.now(IST).strftime("%Y-%m-%d")
    csv_paths = get_signal_csv_paths_for_today()
    paper_csv_path = os.path.join(SIGNAL_DIR, PAPER_TRADE_LOG_PATTERN.format(today_str))
    log.info(
        "V7_ID_5MIN signal CSV sources: " + ", ".join(os.path.basename(p) for p in csv_paths)
    )

    restore_stats, resumed_signals = _restore_intraday_runtime_state(
        signal_csv_paths=csv_paths,
        paper_csv_path=paper_csv_path,
        executed=executed,
    )
    log.info(
        "[RESTORE] "
        f"signals_today={int(restore_stats['signals_today'])} | "
        f"executed_loaded={int(restore_stats['executed_loaded'])} | "
        f"closed_today={int(restore_stats['closed_today'])} | "
        f"open_restored={int(restore_stats['open_restored'])} | "
        f"restored_exact={int(restore_stats['restored_exact'])} | "
        f"restored_reconstructed={int(restore_stats['restored_reconstructed'])} | "
        f"realized_trades={int(restore_stats['realized_trades'])} | "
        f"realized_total={_fmt_rs_signed(restore_stats['realized_total'])} | "
        f"deployed_margin={_fmt_rs(restore_stats['deployed_margin'])}"
    )
    with executed_lock:
        executed_snapshot = set(executed)
    save_executed_signals(executed_snapshot)

    # Semaphore for concurrent trade limit (optional unlimited mode).
    trade_semaphore: Optional[threading.Semaphore] = None
    if args.max_trades > 0:
        trade_semaphore = threading.Semaphore(args.max_trades)
    else:
        log.warning("  Simulation cap  : DISABLED (unlimited concurrent workers)")

    resumed_started = start_resumed_trade_monitors(
        resumed_signals=resumed_signals,
        executed=executed,
        use_ltp=use_ltp,
        trade_semaphore=trade_semaphore,
        entry_price_source=args.entry_price_source,
    )
    if resumed_started > 0:
        log.info(f"[RESUME] Started {resumed_started} restored trade monitor(s).")

    # Callback for watchdog
    def on_csv_change():
        nonlocal executed
        log.info("Signal CSV changed - processing new signals from id_5min_v7_short + id_5min_v7_long...")
        executed = process_new_signals(
            csv_paths,
            executed,
            use_ltp,
            trade_semaphore,
            entry_price_source=args.entry_price_source,
        )

    # Set up watchdog
    os.makedirs(SIGNAL_DIR, exist_ok=True)
    handler = SignalCSVHandler(csv_paths, on_csv_change, debounce_sec=2.0)
    observer = Observer()
    observer.schedule(handler, path=SIGNAL_DIR, recursive=False)
    observer.start()
    log.info("Watchdog V7_ID_5MIN started - monitoring " + ", ".join(csv_paths))

    # Initial check for existing signals
    if any(os.path.exists(p) for p in csv_paths):
        log.info("Checking for existing unprocessed signals...")
        executed = process_new_signals(
            csv_paths,
            executed,
            use_ltp,
            trade_semaphore,
            entry_price_source=args.entry_price_source,
        )

    live_pnl_interval = max(5, LIVE_PNL_LOG_INTERVAL_SEC)
    next_live_pnl_log_ts = 0.0
    _log_live_pnl_snapshot(use_ltp, source="startup")

    # Main loop
    try:
        while True:
            now = datetime.now(IST)
            now_ts = time.time()

            if now_ts >= next_live_pnl_log_ts:
                _log_live_pnl_snapshot(use_ltp, source="heartbeat")
                next_live_pnl_log_ts = now_ts + live_pnl_interval

            # Check if market is still open
            if now.time() > MARKET_CLOSE:
                # Wait for all active trades to complete
                with active_trades_lock:
                    remaining = len(active_trades)
                if remaining > 0:
                    log.info(f"Market closed. Waiting for {remaining} active sim(s)...")
                    time.sleep(10)
                else:
                    log.info("Market closed. All simulations complete.")
                    break

            time.sleep(1)

    except KeyboardInterrupt:
        log.info("Received interrupt. Shutting down...")
    finally:
        observer.stop()
        observer.join()
        with executed_lock:
            executed_snapshot = set(executed)
        save_executed_signals(executed_snapshot)

        # Print daily summary
        with daily_pnl_lock:
            wr = daily_pnl["wins"] / daily_pnl["trades"] * 100 if daily_pnl["trades"] > 0 else 0
            log.info("=" * 55)
            log.info("DAILY PAPER TRADE SUMMARY")
            log.info(f"  Total trades : {daily_pnl['trades']}")
            log.info(f"  Wins         : {daily_pnl['wins']}")
            log.info(f"  Losses       : {daily_pnl['losses']}")
            log.info(f"  Win rate     : {wr:.1f}%")
            log.info(f"  Gross P&L    : Rs.{daily_pnl.get('gross_total', 0.0):+,.2f}")
            log.info(f"  Total cost   : Rs.{daily_pnl.get('total_cost', 0.0):,.2f}")
            log.info(f"  Net profit   : Rs.{daily_pnl.get('net_profit', 0.0):+,.2f}")
            log.info(f"  Net loss     : Rs.{daily_pnl.get('net_loss', 0.0):+,.2f}")
            log.info(f"  Net P&L      : Rs.{daily_pnl['total']:+,.2f}")
            log.info("=" * 55)

        log.info("Paper trade executor stopped.")


if __name__ == "__main__":
    main()





