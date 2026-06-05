# -*- coding: utf-8 -*-
"""
avwap_trade_execution_PAPER_TRADE_FALSE_id_5min_v7.py - Live Trade Executor (Zerodha Real, V7 ID 5-min)
=====================================================================================================
Watches the V7 ID 5-min signal CSVs and places REAL orders on Zerodha via KiteConnect.
See EQIDV2_V7_ID_5MIN_PIPELINE_REFERENCE.txt Section 10 (EXECUTOR — LIVE) for specification.

For each new signal:
  1. Places a MARKET entry order (MIS product for intraday)
  2. Waits for fill confirmation
  3. Places a LIMIT target order + SL-M stop-loss order
  4. Monitors target/SL in a background thread
  5. If either fills -> cancels the other (with retry)
  6. Forces close at 15:20 IST if neither is hit

Concurrent trades run in parallel threads (up to MAX_CONCURRENT_TRADES).

Output:
  - live_signals/live_trades_YYYY-MM-DD_id_5min_v7.csv        (detailed trade log)
  - live_signals/live_trade_summary_id_5min_v7.json           (running P&L summary)
  - live_signals/open_live_trades_state_YYYY-MM-DD_id_5min_v7.json (crash recovery state)

Safety features:
  - Signal deduplication via signal_id tracking
  - Order cancellation with retries on failure
  - Forced position closure at 15:20 IST
  - Thread-safe state management
  - Comprehensive logging
  - Graceful shutdown on Ctrl+C

Usage:
    python avwap_trade_execution_PAPER_TRADE_FALSE_v15.py
    python avwap_trade_execution_PAPER_TRADE_FALSE_v15.py --max-trades 10
    python avwap_trade_execution_PAPER_TRADE_FALSE_v15.py --dry-run  # validate without trading
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import os
import sys
import tempfile
import threading
import time
import traceback
from dataclasses import dataclass
from datetime import date, datetime, timedelta, time as dt_time
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

import socket as _socket
import http.client as http_client

# Force IPv4 for all outbound connections (avoids Kite IP-whitelist failures on
# dynamic IPv6 addresses assigned by the ISP).
_orig_getaddrinfo = _socket.getaddrinfo
def _ipv4_only_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):  # noqa: A002
    return _orig_getaddrinfo(host, port, _socket.AF_INET, type, proto, flags)
_socket.getaddrinfo = _ipv4_only_getaddrinfo

import pandas as pd
import pytz
import requests
import urllib3
from eqidv2_runtime_paths import LIVE_SIGNALS_DIR as RUNTIME_LIVE_SIGNALS_DIR

try:
    from kiteconnect import KiteConnect
except ImportError:
    print("ERROR: kiteconnect package required. Install with: pip install kiteconnect")
    sys.exit(1)

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
SIGNAL_CSV_PATTERNS = ("signals_{}_id_5min_v7_short.csv", "signals_{}_id_5min_v7_long.csv")
TRADE_LOG_PATTERN = "live_trades_{}_id_5min_v7.csv"
LIVE_TRADE_EXEC_LOG_PATTERN = "live_trade_execution_{}_id_5min_v7.log"
EXECUTED_SIGNALS_FILE = os.path.join(SIGNAL_DIR, "executed_signals_live_id_5min_v7.json")
MIS_REJECTED_SYMBOLS_FILE = os.path.join(SIGNAL_DIR, "mis_rejected_symbols_id_5min_v7.json")
SUMMARY_FILE = os.path.join(SIGNAL_DIR, "live_trade_summary_id_5min_v7.json")
OPEN_TRADES_STATE_PATTERN = "open_live_trades_state_{}_id_5min_v7.json"
KILL_SWITCH_COMMAND_FILE = os.path.join(SIGNAL_DIR, "kill_switch_false_id_5min_v7.json")

# Trading hours
MARKET_OPEN = dt_time(9, 15)
MARKET_CLOSE = dt_time(15, 30)
FORCED_CLOSE_TIME = dt_time(15, 20)  # aligned closer to backtest EOD; safe before broker auto-square-off

# Order monitoring
ORDER_POLL_SEC = 1                      # per-cycle order book poll interval (spec: Section 10)
# Mo5 (2026-04-22): bumped default 60→90 via env override. 60s was tight for
# rate-limit retries on partially-filled orders on illiquid names; 90s gives
# the order book cache + rate-limit backoff headroom before we cancel.
FILL_WAIT_TIMEOUT_SEC = max(30, int(os.getenv("EQIDV2_FILL_WAIT_TIMEOUT_SEC", "90")))
MAX_CANCEL_RETRIES = 3
CANCEL_RETRY_WAIT_SEC = 2
ENTRY_RETRY_ATTEMPTS = max(1, int(os.getenv("EQIDV2_ENTRY_RETRY_ATTEMPTS", "2")))
# Mo5 (2026-04-22): inter-retry delay between cancel-then-reissue cycles in
# entry retry loop. 0 caused back-to-back place_order calls that sometimes
# tripped Kite's per-second rate limit when many names retried in the same
# 5-min slot. 1s gives enough spacing while staying well under fill timeout.
ENTRY_RETRY_DELAY_SEC = max(0.0, float(os.getenv("EQIDV2_ENTRY_RETRY_DELAY_SEC", "1.0")))
ORDER_BOOK_CACHE_TTL_SEC = float(os.getenv("EQIDV2_ORDER_BOOK_CACHE_TTL_SEC", "1.0"))
ORDER_POLL_RATE_LIMIT_BACKOFF_SEC = float(
    os.getenv("EQIDV2_ORDER_POLL_RATE_LIMIT_BACKOFF_SEC", "2.0")
)
# 2026-04-24: Kite's per-second order budget (~10/s) can be exhausted when many
# entries fire in the same 5-min slot; exit legs (target LIMIT + SL SLM) and
# the safety market-close then 429 and leave positions naked. Retry with
# exponential backoff on rate-limit errors so protective orders still reach
# the broker.
EXIT_LEG_RETRY_ATTEMPTS = max(1, int(os.getenv("EQIDV2_EXIT_LEG_RETRY_ATTEMPTS", "6")))
EXIT_LEG_RETRY_BASE_BACKOFF_SEC = max(
    0.1, float(os.getenv("EQIDV2_EXIT_LEG_RETRY_BASE_BACKOFF_SEC", "0.5"))
)
EXIT_LEG_RETRY_MAX_BACKOFF_SEC = max(
    EXIT_LEG_RETRY_BASE_BACKOFF_SEC,
    float(os.getenv("EQIDV2_EXIT_LEG_RETRY_MAX_BACKOFF_SEC", "4.0")),
)
GLOBAL_EOD_SWEEP_INTERVAL_SEC = max(
    1.0,
    float(os.getenv("EQIDV2_GLOBAL_EOD_SWEEP_INTERVAL_SEC", "5.0")),
)

# Exposure limits
RISK_LIMITS_ENABLED = str(os.getenv("EQIDV2_ENABLE_RISK_LIMITS", "1")).strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
# M2 (2026-04-22): both caps aligned to bound concurrency so a missing env var
# cannot silently unlock unbounded execution. Defaults bumped 10 -> 20 on
# 2026-04-24 at user request. Bats set EQIDV2_MAX_OPEN_POSITIONS and
# EQIDV2_MAX_CONCURRENT_TRADES explicitly; keep env/code/CLI aligned.
MAX_OPEN_POSITIONS = int(os.getenv("EQIDV2_MAX_OPEN_POSITIONS", "20"))            # max simultaneous open positions
MAX_CAPITAL_DEPLOYED_RS = float(os.getenv("EQIDV2_MAX_CAPITAL_DEPLOYED_RS", "500000"))   # max total margin
INTRADAY_LEVERAGE = 5.0             # MIS leverage on Zerodha

# Concurrency
# 0 or negative means unlimited worker threads (no executor-side cap).
MAX_CONCURRENT_TRADES = int(os.getenv("EQIDV2_MAX_CONCURRENT_TRADES", "20"))

# ---------------------------------------------------------------------------
# v8 research parity
#
# The 1-minute entry engine writes stop/target from v6.SETUP_EXIT_RULES, which
# is what v8 research resolves with. Keep this empty so the executor honors
# those upstream prices without applying older CAND-E4 overrides.
# ---------------------------------------------------------------------------
CANDIDATE_E4_SL_TGT = {}


def _candE4_per_setup_sl_tgt(side, setup, entry_price, csv_stop, csv_target):
    """Look up Cand-E4 (SL%, TGT%) for the (side, setup) pair and recompute
    stop/target prices from the signal's entry_price. Returns
    (stop_price, target_price, applied) where applied is True if the dict
    contained the (side, setup) and the override took effect.
    Caller falls back to csv_stop/csv_target on miss or invalid entry."""
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
#
# G1: reject the signal if the current IST time is outside the side's
#     allowed entry window (LONG 09:15-13:00, SHORT 09:15-14:00). Stateless.
#
# G2: hard cap on entries placed per side per IST trading day
#     (LONG 15/day, SHORT 10/day). Counter persisted to JSON in SIGNAL_DIR
#     so it survives executor restarts mid-day. Increment is atomic with
#     the existing capital reservation.
#
# All env vars are optional overrides; defaults match the backtest spec.
# ---------------------------------------------------------------------------
CANDE4_LONG_TIME_WINDOW_LO  = float(os.getenv("EQIDV2_CANDE4_LONG_WINDOW_LO",  "9.15"))
CANDE4_LONG_TIME_WINDOW_HI  = float(os.getenv("EQIDV2_CANDE4_LONG_WINDOW_HI",  "15.50"))
CANDE4_SHORT_TIME_WINDOW_LO = float(os.getenv("EQIDV2_CANDE4_SHORT_WINDOW_LO", "9.15"))
CANDE4_SHORT_TIME_WINDOW_HI = float(os.getenv("EQIDV2_CANDE4_SHORT_WINDOW_HI", "15.50"))
CANDE4_MAX_LONG_PER_DAY     = int(os.getenv("EQIDV2_CANDE4_MAX_LONG_PER_DAY",  "999999"))
CANDE4_MAX_SHORT_PER_DAY    = int(os.getenv("EQIDV2_CANDE4_MAX_SHORT_PER_DAY", "999999"))
CANDE4_G2_COUNTERS_FILE     = os.path.join(SIGNAL_DIR, "candE4_g2_counters_id_5min_v7.json")

_candE4_g2_lock     = threading.Lock()
_candE4_g2_counters = {"date": "", "long": 0, "short": 0}


def _candE4_g1_time_window_check(side):
    """G1: reject if `now_ist` (current IST time) is outside the side's
    intraday entry window. Returns (allowed, reason)."""
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
    """Load persisted counter file; reset to zero if date mismatches today."""
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
        _atomic_write_json(CANDE4_G2_COUNTERS_FILE, dict(_candE4_g2_counters))
    except Exception:
        pass  # non-fatal -- counter still in memory


def _candE4_g2_check_and_increment(side):
    """G2: atomically check daily side cap and increment on accept.
    Returns (allowed, reason). The increment persists to disk so it survives
    executor restarts within the same trading day."""
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
            return (
                False,
                f"candE4_G2_daily_cap {s} reached ({cur}/{cap})",
            )
        _candE4_g2_counters[side_key] = cur + 1
        _candE4_g2_persist()
    return True, "ok"
# Conservative fallback so prices remain valid for common NSE 0.10/0.05 tick scripts.
DEFAULT_TICK_SIZE = float(os.getenv("EQIDV2_DEFAULT_TICK_SIZE", "0.10"))

# Max entry slip gate: reject LONG signals where live LTP is more than this
# fraction above the model trigger.  Set 0.0 to disable.
LONG_MAX_ENTRY_SLIP_PCT = float(os.getenv("EQIDV2_LONG_MAX_ENTRY_SLIP_PCT", "0.003"))
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
# Fix #20 (post-2026-04-21): stale-detection guard — reject confirmations where
# Stage 2 detected_time_ist lags signal_entry_datetime_ist by more than this
# many seconds. Prevents backlog-burst confirmations from being traded as fresh.
LATE_DETECTION_GUARD_ENABLE = str(os.getenv("EQIDV2_LATE_DETECTION_GUARD_ENABLE", "1")).strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
LATE_DETECTION_MAX_LAG_SEC = int(os.getenv("EQIDV2_LATE_DETECTION_MAX_LAG_SEC", "75"))
# Historical setup thresholds are retained only as lower optional limits. The
# environment limit is the universal hard ceiling for every setup.
_LATE_LAG_THRESHOLDS_BY_SETUP: Dict[str, int] = {
    "A_MOD_BREAK_C1_HIGH":              480,
    "A_MOD_BREAK_C1_LOW":               480,
    "A_MOD_CLOSE_CONTINUATION_BREAK":   780,
    "B_HUGE_C1_CLOSE_RECLAIM_BREAK":    780,
    "A_PULLBACK_C2_THEN_BREAK_C2_HIGH": 780,
    "A_PULLBACK_C2_THEN_BREAK_C2_LOW":  780,
    "B_HUGE_RED_FAILED_BOUNCE":         900,
}

def _late_lag_threshold_for_setup(setup: Optional[str]) -> int:
    setup_threshold = _LATE_LAG_THRESHOLDS_BY_SETUP.get(
        str(setup or "").upper().strip(),
        LATE_DETECTION_MAX_LAG_SEC,
    )
    return min(setup_threshold, LATE_DETECTION_MAX_LAG_SEC)

# Tier-1 fix: late-skip visibility — append every stale-detection skip to a
# daily CSV so the loud-not-silent invariant survives log rotation, and bump a
# heartbeat counter so the supervisor can see the count without parsing logs.
_LATE_SKIPPED_LOCK = threading.Lock()
_late_skipped_count = 0
# NEW-P13 (2026-04-22): pre-trade volume sanity gate. If intended quantity
# would consume more than VOLUME_GATE_MAX_PARTICIPATION_PCT of the trigger
# bar's traded volume, reduce the order to that fraction. If reduction would
# drive qty below 1, skip the trade (ENTRY_SKIPPED_THIN_LIQUIDITY). The gate
# requires the upstream signal to carry a `bar_volume` (or `c2_volume`) field;
# if neither is present the gate logs once-per-symbol-per-day ADVISORY and
# becomes a no-op so an upstream regression cannot silently disable it.
VOLUME_GATE_ENABLE = str(os.getenv("EQIDV2_VOLUME_GATE_ENABLE", "1")).strip().lower() in {
    "1", "true", "yes", "on",
}
VOLUME_GATE_MAX_PARTICIPATION_PCT = max(
    0.0001, float(os.getenv("EQIDV2_VOLUME_GATE_MAX_PARTICIPATION_PCT", "0.05"))
)
# strategy_v2 §J3 fix #1 — executor sleeps until the deterministic bar-open
# entry slot before calling execute_live_trade, so live trades align with the
# same bar-open that backtests assume. Cap the wait so an orphan/future-dated
# signal never blocks a thread beyond this budget.
ENTRY_SLOT_MAX_WAIT_SEC = max(0, int(os.getenv("EQIDV2_ENTRY_SLOT_MAX_WAIT_SEC", "300")))
TRANSIENT_API_RETRY_ATTEMPTS = max(1, int(os.getenv("EQIDV2_TRANSIENT_API_RETRY_ATTEMPTS", "4")))
TRANSIENT_API_RETRY_BASE_SEC = max(0.25, float(os.getenv("EQIDV2_TRANSIENT_API_RETRY_BASE_SEC", "1.0")))
TRANSIENT_ENTRY_ORDER_RECOVER_LOOKBACK_SEC = max(
    1.0,
    float(os.getenv("EQIDV2_TRANSIENT_ENTRY_ORDER_RECOVER_LOOKBACK_SEC", "15.0")),
)
MAX_TICK_DECIMALS = 4


def _optional_int_env(name: str, default: int) -> Optional[int]:
    raw = str(os.getenv(name, str(default))).strip()
    if raw.lower() in {"", "none", "off", "disable", "disabled"}:
        return None
    return int(raw)


MARKET_ORDER_PROTECTION = _optional_int_env("EQIDV2_MARKET_ORDER_PROTECTION", -1)
SLM_ORDER_PROTECTION = _optional_int_env("EQIDV2_SLM_ORDER_PROTECTION", -1)




def _entry_price_within_retry_band(side: str, signal_entry_price: float, live_price: float) -> bool:
    if signal_entry_price <= 0 or live_price <= 0:
        return False
    band = max(0.0, float(ENTRY_RETRY_NEAR_ENTRY_PCT))
    if side == "LONG":
        return live_price <= signal_entry_price * (1.0 + band)
    return live_price >= signal_entry_price * (1.0 - band)


def _is_transient_kite_connection_error(error: Exception) -> bool:
    if isinstance(
        error,
        (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            urllib3.exceptions.ProtocolError,
            urllib3.exceptions.ReadTimeoutError,
            http_client.RemoteDisconnected,
            TimeoutError,
            ConnectionResetError,
            BrokenPipeError,
        ),
    ):
        return True

    text = _sanitize_error_message(error, limit=500).lower()
    return any(
        token in text
        for token in (
            "connection aborted",
            "remote end closed connection without response",
            "remotedisconnected",
            "protocolerror",
            "connection reset by peer",
            "max retries exceeded",
            "read timed out",
            "timeout",
            "temporarily unavailable",
        )
    )


def _transient_retry_sleep(attempt: int, retry_until_ist: Optional[datetime]) -> float:
    base = max(0.25, float(TRANSIENT_API_RETRY_BASE_SEC))
    sleep_sec = base * max(1, attempt)
    if retry_until_ist is not None:
        remaining = max(0.0, (retry_until_ist - datetime.now(IST)).total_seconds())
        sleep_sec = min(sleep_sec, remaining)
    return max(0.0, sleep_sec)


def _entry_retry_deadline(
    signal: dict,
    trade_start_ist: datetime,
    forced_close_dt: datetime,
) -> datetime:
    # strategy_v2 §J3 fix #8 — anchor the retry window to whichever is later:
    # the executor start (trade_start_ist, which is POST any §J3 #1 bar-open
    # sleep) or the model entry slot. Pre-§J3 the anchor was detected_time_ist,
    # which silently chopped the §J3 #1 sleep duration (up to ~5 min) off the
    # ENTRY_RETRY_WAIT_SEC budget. For rows with no entry_datetime (legacy /
    # malformed signals), fall back to the previous detected/received chain so
    # we never rebase onto a stale model slot that fires ENTRY_SKIPPED_STALE
    # on all early-session trades.
    entry_slot_ts = _parse_ist_signal_ts(
        signal.get("signal_entry_datetime_ist")
        or signal.get("signal_bar_time_ist")
        or signal.get("bar_time_ist")
    )
    wait = max(1, ENTRY_RETRY_WAIT_SEC)
    if entry_slot_ts is not None:
        entry_slot_dt = entry_slot_ts.to_pydatetime()
        anchor = trade_start_ist if trade_start_ist >= entry_slot_dt else entry_slot_dt
        candidate = anchor + timedelta(seconds=wait)
    else:
        base_ts = _parse_ist_signal_ts(
            signal.get("detected_time_ist") or signal.get("logtime_ist")
        )
        if base_ts is None:
            base_ts = _parse_ist_signal_ts(signal.get("received_time"))
        if base_ts is None:
            base_ts = _parse_ist_signal_ts(signal.get("signal_datetime"))
        if base_ts is None:
            candidate = trade_start_ist + timedelta(seconds=wait)
        else:
            candidate = base_ts.to_pydatetime() + timedelta(seconds=wait)
    return min(candidate, forced_close_dt)


def _trade_started_after_entry_deadline(
    trade_start_ist: Optional[datetime],
    entry_retry_deadline: Optional[datetime],
) -> bool:
    if trade_start_ist is None or entry_retry_deadline is None:
        return False
    return trade_start_ist >= entry_retry_deadline


def _detection_lag_seconds(signal: dict) -> Optional[float]:
    """Seconds between the model entry slot and Stage 2 detection.

    Positive = detected after the intended entry bar. Used by the executor
    stale-guard to reject backlog-burst confirmations that would otherwise
    look fresh based on detected_time_ist alone.
    """
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
    """Append one row to late_skipped_<date>_id_5min_v7.csv and bump heartbeat counter."""
    global _late_skipped_count
    try:
        date_str = datetime.now(IST).strftime("%Y-%m-%d")
        path = Path(SIGNAL_DIR) / f"late_skipped_{date_str}_id_5min_v7.csv"
        row = {
            "skipped_at_ist": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S%z"),
            "ticker":         str(signal.get("ticker", "")),
            "side":           str(signal.get("side", "")),
            "setup":          str(signal.get("setup", "")),
            "signal_id":      str(signal.get("signal_id", "")),
            "signal_bar":     str(signal.get("signal_time_ist") or signal.get("signal_bar_time_ist") or ""),
            "entry_slot":     str(signal.get("signal_entry_datetime_ist") or ""),
            "detected_time":  str(signal.get("detected_time_ist") or ""),
            "lag_sec":        f"{lag_sec:.1f}",
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


def _safe_get_entry_ltp(
    ticker: str,
    *,
    retry_until_ist: datetime,
    context: str,
) -> Optional[float]:
    attempt = 0
    last_error: Optional[Exception] = None
    while datetime.now(IST) <= retry_until_ist:
        attempt += 1
        try:
            return float(get_ltp(ticker))
        except Exception as exc:
            if not _is_transient_kite_connection_error(exc):
                raise
            last_error = exc
            sleep_sec = _transient_retry_sleep(attempt, retry_until_ist)
            if sleep_sec <= 0:
                break
            log.warning(
                f"[KITE.NET.RETRY] {context}: transient quote error for {ticker} "
                f"(attempt {attempt}): {_sanitize_error_message(exc, limit=180)} "
                f"| retrying in {sleep_sec:.1f}s"
            )
            time.sleep(sleep_sec)
    if last_error is not None:
        log.warning(
            f"[KITE.NET.RETRY] {context}: quote retry window expired for {ticker} "
            f"({_sanitize_error_message(last_error, limit=180)})"
        )
    return None


def _wait_for_near_entry_price(
    ticker: str,
    side: str,
    signal_entry_price: float,
    retry_until_ist: datetime,
) -> Optional[float]:
    last_ltp: Optional[float] = None
    poll_sec = max(1.0, float(ENTRY_RETRY_POLL_SEC))
    while datetime.now(IST) <= retry_until_ist:
        ltp_now = _safe_get_entry_ltp(
            ticker,
            retry_until_ist=retry_until_ist,
            context=f"{ticker} near-entry watch",
        )
        if ltp_now is not None and ltp_now > 0:
            last_ltp = float(ltp_now)
            if _entry_price_within_retry_band(side, signal_entry_price, last_ltp):
                return last_ltp
        time.sleep(poll_sec)
    return last_ltp
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

# Trade log columns
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
    "filled_price",
    "exit_price",
    "stop_price",
    "target_price",
    "outcome",
    "pnl_rs",
    "pnl_pct",
    "entry_order_id",
    "target_order_id",
    "sl_order_id",
    "close_order_id",
    "quality_score",
]
NON_TERMINAL_OUTCOMES = {
    "",
    "ENTRY_PENDING",
    "ENTRY_PENDING_RECOVERED",
    "ENTRY_SUBMITTED",
    "ENTRY_OPEN",
    "OPEN",
}

# Signal CSV columns
SIGNAL_COLUMNS = [
    "signal_id", "signal_datetime", "received_time", "ticker", "side",
    "setup", "impulse_type", "entry_price", "stop_price", "target_price",
    "quality_score", "atr_pct", "rsi", "adx", "quantity",
    "signal_entry_datetime_ist", "signal_bar_time_ist",
]

# Column name mapping: signal generator CSV name -> executor expected name
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

# Fallback margin capital when the signal row omits quantity.
DEFAULT_POSITION_SIZE = float(
    os.getenv(
        "EQIDV7_ID_5MIN_DEFAULT_POSITION_SIZE_RS",
        os.getenv("EQIDV7_ID_5MIN_DEFAULT_POSITION_SIZE_RS", "10000"),
    )
)
# Leave unset so quantity is taken from the signal row when present.
_FORCE_ENTRY_QUANTITY_RAW = str(os.getenv("EQIDV7_ID_5MIN_FORCE_ENTRY_QUANTITY", "")).strip()
try:
    FORCE_ENTRY_QUANTITY: Optional[int] = (
        max(1, int(_FORCE_ENTRY_QUANTITY_RAW)) if _FORCE_ENTRY_QUANTITY_RAW else None
    )
except Exception:
    FORCE_ENTRY_QUANTITY = None

# ============================================================================
# LOGGING
# ============================================================================
def setup_logging() -> logging.Logger:
    logger = logging.getLogger("live_trade")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s")

    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    os.makedirs(SIGNAL_DIR, exist_ok=True)
    today_str = datetime.now(IST).strftime("%Y-%m-%d")
    fh = logging.FileHandler(
        os.path.join(SIGNAL_DIR, LIVE_TRADE_EXEC_LOG_PATTERN.format(today_str)),
        mode="a",
        encoding="utf-8",
    )
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger


log = setup_logging()


# ============================================================================
# KITE SESSION
# ============================================================================
kite: Optional[KiteConnect] = None
kite_pool: List[KiteConnect] = []
kite_session_names: Dict[int, str] = {}
kite_primary_session_name = "N/A"
kite_pool_lock = threading.Lock()
kite_pool_rr_idx = 0
symbol_tick_size_cache: Dict[str, float] = {}
symbol_tick_size_loaded = False
symbol_tick_size_lock = threading.Lock()


def _try_build_kite_client(key_file: str, token_file: str) -> tuple:
    """Returns (KiteConnect client, user_name) or raises on failure."""
    with open(key_file, "r", encoding="utf-8") as f_key:
        key_data = f_key.read().strip().split()
    with open(token_file, "r", encoding="utf-8") as f_tok:
        access_token = f_tok.read().strip()
    if len(key_data) < 2:
        raise ValueError(f"{key_file} must contain 'API_KEY API_SECRET' separated by space.")
    api_key = key_data[0]
    client = KiteConnect(api_key=api_key)
    client.set_access_token(access_token)
    profile = client.profile()
    user_name = profile.get("user_name", "N/A")
    return client, user_name


def _refresh_failed_sessions_bg(
    failed_specs: list,
    retry_every_sec: int = 120,
    max_attempts: int = 10,
) -> None:
    """Background thread: retry failed Kite app sessions and add them to the pool."""
    for attempt in range(1, max_attempts + 1):
        time.sleep(retry_every_sec)
        if not failed_specs:
            break
        remaining = []
        for app_name, key_file, token_file in failed_specs:
            if not (os.path.exists(key_file) and os.path.exists(token_file)):
                remaining.append((app_name, key_file, token_file))
                continue
            try:
                client, user_name = _try_build_kite_client(key_file, token_file)
                with kite_pool_lock:
                    kite_pool.append(client)
                    kite_session_names[id(client)] = app_name
                log.info(
                    f"[KITE.REFRESH] {app_name} session recovered "
                    f"(attempt {attempt}/{max_attempts}) | user={user_name}"
                )
            except Exception as e:
                log.warning(
                    f"[KITE.REFRESH] {app_name} retry failed "
                    f"(attempt {attempt}/{max_attempts}): {e}"
                )
                remaining.append((app_name, key_file, token_file))
        failed_specs = remaining
        if not failed_specs:
            log.info(f"[KITE.REFRESH] All failed sessions recovered after {attempt} attempt(s).")
            break


def setup_kite_session() -> KiteConnect:
    """Set up and return a KiteConnect primary session, with optional app pool."""
    global kite, kite_pool, kite_pool_rr_idx, kite_session_names, kite_primary_session_name

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

    sessions: List[KiteConnect] = []
    session_names: List[str] = []
    failed_auth_specs: list = []
    primary_user = "N/A"
    primary_app = "N/A"

    for app_name, key_file, token_file in specs:
        if not (os.path.exists(key_file) and os.path.exists(token_file)):
            if app_name == "app1":
                log.error(
                    f"Primary credentials missing: {key_file} and/or {token_file}."
                )
            else:
                log.warning(f"[KITE] {app_name} skipped (missing {key_file}/{token_file}).")
            continue

        try:
            client, user_name = _try_build_kite_client(key_file, token_file)
            if primary_app == "N/A":
                primary_app = app_name
                primary_user = user_name
            sessions.append(client)
            session_names.append(app_name)
            log.info(f"[KITE] Session ready: {app_name} | user={user_name}")
        except Exception as e:
            if app_name == "app1":
                log.warning(
                    f"[KITE] Primary session setup failed ({app_name}): {e}. "
                    "Continuing with fallback apps."
                )
            else:
                log.warning(f"[KITE] {app_name} disabled due to setup error: {e}")
            failed_auth_specs.append((app_name, key_file, token_file))

    if not sessions:
        raise RuntimeError(
            "No valid Kite sessions available. Check api_key/access_token files."
        )

    kite = sessions[0]
    with kite_pool_lock:
        kite_pool = list(sessions)
        kite_session_names = {id(client): name for client, name in zip(sessions, session_names)}
        kite_pool_rr_idx = 0
    kite_primary_session_name = primary_app

    log.info(f"Kite primary session established: {primary_app} | user={primary_user}")
    log.info(f"[KITE] Session pool active apps: {len(sessions)}")

    # Retry failed apps in background — tokens may not be refreshed at market open yet
    if failed_auth_specs:
        t = threading.Thread(
            target=_refresh_failed_sessions_bg,
            args=(failed_auth_specs,),
            kwargs={"retry_every_sec": 120, "max_attempts": 10},
            daemon=True,
            name="kite-session-refresh",
        )
        t.start()
        log.info(
            f"[KITE.REFRESH] Background session refresh started for "
            f"{len(failed_auth_specs)} app(s): {[s[0] for s in failed_auth_specs]}"
        )
    return kite


def _next_kite_for_reads() -> KiteConnect:
    """Round-robin read client to spread polling load across app sessions."""
    global kite_pool_rr_idx
    with kite_pool_lock:
        if kite_pool:
            idx = kite_pool_rr_idx % len(kite_pool)
            kite_pool_rr_idx = (kite_pool_rr_idx + 1) % len(kite_pool)
            return kite_pool[idx]
    if kite is None:
        raise RuntimeError("Kite session not initialized")
    return kite


def _iter_kite_write_clients() -> List[Tuple[str, KiteConnect]]:
    with kite_pool_lock:
        pool = list(kite_pool)
        names = dict(kite_session_names)

    clients: List[Tuple[str, KiteConnect]] = []
    seen: Set[int] = set()
    ordered = ([kite] if kite is not None else []) + pool
    for client in ordered:
        if client is None:
            continue
        client_id = id(client)
        if client_id in seen:
            continue
        seen.add(client_id)
        clients.append((names.get(client_id, f"session{len(clients) + 1}"), client))
    return clients


def get_ltp(ticker: str) -> float:
    """Get last traded price. Raises on failure."""
    reader = _next_kite_for_reads()
    data = reader.ltp(f"NSE:{ticker}")
    return float(data[f"NSE:{ticker}"]["last_price"])


def preload_symbol_tick_sizes(exchange: str = "NSE") -> None:
    """Load exchange tick-size metadata once and cache by tradingsymbol."""
    global symbol_tick_size_loaded

    if kite is None:
        return

    try:
        instruments = kite.instruments(exchange)
    except Exception as e:
        log.warning(
            f"[TICK] Could not load {exchange} instrument metadata: {e}. "
            f"Using default tick={DEFAULT_TICK_SIZE:.2f} until available."
        )
        return

    fresh: Dict[str, float] = {}
    for row in instruments:
        symbol = str(row.get("tradingsymbol") or "").upper()
        if not symbol:
            continue
        try:
            tick_size = float(row.get("tick_size", 0.0) or 0.0)
        except Exception:
            tick_size = 0.0
        if tick_size > 0:
            fresh[symbol] = tick_size

    with symbol_tick_size_lock:
        if fresh:
            symbol_tick_size_cache.update(fresh)
        symbol_tick_size_loaded = True

    if fresh:
        log.info(f"[TICK] Loaded tick sizes for {len(fresh)} {exchange} symbols.")
    else:
        log.warning(
            f"[TICK] {exchange} instrument metadata had no tick sizes. "
            f"Using default tick={DEFAULT_TICK_SIZE:.2f}."
        )


def get_tick_size(ticker: str) -> float:
    """Resolve symbol tick size from cache with safe default fallback."""
    symbol = str(ticker or "").upper()
    if not symbol:
        return DEFAULT_TICK_SIZE

    with symbol_tick_size_lock:
        cached = symbol_tick_size_cache.get(symbol)
        loaded = symbol_tick_size_loaded

    if cached and cached > 0:
        return float(cached)

    if not loaded:
        preload_symbol_tick_sizes("NSE")
        with symbol_tick_size_lock:
            cached = symbol_tick_size_cache.get(symbol)
        if cached and cached > 0:
            return float(cached)

    with symbol_tick_size_lock:
        symbol_tick_size_cache[symbol] = DEFAULT_TICK_SIZE

    log.warning(f"[TICK] Tick size missing for {symbol}; defaulting to {DEFAULT_TICK_SIZE:.2f}.")
    return DEFAULT_TICK_SIZE


def round_to_tick(price: float, tick: float = DEFAULT_TICK_SIZE, mode: str = "nearest") -> float:
    """
    Round price to tick size.
    mode:
      - "nearest": nearest tick
      - "up": ceil to next tick
      - "down": floor to previous tick
    """
    tick = float(tick) if tick else DEFAULT_TICK_SIZE
    if tick <= 0:
        tick = DEFAULT_TICK_SIZE
    ratio = float(price) / tick
    mode_key = str(mode or "nearest").strip().lower()
    if mode_key == "up":
        steps = math.ceil(ratio - 1e-12)
    elif mode_key == "down":
        steps = math.floor(ratio + 1e-12)
    else:
        steps = round(ratio)
    rounded = steps * tick
    tick_text = f"{tick:.8f}".rstrip("0").rstrip(".")
    decimals = len(tick_text.split(".")[1]) if "." in tick_text else 0
    decimals = min(MAX_TICK_DECIMALS, max(0, decimals))
    return round(rounded, decimals)


def _is_rate_limit_error(error: Exception) -> bool:
    text = _sanitize_error_message(error, limit=500).lower()
    return (
        "too many requests" in text
        or "rate limit" in text
        or "http 429" in text
        or " 429 " in f" {text} "
        or "maximum allowed order requests" in text
        or "maximum allowed requests per second" in text
    )


def _reset_orders_snapshot_cache() -> None:
    global orders_snapshot_ts_monotonic
    with orders_snapshot_lock:
        orders_snapshot_ts_monotonic = 0.0


def _order_row_timestamp_ist(row: Optional[dict]) -> Optional[datetime]:
    row = row or {}
    for key in ("exchange_update_timestamp", "exchange_timestamp", "order_timestamp"):
        value = row.get(key)
        if isinstance(value, datetime):
            return value.astimezone(IST) if value.tzinfo else IST.localize(value)
        if isinstance(value, str) and value.strip():
            try:
                ts = pd.to_datetime(value.strip(), errors="coerce")
                if pd.isna(ts):
                    continue
                ts = pd.Timestamp(ts)
                if ts.tzinfo is None:
                    ts = ts.tz_localize(IST)
                else:
                    ts = ts.tz_convert(IST)
                return ts.to_pydatetime()
            except Exception:
                continue
    return None


def _find_order_in_list(orders: Sequence[dict], order_id: str) -> Optional[dict]:
    for row in orders:
        if str(row.get("order_id", "")).strip() == str(order_id).strip():
            return row
    return None


def _order_state_from_row(row: Optional[dict]) -> Dict[str, Any]:
    row = row or {}
    return {
        "status": str(row.get("status", "")).strip().upper(),
        "average_price": _safe_float(row.get("average_price", 0.0), 0.0),
        "filled_quantity": _safe_int(row.get("filled_quantity", 0), 0),
    }


def _get_order_row(
    order_id: str,
    force_refresh: bool = False,
    allow_history_fallback: bool = False,
) -> Optional[dict]:
    """
    Fetch one order row by ID with optional history fallback.
    """
    try:
        orders = _get_orders_snapshot(force_refresh=force_refresh)
        row = _find_order_in_list(orders, order_id)
        if row:
            return row
    except Exception as e:
        if not _is_rate_limit_error(e):
            log.warning(f"[LIVE] Order lookup failed for {order_id}: {e}")

    if not allow_history_fallback:
        return None

    try:
        reader = _next_kite_for_reads()
        history = reader.order_history(order_id)
        if isinstance(history, list) and history:
            latest = history[-1]
            if isinstance(latest, dict):
                return latest
    except Exception as e:
        if not _is_rate_limit_error(e):
            log.warning(f"[LIVE] Order history lookup failed for {order_id}: {e}")

    return None


def _get_orders_snapshot(force_refresh: bool = False) -> List[dict]:
    """
    Return a shared broker orderbook snapshot.
    Serializes `kite.orders()` calls across all trade threads and reuses a
    short-lived cache to prevent API throttling under high concurrency.
    """
    global orders_snapshot_cache, orders_snapshot_ts_monotonic, orders_snapshot_rate_limited_until

    if kite is None:
        return []

    now = time.monotonic()
    with orders_snapshot_lock:
        has_cache = bool(orders_snapshot_cache)
        cache_age = (
            now - orders_snapshot_ts_monotonic
            if orders_snapshot_ts_monotonic > 0
            else float("inf")
        )
        cache_fresh = has_cache and (cache_age <= ORDER_BOOK_CACHE_TTL_SEC)

        if (not force_refresh) and cache_fresh:
            return list(orders_snapshot_cache)

        if (not force_refresh) and now < orders_snapshot_rate_limited_until and has_cache:
            return list(orders_snapshot_cache)

        try:
            reader = _next_kite_for_reads()
            rows = reader.orders()
            if not isinstance(rows, list):
                rows = []
            orders_snapshot_cache = list(rows)
            orders_snapshot_ts_monotonic = time.monotonic()
            orders_snapshot_rate_limited_until = 0.0
            return list(orders_snapshot_cache)
        except Exception as e:
            if _is_rate_limit_error(e):
                orders_snapshot_rate_limited_until = max(
                    orders_snapshot_rate_limited_until,
                    now + max(0.5, ORDER_POLL_RATE_LIMIT_BACKOFF_SEC),
                )
                if has_cache and (not force_refresh):
                    return list(orders_snapshot_cache)
            raise


# ============================================================================
# ORDER HELPERS
# ============================================================================
def cancel_order_safe(variety: str, order_id: str) -> bool:
    """Cancel an order with retries. Returns True on success."""
    for attempt in range(1, MAX_CANCEL_RETRIES + 1):
        try:
            kite.cancel_order(variety, order_id)
            _reset_orders_snapshot_cache()
            log.info(f"Cancelled order {order_id} (attempt {attempt})")
            return True
        except Exception as e:
            log.warning(f"Cancel attempt {attempt}/{MAX_CANCEL_RETRIES} for {order_id}: {e}")
            if attempt < MAX_CANCEL_RETRIES:
                time.sleep(CANCEL_RETRY_WAIT_SEC)
    log.error(f"FAILED to cancel order {order_id} after {MAX_CANCEL_RETRIES} attempts")
    return False


def wait_for_fill(order_id: str, timeout_sec: int = FILL_WAIT_TIMEOUT_SEC) -> Optional[float]:
    """
    Wait for an order to fill. Returns average fill price or None on timeout.
    """
    start = time.monotonic()
    while (time.monotonic() - start) < timeout_sec:
        try:
            row = _get_order_row(order_id, force_refresh=False, allow_history_fallback=False)
            if row:
                state = _order_state_from_row(row)
                status = state["status"]
                if status == "COMPLETE":
                    return float(state["average_price"] or 0.0)
                if status in {"REJECTED", "CANCELLED"}:
                    log.warning(f"Order {order_id} status: {status}")
                    return None
        except Exception as e:
            if _is_rate_limit_error(e):
                log.warning(
                    f"Order poll throttled for {order_id}; "
                    "using shared orderbook backoff."
                )
            else:
                log.warning(f"Error polling order {order_id}: {e}")
        time.sleep(ORDER_POLL_SEC)

    # One final forced refresh check before declaring timeout.
    try:
        row = _get_order_row(
            order_id,
            force_refresh=True,
            allow_history_fallback=True,
        )
        if row:
            state = _order_state_from_row(row)
            status = state["status"]
            if status == "COMPLETE":
                return float(state["average_price"] or 0.0)
            if status in {"REJECTED", "CANCELLED"}:
                log.warning(f"Order {order_id} status: {status}")
                return None
    except Exception as e:
        if not _is_rate_limit_error(e):
            log.warning(f"Final status check failed for {order_id}: {e}")

    log.warning(f"Order {order_id} did not fill within {timeout_sec}s")
    return None


def _place_exit_legs(
    ticker: str,
    exit_txn: str,
    quantity: int,
    target_price: float,
    stop_price: float,
) -> Tuple[str, str]:
    """
    Place target LIMIT + stop-loss SLM exit legs and return their order IDs.
    """
    tick_size = get_tick_size(ticker)
    exit_side = str(exit_txn or "").upper()
    if exit_side == "BUY":
        target_mode = "up"
        stop_mode = "up"
    else:
        target_mode = "down"
        stop_mode = "down"

    target_price = round_to_tick(target_price, tick=tick_size, mode=target_mode)
    stop_price = round_to_tick(stop_price, tick=tick_size, mode=stop_mode)

    target_order_id = ""
    try:
        target_order_id = _place_order_with_rate_limit_retry(
            f"{ticker} target leg",
            variety=kite.VARIETY_REGULAR,
            exchange=kite.EXCHANGE_NSE,
            tradingsymbol=ticker,
            transaction_type=exit_txn,
            quantity=quantity,
            product=kite.PRODUCT_MIS,
            order_type=kite.ORDER_TYPE_LIMIT,
            price=target_price,
            validity=kite.VALIDITY_DAY,
            tag="AVWAPTarget",
        )
        sl_order_id = _place_order_with_rate_limit_retry(
            f"{ticker} stop leg",
            variety=kite.VARIETY_REGULAR,
            exchange=kite.EXCHANGE_NSE,
            tradingsymbol=ticker,
            transaction_type=exit_txn,
            quantity=quantity,
            product=kite.PRODUCT_MIS,
            order_type=kite.ORDER_TYPE_SLM,
            trigger_price=stop_price,
            validity=kite.VALIDITY_DAY,
            tag="AVWAPStopLoss",
        )
        _reset_orders_snapshot_cache()
        return target_order_id, sl_order_id
    except Exception:
        if target_order_id:
            cancel_order_safe(kite.VARIETY_REGULAR, target_order_id)
        raise


# ============================================================================
# EXECUTED SIGNALS TRACKING
# ============================================================================
def load_executed_signals() -> Set[str]:
    if os.path.exists(EXECUTED_SIGNALS_FILE):
        try:
            with open(EXECUTED_SIGNALS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if data.get("date") != datetime.now(IST).strftime("%Y-%m-%d"):
                    return set()
                return set(data.get("signals", []))
        except (json.JSONDecodeError, KeyError):
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


def load_mis_rejected_symbols() -> Set[str]:
    if os.path.exists(MIS_REJECTED_SYMBOLS_FILE):
        try:
            with open(MIS_REJECTED_SYMBOLS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if data.get("date") != datetime.now(IST).strftime("%Y-%m-%d"):
                return set()
            return {str(x).upper() for x in data.get("symbols", []) if str(x).strip()}
        except (json.JSONDecodeError, KeyError):
            try:
                bad = MIS_REJECTED_SYMBOLS_FILE + ".corrupt"
                os.replace(MIS_REJECTED_SYMBOLS_FILE, bad)
            except Exception:
                pass
    return set()


def save_mis_rejected_symbols(symbols: Set[str]) -> None:
    payload = {
        "date": datetime.now(IST).strftime("%Y-%m-%d"),
        "symbols": sorted({str(s).upper() for s in symbols if str(s).strip()}),
    }
    _atomic_write_json(MIS_REJECTED_SYMBOLS_FILE, payload)


def hydrate_mis_rejected_symbols() -> int:
    loaded = load_mis_rejected_symbols()
    with mis_rejected_symbols_lock:
        mis_rejected_symbols.clear()
        mis_rejected_symbols.update(loaded)
        return len(mis_rejected_symbols)


def is_symbol_mis_rejected(ticker: str) -> bool:
    symbol = str(ticker or "").upper().strip()
    if not symbol:
        return False
    with mis_rejected_symbols_lock:
        return symbol in mis_rejected_symbols


def mark_symbol_mis_rejected(ticker: str, reason: str = "") -> None:
    symbol = str(ticker or "").upper().strip()
    if not symbol:
        return
    with mis_rejected_symbols_lock:
        if symbol in mis_rejected_symbols:
            return
        mis_rejected_symbols.add(symbol)
        snapshot = set(mis_rejected_symbols)
    save_mis_rejected_symbols(snapshot)
    reason_text = _sanitize_error_message(reason, limit=180)
    if reason_text:
        log.warning(f"[MIS] Marked {symbol} as MIS blocked/missing for today: {reason_text}")
    else:
        log.warning(f"[MIS] Marked {symbol} as MIS blocked/missing for today.")


# ============================================================================
# LIVE TRADE MANAGEMENT
# ============================================================================
@dataclass
class LiveTradeResult:
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
    filled_price: float = 0.0
    exit_price: float = 0.0
    stop_price: float = 0.0
    target_price: float = 0.0
    outcome: str = ""
    pnl_rs: float = 0.0
    pnl_pct: float = 0.0
    entry_order_id: str = ""
    target_order_id: str = ""
    sl_order_id: str = ""
    close_order_id: str = ""
    quality_score: float = 0.0


# Shared state
active_trades: Dict[str, threading.Thread] = {}
active_trades_lock = threading.Lock()
active_positions: Dict[str, Dict[str, Any]] = {}
active_positions_lock = threading.Lock()
inflight_signals: Set[str] = set()
inflight_signals_lock = threading.Lock()
executed_lock = threading.Lock()
mis_rejected_symbols: Set[str] = set()
mis_rejected_symbols_lock = threading.Lock()
closed_signal_ids_today: Set[str] = set()
closed_tickers_today: Set[str] = set()
# strategy_v2 §J3 fix #10 — transient admission set. Populated on dispatch to
# prevent two concurrent signals for the same ticker racing into the same
# slot, cleared when the worker thread exits. closed_tickers_today is the
# permanent "traded today" block and is now only populated after an entry
# fill is confirmed (or after a terminal skip that rules the ticker out for
# the rest of the session, e.g. MIS_BLOCKED / RISK_LIMIT / DRY_RUN).
dispatched_tickers: Set[str] = set()
closed_trades_lock = threading.Lock()
daily_pnl: Dict[str, Any] = {"total": 0.0, "wins": 0, "losses": 0, "trades": 0}
daily_pnl_lock = threading.Lock()
dispatch_lockdown_reason: Optional[str] = None
dispatch_lockdown_lock = threading.Lock()

# Capital / position tracking (margin, not notional - accounts for MIS leverage)
capital_deployed: Dict[str, float] = {}   # signal_id -> margin blocked
capital_lock = threading.Lock()
json_write_lock = threading.Lock()
trade_log_lock = threading.Lock()
orders_snapshot_lock = threading.Lock()
orders_snapshot_cache: List[Dict[str, Any]] = []
orders_snapshot_ts_monotonic = 0.0
orders_snapshot_rate_limited_until = 0.0
kill_switch_cache_lock = threading.Lock()
kill_switch_cache_mtime: float = -1.0
kill_switch_cache_payload: Optional[Dict[str, Any]] = None
global_eod_sweep_lock = threading.Lock()
global_eod_last_sweep_ts = 0.0

JSON_WRITE_RETRIES = 8
JSON_WRITE_RETRY_BASE_SEC = 0.05


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


def _clean_order_id(value: object) -> str:
    text = str(value if value is not None else "").strip()
    if not text or text.lower() == "nan":
        return ""
    if text.endswith(".0") and text[:-2].isdigit():
        return text[:-2]
    return text


def _calc_pnl(side: str, entry_price: float, exit_price: float, qty: int) -> Tuple[float, float]:
    if side.upper() == "SHORT":
        pnl_rs = (entry_price - exit_price) * qty
    else:
        pnl_rs = (exit_price - entry_price) * qty
    pnl_pct = (pnl_rs / (entry_price * qty) * 100) if (entry_price > 0 and qty > 0) else 0.0
    return float(pnl_rs), float(pnl_pct)


def _sanitize_error_message(message: object, limit: int = 240) -> str:
    text = " ".join(str(message).split())
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)] + "..."


def _is_order_write_ip_restriction_error(error: Exception) -> bool:
    text = _sanitize_error_message(error, limit=500).lower()
    return (
        "not allowed to place orders for this app" in text
        or "update allowed ips" in text
        or "allowed to place orders for this app" in text
    )


def _resolve_market_protection(place_kwargs: Dict[str, Any]) -> Optional[int]:
    explicit = place_kwargs.get("market_protection")
    if explicit is not None:
        return int(explicit)

    order_type = str(place_kwargs.get("order_type", "")).strip().upper()
    if order_type == "MARKET":
        return MARKET_ORDER_PROTECTION
    if order_type in {"SL-M", "SLM"}:
        return SLM_ORDER_PROTECTION
    return None


def _place_order_compat(client: KiteConnect, **place_kwargs: Any) -> str:
    market_protection = _resolve_market_protection(place_kwargs)
    if market_protection is None:
        return str(client.place_order(**place_kwargs))

    params = dict(place_kwargs)
    params["market_protection"] = int(market_protection)
    response = client._post(
        "order.place",
        url_args={"variety": params["variety"]},
        params=params,
    )
    return str(response["order_id"])


def _place_order_with_fallback(log_context: str, **place_kwargs: Any) -> str:
    global kite, kite_primary_session_name

    candidates = _iter_kite_write_clients()
    if not candidates:
        raise RuntimeError("Kite session not initialized")

    last_error: Optional[Exception] = None
    total = len(candidates)
    for idx, (app_name, client) in enumerate(candidates, 1):
        try:
            order_id = _place_order_compat(client, **place_kwargs)
            if kite is not client:
                kite = client
                kite_primary_session_name = app_name
                log.warning(
                    f"[KITE.WRITE_FAILOVER] {log_context}: switched active write session to "
                    f"{app_name} after primary write rejection."
                )
            return order_id
        except Exception as exc:
            last_error = exc
            if idx < total:
                if _is_order_write_ip_restriction_error(exc):
                    log.warning(
                        f"[KITE.WRITE_FAILOVER] {log_context}: {app_name} blocked for order "
                        f"placement ({_sanitize_error_message(exc, limit=160)}). Trying next app."
                    )
                    continue
                if _is_transient_kite_connection_error(exc):
                    log.warning(
                        f"[KITE.WRITE_FAILOVER] {log_context}: {app_name} transient connection "
                        f"error ({_sanitize_error_message(exc, limit=160)}). Trying next app."
                    )
                    time.sleep(0.3)
                    continue
            raise

    if last_error is not None:
        raise last_error
    raise RuntimeError(f"{log_context}: order placement failed without a concrete exception")


def _place_order_with_rate_limit_retry(log_context: str, **place_kwargs: Any) -> str:
    """Wrapper around `_place_order_with_fallback` that retries with
    exponential backoff when Kite returns a per-second rate-limit error.
    Non-rate-limit exceptions are re-raised unchanged so existing handling
    (partial-order cleanup, market-close fallback) still applies."""
    attempts = max(1, EXIT_LEG_RETRY_ATTEMPTS)
    for attempt in range(1, attempts + 1):
        try:
            return _place_order_with_fallback(log_context, **place_kwargs)
        except Exception as exc:
            if attempt >= attempts or not _is_rate_limit_error(exc):
                raise
            sleep_for = min(
                EXIT_LEG_RETRY_MAX_BACKOFF_SEC,
                EXIT_LEG_RETRY_BASE_BACKOFF_SEC * (2 ** (attempt - 1)),
            )
            log.warning(
                f"[KITE.RATE_LIMIT.RETRY] {log_context}: attempt {attempt}/{attempts} "
                f"hit rate limit ({_sanitize_error_message(exc, limit=120)}); "
                f"backing off {sleep_for:.2f}s"
            )
            time.sleep(sleep_for)
    raise RuntimeError(f"{log_context}: rate-limit retries exhausted")


def _recover_recent_entry_order_id(
    *,
    ticker: str,
    transaction_type: str,
    not_before_ist: datetime,
) -> Optional[str]:
    try:
        orders = _get_orders_snapshot(force_refresh=True)
    except Exception as exc:
        if not _is_rate_limit_error(exc):
            log.warning(
                f"[KITE.NET.RETRY] {ticker}: broker order recovery lookup failed "
                f"({_sanitize_error_message(exc, limit=180)})"
            )
        return None

    discovered = _pick_latest_tagged_order(orders, ticker, "AVWAPENTRY", transaction_type)
    if not discovered:
        return None

    discovered_ts = _order_row_timestamp_ist(discovered)
    if discovered_ts is None:
        return None

    window_open = not_before_ist - timedelta(seconds=float(TRANSIENT_ENTRY_ORDER_RECOVER_LOOKBACK_SEC))
    if discovered_ts < window_open:
        return None

    order_id = str(discovered.get("order_id", "")).strip()
    return order_id or None


def _place_entry_order_resilient(
    *,
    ticker: str,
    side: str,
    signal_entry_price: float,
    retry_until_ist: datetime,
    log_context: str,
    **place_kwargs: Any,
) -> Optional[str]:
    transaction_type = str(place_kwargs.get("transaction_type", "")).strip().upper()
    last_error: Optional[Exception] = None
    for attempt in range(1, TRANSIENT_API_RETRY_ATTEMPTS + 1):
        started_ist = datetime.now(IST)
        try:
            return _place_order_with_fallback(log_context, **place_kwargs)
        except Exception as exc:
            if not _is_transient_kite_connection_error(exc):
                raise
            last_error = exc

            recovered_order_id = _recover_recent_entry_order_id(
                ticker=ticker,
                transaction_type=transaction_type,
                not_before_ist=started_ist,
            )
            if recovered_order_id:
                log.warning(
                    f"[KITE.NET.RETRY] {log_context}: recovered broker entry order "
                    f"{recovered_order_id} after disconnect."
                )
                return recovered_order_id

            if datetime.now(IST) >= retry_until_ist:
                break

            ltp_now = _safe_get_entry_ltp(
                ticker,
                retry_until_ist=retry_until_ist,
                context=f"{ticker} transient entry retry",
            )
            if ltp_now is None:
                break

            if not _entry_price_within_retry_band(side, signal_entry_price, float(ltp_now)):
                log.warning(
                    f"[KITE.NET.RETRY] {log_context}: price moved away during retry "
                    f"| signal={signal_entry_price:.2f} ltp={ltp_now:.2f}"
                )
                waited_ltp = _wait_for_near_entry_price(
                    ticker=ticker,
                    side=side,
                    signal_entry_price=signal_entry_price,
                    retry_until_ist=retry_until_ist,
                )
                if (
                    waited_ltp is None
                    or waited_ltp <= 0
                    or not _entry_price_within_retry_band(side, signal_entry_price, float(waited_ltp))
                ):
                    log.warning(
                        f"[KITE.NET.RETRY] {log_context}: entry retry window expired "
                        f"before price returned near signal."
                    )
                    break

            sleep_sec = _transient_retry_sleep(attempt, retry_until_ist)
            if sleep_sec <= 0:
                break
            log.warning(
                f"[KITE.NET.RETRY] {log_context}: transient order error "
                f"(attempt {attempt}/{TRANSIENT_API_RETRY_ATTEMPTS}) "
                f"{_sanitize_error_message(exc, limit=180)} | retrying in {sleep_sec:.1f}s"
            )
            time.sleep(sleep_sec)

    if last_error is not None:
        log.warning(
            f"[KITE.NET.RETRY] {log_context}: giving up after transient order errors "
            f"({_sanitize_error_message(last_error, limit=180)})"
        )
    return None


def _is_mis_block_or_missing_error(error: Exception) -> bool:
    text = _sanitize_error_message(error, limit=500).lower()
    patterns = (
        "mis orders are currently blocked",
        "place a cnc order instead",
        "allowed for mis/co trades",
        "not allowed for mis",
        "rms:mis",
        "product mis",
    )
    return any(pat in text for pat in patterns)


def _atomic_write_json(path: str, payload: object) -> None:
    dir_path = os.path.dirname(path) or "."
    os.makedirs(dir_path, exist_ok=True)

    # Serialize local writers and use per-attempt unique temp files to avoid
    # collisions under concurrent trade threads.
    with json_write_lock:
        for attempt in range(1, JSON_WRITE_RETRIES + 1):
            fd = -1
            tmp_path = ""
            try:
                fd, tmp_path = tempfile.mkstemp(
                    prefix=f".{os.path.basename(path)}.",
                    suffix=".tmp",
                    dir=dir_path,
                )
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    fd = -1
                    json.dump(payload, f, indent=2)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp_path, path)
                return
            except OSError as e:
                win_err = int(getattr(e, "winerror", 0) or 0)
                retryable = win_err in (5, 32)  # access denied / sharing violation
                if (not retryable) or attempt >= JSON_WRITE_RETRIES:
                    raise
                time.sleep(JSON_WRITE_RETRY_BASE_SEC * attempt)
            finally:
                if fd >= 0:
                    try:
                        os.close(fd)
                    except OSError:
                        pass
                if tmp_path and os.path.exists(tmp_path):
                    try:
                        os.remove(tmp_path)
                    except OSError:
                        pass


def _open_trades_state_path(for_date: Optional[str] = None) -> str:
    day = for_date or datetime.now(IST).strftime("%Y-%m-%d")
    return os.path.join(SIGNAL_DIR, OPEN_TRADES_STATE_PATTERN.format(day))


def _persist_open_trades_state() -> None:
    today_str = datetime.now(IST).strftime("%Y-%m-%d")
    with active_positions_lock:
        open_rows = [dict(v) for v in active_positions.values()]
    now_ist = datetime.now(IST)
    # Mo6 (2026-04-22): write last_written_at as a TZ-aware ISO-8601 timestamp
    # embedded in the JSON so crash-recovery can derive freshness from the
    # payload itself rather than from file mtime (which AV/indexer touches can
    # silently bump on Windows). Kept legacy `updated_at` for back-compat with
    # any operator tooling that still parses it.
    payload = {
        "date": today_str,
        "open_trades": open_rows,
        "updated_at": now_ist.strftime("%Y-%m-%d %H:%M:%S"),
        "last_written_at": now_ist.isoformat(timespec="seconds"),
    }
    _atomic_write_json(_open_trades_state_path(today_str), payload)


def _load_open_trades_state(today_str: str) -> Dict[str, Dict[str, Any]]:
    state_path = _open_trades_state_path(today_str)
    if not os.path.exists(state_path):
        return {}
    try:
        with open(state_path, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except Exception as e:
        log.warning(f"[RESTORE] Could not parse open-trades state file: {e}")
        return {}

    if str(payload.get("date", "")).strip() != today_str:
        return {}

    # Mo6: prefer the embedded last_written_at timestamp; fall back to mtime
    # only if missing (e.g., file written by an older executor build). Warn
    # when falling back so operators notice stale-format state files.
    last_written_iso = str(payload.get("last_written_at", "")).strip()
    age_sec: Optional[float] = None
    if last_written_iso:
        try:
            age_sec = (datetime.now(IST) - datetime.fromisoformat(last_written_iso)).total_seconds()
        except ValueError:
            age_sec = None
    if age_sec is None:
        try:
            mtime = os.path.getmtime(state_path)
            age_sec = max(0.0, time.time() - mtime)
            log.warning(
                "[RESTORE] open-trades state missing last_written_at; falling back to file mtime "
                f"(age={age_sec:.0f}s). Older executor build wrote this file."
            )
        except OSError:
            age_sec = None
    if age_sec is not None:
        log.info(f"[RESTORE] open-trades state age={age_sec:.0f}s ({len(payload.get('open_trades', []) or [])} rows)")

    rows = payload.get("open_trades", [])
    if isinstance(rows, dict):
        rows = list(rows.values())
    if not isinstance(rows, list):
        return {}

    restored: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        sid = str(row.get("signal_id", "")).strip()
        if not sid:
            continue
        restored[sid] = dict(row)
    return restored


def _load_kill_switch_command_cached() -> Optional[Dict[str, Any]]:
    global kill_switch_cache_mtime, kill_switch_cache_payload
    path = Path(KILL_SWITCH_COMMAND_FILE)
    try:
        mtime = path.stat().st_mtime if path.exists() else -1.0
    except OSError:
        mtime = -1.0

    with kill_switch_cache_lock:
        if mtime == kill_switch_cache_mtime:
            return dict(kill_switch_cache_payload) if isinstance(kill_switch_cache_payload, dict) else None

    payload: Optional[Dict[str, Any]] = None
    if mtime >= 0:
        try:
            with path.open("r", encoding="utf-8") as f:
                raw = json.load(f)
            if isinstance(raw, dict):
                today_str = datetime.now(IST).strftime("%Y-%m-%d")
                cmd_date = str(raw.get("date", today_str)).strip() or today_str
                if cmd_date == today_str:
                    payload = dict(raw)
        except Exception:
            payload = None

    with kill_switch_cache_lock:
        kill_switch_cache_mtime = mtime
        kill_switch_cache_payload = dict(payload) if isinstance(payload, dict) else None
        return dict(kill_switch_cache_payload) if isinstance(kill_switch_cache_payload, dict) else None


def _get_kill_switch_for_trade(signal_id: str, ticker: str) -> Optional[Dict[str, Any]]:
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


def _set_dispatch_lockdown(reason: Optional[str]) -> None:
    global dispatch_lockdown_reason
    with dispatch_lockdown_lock:
        dispatch_lockdown_reason = reason.strip() if reason else None


def _get_dispatch_lockdown() -> Optional[str]:
    with dispatch_lockdown_lock:
        return dispatch_lockdown_reason


def _release_capacity(signal_id: str) -> None:
    with capital_lock:
        capital_deployed.pop(signal_id, None)


def _is_market_open_now(now_ist: Optional[datetime] = None) -> bool:
    ts = now_ist or datetime.now(IST)
    t = ts.time()
    return MARKET_OPEN <= t <= MARKET_CLOSE


def _is_entry_window_open_now(now_ist: Optional[datetime] = None) -> bool:
    """
    New entries are allowed only until forced-close cutoff.
    After cutoff, only position-closing actions are allowed.
    """
    ts = now_ist or datetime.now(IST)
    t = ts.time()
    return MARKET_OPEN <= t < FORCED_CLOSE_TIME


def _global_eod_force_close_sweep(now_ist: Optional[datetime] = None) -> None:
    """
    Fail-safe EOD flattener:
    - blocks new dispatch after 15:20
    - cancels exit legs for tracked positions
    - sends market close orders for any remaining broker MIS quantity
      (including orphan/mismatched positions) to avoid overnight carry.
    """
    if kite is None:
        return

    ts = now_ist or datetime.now(IST)
    if ts.time() < FORCED_CLOSE_TIME:
        return

    global global_eod_last_sweep_ts
    now_mono = time.monotonic()
    with global_eod_sweep_lock:
        if (now_mono - global_eod_last_sweep_ts) < GLOBAL_EOD_SWEEP_INTERVAL_SEC:
            return
        global_eod_last_sweep_ts = now_mono

    cutoff_reason = (
        f"entry cutoff reached at {FORCED_CLOSE_TIME.strftime('%H:%M:%S')} IST; "
        "EOD flatten in progress"
    )
    if not _get_dispatch_lockdown():
        _set_dispatch_lockdown(cutoff_reason)
        log.warning(f"[EOD.GLOBAL] {cutoff_reason}")

    broker_qty_map = _get_broker_mis_position_qty_map()
    if broker_qty_map is None:
        log.warning("[EOD.GLOBAL] Broker position snapshot unavailable; retrying sweep.")
        return

    with active_positions_lock:
        tracked_positions = [dict(v) for v in active_positions.values()]

    # Aggregate expected per ticker to avoid over-closing when multiple signals
    # map to the same symbol.
    per_ticker: Dict[str, Dict[str, Any]] = {}
    for pos in tracked_positions:
        ticker = str(pos.get("ticker", "")).upper().strip()
        side = str(pos.get("side", "")).upper().strip()
        qty = int(_safe_int(pos.get("quantity", 0), 0))
        if not ticker or qty <= 0 or side not in {"LONG", "SHORT"}:
            continue
        entry = per_ticker.setdefault(
            ticker,
            {
                "expected_signed_qty": 0,
                "target_order_ids": set(),
                "sl_order_ids": set(),
            },
        )
        entry["expected_signed_qty"] += qty if side == "LONG" else -qty

        target_oid = str(pos.get("target_order_id", "")).strip()
        sl_oid = str(pos.get("sl_order_id", "")).strip()
        if target_oid:
            entry["target_order_ids"].add(target_oid)
        if sl_oid:
            entry["sl_order_ids"].add(sl_oid)

    all_tickers = sorted(set(per_ticker.keys()) | set(broker_qty_map.keys()))
    for ticker in all_tickers:
        meta = per_ticker.get(
            ticker,
            {
                "expected_signed_qty": 0,
                "target_order_ids": set(),
                "sl_order_ids": set(),
            },
        )
        for oid in sorted(meta["target_order_ids"]):
            cancel_order_safe(kite.VARIETY_REGULAR, oid)
        for oid in sorted(meta["sl_order_ids"]):
            cancel_order_safe(kite.VARIETY_REGULAR, oid)

        expected_signed_qty = int(meta["expected_signed_qty"])
        broker_signed_qty = int(broker_qty_map.get(ticker, 0))
        if broker_signed_qty == 0:
            continue

        if expected_signed_qty == 0:
            log.warning(
                f"[EOD.GLOBAL] Orphan broker MIS position detected for {ticker}: "
                f"broker={broker_signed_qty}. Forcing flatten."
            )
        elif (expected_signed_qty > 0 and broker_signed_qty < 0) or (
            expected_signed_qty < 0 and broker_signed_qty > 0
        ):
            log.warning(
                f"[EOD.GLOBAL] Direction mismatch for {ticker} "
                f"(expected={expected_signed_qty}, broker={broker_signed_qty}). "
                "Forcing flatten using broker quantity."
            )

        close_qty = abs(broker_signed_qty)
        if close_qty <= 0:
            continue

        close_txn = kite.TRANSACTION_TYPE_BUY if broker_signed_qty < 0 else kite.TRANSACTION_TYPE_SELL
        try:
            close_order_id = _place_order_with_fallback(
                f"{ticker} global EOD close",
                variety=kite.VARIETY_REGULAR,
                exchange=kite.EXCHANGE_NSE,
                tradingsymbol=ticker,
                transaction_type=close_txn,
                quantity=int(close_qty),
                product=kite.PRODUCT_MIS,
                order_type=kite.ORDER_TYPE_MARKET,
                validity=kite.VALIDITY_DAY,
                tag="AVWAPEODGlobal",
            )
            _reset_orders_snapshot_cache()
            fill_px = wait_for_fill(str(close_order_id), timeout_sec=12)
            if fill_px and fill_px > 0:
                log.warning(
                    f"[EOD.GLOBAL] Flattened {ticker} qty={close_qty} @ {fill_px:.2f} "
                    f"| order_id={close_order_id}"
                )
            else:
                log.warning(
                    f"[EOD.GLOBAL] Flatten order placed for {ticker} qty={close_qty} "
                    f"| order_id={close_order_id} (fill pending/unconfirmed)"
                )
        except Exception as e:
            log.error(
                f"[EOD.GLOBAL] Flatten failed for {ticker} qty={close_qty}: {e}"
            )


def _check_risk_limits(signal: dict) -> Optional[str]:
    """
    Check open positions and capital deployed.
    Returns a rejection reason string, or None if the trade is allowed.
    """
    if not RISK_LIMITS_ENABLED:
        return None

    with capital_lock:
        open_count = len(capital_deployed)
        total_deployed = sum(capital_deployed.values())

    if open_count >= MAX_OPEN_POSITIONS:
        return f"max open positions reached ({open_count}/{MAX_OPEN_POSITIONS})"

    entry_price = _safe_float(signal.get("entry_price", 0), 0.0)
    quantity = _safe_int(signal.get("quantity", 1), 1)
    margin = (entry_price * quantity) / INTRADAY_LEVERAGE
    if (total_deployed + margin) > MAX_CAPITAL_DEPLOYED_RS:
        return (
            f"margin limit exceeded (deployed Rs.{total_deployed:,.0f} + "
            f"Rs.{margin:,.0f} > Rs.{MAX_CAPITAL_DEPLOYED_RS:,})"
        )

    return None


def _reserve_capacity_for_signal(signal_id: str, signal: dict) -> Tuple[bool, str, float]:
    """
    Atomically validate risk and reserve margin for a signal to avoid races
    between concurrent CSV callbacks/threads.
    """
    # CAND-E4 LIVE GATE (2026-05-04): skip rows whose Detection Engine wrote
    # size_multiplier == 0. These are setups Cand-E4 explicitly drops
    # (currently SHORT C_OR_BREAKDOWN and SHORT D_EMA20_REJECTION -- both
    # PF < 1.20 in fresh-data backtest, no edge). Stops them from being
    # placed at full size in live, recovering ~30% of the backtest gap.
    # If the column is missing (older signal CSVs), default to 1.0 (no-op).
    size_mult_raw = signal.get("size_multiplier", 1.0)
    try:
        size_mult = float(size_mult_raw) if size_mult_raw not in (None, "") else 1.0
    except (TypeError, ValueError):
        size_mult = 1.0
    if size_mult <= 0.0:
        setup = str(signal.get("setup", "?"))
        side = str(signal.get("side", "?"))
        return False, f"candE4_size_multiplier=0 (setup={side} {setup})", 0.0

    # CAND-E4 G1: time-window governor (stateless). Reject if current IST
    # time is outside the side's intraday entry window (LONG 09:15-13:00,
    # SHORT 09:15-14:00).
    g1_side = str(signal.get("side", "")).strip().upper()
    g1_ok, g1_reason = _candE4_g1_time_window_check(g1_side)
    if not g1_ok:
        return False, g1_reason, 0.0

    entry_price = _safe_float(signal.get("entry_price", 0), 0.0)
    quantity = _safe_int(signal.get("quantity", 1), 1)
    if entry_price <= 0:
        return False, "invalid entry_price", 0.0
    if quantity <= 0:
        return False, "invalid quantity", 0.0
    margin = (entry_price * quantity) / INTRADAY_LEVERAGE if INTRADAY_LEVERAGE > 0 else 0.0
    margin = float(max(0.0, margin))

    with capital_lock:
        if signal_id in capital_deployed:
            return False, "already reserved", float(capital_deployed.get(signal_id, 0.0))

        # CAND-E4 G2: daily side cap (LONG 15/day, SHORT 10/day). Atomic
        # check+increment, persisted to disk for crash safety.
        g2_ok, g2_reason = _candE4_g2_check_and_increment(g1_side)
        if not g2_ok:
            return False, g2_reason, 0.0

        if RISK_LIMITS_ENABLED:
            open_count = len(capital_deployed)
            total_deployed = float(sum(capital_deployed.values()))

            if open_count >= MAX_OPEN_POSITIONS:
                return False, f"max open positions reached ({open_count}/{MAX_OPEN_POSITIONS})", 0.0

            if (total_deployed + margin) > MAX_CAPITAL_DEPLOYED_RS:
                return (
                    False,
                    f"margin limit exceeded (deployed Rs.{total_deployed:,.0f} + "
                    f"Rs.{margin:,.0f} > Rs.{MAX_CAPITAL_DEPLOYED_RS:,})",
                    0.0,
                )

        capital_deployed[signal_id] = margin
        return True, "ok", margin


def execute_live_trade(signal: dict, resume_mode: bool = False) -> None:
    """
    Execute or resume a complete live MIS trade on Zerodha:
      1. Market entry order (new trades only)
      2. Wait for fill (new trades only)
      3. Place target (LIMIT) + stop-loss (SL-M), or restore existing legs
      4. Monitor until target/SL fills or 15:20 forced close
    """
    ticker = str(signal["ticker"]).upper()
    side = str(signal["side"]).upper()
    signal_id = str(signal.get("signal_id", ""))
    signal_entry_price = _safe_float(signal.get("entry_price", 0), 0.0)
    signal_stop = _safe_float(signal.get("stop_price", 0), 0.0)
    signal_target = _safe_float(signal.get("target_price", 0), 0.0)
    quantity = _safe_int(signal.get("quantity", 1), 1)

    # CAND-E4 per-setup SL/TGT override: recompute stop/target from
    # CANDIDATE_E4_SL_TGT[(side, setup)] when the dict contains the pair.
    # Falls through to CSV values for unknown setups (no behaviour change).
    _setup_for_candE4 = str(signal.get("setup", "")).strip().upper()
    _csv_stop_pre, _csv_target_pre = signal_stop, signal_target
    signal_stop, signal_target, _candE4_applied = _candE4_per_setup_sl_tgt(
        side, _setup_for_candE4, signal_entry_price, signal_stop, signal_target,
    )
    if _candE4_applied:
        log.info(
            f"[CAND-E4_SLTGT] {ticker} {side} {_setup_for_candE4} "
            f"entry={signal_entry_price:.2f} "
            f"SL {_csv_stop_pre:.2f}->{signal_stop:.2f} "
            f"TGT {_csv_target_pre:.2f}->{signal_target:.2f}"
        )

    trade_start_ist = datetime.now(IST)
    default_trade_id = f"LT-{signal_id[:8]}-{trade_start_ist.strftime('%H%M%S')}"
    trade_id = str(signal.get("trade_id", "")).strip() or default_trade_id
    today = trade_start_ist.date()
    forced_close_dt = IST.localize(datetime.combine(today, FORCED_CLOSE_TIME))
    entry_retry_deadline = _entry_retry_deadline(signal, trade_start_ist, forced_close_dt)

    if side == "SHORT":
        entry_txn = "SELL"
        exit_txn = "BUY"
    else:
        entry_txn = "BUY"
        exit_txn = "SELL"

    result = LiveTradeResult(
        trade_id=trade_id,
        signal_id=signal_id,
        signal_datetime=signal.get("signal_datetime", ""),
        signal_entry_datetime_ist=signal.get("signal_entry_datetime_ist", ""),
        ticker=ticker,
        side=side,
        setup=signal.get("setup", ""),
        impulse_type=signal.get("impulse_type", ""),
        quantity=quantity,
        entry_price=signal_entry_price,
        stop_price=signal_stop,
        target_price=signal_target,
        quality_score=_safe_float(signal.get("quality_score", 0), 0.0),
    )
    if resume_mode:
        log.info(
            f"[RESUME] Restoring trade {trade_id}: {entry_txn} {ticker} qty={quantity} "
            f"| SL={signal_stop} TGT={signal_target}"
        )
    else:
        log.info(
            f"[LIVE] Starting trade {trade_id}: {entry_txn} {ticker} qty={quantity} "
            f"| SL={signal_stop} TGT={signal_target}"
        )

    target_order_id = str(signal.get("target_order_id", "")).strip() if resume_mode else ""
    sl_order_id = str(signal.get("sl_order_id", "")).strip() if resume_mode else ""
    trade_closed = False
    last_kill_switch_command_id = ""

    def _sync_active_position(stage: str) -> None:
        with active_positions_lock:
            active_positions[signal_id] = {
                "signal_id": signal_id,
                "trade_id": result.trade_id,
                "ticker": ticker,
                "side": side,
                "quantity": int(quantity),
                "entry_price": float(result.entry_price),
                "filled_price": float(result.filled_price if result.filled_price > 0 else result.entry_price),
                "stop_price": float(result.stop_price),
                "target_price": float(result.target_price),
                "entry_order_id": str(result.entry_order_id),
                "target_order_id": str(result.target_order_id),
                "sl_order_id": str(result.sl_order_id),
                "entry_time": str(result.entry_time),
                "signal_datetime": str(result.signal_datetime),
                "signal_entry_datetime_ist": str(result.signal_entry_datetime_ist),
                "setup": str(result.setup),
                "impulse_type": str(result.impulse_type),
                "quality_score": float(result.quality_score),
                "stage": stage,
                "updated_at": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S"),
            }
        _persist_open_trades_state()

    def _publish_open_trade_state(outcome: str = "OPEN") -> None:
        """
        Reflect an actively open, already-filled trade in the CSV/dashboard.
        This row is later replaced in-place by the terminal trade result.
        """
        result.outcome = str(outcome or "OPEN")
        result.exit_time = ""
        result.exit_price = 0.0
        result.pnl_rs = 0.0
        result.pnl_pct = 0.0
        _log_trade_result(result)

    def _force_market_close(tag: str, outcome: str, timeout_sec: int = 30) -> bool:
        try:
            close_order_id = _place_order_with_rate_limit_retry(
                f"{ticker} force close {tag}",
                variety=kite.VARIETY_REGULAR,
                exchange=kite.EXCHANGE_NSE,
                tradingsymbol=ticker,
                transaction_type=exit_txn,
                quantity=quantity,
                product=kite.PRODUCT_MIS,
                order_type=kite.ORDER_TYPE_MARKET,
                validity=kite.VALIDITY_DAY,
                tag=str(tag)[:20],
            )
            _reset_orders_snapshot_cache()
            result.close_order_id = str(close_order_id)
            close_price = wait_for_fill(close_order_id, timeout_sec=timeout_sec)
            if close_price and close_price > 0:
                result.exit_price = close_price
                result.outcome = outcome
                return True
            log.error(
                f"[LIVE] Market close unconfirmed for {ticker}; "
                f"close_order_id={close_order_id}"
            )
            return False
        except Exception as close_err:
            log.error(f"[LIVE] Market close failed for {ticker}: {close_err}")
            return False

    def _check_kill_switch_and_close(stage: str) -> bool:
        nonlocal trade_closed, last_kill_switch_command_id
        cmd = _get_kill_switch_for_trade(signal_id, ticker)
        if not cmd:
            return False
        cmd_id = str(cmd.get("command_id", "")).strip() or "NO_CMD_ID"
        if cmd_id == last_kill_switch_command_id:
            return False
        last_kill_switch_command_id = cmd_id

        mode = str(cmd.get("mode", "")).strip().lower() or "manual"
        req_ts = str(cmd.get("requested_at_ist", "")).strip()
        log.warning(
            f"[KILL] Kill switch matched for {ticker} | signal_id={signal_id[:12]} | "
            f"command_id={cmd_id} | mode={mode} | stage={stage}"
            + (f" | requested_at={req_ts}" if req_ts else "")
        )

        if result.target_order_id:
            cancel_order_safe(kite.VARIETY_REGULAR, result.target_order_id)
        if result.sl_order_id:
            cancel_order_safe(kite.VARIETY_REGULAR, result.sl_order_id)

        if _force_market_close(
            tag="AVWAPKillSwitch",
            outcome="MANUAL_KILL_SWITCH",
            timeout_sec=30,
        ):
            trade_closed = True
            log.warning(
                f"[KILL] Position closed via kill switch for {ticker} @ {result.exit_price} "
                f"| close_order_id={result.close_order_id}"
            )
            return True

        _sync_active_position("kill_switch_close_failed")
        log.error(
            f"[KILL] Kill switch close failed for {ticker}; keeping position tracked "
            "for manual reconciliation."
        )
        return False

    try:
        if resume_mode:
            result.entry_order_id = str(signal.get("entry_order_id", "")).strip()
            result.target_order_id = target_order_id
            result.sl_order_id = sl_order_id
            result.entry_time = str(signal.get("entry_time", "")).strip()
            result.filled_price = _safe_float(signal.get("filled_price", signal_entry_price), signal_entry_price)
            if result.filled_price <= 0:
                log.error(
                    f"[RESUME] Missing filled entry price for {ticker} (signal_id={signal_id[:12]}). "
                    "Keeping position state for manual reconciliation."
                )
                _sync_active_position("resume_missing_filled_price")
                return

            margin = (result.filled_price * quantity) / INTRADAY_LEVERAGE
            with capital_lock:
                capital_deployed[signal_id] = margin
            _sync_active_position("restored")
        else:
            if datetime.now(IST) >= forced_close_dt:
                result.outcome = "ENTRY_SKIPPED_AFTER_CUTOFF"
                result.exit_price = signal_entry_price
                trade_closed = True
                log.warning(
                    f"[LIVE] Skipping new entry for {ticker}: "
                    f"cutoff {FORCED_CLOSE_TIME.strftime('%H:%M:%S')} IST already reached."
                )
                # No open position was created; proceed to finalization for audit row.
                pass

            # Fix #20 (post-2026-04-21): stale-detection guard. If Stage 2
            # detected far after the intended entry slot, treat the signal as
            # stale — a delayed confirmation should not re-enter the market on
            # a price action that is already in the past.
            if (not trade_closed) and LATE_DETECTION_GUARD_ENABLE and LATE_DETECTION_MAX_LAG_SEC > 0:
                lag_sec = _detection_lag_seconds(signal)
                setup_name = str(signal.get("setup", "")).upper().strip()
                threshold = _late_lag_threshold_for_setup(setup_name)
                if lag_sec is not None and lag_sec > threshold:
                    result.outcome = "ENTRY_SKIPPED_STALE_DETECTION"
                    result.exit_price = signal_entry_price
                    trade_closed = True
                    _append_late_skipped_csv(signal, lag_sec, threshold)
                    log.warning(
                        f"[STALE.DETECT] Skipping {ticker} {side} {setup_name}: detected "
                        f"{lag_sec:.0f}s after entry slot "
                        f"(universal threshold {threshold}s) | "
                        f"signal_id={signal_id[:12]} | total_late_skipped_today={_late_skipped_count}"
                    )

            # ---- SLIP GATE: pre-entry LTP check ----
            # Fetch current LTP and reject if it has chased too far from the model trigger.
            if (not trade_closed) and signal_entry_price > 0:
                ltp_check = _safe_get_entry_ltp(
                    ticker,
                    retry_until_ist=entry_retry_deadline,
                    context=f"{ticker} pre-entry",
                )
                if ltp_check is None:
                    started_stale = _trade_started_after_entry_deadline(
                        trade_start_ist, entry_retry_deadline
                    )
                    result.outcome = (
                        "ENTRY_SKIPPED_STALE_SIGNAL"
                        if started_stale
                        else "ENTRY_SKIPPED_CONNECTION_RETRY_TIMEOUT"
                    )
                    result.exit_price = signal_entry_price
                    trade_closed = True
                    if started_stale:
                        log.warning(
                            f"[STALE] Skipping {ticker} {side}: signal surfaced at "
                            f"{trade_start_ist.strftime('%H:%M:%S')} after freshness deadline "
                            f"{entry_retry_deadline.strftime('%H:%M:%S')}."
                        )
                    else:
                        log.warning(
                            f"[KITE.NET.RETRY] Skipping {ticker} {side}: no stable quote before "
                            f"freshness deadline {entry_retry_deadline.strftime('%H:%M:%S')}."
                        )
                elif ltp_check > 0:
                    if ENTRY_RETRY_NEAR_ENTRY_ENABLE and ENTRY_RETRY_WAIT_SEC > 0:
                        if not _entry_price_within_retry_band(side, signal_entry_price, float(ltp_check)):
                            log.info(
                                f"[ENTRY.RETRY] Waiting for {ticker} {side} to return near entry "
                                f"| signal={signal_entry_price:.2f} ltp={ltp_check:.2f} "
                                f"| band={ENTRY_RETRY_NEAR_ENTRY_PCT*100:.2f}% "
                                f"| until={entry_retry_deadline.strftime('%H:%M:%S')}"
                            )
                            waited_ltp = _wait_for_near_entry_price(
                                ticker=ticker,
                                side=side,
                                signal_entry_price=signal_entry_price,
                                retry_until_ist=entry_retry_deadline,
                            )
                            if waited_ltp is not None and waited_ltp > 0 and _entry_price_within_retry_band(side, signal_entry_price, float(waited_ltp)):
                                ltp_check = float(waited_ltp)
                                log.info(
                                    f"[ENTRY.RETRY] Re-armed {ticker} {side} near entry "
                                    f"| signal={signal_entry_price:.2f} ltp={ltp_check:.2f}"
                                )
                            else:
                                result.outcome = "ENTRY_SKIPPED_NEAR_ENTRY_TIMEOUT"
                                result.exit_price = signal_entry_price
                                trade_closed = True
                                log.warning(
                                    f"[ENTRY.RETRY] Timed out for {ticker} {side} "
                                    f"| signal={signal_entry_price:.2f} "
                                    f"| last_ltp={(float(waited_ltp) if waited_ltp else float(ltp_check)):.2f}"
                                )
                    if (not trade_closed) and side == "LONG" and LONG_MAX_ENTRY_SLIP_PCT > 0.0:
                        slip = (ltp_check - signal_entry_price) / signal_entry_price
                        if slip > LONG_MAX_ENTRY_SLIP_PCT:
                            result.outcome = "ENTRY_SKIPPED_MAX_SLIP"
                            result.exit_price = signal_entry_price
                            trade_closed = True
                            log.warning(
                                f"[SLIP.GATE] REJECTED {ticker} LONG | "
                                f"model={signal_entry_price:.2f} ltp={ltp_check:.2f} "
                                f"slip={slip*100:.2f}% > cap={LONG_MAX_ENTRY_SLIP_PCT*100:.2f}%"
                            )
                    elif (not trade_closed) and side == "SHORT" and SHORT_MAX_ENTRY_SLIP_PCT > 0.0:
                        slip = (signal_entry_price - ltp_check) / signal_entry_price
                        if slip > SHORT_MAX_ENTRY_SLIP_PCT:
                            result.outcome = "ENTRY_SKIPPED_MAX_SLIP"
                            result.exit_price = signal_entry_price
                            trade_closed = True
                            log.warning(
                                f"[SLIP.GATE] REJECTED {ticker} SHORT | "
                                f"model={signal_entry_price:.2f} ltp={ltp_check:.2f} "
                                f"slip={slip*100:.2f}% > cap={SHORT_MAX_ENTRY_SLIP_PCT*100:.2f}%"
                            )

            # ---- VOLUME GATE: NEW-P13 ----
            # Block / shrink orders that would punch through thin liquidity at the
            # trigger bar. Requires `bar_volume` (or legacy `c2_volume`) on the
            # signal. Missing field → no-op + ADVISORY log.
            if (not trade_closed) and VOLUME_GATE_ENABLE and quantity > 0:
                bar_vol_raw = signal.get("bar_volume", signal.get("c2_volume", None))
                bar_volume: Optional[int] = None
                try:
                    if bar_vol_raw not in (None, ""):
                        bar_volume = int(float(bar_vol_raw))
                except (TypeError, ValueError):
                    bar_volume = None
                if bar_volume is None or bar_volume <= 0:
                    log.info(
                        f"[VOLUME.GATE] ADVISORY {ticker}: signal carries no bar_volume; "
                        "gate inactive for this trade. Upstream (DE) should populate "
                        "`bar_volume` on the trigger bar."
                    )
                else:
                    cap_qty = int(bar_volume * VOLUME_GATE_MAX_PARTICIPATION_PCT)
                    if cap_qty < quantity:
                        if cap_qty < 1:
                            result.outcome = "ENTRY_SKIPPED_THIN_LIQUIDITY"
                            result.exit_price = signal_entry_price
                            trade_closed = True
                            log.warning(
                                f"[VOLUME.GATE] REJECTED {ticker} {side}: "
                                f"intended_qty={quantity} bar_volume={bar_volume} "
                                f"cap@{VOLUME_GATE_MAX_PARTICIPATION_PCT*100:.1f}%={cap_qty}"
                            )
                        else:
                            log.warning(
                                f"[VOLUME.GATE] REDUCED {ticker} {side}: "
                                f"intended_qty={quantity} → {cap_qty} "
                                f"(bar_volume={bar_volume}, "
                                f"cap@{VOLUME_GATE_MAX_PARTICIPATION_PCT*100:.1f}%)"
                            )
                            quantity = int(cap_qty)

            # ---- STEP 1: Place market entry ----
            planned_qty = int(quantity)
            filled_price = 0.0
            entry_filled = False
            filled_qty_total = 0
            filled_notional_total = 0.0
            for entry_attempt in range(1, ENTRY_RETRY_ATTEMPTS + 1):
                if trade_closed:
                    break
                remaining_qty = max(0, planned_qty - filled_qty_total)
                if remaining_qty <= 0:
                    entry_filled = True
                    break

                entry_order_id = _place_entry_order_resilient(
                    ticker=ticker,
                    side=side,
                    signal_entry_price=signal_entry_price,
                    retry_until_ist=entry_retry_deadline,
                    log_context=f"{ticker} entry {entry_txn} attempt {entry_attempt}/{ENTRY_RETRY_ATTEMPTS}",
                    variety=kite.VARIETY_REGULAR,
                    exchange=kite.EXCHANGE_NSE,
                    tradingsymbol=ticker,
                    transaction_type=entry_txn,
                    quantity=remaining_qty,
                    product=kite.PRODUCT_MIS,
                    order_type=kite.ORDER_TYPE_MARKET,
                    validity=kite.VALIDITY_DAY,
                    tag="AVWAPEntry",
                )
                if not entry_order_id:
                    started_stale = _trade_started_after_entry_deadline(
                        trade_start_ist, entry_retry_deadline
                    )
                    result.outcome = (
                        "ENTRY_SKIPPED_STALE_SIGNAL"
                        if started_stale
                        else "ENTRY_SKIPPED_CONNECTION_RETRY_TIMEOUT"
                    )
                    result.exit_price = signal_entry_price
                    trade_closed = True
                    if started_stale:
                        log.warning(
                            f"[STALE] {ticker} {side}: signal surfaced at "
                            f"{trade_start_ist.strftime('%H:%M:%S')} after freshness deadline "
                            f"{entry_retry_deadline.strftime('%H:%M:%S')}."
                        )
                    else:
                        log.warning(
                            f"[KITE.NET.RETRY] {ticker} {side}: transient entry retry window expired "
                            f"before a confirmed broker order could be placed."
                        )
                    break
                _reset_orders_snapshot_cache()
                result.entry_order_id = str(entry_order_id)
                result.entry_time = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S%z")
                if entry_attempt == 1:
                    log.info(f"[LIVE] Entry order placed: {entry_txn} {ticker} ID={entry_order_id}")
                else:
                    log.info(
                        f"[LIVE] Entry retry placed ({entry_attempt}/{ENTRY_RETRY_ATTEMPTS}): "
                        f"{entry_txn} {ticker} ID={entry_order_id}"
                    )

                # ---- STEP 2: Wait for fill ----
                filled_price = wait_for_fill(entry_order_id)
                status_upper = "UNKNOWN"
                filled_qty = 0
                avg_price = 0.0
                entry_row = _get_order_row(
                    str(entry_order_id),
                    force_refresh=True,
                    allow_history_fallback=True,
                )
                if entry_row:
                    entry_state = _order_state_from_row(entry_row)
                    status_upper = entry_state["status"]
                    filled_qty = min(remaining_qty, int(entry_state["filled_quantity"]))
                    avg_price = float(entry_state["average_price"])
                    if status_upper == "COMPLETE" and filled_qty <= 0:
                        filled_qty = remaining_qty
                elif filled_price and filled_price > 0:
                    status_upper = "COMPLETE"
                    filled_qty = remaining_qty
                    avg_price = float(filled_price)

                if filled_qty > 0:
                    if avg_price <= 0:
                        avg_price = float(
                            filled_price if (filled_price and filled_price > 0) else signal_entry_price
                        )
                    filled_qty_total += int(filled_qty)
                    filled_notional_total += float(avg_price) * int(filled_qty)
                    remaining_after = max(0, planned_qty - filled_qty_total)
                    if remaining_after > 0:
                        log.warning(
                            f"[LIVE][ENTRY.PARTIAL] {ticker}: "
                            f"filled={filled_qty_total}/{planned_qty} "
                            f"(last_fill_qty={filled_qty}, status={status_upper})"
                        )
                    else:
                        entry_filled = True
                        break

                retryable_no_fill = (
                    filled_qty <= 0
                    and status_upper not in {"REJECTED", "COMPLETE"}
                )
                has_retry_left = entry_attempt < ENTRY_RETRY_ATTEMPTS

                remaining_after = max(0, planned_qty - filled_qty_total)
                if remaining_after > 0 and has_retry_left:
                    if status_upper not in {"CANCELLED"}:
                        cancel_order_safe(kite.VARIETY_REGULAR, str(entry_order_id))
                    if retryable_no_fill:
                        log.warning(
                            f"[LIVE][ENTRY.RETRY] {ticker} entry not filled "
                            f"(status={status_upper}, attempt={entry_attempt}/{ENTRY_RETRY_ATTEMPTS}). "
                            f"Retrying after {ENTRY_RETRY_DELAY_SEC:.1f}s."
                        )
                    else:
                        log.warning(
                            f"[LIVE][ENTRY.RETRY] {ticker} partial fill "
                            f"{filled_qty_total}/{planned_qty}; retrying leftover {remaining_after} "
                            f"after {ENTRY_RETRY_DELAY_SEC:.1f}s."
                        )
                    # Mo5: spacing between cancel-then-reissue to avoid bursting
                    # Kite's per-second order rate limit when many names retry
                    # inside the same 5-min slot.
                    if ENTRY_RETRY_DELAY_SEC > 0.0:
                        time.sleep(ENTRY_RETRY_DELAY_SEC)
                    continue

                if status_upper not in {"CANCELLED", "REJECTED", "COMPLETE"}:
                    cancel_order_safe(kite.VARIETY_REGULAR, str(entry_order_id))
                if filled_qty_total > 0:
                    log.warning(
                        f"[LIVE][ENTRY.PARTIAL.ACCEPT] {ticker}: "
                        f"proceeding with partial fill {filled_qty_total}/{planned_qty} "
                        f"after {entry_attempt} attempt(s)."
                    )
                    entry_filled = True
                    break
                log.error(
                    f"[LIVE] Entry order {entry_order_id} not filled "
                    f"(status={status_upper}). Aborting trade."
                )
                result.outcome = "ENTRY_FAILED"
                result.exit_price = signal_entry_price
                trade_closed = True
                break

            if entry_filled:
                if filled_qty_total <= 0:
                    filled_qty_total = planned_qty
                    filled_notional_total = float(
                        (filled_price if (filled_price and filled_price > 0) else signal_entry_price)
                        * planned_qty
                    )
                quantity = max(1, int(filled_qty_total))
                result.quantity = quantity
                result.filled_price = float(filled_notional_total / max(1, filled_qty_total))
                if quantity < planned_qty:
                    log.warning(
                        f"[LIVE] Entry partial for {ticker}: qty={quantity}/{planned_qty} "
                        f"avg={result.filled_price:.2f}"
                    )
                else:
                    log.info(f"[LIVE] Entry filled: {ticker} @ {result.filled_price}")

                # If entry fills at/after cutoff, flatten immediately and do not place exit legs.
                if datetime.now(IST) >= forced_close_dt:
                    log.warning(
                        f"[LIVE] Entry for {ticker} filled at/after cutoff "
                        f"{FORCED_CLOSE_TIME.strftime('%H:%M:%S')} IST; forcing immediate close."
                    )
                    if _force_market_close(
                        tag="AVWAPCutoffClose",
                        outcome="EOD_CLOSE_AFTER_CUTOFF",
                        timeout_sec=30,
                    ):
                        trade_closed = True
                    else:
                        _sync_active_position("post_cutoff_entry_close_failed")
                        return

                # Register margin deployment (notional / leverage)
                margin = (result.filled_price * quantity) / INTRADAY_LEVERAGE
                with capital_lock:
                    capital_deployed[signal_id] = margin

                # Recalculate SL/target from the actual fill price using the
                # intended signal-side stop/target multipliers.
                tick_size = get_tick_size(ticker)
                signal_stop_mult = (
                    float(signal_stop / signal_entry_price)
                    if signal_entry_price > 0 and signal_stop > 0
                    else None
                )
                signal_target_mult = (
                    float(signal_target / signal_entry_price)
                    if signal_entry_price > 0 and signal_target > 0
                    else None
                )
                if side == "SHORT":
                    stop_mult = signal_stop_mult if signal_stop_mult and signal_stop_mult > 1.0 else (1.0 + SHORT_STOP_PCT)
                    target_mult = signal_target_mult if signal_target_mult and signal_target_mult < 1.0 else (1.0 - SHORT_TARGET_PCT)
                else:
                    stop_mult = signal_stop_mult if signal_stop_mult and signal_stop_mult < 1.0 else (1.0 - LONG_STOP_PCT)
                    target_mult = signal_target_mult if signal_target_mult and signal_target_mult > 1.0 else (1.0 + LONG_TARGET_PCT)

                old_stop = float(result.stop_price)
                old_target = float(result.target_price)
                result.stop_price = float(round_to_tick(result.filled_price * stop_mult, tick=tick_size))
                result.target_price = float(round_to_tick(result.filled_price * target_mult, tick=tick_size))
                if old_stop > 0 and old_target > 0:
                    if (
                        abs(result.stop_price - old_stop) >= (tick_size / 2.0)
                        or abs(result.target_price - old_target) >= (tick_size / 2.0)
                    ):
                        log.info(
                            f"[LIVE][ENTRY.REBASE] ticker={ticker} | side={side} | "
                            f"signal_id={signal_id[:12]} | signal_entry={signal_entry_price:.2f} | "
                            f"entry_exec={result.filled_price:.2f} | "
                            f"sl:{old_stop:.2f}->{result.stop_price:.2f} | "
                            f"tgt:{old_target:.2f}->{result.target_price:.2f}"
                        )
                _sync_active_position("entry_filled")
                # strategy_v2 §J3 fix #10 — promote ticker to the permanent
                # "traded today" block only after entry fill is confirmed.
                # Pre-fix, dispatch added the ticker immediately, so a
                # transient broker reject would permaban the symbol for the
                # rest of the session.
                with closed_trades_lock:
                    closed_tickers_today.add(ticker)

        # No open position to monitor
        if trade_closed:
            pass
        else:
            # Ensure exit leg orders exist (for new trades and resumed trades alike)
            if not target_order_id or not sl_order_id:
                try:
                    tick_size = get_tick_size(ticker)
                    result.target_price = round_to_tick(result.target_price, tick=tick_size)
                    result.stop_price = round_to_tick(result.stop_price, tick=tick_size)
                    target_order_id, sl_order_id = _place_exit_legs(
                        ticker=ticker,
                        exit_txn=exit_txn,
                        quantity=quantity,
                        target_price=result.target_price,
                        stop_price=result.stop_price,
                    )
                    result.target_order_id = target_order_id
                    result.sl_order_id = sl_order_id
                    _sync_active_position("exit_legs_placed")
                    _publish_open_trade_state("OPEN")
                    if resume_mode:
                        log.info(
                            f"[RESUME] Rebuilt exit legs for {ticker}: "
                            f"target_id={target_order_id} sl_id={sl_order_id}"
                        )
                    else:
                        log.info(
                            f"[LIVE] Exit legs placed for {ticker}: "
                            f"target_id={target_order_id} sl_id={sl_order_id}"
                        )
                except Exception as e:
                    log.error(f"[LIVE] Exit leg placement failed for {ticker}: {e}")
                    if target_order_id:
                        cancel_order_safe(kite.VARIETY_REGULAR, target_order_id)
                    if sl_order_id:
                        cancel_order_safe(kite.VARIETY_REGULAR, sl_order_id)
                    if resume_mode:
                        _sync_active_position("resume_pending_exit_legs")
                        return

                    # New trade fallback: attempt immediate market close.
                    if _force_market_close(
                        tag="AVWAPExitFailClose",
                        outcome="EXIT_SETUP_FAILED_FORCE_CLOSED",
                        timeout_sec=30,
                    ):
                        trade_closed = True
                        log.warning(
                            f"[LIVE] Exit setup fallback close for {ticker}: "
                            f"close_order_id={result.close_order_id} exit={result.exit_price}"
                        )
                    else:
                        _sync_active_position("exit_setup_failed_needs_reconcile")
                        return
            else:
                result.target_order_id = target_order_id
                result.sl_order_id = sl_order_id
                _sync_active_position("monitoring")
                _publish_open_trade_state("OPEN")

            # ---- STEP 5: Monitor until target/SL/forced close ----
            while (not trade_closed) and result.target_order_id and result.sl_order_id:
                now_ist = datetime.now(IST)

                if _check_kill_switch_and_close(stage="monitor_loop"):
                    break

                if now_ist >= forced_close_dt:
                    log.info(f"[LIVE] EOD forced close for {ticker}")
                    cancel_order_safe(kite.VARIETY_REGULAR, result.target_order_id)
                    cancel_order_safe(kite.VARIETY_REGULAR, result.sl_order_id)
                    broker_map = _get_broker_mis_position_qty_map()
                    if broker_map is not None:
                        broker_qty = int(broker_map.get(ticker, 0))
                        expected_sign = -1 if side == "SHORT" else 1
                        if broker_qty == 0 or (broker_qty * expected_sign) <= 0:
                            result.exit_price = result.filled_price or signal_entry_price
                            result.outcome = "EOD_FLAT_NO_FORCE_CLOSE"
                            trade_closed = True
                            log.warning(
                                f"[LIVE] Skipping EOD force-close for {ticker}: "
                                f"broker_qty={broker_qty} (already flat/opposite)."
                            )
                            break
                    if _force_market_close(
                        tag="AVWAPForceClose",
                        outcome="EOD_CLOSE",
                        timeout_sec=30,
                    ):
                        trade_closed = True
                        log.info(
                            f"[LIVE] Forced close: {ticker} @ {result.exit_price} "
                            f"ID={result.close_order_id}"
                        )
                    else:
                        _sync_active_position("forced_close_failed")
                        return
                    break

                try:
                    orders = _get_orders_snapshot(force_refresh=False)
                except Exception as e:
                    if _is_rate_limit_error(e):
                        log.warning(
                            "[LIVE] Order poll throttled by broker; "
                            "using shared orderbook backoff."
                        )
                    else:
                        log.warning(f"[LIVE] Order poll error: {e}")
                    time.sleep(ORDER_POLL_SEC)
                    continue

                target_filled = False
                sl_filled = False
                target_fill_price = result.target_price
                sl_fill_price = result.stop_price
                target_status = ""
                sl_status = ""

                for o in orders:
                    oid = str(o.get("order_id", ""))
                    status = str(o.get("status", "")).upper()
                    if oid == result.target_order_id:
                        target_status = status
                        if status == "COMPLETE":
                            target_filled = True
                            target_fill_price = float(o.get("average_price", result.target_price))
                    if oid == result.sl_order_id:
                        sl_status = status
                        if status == "COMPLETE":
                            sl_filled = True
                            sl_fill_price = float(o.get("average_price", result.stop_price))

                if target_filled:
                    log.info(
                        f"[LIVE] TARGET HIT: {ticker} @ {target_fill_price} "
                        f"-> cancelling SL {result.sl_order_id}"
                    )
                    cancel_order_safe(kite.VARIETY_REGULAR, result.sl_order_id)
                    result.exit_price = target_fill_price
                    result.outcome = "TARGET"
                    trade_closed = True
                    break

                if sl_filled:
                    log.info(
                        f"[LIVE] SL TRIGGERED: {ticker} @ {sl_fill_price} "
                        f"-> cancelling Target {result.target_order_id}"
                    )
                    cancel_order_safe(kite.VARIETY_REGULAR, result.target_order_id)
                    result.exit_price = sl_fill_price
                    result.outcome = "SL"
                    trade_closed = True
                    break

                target_invalid = target_status in {"REJECTED", "CANCELLED"}
                sl_invalid = sl_status in {"REJECTED", "CANCELLED"}
                if target_invalid or sl_invalid:
                    bad_legs = []
                    if target_invalid:
                        bad_legs.append(f"target={target_status}")
                    if sl_invalid:
                        bad_legs.append(f"sl={sl_status}")
                    log.error(
                        f"[LIVE] Exit leg invalid for {ticker}: {', '.join(bad_legs)}. "
                        "Cancelling remaining legs and forcing market close."
                    )
                    if result.target_order_id:
                        cancel_order_safe(kite.VARIETY_REGULAR, result.target_order_id)
                    if result.sl_order_id:
                        cancel_order_safe(kite.VARIETY_REGULAR, result.sl_order_id)
                    if _force_market_close(
                        tag="AVWAPLegFailClose",
                        outcome="EXIT_LEG_INVALID_FORCE_CLOSED",
                        timeout_sec=30,
                    ):
                        trade_closed = True
                        break
                    _sync_active_position("exit_leg_invalid_needs_reconcile")
                    return

                time.sleep(ORDER_POLL_SEC)

    except Exception as e:
        if (not resume_mode) and result.filled_price <= 0 and _is_mis_block_or_missing_error(e):
            err_text = _sanitize_error_message(e, limit=200)
            mark_symbol_mis_rejected(ticker, err_text)
            log.warning(
                f"[LIVE] Entry rejected for {ticker}: MIS blocked/missing. "
                f"Reason: {err_text}"
            )
            result.outcome = "ENTRY_REJECTED_MIS_BLOCKED_OR_MISSING"
            result.exit_price = signal_entry_price
            trade_closed = True
        elif resume_mode:
            log.error(f"[RESUME] Monitor error for {ticker}: {e}")
            log.error(traceback.format_exc())
            _sync_active_position("monitor_error")
            return
        else:
            log.error(f"[LIVE] Trade execution error for {ticker}: {e}")
            log.error(traceback.format_exc())
            result.outcome = f"ERROR: {_sanitize_error_message(e, limit=80)}"
            result.exit_price = result.filled_price or signal_entry_price
            # If a real position may still be open, keep it in state for restart reconciliation.
            if result.filled_price > 0:
                _sync_active_position("error_needs_reconcile")
                return
            trade_closed = True

    # If trade has not actually closed, preserve state and exit without booking P&L.
    if not trade_closed and result.outcome not in {"ENTRY_FAILED"}:
        if resume_mode:
            _sync_active_position("resume_incomplete")
        return

    result.exit_time = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S%z")
    ep = result.filled_price if result.filled_price > 0 else signal_entry_price
    xp = result.exit_price if result.exit_price > 0 else ep
    pnl_rs, pnl_pct = _calc_pnl(side, ep, xp, quantity)
    result.pnl_rs = round(pnl_rs, 2)
    result.pnl_pct = round(pnl_pct, 4)
    try:
        try:
            _log_trade_result(result)
        except Exception as csv_err:
            log.error(f"[LIVE] Trade CSV write failed for {ticker}: {csv_err}")
            log.error(traceback.format_exc())

        with daily_pnl_lock:
            daily_pnl["total"] += result.pnl_rs
            daily_pnl["trades"] += 1
            if result.pnl_rs > 0:
                daily_pnl["wins"] += 1
            elif result.pnl_rs < 0:
                daily_pnl["losses"] += 1
            _save_summary()
            day_total = float(daily_pnl["total"])

        log.info(
            f"[LIVE] RESULT {side} {ticker} | {result.outcome} | "
            f"Entry={ep} Exit={xp} | "
            f"P&L: Rs.{result.pnl_rs:+,.2f} ({result.pnl_pct:+.2f}%) | "
            f"Day total: Rs.{day_total:+,.2f}"
        )
    finally:
        _release_capacity(signal_id)
        with active_positions_lock:
            active_positions.pop(signal_id, None)
        try:
            _persist_open_trades_state()
        except Exception as state_err:
            log.error(f"[LIVE] Failed to persist open-trade cleanup state for {ticker}: {state_err}")


def _log_trade_result(result: LiveTradeResult) -> None:
    """Upsert trade result to daily CSV keyed by signal_id (fallback: trade_id)."""
    def _row_key(row: Dict[str, Any]) -> str:
        sid = str(row.get("signal_id", "")).strip()
        if sid:
            return f"sid:{sid}"
        tid = str(row.get("trade_id", "")).strip()
        return f"tid:{tid}" if tid else ""

    row = {
        "trade_id": result.trade_id,
        "signal_id": result.signal_id,
        "signal_datetime": result.signal_datetime,
        "signal_entry_datetime_ist": result.signal_entry_datetime_ist,
        "entry_time": result.entry_time,
        "exit_time": result.exit_time,
        "ticker": result.ticker,
        "side": result.side,
        "setup": result.setup,
        "impulse_type": result.impulse_type,
        "quantity": result.quantity,
        "entry_price": result.entry_price,
        "filled_price": result.filled_price,
        "exit_price": result.exit_price,
        "stop_price": result.stop_price,
        "target_price": result.target_price,
        "outcome": result.outcome,
        "pnl_rs": result.pnl_rs,
        "pnl_pct": result.pnl_pct,
        "entry_order_id": result.entry_order_id,
        "target_order_id": result.target_order_id,
        "sl_order_id": result.sl_order_id,
        "close_order_id": result.close_order_id,
        "quality_score": result.quality_score,
    }
    today_str = datetime.now(IST).strftime("%Y-%m-%d")
    csv_path = os.path.join(SIGNAL_DIR, TRADE_LOG_PATTERN.format(today_str))
    desired_key = _row_key(row)
    with trade_log_lock:
        rows: List[Dict[str, Any]] = []
        if os.path.exists(csv_path) and os.path.getsize(csv_path) > 0:
            try:
                with open(csv_path, "r", newline="", encoding="utf-8-sig") as f:
                    reader = csv.DictReader(f)
                    for existing in reader:
                        if not existing:
                            continue
                        rows.append(
                            {col: ("" if existing.get(col) is None else existing.get(col, "")) for col in TRADE_LOG_COLUMNS}
                        )
            except Exception as e:
                log.warning(f"[CSV] Could not read trade log for upsert: {e}")
                rows = []

        rewritten: List[Dict[str, Any]] = []
        replaced = False
        seen_keys: Set[str] = set()
        for existing in rows:
            key = _row_key(existing)
            if key and key in seen_keys:
                # De-duplicate legacy duplicate rows; keep most recent replacement.
                continue
            if desired_key and key == desired_key:
                if not replaced:
                    # Never downgrade a terminal outcome to a non-terminal placeholder.
                    # This guards against any future dispatch-vs-thread race where a
                    # non-terminal write (ENTRY_PENDING) arrives after the thread has
                    # already written the correct terminal outcome.
                    existing_outcome = str(existing.get("outcome", "")).strip()
                    new_outcome = str(row.get("outcome", "")).strip()
                    if existing_outcome not in NON_TERMINAL_OUTCOMES and new_outcome in NON_TERMINAL_OUTCOMES:
                        rewritten.append(existing)
                    else:
                        rewritten.append(row)
                    replaced = True
                    seen_keys.add(key)
                continue
            rewritten.append(existing)
            if key:
                seen_keys.add(key)
        if not replaced:
            rewritten.append(row)

        os.makedirs(SIGNAL_DIR, exist_ok=True)
        dir_path = os.path.dirname(csv_path) or "."
        tmp_path = ""
        try:
            fd, tmp_path = tempfile.mkstemp(
                prefix=".live_trades_v15.",
                suffix=".tmp",
                dir=dir_path,
            )
            with os.fdopen(fd, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=TRADE_LOG_COLUMNS, quoting=csv.QUOTE_ALL)
                writer.writeheader()
                for r in rewritten:
                    writer.writerow({col: r.get(col, "") for col in TRADE_LOG_COLUMNS})
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, csv_path)
        finally:
            if tmp_path and os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass


def _log_pending_signal_to_csv(signal: dict, signal_id: str, outcome: str = "ENTRY_PENDING") -> None:
    now_ist = datetime.now(IST)
    now_str = now_ist.strftime("%Y-%m-%d %H:%M:%S%z")
    entry_price = _safe_float(signal.get("entry_price", 0.0), 0.0)
    result = LiveTradeResult(
        trade_id=f"LT-{signal_id[:8]}-{now_ist.strftime('%H%M%S')}",
        signal_id=signal_id,
        signal_datetime=str(signal.get("signal_datetime", "")),
        signal_entry_datetime_ist=str(signal.get("signal_entry_datetime_ist", "")),
        entry_time=now_str,
        exit_time="",
        ticker=str(signal.get("ticker", "")).upper(),
        side=str(signal.get("side", "")).upper(),
        setup=str(signal.get("setup", "")),
        impulse_type=str(signal.get("impulse_type", "")),
        quantity=_safe_int(signal.get("quantity", 1), 1),
        entry_price=entry_price,
        filled_price=0.0,
        exit_price=0.0,
        stop_price=_safe_float(signal.get("stop_price", 0.0), 0.0),
        target_price=_safe_float(signal.get("target_price", 0.0), 0.0),
        outcome=str(outcome or "ENTRY_PENDING"),
        pnl_rs=0.0,
        pnl_pct=0.0,
        quality_score=_safe_float(signal.get("quality_score", 0.0), 0.0),
    )
    _log_trade_result(result)


def _save_summary() -> None:
    """Save running P&L summary to JSON."""
    try:
        wr = daily_pnl["wins"] / daily_pnl["trades"] * 100 if daily_pnl["trades"] > 0 else 0
        summary = {
            "date": datetime.now(IST).strftime("%Y-%m-%d"),
            "mode": "LIVE",
            "total_pnl_rs": round(daily_pnl["total"], 2),
            "total_trades": daily_pnl["trades"],
            "wins": daily_pnl["wins"],
            "losses": daily_pnl["losses"],
            "win_rate_pct": round(wr, 2),
            "last_updated": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S"),
        }
        _atomic_write_json(SUMMARY_FILE, summary)
    except Exception:
        pass


def _log_non_executed_signal_to_csv(signal: dict, signal_id: str, outcome: str) -> None:
    now_ist = datetime.now(IST)
    now_str = now_ist.strftime("%Y-%m-%d %H:%M:%S%z")
    entry_price = _safe_float(signal.get("entry_price", 0.0), 0.0)
    result = LiveTradeResult(
        trade_id=f"LT-{signal_id[:8]}-{now_ist.strftime('%H%M%S')}-SKIP",
        signal_id=signal_id,
        signal_datetime=str(signal.get("signal_datetime", "")),
        signal_entry_datetime_ist=str(signal.get("signal_entry_datetime_ist", "")),
        entry_time="",
        exit_time=now_str,
        ticker=str(signal.get("ticker", "")).upper(),
        side=str(signal.get("side", "")).upper(),
        setup=str(signal.get("setup", "")),
        impulse_type=str(signal.get("impulse_type", "")),
        quantity=_safe_int(signal.get("quantity", 1), 1),
        entry_price=entry_price,
        filled_price=0.0,
        exit_price=entry_price,
        stop_price=_safe_float(signal.get("stop_price", 0.0), 0.0),
        target_price=_safe_float(signal.get("target_price", 0.0), 0.0),
        outcome=str(outcome),
        pnl_rs=0.0,
        pnl_pct=0.0,
        quality_score=_safe_float(signal.get("quality_score", 0.0), 0.0),
    )
    _log_trade_result(result)


def hydrate_mis_rejected_symbols_from_trade_csv(trade_csv_path: str) -> int:
    if not os.path.exists(trade_csv_path) or os.path.getsize(trade_csv_path) <= 0:
        return 0

    detected: Set[str] = set()
    today_date = datetime.now(IST).date()
    try:
        df = pd.read_csv(
            trade_csv_path,
            quotechar='"',
            quoting=csv.QUOTE_ALL,
            on_bad_lines="warn",
            engine="python",
        )
        if df.empty:
            return 0
        for row in df.to_dict("records"):
            row_date = _signal_ist_date(row)
            if row_date is not None and row_date != today_date:
                continue
            ticker = str(row.get("ticker", "")).strip().upper()
            outcome = str(row.get("outcome", "")).strip().upper()
            if not ticker or not outcome:
                continue
            if (
                "ENTRY_REJECTED_MIS_BLOCKED_OR_MISSING" in outcome
                or "MIS ORDERS ARE CURRENTLY BLOCKED" in outcome
                or "MIS BLOCKED" in outcome
            ):
                detected.add(ticker)
    except Exception as e:
        log.warning(f"[MIS] Could not hydrate MIS-rejected symbols from trade CSV: {e}")
        return 0

    if not detected:
        return 0

    with mis_rejected_symbols_lock:
        before = len(mis_rejected_symbols)
        mis_rejected_symbols.update(detected)
        after = len(mis_rejected_symbols)
        snapshot = set(mis_rejected_symbols)
    if after > before:
        save_mis_rejected_symbols(snapshot)
    return len(detected)


def _load_closed_ids_and_realized_summary(
    trade_csv_path: str,
    today_date: date,
) -> Tuple[Set[str], Set[str], Dict[str, float]]:
    closed_ids: Set[str] = set()
    traded_tickers: Set[str] = set()
    realized_total = 0.0
    realized_trades = 0
    realized_wins = 0
    realized_losses = 0

    if os.path.exists(trade_csv_path) and os.path.getsize(trade_csv_path) > 0:
        try:
            df = pd.read_csv(
                trade_csv_path,
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
                    outcome = str(row.get("outcome", "")).strip().upper()
                    is_non_terminal = outcome in NON_TERMINAL_OUTCOMES
                    if sid and not is_non_terminal:
                        closed_ids.add(sid)
                    ticker = str(row.get("ticker", "")).strip().upper()
                    if ticker and not is_non_terminal:
                        traded_tickers.add(ticker)
                    if is_non_terminal:
                        continue
                    pnl_rs = _safe_float(row.get("pnl_rs", 0.0), 0.0)
                    realized_total += pnl_rs
                    realized_trades += 1
                    if pnl_rs > 0:
                        realized_wins += 1
                    elif pnl_rs < 0:
                        realized_losses += 1
        except Exception as e:
            log.warning(f"[RESTORE] Could not parse live trade CSV: {e}")

    return closed_ids, traded_tickers, {
        "realized_total": float(realized_total),
        "realized_trades": float(realized_trades),
        "realized_wins": float(realized_wins),
        "realized_losses": float(realized_losses),
    }


def _load_non_terminal_trade_rows(
    trade_csv_path: str,
    today_date: date,
) -> Dict[str, Dict[str, Any]]:
    rows_by_signal: Dict[str, Dict[str, Any]] = {}
    if not os.path.exists(trade_csv_path) or os.path.getsize(trade_csv_path) <= 0:
        return rows_by_signal

    try:
        df = pd.read_csv(
            trade_csv_path,
            quotechar='"',
            quoting=csv.QUOTE_ALL,
            on_bad_lines="warn",
            engine="python",
        )
        if df.empty:
            return rows_by_signal
        for row in df.to_dict("records"):
            row_date = _signal_ist_date(row)
            if row_date is not None and row_date != today_date:
                continue
            sid = str(row.get("signal_id", "")).strip()
            outcome = str(row.get("outcome", "")).strip().upper()
            if sid and outcome in NON_TERMINAL_OUTCOMES:
                rows_by_signal[sid] = dict(row)
    except Exception as e:
        log.warning(f"[RESTORE] Could not parse non-terminal live trade rows: {e}")

    return rows_by_signal


def _get_broker_mis_position_qty_map() -> Optional[Dict[str, int]]:
    """
    Return broker MIS net quantity per symbol.
    Returns None when broker positions cannot be fetched.
    """
    if kite is None:
        return {}
    try:
        payload = kite.positions()
    except Exception as e:
        log.warning(f"[RESTORE] Could not fetch broker MIS positions snapshot: {e}")
        return None

    net_rows = payload.get("net", []) if isinstance(payload, dict) else []
    if not isinstance(net_rows, list):
        return {}

    out: Dict[str, int] = {}
    for row in net_rows:
        ticker = str(row.get("tradingsymbol", "")).strip().upper()
        if not ticker:
            continue
        product = str(row.get("product", "")).strip().upper()
        if product and product != "MIS":
            continue
        qty = int(_safe_int(row.get("quantity", 0), 0))
        if qty == 0:
            continue
        out[ticker] = out.get(ticker, 0) + qty
    return out


def _order_time_to_csv_str(row: Optional[dict]) -> str:
    row = row or {}
    for key in ("exchange_timestamp", "exchange_update_timestamp", "order_timestamp"):
        value = row.get(key)
        if isinstance(value, datetime):
            ts = value if value.tzinfo else IST.localize(value)
            return ts.astimezone(IST).strftime("%Y-%m-%d %H:%M:%S%z")
        if isinstance(value, str) and value.strip():
            raw = value.strip()
            for parser in (datetime.fromisoformat,):
                try:
                    parsed = parser(raw)
                    ts = parsed if parsed.tzinfo else IST.localize(parsed)
                    return ts.astimezone(IST).strftime("%Y-%m-%d %H:%M:%S%z")
                except Exception:
                    continue
            if len(raw) == 19:
                try:
                    parsed = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")
                    return IST.localize(parsed).strftime("%Y-%m-%d %H:%M:%S%z")
                except Exception:
                    pass
    return datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S%z")


def _order_has_tag(row: Optional[dict], wanted_tag: str) -> bool:
    if not row or not wanted_tag:
        return False
    wanted = str(wanted_tag).strip().upper()
    if not wanted:
        return False
    tag = str(row.get("tag", "")).strip().upper()
    if tag == wanted:
        return True
    tags = row.get("tags", [])
    if isinstance(tags, list):
        return any(str(x).strip().upper() == wanted for x in tags)
    return False


def _is_live_order_status(status: str) -> bool:
    return str(status or "").strip().upper() not in {"", "COMPLETE", "CANCELLED", "REJECTED"}


def _pick_latest_tagged_order(
    orders: Sequence[dict],
    ticker: str,
    wanted_tag: str,
    transaction_type: str,
) -> Optional[dict]:
    ticker = str(ticker or "").strip().upper()
    wanted_tag = str(wanted_tag or "").strip().upper()
    transaction_type = str(transaction_type or "").strip().upper()
    if not ticker or not wanted_tag or not transaction_type:
        return None

    matches: List[dict] = []
    for row in orders:
        if str(row.get("tradingsymbol", "")).strip().upper() != ticker:
            continue
        if str(row.get("transaction_type", "")).strip().upper() != transaction_type:
            continue
        if not _order_has_tag(row, wanted_tag):
            continue
        matches.append(row)

    if not matches:
        return None

    def _sort_key(row: dict) -> Tuple[str, str]:
        ts = (
            str(row.get("exchange_update_timestamp", "")).strip()
            or str(row.get("exchange_timestamp", "")).strip()
            or str(row.get("order_timestamp", "")).strip()
        )
        return ts, str(row.get("order_id", "")).strip()

    matches.sort(key=_sort_key)
    return matches[-1]


def _close_outcome_from_order(row: Optional[dict]) -> str:
    if _order_has_tag(row, "AVWAPKILLSWITCH"):
        return "MANUAL_KILL_SWITCH"
    if _order_has_tag(row, "AVWAPFORCECLOSE") or _order_has_tag(row, "AVWAPCUTOFFCLOSE"):
        return "EOD_CLOSE"
    if _order_has_tag(row, "AVWAPEXITFAILCLOSE"):
        return "EXIT_SETUP_FAILED_FORCE_CLOSED"
    return "BROKER_CLOSE_RESTORED"


def _restore_intraday_runtime_state(
    signal_csv_paths: Sequence[str],
    trade_csv_path: str,
    executed: Set[str],
) -> Tuple[Dict[str, float], List[dict]]:
    """
    Restore runtime state after mid-session restart using persisted open-trade state.
    """
    today_str = datetime.now(IST).strftime("%Y-%m-%d")
    today_date = datetime.now(IST).date()

    signal_rows = read_signals_csv_multi(signal_csv_paths)
    signals_by_id: Dict[str, dict] = {}
    for sig in signal_rows:
        sid = str(sig.get("signal_id", "")).strip()
        if sid:
            signals_by_id[sid] = sig

    closed_ids, traded_tickers, realized = _load_closed_ids_and_realized_summary(
        trade_csv_path=trade_csv_path,
        today_date=today_date,
    )

    with daily_pnl_lock:
        daily_pnl["total"] = float(realized["realized_total"])
        daily_pnl["trades"] = int(realized["realized_trades"])
        daily_pnl["wins"] = int(realized["realized_wins"])
        daily_pnl["losses"] = int(realized["realized_losses"])
        _save_summary()

    raw_open_state = _load_open_trades_state(today_str)
    non_terminal_trade_rows = _load_non_terminal_trade_rows(
        trade_csv_path=trade_csv_path,
        today_date=today_date,
    )
    restore_candidates: Dict[str, Dict[str, Any]] = {
        sid: dict(row) for sid, row in non_terminal_trade_rows.items()
    }
    for sid, pos in raw_open_state.items():
        merged = dict(restore_candidates.get(sid, {}))
        merged.update(pos)
        restore_candidates[sid] = merged

    broker_qty_map = _get_broker_mis_position_qty_map()
    try:
        orders_snapshot = _get_orders_snapshot(force_refresh=True)
    except Exception as e:
        if not _is_rate_limit_error(e):
            log.warning(f"[RESTORE] Could not fetch broker order snapshot: {e}")
        orders_snapshot = []

    restored_positions: Dict[str, Dict[str, Any]] = {}
    resume_signals: List[dict] = []
    stale_restored: List[Dict[str, Any]] = []
    reconciled_realized_total = 0.0
    reconciled_realized_trades = 0
    reconciled_realized_wins = 0
    reconciled_realized_losses = 0

    for sid, pos in restore_candidates.items():
        if sid in closed_ids:
            continue

        base = signals_by_id.get(sid, {})
        ticker = str(pos.get("ticker") or base.get("ticker") or "").upper()
        side = str(pos.get("side") or base.get("side") or "").upper()
        qty = _safe_int(pos.get("quantity", base.get("quantity", 1)), 1)
        entry_price = _safe_float(pos.get("entry_price", base.get("entry_price", 0.0)), 0.0)
        filled_price = _safe_float(pos.get("filled_price", entry_price), entry_price)
        stop_price = _safe_float(pos.get("stop_price", base.get("stop_price", 0.0)), 0.0)
        target_price = _safe_float(pos.get("target_price", base.get("target_price", 0.0)), 0.0)
        entry_order_id = _clean_order_id(pos.get("entry_order_id", ""))
        target_order_id = _clean_order_id(pos.get("target_order_id", ""))
        sl_order_id = _clean_order_id(pos.get("sl_order_id", ""))
        close_order_id = _clean_order_id(pos.get("close_order_id", ""))
        trade_id = str(pos.get("trade_id", "")).strip() or f"RESTORE-{sid[:8]}"
        entry_time = str(pos.get("entry_time", "")).strip()

        if not ticker or side not in {"LONG", "SHORT"}:
            continue
        if qty <= 0 or stop_price <= 0 or target_price <= 0:
            continue

        entry_txn = "BUY" if side == "LONG" else "SELL"
        exit_txn = "SELL" if side == "LONG" else "BUY"

        if not entry_order_id:
            discovered = _pick_latest_tagged_order(orders_snapshot, ticker, "AVWAPENTRY", entry_txn)
            if discovered:
                entry_order_id = str(discovered.get("order_id", "")).strip()
        if not target_order_id:
            discovered = _pick_latest_tagged_order(orders_snapshot, ticker, "AVWAPTARGET", exit_txn)
            if discovered:
                target_order_id = str(discovered.get("order_id", "")).strip()
        if not sl_order_id:
            discovered = _pick_latest_tagged_order(orders_snapshot, ticker, "AVWAPSTOPLOSS", exit_txn)
            if discovered:
                sl_order_id = str(discovered.get("order_id", "")).strip()
        if not close_order_id:
            for close_tag in ("AVWAPFORCECLOSE", "AVWAPCUTOFFCLOSE", "AVWAPKILLSWITCH", "AVWAPEXITFAILCLOSE"):
                discovered = _pick_latest_tagged_order(orders_snapshot, ticker, close_tag, exit_txn)
                if discovered:
                    close_order_id = str(discovered.get("order_id", "")).strip()
                    break

        entry_row = _get_order_row(entry_order_id, force_refresh=True, allow_history_fallback=True) if entry_order_id else None
        target_row = _get_order_row(target_order_id, force_refresh=False, allow_history_fallback=True) if target_order_id else None
        sl_row = _get_order_row(sl_order_id, force_refresh=False, allow_history_fallback=True) if sl_order_id else None
        close_row = _get_order_row(close_order_id, force_refresh=False, allow_history_fallback=True) if close_order_id else None
        entry_state = _order_state_from_row(entry_row)
        target_state = _order_state_from_row(target_row)
        sl_state = _order_state_from_row(sl_row)
        close_state = _order_state_from_row(close_row)

        if entry_state["filled_quantity"] > 0:
            qty = max(qty, int(entry_state["filled_quantity"]))
        if filled_price <= 0 and entry_state["average_price"] > 0:
            filled_price = float(entry_state["average_price"])
        if entry_price <= 0:
            entry_price = float(filled_price if filled_price > 0 else entry_state["average_price"])
        if filled_price <= 0:
            filled_price = float(entry_price)
        if not entry_time:
            entry_time = _order_time_to_csv_str(entry_row)

        terminal_outcome = ""
        terminal_exit_price = 0.0
        terminal_exit_row: Optional[dict] = None

        if target_state["status"] == "COMPLETE" and target_state["filled_quantity"] > 0:
            terminal_outcome = "TARGET"
            terminal_exit_price = float(target_state["average_price"] or target_price)
            terminal_exit_row = target_row
        elif sl_state["status"] == "COMPLETE" and sl_state["filled_quantity"] > 0:
            terminal_outcome = "SL"
            terminal_exit_price = float(sl_state["average_price"] or stop_price)
            terminal_exit_row = sl_row
        elif close_state["status"] == "COMPLETE" and close_state["filled_quantity"] > 0:
            terminal_outcome = _close_outcome_from_order(close_row)
            terminal_exit_price = float(close_state["average_price"] or filled_price or entry_price)
            terminal_exit_row = close_row
        elif entry_state["status"] in {"REJECTED", "CANCELLED"} and entry_state["filled_quantity"] <= 0:
            terminal_outcome = "ENTRY_FAILED"
            terminal_exit_price = float(entry_price)
            terminal_exit_row = entry_row

        if terminal_outcome:
            pnl_rs, pnl_pct = _calc_pnl(side, float(filled_price or entry_price), terminal_exit_price, int(qty))
            restored_result = LiveTradeResult(
                trade_id=trade_id,
                signal_id=sid,
                signal_datetime=str(base.get("signal_datetime", pos.get("signal_datetime", ""))),
                signal_entry_datetime_ist=str(
                    base.get("signal_entry_datetime_ist", pos.get("signal_entry_datetime_ist", ""))
                ),
                entry_time=entry_time,
                exit_time=_order_time_to_csv_str(terminal_exit_row),
                ticker=ticker,
                side=side,
                setup=str(base.get("setup", pos.get("setup", ""))),
                impulse_type=str(base.get("impulse_type", pos.get("impulse_type", ""))),
                quantity=int(qty),
                entry_price=float(entry_price or filled_price),
                filled_price=float(filled_price or entry_price),
                exit_price=float(terminal_exit_price),
                stop_price=float(stop_price),
                target_price=float(target_price),
                outcome=terminal_outcome,
                pnl_rs=float(pnl_rs),
                pnl_pct=float(pnl_pct),
                entry_order_id=entry_order_id,
                target_order_id=target_order_id,
                sl_order_id=sl_order_id,
                close_order_id=close_order_id,
                quality_score=_safe_float(base.get("quality_score", pos.get("quality_score", 0.0)), 0.0),
            )
            _log_trade_result(restored_result)
            closed_ids.add(sid)
            traded_tickers.add(ticker)
            reconciled_realized_total += float(pnl_rs)
            reconciled_realized_trades += 1
            if pnl_rs > 0:
                reconciled_realized_wins += 1
            elif pnl_rs < 0:
                reconciled_realized_losses += 1
            log.info(
                f"[RESTORE] Reconciled {ticker} signal_id={sid[:12]} as {terminal_outcome} "
                f"@ {terminal_exit_price:.2f}."
            )
            continue

        expected_signed_qty = int(qty if side == "LONG" else -qty)
        broker_qty = int(broker_qty_map.get(ticker, 0)) if broker_qty_map is not None else expected_signed_qty
        same_direction = (
            (broker_qty > 0 and expected_signed_qty > 0)
            or (broker_qty < 0 and expected_signed_qty < 0)
        )
        live_exit_orders = _is_live_order_status(target_state["status"]) or _is_live_order_status(sl_state["status"])
        should_restore_open = bool(
            (broker_qty_map is None)
            or (same_direction and abs(broker_qty) >= abs(expected_signed_qty))
            or live_exit_orders
        )
        if not should_restore_open:
            stale_restored.append(
                {
                    "signal_id": sid,
                    "ticker": ticker,
                    "expected_qty": expected_signed_qty,
                    "broker_qty": broker_qty,
                }
            )
            continue

        restored_positions[sid] = {
            "signal_id": sid,
            "trade_id": trade_id,
            "ticker": ticker,
            "side": side,
            "quantity": int(qty),
            "entry_price": float(entry_price if entry_price > 0 else filled_price),
            "filled_price": float(filled_price),
            "stop_price": float(stop_price),
            "target_price": float(target_price),
            "entry_order_id": entry_order_id,
            "target_order_id": target_order_id,
            "sl_order_id": sl_order_id,
            "close_order_id": close_order_id,
            "entry_time": entry_time,
            "signal_datetime": str(base.get("signal_datetime", pos.get("signal_datetime", ""))),
            "signal_entry_datetime_ist": str(
                base.get("signal_entry_datetime_ist", pos.get("signal_entry_datetime_ist", ""))
            ),
            "setup": str(base.get("setup", pos.get("setup", ""))),
            "impulse_type": str(base.get("impulse_type", pos.get("impulse_type", ""))),
            "quality_score": _safe_float(base.get("quality_score", pos.get("quality_score", 0.0)), 0.0),
            "stage": str(pos.get("stage", "restored")),
            "updated_at": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S"),
        }
        resume_signals.append(
            {
                "signal_id": sid,
                "trade_id": trade_id,
                "ticker": ticker,
                "side": side,
                "quantity": int(qty),
                "entry_price": float(entry_price if entry_price > 0 else filled_price),
                "filled_price": float(filled_price),
                "stop_price": float(stop_price),
                "target_price": float(target_price),
                "entry_order_id": entry_order_id,
                "target_order_id": target_order_id,
                "sl_order_id": sl_order_id,
                "close_order_id": close_order_id,
                "entry_time": entry_time,
                "signal_datetime": str(base.get("signal_datetime", pos.get("signal_datetime", ""))),
                "signal_entry_datetime_ist": str(
                    base.get("signal_entry_datetime_ist", pos.get("signal_entry_datetime_ist", ""))
                ),
                "setup": str(base.get("setup", pos.get("setup", ""))),
                "impulse_type": str(base.get("impulse_type", pos.get("impulse_type", ""))),
                "quality_score": _safe_float(base.get("quality_score", pos.get("quality_score", 0.0)), 0.0),
            }
        )

    realized["realized_total"] = float(realized["realized_total"] + reconciled_realized_total)
    realized["realized_trades"] = float(realized["realized_trades"] + reconciled_realized_trades)
    realized["realized_wins"] = float(realized["realized_wins"] + reconciled_realized_wins)
    realized["realized_losses"] = float(realized["realized_losses"] + reconciled_realized_losses)
    with daily_pnl_lock:
        daily_pnl["total"] = float(realized["realized_total"])
        daily_pnl["trades"] = int(realized["realized_trades"])
        daily_pnl["wins"] = int(realized["realized_wins"])
        daily_pnl["losses"] = int(realized["realized_losses"])
        _save_summary()

    if stale_restored:
        stale_ids = {str(x.get("signal_id", "")).strip() for x in stale_restored if str(x.get("signal_id", "")).strip()}
        stale_tickers = {
            str(x.get("ticker", "")).upper().strip() for x in stale_restored if str(x.get("ticker", "")).strip()
        }
        closed_ids.update(stale_ids)
        traded_tickers.update(stale_tickers)
        for row in stale_restored:
            sid = str(row.get("signal_id", "")).strip()
            ticker = str(row.get("ticker", "")).upper().strip()
            log.warning(
                "[RESTORE] Dropping stale tracked state "
                f"{ticker or '?'} signal_id={sid[:12]} "
                f"(expected_qty={row.get('expected_qty', 0)}, broker_qty={row.get('broker_qty', 0)})."
            )
            # Upsert the stuck CSV row (typically ENTRY_PENDING from a killed
            # thread) to a terminal outcome so it does not linger forever.
            if not sid:
                continue
            candidate = restore_candidates.get(sid, {}) or {}
            base = signals_by_id.get(sid, {}) or {}
            try:
                abort_result = LiveTradeResult(
                    trade_id=str(candidate.get("trade_id", "")).strip() or f"ABORT-{sid[:8]}",
                    signal_id=sid,
                    signal_datetime=str(candidate.get("signal_datetime", base.get("signal_datetime", ""))),
                    signal_entry_datetime_ist=str(
                        candidate.get("signal_entry_datetime_ist",
                                      base.get("signal_entry_datetime_ist", ""))
                    ),
                    entry_time=str(candidate.get("entry_time", "")),
                    exit_time=datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S%z"),
                    ticker=ticker,
                    side=str(candidate.get("side", base.get("side", ""))).upper(),
                    setup=str(candidate.get("setup", base.get("setup", ""))),
                    impulse_type=str(candidate.get("impulse_type", base.get("impulse_type", ""))),
                    quantity=_safe_int(candidate.get("quantity", base.get("quantity", 0)), 0),
                    entry_price=_safe_float(candidate.get("entry_price", base.get("entry_price", 0.0)), 0.0),
                    filled_price=0.0,
                    exit_price=_safe_float(candidate.get("entry_price", base.get("entry_price", 0.0)), 0.0),
                    stop_price=_safe_float(candidate.get("stop_price", base.get("stop_price", 0.0)), 0.0),
                    target_price=_safe_float(candidate.get("target_price", base.get("target_price", 0.0)), 0.0),
                    outcome="ENTRY_ABORTED_ON_RESTART",
                    pnl_rs=0.0,
                    pnl_pct=0.0,
                    quality_score=_safe_float(
                        candidate.get("quality_score", base.get("quality_score", 0.0)), 0.0
                    ),
                )
                _log_trade_result(abort_result)
            except Exception as e:
                log.warning(f"[RESTORE] Failed to finalize stuck CSV row for {sid[:12]}: {e}")

    with active_positions_lock:
        active_positions.clear()
        active_positions.update(restored_positions)

    with capital_lock:
        capital_deployed.clear()
        for sid, pos in restored_positions.items():
            margin = float(pos["filled_price"] * pos["quantity"] / INTRADAY_LEVERAGE)
            capital_deployed[sid] = margin

    with closed_trades_lock:
        closed_signal_ids_today.clear()
        closed_signal_ids_today.update(closed_ids)
        closed_signal_ids_today.update(restored_positions.keys())
        closed_tickers_today.clear()
        closed_tickers_today.update(traded_tickers)
        closed_tickers_today.update(
            {
                str(pos.get("ticker", "")).upper()
                for pos in restored_positions.values()
                if str(pos.get("ticker", "")).strip()
            }
        )

    backlog_ids: Set[str] = set()
    with executed_lock:
        # Keep all closed signal_ids blocked for the full trading day.
        # Otherwise, a restart can re-dispatch old rows still present in signal CSVs.
        executed.update(closed_ids)
        executed.update(restored_positions.keys())
        # Re-queue backlog signals that were marked executed earlier but never reached
        # open position state and also have no terminal outcome row.
        backlog_ids = {
            sid
            for sid in executed
            if sid in signals_by_id
            and sid not in closed_ids
            and sid not in restored_positions
        }
        if backlog_ids:
            executed.difference_update(backlog_ids)

    _persist_open_trades_state()
    with capital_lock:
        deployed_margin = float(sum(capital_deployed.values()))
    stats = {
        "signals_today": float(len(signals_by_id)),
        "executed_loaded": float(len(executed)),
        "closed_today": float(len(closed_ids)),
        "open_restored": float(len(restored_positions)),
        "stale_dropped": float(len(stale_restored)),
        "realized_trades": float(realized["realized_trades"]),
        "realized_total": float(realized["realized_total"]),
        "deployed_margin": float(deployed_margin),
        "requeued_backlog": float(len(backlog_ids)),
    }
    return stats, resume_signals


def _detect_orphan_broker_positions() -> List[dict]:
    """
    Detect open MIS positions present at broker but not tracked by executor state.
    """
    broker_qty_map = _get_broker_mis_position_qty_map()
    if broker_qty_map is None:
        return []

    with active_positions_lock:
        tracked: Dict[str, int] = {}
        for pos in active_positions.values():
            t = str(pos.get("ticker", "")).upper().strip()
            if not t:
                continue
            signed_qty = int(
                _safe_int(pos.get("quantity", 0), 0)
                * (-1 if str(pos.get("side", "")).upper() == "SHORT" else 1)
            )
            tracked[t] = tracked.get(t, 0) + signed_qty

    orphans: List[dict] = []
    for ticker, qty in broker_qty_map.items():
        if qty == 0:
            continue
        tracked_qty = tracked.get(ticker)
        if tracked_qty is None or tracked_qty != qty:
            orphans.append(
                {
                    "ticker": ticker,
                    "quantity": qty,
                    "product": "MIS",
                    "average_price": 0.0,
                }
            )
    return orphans


def _detect_stale_tracked_positions() -> List[dict]:
    """
    Detect tracked runtime positions that are absent/mismatched at broker.
    """
    broker_qty_map = _get_broker_mis_position_qty_map()
    if broker_qty_map is None:
        return []

    stale: List[dict] = []
    with active_positions_lock:
        snapshot = [dict(v) for v in active_positions.values()]

    for pos in snapshot:
        sid = str(pos.get("signal_id", "")).strip()
        ticker = str(pos.get("ticker", "")).upper().strip()
        side = str(pos.get("side", "")).upper().strip()
        qty = int(_safe_int(pos.get("quantity", 0), 0))
        if not sid or not ticker or qty <= 0 or side not in {"LONG", "SHORT"}:
            continue
        expected_qty = qty if side == "LONG" else -qty
        broker_qty = int(broker_qty_map.get(ticker, 0))
        same_direction = (
            (broker_qty > 0 and expected_qty > 0)
            or (broker_qty < 0 and expected_qty < 0)
        )
        if broker_qty == 0 or (not same_direction) or abs(broker_qty) < abs(expected_qty):
            target_order_id = _clean_order_id(pos.get("target_order_id", ""))
            sl_order_id = _clean_order_id(pos.get("sl_order_id", ""))
            close_order_id = _clean_order_id(pos.get("close_order_id", ""))
            target_state = _order_state_from_row(
                _get_order_row(target_order_id, force_refresh=False, allow_history_fallback=True)
            ) if target_order_id else {}
            sl_state = _order_state_from_row(
                _get_order_row(sl_order_id, force_refresh=False, allow_history_fallback=True)
            ) if sl_order_id else {}
            close_state = _order_state_from_row(
                _get_order_row(close_order_id, force_refresh=False, allow_history_fallback=True)
            ) if close_order_id else {}
            if (
                _is_live_order_status(target_state.get("status", ""))
                or _is_live_order_status(sl_state.get("status", ""))
                or str(target_state.get("status", "")).upper() == "COMPLETE"
                or str(sl_state.get("status", "")).upper() == "COMPLETE"
                or str(close_state.get("status", "")).upper() == "COMPLETE"
            ):
                continue
            stale.append(
                {
                    "signal_id": sid,
                    "ticker": ticker,
                    "expected_qty": expected_qty,
                    "broker_qty": broker_qty,
                }
            )
    return stale


# ============================================================================
# SIGNAL NORMALISATION
# ============================================================================
def _normalize_signal(raw: dict) -> dict:
    """
    Map signal-generator CSV column names to the names the executor expects.
    Preserve signal quantity/SL/target when present so execution stays aligned
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

    # Live mode: reject rows with missing SL/target rather than silently backfilling.
    # Backfilling hides upstream data-quality problems; the downstream dispatch check
    # (entry_price/stop_price/target_price <= 0) will never fire if we heal the values
    # here. Instead, leave them at 0 so the dispatch loop logs ENTRY_SKIPPED and skips.
    side = str(sig.get("side", "")).strip().upper()
    entry = _safe_float(sig.get("entry_price", 0.0), 0.0)
    stop_price = _safe_float(sig.get("stop_price", 0.0), 0.0)
    target_price = _safe_float(sig.get("target_price", 0.0), 0.0)
    if entry > 0 and side in {"SHORT", "LONG"} and (stop_price <= 0 or target_price <= 0):
        log.warning(
            f"[NORMALIZE] Signal has missing stop_price={stop_price:.4f} or "
            f"target_price={target_price:.4f} from upstream — "
            f"refusing to backfill in live mode. Signal will be rejected at dispatch."
        )

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


def _signal_ist_date(sig: dict) -> Optional[date]:
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


def _sanitize_today_trade_csv() -> Tuple[int, int]:
    """
    Ensure today's live trade CSV only has rows whose signal time is from today (IST).
    Returns (rows_before, rows_removed).
    """
    today_str = datetime.now(IST).strftime("%Y-%m-%d")
    today_date = datetime.now(IST).date()
    csv_path = os.path.join(SIGNAL_DIR, TRADE_LOG_PATTERN.format(today_str))
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
        # Keep unparsable rows for safety; only drop rows that are positively stale.
        keep_mask.append(row_date == today_date if row_date is not None else True)

    df_today = df[pd.Series(keep_mask, index=df.index)].copy()
    if "signal_id" in df_today.columns:
        sid = df_today["signal_id"].astype(str).str.strip()
        df_today = df_today.assign(_sid=sid)
        df_today = df_today[df_today["_sid"] != ""]
        df_today = df_today.drop_duplicates(subset=["_sid"], keep="last")
        df_today = df_today.drop(columns=["_sid"])
    rows_removed = rows_before - int(len(df_today))
    if rows_removed <= 0:
        return rows_before, 0

    for c in TRADE_LOG_COLUMNS:
        if c not in df_today.columns:
            df_today[c] = ""
    df_today = df_today[TRADE_LOG_COLUMNS]
    df_today.to_csv(csv_path, index=False, quoting=csv.QUOTE_ALL)
    return rows_before, rows_removed


def _ensure_today_trade_csv_exists() -> str:
    """Create today's live trade CSV with header so dashboard cards don't stay missing pre-trade."""
    today_str = datetime.now(IST).strftime("%Y-%m-%d")
    csv_path = os.path.join(SIGNAL_DIR, TRADE_LOG_PATTERN.format(today_str))
    os.makedirs(SIGNAL_DIR, exist_ok=True)
    if os.path.exists(csv_path) and os.path.getsize(csv_path) > 0:
        return csv_path

    dir_path = os.path.dirname(csv_path) or "."
    tmp_path = ""
    try:
        fd, tmp_path = tempfile.mkstemp(
            prefix=".live_trades_id_5min_v7.",
            suffix=".tmp",
            dir=dir_path,
        )
        with os.fdopen(fd, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=TRADE_LOG_COLUMNS, quoting=csv.QUOTE_ALL)
            writer.writeheader()
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, csv_path)
    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass
    return csv_path


def _backfill_missing_trade_rows(
    signal_csv_paths: Sequence[str],
    executed: Set[str],
) -> int:
    if not executed:
        return 0
    signals = read_signals_csv_multi(signal_csv_paths)
    if not signals:
        return 0
    by_id: Dict[str, dict] = {}
    for sig in signals:
        sid = str(sig.get("signal_id", "")).strip()
        if sid:
            by_id[sid] = sig
    if not by_id:
        return 0

    today_str = datetime.now(IST).strftime("%Y-%m-%d")
    csv_path = os.path.join(SIGNAL_DIR, TRADE_LOG_PATTERN.format(today_str))
    existing_ids: Set[str] = set()
    if os.path.exists(csv_path) and os.path.getsize(csv_path) > 0:
        try:
            with open(csv_path, "r", newline="", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    sid = str((row or {}).get("signal_id", "")).strip()
                    if sid:
                        existing_ids.add(sid)
        except Exception as e:
            log.warning(f"[CSV] Could not scan trade CSV for backfill: {e}")

    missing_ids = [sid for sid in sorted(executed) if sid in by_id and sid not in existing_ids]
    for sid in missing_ids:
        _log_pending_signal_to_csv(by_id[sid], sid, outcome="ENTRY_PENDING_RECOVERED")
    return len(missing_ids)


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


# ============================================================================
# SIGNAL PROCESSOR
# ============================================================================
def _launch_trade_thread(
    signal: dict,
    signal_id: str,
    trade_semaphore: Optional[threading.Semaphore],
    resume_mode: bool = False,
) -> bool:
    """
    strategy_v2 §J3 fix #9 — if ``trade_semaphore`` is not None, the caller
    MUST have already acquired one permit on it; the worker thread is only
    responsible for releasing that permit in its finally block. Pre-fix,
    the thread acquired inside _run_trade, which meant signals were admitted
    (marked executed, held ticker/margin slots) while 45+ workers queued on
    the semaphore — if the process was killed before they drained, their
    slots were lost for the rest of the session.
    """
    ticker_for_cleanup = str(signal.get("ticker", "")).upper()

    def _run_trade(
        sig=signal,
        sid=signal_id,
        is_resume=resume_mode,
        tkr=ticker_for_cleanup,
    ):
        with inflight_signals_lock:
            if sid in inflight_signals:
                return
            inflight_signals.add(sid)
        try:
            # strategy_v2 §J3 fix #1 — wait until the deterministic bar-open
            # entry slot before calling execute_live_trade so live trades
            # align with the same bar-open that the backtest assumes.
            # Skipped for resumes since the trade is already mid-lifecycle.
            if not is_resume:
                try:
                    entry_slot_ts = _parse_ist_signal_ts(
                        sig.get("signal_entry_datetime_ist")
                        or sig.get("signal_bar_time_ist")
                        or sig.get("bar_time_ist")
                    )
                    if entry_slot_ts is not None:
                        entry_slot_dt = entry_slot_ts.to_pydatetime()
                        now_ist = datetime.now(IST)
                        wait_sec = (entry_slot_dt - now_ist).total_seconds()
                        if wait_sec > ENTRY_SLOT_MAX_WAIT_SEC > 0:
                            log.warning(
                                f"[ENTRY.SLOT] {tkr} signal_id={sid[:12]}: "
                                f"entry_slot {entry_slot_dt.strftime('%H:%M:%S')} "
                                f"is {wait_sec:.0f}s away "
                                f"(> cap {ENTRY_SLOT_MAX_WAIT_SEC}s); skipping."
                            )
                            _log_non_executed_signal_to_csv(
                                sig, sid, "ENTRY_SKIPPED_SLOT_TOO_FAR"
                            )
                            with closed_trades_lock:
                                closed_signal_ids_today.add(sid)
                            return
                        if wait_sec > 0:
                            log.info(
                                f"[ENTRY.SLOT] {tkr} signal_id={sid[:12]}: "
                                f"sleeping {wait_sec:.1f}s until bar-open "
                                f"{entry_slot_dt.strftime('%H:%M:%S')}"
                            )
                            time.sleep(wait_sec)
                except Exception as slot_err:
                    log.warning(
                        f"[ENTRY.SLOT] {tkr} signal_id={sid[:12]}: "
                        f"slot-gate error: {slot_err}"
                    )
            execute_live_trade(sig, resume_mode=is_resume)
        except Exception as e:
            mode = "RESUME" if is_resume else "LIVE"
            log.error(f"{mode} trade thread error for {sig.get('ticker', '?')}: {e}")
            log.error(traceback.format_exc())
        finally:
            if trade_semaphore is not None:
                trade_semaphore.release()
            with inflight_signals_lock:
                inflight_signals.discard(sid)
            with active_trades_lock:
                active_trades.pop(sid, None)
            if tkr:
                # strategy_v2 §J3 fix #10 — release the transient admission
                # set. If the trade filled, execute_live_trade has already
                # promoted the ticker to closed_tickers_today.
                with closed_trades_lock:
                    dispatched_tickers.discard(tkr)

    try:
        t = threading.Thread(
            target=_run_trade,
            daemon=True,
            name=f"{'resume' if resume_mode else 'live'}-trade-{signal_id[:8]}",
        )
        with active_trades_lock:
            active_trades[signal_id] = t
        t.start()
        return True
    except Exception:
        with active_trades_lock:
            active_trades.pop(signal_id, None)
        with inflight_signals_lock:
            inflight_signals.discard(signal_id)
        return False


def start_resumed_trade_monitors(
    resumed_signals: List[dict],
    trade_semaphore: Optional[threading.Semaphore],
    dry_run: bool,
) -> int:
    if dry_run or not resumed_signals:
        return 0

    started = 0
    for signal in resumed_signals:
        sid = str(signal.get("signal_id", "")).strip()
        if not sid:
            continue
        # strategy_v2 §J3 fix #9 — pre-acquire the concurrency permit before
        # launching the thread so a blocked semaphore can't hold an admitted
        # trade hostage in a queued state.
        acquired_sem: Optional[threading.Semaphore] = None
        if trade_semaphore is not None:
            if not trade_semaphore.acquire(blocking=False):
                log.warning(
                    f"[RESUME] Concurrency cap reached for signal_id={sid[:12]}; "
                    "waiting for a permit."
                )
                trade_semaphore.acquire()
            acquired_sem = trade_semaphore
        ok = _launch_trade_thread(
            signal=signal,
            signal_id=sid,
            trade_semaphore=acquired_sem,
            resume_mode=True,
        )
        if ok:
            started += 1
        else:
            if acquired_sem is not None:
                acquired_sem.release()
            log.error(f"[RESUME] Failed to launch monitor thread for signal_id={sid[:12]}")
    return started


def process_new_signals(
    csv_paths: Sequence[str],
    executed: Set[str],
    trade_semaphore: Optional[threading.Semaphore],
    dry_run: bool = False,
) -> Set[str]:
    """
    Read signals CSV, find unprocessed signals, launch trade threads.
    Returns updated executed signals set.
    """
    lockdown = _get_dispatch_lockdown()
    if lockdown:
        log.error(f"[SAFETY] New entries blocked: {lockdown}")
        return executed

    if not _is_entry_window_open_now():
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

        # Validate required fields
        required = ["ticker", "side", "entry_price", "stop_price", "target_price"]
        missing = [k for k in required if not signal.get(k)]
        if missing:
            log.warning(f"Signal {signal_id[:12]} missing fields: {missing}. Skipping.")
            continue

        ticker = str(signal.get("ticker", "")).strip().upper()
        side = str(signal.get("side", "")).strip().upper()
        entry_price = _safe_float(signal.get("entry_price", 0.0), 0.0)
        stop_price = _safe_float(signal.get("stop_price", 0.0), 0.0)
        target_price = _safe_float(signal.get("target_price", 0.0), 0.0)
        quantity = _safe_int(signal.get("quantity", 1), 1)
        if not ticker:
            log.warning(f"Signal {signal_id[:12]} has invalid ticker. Skipping.")
            continue
        if side not in {"LONG", "SHORT"}:
            log.warning(f"Signal {signal_id[:12]} has invalid side='{side}'. Skipping.")
            continue
        if entry_price <= 0 or stop_price <= 0 or target_price <= 0:
            log.warning(
                f"Signal {signal_id[:12]} has invalid prices "
                f"(entry={entry_price}, stop={stop_price}, target={target_price}). Skipping."
            )
            continue
        if quantity <= 0:
            log.warning(f"Signal {signal_id[:12]} has invalid quantity={quantity}. Skipping.")
            continue
        signal["ticker"] = ticker
        signal["side"] = side

        with closed_trades_lock:
            # strategy_v2 §J3 fix #10 — block on either the permanent
            # "filled today" set or the transient admission set so two
            # dispatchers can't race into the same ticker.
            ticker_seen_today = (
                ticker in closed_tickers_today
                or ticker in dispatched_tickers
            )
        if ticker_seen_today:
            outcome = "ENTRY_SKIPPED_TICKER_ALREADY_TRADED_TODAY"
            log.warning(
                f"[DISPATCH.SKIP] {ticker} signal_id={signal_id[:12]} "
                "skipped: ticker already traded earlier today."
            )
            _log_non_executed_signal_to_csv(signal, signal_id, outcome)
            with executed_lock:
                executed.add(signal_id)
                executed_changed = True
            with closed_trades_lock:
                closed_signal_ids_today.add(signal_id)
            continue

        if is_symbol_mis_rejected(ticker):
            outcome = "ENTRY_SKIPPED_MIS_BLOCKED_OR_MISSING"
            log.warning(
                f"[DISPATCH.SKIP] {ticker} signal_id={signal_id[:12]} "
                f"skipped: symbol already marked MIS blocked/missing for today."
            )
            _log_non_executed_signal_to_csv(signal, signal_id, outcome)
            with executed_lock:
                executed.add(signal_id)
                executed_changed = True
            with closed_trades_lock:
                closed_signal_ids_today.add(signal_id)
                closed_tickers_today.add(ticker)
            continue

        # strategy_v2 §J3 fix #9 — pre-acquire the concurrency permit BEFORE
        # reserving capital or marking executed. If no permit is available,
        # defer the signal without burning its admission state; the next
        # dispatcher pass will retry once an in-flight trade releases its
        # permit. Pre-fix, the permit was acquired inside the worker thread,
        # so N-over-cap signals would queue holding ticker/margin slots.
        acquired_sem: Optional[threading.Semaphore] = None
        if trade_semaphore is not None:
            if not trade_semaphore.acquire(blocking=False):
                log.info(
                    f"[DISPATCH.DEFER] {ticker} signal_id={signal_id[:12]} "
                    f"deferred: concurrency cap reached "
                    f"(max={MAX_CONCURRENT_TRADES}). Will retry on next poll."
                )
                continue
            acquired_sem = trade_semaphore

        # Risk + margin reservation (atomic)
        allowed, reason, reserved_margin = _reserve_capacity_for_signal(signal_id, signal)
        if not allowed:
            if acquired_sem is not None:
                acquired_sem.release()
            log.warning(
                f"[RISK] Rejecting {signal.get('side', '?')} "
                f"{signal.get('ticker', '?')}: {reason}"
            )
            outcome = "ENTRY_SKIPPED_RISK_LIMIT"
            _log_non_executed_signal_to_csv(signal, signal_id, outcome)
            with executed_lock:
                executed.add(signal_id)
                executed_changed = True
            with closed_trades_lock:
                closed_signal_ids_today.add(signal_id)
                closed_tickers_today.add(ticker)
            continue

        # Mark executed only after reservation succeeds.
        with executed_lock:
            executed.add(signal_id)
            executed_changed = True

        ticker = signal.get("ticker", "?")
        side = signal.get("side", "?")
        entry_p = signal.get("entry_price", "?")

        if dry_run:
            log.info(
                f"[DRY-RUN] Would execute: {side} {ticker} @ {entry_p} | "
                f"SL={signal.get('stop_price')} TGT={signal.get('target_price')}"
            )
            if acquired_sem is not None:
                acquired_sem.release()
            with closed_trades_lock:
                closed_signal_ids_today.add(signal_id)
                closed_tickers_today.add(ticker)
            _release_capacity(signal_id)
            continue

        sid = signal_id
        # Write PENDING row BEFORE spawning the thread to avoid a race where the thread
        # completes (e.g. immediate deadline-past skip) and writes the terminal outcome
        # before this dispatch code reaches _log_pending_signal_to_csv, which would then
        # silently overwrite the correct terminal outcome with ENTRY_PENDING.
        _log_pending_signal_to_csv(signal, signal_id)
        started = _launch_trade_thread(
            signal=signal,
            signal_id=sid,
            trade_semaphore=acquired_sem,
            resume_mode=False,
        )
        if not started:
            if acquired_sem is not None:
                acquired_sem.release()
            _release_capacity(sid)
            with executed_lock:
                executed.discard(sid)
                executed_changed = True
            log.error(f"[DISPATCH] Failed to launch trade thread for signal_id={sid[:12]}")
            continue
        with closed_trades_lock:
            closed_signal_ids_today.add(signal_id)
            # strategy_v2 §J3 fix #10 — transient admission only. The
            # worker thread promotes to closed_tickers_today once the fill
            # is confirmed, and clears dispatched_tickers in its finally.
            dispatched_tickers.add(ticker)

        log.info(
            f"[DISPATCH] Launched LIVE trade: {side} {ticker} @ {entry_p} "
            f"| reserved_margin=Rs.{reserved_margin:,.2f} | ID={signal_id[:12]}"
        )
        new_count += 1

    if executed_changed:
        with executed_lock:
            executed_snapshot = set(executed)
        save_executed_signals(executed_snapshot)

    if new_count > 0:
        with active_trades_lock:
            active_count = len(active_trades)
        log.info(
            f"Dispatched {new_count} new trade(s). "
            f"Active trades: {active_count}"
        )

    return executed


# ============================================================================
# WATCHDOG FILE MONITOR
# ============================================================================
class SignalCSVHandler(FileSystemEventHandler):
    """Watches multiple signal CSV files for modifications and triggers processing."""

    def __init__(self, csv_paths: Sequence[str], callback, debounce_sec: float = 2.0):
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
    parser = argparse.ArgumentParser(
        description="AVWAP Live Trade Executor V15 (Zerodha Real Orders)"
    )
    parser.add_argument(
        "--max-trades", type=int, default=MAX_CONCURRENT_TRADES,
        help=(
            "Max concurrent trade worker threads "
            f"(default: {MAX_CONCURRENT_TRADES}; 0 or less = unlimited)"
        ),
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Validate signals without placing real orders",
    )
    args = parser.parse_args()
    _set_dispatch_lockdown(None)

    # Audit #1 (2026-04-22) — startup config attestation. Compares the
    # bat-written claim file against effective os.environ for the whitelist
    # of EQIDV2_* vars that drive entry / risk / fill behaviour. Soft mode
    # by default (logs DRIFT and continues); set EQIDV2_CONFIG_CHECK_STRICT=1
    # to make any mismatch a boot exit. EQIDV2_CONFIG_CHECK_BYPASS=1 is the
    # emergency override.
    try:
        from eqidv2_config_attestation import verify_claim as _verify_config_claim
        _verify_config_claim(
            process_name="avwap_trade_execution_PAPER_TRADE_FALSE_id_5min_v7",
            whitelist=(
                "EQIDV2_LATE_DETECTION_MAX_LAG_SEC",
                "EQIDV2_MAX_CONCURRENT_TRADES",
                "EQIDV2_MAX_OPEN_POSITIONS",
                "EQIDV2_FILL_WAIT_TIMEOUT_SEC",
                "EQIDV2_ENTRY_RETRY_DELAY_SEC",
                "EQIDV2_VOLUME_GATE_ENABLE",
                "EQIDV2_VOLUME_GATE_MAX_PARTICIPATION_PCT",
                "EQIDV2_RUNTIME_ROOT",
            ),
            logger=log,
        )
    except SystemExit:
        raise
    except Exception as exc:
        log.warning(
            f"[STARTUP.CONFIG] attestation_skipped err={exc!r} "
            "(non-fatal in soft mode)"
        )

    log.info("=" * 65)
    log.info("AVWAP Live Trade Executor V7 ID 5min -- PAPER_TRADE = FALSE")
    log.info("  ***  REAL ORDERS WILL BE PLACED ON ZERODHA  ***")
    log.info(f"  Mode              : {'DRY-RUN' if args.dry_run else 'LIVE TRADING'}")
    log.info(f"  Max concurrent    : {args.max_trades}")
    log.info(f"  Signal dir        : {os.path.abspath(SIGNAL_DIR)}/")
    log.info(f"  Entry cutoff at   : {FORCED_CLOSE_TIME} IST")
    log.info(f"  Forced close at   : {FORCED_CLOSE_TIME} IST")
    log.info(
        "  Entry sizing      : "
        "signal quantity preferred | "
        f"fallback margin=Rs.{DEFAULT_POSITION_SIZE:,.0f}/signal | "
        f"MIS={INTRADAY_LEVERAGE:.1f}x | "
        f"notional≈Rs.{DEFAULT_POSITION_SIZE * INTRADAY_LEVERAGE:,.0f}"
    )
    if FORCE_ENTRY_QUANTITY is not None:
        log.info(f"  Quantity override : FORCE_ENTRY_QUANTITY={FORCE_ENTRY_QUANTITY} (all NEW entries)")
    log.info(
        "  SL/Target policy  : "
        "signal prices preferred | "
        f"SHORT=SL {SHORT_STOP_PCT*100:.2f}% / TGT {SHORT_TARGET_PCT*100:.2f}% | "
        f"LONG=SL {LONG_STOP_PCT*100:.2f}% / TGT {LONG_TARGET_PCT*100:.2f}% "
        "(fallback profile; rebased from actual fill)"
    )
    if RISK_LIMITS_ENABLED:
        log.info(
            f"  Risk limits       : ENABLED (max_open={MAX_OPEN_POSITIONS}, "
            f"max_margin=Rs.{MAX_CAPITAL_DEPLOYED_RS:,.0f})"
        )
    else:
        log.warning("  Risk limits       : DISABLED (no max-open / margin cap checks)")
    log.info("=" * 65)

    if not args.dry_run:
        log.info("Establishing Kite session...")
        setup_kite_session()
        preload_symbol_tick_sizes("NSE")

    # Load executed signals
    executed = load_executed_signals()
    log.info(f"Loaded {len(executed)} previously executed signals.")
    mis_rejected_loaded = hydrate_mis_rejected_symbols()
    if mis_rejected_loaded > 0:
        log.warning(
            f"[MIS] Loaded {mis_rejected_loaded} symbol(s) already marked MIS blocked/missing today."
        )

    trade_csv_path = _ensure_today_trade_csv_exists()
    log.info(f"[CSV] Live trade log ready: {trade_csv_path}")
    rows_before, rows_removed = _sanitize_today_trade_csv()
    if rows_removed > 0:
        log.warning(
            f"[CSV] Removed {rows_removed}/{rows_before} stale row(s) "
            "from today's live trade CSV."
        )

    # Resolve today's signal CSVs (short + long direction)
    today_str = datetime.now(IST).strftime("%Y-%m-%d")
    csv_paths = get_signal_csv_paths_for_today()
    log.info("Signal CSV sources: " + ", ".join(os.path.basename(p) for p in csv_paths))
    csv_hydrated = hydrate_mis_rejected_symbols_from_trade_csv(trade_csv_path)
    if csv_hydrated > 0:
        log.warning(
            f"[MIS] Hydrated {csv_hydrated} symbol(s) as MIS blocked/missing from today's trade CSV."
        )

    # Trade concurrency semaphore (optional unlimited mode).
    trade_semaphore: Optional[threading.Semaphore] = None
    if args.max_trades > 0:
        trade_semaphore = threading.Semaphore(args.max_trades)
    else:
        log.warning("  Trade worker cap   : DISABLED (unlimited concurrent workers)")

    # Restore runtime state for mid-session restarts.
    if not args.dry_run:
        restore_stats, resume_signals = _restore_intraday_runtime_state(
            signal_csv_paths=csv_paths,
            trade_csv_path=trade_csv_path,
            executed=executed,
        )
        save_executed_signals(executed)
        backfilled = _backfill_missing_trade_rows(
            signal_csv_paths=csv_paths,
            executed=executed,
        )
        if backfilled > 0:
            log.warning(
                f"[CSV] Backfilled {backfilled} missing trade row(s) as ENTRY_PENDING_RECOVERED."
            )
        log.info(
            "[RESTORE] "
            f"signals_today={int(restore_stats['signals_today'])} "
            f"executed={int(restore_stats['executed_loaded'])} "
            f"closed_today={int(restore_stats['closed_today'])} "
            f"open_restored={int(restore_stats['open_restored'])} "
            f"stale_dropped={int(restore_stats['stale_dropped'])} "
            f"requeued_backlog={int(restore_stats['requeued_backlog'])} "
            f"realized_trades={int(restore_stats['realized_trades'])} "
            f"realized_total=Rs.{restore_stats['realized_total']:+,.2f} "
            f"deployed_margin=Rs.{restore_stats['deployed_margin']:,.2f}"
        )

        safety_reasons: List[str] = []
        orphan_positions = _detect_orphan_broker_positions()
        if orphan_positions:
            orphan_summary = ", ".join(
                f"{x['ticker']}:{x['quantity']}" for x in orphan_positions
            )
            reason = (
                "orphan MIS broker positions detected; "
                f"manual reconciliation required ({orphan_summary})"
            )
            safety_reasons.append(reason)
            for orphan in orphan_positions:
                log.error(
                    "[SAFETY] ORPHAN "
                    f"{orphan.get('ticker', '?')} qty={orphan.get('quantity', 0)} "
                    f"product={orphan.get('product', '?')} "
                    f"avg={_safe_float(orphan.get('average_price', 0.0), 0.0):.2f}"
                )

        stale_tracked = _detect_stale_tracked_positions()
        if stale_tracked:
            stale_ids = {str(x.get("signal_id", "")).strip() for x in stale_tracked if str(x.get("signal_id", "")).strip()}
            stale_tickers = {
                str(x.get("ticker", "")).upper().strip()
                for x in stale_tracked
                if str(x.get("ticker", "")).strip()
            }
            with active_positions_lock:
                for sid in stale_ids:
                    active_positions.pop(sid, None)
            with capital_lock:
                for sid in stale_ids:
                    capital_deployed.pop(sid, None)
            with closed_trades_lock:
                closed_signal_ids_today.update(stale_ids)
                closed_tickers_today.update(stale_tickers)
            try:
                _persist_open_trades_state()
            except Exception as e:
                log.error(f"[SAFETY] Failed to persist stale-state cleanup: {e}")
            resume_signals = [
                sig for sig in resume_signals
                if str(sig.get("signal_id", "")).strip() not in stale_ids
            ]
            stale_summary = ", ".join(
                f"{x.get('ticker', '?')}:{x.get('expected_qty', 0)}->{x.get('broker_qty', 0)}"
                for x in stale_tracked
            )
            safety_reasons.append(
                "stale tracked runtime positions dropped; "
                f"manual reconciliation required ({stale_summary})"
            )
            for row in stale_tracked:
                log.error(
                    "[SAFETY] STALE "
                    f"{row.get('ticker', '?')} signal_id={str(row.get('signal_id', ''))[:12]} "
                    f"expected={row.get('expected_qty', 0)} broker={row.get('broker_qty', 0)}"
                )

        if safety_reasons:
            reason = " | ".join(safety_reasons)
            _set_dispatch_lockdown(reason)
            log.error(f"[SAFETY] {reason}")
        else:
            log.info("[SAFETY] Broker MIS positions match restored runtime state.")

        resumed_count = start_resumed_trade_monitors(
            resumed_signals=resume_signals,
            trade_semaphore=trade_semaphore,
            dry_run=args.dry_run,
        )
        if resumed_count > 0:
            log.info(f"[RESTORE] Resumed {resumed_count} open trade monitor(s).")

    # Callback for watchdog
    def on_csv_change():
        nonlocal executed
        log.info("Signal CSV changed - processing new signals from short + long direction CSVs...")
        executed = process_new_signals(
            csv_paths, executed, trade_semaphore, dry_run=args.dry_run,
        )

    # Set up watchdog
    os.makedirs(SIGNAL_DIR, exist_ok=True)
    handler = SignalCSVHandler(csv_paths, on_csv_change, debounce_sec=2.0)
    observer = Observer()
    observer.schedule(handler, path=SIGNAL_DIR, recursive=False)
    observer.start()
    log.info("Watchdog started - monitoring " + ", ".join(csv_paths))

    # Initial check for existing signals
    if any(os.path.exists(p) for p in csv_paths):
        log.info("Checking for existing unprocessed signals...")
        executed = process_new_signals(
            csv_paths, executed, trade_semaphore, dry_run=args.dry_run,
        )

    # Main loop
    try:
        while True:
            now = datetime.now(IST)

            if not args.dry_run and now.time() >= FORCED_CLOSE_TIME:
                _global_eod_force_close_sweep(now_ist=now)

            if now.time() > MARKET_CLOSE:
                with active_trades_lock:
                    remaining = len(active_trades)
                if remaining > 0:
                    log.info(
                        f"Market closed. Waiting for {remaining} active trade(s)..."
                    )
                    time.sleep(10)
                else:
                    log.info("Market closed. All trades complete.")
                    break

            time.sleep(1)

    except KeyboardInterrupt:
        log.info("Received interrupt. Shutting down...")
    finally:
        observer.stop()
        observer.join()
        save_executed_signals(executed)

        # Print daily summary
        with daily_pnl_lock:
            wr = (
                daily_pnl["wins"] / daily_pnl["trades"] * 100
                if daily_pnl["trades"] > 0
                else 0
            )
            log.info("=" * 55)
            log.info("DAILY LIVE TRADE SUMMARY")
            log.info(f"  Total trades : {daily_pnl['trades']}")
            log.info(f"  Wins         : {daily_pnl['wins']}")
            log.info(f"  Losses       : {daily_pnl['losses']}")
            log.info(f"  Win rate     : {wr:.1f}%")
            log.info(f"  Total P&L    : Rs.{daily_pnl['total']:+,.2f}")
            log.info("=" * 55)

        log.info("Live trade executor stopped.")


if __name__ == "__main__":
    main()




