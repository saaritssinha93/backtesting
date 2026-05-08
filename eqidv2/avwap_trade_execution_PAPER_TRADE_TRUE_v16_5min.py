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
  - live_signals/live_trades_YYYY-MM-DD_v16_5min_paper.csv  (detailed trade log)
  - live_signals/live_trade_summary_v16_5min_paper.json     (running P&L summary)

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

import pandas as pd
import pytz
import avwap_combined_runner_v16_5min as v16_runner
from eqidv2_runtime_paths import LIVE_SIGNALS_DIR as RUNTIME_LIVE_SIGNALS_DIR
from avwap_v11_refactored.avwap_common_v11_v15 import (
    default_short_config as v16_5min_default_short_config,
    default_long_config as v16_5min_default_long_config,
)

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
SIGNAL_CSV_PATTERNS = ("signals_{}_v16_5min_short.csv", "signals_{}_v16_5min_long.csv")
PAPER_TRADE_LOG_PATTERN = "paper_trades_{}_v16_5min.csv"
PAPER_TRADE_EXEC_LOG_PATTERN = "paper_trade_execution_{}_v16_5min.log"
EXECUTED_SIGNALS_FILE = os.path.join(SIGNAL_DIR, "executed_signals_paper_v16_5min.json")
SUMMARY_FILE = os.path.join(SIGNAL_DIR, "paper_trade_summary_v16_5min.json")
OPEN_TRADES_STATE_PATTERN = "open_trades_state_{}_v16_5min.json"
KILL_SWITCH_COMMAND_FILE = os.path.join(SIGNAL_DIR, "kill_switch_true_v16_5min.json")

# Trading hours
MARKET_OPEN = dt_time(9, 15)
MARKET_CLOSE = dt_time(15, 30)
FORCED_CLOSE_TIME = dt_time(15, 20)  # aligned closer to backtest EOD; safe before broker auto-square-off

# Simulation
POLL_INTERVAL_SEC = 5
LIVE_PNL_LOG_INTERVAL_SEC = int(os.getenv("LIVE_PNL_LOG_INTERVAL_SEC", "5"))
# 0 or negative means unlimited worker threads (no executor-side cap).
# M2 (2026-04-22): default aligned with live executor (10). Paper bat also sets
# EQIDV2_PAPER_V16_5MIN_MAX_CONCURRENT_TRADES=10 explicitly for uniformity.
MAX_CONCURRENT_TRADES = int(os.getenv("EQIDV2_PAPER_V16_5MIN_MAX_CONCURRENT_TRADES", "10"))

# ---------------------------------------------------------------------------
# CAND-E4 per-setup SL/TGT overrides (R:R-protected: SL>=0.75, TGT>=0.80, R:R>=1.0)
# Source: avwap_combined_runner_v17C_noNF_live_5min.py CANDIDATE_E3_SL_TGT
# ---------------------------------------------------------------------------
CANDIDATE_E4_SL_TGT = {
    ("LONG",  "A_MOD_BREAK_C1_HIGH"):           (0.75, 0.80),
    ("LONG",  "B_AVWAP_RECLAIM_REVERSAL"):      (0.75, 0.80),
    ("LONG",  "B_HUGE_C1_CLOSE_RECLAIM_BREAK"): (0.75, 0.90),
    ("LONG",  "D_EMA20_BOUNCE"):                (0.80, 0.80),
    ("SHORT", "A_MOD_BREAK_C1_LOW"):            (0.80, 0.85),
    ("SHORT", "C_OR_BREAKDOWN"):                (0.85, 0.85),
    ("SHORT", "D_EMA20_REJECTION"):             (0.75, 0.80),
    ("SHORT", "G_LOWER_LOW_BREAK"):             (0.80, 0.80),
}


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
CANDE4_LONG_TIME_WINDOW_LO  = float(os.getenv("EQIDV2_CANDE4_LONG_WINDOW_LO",  "9.25"))
CANDE4_LONG_TIME_WINDOW_HI  = float(os.getenv("EQIDV2_CANDE4_LONG_WINDOW_HI",  "13.00"))
CANDE4_SHORT_TIME_WINDOW_LO = float(os.getenv("EQIDV2_CANDE4_SHORT_WINDOW_LO", "9.25"))
CANDE4_SHORT_TIME_WINDOW_HI = float(os.getenv("EQIDV2_CANDE4_SHORT_WINDOW_HI", "14.00"))
CANDE4_MAX_LONG_PER_DAY     = int(os.getenv("EQIDV2_CANDE4_MAX_LONG_PER_DAY",  "15"))
CANDE4_MAX_SHORT_PER_DAY    = int(os.getenv("EQIDV2_CANDE4_MAX_SHORT_PER_DAY", "10"))
CANDE4_G2_COUNTERS_FILE     = os.path.join(SIGNAL_DIR, "candE4_g2_counters_v16_5min_paper.json")

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
LATE_DETECTION_MAX_LAG_SEC = int(os.getenv("EQIDV2_LATE_DETECTION_MAX_LAG_SEC", "300"))
# Tier-1 fix (2026-04-23): per-setup late-lag thresholds. See live executor for
# rationale (lag=1 480s, lag=2 780s, dynamic bounce 900s). Falls back to
# LATE_DETECTION_MAX_LAG_SEC for any setup not in the table.
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
    if not setup:
        return LATE_DETECTION_MAX_LAG_SEC
    return _LATE_LAG_THRESHOLDS_BY_SETUP.get(str(setup).upper().strip(), LATE_DETECTION_MAX_LAG_SEC)

_LATE_SKIPPED_LOCK = threading.Lock()
_late_skipped_count = 0


def _build_effective_v16_5min_executor_cfgs():
    short_builder = getattr(v16_runner, "default_short_config", None)
    long_builder = getattr(v16_runner, "default_long_config_v9", None)
    short_cfg = short_builder() if callable(short_builder) else v16_5min_default_short_config()
    long_cfg = long_builder() if callable(long_builder) else v16_5min_default_long_config()
    apply_profile = getattr(v16_runner, "apply_live_parity_profile", None)
    if callable(apply_profile):
        short_cfg, long_cfg = apply_profile(short_cfg, long_cfg)
    return short_cfg, long_cfg


_EFFECTIVE_V16_5MIN_SHORT_CFG, _EFFECTIVE_V16_5MIN_LONG_CFG = _build_effective_v16_5min_executor_cfgs()


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
    """Append one row to late_skipped_<date>_v16_5min_PAPER.csv and bump heartbeat counter."""
    global _late_skipped_count
    try:
        date_str = datetime.now(IST).strftime("%Y-%m-%d")
        path = Path(SIGNAL_DIR) / f"late_skipped_{date_str}_v16_5min_PAPER.csv"
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


SHORT_STOP_PCT = float(
    os.getenv(
        "EQIDV16_5MIN_SHORT_STOP_PCT",
        str(float(getattr(_EFFECTIVE_V16_5MIN_SHORT_CFG, "stop_pct", 0.0075))),
    )
)
LONG_STOP_PCT = float(
    os.getenv(
        "EQIDV16_5MIN_LONG_STOP_PCT",
        str(float(getattr(_EFFECTIVE_V16_5MIN_LONG_CFG, "stop_pct", 0.0075))),
    )
)
SHORT_TARGET_PCT = float(
    os.getenv(
        "EQIDV16_5MIN_SHORT_TARGET_PCT",
        str(float(getattr(_EFFECTIVE_V16_5MIN_SHORT_CFG, "target_pct", 0.0100))),
    )
)
LONG_TARGET_PCT = float(
    os.getenv(
        "EQIDV16_5MIN_LONG_TARGET_PCT",
        str(float(getattr(_EFFECTIVE_V16_5MIN_LONG_CFG, "target_pct", 0.0100))),
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
        "EQIDV16_5MIN_DEFAULT_POSITION_SIZE_RS",
        os.getenv("EQIDV16_5MIN_DEFAULT_POSITION_SIZE_RS", "10000"),
    )
)
INTRADAY_LEVERAGE = 5.0             # MIS leverage on Zerodha

# Exposure limits
# Keep paper defaults aligned with the real v15 executor unless explicitly overridden.
RISK_LIMITS_ENABLED = str(
    os.getenv(
        "EQIDV2_PAPER_V16_5MIN_ENABLE_RISK_LIMITS",
        os.getenv("EQIDV2_ENABLE_RISK_LIMITS", "1"),
    )
).strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
# M2 (2026-04-22): default aligned with live executor (10). See note in
# EQIDV2_PAPER_V16_5MIN_MAX_CONCURRENT_TRADES above.
MAX_OPEN_POSITIONS = int(
    os.getenv(
        "EQIDV2_PAPER_V16_5MIN_MAX_OPEN_POSITIONS",
        os.getenv("EQIDV2_MAX_OPEN_POSITIONS", "10"),
    )
)
MAX_CAPITAL_DEPLOYED_RS = float(
    os.getenv(
        "EQIDV2_PAPER_V16_5MIN_MAX_CAPITAL_DEPLOYED_RS",
        os.getenv("EQIDV2_MAX_CAPITAL_DEPLOYED_RS", "500000"),
    )
)

# Leave unset so quantity is taken from the signal row when present.
_FORCE_ENTRY_QUANTITY_RAW = str(os.getenv("EQIDV16_5MIN_FORCE_ENTRY_QUANTITY", "")).strip()
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
    "target_price",
    "outcome",
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
    logger = logging.getLogger("paper_trade_v16_5min")
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


def get_ltp(ticker: str) -> Optional[float]:
    """Get last traded price from Kite with NSE/BSE fallback."""
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
            return ltp
    except Exception as e:
        if _is_kite_auth_error(e) and _refresh_kite_session(f"ltp auth error ticker={ticker}", force=False) and kite is not None:
            try:
                data = kite.ltp(instruments if len(instruments) > 1 else instruments[0])
                ltp = _extract_ltp_from_payload(ticker, data, instruments)
                if ltp is not None:
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
            return ltp
        _set_ltp_error(ticker, f"no_valid_last_price candidates={','.join(instruments)}")
    except Exception as e:
        if _is_kite_auth_error(e) and _refresh_kite_session(f"quote auth error ticker={ticker}", force=False) and kite is not None:
            try:
                data_q = kite.quote(instruments)
                ltp = _extract_ltp_from_payload(ticker, data_q, instruments)
                if ltp is not None:
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
    target_price: float = 0.0
    outcome: str = ""
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
state_file_lock = threading.Lock()
kill_switch_cache_lock = threading.Lock()
kill_switch_cache_mtime: float = -1.0
kill_switch_cache_payload: Optional[dict] = None
daily_pnl: Dict[str, float] = {
    "total": 0.0,
    "wins": 0,
    "losses": 0,
    "trades": 0,
    "gross_profit": 0.0,
    "gross_loss": 0.0,
}
daily_pnl_lock = threading.Lock()

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
        pnl_rs, _ = _calc_pnl(side, entry_price, mark_price, qty)
        total += float(pnl_rs)
    return float(total)


def _capital_snapshot() -> Tuple[int, float]:
    with capital_lock:
        return len(capital_deployed), float(sum(capital_deployed.values()))


def _daily_snapshot() -> Dict[str, float]:
    with daily_pnl_lock:
        return dict(daily_pnl)


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

        pnl_rs, _ = _calc_pnl(side, entry_price, ltp, qty)
        ticker_unrealized[ticker] = ticker_unrealized.get(ticker, 0.0) + float(pnl_rs)

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

        # CAND-E4 G2: daily side cap.
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
                    (
                        f"margin limit exceeded (deployed Rs.{total_deployed:,.0f} + "
                        f"Rs.{margin:,.0f} > Rs.{MAX_CAPITAL_DEPLOYED_RS:,})"
                    ),
                    0.0,
                )

        capital_deployed[signal_id] = margin

    return True, "reserved", margin


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
    trade_start_ist = entry_time_ist if not resume_mode else datetime.now(IST)
    entry_retry_deadline = _entry_retry_deadline(signal, trade_start_ist, forced_close_dt)

    def _finalize_pre_entry_skip(outcome_name: str, warning_message: str) -> bool:
        exit_time_ist = datetime.now(IST)
        trade = PaperTrade(
            trade_id=trade_id,
            signal_id=signal_id,
            signal_datetime=str(signal.get("signal_datetime", "")),
            signal_entry_datetime_ist=str(signal.get("signal_entry_datetime_ist", "")),
            entry_time=trade_start_ist.strftime("%Y-%m-%d %H:%M:%S%z"),
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

        _release_capacity(signal_id)
        with active_trades_lock:
            active_trades.pop(signal_id, None)
        _log_live_pnl_snapshot(use_ltp, source=f"skip:{ticker}")
        return True

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

    # Fix #20 (post-2026-04-21): stale-detection guard.
    # Tier-1 (2026-04-23): per-setup threshold + late_skipped CSV trail.
    if (not resume_mode) and LATE_DETECTION_GUARD_ENABLE and LATE_DETECTION_MAX_LAG_SEC > 0:
        _lag_sec = _detection_lag_seconds(signal)
        _setup_name = str(signal.get("setup", "")).upper().strip()
        _threshold = _late_lag_threshold_for_setup(_setup_name)
        if _lag_sec is not None and _lag_sec > _threshold:
            _append_late_skipped_csv(signal, _lag_sec, _threshold)
            return _finalize_pre_entry_skip(
                "ENTRY_SKIPPED_STALE_DETECTION",
                (
                    f"[STALE.DETECT] Skipping {ticker} {side} {_setup_name}: detected "
                    f"{_lag_sec:.0f}s after entry slot "
                    f"(threshold {_threshold}s, setup-tier; env_max={LATE_DETECTION_MAX_LAG_SEC}s) | "
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

    # --- Near-entry retry window + max entry slip gate ---
    if (not resume_mode) and signal_entry_price > 0 and ENTRY_RETRY_NEAR_ENTRY_ENABLE and ENTRY_RETRY_WAIT_SEC > 0:
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
                log.warning(
                    f"[ENTRY.RETRY] Timed out for {ticker} {side} | signal_id={signal_id[:12]} "
                    f"| signal={signal_entry_price:.2f} | "
                    f"last_ltp={(float(waited_ltp) if waited_ltp else float(raw_entry)):.2f}"
                )
                return False

    # --- Max entry slip gate ---
    # Reject signals where the actual fill price has already deviated too far from
    # the model trigger.  For LONG: reject if raw_entry > signal_entry_price * (1 + cap).
    # For SHORT: reject if raw_entry < signal_entry_price * (1 - cap).
    # Only applied on fresh entries (not resumes) when a meaningful cap is set.
    if (not resume_mode) and signal_entry_price > 0:
        if side == "LONG" and LONG_MAX_ENTRY_SLIP_PCT > 0.0:
            slip = (raw_entry - signal_entry_price) / signal_entry_price
            if slip > LONG_MAX_ENTRY_SLIP_PCT:
                log.warning(
                    f"[SLIP.GATE] REJECTED ticker={ticker} | side=LONG | signal_id={signal_id[:12]} | "
                    f"model_trigger={signal_entry_price:.2f} | raw_entry={raw_entry:.2f} | "
                    f"slip={slip*100:.2f}% > cap={LONG_MAX_ENTRY_SLIP_PCT*100:.2f}%"
                )
                return False
        elif side == "SHORT" and SHORT_MAX_ENTRY_SLIP_PCT > 0.0:
            slip = (signal_entry_price - raw_entry) / signal_entry_price
            if slip > SHORT_MAX_ENTRY_SLIP_PCT:
                log.warning(
                    f"[SLIP.GATE] REJECTED ticker={ticker} | side=SHORT | signal_id={signal_id[:12]} | "
                    f"model_trigger={signal_entry_price:.2f} | raw_entry={raw_entry:.2f} | "
                    f"slip={slip*100:.2f}% > cap={SHORT_MAX_ENTRY_SLIP_PCT*100:.2f}%"
                )
                return False

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
    if (not resume_mode) and entry_source_used == "ltp_on_signal" and signal_entry_price > 0:
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
            "quantity": quantity,
            "entry_price": float(entry_price),
            "stop_price": float(stop_price),
            "target_price": float(target_price),
            "entry_time": entry_time_ist.strftime("%Y-%m-%d %H:%M:%S%z"),
            "last_ltp": _safe_float(signal.get("last_ltp", 0.0), 0.0),
            "restored": bool(resume_mode),
        }
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

    exit_price = entry_price
    outcome = "MONITORING"
    last_valid_ltp: Optional[float] = _safe_float(signal.get("last_ltp", 0.0), 0.0) or None
    ltp_miss_count = 0
    last_kill_switch_command_id = ""

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
            close_src = "last_ltp" if last_valid_ltp is not None else "entry_fallback"
            log.info(
                f"[SIM] FORCED CLOSE {side} {ticker} @ {exit_price} "
                f"(EOD forced close, src={close_src}) | ID={trade_id}"
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
            if ltp_miss_count == 1 or (ltp_miss_count % 12 == 0):
                last_err = get_last_ltp_error(ticker)
                log.warning(
                    f"[LTP.MISS] ticker={ticker} | side={side} | misses={ltp_miss_count} | "
                    f"action=retry_keep_open | ID={trade_id}"
                    + (f" | reason={last_err}" if last_err else "")
                )
            time.sleep(POLL_INTERVAL_SEC)
            continue

        if side == "SHORT":
            if ltp >= stop_price:
                exit_price = stop_price
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
                exit_price = stop_price
                outcome = "SL"
                log.info(f"[SIM] SL HIT {side} {ticker} @ {exit_price} (LTP={ltp}) | ID={trade_id}")
                break
            if ltp >= target_price:
                exit_price = target_price
                outcome = "TARGET"
                log.info(f"[SIM] TARGET HIT {side} {ticker} @ {exit_price} (LTP={ltp}) | ID={trade_id}")
                break

        time.sleep(POLL_INTERVAL_SEC)

    exit_time_ist = datetime.now(IST)
    pnl_rs, pnl_pct = _calc_pnl(side, entry_price, float(exit_price), quantity)

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
        target_price=round(target_price, 2),
        outcome=outcome,
        pnl_rs=round(pnl_rs, 2),
        pnl_pct=round(pnl_pct, 4),
        quality_score=_safe_float(signal.get("quality_score", 0), 0.0),
        p_win=_safe_float(signal.get("p_win", 0.0), 0.0),
        confidence_multiplier=_safe_float(signal.get("confidence_multiplier", 1.0), 1.0),
    )

    _log_trade(trade)

    with daily_pnl_lock:
        daily_pnl["total"] += pnl_rs
        daily_pnl["trades"] += 1
        if pnl_rs > 0:
            daily_pnl["wins"] += 1
            daily_pnl["gross_profit"] += pnl_rs
        elif pnl_rs < 0:
            daily_pnl["losses"] += 1
            daily_pnl["gross_loss"] += pnl_rs
        day_total = float(daily_pnl["total"])
        day_wins = int(daily_pnl["wins"])
        day_losses = int(daily_pnl["losses"])
        _save_summary()

    log.info(
        f"[SIM] RESULT {side} {ticker} | {outcome} | "
        f"P&L: Rs.{pnl_rs:+,.2f} ({pnl_pct:+.2f}%) | "
        f"Day total: Rs.{day_total:+,.2f} ({day_wins}W/{day_losses}L)"
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
            "target_price": trade.target_price,
            "outcome": trade.outcome,
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
            "total_trades": daily_pnl["trades"],
            "wins": daily_pnl["wins"],
            "losses": daily_pnl["losses"],
            "gross_profit_rs": round(daily_pnl.get("gross_profit", 0.0), 2),
            "gross_loss_rs": round(daily_pnl.get("gross_loss", 0.0), 2),
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
) -> Tuple[Set[str], Dict[str, float]]:
    closed_ids: Set[str] = set()
    realized_total = 0.0
    realized_trades = 0
    realized_wins = 0
    realized_losses = 0
    gross_profit = 0.0
    gross_loss = 0.0

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

                    pnl_rs = _safe_float(row.get("pnl_rs", 0.0), 0.0)
                    realized_total += pnl_rs
                    realized_trades += 1
                    if pnl_rs > 0:
                        realized_wins += 1
                        gross_profit += pnl_rs
                    elif pnl_rs < 0:
                        realized_losses += 1
                        gross_loss += pnl_rs
        except Exception as e:
            log.warning(f"[RESTORE] Could not parse paper trade CSV: {e}")

    return closed_ids, {
        "realized_total": float(realized_total),
        "realized_trades": float(realized_trades),
        "realized_wins": float(realized_wins),
        "realized_losses": float(realized_losses),
        "gross_profit": float(gross_profit),
        "gross_loss": float(gross_loss),
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

    closed_ids, realized = _load_closed_ids_and_realized_summary(
        paper_csv_path=paper_csv_path,
        today_date=today_date,
    )

    with daily_pnl_lock:
        daily_pnl["total"] = float(realized["realized_total"])
        daily_pnl["trades"] = int(realized["realized_trades"])
        daily_pnl["wins"] = int(realized["realized_wins"])
        daily_pnl["losses"] = int(realized["realized_losses"])
        daily_pnl["gross_profit"] = float(realized["gross_profit"])
        daily_pnl["gross_loss"] = float(realized["gross_loss"])
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

        allowed, reason, reserved_margin = _reserve_capacity_for_signal(signal_id, signal)
        if not allowed:
            log.warning(
                f"[RISK] Rejecting {signal.get('side', '?')} "
                f"{signal.get('ticker', '?')}: {reason}"
            )
            continue

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
            with executed_lock:
                executed.discard(signal_id)
                executed_changed = True
            log.error(f"[DISPATCH] Failed to launch simulation thread for signal_id={signal_id[:12]}")
            continue

        new_count += 1

        log.info(
            f"[DISPATCH] Launched simulation for "
            f"{signal.get('side', '?')} {signal.get('ticker', '?')} "
            f"@ {signal.get('entry_price', '?')} | p_win={signal.get('p_win', '?')} | "
            f"reserved_margin={_fmt_rs(reserved_margin)} | ID={signal_id[:12]}"
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
    parser = argparse.ArgumentParser(description="AVWAP Paper Trade Executor V16 5min")
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

    use_ltp = not args.no_ltp

    log.info("=" * 65)
    log.info("AVWAP Paper Trade Executor V16 5min -- PAPER_TRADE = TRUE")
    log.info(f"  Mode            : SIMULATION (no real orders)")
    log.info(f"  LTP polling     : {'Enabled' if use_ltp else 'Disabled'}")
    log.info(f"  Entry source    : {args.entry_price_source}")
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
        "V16_5MIN signal CSV sources: " + ", ".join(os.path.basename(p) for p in csv_paths)
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
        log.info("Signal CSV changed - processing new signals from v16_5min_short + v16_5min_long...")
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
    log.info("Watchdog V16_5MIN started - monitoring " + ", ".join(csv_paths))

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
            log.info(f"  Gross profit : Rs.{daily_pnl.get('gross_profit', 0.0):+,.2f}")
            log.info(f"  Gross loss   : Rs.{daily_pnl.get('gross_loss', 0.0):+,.2f}")
            log.info(f"  Total P&L    : Rs.{daily_pnl['total']:+,.2f}")
            log.info("=" * 55)

        log.info("Paper trade executor stopped.")


if __name__ == "__main__":
    main()


