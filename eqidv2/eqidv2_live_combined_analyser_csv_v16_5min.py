# -*- coding: utf-8 -*-
"""
EQIDV2 — V16 5min Live Signal Scanner
======================================
Persistent 5-min slot scanner for V16 Run 7 canonical parameters.

Scans all 1041 NSE tickers every 5 minutes using:
  - V16 Run 7 scanner config (avwap_min_consec_closes=1, mod_impulse_min_atr=0.30, volume=0.80)
  - Nifty RS filter: LONG RS>=1.0%, SHORT RS<=-0.75% (BOTH mode)
  - V16 anti-exhaustion post-scan filters (RSI dead zone, QS two-band, AVWAP dist cap)
  - Target: SHORT=0.75%, LONG=0.75% (Run 7 canonical)

Outputs (to LIVE_SIGNALS_DIR):
  signals_YYYY-MM-DD_v16_5min_short.csv
  signals_YYYY-MM-DD_v16_5min_long.csv

Status files (to logs/):
  eqidv2_live_combined_analyser_csv_v16_5min.status
  eqidv2_live_combined_analyser_csv_v16_5min.heartbeat
  eqidv2_live_combined_analyser_csv_v16_5min.log  (stdout tee)

Usage:
    python eqidv2_live_combined_analyser_csv_v16_5min.py
"""

from __future__ import annotations

import csv
import json
import multiprocessing as mp
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict
from datetime import datetime, timedelta, time as dtime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# V16 runner — params, filters, scan helpers
# ---------------------------------------------------------------------------
import avwap_combined_runner_v16_5min as v16_runner
from avwap_combined_runner_v16_5min import (
    apply_live_parity_profile,
    _apply_v16_post_scan_filters,
    NIFTY_RS_BOTH_MODE_THRESHOLD_LONG_PCT,
    NIFTY_RS_BOTH_MODE_THRESHOLD_SHORT_PCT,
    NIFTY_RS_LOOKBACK_BARS,
    NIFTY_CONTEXT_MIN_DAYMOVE_PCT,
    TEST_SHORT_TARGET_PCT,
    TEST_LONG_TARGET_PCT,
)
from avwap_v11_refactored.avwap_common_v11_v15 import (
    StrategyConfig,
    default_short_config,
    default_long_config,
    prepare_session_bars_for_scan,
    read_15m_parquet,
    list_tickers_15m,
)
from avwap_v11_refactored.avwap_short_strategy_v11 import (
    scan_all_days_for_ticker as scan_short,
    scan_all_days_for_ticker_prepared as scan_short_prepared,
)
from avwap_v11_refactored.avwap_long_strategy_v9_sweep import (
    scan_all_days_for_ticker as scan_long,
    scan_all_days_for_ticker_prepared as scan_long_prepared,
)

# ---------------------------------------------------------------------------
# V15 base — utilities (IST helpers, file locking, signal CSV writing)
# ---------------------------------------------------------------------------
import eqidv2_live_combined_analyser_csv_v15 as base_v15
from eqidv2_runtime_paths import (
    DATA_5M_DIR as RUNTIME_DATA_5M_DIR,
    LIVE_SIGNALS_DIR as RUNTIME_LIVE_SIGNALS_DIR,
    runtime_dir,
)
from live_v16_5min_slot_snapshot import (
    build_slot_snapshots,
    clear_rolling_cache as clear_slot_snapshot_cache,
    read_shard_snapshot,
    slot_context_path as snapshot_slot_context_path,
)

# ===========================================================================
# CONSTANTS
# ===========================================================================
IST = base_v15.IST
DIR_5M = str(RUNTIME_DATA_5M_DIR)
END_5M = "_stocks_indicators_5min.parquet"
NIFTYBEES_TICKER = "NIFTYBEES"

SHORT_SIGNAL_CSV_PATTERN = "signals_{}_v16_5min_short.csv"
LONG_SIGNAL_CSV_PATTERN  = "signals_{}_v16_5min_long.csv"

SLOT_MINUTES        = 5
START_TIME          = dtime(9, 15)
END_TIME            = dtime(15, 0)
HARD_STOP_TIME      = dtime(15, 30)
TAIL_ROWS           = int(os.getenv("EQIDV16_5MIN_TAIL_ROWS", "260"))
SLOT_START_OFFSET_SECONDS = int(os.getenv("EQIDV16_5MIN_SLOT_START_OFFSET_SECONDS", "5"))
SLOT_READY_MAX_WAIT_SECONDS = int(os.getenv("EQIDV16_5MIN_SLOT_READY_MAX_WAIT_SECONDS", "90"))
SLOT_READY_POLL_SECONDS = max(1, int(os.getenv("EQIDV16_5MIN_SLOT_READY_POLL_SECONDS", "2")))
SLOT_READY_SAMPLE_SIZE = max(1, int(os.getenv("EQIDV16_5MIN_SLOT_READY_SAMPLE_SIZE", "24")))
SLOT_READY_MIN_FRESH_RATIO = float(os.getenv("EQIDV16_5MIN_SLOT_READY_MIN_FRESH_RATIO", "0.60"))
USE_SCHEDULER_READY_MARKER = str(
    os.getenv("EQIDV16_5MIN_USE_SCHEDULER_READY_MARKER", "1")
).strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
SLOT_READY_MARKER_MIN_FRESH_RATIO = float(
    os.getenv("EQIDV16_5MIN_SLOT_READY_MARKER_MIN_FRESH_RATIO", "0.70")
)
SKIP_STALE_SLOT_ON_TIMEOUT = str(os.getenv("EQIDV16_5MIN_SKIP_STALE_SLOT_ON_TIMEOUT", "1")).strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
NEUTRALIZE_WEAK_NIFTY_CONTEXT = str(
    os.getenv("EQIDV16_5MIN_NEUTRALIZE_WEAK_NIFTY_CONTEXT", "1")
).strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
DIRECTIONAL_NIFTY_CONTEXT_FALLBACK = str(
    os.getenv("EQIDV16_5MIN_DIRECTIONAL_NIFTY_CONTEXT_FALLBACK", "1")
).strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
NEUTRALIZE_PARTIAL_NIFTY_SESSION = str(
    os.getenv("EQIDV16_5MIN_NEUTRALIZE_PARTIAL_NIFTY_SESSION", "1")
).strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
SCAN_SHARDS         = max(1, int(os.getenv("EQIDV16_5MIN_SCAN_SHARDS", "10")))
SCAN_MAX_WORKERS    = max(1, int(os.getenv("EQIDV16_5MIN_SCAN_MAX_WORKERS", str(SCAN_SHARDS))))
SNAPSHOT_MAX_WORKERS = max(1, int(os.getenv("EQIDV16_5MIN_SNAPSHOT_MAX_WORKERS", str(SCAN_MAX_WORKERS))))
USE_SLOT_SNAPSHOTS = str(os.getenv("EQIDV16_5MIN_USE_SLOT_SNAPSHOTS", "1")).strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
USE_SNAPSHOT_ROLLING_CACHE = str(os.getenv("EQIDV16_5MIN_USE_ROLLING_CACHE", "1")).strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
BENCHMARK_SLOT_RAW  = str(os.getenv("EQIDV16_5MIN_BENCHMARK_SLOT", "")).strip()

INTRADAY_LEVERAGE   = 5.0
DEFAULT_POSITION_SIZE_RS = float(
    os.getenv("EQIDV16_5MIN_DEFAULT_POSITION_SIZE_RS", "10000")
)

_BASE_DIR  = Path(__file__).resolve().parent
_LOG_DIR   = _BASE_DIR / "logs"
_LOG_DIR.mkdir(parents=True, exist_ok=True)
SLOT_READY_MARKER_DIR = runtime_dir("slot_ready_5m")

_SCRIPT_NAME = "eqidv2_live_combined_analyser_csv_v16_5min.py"
_LOG_FILE    = _LOG_DIR / "eqidv2_live_combined_analyser_csv_v16_5min.log"

# Signal CSV schema (shared with V15 executors)
SIGNAL_CSV_COLUMNS = base_v15.SIGNAL_CSV_COLUMNS


# ===========================================================================
# STDOUT TEE — mirrors output to log file
# ===========================================================================
class _Tee:
    def __init__(self, *streams):
        self._streams = streams

    def write(self, data):
        for s in self._streams:
            try:
                s.write(data)
                s.flush()
            except Exception:
                pass

    def flush(self):
        for s in self._streams:
            try:
                s.flush()
            except Exception:
                pass


def _start_tee() -> None:
    try:
        fh = open(_LOG_FILE, "a", encoding="utf-8", buffering=1)
        tee = _Tee(sys.__stdout__, fh)
        sys.stdout = tee  # type: ignore[assignment]
        sys.stderr = tee  # type: ignore[assignment]
    except Exception:
        pass


# ===========================================================================
# HELPERS — 5-min slot management
# ===========================================================================
def _now_ist() -> datetime:
    return base_v15.now_ist()


def _next_5min_slot(now: datetime) -> datetime:
    now = now.astimezone(IST)
    today = now.date()
    start_dt = IST.localize(datetime.combine(today, START_TIME))
    end_dt   = IST.localize(datetime.combine(today, END_TIME))

    if now <= start_dt:
        return start_dt
    if now > end_dt:
        tomorrow = today + timedelta(days=1)
        return IST.localize(datetime.combine(tomorrow, START_TIME))

    minute = (now.minute // SLOT_MINUTES) * SLOT_MINUTES
    slot = now.replace(minute=minute, second=0, microsecond=0)
    if slot <= now:
        slot += timedelta(minutes=SLOT_MINUTES)
    if slot < start_dt:
        slot = start_dt
    if slot > end_dt:
        tomorrow = today + timedelta(days=1)
        slot = IST.localize(datetime.combine(tomorrow, START_TIME))
    return slot


def _list_tickers_5m() -> List[str]:
    """Return sorted ticker list from 5-min parquet directory."""
    return list_tickers_15m(DIR_5M, END_5M)


def _load_5m_parquet(ticker: str, n: int = TAIL_ROWS) -> pd.DataFrame:
    path = os.path.join(DIR_5M, f"{ticker}{END_5M}")
    df = base_v15.read_parquet_tail(path, n=n)
    if df is None or df.empty or "date" not in df.columns:
        return pd.DataFrame()

    dt = pd.to_datetime(df["date"], errors="coerce")
    if getattr(dt.dt, "tz", None) is None:
        dt = dt.dt.tz_localize("UTC")
    dt = dt.dt.tz_convert(IST)

    df = df.copy()
    df["date"] = dt
    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    return df


def _sample_tickers_for_slot_ready(tickers: List[str], sample_size: int) -> List[str]:
    if not tickers:
        return []
    uniq = sorted({str(t).upper().strip() for t in tickers if str(t).strip()})
    if not uniq:
        return []
    size = min(max(1, int(sample_size)), len(uniq))
    if size >= len(uniq):
        return uniq
    if size == 1:
        return [uniq[len(uniq) // 2]]
    step = max(1.0, (len(uniq) - 1) / float(size - 1))
    picks: List[str] = []
    for idx in range(size):
        pos = int(round(idx * step))
        pos = max(0, min(len(uniq) - 1, pos))
        picks.append(uniq[pos])
    return sorted(set(picks))


def _last_bar_for_ticker_ist_5m(ticker: str) -> pd.Timestamp:
    df_tail = _load_5m_parquet(ticker, n=3)
    if df_tail.empty or "date" not in df_tail.columns:
        return pd.NaT
    ts = pd.Timestamp(df_tail.iloc[-1]["date"])
    if ts.tzinfo is None:
        ts = ts.tz_localize(IST)
    else:
        ts = ts.tz_convert(IST)
    return ts.floor("min")


def _slot_ready_marker_path(slot: datetime) -> Path:
    slot_ts = pd.Timestamp(slot)
    if slot_ts.tzinfo is None:
        slot_ts = slot_ts.tz_localize(IST)
    else:
        slot_ts = slot_ts.tz_convert(IST)
    return SLOT_READY_MARKER_DIR / f"slot_{slot_ts.strftime('%Y%m%d_%H%M')}.json"


def _load_slot_ready_marker(slot: datetime) -> Optional[Dict[str, Any]]:
    if not USE_SCHEDULER_READY_MARKER:
        return None
    path = _slot_ready_marker_path(slot)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        ratio = float(payload.get("fresh_ratio", 0.0) or 0.0)
        if ratio < float(SLOT_READY_MARKER_MIN_FRESH_RATIO):
            return None
        return payload
    except Exception:
        return None


def _wait_ready_via_marker(slot: datetime, started: datetime) -> Optional[Tuple[bool, float, float, int]]:
    payload = _load_slot_ready_marker(slot)
    if not payload:
        return None
    waited = max(0.0, (_now_ist() - started).total_seconds())
    ratio = float(payload.get("fresh_ratio", 0.0) or 0.0)
    checked = int(payload.get("checked_count", 0) or 0)
    print(
        f"[WAIT] 5min slot readiness accepted via scheduler marker "
        f"(ratio={ratio:.2f}, checked={checked}, source={payload.get('source', 'marker')})",
        flush=True,
    )
    return True, ratio, waited, checked


def _wait_for_slot_data_ready(slot: datetime, tickers: List[str]) -> Tuple[bool, float, float, int]:
    delay_target = slot + timedelta(seconds=int(SLOT_START_OFFSET_SECONDS))
    now = _now_ist()
    if now < delay_target:
        wait_secs = (delay_target - now).total_seconds()
        print(
            f"[WAIT] Delaying {wait_secs:.0f}s for 5min slot offset "
            f"(until {delay_target.strftime('%H:%M:%S')})",
            flush=True,
        )
        time.sleep(max(0.0, wait_secs))

    sample = _sample_tickers_for_slot_ready(tickers, SLOT_READY_SAMPLE_SIZE)
    max_wait = max(0, int(SLOT_READY_MAX_WAIT_SECONDS))
    marker_ready = _wait_ready_via_marker(slot, now)
    if marker_ready is not None:
        return marker_ready
    if max_wait <= 0 or not sample:
        waited = max(0.0, (_now_ist() - now).total_seconds())
        return False, 0.0, waited, len(sample)

    target_slot = pd.Timestamp(slot)
    if target_slot.tzinfo is None:
        target_slot = target_slot.tz_localize(IST)
    else:
        target_slot = target_slot.tz_convert(IST)

    started = _now_ist()
    deadline = started + timedelta(seconds=max_wait)
    last_ratio = 0.0
    last_checked = 0

    while True:
        marker_ready = _wait_ready_via_marker(slot, started)
        if marker_ready is not None:
            return marker_ready
        fresh = 0
        checked = 0
        for ticker in sample:
            last_bar = _last_bar_for_ticker_ist_5m(ticker)
            if pd.isna(last_bar):
                continue
            checked += 1
            if last_bar >= target_slot:
                fresh += 1

        ratio = (fresh / checked) if checked > 0 else 0.0
        last_ratio = ratio
        last_checked = checked
        if checked > 0 and ratio >= float(SLOT_READY_MIN_FRESH_RATIO):
            waited = (_now_ist() - started).total_seconds()
            print(
                f"[WAIT] 5min slot freshness ready after {waited:.1f}s "
                f"(fresh={fresh}/{checked}, ratio={ratio:.2f}, target>={target_slot.strftime('%H:%M')})",
                flush=True,
            )
            return True, ratio, waited, checked

        now = _now_ist()
        if now >= deadline:
            waited = (now - started).total_seconds()
            print(
                f"[WAIT] 5min slot freshness timeout after {waited:.1f}s "
                f"(ratio={ratio:.2f}, checked={checked}, target>={target_slot.strftime('%H:%M')})",
                flush=True,
            )
            return False, last_ratio, waited, last_checked

        time.sleep(min(float(SLOT_READY_POLL_SECONDS), max(0.0, (deadline - now).total_seconds())))


def _split_tickers_for_scan_shards(tickers: List[str], shard_count: int) -> List[List[str]]:
    ordered = sorted({str(t).strip().upper() for t in tickers if str(t).strip()})
    shard_count = max(1, int(shard_count))
    base_size, remainder = divmod(len(ordered), shard_count)
    out: List[List[str]] = []
    start = 0
    for idx in range(shard_count):
        size = base_size + (1 if idx < remainder else 0)
        end = start + size
        out.append(ordered[start:end])
        start = end
    return out


def _session_open_timestamp(session_date: Any) -> pd.Timestamp:
    if isinstance(session_date, pd.Timestamp):
        date_value = session_date.date()
    elif isinstance(session_date, datetime):
        date_value = session_date.date()
    else:
        date_value = session_date
    return pd.Timestamp(
        IST.localize(datetime.combine(date_value, START_TIME))
    ).floor("min")


def _resolve_nifty_context_flags(
    *,
    day_move_pct: float,
    rs_pct: float,
    session_complete: bool,
) -> Tuple[bool, bool, str]:
    if not session_complete and NEUTRALIZE_PARTIAL_NIFTY_SESSION:
        return True, True, "partial_session_neutral"

    if abs(day_move_pct) < float(NIFTY_CONTEXT_MIN_DAYMOVE_PCT):
        if NEUTRALIZE_WEAK_NIFTY_CONTEXT:
            return True, True, "weak_daymove_neutral"
        return False, False, "weak_daymove_blocked"

    allow_long = rs_pct >= float(NIFTY_RS_BOTH_MODE_THRESHOLD_LONG_PCT)
    allow_short = rs_pct <= -float(NIFTY_RS_BOTH_MODE_THRESHOLD_SHORT_PCT)
    if allow_long or allow_short:
        return allow_long, allow_short, "threshold"

    if DIRECTIONAL_NIFTY_CONTEXT_FALLBACK:
        if day_move_pct > 0:
            return True, False, "directional_daymove_long"
        if day_move_pct < 0:
            return False, True, "directional_daymove_short"

    return False, False, "threshold_blocked"


# ===========================================================================
# NIFTY RS CONTEXT — compute live BOTH-mode RS for current slot
# ===========================================================================
def _compute_nifty_rs_at_slot(slot_ist: datetime) -> Tuple[bool, bool, float]:
    """
    Load NIFTYBEES 5-min data and compute RS at the given slot.

    Returns:
        (allow_long, allow_short, rs_pct)
    """
    try:
        df_nifty = _load_5m_parquet(NIFTYBEES_TICKER)
        if df_nifty is None or df_nifty.empty:
            return True, True, 0.0  # no data — allow both (permissive fallback)

        # Filter to today up to slot.
        today = slot_ist.date()
        if "date" in df_nifty.columns:
            dt = pd.to_datetime(df_nifty["date"], errors="coerce")
            if getattr(dt.dt, "tz", None) is None:
                dt = dt.dt.tz_localize(IST)
            else:
                dt = dt.dt.tz_convert(IST)
            df_nifty["_dt"] = dt
        elif "datetime" in df_nifty.columns:
            df_nifty["_dt"] = pd.to_datetime(df_nifty["datetime"], utc=True).dt.tz_convert(IST)
        elif df_nifty.index.name in ("datetime", "timestamp") or isinstance(df_nifty.index, pd.DatetimeIndex):
            df_nifty["_dt"] = pd.to_datetime(df_nifty.index, utc=True).dt.tz_convert(IST)
        else:
            return True, True, 0.0

        df_today = df_nifty[df_nifty["_dt"].dt.date == today].copy()
        df_today = df_today[df_today["_dt"] <= slot_ist].copy()
        if len(df_today) < 2:
            return True, True, 0.0

        df_today = df_today.sort_values("_dt")
        first_session_slot = pd.Timestamp(df_today["_dt"].iloc[0]).floor("min")
        session_open = _session_open_timestamp(today)
        session_complete = bool(first_session_slot <= session_open)

        # Day move: current close vs first open
        day_open  = float(df_today["open"].iloc[0])
        day_close = float(df_today["close"].iloc[-1])
        if day_open <= 0:
            return True, True, 0.0
        day_move_pct = (day_close - day_open) / day_open * 100.0

        # RS = 4-bar pct change of NIFTYBEES close
        lookback = int(NIFTY_RS_LOOKBACK_BARS)
        if len(df_today) <= lookback:
            rs_pct = day_move_pct  # fallback: use day move
        else:
            close_now  = float(df_today["close"].iloc[-1])
            close_past = float(df_today["close"].iloc[-(lookback + 1)])
            rs_pct = (close_now - close_past) / close_past * 100.0 if close_past > 0 else 0.0

        allow_long, allow_short, decision_reason = _resolve_nifty_context_flags(
            day_move_pct=day_move_pct,
            rs_pct=rs_pct,
            session_complete=session_complete,
        )

        print(
            f"[NIFTY_RS] slot={slot_ist.strftime('%H:%M')} "
            f"day_move={day_move_pct:+.2f}% rs={rs_pct:+.2f}% "
            f"allow_long={allow_long} allow_short={allow_short} "
            f"session_complete={session_complete} reason={decision_reason}",
            flush=True,
        )
        return allow_long, allow_short, rs_pct

    except Exception as exc:
        print(f"[NIFTY_RS] ERROR: {exc} — defaulting to allow both", flush=True)
        return True, True, 0.0


# ===========================================================================
# SNAPSHOT SLOT CONTEXT
# ===========================================================================
def _load_snapshot_slot_context(slot_ist: datetime) -> Tuple[bool, bool, float, Dict[str, Any]]:
    path = snapshot_slot_context_path(slot_ist)
    try:
        if not path.exists():
            raise FileNotFoundError(str(path))
        payload = json.loads(path.read_text(encoding="utf-8"))
        nifty_context = payload.get("nifty_context") or {}
        allow_long = bool(nifty_context.get("allow_long", True))
        allow_short = bool(nifty_context.get("allow_short", True))
        rs_pct = float(nifty_context.get("rs_pct", 0.0) or 0.0)
        reason = str(nifty_context.get("decision_reason", "") or "")
        print(
            f"[NIFTY_RS] slot={pd.Timestamp(slot_ist).strftime('%H:%M')} "
            f"source=snapshot allow_long={allow_long} allow_short={allow_short} "
            f"rs={rs_pct:+.2f}% reason={reason}",
            flush=True,
        )
        return allow_long, allow_short, rs_pct, payload
    except Exception as exc:
        print(f"[NIFTY_RS] snapshot context unavailable: {exc}", flush=True)
        allow_long, allow_short, rs_pct = _compute_nifty_rs_at_slot(slot_ist)
        return allow_long, allow_short, rs_pct, {}


# ===========================================================================
# BUILD EFFECTIVE V16 CONFIGS
# ===========================================================================
def _build_v16_cfgs() -> Tuple[StrategyConfig, StrategyConfig]:
    short_cfg = default_short_config()
    long_cfg  = default_long_config()
    short_cfg, long_cfg = apply_live_parity_profile(short_cfg, long_cfg)
    # Apply the tuned V16_5min target overrides from the combined runner.
    short_cfg.target_pct = float(TEST_SHORT_TARGET_PCT)
    long_cfg.target_pct  = float(TEST_LONG_TARGET_PCT)
    # Point configs at 5-min data dir
    short_cfg.dir_15m = DIR_5M
    short_cfg.end_15m = END_5M
    long_cfg.dir_15m  = DIR_5M
    long_cfg.end_15m  = END_5M
    return short_cfg, long_cfg


# ===========================================================================
# SIGNAL CSV WRITING
# ===========================================================================
def _signal_csv_path(signal_day_str: str, side: str) -> Path:
    pattern = SHORT_SIGNAL_CSV_PATTERN if side.upper() == "SHORT" else LONG_SIGNAL_CSV_PATTERN
    return Path(RUNTIME_LIVE_SIGNALS_DIR) / pattern.format(signal_day_str)


def _safe_float(val: Any, default: float = float("nan")) -> float:
    try:
        v = float(val)
        return v if np.isfinite(v) else default
    except Exception:
        return default


def _write_side_signals_csv(signals: List[dict], side: str, signal_day_str: str) -> int:
    """Write new (deduplicated) signals to the daily side CSV. Returns written count."""
    side_upper = side.upper()
    csv_path = _signal_csv_path(signal_day_str, side_upper)

    signals_side = [s for s in signals if str(s.get("side", "")).upper() == side_upper]
    if not signals_side:
        print(f"[V16_5MIN {side_upper} CSV] written=0 (no signals)", flush=True)
        return 0

    written = 0
    skipped = 0
    received_time = _now_ist().strftime("%Y-%m-%d %H:%M:%S%z")

    with base_v15._locked_signal_csv(str(csv_path)):
        base_v15._ensure_signal_csv_schema(str(csv_path))
        existing_ids  = base_v15._load_existing_ids(str(csv_path))
        existing_keys = base_v15._load_existing_signal_keys(str(csv_path))
        file_exists   = csv_path.exists() and csv_path.stat().st_size > 0
        run_keys: set = set()

        with open(csv_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=SIGNAL_CSV_COLUMNS, quoting=csv.QUOTE_ALL)
            if not file_exists:
                writer.writeheader()

            for sig in signals_side:
                ticker    = str(sig.get("ticker", "")).upper().strip()
                setup     = str(sig.get("setup", ""))
                bar_time_raw = sig.get("bar_time_ist", sig.get("signal_datetime", ""))
                bar_time_ts  = base_v15._parse_ist_timestamp(str(bar_time_raw))
                if not ticker or bar_time_ts is None:
                    skipped += 1
                    continue

                bar_time   = str(bar_time_ts)
                dedupe_key = base_v15._signal_dedupe_key(ticker, side_upper, bar_time, setup)
                if dedupe_key in existing_keys or dedupe_key in run_keys:
                    skipped += 1
                    continue
                signal_id = base_v15._generate_signal_id(ticker, side_upper, bar_time, setup)
                if signal_id in existing_ids:
                    skipped += 1
                    continue

                entry_price  = _safe_float(sig.get("entry_price",  0.0), 0.0)
                stop_price   = _safe_float(sig.get("stop_price",   0.0), 0.0)
                target_price = _safe_float(sig.get("target_price", 0.0), 0.0)
                notional = DEFAULT_POSITION_SIZE_RS * INTRADAY_LEVERAGE
                qty = max(1, int(notional / entry_price)) if entry_price > 0 else 1

                row = {
                    "signal_id":                  signal_id,
                    "signal_datetime":             bar_time,
                    "received_time":               received_time,
                    "detected_time_ist":           received_time,
                    "logtime_ist":                 received_time,
                    "ticker":                      ticker,
                    "side":                        side_upper,
                    "setup":                       setup,
                    "impulse_type":                str(sig.get("impulse_type", "")),
                    "entry_price":                 round(entry_price, 2),
                    "stop_price":                  round(stop_price, 2),
                    "target_price":                round(target_price, 2),
                    "quality_score":               round(_safe_float(sig.get("quality_score", 0.0), 0.0), 4),
                    "atr_pct":                     round(_safe_float(sig.get("atr_pct", 0.0), 0.0), 6),
                    "rsi":                         round(_safe_float(sig.get("rsi_signal", sig.get("rsi", 0.0)), 0.0), 2),
                    "adx":                         round(_safe_float(sig.get("adx_signal", sig.get("adx", 0.0)), 0.0), 2),
                    "quantity":                    qty,
                    "signal_entry_datetime_ist":   bar_time,
                    "signal_bar_time_ist":         bar_time,
                }
                writer.writerow(row)
                existing_ids.add(signal_id)
                existing_keys.add(dedupe_key)
                run_keys.add(dedupe_key)
                written += 1

    print(
        f"[V16_5MIN {side_upper} CSV] written={written} skipped={skipped} path={csv_path}",
        flush=True,
    )
    return written


# ===========================================================================
# SCAN ONE SLOT — scan all tickers and return signal dicts
# ===========================================================================
def _normalize_snapshot_shard_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    if "date" in out.columns:
        dt = pd.to_datetime(out["date"], errors="coerce")
        if getattr(dt.dt, "tz", None) is None:
            dt = dt.dt.tz_localize(IST)
        else:
            dt = dt.dt.tz_convert(IST)
        out["date"] = dt
        out = out.dropna(subset=["date"])
    if "ticker" in out.columns:
        out["ticker"] = out["ticker"].astype(str).str.upper().str.strip()
    sort_cols = [c for c in ["ticker", "date"] if c in out.columns]
    if sort_cols:
        out = out.sort_values(sort_cols).reset_index(drop=True)
    return out


def _scan_partition_worker(
    slot_iso: str,
    partition_name: str,
    partition_tickers: List[str],
    short_cfg: StrategyConfig,
    long_cfg: StrategyConfig,
    allow_long: bool,
    allow_short: bool,
    use_slot_snapshots: bool,
) -> Tuple[Dict[str, Any], List[dict], List[dict]]:
    slot_ts = pd.Timestamp(slot_iso)
    if slot_ts.tzinfo is None:
        slot_ts = slot_ts.tz_localize(IST)
    else:
        slot_ts = slot_ts.tz_convert(IST)
    today = slot_ts.date()

    part_short_rows: List[dict] = []
    part_long_rows: List[dict] = []
    errors = 0
    started = time.perf_counter()
    source_rows = 0

    if use_slot_snapshots:
        shard_df = _normalize_snapshot_shard_df(read_shard_snapshot(slot_ts, partition_name))
        source_rows = int(len(shard_df))
        grouped_frames = list(shard_df.groupby("ticker", sort=True)) if not shard_df.empty else []
    else:
        grouped_frames = [(ticker, _load_5m_parquet(ticker)) for ticker in partition_tickers]

    scanned_tickers = 0
    for ticker, df in grouped_frames:
        try:
            if df is None or df.empty:
                continue
            scanned_tickers += 1

            df_prepared = prepare_session_bars_for_scan(df, short_cfg)
            if df_prepared is None or df_prepared.empty:
                continue

            if "date" in df_prepared.columns:
                df_prepared = df_prepared[pd.to_datetime(df_prepared["date"]).dt.date == today]
            if df_prepared.empty:
                continue

            if "datetime" in df_prepared.columns:
                bar_times = pd.to_datetime(df_prepared["datetime"], utc=True).dt.tz_convert(IST)
                df_prepared = df_prepared[bar_times <= slot_ts]
            if df_prepared.empty:
                continue

            if allow_short:
                s_trades = scan_short_prepared(ticker, df_prepared, short_cfg)
                for trade in s_trades:
                    row = asdict(trade) if not isinstance(trade, dict) else trade
                    row["side"] = "SHORT"
                    part_short_rows.append(row)

            if allow_long:
                l_trades = scan_long_prepared(ticker, df_prepared, long_cfg)
                for trade in l_trades:
                    row = asdict(trade) if not isinstance(trade, dict) else trade
                    row["side"] = "LONG"
                    part_long_rows.append(row)
        except Exception:
            errors += 1

    elapsed = time.perf_counter() - started
    meta = {
        "partition": partition_name,
        "tickers": int(scanned_tickers),
        "errors": int(errors),
        "short_rows": len(part_short_rows),
        "long_rows": len(part_long_rows),
        "source_rows": int(source_rows),
        "elapsed_sec": round(elapsed, 3),
        "mode": "snapshot" if use_slot_snapshots else "raw",
    }
    return meta, part_short_rows, part_long_rows


def _scan_slot(
    slot_ist: datetime,
    short_cfg: StrategyConfig,
    long_cfg: StrategyConfig,
    allow_long: bool,
    allow_short: bool,
    tickers: Optional[List[str]] = None,
    prebuilt_snapshot_meta: Optional[Dict[str, Any]] = None,
) -> Tuple[List[dict], List[dict], Dict[str, Any]]:
    """
    Scan all tickers for today's signals up to (and including) slot_ist.
    Returns (short_signal_dicts, long_signal_dicts).
    """
    tickers = list(tickers or _list_tickers_5m())
    partitions = [part for part in _split_tickers_for_scan_shards(tickers, SCAN_SHARDS) if part]
    snapshot_meta: Dict[str, Any] = {}
    shard_names: List[str] = []

    if USE_SLOT_SNAPSHOTS:
        snapshot_meta = dict(prebuilt_snapshot_meta or {})
        if not snapshot_meta:
            snapshot_meta = build_slot_snapshots(
                slot_ist,
                shard_count=SCAN_SHARDS,
                tail_rows=TAIL_ROWS,
                max_workers=SNAPSHOT_MAX_WORKERS,
                use_rolling_cache=USE_SNAPSHOT_ROLLING_CACHE,
                build_slot_context=True,
            )
        shard_names = sorted(str(k) for k in (snapshot_meta.get("snapshot_paths") or {}).keys())
    else:
        shard_names = [f"{idx:02d}" for idx, _part in enumerate(partitions, 1)]

    effective_workers = min(max(1, int(SCAN_MAX_WORKERS)), len(shard_names))

    short_rows: List[dict] = []
    long_rows: List[dict] = []
    partition_metas: List[Dict[str, Any]] = []

    print(
        f"[SCAN] Starting sharded scan: tickers={len(tickers)} "
        f"shards={len(shard_names)} workers={effective_workers} "
        f"mode={'snapshot' if USE_SLOT_SNAPSHOTS else 'raw'}",
        flush=True,
    )

    scan_started = time.perf_counter()
    with ProcessPoolExecutor(
        max_workers=effective_workers,
        mp_context=mp.get_context("spawn"),
    ) as executor:
        future_map = {
            executor.submit(
                _scan_partition_worker,
                slot_ist.isoformat(),
                shard_names[idx - 1],
                partition_tickers,
                short_cfg,
                long_cfg,
                allow_long,
                allow_short,
                USE_SLOT_SNAPSHOTS,
            ): idx
            for idx, partition_tickers in enumerate(partitions, 1)
        }
        for future in as_completed(future_map):
            meta, part_short_rows, part_long_rows = future.result()
            partition_metas.append(meta)
            short_rows.extend(part_short_rows)
            long_rows.extend(part_long_rows)
            print(
                f"  [SCAN] shard={meta['partition']} tickers={meta['tickers']} "
                f"short={meta['short_rows']} long={meta['long_rows']} "
                f"errors={meta['errors']} rows={meta['source_rows']} "
                f"elapsed={meta['elapsed_sec']:.1f}s",
                flush=True,
            )

    scan_elapsed = time.perf_counter() - scan_started
    total_elapsed = float(snapshot_meta.get("total_elapsed_sec", 0.0)) + scan_elapsed
    print(
        f"[SCAN] Done: tickers={len(tickers)} raw_short={len(short_rows)} raw_long={len(long_rows)} "
        f"partitions={len(partition_metas)} scan_elapsed={scan_elapsed:.1f}s total_elapsed={total_elapsed:.1f}s",
        flush=True,
    )
    meta = {
        "mode": "snapshot" if USE_SLOT_SNAPSHOTS else "raw",
        "snapshot_meta": snapshot_meta,
        "scan_elapsed_sec": round(scan_elapsed, 3),
        "total_elapsed_sec": round(total_elapsed, 3),
        "shards": int(len(shard_names)),
        "effective_workers": int(effective_workers),
    }
    return short_rows, long_rows, meta


# ===========================================================================
# APPLY NIFTY RS FILTER TO SIGNAL DICTS
# ===========================================================================
def _apply_rs_filter_dicts(
    short_rows: List[dict],
    long_rows: List[dict],
    allow_long: bool,
    allow_short: bool,
) -> Tuple[List[dict], List[dict]]:
    if not allow_short:
        short_rows = []
    if not allow_long:
        long_rows = []
    return short_rows, long_rows


# ===========================================================================
# APPLY V16 POST-SCAN FILTERS TO SIGNAL DICTS (via DataFrame)
# ===========================================================================
def _apply_v16_filters_to_dicts(
    short_rows: List[dict],
    long_rows: List[dict],
) -> Tuple[List[dict], List[dict]]:
    short_df = pd.DataFrame(short_rows) if short_rows else pd.DataFrame()
    long_df  = pd.DataFrame(long_rows)  if long_rows  else pd.DataFrame()

    short_df, long_df = _apply_v16_post_scan_filters(short_df, long_df)

    short_out = short_df.to_dict("records") if not short_df.empty else []
    long_out  = long_df.to_dict("records")  if not long_df.empty  else []
    return short_out, long_out


def _blocked_nifty_context_message(slot: datetime, slot_payload: Optional[Dict[str, Any]]) -> str:
    nifty_context = (slot_payload or {}).get("nifty_context") or {}
    reason = str(nifty_context.get("decision_reason", "") or "").strip()
    source = str(nifty_context.get("source", "live_nifty") or "live_nifty")
    day_move_pct = nifty_context.get("day_move_pct")
    rs_pct = nifty_context.get("rs_pct")

    parts = [f"slot={slot.strftime('%H:%M')}", f"source={source}"]
    if reason:
        parts.append(f"reason={reason}")
    try:
        if day_move_pct is not None:
            parts.append(f"day_move={float(day_move_pct):+.2f}%")
    except Exception:
        pass
    try:
        if rs_pct is not None:
            parts.append(f"rs={float(rs_pct):+.2f}%")
    except Exception:
        pass
    return "[SKIP_SLOT] No context. " + " | ".join(parts)


# ===========================================================================
# STATUS / HEARTBEAT HELPERS
# ===========================================================================
def _touch_status(status: str, **extra: Any) -> None:
    os.environ["EQIDV2_RUNTIME_STATUS_FILE"] = str(_LOG_DIR / "eqidv2_live_combined_analyser_csv_v16_5min.status")
    os.environ["EQIDV2_RUNTIME_HEARTBEAT_FILE"] = str(_LOG_DIR / "eqidv2_live_combined_analyser_csv_v16_5min.heartbeat")
    os.environ["EQIDV2_RUNTIME_SCRIPT_NAME"] = _SCRIPT_NAME
    base_v15._touch_runtime_status(status, **extra)


def _touch_heartbeat(state: str = "RUNNING", **extra: Any) -> None:
    base_v15._touch_runtime_heartbeat(state, **extra)


# ===========================================================================
# MAIN LOOP
# ===========================================================================
def main() -> None:
    if mp.current_process().name != "MainProcess":
        return
    _start_tee()

    print(
        "=" * 70 + "\n"
        "EQIDV2 V16 5min Live Scanner — Anti-exhaustion filters\n"
        f"  DATA_5M_DIR : {DIR_5M}\n"
        f"  SIGNALS_DIR : {RUNTIME_LIVE_SIGNALS_DIR}\n"
        f"  TARGET      : SHORT={TEST_SHORT_TARGET_PCT*100:.2f}%, LONG={TEST_LONG_TARGET_PCT*100:.2f}%\n"
        f"  RS filter   : LONG>={NIFTY_RS_BOTH_MODE_THRESHOLD_LONG_PCT}%, SHORT<={-NIFTY_RS_BOTH_MODE_THRESHOLD_SHORT_PCT}%\n"
        "=" * 70,
        flush=True,
    )

    # Set env vars for status file routing
    _touch_status("STARTING")

    short_cfg, long_cfg = _build_v16_cfgs()
    print(
        f"[CONFIG] SHORT: SL={short_cfg.stop_pct*100:.2f}% TGT={short_cfg.target_pct*100:.2f}% "
        f"ADX>={short_cfg.adx_min} volume>={short_cfg.volume_min_ratio}",
        flush=True,
    )
    print(
        f"[CONFIG] LONG:  SL={long_cfg.stop_pct*100:.2f}% TGT={long_cfg.target_pct*100:.2f}% "
        f"ADX>={long_cfg.adx_min} volume>={long_cfg.volume_min_ratio}",
        flush=True,
    )

    tickers = _list_tickers_5m()
    holidays = base_v15._read_holidays_safe()
    cache_day: Optional[Any] = None
    print(f"[INFO] Tickers in 5-min dir: {len(tickers)}", flush=True)
    print(
        f"[INFO] Slot-ready polling: offset={SLOT_START_OFFSET_SECONDS}s max_wait={SLOT_READY_MAX_WAIT_SECONDS}s "
        f"poll={SLOT_READY_POLL_SECONDS}s sample={SLOT_READY_SAMPLE_SIZE} "
        f"min_fresh_ratio={SLOT_READY_MIN_FRESH_RATIO:.2f} "
        f"ready_marker={USE_SCHEDULER_READY_MARKER}",
        flush=True,
    )
    print(
        f"[INFO] Scan sharding: shards={SCAN_SHARDS} max_workers={SCAN_MAX_WORKERS} tail_rows={TAIL_ROWS}",
        flush=True,
    )
    print(
        f"[INFO] Slot snapshots: enabled={USE_SLOT_SNAPSHOTS} "
        f"snapshot_workers={SNAPSHOT_MAX_WORKERS} rolling_cache={USE_SNAPSHOT_ROLLING_CACHE}",
        flush=True,
    )

    if BENCHMARK_SLOT_RAW:
        slot_ts = pd.Timestamp(BENCHMARK_SLOT_RAW)
        if slot_ts.tzinfo is None:
            slot_ts = slot_ts.tz_localize(IST)
        else:
            slot_ts = slot_ts.tz_convert(IST)
        slot_dt = slot_ts.to_pydatetime()
        prebuilt_snapshot_meta: Optional[Dict[str, Any]] = None
        if USE_SLOT_SNAPSHOTS:
            prebuilt_snapshot_meta = build_slot_snapshots(
                slot_dt,
                shard_count=SCAN_SHARDS,
                tail_rows=TAIL_ROWS,
                max_workers=SNAPSHOT_MAX_WORKERS,
                use_rolling_cache=USE_SNAPSHOT_ROLLING_CACHE,
                build_slot_context=True,
            )
            allow_long, allow_short, rs_pct, slot_payload = _load_snapshot_slot_context(slot_dt)
        else:
            allow_long, allow_short, rs_pct = _compute_nifty_rs_at_slot(slot_dt)
            slot_payload = {}
        started = time.perf_counter()
        short_rows, long_rows, scan_meta = _scan_slot(
            slot_dt,
            short_cfg,
            long_cfg,
            allow_long,
            allow_short,
            tickers=tickers,
            prebuilt_snapshot_meta=prebuilt_snapshot_meta,
        )
        elapsed = time.perf_counter() - started
        summary = {
            "slot": slot_ts.strftime("%Y-%m-%d %H:%M:%S%z"),
            "tickers": len(tickers),
            "allow_long": bool(allow_long),
            "allow_short": bool(allow_short),
            "raw_short": len(short_rows),
            "raw_long": len(long_rows),
            "elapsed_sec": round(elapsed, 3),
            "scan_shards": int(SCAN_SHARDS),
            "scan_max_workers": int(SCAN_MAX_WORKERS),
            "snapshot_max_workers": int(SNAPSHOT_MAX_WORKERS),
            "use_slot_snapshots": bool(USE_SLOT_SNAPSHOTS),
            "tail_rows": int(TAIL_ROWS),
            "scan_elapsed_sec": float(scan_meta.get("scan_elapsed_sec", 0.0)),
            "scan_total_elapsed_sec": float(scan_meta.get("total_elapsed_sec", 0.0)),
            "slot_context_source": "snapshot" if slot_payload else "live_nifty",
        }
        snapshot_meta = scan_meta.get("snapshot_meta") or {}
        if snapshot_meta:
            summary["snapshot_build_elapsed_sec"] = float(snapshot_meta.get("total_elapsed_sec", 0.0))
        print("[BENCHMARK] " + json.dumps(summary, sort_keys=True), flush=True)
        return

    while True:
        now = _now_ist()
        _touch_status("RUNNING", phase="LOOP")
        _touch_heartbeat("RUNNING", phase="LOOP")

        if now.time() >= HARD_STOP_TIME:
            _touch_status("STOPPED_AFTER_CUTOFF")
            _touch_heartbeat("STOPPED")
            print("[STOP] Hard-stop reached. Exiting.", flush=True)
            return

        if not base_v15.is_trading_day_safe(now.date(), holidays):
            clear_slot_snapshot_cache()
            nxt = base_v15._next_trading_day_start(now, holidays)
            print(f"[SKIP] Not a trading day. Sleeping until {base_v15._fmt_ist_dt(nxt)}.", flush=True)
            base_v15._sleep_until(nxt)
            holidays = base_v15._read_holidays_safe()
            continue

        slot = _next_5min_slot(now)
        if slot.date() != now.date():
            clear_slot_snapshot_cache()
            nxt = base_v15._next_trading_day_start(now, holidays)
            print(f"[DONE] Past END_TIME. Sleeping until {base_v15._fmt_ist_dt(nxt)}.", flush=True)
            base_v15._sleep_until(nxt)
            holidays = base_v15._read_holidays_safe()
            continue

        if now < slot:
            print(f"[WAIT] Sleeping until slot {slot.strftime('%Y-%m-%d %H:%M:%S%z')}", flush=True)
            base_v15._sleep_until(slot)

        now = _now_ist()
        if now.time() > END_TIME:
            nxt = base_v15._next_trading_day_start(now, holidays)
            print(f"[DONE] Past END_TIME. Sleeping until {base_v15._fmt_ist_dt(nxt)}.", flush=True)
            base_v15._sleep_until(nxt)
            holidays = base_v15._read_holidays_safe()
            continue

        # ------------------------------------------------------------------
        # Run scan for this slot
        # ------------------------------------------------------------------
        if cache_day != slot.date():
            clear_slot_snapshot_cache()
            cache_day = slot.date()
        slot_start = time.perf_counter()
        signal_day_str = slot.strftime("%Y-%m-%d")
        print(f"\n[SLOT] {slot.strftime('%Y-%m-%d %H:%M:%S%z')}", flush=True)
        _touch_status("RUNNING", phase="WAIT_DATA", slot=slot.strftime("%H:%M"))
        _touch_heartbeat("RUNNING", phase="WAIT_DATA", slot=slot.strftime("%H:%M"))
        ready, ratio, waited, checked = _wait_for_slot_data_ready(slot, tickers)
        print(
            f"[WAIT] slot={slot.strftime('%H:%M')} ready={ready} "
            f"fresh_ratio={ratio:.2f} waited={waited:.1f}s checked={checked}",
            flush=True,
        )
        if not ready and SKIP_STALE_SLOT_ON_TIMEOUT:
            _touch_status("RUNNING", phase="SKIP_STALE", slot=slot.strftime("%H:%M"))
            _touch_heartbeat("RUNNING", phase="SKIP_STALE", slot=slot.strftime("%H:%M"))
            print(
                f"[SKIP_SLOT] slot={slot.strftime('%H:%M')} data freshness timed out "
                f"(ratio={ratio:.2f}). Skipping stale scan.",
                flush=True,
            )
            continue
        _touch_status("RUNNING", phase="SCAN", slot=slot.strftime("%H:%M"))
        _touch_heartbeat("RUNNING", phase="SCAN", slot=slot.strftime("%H:%M"))

        prebuilt_snapshot_meta: Optional[Dict[str, Any]] = None
        if USE_SLOT_SNAPSHOTS:
            prebuilt_snapshot_meta = build_slot_snapshots(
                slot,
                shard_count=SCAN_SHARDS,
                tail_rows=TAIL_ROWS,
                max_workers=SNAPSHOT_MAX_WORKERS,
                use_rolling_cache=USE_SNAPSHOT_ROLLING_CACHE,
                build_slot_context=True,
            )
            allow_long, allow_short, rs_pct, _slot_payload = _load_snapshot_slot_context(slot)
        else:
            allow_long, allow_short, rs_pct = _compute_nifty_rs_at_slot(slot)

        if not allow_long and not allow_short:
            print(_blocked_nifty_context_message(slot, _slot_payload if USE_SLOT_SNAPSHOTS else None), flush=True)
        else:
            short_rows, long_rows, scan_meta = _scan_slot(
                slot,
                short_cfg,
                long_cfg,
                allow_long,
                allow_short,
                tickers=tickers,
                prebuilt_snapshot_meta=prebuilt_snapshot_meta,
            )
            short_rows, long_rows = _apply_rs_filter_dicts(short_rows, long_rows, allow_long, allow_short)
            short_rows, long_rows = _apply_v16_filters_to_dicts(short_rows, long_rows)

            short_written = _write_side_signals_csv(short_rows, "SHORT", signal_day_str)
            long_written  = _write_side_signals_csv(long_rows,  "LONG",  signal_day_str)

            elapsed = time.perf_counter() - slot_start
            print(
                f"[SLOT_DONE] slot={slot.strftime('%H:%M')} "
                f"short_written={short_written} long_written={long_written} "
                f"scan_elapsed={float(scan_meta.get('scan_elapsed_sec', 0.0)):.1f}s "
                f"total_elapsed={elapsed:.1f}s",
                flush=True,
            )

        _touch_status("RUNNING", phase="SCAN_DONE", slot=slot.strftime("%H:%M"))
        _touch_heartbeat("RUNNING", phase="SCAN_DONE", slot=slot.strftime("%H:%M"))

        # Sleep until next slot
        next_slot = slot + timedelta(minutes=SLOT_MINUTES)
        now_after = _now_ist()
        if now_after < next_slot:
            time.sleep(2.0)


if __name__ == "__main__":
    mp.freeze_support()
    if mp.current_process().name == "MainProcess":
        main()
