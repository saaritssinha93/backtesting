"""
EQIDV2 ID 5-min v7 persistent live scanner.

This is a 5-minute cadence sibling of
eqidv2_live_combined_analyser_csv_v15_new_persistent.py. Architecture, status
hooks, shard fan-out, and CSV writer mechanics are identical; the differences
are:

  * Slot cadence: 5 min (v15_new uses 15 min).
  * Slot-ready freshness gate defaults to 0.95 (v15_new uses 0.70). Override
    via the EQIDV2_SLOT_READY_MIN_FRESH_RATIO env var on the bat side.
  * Post-slot-ready stagger defaults to 10 s (configurable via
    EQIDV2_POST_READY_STAGGER_SECONDS on the bat side).
  * Output CSVs go to LIVE_SIGNALS_DIR as
        signals_<YYYY-MM-DD>_id_5min_v7_short.csv
        signals_<YYYY-MM-DD>_id_5min_v7_long.csv
  * Session summaries live under <runtime_root>/live_id_5min_v7_persistent/.

The underlying scan invocation still routes through v15_new shard workers; if
the scan layer needs to be swapped for the v16 5-min cascade, only
`_run_slot_scan` has to change.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

import eqidv2_live_combined_analyser_csv_v15 as base_v15
import eqidv2_live_combined_analyser_csv_v15_new as v15_new
from eqidv2_runtime_paths import LIVE_SIGNALS_DIR, RUNTIME_ROOT, runtime_dir
from live_v15_slot_snapshot_v15_new import (
    build_slot_snapshots,
    clear_rolling_cache,
    DEFAULT_SNAPSHOT_MAX_WORKERS,
    normalize_slot_ist,
    slot_key,
)

# Phase 1 swap (2026-05-19): _run_slot_scan now routes through the v7 live
# scan adapter (avwap_5min_ID_v7_live_scan) instead of v15_new shard fan-out.
# The v15_new imports above are retained because clear_rolling_cache and
# normalize_slot_ist / slot_key are still used by main() and replay flows.
import avwap_5min_ID_v7_live_scan as v7_live_scan


SHORT_SIGNAL_CSV_PATTERN = "signals_{}_id_5min_v7_short.csv"
LONG_SIGNAL_CSV_PATTERN = "signals_{}_id_5min_v7_long.csv"
DEFAULT_SNAPSHOT_WORKERS = int(DEFAULT_SNAPSHOT_MAX_WORKERS)
DEFAULT_SCAN_WORKERS = int(v15_new.DEFAULT_SCAN_WORKERS)
DEFAULT_SHARD_COUNT = 10
ID5MIN_V7_DEFAULT_POSITION_SIZE_RS = float(
    os.getenv("EQIDV2_ID5MIN_V7_DEFAULT_POSITION_SIZE_RS", "20000")
)
SLOT_MINUTES = int(os.getenv("EQIDV2_ID5MIN_V7_SLOT_MINUTES", "5"))
# 2026-05-20: fixed post-slot delay replaces the v15 freshness gate (which
# checked the wrong data location and always false-timed-out at 60s). After
# the slot boundary we wait this many seconds for the 5-min fetcher to write
# the new bar, then scan immediately. The adapter emits signals for the latest
# bar that has a successor, so a slightly-late fetcher just shifts which bar
# fires this slot (no missed bars — CSV dedup covers re-emits).
POST_SLOT_DELAY_SEC = int(os.getenv("EQIDV2_ID5MIN_V7_POST_SLOT_DELAY_SEC", "15"))
# Live selection (mirrors v2._apply_daily_selection): collapse to the best
# setup per (ticker, side, bar), dedup one trade per ticker per day, cap the
# day at MAX_TRADES_PER_DAY total and MAX_SAME_SIDE_PER_DAY per side. This
# turns the raw candidate firehose into an executable trade list.
SELECTION_ENABLED = str(os.getenv("EQIDV2_ID5MIN_V7_SELECTION_ENABLED", "1")).strip().lower() in {"1", "true", "yes", "on"}
MAX_TRADES_PER_DAY = int(os.getenv("EQIDV2_ID5MIN_V7_MAX_TRADES_PER_DAY", "24"))
MAX_SAME_SIDE_PER_DAY = int(os.getenv("EQIDV2_ID5MIN_V7_MAX_SAME_SIDE_PER_DAY", "16"))
# Allow the same ticker to re-enter on a later bar (False = backtest-faithful,
# one trade per ticker per day).
ALLOW_TICKER_REENTRY = str(os.getenv("EQIDV2_ID5MIN_V7_ALLOW_TICKER_REENTRY", "0")).strip().lower() in {"1", "true", "yes", "on"}
SESSION_RUNTIME_DIR_NAME = "live_id_5min_v7_persistent"
START_TIME = base_v15.START_TIME
END_TIME = base_v15.dtime(15, 0)
HARD_STOP_TIME = base_v15.dtime(15, 30)
ENTRY_WINDOW_START_RAW = os.getenv("EQIDV2_ID5MIN_V7_ENTRY_WINDOW_START", "09:30").strip()
ENTRY_WINDOW_END_RAW = os.getenv("EQIDV2_ID5MIN_V7_ENTRY_WINDOW_END", "14:30").strip()
ENTRY_SIGNAL_TO_ENTRY_LAG_MIN = int(os.getenv("EQIDV2_ID5MIN_V7_ENTRY_LAG_MIN", "1"))
MAX_ENTRY_TO_DETECTION_LAG_SEC = float(
    os.getenv("EQIDV2_ID5MIN_V7_MAX_ENTRY_TO_DETECTION_LAG_SEC", "30")
)
# This retired scanner shares its CSV writer with the active 1-minute entry
# engine. Keep the scanner itself shadow-only unless explicitly enabled.
LEGACY_LIVE_CSV_WRITE_ENABLED = str(
    os.getenv("EQIDV2_ID5MIN_V7_LEGACY_WRITE_LIVE_CSVS", "0")
).strip().lower() in {"1", "true", "yes", "on"}


def _parse_hhmm_time(value: str, default: str):
    raw = str(value or default).strip()
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            return datetime.strptime(raw, fmt).time()
        except ValueError:
            continue
    return datetime.strptime(default, "%H:%M").time()


ENTRY_WINDOW_START = _parse_hhmm_time(ENTRY_WINDOW_START_RAW, "09:30")
ENTRY_WINDOW_END = _parse_hhmm_time(ENTRY_WINDOW_END_RAW, "14:30")


def _entry_time_from_signal_row(row: pd.Series, bar_time_ts: datetime) -> datetime:
    for key in ("entry_time_ist", "entry_ts", "entry_datetime_ist"):
        raw = row.get(key, "")
        parsed = base_v15._parse_ist_timestamp(str(raw)) if str(raw or "").strip() else None
        if parsed is not None:
            return parsed
    legacy_entry_raw = row.get("signal_entry_datetime_ist", "")
    legacy_entry_ts = (
        base_v15._parse_ist_timestamp(str(legacy_entry_raw))
        if str(legacy_entry_raw or "").strip()
        else None
    )
    if legacy_entry_ts is not None:
        if abs((legacy_entry_ts - bar_time_ts).total_seconds()) < 1:
            return bar_time_ts + timedelta(minutes=ENTRY_SIGNAL_TO_ENTRY_LAG_MIN)
        return legacy_entry_ts
    return bar_time_ts + timedelta(minutes=ENTRY_SIGNAL_TO_ENTRY_LAG_MIN)


def _entry_window_ok(entry_time_ts: datetime) -> bool:
    t = entry_time_ts.astimezone(base_v15.IST).time()
    return ENTRY_WINDOW_START <= t <= ENTRY_WINDOW_END


def _safe_float(value: Any, default: float = np.nan) -> float:
    try:
        out = float(value)
        if np.isfinite(out):
            return out
        return float(default)
    except Exception:
        return float(default)


def _session_summary_dir(runtime_root: Optional[Path | str] = None) -> Path:
    if runtime_root is None:
        out_dir = runtime_dir(SESSION_RUNTIME_DIR_NAME)
    else:
        out_dir = Path(runtime_root) / SESSION_RUNTIME_DIR_NAME
        out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir


def _signal_csv_path(signal_day_str: str, side: str) -> Path:
    pattern = SHORT_SIGNAL_CSV_PATTERN if str(side).upper() == "SHORT" else LONG_SIGNAL_CSV_PATTERN
    return Path(LIVE_SIGNALS_DIR) / pattern.format(signal_day_str)


def _load_existing_intraday_tickers(signal_day_str: str) -> set[str]:
    tickers: set[str] = set()
    for side in ("SHORT", "LONG"):
        path = _signal_csv_path(signal_day_str, side)
        if not path.exists() or path.stat().st_size <= 0:
            continue
        try:
            existing = pd.read_csv(path, usecols=["ticker"])
        except Exception:
            continue
        tickers.update(existing["ticker"].dropna().astype(str).str.upper().str.strip())
    return {t for t in tickers if t}


def _next_slot_after_id5min_v7(now: datetime) -> datetime:
    now = now.astimezone(base_v15.IST)
    today = now.date()
    start_dt = base_v15.IST.localize(datetime.combine(today, START_TIME))
    end_dt = base_v15.IST.localize(datetime.combine(today, END_TIME))

    if now <= start_dt:
        return start_dt
    if now > end_dt:
        tomorrow = today + timedelta(days=1)
        return base_v15.IST.localize(datetime.combine(tomorrow, START_TIME))

    minute = (now.minute // SLOT_MINUTES) * SLOT_MINUTES
    slot = now.replace(minute=minute, second=0, microsecond=0)
    if slot < now:
        slot += timedelta(minutes=SLOT_MINUTES)
    if slot < start_dt:
        slot = start_dt
    if slot > end_dt:
        tomorrow = today + timedelta(days=1)
        slot = base_v15.IST.localize(datetime.combine(tomorrow, START_TIME))
    return slot


def _load_signals_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_parquet(path)
    except Exception:
        return pd.DataFrame()


def _load_slot_signal_frames(
    slot_ts: Any,
    *,
    runtime_root: Optional[Path | str],
    shard_count: int,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    slot_ist = normalize_slot_ist(slot_ts)
    short_frames: List[pd.DataFrame] = []
    long_frames: List[pd.DataFrame] = []
    for shard_id in range(1, int(shard_count) + 1):
        shard_dir = v15_new.shard_output_dir(slot_ist, shard_id, runtime_root)
        short_df = _load_signals_parquet(shard_dir / "short_signals.parquet")
        long_df = _load_signals_parquet(shard_dir / "long_signals.parquet")
        if not short_df.empty:
            short_frames.append(short_df)
        if not long_df.empty:
            long_frames.append(long_df)
    short_all = pd.concat(short_frames, ignore_index=True) if short_frames else pd.DataFrame()
    long_all = pd.concat(long_frames, ignore_index=True) if long_frames else pd.DataFrame()
    return short_all, long_all


def _write_side_signals_csv(
    signals_df: pd.DataFrame,
    *,
    side: str,
    signal_day_str: str,
) -> int:
    # Production writes must use eqidv2_live_signal_writer.write_side_signals().
    # This function is retained only so callers that check LEGACY_LIVE_CSV_WRITE_ENABLED
    # can still run in explicit shadow/debug mode.  Calling it when the flag is
    # off is always a mistake — raise immediately so the error is visible.
    if not LEGACY_LIVE_CSV_WRITE_ENABLED:
        raise RuntimeError(
            "_write_side_signals_csv called on the retired scanner but "
            "EQIDV2_ID5MIN_V7_LEGACY_WRITE_LIVE_CSVS is not enabled. "
            "Production writes must go through entry_engine_1min_v5_id via "
            "eqidv2_live_signal_writer.write_side_signals()."
        )
    side_upper = str(side).upper()
    if signals_df is None or signals_df.empty:
        print(f"[ID5MIN_V7 {side_upper} CSV] scanned=0 written=0", flush=True)
        return 0

    if "side" in signals_df.columns:
        df_side = signals_df.loc[
            signals_df["side"].astype(str).str.upper().eq(side_upper)
        ].copy()
    else:
        df_side = signals_df.copy()
        df_side["side"] = side_upper
    if df_side.empty:
        print(f"[ID5MIN_V7 {side_upper} CSV] scanned={len(signals_df)} written=0", flush=True)
        return 0

    csv_path = _signal_csv_path(signal_day_str, side_upper)
    received_ts = base_v15.now_ist()
    received_time = received_ts.strftime("%Y-%m-%d %H:%M:%S%z")
    written = 0
    skipped_duplicate_key = 0
    skipped_duplicate_id = 0
    skipped_intraday_ticker = 0
    skipped_missing_time = 0
    skipped_entry_window = 0
    skipped_stale_entry = 0
    skipped_future_entry = 0

    with base_v15._locked_signal_csv(str(csv_path)):
        base_v15._ensure_signal_csv_schema(str(csv_path))
        existing_ids = base_v15._load_existing_ids(str(csv_path))
        existing_keys = base_v15._load_existing_signal_keys(str(csv_path))
        existing_tickers = _load_existing_intraday_tickers(signal_day_str)
        file_exists = csv_path.exists() and csv_path.stat().st_size > 0
        run_keys: set = set()
        run_tickers: set = set()

        with open(csv_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=base_v15.SIGNAL_CSV_COLUMNS,
                quoting=csv.QUOTE_ALL,
            )
            if not file_exists:
                writer.writeheader()

            for _, row in df_side.iterrows():
                ticker = str(row.get("ticker", "")).upper().strip()
                setup = str(row.get("setup", ""))
                bar_time_raw = row.get("bar_time_ist", "")
                bar_time_ts = base_v15._parse_ist_timestamp(str(bar_time_raw))
                if not ticker or bar_time_ts is None:
                    skipped_missing_time += 1
                    continue
                entry_time_ts = _entry_time_from_signal_row(row, bar_time_ts)
                if not _entry_window_ok(entry_time_ts):
                    skipped_entry_window += 1
                    continue
                entry_to_detection_lag_sec = (received_ts - entry_time_ts).total_seconds()
                if MAX_ENTRY_TO_DETECTION_LAG_SEC > 0:
                    if entry_to_detection_lag_sec < 0:
                        skipped_future_entry += 1
                        continue
                    if entry_to_detection_lag_sec > MAX_ENTRY_TO_DETECTION_LAG_SEC:
                        skipped_stale_entry += 1
                        continue
                if ticker in existing_tickers or ticker in run_tickers:
                    skipped_intraday_ticker += 1
                    continue
                bar_time = str(bar_time_ts)
                entry_time = str(entry_time_ts)
                dedupe_key = base_v15._signal_dedupe_key(ticker, side_upper, bar_time, setup)
                if dedupe_key in existing_keys or dedupe_key in run_keys:
                    skipped_duplicate_key += 1
                    continue
                signal_id = base_v15._generate_signal_id(ticker, side_upper, bar_time, setup)
                if signal_id in existing_ids:
                    skipped_duplicate_id += 1
                    continue

                entry_price = float(_safe_float(row.get("entry_price", np.nan), 0.0))
                stop_price = float(_safe_float(row.get("sl_price", np.nan), 0.0))
                target_price = float(_safe_float(row.get("target_price", np.nan), 0.0))
                notional = float(ID5MIN_V7_DEFAULT_POSITION_SIZE_RS) * float(base_v15.INTRADAY_LEVERAGE)
                qty = max(1, int(notional / entry_price)) if entry_price > 0 else 1

                atr_pct = 0.0
                rsi_val = 0.0
                adx_val = 0.0
                impulse_type = ""
                diag: Dict[str, Any] = {}
                diag_raw = row.get("diagnostics_json", "")
                if isinstance(diag_raw, dict):
                    diag = dict(diag_raw)
                elif isinstance(diag_raw, str) and diag_raw.strip():
                    try:
                        diag = json.loads(diag_raw)
                    except Exception:
                        diag = {}
                if diag:
                    atr_pct_val = _safe_float(diag.get("atr_pct", np.nan), np.nan)
                    if np.isfinite(atr_pct_val):
                        atr_pct = float(atr_pct_val)
                    else:
                        atr_val = _safe_float(diag.get("atr", np.nan), np.nan)
                        close_val = _safe_float(diag.get("close", entry_price), np.nan)
                        if np.isfinite(atr_val) and np.isfinite(close_val) and close_val > 0:
                            atr_pct = float(atr_val / close_val)
                    rsi_val = float(_safe_float(diag.get("rsi", 0.0), 0.0))
                    adx_val = float(_safe_float(diag.get("adx", 0.0), 0.0))
                    impulse_type = str(diag.get("impulse_type", ""))

                out_row = {
                    "signal_id": signal_id,
                    # V7 executors and dashboard treat signal_datetime as the
                    # executable handoff time. The source candle is retained
                    # separately in signal_bar_time_ist.
                    "signal_datetime": entry_time,
                    "received_time": received_time,
                    "detected_time_ist": received_time,
                    "logtime_ist": received_time,
                    "ticker": ticker,
                    "side": side_upper,
                    "setup": setup,
                    "impulse_type": impulse_type,
                    "entry_price": round(entry_price, 2),
                    "stop_price": round(stop_price, 2),
                    "target_price": round(target_price, 2),
                    "quality_score": round(float(_safe_float(row.get("score", 0.0), 0.0)), 4),
                    "atr_pct": round(atr_pct, 6),
                    "rsi": round(rsi_val, 2),
                    "adx": round(adx_val, 2),
                    "quantity": int(qty),
                    "signal_entry_datetime_ist": entry_time,
                    "signal_bar_time_ist": bar_time,
                }
                writer.writerow(out_row)
                existing_ids.add(signal_id)
                existing_keys.add(dedupe_key)
                run_keys.add(dedupe_key)
                run_tickers.add(ticker)
                written += 1

    print(
        f"[ID5MIN_V7 {side_upper} CSV] scanned={len(df_side)} written={written} "
        f"skipped_dup_key={skipped_duplicate_key} skipped_dup_id={skipped_duplicate_id} "
        f"skipped_intraday_ticker={skipped_intraday_ticker} "
        f"skipped_missing_time={skipped_missing_time} skipped_entry_window={skipped_entry_window} "
        f"skipped_stale_entry={skipped_stale_entry} skipped_future_entry={skipped_future_entry} "
        f"max_entry_to_detection_lag_sec={MAX_ENTRY_TO_DETECTION_LAG_SEC:g} "
        f"entry_window={ENTRY_WINDOW_START_RAW}-{ENTRY_WINDOW_END_RAW} path={csv_path}",
        flush=True,
    )
    return written


def _read_selected_day_state(signal_day_str: str) -> Tuple[set, int, Dict[str, int]]:
    """Read today's already-written (selected) signals from both side CSVs to
    rebuild the cross-slot selection state: tickers already taken, total count,
    and per-side counts. This is the live equivalent of the backtest's
    used_tickers / used_sides dicts in _apply_daily_selection."""
    used_tickers: set = set()
    side_counts: Dict[str, int] = {"LONG": 0, "SHORT": 0}
    total = 0
    for side in ("SHORT", "LONG"):
        path = _signal_csv_path(signal_day_str, side)
        if not path.exists():
            continue
        try:
            existing = pd.read_csv(path)
        except Exception:
            continue
        if existing.empty or "ticker" not in existing.columns:
            continue
        for _, row in existing.iterrows():
            tkr = str(row.get("ticker", "")).upper().strip()
            sd = str(row.get("side", side)).upper().strip()
            if not tkr:
                continue
            used_tickers.add(tkr)
            side_counts[sd] = side_counts.get(sd, 0) + 1
            total += 1
    return used_tickers, total, side_counts


def _select_executable(
    short_df: pd.DataFrame,
    long_df: pd.DataFrame,
    signal_day_str: str,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Turn the raw candidate firehose into an executable trade list, mirroring
    v2._apply_daily_selection:
      1. Best setup per (ticker, side, bar) by quality score.
      2. One trade per ticker per day (unless ALLOW_TICKER_REENTRY).
      3. Cap MAX_TRADES_PER_DAY total and MAX_SAME_SIDE_PER_DAY per side.
    Cross-slot state comes from today's CSVs (already-selected signals).
    """
    frames = [d for d in (short_df, long_df) if d is not None and not d.empty]
    if not frames:
        return short_df, long_df
    df = pd.concat(frames, ignore_index=True)
    if df.empty or "ticker" not in df.columns:
        return short_df, long_df

    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df["side"] = df["side"].astype(str).str.upper().str.strip()
    df["score"] = pd.to_numeric(df["score"], errors="coerce").fillna(0.0)

    # 1. Best setup per (ticker, side, bar): highest score wins.
    df = (
        df.sort_values("score", ascending=False)
        .drop_duplicates(subset=["ticker", "side", "bar_time_ist"], keep="first")
    )

    # 2/3. Walk best-first; apply per-ticker dedup + day/side caps using
    # cumulative state from today's CSVs.
    used_tickers, total, side_counts = _read_selected_day_state(signal_day_str)
    df = df.sort_values(["score", "bar_time_ist"], ascending=[False, True])

    keep_rows: List[Dict[str, Any]] = []
    batch_tickers: set = set()
    for _, row in df.iterrows():
        if total >= MAX_TRADES_PER_DAY:
            break
        tkr = str(row["ticker"]).upper().strip()
        side = str(row["side"]).upper().strip()
        if not ALLOW_TICKER_REENTRY and (tkr in used_tickers or tkr in batch_tickers):
            continue
        if side_counts.get(side, 0) >= MAX_SAME_SIDE_PER_DAY:
            continue
        keep_rows.append(row.to_dict())
        batch_tickers.add(tkr)
        side_counts[side] = side_counts.get(side, 0) + 1
        total += 1

    if not keep_rows:
        return pd.DataFrame(), pd.DataFrame()
    sel = pd.DataFrame(keep_rows)
    sel_short = sel[sel["side"] == "SHORT"].reset_index(drop=True)
    sel_long = sel[sel["side"] == "LONG"].reset_index(drop=True)
    return sel_short, sel_long


def _run_slot_scan(
    slot_ts: Any,
    *,
    runtime_root: Optional[Path | str],
    snapshot_workers: int = 0,
    scan_workers: int = 0,
    shard_count: int = 0,
    slot_bundle_root: Optional[Path | str] = None,
    prefer_slot_bundle: bool = False,
    write_live_csvs: bool = True,
) -> Dict[str, Any]:
    """Phase-1 swap: route the slot scan through avwap_5min_ID_v7_live_scan.

    The snapshot_workers/scan_workers/shard_count/slot_bundle_root/prefer_slot_bundle
    arguments are accepted for backward compatibility with the persistent
    scanner's main() loop and replay flows, but they are ignored — the v7
    adapter does its own hybrid data load (history + live) per ticker and
    runs avwap_5min_ID_v2's setup detection inline.
    """
    slot_ist = normalize_slot_ist(slot_ts)
    slot_runtime_root = Path(runtime_root) if runtime_root is not None else Path(RUNTIME_ROOT)
    slot_runtime_root.mkdir(parents=True, exist_ok=True)

    # Load v2's canonical universe. v2._load_universe() reads filtered_stocks_MIS
    # (now redirected to v2 in this repo) and applies v2's quarantine filter.
    try:
        universe = v7_live_scan.v2._load_universe()
    except Exception as exc:
        universe = []
        print(f"[ID5MIN_V7] universe load failed: {type(exc).__name__}: {exc}", flush=True)
    tickers = sorted({str(t).strip().upper() for t in universe if str(t).strip()})

    scan_started = time.perf_counter()
    try:
        market_ctx = v7_live_scan.build_market_context_once()
    except Exception as exc:
        market_ctx = {}
        print(f"[ID5MIN_V7] market_ctx load failed: {type(exc).__name__}: {exc}", flush=True)
    _t_after_ctx = time.perf_counter()

    try:
        short_df, long_df = v7_live_scan.scan_slot(slot_ist, tickers, market_ctx)
    except Exception as exc:
        short_df = pd.DataFrame()
        long_df = pd.DataFrame()
        print(f"[ID5MIN_V7] scan_slot failed: {type(exc).__name__}: {exc}", flush=True)
    scan_elapsed = time.perf_counter() - scan_started
    print(
        f"[ID5MIN_V7 timing] market_ctx={_t_after_ctx-scan_started:.3f}s "
        f"scan_slot={scan_elapsed-(_t_after_ctx-scan_started):.3f}s total_scan={scan_elapsed:.3f}s",
        flush=True,
    )

    signal_day_str = slot_ist.strftime("%Y-%m-%d")
    raw_short_rows = int(0 if short_df is None else len(short_df))
    raw_long_rows = int(0 if long_df is None else len(long_df))

    # Selection layer: collapse best-per-(ticker,bar) + per-ticker dedup + caps.
    if SELECTION_ENABLED:
        short_df, long_df = _select_executable(short_df, long_df, signal_day_str)

    if bool(write_live_csvs):
        short_written = _write_side_signals_csv(short_df, side="SHORT", signal_day_str=signal_day_str)
        long_written = _write_side_signals_csv(long_df, side="LONG", signal_day_str=signal_day_str)
    else:
        short_written = 0
        long_written = 0

    summary = {
        "slot_ist": slot_ist.strftime("%Y-%m-%d %H:%M:%S%z"),
        "slot_key": slot_key(slot_ist),
        "runtime_root": str(slot_runtime_root),
        "engine": "avwap_5min_ID_v7_live_scan",
        "universe_size": int(len(tickers)),
        "scan_elapsed_sec": round(scan_elapsed, 3),
        "selection_enabled": bool(SELECTION_ENABLED),
        "raw_short_candidates": raw_short_rows,
        "raw_long_candidates": raw_long_rows,
        "short_rows": int(0 if short_df is None else len(short_df)),
        "long_rows": int(0 if long_df is None else len(long_df)),
        "short_written": int(short_written),
        "long_written": int(long_written),
        "entry_window_start": ENTRY_WINDOW_START_RAW,
        "entry_window_end": ENTRY_WINDOW_END_RAW,
        "entry_signal_to_entry_lag_min": int(ENTRY_SIGNAL_TO_ENTRY_LAG_MIN),
        "max_entry_to_detection_lag_sec": float(MAX_ENTRY_TO_DETECTION_LAG_SEC),
        "legacy_live_csv_write_enabled": bool(LEGACY_LIVE_CSV_WRITE_ENABLED),
        "end_to_end_elapsed_sec": round(scan_elapsed, 3),
    }
    summary_path = _session_summary_dir(slot_runtime_root) / f"slot_summary_{slot_key(slot_ist)}.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    return summary


def _run_replay_slots(
    slots: List[str],
    *,
    runtime_root: Optional[Path | str],
    snapshot_workers: int,
    scan_workers: int,
    shard_count: int,
    slot_bundle_root: Optional[Path | str] = None,
    prefer_slot_bundle: bool = False,
    write_live_csvs: bool = False,
) -> Dict[str, Any]:
    clear_rolling_cache()
    summaries = []
    for slot in slots:
        slot_ist = normalize_slot_ist(slot)
        print(f"[REPLAY] slot={slot_ist.strftime('%Y-%m-%d %H:%M:%S%z')}", flush=True)
        summary = _run_slot_scan(
            slot_ist,
            runtime_root=runtime_root,
            snapshot_workers=int(snapshot_workers),
            scan_workers=int(scan_workers),
            shard_count=int(shard_count),
            slot_bundle_root=slot_bundle_root,
            prefer_slot_bundle=bool(prefer_slot_bundle),
            write_live_csvs=bool(write_live_csvs),
        )
        summaries.append(summary)
        print(json.dumps(summary, indent=2, sort_keys=True), flush=True)
    final_summary = {
        "runtime_root": str(Path(runtime_root) if runtime_root is not None else Path(RUNTIME_ROOT)),
        "slots": summaries,
    }
    out_path = _session_summary_dir(runtime_root) / "replay_summary_id_5min_v7_persistent.json"
    out_path.write_text(json.dumps(final_summary, indent=2, sort_keys=True), encoding="utf-8")
    return final_summary


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description=(
            "ID 5-min v7 persistent live scanner: 5-min slot cadence sibling of "
            "the v15_new persistent scanner."
        )
    )
    ap.add_argument("--runtime-root", default=str(RUNTIME_ROOT))
    ap.add_argument("--snapshot-workers", type=int, default=DEFAULT_SNAPSHOT_WORKERS)
    ap.add_argument("--scan-workers", type=int, default=DEFAULT_SCAN_WORKERS)
    ap.add_argument("--shard-count", type=int, default=DEFAULT_SHARD_COUNT)
    ap.add_argument("--replay-slots", nargs="*", default=None)
    ap.add_argument("--slot-bundle-root", default=None)
    ap.add_argument("--prefer-slot-bundle", action="store_true")
    return ap.parse_args()


def main() -> None:
    args = _parse_args()
    runtime_root = Path(args.runtime_root)
    runtime_root.mkdir(parents=True, exist_ok=True)

    if args.replay_slots:
        final_summary = _run_replay_slots(
            [str(v) for v in args.replay_slots],
            runtime_root=runtime_root,
            snapshot_workers=int(args.snapshot_workers),
            scan_workers=int(args.scan_workers),
            shard_count=int(args.shard_count),
            slot_bundle_root=args.slot_bundle_root,
            prefer_slot_bundle=bool(args.prefer_slot_bundle),
        )
        print(json.dumps(final_summary, indent=2, sort_keys=True), flush=True)
        return

    clear_rolling_cache()
    tickers = list(base_v15.list_tickers_15m())
    holidays = base_v15._read_holidays_safe()
    print("[LIVE] Live scanner v7 ID 5mins", flush=True)
    print(f"[INFO] runtime_root={runtime_root}", flush=True)
    print(
        f"[INFO] snapshot_workers={int(args.snapshot_workers)} scan_workers={int(args.scan_workers)} "
        f"shard_count={int(args.shard_count)} slot_minutes={SLOT_MINUTES} "
        f"legacy_live_csv_write_enabled={LEGACY_LIVE_CSV_WRITE_ENABLED} "
        f"max_entry_to_detection_lag_sec={MAX_ENTRY_TO_DETECTION_LAG_SEC:g}",
        flush=True,
    )

    while True:
        now = base_v15.now_ist()
        base_v15._touch_runtime_status("RUNNING", phase="LOOP_ID5MIN_V7")
        base_v15._touch_runtime_heartbeat("RUNNING", phase="LOOP_ID5MIN_V7")

        if now.time() >= HARD_STOP_TIME:
            base_v15._touch_runtime_status("STOPPED_AFTER_CUTOFF", phase="HARD_STOP_ID5MIN_V7")
            base_v15._touch_runtime_heartbeat("STOPPED", phase="HARD_STOP_ID5MIN_V7")
            print("[STOP] Hard-stop reached for today. Exiting.", flush=True)
            return

        if not base_v15.is_trading_day_safe(now.date(), holidays):
            nxt = base_v15._next_trading_day_start(now, holidays)
            clear_rolling_cache()
            print(f"[SKIP] Not a trading day ({now.date()}). Sleeping until {base_v15._fmt_ist_dt(nxt)}.", flush=True)
            base_v15._sleep_until(nxt)
            holidays = base_v15._read_holidays_safe()
            continue

        slot = _next_slot_after_id5min_v7(now)
        if slot.date() != now.date():
            clear_rolling_cache()
            print(f"[WAIT] Next slot is tomorrow {slot}. Sleeping.", flush=True)
            base_v15._sleep_until(slot)
            holidays = base_v15._read_holidays_safe()
            continue

        if now < slot:
            print(f"[WAIT] Sleeping until slot {slot.strftime('%Y-%m-%d %H:%M:%S%z')}", flush=True)
            base_v15._sleep_until(slot)

        now = base_v15.now_ist()
        if now.time() > END_TIME:
            nxt = base_v15._next_trading_day_start(now, holidays)
            clear_rolling_cache()
            print(f"[DONE] Past END_TIME. Sleeping until {base_v15._fmt_ist_dt(nxt)}.", flush=True)
            base_v15._sleep_until(nxt)
            holidays = base_v15._read_holidays_safe()
            continue

        # Fixed post-slot delay (no freshness gate). Wait POST_SLOT_DELAY_SEC for
        # the 5-min fetcher to write this slot's bar, then scan immediately.
        print(
            f"[WAIT] slot={slot.strftime('%H:%M')} fixed post-slot delay {POST_SLOT_DELAY_SEC}s",
            flush=True,
        )
        time.sleep(POST_SLOT_DELAY_SEC)

        slot_started = time.perf_counter()
        base_v15._touch_runtime_status("RUNNING", phase="SCAN_ID5MIN_V7", slot=slot.strftime("%H:%M"))
        base_v15._touch_runtime_heartbeat("RUNNING", phase="SCAN_ID5MIN_V7", slot=slot.strftime("%H:%M"))
        summary = _run_slot_scan(
            slot,
            runtime_root=runtime_root,
            snapshot_workers=int(args.snapshot_workers),
            scan_workers=int(args.scan_workers),
            shard_count=int(args.shard_count),
            write_live_csvs=LEGACY_LIVE_CSV_WRITE_ENABLED,
        )
        slot_elapsed = time.perf_counter() - slot_started
        base_v15._touch_runtime_status(
            "RUNNING",
            phase="SCAN_DONE_ID5MIN_V7",
            slot=slot.strftime("%H:%M"),
            elapsed_sec=f"{slot_elapsed:.3f}",
        )
        base_v15._touch_runtime_heartbeat(
            "RUNNING",
            phase="SCAN_DONE_ID5MIN_V7",
            slot=slot.strftime("%H:%M"),
            elapsed_sec=f"{slot_elapsed:.3f}",
        )
        print(json.dumps(summary, indent=2, sort_keys=True), flush=True)

        next_slot = slot + timedelta(minutes=SLOT_MINUTES)
        now_after = base_v15.now_ist()
        if now_after < next_slot:
            time.sleep(1.0)


if __name__ == "__main__":
    main()
