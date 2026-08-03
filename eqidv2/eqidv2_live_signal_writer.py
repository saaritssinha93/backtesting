"""
Neutral live-signal CSV writer for the v7 pipeline.

Only the entry engine (eqidv2_entry_engine_1min_v5_id.py) should call
write_side_signals() for production writes.  The retired persistent scanner
(eqidv2_live_combined_analyser_csv_id_5min_v7_persistent.py) must never
write to production CSVs; its old write path raises RuntimeError.

This module depends only on eqidv2_live_combined_analyser_csv_v15 (base_v15)
for the lock/schema/dedup helpers and on eqidv2_v7_signal_contract for
timing validation.  It intentionally does NOT import the retired scanner or
the entry engine, keeping the dependency graph acyclic.
"""

from __future__ import annotations

import csv
import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional, Set

import numpy as np
import pandas as pd

import eqidv2_live_combined_analyser_csv_v15 as base_v15
import eqidv2_v7_signal_contract as contract

SCHEMA_VERSION = "v7_signal_contract_2026_07_28_causal_v1"
PIPELINE_VERSION = "v7_live_1min_entry_atomic_slot_v1"

_WRITER_STARTED_AT_IST: str = base_v15.now_ist().strftime("%Y-%m-%d %H:%M:%S%z")


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------

@dataclass
class WriteResult:
    side: str
    written: int = 0
    skipped_duplicate_key: int = 0
    skipped_duplicate_id: int = 0
    skipped_intraday_ticker: int = 0
    skipped_missing_time: int = 0
    skipped_entry_window: int = 0
    skipped_stale_entry: int = 0
    skipped_future_entry: int = 0
    skipped_timing_invalid: int = 0
    written_candidate_ids: list = field(default_factory=list)
    errors: list = field(default_factory=list)

    def as_dict(self) -> Dict[str, Any]:
        return {
            f"write_{self.side.lower()}_{k}": v
            for k, v in self.__dict__.items()
            if k != "side"
        }


# ---------------------------------------------------------------------------
# Helpers (extracted from retired scanner — kept in sync with original logic)
# ---------------------------------------------------------------------------

def _parse_hhmm(value: str, default: str):
    raw = str(value or default).strip()
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            return datetime.strptime(raw, fmt).time()
        except ValueError:
            continue
    return datetime.strptime(default, "%H:%M").time()


def _entry_window_ok(
    entry_ts: datetime,
    window_start: str = "09:30",
    window_end: str = "14:00",
) -> bool:
    t = entry_ts.time() if hasattr(entry_ts, "time") else entry_ts
    return _parse_hhmm(window_start, "09:30") <= t <= _parse_hhmm(window_end, "14:00")


def _entry_time_from_row(
    row: pd.Series,
    bar_time_ts: datetime,
    entry_lag_min: int = contract.ENTRY_LAG_MIN,
) -> datetime:
    for key in ("entry_time_ist", "entry_ts", "entry_datetime_ist"):
        raw = row.get(key, "")
        parsed = base_v15._parse_ist_timestamp(str(raw)) if str(raw or "").strip() else None
        if parsed is not None:
            return parsed
    legacy_raw = row.get("signal_entry_datetime_ist", "")
    legacy_ts = (
        base_v15._parse_ist_timestamp(str(legacy_raw))
        if str(legacy_raw or "").strip()
        else None
    )
    if legacy_ts is not None:
        if abs((legacy_ts - bar_time_ts).total_seconds()) < 1:
            return bar_time_ts + timedelta(minutes=entry_lag_min)
        return legacy_ts
    return bar_time_ts + timedelta(minutes=entry_lag_min)


def _load_intraday_tickers(signal_day_str: str, live_signals_dir: Path) -> Set[str]:
    tickers: Set[str] = set()
    for side in ("SHORT", "LONG"):
        path = live_signals_dir / f"signals_{signal_day_str}_id_5min_v7_{side.lower()}.csv"
        if not path.exists() or path.stat().st_size <= 0:
            continue
        try:
            df = pd.read_csv(path, usecols=["ticker"])
            tickers.update(df["ticker"].dropna().astype(str).str.upper().str.strip())
        except Exception:
            pass
    return {t for t in tickers if t}


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        v = float(value)
        return v if np.isfinite(v) else default
    except Exception:
        return default


# ---------------------------------------------------------------------------
# Core writer
# ---------------------------------------------------------------------------

def write_side_signals(
    signals_df: pd.DataFrame,
    *,
    side: str,
    signal_day_str: str,
    live_signals_dir: Path,
    writer_name: str,
    writer_pid: Optional[int] = None,
    source_session: str = "",
    entry_lag_min: int = contract.ENTRY_LAG_MIN,
    max_lag_sec: float = contract.MAX_LAG_SEC,
    entry_window_start: str = "09:30",
    entry_window_end: str = "14:00",
    validate_timing: bool = True,
) -> WriteResult:
    """
    Append accepted signal rows to the daily live-signal CSV.

    Parameters
    ----------
    signals_df        : DataFrame of signal rows (from entry engine)
    side              : "SHORT" or "LONG"
    signal_day_str    : "YYYY-MM-DD"
    live_signals_dir  : Path to the live_signals/ directory
    writer_name       : identity tag written into every row ("entry_engine_1min_v5_id")
    writer_pid        : os.getpid() of the writing process
    source_session    : human-readable session name for audit
    validate_timing   : if True, reject rows that fail contract timing checks
    """
    side_upper = str(side).upper()
    result = WriteResult(side=side_upper)

    if signals_df is None or signals_df.empty:
        return result

    if "side" in signals_df.columns:
        df = signals_df.loc[signals_df["side"].astype(str).str.upper().eq(side_upper)].copy()
    else:
        df = signals_df.copy()
        df["side"] = side_upper

    if df.empty:
        return result

    csv_path = live_signals_dir / f"signals_{signal_day_str}_id_5min_v7_{side_upper.lower()}.csv"
    now_ts = base_v15.now_ist()
    now_str = now_ts.strftime("%Y-%m-%d %H:%M:%S%z")
    pid_str = str(writer_pid if writer_pid is not None else os.getpid())

    with base_v15._locked_signal_csv(str(csv_path)):
        base_v15._ensure_signal_csv_schema(str(csv_path))
        existing_ids = base_v15._load_existing_ids(str(csv_path))
        existing_keys = base_v15._load_existing_signal_keys(str(csv_path))
        existing_tickers = _load_intraday_tickers(signal_day_str, live_signals_dir)
        file_exists = csv_path.exists() and csv_path.stat().st_size > 0
        run_keys: Set[str] = set()
        run_tickers: Set[str] = set()

        with open(csv_path, "a", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(
                fh,
                fieldnames=base_v15.SIGNAL_CSV_COLUMNS,
                quoting=csv.QUOTE_ALL,
                extrasaction="ignore",
            )
            if not file_exists:
                writer.writeheader()

            for _, row in df.iterrows():
                ticker = str(row.get("ticker", "")).upper().strip()
                setup = str(row.get("setup", ""))
                bar_time_raw = row.get("bar_time_ist", "")
                bar_time_ts = base_v15._parse_ist_timestamp(str(bar_time_raw))
                if not ticker or bar_time_ts is None:
                    result.skipped_missing_time += 1
                    continue

                entry_ts = _entry_time_from_row(row, bar_time_ts, entry_lag_min)
                if not _entry_window_ok(entry_ts, entry_window_start, entry_window_end):
                    result.skipped_entry_window += 1
                    continue

                # Freshness — recompute with NOW so the lag reflects write time
                detected_now = base_v15.now_ist()
                lag_sec = (detected_now - entry_ts).total_seconds()
                if max_lag_sec > 0:
                    if lag_sec < 0:
                        result.skipped_future_entry += 1
                        continue
                    if lag_sec > max_lag_sec:
                        result.skipped_stale_entry += 1
                        continue

                if validate_timing:
                    timing = contract.build_signal_timing(
                        bar_time_ts,
                        detected_time=detected_now,
                        intended_entry_time=entry_ts,
                        entry_lag_min=entry_lag_min,
                        max_lag_sec=max_lag_sec,
                    )
                    ok, reason = contract.validate_signal_timing(timing, max_lag_sec=max_lag_sec)
                    if not ok:
                        result.skipped_timing_invalid += 1
                        continue

                if ticker in existing_tickers or ticker in run_tickers:
                    result.skipped_intraday_ticker += 1
                    continue

                bar_time_str = str(bar_time_ts)
                entry_time_str = str(entry_ts)
                dedupe_key = base_v15._signal_dedupe_key(ticker, side_upper, bar_time_str, setup)
                if dedupe_key in existing_keys or dedupe_key in run_keys:
                    result.skipped_duplicate_key += 1
                    continue

                signal_id = base_v15._generate_signal_id(ticker, side_upper, bar_time_str, setup)
                if signal_id in existing_ids:
                    result.skipped_duplicate_id += 1
                    continue

                entry_price = _safe_float(row.get("entry_price"), 0.0)
                stop_price = _safe_float(row.get("sl_price"), 0.0)
                target_price = _safe_float(row.get("target_price"), 0.0)
                quantity = int(row.get("quantity", 1)) if entry_price > 0 else 1

                diag_raw = row.get("diagnostics_json", "")
                diag: Dict[str, Any] = {}
                if isinstance(diag_raw, dict):
                    diag = dict(diag_raw)
                elif isinstance(diag_raw, str) and diag_raw.strip():
                    try:
                        diag = json.loads(diag_raw)
                    except Exception:
                        pass
                atr_pct = _safe_float(diag.get("atr_pct", row.get("atr_pct", 0.0)))
                rsi_val = _safe_float(diag.get("rsi", row.get("rsi", 0.0)))
                adx_val = _safe_float(diag.get("adx", row.get("adx", 0.0)))
                impulse_type = str(diag.get("impulse_type", row.get("impulse_type", "")))

                # Timing contract fields
                intended_entry_raw = row.get("intended_entry_ist", "")
                intended_entry_str = str(intended_entry_raw) if intended_entry_raw else entry_time_str
                deadline_ts = entry_ts + timedelta(seconds=max_lag_sec)

                out_row = {
                    "signal_id": signal_id,
                    "signal_datetime": entry_time_str,
                    "signal_bar_close_ist": bar_time_str,
                    "signal_price": _safe_float(row.get("signal_close"), 0.0),
                    "received_time": now_str,
                    "detected_time_ist": detected_now.strftime("%Y-%m-%d %H:%M:%S%z"),
                    "logtime_ist": now_str,
                    "ticker": ticker,
                    "side": side_upper,
                    "setup": setup,
                    "impulse_type": impulse_type,
                    "entry_price": round(entry_price, 2),
                    "stop_price": round(stop_price, 2),
                    "target_price": round(target_price, 2),
                    "quality_score": round(_safe_float(row.get("score") or row.get("quality_score")), 4),
                    "atr_pct": round(atr_pct, 6),
                    "rsi": round(rsi_val, 2),
                    "adx": round(adx_val, 2),
                    "quantity": quantity,
                    "signal_entry_datetime_ist": entry_time_str,
                    "signal_bar_time_ist": bar_time_str,
                    "stage2_detected_at_ist": str(row.get("stage2_detected_at_ist", "")),
                    "bar_closed_at_ist": str(row.get("bar_closed_at_ist", bar_time_str)),
                    "decision_ready_at_ist": str(row.get("decision_ready_at_ist", "")),
                    "entry_engine_ready_at_ist": str(
                        row.get("entry_engine_ready_at_ist", row.get("stage2_detected_at_ist", ""))
                    ),
                    "signal_written_at_ist": detected_now.strftime("%Y-%m-%d %H:%M:%S%z"),
                    "candidate_snapshot_sha256": str(row.get("candidate_snapshot_sha256", "")),
                    "runtime_manifest_path": str(row.get("runtime_manifest_path", "")),
                    # contract fields
                    "schema_version": SCHEMA_VERSION,
                    "pipeline_version": PIPELINE_VERSION,
                    "writer_name": str(writer_name),
                    "writer_pid": pid_str,
                    "writer_started_at_ist": _WRITER_STARTED_AT_IST,
                    "source_session": source_session,
                    "candidate_id": str(row.get("candidate_id", "")),
                    "intended_entry_ist": intended_entry_str,
                    "detection_lag_sec": f"{lag_sec:.1f}",
                    "deadline_ist": deadline_ts.strftime("%Y-%m-%d %H:%M:%S%z"),
                }
                writer.writerow(out_row)
                existing_ids.add(signal_id)
                existing_keys.add(dedupe_key)
                run_keys.add(dedupe_key)
                run_tickers.add(ticker)
                result.written += 1
                result.written_candidate_ids.append(str(row.get("candidate_id", "")))

    return result
