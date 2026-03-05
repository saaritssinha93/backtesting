# -*- coding: utf-8 -*-
"""
EQIDV2 LIVE Scanner V5 UNIFIED (single-pass SHORT+LONG)
======================================================

Purpose
-------
Run one scan pass per ticker and emit both V5 outputs:
  - SHORT direct/immediate CSV writes -> signals_YYYY-MM-DD_v5_short.csv
  - LONG anti-chase pending queue writes -> signals_YYYY-MM-DD_v5_long.csv

This avoids running v5_short and v5_long as separate full scans over the same
parquet universe, while preserving side-specific V5 output behavior.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

import eqidv2_live_combined_analyser_csv_v2 as v2


# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent

SHORT_SIGNAL_CSV_PATTERN = "signals_{}_v5_short.csv"
LONG_SIGNAL_CSV_PATTERN = "signals_{}_v5_long.csv"
UNIFIED_SIGNAL_CSV_PATTERN = "signals_{}_v5_unified.csv"

# LONG anti-chase behavior (same defaults as v5_long wrapper)
LONG_LIMIT_WAIT_MIN = int(os.getenv("EQIDV5_LONG_LIMIT_WAIT_MIN", "60"))
LONG_LIMIT_OFFSET_PCT = float(os.getenv("EQIDV5_LONG_LIMIT_OFFSET_PCT", "-0.005"))  # -0.5%
LONG_STOP_PCT = float(os.getenv("EQIDV5_LONG_STOP_PCT", "0.006"))                    # 0.6%
LONG_TARGET_PCT = float(os.getenv("EQIDV5_LONG_TARGET_PCT", "0.018"))                # 1.8%

LONG_RSI_CAP_RAW = os.getenv("EQIDV5_LONG_RSI_CAP", "").strip()
LONG_ADX_MIN_RAW = os.getenv("EQIDV5_LONG_ADX_MIN", "").strip()
LONG_QUALITY_MIN_RAW = os.getenv("EQIDV5_LONG_QUALITY_MIN", "").strip()

LONG_RSI_CAP: Optional[float] = float(LONG_RSI_CAP_RAW) if LONG_RSI_CAP_RAW else None
LONG_ADX_MIN: Optional[float] = float(LONG_ADX_MIN_RAW) if LONG_ADX_MIN_RAW else None
LONG_QUALITY_MIN: Optional[float] = float(LONG_QUALITY_MIN_RAW) if LONG_QUALITY_MIN_RAW else None


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_int(name: str, default: int, min_value: Optional[int] = None) -> int:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        val = int(default)
    else:
        try:
            val = int(str(raw).strip())
        except Exception:
            val = int(default)
    if min_value is not None:
        val = max(int(min_value), val)
    return val


STALE_ONLY_RETRY_ENABLED = _env_bool("EQIDV5_STALE_ONLY_RETRY", True)
LONG_PENDING_POLL_ENABLED = _env_bool("EQIDV5_LONG_PENDING_POLL_ENABLED", True)
LONG_PENDING_POLL_INTERVAL_SEC = max(
    1.0,
    float(os.getenv("EQIDV5_LONG_PENDING_POLL_INTERVAL_SEC", "5")),
)

EMBED_15M_FETCH_ENABLED = _env_bool("EQIDV5_UNIFIED_EMBED_15M_FETCH", True)
EMBED_15M_FETCH_MAX_WORKERS = _env_int(
    "EQIDV5_UNIFIED_15M_FETCH_MAX_WORKERS",
    _env_int("EQIDV2_15M_MAX_WORKERS", 24, min_value=1),
    min_value=1,
)
EMBED_15M_FETCH_BUFFER_SEC = _env_int(
    "EQIDV5_UNIFIED_15M_FETCH_BUFFER_SEC",
    _env_int("EQIDV2_15M_BUFFER_SEC", 6, min_value=0),
    min_value=0,
)
EMBED_15M_FETCH_REFRESH_TOKENS = _env_bool(
    "EQIDV5_UNIFIED_15M_FETCH_REFRESH_TOKENS",
    _env_bool("EQIDV2_15M_REFRESH_TOKENS", False),
)
EMBED_15M_FETCH_RESTART_DELAY_SEC = max(
    5.0,
    float(os.getenv("EQIDV5_UNIFIED_15M_FETCH_RESTART_DELAY_SEC", "20")),
)
EMBED_15M_FETCH_SCRIPT = ROOT / "eqidv2_eod_scheduler_for_15mins_data.py"

PENDING_STATE_FILE = ROOT / "logs" / "eqidv2_long_pending_state_v5_unified.json"
PENDING_STATE_WRITE_LOCK = threading.Lock()
PENDING_JSON_WRITE_RETRIES = 5
PENDING_JSON_WRITE_RETRY_BASE_SEC = 0.05

# Protect temporary mutation of v2 globals (SIGNAL_CSV_PATTERN / USE_KITE_LTP...)
_CSV_BRIDGE_LOCK = threading.RLock()


# -----------------------------------------------------------------------------
# Original handles
# -----------------------------------------------------------------------------
_ORIG_WRITE_SIGNALS_CSV = v2._write_signals_csv
_ORIG_RUN_ONE_SCAN = v2.run_one_scan
_ORIG_RUN_REPLAY_FOR_DATE = v2.run_replay_for_date
_ORIG_SCAN_SHORT_ONE_DAY = v2.scan_short_one_day
_ORIG_SCAN_LONG_ONE_DAY = v2.scan_long_one_day


# -----------------------------------------------------------------------------
# Utilities
# -----------------------------------------------------------------------------
def _safe_float(x: Any, default: float = np.nan) -> float:
    try:
        out = float(x)
        if np.isfinite(out):
            return out
        return float(default)
    except Exception:
        return float(default)


def _to_ist_ts(x: Any) -> Optional[pd.Timestamp]:
    try:
        ts = pd.to_datetime(x, errors="coerce")
        if pd.isna(ts):
            return None
        t = pd.Timestamp(ts)
        if t.tzinfo is None:
            return t.tz_localize(v2.IST)
        return t.tz_convert(v2.IST)
    except Exception:
        return None


def _row_signal_time_ist(row: Dict[str, Any]) -> Optional[pd.Timestamp]:
    for key in ("bar_time_ist", "signal_entry_datetime_ist", "signal_bar_time_ist", "signal_datetime"):
        if key in row:
            ts = _to_ist_ts(row.get(key))
            if ts is not None:
                return ts
    return None


def _atomic_write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, ensure_ascii=False, indent=2)

    with PENDING_STATE_WRITE_LOCK:
        for attempt in range(1, PENDING_JSON_WRITE_RETRIES + 1):
            fd = -1
            tmp_path = ""
            try:
                fd, tmp_path = tempfile.mkstemp(
                    prefix=f".{path.name}.",
                    suffix=".tmp",
                    dir=str(path.parent),
                )
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    fd = -1
                    f.write(text)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp_path, path)
                return
            except OSError as e:
                win_err = int(getattr(e, "winerror", 0) or 0)
                retryable = win_err in (5, 32)
                if (not retryable) or attempt >= PENDING_JSON_WRITE_RETRIES:
                    raise
                time.sleep(PENDING_JSON_WRITE_RETRY_BASE_SEC * attempt)
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


def _load_pending_state() -> Dict[str, Any]:
    if not PENDING_STATE_FILE.exists():
        return {"date": str(v2.now_ist().date()), "pending": {}}
    try:
        raw = json.loads(PENDING_STATE_FILE.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            return {"date": str(v2.now_ist().date()), "pending": {}}
        pending = raw.get("pending", {})
        if not isinstance(pending, dict):
            pending = {}
        return {"date": str(raw.get("date", "")), "pending": pending}
    except Exception:
        return {"date": str(v2.now_ist().date()), "pending": {}}


def _save_pending_state(state: Dict[str, Any]) -> None:
    _atomic_write_json(PENDING_STATE_FILE, state)


class _V2CsvContext:
    """
    Temporarily switch v2 CSV pattern and Kite rebase flag.
    Protected by _CSV_BRIDGE_LOCK at call sites.
    """

    def __init__(self, pattern: str, use_kite_ltp: bool):
        self.pattern = pattern
        self.use_kite_ltp = bool(use_kite_ltp)
        self._prev_pattern = None
        self._prev_use_kite = None

    def __enter__(self):
        self._prev_pattern = v2.SIGNAL_CSV_PATTERN
        self._prev_use_kite = bool(v2.USE_KITE_LTP_FOR_SIGNAL_CSV)
        v2.SIGNAL_CSV_PATTERN = self.pattern
        v2.USE_KITE_LTP_FOR_SIGNAL_CSV = self.use_kite_ltp
        return self

    def __exit__(self, exc_type, exc, tb):
        if self._prev_pattern is not None:
            v2.SIGNAL_CSV_PATTERN = self._prev_pattern
        if self._prev_use_kite is not None:
            v2.USE_KITE_LTP_FOR_SIGNAL_CSV = bool(self._prev_use_kite)
        return False


def _extract_stale_tickers(checks_df: pd.DataFrame) -> List[str]:
    if checks_df is None or checks_df.empty or "ticker" not in checks_df.columns:
        return []

    mask = pd.Series(False, index=checks_df.index)
    for col in ("stale_data", "no_target_day_data"):
        if col in checks_df.columns:
            mask = mask | checks_df[col].astype(str).str.strip().str.lower().isin(
                {"1", "true", "yes", "y", "on"}
            )

    if not bool(mask.any()):
        return []

    tickers = checks_df.loc[mask, "ticker"].astype(str).str.strip().str.upper()
    tickers = tickers[tickers != ""]
    return sorted(set(tickers.tolist()))


# -----------------------------------------------------------------------------
# LONG pending queue
# -----------------------------------------------------------------------------
def _to_pending_record(row: Dict[str, Any], now_ts: pd.Timestamp) -> Optional[Dict[str, Any]]:
    side = str(row.get("side", "")).upper().strip()
    if side != "LONG":
        return None

    signal_ts = _row_signal_time_ist(row)
    if signal_ts is None:
        return None

    ticker = str(row.get("ticker", "")).upper().strip()
    setup = str(row.get("setup", "")).strip()
    impulse = str(row.get("impulse_type", "")).strip()
    if not ticker:
        return None

    entry_signal = _safe_float(row.get("entry_price", np.nan))
    adx = _safe_float(row.get("adx", np.nan))
    rsi = _safe_float(row.get("rsi", np.nan))
    score = _safe_float(row.get("score", row.get("quality_score", np.nan)))
    atr_pct = _safe_float(row.get("atr_pct", np.nan))

    if not (np.isfinite(entry_signal) and entry_signal > 0):
        return None

    if LONG_RSI_CAP is not None and np.isfinite(rsi) and rsi > LONG_RSI_CAP:
        return None
    if LONG_ADX_MIN is not None and np.isfinite(adx) and adx < LONG_ADX_MIN:
        return None
    if LONG_QUALITY_MIN is not None and np.isfinite(score) and score < LONG_QUALITY_MIN:
        return None

    limit_price = round(entry_signal * (1.0 + float(LONG_LIMIT_OFFSET_PCT)), 2)
    stop_price = round(limit_price * (1.0 - float(LONG_STOP_PCT)), 2)
    target_price = round(limit_price * (1.0 + float(LONG_TARGET_PCT)), 2)
    expires_ts = signal_ts + timedelta(minutes=int(LONG_LIMIT_WAIT_MIN))

    return {
        "ticker": ticker,
        "side": "LONG",
        "setup": setup,
        "impulse_type": impulse,
        "signal_time_ist": signal_ts.isoformat(),
        "created_time_ist": now_ts.isoformat(),
        "expires_time_ist": expires_ts.isoformat(),
        "signal_entry_price": float(entry_signal),
        "limit_price": float(limit_price),
        "stop_price": float(stop_price),
        "target_price": float(target_price),
        "score": float(score) if np.isfinite(score) else 0.0,
        "adx": float(adx) if np.isfinite(adx) else 0.0,
        "rsi": float(rsi) if np.isfinite(rsi) else 0.0,
        "atr_pct": float(atr_pct) if np.isfinite(atr_pct) else 0.0,
    }


def _pending_to_signal_row(p: Dict[str, Any]) -> Dict[str, Any]:
    bar_time = str(p.get("signal_time_ist", ""))
    diag = {
        "impulse_type": str(p.get("impulse_type", "")),
        "adx": float(p.get("adx", 0.0)),
        "rsi": float(p.get("rsi", 0.0)),
        "atr_pct": float(p.get("atr_pct", 0.0)),
    }
    return {
        "ticker": str(p.get("ticker", "")).upper(),
        "side": "LONG",
        "bar_time_ist": bar_time,
        "setup": str(p.get("setup", "")),
        "impulse_type": str(p.get("impulse_type", "")),
        "entry_price": float(p.get("limit_price", 0.0)),
        "sl_price": float(p.get("stop_price", 0.0)),
        "target_price": float(p.get("target_price", 0.0)),
        "score": float(p.get("score", 0.0)),
        "adx": float(p.get("adx", 0.0)),
        "rsi": float(p.get("rsi", 0.0)),
        "atr_pct": float(p.get("atr_pct", 0.0)),
        "diagnostics_json": json.dumps(diag, default=str),
    }


def _write_short_rows(df_short: pd.DataFrame) -> int:
    if df_short is None or df_short.empty:
        return 0
    with _V2CsvContext(SHORT_SIGNAL_CSV_PATTERN, use_kite_ltp=True):
        return int(_ORIG_WRITE_SIGNALS_CSV(df_short))


def _write_long_rows_no_rebase(df_long_filled: pd.DataFrame) -> int:
    if df_long_filled is None or df_long_filled.empty:
        return 0
    with _V2CsvContext(LONG_SIGNAL_CSV_PATTERN, use_kite_ltp=False):
        return int(_ORIG_WRITE_SIGNALS_CSV(df_long_filled))


def _process_long_pending(
    signals_df: Optional[pd.DataFrame],
    *,
    log_label: str,
    log_noop: bool,
) -> int:
    now_ts = pd.Timestamp(v2.now_ist())
    today_str = str(now_ts.date())

    state = _load_pending_state()
    if state.get("date") != today_str:
        state = {"date": today_str, "pending": {}}
    pending: Dict[str, Dict[str, Any]] = dict(state.get("pending", {}))

    scanned_total = 0 if signals_df is None else int(len(signals_df))
    long_scanned = 0
    added = 0
    expired = 0
    filled = 0

    # Queue new LONG candidates
    if signals_df is not None and (not signals_df.empty):
        for _, row in signals_df.iterrows():
            payload = dict(row)
            side = str(payload.get("side", "")).upper().strip()
            if side != "LONG":
                continue
            long_scanned += 1

            signal_ts = _row_signal_time_ist(payload)
            if signal_ts is None or signal_ts.date() != now_ts.date():
                continue

            ticker = str(payload.get("ticker", "")).upper().strip()
            setup = str(payload.get("setup", "")).strip()
            key = v2._signal_dedupe_key(ticker, "LONG", str(signal_ts), setup)
            if key in pending:
                continue

            rec = _to_pending_record(payload, now_ts)
            if rec is None:
                continue

            pending[key] = rec
            added += 1

    # Trim expired
    still_pending: Dict[str, Dict[str, Any]] = {}
    active_tickers: List[str] = []
    for key, rec in pending.items():
        exp_ts = _to_ist_ts(rec.get("expires_time_ist"))
        if exp_ts is None or now_ts > exp_ts:
            expired += 1
            continue
        still_pending[key] = rec
        t = str(rec.get("ticker", "")).upper().strip()
        if t:
            active_tickers.append(t)

    # Fill when live LTP <= limit
    ltp_map = v2._fetch_kite_ltp_map(sorted(set(active_tickers))) if active_tickers else {}
    next_pending: Dict[str, Dict[str, Any]] = {}
    long_rows: List[Dict[str, Any]] = []

    for key, rec in still_pending.items():
        ticker = str(rec.get("ticker", "")).upper().strip()
        limit_price = _safe_float(rec.get("limit_price", np.nan))
        ltp = _safe_float(ltp_map.get(ticker, np.nan))
        if np.isfinite(ltp) and ltp > 0 and np.isfinite(limit_price) and ltp <= limit_price:
            long_rows.append(_pending_to_signal_row(rec))
            filled += 1
            continue
        next_pending[key] = rec

    state = {"date": today_str, "pending": next_pending}
    _save_pending_state(state)

    long_written = 0
    if long_rows:
        long_written = _write_long_rows_no_rebase(pd.DataFrame(long_rows))

    should_log = (
        bool(log_noop)
        or scanned_total > 0
        or long_scanned > 0
        or added > 0
        or filled > 0
        or expired > 0
        or long_written > 0
    )
    if should_log:
        print(
            f"{log_label} scanned={scanned_total} | long_scanned={long_scanned} "
            f"| pending_added={added} | pending_filled={filled} "
            f"| pending_expired={expired} | pending_open={len(next_pending)} "
            f"| long_written={long_written}",
            flush=True,
        )
    return int(long_written)


def _write_signals_csv_v5_unified(signals_df: pd.DataFrame) -> int:
    with _CSV_BRIDGE_LOCK:
        scanned = 0 if signals_df is None else int(len(signals_df))
        if signals_df is None or signals_df.empty:
            long_written = _process_long_pending(
                pd.DataFrame(),
                log_label="[V5_UNIFIED LONG]",
                log_noop=False,
            )
            print(
                f"[V5_UNIFIED CSV] scanned=0 | short_rows=0 | short_written=0 | "
                f"long_rows=0 | long_written={long_written}",
                flush=True,
            )
            return int(long_written)

        if "side" in signals_df.columns:
            side_upper = signals_df["side"].astype(str).str.upper()
            df_short = signals_df.loc[side_upper.eq("SHORT")].copy()
            df_long = signals_df.loc[side_upper.eq("LONG")].copy()
        else:
            df_short = pd.DataFrame()
            df_long = pd.DataFrame()

        short_written = _write_short_rows(df_short)
        long_written = _process_long_pending(
            df_long,
            log_label="[V5_UNIFIED LONG]",
            log_noop=True,
        )

        print(
            f"[V5_UNIFIED CSV] scanned={scanned} | short_rows={len(df_short)} "
            f"| short_written={short_written} | long_rows={len(df_long)} "
            f"| long_written={long_written}",
            flush=True,
        )
        return int(short_written + long_written)


def _pending_poll_once() -> int:
    with _CSV_BRIDGE_LOCK:
        return _process_long_pending(
            pd.DataFrame(),
            log_label="[V5_UNIFIED PENDING]",
            log_noop=False,
        )


def _start_pending_poll_worker() -> Optional[threading.Event]:
    if not LONG_PENDING_POLL_ENABLED:
        return None

    stop_event = threading.Event()

    def _worker() -> None:
        print(
            f"[V5_UNIFIED PENDING] worker_started | interval={LONG_PENDING_POLL_INTERVAL_SEC:.1f}s",
            flush=True,
        )
        while not stop_event.is_set():
            now = pd.Timestamp(v2.now_ist())
            in_poll_window = v2.SESSION_START <= now.time() <= v2.END_TIME
            if in_poll_window:
                try:
                    _pending_poll_once()
                except Exception as exc:
                    print(f"[V5_UNIFIED PENDING] poll_error: {exc}", flush=True)
                stop_event.wait(float(LONG_PENDING_POLL_INTERVAL_SEC))
            else:
                stop_event.wait(30.0)

    t = threading.Thread(
        target=_worker,
        name="eqidv2-v5-unified-pending-poller",
        daemon=True,
    )
    t.start()
    return stop_event


class _Embedded15mFetcherSupervisor:
    def __init__(self) -> None:
        self._stop_event = threading.Event()
        self._lock = threading.Lock()
        self._proc: Optional[subprocess.Popen] = None
        self._thread: Optional[threading.Thread] = None
        self._restart_count = 0

    def _build_cmd(self) -> List[str]:
        cmd = [
            sys.executable,
            "-u",
            str(EMBED_15M_FETCH_SCRIPT),
            "--max-workers",
            str(int(EMBED_15M_FETCH_MAX_WORKERS)),
            "--buffer-sec",
            str(int(EMBED_15M_FETCH_BUFFER_SEC)),
        ]
        if EMBED_15M_FETCH_REFRESH_TOKENS:
            cmd.append("--refresh-tokens")
        else:
            cmd.append("--no-refresh-tokens")
        return cmd

    def _launch_locked(self) -> bool:
        cmd = self._build_cmd()
        try:
            proc = subprocess.Popen(
                cmd,
                cwd=str(ROOT),
            )
            self._proc = proc
            print(
                f"[V5_UNIFIED 15M] started pid={proc.pid} | workers={EMBED_15M_FETCH_MAX_WORKERS} "
                f"| buffer={EMBED_15M_FETCH_BUFFER_SEC}s | refresh_tokens={EMBED_15M_FETCH_REFRESH_TOKENS}",
                flush=True,
            )
            return True
        except Exception as exc:
            self._proc = None
            print(f"[V5_UNIFIED 15M] launch_failed: {exc}", flush=True)
            return False

    def _watchdog(self) -> None:
        while not self._stop_event.is_set():
            with self._lock:
                proc = self._proc
                if proc is None:
                    launched = self._launch_locked()
                    if not launched:
                        wait_sec = float(EMBED_15M_FETCH_RESTART_DELAY_SEC)
                        self._stop_event.wait(wait_sec)
                        continue
                    proc = self._proc

            if proc is None:
                self._stop_event.wait(1.0)
                continue

            rc = proc.poll()
            if rc is None:
                self._stop_event.wait(5.0)
                continue

            if self._stop_event.is_set():
                break

            self._restart_count += 1
            print(
                f"[V5_UNIFIED 15M] exited rc={rc} | restarting in "
                f"{EMBED_15M_FETCH_RESTART_DELAY_SEC:.0f}s (attempt={self._restart_count})",
                flush=True,
            )
            with self._lock:
                self._proc = None
            self._stop_event.wait(float(EMBED_15M_FETCH_RESTART_DELAY_SEC))

    def start(self) -> bool:
        if not EMBED_15M_FETCH_SCRIPT.exists():
            print(
                f"[V5_UNIFIED 15M] disabled: missing script {EMBED_15M_FETCH_SCRIPT}",
                flush=True,
            )
            return False

        self._thread = threading.Thread(
            target=self._watchdog,
            name="eqidv2-v5-unified-15m-fetcher",
            daemon=True,
        )
        self._thread.start()
        return True

    def stop(self) -> None:
        self._stop_event.set()

        proc: Optional[subprocess.Popen] = None
        with self._lock:
            proc = self._proc
            self._proc = None

        if proc is not None and proc.poll() is None:
            try:
                proc.terminate()
                proc.wait(timeout=15)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass

        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=5)


# -----------------------------------------------------------------------------
# Overrides
# -----------------------------------------------------------------------------
def _apply_v5_unified_overrides() -> None:
    v2.REPORTS_DIR = ROOT / "reports" / "eqidv2_reports_v5_unified"
    v2.REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    v2.OUT_CHECKS_DIR = ROOT / "out_eqidv2_live_checks_15m_v5_unified"
    v2.OUT_SIGNALS_DIR = ROOT / "out_eqidv2_live_signals_15m_v5_unified"
    v2.OUT_CHECKS_DIR.mkdir(parents=True, exist_ok=True)
    v2.OUT_SIGNALS_DIR.mkdir(parents=True, exist_ok=True)

    v2.STATE_FILE = ROOT / "logs" / "eqidv2_avwap_live_state_v11_v5_unified.json"
    v2.SIGNAL_CSV_PATTERN = UNIFIED_SIGNAL_CSV_PATTERN

    # Keep LONG session horizon since unified process owns LONG pending queue too.
    v2.END_TIME = v2.dtime(15, 0)
    v2.SESSION_END = v2.dtime(15, 0, 0)

    # Enable per-ticker flush so SHORT rows are published immediately.
    v2.IMMEDIATE_SIGNAL_CSV_FLUSH = True
    v2.USE_KITE_LTP_FOR_SIGNAL_CSV = True

    # Explicitly keep both side strategy scanners active.
    v2.scan_short_one_day = _ORIG_SCAN_SHORT_ONE_DAY
    v2.scan_long_one_day = _ORIG_SCAN_LONG_ONE_DAY

    # Install unified CSV bridge.
    v2._write_signals_csv = _write_signals_csv_v5_unified

    # Keep stale-only retry behavior, but do it once for unified scan.
    def _run_one_scan_v5_unified(run_tag: str = "A"):
        checks_df, signals_df = _ORIG_RUN_ONE_SCAN(run_tag)

        def _rename_latest(folder: Path, prefix: str, tag: str) -> None:
            candidates = sorted(
                folder.glob(f"{prefix}_*_{tag}.parquet"),
                key=lambda p: p.stat().st_mtime,
            )
            if not candidates:
                return
            src = candidates[-1]
            if src.stem.endswith("_v5_unified"):
                return
            dst = src.with_name(src.stem + "_v5_unified" + src.suffix)
            try:
                if dst.exists():
                    dst.unlink()
                src.rename(dst)
            except Exception:
                pass

        def _rename_for_tag(tag: str) -> None:
            day_dir = datetime.now(v2.IST).strftime("%Y%m%d")
            _rename_latest(v2.OUT_CHECKS_DIR / day_dir, "checks", tag)
            _rename_latest(v2.OUT_SIGNALS_DIR / day_dir, "signals", tag)

        _rename_for_tag(run_tag)

        if STALE_ONLY_RETRY_ENABLED:
            stale_tickers = _extract_stale_tickers(checks_df)
            if stale_tickers:
                retry_tag = f"{run_tag}R"
                print(
                    f"[V5_UNIFIED RETRY] stale_tickers={len(stale_tickers)} | "
                    f"rerun_subset_tag={retry_tag}",
                    flush=True,
                )
                print(
                    f"[V5_UNIFIED RETRY] stale_ticker_names={','.join(stale_tickers)}",
                    flush=True,
                )
                orig_list_tickers = v2.list_tickers_15m
                try:
                    v2.list_tickers_15m = lambda: stale_tickers
                    checks_retry, signals_retry = _ORIG_RUN_ONE_SCAN(retry_tag)
                finally:
                    v2.list_tickers_15m = orig_list_tickers

                _rename_for_tag(retry_tag)
                if checks_retry is not None and (not checks_retry.empty):
                    checks_df = pd.concat([checks_df, checks_retry], ignore_index=True)
                if signals_retry is not None and (not signals_retry.empty):
                    signals_df = pd.concat([signals_df, signals_retry], ignore_index=True)
                print(
                    f"[V5_UNIFIED RETRY] done | "
                    f"extra_checks={0 if checks_retry is None else len(checks_retry)} "
                    f"| extra_signals={0 if signals_retry is None else len(signals_retry)}",
                    flush=True,
                )

        return checks_df, signals_df

    v2.run_one_scan = _run_one_scan_v5_unified

    def _run_replay_for_date_v5_unified(date_str: str, out_csv: Optional[str] = None) -> pd.DataFrame:
        if out_csv is None:
            out_csv = str(v2.OUT_SIGNALS_DIR / f"replay_signals_{date_str}_v5_unified.csv")
        return _ORIG_RUN_REPLAY_FOR_DATE(date_str, out_csv=out_csv)

    v2.run_replay_for_date = _run_replay_for_date_v5_unified


def main() -> None:
    _apply_v5_unified_overrides()

    argv_lower = {str(a).strip().lower() for a in sys.argv[1:]}
    is_help_mode = ("-h" in argv_lower) or ("--help" in argv_lower)
    is_replay_mode = ("--replay-date" in argv_lower)
    should_start_pending_worker = (
        LONG_PENDING_POLL_ENABLED
        and (not is_help_mode)
        and (not is_replay_mode)
    )
    should_start_embedded_fetcher = (
        EMBED_15M_FETCH_ENABLED
        and (not is_help_mode)
        and (not is_replay_mode)
    )

    if not is_help_mode:
        print(
            "[V5_UNIFIED] single-pass short+long enabled | "
            "short_csv=signals_YYYY-MM-DD_v5_short.csv | "
            "long_csv=signals_YYYY-MM-DD_v5_long.csv",
            flush=True,
        )
        print(
            "[V5_UNIFIED] long_anti_chase="
            f"limit_wait={LONG_LIMIT_WAIT_MIN}m "
            f"limit_offset={LONG_LIMIT_OFFSET_PCT*100:.2f}% "
            f"sl={LONG_STOP_PCT*100:.2f}% "
            f"tgt={LONG_TARGET_PCT*100:.2f}% "
            f"| stale_only_retry={STALE_ONLY_RETRY_ENABLED} "
            f"| end_time={v2.END_TIME.strftime('%H:%M:%S')}",
            flush=True,
        )
        print(
            "[V5_UNIFIED] pending_poll="
            f"{LONG_PENDING_POLL_ENABLED} "
            f"(interval={LONG_PENDING_POLL_INTERVAL_SEC:.1f}s)"
            f" | worker_active={should_start_pending_worker}",
            flush=True,
        )
        print(
            "[V5_UNIFIED] embedded_15m_fetch="
            f"{EMBED_15M_FETCH_ENABLED} "
            f"(workers={EMBED_15M_FETCH_MAX_WORKERS}, "
            f"buffer={EMBED_15M_FETCH_BUFFER_SEC}s, "
            f"refresh_tokens={EMBED_15M_FETCH_REFRESH_TOKENS}) "
            f"| worker_active={should_start_embedded_fetcher}",
            flush=True,
        )
        if should_start_embedded_fetcher:
            print(
                "[V5_UNIFIED] NOTE: disable separate task "
                "EQIDV2_eod_15mins_data_0900 to avoid duplicate 15m fetch updates.",
                flush=True,
            )
        if LONG_RSI_CAP is not None:
            print(f"[V5_UNIFIED] LONG_RSI_CAP={LONG_RSI_CAP}", flush=True)
        if LONG_ADX_MIN is not None:
            print(f"[V5_UNIFIED] LONG_ADX_MIN={LONG_ADX_MIN}", flush=True)
        if LONG_QUALITY_MIN is not None:
            print(f"[V5_UNIFIED] LONG_QUALITY_MIN={LONG_QUALITY_MIN}", flush=True)

    poll_stop: Optional[threading.Event] = None
    fetch_supervisor: Optional[_Embedded15mFetcherSupervisor] = None
    if should_start_pending_worker:
        poll_stop = _start_pending_poll_worker()
    elif (not is_help_mode) and LONG_PENDING_POLL_ENABLED and is_replay_mode:
        print("[V5_UNIFIED] pending worker skipped in replay mode.", flush=True)

    if should_start_embedded_fetcher:
        fetch_supervisor = _Embedded15mFetcherSupervisor()
        if not fetch_supervisor.start():
            fetch_supervisor = None
    elif (not is_help_mode) and EMBED_15M_FETCH_ENABLED and is_replay_mode:
        print("[V5_UNIFIED] embedded 15m fetch skipped in replay mode.", flush=True)

    try:
        v2.main()
    finally:
        if poll_stop is not None:
            poll_stop.set()
        if fetch_supervisor is not None:
            fetch_supervisor.stop()


if __name__ == "__main__":
    main()
