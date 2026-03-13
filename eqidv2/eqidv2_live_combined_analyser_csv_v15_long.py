# -*- coding: utf-8 -*-
# Backup reference (2026-02-26):
# - c:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\backups_codex\20260226_180142\eqidv2_eod_scheduler_for_15mins_data.py
# - c:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\backups_codex\20260226_180142\run_eqidv2_eod_scheduler_for_15mins_data.bat
"""
EQIDV2 LIVE Scanner V15 LONG (long-only anti-chase split pipeline)
==================================================================

This file is a non-destructive wrapper over:
    eqidv2_live_combined_analyser_csv_v2.py

Goals:
1. Keep existing v2 files untouched.
2. Emit only LONG signals.
3. Keep anti-chase LONG pending-entry behavior from v3.
4. Isolate all outputs/state with `v15_long` suffix.
"""

from __future__ import annotations

import json
import os
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
import avwap_combined_runner_v15 as v15_runner
from avwap_v11_refactored.avwap_common_v11_v14 import (
    default_short_config as v15_default_short_config,
)
from avwap_v11_refactored.avwap_common_v7_sweep_v14 import (
    default_long_config as v15_default_long_config,
    in_session as v15_in_session,
    prepare_indicators as v15_prepare_indicators,
    compute_day_avwap as v15_compute_day_avwap,
)
from avwap_v11_refactored.avwap_short_strategy_v11 import (
    scan_one_day as v15_scan_short_one_day,
)
from avwap_v11_refactored.avwap_long_strategy_v9_sweep import (
    scan_one_day as v15_scan_long_one_day,
)


# -----------------------------------------------------------------------------
# V15 LONG ANTI-CHASE CONFIG
# -----------------------------------------------------------------------------
LONG_LIMIT_WAIT_MIN = int(os.getenv("EQIDV15_LONG_LIMIT_WAIT_MIN", "60"))
LONG_LIMIT_OFFSET_PCT = float(os.getenv("EQIDV15_LONG_LIMIT_OFFSET_PCT", "-0.005"))  # -0.5%
LONG_STOP_PCT = float(os.getenv("EQIDV15_LONG_STOP_PCT", "0.0075"))                   # 0.75%
LONG_TARGET_PCT = float(os.getenv("EQIDV15_LONG_TARGET_PCT", "0.011"))                # 1.1%

# Optional signal-quality filters before putting LONG into pending queue.
LONG_RSI_CAP_RAW = os.getenv("EQIDV15_LONG_RSI_CAP", "").strip()
LONG_ADX_MIN_RAW = os.getenv("EQIDV15_LONG_ADX_MIN", "").strip()
LONG_QUALITY_MIN_RAW = os.getenv("EQIDV15_LONG_QUALITY_MIN", "").strip()

LONG_RSI_CAP: Optional[float] = float(LONG_RSI_CAP_RAW) if LONG_RSI_CAP_RAW else None
LONG_ADX_MIN: Optional[float] = float(LONG_ADX_MIN_RAW) if LONG_ADX_MIN_RAW else None
LONG_QUALITY_MIN: Optional[float] = float(LONG_QUALITY_MIN_RAW) if LONG_QUALITY_MIN_RAW else None


ROOT = Path(__file__).resolve().parent
PENDING_STATE_FILE = ROOT / "logs" / "eqidv2_long_pending_state_v15_long.json"
PENDING_STATE_WRITE_LOCK = threading.Lock()
PENDING_JSON_WRITE_RETRIES = 5
PENDING_JSON_WRITE_RETRY_BASE_SEC = 0.05


# Keep original functions so wrapper can delegate safely.
_ORIG_WRITE_SIGNALS_CSV = v2._write_signals_csv
_ORIG_LATEST_ENTRY_SIGNALS_FOR_TICKER = v2._latest_entry_signals_for_ticker
_ORIG_RUN_ONE_SCAN = v2.run_one_scan
_ORIG_RUN_REPLAY_FOR_DATE = v2.run_replay_for_date


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


STALE_ONLY_RETRY_ENABLED = _env_bool("EQIDV15_STALE_ONLY_RETRY", True)
LONG_PENDING_POLL_ENABLED = _env_bool("EQIDV15_LONG_PENDING_POLL_ENABLED", True)
LONG_PENDING_POLL_INTERVAL_SEC = max(
    1.0,
    float(os.getenv("EQIDV15_LONG_PENDING_POLL_INTERVAL_SEC", "5")),
)

_CSV_BRIDGE_LOCK = threading.RLock()
_NIFTY_CONTEXT_MODE_MAP: Dict[str, str] = {}
_NIFTY_CONTEXT_RET_MAP: Dict[str, float] = {}
_NIFTY_CONTEXT_SOURCE = ""
_NIFTY_CONTEXT_CFG = None
_NIFTY_STOCK_RET_CACHE: Dict[str, Dict[str, float]] = {}


def _refresh_v15_nifty_context() -> None:
    global _NIFTY_CONTEXT_MODE_MAP, _NIFTY_CONTEXT_RET_MAP, _NIFTY_CONTEXT_SOURCE
    global _NIFTY_CONTEXT_CFG, _NIFTY_STOCK_RET_CACHE

    if not bool(getattr(v15_runner, "NIFTY_CONTEXT_ENABLED", False)):
        _NIFTY_CONTEXT_MODE_MAP = {}
        _NIFTY_CONTEXT_RET_MAP = {}
        _NIFTY_CONTEXT_SOURCE = ""
        _NIFTY_CONTEXT_CFG = None
        _NIFTY_STOCK_RET_CACHE = {}
        return

    cfg = v15_default_short_config()
    cfg.dir_15m = str(v15_runner._resolve_15m_dir())
    mode_map, ret_map, src, _counts = v15_runner._build_nifty_intraday_context(cfg)
    _NIFTY_CONTEXT_MODE_MAP = dict(mode_map or {})
    _NIFTY_CONTEXT_RET_MAP = dict(ret_map or {})
    _NIFTY_CONTEXT_SOURCE = str(src or "")
    _NIFTY_CONTEXT_CFG = cfg
    _NIFTY_STOCK_RET_CACHE = {}


def _passes_v15_nifty_context(signal: Any) -> bool:
    if not _NIFTY_CONTEXT_MODE_MAP or _NIFTY_CONTEXT_CFG is None:
        return True

    ts = pd.to_datetime(getattr(signal, "bar_time_ist", None), errors="coerce")
    if pd.isna(ts):
        return True
    ts = pd.Timestamp(ts)
    if ts.tzinfo is None:
        ts = ts.tz_localize(v15_runner.IST)
    else:
        ts = ts.tz_convert(v15_runner.IST)

    ts_key = v15_runner._ts_to_key_local(ts)
    mode = str(_NIFTY_CONTEXT_MODE_MAP.get(ts_key, "BOTH")).upper()
    if mode == "SHORT_ONLY":
        return False

    if not bool(getattr(v15_runner, "NIFTY_RS_FILTER_ENABLED", False)):
        return True

    if mode != "BOTH":
        rs_thresh = float(v15_runner.NIFTY_RS_THRESHOLD_PCT)
        apply_rs = True
    elif bool(getattr(v15_runner, "NIFTY_RS_BOTH_MODE_ENABLED", False)):
        rs_thresh = float(v15_runner.NIFTY_RS_BOTH_MODE_THRESHOLD_PCT)
        apply_rs = True
    else:
        rs_thresh = 0.0
        apply_rs = False

    if not apply_rs:
        return True

    stock_ret_map = v15_runner._build_stock_return_map(
        getattr(signal, "ticker", ""),
        _NIFTY_CONTEXT_CFG,
        _NIFTY_STOCK_RET_CACHE,
    )
    stock_ret = stock_ret_map.get(ts_key, np.nan)
    nifty_ret = _NIFTY_CONTEXT_RET_MAP.get(ts_key, np.nan)
    if not (np.isfinite(stock_ret) and np.isfinite(nifty_ret)):
        return True
    rel_val = float(stock_ret - nifty_ret)
    return rel_val >= rs_thresh


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


def _atomic_write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, ensure_ascii=False, indent=2)

    # Serialize local writes and use unique temp files to avoid WinError 32
    # when scan + poll threads persist state concurrently.
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


def _disable_kite_rebase_write(signals_df: pd.DataFrame) -> int:
    """
    Write rows through original v2 CSV appender but with Kite rebase temporarily disabled.
    We already finalize entry/SL/TGT in v15_long pending-fill logic.
    """
    old = bool(v2.USE_KITE_LTP_FOR_SIGNAL_CSV)
    try:
        v2.USE_KITE_LTP_FOR_SIGNAL_CSV = False
        return int(_ORIG_WRITE_SIGNALS_CSV(signals_df))
    finally:
        v2.USE_KITE_LTP_FOR_SIGNAL_CSV = old


def _row_signal_time_ist(row: Dict[str, Any]) -> Optional[pd.Timestamp]:
    for key in ("bar_time_ist", "signal_entry_datetime_ist", "signal_bar_time_ist", "signal_datetime"):
        if key in row:
            ts = _to_ist_ts(row.get(key))
            if ts is not None:
                return ts
    return None


def _latest_entry_signals_for_ticker_v15_long(
    ticker: str,
    df_raw: pd.DataFrame,
    state: Dict[str, Any],
    target_slot_ist: pd.Timestamp,
):
    """
    Delegate to base detector, then keep only LONG rows/checks.
    """
    signals, checks = _ORIG_LATEST_ENTRY_SIGNALS_FOR_TICKER(
        ticker=ticker,
        df_raw=df_raw,
        state=state,
        target_slot_ist=target_slot_ist,
    )
    signals_long = [
        s for s in (signals or [])
        if str(getattr(s, "side", "")).upper().strip() == "LONG" and _passes_v15_nifty_context(s)
    ]
    checks_long = [c for c in (checks or []) if str(c.get("side", "")).upper().strip() == "LONG"]
    return signals_long, checks_long


def _scan_short_disabled(*_args, **_kwargs):
    """v15_long must not compute SHORT side internals."""
    return []


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

    tickers = (
        checks_df.loc[mask, "ticker"]
        .astype(str)
        .str.strip()
        .str.upper()
    )
    tickers = tickers[tickers != ""]
    return sorted(set(tickers.tolist()))


def _apply_v15_strategy_engine_overrides() -> None:
    """Force v2 scanner core to use v15 strategy modules and runner config."""
    v2.default_short_config = v15_default_short_config
    v2.default_long_config = v15_default_long_config
    v2.ref_in_session = v15_in_session
    v2.ref_prepare_indicators = v15_prepare_indicators
    v2.ref_compute_day_avwap = v15_compute_day_avwap
    v2.scan_short_one_day = v15_scan_short_one_day
    v2.scan_long_one_day = v15_scan_long_one_day

    sys.modules["avwap_combined_runner"] = v15_runner
    v2.SHORT_USE_TIME_WINDOWS = bool(
        getattr(v15_runner, "FINAL_SHORT_USE_TIME_WINDOWS", v2.SHORT_USE_TIME_WINDOWS)
    )
    v2.SHORT_SIGNAL_WINDOWS = list(
        getattr(v15_runner, "FINAL_SHORT_SIGNAL_WINDOWS", v2.SHORT_SIGNAL_WINDOWS)
    )
    v2.LONG_USE_TIME_WINDOWS = bool(
        getattr(v15_runner, "FINAL_LONG_USE_TIME_WINDOWS", v2.LONG_USE_TIME_WINDOWS)
    )
    v2.LONG_SIGNAL_WINDOWS = list(
        getattr(v15_runner, "FINAL_LONG_SIGNAL_WINDOWS", v2.LONG_SIGNAL_WINDOWS)
    )
    v2.FORCE_LIVE_PARITY_MIN_BARS_LEFT = bool(
        getattr(v15_runner, "FORCE_LIVE_PARITY_MIN_BARS_LEFT", v2.FORCE_LIVE_PARITY_MIN_BARS_LEFT)
    )
    v2.FORCE_LIVE_PARITY_DISABLE_TOPN = bool(
        getattr(v15_runner, "FORCE_LIVE_PARITY_DISABLE_TOPN", v2.FORCE_LIVE_PARITY_DISABLE_TOPN)
    )


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
    """
    Convert filled pending LONG to the in-memory row schema expected by v2 CSV bridge.
    bar_time_ist intentionally remains original signal time for stable signal identity.
    """
    bar_time = str(p.get("signal_time_ist", ""))
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
    }


def _process_signals_and_pending_v15_long(
    signals_df: Optional[pd.DataFrame],
    *,
    log_label: str,
    log_noop: bool,
) -> int:
    """
    Core bridge for v15_long:
    1) Queue incoming LONG rows as pending anti-chase entries.
    2) Process pending LONG queue against current LTP.
    3) Write only freshly filled LONG rows to signals CSV.
    """
    with _CSV_BRIDGE_LOCK:
        now_ts = pd.Timestamp(v2.now_ist())
        today_str = str(now_ts.date())

        # Load + normalize pending state.
        state = _load_pending_state()
        if state.get("date") != today_str:
            state = {"date": today_str, "pending": {}}
        pending: Dict[str, Dict[str, Any]] = dict(state.get("pending", {}))

        scanned_total = 0 if signals_df is None else int(len(signals_df))
        long_rows: List[Dict[str, Any]] = []
        short_dropped = 0
        long_scanned = 0
        added = 0
        expired = 0
        filled = 0

        # 1) Route incoming rows:
        #    - SHORT -> ignored.
        #    - LONG  -> pending anti-chase queue.
        if signals_df is not None and (not signals_df.empty):
            for _, row in signals_df.iterrows():
                payload = dict(row)
                scanned_total += 0  # explicit no-op for readability
                side = str(payload.get("side", "")).upper().strip()
                if side == "SHORT":
                    short_dropped += 1
                    continue
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

        # 2) Process pending LONG queue against live LTP.
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

        ltp_map = v2._fetch_kite_ltp_map(sorted(set(active_tickers))) if active_tickers else {}

        next_pending: Dict[str, Dict[str, Any]] = {}
        for key, rec in still_pending.items():
            ticker = str(rec.get("ticker", "")).upper().strip()
            limit_price = _safe_float(rec.get("limit_price", np.nan))
            ltp = _safe_float(ltp_map.get(ticker, np.nan))

            if np.isfinite(ltp) and ltp > 0 and np.isfinite(limit_price) and ltp <= limit_price:
                long_rows.append(_pending_to_signal_row(rec))
                filled += 1
                continue

            next_pending[key] = rec

        # Persist queue state after updates.
        state = {"date": today_str, "pending": next_pending}
        _save_pending_state(state)

        # 3) Emit only filled LONG rows through original bridge (without extra rebase).
        long_written = 0
        if long_rows:
            df_filled = pd.DataFrame(long_rows)
            long_written = _disable_kite_rebase_write(df_filled)

        should_log = (
            bool(log_noop)
            or scanned_total > 0
            or short_dropped > 0
            or long_scanned > 0
            or added > 0
            or filled > 0
            or expired > 0
            or long_written > 0
        )
        if should_log:
            print(
                f"{log_label} scanned={0 if signals_df is None else len(signals_df)} "
                f"| short_dropped={short_dropped} "
                f"| long_scanned={long_scanned} | pending_added={added} "
                f"| pending_filled={filled} | pending_expired={expired} "
                f"| pending_open={len(next_pending)} | long_written={long_written}",
                flush=True,
            )
        return int(long_written)


def _write_signals_csv_v15_long(signals_df: pd.DataFrame) -> int:
    return _process_signals_and_pending_v15_long(
        signals_df,
        log_label="[V15_LONG CSV]",
        log_noop=True,
    )


def _pending_poll_once_v15_long() -> int:
    return _process_signals_and_pending_v15_long(
        pd.DataFrame(),
        log_label="[V15_LONG PENDING]",
        log_noop=False,
    )


def _start_pending_poll_worker_v15_long() -> Optional[threading.Event]:
    if not LONG_PENDING_POLL_ENABLED:
        return None

    stop_event = threading.Event()

    def _worker() -> None:
        print(
            f"[V15_LONG PENDING] worker_started | interval={LONG_PENDING_POLL_INTERVAL_SEC:.1f}s",
            flush=True,
        )
        while not stop_event.is_set():
            now = pd.Timestamp(v2.now_ist())
            in_poll_window = v2.SESSION_START <= now.time() <= v2.END_TIME
            if in_poll_window:
                try:
                    _pending_poll_once_v15_long()
                except Exception as exc:
                    print(f"[V15_LONG PENDING] poll_error: {exc}", flush=True)
                stop_event.wait(float(LONG_PENDING_POLL_INTERVAL_SEC))
            else:
                # Stay lightweight outside active polling window.
                stop_event.wait(30.0)

    t = threading.Thread(
        target=_worker,
        name="eqidv2-v15-long-pending-poller",
        daemon=True,
    )
    t.start()
    return stop_event


def _apply_v15_long_overrides() -> None:
    """Patch v2 module-level config/functions to isolate v15_long behavior."""
    _apply_v15_strategy_engine_overrides()

    v2.REPORTS_DIR = ROOT / "reports" / "eqidv2_reports_v15_long"
    v2.REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    v2.OUT_CHECKS_DIR = ROOT / "out_eqidv2_live_checks_15m_v15_long"
    v2.OUT_SIGNALS_DIR = ROOT / "out_eqidv2_live_signals_15m_v15_long"
    v2.OUT_CHECKS_DIR.mkdir(parents=True, exist_ok=True)
    v2.OUT_SIGNALS_DIR.mkdir(parents=True, exist_ok=True)

    v2.STATE_FILE = ROOT / "logs" / "eqidv2_avwap_live_state_v11_v15_long.json"
    v2.SIGNAL_CSV_PATTERN = "signals_{}_v15_long.csv"
    v2.END_TIME = v2.dtime(15, 0)
    v2.SESSION_END = v2.dtime(15, 0, 0)

    # Ensure pending queue is processed even when no fresh signals in a scan cycle.
    v2.IMMEDIATE_SIGNAL_CSV_FLUSH = False

    # Keep Kite enabled because v15_long pending queue relies on LTP.
    v2.USE_KITE_LTP_FOR_SIGNAL_CSV = True
    v2.LONG_TARGET_PCT = float(LONG_TARGET_PCT)

    # Compute only LONG strategy internals in this process.
    v2.scan_short_one_day = _scan_short_disabled
    v2.scan_long_one_day = v15_scan_long_one_day

    # Install LONG-only detector and LONG pending writer.
    v2._latest_entry_signals_for_ticker = _latest_entry_signals_for_ticker_v15_long
    v2._write_signals_csv = _write_signals_csv_v15_long

    # Ensure per-scan parquet outputs get explicit `_v15_long` filename suffix.
    def _run_one_scan_v15_long(run_tag: str = "A"):
        _refresh_v15_nifty_context()
        checks_df, signals_df = _ORIG_RUN_ONE_SCAN(run_tag)

        def _rename_latest(folder: Path, prefix: str, tag: str) -> None:
            candidates = sorted(
                folder.glob(f"{prefix}_*_{tag}.parquet"),
                key=lambda p: p.stat().st_mtime,
            )
            if not candidates:
                return
            src = candidates[-1]
            if src.stem.endswith("_v15_long"):
                return
            dst = src.with_name(src.stem + "_v15_long" + src.suffix)
            try:
                if dst.exists():
                    dst.unlink()
                src.rename(dst)
            except Exception:
                pass

        def _rename_for_tag(tag: str) -> None:
            _rename_latest(v2.OUT_CHECKS_DIR / datetime.now(v2.IST).strftime("%Y%m%d"), "checks", tag)
            _rename_latest(v2.OUT_SIGNALS_DIR / datetime.now(v2.IST).strftime("%Y%m%d"), "signals", tag)

        _rename_for_tag(run_tag)

        if STALE_ONLY_RETRY_ENABLED:
            stale_tickers = _extract_stale_tickers(checks_df)
            if stale_tickers:
                retry_tag = f"{run_tag}R"
                print(
                    f"[V15_LONG RETRY] stale_tickers={len(stale_tickers)} | "
                    f"rerun_subset_tag={retry_tag}",
                    flush=True,
                )
                print(
                    f"[V15_LONG RETRY] stale_ticker_names={','.join(stale_tickers)}",
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
                    f"[V15_LONG RETRY] done | extra_checks={0 if checks_retry is None else len(checks_retry)} "
                    f"| extra_signals={0 if signals_retry is None else len(signals_retry)}",
                    flush=True,
                )

        return checks_df, signals_df

    v2.run_one_scan = _run_one_scan_v15_long

    # Ensure replay default output filename carries `_v15_long`.
    def _run_replay_for_date_v15_long(date_str: str, out_csv: Optional[str] = None) -> pd.DataFrame:
        if out_csv is None:
            out_csv = str(v2.OUT_SIGNALS_DIR / f"replay_signals_{date_str}_v15_long.csv")
        return _ORIG_RUN_REPLAY_FOR_DATE(date_str, out_csv=out_csv)

    v2.run_replay_for_date = _run_replay_for_date_v15_long


def main() -> None:
    _apply_v15_long_overrides()
    _refresh_v15_nifty_context()
    argv_lower = {str(a).strip().lower() for a in sys.argv[1:]}
    is_help_mode = ("-h" in argv_lower) or ("--help" in argv_lower)
    is_replay_mode = ("--replay-date" in argv_lower)
    should_start_pending_worker = (
        LONG_PENDING_POLL_ENABLED
        and (not is_help_mode)
        and (not is_replay_mode)
    )

    if not is_help_mode:
        print(
            "[V15_LONG] LONG-only anti-chase enabled | "
            f"limit_wait={LONG_LIMIT_WAIT_MIN}m "
            f"limit_offset={LONG_LIMIT_OFFSET_PCT*100:.2f}% "
            f"sl={LONG_STOP_PCT*100:.2f}% "
            f"tgt={LONG_TARGET_PCT*100:.2f}% | "
            f"stale_only_retry={STALE_ONLY_RETRY_ENABLED} | "
            f"nifty_context={'on' if bool(getattr(v15_runner, 'NIFTY_CONTEXT_ENABLED', False)) else 'off'} | "
            f"nifty_source={_NIFTY_CONTEXT_SOURCE or 'NA'} | "
            f"end_time={v2.END_TIME.strftime('%H:%M:%S')}",
            flush=True,
        )
        print(
            "[V15_LONG] pending_poll="
            f"{LONG_PENDING_POLL_ENABLED} "
            f"(interval={LONG_PENDING_POLL_INTERVAL_SEC:.1f}s)"
            f" | worker_active={should_start_pending_worker}",
            flush=True,
        )
        if LONG_RSI_CAP is not None:
            print(f"[V15_LONG] LONG_RSI_CAP={LONG_RSI_CAP}", flush=True)
        if LONG_ADX_MIN is not None:
            print(f"[V15_LONG] LONG_ADX_MIN={LONG_ADX_MIN}", flush=True)
        if LONG_QUALITY_MIN is not None:
            print(f"[V15_LONG] LONG_QUALITY_MIN={LONG_QUALITY_MIN}", flush=True)

    poll_stop: Optional[threading.Event] = None
    if should_start_pending_worker:
        poll_stop = _start_pending_poll_worker_v15_long()
    elif (not is_help_mode) and LONG_PENDING_POLL_ENABLED and is_replay_mode:
        print("[V15_LONG] pending worker skipped in replay mode.", flush=True)

    try:
        v2.main()
    finally:
        if poll_stop is not None:
            poll_stop.set()


if __name__ == "__main__":
    main()


