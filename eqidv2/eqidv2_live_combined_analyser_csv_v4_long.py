# -*- coding: utf-8 -*-
"""
EQIDV2 LIVE Scanner V4 LONG (long-only anti-chase split pipeline)
==================================================================

This file is a non-destructive wrapper over:
    eqidv2_live_combined_analyser_csv_v2.py

Goals:
1. Keep existing v2 files untouched.
2. Emit only LONG signals.
3. Keep anti-chase LONG pending-entry behavior from v3.
4. Isolate all outputs/state with `v4_long` suffix.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

import eqidv2_live_combined_analyser_csv_v2 as v2


# -----------------------------------------------------------------------------
# V4_LONG ANTI-CHASE CONFIG
# -----------------------------------------------------------------------------
LONG_LIMIT_WAIT_MIN = int(os.getenv("EQIDV4_LONG_LIMIT_WAIT_MIN", "60"))
LONG_LIMIT_OFFSET_PCT = float(os.getenv("EQIDV4_LONG_LIMIT_OFFSET_PCT", "-0.005"))  # -0.5%
LONG_STOP_PCT = float(os.getenv("EQIDV4_LONG_STOP_PCT", "0.006"))                    # 0.6%
LONG_TARGET_PCT = float(os.getenv("EQIDV4_LONG_TARGET_PCT", "0.018"))                # 1.8%

# Optional signal-quality filters before putting LONG into pending queue.
LONG_RSI_CAP_RAW = os.getenv("EQIDV4_LONG_RSI_CAP", "").strip()
LONG_ADX_MIN_RAW = os.getenv("EQIDV4_LONG_ADX_MIN", "").strip()
LONG_QUALITY_MIN_RAW = os.getenv("EQIDV4_LONG_QUALITY_MIN", "").strip()

LONG_RSI_CAP: Optional[float] = float(LONG_RSI_CAP_RAW) if LONG_RSI_CAP_RAW else None
LONG_ADX_MIN: Optional[float] = float(LONG_ADX_MIN_RAW) if LONG_ADX_MIN_RAW else None
LONG_QUALITY_MIN: Optional[float] = float(LONG_QUALITY_MIN_RAW) if LONG_QUALITY_MIN_RAW else None


ROOT = Path(__file__).resolve().parent
PENDING_STATE_FILE = ROOT / "logs" / "eqidv2_long_pending_state_v4_long.json"


# Keep original functions so wrapper can delegate safely.
_ORIG_WRITE_SIGNALS_CSV = v2._write_signals_csv
_ORIG_LATEST_ENTRY_SIGNALS_FOR_TICKER = v2._latest_entry_signals_for_ticker
_ORIG_RUN_ONE_SCAN = v2.run_one_scan
_ORIG_RUN_REPLAY_FOR_DATE = v2.run_replay_for_date


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
    tmp = path.with_suffix(path.suffix + ".tmp")
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


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
    We already finalize entry/SL/TGT in v4_long pending-fill logic.
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


def _latest_entry_signals_for_ticker_v4_long(
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
    signals_long = [s for s in (signals or []) if str(getattr(s, "side", "")).upper().strip() == "LONG"]
    checks_long = [c for c in (checks or []) if str(c.get("side", "")).upper().strip() == "LONG"]
    return signals_long, checks_long


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


def _write_signals_csv_v4_long(signals_df: pd.DataFrame) -> int:
    """
    V4_LONG bridge:
    1) LONG rows are queued as pending anti-chase entries.
    2) Pending LONG are filled via LTP and written when limit is hit.
    """
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

    print(
        f"[V4_LONG CSV] scanned={0 if signals_df is None else len(signals_df)} "
        f"| short_dropped={short_dropped} "
        f"| long_scanned={long_scanned} | pending_added={added} "
        f"| pending_filled={filled} | pending_expired={expired} "
        f"| pending_open={len(next_pending)} | long_written={long_written}",
        flush=True,
    )
    return int(long_written)


def _apply_v4_long_overrides() -> None:
    """Patch v2 module-level config/functions to isolate v4_long behavior."""
    v2.REPORTS_DIR = ROOT / "reports" / "eqidv2_reports_v4_long"
    v2.REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    v2.OUT_CHECKS_DIR = ROOT / "out_eqidv2_live_checks_15m_v4_long"
    v2.OUT_SIGNALS_DIR = ROOT / "out_eqidv2_live_signals_15m_v4_long"
    v2.OUT_CHECKS_DIR.mkdir(parents=True, exist_ok=True)
    v2.OUT_SIGNALS_DIR.mkdir(parents=True, exist_ok=True)

    v2.STATE_FILE = ROOT / "logs" / "eqidv2_avwap_live_state_v11_v4_long.json"
    v2.SIGNAL_CSV_PATTERN = "signals_{}_v4_long.csv"
    v2.END_TIME = v2.dtime(14, 40)
    v2.SESSION_END = v2.dtime(14, 40, 0)

    # Ensure pending queue is processed even when no fresh signals in a scan cycle.
    v2.IMMEDIATE_SIGNAL_CSV_FLUSH = False

    # Keep Kite enabled because v4_long pending queue relies on LTP.
    v2.USE_KITE_LTP_FOR_SIGNAL_CSV = True

    # Install LONG-only detector and LONG pending writer.
    v2._latest_entry_signals_for_ticker = _latest_entry_signals_for_ticker_v4_long
    v2._write_signals_csv = _write_signals_csv_v4_long

    # Ensure per-scan parquet outputs get explicit `_v4_long` filename suffix.
    def _run_one_scan_v4_long(run_tag: str = "A"):
        checks_df, signals_df = _ORIG_RUN_ONE_SCAN(run_tag)

        def _rename_latest(folder: Path, prefix: str) -> None:
            candidates = sorted(
                folder.glob(f"{prefix}_*_{run_tag}.parquet"),
                key=lambda p: p.stat().st_mtime,
            )
            if not candidates:
                return
            src = candidates[-1]
            if src.stem.endswith("_v4_long"):
                return
            dst = src.with_name(src.stem + "_v4_long" + src.suffix)
            try:
                if dst.exists():
                    dst.unlink()
                src.rename(dst)
            except Exception:
                pass

        _rename_latest(v2.OUT_CHECKS_DIR / datetime.now(v2.IST).strftime("%Y%m%d"), "checks")
        _rename_latest(v2.OUT_SIGNALS_DIR / datetime.now(v2.IST).strftime("%Y%m%d"), "signals")
        return checks_df, signals_df

    v2.run_one_scan = _run_one_scan_v4_long

    # Ensure replay default output filename carries `_v4_long`.
    def _run_replay_for_date_v4_long(date_str: str, out_csv: Optional[str] = None) -> pd.DataFrame:
        if out_csv is None:
            out_csv = str(v2.OUT_SIGNALS_DIR / f"replay_signals_{date_str}_v4_long.csv")
        return _ORIG_RUN_REPLAY_FOR_DATE(date_str, out_csv=out_csv)

    v2.run_replay_for_date = _run_replay_for_date_v4_long


def main() -> None:
    _apply_v4_long_overrides()
    print(
        "[V4_LONG] LONG-only anti-chase enabled | "
        f"limit_wait={LONG_LIMIT_WAIT_MIN}m "
        f"limit_offset={LONG_LIMIT_OFFSET_PCT*100:.2f}% "
        f"sl={LONG_STOP_PCT*100:.2f}% "
        f"tgt={LONG_TARGET_PCT*100:.2f}%",
        flush=True,
    )
    if LONG_RSI_CAP is not None:
        print(f"[V4_LONG] LONG_RSI_CAP={LONG_RSI_CAP}", flush=True)
    if LONG_ADX_MIN is not None:
        print(f"[V4_LONG] LONG_ADX_MIN={LONG_ADX_MIN}", flush=True)
    if LONG_QUALITY_MIN is not None:
        print(f"[V4_LONG] LONG_QUALITY_MIN={LONG_QUALITY_MIN}", flush=True)
    v2.main()


if __name__ == "__main__":
    main()
