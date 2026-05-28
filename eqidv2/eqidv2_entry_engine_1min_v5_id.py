"""
Entry engine 1 min v7 ID.

Consumes "Signal discovery v7 5mins ID" candidate tickers for the previous
5-minute slot, fetches raw 1-minute OHLCV only for those tickers, stores the raw
data as parquet, and writes executable rows into the existing ID 5min v7 live
entry CSVs:

  live_signals/signals_<YYYY-MM-DD>_id_5min_v7_short.csv
  live_signals/signals_<YYYY-MM-DD>_id_5min_v7_long.csv

This module does not calculate indicators. It only uses raw 1-minute OHLCV for
entry price discovery.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
from datetime import datetime, time as dtime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import pytz

import avwap_5min_ID_v6_backtesting as v6
import eqidv2_eod_scheduler_for_5mins_data_live_minimal as scheduler
import eqidv2_live_combined_analyser_csv_id_5min_v7_persistent as v7_persistent
from eqidv2_runtime_paths import RUNTIME_ROOT, RUNTIME_STATUS_DIR, runtime_dir


SESSION_NAME = "Entry engine 1 min v7 ID"
SESSION_SLUG = "entry_engine_1min_v5_ID"
SESSION_ROOT = runtime_dir(SESSION_SLUG)
RAW_1MIN_DIR = runtime_dir("stocks_raw_1min_entry_v5_id_live")
SLOT_RAW_DIR = SESSION_ROOT / "slot_raw_1min"
AUDIT_DIR = SESSION_ROOT / "audit"
LATEST_DIR = SESSION_ROOT / "latest"
HEARTBEAT_DIR = SESSION_ROOT / "heartbeat"
SIGNAL_DISCOVERY_ROOT = runtime_dir("signal_discovery_v7_5mins_ID")
SIGNAL_DISCOVERY_LATEST_JSON = SIGNAL_DISCOVERY_ROOT / "latest" / "latest_candidate_tickers.json"
SIGNAL_DISCOVERY_LATEST_CSV = SIGNAL_DISCOVERY_ROOT / "latest" / "latest_candidate_tickers.csv"

IST = pytz.timezone("Asia/Kolkata")
MARKET_OPEN = dtime(9, 15)
END_TIME = dtime(15, 1)
HARD_STOP = dtime(15, 35)
ENTRY_DELAY_SEC = int(os.getenv("EQIDV2_ENTRY_ENGINE_1MIN_V5_ID_DELAY_SEC", "60"))
ENTRY_DUE_GRACE_SEC = int(os.getenv("EQIDV2_ENTRY_ENGINE_1MIN_V5_ID_DUE_GRACE_SEC", "90"))
POLL_SEC = float(os.getenv("EQIDV2_ENTRY_ENGINE_1MIN_V5_ID_POLL_SEC", "1.0"))
KITE_INTERVAL = "minute"

# v8 backtesting resolves exits strictly from v6.SETUP_EXIT_RULES. The live
# signal-discovery stage stays signal-only; SL/target are attached here.
ENTRY_SEARCH_MAX_DELAY_MIN = int(os.getenv("EQIDV2_ENTRY_ENGINE_1MIN_V7_MAX_DELAY_MIN", "5"))


for _p in (SESSION_ROOT, RAW_1MIN_DIR, SLOT_RAW_DIR, AUDIT_DIR, LATEST_DIR, HEARTBEAT_DIR):
    _p.mkdir(parents=True, exist_ok=True)


def _logger() -> logging.Logger:
    log = logging.getLogger(SESSION_SLUG)
    log.setLevel(logging.INFO)
    if not log.handlers:
        h = logging.StreamHandler()
        h.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
        log.addHandler(h)
    return log


def _set_status_env() -> None:
    os.environ["EQIDV2_RUNTIME_STATUS_FILE"] = str(RUNTIME_STATUS_DIR / f"{SESSION_SLUG}.status")
    os.environ["EQIDV2_RUNTIME_HEARTBEAT_FILE"] = str(RUNTIME_STATUS_DIR / f"{SESSION_SLUG}.heartbeat")


def _touch_status(status: str, **extra: Any) -> None:
    _set_status_env()
    v7_persistent.base_v15._touch_runtime_status(status, session=SESSION_NAME, **extra)
    payload = {
        "status": status,
        "session": SESSION_NAME,
        "updated_at_ist": _fmt_ist(pd.Timestamp.now(tz=IST)),
        **extra,
    }
    (HEARTBEAT_DIR / "entry_engine.status.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )


def _touch_heartbeat(status: str = "RUNNING", **extra: Any) -> None:
    _set_status_env()
    v7_persistent.base_v15._touch_runtime_heartbeat(status, session=SESSION_NAME, **extra)
    payload = {
        "status": status,
        "session": SESSION_NAME,
        "updated_at_ist": _fmt_ist(pd.Timestamp.now(tz=IST)),
        **extra,
    }
    (HEARTBEAT_DIR / "entry_engine.heartbeat.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )


def _ensure_ist_ts(value: Any) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tz is None:
        ts = ts.tz_localize(IST)
    else:
        ts = ts.tz_convert(IST)
    return ts


def _fmt_ist(value: Any) -> str:
    ts = _ensure_ist_ts(value)
    offset = ts.strftime("%z")
    return f"{ts.strftime('%Y-%m-%d %H:%M:%S')}{offset[:3]}:{offset[3:]}"


def _slot_key(slot: pd.Timestamp) -> str:
    return _ensure_ist_ts(slot).strftime("%Y%m%d_%H%M")


def _floor_5m(ts: pd.Timestamp) -> pd.Timestamp:
    ts = _ensure_ist_ts(ts)
    minute = (ts.minute // 5) * 5
    return ts.replace(minute=minute, second=0, microsecond=0)


def _next_entry_run_after(now: datetime) -> pd.Timestamp:
    now_ts = _ensure_ist_ts(now)
    base_slot = _floor_5m(now_ts)
    run_at = base_slot + pd.Timedelta(seconds=ENTRY_DELAY_SEC)
    # The loop can wake a fraction of a second after run_at. Without a grace
    # band, it skips the just-due slot and jumps five minutes ahead.
    if now_ts <= run_at + pd.Timedelta(seconds=ENTRY_DUE_GRACE_SEC):
        return run_at
    return base_slot + pd.Timedelta(minutes=5, seconds=ENTRY_DELAY_SEC)


def _load_candidates_for_slot(slot: pd.Timestamp) -> pd.DataFrame:
    slot = _ensure_ist_ts(slot).floor("min")
    df = pd.DataFrame()
    if SIGNAL_DISCOVERY_LATEST_JSON.exists():
        try:
            payload = json.loads(SIGNAL_DISCOVERY_LATEST_JSON.read_text(encoding="utf-8", errors="replace"))
            rows = payload.get("candidates", [])
            if isinstance(rows, list):
                df = pd.DataFrame(rows)
        except Exception:
            df = pd.DataFrame()
    if df.empty and SIGNAL_DISCOVERY_LATEST_CSV.exists():
        try:
            df = pd.read_csv(SIGNAL_DISCOVERY_LATEST_CSV)
        except Exception:
            df = pd.DataFrame()
    if df.empty:
        return df
    if "signal_time_ist" not in df.columns:
        return pd.DataFrame()
    sig = pd.to_datetime(df["signal_time_ist"], errors="coerce")
    if getattr(sig.dt, "tz", None) is None:
        sig = sig.dt.tz_localize(IST)
    else:
        sig = sig.dt.tz_convert(IST)
    df = df.loc[sig.dt.floor("min").eq(slot)].copy()
    if df.empty:
        return df
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df["side"] = df["side"].astype(str).str.upper().str.strip()
    df["setup"] = df["setup"].astype(str)
    df["signal_time_ist"] = sig.loc[df.index].map(_fmt_ist)
    df["quality_score"] = pd.to_numeric(df.get("quality_score", 0.0), errors="coerce").fillna(0.0)
    return (
        df.sort_values(["quality_score", "ticker", "setup"], ascending=[False, True, True])
        .drop_duplicates(subset=["candidate_id"], keep="first")
        .drop_duplicates(subset=["signal_time_ist", "ticker"], keep="first")
        .reset_index(drop=True)
    )


def _setup_kite_and_tokens(tickers: List[str]) -> Tuple[Any, Dict[str, int]]:
    log = _logger()
    kite = scheduler.setup_kite_session_from_eqidv2_dir()
    tokens = scheduler.core.load_or_fetch_tokens(kite, tickers, log, refresh=False)
    tokens = {str(k).upper(): int(v) for k, v in dict(tokens or {}).items()}
    return kite, tokens


def _normalise_raw_1m(raw: List[Dict[str, Any]], ticker: str) -> pd.DataFrame:
    if not raw:
        return pd.DataFrame()
    df = pd.DataFrame(raw)
    if df.empty:
        return df
    if "date" not in df.columns:
        return pd.DataFrame()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    if getattr(df["date"].dt, "tz", None) is None:
        df["date"] = df["date"].dt.tz_localize(IST)
    else:
        df["date"] = df["date"].dt.tz_convert(IST)
    keep = [c for c in ["date", "open", "high", "low", "close", "volume"] if c in df.columns]
    df = df[keep].dropna(subset=["date"]).copy()
    for col in ("open", "high", "low", "close", "volume"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df["ticker"] = str(ticker).upper()
    return df.sort_values("date").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)


def _upsert_ticker_raw(ticker: str, df_new: pd.DataFrame) -> Path:
    out_path = RAW_1MIN_DIR / f"{str(ticker).upper()}_stocks_raw_1min.parquet"
    if df_new.empty:
        return out_path
    if out_path.exists():
        try:
            old = pd.read_parquet(out_path)
        except Exception:
            old = pd.DataFrame()
        merged = pd.concat([old, df_new], ignore_index=True, sort=False)
    else:
        merged = df_new.copy()
    merged["date"] = pd.to_datetime(merged["date"], errors="coerce")
    if getattr(merged["date"].dt, "tz", None) is None:
        merged["date"] = merged["date"].dt.tz_localize(IST)
    else:
        merged["date"] = merged["date"].dt.tz_convert(IST)
    merged = merged.dropna(subset=["date"]).sort_values("date").drop_duplicates(subset=["date"], keep="last")
    merged.to_parquet(out_path, index=False)
    return out_path


def _slot_raw_path(slot: pd.Timestamp, ticker: str) -> Path:
    slot_dir = SLOT_RAW_DIR / _slot_key(slot)
    slot_dir.mkdir(parents=True, exist_ok=True)
    return slot_dir / f"{str(ticker).upper()}_raw_1min.parquet"


def _fetch_raw_for_candidates(candidates: pd.DataFrame, slot: pd.Timestamp) -> Dict[str, pd.DataFrame]:
    tickers = sorted(candidates["ticker"].dropna().astype(str).str.upper().unique())
    if not tickers:
        return {}
    kite, tokens = _setup_kite_and_tokens(tickers)
    start = _ensure_ist_ts(slot)
    end = start + pd.Timedelta(minutes=max(1, ENTRY_SEARCH_MAX_DELAY_MIN), seconds=30)
    fetched: Dict[str, pd.DataFrame] = {}
    for ticker in tickers:
        token = tokens.get(str(ticker).upper())
        if not token:
            continue
        try:
            raw = kite.historical_data(int(token), start.to_pydatetime(), end.to_pydatetime(), KITE_INTERVAL)
        except Exception as exc:
            print(f"[{SESSION_NAME}] fetch failed {ticker}: {type(exc).__name__}: {exc}", flush=True)
            continue
        df = _normalise_raw_1m(raw, ticker)
        if df.empty:
            continue
        df.to_parquet(_slot_raw_path(slot, ticker), index=False)
        _upsert_ticker_raw(ticker, df)
        fetched[ticker] = df
    return fetched


def _entry_bar_for_candidate(raw_by_ticker: Dict[str, pd.DataFrame], cand: pd.Series) -> Optional[pd.Series]:
    ticker = str(cand.get("ticker", "")).upper()
    df = raw_by_ticker.get(ticker)
    if df is None or df.empty:
        return None
    sig = _ensure_ist_ts(cand.get("signal_time_ist"))
    dates = pd.to_datetime(df["date"], errors="coerce")
    if getattr(dates.dt, "tz", None) is None:
        dates = dates.dt.tz_localize(IST)
    else:
        dates = dates.dt.tz_convert(IST)
    work = df.copy()
    work["date"] = dates
    sub = work[
        (work["date"] >= sig)
        & (work["date"] <= sig + pd.Timedelta(minutes=ENTRY_SEARCH_MAX_DELAY_MIN))
    ].sort_values("date")
    if sub.empty:
        return None
    return sub.iloc[0]


def _build_entry_rows(candidates: pd.DataFrame, raw_by_ticker: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for _, cand in candidates.iterrows():
        setup = str(cand.get("setup", ""))
        side = str(cand.get("side", "")).upper()
        rule_source = "v6"
        rule = v6.SETUP_EXIT_RULES.get(setup)
        if rule is None:
            continue
        entry_bar = _entry_bar_for_candidate(raw_by_ticker, cand)
        if entry_bar is None:
            continue
        entry_price = float(entry_bar.get("open", np.nan))
        if not np.isfinite(entry_price) or entry_price <= 0:
            continue
        sl_pct, tgt_pct = rule
        if side == "LONG":
            sl_price = entry_price * (1.0 - sl_pct / 100.0)
            target_price = entry_price * (1.0 + tgt_pct / 100.0)
        elif side == "SHORT":
            sl_price = entry_price * (1.0 + sl_pct / 100.0)
            target_price = entry_price * (1.0 - tgt_pct / 100.0)
        else:
            continue
        diag = {
            "source_session": SESSION_NAME,
            "signal_discovery_session": str(cand.get("scan_session", "")),
            "selection_mode": str(cand.get("selection_mode", "")),
            "signal_time_ist": str(cand.get("signal_time_ist", "")),
            "entry_time_ist": _fmt_ist(entry_bar.get("date")),
            "sl_pct": sl_pct,
            "target_pct": tgt_pct,
            "exit_rule_source": rule_source,
            "reason": str(cand.get("reason", "")),
            "rs_pct": cand.get("rs_pct", ""),
            "vol_ratio": cand.get("vol_ratio", ""),
        }
        rows.append({
            "ticker": str(cand.get("ticker", "")).upper(),
            "side": side,
            "setup": setup,
            "bar_time_ist": str(cand.get("signal_time_ist", "")),
            "entry_price": entry_price,
            "sl_price": sl_price,
            "target_price": target_price,
            "score": cand.get("quality_score", 0.0),
            "diagnostics_json": json.dumps(diag, default=str),
            "entry_time_ist": _fmt_ist(entry_bar.get("date")),
            "candidate_id": str(cand.get("candidate_id", "")),
            "selection_mode": str(cand.get("selection_mode", "")),
            "exit_rule_source": rule_source,
            "sl_pct": sl_pct,
            "target_pct": tgt_pct,
        })
    return pd.DataFrame(rows)


def _entry_reject_audit(candidates: pd.DataFrame, raw_by_ticker: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    if candidates is None or candidates.empty:
        return pd.DataFrame()
    rows: List[Dict[str, Any]] = []
    for _, cand in candidates.iterrows():
        setup = str(cand.get("setup", ""))
        ticker = str(cand.get("ticker", "")).upper().strip()
        reason = ""
        if setup not in v6.SETUP_EXIT_RULES:
            reason = "missing_v8_setup_exit_rule"
        elif ticker not in raw_by_ticker:
            reason = "missing_1min_fetch"
        elif _entry_bar_for_candidate(raw_by_ticker, cand) is None:
            reason = "missing_1min_entry_bar"
        if reason:
            rows.append({
                "ticker": ticker,
                "side": str(cand.get("side", "")).upper(),
                "setup": setup,
                "signal_time_ist": str(cand.get("signal_time_ist", "")),
                "candidate_id": str(cand.get("candidate_id", "")),
                "selection_mode": str(cand.get("selection_mode", "")),
                "reject_reason": reason,
            })
    return pd.DataFrame(rows)


def _select_executable_entries(entry_df: pd.DataFrame) -> pd.DataFrame:
    """Keep one executable row per signal candle/ticker.

    Candidate discovery intentionally stores all setup hits. Execution should
    not place multiple orders for the same ticker/slot just because two setup
    labels or opposite-side labels fired on the same signal candle.
    """
    if entry_df is None or entry_df.empty:
        return pd.DataFrame()
    df = entry_df.copy()
    df["_score_num"] = pd.to_numeric(df.get("score", 0.0), errors="coerce").fillna(0.0)
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df["side"] = df["side"].astype(str).str.upper().str.strip()
    df["bar_time_ist"] = df["bar_time_ist"].astype(str)
    df = (
        df.sort_values(["_score_num", "setup"], ascending=[False, True])
        .drop_duplicates(subset=["bar_time_ist", "ticker"], keep="first")
        .drop(columns=["_score_num"], errors="ignore")
        .reset_index(drop=True)
    )
    return df


def _live_signal_csv_path(day: str, side: str) -> Path:
    return v7_persistent._signal_csv_path(day, side)


def _load_live_written_tickers(day: str) -> set[str]:
    tickers: set[str] = set()
    for side in ("SHORT", "LONG"):
        path = _live_signal_csv_path(day, side)
        if not path.exists() or path.stat().st_size <= 0:
            continue
        try:
            existing = pd.read_csv(path, usecols=["ticker"])
        except Exception:
            continue
        tickers.update(existing["ticker"].dropna().astype(str).str.upper().str.strip())
    return {t for t in tickers if t}


def _filter_new_intraday_tickers(entry_df: pd.DataFrame, slot: pd.Timestamp) -> pd.DataFrame:
    """Reject tickers already written today in either side live CSV."""
    if entry_df is None or entry_df.empty:
        return pd.DataFrame()
    day = _ensure_ist_ts(slot).strftime("%Y-%m-%d")
    used = _load_live_written_tickers(day)
    df = entry_df.copy()
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df["_score_num"] = pd.to_numeric(df.get("score", 0.0), errors="coerce").fillna(0.0)
    df = df.sort_values(["_score_num", "ticker"], ascending=[False, True])
    keep = []
    batch: set[str] = set()
    for _, row in df.iterrows():
        ticker = str(row.get("ticker", "")).upper().strip()
        if not ticker or ticker in used or ticker in batch:
            continue
        keep.append(row.to_dict())
        batch.add(ticker)
    if not keep:
        return pd.DataFrame(columns=[c for c in df.columns if c != "_score_num"])
    return pd.DataFrame(keep).drop(columns=["_score_num"], errors="ignore").reset_index(drop=True)


def _write_live_entry_csvs(entry_df: pd.DataFrame, slot: pd.Timestamp) -> Tuple[int, int]:
    if entry_df.empty:
        return 0, 0
    day = _ensure_ist_ts(slot).strftime("%Y-%m-%d")
    short_df = entry_df[entry_df["side"].astype(str).str.upper().eq("SHORT")].copy()
    long_df = entry_df[entry_df["side"].astype(str).str.upper().eq("LONG")].copy()
    short_written = v7_persistent._write_side_signals_csv(short_df, side="SHORT", signal_day_str=day)
    long_written = v7_persistent._write_side_signals_csv(long_df, side="LONG", signal_day_str=day)
    return int(short_written), int(long_written)


def run_slot(slot_ts: Any, *, write_live_entries: bool = True) -> Dict[str, Any]:
    slot = _ensure_ist_ts(slot_ts).floor("min")
    t0 = time.perf_counter()
    candidates = _load_candidates_for_slot(slot)
    raw_by_ticker = _fetch_raw_for_candidates(candidates, slot) if not candidates.empty else {}
    raw_entries = _build_entry_rows(candidates, raw_by_ticker) if raw_by_ticker else pd.DataFrame()
    rejected_entries = _entry_reject_audit(candidates, raw_by_ticker)
    selected_entries = _select_executable_entries(raw_entries)
    entries = _filter_new_intraday_tickers(selected_entries, slot)

    latest_entries_csv = LATEST_DIR / "latest_entry_engine_rows.csv"
    entries.to_csv(latest_entries_csv, index=False)
    slot_entries_csv = AUDIT_DIR / f"entry_rows_{_slot_key(slot)}.csv"
    entries.to_csv(slot_entries_csv, index=False)
    raw_slot_entries_csv = AUDIT_DIR / f"entry_rows_raw_candidates_{_slot_key(slot)}.csv"
    raw_entries.to_csv(raw_slot_entries_csv, index=False)
    rejected_entries_csv = AUDIT_DIR / f"entry_rejected_candidates_{_slot_key(slot)}.csv"
    rejected_entries.to_csv(rejected_entries_csv, index=False)
    pd.DataFrame(
        [{"setup": k, "sl_pct": v[0], "target_pct": v[1]} for k, v in sorted(v6.SETUP_EXIT_RULES.items())]
    ).to_csv(LATEST_DIR / "setup_exit_rules_v8.csv", index=False)

    short_written = long_written = 0
    if write_live_entries and not entries.empty:
        short_written, long_written = _write_live_entry_csvs(entries, slot)

    summary = {
        "slot_ist": _fmt_ist(slot),
        "candidate_count": int(len(candidates)),
        "tickers_requested": int(candidates["ticker"].nunique()) if not candidates.empty and "ticker" in candidates.columns else 0,
        "tickers_fetched": int(len(raw_by_ticker)),
        "raw_entry_rows": int(len(raw_entries)),
        "rejected_entry_rows": int(len(rejected_entries)),
        "selected_entry_rows": int(len(selected_entries)),
        "entry_rows": int(len(entries)),
        "deduped_entry_rows": int(max(0, len(raw_entries) - len(selected_entries))),
        "intraday_duplicate_rows": int(max(0, len(selected_entries) - len(entries))),
        "short_written": int(short_written),
        "long_written": int(long_written),
        "raw_dir": str(RAW_1MIN_DIR),
        "latest_entries_csv": str(latest_entries_csv),
        "setup_exit_rules_csv": str(LATEST_DIR / "setup_exit_rules_v8.csv"),
        "entry_search_max_delay_min": int(ENTRY_SEARCH_MAX_DELAY_MIN),
        "elapsed_sec": round(time.perf_counter() - t0, 3),
    }
    (LATEST_DIR / "latest_summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    audit_path = AUDIT_DIR / f"entry_engine_audit_{slot.strftime('%Y-%m-%d')}.jsonl"
    with open(audit_path, "a", encoding="utf-8") as f:
        f.write(json.dumps({"session": SESSION_NAME, **summary}, sort_keys=True) + "\n")
    _touch_status("RUNNING", phase="SCAN_DONE", slot=slot.strftime("%H:%M"), **summary)
    _touch_heartbeat("RUNNING", phase="SCAN_DONE", slot=slot.strftime("%H:%M"))
    return {"session": SESSION_NAME, **summary}


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=SESSION_NAME)
    ap.add_argument("--replay-slot", default="", help="Run one slot, e.g. 2026-05-21 11:10:00+05:30")
    ap.add_argument("--no-write-live-entries", action="store_true")
    return ap.parse_args()


def main() -> None:
    args = _parse_args()
    print(f"[LIVE] {SESSION_NAME}", flush=True)
    print(f"[INFO] raw_1min_dir={RAW_1MIN_DIR}", flush=True)

    if args.replay_slot:
        summary = run_slot(args.replay_slot, write_live_entries=not args.no_write_live_entries)
        print(json.dumps(summary, indent=2, sort_keys=True), flush=True)
        return

    holidays = v7_persistent.base_v15._read_holidays_safe()
    processed: set[str] = set()
    while True:
        now = v7_persistent.base_v15.now_ist()
        _touch_status("RUNNING", phase="LOOP")
        _touch_heartbeat("RUNNING", phase="LOOP")

        if now.time() >= HARD_STOP:
            _touch_status("STOPPED_AFTER_CUTOFF", phase="HARD_STOP")
            _touch_heartbeat("STOPPED", phase="HARD_STOP")
            return
        if not v7_persistent.base_v15.is_trading_day_safe(now.date(), holidays):
            nxt = v7_persistent.base_v15._next_trading_day_start(now, holidays)
            v7_persistent.base_v15._sleep_until(nxt)
            holidays = v7_persistent.base_v15._read_holidays_safe()
            processed.clear()
            continue
        if now.time() < MARKET_OPEN or now.time() > END_TIME:
            time.sleep(5.0)
            continue

        run_at = _next_entry_run_after(now)
        slot = run_at - pd.Timedelta(seconds=ENTRY_DELAY_SEC)
        key = _slot_key(slot)
        if key in processed:
            time.sleep(POLL_SEC)
            continue
        now_ts = _ensure_ist_ts(v7_persistent.base_v15.now_ist())
        if now_ts < run_at:
            time.sleep(min(POLL_SEC, max(0.0, (run_at - now_ts).total_seconds())))
            continue

        _touch_status("RUNNING", phase="ENTRY_SCAN", slot=slot.strftime("%H:%M"))
        _touch_heartbeat("RUNNING", phase="ENTRY_SCAN", slot=slot.strftime("%H:%M"))
        summary = run_slot(slot, write_live_entries=True)
        print(json.dumps(summary, indent=2, sort_keys=True), flush=True)
        processed.add(key)
        time.sleep(POLL_SEC)


if __name__ == "__main__":
    main()
