# -*- coding: utf-8 -*-
"""
compare_twostage_v16_5min.py
============================
Offline comparison: what would the two-stage V16 5min stack have produced
for a given date vs. what the existing live scanner produced.

No Kite API needed. Reads from the live parquet directory directly.

Usage:
    python compare_twostage_v16_5min.py             # today
    python compare_twostage_v16_5min.py 2026-04-16  # specific date

Output:
    - Raw signal pool (Signal Engine equivalent — no filters)
    - Filter funnel breakdown
    - Final confirmed signals (Detection Engine equivalent)
    - Side-by-side comparison with existing scanner's signals CSV
"""

from __future__ import annotations

import os
import sys
import time
from dataclasses import asdict
from datetime import datetime, timedelta, time as dtime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import pytz

# ---------------------------------------------------------------------------
# Runtime
# ---------------------------------------------------------------------------
os.environ.setdefault("EQIDV2_DATA_5M_DIR", r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live")

import avwap_combined_runner_v16_5min as v16_runner
from avwap_combined_runner_v16_5min import (
    apply_live_parity_profile,
    get_v16_filter_reason,
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
    default_long_config as default_long_config_v11,
    prepare_session_bars_for_scan,
    list_tickers_15m,
)
from avwap_v11_refactored.avwap_short_strategy_v11 import (
    scan_all_days_for_ticker_prepared as scan_short_prepared,
)
from avwap_v11_refactored.avwap_long_strategy_v9_sweep import (
    scan_all_days_for_ticker_prepared as scan_long_prepared,
)
import eqidv2_live_combined_analyser_csv_v15 as base_v15
from eqidv2_runtime_paths import DATA_5M_DIR, LIVE_SIGNALS_DIR

IST = pytz.timezone("Asia/Kolkata")

DATA_DIR   = str(DATA_5M_DIR)
END_5M     = "_stocks_indicators_5min.parquet"
START_TIME = dtime(9, 15)

NEUTRALIZE_WEAK_NIFTY_CONTEXT   = True
DIRECTIONAL_NIFTY_CONTEXT_FALLBACK = True
NEUTRALIZE_PARTIAL_NIFTY_SESSION = True

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _safe_float(v: Any, d: float = 0.0) -> float:
    try:
        import math; x = float(v); return x if math.isfinite(x) else d
    except Exception:
        return d


def _load_parquet(ticker: str, n: int = 600) -> Optional[pd.DataFrame]:
    path = os.path.join(DATA_DIR, f"{ticker}{END_5M}")
    df = base_v15.read_parquet_tail(path, n=n)
    if df is None or df.empty or "date" not in df.columns:
        return None
    dt = pd.to_datetime(df["date"], errors="coerce")
    if getattr(dt.dt, "tz", None) is None:
        dt = dt.dt.tz_localize("UTC")
    dt = dt.dt.tz_convert(IST)
    df = df.copy()
    df["date"] = dt
    return df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)


def _build_cfgs() -> Tuple[StrategyConfig, StrategyConfig]:
    v16_runner.NIFTY_CONTEXT_OR_END_TIME  = dtime(9, 20)
    v16_runner.NIFTY_CONTEXT_CONFIRM_TIME = dtime(9, 20)
    short_builder = getattr(v16_runner, "default_short_config", None)
    short_cfg = short_builder() if callable(short_builder) else default_short_config()
    long_builder = getattr(v16_runner, "default_long_config_v9", None)
    long_cfg  = long_builder() if callable(long_builder) else default_long_config_v11()
    short_cfg, long_cfg = apply_live_parity_profile(short_cfg, long_cfg)
    short_cfg.stop_pct   = 0.0075
    long_cfg.stop_pct    = 0.0075
    short_cfg.target_pct = float(TEST_SHORT_TARGET_PCT)
    long_cfg.target_pct  = float(TEST_LONG_TARGET_PCT)
    short_cfg.dir_15m = DATA_DIR
    short_cfg.end_15m = END_5M
    long_cfg.dir_15m  = DATA_DIR
    long_cfg.end_15m  = END_5M
    short_cfg.allow_incomplete_tail = True
    long_cfg.allow_incomplete_tail  = True
    return short_cfg, long_cfg


def _nifty_context(target_date) -> Tuple[float, float, bool, bool]:
    """Returns (rs_pct, day_move_pct, allow_long, allow_short)."""
    df = _load_parquet("NIFTYBEES")
    if df is None or df.empty:
        return 0.0, 0.0, True, True
    today = target_date if hasattr(target_date, "year") else pd.to_datetime(target_date).date()
    df_today = df[df["date"].dt.date == today].sort_values("date")
    if len(df_today) < 2:
        return 0.0, 0.0, True, True

    day_open  = float(df_today["open"].iloc[0])
    day_close = float(df_today["close"].iloc[-1])
    day_move_pct = (day_close - day_open) / day_open * 100.0 if day_open > 0 else 0.0

    # Session completeness
    first_bar = pd.Timestamp(df_today["date"].iloc[0]).floor("min")
    session_open = pd.Timestamp(IST.localize(datetime.combine(today, START_TIME))).floor("min")
    session_complete = bool(first_bar <= session_open)

    if not session_complete and NEUTRALIZE_PARTIAL_NIFTY_SESSION:
        return day_move_pct, day_move_pct, True, True

    lookback = int(NIFTY_RS_LOOKBACK_BARS)
    if len(df_today) <= lookback:
        rs_pct = day_move_pct
    else:
        c_now  = float(df_today["close"].iloc[-1])
        c_past = float(df_today["close"].iloc[-(lookback + 1)])
        rs_pct = (c_now - c_past) / c_past * 100.0 if c_past > 0 else 0.0

    if abs(day_move_pct) < float(NIFTY_CONTEXT_MIN_DAYMOVE_PCT):
        if NEUTRALIZE_WEAK_NIFTY_CONTEXT:
            return rs_pct, day_move_pct, True, True
        return rs_pct, day_move_pct, False, False

    allow_long  = rs_pct >= float(NIFTY_RS_BOTH_MODE_THRESHOLD_LONG_PCT)
    allow_short = rs_pct <= -float(NIFTY_RS_BOTH_MODE_THRESHOLD_SHORT_PCT)

    if not allow_long and not allow_short and DIRECTIONAL_NIFTY_CONTEXT_FALLBACK:
        if day_move_pct > 0:
            allow_long = True
        elif day_move_pct < 0:
            allow_short = True

    return rs_pct, day_move_pct, allow_long, allow_short


def _scan_ticker(
    ticker: str,
    target_date,
    short_cfg: StrategyConfig,
    long_cfg: StrategyConfig,
) -> Tuple[List[dict], List[dict]]:
    """Scan one ticker for today — returns (short_rows, long_rows) raw."""
    df = _load_parquet(ticker)
    if df is None or df.empty:
        return [], []

    today = target_date if hasattr(target_date, "year") else pd.to_datetime(target_date).date()
    try:
        df_prep = prepare_session_bars_for_scan(df, short_cfg)
        if df_prep is None or df_prep.empty:
            return [], []
        if "date" in df_prep.columns:
            df_prep = df_prep[pd.to_datetime(df_prep["date"]).dt.date == today]
        if df_prep.empty:
            return [], []

        short_rows, long_rows = [], []
        for trade in scan_short_prepared(ticker, df_prep, short_cfg):
            row = asdict(trade) if not isinstance(trade, dict) else trade
            row["side"] = "SHORT"
            short_rows.append(row)
        for trade in scan_long_prepared(ticker, df_prep, long_cfg):
            row = asdict(trade) if not isinstance(trade, dict) else dict(trade)
            row["side"] = "LONG"
            long_rows.append(row)
        return short_rows, long_rows
    except Exception:
        return [], []


def _row_to_label(row: dict) -> str:
    ticker = str(row.get("ticker", "")).upper()
    side   = str(row.get("side", ""))
    t_raw  = row.get("signal_time_ist", row.get("entry_time_ist", row.get("signal_datetime", "")))
    t_str  = str(t_raw)[:16] if t_raw else "?"
    qs     = _safe_float(row.get("quality_score", 0))
    rsi    = _safe_float(row.get("rsi_signal", 0))
    dist   = _safe_float(row.get("avwap_dist_atr_signal", 0))
    return f"{ticker:<16} {side:<5} {t_str}  QS={qs:.2f}  RSI={rsi:.1f}  dist={dist:.2f}"


# ---------------------------------------------------------------------------
# Main comparison
# ---------------------------------------------------------------------------
def run_comparison(target_date_str: Optional[str] = None) -> None:
    if target_date_str is None:
        target_date_str = datetime.now(IST).strftime("%Y-%m-%d")
    target_date = pd.to_datetime(target_date_str).date()
    print(f"\n{'='*72}")
    print(f"  Two-Stage V16 5min  ↔  Existing Scanner  |  Date: {target_date_str}")
    print(f"{'='*72}")

    # ── Load existing scanner signals ────────────────────────────────────────
    short_csv = LIVE_SIGNALS_DIR / f"signals_{target_date_str}_v16_5min_short.csv"
    long_csv  = LIVE_SIGNALS_DIR / f"signals_{target_date_str}_v16_5min_long.csv"
    existing_short = pd.read_csv(short_csv) if short_csv.exists() else pd.DataFrame()
    existing_long  = pd.read_csv(long_csv)  if long_csv.exists()  else pd.DataFrame()
    existing_all   = pd.concat([existing_short, existing_long], ignore_index=True)
    existing_keys: set = set()
    for _, r in existing_all.iterrows():
        ticker = str(r.get("ticker", "")).upper()
        side   = str(r.get("side", "")).upper()
        t_raw  = str(r.get("signal_entry_datetime_ist", r.get("signal_datetime", "")))
        setup  = str(r.get("setup", ""))
        ts     = base_v15._parse_ist_timestamp(t_raw)
        if ts is not None:
            existing_keys.add(base_v15._generate_signal_id(ticker, side, str(ts), setup))

    print(f"\n[EXISTING]  SHORT={len(existing_short)}  LONG={len(existing_long)}  "
          f"TOTAL={len(existing_all)}")
    if not existing_all.empty:
        for _, r in existing_all.iterrows():
            qs = _safe_float(r.get("quality_score", 0))
            print(f"  {r['ticker']:<16} {r['side']:<5} "
                  f"{str(r.get('signal_datetime',''))[:16]}  QS={qs:.4f}  "
                  f"entry={r.get('entry_price','?')}")

    # ── Build configs and Nifty context ────────────────────────────────────
    print(f"\n[NIFTY CONTEXT]")
    short_cfg, long_cfg = _build_cfgs()
    rs_pct, day_move_pct, allow_long, allow_short = _nifty_context(target_date)
    print(f"  day_move={day_move_pct:+.2f}%  rs_pct={rs_pct:+.2f}%  "
          f"allow_long={allow_long}  allow_short={allow_short}")

    # ── Scan all tickers ────────────────────────────────────────────────────
    print(f"\n[STAGE 1 — RAW SCAN]  Scanning all tickers in {DATA_DIR} ...")
    tickers = list_tickers_15m(DATA_DIR, END_5M)
    print(f"  Tickers: {len(tickers)}")

    t0 = time.perf_counter()
    raw_short_all: List[dict] = []
    raw_long_all:  List[dict] = []
    errors = 0
    for ticker in tickers:
        try:
            s, l = _scan_ticker(ticker, target_date, short_cfg, long_cfg)
            raw_short_all.extend(s)
            raw_long_all.extend(l)
        except Exception:
            errors += 1
    scan_sec = time.perf_counter() - t0
    print(f"  raw_short={len(raw_short_all)}  raw_long={len(raw_long_all)}  "
          f"errors={errors}  elapsed={scan_sec:.1f}s")

    # ── Apply RS filter ─────────────────────────────────────────────────────
    rs_pass_short = [r for r in raw_short_all if allow_short]
    rs_pass_long  = [r for r in raw_long_all  if allow_long]
    rs_block_short = len(raw_short_all) - len(rs_pass_short)
    rs_block_long  = len(raw_long_all)  - len(rs_pass_long)
    print(f"\n[RS FILTER]  short: {len(raw_short_all)} → {len(rs_pass_short)} "
          f"(-{rs_block_short} blocked)  |  "
          f"long: {len(raw_long_all)} → {len(rs_pass_long)} (-{rs_block_long} blocked)")

    # ── Apply V16 post-scan filters ─────────────────────────────────────────
    counters = {
        "pass": 0, "filtered_short_rsi": 0, "filtered_qs": 0,
        "filtered_dist_cap": 0, "filtered_dist_dead": 0,
        "filtered_vol_exhaust": 0, "filtered_or_gate": 0, "filtered_v16": 0,
    }
    two_stage_signals: List[dict] = []
    filter_log: List[Tuple[str, str, str]] = []  # (ticker, side, reason)

    for row in rs_pass_short + rs_pass_long:
        side   = str(row.get("side", "")).upper()
        ticker = str(row.get("ticker", "")).upper()
        reason = get_v16_filter_reason(row, side)
        if reason is not None:
            if "RSI" in reason:
                counters["filtered_short_rsi"] += 1
            elif "QS=" in reason:
                counters["filtered_qs"] += 1
            elif "avwap_dist_atr" in reason and "cap" in reason:
                counters["filtered_dist_cap"] += 1
            elif "avwap_dist_atr" in reason and "dead" in reason:
                counters["filtered_dist_dead"] += 1
            elif "vol_ratio" in reason:
                counters["filtered_vol_exhaust"] += 1
            elif "OR gate" in reason:
                counters["filtered_or_gate"] += 1
            else:
                counters["filtered_v16"] += 1
            filter_log.append((ticker, side, reason))
        else:
            counters["pass"] += 1
            two_stage_signals.append(row)

    total_input = len(rs_pass_short) + len(rs_pass_long)
    print(f"\n[V16 FILTERS]  in={total_input}  pass={counters['pass']}  "
          f"filtered: RSI={counters['filtered_short_rsi']}  "
          f"QS={counters['filtered_qs']}  "
          f"dist_cap={counters['filtered_dist_cap']}  "
          f"dist_dead={counters['filtered_dist_dead']}  "
          f"vol_exhaust={counters['filtered_vol_exhaust']}")

    # ── Build two-stage signal IDs ─────────────────────────────────────────
    ts_signal_ids: set = set()
    ts_deduped: List[dict] = []
    for row in two_stage_signals:
        ticker = str(row.get("ticker", "")).upper()
        side   = str(row.get("side", "")).upper()
        t_raw  = row.get("signal_time_ist",
                  row.get("entry_time_ist",
                  row.get("signal_bar_time_ist", "")))
        entry_raw = row.get("entry_time_ist", row.get("signal_entry_datetime_ist", t_raw))
        entry_ts  = base_v15._parse_ist_timestamp(str(entry_raw))
        if entry_ts is None:
            continue
        setup = str(row.get("setup", ""))
        sig_id = base_v15._generate_signal_id(ticker, side, str(entry_ts), setup)
        if sig_id in ts_signal_ids:
            continue
        ts_signal_ids.add(sig_id)
        row["_sig_id"] = sig_id
        ts_deduped.append(row)

    print(f"\n{'='*72}")
    print(f"  STAGE 2 OUTPUT (after all filters, deduped): {len(ts_deduped)} signals")
    print(f"{'='*72}")
    for row in ts_deduped:
        print(f"  {_row_to_label(row)}")

    # ── Side-by-side comparison ─────────────────────────────────────────────
    ts_ids  = {r["_sig_id"] for r in ts_deduped}
    matches     = ts_ids & existing_keys
    ts_only     = ts_ids - existing_keys
    existing_only = existing_keys - ts_ids

    print(f"\n{'='*72}")
    print(f"  COMPARISON SUMMARY")
    print(f"{'='*72}")
    print(f"  Existing scanner:  {len(existing_keys)} signals")
    print(f"  Two-stage output:  {len(ts_ids)} signals")
    print(f"  ✓ MATCH (both found):        {len(matches)}")
    print(f"  + TWO-STAGE ONLY (new):      {len(ts_only)}")
    print(f"  - EXISTING ONLY (missed):    {len(existing_only)}")

    if matches:
        print(f"\n  [MATCH] signal_ids in both:")
        for r in ts_deduped:
            if r["_sig_id"] in matches:
                print(f"    ✓ {_row_to_label(r)}")

    if ts_only:
        print(f"\n  [TWO-STAGE ONLY] — these would be new signals from two-stage:")
        for r in ts_deduped:
            if r["_sig_id"] in ts_only:
                print(f"    + {_row_to_label(r)}")

    if existing_only:
        print(f"\n  [EXISTING ONLY] — these were in existing scanner but two-stage missed them:")
        for _, er in existing_all.iterrows():
            ticker = str(er.get("ticker", "")).upper()
            side   = str(er.get("side", "")).upper()
            t_raw  = str(er.get("signal_entry_datetime_ist", er.get("signal_datetime", "")))
            setup  = str(er.get("setup", ""))
            ts_    = base_v15._parse_ist_timestamp(t_raw)
            if ts_ is None:
                continue
            sig_id = base_v15._generate_signal_id(ticker, side, str(ts_), setup)
            if sig_id in existing_only:
                qs = _safe_float(er.get("quality_score", 0))
                print(f"    - {ticker:<16} {side:<5} "
                      f"{str(er.get('signal_datetime',''))[:16]}  QS={qs:.4f}  "
                      f"entry={er.get('entry_price','?')}")
                # Check why it was missed — look in raw scan
                matched_raw = [
                    r for r in (raw_short_all + raw_long_all)
                    if str(r.get("ticker", "")).upper() == ticker
                    and str(r.get("side", "")).upper() == side
                ]
                if not matched_raw:
                    print(f"      ↳ not in raw scan (scan missed it entirely)")
                else:
                    for rr in matched_raw:
                        fr = get_v16_filter_reason(rr, side)
                        rs_ok = (allow_long if side == "LONG" else allow_short)
                        if not rs_ok:
                            print(f"      ↳ blocked by RS filter "
                                  f"(rs_pct={rs_pct:.2f}%, allow_{side.lower()}={rs_ok})")
                        elif fr:
                            print(f"      ↳ blocked by V16 filter: {fr}")
                        else:
                            print(f"      ↳ in raw scan + passes filters — "
                                  f"likely signal_id timestamp mismatch")

    # ── Filter funnel summary ────────────────────────────────────────────────
    print(f"\n  FILTER FUNNEL (Signal Engine → Detection Engine):")
    print(f"    Raw scan:          SHORT={len(raw_short_all)}  LONG={len(raw_long_all)}")
    print(f"    After RS filter:   SHORT={len(rs_pass_short)}  LONG={len(rs_pass_long)}")
    print(f"    After V16 filters: {len(ts_deduped)} (deduped)")
    if filter_log and len(filter_log) <= 20:
        print(f"    Filtered signals:")
        for (t, s, r) in filter_log:
            print(f"      {t:<16} {s:<5} → {r}")
    elif filter_log:
        print(f"    ({len(filter_log)} signals filtered — "
              f"use filter_log variable to inspect)")

    print()


if __name__ == "__main__":
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    run_comparison(date_arg)
