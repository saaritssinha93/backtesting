# -*- coding: utf-8 -*-
"""
avwap_combined_runner_v18a_5min.py
===================================
V18a = V16 backbone + Two-Session Shortlist Pre-Filter
-------------------------------------------------------

Design rationale
----------------
V16 scans all 1044 tickers for AVWAP break/reclaim patterns.
In the two-session shortlisting workflow, only ~200 tickers/session pass
the opening-momentum screen (RVOL, ORP/ATR, CLV, TV).  Running V16 signal
logic on those 200 stocks produces *fewer* entries than V16 on 1044 because:
  (a) Universe is smaller (fewer base opportunities), AND
  (b) V16's volume/RSI/ADX gates re-check attributes already confirmed by
      the shortlist  →  double-gating kills entries.

V18a fixes this with two changes:
  1. Relaxed scan parameters  — volume_min_ratio, RSI, ADX, QS thresholds
     lowered because the shortlist pre-confirms those qualities.
  2. Session-scoped shortlist filter  — after scanning all 1044 tickers,
     keep only trades where the ticker appeared in that day's morning or
     afternoon shortlist.

Expected outcome vs V16
-----------------------
  • More raw signals per shortlisted ticker (relaxed gates)
  • Quality maintained via shortlist momentum confirmation
  • Net: meaningfully more entries than V16 on 200-stock universe

Changes from V16
----------------
Scan config (applied in `main()`):
  volume_min_ratio  : 0.90 → 0.50   (shortlist checked RVOL ≥ 1.0/0.8)
  rsi_min_long      : 52.0 → 45.0   (shortlist CLV confirms direction)
  adx_min LONG      : 24.0 → 18.0   (shortlist ORP/ATR confirms momentum)
  adx_min SHORT     : 30.0 → 24.0   (small relaxation)
  quality_score_min : 4.5  → 3.5    (shortlist RVOL confirms interest)

Post-scan gates (V18a versions are labelled below):
  LONG QS dead zone lower : 7.5  → 6.5
  LONG AVWAP dist dead lo : 1.0  → 0.5   (allow closer AVWAP entries)
  LONG AVWAP dist dead hi : 1.5  → 1.0

Signal windows (extended to capture more session opportunities):
  SHORT: 09:15-11:30, 12:00-14:00   (V16: 09:15-11:00, 12:00-13:30)
  LONG : 09:15-11:30, 12:00-14:30   (V16: 09:15-11:00, 12:00-13:00)

Shortlist session assignment per trade:
  entry_time 09:15–12:30  →  check morning shortlist
  entry_time 12:00–14:30  →  check afternoon shortlist
  overlap 12:00–12:30     →  either shortlist (OR logic)

Output dir: outputs_v18a_5min/
"""

from __future__ import annotations

import os
import sys
import time
import warnings
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import asdict
from datetime import datetime, time as dtime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd
import pytz

warnings.filterwarnings("ignore")

# ---------------------------------------------------------------------------
# Re-use V16 infrastructure — scan engine, exit resolver, reporting, charts
# ---------------------------------------------------------------------------
_this_dir = Path(__file__).resolve().parent
_project_root = _this_dir.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from avwap_combined_runner_v16_5min import (
    # Core scanning
    _Tee,
    _scan_one_ticker_both,
    _finalize_side_scan_df,
    _configs_share_combined_scan_prep,
    _build_executor,
    _resolve_executor_mode,
    # Data directory resolution
    _resolve_15m_dir,
    _resolve_1min_dir,
    _load_india_vix,
    _describe_15m_dir_range,
    _describe_regime_source_availability,
    # Exit resolution
    _resolve_exits_5min,
    # Enrichment
    _enrich_with_or_levels,
    _enrich_with_entry_vol_ratio,
    # Nifty context
    _apply_nifty_intraday_context,
    _build_nifty_intraday_context,
    _replace_current_day_with_live_parity,
    # P&L helpers
    _add_notional_pnl,
    _sort_trades_for_output,
    _calc_price_return_pct,
    _apply_stop_exit_slippage,
    _materialize_exit_variant,
    # Print / reporting
    _print_day_side_mix,
    _print_signal_entry_lag_summary,
    _print_exit_realism_band,
    _print_notional_pnl,
    _print_recent_daily_breakdown,
    _build_daily_breakdown_df,
    _daily_win_stats,
    _fmt_pct_exact,
    # Portfolio sim
    _simulate_cash_constrained,
    _print_portfolio,
    # Charts
    generate_enhanced_charts,
    # Shared constants (unchanged in V18a)
    NIFTY_CONTEXT_ENABLED,
    NIFTY_CONTEXT_TICKERS,
    NIFTY_CONTEXT_OR_END_TIME,
    NIFTY_CONTEXT_CONFIRM_TIME,
    NIFTY_CONTEXT_MIN_DAYMOVE_PCT,
    NIFTY_RS_FILTER_ENABLED,
    NIFTY_RS_LOOKBACK_BARS,
    NIFTY_RS_THRESHOLD_PCT,
    NIFTY_RS_BOTH_MODE_ENABLED,
    NIFTY_RS_BOTH_MODE_THRESHOLD_LONG_PCT,
    NIFTY_RS_BOTH_MODE_THRESHOLD_SHORT_PCT,
    NIFTY_RS_BOTH_MODE_THRESHOLD_PCT,
    EXIT_REALISM_BAND_ENABLED,
    EXIT_REALISM_USE_STRESSED_BASE,
    STOP_EXIT_EXTRA_SLIPPAGE_BPS,
    V15_EOD_EXIT_TIME,
    ENABLE_CASH_CONSTRAINED_PORTFOLIO_SIM,
    ENABLE_LEGACY_CHARTS,
    ENABLE_ENHANCED_CHARTS,
    MAX_WORKERS,
    POSITION_SIZE_RS_SHORT,
    POSITION_SIZE_RS_LONG,
    INTRADAY_LEVERAGE_SHORT,
    INTRADAY_LEVERAGE_LONG,
    PORTFOLIO_START_CAPITAL_RS,
    DISALLOW_BOTH_SIDES_SAME_TICKER_DAY,
    V16_OR_GATE_ENABLED,
    V16_OR_GATE_SHORT_ENABLED,
    V16_OR_GATE_FIRST_BAR_ONLY,
    V16_OR_GATE_TIME,
    V16_LONG_ENTRY_VOL_EXHAUST_ENABLED,
    V16_LONG_ENTRY_VOL_EXHAUST_MULT,
    V16_LONG_ENTRY_VOL_EXHAUST_MIN_BARS,
    V16_SHORT_RSI_DEAD_ZONE_LO,
    V16_SHORT_RSI_DEAD_ZONE_HI,
    V16_LONG_QS_ABS_MAX,
    V16_LONG_AVWAP_DIST_ATR_MAX,
    VIX_SCALE_ENABLED,
    VIX_BASELINE,
    VIX_SCALE_MIN,
    VIX_SCALE_MAX,
    VIX_SCALE_TARGET,
    VIX_SCALE_SL,
    SHORT_LAG_BARS_A_MOD_BREAK_C1_LOW,
    SHORT_LAG_BARS_A_PULLBACK_C2_BREAK_C2_LOW,
    SHORT_LAG_BARS_B_HUGE_FAILED_BOUNCE,
    LONG_LAG_BARS_A_MOD_BREAK_C1_HIGH,
    LONG_LAG_BARS_B_HUGE_PULLBACK_HOLD_BREAK,
    LONG_LAG_BARS_B_HUGE_C1_CLOSE_RECLAIM_BREAK,
    PACK2_ENABLE_SHORT_SETUP_B_HUGE_FAILED_BOUNCE,
    PACK2_SHORT_MAX_VIX_FOR_ENTRIES,
    PACK2_LONG_MAX_VIX_FOR_ENTRIES,
    TEST_TARGET_OVERRIDE,
    TEST_SHORT_TARGET_PCT,
    TEST_LONG_TARGET_PCT,
    FORCE_LIVE_PARITY_MIN_BARS_LEFT,
    FORCE_LIVE_PARITY_DISABLE_TOPN,
    now_ist,
    runtime_dir,
    read_15m_parquet,
    list_tickers_15m,
    generate_backtest_charts,
    build_market_regime_map,
    prepare_session_bars_for_scan,
)

from avwap_v11_refactored.avwap_common_v11_v15 import (
    IST,
    StrategyConfig,
    Trade,
    BacktestMetrics,
    default_short_config,
    default_long_config,
    compute_backtest_metrics,
    print_metrics,
    trades_to_df,
    apply_topn_per_day,
)
from avwap_v11_refactored.avwap_short_strategy_v11 import (
    scan_all_days_for_ticker as scan_short,
    scan_all_days_for_ticker_prepared as scan_short_prepared,
)
from avwap_v11_refactored.avwap_long_strategy_v9_sweep import (
    scan_all_days_for_ticker as scan_long,
    scan_all_days_for_ticker_prepared as scan_long_prepared,
)
from avwap_v11_refactored.avwap_common_v7_sweep_v15 import (
    default_long_config as default_long_config_v9,
)
from eqidv2_runtime_paths import DATA_5M_DIR as RUNTIME_DATA_5M_DIR
from eqidv2_runtime_paths import DATA_1MIN_DIR as RUNTIME_DATA_1MIN_DIR
from eqidv2_runtime_paths import runtime_dir
from filtered_stocks_two_session_history import (
    ACTUAL_END_DATE as TWO_SESSION_HISTORY_END_DATE,
    ACTUAL_START_DATE as TWO_SESSION_HISTORY_START_DATE,
    get_shortlist as get_two_session_shortlist,
)


# ===========================================================================
# V18a CONFIG
# ===========================================================================

# ---- Signal windows (extended from V16) -----------------------------------
V18A_SHORT_SIGNAL_WINDOWS = [
    (dtime(9, 15, 0), dtime(11, 30, 0)),   # morning extended (+30 min vs V16)
    (dtime(12, 0, 0), dtime(14, 0, 0)),    # afternoon extended (+30 min vs V16)
]
V18A_LONG_SIGNAL_WINDOWS = [
    (dtime(9, 15, 0), dtime(11, 30, 0)),   # morning extended (+30 min vs V16)
    (dtime(12, 0, 0), dtime(14, 30, 0)),   # afternoon extended (+90 min vs V16)
]

# ---- Relaxed scan parameters (V16 values shown for comparison) ------------
V18A_LONG_RSI_MIN       = 45.0   # V16: 52.0 — shortlist CLV pre-confirms direction
V18A_LONG_ADX_MIN       = 18.0   # V16: 24.0 — shortlist ORP/ATR pre-confirms momentum
V18A_LONG_QS_MIN        = 3.5    # V16: 4.5  — shortlist RVOL pre-confirms interest
V18A_LONG_VOLUME_MIN    = 0.50   # V16: 0.90 — shortlist RVOL confirms strong volume
V18A_SHORT_ADX_MIN      = 24.0   # V16: 30.0 — moderate relaxation
V18A_SHORT_VOLUME_MIN   = 0.50   # V16: 0.90

# ---- Relaxed V16 post-scan gates for shortlisted tickers ------------------
V18A_LONG_QS_DEAD_LO         = 6.5   # V16: 7.5  — allow more QS range
V18A_LONG_AVWAP_DIST_DEAD_LO = 0.5   # V16: 1.0  — allow closer-AVWAP entries
V18A_LONG_AVWAP_DIST_DEAD_HI = 1.0   # V16: 1.5

# ---- Shortlist thresholds (same as two_session_shortlist_test.py current) -
V18A_MORNING_OBS_TIMES    = {dtime(9, 20), dtime(9, 25), dtime(9, 30)}
V18A_AFTERNOON_OBS_TIMES  = {dtime(12, 50), dtime(12, 55), dtime(13, 0)}
V18A_RVOL_LOOKBACK        = 20

V18A_MORNING_RVOL_MIN     = 1.0
V18A_MORNING_ORP_ATR_MIN  = 0.10
V18A_MORNING_CLV_MIN      = 0.62
V18A_MORNING_TV_MIN       = 3_000_000    # Rs.30L

V18A_AFTERNOON_RVOL_MIN   = 0.8
V18A_AFTERNOON_ORP_ATR_MIN = 0.06
V18A_AFTERNOON_CLV_MIN    = 0.52
V18A_AFTERNOON_TV_MIN     = 2_000_000    # Rs.20L

# ---- Session assignment boundaries ----------------------------------------
V18A_MORNING_TRADE_END      = dtime(12, 30, 0)   # morning shortlist covers up to here
V18A_AFTERNOON_TRADE_START  = dtime(12, 0, 0)    # afternoon shortlist starts here


# ===========================================================================
# SHORTLIST COMPUTATION  (adapted from two_session_shortlist_test.py)
# ===========================================================================

def _rvol_for_shortlist(
    df5: pd.DataFrame,
    today,
    obs_times: set,
) -> float:
    today_w   = df5[(df5["_date"] == today) & (df5["_time"].isin(obs_times))]
    today_vol = int(today_w["volume"].sum())
    if today_vol == 0:
        return 0.0
    past = df5[df5["_date"] < today]
    past_dates = sorted(past["_date"].unique())[-V18A_RVOL_LOOKBACK:]
    if not past_dates:
        return 1.0
    hist = [
        int(past[(past["_date"] == d) & (past["_time"].isin(obs_times))]["volume"].sum())
        for d in past_dates
    ]
    hist = [v for v in hist if v > 0]
    if not hist:
        return 1.0
    return float(today_vol / np.mean(hist))


def _shortlist_one_ticker(args: Tuple) -> dict:
    """
    Worker: compute morning + afternoon shortlist membership for all dates in one ticker.
    Returns dict keyed by date with (morning_long, morning_short, afternoon_long, afternoon_short).
    """
    ticker, path, atr14 = args
    if atr14 <= 0:
        return {}
    try:
        df = pd.read_parquet(path)
    except Exception:
        return {}

    try:
        df["date"] = pd.to_datetime(df["date"], utc=False)
        if df["date"].dt.tz is None:
            df["date"] = df["date"].dt.tz_localize(IST)
        else:
            df["date"] = df["date"].dt.tz_convert(IST)
        df = df.sort_values("date").reset_index(drop=True)
    except Exception:
        return {}

    df["_time"] = df["date"].dt.time
    df["_date"] = df["date"].dt.date

    result: dict = {}
    for today in df["_date"].unique():
        rec: dict = {}

        # ---- MORNING ----
        mw = df[(df["_date"] == today) & (df["_time"].isin(V18A_MORNING_OBS_TIMES))]
        if not mw.empty:
            mh = float(mw["high"].max())
            ml = float(mw["low"].min())
            if mh > ml:
                mc = float(mw.sort_values("_time").iloc[-1]["close"])
                mtv = float((mw["close"] * mw["volume"]).sum())
                mr = _rvol_for_shortlist(df, today, V18A_MORNING_OBS_TIMES)
                mo = (mh - ml) / atr14
                mc_long  = (mc - ml) / (mh - ml)
                mc_short = (mh - mc) / (mh - ml)
                rec["ml"] = (
                    mr  >= V18A_MORNING_RVOL_MIN and
                    mo  >= V18A_MORNING_ORP_ATR_MIN and
                    mc_long >= V18A_MORNING_CLV_MIN and
                    mtv >= V18A_MORNING_TV_MIN
                )
                rec["ms"] = (
                    mr   >= V18A_MORNING_RVOL_MIN and
                    mo   >= V18A_MORNING_ORP_ATR_MIN and
                    mc_short >= V18A_MORNING_CLV_MIN and
                    mtv  >= V18A_MORNING_TV_MIN
                )

        # ---- AFTERNOON ----
        aw = df[(df["_date"] == today) & (df["_time"].isin(V18A_AFTERNOON_OBS_TIMES))]
        if not aw.empty:
            ah = float(aw["high"].max())
            al = float(aw["low"].min())
            if ah > al:
                ac = float(aw.sort_values("_time").iloc[-1]["close"])
                atv = float((aw["close"] * aw["volume"]).sum())
                ar = _rvol_for_shortlist(df, today, V18A_AFTERNOON_OBS_TIMES)
                ao = (ah - al) / atr14
                ac_long  = (ac - al) / (ah - al)
                ac_short = (ah - ac) / (ah - al)
                rec["al"] = (
                    ar  >= V18A_AFTERNOON_RVOL_MIN and
                    ao  >= V18A_AFTERNOON_ORP_ATR_MIN and
                    ac_long >= V18A_AFTERNOON_CLV_MIN and
                    atv >= V18A_AFTERNOON_TV_MIN
                )
                rec["as_"] = (
                    ar   >= V18A_AFTERNOON_RVOL_MIN and
                    ao   >= V18A_AFTERNOON_ORP_ATR_MIN and
                    ac_short >= V18A_AFTERNOON_CLV_MIN and
                    atv  >= V18A_AFTERNOON_TV_MIN
                )

        if rec:
            result[today] = rec

    return {ticker: result}


def build_all_day_shortlists(
    dir_5m: Path,
    tickers: List[str],
    atr_cache: Dict[str, float],
    max_workers: int = MAX_WORKERS,
) -> Dict:
    """
    Pre-compute morning + afternoon shortlist membership for every (ticker, date)
    pair in the backtest dataset.

    Returns nested dict:
      shortlists[date]["morning_long"]   = set of tickers
      shortlists[date]["morning_short"]  = set of tickers
      shortlists[date]["afternoon_long"] = set of tickers
      shortlists[date]["afternoon_short"]= set of tickers
    """
    suffix = "_stocks_indicators_5min.parquet"
    task_args = [
        (t, str(dir_5m / f"{t}{suffix}"), atr_cache.get(t, 0.0))
        for t in tickers
        if (dir_5m / f"{t}{suffix}").exists()
    ]

    print(f"[V18a] Computing shortlists for {len(task_args)} tickers...")
    t0 = time.perf_counter()

    all_ticker_results: List[dict] = []
    executor_mode = _resolve_executor_mode(max_workers)

    if executor_mode == "serial" or max_workers <= 1:
        for k, args in enumerate(task_args, 1):
            res = _shortlist_one_ticker(args)
            if res:
                all_ticker_results.append(res)
            if k % 200 == 0:
                print(f"  [SHORTLIST] {k}/{len(task_args)} tickers done")
    else:
        done = 0
        with _build_executor(executor_mode, max_workers) as exc:
            futures = {exc.submit(_shortlist_one_ticker, a): a[0] for a in task_args}
            for future in as_completed(futures):
                done += 1
                try:
                    res = future.result()
                    if res:
                        all_ticker_results.append(res)
                except Exception:
                    pass
                if done % 200 == 0:
                    print(f"  [SHORTLIST] {done}/{len(task_args)} tickers done")

    # Aggregate into date-keyed structure
    shortlists: Dict = {}
    for ticker_dict in all_ticker_results:
        for ticker, date_data in ticker_dict.items():
            for date, flags in date_data.items():
                if date not in shortlists:
                    shortlists[date] = {
                        "morning_long":    set(),
                        "morning_short":   set(),
                        "afternoon_long":  set(),
                        "afternoon_short": set(),
                    }
                if flags.get("ml"):
                    shortlists[date]["morning_long"].add(ticker)
                if flags.get("ms"):
                    shortlists[date]["morning_short"].add(ticker)
                if flags.get("al"):
                    shortlists[date]["afternoon_long"].add(ticker)
                if flags.get("as_"):
                    shortlists[date]["afternoon_short"].add(ticker)

    elapsed = time.perf_counter() - t0
    n_dates = len(shortlists)
    avg_ml = np.mean([len(v["morning_long"])   for v in shortlists.values()]) if shortlists else 0
    avg_ms = np.mean([len(v["morning_short"])  for v in shortlists.values()]) if shortlists else 0
    avg_al = np.mean([len(v["afternoon_long"]) for v in shortlists.values()]) if shortlists else 0
    avg_as = np.mean([len(v["afternoon_short"])for v in shortlists.values()]) if shortlists else 0
    print(
        f"[V18a] Shortlists built in {elapsed:.1f}s | "
        f"{n_dates} trading days | "
        f"avg morning long={avg_ml:.0f}, short={avg_ms:.0f} | "
        f"avg afternoon long={avg_al:.0f}, short={avg_as:.0f}"
    )
    return shortlists


def build_atr_cache_v18a(tickers: List[str], dir_daily: Path) -> Dict[str, float]:
    """Load latest ATR_14d from daily parquets for all tickers."""
    cache: Dict[str, float] = {}
    suffix = "_stocks_indicators_daily.parquet"
    for ticker in tickers:
        path = dir_daily / f"{ticker}{suffix}"
        if not path.exists():
            continue
        try:
            df = pd.read_parquet(path)
            if "ATR" not in df.columns:
                continue
            series = df["ATR"].dropna()
            if series.empty:
                continue
            val = float(series.iloc[-1])
            if val > 0:
                cache[ticker] = val
        except Exception:
            continue
    return cache


# ===========================================================================
# V18a POST-SCAN FILTER
# ===========================================================================

def _apply_v18a_shortlist_filter(
    short_df: pd.DataFrame,
    long_df: pd.DataFrame,
    shortlists: Dict,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Keep only trades where the ticker appeared in that day's shortlist
    for the relevant session.

    Session assignment:
      entry_time 09:15–12:30  →  morning shortlist  (long or short)
      entry_time 12:00–14:30  →  afternoon shortlist
      overlap 12:00–12:30     →  either (OR logic — more permissive)
    """
    def _in_shortlist(row, session_key: str) -> bool:
        trade_date = pd.to_datetime(row.get("trade_date", pd.NaT)).date()
        day_sl = shortlists.get(trade_date)
        if day_sl is None:
            return False
        ticker = str(row.get("ticker", "")).upper()
        return ticker in day_sl.get(session_key, set())

    def _filter_df(df: pd.DataFrame, side: str) -> pd.DataFrame:
        if df.empty:
            return df
        long_key = "long" in side.lower()
        morning_key   = "morning_long"   if long_key else "morning_short"
        afternoon_key = "afternoon_long" if long_key else "afternoon_short"

        entry_times = pd.to_datetime(df["entry_time_ist"], errors="coerce")
        entry_clock = entry_times.dt.time

        keep = pd.Series(False, index=df.index)
        for idx in df.index:
            t = entry_clock[idx]
            if t is None or pd.isna(t):
                continue
            row = df.loc[idx]
            in_morning   = (t <= V18A_MORNING_TRADE_END)
            in_afternoon = (t >= V18A_AFTERNOON_TRADE_START)

            if in_morning and in_afternoon:
                # overlap zone — accept from either shortlist
                keep[idx] = _in_shortlist(row, morning_key) or _in_shortlist(row, afternoon_key)
            elif in_morning:
                keep[idx] = _in_shortlist(row, morning_key)
            elif in_afternoon:
                keep[idx] = _in_shortlist(row, afternoon_key)
            # else: outside all session windows → drop

        return df[keep].copy()

    before_short = len(short_df)
    before_long  = len(long_df)
    short_df = _filter_df(short_df, "short")
    long_df  = _filter_df(long_df, "long")
    print(
        f"[V18a_SHORTLIST] SHORT: {before_short}→{len(short_df)} "
        f"(-{before_short - len(short_df)} not in shortlist) | "
        f"LONG: {before_long}→{len(long_df)} "
        f"(-{before_long - len(long_df)} not in shortlist)"
    )
    return short_df, long_df


# ===========================================================================
# V18a POST-SCAN QUALITY GATES (relaxed from V16)
# ===========================================================================

def _apply_v18a_post_scan_filters(
    short_df: pd.DataFrame,
    long_df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    V16 anti-exhaustion gates with V18a relaxations.

    SHORT: same RSI dead zone as V16 (35-40)
    LONG:  QS dead zone lower bound relaxed 7.5→6.5
           AVWAP dead zone relaxed 1.0-1.5 → 0.5-1.0
           AVWAP dist cap unchanged (3.0 ATR max)
           Volume exhaustion filter unchanged
    """
    short_before = len(short_df)
    long_before  = len(long_df)

    # SHORT: RSI dead zone (same as V16)
    short_rsi_removed = 0
    if not short_df.empty and "rsi_signal" in short_df.columns:
        rsi_col = pd.to_numeric(short_df["rsi_signal"], errors="coerce").fillna(-1.0)
        mask = (rsi_col >= V16_SHORT_RSI_DEAD_ZONE_LO) & (rsi_col < V16_SHORT_RSI_DEAD_ZONE_HI)
        short_rsi_removed = int(mask.sum())
        short_df = short_df[~mask].copy()

    # LONG: QS two-band filter (relaxed dead zone lower bound)
    long_qs_removed = 0
    if not long_df.empty and "quality_score" in long_df.columns:
        qs_col = pd.to_numeric(long_df["quality_score"], errors="coerce").fillna(0.0)
        mask = (
            ((qs_col > V18A_LONG_QS_DEAD_LO) & (qs_col <= 8.0))   # relaxed dead zone
            | (qs_col > V16_LONG_QS_ABS_MAX)                        # exhaustion cap unchanged
        )
        long_qs_removed = int(mask.sum())
        long_df = long_df[~mask].copy()

    # LONG: AVWAP dist cap (unchanged from V16)
    long_dist_removed = 0
    if not long_df.empty and "avwap_dist_atr_signal" in long_df.columns:
        dist_col = pd.to_numeric(long_df["avwap_dist_atr_signal"], errors="coerce").fillna(0.0)
        mask = (dist_col > 0) & (dist_col > V16_LONG_AVWAP_DIST_ATR_MAX)
        long_dist_removed = int(mask.sum())
        long_df = long_df[~mask].copy()

    # LONG: AVWAP dead zone (relaxed: 0.5-1.0 ATR instead of 1.0-1.5)
    long_avwap_dead_removed = 0
    if not long_df.empty and "avwap_dist_atr_signal" in long_df.columns:
        dist_col2 = pd.to_numeric(long_df["avwap_dist_atr_signal"], errors="coerce").fillna(0.0)
        mask = (dist_col2 >= V18A_LONG_AVWAP_DIST_DEAD_LO) & (dist_col2 < V18A_LONG_AVWAP_DIST_DEAD_HI)
        long_avwap_dead_removed = int(mask.sum())
        long_df = long_df[~mask].copy()

    # LONG: Volume exhaustion filter (unchanged from V16)
    long_vol_exhaust_removed = 0
    if V16_LONG_ENTRY_VOL_EXHAUST_ENABLED and not long_df.empty and "entry_bar_vol_ratio" in long_df.columns:
        vr_col  = pd.to_numeric(long_df["entry_bar_vol_ratio"], errors="coerce")
        bfo_col = pd.to_numeric(long_df["bars_from_open"], errors="coerce")
        mask = (
            vr_col.notna()
            & (vr_col > V16_LONG_ENTRY_VOL_EXHAUST_MULT)
            & (bfo_col >= V16_LONG_ENTRY_VOL_EXHAUST_MIN_BARS)
        )
        long_vol_exhaust_removed = int(mask.sum())
        long_df = long_df[~mask].copy()

    print(
        f"[V18a_FILTER] SHORT: {short_before}→{len(short_df)} "
        f"(-{short_rsi_removed} RSI dead zone) | "
        f"LONG: {long_before}→{len(long_df)} "
        f"(-{long_qs_removed} QS dead+exhaust, "
        f"-{long_dist_removed} dist>3ATR, "
        f"-{long_avwap_dead_removed} AVWAP dead 0.5-1.0ATR, "
        f"-{long_vol_exhaust_removed} vol exhaust)"
    )
    return short_df, long_df


# ===========================================================================
# PARALLEL SCAN (same as V16 but uses V18a configs passed in)
# ===========================================================================

def _run_both_v18a(
    short_cfg: StrategyConfig,
    long_cfg: StrategyConfig,
    max_workers: int = MAX_WORKERS,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Parallel combined scan — same as V16's _run_both_parallel."""
    tickers = list_tickers_15m(short_cfg.dir_15m, short_cfg.end_15m)
    print(f"[COMBINED] Tickers found: {len(tickers)}")

    share_prep = _configs_share_combined_scan_prep(short_cfg, long_cfg)
    task_args = [
        (t, os.path.join(short_cfg.dir_15m, f"{t}{short_cfg.end_15m}"),
         short_cfg, long_cfg, share_prep)
        for t in tickers
    ]

    short_dicts: List[dict] = []
    long_dicts:  List[dict] = []
    scan_errors: List[Tuple[str, str]] = []
    executor_mode = _resolve_executor_mode(max_workers)

    if executor_mode == "serial":
        for k, args in enumerate(task_args, 1):
            try:
                sr, lr = _scan_one_ticker_both(args)
                short_dicts.extend(sr)
                long_dicts.extend(lr)
            except Exception as exc:
                scan_errors.append((args[0], str(exc)))
            if k % 50 == 0:
                print(f"  [COMBINED] {k}/{len(tickers)} | short={len(short_dicts)} long={len(long_dicts)}")
    else:
        done = 0
        print(f"  [COMBINED] executor={executor_mode} | workers={max_workers}")
        with _build_executor(executor_mode, max_workers) as executor:
            futures = {executor.submit(_scan_one_ticker_both, a): a[0] for a in task_args}
            for future in as_completed(futures):
                done += 1
                try:
                    sr, lr = future.result()
                    short_dicts.extend(sr)
                    long_dicts.extend(lr)
                except Exception as exc:
                    scan_errors.append((futures[future], str(exc)))
                if done % 100 == 0:
                    print(f"  [COMBINED] {done}/{len(tickers)} | short={len(short_dicts)} long={len(long_dicts)}")

    if scan_errors:
        print(f"  [COMBINED] {len(scan_errors)} tickers skipped: {', '.join(t for t, _ in scan_errors[:10])}")

    short_df = _finalize_side_scan_df(pd.DataFrame(short_dicts) if short_dicts else pd.DataFrame(), short_cfg)
    long_df  = _finalize_side_scan_df(pd.DataFrame(long_dicts)  if long_dicts  else pd.DataFrame(), long_cfg)
    return short_df, long_df


# ===========================================================================
# V18a MAIN
# ===========================================================================

def main() -> None:
    _script_dir = Path(__file__).resolve().parent
    _project_root = _script_dir if _script_dir.name != "avwap_v11_refactored" else _script_dir.parent
    _outputs_dir = runtime_dir("outputs_v18a_5min")
    _outputs_dir.mkdir(parents=True, exist_ok=True)

    ts = now_ist().strftime("%Y%m%d_%H%M%S")
    log_path = _outputs_dir / f"avwap_combined_runner_v18a_{ts}.txt"

    _orig_stdout, _orig_stderr = sys.stdout, sys.stderr
    with open(log_path, "w", encoding="utf-8") as _log_fh:
        sys.stdout = _Tee(_orig_stdout, _log_fh)
        sys.stderr = _Tee(_orig_stderr, _log_fh)

        try:
            run_started = time.perf_counter()
            print("=" * 72)
            print("AVWAP v18a 5min  --  V16 + Two-Session Shortlist Pre-Filter")
            print("  Scan       : all 1044 tickers, relaxed V18a gates")
            print("  Post-filter: keep only shortlisted ticker-days")
            print("  Entry signals: 5-min | Exit: 1-min (5-min fallback)")
            print("=" * 72)

            # ---- Resolve data dirs ----
            dir_15m = _resolve_15m_dir()
            print(f"[INFO] 5min dir: {dir_15m}")
            n5 = len(list(dir_15m.glob("*_stocks_indicators_5min.parquet")))
            print(f"[INFO] 5min parquets: {n5}")
            ds, de, dsrc = _describe_15m_dir_range(dir_15m)
            if ds and de:
                print(f"[INFO] Coverage: {ds.strftime('%Y-%m-%d')} → {de.strftime('%Y-%m-%d')} ({dsrc})")

            dir_1m = _resolve_1min_dir()
            print(f"[INFO] 1min dir: {dir_1m}")

            # ---- Daily ATR for shortlist computation ----
            _runtime_root = Path(r"C:\TradingData\eqidv2")
            _daily_dir = _runtime_root / "stocks_indicators_daily_eq"
            if not _daily_dir.is_dir():
                _daily_dir = _script_dir / "stocks_indicators_daily_eq"

            tickers_all = list_tickers_15m(dir_15m, "_stocks_indicators_5min.parquet")
            print(f"[INFO] Tickers in scan universe: {len(tickers_all)}")

            print(f"[INFO] Building ATR cache from {_daily_dir} ...")
            atr_cache = build_atr_cache_v18a(tickers_all, _daily_dir)
            print(f"[INFO] ATR cache: {len(atr_cache)}/{len(tickers_all)} loaded")

            # ---- Build per-day shortlists ----
            print("\n[PHASE 0] Building per-day shortlists...")
            p0_start = time.perf_counter()
            shortlists = build_all_day_shortlists(dir_15m, tickers_all, atr_cache, MAX_WORKERS)
            print(f"[TIMING] Phase 0: {time.perf_counter() - p0_start:.1f}s")

            # ---- Build scan configs (V18a relaxations) ----
            short_cfg = default_short_config(reports_dir=_outputs_dir)
            long_cfg  = default_long_config_v9(reports_dir=_outputs_dir)
            short_cfg.dir_15m = str(dir_15m)
            long_cfg.dir_15m  = str(dir_15m)
            short_cfg.end_15m = "_stocks_indicators_5min.parquet"
            long_cfg.end_15m  = "_stocks_indicators_5min.parquet"
            short_cfg.market_regime_tickers = tuple(NIFTY_CONTEXT_TICKERS)
            long_cfg.market_regime_tickers  = tuple(NIFTY_CONTEXT_TICKERS)

            # V16 base profile
            short_cfg.enable_liquidity_sweep_filter   = False
            short_cfg.reversal_requires_sweep          = True
            short_cfg.enable_avwap_no_trade_zone       = False
            short_cfg.enable_mode_selector             = True
            short_cfg.use_prev_close_for_day_mode      = False
            short_cfg.use_time_windows                 = False
            short_cfg.min_bars_left_after_entry        = 0
            short_cfg.enable_ema200_filter             = False
            short_cfg.require_vwap_side_persistence    = False
            short_cfg.vwap_side_lookback_bars          = 5
            short_cfg.vwap_side_min_count              = 3
            short_cfg.require_structure_filter         = False
            short_cfg.structure_lookback_bars          = 30
            short_cfg.mod_impulse_min_atr              = 0.30
            short_cfg.avwap_min_consec_closes          = 1
            short_cfg.rsi_max_short                    = 58.0
            short_cfg.stochk_max                       = 90.0
            short_cfg.stop_pct                         = 0.0075
            short_cfg.target_pct                       = 0.00800
            short_cfg.be_trigger_pct                   = 0.0042
            short_cfg.trail_pct                        = 0.0023
            short_cfg.enable_partial_exit              = False
            short_cfg.enable_risk_based_position_sizing = False
            short_cfg.risk_per_trade_pct_of_capital    = 0.0035
            short_cfg.max_trades_per_ticker_per_day    = 6
            short_cfg.enable_topn_per_day              = False
            short_cfg.topn_per_day                     = 0
            short_cfg.entry_time_cutoff                = dtime(14, 0, 0)  # extended
            short_cfg.min_opening_range_width_pct      = 1.00
            short_cfg.signal_avwap_dist_atr_max        = 2.10

            long_cfg.require_entry_close_confirm       = True
            long_cfg.enable_liquidity_sweep_filter     = False
            long_cfg.enable_avwap_no_trade_zone        = False
            long_cfg.adx_slope_min                     = 0.50
            long_cfg.avwap_min_consec_closes           = 1
            long_cfg.stochk_min                        = 15.0
            long_cfg.stochk_max                        = 95.0
            long_cfg.atr_pct_min                       = 0.0025
            long_cfg.enable_setup_a_pullback_c2_break                  = True
            long_cfg.enable_setup_a_close_continuation_break           = True
            long_cfg.enable_setup_b_huge_c1_close_reclaim_break        = True
            long_cfg.stop_pct                          = 0.0075
            long_cfg.target_pct                        = 0.00800
            long_cfg.be_trigger_pct                    = 0.0055
            long_cfg.trail_pct                         = 0.0028
            long_cfg.min_bars_left_after_entry         = 0
            long_cfg.max_vix_for_entries               = 13.0
            long_cfg.max_trades_per_ticker_per_day     = 5
            long_cfg.enable_topn_per_day               = False
            long_cfg.topn_per_day                      = 0

            # ---- V18a RELAXATIONS applied here ----
            short_cfg.adx_min            = V18A_SHORT_ADX_MIN      # 30.0 → 24.0
            short_cfg.volume_min_ratio   = V18A_SHORT_VOLUME_MIN    # 0.90 → 0.50
            long_cfg.adx_min             = V18A_LONG_ADX_MIN        # 24.0 → 18.0
            long_cfg.volume_min_ratio    = V18A_LONG_VOLUME_MIN     # 0.90 → 0.50
            long_cfg.rsi_min_long        = V18A_LONG_RSI_MIN        # 52.0 → 45.0
            long_cfg.quality_score_min   = V18A_LONG_QS_MIN         # 4.5  → 3.5

            # Lag bars
            short_cfg.lag_bars_short_a_mod_break_c1_low             = SHORT_LAG_BARS_A_MOD_BREAK_C1_LOW
            short_cfg.lag_bars_short_a_pullback_c2_break_c2_low     = SHORT_LAG_BARS_A_PULLBACK_C2_BREAK_C2_LOW
            short_cfg.lag_bars_short_b_huge_failed_bounce            = SHORT_LAG_BARS_B_HUGE_FAILED_BOUNCE
            short_cfg.enable_setup_b_huge_failed_bounce              = PACK2_ENABLE_SHORT_SETUP_B_HUGE_FAILED_BOUNCE
            short_cfg.max_vix_for_entries                            = float(PACK2_SHORT_MAX_VIX_FOR_ENTRIES)
            long_cfg.max_vix_for_entries                             = float(PACK2_LONG_MAX_VIX_FOR_ENTRIES)
            long_cfg.lag_bars_long_a_mod_break_c1_high              = LONG_LAG_BARS_A_MOD_BREAK_C1_HIGH
            long_cfg.lag_bars_long_a_close_continuation_break        = 2
            long_cfg.lag_bars_long_a_pullback_c2_break_c2_high       = 1
            long_cfg.lag_bars_long_b_huge_pullback_hold_break        = LONG_LAG_BARS_B_HUGE_PULLBACK_HOLD_BREAK
            long_cfg.lag_bars_long_b_huge_c1_close_reclaim_break     = LONG_LAG_BARS_B_HUGE_C1_CLOSE_RECLAIM_BREAK

            if FORCE_LIVE_PARITY_MIN_BARS_LEFT:
                short_cfg.min_bars_left_after_entry = 0
                long_cfg.min_bars_left_after_entry  = 0
            if FORCE_LIVE_PARITY_DISABLE_TOPN:
                short_cfg.enable_topn_per_day = False
                long_cfg.enable_topn_per_day  = False

            # V18a extended signal windows
            short_cfg.use_time_windows = True
            long_cfg.use_time_windows  = True
            short_cfg.signal_windows   = list(V18A_SHORT_SIGNAL_WINDOWS)
            long_cfg.signal_windows    = list(V18A_LONG_SIGNAL_WINDOWS)

            if TEST_TARGET_OVERRIDE:
                short_cfg.target_pct = TEST_SHORT_TARGET_PCT
                long_cfg.target_pct  = TEST_LONG_TARGET_PCT

            # VIX scaling
            _vix_map = _load_india_vix(_project_root)
            short_cfg.vix_scale_enabled = VIX_SCALE_ENABLED
            long_cfg.vix_scale_enabled  = VIX_SCALE_ENABLED
            if VIX_SCALE_ENABLED and _vix_map:
                for cfg in (short_cfg, long_cfg):
                    cfg.vix_daily = _vix_map
                    cfg.vix_baseline = VIX_BASELINE
                    cfg.vix_scale_min = VIX_SCALE_MIN
                    cfg.vix_scale_max = VIX_SCALE_MAX
                    cfg.vix_scale_target = VIX_SCALE_TARGET
                    cfg.vix_scale_sl = VIX_SCALE_SL

            # Regime map
            regime_map, regime_source = build_market_regime_map(short_cfg)
            if regime_map:
                short_cfg.market_regime_map          = regime_map
                long_cfg.market_regime_map           = regime_map
                short_cfg.enable_market_regime_filter = True
                long_cfg.enable_market_regime_filter  = True
                print(f"[REGIME] {len(regime_map)} bars | source={regime_source}")
            else:
                short_cfg.enable_market_regime_filter = False
                long_cfg.enable_market_regime_filter  = False
                print("[REGIME] Disabled — no market index parquet found.")

            print(
                f"[V18a] Key changes: "
                f"LONG RSI {V18A_LONG_RSI_MIN} | ADX {V18A_LONG_ADX_MIN} | "
                f"QS_min {V18A_LONG_QS_MIN} | vol_min {V18A_LONG_VOLUME_MIN} | "
                f"QS_dead_lo {V18A_LONG_QS_DEAD_LO} | "
                f"AVWAP_dead {V18A_LONG_AVWAP_DIST_DEAD_LO}-{V18A_LONG_AVWAP_DIST_DEAD_HI}ATR"
            )
            print(
                f"[V18a] Signal windows: "
                f"SHORT {V18A_SHORT_SIGNAL_WINDOWS} | "
                f"LONG {V18A_LONG_SIGNAL_WINDOWS}"
            )
            print("-" * 72)

            # ================================================================
            # PHASE 1 — Scan all tickers with V18a relaxed configs
            # ================================================================
            print("\n[PHASE 1] Scanning all tickers (V18a relaxed gates)...")
            p1_start = time.perf_counter()
            short_df, long_df = _run_both_v18a(short_cfg, long_cfg, MAX_WORKERS)
            print(f"  Raw scan results: SHORT={len(short_df)}  LONG={len(long_df)}")

            # Nifty context filter (same as V16)
            if NIFTY_CONTEXT_ENABLED:
                mode_map, nifty_ret_map, context_src, context_counts = _build_nifty_intraday_context(short_cfg)
                if mode_map:
                    print(
                        f"[NIFTY] {context_src} | "
                        f"LONG_ONLY={context_counts.get('LONG_ONLY',0)}, "
                        f"SHORT_ONLY={context_counts.get('SHORT_ONLY',0)}, "
                        f"BOTH={context_counts.get('BOTH',0)}"
                    )
                    short_df, long_df = _apply_nifty_intraday_context(
                        short_df, long_df, short_cfg, mode_map, nifty_ret_map
                    )
                else:
                    print("[NIFTY] No NIFTY parquet — skipped.")

            short_df, long_df = _replace_current_day_with_live_parity(short_df, long_df)

            # OR gate enrichment (disabled by default in V16)
            if V16_OR_GATE_ENABLED:
                short_df, long_df = _enrich_with_or_levels(
                    short_df, long_df, dir_15m=str(dir_15m),
                    parquet_suffix=short_cfg.end_15m,
                )

            # Volume exhaustion enrichment
            if V16_LONG_ENTRY_VOL_EXHAUST_ENABLED and not long_df.empty:
                long_df = _enrich_with_entry_vol_ratio(
                    long_df, dir_15m=str(dir_15m), parquet_suffix=short_cfg.end_15m,
                )

            # V18a post-scan quality gates (relaxed from V16)
            print("\n[PHASE 1b] Applying V18a relaxed quality gates...")
            short_df, long_df = _apply_v18a_post_scan_filters(short_df, long_df)

            # ================================================================
            # PHASE 1c — Shortlist filter (the core V18a step)
            # ================================================================
            print("\n[PHASE 1c] Applying session shortlist filter...")
            short_df, long_df = _apply_v18a_shortlist_filter(short_df, long_df, shortlists)

            print(f"[TIMING] Phase 1 total: {time.perf_counter() - p1_start:.1f}s")
            print(f"  After all filters: SHORT={len(short_df)}  LONG={len(long_df)}")

            if short_df.empty and long_df.empty:
                print("[DONE] No trades survived all filters.")
                return

            # ================================================================
            # PHASE 2 — Re-resolve exits
            # ================================================================
            print("\n[PHASE 2] Re-resolving exits with 1-min data...")
            p2_start = time.perf_counter()
            suffix_5m = ".parquet"
            if dir_1m.is_dir():
                for sf in list(dir_1m.glob("*"))[:5]:
                    if sf.suffix:
                        suffix_5m = sf.suffix
                        break

            if not short_df.empty:
                short_df = _resolve_exits_5min(
                    short_df, dir_1m, suffix_5m,
                    short_cfg.parquet_engine, eod_exit_time=V15_EOD_EXIT_TIME,
                )
            if not long_df.empty:
                long_df = _resolve_exits_5min(
                    long_df, dir_1m, suffix_5m,
                    long_cfg.parquet_engine, eod_exit_time=V15_EOD_EXIT_TIME,
                )
            print(f"[TIMING] Phase 2: {time.perf_counter() - p2_start:.1f}s")

            # ================================================================
            # PHASE 3 — Metrics + outputs
            # ================================================================
            if not short_df.empty:
                short_df = _add_notional_pnl(short_df)
                short_df = _sort_trades_for_output(short_df)
            if not long_df.empty:
                long_df = _add_notional_pnl(long_df)
                long_df = _sort_trades_for_output(long_df)

            combined = pd.concat([short_df, long_df], ignore_index=True)
            combined = _add_notional_pnl(combined)
            combined = _sort_trades_for_output(combined)
            short_df = _sort_trades_for_output(
                combined[combined["side"].astype(str).str.upper().eq("SHORT")].copy()
            )
            long_df = _sort_trades_for_output(
                combined[combined["side"].astype(str).str.upper().eq("LONG")].copy()
            )
            _print_day_side_mix(combined)
            _print_signal_entry_lag_summary(combined)

            print_metrics("SHORT (V18a)", compute_backtest_metrics(short_df))
            print_metrics("LONG  (V18a)", compute_backtest_metrics(long_df))
            print_metrics("COMBINED (V18a)", compute_backtest_metrics(combined))

            if EXIT_REALISM_BAND_ENABLED:
                _print_exit_realism_band("SHORT",    short_df)
                _print_exit_realism_band("LONG",     long_df)
                _print_exit_realism_band("COMBINED", combined)

            _print_notional_pnl(combined)
            _print_recent_daily_breakdown(combined, n_weeks=4)

            # Per-session breakdown
            print("\n[V18a] MORNING SESSION BREAKDOWN (entry 09:15-12:30):")
            et = pd.to_datetime(combined["entry_time_ist"], errors="coerce").dt.time
            morning_mask = et <= V18A_MORNING_TRADE_END
            afternoon_mask = et >= V18A_AFTERNOON_TRADE_START
            morning_df   = combined[morning_mask & ~afternoon_mask].copy()
            afternoon_df = combined[afternoon_mask & ~morning_mask].copy()
            overlap_df   = combined[morning_mask & afternoon_mask].copy()
            print(f"  Morning (09:15-11:59):   {len(morning_df)} trades")
            if not morning_df.empty:
                m_met = compute_backtest_metrics(morning_df)
                dw_m, wd_m, td_m = _daily_win_stats(morning_df)
                m_win = (
                    pd.to_numeric(morning_df["pnl_pct"], errors="coerce").gt(0).mean() * 100.0
                    if "pnl_pct" in morning_df.columns else 0.0
                )
                print(f"    PF={m_met.profit_factor:.3f} | Win={m_win:.1f}% | DayWin={dw_m:.1f}% ({wd_m}/{td_m})")
            print(f"  Afternoon (13:00-14:30): {len(afternoon_df)} trades")
            if not afternoon_df.empty:
                a_met = compute_backtest_metrics(afternoon_df)
                dw_a, wd_a, td_a = _daily_win_stats(afternoon_df)
                a_win = (
                    pd.to_numeric(afternoon_df["pnl_pct"], errors="coerce").gt(0).mean() * 100.0
                    if "pnl_pct" in afternoon_df.columns else 0.0
                )
                print(f"    PF={a_met.profit_factor:.3f} | Win={a_win:.1f}% | DayWin={dw_a:.1f}% ({wd_a}/{td_a})")
            print(f"  Overlap (12:00-12:30):   {len(overlap_df)} trades")

            # Save CSV
            out_csv = _outputs_dir / f"avwap_longshort_trades_v18a_5min_ALL_DAYS_{ts}.csv"
            combined.to_csv(out_csv, index=False)
            day_csv = _outputs_dir / f"avwap_daywise_v18a_5min_ALL_DAYS_{ts}.csv"
            _build_daily_breakdown_df(combined, include_total=True).to_csv(day_csv, index=False)
            print(f"\n[SAVED] {out_csv.name}")
            print(f"[SAVED] {day_csv.name}")

            # Charts
            if ENABLE_ENHANCED_CHARTS:
                chart_dir = _outputs_dir / "charts" / "enhanced"
                cfiles = generate_enhanced_charts(
                    combined, short_df, long_df,
                    save_dir=chart_dir, ts_label=ts,
                )
                if cfiles:
                    print(f"[CHARTS] {len(cfiles)} charts → {chart_dir}/")

            elapsed = time.perf_counter() - run_started
            print(f"\n[DONE] V18a total run time: {elapsed:.1f}s  |  log: {log_path.name}")

        finally:
            sys.stdout = _orig_stdout
            sys.stderr = _orig_stderr


if __name__ == "__main__":
    main()
