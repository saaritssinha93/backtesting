# -*- coding: utf-8 -*-
"""
avwap_combined_runner_v4.py — AVWAP v11 COMBINED LONG + SHORT with anti-chase (v4)
====================================================================================

Upgraded from avwap_combined_runner_v3.py with the following improvements:

ANTI-CHASE ENTRY MODEL (LONG):
  - Switched default model: limit_retrace (-0.5%, 60min wait)
      →  next_open_guard  (cap 0.8% above signal price)
    This significantly increases LONG fill rate vs v3.
  - Alternative: limit_retrace with tighter offset=-0.2%  wait=30min

SHORT SIDE:
  - RUN_SHORT_SIDE = True  (was False in v3)
    SHORT signals use the v2 runner's parallel scan + 5-min exit resolution.

RISK / R:R (improved over v3):
  - LONG  SL 0.60%  (keep) | LONG  TGT 1.80% → 2.00%  →  R:R 3.33:1
  - SHORT SL 0.75%          | SHORT TGT 1.50%           →  R:R 2.0:1
    (SHORT risk inherited from v2 runner configs)

ENTRY MODEL OPTIONS (LONG_ENTRY_MODEL):
  "next_open_guard" — enter at next bar's open if gap ≤ LONG_CHASE_CAP_PCT
  "limit_retrace"   — place limit at signal_entry*(1+LONG_LIMIT_OFFSET_PCT),
                      wait up to LONG_LIMIT_WAIT_MIN minutes for a fill
  "signal_entry"    — use signal bar's close directly (maximum entries, no anti-chase)

All shared infrastructure imported from avwap_combined_runner_v2 (v2 runner).
Outputs saved to  outputs_v4/  (separate from v1/v2/v3 outputs).
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# Ensure project root importability when run directly
_THIS_DIR = Path(__file__).resolve().parent
if str(_THIS_DIR) not in sys.path:
    sys.path.insert(0, str(_THIS_DIR))

# Core common (IST, metrics, config factories, charts)
from avwap_v11_refactored.avwap_common import (  # noqa: E402
    IST,
    compute_backtest_metrics,
    default_long_config,
    default_short_config,
    generate_backtest_charts,
    now_ist,
    print_metrics,
)

# Shared utilities from the v2 runner (avoid duplication)
from avwap_combined_runner_v2 import (  # noqa: E402
    _Tee,
    _add_notional_pnl,
    _print_day_side_mix,
    _print_notional_pnl,
    _print_signal_entry_lag_summary,
    _resolve_5min_dir,
    _resolve_exits_5min,
    _run_side_parallel,
    _sort_trades_for_output,
    generate_enhanced_charts,
    read_5m_parquet,
    MAX_WORKERS as _V2_MAX_WORKERS,
)


# ===========================================================================
# v4 RUN CONFIG
# ===========================================================================
RUN_SHORT_SIDE = True    # enabled (was False in v3)
RUN_LONG_SIDE  = True
MAX_WORKERS    = _V2_MAX_WORKERS   # inherit from v2 (4)

# ---- LONG anti-chase entry model ----
# "next_open_guard"  → most entries, realistic (recommended)
# "limit_retrace"    → fewer entries but better average fill price
# "signal_entry"     → maximum entries, no price guard
LONG_ENTRY_MODEL       = "next_open_guard"
LONG_CHASE_CAP_PCT     = 0.0040    # best sweep: cap up to 0.4% above signal
LONG_LIMIT_OFFSET_PCT  = -0.0060   # best sweep companion setting
LONG_LIMIT_WAIT_MIN    = 60        # best sweep companion setting

# ---- v4 LONG risk model (rebuilt from executed entry) ----
LONG_STOP_PCT_V4   = 0.0060   # 0.60%
LONG_TARGET_PCT_V4 = 0.0250   # 2.50%

# ---- v4 SHORT risk (same as v2 optimised) ----
SHORT_STOP_PCT_V4   = 0.0055  # 0.55%
SHORT_TARGET_PCT_V4 = 0.0150  # 1.50%

# ---- Optional LONG signal filters (None = disabled, inherited from base scan) ----
LONG_RSI_CAP:     Optional[float] = None
LONG_ADX_MIN_V4:  Optional[float] = None
LONG_QUALITY_MIN: Optional[float] = None


# ===========================================================================
# IST helpers
# ===========================================================================
def _to_ist_series(series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(series, errors="coerce")
    try:
        if getattr(dt.dt, "tz", None) is None:
            return dt.dt.tz_localize(IST)
        return dt.dt.tz_convert(IST)
    except Exception:
        return dt


def _to_ist_scalar(value: Any) -> pd.Timestamp:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return pd.NaT
    if ts.tzinfo is None:
        return ts.tz_localize(IST)
    return ts.tz_convert(IST)


def _pick_col(df: pd.DataFrame, *candidates: str) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _apply_short_risk_override(
    short_df: pd.DataFrame,
    short_stop_pct: float,
    short_target_pct: float,
) -> pd.DataFrame:
    """Rebuild SHORT stop/target prices before 5-min exit re-resolution."""
    if short_df.empty:
        return short_df

    d = short_df.copy()
    entry = pd.to_numeric(d.get("entry_price"), errors="coerce")
    d["stop_price"] = entry * (1.0 + float(short_stop_pct))
    d["sl_price"] = d["stop_price"]
    d["target_price"] = entry * (1.0 - float(short_target_pct))
    return d


# ===========================================================================
# 5-MIN DATA LOADER (with per-ticker cache)
# ===========================================================================
def _load_5m_for_ticker(
    ticker: str,
    dir_5m: Path,
    suffix_5m: str,
    engine: str,
    cache: Dict[str, pd.DataFrame],
) -> pd.DataFrame:
    if ticker in cache:
        return cache[ticker]

    found = None
    for pattern in [
        f"{ticker}{suffix_5m}",
        f"{ticker}.parquet",
        f"{ticker}_5min.parquet",
        f"{ticker}_stocks_indicators_5min.parquet",
    ]:
        p = dir_5m / pattern
        if p.exists():
            found = p
            break

    if found is None:
        cache[ticker] = pd.DataFrame()
        return cache[ticker]

    df_5m = read_5m_parquet(str(found), engine=engine)
    if df_5m.empty:
        cache[ticker] = df_5m
        return df_5m

    dt_col = _pick_col(df_5m, "datetime", "date", "DateTime", "timestamp", "Timestamp")
    if dt_col is None:
        cache[ticker] = pd.DataFrame()
        return cache[ticker]

    if dt_col != "datetime":
        df_5m = df_5m.rename(columns={dt_col: "datetime"})

    df_5m["datetime"] = _to_ist_series(df_5m["datetime"])
    df_5m = df_5m.dropna(subset=["datetime"]).sort_values("datetime").reset_index(drop=True)
    cache[ticker] = df_5m
    return df_5m


# ===========================================================================
# LONG ANTI-CHASE ENTRY MODEL (v4)
# ===========================================================================
def _apply_long_entry_model_v4(
    long_df: pd.DataFrame,
    dir_5m: Path,
    suffix_5m: str = ".parquet",
    engine: str = "pyarrow",
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Rebuild LONG entries using the configured anti-chase model.

    Models:
      next_open_guard  — accept next bar open if gap ≤ LONG_CHASE_CAP_PCT
      limit_retrace    — fill at signal_entry * (1 + LONG_LIMIT_OFFSET_PCT)
                         within LONG_LIMIT_WAIT_MIN minutes
      signal_entry     — use signal bar close directly (no guard)

    Returns:
      filled_df  — trades that got a valid entry
      skip_df    — trades that were rejected with reason
    """
    if long_df.empty:
        return long_df, pd.DataFrame()

    if not dir_5m.is_dir():
        print(f"[V4][WARN] 5-min dir missing: {dir_5m}. LONG anti-chase skipped.")
        return long_df, pd.DataFrame()

    df = long_df.copy()
    if "stop_price" not in df.columns and "sl_price" in df.columns:
        df["stop_price"] = df["sl_price"]

    for c in ["signal_time_ist", "entry_time_ist", "exit_time_ist"]:
        if c in df.columns:
            df[c] = _to_ist_series(df[c])

    out_rows: List[Dict[str, Any]] = []
    skip_rows: List[Dict[str, Any]] = []
    cache_5m: Dict[str, pd.DataFrame] = {}

    for row in df.to_dict("records"):
        ticker = str(row.get("ticker", "")).upper()
        side = str(row.get("side", "")).upper()
        if side != "LONG":
            out_rows.append(row)
            continue

        signal_time  = _to_ist_scalar(row.get("signal_time_ist"))
        signal_entry = float(pd.to_numeric(row.get("entry_price", np.nan), errors="coerce"))
        rsi_val      = float(pd.to_numeric(row.get("rsi_signal", np.nan), errors="coerce"))
        adx_val      = float(pd.to_numeric(row.get("adx_signal", np.nan), errors="coerce"))
        q_val        = float(pd.to_numeric(row.get("quality_score", np.nan), errors="coerce"))

        if pd.isna(signal_time) or not np.isfinite(signal_entry) or signal_entry <= 0:
            skip_rows.append({"ticker": ticker, "side": side, "signal_time_ist": signal_time,
                               "signal_entry_price": signal_entry, "reason": "invalid_signal_time_or_entry"})
            continue

        # Optional filters
        if LONG_RSI_CAP is not None and np.isfinite(rsi_val) and rsi_val > LONG_RSI_CAP:
            skip_rows.append({"ticker": ticker, "side": side, "signal_time_ist": signal_time,
                               "signal_entry_price": signal_entry, "rsi_signal": rsi_val,
                               "reason": f"rsi_cap_exceeded>{LONG_RSI_CAP}"})
            continue

        if LONG_ADX_MIN_V4 is not None and np.isfinite(adx_val) and adx_val < LONG_ADX_MIN_V4:
            skip_rows.append({"ticker": ticker, "side": side, "signal_time_ist": signal_time,
                               "signal_entry_price": signal_entry, "adx_signal": adx_val,
                               "reason": f"adx_below_min<{LONG_ADX_MIN_V4}"})
            continue

        if LONG_QUALITY_MIN is not None and np.isfinite(q_val) and q_val < LONG_QUALITY_MIN:
            skip_rows.append({"ticker": ticker, "side": side, "signal_time_ist": signal_time,
                               "signal_entry_price": signal_entry, "quality_score": q_val,
                               "reason": f"quality_below_min<{LONG_QUALITY_MIN}"})
            continue

        # Load 5-min bars
        bars = _load_5m_for_ticker(ticker, dir_5m, suffix_5m, engine, cache_5m)
        if bars.empty:
            skip_rows.append({"ticker": ticker, "side": side, "signal_time_ist": signal_time,
                               "signal_entry_price": signal_entry, "reason": "no_5m_data"})
            continue

        high_col = _pick_col(bars, "high", "High")
        low_col  = _pick_col(bars, "low", "Low")
        open_col = _pick_col(bars, "open", "Open")
        if low_col is None or high_col is None or open_col is None:
            skip_rows.append({"ticker": ticker, "side": side, "signal_time_ist": signal_time,
                               "signal_entry_price": signal_entry, "reason": "missing_ohlc_cols_5m"})
            continue

        day_norm = signal_time.normalize()
        bars_after = bars[
            (bars["datetime"] > signal_time)
            & (bars["datetime"].dt.normalize() == day_norm)
        ].copy()

        if bars_after.empty:
            skip_rows.append({"ticker": ticker, "side": side, "signal_time_ist": signal_time,
                               "signal_entry_price": signal_entry, "reason": "no_5m_bars_after_signal"})
            continue

        # Resolve entry
        entry_time  = pd.NaT
        entry_price = np.nan
        reason      = ""

        if LONG_ENTRY_MODEL == "signal_entry":
            entry_time  = signal_time
            entry_price = round(signal_entry, 2)
            reason      = "signal_entry"

        elif LONG_ENTRY_MODEL == "next_open_guard":
            first_bar = bars_after.iloc[0]
            next_open = float(first_bar[open_col])
            cap_price = signal_entry * (1.0 + float(LONG_CHASE_CAP_PCT))
            if next_open > cap_price:
                skip_rows.append({"ticker": ticker, "side": side, "signal_time_ist": signal_time,
                                   "signal_entry_price": signal_entry, "next_open": next_open,
                                   "cap_price": cap_price,
                                   "reason": f"chase_cap_exceeded>{LONG_CHASE_CAP_PCT:.4f}"})
                continue
            entry_time  = first_bar["datetime"]
            entry_price = round(next_open, 2)
            reason      = "next_open_guard"

        elif LONG_ENTRY_MODEL == "limit_retrace":
            limit_px = round(signal_entry * (1.0 + float(LONG_LIMIT_OFFSET_PCT)), 2)
            cutoff   = signal_time + pd.Timedelta(minutes=int(LONG_LIMIT_WAIT_MIN))
            cand     = bars_after[bars_after["datetime"] <= cutoff]
            hit      = cand[pd.to_numeric(cand[low_col], errors="coerce") <= limit_px]
            if hit.empty:
                skip_rows.append({"ticker": ticker, "side": side, "signal_time_ist": signal_time,
                                   "signal_entry_price": signal_entry, "limit_price": limit_px,
                                   "wait_min": LONG_LIMIT_WAIT_MIN,
                                   "reason": "limit_not_hit_in_window"})
                continue
            first_hit   = hit.iloc[0]
            entry_time  = first_hit["datetime"]
            entry_price = float(limit_px)
            reason      = "limit_retrace"

        else:
            raise ValueError(f"Unsupported LONG_ENTRY_MODEL={LONG_ENTRY_MODEL}")

        if pd.isna(entry_time) or not np.isfinite(entry_price) or entry_price <= 0:
            skip_rows.append({"ticker": ticker, "side": side, "signal_time_ist": signal_time,
                               "signal_entry_price": signal_entry, "reason": "entry_not_resolved"})
            continue

        # Rebuild stops and targets from actual executed entry
        stop_price   = round(entry_price * (1.0 - float(LONG_STOP_PCT_V4)), 2)
        target_price = round(entry_price * (1.0 + float(LONG_TARGET_PCT_V4)), 2)

        row["entry_time_ist"]          = entry_time
        row["entry_price"]             = float(entry_price)
        row["stop_price"]              = float(stop_price)
        row["sl_price"]                = float(stop_price)
        row["target_price"]            = float(target_price)
        row["entry_model_v4"]          = reason
        row["entry_vs_signal_pct_v4"]  = float((entry_price / signal_entry - 1.0) * 100.0)
        out_rows.append(row)

    out_df  = pd.DataFrame(out_rows)
    skip_df = pd.DataFrame(skip_rows)
    print(
        f"[V4][LONG ENTRY] model={LONG_ENTRY_MODEL} | input={len(df)} | "
        f"kept={len(out_df)} | skipped={len(skip_df)}"
    )
    return out_df, skip_df


# ===========================================================================
# MAIN
# ===========================================================================
def main() -> None:
    _outputs_dir = _THIS_DIR / "outputs_v4"
    _outputs_dir.mkdir(parents=True, exist_ok=True)
    _logs_dir = _THIS_DIR / "logs"
    _logs_dir.mkdir(parents=True, exist_ok=True)

    ts = now_ist().strftime("%Y%m%d_%H%M%S")
    log_path = _outputs_dir / f"avwap_combined_runner_v4_{ts}.txt"

    _orig_stdout, _orig_stderr = sys.stdout, sys.stderr
    with open(log_path, "w", encoding="utf-8") as _log_fh:
        sys.stdout = _Tee(_orig_stdout, _log_fh)
        sys.stderr = _Tee(_orig_stderr, _log_fh)

        try:
            print("=" * 72)
            print("AVWAP COMBINED RUNNER v4 — LONG (anti-chase) + SHORT")
            print(f"  RUN_SHORT_SIDE={RUN_SHORT_SIDE} | RUN_LONG_SIDE={RUN_LONG_SIDE}")
            print(
                f"  LONG anti-chase: model={LONG_ENTRY_MODEL} | "
                f"cap={LONG_CHASE_CAP_PCT*100:.2f}% | "
                f"offset={LONG_LIMIT_OFFSET_PCT*100:.2f}% | "
                f"wait={LONG_LIMIT_WAIT_MIN}min"
            )
            print(f"  LONG  risk: SL={LONG_STOP_PCT_V4*100:.2f}% | TGT={LONG_TARGET_PCT_V4*100:.2f}% | R:R={LONG_TARGET_PCT_V4/LONG_STOP_PCT_V4:.2f}:1")
            print(f"  SHORT risk: SL={SHORT_STOP_PCT_V4*100:.2f}% | TGT={SHORT_TARGET_PCT_V4*100:.2f}% | R:R={SHORT_TARGET_PCT_V4/SHORT_STOP_PCT_V4:.2f}:1")
            print(f"  Outputs → {_outputs_dir}")
            print("=" * 72)

            dir_5m = _resolve_5min_dir()
            print(f"[INFO] 5-min data directory: {dir_5m}")
            if dir_5m.is_dir():
                print(f"[INFO] 5-min parquet files found: {len(list(dir_5m.glob('*.parquet')))}")
            else:
                print("[WARN] 5-min dir missing; exits remain at 15-min resolution.")

            # Build configs using v2-style overrides for both sides
            short_cfg = default_short_config(reports_dir=_outputs_dir)
            long_cfg  = default_long_config(
                reports_dir=_outputs_dir,
                stop_pct=LONG_STOP_PCT_V4,
                target_pct=LONG_TARGET_PCT_V4,
            )

            # Base scan profile from the best-performing v4 sweep base (base1 from v2 sweep).
            from datetime import time as dtime

            # SHORT base scan profile
            short_cfg.stop_pct = 0.0085
            short_cfg.target_pct = 0.0120
            short_cfg.adx_min = 22.0
            short_cfg.adx_slope_min = 0.60
            short_cfg.rsi_max_short = 54.0
            short_cfg.stochk_max = 75.0
            short_cfg.be_trigger_pct = 0.0045
            short_cfg.trail_pct = 0.0028
            short_cfg.use_atr_pct_filter = False
            short_cfg.atr_pct_min = 0.0015
            short_cfg.use_volume_filter = True
            short_cfg.volume_min_ratio = 0.9
            short_cfg.require_avwap_rule = False
            short_cfg.avwap_touch = False
            short_cfg.avwap_min_consec_closes = 1
            short_cfg.avwap_mode = "any"
            short_cfg.avwap_dist_atr_mult = 0.05
            short_cfg.require_entry_close_confirm = True
            short_cfg.max_trades_per_ticker_per_day = 1
            short_cfg.min_bars_left_after_entry = 2
            short_cfg.min_bars_for_scan = 7
            short_cfg.enable_topn_per_day = False
            short_cfg.topn_per_day = 60
            short_cfg.use_time_windows = True
            short_cfg.signal_windows = [(dtime(9, 15), dtime(15, 15))]
            short_cfg.lag_bars_short_a_mod_break_c1_low = 1
            short_cfg.lag_bars_short_a_pullback_c2_break_c2_low = 2
            short_cfg.lag_bars_short_b_huge_failed_bounce = -1

            # LONG base scan profile (pre-anti-chase)
            long_cfg.adx_min = 16.0
            long_cfg.adx_slope_min = 1.25
            long_cfg.rsi_min_long = 45.0
            long_cfg.stochk_min = 15.0
            long_cfg.stochk_max = 98.0
            long_cfg.be_trigger_pct = 0.0045
            long_cfg.trail_pct = 0.0022
            long_cfg.use_atr_pct_filter = False
            long_cfg.atr_pct_min = 0.0015
            long_cfg.use_volume_filter = True
            long_cfg.volume_min_ratio = 0.9
            long_cfg.require_avwap_rule = False
            long_cfg.avwap_touch = False
            long_cfg.avwap_min_consec_closes = 1
            long_cfg.avwap_mode = "any"
            long_cfg.avwap_dist_atr_mult = 0.05
            long_cfg.require_entry_close_confirm = True
            long_cfg.max_trades_per_ticker_per_day = 1
            long_cfg.min_bars_left_after_entry = 2
            long_cfg.min_bars_for_scan = 7
            long_cfg.enable_topn_per_day = False
            long_cfg.topn_per_day = 60
            long_cfg.enable_setup_a_pullback_c2_break = False
            long_cfg.use_time_windows = True
            long_cfg.signal_windows = [(dtime(9, 15), dtime(15, 15))]
            long_cfg.lag_bars_long_a_mod_break_c1_high = 1
            long_cfg.lag_bars_long_a_pullback_c2_break_c2_high = 2
            long_cfg.lag_bars_long_b_huge_pullback_hold_break = -1

            short_df = pd.DataFrame()
            long_df  = pd.DataFrame()
            skip_df  = pd.DataFrame()

            # PHASE 1: Scan signals
            if RUN_SHORT_SIDE:
                print("\n[PHASE 1a] Scanning SHORT signals...")
                short_df = _run_side_parallel("SHORT", short_cfg, max_workers=MAX_WORKERS)
            else:
                print("\n[PHASE 1a] SHORT scan disabled.")

            if RUN_LONG_SIDE:
                print("\n[PHASE 1b] Scanning LONG signals...")
                long_df = _run_side_parallel("LONG", long_cfg, max_workers=MAX_WORKERS)
            else:
                print("\n[PHASE 1b] LONG scan disabled.")

            if short_df.empty and long_df.empty:
                print("[DONE] No trades found from scans.")
                return

            suffix_5m = ".parquet"

            # PHASE 2: Apply LONG anti-chase entry model
            if RUN_LONG_SIDE and not long_df.empty:
                print("\n[PHASE 2] Applying LONG anti-chase entry model v4...")
                long_df, skip_df = _apply_long_entry_model_v4(
                    long_df=long_df,
                    dir_5m=dir_5m,
                    suffix_5m=suffix_5m,
                    engine=long_cfg.parquet_engine,
                )

            # PHASE 3: Re-resolve exits on 5-min bars
            print("\n[PHASE 3] Re-resolving exits on 5-min bars...")
            if not short_df.empty:
                short_df = _apply_short_risk_override(
                    short_df,
                    short_stop_pct=SHORT_STOP_PCT_V4,
                    short_target_pct=SHORT_TARGET_PCT_V4,
                )
                short_df = _resolve_exits_5min(short_df, dir_5m, suffix_5m=suffix_5m, engine=short_cfg.parquet_engine)
            if not long_df.empty:
                long_df  = _resolve_exits_5min(long_df, dir_5m, suffix_5m=suffix_5m, engine=long_cfg.parquet_engine)

            if not short_df.empty:
                short_df = _add_notional_pnl(short_df)
                short_df = _sort_trades_for_output(short_df)
            if not long_df.empty:
                long_df  = _add_notional_pnl(long_df)
                long_df  = _sort_trades_for_output(long_df)

            combined = pd.concat([short_df, long_df], ignore_index=True)
            if combined.empty:
                print("[DONE] No trades after LONG anti-chase filtering.")
                if not skip_df.empty:
                    skip_csv = _outputs_dir / f"avwap_long_entry_skips_v4_{ts}.csv"
                    skip_df.to_csv(skip_csv, index=False)
                    print(f"[FILE SAVED] {skip_csv}")
                return

            combined = _add_notional_pnl(combined)
            combined = _sort_trades_for_output(combined)
            _print_day_side_mix(combined)
            _print_signal_entry_lag_summary(combined)

            # Metrics
            if not short_df.empty:
                print_metrics("SHORT v4 (5-min exits)", compute_backtest_metrics(short_df))
            else:
                print("[INFO] SHORT metrics skipped (no short trades).")

            if not long_df.empty:
                print_metrics(f"LONG v4 (5-min exits, anti-chase={LONG_ENTRY_MODEL})", compute_backtest_metrics(long_df))
            else:
                print("[INFO] LONG metrics skipped (no long trades).")

            print_metrics("COMBINED v4 (5-min exits)", compute_backtest_metrics(combined))
            _print_notional_pnl(combined)

            # Save CSVs
            out_csv = _outputs_dir / f"avwap_longshort_trades_ALL_DAYS_v4_{ts}.csv"
            combined.to_csv(out_csv, index=False)
            print(f"[FILE SAVED] {out_csv}")

            if not long_df.empty:
                out_long = _outputs_dir / f"avwap_long_trades_only_v4_{ts}.csv"
                long_df.to_csv(out_long, index=False)
                print(f"[FILE SAVED] {out_long}")

            if not short_df.empty:
                out_short = _outputs_dir / f"avwap_short_trades_only_v4_{ts}.csv"
                short_df.to_csv(out_short, index=False)
                print(f"[FILE SAVED] {out_short}")

            if not skip_df.empty:
                skip_csv = _outputs_dir / f"avwap_long_entry_skips_v4_{ts}.csv"
                skip_df.to_csv(skip_csv, index=False)
                print(f"[FILE SAVED] {skip_csv}")

            # Charts
            print("\n[INFO] Generating charts...")
            chart_dir_legacy   = _outputs_dir / "charts_v4" / "legacy"
            chart_dir_enhanced = _outputs_dir / "charts_v4" / "enhanced"
            legacy_files = generate_backtest_charts(
                combined, short_df, long_df,
                save_dir=chart_dir_legacy, ts_label=f"{ts}_v4",
            )
            enhanced_files = generate_enhanced_charts(
                combined, short_df, long_df,
                save_dir=chart_dir_enhanced, ts_label=f"{ts}_v4",
            )
            print(
                f"[INFO] Charts: legacy={len(legacy_files or [])} | enhanced={len(enhanced_files or [])}"
            )
            print("[DONE]")

        finally:
            sys.stdout = _orig_stdout
            sys.stderr = _orig_stderr

    print(f"[LOG SAVED] {log_path}")


if __name__ == "__main__":
    main()
