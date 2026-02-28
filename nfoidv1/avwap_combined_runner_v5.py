# -*- coding: utf-8 -*-
"""
avwap_combined_runner_v5.py — AVWAP v11 COMBINED LONG + SHORT runner (v5)
==========================================================================

Inherits all v2 parameter optimisations PLUS two new directional indicators:

NEW IN v5:
  1. Supertrend (period=10, multiplier=3.0)
     • LONG : SUPERTREND_DIR must be +1 (price above lower band — bullish)
     • SHORT: SUPERTREND_DIR must be -1 (price below upper band — bearish)
     → Eliminates contra-trend impulses that pass ADX/RSI/Stoch filters

  2. MACD (12, 26, 9) histogram
     • LONG : histogram > 0  OR  histogram is rising vs previous bar
     • SHORT: histogram < 0  OR  histogram is falling vs previous bar
     → Adds momentum-direction confirmation; catches early turns above zero

COMBINED EFFECT:
  These two filters sit ON TOP of the existing trend filter stack.
  Any signal that passes ADX+RSI+Stoch+EMA+AVWAP but has
  contra-Supertrend or contra-MACD momentum is rejected.
  Expected result: fewer but higher-probability entries, improved
  win rate and profit factor with marginal reduction in trade count.

PARAMETERS (inherited from v2 — no changes):
  SHORT: SL 0.75% | TGT 1.50% | R:R 2.0:1 | BE trigger 0.40% | trail 0.22%
  LONG : SL 0.60% | TGT 1.80% | R:R 3.0:1 | BE trigger 0.45% | trail 0.22%
  Signal window: 09:15–15:00 (both sides)
  ADX slope: SHORT 0.75 / LONG 0.60
  RSI     : SHORT max 62 / LONG min 35
  Stoch   : SHORT max 80 / LONG min 22
  AVWAP dist ATR mult: 0.05

Outputs saved to  outputs_v5/  (separate from all prior versions).
"""

from __future__ import annotations

import heapq
import os
import sys
import warnings
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict
from datetime import time as dtime
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional

import numpy as np
import pandas as pd

# ===========================================================================
# CONSOLE OUTPUT TEE
# ===========================================================================
class _Tee:
    """Write to multiple streams (e.g., console + log file)."""

    def __init__(self, *streams):
        self.streams = [s for s in streams if s is not None]

    def write(self, data):
        for s in self.streams:
            try:
                s.write(data)
            except Exception:
                pass

    def flush(self):
        for s in self.streams:
            try:
                s.flush()
            except Exception:
                pass

    def isatty(self):
        return False


# Ensure the package is importable when running this file directly
_this_dir = Path(__file__).resolve().parent
_project_root = _this_dir.parent if _this_dir.name == "avwap_v11_refactored" else _this_dir
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from avwap_v11_refactored.avwap_common import (
    IST,
    StrategyConfig,
    Trade,
    BacktestMetrics,
    default_short_config,
    default_long_config,
    now_ist,
    trades_to_df,
    apply_topn_per_day,
    compute_backtest_metrics,
    print_metrics,
    read_15m_parquet,
    list_tickers_15m,
    generate_backtest_charts,
)

# V5 strategy scan functions (use Supertrend + MACD filters)
from avwap_v11_refactored.avwap_short_strategy_v5 import (
    scan_all_days_for_ticker_v5 as scan_short_v5,
)
from avwap_v11_refactored.avwap_long_strategy_v5 import (
    scan_all_days_for_ticker_v5 as scan_long_v5,
)


# ===========================================================================
# RUNNER CONFIG v5 — same as v2 optimised constants
# ===========================================================================
POSITION_SIZE_RS_SHORT = 50_000
POSITION_SIZE_RS_LONG  = 100_000
INTRADAY_LEVERAGE_SHORT = 5.0
INTRADAY_LEVERAGE_LONG  = 5.0

ENABLE_CASH_CONSTRAINED_PORTFOLIO_SIM = False
FORCE_LIVE_PARITY_MIN_BARS_LEFT  = True
FORCE_LIVE_PARITY_DISABLE_TOPN   = True

# ---- Signal-window override ----
FINAL_SIGNAL_WINDOW_OVERRIDE     = True
FINAL_SHORT_USE_TIME_WINDOWS     = True
FINAL_SHORT_SIGNAL_WINDOWS       = [(dtime(9, 15, 0), dtime(15, 0, 0))]
FINAL_LONG_USE_TIME_WINDOWS      = True
FINAL_LONG_SIGNAL_WINDOWS        = [(dtime(9, 15, 0), dtime(15, 0, 0))]

# ---- Risk (v2 improved R:R, unchanged in v5) ----
SHORT_STOP_PCT    = 0.0075   # R:R → 2.0:1
SHORT_TARGET_PCT  = 0.0150
LONG_STOP_PCT     = 0.0060
LONG_TARGET_PCT   = 0.0180   # R:R → 3.0:1

# ---- Breakeven / trailing stop ----
SHORT_BE_TRIGGER_PCT = 0.0040
SHORT_TRAIL_PCT      = 0.0022
LONG_BE_TRIGGER_PCT  = 0.0045
LONG_TRAIL_PCT       = 0.0022

# ---- Trend filters (relaxed from v1, unchanged in v5 — Supertrend/MACD now add quality) ----
SHORT_ADX_MIN        = 20.0
SHORT_ADX_SLOPE_MIN  = 0.75
SHORT_RSI_MAX        = 62.0
SHORT_STOCHK_MAX     = 80.0

LONG_ADX_MIN         = 18.0
LONG_ADX_SLOPE_MIN   = 0.60
LONG_RSI_MIN         = 35.0
LONG_STOCHK_MIN      = 22.0
LONG_STOCHK_MAX      = 98.0

# ---- Volume / ATR ----
USE_VOLUME_FILTER  = True
VOLUME_MIN_RATIO   = 1.0
USE_ATR_PCT_FILTER = True
ATR_PCT_MIN        = 0.0018

# ---- AVWAP ----
AVWAP_DIST_ATR_MULT = 0.05

# ---- Entry quality ----
REQUIRE_ENTRY_CLOSE_CONFIRM           = True
MAX_TRADES_PER_TICKER_PER_DAY         = 2
ENABLE_LONG_SETUP_A_PULLBACK_C2_BREAK = True

# ---- Per-setup signal->entry lag (bars) ----
SHORT_LAG_BARS_A_MOD_BREAK_C1_LOW         = 1
SHORT_LAG_BARS_A_PULLBACK_C2_BREAK_C2_LOW = 2
SHORT_LAG_BARS_B_HUGE_FAILED_BOUNCE       = -1
LONG_LAG_BARS_A_MOD_BREAK_C1_HIGH         = 1
LONG_LAG_BARS_A_PULLBACK_C2_BREAK_C2_HIGH = 2
LONG_LAG_BARS_B_HUGE_PULLBACK_HOLD_BREAK  = -1

# ---- Portfolio sim ----
PORTFOLIO_START_CAPITAL_RS          = 1_000_000
DISALLOW_BOTH_SIDES_SAME_TICKER_DAY = True

# ---- Parallelism ----
MAX_WORKERS = 4
DIR_5MIN    = None


# ===========================================================================
# 5-MINUTE DATA HELPERS
# ===========================================================================
def _resolve_5min_dir() -> Path:
    _script_dir = Path(__file__).resolve().parent
    _proj = _script_dir.parent if _script_dir.name == "avwap_v11_refactored" else _script_dir
    candidates = [
        _proj / "data" / "stocks_indicators_5min_eq",
        _proj / "stocks_indicators_5min_eq",
        _proj.parent / "data" / "stocks_indicators_5min_eq",
        _proj.parent / "stocks_indicators_5min_eq",
    ]
    for c in candidates:
        if c.is_dir():
            return c
    return candidates[0]


def read_5m_parquet(path: str, engine: str = "pyarrow") -> pd.DataFrame:
    try:
        p = Path(path)
        if not p.exists():
            return pd.DataFrame()
        df = pd.read_parquet(p, engine=engine)
        if df.empty:
            return df

        dt_col = None
        for c in ["datetime", "date", "DateTime", "timestamp", "Timestamp"]:
            if c in df.columns:
                dt_col = c
                break

        if dt_col is not None:
            if dt_col != "datetime":
                df = df.rename(columns={dt_col: "datetime"})
            df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
        elif df.index.name in {"datetime", "date", "DateTime", "timestamp", "Timestamp"} or isinstance(df.index, pd.DatetimeIndex):
            idx_name = df.index.name if df.index.name else "datetime"
            df = df.reset_index()
            if idx_name != "datetime" and idx_name in df.columns:
                df = df.rename(columns={idx_name: "datetime"})
            if "datetime" in df.columns:
                df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")

        if "datetime" in df.columns:
            df = df.dropna(subset=["datetime"]).sort_values("datetime").reset_index(drop=True)
        return df
    except Exception:
        return pd.DataFrame()


def _find_5m_file_for_ticker(dir_5m: Path, ticker: str, suffix_5m: str = ".parquet") -> Optional[Path]:
    """Resolve a 5-min parquet path for a ticker across common filename conventions."""
    suffix = str(suffix_5m or "").strip()
    candidates: List[str] = []
    if suffix:
        candidates.append(f"{ticker}{suffix}")
        if suffix.startswith("."):
            candidates.append(f"{ticker}_stocks_indicators_5min{suffix}")
    candidates.extend([
        f"{ticker}.parquet",
        f"{ticker}_5min.parquet",
        f"{ticker}_stocks_indicators_5min.parquet",
    ])
    seen = set()
    for name in candidates:
        if not name or name in seen:
            continue
        seen.add(name)
        p = dir_5m / name
        if p.exists():
            return p
    for pattern in [f"{ticker}*5min*.parquet", f"{ticker}*5m*.parquet"]:
        matches = sorted(dir_5m.glob(pattern))
        if matches:
            return matches[0]
    return None


# ===========================================================================
# 5-MIN EXIT RESOLUTION
# ===========================================================================
def _resolve_exits_5min(
    trades_df: pd.DataFrame,
    dir_5m: Path,
    suffix_5m: str = ".parquet",
    engine: str = "pyarrow",
) -> pd.DataFrame:
    if trades_df.empty:
        return trades_df
    if not dir_5m.is_dir():
        print(f"[WARN] 5-min data directory not found: {dir_5m}")
        print("[WARN] Falling back to 15-min exit resolution.")
        return trades_df

    df = trades_df.copy()
    if "stop_price" not in df.columns and "sl_price" in df.columns:
        df["stop_price"] = df["sl_price"]

    required = {"ticker", "side", "entry_price", "entry_time_ist", "stop_price", "target_price"}
    if not required.issubset(set(df.columns)):
        missing = required - set(df.columns)
        print(f"[WARN] Missing columns for 5-min resolution: {missing}")
        return df

    for c in ["entry_time_ist", "exit_time_ist", "signal_time_ist"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")

    _cache_5m: Dict[str, pd.DataFrame] = {}
    updated_rows = 0
    total_rows = len(df)

    for idx in df.index:
        ticker      = str(df.at[idx, "ticker"])
        side        = str(df.at[idx, "side"]).upper()
        entry_price = float(df.at[idx, "entry_price"])
        entry_time  = df.at[idx, "entry_time_ist"]
        stop_price  = float(df.at[idx, "stop_price"])   if pd.notna(df.at[idx, "stop_price"])   else None
        target_price = float(df.at[idx, "target_price"]) if pd.notna(df.at[idx, "target_price"]) else None

        if pd.isna(entry_time) or stop_price is None or target_price is None:
            continue

        if ticker not in _cache_5m:
            fpath = _find_5m_file_for_ticker(dir_5m, ticker, suffix_5m=suffix_5m)
            if fpath is None:
                _cache_5m[ticker] = pd.DataFrame()
            else:
                _cache_5m[ticker] = read_5m_parquet(str(fpath), engine)

        df_5m = _cache_5m[ticker]
        if df_5m.empty:
            continue
        if "datetime" not in df_5m.columns:
            continue

        trade_date = pd.Timestamp(entry_time).normalize()
        mask = (df_5m["datetime"] > entry_time) & (df_5m["datetime"].dt.normalize() == trade_date)
        bars = df_5m.loc[mask].sort_values("datetime")

        if bars.empty:
            continue

        new_exit_price = None
        new_exit_time  = None
        new_outcome    = None

        for _, bar in bars.iterrows():
            bar_high = float(bar.get("high", bar.get("High", np.nan)))
            bar_low  = float(bar.get("low",  bar.get("Low",  np.nan)))
            bar_time = bar["datetime"]

            if np.isnan(bar_high) or np.isnan(bar_low):
                continue

            if side == "SHORT":
                if bar_high >= stop_price:
                    new_exit_price, new_exit_time, new_outcome = stop_price,   bar_time, "SL"
                    break
                if bar_low <= target_price:
                    new_exit_price, new_exit_time, new_outcome = target_price, bar_time, "TARGET"
                    break
            else:
                if bar_low <= stop_price:
                    new_exit_price, new_exit_time, new_outcome = stop_price,   bar_time, "SL"
                    break
                if bar_high >= target_price:
                    new_exit_price, new_exit_time, new_outcome = target_price, bar_time, "TARGET"
                    break

        if new_exit_price is None:
            last_bar = bars.iloc[-1]
            new_exit_price = float(last_bar.get("close", last_bar.get("Close", entry_price)))
            new_exit_time  = last_bar["datetime"]
            new_outcome    = "EOD"

        df.at[idx, "exit_price"]    = new_exit_price
        df.at[idx, "exit_time_ist"] = new_exit_time
        df.at[idx, "outcome"]       = new_outcome

        if side == "SHORT":
            raw_pct = (entry_price - new_exit_price) / entry_price * 100.0 if entry_price != 0 else 0.0
        else:
            raw_pct = (new_exit_price - entry_price) / entry_price * 100.0 if entry_price != 0 else 0.0

        df.at[idx, "pnl_pct_gross"] = raw_pct
        slippage_pct   = float(df.at[idx, "slippage_pct"])   if "slippage_pct"   in df.columns and pd.notna(df.at[idx, "slippage_pct"])   else 0.0005
        commission_pct = float(df.at[idx, "commission_pct"]) if "commission_pct" in df.columns and pd.notna(df.at[idx, "commission_pct"]) else 0.0003
        cost_pct = (slippage_pct + commission_pct) * 100.0 * 2
        df.at[idx, "pnl_pct"] = raw_pct - cost_pct
        updated_rows += 1

    print(f"[5MIN] Re-resolved exits for {updated_rows}/{total_rows} trades using 5-min data.")
    return df


# ===========================================================================
# PARALLEL SCAN WORKERS — V5 (call v5 strategy functions)
# ===========================================================================
def _scan_one_ticker_short_v5(args: Tuple[str, str, StrategyConfig]) -> List[dict]:
    ticker, path, cfg = args
    df = read_15m_parquet(path, cfg.parquet_engine)
    if df.empty:
        return []
    trades = scan_short_v5(ticker, df, cfg)
    return [asdict(t) for t in trades]


def _scan_one_ticker_long_v5(args: Tuple[str, str, StrategyConfig]) -> List[dict]:
    ticker, path, cfg = args
    df = read_15m_parquet(path, cfg.parquet_engine)
    if df.empty:
        return []
    trades = scan_long_v5(ticker, df, cfg)
    return [asdict(t) for t in trades]


def _run_side_parallel_v5(
    side: str,
    cfg: StrategyConfig,
    max_workers: int = MAX_WORKERS,
) -> pd.DataFrame:
    tickers = list_tickers_15m(cfg.dir_15m, cfg.end_15m)
    print(f"[{side}-v5] Tickers found: {len(tickers)}")

    worker_fn = _scan_one_ticker_short_v5 if side == "SHORT" else _scan_one_ticker_long_v5
    task_args = [
        (t, os.path.join(cfg.dir_15m, f"{t}{cfg.end_15m}"), cfg)
        for t in tickers
    ]

    all_dicts: List[dict] = []

    if max_workers <= 1:
        for k, args in enumerate(task_args, 1):
            result = worker_fn(args)
            all_dicts.extend(result)
            if k % 50 == 0:
                print(f"  [{side}-v5] scanned {k}/{len(tickers)} | trades={len(all_dicts)}")
    else:
        done_count = 0
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(worker_fn, a): a[0] for a in task_args}
            for future in as_completed(futures):
                done_count += 1
                try:
                    result = future.result()
                    all_dicts.extend(result)
                except Exception as e:
                    ticker_name = futures[future]
                    print(f"  [{side}-v5] ERROR on {ticker_name}: {e}")
                if done_count % 100 == 0:
                    print(f"  [{side}-v5] scanned {done_count}/{len(tickers)} | trades={len(all_dicts)}")

    if not all_dicts:
        return pd.DataFrame()

    out = pd.DataFrame(all_dicts)
    out = apply_topn_per_day(out, cfg)

    for c in ["signal_time_ist", "entry_time_ist", "exit_time_ist"]:
        if c in out.columns:
            out[c] = pd.to_datetime(out[c], errors="coerce")

    sort_cols = [c for c in ["trade_date", "ticker", "entry_time_ist"] if c in out.columns]
    if sort_cols:
        out = out.sort_values(sort_cols).reset_index(drop=True)

    return out


# ===========================================================================
# NOTIONAL P&L
# ===========================================================================
def _add_notional_pnl(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    d = df.copy()

    if "pnl_pct_price" not in d.columns:
        d["pnl_pct_price"] = pd.to_numeric(d.get("pnl_pct", 0.0), errors="coerce").fillna(0.0)

    if "pnl_pct_gross_price" not in d.columns:
        if "pnl_pct_gross" in d.columns:
            d["pnl_pct_gross_price"] = pd.to_numeric(d["pnl_pct_gross"], errors="coerce").fillna(0.0)
        elif {"entry_price", "exit_price", "side"}.issubset(d.columns):
            ep = pd.to_numeric(d["entry_price"], errors="coerce")
            xp = pd.to_numeric(d["exit_price"],  errors="coerce")
            s  = d["side"].astype(str).str.upper()
            denom = ep.replace(0, np.nan)
            gross = np.where(s.eq("SHORT"), (ep - xp) / denom * 100.0, (xp - ep) / denom * 100.0)
            d["pnl_pct_gross_price"] = pd.to_numeric(gross, errors="coerce").fillna(0.0)
        else:
            d["pnl_pct_gross_price"] = 0.0

    side_u = d["side"].astype(str).str.upper() if "side" in d.columns else pd.Series([""] * len(d))

    if "position_size_rs" not in d.columns:
        d["position_size_rs"] = np.nan
    d.loc[side_u.eq("SHORT") & d["position_size_rs"].isna(), "position_size_rs"] = float(POSITION_SIZE_RS_SHORT)
    d.loc[~side_u.eq("SHORT") & d["position_size_rs"].isna(), "position_size_rs"] = float(POSITION_SIZE_RS_LONG)
    d["position_size_rs"] = pd.to_numeric(d["position_size_rs"], errors="coerce").fillna(0.0)

    if "leverage" not in d.columns:
        d["leverage"] = np.nan
    d.loc[side_u.eq("SHORT") & d["leverage"].isna(), "leverage"] = float(INTRADAY_LEVERAGE_SHORT)
    d.loc[~side_u.eq("SHORT") & d["leverage"].isna(), "leverage"] = float(INTRADAY_LEVERAGE_LONG)
    d["leverage"] = pd.to_numeric(d["leverage"], errors="coerce").fillna(1.0)

    d["notional_exposure_rs"] = d["position_size_rs"] * d["leverage"]
    d["pnl_pct"]       = pd.to_numeric(d["pnl_pct_price"],       errors="coerce").fillna(0.0) * d["leverage"]
    d["pnl_pct_gross"] = pd.to_numeric(d["pnl_pct_gross_price"], errors="coerce").fillna(0.0) * d["leverage"]
    d["pnl_rs"]        = (pd.to_numeric(d["pnl_pct_price"],       errors="coerce").fillna(0.0) / 100.0) * d["notional_exposure_rs"]
    d["pnl_rs_gross"]  = (pd.to_numeric(d["pnl_pct_gross_price"], errors="coerce").fillna(0.0) / 100.0) * d["notional_exposure_rs"]

    return d


def _sort_trades_for_output(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    d = df.copy()
    if "trade_date" in d.columns:
        d["trade_date"] = pd.to_datetime(d["trade_date"], errors="coerce")
    for c in ["signal_time_ist", "entry_time_ist", "exit_time_ist"]:
        if c in d.columns:
            d[c] = pd.to_datetime(d[c], errors="coerce")
    sort_cols = [c for c in ["trade_date", "entry_time_ist", "signal_time_ist", "ticker", "side"] if c in d.columns]
    if sort_cols:
        d = d.sort_values(sort_cols).reset_index(drop=True)
    return d


def _print_day_side_mix(df: pd.DataFrame) -> None:
    if df.empty or not {"trade_date", "side"}.issubset(df.columns):
        return
    d = df.copy()
    d["trade_date"] = pd.to_datetime(d["trade_date"], errors="coerce").dt.date
    d["side"] = d["side"].astype(str).str.upper()
    pivot = d.groupby(["trade_date", "side"]).size().unstack(fill_value=0)
    short_s = pivot["SHORT"] if "SHORT" in pivot.columns else pd.Series(0, index=pivot.index)
    long_s  = pivot["LONG"]  if "LONG"  in pivot.columns else pd.Series(0, index=pivot.index)
    only_short = int(((short_s > 0) & (long_s == 0)).sum())
    only_long  = int(((long_s  > 0) & (short_s == 0)).sum())
    both       = int(((short_s > 0) & (long_s  > 0)).sum())
    total_days = int(len(pivot))
    print(
        f"[INFO] Day-side mix: both={both} | short_only={only_short} | "
        f"long_only={only_long} | total_days={total_days}"
    )


def _print_signal_entry_lag_summary(df: pd.DataFrame) -> None:
    if df.empty:
        return
    required = {"signal_time_ist", "entry_time_ist"}
    if not required.issubset(df.columns):
        return
    d = df.copy()
    d["signal_time_ist"] = pd.to_datetime(d["signal_time_ist"], errors="coerce")
    d["entry_time_ist"]  = pd.to_datetime(d["entry_time_ist"],  errors="coerce")
    d = d.dropna(subset=["signal_time_ist", "entry_time_ist"]).copy()
    if d.empty:
        return
    for c in ["side", "setup", "impulse_type"]:
        if c not in d.columns:
            d[c] = ""
        d[c] = d[c].fillna("").astype(str)
    d["side"]    = d["side"].str.upper()
    d["lag_min"] = (d["entry_time_ist"] - d["signal_time_ist"]).dt.total_seconds() / 60.0
    grouped = d.groupby(["side", "setup", "impulse_type"], dropna=False)["lag_min"]
    lag_summary = grouped.agg(
        count="size", min="min", p50="median", mean="mean",
        p90=lambda s: s.quantile(0.90), max="max",
    ).reset_index()
    lag_summary["p50_bars_15m"] = lag_summary["p50"] / 15.0
    lag_summary = lag_summary.sort_values(["side", "setup", "impulse_type"]).reset_index(drop=True)
    for col in ["min", "p50", "mean", "p90", "max", "p50_bars_15m"]:
        lag_summary[col] = pd.to_numeric(lag_summary[col], errors="coerce").round(2)
    neg_rows  = int((d["lag_min"] < 0).sum())
    zero_rows = int((d["lag_min"] == 0).sum())
    print("\n[DEBUG] Signal->Entry lag by setup (minutes)")
    print(f"[DEBUG] Rows={len(d)} | negative_lag_rows={neg_rows} | same_timestamp_rows={zero_rows}")
    print(lag_summary.to_string(index=False))


def _print_notional_pnl(combined: pd.DataFrame) -> None:
    if "pnl_rs" not in combined.columns:
        return
    pnl_short = float(combined.loc[combined["side"].eq("SHORT"), "pnl_rs"].sum())
    pnl_long  = float(combined.loc[combined["side"].eq("LONG"),  "pnl_rs"].sum())
    pnl_all   = float(combined["pnl_rs"].sum())
    print(f"\n{'=' * 20} NOTIONAL P&L SUMMARY (Rs.) {'=' * 20}")
    print(f"SHORT notional P&L            : Rs.{pnl_short:,.2f}")
    print(f"LONG  notional P&L            : Rs.{pnl_long:,.2f}")
    print(f"TOTAL notional P&L            : Rs.{pnl_all:,.2f}")
    print("=" * 61)


# ===========================================================================
# PORTFOLIO SIMULATION (cash-constrained)
# ===========================================================================
def _simulate_cash_constrained(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    empty_stats = {
        "start_capital": PORTFOLIO_START_CAPITAL_RS, "taken": 0, "skipped": 0,
        "net_pnl_rs": 0.0, "final_equity": float(PORTFOLIO_START_CAPITAL_RS),
        "roi_pct": 0.0, "max_concurrent": 0, "min_cash": float(PORTFOLIO_START_CAPITAL_RS),
    }
    if df.empty:
        return df, empty_stats

    d = df.copy()
    for c in ["entry_time_ist", "exit_time_ist"]:
        if c in d.columns:
            d[c] = pd.to_datetime(d[c], errors="coerce")
    d = d.sort_values(["entry_time_ist", "exit_time_ist", "ticker", "side"]).reset_index(drop=True)

    cash = float(PORTFOLIO_START_CAPITAL_RS)
    open_heap: list = []
    seen_ticker_day: set = set()

    taken_flags    = np.zeros(len(d), dtype=bool)
    cash_before_arr = np.zeros(len(d))
    cash_after_arr  = np.zeros(len(d))
    pos_sizes_arr   = np.zeros(len(d))
    pnl_rs_sim_arr  = np.zeros(len(d))
    taken = skipped = max_conc = 0
    min_cash = cash

    for row in d.itertuples():
        idx      = row.Index
        entry_ts = row.entry_time_ist
        while open_heap and open_heap[0][0] <= entry_ts:
            _, size, pnl_rs = heapq.heappop(open_heap)
            cash += size + pnl_rs

        cb     = cash
        side   = str(row.side).upper()
        ticker = str(row.ticker)
        day    = str(row.trade_date)

        pos = float(POSITION_SIZE_RS_SHORT if side == "SHORT" else POSITION_SIZE_RS_LONG)
        pnl = float(getattr(row, "pnl_rs", 0.0))

        take = True
        if DISALLOW_BOTH_SIDES_SAME_TICKER_DAY:
            key = (ticker, day)
            if key in seen_ticker_day:
                take = False

        if cash < pos:
            take = False

        if take:
            cash -= pos
            heapq.heappush(open_heap, (entry_ts, pos, pnl))
            taken += 1
            seen_ticker_day.add((ticker, day))
        else:
            skipped += 1
            pos = 0.0
            pnl = 0.0

        taken_flags[idx]    = take
        cash_before_arr[idx] = cb
        cash_after_arr[idx]  = cash
        pos_sizes_arr[idx]   = pos
        pnl_rs_sim_arr[idx]  = pnl
        max_conc = max(max_conc, len(open_heap))
        min_cash = min(min_cash, cash)

    while open_heap:
        _, size, pnl_rs = heapq.heappop(open_heap)
        cash += size + pnl_rs

    final_equity = cash
    net_pnl = final_equity - float(PORTFOLIO_START_CAPITAL_RS)
    roi = (net_pnl / float(PORTFOLIO_START_CAPITAL_RS) * 100.0) if PORTFOLIO_START_CAPITAL_RS > 0 else 0.0

    d["taken"]              = taken_flags
    d["cash_before"]        = cash_before_arr
    d["cash_after"]         = cash_after_arr
    d["position_size_rs_sim"] = pos_sizes_arr
    d["pnl_rs_sim"]         = pnl_rs_sim_arr

    stats = {
        "start_capital": float(PORTFOLIO_START_CAPITAL_RS), "taken": int(taken),
        "skipped": int(skipped), "net_pnl_rs": float(net_pnl),
        "final_equity": float(final_equity), "roi_pct": float(roi),
        "max_concurrent": int(max_conc), "min_cash": float(min_cash),
    }
    return d, stats


def _print_portfolio(stats: Dict[str, Any]) -> None:
    print("\n================ PORTFOLIO SUMMARY (cash-constrained) ================")
    print(f"Start capital                 : Rs.{stats['start_capital']:,.2f}")
    print(f"Taken trades                  : {stats['taken']}")
    print(f"Skipped trades                : {stats['skipped']}")
    print(f"Net P&L                       : Rs.{stats['net_pnl_rs']:,.2f}")
    print(f"Final equity                  : Rs.{stats['final_equity']:,.2f}")
    print(f"ROI on start capital          : {stats['roi_pct']:.2f}%")
    print(f"Max concurrent positions      : {stats['max_concurrent']}")
    print(f"Minimum cash during run       : Rs.{stats['min_cash']:,.2f}")
    print("=" * 69)


# ===========================================================================
# ENHANCED CHARTING SUITE (20 charts — v5 labels)
# ===========================================================================
def generate_enhanced_charts(
    combined: pd.DataFrame,
    short_df: pd.DataFrame,
    long_df: pd.DataFrame,
    save_dir: Path,
    ts_label: str = "",
) -> List[str]:
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.ticker as mticker
    except ImportError:
        print("[WARN] matplotlib not available — skipping chart generation.")
        return []

    warnings.filterwarnings("ignore", category=UserWarning, module="matplotlib")
    save_dir = Path(save_dir)
    save_dir.mkdir(parents=True, exist_ok=True)
    saved: List[str] = []

    def _safe_col(df_: pd.DataFrame, col: str) -> pd.Series:
        if col in df_.columns:
            return pd.to_numeric(df_[col], errors="coerce").fillna(0.0)
        return pd.Series(np.zeros(len(df_)), index=df_.index)

    def _save(fig, name: str):
        p = save_dir / f"{name}_{ts_label}.png"
        fig.savefig(p, dpi=150, bbox_inches="tight", facecolor="white")
        plt.close(fig)
        saved.append(str(p))

    combined_sorted = combined.copy()
    if "trade_date" in combined_sorted.columns:
        combined_sorted["trade_date"] = pd.to_datetime(combined_sorted["trade_date"], errors="coerce")
        combined_sorted = combined_sorted.sort_values("trade_date").reset_index(drop=True)

    pnl_pct = _safe_col(combined_sorted, "pnl_pct")
    pnl_rs  = _safe_col(combined_sorted, "pnl_rs")
    cum_pnl = pnl_rs.cumsum()

    # 1. Cumulative P&L
    fig, ax = plt.subplots(figsize=(14, 6))
    ax.plot(cum_pnl.values, label="Combined", linewidth=2, color="#2563EB")
    if not short_df.empty:
        s_pnl = _safe_col(short_df.sort_values("trade_date") if "trade_date" in short_df.columns else short_df, "pnl_rs")
        ax.plot(s_pnl.cumsum().values, label="Short", linewidth=1.5, color="#DC2626", alpha=0.8)
    if not long_df.empty:
        l_pnl = _safe_col(long_df.sort_values("trade_date") if "trade_date" in long_df.columns else long_df, "pnl_rs")
        ax.plot(l_pnl.cumsum().values, label="Long", linewidth=1.5, color="#16A34A", alpha=0.8)
    ax.axhline(0, color="grey", linewidth=0.8, linestyle="--")
    ax.fill_between(range(len(cum_pnl)), cum_pnl.values, 0, where=cum_pnl.values >= 0, alpha=0.15, color="#2563EB")
    ax.fill_between(range(len(cum_pnl)), cum_pnl.values, 0, where=cum_pnl.values < 0, alpha=0.15, color="#DC2626")
    ax.set_title("Cumulative P&L (Rs.) — v5 Supertrend+MACD", fontsize=14, fontweight="bold")
    ax.set_xlabel("Trade #")
    ax.set_ylabel("Cumulative P&L (Rs.)")
    ax.legend(fontsize=11)
    ax.grid(True, alpha=0.3)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"₹{x:,.0f}"))
    _save(fig, "01_cumulative_pnl")

    # 2. Daily P&L
    if "trade_date" in combined_sorted.columns:
        daily = combined_sorted.groupby("trade_date")["pnl_rs"].sum()
        fig, ax = plt.subplots(figsize=(14, 5))
        colors = ["#16A34A" if v >= 0 else "#DC2626" for v in daily.values]
        ax.bar(range(len(daily)), daily.values, color=colors, alpha=0.85, width=0.8)
        ax.axhline(0, color="grey", linewidth=0.8, linestyle="--")
        ax.set_title("Daily Net P&L (Rs.) — v5", fontsize=14, fontweight="bold")
        ax.set_xlabel("Trading Day")
        ax.set_ylabel("P&L (Rs.)")
        ax.grid(True, alpha=0.3, axis="y")
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"₹{x:,.0f}"))
        n = len(daily)
        tick_positions = [0, n // 4, n // 2, 3 * n // 4, n - 1] if n > 5 else list(range(n))
        ax.set_xticks(tick_positions)
        ax.set_xticklabels([str(daily.index[i])[:10] for i in tick_positions], rotation=30, fontsize=8)
        _save(fig, "02_daily_pnl")

    # 3. Drawdown Curve
    if len(cum_pnl) > 0:
        running_max = cum_pnl.cummax()
        drawdown = cum_pnl - running_max
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 8), height_ratios=[2, 1], sharex=True)
        ax1.plot(cum_pnl.values, linewidth=2, color="#2563EB", label="Equity Curve")
        ax1.plot(running_max.values, linewidth=1, color="#9CA3AF", linestyle="--", label="High Watermark")
        ax1.fill_between(range(len(cum_pnl)), cum_pnl.values, running_max.values, alpha=0.2, color="#DC2626")
        ax1.set_title("Equity Curve & Drawdown — v5", fontsize=14, fontweight="bold")
        ax1.set_ylabel("Cumulative P&L (Rs.)")
        ax1.legend(fontsize=10)
        ax1.grid(True, alpha=0.3)
        ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"₹{x:,.0f}"))
        ax2.fill_between(range(len(drawdown)), drawdown.values, 0, color="#DC2626", alpha=0.4)
        ax2.plot(drawdown.values, color="#DC2626", linewidth=1)
        ax2.set_ylabel("Drawdown (Rs.)")
        ax2.set_xlabel("Trade #")
        ax2.grid(True, alpha=0.3)
        ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"₹{x:,.0f}"))
        plt.tight_layout()
        _save(fig, "03_drawdown_curve")

    # 4. Win Rate by Side
    if "side" in combined_sorted.columns:
        sides_data = []
        for s_name, s_df in [("SHORT", short_df), ("LONG", long_df), ("COMBINED", combined_sorted)]:
            if s_df.empty:
                continue
            s_pnl = _safe_col(s_df, "pnl_pct")
            wins   = (s_pnl > 0).sum()
            losses = (s_pnl < 0).sum()
            be     = (s_pnl == 0).sum()
            total  = len(s_pnl)
            wr = wins / total * 100 if total > 0 else 0
            sides_data.append({"side": s_name, "win_rate": wr, "wins": wins, "losses": losses, "breakeven": be, "total": total})
        if sides_data:
            fig, ax = plt.subplots(figsize=(10, 6))
            x_pos = range(len(sides_data))
            colors_wr = ["#DC2626", "#16A34A", "#2563EB"][:len(sides_data)]
            bars = ax.bar(x_pos, [d["win_rate"] for d in sides_data], color=colors_wr, alpha=0.85, width=0.5)
            for bar, d in zip(bars, sides_data):
                ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 1,
                        f'{d["win_rate"]:.1f}%\n({d["wins"]}W/{d["losses"]}L/{d["breakeven"]}BE)',
                        ha="center", va="bottom", fontsize=10, fontweight="bold")
            ax.set_xticks(x_pos)
            ax.set_xticklabels([d["side"] for d in sides_data], fontsize=12)
            ax.set_ylim(0, max(d["win_rate"] for d in sides_data) + 15)
            ax.set_title("Win Rate by Side — v5", fontsize=14, fontweight="bold")
            ax.set_ylabel("Win Rate (%)")
            ax.grid(True, alpha=0.3, axis="y")
            _save(fig, "04_win_rate_by_side")

    # 5. P&L Distribution
    fig, ax = plt.subplots(figsize=(12, 6))
    pnl_vals = pnl_pct.dropna()
    if len(pnl_vals) > 0:
        n_bins = min(80, max(20, len(pnl_vals) // 10))
        ax.hist(pnl_vals, bins=n_bins, color="#2563EB", alpha=0.7, edgecolor="white", linewidth=0.5)
        ax.axvline(pnl_vals.mean(),   color="#DC2626", linestyle="--", linewidth=2, label=f"Mean: {pnl_vals.mean():.2f}%")
        ax.axvline(pnl_vals.median(), color="#F59E0B", linestyle="--", linewidth=2, label=f"Median: {pnl_vals.median():.2f}%")
        ax.axvline(0, color="grey", linewidth=1, linestyle="-")
        ax.set_title("P&L Distribution (%) — All Trades v5", fontsize=14, fontweight="bold")
        ax.set_xlabel("P&L (%)")
        ax.set_ylabel("Frequency")
        ax.legend(fontsize=11)
        ax.grid(True, alpha=0.3, axis="y")
    _save(fig, "05_pnl_distribution_combined")

    # 6. P&L by Side (overlay)
    fig, ax = plt.subplots(figsize=(12, 6))
    if not short_df.empty:
        s_pnl = _safe_col(short_df, "pnl_pct").dropna()
        if len(s_pnl) > 0:
            ax.hist(s_pnl, bins=50, color="#DC2626", alpha=0.5, edgecolor="white", linewidth=0.3, label=f"Short (μ={s_pnl.mean():.2f}%)")
    if not long_df.empty:
        l_pnl = _safe_col(long_df, "pnl_pct").dropna()
        if len(l_pnl) > 0:
            ax.hist(l_pnl, bins=50, color="#16A34A", alpha=0.5, edgecolor="white", linewidth=0.3, label=f"Long (μ={l_pnl.mean():.2f}%)")
    ax.axvline(0, color="grey", linewidth=1, linestyle="-")
    ax.set_title("P&L Distribution by Side (Overlay) — v5", fontsize=14, fontweight="bold")
    ax.set_xlabel("P&L (%)")
    ax.set_ylabel("Frequency")
    ax.legend(fontsize=11)
    ax.grid(True, alpha=0.3, axis="y")
    _save(fig, "06_pnl_distribution_by_side")

    # 7. Outcome Pie
    if "outcome" in combined_sorted.columns:
        outcome_counts = combined_sorted["outcome"].value_counts()
        fig, ax = plt.subplots(figsize=(8, 8))
        colors_pie = {"TARGET": "#16A34A", "SL": "#DC2626", "EOD": "#F59E0B", "BE": "#6366F1"}
        pie_colors = [colors_pie.get(o, "#9CA3AF") for o in outcome_counts.index]
        wedges, texts, autotexts = ax.pie(
            outcome_counts.values, labels=outcome_counts.index, autopct="%1.1f%%",
            colors=pie_colors, startangle=90, textprops={"fontsize": 12},
        )
        for t in autotexts:
            t.set_fontweight("bold")
        ax.set_title("Trade Outcome Breakdown — v5", fontsize=14, fontweight="bold")
        _save(fig, "07_outcome_pie")

    # 8. Outcome by Side
    if "outcome" in combined_sorted.columns and "side" in combined_sorted.columns:
        cross = pd.crosstab(combined_sorted["outcome"], combined_sorted["side"])
        fig, ax = plt.subplots(figsize=(10, 6))
        cross.plot(kind="bar", ax=ax, color=["#16A34A", "#DC2626"], alpha=0.85, edgecolor="white")
        ax.set_title("Outcome Breakdown by Side — v5", fontsize=14, fontweight="bold")
        ax.set_xlabel("Outcome")
        ax.set_ylabel("Count")
        ax.legend(title="Side", fontsize=10)
        ax.grid(True, alpha=0.3, axis="y")
        plt.xticks(rotation=0)
        _save(fig, "08_outcome_by_side")

    # 9. Monthly P&L
    if "trade_date" in combined_sorted.columns:
        combined_sorted["_month"] = combined_sorted["trade_date"].dt.to_period("M")
        monthly = combined_sorted.groupby("_month")["pnl_rs"].sum()
        if len(monthly) > 1:
            fig, ax = plt.subplots(figsize=(14, 5))
            colors_monthly = ["#16A34A" if v >= 0 else "#DC2626" for v in monthly.values]
            ax.bar(range(len(monthly)), monthly.values, color=colors_monthly, alpha=0.85, width=0.7)
            ax.set_xticks(range(len(monthly)))
            ax.set_xticklabels([str(m) for m in monthly.index], rotation=45, fontsize=9)
            ax.axhline(0, color="grey", linewidth=0.8, linestyle="--")
            ax.set_title("Monthly P&L (Rs.) — v5", fontsize=14, fontweight="bold")
            ax.set_ylabel("P&L (Rs.)")
            ax.grid(True, alpha=0.3, axis="y")
            ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"₹{x:,.0f}"))
            _save(fig, "09_monthly_pnl")
        combined_sorted.drop(columns=["_month"], inplace=True, errors="ignore")

    # 10. Rolling Sharpe
    if len(pnl_pct) >= 20:
        rolling_window = 20
        rolling_mean   = pnl_pct.rolling(rolling_window).mean()
        rolling_std    = pnl_pct.rolling(rolling_window).std()
        rolling_sharpe = rolling_mean / rolling_std.replace(0, np.nan) * np.sqrt(252)
        fig, ax = plt.subplots(figsize=(14, 5))
        ax.plot(rolling_sharpe.values, linewidth=1.5, color="#6366F1", alpha=0.8)
        ax.axhline(0, color="grey", linewidth=0.8, linestyle="--")
        ax.axhline(rolling_sharpe.mean(), color="#F59E0B", linewidth=1, linestyle="--",
                   label=f"Avg: {rolling_sharpe.mean():.2f}")
        ax.fill_between(range(len(rolling_sharpe)), rolling_sharpe.values, 0,
                        where=rolling_sharpe.values >= 0, alpha=0.15, color="#16A34A")
        ax.fill_between(range(len(rolling_sharpe)), rolling_sharpe.values, 0,
                        where=rolling_sharpe.values < 0, alpha=0.15, color="#DC2626")
        ax.set_title(f"Rolling Sharpe ({rolling_window}-trade window) — v5", fontsize=14, fontweight="bold")
        ax.set_xlabel("Trade #")
        ax.set_ylabel("Sharpe Ratio (annualized)")
        ax.legend(fontsize=11)
        ax.grid(True, alpha=0.3)
        _save(fig, "10_rolling_sharpe")

    # 11. Trade Duration
    if {"entry_time_ist", "exit_time_ist"}.issubset(combined_sorted.columns):
        entry_t = pd.to_datetime(combined_sorted["entry_time_ist"], errors="coerce")
        exit_t  = pd.to_datetime(combined_sorted["exit_time_ist"],  errors="coerce")
        durations_min = (exit_t - entry_t).dt.total_seconds() / 60.0
        durations_min = durations_min.dropna()
        durations_min = durations_min[durations_min > 0]
        if len(durations_min) > 0:
            fig, ax = plt.subplots(figsize=(12, 5))
            ax.hist(durations_min, bins=min(50, max(10, len(durations_min) // 5)),
                    color="#0EA5E9", alpha=0.7, edgecolor="white")
            ax.axvline(durations_min.mean(),   color="#DC2626", linestyle="--", linewidth=2,
                       label=f"Mean: {durations_min.mean():.1f} min")
            ax.axvline(durations_min.median(), color="#F59E0B", linestyle="--", linewidth=2,
                       label=f"Median: {durations_min.median():.1f} min")
            ax.set_title("Trade Duration Distribution (minutes) — v5", fontsize=14, fontweight="bold")
            ax.set_xlabel("Duration (minutes)")
            ax.set_ylabel("Frequency")
            ax.legend(fontsize=11)
            ax.grid(True, alpha=0.3, axis="y")
            _save(fig, "11_trade_duration_dist")

    # 12. Top Winners & Losers
    if "pnl_rs" in combined_sorted.columns and "ticker" in combined_sorted.columns:
        top_win  = combined_sorted.nlargest(10,  "pnl_rs")[["ticker", "trade_date", "side", "pnl_rs"]]
        top_loss = combined_sorted.nsmallest(10, "pnl_rs")[["ticker", "trade_date", "side", "pnl_rs"]]
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 7))
        if len(top_win) > 0:
            labels_w = [f"{r.ticker}\n{str(r.trade_date)[:10]}" for r in top_win.itertuples()]
            ax1.barh(range(len(top_win)), top_win["pnl_rs"].values, color="#16A34A", alpha=0.85)
            ax1.set_yticks(range(len(top_win)))
            ax1.set_yticklabels(labels_w, fontsize=9)
            ax1.set_title("Top 10 Winners (Rs.) — v5", fontsize=12, fontweight="bold")
            ax1.set_xlabel("P&L (Rs.)")
            ax1.grid(True, alpha=0.3, axis="x")
            ax1.invert_yaxis()
        if len(top_loss) > 0:
            labels_l = [f"{r.ticker}\n{str(r.trade_date)[:10]}" for r in top_loss.itertuples()]
            ax2.barh(range(len(top_loss)), top_loss["pnl_rs"].values, color="#DC2626", alpha=0.85)
            ax2.set_yticks(range(len(top_loss)))
            ax2.set_yticklabels(labels_l, fontsize=9)
            ax2.set_title("Top 10 Losers (Rs.) — v5", fontsize=12, fontweight="bold")
            ax2.set_xlabel("P&L (Rs.)")
            ax2.grid(True, alpha=0.3, axis="x")
            ax2.invert_yaxis()
        plt.tight_layout()
        _save(fig, "12_top_winners_losers")

    # 13. P&L by Setup / Impulse Type
    for col_name, chart_num, title in [
        ("setup", "13a", "P&L by Setup Type"),
        ("impulse_type", "13b", "P&L by Impulse Type"),
    ]:
        if col_name in combined_sorted.columns:
            grp = combined_sorted.groupby(col_name)["pnl_rs"].agg(["sum", "mean", "count"])
            grp = grp.sort_values("sum", ascending=True)
            if 0 < len(grp) <= 20:
                fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, max(5, len(grp) * 0.5)))
                colors_setup  = ["#16A34A" if v >= 0 else "#DC2626" for v in grp["sum"].values]
                colors_setup2 = ["#16A34A" if v >= 0 else "#DC2626" for v in grp["mean"].values]
                ax1.barh(range(len(grp)), grp["sum"].values, color=colors_setup, alpha=0.85)
                ax1.set_yticks(range(len(grp)))
                ax1.set_yticklabels(grp.index, fontsize=9)
                ax1.set_title(f"Total {title} (Rs.) — v5", fontsize=12, fontweight="bold")
                ax1.set_xlabel("Total P&L (Rs.)")
                ax1.grid(True, alpha=0.3, axis="x")
                ax2.barh(range(len(grp)), grp["mean"].values, color=colors_setup2, alpha=0.85)
                ax2.set_yticks(range(len(grp)))
                ax2.set_yticklabels(grp.index, fontsize=9)
                ax2.set_title(f"Avg {title} per Trade (Rs.) — v5", fontsize=12, fontweight="bold")
                ax2.set_xlabel("Avg P&L (Rs.)")
                ax2.grid(True, alpha=0.3, axis="x")
                plt.tight_layout()
                _save(fig, f"{chart_num}_pnl_by_{col_name}")

    # 14. Quality Score vs P&L
    if "quality_score" in combined_sorted.columns:
        qs          = pd.to_numeric(combined_sorted["quality_score"], errors="coerce")
        pnl_scatter = _safe_col(combined_sorted, "pnl_pct")
        mask        = qs.notna()
        if mask.sum() > 5:
            fig, ax = plt.subplots(figsize=(10, 8))
            sides_col = combined_sorted.loc[mask, "side"].astype(str).str.upper() if "side" in combined_sorted.columns else pd.Series(["COMBINED"] * mask.sum())
            for s_name, color in [("SHORT", "#DC2626"), ("LONG", "#16A34A")]:
                s_mask = sides_col == s_name
                if s_mask.any():
                    ax.scatter(qs[mask][s_mask], pnl_scatter[mask][s_mask],
                               alpha=0.5, s=30, color=color, label=s_name, edgecolors="white", linewidth=0.3)
            valid = mask & qs.notna() & pnl_scatter.notna()
            if valid.sum() > 2:
                coeffs = np.polyfit(qs[valid], pnl_scatter[valid], 1)
                x_line = np.linspace(qs[valid].min(), qs[valid].max(), 100)
                ax.plot(x_line, np.polyval(coeffs, x_line), "--", color="#F59E0B", linewidth=2,
                        label=f"Trend (slope={coeffs[0]:.3f})")
            ax.axhline(0, color="grey", linewidth=0.8, linestyle="--")
            ax.set_title("Quality Score vs P&L (%) — v5", fontsize=14, fontweight="bold")
            ax.set_xlabel("Quality Score")
            ax.set_ylabel("P&L (%)")
            ax.legend(fontsize=10)
            ax.grid(True, alpha=0.3)
            _save(fig, "14_quality_score_vs_pnl")

    # 15. Avg P&L by Hour of Day
    if "entry_time_ist" in combined_sorted.columns:
        entry_t = pd.to_datetime(combined_sorted["entry_time_ist"], errors="coerce")
        combined_sorted["_entry_hour"] = entry_t.dt.hour
        hourly_pnl = combined_sorted.groupby("_entry_hour")["pnl_rs"].agg(["mean", "sum", "count"])
        hourly_pnl = hourly_pnl[(hourly_pnl.index >= 9) & (hourly_pnl.index <= 15)]
        if len(hourly_pnl) > 0:
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
            colors_h1 = ["#16A34A" if v >= 0 else "#DC2626" for v in hourly_pnl["mean"].values]
            colors_h2 = ["#16A34A" if v >= 0 else "#DC2626" for v in hourly_pnl["sum"].values]
            ax1.bar(hourly_pnl.index, hourly_pnl["mean"].values, color=colors_h1, alpha=0.85, width=0.6)
            ax1.set_title("Avg P&L per Trade by Entry Hour — v5", fontsize=12, fontweight="bold")
            ax1.set_xlabel("Hour (IST)")
            ax1.set_ylabel("Avg P&L (Rs.)")
            ax1.grid(True, alpha=0.3, axis="y")
            ax2.bar(hourly_pnl.index, hourly_pnl["sum"].values, color=colors_h2, alpha=0.85, width=0.6)
            ax2.set_title("Total P&L by Entry Hour — v5", fontsize=12, fontweight="bold")
            ax2.set_xlabel("Hour (IST)")
            ax2.set_ylabel("Total P&L (Rs.)")
            ax2.grid(True, alpha=0.3, axis="y")
            ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"₹{x:,.0f}"))
            plt.tight_layout()
            _save(fig, "15_avg_pnl_by_hour")
        combined_sorted.drop(columns=["_entry_hour"], inplace=True, errors="ignore")

    print(f"[CHARTS] Generated {len(saved)} charts in {save_dir}/")
    return saved


# ===========================================================================
# MAIN
# ===========================================================================
def main() -> None:
    _script_dir    = Path(__file__).resolve().parent
    _project_root  = _script_dir.parent if _script_dir.name == "avwap_v11_refactored" else _script_dir
    _outputs_dir   = _project_root / "outputs_v5"
    _outputs_dir.mkdir(parents=True, exist_ok=True)

    ts = now_ist().strftime("%Y%m%d_%H%M%S")
    log_path = _outputs_dir / f"avwap_combined_runner_v5_{ts}.txt"

    _orig_stdout, _orig_stderr = sys.stdout, sys.stderr
    with open(log_path, "w", encoding="utf-8") as _log_fh:
        sys.stdout = _Tee(_orig_stdout, _log_fh)
        sys.stderr = _Tee(_orig_stderr, _log_fh)

        try:
            print("=" * 72)
            print("AVWAP v11 COMBINED runner v5 — LONG + SHORT (Supertrend + MACD)")
            print("  Outputs → outputs_v5/")
            print(f"  SHORT: SL={SHORT_STOP_PCT*100:.2f}% | TGT={SHORT_TARGET_PCT*100:.2f}% | R:R={SHORT_TARGET_PCT/SHORT_STOP_PCT:.2f}:1")
            print(f"  LONG : SL={LONG_STOP_PCT*100:.2f}% | TGT={LONG_TARGET_PCT*100:.2f}% | R:R={LONG_TARGET_PCT/LONG_STOP_PCT:.2f}:1")
            print(f"  SHORT ADX: min={SHORT_ADX_MIN} slope={SHORT_ADX_SLOPE_MIN} | RSI max={SHORT_RSI_MAX} | Stoch max={SHORT_STOCHK_MAX}")
            print(f"  LONG  ADX: min={LONG_ADX_MIN}  slope={LONG_ADX_SLOPE_MIN}  | RSI min={LONG_RSI_MIN}  | Stoch min={LONG_STOCHK_MIN}")
            print(f"  Signal window: 09:15 – 15:00 (both sides)")
            print(f"  BE trigger: SHORT={SHORT_BE_TRIGGER_PCT*100:.2f}% / LONG={LONG_BE_TRIGGER_PCT*100:.2f}%")
            print(f"  Trail     : SHORT={SHORT_TRAIL_PCT*100:.2f}%  / LONG={LONG_TRAIL_PCT*100:.2f}%")
            print(f"  Leverage  : SHORT={INTRADAY_LEVERAGE_SHORT}x | LONG={INTRADAY_LEVERAGE_LONG}x")
            print(f"  NEW v5    : Supertrend(10,3.0) + MACD(12,26,9) directional filters")
            print("=" * 72)

            dir_5m = _resolve_5min_dir()
            print(f"[INFO] 5-min data directory: {dir_5m}")
            if dir_5m.is_dir():
                print(f"[INFO] 5-min parquet files found: {len(list(dir_5m.glob('*.parquet')))}")
            else:
                print("[WARN] 5-min data directory not found — falling back to 15-min exits.")

            # Build configs with v2/v5 overrides
            short_cfg = default_short_config(reports_dir=_outputs_dir)
            long_cfg  = default_long_config(reports_dir=_outputs_dir)

            # Risk
            short_cfg.stop_pct   = float(SHORT_STOP_PCT)
            short_cfg.target_pct = float(SHORT_TARGET_PCT)
            long_cfg.stop_pct    = float(LONG_STOP_PCT)
            long_cfg.target_pct  = float(LONG_TARGET_PCT)

            # BE / trail
            short_cfg.be_trigger_pct = float(SHORT_BE_TRIGGER_PCT)
            short_cfg.trail_pct      = float(SHORT_TRAIL_PCT)
            long_cfg.be_trigger_pct  = float(LONG_BE_TRIGGER_PCT)
            long_cfg.trail_pct       = float(LONG_TRAIL_PCT)

            # Trend filters
            short_cfg.adx_min       = float(SHORT_ADX_MIN)
            short_cfg.adx_slope_min = float(SHORT_ADX_SLOPE_MIN)
            short_cfg.rsi_max_short = float(SHORT_RSI_MAX)
            short_cfg.stochk_max    = float(SHORT_STOCHK_MAX)

            long_cfg.adx_min        = float(LONG_ADX_MIN)
            long_cfg.adx_slope_min  = float(LONG_ADX_SLOPE_MIN)
            long_cfg.rsi_min_long   = float(LONG_RSI_MIN)
            long_cfg.stochk_min     = float(LONG_STOCHK_MIN)
            long_cfg.stochk_max     = float(LONG_STOCHK_MAX)

            # Volume / ATR / AVWAP
            short_cfg.use_volume_filter  = long_cfg.use_volume_filter  = bool(USE_VOLUME_FILTER)
            short_cfg.volume_min_ratio   = long_cfg.volume_min_ratio   = float(VOLUME_MIN_RATIO)
            short_cfg.use_atr_pct_filter = long_cfg.use_atr_pct_filter = bool(USE_ATR_PCT_FILTER)
            short_cfg.atr_pct_min        = long_cfg.atr_pct_min        = float(ATR_PCT_MIN)
            short_cfg.avwap_dist_atr_mult = long_cfg.avwap_dist_atr_mult = float(AVWAP_DIST_ATR_MULT)

            # Entry quality
            short_cfg.require_entry_close_confirm = long_cfg.require_entry_close_confirm = bool(REQUIRE_ENTRY_CLOSE_CONFIRM)
            short_cfg.max_trades_per_ticker_per_day = long_cfg.max_trades_per_ticker_per_day = int(MAX_TRADES_PER_TICKER_PER_DAY)
            long_cfg.enable_setup_a_pullback_c2_break = bool(ENABLE_LONG_SETUP_A_PULLBACK_C2_BREAK)

            # Lag bars
            short_cfg.lag_bars_short_a_mod_break_c1_low          = int(SHORT_LAG_BARS_A_MOD_BREAK_C1_LOW)
            short_cfg.lag_bars_short_a_pullback_c2_break_c2_low  = int(SHORT_LAG_BARS_A_PULLBACK_C2_BREAK_C2_LOW)
            short_cfg.lag_bars_short_b_huge_failed_bounce         = int(SHORT_LAG_BARS_B_HUGE_FAILED_BOUNCE)
            long_cfg.lag_bars_long_a_mod_break_c1_high            = int(LONG_LAG_BARS_A_MOD_BREAK_C1_HIGH)
            long_cfg.lag_bars_long_a_pullback_c2_break_c2_high    = int(LONG_LAG_BARS_A_PULLBACK_C2_BREAK_C2_HIGH)
            long_cfg.lag_bars_long_b_huge_pullback_hold_break      = int(LONG_LAG_BARS_B_HUGE_PULLBACK_HOLD_BREAK)

            # Live parity
            if FORCE_LIVE_PARITY_MIN_BARS_LEFT:
                short_cfg.min_bars_left_after_entry = long_cfg.min_bars_left_after_entry = 0
            if FORCE_LIVE_PARITY_DISABLE_TOPN:
                short_cfg.enable_topn_per_day = long_cfg.enable_topn_per_day = False

            # Signal windows (applied last)
            if FINAL_SIGNAL_WINDOW_OVERRIDE:
                short_cfg.use_time_windows = bool(FINAL_SHORT_USE_TIME_WINDOWS)
                long_cfg.use_time_windows  = bool(FINAL_LONG_USE_TIME_WINDOWS)
                short_cfg.signal_windows   = list(FINAL_SHORT_SIGNAL_WINDOWS)
                long_cfg.signal_windows    = list(FINAL_LONG_SIGNAL_WINDOWS)

            print(f"[INFO] SHORT windows: {[(a.strftime('%H:%M'), b.strftime('%H:%M')) for a, b in short_cfg.signal_windows]}")
            print(f"[INFO] LONG  windows: {[(a.strftime('%H:%M'), b.strftime('%H:%M')) for a, b in long_cfg.signal_windows]}")
            print(f"[INFO] Live parity: min_bars_left=0={FORCE_LIVE_PARITY_MIN_BARS_LEFT} | disable_topn={FORCE_LIVE_PARITY_DISABLE_TOPN}")
            print(f"[INFO] max_workers={MAX_WORKERS} | disallow_both_sides_same_ticker={DISALLOW_BOTH_SIDES_SAME_TICKER_DAY}")
            print("-" * 72)

            # PHASE 1: Entry signals (15-min) using v5 scan (Supertrend + MACD)
            print("\n[PHASE 1] Scanning entry signals using 15-min data (v5: Supertrend + MACD)...")
            short_df = _run_side_parallel_v5("SHORT", short_cfg, MAX_WORKERS)
            long_df  = _run_side_parallel_v5("LONG",  long_cfg,  MAX_WORKERS)

            if short_df.empty and long_df.empty:
                print("[DONE] No trades found.")
                return

            # PHASE 2: Re-resolve exits (5-min)
            print("\n[PHASE 2] Re-resolving exits using 5-min data...")
            suffix_5m = ".parquet"
            if dir_5m.is_dir():
                sample_files = list(dir_5m.glob("*"))[:5]
                for sf in sample_files:
                    if sf.suffix:
                        suffix_5m = sf.suffix
                        break

            if not short_df.empty:
                print(f"  [SHORT] {len(short_df)} trades to re-resolve...")
                short_df = _resolve_exits_5min(short_df, dir_5m, suffix_5m, short_cfg.parquet_engine)
            if not long_df.empty:
                print(f"  [LONG] {len(long_df)} trades to re-resolve...")
                long_df = _resolve_exits_5min(long_df, dir_5m, suffix_5m, long_cfg.parquet_engine)

            # Notional P&L
            if not short_df.empty:
                short_df = _add_notional_pnl(short_df)
                short_df = _sort_trades_for_output(short_df)
            if not long_df.empty:
                long_df = _add_notional_pnl(long_df)
                long_df = _sort_trades_for_output(long_df)

            combined = pd.concat([short_df, long_df], ignore_index=True)
            combined = _add_notional_pnl(combined)
            combined = _sort_trades_for_output(combined)

            _print_day_side_mix(combined)
            _print_signal_entry_lag_summary(combined)

            # Metrics
            if not short_df.empty:
                print_metrics("SHORT v5 (5-min exits)", compute_backtest_metrics(short_df))
            if not long_df.empty:
                print_metrics("LONG v5 (5-min exits)", compute_backtest_metrics(long_df))
            print_metrics("COMBINED v5 (5-min exits)", compute_backtest_metrics(combined))
            _print_notional_pnl(combined)

            # Optional portfolio sim
            if ENABLE_CASH_CONSTRAINED_PORTFOLIO_SIM:
                sim_df, pstats = _simulate_cash_constrained(combined)
                _print_portfolio(pstats)
                combined = _sort_trades_for_output(sim_df)
                _print_day_side_mix(combined)

            # Save CSV
            out_csv = _outputs_dir / f"avwap_longshort_trades_ALL_DAYS_v5_{ts}.csv"
            combined.to_csv(out_csv, index=False)

            # Charts
            print("\n[INFO] Generating charts...")
            chart_dir_legacy   = _outputs_dir / "charts_v5" / "legacy"
            chart_dir_enhanced = _outputs_dir / "charts_v5" / "enhanced"
            legacy_files   = generate_backtest_charts(combined, short_df, long_df, save_dir=chart_dir_legacy,   ts_label=f"{ts}_v5")
            enhanced_files = generate_enhanced_charts(combined, short_df, long_df, save_dir=chart_dir_enhanced, ts_label=f"{ts}_v5")
            print(f"[INFO] Charts: legacy={len(legacy_files or [])} | enhanced={len(enhanced_files or [])}")

            # Sample output
            cols = [c for c in [
                "trade_date", "ticker", "side", "setup", "impulse_type",
                "quality_score", "entry_price", "exit_price", "outcome",
                "pnl_pct", "pnl_pct_price", "leverage", "position_size_rs",
                "notional_exposure_rs", "pnl_rs",
            ] if c in combined.columns]
            print("\n=============== SAMPLE (first 30 rows) ===============")
            print(combined.head(30)[cols].to_string(index=False))
            print(f"\n[FILE SAVED] {out_csv}")
            print(f"[OUTPUTS DIR] {_outputs_dir}")
            print("[DONE]")

        finally:
            sys.stdout = _orig_stdout
            sys.stderr = _orig_stderr

    print(f"[LOG SAVED] {log_path}")


if __name__ == "__main__":
    main()
