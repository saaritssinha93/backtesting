# -*- coding: utf-8 -*-
"""
avwap_combined_runner_v12_sweep.py  —  V12 parameter sweep runner
=================================================================
Sweeps SHORT/LONG SL, target, VIX cap, ADX, volume ratio.
All params via CLI  —  original avwap_combined_runner_v12.py is UNTOUCHED.

Usage:
    python avwap_combined_runner_v12_sweep.py --cfg C1 --s_sl 0.0062 --s_tgt 0.0088 \
           --l_sl 0.0050 --l_tgt 0.0110 --vix_l 13.0
"""

from __future__ import annotations

import argparse
import os
import sys
import warnings
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict
from datetime import time as dtime
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Tee stdout/stderr to file
# ---------------------------------------------------------------------------
class _Tee:
    def __init__(self, *streams):
        self.streams = [s for s in streams if s is not None]
    def write(self, data):
        for s in self.streams:
            try: s.write(data)
            except Exception: pass
    def flush(self):
        for s in self.streams:
            try: s.flush()
            except Exception: pass
    def isatty(self): return False


# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------
_this_dir    = Path(__file__).resolve().parent
_project_root = _this_dir.parent if _this_dir.name == "avwap_v11_refactored" else _this_dir
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from avwap_v11_refactored.avwap_common_v11 import (
    IST, StrategyConfig, Trade, BacktestMetrics,
    default_short_config, default_long_config, now_ist,
    trades_to_df, apply_topn_per_day, compute_backtest_metrics,
    print_metrics, read_15m_parquet, list_tickers_15m,
    generate_backtest_charts, build_market_regime_map,
)
from avwap_v11_refactored.avwap_short_strategy_v11 import (
    scan_all_days_for_ticker as scan_short,
)
from avwap_v11_refactored.avwap_long_strategy_v9_sweep import (
    scan_all_days_for_ticker as scan_long,
)
from avwap_v11_refactored.avwap_common_v7_sweep import (
    default_long_config as default_long_config_v9,
)

# ---------------------------------------------------------------------------
# Constants (same as V12 baseline)
# ---------------------------------------------------------------------------
POSITION_SIZE_RS_SHORT  = 50_000
POSITION_SIZE_RS_LONG   = 50_000
INTRADAY_LEVERAGE_SHORT = 5.0
INTRADAY_LEVERAGE_LONG  = 5.0
MAX_WORKERS             = 4

FINAL_SIGNAL_WINDOW_OVERRIDE = True
FINAL_SHORT_USE_TIME_WINDOWS = True
FINAL_SHORT_SIGNAL_WINDOWS   = [(dtime(9, 15, 0), dtime(14, 30, 0))]
FINAL_LONG_USE_TIME_WINDOWS  = True
FINAL_LONG_SIGNAL_WINDOWS    = [(dtime(9, 15, 0), dtime(14, 30, 0))]

SHORT_LAG_BARS_A_MOD_BREAK_C1_LOW         = 1
SHORT_LAG_BARS_A_PULLBACK_C2_BREAK_C2_LOW = 2
SHORT_LAG_BARS_B_HUGE_FAILED_BOUNCE       = -1
LONG_LAG_BARS_A_MOD_BREAK_C1_HIGH         = 1
LONG_LAG_BARS_A_PULLBACK_C2_BREAK_C2_HIGH = 2
LONG_LAG_BARS_B_HUGE_PULLBACK_HOLD_BREAK  = -1

PACK2_ENABLE_SHORT_SETUP_B_HUGE_FAILED_BOUNCE = True

# ---------------------------------------------------------------------------
# CLI args
# ---------------------------------------------------------------------------
def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="V12 sweep runner")
    # Config identity
    p.add_argument("--cfg",    type=str,   default="C0",   help="Config name (used for output dir)")
    # SHORT SL / Target / BE / Trail
    p.add_argument("--s_sl",   type=float, default=0.0062, help="SHORT stop_pct")
    p.add_argument("--s_tgt",  type=float, default=0.0088, help="SHORT target_pct")
    p.add_argument("--s_be",   type=float, default=0.0042, help="SHORT be_trigger_pct")
    p.add_argument("--s_trail",type=float, default=0.0023, help="SHORT trail_pct")
    # LONG SL / Target / BE / Trail
    p.add_argument("--l_sl",   type=float, default=0.0050, help="LONG stop_pct")
    p.add_argument("--l_tgt",  type=float, default=0.0110, help="LONG target_pct")
    p.add_argument("--l_be",   type=float, default=0.0055, help="LONG be_trigger_pct")
    p.add_argument("--l_trail",type=float, default=0.0028, help="LONG trail_pct")
    # VIX caps (0 = disabled)
    p.add_argument("--vix_s",  type=float, default=0.0,   help="SHORT max VIX for entries (0=off)")
    p.add_argument("--vix_l",  type=float, default=0.0,   help="LONG  max VIX for entries (0=off)")
    # Volume / ADX filters
    p.add_argument("--vol_s",  type=float, default=1.20,  help="SHORT volume_min_ratio")
    p.add_argument("--vol_l",  type=float, default=1.15,  help="LONG  volume_min_ratio")
    p.add_argument("--adx_s",  type=float, default=22.0,  help="SHORT adx_min")
    p.add_argument("--adx_l",  type=float, default=22.0,  help="LONG  adx_min")
    # VIX dynamic scaling
    p.add_argument("--vix_scale", action="store_true",    help="Enable VIX dynamic scaling")
    # Partial exit on SHORT (True by default, matching V12 profile)
    p.add_argument("--no_partial", action="store_true",   help="Disable partial exit on SHORT")
    # RSI / Stoch filters
    p.add_argument("--rsi_max_s", type=float, default=55.0, help="SHORT rsi_max_short")
    p.add_argument("--rsi_min_l", type=float, default=45.0, help="LONG  rsi_min_long")
    p.add_argument("--stochk_max_s", type=float, default=78.0, help="SHORT stochk_max")
    return p.parse_args()

# ---------------------------------------------------------------------------
# 1-min data helpers (identical to V12)
# ---------------------------------------------------------------------------
def _resolve_5min_dir() -> Path:
    candidates = [
        _project_root / "data" / "stocks_indicators_1min_eq",
        _project_root / "stocks_indicators_1min_eq",
        _project_root.parent / "data" / "stocks_indicators_1min_eq",
        _project_root.parent / "stocks_indicators_1min_eq",
    ]
    for c in candidates:
        if c.is_dir():
            return c
    return candidates[0]


def read_5m_parquet(path: str, engine: str = "pyarrow") -> pd.DataFrame:
    try:
        df = pd.read_parquet(path, engine=engine)
        for col in ["datetime", "Datetime", "Date", "date", "timestamp"]:
            if col in df.columns:
                df.rename(columns={col: "datetime"}, inplace=True)
                break
        if "datetime" not in df.columns and isinstance(df.index, pd.DatetimeIndex):
            df = df.reset_index().rename(columns={"index": "datetime"})
        if "datetime" in df.columns:
            df["datetime"] = pd.to_datetime(df["datetime"], utc=False, errors="coerce")
            if df["datetime"].dt.tz is not None:
                df["datetime"] = df["datetime"].dt.tz_convert(IST).dt.tz_localize(None)
        return df
    except Exception:
        return pd.DataFrame()


def _resolve_exits_5min(df: pd.DataFrame, dir_5m: Path, suffix: str, engine: str) -> pd.DataFrame:
    required = {"ticker", "side", "entry_price", "entry_time_ist", "stop_price", "target_price"}
    if not required.issubset(set(df.columns)):
        return df
    for c in ["entry_time_ist", "exit_time_ist", "signal_time_ist"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")
    _cache: Dict[str, pd.DataFrame] = {}
    updated = 0
    for idx in df.index:
        ticker      = str(df.at[idx, "ticker"])
        side        = str(df.at[idx, "side"]).upper()
        entry_price = float(df.at[idx, "entry_price"])
        entry_time  = df.at[idx, "entry_time_ist"]
        stop_price  = float(df.at[idx, "stop_price"])  if pd.notna(df.at[idx, "stop_price"])   else None
        target_price= float(df.at[idx, "target_price"])if pd.notna(df.at[idx, "target_price"]) else None
        if pd.isna(entry_time) or stop_price is None or target_price is None:
            continue
        if ticker not in _cache:
            found = False
            for pat in [f"{ticker}{suffix}", f"{ticker}.parquet",
                        f"{ticker}_5min.parquet", f"{ticker}_1min.parquet"]:
                fp = dir_5m / pat
                if fp.exists():
                    _cache[ticker] = read_5m_parquet(str(fp), engine)
                    found = True; break
            if not found:
                _cache[ticker] = pd.DataFrame()
        df5 = _cache[ticker]
        if df5.empty:
            continue
        trade_date = pd.Timestamp(entry_time).normalize()
        mask = (df5["datetime"] > entry_time) & (df5["datetime"].dt.normalize() == trade_date)
        bars = df5.loc[mask].sort_values("datetime")
        if bars.empty:
            continue
        new_exit_price = new_exit_time = new_outcome = None
        for _, bar in bars.iterrows():
            bh = float(bar.get("high", bar.get("High", np.nan)))
            bl = float(bar.get("low",  bar.get("Low",  np.nan)))
            bt = bar["datetime"]
            if np.isnan(bh) or np.isnan(bl):
                continue
            if side == "SHORT":
                if bh >= stop_price:
                    new_exit_price, new_exit_time, new_outcome = stop_price, bt, "SL"; break
                if bl <= target_price:
                    new_exit_price, new_exit_time, new_outcome = target_price, bt, "TARGET"; break
            else:
                if bl <= stop_price:
                    new_exit_price, new_exit_time, new_outcome = stop_price, bt, "SL"; break
                if bh >= target_price:
                    new_exit_price, new_exit_time, new_outcome = target_price, bt, "TARGET"; break
        if new_exit_price is None:
            lb = bars.iloc[-1]
            new_exit_price = float(lb.get("close", lb.get("Close", entry_price)))
            new_exit_time  = lb["datetime"]
            new_outcome    = "EOD"
        df.at[idx, "exit_price"]   = new_exit_price
        df.at[idx, "exit_time_ist"]= new_exit_time
        df.at[idx, "outcome"]      = new_outcome
        if side == "SHORT":
            raw_pct = (entry_price - new_exit_price) / entry_price * 100.0 if entry_price else 0.0
        else:
            raw_pct = (new_exit_price - entry_price) / entry_price * 100.0 if entry_price else 0.0
        df.at[idx, "pnl_pct_gross"] = raw_pct
        slip = float(df.at[idx, "slippage_pct"])   if "slippage_pct"   in df.columns and pd.notna(df.at[idx, "slippage_pct"])   else 0.0005
        comm = float(df.at[idx, "commission_pct"]) if "commission_pct" in df.columns and pd.notna(df.at[idx, "commission_pct"]) else 0.0003
        df.at[idx, "pnl_pct"] = raw_pct - (slip + comm) * 100.0 * 2
        updated += 1
    print(f"[1MIN] Re-resolved exits for {updated}/{len(df)} trades using 1-min data.")
    return df


# ---------------------------------------------------------------------------
# Worker functions
# ---------------------------------------------------------------------------
def _scan_one_short(args: Tuple[str, str, StrategyConfig]) -> List[dict]:
    ticker, path, cfg = args
    df = read_15m_parquet(path, cfg.parquet_engine)
    return [asdict(t) for t in scan_short(ticker, df, cfg)] if not df.empty else []


def _scan_one_long(args: Tuple[str, str, StrategyConfig]) -> List[dict]:
    ticker, path, cfg = args
    df = read_15m_parquet(path, cfg.parquet_engine)
    return [asdict(t) for t in scan_long(ticker, df, cfg)] if not df.empty else []


def _run_side_parallel(side: str, cfg, max_workers: int) -> pd.DataFrame:
    tickers   = list_tickers_15m(cfg.dir_15m, cfg.end_15m)
    print(f"[{side}] Tickers found: {len(tickers)}")
    worker_fn = _scan_one_short if side == "SHORT" else _scan_one_long
    task_args = [(t, os.path.join(cfg.dir_15m, f"{t}{cfg.end_15m}"), cfg) for t in tickers]
    all_dicts: List[dict] = []
    done = 0
    with ProcessPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(worker_fn, a): a[0] for a in task_args}
        for fut in as_completed(futs):
            done += 1
            try:
                all_dicts.extend(fut.result())
            except Exception as e:
                print(f"  [{side}] ERROR {futs[fut]}: {e}")
            if done % 100 == 0:
                print(f"  [{side}] scanned {done}/{len(tickers)} | trades={len(all_dicts)}")
    print(f"  [{side}] scanned {len(tickers)}/{len(tickers)} | trades={len(all_dicts)}")
    if not all_dicts:
        return pd.DataFrame()
    df = pd.DataFrame(all_dicts)
    # Fill missing position_size_rs
    if "position_size_rs" not in df.columns:
        df["position_size_rs"] = float(
            POSITION_SIZE_RS_SHORT if side == "SHORT" else POSITION_SIZE_RS_LONG)
    else:
        fill = float(POSITION_SIZE_RS_SHORT if side == "SHORT" else POSITION_SIZE_RS_LONG)
        mask = df["position_size_rs"].isna() | (df["position_size_rs"] <= 0)
        df.loc[mask, "position_size_rs"] = fill
    return df


def _add_notional_pnl(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "pnl_pct" not in df.columns:
        return df
    df = df.copy()
    side_u = df["side"].astype(str).str.upper() if "side" in df.columns else pd.Series(["LONG"] * len(df))
    if "position_size_rs" not in df.columns:
        df["position_size_rs"] = POSITION_SIZE_RS_LONG
    mask_s = side_u.eq("SHORT")
    df.loc[mask_s  & (df["position_size_rs"].isna() | (df["position_size_rs"] <= 0)), "position_size_rs"] = float(POSITION_SIZE_RS_SHORT)
    df.loc[~mask_s & (df["position_size_rs"].isna() | (df["position_size_rs"] <= 0)), "position_size_rs"] = float(POSITION_SIZE_RS_LONG)
    lev = pd.Series(INTRADAY_LEVERAGE_LONG, index=df.index)
    lev[mask_s] = INTRADAY_LEVERAGE_SHORT
    df["leverage"]             = lev
    df["notional_exposure_rs"] = df["position_size_rs"] * lev
    df["pnl_rs"]               = df["pnl_pct"] / 100.0 * df["position_size_rs"] * lev
    return df


def _sort_trades(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df
    for c in ["trade_date", "entry_time_ist"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")
    sort_by = [c for c in ["trade_date", "entry_time_ist", "ticker"] if c in df.columns]
    return df.sort_values(sort_by).reset_index(drop=True) if sort_by else df


# ---------------------------------------------------------------------------
# VIX loader
# ---------------------------------------------------------------------------
def _load_india_vix(proj: Path) -> dict:
    fp = proj / "india_vix.parquet"
    if not fp.exists():
        return {}
    try:
        df = pd.read_parquet(fp)
        df.columns = [c.lower() for c in df.columns]
        if "date" not in df.columns and df.index.dtype == "datetime64[ns]":
            df = df.reset_index().rename(columns={"index": "date"})
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
        vc = next((c for c in ["close", "vix", "india_vix"] if c in df.columns), None)
        return dict(zip(df["date"], df[vc].astype(float))) if vc else {}
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# Print helpers
# ---------------------------------------------------------------------------
def _print_notional_pnl(df: pd.DataFrame) -> None:
    if df.empty or "pnl_rs" not in df.columns: return
    side_u = df["side"].astype(str).str.upper()
    s_pnl = df.loc[side_u.eq("SHORT"), "pnl_rs"].sum()
    l_pnl = df.loc[~side_u.eq("SHORT"), "pnl_rs"].sum()
    print("\n==================== NOTIONAL P&L SUMMARY (Rs.) ====================")
    print(f"SHORT notional P&L            : Rs.{s_pnl:,.2f}")
    print(f"LONG  notional P&L            : Rs.{l_pnl:,.2f}")
    print(f"TOTAL notional P&L            : Rs.{s_pnl + l_pnl:,.2f}")


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main() -> None:
    args = _parse_args()

    _outputs_base = _project_root / "outputs_v12_sweep"
    _outputs_dir  = _outputs_base / args.cfg
    _outputs_dir.mkdir(parents=True, exist_ok=True)

    ts       = now_ist().strftime("%Y%m%d_%H%M%S")
    log_path = _outputs_dir / f"sweep_{args.cfg}_{ts}.txt"

    _orig_stdout, _orig_stderr = sys.stdout, sys.stderr
    with open(log_path, "w", encoding="utf-8") as _log_fh:
        sys.stdout = _Tee(_orig_stdout, _log_fh)
        sys.stderr = _Tee(_orig_stderr, _log_fh)
        try:
            print("=" * 70)
            print(f"V12 SWEEP — Config: {args.cfg}")
            print(f"  SHORT: SL={args.s_sl*100:.2f}%  TGT={args.s_tgt*100:.2f}%  "
                  f"BE={args.s_be*100:.2f}%  Trail={args.s_trail*100:.2f}%")
            print(f"         ADX>={args.adx_s}  Vol>={args.vol_s}  "
                  f"RSI<={args.rsi_max_s}  StochK<={args.stochk_max_s}  "
                  f"VIX<={args.vix_s if args.vix_s>0 else 'OFF'}")
            print(f"  LONG:  SL={args.l_sl*100:.2f}%  TGT={args.l_tgt*100:.2f}%  "
                  f"BE={args.l_be*100:.2f}%  Trail={args.l_trail*100:.2f}%")
            print(f"         ADX>={args.adx_l}  Vol>={args.vol_l}  "
                  f"RSI>={args.rsi_min_l}  "
                  f"VIX<={args.vix_l if args.vix_l>0 else 'OFF'}")
            print(f"  VIX scaling: {'ON' if args.vix_scale else 'OFF'}  "
                  f"Partial exit SHORT: {'OFF' if args.no_partial else 'ON'}")
            print("=" * 70)

            dir_5m = _resolve_5min_dir()

            # ---- Build configs ----
            short_cfg = default_short_config(reports_dir=_outputs_dir)
            long_cfg  = default_long_config_v9(reports_dir=_outputs_dir)

            # --- SHORT profile (V11 playbook) ---
            short_cfg.enable_liquidity_sweep_filter      = False
            short_cfg.reversal_requires_sweep            = True
            short_cfg.enable_avwap_no_trade_zone         = True
            short_cfg.enable_mode_selector               = True
            short_cfg.use_time_windows                   = False
            short_cfg.min_bars_left_after_entry          = 0
            short_cfg.enable_ema200_filter               = False
            short_cfg.require_vwap_side_persistence      = False
            short_cfg.vwap_side_lookback_bars            = 5
            short_cfg.vwap_side_min_count                = 3
            short_cfg.require_structure_filter           = False
            short_cfg.structure_lookback_bars            = 30
            short_cfg.max_trades_per_ticker_per_day      = 2
            short_cfg.enable_topn_per_day                = False
            short_cfg.topn_per_day                       = 0
            short_cfg.enable_risk_based_position_sizing  = False
            short_cfg.risk_per_trade_pct_of_capital      = 0.0035
            short_cfg.enable_partial_exit                = not args.no_partial
            short_cfg.partial_exit_fraction              = 0.50
            short_cfg.partial_target_fraction            = 0.50
            # Sweepable params
            short_cfg.stop_pct        = args.s_sl
            short_cfg.target_pct      = args.s_tgt
            short_cfg.be_trigger_pct  = args.s_be
            short_cfg.trail_pct       = args.s_trail
            short_cfg.adx_min         = args.adx_s
            short_cfg.adx_slope_min   = 0.80
            short_cfg.volume_min_ratio= args.vol_s
            short_cfg.rsi_max_short   = args.rsi_max_s
            short_cfg.stochk_max      = args.stochk_max_s

            # --- LONG profile (V9 s5) ---
            long_cfg.require_entry_close_confirm             = True
            long_cfg.enable_liquidity_sweep_filter           = False
            long_cfg.enable_avwap_no_trade_zone              = False
            long_cfg.atr_pct_min                             = 0.0025
            long_cfg.enable_setup_a_close_continuation_break= False
            long_cfg.enable_setup_b_huge_c1_close_reclaim_break = False
            long_cfg.enable_topn_per_day                     = False
            long_cfg.topn_per_day                            = 0
            long_cfg.min_bars_left_after_entry               = 0
            long_cfg.rsi_min_long                            = args.rsi_min_l
            long_cfg.stochk_min                              = 25.0
            long_cfg.stochk_max                              = 95.0
            # Sweepable params
            long_cfg.stop_pct         = args.l_sl
            long_cfg.target_pct       = args.l_tgt
            long_cfg.be_trigger_pct   = args.l_be
            long_cfg.trail_pct        = args.l_trail
            long_cfg.adx_min          = args.adx_l
            long_cfg.adx_slope_min    = 0.85
            long_cfg.volume_min_ratio = args.vol_l

            # Lag bars
            short_cfg.lag_bars_short_a_mod_break_c1_low         = SHORT_LAG_BARS_A_MOD_BREAK_C1_LOW
            short_cfg.lag_bars_short_a_pullback_c2_break_c2_low = SHORT_LAG_BARS_A_PULLBACK_C2_BREAK_C2_LOW
            short_cfg.lag_bars_short_b_huge_failed_bounce        = SHORT_LAG_BARS_B_HUGE_FAILED_BOUNCE
            short_cfg.enable_setup_b_huge_failed_bounce          = PACK2_ENABLE_SHORT_SETUP_B_HUGE_FAILED_BOUNCE
            long_cfg.lag_bars_long_a_mod_break_c1_high           = LONG_LAG_BARS_A_MOD_BREAK_C1_HIGH
            long_cfg.lag_bars_long_a_pullback_c2_break_c2_high   = LONG_LAG_BARS_A_PULLBACK_C2_BREAK_C2_HIGH
            long_cfg.lag_bars_long_b_huge_pullback_hold_break     = LONG_LAG_BARS_B_HUGE_PULLBACK_HOLD_BREAK

            # VIX caps (0 = disabled, matches V12 PACK2 logic)
            short_cfg.max_vix_for_entries = float(args.vix_s)
            long_cfg.max_vix_for_entries  = float(args.vix_l)

            # Signal windows
            if FINAL_SIGNAL_WINDOW_OVERRIDE:
                short_cfg.use_time_windows = FINAL_SHORT_USE_TIME_WINDOWS
                long_cfg.use_time_windows  = FINAL_LONG_USE_TIME_WINDOWS
                short_cfg.signal_windows   = list(FINAL_SHORT_SIGNAL_WINDOWS)
                long_cfg.signal_windows    = list(FINAL_LONG_SIGNAL_WINDOWS)

            # VIX: always load daily map so max_vix_for_entries cap works
            _vix_map = _load_india_vix(_project_root)
            short_cfg.vix_scale_enabled = args.vix_scale
            long_cfg.vix_scale_enabled  = args.vix_scale
            if _vix_map:
                # Always assign vix_daily so max_vix_for_entries filter is active
                for cfg_ in (short_cfg, long_cfg):
                    cfg_.vix_daily = _vix_map
            if args.vix_scale and _vix_map:
                for cfg_ in (short_cfg, long_cfg):
                    cfg_.vix_baseline     = 11.5
                    cfg_.vix_scale_min    = 0.75
                    cfg_.vix_scale_max    = 1.50
                    cfg_.vix_scale_target = True
                    cfg_.vix_scale_sl     = True
                print(f"[VIX] Dynamic scaling ON — {len(_vix_map)} days loaded.")
            elif _vix_map:
                print(f"[VIX] Scaling OFF but vix_daily loaded ({len(_vix_map)} days) — max_vix_for_entries cap active.")
            else:
                print("[VIX] No VIX data found — max_vix_for_entries cap disabled.")

            # Market regime
            regime_map, regime_source = build_market_regime_map(short_cfg)
            for cfg_ in (short_cfg, long_cfg):
                if regime_map:
                    cfg_.market_regime_map         = regime_map
                    cfg_.enable_market_regime_filter= True
                else:
                    cfg_.enable_market_regime_filter= False
            if regime_map:
                print(f"[REGIME] Enabled via {regime_source}: {len(regime_map)} bars.")
            else:
                print("[REGIME] Disabled — no market index found.")

            print(f"[INFO] SHORT: SL={short_cfg.stop_pct*100:.2f}%  TGT={short_cfg.target_pct*100:.2f}%")
            print(f"[INFO] LONG:  SL={long_cfg.stop_pct*100:.2f}%   TGT={long_cfg.target_pct*100:.2f}%")
            print(f"[INFO] Output: {_outputs_dir}")
            print("-" * 70)

            # ---- Phase 1: Scan ----
            print("\n[PHASE 1] Scanning entry signals (15-min data)...")
            short_df = _run_side_parallel("SHORT", short_cfg, MAX_WORKERS)
            long_df  = _run_side_parallel("LONG",  long_cfg,  MAX_WORKERS)

            if short_df.empty and long_df.empty:
                print("[DONE] No trades found.")
                return

            # ---- Phase 2: 1-min exit resolution ----
            print("\n[PHASE 2] Re-resolving exits (1-min data)...")
            suffix = ".parquet"
            if dir_5m.is_dir():
                for sf in list(dir_5m.glob("*"))[:5]:
                    if sf.suffix:
                        suffix = sf.suffix; break

            if not short_df.empty:
                print(f"  [SHORT] {len(short_df)} trades to re-resolve...")
                short_df = _resolve_exits_5min(short_df, dir_5m, suffix, short_cfg.parquet_engine)
            if not long_df.empty:
                print(f"  [LONG] {len(long_df)} trades to re-resolve...")
                long_df = _resolve_exits_5min(long_df, dir_5m, suffix, long_cfg.parquet_engine)

            # ---- P&L ----
            if not short_df.empty:
                short_df = _add_notional_pnl(short_df)
                short_df = _sort_trades(short_df)
            if not long_df.empty:
                long_df = _add_notional_pnl(long_df)
                long_df = _sort_trades(long_df)

            combined = pd.concat([short_df, long_df], ignore_index=True)
            combined = _add_notional_pnl(combined)
            combined = _sort_trades(combined)
            side_u   = combined["side"].astype(str).str.upper()
            short_df = _sort_trades(combined[side_u.eq("SHORT")].copy())
            long_df  = _sort_trades(combined[~side_u.eq("SHORT")].copy())

            # ---- Metrics ----
            print_metrics("SHORT (net slippage+comm, 1-min exits)", compute_backtest_metrics(short_df))
            print_metrics("LONG  (net slippage+comm, 1-min exits)", compute_backtest_metrics(long_df))
            print_metrics("COMBINED (net slippage+comm, 1-min exits)", compute_backtest_metrics(combined))
            _print_notional_pnl(combined)

            # ---- Save CSV ----
            out_csv = _outputs_dir / f"trades_v12_sweep_{args.cfg}_{ts}.csv"
            combined.to_csv(out_csv, index=False)
            print(f"\n[FILE SAVED] {out_csv}")
            print(f"[OUTPUTS DIR] {_outputs_dir}")
            print("[DONE]")

        finally:
            sys.stdout = _orig_stdout
            sys.stderr = _orig_stderr


if __name__ == "__main__":
    main()
