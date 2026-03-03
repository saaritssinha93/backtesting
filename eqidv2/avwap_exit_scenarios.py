# -*- coding: utf-8 -*-
"""
Shared 5-minute exit scenario resolvers for VT4/VT5/VT6 runners.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import numpy as np
import pandas as pd

from avwap_combined_runner import read_5m_parquet


def _pick_col(df: pd.DataFrame, *candidates: str) -> Optional[str]:
    for col in candidates:
        if col in df.columns:
            return col
    return None


def _clip_float(value: float, lo: float, hi: float) -> float:
    return float(min(max(value, lo), hi))


def _finalize_pnl(df: pd.DataFrame, idx: Any, side: str, entry_price: float, exit_price: float) -> None:
    if side == "SHORT":
        raw_pct = (entry_price - exit_price) / entry_price * 100.0 if entry_price else 0.0
    else:
        raw_pct = (exit_price - entry_price) / entry_price * 100.0 if entry_price else 0.0

    df.at[idx, "pnl_pct_gross"] = raw_pct

    slippage_pct = float(df.at[idx, "slippage_pct"]) if "slippage_pct" in df.columns and pd.notna(
        df.at[idx, "slippage_pct"]
    ) else 0.0005
    commission_pct = float(df.at[idx, "commission_pct"]) if "commission_pct" in df.columns and pd.notna(
        df.at[idx, "commission_pct"]
    ) else 0.0003
    cost_pct = (slippage_pct + commission_pct) * 100.0 * 2
    df.at[idx, "pnl_pct"] = raw_pct - cost_pct


def _load_5m_bars(
    ticker: str,
    dir_5m: Path,
    suffix_5m: str,
    engine: str,
    cache: Dict[str, pd.DataFrame],
) -> pd.DataFrame:
    if ticker in cache:
        return cache[ticker]

    found = None
    for pattern in (
        f"{ticker}{suffix_5m}",
        f"{ticker}.parquet",
        f"{ticker}_5min.parquet",
        f"{ticker}_stocks_indicators_5min.parquet",
        f"{ticker}_1min.parquet",
        f"{ticker}_stocks_indicators_1min.parquet",
    ):
        candidate = dir_5m / pattern
        if candidate.exists():
            found = candidate
            break

    if found is None:
        cache[ticker] = pd.DataFrame()
        return cache[ticker]

    bars = read_5m_parquet(str(found), engine)
    if bars.empty:
        cache[ticker] = bars
        return bars

    dt_col = _pick_col(bars, "datetime", "date", "DateTime", "timestamp", "Timestamp")
    if dt_col is None:
        cache[ticker] = pd.DataFrame()
        return cache[ticker]
    if dt_col != "datetime":
        bars = bars.rename(columns={dt_col: "datetime"})

    bars["datetime"] = pd.to_datetime(bars["datetime"], errors="coerce")
    bars = bars.dropna(subset=["datetime"]).sort_values("datetime").reset_index(drop=True)
    cache[ticker] = bars
    return bars


def _prepare_exit_df(trades_df: pd.DataFrame) -> pd.DataFrame:
    df = trades_df.copy()
    if "stop_price" not in df.columns and "sl_price" in df.columns:
        df["stop_price"] = df["sl_price"]

    for col in ("entry_time_ist", "exit_time_ist", "signal_time_ist"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def _required_columns_present(df: pd.DataFrame) -> bool:
    required = {"ticker", "side", "entry_price", "entry_time_ist", "stop_price", "target_price"}
    return required.issubset(set(df.columns))


def resolve_exits_partial_half_target_5min(
    trades_df: pd.DataFrame,
    dir_5m: Path,
    suffix_5m: str = ".parquet",
    engine: str = "pyarrow",
) -> pd.DataFrame:
    """
    Scenario VT4:
    - Exit half quantity at half target.
    - Move remaining stop to entry.
    - Keep remaining half for the original target.
    """
    if trades_df.empty:
        return trades_df
    if not dir_5m.is_dir():
        print(f"[VT4][WARN] 5-min data directory not found: {dir_5m}.")
        return trades_df

    df = _prepare_exit_df(trades_df)
    if not _required_columns_present(df):
        print("[VT4][WARN] Missing required columns for 5-min partial-exit resolver.")
        return df

    cache: Dict[str, pd.DataFrame] = {}
    updated = 0

    for idx in df.index:
        ticker = str(df.at[idx, "ticker"])
        side = str(df.at[idx, "side"]).upper()
        entry_time = df.at[idx, "entry_time_ist"]
        entry_price = float(df.at[idx, "entry_price"])
        stop_price = float(df.at[idx, "stop_price"]) if pd.notna(df.at[idx, "stop_price"]) else np.nan
        target_full = float(df.at[idx, "target_price"]) if pd.notna(df.at[idx, "target_price"]) else np.nan

        if pd.isna(entry_time) or not np.isfinite(stop_price) or not np.isfinite(target_full):
            continue
        if not np.isfinite(entry_price) or entry_price <= 0:
            continue
        if side not in ("SHORT", "LONG"):
            continue

        bars_all = _load_5m_bars(ticker, dir_5m, suffix_5m, engine, cache)
        if bars_all.empty:
            continue

        high_col = _pick_col(bars_all, "high", "High")
        low_col = _pick_col(bars_all, "low", "Low")
        close_col = _pick_col(bars_all, "close", "Close")
        if not high_col or not low_col or not close_col:
            continue

        trade_day = pd.Timestamp(entry_time).normalize()
        bars = bars_all[
            (bars_all["datetime"] > entry_time)
            & (bars_all["datetime"].dt.normalize() == trade_day)
        ].sort_values("datetime")
        if bars.empty:
            continue

        target_half = float(entry_price + (target_full - entry_price) * 0.5)

        leg1_open = True
        leg2_open = True
        leg2_stop = float(stop_price)

        leg1_exit_price = np.nan
        leg1_exit_time = pd.NaT
        leg1_outcome = ""

        leg2_exit_price = np.nan
        leg2_exit_time = pd.NaT
        leg2_outcome = ""

        for _, bar in bars.iterrows():
            bar_time = bar["datetime"]
            bar_high = float(pd.to_numeric(bar.get(high_col), errors="coerce"))
            bar_low = float(pd.to_numeric(bar.get(low_col), errors="coerce"))
            bar_close = float(pd.to_numeric(bar.get(close_col), errors="coerce"))
            if not np.isfinite(bar_high) or not np.isfinite(bar_low):
                continue

            if side == "SHORT":
                if leg1_open and leg2_open:
                    if bar_high >= stop_price:
                        leg1_open = False
                        leg2_open = False
                        leg1_exit_price, leg1_exit_time, leg1_outcome = float(stop_price), bar_time, "SL"
                        leg2_exit_price, leg2_exit_time, leg2_outcome = float(stop_price), bar_time, "SL"
                        break
                    if bar_low <= target_half:
                        leg1_open = False
                        leg1_exit_price, leg1_exit_time, leg1_outcome = float(target_half), bar_time, "TARGET_HALF"
                        leg2_stop = float(entry_price)

                        # Conservative intrabar ordering for remainder after BE activation.
                        if bar_high >= leg2_stop:
                            leg2_open = False
                            leg2_exit_price, leg2_exit_time, leg2_outcome = float(leg2_stop), bar_time, "BE"
                            break
                        if bar_low <= target_full:
                            leg2_open = False
                            leg2_exit_price, leg2_exit_time, leg2_outcome = float(target_full), bar_time, "TARGET"
                            break
                elif leg2_open:
                    if bar_high >= leg2_stop:
                        leg2_open = False
                        leg2_exit_price, leg2_exit_time = float(leg2_stop), bar_time
                        leg2_outcome = "BE" if abs(leg2_stop - entry_price) < 1e-9 else "SL"
                        break
                    if bar_low <= target_full:
                        leg2_open = False
                        leg2_exit_price, leg2_exit_time, leg2_outcome = float(target_full), bar_time, "TARGET"
                        break
            else:
                if leg1_open and leg2_open:
                    if bar_low <= stop_price:
                        leg1_open = False
                        leg2_open = False
                        leg1_exit_price, leg1_exit_time, leg1_outcome = float(stop_price), bar_time, "SL"
                        leg2_exit_price, leg2_exit_time, leg2_outcome = float(stop_price), bar_time, "SL"
                        break
                    if bar_high >= target_half:
                        leg1_open = False
                        leg1_exit_price, leg1_exit_time, leg1_outcome = float(target_half), bar_time, "TARGET_HALF"
                        leg2_stop = float(entry_price)

                        # Conservative intrabar ordering for remainder after BE activation.
                        if bar_low <= leg2_stop:
                            leg2_open = False
                            leg2_exit_price, leg2_exit_time, leg2_outcome = float(leg2_stop), bar_time, "BE"
                            break
                        if bar_high >= target_full:
                            leg2_open = False
                            leg2_exit_price, leg2_exit_time, leg2_outcome = float(target_full), bar_time, "TARGET"
                            break
                elif leg2_open:
                    if bar_low <= leg2_stop:
                        leg2_open = False
                        leg2_exit_price, leg2_exit_time = float(leg2_stop), bar_time
                        leg2_outcome = "BE" if abs(leg2_stop - entry_price) < 1e-9 else "SL"
                        break
                    if bar_high >= target_full:
                        leg2_open = False
                        leg2_exit_price, leg2_exit_time, leg2_outcome = float(target_full), bar_time, "TARGET"
                        break

        if leg1_open or leg2_open:
            last_bar = bars.iloc[-1]
            last_close = float(pd.to_numeric(last_bar.get(close_col), errors="coerce"))
            last_time = last_bar["datetime"]
            if not np.isfinite(last_close):
                last_close = float(entry_price)
            if leg1_open:
                leg1_exit_price, leg1_exit_time, leg1_outcome = last_close, last_time, "EOD"
            if leg2_open:
                leg2_exit_price, leg2_exit_time, leg2_outcome = last_close, last_time, "EOD"

        if not np.isfinite(leg1_exit_price) or not np.isfinite(leg2_exit_price):
            continue

        final_exit_price = float((leg1_exit_price * 0.5) + (leg2_exit_price * 0.5))
        final_exit_time = max(pd.Timestamp(leg1_exit_time), pd.Timestamp(leg2_exit_time))

        if leg1_outcome == "SL" and leg2_outcome == "SL":
            final_outcome = "SL"
        elif leg1_outcome == "TARGET_HALF" and leg2_outcome == "TARGET":
            final_outcome = "TARGET_PARTIAL_FULL"
        elif leg1_outcome == "TARGET_HALF" and leg2_outcome == "BE":
            final_outcome = "TARGET_PARTIAL_BE"
        elif leg1_outcome == "TARGET_HALF" and leg2_outcome == "SL":
            final_outcome = "TARGET_PARTIAL_SL"
        elif leg1_outcome == "TARGET_HALF" and leg2_outcome == "EOD":
            final_outcome = "TARGET_PARTIAL_EOD"
        elif leg1_outcome == "EOD" and leg2_outcome == "EOD":
            final_outcome = "EOD"
        else:
            final_outcome = f"PARTIAL_{leg1_outcome}_{leg2_outcome}"

        df.at[idx, "target_half_price"] = target_half
        df.at[idx, "breakeven_after_half_target"] = bool(leg1_outcome == "TARGET_HALF")
        df.at[idx, "leg1_exit_price"] = float(leg1_exit_price)
        df.at[idx, "leg1_exit_time_ist"] = leg1_exit_time
        df.at[idx, "leg1_outcome"] = leg1_outcome
        df.at[idx, "leg2_exit_price"] = float(leg2_exit_price)
        df.at[idx, "leg2_exit_time_ist"] = leg2_exit_time
        df.at[idx, "leg2_outcome"] = leg2_outcome

        df.at[idx, "exit_price"] = final_exit_price
        df.at[idx, "exit_time_ist"] = final_exit_time
        df.at[idx, "outcome"] = final_outcome
        _finalize_pnl(df, idx, side, entry_price, final_exit_price)
        updated += 1

    print(f"[VT4][5MIN] Re-resolved exits for {updated}/{len(df)} trades (partial half-target model).")
    return df


def calibrate_timer_params_5min(
    baseline_df: pd.DataFrame,
    dir_5m: Path,
    suffix_5m: str = ".parquet",
    engine: str = "pyarrow",
) -> Dict[str, Dict[str, float]]:
    """
    Derive timer/progress/adverse thresholds from baseline backtest outcomes.
    """
    df = _prepare_exit_df(baseline_df)
    if df.empty or not _required_columns_present(df):
        return {
            "timer_min": {"SHORT": 60.0, "LONG": 60.0},
            "progress_ratio_min": {"SHORT": 0.15, "LONG": 0.15},
            "adverse_ratio_min": {"SHORT": 0.30, "LONG": 0.30},
        }

    df["side"] = df["side"].astype(str).str.upper()
    df["outcome"] = df.get("outcome", "").astype(str).str.upper()

    timers: Dict[str, float] = {}
    progress_ratio_min: Dict[str, float] = {}
    adverse_ratio_min: Dict[str, float] = {}

    cache: Dict[str, pd.DataFrame] = {}

    for side in ("SHORT", "LONG"):
        side_df = df[df["side"] == side].copy()

        hit_target = side_df[
            (side_df["outcome"] == "TARGET")
            & side_df["entry_time_ist"].notna()
            & side_df["exit_time_ist"].notna()
        ].copy()
        if not hit_target.empty:
            mins = (
                (hit_target["exit_time_ist"] - hit_target["entry_time_ist"]).dt.total_seconds() / 60.0
            ).replace([np.inf, -np.inf], np.nan).dropna()
            mins = mins[mins > 0]
        else:
            mins = pd.Series(dtype=float)

        if len(mins) >= 5:
            raw_timer = float(np.nanquantile(mins.to_numpy(), 0.70))
        elif len(mins) > 0:
            raw_timer = float(np.nanmedian(mins.to_numpy()))
        else:
            raw_timer = 60.0
        timer_min = float(int(_clip_float(round(raw_timer / 5.0) * 5.0, 15.0, 180.0)))
        timers[side] = timer_min

        progress_samples: list[float] = []
        adverse_samples: list[float] = []

        for _, row in side_df.iterrows():
            ticker = str(row.get("ticker", ""))
            entry_time = row.get("entry_time_ist")
            entry_price = float(pd.to_numeric(row.get("entry_price"), errors="coerce"))
            stop_price = float(pd.to_numeric(row.get("stop_price"), errors="coerce"))
            target_price = float(pd.to_numeric(row.get("target_price"), errors="coerce"))
            outcome = str(row.get("outcome", "")).upper()

            if pd.isna(entry_time) or not np.isfinite(entry_price) or entry_price <= 0:
                continue
            if not np.isfinite(stop_price) or not np.isfinite(target_price):
                continue

            bars_all = _load_5m_bars(ticker, dir_5m, suffix_5m, engine, cache)
            if bars_all.empty:
                continue
            high_col = _pick_col(bars_all, "high", "High")
            low_col = _pick_col(bars_all, "low", "Low")
            if not high_col or not low_col:
                continue

            trade_day = pd.Timestamp(entry_time).normalize()
            cutoff = pd.Timestamp(entry_time) + pd.Timedelta(minutes=float(timer_min))
            bars = bars_all[
                (bars_all["datetime"] > entry_time)
                & (bars_all["datetime"] <= cutoff)
                & (bars_all["datetime"].dt.normalize() == trade_day)
            ]
            if bars.empty:
                continue

            high_max = float(pd.to_numeric(bars[high_col], errors="coerce").max())
            low_min = float(pd.to_numeric(bars[low_col], errors="coerce").min())
            if not np.isfinite(high_max) or not np.isfinite(low_min):
                continue

            target_dist = abs(target_price - entry_price)
            stop_dist = abs(stop_price - entry_price)
            if target_dist <= 0 or stop_dist <= 0:
                continue

            if side == "SHORT":
                mfe = max(0.0, entry_price - low_min)
                mae = max(0.0, high_max - entry_price)
            else:
                mfe = max(0.0, high_max - entry_price)
                mae = max(0.0, entry_price - low_min)

            if outcome == "TARGET":
                progress_samples.append(float(mfe / target_dist))
            if outcome == "SL":
                adverse_samples.append(float(mae / stop_dist))

        if len(progress_samples) >= 5:
            prog = float(np.nanquantile(np.asarray(progress_samples, dtype=float), 0.20))
        elif len(progress_samples) > 0:
            prog = float(np.nanmedian(np.asarray(progress_samples, dtype=float)))
        else:
            prog = 0.15

        if len(adverse_samples) >= 5:
            adv = float(np.nanquantile(np.asarray(adverse_samples, dtype=float), 0.40))
        elif len(adverse_samples) > 0:
            adv = float(np.nanmedian(np.asarray(adverse_samples, dtype=float)))
        else:
            adv = 0.30

        progress_ratio_min[side] = _clip_float(prog, 0.05, 0.40)
        adverse_ratio_min[side] = _clip_float(adv, 0.10, 0.85)

    return {
        "timer_min": timers,
        "progress_ratio_min": progress_ratio_min,
        "adverse_ratio_min": adverse_ratio_min,
    }


def resolve_exits_timer_stall_5min(
    trades_df: pd.DataFrame,
    dir_5m: Path,
    timer_min_by_side: Dict[str, float],
    progress_ratio_min_by_side: Dict[str, float],
    require_adverse: bool = False,
    adverse_ratio_min_by_side: Optional[Dict[str, float]] = None,
    suffix_5m: str = ".parquet",
    engine: str = "pyarrow",
) -> pd.DataFrame:
    """
    Scenario VT5 / VT6:
    - Timer-based early exit when trade stalls.
    - VT5: stall only.
    - VT6: stall + adverse movement confirmation.
    """
    if trades_df.empty:
        return trades_df
    if not dir_5m.is_dir():
        tag = "VT6" if require_adverse else "VT5"
        print(f"[{tag}][WARN] 5-min data directory not found: {dir_5m}.")
        return trades_df

    df = _prepare_exit_df(trades_df)
    if not _required_columns_present(df):
        tag = "VT6" if require_adverse else "VT5"
        print(f"[{tag}][WARN] Missing required columns for timer resolver.")
        return df

    cache: Dict[str, pd.DataFrame] = {}
    updated = 0
    tag = "VT6" if require_adverse else "VT5"

    for idx in df.index:
        ticker = str(df.at[idx, "ticker"])
        side = str(df.at[idx, "side"]).upper()
        entry_time = df.at[idx, "entry_time_ist"]
        entry_price = float(df.at[idx, "entry_price"])
        stop_price = float(df.at[idx, "stop_price"]) if pd.notna(df.at[idx, "stop_price"]) else np.nan
        target_price = float(df.at[idx, "target_price"]) if pd.notna(df.at[idx, "target_price"]) else np.nan

        if side not in ("SHORT", "LONG"):
            continue
        if pd.isna(entry_time) or not np.isfinite(entry_price) or entry_price <= 0:
            continue
        if not np.isfinite(stop_price) or not np.isfinite(target_price):
            continue

        timer_min = float(timer_min_by_side.get(side, 60.0))
        progress_ratio_min = float(progress_ratio_min_by_side.get(side, 0.15))
        adverse_ratio_min = float((adverse_ratio_min_by_side or {}).get(side, 0.30))
        timer_min = _clip_float(timer_min, 15.0, 180.0)
        progress_ratio_min = _clip_float(progress_ratio_min, 0.0, 2.0)
        adverse_ratio_min = _clip_float(adverse_ratio_min, 0.0, 3.0)

        bars_all = _load_5m_bars(ticker, dir_5m, suffix_5m, engine, cache)
        if bars_all.empty:
            continue

        high_col = _pick_col(bars_all, "high", "High")
        low_col = _pick_col(bars_all, "low", "Low")
        close_col = _pick_col(bars_all, "close", "Close")
        if not high_col or not low_col or not close_col:
            continue

        trade_day = pd.Timestamp(entry_time).normalize()
        bars = bars_all[
            (bars_all["datetime"] > entry_time)
            & (bars_all["datetime"].dt.normalize() == trade_day)
        ].sort_values("datetime")
        if bars.empty:
            continue

        cutoff_ts = pd.Timestamp(entry_time) + pd.Timedelta(minutes=timer_min)
        target_dist = abs(target_price - entry_price)
        stop_dist = abs(stop_price - entry_price)
        if target_dist <= 0:
            target_dist = max(abs(entry_price) * 0.01, 1e-9)
        if stop_dist <= 0:
            stop_dist = max(abs(entry_price) * 0.005, 1e-9)

        mfe = 0.0
        mae = 0.0
        new_exit_price = np.nan
        new_exit_time = pd.NaT
        new_outcome = ""

        for _, bar in bars.iterrows():
            bar_time = bar["datetime"]
            bar_high = float(pd.to_numeric(bar.get(high_col), errors="coerce"))
            bar_low = float(pd.to_numeric(bar.get(low_col), errors="coerce"))
            bar_close = float(pd.to_numeric(bar.get(close_col), errors="coerce"))
            if not np.isfinite(bar_high) or not np.isfinite(bar_low):
                continue

            if side == "SHORT":
                mfe = max(mfe, max(0.0, entry_price - bar_low))
                mae = max(mae, max(0.0, bar_high - entry_price))

                if bar_high >= stop_price:
                    new_exit_price = float(stop_price)
                    new_exit_time = bar_time
                    new_outcome = "SL"
                    break
                if bar_low <= target_price:
                    new_exit_price = float(target_price)
                    new_exit_time = bar_time
                    new_outcome = "TARGET"
                    break
            else:
                mfe = max(mfe, max(0.0, bar_high - entry_price))
                mae = max(mae, max(0.0, entry_price - bar_low))

                if bar_low <= stop_price:
                    new_exit_price = float(stop_price)
                    new_exit_time = bar_time
                    new_outcome = "SL"
                    break
                if bar_high >= target_price:
                    new_exit_price = float(target_price)
                    new_exit_time = bar_time
                    new_outcome = "TARGET"
                    break

            if bar_time >= cutoff_ts:
                mfe_ratio = float(mfe / target_dist)
                mae_ratio = float(mae / stop_dist)
                stalled = mfe_ratio < progress_ratio_min
                adverse_ok = mae_ratio >= adverse_ratio_min if require_adverse else True
                if stalled and adverse_ok:
                    new_exit_price = float(bar_close) if np.isfinite(bar_close) else float(entry_price)
                    new_exit_time = bar_time
                    new_outcome = "TIMEOUT_STALLED_ADVERSE" if require_adverse else "TIMEOUT_STALLED"
                    break

        if not np.isfinite(new_exit_price):
            last_bar = bars.iloc[-1]
            new_exit_price = float(pd.to_numeric(last_bar.get(close_col), errors="coerce"))
            if not np.isfinite(new_exit_price):
                new_exit_price = float(entry_price)
            new_exit_time = last_bar["datetime"]
            new_outcome = "EOD"

        df.at[idx, "timeout_timer_min"] = float(timer_min)
        df.at[idx, "timeout_progress_ratio_min"] = float(progress_ratio_min)
        df.at[idx, "timeout_adverse_ratio_min"] = float(adverse_ratio_min)
        df.at[idx, "exit_price"] = float(new_exit_price)
        df.at[idx, "exit_time_ist"] = new_exit_time
        df.at[idx, "outcome"] = new_outcome
        _finalize_pnl(df, idx, side, entry_price, float(new_exit_price))
        updated += 1

    mode_text = "timer+stall+adverse" if require_adverse else "timer+stall"
    print(f"[{tag}][5MIN] Re-resolved exits for {updated}/{len(df)} trades ({mode_text}).")
    return df


def resolve_exits_baseline_5min(
    trades_df: pd.DataFrame,
    dir_5m: Path,
    suffix_5m: str = ".parquet",
    engine: str = "pyarrow",
) -> pd.DataFrame:
    """
    Baseline 5-minute SL/target resolver with robust 5-minute file discovery.
    """
    if trades_df.empty:
        return trades_df
    if not dir_5m.is_dir():
        print(f"[VT7][WARN] 5-min data directory not found: {dir_5m}.")
        return trades_df

    df = _prepare_exit_df(trades_df)
    if not _required_columns_present(df):
        print("[VT7][WARN] Missing required columns for 5-min baseline resolver.")
        return df

    cache: Dict[str, pd.DataFrame] = {}
    updated = 0

    for idx in df.index:
        ticker = str(df.at[idx, "ticker"])
        side = str(df.at[idx, "side"]).upper()
        entry_time = df.at[idx, "entry_time_ist"]
        entry_price = float(df.at[idx, "entry_price"])
        stop_price = float(df.at[idx, "stop_price"]) if pd.notna(df.at[idx, "stop_price"]) else np.nan
        target_price = float(df.at[idx, "target_price"]) if pd.notna(df.at[idx, "target_price"]) else np.nan

        if pd.isna(entry_time) or not np.isfinite(entry_price) or entry_price <= 0:
            continue
        if not np.isfinite(stop_price) or not np.isfinite(target_price):
            continue
        if side not in ("SHORT", "LONG"):
            continue

        bars_all = _load_5m_bars(ticker, dir_5m, suffix_5m, engine, cache)
        if bars_all.empty:
            continue

        high_col = _pick_col(bars_all, "high", "High")
        low_col = _pick_col(bars_all, "low", "Low")
        close_col = _pick_col(bars_all, "close", "Close")
        if not high_col or not low_col or not close_col:
            continue

        trade_day = pd.Timestamp(entry_time).normalize()
        bars = bars_all[
            (bars_all["datetime"] > entry_time)
            & (bars_all["datetime"].dt.normalize() == trade_day)
        ].sort_values("datetime")
        if bars.empty:
            continue

        new_exit_price = np.nan
        new_exit_time = pd.NaT
        new_outcome = ""

        for _, bar in bars.iterrows():
            bar_time = bar["datetime"]
            bar_high = float(pd.to_numeric(bar.get(high_col), errors="coerce"))
            bar_low = float(pd.to_numeric(bar.get(low_col), errors="coerce"))
            if not np.isfinite(bar_high) or not np.isfinite(bar_low):
                continue

            if side == "SHORT":
                if bar_high >= stop_price:
                    new_exit_price = float(stop_price)
                    new_exit_time = bar_time
                    new_outcome = "SL"
                    break
                if bar_low <= target_price:
                    new_exit_price = float(target_price)
                    new_exit_time = bar_time
                    new_outcome = "TARGET"
                    break
            else:
                if bar_low <= stop_price:
                    new_exit_price = float(stop_price)
                    new_exit_time = bar_time
                    new_outcome = "SL"
                    break
                if bar_high >= target_price:
                    new_exit_price = float(target_price)
                    new_exit_time = bar_time
                    new_outcome = "TARGET"
                    break

        if not np.isfinite(new_exit_price):
            last_bar = bars.iloc[-1]
            last_close = float(pd.to_numeric(last_bar.get(close_col), errors="coerce"))
            if not np.isfinite(last_close):
                last_close = float(entry_price)
            new_exit_price = float(last_close)
            new_exit_time = last_bar["datetime"]
            new_outcome = "EOD"

        df.at[idx, "exit_price"] = float(new_exit_price)
        df.at[idx, "exit_time_ist"] = new_exit_time
        df.at[idx, "outcome"] = new_outcome
        _finalize_pnl(df, idx, side, entry_price, float(new_exit_price))
        updated += 1

    print(f"[VT7][5MIN] Re-resolved exits for {updated}/{len(df)} trades (baseline model).")
    return df
