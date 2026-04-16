# -*- coding: utf-8 -*-
"""
quick_reresolve_v18c3_longfilter.py
------------------------------------
Tests post-hoc LONG quality filters on v18c3 entries with TGT=1%/SL=1%.

Filter A: RS >= 0.75% for BOTH-mode LONG (matches v17b/v16 threshold)
Filter B: RS >= 0.75% BOTH-mode + rs_norm >= 3.0 (ATR-normalised RS quality)

Runs both in sequence and prints comparison.
"""
from __future__ import annotations
import sys
from pathlib import Path
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import avwap_combined_runner_v18c3_5min as runner

SHORT_TGT = 0.01
LONG_TGT  = 0.01
SHORT_SL  = 0.01
LONG_SL   = 0.01

EXIT_COLS = [
    "exit_time_ist", "exit_price", "outcome",
    "pnl_pct", "pnl_pct_gross", "pnl_pct_price", "pnl_pct_gross_price",
    "pnl_rs", "pnl_rs_gross",
    "exit_stop_pct_cfg", "exit_target_pct_cfg",
    "exit_be_trigger_pct_cfg", "exit_trail_pct_cfg",
    "exit_resolution_case", "exit_bar_ambiguous",
    "stop_fill_penalty_applied", "stop_fill_penalty_bps",
    "exit_price_base", "exit_time_ist_base", "outcome_base",
    "pnl_pct_gross_price_base", "pnl_pct_price_base",
    "exit_price_pess", "exit_time_ist_pess", "outcome_pess",
    "pnl_pct_gross_price_pess", "pnl_pct_price_pess",
    "exit_price_opt", "exit_time_ist_opt", "outcome_opt",
    "pnl_pct_gross_price_opt", "pnl_pct_price_opt",
    "leverage", "notional_exposure_rs",
    "stop_price", "sl_price", "target_price",
]

CSV_PATH = sorted(
    Path(r"C:\TradingData\eqidv2\outputs_v18c3_5min").glob("*trades*.csv")
)[-1]

# Patch exit profile for TGT/SL override
_orig_ep = runner._v17c_setup_exit_profile
def _patched_ep(side_val, setup_val,
                _o=_orig_ep,
                _st=SHORT_TGT, _lt=LONG_TGT,
                _ssl=SHORT_SL, _lsl=LONG_SL):
    p = _o(side_val, setup_val)
    if str(side_val).upper() == "SHORT":
        p["target_pct"] = _st
        p["stop_pct"]   = _ssl
    else:
        p["target_pct"] = _lt
        p["stop_pct"]   = _lsl
    return p
runner._v17c_setup_exit_profile = _patched_ep


def _recompute_prices(df, tgt, sl_pct):
    ep = df["entry_price"].astype(float)
    is_short = df["side"].str.upper().eq("SHORT")
    df["stop_price"]   = np.where(is_short, ep*(1+sl_pct), ep*(1-sl_pct))
    df["target_price"] = np.where(is_short, ep*(1-tgt),    ep*(1+tgt))
    df["sl_price"]     = df["stop_price"]
    return df


def _run_once(label: str, long_df_filtered: pd.DataFrame, short_df: pd.DataFrame):
    print(f"\n{'='*70}")
    print(f"  {label}  |  LONG={len(long_df_filtered)}  SHORT={len(short_df)}")
    print(f"{'='*70}")

    dir_1m = runner._resolve_1min_dir()
    suffix_5m = ".parquet"
    for sf in list(dir_1m.glob("*"))[:5]:
        if sf.suffix:
            suffix_5m = sf.suffix
            break

    s_df = short_df.copy()
    l_df = long_df_filtered.copy()

    if not s_df.empty:
        s_df = runner._resolve_exits_5min(s_df, dir_1m, suffix_5m, "pyarrow",
                                          eod_exit_time=runner.V15_EOD_EXIT_TIME)
    if not l_df.empty:
        l_df = runner._resolve_exits_5min(l_df, dir_1m, suffix_5m, "pyarrow",
                                          eod_exit_time=runner.V15_EOD_EXIT_TIME)

    if not s_df.empty:
        s_df = runner._add_notional_pnl(s_df)
    if not l_df.empty:
        l_df = runner._add_notional_pnl(l_df)

    combined = pd.concat([s_df, l_df], ignore_index=True)
    combined = runner._add_notional_pnl(combined)
    combined = runner._sort_trades_for_output(combined)
    s_df = runner._sort_trades_for_output(
        combined[combined["side"].str.upper().eq("SHORT")].copy()
    )
    l_df = runner._sort_trades_for_output(
        combined[combined["side"].str.upper().eq("LONG")].copy()
    )

    runner._print_day_side_mix(combined)

    from avwap_v11_refactored.avwap_common_v11_v15 import compute_backtest_metrics, print_metrics
    print_metrics(f"SHORT [{label}]",    compute_backtest_metrics(s_df))
    print_metrics(f"LONG  [{label}]",    compute_backtest_metrics(l_df))
    print_metrics(f"COMBINED [{label}]", compute_backtest_metrics(combined))

    runner._print_exit_realism_band("SHORT",    s_df)
    runner._print_exit_realism_band("LONG",     l_df)
    runner._print_exit_realism_band("COMBINED", combined)
    runner._print_notional_pnl(combined)
    runner._print_recent_daily_breakdown(combined, n_weeks=2)


def main():
    print(f"[LOAD] {CSV_PATH.name}")
    df = pd.read_csv(CSV_PATH)
    drop_cols = [c for c in EXIT_COLS if c in df.columns]
    df = df.drop(columns=drop_cols)
    print(f"[PREP] Stripped {len(drop_cols)} exit columns")

    # Base DataFrames
    short_df = df[df["side"].str.upper().eq("SHORT")].copy().reset_index(drop=True)
    long_df  = df[df["side"].str.upper().eq("LONG")].copy().reset_index(drop=True)

    # Recompute prices
    short_df = _recompute_prices(short_df, SHORT_TGT, SHORT_SL)
    long_df  = _recompute_prices(long_df,  LONG_TGT,  LONG_SL)

    # Derived columns for filtering
    long_df["_rs"]  = pd.to_numeric(long_df["nifty_rel_strength_pct"], errors="coerce")
    long_df["_atr"] = pd.to_numeric(long_df["atr_pct_signal"], errors="coerce") * 100
    long_df["_rsn"] = long_df["_rs"] / long_df["_atr"].replace(0, np.nan)
    long_df["_mode"] = long_df["nifty_context_mode"].fillna("BOTH")

    # ---- Filter A: RS >= 0.75% for BOTH mode (same as v17b/v16 threshold) ----
    mask_a = (long_df["_mode"] != "BOTH") | (long_df["_rs"] >= 0.75)
    long_a = long_df[mask_a].copy().reset_index(drop=True)
    print(f"\n[FILTER A] BOTH-mode RS>=0.75%: {len(long_df)}->{len(long_a)} LONG "
          f"(removed {len(long_df)-len(long_a)} weak BOTH-mode trades)")
    _run_once("v18c3_LongFilterA_RS075_TGT1_SL1", long_a, short_df.copy())

    # ---- Filter B: RS >= 0.75% BOTH + rs_norm >= 3.0 ----
    mask_b = mask_a & (long_df["_rsn"] >= 3.0)
    long_b = long_df[mask_b].copy().reset_index(drop=True)
    print(f"\n[FILTER B] RS>=0.75% BOTH + rs_norm>=3.0: {len(long_df)}->{len(long_b)} LONG "
          f"(removed {len(long_df)-len(long_b)} trades)")
    _run_once("v18c3_LongFilterB_RS075_RsNorm3_TGT1_SL1", long_b, short_df.copy())

    print("\n[DONE]")


if __name__ == "__main__":
    main()
