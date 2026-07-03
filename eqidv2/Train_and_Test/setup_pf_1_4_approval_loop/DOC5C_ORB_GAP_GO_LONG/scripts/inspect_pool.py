r"""inspect_pool.py — research-only. Inspect the DOC5C_ORB_GAP_GO_LONG pool.

Prints session distribution, TRAIN/TEST split counts under the task's mandated
split (TRAIN 2026-05-18.. / TEST 2026-06-20..), and the distribution of the
gap-and-go specific columns (gap_pct, orh_dist_atr, vwap_slope_atr, vol_ratio,
close_loc, rs_pct) so we know which knobs have any spread to sweep.

NO edits to final_setup_conf.py, NO live trades.
"""
from __future__ import annotations
import sys
from pathlib import Path
import numpy as np
import pandas as pd

POOL = Path(r"c:/Users/Saarit/OneDrive/Desktop/Trading/backtesting/eqidv2/backtesting/eqidv2/Train_and_Test/doc5_long_setups/pool/historical_all_available_pre_dedupe_live_candidates.csv")
SETUP = "DOC5C_ORB_GAP_GO_LONG"
TRAIN_START = pd.Timestamp("2026-05-18")
TEST_START = pd.Timestamp("2026-06-20")

df = pd.read_csv(POOL)
df = df[df["setup"] == SETUP].copy()
df["_ts"] = pd.to_datetime(df["signal_time_ist"], errors="coerce", utc=True).dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
df["_day"] = df["_ts"].dt.normalize()
print(f"TOTAL DOC5C rows (pre-dedupe): {len(df)}")
print(f"date span: {df['_day'].min()} .. {df['_day'].max()}")

sessions = sorted(df["_day"].dropna().unique())
print(f"\ndistinct sessions with >=1 DOC5C row: {len(sessions)}")

# task split
test_s = [s for s in sessions if s >= TEST_START]
train_s = [s for s in sessions if TRAIN_START <= s < (test_s[0] if test_s else TEST_START)]
print(f"\n== TASK SPLIT ==")
print(f"TRAIN {TRAIN_START.date()}..{(test_s[0] if test_s else TEST_START)} : {len(train_s)} sessions")
print(f"TEST  {TEST_START.date()}..latest : {len(test_s)} sessions")
print(f"TRAIN sessions: {[str(pd.Timestamp(s).date()) for s in train_s]}")
print(f"TEST  sessions: {[str(pd.Timestamp(s).date()) for s in test_s]}")

train_df = df[df["_day"].isin(set(train_s))]
test_df = df[df["_day"].isin(set(test_s))]
print(f"\npre-dedupe rows: TRAIN={len(train_df)} TEST={len(test_df)}")

# one-ticker-per-day dedupe estimate
def dd(x):
    return x.sort_values("_ts").drop_duplicates(subset=["_day", "ticker"], keep="first")
print(f"post one-ticker/day dedupe: TRAIN={len(dd(train_df))} TEST={len(dd(test_df))}")

print("\n== rows per session (all DOC5C) ==")
print(df.groupby("_day").size().to_string())

# knob distributions on TRAIN+TEST (task window only)
win = df[df["_day"] >= TRAIN_START]
print(f"\n== knob distributions on task window ({len(win)} rows) ==")
for c in ["gap_pct", "orh_dist_atr", "vwap_slope_atr", "vol_ratio", "close_loc",
          "rs_pct", "atr_pct", "body_pct", "vwap_dist_atr", "quality_score",
          "market_ret_pct", "signal_minute"]:
    if c in win.columns:
        s = pd.to_numeric(win[c], errors="coerce").dropna()
        if len(s):
            qs = s.quantile([0, .1, .25, .5, .75, .9, 1.0]).round(4).tolist()
            print(f"{c:16s} n={len(s):4d} min/10/25/50/75/90/max = {qs}")
print("\nregime counts (task window):")
print(win["regime"].value_counts().to_string())
print("\nsignal_minute histogram (task window):")
print(pd.to_numeric(win["signal_minute"], errors="coerce").describe().round(1).to_string())
