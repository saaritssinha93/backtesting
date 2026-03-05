# -*- coding: utf-8 -*-
"""
fetch_india_vix.py — Build india_vix.parquet from local 15-min VIX CSV.

Reads: vix_indicators_15min_eq/india_vix_15minute_*.csv
Output: india_vix.parquet  (columns: date [str YYYY-MM-DD], india_vix [float])

Uses the first bar's OPEN of each trading day as the daily VIX value —
this is the VIX level visible at 09:15 before any signals fire (no lookahead).

Run once:  python fetch_india_vix.py
"""

import glob
import pathlib
import pandas as pd

_root = pathlib.Path(__file__).resolve().parent
_vix_dir = _root / "vix_indicators_15min_eq"
_out = _root / "india_vix.parquet"

# ── Find all VIX CSV files in the directory ────────────────────────────────
csv_files = sorted(_vix_dir.glob("india_vix_*.csv"))
if not csv_files:
    raise SystemExit(f"[ERROR] No india_vix_*.csv found in {_vix_dir}")

print(f"Found {len(csv_files)} VIX CSV file(s):")
for f in csv_files:
    print(f"  {f.name}")

# ── Load and concatenate ───────────────────────────────────────────────────
frames = []
for f in csv_files:
    df = pd.read_csv(f, parse_dates=["date"])
    frames.append(df)
raw = pd.concat(frames, ignore_index=True)
print(f"\nTotal rows loaded: {len(raw):,}")

# ── Parse date column (handles timezone suffix +05:30) ─────────────────────
raw["date"] = pd.to_datetime(raw["date"], utc=False, errors="coerce")
raw = raw.dropna(subset=["date"])
raw["date_only"] = raw["date"].dt.date

# ── Take the first bar per day → use its OPEN as daily VIX ────────────────
# Sort by datetime to ensure first bar is 09:15
raw = raw.sort_values("date")
daily = (
    raw.groupby("date_only", as_index=False)
    .first()                              # first 15-min bar of each day
    [["date_only", "open"]]
    .rename(columns={"date_only": "date", "open": "india_vix"})
)
daily["date"] = daily["date"].astype(str)   # YYYY-MM-DD string
daily["india_vix"] = daily["india_vix"].astype(float)
daily = daily.sort_values("date").reset_index(drop=True)

# ── Save ───────────────────────────────────────────────────────────────────
daily.to_parquet(_out, index=False)
print(f"\nSaved {len(daily)} daily VIX values -> {_out}")
print(daily.to_string(index=False))
