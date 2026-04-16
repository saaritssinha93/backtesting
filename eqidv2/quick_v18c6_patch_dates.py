# -*- coding: utf-8 -*-
"""
quick_v18c6_patch_dates.py
==========================
Targeted scan for Apr 9, 10, 13 only — merges results into the existing
v18c6 output CSV without a full re-run.

How it works
------------
1. Builds the two-session shortlist for Apr 9/10/13 using stocks_indicators_5min_eq_live
   (existing shortlist only covers up to Apr 8).
2. Patches v18c5's get_two_session_shortlist / get_two_session_shortlist_entries to
   serve Apr 9/10/13 entries from the in-memory shortlist built in step 1.
3. Imports v18c6 (all patches applied: data-dir, RS filter, exit profile).
4. Patches read_15m_parquet to filter rows to TARGET_DATES only, so the
   scan sees exactly those 3 days per ticker.
5. Forces MAX_WORKERS=1 (serial) so the in-process patches apply.
6. Calls v18c6.main() which generates a new timestamped output file.
7. Reads that new file, keeps only TARGET_DATES rows, merges into the
   existing output CSV, sorts by trade_date + entry_time_ist, saves back.
"""
from __future__ import annotations

import os
import sys
from datetime import date
from pathlib import Path

# Ensure the eqidv2 directory is on sys.path
_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

import numpy as np
import pandas as pd

TARGET_DATES = {"2026-04-09", "2026-04-10", "2026-04-13"}
TARGET_DATE_OBJS = {date(2026, 4, 9), date(2026, 4, 10), date(2026, 4, 13)}

EXISTING_CSV = Path(
    r"C:\TradingData\eqidv2\outputs_v18c6_5min"
    r"\avwap_longshort_trades_v18c6_5min_ALL_DAYS_20260414_083551.csv"
)

# ── 1. Build two-session shortlist for Apr 9-13 using live data ─────────────
print("\n[SHORTLIST] Building Apr 9-13 shortlist from stocks_indicators_5min_eq_live ...")

import two_session_shortlist_history_builder_v2 as sl_builder

_LIVE_5M_DIR = Path(r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live")
# Override the builder's HIST_5M_DIR so _load_intraday reads from live
sl_builder.HIST_5M_DIR = _LIVE_5M_DIR
print(f"[PATCH] sl_builder.HIST_5M_DIR -> {_LIVE_5M_DIR}")

# Patch _build_atr_by_date to carry forward the last known ATR for Apr 10/13.
# The daily data dir only has data through Apr 9, so Apr 10/13 ATR is missing,
# which causes _session_summary to drop those dates entirely.
# Carrying forward Apr 9's ATR is a safe approximation for a 1-3 day gap.
_orig_build_atr = sl_builder._build_atr_by_date


def _patched_build_atr_by_date(df_daily):
    atr_map = _orig_build_atr(df_daily)
    if not atr_map:
        return atr_map
    sorted_dates = sorted(atr_map.keys())
    last_atr = atr_map[sorted_dates[-1]]
    for d in TARGET_DATE_OBJS:
        if d not in atr_map:
            atr_map[d] = last_atr
    return atr_map


sl_builder._build_atr_by_date = _patched_build_atr_by_date
print("[PATCH] _build_atr_by_date -> carries forward last ATR for Apr 10/13")

# Build with a 30-day lookback so RVOL rolling averages have enough history.
# We start from Mar 10 but will only use the Apr 9/10/13 entries for patching.
_RVOL_LOOKBACK_START = date(2026, 3, 10)
sl_payload = sl_builder.build_history(
    start_date=_RVOL_LOOKBACK_START,
    end_date=date(2026, 4, 13),
)
_ALL_SHORTLISTS = sl_payload.get("shortlists", {})
# Keep only the 3 target date entries for the shortlist patch
_NEW_SHORTLISTS = {k: v for k, v in _ALL_SHORTLISTS.items() if k in TARGET_DATES}
print(f"[SHORTLIST] Built entries for dates: {sorted(_NEW_SHORTLISTS.keys())}")


# ── 2. Import base module and patch shortlist lookups ───────────────────────
import avwap_combined_runner_v18c5_5min as _base

import filtered_stocks_two_session_history_v2 as _sl_existing

_orig_get_shortlist = _sl_existing.get_shortlist
_orig_get_entries = _sl_existing.get_shortlist_entries


def _patched_get_shortlist(scan_date, session, side="combined", as_set=False):
    """Fall back to in-memory new shortlist for Apr 9-13 dates."""
    day_data = _NEW_SHORTLISTS.get(str(scan_date))
    if day_data is not None:
        session_data = day_data.get(str(session).lower(), {})
        values = list(session_data.get(str(side).lower(), []))
        return set(values) if as_set else values
    return _orig_get_shortlist(scan_date, session, side, as_set)


def _patched_get_shortlist_entries(scan_date, session, side="combined"):
    """Return entry dicts {ticker, score, rank, rank_pct} from new shortlist for Apr 9-13."""
    day_data = _NEW_SHORTLISTS.get(str(scan_date))
    if day_data is not None:
        session_data = day_data.get(str(session).lower(), {})
        values = list(session_data.get(str(side).lower(), []))
        score_map = (
            session_data.get("scores", {}).get(str(side).lower(), {})
        )
        total = len(values)
        out = []
        for idx, ticker in enumerate(values, start=1):
            rank_pct = (idx / total) if total else 1.0
            out.append({
                "ticker": ticker,
                "score": float(score_map.get(ticker, 0.0)),
                "rank": idx,
                "rank_pct": float(rank_pct),
            })
        return out
    return _orig_get_entries(scan_date, session, side)


_base.get_two_session_shortlist = _patched_get_shortlist
_base.get_two_session_shortlist_entries = _patched_get_shortlist_entries
print("[PATCH] get_two_session_shortlist -> patched for Apr 9-13 dates")
print("[PATCH] get_two_session_shortlist_entries -> patched for Apr 9-13 dates")

# ── 3. Import v18c6 which applies all module-level patches ──────────────────
import avwap_combined_runner_v18c6_5min as v18c6

# ── 4. Patch read_15m_parquet to filter to target dates only ────────────────
_orig_read = _base.read_15m_parquet


def _date_filtered_read(path, engine=None):
    df = _orig_read(path, engine)
    if df.empty:
        return df
    date_str = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    filtered = df[date_str.isin(TARGET_DATES)].reset_index(drop=True)
    return filtered


_base.read_15m_parquet = _date_filtered_read
print(f"[PATCH] read_15m_parquet -> returns only {sorted(TARGET_DATES)}")

# ── 5. Force serial execution so the in-process patches apply ───────────────
_base.MAX_WORKERS = 1
print("[PATCH] MAX_WORKERS -> 1 (serial, patch applies)")

# ── 6. Record existing output files before the run ──────────────────────────
output_dir = Path(r"C:\TradingData\eqidv2\outputs_v18c6_5min")
before = {p.name for p in output_dir.iterdir() if p.is_file()}

# ── 7. Run the full v18c6 pipeline (scans only 3 dates per ticker) ──────────
print(f"\n[RUN] Starting targeted v18c6 scan for {sorted(TARGET_DATES)} ...\n")
try:
    v18c6.main()
except Exception as _run_exc:
    # Chart generation can crash on older matplotlib/env issues.
    # The trades CSV is written before charts, so we can still merge.
    print(f"[WARN] v18c6.main() raised an exception (likely chart generation): {_run_exc}")
    print("[WARN] Proceeding to check for new trades CSV anyway.")

# ── 8. Find new output CSV produced by this run ─────────────────────────────
after = {p.name for p in output_dir.iterdir() if p.is_file()}
new_files = [output_dir / n for n in (after - before) if "trades" in n and n.endswith(".csv")]

if not new_files:
    print("[ERROR] No new trades CSV found -- nothing to merge.")
    sys.exit(1)

new_csv = sorted(new_files)[-1]
print(f"\n[MERGE] New targeted output: {new_csv.name}")

new_df = pd.read_csv(new_csv)
new_df["trade_date"] = pd.to_datetime(new_df["trade_date"], errors="coerce")
date_str_col = new_df["trade_date"].dt.strftime("%Y-%m-%d")
new_rows = new_df[date_str_col.isin(TARGET_DATES)].copy()
print(f"[MERGE] New rows for target dates: {len(new_rows)}")

# ── 9. Load existing output and drop any stale rows for those dates ──────────
existing_df = pd.read_csv(EXISTING_CSV)
existing_df["trade_date"] = pd.to_datetime(existing_df["trade_date"], errors="coerce")
existing_date_str = existing_df["trade_date"].dt.strftime("%Y-%m-%d")
existing_clean = existing_df[~existing_date_str.isin(TARGET_DATES)].copy()
print(f"[MERGE] Existing rows (excl. target dates): {len(existing_clean)}")

# ── 10. Concatenate, sort, save ──────────────────────────────────────────────
merged = pd.concat([existing_clean, new_rows], ignore_index=True)
merged["trade_date"] = pd.to_datetime(merged["trade_date"], errors="coerce")
if "entry_time_ist" in merged.columns:
    merged["entry_time_ist"] = pd.to_datetime(merged["entry_time_ist"], errors="coerce")
    merged = merged.sort_values(["trade_date", "entry_time_ist"]).reset_index(drop=True)
else:
    merged = merged.sort_values("trade_date").reset_index(drop=True)

merged.to_csv(EXISTING_CSV, index=False)
print(f"\n[DONE] Saved {len(merged)} total rows -> {EXISTING_CSV.name}")

# ── 11. Print day-wise summary for the patched dates ────────────────────────
if not new_rows.empty:
    new_rows = new_rows.copy()
    new_rows["pnl_rs"] = pd.to_numeric(new_rows.get("pnl_rs", 0), errors="coerce").fillna(0)
    new_rows["side"] = new_rows["side"].str.upper()
    new_rows["outcome"] = new_rows["outcome"].str.upper()
    print("\n--- Day-wise summary for patched dates ---")
    for d, grp in new_rows.groupby(new_rows["trade_date"].dt.strftime("%Y-%m-%d")):
        n = len(grp)
        w = (grp["pnl_rs"] > 0).sum()
        l = (grp["pnl_rs"] < 0).sum()
        s = grp["pnl_rs"].sum()
        oc = grp["outcome"].value_counts().to_dict()
        longs = (grp["side"] == "LONG").sum()
        shorts = (grp["side"] == "SHORT").sum()
        oc_str = " ".join(f"{k[0]}:{v}" for k, v in oc.items())
        print(f"  {d}  L={longs} S={shorts} Tot={n} W={w} L={l}  "
              f"Win%={w/n*100:.0f}%  Rs={s:+,.0f}  {oc_str}")
else:
    print("\n[INFO] No new trades found for target dates.")
