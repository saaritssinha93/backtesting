# -*- coding: utf-8 -*-
"""
One-off / maintenance cleaner for the 5m (or 1m) parquet store.

Collapses duplicate timestamps that accumulated from the old moving_files.py
full-row-hash append behaviour. For each <TICKER>_stocks_indicators_*.parquet:
  - drop_duplicates(subset=["date"], keep="last")   (freshest row per bar)
  - sort by "date"
  - rewrite atomically (tmp file + os.replace) only if anything changed.

Default target is the BACKTEST store (_eq_live2). It NEVER touches the live
store unless you pass --dir explicitly. Always snapshot before running on real
data (a robocopy backup is enough).

Usage:
    python dedupe_5min_store.py                 # dry-run the default backtest dir
    python dedupe_5min_store.py --apply          # apply to default backtest dir
    python dedupe_5min_store.py --dir <path> --apply
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pandas as pd

DEFAULT_DIR = r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2"
LIVE_DIR_MARKER = "_eq_live"  # refuse to touch the live store without --force
OPENING_SNAPSHOT_HHMM = (9, 15)  # 5m hybrid convention: 09:15 = opening snapshot


def _opening_snapshot_series(df: pd.DataFrame) -> "pd.Series":
    ts = pd.to_datetime(df["date"], errors="coerce")
    try:
        ts = ts.dt.tz_convert("Asia/Kolkata")
    except (TypeError, AttributeError):
        try:
            ts = ts.dt.tz_localize("Asia/Kolkata")
        except (TypeError, AttributeError):
            pass
    return (
        (ts.dt.hour == OPENING_SNAPSHOT_HHMM[0]) & (ts.dt.minute == OPENING_SNAPSHOT_HHMM[1])
    ).fillna(False).astype(bool)


def process_file(fpath: Path, apply: bool, stamp: bool) -> tuple[int, int, bool]:
    """Return (rows_before, rows_after, changed). Writes only when apply and changed."""
    df = pd.read_parquet(fpath)
    before = len(df)
    if df.empty or "date" not in df.columns:
        return before, before, False

    cleaned = (
        df.sort_values("date")
        .drop_duplicates(subset=["date"], keep="last")
        .reset_index(drop=True)
    )
    changed = len(cleaned) != before

    if stamp:
        new_flag = _opening_snapshot_series(cleaned)
        if "opening_snapshot" not in cleaned.columns or not cleaned["opening_snapshot"].astype(bool).equals(new_flag):
            cleaned = cleaned.copy()
            cleaned["opening_snapshot"] = new_flag
            changed = True

    if changed and apply:
        tmp = fpath.with_suffix(".parquet.tmp")
        cleaned.to_parquet(tmp, index=False, engine="pyarrow", compression="snappy")
        os.replace(tmp, fpath)
    return before, len(cleaned), changed


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default=DEFAULT_DIR, help="Parquet store directory to clean.")
    ap.add_argument("--apply", action="store_true", help="Actually rewrite files (default is dry-run).")
    ap.add_argument("--force-live", action="store_true", help="Allow running on a live (_eq_live) store.")
    ap.add_argument(
        "--stamp-opening-snapshot",
        action="store_true",
        help="Backfill the additive opening_snapshot bool column (09:15 row) on existing files.",
    )
    args = ap.parse_args()

    target = Path(args.dir)
    if not target.exists():
        print(f"[ERROR] dir not found: {target}")
        return 2
    if target.name.endswith("_eq_live") and not args.force_live:
        print(f"[REFUSED] {target} looks like the LIVE store. Use --force-live to override.")
        return 2

    files = sorted(target.glob("*.parquet"))
    mode = "APPLY" if args.apply else "DRY-RUN"
    print(f"{mode} dedupe{' + stamp opening_snapshot' if args.stamp_opening_snapshot else ''} on {len(files)} files in {target}")
    total_removed = files_changed = 0
    for fp in files:
        try:
            before, after, changed = process_file(fp, args.apply, args.stamp_opening_snapshot)
        except Exception as exc:
            print(f"  ERROR {fp.name}: {exc}")
            continue
        if changed:
            files_changed += 1
            total_removed += before - after
    print(f"\nDone. files_changed={files_changed} rows_removed={total_removed} "
          f"({'written' if args.apply else 'dry-run, nothing written'})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
