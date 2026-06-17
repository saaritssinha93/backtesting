"""Diff live conf-path entries against the v11 same-day backtest (Phase-4 / §F).

Join key = (ticker, side, setup, 5-min signal bar). Reports matched / live-only /
v11-only counts overall and per setup. Fill price/timing differences are expected
and ignored; this checks WHICH entries fired, not at what price.

Usage:
  py -3.12 diff_conf_entries_vs_v11.py --v11 <v11_ID_trades.csv> --live "<glob of live entry CSVs>"

Examples of --live for one day D (D8 = date without dashes, e.g. 20260615):
  entry-engine selected rows : "C:/TradingData/eqidv2/entry_engine_1min_v5_ID/audit/entry_rows_<D8>_*.csv"
  paper executed trades      : "C:/TradingData/eqidv2/live_signals/paper_trades_<D>_id_5min_v7.csv"
  (use entry_rows_<D8>_*.csv — NOT entry_rows_*<D8>*.csv, which also matches entry_rows_raw_candidates_*)
"""
from __future__ import annotations

import argparse
import glob
import os
import sys

import pandas as pd
from pandas.errors import EmptyDataError

TICKER_COLS = ("ticker", "symbol", "tradingsymbol")
SIDE_COLS = ("side",)
SETUP_COLS = ("setup", "setup_name")
TS_COLS = (
    "signal_time_ist",
    "bar_time_ist",
    "signal_bar_time_ist",
    "signal_datetime",
    "signal_ts",
    "scan_slot_ist",
    "entry_time",
    "signal_entry_datetime_ist",
)
RAW_AUDIT_PREFIXES = ("entry_rows_raw_candidates_",)


def _pick(df: pd.DataFrame, names) -> str | None:
    for n in names:
        if n in df.columns:
            return n
    return None


def _key(df: pd.DataFrame, label: str) -> pd.DataFrame:
    tcol, scol, ucol = _pick(df, TICKER_COLS), _pick(df, SIDE_COLS), _pick(df, SETUP_COLS)
    tscol = _pick(df, TS_COLS)
    miss = [n for n, c in [("ticker", tcol), ("side", scol), ("setup", ucol), ("signal_ts", tscol)] if c is None]
    if miss:
        raise SystemExit(f"[{label}] missing required column(s): {miss} (have: {list(df.columns)[:20]})")
    out = pd.DataFrame({
        "ticker": df[tcol].astype(str).str.upper().str.strip(),
        "side": df[scol].astype(str).str.upper().str.strip(),
        "setup": df[ucol].astype(str).str.upper().str.strip(),
        "bar": pd.to_datetime(df[tscol], errors="coerce").dt.floor("5min"),
    }).dropna(subset=["bar"])
    out = out[out["ticker"].ne("") & out["side"].isin(["LONG", "SHORT"])]
    return out.drop_duplicates()


def _read_csv_required(path: str, label: str) -> pd.DataFrame:
    try:
        return pd.read_csv(path, low_memory=False)
    except EmptyDataError:
        raise SystemExit(f"[{label}] empty CSV: {path}") from None


def _load_live_frames(pattern: str) -> tuple[pd.DataFrame, list[str], list[str]]:
    files = sorted(glob.glob(pattern))
    if not files:
        raise SystemExit(f"no live files matched: {pattern}")

    frames: list[pd.DataFrame] = []
    loaded: list[str] = []
    skipped: list[str] = []
    for path in files:
        name = os.path.basename(path)
        if name.startswith(RAW_AUDIT_PREFIXES):
            skipped.append(f"{name} (raw-candidate audit)")
            continue
        try:
            df = pd.read_csv(path, low_memory=False)
        except EmptyDataError:
            skipped.append(f"{name} (empty)")
            continue
        except Exception as exc:
            skipped.append(f"{name} ({type(exc).__name__})")
            continue
        if df.empty:
            skipped.append(f"{name} (empty)")
            continue
        frames.append(df)
        loaded.append(path)

    if not frames:
        sample = "; ".join(skipped[:8])
        raise SystemExit(f"no usable selected live files in glob: {pattern}" + (f" | skipped: {sample}" if sample else ""))
    return pd.concat(frames, ignore_index=True), loaded, skipped


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--v11", required=True, help="v11 trades CSV (reference)")
    ap.add_argument("--live", required=True, help="glob of live entry CSV(s)")
    args = ap.parse_args()

    v11 = _key(_read_csv_required(args.v11, "v11"), "v11")
    live_raw, live_files, skipped_files = _load_live_frames(args.live)
    live = live_raw
    live = _key(live, "live")

    keys = ["ticker", "side", "setup", "bar"]
    m = v11.merge(live, on=keys, how="outer", indicator=True)
    matched = int((m["_merge"] == "both").sum())
    v11_only = int((m["_merge"] == "left_only").sum())
    live_only = int((m["_merge"] == "right_only").sum())
    denom = matched + v11_only
    recall = (matched / denom * 100.0) if denom else 0.0

    print("=" * 78)
    print(f"v11 entries   : {len(v11)}    live entries: {len(live)}    files loaded: {len(live_files)}")
    if skipped_files:
        print(f"files skipped : {len(skipped_files)} ({'; '.join(skipped_files[:8])})")
    print(f"matched       : {matched}")
    print(f"v11-only (live missed) : {v11_only}")
    print(f"live-only (extra)      : {live_only}")
    print(f"entry recall (matched / v11) : {recall:.1f}%")
    print("-" * 78)
    print("per-setup (matched / v11-only / live-only):")
    for setup in sorted(set(m["setup"])):
        s = m[m["setup"] == setup]["_merge"].value_counts()
        print(f"  {setup:32} {int(s.get('both',0)):4} / {int(s.get('left_only',0)):4} / {int(s.get('right_only',0)):4}")
    print("=" * 78)
    print("Note: fill price/timing diffs are expected & ignored. v11-only beyond the known")
    print("Tier-C recall (V_RECLAIM ~75%) or executor caps (20-pos/dedup/F&O/freshness) warrant a look.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
