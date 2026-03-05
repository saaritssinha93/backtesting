# -*- coding: utf-8 -*-
"""
nfo_instruments_snapshot_builder_v6_nfo_options.py
==================================================

Exports a dated snapshot of NFO instruments so you can build historical
instrument-master archives over time.

Output files:
  - nfo_instruments_YYYYMMDD.csv
  - nfo_instruments_YYYYMMDD.parquet
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from avwap_v11_refactored.avwap_common import now_ist
from nfo_options_data_fetcher_v6_nfo_options import _build_kite_client, _validate_kite_session


THIS_DIR = Path(__file__).resolve().parent
DEFAULT_OUT_DIR = THIS_DIR / "nfo_instrument_snapshots_v6_nfo_options"


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Export dated NFO instrument snapshot.")
    p.add_argument("--output-dir", default=str(DEFAULT_OUT_DIR), help="Snapshot output directory.")
    p.add_argument("--api-key-file", default="api_key.txt", help="API key file.")
    p.add_argument("--access-token-file", default="access_token.txt", help="Access token file.")
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    out_dir = Path(args.output_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 78)
    print("NFO INSTRUMENT SNAPSHOT BUILDER v6 — dated archive for accurate ATM mapping")
    print("=" * 78)
    print()
    print("WHY THIS SCRIPT EXISTS:")
    print("  Zerodha's live NFO instrument master (kite.instruments('NFO')) only contains")
    print("  CURRENTLY ACTIVE contracts. Options that expired in past months are gone.")
    print("  If you run the data fetcher against historical trades without dated snapshots,")
    print("  the fetcher maps each trade to whatever expiry is available NOW, which may be")
    print("  months away from the original trade date — wrong strike, wrong premium, wrong P&L.")
    print()
    print("  By running THIS script every trading day and saving dated snapshots, you build")
    print("  a historical archive. The fetcher then uses each date's exact snapshot to find")
    print("  the ATM contract that was ACTUALLY trading on that day.")
    print()
    print("HOW TO SCHEDULE (run once per trading day at 9:00–9:15 AM IST):")
    print("  Windows Task Scheduler:")
    print("    Action: python nfo_instruments_snapshot_builder_v6_nfo_options.py \\")
    print(f"                   --output-dir {out_dir}")
    print("  Linux/Mac cron (9:00 AM IST = 03:30 UTC):")
    print("    30 3 * * 1-5  cd /path/to/nfoidv1 && python nfo_instruments_snapshot_builder_v6_nfo_options.py")
    print()
    print(f"[INFO] output directory      : {out_dir}")
    existing = sorted(out_dir.glob("nfo_instruments_2*.csv"))
    if existing:
        print(f"[INFO] existing snapshots    : {len(existing)} files")
        print(f"[INFO] date range in archive : {existing[0].stem.replace('nfo_instruments_','')} "
              f".. {existing[-1].stem.replace('nfo_instruments_','')}")
    else:
        print("[INFO] existing snapshots    : 0 (first run — start building your archive today)")
    print("-" * 78)

    kite = _build_kite_client(Path(args.api_key_file), Path(args.access_token_file))
    _validate_kite_session(kite, Path(args.api_key_file), Path(args.access_token_file))

    print("[INFO] fetching live NFO instrument master from Kite Connect...")
    rows = kite.instruments("NFO")
    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError("NFO instruments response is empty.")

    date_tag = now_ist().strftime("%Y%m%d")
    csv_path = out_dir / f"nfo_instruments_{date_tag}.csv"
    pq_path  = out_dir / f"nfo_instruments_{date_tag}.parquet"

    df.to_csv(csv_path, index=False)
    df.to_parquet(pq_path, index=False)

    # Detailed snapshot summary
    print()
    print(f"[INFO] total instruments in snapshot   : {len(df)}")
    if "instrument_type" in df.columns:
        type_vc = df["instrument_type"].astype(str).value_counts(dropna=False).head(8)
        print("[INFO] breakdown by instrument_type:")
        for t, n in type_vc.items():
            print(f"  - {t:<6}: {int(n):>6}")
    if "expiry" in df.columns:
        exp = pd.to_datetime(df["expiry"], errors="coerce").dropna()
        if not exp.empty:
            expiry_dates = sorted(exp.unique())
            print(f"[INFO] expiry dates in snapshot        : {len(expiry_dates)}")
            print(f"[INFO] nearest expiry                  : {expiry_dates[0].date() if expiry_dates else 'N/A'}")
            print(f"[INFO] farthest expiry                 : {expiry_dates[-1].date() if expiry_dates else 'N/A'}")
            near_exp = [e for e in expiry_dates if e.date() <= (now_ist().date() + pd.Timedelta(days=10))]
            if near_exp:
                print(f"[INFO] expiries within 10 days         : {[str(e.date()) for e in near_exp]}")
    if "name" in df.columns:
        opt_df = df[df.get("instrument_type", pd.Series()).astype(str).isin(["CE", "PE"])] if "instrument_type" in df.columns else df
        underlying_count = opt_df["name"].nunique() if "name" in opt_df.columns else "N/A"
        print(f"[INFO] unique underlyings with options  : {underlying_count}")
    print()
    print(f"[FILE SAVED] {csv_path}")
    print(f"[FILE SAVED] {pq_path}")
    print()
    print("[DONE] Snapshot saved successfully.")
    print()
    print("NEXT STEP — use this snapshot archive in the data fetcher:")
    print(f"  python nfo_options_data_fetcher_v6_nfo_options.py \\")
    print(f"         --historical-instruments-dir {out_dir} \\")
    print(f"         --snapshot-fallback-current-master \\")
    print(f"         --max-days-to-expiry 62")
    print("=" * 78)


if __name__ == "__main__":
    main()
