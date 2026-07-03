r"""build_pool_a_mod_c1_high.py — research-only pool recreation for A_MOD_BREAK_C1_HIGH.

Combines, on the SAME raw (pre-gate) basis the unified pool used for this setup:
  1. master rows : outputs_ID_v11_unified_pool (2025-11-03..2026-06-24, basis=raw)
  2. tail rows   : a fresh v11 `historical_all_available` raw-candidate generation
                   for the missing sessions 2026-06-25..2026-07-01 (--tail_dir)

Filters to --start..--end, dedupes on (ticker, side, setup, signal_time_ist), and
writes the pool file `setup_train_test.load_pool` expects.

Usage:
  py -3.12 build_pool_a_mod_c1_high.py [--tail_dir <dir-with-raw-candidates-csv>]
"""
from __future__ import annotations

import argparse
import glob
import json
from pathlib import Path

import pandas as pd

SETUP = "A_MOD_BREAK_C1_HIGH"
MASTER = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_unified_pool\historical_all_available_pre_dedupe_live_candidates.csv")
HERE = Path(__file__).resolve()
WORK = HERE.parents[1]
FNAME = "historical_all_available_pre_dedupe_live_candidates.csv"
KEY = ["ticker", "side", "setup", "signal_time_ist"]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tail_dir", default="", help="dir containing fresh raw-candidates CSV(s) for the tail sessions")
    ap.add_argument("--start", default="2026-03-01")
    ap.add_argument("--end", default="2026-07-02")
    ap.add_argument("--out", default=str(WORK / "pools" / "pool_full"))
    args = ap.parse_args()

    frames: list[pd.DataFrame] = []

    # 1) master extract (chunked; keep only our setup)
    kept = 0
    for ch in pd.read_csv(MASTER, chunksize=100_000, low_memory=False):
        m = ch["setup"].astype(str).str.strip() == SETUP
        if m.any():
            frames.append(ch.loc[m].copy())
            kept += int(m.sum())
    print(f"[pool] master rows kept: {kept:,}")

    # 2) tail raw candidates (fresh generation), if provided
    tail_rows = 0
    if args.tail_dir:
        tfiles = sorted(
            glob.glob(str(Path(args.tail_dir) / "**" / "*raw_candidates.csv"), recursive=True)
        )
        for f in tfiles:
            try:
                d = pd.read_csv(f, low_memory=False)
            except Exception:
                continue
            m = d["setup"].astype(str).str.strip() == SETUP
            if m.any():
                frames.append(d.loc[m].copy())
                tail_rows += int(m.sum())
        print(f"[pool] tail rows kept: {tail_rows:,} from {len(tfiles)} file(s)")

    pool = pd.concat(frames, ignore_index=True, sort=False)
    ts = pd.to_datetime(pool["signal_time_ist"], errors="coerce")
    pool = pool.loc[(ts >= pd.Timestamp(args.start, tz="Asia/Kolkata"))
                    & (ts < pd.Timestamp(args.end, tz="Asia/Kolkata") + pd.Timedelta(days=1))].copy()
    before = len(pool)
    pool = pool.drop_duplicates(subset=KEY, keep="first").reset_index(drop=True)
    pool = pool.sort_values("signal_time_ist").reset_index(drop=True)

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / FNAME
    pool.to_csv(out_csv, index=False)

    d = pd.to_datetime(pool["signal_time_ist"], errors="coerce").dt.date
    sessions = sorted(set(d.astype(str)))
    manifest = {
        "setup": SETUP,
        "master_src": str(MASTER),
        "tail_dir": args.tail_dir,
        "requested_range": [args.start, args.end],
        "rows_master": kept,
        "rows_tail": tail_rows,
        "rows_pre_dedupe_in_range": before,
        "rows_final": len(pool),
        "first_session": sessions[0] if sessions else None,
        "last_session": sessions[-1] if sessions else None,
        "n_sessions": len(sessions),
        "sessions": sessions,
    }
    (out_dir / "_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"[pool] wrote {out_csv} rows={len(pool)} sessions={len(sessions)} "
          f"span={sessions[0] if sessions else '-'}..{sessions[-1] if sessions else '-'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
