r"""build_pool_amccb.py — research-only pool recreation for A_MOD_CLOSE_CONTINUATION_BREAK.

Combines, on the SAME raw (pre-gate) basis the unified pool used for this setup:
  1. master rows : outputs_ID_v11_unified_pool (basis=raw, sessions ..2026-06-24)
  2. gap rows    : outputs_ID_v11_conf_fresh_20260629 per-day raw_candidates.csv for
                   sessions PRESENT there but MISSING in the master (2026-06-17/18/19/23)
  3. tail rows   : a fresh v11 `historical_all_available` raw-candidate generation
                   for 2026-06-25..2026-07-02 (--tail_dir)

Basis cross-check (done before this script was written): on overlapping days the master
and conf_fresh raw scans agree within 1-4 rows (06-15: 290 vs 293; 06-12: 282 vs 285),
so mixing sources is safe.

Filters to --start..--end, dedupes on (ticker, side, setup, signal_time_ist), and
writes the pool file `setup_train_test.load_pool` expects.

Usage:
  py -3.12 build_pool_amccb.py [--tail_dir <dir-with-per-day-raw_candidates.csv>]
"""
from __future__ import annotations

import argparse
import glob
import json
from pathlib import Path

import pandas as pd

SETUP = "A_MOD_CLOSE_CONTINUATION_BREAK"
MASTER = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_unified_pool\historical_all_available_pre_dedupe_live_candidates.csv")
CONF_FRESH_DAYS = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_conf_fresh_20260629\historical_all_available_days")
HERE = Path(__file__).resolve()
WORK = HERE.parents[1]
FNAME = "historical_all_available_pre_dedupe_live_candidates.csv"
KEY = ["ticker", "side", "setup", "signal_time_ist"]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tail_dir", default=str(WORK / "pools" / "_tail_raw_gen" / "historical_all_available_days"))
    ap.add_argument("--start", default="2026-03-01")
    ap.add_argument("--end", default="2026-07-02")
    ap.add_argument("--out", default=str(WORK / "pools" / "pool_full"))
    args = ap.parse_args()

    frames: list[pd.DataFrame] = []

    # 1) master extract (chunked; keep only our setup)
    kept = 0
    master_days: set[str] = set()
    for ch in pd.read_csv(MASTER, chunksize=100_000, low_memory=False):
        m = ch["setup"].astype(str).str.strip() == SETUP
        if m.any():
            sub = ch.loc[m].copy()
            frames.append(sub)
            kept += int(m.sum())
            master_days.update(
                pd.to_datetime(sub["signal_time_ist"], errors="coerce").dt.date.astype(str).unique()
            )
    print(f"[pool] master rows kept: {kept:,} over {len(master_days)} sessions")

    # 2) conf_fresh gap fill: only days the master does NOT have at all
    gap_rows = 0
    gap_days: list[str] = []
    if CONF_FRESH_DAYS.is_dir():
        for daydir in sorted(CONF_FRESH_DAYS.iterdir()):
            day = daydir.name
            if day in master_days:
                continue
            f = daydir / "raw_candidates.csv"
            if not f.exists():
                continue
            try:
                d = pd.read_csv(f, low_memory=False)
            except Exception:
                continue
            m = d["setup"].astype(str).str.strip() == SETUP
            if m.any():
                sub = d.loc[m].copy()
                sub["_basis"] = "raw_conf_fresh_gapfill"
                frames.append(sub)
                gap_rows += int(m.sum())
                gap_days.append(day)
    print(f"[pool] conf_fresh gap-fill rows: {gap_rows:,} from days {gap_days}")

    # 3) tail raw candidates (fresh generation)
    tail_rows = 0
    tail_days: list[str] = []
    if args.tail_dir and Path(args.tail_dir).is_dir():
        tfiles = sorted(glob.glob(str(Path(args.tail_dir) / "*" / "raw_candidates.csv")))
        for f in tfiles:
            try:
                d = pd.read_csv(f, low_memory=False)
            except Exception:
                continue
            if d.empty or "setup" not in d.columns:
                continue
            m = d["setup"].astype(str).str.strip() == SETUP
            if m.any():
                sub = d.loc[m].copy()
                sub["_basis"] = "raw_tail_gen"
                frames.append(sub)
                tail_rows += int(m.sum())
                tail_days.append(Path(f).parent.name)
        print(f"[pool] tail rows kept: {tail_rows:,} from days {tail_days}")

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
        "conf_fresh_src": str(CONF_FRESH_DAYS),
        "tail_dir": str(args.tail_dir),
        "requested_range": [args.start, args.end],
        "rows_master": kept,
        "rows_conf_fresh_gapfill": gap_rows,
        "gap_days": gap_days,
        "rows_tail": tail_rows,
        "tail_days": tail_days,
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
