r"""build_pool_generic.py — research-only pool recreation for the setup_recovery_full_loop campaign.

Same raw (pre-gate) basis as the unified pool + prior full-loop campaigns:
  1. master rows   : outputs_ID_v11_unified_pool (2025-11-03..2026-06-24, basis=raw)
  2. gapfill rows  : reused fresh v11 raw generations from the A_MOD_BREAK_C1_HIGH
                     campaign (2026-06-17..06-23 regen; 05-28/06-26 unrecoverable)
  3. tail rows     : reused A_MOD tail gen (2026-06-25..07-01)
  4. d0702 rows    : fresh v11 raw generation for 2026-07-02 (this campaign)

Filters to --start..--end, dedupes on (ticker, side, setup, signal_time_ist), and
writes the pool file `setup_train_test.load_pool` expects.

Usage:
  py -3.12 build_pool_generic.py --setup C_OR_BREAKDOWN --out <dir>
"""
from __future__ import annotations

import argparse
import glob
import json
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve()
TT_DIR = HERE.parents[2]                     # Train_and_Test
MASTER = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_unified_pool\historical_all_available_pre_dedupe_live_candidates.csv")
AMOD_POOLS = TT_DIR / "setup_pf_1_4_full_loop" / "A_MOD_BREAK_C1_HIGH" / "pools"
D0702 = HERE.parent / "d0702_raw_gen"
FNAME = "historical_all_available_pre_dedupe_live_candidates.csv"
KEY = ["ticker", "side", "setup", "signal_time_ist"]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--setup", required=True)
    ap.add_argument("--start", default="2026-03-01")
    ap.add_argument("--end", default="2026-07-02")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    setup = args.setup.strip().upper()

    frames: list[pd.DataFrame] = []

    kept = 0
    for ch in pd.read_csv(MASTER, chunksize=200_000, low_memory=False):
        m = ch["setup"].astype(str).str.strip() == setup
        if m.any():
            frames.append(ch.loc[m].copy())
            kept += int(m.sum())
    print(f"[pool] master rows kept: {kept:,}")

    fresh_rows = 0
    fresh_files = sorted(
        glob.glob(str(AMOD_POOLS / "_tail_raw_gen" / "**" / "*raw_candidates.csv"), recursive=True)
        + glob.glob(str(AMOD_POOLS / "_gapfill_raw_gen" / "**" / "*raw_candidates.csv"), recursive=True)
        + glob.glob(str(D0702 / "**" / "*raw_candidates.csv"), recursive=True)
    )
    # keep only the plain raw_candidates files (skip ranked_)
    fresh_files = [f for f in fresh_files if Path(f).name.endswith("raw_candidates.csv")
                   and not Path(f).name.startswith("historical_all_available_ranked")
                   and "ranked_raw_candidates" not in Path(f).name]
    # prefer per-day files to avoid double-count with rollups: drop rollup files when
    # per-day dirs exist for the same generation
    per_day = [f for f in fresh_files if "historical_all_available_days" in f]
    rollups = [f for f in fresh_files if "historical_all_available_days" not in f]
    use = per_day if per_day else rollups
    for f in use:
        try:
            d = pd.read_csv(f, low_memory=False)
        except Exception:
            continue
        if "setup" not in d.columns:
            continue
        m = d["setup"].astype(str).str.strip() == setup
        if m.any():
            frames.append(d.loc[m].copy())
            fresh_rows += int(m.sum())
    print(f"[pool] fresh-gen rows kept: {fresh_rows:,} from {len(use)} per-day file(s)")

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
        "setup": setup,
        "master_src": str(MASTER),
        "reused_fresh_gens": [str(AMOD_POOLS / "_tail_raw_gen"), str(AMOD_POOLS / "_gapfill_raw_gen"), str(D0702)],
        "requested_range": [args.start, args.end],
        "rows_master": kept,
        "rows_fresh": fresh_rows,
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
