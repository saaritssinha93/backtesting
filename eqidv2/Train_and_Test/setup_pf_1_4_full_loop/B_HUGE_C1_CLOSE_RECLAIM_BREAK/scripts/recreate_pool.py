r"""recreate_pool.py — recreate the B_HUGE_C1_CLOSE_RECLAIM_BREAK research pool for the
PF-band FULL-LOOP campaign (TRAIN 2026-03-01..2026-05-30 / TEST 2026-06-01..2026-07-02).

RESEARCH-ONLY. Writes ONLY under Train_and_Test/setup_pf_1_4_full_loop/. Never touches
final_setup_conf.py, live tasks, or the original repo pools.

Basis:
RAW candidates from the production clean-pool scanner
(`avwap_5min_ID_v11_backtesting.py --mode historical_all_available`, ab-gate enabled so
A_*/B_* probation setups are included in the raw scan), harvested from the same four
segments the A_MOD campaigns verified for cross-source determinism:
  1. outputs_ID_v11_cleanpool/chunk_202603..202606   (2026-03-02 .. 2026-06-10)
  2. outputs_ID_v11_unified_recent_raw               (2026-06-11 .. 2026-06-15)
  3. outputs_ID_v11_conf_fresh_20260629              (2026-06-16 .. 2026-06-24)
  4. A_MOD_BREAK_C1_LOW/pools/_fresh_raw_20260625_20260701 (2026-06-25 .. 2026-07-01)

Output (same filename convention the tuner's load_pool() expects):
  pools/B_HUGE_C1_CLOSE_RECLAIM_BREAK/historical_all_available_pre_dedupe_live_candidates.csv
  pools/pool_manifest.json
  pools/sessions_coverage.csv

Run from repo root:
  py -3.12 Train_and_Test\setup_pf_1_4_full_loop\B_HUGE_C1_CLOSE_RECLAIM_BREAK\scripts\recreate_pool.py
"""
from __future__ import annotations

import glob
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

SETUP = "B_HUGE_C1_CLOSE_RECLAIM_BREAK"
SIDE = "LONG"
_HERE = Path(__file__).resolve().parent          # .../scripts
WORK = _HERE.parent                              # .../B_HUGE_C1_CLOSE_RECLAIM_BREAK
POOLS = WORK / "pools"
FULL_LOOP = WORK.parent                          # .../setup_pf_1_4_full_loop

ROOT = Path(r"C:\TradingData\eqidv2")
SOURCES = [
    ("cleanpool_chunks", sorted(glob.glob(str(ROOT / "outputs_ID_v11_cleanpool" / "chunk_2026*" / "historical_all_available_raw_candidates.csv")))),
    ("unified_recent_raw", [str(ROOT / "outputs_ID_v11_unified_recent_raw" / "historical_all_available_raw_candidates.csv")]),
    ("conf_fresh_20260629", [str(ROOT / "outputs_ID_v11_conf_fresh_20260629" / "historical_all_available_raw_candidates.csv")]),
    ("fresh_scan_20260625_20260701", [str(FULL_LOOP / "A_MOD_BREAK_C1_LOW" / "pools" / "_fresh_raw_20260625_20260701" / "historical_all_available_raw_candidates.csv")]),
]

REQ_TRAIN = ("2026-03-01", "2026-05-30")
REQ_TEST = ("2026-06-01", "2026-07-02")
# 2026-07-02: 5-min feed complete but the EOD 1-min sync had NOT run at build time
# (1-min rows stop at 09:30) -> SL/target exits cannot be simulated realistically;
# the session is EXCLUDED and reported as requested-but-unusable.
EXCLUDE_SESSIONS = {"2026-07-02"}
KEY = ["ticker", "side", "setup", "signal_time_ist"]


def main() -> int:
    frames = []
    per_source = {}
    for label, files in SOURCES:
        rows = 0
        for f in files:
            p = Path(f)
            if not p.exists():
                print(f"[pool] WARN missing source file: {p}")
                continue
            df = pd.read_csv(p, low_memory=False)
            df = df[(df["setup"].astype(str).str.strip() == SETUP)
                    & (df["side"].astype(str).str.upper().str.strip() == SIDE)].copy()
            if df.empty:
                continue
            df["_source"] = label
            frames.append(df)
            rows += len(df)
        per_source[label] = rows
        print(f"[pool] {label}: {rows} {SETUP} rows")
    if not frames:
        raise SystemExit("[pool] no source rows found")

    cols = list(dict.fromkeys([c for f in frames for c in f.columns]))
    pool = pd.concat([f.reindex(columns=cols) for f in frames], ignore_index=True, sort=False)

    sig = pd.to_datetime(pool["signal_time_ist"], errors="coerce", utc=True)
    pool["_sig"] = sig.dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
    pool = pool.dropna(subset=["_sig"])
    pool["_d"] = pool["_sig"].dt.strftime("%Y-%m-%d")

    # clip to the campaign window, drop incomplete sessions, dedupe across sources
    lo, hi = REQ_TRAIN[0], REQ_TEST[1]
    pool = pool[(pool["_d"] >= lo) & (pool["_d"] <= hi)].copy()
    n_excl = int(pool["_d"].isin(EXCLUDE_SESSIONS).sum())
    if n_excl:
        print(f"[pool] dropping {n_excl} rows from incomplete sessions {sorted(EXCLUDE_SESSIONS)} "
              f"(1-min EOD sync not run -> exits unresolvable)")
    pool = pool[~pool["_d"].isin(EXCLUDE_SESSIONS)].copy()
    before = len(pool)
    pool = pool.sort_values(["_sig", "ticker"]).drop_duplicates(subset=KEY, keep="first").reset_index(drop=True)
    print(f"[pool] {before} rows in window -> {len(pool)} after cross-source dedupe")

    sess = sorted(pool["_d"].unique())
    tr_sess = [s for s in sess if REQ_TRAIN[0] <= s <= REQ_TRAIN[1]]
    te_sess = [s for s in sess if REQ_TEST[0] <= s <= REQ_TEST[1]]

    # calendar-day gap report (weekdays with no session in pool)
    all_wd = [d.strftime("%Y-%m-%d") for d in pd.bdate_range(lo, hi)]
    missing = [d for d in all_wd if d not in set(sess)]

    out_dir = POOLS / SETUP
    out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / "historical_all_available_pre_dedupe_live_candidates.csv"
    pool.drop(columns=["_sig", "_d"]).to_csv(out_csv, index=False)

    cov = pool.groupby("_d").agg(rows=("ticker", "size"), tickers=("ticker", "nunique")).reset_index()
    cov = cov.rename(columns={"_d": "session"})
    cov["window"] = ["TRAIN" if s <= REQ_TRAIN[1] else "TEST" for s in cov["session"]]
    cov.to_csv(POOLS / "sessions_coverage.csv", index=False)

    manifest = {
        "built_utc": datetime.now(timezone.utc).isoformat(),
        "setup": SETUP, "side": SIDE,
        "basis": "RAW candidates (production clean-pool scanner, ab-gate enabled) — same 4-segment basis as the A_MOD full-loop campaigns",
        "requested": {"TRAIN": list(REQ_TRAIN), "TEST": list(REQ_TEST)},
        "actual": {"TRAIN": [tr_sess[0], tr_sess[-1]] if tr_sess else None,
                   "TEST": [te_sess[0], te_sess[-1]] if te_sess else None,
                   "n_train_sessions": len(tr_sess), "n_test_sessions": len(te_sess)},
        "excluded_sessions": sorted(EXCLUDE_SESSIONS),
        "excluded_reason": "1-min EOD sync incomplete at build time (rows stop 09:30) -> exits unresolvable",
        "missing_weekdays_in_window": missing,
        "rows_total": int(len(pool)),
        "rows_per_source_prededup": per_source,
        "n_symbols": int(pool["ticker"].nunique()),
        "out_csv": str(out_csv),
    }
    (POOLS / "pool_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    print(f"[pool] requested TRAIN {REQ_TRAIN[0]}..{REQ_TRAIN[1]} -> actual "
          f"{manifest['actual']['TRAIN']} ({len(tr_sess)} sessions)")
    print(f"[pool] requested TEST  {REQ_TEST[0]}..{REQ_TEST[1]} -> actual "
          f"{manifest['actual']['TEST']} ({len(te_sess)} sessions)")
    print(f"[pool] first/last available session in pool: {sess[0]} / {sess[-1]}")
    print(f"[pool] missing weekdays (holiday or no-data): {missing}")
    print(f"[pool] TRAIN sessions: {tr_sess}")
    print(f"[pool] TEST sessions:  {te_sess}")
    print(f"[pool] rows={len(pool)} symbols={pool['ticker'].nunique()} -> {out_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
