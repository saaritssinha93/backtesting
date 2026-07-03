r"""recreate_pool.py — recreate the A_MOD_BREAK_C1_LOW research pool for the
FROM-SCRATCH RECOVERY loop (TRAIN 2026-03-01..2026-05-30 / TEST 2026-06-01..2026-07-02).

RESEARCH-ONLY. Writes ONLY under Train_and_Test/setup_from_scratch_recovery_loop/.

Same verified deterministic RAW-candidate basis as the previous campaign (cross-source
row-set identity was proven on shared dates):
  1. outputs_ID_v11_cleanpool/chunk_202603..202606      (2026-03-02 .. 2026-06-10)
  2. outputs_ID_v11_unified_recent_raw                  (2026-06-11 .. 2026-06-15)
  3. outputs_ID_v11_conf_fresh_20260629                 (2026-06-16 .. 2026-06-24)
  4. setup_pf_1_4_full_loop/<SETUP>/pools/_fresh_raw_20260625_20260701 (fresh scan,
     2026-06-25 .. 2026-07-01; produced this week by the production scanner with
     ab-gate enabled — reused read-only, command preserved in its _fresh_scan.log)

Output: pools/A_MOD_BREAK_C1_LOW/historical_all_available_pre_dedupe_live_candidates.csv
        pools/pool_manifest.json + pools/sessions_coverage.csv

Run:  py -3.12 Train_and_Test\setup_from_scratch_recovery_loop\A_MOD_BREAK_C1_LOW\scripts\recreate_pool.py
"""
from __future__ import annotations

import glob
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

SETUP = "A_MOD_BREAK_C1_LOW"
SIDE = "SHORT"
_HERE = Path(__file__).resolve().parent
WORK = _HERE.parent
POOLS = WORK / "pools"
TT_DIR = WORK.parent.parent
ROOT = Path(r"C:\TradingData\eqidv2")
FRESH = TT_DIR / "setup_pf_1_4_full_loop" / SETUP / "pools" / "_fresh_raw_20260625_20260701"

SOURCES = [
    ("cleanpool_chunks", sorted(glob.glob(str(ROOT / "outputs_ID_v11_cleanpool" / "chunk_2026*" / "historical_all_available_raw_candidates.csv")))),
    ("unified_recent_raw", [str(ROOT / "outputs_ID_v11_unified_recent_raw" / "historical_all_available_raw_candidates.csv")]),
    ("conf_fresh_20260629", [str(ROOT / "outputs_ID_v11_conf_fresh_20260629" / "historical_all_available_raw_candidates.csv")]),
    ("fresh_scan_20260625_20260701", [str(FRESH / "historical_all_available_raw_candidates.csv")]),
]
REQ_TRAIN = ("2026-03-01", "2026-05-30")
REQ_TEST = ("2026-06-01", "2026-07-02")
KEY = ["ticker", "side", "setup", "signal_time_ist"]


def main() -> int:
    frames, per_source = [], {}
    for label, files in SOURCES:
        rows = 0
        for f in files:
            p = Path(f)
            if not p.exists():
                print(f"[pool] WARN missing: {p}")
                continue
            df = pd.read_csv(p, low_memory=False)
            df = df[(df["setup"].astype(str).str.strip() == SETUP)
                    & (df["side"].astype(str).str.upper().str.strip() == SIDE)].copy()
            if not df.empty:
                df["_source"] = label
                frames.append(df)
                rows += len(df)
        per_source[label] = rows
        print(f"[pool] {label}: {rows} rows")
    cols = list(dict.fromkeys([c for f in frames for c in f.columns]))
    pool = pd.concat([f.reindex(columns=cols) for f in frames], ignore_index=True, sort=False)
    sig = pd.to_datetime(pool["signal_time_ist"], errors="coerce", utc=True)
    pool["_sig"] = sig.dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
    pool = pool.dropna(subset=["_sig"])
    pool["_d"] = pool["_sig"].dt.strftime("%Y-%m-%d")
    pool = pool[(pool["_d"] >= REQ_TRAIN[0]) & (pool["_d"] <= REQ_TEST[1])]
    pool = pool.sort_values(["_sig", "ticker"]).drop_duplicates(subset=KEY, keep="first").reset_index(drop=True)

    sess = sorted(pool["_d"].unique())
    tr = [s for s in sess if s <= REQ_TRAIN[1]]
    te = [s for s in sess if s >= REQ_TEST[0]]
    missing = [d.strftime("%Y-%m-%d") for d in pd.bdate_range(REQ_TRAIN[0], REQ_TEST[1])
               if d.strftime("%Y-%m-%d") not in set(sess)]

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
        "basis": "RAW candidates, production scanner (ab-gate enabled); 4 deterministic sources",
        "requested": {"TRAIN": list(REQ_TRAIN), "TEST": list(REQ_TEST)},
        "actual": {"TRAIN": [tr[0], tr[-1]], "TEST": [te[0], te[-1]],
                   "n_train_sessions": len(tr), "n_test_sessions": len(te)},
        "missing_weekdays": missing, "rows_total": int(len(pool)),
        "rows_per_source_prededup": per_source, "n_symbols": int(pool["ticker"].nunique()),
        "out_csv": str(out_csv),
    }
    (POOLS / "pool_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"[pool] TRAIN {tr[0]}..{tr[-1]} ({len(tr)}) | TEST {te[0]}..{te[-1]} ({len(te)}) | "
          f"rows={len(pool)} symbols={pool['ticker'].nunique()}")
    print(f"[pool] missing weekdays: {missing}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
