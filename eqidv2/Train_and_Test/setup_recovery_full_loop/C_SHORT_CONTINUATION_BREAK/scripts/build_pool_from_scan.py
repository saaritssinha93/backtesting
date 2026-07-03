r"""build_pool_from_scan.py — build the C_SHORT_CONTINUATION_BREAK research pool from the
as-promoted research scan output (PRE-collapse basis — see POOL_RECREATION_REPORT.md).

The production same-candle collapse awards 100% of this setup's candles to other labels
(zero collapsed rows across 03-01..07-02), so the only researchable basis is the
pre-collapse per-label file. Any positive finding therefore REQUIRES a collapse-priority
change before it could ever fire live — documented in the final recommendation.

Usage: py -3.12 build_pool_from_scan.py
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

SETUP = "C_SHORT_CONTINUATION_BREAK"
HERE = Path(__file__).resolve()
WORK = HERE.parents[1]
SCAN = WORK / "pools" / "_research_scan_20260301_20260702"
OUT = WORK / "pools" / "pool_full"
FNAME = "historical_all_available_pre_dedupe_live_candidates.csv"
KEY = ["ticker", "side", "setup", "signal_time_ist"]


def main() -> int:
    pre = pd.read_csv(SCAN / "precollapse_target_candidates.csv", low_memory=False)
    pre = pre[pre["setup"].astype(str).str.strip() == SETUP].copy()
    ts = pd.to_datetime(pre["signal_time_ist"], errors="coerce")
    pre = pre.loc[(ts >= pd.Timestamp("2026-03-01", tz="Asia/Kolkata"))
                  & (ts < pd.Timestamp("2026-07-03", tz="Asia/Kolkata"))]
    before = len(pre)
    pre = pre.drop_duplicates(subset=KEY, keep="first").sort_values("signal_time_ist").reset_index(drop=True)

    OUT.mkdir(parents=True, exist_ok=True)
    pre.to_csv(OUT / FNAME, index=False)
    d = pd.to_datetime(pre["signal_time_ist"], errors="coerce").dt.date
    sessions = sorted(set(d.astype(str)))
    manifest = {
        "setup": SETUP,
        "basis": "PRE-COLLAPSE per-label rows (research_scan_cshort.py); production collapse "
                 "gives this setup ZERO rows — live trading would require a priority change",
        "src": str(SCAN),
        "rows_pre_dedupe": before,
        "rows_final": len(pre),
        "n_sessions": len(sessions),
        "first_session": sessions[0] if sessions else None,
        "last_session": sessions[-1] if sessions else None,
        "sessions": sessions,
    }
    (OUT / "_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"[pool] wrote {OUT / FNAME} rows={len(pre)} sessions={len(sessions)} "
          f"span={sessions[0] if sessions else '-'}..{sessions[-1] if sessions else '-'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
