r"""build_pool.py — one-time: filter the 194MB unified pool down to P_PDH_BREAK_RETEST_LONG
rows and write a tiny per-setup pool under this approval-loop folder, so every subsequent
optimizer run loads in <1s instead of re-reading the whole unified CSV.

Research-only. No conf edits. No live trades."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

SETUP = "P_PDH_BREAK_RETEST_LONG"
SRC = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_unified_pool\historical_all_available_pre_dedupe_live_candidates.csv")
_HERE = Path(__file__).resolve().parent
OUT_DIR = _HERE.parent / "pool"
OUT = OUT_DIR / "historical_all_available_pre_dedupe_live_candidates.csv"


def main() -> int:
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass
    print(f"[build_pool] reading {SRC} ...")
    df = pd.read_csv(SRC, low_memory=False)
    print(f"[build_pool] total rows {len(df)}")
    sub = df[df["setup"].astype(str).str.strip() == SETUP].copy()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    sub.to_csv(OUT, index=False)
    print(f"[build_pool] wrote {len(sub)} {SETUP} rows -> {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
