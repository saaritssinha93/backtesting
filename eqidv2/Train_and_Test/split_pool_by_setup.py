r"""split_pool_by_setup.py — one-time split of the big unified pool CSV into per-setup
pool dirs so the per-setup loop runner loads instantly (instead of re-reading 200MB).

Reads <pool>/historical_all_available_pre_dedupe_live_candidates.csv in chunks and writes
  <out>/<SETUP>/historical_all_available_pre_dedupe_live_candidates.csv
for every setup requested (or all). The per-setup file keeps the SAME filename, so
setup_train_test._read_one_pool / load_pool picks it up when POOL_DIR points at it.

Run:  py -3.12 Train_and_Test\split_pool_by_setup.py [--pool <dir>] [--out <dir>] [--setups A,B,..]
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

DEFAULT_POOL = r"C:\TradingData\eqidv2\outputs_ID_v11_unified_pool"
DEFAULT_OUT = (r"C:\Users\Saarit\AppData\Local\Temp\claude"
               r"\c--Users-Saarit-OneDrive-Desktop-Trading-backtesting-eqidv2-backtesting-eqidv2"
               r"\41d4e196-2e06-4276-945a-008c377c414d\scratchpad\setup_pools")
FNAME = "historical_all_available_pre_dedupe_live_candidates.csv"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pool", default=DEFAULT_POOL)
    ap.add_argument("--out", default=DEFAULT_OUT)
    ap.add_argument("--setups", default="", help="comma list (default: all setups in the pool)")
    args = ap.parse_args()

    src = Path(args.pool) / FNAME
    if not src.exists():
        raise SystemExit(f"no pool csv at {src}")
    out_root = Path(args.out)
    out_root.mkdir(parents=True, exist_ok=True)
    want = {s.strip() for s in args.setups.split(",") if s.strip()} or None

    # Stream in chunks; group by setup; append to per-setup files. dtype=str so no
    # column-type inference cost (the runner re-parses numerics like load_pool does).
    seen: set[str] = set()
    n_total = 0
    counts: dict[str, int] = {}
    reader = pd.read_csv(src, dtype=str, chunksize=50_000, low_memory=False)
    for chunk in reader:
        chunk["setup"] = chunk["setup"].astype(str).str.strip()
        for setup, g in chunk.groupby("setup"):
            if want is not None and setup not in want:
                continue
            safe = "".join(c if c.isalnum() or c in "._-" else "_" for c in setup)
            d = out_root / safe
            d.mkdir(parents=True, exist_ok=True)
            path = d / FNAME
            first = setup not in seen
            g.to_csv(path, mode=("w" if first else "a"), header=first, index=False)
            seen.add(setup)
            counts[setup] = counts.get(setup, 0) + len(g)
        n_total += len(chunk)
    print(f"[split] read {n_total} rows from {src}")
    for s in sorted(counts, key=lambda k: -counts[k]):
        print(f"  {s:40} {counts[s]:>7} -> {out_root / s}")
    print(f"[split] wrote {len(counts)} per-setup pools under {out_root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
