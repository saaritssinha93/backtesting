r"""precompute.py — build all derived matrices for one setup (research-only).

Usage: py -3.12 precompute.py --setup C_OR_BREAKDOWN
"""
from __future__ import annotations

import argparse
from pathlib import Path

import recovery_lib as rl


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--setup", required=True)
    ap.add_argument("--slippage_bps", type=float, default=15.0)
    args = ap.parse_args()
    setup = args.setup.strip().upper()
    workdir = Path(__file__).resolve().parents[1] / "setup_recovery_full_loop" / setup
    workdir = Path(str(workdir).replace("_shared" + "\\", ""))  # safety no-op
    workdir = rl.TT_DIR / "setup_recovery_full_loop" / setup

    eng = rl.ResearchEngine(setup, workdir, slippage_bps=args.slippage_bps)
    w = eng.w
    print("[precompute] done.")
    for k in ("FIT", "VAL", "TRAIN", "TEST"):
        ss = w["sessions"][k]
        print(f"  {k:5s} rows={len(w[k])} sessions={len(ss)} span={ss[0] if ss else '-'}..{ss[-1] if ss else '-'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
