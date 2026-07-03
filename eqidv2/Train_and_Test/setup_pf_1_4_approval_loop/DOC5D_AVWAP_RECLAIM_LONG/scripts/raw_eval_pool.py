r"""raw_eval_pool.py — quick RAW-detection screener for a reinvented DOC5D pool.
============================================================================
Research-only. Given a pool dir + slippage + exit bracket, prints raw-detection
(no mask / no premom / one-per-day dedupe) metrics on the user's split
(TRAIN from 2026-05-18, TEST from 2026-06-20) at the requested bps. Used to
screen detector variants before spending a full Optuna PF-band run.

  py -3.12 .../scripts/raw_eval_pool.py --pool .../pool_reinvent_v2 --bps 5 --sl 1.0 --tgt 1.5
"""
from __future__ import annotations
import argparse, sys
from pathlib import Path
import pandas as pd

_HERE = Path(__file__).resolve()
_SETUP_DIR = _HERE.parent.parent
_LOOP_DIR = _SETUP_DIR.parent
_TT_DIR = _LOOP_DIR.parent
_REPO = _TT_DIR.parent
for _p in (str(_REPO), str(_TT_DIR), str(_LOOP_DIR / "_engine")):
    if _p not in sys.path:
        sys.path.insert(0, _p)
import setup_train_test as tt          # noqa: E402
import pf_band_fitval_loop as pfb      # noqa: E402

SETUP = "DOC5D_AVWAP_RECLAIM_LONG"


def _rng(ss):
    return f"{pd.Timestamp(ss[0]).date()}..{pd.Timestamp(ss[-1]).date()}" if ss else "(empty)"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pool", required=True)
    ap.add_argument("--bps", type=float, default=5.0)
    ap.add_argument("--sl", type=float, default=1.0)
    ap.add_argument("--tgt", type=float, default=1.5)
    args = ap.parse_args()

    tt.POOL_DIRS = [Path(args.pool)]; tt.POOL_DIR = Path(args.pool)
    pool = tt.load_pool(); pool = pool[pool["setup"] == SETUP].copy()
    sess = sorted(pd.Series(pool["_day"].dropna().unique()))
    TESTs = [s for s in sess if s >= pd.Timestamp("2026-06-20")]
    if len(TESTs) < 4:
        TESTs = sess[-5:]
    ft = TESTs[0]
    TRAINs = [s for s in sess if pd.Timestamp("2026-05-18") <= s < ft]
    half = len(TRAINs) // 2
    FITs, VALs = TRAINs[:half], TRAINs[half:]
    print(f"[raw-eval] pool={args.pool} bps={args.bps} exit={args.sl}/{args.tgt}")
    print(f"[raw-eval] sessions={len(sess)} ({_rng(sess)})")
    print(f"[raw-eval] FIT {_rng(FITs)}({len(FITs)}) VAL {_rng(VALs)}({len(VALs)}) "
          f"TRAIN {_rng(TRAINs)}({len(TRAINs)}) TEST {_rng(TESTs)}({len(TESTs)})")

    span = set(TRAINs + TESTs)
    pfb._set_slippage(args.bps); tt.MAX_POSITIONS = 20; tt.DAILY_LOSS_RS = 0.0
    sub = tt.attach_entries(pool[pool["_day"].isin(span)].copy())
    cfg = {"sl": args.sl, "tgt": args.tgt, "mask_terms": [], "premom_terms": [],
           "guard": None, "status": "OK", "max_positions": 20, "daily_loss_rs": 0.0}
    for lbl, ss in (("FIT", FITs), ("VAL", VALs), ("TRAIN", TRAINs), ("TEST", TESTs)):
        m = pfb.full_metrics(SETUP, cfg, sub[sub["_day"].isin(set(ss))].copy())
        print(f"[raw-eval] {lbl:5s} n={m['n']:3d} PF={m['net_pf']:.3f} net=Rs{m['net_pnl']:>8,.0f} "
              f"win%={m['win_rate']:.1f} SL/TGT/EOD={m['sl_cnt']}/{m['tgt_cnt']}/{m['eod_cnt']} "
              f"tgt%={m['target_rate']:.1f} tpd={m['trades_per_day']} dayDom={m['day_dom']} "
              f"symDom={m['sym_dom']} dbp={m['day_block_p']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
