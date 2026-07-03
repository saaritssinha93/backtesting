r"""screen_variants.py — raw-quality screen of the reinvented reclaim variants @5bps.
Research-only. For each DOC5D_RECLAIM_v* setup in the pool, prints raw-detection
(no mask/premom, one-per-day dedupe, top_n picks best-quality per day) TRAIN/TEST
metrics over a small exit grid, so we can pick which variant(s) to send to the
full PF-band loop. Costs @5bps (the target regime).
"""
from __future__ import annotations
import sys
from pathlib import Path
import pandas as pd

_HERE = Path(__file__).resolve()
_SETUP_DIR = _HERE.parent.parent; _LOOP_DIR = _SETUP_DIR.parent; _TT_DIR = _LOOP_DIR.parent
_REPO = _TT_DIR.parent
for _p in (str(_REPO), str(_TT_DIR), str(_LOOP_DIR / "_engine")):
    if _p not in sys.path:
        sys.path.insert(0, _p)
import setup_train_test as tt          # noqa: E402
import pf_band_fitval_loop as pfb      # noqa: E402

POOL = _SETUP_DIR / "pool_reinvent"
VARIANTS = ["vA", "vB", "vC", "vD"]
EXITS = [(0.9, 1.5), (1.0, 1.5), (1.1, 1.8), (1.2, 2.0)]
TOPN = [0, 2]   # 0 = no cap, 2 = best-2-per-day


def _split(sess):
    TESTs = [s for s in sess if s >= pd.Timestamp("2026-06-20")]
    if len(TESTs) < 4:
        TESTs = sess[-5:]
    ft = TESTs[0]
    TRAINs = [s for s in sess if pd.Timestamp("2026-05-18") <= s < ft]
    return TRAINs, TESTs


def main() -> int:
    tt.POOL_DIRS = [POOL]; tt.POOL_DIR = POOL
    pool_all = tt.load_pool()
    pfb._set_slippage(5.0)
    for V in VARIANTS:
        setup = f"DOC5D_RECLAIM_{V}"
        pool = pool_all[pool_all["setup"] == setup].copy()
        if pool.empty:
            print(f"{setup}: EMPTY"); continue
        sess = sorted(pd.Series(pool["_day"].dropna().unique()))
        TRAINs, TESTs = _split(sess)
        span = set(TRAINs + TESTs)
        sub = tt.attach_entries(pool[pool["_day"].isin(span)].copy())
        print(f"\n==== {setup}  TRAIN {len(TRAINs)}sess  TEST {len(TESTs)}sess ====")
        for tn in TOPN:
            for sl, tgt in EXITS:
                tt.MAX_POSITIONS = 20; tt.DAILY_LOSS_RS = 0.0
                guard = {"top_n": tn} if tn else None
                cfg = {"sl": sl, "tgt": tgt, "mask_terms": [], "premom_terms": [],
                       "guard": guard, "status": "OK", "max_positions": 20, "daily_loss_rs": 0.0}
                mtr = pfb.full_metrics(setup, cfg, sub[sub["_day"].isin(set(TRAINs))].copy())
                mte = pfb.full_metrics(setup, cfg, sub[sub["_day"].isin(set(TESTs))].copy())
                print(f" top_n={tn} SL{sl}/T{tgt} | TRAIN n={mtr['n']:3d} PF={mtr['net_pf']:.2f} "
                      f"win%={mtr['win_rate']:.0f} tpd={mtr['trades_per_day']:.1f} tgt%={mtr['target_rate']:.0f} "
                      f"| TEST n={mte['n']:3d} PF={mte['net_pf']:.2f} win%={mte['win_rate']:.0f} "
                      f"tpd={mte['trades_per_day']:.1f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
