r"""sweep_exit_last2mo.py — SL x Target sweep for a fixed-entry near-miss config.
============================================================================
Research-only. Holds the entry gating (mask/premom/guard) FIXED and sweeps the
exit bracket SL x Target, printing TRAIN(05-18..06-19), TEST(06-20..06-30) and
FULL(last-2-months) metrics @5bps for each bracket. Flags any bracket that meets
the goal (TRAIN PF in [1.30,1.70] AND TEST PF>1.40 with meaningful counts) and
the best FULL-window bracket.

NOTE: choosing a bracket by looking at TEST is TEST-fitting; the FULL-window PF
(all 2 months) is the honest arbiter and is printed alongside.

  py -3.12 .../scripts/sweep_exit_last2mo.py --pool .../pool_vA --label vA-s23 \
     --cfg '{"mask_terms":[],"premom_terms":[["sig5_adx_calc","<=",20.8676]],"guard":{"min_slot":"11:00","top_n":3},"max_positions":20,"daily_loss_rs":0.0}'
"""
from __future__ import annotations
import argparse, json, sys
from pathlib import Path
import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve()
_SETUP_DIR = _HERE.parent.parent; _LOOP_DIR = _SETUP_DIR.parent; _TT_DIR = _LOOP_DIR.parent
_REPO = _TT_DIR.parent
for _p in (str(_REPO), str(_TT_DIR), str(_LOOP_DIR / "_engine")):
    if _p not in sys.path:
        sys.path.insert(0, _p)
import setup_train_test as tt          # noqa: E402
import pf_band_fitval_loop as pfb      # noqa: E402

SETUP = "DOC5D_AVWAP_RECLAIM_LONG"
SL_GRID = [0.5, 0.6, 0.7, 0.85, 1.0, 1.1, 1.2, 1.35, 1.5]
TGT_GRID = [0.6, 0.8, 1.0, 1.25, 1.5, 1.75, 2.0, 2.5, 3.0]
BPS = 5.0
PF_LO, PF_HI, TEST_MIN = 1.30, 1.70, 1.40
MIN_TR, MIN_TE = 20, 6


def _metrics(cfg, df):
    m = pfb.full_metrics(SETUP, cfg, df)
    return m["n"], m["net_pf"], m["net_pnl"], m["win_rate"], m["day_dom"], m["trade_dom_gross"]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pool", required=True)
    ap.add_argument("--cfg", required=True)
    ap.add_argument("--label", default="config")
    args = ap.parse_args()
    base = json.loads(args.cfg)
    mask = [tuple(t) for t in base.get("mask_terms", [])]
    prem = [tuple(t) for t in base.get("premom_terms", [])]
    guard = base.get("guard") or None
    maxpos = int(base.get("max_positions", 20)); dloss = float(base.get("daily_loss_rs", 0.0))

    tt.POOL_DIRS = [Path(args.pool)]; tt.POOL_DIR = Path(args.pool)
    pool = tt.load_pool(); pool = pool[pool["setup"] == SETUP].copy()
    sess = sorted(pd.Series(pool["_day"].dropna().unique()))
    TESTs = [s for s in sess if s >= pd.Timestamp("2026-06-20")]
    ft = TESTs[0]; TRAINs = [s for s in sess if pd.Timestamp("2026-05-18") <= s < ft]
    pfb._set_slippage(BPS); tt.MAX_POSITIONS = maxpos; tt.DAILY_LOSS_RS = dloss
    sub = tt.attach_entries(pool.copy())
    TR = sub[sub["_day"].isin(set(TRAINs))].copy()
    TE = sub[sub["_day"].isin(set(TESTs))].copy()
    FULL = sub.copy()

    print(f"[sweep] {args.label}  pool={Path(args.pool).name}  @{BPS}bps  entry FIXED "
          f"(mask={mask or '-'} premom={prem or '-'} guard={guard or '-'})")
    print(f"[sweep] TRAIN {len(TRAINs)}sess  TEST {len(TESTs)}sess  FULL {len(sess)}sess "
          f"({pd.Timestamp(sess[0]).date()}..{pd.Timestamp(sess[-1]).date()})")
    print(f"[sweep] GOAL = TRAIN PF in [{PF_LO},{PF_HI}] AND TEST PF>{TEST_MIN} "
          f"(TRAIN n>={MIN_TR}, TEST n>={MIN_TE})\n")
    hdr = f"  {'SL':>4s} {'TGT':>4s} | {'TRn':>4s} {'TRpf':>5s} | {'TEn':>3s} {'TEpf':>5s} | {'FULn':>4s} {'FULpf':>5s} {'FULnet':>8s}  flag"
    print(hdr); print("  " + "-" * (len(hdr) - 2))

    goal_hits, best_full = [], None
    rows = []
    for sl in SL_GRID:
        for tgt in TGT_GRID:
            if tgt <= sl:  # require target >= SL (positive-ish R:R); skip inverted tiny targets
                pass       # keep them anyway for completeness
            cfg = {"sl": sl, "tgt": tgt, "mask_terms": mask, "premom_terms": prem,
                   "guard": guard, "status": "OK", "max_positions": maxpos, "daily_loss_rs": dloss}
            ntr, pftr, _, _, _, _ = _metrics(cfg, TR)
            nte, pfte, _, _, _, _ = _metrics(cfg, TE)
            nfu, pffu, netfu, winfu, ddfu, tdfu = _metrics(cfg, FULL)
            goal = (PF_LO <= pftr <= PF_HI and pfte > TEST_MIN and ntr >= MIN_TR and nte >= MIN_TE)
            flag = "<== GOAL" if goal else ""
            if goal:
                goal_hits.append((sl, tgt, ntr, pftr, nte, pfte, nfu, pffu, netfu, ddfu, tdfu))
            if best_full is None or pffu > best_full[7]:
                best_full = (sl, tgt, ntr, pftr, nte, pfte, nfu, pffu, netfu, ddfu, tdfu)
            rows.append((sl, tgt, ntr, pftr, nte, pfte, nfu, pffu, netfu, flag))
    for sl, tgt, ntr, pftr, nte, pfte, nfu, pffu, netfu, flag in rows:
        print(f"  {sl:4.2f} {tgt:4.2f} | {ntr:4d} {pftr:5.2f} | {nte:3d} {pfte:5.2f} | "
              f"{nfu:4d} {pffu:5.2f} {netfu:8,.0f}  {flag}")

    print("\n==== GOAL-MATCHING BRACKETS (TRAIN band + TEST>1.40) ====")
    if goal_hits:
        for sl, tgt, ntr, pftr, nte, pfte, nfu, pffu, netfu, ddfu, tdfu in goal_hits:
            print(f"  SL{sl}/T{tgt}: TRAIN {ntr}/{pftr:.2f}  TEST {nte}/{pfte:.2f}  "
                  f"FULL {nfu}/{pffu:.2f} netRs{netfu:,.0f}  (full day_dom={ddfu} trade_dom={tdfu})")
    else:
        print("  NONE — no SL x target bracket puts TRAIN in-band AND TEST>1.40 with meaningful counts.")
    b = best_full
    print(f"\n==== BEST FULL-WINDOW (honest arbiter) ====")
    print(f"  SL{b[0]}/T{b[1]}: FULL n={b[6]} PF={b[7]:.2f} netRs{b[8]:,.0f}  "
          f"(TRAIN {b[2]}/{b[3]:.2f}  TEST {b[4]}/{b[5]:.2f}  day_dom={b[9]} trade_dom={b[10]})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
