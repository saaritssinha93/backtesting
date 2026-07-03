r"""structural_pf_band.py — reinvented DOC5A search at 5 bps/leg.
============================================================================
Runs the shared pf-band FIT/VAL->TRAIN->TEST engine on the RICH variant pool
(mine_rich_pool.py), but EXTENDS the searchable mask feature set with the doc's
own structural detector knobs, so the search tunes the ENTRY STRUCTURE (not just
generic re-filtering):

  vwap_slope_atr  established_bars  pullback_depth_atr  orh_dist_atr
  ema20_dist_atr  adx_sig  rsi_sig

Cost model: 5 bps/leg (per user request). Band gate unchanged: TRAIN PF in
[1.30,1.70], TEST PF > 1.40, non-dominated, robust. Writes artifacts to
<SETUP>/reinvent_5bps/DOC5A_AVWAP_PULLBACK_LONG/ so the 15 bps campaign is kept.

Research-only. No conf edits. No live trades.

Run from repo root:
  py -3.12 Train_and_Test/setup_pf_1_4_approval_loop/DOC5A_AVWAP_PULLBACK_LONG/scripts/structural_pf_band.py
"""
from __future__ import annotations

import sys
from pathlib import Path

HERE = Path(__file__).resolve()
SETUP_DIR = HERE.parent.parent                       # DOC5A_AVWAP_PULLBACK_LONG/
REPO = HERE
for _ in range(12):
    if (REPO / "Train_and_Test").exists() and (REPO / "final_setup_conf.py").exists():
        break
    REPO = REPO.parent
ENGINE = REPO / "Train_and_Test" / "setup_pf_1_4_approval_loop" / "_engine"
for p in (str(REPO), str(REPO / "Train_and_Test"), str(ENGINE)):
    if p not in sys.path:
        sys.path.insert(0, p)

import pf_band_fitval_loop as pbl  # noqa: E402

STRUCTURAL = ["vwap_slope_atr", "established_bars", "pullback_depth_atr",
              "orh_dist_atr", "ema20_dist_atr", "adx_sig", "rsi_sig"]
for f in STRUCTURAL:
    if f not in pbl.MASK_FEATS:
        pbl.MASK_FEATS.append(f)

POOL = SETUP_DIR / "variant_pool"
OUT = SETUP_DIR / "reinvent_5bps"


def main() -> int:
    argv = [
        "structural_pf_band",
        "--setup", "DOC5A_AVWAP_PULLBACK_LONG",
        "--pool", str(POOL),
        "--out", str(OUT),
        "--search_slippage_bps", "5",
        "--test_pf_min", "1.40",
        "--train_start", "2026-05-18",
        "--test_start", "2026-06-20",
        "--max_mask_terms", "2",
        "--max_pm_terms", "1",
        "--trials", "900",
        "--time_budget_min", "26",
        "--seed", "7",
    ]
    sys.argv = argv
    print(f"[structural] extended MASK_FEATS with: {STRUCTURAL}")
    print(f"[structural] pool={POOL}")
    print(f"[structural] cost=5bps/leg  band TRAIN[1.30,1.70]  TEST>1.40")
    return pbl.main()


if __name__ == "__main__":
    raise SystemExit(main())
