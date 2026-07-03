r"""smoke.py — verify the tt pipeline runs end-to-end on the unified pool for
P_PDH_BREAK_RETEST_LONG, print the available sessions, and eval the demoted
baseline config on a tentative TRAIN/TEST split. Research-only; no conf edits."""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
# scripts/ -> P_PDH.../ -> setup_pf_1_4_approval_loop/ -> Train_and_Test/
_TT = _HERE.parent.parent.parent
for _p in (str(_TT.parent), str(_TT)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import setup_train_test as tt  # noqa: E402

SETUP = "P_PDH_BREAK_RETEST_LONG"
POOL = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_unified_pool")

BASELINE = {
    "status": "OK", "sl": 0.50, "tgt": 0.60,
    "mask_terms": [("body_pct", "<=", 0.749993)],
    "premom_terms": [("pre_entry_momentum_score", ">=", 75.071712),
                     ("pre3_range_r", ">=", 0.499787)],
    "guard": None,
}


def main() -> int:
    tt.SLIPPAGE_BPS = 15.0
    tt.POOL_DIRS = [POOL]
    tt.POOL_DIR = POOL
    pool = tt.load_pool()
    pool = pool[pool["setup"] == SETUP].copy()
    sessions = sorted(pd.Series(pool["_day"].dropna().unique()))
    print(f"[smoke] {SETUP}: {len(pool)} raw rows over {len(sessions)} sessions")
    print(f"[smoke] first session {pd.Timestamp(sessions[0]).date()}  last {pd.Timestamp(sessions[-1]).date()}")

    # tentative split: TEST = last 9 sessions, TRAIN = 27 before
    TEST_s = sessions[-9:]
    TRAIN_s = sessions[-36:-9]
    print(f"[smoke] TRAIN {pd.Timestamp(TRAIN_s[0]).date()}..{pd.Timestamp(TRAIN_s[-1]).date()} ({len(TRAIN_s)} sess)")
    print(f"[smoke] TEST  {pd.Timestamp(TEST_s[0]).date()}..{pd.Timestamp(TEST_s[-1]).date()} ({len(TEST_s)} sess)")

    span = set(map(pd.Timestamp, list(TRAIN_s) + list(TEST_s)))
    sub = pool[pool["_day"].isin(span)].copy()
    sub = tt.attach_entries(sub)
    print(f"[smoke] rows with 1m entry in span: {len(sub)}")

    def _slice(ss):
        return sub[sub["_day"].isin(set(map(pd.Timestamp, ss)))].copy()

    for lbl, ss in (("TRAIN", TRAIN_s), ("TEST", TEST_s)):
        df = _slice(ss)
        fam = tt.eval_family({SETUP: BASELINE}, df)
        det = tt.book_detail(fam["book"], {SETUP: (BASELINE["sl"], BASELINE["tgt"])}) if fam["trades"] else pd.DataFrame()
        wins = int((det["net_pnl_rs"] > 0).sum()) if not det.empty else 0
        print(f"[smoke] {lbl}: n={fam['trades']} PF={fam['net_pf']:.3f} net=Rs{fam['net_pnl']:,.0f} "
              f"win={wins} dbp={fam['day_block_p']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
