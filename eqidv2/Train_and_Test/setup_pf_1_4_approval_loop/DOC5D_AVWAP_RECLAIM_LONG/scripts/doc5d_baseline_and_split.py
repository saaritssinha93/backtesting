r"""doc5d_baseline_and_split.py — Stage-1 RAW-detection baseline for DOC5D_AVWAP_RECLAIM_LONG.
============================================================================
Research-only. Reuses the repo's setup_train_test pipeline (entry / exit / cost /
dedupe / portfolio overlay) and the shared engine's full_metrics, so the baseline
is computed on the exact same machinery the approval loop uses. NO conf edits, NO
live trades.

Prints, and dumps to raw_baseline.json:
  * the exact FIT / VAL / TRAIN / TEST sessions under the user's split
    (TRAIN from 2026-05-18, TEST from 2026-06-20)
  * RAW-detection metrics (no mask, no pre-momentum, no guard; doc default exit
    SL 0.70 / Tgt 1.25) on each window at 15 bps and 5 bps / leg.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

_HERE = Path(__file__).resolve()
_SETUP_DIR = _HERE.parent.parent                         # .../DOC5D_AVWAP_RECLAIM_LONG
_LOOP_DIR = _SETUP_DIR.parent                            # .../setup_pf_1_4_approval_loop
_TT_DIR = _LOOP_DIR.parent                               # .../Train_and_Test
_REPO = _TT_DIR.parent
for _p in (str(_REPO), str(_TT_DIR), str(_LOOP_DIR / "_engine")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import setup_train_test as tt                            # noqa: E402
import pf_band_fitval_loop as pfb                        # noqa: E402

SETUP = "DOC5D_AVWAP_RECLAIM_LONG"
POOL = _TT_DIR / "doc5_long_setups" / "pool"
TRAIN_START = pd.Timestamp("2026-05-18")
TEST_START = pd.Timestamp("2026-06-20")
MIN_TEST_SESSIONS = 4
N_TEST_FALLBACK = 5

# doc-suggested raw baseline exit (scan_doc5_long_setups SETUPS[DOC5D] = 0.70/1.25)
BASE_CFG = {"sl": 0.70, "tgt": 1.25, "mask_terms": [], "premom_terms": [],
            "guard": None, "status": "OK", "max_positions": 20, "daily_loss_rs": 0.0}


def _rng(ss):
    return f"{pd.Timestamp(ss[0]).date()}..{pd.Timestamp(ss[-1]).date()}" if ss else "(empty)"


def main() -> int:
    tt.POOL_DIRS = [POOL]; tt.POOL_DIR = POOL
    pool = tt.load_pool()
    pool = pool[pool["setup"] == SETUP].copy()
    sessions = sorted(pd.Series(pool["_day"].dropna().unique()))
    print(f"[baseline] DOC5D pool rows={len(pool)} sessions={len(sessions)} "
          f"({_rng(sessions)})")

    test_cal = [s for s in sessions if s >= TEST_START]
    if len(test_cal) >= MIN_TEST_SESSIONS:
        TEST_s = test_cal
        note = f"TEST = calendar sessions >= {TEST_START.date()} ({len(TEST_s)} sessions)."
    else:
        TEST_s = sessions[-N_TEST_FALLBACK:]
        note = (f"calendar TEST (>= {TEST_START.date()}) had only {len(test_cal)} session(s) "
                f"-> FELL BACK to last {N_TEST_FALLBACK} sessions.")
    first_test = TEST_s[0]
    TRAIN_s = [s for s in sessions if (s >= TRAIN_START and s < first_test)]
    half = len(TRAIN_s) // 2
    FIT_s, VAL_s = TRAIN_s[:half], TRAIN_s[half:]

    print(f"[baseline] split note: {note}")
    print(f"[baseline] FIT   {_rng(FIT_s)} ({len(FIT_s)})  {[str(pd.Timestamp(s).date()) for s in FIT_s]}")
    print(f"[baseline] VAL   {_rng(VAL_s)} ({len(VAL_s)})  {[str(pd.Timestamp(s).date()) for s in VAL_s]}")
    print(f"[baseline] TRAIN {_rng(TRAIN_s)} ({len(TRAIN_s)})")
    print(f"[baseline] TEST  {_rng(TEST_s)} ({len(TEST_s)})  {[str(pd.Timestamp(s).date()) for s in TEST_s]}")

    span = set(map(pd.Timestamp, FIT_s + VAL_s + TEST_s))
    sub_all = pool[pool["_day"].isin(span)].copy()

    def _slice(base, ss):
        return base[base["_day"].isin(set(map(pd.Timestamp, ss)))].copy()

    out = {"setup": SETUP, "split_note": note,
           "windows": {"FIT": _rng(FIT_s), "VAL": _rng(VAL_s),
                       "TRAIN": _rng(TRAIN_s), "TEST": _rng(TEST_s),
                       "FIT_sessions": [str(pd.Timestamp(s).date()) for s in FIT_s],
                       "VAL_sessions": [str(pd.Timestamp(s).date()) for s in VAL_s],
                       "TEST_sessions": [str(pd.Timestamp(s).date()) for s in TEST_s]},
           "baseline_cfg": {"sl": BASE_CFG["sl"], "tgt": BASE_CFG["tgt"],
                            "mask_terms": [], "premom_terms": [], "guard": None},
           "metrics": {}}

    tt.MAX_POSITIONS = BASE_CFG["max_positions"]; tt.DAILY_LOSS_RS = BASE_CFG["daily_loss_rs"]
    for bps in (15.0, 5.0):
        pfb._set_slippage(bps)
        sub = tt.attach_entries(sub_all)
        for lbl, ss in (("FIT", FIT_s), ("VAL", VAL_s), ("TRAIN", TRAIN_s), ("TEST", TEST_s)):
            m = pfb.full_metrics(SETUP, BASE_CFG, _slice(sub, ss))
            key = f"{lbl}_{int(bps)}bps"
            out["metrics"][key] = {k: v for k, v in m.items() if k != "detail"}
            print(f"[baseline] {key:12s} n={m['n']:3d} PF={m['net_pf']:.3f} "
                  f"net=Rs{m['net_pnl']:>8,.0f} win%={m['win_rate']:.1f} "
                  f"SL/TGT/EOD={m['sl_cnt']}/{m['tgt_cnt']}/{m['eod_cnt']} "
                  f"tgt%={m['target_rate']:.1f} tpd={m['trades_per_day']} "
                  f"dayDom={m['day_dom']} symDom={m['sym_dom']} dbp={m['day_block_p']}")

    (_SETUP_DIR / "raw_baseline.json").write_text(json.dumps(out, indent=2, default=str), encoding="utf-8")
    print(f"[baseline] wrote {_SETUP_DIR / 'raw_baseline.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
