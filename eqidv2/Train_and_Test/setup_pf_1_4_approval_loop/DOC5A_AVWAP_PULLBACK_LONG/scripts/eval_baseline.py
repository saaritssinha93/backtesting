r"""eval_baseline.py — Stage-1 baseline for DOC5A_AVWAP_PULLBACK_LONG.
============================================================================
Research-only. Evaluates the RAW / doc-default config (SL 0.70% / Tgt 1.25%,
no mask, no pre-momentum, no guards) on FIT / VAL / full TRAIN / TEST using the
repo pipeline (setup_train_test.py) at 15 bps/leg — the same entry/exit/cost/
dedupe/overlay model the pf-band loop uses. Prints full metrics per window so
BASELINE_RESULT.md has a real, non-tuned reference point.

Run from repo root:
  py -3.12 Train_and_Test/setup_pf_1_4_approval_loop/DOC5A_AVWAP_PULLBACK_LONG/scripts/eval_baseline.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve()
REPO = HERE
for _ in range(12):
    if (REPO / "Train_and_Test").exists() and (REPO / "final_setup_conf.py").exists():
        break
    REPO = REPO.parent
ENGINE = REPO / "Train_and_Test" / "setup_pf_1_4_approval_loop" / "_engine"
for p in (str(REPO), str(REPO / "Train_and_Test"), str(ENGINE)):
    if p not in sys.path:
        sys.path.insert(0, p)

import setup_train_test as tt              # noqa: E402
import pf_band_fitval_loop as pbl          # noqa: E402

SETUP = "DOC5A_AVWAP_PULLBACK_LONG"
POOL = REPO / "Train_and_Test" / "doc5_long_setups" / "pool"
TRAIN_START = pd.Timestamp("2026-05-18")
TEST_START = pd.Timestamp("2026-06-20")
BASE_CFG = {"sl": 0.70, "tgt": 1.25, "mask_terms": [], "premom_terms": [],
            "guard": None, "status": "OK", "max_positions": 20, "daily_loss_rs": 0.0}


def _mline(m):
    return (f"n={m['n']} PF={m['net_pf']} net=Rs{m['net_pnl']:,.0f} win%={m['win_rate']} "
            f"avgW=Rs{m['avg_win']:,.0f} avgL=Rs{m['avg_loss']:,.0f} maxDD=Rs{m['max_dd']:,.0f} "
            f"SL/TGT/EOD={m['sl_cnt']}/{m['tgt_cnt']}/{m['eod_cnt']} tgt%={m['target_rate']} "
            f"tpd={m['trades_per_day']} tradeDom={m['trade_dom_gross']} dayDom={m['day_dom']} "
            f"symDom={m['sym_dom']} dbp={m['day_block_p']}")


def main() -> int:
    tt.POOL_DIRS = [POOL]; tt.POOL_DIR = POOL
    pool = tt.load_pool()
    pool = pool[pool["setup"] == SETUP].copy()
    sessions = sorted(pd.Series(pool["_day"].dropna().unique()))
    test_s = [s for s in sessions if s >= TEST_START]
    if len(test_s) < 4:
        test_s = sessions[-5:]
    first_test = test_s[0]
    train_s = [s for s in sessions if (s >= TRAIN_START and s < first_test)]
    half = len(train_s) // 2
    fit_s, val_s = train_s[:half], train_s[half:]

    def _rng(ss):
        return f"{pd.Timestamp(ss[0]).date()}..{pd.Timestamp(ss[-1]).date()}" if ss else "(empty)"
    print(f"[baseline] {SETUP}")
    print(f"[baseline] FIT   {_rng(fit_s)} ({len(fit_s)})")
    print(f"[baseline] VAL   {_rng(val_s)} ({len(val_s)})")
    print(f"[baseline] TRAIN {_rng(train_s)} ({len(train_s)})")
    print(f"[baseline] TEST  {_rng(test_s)} ({len(test_s)})")

    pbl._set_slippage(15.0)
    span = set(map(pd.Timestamp, fit_s + val_s + test_s))
    sub = tt.attach_entries(pool[pool["_day"].isin(span)].copy())

    def _slice(ss):
        return sub[sub["_day"].isin(set(map(pd.Timestamp, ss)))].copy()

    tt.MAX_POSITIONS = BASE_CFG["max_positions"]; tt.DAILY_LOSS_RS = BASE_CFG["daily_loss_rs"]
    for lbl, ss in (("FIT", fit_s), ("VAL", val_s), ("TRAIN", fit_s + val_s), ("TEST", test_s)):
        m = pbl.full_metrics(SETUP, BASE_CFG, _slice(ss))
        print(f"  {lbl:<5} @15bps {_mline(m)}")
    print("\nbaseline cfg:", {k: v for k, v in BASE_CFG.items() if k not in ('status',)})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
