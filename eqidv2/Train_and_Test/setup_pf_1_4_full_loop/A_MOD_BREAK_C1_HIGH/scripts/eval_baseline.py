r"""eval_baseline.py — baseline evaluation for A_MOD_BREAK_C1_HIGH (research-only).

Splits the recreated pool into TRAIN (2026-03-01..2026-05-30) / TEST (2026-06-01..)
with FIT = first 60% of TRAIN sessions, VAL = remaining 40%, then evaluates:

  raw_detector     : detector output, production exit (SL 0.70 / Tgt 1.00), no gate
  baseline_current : + live gate  rs_pct>=2.0 & atr_pct<=0.006, guard max_slot 11:05,
                     top-2 per slot by vwap_dist_atr (mirrors v11 overlay + entry engine)

Usage: py -3.12 eval_baseline.py [--pool <dir>] [--tag baseline] [--cfg_json <path>]
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve()
WORK = HERE.parents[1]
TT_DIR = HERE.parents[3]          # Train_and_Test
REPO = TT_DIR.parent
ENGINE_DIR = TT_DIR / "setup_pf_1_4_approval_loop" / "_engine"
for p in (REPO, TT_DIR, ENGINE_DIR):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

import setup_train_test as tt  # noqa: E402
import pf_band_fitval_loop as eng  # noqa: E402

SETUP = "A_MOD_BREAK_C1_HIGH"
TRAIN_START = pd.Timestamp("2026-03-01")
TRAIN_END = pd.Timestamp("2026-05-30")     # inclusive
TEST_START = pd.Timestamp("2026-06-01")
TEST_END = pd.Timestamp("2026-07-02")      # inclusive (completed sessions only)
FIT_FRAC = 0.60

BASELINE_CFG = {
    "sl": 0.70, "tgt": 1.00,
    "mask_terms": [("rs_pct", ">=", 2.0), ("atr_pct", "<=", 0.006)],
    "premom_terms": [],
    "guard": {"max_slot": "11:05", "top_n": 2},
    "status": "OK", "max_positions": 20, "daily_loss_rs": 0.0,
}
RAW_CFG = {
    "sl": 0.70, "tgt": 1.00,
    "mask_terms": [], "premom_terms": [], "guard": None,
    "status": "OK", "max_positions": 20, "daily_loss_rs": 0.0,
}


def eval_cfg(cfg, df):
    tt.MAX_POSITIONS = int(cfg.get("max_positions") or 20)
    tt.DAILY_LOSS_RS = float(cfg.get("daily_loss_rs") or 0.0)
    m = eng.full_metrics(SETUP, cfg, df)
    return {k: v for k, v in m.items() if k != "detail"}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pool", default=str(WORK / "pools" / "pool_full"))
    ap.add_argument("--tag", default="baseline")
    ap.add_argument("--cfg_json", default="", help="optional: evaluate this cfg JSON instead of baseline")
    args = ap.parse_args()

    tt.POOL_DIRS = [str(Path(args.pool).resolve())]
    tt.SLIPPAGE_BPS = 15.0
    tt.MAX_POSITIONS = 20
    tt.DAILY_LOSS_RS = 0.0
    tt._entry.cache_clear(); tt._resolve_full.cache_clear(); tt._premom.cache_clear()

    pool = tt.load_pool()
    pool = pool[pool["setup"].astype(str).str.upper().eq(SETUP)].copy()

    train_raw = pool[(pool["_day"] >= TRAIN_START) & (pool["_day"] <= TRAIN_END)].reset_index(drop=True)
    test_raw = pool[(pool["_day"] >= TEST_START) & (pool["_day"] <= TEST_END)].reset_index(drop=True)

    train = tt.attach_entries(train_raw)
    test = tt.attach_entries(test_raw)

    tr_sessions = sorted(train["_day"].dt.strftime("%Y-%m-%d").unique())
    te_sessions = sorted(test["_day"].dt.strftime("%Y-%m-%d").unique())
    n_fit = max(1, int(round(FIT_FRAC * len(tr_sessions))))
    fit_sessions, val_sessions = tr_sessions[:n_fit], tr_sessions[n_fit:]
    fit = train[train["_day"].dt.strftime("%Y-%m-%d").isin(fit_sessions)].reset_index(drop=True)
    val = train[train["_day"].dt.strftime("%Y-%m-%d").isin(val_sessions)].reset_index(drop=True)

    cfgs = {"raw_detector": RAW_CFG, "baseline_current": BASELINE_CFG}
    if args.cfg_json:
        cfgs = {args.tag: json.loads(Path(args.cfg_json).read_text())}

    out = {
        "setup": SETUP,
        "pool": str(args.pool),
        "slippage_bps": 15.0,
        "windows": {
            "TRAIN": [tr_sessions[0] if tr_sessions else None, tr_sessions[-1] if tr_sessions else None, len(tr_sessions)],
            "FIT": [fit_sessions[0] if fit_sessions else None, fit_sessions[-1] if fit_sessions else None, len(fit_sessions)],
            "VAL": [val_sessions[0] if val_sessions else None, val_sessions[-1] if val_sessions else None, len(val_sessions)],
            "TEST": [te_sessions[0] if te_sessions else None, te_sessions[-1] if te_sessions else None, len(te_sessions)],
        },
        "rows": {"train_entries": len(train), "test_entries": len(test),
                 "fit_entries": len(fit), "val_entries": len(val)},
        "results": {},
    }
    for name, cfg in cfgs.items():
        res = {}
        for wname, wdf in (("FIT", fit), ("VAL", val), ("TRAIN", train), ("TEST", test)):
            res[wname] = eval_cfg(cfg, wdf)
        out["results"][name] = {"cfg": {k: v for k, v in cfg.items() if k != "status"}, "metrics": res}
        print(f"--- {name} ---")
        for wname in ("FIT", "VAL", "TRAIN", "TEST"):
            m = res[wname]
            print(f"  {wname:5s} n={m['n']:>4} PF={m['net_pf']:<7} net=Rs{m['net_pnl']:>10,.0f} "
                  f"win%={m['win_rate']:<5} tr/day={m['trades_per_day']} "
                  f"sl={m.get('sl_cnt')} tgt={m.get('tgt_cnt')} eod={m.get('eod_cnt')} "
                  f"dayp={m.get('day_block_p')} domT/D/S={m.get('trade_dom_gross')}/{m.get('day_dom')}/{m.get('sym_dom')}")

    out_path = WORK / f"{args.tag}_result.json"
    out_path.write_text(json.dumps(out, indent=2, default=str), encoding="utf-8")
    print(f"[baseline] wrote {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
