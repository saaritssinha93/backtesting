r"""eval_baseline_recovery.py — baseline for the REDESIGNED (uncollapsed) pool.

Evaluates the card conditions as re-detected by scan_redesigned_pool.py, at the
production exit (SL 0.70 / Tgt 1.50), through the standard tt pipeline
(family dedupe -> portfolio overlay -> 1-min entry/exit, statutory costs,
15 bps/leg slippage), on FIT / VAL / TRAIN / TEST, plus per-regime TRAIN slices.

Usage: py -3.12 eval_baseline_recovery.py [--pool <dir>] [--tag baseline_redesigned]
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve()
WORK = HERE.parents[1]
TT_DIR = HERE.parents[3]
REPO = TT_DIR.parent
ENGINE_DIR = TT_DIR / "setup_pf_1_4_approval_loop" / "_engine"
for p in (REPO, TT_DIR, ENGINE_DIR):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

import setup_train_test as tt  # noqa: E402
import pf_band_fitval_loop as eng  # noqa: E402

SETUP = "A_MOD_CLOSE_CONTINUATION_BREAK"
TRAIN_START = pd.Timestamp("2026-03-01")
TRAIN_END = pd.Timestamp("2026-05-30")
TEST_START = pd.Timestamp("2026-06-01")
TEST_END = pd.Timestamp("2026-07-01")   # 07-02 excluded: truncated 1-min data
FIT_FRAC = 0.60

CFG = {"sl": 0.70, "tgt": 1.50, "mask_terms": [], "premom_terms": [], "guard": None,
       "status": "OK", "max_positions": 20, "daily_loss_rs": 0.0}


def eval_cfg(cfg, df):
    tt.MAX_POSITIONS = int(cfg.get("max_positions") or 20)
    tt.DAILY_LOSS_RS = float(cfg.get("daily_loss_rs") or 0.0)
    m = eng.full_metrics(SETUP, cfg, df)
    return {k: v for k, v in m.items() if k != "detail"}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pool", default=str(WORK / "pools" / "pool_enriched"))
    ap.add_argument("--tag", default="baseline_redesigned")
    ap.add_argument("--cfg_json", default="")
    args = ap.parse_args()
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass

    tt.POOL_DIRS = [str(Path(args.pool).resolve())]
    tt.SLIPPAGE_BPS = 15.0
    tt._entry.cache_clear(); tt._resolve_full.cache_clear(); tt._premom.cache_clear()

    pool = tt.load_pool()
    pool = pool[pool["setup"].astype(str).str.upper().eq(SETUP)].copy()
    train_raw = pool[(pool["_day"] >= TRAIN_START) & (pool["_day"] <= TRAIN_END)].reset_index(drop=True)
    test_raw = pool[(pool["_day"] >= TEST_START) & (pool["_day"] <= TEST_END)].reset_index(drop=True)
    train = tt.attach_entries(train_raw)
    test = tt.attach_entries(test_raw)
    print(f"[base-r] entries: train {len(train)} test {len(test)}", flush=True)

    tr_sessions = sorted(train["_day"].dt.strftime("%Y-%m-%d").unique())
    te_sessions = sorted(test["_day"].dt.strftime("%Y-%m-%d").unique())
    n_fit = max(1, int(round(FIT_FRAC * len(tr_sessions))))
    fit_s, val_s = tr_sessions[:n_fit], tr_sessions[n_fit:]
    fit = train[train["_day"].dt.strftime("%Y-%m-%d").isin(fit_s)].reset_index(drop=True)
    val = train[train["_day"].dt.strftime("%Y-%m-%d").isin(val_s)].reset_index(drop=True)

    cfg = json.loads(Path(args.cfg_json).read_text()) if args.cfg_json else CFG

    out = {"setup": SETUP, "pool": str(args.pool), "slippage_bps": 15.0,
           "windows": {"TRAIN": [tr_sessions[0], tr_sessions[-1], len(tr_sessions)],
                       "FIT": [fit_s[0], fit_s[-1], len(fit_s)],
                       "VAL": [val_s[0], val_s[-1], len(val_s)],
                       "TEST": [te_sessions[0], te_sessions[-1], len(te_sessions)]},
           "rows": {"train_entries": len(train), "test_entries": len(test)},
           "results": {}, "regime_slices_train": {}}
    for wname, wdf in (("FIT", fit), ("VAL", val), ("TRAIN", train), ("TEST", test)):
        m = eval_cfg(cfg, wdf)
        out["results"][wname] = m
        print(f"  {wname:5s} n={m['n']:>5} PF={m['net_pf']:<7} net=Rs{m['net_pnl']:>11,.0f} "
              f"win%={m['win_rate']:<5} tr/day={m['trades_per_day']} "
              f"sl/tgt/eod={m.get('sl_cnt')}/{m.get('tgt_cnt')}/{m.get('eod_cnt')}", flush=True)

    for rg in ("BULL", "BEAR", "NEUTRAL", "TREND"):
        sl = train[train["regime"].astype(str).str.upper() == rg].reset_index(drop=True)
        if len(sl) < 5:
            out["regime_slices_train"][rg] = {"n_pool": int(len(sl))}
            continue
        m = eval_cfg(cfg, sl)
        out["regime_slices_train"][rg] = m
        print(f"  TRAIN[{rg:7s}] n={m['n']:>5} PF={m['net_pf']:<7} net=Rs{m['net_pnl']:>11,.0f}",
              flush=True)

    (WORK / f"{args.tag}_result.json").write_text(json.dumps(out, indent=2, default=str),
                                                  encoding="utf-8")
    print(f"[base-r] wrote {WORK / (args.tag + '_result.json')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
