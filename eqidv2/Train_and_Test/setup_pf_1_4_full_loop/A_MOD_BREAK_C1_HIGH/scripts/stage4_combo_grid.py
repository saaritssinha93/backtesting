r"""stage4_combo_grid.py — Stage-4 evidence-driven combination grid on the morning pool.

Combines ONLY the knobs that survived (or were neutral in) Stage-3 VAL:
  vol_ratio floor x top_n x SL x TGT (+ daily-loss / max-positions variants)
Flow: FIT -> VAL -> full-TRAIN confirm (band 1.30-1.80) -> TEST once for band members.
Everything is logged to stage4_combo_results.csv.
"""
from __future__ import annotations

import itertools
import json
import sys
import time
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

SETUP = "A_MOD_BREAK_C1_HIGH"
TRAIN_START = pd.Timestamp("2026-03-01")
TRAIN_END = pd.Timestamp("2026-05-30")
TEST_START = pd.Timestamp("2026-06-01")
FIT_FRAC = 0.60
PF_LO, PF_HI, TEST_PF_MIN = 1.30, 1.80, 1.40


def metrics(cfg, df, full=False):
    tt.MAX_POSITIONS = int(cfg.get("max_positions") or 20)
    tt.DAILY_LOSS_RS = float(cfg.get("daily_loss_rs") or 0.0)
    fn = eng.full_metrics if full else eng.fast_metrics
    m = fn(SETUP, cfg, df)
    return {k: v for k, v in m.items() if k != "detail"}


def main() -> int:
    pool_dir = sys.argv[1] if len(sys.argv) > 1 else str(WORK / "pools" / "pool_morning")
    tt.POOL_DIRS = [str(Path(pool_dir).resolve())]
    tt.SLIPPAGE_BPS = 15.0
    tt._entry.cache_clear(); tt._resolve_full.cache_clear(); tt._premom.cache_clear()

    pool = tt.load_pool()
    pool = pool[pool["setup"].astype(str).str.upper().eq(SETUP)].copy()
    train_raw = pool[(pool["_day"] >= TRAIN_START) & (pool["_day"] <= TRAIN_END)].reset_index(drop=True)
    test_raw = pool[pool["_day"] >= TEST_START].reset_index(drop=True)
    train = tt.attach_entries(train_raw)
    test = tt.attach_entries(test_raw)
    tr_sessions = sorted(train["_day"].dt.strftime("%Y-%m-%d").unique())
    n_fit = max(1, int(round(FIT_FRAC * len(tr_sessions))))
    fit_s, val_s = tr_sessions[:n_fit], tr_sessions[n_fit:]
    fit = train[train["_day"].dt.strftime("%Y-%m-%d").isin(fit_s)].reset_index(drop=True)
    val = train[train["_day"].dt.strftime("%Y-%m-%d").isin(val_s)].reset_index(drop=True)
    print(f"[combo] FIT {len(fit_s)}s/{len(fit)}r VAL {len(val_s)}s/{len(val)}r TRAIN {len(tr_sessions)}s/{len(train)}r TEST {len(test)}r")

    grid = []
    for vol, tn, sl, tgt in itertools.product(
            [None, 2.2, 2.6, 3.0],
            [None, 1, 2],
            [0.85, 1.0, 1.2, 1.5],
            [1.0, 1.25, 1.5, 2.0]):
        mask = [("vol_ratio", ">=", vol)] if vol else []
        guard = {"top_n": tn} if tn else None
        grid.append({"sl": sl, "tgt": tgt, "mask_terms": mask, "premom_terms": [],
                     "guard": guard, "status": "OK", "max_positions": 20, "daily_loss_rs": 0.0})
    # reference: production exit on morning pool
    grid.append({"sl": 0.70, "tgt": 1.00, "mask_terms": [], "premom_terms": [], "guard": None,
                 "status": "OK", "max_positions": 20, "daily_loss_rs": 0.0})

    rows = []
    t0 = time.time()
    for i, cfg in enumerate(grid, 1):
        mf = metrics(cfg, fit)
        mv = metrics(cfg, val)
        rec = {"idx": i,
               "cfg": json.dumps({k: cfg[k] for k in ("sl", "tgt", "mask_terms", "guard")}, default=str),
               "fit_n": mf["n"], "fit_pf": mf["net_pf"], "val_n": mv["n"], "val_pf": mv["net_pf"]}
        # FIT/VAL gate: both must be >= PF_LO - 0.1 to bother confirming
        if (mf["n"] >= 12 and mv["n"] >= 8 and
                float(mf["net_pf"]) >= PF_LO - 0.10 and float(mv["net_pf"]) >= PF_LO - 0.10):
            mtr = metrics(cfg, train, full=True)
            rec.update({"train_n": mtr["n"], "train_pf": mtr["net_pf"], "train_net": mtr["net_pnl"],
                        "train_dayp": mtr["day_block_p"], "train_domT": mtr["trade_dom_gross"],
                        "train_domD": mtr["day_dom"], "train_domS": mtr["sym_dom"]})
            if mtr["n"] >= 20 and PF_LO <= float(mtr["net_pf"]) <= PF_HI:
                mte = metrics(cfg, test, full=True)
                rec.update({"test_n": mte["n"], "test_pf": mte["net_pf"], "test_net": mte["net_pnl"],
                            "test_dayp": mte["day_block_p"], "test_domT": mte["trade_dom_gross"],
                            "test_domD": mte["day_dom"], "test_domS": mte["sym_dom"],
                            "passes": bool(mte["n"] >= 6 and float(mte["net_pf"]) > TEST_PF_MIN
                                           and float(mte["net_pnl"]) > 0)})
        rows.append(rec)
        if i % 20 == 0:
            print(f"[combo] {i}/{len(grid)} elapsed={time.time()-t0:.0f}s", flush=True)

    df = pd.DataFrame(rows)
    out = WORK / "stage4_combo_results.csv"
    df.to_csv(out, index=False)
    print(f"[combo] wrote {out}")
    conf = df.dropna(subset=["train_pf"]) if "train_pf" in df.columns else pd.DataFrame()
    if len(conf):
        print("\n=== configs confirmed on full TRAIN ===")
        cols = [c for c in ("cfg", "fit_pf", "val_pf", "train_n", "train_pf", "test_n", "test_pf", "passes") if c in conf.columns]
        print(conf[cols].sort_values("train_pf", ascending=False).head(15).to_string(index=False))
    else:
        print("\n=== NO config cleared the FIT/VAL pre-gate ===")
        print(df.sort_values("fit_pf", ascending=False).head(10)[["cfg", "fit_n", "fit_pf", "val_n", "val_pf"]].to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
