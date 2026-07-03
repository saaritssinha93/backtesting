r"""stage4e_combos.py — staged combination search on the first-20bar-high-break pool.

Stage A: 10 shortlisted single terms (from stage-3E VAL-stability) x 1 exit -> prune to
         survivors (FIT-lift & VAL-hold).
Stage B: survivor pairs x 3 exits x 4 guard variants -> FIT/VAL.
Stage C: full-TRAIN confirm for FIT&VAL>=1.15 configs; TEST once for TRAIN band 1.30-1.80.

Thresholds are FIT-quantiles of THIS pool (leak-safe). Results -> stage4e_combo_results.csv.
"""
from __future__ import annotations

import itertools
import json
import sys
import time
from pathlib import Path

import numpy as np
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

EXITS_A = [(1.2, 1.5)]
EXITS_B = [(1.0, 1.25), (1.2, 1.5), (1.5, 2.0)]
GUARDS_B = [None, {"top_n": 2}, {"top_n": 1}, {"max_slot": "12:30", "top_n": 2}]

SHORTLIST = [  # (feature, op, FIT-quantile)
    ("bb_pos", ">=", 0.30), ("macd_above_sig", ">=", 0.30), ("macd_above_sig", ">=", 0.70),
    ("cci_x", ">=", 0.30), ("ema20_dist_atr", ">=", 0.30), ("range_compress3", ">=", 0.30),
    ("pre3_vol_ratio", ">=", 0.70), ("day_ret_pct", ">=", 0.70),
    ("vol_ratio", ">=", 0.70), ("mfi_x", ">=", 0.30),
]


def metrics(cfg, df, full=False):
    tt.MAX_POSITIONS = int(cfg.get("max_positions") or 20)
    tt.DAILY_LOSS_RS = float(cfg.get("daily_loss_rs") or 0.0)
    fn = eng.full_metrics if full else eng.fast_metrics
    m = fn(SETUP, cfg, df)
    return {k: v for k, v in m.items() if k != "detail"}


def mk(sl, tgt, mask, guard=None):
    return {"sl": sl, "tgt": tgt, "mask_terms": mask, "premom_terms": [], "guard": guard,
            "status": "OK", "max_positions": 20, "daily_loss_rs": 0.0}


def main() -> int:
    pool_dir = sys.argv[1] if len(sys.argv) > 1 else str(WORK / "pools" / "pool_enriched_first_20bh")
    tt.POOL_DIRS = [str(Path(pool_dir).resolve())]
    tt.SLIPPAGE_BPS = 15.0
    tt._entry.cache_clear(); tt._resolve_full.cache_clear(); tt._premom.cache_clear()

    pool = tt.load_pool()
    pool = pool[pool["setup"].astype(str).str.upper().eq(SETUP)].copy()
    train = tt.attach_entries(pool[(pool["_day"] >= TRAIN_START) & (pool["_day"] <= TRAIN_END)].reset_index(drop=True))
    test = tt.attach_entries(pool[pool["_day"] >= TEST_START].reset_index(drop=True))
    trs = sorted(train["_day"].dt.strftime("%Y-%m-%d").unique())
    n_fit = max(1, int(round(FIT_FRAC * len(trs))))
    fit_s, val_s = trs[:n_fit], trs[n_fit:]
    fit = train[train["_day"].dt.strftime("%Y-%m-%d").isin(fit_s)].reset_index(drop=True)
    val = train[train["_day"].dt.strftime("%Y-%m-%d").isin(val_s)].reset_index(drop=True)

    def q(col, qq):
        x = pd.to_numeric(fit.get(col), errors="coerce").dropna()
        return round(float(x.quantile(qq)), 6) if len(x) else np.nan

    terms = []
    for f, op, qq in SHORTLIST:
        thr = q(f, qq)
        if np.isfinite(thr):
            terms.append((f"{f}{op}q{int(qq*100)}", (f, op, thr)))

    sl0, tg0 = EXITS_A[0]
    base = mk(sl0, tg0, [])
    bf, bv = metrics(base, fit), metrics(base, val)
    print(f"[4E] base {sl0}/{tg0}: FIT n={bf['n']} pf={bf['net_pf']} | VAL n={bv['n']} pf={bv['net_pf']}")

    rows = []
    # Stage A — singles
    singles = []
    for label, t in terms:
        cfg = mk(sl0, tg0, [t])
        mf, mv = metrics(cfg, fit), metrics(cfg, val)
        rows.append({"stage": "A", "label": label, "cfg": json.dumps(cfg["mask_terms"]),
                     "guard": "-", "sl": sl0, "tgt": tg0,
                     "fit_n": mf["n"], "fit_pf": mf["net_pf"], "val_n": mv["n"], "val_pf": mv["net_pf"]})
        if (mf["n"] >= 60 and mv["n"] >= 40
                and float(mf["net_pf"]) >= float(bf["net_pf"]) - 0.02
                and float(mv["net_pf"]) >= float(bv["net_pf"]) - 0.02):
            singles.append((label, t, float(mf["net_pf"]) + float(mv["net_pf"])))
    singles = sorted(singles, key=lambda x: -x[2])[:6]
    print(f"[4E] stage A survivors: {[s[0] for s in singles]}")

    # Stage B — pairs (and best singles) x exits x guards
    t0 = time.time()
    combos = [[s[1]] for s in singles] + [[a[1], b[1]] for a, b in itertools.combinations(singles, 2)]
    n_eval = 0
    band_members = []
    for mask, (sl, tgt), guard in itertools.product(combos, EXITS_B, GUARDS_B):
        cfg = mk(sl, tgt, list(mask), guard)
        mf = metrics(cfg, fit)
        if mf["n"] < 40 or float(mf["net_pf"]) < 1.05:
            rows.append({"stage": "B", "label": "|".join(m[0] for m in mask), "cfg": json.dumps(cfg["mask_terms"]),
                         "guard": json.dumps(guard), "sl": sl, "tgt": tgt,
                         "fit_n": mf["n"], "fit_pf": mf["net_pf"], "val_n": np.nan, "val_pf": np.nan})
            continue
        mv = metrics(cfg, val)
        rec = {"stage": "B", "label": "|".join(m[0] for m in mask), "cfg": json.dumps(cfg["mask_terms"]),
               "guard": json.dumps(guard), "sl": sl, "tgt": tgt,
               "fit_n": mf["n"], "fit_pf": mf["net_pf"], "val_n": mv["n"], "val_pf": mv["net_pf"]}
        n_eval += 1
        # Stage C — confirm + TEST once
        if mv["n"] >= 25 and float(mv["net_pf"]) >= 1.15 and float(mf["net_pf"]) >= 1.15:
            mtr = metrics(cfg, train, full=True)
            rec.update({"train_n": mtr["n"], "train_pf": mtr["net_pf"], "train_net": mtr["net_pnl"],
                        "train_dayp": mtr["day_block_p"], "train_avg_loss": mtr.get("avg_loss"),
                        "train_domT": mtr["trade_dom_gross"], "train_domD": mtr["day_dom"], "train_domS": mtr["sym_dom"]})
            if mtr["n"] >= 30 and PF_LO <= float(mtr["net_pf"]) <= PF_HI:
                mte = metrics(cfg, test, full=True)
                rec.update({"test_n": mte["n"], "test_pf": mte["net_pf"], "test_net": mte["net_pnl"],
                            "test_dayp": mte["day_block_p"], "test_avg_loss": mte.get("avg_loss"),
                            "test_domT": mte["trade_dom_gross"], "test_domD": mte["day_dom"], "test_domS": mte["sym_dom"],
                            "passes": bool(mte["n"] >= 10 and float(mte["net_pf"]) > TEST_PF_MIN and float(mte["net_pnl"]) > 0)})
                band_members.append(rec)
        rows.append(rec)

    df = pd.DataFrame(rows)
    out = WORK / "stage4e_combo_results.csv"
    df.to_csv(out, index=False)
    print(f"[4E] wrote {out} rows={len(df)} val-evaluated={n_eval} elapsed={time.time()-t0:.0f}s")

    if band_members:
        print("\n=== TRAIN-band members (TEST evaluated once) ===")
        bdf = pd.DataFrame(band_members)
        cols = [c for c in ("label", "guard", "sl", "tgt", "train_n", "train_pf", "test_n", "test_pf", "test_net", "passes") if c in bdf.columns]
        print(bdf[cols].sort_values("test_pf", ascending=False).to_string(index=False))
    else:
        got = df.dropna(subset=["val_pf"]) if "val_pf" in df.columns else pd.DataFrame()
        print("\n=== no TRAIN-band member; top VAL configs ===")
        if len(got):
            print(got.sort_values("val_pf", ascending=False).head(12)[
                ["label", "guard", "sl", "tgt", "fit_n", "fit_pf", "val_n", "val_pf"]].to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
