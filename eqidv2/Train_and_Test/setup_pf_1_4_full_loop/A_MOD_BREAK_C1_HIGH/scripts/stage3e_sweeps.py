r"""stage3e_sweeps.py — Stage-3E sweeps over the ENRICHED feature set (FIT/VAL, leak-safe).

Single-term gates on every recomputed indicator / pre-momentum / structural feature,
at FIT-quantile thresholds (q30/q70) in both directions, evaluated at the least-bad exit
(SL 1.2 / Tgt 1.5) + binary/structural terms. Writes stage3e_sweep_results.csv.

Usage: py -3.12 stage3e_sweeps.py [pool_dir] [--exit sl,tgt]
"""
from __future__ import annotations

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
FIT_FRAC = 0.60
SL0, TGT0 = 1.2, 1.5

NUMERIC_FEATS = [
    # indicators
    "rsi_x", "rsi_slope3", "adx_x", "cci_x", "mfi_x", "stoch_k", "stoch_cross",
    "macd_hist_x", "macd_hist_delta3", "macd_above_sig",
    "ema20_dist_atr", "ema50_dist_atr", "ema20_slope5_atr",
    "bb_pos", "bb_width_pct", "obv_slope5_norm",
    # pre-momentum
    "pre1_ret_atr", "pre3_ret_atr", "pre5_ret_atr", "green_streak_pre",
    "pre3_vol_ratio", "range_compress3", "pre_rsi", "vwap_hold_bars",
    # structural
    "break_margin_atr", "dist_20bar_high_atr", "or_high_dist_atr",
    "pdh_dist_atr", "gap_pct", "day_ret_pct", "day_range_pos",
    "upmove_from_daylow_atr", "bar_of_day", "price_level", "notional_5m_rs",
]
BINARY_TERMS = [
    ("is_20bar_high", ">=", 1.0), ("is_20bar_high", "<=", 0.0),
    ("above_or_high", ">=", 1.0), ("above_or_high", "<=", 0.0),
    ("above_pdh", ">=", 1.0), ("above_pdh", "<=", 0.0),
    ("ema_stack", ">=", 2.0), ("ema_stack", "<=", 1.0),
    ("dow", "<=", 1.0), ("dow", ">=", 3.0),
]


def fastm(cfg, df):
    tt.MAX_POSITIONS = int(cfg.get("max_positions") or 20)
    tt.DAILY_LOSS_RS = float(cfg.get("daily_loss_rs") or 0.0)
    return eng.fast_metrics(SETUP, cfg, df)


def mk(mask):
    return {"sl": SL0, "tgt": TGT0, "mask_terms": mask, "premom_terms": [], "guard": None,
            "status": "OK", "max_positions": 20, "daily_loss_rs": 0.0}


def main() -> int:
    pool_dir = sys.argv[1] if len(sys.argv) > 1 else str(WORK / "pools" / "pool_enriched")
    tt.POOL_DIRS = [str(Path(pool_dir).resolve())]
    tt.SLIPPAGE_BPS = 15.0
    tt._entry.cache_clear(); tt._resolve_full.cache_clear(); tt._premom.cache_clear()

    pool = tt.load_pool()
    pool = pool[pool["setup"].astype(str).str.upper().eq(SETUP)].copy()
    train_raw = pool[(pool["_day"] >= TRAIN_START) & (pool["_day"] <= TRAIN_END)].reset_index(drop=True)
    train = tt.attach_entries(train_raw)
    trs = sorted(train["_day"].dt.strftime("%Y-%m-%d").unique())
    n_fit = max(1, int(round(FIT_FRAC * len(trs))))
    fit_s, val_s = trs[:n_fit], trs[n_fit:]
    fit = train[train["_day"].dt.strftime("%Y-%m-%d").isin(fit_s)].reset_index(drop=True)
    val = train[train["_day"].dt.strftime("%Y-%m-%d").isin(val_s)].reset_index(drop=True)
    print(f"[3E] pool={Path(pool_dir).name} FIT {len(fit_s)}s/{len(fit)}r VAL {len(val_s)}s/{len(val)}r")

    avail = [f for f in NUMERIC_FEATS if f in fit.columns and pd.to_numeric(fit[f], errors="coerce").notna().mean() > 0.5]
    missing = [f for f in NUMERIC_FEATS if f not in avail]
    print(f"[3E] usable numeric feats: {len(avail)} | dropped (coverage<50% or absent): {missing}")

    sweeps = []
    for f in avail:
        x = pd.to_numeric(fit[f], errors="coerce").dropna()
        for qq in (0.30, 0.70):
            thr = round(float(x.quantile(qq)), 6)
            for d in (">=", "<="):
                sweeps.append((f, f"{f}{d}q{int(qq*100)}({thr})", mk([(f, d, thr)])))
    for f, d, v in BINARY_TERMS:
        if f in fit.columns:
            sweeps.append((f, f"{f}{d}{v}", mk([(f, d, v)])))

    base = mk([])
    bf = fastm(base, fit); bv = fastm(base, val)
    print(f"[3E] base @ {SL0}/{TGT0}: FIT n={bf['n']} pf={bf['net_pf']} | VAL n={bv['n']} pf={bv['net_pf']}")

    rows = []
    t0 = time.time()
    for i, (feat, label, cfg) in enumerate(sweeps, 1):
        mf = fastm(cfg, fit); mv = fastm(cfg, val)
        rows.append({"feature": feat, "label": label,
                     "cfg": json.dumps(cfg["mask_terms"]),
                     "fit_n": mf["n"], "fit_pf": mf["net_pf"], "fit_net": mf["net_pnl"],
                     "val_n": mv["n"], "val_pf": mv["net_pf"], "val_net": mv["net_pnl"],
                     "fit_lift": round(float(mf["net_pf"]) - float(bf["net_pf"]), 3) if np.isfinite(mf["net_pf"]) else np.nan,
                     "val_lift": round(float(mv["net_pf"]) - float(bv["net_pf"]), 3) if np.isfinite(mv["net_pf"]) else np.nan})
        if i % 30 == 0:
            print(f"[3E] {i}/{len(sweeps)} elapsed={time.time()-t0:.0f}s", flush=True)

    df = pd.DataFrame(rows)
    out = WORK / f"stage3e_sweep_results_{Path(pool_dir).name}.csv"
    df.to_csv(out, index=False)
    print(f"[3E] wrote {out}")

    keep = df[(df.fit_lift > 0.05) & (df.val_lift > 0.0) & (df.fit_n >= 60) & (df.val_n >= 40)]
    print("\n=== FIT-improving AND VAL-holding terms ===")
    print(keep.sort_values("fit_lift", ascending=False).head(25)[
        ["label", "fit_n", "fit_pf", "val_n", "val_pf", "fit_lift", "val_lift"]].to_string(index=False)
        if len(keep) else "NONE")
    print("\n=== top 10 by VAL PF (n>=40) regardless of lift ===")
    print(df[df.val_n >= 40].sort_values("val_pf", ascending=False).head(10)[
        ["label", "fit_n", "fit_pf", "val_n", "val_pf"]].to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
