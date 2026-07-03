r"""stage3_sweeps.py — Stage-3 single-parameter sweeps for A_MOD_BREAK_C1_HIGH.

Leak-safe: thresholds are quantiles computed on FIT candidates only.
Every config is evaluated on FIT and VAL (fast metrics, 15bps). Results go to
stage3_sweep_results.csv; a digest of stable knobs prints at the end.

Groups swept: exits (SLxTGT surface on raw book), gate single-terms across
indicator / non-indicator / candle / time / crowding / regime / pre-momentum knobs.
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

BASE_EXIT = (0.70, 1.00)   # production exit for gate sweeps until exit sweep says otherwise


def fastm(cfg, df):
    tt.MAX_POSITIONS = int(cfg.get("max_positions") or 20)
    tt.DAILY_LOSS_RS = float(cfg.get("daily_loss_rs") or 0.0)
    m = eng.fast_metrics(SETUP, cfg, df)
    return m


def mk(sl, tgt, mask=None, guard=None, pm=None, maxpos=20, dloss=0.0):
    return {"sl": float(sl), "tgt": float(tgt), "mask_terms": mask or [],
            "premom_terms": pm or [], "guard": guard, "status": "OK",
            "max_positions": maxpos, "daily_loss_rs": dloss}


def main() -> int:
    pool_dir = sys.argv[1] if len(sys.argv) > 1 else str(WORK / "pools" / "pool_full")
    tt.POOL_DIRS = [str(Path(pool_dir).resolve())]
    tt.SLIPPAGE_BPS = 15.0
    tt._entry.cache_clear(); tt._resolve_full.cache_clear(); tt._premom.cache_clear()

    pool = tt.load_pool()
    pool = pool[pool["setup"].astype(str).str.upper().eq(SETUP)].copy()
    train_raw = pool[(pool["_day"] >= TRAIN_START) & (pool["_day"] <= TRAIN_END)].reset_index(drop=True)
    train = tt.attach_entries(train_raw)
    tr_sessions = sorted(train["_day"].dt.strftime("%Y-%m-%d").unique())
    n_fit = max(1, int(round(FIT_FRAC * len(tr_sessions))))
    fit_s, val_s = tr_sessions[:n_fit], tr_sessions[n_fit:]
    fit = train[train["_day"].dt.strftime("%Y-%m-%d").isin(fit_s)].reset_index(drop=True)
    val = train[train["_day"].dt.strftime("%Y-%m-%d").isin(val_s)].reset_index(drop=True)
    print(f"[sweep] FIT sessions={len(fit_s)} rows={len(fit)} | VAL sessions={len(val_s)} rows={len(val)}")

    # FIT-only quantiles for thresholds (leak-safe)
    def q(col, qq):
        x = pd.to_numeric(fit.get(col), errors="coerce").dropna()
        return round(float(x.quantile(qq)), 6) if len(x) else np.nan

    sweeps: list[tuple[str, str, dict]] = []   # (group, label, cfg)

    # --- Group E: exit surface on raw detector ---
    for sl, tgt in itertools.product([0.50, 0.70, 0.85, 1.00, 1.20, 1.50],
                                     [0.60, 0.80, 1.00, 1.25, 1.50, 2.00, 2.50]):
        sweeps.append(("exit", f"sl{sl}_tgt{tgt}", mk(sl, tgt)))

    sl0, tgt0 = BASE_EXIT

    # --- Group I: indicator single terms ---
    for v in [1.0, 2.0, 3.0, 4.0, 5.0]:
        sweeps.append(("rs_pct", f"rs>={v}", mk(sl0, tgt0, mask=[("rs_pct", ">=", v)])))
    for v in [0.004, 0.005, 0.006, 0.008]:
        sweeps.append(("atr_pct", f"atr<={v}", mk(sl0, tgt0, mask=[("atr_pct", "<=", v)])))
    for v in [0.004, 0.006]:
        sweeps.append(("atr_pct", f"atr>={v}", mk(sl0, tgt0, mask=[("atr_pct", ">=", v)])))
    for v in [1.8, 2.2, 2.6, 3.0]:
        sweeps.append(("vol_ratio", f"vol>={v}", mk(sl0, tgt0, mask=[("vol_ratio", ">=", v)])))
    for qq in [0.25, 0.5, 0.75]:
        sweeps.append(("rsi", f"rsi<=q{qq}", mk(sl0, tgt0, mask=[("rsi", "<=", q("rsi", qq))])))
        sweeps.append(("rsi", f"rsi>=q{qq}", mk(sl0, tgt0, mask=[("rsi", ">=", q("rsi", qq))])))
        sweeps.append(("adx", f"adx>=q{qq}", mk(sl0, tgt0, mask=[("adx", ">=", q("adx", qq))])))
        sweeps.append(("quality", f"qs>=q{qq}", mk(sl0, tgt0, mask=[("quality_score", ">=", q("quality_score", qq))])))
        sweeps.append(("ranker", f"rk>=q{qq}", mk(sl0, tgt0, mask=[("ranker_score", ">=", q("ranker_score", qq))])))
        sweeps.append(("macd", f"macdD>=q{qq}", mk(sl0, tgt0, mask=[("macd_hist_delta", ">=", q("macd_hist_delta", qq))])))
        sweeps.append(("ema20s", f"emaS>=q{qq}", mk(sl0, tgt0, mask=[("ema20_slope", ">=", q("ema20_slope", qq))])))
    # --- Group N: non-indicator price action ---
    for qq in [0.25, 0.5, 0.75]:
        sweeps.append(("body", f"body>=q{qq}", mk(sl0, tgt0, mask=[("body_pct", ">=", q("body_pct", qq))])))
        sweeps.append(("cloc", f"cloc>=q{qq}", mk(sl0, tgt0, mask=[("close_loc", ">=", q("close_loc", qq))])))
        sweeps.append(("uwick", f"uw<=q{qq}", mk(sl0, tgt0, mask=[("upper_wick_pct", "<=", q("upper_wick_pct", qq))])))
        sweeps.append(("srange", f"srng>=q{qq}", mk(sl0, tgt0, mask=[("signal_range_pct", ">=", q("signal_range_pct", qq))])))
        sweeps.append(("wskew", f"wsk<=q{qq}", mk(sl0, tgt0, mask=[("wick_skew_pct", "<=", q("wick_skew_pct", qq))])))
    for v in [1.0, 1.5, 2.0, 2.8]:
        sweeps.append(("vwapd", f"vd<={v}", mk(sl0, tgt0, mask=[("vwap_dist_atr", "<=", v)])))
    for v in [0.5, 1.0, 1.5]:
        sweeps.append(("vwapd", f"vd>={v}", mk(sl0, tgt0, mask=[("vwap_dist_atr", ">=", v)])))
    # market condition / regime
    sweeps.append(("regime", "reg==BULL", mk(sl0, tgt0, mask=[("regime", "==", "BULL")])))
    sweeps.append(("regime", "reg!=BEAR", mk(sl0, tgt0, mask=[("regime", "!=", "BEAR")])))
    for v in [0.0, 0.2]:
        sweeps.append(("mret", f"mret>={v}", mk(sl0, tgt0, mask=[("market_ret_pct", ">=", v)])))

    # --- Group T: time windows + crowding guards ---
    for mx in ["10:30", "11:05", "12:00", "12:30", "13:00", "14:30"]:
        sweeps.append(("maxslot", f"max{mx}", mk(sl0, tgt0, guard={"max_slot": mx})))
    for mn in ["09:45", "10:00", "10:30", "11:00"]:
        sweeps.append(("minslot", f"min{mn}", mk(sl0, tgt0, guard={"min_slot": mn})))
    for tn in [1, 2, 3]:
        sweeps.append(("topn", f"top{tn}", mk(sl0, tgt0, guard={"top_n": tn})))
    sweeps.append(("dloss", "dloss4000", mk(sl0, tgt0, dloss=4000.0)))
    sweeps.append(("maxpos", "maxpos10", mk(sl0, tgt0, maxpos=10)))

    # --- Group P: pre-momentum single terms (median split both directions) ---
    # build a pm feature frame from a FIT sample exactly the way the engine does
    pm_source = fit.sample(n=min(1000, len(fit)), random_state=7).sort_index()
    pm_rows = []
    for r in pm_source.itertuples():
        try:
            feats, _reason = tt._premom(r.ticker, r.side, r.tt_entry_iso, float(r.tt_fill), 0.90,
                                        r.tt_sig_ts.isoformat())
            if feats:
                pm_rows.append(feats)
        except Exception:
            continue
    pm_frame = pd.DataFrame([f if isinstance(f, dict) else {} for f in pm_rows])
    print(f"[sweep] premom sample rows: {len(pm_frame)} cols: {list(pm_frame.columns)[:10]}")
    for f in ["pre_entry_momentum_score", "sig5_adx_calc", "sig5_rsi_dir", "sig5_vol_ratio20",
              "pre1_adx", "pre3_range_r", "pre5_mom_r", "pre3_close_pos"]:
        if f not in pm_frame.columns:
            continue
        med = pd.to_numeric(pm_frame[f], errors="coerce").dropna()
        if len(med) < 50:
            continue
        thr = round(float(med.quantile(0.5)), 6)
        for d in (">=", "<="):
            sweeps.append(("premom", f"{f}{d}{thr}", mk(sl0, tgt0, pm=[(f, d, thr)])))

    rows = []
    t0 = time.time()
    for i, (grp, label, cfg) in enumerate(sweeps, 1):
        mf = fastm(cfg, fit)
        mv = fastm(cfg, val)
        rows.append({"group": grp, "label": label,
                     "cfg": json.dumps({k: cfg[k] for k in ("sl", "tgt", "mask_terms", "premom_terms", "guard", "max_positions", "daily_loss_rs")}, default=str),
                     "fit_n": mf["n"], "fit_pf": mf["net_pf"], "fit_net": mf["net_pnl"],
                     "val_n": mv["n"], "val_pf": mv["net_pf"], "val_net": mv["net_pnl"]})
        if i % 25 == 0:
            print(f"[sweep] {i}/{len(sweeps)} elapsed={time.time()-t0:.0f}s", flush=True)

    df = pd.DataFrame(rows)
    out = WORK / "stage3_sweep_results.csv"
    df.to_csv(out, index=False)
    print(f"[sweep] wrote {out} ({len(df)} configs)")

    print("\n=== exit surface top-10 by FIT PF (n>=200) ===")
    ex = df[(df.group == "exit") & (df.fit_n >= 200)].sort_values("fit_pf", ascending=False)
    print(ex.head(10)[["label", "fit_n", "fit_pf", "val_n", "val_pf"]].to_string(index=False))

    print("\n=== gate terms: FIT-improving AND VAL-holding (vs raw base) ===")
    base_fit = df[(df.group == "exit") & (df.label == f"sl{BASE_EXIT[0]}_tgt{BASE_EXIT[1]}")]
    bf = float(base_fit["fit_pf"].iloc[0]) if len(base_fit) else np.nan
    bv = float(base_fit["val_pf"].iloc[0]) if len(base_fit) else np.nan
    g = df[~df.group.isin(["exit"])].copy()
    g["fit_lift"] = g["fit_pf"] - bf
    g["val_lift"] = g["val_pf"] - bv
    keep = g[(g.fit_lift > 0.05) & (g.val_lift > 0.0) & (g.fit_n >= 60)].sort_values("fit_lift", ascending=False)
    print(keep.head(30)[["group", "label", "fit_n", "fit_pf", "val_n", "val_pf", "fit_lift", "val_lift"]].to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
