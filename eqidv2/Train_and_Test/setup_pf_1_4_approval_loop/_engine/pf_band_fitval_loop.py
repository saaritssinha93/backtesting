r"""pf_band_fitval_loop.py — research-only PF-BAND approval loop for ONE setup.
============================================================================
Shared engine for the "TRAIN PF 1.30-1.70 / TEST PF>1.40" approval campaign.
Lives under Train_and_Test/ (research-only). It NEVER writes final_setup_conf.py
and performs NO live execution — backtest/replay only.

What it does
------------
1. Loads the setup's per-setup pool (pre_dedupe_live_candidates) via
   setup_train_test.load_pool() — reuses the repo's entry/exit/cost/dedupe/
   mask/pre-momentum/portfolio-overlay pipeline (single source of truth).
2. Session split (calendar-aware, prints exact sessions before running):
     TEST  = completed sessions on/after --test_start (default 2026-06-20).
             If fewer than --min_test_sessions exist, FALL BACK to the last
             --n_test_fallback available sessions and log the adjustment.
     TRAIN = completed sessions in [--train_start (default 2026-05-18), firstTEST)
     FIT   = first half of TRAIN ;  VAL = second half of TRAIN
3. Optimises ONLY on FIT/VAL with a BAND objective that rewards PF climbing
   toward 1.70 and PENALISES overshoot above 1.70 (anti-overfit), plus a
   FIT/VAL gap penalty.  Optuna TPE if available, else seeded random search.
4. Confirms the single best FIT/VAL config on full TRAIN, then scores TEST ONCE
   (no further tuning).  Acceptance = TRAIN PF in [1.30,1.70] AND TEST PF>1.40
   AND meaningful trades AND no single trade/day/symbol dominates.
5. Writes the approval-loop artifacts into
   Train_and_Test/setup_pf_1_4_approval_loop/<SETUP>/ :
     BASELINE_RESULT.md, ITERATION_LOG.md, FAILURE_ANALYSIS.md,
     CANDIDATE_CONFIGS.md, APPROVAL_REQUIRED_FINAL_RECOMMENDATION.md,
     candidates/<SETUP>_candidate_001.json (only if it passes), trials.csv,
     best_config.json, run_summary.json, equity_*.png.

Run from repo root:
  py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py \
     --setup L_PRESSURE_BURST_VWAP \
     --pool C:\TradingData\eqidv2\setup_pools_2026_06_29\L_PRESSURE_BURST_VWAP \
     --trials 400 --time_budget_min 18 --seed 7
"""
from __future__ import annotations

import argparse
import json
import random
import sys
import time
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
_TT_DIR = _HERE.parent.parent          # Train_and_Test/
_REPO = _TT_DIR.parent                 # repo root
for _p in (str(_REPO), str(_TT_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import setup_train_test as tt  # noqa: E402

try:
    import optuna
    optuna.logging.set_verbosity(optuna.logging.WARNING)
    _HAVE_OPTUNA = True
except Exception:
    _HAVE_OPTUNA = False

# ---- search space: ONLY repo-supported knobs --------------------------------
MASK_FEATS = ["rs_pct", "vol_ratio", "atr_pct", "body_pct", "close_loc", "vwap_dist_atr",
              "quality_score", "ranker_score", "signal_range_pct", "upper_wick_pct",
              "lower_wick_pct", "wick_skew_pct", "stock_ret_pct", "rs_rank",
              "breadth_above_vwap", "breadth_pos_ret", "vwap_slope_atr",
              "breakout_strength_atr", "orh_dist_atr", "pdh_dist_atr",
              "prev20_dist_atr", "retest_depth_atr",
              "pullback_from_breakout_high_atr", "retest_close_reclaim_atr",
              "retest_max_high_since_breakout_atr", "breakout_age_bars"]
PM_FEATS = ["pre_entry_momentum_score", "sig5_adx_calc", "sig5_rsi_dir", "sig5_vol_ratio20",
            "pre1_adx", "pre3_range_r", "pre5_mom_r", "pre3_close_pos"]
QGRID = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
# Wider exit grids than the generic loop — explore small/large SL x small/large target.
SL_GRID = [0.50, 0.70, 0.85, 1.00, 1.10, 1.20, 1.50]
TGT_GRID = [0.60, 0.80, 1.00, 1.25, 1.50, 2.00, 2.50]
MIN_SLOTS = ["09:30", "09:45", "10:00", "10:30", "11:00"]
MAX_SLOTS = ["12:00", "12:30", "13:00", "14:00", "14:30"]
MAXPOS_GRID = [10, 20]
DLOSS_GRID = [0.0, 4000.0]

PF_LO, PF_HI = 1.30, 1.70      # TRAIN band
TEST_PF_MIN = 1.30             # robust TEST gate: PF plus day-block significance
TEST_DAY_BLOCK_P_MAX = 0.10
TRAIN_TARGET_RATE_MIN = 12.0   # reject targets that almost never fill in TRAIN
NEIGHBORHOOD_PF_MIN = 1.15     # threshold +/- 1 quantile step must stay alive
DROPOUT_PF_MIN = 1.00          # dropping one term must not turn the book into a loser
DEFAULT_GAP_LAMBDA = 0.80


def _clamp_pf(pf):
    return 10.0 if not np.isfinite(pf) else min(float(pf), 10.0)


def _set_slippage(s):
    tt.SLIPPAGE_BPS = float(s)
    tt._entry.cache_clear(); tt._resolve_full.cache_clear(); tt._premom.cache_clear()


def _eval_fast(setup, cfg, df):
    fam = tt.eval_family({setup: cfg}, df)
    book = fam.get("book")
    n_days = int(pd.Series(book["_day"]).nunique()) if (book is not None and len(book)) else 0
    return int(fam["trades"]), float(fam["net_pf"]), n_days


def full_metrics(setup, cfg, df):
    """Full per-window metric set via per-trade detail."""
    fam = tt.eval_family({setup: cfg}, df)
    exits = {setup: (cfg["sl"], cfg["tgt"])}
    det = tt.book_detail(fam["book"], exits) if fam["trades"] else pd.DataFrame()
    m = {"n": int(fam["trades"]), "net_pf": round(float(fam["net_pf"]), 3),
         "net_pnl": round(float(fam["net_pnl"]), 0),
         "day_block_p": (None if not np.isfinite(fam["day_block_p"]) else round(float(fam["day_block_p"]), 4)),
         "wins": 0, "losses": 0, "win_rate": 0.0, "gross_profit": 0.0, "gross_loss": 0.0,
         "avg_win": 0.0, "avg_loss": 0.0, "max_dd": 0.0, "n_days": 0, "n_syms": 0,
         "trades_per_day": 0.0, "sl_cnt": 0, "tgt_cnt": 0, "eod_cnt": 0, "other_cnt": 0,
         "target_rate": 0.0,
         "trade_dom_gross": None, "day_dom": None, "sym_dom": None,
         "top_day": None, "top_sym": None, "detail": det}
    if det.empty:
        return m
    net = det["net_pnl_rs"].to_numpy()
    tot = float(net.sum())
    wins = net[net > 0]; losses = net[net < 0]
    gp = float(wins.sum()); gl = float(-losses.sum())
    oc = det["outcome"].astype(str).str.upper()
    eq = net.cumsum(); dd = eq - np.maximum.accumulate(eq)
    m.update({
        "wins": int((net > 0).sum()), "losses": int((net < 0).sum()),
        "win_rate": round(float((net > 0).mean()) * 100, 1),
        "gross_profit": round(gp, 0), "gross_loss": round(gl, 0),
        "avg_win": round(float(wins.mean()), 0) if len(wins) else 0.0,
        "avg_loss": round(float(losses.mean()), 0) if len(losses) else 0.0,
        "max_dd": round(float(dd.min()), 0) if len(dd) else 0.0,
        "n_days": int(det["trade_date"].nunique()), "n_syms": int(det["ticker"].nunique()),
        "sl_cnt": int((oc == "SL").sum()), "tgt_cnt": int((oc == "TARGET").sum()),
        "eod_cnt": int((oc == "EOD").sum()),
        "other_cnt": int((~oc.isin(["SL", "TARGET", "EOD"])).sum()),
    })
    m["target_rate"] = round(float((oc == "TARGET").mean()) * 100, 1)
    m["trades_per_day"] = round(m["n"] / max(1, m["n_days"]), 2)
    m["trade_dom_gross"] = round(float(net.max()) / gp, 3) if gp > 0 else 9.99
    day_sum = det.groupby("trade_date")["net_pnl_rs"].sum()
    sym_sum = det.groupby("ticker")["net_pnl_rs"].sum()
    if tot > 0:
        m["day_dom"] = round(float(day_sum.max()) / tot, 3)
        m["sym_dom"] = round(float(sym_sum.max()) / tot, 3)
    else:
        m["day_dom"] = m["sym_dom"] = 9.99
    m["top_day"] = f"{day_sum.idxmax()}: Rs{day_sum.max():,.0f}"
    m["top_sym"] = f"{sym_sum.idxmax()}: Rs{sym_sum.max():,.0f}"
    return m


def fast_metrics(setup, cfg, df):
    """Fast metric pack from eval_family only, without per-trade outcome detail."""
    fam = tt.eval_family({setup: cfg}, df)
    book = fam.get("book")
    net = np.asarray(fam.get("net", np.array([])), dtype=float)
    finite = np.isfinite(net)
    netf = net[finite]
    gp = float(netf[netf > 0].sum()) if len(netf) else 0.0
    gl = float(-netf[netf < 0].sum()) if len(netf) else 0.0
    eq = netf.cumsum() if len(netf) else np.array([])
    dd = eq - np.maximum.accumulate(eq) if len(eq) else np.array([])
    n_days = n_syms = 0
    day_dom = sym_dom = 9.99
    top_day = top_sym = None
    if book is not None and len(book) and len(net) == len(book):
        b = book.loc[finite].copy()
        n_days = int(b["_day"].nunique()) if "_day" in b.columns else 0
        n_syms = int(b["ticker"].nunique()) if "ticker" in b.columns else 0
        if len(b) and len(netf):
            day_sum = pd.Series(netf, index=b["_day"].to_numpy()).groupby(level=0).sum()
            sym_sum = pd.Series(netf, index=b["ticker"].to_numpy()).groupby(level=0).sum()
            total = float(netf.sum())
            if total > 0:
                day_dom = round(float(day_sum.max()) / total, 3)
                sym_dom = round(float(sym_sum.max()) / total, 3)
            top_day = f"{day_sum.idxmax()}: Rs{day_sum.max():,.0f}"
            top_sym = f"{sym_sum.idxmax()}: Rs{sym_sum.max():,.0f}"
    return {
        "n": int(fam["trades"]),
        "net_pf": round(float(fam["net_pf"]), 3),
        "net_pnl": round(float(fam["net_pnl"]), 0),
        "day_block_p": (None if not np.isfinite(fam["day_block_p"]) else round(float(fam["day_block_p"]), 4)),
        "wins": int((netf > 0).sum()),
        "losses": int((netf < 0).sum()),
        "win_rate": round(float((netf > 0).mean()) * 100, 1) if len(netf) else 0.0,
        "gross_profit": round(gp, 0),
        "gross_loss": round(gl, 0),
        "avg_win": round(float(netf[netf > 0].mean()), 0) if (netf > 0).any() else 0.0,
        "avg_loss": round(float(netf[netf < 0].mean()), 0) if (netf < 0).any() else 0.0,
        "max_dd": round(float(dd.min()), 0) if len(dd) else 0.0,
        "n_days": n_days,
        "n_syms": n_syms,
        "trades_per_day": round(int(fam["trades"]) / max(1, n_days), 2),
        "sl_cnt": 0,
        "tgt_cnt": 0,
        "eod_cnt": 0,
        "other_cnt": 0,
        "target_rate": 0.0,
        "trade_dom_gross": round(float(netf.max()) / gp, 3) if gp > 0 and len(netf) else 9.99,
        "day_dom": day_dom,
        "sym_dom": sym_dom,
        "top_day": top_day,
        "top_sym": top_sym,
        "detail": pd.DataFrame(),
        "detail_mode": "fast_no_outcomes",
    }


def _suggest(trial, mask_quant, pm_quant, max_mask, max_pm):
    def cat(n, ch): return trial.suggest_categorical(n, ch)
    def integ(n, lo, hi): return trial.suggest_int(n, lo, hi)
    mask_terms = []
    for i in range(integ("n_mask", 0, max_mask)):
        f = cat(f"mask{i}_feat", [x for x in MASK_FEATS if x in mask_quant])
        op = cat(f"mask{i}_op", [">=", "<="]); q = cat(f"mask{i}_q", QGRID)
        mask_terms.append((f, op, round(float(mask_quant[f][q]), 6)))
    premom_terms = []
    for i in range(integ("n_pm", 0, max_pm)):
        f = cat(f"pm{i}_feat", [x for x in PM_FEATS if x in pm_quant])
        op = cat(f"pm{i}_op", [">=", "<="]); q = cat(f"pm{i}_q", QGRID)
        premom_terms.append((f, op, round(float(pm_quant[f][q]), 6)))
    guard = {}
    if cat("use_min_slot", [False, True]):
        guard["min_slot"] = cat("min_slot", MIN_SLOTS)
    if cat("use_max_slot", [False, True]):
        guard["max_slot"] = cat("max_slot", MAX_SLOTS)
    tn = cat("top_n", [0, 1, 2, 3])
    if tn:
        guard["top_n"] = int(tn)
    return {"sl": float(cat("sl", SL_GRID)), "tgt": float(cat("tgt", TGT_GRID)),
            "mask_terms": mask_terms, "premom_terms": premom_terms, "guard": (guard or None),
            "status": "OK", "max_positions": int(cat("max_positions", MAXPOS_GRID)),
            "daily_loss_rs": float(cat("daily_loss_rs", DLOSS_GRID))}


class _RandTrial:
    def __init__(self, rng): self.rng = rng; self.params = {}
    def suggest_categorical(self, n, ch):
        v = ch[self.rng.randrange(len(ch))]; self.params[n] = v; return v
    def suggest_int(self, n, lo, hi):
        v = self.rng.randint(lo, hi); self.params[n] = v; return v
    def set_user_attr(self, *a, **k): pass


def band_reward(pf):
    """Tent peaking at PF_HI=1.70; linear climb below, 1.5x penalty for overshoot."""
    if pf <= PF_HI:
        return pf
    return PF_HI - 1.5 * (pf - PF_HI)


def cfg_to_conf_block(setup, side, cfg):
    return {
        "side": side,
        "exit": {"sl_pct": cfg["sl"], "tgt_pct": cfg["tgt"]},
        "mask_terms": [list(t) for t in cfg["mask_terms"]],
        "pre_momentum_terms": [list(t) for t in cfg["premom_terms"]],
        "entry_guards": cfg["guard"] or {},
        "max_positions": cfg.get("max_positions"),
        "daily_loss_rs": cfg.get("daily_loss_rs"),
    }


def conf_to_cfg(block):
    """Convert a final_setup_conf entry -> eval cfg (baseline)."""
    ex = block.get("exit", {}) or {}
    g = dict(block.get("entry_guards", {}) or {})
    guard = {}
    if "min_slot" in g: guard["min_slot"] = g["min_slot"]
    if "max_slot" in g: guard["max_slot"] = g["max_slot"]
    if g.get("top_n"): guard["top_n"] = int(g["top_n"])
    return {"sl": float(ex.get("sl_pct", 0.70)), "tgt": float(ex.get("tgt_pct", 1.25)),
            "mask_terms": [tuple(t) for t in block.get("mask_terms", []) or []],
            "premom_terms": [tuple(t) for t in block.get("pre_momentum_terms", []) or []],
            "guard": (guard or None), "status": "OK",
            "max_positions": int(block.get("max_positions") or 20),
            "daily_loss_rs": float(block.get("daily_loss_rs") or 0.0)}


def get_baseline_block(setup):
    try:
        import final_setup_conf as fsc
        if setup in fsc.FINAL_SETUP_CONF:
            return fsc.FINAL_SETUP_CONF[setup], "FINAL_SETUP_CONF (active)"
        if setup in fsc.RESEARCH_WATCH_CONF:
            return fsc.RESEARCH_WATCH_CONF[setup], "RESEARCH_WATCH_CONF (disabled)"
    except Exception as e:
        print(f"[baseline] could not import final_setup_conf: {e}")
    return None, "none (no conf entry)"


def _metric_view(setup, cfg, df):
    n, pf, n_days = _eval_fast(setup, cfg, df)
    return {"n": int(n), "pf": round(float(pf), 4), "n_days": int(n_days)}


def _finite_le(value, limit):
    return value is not None and np.isfinite(float(value)) and float(value) <= float(limit)


def _copy_cfg(cfg):
    out = dict(cfg)
    out["mask_terms"] = [tuple(t) for t in cfg.get("mask_terms", []) or []]
    out["premom_terms"] = [tuple(t) for t in cfg.get("premom_terms", []) or []]
    out["guard"] = (dict(cfg["guard"]) if cfg.get("guard") else None)
    return out


def _neighbor_thresholds(feat, thr, quant):
    if feat not in quant:
        return []
    qs = sorted(quant[feat])
    vals = [float(quant[feat][q]) for q in qs]
    if not vals:
        return []
    i = min(range(len(vals)), key=lambda k: abs(vals[k] - float(thr)))
    out = []
    for j in (i - 1, i + 1):
        if 0 <= j < len(vals):
            out.append((qs[j], round(vals[j], 6)))
    return out


def robustness_report(setup, cfg, train_df, mask_quant, pm_quant, args):
    """Check whether the selected config is a stable pocket or a knife-edge.

    - Neighborhood: each numeric threshold is shifted one TRAIN quantile step up/down.
    - Dropout: each individual mask/premom term is removed once.
    """
    base_cfg = _copy_cfg(cfg)
    tt.MAX_POSITIONS = base_cfg.get("max_positions", 20)
    tt.DAILY_LOSS_RS = base_cfg.get("daily_loss_rs", 0.0)
    base = _metric_view(setup, base_cfg, train_df)
    checks = []
    terms = list(base_cfg.get("mask_terms", []) or [])
    pm_terms = list(base_cfg.get("premom_terms", []) or [])

    for kind, idx, term in (
        [("mask", i, t) for i, t in enumerate(terms)]
        + [("premom", i, t) for i, t in enumerate(pm_terms)]
    ):
        feat, op, thr = term
        quant = mask_quant if kind == "mask" else pm_quant
        for q, new_thr in _neighbor_thresholds(feat, thr, quant):
            variant = _copy_cfg(base_cfg)
            key = "mask_terms" if kind == "mask" else "premom_terms"
            vt = list(variant[key])
            vt[idx] = (feat, op, new_thr)
            variant[key] = vt
            m = _metric_view(setup, variant, train_df)
            ok = m["n"] >= args.min_trades_train and m["pf"] >= args.neighborhood_pf_min
            checks.append({
                "check": "neighbor",
                "kind": kind,
                "term": f"{feat}{op}{thr}",
                "variant": f"{feat}{op}{new_thr} (q={q})",
                "n": m["n"],
                "pf": m["pf"],
                "pass": bool(ok),
            })

    dropouts = []
    for kind, idx, term in (
        [("mask", i, t) for i, t in enumerate(terms)]
        + [("premom", i, t) for i, t in enumerate(pm_terms)]
    ):
        variant = _copy_cfg(base_cfg)
        key = "mask_terms" if kind == "mask" else "premom_terms"
        vt = list(variant[key])
        removed = vt.pop(idx)
        variant[key] = vt
        m = _metric_view(setup, variant, train_df)
        ok = m["n"] >= args.min_trades_train and m["pf"] >= args.dropout_pf_min
        dropouts.append({
            "check": "dropout",
            "kind": kind,
            "term": f"{removed[0]}{removed[1]}{removed[2]}",
            "variant": "term removed",
            "n": m["n"],
            "pf": m["pf"],
            "pass": bool(ok),
        })

    neighbor_pass = all(c["pass"] for c in checks) if checks else True
    dropout_pass = all(c["pass"] for c in dropouts) if dropouts else True
    return {
        "base": base,
        "neighborhood_pf_min": args.neighborhood_pf_min,
        "dropout_pf_min": args.dropout_pf_min,
        "neighbor_checks": checks,
        "dropout_checks": dropouts,
        "neighbor_pass": bool(neighbor_pass),
        "dropout_pass": bool(dropout_pass),
        "passed": bool(neighbor_pass and dropout_pass),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--setup", required=True)
    ap.add_argument("--pool", required=True)
    ap.add_argument("--trials", type=int, default=400)
    ap.add_argument("--time_budget_min", type=float, default=18.0)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--train_start", default="2026-05-18")
    ap.add_argument("--test_start", default="2026-06-20")
    ap.add_argument("--min_test_sessions", type=int, default=4)
    ap.add_argument("--n_test_fallback", type=int, default=5)
    ap.add_argument("--min_trades_train", type=int, default=20)
    ap.add_argument("--min_trades_test", type=int, default=6)
    ap.add_argument("--dom_cap", type=float, default=0.50)
    ap.add_argument("--max_trades_day", type=float, default=6.0)
    ap.add_argument("--gap_lambda", type=float, default=DEFAULT_GAP_LAMBDA)
    ap.add_argument("--fv_floor", type=int, default=6)
    ap.add_argument("--search_slippage_bps", type=float, default=15.0)
    ap.add_argument("--max_mask_terms", type=int, default=1)
    ap.add_argument("--max_pm_terms", type=int, default=1)
    ap.add_argument("--test_pf_min", type=float, default=TEST_PF_MIN)
    ap.add_argument("--max_test_day_block_p", type=float, default=TEST_DAY_BLOCK_P_MAX)
    ap.add_argument("--min_train_target_rate", type=float, default=TRAIN_TARGET_RATE_MIN)
    ap.add_argument("--neighborhood_pf_min", type=float, default=NEIGHBORHOOD_PF_MIN)
    ap.add_argument("--dropout_pf_min", type=float, default=DROPOUT_PF_MIN)
    ap.add_argument("--pm_quantile_sample", type=int, default=1500,
                    help="0 means use all TRAIN rows for premom quantiles; sample only affects search grid construction")
    ap.add_argument("--out", default=str(_HERE.parent))
    args = ap.parse_args()
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass

    setup = args.setup.strip().upper()
    outdir = Path(args.out) / setup
    (outdir / "candidates").mkdir(parents=True, exist_ok=True)

    engine = "Optuna TPE" if _HAVE_OPTUNA else "Optuna unavailable; using seeded random search fallback."
    print(f"[pf-band] setup={setup}")
    print(f"[pf-band] optimizer: {engine}")

    tt.POOL_DIRS = [Path(args.pool)]; tt.POOL_DIR = Path(args.pool)
    pool = tt.load_pool()
    pool = pool[pool["setup"] == setup].copy()
    side = (pool["side"].mode().iloc[0] if len(pool) else "LONG")
    sessions = sorted(pd.Series(pool["_day"].dropna().unique()))
    if len(sessions) < 12:
        print(f"[pf-band] INSUFFICIENT sessions ({len(sessions)}); abort.")
        return 0

    ts_dt = pd.Timestamp(args.test_start)
    tr_dt = pd.Timestamp(args.train_start)
    test_cal = [s for s in sessions if s >= ts_dt]
    split_note = ""
    if len(test_cal) >= args.min_test_sessions:
        TEST_s = test_cal
        split_note = (f"TEST = calendar sessions >= {args.test_start} ({len(TEST_s)} sessions).")
    else:
        TEST_s = sessions[-args.n_test_fallback:]
        split_note = (f"calendar TEST (>= {args.test_start}) had only {len(test_cal)} session(s) "
                      f"-> FELL BACK to the last {args.n_test_fallback} available sessions.")
    first_test = TEST_s[0]
    TRAIN_s = [s for s in sessions if (s >= tr_dt and s < first_test)]
    if len(TRAIN_s) < args.min_trades_train // 4:
        print(f"[pf-band] TRAIN too small ({len(TRAIN_s)} sessions from {args.train_start}); abort.")
        return 0
    half = len(TRAIN_s) // 2
    FIT_s, VAL_s = TRAIN_s[:half], TRAIN_s[half:]

    def _rng(ss):
        return f"{pd.Timestamp(ss[0]).date()}..{pd.Timestamp(ss[-1]).date()}" if ss else "(empty)"
    print(f"[pf-band] split note: {split_note}")
    print(f"[pf-band] FIT   {_rng(FIT_s)} ({len(FIT_s)})  | {[str(pd.Timestamp(s).date()) for s in FIT_s]}")
    print(f"[pf-band] VAL   {_rng(VAL_s)} ({len(VAL_s)})  | {[str(pd.Timestamp(s).date()) for s in VAL_s]}")
    print(f"[pf-band] TRAIN {_rng(TRAIN_s)} ({len(TRAIN_s)})")
    print(f"[pf-band] TEST  {_rng(TEST_s)} ({len(TEST_s)})  | {[str(pd.Timestamp(s).date()) for s in TEST_s]}")

    span = set(map(pd.Timestamp, FIT_s + VAL_s + TEST_s))
    sub = pool[pool["_day"].isin(span)].copy()
    _set_slippage(args.search_slippage_bps)
    sub = tt.attach_entries(sub)

    def _slice(ss):
        return sub[sub["_day"].isin(set(map(pd.Timestamp, ss)))].copy()
    FIT, VAL, TRAIN, TEST = _slice(FIT_s), _slice(VAL_s), _slice(FIT_s + VAL_s), _slice(TEST_s)
    print(f"[pf-band] raw entries @ {args.search_slippage_bps}bps: FIT={len(FIT)} VAL={len(VAL)} "
          f"TRAIN={len(TRAIN)} TEST={len(TEST)}")

    # quantile grids from TRAIN only (never TEST)
    mask_quant = {}
    for f in MASK_FEATS:
        if f in TRAIN.columns:
            s = pd.to_numeric(TRAIN[f], errors="coerce").dropna()
            if len(s) >= 8 and s.nunique() > 1:
                mask_quant[f] = {q: float(s.quantile(q)) for q in QGRID}
    pm_source = TRAIN
    if args.max_pm_terms <= 0:
        pm_source = TRAIN.iloc[0:0]
    elif args.pm_quantile_sample and len(TRAIN) > args.pm_quantile_sample:
        pm_source = TRAIN.sample(n=int(args.pm_quantile_sample), random_state=args.seed).sort_index()
    pm_recs = []
    if len(pm_source):
        print(f"[pf-band] premom quantile rows={len(pm_source)} of TRAIN {len(TRAIN)}", flush=True)
    for j, r in enumerate(pm_source.itertuples(), 1):
        feats, reason = tt._premom(r.ticker, r.side, r.tt_entry_iso, float(r.tt_fill), 0.90, r.tt_sig_ts.isoformat())
        fd = dict(feats) if not reason else {}
        pm_recs.append({f: fd.get(f, np.nan) for f in PM_FEATS})
        if j % 500 == 0:
            print(f"[pf-band] premom quantiles {j}/{len(pm_source)}", flush=True)
    pm_df = pd.DataFrame(pm_recs)
    pm_quant = {}
    for f in PM_FEATS:
        s = pd.to_numeric(pm_df[f], errors="coerce").dropna()
        if len(s) >= 8 and s.nunique() > 1:
            pm_quant[f] = {q: float(s.quantile(q)) for q in QGRID}
    print(f"[pf-band] searchable mask={sorted(mask_quant)} premom={sorted(pm_quant)}")

    GL = float(args.gap_lambda)
    trial_rows = []
    t0 = time.time()

    def score_cfg(cfg):
        tt.MAX_POSITIONS = cfg.get("max_positions", 20)
        tt.DAILY_LOSS_RS = cfg.get("daily_loss_rs", 0.0)
        nf, pf_f, _ = _eval_fast(setup, cfg, FIT)
        nv, pf_v, _ = _eval_fast(setup, cfg, VAL)
        if nf < args.fv_floor or nv < args.fv_floor:
            return -5.0 + min(nf, nv) / max(1, args.fv_floor), nf, pf_f, nv, pf_v
        cf, cv = _clamp_pf(pf_f), _clamp_pf(pf_v)
        sc = band_reward(min(cf, cv)) - GL * abs(cf - cv)
        # Trade-count is ONLY a tiebreaker among genuinely-positive (in-band) configs,
        # so a high-frequency loser can never be selected as "best" when no edge exists.
        if min(cf, cv) >= PF_LO:
            sc += 0.003 * min(min(nf, nv), 40)
        return sc, nf, pf_f, nv, pf_v

    def record(cfg, sc, nf, pf_f, nv, pf_v):
        trial_rows.append({"sl": cfg["sl"], "tgt": cfg["tgt"],
                           "mask": ";".join(f"{a}{o}{b}" for a, o, b in cfg["mask_terms"]) or "-",
                           "premom": ";".join(f"{a}{o}{b}" for a, o, b in cfg["premom_terms"]) or "-",
                           "guard": json.dumps(cfg["guard"]) if cfg["guard"] else "-",
                           "max_positions": cfg.get("max_positions"), "daily_loss_rs": cfg.get("daily_loss_rs"),
                           "fit_n": nf, "fit_pf": round(pf_f, 3), "val_n": nv, "val_pf": round(pf_v, 3),
                           "score": round(float(sc), 4)})

    best = {"score": -1e9, "cfg": None}

    if _HAVE_OPTUNA:
        def objective(trial):
            cfg = _suggest(trial, mask_quant, pm_quant, args.max_mask_terms, args.max_pm_terms)
            sc, nf, pf_f, nv, pf_v = score_cfg(cfg)
            record(cfg, sc, nf, pf_f, nv, pf_v)
            if sc > best["score"]:
                best["score"], best["cfg"] = sc, cfg
            return sc
        study = optuna.create_study(direction="maximize", sampler=optuna.samplers.TPESampler(seed=args.seed))
        study.optimize(objective, n_trials=args.trials, timeout=args.time_budget_min * 60.0)
        n_done = len(study.trials)
    else:
        rng = random.Random(args.seed); n_done = 0
        for _ in range(args.trials):
            if time.time() - t0 > args.time_budget_min * 60.0:
                break
            cfg = _suggest(_RandTrial(rng), mask_quant, pm_quant, args.max_mask_terms, args.max_pm_terms)
            sc, nf, pf_f, nv, pf_v = score_cfg(cfg)
            record(cfg, sc, nf, pf_f, nv, pf_v)
            if sc > best["score"]:
                best["score"], best["cfg"] = sc, cfg
            n_done += 1
    print(f"[pf-band] completed {n_done} trials | best FIT/VAL score={best['score']:.4f}")

    bc = best["cfg"]
    tt.MAX_POSITIONS = bc.get("max_positions", 20); tt.DAILY_LOSS_RS = bc.get("daily_loss_rs", 0.0)
    mTR0 = fast_metrics(setup, bc, TRAIN)
    mTE0 = fast_metrics(setup, bc, TEST)
    needs_detail = (
        (mTR0["n"] >= args.min_trades_train and mTR0["net_pf"] >= max(0.90, PF_LO - 0.15))
        or (mTE0["n"] >= args.min_trades_test and mTE0["net_pf"] >= args.test_pf_min)
    )
    if needs_detail:
        mTR = full_metrics(setup, bc, TRAIN); mTE = full_metrics(setup, bc, TEST)
        _set_slippage(5.0); TR5, TE5 = _slice(FIT_s + VAL_s), _slice(TEST_s)
        mTR5 = full_metrics(setup, bc, TR5); mTE5 = full_metrics(setup, bc, TE5)
        _set_slippage(args.search_slippage_bps)
        detail_mode = "full_detail"
    else:
        mTR, mTE = mTR0, mTE0
        mTR5, mTE5 = mTR0.copy(), mTE0.copy()
        detail_mode = "fast_no_edge"
        print("[pf-band] best config is below robust train/test floors; using fast no-edge metrics", flush=True)

    # baseline (current conf config), scored on the same windows
    base_block, base_src = get_baseline_block(setup)
    mBTR = mBTE = None
    base_cfg = None
    if base_block is not None:
        base_cfg = conf_to_cfg(base_block)
        tt.MAX_POSITIONS = base_cfg.get("max_positions", 20); tt.DAILY_LOSS_RS = base_cfg.get("daily_loss_rs", 0.0)
        mBTR0 = fast_metrics(setup, base_cfg, TRAIN); mBTE0 = fast_metrics(setup, base_cfg, TEST)
        if (mBTR0["net_pf"] >= max(0.90, PF_LO - 0.15)) or (mBTE0["net_pf"] >= args.test_pf_min):
            mBTR = full_metrics(setup, base_cfg, TRAIN); mBTE = full_metrics(setup, base_cfg, TEST)
        else:
            mBTR, mBTE = mBTR0, mBTE0

    # ---- verdict (robust acceptance) ----
    def dom_ok(m):
        return (m["trade_dom_gross"] is not None and m["trade_dom_gross"] <= args.dom_cap and
                m["day_dom"] is not None and m["day_dom"] <= args.dom_cap and
                m["sym_dom"] is not None and m["sym_dom"] <= args.dom_cap)
    if detail_mode == "fast_no_edge":
        robust = {
            "base": {"n": mTR["n"], "pf": mTR["net_pf"], "n_days": mTR["n_days"]},
            "neighborhood_pf_min": args.neighborhood_pf_min,
            "dropout_pf_min": args.dropout_pf_min,
            "neighbor_checks": [],
            "dropout_checks": [],
            "neighbor_pass": False,
            "dropout_pass": False,
            "passed": False,
            "skipped": "selected config below robust train/test floors",
        }
    else:
        robust = robustness_report(setup, bc, TRAIN, mask_quant, pm_quant, args)
    hard_reasons = []
    insuff_reasons = []
    warnings = []
    if mTR["n"] < args.min_trades_train:
        hard_reasons.append("TRAIN too few trades (train_n<%d)" % args.min_trades_train)
    if mTR["net_pf"] < PF_LO:
        hard_reasons.append("TRAIN PF too low (<%.2f)" % PF_LO)
    if mTR["net_pf"] > PF_HI:
        warnings.append("TRAIN PF above preferred band (>%.2f)" % PF_HI)
    if mTR["target_rate"] < args.min_train_target_rate:
        hard_reasons.append("TRAIN target-fill rate below %.1f%%" % args.min_train_target_rate)
    if not dom_ok(mTR):
        hard_reasons.append("TRAIN concentrated (one trade/day/symbol dominates)")
    if mTR["trades_per_day"] > args.max_trades_day:
        hard_reasons.append("TRAIN too many trades/day")
    if not robust["neighbor_pass"]:
        hard_reasons.append("neighborhood robustness failed")
    if not robust["dropout_pass"]:
        hard_reasons.append("term-dropout robustness failed")

    if mTE["n"] < args.min_trades_test:
        insuff_reasons.append("TEST too few trades (test_n<%d)" % args.min_trades_test)
    else:
        if mTE["net_pf"] < args.test_pf_min:
            hard_reasons.append("TEST PF below %.2f" % args.test_pf_min)
        if mTE["day_block_p"] is None:
            insuff_reasons.append("TEST day-block p unavailable")
        elif not _finite_le(mTE["day_block_p"], args.max_test_day_block_p):
            hard_reasons.append("TEST day-block p above %.2f" % args.max_test_day_block_p)
        if not dom_ok(mTE):
            hard_reasons.append("TEST concentrated (one trade/day/symbol dominates)")
        if mTE["trades_per_day"] > args.max_trades_day:
            hard_reasons.append("TEST too many trades/day")

    passed = (not hard_reasons and not insuff_reasons and not warnings)
    if passed:
        verdict = "APPROVAL_REQUIRED"
    elif not hard_reasons and insuff_reasons:
        verdict = "INSUFFICIENT_OOS"
    else:
        verdict = "REJECT"
    reasons = hard_reasons + insuff_reasons + warnings

    # ---- persist artifacts ----
    pd.DataFrame(trial_rows).sort_values("score", ascending=False).to_csv(outdir / "trials.csv", index=False)
    conf_block = cfg_to_conf_block(setup, side, bc)
    (outdir / "best_config.json").write_text(json.dumps({
        "setup": setup, "verdict": verdict, "optimizer": engine,
        "hard_reasons": hard_reasons,
        "insufficient_reasons": insuff_reasons,
        "warnings": warnings,
        "detail_mode": detail_mode,
        "windows": {"FIT": _rng(FIT_s), "VAL": _rng(VAL_s), "TRAIN": _rng(TRAIN_s), "TEST": _rng(TEST_s)},
        "best_config": conf_block,
        "robustness": robust,
        "metrics": {"train_15bps": {k: v for k, v in mTR.items() if k != "detail"},
                    "test_15bps": {k: v for k, v in mTE.items() if k != "detail"},
                    "train_5bps": {k: v for k, v in mTR5.items() if k != "detail"},
                    "test_5bps": {k: v for k, v in mTE5.items() if k != "detail"}},
    }, indent=2, default=str), encoding="utf-8")

    def _mline(m):
        return (f"n={m['n']} PF={m['net_pf']} net=Rs{m['net_pnl']:,.0f} win%={m['win_rate']} "
                f"avgW=Rs{m['avg_win']:,.0f} avgL=Rs{m['avg_loss']:,.0f} maxDD=Rs{m['max_dd']:,.0f} "
                f"SL/TGT/EOD={m['sl_cnt']}/{m['tgt_cnt']}/{m['eod_cnt']} tgt%={m['target_rate']} "
                f"tpd={m['trades_per_day']} "
                f"tradeDom={m['trade_dom_gross']} dayDom={m['day_dom']} symDom={m['sym_dom']} dbp={m['day_block_p']}")

    bm = "; ".join(f"{a}{o}{b}" for a, o, b in bc["mask_terms"]) or "(none)"
    bp = "; ".join(f"{a}{o}{b}" for a, o, b in bc["premom_terms"]) or "(none)"
    cmd = (f"py -3.12 Train_and_Test\\setup_pf_1_4_approval_loop\\_engine\\pf_band_fitval_loop.py "
           f"--setup {setup} --pool {args.pool} --trials {args.trials} "
           f"--time_budget_min {args.time_budget_min} --seed {args.seed} "
           f"--gap_lambda {args.gap_lambda} --max_mask_terms {args.max_mask_terms} "
           f"--max_pm_terms {args.max_pm_terms} --test_pf_min {args.test_pf_min} "
           f"--max_test_day_block_p {args.max_test_day_block_p} "
           f"--min_train_target_rate {args.min_train_target_rate} "
           f"--neighborhood_pf_min {args.neighborhood_pf_min} --dropout_pf_min {args.dropout_pf_min} "
           f"--pm_quantile_sample {args.pm_quantile_sample}")

    run_summary = {
        "setup": setup, "side": side, "verdict": verdict, "reject_reasons": reasons,
        "hard_reasons": hard_reasons,
        "insufficient_reasons": insuff_reasons,
        "warnings": warnings,
        "detail_mode": detail_mode,
        "optimizer": engine, "n_trials": n_done, "split_note": split_note,
        "robust_gate": {
            "test_pf_min": args.test_pf_min,
            "max_test_day_block_p": args.max_test_day_block_p,
            "min_train_target_rate": args.min_train_target_rate,
            "max_mask_terms": args.max_mask_terms,
            "max_pm_terms": args.max_pm_terms,
            "gap_lambda": args.gap_lambda,
            "neighborhood_pf_min": args.neighborhood_pf_min,
            "dropout_pf_min": args.dropout_pf_min,
            "pm_quantile_sample": args.pm_quantile_sample,
        },
        "windows": {"FIT": _rng(FIT_s), "VAL": _rng(VAL_s), "TRAIN": _rng(TRAIN_s), "TEST": _rng(TEST_s),
                    "FIT_sessions": [str(pd.Timestamp(s).date()) for s in FIT_s],
                    "VAL_sessions": [str(pd.Timestamp(s).date()) for s in VAL_s],
                    "TEST_sessions": [str(pd.Timestamp(s).date()) for s in TEST_s]},
        "best_cfg": conf_block,
        "robustness": robust,
        "best_metrics": {"train_15bps": {k: v for k, v in mTR.items() if k != "detail"},
                         "test_15bps": {k: v for k, v in mTE.items() if k != "detail"},
                         "train_5bps": {k: v for k, v in mTR5.items() if k != "detail"},
                         "test_5bps": {k: v for k, v in mTE5.items() if k != "detail"}},
        "baseline_src": base_src,
        "baseline_cfg": (cfg_to_conf_block(setup, side, base_cfg) if base_cfg else None),
        "baseline_metrics": ({"train_15bps": {k: v for k, v in mBTR.items() if k != "detail"},
                              "test_15bps": {k: v for k, v in mBTE.items() if k != "detail"}}
                             if mBTR is not None else None),
        "command": cmd,
    }
    (outdir / "run_summary.json").write_text(json.dumps(run_summary, indent=2, default=str), encoding="utf-8")

    # equity PNGs
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        for lbl, m in (("train", mTR), ("test", mTE)):
            det = m["detail"]
            plt.figure(figsize=(7, 3))
            if not det.empty:
                det = det.sort_values("entry_time")
                plt.plot(range(1, len(det) + 1), det["net_pnl_rs"].cumsum().to_numpy(), marker=".")
                plt.title(f"{setup} {lbl} equity (best cfg @15bps) n={m['n']} PF={m['net_pf']}")
            else:
                plt.title(f"{setup} {lbl} — no trades")
            plt.xlabel("trade #"); plt.ylabel("cum net Rs"); plt.grid(alpha=0.3); plt.tight_layout()
            plt.savefig(outdir / f"equity_{lbl}.png", dpi=90); plt.close()
    except Exception as e:
        print(f"[pf-band] equity plot skipped: {e}")

    _write_markdown(outdir, setup, side, engine, n_done, split_note, _rng, FIT_s, VAL_s, TRAIN_s, TEST_s,
                    bc, bm, bp, conf_block, mTR, mTE, mTR5, mTE5, mBTR, mBTE, base_src, base_cfg,
                    verdict, reasons, hard_reasons, insuff_reasons, warnings, robust, passed, cmd, args, trial_rows, _mline)

    print("\n" + "=" * 90)
    print(f"VERDICT: {verdict}   setup={setup} side={side}  optimizer={engine}")
    if reasons:
        print("reject reasons: " + " | ".join(reasons))
    print(f"robustness: neighbor={robust['neighbor_pass']} dropout={robust['dropout_pass']}")
    print(f"best cfg: SL/Tgt={bc['sl']}/{bc['tgt']} mask=[{bm}] premom=[{bp}] guard={bc['guard'] or '-'} "
          f"maxpos={bc.get('max_positions')} dloss={bc.get('daily_loss_rs')}")
    print(f"  @15bps TRAIN {_mline(mTR)}")
    print(f"  @15bps TEST  {_mline(mTE)}")
    if mBTR is not None:
        print(f"  baseline ({base_src}):")
        print(f"    @15bps TRAIN {_mline(mBTR)}")
        print(f"    @15bps TEST  {_mline(mBTE)}")
    print(f"  artifacts -> {outdir}")
    print("=" * 90)
    return 0


def _write_markdown(outdir, setup, side, engine, n_done, split_note, _rng, FIT_s, VAL_s, TRAIN_s, TEST_s,
                    bc, bm, bp, conf_block, mTR, mTE, mTR5, mTE5, mBTR, mBTE, base_src, base_cfg,
                    verdict, reasons, hard_reasons, insuff_reasons, warnings, robust, passed, cmd, args, trial_rows, _mline):
    today = date.today().isoformat()

    def mtable(m):
        return (f"| trades | {m['n']} |\n| net PF | {m['net_pf']} |\n| net PnL | Rs{m['net_pnl']:,.0f} |\n"
                f"| win rate | {m['win_rate']}% |\n| wins / losses | {m['wins']} / {m['losses']} |\n"
                f"| avg win / avg loss | Rs{m['avg_win']:,.0f} / Rs{m['avg_loss']:,.0f} |\n"
                f"| gross profit / loss | Rs{m['gross_profit']:,.0f} / Rs{m['gross_loss']:,.0f} |\n"
                f"| max drawdown | Rs{m['max_dd']:,.0f} |\n"
                f"| SL / TGT / EOD exits | {m['sl_cnt']} / {m['tgt_cnt']} / {m['eod_cnt']} |\n"
                f"| target-fill rate | {m['target_rate']}% |\n"
                f"| trades/day | {m['trades_per_day']} |\n| days / symbols | {m['n_days']} / {m['n_syms']} |\n"
                f"| top-trade gross share | {m['trade_dom_gross']} |\n| top-day net share | {m['day_dom']} |\n"
                f"| top-symbol net share | {m['sym_dom']} |\n| day-block p | {m['day_block_p']} |\n"
                f"| top day | {m['top_day']} |\n| top symbol | {m['top_sym']} |")

    # BASELINE_RESULT.md
    if base_cfg is not None and mBTR is not None:
        base_lines = [
            f"# {setup} ({side}) — BASELINE_RESULT", "",
            f"_Generated {today}. Research-only; NO live trades; NO final_setup_conf.py edits._", "",
            f"- **Config source:** {base_src}",
            f"- **Baseline exit:** SL {base_cfg['sl']}% / Tgt {base_cfg['tgt']}%",
            f"- **Baseline mask_terms:** {conf_block_str(base_cfg['mask_terms'])}",
            f"- **Baseline pre_momentum_terms:** {conf_block_str(base_cfg['premom_terms'])}",
            f"- **Baseline entry_guards:** {base_cfg['guard'] or '{}'}",
            f"- **Exit model:** resolve on 1-min OHLC to 15:20 IST (TARGET/SL/EOD); net of NSE intraday costs; "
            f"entry = next 1-min open after the 5-min signal + slippage.", "",
            f"## Sessions (exact)", "",
            f"- {split_note}",
            f"- **TRAIN** {_rng(TRAIN_s)} ({len(TRAIN_s)} sessions) = FIT {_rng(FIT_s)} + VAL {_rng(VAL_s)}",
            f"- **TEST**  {_rng(TEST_s)} ({len(TEST_s)} sessions): "
            f"{', '.join(str(pd.Timestamp(s).date()) for s in TEST_s)}", "",
            f"## Baseline TRAIN metrics (@15 bps/leg)", "", "| metric | value |", "|---|---|", mtable(mBTR), "",
            f"## Baseline TEST metrics (@15 bps/leg)", "", "| metric | value |", "|---|---|", mtable(mBTE), "",
            "## Initial diagnosis", "",
            f"- Baseline TRAIN PF {mBTR['net_pf']} / TEST PF {mBTE['net_pf']} "
            f"(target: TRAIN PF >= {PF_LO:.2f}, TEST PF >= {args.test_pf_min:.2f} "
            f"with day-block p <= {args.max_test_day_block_p:.2f}).",
            f"- TEST sample is {mTE['n']} trades over {mTE['n_days']} day(s) — thin June data is the binding "
            f"constraint on OOS confidence.",
        ]
    else:
        base_lines = [f"# {setup} ({side}) — BASELINE_RESULT", "",
                      f"_Generated {today}._", "",
                      f"- No baseline config found in final_setup_conf.py ({base_src}); this setup has no tuned "
                      f"config of record. Baseline = raw detection only.", "",
                      f"- {split_note}",
                      f"- TRAIN {_rng(TRAIN_s)} ({len(TRAIN_s)}) | TEST {_rng(TEST_s)} ({len(TEST_s)})"]
    (outdir / "BASELINE_RESULT.md").write_text("\n".join(base_lines), encoding="utf-8")

    # ITERATION_LOG.md — search summary + top trials as evidence-backed iterations
    tdf = pd.DataFrame(trial_rows).sort_values("score", ascending=False)
    top = tdf.head(20)
    il = [f"# {setup} ({side}) — ITERATION_LOG", "",
          f"_Generated {today}. Optimizer: {engine}. {n_done} FIT/VAL trials "
          f"(each trial = one logical config: exit SL/Tgt + <={args.max_mask_terms} mask + "
          f"<={args.max_pm_terms} pre-momentum + guards)._", "",
          "Search protocol: optimise ONLY on FIT/VAL with the band objective "
          f"`reward(min(FIT_PF,VAL_PF)) - {args.gap_lambda:.2f}*|FIT_PF-VAL_PF|`, "
          "where reward tents at PF 1.70 and "
          "penalises overshoot (anti-overfit). The single best FIT/VAL config is then confirmed on full "
          "TRAIN and scored ONCE on TEST. Quantile thresholds drawn from TRAIN only (never TEST). "
          "The robust gate then checks TEST PF+day-block p, TRAIN target-fill, "
          "threshold-neighborhood stability, and term-dropout stability.", "",
          f"Exit grid searched: SL {SL_GRID} x Tgt {TGT_GRID} (small/large SL x small/large target, "
          f"asymmetric combos). Mask feats: {sorted(set(t.split(';')[0] for t in tdf['mask'] if t!='-'))[:12]}.", "",
          "## Top 20 FIT/VAL trials (by band score)", "",
          "| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |",
          "|---|----|-----|------|--------|-------|----------|----------|-------|"]
    for i, (_, r) in enumerate(top.iterrows(), 1):
        il.append(f"| {i} | {r['sl']} | {r['tgt']} | {r['mask']} | {r['premom']} | {r['guard']} | "
                  f"{r['fit_n']}/{r['fit_pf']} | {r['val_n']}/{r['val_pf']} | {r['score']} |")
    il += ["", "## Best config — confirmation runs", "",
           f"- **Best (FIT/VAL):** SL {bc['sl']}/Tgt {bc['tgt']} | mask [{bm}] | premom [{bp}] | "
           f"guard {bc['guard'] or '-'} | maxpos {bc.get('max_positions')} | dloss {bc.get('daily_loss_rs')}",
           f"- **TRAIN @15bps:** {_mline(mTR)}",
           f"- **TEST  @15bps:** {_mline(mTE)}",
           f"- **TRAIN @5bps:**  {_mline(mTR5)}",
           f"- **TEST  @5bps:**  {_mline(mTE5)}", "",
           "## Robustness diagnostics", "",
           f"- neighborhood pass: `{robust['neighbor_pass']}` "
           f"(PF floor {robust['neighborhood_pf_min']})",
           f"- term-dropout pass: `{robust['dropout_pass']}` "
           f"(PF floor {robust['dropout_pf_min']})",
           f"- hard reasons: `{'; '.join(hard_reasons) or '-'}`",
           f"- insufficient reasons: `{'; '.join(insuff_reasons) or '-'}`",
           f"- warnings: `{'; '.join(warnings) or '-'}`", "",
           f"- **Keep/reject:** {verdict}" + ("" if passed else "  — " + "; ".join(reasons)), "",
           f"## Command\n```\n{cmd}\n```"]
    (outdir / "ITERATION_LOG.md").write_text("\n".join(il), encoding="utf-8")

    # FAILURE_ANALYSIS.md
    det = mTE["detail"] if not mTE["detail"].empty else mTR["detail"]
    fa = [f"# {setup} ({side}) — FAILURE_ANALYSIS", "", f"_Generated {today}._", ""]
    if det is not None and not det.empty:
        worst = det.nsmallest(8, "net_pnl_rs")[["trade_date", "ticker", "outcome", "bars_held", "net_pnl_rs"]]
        dayg = det.groupby("trade_date")["net_pnl_rs"].sum().sort_values()
        symg = det.groupby("ticker")["net_pnl_rs"].sum().sort_values()
        oc = det["outcome"].astype(str).str.upper().value_counts().to_dict()
        fa += ["## Worst trades (best-config book)", "", "| date | ticker | outcome | bars | net Rs |", "|---|---|---|---|---|"]
        for _, r in worst.iterrows():
            fa.append(f"| {r['trade_date']} | {r['ticker']} | {r['outcome']} | {r['bars_held']} | {r['net_pnl_rs']:,.0f} |")
        fa += ["", "## Worst days", ""] + [f"- {d}: Rs{v:,.0f}" for d, v in dayg.head(5).items()]
        fa += ["", "## Worst symbols", ""] + [f"- {s}: Rs{v:,.0f}" for s, v in symg.head(5).items()]
        fa += ["", "## Exit-type distribution (best cfg)", "", f"- {oc}",
               "", "## Notes",
               f"- SL/TGT/EOD split TRAIN = {mTR['sl_cnt']}/{mTR['tgt_cnt']}/{mTR['eod_cnt']}, "
               f"TEST = {mTE['sl_cnt']}/{mTE['tgt_cnt']}/{mTE['eod_cnt']}.",
               f"- TRAIN avg win/avg loss = Rs{mTR['avg_win']:,.0f}/Rs{mTR['avg_loss']:,.0f}; "
               f"TEST = Rs{mTE['avg_win']:,.0f}/Rs{mTE['avg_loss']:,.0f}.",
               f"- Concentration: TEST top-day share {mTE['day_dom']}, top-symbol share {mTE['sym_dom']}, "
               f"top-trade gross share {mTE['trade_dom_gross']}."]
    else:
        fa += ["- Best config produced no resolvable book on TEST — likely over-tight gating or thin sample."]
    if reasons:
        fa += ["", "## Classified failure reasons", ""] + [f"- {r}" for r in reasons]
    fa += ["", "## Robust gate diagnostics", "",
           f"- neighborhood pass: {robust['neighbor_pass']}",
           f"- term-dropout pass: {robust['dropout_pass']}",
           f"- TRAIN target-fill rate: {mTR['target_rate']}% "
           f"(min {args.min_train_target_rate}%)",
           f"- TEST PF/day-block p: {mTE['net_pf']} / {mTE['day_block_p']} "
           f"(gate {args.test_pf_min} / {args.max_test_day_block_p})"]
    (outdir / "FAILURE_ANALYSIS.md").write_text("\n".join(fa), encoding="utf-8")

    # CANDIDATE_CONFIGS.md + candidate JSON (only if passed)
    if passed:
        cc = [f"# {setup} ({side}) — CANDIDATE_CONFIGS (PASSED band gate)", "", f"_Generated {today}._", "",
              "## Candidate 001", "", "```json", json.dumps(conf_block, indent=2), "```", "",
              f"- TRAIN @15bps: {_mline(mTR)}", f"- TEST  @15bps: {_mline(mTE)}",
              f"- TRAIN @5bps:  {_mline(mTR5)}", f"- TEST  @5bps:  {_mline(mTE5)}", "",
              "- Risk notes: see APPROVAL_REQUIRED_FINAL_RECOMMENDATION.md.",
              "- Recommendation: APPROVAL REQUIRED (do not auto-promote)."]
        (outdir / "CANDIDATE_CONFIGS.md").write_text("\n".join(cc), encoding="utf-8")
        cand = {"setup": setup, "side": side, "verdict": "APPROVAL_REQUIRED",
                "config": conf_block,
                "train_15bps": {k: v for k, v in mTR.items() if k != "detail"},
                "test_15bps": {k: v for k, v in mTE.items() if k != "detail"},
                "windows": {"TRAIN": _rng(TRAIN_s), "TEST": _rng(TEST_s)}}
        (outdir / "candidates" / f"{setup}_candidate_001.json").write_text(
            json.dumps(cand, indent=2, default=str), encoding="utf-8")
    else:
        (outdir / "CANDIDATE_CONFIGS.md").write_text(
            f"# {setup} ({side}) — CANDIDATE_CONFIGS\n\n_Generated {today}._\n\n"
            f"**No candidate cleared the robust gate** (TRAIN PF >= {PF_LO:.2f}, "
            f"TEST PF >= {args.test_pf_min:.2f}, TEST day-block p <= {args.max_test_day_block_p:.2f}, "
            f"target-fill, neighborhood, dropout, meaningful trades, and concentration checks).\n\n"
            f"Verdict: **{verdict}**\n\n"
            f"Reject reasons: {'; '.join(reasons)}\n", encoding="utf-8")

    # APPROVAL_REQUIRED_FINAL_RECOMMENDATION.md
    rec = "YES — APPROVAL REQUIRED" if passed else "NO"
    ar = [f"# {setup} ({side}) — APPROVAL_REQUIRED / FINAL RECOMMENDATION", "", f"_Generated {today}._", "",
          f"## Approval recommendation: **{rec}**", "",
          f"- Verdict: **{verdict}**" + ("" if passed else "  (" + "; ".join(reasons) + ")"), "",
          "## Best candidate config (proposed)", "", "```json", json.dumps(conf_block, indent=2), "```", "",
          "## Metrics", "", "| window | @15 bps/leg | @5 bps/leg |", "|---|---|---|",
          f"| TRAIN | {_mline(mTR)} | {_mline(mTR5)} |",
          f"| TEST  | {_mline(mTE)} | {_mline(mTE5)} |", ""]
    if passed:
        ar += ["## If approved, the file that would need editing", "",
               "- `final_setup_conf.py` (repo root) — add/replace the `" + setup + "` entry under "
               "`FINAL_SETUP_CONF` with the JSON block above.", "",
               "> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**", ""]
    else:
        ar += ["## No promotion proposed", "",
               "- This setup did **not** clear the robust TRAIN+TEST gate. No edit to `final_setup_conf.py` "
               "is recommended.",
               "- `INSUFFICIENT_OOS` means the train-side config was not confirmed or rejected because "
               "the OOS sample lacked enough statistical power.", "",
               "> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**", ""]
    ar += ["## Rerun command", "```", cmd, "```", "",
           "## Remaining risks", "",
           f"- TEST sample = {mTE['n']} trades / {mTE['n_days']} day(s) (thin June data).",
           f"- Robustness: neighborhood={robust['neighbor_pass']}, dropout={robust['dropout_pass']}.",
           f"- TEST concentration: top-day {mTE['day_dom']}, top-symbol {mTE['sym_dom']}, "
           f"top-trade {mTE['trade_dom_gross']} (cap {args.dom_cap}).",
           "- Native/screening vs live-faithful: readmit setups are live-faithful; others are screening-only "
           "(confirm on the v11 conf backtest before any live use)."]
    (outdir / "APPROVAL_REQUIRED_FINAL_RECOMMENDATION.md").write_text("\n".join(ar), encoding="utf-8")


def conf_block_str(terms):
    return "[" + ", ".join(f"{a}{o}{b}" for a, o, b in terms) + "]" if terms else "[] (none)"


if __name__ == "__main__":
    raise SystemExit(main())
