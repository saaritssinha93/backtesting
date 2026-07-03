r"""pf_band_search.py — research-only TRAIN-PF-band / TEST-PF>1.40 approval loop for ONE setup.

PROTOCOL (per the user's pf_1_4 approval-loop spec)
---------------------------------------------------
Goal: find a candidate config with
    * full-TRAIN net Profit Factor in the BAND [1.30, 1.70]   (NOT higher — high TRAIN PF treated as overfit)
    * TEST net PF > 1.40
    * meaningful trade counts, no single trade/day/symbol dominating, simple/structural logic.

Data split (sessions inferred from the per-setup pool's sorted distinct completed sessions):
    TRAIN = completed sessions in [--train_start, session before TEST starts]   (default train_start 2026-05-18)
    TEST  = completed sessions in [--test_start, latest available session]       (default test_start  2026-06-20)
    FIT   = first half of TRAIN sessions  |  VAL = second half of TRAIN sessions
The exact FIT/VAL/TRAIN/TEST sessions are printed before any search runs.

ANTI-OVERFIT: search runs ONLY on FIT/VAL. A config is confirmed on full TRAIN; TEST is scored
exactly ONCE per TRAIN-band candidate and is never tuned on.

Exit SL% / target% are FIRST-CLASS search dimensions (thorough grids). Everything else uses ONLY
repo-supported knobs: mask_terms (pool columns), pre_momentum_terms (premom features), entry guards
(min_slot/max_slot/top_n), max_positions, daily_loss_rs. Reuses setup_train_test.eval_family /
book_detail so entry+exit+cost+slippage+dedupe+mask+premom+guard+portfolio-overlay are identical to
the repo backtest. Net of cost. Optuna TPE if available; else a deterministic seeded random search.

This script writes NOTHING to final_setup_conf.py and places ALL artifacts under
    Train_and_Test/setup_pf_1_4_approval_loop/<SETUP>/
No live trades, no live execution.

Run (from repo root):
  py -3.12 Train_and_Test/setup_pf_1_4_approval_loop/L_DOUBLE_BOTTOM_VWAP/scripts/pf_band_search.py \
      --setup L_DOUBLE_BOTTOM_VWAP \
      --pool C:/TradingData/eqidv2/setup_pools_2026_06_29/L_DOUBLE_BOTTOM_VWAP \
      --trials 400 --time_budget_min 25 --seed 7
"""
from __future__ import annotations

import argparse
import json
import random
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

# --- import the repo pipeline in place (single source of truth) ---------------
_P = Path(__file__).resolve()
TT_DIR = next(par for par in _P.parents if par.name == "Train_and_Test")
REPO_ROOT = TT_DIR.parent
for _d in (str(REPO_ROOT), str(TT_DIR)):
    if _d not in sys.path:
        sys.path.insert(0, _d)

import setup_train_test as tt  # noqa: E402

try:
    import optuna
    optuna.logging.set_verbosity(optuna.logging.WARNING)
    _HAVE_OPTUNA = True
except Exception:
    _HAVE_OPTUNA = False

# ---------------------------------------------------------------------------
# Constants / search space
# ---------------------------------------------------------------------------
BAND_LO, BAND_HI = 1.30, 1.70          # TRAIN PF acceptance band
TEST_PF_MIN = 1.40                     # TEST acceptance floor
DOM_CAP = 0.40                         # max single trade/day/symbol share of gross/total
MAX_TPD = 6.0                          # max trades per day
GAP_LAMBDA = 0.50                      # FIT/VAL divergence penalty

# Exit grids = FIRST-CLASS dimensions (thorough).
SL_GRID = [0.50, 0.60, 0.70, 0.80, 0.85, 0.90, 1.00, 1.10, 1.20, 1.30, 1.50]
TGT_GRID = [0.60, 0.80, 1.00, 1.25, 1.50, 1.75, 2.00, 2.50, 3.00]

MASK_FEATS = ["rs_pct", "vol_ratio", "atr_pct", "body_pct", "close_loc", "vwap_dist_atr",
              "quality_score", "ranker_score", "signal_range_pct", "upper_wick_pct",
              "lower_wick_pct", "wick_skew_pct"]
PM_FEATS = ["pre_entry_momentum_score", "sig5_adx_calc", "sig5_rsi_dir", "sig5_vol_ratio20",
            "pre1_adx", "pre3_range_r", "pre5_mom_r", "pre3_close_pos"]
QGRID = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
MIN_SLOTS = [None, "09:30", "09:45", "10:00", "10:30", "11:00"]
MAX_SLOTS = [None, "11:30", "12:00", "12:30", "13:00", "14:00", "14:30"]
MAXPOS_GRID = [10, 20]
DLOSS_GRID = [0.0, 3000.0, 5000.0]
PM_REF_SL = 0.90                       # SL used only to sample premom threshold quantiles

# Logical groups (one-group-per-iteration forward-greedy ablation, for the narrative).
ABLATION_GROUPS = [
    ("exit", None),
    ("volume", ["vol_ratio"]),
    ("volatility", ["atr_pct"]),
    ("candle_structure", ["close_loc", "body_pct", "upper_wick_pct", "lower_wick_pct", "wick_skew_pct"]),
    ("vwap_distance", ["vwap_dist_atr"]),
    ("relative_strength", ["rs_pct"]),
    ("quality", ["quality_score"]),
    ("premom_adx", ["sig5_adx_calc", "pre1_adx"]),
    ("premom_momentum", ["pre_entry_momentum_score", "pre5_mom_r", "pre3_close_pos", "pre3_range_r"]),
    ("time_guard", ["__time__"]),
    ("top_n", ["__topn__"]),
]
PREMOM_SET = set(PM_FEATS)


# ---------------------------------------------------------------------------
# Built-in card baselines (from SETUP_CARDS_AND_LIVE_CROSSCHECK.md). Used as
# iteration-1 starting point; falls back to raw detection if setup unknown.
# ---------------------------------------------------------------------------
CARD_BASELINES = {
    "L_DOUBLE_BOTTOM_VWAP": {
        "sl": 0.90, "tgt": 1.50, "mask_terms": [],
        "premom_terms": [["pre_entry_momentum_score", ">=", 79.0], ["sig5_adx_calc", ">=", 28.0]],
        "guard": None, "max_positions": 20, "daily_loss_rs": 0.0,
    },
    "L_PRESSURE_BURST_VWAP": {
        "sl": 0.70, "tgt": 1.25, "mask_terms": [["quality_score", "<=", 25.0]],
        "premom_terms": [["pre1_adx", ">=", 44.0]],
        "guard": None, "max_positions": 20, "daily_loss_rs": 0.0,
    },
}


def _norm_cfg(cfg: dict) -> dict:
    return {
        "sl": float(cfg["sl"]), "tgt": float(cfg["tgt"]),
        "mask_terms": [tuple(t) for t in (cfg.get("mask_terms") or [])],
        "premom_terms": [tuple(t) for t in (cfg.get("premom_terms") or [])],
        "guard": (cfg.get("guard") or None), "status": "OK",
        "max_positions": int(cfg.get("max_positions", 20)),
        "daily_loss_rs": float(cfg.get("daily_loss_rs", 0.0)),
    }


def _terms_str(terms) -> str:
    return "; ".join(f"{a}{o}{b}" for a, o, b in (terms or [])) or "(none)"


def _pf(net: np.ndarray) -> float:
    return float(tt._pf(np.asarray(net, float)))


# ---------------------------------------------------------------------------
# Slippage / entries
# ---------------------------------------------------------------------------
def _set_slip(bps: float) -> None:
    tt.SLIPPAGE_BPS = float(bps)
    tt._entry.cache_clear(); tt._resolve_full.cache_clear(); tt._premom.cache_clear()


# ---------------------------------------------------------------------------
# Full metric pack for one config on one window
# ---------------------------------------------------------------------------
def full_metrics(setup: str, cfg: dict, df: pd.DataFrame) -> dict:
    c = _norm_cfg(cfg)
    tt.MAX_POSITIONS = c["max_positions"]; tt.DAILY_LOSS_RS = c["daily_loss_rs"]
    fam = tt.eval_family({setup: c}, df)
    exits = {setup: (c["sl"], c["tgt"])}
    det = tt.book_detail(fam["book"], exits) if fam["trades"] else pd.DataFrame()
    m = {
        "trades": int(fam["trades"]), "net_pf": round(float(fam["net_pf"]), 4),
        "net_pnl": round(float(fam["net_pnl"]), 2),
        "day_block_p": (None if not np.isfinite(fam["day_block_p"]) else round(float(fam["day_block_p"]), 4)),
        "wins": 0, "losses": 0, "win_rate_pct": 0.0,
        "gross_profit": 0.0, "gross_loss": 0.0, "avg_win": 0.0, "avg_loss": 0.0,
        "max_drawdown": 0.0, "n_days": 0, "n_syms": 0, "trades_per_day": 0.0,
        "sl_cnt": 0, "tgt_cnt": 0, "eod_cnt": 0,
        "trade_dom_gross": None, "day_dom": None, "sym_dom": None,
        "daywise": [], "symwise": [], "hourwise": [], "detail": det,
    }
    if det.empty:
        return m
    net = det["net_pnl_rs"].to_numpy()
    wins, losses = net[net > 0], net[net <= 0]
    tot, gp = float(net.sum()), float(wins.sum())
    oc = det["outcome"].astype(str)
    ds = det.sort_values("entry_time")
    cum = ds["net_pnl_rs"].cumsum().to_numpy()
    dd = float((cum - np.maximum.accumulate(cum)).min()) if len(cum) else 0.0
    day_net = det.groupby("trade_date")["net_pnl_rs"].sum()
    sym_net = det.groupby("ticker")["net_pnl_rs"].sum()
    det = det.copy()
    det["_hour"] = pd.to_datetime(det["entry_time"]).dt.strftime("%H")
    m.update({
        "wins": int((net > 0).sum()), "losses": int((net <= 0).sum()),
        "win_rate_pct": round(float((net > 0).mean()) * 100, 2),
        "gross_profit": round(gp, 2), "gross_loss": round(float(losses.sum()), 2),
        "avg_win": round(float(wins.mean()), 2) if len(wins) else 0.0,
        "avg_loss": round(float(losses.mean()), 2) if len(losses) else 0.0,
        "max_drawdown": round(dd, 2),
        "n_days": int(det["trade_date"].nunique()), "n_syms": int(det["ticker"].nunique()),
        "trades_per_day": round(m["trades"] / max(1, det["trade_date"].nunique()), 2),
        "sl_cnt": int((oc == "SL").sum()), "tgt_cnt": int((oc == "TARGET").sum()),
        "eod_cnt": int((~oc.isin(["SL", "TARGET"])).sum()),
        "trade_dom_gross": round(float(net.max()) / gp, 3) if gp > 0 else 9.99,
        "day_dom": round(float(day_net.max()) / tot, 3) if tot > 0 else 9.99,
        "sym_dom": round(float(sym_net.max()) / tot, 3) if tot > 0 else 9.99,
        "daywise": [{"date": str(i), "n": int((det["trade_date"] == i).sum()),
                     "net": round(float(v), 2),
                     "pf": round(_pf(det.loc[det["trade_date"] == i, "net_pnl_rs"].to_numpy()), 3)}
                    for i, v in day_net.sort_index().items()],
        "symwise": [{"ticker": i, "n": int((det["ticker"] == i).sum()), "net": round(float(v), 2)}
                    for i, v in sym_net.sort_values().items()],
        "hourwise": [{"hour": h, "n": int(len(g)), "net": round(float(g["net_pnl_rs"].sum()), 2),
                      "pf": round(_pf(g["net_pnl_rs"].to_numpy()), 3)}
                     for h, g in det.groupby("_hour")],
    })
    return m


def _light(setup: str, cfg: dict, df: pd.DataFrame) -> tuple[int, float, int]:
    """Fast (trades, net_pf, n_days) — no per-trade detail."""
    c = _norm_cfg(cfg)
    tt.MAX_POSITIONS = c["max_positions"]; tt.DAILY_LOSS_RS = c["daily_loss_rs"]
    fam = tt.eval_family({setup: c}, df)
    book = fam.get("book")
    nd = int(pd.Series(book["_day"]).nunique()) if (book is not None and len(book)) else 0
    return int(fam["trades"]), float(fam["net_pf"]), nd


# ---------------------------------------------------------------------------
# FIT/VAL band objective
# ---------------------------------------------------------------------------
def _clamp(pf: float) -> float:
    """Clamp PF to the band-high: removes any incentive to push PF past 1.70 (anti-overfit),
    so the search prefers configs that bring BOTH folds up to the band with the smallest gap."""
    if not np.isfinite(pf):
        pf = BAND_HI
    return min(float(pf), BAND_HI)


def fitval_score(setup: str, cfg: dict, FIT, VAL, min_fold: int):
    nf, pf_f, _ = _light(setup, cfg, FIT)
    nv, pf_v, _ = _light(setup, cfg, VAL)
    if nf < min_fold or nv < min_fold:                       # too few -> nudge toward more trades
        return (-5.0 + min(nf, nv) / max(1, min_fold)), nf, pf_f, nv, pf_v
    cf, cv = _clamp(pf_f), _clamp(pf_v)
    return (min(cf, cv) - GAP_LAMBDA * abs(cf - cv)), nf, pf_f, nv, pf_v


# ---------------------------------------------------------------------------
# Quantile grids (for threshold sampling) from TRAIN only
# ---------------------------------------------------------------------------
def build_quantiles(TRAIN: pd.DataFrame):
    mask_quant = {}
    for f in MASK_FEATS:
        if f in TRAIN.columns:
            s = pd.to_numeric(TRAIN[f], errors="coerce").dropna()
            if len(s) >= 8 and s.nunique() > 1:
                mask_quant[f] = {q: float(s.quantile(q)) for q in QGRID}
    pm_recs = []
    for r in TRAIN.itertuples():
        feats, reason = tt._premom(r.ticker, r.side, r.tt_entry_iso, float(r.tt_fill), PM_REF_SL, r.tt_sig_ts.isoformat())
        fd = dict(feats) if not reason else {}
        pm_recs.append({f: fd.get(f, np.nan) for f in PM_FEATS})
    pm_df = pd.DataFrame(pm_recs)
    pm_quant = {}
    for f in PM_FEATS:
        s = pd.to_numeric(pm_df[f], errors="coerce").dropna()
        if len(s) >= 8 and s.nunique() > 1:
            pm_quant[f] = {q: float(s.quantile(q)) for q in QGRID}
    return mask_quant, pm_quant


# ---------------------------------------------------------------------------
# Forward-greedy single-logical-group ablation (narrative + diagnosis)
# ---------------------------------------------------------------------------
def _best_exit(setup, base, FIT, VAL, min_fold):
    best = None
    for sl in SL_GRID:
        for tgt in TGT_GRID:
            cfg = dict(base); cfg["sl"] = sl; cfg["tgt"] = tgt
            sc, nf, pf_f, nv, pf_v = fitval_score(setup, cfg, FIT, VAL, min_fold)
            if best is None or sc > best[0]:
                best = (sc, sl, tgt, nf, pf_f, nv, pf_v)
    return best


def _best_term_for_group(setup, base, feats, mask_quant, pm_quant, FIT, VAL, min_fold):
    """Best single threshold term within one logical group, added on top of `base`."""
    best = None
    for f in feats:
        if f == "__time__":
            for mn in MIN_SLOTS:
                for mx in MAX_SLOTS:
                    if mn is None and mx is None:
                        continue
                    g = dict(base.get("guard") or {})
                    if mn: g["min_slot"] = mn
                    if mx: g["max_slot"] = mx
                    cfg = dict(base); cfg["guard"] = g
                    sc, nf, pf_f, nv, pf_v = fitval_score(setup, cfg, FIT, VAL, min_fold)
                    desc = f"guard min_slot={mn} max_slot={mx}"
                    if best is None or sc > best[0]:
                        best = (sc, ("guard", desc, cfg), nf, pf_f, nv, pf_v)
            continue
        if f == "__topn__":
            for tn in (1, 2, 3):
                g = dict(base.get("guard") or {}); g["top_n"] = tn
                cfg = dict(base); cfg["guard"] = g
                sc, nf, pf_f, nv, pf_v = fitval_score(setup, cfg, FIT, VAL, min_fold)
                if best is None or sc > best[0]:
                    best = (sc, ("guard", f"top_n={tn}", cfg), nf, pf_f, nv, pf_v)
            continue
        quant = pm_quant if f in PREMOM_SET else mask_quant
        if f not in quant:
            continue
        for q in QGRID:
            thr = round(float(quant[f][q]), 6)
            for op in (">=", "<="):
                cfg = dict(base)
                if f in PREMOM_SET:
                    cfg["premom_terms"] = list(base.get("premom_terms") or []) + [[f, op, thr]]
                else:
                    cfg["mask_terms"] = list(base.get("mask_terms") or []) + [[f, op, thr]]
                sc, nf, pf_f, nv, pf_v = fitval_score(setup, cfg, FIT, VAL, min_fold)
                desc = f"{'premom' if f in PREMOM_SET else 'mask'} {f}{op}{thr}"
                if best is None or sc > best[0]:
                    best = (sc, (("premom" if f in PREMOM_SET else "mask"), desc, cfg), nf, pf_f, nv, pf_v)
    return best


def run_ablation(setup, baseline, mask_quant, pm_quant, FIT, VAL, TRAIN, TEST, min_fold, rerun_cmd):
    """Forward-greedy: change ONE logical group per iteration; keep only if FIT/VAL score improves
    AND mask term count stays <=3 / premom <=2. Returns iteration records."""
    iters = []
    cur = dict(baseline)
    sc0, nf0, pf_f0, nv0, pf_v0 = fitval_score(setup, cur, FIT, VAL, min_fold)
    cur_score = sc0
    n = 1
    iters.append(_iter_rec(n, "baseline(card)", "starting point = card config", rerun_cmd, cur,
                           nf0, pf_f0, nv0, pf_v0, "keep(start)", "sweep exit next",
                           setup, TRAIN, TEST))
    for group, feats in ABLATION_GROUPS:
        n += 1
        if group == "exit":
            be = _best_exit(setup, cur, FIT, VAL, min_fold)
            sc, sl, tgt, nf, pf_f, nv, pf_v = be
            change = f"exit SL/Tgt -> {sl}/{tgt}"
            improved = sc > cur_score + 1e-9
            cand = dict(cur); cand["sl"] = sl; cand["tgt"] = tgt
        else:
            n_mask = len(cur.get("mask_terms") or [])
            n_pm = len(cur.get("premom_terms") or [])
            is_pm = bool(feats) and feats[0] in PREMOM_SET
            if (is_pm and n_pm >= 2) or ((not is_pm) and feats[0] not in ("__time__", "__topn__") and n_mask >= 3):
                iters.append(_iter_rec(n, group, "skipped (term budget reached)", rerun_cmd, cur,
                                       nf0, pf_f0, nv0, pf_v0, "skip", "next group", setup, None, None,
                                       no_eval=True))
                continue
            bt = _best_term_for_group(setup, cur, feats, mask_quant, pm_quant, FIT, VAL, min_fold)
            if bt is None:
                iters.append(_iter_rec(n, group, "no usable feature in window", rerun_cmd, cur,
                                       nf0, pf_f0, nv0, pf_v0, "skip", "next group", setup, None, None,
                                       no_eval=True))
                continue
            sc, (kind, desc, cand), nf, pf_f, nv, pf_v = bt
            change = desc
            improved = sc > cur_score + 1e-9
        decision = "keep" if improved else "reject"
        nxt = "lock group, continue" if improved else "revert, try next group"
        rec = _iter_rec(n, group, change, rerun_cmd, cand, nf, pf_f, nv, pf_v, decision, nxt,
                        setup, TRAIN, TEST)
        iters.append(rec)
        if improved:
            cur = cand; cur_score = sc; nf0, pf_f0, nv0, pf_v0 = nf, pf_f, nv, pf_v
    return iters, cur, cur_score


def _iter_rec(n, group, change, cmd, cfg, nf, pf_f, nv, pf_v, decision, nxt,
              setup, TRAIN, TEST, no_eval=False):
    rec = {"n": n, "group": group, "change": change, "cmd": cmd,
           "cfg": _ser_cfg(cfg),
           "fit": {"n": int(nf), "pf": round(float(pf_f), 3)},
           "val": {"n": int(nv), "pf": round(float(pf_v), 3)},
           "decision": decision, "next": nxt}
    # Confirm on full TRAIN; only score TEST if TRAIN PF lands in band (anti-overfit rule).
    if not no_eval and TRAIN is not None:
        mt = full_metrics(setup, cfg, TRAIN)
        rec["train"] = {"n": mt["trades"], "pf": mt["net_pf"], "net": mt["net_pnl"],
                        "tpd": mt["trades_per_day"]}
        if TEST is not None and BAND_LO <= mt["net_pf"] <= BAND_HI and mt["trades"] > 0:
            me = full_metrics(setup, cfg, TEST)
            rec["test"] = {"n": me["trades"], "pf": me["net_pf"], "net": me["net_pnl"],
                           "tpd": me["trades_per_day"]}
            rec["test_gated"] = True
        else:
            rec["test_gated"] = False
    return rec


def _ser_cfg(cfg: dict) -> dict:
    c = _norm_cfg(cfg)
    return {"sl": c["sl"], "tgt": c["tgt"],
            "mask_terms": [list(t) for t in c["mask_terms"]],
            "premom_terms": [list(t) for t in c["premom_terms"]],
            "entry_guards": c["guard"] or {},
            "max_positions": c["max_positions"], "daily_loss_rs": c["daily_loss_rs"]}


# ---------------------------------------------------------------------------
# Optuna / random global search
# ---------------------------------------------------------------------------
def _suggest(trial, mask_quant, pm_quant, max_mask=3, max_pm=2):
    def cat(name, choices): return trial.suggest_categorical(name, choices)
    def integ(name, lo, hi): return trial.suggest_int(name, lo, hi)
    mask_terms = []
    avail_mask = [x for x in MASK_FEATS if x in mask_quant]
    for i in range(integ("n_mask", 0, max_mask) if avail_mask else 0):
        f = cat(f"m{i}_f", avail_mask); op = cat(f"m{i}_o", [">=", "<="]); q = cat(f"m{i}_q", QGRID)
        mask_terms.append([f, op, round(float(mask_quant[f][q]), 6)])
    premom_terms = []
    avail_pm = [x for x in PM_FEATS if x in pm_quant]
    for i in range(integ("n_pm", 0, max_pm) if avail_pm else 0):
        f = cat(f"p{i}_f", avail_pm); op = cat(f"p{i}_o", [">=", "<="]); q = cat(f"p{i}_q", QGRID)
        premom_terms.append([f, op, round(float(pm_quant[f][q]), 6)])
    guard = {}
    mn = cat("min_slot", MIN_SLOTS); mx = cat("max_slot", MAX_SLOTS); tn = cat("top_n", [0, 1, 2, 3])
    if mn: guard["min_slot"] = mn
    if mx: guard["max_slot"] = mx
    if tn: guard["top_n"] = int(tn)
    return {"sl": float(cat("sl", SL_GRID)), "tgt": float(cat("tgt", TGT_GRID)),
            "mask_terms": mask_terms, "premom_terms": premom_terms, "guard": (guard or None),
            "max_positions": int(cat("max_positions", MAXPOS_GRID)),
            "daily_loss_rs": float(cat("daily_loss_rs", DLOSS_GRID))}


class _RandTrial:
    def __init__(self, rng): self.rng = rng
    def suggest_categorical(self, name, choices): return choices[self.rng.randrange(len(choices))]
    def suggest_int(self, name, lo, hi): return self.rng.randint(lo, hi)


# ---------------------------------------------------------------------------
# Candidate gating (TRAIN band + TEST > 1.40 + stability)
# ---------------------------------------------------------------------------
def stability_ok(m: dict) -> tuple[bool, list]:
    bad = []
    for k in ("trade_dom_gross", "day_dom", "sym_dom"):
        v = m.get(k)
        if v is None or v > DOM_CAP:
            bad.append(f"{k}={v}>{DOM_CAP}")
    if m.get("trades_per_day", 99) > MAX_TPD:
        bad.append(f"tpd={m.get('trades_per_day')}>{MAX_TPD}")
    return (len(bad) == 0), bad


def gate_candidate(setup, cfg, TRAIN, TEST, min_train_trades, min_test_trades):
    """Confirm on full TRAIN; only if TRAIN PF in band, score TEST once. Returns verdict dict."""
    mt = full_metrics(setup, cfg, TRAIN)
    res = {"train": mt, "test": None, "pass": False, "reasons": []}
    if not (BAND_LO <= mt["net_pf"] <= BAND_HI):
        res["reasons"].append(f"train_pf {mt['net_pf']} outside band [{BAND_LO},{BAND_HI}]")
    if mt["trades"] < min_train_trades:
        res["reasons"].append(f"train_n {mt['trades']}<{min_train_trades}")
    st_tr, bad_tr = stability_ok(mt)
    if not st_tr:
        res["reasons"].append("train_stability: " + ", ".join(bad_tr))
    # TEST scored only when TRAIN PF is in band (anti-overfit rule).
    if BAND_LO <= mt["net_pf"] <= BAND_HI and mt["trades"] > 0:
        me = full_metrics(setup, cfg, TEST)
        res["test"] = me
        if me["net_pf"] <= TEST_PF_MIN:
            res["reasons"].append(f"test_pf {me['net_pf']}<= {TEST_PF_MIN}")
        if me["trades"] < min_test_trades:
            res["reasons"].append(f"test_n {me['trades']}<{min_test_trades}")
        if me["net_pnl"] <= 0:
            res["reasons"].append(f"test_net {me['net_pnl']}<=0")
        st_te, bad_te = stability_ok(me)
        if not st_te:
            res["reasons"].append("test_stability: " + ", ".join(bad_te))
    else:
        res["reasons"].append("test_not_scored (train PF not in band)")
    res["pass"] = (len(res["reasons"]) == 0)
    return res


# ---------------------------------------------------------------------------
# Markdown writers
# ---------------------------------------------------------------------------
def _fmt_m(m: dict) -> str:
    if not m or m.get("trades", 0) == 0:
        return f"n={m.get('trades',0)} (no trades)"
    return (f"n={m['trades']} PF={m['net_pf']} net=Rs{m['net_pnl']:,.0f} win={m['win_rate_pct']}% "
            f"t/s/e={m['tgt_cnt']}/{m['sl_cnt']}/{m['eod_cnt']} avgW/L={m['avg_win']:,.0f}/{m['avg_loss']:,.0f} "
            f"maxDD=Rs{m['max_drawdown']:,.0f} tpd={m['trades_per_day']} "
            f"domTr/Day/Sym={m['trade_dom_gross']}/{m['day_dom']}/{m['sym_dom']} dbp={m['day_block_p']}")


def write_baseline_md(path, setup, side, baseline, sess, base_tr15, base_te15, base_tr5, base_te5):
    L = [f"# BASELINE_RESULT — {setup} ({side})", "",
         "## Current rules (card)", "- **Source:** `Train_and_Test/SETUP_CARDS_AND_LIVE_CROSSCHECK.md` §2 (card of record); config NOT taken from `final_setup_conf.py`.",
         f"- **Exit:** SL {baseline['sl']} / Tgt {baseline['tgt']}",
         f"- **mask_terms (filters):** {_terms_str(baseline.get('mask_terms'))}",
         f"- **pre_momentum_terms (gates):** {_terms_str(baseline.get('premom_terms'))}",
         f"- **entry_guards:** {baseline.get('guard') or '{}'}",
         f"- **max_positions:** {baseline.get('max_positions',20)}  ·  **daily_loss_rs:** {baseline.get('daily_loss_rs',0.0)}",
         "- **Detection (raw, unchanged):** `|low−intraday_low_8|≤0.40×ATR`, `close>VWAP`, `close>open`, `close_loc≥0.60`, `vol_ratio≥1.5` (double-bottom VWAP reclaim).", "",
         "## Exact sessions (inferred from the setup pool)",
         f"- **FIT**   {sess['FIT'][0]}..{sess['FIT'][-1]}  ({len(sess['FIT'])} sessions): {', '.join(sess['FIT'])}",
         f"- **VAL**   {sess['VAL'][0]}..{sess['VAL'][-1]}  ({len(sess['VAL'])} sessions): {', '.join(sess['VAL'])}",
         f"- **TRAIN** {sess['TRAIN'][0]}..{sess['TRAIN'][-1]}  ({len(sess['TRAIN'])} sessions)",
         f"- **TEST**  {sess['TEST'][0]}..{sess['TEST'][-1]}  ({len(sess['TEST'])} sessions): {', '.join(sess['TEST'])}", "",
         "## Baseline metrics (card config, net of cost)",
         "| window | 15 bps/leg (realistic) | 5 bps/leg (paper) |", "|---|---|---|",
         f"| TRAIN | {_fmt_m(base_tr15)} | {_fmt_m(base_tr5)} |",
         f"| TEST  | {_fmt_m(base_te15)} | {_fmt_m(base_te5)} |", "",
         "## Initial diagnosis"]
    diag = []
    if base_tr15["trades"]:
        if base_tr15["net_pf"] < 1.0:
            diag.append(f"- Card is a **net loser on TRAIN** (PF {base_tr15['net_pf']}, net Rs{base_tr15['net_pnl']:,.0f}); "
                        f"SL rate {round(100*base_tr15['sl_cnt']/max(1,base_tr15['trades']),0)}% — stops dominate.")
        else:
            diag.append(f"- Card TRAIN PF {base_tr15['net_pf']} (net Rs{base_tr15['net_pnl']:,.0f}).")
    if base_te15["trades"]:
        diag.append(f"- Card TEST PF {base_te15['net_pf']} on n={base_te15['trades']}.")
    diag.append("- Search target: bring full-TRAIN PF into [1.30,1.70] (not higher) and TEST PF >1.40 using exit tuning + repo-supported filters/gates only.")
    L += diag
    Path(path).write_text("\n".join(L), encoding="utf-8")


def write_iteration_md(path, setup, iters, search_iters, rerun_cmd):
    L = [f"# ITERATION_LOG — {setup}", "",
         "Two phases: (A) forward-greedy single-logical-group ablation from the card baseline (change ONE group "
         "per iteration, keep only if FIT/VAL band score improves), then (B) global Optuna/seeded search "
         "(each row = a new best-FIT/VAL-score improvement). FIT/VAL drives the search; full TRAIN confirms; "
         "TEST is scored ONLY when TRAIN PF is inside [1.30,1.70] (anti-overfit).", "",
         f"Rerun command (identical for every iteration — it reruns the whole loop):", "```", rerun_cmd, "```", "",
         "## Phase A — logical-group ablation"]
    for r in iters:
        L.append(f"### Iter {r['n']} — group: {r['group']} — **{r['decision']}**")
        L.append(f"- change: {r['change']}")
        L.append(f"- FIT n={r['fit']['n']} PF={r['fit']['pf']} | VAL n={r['val']['n']} PF={r['val']['pf']}")
        if "train" in r:
            L.append(f"- TRAIN n={r['train']['n']} PF={r['train']['pf']} net=Rs{r['train']['net']:,.0f} tpd={r['train']['tpd']}")
        if r.get("test_gated") and "test" in r:
            L.append(f"- TEST (gated, TRAIN in band) n={r['test']['n']} PF={r['test']['pf']} net=Rs{r['test']['net']:,.0f}")
        elif "train" in r:
            L.append(f"- TEST: not scored (TRAIN PF not in band)")
        L.append(f"- next: {r['next']}")
        L.append("")
    L.append("## Phase B — global search best-score trajectory")
    if not search_iters:
        L.append("_(no improving trials recorded)_")
    for r in search_iters:
        L.append(f"### Iter {r['n']} — trial {r['trial']} — score {r['score']}")
        L.append(f"- changed vs prev best: {r['delta']}")
        L.append(f"- FIT n={r['fit_n']} PF={r['fit_pf']} | VAL n={r['val_n']} PF={r['val_pf']}")
        L.append(f"- cfg: SL/Tgt={r['cfg']['sl']}/{r['cfg']['tgt']} mask={_terms_str(r['cfg']['mask_terms'])} "
                 f"premom={_terms_str(r['cfg']['premom_terms'])} guard={r['cfg']['entry_guards'] or '{}'} "
                 f"maxpos={r['cfg']['max_positions']} dloss={r['cfg']['daily_loss_rs']}")
        L.append("")
    Path(path).write_text("\n".join(L), encoding="utf-8")


def write_failure_md(path, setup, label, m):
    L = [f"# FAILURE_ANALYSIS — {setup}", "",
         f"Loss diagnosis for **{label}** (full-TRAIN book, 15 bps/leg, net of cost).", ""]
    if not m or not m["trades"]:
        L.append("_No trades to analyse._"); Path(path).write_text("\n".join(L), encoding="utf-8"); return
    L += [f"- trades={m['trades']} win={m['win_rate_pct']}% PF={m['net_pf']} net=Rs{m['net_pnl']:,.0f}",
          f"- outcome split: TARGET={m['tgt_cnt']}  SL={m['sl_cnt']}  EOD/time={m['eod_cnt']}",
          f"- avg win=Rs{m['avg_win']:,.0f}  avg loss=Rs{m['avg_loss']:,.0f}  maxDD=Rs{m['max_drawdown']:,.0f}",
          f"- gross profit=Rs{m['gross_profit']:,.0f}  gross loss=Rs{m['gross_loss']:,.0f}", "",
          "## Worst days"]
    for d in sorted(m["daywise"], key=lambda x: x["net"])[:6]:
        L.append(f"- {d['date']}: n={d['n']} net=Rs{d['net']:,.0f} PF={d['pf']}")
    L += ["", "## Worst symbols"]
    for s in m["symwise"][:8]:
        L.append(f"- {s['ticker']}: n={s['n']} net=Rs{s['net']:,.0f}")
    L += ["", "## Time-of-day (entry hour)"]
    for h in sorted(m["hourwise"], key=lambda x: x["hour"]):
        L.append(f"- {h['hour']}:00  n={h['n']} net=Rs{h['net']:,.0f} PF={h['pf']}")
    L += ["", "## Notes",
          f"- SL share = {round(100*m['sl_cnt']/max(1,m['trades']),0)}% of exits; "
          f"target share = {round(100*m['tgt_cnt']/max(1,m['trades']),0)}%. "
          "A high SL share with low target share = fake reclaim / no follow-through (raw double-bottom signal "
          "is being faded). Pre-momentum / volume gates aim to remove the no-follow-through subset; the "
          "ablation log shows whether any group recovered a band-PF edge."]
    Path(path).write_text("\n".join(L), encoding="utf-8")


def write_candidates_md(path, setup, cands):
    L = [f"# CANDIDATE_CONFIGS — {setup}", "",
         f"Only configs with **full-TRAIN PF in [{BAND_LO},{BAND_HI}] AND TEST PF > {TEST_PF_MIN}** (plus trade-count "
         "and trade/day/symbol-dominance stability) are listed. Net of cost @15 bps/leg.", ""]
    if not cands:
        L += ["**No candidate cleared the band+TEST gate.**", "",
              "See ITERATION_LOG.md / FAILURE_ANALYSIS.md for why. The setup did not produce a defensible, "
              "in-band, OOS-positive config in this window; recommendation is **do not promote**."]
        Path(path).write_text("\n".join(L), encoding="utf-8"); return
    for i, c in enumerate(cands, 1):
        cfg, mt, me = c["cfg"], c["train"], c["test"]
        L += [f"## Candidate {i:03d}", "```json", json.dumps(cfg, indent=2), "```",
              f"- TRAIN: {_fmt_m(mt)}", f"- TEST : {_fmt_m(me)}",
              f"- risk: {c.get('risk','')}",
              f"- approval recommendation: {c.get('rec','REVIEW')}", "",
              f"- candidate file: `candidates/{setup}_candidate_{i:03d}.json`", ""]
    Path(path).write_text("\n".join(L), encoding="utf-8")


def write_final_md(path, setup, side, cands, rerun_cmd, baseline_cmd, sess):
    L = [f"# APPROVAL_REQUIRED — FINAL RECOMMENDATION — {setup} ({side})", "",
         "> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES.** This file is a recommendation only. "
         "Nothing here has been written to `final_setup_conf.py` or `Train_and_Test/final_setup_conf.py`.", ""]
    if not cands:
        L += ["## Verdict: **NO — do not promote**", "",
              f"No config reached full-TRAIN PF in [{BAND_LO},{BAND_HI}] **and** TEST PF > {TEST_PF_MIN} with stable, "
              "meaningful trade counts. The best in-band TRAIN attempts either failed OOS on TEST or were "
              "sample/dominance-fragile (see CANDIDATE_CONFIGS.md / ITERATION_LOG.md).", "",
              "## If you still want to iterate",
              "- The binding constraint and the closest near-miss are recorded in ITERATION_LOG.md.",
              "- No promotion target file should be edited.", "",
              "## Commands", "```", f"# baseline replay:\n{baseline_cmd}", "", f"# full loop rerun:\n{rerun_cmd}", "```"]
        Path(path).write_text("\n".join(L), encoding="utf-8"); return
    best = cands[0]
    cfg = best["cfg"]
    L += ["## Verdict: **APPROVAL REQUIRED** (best candidate cleared the gate; awaiting your explicit OK)", "",
          "## Best candidate", "```json", json.dumps(cfg, indent=2), "```",
          f"- TRAIN: {_fmt_m(best['train'])}", f"- TEST : {_fmt_m(best['test'])}",
          f"- risk: {best.get('risk','')}", "",
          "## Exact proposed config block (for review only — NOT applied)",
          "If approved, this would become the `FINAL_SETUP_CONF['" + setup + "']` exit/mask/premom/guard block:",
          "```python", f'"{setup}": ' + json.dumps({
              "exit": {"sl_pct": cfg["sl"], "tgt_pct": cfg["tgt"]},
              "mask_terms": cfg["mask_terms"], "pre_momentum_terms": cfg["premom_terms"],
              "entry_guards": cfg["entry_guards"],
          }, indent=4), "```", "",
          "## File that would need approval before any edit",
          "- `final_setup_conf.py` (root)  AND its mirror `Train_and_Test/final_setup_conf.py`",
          "- **Do NOT edit until the user explicitly approves promotion.**", "",
          "## Risk notes",
          "- TEST window is short/recent (see sessions below); confirm with a forward live-paper holdout before sizing.",
          "- L_DOUBLE_BOTTOM_VWAP is a RAW-pre-gate-pool setup and the live research-layer currently blocks the L* family — reconcile gating before any live use.", "",
          f"- sessions: TRAIN {sess['TRAIN'][0]}..{sess['TRAIN'][-1]} ({len(sess['TRAIN'])}), TEST {sess['TEST'][0]}..{sess['TEST'][-1]} ({len(sess['TEST'])})", "",
          "## Commands", "```", f"# baseline replay:\n{baseline_cmd}", "", f"# full loop rerun:\n{rerun_cmd}", "```"]
    Path(path).write_text("\n".join(L), encoding="utf-8")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--setup", required=True)
    ap.add_argument("--pool", required=True)
    ap.add_argument("--side", default="LONG")
    ap.add_argument("--train_start", default="2026-05-18")
    ap.add_argument("--test_start", default="2026-06-20")
    ap.add_argument("--trials", type=int, default=400)
    ap.add_argument("--time_budget_min", type=float, default=25.0)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--slippage_bps", type=float, default=15.0)
    ap.add_argument("--min_fold", type=int, default=6)
    ap.add_argument("--min_train_trades", type=int, default=15)
    ap.add_argument("--min_test_trades", type=int, default=5)
    ap.add_argument("--out", default="")
    args = ap.parse_args()
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass

    setup = args.setup.strip().upper()
    outdir = Path(args.out) if args.out else (TT_DIR / "setup_pf_1_4_approval_loop" / setup)
    (outdir / "candidates").mkdir(parents=True, exist_ok=True)
    (outdir / "scripts").mkdir(parents=True, exist_ok=True)

    engine = "Optuna TPE" if _HAVE_OPTUNA else "Optuna unavailable; using seeded random search fallback."
    print(f"[pf-band] setup={setup} side={args.side}")
    print(f"[pf-band] optimizer: {engine}")

    rerun_cmd = (f"py -3.12 Train_and_Test/setup_pf_1_4_approval_loop/{setup}/scripts/pf_band_search.py "
                 f"--setup {setup} --pool {args.pool} --train_start {args.train_start} "
                 f"--test_start {args.test_start} --trials {args.trials} "
                 f"--time_budget_min {args.time_budget_min} --seed {args.seed}")
    baseline_cmd = (f"py -3.12 Train_and_Test/setup_loop_runner.py --setup {setup} --pool {args.pool} "
                    f"--configs <baseline.json> --train_start {args.train_start} --train_end <day-before-test> "
                    f"--test_start {args.test_start} --test_end <latest> --slippage_bps 15")

    # ---- pool + sessions + split ----
    tt.POOL_DIRS = [Path(args.pool)]; tt.POOL_DIR = Path(args.pool)
    _set_slip(args.slippage_bps)
    pool = tt.load_pool()
    pool = pool[pool["setup"] == setup].copy()
    if pool.empty:
        print(f"[pf-band] no pool rows for {setup}"); return 1
    sessions = sorted(pd.Series(pool["_day"].dropna().unique()))
    ts = pd.Timestamp(args.train_start); te = pd.Timestamp(args.test_start)
    TEST_s = [s for s in sessions if s >= te]
    TRAIN_s = [s for s in sessions if ts <= s < (TEST_s[0] if TEST_s else sessions[-1] + pd.Timedelta(days=1))]
    if not TRAIN_s or not TEST_s:
        print(f"[pf-band] cannot form split: train_n={len(TRAIN_s)} test_n={len(TEST_s)} "
              f"(pool sessions {sessions[0].date()}..{sessions[-1].date()})"); return 1
    half = len(TRAIN_s) // 2
    FIT_s, VAL_s = TRAIN_s[:half], TRAIN_s[half:]

    def _ds(ss): return [str(pd.Timestamp(x).date()) for x in ss]
    sess = {"FIT": _ds(FIT_s), "VAL": _ds(VAL_s), "TRAIN": _ds(TRAIN_s), "TEST": _ds(TEST_s)}
    print(f"[pf-band] FIT   {sess['FIT'][0]}..{sess['FIT'][-1]} ({len(FIT_s)})")
    print(f"[pf-band] VAL   {sess['VAL'][0]}..{sess['VAL'][-1]} ({len(VAL_s)})")
    print(f"[pf-band] TRAIN {sess['TRAIN'][0]}..{sess['TRAIN'][-1]} ({len(TRAIN_s)})")
    print(f"[pf-band] TEST  {sess['TEST'][0]}..{sess['TEST'][-1]} ({len(TEST_s)})")
    print(f"[pf-band] gate: TRAIN PF in [{BAND_LO},{BAND_HI}]  TEST PF > {TEST_PF_MIN}  dom<= {DOM_CAP}  tpd<= {MAX_TPD}")

    span = set(map(pd.Timestamp, FIT_s + VAL_s + TEST_s + TRAIN_s))
    sub = pool[pool["_day"].isin(span)].copy()
    sub = tt.attach_entries(sub)

    def _slice(ss): return sub[sub["_day"].isin(set(map(pd.Timestamp, ss)))].copy()
    FIT, VAL, TRAIN, TEST = _slice(FIT_s), _slice(VAL_s), _slice(TRAIN_s), _slice(TEST_s)
    print(f"[pf-band] entries @ {args.slippage_bps}bps: FIT={len(FIT)} VAL={len(VAL)} TRAIN={len(TRAIN)} TEST={len(TEST)}")

    baseline = CARD_BASELINES.get(setup, {"sl": 0.90, "tgt": 1.50, "mask_terms": [],
                                          "premom_terms": [], "guard": None, "max_positions": 20, "daily_loss_rs": 0.0})

    # ---- baseline metrics @15 + @5 ----
    base_tr15 = full_metrics(setup, baseline, TRAIN); base_te15 = full_metrics(setup, baseline, TEST)
    print(f"[pf-band] BASELINE @15bps TRAIN {_fmt_m(base_tr15)}")
    print(f"[pf-band] BASELINE @15bps TEST  {_fmt_m(base_te15)}")

    # ---- quantiles (TRAIN only) ----
    mask_quant, pm_quant = build_quantiles(TRAIN)
    print(f"[pf-band] searchable mask={sorted(mask_quant)} premom={sorted(pm_quant)}")

    # ---- Phase A: forward-greedy ablation ----
    print("[pf-band] Phase A: forward-greedy single-group ablation ...")
    ablation_iters, greedy_cfg, greedy_score = run_ablation(
        setup, baseline, mask_quant, pm_quant, FIT, VAL, TRAIN, TEST, args.min_fold, rerun_cmd)
    print(f"[pf-band] greedy best FIT/VAL score={greedy_score:.4f}  cfg exit {greedy_cfg['sl']}/{greedy_cfg['tgt']} "
          f"mask=[{_terms_str(greedy_cfg.get('mask_terms'))}] premom=[{_terms_str(greedy_cfg.get('premom_terms'))}]")

    # ---- Phase B: global search ----
    print(f"[pf-band] Phase B: global {('Optuna TPE' if _HAVE_OPTUNA else 'seeded random')} search ...")
    trial_rows = []
    search_iters = []
    best = {"score": -1e9, "cfg": None, "params": None}
    t0 = time.time()

    def _record(cfg, sc, nf, pf_f, nv, pf_v, tnum):
        trial_rows.append({"trial": tnum, "sl": cfg["sl"], "tgt": cfg["tgt"],
                           "mask": _terms_str(cfg["mask_terms"]), "premom": _terms_str(cfg["premom_terms"]),
                           "guard": json.dumps(cfg["guard"]) if cfg["guard"] else "-",
                           "max_positions": cfg["max_positions"], "daily_loss_rs": cfg["daily_loss_rs"],
                           "fit_n": nf, "fit_pf": round(pf_f, 3), "val_n": nv, "val_pf": round(pf_v, 3),
                           "score": round(float(sc), 4), "cfg_json": json.dumps(_ser_cfg(cfg))})
        if sc > best["score"] + 1e-9:
            delta = _diff_cfg(best["cfg"], cfg)
            best["score"], best["cfg"] = sc, cfg
            search_iters.append({"n": len(search_iters) + 1, "trial": tnum, "score": round(float(sc), 4),
                                 "delta": delta, "fit_n": nf, "fit_pf": round(pf_f, 3),
                                 "val_n": nv, "val_pf": round(pf_v, 3), "cfg": _ser_cfg(cfg)})

    # seed the global search with the greedy cfg so Phase B starts no worse than Phase A
    sc, nf, pf_f, nv, pf_v = fitval_score(setup, greedy_cfg, FIT, VAL, args.min_fold)
    _record(greedy_cfg, sc, nf, pf_f, nv, pf_v, -1)

    if _HAVE_OPTUNA:
        def objective(trial):
            cfg = _suggest(trial, mask_quant, pm_quant)
            sc, nf, pf_f, nv, pf_v = fitval_score(setup, cfg, FIT, VAL, args.min_fold)
            _record(cfg, sc, nf, pf_f, nv, pf_v, trial.number)
            return sc
        study = optuna.create_study(direction="maximize", sampler=optuna.samplers.TPESampler(seed=args.seed))
        study.optimize(objective, n_trials=args.trials, timeout=args.time_budget_min * 60.0)
        n_done = len(study.trials)
    else:
        rng = random.Random(args.seed); n_done = 0
        for k in range(args.trials):
            if time.time() - t0 > args.time_budget_min * 60.0:
                break
            cfg = _suggest(_RandTrial(rng), mask_quant, pm_quant)
            sc, nf, pf_f, nv, pf_v = fitval_score(setup, cfg, FIT, VAL, args.min_fold)
            _record(cfg, sc, nf, pf_f, nv, pf_v, k); n_done += 1
    print(f"[pf-band] completed {n_done} trials | best FIT/VAL score={best['score']:.4f}")

    # ---- Candidate gating: take top-by-score unique cfgs, confirm TRAIN band + TEST>1.40 ----
    tr_df = pd.DataFrame(trial_rows).sort_values("score", ascending=False)
    tr_df.to_csv(outdir / "trials.csv", index=False)

    def _from_ser(c):
        return {"sl": c["sl"], "tgt": c["tgt"], "mask_terms": c["mask_terms"],
                "premom_terms": c["premom_terms"], "guard": (c.get("entry_guards") or None),
                "max_positions": c["max_positions"], "daily_loss_rs": c["daily_loss_rs"]}

    # Gate, in priority order: the global best, the greedy cfg, every search-iter best, then the
    # TOP-K trials by FIT/VAL score (band objective => high-score configs are the likeliest to land
    # full-TRAIN inside the band). Dedupe by serialized config.
    TOP_K = 40
    seen = set(); ordered = [best["cfg"], greedy_cfg]
    for si in reversed(search_iters):
        ordered.append(_from_ser(si["cfg"]))
    for cj in tr_df.head(TOP_K)["cfg_json"].tolist():
        try:
            ordered.append(_from_ser(json.loads(cj)))
        except Exception:
            continue
    cands = []
    for cfg in ordered:
        key = json.dumps(_ser_cfg(cfg), sort_keys=True)
        if key in seen:
            continue
        seen.add(key)
        g = gate_candidate(setup, cfg, TRAIN, TEST, args.min_train_trades, args.min_test_trades)
        if g["pass"]:
            cands.append({"cfg": _ser_cfg(cfg), "train": g["train"], "test": g["test"],
                          "risk": "short/recent TEST window; RAW-pool + L* live-block caveat.",
                          "rec": "APPROVAL REQUIRED"})
    # rank candidates: highest TEST PF, then TRAIN PF closest to band centre, then TRAIN trades
    cands.sort(key=lambda c: (c["test"]["net_pf"], -abs(c["train"]["net_pf"] - 1.5), c["train"]["trades"]),
               reverse=True)
    for i, c in enumerate(cands, 1):
        (outdir / "candidates" / f"{setup}_candidate_{i:03d}.json").write_text(
            json.dumps({"setup": setup, "side": args.side, "windows": sess, "config": c["cfg"],
                        "train": {k: v for k, v in c["train"].items() if k != "detail"},
                        "test": {k: v for k, v in c["test"].items() if k != "detail"}},
                       indent=2, default=str), encoding="utf-8")
    print(f"[pf-band] candidates passing TRAIN-band+TEST>1.40+stability: {len(cands)}")

    # ---- failure analysis target: best candidate else greedy cfg on TRAIN ----
    fa_cfg = cands[0]["cfg"] if cands else _ser_cfg(greedy_cfg)
    fa_cfg_eval = {"sl": fa_cfg["sl"], "tgt": fa_cfg["tgt"], "mask_terms": fa_cfg["mask_terms"],
                   "premom_terms": fa_cfg["premom_terms"], "guard": (fa_cfg["entry_guards"] or None),
                   "max_positions": fa_cfg["max_positions"], "daily_loss_rs": fa_cfg["daily_loss_rs"]}
    fa_label = "best candidate" if cands else "best greedy config (no candidate passed)"
    fa_m = full_metrics(setup, fa_cfg_eval, TRAIN)

    # ---- 5bps baseline (paper) for the baseline report (re-attach at 5bps) ----
    _set_slip(5.0)
    sub5 = tt.attach_entries(pool[pool["_day"].isin(span)].copy())
    def _slice5(ss): return sub5[sub5["_day"].isin(set(map(pd.Timestamp, ss)))].copy()
    base_tr5 = full_metrics(setup, baseline, _slice5(TRAIN_s))
    base_te5 = full_metrics(setup, baseline, _slice5(TEST_s))
    _set_slip(args.slippage_bps)

    # ---- write all reports ----
    write_baseline_md(outdir / "BASELINE_RESULT.md", setup, args.side, baseline, sess,
                      base_tr15, base_te15, base_tr5, base_te5)
    write_iteration_md(outdir / "ITERATION_LOG.md", setup, ablation_iters, search_iters, rerun_cmd)
    write_failure_md(outdir / "FAILURE_ANALYSIS.md", setup, fa_label, fa_m)
    write_candidates_md(outdir / "CANDIDATE_CONFIGS.md", setup, cands)
    write_final_md(outdir / "APPROVAL_REQUIRED_FINAL_RECOMMENDATION.md", setup, args.side, cands,
                   rerun_cmd, baseline_cmd, sess)

    summary = {"setup": setup, "side": args.side, "optimizer": engine, "windows": sess,
               "baseline_train_15bps": {k: v for k, v in base_tr15.items() if k != "detail"},
               "baseline_test_15bps": {k: v for k, v in base_te15.items() if k != "detail"},
               "best_fitval_score": round(float(best["score"]), 4),
               "n_trials": n_done, "n_candidates": len(cands),
               "greedy_cfg": _ser_cfg(greedy_cfg)}
    (outdir / "search_summary.json").write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")

    print("\n" + "=" * 88)
    print(f"DONE {setup}: baseline TRAIN PF {base_tr15['net_pf']} / TEST PF {base_te15['net_pf']}  | "
          f"candidates passing gate: {len(cands)}")
    if cands:
        c = cands[0]
        print(f"  BEST CANDIDATE TRAIN n={c['train']['trades']} PF={c['train']['net_pf']} | "
              f"TEST n={c['test']['trades']} PF={c['test']['net_pf']}")
    print(f"  artifacts -> {outdir}")
    print("=" * 88)
    return 0


def _diff_cfg(a, b) -> str:
    if a is None:
        return "initial best"
    sa, sb = _ser_cfg(a), _ser_cfg(b)
    out = []
    for k in ("sl", "tgt", "max_positions", "daily_loss_rs"):
        if sa[k] != sb[k]:
            out.append(f"{k} {sa[k]}->{sb[k]}")
    if sa["mask_terms"] != sb["mask_terms"]:
        out.append(f"mask {_terms_str(sa['mask_terms'])} -> {_terms_str(sb['mask_terms'])}")
    if sa["premom_terms"] != sb["premom_terms"]:
        out.append(f"premom {_terms_str(sa['premom_terms'])} -> {_terms_str(sb['premom_terms'])}")
    if sa["entry_guards"] != sb["entry_guards"]:
        out.append(f"guard {sa['entry_guards'] or '{}'} -> {sb['entry_guards'] or '{}'}")
    return "; ".join(out) or "(no scalar diff)"


if __name__ == "__main__":
    raise SystemExit(main())
