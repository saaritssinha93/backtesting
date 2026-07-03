r"""optimize_ppdh.py — single-setup approval-loop optimizer for P_PDH_BREAK_RETEST_LONG (LONG).

Reuses the repo pipeline (setup_train_test = entry + 1-min resolve + statutory/flat cost +
family dedupe + pre-momentum + mask + portfolio overlay). NO scanner logic invented; only
repo-supported knobs are tuned: exit SL/Tgt, mask_terms (pool signal columns), pre_momentum_terms,
entry_guards (min_slot/max_slot/top_n), max_positions, daily_loss_rs, regime_align/band.

Protocol (anti-overfit):
  - windows inferred from the setup pool's sorted distinct sessions.
    TEST  = last --test_n sessions (held out; the requested 2026-06-20+ window is unavailable
            in any P_PDH pool, so we use the nearest-available recent block and PRINT it).
    TRAIN = the --train_n sessions immediately before TEST.
    FIT   = first half of TRAIN sessions ; VAL = second half.
  - SEARCH (Optuna TPE if available, else seeded random) optimizes ONLY on FIT/VAL:
        score = min(FIT_PF, VAL_PF) - GAP_LAMBDA*|FIT_PF - VAL_PF|, subject to per-window
        trade floors + trades/day + dominance caps.
  - A hand-iteration grid (deterministic, one logical group changed at a time) is ALSO run and
    logged, so the iteration log is explainable.
  - CONFIRM: every distinct config (hand + Optuna) is scored on FULL TRAIN. A config is a
    CANDIDATE only if full-TRAIN PF in [train_pf_min, train_pf_max] AND TEST PF > test_pf_min
    AND it passes stability (dominance) checks. TEST is scored once per config, after the search.

Outputs (all under the approval-loop folder): run_summary.json, iterations.csv, optuna_trials.csv,
candidates/*.json, baseline_*.csv, candidate_*_trades.csv. No final_setup_conf.py edit. No live trades.
"""
from __future__ import annotations

import argparse
import json
import math
import random
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
_SETUP_DIR = _HERE.parent
# scripts/ -> P_PDH.../ -> setup_pf_1_4_approval_loop/ -> Train_and_Test/ -> repo root
_TT = _SETUP_DIR.parent.parent
for _p in (str(_TT.parent), str(_TT)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import setup_train_test as tt  # noqa: E402

try:
    import optuna  # noqa: E402
    optuna.logging.set_verbosity(optuna.logging.WARNING)
    HAVE_OPTUNA = True
except Exception:
    HAVE_OPTUNA = False

SETUP = "P_PDH_BREAK_RETEST_LONG"
POOL = _SETUP_DIR / "pool"

# ---- demoted baseline (final_setup_conf RESEARCH_WATCH / demotion block) ----
BASELINE = {
    "status": "OK", "sl": 0.50, "tgt": 0.60,
    "mask_terms": [("body_pct", "<=", 0.749993)],
    "premom_terms": [("pre_entry_momentum_score", ">=", 75.071712),
                     ("pre3_range_r", ">=", 0.499787)],
    "guard": None,
}

# Search universe (anti-overfit: drop market_ret/signal_minute/notional vectors).
BAN = {"market_ret_pct", "market_abs_ret_pct", "signal_minute", "notional"}
MASK_FEATS = [f for f in ["rs_pct", "vol_ratio", "atr_pct", "body_pct", "close_loc",
                          "vwap_dist_atr", "quality_score", "ranker_score", "signal_range_pct",
                          "upper_wick_pct", "lower_wick_pct", "wick_skew_pct"] if f not in BAN]
PM_FEATS = [f for f in ["pre_entry_momentum_score", "sig5_adx_calc", "sig5_rsi_dir",
                        "sig5_vol_ratio20", "pre1_adx", "pre3_range_r", "pre5_mom_r",
                        "pre3_close_pos"] if f not in BAN]
QGRID = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
SL_GRID = [0.50, 0.60, 0.70, 0.85, 1.00, 1.20]
TGT_GRID = [0.60, 0.80, 1.00, 1.25, 1.50, 2.00, 2.50]


# ---------------------------------------------------------------------------
# metric battery
# ---------------------------------------------------------------------------
def full_metrics(cfg: dict, df: pd.DataFrame) -> dict:
    tt.MAX_POSITIONS = int(cfg.get("max_positions", 20))
    tt.DAILY_LOSS_RS = float(cfg.get("daily_loss_rs", 0.0))
    tt.REGIME_ALIGN = bool(cfg.get("regime_align", False))
    tt.REGIME_BAND = float(cfg.get("regime_band", 0.0))
    fam = tt.eval_family({SETUP: cfg}, df)
    exits = {SETUP: (float(cfg["sl"]), float(cfg["tgt"]))}
    det = tt.book_detail(fam["book"], exits) if fam["trades"] else pd.DataFrame()
    m = {
        "trades": int(fam["trades"]), "net_pf": round(float(fam["net_pf"]), 4),
        "net_pnl": round(float(fam["net_pnl"]), 0),
        "day_block_p": (None if not np.isfinite(fam["day_block_p"]) else round(float(fam["day_block_p"]), 4)),
        "wins": 0, "losses": 0, "win_pct": 0.0, "gross_profit": 0.0, "gross_loss": 0.0,
        "avg_win": 0.0, "avg_loss": 0.0, "win_loss_ratio": None, "max_drawdown": 0.0,
        "n_days": 0, "n_syms": 0, "trades_per_day": 0.0, "max_trades_day": 0,
        "sl_cnt": 0, "tgt_cnt": 0, "eod_cnt": 0,
        "top_trade_gross_share": None, "top_day_net_share": None, "top_symbol_net_share": None,
        "worst_day": None, "worst_day_net": None, "worst_sym": None, "worst_sym_net": None,
    }
    if det.empty:
        return m
    net = det["net_pnl_rs"].astype(float)
    gross = det["gross_pnl_rs"].astype(float)
    oc = det["outcome"].astype(str).str.upper()
    wins, losses = net[net > 0], net[net <= 0]
    gp = float(gross[gross > 0].sum()); gl = float(-gross[gross < 0].sum())
    m["wins"] = int((net > 0).sum()); m["losses"] = int((net <= 0).sum())
    m["win_pct"] = round(float((net > 0).mean()) * 100, 1)
    m["gross_profit"] = round(gp, 0); m["gross_loss"] = round(gl, 0)
    m["avg_win"] = round(float(wins.mean()), 0) if len(wins) else 0.0
    m["avg_loss"] = round(float(losses.mean()), 0) if len(losses) else 0.0
    m["win_loss_ratio"] = (round(abs(m["avg_win"] / m["avg_loss"]), 2)
                           if m["avg_loss"] else None)
    eq = net.cumsum(); dd = eq - eq.cummax()
    m["max_drawdown"] = round(float(dd.min()), 0) if len(dd) else 0.0
    m["n_days"] = int(det["trade_date"].nunique()); m["n_syms"] = int(det["ticker"].nunique())
    m["trades_per_day"] = round(m["trades"] / max(1, m["n_days"]), 2)
    m["max_trades_day"] = int(det.groupby("trade_date").size().max())
    m["sl_cnt"] = int((oc == "SL").sum()); m["tgt_cnt"] = int((oc == "TARGET").sum())
    m["eod_cnt"] = int((~oc.isin(["SL", "TARGET"])).sum())
    if gp > 0:
        m["top_trade_gross_share"] = round(float(gross[gross > 0].max()) / gp, 3)
    tot = float(net.sum())
    day_net = det.groupby("trade_date")["net_pnl_rs"].sum()
    sym_net = det.groupby("ticker")["net_pnl_rs"].sum()
    if tot > 0:
        m["top_day_net_share"] = round(float(day_net.max()) / tot, 3)
        m["top_symbol_net_share"] = round(float(sym_net.max()) / tot, 3)
    m["worst_day"] = str(day_net.idxmin()); m["worst_day_net"] = round(float(day_net.min()), 0)
    m["worst_sym"] = str(sym_net.idxmin()); m["worst_sym_net"] = round(float(sym_net.min()), 0)
    return m, det


def _m(cfg, df):
    r = full_metrics(cfg, df)
    return r[0] if isinstance(r, tuple) else r


def fast_pf(cfg, df):
    tt.MAX_POSITIONS = int(cfg.get("max_positions", 20))
    tt.DAILY_LOSS_RS = float(cfg.get("daily_loss_rs", 0.0))
    tt.REGIME_ALIGN = bool(cfg.get("regime_align", False))
    tt.REGIME_BAND = float(cfg.get("regime_band", 0.0))
    fam = tt.eval_family({SETUP: cfg}, df)
    book = fam.get("book")
    nd = int(pd.Series(book["_day"]).nunique()) if (book is not None and len(book)) else 0
    return int(fam["trades"]), float(fam["net_pf"]), nd


# ---------------------------------------------------------------------------
# config helpers
# ---------------------------------------------------------------------------
def cfg_key(cfg: dict) -> str:
    return json.dumps({
        "sl": cfg["sl"], "tgt": cfg["tgt"],
        "mask": sorted([list(t) for t in cfg.get("mask_terms", [])], key=str),
        "pm": sorted([list(t) for t in cfg.get("premom_terms", [])], key=str),
        "guard": cfg.get("guard") or {},
        "max_positions": cfg.get("max_positions", 20),
        "daily_loss_rs": cfg.get("daily_loss_rs", 0.0),
        "regime_align": cfg.get("regime_align", False),
        "regime_band": cfg.get("regime_band", 0.0),
    }, sort_keys=True)


def mk(sl, tgt, mask=None, pm=None, guard=None, **kw):
    return {"status": "OK", "sl": float(sl), "tgt": float(tgt),
            "mask_terms": [tuple(t) for t in (mask or [])],
            "premom_terms": [tuple(t) for t in (pm or [])],
            "guard": guard or None,
            "max_positions": kw.get("max_positions", 20),
            "daily_loss_rs": kw.get("daily_loss_rs", 0.0),
            "regime_align": kw.get("regime_align", False),
            "regime_band": kw.get("regime_band", 0.0)}


def cfg_public(cfg):
    return {"exit": {"sl_pct": cfg["sl"], "tgt_pct": cfg["tgt"]},
            "mask_terms": [list(t) for t in cfg.get("mask_terms", [])],
            "pre_momentum_terms": [list(t) for t in cfg.get("premom_terms", [])],
            "entry_guards": cfg.get("guard") or {},
            "max_positions": cfg.get("max_positions", 20),
            "daily_loss_rs": cfg.get("daily_loss_rs", 0.0),
            "regime_align": cfg.get("regime_align", False),
            "regime_band": cfg.get("regime_band", 0.0)}


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def main() -> int:
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument("--test_n", type=int, default=9)
    ap.add_argument("--train_n", type=int, default=27)
    ap.add_argument("--trials", type=int, default=400)
    ap.add_argument("--time_budget_min", type=float, default=18.0)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--slippage_bps", type=float, default=15.0)
    ap.add_argument("--train_pf_min", type=float, default=1.30)
    ap.add_argument("--train_pf_max", type=float, default=1.70)
    ap.add_argument("--test_pf_min", type=float, default=1.40)
    ap.add_argument("--min_train_trades", type=int, default=30)
    ap.add_argument("--min_test_trades", type=int, default=15)
    ap.add_argument("--min_fv_trades", type=int, default=8)
    ap.add_argument("--dom_cap", type=float, default=0.45)
    ap.add_argument("--max_trades_day", type=float, default=5.0)
    ap.add_argument("--gap_lambda", type=float, default=0.5)
    ap.add_argument("--max_mask_terms", type=int, default=2)
    ap.add_argument("--max_pm_terms", type=int, default=2)
    args = ap.parse_args()

    engine = "Optuna TPE" if HAVE_OPTUNA else "Optuna unavailable; using seeded random search fallback."
    print(f"[opt] setup={SETUP}  optimizer: {engine}")

    tt.SLIPPAGE_BPS = float(args.slippage_bps)
    tt._entry.cache_clear(); tt._resolve_full.cache_clear(); tt._premom.cache_clear()
    tt.POOL_DIRS = [POOL]; tt.POOL_DIR = POOL
    pool = tt.load_pool()
    pool = pool[pool["setup"] == SETUP].copy()
    sessions = sorted(pd.Series(pool["_day"].dropna().unique()))
    if len(sessions) < args.test_n + args.train_n:
        print(f"[opt] WARN only {len(sessions)} sessions; need {args.test_n+args.train_n}")
    TEST_s = sessions[-args.test_n:]
    TRAIN_s = sessions[-(args.test_n + args.train_n):-args.test_n]
    half = len(TRAIN_s) // 2
    FIT_s, VAL_s = TRAIN_s[:half], TRAIN_s[half:]

    def rng(ss):
        return f"{pd.Timestamp(ss[0]).date()}..{pd.Timestamp(ss[-1]).date()}"
    print(f"[opt] requested user windows: TRAIN 2026-05-18.. / TEST 2026-06-20.. "
          f"-> UNAVAILABLE in any P_PDH pool (data ends {pd.Timestamp(sessions[-1]).date()}).")
    print(f"[opt] NEAREST-AVAILABLE sessions used:")
    print(f"[opt]   FIT   {rng(FIT_s)}  ({len(FIT_s)} sessions)  {[str(pd.Timestamp(s).date()) for s in FIT_s]}")
    print(f"[opt]   VAL   {rng(VAL_s)}  ({len(VAL_s)} sessions)  {[str(pd.Timestamp(s).date()) for s in VAL_s]}")
    print(f"[opt]   TRAIN {rng(TRAIN_s)} ({len(TRAIN_s)} sessions)")
    print(f"[opt]   TEST  {rng(TEST_s)} ({len(TEST_s)} sessions)  {[str(pd.Timestamp(s).date()) for s in TEST_s]}")

    span = set(map(pd.Timestamp, list(TRAIN_s) + list(TEST_s)))
    sub = pool[pool["_day"].isin(span)].copy()
    sub = tt.attach_entries(sub)
    print(f"[opt] rows with 1m entry: {len(sub)}")

    def sl_(ss):
        return sub[sub["_day"].isin(set(map(pd.Timestamp, ss)))].copy()
    FIT, VAL, TRAIN, TEST = sl_(FIT_s), sl_(VAL_s), sl_(TRAIN_s), sl_(TEST_s)
    print(f"[opt] attached: FIT={len(FIT)} VAL={len(VAL)} TRAIN={len(TRAIN)} TEST={len(TEST)}")

    # ----- baseline -----
    bm_tr, det_tr = full_metrics(BASELINE, TRAIN)
    bm_te, det_te = full_metrics(BASELINE, TEST)
    print(f"[opt] BASELINE TRAIN n={bm_tr['trades']} PF={bm_tr['net_pf']} net={bm_tr['net_pnl']}")
    print(f"[opt] BASELINE TEST  n={bm_te['trades']} PF={bm_te['net_pf']} net={bm_te['net_pnl']}")
    if not det_tr.empty:
        det_tr.to_csv(_SETUP_DIR / "baseline_train_trades.csv", index=False)
    if not det_te.empty:
        det_te.to_csv(_SETUP_DIR / "baseline_test_trades.csv", index=False)

    # ----- TRAIN-only quantile grids (never TEST) -----
    mask_quant = {}
    for f in MASK_FEATS:
        if f in TRAIN.columns:
            s = pd.to_numeric(TRAIN[f], errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
            if len(s) >= 8 and s.nunique() > 1:
                mask_quant[f] = {q: float(s.quantile(q)) for q in QGRID}
    pm_recs = []
    for r in TRAIN.itertuples():
        feats, reason = tt._premom(r.ticker, r.side, r.tt_entry_iso, float(r.tt_fill), 0.70, r.tt_sig_ts.isoformat())
        fd = dict(feats) if not reason else {}
        pm_recs.append({f: fd.get(f, np.nan) for f in PM_FEATS})
    pm_df = pd.DataFrame(pm_recs)
    pm_quant = {}
    for f in PM_FEATS:
        s = pd.to_numeric(pm_df[f], errors="coerce").dropna()
        if len(s) >= 8 and s.nunique() > 1:
            pm_quant[f] = {q: float(s.quantile(q)) for q in QGRID}
    print(f"[opt] searchable mask={sorted(mask_quant)} premom={sorted(pm_quant)}")

    evaluated: dict[str, dict] = {}   # cfg_key -> record

    def confirm(cfg, source, note):
        """Score a config on FIT/VAL + full TRAIN; if TRAIN PF in band, score TEST. Cache by key."""
        k = cfg_key(cfg)
        if k in evaluated:
            evaluated[k]["sources"].add(source)
            return evaluated[k]
        nf, pff, dff = fast_pf(cfg, FIT)
        nv, pfv, dfv = fast_pf(cfg, VAL)
        tr_m = _m(cfg, TRAIN)
        rec = {"source": source, "sources": {source}, "note": note, "cfg": cfg_public(cfg),
               "sl": cfg["sl"], "tgt": cfg["tgt"],
               "fit_n": nf, "fit_pf": round(pff, 3), "val_n": nv, "val_pf": round(pfv, 3),
               "train": tr_m, "test": None, "candidate": False, "reject_reason": ""}
        in_band = (args.train_pf_min <= tr_m["net_pf"] <= args.train_pf_max
                   and tr_m["trades"] >= args.min_train_trades)
        if in_band:
            te_m = _m(cfg, TEST)
            rec["test"] = te_m
            ok, why = candidate_ok(tr_m, te_m, args)
            rec["candidate"] = ok
            rec["reject_reason"] = "" if ok else why
        else:
            if tr_m["net_pf"] < args.train_pf_min:
                rec["reject_reason"] = f"TRAIN PF {tr_m['net_pf']}<{args.train_pf_min}"
            elif tr_m["net_pf"] > args.train_pf_max:
                rec["reject_reason"] = f"TRAIN PF {tr_m['net_pf']}>{args.train_pf_max} (overfit risk)"
            else:
                rec["reject_reason"] = f"TRAIN trades {tr_m['trades']}<{args.min_train_trades}"
        evaluated[k] = rec
        return rec

    # ============= HAND ITERATIONS (deterministic, one logical group at a time) =============
    print("\n[opt] === hand iteration sweep ===")
    hand = []
    # exit grid on the RAW (ungated) book + on the baseline-gated book
    for tag, base in (("ungated", mk(0.5, 0.6)),
                      ("baselinegate", mk(0.5, 0.6, BASELINE["mask_terms"], BASELINE["premom_terms"]))):
        for sl in SL_GRID:
            for tgt in TGT_GRID:
                hand.append((f"exit_{tag}_SL{sl}_T{tgt}", "exit/SL/target",
                             mk(sl, tgt, base["mask_terms"], base["premom_terms"])))
    # single mask filters at a few quantiles on a mid exit (0.70/1.25)
    for f in mask_quant:
        for q in (0.3, 0.5, 0.7):
            for op in (">=", "<="):
                thr = round(mask_quant[f][q], 6)
                hand.append((f"mask_{f}{op}{thr}_q{q}", "filter/mask",
                             mk(0.70, 1.25, [[f, op, thr]])))
    # single premom gates
    for f in pm_quant:
        for q in (0.4, 0.6):
            for op in (">=", "<="):
                thr = round(pm_quant[f][q], 6)
                hand.append((f"pm_{f}{op}{thr}_q{q}", "gate/pre_momentum",
                             mk(0.70, 1.25, None, [[f, op, thr]])))
    # time guards
    for ms in ("09:45", "10:00", "10:30"):
        hand.append((f"guard_min_slot_{ms}", "guard/time", mk(0.70, 1.25, guard={"min_slot": ms})))
    for mx in ("11:30", "12:30", "13:30"):
        hand.append((f"guard_max_slot_{mx}", "guard/time", mk(0.70, 1.25, guard={"max_slot": mx})))
    for tn in (1, 2, 3):
        hand.append((f"guard_top_n_{tn}", "guard/top_n", mk(0.70, 1.25, guard={"top_n": tn})))

    iter_rows = []
    for i, (name, group, cfg) in enumerate(hand, 1):
        rec = confirm(cfg, "hand", name)
        tr, te = rec["train"], rec["test"]
        iter_rows.append({
            "iter": i, "name": name, "group": group, "sl": cfg["sl"], "tgt": cfg["tgt"],
            "fit_n": rec["fit_n"], "fit_pf": rec["fit_pf"], "val_n": rec["val_n"], "val_pf": rec["val_pf"],
            "train_n": tr["trades"], "train_pf": tr["net_pf"], "train_net": tr["net_pnl"],
            "train_sl": tr["sl_cnt"], "train_tgt": tr["tgt_cnt"], "train_eod": tr["eod_cnt"],
            "test_n": (te["trades"] if te else ""), "test_pf": (te["net_pf"] if te else ""),
            "test_net": (te["net_pnl"] if te else ""),
            "candidate": rec["candidate"], "reject_reason": rec["reject_reason"],
        })
    print(f"[opt] hand iterations: {len(hand)} ({len(evaluated)} unique configs so far)")

    # ============= OPTUNA / RANDOM FIT-VAL SEARCH =============
    print("\n[opt] === FIT/VAL search ===")
    GL = float(args.gap_lambda)
    minfv = int(args.min_fv_trades)
    t0 = time.time()
    search_rows = []
    best = {"score": -1e9, "cfg": None}

    def build(s):
        def cat(n, ch): return s.suggest_categorical(n, ch)
        def integ(n, lo, hi): return s.suggest_int(n, lo, hi)
        mask = []
        for i in range(integ("n_mask", 0, args.max_mask_terms)):
            f = cat(f"m{i}f", [x for x in MASK_FEATS if x in mask_quant])
            op = cat(f"m{i}o", [">=", "<="]); q = cat(f"m{i}q", QGRID)
            mask.append([f, op, round(float(mask_quant[f][q]), 6)])
        pm = []
        for i in range(integ("n_pm", 0, args.max_pm_terms)):
            f = cat(f"p{i}f", [x for x in PM_FEATS if x in pm_quant])
            op = cat(f"p{i}o", [">=", "<="]); q = cat(f"p{i}q", QGRID)
            pm.append([f, op, round(float(pm_quant[f][q]), 6)])
        guard = {}
        if cat("use_min", [False, True]):
            guard["min_slot"] = cat("min_slot", ["09:30", "09:45", "10:00", "10:30"])
        if cat("use_max", [False, True]):
            guard["max_slot"] = cat("max_slot", ["11:30", "12:30", "13:30", "14:00"])
        tn = cat("top_n", [0, 1, 2, 3])
        if tn:
            guard["top_n"] = int(tn)
        return mk(cat("sl", SL_GRID), cat("tgt", TGT_GRID), mask, pm, guard or None,
                  max_positions=int(cat("maxpos", [10, 20])),
                  daily_loss_rs=float(cat("dloss", [0.0, 2500.0, 4000.0])))

    def score(cfg):
        nf, pff, dff = fast_pf(cfg, FIT)
        nv, pfv, dfv = fast_pf(cfg, VAL)
        tpdf, tpdv = nf / max(1, dff), nv / max(1, dfv)
        if nf < minfv or nv < minfv:
            return -5.0 + min(nf, nv) / max(1, minfv), nf, pff, nv, pfv
        if tpdf > args.max_trades_day or tpdv > args.max_trades_day:
            return -2.0, nf, pff, nv, pfv
        cf = 10.0 if not np.isfinite(pff) else min(pff, 10.0)
        cv = 10.0 if not np.isfinite(pfv) else min(pfv, 10.0)
        return (min(cf, cv) - GL * abs(cf - cv)), nf, pff, nv, pfv

    class RT:
        def __init__(self, r): self.r = r
        def suggest_categorical(self, n, ch): return ch[self.r.randrange(len(ch))]
        def suggest_int(self, n, lo, hi): return self.r.randint(lo, hi)

    def run_trial(s):
        cfg = build(s)
        sc, nf, pff, nv, pfv = score(cfg)
        search_rows.append({"sl": cfg["sl"], "tgt": cfg["tgt"],
                            "mask": ";".join(f"{a}{o}{b}" for a, o, b in cfg["mask_terms"]) or "-",
                            "premom": ";".join(f"{a}{o}{b}" for a, o, b in cfg["premom_terms"]) or "-",
                            "guard": json.dumps(cfg["guard"]) if cfg["guard"] else "-",
                            "fit_n": nf, "fit_pf": round(pff, 3), "val_n": nv, "val_pf": round(pfv, 3),
                            "score": round(float(sc), 4)})
        if sc > best["score"]:
            best["score"], best["cfg"] = sc, cfg
        return sc

    if HAVE_OPTUNA:
        def objective(trial):
            return run_trial(trial)
        study = optuna.create_study(direction="maximize",
                                    sampler=optuna.samplers.TPESampler(seed=args.seed))
        study.optimize(objective, n_trials=args.trials, timeout=args.time_budget_min * 60.0)
        n_done = len(study.trials)
    else:
        r = random.Random(args.seed); n_done = 0
        for _ in range(args.trials):
            if time.time() - t0 > args.time_budget_min * 60.0:
                break
            run_trial(RT(r)); n_done += 1
    print(f"[opt] search trials={n_done} best FIT/VAL score={best['score']:.3f}")

    # confirm the TOP search configs (by score) on TRAIN/TEST
    top = sorted(search_rows, key=lambda x: x["score"], reverse=True)
    seen = set(); confirmed = 0
    # rebuild cfg objects from the recorded fields for the top trials
    def reparse_terms(s):
        out = []
        if s and s != "-":
            for tk in s.split(";"):
                for op in (">=", "<=", "==", "!="):
                    if op in tk:
                        a, b = tk.split(op); out.append([a, op, float(b)]); break
        return out
    for row in top[:120]:
        guard = None if row["guard"] == "-" else json.loads(row["guard"])
        cfg = mk(row["sl"], row["tgt"], reparse_terms(row["mask"]), reparse_terms(row["premom"]), guard)
        k = cfg_key(cfg)
        if k in seen:
            continue
        seen.add(k)
        confirm(cfg, "optuna", f"score={row['score']}")
        confirmed += 1
    print(f"[opt] confirmed {confirmed} top search configs on TRAIN/TEST")

    # ============= COLLECT CANDIDATES =============
    cands = [r for r in evaluated.values() if r["candidate"]]
    # rank candidates: prefer test PF, then test trades, then balanced train PF, low dominance
    def cand_rank(r):
        te = r["test"]; tr = r["train"]
        return (te["net_pf"], te["trades"], -abs(tr["net_pf"] - 1.5))
    cands.sort(key=cand_rank, reverse=True)
    print(f"\n[opt] CANDIDATES passing TRAIN[{args.train_pf_min},{args.train_pf_max}] & TEST>{args.test_pf_min}: {len(cands)}")
    for r in cands[:15]:
        tr, te = r["train"], r["test"]
        print(f"   SL/Tgt={r['sl']}/{r['tgt']} mask={r['cfg']['mask_terms']} pm={r['cfg']['pre_momentum_terms']} "
              f"guard={r['cfg']['entry_guards']} | TRAIN n={tr['trades']} PF={tr['net_pf']} | "
              f"TEST n={te['trades']} PF={te['net_pf']} net={te['net_pnl']}")

    # ============= WRITE ARTIFACTS =============
    pd.DataFrame(iter_rows).to_csv(_SETUP_DIR / "iterations.csv", index=False)
    pd.DataFrame(top).to_csv(_SETUP_DIR / "optuna_trials.csv", index=False)
    summary = {
        "setup": SETUP, "engine": engine,
        "slippage_bps": args.slippage_bps,
        "requested_windows": {"train": "2026-05-18..(pre-test)", "test": "2026-06-20..latest",
                              "note": "UNAVAILABLE in any P_PDH pool; data ends "
                                      f"{pd.Timestamp(sessions[-1]).date()} -> nearest-available used"},
        "windows": {"FIT": rng(FIT_s), "VAL": rng(VAL_s), "TRAIN": rng(TRAIN_s), "TEST": rng(TEST_s),
                    "FIT_sessions": [str(pd.Timestamp(s).date()) for s in FIT_s],
                    "VAL_sessions": [str(pd.Timestamp(s).date()) for s in VAL_s],
                    "TEST_sessions": [str(pd.Timestamp(s).date()) for s in TEST_s]},
        "gates": {"train_pf_min": args.train_pf_min, "train_pf_max": args.train_pf_max,
                  "test_pf_min": args.test_pf_min, "min_train_trades": args.min_train_trades,
                  "min_test_trades": args.min_test_trades, "dom_cap": args.dom_cap,
                  "max_trades_day": args.max_trades_day},
        "baseline": {"config": cfg_public(BASELINE), "train": bm_tr, "test": bm_te},
        "n_unique_configs": len(evaluated),
        "n_candidates": len(cands),
        "candidates": [{"cfg": r["cfg"], "train": r["train"], "test": r["test"],
                        "sources": sorted(r["sources"]), "note": r["note"]} for r in cands],
        "best_fitval_score": best["score"],
    }
    (_SETUP_DIR / "run_summary.json").write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")

    # candidate JSONs + per-trade detail
    (_SETUP_DIR / "candidates").mkdir(exist_ok=True)
    for i, r in enumerate(cands, 1):
        cj = {"setup": SETUP, "rank": i, "config": r["cfg"],
              "train_metrics": r["train"], "test_metrics": r["test"],
              "windows": summary["windows"], "sources": sorted(r["sources"])}
        (_SETUP_DIR / "candidates" / f"{SETUP}_candidate_{i:03d}.json").write_text(
            json.dumps(cj, indent=2, default=str), encoding="utf-8")
        cfgobj = mk(r["cfg"]["exit"]["sl_pct"], r["cfg"]["exit"]["tgt_pct"],
                    r["cfg"]["mask_terms"], r["cfg"]["pre_momentum_terms"],
                    r["cfg"]["entry_guards"] or None)
        for lbl, dfw in (("train", TRAIN), ("test", TEST)):
            _, det = full_metrics(cfgobj, dfw)
            if not det.empty:
                det.to_csv(_SETUP_DIR / "candidates" / f"{SETUP}_candidate_{i:03d}_{lbl}_trades.csv", index=False)

    print(f"\n[opt] wrote run_summary.json, iterations.csv, optuna_trials.csv, candidates/ under {_SETUP_DIR}")
    return 0


def candidate_ok(tr, te, args) -> tuple[bool, str]:
    reasons = []
    if te["trades"] < args.min_test_trades:
        reasons.append(f"test_n {te['trades']}<{args.min_test_trades}")
    if te["net_pf"] <= args.test_pf_min:
        reasons.append(f"test_pf {te['net_pf']}<={args.test_pf_min}")
    for lbl, m in (("train", tr), ("test", te)):
        for k in ("top_trade_gross_share", "top_day_net_share", "top_symbol_net_share"):
            v = m.get(k)
            if v is not None and v > args.dom_cap:
                reasons.append(f"{lbl}_{k} {v}>{args.dom_cap}")
    return (len(reasons) == 0), "; ".join(reasons)


if __name__ == "__main__":
    raise SystemExit(main())
