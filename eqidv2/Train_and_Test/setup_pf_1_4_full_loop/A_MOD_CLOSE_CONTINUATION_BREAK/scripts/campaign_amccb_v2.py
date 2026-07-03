r"""campaign_amccb_v2.py — EXPANDED-FEATURE optimization campaign for
A_MOD_CLOSE_CONTINUATION_BREAK (research-only; never writes final_setup_conf.py).

Differences vs campaign_amccb.py (v1):
  * pool = pools/pool_enriched — every signal row carries ~35 x_* causal indicator /
    price-action / day-context features (RSI, ADX, MACD, BB, Keltner, Stoch, W%R, CCI,
    MFI, OBV, ROC, EMA structure, session-VWAP, day/OR/prev-day levels, candle
    structure, volume) + the 8 repo pre-momentum features as x_pm_* columns.
  * masks: up to 3 AND-terms over the expanded space (v1 was 2 over 11 features).
  * explicit STRUCTURAL HYPOTHESIS configs (hand-written, explainable rule packs)
    are scored first and logged.
  * search runs mask-only (cheap, cached resolves); any candidate whose config uses
    x_pm_* terms is RE-VERIFIED through the true pre_momentum_terms engine path
    before being accepted (pre-dedupe semantics, live-faithful).
  * TEST discipline unchanged: FIT/VAL band objective, TRAIN-only quantiles, full-TRAIN
    band [1.30,1.80] gate before TEST, TEST budget, domination caps, robustness.

Deployment note: x_* terms are signal-bar 5-min conditions — deployable as a flag-gated
detector extension (S9/DOC5D pattern); x_pm_* terms map to pre_momentum_terms.

Usage:
  py -3.12 campaign_amccb_v2.py [--trials 3000] [--time_budget_min 60] [--seed 11]
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

try:
    import optuna
    optuna.logging.set_verbosity(optuna.logging.WARNING)
    HAVE_OPTUNA = True
except Exception:
    HAVE_OPTUNA = False

SETUP = "A_MOD_CLOSE_CONTINUATION_BREAK"
SIDE = "LONG"
TRAIN_START = pd.Timestamp("2026-03-01")
TRAIN_END = pd.Timestamp("2026-05-30")
TEST_START = pd.Timestamp("2026-06-01")
TEST_END = pd.Timestamp("2026-07-01")
FIT_FRAC = 0.60

PF_LO, PF_HI = 1.30, 1.80
TENT_HI = 1.70
TEST_PF_MIN = 1.40
GAP_LAMBDA = 0.80
FV_FLOOR = 6
DOM_TRADE, DOM_DAY, DOM_SYM = 0.35, 0.40, 0.40

SL_GRID = [0.40, 0.50, 0.60, 0.70, 0.85, 1.00, 1.20]
TGT_GRID = [0.60, 0.80, 1.00, 1.25, 1.50, 2.00, 2.50]
MIN_SLOTS = ["09:30", "10:00", "10:30", "11:00"]
MAX_SLOTS = ["11:00", "12:00", "13:00", "14:00"]
QGRID = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
SWEEP_Q = [0.1, 0.3, 0.5, 0.7, 0.9]
SWEEP_ANCHORS = [(0.70, 1.50), (1.00, 2.00)]   # production + best v1 exit family

BASE_FEATS = ["rs_pct", "vol_ratio", "atr_pct", "body_pct", "close_loc",
              "vwap_dist_atr", "quality_score", "signal_range_pct",
              "upper_wick_pct", "lower_wick_pct", "wick_skew_pct"]
BINARY_FEATS = ["x_ema20_gt50", "x_ema_stack", "x_macd_above_sig", "x_above_pdh"]
MIN_COVERAGE = 0.90

STATE: dict = {}
ITER_LOG: list[dict] = []
TEST_EVALS = 0


def _clamp_pf(pf):
    return 10.0 if not np.isfinite(pf) else min(float(pf), 10.0)


def band_reward(pf):
    return pf if pf <= TENT_HI else TENT_HI - 1.5 * (pf - TENT_HI)


def mk_cfg(sl=0.70, tgt=1.50, mask=None, premom=None, guard=None,
           max_positions=20, daily_loss_rs=0.0):
    return {"sl": float(sl), "tgt": float(tgt),
            "mask_terms": [tuple(t) for t in (mask or [])],
            "premom_terms": [tuple(t) for t in (premom or [])],
            "guard": (dict(guard) if guard else None), "status": "OK",
            "max_positions": int(max_positions), "daily_loss_rs": float(daily_loss_rs)}


def cfg_key(cfg):
    return json.dumps({"sl": cfg["sl"], "tgt": cfg["tgt"],
                       "mask": sorted(map(list, cfg["mask_terms"])),
                       "pm": sorted(map(list, cfg["premom_terms"])),
                       "guard": cfg["guard"]}, sort_keys=True)


def cfg_str(cfg):
    m = ";".join(f"{a}{o}{b}" for a, o, b in cfg["mask_terms"]) or "-"
    p = ";".join(f"{a}{o}{b}" for a, o, b in cfg["premom_terms"]) or "-"
    g = json.dumps(cfg["guard"]) if cfg["guard"] else "-"
    return f"SL{cfg['sl']}/T{cfg['tgt']} mask[{m}] pm[{p}] g{g}"


def fast_eval(cfg, df):
    tt.MAX_POSITIONS = cfg.get("max_positions", 20)
    tt.DAILY_LOSS_RS = cfg.get("daily_loss_rs", 0.0)
    fam = tt.eval_family({SETUP: cfg}, df)
    return int(fam["trades"]), float(fam["net_pf"]), float(fam["net_pnl"])


def full_eval(cfg, df):
    tt.MAX_POSITIONS = cfg.get("max_positions", 20)
    tt.DAILY_LOSS_RS = cfg.get("daily_loss_rs", 0.0)
    return eng.full_metrics(SETUP, cfg, df)


def score_cfg(cfg):
    nf, pf_f, _ = fast_eval(cfg, STATE["FIT"])
    nv, pf_v, _ = fast_eval(cfg, STATE["VAL"])
    if nf < FV_FLOOR or nv < FV_FLOOR:
        return -5.0 + min(nf, nv) / max(1, FV_FLOOR), nf, pf_f, nv, pf_v
    cf, cv = _clamp_pf(pf_f), _clamp_pf(pf_v)
    sc = band_reward(min(cf, cv)) - GAP_LAMBDA * abs(cf - cv)
    if min(cf, cv) >= PF_LO:
        sc += 0.003 * min(min(nf, nv), 40)
    return sc, nf, pf_f, nv, pf_v


def log_iter(group, change, reason, fitm, valm, trainm=None, testm=None,
             decision="", fail_class="", next_action=""):
    ITER_LOG.append({
        "iter": len(ITER_LOG) + 1, "group": group, "change": change,
        "old": "-", "new": "-", "reason": reason,
        "fit": fitm, "val": valm, "train": trainm, "test": testm,
        "decision": decision, "failure_class": fail_class, "next_action": next_action,
    })


def fv_pack(n, pf, pnl=0):
    return {"n": int(n), "pf": round(float(pf), 3), "net": round(float(pnl), 0)}


def dom_ok(m):
    return (m["trade_dom_gross"] is not None and m["trade_dom_gross"] <= DOM_TRADE
            and m["day_dom"] is not None and m["day_dom"] <= DOM_DAY
            and m["sym_dom"] is not None and m["sym_dom"] <= DOM_SYM)


def save_json(name, obj):
    (WORK / name).write_text(json.dumps(obj, indent=2, default=str), encoding="utf-8")
    print(f"[v2] wrote {WORK / name}", flush=True)


# ---------------------------------------------------------------------------
def prepare(pool_dir: Path):
    tt.POOL_DIRS = [str(pool_dir)]
    tt.SLIPPAGE_BPS = 15.0
    tt.MAX_POSITIONS = 20
    tt.DAILY_LOSS_RS = 0.0
    tt._entry.cache_clear(); tt._resolve_full.cache_clear(); tt._premom.cache_clear()

    pool = tt.load_pool()
    pool = pool[pool["setup"].astype(str).str.upper().eq(SETUP)].copy()
    train_raw = pool[(pool["_day"] >= TRAIN_START) & (pool["_day"] <= TRAIN_END)].reset_index(drop=True)
    test_raw = pool[(pool["_day"] >= TEST_START) & (pool["_day"] <= TEST_END)].reset_index(drop=True)
    t0 = time.time()
    train = tt.attach_entries(train_raw)
    test = tt.attach_entries(test_raw)
    print(f"[v2] attach: train {len(train)} test {len(test)} in {time.time()-t0:.0f}s", flush=True)

    tr_sessions = sorted(train["_day"].dt.strftime("%Y-%m-%d").unique())
    n_fit = max(1, int(round(FIT_FRAC * len(tr_sessions))))
    fit_s, val_s = tr_sessions[:n_fit], tr_sessions[n_fit:]
    fit = train[train["_day"].dt.strftime("%Y-%m-%d").isin(fit_s)].reset_index(drop=True)
    val = train[train["_day"].dt.strftime("%Y-%m-%d").isin(val_s)].reset_index(drop=True)

    # dynamic searchable feature list: base + numeric x_* with coverage>=90% on TRAIN
    feats = list(BASE_FEATS)
    for c in sorted(train.columns):
        if not c.startswith("x_") or c in BINARY_FEATS:
            continue
        s = pd.to_numeric(train[c], errors="coerce")
        if s.notna().mean() >= MIN_COVERAGE and s.nunique() > 8:
            feats.append(c)
    mask_quant = {}
    for f in feats:
        s = pd.to_numeric(train[f], errors="coerce").dropna()
        if len(s) >= 8 and s.nunique() > 1:
            mask_quant[f] = {q: float(s.quantile(q)) for q in QGRID}
    binaries = [b for b in BINARY_FEATS if b in train.columns
                and pd.to_numeric(train[b], errors="coerce").notna().mean() >= MIN_COVERAGE]

    STATE.update({"TRAIN": train, "TEST": test, "FIT": fit, "VAL": val,
                  "fit_sessions": fit_s, "val_sessions": val_s,
                  "mask_quant": mask_quant, "binaries": binaries})
    print(f"[v2] FIT {fit_s[0]}..{fit_s[-1]} n={len(fit)} | VAL {val_s[0]}..{val_s[-1]} n={len(val)} "
          f"| TEST n={len(test)}", flush=True)
    print(f"[v2] searchable features: {len(mask_quant)} numeric + {len(binaries)} binary", flush=True)


# ---------------------------------------------------------------------------
# structural hypothesis packs (explainable rule sets, tested first)
# ---------------------------------------------------------------------------
def structural_hypotheses():
    q = STATE["mask_quant"]

    def thr(f, qq):
        return round(float(q[f][qq]), 6) if f in q else None

    H = []

    def add(name, why, mask, guard=None):
        H.append((name, why, [t for t in mask if t[2] is not None], guard))

    add("fresh_at_day_high", "close AT the day high (no overhead supply) and not VWAP-overextended",
        [("x_dist_dayhigh_atr", "<=", thr("x_dist_dayhigh_atr", 0.2)),
         ("x_svwap_dist_atr", "<=", thr("x_svwap_dist_atr", 0.7))])
    add("trend_alignment", "EMA20>50>200 stack with real trend strength — only aligned continuation",
        [("x_ema_stack", ">=", 0.5), ("x_adx", ">=", thr("x_adx", 0.6))])
    add("pdh_break_fresh", "first push above prev-day high (structural breakout level)",
        [("x_above_pdh", ">=", 0.5), ("x_pdh_dist_atr", "<=", thr("x_pdh_dist_atr", 0.8))])
    add("squeeze_expansion", "BB squeeze then range expansion — fresh energy release",
        [("x_bb_width_pct", "<=", thr("x_bb_width_pct", 0.3)),
         ("x_range_vs_avg20", ">=", thr("x_range_vs_avg20", 0.7))])
    add("not_exhausted", "few prior up-bars and moderate 1-hour ROC — avoid chasing bar #4",
        [("x_consec_up3", "<=", 1.0), ("x_roc12", "<=", thr("x_roc12", 0.5))])
    add("macd_turn_up", "MACD histogram above zero AND rising into the break",
        [("x_macd_above_sig", ">=", 0.5), ("x_macd_hist_delta_atr", ">=", 0.0)])
    add("stoch_not_overbought", "oscillator not pinned — room to run",
        [("x_stoch_k", "<=", 80.0)])
    add("gap_up_continuation", "gap-up day holding positive — day-context tailwind",
        [("x_gap_pct", ">=", 0.0), ("x_day_ret_pct", ">=", 0.0)])
    add("or_breakout_early", "above the opening range with morning entry only",
        [("x_orh_dist_atr", ">=", 0.0)], guard={"max_slot": "12:00"})
    add("rsi_momentum_zone", "RSI in the 55-70 momentum zone (not overbought) and rising",
        [("x_rsi", ">=", 55.0), ("x_rsi", "<=", 70.0), ("x_rsi_slope3", ">=", 0.0)])
    add("volume_thrust", "true volume thrust with buyer-controlled flow (MFI)",
        [("x_vol_vs_avg20", ">=", thr("x_vol_vs_avg20", 0.8)),
         ("x_mfi14", ">=", thr("x_mfi14", 0.6))])
    add("obv_confirm", "OBV rising over the last 5 bars — accumulation behind the break",
        [("x_obv_slope5", ">=", thr("x_obv_slope5", 0.7))])
    add("low_vol_name", "low-ATR name where a 1%% move is meaningful and SL is not noise",
        [("x_atr_pct", "<=", thr("x_atr_pct", 0.3)), ("x_adx", ">=", thr("x_adx", 0.5))])
    add("kelt_breakout", "closing above the upper Keltner band — genuine expansion",
        [("x_kelt_pos", ">=", 1.0)])
    add("day_range_fresh", "day range still small — move NOT already spent",
        [("x_dayrange_atr", "<=", thr("x_dayrange_atr", 0.3)),
         ("x_pos_in_dayrange", ">=", 0.9)])
    add("pm_trend_confirm", "1-min pre-entry trend strength + volume (premom engine themes)",
        [("x_pm_pre1_adx", ">=", thr("x_pm_pre1_adx", 0.7)),
         ("x_pm_sig5_vol_ratio20", ">=", thr("x_pm_sig5_vol_ratio20", 0.7))])
    add("early_bird", "bar index <= 12 (first hour) — signals before the chop",
        [("x_bar_idx", "<=", 12.0)])
    add("late_reject", "no entries after 13:00 via day-position: avoid EOD-exit churn",
        [("x_bar_idx", "<=", 45.0)], guard={"max_slot": "13:00"})
    return H


def stage_hypotheses():
    rows = []
    for name, why, mask, guard in structural_hypotheses():
        for sl, tgt in SWEEP_ANCHORS:
            cfg = mk_cfg(sl=sl, tgt=tgt, mask=mask, guard=guard)
            sc, nf, pf_f, nv, pf_v = score_cfg(cfg)
            dec = "shortlist" if (nf >= FV_FLOOR and nv >= FV_FLOOR and min(pf_f, pf_v) >= 0.9) else "reject"
            log_iter("hypothesis", f"{name}@SL{sl}/T{tgt}", why,
                     fv_pack(nf, pf_f), fv_pack(nv, pf_v), decision=dec,
                     fail_class=("" if dec == "shortlist" else "FIT/VAL PF below 0.9 or too thin"),
                     next_action=("feed TPE region" if dec == "shortlist" else "drop"))
            rows.append({"name": name, "why": why, "sl": sl, "tgt": tgt,
                         "mask": [list(t) for t in mask], "guard": guard,
                         "fit_n": nf, "fit_pf": round(pf_f, 3),
                         "val_n": nv, "val_pf": round(pf_v, 3), "score": round(float(sc), 4),
                         "decision": dec})
            print(f"[v2-hyp] {name}@SL{sl}/T{tgt}: FIT {nf}/{pf_f:.3f} VAL {nv}/{pf_v:.3f} -> {dec}",
                  flush=True)
    save_json("hypotheses_v2.json", rows)
    return rows


# ---------------------------------------------------------------------------
def stage_sweeps():
    res = []
    for f, qs in STATE["mask_quant"].items():
        for qq in SWEEP_Q:
            thr = round(float(qs[qq]), 6)
            for op in (">=", "<="):
                for sl, tgt in SWEEP_ANCHORS:
                    cfg = mk_cfg(sl=sl, tgt=tgt, mask=[(f, op, thr)])
                    sc, nf, pf_f, nv, pf_v = score_cfg(cfg)
                    dec = ("keep" if (nf >= FV_FLOOR and nv >= FV_FLOOR and min(pf_f, pf_v) >= 1.0)
                           else "reject")
                    res.append({"label": f"{f}{op}{thr}(q{qq})@SL{sl}/T{tgt}", "feat": f,
                                "fit_n": nf, "fit_pf": round(pf_f, 3),
                                "val_n": nv, "val_pf": round(pf_v, 3),
                                "score": round(float(sc), 4), "decision": dec})
                    log_iter("sweep-x", f"{f}{op}{thr}@SL{sl}/T{tgt}", "expanded-feature single term",
                             fv_pack(nf, pf_f), fv_pack(nv, pf_v), decision=dec,
                             fail_class=("" if dec == "keep" else "weak FIT/VAL"),
                             next_action=("combo pool" if dec == "keep" else "drop"))
        print(f"[v2-sweep] {f} done", flush=True)
    for b in STATE["binaries"]:
        for want in (0.5, -0.5):     # >=0.5 means ==1 ; <=0.5 wrapped below
            op = ">=" if want > 0 else "<="
            for sl, tgt in SWEEP_ANCHORS:
                cfg = mk_cfg(sl=sl, tgt=tgt, mask=[(b, op, 0.5)])
                sc, nf, pf_f, nv, pf_v = score_cfg(cfg)
                dec = ("keep" if (nf >= FV_FLOOR and nv >= FV_FLOOR and min(pf_f, pf_v) >= 1.0)
                       else "reject")
                res.append({"label": f"{b}{op}0.5@SL{sl}/T{tgt}", "feat": b,
                            "fit_n": nf, "fit_pf": round(pf_f, 3),
                            "val_n": nv, "val_pf": round(pf_v, 3),
                            "score": round(float(sc), 4), "decision": dec})
                log_iter("sweep-x", f"{b}{op}0.5@SL{sl}/T{tgt}", "binary structure flag",
                         fv_pack(nf, pf_f), fv_pack(nv, pf_v), decision=dec,
                         fail_class=("" if dec == "keep" else "weak FIT/VAL"),
                         next_action=("combo pool" if dec == "keep" else "drop"))
    save_json("sweeps_v2.json", res)
    return res


# ---------------------------------------------------------------------------
def _suggest(trial, max_mask=3):
    mq = STATE["mask_quant"]
    feats = sorted(mq)
    bins = STATE["binaries"]
    def cat(n, ch): return trial.suggest_categorical(n, ch)
    mask_terms = []
    used = set()
    for i in range(trial.suggest_int("n_mask", 1, max_mask)):
        kind = cat(f"m{i}_kind", ["num", "bin"] if bins else ["num"])
        if kind == "bin":
            f = cat(f"m{i}_bfeat", bins)
            op = cat(f"m{i}_bop", [">=", "<="])
            term = (f, op, 0.5)
        else:
            f = cat(f"m{i}_feat", feats)
            op = cat(f"m{i}_op", [">=", "<="])
            qq = cat(f"m{i}_q", QGRID)
            term = (f, op, round(float(mq[f][qq]), 6))
        if f in used:
            continue
        used.add(f)
        mask_terms.append(term)
    guard = {}
    if cat("use_min_slot", [False, True]):
        guard["min_slot"] = cat("min_slot", MIN_SLOTS)
    if cat("use_max_slot", [False, True]):
        guard["max_slot"] = cat("max_slot", MAX_SLOTS)
    tn = cat("top_n", [0, 1, 2, 3])
    if tn:
        guard["top_n"] = int(tn)
    return mk_cfg(sl=cat("sl", SL_GRID), tgt=cat("tgt", TGT_GRID),
                  mask=mask_terms, guard=(guard or None))


class _RandTrial:
    def __init__(self, rng): self.rng = rng
    def suggest_categorical(self, n, ch): return ch[self.rng.randrange(len(ch))]
    def suggest_int(self, n, lo, hi): return self.rng.randint(lo, hi)


def stage_search(trials, seed, time_budget_min):
    trial_rows = []
    pool: dict[str, dict] = {}

    def consider(cfg, tag):
        sc, nf, pf_f, nv, pf_v = score_cfg(cfg)
        trial_rows.append({"tag": tag, "sl": cfg["sl"], "tgt": cfg["tgt"],
                           "mask": ";".join(f"{a}{o}{b}" for a, o, b in cfg["mask_terms"]) or "-",
                           "guard": json.dumps(cfg["guard"]) if cfg["guard"] else "-",
                           "fit_n": nf, "fit_pf": round(pf_f, 3),
                           "val_n": nv, "val_pf": round(pf_v, 3), "score": round(float(sc), 4)})
        k = cfg_key(cfg)
        if k not in pool or sc > pool[k]["score"]:
            pool[k] = {"score": float(sc), "cfg": cfg, "fit_pf": pf_f, "val_pf": pf_v,
                       "fit_n": nf, "val_n": nv, "tag": tag}
        return sc

    t0 = time.time()
    if HAVE_OPTUNA:
        def objective(trial):
            return consider(_suggest(trial), "optuna_v2")
        study = optuna.create_study(direction="maximize",
                                    sampler=optuna.samplers.TPESampler(seed=seed,
                                                                       n_startup_trials=80))
        study.optimize(objective, n_trials=trials, timeout=time_budget_min * 60.0)
        n_done = len(study.trials)
        engine = "Optuna TPE"
    else:
        print("Optuna unavailable; using seeded random search fallback.", flush=True)
        rng = random.Random(seed); n_done = 0
        for _ in range(trials):
            if time.time() - t0 > time_budget_min * 60.0:
                break
            consider(_suggest(_RandTrial(rng)), "random_v2")
            n_done += 1
        engine = "seeded random search"
    print(f"[v2] search: {n_done} trials via {engine} in {time.time()-t0:.0f}s", flush=True)

    tdf = pd.DataFrame(trial_rows).sort_values("score", ascending=False)
    tdf.to_csv(WORK / "trials_v2.csv", index=False)
    ranked = sorted(pool.values(), key=lambda x: -x["score"])
    # log top 60 as iterations
    for r in ranked[:60]:
        dec = "confirm-queue" if r["score"] > 0.9 else "reject"
        log_iter("combo-tpe", cfg_str(r["cfg"]), "TPE combination over expanded features",
                 fv_pack(r["fit_n"], r["fit_pf"]), fv_pack(r["val_n"], r["val_pf"]),
                 decision=dec, fail_class=("" if dec != "reject" else "band score <= 0.9"),
                 next_action=("stage confirm" if dec != "reject" else "drop"))
    save_json("combos_v2.json", [{**{k: v for k, v in r.items() if k != "cfg"},
                                  "cfg": {k: v for k, v in r["cfg"].items() if k != "status"}}
                                 for r in ranked[:60]])
    return ranked


# ---------------------------------------------------------------------------
def reverify_with_true_premom(cfg):
    """If cfg uses x_pm_* mask terms, move them to true premom terms (pre-dedupe path)."""
    pm_terms = [(f[len("x_pm_"):], op, thr) for f, op, thr in cfg["mask_terms"]
                if f.startswith("x_pm_")]
    if not pm_terms:
        return None
    rest = [t for t in cfg["mask_terms"] if not t[0].startswith("x_pm_")]
    cfg2 = mk_cfg(sl=cfg["sl"], tgt=cfg["tgt"], mask=rest, premom=pm_terms,
                  guard=cfg["guard"], max_positions=cfg["max_positions"],
                  daily_loss_rs=cfg["daily_loss_rs"])
    return cfg2


def stage_confirm(ranked, test_budget=10, max_confirm=18, prior=None):
    global TEST_EVALS
    results = list(prior or [])
    seen = set()
    confirmed = 0
    for r in ranked:
        if confirmed >= max_confirm or TEST_EVALS >= test_budget:
            break
        cfg = r["cfg"]
        sig = json.dumps([sorted(map(list, cfg["mask_terms"])), cfg["guard"]], sort_keys=True)
        if sig in seen:
            continue
        seen.add(sig)
        confirmed += 1
        mTR = full_eval(cfg, STATE["TRAIN"])
        trainm = {k: v for k, v in mTR.items() if k != "detail"}
        in_band = PF_LO <= mTR["net_pf"] <= PF_HI and mTR["n"] >= 20 and mTR["net_pnl"] > 0
        rec = {"tag": r.get("tag"), "score": r["score"],
               "cfg": {k: v for k, v in cfg.items() if k != "status"},
               "fit_pf": r["fit_pf"], "val_pf": r["val_pf"],
               "train": trainm, "in_band": bool(in_band), "test": None, "verdict": None}
        if not in_band:
            rec["verdict"] = ("REJECT: TRAIN PF outside [1.30,1.80]"
                              if not (PF_LO <= mTR["net_pf"] <= PF_HI)
                              else "REJECT: TRAIN too thin/negative")
            log_iter("confirm-v2", cfg_str(cfg), "full-TRAIN confirmation",
                     fv_pack(r["fit_n"], r["fit_pf"]), fv_pack(r["val_n"], r["val_pf"]),
                     trainm=trainm, decision="reject", fail_class=rec["verdict"],
                     next_action="next candidate")
            results.append(rec)
            continue
        # true-premom re-verification when x_pm_* terms are present
        cfg_pm = reverify_with_true_premom(cfg)
        if cfg_pm is not None:
            mTRpm = full_eval(cfg_pm, STATE["TRAIN"])
            rec["train_true_premom"] = {k: v for k, v in mTRpm.items() if k != "detail"}
            if not (PF_LO <= mTRpm["net_pf"] <= PF_HI and mTRpm["n"] >= 20 and mTRpm["net_pnl"] > 0):
                rec["verdict"] = "REJECT: fails when x_pm terms run through true premom path"
                log_iter("confirm-v2", cfg_str(cfg), "true-premom re-verification failed",
                         fv_pack(r["fit_n"], r["fit_pf"]), fv_pack(r["val_n"], r["val_pf"]),
                         trainm=rec["train_true_premom"], decision="reject",
                         fail_class=rec["verdict"], next_action="next candidate")
                results.append(rec)
                continue
            cfg_eval = cfg_pm
        else:
            cfg_eval = cfg
        TEST_EVALS += 1
        mTE = full_eval(cfg_eval, STATE["TEST"])
        testm = {k: v for k, v in mTE.items() if k != "detail"}
        rec["test"] = testm
        checks = {
            "test_pf_gt_1.40": mTE["net_pf"] > TEST_PF_MIN,
            "test_net_pos": mTE["net_pnl"] > 0,
            "test_n_ge_5": mTE["n"] >= 5,
            "train_dom_ok": dom_ok(mTR),
            "test_dom_ok": dom_ok(mTE),
            "fit_val_no_collapse": min(r["fit_pf"], r["val_pf"]) >= 1.05,
        }
        rec["checks"] = checks
        passed = all(checks.values())
        if passed:
            rob = eng.robustness_report(SETUP, cfg, STATE["TRAIN"], STATE["mask_quant"], {},
                                        argparse.Namespace(min_trades_train=20,
                                                           neighborhood_pf_min=1.10,
                                                           dropout_pf_min=1.00))
            rec["robustness"] = {k: v for k, v in rob.items()
                                 if k in ("neighbor_pass", "dropout_pass", "passed", "base")}
            rec["robustness_detail"] = rob
            passed = passed and rob["passed"]
        rec["verdict"] = "CANDIDATE" if passed else \
            "REJECT: " + ";".join(k for k, v in checks.items() if not v) + \
            ("" if rec.get("robustness", {}).get("passed", True) else ";robustness_failed")
        log_iter("confirm+test-v2", cfg_str(cfg), "full-TRAIN in band -> single TEST scoring",
                 fv_pack(r["fit_n"], r["fit_pf"]), fv_pack(r["val_n"], r["val_pf"]),
                 trainm=trainm, testm=testm,
                 decision=("KEEP-CANDIDATE" if passed else "reject"),
                 fail_class=("" if passed else rec["verdict"]),
                 next_action=("write candidate json" if passed else "next candidate"))
        results.append(rec)
    save_json("confirmations_v2.json", results)
    return results


# ---------------------------------------------------------------------------
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pool", default=str(WORK / "pools" / "pool_enriched"))
    ap.add_argument("--trials", type=int, default=3000)
    ap.add_argument("--seed", type=int, default=11)
    ap.add_argument("--time_budget_min", type=float, default=60.0)
    ap.add_argument("--test_budget", type=int, default=10)
    args = ap.parse_args()
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass

    print(f"[v2] optimizer: {'Optuna TPE' if HAVE_OPTUNA else 'Optuna unavailable; using seeded random search fallback.'}",
          flush=True)
    prepare(Path(args.pool))

    hyp = stage_hypotheses()
    save_json("iteration_records_v2.json", ITER_LOG)
    sweeps = stage_sweeps()
    save_json("iteration_records_v2.json", ITER_LOG)
    ranked = stage_search(args.trials, args.seed, args.time_budget_min)
    save_json("iteration_records_v2.json", ITER_LOG)

    # seed the confirm queue with shortlisted hypotheses too
    hyp_ranked = [{"score": h["score"],
                   "cfg": mk_cfg(sl=h["sl"], tgt=h["tgt"], mask=[tuple(t) for t in h["mask"]],
                                 guard=h["guard"]),
                   "fit_pf": h["fit_pf"], "val_pf": h["val_pf"],
                   "fit_n": h["fit_n"], "val_n": h["val_n"], "tag": f"hyp:{h['name']}"}
                  for h in hyp if h["decision"] == "shortlist"]
    queue = sorted(hyp_ranked + ranked, key=lambda x: -x["score"])
    results = stage_confirm(queue, test_budget=args.test_budget)
    save_json("iteration_records_v2.json", ITER_LOG)

    n_cand = sum(1 for r in results if r.get("verdict") == "CANDIDATE")
    print(f"[v2] DONE. iterations={len(ITER_LOG)} test_evals={TEST_EVALS} candidates={n_cand}",
          flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
