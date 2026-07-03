r"""campaign_recovery.py — from-scratch recovery campaign on the REDESIGNED
(uncollapsed) A_MOD_CLOSE_CONTINUATION_BREAK pool. Research-only; never writes
final_setup_conf.py; no live execution.

Stages (one process, warm caches):
  hypotheses : R1-R8 redesigned setup versions (explainable rule packs) x exit anchors
  sweeps     : one-knob sweeps (numeric quantiles + structural/regime binaries) x anchors
  search     : Optuna TPE over masks (<=3 AND terms) + slot guards + top_n + exit grids
  confirm    : full-TRAIN band [1.30,1.80] gate -> single TEST scoring (budget) +
               domination caps + neighborhood/dropout robustness
  rescue     : if nothing passes — simplified single/double-term configs around the best
               exits + a second seeded TPE round, re-confirmed under the same gate

Anti-overfit: FIT/VAL band objective (tent 1.70, gap penalty 0.80), TRAIN-only quantiles,
TEST never used for tuning, budget-capped TEST evaluations.

Usage: py -3.12 campaign_recovery.py [--trials 3000] [--time_budget_min 45] [--seed 17]
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
MIN_SLOTS = ["10:00", "10:30", "11:00", "12:00"]
MAX_SLOTS = ["11:30", "12:30", "13:30", "14:30"]
QGRID = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
SWEEP_Q = [0.1, 0.3, 0.5, 0.7, 0.9]
SWEEP_ANCHORS = [(0.70, 1.50), (1.00, 2.00)]

BASE_FEATS = ["rs_pct", "vol_ratio", "atr_pct", "body_pct", "close_loc",
              "vwap_dist_atr", "quality_score", "signal_range_pct",
              "upper_wick_pct", "lower_wick_pct", "wick_skew_pct"]
BINARY_FEATS = ["x_ema20_gt50", "x_ema_stack", "x_macd_above_sig", "x_above_pdh",
                "x_fresh_break", "x_prev_pullback", "x_first_break_of_day",
                "x_reg_bull", "x_reg_notbear", "x_reg_bulltrend"]
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
                       "guard": cfg["guard"]}, sort_keys=True)


def cfg_str(cfg):
    m = ";".join(f"{a}{o}{b}" for a, o, b in cfg["mask_terms"]) or "-"
    g = json.dumps(cfg["guard"]) if cfg["guard"] else "-"
    return f"SL{cfg['sl']}/T{cfg['tgt']} mask[{m}] g{g}"


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
    print(f"[rec] wrote {WORK / name}", flush=True)


# ---------------------------------------------------------------------------
def prepare(pool_dir: Path):
    tt.POOL_DIRS = [str(pool_dir)]
    tt.SLIPPAGE_BPS = 15.0
    tt.MAX_POSITIONS = 20
    tt.DAILY_LOSS_RS = 0.0
    tt._entry.cache_clear(); tt._resolve_full.cache_clear(); tt._premom.cache_clear()

    pool = tt.load_pool()
    pool = pool[pool["setup"].astype(str).str.upper().eq(SETUP)].copy()
    # regime binaries (deployable as detector conditions; AND-mask-compatible)
    rg = pool["regime"].astype(str).str.upper()
    pool["x_reg_bull"] = rg.eq("BULL").astype(float)
    pool["x_reg_notbear"] = (~rg.eq("BEAR")).astype(float)
    pool["x_reg_bulltrend"] = rg.isin(["BULL", "TREND"]).astype(float)

    train_raw = pool[(pool["_day"] >= TRAIN_START) & (pool["_day"] <= TRAIN_END)].reset_index(drop=True)
    test_raw = pool[(pool["_day"] >= TEST_START) & (pool["_day"] <= TEST_END)].reset_index(drop=True)
    t0 = time.time()
    train = tt.attach_entries(train_raw)
    test = tt.attach_entries(test_raw)
    print(f"[rec] attach: train {len(train_raw)}->{len(train)} test {len(test_raw)}->{len(test)} "
          f"in {time.time()-t0:.0f}s", flush=True)

    tr_sessions = sorted(train["_day"].dt.strftime("%Y-%m-%d").unique())
    n_fit = max(1, int(round(FIT_FRAC * len(tr_sessions))))
    fit_s, val_s = tr_sessions[:n_fit], tr_sessions[n_fit:]
    fit = train[train["_day"].dt.strftime("%Y-%m-%d").isin(fit_s)].reset_index(drop=True)
    val = train[train["_day"].dt.strftime("%Y-%m-%d").isin(val_s)].reset_index(drop=True)

    feats = list(BASE_FEATS) + ["x_bar_i", "x_break_rank_day"]
    for c in sorted(train.columns):
        if not c.startswith("x_") or c in BINARY_FEATS or c in feats:
            continue
        s = pd.to_numeric(train[c], errors="coerce")
        if s.notna().mean() >= MIN_COVERAGE and s.nunique() > 8:
            feats.append(c)
    mask_quant = {}
    for f in feats:
        if f not in train.columns:
            continue
        s = pd.to_numeric(train[f], errors="coerce").dropna()
        if len(s) >= 8 and s.nunique() > 1:
            mask_quant[f] = {q: float(s.quantile(q)) for q in QGRID}
    binaries = [b for b in BINARY_FEATS if b in train.columns
                and pd.to_numeric(train[b], errors="coerce").notna().mean() >= MIN_COVERAGE
                and pd.to_numeric(train[b], errors="coerce").mean() not in (0.0, 1.0)]

    STATE.update({"TRAIN": train, "TEST": test, "FIT": fit, "VAL": val,
                  "fit_sessions": fit_s, "val_sessions": val_s,
                  "mask_quant": mask_quant, "binaries": binaries})
    print(f"[rec] FIT {fit_s[0]}..{fit_s[-1]} n={len(fit)} | VAL {val_s[0]}..{val_s[-1]} n={len(val)} "
          f"| TEST n={len(test)}", flush=True)
    print(f"[rec] searchable: {len(mask_quant)} numeric + {len(binaries)} binary "
          f"({binaries})", flush=True)


# ---------------------------------------------------------------------------
def redesign_packs():
    q = STATE["mask_quant"]

    def thr(f, qq):
        return round(float(q[f][qq]), 6) if f in q else None

    R = []

    def add(name, why, mask, guard=None):
        R.append((name, why, [t for t in mask if t[2] is not None], guard))

    add("R1_uncollapsed_card", "the card itself, all regimes, uncollapsed — true baseline", [])
    add("R2_notbear", "continuation long only when the tape is NOT bear — index-beta alignment",
        [("x_reg_notbear", ">=", 0.5)])
    add("R2b_bulltrend", "strict tape alignment: BULL or strong TREND days only",
        [("x_reg_bulltrend", ">=", 0.5)])
    add("R3_first_break", "only the FIRST qualifying break per ticker-day — no chasing repeats",
        [("x_first_break_of_day", ">=", 0.5)])
    add("R4_fresh_break", "prior bar had NOT already broken — join fresh, not bar #3 of a run",
        [("x_fresh_break", ">=", 0.5)])
    add("R5_pullback_then_break", "two-stage: prior bar was a pullback/red, this bar breaks — spring pattern",
        [("x_prev_pullback", ">=", 0.5)])
    add("R6_morning", "first 90 scannable minutes only (10:00-11:30) — continuation before lunch chop",
        [], guard={"max_slot": "11:30"})
    add("R7_aligned_thrust", "not-bear tape + real volume thrust + fresh",
        [("x_reg_notbear", ">=", 0.5), ("vol_ratio", ">=", thr("vol_ratio", 0.7)),
         ("x_fresh_break", ">=", 0.5)])
    add("R8_ranked_top1", "strongest signal per slot only (vwap_dist_atr ranked), morning",
        [], guard={"top_n": 1, "max_slot": "12:30"})
    add("R23_notbear_first", "not-bear + first break of day",
        [("x_reg_notbear", ">=", 0.5), ("x_first_break_of_day", ">=", 0.5)])
    add("R24_notbear_fresh", "not-bear + fresh break",
        [("x_reg_notbear", ">=", 0.5), ("x_fresh_break", ">=", 0.5)])
    add("R26_notbear_morning", "not-bear + morning window",
        [("x_reg_notbear", ">=", 0.5)], guard={"max_slot": "11:30"})
    add("R2b4_bulltrend_fresh", "bull/trend tape + fresh break",
        [("x_reg_bulltrend", ">=", 0.5), ("x_fresh_break", ">=", 0.5)])
    add("R35_first_pullback", "first break that follows a pullback bar",
        [("x_first_break_of_day", ">=", 0.5), ("x_prev_pullback", ">=", 0.5)])
    return R


def stage_hypotheses(anchors=None):
    anchors = anchors or SWEEP_ANCHORS + [(0.50, 1.00)]
    rows = []
    for name, why, mask, guard in redesign_packs():
        for sl, tgt in anchors:
            cfg = mk_cfg(sl=sl, tgt=tgt, mask=mask, guard=guard)
            sc, nf, pf_f, nv, pf_v = score_cfg(cfg)
            dec = "shortlist" if (nf >= FV_FLOOR and nv >= FV_FLOOR and min(pf_f, pf_v) >= 0.9) else "reject"
            log_iter("redesign", f"{name}@SL{sl}/T{tgt}", why,
                     fv_pack(nf, pf_f), fv_pack(nv, pf_v), decision=dec,
                     fail_class=("" if dec == "shortlist" else "FIT/VAL PF < 0.9 or too thin"),
                     next_action=("sweep on skeleton / confirm" if dec == "shortlist" else "drop"))
            rows.append({"name": name, "why": why, "sl": sl, "tgt": tgt,
                         "mask": [list(t) for t in mask], "guard": guard,
                         "fit_n": nf, "fit_pf": round(pf_f, 3),
                         "val_n": nv, "val_pf": round(pf_v, 3), "score": round(float(sc), 4),
                         "decision": dec})
            print(f"[rec-hyp] {name}@SL{sl}/T{tgt}: FIT {nf}/{pf_f:.3f} VAL {nv}/{pf_v:.3f} -> {dec}",
                  flush=True)
    save_json("hypotheses.json", rows)
    return rows


# ---------------------------------------------------------------------------
def stage_sweeps():
    res = []
    for f, qs in STATE["mask_quant"].items():
        for qq in SWEEP_Q:
            t = round(float(qs[qq]), 6)
            for op in (">=", "<="):
                for sl, tgt in SWEEP_ANCHORS:
                    cfg = mk_cfg(sl=sl, tgt=tgt, mask=[(f, op, t)])
                    sc, nf, pf_f, nv, pf_v = score_cfg(cfg)
                    dec = ("keep" if (nf >= FV_FLOOR and nv >= FV_FLOOR and min(pf_f, pf_v) >= 1.0)
                           else "reject")
                    res.append({"label": f"{f}{op}{t}(q{qq})@SL{sl}/T{tgt}", "feat": f,
                                "fit_n": nf, "fit_pf": round(pf_f, 3),
                                "val_n": nv, "val_pf": round(pf_v, 3),
                                "score": round(float(sc), 4), "decision": dec})
                    log_iter("sweep", f"{f}{op}{t}@SL{sl}/T{tgt}", "single-term sweep",
                             fv_pack(nf, pf_f), fv_pack(nv, pf_v), decision=dec,
                             fail_class=("" if dec == "keep" else "weak FIT/VAL"),
                             next_action=("combo pool" if dec == "keep" else "drop"))
        print(f"[rec-sweep] {f} done", flush=True)
    for b in STATE["binaries"]:
        for op in (">=", "<="):
            for sl, tgt in SWEEP_ANCHORS:
                cfg = mk_cfg(sl=sl, tgt=tgt, mask=[(b, op, 0.5)])
                sc, nf, pf_f, nv, pf_v = score_cfg(cfg)
                dec = ("keep" if (nf >= FV_FLOOR and nv >= FV_FLOOR and min(pf_f, pf_v) >= 1.0)
                       else "reject")
                res.append({"label": f"{b}{op}0.5@SL{sl}/T{tgt}", "feat": b,
                            "fit_n": nf, "fit_pf": round(pf_f, 3),
                            "val_n": nv, "val_pf": round(pf_v, 3),
                            "score": round(float(sc), 4), "decision": dec})
                log_iter("sweep", f"{b}{op}0.5@SL{sl}/T{tgt}", "binary flag sweep",
                         fv_pack(nf, pf_f), fv_pack(nv, pf_v), decision=dec,
                         fail_class=("" if dec == "keep" else "weak FIT/VAL"),
                         next_action=("combo pool" if dec == "keep" else "drop"))
    save_json("sweeps.json", res)
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
            term = (f, ">=", 0.5)      # binaries are one-sided structural requirements
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


def stage_search(trials, seed, time_budget_min, tag="optuna_rec"):
    trial_rows = []
    pool: dict[str, dict] = {}

    def consider(cfg, tg):
        sc, nf, pf_f, nv, pf_v = score_cfg(cfg)
        trial_rows.append({"tag": tg, "sl": cfg["sl"], "tgt": cfg["tgt"],
                           "mask": ";".join(f"{a}{o}{b}" for a, o, b in cfg["mask_terms"]) or "-",
                           "guard": json.dumps(cfg["guard"]) if cfg["guard"] else "-",
                           "fit_n": nf, "fit_pf": round(pf_f, 3),
                           "val_n": nv, "val_pf": round(pf_v, 3), "score": round(float(sc), 4)})
        k = cfg_key(cfg)
        if k not in pool or sc > pool[k]["score"]:
            pool[k] = {"score": float(sc), "cfg": cfg, "fit_pf": pf_f, "val_pf": pf_v,
                       "fit_n": nf, "val_n": nv, "tag": tg}
        return sc

    t0 = time.time()
    if HAVE_OPTUNA:
        def objective(trial):
            return consider(_suggest(trial), tag)
        study = optuna.create_study(direction="maximize",
                                    sampler=optuna.samplers.TPESampler(seed=seed,
                                                                       n_startup_trials=100))
        study.optimize(objective, n_trials=trials, timeout=time_budget_min * 60.0)
        n_done = len(study.trials)
        engine = "Optuna TPE"
    else:
        print("Optuna unavailable; using seeded random search fallback.", flush=True)
        rng = random.Random(seed); n_done = 0
        for _ in range(trials):
            if time.time() - t0 > time_budget_min * 60.0:
                break
            consider(_suggest(_RandTrial(rng)), "random_rec")
            n_done += 1
        engine = "seeded random search"
    print(f"[rec] search({tag}): {n_done} trials via {engine} in {time.time()-t0:.0f}s", flush=True)

    pd.DataFrame(trial_rows).sort_values("score", ascending=False).to_csv(
        WORK / f"trials_{tag}.csv", index=False)
    ranked = sorted(pool.values(), key=lambda x: -x["score"])
    for r in ranked[:60]:
        dec = "confirm-queue" if r["score"] > 0.9 else "reject"
        log_iter("combo-tpe", cfg_str(r["cfg"]), f"TPE combination ({tag})",
                 fv_pack(r["fit_n"], r["fit_pf"]), fv_pack(r["val_n"], r["val_pf"]),
                 decision=dec, fail_class=("" if dec != "reject" else "band score <= 0.9"),
                 next_action=("stage confirm" if dec != "reject" else "drop"))
    save_json(f"combos_{tag}.json", [{**{k: v for k, v in r.items() if k != "cfg"},
                                      "cfg": {k: v for k, v in r["cfg"].items() if k != "status"}}
                                     for r in ranked[:60]])
    return ranked


# ---------------------------------------------------------------------------
def stage_confirm(ranked, test_budget=10, max_confirm=20, prior=None):
    global TEST_EVALS
    results = list(prior or [])
    seen = {json.dumps([sorted(map(list, (r.get("cfg") or {}).get("mask_terms", []))),
                        (r.get("cfg") or {}).get("guard")], sort_keys=True)
            for r in results if r.get("cfg")}
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
            log_iter("confirm", cfg_str(cfg), "full-TRAIN confirmation",
                     fv_pack(r["fit_n"], r["fit_pf"]), fv_pack(r["val_n"], r["val_pf"]),
                     trainm=trainm, decision="reject", fail_class=rec["verdict"],
                     next_action="next candidate")
            results.append(rec)
            continue
        TEST_EVALS += 1
        mTE = full_eval(cfg, STATE["TEST"])
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
        log_iter("confirm+test", cfg_str(cfg), "full-TRAIN in band -> single TEST scoring",
                 fv_pack(r["fit_n"], r["fit_pf"]), fv_pack(r["val_n"], r["val_pf"]),
                 trainm=trainm, testm=testm,
                 decision=("KEEP-CANDIDATE" if passed else "reject"),
                 fail_class=("" if passed else rec["verdict"]),
                 next_action=("write candidate json" if passed else "next candidate"))
        results.append(rec)
    save_json("confirmations.json", results)
    return results


# ---------------------------------------------------------------------------
def stage_rescue(sweeps, hyp, seed):
    """Simplified/derived configs + a second TPE round."""
    print("[rec] rescue loop", flush=True)
    pool: dict[str, dict] = {}

    def consider(cfg, tg):
        sc, nf, pf_f, nv, pf_v = score_cfg(cfg)
        k = cfg_key(cfg)
        if k not in pool or sc > pool[k]["score"]:
            pool[k] = {"score": float(sc), "cfg": cfg, "fit_pf": pf_f, "val_pf": pf_v,
                       "fit_n": nf, "val_n": nv, "tag": tg}
        log_iter("rescue", cfg_str(cfg), tg, fv_pack(nf, pf_f), fv_pack(nv, pf_v),
                 decision=("shortlist" if sc > 0.8 else "reject"),
                 fail_class=("" if sc > 0.8 else "band score <= 0.8"),
                 next_action=("confirm" if sc > 0.8 else "drop"))

    top_terms = sorted([r for r in sweeps if r["decision"] == "keep"], key=lambda r: -r["score"])[:12]
    top_hyp = sorted([h for h in hyp if h["decision"] == "shortlist"], key=lambda h: -h["score"])[:6]
    exits = [(0.5, 1.0), (0.7, 1.25), (0.7, 2.0), (0.85, 1.5), (1.0, 2.0), (1.2, 2.5)]
    for sl, tgt in exits:
        for r in top_terms:
            lab = r["label"].split("@")[0]
            f = lab.split(">=")[0].split("<=")[0]
            op = ">=" if ">=" in lab else "<="
            t = float(lab.split(op)[1].split("(")[0])
            consider(mk_cfg(sl=sl, tgt=tgt, mask=[(f, op, t)]), f"rescue single {lab}")
        for h in top_hyp:
            consider(mk_cfg(sl=sl, tgt=tgt, mask=[tuple(t) for t in h["mask"]],
                            guard=h["guard"]), f"rescue hyp {h['name']}")
    ranked2 = stage_search(1200, seed + 101, 20.0, tag="rescue_tpe")
    ranked = sorted(list(pool.values()) + ranked2, key=lambda x: -x["score"])
    save_json("rescue.json", [{**{k: v for k, v in r.items() if k != "cfg"},
                               "cfg": {k: v for k, v in r["cfg"].items() if k != "status"}}
                              for r in ranked[:40]])
    return ranked


# ---------------------------------------------------------------------------
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pool", default=str(WORK / "pools" / "pool_enriched"))
    ap.add_argument("--trials", type=int, default=3000)
    ap.add_argument("--seed", type=int, default=17)
    ap.add_argument("--time_budget_min", type=float, default=45.0)
    ap.add_argument("--test_budget", type=int, default=10)
    args = ap.parse_args()
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass

    print(f"[rec] optimizer: {'Optuna TPE' if HAVE_OPTUNA else 'Optuna unavailable; using seeded random search fallback.'}",
          flush=True)
    prepare(Path(args.pool))

    hyp = stage_hypotheses()
    save_json("iteration_records.json", ITER_LOG)
    sweeps = stage_sweeps()
    save_json("iteration_records.json", ITER_LOG)
    ranked = stage_search(args.trials, args.seed, args.time_budget_min)
    save_json("iteration_records.json", ITER_LOG)

    hyp_ranked = [{"score": h["score"],
                   "cfg": mk_cfg(sl=h["sl"], tgt=h["tgt"], mask=[tuple(t) for t in h["mask"]],
                                 guard=h["guard"]),
                   "fit_pf": h["fit_pf"], "val_pf": h["val_pf"],
                   "fit_n": h["fit_n"], "val_n": h["val_n"], "tag": f"hyp:{h['name']}"}
                  for h in hyp if h["decision"] == "shortlist"]
    queue = sorted(hyp_ranked + ranked, key=lambda x: -x["score"])
    results = stage_confirm(queue, test_budget=args.test_budget)
    save_json("iteration_records.json", ITER_LOG)

    n_cand = sum(1 for r in results if r.get("verdict") == "CANDIDATE")
    if n_cand == 0 and TEST_EVALS < args.test_budget:
        rranked = stage_rescue(sweeps, hyp, args.seed)
        results = stage_confirm(rranked, test_budget=args.test_budget, prior=results)
        save_json("iteration_records.json", ITER_LOG)
        n_cand = sum(1 for r in results if r.get("verdict") == "CANDIDATE")

    print(f"[rec] DONE. iterations={len(ITER_LOG)} test_evals={TEST_EVALS} candidates={n_cand}",
          flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
