r"""full_loop.py — Stages 3-8 for C_OR_BREAKOUT (LONG) (research-only).

Staged protocol (campaign spec):
  Stage 3: redesign versions (each a logical variant of the setup)
  Stage 4: per-version one-knob-at-a-time sweeps on FIT, checked on VAL
  Stage 5: combination search (Optuna TPE / seeded random fallback) on FIT+VAL band score
  Stage 6: full-TRAIN confirmation of stable FIT/VAL configs
  Stage 7: TEST scored ONCE per confirmed config (only if TRAIN PF in [1.30, 1.80])
  Stage 8: rescue loop around the best TRAIN-confirmed config (simplify / exits / time)

Anti-overfit: search never touches TEST; quantile grids from FIT only; TEST evaluations
capped; every scored config logged to iteration_log.csv.
"""
from __future__ import annotations

import json
import random
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parents[2] / "_shared"))
import recovery_lib as rl  # noqa: E402

SETUP = "C_OR_BREAKOUT"
WORK = HERE.parents[1]
MAX_TEST_EVALS = 15
SEED = 7

try:
    import optuna
    optuna.logging.set_verbosity(optuna.logging.WARNING)
    HAVE_OPTUNA = True
except Exception:
    HAVE_OPTUNA = False


def base_cfg(**kw):
    c = {"sl": 0.90, "tgt": 2.00, "prefilter_terms": [], "mask_terms": [], "premom_terms": [],
         "guard": None, "max_positions": 20, "daily_loss_rs": 0.0}
    c.update(kw)
    return c


def main() -> int:
    t0 = time.time()
    eng = rl.ResearchEngine(SETUP, WORK)
    w = eng.w
    FIT, VAL, TRAIN, TEST = w["FIT"], w["VAL"], w["TRAIN"], w["TEST"]
    log = rl.IterLog(WORK)
    print(f"[loop] optimizer: {'Optuna TPE' if HAVE_OPTUNA else 'Optuna unavailable; using seeded random search fallback.'}")

    def q(feat, qq, df=FIT):
        s = pd.to_numeric(df[feat], errors="coerce").dropna()
        return round(float(s.quantile(qq)), 6) if len(s) else None

    def ev(cfg, wname):
        return eng.eval_cfg(cfg, w[wname], wname=wname)

    def fv(cfg):
        return ev(cfg, "FIT"), ev(cfg, "VAL")

    # ---------------- Stage 3: redesign versions ----------------
    VERSIONS = {
        "V0_raw": base_cfg(),
        "V1_mid_exit": base_cfg(sl=0.90, tgt=1.25),
        "V2_fresh_break": base_cfg(prefilter_terms=[("fire_seq", "<=", 1.0)]),
        "V3_fresh_morning": base_cfg(prefilter_terms=[("fire_seq", "<=", 1.0)],
                                     guard={"max_slot": "12:30"}),
        "V4_rs_leader": base_cfg(prefilter_terms=[("rs_pct", ">=", q("rs_pct", 0.75) or 1.0)]),
        "V5_not_overext": base_cfg(mask_terms=[("vwap_dist_atr", "<=", q("vwap_dist_atr", 0.75) or 2.0)]),
        "V6_vol_band": base_cfg(prefilter_terms=[("vol_ratio", ">=", 1.8), ("vol_ratio", "<=", 3.2)]),
        "V7_candle_quality": base_cfg(prefilter_terms=[("body_pct", ">=", 0.60),
                                                       ("upper_wick_pct", "<=", 0.25)]),
        "V8_bull_tape": base_cfg(mask_terms=[("market_ret_pct", ">=", 0.0)]),
        "V9_ranked_top2": base_cfg(guard={"top_n": 2}),
        "V10_late_drift": base_cfg(guard={"min_slot": "13:00"}),
        "V11_broad_gate": base_cfg(premom_terms=[("sig5_adx_calc", ">=", 30.0), ("pre1_adx", "<=", 25.0)]),
        "V12_fresh_vol": base_cfg(prefilter_terms=[("fire_seq", "<=", 1.0), ("vol_ratio", ">=", 2.0)]),
    }
    stage3 = {}
    for name, cfg in VERSIONS.items():
        mF, mV = fv(cfg)
        sc = rl.score_fit_val(mF, mV)
        stage3[name] = (cfg, mF, mV, sc)
        log.log("S3_versions", name, "version base", cfg, mF, mV,
                decision="keep" if sc > 0 else "weak",
                note=f"FIT {mF['n']}/{mF['net_pf']} VAL {mV['n']}/{mV['net_pf']}")
        print(f"[S3] {name:20s} FIT n={mF['n']:4d} pf={mF['net_pf']:6.3f} | "
              f"VAL n={mV['n']:4d} pf={mV['net_pf']:6.3f} | score {sc:6.3f}", flush=True)
    log.flush()

    # ---------------- Stage 4: per-version one-knob sweeps ----------------
    # knob grids: (kind, feat, op, [relaxed, medium, strict]) — FIT quantiles
    def gq(feat, ops, df=FIT):
        s = pd.to_numeric(df[feat], errors="coerce").dropna()
        if len(s) < 50 or s.nunique() < 5:
            return None
        qs = [0.10, 0.25, 0.50, 0.75, 0.90]
        vals = sorted(set(round(float(s.quantile(x)), 6) for x in qs))
        return [(feat, ops, v) for v in vals]

    MASK_KNOBS = []
    for feat, ops in [("vol_ratio", ">="), ("vol_ratio", "<="), ("quality_score", ">="),
                      ("rs_pct", ">="), ("atr_pct", "<="), ("atr_pct", ">="), ("body_pct", ">="),
                      ("close_loc", ">="), ("vwap_dist_atr", ">="), ("vwap_dist_atr", "<="),
                      ("adx", ">="), ("rsi", ">="), ("rsi", "<="), ("ema20_slope", ">="),
                      ("macd_hist", ">="), ("market_ret_pct", ">="), ("stock_ret", ">="),
                      ("signal_range_pct", ">="), ("signal_range_pct", "<="),
                      ("upper_wick_pct", "<="), ("lower_wick_pct", "<="),
                      ("fresh_age_bars", "<="), ("fire_seq", "<=")]:
        g = gq(feat, ops)
        if g:
            MASK_KNOBS.append(g)
    PM_KNOBS = []
    for feat, ops in [("pm_sig5_adx_calc", ">="), ("pm_pre1_adx", "<="), ("pm_pre5_mom_r", ">="),
                      ("pm_pre5_mom_r", "<="), ("pm_pre3_range_r", "<="), ("pm_pre3_range_r", ">="),
                      ("pm_pre_entry_momentum_score", ">="), ("pm_sig5_rsi_dir", ">="),
                      ("pm_pre3_close_pos", ">="), ("pm_sig5_vol_ratio20", ">="),
                      ("pm_pre2_mom_r", ">="), ("pm_pre10_mom_r", ">=")]:
        g = gq(feat, ops)
        if g:
            PM_KNOBS.append([(f.replace("pm_", ""), o, v) for f, o, v in g])
    EXITS = [(s, t) for s in rl.SL_GRID for t in rl.TGT_GRID]
    GUARDS = [None, {"max_slot": "12:30"}, {"max_slot": "13:30"}, {"min_slot": "11:30"},
              {"min_slot": "12:30"}, {"top_n": 1}, {"top_n": 2}, {"top_n": 3},
              {"max_slot": "13:00", "top_n": 2}]

    # pick versions worth sweeping (score above raw baseline or structurally interesting)
    sweep_names = [n for n, (_, _, _, sc) in stage3.items()
                   if sc >= max(-1.0, stage3["V0_raw"][3])] or list(VERSIONS)[:6]
    if "V2_fresh_break" not in sweep_names:
        sweep_names.append("V2_fresh_break")
    print(f"[S4] sweeping versions: {sweep_names}", flush=True)

    sweep_best = []   # (score, cfg, mF, mV, tag)

    def try_cfg(stage, group, change, cfg):
        mF, mV = fv(cfg)
        sc = rl.score_fit_val(mF, mV)
        dec = "candidate" if (sc > 1.0 and min(mF["n"], mV["n"]) >= rl.FV_FLOOR) else ""
        log.log(stage, group, change, cfg, mF, mV, decision=dec)
        sweep_best.append((sc, json.dumps(cfg, default=str), mF, mV, f"{stage}:{group}:{change}"))
        return sc, mF, mV

    for vname in sweep_names:
        vcfg = stage3[vname][0]
        # exits (one group)
        for sl, tgt in EXITS:
            if (sl, tgt) == (vcfg["sl"], vcfg["tgt"]):
                continue
            c = dict(vcfg); c["sl"], c["tgt"] = sl, tgt
            try_cfg("S4_sweep", vname, f"exit {sl}/{tgt}", c)
        # guards
        for gd in GUARDS:
            if gd == vcfg.get("guard"):
                continue
            c = dict(vcfg); c["guard"] = gd
            try_cfg("S4_sweep", vname, f"guard {gd}", c)
        # single added mask terms
        for grid in MASK_KNOBS:
            for term in grid[::2]:            # relaxed/medium/strict subset
                c = dict(vcfg); c["mask_terms"] = list(vcfg["mask_terms"]) + [term]
                try_cfg("S4_sweep", vname, f"mask +{term[0]}{term[1]}{term[2]}", c)
        # single added premom terms
        for grid in PM_KNOBS:
            for term in grid[::2]:
                c = dict(vcfg); c["premom_terms"] = list(vcfg["premom_terms"]) + [term]
                try_cfg("S4_sweep", vname, f"pm +{term[0]}{term[1]}{term[2]}", c)
        log.flush()
        print(f"[S4] {vname} done ({len(log.rows)} iterations logged, "
              f"{time.time()-t0:,.0f}s elapsed)", flush=True)

    # ---------------- Stage 5: combination search ----------------
    stable = sorted(sweep_best, key=lambda x: -x[0])
    # terms that appeared in top-quartile sweeps → building blocks
    print(f"[S5] combo search over stable building blocks (Optuna={HAVE_OPTUNA})", flush=True)

    def suggest_cfg(trial):
        vname = trial.suggest_categorical("version", sweep_names)
        c = json.loads(json.dumps(stage3[vname][0], default=str))
        c["prefilter_terms"] = [tuple(t) for t in c["prefilter_terms"]]
        c["sl"] = trial.suggest_categorical("sl", rl.SL_GRID)
        c["tgt"] = trial.suggest_categorical("tgt", rl.TGT_GRID)
        n_mask = trial.suggest_int("n_mask", 0, 2)
        mts = list(map(tuple, c["mask_terms"]))
        for i in range(n_mask):
            gi_ = trial.suggest_int(f"mask{i}_grid", 0, len(MASK_KNOBS) - 1)
            ti = trial.suggest_int(f"mask{i}_t", 0, 4)
            grid = MASK_KNOBS[gi_]
            mts.append(grid[min(ti, len(grid) - 1)])
        c["mask_terms"] = mts
        n_pm = trial.suggest_int("n_pm", 0, 1)
        pts = list(map(tuple, c["premom_terms"]))
        for i in range(n_pm):
            gi_ = trial.suggest_int(f"pm{i}_grid", 0, len(PM_KNOBS) - 1)
            ti = trial.suggest_int(f"pm{i}_t", 0, 4)
            grid = PM_KNOBS[gi_]
            pts.append(grid[min(ti, len(grid) - 1)])
        c["premom_terms"] = pts
        gsel = trial.suggest_int("guard", 0, len(GUARDS) - 1)
        c["guard"] = GUARDS[gsel]
        return c

    combo_results = []

    def objective(trial):
        c = suggest_cfg(trial)
        mF, mV = fv(c)
        sc = rl.score_fit_val(mF, mV)
        combo_results.append((sc, json.dumps(c, default=str)))
        log.log("S5_combo", "optuna" if HAVE_OPTUNA else "randsearch",
                f"trial {len(combo_results)}", c, mF, mV)
        return sc

    N_TRIALS = 400
    if HAVE_OPTUNA:
        study = optuna.create_study(direction="maximize",
                                    sampler=optuna.samplers.TPESampler(seed=SEED))
        study.optimize(objective, n_trials=N_TRIALS, timeout=35 * 60)
    else:
        print("Optuna unavailable; using seeded random search fallback.")
        rng = random.Random(SEED)

        class _T:
            def suggest_categorical(self, n, ch): return ch[rng.randrange(len(ch))]
            def suggest_int(self, n, lo, hi): return rng.randint(lo, hi)
        st = time.time()
        for _ in range(N_TRIALS):
            if time.time() - st > 35 * 60:
                break
            objective(_T())
    log.flush()

    # ---------------- Stage 6/7: TRAIN confirmation + TEST (gated, capped) ----------------
    allc = [(sc, cj) for sc, cj, *_ in sweep_best] + combo_results
    allc.sort(key=lambda x: -x[0])
    seen, ordered = set(), []
    for sc, cj in allc:
        if cj not in seen:
            seen.add(cj)
            ordered.append((sc, cj))
    print(f"[S6] confirming top configs on TRAIN (unique configs: {len(ordered)})", flush=True)

    test_evals = 0
    candidates = []
    confirmed = []
    for sc, cj in ordered[:40]:
        if sc < 0.8:
            break
        cfg = json.loads(cj)
        for k in ("prefilter_terms", "mask_terms", "premom_terms"):
            cfg[k] = [tuple(t) for t in (cfg.get(k) or [])]
        mTR = eng.eval_cfg(cfg, TRAIN, wname="TRAIN", day_block=True)
        in_band = (rl.PF_LO <= mTR["net_pf"] <= rl.PF_HI and mTR["n"] >= rl.MIN_TRADES_TRAIN
                   and mTR["net_pnl"] > 0 and rl.dom_ok(mTR))
        note = f"TRAIN {mTR['n']}/{mTR['net_pf']}/Rs{mTR['net_pnl']:,.0f}"
        if not in_band:
            log.log("S6_confirm", "train", "confirm", cfg, None, None, mTR,
                    decision="reject_train", note=note)
            continue
        confirmed.append((sc, cfg, mTR))
        if test_evals >= MAX_TEST_EVALS:
            log.log("S6_confirm", "train", "confirm", cfg, None, None, mTR,
                    decision="in_band_no_test_budget", note=note)
            continue
        test_evals += 1
        mTE = eng.eval_cfg(cfg, TEST, wname="TEST", day_block=True)
        ok = (mTE["net_pf"] > rl.TEST_PF_MIN and mTE["n"] >= rl.MIN_TRADES_TEST
              and mTE["net_pnl"] > 0 and rl.dom_ok(mTE)
              and (mTE["day_block_p"] is None or mTE["day_block_p"] <= 0.10))
        log.log("S7_test", "test", f"test eval #{test_evals}", cfg, None, None, mTR, mTE,
                decision="CANDIDATE" if ok else "reject_test",
                note=note + f" | TEST {mTE['n']}/{mTE['net_pf']}/Rs{mTE['net_pnl']:,.0f} dbp={mTE['day_block_p']}")
        print(f"[S7] {'PASS' if ok else 'fail'} {rl.cfg_str(cfg)}\n"
              f"     TRAIN {rl.mline(mTR)}\n     TEST  {rl.mline(mTE)}", flush=True)
        if ok:
            candidates.append({"cfg": cfg, "score_fv": sc,
                               "train": {k: v for k, v in mTR.items() if k != "detail"},
                               "test": {k: v for k, v in mTE.items() if k != "detail"}})
    log.flush()

    # ---------------- Stage 8: rescue loop around best TRAIN-confirmed ----------------
    if not candidates and confirmed:
        print("[S8] rescue loop", flush=True)
        confirmed.sort(key=lambda x: -x[0])
        base = confirmed[0][1]
        rescue_cfgs = []
        # simplify: dropout of each term
        for key in ("prefilter_terms", "mask_terms", "premom_terms"):
            for i in range(len(base.get(key) or [])):
                c = json.loads(json.dumps(base, default=str))
                c[key] = [tuple(t) for t in c[key]]; c[key].pop(i)
                for k2 in ("prefilter_terms", "mask_terms", "premom_terms"):
                    c[k2] = [tuple(t) for t in (c.get(k2) or [])]
                rescue_cfgs.append(("drop " + key, c))
        # exit re-tune
        for sl, tgt in EXITS:
            c = json.loads(json.dumps(base, default=str))
            for k2 in ("prefilter_terms", "mask_terms", "premom_terms"):
                c[k2] = [tuple(t) for t in (c.get(k2) or [])]
            c["sl"], c["tgt"] = sl, tgt
            rescue_cfgs.append((f"exit {sl}/{tgt}", c))
        # time restriction
        for gd in GUARDS:
            c = json.loads(json.dumps(base, default=str))
            for k2 in ("prefilter_terms", "mask_terms", "premom_terms"):
                c[k2] = [tuple(t) for t in (c.get(k2) or [])]
            c["guard"] = gd
            rescue_cfgs.append((f"guard {gd}", c))
        scored = []
        for chg, c in rescue_cfgs:
            mF, mV = fv(c)
            sc = rl.score_fit_val(mF, mV)
            log.log("S8_rescue", "rescue", chg, c, mF, mV)
            scored.append((sc, chg, c))
        scored.sort(key=lambda x: -x[0])
        for sc, chg, c in scored[:6]:
            mTR = eng.eval_cfg(c, TRAIN, wname="TRAIN", day_block=True)
            if not (rl.PF_LO <= mTR["net_pf"] <= rl.PF_HI and mTR["n"] >= rl.MIN_TRADES_TRAIN
                    and mTR["net_pnl"] > 0 and rl.dom_ok(mTR)):
                log.log("S8_rescue", "train", chg, c, None, None, mTR, decision="reject_train")
                continue
            if test_evals >= MAX_TEST_EVALS:
                break
            test_evals += 1
            mTE = eng.eval_cfg(c, TEST, wname="TEST", day_block=True)
            ok = (mTE["net_pf"] > rl.TEST_PF_MIN and mTE["n"] >= rl.MIN_TRADES_TEST
                  and mTE["net_pnl"] > 0 and rl.dom_ok(mTE)
                  and (mTE["day_block_p"] is None or mTE["day_block_p"] <= 0.10))
            log.log("S8_rescue", "test", chg, c, None, None, mTR, mTE,
                    decision="CANDIDATE" if ok else "reject_test")
            if ok:
                candidates.append({"cfg": c, "score_fv": sc,
                                   "train": {k: v for k, v in mTR.items() if k != "detail"},
                                   "test": {k: v for k, v in mTE.items() if k != "detail"}})
        log.flush()

    # ---------------- persist ----------------
    (WORK / "candidates").mkdir(exist_ok=True)
    for i, cand in enumerate(candidates, 1):
        (WORK / "candidates" / f"{SETUP}_candidate_{i:03d}.json").write_text(
            json.dumps(cand, indent=2, default=str), encoding="utf-8")
    summary = {
        "setup": SETUP,
        "optimizer": "Optuna TPE" if HAVE_OPTUNA else "seeded random search fallback",
        "iterations_logged": len(log.rows),
        "test_evals_used": test_evals,
        "n_confirmed_in_band": len(confirmed),
        "n_candidates": len(candidates),
        "elapsed_sec": round(time.time() - t0, 0),
        "best_confirmed": ([{"cfg": rl.cfg_str(c), "train_pf": m["net_pf"], "train_n": m["n"]}
                            for _, c, m in sorted(confirmed, key=lambda x: -x[0])[:5]]),
    }
    (WORK / "run_summary.json").write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
