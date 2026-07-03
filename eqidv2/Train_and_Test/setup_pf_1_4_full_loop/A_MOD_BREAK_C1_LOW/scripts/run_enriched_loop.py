r"""run_enriched_loop.py — PHASE 2: enriched-feature search for A_MOD_BREAK_C1_LOW (SHORT).
=============================================================================================
RESEARCH-ONLY. No live trades, no final_setup_conf.py edits. Artifacts stay in this folder.

Phase 1 (run_full_loop.py) searched the 11 scanner-exported features and hit a hard
ceiling: min(FIT,VAL) PF ~0.93. This phase widens the dictionary with 36 CAUSAL
point-in-time 5-minute indicator/context features (enrich_features.py):
RSI/ADX + slopes, EMA20/50 structure, MACD hist, Bollinger position/width, Stoch, CCI,
MFI, OBV slope, volume z, session-VWAP distance/persistence, day-range position,
gap/day-return, C1-break geometry, multi-bar momentum in ATRs, red-streak,
buying/selling pressure, consolidation tightness, range expansion.

Protocol (unchanged discipline):
  Stage E1  standalone single-feature scan on FIT+VAL (baseline exits, no premom)
  Stage E2  Optuna TPE combination search on FIT/VAL band objective
            (<=3 mask + optional regime + <=2 premom + guards + extended exits),
            warm-started from the best E1 features. NEVER sees TEST.
  Stage E3  top distinct finalists -> full TRAIN confirm -> TEST scored ONCE per
            finalist whose TRAIN lands in [1.30, 1.80]  (budget-capped)
  Stage E4  rescue (premom-off, simplification, window restriction) if nothing passes

Run from repo root:
  py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_MOD_BREAK_C1_LOW\scripts\run_enriched_loop.py ^
     --trials 1200 --time_budget_min 60 --seed 11
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
WORK = _HERE.parent
TT_DIR = WORK.parent.parent
REPO = TT_DIR.parent
ENGINE_DIR = TT_DIR / "setup_pf_1_4_approval_loop" / "_engine"
for _p in (str(REPO), str(TT_DIR), str(ENGINE_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import setup_train_test as tt                      # noqa: E402
import pf_band_fitval_loop as eng                  # noqa: E402
from enrich_features import ENRICHED_FEATS         # noqa: E402

try:
    import optuna
    optuna.logging.set_verbosity(optuna.logging.WARNING)
    _HAVE_OPTUNA = True
except Exception:
    _HAVE_OPTUNA = False

SETUP = "A_MOD_BREAK_C1_LOW"
SIDE = "SHORT"
PF_LO, PF_HI = 1.30, 1.80
eng.PF_LO, eng.PF_HI = PF_LO, PF_HI
TEST_PF_MIN = 1.40
TRADE_DOM_CAP, DAY_DOM_CAP, SYM_DOM_CAP = 0.35, 0.40, 0.40
DAY_BLOCK_P_MAX = 0.10
MIN_TRAIN_TRADES = 20
MIN_TEST_TRADES = 5
MAX_TRADES_DAY = 6.0
MIN_TRAIN_TARGET_RATE = 12.0

BASE_FEATS = ["rs_pct", "vol_ratio", "atr_pct", "body_pct", "close_loc", "vwap_dist_atr",
              "quality_score", "signal_range_pct", "upper_wick_pct", "lower_wick_pct",
              "wick_skew_pct"]
ALL_FEATS = BASE_FEATS + list(ENRICHED_FEATS)
PM_FEATS = list(eng.PM_FEATS)
QGRID = list(eng.QGRID)
SL_GRID = [0.50, 0.70, 0.85, 1.00, 1.20, 1.50, 2.00]
TGT_GRID = [0.60, 0.80, 1.00, 1.25, 1.50, 2.00, 2.50, 3.00]
MIN_SLOTS = ["09:30", "09:45", "10:00", "10:30", "11:00", "12:00"]
MAX_SLOTS = ["11:30", "12:00", "12:30", "13:00", "14:00", "14:30"]
TODAY = date.today().isoformat()


def evalm(cfg, df, full=False):
    tt.MAX_POSITIONS = cfg.get("max_positions", 20)
    tt.DAILY_LOSS_RS = cfg.get("daily_loss_rs", 0.0)
    return (eng.full_metrics if full else eng.fast_metrics)(SETUP, cfg, df)


def eval_fast(cfg, df):
    tt.MAX_POSITIONS = cfg.get("max_positions", 20)
    tt.DAILY_LOSS_RS = cfg.get("daily_loss_rs", 0.0)
    return eng._eval_fast(SETUP, cfg, df)


def band_score(pf_f, pf_v, nf, nv, fv_floor, gap_lambda):
    if nf < fv_floor or nv < fv_floor:
        return -5.0 + min(nf, nv) / max(1, fv_floor)
    cf, cv = eng._clamp_pf(pf_f), eng._clamp_pf(pf_v)
    sc = eng.band_reward(min(cf, cv)) - gap_lambda * abs(cf - cv)
    if min(cf, cv) >= PF_LO:
        sc += 0.003 * min(min(nf, nv), 40)
    return sc


def cfg_key(cfg):
    return json.dumps({"sl": cfg["sl"], "tgt": cfg["tgt"],
                       "m": sorted(map(list, cfg["mask_terms"])),
                       "p": sorted(map(list, cfg["premom_terms"])),
                       "g": cfg["guard"] or {}, "mp": cfg.get("max_positions", 20),
                       "dl": cfg.get("daily_loss_rs", 0.0)}, default=str)


def terms_str(terms):
    return "; ".join(f"{a}{o}{b}" for a, o, b in terms) or "(none)"


def dom_ok(m):
    return (m["trade_dom_gross"] is not None and m["trade_dom_gross"] <= TRADE_DOM_CAP
            and m["day_dom"] is not None and m["day_dom"] <= DAY_DOM_CAP
            and m["sym_dom"] is not None and m["sym_dom"] <= SYM_DOM_CAP)


def acceptance(mTR, mTE, robust):
    hard, warn = [], []
    if not (PF_LO <= mTR["net_pf"] <= PF_HI):
        hard.append(f"TRAIN PF {mTR['net_pf']} outside [{PF_LO},{PF_HI}]")
    if mTR["n"] < MIN_TRAIN_TRADES:
        hard.append(f"TRAIN n {mTR['n']} < {MIN_TRAIN_TRADES}")
    if mTR["net_pnl"] <= 0:
        hard.append("TRAIN net PnL not positive")
    if mTR["target_rate"] < MIN_TRAIN_TARGET_RATE:
        hard.append(f"TRAIN target-fill {mTR['target_rate']}% < {MIN_TRAIN_TARGET_RATE}%")
    if mTR["trades_per_day"] > MAX_TRADES_DAY:
        hard.append("TRAIN trades/day above cap")
    if not dom_ok(mTR):
        hard.append("TRAIN domination")
    if mTE["n"] < MIN_TEST_TRADES:
        hard.append(f"TEST n {mTE['n']} < {MIN_TEST_TRADES}")
    else:
        if mTE["net_pf"] <= TEST_PF_MIN:
            hard.append(f"TEST PF {mTE['net_pf']} <= {TEST_PF_MIN}")
        if mTE["net_pnl"] <= 0:
            hard.append("TEST net PnL not positive")
        if not dom_ok(mTE):
            hard.append("TEST domination")
        if mTE["day_block_p"] is None or not np.isfinite(float(mTE["day_block_p"])):
            warn.append("TEST day-block p unavailable")
        elif mTE["day_block_p"] > DAY_BLOCK_P_MAX:
            hard.append(f"TEST day-block p {mTE['day_block_p']} > {DAY_BLOCK_P_MAX}")
        if mTE["trades_per_day"] > MAX_TRADES_DAY:
            hard.append("TEST trades/day above cap")
        if mTE["n"] < 20:
            warn.append(f"TEST n {mTE['n']} < 20 (thin)")
    if robust is not None:
        if not robust["neighbor_pass"]:
            hard.append("neighborhood robustness failed")
        if not robust["dropout_pass"]:
            hard.append("term-dropout robustness failed")
    if mTR["net_pf"] > 1.70:
        warn.append("TRAIN PF in upper band (1.70-1.80)")
    return (len(hard) == 0), hard, warn


def mline(m):
    return (f"n={m['n']} PF={m['net_pf']} net=Rs{m['net_pnl']:,.0f} win%={m['win_rate']} "
            f"avgW=Rs{m['avg_win']:,.0f} avgL=Rs{m['avg_loss']:,.0f} maxDD=Rs{m['max_dd']:,.0f} "
            f"SL/TGT/EOD={m['sl_cnt']}/{m['tgt_cnt']}/{m['eod_cnt']} tgt%={m['target_rate']} "
            f"tpd={m['trades_per_day']} tradeDom={m['trade_dom_gross']} dayDom={m['day_dom']} "
            f"symDom={m['sym_dom']} dbp={m['day_block_p']}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pool", default=str(WORK / "pools" / SETUP))
    ap.add_argument("--train_start", default="2026-03-01")
    ap.add_argument("--train_end", default="2026-05-30")
    ap.add_argument("--test_start", default="2026-06-01")
    ap.add_argument("--test_end", default="2026-07-02")
    ap.add_argument("--fit_frac", type=float, default=0.60)
    ap.add_argument("--trials", type=int, default=1200)
    ap.add_argument("--time_budget_min", type=float, default=60.0)
    ap.add_argument("--seed", type=int, default=11)
    ap.add_argument("--gap_lambda", type=float, default=0.80)
    ap.add_argument("--fv_floor", type=int, default=10)
    ap.add_argument("--max_mask_terms", type=int, default=3)
    ap.add_argument("--max_pm_terms", type=int, default=2)
    ap.add_argument("--n_finalists", type=int, default=8)
    ap.add_argument("--max_test_evals", type=int, default=8)
    ap.add_argument("--search_slippage_bps", type=float, default=15.0)
    ap.add_argument("--pm_quantile_sample", type=int, default=1500)
    ap.add_argument("--min_trades_train", type=int, default=MIN_TRAIN_TRADES)
    ap.add_argument("--neighborhood_pf_min", type=float, default=1.15)
    ap.add_argument("--dropout_pf_min", type=float, default=1.00)
    args = ap.parse_args()
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass
    rng = random.Random(args.seed)

    engine_name = "Optuna TPE" if _HAVE_OPTUNA else "seeded random search"
    if not _HAVE_OPTUNA:
        print("Optuna unavailable; using seeded random search fallback.")
    print(f"[enr] PHASE-2 enriched search — {len(ALL_FEATS)} mask feats | optimizer {engine_name}")

    # ---- load pool + merge enriched features -------------------------------------
    tt.POOL_DIRS = [Path(args.pool)]
    tt.POOL_DIR = Path(args.pool)
    pool = tt.load_pool()
    pool = pool[pool["setup"] == SETUP].copy()
    enr = pd.read_csv(Path(args.pool) / "enriched_features.csv", low_memory=False)
    enr["_sig"] = pd.to_datetime(enr["_sig"])
    pool["_signaive"] = pool["tt_sig_ts"].dt.tz_localize(None)
    before = len(pool)
    pool = pool.merge(enr.rename(columns={"_sig": "_signaive"}), on=["ticker", "_signaive"], how="left")
    cov = pool[list(ENRICHED_FEATS)].notna().all(axis=1).mean()
    print(f"[enr] merged enriched feats: {before} rows, full-coverage share {cov*100:.1f}%")

    sessions = sorted(pd.Series(pool["_day"].dropna().unique()))

    def _in(s, lo, hi):
        return pd.Timestamp(lo) <= s <= pd.Timestamp(hi)
    TRAIN_s = [s for s in sessions if _in(s, args.train_start, args.train_end)]
    TEST_s = [s for s in sessions if _in(s, args.test_start, args.test_end)]
    n_fit = max(1, int(round(len(TRAIN_s) * args.fit_frac)))
    FIT_s, VAL_s = TRAIN_s[:n_fit], TRAIN_s[n_fit:]

    def _rng_lbl(ss):
        return f"{pd.Timestamp(ss[0]).date()}..{pd.Timestamp(ss[-1]).date()}" if ss else "(empty)"
    print(f"[enr] TRAIN {_rng_lbl(TRAIN_s)} ({len(TRAIN_s)}) | FIT {len(FIT_s)} / VAL {len(VAL_s)} | "
          f"TEST {_rng_lbl(TEST_s)} ({len(TEST_s)})")

    span = set(map(pd.Timestamp, TRAIN_s + TEST_s))
    sub = pool[pool["_day"].isin(span)].copy()
    eng._set_slippage(args.search_slippage_bps)
    t0 = time.time()
    sub = tt.attach_entries(sub)
    print(f"[enr] entries attached: {len(sub)} rows in {time.time()-t0:.0f}s")

    def _slice(ss):
        return sub[sub["_day"].isin(set(map(pd.Timestamp, ss)))].copy()
    FIT, VAL, TRAIN, TEST = _slice(FIT_s), _slice(VAL_s), _slice(TRAIN_s), _slice(TEST_s)

    mask_quant = {}
    for f in ALL_FEATS:
        if f in TRAIN.columns:
            s = pd.to_numeric(TRAIN[f], errors="coerce").dropna()
            if len(s) >= 8 and s.nunique() > 1:
                mask_quant[f] = {q: float(s.quantile(q)) for q in QGRID}
    pm_source = TRAIN
    if args.pm_quantile_sample and len(TRAIN) > args.pm_quantile_sample:
        pm_source = TRAIN.sample(n=int(args.pm_quantile_sample), random_state=args.seed).sort_index()
    print(f"[enr] premom quantiles on {len(pm_source)} rows ...")
    pm_recs = []
    for j, r in enumerate(pm_source.itertuples(), 1):
        feats, reason = tt._premom(r.ticker, r.side, r.tt_entry_iso, float(r.tt_fill), 0.90, r.tt_sig_ts.isoformat())
        fd = dict(feats) if not reason else {}
        pm_recs.append({f: fd.get(f, np.nan) for f in PM_FEATS})
        if j % 500 == 0:
            print(f"[enr] premom quantiles {j}/{len(pm_source)}", flush=True)
    pm_df_q = pd.DataFrame(pm_recs)
    pm_quant = {}
    for f in PM_FEATS:
        s = pd.to_numeric(pm_df_q[f], errors="coerce").dropna()
        if len(s) >= 8 and s.nunique() > 1:
            pm_quant[f] = {q: float(s.quantile(q)) for q in QGRID}
    print(f"[enr] searchable mask feats: {len(mask_quant)} | premom: {len(pm_quant)}")

    base_block, base_src = eng.get_baseline_block(SETUP)
    base_cfg = eng.conf_to_cfg(base_block)

    iter_rows = []
    it_no = [0]

    def log_iter(stage, group, change, cfg, mF=None, mV=None, mTR=None, mTE=None, keep="", why=""):
        it_no[0] += 1
        iter_rows.append({
            "iter": it_no[0], "stage": stage, "group": group, "change": change,
            "sl": cfg["sl"], "tgt": cfg["tgt"], "mask": terms_str(cfg["mask_terms"]),
            "premom": terms_str(cfg["premom_terms"]),
            "guard": json.dumps(cfg["guard"]) if cfg["guard"] else "-",
            "fit_n": (mF or {}).get("n"), "fit_pf": (mF or {}).get("net_pf"),
            "val_n": (mV or {}).get("n"), "val_pf": (mV or {}).get("net_pf"),
            "train_n": (mTR or {}).get("n"), "train_pf": (mTR or {}).get("net_pf"),
            "train_net": (mTR or {}).get("net_pnl"),
            "test_n": (mTE or {}).get("n"), "test_pf": (mTE or {}).get("net_pf"),
            "test_net": (mTE or {}).get("net_pnl"),
            "keep": keep, "why": why,
        })

    # ---- Stage E1: standalone single-feature scan --------------------------------
    scan_rows = []
    print(f"[E1] standalone single-feature scan over {len(mask_quant)} features ...")
    t1 = time.time()
    for fi, f in enumerate(sorted(mask_quant), 1):
        for q in QGRID:
            thr = round(mask_quant[f][q], 6)
            for op in (">=", "<="):
                cfg = {"sl": base_cfg["sl"], "tgt": base_cfg["tgt"],
                       "mask_terms": [(f, op, thr)], "premom_terms": [],
                       "guard": None, "status": "OK", "max_positions": 20, "daily_loss_rs": 0.0}
                try:
                    nf, pf_f, _ = eval_fast(cfg, FIT)
                    nv, pf_v, _ = eval_fast(cfg, VAL)
                except Exception as e:
                    scan_rows.append({"feat": f, "op": op, "q": q, "thr": thr, "error": type(e).__name__})
                    continue
                sc = band_score(pf_f, pf_v, nf, nv, args.fv_floor, args.gap_lambda)
                scan_rows.append({"feat": f, "op": op, "q": q, "thr": thr,
                                  "fit_n": nf, "fit_pf": round(pf_f, 3),
                                  "val_n": nv, "val_pf": round(pf_v, 3), "score": round(sc, 4)})
        if fi % 10 == 0:
            print(f"[E1] {fi}/{len(mask_quant)} features, {time.time()-t1:.0f}s", flush=True)
    scan_df = pd.DataFrame(scan_rows).sort_values("score", ascending=False)
    scan_df.to_csv(WORK / "sweeps_enriched.csv", index=False)
    top_feats = list(dict.fromkeys(scan_df.head(30)["feat"])) if not scan_df.empty else []
    print(f"[E1] done {len(scan_rows)} scans in {time.time()-t1:.0f}s; "
          f"top feats: {top_feats[:10]}")
    for _, r in scan_df.head(20).iterrows():
        log_iter("E1-scan", "single-feature", f"{r['feat']}{r['op']}{r['thr']} (q{r['q']})",
                 {"sl": base_cfg["sl"], "tgt": base_cfg["tgt"], "mask_terms": [(r["feat"], r["op"], r["thr"])],
                  "premom_terms": [], "guard": None},
                 {"n": r["fit_n"], "net_pf": r["fit_pf"]}, {"n": r["val_n"], "net_pf": r["val_pf"]},
                 keep="-", why=f"score {r['score']}")

    # ---- Stage E2: TPE combination search ----------------------------------------
    trial_rows = []
    best = {"score": -1e9, "cfg": None}
    seen = {}

    def _suggest_cfg(trial):
        mask_terms = []
        n_mask = trial.suggest_int("n_mask", 0, args.max_mask_terms)
        for i in range(n_mask):
            f = trial.suggest_categorical(f"mask{i}_feat", sorted(mask_quant))
            op = trial.suggest_categorical(f"mask{i}_op", [">=", "<="])
            q = trial.suggest_categorical(f"mask{i}_q", QGRID)
            mask_terms.append((f, op, round(float(mask_quant[f][q]), 6)))
        rg = trial.suggest_categorical("regime_term", ["none", "==BEAR", "!=BEAR", "==NEUTRAL", "!=TREND"])
        if rg != "none":
            mask_terms.append(("regime", rg[:2], rg[2:]))
        premom_terms = []
        n_pm = trial.suggest_int("n_pm", 0, args.max_pm_terms)
        for i in range(n_pm):
            f = trial.suggest_categorical(f"pm{i}_feat", sorted(pm_quant))
            op = trial.suggest_categorical(f"pm{i}_op", [">=", "<="])
            q = trial.suggest_categorical(f"pm{i}_q", QGRID)
            premom_terms.append((f, op, round(float(pm_quant[f][q]), 6)))
        guard = {}
        if trial.suggest_categorical("use_min_slot", [False, True]):
            guard["min_slot"] = trial.suggest_categorical("min_slot", MIN_SLOTS)
        if trial.suggest_categorical("use_max_slot", [False, True]):
            guard["max_slot"] = trial.suggest_categorical("max_slot", MAX_SLOTS)
        tn = trial.suggest_categorical("top_n", [0, 1, 2, 3])
        if tn:
            guard["top_n"] = int(tn)
        return {"sl": float(trial.suggest_categorical("sl", SL_GRID)),
                "tgt": float(trial.suggest_categorical("tgt", TGT_GRID)),
                "mask_terms": mask_terms, "premom_terms": premom_terms,
                "guard": (guard or None), "status": "OK",
                "max_positions": int(trial.suggest_categorical("max_positions", [10, 20])),
                "daily_loss_rs": float(trial.suggest_categorical("daily_loss_rs", [0.0, 4000.0]))}

    def _score_and_record(cfg):
        k = cfg_key(cfg)
        if k in seen:
            return seen[k]
        try:
            nf, pf_f, _ = eval_fast(cfg, FIT)
            nv, pf_v, _ = eval_fast(cfg, VAL)
        except Exception:
            seen[k] = -9.0
            return -9.0
        sc = band_score(pf_f, pf_v, nf, nv, args.fv_floor, args.gap_lambda)
        trial_rows.append({"sl": cfg["sl"], "tgt": cfg["tgt"],
                           "mask": terms_str(cfg["mask_terms"]), "premom": terms_str(cfg["premom_terms"]),
                           "guard": json.dumps(cfg["guard"]) if cfg["guard"] else "-",
                           "max_positions": cfg.get("max_positions"), "daily_loss_rs": cfg.get("daily_loss_rs"),
                           "fit_n": nf, "fit_pf": round(pf_f, 3), "val_n": nv, "val_pf": round(pf_v, 3),
                           "score": round(float(sc), 4), "cfg_json": k})
        if sc > best["score"]:
            best["score"], best["cfg"] = sc, eng._copy_cfg(cfg)
        seen[k] = sc
        return sc

    print(f"[E2] TPE search: {args.trials} trials / {args.time_budget_min} min")
    t2 = time.time()
    if _HAVE_OPTUNA:
        study = optuna.create_study(direction="maximize",
                                    sampler=optuna.samplers.TPESampler(seed=args.seed))
        # warm starts: best E1 features solo + paired with phase-1's best structure
        for f in top_feats[:10]:
            row = scan_df[scan_df["feat"] == f].iloc[0]
            base_e = {"n_mask": 1, "mask0_feat": f, "mask0_op": row["op"], "mask0_q": row["q"],
                      "regime_term": "none", "n_pm": 0, "use_min_slot": False, "use_max_slot": False,
                      "top_n": 0, "sl": 1.0, "tgt": 1.25, "max_positions": 20, "daily_loss_rs": 0.0}
            study.enqueue_trial(base_e)
            study.enqueue_trial({**base_e, "regime_term": "==BEAR", "use_max_slot": True,
                                 "max_slot": "12:00", "top_n": 2, "sl": 1.5, "tgt": 2.5})

        def objective(trial):
            return _score_and_record(_suggest_cfg(trial))
        study.optimize(objective, n_trials=args.trials, timeout=args.time_budget_min * 60.0)
        n_done = len(study.trials)
    else:
        n_done = 0
        for _ in range(args.trials):
            if time.time() - t2 > args.time_budget_min * 60.0:
                break
            _score_and_record(_suggest_cfg(eng._RandTrial(rng)))
            n_done += 1
    tdf = pd.DataFrame(trial_rows).sort_values("score", ascending=False).reset_index(drop=True)
    tdf.to_csv(WORK / "trials_enriched.csv", index=False)
    print(f"[E2] {n_done} trials ({len(tdf)} unique) in {time.time()-t2:.0f}s; "
          f"best score={best['score']:.4f}")

    # ---- Stage E3: finalists ------------------------------------------------------
    finalists, fam_seen, test_evals = [], set(), 0
    for _, r in tdf.iterrows():
        if len(finalists) >= args.n_finalists or float(r["score"]) < 0:
            break
        cfg = json.loads(r["cfg_json"])
        cfg = {"sl": cfg["sl"], "tgt": cfg["tgt"],
               "mask_terms": [(f, op, (thr if isinstance(thr, str) else float(thr)))
                              for f, op, thr in (tuple(t) for t in cfg["m"])],
               "premom_terms": [(f, op, float(thr)) for f, op, thr in (tuple(t) for t in cfg["p"])],
               "guard": (cfg["g"] or None), "status": "OK",
               "max_positions": cfg.get("mp", 20), "daily_loss_rs": cfg.get("dl", 0.0)}
        fam = (cfg["sl"], cfg["tgt"],
               frozenset((f, op) for f, op, _ in cfg["mask_terms"]),
               frozenset((f, op) for f, op, _ in cfg["premom_terms"]),
               frozenset((cfg["guard"] or {}).keys()))
        if fam in fam_seen:
            continue
        fam_seen.add(fam)
        finalists.append({"cfg": cfg, "fitval_score": float(r["score"]),
                          "fit_pf": float(r["fit_pf"]), "val_pf": float(r["val_pf"]),
                          "fit_n": int(r["fit_n"]), "val_n": int(r["val_n"])})

    results = []
    ns = argparse.Namespace(min_trades_train=args.min_trades_train,
                            neighborhood_pf_min=args.neighborhood_pf_min,
                            dropout_pf_min=args.dropout_pf_min)
    for fi, cand in enumerate(finalists, 1):
        cfg = cand["cfg"]
        try:
            mTR = evalm(cfg, TRAIN, full=True)
        except Exception as e:
            results.append({"id": fi, "cfg": cfg, "passed": False,
                            "hard_reasons": [f"eval error {type(e).__name__}"], "warnings": []})
            continue
        in_band = PF_LO <= mTR["net_pf"] <= PF_HI and mTR["n"] >= MIN_TRAIN_TRADES
        rec = {"id": fi, "cfg": cfg, **{f"fitval_{k}": v for k, v in cand.items() if k != "cfg"},
               "train": {k: v for k, v in mTR.items() if k != "detail"}}
        if in_band and test_evals < args.max_test_evals:
            robust = eng.robustness_report(SETUP, cfg, TRAIN, mask_quant, pm_quant, ns)
            mTE = evalm(cfg, TEST, full=True)
            test_evals += 1
            passed, hard, warn = acceptance(mTR, mTE, robust)
            rec.update({"test": {k: v for k, v in mTE.items() if k != "detail"},
                        "robust": {k: robust[k] for k in ("neighbor_pass", "dropout_pass", "passed")},
                        "passed": passed, "hard_reasons": hard, "warnings": warn})
            log_iter("E3-finalist", "combination", f"finalist #{fi}", cfg, None, None, mTR, mTE,
                     keep=("PASS" if passed else "reject"), why="; ".join(hard + warn) or "all gates passed")
            mTR["detail"].to_csv(WORK / f"enr_finalist_{fi:02d}_trades_train.csv", index=False)
            if not mTE["detail"].empty:
                mTE["detail"].to_csv(WORK / f"enr_finalist_{fi:02d}_trades_test.csv", index=False)
        else:
            rec.update({"passed": False,
                        "hard_reasons": [f"TRAIN not in band or too thin (PF {mTR['net_pf']}, n {mTR['n']})"
                                         if not in_band else "TEST budget exhausted"],
                        "warnings": []})
            log_iter("E3-finalist", "combination", f"finalist #{fi}", cfg, None, None, mTR, None,
                     keep="reject", why=rec["hard_reasons"][0])
        results.append(rec)
        print(f"[E3] finalist {fi}: TRAIN {mline(mTR)}"
              + (f"\n[E3] finalist {fi}: TEST  {mline(rec['test'])}" if "test" in rec else ""))

    # ---- Stage E4: rescue -----------------------------------------------------------
    passing = [r for r in results if r.get("passed")]
    if not passing and best["cfg"] is not None:
        print("[E4] rescue loop")
        rescue_cands = []
        bc = best["cfg"]
        for i in range(len(bc["mask_terms"])):
            c = eng._copy_cfg(bc); mt = list(c["mask_terms"]); mt.pop(i); c["mask_terms"] = mt
            rescue_cands.append((f"R-drop-mask-{i}", c))
        for i in range(len(bc["premom_terms"])):
            c = eng._copy_cfg(bc); pt = list(c["premom_terms"]); pt.pop(i); c["premom_terms"] = pt
            rescue_cands.append((f"R-drop-premom-{i}", c))
        c = eng._copy_cfg(bc); c["premom_terms"] = []
        rescue_cands.append(("R-premom-off", c))
        for g in ({"max_slot": "12:00"}, {"min_slot": "10:00"}, {"min_slot": "10:00", "max_slot": "14:00"}):
            c = eng._copy_cfg(bc); gg = dict(c["guard"] or {}); gg.update(g); c["guard"] = gg
            rescue_cands.append((f"R-window-{json.dumps(g)}", c))
        for slv, tgv in ((2.0, 3.0), (1.5, 3.0), (2.0, 2.5)):
            c = eng._copy_cfg(bc); c["sl"], c["tgt"] = slv, tgv
            rescue_cands.append((f"R-exit-{slv}/{tgv}", c))
        scored = []
        for tag, cfg in rescue_cands:
            try:
                nf, pf_f, _ = eval_fast(cfg, FIT)
                nv, pf_v, _ = eval_fast(cfg, VAL)
            except Exception:
                continue
            scored.append((band_score(pf_f, pf_v, nf, nv, args.fv_floor, args.gap_lambda),
                           tag, cfg, nf, pf_f, nv, pf_v))
        for sc, tag, cfg, nf, pf_f, nv, pf_v in sorted(scored, key=lambda x: -x[0]):
            if test_evals >= args.max_test_evals:
                break
            try:
                mTR = evalm(cfg, TRAIN, full=True)
            except Exception:
                continue
            in_band = PF_LO <= mTR["net_pf"] <= PF_HI and mTR["n"] >= MIN_TRAIN_TRADES
            if not in_band:
                log_iter("E4-rescue", tag, "rescue variant", cfg,
                         {"n": nf, "net_pf": round(pf_f, 3)}, {"n": nv, "net_pf": round(pf_v, 3)}, mTR, None,
                         keep="reject", why=f"TRAIN out of band (PF {mTR['net_pf']}, n {mTR['n']})")
                continue
            robust = eng.robustness_report(SETUP, cfg, TRAIN, mask_quant, pm_quant, ns)
            mTE = evalm(cfg, TEST, full=True)
            test_evals += 1
            passed, hard, warn = acceptance(mTR, mTE, robust)
            log_iter("E4-rescue", tag, "rescue variant", cfg,
                     {"n": nf, "net_pf": round(pf_f, 3)}, {"n": nv, "net_pf": round(pf_v, 3)}, mTR, mTE,
                     keep=("PASS" if passed else "reject"), why="; ".join(hard + warn) or "all gates passed")
            rec = {"id": 200 + len(results), "tag": tag, "cfg": cfg,
                   "train": {k: v for k, v in mTR.items() if k != "detail"},
                   "test": {k: v for k, v in mTE.items() if k != "detail"},
                   "robust": {k: robust[k] for k in ("neighbor_pass", "dropout_pass", "passed")},
                   "passed": passed, "hard_reasons": hard, "warnings": warn}
            results.append(rec)
            if passed:
                passing.append(rec)
                break

    # ---- persist -------------------------------------------------------------------
    pd.DataFrame(iter_rows).to_csv(WORK / "iteration_log_enriched.csv", index=False)
    summary = {
        "generated": TODAY, "phase": "2-enriched", "setup": SETUP, "side": SIDE,
        "optimizer": engine_name, "n_feats": len(mask_quant),
        "windows": {"FIT": _rng_lbl(FIT_s), "VAL": _rng_lbl(VAL_s), "TRAIN": _rng_lbl(TRAIN_s),
                    "TEST": _rng_lbl(TEST_s), "n_train_sessions": len(TRAIN_s),
                    "n_test_sessions": len(TEST_s)},
        "band": [PF_LO, PF_HI], "test_pf_min": TEST_PF_MIN,
        "n_scan": len(scan_rows), "n_trials": int(n_done), "n_unique": int(len(tdf)),
        "n_test_evals": test_evals,
        "best_fitval_score": best["score"],
        "best_cfg": (eng.cfg_to_conf_block(SETUP, SIDE, best["cfg"]) if best["cfg"] else None),
        "results": [{k: (v if k != "cfg" else eng.cfg_to_conf_block(SETUP, SIDE, v)) for k, v in r.items()}
                    for r in results],
        "n_passing": len(passing),
    }
    (WORK / "run_summary_enriched.json").write_text(
        json.dumps(tt._json_sanitize(summary), indent=2, default=str), encoding="utf-8")
    print(f"[enr] DONE — {len(passing)} passing candidate(s); "
          f"iterations: scan {len(scan_rows)} + trials {n_done} + confirms {len(iter_rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
