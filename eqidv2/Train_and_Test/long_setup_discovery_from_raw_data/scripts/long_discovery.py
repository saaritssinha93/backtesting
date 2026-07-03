r"""long_discovery.py — discover a NEW LONG setup over the union of all LONG raw-candidates.

Treats the WHOLE LONG raw-candidate universe (all source detectors, relabeled to one synthetic setup
NEW_LONG_ALL) as the discovery space and searches a feature-defined LONG selection rule that runs through
the EXACT existing pipeline (next-1min-open entry, 1-min OHLC exit, statutory cost, one-ticker/day dedupe,
portfolio overlay). Search = MASK terms over 5-min pool features + categorical regime + exit SL/target +
entry guards (min_slot/max_slot/top_n) + max_positions/daily_loss. Pre-momentum (1-min, per-row) is left OFF
at this scale for tractability — the pool's 5-min features already cover momentum/trend/volume.

Protocol: FIT/VAL drive (band objective min(FIT_PF,VAL_PF)−λ·gap, PF clamped to 1.70 = anti-overfit);
full TRAIN confirms; TEST scored once for TRAIN-band candidates. Optuna TPE (else seeded random).
Writes trials.csv, search_summary.json, ITERATION_LOG.md, CANDIDATE_CONFIGS.md,
BEST_LONG_SETUP_RECOMMENDATION.md + candidates/<name>.json. No live trades; no final_setup_conf edits.

Run from repo root:
  py -3.12 Train_and_Test/long_setup_discovery_from_raw_data/scripts/long_discovery.py \
      --train_start 2026-04-28 --test_start 2026-06-12 --trials 300 --time_budget_min 25 --seed 7
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

_P = Path(__file__).resolve()
TT_DIR = next(par for par in _P.parents if par.name == "Train_and_Test")
REPO_ROOT = TT_DIR.parent
for _d in (str(REPO_ROOT), str(TT_DIR)):
    if _d not in sys.path:
        sys.path.insert(0, _d)

import setup_train_test as tt          # noqa: E402
# reuse the band-search helpers (full_metrics, _light, build_quantiles, gate, constants)
sys.path.insert(0, str(TT_DIR / "setup_pf_1_4_approval_loop" / "B_AVWAP_RECLAIM_REVERSAL" / "scripts"))
import pf_band_search as pb            # noqa: E402

try:
    import optuna
    optuna.logging.set_verbosity(optuna.logging.WARNING)
    _HAVE_OPTUNA = True
except Exception:
    _HAVE_OPTUNA = False

POOL_DIR = Path(r"C:/TradingData/eqidv2/outputs_ID_v11_unified_pool")
SETUP = "NEW_LONG_ALL"
MASK_FEATS = ["vwap_dist_atr", "vol_ratio", "atr_pct", "body_pct", "close_loc", "upper_wick_pct",
              "lower_wick_pct", "wick_skew_pct", "rs_pct", "quality_score", "signal_range_pct",
              "signal_minute", "rsi", "adx", "macd_hist", "ema20_slope"]
REGIME_CHOICES = [None, "NEUTRAL", "!BULL", "!BEAR", "TREND"]
QGRID = pb.QGRID
SL_GRID = pb.SL_GRID
TGT_GRID = pb.TGT_GRID
MIN_SLOTS = pb.MIN_SLOTS
MAX_SLOTS = pb.MAX_SLOTS
BAND_LO, BAND_HI, GAP_LAMBDA = pb.BAND_LO, pb.BAND_HI, pb.GAP_LAMBDA


def _suggest(trial, mask_quant, max_mask=3):
    def cat(n, c): return trial.suggest_categorical(n, c)
    def integ(n, lo, hi): return trial.suggest_int(n, lo, hi)
    avail = [x for x in MASK_FEATS if x in mask_quant]
    mask_terms = []
    for i in range(integ("n_mask", 1, max_mask) if avail else 0):
        f = cat(f"m{i}_f", avail); op = cat(f"m{i}_o", [">=", "<="]); q = cat(f"m{i}_q", QGRID)
        mask_terms.append([f, op, round(float(mask_quant[f][q]), 6)])
    rg = cat("regime", REGIME_CHOICES)
    if rg:
        mask_terms += ([["regime", "!=", rg[1:]]] if rg.startswith("!") else [["regime", "==", rg]])
    guard = {}
    mn = cat("min_slot", MIN_SLOTS); mx = cat("max_slot", MAX_SLOTS); tn = cat("top_n", [0, 1, 2, 3])
    if mn: guard["min_slot"] = mn
    if mx: guard["max_slot"] = mx
    if tn: guard["top_n"] = int(tn)
    return {"sl": float(cat("sl", SL_GRID)), "tgt": float(cat("tgt", TGT_GRID)),
            "mask_terms": mask_terms, "premom_terms": [], "guard": (guard or None),
            "max_positions": int(cat("max_positions", [10, 20])), "daily_loss_rs": float(cat("daily_loss_rs", [0.0, 5000.0]))}


class _Rand:
    def __init__(s, rng): s.rng = rng
    def suggest_categorical(s, n, c): return c[s.rng.randrange(len(c))]
    def suggest_int(s, n, lo, hi): return s.rng.randint(lo, hi)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--train_start", default="2026-04-28")
    ap.add_argument("--test_start", default="2026-06-12")
    ap.add_argument("--trials", type=int, default=300)
    ap.add_argument("--time_budget_min", type=float, default=25.0)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--slippage_bps", type=float, default=15.0)
    ap.add_argument("--min_fold", type=int, default=15)
    ap.add_argument("--min_train_trades", type=int, default=40)
    ap.add_argument("--min_test_trades", type=int, default=15)
    args = ap.parse_args()
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    except Exception:
        pass
    outdir = TT_DIR / "long_setup_discovery_from_raw_data"
    (outdir / "candidates").mkdir(parents=True, exist_ok=True)
    engine = "Optuna TPE" if _HAVE_OPTUNA else "Optuna unavailable; using seeded random search fallback."
    print(f"[long-disc] optimizer: {engine}")

    tt.POOL_DIRS = [POOL_DIR]; tt.POOL_DIR = POOL_DIR
    pb._set_slip(args.slippage_bps)
    pool = tt.load_pool()
    pool = pool[pool["side"].astype(str).str.upper() == "LONG"].copy()
    pool["setup"] = SETUP                                   # relabel union -> one synthetic setup
    sessions = sorted(pd.Series(pool["_day"].dropna().unique()))
    ts, te = pd.Timestamp(args.train_start), pd.Timestamp(args.test_start)
    TEST_s = [s for s in sessions if s >= te]
    TRAIN_s = [s for s in sessions if ts <= s < (TEST_s[0] if TEST_s else sessions[-1] + pd.Timedelta(days=1))]
    half = len(TRAIN_s) // 2
    FIT_s, VAL_s = TRAIN_s[:half], TRAIN_s[half:]
    span = set(map(pd.Timestamp, TRAIN_s + TEST_s))
    sub = pool[pool["_day"].isin(span)].copy()
    print(f"[long-disc] LONG union rows in window: {len(sub):,} | attaching entries ...")
    sub = tt.attach_entries(sub)

    def _sl(ss): return sub[sub["_day"].isin(set(map(pd.Timestamp, ss)))].copy()
    FIT, VAL, TRAIN, TEST = _sl(FIT_s), _sl(VAL_s), _sl(TRAIN_s), _sl(TEST_s)

    def _d(ss): return f"{pd.Timestamp(ss[0]).date()}..{pd.Timestamp(ss[-1]).date()}"
    print(f"[long-disc] FIT {_d(FIT_s)}({len(FIT_s)}) VAL {_d(VAL_s)}({len(VAL_s)}) "
          f"TRAIN {_d(TRAIN_s)}({len(TRAIN_s)}) TEST {_d(TEST_s)}({len(TEST_s)})")
    print(f"[long-disc] entries: FIT={len(FIT)} VAL={len(VAL)} TRAIN={len(TRAIN)} TEST={len(TEST)}")
    print(f"[long-disc] gate: TRAIN PF in [{BAND_LO},{BAND_HI}] TEST PF>{pb.TEST_PF_MIN} dom<={pb.DOM_CAP} "
          f"min_train_n={args.min_train_trades} min_test_n={args.min_test_trades}")

    # baseline = take ALL LONG (no filter) at a neutral exit, to show the raw universe edge
    base_cfg = {"sl": 0.9, "tgt": 1.5, "mask_terms": [], "premom_terms": [], "guard": None,
                "max_positions": 20, "daily_loss_rs": 0.0}
    bt = pb.full_metrics(SETUP, base_cfg, TRAIN); be = pb.full_metrics(SETUP, base_cfg, TEST)
    print(f"[long-disc] BASELINE (all LONG, 0.9/1.5) TRAIN n{bt['trades']} PF{bt['net_pf']} | TEST n{be['trades']} PF{be['net_pf']}")

    mask_quant = {}
    for f in MASK_FEATS:
        if f in TRAIN.columns:
            s = pd.to_numeric(TRAIN[f], errors="coerce").dropna()
            if len(s) >= 50 and s.nunique() > 5:
                mask_quant[f] = {q: float(s.quantile(q)) for q in QGRID}
    print(f"[long-disc] searchable mask feats: {sorted(mask_quant)}")

    trial_rows, search_iters = [], []
    best = {"score": -1e9, "cfg": None}
    t0 = time.time()

    def record(cfg, sc, nf, pf_f, nv, pf_v, tnum):
        trial_rows.append({"trial": tnum, "sl": cfg["sl"], "tgt": cfg["tgt"],
                           "mask": "; ".join(f"{a}{o}{b}" for a, o, b in cfg["mask_terms"]) or "-",
                           "guard": json.dumps(cfg["guard"]) if cfg["guard"] else "-",
                           "max_positions": cfg["max_positions"], "daily_loss_rs": cfg["daily_loss_rs"],
                           "fit_n": nf, "fit_pf": round(pf_f, 3), "val_n": nv, "val_pf": round(pf_v, 3),
                           "score": round(float(sc), 4), "cfg_json": json.dumps(pb._ser_cfg(cfg))})
        if sc > best["score"] + 1e-9:
            best["score"], best["cfg"] = sc, cfg
            search_iters.append({"n": len(search_iters) + 1, "trial": tnum, "score": round(float(sc), 4),
                                 "fit_n": nf, "fit_pf": round(pf_f, 3), "val_n": nv, "val_pf": round(pf_v, 3),
                                 "cfg": pb._ser_cfg(cfg)})

    def score_cfg(cfg):
        return pb.fitval_score(SETUP, cfg, FIT, VAL, args.min_fold)

    if _HAVE_OPTUNA:
        def objective(trial):
            cfg = _suggest(trial, mask_quant)
            sc, nf, pf_f, nv, pf_v = score_cfg(cfg)
            record(cfg, sc, nf, pf_f, nv, pf_v, trial.number)
            return sc
        study = optuna.create_study(direction="maximize", sampler=optuna.samplers.TPESampler(seed=args.seed))
        study.optimize(objective, n_trials=args.trials, timeout=args.time_budget_min * 60.0)
        n_done = len(study.trials)
    else:
        rng = random.Random(args.seed); n_done = 0
        for k in range(args.trials):
            if time.time() - t0 > args.time_budget_min * 60.0:
                break
            cfg = _suggest(_Rand(rng), mask_quant)
            sc, nf, pf_f, nv, pf_v = score_cfg(cfg)
            record(cfg, sc, nf, pf_f, nv, pf_v, k); n_done += 1
    print(f"[long-disc] completed {n_done} trials | best FIT/VAL score={best['score']:.4f}")

    tr_df = pd.DataFrame(trial_rows).sort_values("score", ascending=False)
    tr_df.to_csv(outdir / "trials.csv", index=False)

    # gate top-K unique configs on TRAIN band + TEST>1.40 + stability
    def _from(c): return {"sl": c["sl"], "tgt": c["tgt"], "mask_terms": c["mask_terms"], "premom_terms": [],
                          "guard": (c.get("entry_guards") or None), "max_positions": c["max_positions"],
                          "daily_loss_rs": c["daily_loss_rs"]}
    seen, cands = set(), []
    ordered = ([best["cfg"]] + [_from(si["cfg"]) for si in reversed(search_iters)]
               + [_from(json.loads(cj)) for cj in tr_df.head(40)["cfg_json"]])
    for cfg in ordered:
        key = json.dumps(pb._ser_cfg(cfg), sort_keys=True)
        if key in seen:
            continue
        seen.add(key)
        g = pb.gate_candidate(SETUP, cfg, TRAIN, TEST, args.min_train_trades, args.min_test_trades)
        if g["pass"]:
            cands.append({"cfg": pb._ser_cfg(cfg), "train": g["train"], "test": g["test"]})
    cands.sort(key=lambda c: (c["test"]["net_pf"], -abs(c["train"]["net_pf"] - 1.5), c["train"]["trades"]), reverse=True)
    for i, c in enumerate(cands, 1):
        (outdir / "candidates" / f"{SETUP}_candidate_{i:03d}.json").write_text(
            json.dumps({"setup": SETUP, "config": c["cfg"],
                        "train": {k: v for k, v in c["train"].items() if k != "detail"},
                        "test": {k: v for k, v in c["test"].items() if k != "detail"}}, indent=2, default=str), encoding="utf-8")
    print(f"[long-disc] candidates passing gate: {len(cands)}")

    # near-miss = best by score (for transparency even if it fails)
    nm = pb.full_metrics(SETUP, best["cfg"], TRAIN); nm_te = pb.full_metrics(SETUP, best["cfg"], TEST)

    def fmt(m): return (f"n={m['trades']} PF={m['net_pf']} net=Rs{m['net_pnl']:,.0f} win={m['win_rate_pct']}% "
                        f"t/s/e={m['tgt_cnt']}/{m['sl_cnt']}/{m['eod_cnt']} tpd={m['trades_per_day']} "
                        f"dayDom={m['day_dom']} symDom={m['sym_dom']} maxDD=Rs{m['max_drawdown']:,.0f}")

    summary = {"setup": SETUP, "optimizer": engine,
               "windows": {"FIT": _d(FIT_s), "VAL": _d(VAL_s), "TRAIN": _d(TRAIN_s), "TEST": _d(TEST_s)},
               "universe_rows_window": int(len(sub)), "n_trials": n_done,
               "baseline_all_long": {"train": {k: v for k, v in bt.items() if k != "detail"},
                                     "test": {k: v for k, v in be.items() if k != "detail"}},
               "best_fitval_score": round(float(best["score"]), 4), "n_candidates": len(cands),
               "best_cfg": pb._ser_cfg(best["cfg"]),
               "best_full": {"train": {k: v for k, v in nm.items() if k != "detail"},
                             "test": {k: v for k, v in nm_te.items() if k != "detail"}}}
    (outdir / "search_summary.json").write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")

    # ITERATION_LOG (search trajectory)
    il = ["# ITERATION_LOG — NEW_LONG_ALL discovery (mask-only band search over the LONG union)", "",
          f"Optimizer: {engine}. Universe = {len(sub):,} LONG raw-candidates in window (all source detectors, "
          "relabeled NEW_LONG_ALL). FIT/VAL drive; TRAIN confirms; TEST scored for TRAIN-band candidates.", "",
          f"- FIT {_d(FIT_s)} | VAL {_d(VAL_s)} | TRAIN {_d(TRAIN_s)} | TEST {_d(TEST_s)}",
          f"- BASELINE all-LONG (0.9/1.5): TRAIN {fmt(bt)} | TEST {fmt(be)}", "",
          "## Best-FIT/VAL-score improvement trajectory"]
    for r in search_iters:
        c = r["cfg"]
        il.append(f"### iter {r['n']} (trial {r['trial']}) score {r['score']} — FIT {r['fit_n']}/{r['fit_pf']} VAL {r['val_n']}/{r['val_pf']}")
        il.append(f"- cfg: SL/Tgt {c['sl']}/{c['tgt']} mask=[{'; '.join(f'{a}{o}{b}' for a,o,b in c['mask_terms']) or '-'}] "
                  f"guard={c['entry_guards'] or '{}'} maxpos={c['max_positions']} dloss={c['daily_loss_rs']}")
    (outdir / "ITERATION_LOG.md").write_text("\n".join(il), encoding="utf-8")

    # CANDIDATE_CONFIGS
    cc = ["# CANDIDATE_CONFIGS — NEW_LONG_ALL", "",
          f"Pass = full-TRAIN PF in [{BAND_LO},{BAND_HI}] AND TEST PF > {pb.TEST_PF_MIN} AND train_n≥{args.min_train_trades} "
          f"AND test_n≥{args.min_test_trades} AND trade/day/symbol dominance ≤ {pb.DOM_CAP}. Net @ {args.slippage_bps:.0f} bps/leg.", ""]
    if not cands:
        cc += ["**No candidate cleared the gate.**", "",
               "### Closest near-miss (best FIT/VAL score)",
               "```json", json.dumps(pb._ser_cfg(best["cfg"]), indent=2), "```",
               f"- TRAIN: {fmt(nm)}", f"- TEST : {fmt(nm_te)}"]
    else:
        for i, c in enumerate(cands, 1):
            cc += [f"## Candidate {i:03d}", "```json", json.dumps(c["cfg"], indent=2), "```",
                   f"- TRAIN: {fmt(c['train'])}", f"- TEST : {fmt(c['test'])}",
                   f"- file: candidates/{SETUP}_candidate_{i:03d}.json", ""]
    (outdir / "CANDIDATE_CONFIGS.md").write_text("\n".join(cc), encoding="utf-8")

    print("\n" + "=" * 88)
    print(f"DONE NEW_LONG_ALL discovery | candidates passing gate: {len(cands)}")
    print(f"  baseline all-LONG TRAIN PF {bt['net_pf']} / TEST PF {be['net_pf']}")
    print(f"  best cfg: SL/Tgt {best['cfg']['sl']}/{best['cfg']['tgt']} "
          f"mask=[{'; '.join(f'{a}{o}{b}' for a,o,b in best['cfg']['mask_terms']) or '-'}] guard={best['cfg']['guard'] or '{}'}")
    print(f"  best  TRAIN {fmt(nm)}")
    print(f"  best  TEST  {fmt(nm_te)}")
    print(f"  artifacts -> {outdir}")
    print("=" * 88)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
