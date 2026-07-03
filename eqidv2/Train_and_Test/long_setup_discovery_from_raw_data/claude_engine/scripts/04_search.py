r"""04_search.py -- Stage 4-7 staged FIT/VAL search -> TRAIN confirm -> TEST validation.

Greedy coordinate search per family (and pooled ALL): change ONE logical group at a time
(bracket -> time guard -> atr floor -> momentum -> overextension -> candle strength -> guards),
keep the value maximizing the FIT/VAL band objective (anti-overfit), log EVERY trial.
Confirm the family's best on full TRAIN; validate TRAIN-gate passers on TEST (TEST never tuned).

Primary cost = 5 bps/leg (realistic for top-250 liquid names); 15 bps/leg reported as stress.

Writes: ITERATION_LOG.md, trials.csv, search_summary.json, CANDIDATE_CONFIGS.md,
candidates/<NAME>_candidate_XXX.json, and a draft BEST_LONG_SETUP_RECOMMENDATION.md.
Run: py -3.12 Train_and_Test/long_setup_discovery_from_raw_data/scripts/04_search.py
"""
from __future__ import annotations
import json, sys, time
from pathlib import Path
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import lib_long_disc as L

SLIP_PRIMARY = 5.0
SLIP_STRESS = 15.0
MIN_FOLD_TRADES = 25
MIN_TRAIN_TRADES = 60
MIN_TEST_TRADES = 20
LAMBDA = 0.5
PF_CLAMP = 2.0
TRAIN_PF_GATE = 1.15
TEST_PF_PASS = 1.30
DOM_DAY_CAP = 0.50
DOM_SYM_CAP = 0.40
TOP_TRADE_CAP = 0.25

FAMILIES = list(L.FAMILY_LABELS.keys()) + ["ALL"]

MASK_GROUPS = {"atr_floor", "momentum", "overext", "strength"}
STAGES = [
    ("bracket", list(L.BRACKETS) + list(L.VARIANTS),
     "tight bracket / exit-variant selection (headline 0.75/0.75 anchor)"),
    ("time_guard", [None, 660, 690, 720, 750],
     "morning-only: strongest edge (35pp) — afternoon follow-through collapses"),
    ("atr_floor", [None, 0.25, 0.30, 0.35, 0.45],
     "ATR%% floor: need room to reach +0.75%% fast (30pp edge); reject dead-low ATR"),
    ("momentum", [None, ("mom2_pct", ">=", 0.0), ("mom3_pct", ">=", 0.1), ("adx", ">=", 20),
                  ("adx", ">=", 25), ("macd_hist", ">=", 0.0)],
     "momentum continuation (~15pp): prior 2-3 bar push / trend strength"),
    ("overext", [None, ("vwap_dist_atr", "<=", 3.0), ("vwap_dist_atr", "<=", 2.0), ("rsi", "<=", 80)],
     "avoid overextension: far-above-VWAP and overbought pop less"),
    ("strength", [None, ("close_loc", ">=", 0.6), ("body_frac", ">=", 0.5)],
     "candle strength: decisive close near high"),
    ("topn", [None, 1, 2, 3], "top_n strongest per (day,slot) by atr_pct"),
    ("cap_sym", [None, 2, 3], "per-symbol-per-day cap (spread risk, more names)"),
]


def base_cfg(fam):
    return dict(family=fam, bracket="b_075_075", slip_bps=SLIP_PRIMARY, mask=[],
                min_minute=None, max_minute=None, top_n=None, rank_feat="atr_pct",
                max_per_sym_day=None, max_book_concurrent=20, _group_terms={})


def apply_value(cfg, group, value):
    c = json.loads(json.dumps({k: v for k, v in cfg.items() if k != "mask" and k != "_group_terms"}))
    c["mask"] = [list(t) for t in cfg["mask"]]
    c["_group_terms"] = dict(cfg["_group_terms"])
    if group == "bracket":
        c["bracket"] = value
    elif group == "time_guard":
        c["max_minute"] = value
    elif group == "topn":
        c["top_n"] = value
    elif group == "cap_sym":
        c["max_per_sym_day"] = value
    elif group in MASK_GROUPS:
        prior = c["_group_terms"].get(group)
        if prior is not None:
            c["mask"] = [t for t in c["mask"] if not (t[0] == prior[0] and t[1] == prior[1] and t[2] == prior[2])]
            c["_group_terms"].pop(group, None)
        if value is not None:
            term = ["atr_pct", ">=", value] if group == "atr_floor" else list(value)
            c["mask"].append(term)
            c["_group_terms"][group] = term
    return c


def clean_cfg(cfg):
    return {k: v for k, v in cfg.items() if k != "_group_terms"}


def pf_clamped(m):
    return min(m["pf"], PF_CLAMP)


def band_score(fit, val):
    if fit["trades"] < MIN_FOLD_TRADES or val["trades"] < MIN_FOLD_TRADES:
        return -1e9
    a, b = pf_clamped(fit), pf_clamped(val)
    return min(a, b) - LAMBDA * abs(a - b)


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    df = pd.read_parquet(L.RESULTS / "signals_resolved.parquet")
    S = L.load_sessions()
    days = lambda k: set(pd.Timestamp(x) for x in S[k])
    FIT = df[df["_day"].isin(days("fit"))].copy()
    VAL = df[df["_day"].isin(days("val"))].copy()
    TRAIN = df[df["_day"].isin(days("train"))].copy()
    TEST = df[df["_day"].isin(days("test"))].copy()
    nF, nV, nTR, nTE = len(S["fit"]), len(S["val"]), len(S["train"]), len(S["test"])
    print(f"[search] FIT={len(FIT):,}/{nF}d VAL={len(VAL):,}/{nV}d TRAIN={len(TRAIN):,}/{nTR}d TEST={len(TEST):,}/{nTE}d")

    trials = []
    fam_best = {}
    t0 = time.time()

    for fam in FAMILIES:
        cfg = base_cfg(fam)
        fit0 = L.evaluate(FIT, clean_cfg(cfg), nF)
        val0 = L.evaluate(VAL, clean_cfg(cfg), nV)
        cur_score = band_score(fit0, val0)
        # TWO coordinate-descent rounds so the bracket re-optimizes AFTER the filters are set
        # (a 0.75/1.00 bracket pays at a lower win-rate than 0.60/0.60 — order matters).
        for rnd in (1, 2):
            for group, values, reason in STAGES:
                old = {"bracket": cfg["bracket"], "time_guard": cfg["max_minute"], "topn": cfg["top_n"],
                       "cap_sym": cfg["max_per_sym_day"]}.get(group, cfg["_group_terms"].get(group))
                best_local = (cur_score, cfg)
                for v in values:
                    cand = apply_value(cfg, group, v)
                    fit = L.evaluate(FIT, clean_cfg(cand), nF)
                    val = L.evaluate(VAL, clean_cfg(cand), nV)
                    sc = band_score(fit, val)
                    keep = sc > best_local[0] + 1e-9
                    trials.append(dict(
                        family=fam, round=rnd, group=group, old=str(old), new=str(v), reason=reason,
                        fit_n=fit["trades"], fit_pf=fit["pf"], fit_win=fit["win_rate"],
                        val_n=val["trades"], val_pf=val["pf"], val_win=val["win_rate"],
                        score=round(sc, 4) if sc > -1e8 else None,
                        bracket=cand["bracket"], max_minute=cand["max_minute"],
                        mask=";".join(f"{a}{o}{b}" for a, o, b in cand["mask"]) or "-",
                        top_n=cand["top_n"], cap_sym=cand["max_per_sym_day"],
                        keep=bool(keep), next_action="adopt" if keep else "revert"))
                    if keep:
                        best_local = (sc, cand)
                cfg = best_local[1]
                cur_score = best_local[0]
        # confirm on TRAIN, validate on TEST
        tr = L.evaluate(TRAIN, clean_cfg(cfg), nTR)
        grossTR = L.evaluate(TRAIN, {**clean_cfg(cfg), "cost_mode": "gross"}, nTR)
        te = L.evaluate(TEST, clean_cfg(cfg), nTE)
        tr15 = L.evaluate(TRAIN, {**clean_cfg(cfg), "slip_bps": SLIP_STRESS}, nTR)
        te15 = L.evaluate(TEST, {**clean_cfg(cfg), "slip_bps": SLIP_STRESS}, nTE)
        fam_best[fam] = dict(cfg=clean_cfg(cfg), fit=fit0, train=tr, test=te, train15=tr15, test15=te15,
                             gross_train=grossTR, score=cur_score)
        print(f"  {fam:20s} score={cur_score:6.3f} | price-path(0-cost) PF{grossTR['pf']} | "
              f"TRAIN n{tr['trades']} pf{tr['pf']} win{tr['win_rate']} | "
              f"TEST n{te['trades']} pf{te['pf']} win{te['win_rate']} | {cfg['bracket']}")

    # ---- candidate gate -------------------------------------------------------
    def passes(b):
        tr, te = b["train"], b["test"]
        return (tr["trades"] >= MIN_TRAIN_TRADES and tr["pf"] >= TRAIN_PF_GATE and
                te["trades"] >= MIN_TEST_TRADES and te["pf"] >= TEST_PF_PASS and
                te["day_dom"] <= DOM_DAY_CAP and te["sym_dom"] <= DOM_SYM_CAP and
                te["top_trade_share"] <= TOP_TRADE_CAP)
    cands = [(f, b) for f, b in fam_best.items() if passes(b)]
    cands.sort(key=lambda fb: (fb[1]["test"]["pf"], fb[1]["test"]["trades"]), reverse=True)
    print(f"[search] candidates passing TRAIN+TEST gate: {len(cands)} -> {[f for f,_ in cands]}")

    # ---- write trials.csv + search_summary ------------------------------------
    pd.DataFrame(trials).to_csv(L.OUTDIR / "trials.csv", index=False)
    summary = dict(slip_primary_bps=SLIP_PRIMARY, slip_stress_bps=SLIP_STRESS,
                   windows={k: [S[k][0], S[k][-1]] for k in ("fit", "val", "train", "test")},
                   n_trials=len(trials), families={f: {"cfg": b["cfg"],
                   "train": b["train"], "test": b["test"], "train15": b["train15"], "test15": b["test15"],
                   "score": b["score"], "pass": passes(b)} for f, b in fam_best.items()},
                   n_candidates=len(cands), candidates=[f for f, _ in cands])
    L.save_json(L.OUTDIR / "search_summary.json", summary)

    # ---- candidate json files + CANDIDATE_CONFIGS.md --------------------------
    for i, (fam, b) in enumerate(cands, 1):
        name = f"NEW_LONG_{fam}"
        L.save_json(L.CAND / f"{name}_candidate_{i:03d}.json",
                    dict(setup=name, family=fam, label=L.FAMILY_LABELS.get(fam, "pooled union"),
                         config=b["cfg"], train=b["train"], test=b["test"],
                         train_15bps=b["train15"], test_15bps=b["test15"]))

    def fmt(m):
        return (f"n={m['trades']} PF={m['pf']} net=Rs{m['net_pnl']:,.0f} win={m['win_rate']}% "
                f"exp=Rs{m['expectancy']}/tr tgt/sl/eod/time={m['tgt_cnt']}/{m['sl_cnt']}/{m['eod_cnt']}/{m['time_cnt']} "
                f"tpd={m['trades_per_day']} dayDom={m['day_dom']} symDom={m['sym_dom']} "
                f"topTr={m['top_trade_share']} hold={m['avg_hold_min']}m maxDD=Rs{m['max_dd']:,.0f} tie={m['tie_pct']}%")

    cc = ["# CANDIDATE_CONFIGS — fast-momentum LONG (~0.75% symmetric) candidates", "",
          f"Pass gate = TRAIN trades≥{MIN_TRAIN_TRADES} & TRAIN PF≥{TRAIN_PF_GATE} & TEST trades≥{MIN_TEST_TRADES} "
          f"& **TEST PF≥{TEST_PF_PASS}** & dayDom≤{DOM_DAY_CAP} & symDom≤{DOM_SYM_CAP} & topTradeShare≤{TOP_TRADE_CAP}.",
          f"Primary cost = {SLIP_PRIMARY:.0f} bps/leg slippage + statutory NSE; 15 bps/leg shown as stress.", ""]
    if not cands:
        cc.append("**No candidate cleared the TEST gate.** See search_summary.json for the best per-family "
                  "configs and ITERATION_LOG.md for why. Closest near-misses:\n")
        near = sorted(fam_best.items(), key=lambda fb: (fb[1]["test"]["pf"]), reverse=True)[:5]
        for fam, b in near:
            cc += [f"## near-miss {fam} ({L.FAMILY_LABELS.get(fam,'pooled union')})",
                   "```json", json.dumps(b["cfg"], indent=2), "```",
                   f"- TRAIN @5bps: {fmt(b['train'])}", f"- TEST  @5bps: {fmt(b['test'])}",
                   f"- TEST  @15bps: {fmt(b['test15'])}", ""]
    else:
        for i, (fam, b) in enumerate(cands, 1):
            cc += [f"## Candidate {i:03d} — NEW_LONG_{fam} ({L.FAMILY_LABELS.get(fam,'pooled union')})",
                   "```json", json.dumps(b["cfg"], indent=2), "```",
                   f"- TRAIN @5bps: {fmt(b['train'])}", f"- TEST  @5bps: {fmt(b['test'])}",
                   f"- TRAIN @15bps: {fmt(b['train15'])}", f"- TEST  @15bps: {fmt(b['test15'])}",
                   f"- file: candidates/NEW_LONG_{fam}_candidate_{i:03d}.json", ""]
    (L.OUTDIR / "CANDIDATE_CONFIGS.md").write_text("\n".join(cc), encoding="utf-8")

    # ---- ITERATION_LOG.md -----------------------------------------------------
    il = ["# ITERATION_LOG — fast-momentum LONG discovery (staged FIT/VAL search)", "",
          f"Greedy coordinate search per family; ONE logical group changed per step; kept iff it raised the "
          f"FIT/VAL band score = min(PF_fit,PF_val) − {LAMBDA}·|PF_fit−PF_val| (PF clamped to {PF_CLAMP}, "
          f"each fold ≥{MIN_FOLD_TRADES} trades). {len(trials)} trials logged. Cost {SLIP_PRIMARY:.0f} bps/leg.",
          f"FIT {S['fit'][0]}..{S['fit'][-1]} | VAL {S['val'][0]}..{S['val'][-1]} | "
          f"TRAIN {S['train'][0]}..{S['train'][-1]} | TEST {S['test'][0]}..{S['test'][-1]}", ""]
    cur_fam = None
    for t in trials:
        if t["family"] != cur_fam:
            cur_fam = t["family"]
            il.append(f"\n## {cur_fam} — {L.FAMILY_LABELS.get(cur_fam,'pooled union')}")
        sc = "n/a(<min)" if t["score"] is None else f"{t['score']}"
        il.append(f"- [{t['group']}] {t['old']} -> {t['new']} | {('KEEP' if t['keep'] else 'reject')} | "
                  f"FIT n{t['fit_n']}/PF{t['fit_pf']}/win{t['fit_win']} VAL n{t['val_n']}/PF{t['val_pf']}/win{t['val_win']} "
                  f"score={sc} | next={t['next_action']} | _{t['reason']}_")
    for fam, b in fam_best.items():
        il.append(f"\n### {fam} BEST -> TRAIN {fmt(b['train'])}")
        il.append(f"    TEST {fmt(b['test'])}  | passes_gate={passes(b)}")
    (L.OUTDIR / "ITERATION_LOG.md").write_text("\n".join(il), encoding="utf-8")

    print(f"[search] {len(trials)} trials in {time.time()-t0:.0f}s. wrote ITERATION_LOG/CANDIDATE_CONFIGS/"
          f"trials.csv/search_summary.json + {len(cands)} candidate json(s).")
    return fam_best, cands, S


if __name__ == "__main__":
    main()
