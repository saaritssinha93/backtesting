r"""06_search_ext.py -- EXTENSION search: LONG + SHORT families across the WIDER target /
R-multiple bracket grid, with a slippage sweep (limit-entry 2 bps / market 5 bps / stress 15 bps).

Same staged 2-round FIT/VAL band search as 04, but the bracket stage uses BRACKETS_EXT and
both sides are searched (long families F*, short families S*, pooled ALL_LONG / ALL_SHORT).

Writes EXTENSION_RESULTS.md + ext_search_summary.json + candidates/EXT_*.json (gate passers).
Run: py -3.12 .../claude_engine/scripts/06_search_ext.py
"""
from __future__ import annotations
import json, sys, time
from pathlib import Path
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import lib_long_disc as L

SLIP_PRIMARY, SLIP_LIMIT, SLIP_STRESS = 5.0, 2.0, 15.0
MIN_FOLD, MIN_TRAIN_N, MIN_TEST_N = 25, 60, 20
LAMBDA, PF_CLAMP = 0.5, 2.5
TRAIN_PF_GATE, TEST_PF_PASS = 1.15, 1.30
DOM_DAY, DOM_SYM, TOP_TR = 0.50, 0.40, 0.25

BRK = list(L.BRACKETS_EXT) + list(L.EXT_VARIANTS)
MASK_GROUPS = {"atr_floor", "momentum", "overext", "strength"}
STAGES = [
    ("bracket", BRK, "wider target / R-multiple selection"),
    ("time_guard", [None, 660, 690, 720, 750], "morning-only follow-through edge"),
    ("atr_floor", [None, 0.25, 0.30, 0.35, 0.45], "ATR%% floor: room to reach the target"),
    ("momentum", [None, ("mom2_pct", ">=", 0.0), ("mom3_pct", ">=", 0.1), ("adx", ">=", 20),
                  ("adx", ">=", 25), ("macd_hist", ">=", 0.0)], "momentum/trend strength"),
    ("overext", [None, ("vwap_dist_atr", "<=", 3.0), ("vwap_dist_atr", "<=", 2.0), ("rsi", "<=", 80)],
     "avoid overextension"),
    ("strength", [None, ("close_loc", ">=", 0.6), ("body_frac", ">=", 0.5), ("body_frac", "<=", -0.5)],
     "candle strength (sign auto-picks per side)"),
    ("topn", [None, 1, 2, 3], "top_n per (day,slot)"),
    ("cap_sym", [None, 2, 3], "per-symbol-per-day cap"),
]


def base_cfg(fam, side):
    return dict(family=fam, side=side, bracket="x_075_100", slip_bps=SLIP_PRIMARY, mask=[],
                min_minute=None, max_minute=None, top_n=None, rank_feat="atr_pct",
                max_per_sym_day=None, max_book_concurrent=20, _gt={})


def apply_value(cfg, group, value):
    c = {k: v for k, v in cfg.items() if k not in ("mask", "_gt")}
    c["mask"] = [list(t) for t in cfg["mask"]]
    c["_gt"] = dict(cfg["_gt"])
    if group == "bracket":
        c["bracket"] = value
    elif group == "time_guard":
        c["max_minute"] = value
    elif group == "topn":
        c["top_n"] = value
    elif group == "cap_sym":
        c["max_per_sym_day"] = value
    elif group in MASK_GROUPS:
        prior = c["_gt"].get(group)
        if prior is not None:
            c["mask"] = [t for t in c["mask"] if not (t[0] == prior[0] and t[1] == prior[1] and t[2] == prior[2])]
            c["_gt"].pop(group, None)
        if value is not None:
            term = ["atr_pct", ">=", value] if group == "atr_floor" else list(value)
            c["mask"].append(term); c["_gt"][group] = term
    return c


def clean(cfg):
    return {k: v for k, v in cfg.items() if k != "_gt"}


def band(fit, val):
    if fit["trades"] < MIN_FOLD or val["trades"] < MIN_FOLD:
        return -1e9
    a, b = min(fit["pf"], PF_CLAMP), min(val["pf"], PF_CLAMP)
    return min(a, b) - LAMBDA * abs(a - b)


def fmt(m):
    return (f"n={m['trades']} PF={m['pf']} net=Rs{m['net_pnl']:,.0f} win={m['win_rate']}% exp=Rs{m['expectancy']} "
            f"tgt/sl/eod/time={m['tgt_cnt']}/{m['sl_cnt']}/{m['eod_cnt']}/{m['time_cnt']} tpd={m['trades_per_day']} "
            f"dayDom={m['day_dom']} symDom={m['sym_dom']} topTr={m['top_trade_share']} hold={m['avg_hold_min']}m")


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    df = pd.read_parquet(L.RESULTS / "signals_resolved_ext.parquet")
    S = L.load_sessions()
    dset = lambda k: set(pd.Timestamp(x) for x in S[k])
    FIT, VAL = df[df["_day"].isin(dset("fit"))], df[df["_day"].isin(dset("val"))]
    TRAIN, TEST = df[df["_day"].isin(dset("train"))], df[df["_day"].isin(dset("test"))]
    nF, nV, nTR, nTE = len(S["fit"]), len(S["val"]), len(S["train"]), len(S["test"])
    fams = ([(k, "LONG") for k in L.FAMILY_LABELS] + [("ALL_LONG", "LONG")]
            + [(k, "SHORT") for k in L.SHORT_FAMILY_LABELS] + [("ALL_SHORT", "SHORT")])
    print(f"[ext-search] rows TRAIN={len(TRAIN):,} TEST={len(TEST):,} | {len(fams)} family×side")

    results, t0 = {}, time.time()
    for fam, side in fams:
        cfg = base_cfg(fam, side)
        f0, v0 = L.evaluate(FIT, clean(cfg), nF), L.evaluate(VAL, clean(cfg), nV)
        cur = band(f0, v0)
        for _ in (1, 2):
            for group, values, _r in STAGES:
                best = (cur, cfg)
                for val in values:
                    cand = apply_value(cfg, group, val)
                    fit, vv = L.evaluate(FIT, clean(cand), nF), L.evaluate(VAL, clean(cand), nV)
                    sc = band(fit, vv)
                    if sc > best[0] + 1e-9:
                        best = (sc, cand)
                cfg, cur = best[1], best[0]
        tr = L.evaluate(TRAIN, clean(cfg), nTR)
        te = L.evaluate(TEST, clean(cfg), nTE)
        te2 = L.evaluate(TEST, {**clean(cfg), "slip_bps": SLIP_LIMIT}, nTE)
        te15 = L.evaluate(TEST, {**clean(cfg), "slip_bps": SLIP_STRESS}, nTE)
        grossTR = L.evaluate(TRAIN, {**clean(cfg), "cost_mode": "gross"}, nTR)
        results[f"{fam}"] = dict(cfg=clean(cfg), side=side, train=tr, test=te, test_2bps=te2,
                                 test_15bps=te15, gross_train=grossTR, score=cur)
        print(f"  {fam:18s}[{side[0]}] gross{grossTR['pf']:.2f} | TRAIN n{tr['trades']} PF{tr['pf']} win{tr['win_rate']} | "
              f"TEST PF{te['pf']}(2bps {te2['pf']}/15bps {te15['pf']}) win{te['win_rate']} | {cfg['bracket']}")

    def passes(b):
        tr, te = b["train"], b["test"]
        return (tr["trades"] >= MIN_TRAIN_N and tr["pf"] >= TRAIN_PF_GATE and te["trades"] >= MIN_TEST_N
                and te["pf"] >= TEST_PF_PASS and te["day_dom"] <= DOM_DAY and te["sym_dom"] <= DOM_SYM
                and te["top_trade_share"] <= TOP_TR)
    cands = sorted([(f, b) for f, b in results.items() if passes(b)],
                   key=lambda fb: fb[1]["test"]["pf"], reverse=True)
    print(f"[ext-search] gate passers (TEST PF>={TEST_PF_PASS} @5bps): {len(cands)} -> {[f for f,_ in cands]}")

    L.save_json(L.OUTDIR / "ext_search_summary.json",
                dict(slips=[SLIP_LIMIT, SLIP_PRIMARY, SLIP_STRESS], n_family_side=len(fams),
                     results={f: {k: v for k, v in b.items()} for f, b in results.items()},
                     n_candidates=len(cands), candidates=[f for f, _ in cands]))
    for i, (fam, b) in enumerate(cands, 1):
        L.save_json(L.CAND / f"EXT_{b['side']}_{fam}_candidate_{i:03d}.json",
                    dict(setup=f"NEW_{b['side']}_{fam}", side=b["side"], config=b["cfg"],
                         train=b["train"], test=b["test"], test_2bps=b["test_2bps"], test_15bps=b["test_15bps"]))

    # report
    out = ["# EXTENSION_RESULTS — wider targets, limit-entry slippage, and the SHORT side", "",
           "Beyond the tight ±0.75% theme (user-approved follow-up). Same engine, same FIT/VAL→TRAIN→TEST",
           "protocol, 1-min intrabar resolution (SL-first tie). Three axes tested together:",
           "1. **Wider targets / R-multiples** — bracket grid up to 1.0/3.0 (the small +0.10% price-path edge",
           "   should clear cost if the target is large enough).",
           "2. **Limit-entry (lower slippage)** — TEST PF reported at 2 bps/leg (limit proxy), 5 bps (market), 15 bps.",
           "3. **SHORT side** — 10 mirror families + pooled ALL_SHORT.", "",
           f"Universe top-{L.load_universe()['n']} liquid. TRAIN {S['train'][0]}..{S['train'][-1]} / "
           f"TEST {S['test'][0]}..{S['test'][-1]}. Gate = TRAIN PF≥{TRAIN_PF_GATE}, TEST PF≥{TEST_PF_PASS}@5bps, "
           f"trades & dominance stable.", "",
           "## Best per family×side (after 2-round FIT/VAL search)",
           "| family | side | bracket | gross(0-cost) PF | TRAIN PF (n) | TEST PF @2bps | @5bps | @15bps | TEST win% | pass |",
           "|---|---|---|---:|---:|---:|---:|---:|---:|:--:|"]
    for f, b in sorted(results.items(), key=lambda kv: kv[1]["test"]["pf"], reverse=True):
        tr, te = b["train"], b["test"]
        out.append(f"| {f} | {b['side']} | {b['cfg']['bracket']} | {b['gross_train']['pf']} | "
                   f"{tr['pf']} ({tr['trades']}) | {b['test_2bps']['pf']} | {te['pf']} | {b['test_15bps']['pf']} | "
                   f"{te['win_rate']} | {'✅' if passes(b) else '—'} |")
    out += ["", "## Verdict"]
    if cands:
        out.append(f"**{len(cands)} configuration(s) cleared the gate** (TEST PF≥{TEST_PF_PASS}@5bps). Best per side below; "
                   "candidate JSONs under `candidates/EXT_*`. **Still research-only — DO NOT PROMOTE WITHOUT APPROVAL.**")
        for f, b in cands:
            out += [f"### {b['side']} {f} — {b['cfg']['bracket']}", "```json", json.dumps(b["cfg"], indent=2), "```",
                    f"- TRAIN @5bps: {fmt(b['train'])}", f"- TEST @5bps: {fmt(b['test'])}",
                    f"- TEST @2bps(limit): {fmt(b['test_2bps'])} | @15bps(stress): {fmt(b['test_15bps'])}"]
    else:
        best = max(results.items(), key=lambda kv: kv[1]["test"]["pf"])
        out.append(f"**No configuration cleared the gate at 5 bps/leg.** Best near-miss: {best[0]} ({best[1]['side']}) "
                   f"TEST PF {best[1]['test']['pf']} @5bps / {best[1]['test_2bps']['pf']} @2bps. "
                   "Wider targets do not rescue it (far targets are rarely reached intraday; tight stops are hit first), "
                   "and the SHORT side behaves like the LONG side after costs. **REJECT.**")
    out += ["", "## DO NOT PROMOTE TO FINAL CONFIG WITHOUT USER APPROVAL — final_setup_conf.py untouched."]
    (L.OUTDIR / "EXTENSION_RESULTS.md").write_text("\n".join(out), encoding="utf-8")
    print(f"[ext-search] done in {time.time()-t0:.0f}s. wrote EXTENSION_RESULTS.md + ext_search_summary.json")


if __name__ == "__main__":
    main()
