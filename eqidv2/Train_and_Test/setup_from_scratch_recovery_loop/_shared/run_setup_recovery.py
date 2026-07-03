r"""run_setup_recovery.py — orchestrates the from-scratch recovery for ONE setup.

RESEARCH-ONLY. Driven by each setup's scripts/run_recovery.py thin wrapper:
  py -3.12 Train_and_Test\setup_from_scratch_recovery_loop\<SETUP>\scripts\run_recovery.py

Stages (see recovery_engine.py for mechanics):
  W  winner/loser study on the FIT window (features/time/symbols; thresholds
     learned here are validated on untouched VAL).
  B  baseline (conf/default config, market entry, plain SL/TGT) on FIT/VAL/TRAIN/TEST.
  F1 exit engineering on the round-2 anchor filters (break-even / trailing /
     time-stop grid via resolver2 — never searched in rounds 1-3).
  F2 retest/limit entries on the anchor filters (pullback alpha x window K).
  F3 FIT-mined structural filters (top separation features from stage W at the
     FIT-winners' median threshold; singles + pairs).
  F4 time-window / top_n variants of the anchor.
  F5 fade (side-flipped detection) — only if the shared execution diagnostics
     showed a fade TRAIN PF >= 1.05 on any exit.
Each family: FIT/VAL band-objective scoring -> full-TRAIN confirm (band, n>=25,
domination caps, tpd<=6) -> robustness-lite -> ONE TEST evaluation. Everything
logged to iteration_log.csv; reports written per the campaign mandate.
"""
from __future__ import annotations

import itertools
import json
import sys
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))
import recovery_engine as re2  # noqa: E402

RECOVERY = _HERE.parent
TODAY = date.today().isoformat()
WARN = "> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**"

FEATS = ["rsi", "rsi_slope3", "adx5", "adx5_slope3", "mfi", "cci", "stoch_k", "stoch_kd",
         "macd_atr", "macd_sig_atr", "macd_hist_atr", "bb_pos", "bb_width_pct",
         "ema20_dist_atr", "ema50_dist_atr", "ema200_dist_atr", "ema_stack_atr",
         "ema20_slope3_atr", "sma20_dist_atr", "roc5_pct", "willr14", "obv_slope10_norm",
         "vol_z20", "pressure5", "candle_range_atr", "rechigh_dist_atr", "reclow_dist_atr",
         "day_ret_pct", "gap_pct", "dist_day_high_atr", "dist_day_low_atr",
         "or15_break_atr", "or15_lose_atr", "pdh_dist_atr", "pdl_dist_atr",
         "prev3_up", "prev_body_pct", "vol_ratio", "atr_pct", "body_pct", "close_loc",
         "vwap_dist_atr", "quality_score", "rs_pct", "signal_range_pct",
         "upper_wick_pct", "lower_wick_pct", "wick_skew_pct"]

META = {
    "B_AVWAP_RECLAIM_REVERSAL": {
        "side": "LONG",
        "baseline": {"sl": 0.70, "tgt": 1.50, "entry": "market", "max_positions": 20,
                     "mask_terms": [("vwap_dist_atr", "<=", 1.0)], "premom_terms": []},
        "anchor": {"sl": 1.2, "tgt": 2.5, "entry": "market", "max_positions": 20, "top_n": 2,
                   "mask_terms": [("macd_atr", ">=", 0.314566), ("regime", "!=", "BULL"),
                                  ("signal_range_pct", ">=", 1.149468)],
                   "premom_terms": []},
        "intent": "a below-VWAP stock reclaims session VWAP on a strong up-bar in a non-bear "
                  "regime — mean-reversion-to-trend transition from weakness",
    },
    "B_HUGE_C1_CLOSE_RECLAIM_BREAK": {
        "side": "LONG",
        "baseline": {"sl": 1.00, "tgt": 1.50, "entry": "market", "max_positions": 20,
                     "mask_terms": [("regime", "!=", "BULL")], "premom_terms": []},
        "anchor": {"sl": 1.0, "tgt": 2.5, "entry": "market", "max_positions": 10,
                   "min_slot": "12:00", "max_slot": "14:30", "top_n": 1,
                   "mask_terms": [("regime", "!=", "BULL")],
                   "premom_terms": [("pre5_mom_r", ">=", 0.546221)]},
        "intent": "momentum continuation — break of a prior HUGE GREEN bar's high in a "
                  "non-bear regime",
    },
    "B_HUGE_RED_FAILED_BOUNCE": {
        "side": "SHORT",
        "baseline": {"sl": 0.90, "tgt": 1.25, "entry": "market", "max_positions": 20,
                     "mask_terms": [],
                     "premom_terms": [("pre3_close_pos", "<=", 0.581797),
                                      ("sig5_rsi_dir", "<=", 64.104659),
                                      ("pre5_mom_r", "<=", 0.284145)]},
        "anchor": {"sl": 1.0, "tgt": 1.5, "entry": "market", "max_positions": 20,
                   "min_slot": "09:45", "top_n": 3,
                   "mask_terms": [("gap_pct", "<=", -0.412302), ("regime", "==", "BEAR")],
                   "premom_terms": [("sig5_adx_calc", "<=", 25.661066)]},
        "intent": "after a huge RED bar the bounce fails -> downside continuation",
    },
    "B_HUGE_FAILED_BOUNCE": {
        "side": "SHORT",
        "baseline": {"sl": 0.70, "tgt": 1.25, "entry": "market", "max_positions": 20,
                     "mask_terms": [], "premom_terms": []},
        "anchor": {"sl": 1.2, "tgt": 1.5, "entry": "market", "max_positions": 10,
                   "min_slot": "12:00", "max_slot": "14:00", "top_n": 1,
                   "mask_terms": [("regime", "!=", "BULL")],
                   "premom_terms": [("pre3_close_pos", "<=", 0.564802),
                                    ("pre3_range_r", ">=", 0.279969)]},
        "intent": "generic huge-bar failed bounce (either colour) -> downside continuation",
    },
    "B_HUGE_GREEN_PULLBACK_HOLD_THEN_BREAK": {
        "side": "LONG",
        "baseline": {"sl": 0.70, "tgt": 1.25, "entry": "market", "max_positions": 20,
                     "mask_terms": [], "premom_terms": []},
        "anchor": {"sl": 1.5, "tgt": 2.0, "entry": "market", "max_positions": 10,
                   "max_slot": "11:30", "top_n": 1,
                   "mask_terms": [],
                   "premom_terms": [("pre3_close_pos", ">=", 0.541682),
                                    ("sig5_vol_ratio20", ">=", 2.575953)]},
        "intent": "huge green bar, pullback holds the body, close-near-high break -> "
                  "continuation of institutional impulse",
    },
}


def _spec(base: dict, **over) -> dict:
    s = {"entry": "market", "mask_terms": [], "premom_terms": [], "min_slot": None,
         "max_slot": None, "top_n": None, "be": None, "trail": None, "tstop": None,
         "max_positions": 10}
    s.update(base)
    s.update(over)
    return s


def build_variants(setup: str, meta: dict, work: Path) -> dict:
    side = meta["side"]
    anchor = meta["anchor"]
    fams: dict[str, list] = {}

    # F1 — exit engineering on anchor filters
    f1 = []
    for be, trail, tstop in itertools.product([None, 0.4, 0.6], [None, 0.8, 1.2],
                                              [None, 60, 120]):
        f1.append(_spec(anchor, be=be, trail=trail, tstop=tstop))
        f1.append(_spec(anchor, be=be, trail=trail, tstop=tstop,
                        tgt=anchor["tgt"] + 0.5))
    fams["F1_exit_engineering"] = f1

    # F2 — retest/limit entries on anchor filters
    f2 = [_spec(anchor)]
    for a, k in itertools.product([0.3, 0.5, 0.8], [15, 30]):
        f2.append(_spec(anchor, entry=("retest", a, k)))
        f2.append(_spec(anchor, entry=("retest", a, k), sl=max(0.7, anchor["sl"] - 0.3)))
    fams["F2_retest_entry"] = f2

    # F3 — FIT-mined structural filters (from winner/loser book)
    f3 = []
    wl = work / "winner_loser_book.csv"
    if wl.exists():
        j = pd.read_csv(wl)
        j["win"] = j["net_pnl_rs"] > 0
        gaps = []
        for f in FEATS:
            if f not in j.columns:
                continue
            x = pd.to_numeric(j[f], errors="coerce")
            if x.notna().mean() < 0.5 or x.nunique() < 5:
                continue
            w, l = x[j["win"]], x[~j["win"]]
            sd = x.std()
            if not (np.isfinite(sd) and sd > 0):
                continue
            gaps.append((abs(w.mean() - l.mean()) / sd, f,
                         float(w.median()), bool(w.mean() >= l.mean())))
        gaps.sort(reverse=True)
        terms = []
        for _g, f, med, up in gaps[:4]:
            terms.append((f, ">=" if up else "<=", round(med, 6)))
        for t in terms:
            f3.append(_spec(anchor, mask_terms=list(anchor["mask_terms"]) + [t]))
            f3.append(_spec({"sl": anchor["sl"], "tgt": anchor["tgt"], "top_n": 1,
                             "max_positions": 10}, mask_terms=[t]))
        for t1, t2 in itertools.combinations(terms, 2):
            f3.append(_spec({"sl": anchor["sl"], "tgt": anchor["tgt"], "top_n": 1,
                             "max_positions": 10}, mask_terms=[t1, t2]))
    fams["F3_fit_mined_filters"] = f3

    # F4 — time windows / top_n on anchor
    f4 = []
    for mn, mx in ((None, "11:30"), ("10:00", "14:00"), ("12:00", None), (None, None)):
        for tn in (1, 2):
            f4.append(_spec(anchor, min_slot=mn, max_slot=mx, top_n=tn))
    fams["F4_time_topn"] = f4

    # F5 — fade (gated on shared diagnostics)
    diag = _HERE / "diagnostics.json"
    if diag.exists():
        d = json.loads(diag.read_text(encoding="utf-8"))
        fades = (d.get(setup) or {}).get("fade") or []
        if any(f["train_pf"] >= 1.05 and f["train_n"] >= 40 for f in fades):
            fams["F5_fade"] = [{"__fade__": True, **_spec({"sl": sl, "tgt": tg, "top_n": 1,
                                                           "max_positions": 10})}
                               for sl, tg in ((0.9, 1.25), (1.2, 1.5), (0.7, 1.0))]
    return fams


def main(setup: str) -> int:
    meta = META[setup]
    side = meta["side"]
    work = RECOVERY / setup
    (work / "candidates").mkdir(exist_ok=True)
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass

    # ---- Stage W: winner/loser study (FIT only) ----
    print(f"[recovery] {setup}: stage W (winner/loser study, FIT window)")
    re2.winner_loser_study(setup, side, (meta["baseline"]["sl"], meta["baseline"]["tgt"]),
                           work, FEATS)

    # ---- Stage B: baseline ----
    wins, meta_w = re2.load_windows(setup)
    base = _spec(meta["baseline"])
    mB = {}
    for w in ("FIT", "VAL", "TRAIN", "TEST"):
        mB[w], _ = re2.eval_variant_cfg(wins, base, w)
        print(f"[recovery] baseline {w}: n={mB[w]['n']} PF={mB[w]['net_pf']} net={mB[w]['net_pnl']}")

    # ---- Stages F1..F5 ----
    variants = build_variants(setup, meta, work)
    # NOTE: fade variants flip the side INSIDE load; recovery_engine works on the
    # pool side column, so fades are executed via a side-flipped copy here.
    fade_specs = variants.pop("F5_fade", None)
    summary = re2.run_recovery(setup, side, variants, work)

    if fade_specs:
        print(f"[recovery] F5 fade: flipping side")
        # flip side in the loaded pools by monkeypatching load_windows output
        orig_load = re2.load_windows

        def _flipped(s):
            w, m = orig_load(s)
            flip = "SHORT" if side == "LONG" else "LONG"
            out = {}
            for k, df in w.items():
                d2 = df.copy()
                d2["side"] = flip
                # re-attach entries for the flipped side (slippage direction flips)
                d2 = d2.drop(columns=["tt_entry_ok", "tt_entry_iso", "tt_fill", "tt_qty",
                                      "notional"], errors="ignore")
                out[k] = re2.tt.attach_entries(d2)
            return out, m
        re2.load_windows = _flipped
        try:
            fade_sum = re2.run_recovery(setup, "FADE_" + ("SHORT" if side == "LONG" else "LONG"),
                                        {"F5_fade": [
                                            {k: v for k, v in s.items() if k != "__fade__"}
                                            for s in fade_specs]}, work)
            summary["results"] += fade_sum["results"]
            summary["n_iterations"] += fade_sum["n_iterations"]
        finally:
            re2.load_windows = orig_load

    summary["baseline"] = mB
    summary["baseline_spec"] = re2.tt._json_sanitize(base)
    (work / "recovery_summary.json").write_text(
        json.dumps(re2.tt._json_sanitize(summary), indent=2, default=str), encoding="utf-8")

    write_reports(setup, meta, work, summary, mB)
    return 0


def _m(mm):
    if not mm:
        return "(none)"
    return (f"n={mm['n']} PF={mm['net_pf']} net=Rs{mm['net_pnl']:,.0f} win%={mm['win_rate']} "
            f"SL/TGT/BE/TRAIL/TIME/EOD={mm['sl_cnt']}/{mm['tgt_cnt']}/{mm['be_cnt']}/"
            f"{mm['trail_cnt']}/{mm['time_cnt']}/{mm['eod_cnt']} tpd={mm['trades_per_day']} "
            f"tradeDom={mm['trade_dom_gross']} dayDom={mm['day_dom']} symDom={mm['sym_dom']} "
            f"dbp={mm['day_block_p']}")


def write_reports(setup, meta, work, summary, mB):
    side = meta["side"]
    hdr = (f"_Generated {TODAY}. From-scratch recovery loop. Research-only; NO live trades; "
           f"NO final_setup_conf.py edits._")
    man = json.loads((work / "pools" / "pool_manifest.json").read_text(encoding="utf-8"))

    pr = [f"# {setup} ({side}) — POOL_RECREATION_REPORT (recovery loop)", "", hdr, "",
          f"- Pool carried over from the verified 2026-07-02/03 recreation (same mandate "
          f"windows), then ENRICHED with ~38 point-in-time indicator/price-action features. "
          f"Lineage: `Train_and_Test/setup_pf_1_4_full_loop/{setup}/pools/` -> "
          f"`{work / 'pools'}`.",
          f"- Basis: {man['basis']}",
          f"- requested TRAIN {man['requested']['TRAIN']} -> actual {man['actual']['TRAIN']} "
          f"({man['actual']['n_train_sessions']} sessions)",
          f"- requested TEST {man['requested']['TEST']} -> actual {man['actual']['TEST']} "
          f"({man['actual']['n_test_sessions']} sessions)",
          f"- excluded sessions: {man.get('excluded_sessions')} ({man.get('excluded_reason')})",
          f"- rows {man['rows_total']}, symbols {man['n_symbols']}",
          f"- 5-min: stocks_indicators_5min_eq_live2 (signals + enrichment); 1-min: "
          f"stocks_indicators_1min_eq via v11 loader (entries + exits to 15:20 IST).",
          f"- windows in this run: " + json.dumps(summary["windows"])]
    (work / "POOL_RECREATION_REPORT.md").write_text("\n".join(pr), encoding="utf-8")

    bl = [f"# {setup} ({side}) — BASELINE_RESULT (recovery loop)", "", hdr, "",
          f"- baseline spec: `{json.dumps(summary['baseline_spec'])}`", ""]
    for w in ("FIT", "VAL", "TRAIN", "TEST"):
        bl.append(f"- **{w}**: {_m(mB[w])}")
    (work / "BASELINE_RESULT.md").write_text("\n".join(bl), encoding="utf-8")

    # FROM_SCRATCH_LOGIC_REVIEW — the 12 mandated questions, answered with data
    diag = {}
    dj = _HERE / "diagnostics.json"
    if dj.exists():
        diag = (json.loads(dj.read_text(encoding="utf-8")) or {}).get(setup, {})
    ca = diag.get("cost_anatomy", {})
    mm = diag.get("mfe_mae", {})
    rt = diag.get("retest", {})
    fd = diag.get("fade", [])
    fr = [f"# {setup} ({side}) — FROM_SCRATCH_LOGIC_REVIEW", "", hdr, "",
          f"**1. What is this setup trying to capture?** {meta['intent']}.", "",
          "**2. Why should it work theoretically?** A huge/structural 5-min event implies "
          "institutional participation; the follow-through (or its failure) should have "
          "short-horizon drift beyond noise.", "",
          f"**3. Why did earlier optimization fail?** Rounds 1-3 (~1,200+ configs) proved the "
          f"detection is a high-frequency net loser at statutory+15bps and that mask-space "
          f"pockets don't carry OOS. Cost anatomy on the broad TRAIN book: "
          f"gross(0bps) PF {ca.get('gross_0bps', {}).get('pf', '?')} -> net@5bps "
          f"{ca.get('net_5bps', {}).get('pf', '?')} -> net@15bps "
          f"{ca.get('net_15bps', {}).get('pf', '?')} — "
          + ("there is NO gross edge to recover (selection was never the problem: the raw signal "
             "is directionless)." if (ca.get('gross_0bps', {}).get('pf', 0) or 0) < 1.05 else
             "a thin gross edge exists but costs consume it; execution quality matters most."), "",
          "**4. Are the entry rules logically weak?** The signal fires at the close of an "
          "extended bar and buys/sells the NEXT 1-min open — the worst price of the sequence. "
          f"Retest-depth data: within 30 min a 0.3-ATR pullback fills "
          f"{rt.get('30', rt.get(30, {})).get('fill@0.3atr_%', '?')}% of the time "
          f"(0.6 ATR: {rt.get('30', rt.get(30, {})).get('fill@0.6atr_%', '?')}%), so limit "
          "entries are mechanically feasible — F2 tests whether they help or adversely select.", "",
          f"**5. Are filters blocking winners / allowing losers?** See WINNER_LOSER_STUDY.md "
          f"(FIT-only): the top separation features feed F3 directly.", "",
          f"**6. Are SL/target mismatched with actual 1-min movement?** MFE/MAE medians at 60 min: "
          f"MFE {mm.get('60', mm.get(60, {})).get('mfe_med', '?')}% vs MAE "
          f"{mm.get('60', mm.get(60, {})).get('mae_med', '?')}%; "
          f"only {mm.get('60', mm.get(60, {})).get('pct_mfe_ge_0.5', '?')}% of trades ever see "
          f"+0.5% in the first hour — wide targets are structurally optimistic for most rows.", "",
          "**7. Are exits too early/late/tight/wide?** F1 answers empirically (BE/trail/time "
          "grid). Baseline books are SL+EOD heavy with avgW~avgL — the classic no-edge shape.", "",
          "**8. Bad time windows?** Hour table in WINNER_LOSER_STUDY.md; F4 tests the coarse "
          "windows.", "",
          "**9. Symbols/days/regimes destroying the edge?** Domination metrics in every "
          "confirmation (caps trade 0.35 / day 0.40 / sym 0.40); worst-day/symbol tables in "
          "WINNER_LOSER_STUDY.md.", "",
          "**10. Pool correctly recreated?** Yes — verified recreation for the mandated windows "
          "(POOL_RECREATION_REPORT.md lineage); 2026-07-02 excluded (1-min EOD sync incomplete).", "",
          "**11. Lookahead/leakage/unrealistic exits?** Entries next-1-min-open +15bps adverse; "
          "exits first-touch pessimistic (same-bar SL before TGT; BE/trail effective next bar; "
          "resolver validated 300/300 vs production); thresholds from FIT/TRAIN only; TEST "
          "evaluated once per family.", "",
          f"**12. Should the setup be redesigned within the same idea?** That is this loop: "
          f"F1 exit engineering, F2 retest entries, F3 FIT-mined confirmations, F4 windows, "
          f"F5 fade (diagnostics: best fade TRAIN PF "
          f"{max((f['train_pf'] for f in fd), default='n/a')}).", ""]
    (work / "FROM_SCRATCH_LOGIC_REVIEW.md").write_text("\n".join(fr), encoding="utf-8")

    fams = {r["family"]: r for r in summary["results"]}
    ideas = [f"# {setup} ({side}) — REDESIGNED_SETUP_IDEAS", "", hdr, "",
             f"Setup intent: {meta['intent']}.", "",
             "| family | idea | why it makes sense | outcome |", "|---|---|---|---|"]
    idea_rows = [
        ("F1_exit_engineering", "break-even / trailing / time-stop exits on the round-2 anchor",
         "family losses are SL+EOD heavy; reshape the loss tail without changing selection"),
        ("F2_retest_entry", "resting limit at a pullback of alpha*ATR (cancel after K min)",
         "huge-bar signals are extended at the close; chasing the next open pays the worst price"),
        ("F3_fit_mined_filters", "filters mined from FIT winners-vs-losers medians",
         "let the data name the confirmation instead of guessing; validated on untouched VAL"),
        ("F4_time_topn", "open-vs-midday windows + stricter per-slot ranking",
         "hour-of-day PnL is uneven; duplicates within a slot dilute quality"),
        ("F5_fade", "flip the side of the detection",
         "if continuation systematically fails, the failure itself may be the trade"),
    ]
    for fam, idea, why in idea_rows:
        r = fams.get(fam)
        if r is None:
            out = "not run (gated off by diagnostics)"
        elif r.get("passed"):
            out = "**PASS**"
        else:
            out = "; ".join(r.get("hard", [])) or "reject"
        ideas.append(f"| {fam} | {idea} | {why} | {out} |")
    (work / "REDESIGNED_SETUP_IDEAS.md").write_text("\n".join(ideas), encoding="utf-8")

    il = pd.read_csv(work / "iteration_log.csv")
    md = [f"# {setup} ({side}) — ITERATION_LOG (recovery loop)", "", hdr, "",
          f"{summary['n_iterations']} iterations; {summary['n_test_evals']} TEST evaluations "
          f"(budget-capped). Full row-level log: `iteration_log.csv`.", "",
          "## Stage results (TRAIN confirms + TEST-once rows)", "",
          "| iter | family | stage | FIT n/PF | VAL n/PF | TRAIN n/PF | TEST n/PF | keep | why |",
          "|---|---|---|---|---|---|---|---|---|"]
    key = il[il["stage"] != "fitval"]
    for _, r in key.iterrows():
        md.append(f"| {r['iter']} | {r['family']} | {r['stage']} | "
                  f"{r.get('fit_n', '-')}/{r.get('fit_pf', '-')} | {r.get('val_n', '-')}/{r.get('val_pf', '-')} | "
                  f"{r.get('train_n', '-')}/{r.get('train_pf', '-')} | {r.get('test_n', '-')}/{r.get('test_pf', '-')} | "
                  f"{r['keep']} | {str(r['why'])[:80]} |")
    md += ["", "## Top 20 FIT/VAL configs overall", "",
           "| family | FIT n/PF | VAL n/PF | score | spec |", "|---|---|---|---|---|"]
    top = il[il["stage"] == "fitval"].sort_values("score", ascending=False).head(20)
    for _, r in top.iterrows():
        md.append(f"| {r['family']} | {r['fit_n']}/{r['fit_pf']} | {r['val_n']}/{r['val_pf']} | "
                  f"{r['score']} | `{str(r['spec'])[:130]}` |")
    (work / "ITERATION_LOG.md").write_text("\n".join(md), encoding="utf-8")

    ps = [f"# {setup} ({side}) — PARAMETER_SWEEP_SUMMARY (recovery loop)", "", hdr, "",
          "Per-family FIT/VAL outcomes (band objective; higher = closer to a stable "
          "PF 1.30-1.80). Rejected ranges are visible as low scores in iteration_log.csv.", ""]
    fit = il[il["stage"] == "fitval"]
    for fam, g in fit.groupby("family"):
        ps += [f"## {fam}", "",
               f"- configs {len(g)} | best score {g['score'].max():.3f} | "
               f"median {g['score'].median():.3f}",
               f"- best: `{g.sort_values('score', ascending=False).iloc[0]['spec'][:160]}`", ""]
    (work / "PARAMETER_SWEEP_SUMMARY.md").write_text("\n".join(ps), encoding="utf-8")

    passing = [r for r in summary["results"] if r.get("passed")]
    if passing:
        cc = [f"# {setup} ({side}) — CANDIDATE_CONFIGS (recovery loop)", "", hdr, ""]
        for i, r in enumerate(passing, 1):
            cid = f"{setup}_recovery_candidate_{i:03d}"
            cc += [f"## {cid} ({r['family']})", "", "```json", json.dumps(r["spec"], indent=2),
                   "```", "", f"- TRAIN: {_m(r['train'])}", f"- TEST: {_m(r['test'])}",
                   f"- robustness: {r['robust']}", "- **APPROVAL REQUIRED** (not promoted)."]
            (work / "candidates" / f"{cid}.json").write_text(
                json.dumps({"setup": setup, "side": side, "verdict": "APPROVAL_REQUIRED",
                            "spec": r["spec"], "train": r["train"], "test": r["test"],
                            "robust": r["robust"]}, indent=2, default=str), encoding="utf-8")
        (work / "CANDIDATE_CONFIGS.md").write_text("\n".join(cc), encoding="utf-8")
    else:
        (work / "CANDIDATE_CONFIGS.md").write_text(
            f"# {setup} ({side}) — CANDIDATE_CONFIGS (recovery loop)\n\n{hdr}\n\n"
            f"**No redesigned candidate cleared the gate** (TRAIN PF [1.30,1.80] & n>=25 & "
            f"domination caps; TEST PF>1.40 & positive & clean & day-block p<=0.10; "
            f"robustness). Per-family kill reasons are in REDESIGNED_SETUP_IDEAS.md and "
            f"ITERATION_LOG.md.\n", encoding="utf-8")

    rec = "YES — APPROVAL REQUIRED" if passing else "NO"
    ar = [f"# {setup} ({side}) — APPROVAL_REQUIRED_FINAL_RECOMMENDATION (recovery loop)", "",
          hdr, "", f"## Approval recommendation: **{rec}**", ""]
    if passing:
        ar += ["Best candidate:", "", "```json", json.dumps(passing[0]["spec"], indent=2), "```",
               "", f"- TRAIN: {_m(passing[0]['train'])}", f"- TEST: {_m(passing[0]['test'])}", "",
               "- File needing approval before edit: `final_setup_conf.py` (+ Train_and_Test "
               "mirror). NOTE: retest entries / BE/trail/time exits additionally need engine "
               "support before live use — flag at approval.", ""]
    else:
        ar += ["- No promotion proposed. The closest configs and why they failed are listed in "
               "ITERATION_LOG.md / REDESIGNED_SETUP_IDEAS.md.", ""]
    ar += [WARN, "", "## Rerun", "```",
           f"py -3.12 Train_and_Test\\setup_from_scratch_recovery_loop\\{setup}\\scripts\\run_recovery.py",
           "```"]
    (work / "APPROVAL_REQUIRED_FINAL_RECOMMENDATION.md").write_text("\n".join(ar), encoding="utf-8")
    print(f"[recovery] reports written under {work}")


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--setup", required=True, choices=sorted(META))
    a = ap.parse_args()
    raise SystemExit(main(a.setup))
