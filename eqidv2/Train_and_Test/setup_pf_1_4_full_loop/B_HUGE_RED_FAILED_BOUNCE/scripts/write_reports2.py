r"""write_reports2.py — ROUND-2 report for B_HUGE_RED_FAILED_BOUNCE (enriched feature space).
Reads round2/run_summary.json; writes round2/ROUND2_RESULTS.md and, if any
candidate passed, candidates/B_HUGE_RED_FAILED_BOUNCE_candidate_r2_XXX.json + updates
CANDIDATE_CONFIGS.md / APPROVAL_REQUIRED_FINAL_RECOMMENDATION.md.
"""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd

_HERE = Path(__file__).resolve().parent
WORK = _HERE.parent
R2 = WORK / "round2"
SETUP = "B_HUGE_RED_FAILED_BOUNCE"
SIDE = "SHORT"
TODAY = date.today().isoformat()
HDR = f"_Generated {TODAY}. ROUND 2 (enriched indicator/price-action feature space). Research-only; NO live trades; NO final_setup_conf.py edits._"
WARNING = "> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**"


def _m(mm):
    if not mm:
        return "(not run)"
    return (f"n={mm['n']} PF={mm['net_pf']} net=Rs{mm['net_pnl']:,.0f} win%={mm['win_rate']} "
            f"avgW=Rs{mm['avg_win']:,.0f} avgL=Rs{mm['avg_loss']:,.0f} maxDD=Rs{mm['max_dd']:,.0f} "
            f"SL/TGT/EOD={mm['sl_cnt']}/{mm['tgt_cnt']}/{mm['eod_cnt']} tpd={mm['trades_per_day']} "
            f"tradeDom={mm['trade_dom_gross']} dayDom={mm['day_dom']} symDom={mm['sym_dom']} dbp={mm['day_block_p']}")


def main() -> int:
    summary = json.loads((R2 / "run_summary.json").read_text(encoding="utf-8"))
    trials = pd.read_csv(R2 / "trials.csv") if (R2 / "trials.csv").exists() else pd.DataFrame()
    sweeps = pd.read_csv(R2 / "sweeps.csv") if (R2 / "sweeps.csv").exists() else pd.DataFrame()
    passing = [r for r in summary["results"] if r.get("passed")]

    md = [f"# {SETUP} ({SIDE}) — ROUND2_RESULTS (enriched feature space)", "", HDR, "",
          f"- Optimizer: {summary['optimizer']} | trials {summary['n_trials']} "
          f"({summary['n_unique_configs']} unique) | sweeps {summary['n_sweeps']} | "
          f"TEST evals used {summary['n_test_evals']}",
          f"- Windows: TRAIN {summary['windows']['TRAIN']} ({summary['windows']['n_train_sessions']} sess) | "
          f"TEST {summary['windows']['TEST']} ({summary['windows']['n_test_sessions']} sess)",
          f"- Search space: 3 mask terms over base + enriched features (RSI/ADX/MACD/EMA/BB/Stoch/MFI/CCI/OBV/"
          f"pressure/vol-z/ROC/W%R + day/gap/OR/prev-day geometry + prev-candle structure), "
          f"2 pre-momentum terms, regime, slots, top_n, max_positions, daily_loss, exit grid.",
          f"- **Passing candidates: {len(passing)}**", ""]

    md += ["## Baseline (round-1 conf/default config on this pool)", ""]
    for lbl in ("FIT", "VAL", "TRAIN", "TEST"):
        md.append(f"- {lbl}: {_m(summary['baseline_metrics'][lbl])}")
    md += ["", "## Finalists / rescue results", ""]
    for r in summary["results"]:
        tag = r.get("tag", f"finalist #{r['id']}")
        md.append(f"### {tag} — {'**PASS**' if r.get('passed') else 'reject'}")
        md.append("")
        md.append("```json")
        md.append(json.dumps(r["cfg"], indent=2))
        md.append("```")
        if r.get("train"):
            md.append(f"- TRAIN: {_m(r['train'])}")
        if r.get("test"):
            md.append(f"- TEST:  {_m(r['test'])}")
        if r.get("robust"):
            md.append(f"- robustness: neighbor={r['robust']['neighbor_pass']} dropout={r['robust']['dropout_pass']}")
        md.append(f"- reasons: {'; '.join(r.get('hard_reasons', []) + r.get('warnings', [])) or 'all gates passed'}")
        md.append("")
    if not trials.empty:
        md += ["## Top 25 FIT/VAL trials", "",
               "| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |",
               "|---|----|----|------|--------|-------|----------|----------|-------|"]
        for i, (_, r) in enumerate(trials.head(25).iterrows(), 1):
            md.append(f"| {i} | {r['sl']} | {r['tgt']} | {r['mask']} | {r['premom']} | {r['guard']} | "
                      f"{r['fit_n']}/{r['fit_pf']} | {r['val_n']}/{r['val_pf']} | {r['score']} |")
    if not sweeps.empty:
        good = sweeps[sweeps["vs_baseline"] == "improve"].sort_values("score", ascending=False).head(20)
        md += ["", "## Best round-2 single-knob improvements", ""]
        for _, r in good.iterrows():
            md.append(f"- **{r['group']} / {r['knob']}** -> {r['new']} "
                      f"(FIT {r['fit_n']}/{r['fit_pf']}, VAL {r['val_n']}/{r['val_pf']}, score {r['score']})")
    md += ["", "## Live-parity caveat for enriched features", "",
           "- Enriched mask features are computed from the SAME live 5-min indicator feed, but the current "
           "conf gate only reads scanner-emitted candidate fields. Promoting an enriched-mask candidate "
           "requires a small gate extension (look up the indicator columns at apply time). Flag this at "
           "approval.", "", WARNING]
    (R2 / "ROUND2_RESULTS.md").write_text("\n".join(md), encoding="utf-8")

    if passing:
        (WORK / "candidates").mkdir(exist_ok=True)
        cc = [f"# {SETUP} ({SIDE}) — CANDIDATE_CONFIGS (ROUND 2 — passed all gates)", "", HDR, ""]
        for i, r in enumerate(passing, 1):
            cid = f"{SETUP}_candidate_r2_{i:03d}"
            cc += [f"## Candidate r2-{i:03d}", "", "```json", json.dumps(r["cfg"], indent=2), "```", "",
                   f"- TRAIN: {_m(r['train'])}", f"- TEST:  {_m(r['test'])}",
                   f"- robustness: neighbor={r['robust']['neighbor_pass']} dropout={r['robust']['dropout_pass']}",
                   f"- warnings: {'; '.join(r.get('warnings', [])) or 'none'}",
                   "- Recommendation: **APPROVAL REQUIRED** (do not auto-promote).", ""]
            (WORK / "candidates" / f"{cid}.json").write_text(
                json.dumps({"setup": SETUP, "side": SIDE, "verdict": "APPROVAL_REQUIRED", "round": 2,
                            "config": r["cfg"], "train": r["train"], "test": r["test"],
                            "robust": r["robust"], "warnings": r.get("warnings", [])},
                           indent=2, default=str), encoding="utf-8")
        (WORK / "CANDIDATE_CONFIGS.md").write_text("\n".join(cc), encoding="utf-8")
    print(f"[reports2] ROUND2_RESULTS.md written; passing={len(passing)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
