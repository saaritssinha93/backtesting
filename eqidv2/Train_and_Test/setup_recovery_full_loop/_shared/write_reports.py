r"""write_reports.py — generate the campaign markdown deliverables from run artifacts.

Usage: py -3.12 write_reports.py --setup C_OR_BREAKDOWN
Reads:  baseline_result.json, iteration_log.csv, run_summary.json, candidates/*.json
Writes: BASELINE_RESULT.md, REDESIGNED_SETUP_IDEAS.md, PARAMETER_SWEEP_SUMMARY.md,
        ITERATION_LOG.md, FAILURE_ANALYSIS.md, CANDIDATE_CONFIGS.md,
        APPROVAL_REQUIRED_FINAL_RECOMMENDATION.md
"""
from __future__ import annotations

import argparse
import glob
import json
import sys
from datetime import date
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parent))
import recovery_lib as rl  # noqa: E402

TODAY = date.today().isoformat()


def mtable(m: dict) -> str:
    keys = [("n", "trades"), ("net_pf", "net PF"), ("net_pnl", "net PnL Rs"),
            ("win_rate", "win %"), ("max_dd", "max drawdown Rs"),
            ("sl_cnt", "SL exits"), ("tgt_cnt", "TGT exits"), ("eod_cnt", "EOD exits"),
            ("target_rate", "target-fill %"), ("trades_per_day", "trades/day"),
            ("n_days", "days"), ("n_syms", "symbols"),
            ("trade_dom_gross", "top-trade gross share"), ("day_dom", "top-day net share"),
            ("sym_dom", "top-symbol net share"), ("day_block_p", "day-block p"),
            ("top_day", "top day"), ("top_sym", "top symbol")]
    lines = ["| metric | value |", "|---|---|"]
    for k, lbl in keys:
        v = m.get(k)
        lines.append(f"| {lbl} | {v} |")
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--setup", required=True)
    args = ap.parse_args()
    setup = args.setup.strip().upper()
    work = rl.TT_DIR / "setup_recovery_full_loop" / setup

    base = json.loads((work / "baseline_result.json").read_text(encoding="utf-8"))
    il = pd.read_csv(work / "iteration_log.csv")
    summ = json.loads((work / "run_summary.json").read_text(encoding="utf-8"))
    cands = [json.loads(Path(f).read_text(encoding="utf-8"))
             for f in sorted(glob.glob(str(work / "candidates" / "*.json")))]

    wnd = base["windows"]

    # ---- BASELINE_RESULT.md ----
    lines = [f"# {setup} — BASELINE_RESULT", "", f"_Generated {TODAY}. Research-only._", "",
             "## Windows (exact sessions)", "",
             "| window | span | sessions |", "|---|---|---|"]
    for k in ("FIT", "VAL", "TRAIN", "TEST"):
        s = wnd[k]
        lines.append(f"| {k} | {s[0]}..{s[1]} | {s[2]} |")
    lines += ["", "## Baselines @15 bps/leg (research engine)", ""]
    for name, blk in base["results"].items():
        lines += [f"### {name}", ""]
        for w in ("FIT", "VAL", "TRAIN", "TEST"):
            m = blk["metrics"][w]
            lines.append(f"- **{w}**: n={m['n']} PF={m['net_pf']} net=Rs{m['net_pnl']:,.0f} "
                         f"win%={m['win_rate']} tpd={m['trades_per_day']} "
                         f"SL/TGT/EOD={m['sl_cnt']}/{m['tgt_cnt']}/{m['eod_cnt']} "
                         f"domT/D/S={m['trade_dom_gross']}/{m['day_dom']}/{m['sym_dom']} "
                         f"dbp={m.get('day_block_p')}")
        lines.append("")
    (work / "BASELINE_RESULT.md").write_text("\n".join(lines), encoding="utf-8")

    # ---- REDESIGNED_SETUP_IDEAS.md ----
    s3 = il[il["stage"] == "S3_versions"]
    lines = [f"# {setup} — REDESIGNED_SETUP_IDEAS (Stage 3 versions)", "",
             f"_Generated {TODAY}._", "",
             "Each version is one logical redesign of the setup; scored on FIT/VAL before any sweeps.",
             "", "| version | config | FIT n/PF | VAL n/PF | band score | verdict |", "|---|---|---|---|---|---|"]
    for _, r in s3.iterrows():
        lines.append(f"| {r['group']} | `{r['cfg']}` | {r['fit_n']}/{r['fit_pf']} | "
                     f"{r['val_n']}/{r['val_pf']} | {r['score']} | {r['decision']} |")
    (work / "REDESIGNED_SETUP_IDEAS.md").write_text("\n".join(lines), encoding="utf-8")

    # ---- PARAMETER_SWEEP_SUMMARY.md ----
    s4 = il[il["stage"] == "S4_sweep"].copy()
    lines = [f"# {setup} — PARAMETER_SWEEP_SUMMARY (Stage 4)", "", f"_Generated {TODAY}._", "",
             f"One-knob-at-a-time sweeps from each version base: {int(len(s4))} iterations across "
             f"{s4['group'].nunique() if len(s4) else 0} versions. FIT quantile grids; VAL as check.", ""]
    if len(s4):
        s4["knob"] = s4["change"].str.split().str[0] + " " + s4["change"].str.split().str[1].str.replace(r"[<>=0-9.\-]+$", "", regex=True)
        top = s4.sort_values("score", ascending=False).head(30)
        lines += ["## Top 30 sweep iterations", "",
                  "| iter | version | change | FIT n/PF | VAL n/PF | score |", "|---|---|---|---|---|---|"]
        for _, r in top.iterrows():
            lines.append(f"| {r['iter']} | {r['group']} | {r['change']} | {r['fit_n']}/{r['fit_pf']} | "
                         f"{r['val_n']}/{r['val_pf']} | {r['score']} |")
        by_knob = (s4.groupby(s4["change"].str.extract(r"^(\w+ [\w.]+)", expand=False).fillna(s4["change"]))
                   .agg(best_score=("score", "max"), n=("score", "size")).sort_values("best_score", ascending=False))
        lines += ["", "## Best score by knob family (top 25)", "", by_knob.head(25).to_markdown(), ""]
    (work / "PARAMETER_SWEEP_SUMMARY.md").write_text("\n".join(lines), encoding="utf-8")

    # ---- ITERATION_LOG.md ----
    lines = [f"# {setup} — ITERATION_LOG", "", f"_Generated {TODAY}. Optimizer: {summ['optimizer']}. "
             f"Total logged iterations: {len(il)} (full detail in iteration_log.csv)._", "",
             "Protocol: each iteration changes ONE logical group (version base / exit / guard / one mask "
             "term / one premom term / combo trial), is scored on FIT and VAL, and only stable configs "
             "were confirmed on full TRAIN. TEST was scored ONCE per confirmed config "
             f"({summ['test_evals_used']} of max {15} TEST evaluations used). "
             "Band objective: reward(min(FIT_PF,VAL_PF)) tent-peaked at 1.80 − 0.8·|FIT_PF−VAL_PF|.", "",
             "## Stage row counts", "", il["stage"].value_counts().to_markdown(), "",
             "## TRAIN confirmations + TEST verdicts", "",
             "| iter | stage | change | cfg | TRAIN n/PF/net | TEST n/PF/net | decision |",
             "|---|---|---|---|---|---|---|"]
    conf = il[il["train_pf"].notna()]
    for _, r in conf.iterrows():
        tr = f"{int(r['train_n'])}/{r['train_pf']}/Rs{r['train_net']:,.0f}" if pd.notna(r["train_pf"]) else "-"
        te = f"{int(r['test_n'])}/{r['test_pf']}/Rs{r['test_net']:,.0f}" if pd.notna(r["test_pf"]) else "-"
        lines.append(f"| {r['iter']} | {r['stage']} | {r['change']} | `{r['cfg']}` | {tr} | {te} | {r['decision']} |")
    lines += ["", "## Top 20 FIT/VAL iterations overall", "",
              "| iter | stage | change | FIT n/PF | VAL n/PF | score |", "|---|---|---|---|---|---|"]
    for _, r in il.sort_values("score", ascending=False).head(20).iterrows():
        lines.append(f"| {r['iter']} | {r['stage']} | {r['change']} | {r['fit_n']}/{r['fit_pf']} | "
                     f"{r['val_n']}/{r['val_pf']} | {r['score']} |")
    (work / "ITERATION_LOG.md").write_text("\n".join(lines), encoding="utf-8")

    # ---- FAILURE_ANALYSIS.md ----
    lines = [f"# {setup} — FAILURE_ANALYSIS", "", f"_Generated {TODAY}._", ""]
    rej_t = conf[conf["decision"] == "reject_train"]
    rej_te = conf[conf["decision"] == "reject_test"]
    lines += [f"- Unique configs confirmed on TRAIN: {len(conf)}; in-band TRAIN passes: "
              f"{summ['n_confirmed_in_band']}; TEST rejections: {len(rej_te)}; final candidates: "
              f"{summ['n_candidates']}.", ""]
    if len(rej_te):
        lines += ["## Configs that passed TRAIN band but failed TEST", "",
                  "| cfg | TRAIN n/PF | TEST n/PF | note |", "|---|---|---|---|"]
        for _, r in rej_te.iterrows():
            lines.append(f"| `{r['cfg']}` | {int(r['train_n'])}/{r['train_pf']} | "
                         f"{int(r['test_n'])}/{r['test_pf']} | {r['note']} |")
        lines.append("")
    lines += ["## Reading", "",
              "- See WINNER_LOSER_STUDY.md for the Stage-2 loss taxonomy (time, volume, trend, "
              "volatility, regime, concentration buckets).",
              "- A TRAIN-band pass that collapses on TEST = regime-dependence or knife-edge "
              "thresholds; both are rejected rather than re-tuned on TEST (anti-overfit rule).", ""]
    (work / "FAILURE_ANALYSIS.md").write_text("\n".join(lines), encoding="utf-8")

    # ---- CANDIDATE_CONFIGS.md + APPROVAL_REQUIRED_FINAL_RECOMMENDATION.md ----
    if cands:
        lines = [f"# {setup} — CANDIDATE_CONFIGS", "", f"_Generated {TODAY}._", ""]
        for i, c in enumerate(cands, 1):
            lines += [f"## Candidate {i:03d}", "", "```json",
                      json.dumps(c["cfg"], indent=2, default=str), "```", "",
                      f"- TRAIN: n={c['train']['n']} PF={c['train']['net_pf']} net=Rs{c['train']['net_pnl']:,.0f} "
                      f"domT/D/S={c['train']['trade_dom_gross']}/{c['train']['day_dom']}/{c['train']['sym_dom']}",
                      f"- TEST : n={c['test']['n']} PF={c['test']['net_pf']} net=Rs{c['test']['net_pnl']:,.0f} "
                      f"domT/D/S={c['test']['trade_dom_gross']}/{c['test']['day_dom']}/{c['test']['sym_dom']} "
                      f"dbp={c['test']['day_block_p']}", ""]
        (work / "CANDIDATE_CONFIGS.md").write_text("\n".join(lines), encoding="utf-8")
    else:
        (work / "CANDIDATE_CONFIGS.md").write_text(
            f"# {setup} — CANDIDATE_CONFIGS\n\n_Generated {TODAY}._\n\n"
            f"**No candidate cleared the robust gate** (TRAIN PF in [{rl.PF_LO},{rl.PF_HI}], "
            f"TEST PF > {rl.TEST_PF_MIN}, positive net both windows, n_train>={rl.MIN_TRADES_TRAIN}, "
            f"n_test>={rl.MIN_TRADES_TEST}, dom caps {rl.DOM_TRADE}/{rl.DOM_DAY}/{rl.DOM_SYM}, "
            f"TEST day-block p<=0.10).\n", encoding="utf-8")

    rec = "YES — APPROVAL REQUIRED" if cands else "NO"
    lines = [f"# {setup} — APPROVAL_REQUIRED_FINAL_RECOMMENDATION", "", f"_Generated {TODAY}._", "",
             f"## Approval recommendation: **{rec}**", ""]
    if cands:
        best = max(cands, key=lambda c: c["test"]["net_pf"])
        lines += ["## Best candidate", "", "```json", json.dumps(best["cfg"], indent=2, default=str), "```", "",
                  "### TRAIN", "", mtable(best["train"]), "", "### TEST", "", mtable(best["test"]), ""]
    lines += ["", "## Hard rules honored", "",
              "- Search on FIT only; VAL as check; TRAIN confirm; TEST scored once per confirmed config "
              f"({summ['test_evals_used']}/{15} TEST evals).",
              "- No edits to final_setup_conf.py (root or mirror). Research-only.",
              "", "> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**"]
    (work / "APPROVAL_REQUIRED_FINAL_RECOMMENDATION.md").write_text("\n".join(lines), encoding="utf-8")

    print(f"[reports] wrote 7 markdown reports under {work}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
