r"""render_recovery_reports.py — render recovery-loop artifacts into the required reports.

Reads: baseline_redesigned_result.json, hypotheses.json, sweeps.json, combos_optuna_rec.json,
combos_rescue_tpe.json (optional), confirmations.json, rescue.json (optional),
iteration_records.json, mfe_mae_study.json, pools/pool_redesigned/_manifest.json

Writes: BASELINE_RESULT.md, PARAMETER_SWEEP_SUMMARY.md, ITERATION_LOG.md,
CANDIDATE_CONFIGS.md, APPROVAL_REQUIRED_FINAL_RECOMMENDATION.md, candidates/*.json
"""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path

HERE = Path(__file__).resolve()
WORK = HERE.parents[1]
SETUP = "A_MOD_CLOSE_CONTINUATION_BREAK"
SIDE = "LONG"
TODAY = date.today().isoformat()
HEAD = f"_Generated {TODAY}. Research-only; NO live trades; NO final_setup_conf.py edits._"


def _load(name, default=None):
    p = WORK / name
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else default


def mline(m):
    if not m:
        return "(not run)"
    return (f"n={m['n']} PF={m['net_pf']} net=Rs{m['net_pnl']:,.0f} win%={m.get('win_rate')} "
            f"avgW=Rs{m.get('avg_win', 0):,.0f} avgL=Rs{m.get('avg_loss', 0):,.0f} "
            f"SL/TGT/EOD={m.get('sl_cnt')}/{m.get('tgt_cnt')}/{m.get('eod_cnt')} "
            f"tpd={m.get('trades_per_day')} domT/D/S={m.get('trade_dom_gross')}/{m.get('day_dom')}/{m.get('sym_dom')} "
            f"dbp={m.get('day_block_p')}")


def render_baseline():
    b = _load("baseline_redesigned_result.json")
    man = _load("pools/pool_redesigned/_manifest.json", {})
    if not b:
        return
    w = b["windows"]
    lines = [f"# {SETUP} ({SIDE}) — BASELINE_RESULT (recovery loop)", "", HEAD, "",
             "## Original rules (card, re-detected uncollapsed)", "",
             "- Entry trigger: 5-min bar, close>open, close_loc >= 0.75, close > prev bar high, "
             "range 0.60-2.20 x ATR (moderate impulse), above causal session VWAP, rs_pct > 0 "
             "vs NIFTYBEES, vol_ratio >= 1.4 (common floor 1.5), quality >= 6.8; liquidity floors "
             "px>=80 / bar>=Rs1M / day>=Rs20M after 10:00; scan 10:00-14:30.",
             "- Pre-momentum: none (original). Filters: none beyond detector. Guards: none.",
             "- SL/Target: 0.70% / 1.50% (production exit rule). Exit: first-touch on 1-min bars "
             "else EOD 15:20; entry next 1-min open + 15 bps/leg; statutory NSE costs.",
             f"- Pool: {man.get('rows'):,} rows / {man.get('n_tickers')} tickers / "
             f"{man.get('n_sessions')} sessions; regime mix {man.get('regime_mix')}.", "",
             "## Windows", "",
             f"- TRAIN {w['TRAIN'][0]}..{w['TRAIN'][1]} ({w['TRAIN'][2]} sessions); "
             f"FIT {w['FIT'][0]}..{w['FIT'][1]} ({w['FIT'][2]}); VAL {w['VAL'][0]}..{w['VAL'][1]} ({w['VAL'][2]})",
             f"- TEST {w['TEST'][0]}..{w['TEST'][1]} ({w['TEST'][2]} sessions; 07-02 excluded — "
             "truncated 1-min data)", "",
             "## Baseline metrics (@15 bps/leg, statutory costs)", ""]
    for wname in ("FIT", "VAL", "TRAIN", "TEST"):
        lines.append(f"- **{wname}**: {mline(b['results'][wname])}")
    lines += ["", "## TRAIN regime slices", ""]
    for rg, m in b.get("regime_slices_train", {}).items():
        if m.get("n"):
            lines.append(f"- {rg}: {mline(m)}")
    lines += ["", "## Diagnosis", "",
              "- The uncollapsed card is uniformly and heavily negative in EVERY regime "
              "(BULL 0.22 / BEAR 0.30 / NEUTRAL 0.28 / TREND 0.30) — the collapse-shadowing "
              "hypothesis is refuted: the trigger itself has no edge at production exits.",
              "- MFE/MAE (1-min paths): median MFE +0.37% vs median MAE -1.05%; close-to-EOD "
              "median -0.47%. All 49 SL x target brackets are physically infeasible — the "
              "perfect-exit hit-rate ceiling is ~half the win rate needed for PF 1.3 "
              "(see WINNER_LOSER_STUDY.md).",
              "- Recovery therefore depends entirely on filters finding a sub-pocket with a "
              "several-fold different forward distribution."]
    (WORK / "BASELINE_RESULT.md").write_text("\n".join(lines), encoding="utf-8")
    print("wrote BASELINE_RESULT.md")


def render_sweeps():
    hyp = _load("hypotheses.json", [])
    sw = _load("sweeps.json", [])
    lines = [f"# {SETUP} ({SIDE}) — PARAMETER_SWEEP_SUMMARY (recovery loop)", "", HEAD, "",
             "Band score = `reward(min(PF_fit,PF_val)) − 0.80·|PF_fit−PF_val|`, tent at 1.70. "
             "`keep` needs FIT & VAL PF >= 1.0 with >= 6 trades each.", "",
             "## Redesigned setup versions (hypothesis packs)", "",
             "| version | exit | FIT n/PF | VAL n/PF | score | decision |",
             "|---|---|---|---|---|---|"]
    for r in sorted(hyp, key=lambda x: -x["score"]):
        lines.append(f"| {r['name']} | SL{r['sl']}/T{r['tgt']} | {r['fit_n']}/{r['fit_pf']} "
                     f"| {r['val_n']}/{r['val_pf']} | {r['score']} | {r['decision']} |")
    keeps = [r for r in sw if r["decision"] == "keep"]
    lines += ["", f"## Single-term sweeps: {len(keeps)} keeps / {len(sw)} tested", "",
              "Top 20 by band score:", "", "| term | FIT n/PF | VAL n/PF | score | decision |",
              "|---|---|---|---|---|"]
    for r in sorted(sw, key=lambda x: -x["score"])[:20]:
        lines.append(f"| {r['label']} | {r['fit_n']}/{r['fit_pf']} | {r['val_n']}/{r['val_pf']} "
                     f"| {r['score']} | {r['decision']} |")
    best_per = {}
    for r in sw:
        f = r["feat"]
        if f not in best_per or r["score"] > best_per[f]["score"]:
            best_per[f] = r
    lines += ["", "## Best value per knob (all knobs tested relaxed/medium/strict)", "",
              "| knob | best value | FIT n/PF | VAL n/PF | score |", "|---|---|---|---|---|"]
    for f, r in sorted(best_per.items(), key=lambda kv: -kv[1]["score"]):
        lines.append(f"| {f} | {r['label']} | {r['fit_n']}/{r['fit_pf']} "
                     f"| {r['val_n']}/{r['val_pf']} | {r['score']} |")
    lines += ["", "## Notes", "",
              "- Thresholds are TRAIN deciles only; market_ret/notional/signal-minute masks "
              "excluded (documented overfit vectors); time-of-day via slot guards.",
              "- Exit grid anchored at SL0.70/T1.50 and SL1.00/T2.00 for sweeps; the full "
              "7x7 exit grid is explored inside the TPE search; MFE/MAE bracket feasibility "
              "in WINNER_LOSER_STUDY.md shows every bracket's win-rate ceiling."]
    (WORK / "PARAMETER_SWEEP_SUMMARY.md").write_text("\n".join(lines), encoding="utf-8")
    print("wrote PARAMETER_SWEEP_SUMMARY.md")


def render_iterlog():
    recs = _load("iteration_records.json", [])
    lines = [f"# {SETUP} ({SIDE}) — ITERATION_LOG (recovery loop)", "", HEAD, "",
             f"Total logged iterations: **{len(recs)}**. Full trials: `trials_optuna_rec.csv` "
             "(+ `trials_rescue_tpe.csv`).", "",
             "Command:", "```",
             "py -3.12 Train_and_Test\\setup_from_scratch_recovery_loop\\A_MOD_CLOSE_CONTINUATION_BREAK\\scripts\\campaign_recovery.py --trials 3000 --time_budget_min 45 --seed 17",
             "```", "",
             "| # | group | change | FIT n/PF | VAL n/PF | TRAIN | TEST | decision | failure / next |",
             "|---|---|---|---|---|---|---|---|---|"]
    for r in recs:
        fit = f"{r['fit']['n']}/{r['fit']['pf']}" if r.get("fit") else "-"
        val = f"{r['val']['n']}/{r['val']['pf']}" if r.get("val") else "-"
        tr = (f"n={r['train']['n']} PF={r['train']['net_pf']}" if r.get("train") else "-")
        te = (f"n={r['test']['n']} PF={r['test']['net_pf']}" if r.get("test") else "-")
        ch = str(r["change"]).replace("|", "\\|")
        fc = (str(r.get("failure_class") or "") + " / " + str(r.get("next_action") or "")).replace("|", "\\|")
        lines.append(f"| {r['iter']} | {r['group']} | {ch} | {fit} | {val} | {tr} | {te} "
                     f"| {r['decision']} | {fc} |")
    (WORK / "ITERATION_LOG.md").write_text("\n".join(lines), encoding="utf-8")
    print(f"wrote ITERATION_LOG.md ({len(recs)} iterations)")


def render_candidates():
    conf = _load("confirmations.json", []) or []
    cands = [c for c in conf if c.get("verdict") == "CANDIDATE"]
    (WORK / "candidates").mkdir(exist_ok=True)
    lines = [f"# {SETUP} ({SIDE}) — CANDIDATE_CONFIGS (recovery loop)", "", HEAD, ""]
    if not cands:
        lines += ["**No candidate cleared the full gate** (TRAIN PF in [1.30,1.80], TEST PF > 1.40, "
                  "positive PnL, meaningful trades, domination caps, FIT/VAL coherence, robustness).",
                  "", "See APPROVAL_REQUIRED_FINAL_RECOMMENDATION.md for evidence and near-misses."]
    for i, c in enumerate(cands, 1):
        cfg = c["cfg"]
        block = {"side": SIDE, "exit": {"sl_pct": cfg["sl"], "tgt_pct": cfg["tgt"]},
                 "mask_terms": [list(t) for t in cfg["mask_terms"]],
                 "pre_momentum_terms": [list(t) for t in cfg["premom_terms"]],
                 "entry_guards": cfg["guard"] or {},
                 "max_positions": cfg["max_positions"], "daily_loss_rs": cfg["daily_loss_rs"],
                 "detector": "redesigned uncollapsed card (scan_redesigned_pool.py) — needs "
                             "flag-gated detector extension for deployment"}
        f = WORK / "candidates" / f"{SETUP}_candidate_{i:03d}.json"
        f.write_text(json.dumps({"setup": SETUP, "side": SIDE, "verdict": "APPROVAL_REQUIRED",
                                 "config": block, "train_15bps": c["train"], "test_15bps": c["test"],
                                 "checks": c.get("checks"), "robustness": c.get("robustness")},
                                indent=2, default=str), encoding="utf-8")
        lines += [f"## Candidate {i:03d}", "", "```json", json.dumps(block, indent=2), "```", "",
                  f"- TRAIN @15bps: {mline(c['train'])}", f"- TEST @15bps: {mline(c['test'])}",
                  f"- checks: {c.get('checks')}", f"- robustness: {c.get('robustness')}",
                  f"- file: `candidates/{f.name}`",
                  "- Recommendation: **APPROVAL REQUIRED** (do not auto-promote).", ""]
    (WORK / "CANDIDATE_CONFIGS.md").write_text("\n".join(lines), encoding="utf-8")
    print(f"wrote CANDIDATE_CONFIGS.md ({len(cands)} candidates)")
    return cands


def render_final(cands):
    conf = _load("confirmations.json", []) or []
    lines = [f"# {SETUP} ({SIDE}) — APPROVAL_REQUIRED / FINAL RECOMMENDATION (recovery loop)",
             "", HEAD, ""]
    if cands:
        best = max(cands, key=lambda c: (c["test"]["net_pf"], c["train"]["net_pf"]))
        cfg = best["cfg"]
        block = {"side": SIDE, "exit": {"sl_pct": cfg["sl"], "tgt_pct": cfg["tgt"]},
                 "mask_terms": [list(t) for t in cfg["mask_terms"]],
                 "pre_momentum_terms": [list(t) for t in cfg["premom_terms"]],
                 "entry_guards": cfg["guard"] or {},
                 "max_positions": cfg["max_positions"], "daily_loss_rs": cfg["daily_loss_rs"]}
        lines += ["## Approval recommendation: **YES — APPROVAL REQUIRED**", "",
                  "```json", json.dumps(block, indent=2), "```", "",
                  f"- TRAIN @15bps: {mline(best['train'])}",
                  f"- TEST @15bps: {mline(best['test'])}", "",
                  "## Files needing approval before any edit", "",
                  "- `final_setup_conf.py` (config block) AND a flag-gated detector extension in "
                  "`avwap_5min_ID_v2_backtesting.py` (S9/DOC5D pattern) — the redesigned scan is "
                  "not produced by the production pipeline.",
                  "", "> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**", ""]
    else:
        near = sorted([c for c in conf if c.get("train")], key=lambda c: -c["train"]["net_pf"])[:4]
        lines += ["## Approval recommendation: **NO**", "",
                  "> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES** (nothing to promote)", "",
                  "## Why NO — the from-scratch evidence chain", "",
                  "1. Production pool = collapse residue (96.8% BEAR): two campaigns, 1,673 "
                  "iterations -> REJECT (documented in setup_pf_1_4_full_loop/).",
                  "2. Redesigned uncollapsed pool (42,757 signals, all regimes, morning window "
                  "restored): baseline TRAIN PF 0.223 / TEST 0.173; EVERY regime slice negative "
                  "(BULL 0.22, BEAR 0.30, NEUTRAL 0.28, TREND 0.30, all with n >= 448).",
                  "3. MFE/MAE on 4,000 1-min paths: median MFE +0.37% vs MAE -1.05%; close-to-EOD "
                  "median -0.47%. ALL 49 exit brackets physically infeasible — perfect-exit "
                  "hit-rate ceiling ~= half the win rate needed for PF 1.3.",
                  "4. Winner/loser separation: fresh-break, first-break, pullback-then-break, "
                  "regime, hour — none separates winners from losers (PF 0.19-0.41 everywhere).",
                  "5. Redesign packs, single-term sweeps, TPE combinations and the rescue loop "
                  "(this campaign's ITERATION_LOG) confirmed no stable in-band pocket exists.", "",
                  "## Closest confirmations (full TRAIN)", ""]
        for c in near:
            cfg = c["cfg"]
            m = ";".join(f"{a}{o}{b}" for a, o, b in cfg["mask_terms"]) or "-"
            lines += [f"- SL{cfg['sl']}/T{cfg['tgt']} [{m}] g={cfg['guard']}: {c['verdict']}",
                      f"  - TRAIN {mline(c.get('train'))}",
                      f"  - TEST  {mline(c.get('test'))}"]
        lines += ["", "## Conclusion on edge", "",
                  "The card's trigger — buying the close-near-high break of the prior 5-min bar "
                  "after strength — is a systematic LOCAL-EXTREME purchase. Its forward 1-min "
                  "distribution is negatively skewed in every regime, every hour, and every "
                  "structural variant; costs are ~27% of the median favorable excursion. "
                  "**The setup has no real edge at 5-min next-tick granularity in this universe.** "
                  "Recommend permanently retiring it (keep GATE_BLOCKED / never promote) and "
                  "spending future iteration budget on setups whose baseline is at least "
                  "cost-line-adjacent."]
    lines += ["", "## Rerun commands", "", "```",
              "cd <repo root>",
              "py -3.12 Train_and_Test\\setup_from_scratch_recovery_loop\\A_MOD_CLOSE_CONTINUATION_BREAK\\scripts\\scan_redesigned_pool.py",
              "py -3.12 Train_and_Test\\setup_pf_1_4_full_loop\\A_MOD_CLOSE_CONTINUATION_BREAK\\scripts\\enrich_pool_features.py --no-premom --pool Train_and_Test\\setup_from_scratch_recovery_loop\\A_MOD_CLOSE_CONTINUATION_BREAK\\pools\\pool_redesigned --out Train_and_Test\\setup_from_scratch_recovery_loop\\A_MOD_CLOSE_CONTINUATION_BREAK\\pools\\pool_enriched",
              "py -3.12 Train_and_Test\\setup_from_scratch_recovery_loop\\A_MOD_CLOSE_CONTINUATION_BREAK\\scripts\\eval_baseline_recovery.py",
              "py -3.12 Train_and_Test\\setup_from_scratch_recovery_loop\\A_MOD_CLOSE_CONTINUATION_BREAK\\scripts\\mfe_mae_study.py",
              "py -3.12 Train_and_Test\\setup_from_scratch_recovery_loop\\A_MOD_CLOSE_CONTINUATION_BREAK\\scripts\\winner_loser_study.py",
              "py -3.12 Train_and_Test\\setup_from_scratch_recovery_loop\\A_MOD_CLOSE_CONTINUATION_BREAK\\scripts\\campaign_recovery.py --trials 3000 --time_budget_min 45 --seed 17",
              "py -3.12 Train_and_Test\\setup_from_scratch_recovery_loop\\A_MOD_CLOSE_CONTINUATION_BREAK\\scripts\\render_recovery_reports.py",
              "```", "",
              "## Remaining risks / caveats", "",
              "- Redesigned scan is research-side; live deployment would need a flag-gated "
              "detector (S9/DOC5D pattern) and a fresh sign-off run.",
              "- 15 bps/leg slippage assumed; small-caps may be worse.",
              "- TEST excludes 2026-07-02 (truncated 1-min data); 2026-06-26 has no 5-min data."]
    (WORK / "APPROVAL_REQUIRED_FINAL_RECOMMENDATION.md").write_text("\n".join(lines), encoding="utf-8")
    print("wrote APPROVAL_REQUIRED_FINAL_RECOMMENDATION.md")


def main():
    render_baseline()
    render_sweeps()
    render_iterlog()
    cands = render_candidates()
    render_final(cands)


if __name__ == "__main__":
    main()
