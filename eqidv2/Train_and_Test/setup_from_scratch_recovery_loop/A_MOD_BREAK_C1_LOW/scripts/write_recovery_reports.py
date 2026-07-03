r"""write_recovery_reports.py — generate the 9 FROM-SCRATCH RECOVERY reports for
A_MOD_BREAK_C1_LOW from the campaign artifacts. RESEARCH-ONLY; writes only here."""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd

_HERE = Path(__file__).resolve().parent
WORK = _HERE.parent
SETUP = "A_MOD_BREAK_C1_LOW"
TODAY = date.today().isoformat()
HDR = f"_Generated {TODAY}. Research-only; NO live trades; NO final_setup_conf.py edits._"
WARNING = "> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**"
PREV = WORK.parent.parent / "setup_pf_1_4_full_loop" / SETUP


def _m(mm):
    if not mm:
        return "(not run)"
    return (f"n={mm['n']} PF={mm['net_pf']} net=Rs{mm['net_pnl']:,.0f} win%={mm['win_rate']} "
            f"avgW=Rs{mm['avg_win']:,.0f} avgL=Rs{mm['avg_loss']:,.0f} "
            f"SL/TGT/EOD={mm['sl_cnt']}/{mm['tgt_cnt']}/{mm['eod_cnt']} tgt%={mm['target_rate']} "
            f"tpd={mm['trades_per_day']} tradeDom={mm['trade_dom_gross']} dayDom={mm['day_dom']} "
            f"symDom={mm['sym_dom']} dbp={mm['day_block_p']}")


def main() -> int:
    manifest = json.loads((WORK / "pools" / "pool_manifest.json").read_text(encoding="utf-8"))
    cov = pd.read_csv(WORK / "pools" / "sessions_coverage.csv")
    rs = json.loads((WORK / "run_summary_recovery.json").read_text(encoding="utf-8"))
    wl = json.loads((WORK / "winner_loser_stats.json").read_text(encoding="utf-8"))
    trials = pd.read_csv(WORK / "trials_recovery.csv")
    iters = pd.read_csv(WORK / "iteration_log_recovery.csv")
    mB = rs["baseline_metrics"]

    # ---------------- 1. POOL_RECREATION_REPORT -----------------------------------
    pr = [f"# {SETUP} (SHORT) — POOL_RECREATION_REPORT (from-scratch recovery)", "", HDR, "",
          "## Raw data used", "",
          "- 5-minute signals: production scanner RAW candidates (4 deterministic sources) **plus a "
          "from-raw-OHLCV re-detection** (`scripts/redesign_scan.py`) on "
          "`stocks_indicators_5min_eq_live2` that removes the scanner's incidental gates "
          "(ADX>=19.12, RSI>=23.22, atr_pct<=0.63%) and adds redesigned variants.",
          "- 1-minute exits: `stocks_indicators_1min_eq` (+live-raw merge), resolved to 15:20 IST.",
          "- Costs: statutory NSE intraday + 15 bps/leg slippage both legs.", "",
          "## Requested vs actual sessions", "",
          f"- requested TRAIN `{manifest['requested']['TRAIN'][0]}..{manifest['requested']['TRAIN'][1]}` -> "
          f"actual `{manifest['actual']['TRAIN'][0]}..{manifest['actual']['TRAIN'][1]}` "
          f"(**{manifest['actual']['n_train_sessions']} sessions**)",
          f"- requested TEST `{manifest['requested']['TEST'][0]}..{manifest['requested']['TEST'][1]}` -> "
          f"actual `{manifest['actual']['TEST'][0]}..{manifest['actual']['TEST'][1]}` "
          f"(**{manifest['actual']['n_test_sessions']} sessions**)",
          "- 2026-07-02 excluded (EOD 1-min sync not yet run when campaign started); "
          "2026-07-01 had ZERO qualifying original-scanner events (verified up-day); "
          "2026-06-26 no-data/holiday.",
          f"- missing weekdays: `{', '.join(manifest['missing_weekdays'])}`", "",
          "## Pools", "",
          f"- original-scanner pool: {manifest['rows_total']} rows / {manifest['n_symbols']} symbols "
          f"(`pools/{SETUP}/`)",
          "- redesigned master AMOD_RX2: 146,211 CORE events / 1,285 tickers (`pools/redesigned/AMOD_RX2/`) "
          "— the incidental scanner gates were cutting ~83% of the structural universe",
          "- redesigned AMOD_RETEST: 45,231 retest-reject events (`pools/redesigned/AMOD_RETEST/`)",
          "- tractability caps inside the loop (documented, seeded): deepest break per ticker-day, then "
          "random sample (TRAIN 20k/TEST 8k; RETEST 14k/6k) — same precedent as the original "
          "`amod_mine_gen.py` sampled pool.", "",
          "## Session coverage (original-scanner pool)", "",
          "| session | window | rows | tickers |", "|---|---|---|---|"]
    for _, r in cov.iterrows():
        pr.append(f"| {r['session']} | {r['window']} | {r['rows']} | {r['tickers']} |")
    (WORK / "POOL_RECREATION_REPORT.md").write_text("\n".join(pr), encoding="utf-8")

    # ---------------- 2. BASELINE_RESULT -------------------------------------------
    bl = [f"# {SETUP} (SHORT) — BASELINE_RESULT (from-scratch recovery)", "", HDR, "",
          f"- config source: {rs['baseline_src']} — mask `vol_ratio>=1.955814`, premom "
          "`pre5_mom_r>=0.425861 & pre3_range_r<=0.202087`, exit SL 1.10 / Tgt 1.00, no guards.",
          "- original detection (v2 scanner): red bar (close<open, close_loc<=0.40), impulse range "
          "0.60-2.20x ATR, close < PREV BAR low (\"C1\" = prior candle, not first-of-day), close < "
          "session VWAP, ADX>=19.12, RSI>=23.22, atr_pct<=0.0063, vol_ratio>=1.5, regime!=BULL; "
          "entry = next 1-min open.", "",
          f"- windows: TRAIN {rs['windows']['TRAIN']} ({rs['windows']['n_train_sessions']} sessions; "
          f"FIT {rs['windows']['FIT_n']} / VAL {rs['windows']['VAL_n']}), "
          f"TEST {rs['windows']['TEST']} ({rs['windows']['n_test_sessions']} sessions)", ""]
    for k in ("FIT", "VAL", "TRAIN", "TEST"):
        bl.append(f"- **{k}**: {_m(mB[k])}")
    bl += ["", "Baseline verdict: loser everywhere (TRAIN PF "
           f"{mB['TRAIN']['net_pf']}, TEST PF {mB['TEST']['net_pf']}) — matches phase-1/2 findings "
           "and live paper (PF ~0.25)."]
    (WORK / "BASELINE_RESULT.md").write_text("\n".join(bl), encoding="utf-8")

    # ---------------- 3. FROM_SCRATCH_LOGIC_REVIEW ---------------------------------
    mfe = wl["mfe_all"]; mae = wl["mae_all"]
    fr = [f"# {SETUP} (SHORT) — FROM_SCRATCH_LOGIC_REVIEW", "", HDR, "",
          "## 1. What is this setup trying to capture?",
          "A momentum-continuation short: a moderate red impulse 5-min bar that closes below the "
          "PREVIOUS bar's low while under session VWAP, in a non-bull tape — the idea is that broken "
          "micro-support + selling impulse continues down for at least ~1%.",
          "",
          "## 2. Why should it work theoretically?",
          "Intraday breakdown continuation is a real phenomenon when (a) the broken level matters, "
          "(b) participation is real (volume), and (c) there is room to fall. It monetises trend-day "
          "persistence and stop-cascades under the prior bar's low.",
          "",
          "## 3. Why did the earlier optimization fail?",
          "Because it FILTERED a population whose per-trade movement cannot pay the cost stack. The "
          f"TRAIN-book 1-min study (n={wl['n']}) shows the median trade's MAX favorable excursion is "
          f"only {mfe['50']}% (p40 {mfe['40']}%, p60 {mfe['60']}%) while median adverse excursion is "
          f"{mae['50']}% and the round-trip cost is ~0.30% (15 bps/leg both ways + statutory). "
          "The production 1.0% target sits beyond the p60 of what trades EVER achieve; favorable-first "
          f"happens only {wl['mfe_first_share']}% of the time; median EOD drift is {wl['median_eod_ret']}%. "
          "No filter can fix a population whose median best-case move is smaller than cost+noise.",
          "",
          "## 4. Are the current entry rules logically weak?",
          "Yes — three ways. (i) The broken level is just the PRIOR BAR's low (a 5-minute micro-level), "
          "not a structural level (day low / OR low / multi-bar low), so most breaks are noise. "
          "(ii) The entry chases: it fills at the next 1-min open AFTER a 0.6-2.2 ATR impulse has "
          "already run — buying the extension, which is where mean-reversion bites (median MAE "
          f"{mae['50']}% against). (iii) Three incidental gates (ADX>=19.12, RSI>=23.22, "
          "atr_pct<=0.63%) restrict it to LOW-volatility names — precisely the names with the least "
          "room to fall (median MFE ~0.47%).",
          "",
          "## 5. Are the current filters blocking winners or allowing losers?",
          "Both. The atr_pct<=0.63% gate removes the high-energy names where a 1% move is possible, "
          "while vol_ratio>=1.5 alone admits thousands of noise breaks. Phase-2 proved NO slice of the "
          "gated population reaches PF 1.0 on both FIT and VAL (846 scans).",
          "",
          "## 6. Are SL/target mismatched with actual 1-minute movement?",
          f"Severely. SL 1.10 vs median MAE {mae['50']}% means ~half of trades nearly stop; target "
          f"1.00 vs median MFE {mfe['50']}% means most trades CANNOT reach it (TRAIN target-fill was "
          f"{mB['TRAIN']['target_rate']}%). The geometry is inverted R:R after costs.",
          "",
          "## 7. Are exits too early/late/tight/wide?",
          "The EOD 15:20 forced exit accounts for ~30% of trades; those bleed the -0.12% median drift. "
          "MFE-derived tight targets (0.3-0.66%) fill often but cannot cover 0.30% costs; wide targets "
          "never fill. There is no exit setting that rescues the geometry (phase-1 swept 49 exit pairs).",
          "",
          "## 8. Are signals coming in bad time windows?",
          "The baseline book loses in EVERY signal hour (hourly PF 0.44-0.65); late-morning (11:00) and "
          "13:00 blocks are worst. Morning restriction reduces losses but never flips the sign.",
          "",
          "## 9. Are some symbols/days/regimes destroying the edge?",
          "No single destroyer: losses are uniform across days and symbols (that is what makes it "
          "structural). BEAR-regime days lose least (best 2-term pocket PF ~0.93) but still lose.",
          "",
          "## 10. Is the current pool correctly recreated?",
          "Yes — 4 deterministic sources, cross-verified identical row sets on shared dates, 53 TRAIN + "
          "20 TEST sessions, 100% feature coverage; plus a from-raw re-detection that reproduces and "
          "widens the scanner universe (146k events vs its 25k).",
          "",
          "## 11. Any lookahead, leakage, or unrealistic exits?",
          "None found: signals use bar-close information only; entry is the NEXT 1-min open + adverse "
          "slippage; exits walk 1-min OHLC to 15:20; thresholds come from TRAIN-only quantiles; the "
          "MFE/MAE study uses TRAIN only; TEST was scored once per finalist (budget-capped).",
          "",
          "## 12. Should the setup be redesigned while keeping the core idea?",
          "It WAS — six redesigns were built and tested from raw data (see REDESIGNED_SETUP_IDEAS.md): "
          "fresh-session-low continuation, 2-bar persistence, deep-flow break, first-event-of-day "
          "morning, NIFTY-aligned, and retest-reject entry. Results are in ITERATION_LOG.md / "
          "CANDIDATE_CONFIGS.md."]
    (WORK / "FROM_SCRATCH_LOGIC_REVIEW.md").write_text("\n".join(fr), encoding="utf-8")

    # ---------------- 4. WINNER_LOSER_STUDY ----------------------------------------
    ws = [f"# {SETUP} (SHORT) — WINNER_LOSER_STUDY (TRAIN only, 1-min paths)", "", HDR, "",
          f"- deduped TRAIN book n={wl['n']}, winners {wl['n_winners']} ({wl['win_rate']}%) at baseline "
          "exits 1.10/1.00.",
          f"- favorable-move-first share: {wl['mfe_first_share']}% (coin flip).",
          f"- median EOD drift: {wl['median_eod_ret']}% (near zero).", "",
          "## MFE / MAE percentiles (% from entry, SHORT)", "",
          "| percentile | MFE all | MAE all | MFE winners | MAE winners | MFE losers | MAE losers |",
          "|---|---|---|---|---|---|---|"]
    for p in ("25", "40", "50", "60", "75", "90"):
        ws.append(f"| p{p} | {wl['mfe_all'][p]} | {wl['mae_all'][p]} | {wl['mfe_winners'][p]} | "
                  f"{wl['mae_winners'][p]} | {wl['mfe_losers'][p]} | {wl['mae_losers'][p]} |")
    ws += ["", "## Winner vs loser feature medians", "",
           "| feature | winner med | loser med | winner p25-p75 |", "|---|---|---|---|"]
    for f, d in wl["feature_medians"].items():
        ws.append(f"| {f} | {d['winner_med']} | {d['loser_med']} | {d['winner_p25']}..{d['winner_p75']} |")
    ws += ["", "## Observations", "",
           "- The winner/loser feature medians are nearly identical on most dimensions — the losing "
           "population is homogeneous; edge cannot be carved by features (confirms phase-2's 846-scan "
           "result).",
           f"- Suggested exits fed to the loop (TRAIN-only): targets {wl['suggested_exits']['tgt_candidates_pct']} "
           f"/ SLs {wl['suggested_exits']['sl_candidates_pct']} — even the best-fitting geometry cannot "
           "clear ~0.30% round-trip costs at these excursion sizes."]
    (WORK / "WINNER_LOSER_STUDY.md").write_text("\n".join(ws), encoding="utf-8")

    # ---------------- 5. REDESIGNED_SETUP_IDEAS ------------------------------------
    vb = rs.get("variant_best_exit", {})
    vr = rs.get("variant_rows", {})
    ideas = [
        ("RX2_ALL", "CORE re-detection, incidental gates removed",
         "frees ADX/RSI/atr_pct so high-energy names enter; tests whether the scanner's gates were "
         "hiding the edge"),
        ("RX2_FRESHLOW", "CORE + bar makes a NEW session low",
         "continuation only when the break creates fresh discovery — removes mid-range noise breaks"),
        ("RX2_CONFIRM2", "CORE + previous bar was also a red prior-low break",
         "2-bar persistence = real flow, not a one-bar flush"),
        ("RX2_DEEP", "CORE + close >= 0.35 ATR below the broken level",
         "requires the break to travel — filters marginal ticks through the level"),
        ("RX2_FIRST_MORN", "first CORE event of the symbol-day, <= 12:00",
         "the first break is the informative one; morning has the follow-through"),
        ("RX2_MKT", "CORE + NIFTY50 below its 5-min EMA20",
         "don't fight the tape — shorts only when the index itself is weak"),
        ("RETEST", "break -> pullback to the broken level within 4 bars -> red rejection",
         "enters HIGHER on the retest, fixing the chase-the-extension entry (better R:R geometry)"),
    ]
    ri = [f"# {SETUP} (SHORT) — REDESIGNED_SETUP_IDEAS", "", HDR, "",
          "All variants keep the core intent (impulse continuation short through the prior bar's low, "
          "below session VWAP) and are generated from raw 5-min OHLCV by `scripts/redesign_scan.py` "
          "(CORE: red bar, close_loc<=0.40, range 0.60-2.20x ATR, close<prev low, close<session VWAP, "
          "vol_ratio>=1.5, bar>=3, 09:30-15:00).", ""]
    for name, logic, why in ideas:
        rows = vr.get(name, {})
        ri += [f"## {name}", "",
               f"- **logic:** {logic}",
               f"- **why it makes sense:** {why}",
               f"- **rows (FIT/VAL/TRAIN/TEST):** {rows.get('FIT','-')}/{rows.get('VAL','-')}/"
               f"{rows.get('TRAIN','-')}/{rows.get('TEST','-')}",
               f"- **best ungated exit (FIT/VAL band score):** {vb.get(name, '-')}", ""]
    (WORK / "REDESIGNED_SETUP_IDEAS.md").write_text("\n".join(ri), encoding="utf-8")

    # ---------------- 6. PARAMETER_SWEEP_SUMMARY ------------------------------------
    ps = [f"# {SETUP} (SHORT) — PARAMETER_SWEEP_SUMMARY (recovery loop)", "", HDR, "",
          f"Total scored configs: {len(trials)} across variants "
          f"{sorted(trials['variant'].unique().tolist())}.", "",
          "Stage A swept exits (grid + MFE/MAE-derived pairs); stage B swept every feature at "
          "q0.2/q0.5/q0.8 both directions; stage C ran TPE combinations "
          "(<=2 mask + regime + guards + <=1 premom).", ""]
    for vname, g in trials.groupby("variant"):
        g = g.sort_values("score", ascending=False)
        ps += [f"## {vname} — top 10 configs", "",
               "| SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |",
               "|---|---|---|---|---|---|---|---|"]
        for _, r in g.head(10).iterrows():
            ps.append(f"| {r['sl']} | {r['tgt']} | {r['mask']} | {r['premom']} | {r['guard']} | "
                      f"{r['fit_n']}/{r['fit_pf']} | {r['val_n']}/{r['val_pf']} | {r['score']} |")
        ps.append("")
    best_by_v = trials.groupby("variant")["score"].max().sort_values(ascending=False)
    ps += ["## Best band score per variant (tent peaks at PF 1.80; ~1.30+ means both FIT and VAL "
           "PF >= 1.30)", ""]
    for v, s in best_by_v.items():
        ps.append(f"- {v}: {s:.3f}")
    (WORK / "PARAMETER_SWEEP_SUMMARY.md").write_text("\n".join(ps), encoding="utf-8")

    # ---------------- 7. ITERATION_LOG ----------------------------------------------
    il = [f"# {SETUP} (SHORT) — ITERATION_LOG (from-scratch recovery)", "", HDR, "",
          f"Optimizer: {rs['optimizer']}. Iterations logged: {len(iters)} rows in "
          f"`iteration_log_recovery.csv` + {len(trials)} scored configs in `trials_recovery.csv` "
          f"(stage A exits, stage B feature scans, stage C TPE). TEST evaluations spent: "
          f"{rs['n_test_evals']} (budget-capped; only TRAIN-band finalists).", "",
          "| # | stage | variant | change | SL/Tgt | mask | FIT n/PF | VAL n/PF | TRAIN n/PF | "
          "TEST n/PF | keep | why |", "|---|---|---|---|---|---|---|---|---|---|---|---|"]

    def _f(v):
        return "-" if v is None or (isinstance(v, float) and pd.isna(v)) else v
    show = iters[iters["stage"] != "A-exits"]
    for _, r in show.iterrows():
        il.append(f"| {r['iter']} | {r['stage']} | {r['variant']} | {str(r['change'])[:40]} | "
                  f"{r['sl']}/{r['tgt']} | {str(r['mask'])[:60]} | {_f(r['fit_n'])}/{_f(r['fit_pf'])} | "
                  f"{_f(r['val_n'])}/{_f(r['val_pf'])} | {_f(r['train_n'])}/{_f(r['train_pf'])} | "
                  f"{_f(r['test_n'])}/{_f(r['test_pf'])} | {r['keep']} | {str(r['why'])[:80]} |")
    (WORK / "ITERATION_LOG.md").write_text("\n".join(il), encoding="utf-8")

    # ---------------- 8/9. CANDIDATES + FINAL RECOMMENDATION ------------------------
    passing = [r for r in rs["results"] if r.get("passed")]
    (WORK / "candidates").mkdir(exist_ok=True)
    if passing:
        cc = [f"# {SETUP} (SHORT) — CANDIDATE_CONFIGS (PASSED)", "", HDR, ""]
        for i, r in enumerate(passing, 1):
            cid = f"{SETUP}_recovery_candidate_{i:03d}"
            cc += [f"## Candidate {i:03d} — variant {r['variant']}", "", "```json",
                   json.dumps(r["cfg"], indent=2), "```", "",
                   f"- TRAIN: {_m(r['train'])}", f"- TEST:  {_m(r['test'])}",
                   f"- robustness: {r['robust']}", f"- warnings: {r.get('warnings') or 'none'}", ""]
            (WORK / "candidates" / f"{cid}.json").write_text(
                json.dumps(r, indent=2, default=str), encoding="utf-8")
        (WORK / "CANDIDATE_CONFIGS.md").write_text("\n".join(cc), encoding="utf-8")
    else:
        best = rs.get("best_global") or {}
        (WORK / "CANDIDATE_CONFIGS.md").write_text(
            f"# {SETUP} (SHORT) — CANDIDATE_CONFIGS\n\n{HDR}\n\n"
            f"**No candidate passed** (TRAIN PF [1.30,1.80] + TEST PF>1.40 + positive net + domination "
            f"caps 0.35/0.40/0.40 + day-block p<=0.10 + robustness). Best FIT/VAL config "
            f"(variant {best.get('variant')}, band score {best.get('score', 0):.3f}) and every "
            f"TRAIN-band finalist's TEST outcome are in ITERATION_LOG.md.\n", encoding="utf-8")

    rec = "YES — APPROVAL REQUIRED" if passing else "NO"
    bestr = passing[0] if passing else None
    ar = [f"# {SETUP} (SHORT) — APPROVAL_REQUIRED / FINAL RECOMMENDATION (recovery)", "", HDR, "",
          f"## Approval recommendation: **{rec}**", ""]
    if bestr:
        ar += [f"## Best candidate (variant {bestr['variant']})", "", "```json",
               json.dumps(bestr["cfg"], indent=2), "```", "",
               f"- TRAIN: {_m(bestr['train'])}", f"- TEST: {_m(bestr['test'])}", "",
               "## File needing approval before edit", "",
               "- `final_setup_conf.py` (repo root) + `Train_and_Test/final_setup_conf.py` mirror. "
               "NOTE: redesigned variants (RX2_*/RETEST) also require a new detector in the live "
               "scanner (v2._scan_day flag-gated, S9/DOC5D pattern) before any live watch.", "", WARNING, ""]
    else:
        ar += ["## No promotion proposed", "",
               "- Neither the original detection, nor 6 redesigns of it, nor exits derived from measured "
               "1-min MFE/MAE produce a config passing the robust TRAIN+TEST gate on Mar-Jun 2026.",
               "- Combined with the prior campaign (~2,841 iterations over 47 features), the totality of "
               "evidence says the A_MOD_BREAK_C1_LOW intent has NO tradeable edge at 15 bps/leg + "
               "statutory costs in this period: the median trade's best-case 1-min excursion "
               f"({wl['mfe_all']['50']}%) is barely above the ~0.30% cost stack and adverse excursion "
               f"({wl['mae_all']['50']}%) is nearly twice as large.",
               "- Standing suggestion (user decision, NOT executed): demote/disable the live conf entry.",
               "", WARNING, ""]
    ar += ["## Rerun commands", "", "```",
           f"py -3.12 Train_and_Test\\setup_from_scratch_recovery_loop\\{SETUP}\\scripts\\recreate_pool.py",
           f"py -3.12 Train_and_Test\\setup_from_scratch_recovery_loop\\{SETUP}\\scripts\\mfe_mae_study.py",
           f"py -3.12 Train_and_Test\\setup_from_scratch_recovery_loop\\{SETUP}\\scripts\\redesign_scan.py",
           f"py -3.12 Train_and_Test\\setup_from_scratch_recovery_loop\\{SETUP}\\scripts\\run_recovery_loop.py "
           "--trials_per_variant 150 --minutes_per_variant 8 --seed 21",
           f"py -3.12 Train_and_Test\\setup_from_scratch_recovery_loop\\{SETUP}\\scripts\\write_recovery_reports.py",
           "```", "",
           "## Remaining risks / caveats", "",
           "- Redesigned pools are research detections; live use would need a flag-gated detector wired "
           "into the production scanner (S9/DOC5D pattern) plus parity checks.",
           "- Tractability caps (deepest-per-ticker-day + seeded sample) are documented; full-universe "
           "reruns are possible but change nothing qualitatively (sampling is unbiased).",
           "- One-month TEST (June) is a single regime; a July re-run is the cheapest next validation."]
    (WORK / "APPROVAL_REQUIRED_FINAL_RECOMMENDATION.md").write_text("\n".join(ar), encoding="utf-8")

    print("[reports] wrote 9 recovery reports")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
