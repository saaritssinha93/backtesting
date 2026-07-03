r"""append_phase2_reports.py — fold the PHASE-2 (enriched 47-feature) results into the
campaign markdown reports. RESEARCH-ONLY; writes only inside this campaign folder."""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd

_HERE = Path(__file__).resolve().parent
WORK = _HERE.parent
SETUP = "A_MOD_BREAK_C1_LOW"
TODAY = date.today().isoformat()


def _m(mm):
    return (f"n={mm['n']} PF={mm['net_pf']} net=Rs{mm['net_pnl']:,.0f} win%={mm['win_rate']} "
            f"tgt%={mm['target_rate']} dayDom={mm['day_dom']} dbp={mm['day_block_p']}")


def main() -> int:
    rs = json.loads((WORK / "run_summary_enriched.json").read_text(encoding="utf-8"))
    scan = pd.read_csv(WORK / "sweeps_enriched.csv").dropna(subset=["score"])

    # ---------- PARAMETER_INVENTORY ----------
    inv = (WORK / "PARAMETER_INVENTORY.md").read_text(encoding="utf-8")
    inv += (
        f"\n\n## 5. PHASE 2 — enriched feature dictionary (added {TODAY})\n\n"
        "36 additional CAUSAL point-in-time 5-minute features were computed per pool row "
        "(`scripts/enrich_features.py`, 100% row coverage) and searched alongside the "
        "base 11:\n\n"
        "- **indicators:** rsi, rsi_slope3, adx5, adx_slope3, ema20_dist_atr, ema50_dist_atr, "
        "ema20_slope_atr, ema_stack_atr, macd_hist_atr, macd_hist_slope3, bb_pos, bb_width_atr, "
        "stoch_k, stoch_kd, cci20, mfi14, obv_slope6, vol_z\n"
        "- **session/day context:** sess_vwap_dist_atr, below_vwap_streak6, day_pos, "
        "day_low_dist_atr, day_high_dist_atr, bars_since_day_low, bars_since_day_high, gap_pct, "
        "day_ret_pct, c1_range_atr, c1_break_depth_atr\n"
        "- **price action:** ret3_atr, ret6_atr, ret12_atr, red_streak, body_sum6_atr, "
        "range6_atr, range_expansion\n\n"
        "Search widened to <=3 mask terms + regime + <=2 pre-momentum terms; exits extended to "
        "SL up to 2.0% / target up to 3.0%.\n"
    )
    (WORK / "PARAMETER_INVENTORY.md").write_text(inv, encoding="utf-8")

    # ---------- PARAMETER_SWEEP_SUMMARY ----------
    ps = (WORK / "PARAMETER_SWEEP_SUMMARY.md").read_text(encoding="utf-8")
    lines = [f"\n\n## PHASE 2 — standalone single-feature scan, 47 features x 9 quantiles x 2 ops "
             f"({len(scan)} scans, added {TODAY})\n",
             "Baseline exits 1.10/1.00, no pre-momentum, scored on FIT+VAL.\n",
             "| feat | op | thr (q) | FIT n/PF | VAL n/PF | score |", "|---|---|---|---|---|---|"]
    for _, r in scan.sort_values("score", ascending=False).head(20).iterrows():
        lines.append(f"| {r['feat']} | {r['op']} | {r['thr']} (q{r['q']}) | "
                     f"{int(r['fit_n'])}/{r['fit_pf']} | {int(r['val_n'])}/{r['val_pf']} | {r['score']} |")
    lines += ["",
              f"**Key finding:** 0 of {len(scan)} single-feature slices reach min(FIT,VAL) PF >= 1.0 "
              "(best: gap_pct<=-1.83 at PF 0.48/0.55). The losing population is homogeneous across "
              "every indicator, price-action, volume, session-context and day-context dimension — "
              "there is no structural sub-population where this breakdown pattern is net-profitable "
              "at realistic costs."]
    (WORK / "PARAMETER_SWEEP_SUMMARY.md").write_text(ps + "\n".join(lines), encoding="utf-8")

    # ---------- ITERATION_LOG ----------
    il = (WORK / "ITERATION_LOG.md").read_text(encoding="utf-8")
    add = [f"\n\n# PHASE 2 — enriched search (added {TODAY})\n",
           f"- Stage E1 single-feature scan: {rs['n_scan']} iterations (sweeps_enriched.csv)",
           f"- Stage E2 Optuna TPE: {rs['n_trials']} trials / {rs['n_unique']} unique configs "
           f"(trials_enriched.csv), best FIT/VAL band score {rs['best_fitval_score']:.3f}",
           f"- Stage E3/E4 confirmations + rescue: see iteration_log_enriched.csv "
           f"({rs['n_test_evals']} TEST evaluations spent)",
           "", "## Phase-2 finalists (TRAIN-band configs, TEST scored once each)", "",
           "| # | TRAIN | TEST | verdict |", "|---|---|---|---|"]
    for r in rs["results"]:
        if "test" not in r:
            continue
        add.append(f"| {r.get('tag', r['id'])} | {_m(r['train'])} | {_m(r['test'])} | "
                   f"{'PASS' if r.get('passed') else 'REJECT: ' + '; '.join(r['hard_reasons'][:3])} |")
    add += ["", "**Every TRAIN-band finalist collapsed out-of-sample (TEST PF 0.05-0.28, zero "
            "target exits, all net-negative, all day-concentrated). The in-band TRAIN pockets are "
            "noise, not edge.**"]
    (WORK / "ITERATION_LOG.md").write_text(il + "\n".join(add), encoding="utf-8")

    # ---------- FAILURE_ANALYSIS ----------
    fa = (WORK / "FAILURE_ANALYSIS.md").read_text(encoding="utf-8")
    fa += (
        f"\n\n## PHASE 2 — enriched-search failure evidence (added {TODAY})\n\n"
        f"- {rs['n_scan']} standalone feature scans: best single slice PF 0.48/0.55 (gap_pct) — "
        "0 slices at PF>=1.0 on both FIT and VAL.\n"
        f"- {rs['n_trials']} TPE trials found 3-term TRAIN-band cohorts (PF 1.41-1.80, n 22-42) — "
        "ALL failed the TRAIN day-domination cap (0.50-0.81 vs 0.40) and ALL collapsed on TEST "
        "(PF 0.049-0.283, 0 target fills, every book net-negative).\n"
        "- Interpretation: with a base population PF of ~0.40, any cohort that reaches PF 1.3+ "
        "in-sample is a handful of lucky day-clustered trades; the OOS month falsifies every one. "
        "This mirrors the P_PDH structural-wall finding (2026-06-30).\n"
    )
    (WORK / "FAILURE_ANALYSIS.md").write_text(fa, encoding="utf-8")

    # ---------- CANDIDATE_CONFIGS + FINAL RECOMMENDATION ----------
    cc = (WORK / "CANDIDATE_CONFIGS.md").read_text(encoding="utf-8")
    cc += (f"\n\n## PHASE 2 (enriched, added {TODAY})\n\n"
           "**Still no passing candidate.** 2,074 additional iterations over 47 mask features, "
           "8 pre-momentum features, regime, guards, and extended exits produced 8 TRAIN-band "
           "finalists — every one rejected on TEST collapse + domination.\n")
    (WORK / "CANDIDATE_CONFIGS.md").write_text(cc, encoding="utf-8")

    ar = (WORK / "APPROVAL_REQUIRED_FINAL_RECOMMENDATION.md").read_text(encoding="utf-8")
    ar += (
        f"\n\n## PHASE 2 addendum ({TODAY}) — recommendation remains **NO**\n\n"
        f"- Combined campaign: ~2,841 iterations (phase 1: 767 base-feature; phase 2: {rs['n_scan']} "
        f"feature scans + {rs['n_trials']} TPE trials + confirmations).\n"
        "- The full indicator space (RSI/ADX/EMA/MACD/BB/Stoch/CCI/MFI/OBV/vol-z/VWAP-context/"
        "day-context/C1-geometry/momentum/streak/pressure/compression) contains NO slice where "
        "A_MOD_BREAK_C1_LOW is net-profitable at 15 bps/leg + statutory costs on Mar-Jun 2026.\n"
        "- Every TRAIN-band config found is day-concentrated noise that loses 70-95% of risked "
        "capital OOS. Promoting any of them would be exactly the fake-overfit failure mode this "
        "campaign was designed to prevent.\n"
        "- Suggested user decision (NOT executed): demote/disable the live conf entry for this "
        "setup — its production config lost on both TRAIN (PF 0.54) and TEST (PF 0.34) on the "
        "recreated pool.\n\n"
        "> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**\n\n"
        "## Phase-2 rerun commands\n\n```\n"
        f"py -3.12 Train_and_Test\\setup_pf_1_4_full_loop\\{SETUP}\\scripts\\enrich_features.py\n"
        f"py -3.12 Train_and_Test\\setup_pf_1_4_full_loop\\{SETUP}\\scripts\\run_enriched_loop.py "
        "--trials 1200 --time_budget_min 60 --seed 11\n"
        f"py -3.12 Train_and_Test\\setup_pf_1_4_full_loop\\{SETUP}\\scripts\\append_phase2_reports.py\n"
        "```\n"
    )
    (WORK / "APPROVAL_REQUIRED_FINAL_RECOMMENDATION.md").write_text(ar, encoding="utf-8")

    print("[phase2-reports] appended phase-2 sections to 5 reports")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
