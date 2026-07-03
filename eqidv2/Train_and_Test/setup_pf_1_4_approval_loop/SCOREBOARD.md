# PF-Band Approval Campaign — SCOREBOARD

_Generated 2026-07-01. Research-only. NO `final_setup_conf.py` edits, NO live trades._

**Gate:** TRAIN PF in **[1.30, 1.70]** (reject >1.70 as overfit) **AND** TEST PF **> 1.40**, with
meaningful trade counts and no single trade/day/symbol dominating. All metrics **net of NSE
intraday costs @ 15 bps/leg**. Optimised only on FIT/VAL; TRAIN confirms; TEST scored once.

**Engine:** `setup_pf_1_4_approval_loop/_engine/pf_band_fitval_loop.py` (Optuna TPE, 200–220 trials/setup),
reusing the repo pipeline via `setup_train_test.py` (entry+exit+cost+dedupe+mask+premom+overlay).

**Session split (June-OOS group):** TRAIN `2026-05-18…06-11` (18 sess) = FIT `05-18…06-01` + VAL `06-02…06-11`;
TEST `2026-06-12…06-24` (5 sess). _Strict `TEST≥06-20` gave only 2 sessions (06-22, 06-24); fell back to the
last 5 available per the "nearest available" rule._

---

## Result: 0 of 12 PASSED — APPROVAL: nothing to promote

### A. June-OOS evaluated (pool has June data)

| Setup | Side | Conf status | Best TRAIN PF (n) | Best TEST PF (n) | Baseline TRAIN/TEST PF | Verdict | Primary failure |
|---|---|---|---|---|---|---|---|
| A_MOD_BREAK_C1_LOW | SHORT | **ACTIVE** | 1.36 (58) | 0.54 (13) | 0.74 / 0.53 | REJECT | TEST PF<1.40; active baseline also TEST loser |
| B_HUGE_RED_FAILED_BOUNCE | SHORT | **ACTIVE** | 0.67 (161) | 0.38 (42) | 1.01 / 0.00 (n=3) | REJECT | no edge; baseline TEST=0 |
| G_HIGHER_HIGH_BREAK | LONG | **ACTIVE** | 1.42 (18) | 0.61 (11) | inf (n=1) / 0.00 (n=3) | REJECT | TRAIN thin; TEST collapses |
| G_LOWER_LOW_BREAK | SHORT | **ACTIVE** | 1.73 (55) | 0.39 (5) | 0.98 (n=7) / – (n=0) | REJECT | TRAIN overfit (>1.70); TEST collapses |
| L_DOUBLE_BOTTOM_VWAP | LONG | **ACTIVE** | 0.42 (251) | 0.54 (42) | 1.77 (n=7) / 2.47 (n=3) | REJECT | best=loser; baseline tiny-n only |
| L_PRESSURE_BURST_VWAP | LONG | research-watch | 0.55 (153) | 0.54 (39) | 0.36 / 0.44 | REJECT | no edge |
| B_AVWAP_RECLAIM_REVERSAL | LONG | research-watch | 0.87 (21) | 0.00 (1) | 0.29 / 0.46 | REJECT | no edge; TEST n=1 |
| B_HUGE_C1_CLOSE_RECLAIM_BREAK | LONG | research-watch | 0.60 (104) | 0.74 (18) | 0.57 / 1.38 | REJECT | TRAIN loser (baseline TEST a fluke) |
| E_VWAP_LOSE_EARLY_SHORT | SHORT | research-watch | 1.16 (21) | 0.46 (11) | 0.51 / 0.33 | REJECT | TRAIN near-band but TEST collapses (overfit) |

### B. May-window fallback (pool ends May; **NO June data** — not a June OOS test)

| Setup | Side | Conf status | Best TRAIN PF (n) | Best TEST PF (n) | Baseline TRAIN/TEST PF | Verdict | Note |
|---|---|---|---|---|---|---|---|
| L_BB_SQUEEZE_LONG | LONG | research-watch | 0.65 (181) | 1.82 (7) | 0.42 / 1.12 | REJECT | TEST fluke on losing TRAIN |
| MR_CONTROLLED_VWAP_EXTREME_FADE_LONG | LONG | research-watch | 1.83 (14) | 0.00 (5) | 0.40 / 0.12 | REJECT | overfit TRAIN; TEST=0 |
| MR_VWAP_EXTREME_RECLAIM_LONG | LONG | research-watch | 0.70 (56) | 0.23 (7) | – | REJECT | no edge |

---

## Read-outs for your approval

**Promote:** none — 0 setups cleared the band gate.

**Demote candidates (currently ACTIVE, no positive June OOS):** `A_MOD_BREAK_C1_LOW`,
`B_HUGE_RED_FAILED_BOUNCE`, `G_HIGHER_HIGH_BREAK`, `G_LOWER_LOW_BREAK`, `L_DOUBLE_BOTTOM_VWAP`.
Both their **current-conf baseline** and the **best config the search could find** are TEST-negative
(or positive only on n≤3). Several have a respectable TRAIN-side band config (A_MOD 1.36, G_HIGHER 1.42,
G_LOWER 1.73) that does **not** survive the June TEST.

**Keep parked (research-watch, re-confirmed no edge):** the other 7.

**IMPORTANT CAVEAT — thin June data.** The TEST window is 5 sessions and selective configs land at
3–13 test trades. So "TEST PF>1.40" is a **noisy** bar: these are best read as *"no positive OOS evidence"*
rather than hard proof of loss. A clean re-test needs a fresh pool covering more late-June/July sessions.

> **DO NOT MOVE ANYTHING TO/FROM FINAL CONFIG UNTIL USER APPROVES.**

Per-setup detail: `setup_pf_1_4_approval_loop/<SETUP>/{BASELINE_RESULT, ITERATION_LOG, FAILURE_ANALYSIS,
CANDIDATE_CONFIGS, APPROVAL_REQUIRED_FINAL_RECOMMENDATION}.md` + `run_summary.json` + `trials.csv`.
