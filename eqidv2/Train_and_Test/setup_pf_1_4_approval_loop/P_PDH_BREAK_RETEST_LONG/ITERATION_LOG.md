# P_PDH_BREAK_RETEST_LONG — ITERATION_LOG

Optimizer: **Optuna TPE (4.9.0)** + deterministic hand/structural sweeps. Cost: statutory NSE + **15 bps/leg** slippage.
Windows (nearest-available; June unavailable — see BASELINE_RESULT §3): TRAIN `2026-04-01..2026-05-15` (27 sess), TEST `2026-05-18..2026-05-29` (9 sess), FIT/VAL = halves of TRAIN.
Target band: TRAIN PF ∈ [1.30, 1.70], TEST PF > 1.40, train_n ≥ ~25, test_n ≥ 15, no day/symbol/trade share > 0.45.

Total configs evaluated: **197 hand iterations + 400 Optuna trials (120 confirmed on TRAIN/TEST) + 20 focused structural iterations ≈ 617**. Far exceeds the 25-iteration floor; the setup is clearly dead (see verdict).

Commands:
```
py -3.12 Train_and_Test/setup_pf_1_4_approval_loop/P_PDH_BREAK_RETEST_LONG/scripts/build_pool.py
py -3.12 Train_and_Test/setup_pf_1_4_approval_loop/P_PDH_BREAK_RETEST_LONG/scripts/optimize_ppdh.py --trials 400 --time_budget_min 16 --test_n 9 --train_n 27
py -3.12 Train_and_Test/setup_pf_1_4_approval_loop/P_PDH_BREAK_RETEST_LONG/scripts/analyze.py
py -3.12 Train_and_Test/setup_pf_1_4_approval_loop/P_PDH_BREAK_RETEST_LONG/scripts/iterate2.py
```

---

## Phase 0 — Baseline (demoted conf config)
SL/Tgt 0.50/0.60, mask `body_pct≤0.75`, premom `score≥75.07 & range_r≥0.50`.
- TRAIN: n=36 PF **0.238** net −12,055 (20 SL / 10 TGT / 6 EOD, win 31%)
- TEST: n=9 PF **0.675** net −876 (4 SL / 5 TGT, win 56%)
- **Reject — TRAIN PF too low.** Diagnosis: 0.50/0.60 scalp, avg_loss (−633) ≈ 2× avg_win (+343); cost+slippage dominates. (full file `iterations.csv`)

## Phase 1 — Exit / SL / target grid (change group: exit)
6 SL × 7 Tgt on (a) the **ungated** firehose and (b) the **baseline-gated** book. Full grid in `iterations.csv`.

**(a) Ungated TRAIN PF** — every cell **0.19–0.37** (386 trades). The raw signal has no directional edge regardless of exit.
| SL\Tgt | 0.60 | 1.00 | 1.50 | 2.50 |
|---|---|---|---|---|
| 0.50 | 0.19 | 0.23 | 0.26 | 0.27 |
| 0.70 | 0.24 | 0.30 | 0.32 | 0.32 |
| 1.20 | 0.27 | 0.34 | 0.36 | 0.37 |

→ **Reject all — TRAIN PF too low.** Classification: *no edge in raw pool*. Wider SL/target lifts PF marginally (cost amortized over bigger moves) but never near 1.30.

**(b) Baseline-gated TRAIN PF** — best legit cell **0.70/2.50 = 1.28** (still < 1.30); `1.20/0.60 = 8.14` is a small-sample artifact (rejected: *one-trade/overfit*). Wider targets clearly help but cap below the band.
→ **Reject all — TRAIN PF too low** (or *overfit artifact* for the 8.14 cell).

## Phase 2 — Single-knob filters / gates / guards (hand)
~110 configs: one mask term (12 features × {q30,q50,q70} × {≥,≤}) at exit 0.70/1.25; one premom gate (8 features × {q40,q60} × {≥,≤}); time guards (min_slot/max_slot/top_n). Full file `iterations.csv`.
→ **0 reached TRAIN PF ≥ 1.30 with a meaningful sample.** Single structural terms don't gate hard enough; the firehose stays a loser.

## Phase 3 — Optuna TPE FIT/VAL search (change group: combined mask + premom + guards + overlay)
400 trials, objective `min(FIT_PF,VAL_PF) − 0.5·|gap|`, ≤2 mask + ≤2 premom terms, guards, max_positions, daily_loss. Best FIT/VAL score **1.01**. Top 120 confirmed on TRAIN/TEST (`optuna_confirmed.csv`).
- TRAIN PF distribution of top 120: **mean 0.93, max 2.02**.
- Only configs with TRAIN PF ≥ 1.30: a single overfit pocket `lower_wick_pct≥0.007 & vol_ratio≥7.12` → TRAIN **2.02 / n=21**, TEST **0.84 / n=3**. → **Reject — TRAIN PF too high / overfit; test collapses; too few trades.**
- Configs with the best TEST PF (`wick_skew≥−0.053 & close_loc≥0.993`): TEST 1.37–1.39 but **TRAIN only 0.96–1.05** (below band) and **test_dom_day 2.0–3.4** (one day = all profit). → **Reject — TRAIN PF too low + one day dominated.**
- **0 candidates** (TRAIN band + TEST>1.40 + stability).

## Phase 4 — Focused structural iterations (`iterations2.csv`, 20 logged)
Each changes one logical group; TRAIN+TEST+dominance always computed.

| # | Change (group) | TRAIN n/PF | TEST n/PF | domday | Verdict / classification |
|---|---|---|---|---|---|
| A1 | G + exit 0.70/2.00 (exit) | 20 / 1.05 | 7 / 0.72 | – | too few trades (train) |
| A2 | G + exit 0.70/2.50 (exit) | 20 / **1.28** | 7 / 0.83 | – | too few trades; TEST<1.40 |
| A3 | G + exit 0.85/2.50 (exit) | 17 / 0.86 | 5 / 0.79 | – | too few; TRAIN low |
| A4 | G + exit 1.00/2.50 (exit) | 15 / 1.18 | 5 / 0.70 | – | too few; TEST<1.40 |
| A5 | G + exit 0.50/0.80 (file alt) | 36 / 0.30 | 9 / 1.04 | 10.0 | TRAIN PF too low; one day dominated |
| B1 | + rs_pct≥0 (mask) | 20 / 1.05 | 7 / 0.72 | – | too few; TEST<1.40 |
| B2 | + close_loc≥0.6 (mask) | 19 / 1.18 | 6 / 0.80 | – | too few; TEST<1.40 |
| B3 | + vol_ratio≥2.0 (mask) | 16 / 1.20 | 6 / 1.03 | 21.3 | too few; one day dominated |
| B4 | + vol_ratio≤4.0 (mask) | 11 / 0.43 | 4 / 0.55 | – | too few; TRAIN low |
| B5 | + quality_score≥median (mask) | 9 / **4.22** | 6 / 1.03 | 21.3 | too few; **overfit**; one day dominated |
| C1 | + min_slot 09:45 (time) | 20 / 1.05 | 7 / 0.72 | – | too few; TEST<1.40 |
| C2 | + max_slot 11:30 morning (time) | 10 / 1.32 | 3 / 6.00 | 1.2 | too few; **single test day** |
| C3 | + window 09:45–12:30 (time) | 13 / 1.26 | 5 / 1.83 | 2.2 | too few; **single test day** |
| C4 | + top_n 1/slot (guard) | 17 / 0.96 | 7 / 0.72 | – | too few; TRAIN low |
| D1 | + regime_align (regime) | 14 / 0.85 | 6 / 1.03 | 21.3 | too few; one day dominated |
| D2 | + daily_loss_rs 2500 (overlay) | 20 / 1.05 | 7 / 0.72 | – | too few; TEST<1.40 |
| E1 | mask rs≥0 & close_loc≥0.6 (mask) | 374 / 0.29 | 115 / 0.31 | – | TRAIN PF too low |
| E2 | mask vol≥2 & rs≥0 (mask) | 260 / 0.31 | 85 / 0.23 | – | TRAIN PF too low |
| E3 | pm sig5_adx≥25 only (gate) | 222 / 0.34 | 78 / 0.29 | – | TRAIN PF too low |
| E4 | pm score≥85 & range_r≥0.50 (gate) | 12 / 0.66 | 1 / 0.00 | – | too few; TRAIN low |

→ **PASS = 0 of 20.**

---

## The structural wall (why every iteration fails)
- **Loose enough for a real sample (n ≥ 25) ⟹ PF ≈ 0.29–0.34** (E1/E2/E3; the raw signal is a deep loser).
- **Tight enough for PF ≥ 1.30 ⟹ n ≤ ~20**, which is overfit and whose TEST either collapses (<1.0) or is carried by a single day (C2 n=3, C3 n=5, B-series domday 21×).

There is **no** config with TRAIN PF ∈ [1.30,1.70] **and** train_n ≥ 25 **and** TEST PF > 1.40 **and** no single-day/symbol domination.

**Next action:** none — stop. The April–May backtest agrees with the live-June failure (PF 0.25). See FAILURE_ANALYSIS.md and APPROVAL_REQUIRED_FINAL_RECOMMENDATION.md.
