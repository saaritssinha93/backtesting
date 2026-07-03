# C_OR_BREAKDOWN — Baseline

**Side:** SHORT  **Status in source-of-truth:** ACTIVE (survival-book mined short).  **Processed:** 2026-06-29.
**Faithfulness:** readmit basis → fast pool harness is live-faithful (same caveat as B_HUGE_RED on basis size).

## Split (printed)
- TRAIN = 2026-04-13 .. 2026-05-25 (6 weeks); TEST = 2026-05-26 .. 2026-06-24.
- Pool: per-setup slice of `outputs_ID_v11_unified_pool` (25,272 raw rows total).
- Net of v6 cost; reported at **both 5 and 15 bps/leg** slippage (slippage lesson from setup 1).

## Conf gate of record (live today)
- exit SL 0.90 / Tgt **2.00** (wide target)
- mask_terms: none
- pre_momentum_terms (ALL required, missing→block): `sig5_adx_calc ≥ 39.670518`, `pre1_adx ≤ 21.368044`
- entry_guards: none

## Backtest-vs-live logic check
**No mismatch.** Bootstrap-only (doc §5.4 — C_OR_BREAKDOWN is not in the v11 overlay universe), and the live
bootstrap applies the conf `pre_momentum_terms` from `FINAL_SETUP_CONF` verbatim. Backtest == live.

## Baseline result (conf gate, fresh window)

| Slippage | Period | n | net PF | net Rs | win% | T/SL/EOD% | day_block_p |
|---|---|---:|---:|---:|---:|---:|---:|
| 15 bps/leg | TRAIN | 83 | **0.64** | -15,814 | 37.3 | 11/39/51 | 0.94 |
| 15 bps/leg | TEST  | 53 | **0.62** | -9,340 | 39.6 | 8/32/60 | 0.97 |
| 5 bps/leg  | TRAIN | 83 | **0.94** | -1,995 | 42.2 | 11/36/53 | 0.57 |
| 5 bps/leg  | TEST  | 53 | **1.12** | +2,144 | 54.7 | 9/28/62 | 0.36 |

Reference — **ungated raw**: catastrophic at both slippages (TRAIN PF 0.28–0.47, −Rs200k–390k). The gate helps
massively but does not reach robust profitability.

## Observations
- **TRAIN is a loser even at 5 bps (0.94).** TEST 1.12 @5 bps is marginal and **one day = 185% of TEST net**
  (concentration). At realistic 15 bps both periods are clear losers (~0.62).
- The wide **2.0% target is rarely reached** (≈10% TARGET) → ~50–60% EOD; high SL rate (32–39%).
  The breakdown-continuation thesis is not following through to 2% on this window.
- Sample is healthier than B_HUGE_RED (n=83 train / 53 test).
- Conf claimed train 2.78 / test 5.26 — not reproduced (basis-size discrepancy again: n=83 vs conf n=29).

## Loss-mode analysis + iterations
See `C_OR_BREAKDOWN_experiment_log.md`.
