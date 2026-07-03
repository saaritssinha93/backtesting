# A_MOD_BREAK_C1_LOW — Experiment Log

Window: TRAIN 2026-04-13..2026-05-25, TEST 2026-05-26..2026-06-24. Net of v6 cost; reported at 15 bps/leg
(realistic/deployable) and 5 bps/leg (paper). Bar: TRAIN PF>1.2, TEST PF≥1.3, non-concentrated, robust at realistic cost.

## Baseline (conf gate: vol_ratio≥1.956 + premom pre5_mom_r≥0.426 & pre3_range_r≤0.202, exit 1.10/1.00)
- @15 bps: TRAIN 0.62 (n72) / TEST 0.57 (n23) — clear loser.
- @5 bps : TRAIN 1.23 (n71, well-distributed) / TEST 1.06 (n22, one day=243% of test net).

Loss modes (15 bps): 33% TARGET / 28% SL / 39% EOD; asymmetric 1.10/1.00 exit → high-win scalp; losers spread
across hours; minor repeat-symbol clusters (ACI n2, JARO n2).

## Hand iterations (12 configs × both slippages)

| # | Change group | @5 bps TRAIN/TEST | @15 bps TRAIN/TEST | Keep? | Reason |
|---|---|---|---|---|---|
| 1 | baseline 1.10/1.00 | 1.23 / 1.06 | 0.62 / 0.57 | — | best TEST @5 bps; loser @15 bps |
| 2 | exit 0.9/1.0 | 0.94 / 0.75 | 0.47 / 0.49 | reject | tighter SL hurts the high-win scalp |
| 3 | exit 0.7/1.0 | 0.56 / 0.79 | 0.30 / 0.40 | reject | far worse |
| 4 | exit 1.1/1.5 | 1.16 / 1.10 | 0.70 / 0.64 | reject | similar @5 bps; loser @15 bps |
| 5 | exit 0.9/1.5 | 0.85 / 0.82 | 0.51 / 0.55 | reject | worse |
| 6 | exit 1.1/0.8 | 1.13 / 0.94 | 0.58 / 0.55 | reject | higher win, lower PF |
| 7 | premom loose (≥0.30) | 0.89 / 0.75 | 0.52 / 0.46 | reject | loosening dilutes edge |
| 8 | premom tight (≥0.55) | 1.44 / 0.93 (n11/8) | 0.75 / 0.43 | reject | sample too thin; TEST fails |
| 9 | no premom (mask only) | 0.53 / 0.75 | 0.29 / 0.46 | reject | premom gate IS essential |
| 10 | + vol band ≤4.0 | **1.48 / 0.96** (n48, dbp 0.16) | 0.76 / 0.53 | reject | best TRAIN @5 bps but TEST degrades; loser @15 bps |
| 11 | + rs_pct≤-2.0 | 1.38 / 0.00 (n1) | 0.71 / 0.00 (n2) | reject | TEST sample destroyed |
| 12 | guard max_slot 12:30 | 1.16 / 0.81 | 0.58 / 0.55 | reject | TEST collapse |

**Read:** At 5 bps the baseline gate is near-optimal for TEST (1.06); every TRAIN-boosting variant (volband 1.48,
mom-tight 1.44, rs-weak 1.38) **degrades TEST** — classic overfit. No config reaches TEST ≥ 1.3. At realistic
15 bps **every config is a loser** (best 0.76/0.53). The pre-momentum gate is load-bearing (mask-only → 0.29/0.53).

## Capstone — maxpf @15 bps (can anything survive realistic cost?)
Best TRAIN cfg = mask `atr_pct≥0.0034 & quality_score≤48.5` + premom `pre3_range_r≤0.176 & sig5_rsi_dir≥62.8`,
exit 1.2/1.5 → TRAIN PF 1.43 (n=36) but **TEST p = 0.974 → BH-FDR-dropped** (a complete coin flip out of sample).
Nothing survives realistic cost.

## Verdict
**REJECT for sizing / keep unsized.** Loser at realistic 15 bps/leg across all 12 iterations + the maxpf capstone
(test p 0.97). At paper 5 bps the conf gate is the best TEST config (1.06) but below the 1.3 bar and one-day-
dominated; TRAIN-boosting variants overfit (TEST degrades). Strongest of the four mined shorts at paper cost, but
not deployable. No config change made.
