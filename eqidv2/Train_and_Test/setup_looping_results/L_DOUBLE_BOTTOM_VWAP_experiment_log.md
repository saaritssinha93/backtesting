# L_DOUBLE_BOTTOM_VWAP — Experiment Log

Window: TRAIN 2026-04-13..2026-05-25, TEST 2026-05-26..2026-06-24. Net of v6 cost; 5 + 15 bps/leg.
Bar: TRAIN PF>1.2, TEST PF≥1.3, non-concentrated, no TRAIN→TEST collapse, robust at realistic cost.

## Baseline (conf gate: pre_entry_momentum_score≥79 & sig5_adx_calc≥28, exit 0.9/1.5)
- @15 bps: TRAIN 0.88 (n38) / TEST 0.29 (n29) — **TEST 72% SL**.
- @5 bps : TRAIN 1.30 (n38) / TEST 0.48 (n29) — **TEST 65% SL**.

## Hand iterations (12 configs × both slippages)

| # | Change group | @5 bps TRAIN/TEST | @15 bps TRAIN/TEST | TEST SL% | Keep? |
|---|---|---|---|---|---|
| 1 | baseline (0.9/1.5) | 1.30 / 0.48 | 0.88 / 0.29 | 65–72 | reject |
| 2 | alt G-gate (pre2_mom≥0.42) | 1.60 / 0.56 | 1.02 / 0.31 | 60–68 | reject (TRAIN ok, TEST collapse) |
| 3 | exit 0.9/1.25 | 1.43 / 0.47 | 0.82 / 0.28 | 65–72 | reject |
| 4 | exit 0.9/2.0 | 1.26 / 0.41 | 0.97 / 0.25 | 69–76 | reject |
| 5 | exit 0.7/1.5 | 1.29 / 0.50 | 0.68 / 0.21 | 70–79 | reject |
| 6 | exit 1.1/1.5 (widest SL) | 1.28 / 0.92 | 0.90 / 0.74 | 53 | reject (best TEST, still <1) |
| 7 | mom loose ≥70 | 1.13 / 0.62 | 0.77 / 0.40 | 51–58 | reject |
| 8 | mom tight ≥85 | 1.25 / 0.66 | 0.97 / 0.56 | 67 | reject |
| 9 | adx tight ≥34 | 0.79 / 0.70 | 0.59 / 0.43 | 63–68 | reject |
| 10 | + vol_ratio≥2 | 1.17 / 0.41 | 0.77 / 0.22 | 67–75 | reject |
| 11 | + rs_pct≥0.5 | 1.55 / 0.46 | 0.95 / 0.27 | 65–73 | reject (TRAIN ok, TEST collapse) |
| 12 | no premom (mask off) | 0.69 / 0.53 | 0.43 / 0.34 | — | reject (premom gate helps TRAIN only) |

## Verdict
**REJECT (keep parked).** Universal TEST collapse: every gate/exit/slippage combination has TEST PF ≤ 0.92 with a
**60–79% SL rate**. Several configs are TRAIN-positive at 5 bps (alt-gate 1.60, rs-strong 1.55, 0.9/1.25 1.43) but
**TEST collapses regardless** — this is a regime failure for double-bottom-reclaim LONGS in the late-May/June test
period (longs walled by stops), not an overfit-able or cost-driven problem. Widening the SL to 1.1% (i06) only
lifts TEST to 0.92 (still a loser). The doc's published train 2.55 / test 3.57 (RAW-pool, with a live-gating
caveat) is **not reproduced**. No config change made; the live research layer also still blocks the L* family.
