# Exit / Walk-forward / Regime Diagnostics - FAST_MOMENTUM_LONG

Rule: `LONG_VOLUME_EXPANSION_BREAKOUT_vol2_h5`  |  base signals: 11,679 over 40 sessions  (2026-04-30..2026-06-29)
Pocket exit (from optimizer): `sl0.75_tgt1_tb3`  |  exit used for B/C (best base-population bracket, all sessions): `sl0.75_tgt1_tb9`

These three parts test the three follow-ups to the TRAIN-PF optimizer: can a better EXIT rescue the edge (A), does any edge survive WALK-FORWARD (B), and does market REGIME conditioning generalize (C).

## PART A - Exit asymmetry / break-even sensitivity

`be_wr` = win-rate the bracket needs just to break even (= avgLoss/(avgWin+avgLoss), costs baked in). `margin` = actual WR - be_wr. A real edge shows a POSITIVE margin on the **base population** (no overfit filter), OOS.

### Base population (all vol2_h5 breakouts), TEST window, every bracket:

| exit | n | WR | avg_win | avg_loss | be_WR | margin | PF | net |
|---|--:|--:|--:|--:|--:|--:|--:|--:|
| sl0.75_tgt1_tb9 | 4463 | 21% | 545 | 677 | 55% | -34.5% | 0.21 | -1,881,007 |
| sl0.75_tgt0.75_tb9 | 4463 | 24% | 429 | 682 | 61% | -37.7% | 0.20 | -1,867,806 |
| sl0p75_tgt1_tb9_trail0p5_after0p75 | 4463 | 24% | 401 | 682 | 63% | -39.2% | 0.18 | -1,897,217 |
| sl0.75_tgt1_tb6 | 4463 | 18% | 498 | 630 | 56% | -37.6% | 0.18 | -1,892,435 |
| sl0.5_tgt0.75_tb9 | 4463 | 20% | 432 | 617 | 59% | -39.1% | 0.17 | -1,829,460 |
| sl0.75_tgt0.75_tb6 | 4463 | 20% | 408 | 634 | 61% | -40.4% | 0.17 | -1,877,746 |
| sl0.6_tgt0.6_tb9 | 4463 | 24% | 332 | 657 | 66% | -42.1% | 0.16 | -1,857,635 |
| sl0.5_tgt0.75_tb6 | 4463 | 18% | 411 | 589 | 59% | -41.3% | 0.15 | -1,842,542 |
| sl0.6_tgt0.6_tb6 | 4463 | 21% | 323 | 617 | 66% | -44.5% | 0.14 | -1,866,401 |
| sl0.75_tgt1_tb3 | 4463 | 14% | 445 | 553 | 55% | -40.9% | 0.14 | -1,824,640 |
| sl0.5_tgt0.5_tb9 | 4463 | 24% | 252 | 626 | 71% | -47.3% | 0.13 | -1,852,694 |
| sl0.75_tgt0.75_tb3 | 4463 | 16% | 376 | 554 | 60% | -44.0% | 0.13 | -1,824,879 |
| sl0.75_tgt0.75_tb6_be0p4 | 4463 | 15% | 402 | 587 | 59% | -44.2% | 0.12 | -1,949,887 |
| sl0.75_tgt1_tb6_be0p4 | 4463 | 13% | 481 | 576 | 54% | -41.9% | 0.12 | -1,975,188 |
| sl0.5_tgt0.75_tb3 | 4463 | 14% | 376 | 532 | 59% | -44.4% | 0.12 | -1,797,540 |
| sl0.6_tgt0.6_tb6_be0p4 | 4463 | 17% | 323 | 583 | 64% | -47.3% | 0.11 | -1,913,349 |
| sl0.5_tgt0.5_tb6 | 4463 | 21% | 249 | 597 | 71% | -49.3% | 0.11 | -1,862,009 |
| sl0.6_tgt0.6_tb3 | 4463 | 16% | 303 | 546 | 64% | -48.2% | 0.11 | -1,825,169 |
| sl0.5_tgt0.5_tb3 | 4463 | 17% | 236 | 537 | 69% | -52.7% | 0.09 | -1,820,698 |

**Best OOS bracket for the base population: `sl0.75_tgt1_tb9`** (PF 0.21, margin -34.5%). Read the margin column: if every bracket is negative, no exit redesign rescues this setup - the entry has no edge to harvest. If the best bracket flips positive, the exit was the bottleneck.

## PART B - Walk-forward (the honest judge)

Anchored expanding folds (min train 15 sessions, test 5, step 5); exit `sl0.75_tgt1_tb9`. A setup is ROBUST only if >=60% of folds test PF>=1.3 AND median test PF>=1.3.

### B1 - base setup, unconditional (no filter) per fold:

| fold | test window | train PF | test PF | test n | test net |
|--:|---|--:|--:|--:|--:|
| 0 | 2026-05-22..2026-05-29 | 0.24 | 0.22 | 1848 | -762,600 |
| 1 | 2026-06-01..2026-06-05 | 0.23 | 0.23 | 1361 | -543,446 |
| 2 | 2026-06-08..2026-06-12 | 0.23 | 0.27 | 2127 | -783,418 |
| 3 | 2026-06-15..2026-06-19 | 0.24 | 0.21 | 2327 | -987,014 |
| 4 | 2026-06-22..2026-06-29 | 0.23 | 0.21 | 2136 | -893,993 |

-> folds positive 0/5 (frac 0.0), median test PF 0.22 -> **DEAD**

### B2 - re-tuned filter pocket per fold (search TRAIN, score unseen TEST):

| fold | test window | train PF | train n | test PF | test n | pocket |
|--:|---|--:|--:|--:|--:|---|
| 0 | 2026-05-22..2026-05-29 | - | - | - | - | no_pocket_in_band |
| 1 | 2026-06-01..2026-06-05 | 1.6 | 27 | 0.18 | 6 | atr_pct 0.8-2.25, adx>=36, slot 8-36 . top_n=None |
| 2 | 2026-06-08..2026-06-12 | 1.32 | 31 | 0.28 | 12 | atr_pct 0.8-2.25, adx>=36, close_loc>=0.76 . top_n=None |
| 3 | 2026-06-15..2026-06-19 | 1.37 | 31 | 0.48 | 6 | atr_pct 0.8-2.25, adx>=36, close_loc>=0.84 . top_n=None |
| 4 | 2026-06-22..2026-06-29 | - | - | - | - | no_pocket_in_band |

-> folds positive 0/3 (frac 0.0), median test PF 0.28 -> **DEAD**

## PART C - Regime conditioning (causal NIFTY50)

Causal market features joined at the signal bar (coverage 100% of rows). Note: walk_forward.py bans contemporaneous market_ret as a known overfit vector, so regime is credited ONLY if it holds across folds - never on a single split.

### C1 - direct market gate on the base setup, single split:

| market gate | train PF | train n | test PF | test n |
|---|--:|--:|--:|--:|
| (none / base) | 0.24 | 7179 | 0.21 | 4463 |
| mkt_above_ema20 | 0.26 | 4477 | 0.21 | 2557 |
| mkt_trend_up (ema20>50 & above) | 0.28 | 3267 | 0.23 | 2073 |
| mkt_ret_open>=0 | 0.25 | 4589 | 0.23 | 2868 |
| mkt_mom_30m>=0 | 0.27 | 4269 | 0.20 | 2469 |

### C2 - walk-forward with regime IN the search universe:

| fold | test window | train PF | test PF | test n | used regime? | pocket |
|--:|---|--:|--:|--:|:--:|---|
| 0 | 2026-05-22..2026-05-29 | - | - | - | - | no_pocket |
| 1 | 2026-06-01..2026-06-05 | 1.4 | 0.0 | 3 | yes | atr_pct 0.8-2.25, mkt_ema_stack, slot 10-48 . top_n=None |
| 2 | 2026-06-08..2026-06-12 | 1.32 | 0.28 | 12 | no | atr_pct 0.8-2.25, adx>=36, close_loc>=0.76 . top_n=None |
| 3 | 2026-06-15..2026-06-19 | 1.37 | 0.48 | 6 | no | atr_pct 0.8-2.25, adx>=36, close_loc>=0.84 . top_n=None |
| 4 | 2026-06-22..2026-06-29 | - | - | - | - | no_pocket |

-> with regime available: folds positive 0/2 (frac 0.0), median test PF 0.38, regime chosen in 0/2 evaluated folds -> **INSUFFICIENT_DATA**

## Bottom line

- **Base setup, walk-forward:** DEAD (median test PF 0.22, 0/5 folds positive).
- **Re-tuned filter pocket, walk-forward:** DEAD (median test PF 0.28, 0/3 folds positive).
- **Regime conditioning, walk-forward:** INSUFFICIENT_DATA (median test PF 0.38, frac positive 0.0).

If all three are FRAGILE/DEAD, the conclusion is structural: this entry has no regime-robust long edge at this frequency in this window, and neither exit redesign, per-fold filtering, nor market-regime gating changes that. That is the honest answer to "why TEST isn't improving."
