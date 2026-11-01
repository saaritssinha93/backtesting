# Confirmation rules: second research round

Frozen evidence: 59 sessions from 2026-07-29 through 2026-10-27. October 28 is excluded to preserve the accepted baseline comparison; this is not a statement about its current collection status.

Eight additional confirmation rules produced 160 new comparisons, following 220 previous comparisons (380 cumulative). 2 rule rows passed the fixed development gates; 2 choices were fixed from development. 0 choices passed the later screening gates.

All main files remain at the accepted baseline. Development-only selections and attractive retrospective rows are disclosed as diagnostics. Every later block has already been inspected, so this is not independent validation.

Rules: confirmation volume versus previous 10 minutes at 1.0/1.5; session EMA5/13 alignment and EMA5 slope; directional VWAP distance in 0..0.5%/1.0%; 10-minute directional efficiency >=0.3; previous 3-minute breakout; directional candle close location >=0.8. All features use completed bars at confirmation and apply before ranking/quota.

The duplicate 09:31 SHORT request is evaluated once. Additional filters tighten the original eligible candidate pool; lowering the original eligibility thresholds was not tested. The experiment is therefore not an exhaustive search of all possible confirmations.

Development and later screening each require at least 8 executed trades, win rate strictly above 75%, setup and full-portfolio net at least baseline, and strictly reduced losing amount. Eight trades is not statistical proof. Wilson bounds remain descriptive under repeated selection.

Development dates: 2026-07-29 to 2026-09-18 (35 sessions). Later dates: 2026-09-21 to 2026-10-27 (24 sessions). Rules were fixed before this round's outcome calculations, and development choices were not replaced after inspecting later results.

All rupee amounts below are after the modeled 5 bps round-trip charge. Stress replays at 9 and 15 bps are included. The nominal sizing is Rs 100,000 capital at 5x leverage per entry; G7 uses integer quantity. These are modeled fills and costs, not live realized returns.

| Slot | Strategy | Baseline W/L, rate | Development choice | Dev rate | Later rate | All candidate rate | Decision |
|---|---|---:|---|---:|---:|---:|---|
| 0931_LONG | G3 | 2/2, 50.00% | baseline | 50.00% | n/a | 50.00% | NO_DEVELOPMENT_QUALIFIER |
| 0931_LONG | G7_V4_LOCK2 | 2/2, 50.00% | baseline | 50.00% | n/a | 50.00% | NO_DEVELOPMENT_QUALIFIER |
| 0931_SHORT | G3 | 10/4, 71.43% | baseline | 87.50% | 50.00% | 71.43% | NO_DEVELOPMENT_QUALIFIER |
| 0931_SHORT | G7_V4_LOCK2 | 7/3, 70.00% | baseline | 66.67% | 75.00% | 70.00% | NO_DEVELOPMENT_QUALIFIER |
| 1001_LONG | G3 | 5/4, 55.56% | baseline | 57.14% | 50.00% | 55.56% | NO_DEVELOPMENT_QUALIFIER |
| 1001_LONG | G7_V4_LOCK2 | 4/2, 66.67% | baseline | 80.00% | 0.00% | 66.67% | NO_DEVELOPMENT_QUALIFIER |
| 1051_LONG | G3 | 15/6, 71.43% | r2_close_location_ge_0p8 | 78.57% | 50.00% | 75.00% | REJECTED_LATER |
| 1051_LONG | G7_V4_LOCK2 | 11/4, 73.33% | r2_close_location_ge_0p8 | 88.89% | 50.00% | 81.82% | REJECTED_LATER |
| 1111_SHORT | G3 | 6/3, 66.67% | baseline | 83.33% | 33.33% | 66.67% | NO_DEVELOPMENT_QUALIFIER |
| 1111_SHORT | G7_V4_LOCK2 | 4/2, 66.67% | baseline | 75.00% | 50.00% | 66.67% | NO_DEVELOPMENT_QUALIFIER |
| 1146_SHORT | G3 | 13/6, 68.42% | baseline | 70.00% | 66.67% | 68.42% | NO_DEVELOPMENT_QUALIFIER |
| 1146_SHORT | G7_V4_LOCK2 | 9/5, 64.29% | baseline | 71.43% | 57.14% | 64.29% | NO_DEVELOPMENT_QUALIFIER |
| 1226_SHORT | G3 | 7/4, 63.64% | baseline | 100.00% | 33.33% | 63.64% | NO_DEVELOPMENT_QUALIFIER |
| 1226_SHORT | G7_V4_LOCK2 | 5/2, 71.43% | baseline | 100.00% | 33.33% | 71.43% | NO_DEVELOPMENT_QUALIFIER |
| 1231_SHORT | G3 | 9/4, 69.23% | baseline | 85.71% | 50.00% | 69.23% | NO_DEVELOPMENT_QUALIFIER |
| 1231_SHORT | G7_V4_LOCK2 | 6/3, 66.67% | baseline | 100.00% | 50.00% | 66.67% | NO_DEVELOPMENT_QUALIFIER |
| 1301_SHORT | G3 | 18/7, 72.00% | baseline | 66.67% | 76.92% | 72.00% | NO_DEVELOPMENT_QUALIFIER |
| 1301_SHORT | G7_V4_LOCK2 | 10/4, 71.43% | baseline | 62.50% | 83.33% | 71.43% | NO_DEVELOPMENT_QUALIFIER |
| 1321_SHORT | G3 | 14/8, 63.64% | baseline | 90.00% | 41.67% | 63.64% | NO_DEVELOPMENT_QUALIFIER |
| 1321_SHORT | G7_V4_LOCK2 | 10/6, 62.50% | baseline | 83.33% | 50.00% | 62.50% | NO_DEVELOPMENT_QUALIFIER |

Only 10:51 LONG's top-20%-of-candle close passed development in both strategies. G3: 11 wins/14 trades (78.57%) in development, then 1/2 (50%) later, with later net Rs 4,300 below baseline. G7: 8/9 (88.89%), then 1/2 (50%), with later net Rs 904.44 below baseline. Both are rejected. G7's attractive all-history 9/11 (81.82%) adds only Rs 20.88 overall and has a 95% Wilson lower bound of 52.30%; it does not establish a reliable >75% win rate.

The late-slot retrospective exception is G7 13:01 SHORT with close location >=0.8: 8/10 wins overall and Rs 2,992.08 extra net. Its development result is only 4/6 wins (66.67%) and later 4/4 is too small; it was not selected. Reporting it as a validated improvement would conceal those failures.

| Scope | Period | Strategy | Trades | Wins | Losses | Win rate | Net Rs |
|---|---|---|---:|---:|---:|---:|---:|
| REQUESTED_10_SLOTS | all | G3 | 147 | 99 | 48 | 67.35% | 262,407.00 |
| REQUESTED_10_SLOTS | october | G3 | 45 | 20 | 25 | 44.44% | -32,469.98 |
| FULL_PORTFOLIO | all | G3 | 546 | 394 | 152 | 72.16% | 1,224,672.05 |
| FULL_PORTFOLIO | october | G3 | 152 | 87 | 65 | 57.24% | 114,547.91 |
| REQUESTED_10_SLOTS | all | G7_V4_LOCK2 | 101 | 68 | 33 | 67.33% | 124,431.65 |
| REQUESTED_10_SLOTS | october | G7_V4_LOCK2 | 31 | 14 | 17 | 45.16% | -24,652.64 |
| FULL_PORTFOLIO | all | G7_V4_LOCK2 | 397 | 320 | 77 | 80.60% | 859,638.79 |
| FULL_PORTFOLIO | october | G7_V4_LOCK2 | 110 | 78 | 32 | 70.91% | 131,028.36 |

Loss diagnosis from the accepted requested-slot trades: G3 had 48 losses, including 28 time exits; G7 had 33 losses, including 20 time exits. Median losing holds were 132 and 133 minutes. Holding time does not establish when a reversal began. None of G7's 33 losses armed its completed-close +0.6% protection. The protection cannot rescue trades that never reach its arming condition.

Requested-slot October win rates fell to 20/45 (44.44%) for G3 and 14/31 (45.16%) for G7, versus 79/102 (77.45%) and 54/70 (77.14%) earlier. This is a deterioration in this sample, not proof of a specific market cause. The detailed diagnosis also records the intrabar limitations of maximum-favorable-excursion measurements.

| Available October date | Requested slots G3 net Rs | Requested slots G7 net Rs |
|---|---:|---:|
| 2026-10-05 | 4,150.00 | 4,137.54 |
| 2026-10-06 | -14,601.79 | -4,770.11 |
| 2026-10-07 | -2,499.18 | -2,580.23 |
| 2026-10-08 | 6,405.06 | 7,184.40 |
| 2026-10-09 | 4,237.05 | 3,560.04 |
| 2026-10-12 | 10,505.53 | 10,493.42 |
| 2026-10-13 | -4,551.49 | -8,445.35 |
| 2026-10-14 | 5,938.98 | 401.28 |
| 2026-10-15 | -16,670.29 | -11,468.68 |
| 2026-10-16 | -13,027.94 | -6,276.94 |
| 2026-10-19 | -268.56 | -2,524.73 |
| 2026-10-21 | -802.67 | 4,415.77 |
| 2026-10-22 | 3,583.61 | 0.00 |
| 2026-10-23 | -6,538.99 | -6,281.36 |
| 2026-10-26 | -1,829.31 | -6,249.51 |
| 2026-10-27 | -6,500.00 | -6,248.18 |

Daywise tables contain the 16 available October sessions (October 5-27); dates absent from the source are not manufactured as zero-result sessions. All accepted result tables remain baseline because no change survived. Development-selected daywise rows are separate rejected diagnostics.

All 259 candidate features were reconstructed using bars at or before confirmation. Exact minute coverage, source flags, candle matching and future-bar invariance checks passed. Sources include immutable snapshots and historical parquet files; timestamp cutoff proves calculation causality, not the original real-time arrival or revision status of every bar.

Validation: 26 exact baseline replay checks, 140 accepted-ledger period reconciliations, and 243 batch member hashes passed. October has 320 slot/strategy/day rows including zero-trade sessions.

See slot_comparison.csv, development_gates.csv, all_variant_results.csv, promotion_decisions.csv, baseline_cost_stress.csv, and October daywise files. If any choice is marked REQUIRES_COMBINED_REPLAY it is not deployable and its isolated improvements must not be summed.
