# G3 V24 compression and breakout diagnostic

68 completed available sessions, 2026-07-29 through 2026-11-09. All 930 admitted opportunities and 824 filled baseline trades are retained. **This study labels existing trades; it does not change selection, entries, exits, capital or PnL.**

Labels use observations available by the original G3 confirmation. One-minute Bollinger BandWidth uses 20 consecutive same-session closes and two population standard deviations. Compression compares the preceding bar with the twentieth percentile of its preceding 100 valid BandWidth observations; that reference excludes the observation being classified and may contain prior sessions. The primary label requires preceding-bar compression, a completed current close beyond the preceding five-bar high/low in G3 direction, and increasing BandWidth. Exact earlier-breakout and range-anchor rules are in config.json. A full context needs 25 same-session bars. Earlier slots are explicitly not applicable, rather than being treated as failed or late breakouts. Unavailable evidence is separately labeled. Neither condition deletes a baseline trade. No thresholds were optimized.

Existing G3 volume confirmation is already satisfied by the frozen admitted inventory. It is held constant here; this study does not isolate an incremental benefit from volume or add another volume gate.

Modeled exposure remains Rs 500,000 per fill with Rs 250 round-trip cost and unlimited concurrent capital. The V23 targets, 1.50% initial stop, 80%-of-target completed-close breakeven protection, slot caps, G7 confirmations and 15:15 exit remain unchanged.

## Full history by context

| Context | Orders | Fills | Fill rate | Win rate | Net Rs | Loss amount Rs | Mean/fill Rs | Median/fill Rs | Mean/day Rs | Median/day Rs | Daily-close DD Rs |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| Unchanged baseline: all trades | 930 | 824 | 88.60% | 65.66% | 1,221,079.86 | 864,226.26 | 1,481.89 | 2,549.56 | 17,957.06 | 13,753.83 | 31,338.39 |
| Fresh compression breakout (small sample) | 12 | 12 | 100.00% | 66.67% | 15,103.34 | 7,148.54 | 1,258.61 | 1,434.63 | 222.11 | 0.00 | 3,461.93 |
| Earlier breakout continuation | 103 | 89 | 86.41% | 52.81% | 65,803.08 | 116,182.50 | 739.36 | 551.14 | 967.69 | 0.00 | 21,765.91 |
| Breakout without compression | 364 | 316 | 86.81% | 65.82% | 433,059.81 | 338,100.95 | 1,370.44 | 2,410.43 | 6,368.53 | 3,626.11 | 22,470.46 |
| Other ready context | 381 | 344 | 90.29% | 67.73% | 546,574.18 | 334,517.31 | 1,588.88 | 2,983.46 | 8,037.86 | 4,963.81 | 15,994.29 |
| Early session: not applicable | 70 | 63 | 90.00% | 71.43% | 160,539.44 | 68,276.96 | 2,548.25 | 3,750.00 | 2,360.87 | 0.00 | 11,891.06 |
| Unavailable evidence (small sample) | 0 | 0 | 0.00% | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |

Context rows partition the baseline; their order, trade, win/loss and net-PnL totals reconcile exactly. Loss amount sums negative after-cost trade PnL. Flat fills do not count as wins. Daily-close drawdown uses zero initial cumulative PnL and every available date; smaller context-group drawdown may merely reflect fewer trades and is not a strategy improvement. A small-sample label means fewer than 30 fills.

## Direction

### LONG

| Context | Orders | Fills | Fill rate | Win rate | Net Rs | Loss amount Rs | Mean/fill Rs | Median/fill Rs | Mean/day Rs | Median/day Rs | Daily-close DD Rs |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| Unchanged baseline: all trades | 408 | 373 | 91.42% | 65.95% | 628,891.75 | 378,118.69 | 1,686.04 | 3,081.33 | 9,248.41 | 6,109.41 | 20,311.71 |
| Fresh compression breakout (small sample) | 1 | 1 | 100.00% | 0.00% | -250.00 | 250.00 | -250.00 | -250.00 | -3.68 | 0.00 | 250.00 |
| Earlier breakout continuation (small sample) | 32 | 28 | 87.50% | 32.14% | -14,804.09 | 52,458.16 | -528.72 | -1,513.82 | -217.71 | 0.00 | 29,105.04 |
| Breakout without compression | 173 | 151 | 87.28% | 69.54% | 262,248.99 | 150,677.75 | 1,736.75 | 3,001.68 | 3,856.60 | 211.11 | 11,514.78 |
| Other ready context | 169 | 161 | 95.27% | 68.32% | 268,951.16 | 156,276.80 | 1,670.50 | 3,500.00 | 3,955.16 | 2,740.65 | 16,358.76 |
| Early session: not applicable | 33 | 32 | 96.97% | 68.75% | 112,745.69 | 18,455.99 | 3,523.30 | 3,750.00 | 1,658.02 | 0.00 | 7,750.00 |
| Unavailable evidence (small sample) | 0 | 0 | 0.00% | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |

### SHORT

| Context | Orders | Fills | Fill rate | Win rate | Net Rs | Loss amount Rs | Mean/fill Rs | Median/fill Rs | Mean/day Rs | Median/day Rs | Daily-close DD Rs |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| Unchanged baseline: all trades | 522 | 451 | 86.40% | 65.41% | 592,188.10 | 486,107.57 | 1,313.06 | 1,814.76 | 8,708.65 | 4,318.11 | 43,336.40 |
| Fresh compression breakout (small sample) | 11 | 11 | 100.00% | 72.73% | 15,353.34 | 6,898.54 | 1,395.76 | 2,427.22 | 225.78 | 0.00 | 3,211.93 |
| Earlier breakout continuation | 71 | 61 | 85.92% | 62.30% | 80,607.18 | 63,724.34 | 1,321.43 | 2,218.61 | 1,185.40 | 0.00 | 13,121.66 |
| Breakout without compression | 191 | 165 | 86.39% | 62.42% | 170,810.82 | 187,423.21 | 1,035.22 | 1,044.89 | 2,511.92 | 77.97 | 26,160.59 |
| Other ready context | 212 | 183 | 86.32% | 67.21% | 277,623.02 | 178,240.51 | 1,517.07 | 2,470.69 | 4,082.69 | 2,055.28 | 17,870.66 |
| Early session: not applicable | 37 | 31 | 83.78% | 74.19% | 47,793.74 | 49,820.97 | 1,541.73 | 2,172.58 | 702.85 | 0.00 | 14,669.22 |
| Unavailable evidence (small sample) | 0 | 0 | 0.00% | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |

## Chronological checks

### earlier_59: 2026-07-29 through 2026-10-27

| Context | Orders | Fills | Fill rate | Win rate | Net Rs | Loss amount Rs | Mean/fill Rs | Median/fill Rs | Mean/day Rs | Median/day Rs | Daily-close DD Rs |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| Unchanged baseline: all trades | 777 | 687 | 88.42% | 67.69% | 1,113,740.27 | 665,963.36 | 1,621.16 | 2,550.00 | 18,876.95 | 14,177.91 | 31,338.39 |
| Fresh compression breakout (small sample) | 12 | 12 | 100.00% | 66.67% | 15,103.34 | 7,148.54 | 1,258.61 | 1,434.63 | 255.99 | 0.00 | 3,461.93 |
| Earlier breakout continuation | 87 | 75 | 86.21% | 56.00% | 70,864.37 | 88,621.21 | 944.86 | 868.73 | 1,201.09 | 0.00 | 13,121.66 |
| Breakout without compression | 305 | 265 | 86.89% | 67.17% | 400,283.07 | 250,872.39 | 1,510.50 | 2,256.02 | 6,784.46 | 3,502.23 | 21,663.41 |
| Other ready context | 311 | 279 | 89.71% | 70.25% | 483,622.57 | 258,794.26 | 1,733.41 | 3,150.00 | 8,196.99 | 5,244.98 | 15,994.29 |
| Early session: not applicable | 62 | 56 | 90.32% | 73.21% | 143,866.92 | 60,526.96 | 2,569.05 | 3,750.00 | 2,438.42 | 0.00 | 11,891.06 |
| Unavailable evidence (small sample) | 0 | 0 | 0.00% | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |

### later_9: 2026-10-28 through 2026-11-09

| Context | Orders | Fills | Fill rate | Win rate | Net Rs | Loss amount Rs | Mean/fill Rs | Median/fill Rs | Mean/day Rs | Median/day Rs | Daily-close DD Rs |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| Unchanged baseline: all trades | 153 | 137 | 89.54% | 55.47% | 107,339.58 | 198,262.90 | 783.50 | 2,297.68 | 11,926.62 | 5,227.96 | 26,246.72 |
| Fresh compression breakout (small sample) | 0 | 0 | 0.00% | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
| Earlier breakout continuation (small sample) | 16 | 14 | 87.50% | 35.71% | -5,061.29 | 27,561.29 | -361.52 | -1,621.87 | -562.37 | -1,228.58 | 11,691.76 |
| Breakout without compression | 59 | 51 | 86.44% | 58.82% | 32,776.74 | 87,228.56 | 642.68 | 2,550.00 | 3,641.86 | 4,895.12 | 16,236.71 |
| Other ready context | 70 | 65 | 92.86% | 56.92% | 62,951.62 | 75,723.05 | 968.49 | 1,797.38 | 6,994.62 | 4,224.72 | 15,149.20 |
| Early session: not applicable (small sample) | 8 | 7 | 87.50% | 57.14% | 16,672.52 | 7,750.00 | 2,381.79 | 2,692.33 | 1,852.50 | 0.00 | 7,750.00 |
| Unavailable evidence (small sample) | 0 | 0 | 0.00% | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |

## Comparisons on shared context

The primary group is **Fresh compression breakout**. Its comparison group contains all other ready labels. Early-session not-applicable and unavailable evidence are both excluded from these comparisons only; their trades remain in baseline totals.

### full_68

Primary fills: 12; other available fills: 749. Days containing both: 11.

Unadjusted mean PnL per fill: primary Rs 1,258.61, other ready Rs 1,395.78. Win rates: 66.67% and 65.15% respectively.

Equal-day average difference in mean PnL per fill: **Rs -192.87**. Equal-day win-rate difference: +3.66 percentage points.

Common setup/side strata: 10, covering 12 primary and 180 comparison fills. Standardized mean-PnL difference: **Rs -535.54**; win-rate difference: -6.30 percentage points.

**Small sample / limited overlap:** fewer than 30 primary fills or fewer than 10 paired days; treat the differences as descriptive.

### earlier_59

Primary fills: 12; other available fills: 619. Days containing both: 11.

Unadjusted mean PnL per fill: primary Rs 1,258.61, other ready Rs 1,542.44. Win rates: 66.67% and 67.21% respectively.

Equal-day average difference in mean PnL per fill: **Rs -192.87**. Equal-day win-rate difference: +3.66 percentage points.

Common setup/side strata: 10, covering 12 primary and 158 comparison fills. Standardized mean-PnL difference: **Rs -636.33**; win-rate difference: -8.52 percentage points.

**Small sample / limited overlap:** fewer than 30 primary fills or fewer than 10 paired days; treat the differences as descriptive.

### later_9

Primary fills: 0; other available fills: 130. Days containing both: 0.

Unadjusted mean PnL per fill: primary Rs not estimable (no common support), other ready Rs 697.44. Win rates: not estimable (no common support)% and 55.38% respectively.

Equal-day average difference in mean PnL per fill: **Rs not estimable (no common support)**. Equal-day win-rate difference: not estimable (no common support) percentage points.

Common setup/side strata: 0, covering 0 primary and 0 comparison fills. Standardized mean-PnL difference: **Rs not estimable (no common support)**; win-rate difference: not estimable (no common support) percentage points.

**Small sample / limited overlap:** fewer than 30 primary fills or fewer than 10 paired days; treat the differences as descriptive.

Both groups receive identical pooled-fill weights within each common setup/side stratum. Weights depend on counts, not outcomes. Day pairing and setup/side standardization are separate checks; they do not jointly remove all day, stock, side, slot or market-regime confounding. No causal benefit or statistical significance is claimed.

## If the primary label alone were required

**This was not applied.** The descriptive subset would retain 12 fills (1.46%) and Rs 15,103.34 net PnL. It would omit 533 winning and 250 losing baseline fills, forgoing Rs 2,063,054.23 winning PnL while avoiding Rs 857,077.72 losing PnL. This illustrates the cost of a mandatory filter; it is not a new strategy result or a recommendation.

## Interpretation and scope

The unchanged baseline remains 824 fills and Rs 1,221,079.86 net PnL. Any difference between label groups is an association within that fixed book. Check direction, periods, common-support comparisons and sample sizes before treating the label as useful entry information.

All observations are retrospective and the source history has been used in earlier research. The earlier/later slices are sensitivity checks, not untouched out-of-sample evidence. No label is promoted to a live filter or ranking change.

Source limitations remain inherited: October 1 is excluded, the November 8 special session uses a different schedule, October 28 has partial expanded-slot stock coverage, and older archived universes are not all strictly point-in-time. Existing versions, source artifacts and live/paper settings remain unchanged.

[Day-wise results](DAY_WISE_RESULTS.md) | [Every labeled trade](labeled_trades.csv) | [All admitted orders](labeled_orders.csv) | [Matched-context comparisons](comparison_summary.csv)
