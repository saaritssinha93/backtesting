# G3 V24 compression and breakout diagnostic

1 completed available sessions, 2026-07-29 through 2026-07-29. All 17 admitted opportunities and 13 filled baseline trades are retained. **This study labels existing trades; it does not change selection, entries, exits, capital or PnL.**

Labels use observations available by the original G3 confirmation. One-minute Bollinger BandWidth uses 20 consecutive same-session closes and two population standard deviations. Compression compares the preceding bar with the twentieth percentile of its preceding 100 valid BandWidth observations; that reference excludes the observation being classified and may contain prior sessions. The primary label requires preceding-bar compression, a completed current close beyond the preceding five-bar high/low in G3 direction, and increasing BandWidth. Exact earlier-breakout and range-anchor rules are in config.json. A full context needs 25 same-session bars. Earlier slots are explicitly not applicable, rather than being treated as failed or late breakouts. Unavailable evidence is separately labeled. Neither condition deletes a baseline trade. No thresholds were optimized.

Existing G3 volume confirmation is already satisfied by the frozen admitted inventory. It is held constant here; this study does not isolate an incremental benefit from volume or add another volume gate.

Modeled exposure remains Rs 500,000 per fill with Rs 250 round-trip cost and unlimited concurrent capital. The V23 targets, 1.50% initial stop, 80%-of-target completed-close breakeven protection, slot caps, G7 confirmations and 15:15 exit remain unchanged.

## Full history by context

| Context | Orders | Fills | Fill rate | Win rate | Net Rs | Loss amount Rs | Mean/fill Rs | Median/fill Rs | Mean/day Rs | Median/day Rs | Daily-close DD Rs |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| Unchanged baseline: all trades (small sample) | 17 | 13 | 76.47% | 84.62% | 78,110.72 | 8,233.90 | 6,008.52 | 3,750.00 | 78,110.72 | 78,110.72 | 0.00 |
| Fresh compression breakout (small sample) | 0 | 0 | 0.00% | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
| Earlier breakout continuation (small sample) | 0 | 0 | 0.00% | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
| Breakout without compression (small sample) | 8 | 5 | 62.50% | 80.00% | 48,443.73 | 483.90 | 9,688.75 | 3,750.00 | 48,443.73 | 48,443.73 | 0.00 |
| Other ready context (small sample) | 9 | 8 | 88.89% | 87.50% | 29,666.99 | 7,750.00 | 3,708.37 | 3,486.92 | 29,666.99 | 29,666.99 | 0.00 |
| Early session: not applicable (small sample) | 0 | 0 | 0.00% | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
| Unavailable evidence (small sample) | 0 | 0 | 0.00% | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |

Context rows partition the baseline; their order, trade, win/loss and net-PnL totals reconcile exactly. Loss amount sums negative after-cost trade PnL. Flat fills do not count as wins. Daily-close drawdown uses zero initial cumulative PnL and every available date; smaller context-group drawdown may merely reflect fewer trades and is not a strategy improvement. A small-sample label means fewer than 30 fills.

## Direction

### LONG

| Context | Orders | Fills | Fill rate | Win rate | Net Rs | Loss amount Rs | Mean/fill Rs | Median/fill Rs | Mean/day Rs | Median/day Rs | Daily-close DD Rs |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| Unchanged baseline: all trades (small sample) | 13 | 10 | 76.92% | 80.00% | 61,317.57 | 8,233.90 | 6,131.76 | 3,486.92 | 61,317.57 | 61,317.57 | 0.00 |
| Fresh compression breakout (small sample) | 0 | 0 | 0.00% | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
| Earlier breakout continuation (small sample) | 0 | 0 | 0.00% | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
| Breakout without compression (small sample) | 7 | 4 | 57.14% | 75.00% | 40,393.73 | 483.90 | 10,098.43 | 2,688.82 | 40,393.73 | 40,393.73 | 0.00 |
| Other ready context (small sample) | 6 | 6 | 100.00% | 83.33% | 20,923.84 | 7,750.00 | 3,487.31 | 3,486.92 | 20,923.84 | 20,923.84 | 0.00 |
| Early session: not applicable (small sample) | 0 | 0 | 0.00% | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
| Unavailable evidence (small sample) | 0 | 0 | 0.00% | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |

### SHORT

| Context | Orders | Fills | Fill rate | Win rate | Net Rs | Loss amount Rs | Mean/fill Rs | Median/fill Rs | Mean/day Rs | Median/day Rs | Daily-close DD Rs |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| Unchanged baseline: all trades (small sample) | 4 | 3 | 75.00% | 100.00% | 16,793.15 | -0.00 | 5,597.72 | 7,400.00 | 16,793.15 | 16,793.15 | 0.00 |
| Fresh compression breakout (small sample) | 0 | 0 | 0.00% | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
| Earlier breakout continuation (small sample) | 0 | 0 | 0.00% | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
| Breakout without compression (small sample) | 1 | 1 | 100.00% | 100.00% | 8,050.00 | -0.00 | 8,050.00 | 8,050.00 | 8,050.00 | 8,050.00 | 0.00 |
| Other ready context (small sample) | 3 | 2 | 66.67% | 100.00% | 8,743.15 | -0.00 | 4,371.58 | 4,371.58 | 8,743.15 | 8,743.15 | 0.00 |
| Early session: not applicable (small sample) | 0 | 0 | 0.00% | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
| Unavailable evidence (small sample) | 0 | 0 | 0.00% | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |

## Chronological checks

### earlier_1: 2026-07-29 through 2026-07-29

| Context | Orders | Fills | Fill rate | Win rate | Net Rs | Loss amount Rs | Mean/fill Rs | Median/fill Rs | Mean/day Rs | Median/day Rs | Daily-close DD Rs |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| Unchanged baseline: all trades (small sample) | 17 | 13 | 76.47% | 84.62% | 78,110.72 | 8,233.90 | 6,008.52 | 3,750.00 | 78,110.72 | 78,110.72 | 0.00 |
| Fresh compression breakout (small sample) | 0 | 0 | 0.00% | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
| Earlier breakout continuation (small sample) | 0 | 0 | 0.00% | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
| Breakout without compression (small sample) | 8 | 5 | 62.50% | 80.00% | 48,443.73 | 483.90 | 9,688.75 | 3,750.00 | 48,443.73 | 48,443.73 | 0.00 |
| Other ready context (small sample) | 9 | 8 | 88.89% | 87.50% | 29,666.99 | 7,750.00 | 3,708.37 | 3,486.92 | 29,666.99 | 29,666.99 | 0.00 |
| Early session: not applicable (small sample) | 0 | 0 | 0.00% | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
| Unavailable evidence (small sample) | 0 | 0 | 0.00% | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |

### later_0: no sessions

| Context | Orders | Fills | Fill rate | Win rate | Net Rs | Loss amount Rs | Mean/fill Rs | Median/fill Rs | Mean/day Rs | Median/day Rs | Daily-close DD Rs |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| Unchanged baseline: all trades (small sample) | 0 | 0 | 0.00% | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
| Fresh compression breakout (small sample) | 0 | 0 | 0.00% | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
| Earlier breakout continuation (small sample) | 0 | 0 | 0.00% | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
| Breakout without compression (small sample) | 0 | 0 | 0.00% | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
| Other ready context (small sample) | 0 | 0 | 0.00% | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
| Early session: not applicable (small sample) | 0 | 0 | 0.00% | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
| Unavailable evidence (small sample) | 0 | 0 | 0.00% | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |

## Comparisons on shared context

The primary group is **Fresh compression breakout**. Its comparison group contains all other ready labels. Early-session not-applicable and unavailable evidence are both excluded from these comparisons only; their trades remain in baseline totals.

### full_1

Primary fills: 0; other available fills: 13. Days containing both: 0.

Unadjusted mean PnL per fill: primary Rs not estimable (no common support), other ready Rs 6,008.52. Win rates: not estimable (no common support)% and 84.62% respectively.

Equal-day average difference in mean PnL per fill: **Rs not estimable (no common support)**. Equal-day win-rate difference: not estimable (no common support) percentage points.

Common setup/side strata: 0, covering 0 primary and 0 comparison fills. Standardized mean-PnL difference: **Rs not estimable (no common support)**; win-rate difference: not estimable (no common support) percentage points.

**Small sample / limited overlap:** fewer than 30 primary fills or fewer than 10 paired days; treat the differences as descriptive.

### earlier_1

Primary fills: 0; other available fills: 13. Days containing both: 0.

Unadjusted mean PnL per fill: primary Rs not estimable (no common support), other ready Rs 6,008.52. Win rates: not estimable (no common support)% and 84.62% respectively.

Equal-day average difference in mean PnL per fill: **Rs not estimable (no common support)**. Equal-day win-rate difference: not estimable (no common support) percentage points.

Common setup/side strata: 0, covering 0 primary and 0 comparison fills. Standardized mean-PnL difference: **Rs not estimable (no common support)**; win-rate difference: not estimable (no common support) percentage points.

**Small sample / limited overlap:** fewer than 30 primary fills or fewer than 10 paired days; treat the differences as descriptive.

### later_0

Primary fills: 0; other available fills: 0. Days containing both: 0.

Unadjusted mean PnL per fill: primary Rs not estimable (no common support), other ready Rs not estimable (no common support). Win rates: not estimable (no common support)% and not estimable (no common support)% respectively.

Equal-day average difference in mean PnL per fill: **Rs not estimable (no common support)**. Equal-day win-rate difference: not estimable (no common support) percentage points.

Common setup/side strata: 0, covering 0 primary and 0 comparison fills. Standardized mean-PnL difference: **Rs not estimable (no common support)**; win-rate difference: not estimable (no common support) percentage points.

**Small sample / limited overlap:** fewer than 30 primary fills or fewer than 10 paired days; treat the differences as descriptive.

Both groups receive identical pooled-fill weights within each common setup/side stratum. Weights depend on counts, not outcomes. Day pairing and setup/side standardization are separate checks; they do not jointly remove all day, stock, side, slot or market-regime confounding. No causal benefit or statistical significance is claimed.

## If the primary label alone were required

**This was not applied.** The descriptive subset would retain 0 fills (0.00%) and Rs 0.00 net PnL. It would omit 11 winning and 2 losing baseline fills, forgoing Rs 86,344.62 winning PnL while avoiding Rs 8,233.90 losing PnL. This illustrates the cost of a mandatory filter; it is not a new strategy result or a recommendation.

## Interpretation and scope

The unchanged baseline remains 13 fills and Rs 78,110.72 net PnL. Any difference between label groups is an association within that fixed book. Check direction, periods, common-support comparisons and sample sizes before treating the label as useful entry information.

All observations are retrospective and the source history has been used in earlier research. The earlier/later slices are sensitivity checks, not untouched out-of-sample evidence. No label is promoted to a live filter or ranking change.

Source limitations remain inherited: October 1 is excluded, the November 8 special session uses a different schedule, October 28 has partial expanded-slot stock coverage, and older archived universes are not all strictly point-in-time. Existing versions, source artifacts and live/paper settings remain unchanged.

[Day-wise results](DAY_WISE_RESULTS.md) | [Every labeled trade](labeled_trades.csv) | [All admitted orders](labeled_orders.csv) | [Matched-context comparisons](comparison_summary.csv)
