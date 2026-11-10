# G3 V24 RSI entry-timing experiment

1 available completed sessions from 2026-07-29 through 2026-07-29. All variants start with the exact same G3/G7-admitted stock and slot inventory. Entry timing alone varies.

The RSI hypothesis waits for a price pause with RSI in 45–55, then a fresh completed-candle cross above 55 for LONG or below 45 for SHORT. A later candle must break the cross candle's directional extreme within the original entry expiry. Exact state/invalidation and any control-arm rules are recorded in config.json. These starting thresholds were fixed before the outcome comparison; they are not optimized.

The baseline retains its existing targets, 1.50% initial stop and 80%-of-target completed-close breakeven rule. The same exit policy is rebased to each variant's actual entry price. Modeled exposure is Rs 500,000 and round-trip cost Rs 250 per fill with unlimited concurrent capital. Original and added slot caps remain fixed. Stops use the inherited conservative ambiguous-bar and adverse-gap rules; square-off remains 15:15.

## Full matched history

| Policy | Orders | Fills | Wins | Losses | Flat | Win rate | Net PnL Rs | Loss amount Rs | Mean/fill Rs | Median/fill Rs | Mean/day Rs | Median/day Rs | Daily-close DD Rs |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| V23 unchanged entry timing | 17 | 13 | 11 | 2 | 0 | 84.62% | 78,110.72 | 8,233.90 | 6,008.52 | 3,750.00 | 78,110.72 | 78,110.72 | 0.00 |
| RSI reset and recovery | 17 | 2 | 1 | 1 | 0 | 50.00% | 1,216.96 | 1,356.60 | 608.48 | 608.48 | 1,216.96 | 1,216.96 | 0.00 |
| RSI recovery with original trigger retained | 17 | 0 | 0 | 0 | 0 | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |

Loss amount is the absolute sum of negative after-cost trade PnL; flat trades do not count as wins. Daily-close drawdown starts from zero cumulative PnL and includes available zero-trade sessions. It does not measure intraday drawdown.

## Direction

### LONG

| Policy | Orders | Fills | Wins | Losses | Flat | Win rate | Net PnL Rs | Loss amount Rs | Mean/fill Rs | Median/fill Rs | Mean/day Rs | Median/day Rs | Daily-close DD Rs |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| V23 unchanged entry timing | 13 | 10 | 8 | 2 | 0 | 80.00% | 61,317.57 | 8,233.90 | 6,131.76 | 3,486.92 | 61,317.57 | 61,317.57 | 0.00 |
| RSI reset and recovery | 13 | 0 | 0 | 0 | 0 | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
| RSI recovery with original trigger retained | 13 | 0 | 0 | 0 | 0 | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |

### SHORT

| Policy | Orders | Fills | Wins | Losses | Flat | Win rate | Net PnL Rs | Loss amount Rs | Mean/fill Rs | Median/fill Rs | Mean/day Rs | Median/day Rs | Daily-close DD Rs |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| V23 unchanged entry timing | 4 | 3 | 3 | 0 | 0 | 100.00% | 16,793.15 | -0.00 | 5,597.72 | 7,400.00 | 16,793.15 | 16,793.15 | 0.00 |
| RSI reset and recovery | 4 | 2 | 1 | 1 | 0 | 50.00% | 1,216.96 | 1,356.60 | 608.48 | 608.48 | 1,216.96 | 1,216.96 | 0.00 |
| RSI recovery with original trigger retained | 4 | 0 | 0 | 0 | 0 | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |

## Chronological comparisons

### earlier_1: 2026-07-29 through 2026-07-29

| Policy | Orders | Fills | Wins | Losses | Flat | Win rate | Net PnL Rs | Loss amount Rs | Mean/fill Rs | Median/fill Rs | Mean/day Rs | Median/day Rs | Daily-close DD Rs |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| V23 unchanged entry timing | 17 | 13 | 11 | 2 | 0 | 84.62% | 78,110.72 | 8,233.90 | 6,008.52 | 3,750.00 | 78,110.72 | 78,110.72 | 0.00 |
| RSI reset and recovery | 17 | 2 | 1 | 1 | 0 | 50.00% | 1,216.96 | 1,356.60 | 608.48 | 608.48 | 1,216.96 | 1,216.96 | 0.00 |
| RSI recovery with original trigger retained | 17 | 0 | 0 | 0 | 0 | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |

### later_0: no available sessions

| Policy | Orders | Fills | Wins | Losses | Flat | Win rate | Net PnL Rs | Loss amount Rs | Mean/fill Rs | Median/fill Rs | Mean/day Rs | Median/day Rs | Daily-close DD Rs |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| V23 unchanged entry timing | 0 | 0 | 0 | 0 | 0 | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
| RSI reset and recovery | 0 | 0 | 0 | 0 | 0 | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
| RSI recovery with original trigger retained | 0 | 0 | 0 | 0 | 0 | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |

### inherited_1: 2026-07-29 through 2026-07-29

| Policy | Orders | Fills | Wins | Losses | Flat | Win rate | Net PnL Rs | Loss amount Rs | Mean/fill Rs | Median/fill Rs | Mean/day Rs | Median/day Rs | Daily-close DD Rs |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| V23 unchanged entry timing | 17 | 13 | 11 | 2 | 0 | 84.62% | 78,110.72 | 8,233.90 | 6,008.52 | 3,750.00 | 78,110.72 | 78,110.72 | 0.00 |
| RSI reset and recovery | 17 | 2 | 1 | 1 | 0 | 50.00% | 1,216.96 | 1,356.60 | 608.48 | 608.48 | 1,216.96 | 1,216.96 | 0.00 |
| RSI recovery with original trigger retained | 17 | 0 | 0 | 0 | 0 | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |

### new_0: no available sessions

| Policy | Orders | Fills | Wins | Losses | Flat | Win rate | Net PnL Rs | Loss amount Rs | Mean/fill Rs | Median/fill Rs | Mean/day Rs | Median/day Rs | Daily-close DD Rs |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| V23 unchanged entry timing | 0 | 0 | 0 | 0 | 0 | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
| RSI reset and recovery | 0 | 0 | 0 | 0 | 0 | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
| RSI recovery with original trigger retained | 0 | 0 | 0 | 0 | 0 | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |

### 2026-11-06: no available sessions

| Policy | Orders | Fills | Wins | Losses | Flat | Win rate | Net PnL Rs | Loss amount Rs | Mean/fill Rs | Median/fill Rs | Mean/day Rs | Median/day Rs | Daily-close DD Rs |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| V23 unchanged entry timing | 0 | 0 | 0 | 0 | 0 | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
| RSI reset and recovery | 0 | 0 | 0 | 0 | 0 | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
| RSI recovery with original trigger retained | 0 | 0 | 0 | 0 | 0 | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |

### 2026-11-09: no available sessions

| Policy | Orders | Fills | Wins | Losses | Flat | Win rate | Net PnL Rs | Loss amount Rs | Mean/fill Rs | Median/fill Rs | Mean/day Rs | Median/day Rs | Daily-close DD Rs |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| V23 unchanged entry timing | 0 | 0 | 0 | 0 | 0 | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
| RSI reset and recovery | 0 | 0 | 0 | 0 | 0 | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |
| RSI recovery with original trigger retained | 0 | 0 | 0 | 0 | 0 | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |

## What changed

| Policy | Unchanged fills | Retimed fills | Missed winners | Missed losers | New fills | Profit missed Rs | Loss avoided Rs | Retiming PnL change Rs | New-fill net Rs | Total PnL change Rs |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| RSI reset and recovery | 0 | 1 | 10 | 2 | 1 | 85,001.47 | 8,233.90 | +1,230.41 | -1,356.60 | -76,893.77 |
| RSI recovery with original trigger retained | 0 | 0 | 11 | 2 | 0 | 86,344.62 | 8,233.90 | +0.00 | +0.00 | -78,110.72 |

A missed baseline fill contributes the negative of its original PnL to the comparison. A new fill contributes its entire net PnL. A retimed fill keeps the same candidate SID and contributes candidate PnL minus baseline PnL. These three effects reconcile exactly to the total change. Fewer losing fills alone is not evidence that the entry timing improved: profitable fills can also be missed.

## Interpretation

- **RSI reset and recovery**: net PnL change Rs -76,893.77; loss-amount change Rs -6,877.30; fill-count change -11; win-rate change -34.62 percentage points; daily-close drawdown change Rs +0.00.
- **RSI recovery with original trigger retained**: net PnL change Rs -78,110.72; loss-amount change Rs -8,233.90; fill-count change -13; win-rate change -84.62 percentage points; daily-close drawdown change Rs +0.00.

All observations are retrospective. The earlier/later slices are sensitivity checks, not untouched out-of-sample evidence. This study does not promote a live policy. Delaying entries can miss uninterrupted breakouts; a higher win rate or smaller loss total can occur simply because fewer trades fill.

Coverage remains inherited from the V24 reference: October 1 is excluded for incomplete source data and the November 8 special session uses a different schedule. October 28 retains its documented partial expanded-slot stock coverage. Older archived universe definitions are not all strictly point-in-time. Feature availability and skipped-entry reasons are included in the audit artifacts.

[Day-wise results](DAY_WISE_RESULTS.md) | [All daily metrics](daily.csv) | [Trade attribution](trade_attribution.csv) | [Order status counts](order_status_counts.csv)

Existing strategies, prior result artifacts, production schedules and live/paper settings remain unchanged.
