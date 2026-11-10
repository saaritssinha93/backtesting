# G3 V24 RSI entry-timing experiment

68 available completed sessions from 2026-07-29 through 2026-11-09. All variants start with the exact same G3/G7-admitted stock and slot inventory. Entry timing alone varies.

The RSI hypothesis waits for a price pause with RSI in 45–55, then a fresh completed-candle cross above 55 for LONG or below 45 for SHORT. A later candle must break the cross candle's directional extreme within the original entry expiry. Exact state/invalidation and any control-arm rules are recorded in config.json. These starting thresholds were fixed before the outcome comparison; they are not optimized.

The baseline retains its existing targets, 1.50% initial stop and 80%-of-target completed-close breakeven rule. The same exit policy is rebased to each variant's actual entry price. Modeled exposure is Rs 500,000 and round-trip cost Rs 250 per fill with unlimited concurrent capital. Original and added slot caps remain fixed. Stops use the inherited conservative ambiguous-bar and adverse-gap rules; square-off remains 15:15.

## Full matched history

| Policy | Orders | Fills | Wins | Losses | Flat | Win rate | Net PnL Rs | Loss amount Rs | Mean/fill Rs | Median/fill Rs | Mean/day Rs | Median/day Rs | Daily-close DD Rs |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| V23 unchanged entry timing | 930 | 824 | 541 | 254 | 29 | 65.66% | 1,221,079.86 | 864,226.26 | 1,481.89 | 2,549.56 | 17,957.06 | 13,753.83 | 31,338.39 |
| RSI reset and recovery | 930 | 119 | 72 | 41 | 6 | 60.50% | 106,976.45 | 149,080.11 | 898.96 | 1,323.68 | 1,573.18 | 186.98 | 28,220.92 |
| RSI recovery with original trigger retained | 930 | 50 | 32 | 15 | 3 | 64.00% | 59,539.84 | 52,181.10 | 1,190.80 | 1,220.64 | 875.59 | 0.00 | 16,739.41 |

Loss amount is the absolute sum of negative after-cost trade PnL; flat trades do not count as wins. Daily-close drawdown starts from zero cumulative PnL and includes available zero-trade sessions. It does not measure intraday drawdown.

## Direction

### LONG

| Policy | Orders | Fills | Wins | Losses | Flat | Win rate | Net PnL Rs | Loss amount Rs | Mean/fill Rs | Median/fill Rs | Mean/day Rs | Median/day Rs | Daily-close DD Rs |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| V23 unchanged entry timing | 408 | 373 | 246 | 111 | 16 | 65.95% | 628,891.75 | 378,118.69 | 1,686.04 | 3,081.33 | 9,248.41 | 6,109.41 | 20,311.71 |
| RSI reset and recovery | 408 | 41 | 26 | 12 | 3 | 63.41% | 58,085.15 | 51,591.96 | 1,416.71 | 1,391.65 | 854.19 | 0.00 | 13,905.56 |
| RSI recovery with original trigger retained | 408 | 16 | 9 | 7 | 0 | 56.25% | 16,471.47 | 24,313.06 | 1,029.47 | 602.26 | 242.23 | 0.00 | 14,148.73 |

### SHORT

| Policy | Orders | Fills | Wins | Losses | Flat | Win rate | Net PnL Rs | Loss amount Rs | Mean/fill Rs | Median/fill Rs | Mean/day Rs | Median/day Rs | Daily-close DD Rs |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| V23 unchanged entry timing | 522 | 451 | 295 | 143 | 13 | 65.41% | 592,188.10 | 486,107.57 | 1,313.06 | 1,814.76 | 8,708.65 | 4,318.11 | 43,336.40 |
| RSI reset and recovery | 522 | 78 | 46 | 29 | 3 | 58.97% | 48,891.30 | 97,488.15 | 626.81 | 1,274.67 | 718.99 | 0.00 | 20,024.29 |
| RSI recovery with original trigger retained | 522 | 34 | 23 | 8 | 3 | 67.65% | 43,068.37 | 27,868.05 | 1,266.72 | 1,772.32 | 633.36 | 0.00 | 8,989.41 |

## Chronological comparisons

### earlier_59: 2026-07-29 through 2026-10-27

| Policy | Orders | Fills | Wins | Losses | Flat | Win rate | Net PnL Rs | Loss amount Rs | Mean/fill Rs | Median/fill Rs | Mean/day Rs | Median/day Rs | Daily-close DD Rs |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| V23 unchanged entry timing | 777 | 687 | 465 | 201 | 21 | 67.69% | 1,113,740.27 | 665,963.36 | 1,621.16 | 2,550.00 | 18,876.95 | 14,177.91 | 31,338.39 |
| RSI reset and recovery | 777 | 93 | 58 | 31 | 4 | 62.37% | 119,683.73 | 102,075.66 | 1,286.92 | 1,678.64 | 2,028.54 | 0.00 | 11,448.91 |
| RSI recovery with original trigger retained | 777 | 41 | 26 | 12 | 3 | 63.41% | 53,819.63 | 41,838.59 | 1,312.67 | 1,225.67 | 912.20 | 0.00 | 14,441.67 |

### later_9: 2026-10-28 through 2026-11-09

| Policy | Orders | Fills | Wins | Losses | Flat | Win rate | Net PnL Rs | Loss amount Rs | Mean/fill Rs | Median/fill Rs | Mean/day Rs | Median/day Rs | Daily-close DD Rs |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| V23 unchanged entry timing | 153 | 137 | 76 | 53 | 8 | 55.47% | 107,339.58 | 198,262.90 | 783.50 | 2,297.68 | 11,926.62 | 5,227.96 | 26,246.72 |
| RSI reset and recovery | 153 | 26 | 14 | 10 | 2 | 53.85% | -12,707.28 | 47,004.45 | -488.74 | 120.25 | -1,411.92 | 1,323.68 | 28,220.92 |
| RSI recovery with original trigger retained | 153 | 9 | 6 | 3 | 0 | 66.67% | 5,720.21 | 10,342.52 | 635.58 | 1,209.55 | 635.58 | 1,209.55 | 8,989.41 |

### inherited_66: 2026-07-29 through 2026-11-05

| Policy | Orders | Fills | Wins | Losses | Flat | Win rate | Net PnL Rs | Loss amount Rs | Mean/fill Rs | Median/fill Rs | Mean/day Rs | Median/day Rs | Daily-close DD Rs |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| V23 unchanged entry timing | 891 | 790 | 520 | 241 | 29 | 65.82% | 1,202,036.50 | 808,171.11 | 1,521.57 | 2,542.90 | 18,212.67 | 13,753.83 | 31,338.39 |
| RSI reset and recovery | 891 | 112 | 70 | 36 | 6 | 62.50% | 127,526.44 | 122,492.15 | 1,138.63 | 1,440.89 | 1,932.22 | 186.98 | 21,602.79 |
| RSI recovery with original trigger retained | 891 | 49 | 32 | 14 | 3 | 65.31% | 60,892.95 | 50,828.00 | 1,242.71 | 1,225.67 | 922.62 | 0.00 | 16,739.41 |

### new_2: 2026-11-06 through 2026-11-09

| Policy | Orders | Fills | Wins | Losses | Flat | Win rate | Net PnL Rs | Loss amount Rs | Mean/fill Rs | Median/fill Rs | Mean/day Rs | Median/day Rs | Daily-close DD Rs |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| V23 unchanged entry timing | 39 | 34 | 21 | 13 | 0 | 61.76% | 19,043.35 | 56,055.15 | 560.10 | 3,516.14 | 9,521.68 | 9,521.68 | 0.00 |
| RSI reset and recovery | 39 | 7 | 2 | 5 | 0 | 28.57% | -20,549.99 | 26,587.96 | -2,935.71 | -1,984.86 | -10,275.00 | -10,275.00 | 23,250.00 |
| RSI recovery with original trigger retained | 39 | 1 | 0 | 1 | 0 | 0.00% | -1,353.10 | 1,353.10 | -1,353.10 | -1,353.10 | -676.55 | -676.55 | 1,353.10 |

### 2026-11-06: 2026-11-06 through 2026-11-06

| Policy | Orders | Fills | Wins | Losses | Flat | Win rate | Net PnL Rs | Loss amount Rs | Mean/fill Rs | Median/fill Rs | Mean/day Rs | Median/day Rs | Daily-close DD Rs |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| V23 unchanged entry timing | 17 | 15 | 11 | 4 | 0 | 73.33% | 14,421.38 | 25,265.15 | 961.43 | 3,750.00 | 14,421.38 | 14,421.38 | 0.00 |
| RSI reset and recovery | 17 | 3 | 0 | 3 | 0 | 0.00% | -23,250.00 | 23,250.00 | -7,750.00 | -7,750.00 | -23,250.00 | -23,250.00 | 23,250.00 |
| RSI recovery with original trigger retained | 17 | 0 | 0 | 0 | 0 | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 |

### 2026-11-09: 2026-11-09 through 2026-11-09

| Policy | Orders | Fills | Wins | Losses | Flat | Win rate | Net PnL Rs | Loss amount Rs | Mean/fill Rs | Median/fill Rs | Mean/day Rs | Median/day Rs | Daily-close DD Rs |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| V23 unchanged entry timing | 22 | 19 | 10 | 9 | 0 | 52.63% | 4,621.97 | 30,790.01 | 243.26 | 1,361.98 | 4,621.97 | 4,621.97 | 0.00 |
| RSI reset and recovery | 22 | 4 | 2 | 2 | 0 | 50.00% | 2,700.01 | 3,337.96 | 675.00 | -607.89 | 2,700.01 | 2,700.01 | 0.00 |
| RSI recovery with original trigger retained | 22 | 1 | 0 | 1 | 0 | 0.00% | -1,353.10 | 1,353.10 | -1,353.10 | -1,353.10 | -1,353.10 | -1,353.10 | 1,353.10 |

## What changed

| Policy | Unchanged fills | Retimed fills | Missed winners | Missed losers | New fills | Profit missed Rs | Loss avoided Rs | Retiming PnL change Rs | New-fill net Rs | Total PnL change Rs |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| RSI reset and recovery | 0 | 98 | 487 | 217 | 21 | 1,885,600.94 | 725,150.78 | +33,637.79 | +12,708.96 | -1,114,103.41 |
| RSI recovery with original trigger retained | 8 | 42 | 509 | 240 | 0 | 1,966,304.11 | 821,800.62 | -17,036.52 | +0.00 | -1,161,540.01 |

A missed baseline fill contributes the negative of its original PnL to the comparison. A new fill contributes its entire net PnL. A retimed fill keeps the same candidate SID and contributes candidate PnL minus baseline PnL. These three effects reconcile exactly to the total change. Fewer losing fills alone is not evidence that the entry timing improved: profitable fills can also be missed.

## Interpretation

- **RSI reset and recovery**: net PnL change Rs -1,114,103.41; loss-amount change Rs -715,146.15; fill-count change -705; win-rate change -5.15 percentage points; daily-close drawdown change Rs -3,117.48.
- **RSI recovery with original trigger retained**: net PnL change Rs -1,161,540.01; loss-amount change Rs -812,045.16; fill-count change -774; win-rate change -1.66 percentage points; daily-close drawdown change Rs -14,598.98.

All observations are retrospective. The earlier/later slices are sensitivity checks, not untouched out-of-sample evidence. This study does not promote a live policy. Delaying entries can miss uninterrupted breakouts; a higher win rate or smaller loss total can occur simply because fewer trades fill.

Coverage remains inherited from the V24 reference: October 1 is excluded for incomplete source data and the November 8 special session uses a different schedule. October 28 retains its documented partial expanded-slot stock coverage. Older archived universe definitions are not all strictly point-in-time. Feature availability and skipped-entry reasons are included in the audit artifacts.

[Day-wise results](DAY_WISE_RESULTS.md) | [All daily metrics](daily.csv) | [Trade attribution](trade_attribution.csv) | [Order status counts](order_status_counts.csv)

Existing strategies, prior result artifacts, production schedules and live/paper settings remain unchanged.
