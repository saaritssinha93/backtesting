# G3 V24 selection-ranking backtest

68 available sessions from 2026-07-29 through 2026-11-09. The comparison changes selection ranking while retaining the adopted V23 exits and entry caps.

The unchanged baseline uses a 1.50% initial stop and each setup's existing target. A completed post-entry one-minute close at 80% of the target activates a stop at +0.05% signed return from entry on the next bar. The entry bar cannot arm protection. Stops only tighten. Adverse gap-open fills, stop-first ambiguous bars and the 15:15 time exit are retained.

All arms use Rs 500,000 modeled exposure and Rs 250 round-trip costs per filled trade with unlimited concurrent capital. The 34 original pairs retain cap three; the 39 added pairs retain cap one. The 73 pairs run through 14:00. Ranking keeps these caps; it does not guarantee identical admission or fill counts.

The existing new-pair volume veto and G7 confirmation rules retain their post-ranking timing. A vetoed selected candidate does not promote the next stock. V23 already includes G7 filters, so this research baseline differs from the production paper strategy examined on 9 November.

## Full available history

| Policy | Orders | Trades | Wins | Losses | Flat | Win rate | Net PnL Rs | Loss amount Rs | Mean/trade Rs | Median/trade Rs | Daily-close DD Rs |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| V23 unchanged baseline | 930 | 824 | 541 | 254 | 29 | 65.66% | 1,221,079.86 | 864,226.26 | 1,481.89 | 2,549.56 | 31,338.39 |
| Stretch ranking | 922 | 817 | 526 | 263 | 28 | 64.38% | 1,092,872.49 | 942,444.73 | 1,337.67 | 2,439.03 | 40,515.61 |
| Breakout-position ranking | 929 | 821 | 535 | 257 | 29 | 65.16% | 1,141,661.30 | 902,454.74 | 1,390.57 | 2,427.22 | 34,908.60 |
| Stretch + position ranking | 922 | 815 | 532 | 255 | 28 | 65.28% | 1,116,176.55 | 922,504.61 | 1,369.54 | 2,439.03 | 44,354.45 |
| Stretch + position + volume ranking | 928 | 821 | 521 | 270 | 30 | 63.46% | 1,037,903.29 | 964,919.41 | 1,264.19 | 2,119.66 | 74,455.05 |

Loss amount is the absolute sum of negative after-cost trade PnL. Flat trades are not wins. Daily-close drawdown includes zero-trade dates and starts from zero equity; it is not intraday drawdown.

## Chronological comparisons

### inherited_66: 2026-07-29 through 2026-11-05

| Policy | Orders | Trades | Wins | Losses | Flat | Win rate | Net PnL Rs | Loss amount Rs | Mean/trade Rs | Median/trade Rs | Daily-close DD Rs |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| V23 unchanged baseline | 891 | 790 | 520 | 241 | 29 | 65.82% | 1,202,036.50 | 808,171.11 | 1,521.57 | 2,542.90 | 31,338.39 |
| Stretch ranking | 884 | 782 | 504 | 250 | 28 | 64.45% | 1,079,244.75 | 880,324.09 | 1,380.11 | 2,409.48 | 40,515.61 |
| Breakout-position ranking | 891 | 787 | 513 | 245 | 29 | 65.18% | 1,120,570.67 | 848,753.96 | 1,423.85 | 2,391.73 | 34,908.60 |
| Stretch + position ranking | 884 | 781 | 510 | 243 | 28 | 65.30% | 1,094,128.94 | 868,803.83 | 1,400.93 | 2,427.22 | 44,354.45 |
| Stretch + position + volume ranking | 890 | 787 | 500 | 257 | 30 | 63.53% | 1,018,859.94 | 908,864.25 | 1,294.61 | 2,040.31 | 74,455.05 |

### early_59: 2026-07-29 through 2026-10-27

| Policy | Orders | Trades | Wins | Losses | Flat | Win rate | Net PnL Rs | Loss amount Rs | Mean/trade Rs | Median/trade Rs | Daily-close DD Rs |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| V23 unchanged baseline | 777 | 687 | 465 | 201 | 21 | 67.69% | 1,113,740.27 | 665,963.36 | 1,621.16 | 2,550.00 | 31,338.39 |
| Stretch ranking | 774 | 683 | 452 | 211 | 20 | 66.18% | 1,012,809.66 | 722,714.33 | 1,482.88 | 2,470.69 | 40,515.61 |
| Breakout-position ranking | 780 | 687 | 460 | 206 | 21 | 66.96% | 1,043,527.84 | 699,322.81 | 1,518.96 | 2,439.03 | 34,908.60 |
| Stretch + position ranking | 773 | 682 | 458 | 204 | 20 | 67.16% | 1,022,863.67 | 717,345.12 | 1,499.80 | 2,484.11 | 44,354.45 |
| Stretch + position + volume ranking | 778 | 687 | 450 | 215 | 22 | 65.50% | 964,914.14 | 746,265.21 | 1,404.53 | 2,172.58 | 68,192.82 |

### later_9: 2026-10-28 through 2026-11-09

| Policy | Orders | Trades | Wins | Losses | Flat | Win rate | Net PnL Rs | Loss amount Rs | Mean/trade Rs | Median/trade Rs | Daily-close DD Rs |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| V23 unchanged baseline | 153 | 137 | 76 | 53 | 8 | 55.47% | 107,339.58 | 198,262.90 | 783.50 | 2,297.68 | 26,246.72 |
| Stretch ranking | 148 | 134 | 74 | 52 | 8 | 55.22% | 80,062.83 | 219,730.39 | 597.48 | 2,047.53 | 24,925.86 |
| Breakout-position ranking | 149 | 134 | 75 | 51 | 8 | 55.97% | 98,133.46 | 203,131.93 | 732.34 | 2,344.71 | 26,246.72 |
| Stretch + position ranking | 149 | 133 | 74 | 51 | 8 | 55.64% | 93,312.87 | 205,159.49 | 701.60 | 2,297.68 | 26,246.72 |
| Stretch + position + volume ranking | 150 | 134 | 71 | 55 | 8 | 52.99% | 72,989.15 | 218,654.20 | 544.70 | 1,285.77 | 24,925.86 |

### new_2: 2026-11-06 through 2026-11-09

| Policy | Orders | Trades | Wins | Losses | Flat | Win rate | Net PnL Rs | Loss amount Rs | Mean/trade Rs | Median/trade Rs | Daily-close DD Rs |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| V23 unchanged baseline | 39 | 34 | 21 | 13 | 0 | 61.76% | 19,043.35 | 56,055.15 | 560.10 | 3,516.14 | 0.00 |
| Stretch ranking | 38 | 35 | 22 | 13 | 0 | 62.86% | 13,627.74 | 62,120.64 | 389.36 | 3,500.00 | 0.00 |
| Breakout-position ranking | 38 | 34 | 22 | 12 | 0 | 64.71% | 21,090.63 | 53,700.78 | 620.31 | 3,500.00 | 0.00 |
| Stretch + position ranking | 38 | 34 | 22 | 12 | 0 | 64.71% | 22,047.60 | 53,700.78 | 648.46 | 3,516.14 | 0.00 |
| Stretch + position + volume ranking | 38 | 34 | 21 | 13 | 0 | 61.76% | 19,043.35 | 56,055.15 | 560.10 | 3,516.14 | 0.00 |

### fit_44: 2026-07-29 through 2026-10-05

| Policy | Orders | Trades | Wins | Losses | Flat | Win rate | Net PnL Rs | Loss amount Rs | Mean/trade Rs | Median/trade Rs | Daily-close DD Rs |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| V23 unchanged baseline | 569 | 496 | 345 | 134 | 17 | 69.56% | 889,147.16 | 426,032.89 | 1,792.64 | 2,550.00 | 15,638.86 |
| Stretch ranking | 568 | 495 | 336 | 142 | 17 | 67.88% | 809,025.60 | 473,396.60 | 1,634.40 | 2,457.56 | 16,122.12 |
| Breakout-position ranking | 570 | 493 | 338 | 138 | 17 | 68.56% | 834,145.72 | 445,569.31 | 1,691.98 | 2,395.28 | 17,879.75 |
| Stretch + position ranking | 567 | 493 | 340 | 136 | 17 | 68.97% | 831,886.00 | 457,600.98 | 1,687.40 | 2,536.77 | 14,124.92 |
| Stretch + position + volume ranking | 569 | 493 | 332 | 144 | 17 | 67.34% | 788,846.79 | 470,627.26 | 1,600.09 | 2,212.36 | 16,820.68 |

### validation_15: 2026-10-06 through 2026-10-27

| Policy | Orders | Trades | Wins | Losses | Flat | Win rate | Net PnL Rs | Loss amount Rs | Mean/trade Rs | Median/trade Rs | Daily-close DD Rs |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| V23 unchanged baseline | 208 | 191 | 120 | 67 | 4 | 62.83% | 224,593.11 | 239,930.47 | 1,175.88 | 2,520.08 | 31,338.39 |
| Stretch ranking | 206 | 188 | 116 | 69 | 3 | 61.70% | 203,784.07 | 249,317.73 | 1,083.96 | 2,508.81 | 40,515.61 |
| Breakout-position ranking | 210 | 194 | 122 | 68 | 4 | 62.89% | 209,382.12 | 253,753.50 | 1,079.29 | 2,468.28 | 34,908.60 |
| Stretch + position ranking | 206 | 189 | 118 | 68 | 3 | 62.43% | 190,977.67 | 259,744.14 | 1,010.46 | 2,439.03 | 44,354.45 |
| Stretch + position + volume ranking | 209 | 194 | 118 | 71 | 5 | 60.82% | 176,067.35 | 275,637.94 | 907.56 | 1,964.42 | 68,192.82 |

## Displaced and replacement trades

| Ranking arm | Removed winners | Removed losers | Added winners | Added losers | Winning PnL removed Rs | Losing PnL avoided Rs | Winning PnL added Rs | Losing PnL added Rs | Net change Rs |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| Stretch ranking | 54 | 31 | 39 | 40 | 173,094.16 | 90,274.27 | 123,105.26 | 168,492.73 | -128,207.36 |
| Breakout-position ranking | 38 | 25 | 32 | 28 | 129,602.25 | 68,447.46 | 88,412.17 | 106,675.94 | -79,418.56 |
| Stretch + position ranking | 43 | 25 | 34 | 26 | 139,795.44 | 76,652.17 | 93,170.48 | 134,930.52 | -104,903.31 |
| Stretch + position + volume ranking | 56 | 22 | 36 | 38 | 187,281.58 | 49,306.28 | 104,798.16 | 149,999.42 | -183,176.56 |

## Interpretation

No ranking arm qualified on the first 44 sessions: qualification requires higher net profit with no increase in loss amount or daily-close drawdown. The unchanged baseline remains the diagnostic choice.

These are retrospective comparisons. Earlier strategy choices used this history, and the 9 November trades informed the selection hypothesis. The chronological slices are sensitivity checks, not untouched out-of-sample proof. No new live policy is promoted from this report.

The inherited source excludes 1 October for incomplete sealed data. The 8 November special session uses a different schedule and is not part of this normal-session backtest. Other source exclusions and unavailable features are recorded in the configuration and coverage artifacts.

The inherited 28 October expanded-slot source covers 211 of 215 stocks, with 116 unavailable appended stock/slot rows. These rows remain absent in every arm; healthy-stock observations are retained. Some earlier archived universes are not strictly point-in-time, so survivorship/cohort bias remains a limitation.

[Day-wise results](DAY_WISE_RESULTS.md) | [All daily metrics](daily.csv) | [Displaced/replacement trades](trade_changes.csv) | [Overall metrics](overall.csv)

Previous versions, production schedules, live trading and paper runtime settings were not changed.
