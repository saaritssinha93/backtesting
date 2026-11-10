# G3 V24 selection-ranking backtest

1 available sessions from 2026-07-29 through 2026-07-29. The comparison changes selection ranking while retaining the adopted V23 exits and entry caps.

The unchanged baseline uses a 1.50% initial stop and each setup's existing target. A completed post-entry one-minute close at 80% of the target activates a stop at +0.05% signed return from entry on the next bar. The entry bar cannot arm protection. Stops only tighten. Adverse gap-open fills, stop-first ambiguous bars and the 15:15 time exit are retained.

All arms use Rs 500,000 modeled exposure and Rs 250 round-trip costs per filled trade with unlimited concurrent capital. The 34 original pairs retain cap three; the 39 added pairs retain cap one. The 73 pairs run through 14:00. Ranking keeps these caps; it does not guarantee identical admission or fill counts.

The existing new-pair volume veto and G7 confirmation rules retain their post-ranking timing. A vetoed selected candidate does not promote the next stock. V23 already includes G7 filters, so this research baseline differs from the production paper strategy examined on 9 November.

## Full available history

| Policy | Orders | Trades | Wins | Losses | Flat | Win rate | Net PnL Rs | Loss amount Rs | Mean/trade Rs | Median/trade Rs | Daily-close DD Rs |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| V23 unchanged baseline | 17 | 13 | 11 | 2 | 0 | 84.62% | 78,110.72 | 8,233.90 | 6,008.52 | 3,750.00 | 0.00 |
| Stretch ranking | 17 | 13 | 11 | 2 | 0 | 84.62% | 78,110.72 | 8,233.90 | 6,008.52 | 3,750.00 | 0.00 |
| Breakout-position ranking | 17 | 13 | 11 | 2 | 0 | 84.62% | 75,104.81 | 8,233.90 | 5,777.29 | 3,750.00 | 0.00 |
| Stretch + position ranking | 17 | 13 | 11 | 2 | 0 | 84.62% | 75,104.81 | 8,233.90 | 5,777.29 | 3,750.00 | 0.00 |
| Stretch + position + volume ranking | 17 | 13 | 11 | 2 | 0 | 84.62% | 75,104.81 | 8,233.90 | 5,777.29 | 3,750.00 | 0.00 |

Loss amount is the absolute sum of negative after-cost trade PnL. Flat trades are not wins. Daily-close drawdown includes zero-trade dates and starts from zero equity; it is not intraday drawdown.

## Chronological comparisons

### inherited_1: 2026-07-29 through 2026-07-29

| Policy | Orders | Trades | Wins | Losses | Flat | Win rate | Net PnL Rs | Loss amount Rs | Mean/trade Rs | Median/trade Rs | Daily-close DD Rs |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| V23 unchanged baseline | 17 | 13 | 11 | 2 | 0 | 84.62% | 78,110.72 | 8,233.90 | 6,008.52 | 3,750.00 | 0.00 |
| Stretch ranking | 17 | 13 | 11 | 2 | 0 | 84.62% | 78,110.72 | 8,233.90 | 6,008.52 | 3,750.00 | 0.00 |
| Breakout-position ranking | 17 | 13 | 11 | 2 | 0 | 84.62% | 75,104.81 | 8,233.90 | 5,777.29 | 3,750.00 | 0.00 |
| Stretch + position ranking | 17 | 13 | 11 | 2 | 0 | 84.62% | 75,104.81 | 8,233.90 | 5,777.29 | 3,750.00 | 0.00 |
| Stretch + position + volume ranking | 17 | 13 | 11 | 2 | 0 | 84.62% | 75,104.81 | 8,233.90 | 5,777.29 | 3,750.00 | 0.00 |

### early_1: 2026-07-29 through 2026-07-29

| Policy | Orders | Trades | Wins | Losses | Flat | Win rate | Net PnL Rs | Loss amount Rs | Mean/trade Rs | Median/trade Rs | Daily-close DD Rs |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| V23 unchanged baseline | 17 | 13 | 11 | 2 | 0 | 84.62% | 78,110.72 | 8,233.90 | 6,008.52 | 3,750.00 | 0.00 |
| Stretch ranking | 17 | 13 | 11 | 2 | 0 | 84.62% | 78,110.72 | 8,233.90 | 6,008.52 | 3,750.00 | 0.00 |
| Breakout-position ranking | 17 | 13 | 11 | 2 | 0 | 84.62% | 75,104.81 | 8,233.90 | 5,777.29 | 3,750.00 | 0.00 |
| Stretch + position ranking | 17 | 13 | 11 | 2 | 0 | 84.62% | 75,104.81 | 8,233.90 | 5,777.29 | 3,750.00 | 0.00 |
| Stretch + position + volume ranking | 17 | 13 | 11 | 2 | 0 | 84.62% | 75,104.81 | 8,233.90 | 5,777.29 | 3,750.00 | 0.00 |

### later_0: no available sessions

| Policy | Orders | Trades | Wins | Losses | Flat | Win rate | Net PnL Rs | Loss amount Rs | Mean/trade Rs | Median/trade Rs | Daily-close DD Rs |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| V23 unchanged baseline | 0 | 0 | 0 | 0 | 0 | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 |
| Stretch ranking | 0 | 0 | 0 | 0 | 0 | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 |
| Breakout-position ranking | 0 | 0 | 0 | 0 | 0 | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 |
| Stretch + position ranking | 0 | 0 | 0 | 0 | 0 | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 |
| Stretch + position + volume ranking | 0 | 0 | 0 | 0 | 0 | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 |

### new_0: no available sessions

| Policy | Orders | Trades | Wins | Losses | Flat | Win rate | Net PnL Rs | Loss amount Rs | Mean/trade Rs | Median/trade Rs | Daily-close DD Rs |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| V23 unchanged baseline | 0 | 0 | 0 | 0 | 0 | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 |
| Stretch ranking | 0 | 0 | 0 | 0 | 0 | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 |
| Breakout-position ranking | 0 | 0 | 0 | 0 | 0 | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 |
| Stretch + position ranking | 0 | 0 | 0 | 0 | 0 | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 |
| Stretch + position + volume ranking | 0 | 0 | 0 | 0 | 0 | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 |

### fit_44: 2026-07-29 through 2026-07-29

| Policy | Orders | Trades | Wins | Losses | Flat | Win rate | Net PnL Rs | Loss amount Rs | Mean/trade Rs | Median/trade Rs | Daily-close DD Rs |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| V23 unchanged baseline | 17 | 13 | 11 | 2 | 0 | 84.62% | 78,110.72 | 8,233.90 | 6,008.52 | 3,750.00 | 0.00 |
| Stretch ranking | 17 | 13 | 11 | 2 | 0 | 84.62% | 78,110.72 | 8,233.90 | 6,008.52 | 3,750.00 | 0.00 |
| Breakout-position ranking | 17 | 13 | 11 | 2 | 0 | 84.62% | 75,104.81 | 8,233.90 | 5,777.29 | 3,750.00 | 0.00 |
| Stretch + position ranking | 17 | 13 | 11 | 2 | 0 | 84.62% | 75,104.81 | 8,233.90 | 5,777.29 | 3,750.00 | 0.00 |
| Stretch + position + volume ranking | 17 | 13 | 11 | 2 | 0 | 84.62% | 75,104.81 | 8,233.90 | 5,777.29 | 3,750.00 | 0.00 |

### validation_15: no available sessions

| Policy | Orders | Trades | Wins | Losses | Flat | Win rate | Net PnL Rs | Loss amount Rs | Mean/trade Rs | Median/trade Rs | Daily-close DD Rs |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| V23 unchanged baseline | 0 | 0 | 0 | 0 | 0 | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 |
| Stretch ranking | 0 | 0 | 0 | 0 | 0 | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 |
| Breakout-position ranking | 0 | 0 | 0 | 0 | 0 | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 |
| Stretch + position ranking | 0 | 0 | 0 | 0 | 0 | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 |
| Stretch + position + volume ranking | 0 | 0 | 0 | 0 | 0 | 0.00% | 0.00 | -0.00 | 0.00 | 0.00 | 0.00 |

## Displaced and replacement trades

| Ranking arm | Removed winners | Removed losers | Added winners | Added losers | Winning PnL removed Rs | Losing PnL avoided Rs | Winning PnL added Rs | Losing PnL added Rs | Net change Rs |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| Stretch ranking | 0 | 0 | 0 | 0 | 0.00 | -0.00 | 0.00 | -0.00 | +0.00 |
| Breakout-position ranking | 1 | 0 | 1 | 0 | 3,223.84 | -0.00 | 217.92 | -0.00 | -3,005.91 |
| Stretch + position ranking | 1 | 0 | 1 | 0 | 3,223.84 | -0.00 | 217.92 | -0.00 | -3,005.91 |
| Stretch + position + volume ranking | 1 | 0 | 1 | 0 | 3,223.84 | -0.00 | 217.92 | -0.00 | -3,005.91 |

## Interpretation

No ranking arm qualified on the first 44 sessions: qualification requires higher net profit with no increase in loss amount or daily-close drawdown. The unchanged baseline remains the diagnostic choice.

These are retrospective comparisons. Earlier strategy choices used this history, and the 9 November trades informed the selection hypothesis. The chronological slices are sensitivity checks, not untouched out-of-sample proof. No new live policy is promoted from this report.

The inherited source excludes 1 October for incomplete sealed data. The 8 November special session uses a different schedule and is not part of this normal-session backtest. Other source exclusions and unavailable features are recorded in the configuration and coverage artifacts.

[Day-wise results](DAY_WISE_RESULTS.md) | [All daily metrics](daily.csv) | [Displaced/replacement trades](trade_changes.csv) | [Overall metrics](overall.csv)

Previous versions, production schedules, live trading and paper runtime settings were not changed.
