# Opening Range Breakout: Break-Retest-Hold Research

## Decision

**REJECT — do not wire into V11 backtesting or V7 live.**

The redesigned setup did not produce a configuration that was profitable in
both development folds and validation. The frozen fallback then lost every
trade in the confirmation test.

## Causal event definition

This experiment replaced the rejected ordinary opening-range breakout with:

1. A completed 5-minute close above OR15, OR20, or OR30.
2. A retest during the next one to four completed 5-minute bars.
3. The retest low approaches or dips slightly below the opening-range level,
   while the completed retest close holds near or above that level.
4. A confirmation close during the next one to three completed 5-minute bars
   must break both the retest high and the opening-range level.
5. Entry is only allowed on a break of the completed confirmation bar's high
   during the following three 1-minute bars. The order is cancelled if price
   invalidates the opening-range level first.

No future bar was used to form a signal or fill.

## Data split

| Fold | Sessions | Date range |
|---|---:|---|
| Development A | 20 | 2026-05-05 to 2026-06-02 |
| Development B | 20 | 2026-06-03 to 2026-07-01 |
| Validation | 10 | 2026-07-02 to 2026-07-15 |
| Confirmation test | 10 | 2026-07-16 to 2026-07-29 |

The test fold was not used to select the signal parameters or exit. Because
the same date range has now been inspected in earlier ORB experiments, it is
properly described as a confirmation test rather than a pristine unseen test.

## Search coverage

- 1,285 matched 5-minute/1-minute symbol files scanned.
- 58,763 broad causal break-retest-hold candidates.
- 21,220 candidates obtained valid, non-ambiguous 1-minute entries.
- 2,500 signal configurations searched across OR15/OR20/OR30, liquidity,
  volatility, retest geometry, confirmation timing, trend/momentum, volume,
  market regime, and per-slot ranking.
- 29,400 detailed stop/target/holding-period combinations evaluated on the top
  development configurations.
- Exit grid: targets 0.25% to 0.85%, stops 0.35% to 0.95%, and maximum holds
  from 15 to 60 minutes.
- Entry prices include 5 bps slippage. Stop and time exits include another
  5 bps; targets are filled at their target price. Statutory transaction costs
  are deducted. The stress result applies 1.5 times normal exit slippage.

## Frozen fallback

No candidate passed the required three-fold gate:

- PF greater than 1.05 and positive expectancy in Development A.
- PF greater than 1.05 and positive expectancy in Development B.
- PF greater than 1.05, positive expectancy, and at least six trades in
  validation.

The best minimum-fold fallback was an OR20 retest with a 0.35% target, 0.55%
stop, and 30-minute maximum hold.

| Fold | Trades | Profit factor | Expectancy | Net P&L |
|---|---:|---:|---:|---:|
| Development A | 16 | 0.970 | -₹4.18 | -₹66.91 |
| Development B | 13 | 2.102 | ₹96.28 | ₹1,251.63 |
| Validation | 6 | 15.404 | ₹207.09 | ₹1,242.51 |
| Confirmation test | 8 | 0.000 | -₹457.23 | -₹3,657.81 |
| Test at 1.5x slippage | 8 | 0.000 | -₹481.98 | -₹3,855.85 |

The eight test trades occurred on four active days. All eight lost; five exited
by time and three hit the stop. This is not a marginal miss that should be
repaired by another small parameter adjustment.

## Conclusion

The break-retest-hold definition improves selectivity but does not establish a
stable ORB edge in this three-month sample. Wiring it would promote an
overfit, failed setup into shared backtest/live code. No existing V11, V7, or
other repository files were changed by this experiment.
