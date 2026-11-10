# G3 V24: compression-breakout diagnostic

Tested Bollinger BandWidth compression followed by a fresh directional breakout, retaining G3's existing volume qualification. **The evidence does not support adding this as a mandatory entry filter.** Only 12 baseline fills matched the fresh pattern, their average and median profit were lower than other eligible trades, and none occurred in the latest nine sessions.

The study covers the same 68 completed available V24 sessions from July 29 through November 9, 2026. It labels the original 930 admitted opportunities and 824 filled trades. Entry selection, timing, targets, stops and position sizes are unchanged. These are context comparisons within the existing backtest, not separately executed strategies.

| Context | Filled trades | Win rate | Mean net PnL per fill | Median net PnL per fill | Total net PnL |
|:--|--:|--:|--:|--:|--:|
| Entire unchanged baseline | 824 | 65.66% | Rs 1,481.89 | Rs 2,549.56 | Rs 1,221,079.86 |
| Fresh compression breakout | 12 | 66.67% | Rs 1,258.61 | Rs 1,434.63 | Rs 15,103.34 |
| Earlier breakout continuation | 89 | 52.81% | Rs 739.36 | Rs 551.14 | Rs 65,803.08 |
| Breakout without qualifying compression | 316 | 65.82% | Rs 1,370.44 | Rs 2,410.43 | Rs 433,059.81 |
| Other ready context | 344 | 67.73% | Rs 1,588.88 | Rs 2,983.46 | Rs 546,574.18 |
| Early-session context: not applicable | 63 | 71.43% | Rs 2,548.25 | Rs 3,750.00 | Rs 160,539.44 |

All PnL uses the inherited Rs 500,000 exposure and Rs 250 round-trip modeled cost per fill, with unlimited concurrent capital. Baseline daily-close drawdown remains Rs 31,338.39. No trades were excluded for unavailable evidence.

## What the comparison says

The 12 fresh-pattern fills had a 66.67% win rate versus 65.15% across 749 other ready fills. However, mean PnL was Rs 1,258.61 versus Rs 1,395.78. With identical pooled-count weights across common setup/direction groups, the fresh-pattern mean was Rs 535.54 lower and win rate 6.30 percentage points lower. These comparisons have limited overlap and a small sample; they do not establish a causal benefit or disadvantage.

Of the fresh-pattern fills, 11 were shorts and only one was long. All occurred in the earlier 59-session slice; the later nine-session slice had none. The study therefore cannot establish effectiveness in both directions or in the latest period.

Requiring this pattern alone would retain only 12 of 824 fills and omit 533 winners alongside 250 losers. That would sacrifice substantial profitable activity. This subset illustration was not applied to the strategy.

Earlier-breakout continuation has weaker raw results, but its 89 fills still contribute positive Rs 65,803.08. Its long side lost Rs 14,804.09 across 28 fills, while its short side earned Rs 80,607.18 across 61 fills. This is a retrospective diagnostic clue, not a validated direction-specific veto.

## Frozen definition

Features are evaluated at the original completed one-minute G3 confirmation, using only data available by that time:

1. Bollinger Bands use 20 consecutive valid same-session closes, two population standard deviations, and BandWidth `100 * (upper - lower) / SMA`.
2. A bar is compressed when its width is at or below the twentieth percentile of the preceding 100 valid width observations. The reference excludes that bar and may include prior completed sessions.
3. A fresh compression breakout requires preceding-minute compression, a confirmation close above the preceding five candles' high for a long or below their low for a short, and increasing width. The five-candle range excludes confirmation.
4. Earlier continuation uses the most recent compressed anchor two to five bars before confirmation, a fixed five-candle range ending at that anchor, and the first close outside either side of that range. That first break must occur before confirmation in the trade's direction, with confirmation still beyond the same boundary.
5. Remaining observations are classified as breakouts without qualifying compression or other ready contexts. The full context requires 25 consecutive same-session bars; earlier trades receive a separate not-applicable label and remain in the baseline.

The admitted inventory already satisfies existing G3 volume qualification. No additional volume threshold was added or optimized, and this study cannot isolate volume's incremental effect.

## Results and verification

- [Full report, directions and chronological comparisons](results_through_20261109/REPORT.md)
- [Day-wise results for all 68 sessions](results_through_20261109/DAY_WISE_RESULTS.md)
- [Every labeled filled trade](results_through_20261109/labeled_trades.csv)
- [All admitted opportunities](results_through_20261109/labeled_orders.csv)
- [Matched-context comparison summary](results_through_20261109/comparison_summary.csv)
- [Exact configuration and fingerprints](results_through_20261109/config.json)
- [Verification](results_through_20261109/verification.json)

Seventeen feature/reporting tests passed, including causal-prefix invariance, range/reference exclusions, same-session readiness, long/short handling and cohort reconciliation. Post-publication verification passed for all 15 manifested result files, 172 loaded local code files, 22 frozen baseline artifacts and the feature-cache signature/payload. Published labeled trades exactly preserve the original 824 trade identities, entries, exits and PnL.

To reproduce into a new directory from the workspace root:

```powershell
python -m g3_v24_selection_20261109.compression_diagnostic_20261110.run --output g3_v24_selection_20261109/compression_diagnostic_20261110/reproduction
```

The destination must not already exist. Previous versions and results, and live/paper settings, remain unchanged.

## Limits

This is retrospective research on previously examined history. Earlier/later comparisons are sensitivity checks, not untouched out-of-sample evidence. Inherited exclusions and limitations remain: October 1 has incomplete source data, the November 8 special session has a different schedule and is excluded, October 28 has partial expanded-slot stock coverage, and older archived universes are not all strictly point-in-time. November 10 was incomplete at study start and is excluded.
