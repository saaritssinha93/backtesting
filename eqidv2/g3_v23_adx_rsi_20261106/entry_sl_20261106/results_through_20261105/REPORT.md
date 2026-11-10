# V23 entry-relative stop study through 2026-11-05

All **790 baseline fills across 66 sessions** are retained. The verified baseline net profit is **Rs 1,177,522.10**. This study changes stop distances using information available at entry; it does not remove trades or increase their size.

**The baseline already uses the entry price:** its initial stop is 1.25% from entry, tightening to 1% after the native 120-minute interval. The new study tests whether a distance learned from earlier trade excursions, entry volatility or confirmed price structure can improve that rule.

**No entry-stop alternative qualified for the current V23 backtest.** Baseline stops remain selected. No loss reduction is claimed for the current mode; unselected policies remain diagnostic comparisons.

Recorded qualification checks: No fit44 model reduced losses while preserving baseline net profit and drawdown..

## All-session comparison

| Policy / period | Trades | Wins | Losses | Win rate | Net P&L (Rs) | Loss amount (Rs) | Daily-close drawdown (Rs) |
|:--|--:|--:|--:|--:|--:|--:|--:|
| baseline_v21_g7 | 790 | 535 | 255 | 67.72% | 1,177,522.10 | 863,772.35 | 35,384.36 |
| mae_q90 | 790 | 480 | 310 | 60.76% | 902,991.75 | 964,897.38 | 29,781.16 |
| mae_q95 | 790 | 510 | 280 | 64.56% | 1,005,285.48 | 952,373.88 | 27,694.25 |
| mae_q975 | 790 | 524 | 266 | 66.33% | 1,094,195.35 | 901,377.91 | 33,565.58 |
| mae_q99 | 790 | 532 | 258 | 67.34% | 1,148,165.32 | 884,464.92 | 34,816.00 |
| atr_q95 | 790 | 510 | 280 | 64.56% | 1,032,717.05 | 937,106.96 | 25,857.60 |
| volatility_bin_q95 | 790 | 514 | 276 | 65.06% | 1,050,854.81 | 922,515.13 | 24,565.58 |
| structure_q95 | 790 | 510 | 280 | 64.56% | 1,003,422.45 | 954,236.91 | 27,694.25 |

Loss amount is the absolute sum of negative after-cost trade P&L before offsetting winners. Exactly flat trades count as neither wins nor losses; win rate uses all fills.

## Entry-relative stop logic

- LONG stop price = actual entry price × (1 − stop distance % / 100).
- SHORT stop price = actual entry price × (1 + stop distance % / 100).
- The fitted distance is established using permitted entry-time information. ATR-based models use confirmed volatility, and structure-based models use a previously completed price boundary; the price level is then calculated from the actual fill.
- The initial distance cannot exceed the baseline 1.25%. The native 120-minute tightening takes the smaller of the initial distance and 1%, so an already tighter stop never widens.
- For example, a LONG entry at Rs 100 with a 0.8% stop has a planned stop at Rs 99.20; the corresponding SHORT stop is Rs 100.80. The stop percentage is a price distance before costs. An opening gap can create a larger realised loss.

The exact fitted constants are preserved in [fit.json](fit.json), and the planned price level for each trade is available in [stop_levels.csv](stop_levels.csv). These files distinguish the rule from its realised execution price.

- **baseline_v21_g7:** 1.25% initial entry stop, tightened to1.00% after120 minutes.
- **mae_q90:** Training native-winner MAE upper-bound90th percentile, rounded upward to0.05 percentage points. Fitted initial stop: 0.70% of actual entry.
- **mae_q95:** Training native-winner MAE upper-bound95th percentile, rounded upward to0.05 percentage points. Fitted initial stop: 0.90% of actual entry.
- **mae_q975:** Training native-winner MAE upper-bound97.5th percentile, rounded upward to0.05 percentage points. Fitted initial stop: 1.00% of actual entry.
- **mae_q99:** Training native-winner MAE upper-bound99th percentile, rounded upward to0.05 percentage points. Fitted initial stop: 1.20% of actual entry.
- **atr_q95:** Training winner MAE/ATR95th percentile rounded upward to0.25 ATR; multiply the confirmed one-minuteATR by this frozen multiple, with0.50% entry floor and1.25% cap. Fitted multiple: 6.25x ATR.
- **volatility_bin_q95:** Two volatility groups split at training-fill median ATR/entry. Each group's winner MAE95 stop rounded upward to0.05 points; at least60 training winners per group or use pooledq95. Fitted ATR split: 0.125580% of entry; low stop 0.70%, high stop 1.00%.
- **structure_q95:** Distance from actual entry to opposing completed confirmation-candle edge plus0.25ATR; floor at fitted globalq95, cap1.25%. Invalid opposing edge uses baseline1.25%. Fitted floor: 0.90%.

## Fitting and period checks

Fitting-period candidate: **baseline_v21_g7**. Current selected policy: **baseline_v21_g7**. Diagnostic comparison: **baseline_v21_g7**. Stop parameters are fitted on the first **44 sessions through 5 October 2026**. Candidate selection also uses that fitting period only, requiring net profit at least baseline, lower loss amount and no worse daily-close drawdown.

The selected candidate is then checked on the next **15 sessions, 6–27 October**, the final **seven sessions, 28 October–5 November**, and the full 66-session result. Those checks do not refit thresholds or select a different runner-up. All history has already been inspected in earlier research; these are retrospective stability checks, not untouched forward validation.

### First 44: fitting

| Policy / period | Trades | Wins | Losses | Win rate | Net P&L (Rs) | Loss amount (Rs) | Daily-close drawdown (Rs) |
|:--|--:|--:|--:|--:|--:|--:|--:|
| baseline_v21_g7 | 496 | 356 | 140 | 71.77% | 900,448.07 | 437,666.18 | 9,785.32 |

### Next 15: validation diagnostic

| Policy / period | Trades | Wins | Losses | Win rate | Net P&L (Rs) | Loss amount (Rs) | Daily-close drawdown (Rs) |
|:--|--:|--:|--:|--:|--:|--:|--:|
| baseline_v21_g7 | 191 | 120 | 71 | 62.83% | 207,545.90 | 259,085.81 | 35,384.36 |

### Final seven: later diagnostic

| Policy / period | Trades | Wins | Losses | Win rate | Net P&L (Rs) | Loss amount (Rs) | Daily-close drawdown (Rs) |
|:--|--:|--:|--:|--:|--:|--:|--:|
| baseline_v21_g7 | 103 | 59 | 44 | 57.28% | 69,528.13 | 167,020.35 | 24,615.85 |

### 5 November only

| Policy / period | Trades | Wins | Losses | Win rate | Net P&L (Rs) | Loss amount (Rs) | Daily-close drawdown (Rs) |
|:--|--:|--:|--:|--:|--:|--:|--:|
| baseline_v21_g7 | 19 | 11 | 8 | 57.89% | 27,998.05 | 23,443.12 | 0.00 |

The diagnostic candidate averages Rs 1,490.53 per trade, with median Rs 2,550.00.

## Effects on original winners and losers

Winner/loser labels below describe completed outcomes after replay. They are never used to decide the stop for an individual later trade. A smaller stop can reduce a loss while also stopping a trade that would eventually have recovered.

| Policy | Original winners worsened | Original losers improved | Winners became losses | Losers became wins | Winner P&L change (Rs) | Loser P&L change (Rs) |
|:--|--:|--:|--:|--:|--:|--:|
| baseline_v21_g7 | 0 | 0 | 0 | 0 | +0.00 | +0.00 |
| mae_q90 | 55 | 111 | 55 | 0 | -379,655.32 | +105,124.97 |
| mae_q95 | 25 | 97 | 25 | 0 | -202,413.24 | +30,176.62 |
| mae_q975 | 11 | 58 | 11 | 0 | -103,471.19 | +20,144.44 |
| mae_q99 | 3 | 55 | 3 | 0 | -27,414.20 | -1,942.57 |
| atr_q95 | 25 | 63 | 25 | 0 | -164,834.70 | +20,029.66 |
| volatility_bin_q95 | 21 | 78 | 21 | 0 | -160,174.51 | +33,507.22 |
| structure_q95 | 25 | 97 | 25 | 0 | -202,413.24 | +28,313.59 |

The diagnostic policy improves net profit on 0 days and reduces it on 0 days. Every day, including unchanged and zero-trade days, remains in the tables.

## Source, execution and cost limits

- The initial stop applies from the entry bar using the inherited conservative full-bar, stop-first convention. One-minute OHLC cannot prove whether that bar’s adverse extreme occurred before or after an intrabar entry. This ambiguity can make a tight-stop result conservative; it is not resolved by pretending to know the intrabar path.
- An adverse gap after the entry bar fills at the actual opening price. Native targets and the 15:15 time exit remain unchanged. The native entry-bar open is not used as a stop fill.
- Exposure remains Rs 500,000 per fill, based on Rs 100,000 capital at 5x, with Rs 250 modelled round-trip costs and unlimited concurrent capital. Initial stop distances do not include these costs. Extra adverse exit-slippage sensitivity is reported separately.
- All 73 setup/direction pairs through 14:00 remain configured, with last confirmation at 14:01. Original pairs retain cap 3; the 39 new pairs retain cap 1 and their volume gate.
- Results are retrospective and do not guarantee that only losses can be reduced. LIVE execution, scheduling, approvals and position management are unchanged.
- All 218 stock sources contain all 360 one-minute bars from 09:16 through the strategy exit cutoff 15:15 on 5 November.
- 217 stocks are eligible in the sealed feature ledger. SAIL is excluded for the day because its 09:20 futures OI observation is missing.
- KAYNES is excluded only from 09:20 opening setups for missing opening OI evidence; later setups retain valid source data.
- All 73 setup/direction pairs through 14:00 are evaluated using the same sealed snapshot; all stock futures use November contracts.
- October 1 remains excluded from older history by the immutable source. No November 6 intraday data are included.
- Inherited earlier-source limits remain: some archived cohorts use after-dated membership/full-day eligibility; October 30 reconstructed opening OI does not prove contemporaneous availability.

[Entry excursion study and ambiguity treatment](mae_study.md)

[Current day-wise results](DAY_WISE_RESULTS.md) · [Current CSV](day_wise_results.csv) · [Diagnostic candidate day-wise results](CANDIDATE_DAY_WISE_RESULTS.md) · [Candidate CSV](candidate_day_wise_results.csv)

[All totals](overall.csv) · [All daily rows](daily.csv) · [Per-trade changes](trade_changes.csv) · [Trade effect summary](trade_effect_summary.csv) · [Planned stop levels](stop_levels.csv) · [Cost stress](cost_stress.csv) · [Native parity](baseline_parity.csv) · [Fitted parameters](fit.json) · [Configuration](config.json)
