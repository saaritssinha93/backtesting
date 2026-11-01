# 09:31 and 10:01 LONG replacement-aware audit

## Decision

No predeclared rule qualified under the frozen missing-slot addendum. Retain the accepted baseline for all four combinations:

- `0931_LONG / G3`: baseline
- `0931_LONG / G7_V4_LOCK2`: baseline
- `1001_LONG / G3`: baseline
- `1001_LONG / G7_V4_LOCK2`: baseline

None of the results supports a reliable win rate above 75%. The few point estimates above 75% came from only one to three trades, and every Wilson 95% lower bound remained far below 75%.

## Method and parity

The frozen `G3_G7_V4_LOCK_MULTI_MISSING_SLOTS_PREQUOTA_V1` grid was applied to sealed pre-quota candidates before native ranking and quota, allowing replacements. The LONG move rule used the absolute confirmation-time move magnitude. Every changed selection was simulated from its one-minute path and inserted into a fresh full-portfolio allocation.

G7 retained its existing `1001_LONG` maximum confirmation extension of 4 ATR before ranking. It then used freshly reconstructed Nifty permission and low-efficiency decisions for every baseline and replacement opportunity, followed by +0.6%/+0.1% protection and the causal two-realized-loss lock. Across the 13 unique G7 opportunities selected by any variant, Nifty allowed 11 and blocked 2; the low-efficiency rule vetoed none.

All six replay checks passed: the accepted G3 allocator and G7 protection/lock controls, plus fresh baseline replacement of each of the two setups in each strategy. The checks covered 626 G3 and 595 G7 portfolio rows. `selected_variant_daywise.csv` also reconciles exactly to the 5 bps baseline metrics for all four groups and all seven reported periods: 28 of 28 checks passed in `daywise_baseline_verification.json`.

The sealed pools contained five `0931_LONG` candidates across four sessions and ten `1001_LONG` candidates across ten sessions. Baseline ranking selected 4 and 10 for G3. G7 selected 4 and 8 because its existing 4 ATR `1001_LONG` guard rejected two rows above 4.6 ATR. This is sparse evidence, especially for 09:31.

One simulator schema condition required normalization in this isolated runner. When every selected opportunity in a variant was unfilled, the native simulator emitted the rows but omitted fill-only columns. The runner added explicit `NaN`/`NaT` values for `entry_path_index`, `entry_price`, and `entry_ts` only when all rows were unfilled. It raises if a filled row lacks them. No candidate, fill, rule, or outcome was removed or changed.

## Development gates

Every nonbaseline rule failed the minimum of eight executed development trades. Therefore no later-period result was used to choose a replacement.

| Setup | Strategy | Baseline development | Maximum nonbaseline trades | Rules passing >75% point WR | Rules passing all gates |
|---|---|---:|---:|---:|---:|
| 09:31 LONG | G3 | 4 trades, 2-2 | 3 | 0/11 | 0/11 |
| 09:31 LONG | G7 V4 lock2 | 4 trades, 2-2 | 3 | 0/11 | 0/11 |
| 10:01 LONG | G3 | 7 trades, 4-3 | 7 | 5/11 | 0/11 |
| 10:01 LONG | G7 V4 lock2 | 5 trades, 4-1 | 5 | 8/11 | 0/11 |

The exact gate pass counts below show why a high point win rate alone was insufficient. Each value is the number of nonbaseline rules passing that individual gate, out of 11.

| Setup | Strategy | Min 8 trades | WR >75% | Setup net not below baseline | Loss amount reduced | Portfolio net not below baseline |
|---|---|---:|---:|---:|---:|---:|
| 09:31 LONG | G3 | 0 | 0 | 0 | 9 | 0 |
| 09:31 LONG | G7 V4 lock2 | 0 | 0 | 2 | 9 | 2 |
| 10:01 LONG | G3 | 0 | 5 | 2 | 9 | 2 |
| 10:01 LONG | G7 V4 lock2 | 0 | 8 | 1 | 7 | 4 |

No single rule passed in both strategies for either setup.

## Baseline results at 5 bps

| Setup | Strategy | Trades | W-L | Win rate | Wilson 95% lower | Net PnL |
|---|---|---:|---:|---:|---:|---:|
| 09:31 LONG | G3 | 4 | 2-2 | 50.00% | 15.00% | Rs 11,781.95 |
| 09:31 LONG | G7 V4 lock2 | 4 | 2-2 | 50.00% | 15.00% | Rs 2,624.58 |
| 10:01 LONG | G3 | 9 | 5-4 | 55.56% | 26.67% | Rs 22,510.81 |
| 10:01 LONG | G7 V4 lock2 | 6 | 4-2 | 66.67% | 30.00% | Rs 16,713.61 |

## Closest rejected rules

`1001_LONG / G3 / extension_le_3p5` was the closest development result. It passed every gate except sample size: 3 wins from 3 trades, 100% point win rate, 43.85% Wilson lower bound, Rs 29,984.07 setup net, Rs 2,250.00 above setup baseline, Rs 17,000.00 less losing amount, and Rs 786.78 more full-portfolio net. It cannot qualify with only three executed trades. In later data it removed the sole validation winner but retained the assessment loss: 0-1 and -Rs 5,250.00 in later/October, Rs 26.74 below baseline. Across all sessions it was 3-1, exactly 75%, with Rs 24,734.07.

`1001_LONG / G7 V4 lock2 / extension_le_3p5` had 3 wins from 3 development trades and removed Rs 5,113.19 of development losses. It failed the eight-trade gate and reduced development setup net by Rs 4,598.52, although full-portfolio net improved by Rs 5,125.26 because allocation changed elsewhere. It retained the later/October loss and finished all sessions at 3-1, exactly 75%, with Rs 12,115.09.

`1001_LONG / G7 V4 lock2 / move_magnitude_le_0p50` is the tempting retrospective result: 2 wins from 2 trades overall, Rs 398.45 above the setup baseline, and Rs 4,872.53 above the full-portfolio baseline. On development alone it had only two trades, setup net was Rs 4,848.51 below baseline, and portfolio net was Rs 374.43 below baseline. Its all-period improvement came from deleting the single later/October loss. This is not an honest promotion candidate.

No `0931_LONG` rule produced a point win rate above 50%, and no later or October observations exist for this setup. Its four executed baseline trades all occurred in development, so later stability cannot be assessed.

## Later and October baseline behavior

| Setup | Strategy | Validation | Assessment | Later | October |
|---|---|---|---|---|---|
| 09:31 LONG | G3 | 0 trades | 0 trades | 0 trades | 0 trades |
| 09:31 LONG | G7 V4 lock2 | 0 trades | 0 trades | 0 trades | 0 trades |
| 10:01 LONG | G3 | 1-0, Rs 26.74 | 0-1, -Rs 5,250.00 | 1-1, -Rs 5,223.26 | 1-1, -Rs 5,223.26 |
| 10:01 LONG | G7 V4 lock2 | 0 trades | 0-1, -Rs 5,246.96 | 0-1, -Rs 5,246.96 | 0-1, -Rs 5,246.96 |

The later evidence is negative and too small to validate any rule. October is the same inventory as the later rows for these setups.

## Baseline cost stress

Because no variant qualified, this is the effective selected result at each modeled cost.

| Setup | Strategy | 5 bps | 9 bps | 15 bps |
|---|---|---|---|---|
| 09:31 LONG | G3 | 2-2, Rs 11,781.95 | 2-2, Rs 10,981.95 | 2-2, Rs 9,781.95 |
| 09:31 LONG | G7 V4 lock2 | 2-2, Rs 2,624.58 | 2-2, Rs 1,839.05 | 1-3, Rs 660.77 |
| 10:01 LONG | G3 | 5-4, Rs 22,510.81 | 4-5, Rs 20,710.81 | 4-5, Rs 18,010.81 |
| 10:01 LONG | G7 V4 lock2 | 4-2, Rs 16,713.61 | 4-2, Rs 15,519.89 | 3-3, Rs 13,729.32 |

Higher costs turn small protected profits into losses and can change the causal G7 lock path. The net PnL remains positive, but the already weak setup win rates become less favorable.

## Artifacts

- `development_selection.csv`: every rule and exact development failure reasons
- `variant_setup_metrics.csv`: setup metrics for every rule, period, and cost
- `variant_portfolio_metrics.csv`: full-portfolio metrics for every rule, period, and cost
- `candidate_variant_decisions.csv`: pre-ranking gate, ranks, replacements, and selections
- `variant_trade_details.csv`: freshly simulated target-slot rows
- `selected_variant_daywise.csv`: effective baseline daywise results
- `baseline_reproduction.json`: six exact replay parity checks
- `daywise_baseline_verification.json`: 28 daywise-to-period reconciliation checks
- `g7_candidate_features.csv`: causal Nifty and low-efficiency decisions
- `source_provenance.json` and `artifact_manifest.json`: input and artifact hashes

This was a backtest-only audit. No main strategy file or active configuration was changed.
