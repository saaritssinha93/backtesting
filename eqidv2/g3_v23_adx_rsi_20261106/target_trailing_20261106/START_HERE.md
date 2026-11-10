# V23 earlier target-based trailing study

Six additional trailing rules were tested against the current fixed 1.50% stop on the same 790 fills across 66 sessions, 29 July-5 November 2026. Every original target, entry, position size and cost assumption is retained. None passed the predeclared selection checks; the current V23 backtest remains fixed1p50.

| Policy | Net profit after costs | Loss amount | Win rate | Daily-close drawdown |
|:--|--:|--:|--:|--:|
| Fixed 1.50% baseline | Rs 1,220,580.55 | Rs 837,339.00 | 68.35% | Rs 33,894.47 |
| Cost-covered entry stop after 80% target progress | Rs 1,202,036.50 | Rs 808,171.11 | 65.82% | Rs 31,338.39 |
| Delayed target/ATR hybrid | Rs 1,176,254.05 | Rs 811,976.59 | 69.37% | Rs 33,894.47 |

The 80%-target rule is a compromise: loss amount falls by Rs 29,167.90 and daily-close drawdown by Rs 2,556.08, but net profit falls by Rs 18,544.05 (1.52%). It reduces 20 original winners' profits while improving 10 original losers. The ATR hybrid turns 8 losers into winners, but reduces 38 original winners' profits; net profit falls by Rs 44,326.50 (3.63%). Neither is an improvement that preserves profit.

## What the rules do

For the 80%-target rule, retain the initial 1.50% SL and original target. Once a completed postentry one-minute candle closes at least 80% of the way to that target, set a stop covering the modeled cost, effective from the next bar. For a LONG at Rs 100 with a 0.80% target, a close at Rs 100.64 or higher triggers a stop at Rs 100.05. The original Rs 100.80 target stays active. SHORT logic is mirrored. A gap or extra slippage can still make this stop lose money.

The delayed ATR hybrid waits for at least five completed postentry candles and two consecutive closes at or above 75% of the target distance. It then trails the best completed close by the larger of 0.35% or three ATR, with a minimum tightened stop of -0.75% from entry. Stops never loosen. All actions start from the following bar; the entry bar cannot activate them.

The other tested rules were 50%-target progress tightening to -1.00%, 75%-target progress tightening to -0.75%, staged tightening at 50/75/90% progress, and delayed 50%-target tightening. All six parameter sets were frozen before this replay; no additional grid search followed the results.

## Reading the results

Loss amount is the absolute sum of negative after-cost trade P&L before offsetting winners. Fixed exposure is Rs 500,000 per trade, with Rs 250 modeled round-trip costs and unlimited concurrent capital. The breakeven rule has 29 flat trades; they are not counted as wins. Adding just one basis point of adverse slippage on changed exits turns those 29 flat trades into losses and reduces that model's net profit to Rs 1,200,536.50. Drawdown here is measured at daily close, not intraday.

The first 44 sessions alone were used for candidate selection. No alternative both preserved profit/drawdown and reduced loss amount there. The next 15 sessions, final 7 and full history are reported separately. All periods had been observed in earlier research, so this is retrospective evidence, not an untouched holdout or a forward guarantee. All 31 tests and two subtests passed, and the 790 fixed1.50 baseline exits reproduce exactly.

[Full report and all six alternatives](results_through_20261105/REPORT.md) | [80%-target candidate day-wise comparison](results_through_20261105/CANDIDATE_DAY_WISE_RESULTS.md) | [Current baseline day-wise results](results_through_20261105/DAY_WISE_RESULTS.md) | [Every policy/day](results_through_20261105/daily.csv) | [Slippage sensitivity](results_through_20261105/cost_stress.csv) | [Frozen protocol](protocol.json)

The source paths are one-minute candles. Kite documents candle fields in its [historical-data API](https://kite.trade/docs/connect/v3/historical/); these records do not establish intrabar tick ordering. The replay therefore uses next-bar stop changes and conservative stop-first ordering when both target and stop are touched. No new market-data fetch was performed for this comparison.

Reproduce separately without overwriting a saved result:

```powershell
python -m g3_v23_adx_rsi_20261106.target_trailing_20261106.run --output g3_v23_adx_rsi_20261106/target_trailing_20261106/recheck
python -m pytest g3_v23_adx_rsi_20261106/target_trailing_20261106 -q
```

Existing results and the current default runner are preserved. LIVE execution is unchanged.
