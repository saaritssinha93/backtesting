# G3 V24 RSI entry-timing experiment

Completed on 10 November 2026 using the same 68 completed sessions, 29 July through 9 November, as the V24 reference. November 10 was still in progress when the study started.

**Result: retain the existing entry timing.** The proposed RSI reset-and-recovery rule reduced fills from 824 to 119, win rate from 65.66% to 60.50%, and net modeled profit from Rs 1,221,079.86 to Rs 106,976.45. The control that also requires the original G3 trigger filled 50 trades, won 64.00%, and earned Rs 59,539.84. Neither meets the objective of reducing losses while preserving profit and trading activity.

- [Full comparison, including LONG/SHORT and period results](results_through_20261109/REPORT.md)
- [All 68 day-wise results](results_through_20261109/DAY_WISE_RESULTS.md)
- [Every missed, new and retimed trade](results_through_20261109/trade_attribution.csv)
- [Entry-event status counts](results_through_20261109/order_status_counts.csv)
- [Frozen configuration](results_through_20261109/config.json)
- [Verification](results_through_20261109/verification.json)

## Exact hypothesis tested

All 930 G3/V23 admitted stock/slot/direction orders are retained as opportunities. G3 selection, G7 confirmations, 34 original pair caps of three, 39 new pair caps of one, and configured slots through 14:00 remain fixed. No replacement stock is promoted when an entry does not fill.

1. Starting at the completed G3 confirmation candle, observe completed one-minute bars in chronological order. RSI(14) uses Wilder smoothing, its arithmetic seed, and prior completed sessions for warmup.
2. Arm a reset when RSI is in the inclusive 45–55 zone and the close moves against the proposed trade: lower than the previous close for LONG, higher for SHORT.
3. Require a later fresh crossing above 55 for LONG or below 45 for SHORT. The reset remains armed until that crossing or expiry.
4. Freeze an entry trigger at that completed crossing candle's high for LONG or low for SHORT. It may fill only on a subsequent bar, through the original confirmation plus ten minutes. A gap fills at the worse open.
5. In the control, also require the original G3 trigger: use the higher trigger for LONG or lower for SHORT. No thresholds were optimized.

Targets and exits retain the adopted V23 rules: native target, 1.50% initial stop, and entry plus 0.05% signed-return protection activated on the bar after a completed post-entry close reaches 80% of the target. Prices rebase to the actual new entry. Exposure remains Rs 500,000 per fill, costs Rs 250 per round trip, with unlimited concurrent capital. Conservative same-bar stop/target handling and the 15:15 exit are retained.

## Why it reduced activity

All 930 orders had usable RSI data for their full decision windows. In the main timing arm, 657 never reset, 128 reset without recovering, and 26 recovered without a subsequent price-trigger touch before expiry. Only 119 filled. It missed 487 baseline winners as well as 217 baseline losers; the reduction in aggregate losses therefore does not establish better selection or timing.

The 98 baseline opportunities that also filled under the timing rule gained Rs 33,637.79 through retiming. That conditional subset must not be added selectively to the unchanged baseline: membership depends on subsequent observations, and doing so would introduce hindsight. The full causal strategy, including missed trades and 21 newly filled opportunities, lost Rs 1,114,103.41 relative to baseline.

## Verification and reproduction

All 28 new automated tests pass. The original 824 baseline trades reproduce exactly, including entries, exits and net PnL. Indicator-window prices and times reconcile with the frozen execution bars. Reports reconcile every day and attribute the complete PnL difference to missed, new and retimed fills.

```powershell
python -m pytest g3_v24_selection_20261109/rsi_timing_20261110 -q
python -m g3_v24_selection_20261109.rsi_timing_20261110.run --output g3_v24_selection_20261109/rsi_timing_20261110/results_recheck
```

Use a fresh output-directory name. The study consumes the published, hash-verified V24 execution caches and the same sealed stock histories; it does not reconstruct or overwrite historical source data. Cache ancestry remains documented. Existing results and live/paper settings are unchanged.

All results are retrospective. The reference's existing source/universe limitations remain: October 1 and the November 8 special session are excluded, October 28 has partial expanded-stock coverage, and older universes are not strictly point-in-time.
