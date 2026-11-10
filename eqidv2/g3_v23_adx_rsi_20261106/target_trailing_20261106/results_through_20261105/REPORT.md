# V23 target-relative trailing study through 2026-11-05

All **790 entries across 66 sessions** from 2026-07-29 through 2026-11-05 are retained. The comparison baseline is the adopted **fixed 1.50% SL**, with net profit **Rs 1,220,580.55** after modelled costs. Every alternative retains the original profit target and starts with the same 1.50% stop. Entry selection, fill prices, times and position sizes are unchanged.

**No early-trailing alternative qualified for replacement.** Fixed 1.50% remains the research-selected V23 backtest policy. Alternative results are retained as research comparisons, including any improvements that did not survive every period check.

The current V23 default remains **fixed1p50**, regardless of this research selection. No current-policy pointer or live execution setting is changed by this study.

Recorded checks: No fit44 trailing model improved losses while preserving fixed1.50% baseline profit and drawdown.

## Full-history comparison

| Policy / period | Trades | Wins | Losses | Win rate | Net P&L (Rs) | Loss amount (Rs) | Daily-close drawdown (Rs) |
|:--|--:|--:|--:|--:|--:|--:|--:|
| fixed_1p50 | 790 | 540 | 250 | 68.35% | 1,220,580.55 | 837,339.00 | 33,894.47 |
| cushion_half_target | 790 | 539 | 251 | 68.23% | 1,202,836.30 | 851,333.25 | 38,506.43 |
| cushion_threequarter_target | 790 | 537 | 253 | 67.97% | 1,202,798.34 | 849,755.04 | 34,707.74 |
| breakeven_eighty_target | 790 | 520 | 241 | 65.82% | 1,202,036.50 | 808,171.11 | 31,338.39 |
| target_steps | 790 | 529 | 253 | 66.96% | 1,187,676.83 | 846,254.44 | 30,756.43 |
| patient_half_cushion | 790 | 540 | 250 | 68.35% | 1,215,316.27 | 842,603.28 | 37,207.74 |
| target_atr_hybrid | 790 | 548 | 242 | 69.37% | 1,176,254.05 | 811,976.59 | 33,894.47 |

Loss amount is the absolute sum of negative after-cost trade P&L before offsetting winners. Flat trades are neither wins nor losses; win rate uses all fills. Daily-close drawdown is not intraday drawdown.

## Expectancy and payoff

| Policy | Mean net/trade (Rs) | Median net/trade (Rs) | Profit factor | Mean winner (Rs) | Mean losing amount (Rs) |
|:--|--:|--:|--:|--:|--:|
| fixed_1p50 | 1,545.04 | 2,550.00 | 2.46 | 3,810.96 | 3,349.36 |
| cushion_half_target | 1,522.58 | 2,550.00 | 2.41 | 3,811.08 | 3,391.77 |
| cushion_threequarter_target | 1,522.53 | 2,550.00 | 2.42 | 3,822.26 | 3,358.72 |
| breakeven_eighty_target | 1,521.57 | 2,542.90 | 2.49 | 3,865.78 | 3,353.41 |
| target_steps | 1,503.39 | 2,550.00 | 2.40 | 3,844.86 | 3,344.88 |
| patient_half_cushion | 1,538.38 | 2,550.00 | 2.44 | 3,810.96 | 3,370.41 |
| target_atr_hybrid | 1,488.93 | 2,279.67 | 2.45 | 3,628.16 | 3,355.28 |

## Why these triggers differ

The previous R-based experiment waited for a move measured against the 1.50% initial stop. Many original targets are smaller than 1R. These new rules instead measure completed-close progress toward each trade's original target T, allowing protection before the target is hit.

For example, with a 0.80% target, half-target progress is a favourable 0.40% close. The half-target cushion then moves the stop from -1.50% to -1.00% of entry. It limits remaining risk while leaving pullback room; it does not lock in a profit. A +0.05% stop covers the modelled round-trip costs at an exact fill, but gaps or extra slippage can still lose money.

All six alternatives retain the native target throughout the trade. No target is removed, no initial stop is tightened before entry, and no partial position is sold.

- **fixed_1p50:** Current fixed1.50% initial stop, original target and15:15 time exit; no timed tightening.
- **cushion_half_target:** After a completed postentry close reaches50% of the original target distance, tighten stop to-1.00% of entry from the next bar.
- **cushion_threequarter_target:** After a completed postentry close reaches75% of the original target distance, tighten stop to-0.75% from the next bar.
- **breakeven_eighty_target:** After a completed postentry close reaches80% of the original target distance, tighten stop to+0.05% entry return to cover modeled costs at an exact fill.
- **target_steps:** Completed-close stages:50% of original target=>-1.00% entry stop;75%=>-0.50%;90%=>+0.05%. Stops only tighten.
- **patient_half_cushion:** Wait at least5 completed postentry bars and2 consecutive completed closes at or above50% of original target, then tighten to-1.00% entry stop.
- **target_atr_hybrid:** After at least5 completed postentry bars and2 consecutive closes at or above75% of original target, arm a next-bar trail: maximum of previous stop,-0.75%,and best completed postentry close return minus max(0.35%,3ATR/entry*100). Keep ratcheting after activation.

The patient variants require at least five completed post-entry bars and two consecutive qualifying closes. The ATR hybrid trails the best completed post-entry close with a distance of max(0.35% of entry, 3 ATR), starting from a -0.75% stop floor. ATR uses the causal confirmation-path prefix, with verified confirmation ATR during warm-up. It is not a full-session or overnight ATR definition.

Entry-bar extrema and entry-bar closes cannot activate a new guard. All newly observed stop updates apply from the next bar and can only tighten. Native targets remain active; adverse opening gaps fill at the open, ambiguous stop/target bars use stop-first handling, and every position still exits by 15:15.

## Selection and period checks

Fitting-period candidate: **fixed_1p50**. Research-selected policy: **fixed_1p50**. Diagnostic comparison: **breakeven_eighty_target**. Selection uses only the first44 sessions through 5 October, requiring net profit at least the fixed1.50 baseline, strictly lower loss amount and no worse daily-close drawdown. The single chosen candidate is then checked separately on the next15 sessions through 27 October, final7 through 5 November, and full66. A failed candidate is not replaced using later outcomes.

**All 66 sessions were already observed in prior research.** These are retrospective stability checks, not an untouched forward holdout. Multiple experiments increase the risk of finding a historical coincidence; these results do not guarantee improvement.

### First44: selection

| Policy / period | Trades | Wins | Losses | Win rate | Net P&L (Rs) | Loss amount (Rs) | Daily-close drawdown (Rs) |
|:--|--:|--:|--:|--:|--:|--:|--:|
| fixed_1p50 | 496 | 358 | 138 | 72.18% | 910,214.75 | 433,544.39 | 11,888.86 |
| breakeven_eighty_target | 496 | 345 | 134 | 69.56% | 889,147.16 | 426,032.89 | 15,638.86 |

### Next15: validation

| Policy / period | Trades | Wins | Losses | Win rate | Net P&L (Rs) | Loss amount (Rs) | Daily-close drawdown (Rs) |
|:--|--:|--:|--:|--:|--:|--:|--:|
| fixed_1p50 | 191 | 121 | 70 | 63.35% | 219,768.93 | 248,354.66 | 33,894.47 |
| breakeven_eighty_target | 191 | 120 | 67 | 62.83% | 224,593.11 | 239,930.47 | 31,338.39 |

### Final7: later check

| Policy / period | Trades | Wins | Losses | Win rate | Net P&L (Rs) | Loss amount (Rs) | Daily-close drawdown (Rs) |
|:--|--:|--:|--:|--:|--:|--:|--:|
| fixed_1p50 | 103 | 61 | 42 | 59.22% | 90,596.87 | 155,439.95 | 26,246.72 |
| breakeven_eighty_target | 103 | 55 | 40 | 53.40% | 88,296.23 | 142,207.74 | 26,246.72 |

### 5 November only

| Policy / period | Trades | Wins | Losses | Win rate | Net P&L (Rs) | Loss amount (Rs) | Daily-close drawdown (Rs) |
|:--|--:|--:|--:|--:|--:|--:|--:|
| fixed_1p50 | 19 | 11 | 8 | 57.89% | 28,300.36 | 23,140.81 | 0.00 |
| breakeven_eighty_target | 19 | 9 | 8 | 47.37% | 20,200.36 | 23,140.81 | 0.00 |

## Effect on existing winners and losers

These outcome labels are computed after replay for diagnosis; they never determine an entry or exit decision. Preserving aggregate profit does not preserve every original winner.

| Policy | Baseline winners worsened | Baseline losers improved | Winners became losses | Losers became wins | Winner P&L change (Rs) | Loser P&L change (Rs) |
|:--|--:|--:|--:|--:|--:|--:|
| fixed_1p50 | 0 | 0 | 0 | 0 | +0.00 | +0.00 |
| cushion_half_target | 1 | 3 | 1 | 0 | -9,000.00 | -8,744.25 |
| cushion_threequarter_target | 3 | 4 | 3 | 0 | -17,366.17 | -416.04 |
| breakeven_eighty_target | 20 | 10 | 1 | 0 | -48,238.84 | +29,694.79 |
| target_steps | 11 | 8 | 6 | 0 | -42,988.28 | +10,084.57 |
| patient_half_cushion | 0 | 3 | 0 | 0 | +0.00 | -5,264.28 |
| target_atr_hybrid | 38 | 8 | 0 | 8 | -82,115.85 | +37,789.35 |

## Activation and exit diagnostics

| Policy | Trades with activation | Stop updates | Changed exits | Exit followed by later original-target touch |
|:--|--:|--:|--:|--:|
| fixed_1p50 | 0 | 0 | 0 | 2 |
| cushion_half_target | 450 | 450 | 12 | 3 |
| cushion_threequarter_target | 309 | 309 | 11 | 3 |
| breakeven_eighty_target | 262 | 262 | 30 | 12 |
| target_steps | 450 | 828 | 26 | 6 |
| patient_half_cushion | 389 | 389 | 8 | 2 |
| target_atr_hybrid | 205 | 658 | 56 | 33 |

A later original-target touch and full-session MFE use future prices only for post-replay diagnostics. Neither is a trading signal, and a later target touch does not prove that holding through the intervening drawdown was safely executable.

The diagnostic comparison improves net P&L on 9 days and worsens it on 15 days. All dates, including unchanged and zero-trade dates, remain in the day-wise reports.

## Costs, source limits and scope

- Fixed Rs500,000 exposure per entry, Rs100,000 capital at5x, unlimited concurrent capital and Rs250 modelled round-trip costs per fill. Extra adverse slippage of0,1,2 and5bps on changed exits is a sensitivity check, not a selection input.
- The initial1.50% stop permits Rs7,500 nominal price loss plus Rs250 costs per trade. Adverse gaps can exceed this amount; a stop is not a guaranteed fill price.
- All73 setup/direction pairs through14:00 remain enabled, with final confirmation at14:01. Original pairs retain cap3; new pairs retain cap1 and their existing volume gate.
- Kite historical candles provide timestamp and OHLCV fields (with optional OI); the inference from this candle format is that it cannot establish exact intrabar ordering, bid/ask spreads or queue position. This motivates the next-bar and stop-first conventions. [Kite historical-data documentation](https://kite.trade/docs/connect/v3/historical/). No tick, volume-delta or volatility-index information is inferred from these paths; no new API data fetch was required for this replay.
- LIVE execution, approvals, scheduling and broker order handling are unchanged.
- All 218 stock sources contain all 360 one-minute bars from 09:16 through the strategy exit cutoff 15:15 on 5 November.
- 217 stocks are eligible in the sealed feature ledger. SAIL is excluded for the day because its 09:20 futures OI observation is missing.
- KAYNES is excluded only from 09:20 opening setups for missing opening OI evidence; later setups retain valid source data.
- All 73 setup/direction pairs through 14:00 are evaluated using the same sealed snapshot; all stock futures use November contracts.
- October 1 remains excluded from older history by the immutable source. No November 6 intraday data are included.
- Inherited earlier-source limits remain: some archived cohorts use after-dated membership/full-day eligibility; October 30 reconstructed opening OI does not prove contemporaneous availability.

[Research-selected day-wise results](DAY_WISE_RESULTS.md) | [Research-selected CSV](day_wise_results.csv) | [Candidate day-wise results](CANDIDATE_DAY_WISE_RESULTS.md) | [All policies](overall.csv) | [All daily rows](daily.csv) | [Every trade comparison](trade_changes.csv) | [Extra-slippage sensitivity](cost_stress.csv) | [Stop updates](stop_updates.csv) | [Configuration](config.json) | [Research-selected trades](selected_trades.csv)
