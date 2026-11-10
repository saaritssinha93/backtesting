# G3 V21: two small confirmation changes

This is an isolated backtest based on uncapped G3 V19. Only two enabled SHORT slots have new confirmation thresholds:

| Slot | Confirmation rule | Frozen | V21 | Absolute change |
|:---|:---|---:|---:|---:|
| 1016_SHORT | Minimum 1m candle body ratio | 0.40 | 0.35 | -0.05 |
| 1046_SHORT | Maximum 1m wick ratio | 0.60 | 0.65 | +0.05 |

Both slots were chosen from dates through 30 September: each had at least two orders failing only that confirmation check within 0.05, and the candidate-only native replay had positive net P&L. The rule is keyed only to setup and completed confirmation data, never to a future mover rank or a stock name. All other G3 V19 rules and portfolio constraints remain fixed.

## Portfolio comparison

| Period | Arm | Sessions | Orders | Executed | Win rate | Net P&L (Rs) | Daily close drawdown (Rs) |
|:---|:---|---:|---:|---:|---:|---:|---:|
| training_through_sep30 | v19_uncapped | 43 | 390 | 332 | 74.70% | 778,438.77 | 15,829.60 |
| training_through_sep30 | v21_selective_confirmation | 43 | 394 | 335 | 74.93% | 786,859.23 | 15,829.60 |
| threshold_eval_oct_nov | v19_uncapped | 21 | 241 | 183 | 65.57% | 226,531.97 | 28,166.32 |
| threshold_eval_oct_nov | v21_selective_confirmation | 21 | 241 | 183 | 65.57% | 226,531.97 | 28,166.32 |
| after_slot_cutoff | v19_uncapped | 5 | 57 | 50 | 60.00% | 27,717.05 | 28,166.32 |
| after_slot_cutoff | v21_selective_confirmation | 5 | 57 | 50 | 60.00% | 27,717.05 | 28,166.32 |
| full_64 | v19_uncapped | 64 | 631 | 515 | 71.46% | 1,004,970.75 | 28,166.32 |
| full_64 | v21_selective_confirmation | 64 | 635 | 518 | 71.62% | 1,013,391.20 | 28,166.32 |

V19's first 63 days reproduce the saved V19 daily P&L and trade counts: **PASS**. The 64th sealed session is 3 November; 4 November is not a completed source day in this replay.

**New orders:** 4 across all 64 sessions; 3 executed. New daily top-two movers selected: 1. A new order may displace another through portfolio capital limits, so its own P&L is not the full portfolio difference.

## Newly admitted orders

| Day | Slot | Stock | Actual confirmation | Frozen threshold | V21 threshold | Portfolio result | Net P&L (Rs) |
|:---|:---|:---|---:|---:|---:|:---|---:|
| 2026-07-30 | 1016_SHORT | BAJFINANCE | 0.3913 | 0.40 | 0.35 | EXECUTED | 3,150.00 |
| 2026-09-03 | 1016_SHORT | MCX | 0.3765 | 0.40 | 0.35 | EXECUTED | 470.45 |
| 2026-09-15 | 1046_SHORT | COCHINSHIP | 0.6429 | 0.60 | 0.65 | EXECUTED | 4,800.00 |
| 2026-09-25 | 1046_SHORT | ETERNAL | 0.6000 | 0.60 | 0.65 | SOURCE_UNFILLED | 0.00 |

**Interpretation:** These settings were chosen using earlier outcomes. The frozen G3 slot allowlist itself used outcomes through 27 October. The five sessions after that date offer only a small later check. If the October-November evaluation period has no newly admitted orders, its identical P&L does not validate the adjustment. A full-day +3% move does not imply a profitable G3 entry.

[Daily results](daily_comparison.csv) | [Added orders](new_orders.csv) | [Full selection audit](selection_audit.csv) | [V19 trades](v19_trades.csv) | [V21 trades](v21_trades.csv)
