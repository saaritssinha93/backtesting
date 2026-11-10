# V23 indicator-based loss-control study through 2026-11-05

All **790 entries across 66 sessions** are retained. The verified baseline net P&L is **Rs 1,177,522.10**. Rules manage exits after entry; they do not remove a selected stock or increase position size.

**No loss-control alternative qualified for the current V23 backtest.** Baseline exits remain selected, and no loss reduction is claimed for the current mode. The diagnostic candidate below remains exploratory.

Recorded promotion checks: No earlier59 candidate reduced losses while preserving net profit and drawdown..

## All-session comparison

| Policy / period | Trades | Wins | Losses | Win rate | Net P&L (Rs) | Loss amount (Rs) | Daily-close drawdown (Rs) |
|:--|--:|--:|--:|--:|--:|--:|--:|
| baseline_v21_g7 | 790 | 535 | 255 | 67.72% | 1,177,522.10 | 863,772.35 | 35,384.36 |
| sustained_loss_trend20 | 790 | 506 | 284 | 64.05% | 994,908.78 | 960,416.40 | 33,720.72 |
| deep_loss_failure15 | 790 | 521 | 269 | 65.95% | 1,061,509.33 | 934,900.55 | 30,374.71 |
| stalled_loser45 | 790 | 503 | 287 | 63.67% | 1,030,720.90 | 915,924.62 | 35,811.17 |
| recovery_guard | 790 | 517 | 273 | 65.44% | 1,140,545.64 | 860,221.42 | 31,589.16 |
| trend_failure_profit_guard | 790 | 564 | 226 | 71.39% | 1,034,884.96 | 784,095.86 | 30,617.05 |
| late_loss_trend90 | 790 | 477 | 313 | 60.38% | 955,290.62 | 928,887.67 | 35,425.81 |
| recovery_and_profit_guard | 790 | 545 | 245 | 68.99% | 988,841.81 | 793,831.46 | 34,152.40 |

Loss amount is the absolute sum of negative after-cost trade P&L before offsetting winners. Wins and losses exclude exactly flat trades; win rate uses all entries.

## Selection and period checks

Earlier-period candidate: **baseline_v21_g7**. Current selection: **baseline_v21_g7**. Diagnostic comparison: **recovery_guard**. Candidate parameters were fixed before these outcomes were calculated. Selection uses the earlier 59 sessions ending 2026-10-27. The chosen rule is then checked over the later seven and full history without retuning. All history had already been observed in prior research, so these are retrospective stability checks, not untouched validation.

### Earlier 59

| Policy / period | Trades | Wins | Losses | Win rate | Net P&L (Rs) | Loss amount (Rs) | Daily-close drawdown (Rs) |
|:--|--:|--:|--:|--:|--:|--:|--:|
| baseline_v21_g7 | 687 | 476 | 211 | 69.29% | 1,107,993.97 | 696,752.00 | 35,384.36 |
| recovery_guard | 687 | 462 | 225 | 67.25% | 1,091,517.17 | 684,549.37 | 31,589.16 |

### Later seven

| Policy / period | Trades | Wins | Losses | Win rate | Net P&L (Rs) | Loss amount (Rs) | Daily-close drawdown (Rs) |
|:--|--:|--:|--:|--:|--:|--:|--:|
| baseline_v21_g7 | 103 | 59 | 44 | 57.28% | 69,528.13 | 167,020.35 | 24,615.85 |
| recovery_guard | 103 | 55 | 48 | 53.40% | 49,028.47 | 175,672.05 | 27,474.15 |

### November 5

| Policy / period | Trades | Wins | Losses | Win rate | Net P&L (Rs) | Loss amount (Rs) | Daily-close drawdown (Rs) |
|:--|--:|--:|--:|--:|--:|--:|--:|
| baseline_v21_g7 | 19 | 11 | 8 | 57.89% | 27,998.05 | 23,443.12 | 0.00 |
| recovery_guard | 19 | 10 | 9 | 52.63% | 22,748.05 | 24,943.12 | 0.00 |

### First chronological block

| Policy / period | Trades | Wins | Losses | Win rate | Net P&L (Rs) | Loss amount (Rs) | Daily-close drawdown (Rs) |
|:--|--:|--:|--:|--:|--:|--:|--:|
| baseline_v21_g7 | 280 | 195 | 85 | 69.64% | 484,284.55 | 262,273.00 | 7,317.10 |
| recovery_guard | 280 | 190 | 90 | 67.86% | 481,155.40 | 260,106.81 | 9,594.56 |

### Middle chronological block

| Policy / period | Trades | Wins | Losses | Win rate | Net P&L (Rs) | Loss amount (Rs) | Daily-close drawdown (Rs) |
|:--|--:|--:|--:|--:|--:|--:|--:|
| baseline_v21_g7 | 216 | 161 | 55 | 74.54% | 416,163.52 | 175,393.18 | 9,785.32 |
| recovery_guard | 216 | 157 | 59 | 72.69% | 401,518.28 | 176,217.97 | 9,785.32 |

### Last chronological block

| Policy / period | Trades | Wins | Losses | Win rate | Net P&L (Rs) | Loss amount (Rs) | Daily-close drawdown (Rs) |
|:--|--:|--:|--:|--:|--:|--:|--:|
| baseline_v21_g7 | 294 | 179 | 115 | 60.88% | 277,074.02 | 426,106.16 | 35,384.36 |
| recovery_guard | 294 | 170 | 124 | 57.82% | 257,871.96 | 423,896.64 | 31,589.16 |

## Changes to existing winners and losers

These outcome labels are used only for this report, never to decide an exit. Preserving total net profit does not preserve every original winner or every day.

| Policy | Original winners worsened | Original losers improved | Winners became losses | Losers became wins | Winner P&L change (Rs) | Loser P&L change (Rs) |
|:--|--:|--:|--:|--:|--:|--:|
| baseline_v21_g7 | 0 | 0 | 0 | 0 | +0.00 | +0.00 |
| sustained_loss_trend20 | 29 | 85 | 29 | 0 | -204,839.56 | +22,226.24 |
| deep_loss_failure15 | 14 | 70 | 14 | 0 | -115,865.19 | -147.57 |
| stalled_loser45 | 32 | 41 | 32 | 0 | -190,045.03 | +43,243.84 |
| recovery_guard | 18 | 16 | 18 | 0 | -67,596.04 | +30,619.58 |
| trend_failure_profit_guard | 73 | 29 | 0 | 29 | -229,563.62 | +86,926.49 |
| late_loss_trend90 | 58 | 86 | 58 | 0 | -299,048.86 | +76,817.38 |
| recovery_and_profit_guard | 88 | 41 | 17 | 27 | -290,939.83 | +102,259.54 |

The diagnostic candidate changes net profit on 11 days for the better and 19 days for the worse. Unchanged days remain in the day-wise tables.

## Indicator and rule definitions

EMA9/EMA20, RSI14 and efficiency ratio ER10 here use each trade execution path starting after its completed confirmation. They are **confirmation-anchored indicators**, not full-session or overnight-continuous platform readings. Only completed prefix bars are used, and warm-up requirements must be met. ER measures price progress relative to the sum of absolute price changes; RSI and direction tests are normalized for long versus short trades.

Maximum favorable/adverse excursions use observed post-entry bars. The entry-bar high/low cannot activate a new guard. Newly observed exit or stop decisions apply on the following bar; opening gaps can fill worse than a stop price. Native targets, staged stops, stop-first ties and the 15:15 time exit remain the reference conventions.

- **baseline_v21_g7:** Original target, staged 1.25% stop tightening to 1% after120 minutes, and15:15 exit; all entries retained.
- **sustained_loss_trend20:** After20 minutes, return<=-0.60%, signedER10<=-0.35, adverseEMA9 position and adverse3-barEMA9 slope for3 completed bars: exit atnextopen.
- **deep_loss_failure15:** After15 minutes, return<=-0.90%, signedER10<=-0.25, directionalRSI14<=40 and adverseEMA9 position: exit atnextopen.
- **stalled_loser45:** After45 minutes, return<=-0.40%, observed post-entryMFE<0.20%, absER10<=0.20: exit atnextopen.
- **recovery_guard:** After observing post-entryMAE>=0.80%, completed close recovery>=-0.10% activates an entry-relative -0.25%stop fromnextbar.
- **trend_failure_profit_guard:** Observedpost-entryMFE>=0.60%, close>=+0.15%, directionalRSI14<50 and adverse3-barEMA9slope activates entry-relative+0.10%stop fromnextbar.
- **late_loss_trend90:** After90 minutes, return<=-0.30%, adverseEMA9vsEMA20 andclosevsEMA20, directionalRSI14<=45: exit atnextopen.
- **recovery_and_profit_guard:** Union of unchanged recovery_guard andtrend_failure_profit_guard; stops may tighten but never loosen.

## Coverage and cost assumptions

- All 73 setup/direction pairs through the 14:00 signal remain configured. Original pairs retain cap 3; the 39 new pairs retain cap 1 and their volume gate. Final confirmation is 14:01 and execution runs through 15:15.
- Fixed exposure is Rs 500,000 per entry, Rs 100,000 capital at 5x, with Rs 250 modelled round-trip costs and unlimited concurrent capital. No position-size increase pays for a loss reduction.
- Extra adverse exit slippage is reported separately in the cost-stress file. It is not silently added to, or omitted from, the baseline comparison.
- One-minute OHLC cannot reveal the exact intrabar path or executable bid/ask prices. All results are retrospective; LIVE execution, approvals and scheduling are unchanged.
- All 218 stock sources contain all 360 one-minute bars from 09:16 through the strategy exit cutoff 15:15 on 5 November.
- 217 stocks are eligible in the sealed feature ledger. SAIL is excluded for the day because its 09:20 futures OI observation is missing.
- KAYNES is excluded only from 09:20 opening setups for missing opening OI evidence; later setups retain valid source data.
- All 73 setup/direction pairs through 14:00 are evaluated using the same sealed snapshot; all stock futures use November contracts.
- October 1 remains excluded from older history by the immutable source. No November 6 intraday data are included.
- Inherited earlier-source limits remain: some archived cohorts use after-dated membership/full-day eligibility; October 30 reconstructed opening OI does not prove contemporaneous availability.

[Current day-wise results](DAY_WISE_RESULTS.md) · [Current day-wise CSV](day_wise_results.csv) · [Diagnostic day-wise results](CANDIDATE_DAY_WISE_RESULTS.md) · [Diagnostic day-wise CSV](candidate_day_wise_results.csv)

[All totals](overall.csv) · [All daily results](daily.csv) · [Per-trade changes](trade_changes.csv) · [Trade effect summary](trade_effect_summary.csv) · [Cost stress](cost_stress.csv) · [Configuration](config.json)
