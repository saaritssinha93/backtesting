# Entry-relative SL: preserving recovery room

**Separate follow-up to the seven tighter-stop tests.** Those tests increased total loss amounts. This study asks whether keeping the original entry stop in place longer avoids premature exits. It also tests one1.50% stop with explicitly higher initial risk. All four rules apply to all790 entries; the engine never knows which trades will be winners or losers.

LONG SL = actual entry × (1 − stop% /100); SHORT SL = actual entry × (1 + stop% /100). Targets remain unchanged and every position closes by15:15. All73 pairs through14:00 remain configured.

## Full66 sessions through5 November2026

| Policy / period | Trades | Wins | Losses | Win rate | Net P&L (Rs) | Loss amount (Rs) | Daily-close drawdown (Rs) |
|:--|--:|--:|--:|--:|--:|--:|--:|
| Baseline:1.25%, tighten to1% after120 minutes | 790 | 535 | 255 | 67.72% | 1,177,522.10 | 863,772.35 | 35,384.36 |
| Keep1.25% from entry throughout | 790 | 535 | 255 | 67.72% | 1,176,067.71 | 865,226.74 | 31,954.04 |
| 1.25% from entry; tighten to1% only after240 minutes | 790 | 535 | 255 | 67.72% | 1,171,883.88 | 869,410.57 | 34,008.02 |
| Keep1.50% from entry throughout (higher initial risk) | 790 | 540 | 250 | 68.35% | 1,220,580.55 | 837,339.00 | 33,894.47 |

Fit-selected candidate: **baseline_v21_g7**. Current research policy: **baseline_v21_g7**. Qualification: **False**.

No new rule is adopted because: No fit44 model reduced losses while preserving baseline net profit and drawdown..

## Period checks

| Policy / period | Trades | Wins | Losses | Win rate | Net P&L (Rs) | Loss amount (Rs) | Daily-close drawdown (Rs) |
|:--|--:|--:|--:|--:|--:|--:|--:|
| fit_44 / baseline_v21_g7 | 496 | 356 | 140 | 71.77% | 900,448.07 | 437,666.18 | 9,785.32 |
| fit_44 / constant_1p50 | 496 | 358 | 138 | 72.18% | 910,214.75 | 433,544.39 | 11,888.86 |
| validation_15 / baseline_v21_g7 | 191 | 120 | 71 | 62.83% | 207,545.90 | 259,085.81 | 35,384.36 |
| validation_15 / constant_1p50 | 191 | 121 | 70 | 63.35% | 219,768.93 | 248,354.66 | 33,894.47 |
| later_7 / baseline_v21_g7 | 103 | 59 | 44 | 57.28% | 69,528.13 | 167,020.35 | 24,615.85 |
| later_7 / constant_1p50 | 103 | 61 | 42 | 59.22% | 90,596.87 | 155,439.95 | 26,246.72 |
| nov05 / baseline_v21_g7 | 19 | 11 | 8 | 57.89% | 27,998.05 | 23,443.12 | 0.00 |
| nov05 / constant_1p50 | 19 | 11 | 8 | 57.89% | 28,300.36 | 23,140.81 | 0.00 |

Only the first44 sessions select a candidate. The following15 and final7 sessions gate that one candidate separately; a failed candidate is not replaced using later results. All history was previously observed in other research, so none is an untouched forward holdout.

## What changed in trades

| Policy | Baseline winners changed | Baseline losers improved | Losers turned wins | Trades held longer | Worst trade (Rs) |
|:--|--:|--:|--:|--:|--:|
| baseline_v21_g7 | 0 | 0 | 0 | 0 | -6,639.33 |
| constant_1p25 | 0 | 14 | 0 | 37 | -6,639.33 |
| delay_tighten240 | 0 | 11 | 0 | 31 | -6,639.33 |
| constant_1p50 | 0 | 36 | 5 | 89 | -7,750.00 |

Wider stops preserve the original modelled winners here because they cannot trigger earlier and targets are unchanged. Some original losers can lose more; the report compares all of them, not only the recoveries. Holding longer can also increase intraday exposure.

## Risk and data limits

- Keep1.25%: nominal initial stop loss remains Rs6,250 before Rs250 modelled costs. The original two-hour tightening is delayed or removed; time exposed to the initial risk increases.
- Keep1.50%: nominal initial stop loss rises to Rs7,500 before Rs250 costs:20% higher price risk and Rs1,250 additional loss per entry. Price gaps can exceed both nominal limits.
- Fixed exposure Rs500,000 per entry, unlimited concurrent capital; all790 entries and Rs197,500 modelled costs preserved. Extra changed-exit slippage is reported separately at0/1/2/5bps.
- The inherited conservative entry-bar and stop-first assumptions remain. One-minute OHLC does not reveal exact intrabar sequencing or real bid/ask execution. Daily-close drawdown does not measure intraday drawdown.
- Yesterday means5 November; the strategy session is complete through15:15. SAIL retains its source day exclusion and KAYNES its opening09:20 exclusion. October1 remains excluded from older history.
- LIVE execution, approvals and scheduling are unchanged.

[Current day-wise results](DAY_WISE_RESULTS.md) · [Candidate day-wise results](CANDIDATE_DAY_WISE_RESULTS.md) · [All daily rows](daily.csv) · [Trade changes](trade_changes.csv) · [Slippage sensitivity](cost_stress.csv) · [Per-entry stop levels](stop_levels.csv) · [Configuration](config.json) · [Earlier tighter-stop study](../../results_through_20261105/REPORT.md)
