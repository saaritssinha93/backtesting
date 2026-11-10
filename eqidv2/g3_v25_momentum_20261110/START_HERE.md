# G3 V25: full-universe momentum research

**Result: no tested replacement improved the baseline sufficiently to qualify.** Eight new strategy variants were tested across 68 available sessions through November 9, 2026. Every variant lost money over the complete history. Existing G3 versions and live/paper settings remain unchanged.

The study covers **14,353 stock-days**, including **652 daily top-five** and **271 daily top-two** long/short mover cases reaching at least 3%. Full-day mover labels were used only after causal entries and fills were fixed.

| Strategy | Fills | Win rate | Net PnL | Daily-close drawdown | Top-five cases traded |
|:--|--:|--:|--:|--:|--:|
| Unchanged G3 baseline | 824 | 65.66% | Rs 1,221,079.86 | Rs 31,338.39 | 124 / 652 |
| Broad breakout, 1.5% target | 816 | 47.43% | Rs -22,619.87 | Rs 207,710.73 | 202 / 652 |
| Leader pullback, 1.5% target | 814 | 47.67% | Rs -92,644.36 | Rs 228,858.20 | 89 / 652 |

The broad breakout path captured more movers but lost Rs 793,220.56 on its other trades. Meanwhile, G3 earned **Rs 686,034.94 outside the daily top-five lists**, representing **56.18% of baseline net profit**. Avoiding all smaller movers would remove much of its existing edge.

Leader pullback with the 1.5% target had the highest validation net among the new variants, but failed the required mean-profit, profit-factor and drawdown comparisons. Its stronger latest-nine-session result does not overturn its negative full history or failed validation. No automatic adoption occurred.

## Read the results

- [Findings, missed-entry diagnosis and recommendation](review/FINDINGS.md)
- [Every daily top-five long/short mover and entry comparison](results_through_20261109/FULL_HISTORY_DAILY_TOP5_MOVERS.md)
- [Day-wise missed-entry reasons](review/DAILY_MISSED_ENTRY_REASONS.md)
- [Day-wise results for every strategy](results_through_20261109/DAY_WISE_RESULTS.md)
- [Full report, directions and chronological comparisons](results_through_20261109/REPORT.md)
- [Equity and drawdown chart](review/EQUITY_AND_DRAWDOWN.png)
- [Frozen research plan](RESEARCH_PLAN.md)
- [Exact configuration](results_through_20261109/config.json)
- [Validation selection and failed gates](results_through_20261109/selection.json)

## What was tested

The scanner uses all available stock minute histories rather than only existing G3 candidates. At five-minute scan clocks from 09:35 through 14:00, it tests broad breakouts, relative-strength leader breakouts, controlled leader pullbacks and their union. Each family has fixed 0.8% and 1.5% target variants. The 1.5% stop and adopted 80%-of-target breakeven protection remain common. The plan documents every threshold, quota and fill assumption.

Each new family permits one order per clock/direction, six per direction/day and one per stock/direction/day. Orders enter at the next minute open unless the adverse gap exceeds 0.15%. These daily caps are an explicit experiment assumption; the results do not describe an uncapped scanner.

Modeled exposure is Rs 500,000 per fill with Rs 250 round-trip cost and unlimited concurrent capital. Additional runs stress two basis points of adverse slippage per side. Matched-target controls retain G3's original fills while changing only targets.

Thirty-seven tests passed. Independent reviews verified all 100 frozen result artifacts, source/code lineage, all 824 baseline trade identities and economics, daily reconciliation, mover rankings and capture calculations. The displayed research baseline remains exactly Rs 1,221,079.86 net.

## Reproduce

From the workspace root:

```powershell
python -m pytest g3_v25_momentum_20261110 -q
python -m g3_v25_momentum_20261110.run --output g3_v25_momentum_20261110/reproduction
```

The output directory must be new. The runner verifies frozen inputs and reuses only checkpoints with matching source/code fingerprints. This package has no broker execution authority.

These are previously examined histories, not untouched out-of-sample data. Eight source dates lack minute volume; this scanner consistently uses price only. The source omits the 09:15 minute, earlier universes and complete-stock-day eligibility are retrospective, October 1 and the November 8 special session are excluded, and November 10 was still in progress. No future profit is implied by these backtests.
