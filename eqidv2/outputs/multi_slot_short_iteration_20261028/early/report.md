# 09:31 and 12:26 SHORT replacement-aware audit

The shared fixed grid was applied before native ranking and quota. Replacements were freshly simulated from one-minute paths. G7 then applied dated Nifty permission, the existing 12:26 traded-value veto, a freshly calculated low-efficiency veto for every replacement, +0.6/+0.1 protection, and the causal two-realized-loss lock.

All six baseline controls passed. The pools contain 20 native-eligible 09:31 candidates and 24 native-eligible 12:26 candidates. The G7 union contains 34 selected candidates across the grid, including eight replacements; recomputed permission agrees with the sealed permission for all 26 existing candidates.

## Honest decision

No predeclared rule passed the development gates. Keep baseline in all four cases.

| Setup | Strategy | Reason |
|---|---|---|
| 09:31 SHORT | G3 | Trials reaching eight development trades were unchanged or failed required loss reduction. |
| 09:31 SHORT | G7 V4 lock2 | No trial reached eight development executions; maximum six. |
| 12:26 SHORT | G3 | No trial reached eight; maximum five. Baseline development had zero losses, so losing amount could not be strictly reduced. |
| 12:26 SHORT | G7 V4 lock2 | No trial reached eight; maximum four. Baseline development also had zero losses. |

No same rule qualified in both strategies for either slot.

## Baseline at 5 bps

| Setup | Strategy | Overall W/L | Win rate | Wilson lower | Net | October W/L | October net |
|---|---|---:|---:|---:|---:|---:|---:|
| 09:31 SHORT | G3 | 10/4 | 71.43% | 45.35% | Rs30,779.75 | 1/3 | -Rs14,521.47 |
| 09:31 SHORT | G7 V4 lock2 | 7/3 | 70.00% | 39.68% | Rs747.84 | 2/1 | -Rs1,021.77 |
| 12:26 SHORT | G3 | 7/4 | 63.64% | 35.38% | Rs30,288.03 | 1/3 | -Rs5,919.82 |
| 12:26 SHORT | G7 V4 lock2 | 5/2 | 71.43% | 35.89% | Rs15,102.14 | 1/1 | Rs408.43 |

The closest attractive row was **09:31 G3 body ratio <=0.80**: 10/3, 76.92%, Rs37,279.75, +Rs6,500 versus baseline. It made no change in development, so development could not choose it; its improvement appeared later. Its Wilson lower bound is 49.74%. The same rule worsened G7 to 6/3, 66.67%, Rs497.88. It is descriptive and unsupported.

## Baseline cost stress

| Setup | Strategy | 5 bps | 9 bps | 15 bps |
|---|---|---:|---:|---:|
| 09:31 SHORT | G3 | 10/4, Rs30,779.75 | 10/4, Rs27,979.75 | 9/5, Rs23,779.75 |
| 09:31 SHORT | G7 V4 lock2 | 7/3, Rs747.84 | 7/3, -Rs1,249.11 | 3/7, -Rs4,244.53 |
| 12:26 SHORT | G3 | 7/4, Rs30,288.03 | 7/4, Rs28,088.03 | 7/4, Rs24,788.03 |
| 12:26 SHORT | G7 V4 lock2 | 5/2, Rs15,102.14 | 5/2, Rs13,705.33 | 4/2 over six trades, Rs8,839.52 |

G7 09:31 is cost fragile: its small positive 5 bps result turns negative at 9 bps. At 15 bps, protected +0.1% exits change after-cost sign and alter the causal lock path.

A point win rate above 75% is not validated when the Wilson lower bound remains below 75%; every lower bound here is far below 75%. The same 59 sessions have been inspected before, so later blocks are chronological diagnostics rather than independent holdouts.

See `development_selection.csv`, `joint_development_selection.csv`, `variant_setup_metrics.csv`, `variant_portfolio_metrics.csv`, `candidate_variant_decisions.csv`, `selection_changes.csv`, `variant_trade_details.csv`, and `baseline_reproduction.json`.
