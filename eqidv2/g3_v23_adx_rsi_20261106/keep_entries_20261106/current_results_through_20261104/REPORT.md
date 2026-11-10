# V23 — keep all entries

**ADX/RSI entry vetoes are disabled in this revision.** All 871 V21 + G7 selected orders and 771 native filled trades are retained. The previous 561-trade entry-filter experiment is archived for comparison and is not the current V23 mode.

The old filter removed 72 losing trades and 138 winning trades. Its higher win rate did not compensate for all of the winners skipped, which is why net profit fell.

Current exit policy: **baseline_v21_g7**. Position sizing remains the baseline.

| Version | Trades | Wins | Losses | Win rate | Net P&L (Rs) | Gross loss amount (Rs) | Daily-close drawdown (Rs) |
|:--|--:|--:|--:|--:|--:|--:|--:|
| V21 + G7 baseline | 771 | 524 | 247 | 67.96% | 1,149,524.05 | 840,329.22 | 35,384.36 |
| Previous V23 entry-filter experiment | 561 | 386 | 175 | 68.81% | 878,716.64 | 615,245.12 | 28,457.07 |
| Current V23 keep-all-entries | 771 | 524 | 247 | 67.96% | 1,149,524.05 | 840,329.22 | 35,384.36 |

**No exit overlay met all selection requirements.** The current result therefore retains the baseline exits and sizing as well as its entries. No loss reduction is claimed for the current mode. The alternative results remain available in the studies.

## Same-trade alternative: profit protection

The +0.6% activation / +0.1% stop experiment retains 771 trades, with 554 wins, 217 losses and a 71.85% win rate. Net P&L is Rs 928,591.68, loss amount Rs 753,876.78, and daily-close drawdown Rs 30,606.72. It reduces loss amounts and drawdown, but also cuts profitable trades short. It was not selected because profit falls.

[Alternative day-wise comparison](PROFIT_PROTECTION_DAY_WISE.md) · [Alternative day-wise CSV](profit_protection_day_wise.csv)

## Later six sessions (retrospective)

| Policy | Trades | Win rate | Net P&L (Rs) | Loss amount (Rs) | Daily-close drawdown (Rs) |
|:--|--:|--:|--:|--:|--:|
| baseline_v21_g7 | 84 | 57.14% | 41,530.08 | 143,577.23 | 24,615.85 |

## Day-wise results

[All 65 days, formatted table](DAY_WISE_RESULTS.md) · [Day-wise CSV](day_wise_results.csv)

## Scope and limits

65 sealed sessions from 29 July through 4 November 2026; November 5–6 are not included. All 73 setup/direction pairs through 14:00 remain configured. Original pairs retain cap 3 and new pairs cap 1 plus the volume gate. Native entry filters, prices and times are retained.

Fixed entry exposure is Rs 500,000 (Rs 100,000 capital at 5x), with unlimited portfolio capital. These are research results, not LIVE sizing. Entry identities and daily counts are checked against the baseline. Exit changes use observed prices/entry-time indicators, never future winner/loser labels. Selection uses the earlier 59 sessions, with later six reported separately. All history was previously observed, so this is not independent forward validation.

Only losing trades cannot be singled out in advance. A causal exit rule can also change winning trades. Smaller sizing reduces loss amounts without necessarily reducing losing trade count; the sizing study did not qualify under the profit/loss/drawdown constraints.

[Exit study and later-period results](../exit_study/results_through_20261104/REPORT.md) · [Sizing study](../risk_study/results_through_20261104/REPORT.md) · [Configuration](config.json)

LIVE execution, scheduler, approval and position management are unchanged.
