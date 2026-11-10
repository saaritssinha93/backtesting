# V23 fixed-entry exit study

Selected policy: **baseline_v21_g7**. Every arm retains all **771** native entry identities, entry timestamps and prices.

All 771 exits were first reproduced exactly using the original staged minute engine and hash-verified historical inputs. No missing source or failed replay is silently dropped.

Five fixed causal overlays were declared before outcome computation. Selection uses only the first 59 sessions: same trade count, net P&L at least baseline, strictly lower gross losses and daily-close drawdown. If no arm qualifies, baseline remains selected.

## Full 65 sessions

| Policy | Trades | Losses | Win rate | Net P&L (Rs) | Gross losses (Rs) | Mean/trade (Rs) | Median/trade (Rs) | Daily-close DD (Rs) |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|
| baseline_v21_g7 | 771 | 247 | 67.96% | 1,149,524.05 | 840,329.22 | 1,490.95 | 2,550.00 | 35,384.36 |
| weak_stop_1p0 | 771 | 252 | 67.32% | 1,087,207.91 | 874,095.36 | 1,410.13 | 2,549.12 | 36,616.89 |
| weak_loss20m_0p35 | 771 | 277 | 64.07% | 1,022,120.80 | 861,298.03 | 1,325.71 | 2,212.36 | 23,397.89 |
| protect_mfe0p4_lock0p05 | 771 | 181 | 45.01% | 729,307.46 | 656,855.39 | 945.92 | 0.00 | 28,901.89 |
| protect_mfe0p6_lock0p1 | 771 | 217 | 71.85% | 928,591.68 | 753,876.78 | 1,204.40 | 892.27 | 30,606.72 |
| weak_stop1p0_protect0p6 | 771 | 221 | 71.34% | 875,275.54 | 782,392.92 | 1,135.25 | 791.23 | 31,839.25 |

## early_59

| Policy | Trades | Losses | Win rate | Net P&L (Rs) | Gross losses (Rs) | Mean/trade (Rs) | Median/trade (Rs) | Daily-close DD (Rs) |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|
| baseline_v21_g7 | 687 | 211 | 69.29% | 1,107,993.97 | 696,752.00 | 1,612.80 | 2,550.00 | 35,384.36 |
| weak_stop_1p0 | 687 | 216 | 68.56% | 1,041,007.86 | 735,188.11 | 1,515.30 | 2,550.00 | 36,616.89 |
| weak_loss20m_0p35 | 687 | 236 | 65.65% | 1,007,797.05 | 712,664.47 | 1,466.95 | 2,439.03 | 23,143.08 |
| protect_mfe0p4_lock0p05 | 687 | 150 | 46.14% | 730,249.21 | 533,147.90 | 1,062.95 | 0.00 | 28,901.89 |
| protect_mfe0p6_lock0p1 | 687 | 183 | 73.36% | 918,912.83 | 621,050.35 | 1,337.57 | 1,098.52 | 30,606.72 |
| weak_stop1p0_protect0p6 | 687 | 187 | 72.78% | 860,926.72 | 654,236.46 | 1,253.17 | 929.27 | 31,839.25 |

## later_6

| Policy | Trades | Losses | Win rate | Net P&L (Rs) | Gross losses (Rs) | Mean/trade (Rs) | Median/trade (Rs) | Daily-close DD (Rs) |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|
| baseline_v21_g7 | 84 | 36 | 57.14% | 41,530.08 | 143,577.23 | 494.41 | 1,503.47 | 24,615.85 |
| weak_stop_1p0 | 84 | 36 | 57.14% | 46,200.05 | 138,907.26 | 550.00 | 1,503.47 | 22,115.85 |
| weak_loss20m_0p35 | 84 | 41 | 51.19% | 14,323.74 | 148,633.56 | 170.52 | 98.32 | 23,397.89 |
| protect_mfe0p4_lock0p05 | 84 | 31 | 35.71% | -941.75 | 123,707.49 | -11.21 | 0.00 | 27,019.17 |
| protect_mfe0p6_lock0p1 | 84 | 34 | 59.52% | 9,678.85 | 132,826.44 | 115.22 | 250.00 | 24,615.85 |
| weak_stop1p0_protect0p6 | 84 | 34 | 59.52% | 14,348.82 | 128,156.47 | 170.82 | 250.00 | 22,115.85 |

## Execution assumptions and limitations

- Fixed Rs100,000 capital / Rs500,000 exposure per fill; Rs250 round-trip cost per trade; Rs192,750 total costs in every arm. Capital is unlimited, so earlier exits do not cause extra entries or reranking.
- The weak-entry flag uses completed ADX/DI/RSI available at confirmation. No trade outcome, future label, or later session selects individual trades.
- A new weak-entry stop first applies after the entry bar. Protection observes extrema only after a post-entry bar completes, becoming effective next bar. A weak loss timer exits at the following open.
- Original target and 15:15 close exits remain. Native intrabar stop-first handling is conservative when stop and target both touch; gaps through an active stop fill at the adverse open. A pre-scheduled market exit fills at open before subsequent intrabar extremes.
- One-minute OHLC does not reveal the intrabar path or actual bid/ask fills. The native entry-bar stop assumption is retained. Protection can cut future winners, and a historical improvement does not guarantee lower losses tomorrow.
- The six later sessions are retrospective diagnostics, not untouched prospective validation. October 1 remains excluded by the source history because its immutable data are incomplete.
- daily.csv contains every candidate and all 65 days; selected_trades.csv keeps exact baseline entries and the selected exit outcomes. Source proofs and native replay parity are saved alongside.

Inherited source limitations remain: earlier archived cohorts include after-dated membership and full-day eligibility; October30 uses reconstructed source/opening OI that does not prove contemporaneous live availability.

Order counts distinguish 871 G7-admitted orders from 771 actual native fills. Breakeven trades are neither wins nor losses. Ten execution/causality/selection unit tests passed.

The protect_mfe0p4_lock0p05 arm has **243 flat trades after costs**, alongside 347 wins and 181 losses (771 total). Its 45.01% win rate counts only strictly positive net trades, divided by all 771; the remaining 243 are neither wins nor losses.
