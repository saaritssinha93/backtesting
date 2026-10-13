# V13-V10-G-4 late-morning backtest, 13 October 2026

G-4 is an isolated **backtest-only** study of 10:05, 10:10, 10:15, 10:20,
10:25 and 10:30, with both LONG and SHORT at each time. Each signal uses the
next completed one-minute bar for confirmation. The G-3 09:50 entry model is
the donor: at most one maximum-liquidity selection per setup/day, a 1.25%
initial stop tightened to 1.00% after 120 minutes, 0.75% LONG / 0.93% SHORT
targets and 5 basis points round-trip cost. This study does not activate any
paper or live setup or update a dashboard or latest pointer.

The comparison pins the saved **G-3 V2** 49-session run ending 12 October,
including its existing 09:50 LONG configuration. G-3 source code subsequently
acquired a separate 09:20 revision; it is not part of this pinned comparison.
The 38 sealed sessions from 29 July through 23 September form the threshold
selection sample. The 11 later sessions from 24 September through 12 October
are a chronological check; 1 October is excluded because the immutable input
was incomplete. The later sessions were rebuilt from verified one-minute
equity/five-minute futures-OI snapshots. G-4 uses the G-3 V2 clock map so a
later G-3 revision cannot change this source reconstruction.

For each of 12 time/side setups, the sweep evaluated five progressively
stricter joint levels and 13 single-gate tightenings, 18 variants per side.
It chose a variant using the first 38 sessions only. Qualification required
at least 5 training fills and 70% wins, at least 3 later fills and 70% wins,
and at least 70% across all observed fills, both for standalone setup
outcomes and trades executed in the combined portfolio. These are observed
historical rates, not a forecast or proof of a 70% future win rate.

| Setup | Training wins/fills | Later wins/fills | All wins/fills | Qualified |
| --- | ---: | ---: | ---: | --- |
| 10:05 LONG | 6/10 | 0/0 | 6/10 | No |
| 10:05 SHORT | 3/5 | 2/3 | 5/8 | No |
| 10:10 LONG | 3/5 | 1/1 | 4/6 | No |
| 10:10 SHORT | 1/1 | 2/2 | 3/3 | No; too few training fills |
| 10:15 LONG | 4/5 | 0/0 | 4/5 | No; no later fills |
| 10:15 SHORT | 2/4 | 0/0 | 2/4 | No |
| 10:20 LONG | 6/11 | 0/0 | 6/11 | No |
| 10:20 SHORT | 3/7 | 0/1 | 3/8 | No |
| 10:25 LONG | 5/7 | 0/1 | 5/8 | No |
| 10:25 SHORT | 3/3 | 3/3 | 6/6 | No; too few training fills |
| 10:30 LONG | 3/5 | 0/1 | 3/6 | No |
| 10:30 SHORT | 3/4 | 0/0 | 3/4 | No |

| Portfolio, 49 sessions | G-3 V2 baseline | G-4 with all 12 additions (exploratory) |
| --- | ---: | ---: |
| Selected orders | 123 | 212 |
| Executed fills | 112 | 191 |
| Wins / losses | 80 / 32 | 130 / 61 |
| Win rate | 71.43% | 68.06% |
| Net P&L after modeled cost | ₹286,682.16 | ₹318,265.98 |

The exploratory all-12 combination added **₹31,583.83** net P&L but reduced
the observed portfolio win rate by **3.37 percentage points**. Its 79 added
executed trades won 50 times (63.29%). The later 11-session portfolio P&L
rose by ₹8,078.99, while its win rate was 64.52%. No original G-3 V2 order's
execution or P&L changed in the combined replay. Since **zero setups met the
individual 70% qualification rule**, there is no approved G-4 configuration
to promote from this study. The 10:25 SHORT result (6/6) is a useful research
lead, but three training fills do not meet even the modest five-fill screen.
At the loosest 09:50-style level, ten of the twelve setups had fewer than
three later-session selection days. Tightening their 5-minute or 1-minute
gates cannot create more eligible days, so none can meet the later-sample
minimum within this strict family. Of the other two, 10:25 SHORT had only
four training selection days at the loosest level, while 10:05 SHORT won
three of five training fills and two of three later fills.

Run: `C:\TradingData\eqidv2\backtesting_result_v13_v10_g_4\runs\2026-10-12\20261013T010748590154_5e72f1aa_late_slots`.
The run contains `summary.json`, `setup_decisions.csv`, the complete
`strictness_sweep.csv`, selected orders, executed ledgers, daily results,
source-manifest hashes, and `manifest.json` with artifact hashes.
The reproducible runner is `fno_v13_v10_g_4_backtest.py`, using
`configs/strategies/v13_v10_g_4_backtest_v1.json`.
