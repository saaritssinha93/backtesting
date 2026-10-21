# G4: 37 existing setups plus the 12 requested midday additions

The default `python fno_v13_v10_g_4_backtest.py` command now runs profile
`V13-V10-G-4-SELECTED-MIDDAY-TWELVE-V5`. It preserves the existing 37 G4
setups and enables only the twelve additions below. The config is
`configs/strategies/v13_v10_g_4_backtest_selected_midday_v5.json`.
Other research profiles remain available through their explicit `--config`.

This run does not optimize targets or filters again. It reconstructs the
requested selections, verifies them against the pinned entry and target
studies, replays all 49 setups from minute paths, and reallocates the combined
portfolio using its capital limit. The source-selected malformed negative
liquidity row remains excluded. No baseline execution changes.

## Results

Period: July 29-October 12, 2026, 49 complete sessions. October 1 is excluded
because immutable input was incomplete. July and October are partial months.

| Portfolio | Trades | Wins | Losses | Win rate | Net P&L (Rs) | Daily realized maximum drawdown (Rs) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Existing G4 | 289 | 205 | 84 | 70.93% | 607,998.28 | 21,084.23 |
| Requested additions | 125 | 100 | 25 | 80.00% | 385,342.79 | 7,317.32 |
| Combined G4 | 414 | 305 | 109 | 73.67% | 993,341.07 | 15,619.16 |

Combined gross P&L is Rs 1,096,841.07; modeled costs are Rs 103,500.00.
Profit factor is 3.3064, with 40 positive and nine negative sessions.
There are 475 selected orders and 419 native fills; five fills are rejected
by the capital limit, including four new midday fills.

| Signal time | Side | Target | Executed trades | Wins | Win rate | Net P&L (Rs) |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 11:35 | LONG | 7.15% | 7 | 5 | 71.43% | 44,987.89 |
| 11:35 | SHORT | 0.87% | 17 | 15 | 88.24% | 42,843.01 |
| 11:40 | LONG | 4.03% | 7 | 5 | 71.43% | 23,804.50 |
| 11:40 | SHORT | 1.19% | 10 | 8 | 80.00% | 21,358.15 |
| 11:45 | SHORT | 4.95% | 14 | 10 | 71.43% | 49,436.60 |
| 12:05 | LONG | 1.40% | 6 | 5 | 83.33% | 17,704.04 |
| 12:10 | SHORT | 0.79% | 13 | 11 | 84.62% | 29,344.16 |
| 12:15 | LONG | 1.24% | 6 | 5 | 83.33% | 12,241.94 |
| 12:15 | SHORT | 3.49% | 17 | 13 | 76.47% | 39,028.91 |
| 12:25 | SHORT | 2.13% | 9 | 7 | 77.78% | 36,621.92 |
| 12:30 | LONG | 2.53% | 9 | 8 | 88.89% | 46,034.48 |
| 12:30 | SHORT | 1.85% | 10 | 8 | 80.00% | 21,937.19 |

All twelve meet 70% wins, positive net P&L, and at least five actual fills.
Setup IDs use the confirmation clock, one minute after the displayed signal
time; for example `1136_LONG` is the 11:35 LONG setup.

| Month | Combined trades | Wins | Win rate | Net P&L (Rs) |
| --- | ---: | ---: | ---: | ---: |
| July | 28 | 21 | 75.00% | 118,284.28 |
| August | 180 | 136 | 75.56% | 399,038.31 |
| September | 163 | 120 | 73.62% | 391,138.80 |
| October | 43 | 28 | 65.12% | 84,879.68 |

In the later eleven sessions, the combined book has 84 trades, 60 wins
(71.43%), and Rs 217,084.41 net P&L. Additions contribute 32 trades, 25 wins
(78.13%), and Rs 88,157.04. These sessions participated in preceding target
research, so this is descriptive historical performance, not independent
validation. Some individual setups have no later-period executions.

## Fixed execution assumptions

- Initial stop 1.25%, tightened to 1.00% after 120 minutes.
- Exact next-minute confirmation; ten-minute entry expiry; 15:15 square-off.
- Highest-liquidity qualifying stock, at most one entry per setup/session.
- Rs 100,000 capital per entry, five-times exposure, Rs 1,000,000 total capital.
- Five basis points round-trip cost. Broker-specific costs, additional
  slippage and market impact are not modeled.
- A win means positive net P&L, including profitable time exits.
- Targets and setup membership were selected after historical inspection.
  The backtest does not establish a future minimum win rate.

## Saved evidence and rerun

Run directory:
`C:/TradingData/eqidv2/backtesting_result_v13_v10_g_4/runs/2026-10-13/20261013_selected_midday_twelve_v5`

`report.html` contains totals, the twelve additions, monthly and period
comparisons, all 49 setup results, and every combined daily result. CSVs
include `daily_results.csv`, `monthly_results.csv`, `setup_results.csv`,
`period_results.csv`, `combined_trades.csv`, `added_trades.csv`,
`enabled_setups.csv`, and the selection audit. `config.json` saves the exact
new settings, while `summary.json` and `manifest.json` retain source proofs.

```powershell
python fno_v13_v10_g_4_backtest.py
```

The command creates a fresh run directory. To select the config explicitly:

```powershell
python fno_v13_v10_g_4_backtest.py --config configs/strategies/v13_v10_g_4_backtest_selected_midday_v5.json
```

Validation: 143 focused tests passed. An independent allocator using a
separate heap implementation passed 499 checks, including all 475 order
decisions, all 49 setups, accounting at each reporting level, all source
hashes, 14 code hashes, and 16 output artifact hashes. The receipt is
`.codex_tmp/g4_midday_selected12_20261013/independent_validation.json`.
The HTML report's six table inventories were verified as well.

Run manifest SHA-256:
`336a072bee3edbcec7ba4d69a16e969871b5aa09d6a9379a6f009885559f0c17`.

All changes and outputs are for G4 backtesting only.
