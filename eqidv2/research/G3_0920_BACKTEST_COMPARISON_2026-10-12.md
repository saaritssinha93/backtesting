# G-3 09:20 LONG/SHORT historical comparison

Research run: July 29–October 12, 2026, the 49 sessions in the published
G-3 V2-equivalent backtest. The additive 09:20 LONG/SHORT variant clones the
current 09:25 setup filters and exits, uses the completed 09:20 first cash
bar and same-contract 09:16-to-09:20 OI change, confirms on the real 09:21
minute, and can fill no earlier than 09:22. The active live and paper setups
were not changed.

| Measure | Published G-3 | With 09:20 LONG/SHORT | Change |
| --- | ---: | ---: | ---: |
| Selected orders | 123 | 142 | +19 |
| Executed trades | 112 | 131 | +19 |
| Wins / losses | 80 / 32 | 88 / 43 | +8 / +11 |
| Win rate | 71.43% | 67.18% | -4.25 percentage points |
| Gross P&L | ₹314,682.16 | ₹314,732.61 | +₹50.45 |
| Trading costs | ₹28,000.00 | ₹32,750.00 | +₹4,750.00 |
| **Net P&L** | **₹286,682.16** | **₹281,982.61** | **-₹4,699.55** |
| Daily-equity maximum drawdown | ₹14,142.14 | ₹18,874.95 | +₹4,732.81 |

The added 09:20 LONG side produced 14 executed trades, seven wins, and
**-₹4,708.78 net**. The SHORT side produced five executed trades, one win,
and **+₹9.23 net**. No published trade was displaced or newly accepted by
the portfolio allocator. The 09:25 setups remain in the variant; in the
published baseline their LONG and SHORT trades contributed ₹38,780.35 and
₹25,233.82 net respectively. These per-slot totals are descriptive, not a
matched head-to-head test because opening OI coverage varies by date.

The feature export has one row for each of 10,275 stocks across the 49
published sessions; 3,699 stock-days have a valid 09:16-to-09:20 OI change
and 3,696 have complete 09:20 research features. Only 29 sessions have any
valid opening OI, and 20 have none. Eighteen sessions have at least 90%
stock-level OI coverage. The source export also contains October 1, which
is outside the published baseline and was excluded from the comparison.
August 24–25 have no eligible saved near-month mapping and are outside that
baseline. All selected 09:20 trades use retrospective opening data; **none**
has source receipt evidence proving a live decision by 09:22.

July 29–August 10 and September 21–23 use a hash-verified sealed monthly
stock/futures mapping because a complete dated mapping was unavailable. It
is labelled retrospective; the monthly construction could introduce hindsight
universe bias. September 21–23 have no valid archived 09:16 OI. Other dates
also have sparse or absent opening OI, so the measured P&L is a comparison
over observable 09:20 opportunities, not an estimate of every opportunity
that may have existed. Kite's expired-futures continuous history supplies
daily candles, not the missing 09:16 minute OI, as its
[API documentation](https://www.kite.trade/docs/connect/v3/historical/) and
[support answer](https://kite.trade/forum/discussion/13783/how-to-fetch-minute-candles-for-nifty19decfut-for-five-minutes-with-oi-data)
explain. The research runner does not substitute a proxy.

The published baseline is the hash-verified `current_backtest.json` pointing
to `20261012T185109507457_a9213404_0950_long_t075`, including its 09:50
LONG target variant. Its frozen trade outcomes were reused; only added 09:20
trades were simulated on complete, genuine 09:22–15:15 one-minute cash paths.
Both arms were reallocated using ₹1,000,000 portfolio capital, ₹100,000
margin per entry, 5× exposure, and five basis points round-trip cost. This
does not use the separate ₹10,000 live sizing.

Artifacts:

- Sealed features and coverage:
  `outputs/g3_0920_research_full_run_20261012T2320/2026-07-29_through_2026-10-12/`.
- Paired result, including `summary.json`, `daily_comparison.csv`,
  `added_0920_trades.csv`, `selection_audit_0920.csv`, and SHA-256 manifest:
  `C:\TradingData\eqidv2\backtesting_result_v13_v10_g_3\research_0920\runs\2026-10-12\paired_full_0920_final_20261012T2330\`.
- Reproducible setup and commands: `research/G3_0920_RESEARCH.md`.

All 31 focused feature, mapping, selection, replay, and P&L reconciliation
tests pass. The result manifest and its recorded research-code hashes match
the final files. The active G-3 contract, live/paper schedules, and published
backtest pointer remain unchanged.
