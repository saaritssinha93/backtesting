# G-3 09:20: five-minute relaxation with a 65% win-rate screen

**Result:** Loosening the 09:20 five-minute filters did not add a trade. All 24
tested configurations selected the same 12 stocks as the prior 09:21-confirmation
choice. Those 12 historical additions won 8 times (**66.67%**) and contributed
**+₹21,509.38 net P&L** after costs. The 65% pooled threshold is met in this
backtest, but no looser five-minute setting improves the trade count or result.
This remains **backtest-only**; live and paper configurations, dashboards, and
the published G-3 backtest pointer were not changed.

## What changed in the experiment

The 09:21 confirmation was held fixed at body ratio ≥0.75, directional wick
≤0.35, and volume ratio ≥1.6 for LONG; body ≥0.65, wick ≤0.35, and volume
≥1.7 for SHORT. The completed 09:20 five-minute signal filters were relaxed
gradually from the copied 09:25 settings to the native G-3 contract floors:

| Side | Filter | Reference | Loosest tested |
| --- | --- | ---: | ---: |
| LONG | Directional price, active/core | ≥0.30% / ≥0.30% | ≥0.10% / ≥0.10% |
| LONG | 09:16→09:20 OI increase | ≥0.10% | ≥0.05% |
| LONG | Five-minute volume ratio | ≥3.0 | ≥0.8 |
| SHORT | Directional price, active/core | ≥0.13% / ≥0.20% | ≥0.10% / ≥0.10% |
| SHORT | 09:16→09:20 OI increase | ≥0.05% | ≥0.05% |
| SHORT | Five-minute volume ratio | ≥1.5 | ≥0.8 |

The 13 cumulative stages relaxed volume first, then price, then LONG OI;
11 one- or two-axis probes checked intermediate combinations. The dated
universe was reselected, ranked, simulated, and portfolio-replayed for each
profile with the same G-3 entry, exit, daily quota, capital, and costs. The
reference first stage reproduces the previously saved 09:21-only choice.

| Five-minute setting | Signal-filter passes | Selected 09:20 orders | Wins | Added net P&L |
| --- | ---: | ---: | ---: | ---: |
| Reference | 13 | 12 | 8/12 | +₹21,509.38 |
| Native contract floors on all tested five-minute gates | 14 | 12 | 8/12 | +₹21,509.38 |
| Every other tested stage | 13–14 | 12 | 8/12 | +₹21,509.38 |

The extra filter pass at the looser price thresholds is **VOLTAS LONG on July
31**. It ranks second behind that day's selected LONG, so the one-LONG-per-day
quota keeps it out. Across all 24 stages the selected signal-ID set and replay
outcomes are identical. The tested five-minute relaxations, including the
contract-floor endpoint, did not increase the selected count in the saved
cohort; a different confirmation or selection rule would be a separate
strategy experiment.

The **loose experimental configuration** uses the "Loosest tested" column above:
LONG and SHORT five-minute price ≥0.10%, OI rise ≥0.05%, and volume ratio
≥0.8, while retaining the fixed 09:21 gates. It is tied with the reference
configuration on every observed trade and P&L measure, so the search has no
evidence-based reason to change the published strategy to those floors.

## P&L and validation split

| Period | Added fills | Added wins | Added win rate | Incremental net P&L |
| --- | ---: | ---: | ---: | ---: |
| July 29–September 30 discovery, 43 sessions | 9 | 8 | 88.89% | +₹28,479.16 |
| October 5–12 recent, 6 sessions | 3 | 0 | 0% | −₹6,969.79 |
| All 49 published sessions | 12 | 8 | **66.67%** | **+₹21,509.38** |

For all dates, added LONG is **8/9 wins and +₹33,003.43 net**; added SHORT is
**0/3 and −₹11,494.05 net**. The published G-3 baseline has 112 executed
trades, 80 wins (71.43%), and ₹286,682.16 net P&L. Adding the research 09:20
orders gives 124 trades, 88 wins (70.97%), and ₹308,191.53 net P&L, with
daily-equity maximum drawdown still ₹14,142.14. The October combined ledger
drops from 7/11 wins to 7/14 and loses ₹6,969.79 of baseline net P&L.

All 24 profiles clear the **observed full-history** screen of at least 10
added fills, ≥65% added and combined win rates, and positive incremental net.
None qualifies on the earlier discovery period under that same minimum-fill
rule (only nine added fills), and the recent period has only three. Therefore
there is no independently validated ≥65% five-minute relaxation to promote.
The pooled 66.67% is historical and tuned; it is not a prospective win-rate
guarantee or approval for SHORT entries.

The sealed feature export has 3,696 complete opening-feature stock-days across
these sessions; 20 sessions have no valid 09:16 OI for any stock. The opening
OI was assembled retrospectively, and this run does not prove that each value
was available at the live 09:22 decision. October 1 is absent from the
published G-3 baseline and therefore excluded from the paired comparison.

## Reproduction and checks

The saved research run is
`C:\TradingData\eqidv2\backtesting_result_v13_v10_g_3\research_0920\runs\2026-10-12\search_5m65_20261012T2355`.
`trials.csv` contains all 24 gates and period results;
`selected_membership_all_profiles.csv` records their actual picks;
`best_observed_65_orders.csv`, `best_observed_65_variant_trades.csv`, and
`best_observed_65_daily_comparison.csv` contain the reference research ledger.
`summary.json` records the source and coverage evidence, while `manifest.json`
contains SHA-256 hashes for the artifacts and source code. All artifact and
source hashes verified after the run; the published pointer hash remains
unchanged. All 38 focused 09:20 tests passed.

Reproduce into a **new** output directory:

```powershell
python -m research.g3_0920_five_minute_65_search --output-dir C:\TradingData\eqidv2\backtesting_result_v13_v10_g_3\research_0920\runs\2026-10-12\search_5m65_reproduction_NEW
```
