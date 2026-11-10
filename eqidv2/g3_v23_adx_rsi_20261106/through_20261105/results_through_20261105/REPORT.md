# V23 results through 2026-11-05

**2026-11-05 is included as a complete strategy session.** The updated history contains **66 sessions**, from 2026-07-29 through 2026-11-05, and **790 current V23 filled trades**.

## Added session and updated total

| Policy / period | Trades | Wins | Losses | Win rate | Net P&L (Rs) | Loss amount (Rs) | Daily-close drawdown (Rs) |
|:--|--:|--:|--:|--:|--:|--:|--:|
| Previous 65 sessions through 2026-11-04 | 771 | 524 | 247 | 67.96% | 1,149,524.05 | 840,329.22 | 35,384.36 |
| Added full session: 2026-11-05 | 19 | 11 | 8 | 57.89% | 27,998.05 | 23,443.12 | 0.00 |
| Updated full history: 66 sessions | 790 | 535 | 255 | 67.72% | 1,177,522.10 | 863,772.35 | 35,384.36 |

Drawdown in each row is recalculated within that period; drawdowns are not additive.

## Fixed-policy comparison over the full history

| Policy / period | Trades | Wins | Losses | Win rate | Net P&L (Rs) | Loss amount (Rs) | Daily-close drawdown (Rs) |
|:--|--:|--:|--:|--:|--:|--:|--:|
| V21 + G7 / current V23 (all entries retained) | 790 | 535 | 255 | 67.72% | 1,177,522.10 | 863,772.35 | 35,384.36 |
| Archived entry-filter experiment (not current) | 576 | 397 | 179 | 68.92% | 917,700.45 | 627,702.48 | 28,457.07 |
| Profit-protection experiment (not selected) | 790 | 565 | 225 | 71.52% | 941,955.42 | 777,319.91 | 30,606.72 |

ADX/RSI entry vetoes remain disabled in the current mode. All baseline entry identities, prices and times are retained. The earlier entry-filter experiment remains a comparison only: it removes winners as well as losers.

The selected exit and sizing policies, five exit alternatives and five sizing alternatives were frozen before appending this session. Selection still uses the earlier 59 sessions ending 2026-10-27; the added day does not select thresholds. Earlier attempts did not simultaneously retain baseline net profit, reduce loss amounts and reduce daily-close drawdown. Historical results do not establish that only losing trades can be reduced in future.

## Day-wise results

[Current V23 — every session](DAY_WISE_RESULTS.md) · [Current day-wise CSV](day_wise_results.csv) · [Profit-protection experiment](PROFIT_PROTECTION_DAY_WISE.md) · [Alternative day-wise CSV](profit_protection_day_wise.csv)

## Coverage and assumptions

- All 66 sessions have a row, including sessions without trades. October 1 remains excluded by the original sealed history because its source is incomplete.
- All 73 setup/direction pairs remain enabled through the 14:00 signal, with final confirmation at 14:01. Execution paths run through 15:15. Original pairs retain cap 3; new pairs cap 1 plus the existing volume gate.
- Rs 100,000 capital at 5x exposure per entry, Rs 500,000 fixed notional, Rs 250 modelled round-trip costs, and unlimited concurrent capital. Loss amount is the absolute sum of negative after-cost trade P&L before offsetting winners.
- The full strategy session does not mean absent source data were filled synthetically. Source-specific exclusions are retained and disclosed below.
- All 218 stock sources contain all 360 one-minute bars from 09:16 through the strategy exit cutoff 15:15 on 5 November.
- 217 stocks are eligible in the sealed feature ledger. SAIL is excluded for the day because its 09:20 futures OI observation is missing.
- KAYNES is excluded only from 09:20 opening setups for missing opening OI evidence; later setups retain valid source data.
- All 73 setup/direction pairs through 14:00 are evaluated using the same sealed snapshot; all stock futures use November contracts.
- October 1 remains excluded from older history by the immutable source. No November 6 intraday data are included.
- Inherited earlier-source limits remain: some archived cohorts use after-dated membership/full-day eligibility; October 30 reconstructed opening OI does not prove contemporaneous availability.
- One-minute OHLC cannot reveal the intrabar path. Exit experiments retain stop-first tie handling and adverse-open stop fills. New protection starts after the observed bar.
- All periods are retrospective. This is not independent forward validation or LIVE sizing. LIVE execution, approvals and scheduling are unchanged.

[All exit policy totals](overall.csv) · [All exit policy daily rows](daily.csv) · [Sizing totals](sizing_overall.csv) · [Sizing daily results](sizing_daily.csv) · [Configuration](config.json)
