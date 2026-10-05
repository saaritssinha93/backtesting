# V13-v10-G full backtesting result

Generated: 2026-10-04 IST

## Result scope

The primary result is the immutable cutoff-corrected full-history bundle covering 38 eligible sessions from 2026-07-29 through 2026-09-23. All 22 artifacts in its bundle manifest pass SHA-256 verification. The bundle state is `COMPLETE`, its strategy outputs are unchanged by the cutoff metadata correction, and it has no execution authority.

Five later successful daily replays (2026-09-24, 25, 28, 29 and 30) are shown separately and then appended into a combined descriptive view. That combined view is not a new single immutable full-history bundle.

## Headline result — immutable bundle through 2026-09-23

| Metric | Result |
|---|---:|
| Eligible sessions | 38 |
| Selected orders | 85 |
| Executed trades | 77 |
| Unfilled selections | 8 |
| Fill rate | 90.59% |
| Trades per session | 2.03 |
| Wins / losses | 47 / 30 |
| Win rate | 61.04% |
| Profit factor | 2.8291 |
| Gross P&L | Rs 1,95,486.32 |
| Modeled costs | Rs 19,250.00 |
| Net P&L | **Rs 1,76,236.32** |
| Net return on Rs 10 lakh portfolio capital | 17.62% |
| Average / median trade | Rs 2,288.78 / Rs 2,172.58 |
| Daily-close maximum drawdown | Rs 16,250.00 |
| Peak drawdown date | 2026-09-18 |
| Profitable / losing / flat sessions | 20 / 9 / 9 |
| Longest trade win / loss streak | 7 / 5 |
| Average holding time | 161.53 minutes |
| Peak concurrent positions | 5 |
| Peak reserved capital | Rs 5,00,000 |
| Peak modeled gross exposure | Rs 25,00,000 |

Best session: 2026-08-28, 5/5 winners, net Rs 32,961.05. Worst session: 2026-09-18, 0/3 winners, net -Rs 9,750.00.

## Monthly breakdown

| Month | Sessions | Selected | Trades | W-L | Win rate | PF | Gross P&L | Costs | Net P&L |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-07 | 3 | 5 | 4 | 4-0 | 100.00% | N/A | Rs 27,243.15 | Rs 1,000 | Rs 26,243.15 |
| 2026-08 | 19 | 50 | 49 | 31-18 | 63.27% | 3.1054 | Rs 1,35,601.85 | Rs 12,250 | Rs 1,23,351.85 |
| 2026-09 through 23rd | 16 | 30 | 24 | 12-12 | 50.00% | 1.7055 | Rs 32,641.31 | Rs 6,000 | Rs 26,641.31 |

## Side breakdown

| Side | Trades | W-L | Win rate | PF | Net P&L | Average trade |
|---|---:|---:|---:|---:|---:|---:|
| LONG | 37 | 20-17 | 54.05% | 2.6438 | Rs 83,928.78 | Rs 2,268.35 |
| SHORT | 40 | 27-13 | 67.50% | 3.0379 | Rs 92,307.54 | Rs 2,307.69 |

## Setup breakdown

| Setup | Trades | W-L | Win rate | PF | Net P&L |
|---|---:|---:|---:|---:|---:|
| 1001_LONG | 7 | 4-3 | 57.14% | 4.5881 | Rs 34,984.07 |
| 0926_LONG | 8 | 7-1 | 87.50% | 12.2917 | Rs 29,580.35 |
| 0926_SHORT | 12 | 8-4 | 66.67% | 3.5368 | Rs 27,764.98 |
| 0941_SHORT | 7 | 4-3 | 57.14% | 3.7083 | Rs 26,405.63 |
| 0931_SHORT | 8 | 6-2 | 75.00% | 3.1529 | Rs 20,237.02 |
| 1121_SHORT | 3 | 3-0 | 100.00% | N/A | Rs 15,801.42 |
| 0936_LONG | 4 | 3-1 | 75.00% | 5.3037 | Rs 13,986.94 |
| 0951_SHORT | 7 | 5-2 | 71.43% | 2.5806 | Rs 10,273.82 |
| 0946_LONG | 1 | 1-0 | 100.00% | N/A | Rs 5,900.00 |
| 0931_LONG | 3 | 1-2 | 33.33% | 1.7051 | Rs 4,031.95 |
| 0941_LONG | 8 | 2-6 | 25.00% | 0.9967 | -Rs 54.52 |
| 0946_SHORT | 2 | 1-1 | 50.00% | 0.1206 | -Rs 3,825.33 |
| 0936_SHORT | 1 | 0-1 | 0.00% | 0.0000 | -Rs 4,350.00 |
| 0956_LONG | 6 | 2-4 | 33.33% | 0.6538 | -Rs 4,500.00 |

## Exit breakdown

| Exit | Trades | Wins | Losses | Net P&L | Average holding |
|---|---:|---:|---:|---:|---:|
| TARGET | 25 | 25 | 0 | Rs 2,00,100.00 | 100.72 min |
| TIME_EXIT_1515 | 26 | 22 | 4 | Rs 66,986.32 | 329.27 min |
| STOP | 26 | 0 | 26 | -Rs 90,850.00 | 52.27 min |

## Generalization warning

The seven sessions after the original 2026-09-11 research cutoff produced 10 selections, 9 executions, 3 wins, 6 losses, 33.33% win rate, PF 0.7954 and **-Rs 3,988.87 net**. This is materially weaker than the earlier history. The strategy evidence label is `EXPLORATORY_REUSED_HISTORY_NO_UNTOUCHED_TEST`; the exit table was fitted on reused history, so the headline is not clean out-of-sample evidence.

## Later daily replays and combined view through 2026-09-30

| Date | Selected | Trades | W-L | Net P&L |
|---|---:|---:|---:|---:|
| 2026-09-24 | 2 | 1 | 1-0 | Rs 7,414.16 |
| 2026-09-25 | 2 | 2 | 2-0 | Rs 14,150.00 |
| 2026-09-28 | 2 | 2 | 0-2 | -Rs 6,500.00 |
| 2026-09-29 | 1 | 1 | 0-1 | -Rs 3,250.00 |
| 2026-09-30 | 2 | 2 | 1-1 | Rs 6,500.00 |
| Added period | 9 | 8 | 4-4 | **Rs 18,314.16** |

Appending those five successful daily replays to the immutable bundle gives a descriptive 43-session view: 94 selections, 85 trades, 51 wins, 34 losses, 60.00% win rate, PF 2.7791, Rs 2,15,800.47 gross, Rs 21,250.00 costs, and **Rs 1,94,550.47 net**. Trades per session are 1.98 and the daily-close maximum drawdown remains Rs 16,250.00. This combined number is useful for monitoring but is not a newly sealed full-history artifact.

## Daily results — immutable bundle

| Date | Selected | Trades | W-L | Net P&L | Cumulative P&L | Drawdown |
|---|---:|---:|---:|---:|---:|---:|
| 2026-07-29 | 3 | 2 | 2-0 | Rs 16,093.15 | Rs 16,093.15 | Rs 0.00 |
| 2026-07-30 | 0 | 0 | 0-0 | Rs 0.00 | Rs 16,093.15 | Rs 0.00 |
| 2026-07-31 | 2 | 2 | 2-0 | Rs 10,150.00 | Rs 26,243.15 | Rs 0.00 |
| 2026-08-03 | 4 | 4 | 2-2 | Rs 11,650.00 | Rs 37,893.15 | Rs 0.00 |
| 2026-08-04 | 3 | 3 | 2-1 | Rs 11,208.27 | Rs 49,101.42 | Rs 0.00 |
| 2026-08-05 | 0 | 0 | 0-0 | Rs 0.00 | Rs 49,101.42 | Rs 0.00 |
| 2026-08-06 | 3 | 3 | 2-1 | Rs 12,530.61 | Rs 61,632.03 | Rs 0.00 |
| 2026-08-07 | 0 | 0 | 0-0 | Rs 0.00 | Rs 61,632.03 | Rs 0.00 |
| 2026-08-10 | 3 | 3 | 2-1 | Rs 13,250.00 | Rs 74,882.03 | Rs 0.00 |
| 2026-08-11 | 5 | 5 | 2-3 | Rs 8,500.00 | Rs 83,382.03 | Rs 0.00 |
| 2026-08-12 | 6 | 6 | 4-2 | Rs 4,663.37 | Rs 88,045.40 | Rs 0.00 |
| 2026-08-13 | 0 | 0 | 0-0 | Rs 0.00 | Rs 88,045.40 | Rs 0.00 |
| 2026-08-14 | 2 | 2 | 2-0 | Rs 8,567.70 | Rs 96,613.09 | Rs 0.00 |
| 2026-08-17 | 2 | 2 | 0-2 | -Rs 7,950.00 | Rs 88,663.09 | Rs 7,950.00 |
| 2026-08-18 | 3 | 3 | 2-1 | Rs 10,622.50 | Rs 99,285.59 | Rs 0.00 |
| 2026-08-19 | 2 | 2 | 2-0 | Rs 4,768.52 | Rs 1,04,054.11 | Rs 0.00 |
| 2026-08-20 | 2 | 2 | 1-1 | -Rs 4,063.06 | Rs 99,991.05 | Rs 4,063.06 |
| 2026-08-21 | 1 | 1 | 0-1 | -Rs 3,250.00 | Rs 96,741.05 | Rs 7,313.06 |
| 2026-08-26 | 2 | 2 | 2-0 | Rs 11,618.23 | Rs 1,08,359.29 | Rs 0.00 |
| 2026-08-27 | 4 | 4 | 2-2 | -Rs 1,575.33 | Rs 1,06,783.95 | Rs 1,575.33 |
| 2026-08-28 | 5 | 5 | 5-0 | Rs 32,961.05 | Rs 1,39,745.01 | Rs 0.00 |
| 2026-08-31 | 3 | 2 | 1-1 | Rs 9,850.00 | Rs 1,49,595.01 | Rs 0.00 |
| 2026-09-01 | 2 | 1 | 1-0 | Rs 4,600.00 | Rs 1,54,195.01 | Rs 0.00 |
| 2026-09-02 | 1 | 1 | 1-0 | Rs 11,900.00 | Rs 1,66,095.01 | Rs 0.00 |
| 2026-09-03 | 1 | 0 | 0-0 | Rs 0.00 | Rs 1,66,095.01 | Rs 0.00 |
| 2026-09-04 | 1 | 1 | 1-0 | Rs 4,600.00 | Rs 1,70,695.01 | Rs 0.00 |
| 2026-09-07 | 6 | 6 | 2-4 | -Rs 5,996.89 | Rs 1,64,698.11 | Rs 5,996.89 |
| 2026-09-08 | 1 | 0 | 0-0 | Rs 0.00 | Rs 1,64,698.11 | Rs 5,996.89 |
| 2026-09-09 | 3 | 3 | 1-2 | -Rs 4,080.46 | Rs 1,60,617.65 | Rs 10,077.36 |
| 2026-09-10 | 2 | 1 | 1-0 | Rs 3,870.65 | Rs 1,64,488.30 | Rs 6,206.71 |
| 2026-09-11 | 3 | 2 | 2-0 | Rs 15,736.89 | Rs 1,80,225.19 | Rs 0.00 |
| 2026-09-15 | 4 | 4 | 3-1 | Rs 12,261.13 | Rs 1,92,486.32 | Rs 0.00 |
| 2026-09-16 | 1 | 1 | 0-1 | -Rs 3,250.00 | Rs 1,89,236.32 | Rs 3,250.00 |
| 2026-09-17 | 1 | 1 | 0-1 | -Rs 3,250.00 | Rs 1,85,986.32 | Rs 6,500.00 |
| 2026-09-18 | 3 | 3 | 0-3 | -Rs 9,750.00 | Rs 1,76,236.32 | Rs 16,250.00 |
| 2026-09-21 | 0 | 0 | 0-0 | Rs 0.00 | Rs 1,76,236.32 | Rs 16,250.00 |
| 2026-09-22 | 0 | 0 | 0-0 | Rs 0.00 | Rs 1,76,236.32 | Rs 16,250.00 |
| 2026-09-23 | 1 | 0 | 0-0 | Rs 0.00 | Rs 1,76,236.32 | Rs 16,250.00 |

## Model and strategy assumptions

- Retained V13-v10-G rules: V13-v10-F core selections are preserved; only SHORT five-minute price thresholds are multiplied by 0.65, and newly eligible candidates fill vacant native setup quota.
- Rs 1,00,000 allocated capital per filled trade with modeled 5x exposure (Rs 5,00,000 position value).
- Rs 10,00,000 portfolio capital; no separate maximum-position cap. Observed peak was five concurrent positions.
- Flat 5 bps modeled round-trip cost, full exits, no partial exit or break-even stop.
- Pending entry expiry is 10 minutes; square-off is 15:15 IST.
- Rupee results use 5x modeled exposure and non-compounded fixed sizing. At 1x total position value, rupee P&L and drawdown would be one-fifth while selections, fills, win rate and PF remain unchanged.
- Daily-close drawdown is realized end-of-day drawdown, not intraday mark-to-market drawdown.

## Verification and source artifacts

- Corrected bundle: `C:\TradingData\eqidv2\fno_oi\strategy_research\v13_v10_g_full_history\run_20260925_cutoff_corrected_through_20260923`
- Bundle manifest: `bundle_manifest.json` — 22/22 artifact hashes verified, zero mismatches.
- Primary ledger: `g_backtest/portfolio_trades.csv`
- Selected-trade ledger: `g_backtest/selected_trades.csv`
- Headline summary: `g_backtest/summary.json`
- Run metadata: `g_backtest/run_metadata.json`
- Latest successful daily report: `C:\TradingData\eqidv2\backtesting_result_v13_v10_g\latest\latest_backtesting_result_v13_v10_g.md`

An attempted standalone replay against today’s mutable raw-data directory failed closed on source drift in `360ONE26SEPFUT_5minute.parquet`. No guard was bypassed. The immutable bundle and its saved strategy outputs remain hash-valid; exact regeneration requires the originally hashed raw source snapshot.
