# V13-v10-G-2 full backtest — staged 1.25% to 1.00% after 120 minutes

Generated: 2026-10-10T07:48:23.537770+00:00

Research-only retrospective exit sensitivity. G selections and targets are unchanged; every stop starts at 1.25% and tightens once to 1.00% after 120 minutes from entry. Live and paper configurations were not changed.

## Headline comparison

| Metric | V13-v10-G | V13-v10-G-2 |
|---|---:|---:|
| Selected orders | 105 | 105 |
| Executed trades | 96 | 96 |
| Wins | 57 | 66 |
| Losses | 39 | 30 |
| Win rate | 59.38% | 68.75% |
| Profit factor | 2.6231 | 2.7675 |
| Net P&L | Rs 207,998.10 | Rs 243,064.67 |
| Daily-close maximum drawdown | Rs 16,250.00 | Rs 14,142.14 |
| Median trade | Rs 1,629.02 | Rs 3,554.92 |
| Gross P&L | Rs 231,998.10 | Rs 267,064.67 |
| Modeled costs | Rs 24,000.00 | Rs 24,000.00 |
| Average trade | Rs 2,166.65 | Rs 2,531.92 |
| Net return on Rs 10 lakh capital | 20.80% | 24.31% |
| Peak concurrent positions | 5 | 6 |
| Peak reserved capital | Rs 500,000.00 | Rs 600,000.00 |
| Peak modeled gross exposure | Rs 2,500,000.00 | Rs 3,000,000.00 |
| Peak open initial risk | Rs 17,800.00 | Rs 39,000.00 |

G-2 net change versus G: **Rs 35,066.57**.
Of G's 35 stop exits, 15 were avoided: 5 later hit their unchanged targets and 10 reached the 15:15 exit. The remaining 20 stopped trades exited under the staged stop rule.

Post-2026-09-11 comparison: G produced 28 trades, 46.43% wins, PF 1.5414, and Rs 27,772.92; G-2 produced 28 trades, 60.71% wins, PF 2.1369, and Rs 58,987.90.

Best G-2 session: 2026-08-28, Rs 32,961.05. Worst G-2 session: 2026-09-28, -Rs 13,000.00.

## Monthly results

| Month | Sessions | Selected | Trades | W-L | Win rate | PF | Net P&L |
|---|---:|---:|---:|---:|---:|---:|---:|
| 2026-07 | 3 | 5 | 4 | 4-0 | 100.00% | N/A | Rs 26,243.15 |
| 2026-08 | 19 | 50 | 49 | 34-15 | 69.39% | 2.6468 | Rs 118,595.63 |
| 2026-09 | 21 | 39 | 32 | 21-11 | 65.62% | 3.0254 | Rs 85,078.25 |
| 2026-10 | 5 | 11 | 11 | 7-4 | 63.64% | 1.5595 | Rs 13,147.63 |

## Side results

| Group | Trades | W-L | Win rate | PF | Net P&L |
|---|---:|---:|---:|---:|---:|
| LONG | 42 | 26-16 | 61.90% | 2.2222 | Rs 85,008.60 |
| SHORT | 54 | 40-14 | 74.07% | 3.3255 | Rs 158,056.07 |

## Setup results

| Group | Trades | W-L | Win rate | PF | Net P&L |
|---|---:|---:|---:|---:|---:|
| 0941_SHORT | 10 | 7-3 | 70.00% | 4.1320 | Rs 51,065.40 |
| 0931_SHORT | 12 | 10-2 | 83.33% | 4.7259 | Rs 43,779.75 |
| 0926_LONG | 8 | 7-1 | 87.50% | 12.2917 | Rs 29,580.35 |
| 1001_LONG | 8 | 5-3 | 62.50% | 2.6330 | Rs 27,760.81 |
| 1121_SHORT | 5 | 5-0 | 100.00% | N/A | Rs 26,893.78 |
| 0926_SHORT | 15 | 9-6 | 60.00% | 2.0205 | Rs 25,233.82 |
| 0956_LONG | 8 | 6-2 | 75.00% | 3.6292 | Rs 18,473.69 |
| 0951_SHORT | 9 | 7-2 | 77.78% | 2.9445 | Rs 16,888.62 |
| 0936_LONG | 5 | 3-2 | 60.00% | 2.3878 | Rs 10,018.33 |
| 0931_LONG | 3 | 1-2 | 33.33% | 1.2633 | Rs 2,031.95 |
| 0946_LONG | 2 | 1-1 | 50.00% | 1.1238 | Rs 650.00 |
| 0936_SHORT | 1 | 1-0 | 100.00% | N/A | Rs 170.03 |
| 0941_LONG | 8 | 3-5 | 37.50% | 0.8457 | -Rs 3,506.53 |
| 0946_SHORT | 2 | 1-1 | 50.00% | 0.0807 | -Rs 5,975.33 |

## Exit results

| Group | Trades | W-L | Win rate | PF | Net P&L |
|---|---:|---:|---:|---:|---:|
| TIME_EXIT_1515 | 40 | 30-10 | 75.00% | 5.0768 | Rs 81,614.67 |
| TARGET | 36 | 36-0 | 100.00% | N/A | Rs 278,950.00 |
| STOP | 10 | 0-10 | 0.00% | 0.0000 | -Rs 65,000.00 |
| TIGHTENED_STOP | 10 | 0-10 | 0.00% | 0.0000 | -Rs 52,500.00 |

## Assumptions and evidence

- Window: 2026-07-29 through 2026-10-08, 48 eligible sessions.
- Initial stop 1.25%, tightened once to 1.00% after 120 minutes; both measured from actual entry price. Targets unchanged.
- Timer uses the entry candle end as a conservative fill-time proxy; activation occurs at the first eligible candle open.
- Gap-through stops fill at the adverse open; intrabar stop/target ties remain stop-first.
- Open-exit excursions exclude later exit-candle extrema; other candle extrema can include pre/post-fill movement.
- Rs 1,00,000 allocated per filled trade, modeled 5x exposure, Rs 10,00,000 portfolio capital.
- Flat 5 bps modeled round-trip cost, 10-minute entry expiry and 15:15 IST square-off.
- The retained 15:15 research exit is not a broker-realistic MIS deployment cutoff; a separate earlier-cutoff replay is required before live use.
- Full exits; no partial exit and no breakeven stop.
- Daily-close drawdown is realized end-of-day drawdown, not intraday mark-to-market drawdown.
- Evidence: `RETROSPECTIVE_EXIT_SENSITIVITY_REUSED_HISTORY_NOT_PROMOTED`. This reused history and is not an untouched holdout.
- The immutable source bundle was verified against every artifact hash before replay.
- No live or paper setting was changed; this output has no execution authority.

## Artifacts

- `selected_trades.csv`: order-level G-2 replay.
- `portfolio_trades.csv`: chronological portfolio ledger.
- `summary.json`: engine summary and fixed settings.
- `frozen_config.json`: exact G-2 configuration.
- `daily_results.csv`, `monthly_results.csv`, `side_results.csv`, `setup_results.csv`, `exit_results.csv`.
- `g_vs_g2_comparison.csv` and `stop_comparison.csv`.
- `trade_change_analysis.csv`: every materially changed trade and P&L delta.
- `provenance.json`: input and output hashes.
