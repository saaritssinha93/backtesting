# V13-v10-G-2 full backtest — staged 1.25% to 1.00% after 120 minutes

Generated: 2026-10-10T07:48:20.715893+00:00

Research-only retrospective exit sensitivity. G selections and targets are unchanged; every stop starts at 1.25% and tightens once to 1.00% after 120 minutes from entry. Live and paper configurations were not changed.

## Headline comparison

| Metric | V13-v10-G | V13-v10-G-2 |
|---|---:|---:|
| Selected orders | 95 | 95 |
| Executed trades | 86 | 86 |
| Wins | 51 | 60 |
| Losses | 35 | 26 |
| Win rate | 59.30% | 69.77% |
| Profit factor | 2.6989 | 3.0551 |
| Net P&L | Rs 191,300.47 | Rs 234,317.04 |
| Daily-close maximum drawdown | Rs 16,250.00 | Rs 14,142.14 |
| Median trade | Rs 2,043.73 | Rs 4,041.20 |
| Gross P&L | Rs 212,800.47 | Rs 255,817.04 |
| Modeled costs | Rs 21,500.00 | Rs 21,500.00 |
| Average trade | Rs 2,224.42 | Rs 2,724.62 |
| Net return on Rs 10 lakh capital | 19.13% | 23.43% |
| Peak concurrent positions | 5 | 6 |
| Peak reserved capital | Rs 500,000.00 | Rs 600,000.00 |
| Peak modeled gross exposure | Rs 2,500,000.00 | Rs 3,000,000.00 |
| Peak open initial risk | Rs 17,800.00 | Rs 39,000.00 |

G-2 net change versus G: **Rs 43,016.57**.
Of G's 31 stop exits, 15 were avoided: 5 later hit their unchanged targets and 10 reached the 15:15 exit. The remaining 16 stopped trades exited under the staged stop rule.

Post-2026-09-11 comparison: G produced 18 trades, 38.89% wins, PF 1.3098, and Rs 11,075.29; G-2 produced 18 trades, 61.11% wins, PF 2.7698, and Rs 50,240.28.

Best G-2 session: 2026-08-28, Rs 32,961.05. Worst G-2 session: 2026-09-28, -Rs 13,000.00.

## Monthly results

| Month | Sessions | Selected | Trades | W-L | Win rate | PF | Net P&L |
|---|---:|---:|---:|---:|---:|---:|---:|
| 2026-07 | 3 | 5 | 4 | 4-0 | 100.00% | N/A | Rs 26,243.15 |
| 2026-08 | 19 | 50 | 49 | 34-15 | 69.39% | 2.6468 | Rs 118,595.63 |
| 2026-09 | 21 | 39 | 32 | 21-11 | 65.62% | 3.0254 | Rs 85,078.25 |
| 2026-10 | 2 | 1 | 1 | 1-0 | 100.00% | N/A | Rs 4,400.00 |

## Side results

| Group | Trades | W-L | Win rate | PF | Net P&L |
|---|---:|---:|---:|---:|---:|
| LONG | 37 | 24-13 | 64.86% | 2.8597 | Rs 97,731.85 |
| SHORT | 49 | 36-13 | 73.47% | 3.2221 | Rs 136,585.19 |

## Setup results

| Group | Trades | W-L | Win rate | PF | Net P&L |
|---|---:|---:|---:|---:|---:|
| 0931_SHORT | 10 | 9-1 | 90.00% | 9.6288 | Rs 45,301.22 |
| 0941_SHORT | 9 | 6-3 | 66.67% | 3.4021 | Rs 39,165.40 |
| 0926_LONG | 8 | 7-1 | 87.50% | 12.2917 | Rs 29,580.35 |
| 1001_LONG | 7 | 4-3 | 57.14% | 2.6314 | Rs 27,734.07 |
| 0926_SHORT | 15 | 9-6 | 60.00% | 2.0205 | Rs 25,233.82 |
| 0956_LONG | 6 | 5-1 | 83.33% | 11.9630 | Rs 19,473.69 |
| 0951_SHORT | 9 | 7-2 | 77.78% | 2.9445 | Rs 16,888.62 |
| 0936_LONG | 4 | 3-1 | 75.00% | 23.9867 | Rs 16,518.33 |
| 1121_SHORT | 3 | 3-0 | 100.00% | N/A | Rs 15,801.42 |
| 0946_LONG | 1 | 1-0 | 100.00% | N/A | Rs 5,900.00 |
| 0931_LONG | 3 | 1-2 | 33.33% | 1.2633 | Rs 2,031.95 |
| 0936_SHORT | 1 | 1-0 | 100.00% | N/A | Rs 170.03 |
| 0941_LONG | 8 | 3-5 | 37.50% | 0.8457 | -Rs 3,506.53 |
| 0946_SHORT | 2 | 1-1 | 50.00% | 0.0807 | -Rs 5,975.33 |

## Exit results

| Group | Trades | W-L | Win rate | PF | Net P&L |
|---|---:|---:|---:|---:|---:|
| TIME_EXIT_1515 | 37 | 27-10 | 72.97% | 4.7597 | Rs 75,267.04 |
| TARGET | 33 | 33-0 | 100.00% | N/A | Rs 253,050.00 |
| STOP | 8 | 0-8 | 0.00% | 0.0000 | -Rs 52,000.00 |
| TIGHTENED_STOP | 8 | 0-8 | 0.00% | 0.0000 | -Rs 42,000.00 |

## Assumptions and evidence

- Window: 2026-07-29 through 2026-10-05, 45 eligible sessions.
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
