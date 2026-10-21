# G3 10:35 one-minute extension guard — 14 October 2026

The 10:35 LONG and SHORT setups now reject an entry when the **completed 10:36 one-minute confirmation close** is more than **3.0 ATR14** beyond the one-minute EMA20 in the trade direction. For LONG, extension is `(close - EMA20) / ATR14`; for SHORT, it is `(EMA20 - close) / ATR14`. ATR14 is the simple mean of the last 14 completed one-minute true ranges, including the confirmation bar. At least 20 completed one-minute bars are required. Missing or non-finite inputs reject the candidate. Other setup clocks are unchanged. The existing stop losses and targets are unchanged.

The production/paper identity is `V13-V10-G-3-PRODUCTION-V9`; the 49-setup historical identity is `V13-V10-G-3-BACKTEST-TARGET-GT075-RESTORED3-V11`. The active profile remains at `PAPER_VALIDATION`, with live activation unauthorized. This is an entry filter, not an exit policy or a prediction of the session's high/low.

## Same-period historical comparison

Both packages use the 50 included sessions from 29 July through 13 October 2026. The 1 October session remains excluded for incomplete immutable input. Figures include the normal minute-level exit and portfolio allocation replay.

| Metric | Prior V10 | Guarded V11 | Change |
| --- | ---: | ---: | ---: |
| Selected orders, all setups | 530 | 519 | -11 |
| Executed trades | 458 | 451 | -7 |
| Wins / losses | 348 / 110 | 345 / 106 | -3 / -4 |
| Win rate | 75.98% | 76.50% | +0.51 percentage points |
| Net P&L | ₹12,41,425.20 | ₹12,33,323.84 | **-₹8,101.35** |
| Maximum drawdown | ₹10,233.39 | ₹10,233.39 | no change |
| 10:35 selections, both sides | 21 | 10 | -11 |
| 10:35 executed trades | 19 | 10 | -9 |
| 10:35 trade net P&L | ₹52,849.60 | ₹33,998.25 | -₹18,851.35 |

The guarded strategy still clears a 70% historical win rate, but it **reduces** net P&L on this sample. The smaller total P&L reduction than the 10:35 cohort reduction reflects capital reallocation to other entries.

Days with a changed total portfolio P&L (new minus prior):

| Day | Difference |
| --- | ---: |
| 2026-07-29 | +₹1,990.11 |
| 2026-08-06 | -₹10,350.00 |
| 2026-08-10 | -₹2,527.78 |
| 2026-08-11 | -₹10,350.00 |
| 2026-08-14 | +₹2,537.75 |
| 2026-08-18 | -₹406.63 |
| 2026-08-20 | +₹5,915.36 |
| 2026-08-26 | -₹1,919.64 |
| 2026-08-28 | +₹1,688.44 |
| 2026-09-07 | +₹6,248.63 |
| 2026-09-22 | -₹9,801.28 |
| 2026-09-28 | +₹3,139.50 |
| 2026-10-07 | +₹5,734.18 |

The verified V11 run is `C:\TradingData\eqidv2\backtesting_result_v13_v10_g_3\backtests\runs\2026-10-13\20261014T1710_1035_1m_extension_atr3_v11`. Its `report.html`, `summary.json`, `daily_results.csv`, `guarded_1035_selection.csv`, `portfolio_trades.csv`, and `manifest.json` contain the complete result and evidence. The archived V10 run is `C:\TradingData\eqidv2\backtesting_result_v13_v10_g_3\backtests\runs\2026-10-13\20261014T153247596531_bfdd5103_current_full_history`.

The older 10:35 candidate cache did not store one-minute EMA20/ATR14. The guard replay reconstructed them from causal one-minute OHLC and checked the 10:36 OHLC against the pinned candidate. Immutable daily snapshots cover the recent days, but 54 older underlying one-minute files differ in whole-file hash from the original sealed dataset. Therefore, this comparison is useful research evidence, not an independent sealed-data or out-of-sample qualification. The 3.0 threshold was proposed after inspecting UPL's 14 October entry; that day is not part of this 50-session result.

An early 14 October replay was blocked because no valid 09:20 opening inputs were available. The later after-close recovery produced a complete, verified daily replay. Its UPL 10:36 LONG candidate had a 3.477 ATR extension and was rejected by the new filter.

## Full end-of-day run through 14 October

The current verified 51-session V11 history is `C:\TradingData\eqidv2\backtesting_result_v13_v10_g_3\backtests\runs\2026-10-14\20261014T1812_1035_1m_extension_atr3_full_eod_v11`. It has **530 selected orders, 461 executed trades, 353 wins, 108 losses, 76.57% win rate, ₹12,60,941.23 net P&L, and ₹10,233.39 maximum drawdown**. The 14 October contribution was **10 trades, 8 wins, 2 losses, and +₹27,617.38 net**. The earlier 50-session guard comparison remains the same; adding the completed day moved cumulative net P&L from ₹12,33,323.84 to ₹12,60,941.23.

The 14 October replay included 212 of 213 stocks. AMBUJACEM was excluded for missing required futures OI; five further stocks were excluded from the 09:20 setup only. There were no blocking coverage problems. Both the scheduled run and an independent manual full-history rerun produced the same 51-session totals and passed package verification.
