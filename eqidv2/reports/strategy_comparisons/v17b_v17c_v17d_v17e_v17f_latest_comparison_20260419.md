# Latest v17b vs v17c vs v17d vs v17e vs v17f Result Comparison

Comparison date: 2026-04-19

Note: the latest result CSV filenames still carry a `v16_5min` suffix, but the output folders and console logs confirm the actual strategy version.

## 1. Latest artifacts used
| Version | Run timestamp | Console log | Trades CSV | Daywise CSV | Notes |
|---|---|---|---|---|---|
| v17b latest | 2026-04-18 20:28:05 | C:\TradingData\eqidv2\outputs_v17b_5min\avwap_combined_runner_20260418_202805.txt | C:\TradingData\eqidv2\outputs_v17b_5min\avwap_longshort_trades_v16_5min_ALL_DAYS_20260418_202805.csv | C:\TradingData\eqidv2\outputs_v17b_5min\avwap_daywise_breakdown_v16_5min_ALL_DAYS_20260418_202805.csv | Latest v17b run. |
| v17c latest | 2026-04-18 21:02:36 | C:\TradingData\eqidv2\outputs_v17c_5min\avwap_combined_runner_20260418_210236.txt | C:\TradingData\eqidv2\outputs_v17c_5min\avwap_longshort_trades_v16_5min_ALL_DAYS_20260418_210236.csv | C:\TradingData\eqidv2\outputs_v17c_5min\avwap_daywise_breakdown_v16_5min_ALL_DAYS_20260418_210236.csv | Latest v17c run. |
| v17d latest | 2026-04-18 22:54:30 | C:\TradingData\eqidv2\outputs_v17d_5min\avwap_combined_runner_20260418_225430.txt | C:\TradingData\eqidv2\outputs_v17d_5min\avwap_longshort_trades_v16_5min_ALL_DAYS_20260418_225430.csv | C:\TradingData\eqidv2\outputs_v17d_5min\avwap_daywise_breakdown_v16_5min_ALL_DAYS_20260418_225430.csv | Latest v17d run; aggressive BOTH RS 0.50% research variant. |
| v17e latest completed | 2026-04-19 14:43:55 | C:\TradingData\eqidv2\outputs_v17e_5min\avwap_combined_runner_20260419_144355.txt | C:\TradingData\eqidv2\outputs_v17e_5min\avwap_longshort_trades_v16_5min_ALL_DAYS_20260419_144355.csv | C:\TradingData\eqidv2\outputs_v17e_5min\avwap_daywise_breakdown_v16_5min_ALL_DAYS_20260419_144355.csv | Latest completed v17e run. Ignored newer zero-byte/incomplete log at 2026-04-19 21:54:55. |
| v17f latest | 2026-04-19 12:52:08 | C:\TradingData\eqidv2\outputs_v17f_5min\avwap_combined_runner_20260419_125208.txt | C:\TradingData\eqidv2\outputs_v17f_5min\avwap_longshort_trades_v16_5min_ALL_DAYS_20260419_125208.csv | C:\TradingData\eqidv2\outputs_v17f_5min\avwap_daywise_breakdown_v16_5min_ALL_DAYS_20260419_125208.csv | Latest v17f run. |

## 2. Filter flow and runtime
| Metric | v17b latest | v17c latest | v17d latest | v17e latest completed | v17f latest |
|---|---|---|---|---|---|
| V16_FILTER | [V16_FILTER] SHORT: 448->371 (-77 RSI 30-40 dead zone, -0 OR gate) \| LONG: 2202->990 (-178 QS 7.6-7.9dead+QS>12, -580 dist>3.0ATR, -327 dist 1.0-1.5ATR dead, -127 vol>5x exhaust, -0 OR gate) | [V16_FILTER] SHORT: 448->371 (-77 RSI 30-40 dead zone, -0 OR gate) \| LONG: 2202->990 (-178 QS 7.6-7.9dead+QS>12, -580 dist>3.0ATR, -327 dist 1.0-1.5ATR dead, -127 vol>5x exhaust, -0 OR gate) | [V16_FILTER] SHORT: 709->556 (-153 RSI 30-40 dead zone, -0 OR gate) \| LONG: 2202->990 (-178 QS 7.6-7.9dead+QS>12, -580 dist>3.0ATR, -327 dist 1.0-1.5ATR dead, -127 vol>5x exhaust, -0 OR gate) | [V16_FILTER] SHORT: 645->504 (-141 RSI 30-40 dead zone, -0 OR gate) \| LONG: 2202->990 (-178 QS 7.6-7.9dead+QS>12, -580 dist>3.0ATR, -327 dist 1.0-1.5ATR dead, -127 vol>5x exhaust, -0 OR gate) | [V16_FILTER] SHORT: 709->556 (-153 RSI 30-40 dead zone, -0 OR gate) \| LONG: 2202->990 (-178 QS 7.6-7.9dead+QS>12, -580 dist>3.0ATR, -327 dist 1.0-1.5ATR dead, -127 vol>5x exhaust, -0 OR gate) |
| V17B_FILTER | [V17B_FILTER] SHORT: 371->164 (-49 SO-RSI[21,28) -37 SO-ADX>=44 -26 pullback -43 open<09:45 -45 AVWAP[0.5,1.0) -7 BOTH-ADX[25,30)) \| LONG: 990->914 (-19 late-A_MOD_BR bars>=13 vol>=3.7x -57 RSI[60,65)) |  |  |  |  |
| V17C_FILTER |  | [V17C_FILTER] SHORT: 371->259 (-49 SHORT_ONLY RSI [21.0,28.0), -37 SHORT_ONLY ADX>=44.0, -26 pullback setup (+0 strong-trend pullback exceptions)) \| LONG: 990->960 (-30 late A_MOD_BREAK_C1_HIGH bars>=12, vol>=3.5x) |  |  |  |
| V17D_FILTER |  |  | [V17D_FILTER] SHORT: 556->294 (-48 pullback \| -64 rs_nan \| -18 rs<=-2.00% \| -10 adx[20.0,25.0) \| -40 adx>=50.0 \| -50 BOTH avwap[0.50,1.00) \| -27 BOTH time pockets \| -5 late gate after 13:30) \| LONG: 990->960 (-30 late A_MOD_BREAK_C1_HIGH bars>=12, vol>=3.5x) | [V17D_FILTER] SHORT: 504->260 (-44 pullback \| -64 rs_nan \| -22 rs<=-1.80% \| -21 adx[20.0,25.0) \| -36 adx>=50.0 \| -33 BOTH avwap[0.50,1.00) \| -22 BOTH time pockets \| -2 late gate after 13:30) \| LONG: 990->960 (-30 late A_MOD_BREAK_C1_HIGH bars>=12, vol>=3.5x) | [V17D_FILTER] SHORT: 556->294 (-48 pullback \| -64 rs_nan \| -18 rs<=-2.00% \| -10 adx[20.0,25.0) \| -40 adx>=50.0 \| -50 BOTH avwap[0.50,1.00) \| -27 BOTH time pockets \| -5 late gate after 13:30) \| LONG: 990->960 (-30 late A_MOD_BREAK_C1_HIGH bars>=12, vol>=3.5x) |
| V17E_FILTER |  |  |  | [V17E_FILTER] SHORT: 260->158 (-55 rsi[20,25) \| -13 avwap>=2.00 \| -21 adx[45,50) \| -13 SHORT_ONLY rs>-0.40% \| -0 rs[-0.25,0.00)) |  |
| V17F_FILTER |  |  |  |  | [V17F_FILTER] SHORT extra: 294->269 (-4 SHORT_ONLY rs>-0.25% \| -15 SHORT_ONLY atr_pct>=0.70% \| -6 BOTH 12:15-12:45) |
| Phase 1 runtime | 1603.9s | 1407.9s | 1050.7s | 1229.1s | 993.2s |
| Phase 2 runtime | 37.8s | 31.1s | 31.1s | 27.9s | 33.6s |
| Total runtime | 1649.5s | 1434.4s | 1083.3s | 1257.0s | 1033.3s |

## 3. Combined headline metrics
| Metric | v17b latest | v17c latest | v17d latest | v17e latest completed | v17f latest |
|---|---|---|---|---|---|
| Total trades | 1078 | 1219 | 1254 | 1118 | 1229 |
| Unique trade days | 183 | 192 | 192 | 179 | 191 |
| TARGET hits | 763  \| hit-rate  = 70.78% | 846  \| hit-rate  = 69.40% | 864  \| hit-rate  = 68.90% | 786  \| hit-rate  = 70.30% | 854  \| hit-rate  = 69.49% |
| SL hits | 264  \| sl-rate   = 24.49% | 311  \| sl-rate   = 25.51% | 320  \| sl-rate   = 25.52% | 275  \| sl-rate   = 24.60% | 306  \| sl-rate   = 24.90% |
| EOD exits | 51  \| eod-rate  = 4.73% | 62  \| eod-rate  = 5.09% | 70  \| eod-rate  = 5.58% | 57  \| eod-rate  = 5.10% | 69  \| eod-rate  = 5.61% |
| Avg PnL % (net, per trade) | 1.0741% | 0.9859% | 0.9648% | 1.0486% | 1.0145% |
| Sum PnL % (net, all trades) | 1157.8342% | 1201.8242% | 1209.8867% | 1172.3875% | 1246.8526% |
| Profit factor | 1.892 | 1.786 | 1.767 | 1.862 | 1.826 |
| Max drawdown (cumul PnL %) | 43.5182% | 58.9204% | 48.2659% | 52.8541% | 44.8046% |
| Sharpe ratio (annualized) | 4.975 | 4.511 | 4.416 | 4.846 | 4.679 |
| Sortino ratio (annualized) | 15.219 | 14.734 | 13.800 | 15.196 | 14.268 |
| Calmar ratio | 26.606 | 20.397 | 25.067 | 22.182 | 27.829 |

## 4. Short-side metrics
| Metric | v17b latest | v17c latest | v17d latest | v17e latest completed | v17f latest |
|---|---|---|---|---|---|
| Total trades | 164 | 259 | 294 | 158 | 269 |
| Unique trade days | 77 | 93 | 93 | 70 | 89 |
| TARGET hits | 122  \| hit-rate  = 74.39% | 173  \| hit-rate  = 66.80% | 191  \| hit-rate  = 64.97% | 113  \| hit-rate  = 71.52% | 181  \| hit-rate  = 67.29% |
| SL hits | 33  \| sl-rate   = 20.12% | 68  \| sl-rate   = 26.25% | 77  \| sl-rate   = 26.19% | 32  \| sl-rate   = 20.25% | 63  \| sl-rate   = 23.42% |
| EOD exits | 9  \| eod-rate  = 5.49% | 18  \| eod-rate  = 6.95% | 26  \| eod-rate  = 8.84% | 13  \| eod-rate  = 8.23% | 25  \| eod-rate  = 9.29% |
| Avg PnL % (net, per trade) | 1.4301% | 0.9171% | 0.8353% | 1.3170% | 1.0504% |
| Sum PnL % (net, all trades) | 234.5346% | 237.5269% | 245.5894% | 208.0903% | 282.5554% |
| Profit factor | 2.463 | 1.719 | 1.648 | 2.296 | 1.911 |
| Max drawdown (cumul PnL %) | 20.3056% | 31.2090% | 23.4763% | 16.0121% | 21.8068% |
| Sharpe ratio (annualized) | 7.093 | 4.179 | 3.815 | 6.471 | 4.966 |
| Sortino ratio (annualized) | 17.599 | 14.726 | 11.069 | 17.411 | 12.900 |
| Calmar ratio | 11.550 | 7.611 | 10.461 | 12.996 | 12.957 |

## 5. Long-side metrics
| Metric | v17b latest | v17c latest | v17d latest | v17e latest completed | v17f latest |
|---|---|---|---|---|---|
| Total trades | 914 | 960 | 960 | 960 | 960 |
| Unique trade days | 132 | 134 | 134 | 134 | 134 |
| TARGET hits | 641  \| hit-rate  = 70.13% | 673  \| hit-rate  = 70.10% | 673  \| hit-rate  = 70.10% | 673  \| hit-rate  = 70.10% | 673  \| hit-rate  = 70.10% |
| SL hits | 231  \| sl-rate   = 25.27% | 243  \| sl-rate   = 25.31% | 243  \| sl-rate   = 25.31% | 243  \| sl-rate   = 25.31% | 243  \| sl-rate   = 25.31% |
| EOD exits | 42  \| eod-rate  = 4.60% | 44  \| eod-rate  = 4.58% | 44  \| eod-rate  = 4.58% | 44  \| eod-rate  = 4.58% | 44  \| eod-rate  = 4.58% |
| Avg PnL % (net, per trade) | 1.0102% | 1.0045% | 1.0045% | 1.0045% | 1.0045% |
| Sum PnL % (net, all trades) | 923.2996% | 964.2972% | 964.2972% | 964.2972% | 964.2972% |
| Profit factor | 1.812 | 1.804 | 1.804 | 1.804 | 1.804 |
| Max drawdown (cumul PnL %) | 44.1979% | 48.8968% | 48.8968% | 48.8968% | 48.8968% |
| Sharpe ratio (annualized) | 4.630 | 4.599 | 4.599 | 4.599 | 4.599 |
| Sortino ratio (annualized) | 14.630 | 14.735 | 14.735 | 14.735 | 14.735 |
| Calmar ratio | 20.890 | 19.721 | 19.721 | 19.721 | 19.721 |

## 6. Daily trade count comparison (all days, total trades)
| Date | v17b latest | v17c latest | v17d latest | v17e latest completed | v17f latest |
|---|---|---|---|---|---|
| 2025-06-03 | 2 | 2 | 2 | 2 | 2 |
| 2025-06-04 | 7 | 7 | 7 | 7 | 7 |
| 2025-06-05 | 8 | 9 | 9 | 9 | 9 |
| 2025-06-06 | 10 | 13 | 13 | 13 | 13 |
| 2025-06-09 | 1 | 1 | 2 | 1 | 2 |
| 2025-06-10 | 3 | 4 | 4 | 4 | 4 |
| 2025-06-11 | 14 | 15 | 15 | 15 | 15 |
| 2025-06-12 | 0 | 2 | 3 | 2 | 3 |
| 2025-06-16 | 4 | 5 | 5 | 5 | 5 |
| 2025-06-17 | 1 | 1 | 2 | 0 | 2 |
| 2025-06-18 | 5 | 8 | 6 | 6 | 6 |
| 2025-06-19 | 6 | 5 | 5 | 5 | 5 |
| 2025-06-20 | 8 | 10 | 10 | 10 | 9 |
| 2025-06-23 | 1 | 1 | 1 | 0 | 1 |
| 2025-06-24 | 11 | 11 | 11 | 11 | 11 |
| 2025-06-25 | 18 | 18 | 18 | 18 | 18 |
| 2025-06-26 | 9 | 9 | 9 | 9 | 9 |
| 2025-06-27 | 6 | 5 | 5 | 5 | 5 |
| 2025-06-30 | 3 | 3 | 3 | 3 | 3 |
| 2025-07-01 | 2 | 2 | 2 | 2 | 2 |
| 2025-07-02 | 3 | 3 | 5 | 2 | 5 |
| 2025-07-03 | 9 | 9 | 9 | 9 | 9 |
| 2025-07-07 | 4 | 5 | 4 | 4 | 4 |
| 2025-07-08 | 1 | 1 | 1 | 1 | 1 |
| 2025-07-09 | 9 | 8 | 8 | 8 | 8 |
| 2025-07-10 | 0 | 1 | 1 | 0 | 1 |
| 2025-07-11 | 1 | 1 | 2 | 1 | 2 |
| 2025-07-15 | 15 | 16 | 16 | 16 | 16 |
| 2025-07-17 | 0 | 2 | 1 | 1 | 1 |
| 2025-07-18 | 1 | 2 | 3 | 3 | 2 |
| 2025-07-21 | 6 | 6 | 7 | 6 | 7 |
| 2025-07-22 | 2 | 2 | 2 | 2 | 2 |
| 2025-07-24 | 2 | 2 | 2 | 2 | 2 |
| 2025-07-25 | 4 | 5 | 10 | 4 | 10 |
| 2025-07-28 | 0 | 0 | 2 | 1 | 2 |
| 2025-07-29 | 3 | 4 | 2 | 1 | 2 |
| 2025-07-30 | 5 | 6 | 5 | 5 | 5 |
| 2025-07-31 | 2 | 2 | 4 | 2 | 4 |
| 2025-08-01 | 2 | 2 | 3 | 2 | 3 |
| 2025-08-04 | 3 | 3 | 3 | 3 | 3 |
| 2025-08-05 | 1 | 1 | 3 | 0 | 2 |
| 2025-08-06 | 2 | 2 | 3 | 1 | 3 |
| 2025-08-07 | 2 | 2 | 3 | 2 | 3 |
| 2025-08-08 | 1 | 2 | 2 | 2 | 2 |
| 2025-08-11 | 6 | 6 | 6 | 5 | 6 |
| 2025-08-12 | 7 | 7 | 7 | 7 | 6 |
| 2025-08-13 | 8 | 11 | 10 | 10 | 10 |
| 2025-08-14 | 2 | 2 | 2 | 2 | 2 |
| 2025-08-18 | 17 | 17 | 17 | 17 | 17 |
| 2025-08-19 | 9 | 10 | 10 | 10 | 10 |
| 2025-08-20 | 6 | 7 | 8 | 7 | 8 |
| 2025-08-21 | 3 | 3 | 3 | 3 | 3 |
| 2025-08-25 | 8 | 9 | 9 | 9 | 9 |
| 2025-08-26 | 1 | 15 | 10 | 7 | 9 |
| 2025-08-28 | 0 | 1 | 0 | 0 | 0 |
| 2025-08-29 | 3 | 4 | 4 | 4 | 4 |
| 2025-09-01 | 14 | 14 | 14 | 14 | 14 |
| 2025-09-02 | 24 | 25 | 25 | 25 | 25 |
| 2025-09-03 | 7 | 8 | 8 | 8 | 8 |
| 2025-09-04 | 13 | 13 | 13 | 13 | 13 |
| 2025-09-05 | 4 | 4 | 5 | 4 | 5 |
| 2025-09-08 | 10 | 10 | 10 | 10 | 10 |
| 2025-09-09 | 6 | 6 | 6 | 6 | 6 |
| 2025-09-10 | 14 | 14 | 14 | 14 | 14 |
| 2025-09-11 | 6 | 5 | 6 | 5 | 6 |
| 2025-09-12 | 4 | 5 | 5 | 5 | 5 |
| 2025-09-15 | 6 | 7 | 7 | 7 | 7 |
| 2025-09-16 | 9 | 9 | 9 | 9 | 9 |
| 2025-09-17 | 6 | 6 | 6 | 6 | 6 |
| 2025-09-18 | 8 | 8 | 8 | 8 | 8 |
| 2025-09-19 | 1 | 2 | 2 | 2 | 2 |
| 2025-09-22 | 3 | 3 | 3 | 3 | 3 |
| 2025-09-23 | 0 | 1 | 0 | 0 | 0 |
| 2025-09-24 | 1 | 1 | 0 | 0 | 0 |
| 2025-09-25 | 0 | 0 | 1 | 0 | 1 |
| 2025-09-26 | 3 | 5 | 2 | 2 | 2 |
| 2025-09-29 | 12 | 12 | 12 | 12 | 12 |
| 2025-09-30 | 2 | 4 | 2 | 2 | 2 |
| 2025-10-01 | 8 | 9 | 9 | 9 | 9 |
| 2025-10-03 | 5 | 5 | 5 | 5 | 5 |
| 2025-10-06 | 9 | 10 | 10 | 10 | 10 |
| 2025-10-07 | 11 | 9 | 9 | 9 | 9 |
| 2025-10-08 | 4 | 4 | 3 | 3 | 3 |
| 2025-10-09 | 6 | 6 | 5 | 5 | 5 |
| 2025-10-10 | 22 | 23 | 23 | 23 | 23 |
| 2025-10-13 | 2 | 3 | 3 | 1 | 2 |
| 2025-10-15 | 8 | 8 | 8 | 8 | 8 |
| 2025-10-16 | 10 | 11 | 11 | 11 | 11 |
| 2025-10-17 | 8 | 8 | 8 | 8 | 8 |
| 2025-10-20 | 4 | 4 | 4 | 4 | 4 |
| 2025-10-23 | 14 | 15 | 15 | 15 | 15 |
| 2025-10-27 | 6 | 6 | 6 | 6 | 6 |
| 2025-10-28 | 7 | 7 | 6 | 6 | 6 |
| 2025-10-29 | 9 | 9 | 9 | 9 | 9 |
| 2025-10-30 | 0 | 0 | 1 | 0 | 1 |
| 2025-10-31 | 3 | 3 | 3 | 3 | 3 |
| 2025-11-03 | 6 | 6 | 6 | 6 | 6 |
| 2025-11-04 | 0 | 0 | 1 | 0 | 1 |
| 2025-11-06 | 1 | 1 | 4 | 0 | 4 |
| 2025-11-07 | 2 | 2 | 2 | 1 | 2 |
| 2025-11-10 | 5 | 5 | 5 | 5 | 5 |
| 2025-11-11 | 2 | 3 | 0 | 0 | 0 |
| 2025-11-12 | 9 | 9 | 9 | 9 | 9 |
| 2025-11-13 | 5 | 7 | 7 | 7 | 7 |
| 2025-11-14 | 1 | 3 | 3 | 2 | 3 |
| 2025-11-18 | 2 | 4 | 8 | 2 | 7 |
| 2025-11-19 | 3 | 3 | 3 | 3 | 3 |
| 2025-11-20 | 9 | 9 | 9 | 9 | 9 |
| 2025-11-21 | 2 | 2 | 2 | 1 | 2 |
| 2025-11-24 | 4 | 4 | 6 | 2 | 6 |
| 2025-11-25 | 1 | 1 | 0 | 0 | 0 |
| 2025-11-26 | 13 | 13 | 13 | 13 | 13 |
| 2025-11-27 | 2 | 4 | 4 | 4 | 4 |
| 2025-11-28 | 8 | 8 | 9 | 9 | 9 |
| 2025-12-01 | 4 | 4 | 4 | 4 | 4 |
| 2025-12-02 | 0 | 0 | 1 | 0 | 1 |
| 2025-12-03 | 0 | 1 | 4 | 2 | 3 |
| 2025-12-04 | 9 | 9 | 9 | 9 | 9 |
| 2025-12-05 | 1 | 1 | 1 | 1 | 1 |
| 2025-12-08 | 2 | 3 | 4 | 2 | 4 |
| 2025-12-09 | 2 | 2 | 6 | 3 | 6 |
| 2025-12-10 | 8 | 8 | 7 | 7 | 7 |
| 2025-12-11 | 1 | 1 | 1 | 1 | 1 |
| 2025-12-12 | 4 | 4 | 4 | 4 | 4 |
| 2025-12-16 | 2 | 2 | 4 | 2 | 4 |
| 2025-12-17 | 3 | 3 | 4 | 4 | 4 |
| 2025-12-18 | 2 | 3 | 3 | 3 | 3 |
| 2025-12-19 | 7 | 7 | 7 | 7 | 7 |
| 2025-12-22 | 11 | 11 | 11 | 11 | 11 |
| 2025-12-23 | 2 | 2 | 1 | 1 | 1 |
| 2025-12-24 | 4 | 4 | 4 | 4 | 4 |
| 2025-12-26 | 2 | 2 | 2 | 0 | 2 |
| 2025-12-29 | 3 | 3 | 2 | 2 | 2 |
| 2025-12-30 | 2 | 2 | 2 | 2 | 2 |
| 2025-12-31 | 11 | 11 | 11 | 11 | 11 |
| 2026-01-01 | 2 | 3 | 3 | 3 | 3 |
| 2026-01-02 | 21 | 22 | 22 | 22 | 22 |
| 2026-01-05 | 4 | 5 | 5 | 5 | 5 |
| 2026-01-06 | 2 | 2 | 1 | 1 | 1 |
| 2026-01-07 | 2 | 2 | 3 | 2 | 3 |
| 2026-01-08 | 3 | 5 | 4 | 3 | 4 |
| 2026-01-09 | 3 | 14 | 8 | 4 | 8 |
| 2026-01-12 | 0 | 0 | 1 | 0 | 0 |
| 2026-01-13 | 1 | 1 | 1 | 1 | 1 |
| 2026-01-14 | 2 | 2 | 2 | 2 | 2 |
| 2026-01-16 | 10 | 13 | 13 | 13 | 13 |
| 2026-01-19 | 1 | 1 | 1 | 1 | 1 |
| 2026-01-20 | 4 | 4 | 5 | 4 | 4 |
| 2026-01-21 | 5 | 7 | 12 | 7 | 8 |
| 2026-01-22 | 9 | 9 | 9 | 9 | 9 |
| 2026-01-27 | 1 | 1 | 0 | 0 | 0 |
| 2026-01-28 | 2 | 2 | 2 | 2 | 2 |
| 2026-01-29 | 4 | 5 | 5 | 4 | 5 |
| 2026-02-01 | 8 | 9 | 8 | 8 | 8 |
| 2026-02-02 | 3 | 3 | 2 | 1 | 2 |
| 2026-02-03 | 5 | 5 | 5 | 5 | 5 |
| 2026-02-04 | 8 | 9 | 7 | 7 | 7 |
| 2026-02-05 | 3 | 4 | 6 | 2 | 5 |
| 2026-02-06 | 3 | 6 | 8 | 6 | 8 |
| 2026-02-09 | 11 | 13 | 13 | 13 | 13 |
| 2026-02-10 | 11 | 11 | 11 | 11 | 11 |
| 2026-02-11 | 3 | 3 | 3 | 3 | 3 |
| 2026-02-12 | 0 | 1 | 2 | 0 | 1 |
| 2026-02-13 | 2 | 2 | 6 | 3 | 5 |
| 2026-02-16 | 4 | 6 | 5 | 5 | 5 |
| 2026-02-17 | 11 | 11 | 11 | 11 | 11 |
| 2026-02-18 | 5 | 5 | 6 | 5 | 6 |
| 2026-02-23 | 4 | 4 | 4 | 4 | 4 |
| 2026-02-24 | 0 | 2 | 6 | 4 | 6 |
| 2026-02-25 | 12 | 15 | 15 | 15 | 15 |
| 2026-02-26 | 12 | 12 | 13 | 12 | 12 |
| 2026-02-27 | 2 | 5 | 8 | 5 | 8 |
| 2026-03-02 | 2 | 4 | 3 | 2 | 3 |
| 2026-03-04 | 3 | 4 | 3 | 2 | 3 |
| 2026-03-06 | 4 | 4 | 3 | 3 | 3 |
| 2026-03-09 | 1 | 2 | 1 | 1 | 1 |
| 2026-03-10 | 11 | 12 | 12 | 12 | 12 |
| 2026-03-11 | 3 | 4 | 3 | 2 | 2 |
| 2026-03-12 | 2 | 2 | 1 | 1 | 1 |
| 2026-03-13 | 13 | 14 | 21 | 12 | 21 |
| 2026-03-16 | 5 | 7 | 6 | 4 | 6 |
| 2026-03-17 | 14 | 14 | 13 | 13 | 13 |
| 2026-03-18 | 20 | 23 | 23 | 23 | 23 |
| 2026-03-19 | 1 | 4 | 10 | 4 | 7 |
| 2026-03-20 | 8 | 9 | 7 | 7 | 6 |
| 2026-03-23 | 7 | 11 | 16 | 9 | 15 |
| 2026-03-24 | 6 | 7 | 6 | 6 | 6 |
| 2026-03-25 | 28 | 29 | 29 | 29 | 29 |
| 2026-03-27 | 6 | 7 | 6 | 2 | 6 |
| 2026-03-30 | 3 | 4 | 3 | 2 | 3 |
| 2026-04-01 | 18 | 18 | 18 | 18 | 18 |
| 2026-04-02 | 1 | 3 | 3 | 3 | 2 |
| 2026-04-06 | 2 | 4 | 3 | 2 | 3 |
| 2026-04-07 | 1 | 1 | 1 | 0 | 1 |
| 2026-04-08 | 1 | 2 | 2 | 2 | 2 |
| 2026-04-09 | 0 | 2 | 2 | 2 | 2 |
| 2026-04-10 | 7 | 7 | 7 | 7 | 7 |
| 2026-04-17 | 17 | 18 | 18 | 18 | 18 |

## 7. Daily count comparison file
- Full daily comparison CSV with total/long/short counts and net/notional PnL per version: `C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\reports\strategy_comparisons\v17b_v17c_v17d_v17e_v17f_daily_counts_20260419.csv`

## 8. Quick ranking view
| Metric | v17b latest | v17c latest | v17d latest | v17e latest completed | v17f latest |
|---|---|---|---|---|---|
| Combined net PnL % | 1157.8342% | 1201.8242% | 1209.8867% | 1172.3875% | 1246.8526% |
| Combined PF | 1.892 | 1.786 | 1.767 | 1.862 | 1.826 |
| Combined MaxDD % | 43.5182% | 58.9204% | 48.2659% | 52.8541% | 44.8046% |
| Short net PnL % | 234.5346% | 237.5269% | 245.5894% | 208.0903% | 282.5554% |
| Short PF | 2.463 | 1.719 | 1.648 | 2.296 | 1.911 |
| Short MaxDD % | 20.3056% | 31.2090% | 23.4763% | 16.0121% | 21.8068% |
| Combined trades | 1078 | 1219 | 1254 | 1118 | 1229 |

## 9. Bottom-line read
| Question | Answer |
|---|---|
| Highest combined net PnL | v17f latest at 1246.8526%. |
| Best combined PF | v17b latest at 1.892, with v17e next at 1.862 and v17f next among the higher-throughput variants at 1.826. |
| Lowest combined max drawdown | v17b latest at 43.5182%; v17f is next at 44.8046%. |
| Highest short net PnL | v17f latest at 282.5554%. |
| Best short PF | v17b latest at 2.463; v17e is next at 2.296 and v17f is 1.911. |
| Lowest short max drawdown | v17e latest completed at 16.0121%, which is better than v17b 20.3056% and all other compared variants. |
| Highest total trades | v17d latest at 1254; v17f stays high at 1229 with materially better PF and drawdown. |
| Overall read | v17e is the selective quality-first short filter set. v17f is the strongest balance of throughput and quality among the expanded variants. v17b still remains the pure PF/DD benchmark overall. |
