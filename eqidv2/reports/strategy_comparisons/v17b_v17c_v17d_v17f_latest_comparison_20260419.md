# Latest v17b vs v17c vs v17d vs v17f Result Comparison

Comparison date: 2026-04-19

Note: the latest result CSV filenames still carry a `v16_5min` suffix, but the output folders and console logs confirm the actual strategy version.

## 1. Latest artifacts used
| Version | Run timestamp | Console log | Trades CSV | Daywise CSV | Notes |
|---|---|---|---|---|---|
| v17b latest | 2026-04-18 20:28:05 | C:\TradingData\eqidv2\outputs_v17b_5min\avwap_combined_runner_20260418_202805.txt | C:\TradingData\eqidv2\outputs_v17b_5min\avwap_longshort_trades_v16_5min_ALL_DAYS_20260418_202805.csv | C:\TradingData\eqidv2\outputs_v17b_5min\avwap_daywise_breakdown_v16_5min_ALL_DAYS_20260418_202805.csv | Latest v17b run. |
| v17c latest | 2026-04-18 21:02:36 | C:\TradingData\eqidv2\outputs_v17c_5min\avwap_combined_runner_20260418_210236.txt | C:\TradingData\eqidv2\outputs_v17c_5min\avwap_longshort_trades_v16_5min_ALL_DAYS_20260418_210236.csv | C:\TradingData\eqidv2\outputs_v17c_5min\avwap_daywise_breakdown_v16_5min_ALL_DAYS_20260418_210236.csv | Latest v17c run. |
| v17d latest | 2026-04-18 22:54:30 | C:\TradingData\eqidv2\outputs_v17d_5min\avwap_combined_runner_20260418_225430.txt | C:\TradingData\eqidv2\outputs_v17d_5min\avwap_longshort_trades_v16_5min_ALL_DAYS_20260418_225430.csv | C:\TradingData\eqidv2\outputs_v17d_5min\avwap_daywise_breakdown_v16_5min_ALL_DAYS_20260418_225430.csv | Latest v17d run; aggressive BOTH RS 0.50% research variant. |
| v17f latest | 2026-04-19 12:52:08 | C:\TradingData\eqidv2\outputs_v17f_5min\avwap_combined_runner_20260419_125208.txt | C:\TradingData\eqidv2\outputs_v17f_5min\avwap_longshort_trades_v16_5min_ALL_DAYS_20260419_125208.csv | C:\TradingData\eqidv2\outputs_v17f_5min\avwap_daywise_breakdown_v16_5min_ALL_DAYS_20260419_125208.csv | New v17f run built from v17d expansion plus targeted short cleanup. |

## 2. What v17f changes
| Area | v17d latest research base | v17f latest |
|---|---|---|
| BOTH-mode short RS threshold | `-0.50%` | unchanged |
| Short windows / cutoff | `09:15-12:00, 12:00-14:00`, cutoff `14:00` | unchanged |
| ADX chop scope | SHORT_ONLY only | unchanged |
| Extra short cleanup | none | drop SHORT_ONLY `rs > -0.25%` |
| Extra short cleanup | none | drop SHORT_ONLY `atr_pct >= 0.70%` |
| Extra short cleanup | none | drop BOTH `12:15-12:45` pocket |

## 3. Filter flow and runtime
| Metric | v17b latest | v17c latest | v17d latest | v17f latest |
|---|---|---|---|---|
| V16_FILTER | [V16_FILTER] SHORT: 448->371 (-77 RSI 30-40 dead zone, -0 OR gate) \| LONG: 2202->990 (-178 QS 7.6-7.9dead+QS>12, -580 dist>3.0ATR, -327 dist 1.0-1.5ATR dead, -127 vol>5x exhaust, -0 OR gate) | [V16_FILTER] SHORT: 448->371 (-77 RSI 30-40 dead zone, -0 OR gate) \| LONG: 2202->990 (-178 QS 7.6-7.9dead+QS>12, -580 dist>3.0ATR, -327 dist 1.0-1.5ATR dead, -127 vol>5x exhaust, -0 OR gate) | [V16_FILTER] SHORT: 709->556 (-153 RSI 30-40 dead zone, -0 OR gate) \| LONG: 2202->990 (-178 QS 7.6-7.9dead+QS>12, -580 dist>3.0ATR, -327 dist 1.0-1.5ATR dead, -127 vol>5x exhaust, -0 OR gate) | [V16_FILTER] SHORT: 709->556 (-153 RSI 30-40 dead zone, -0 OR gate) \| LONG: 2202->990 (-178 QS 7.6-7.9dead+QS>12, -580 dist>3.0ATR, -327 dist 1.0-1.5ATR dead, -127 vol>5x exhaust, -0 OR gate) |
| V17B_FILTER | [V17B_FILTER] SHORT: 371->164 (-49 SO-RSI[21,28) -37 SO-ADX>=44 -26 pullback -43 open<09:45 -45 AVWAP[0.5,1.0) -7 BOTH-ADX[25,30)) \| LONG: 990->914 (-19 late-A_MOD_BR bars>=13 vol>=3.7x -57 RSI[60,65)) |  |  |  |
| V17C_FILTER |  | [V17C_FILTER] SHORT: 371->259 (-49 SHORT_ONLY RSI [21.0,28.0), -37 SHORT_ONLY ADX>=44.0, -26 pullback setup (+0 strong-trend pullback exceptions)) \| LONG: 990->960 (-30 late A_MOD_BREAK_C1_HIGH bars>=12, vol>=3.5x) |  |  |
| V17D_FILTER |  |  | [V17D_FILTER] SHORT: 556->294 (-48 pullback \| -64 rs_nan \| -18 rs<=-2.00% \| -10 adx[20.0,25.0) \| -40 adx>=50.0 \| -50 BOTH avwap[0.50,1.00) \| -27 BOTH time pockets \| -5 late gate after 13:30) \| LONG: 990->960 (-30 late A_MOD_BREAK_C1_HIGH bars>=12, vol>=3.5x) | [V17D_FILTER] SHORT: 556->294 (-48 pullback \| -64 rs_nan \| -18 rs<=-2.00% \| -10 adx[20.0,25.0) \| -40 adx>=50.0 \| -50 BOTH avwap[0.50,1.00) \| -27 BOTH time pockets \| -5 late gate after 13:30) \| LONG: 990->960 (-30 late A_MOD_BREAK_C1_HIGH bars>=12, vol>=3.5x) |
| V17F_FILTER |  |  |  | [V17F_FILTER] SHORT extra: 294->269 (-4 SHORT_ONLY rs>-0.25% \| -15 SHORT_ONLY atr_pct>=0.70% \| -6 BOTH 12:15-12:45) |
| Total runtime | 1649.5s | 1434.4s | 1083.3s | 1033.3s |

## 4. Combined headline metrics
| Metric | v17b latest | v17c latest | v17d latest | v17f latest |
|---|---|---|---|---|
| Total trades | 1078 | 1219 | 1254 | 1229 |
| Unique trade days | 183 | 192 | 192 | 191 |
| TARGET hits | 763  \| hit-rate  = 70.78% | 846  \| hit-rate  = 69.40% | 864  \| hit-rate  = 68.90% | 854  \| hit-rate  = 69.49% |
| SL hits | 264  \| sl-rate   = 24.49% | 311  \| sl-rate   = 25.51% | 320  \| sl-rate   = 25.52% | 306  \| sl-rate   = 24.90% |
| EOD exits | 51  \| eod-rate  = 4.73% | 62  \| eod-rate  = 5.09% | 70  \| eod-rate  = 5.58% | 69  \| eod-rate  = 5.61% |
| Avg PnL % (net, per trade) | 1.0741% | 0.9859% | 0.9648% | 1.0145% |
| Sum PnL % (net, all trades) | 1157.8342% | 1201.8242% | 1209.8867% | 1246.8526% |
| Profit factor | 1.892 | 1.786 | 1.767 | 1.826 |
| Max drawdown (cumul PnL %) | 43.5182% | 58.9204% | 48.2659% | 44.8046% |
| Sharpe ratio (annualized) | 4.975 | 4.511 | 4.416 | 4.679 |
| Sortino ratio (annualized) | 15.219 | 14.734 | 13.800 | 14.268 |
| Calmar ratio | 26.606 | 20.397 | 25.067 | 27.829 |

## 5. Short-side metrics
| Metric | v17b latest | v17c latest | v17d latest | v17f latest |
|---|---|---|---|---|
| Total trades | 164 | 259 | 294 | 269 |
| Unique trade days | 77 | 93 | 93 | 89 |
| TARGET hits | 122  \| hit-rate  = 74.39% | 173  \| hit-rate  = 66.80% | 191  \| hit-rate  = 64.97% | 181  \| hit-rate  = 67.29% |
| SL hits | 33  \| sl-rate   = 20.12% | 68  \| sl-rate   = 26.25% | 77  \| sl-rate   = 26.19% | 63  \| sl-rate   = 23.42% |
| EOD exits | 9  \| eod-rate  = 5.49% | 18  \| eod-rate  = 6.95% | 26  \| eod-rate  = 8.84% | 25  \| eod-rate  = 9.29% |
| Avg PnL % (net, per trade) | 1.4301% | 0.9171% | 0.8353% | 1.0504% |
| Sum PnL % (net, all trades) | 234.5346% | 237.5269% | 245.5894% | 282.5554% |
| Profit factor | 2.463 | 1.719 | 1.648 | 1.911 |
| Max drawdown (cumul PnL %) | 20.3056% | 31.2090% | 23.4763% | 21.8068% |
| Sharpe ratio (annualized) | 7.093 | 4.179 | 3.815 | 4.966 |
| Sortino ratio (annualized) | 17.599 | 14.726 | 11.069 | 12.900 |
| Calmar ratio | 11.550 | 7.611 | 10.461 | 12.957 |

## 6. Long-side metrics
| Metric | v17b latest | v17c latest | v17d latest | v17f latest |
|---|---|---|---|---|
| Total trades | 914 | 960 | 960 | 960 |
| Unique trade days | 132 | 134 | 134 | 134 |
| TARGET hits | 641  \| hit-rate  = 70.13% | 673  \| hit-rate  = 70.10% | 673  \| hit-rate  = 70.10% | 673  \| hit-rate  = 70.10% |
| SL hits | 231  \| sl-rate   = 25.27% | 243  \| sl-rate   = 25.31% | 243  \| sl-rate   = 25.31% | 243  \| sl-rate   = 25.31% |
| EOD exits | 42  \| eod-rate  = 4.60% | 44  \| eod-rate  = 4.58% | 44  \| eod-rate  = 4.58% | 44  \| eod-rate  = 4.58% |
| Avg PnL % (net, per trade) | 1.0102% | 1.0045% | 1.0045% | 1.0045% |
| Sum PnL % (net, all trades) | 923.2996% | 964.2972% | 964.2972% | 964.2972% |
| Profit factor | 1.812 | 1.804 | 1.804 | 1.804 |
| Max drawdown (cumul PnL %) | 44.1979% | 48.8968% | 48.8968% | 48.8968% |
| Sharpe ratio (annualized) | 4.630 | 4.599 | 4.599 | 4.599 |
| Sortino ratio (annualized) | 14.630 | 14.735 | 14.735 | 14.735 |
| Calmar ratio | 20.890 | 19.721 | 19.721 | 19.721 |

## 7. V17F deltas vs latest baselines (combined)
| Metric | Total trades | Unique trade days | Avg PnL % (net, per trade) | Sum PnL % (net, all trades) | Profit factor | Max drawdown (cumul PnL %) | Sharpe ratio (annualized) | Sortino ratio (annualized) | Calmar ratio |
|---|---|---|---|---|---|---|---|---|---|
| v17f - v17b latest | +151 | +8 | -0.0596 | +89.0184 | -0.066 | +1.2864 | -0.296 | -0.951 | +1.223 |
| v17f - v17c latest | +10 | -1 | +0.0286 | +45.0284 | +0.040 | -14.1158 | +0.168 | -0.466 | +7.432 |
| v17f - v17d latest | -25 | -1 | +0.0497 | +36.9659 | +0.059 | -3.4613 | +0.263 | +0.468 | +2.762 |

## 8. V17F deltas vs latest baselines (short)
| Metric | Total trades | Unique trade days | Avg PnL % (net, per trade) | Sum PnL % (net, all trades) | Profit factor | Max drawdown (cumul PnL %) | Sharpe ratio (annualized) | Sortino ratio (annualized) | Calmar ratio |
|---|---|---|---|---|---|---|---|---|---|
| v17f - v17b latest | +105 | +12 | -0.3797 | +48.0208 | -0.552 | +1.5012 | -2.127 | -4.699 | +1.407 |
| v17f - v17c latest | +10 | -4 | +0.1333 | +45.0285 | +0.192 | -9.4022 | +0.787 | -1.826 | +5.346 |
| v17f - v17d latest | -25 | -4 | +0.2151 | +36.9660 | +0.263 | -1.6695 | +1.151 | +1.831 | +2.496 |

## 9. Setup contribution by side
| Side | Setup | v17b latest trades | v17b latest pnl | v17c latest trades | v17c latest pnl | v17d latest trades | v17d latest pnl | v17f latest trades | v17f latest pnl |
|---|---|---|---|---|---|---|---|---|---|
| SHORT | A_MOD_BREAK_C1_LOW | 162 | 228.1346% | 257 | 231.1269% | 292 | 239.1894% | 269 | 282.5554% |
| SHORT | B_HUGE_RED_FAILED_BOUNCE | 2 | 6.4000% | 2 | 6.4000% | 2 | 6.4000% | 0 | 0.0000% |
| LONG | A_MOD_BREAK_C1_HIGH | 639 | 596.2928% | 676 | 653.8584% | 676 | 653.8584% | 676 | 653.8584% |
| LONG | A_MOD_CLOSE_CONTINUATION_BREAK | 117 | 102.9995% | 123 | 90.6040% | 123 | 90.6040% | 123 | 90.6040% |
| LONG | B_HUGE_C1_CLOSE_RECLAIM_BREAK | 158 | 224.0073% | 161 | 219.8349% | 161 | 219.8349% | 161 | 219.8349% |

## 10. Monthly combined summary
| Month | v17b latest trades | v17b latest pnl | v17c latest trades | v17c latest pnl | v17d latest trades | v17d latest pnl | v17f latest trades | v17f latest pnl |
|---|---|---|---|---|---|---|---|---|
| 2025-06 | 117 | 72.1087% | 129 | 102.4431% | 130 | 102.6252% | 129 | 107.3263% |
| 2025-07 | 69 | 82.7050% | 77 | 80.7176% | 86 | 91.2492% | 85 | 94.3994% |
| 2025-08 | 81 | 143.8300% | 104 | 167.9318% | 103 | 135.2928% | 100 | 141.4951% |
| 2025-09 | 153 | 136.8506% | 162 | 141.9472% | 158 | 150.9791% | 158 | 150.9791% |
| 2025-10 | 136 | 104.8603% | 140 | 101.8625% | 138 | 103.3636% | 137 | 108.0648% |
| 2025-11 | 75 | 69.0266% | 84 | 66.2266% | 91 | 59.9736% | 90 | 64.6747% |
| 2025-12 | 80 | 97.8092% | 83 | 99.5081% | 92 | 82.0795% | 91 | 86.7807% |
| 2026-01 | 76 | 122.3804% | 98 | 90.0748% | 97 | 114.7542% | 91 | 103.4553% |
| 2026-02 | 107 | 164.7465% | 126 | 175.1315% | 139 | 152.1539% | 135 | 155.1562% |
| 2026-03 | 137 | 141.0098% | 161 | 167.3774% | 166 | 199.3800% | 160 | 211.7845% |
| 2026-04 | 47 | 22.5071% | 55 | 8.6038% | 54 | 18.0354% | 53 | 22.7365% |

## 11. Last 10 trading days
| Date | v17b latest trades | v17b latest pnl | v17c latest trades | v17c latest pnl | v17d latest trades | v17d latest pnl | v17f latest trades | v17f latest pnl |
|---|---|---|---|---|---|---|---|---|
| 2026-03-27 | 6 | 11.2989% | 7 | 14.4989% | 6 | 17.1904% | 6 | 17.1904% |
| 2026-03-30 | 3 | 1.6989% | 4 | 4.8989% | 3 | 9.6000% | 3 | 9.6000% |
| 2026-04-01 | 18 | -0.5777% | 18 | -0.5777% | 18 | -0.5777% | 18 | -0.5777% |
| 2026-04-02 | 1 | -4.7011% | 3 | -6.2022% | 3 | -9.3729% | 2 | -4.6718% |
| 2026-04-06 | 2 | -1.5011% | 4 | -10.9034% | 3 | -6.2022% | 3 | -6.2022% |
| 2026-04-07 | 1 | -4.7011% | 1 | -4.7011% | 1 | -4.7011% | 1 | -4.7011% |
| 2026-04-08 | 1 | -4.6989% | 2 | -9.3977% | 2 | -9.3977% | 2 | -9.3977% |
| 2026-04-09 | 0 | 0.0000% | 2 | -1.5011% | 2 | 6.4000% | 2 | 6.4000% |
| 2026-04-10 | 7 | 12.5698% | 7 | 12.5698% | 7 | 12.5698% | 7 | 12.5698% |
| 2026-04-17 | 17 | 26.1173% | 18 | 29.3173% | 18 | 29.3173% | 18 | 29.3173% |

## 12. Pairwise trade overlap
| Pair | Shared trades | Only first version | Only second version |
|---|---|---|---|
| v17b latest vs v17c latest | 1067 | 11 | 152 |
| v17c latest vs v17d latest | 1119 | 100 | 135 |
| v17d latest vs v17f latest | 1229 | 25 | 0 |
| v17b latest vs v17f latest | 1020 | 58 | 209 |

## 13. Bottom-line read
| Question | Answer |
|---|---|
| Highest combined net PnL | v17f latest at 1246.8526%. |
| Best combined PF | v17b latest at 1.892; v17f is next-best here at 1.826 among the expanded-throughput variants. |
| Lowest combined max drawdown | v17b latest at 43.5182%; v17f is 44.8046%, better than v17c and v17d latest. |
| Highest short net PnL | v17f latest at 282.5554%. |
| Best short PF | v17b latest at 2.463; v17f improves to 1.911 versus v17c 1.719 and v17d latest 1.648. |
| Lowest short max drawdown | v17b latest at 20.3056%; v17f is 21.8068%, better than v17c and v17d latest. |
| Highest total trades | v17d latest at 1254; v17f keeps a high 1229 while materially improving PF and drawdown. |
| Overall read | v17f is the strongest verified balance of throughput and quality among v17c/v17d/v17f. It does not beat v17b on pure PF/DD, but it decisively beats v17c and latest v17d on combined PnL, PF, and drawdown at a much higher trade count than v17b. |
