# Latest v17b vs v17c vs v17d Result Comparison

Comparison date: 2026-04-19

Note: the latest result CSV filenames still carry a `v16_5min` suffix, but the output folders and console logs confirm the actual strategy version.

## 1. Latest artifacts used
| Version | Run timestamp | Console log | Trades CSV | Daywise CSV | Notes |
|---|---|---|---|---|---|
| v17b latest | 2026-04-18 20:28:05 | C:\TradingData\eqidv2\outputs_v17b_5min\avwap_combined_runner_20260418_202805.txt | C:\TradingData\eqidv2\outputs_v17b_5min\avwap_longshort_trades_v16_5min_ALL_DAYS_20260418_202805.csv | C:\TradingData\eqidv2\outputs_v17b_5min\avwap_daywise_breakdown_v16_5min_ALL_DAYS_20260418_202805.csv | Latest v17b run. |
| v17c latest | 2026-04-18 21:02:36 | C:\TradingData\eqidv2\outputs_v17c_5min\avwap_combined_runner_20260418_210236.txt | C:\TradingData\eqidv2\outputs_v17c_5min\avwap_longshort_trades_v16_5min_ALL_DAYS_20260418_210236.csv | C:\TradingData\eqidv2\outputs_v17c_5min\avwap_daywise_breakdown_v16_5min_ALL_DAYS_20260418_210236.csv | Latest v17c run. |
| v17d latest | 2026-04-18 22:54:30 | C:\TradingData\eqidv2\outputs_v17d_5min\avwap_combined_runner_20260418_225430.txt | C:\TradingData\eqidv2\outputs_v17d_5min\avwap_longshort_trades_v16_5min_ALL_DAYS_20260418_225430.csv | C:\TradingData\eqidv2\outputs_v17d_5min\avwap_daywise_breakdown_v16_5min_ALL_DAYS_20260418_225430.csv | Actual latest v17d output; this run used aggressive BOTH-mode short RS threshold 0.50% via env override. |

## 2. Run signatures
| Version | Signature |
|---|---|
| v17b latest | AVWAP v16_5min COMBINED runner - Anti-exhaustion filters + V11 SHORT + V9 LONG \| [INFO] Wave2 SHORT filters: entry_cutoff=<13:30 \| min_OR_width=1.00% \| signal_avwap_dist_atr_max=2.10 \| BOTH_mode_RS<=-0.75% \| [INFO] SHORT windows: use_time_windows=True \| 09:15-12:00, 12:00-13:30 |
| v17c latest | AVWAP v16_5min COMBINED runner - Anti-exhaustion filters + V11 SHORT + V9 LONG \| [INFO] Wave2 SHORT filters: entry_cutoff=<13:30 \| min_OR_width=1.00% \| signal_avwap_dist_atr_max=2.10 \| BOTH_mode_RS<=-0.75% \| [INFO] SHORT windows: use_time_windows=True \| 09:15-12:00, 12:00-13:30 |
| v17d latest | AVWAP v16_5min COMBINED runner - Anti-exhaustion filters + V11 SHORT + V9 LONG \| [INFO] Wave2 SHORT filters: entry_cutoff=<14:00 \| min_OR_width=1.00% \| signal_avwap_dist_atr_max=2.10 \| BOTH_mode_RS<=-0.50% \| [INFO] SHORT windows: use_time_windows=True \| 09:15-12:00, 12:00-14:00 |

## 3. Filter flow and runtime
| Metric | v17b latest | v17c latest | v17d latest |
|---|---|---|---|
| V16_FILTER | [V16_FILTER] SHORT: 448->371 (-77 RSI 30-40 dead zone, -0 OR gate) \| LONG: 2202->990 (-178 QS 7.6-7.9dead+QS>12, -580 dist>3.0ATR, -327 dist 1.0-1.5ATR dead, -127 vol>5x exhaust, -0 OR gate) | [V16_FILTER] SHORT: 448->371 (-77 RSI 30-40 dead zone, -0 OR gate) \| LONG: 2202->990 (-178 QS 7.6-7.9dead+QS>12, -580 dist>3.0ATR, -327 dist 1.0-1.5ATR dead, -127 vol>5x exhaust, -0 OR gate) | [V16_FILTER] SHORT: 709->556 (-153 RSI 30-40 dead zone, -0 OR gate) \| LONG: 2202->990 (-178 QS 7.6-7.9dead+QS>12, -580 dist>3.0ATR, -327 dist 1.0-1.5ATR dead, -127 vol>5x exhaust, -0 OR gate) |
| V17B_FILTER | [V17B_FILTER] SHORT: 371->164 (-49 SO-RSI[21,28) -37 SO-ADX>=44 -26 pullback -43 open<09:45 -45 AVWAP[0.5,1.0) -7 BOTH-ADX[25,30)) \| LONG: 990->914 (-19 late-A_MOD_BR bars>=13 vol>=3.7x -57 RSI[60,65)) |  |  |
| V17C_FILTER |  | [V17C_FILTER] SHORT: 371->259 (-49 SHORT_ONLY RSI [21.0,28.0), -37 SHORT_ONLY ADX>=44.0, -26 pullback setup (+0 strong-trend pullback exceptions)) \| LONG: 990->960 (-30 late A_MOD_BREAK_C1_HIGH bars>=12, vol>=3.5x) |  |
| V17D_FILTER |  |  | [V17D_FILTER] SHORT: 556->294 (-48 pullback \| -64 rs_nan \| -18 rs<=-2.00% \| -10 adx[20.0,25.0) \| -40 adx>=50.0 \| -50 BOTH avwap[0.50,1.00) \| -27 BOTH time pockets \| -5 late gate after 13:30) \| LONG: 990->960 (-30 late A_MOD_BREAK_C1_HIGH bars>=12, vol>=3.5x) |
| Phase 1 runtime | 1603.9s | 1407.9s | 1050.7s |
| Phase 2 runtime | 37.8s | 31.1s | 31.1s |
| Total runtime | 1649.5s | 1434.4s | 1083.3s |
| Day-side mix | [INFO] Day-side mix: both=26 \| short_only=51 \| long_only=106 \| total_days=183 | [INFO] Day-side mix: both=35 \| short_only=58 \| long_only=99 \| total_days=192 | [INFO] Day-side mix: both=35 \| short_only=58 \| long_only=99 \| total_days=192 |

## 4. Combined headline metrics
| Metric | v17b latest | v17c latest | v17d latest |
|---|---|---|---|
| Total trades | 1078 | 1219 | 1254 |
| Unique trade days | 183 | 192 | 192 |
| TARGET hits | 763  \| hit-rate  = 70.78% | 846  \| hit-rate  = 69.40% | 864  \| hit-rate  = 68.90% |
| Hit rate | 70.78% | 69.40% | 68.90% |
| SL rate | 24.49% | 25.51% | 25.52% |
| EOD rate | 4.73% | 5.09% | 5.58% |
| SL hits | 264  \| sl-rate   = 24.49% | 311  \| sl-rate   = 25.51% | 320  \| sl-rate   = 25.52% |
| EOD exits | 51  \| eod-rate  = 4.73% | 62  \| eod-rate  = 5.09% | 70  \| eod-rate  = 5.58% |
| Avg PnL % (net, per trade) | 1.0741% | 0.9859% | 0.9648% |
| Sum PnL % (net, all trades) | 1157.8342% | 1201.8242% | 1209.8867% |
| Profit factor | 1.892 | 1.786 | 1.767 |
| Max drawdown (cumul PnL %) | 43.5182% | 58.9204% | 48.2659% |
| Sharpe ratio (annualized) | 4.975 | 4.511 | 4.416 |
| Sortino ratio (annualized) | 15.219 | 14.734 | 13.800 |
| Calmar ratio | 26.606 | 20.397 | 25.067 |
| Notional P&L | Rs.231,566.84 | Rs.240,364.84 | Rs.241,977.33 |

## 5. Combined deltas
| Metric | v17c-v17b | v17d-v17c | v17d-v17b |
|---|---|---|---|
| Total trades | +141.0000 | +35.0000 | +176.0000 |
| Unique trade days | +9.0000 | +0.0000 | +9.0000 |
| Avg PnL % (net, per trade) | -0.0882 | -0.0211 | -0.1093 |
| Sum PnL % (net, all trades) | +43.9900 | +8.0625 | +52.0525 |
| Profit factor | -0.1060 | -0.0190 | -0.1250 |
| Max drawdown (cumul PnL %) | +15.4022 | -10.6545 | +4.7477 |
| Sharpe ratio (annualized) | -0.4640 | -0.0950 | -0.5590 |
| Sortino ratio (annualized) | -0.4850 | -0.9340 | -1.4190 |
| Calmar ratio | -6.2090 | +4.6700 | -1.5390 |
| Hit rate | -1.38 pts | -0.50 pts | -1.88 pts |
| SL rate | +1.02 pts | +0.01 pts | +1.03 pts |
| EOD rate | +0.36 pts | +0.49 pts | +0.85 pts |

## 6. Short-side metrics
| Metric | v17b latest | v17c latest | v17d latest |
|---|---|---|---|
| Total trades | 164 | 259 | 294 |
| Unique trade days | 77 | 93 | 93 |
| TARGET hits | 122  \| hit-rate  = 74.39% | 173  \| hit-rate  = 66.80% | 191  \| hit-rate  = 64.97% |
| Hit rate | 74.39% | 66.80% | 64.97% |
| SL rate | 20.12% | 26.25% | 26.19% |
| EOD rate | 5.49% | 6.95% | 8.84% |
| SL hits | 33  \| sl-rate   = 20.12% | 68  \| sl-rate   = 26.25% | 77  \| sl-rate   = 26.19% |
| EOD exits | 9  \| eod-rate  = 5.49% | 18  \| eod-rate  = 6.95% | 26  \| eod-rate  = 8.84% |
| Avg PnL % (net, per trade) | 1.4301% | 0.9171% | 0.8353% |
| Sum PnL % (net, all trades) | 234.5346% | 237.5269% | 245.5894% |
| Profit factor | 2.463 | 1.719 | 1.648 |
| Max drawdown (cumul PnL %) | 20.3056% | 31.2090% | 23.4763% |
| Sharpe ratio (annualized) | 7.093 | 4.179 | 3.815 |
| Sortino ratio (annualized) | 17.599 | 14.726 | 11.069 |
| Calmar ratio | 11.550 | 7.611 | 10.461 |
| Notional P&L | Rs.46,906.92 | Rs.47,505.39 | Rs.49,117.89 |

## 7. Long-side metrics
| Metric | v17b latest | v17c latest | v17d latest |
|---|---|---|---|
| Total trades | 914 | 960 | 960 |
| Unique trade days | 132 | 134 | 134 |
| TARGET hits | 641  \| hit-rate  = 70.13% | 673  \| hit-rate  = 70.10% | 673  \| hit-rate  = 70.10% |
| Hit rate | 70.13% | 70.10% | 70.10% |
| SL rate | 25.27% | 25.31% | 25.31% |
| EOD rate | 4.60% | 4.58% | 4.58% |
| SL hits | 231  \| sl-rate   = 25.27% | 243  \| sl-rate   = 25.31% | 243  \| sl-rate   = 25.31% |
| EOD exits | 42  \| eod-rate  = 4.60% | 44  \| eod-rate  = 4.58% | 44  \| eod-rate  = 4.58% |
| Avg PnL % (net, per trade) | 1.0102% | 1.0045% | 1.0045% |
| Sum PnL % (net, all trades) | 923.2996% | 964.2972% | 964.2972% |
| Profit factor | 1.812 | 1.804 | 1.804 |
| Max drawdown (cumul PnL %) | 44.1979% | 48.8968% | 48.8968% |
| Sharpe ratio (annualized) | 4.630 | 4.599 | 4.599 |
| Sortino ratio (annualized) | 14.630 | 14.735 | 14.735 |
| Calmar ratio | 20.890 | 19.721 | 19.721 |
| Notional P&L | Rs.184,659.92 | Rs.192,859.45 | Rs.192,859.45 |

## 8. Setup contribution by side
| Side | Setup | v17b latest trades | v17b latest pnl | v17c latest trades | v17c latest pnl | v17d latest trades | v17d latest pnl |
|---|---|---|---|---|---|---|---|
| SHORT | A_MOD_BREAK_C1_LOW | 162 | 228.1346% | 257 | 231.1269% | 292 | 239.1894% |
| SHORT | B_HUGE_RED_FAILED_BOUNCE | 2 | 6.4000% | 2 | 6.4000% | 2 | 6.4000% |
| LONG | A_MOD_BREAK_C1_HIGH | 639 | 596.2928% | 676 | 653.8584% | 676 | 653.8584% |
| LONG | A_MOD_CLOSE_CONTINUATION_BREAK | 117 | 102.9995% | 123 | 90.6040% | 123 | 90.6040% |
| LONG | B_HUGE_C1_CLOSE_RECLAIM_BREAK | 158 | 224.0073% | 161 | 219.8349% | 161 | 219.8349% |

## 9. Monthly combined summary
| Month | v17b latest trades | v17b latest pnl | v17c latest trades | v17c latest pnl | v17d latest trades | v17d latest pnl |
|---|---|---|---|---|---|---|
| 2025-06 | 117 | 72.1087% | 129 | 102.4431% | 130 | 102.6252% |
| 2025-07 | 69 | 82.7050% | 77 | 80.7176% | 86 | 91.2492% |
| 2025-08 | 81 | 143.8300% | 104 | 167.9318% | 103 | 135.2928% |
| 2025-09 | 153 | 136.8506% | 162 | 141.9472% | 158 | 150.9791% |
| 2025-10 | 136 | 104.8603% | 140 | 101.8625% | 138 | 103.3636% |
| 2025-11 | 75 | 69.0266% | 84 | 66.2266% | 91 | 59.9736% |
| 2025-12 | 80 | 97.8092% | 83 | 99.5081% | 92 | 82.0795% |
| 2026-01 | 76 | 122.3804% | 98 | 90.0748% | 97 | 114.7542% |
| 2026-02 | 107 | 164.7465% | 126 | 175.1315% | 139 | 152.1539% |
| 2026-03 | 137 | 141.0098% | 161 | 167.3774% | 166 | 199.3800% |
| 2026-04 | 47 | 22.5071% | 55 | 8.6038% | 54 | 18.0354% |

## 10. Last 10 trading days
| Date | v17b latest trades | v17b latest pnl | v17c latest trades | v17c latest pnl | v17d latest trades | v17d latest pnl |
|---|---|---|---|---|---|---|
| 2026-03-27 | 6 | 11.2989% | 7 | 14.4989% | 6 | 17.1904% |
| 2026-03-30 | 3 | 1.6989% | 4 | 4.8989% | 3 | 9.6000% |
| 2026-04-01 | 18 | -0.5777% | 18 | -0.5777% | 18 | -0.5777% |
| 2026-04-02 | 1 | -4.7011% | 3 | -6.2022% | 3 | -9.3729% |
| 2026-04-06 | 2 | -1.5011% | 4 | -10.9034% | 3 | -6.2022% |
| 2026-04-07 | 1 | -4.7011% | 1 | -4.7011% | 1 | -4.7011% |
| 2026-04-08 | 1 | -4.6989% | 2 | -9.3977% | 2 | -9.3977% |
| 2026-04-09 | 0 | 0.0000% | 2 | -1.5011% | 2 | 6.4000% |
| 2026-04-10 | 7 | 12.5698% | 7 | 12.5698% | 7 | 12.5698% |
| 2026-04-17 | 17 | 26.1173% | 18 | 29.3173% | 18 | 29.3173% |

## 11. Pairwise trade overlap
| Pair | Shared trades | Only v17b | Only v17c | Shared changed pnl | Shared changed outcome | Only v17d |
|---|---|---|---|---|---|---|
| v17b vs v17c | 1067 | 11.0 | 152.0 | 0 | 0 | nan |
| v17c vs v17d | 1119 | nan | 100.0 | 0 | 0 | 135.0 |
| v17b vs v17d | 1030 | 48.0 | nan | 0 | 0 | 224.0 |

## 12. Quick ranking view
| Metric | v17b latest | v17c latest | v17d latest |
|---|---|---|---|
| Combined net PnL % | 1157.8342 | 1201.8242 | 1209.8867 |
| Combined PF | 1.892 | 1.786 | 1.767 |
| Combined MaxDD % | 43.5182 | 58.9204 | 48.2659 |
| Short net PnL % | 234.5346 | 237.5269 | 245.5894 |
| Short PF | 2.463 | 1.719 | 1.648 |
| Short MaxDD % | 20.3056 | 31.2090 | 23.4763 |
| Combined trades | 1078 | 1219 | 1254 |

## 13. Bottom-line read

| Question | Answer |
|---|---|
| Highest combined net PnL | `v17d latest` at `1209.8867%`, but this latest v17d result is the aggressive RS override variant rather than the stricter default profile later selected for the file. |
| Best combined PF | `v17b latest` at `1.892`. |
| Lowest combined max drawdown | `v17b latest` at `43.5182%`. |
| Highest short net PnL | `v17d latest` at `245.5894%`. |
| Best short PF | `v17b latest` at `2.463`. |
| Lowest short max drawdown | `v17b latest` at `20.3056%`. |
| Highest total trades | `v17d latest` at `1254`. |
| Best quality/risk profile overall | `v17b` still leads on PF and drawdown quality. `v17c` is the first throughput expansion. Latest `v17d` adds more trades and slightly more total PnL again, but gives back PF and drawdown quality versus `v17b`. |