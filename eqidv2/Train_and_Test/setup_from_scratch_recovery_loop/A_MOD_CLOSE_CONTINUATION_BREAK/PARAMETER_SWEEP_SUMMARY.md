# A_MOD_CLOSE_CONTINUATION_BREAK (LONG) — PARAMETER_SWEEP_SUMMARY (recovery loop)

_Generated 2026-07-03. Research-only; NO live trades; NO final_setup_conf.py edits._

Band score = `reward(min(PF_fit,PF_val)) − 0.80·|PF_fit−PF_val|`, tent at 1.70. `keep` needs FIT & VAL PF >= 1.0 with >= 6 trades each.

## Redesigned setup versions (hypothesis packs)

| version | exit | FIT n/PF | VAL n/PF | score | decision |
|---|---|---|---|---|---|
| R8_ranked_top1 | SL1.0/T2.0 | 892/0.43 | 614/0.435 | 0.4267 | reject |
| R8_ranked_top1 | SL0.7/T1.5 | 895/0.368 | 614/0.355 | 0.3449 | reject |
| R24_notbear_fresh | SL1.0/T2.0 | 1371/0.267 | 1167/0.269 | 0.2649 | reject |
| R4_fresh_break | SL1.0/T2.0 | 1891/0.246 | 1441/0.244 | 0.2422 | reject |
| R8_ranked_top1 | SL0.5/T1.0 | 895/0.26 | 614/0.287 | 0.2388 | reject |
| R2b4_bulltrend_fresh | SL1.0/T2.0 | 1085/0.25 | 656/0.234 | 0.2202 | reject |
| R2_notbear | SL1.0/T2.0 | 1435/0.26 | 1173/0.236 | 0.2169 | reject |
| R1_uncollapsed_card | SL1.0/T2.0 | 1963/0.25 | 1488/0.228 | 0.2107 | reject |
| R5_pullback_then_break | SL1.0/T2.0 | 1809/0.243 | 1293/0.221 | 0.2037 | reject |
| R35_first_pullback | SL1.0/T2.0 | 1617/0.24 | 1145/0.215 | 0.1949 | reject |
| R3_first_break | SL1.0/T2.0 | 1786/0.265 | 1306/0.226 | 0.1943 | reject |
| R7_aligned_thrust | SL1.0/T2.0 | 1117/0.355 | 859/0.258 | 0.1805 | reject |
| R23_notbear_first | SL1.0/T2.0 | 1295/0.282 | 1049/0.22 | 0.1715 | reject |
| R2b_bulltrend | SL1.0/T2.0 | 1128/0.26 | 689/0.208 | 0.1661 | reject |
| R24_notbear_fresh | SL0.7/T1.5 | 2044/0.238 | 1676/0.196 | 0.1619 | reject |
| R2_notbear | SL0.7/T1.5 | 2111/0.239 | 1773/0.196 | 0.1612 | reject |
| R1_uncollapsed_card | SL0.7/T1.5 | 2980/0.244 | 2208/0.196 | 0.1574 | reject |
| R5_pullback_then_break | SL0.7/T1.5 | 2579/0.243 | 1865/0.195 | 0.1568 | reject |
| R6_morning | SL1.0/T2.0 | 1296/0.411 | 937/0.27 | 0.1563 | reject |
| R5_pullback_then_break | SL0.5/T1.0 | 3789/0.197 | 2682/0.172 | 0.1517 | reject |
| R35_first_pullback | SL0.5/T1.0 | 3191/0.192 | 2144/0.168 | 0.149 | reject |
| R35_first_pullback | SL0.7/T1.5 | 2274/0.242 | 1625/0.189 | 0.1459 | reject |
| R4_fresh_break | SL0.7/T1.5 | 2816/0.242 | 2065/0.187 | 0.1429 | reject |
| R6_morning | SL0.7/T1.5 | 1750/0.359 | 1307/0.238 | 0.1422 | reject |
| R7_aligned_thrust | SL0.7/T1.5 | 1461/0.303 | 1154/0.213 | 0.1418 | reject |
| R7_aligned_thrust | SL0.5/T1.0 | 1999/0.239 | 1440/0.185 | 0.1417 | reject |
| R3_first_break | SL0.7/T1.5 | 2704/0.244 | 1946/0.185 | 0.1378 | reject |
| R2b_bulltrend | SL0.7/T1.5 | 1591/0.223 | 961/0.175 | 0.137 | reject |
| R24_notbear_fresh | SL0.5/T1.0 | 3108/0.207 | 2402/0.168 | 0.1364 | reject |
| R2_notbear | SL0.5/T1.0 | 3380/0.2 | 2602/0.164 | 0.1353 | reject |
| R3_first_break | SL0.5/T1.0 | 3977/0.199 | 2842/0.163 | 0.1342 | reject |
| R26_notbear_morning | SL1.0/T2.0 | 839/0.431 | 727/0.266 | 0.1333 | reject |
| R4_fresh_break | SL0.5/T1.0 | 4255/0.201 | 3050/0.163 | 0.1326 | reject |
| R1_uncollapsed_card | SL0.5/T1.0 | 4638/0.202 | 3365/0.162 | 0.1311 | reject |
| R6_morning | SL0.5/T1.0 | 2442/0.28 | 1812/0.197 | 0.1308 | reject |
| R2b4_bulltrend_fresh | SL0.5/T1.0 | 2222/0.201 | 1225/0.161 | 0.1288 | reject |
| R23_notbear_first | SL0.5/T1.0 | 2906/0.199 | 2240/0.158 | 0.1257 | reject |
| R23_notbear_first | SL0.7/T1.5 | 1923/0.255 | 1555/0.182 | 0.1244 | reject |
| R26_notbear_morning | SL0.7/T1.5 | 1130/0.354 | 974/0.223 | 0.1185 | reject |
| R2b4_bulltrend_fresh | SL0.7/T1.5 | 1523/0.234 | 883/0.169 | 0.1172 | reject |
| R26_notbear_morning | SL0.5/T1.0 | 1648/0.275 | 1332/0.185 | 0.1137 | reject |
| R2b_bulltrend | SL0.5/T1.0 | 2379/0.199 | 1340/0.145 | 0.1021 | reject |

## Single-term sweeps: 0 keeps / 1020 tested

Top 20 by band score:

| term | FIT n/PF | VAL n/PF | score | decision |
|---|---|---|---|---|
| x_bar_idx>=58.0(q0.9)@SL1.0/T2.0 | 743/0.333 | 491/0.349 | 0.32 | reject |
| x_gap_pct>=1.588235(q0.9)@SL1.0/T2.0 | 524/0.339 | 352/0.309 | 0.2852 | reject |
| x_bar_idx>=58.0(q0.9)@SL0.7/T1.5 | 819/0.306 | 533/0.292 | 0.2798 | reject |
| x_bar_i<=7.0(q0.1)@SL1.0/T2.0 | 1237/0.277 | 351/0.278 | 0.2765 | reject |
| x_bar_i<=26.0(q0.5)@SL1.0/T2.0 | 1953/0.282 | 1108/0.275 | 0.2684 | reject |
| x_dayrange_atr>=11.362963(q0.7)@SL1.0/T2.0 | 1261/0.264 | 814/0.27 | 0.2593 | reject |
| x_dayrange_atr>=4.974133(q0.1)@SL1.0/T2.0 | 1858/0.256 | 1358/0.257 | 0.2548 | reject |
| body_pct<=0.4444(q0.1)@SL0.7/T1.5 | 1029/0.258 | 608/0.263 | 0.2533 | reject |
| x_bar_idx>=28.0(q0.3)@SL1.0/T2.0 | 1455/0.269 | 1003/0.26 | 0.2523 | reject |
| x_bar_i<=37.0(q0.7)@SL1.0/T2.0 | 1964/0.256 | 1362/0.262 | 0.2514 | reject |
| body_pct<=0.4444(q0.1)@SL1.0/T2.0 | 950/0.269 | 584/0.294 | 0.2491 | reject |
| lower_wick_pct>=0.108905(q0.7)@SL1.0/T2.0 | 1785/0.261 | 1293/0.276 | 0.249 | reject |
| wick_skew_pct<=-0.178605(q0.1)@SL1.0/T2.0 | 1045/0.271 | 693/0.299 | 0.2486 | reject |
| signal_range_pct>=1.074667(q0.9)@SL1.0/T2.0 | 990/0.25 | 802/0.253 | 0.2482 | reject |
| x_pdl_dist_atr>=22.026537(q0.9)@SL1.0/T2.0 | 655/0.28 | 402/0.261 | 0.2465 | reject |
| x_pdl_dist_atr>=14.523594(q0.7)@SL1.0/T2.0 | 1079/0.248 | 844/0.247 | 0.2454 | reject |
| quality_score>=159.4978(q0.9)@SL1.0/T2.0 | 1108/0.268 | 635/0.298 | 0.2446 | reject |
| x_bar_i<=16.0(q0.3)@SL1.0/T2.0 | 1849/0.284 | 772/0.262 | 0.2443 | reject |
| x_dayrange_atr>=15.680512(q0.9)@SL1.0/T2.0 | 822/0.289 | 469/0.264 | 0.2433 | reject |
| body_pct<=0.77(q0.5)@SL1.0/T2.0 | 1668/0.283 | 1204/0.261 | 0.243 | reject |

## Best value per knob (all knobs tested relaxed/medium/strict)

| knob | best value | FIT n/PF | VAL n/PF | score |
|---|---|---|---|---|
| x_bar_idx | x_bar_idx>=58.0(q0.9)@SL1.0/T2.0 | 743/0.333 | 491/0.349 | 0.32 |
| x_gap_pct | x_gap_pct>=1.588235(q0.9)@SL1.0/T2.0 | 524/0.339 | 352/0.309 | 0.2852 |
| x_bar_i | x_bar_i<=7.0(q0.1)@SL1.0/T2.0 | 1237/0.277 | 351/0.278 | 0.2765 |
| x_dayrange_atr | x_dayrange_atr>=11.362963(q0.7)@SL1.0/T2.0 | 1261/0.264 | 814/0.27 | 0.2593 |
| body_pct | body_pct<=0.4444(q0.1)@SL0.7/T1.5 | 1029/0.258 | 608/0.263 | 0.2533 |
| lower_wick_pct | lower_wick_pct>=0.108905(q0.7)@SL1.0/T2.0 | 1785/0.261 | 1293/0.276 | 0.249 |
| wick_skew_pct | wick_skew_pct<=-0.178605(q0.1)@SL1.0/T2.0 | 1045/0.271 | 693/0.299 | 0.2486 |
| signal_range_pct | signal_range_pct>=1.074667(q0.9)@SL1.0/T2.0 | 990/0.25 | 802/0.253 | 0.2482 |
| x_pdl_dist_atr | x_pdl_dist_atr>=22.026537(q0.9)@SL1.0/T2.0 | 655/0.28 | 402/0.261 | 0.2465 |
| quality_score | quality_score>=159.4978(q0.9)@SL1.0/T2.0 | 1108/0.268 | 635/0.298 | 0.2446 |
| x_fresh_break | x_fresh_break>=0.5@SL1.0/T2.0 | 1891/0.246 | 1441/0.244 | 0.2422 |
| x_bb_width_pct | x_bb_width_pct<=0.044555(q0.9)@SL1.0/T2.0 | 1651/0.262 | 1261/0.25 | 0.2404 |
| x_stoch_k | x_stoch_k>=95.683636(q0.5)@SL1.0/T2.0 | 1817/0.25 | 1309/0.245 | 0.2404 |
| x_willr | x_willr>=-4.316364(q0.5)@SL1.0/T2.0 | 1817/0.25 | 1309/0.245 | 0.2404 |
| rs_pct | rs_pct>=5.03868(q0.9)@SL1.0/T2.0 | 1040/0.254 | 643/0.272 | 0.2401 |
| vwap_dist_atr | vwap_dist_atr>=1.86566(q0.3)@SL1.0/T2.0 | 1878/0.248 | 1390/0.259 | 0.2399 |
| x_svwap_dist_atr | x_svwap_dist_atr>=1.865662(q0.3)@SL1.0/T2.0 | 1878/0.248 | 1390/0.259 | 0.2399 |
| x_orh_dist_atr | x_orh_dist_atr>=3.808399(q0.7)@SL1.0/T2.0 | 1424/0.24 | 1137/0.241 | 0.2386 |
| x_ema20_slope3_atr | x_ema20_slope3_atr>=0.285788(q0.3)@SL1.0/T2.0 | 1989/0.239 | 1492/0.24 | 0.2378 |
| x_adx | x_adx>=48.47438(q0.9)@SL1.0/T2.0 | 810/0.24 | 656/0.242 | 0.2376 |
| close_loc | close_loc>=0.9912(q0.7)@SL1.0/T2.0 | 1514/0.247 | 1058/0.241 | 0.2356 |
| x_svwap_dist_pct | x_svwap_dist_pct>=2.347151(q0.9)@SL1.0/T2.0 | 970/0.295 | 675/0.262 | 0.2355 |
| x_roc3 | x_roc3<=1.436323(q0.9)@SL1.0/T2.0 | 1692/0.251 | 1208/0.242 | 0.2354 |
| x_ema20_dist_atr | x_ema20_dist_atr>=1.577299(q0.3)@SL1.0/T2.0 | 1996/0.25 | 1485/0.242 | 0.2352 |
| x_kelt_pos | x_kelt_pos>=1.025766(q0.3)@SL1.0/T2.0 | 1996/0.25 | 1485/0.242 | 0.2352 |
| x_roc12 | x_roc12>=2.795961(q0.9)@SL1.0/T2.0 | 986/0.283 | 746/0.256 | 0.2352 |
| x_day_ret_pct | x_day_ret_pct>=2.623723(q0.7)@SL1.0/T2.0 | 1812/0.243 | 1429/0.254 | 0.2346 |
| x_dist_dayhigh_atr | x_dist_dayhigh_atr<=1.65932(q0.7)@SL1.0/T2.0 | 1983/0.244 | 1448/0.239 | 0.2343 |
| x_cci20 | x_cci20<=100.45518(q0.5)@SL1.0/T2.0 | 1601/0.253 | 1099/0.242 | 0.2327 |
| x_rsi_slope3 | x_rsi_slope3>=15.283638(q0.9)@SL1.0/T2.0 | 1033/0.268 | 725/0.247 | 0.2307 |
| x_vol_vs_avg20 | x_vol_vs_avg20>=1.545458(q0.1)@SL1.0/T2.0 | 1940/0.251 | 1472/0.239 | 0.23 |
| x_pos_in_dayrange | x_pos_in_dayrange>=0.615054(q0.1)@SL1.0/T2.0 | 2009/0.243 | 1501/0.236 | 0.2299 |
| x_reg_bulltrend | x_reg_bulltrend<=0.5@SL1.0/T2.0 | 1518/0.238 | 1216/0.233 | 0.229 |
| x_ema200_dist_atr | x_ema200_dist_atr>=11.401821(q0.9)@SL1.0/T2.0 | 685/0.234 | 433/0.242 | 0.2282 |
| x_stoch_d | x_stoch_d>=84.469348(q0.5)@SL1.0/T2.0 | 1800/0.272 | 1354/0.248 | 0.2282 |
| x_pdh_dist_atr | x_pdh_dist_atr>=8.39447(q0.9)@SL1.0/T2.0 | 641/0.236 | 473/0.248 | 0.227 |
| x_ema50_dist_atr | x_ema50_dist_atr>=2.263982(q0.3)@SL1.0/T2.0 | 1913/0.245 | 1481/0.235 | 0.2266 |
| x_roc6 | x_roc6>=1.044175(q0.7)@SL1.0/T2.0 | 2006/0.269 | 1534/0.245 | 0.2263 |
| x_atr_pct | x_atr_pct>=0.001887(q0.1)@SL1.0/T2.0 | 1990/0.253 | 1506/0.238 | 0.2258 |
| x_mfi14 | x_mfi14>=63.09824(q0.3)@SL1.0/T2.0 | 1927/0.257 | 1442/0.24 | 0.2256 |
| upper_wick_pct | upper_wick_pct<=0.034404(q0.5)@SL1.0/T2.0 | 1573/0.245 | 1100/0.234 | 0.225 |
| atr_pct | atr_pct>=0.001887(q0.1)@SL1.0/T2.0 | 1990/0.253 | 1504/0.237 | 0.2242 |
| x_bb_pos | x_bb_pos>=0.882815(q0.3)@SL1.0/T2.0 | 1969/0.251 | 1449/0.235 | 0.2222 |
| x_range_vs_avg20 | x_range_vs_avg20>=2.093972(q0.9)@SL1.0/T2.0 | 1085/0.288 | 763/0.25 | 0.2205 |
| x_macd_hist_delta_atr | x_macd_hist_delta_atr>=0.047252(q0.3)@SL1.0/T2.0 | 1860/0.263 | 1389/0.239 | 0.2203 |
| x_obv_slope5 | x_obv_slope5>=0.644501(q0.1)@SL1.0/T2.0 | 1941/0.253 | 1454/0.234 | 0.2189 |
| vol_ratio | vol_ratio>=1.90026(q0.3)@SL1.0/T2.0 | 1893/0.263 | 1407/0.238 | 0.2184 |
| x_rsi | x_rsi>=61.964547(q0.3)@SL1.0/T2.0 | 1979/0.254 | 1497/0.234 | 0.2184 |
| x_adx_slope3 | x_adx_slope3<=2.96856(q0.7)@SL1.0/T2.0 | 1720/0.263 | 1232/0.238 | 0.2177 |
| x_reg_notbear | x_reg_notbear<=0.5@SL0.7/T1.5 | 1369/0.223 | 766/0.22 | 0.2177 |
| x_ema20_gt50 | x_ema20_gt50>=0.5@SL1.0/T2.0 | 1927/0.244 | 1505/0.229 | 0.2175 |
| x_above_pdh | x_above_pdh>=0.5@SL1.0/T2.0 | 1763/0.219 | 1464/0.223 | 0.2164 |
| x_macd_hist_atr | x_macd_hist_atr>=-0.139386(q0.1)@SL1.0/T2.0 | 1985/0.247 | 1458/0.23 | 0.216 |
| x_ema_stack | x_ema_stack>=0.5@SL1.0/T2.0 | 1769/0.225 | 1451/0.237 | 0.2147 |
| x_macd_above_sig | x_macd_above_sig<=0.5@SL1.0/T2.0 | 1162/0.237 | 817/0.225 | 0.2145 |
| x_reg_bull | x_reg_bull<=0.5@SL1.0/T2.0 | 1559/0.257 | 1216/0.233 | 0.2141 |
| x_break_rank_day | x_break_rank_day>=0.0(q0.1)@SL1.0/T2.0 | 1963/0.25 | 1488/0.228 | 0.2107 |
| x_prev_pullback | x_prev_pullback>=0.5@SL1.0/T2.0 | 1809/0.243 | 1293/0.221 | 0.2037 |
| x_first_break_of_day | x_first_break_of_day>=0.5@SL1.0/T2.0 | 1786/0.265 | 1306/0.226 | 0.1943 |

## Notes

- Thresholds are TRAIN deciles only; market_ret/notional/signal-minute masks excluded (documented overfit vectors); time-of-day via slot guards.
- Exit grid anchored at SL0.70/T1.50 and SL1.00/T2.00 for sweeps; the full 7x7 exit grid is explored inside the TPE search; MFE/MAE bracket feasibility in WINNER_LOSER_STUDY.md shows every bracket's win-rate ceiling.