# A_MOD_CLOSE_CONTINUATION_BREAK (LONG) — ITERATION_LOG (recovery loop)

_Generated 2026-07-03. Research-only; NO live trades; NO final_setup_conf.py edits._

Total logged iterations: **1222**. Full trials: `trials_optuna_rec.csv` (+ `trials_rescue_tpe.csv`).

Command:
```
py -3.12 Train_and_Test\setup_from_scratch_recovery_loop\A_MOD_CLOSE_CONTINUATION_BREAK\scripts\campaign_recovery.py --trials 3000 --time_budget_min 45 --seed 17
```

| # | group | change | FIT n/PF | VAL n/PF | TRAIN | TEST | decision | failure / next |
|---|---|---|---|---|---|---|---|---|
| 1 | redesign | R1_uncollapsed_card@SL0.7/T1.5 | 2980/0.244 | 2208/0.196 | - | - | reject | FIT/VAL PF < 0.9 or too thin / drop |
| 2 | redesign | R1_uncollapsed_card@SL1.0/T2.0 | 1963/0.25 | 1488/0.228 | - | - | reject | FIT/VAL PF < 0.9 or too thin / drop |
| 3 | redesign | R1_uncollapsed_card@SL0.5/T1.0 | 4638/0.202 | 3365/0.162 | - | - | reject | FIT/VAL PF < 0.9 or too thin / drop |
| 4 | redesign | R2_notbear@SL0.7/T1.5 | 2111/0.239 | 1773/0.196 | - | - | reject | FIT/VAL PF < 0.9 or too thin / drop |
| 5 | redesign | R2_notbear@SL1.0/T2.0 | 1435/0.26 | 1173/0.236 | - | - | reject | FIT/VAL PF < 0.9 or too thin / drop |
| 6 | redesign | R2_notbear@SL0.5/T1.0 | 3380/0.2 | 2602/0.164 | - | - | reject | FIT/VAL PF < 0.9 or too thin / drop |
| 7 | redesign | R2b_bulltrend@SL0.7/T1.5 | 1591/0.223 | 961/0.175 | - | - | reject | FIT/VAL PF < 0.9 or too thin / drop |
| 8 | redesign | R2b_bulltrend@SL1.0/T2.0 | 1128/0.26 | 689/0.208 | - | - | reject | FIT/VAL PF < 0.9 or too thin / drop |
| 9 | redesign | R2b_bulltrend@SL0.5/T1.0 | 2379/0.199 | 1340/0.145 | - | - | reject | FIT/VAL PF < 0.9 or too thin / drop |
| 10 | redesign | R3_first_break@SL0.7/T1.5 | 2704/0.244 | 1946/0.185 | - | - | reject | FIT/VAL PF < 0.9 or too thin / drop |
| 11 | redesign | R3_first_break@SL1.0/T2.0 | 1786/0.265 | 1306/0.226 | - | - | reject | FIT/VAL PF < 0.9 or too thin / drop |
| 12 | redesign | R3_first_break@SL0.5/T1.0 | 3977/0.199 | 2842/0.163 | - | - | reject | FIT/VAL PF < 0.9 or too thin / drop |
| 13 | redesign | R4_fresh_break@SL0.7/T1.5 | 2816/0.242 | 2065/0.187 | - | - | reject | FIT/VAL PF < 0.9 or too thin / drop |
| 14 | redesign | R4_fresh_break@SL1.0/T2.0 | 1891/0.246 | 1441/0.244 | - | - | reject | FIT/VAL PF < 0.9 or too thin / drop |
| 15 | redesign | R4_fresh_break@SL0.5/T1.0 | 4255/0.201 | 3050/0.163 | - | - | reject | FIT/VAL PF < 0.9 or too thin / drop |
| 16 | redesign | R5_pullback_then_break@SL0.7/T1.5 | 2579/0.243 | 1865/0.195 | - | - | reject | FIT/VAL PF < 0.9 or too thin / drop |
| 17 | redesign | R5_pullback_then_break@SL1.0/T2.0 | 1809/0.243 | 1293/0.221 | - | - | reject | FIT/VAL PF < 0.9 or too thin / drop |
| 18 | redesign | R5_pullback_then_break@SL0.5/T1.0 | 3789/0.197 | 2682/0.172 | - | - | reject | FIT/VAL PF < 0.9 or too thin / drop |
| 19 | redesign | R6_morning@SL0.7/T1.5 | 1750/0.359 | 1307/0.238 | - | - | reject | FIT/VAL PF < 0.9 or too thin / drop |
| 20 | redesign | R6_morning@SL1.0/T2.0 | 1296/0.411 | 937/0.27 | - | - | reject | FIT/VAL PF < 0.9 or too thin / drop |
| 21 | redesign | R6_morning@SL0.5/T1.0 | 2442/0.28 | 1812/0.197 | - | - | reject | FIT/VAL PF < 0.9 or too thin / drop |
| 22 | redesign | R7_aligned_thrust@SL0.7/T1.5 | 1461/0.303 | 1154/0.213 | - | - | reject | FIT/VAL PF < 0.9 or too thin / drop |
| 23 | redesign | R7_aligned_thrust@SL1.0/T2.0 | 1117/0.355 | 859/0.258 | - | - | reject | FIT/VAL PF < 0.9 or too thin / drop |
| 24 | redesign | R7_aligned_thrust@SL0.5/T1.0 | 1999/0.239 | 1440/0.185 | - | - | reject | FIT/VAL PF < 0.9 or too thin / drop |
| 25 | redesign | R8_ranked_top1@SL0.7/T1.5 | 895/0.368 | 614/0.355 | - | - | reject | FIT/VAL PF < 0.9 or too thin / drop |
| 26 | redesign | R8_ranked_top1@SL1.0/T2.0 | 892/0.43 | 614/0.435 | - | - | reject | FIT/VAL PF < 0.9 or too thin / drop |
| 27 | redesign | R8_ranked_top1@SL0.5/T1.0 | 895/0.26 | 614/0.287 | - | - | reject | FIT/VAL PF < 0.9 or too thin / drop |
| 28 | redesign | R23_notbear_first@SL0.7/T1.5 | 1923/0.255 | 1555/0.182 | - | - | reject | FIT/VAL PF < 0.9 or too thin / drop |
| 29 | redesign | R23_notbear_first@SL1.0/T2.0 | 1295/0.282 | 1049/0.22 | - | - | reject | FIT/VAL PF < 0.9 or too thin / drop |
| 30 | redesign | R23_notbear_first@SL0.5/T1.0 | 2906/0.199 | 2240/0.158 | - | - | reject | FIT/VAL PF < 0.9 or too thin / drop |
| 31 | redesign | R24_notbear_fresh@SL0.7/T1.5 | 2044/0.238 | 1676/0.196 | - | - | reject | FIT/VAL PF < 0.9 or too thin / drop |
| 32 | redesign | R24_notbear_fresh@SL1.0/T2.0 | 1371/0.267 | 1167/0.269 | - | - | reject | FIT/VAL PF < 0.9 or too thin / drop |
| 33 | redesign | R24_notbear_fresh@SL0.5/T1.0 | 3108/0.207 | 2402/0.168 | - | - | reject | FIT/VAL PF < 0.9 or too thin / drop |
| 34 | redesign | R26_notbear_morning@SL0.7/T1.5 | 1130/0.354 | 974/0.223 | - | - | reject | FIT/VAL PF < 0.9 or too thin / drop |
| 35 | redesign | R26_notbear_morning@SL1.0/T2.0 | 839/0.431 | 727/0.266 | - | - | reject | FIT/VAL PF < 0.9 or too thin / drop |
| 36 | redesign | R26_notbear_morning@SL0.5/T1.0 | 1648/0.275 | 1332/0.185 | - | - | reject | FIT/VAL PF < 0.9 or too thin / drop |
| 37 | redesign | R2b4_bulltrend_fresh@SL0.7/T1.5 | 1523/0.234 | 883/0.169 | - | - | reject | FIT/VAL PF < 0.9 or too thin / drop |
| 38 | redesign | R2b4_bulltrend_fresh@SL1.0/T2.0 | 1085/0.25 | 656/0.234 | - | - | reject | FIT/VAL PF < 0.9 or too thin / drop |
| 39 | redesign | R2b4_bulltrend_fresh@SL0.5/T1.0 | 2222/0.201 | 1225/0.161 | - | - | reject | FIT/VAL PF < 0.9 or too thin / drop |
| 40 | redesign | R35_first_pullback@SL0.7/T1.5 | 2274/0.242 | 1625/0.189 | - | - | reject | FIT/VAL PF < 0.9 or too thin / drop |
| 41 | redesign | R35_first_pullback@SL1.0/T2.0 | 1617/0.24 | 1145/0.215 | - | - | reject | FIT/VAL PF < 0.9 or too thin / drop |
| 42 | redesign | R35_first_pullback@SL0.5/T1.0 | 3191/0.192 | 2144/0.168 | - | - | reject | FIT/VAL PF < 0.9 or too thin / drop |
| 43 | sweep | rs_pct>=0.36972@SL0.7/T1.5 | 2984/0.242 | 2223/0.198 | - | - | reject | weak FIT/VAL / drop |
| 44 | sweep | rs_pct>=0.36972@SL1.0/T2.0 | 1997/0.256 | 1507/0.241 | - | - | reject | weak FIT/VAL / drop |
| 45 | sweep | rs_pct<=0.36972@SL0.7/T1.5 | 1001/0.288 | 758/0.125 | - | - | reject | weak FIT/VAL / drop |
| 46 | sweep | rs_pct<=0.36972@SL1.0/T2.0 | 836/0.298 | 635/0.141 | - | - | reject | weak FIT/VAL / drop |
| 47 | sweep | rs_pct>=1.05992@SL0.7/T1.5 | 2927/0.225 | 2263/0.193 | - | - | reject | weak FIT/VAL / drop |
| 48 | sweep | rs_pct>=1.05992@SL1.0/T2.0 | 2035/0.242 | 1551/0.235 | - | - | reject | weak FIT/VAL / drop |
| 49 | sweep | rs_pct<=1.05992@SL0.7/T1.5 | 1658/0.283 | 1230/0.171 | - | - | reject | weak FIT/VAL / drop |
| 50 | sweep | rs_pct<=1.05992@SL1.0/T2.0 | 1261/0.274 | 899/0.209 | - | - | reject | weak FIT/VAL / drop |
| 51 | sweep | rs_pct>=1.8046@SL0.7/T1.5 | 2889/0.212 | 2105/0.213 | - | - | reject | weak FIT/VAL / drop |
| 52 | sweep | rs_pct>=1.8046@SL1.0/T2.0 | 2070/0.229 | 1581/0.24 | - | - | reject | weak FIT/VAL / drop |
| 53 | sweep | rs_pct<=1.8046@SL0.7/T1.5 | 1926/0.294 | 1474/0.161 | - | - | reject | weak FIT/VAL / drop |
| 54 | sweep | rs_pct<=1.8046@SL1.0/T2.0 | 1380/0.336 | 1023/0.188 | - | - | reject | weak FIT/VAL / drop |
| 55 | sweep | rs_pct>=2.81472@SL0.7/T1.5 | 2504/0.202 | 1571/0.229 | - | - | reject | weak FIT/VAL / drop |
| 56 | sweep | rs_pct>=2.81472@SL1.0/T2.0 | 1929/0.23 | 1376/0.263 | - | - | reject | weak FIT/VAL / drop |
| 57 | sweep | rs_pct<=2.81472@SL0.7/T1.5 | 2216/0.276 | 1671/0.168 | - | - | reject | weak FIT/VAL / drop |
| 58 | sweep | rs_pct<=2.81472@SL1.0/T2.0 | 1495/0.296 | 1113/0.205 | - | - | reject | weak FIT/VAL / drop |
| 59 | sweep | rs_pct>=5.03868@SL0.7/T1.5 | 1075/0.211 | 643/0.226 | - | - | reject | weak FIT/VAL / drop |
| 60 | sweep | rs_pct>=5.03868@SL1.0/T2.0 | 1040/0.254 | 643/0.272 | - | - | reject | weak FIT/VAL / drop |
| 61 | sweep | rs_pct<=5.03868@SL0.7/T1.5 | 2588/0.249 | 1920/0.198 | - | - | reject | weak FIT/VAL / drop |
| 62 | sweep | rs_pct<=5.03868@SL1.0/T2.0 | 1709/0.272 | 1270/0.211 | - | - | reject | weak FIT/VAL / drop |
| 63 | sweep | vol_ratio>=1.61132@SL0.7/T1.5 | 2913/0.249 | 2183/0.193 | - | - | reject | weak FIT/VAL / drop |
| 64 | sweep | vol_ratio>=1.61132@SL1.0/T2.0 | 1938/0.249 | 1447/0.229 | - | - | reject | weak FIT/VAL / drop |
| 65 | sweep | vol_ratio<=1.61132@SL0.7/T1.5 | 854/0.213 | 498/0.17 | - | - | reject | weak FIT/VAL / drop |
| 66 | sweep | vol_ratio<=1.61132@SL1.0/T2.0 | 825/0.204 | 495/0.183 | - | - | reject | weak FIT/VAL / drop |
| 67 | sweep | vol_ratio>=1.90026@SL0.7/T1.5 | 2783/0.242 | 2044/0.195 | - | - | reject | weak FIT/VAL / drop |
| 68 | sweep | vol_ratio>=1.90026@SL1.0/T2.0 | 1893/0.263 | 1407/0.238 | - | - | reject | weak FIT/VAL / drop |
| 69 | sweep | vol_ratio<=1.90026@SL0.7/T1.5 | 1902/0.232 | 1301/0.16 | - | - | reject | weak FIT/VAL / drop |
| 70 | sweep | vol_ratio<=1.90026@SL1.0/T2.0 | 1475/0.229 | 1034/0.2 | - | - | reject | weak FIT/VAL / drop |
| 71 | sweep | vol_ratio>=2.3124@SL0.7/T1.5 | 2552/0.252 | 1846/0.18 | - | - | reject | weak FIT/VAL / drop |
| 72 | sweep | vol_ratio>=2.3124@SL1.0/T2.0 | 1784/0.273 | 1275/0.22 | - | - | reject | weak FIT/VAL / drop |
| 73 | sweep | vol_ratio<=2.3124@SL0.7/T1.5 | 2354/0.231 | 1702/0.18 | - | - | reject | weak FIT/VAL / drop |
| 74 | sweep | vol_ratio<=2.3124@SL1.0/T2.0 | 1674/0.242 | 1233/0.215 | - | - | reject | weak FIT/VAL / drop |
| 75 | sweep | vol_ratio>=2.97468@SL0.7/T1.5 | 2190/0.275 | 1562/0.204 | - | - | reject | weak FIT/VAL / drop |
| 76 | sweep | vol_ratio>=2.97468@SL1.0/T2.0 | 1642/0.303 | 1143/0.212 | - | - | reject | weak FIT/VAL / drop |
| 77 | sweep | vol_ratio<=2.97468@SL0.7/T1.5 | 2619/0.229 | 1942/0.166 | - | - | reject | weak FIT/VAL / drop |
| 78 | sweep | vol_ratio<=2.97468@SL1.0/T2.0 | 1809/0.249 | 1371/0.21 | - | - | reject | weak FIT/VAL / drop |
| 79 | sweep | vol_ratio>=4.56842@SL0.7/T1.5 | 1345/0.315 | 894/0.205 | - | - | reject | weak FIT/VAL / drop |
| 80 | sweep | vol_ratio>=4.56842@SL1.0/T2.0 | 1162/0.348 | 781/0.215 | - | - | reject | weak FIT/VAL / drop |
| 81 | sweep | vol_ratio<=4.56842@SL0.7/T1.5 | 2855/0.232 | 2132/0.189 | - | - | reject | weak FIT/VAL / drop |
| 82 | sweep | vol_ratio<=4.56842@SL1.0/T2.0 | 1930/0.252 | 1460/0.217 | - | - | reject | weak FIT/VAL / drop |
| 83 | sweep | atr_pct>=0.001887@SL0.7/T1.5 | 3031/0.242 | 2269/0.199 | - | - | reject | weak FIT/VAL / drop |
| 84 | sweep | atr_pct>=0.001887@SL1.0/T2.0 | 1990/0.253 | 1504/0.237 | - | - | reject | weak FIT/VAL / drop |
| 85 | sweep | atr_pct<=0.001887@SL0.7/T1.5 | 726/0.196 | 563/0.126 | - | - | reject | weak FIT/VAL / drop |
| 86 | sweep | atr_pct<=0.001887@SL1.0/T2.0 | 640/0.206 | 484/0.143 | - | - | reject | weak FIT/VAL / drop |
| 87 | sweep | atr_pct>=0.002573@SL0.7/T1.5 | 3120/0.246 | 2363/0.201 | - | - | reject | weak FIT/VAL / drop |
| 88 | sweep | atr_pct>=0.002573@SL1.0/T2.0 | 2054/0.266 | 1546/0.237 | - | - | reject | weak FIT/VAL / drop |
| 89 | sweep | atr_pct<=0.002573@SL0.7/T1.5 | 1263/0.227 | 921/0.148 | - | - | reject | weak FIT/VAL / drop |
| 90 | sweep | atr_pct<=0.002573@SL1.0/T2.0 | 980/0.256 | 689/0.163 | - | - | reject | weak FIT/VAL / drop |
| 91 | sweep | atr_pct>=0.00327@SL0.7/T1.5 | 3159/0.251 | 2334/0.195 | - | - | reject | weak FIT/VAL / drop |
| 92 | sweep | atr_pct>=0.00327@SL1.0/T2.0 | 2116/0.271 | 1614/0.23 | - | - | reject | weak FIT/VAL / drop |
| 93 | sweep | atr_pct<=0.00327@SL0.7/T1.5 | 1579/0.264 | 1111/0.148 | - | - | reject | weak FIT/VAL / drop |
| 94 | sweep | atr_pct<=0.00327@SL1.0/T2.0 | 1144/0.29 | 775/0.167 | - | - | reject | weak FIT/VAL / drop |
| 95 | sweep | atr_pct>=0.004278@SL0.7/T1.5 | 2638/0.245 | 1955/0.208 | - | - | reject | weak FIT/VAL / drop |
| 96 | sweep | atr_pct>=0.004278@SL1.0/T2.0 | 2034/0.269 | 1579/0.241 | - | - | reject | weak FIT/VAL / drop |
| 97 | sweep | atr_pct<=0.004278@SL0.7/T1.5 | 1906/0.262 | 1317/0.175 | - | - | reject | weak FIT/VAL / drop |
| 98 | sweep | atr_pct<=0.004278@SL1.0/T2.0 | 1310/0.283 | 886/0.199 | - | - | reject | weak FIT/VAL / drop |
| 99 | sweep | atr_pct>=0.007081@SL0.7/T1.5 | 962/0.208 | 749/0.205 | - | - | reject | weak FIT/VAL / drop |
| 100 | sweep | atr_pct>=0.007081@SL1.0/T2.0 | 945/0.25 | 749/0.234 | - | - | reject | weak FIT/VAL / drop |
| 101 | sweep | atr_pct<=0.007081@SL0.7/T1.5 | 2444/0.249 | 1769/0.197 | - | - | reject | weak FIT/VAL / drop |
| 102 | sweep | atr_pct<=0.007081@SL1.0/T2.0 | 1593/0.249 | 1180/0.222 | - | - | reject | weak FIT/VAL / drop |
| 103 | sweep | body_pct>=0.4444@SL0.7/T1.5 | 2901/0.251 | 2215/0.189 | - | - | reject | weak FIT/VAL / drop |
| 104 | sweep | body_pct>=0.4444@SL1.0/T2.0 | 1934/0.25 | 1480/0.218 | - | - | reject | weak FIT/VAL / drop |
| 105 | sweep | body_pct<=0.4444@SL0.7/T1.5 | 1029/0.258 | 608/0.263 | - | - | reject | weak FIT/VAL / drop |
| 106 | sweep | body_pct<=0.4444@SL1.0/T2.0 | 950/0.269 | 584/0.294 | - | - | reject | weak FIT/VAL / drop |
| 107 | sweep | body_pct>=0.6593@SL0.7/T1.5 | 2775/0.245 | 2031/0.195 | - | - | reject | weak FIT/VAL / drop |
| 108 | sweep | body_pct>=0.6593@SL1.0/T2.0 | 1906/0.255 | 1375/0.204 | - | - | reject | weak FIT/VAL / drop |
| 109 | sweep | body_pct<=0.6593@SL0.7/T1.5 | 1990/0.247 | 1340/0.197 | - | - | reject | weak FIT/VAL / drop |
| 110 | sweep | body_pct<=0.6593@SL1.0/T2.0 | 1484/0.267 | 1019/0.242 | - | - | reject | weak FIT/VAL / drop |
| 111 | sweep | body_pct>=0.77@SL0.7/T1.5 | 2595/0.245 | 1846/0.161 | - | - | reject | weak FIT/VAL / drop |
| 112 | sweep | body_pct>=0.77@SL1.0/T2.0 | 1793/0.251 | 1303/0.198 | - | - | reject | weak FIT/VAL / drop |
| 113 | sweep | body_pct<=0.77@SL0.7/T1.5 | 2320/0.254 | 1712/0.206 | - | - | reject | weak FIT/VAL / drop |
| 114 | sweep | body_pct<=0.77@SL1.0/T2.0 | 1668/0.283 | 1204/0.261 | - | - | reject | weak FIT/VAL / drop |
| 115 | sweep | body_pct>=0.8559@SL0.7/T1.5 | 2150/0.252 | 1467/0.167 | - | - | reject | weak FIT/VAL / drop |
| 116 | sweep | body_pct>=0.8559@SL1.0/T2.0 | 1619/0.262 | 1128/0.206 | - | - | reject | weak FIT/VAL / drop |
| 117 | sweep | body_pct<=0.8559@SL0.7/T1.5 | 2629/0.238 | 1997/0.2 | - | - | reject | weak FIT/VAL / drop |
| 118 | sweep | body_pct<=0.8559@SL1.0/T2.0 | 1826/0.269 | 1374/0.238 | - | - | reject | weak FIT/VAL / drop |
| 119 | sweep | body_pct>=0.9741@SL0.7/T1.5 | 1099/0.253 | 678/0.163 | - | - | reject | weak FIT/VAL / drop |
| 120 | sweep | body_pct>=0.9741@SL1.0/T2.0 | 977/0.265 | 626/0.2 | - | - | reject | weak FIT/VAL / drop |
| 121 | sweep | body_pct<=0.9741@SL0.7/T1.5 | 2916/0.244 | 2141/0.196 | - | - | reject | weak FIT/VAL / drop |
| 122 | sweep | body_pct<=0.9741@SL1.0/T2.0 | 1935/0.267 | 1454/0.24 | - | - | reject | weak FIT/VAL / drop |
| 123 | sweep | close_loc>=0.7857@SL0.7/T1.5 | 2884/0.248 | 2139/0.186 | - | - | reject | weak FIT/VAL / drop |
| 124 | sweep | close_loc>=0.7857@SL1.0/T2.0 | 1904/0.262 | 1455/0.216 | - | - | reject | weak FIT/VAL / drop |
| 125 | sweep | close_loc<=0.7857@SL0.7/T1.5 | 1055/0.277 | 693/0.17 | - | - | reject | weak FIT/VAL / drop |
| 126 | sweep | close_loc<=0.7857@SL1.0/T2.0 | 944/0.262 | 666/0.191 | - | - | reject | weak FIT/VAL / drop |
| 127 | sweep | close_loc>=0.8539@SL0.7/T1.5 | 2718/0.258 | 1948/0.184 | - | - | reject | weak FIT/VAL / drop |
| 128 | sweep | close_loc>=0.8539@SL1.0/T2.0 | 1849/0.25 | 1345/0.227 | - | - | reject | weak FIT/VAL / drop |
| 129 | sweep | close_loc<=0.8539@SL0.7/T1.5 | 2009/0.254 | 1485/0.178 | - | - | reject | weak FIT/VAL / drop |
| 130 | sweep | close_loc<=0.8539@SL1.0/T2.0 | 1505/0.261 | 1143/0.216 | - | - | reject | weak FIT/VAL / drop |
| 131 | sweep | close_loc>=0.9194@SL0.7/T1.5 | 2491/0.246 | 1723/0.185 | - | - | reject | weak FIT/VAL / drop |
| 132 | sweep | close_loc>=0.9194@SL1.0/T2.0 | 1718/0.247 | 1227/0.23 | - | - | reject | weak FIT/VAL / drop |
| 133 | sweep | close_loc<=0.9194@SL0.7/T1.5 | 2440/0.249 | 1826/0.188 | - | - | reject | weak FIT/VAL / drop |
| 134 | sweep | close_loc<=0.9194@SL1.0/T2.0 | 1784/0.266 | 1317/0.208 | - | - | reject | weak FIT/VAL / drop |
| 135 | sweep | close_loc>=0.9912@SL0.7/T1.5 | 2031/0.249 | 1382/0.191 | - | - | reject | weak FIT/VAL / drop |
| 136 | sweep | close_loc>=0.9912@SL1.0/T2.0 | 1514/0.247 | 1058/0.241 | - | - | reject | weak FIT/VAL / drop |
| 137 | sweep | close_loc<=0.9912@SL0.7/T1.5 | 2730/0.24 | 2051/0.178 | - | - | reject | weak FIT/VAL / drop |
| 138 | sweep | close_loc<=0.9912@SL1.0/T2.0 | 1885/0.27 | 1404/0.22 | - | - | reject | weak FIT/VAL / drop |
| 139 | sweep | close_loc>=1.0@SL0.7/T1.5 | 2003/0.251 | 1346/0.188 | - | - | reject | weak FIT/VAL / drop |
| 140 | sweep | close_loc>=1.0@SL1.0/T2.0 | 1496/0.258 | 1033/0.23 | - | - | reject | weak FIT/VAL / drop |
| 141 | sweep | close_loc<=1.0@SL0.7/T1.5 | 2980/0.244 | 2208/0.196 | - | - | reject | weak FIT/VAL / drop |
| 142 | sweep | close_loc<=1.0@SL1.0/T2.0 | 1963/0.25 | 1488/0.228 | - | - | reject | weak FIT/VAL / drop |
| 143 | sweep | vwap_dist_atr>=0.83472@SL0.7/T1.5 | 2953/0.237 | 2194/0.204 | - | - | reject | weak FIT/VAL / drop |
| 144 | sweep | vwap_dist_atr>=0.83472@SL1.0/T2.0 | 2023/0.26 | 1478/0.234 | - | - | reject | weak FIT/VAL / drop |
| 145 | sweep | vwap_dist_atr<=0.83472@SL0.7/T1.5 | 1120/0.204 | 629/0.163 | - | - | reject | weak FIT/VAL / drop |
| 146 | sweep | vwap_dist_atr<=0.83472@SL1.0/T2.0 | 974/0.224 | 583/0.186 | - | - | reject | weak FIT/VAL / drop |
| 147 | sweep | vwap_dist_atr>=1.86566@SL0.7/T1.5 | 2619/0.251 | 2005/0.197 | - | - | reject | weak FIT/VAL / drop |
| 148 | sweep | vwap_dist_atr>=1.86566@SL1.0/T2.0 | 1878/0.248 | 1390/0.259 | - | - | reject | weak FIT/VAL / drop |
| 149 | sweep | vwap_dist_atr<=1.86566@SL0.7/T1.5 | 2104/0.231 | 1442/0.165 | - | - | reject | weak FIT/VAL / drop |
| 150 | sweep | vwap_dist_atr<=1.86566@SL1.0/T2.0 | 1534/0.259 | 1102/0.197 | - | - | reject | weak FIT/VAL / drop |
| 151 | sweep | vwap_dist_atr>=2.7676@SL0.7/T1.5 | 2173/0.253 | 1646/0.197 | - | - | reject | weak FIT/VAL / drop |
| 152 | sweep | vwap_dist_atr>=2.7676@SL1.0/T2.0 | 1570/0.273 | 1173/0.241 | - | - | reject | weak FIT/VAL / drop |
| 153 | sweep | vwap_dist_atr<=2.7676@SL0.7/T1.5 | 2514/0.256 | 1798/0.184 | - | - | reject | weak FIT/VAL / drop |
| 154 | sweep | vwap_dist_atr<=2.7676@SL1.0/T2.0 | 1768/0.266 | 1283/0.196 | - | - | reject | weak FIT/VAL / drop |
| 155 | sweep | vwap_dist_atr>=3.81808@SL0.7/T1.5 | 1612/0.265 | 1194/0.196 | - | - | reject | weak FIT/VAL / drop |
| 156 | sweep | vwap_dist_atr>=3.81808@SL1.0/T2.0 | 1252/0.3 | 896/0.25 | - | - | reject | weak FIT/VAL / drop |
| 157 | sweep | vwap_dist_atr<=3.81808@SL0.7/T1.5 | 2754/0.247 | 2009/0.198 | - | - | reject | weak FIT/VAL / drop |
| 158 | sweep | vwap_dist_atr<=3.81808@SL1.0/T2.0 | 1882/0.25 | 1403/0.214 | - | - | reject | weak FIT/VAL / drop |
| 159 | sweep | vwap_dist_atr>=5.55542@SL0.7/T1.5 | 854/0.265 | 484/0.197 | - | - | reject | weak FIT/VAL / drop |
| 160 | sweep | vwap_dist_atr>=5.55542@SL1.0/T2.0 | 718/0.32 | 439/0.262 | - | - | reject | weak FIT/VAL / drop |
| 161 | sweep | vwap_dist_atr<=5.55542@SL0.7/T1.5 | 2944/0.241 | 2193/0.196 | - | - | reject | weak FIT/VAL / drop |
| 162 | sweep | vwap_dist_atr<=5.55542@SL1.0/T2.0 | 1943/0.252 | 1463/0.223 | - | - | reject | weak FIT/VAL / drop |
| 163 | sweep | quality_score>=37.8186@SL0.7/T1.5 | 2970/0.239 | 2220/0.198 | - | - | reject | weak FIT/VAL / drop |
| 164 | sweep | quality_score>=37.8186@SL1.0/T2.0 | 1990/0.252 | 1512/0.241 | - | - | reject | weak FIT/VAL / drop |
| 165 | sweep | quality_score<=37.8186@SL0.7/T1.5 | 743/0.287 | 611/0.151 | - | - | reject | weak FIT/VAL / drop |
| 166 | sweep | quality_score<=37.8186@SL1.0/T2.0 | 651/0.283 | 534/0.149 | - | - | reject | weak FIT/VAL / drop |
| 167 | sweep | quality_score>=60.0106@SL0.7/T1.5 | 2959/0.225 | 2227/0.197 | - | - | reject | weak FIT/VAL / drop |
| 168 | sweep | quality_score>=60.0106@SL1.0/T2.0 | 2019/0.247 | 1530/0.239 | - | - | reject | weak FIT/VAL / drop |
| 169 | sweep | quality_score<=60.0106@SL0.7/T1.5 | 1583/0.266 | 1230/0.134 | - | - | reject | weak FIT/VAL / drop |
| 170 | sweep | quality_score<=60.0106@SL1.0/T2.0 | 1240/0.289 | 896/0.189 | - | - | reject | weak FIT/VAL / drop |
| 171 | sweep | quality_score>=79.549@SL0.7/T1.5 | 2864/0.211 | 2067/0.208 | - | - | reject | weak FIT/VAL / drop |
| 172 | sweep | quality_score>=79.549@SL1.0/T2.0 | 2037/0.239 | 1521/0.232 | - | - | reject | weak FIT/VAL / drop |
| 173 | sweep | quality_score<=79.549@SL0.7/T1.5 | 1927/0.298 | 1473/0.17 | - | - | reject | weak FIT/VAL / drop |
| 174 | sweep | quality_score<=79.549@SL1.0/T2.0 | 1395/0.297 | 1041/0.204 | - | - | reject | weak FIT/VAL / drop |
| 175 | sweep | quality_score>=104.7686@SL0.7/T1.5 | 2573/0.216 | 1664/0.219 | - | - | reject | weak FIT/VAL / drop |
| 176 | sweep | quality_score>=104.7686@SL1.0/T2.0 | 1965/0.23 | 1418/0.259 | - | - | reject | weak FIT/VAL / drop |
| 177 | sweep | quality_score<=104.7686@SL0.7/T1.5 | 2189/0.26 | 1654/0.171 | - | - | reject | weak FIT/VAL / drop |
| 178 | sweep | quality_score<=104.7686@SL1.0/T2.0 | 1508/0.281 | 1111/0.215 | - | - | reject | weak FIT/VAL / drop |
| 179 | sweep | quality_score>=159.4978@SL0.7/T1.5 | 1188/0.232 | 635/0.251 | - | - | reject | weak FIT/VAL / drop |
| 180 | sweep | quality_score>=159.4978@SL1.0/T2.0 | 1108/0.268 | 635/0.298 | - | - | reject | weak FIT/VAL / drop |
| 181 | sweep | quality_score<=159.4978@SL0.7/T1.5 | 2577/0.253 | 1916/0.197 | - | - | reject | weak FIT/VAL / drop |
| 182 | sweep | quality_score<=159.4978@SL1.0/T2.0 | 1710/0.265 | 1269/0.193 | - | - | reject | weak FIT/VAL / drop |
| 183 | sweep | signal_range_pct>=0.233071@SL0.7/T1.5 | 3025/0.241 | 2255/0.2 | - | - | reject | weak FIT/VAL / drop |
| 184 | sweep | signal_range_pct>=0.233071@SL1.0/T2.0 | 1974/0.256 | 1493/0.24 | - | - | reject | weak FIT/VAL / drop |
| 185 | sweep | signal_range_pct<=0.233071@SL0.7/T1.5 | 844/0.229 | 561/0.149 | - | - | reject | weak FIT/VAL / drop |
| 186 | sweep | signal_range_pct<=0.233071@SL1.0/T2.0 | 745/0.234 | 498/0.15 | - | - | reject | weak FIT/VAL / drop |
| 187 | sweep | signal_range_pct>=0.351809@SL0.7/T1.5 | 3093/0.231 | 2341/0.195 | - | - | reject | weak FIT/VAL / drop |
| 188 | sweep | signal_range_pct>=0.351809@SL1.0/T2.0 | 2041/0.255 | 1556/0.23 | - | - | reject | weak FIT/VAL / drop |
| 189 | sweep | signal_range_pct<=0.351809@SL0.7/T1.5 | 1429/0.258 | 914/0.164 | - | - | reject | weak FIT/VAL / drop |
| 190 | sweep | signal_range_pct<=0.351809@SL1.0/T2.0 | 1093/0.269 | 693/0.199 | - | - | reject | weak FIT/VAL / drop |
| 191 | sweep | signal_range_pct>=0.469595@SL0.7/T1.5 | 3122/0.242 | 2358/0.197 | - | - | reject | weak FIT/VAL / drop |
| 192 | sweep | signal_range_pct>=0.469595@SL1.0/T2.0 | 2127/0.263 | 1582/0.224 | - | - | reject | weak FIT/VAL / drop |
| 193 | sweep | signal_range_pct<=0.469595@SL0.7/T1.5 | 1676/0.277 | 1148/0.175 | - | - | reject | weak FIT/VAL / drop |
| 194 | sweep | signal_range_pct<=0.469595@SL1.0/T2.0 | 1221/0.275 | 813/0.205 | - | - | reject | weak FIT/VAL / drop |
| 195 | sweep | signal_range_pct>=0.644082@SL0.7/T1.5 | 2809/0.239 | 2000/0.203 | - | - | reject | weak FIT/VAL / drop |
| 196 | sweep | signal_range_pct>=0.644082@SL1.0/T2.0 | 2103/0.262 | 1572/0.237 | - | - | reject | weak FIT/VAL / drop |
| 197 | sweep | signal_range_pct<=0.644082@SL0.7/T1.5 | 1958/0.254 | 1436/0.166 | - | - | reject | weak FIT/VAL / drop |
| 198 | sweep | signal_range_pct<=0.644082@SL1.0/T2.0 | 1371/0.27 | 951/0.214 | - | - | reject | weak FIT/VAL / drop |
| 199 | sweep | signal_range_pct>=1.074667@SL0.7/T1.5 | 991/0.225 | 803/0.205 | - | - | reject | weak FIT/VAL / drop |
| 200 | sweep | signal_range_pct>=1.074667@SL1.0/T2.0 | 990/0.25 | 802/0.253 | - | - | reject | weak FIT/VAL / drop |
| 201 | sweep | signal_range_pct<=1.074667@SL0.7/T1.5 | 2463/0.249 | 1799/0.189 | - | - | reject | weak FIT/VAL / drop |
| 202 | sweep | signal_range_pct<=1.074667@SL1.0/T2.0 | 1633/0.26 | 1198/0.222 | - | - | reject | weak FIT/VAL / drop |
| 203 | sweep | upper_wick_pct>=0.0@SL0.7/T1.5 | 2980/0.244 | 2208/0.196 | - | - | reject | weak FIT/VAL / drop |
| 204 | sweep | upper_wick_pct>=0.0@SL1.0/T2.0 | 1963/0.25 | 1488/0.228 | - | - | reject | weak FIT/VAL / drop |
| 205 | sweep | upper_wick_pct<=0.0@SL0.7/T1.5 | 2003/0.251 | 1346/0.188 | - | - | reject | weak FIT/VAL / drop |
| 206 | sweep | upper_wick_pct<=0.0@SL1.0/T2.0 | 1496/0.258 | 1033/0.23 | - | - | reject | weak FIT/VAL / drop |
| 207 | sweep | upper_wick_pct>=0.004807@SL0.7/T1.5 | 2755/0.24 | 2078/0.182 | - | - | reject | weak FIT/VAL / drop |
| 208 | sweep | upper_wick_pct>=0.004807@SL1.0/T2.0 | 1894/0.273 | 1412/0.229 | - | - | reject | weak FIT/VAL / drop |
| 209 | sweep | upper_wick_pct<=0.004807@SL0.7/T1.5 | 2016/0.251 | 1360/0.186 | - | - | reject | weak FIT/VAL / drop |
| 210 | sweep | upper_wick_pct<=0.004807@SL1.0/T2.0 | 1505/0.258 | 1037/0.235 | - | - | reject | weak FIT/VAL / drop |
| 211 | sweep | upper_wick_pct>=0.034404@SL0.7/T1.5 | 2675/0.243 | 2012/0.184 | - | - | reject | weak FIT/VAL / drop |
| 212 | sweep | upper_wick_pct>=0.034404@SL1.0/T2.0 | 1911/0.274 | 1432/0.214 | - | - | reject | weak FIT/VAL / drop |
| 213 | sweep | upper_wick_pct<=0.034404@SL0.7/T1.5 | 2232/0.263 | 1564/0.187 | - | - | reject | weak FIT/VAL / drop |
| 214 | sweep | upper_wick_pct<=0.034404@SL1.0/T2.0 | 1573/0.245 | 1100/0.234 | - | - | reject | weak FIT/VAL / drop |
| 215 | sweep | upper_wick_pct>=0.069044@SL0.7/T1.5 | 2453/0.254 | 1775/0.185 | - | - | reject | weak FIT/VAL / drop |
| 216 | sweep | upper_wick_pct>=0.069044@SL1.0/T2.0 | 1824/0.265 | 1364/0.228 | - | - | reject | weak FIT/VAL / drop |
| 217 | sweep | upper_wick_pct<=0.069044@SL0.7/T1.5 | 2372/0.25 | 1674/0.186 | - | - | reject | weak FIT/VAL / drop |
| 218 | sweep | upper_wick_pct<=0.069044@SL1.0/T2.0 | 1622/0.247 | 1158/0.213 | - | - | reject | weak FIT/VAL / drop |
| 219 | sweep | upper_wick_pct>=0.141811@SL0.7/T1.5 | 1023/0.249 | 750/0.206 | - | - | reject | weak FIT/VAL / drop |
| 220 | sweep | upper_wick_pct>=0.141811@SL1.0/T2.0 | 1015/0.271 | 747/0.232 | - | - | reject | weak FIT/VAL / drop |
| 221 | sweep | upper_wick_pct<=0.141811@SL0.7/T1.5 | 2661/0.242 | 1905/0.193 | - | - | reject | weak FIT/VAL / drop |
| 222 | sweep | upper_wick_pct<=0.141811@SL1.0/T2.0 | 1756/0.252 | 1279/0.226 | - | - | reject | weak FIT/VAL / drop |
| 223 | sweep | lower_wick_pct>=0.0@SL0.7/T1.5 | 2980/0.244 | 2208/0.196 | - | - | reject | weak FIT/VAL / drop |
| 224 | sweep | lower_wick_pct>=0.0@SL1.0/T2.0 | 1963/0.25 | 1488/0.228 | - | - | reject | weak FIT/VAL / drop |
| 225 | sweep | lower_wick_pct<=0.0@SL0.7/T1.5 | 1946/0.255 | 1322/0.172 | - | - | reject | weak FIT/VAL / drop |
| 226 | sweep | lower_wick_pct<=0.0@SL1.0/T2.0 | 1517/0.258 | 1053/0.217 | - | - | reject | weak FIT/VAL / drop |
| 227 | sweep | lower_wick_pct>=0.012641@SL0.7/T1.5 | 2698/0.252 | 2008/0.19 | - | - | reject | weak FIT/VAL / drop |
| 228 | sweep | lower_wick_pct>=0.012641@SL1.0/T2.0 | 1874/0.268 | 1389/0.236 | - | - | reject | weak FIT/VAL / drop |
| 229 | sweep | lower_wick_pct<=0.012641@SL0.7/T1.5 | 2053/0.243 | 1428/0.166 | - | - | reject | weak FIT/VAL / drop |
| 230 | sweep | lower_wick_pct<=0.012641@SL1.0/T2.0 | 1550/0.235 | 1097/0.212 | - | - | reject | weak FIT/VAL / drop |
| 231 | sweep | lower_wick_pct>=0.055494@SL0.7/T1.5 | 2610/0.249 | 1929/0.205 | - | - | reject | weak FIT/VAL / drop |
| 232 | sweep | lower_wick_pct>=0.055494@SL1.0/T2.0 | 1831/0.28 | 1331/0.232 | - | - | reject | weak FIT/VAL / drop |
| 233 | sweep | lower_wick_pct<=0.055494@SL0.7/T1.5 | 2300/0.255 | 1624/0.161 | - | - | reject | weak FIT/VAL / drop |
| 234 | sweep | lower_wick_pct<=0.055494@SL1.0/T2.0 | 1613/0.265 | 1180/0.205 | - | - | reject | weak FIT/VAL / drop |
| 235 | sweep | lower_wick_pct>=0.108905@SL0.7/T1.5 | 2364/0.253 | 1680/0.213 | - | - | reject | weak FIT/VAL / drop |
| 236 | sweep | lower_wick_pct>=0.108905@SL1.0/T2.0 | 1785/0.261 | 1293/0.276 | - | - | reject | weak FIT/VAL / drop |
| 237 | sweep | lower_wick_pct<=0.108905@SL0.7/T1.5 | 2529/0.238 | 1800/0.164 | - | - | reject | weak FIT/VAL / drop |
| 238 | sweep | lower_wick_pct<=0.108905@SL1.0/T2.0 | 1726/0.253 | 1247/0.21 | - | - | reject | weak FIT/VAL / drop |
| 239 | sweep | lower_wick_pct>=0.226032@SL0.7/T1.5 | 1052/0.228 | 707/0.283 | - | - | reject | weak FIT/VAL / drop |
| 240 | sweep | lower_wick_pct>=0.226032@SL1.0/T2.0 | 1036/0.267 | 697/0.314 | - | - | reject | weak FIT/VAL / drop |
| 241 | sweep | lower_wick_pct<=0.226032@SL0.7/T1.5 | 2733/0.257 | 1989/0.179 | - | - | reject | weak FIT/VAL / drop |
| 242 | sweep | lower_wick_pct<=0.226032@SL1.0/T2.0 | 1803/0.254 | 1357/0.202 | - | - | reject | weak FIT/VAL / drop |
| 243 | sweep | wick_skew_pct>=-0.178605@SL0.7/T1.5 | 2795/0.245 | 2066/0.186 | - | - | reject | weak FIT/VAL / drop |
| 244 | sweep | wick_skew_pct>=-0.178605@SL1.0/T2.0 | 1855/0.246 | 1400/0.219 | - | - | reject | weak FIT/VAL / drop |
| 245 | sweep | wick_skew_pct<=-0.178605@SL0.7/T1.5 | 1074/0.246 | 701/0.253 | - | - | reject | weak FIT/VAL / drop |
| 246 | sweep | wick_skew_pct<=-0.178605@SL1.0/T2.0 | 1045/0.271 | 693/0.299 | - | - | reject | weak FIT/VAL / drop |
| 247 | sweep | wick_skew_pct>=-0.067809@SL0.7/T1.5 | 2669/0.243 | 1916/0.176 | - | - | reject | weak FIT/VAL / drop |
| 248 | sweep | wick_skew_pct>=-0.067809@SL1.0/T2.0 | 1801/0.265 | 1318/0.212 | - | - | reject | weak FIT/VAL / drop |
| 249 | sweep | wick_skew_pct<=-0.067809@SL0.7/T1.5 | 2194/0.241 | 1525/0.203 | - | - | reject | weak FIT/VAL / drop |
| 250 | sweep | wick_skew_pct<=-0.067809@SL1.0/T2.0 | 1630/0.266 | 1161/0.253 | - | - | reject | weak FIT/VAL / drop |
| 251 | sweep | wick_skew_pct>=-0.012579@SL0.7/T1.5 | 2479/0.238 | 1818/0.164 | - | - | reject | weak FIT/VAL / drop |
| 252 | sweep | wick_skew_pct>=-0.012579@SL1.0/T2.0 | 1746/0.27 | 1291/0.201 | - | - | reject | weak FIT/VAL / drop |
| 253 | sweep | wick_skew_pct<=-0.012579@SL0.7/T1.5 | 2426/0.252 | 1753/0.206 | - | - | reject | weak FIT/VAL / drop |
| 254 | sweep | wick_skew_pct<=-0.012579@SL1.0/T2.0 | 1725/0.254 | 1234/0.239 | - | - | reject | weak FIT/VAL / drop |
| 255 | sweep | wick_skew_pct>=0.017402@SL0.7/T1.5 | 2157/0.259 | 1541/0.183 | - | - | reject | weak FIT/VAL / drop |
| 256 | sweep | wick_skew_pct>=0.017402@SL1.0/T2.0 | 1642/0.267 | 1187/0.211 | - | - | reject | weak FIT/VAL / drop |
| 257 | sweep | wick_skew_pct<=0.017402@SL0.7/T1.5 | 2584/0.239 | 1905/0.187 | - | - | reject | weak FIT/VAL / drop |
| 258 | sweep | wick_skew_pct<=0.017402@SL1.0/T2.0 | 1756/0.254 | 1314/0.221 | - | - | reject | weak FIT/VAL / drop |
| 259 | sweep | wick_skew_pct>=0.087033@SL0.7/T1.5 | 1096/0.264 | 694/0.179 | - | - | reject | weak FIT/VAL / drop |
| 260 | sweep | wick_skew_pct>=0.087033@SL1.0/T2.0 | 1056/0.276 | 687/0.205 | - | - | reject | weak FIT/VAL / drop |
| 261 | sweep | wick_skew_pct<=0.087033@SL0.7/T1.5 | 2735/0.246 | 2014/0.196 | - | - | reject | weak FIT/VAL / drop |
| 262 | sweep | wick_skew_pct<=0.087033@SL1.0/T2.0 | 1845/0.254 | 1373/0.237 | - | - | reject | weak FIT/VAL / drop |
| 263 | sweep | x_bar_i>=7.0@SL0.7/T1.5 | 2727/0.24 | 2124/0.184 | - | - | reject | weak FIT/VAL / drop |
| 264 | sweep | x_bar_i>=7.0@SL1.0/T2.0 | 1792/0.256 | 1426/0.218 | - | - | reject | weak FIT/VAL / drop |
| 265 | sweep | x_bar_i<=7.0@SL0.7/T1.5 | 1440/0.279 | 351/0.249 | - | - | reject | weak FIT/VAL / drop |
| 266 | sweep | x_bar_i<=7.0@SL1.0/T2.0 | 1237/0.277 | 351/0.278 | - | - | reject | weak FIT/VAL / drop |
| 267 | sweep | x_bar_i>=16.0@SL0.7/T1.5 | 2150/0.219 | 2053/0.178 | - | - | reject | weak FIT/VAL / drop |
| 268 | sweep | x_bar_i>=16.0@SL1.0/T2.0 | 1504/0.242 | 1377/0.228 | - | - | reject | weak FIT/VAL / drop |
| 269 | sweep | x_bar_i<=16.0@SL0.7/T1.5 | 2531/0.258 | 811/0.244 | - | - | reject | weak FIT/VAL / drop |
| 270 | sweep | x_bar_i<=16.0@SL1.0/T2.0 | 1849/0.284 | 772/0.262 | - | - | reject | weak FIT/VAL / drop |
| 271 | sweep | x_bar_i>=26.0@SL0.7/T1.5 | 1408/0.207 | 1922/0.174 | - | - | reject | weak FIT/VAL / drop |
| 272 | sweep | x_bar_i>=26.0@SL1.0/T2.0 | 1042/0.201 | 1316/0.209 | - | - | reject | weak FIT/VAL / drop |
| 273 | sweep | x_bar_i<=26.0@SL0.7/T1.5 | 2877/0.258 | 1312/0.246 | - | - | reject | weak FIT/VAL / drop |
| 274 | sweep | x_bar_i<=26.0@SL1.0/T2.0 | 1953/0.282 | 1108/0.275 | - | - | reject | weak FIT/VAL / drop |
| 275 | sweep | x_bar_i>=37.0@SL0.7/T1.5 | 775/0.215 | 1697/0.165 | - | - | reject | weak FIT/VAL / drop |
| 276 | sweep | x_bar_i>=37.0@SL1.0/T2.0 | 590/0.221 | 1190/0.188 | - | - | reject | weak FIT/VAL / drop |
| 277 | sweep | x_bar_i<=37.0@SL0.7/T1.5 | 2936/0.253 | 1851/0.211 | - | - | reject | weak FIT/VAL / drop |
| 278 | sweep | x_bar_i<=37.0@SL1.0/T2.0 | 1964/0.256 | 1362/0.262 | - | - | reject | weak FIT/VAL / drop |
| 279 | sweep | x_bar_i>=52.0@SL0.7/T1.5 | 147/0.171 | 1120/0.156 | - | - | reject | weak FIT/VAL / drop |
| 280 | sweep | x_bar_i>=52.0@SL1.0/T2.0 | 147/0.184 | 879/0.164 | - | - | reject | weak FIT/VAL / drop |
| 281 | sweep | x_bar_i<=52.0@SL0.7/T1.5 | 2992/0.245 | 2221/0.199 | - | - | reject | weak FIT/VAL / drop |
| 282 | sweep | x_bar_i<=52.0@SL1.0/T2.0 | 1959/0.249 | 1490/0.244 | - | - | reject | weak FIT/VAL / drop |
| 283 | sweep | x_break_rank_day>=0.0@SL0.7/T1.5 | 2980/0.244 | 2208/0.196 | - | - | reject | weak FIT/VAL / drop |
| 284 | sweep | x_break_rank_day>=0.0@SL1.0/T2.0 | 1963/0.25 | 1488/0.228 | - | - | reject | weak FIT/VAL / drop |
| 285 | sweep | x_break_rank_day<=0.0@SL0.7/T1.5 | 2704/0.244 | 1946/0.185 | - | - | reject | weak FIT/VAL / drop |
| 286 | sweep | x_break_rank_day<=0.0@SL1.0/T2.0 | 1786/0.265 | 1306/0.226 | - | - | reject | weak FIT/VAL / drop |
| 287 | sweep | x_break_rank_day>=0.0@SL0.7/T1.5 | 2980/0.244 | 2208/0.196 | - | - | reject | weak FIT/VAL / drop |
| 288 | sweep | x_break_rank_day>=0.0@SL1.0/T2.0 | 1963/0.25 | 1488/0.228 | - | - | reject | weak FIT/VAL / drop |
| 289 | sweep | x_break_rank_day<=0.0@SL0.7/T1.5 | 2704/0.244 | 1946/0.185 | - | - | reject | weak FIT/VAL / drop |
| 290 | sweep | x_break_rank_day<=0.0@SL1.0/T2.0 | 1786/0.265 | 1306/0.226 | - | - | reject | weak FIT/VAL / drop |
| 291 | sweep | x_break_rank_day>=0.0@SL0.7/T1.5 | 2980/0.244 | 2208/0.196 | - | - | reject | weak FIT/VAL / drop |
| 292 | sweep | x_break_rank_day>=0.0@SL1.0/T2.0 | 1963/0.25 | 1488/0.228 | - | - | reject | weak FIT/VAL / drop |
| 293 | sweep | x_break_rank_day<=0.0@SL0.7/T1.5 | 2704/0.244 | 1946/0.185 | - | - | reject | weak FIT/VAL / drop |
| 294 | sweep | x_break_rank_day<=0.0@SL1.0/T2.0 | 1786/0.265 | 1306/0.226 | - | - | reject | weak FIT/VAL / drop |
| 295 | sweep | x_break_rank_day>=1.0@SL0.7/T1.5 | 2012/0.242 | 1449/0.18 | - | - | reject | weak FIT/VAL / drop |
| 296 | sweep | x_break_rank_day>=1.0@SL1.0/T2.0 | 1515/0.259 | 1126/0.196 | - | - | reject | weak FIT/VAL / drop |
| 297 | sweep | x_break_rank_day<=1.0@SL0.7/T1.5 | 2826/0.247 | 2091/0.19 | - | - | reject | weak FIT/VAL / drop |
| 298 | sweep | x_break_rank_day<=1.0@SL1.0/T2.0 | 1873/0.26 | 1411/0.225 | - | - | reject | weak FIT/VAL / drop |
| 299 | sweep | x_break_rank_day>=2.0@SL0.7/T1.5 | 1179/0.239 | 695/0.193 | - | - | reject | weak FIT/VAL / drop |
| 300 | sweep | x_break_rank_day>=2.0@SL1.0/T2.0 | 1016/0.304 | 636/0.237 | - | - | reject | weak FIT/VAL / drop |
| 301 | sweep | x_break_rank_day<=2.0@SL0.7/T1.5 | 2909/0.245 | 2155/0.195 | - | - | reject | weak FIT/VAL / drop |
| 302 | sweep | x_break_rank_day<=2.0@SL1.0/T2.0 | 1922/0.255 | 1451/0.216 | - | - | reject | weak FIT/VAL / drop |
| 303 | sweep | x_adx>=15.427722@SL0.7/T1.5 | 3012/0.243 | 2198/0.199 | - | - | reject | weak FIT/VAL / drop |
| 304 | sweep | x_adx>=15.427722@SL1.0/T2.0 | 1985/0.247 | 1475/0.222 | - | - | reject | weak FIT/VAL / drop |
| 305 | sweep | x_adx<=15.427722@SL0.7/T1.5 | 970/0.27 | 560/0.167 | - | - | reject | weak FIT/VAL / drop |
| 306 | sweep | x_adx<=15.427722@SL1.0/T2.0 | 874/0.284 | 517/0.204 | - | - | reject | weak FIT/VAL / drop |
| 307 | sweep | x_adx>=21.741343@SL0.7/T1.5 | 2894/0.249 | 2161/0.189 | - | - | reject | weak FIT/VAL / drop |
| 308 | sweep | x_adx>=21.741343@SL1.0/T2.0 | 1983/0.247 | 1471/0.24 | - | - | reject | weak FIT/VAL / drop |
| 309 | sweep | x_adx<=21.741343@SL0.7/T1.5 | 1753/0.252 | 1143/0.171 | - | - | reject | weak FIT/VAL / drop |
| 310 | sweep | x_adx<=21.741343@SL1.0/T2.0 | 1354/0.258 | 871/0.181 | - | - | reject | weak FIT/VAL / drop |
| 311 | sweep | x_adx>=27.964598@SL0.7/T1.5 | 2685/0.23 | 2059/0.2 | - | - | reject | weak FIT/VAL / drop |
| 312 | sweep | x_adx>=27.964598@SL1.0/T2.0 | 1908/0.236 | 1457/0.234 | - | - | reject | weak FIT/VAL / drop |
| 313 | sweep | x_adx<=27.964598@SL0.7/T1.5 | 2114/0.266 | 1465/0.175 | - | - | reject | weak FIT/VAL / drop |
| 314 | sweep | x_adx<=27.964598@SL1.0/T2.0 | 1526/0.28 | 1065/0.193 | - | - | reject | weak FIT/VAL / drop |
| 315 | sweep | x_adx>=35.730232@SL0.7/T1.5 | 2105/0.217 | 1666/0.209 | - | - | reject | weak FIT/VAL / drop |
| 316 | sweep | x_adx>=35.730232@SL1.0/T2.0 | 1638/0.252 | 1293/0.219 | - | - | reject | weak FIT/VAL / drop |
| 317 | sweep | x_adx<=35.730232@SL0.7/T1.5 | 2456/0.264 | 1724/0.18 | - | - | reject | weak FIT/VAL / drop |
| 318 | sweep | x_adx<=35.730232@SL1.0/T2.0 | 1653/0.27 | 1173/0.222 | - | - | reject | weak FIT/VAL / drop |
| 319 | sweep | x_adx>=48.47438@SL0.7/T1.5 | 909/0.219 | 704/0.224 | - | - | reject | weak FIT/VAL / drop |
| 320 | sweep | x_adx>=48.47438@SL1.0/T2.0 | 810/0.24 | 656/0.242 | - | - | reject | weak FIT/VAL / drop |
| 321 | sweep | x_adx<=48.47438@SL0.7/T1.5 | 2793/0.256 | 2079/0.184 | - | - | reject | weak FIT/VAL / drop |
| 322 | sweep | x_adx<=48.47438@SL1.0/T2.0 | 1829/0.262 | 1372/0.219 | - | - | reject | weak FIT/VAL / drop |
| 323 | sweep | x_adx_slope3>=-3.058059@SL0.7/T1.5 | 2931/0.232 | 2222/0.193 | - | - | reject | weak FIT/VAL / drop |
| 324 | sweep | x_adx_slope3>=-3.058059@SL1.0/T2.0 | 1999/0.249 | 1485/0.225 | - | - | reject | weak FIT/VAL / drop |
| 325 | sweep | x_adx_slope3<=-3.058059@SL0.7/T1.5 | 1009/0.267 | 609/0.171 | - | - | reject | weak FIT/VAL / drop |
| 326 | sweep | x_adx_slope3<=-3.058059@SL1.0/T2.0 | 897/0.278 | 564/0.21 | - | - | reject | weak FIT/VAL / drop |
| 327 | sweep | x_adx_slope3>=-0.766328@SL0.7/T1.5 | 2807/0.231 | 2151/0.186 | - | - | reject | weak FIT/VAL / drop |
| 328 | sweep | x_adx_slope3>=-0.766328@SL1.0/T2.0 | 1980/0.238 | 1463/0.222 | - | - | reject | weak FIT/VAL / drop |
| 329 | sweep | x_adx_slope3<=-0.766328@SL0.7/T1.5 | 1918/0.274 | 1257/0.183 | - | - | reject | weak FIT/VAL / drop |
| 330 | sweep | x_adx_slope3<=-0.766328@SL1.0/T2.0 | 1415/0.296 | 960/0.182 | - | - | reject | weak FIT/VAL / drop |
| 331 | sweep | x_adx_slope3>=1.028173@SL0.7/T1.5 | 2560/0.245 | 2015/0.185 | - | - | reject | weak FIT/VAL / drop |
| 332 | sweep | x_adx_slope3>=1.028173@SL1.0/T2.0 | 1843/0.242 | 1393/0.204 | - | - | reject | weak FIT/VAL / drop |
| 333 | sweep | x_adx_slope3<=1.028173@SL0.7/T1.5 | 2310/0.258 | 1533/0.171 | - | - | reject | weak FIT/VAL / drop |
| 334 | sweep | x_adx_slope3<=1.028173@SL1.0/T2.0 | 1580/0.279 | 1095/0.24 | - | - | reject | weak FIT/VAL / drop |
| 335 | sweep | x_adx_slope3>=2.96856@SL0.7/T1.5 | 2071/0.224 | 1653/0.181 | - | - | reject | weak FIT/VAL / drop |
| 336 | sweep | x_adx_slope3>=2.96856@SL1.0/T2.0 | 1614/0.263 | 1253/0.219 | - | - | reject | weak FIT/VAL / drop |
| 337 | sweep | x_adx_slope3<=2.96856@SL0.7/T1.5 | 2550/0.252 | 1818/0.184 | - | - | reject | weak FIT/VAL / drop |
| 338 | sweep | x_adx_slope3<=2.96856@SL1.0/T2.0 | 1720/0.263 | 1232/0.238 | - | - | reject | weak FIT/VAL / drop |
| 339 | sweep | x_adx_slope3>=5.645529@SL0.7/T1.5 | 889/0.224 | 843/0.173 | - | - | reject | weak FIT/VAL / drop |
| 340 | sweep | x_adx_slope3>=5.645529@SL1.0/T2.0 | 824/0.276 | 786/0.182 | - | - | reject | weak FIT/VAL / drop |
| 341 | sweep | x_adx_slope3<=5.645529@SL0.7/T1.5 | 2815/0.248 | 1977/0.199 | - | - | reject | weak FIT/VAL / drop |
| 342 | sweep | x_adx_slope3<=5.645529@SL1.0/T2.0 | 1881/0.254 | 1359/0.22 | - | - | reject | weak FIT/VAL / drop |
| 343 | sweep | x_atr_pct>=0.001887@SL0.7/T1.5 | 3031/0.242 | 2271/0.2 | - | - | reject | weak FIT/VAL / drop |
| 344 | sweep | x_atr_pct>=0.001887@SL1.0/T2.0 | 1990/0.253 | 1506/0.238 | - | - | reject | weak FIT/VAL / drop |
| 345 | sweep | x_atr_pct<=0.001887@SL0.7/T1.5 | 724/0.197 | 561/0.127 | - | - | reject | weak FIT/VAL / drop |
| 346 | sweep | x_atr_pct<=0.001887@SL1.0/T2.0 | 639/0.207 | 484/0.143 | - | - | reject | weak FIT/VAL / drop |
| 347 | sweep | x_atr_pct>=0.002572@SL0.7/T1.5 | 3120/0.246 | 2363/0.201 | - | - | reject | weak FIT/VAL / drop |
| 348 | sweep | x_atr_pct>=0.002572@SL1.0/T2.0 | 2054/0.266 | 1546/0.237 | - | - | reject | weak FIT/VAL / drop |
| 349 | sweep | x_atr_pct<=0.002572@SL0.7/T1.5 | 1263/0.225 | 919/0.147 | - | - | reject | weak FIT/VAL / drop |
| 350 | sweep | x_atr_pct<=0.002572@SL1.0/T2.0 | 980/0.254 | 686/0.164 | - | - | reject | weak FIT/VAL / drop |
| 351 | sweep | x_atr_pct>=0.00327@SL0.7/T1.5 | 3160/0.251 | 2334/0.195 | - | - | reject | weak FIT/VAL / drop |
| 352 | sweep | x_atr_pct>=0.00327@SL1.0/T2.0 | 2117/0.271 | 1614/0.23 | - | - | reject | weak FIT/VAL / drop |
| 353 | sweep | x_atr_pct<=0.00327@SL0.7/T1.5 | 1578/0.265 | 1111/0.148 | - | - | reject | weak FIT/VAL / drop |
| 354 | sweep | x_atr_pct<=0.00327@SL1.0/T2.0 | 1143/0.29 | 775/0.167 | - | - | reject | weak FIT/VAL / drop |
| 355 | sweep | x_atr_pct>=0.004278@SL0.7/T1.5 | 2636/0.245 | 1955/0.208 | - | - | reject | weak FIT/VAL / drop |
| 356 | sweep | x_atr_pct>=0.004278@SL1.0/T2.0 | 2034/0.269 | 1579/0.241 | - | - | reject | weak FIT/VAL / drop |
| 357 | sweep | x_atr_pct<=0.004278@SL0.7/T1.5 | 1906/0.262 | 1317/0.175 | - | - | reject | weak FIT/VAL / drop |
| 358 | sweep | x_atr_pct<=0.004278@SL1.0/T2.0 | 1310/0.283 | 886/0.199 | - | - | reject | weak FIT/VAL / drop |
| 359 | sweep | x_atr_pct>=0.007081@SL0.7/T1.5 | 962/0.208 | 749/0.205 | - | - | reject | weak FIT/VAL / drop |
| 360 | sweep | x_atr_pct>=0.007081@SL1.0/T2.0 | 945/0.25 | 749/0.234 | - | - | reject | weak FIT/VAL / drop |
| 361 | sweep | x_atr_pct<=0.007081@SL0.7/T1.5 | 2444/0.249 | 1768/0.198 | - | - | reject | weak FIT/VAL / drop |
| 362 | sweep | x_atr_pct<=0.007081@SL1.0/T2.0 | 1593/0.249 | 1178/0.222 | - | - | reject | weak FIT/VAL / drop |
| 363 | sweep | x_bar_idx>=15.0@SL0.7/T1.5 | 2567/0.252 | 1892/0.197 | - | - | reject | weak FIT/VAL / drop |
| 364 | sweep | x_bar_idx>=15.0@SL1.0/T2.0 | 1736/0.251 | 1287/0.244 | - | - | reject | weak FIT/VAL / drop |
| 365 | sweep | x_bar_idx<=15.0@SL0.7/T1.5 | 877/0.194 | 670/0.163 | - | - | reject | weak FIT/VAL / drop |
| 366 | sweep | x_bar_idx<=15.0@SL1.0/T2.0 | 764/0.213 | 585/0.161 | - | - | reject | weak FIT/VAL / drop |
| 367 | sweep | x_bar_idx>=28.0@SL0.7/T1.5 | 2083/0.245 | 1446/0.199 | - | - | reject | weak FIT/VAL / drop |
| 368 | sweep | x_bar_idx>=28.0@SL1.0/T2.0 | 1455/0.269 | 1003/0.26 | - | - | reject | weak FIT/VAL / drop |
| 369 | sweep | x_bar_idx<=28.0@SL0.7/T1.5 | 1716/0.232 | 1266/0.182 | - | - | reject | weak FIT/VAL / drop |
| 370 | sweep | x_bar_idx<=28.0@SL1.0/T2.0 | 1303/0.234 | 931/0.186 | - | - | reject | weak FIT/VAL / drop |
| 371 | sweep | x_bar_idx>=38.0@SL0.7/T1.5 | 1617/0.268 | 1118/0.195 | - | - | reject | weak FIT/VAL / drop |
| 372 | sweep | x_bar_idx>=38.0@SL1.0/T2.0 | 1198/0.29 | 799/0.256 | - | - | reject | weak FIT/VAL / drop |
| 373 | sweep | x_bar_idx<=38.0@SL0.7/T1.5 | 2144/0.23 | 1581/0.184 | - | - | reject | weak FIT/VAL / drop |
| 374 | sweep | x_bar_idx<=38.0@SL1.0/T2.0 | 1508/0.246 | 1117/0.205 | - | - | reject | weak FIT/VAL / drop |
| 375 | sweep | x_bar_idx>=48.0@SL0.7/T1.5 | 1255/0.293 | 862/0.241 | - | - | reject | weak FIT/VAL / drop |
| 376 | sweep | x_bar_idx>=48.0@SL1.0/T2.0 | 962/0.303 | 653/0.235 | - | - | reject | weak FIT/VAL / drop |
| 377 | sweep | x_bar_idx<=48.0@SL0.7/T1.5 | 2521/0.24 | 1851/0.188 | - | - | reject | weak FIT/VAL / drop |
| 378 | sweep | x_bar_idx<=48.0@SL1.0/T2.0 | 1696/0.249 | 1256/0.218 | - | - | reject | weak FIT/VAL / drop |
| 379 | sweep | x_bar_idx>=58.0@SL0.7/T1.5 | 819/0.306 | 533/0.292 | - | - | reject | weak FIT/VAL / drop |
| 380 | sweep | x_bar_idx>=58.0@SL1.0/T2.0 | 743/0.333 | 491/0.349 | - | - | reject | weak FIT/VAL / drop |
| 381 | sweep | x_bar_idx<=58.0@SL0.7/T1.5 | 2837/0.241 | 2102/0.191 | - | - | reject | weak FIT/VAL / drop |
| 382 | sweep | x_bar_idx<=58.0@SL1.0/T2.0 | 1874/0.252 | 1416/0.218 | - | - | reject | weak FIT/VAL / drop |
| 383 | sweep | x_bb_pos>=0.680839@SL0.7/T1.5 | 2953/0.249 | 2219/0.2 | - | - | reject | weak FIT/VAL / drop |
| 384 | sweep | x_bb_pos>=0.680839@SL1.0/T2.0 | 2001/0.248 | 1458/0.227 | - | - | reject | weak FIT/VAL / drop |
| 385 | sweep | x_bb_pos<=0.680839@SL0.7/T1.5 | 1089/0.184 | 550/0.159 | - | - | reject | weak FIT/VAL / drop |
| 386 | sweep | x_bb_pos<=0.680839@SL1.0/T2.0 | 946/0.19 | 515/0.193 | - | - | reject | weak FIT/VAL / drop |
| 387 | sweep | x_bb_pos>=0.882815@SL0.7/T1.5 | 2798/0.245 | 2133/0.198 | - | - | reject | weak FIT/VAL / drop |
| 388 | sweep | x_bb_pos>=0.882815@SL1.0/T2.0 | 1969/0.251 | 1449/0.235 | - | - | reject | weak FIT/VAL / drop |
| 389 | sweep | x_bb_pos<=0.882815@SL0.7/T1.5 | 1952/0.244 | 1244/0.181 | - | - | reject | weak FIT/VAL / drop |
| 390 | sweep | x_bb_pos<=0.882815@SL1.0/T2.0 | 1411/0.266 | 958/0.194 | - | - | reject | weak FIT/VAL / drop |
| 391 | sweep | x_bb_pos>=0.976544@SL0.7/T1.5 | 2490/0.25 | 1902/0.188 | - | - | reject | weak FIT/VAL / drop |
| 392 | sweep | x_bb_pos>=0.976544@SL1.0/T2.0 | 1773/0.255 | 1367/0.229 | - | - | reject | weak FIT/VAL / drop |
| 393 | sweep | x_bb_pos<=0.976544@SL0.7/T1.5 | 2366/0.25 | 1639/0.172 | - | - | reject | weak FIT/VAL / drop |
| 394 | sweep | x_bb_pos<=0.976544@SL1.0/T2.0 | 1659/0.267 | 1170/0.205 | - | - | reject | weak FIT/VAL / drop |
| 395 | sweep | x_bb_pos>=1.060242@SL0.7/T1.5 | 2041/0.255 | 1539/0.182 | - | - | reject | weak FIT/VAL / drop |
| 396 | sweep | x_bb_pos>=1.060242@SL1.0/T2.0 | 1564/0.266 | 1151/0.236 | - | - | reject | weak FIT/VAL / drop |
| 397 | sweep | x_bb_pos<=1.060242@SL0.7/T1.5 | 2692/0.241 | 1890/0.179 | - | - | reject | weak FIT/VAL / drop |
| 398 | sweep | x_bb_pos<=1.060242@SL1.0/T2.0 | 1817/0.275 | 1317/0.212 | - | - | reject | weak FIT/VAL / drop |
| 399 | sweep | x_bb_pos>=1.174088@SL0.7/T1.5 | 1119/0.269 | 773/0.189 | - | - | reject | weak FIT/VAL / drop |
| 400 | sweep | x_bb_pos>=1.174088@SL1.0/T2.0 | 1001/0.293 | 714/0.232 | - | - | reject | weak FIT/VAL / drop |
| 401 | sweep | x_bb_pos<=1.174088@SL0.7/T1.5 | 2949/0.248 | 2100/0.191 | - | - | reject | weak FIT/VAL / drop |
| 402 | sweep | x_bb_pos<=1.174088@SL1.0/T2.0 | 1942/0.251 | 1423/0.233 | - | - | reject | weak FIT/VAL / drop |
| 403 | sweep | x_bb_width_pct>=0.006553@SL0.7/T1.5 | 3050/0.245 | 2282/0.198 | - | - | reject | weak FIT/VAL / drop |
| 404 | sweep | x_bb_width_pct>=0.006553@SL1.0/T2.0 | 1993/0.255 | 1500/0.236 | - | - | reject | weak FIT/VAL / drop |
| 405 | sweep | x_bb_width_pct<=0.006553@SL0.7/T1.5 | 799/0.238 | 540/0.149 | - | - | reject | weak FIT/VAL / drop |
| 406 | sweep | x_bb_width_pct<=0.006553@SL1.0/T2.0 | 716/0.244 | 479/0.159 | - | - | reject | weak FIT/VAL / drop |
| 407 | sweep | x_bb_width_pct>=0.010943@SL0.7/T1.5 | 3107/0.251 | 2312/0.198 | - | - | reject | weak FIT/VAL / drop |
| 408 | sweep | x_bb_width_pct>=0.010943@SL1.0/T2.0 | 2058/0.264 | 1533/0.234 | - | - | reject | weak FIT/VAL / drop |
| 409 | sweep | x_bb_width_pct<=0.010943@SL0.7/T1.5 | 1416/0.255 | 969/0.161 | - | - | reject | weak FIT/VAL / drop |
| 410 | sweep | x_bb_width_pct<=0.010943@SL1.0/T2.0 | 1067/0.253 | 722/0.168 | - | - | reject | weak FIT/VAL / drop |
| 411 | sweep | x_bb_width_pct>=0.016102@SL0.7/T1.5 | 3115/0.247 | 2270/0.199 | - | - | reject | weak FIT/VAL / drop |
| 412 | sweep | x_bb_width_pct>=0.016102@SL1.0/T2.0 | 2118/0.267 | 1588/0.224 | - | - | reject | weak FIT/VAL / drop |
| 413 | sweep | x_bb_width_pct<=0.016102@SL0.7/T1.5 | 1729/0.242 | 1241/0.161 | - | - | reject | weak FIT/VAL / drop |
| 414 | sweep | x_bb_width_pct<=0.016102@SL1.0/T2.0 | 1232/0.273 | 852/0.168 | - | - | reject | weak FIT/VAL / drop |
| 415 | sweep | x_bb_width_pct>=0.024068@SL0.7/T1.5 | 2566/0.251 | 1831/0.2 | - | - | reject | weak FIT/VAL / drop |
| 416 | sweep | x_bb_width_pct>=0.024068@SL1.0/T2.0 | 1957/0.299 | 1491/0.243 | - | - | reject | weak FIT/VAL / drop |
| 417 | sweep | x_bb_width_pct<=0.024068@SL0.7/T1.5 | 2061/0.253 | 1459/0.168 | - | - | reject | weak FIT/VAL / drop |
| 418 | sweep | x_bb_width_pct<=0.024068@SL1.0/T2.0 | 1392/0.266 | 1011/0.197 | - | - | reject | weak FIT/VAL / drop |
| 419 | sweep | x_bb_width_pct>=0.044555@SL0.7/T1.5 | 1002/0.197 | 689/0.238 | - | - | reject | weak FIT/VAL / drop |
| 420 | sweep | x_bb_width_pct>=0.044555@SL1.0/T2.0 | 965/0.252 | 688/0.274 | - | - | reject | weak FIT/VAL / drop |
| 421 | sweep | x_bb_width_pct<=0.044555@SL0.7/T1.5 | 2511/0.269 | 1864/0.193 | - | - | reject | weak FIT/VAL / drop |
| 422 | sweep | x_bb_width_pct<=0.044555@SL1.0/T2.0 | 1651/0.262 | 1261/0.25 | - | - | reject | weak FIT/VAL / drop |
| 423 | sweep | x_cci20>=14.914568@SL0.7/T1.5 | 2933/0.25 | 2231/0.2 | - | - | reject | weak FIT/VAL / drop |
| 424 | sweep | x_cci20>=14.914568@SL1.0/T2.0 | 1985/0.25 | 1470/0.23 | - | - | reject | weak FIT/VAL / drop |
| 425 | sweep | x_cci20<=14.914568@SL0.7/T1.5 | 1074/0.184 | 555/0.153 | - | - | reject | weak FIT/VAL / drop |
| 426 | sweep | x_cci20<=14.914568@SL1.0/T2.0 | 949/0.187 | 513/0.193 | - | - | reject | weak FIT/VAL / drop |
| 427 | sweep | x_cci20>=66.027163@SL0.7/T1.5 | 2820/0.253 | 2148/0.19 | - | - | reject | weak FIT/VAL / drop |
| 428 | sweep | x_cci20>=66.027163@SL1.0/T2.0 | 1996/0.263 | 1456/0.232 | - | - | reject | weak FIT/VAL / drop |
| 429 | sweep | x_cci20<=66.027163@SL0.7/T1.5 | 1846/0.229 | 1221/0.177 | - | - | reject | weak FIT/VAL / drop |
| 430 | sweep | x_cci20<=66.027163@SL1.0/T2.0 | 1369/0.235 | 932/0.194 | - | - | reject | weak FIT/VAL / drop |
| 431 | sweep | x_cci20>=100.45518@SL0.7/T1.5 | 2559/0.241 | 1962/0.171 | - | - | reject | weak FIT/VAL / drop |
| 432 | sweep | x_cci20>=100.45518@SL1.0/T2.0 | 1854/0.261 | 1385/0.216 | - | - | reject | weak FIT/VAL / drop |
| 433 | sweep | x_cci20<=100.45518@SL0.7/T1.5 | 2228/0.248 | 1558/0.189 | - | - | reject | weak FIT/VAL / drop |
| 434 | sweep | x_cci20<=100.45518@SL1.0/T2.0 | 1601/0.253 | 1099/0.242 | - | - | reject | weak FIT/VAL / drop |
| 435 | sweep | x_cci20>=136.810781@SL0.7/T1.5 | 2059/0.245 | 1648/0.188 | - | - | reject | weak FIT/VAL / drop |
| 436 | sweep | x_cci20>=136.810781@SL1.0/T2.0 | 1622/0.293 | 1239/0.233 | - | - | reject | weak FIT/VAL / drop |
| 437 | sweep | x_cci20<=136.810781@SL0.7/T1.5 | 2605/0.24 | 1776/0.184 | - | - | reject | weak FIT/VAL / drop |
| 438 | sweep | x_cci20<=136.810781@SL1.0/T2.0 | 1758/0.253 | 1265/0.207 | - | - | reject | weak FIT/VAL / drop |
| 439 | sweep | x_cci20>=200.177307@SL0.7/T1.5 | 1048/0.267 | 801/0.172 | - | - | reject | weak FIT/VAL / drop |
| 440 | sweep | x_cci20>=200.177307@SL1.0/T2.0 | 959/0.307 | 758/0.187 | - | - | reject | weak FIT/VAL / drop |
| 441 | sweep | x_cci20<=200.177307@SL0.7/T1.5 | 2833/0.236 | 2048/0.193 | - | - | reject | weak FIT/VAL / drop |
| 442 | sweep | x_cci20<=200.177307@SL1.0/T2.0 | 1907/0.255 | 1402/0.227 | - | - | reject | weak FIT/VAL / drop |
| 443 | sweep | x_day_ret_pct>=0.116244@SL0.7/T1.5 | 2932/0.233 | 2217/0.199 | - | - | reject | weak FIT/VAL / drop |
| 444 | sweep | x_day_ret_pct>=0.116244@SL1.0/T2.0 | 1981/0.242 | 1495/0.236 | - | - | reject | weak FIT/VAL / drop |
| 445 | sweep | x_day_ret_pct<=0.116244@SL0.7/T1.5 | 878/0.317 | 348/0.151 | - | - | reject | weak FIT/VAL / drop |
| 446 | sweep | x_day_ret_pct<=0.116244@SL1.0/T2.0 | 724/0.329 | 307/0.188 | - | - | reject | weak FIT/VAL / drop |
| 447 | sweep | x_day_ret_pct>=0.905433@SL0.7/T1.5 | 2875/0.228 | 2262/0.196 | - | - | reject | weak FIT/VAL / drop |
| 448 | sweep | x_day_ret_pct>=0.905433@SL1.0/T2.0 | 2005/0.233 | 1516/0.228 | - | - | reject | weak FIT/VAL / drop |
| 449 | sweep | x_day_ret_pct<=0.905433@SL0.7/T1.5 | 1680/0.287 | 1138/0.161 | - | - | reject | weak FIT/VAL / drop |
| 450 | sweep | x_day_ret_pct<=0.905433@SL1.0/T2.0 | 1257/0.293 | 850/0.18 | - | - | reject | weak FIT/VAL / drop |
| 451 | sweep | x_day_ret_pct>=1.630294@SL0.7/T1.5 | 2783/0.209 | 2160/0.217 | - | - | reject | weak FIT/VAL / drop |
| 452 | sweep | x_day_ret_pct>=1.630294@SL1.0/T2.0 | 2014/0.222 | 1553/0.247 | - | - | reject | weak FIT/VAL / drop |
| 453 | sweep | x_day_ret_pct<=1.630294@SL0.7/T1.5 | 2008/0.288 | 1414/0.16 | - | - | reject | weak FIT/VAL / drop |
| 454 | sweep | x_day_ret_pct<=1.630294@SL1.0/T2.0 | 1424/0.313 | 1020/0.187 | - | - | reject | weak FIT/VAL / drop |
| 455 | sweep | x_day_ret_pct>=2.623723@SL0.7/T1.5 | 2342/0.211 | 1710/0.219 | - | - | reject | weak FIT/VAL / drop |
| 456 | sweep | x_day_ret_pct>=2.623723@SL1.0/T2.0 | 1812/0.243 | 1429/0.254 | - | - | reject | weak FIT/VAL / drop |
| 457 | sweep | x_day_ret_pct<=2.623723@SL0.7/T1.5 | 2253/0.277 | 1654/0.174 | - | - | reject | weak FIT/VAL / drop |
| 458 | sweep | x_day_ret_pct<=2.623723@SL1.0/T2.0 | 1554/0.283 | 1102/0.219 | - | - | reject | weak FIT/VAL / drop |
| 459 | sweep | x_day_ret_pct>=4.814948@SL0.7/T1.5 | 1043/0.205 | 683/0.242 | - | - | reject | weak FIT/VAL / drop |
| 460 | sweep | x_day_ret_pct>=4.814948@SL1.0/T2.0 | 996/0.247 | 683/0.282 | - | - | reject | weak FIT/VAL / drop |
| 461 | sweep | x_day_ret_pct<=4.814948@SL0.7/T1.5 | 2603/0.248 | 1912/0.191 | - | - | reject | weak FIT/VAL / drop |
| 462 | sweep | x_day_ret_pct<=4.814948@SL1.0/T2.0 | 1730/0.265 | 1257/0.197 | - | - | reject | weak FIT/VAL / drop |
| 463 | sweep | x_dayrange_atr>=4.974133@SL0.7/T1.5 | 2703/0.236 | 1967/0.206 | - | - | reject | weak FIT/VAL / drop |
| 464 | sweep | x_dayrange_atr>=4.974133@SL1.0/T2.0 | 1858/0.256 | 1358/0.257 | - | - | reject | weak FIT/VAL / drop |
| 465 | sweep | x_dayrange_atr<=4.974133@SL0.7/T1.5 | 849/0.21 | 764/0.157 | - | - | reject | weak FIT/VAL / drop |
| 466 | sweep | x_dayrange_atr<=4.974133@SL1.0/T2.0 | 782/0.228 | 697/0.164 | - | - | reject | weak FIT/VAL / drop |
| 467 | sweep | x_dayrange_atr>=7.2487@SL0.7/T1.5 | 2366/0.247 | 1628/0.212 | - | - | reject | weak FIT/VAL / drop |
| 468 | sweep | x_dayrange_atr>=7.2487@SL1.0/T2.0 | 1667/0.244 | 1178/0.247 | - | - | reject | weak FIT/VAL / drop |
| 469 | sweep | x_dayrange_atr<=7.2487@SL0.7/T1.5 | 2059/0.24 | 1675/0.17 | - | - | reject | weak FIT/VAL / drop |
| 470 | sweep | x_dayrange_atr<=7.2487@SL1.0/T2.0 | 1583/0.246 | 1225/0.191 | - | - | reject | weak FIT/VAL / drop |
| 471 | sweep | x_dayrange_atr>=9.092502@SL0.7/T1.5 | 2038/0.247 | 1365/0.187 | - | - | reject | weak FIT/VAL / drop |
| 472 | sweep | x_dayrange_atr>=9.092502@SL1.0/T2.0 | 1458/0.26 | 1016/0.239 | - | - | reject | weak FIT/VAL / drop |
| 473 | sweep | x_dayrange_atr<=9.092502@SL0.7/T1.5 | 2588/0.255 | 1959/0.186 | - | - | reject | weak FIT/VAL / drop |
| 474 | sweep | x_dayrange_atr<=9.092502@SL1.0/T2.0 | 1831/0.276 | 1377/0.197 | - | - | reject | weak FIT/VAL / drop |
| 475 | sweep | x_dayrange_atr>=11.362963@SL0.7/T1.5 | 1675/0.226 | 1056/0.209 | - | - | reject | weak FIT/VAL / drop |
| 476 | sweep | x_dayrange_atr>=11.362963@SL1.0/T2.0 | 1261/0.264 | 814/0.27 | - | - | reject | weak FIT/VAL / drop |
| 477 | sweep | x_dayrange_atr<=11.362963@SL0.7/T1.5 | 2821/0.251 | 2091/0.187 | - | - | reject | weak FIT/VAL / drop |
| 478 | sweep | x_dayrange_atr<=11.362963@SL1.0/T2.0 | 1911/0.252 | 1406/0.2 | - | - | reject | weak FIT/VAL / drop |
| 479 | sweep | x_dayrange_atr>=15.680512@SL0.7/T1.5 | 966/0.24 | 504/0.2 | - | - | reject | weak FIT/VAL / drop |
| 480 | sweep | x_dayrange_atr>=15.680512@SL1.0/T2.0 | 822/0.289 | 469/0.264 | - | - | reject | weak FIT/VAL / drop |
| 481 | sweep | x_dayrange_atr<=15.680512@SL0.7/T1.5 | 2928/0.246 | 2207/0.196 | - | - | reject | weak FIT/VAL / drop |
| 482 | sweep | x_dayrange_atr<=15.680512@SL1.0/T2.0 | 1964/0.254 | 1472/0.222 | - | - | reject | weak FIT/VAL / drop |
| 483 | sweep | x_dist_dayhigh_atr>=0.0@SL0.7/T1.5 | 2980/0.244 | 2208/0.196 | - | - | reject | weak FIT/VAL / drop |
| 484 | sweep | x_dist_dayhigh_atr>=0.0@SL1.0/T2.0 | 1963/0.25 | 1488/0.228 | - | - | reject | weak FIT/VAL / drop |
| 485 | sweep | x_dist_dayhigh_atr<=0.0@SL0.7/T1.5 | 1044/0.214 | 697/0.186 | - | - | reject | weak FIT/VAL / drop |
| 486 | sweep | x_dist_dayhigh_atr<=0.0@SL1.0/T2.0 | 912/0.21 | 681/0.238 | - | - | reject | weak FIT/VAL / drop |
| 487 | sweep | x_dist_dayhigh_atr>=0.192123@SL0.7/T1.5 | 2521/0.258 | 1764/0.186 | - | - | reject | weak FIT/VAL / drop |
| 488 | sweep | x_dist_dayhigh_atr>=0.192123@SL1.0/T2.0 | 1737/0.27 | 1238/0.24 | - | - | reject | weak FIT/VAL / drop |
| 489 | sweep | x_dist_dayhigh_atr<=0.192123@SL0.7/T1.5 | 1981/0.202 | 1623/0.192 | - | - | reject | weak FIT/VAL / drop |
| 490 | sweep | x_dist_dayhigh_atr<=0.192123@SL1.0/T2.0 | 1508/0.237 | 1211/0.224 | - | - | reject | weak FIT/VAL / drop |
| 491 | sweep | x_dist_dayhigh_atr>=0.44481@SL0.7/T1.5 | 2056/0.26 | 1416/0.199 | - | - | reject | weak FIT/VAL / drop |
| 492 | sweep | x_dist_dayhigh_atr>=0.44481@SL1.0/T2.0 | 1463/0.275 | 1019/0.237 | - | - | reject | weak FIT/VAL / drop |
| 493 | sweep | x_dist_dayhigh_atr<=0.44481@SL0.7/T1.5 | 2538/0.238 | 2023/0.178 | - | - | reject | weak FIT/VAL / drop |
| 494 | sweep | x_dist_dayhigh_atr<=0.44481@SL1.0/T2.0 | 1839/0.261 | 1399/0.225 | - | - | reject | weak FIT/VAL / drop |
| 495 | sweep | x_dist_dayhigh_atr>=1.65932@SL0.7/T1.5 | 1744/0.259 | 1044/0.168 | - | - | reject | weak FIT/VAL / drop |
| 496 | sweep | x_dist_dayhigh_atr>=1.65932@SL1.0/T2.0 | 1284/0.266 | 797/0.214 | - | - | reject | weak FIT/VAL / drop |
| 497 | sweep | x_dist_dayhigh_atr<=1.65932@SL0.7/T1.5 | 2850/0.246 | 2146/0.196 | - | - | reject | weak FIT/VAL / drop |
| 498 | sweep | x_dist_dayhigh_atr<=1.65932@SL1.0/T2.0 | 1983/0.244 | 1448/0.239 | - | - | reject | weak FIT/VAL / drop |
| 499 | sweep | x_dist_dayhigh_atr>=3.923384@SL0.7/T1.5 | 1064/0.258 | 534/0.174 | - | - | reject | weak FIT/VAL / drop |
| 500 | sweep | x_dist_dayhigh_atr>=3.923384@SL1.0/T2.0 | 900/0.265 | 489/0.2 | - | - | reject | weak FIT/VAL / drop |
| 501 | sweep | x_dist_dayhigh_atr<=3.923384@SL0.7/T1.5 | 2951/0.246 | 2225/0.202 | - | - | reject | weak FIT/VAL / drop |
| 502 | sweep | x_dist_dayhigh_atr<=3.923384@SL1.0/T2.0 | 1968/0.257 | 1489/0.227 | - | - | reject | weak FIT/VAL / drop |
| 503 | sweep | x_ema200_dist_atr>=-1.643545@SL0.7/T1.5 | 2861/0.237 | 2202/0.199 | - | - | reject | weak FIT/VAL / drop |
| 504 | sweep | x_ema200_dist_atr>=-1.643545@SL1.0/T2.0 | 1959/0.246 | 1507/0.229 | - | - | reject | weak FIT/VAL / drop |
| 505 | sweep | x_ema200_dist_atr<=-1.643545@SL0.7/T1.5 | 895/0.305 | 354/0.118 | - | - | reject | weak FIT/VAL / drop |
| 506 | sweep | x_ema200_dist_atr<=-1.643545@SL1.0/T2.0 | 725/0.307 | 313/0.174 | - | - | reject | weak FIT/VAL / drop |
| 507 | sweep | x_ema200_dist_atr>=2.578303@SL0.7/T1.5 | 2624/0.217 | 2126/0.2 | - | - | reject | weak FIT/VAL / drop |
| 508 | sweep | x_ema200_dist_atr>=2.578303@SL1.0/T2.0 | 1864/0.224 | 1490/0.235 | - | - | reject | weak FIT/VAL / drop |
| 509 | sweep | x_ema200_dist_atr<=2.578303@SL0.7/T1.5 | 1718/0.29 | 1201/0.146 | - | - | reject | weak FIT/VAL / drop |
| 510 | sweep | x_ema200_dist_atr<=2.578303@SL1.0/T2.0 | 1283/0.3 | 934/0.182 | - | - | reject | weak FIT/VAL / drop |
| 511 | sweep | x_ema200_dist_atr>=4.983787@SL0.7/T1.5 | 2278/0.199 | 1778/0.231 | - | - | reject | weak FIT/VAL / drop |
| 512 | sweep | x_ema200_dist_atr>=4.983787@SL1.0/T2.0 | 1672/0.218 | 1288/0.254 | - | - | reject | weak FIT/VAL / drop |
| 513 | sweep | x_ema200_dist_atr<=4.983787@SL0.7/T1.5 | 2301/0.263 | 1679/0.16 | - | - | reject | weak FIT/VAL / drop |
| 514 | sweep | x_ema200_dist_atr<=4.983787@SL1.0/T2.0 | 1623/0.268 | 1176/0.194 | - | - | reject | weak FIT/VAL / drop |
| 515 | sweep | x_ema200_dist_atr>=7.38697@SL0.7/T1.5 | 1633/0.213 | 1306/0.193 | - | - | reject | weak FIT/VAL / drop |
| 516 | sweep | x_ema200_dist_atr>=7.38697@SL1.0/T2.0 | 1224/0.231 | 1042/0.227 | - | - | reject | weak FIT/VAL / drop |
| 517 | sweep | x_ema200_dist_atr<=7.38697@SL0.7/T1.5 | 2663/0.257 | 1960/0.19 | - | - | reject | weak FIT/VAL / drop |
| 518 | sweep | x_ema200_dist_atr<=7.38697@SL1.0/T2.0 | 1795/0.276 | 1345/0.202 | - | - | reject | weak FIT/VAL / drop |
| 519 | sweep | x_ema200_dist_atr>=11.401821@SL0.7/T1.5 | 821/0.218 | 459/0.178 | - | - | reject | weak FIT/VAL / drop |
| 520 | sweep | x_ema200_dist_atr>=11.401821@SL1.0/T2.0 | 685/0.234 | 433/0.242 | - | - | reject | weak FIT/VAL / drop |
| 521 | sweep | x_ema200_dist_atr<=11.401821@SL0.7/T1.5 | 2934/0.24 | 2152/0.199 | - | - | reject | weak FIT/VAL / drop |
| 522 | sweep | x_ema200_dist_atr<=11.401821@SL1.0/T2.0 | 1939/0.254 | 1430/0.223 | - | - | reject | weak FIT/VAL / drop |
| 523 | sweep | x_ema20_dist_atr>=0.752661@SL0.7/T1.5 | 2943/0.25 | 2209/0.202 | - | - | reject | weak FIT/VAL / drop |
| 524 | sweep | x_ema20_dist_atr>=0.752661@SL1.0/T2.0 | 1987/0.258 | 1476/0.237 | - | - | reject | weak FIT/VAL / drop |
| 525 | sweep | x_ema20_dist_atr<=0.752661@SL0.7/T1.5 | 1042/0.202 | 556/0.15 | - | - | reject | weak FIT/VAL / drop |
| 526 | sweep | x_ema20_dist_atr<=0.752661@SL1.0/T2.0 | 907/0.208 | 514/0.161 | - | - | reject | weak FIT/VAL / drop |
| 527 | sweep | x_ema20_dist_atr>=1.577299@SL0.7/T1.5 | 2851/0.241 | 2166/0.198 | - | - | reject | weak FIT/VAL / drop |
| 528 | sweep | x_ema20_dist_atr>=1.577299@SL1.0/T2.0 | 1996/0.25 | 1485/0.242 | - | - | reject | weak FIT/VAL / drop |
| 529 | sweep | x_ema20_dist_atr<=1.577299@SL0.7/T1.5 | 1874/0.234 | 1189/0.151 | - | - | reject | weak FIT/VAL / drop |
| 530 | sweep | x_ema20_dist_atr<=1.577299@SL1.0/T2.0 | 1380/0.237 | 903/0.174 | - | - | reject | weak FIT/VAL / drop |
| 531 | sweep | x_ema20_dist_atr>=2.158726@SL0.7/T1.5 | 2620/0.236 | 2016/0.18 | - | - | reject | weak FIT/VAL / drop |
| 532 | sweep | x_ema20_dist_atr>=2.158726@SL1.0/T2.0 | 1902/0.268 | 1400/0.227 | - | - | reject | weak FIT/VAL / drop |
| 533 | sweep | x_ema20_dist_atr<=2.158726@SL0.7/T1.5 | 2215/0.259 | 1528/0.193 | - | - | reject | weak FIT/VAL / drop |
| 534 | sweep | x_ema20_dist_atr<=2.158726@SL1.0/T2.0 | 1542/0.257 | 1088/0.226 | - | - | reject | weak FIT/VAL / drop |
| 535 | sweep | x_ema20_dist_atr>=2.735319@SL0.7/T1.5 | 2150/0.24 | 1618/0.182 | - | - | reject | weak FIT/VAL / drop |
| 536 | sweep | x_ema20_dist_atr>=2.735319@SL1.0/T2.0 | 1642/0.258 | 1250/0.224 | - | - | reject | weak FIT/VAL / drop |
| 537 | sweep | x_ema20_dist_atr<=2.735319@SL0.7/T1.5 | 2500/0.268 | 1808/0.194 | - | - | reject | weak FIT/VAL / drop |
| 538 | sweep | x_ema20_dist_atr<=2.735319@SL1.0/T2.0 | 1700/0.273 | 1228/0.226 | - | - | reject | weak FIT/VAL / drop |
| 539 | sweep | x_ema20_dist_atr>=3.593707@SL0.7/T1.5 | 1057/0.226 | 681/0.187 | - | - | reject | weak FIT/VAL / drop |
| 540 | sweep | x_ema20_dist_atr>=3.593707@SL1.0/T2.0 | 983/0.279 | 668/0.217 | - | - | reject | weak FIT/VAL / drop |
| 541 | sweep | x_ema20_dist_atr<=3.593707@SL0.7/T1.5 | 2777/0.258 | 2040/0.196 | - | - | reject | weak FIT/VAL / drop |
| 542 | sweep | x_ema20_dist_atr<=3.593707@SL1.0/T2.0 | 1843/0.262 | 1384/0.23 | - | - | reject | weak FIT/VAL / drop |
| 543 | sweep | x_ema20_slope3_atr>=0.024218@SL0.7/T1.5 | 2932/0.251 | 2222/0.193 | - | - | reject | weak FIT/VAL / drop |
| 544 | sweep | x_ema20_slope3_atr>=0.024218@SL1.0/T2.0 | 1975/0.26 | 1500/0.232 | - | - | reject | weak FIT/VAL / drop |
| 545 | sweep | x_ema20_slope3_atr<=0.024218@SL0.7/T1.5 | 1051/0.206 | 616/0.16 | - | - | reject | weak FIT/VAL / drop |
| 546 | sweep | x_ema20_slope3_atr<=0.024218@SL1.0/T2.0 | 930/0.209 | 569/0.18 | - | - | reject | weak FIT/VAL / drop |
| 547 | sweep | x_ema20_slope3_atr>=0.285788@SL0.7/T1.5 | 2842/0.242 | 2156/0.189 | - | - | reject | weak FIT/VAL / drop |
| 548 | sweep | x_ema20_slope3_atr>=0.285788@SL1.0/T2.0 | 1989/0.239 | 1492/0.24 | - | - | reject | weak FIT/VAL / drop |
| 549 | sweep | x_ema20_slope3_atr<=0.285788@SL0.7/T1.5 | 1814/0.243 | 1229/0.169 | - | - | reject | weak FIT/VAL / drop |
| 550 | sweep | x_ema20_slope3_atr<=0.285788@SL1.0/T2.0 | 1381/0.238 | 929/0.182 | - | - | reject | weak FIT/VAL / drop |
| 551 | sweep | x_ema20_slope3_atr>=0.472507@SL0.7/T1.5 | 2568/0.238 | 2012/0.181 | - | - | reject | weak FIT/VAL / drop |
| 552 | sweep | x_ema20_slope3_atr>=0.472507@SL1.0/T2.0 | 1867/0.262 | 1427/0.212 | - | - | reject | weak FIT/VAL / drop |
| 553 | sweep | x_ema20_slope3_atr<=0.472507@SL0.7/T1.5 | 2257/0.259 | 1536/0.184 | - | - | reject | weak FIT/VAL / drop |
| 554 | sweep | x_ema20_slope3_atr<=0.472507@SL1.0/T2.0 | 1550/0.269 | 1097/0.229 | - | - | reject | weak FIT/VAL / drop |
| 555 | sweep | x_ema20_slope3_atr>=0.658683@SL0.7/T1.5 | 2107/0.227 | 1588/0.183 | - | - | reject | weak FIT/VAL / drop |
| 556 | sweep | x_ema20_slope3_atr>=0.658683@SL1.0/T2.0 | 1624/0.259 | 1234/0.231 | - | - | reject | weak FIT/VAL / drop |
| 557 | sweep | x_ema20_slope3_atr<=0.658683@SL0.7/T1.5 | 2540/0.255 | 1814/0.187 | - | - | reject | weak FIT/VAL / drop |
| 558 | sweep | x_ema20_slope3_atr<=0.658683@SL1.0/T2.0 | 1694/0.268 | 1257/0.234 | - | - | reject | weak FIT/VAL / drop |
| 559 | sweep | x_ema20_slope3_atr>=0.93517@SL0.7/T1.5 | 982/0.221 | 677/0.174 | - | - | reject | weak FIT/VAL / drop |
| 560 | sweep | x_ema20_slope3_atr>=0.93517@SL1.0/T2.0 | 931/0.278 | 670/0.202 | - | - | reject | weak FIT/VAL / drop |
| 561 | sweep | x_ema20_slope3_atr<=0.93517@SL0.7/T1.5 | 2825/0.253 | 2066/0.198 | - | - | reject | weak FIT/VAL / drop |
| 562 | sweep | x_ema20_slope3_atr<=0.93517@SL1.0/T2.0 | 1873/0.258 | 1391/0.23 | - | - | reject | weak FIT/VAL / drop |
| 563 | sweep | x_ema50_dist_atr>=0.753261@SL0.7/T1.5 | 2921/0.237 | 2183/0.202 | - | - | reject | weak FIT/VAL / drop |
| 564 | sweep | x_ema50_dist_atr>=0.753261@SL1.0/T2.0 | 1989/0.249 | 1491/0.229 | - | - | reject | weak FIT/VAL / drop |
| 565 | sweep | x_ema50_dist_atr<=0.753261@SL0.7/T1.5 | 905/0.24 | 442/0.153 | - | - | reject | weak FIT/VAL / drop |
| 566 | sweep | x_ema50_dist_atr<=0.753261@SL1.0/T2.0 | 732/0.233 | 411/0.169 | - | - | reject | weak FIT/VAL / drop |
| 567 | sweep | x_ema50_dist_atr>=2.263982@SL0.7/T1.5 | 2717/0.235 | 2109/0.203 | - | - | reject | weak FIT/VAL / drop |
| 568 | sweep | x_ema50_dist_atr>=2.263982@SL1.0/T2.0 | 1913/0.245 | 1481/0.235 | - | - | reject | weak FIT/VAL / drop |
| 569 | sweep | x_ema50_dist_atr<=2.263982@SL0.7/T1.5 | 1829/0.255 | 1189/0.172 | - | - | reject | weak FIT/VAL / drop |
| 570 | sweep | x_ema50_dist_atr<=2.263982@SL1.0/T2.0 | 1368/0.259 | 924/0.189 | - | - | reject | weak FIT/VAL / drop |
| 571 | sweep | x_ema50_dist_atr>=3.258936@SL0.7/T1.5 | 2399/0.221 | 1966/0.198 | - | - | reject | weak FIT/VAL / drop |
| 572 | sweep | x_ema50_dist_atr>=3.258936@SL1.0/T2.0 | 1742/0.257 | 1424/0.225 | - | - | reject | weak FIT/VAL / drop |
| 573 | sweep | x_ema50_dist_atr<=3.258936@SL0.7/T1.5 | 2260/0.261 | 1578/0.179 | - | - | reject | weak FIT/VAL / drop |
| 574 | sweep | x_ema50_dist_atr<=3.258936@SL1.0/T2.0 | 1575/0.27 | 1120/0.205 | - | - | reject | weak FIT/VAL / drop |
| 575 | sweep | x_ema50_dist_atr>=4.268179@SL0.7/T1.5 | 1887/0.212 | 1498/0.193 | - | - | reject | weak FIT/VAL / drop |
| 576 | sweep | x_ema50_dist_atr>=4.268179@SL1.0/T2.0 | 1450/0.231 | 1201/0.228 | - | - | reject | weak FIT/VAL / drop |
| 577 | sweep | x_ema50_dist_atr<=4.268179@SL0.7/T1.5 | 2559/0.257 | 1827/0.196 | - | - | reject | weak FIT/VAL / drop |
| 578 | sweep | x_ema50_dist_atr<=4.268179@SL1.0/T2.0 | 1736/0.268 | 1249/0.223 | - | - | reject | weak FIT/VAL / drop |
| 579 | sweep | x_ema50_dist_atr>=5.64743@SL0.7/T1.5 | 972/0.209 | 579/0.196 | - | - | reject | weak FIT/VAL / drop |
| 580 | sweep | x_ema50_dist_atr>=5.64743@SL1.0/T2.0 | 829/0.255 | 555/0.228 | - | - | reject | weak FIT/VAL / drop |
| 581 | sweep | x_ema50_dist_atr<=5.64743@SL0.7/T1.5 | 2881/0.251 | 2068/0.194 | - | - | reject | weak FIT/VAL / drop |
| 582 | sweep | x_ema50_dist_atr<=5.64743@SL1.0/T2.0 | 1907/0.26 | 1404/0.214 | - | - | reject | weak FIT/VAL / drop |
| 583 | sweep | x_gap_pct>=-1.507538@SL0.7/T1.5 | 2778/0.243 | 2190/0.201 | - | - | reject | weak FIT/VAL / drop |
| 584 | sweep | x_gap_pct>=-1.507538@SL1.0/T2.0 | 1873/0.254 | 1459/0.225 | - | - | reject | weak FIT/VAL / drop |
| 585 | sweep | x_gap_pct<=-1.507538@SL0.7/T1.5 | 688/0.28 | 186/0.188 | - | - | reject | weak FIT/VAL / drop |
| 586 | sweep | x_gap_pct<=-1.507538@SL1.0/T2.0 | 516/0.329 | 185/0.229 | - | - | reject | weak FIT/VAL / drop |
| 587 | sweep | x_gap_pct>=-0.296403@SL0.7/T1.5 | 2383/0.23 | 2093/0.201 | - | - | reject | weak FIT/VAL / drop |
| 588 | sweep | x_gap_pct>=-0.296403@SL1.0/T2.0 | 1689/0.255 | 1422/0.214 | - | - | reject | weak FIT/VAL / drop |
| 589 | sweep | x_gap_pct<=-0.296403@SL0.7/T1.5 | 1640/0.236 | 903/0.173 | - | - | reject | weak FIT/VAL / drop |
| 590 | sweep | x_gap_pct<=-0.296403@SL1.0/T2.0 | 1245/0.244 | 755/0.202 | - | - | reject | weak FIT/VAL / drop |
| 591 | sweep | x_gap_pct>=0.125487@SL0.7/T1.5 | 2014/0.231 | 1796/0.186 | - | - | reject | weak FIT/VAL / drop |
| 592 | sweep | x_gap_pct>=0.125487@SL1.0/T2.0 | 1445/0.257 | 1318/0.228 | - | - | reject | weak FIT/VAL / drop |
| 593 | sweep | x_gap_pct<=0.125487@SL0.7/T1.5 | 2176/0.223 | 1564/0.182 | - | - | reject | weak FIT/VAL / drop |
| 594 | sweep | x_gap_pct<=0.125487@SL1.0/T2.0 | 1545/0.235 | 1133/0.217 | - | - | reject | weak FIT/VAL / drop |
| 595 | sweep | x_gap_pct>=0.625543@SL0.7/T1.5 | 1545/0.26 | 1271/0.198 | - | - | reject | weak FIT/VAL / drop |
| 596 | sweep | x_gap_pct>=0.625543@SL1.0/T2.0 | 1175/0.275 | 1029/0.236 | - | - | reject | weak FIT/VAL / drop |
| 597 | sweep | x_gap_pct<=0.625543@SL0.7/T1.5 | 2448/0.241 | 1865/0.193 | - | - | reject | weak FIT/VAL / drop |
| 598 | sweep | x_gap_pct<=0.625543@SL1.0/T2.0 | 1696/0.247 | 1302/0.212 | - | - | reject | weak FIT/VAL / drop |
| 599 | sweep | x_gap_pct>=1.588235@SL0.7/T1.5 | 692/0.297 | 356/0.237 | - | - | reject | weak FIT/VAL / drop |
| 600 | sweep | x_gap_pct>=1.588235@SL1.0/T2.0 | 524/0.339 | 352/0.309 | - | - | reject | weak FIT/VAL / drop |
| 601 | sweep | x_gap_pct<=1.588235@SL0.7/T1.5 | 2747/0.238 | 2127/0.189 | - | - | reject | weak FIT/VAL / drop |
| 602 | sweep | x_gap_pct<=1.588235@SL1.0/T2.0 | 1845/0.234 | 1426/0.205 | - | - | reject | weak FIT/VAL / drop |
| 603 | sweep | x_kelt_pos>=0.750887@SL0.7/T1.5 | 2943/0.25 | 2209/0.202 | - | - | reject | weak FIT/VAL / drop |
| 604 | sweep | x_kelt_pos>=0.750887@SL1.0/T2.0 | 1987/0.258 | 1476/0.237 | - | - | reject | weak FIT/VAL / drop |
| 605 | sweep | x_kelt_pos<=0.750887@SL0.7/T1.5 | 1042/0.202 | 556/0.15 | - | - | reject | weak FIT/VAL / drop |
| 606 | sweep | x_kelt_pos<=0.750887@SL1.0/T2.0 | 907/0.208 | 514/0.161 | - | - | reject | weak FIT/VAL / drop |
| 607 | sweep | x_kelt_pos>=1.025766@SL0.7/T1.5 | 2851/0.241 | 2166/0.198 | - | - | reject | weak FIT/VAL / drop |
| 608 | sweep | x_kelt_pos>=1.025766@SL1.0/T2.0 | 1996/0.25 | 1485/0.242 | - | - | reject | weak FIT/VAL / drop |
| 609 | sweep | x_kelt_pos<=1.025766@SL0.7/T1.5 | 1874/0.234 | 1189/0.151 | - | - | reject | weak FIT/VAL / drop |
| 610 | sweep | x_kelt_pos<=1.025766@SL1.0/T2.0 | 1380/0.237 | 903/0.174 | - | - | reject | weak FIT/VAL / drop |
| 611 | sweep | x_kelt_pos>=1.219575@SL0.7/T1.5 | 2620/0.236 | 2016/0.18 | - | - | reject | weak FIT/VAL / drop |
| 612 | sweep | x_kelt_pos>=1.219575@SL1.0/T2.0 | 1902/0.268 | 1400/0.227 | - | - | reject | weak FIT/VAL / drop |
| 613 | sweep | x_kelt_pos<=1.219575@SL0.7/T1.5 | 2215/0.259 | 1528/0.193 | - | - | reject | weak FIT/VAL / drop |
| 614 | sweep | x_kelt_pos<=1.219575@SL1.0/T2.0 | 1542/0.257 | 1088/0.226 | - | - | reject | weak FIT/VAL / drop |
| 615 | sweep | x_kelt_pos>=1.411773@SL0.7/T1.5 | 2150/0.24 | 1618/0.182 | - | - | reject | weak FIT/VAL / drop |
| 616 | sweep | x_kelt_pos>=1.411773@SL1.0/T2.0 | 1642/0.258 | 1250/0.224 | - | - | reject | weak FIT/VAL / drop |
| 617 | sweep | x_kelt_pos<=1.411773@SL0.7/T1.5 | 2500/0.268 | 1808/0.194 | - | - | reject | weak FIT/VAL / drop |
| 618 | sweep | x_kelt_pos<=1.411773@SL1.0/T2.0 | 1700/0.273 | 1228/0.226 | - | - | reject | weak FIT/VAL / drop |
| 619 | sweep | x_kelt_pos>=1.697902@SL0.7/T1.5 | 1057/0.226 | 681/0.187 | - | - | reject | weak FIT/VAL / drop |
| 620 | sweep | x_kelt_pos>=1.697902@SL1.0/T2.0 | 983/0.279 | 668/0.217 | - | - | reject | weak FIT/VAL / drop |
| 621 | sweep | x_kelt_pos<=1.697902@SL0.7/T1.5 | 2777/0.258 | 2040/0.196 | - | - | reject | weak FIT/VAL / drop |
| 622 | sweep | x_kelt_pos<=1.697902@SL1.0/T2.0 | 1843/0.262 | 1384/0.23 | - | - | reject | weak FIT/VAL / drop |
| 623 | sweep | x_macd_hist_atr>=-0.139386@SL0.7/T1.5 | 2986/0.246 | 2193/0.193 | - | - | reject | weak FIT/VAL / drop |
| 624 | sweep | x_macd_hist_atr>=-0.139386@SL1.0/T2.0 | 1985/0.247 | 1458/0.23 | - | - | reject | weak FIT/VAL / drop |
| 625 | sweep | x_macd_hist_atr<=-0.139386@SL0.7/T1.5 | 848/0.206 | 516/0.185 | - | - | reject | weak FIT/VAL / drop |
| 626 | sweep | x_macd_hist_atr<=-0.139386@SL1.0/T2.0 | 720/0.218 | 480/0.243 | - | - | reject | weak FIT/VAL / drop |
| 627 | sweep | x_macd_hist_atr>=0.044776@SL0.7/T1.5 | 2862/0.255 | 2130/0.193 | - | - | reject | weak FIT/VAL / drop |
| 628 | sweep | x_macd_hist_atr>=0.044776@SL1.0/T2.0 | 2001/0.259 | 1412/0.231 | - | - | reject | weak FIT/VAL / drop |
| 629 | sweep | x_macd_hist_atr<=0.044776@SL0.7/T1.5 | 1673/0.233 | 1194/0.185 | - | - | reject | weak FIT/VAL / drop |
| 630 | sweep | x_macd_hist_atr<=0.044776@SL1.0/T2.0 | 1280/0.223 | 899/0.208 | - | - | reject | weak FIT/VAL / drop |
| 631 | sweep | x_macd_hist_atr>=0.164346@SL0.7/T1.5 | 2630/0.255 | 1954/0.173 | - | - | reject | weak FIT/VAL / drop |
| 632 | sweep | x_macd_hist_atr>=0.164346@SL1.0/T2.0 | 1901/0.266 | 1362/0.215 | - | - | reject | weak FIT/VAL / drop |
| 633 | sweep | x_macd_hist_atr<=0.164346@SL0.7/T1.5 | 2119/0.23 | 1580/0.186 | - | - | reject | weak FIT/VAL / drop |
| 634 | sweep | x_macd_hist_atr<=0.164346@SL1.0/T2.0 | 1533/0.263 | 1110/0.23 | - | - | reject | weak FIT/VAL / drop |
| 635 | sweep | x_macd_hist_atr>=0.283827@SL0.7/T1.5 | 2072/0.258 | 1520/0.185 | - | - | reject | weak FIT/VAL / drop |
| 636 | sweep | x_macd_hist_atr>=0.283827@SL1.0/T2.0 | 1621/0.265 | 1181/0.232 | - | - | reject | weak FIT/VAL / drop |
| 637 | sweep | x_macd_hist_atr<=0.283827@SL0.7/T1.5 | 2531/0.238 | 1866/0.191 | - | - | reject | weak FIT/VAL / drop |
| 638 | sweep | x_macd_hist_atr<=0.283827@SL1.0/T2.0 | 1755/0.256 | 1275/0.205 | - | - | reject | weak FIT/VAL / drop |
| 639 | sweep | x_macd_hist_atr>=0.467109@SL0.7/T1.5 | 956/0.318 | 593/0.211 | - | - | reject | weak FIT/VAL / drop |
| 640 | sweep | x_macd_hist_atr>=0.467109@SL1.0/T2.0 | 872/0.35 | 569/0.232 | - | - | reject | weak FIT/VAL / drop |
| 641 | sweep | x_macd_hist_atr<=0.467109@SL0.7/T1.5 | 2820/0.242 | 2085/0.187 | - | - | reject | weak FIT/VAL / drop |
| 642 | sweep | x_macd_hist_atr<=0.467109@SL1.0/T2.0 | 1895/0.245 | 1441/0.225 | - | - | reject | weak FIT/VAL / drop |
| 643 | sweep | x_macd_hist_delta_atr>=0.015327@SL0.7/T1.5 | 2923/0.246 | 2196/0.192 | - | - | reject | weak FIT/VAL / drop |
| 644 | sweep | x_macd_hist_delta_atr>=0.015327@SL1.0/T2.0 | 1933/0.272 | 1446/0.227 | - | - | reject | weak FIT/VAL / drop |
| 645 | sweep | x_macd_hist_delta_atr<=0.015327@SL0.7/T1.5 | 1053/0.251 | 486/0.226 | - | - | reject | weak FIT/VAL / drop |
| 646 | sweep | x_macd_hist_delta_atr<=0.015327@SL1.0/T2.0 | 976/0.249 | 481/0.215 | - | - | reject | weak FIT/VAL / drop |
| 647 | sweep | x_macd_hist_delta_atr>=0.047252@SL0.7/T1.5 | 2741/0.251 | 2019/0.174 | - | - | reject | weak FIT/VAL / drop |
| 648 | sweep | x_macd_hist_delta_atr>=0.047252@SL1.0/T2.0 | 1860/0.263 | 1389/0.239 | - | - | reject | weak FIT/VAL / drop |
| 649 | sweep | x_macd_hist_delta_atr<=0.047252@SL0.7/T1.5 | 1954/0.249 | 1355/0.201 | - | - | reject | weak FIT/VAL / drop |
| 650 | sweep | x_macd_hist_delta_atr<=0.047252@SL1.0/T2.0 | 1486/0.267 | 1063/0.225 | - | - | reject | weak FIT/VAL / drop |
| 651 | sweep | x_macd_hist_delta_atr>=0.071443@SL0.7/T1.5 | 2478/0.253 | 1794/0.17 | - | - | reject | weak FIT/VAL / drop |
| 652 | sweep | x_macd_hist_delta_atr>=0.071443@SL1.0/T2.0 | 1738/0.265 | 1280/0.231 | - | - | reject | weak FIT/VAL / drop |
| 653 | sweep | x_macd_hist_delta_atr<=0.071443@SL0.7/T1.5 | 2360/0.247 | 1729/0.171 | - | - | reject | weak FIT/VAL / drop |
| 654 | sweep | x_macd_hist_delta_atr<=0.071443@SL1.0/T2.0 | 1684/0.26 | 1240/0.211 | - | - | reject | weak FIT/VAL / drop |
| 655 | sweep | x_macd_hist_delta_atr>=0.09679@SL0.7/T1.5 | 2126/0.247 | 1488/0.172 | - | - | reject | weak FIT/VAL / drop |
| 656 | sweep | x_macd_hist_delta_atr>=0.09679@SL1.0/T2.0 | 1614/0.265 | 1101/0.221 | - | - | reject | weak FIT/VAL / drop |
| 657 | sweep | x_macd_hist_delta_atr<=0.09679@SL0.7/T1.5 | 2605/0.245 | 1997/0.19 | - | - | reject | weak FIT/VAL / drop |
| 658 | sweep | x_macd_hist_delta_atr<=0.09679@SL1.0/T2.0 | 1804/0.262 | 1345/0.238 | - | - | reject | weak FIT/VAL / drop |
| 659 | sweep | x_macd_hist_delta_atr>=0.136315@SL0.7/T1.5 | 1126/0.255 | 741/0.193 | - | - | reject | weak FIT/VAL / drop |
| 660 | sweep | x_macd_hist_delta_atr>=0.136315@SL1.0/T2.0 | 1018/0.278 | 692/0.235 | - | - | reject | weak FIT/VAL / drop |
| 661 | sweep | x_macd_hist_delta_atr<=0.136315@SL0.7/T1.5 | 2885/0.248 | 2103/0.196 | - | - | reject | weak FIT/VAL / drop |
| 662 | sweep | x_macd_hist_delta_atr<=0.136315@SL1.0/T2.0 | 1912/0.246 | 1452/0.222 | - | - | reject | weak FIT/VAL / drop |
| 663 | sweep | x_mfi14>=47.99881@SL0.7/T1.5 | 2909/0.254 | 2208/0.194 | - | - | reject | weak FIT/VAL / drop |
| 664 | sweep | x_mfi14>=47.99881@SL1.0/T2.0 | 1959/0.26 | 1469/0.226 | - | - | reject | weak FIT/VAL / drop |
| 665 | sweep | x_mfi14<=47.99881@SL0.7/T1.5 | 1096/0.186 | 613/0.176 | - | - | reject | weak FIT/VAL / drop |
| 666 | sweep | x_mfi14<=47.99881@SL1.0/T2.0 | 982/0.177 | 561/0.198 | - | - | reject | weak FIT/VAL / drop |
| 667 | sweep | x_mfi14>=63.09824@SL0.7/T1.5 | 2757/0.249 | 2107/0.192 | - | - | reject | weak FIT/VAL / drop |
| 668 | sweep | x_mfi14>=63.09824@SL1.0/T2.0 | 1927/0.257 | 1442/0.24 | - | - | reject | weak FIT/VAL / drop |
| 669 | sweep | x_mfi14<=63.09824@SL0.7/T1.5 | 1979/0.227 | 1262/0.162 | - | - | reject | weak FIT/VAL / drop |
| 670 | sweep | x_mfi14<=63.09824@SL1.0/T2.0 | 1463/0.243 | 973/0.197 | - | - | reject | weak FIT/VAL / drop |
| 671 | sweep | x_mfi14>=72.669313@SL0.7/T1.5 | 2509/0.25 | 1925/0.191 | - | - | reject | weak FIT/VAL / drop |
| 672 | sweep | x_mfi14>=72.669313@SL1.0/T2.0 | 1837/0.274 | 1391/0.242 | - | - | reject | weak FIT/VAL / drop |
| 673 | sweep | x_mfi14<=72.669313@SL0.7/T1.5 | 2282/0.246 | 1581/0.166 | - | - | reject | weak FIT/VAL / drop |
| 674 | sweep | x_mfi14<=72.669313@SL1.0/T2.0 | 1584/0.252 | 1123/0.192 | - | - | reject | weak FIT/VAL / drop |
| 675 | sweep | x_mfi14>=80.899182@SL0.7/T1.5 | 2070/0.261 | 1585/0.187 | - | - | reject | weak FIT/VAL / drop |
| 676 | sweep | x_mfi14>=80.899182@SL1.0/T2.0 | 1574/0.273 | 1224/0.218 | - | - | reject | weak FIT/VAL / drop |
| 677 | sweep | x_mfi14<=80.899182@SL0.7/T1.5 | 2558/0.251 | 1851/0.187 | - | - | reject | weak FIT/VAL / drop |
| 678 | sweep | x_mfi14<=80.899182@SL1.0/T2.0 | 1761/0.26 | 1267/0.224 | - | - | reject | weak FIT/VAL / drop |
| 679 | sweep | x_mfi14>=89.910371@SL0.7/T1.5 | 1047/0.276 | 746/0.197 | - | - | reject | weak FIT/VAL / drop |
| 680 | sweep | x_mfi14>=89.910371@SL1.0/T2.0 | 937/0.301 | 733/0.229 | - | - | reject | weak FIT/VAL / drop |
| 681 | sweep | x_mfi14<=89.910371@SL0.7/T1.5 | 2842/0.239 | 2032/0.193 | - | - | reject | weak FIT/VAL / drop |
| 682 | sweep | x_mfi14<=89.910371@SL1.0/T2.0 | 1869/0.251 | 1387/0.215 | - | - | reject | weak FIT/VAL / drop |
| 683 | sweep | x_obv_slope5>=0.644501@SL0.7/T1.5 | 2946/0.234 | 2210/0.207 | - | - | reject | weak FIT/VAL / drop |
| 684 | sweep | x_obv_slope5>=0.644501@SL1.0/T2.0 | 1941/0.253 | 1454/0.234 | - | - | reject | weak FIT/VAL / drop |
| 685 | sweep | x_obv_slope5<=0.644501@SL0.7/T1.5 | 1043/0.228 | 559/0.15 | - | - | reject | weak FIT/VAL / drop |
| 686 | sweep | x_obv_slope5<=0.644501@SL1.0/T2.0 | 964/0.217 | 526/0.176 | - | - | reject | weak FIT/VAL / drop |
| 687 | sweep | x_obv_slope5>=2.301614@SL0.7/T1.5 | 2828/0.247 | 2092/0.2 | - | - | reject | weak FIT/VAL / drop |
| 688 | sweep | x_obv_slope5>=2.301614@SL1.0/T2.0 | 1932/0.266 | 1439/0.234 | - | - | reject | weak FIT/VAL / drop |
| 689 | sweep | x_obv_slope5<=2.301614@SL0.7/T1.5 | 1937/0.23 | 1337/0.16 | - | - | reject | weak FIT/VAL / drop |
| 690 | sweep | x_obv_slope5<=2.301614@SL1.0/T2.0 | 1461/0.232 | 1052/0.183 | - | - | reject | weak FIT/VAL / drop |
| 691 | sweep | x_obv_slope5>=3.616402@SL0.7/T1.5 | 2550/0.268 | 1826/0.205 | - | - | reject | weak FIT/VAL / drop |
| 692 | sweep | x_obv_slope5>=3.616402@SL1.0/T2.0 | 1824/0.293 | 1332/0.243 | - | - | reject | weak FIT/VAL / drop |
| 693 | sweep | x_obv_slope5<=3.616402@SL0.7/T1.5 | 2287/0.239 | 1712/0.158 | - | - | reject | weak FIT/VAL / drop |
| 694 | sweep | x_obv_slope5<=3.616402@SL1.0/T2.0 | 1635/0.252 | 1218/0.196 | - | - | reject | weak FIT/VAL / drop |
| 695 | sweep | x_obv_slope5>=5.13893@SL0.7/T1.5 | 2126/0.282 | 1498/0.204 | - | - | reject | weak FIT/VAL / drop |
| 696 | sweep | x_obv_slope5>=5.13893@SL1.0/T2.0 | 1658/0.301 | 1165/0.231 | - | - | reject | weak FIT/VAL / drop |
| 697 | sweep | x_obv_slope5<=5.13893@SL0.7/T1.5 | 2579/0.226 | 1848/0.182 | - | - | reject | weak FIT/VAL / drop |
| 698 | sweep | x_obv_slope5<=5.13893@SL1.0/T2.0 | 1731/0.248 | 1270/0.196 | - | - | reject | weak FIT/VAL / drop |
| 699 | sweep | x_obv_slope5>=8.001018@SL0.7/T1.5 | 1094/0.281 | 739/0.185 | - | - | reject | weak FIT/VAL / drop |
| 700 | sweep | x_obv_slope5>=8.001018@SL1.0/T2.0 | 1007/0.352 | 699/0.204 | - | - | reject | weak FIT/VAL / drop |
| 701 | sweep | x_obv_slope5<=8.001018@SL0.7/T1.5 | 2825/0.233 | 2031/0.197 | - | - | reject | weak FIT/VAL / drop |
| 702 | sweep | x_obv_slope5<=8.001018@SL1.0/T2.0 | 1861/0.231 | 1376/0.219 | - | - | reject | weak FIT/VAL / drop |
| 703 | sweep | x_orh_dist_atr>=-2.615393@SL0.7/T1.5 | 2962/0.24 | 2224/0.198 | - | - | reject | weak FIT/VAL / drop |
| 704 | sweep | x_orh_dist_atr>=-2.615393@SL1.0/T2.0 | 1982/0.25 | 1500/0.227 | - | - | reject | weak FIT/VAL / drop |
| 705 | sweep | x_orh_dist_atr<=-2.615393@SL0.7/T1.5 | 1046/0.243 | 445/0.184 | - | - | reject | weak FIT/VAL / drop |
| 706 | sweep | x_orh_dist_atr<=-2.615393@SL1.0/T2.0 | 893/0.256 | 411/0.217 | - | - | reject | weak FIT/VAL / drop |
| 707 | sweep | x_orh_dist_atr>=0.085327@SL0.7/T1.5 | 2770/0.224 | 2170/0.195 | - | - | reject | weak FIT/VAL / drop |
| 708 | sweep | x_orh_dist_atr>=0.085327@SL1.0/T2.0 | 1950/0.243 | 1472/0.229 | - | - | reject | weak FIT/VAL / drop |
| 709 | sweep | x_orh_dist_atr<=0.085327@SL0.7/T1.5 | 1878/0.282 | 1140/0.182 | - | - | reject | weak FIT/VAL / drop |
| 710 | sweep | x_orh_dist_atr<=0.085327@SL1.0/T2.0 | 1399/0.293 | 880/0.196 | - | - | reject | weak FIT/VAL / drop |
| 711 | sweep | x_orh_dist_atr>=1.845455@SL0.7/T1.5 | 2395/0.229 | 1921/0.201 | - | - | reject | weak FIT/VAL / drop |
| 712 | sweep | x_orh_dist_atr>=1.845455@SL1.0/T2.0 | 1769/0.249 | 1393/0.224 | - | - | reject | weak FIT/VAL / drop |
| 713 | sweep | x_orh_dist_atr<=1.845455@SL0.7/T1.5 | 2302/0.261 | 1597/0.183 | - | - | reject | weak FIT/VAL / drop |
| 714 | sweep | x_orh_dist_atr<=1.845455@SL1.0/T2.0 | 1614/0.276 | 1148/0.211 | - | - | reject | weak FIT/VAL / drop |
| 715 | sweep | x_orh_dist_atr>=3.808399@SL0.7/T1.5 | 1806/0.212 | 1493/0.216 | - | - | reject | weak FIT/VAL / drop |
| 716 | sweep | x_orh_dist_atr>=3.808399@SL1.0/T2.0 | 1424/0.24 | 1137/0.241 | - | - | reject | weak FIT/VAL / drop |
| 717 | sweep | x_orh_dist_atr<=3.808399@SL0.7/T1.5 | 2603/0.249 | 1864/0.184 | - | - | reject | weak FIT/VAL / drop |
| 718 | sweep | x_orh_dist_atr<=3.808399@SL1.0/T2.0 | 1781/0.271 | 1285/0.211 | - | - | reject | weak FIT/VAL / drop |
| 719 | sweep | x_orh_dist_atr>=7.107927@SL0.7/T1.5 | 903/0.222 | 610/0.222 | - | - | reject | weak FIT/VAL / drop |
| 720 | sweep | x_orh_dist_atr>=7.107927@SL1.0/T2.0 | 764/0.251 | 572/0.273 | - | - | reject | weak FIT/VAL / drop |
| 721 | sweep | x_orh_dist_atr<=7.107927@SL0.7/T1.5 | 2878/0.243 | 2124/0.195 | - | - | reject | weak FIT/VAL / drop |
| 722 | sweep | x_orh_dist_atr<=7.107927@SL1.0/T2.0 | 1891/0.262 | 1431/0.214 | - | - | reject | weak FIT/VAL / drop |
| 723 | sweep | x_pdh_dist_atr>=-10.008219@SL0.7/T1.5 | 2970/0.236 | 2227/0.204 | - | - | reject | weak FIT/VAL / drop |
| 724 | sweep | x_pdh_dist_atr>=-10.008219@SL1.0/T2.0 | 2001/0.245 | 1498/0.224 | - | - | reject | weak FIT/VAL / drop |
| 725 | sweep | x_pdh_dist_atr<=-10.008219@SL0.7/T1.5 | 813/0.294 | 401/0.174 | - | - | reject | weak FIT/VAL / drop |
| 726 | sweep | x_pdh_dist_atr<=-10.008219@SL1.0/T2.0 | 687/0.319 | 365/0.223 | - | - | reject | weak FIT/VAL / drop |
| 727 | sweep | x_pdh_dist_atr>=-3.095306@SL0.7/T1.5 | 2701/0.217 | 2182/0.194 | - | - | reject | weak FIT/VAL / drop |
| 728 | sweep | x_pdh_dist_atr>=-3.095306@SL1.0/T2.0 | 1896/0.232 | 1510/0.216 | - | - | reject | weak FIT/VAL / drop |
| 729 | sweep | x_pdh_dist_atr<=-3.095306@SL0.7/T1.5 | 1640/0.291 | 1041/0.174 | - | - | reject | weak FIT/VAL / drop |
| 730 | sweep | x_pdh_dist_atr<=-3.095306@SL1.0/T2.0 | 1224/0.312 | 814/0.218 | - | - | reject | weak FIT/VAL / drop |
| 731 | sweep | x_pdh_dist_atr>=0.542707@SL0.7/T1.5 | 2291/0.201 | 1950/0.184 | - | - | reject | weak FIT/VAL / drop |
| 732 | sweep | x_pdh_dist_atr>=0.542707@SL1.0/T2.0 | 1704/0.227 | 1440/0.212 | - | - | reject | weak FIT/VAL / drop |
| 733 | sweep | x_pdh_dist_atr<=0.542707@SL0.7/T1.5 | 2202/0.269 | 1517/0.183 | - | - | reject | weak FIT/VAL / drop |
| 734 | sweep | x_pdh_dist_atr<=0.542707@SL1.0/T2.0 | 1484/0.284 | 1048/0.224 | - | - | reject | weak FIT/VAL / drop |
| 735 | sweep | x_pdh_dist_atr>=3.583389@SL0.7/T1.5 | 1748/0.203 | 1384/0.215 | - | - | reject | weak FIT/VAL / drop |
| 736 | sweep | x_pdh_dist_atr>=3.583389@SL1.0/T2.0 | 1362/0.217 | 1109/0.259 | - | - | reject | weak FIT/VAL / drop |
| 737 | sweep | x_pdh_dist_atr<=3.583389@SL0.7/T1.5 | 2581/0.254 | 1871/0.164 | - | - | reject | weak FIT/VAL / drop |
| 738 | sweep | x_pdh_dist_atr<=3.583389@SL1.0/T2.0 | 1718/0.265 | 1264/0.194 | - | - | reject | weak FIT/VAL / drop |
| 739 | sweep | x_pdh_dist_atr>=8.39447@SL0.7/T1.5 | 749/0.227 | 510/0.208 | - | - | reject | weak FIT/VAL / drop |
| 740 | sweep | x_pdh_dist_atr>=8.39447@SL1.0/T2.0 | 641/0.236 | 473/0.248 | - | - | reject | weak FIT/VAL / drop |
| 741 | sweep | x_pdh_dist_atr<=8.39447@SL0.7/T1.5 | 2901/0.249 | 2125/0.196 | - | - | reject | weak FIT/VAL / drop |
| 742 | sweep | x_pdh_dist_atr<=8.39447@SL1.0/T2.0 | 1925/0.255 | 1436/0.215 | - | - | reject | weak FIT/VAL / drop |
| 743 | sweep | x_pdl_dist_atr>=2.778379@SL0.7/T1.5 | 2859/0.245 | 2215/0.2 | - | - | reject | weak FIT/VAL / drop |
| 744 | sweep | x_pdl_dist_atr>=2.778379@SL1.0/T2.0 | 1946/0.25 | 1515/0.225 | - | - | reject | weak FIT/VAL / drop |
| 745 | sweep | x_pdl_dist_atr<=2.778379@SL0.7/T1.5 | 795/0.277 | 347/0.159 | - | - | reject | weak FIT/VAL / drop |
| 746 | sweep | x_pdl_dist_atr<=2.778379@SL1.0/T2.0 | 659/0.291 | 311/0.168 | - | - | reject | weak FIT/VAL / drop |
| 747 | sweep | x_pdl_dist_atr>=7.132877@SL0.7/T1.5 | 2528/0.219 | 1907/0.207 | - | - | reject | weak FIT/VAL / drop |
| 748 | sweep | x_pdl_dist_atr>=7.132877@SL1.0/T2.0 | 1762/0.235 | 1348/0.237 | - | - | reject | weak FIT/VAL / drop |
| 749 | sweep | x_pdl_dist_atr<=7.132877@SL0.7/T1.5 | 1915/0.259 | 1410/0.166 | - | - | reject | weak FIT/VAL / drop |
| 750 | sweep | x_pdl_dist_atr<=7.132877@SL1.0/T2.0 | 1453/0.289 | 1125/0.175 | - | - | reject | weak FIT/VAL / drop |
| 751 | sweep | x_pdl_dist_atr>=10.395499@SL0.7/T1.5 | 1936/0.221 | 1518/0.208 | - | - | reject | weak FIT/VAL / drop |
| 752 | sweep | x_pdl_dist_atr>=10.395499@SL1.0/T2.0 | 1367/0.23 | 1131/0.231 | - | - | reject | weak FIT/VAL / drop |
| 753 | sweep | x_pdl_dist_atr<=10.395499@SL0.7/T1.5 | 2542/0.258 | 1885/0.176 | - | - | reject | weak FIT/VAL / drop |
| 754 | sweep | x_pdl_dist_atr<=10.395499@SL1.0/T2.0 | 1786/0.282 | 1334/0.211 | - | - | reject | weak FIT/VAL / drop |
| 755 | sweep | x_pdl_dist_atr>=14.523594@SL0.7/T1.5 | 1449/0.227 | 1089/0.189 | - | - | reject | weak FIT/VAL / drop |
| 756 | sweep | x_pdl_dist_atr>=14.523594@SL1.0/T2.0 | 1079/0.248 | 844/0.247 | - | - | reject | weak FIT/VAL / drop |
| 757 | sweep | x_pdl_dist_atr<=14.523594@SL0.7/T1.5 | 2836/0.248 | 2060/0.184 | - | - | reject | weak FIT/VAL / drop |
| 758 | sweep | x_pdl_dist_atr<=14.523594@SL1.0/T2.0 | 1937/0.257 | 1406/0.197 | - | - | reject | weak FIT/VAL / drop |
| 759 | sweep | x_pdl_dist_atr>=22.026537@SL0.7/T1.5 | 782/0.247 | 438/0.213 | - | - | reject | weak FIT/VAL / drop |
| 760 | sweep | x_pdl_dist_atr>=22.026537@SL1.0/T2.0 | 655/0.28 | 402/0.261 | - | - | reject | weak FIT/VAL / drop |
| 761 | sweep | x_pdl_dist_atr<=22.026537@SL0.7/T1.5 | 2952/0.241 | 2199/0.195 | - | - | reject | weak FIT/VAL / drop |
| 762 | sweep | x_pdl_dist_atr<=22.026537@SL1.0/T2.0 | 1960/0.251 | 1476/0.218 | - | - | reject | weak FIT/VAL / drop |
| 763 | sweep | x_pos_in_dayrange>=0.615054@SL0.7/T1.5 | 2972/0.241 | 2215/0.2 | - | - | reject | weak FIT/VAL / drop |
| 764 | sweep | x_pos_in_dayrange>=0.615054@SL1.0/T2.0 | 2009/0.243 | 1501/0.236 | - | - | reject | weak FIT/VAL / drop |
| 765 | sweep | x_pos_in_dayrange<=0.615054@SL0.7/T1.5 | 1060/0.263 | 546/0.126 | - | - | reject | weak FIT/VAL / drop |
| 766 | sweep | x_pos_in_dayrange<=0.615054@SL1.0/T2.0 | 904/0.288 | 495/0.161 | - | - | reject | weak FIT/VAL / drop |
| 767 | sweep | x_pos_in_dayrange>=0.816152@SL0.7/T1.5 | 2864/0.235 | 2184/0.193 | - | - | reject | weak FIT/VAL / drop |
| 768 | sweep | x_pos_in_dayrange>=0.816152@SL1.0/T2.0 | 1984/0.261 | 1464/0.229 | - | - | reject | weak FIT/VAL / drop |
| 769 | sweep | x_pos_in_dayrange<=0.816152@SL0.7/T1.5 | 1785/0.262 | 1142/0.182 | - | - | reject | weak FIT/VAL / drop |
| 770 | sweep | x_pos_in_dayrange<=0.816152@SL1.0/T2.0 | 1310/0.282 | 879/0.204 | - | - | reject | weak FIT/VAL / drop |
| 771 | sweep | x_pos_in_dayrange>=0.940519@SL0.7/T1.5 | 2514/0.229 | 1975/0.183 | - | - | reject | weak FIT/VAL / drop |
| 772 | sweep | x_pos_in_dayrange>=0.940519@SL1.0/T2.0 | 1842/0.247 | 1386/0.232 | - | - | reject | weak FIT/VAL / drop |
| 773 | sweep | x_pos_in_dayrange<=0.940519@SL0.7/T1.5 | 2201/0.269 | 1521/0.185 | - | - | reject | weak FIT/VAL / drop |
| 774 | sweep | x_pos_in_dayrange<=0.940519@SL1.0/T2.0 | 1535/0.286 | 1092/0.243 | - | - | reject | weak FIT/VAL / drop |
| 775 | sweep | x_pos_in_dayrange>=0.977621@SL0.7/T1.5 | 1890/0.223 | 1523/0.184 | - | - | reject | weak FIT/VAL / drop |
| 776 | sweep | x_pos_in_dayrange>=0.977621@SL1.0/T2.0 | 1474/0.247 | 1156/0.232 | - | - | reject | weak FIT/VAL / drop |
| 777 | sweep | x_pos_in_dayrange<=0.977621@SL0.7/T1.5 | 2638/0.252 | 1828/0.189 | - | - | reject | weak FIT/VAL / drop |
| 778 | sweep | x_pos_in_dayrange<=0.977621@SL1.0/T2.0 | 1805/0.259 | 1281/0.236 | - | - | reject | weak FIT/VAL / drop |
| 779 | sweep | x_pos_in_dayrange>=1.0@SL0.7/T1.5 | 1044/0.214 | 697/0.186 | - | - | reject | weak FIT/VAL / drop |
| 780 | sweep | x_pos_in_dayrange>=1.0@SL1.0/T2.0 | 912/0.21 | 681/0.238 | - | - | reject | weak FIT/VAL / drop |
| 781 | sweep | x_pos_in_dayrange<=1.0@SL0.7/T1.5 | 2980/0.244 | 2208/0.196 | - | - | reject | weak FIT/VAL / drop |
| 782 | sweep | x_pos_in_dayrange<=1.0@SL1.0/T2.0 | 1963/0.25 | 1488/0.228 | - | - | reject | weak FIT/VAL / drop |
| 783 | sweep | x_range_vs_avg20>=0.897434@SL0.7/T1.5 | 2945/0.239 | 2228/0.185 | - | - | reject | weak FIT/VAL / drop |
| 784 | sweep | x_range_vs_avg20>=0.897434@SL1.0/T2.0 | 1966/0.248 | 1473/0.231 | - | - | reject | weak FIT/VAL / drop |
| 785 | sweep | x_range_vs_avg20<=0.897434@SL0.7/T1.5 | 1012/0.244 | 525/0.197 | - | - | reject | weak FIT/VAL / drop |
| 786 | sweep | x_range_vs_avg20<=0.897434@SL1.0/T2.0 | 922/0.257 | 515/0.207 | - | - | reject | weak FIT/VAL / drop |
| 787 | sweep | x_range_vs_avg20>=1.213519@SL0.7/T1.5 | 2848/0.239 | 2088/0.179 | - | - | reject | weak FIT/VAL / drop |
| 788 | sweep | x_range_vs_avg20>=1.213519@SL1.0/T2.0 | 1946/0.255 | 1424/0.23 | - | - | reject | weak FIT/VAL / drop |
| 789 | sweep | x_range_vs_avg20<=1.213519@SL0.7/T1.5 | 1851/0.253 | 1266/0.206 | - | - | reject | weak FIT/VAL / drop |
| 790 | sweep | x_range_vs_avg20<=1.213519@SL1.0/T2.0 | 1441/0.255 | 991/0.224 | - | - | reject | weak FIT/VAL / drop |
| 791 | sweep | x_range_vs_avg20>=1.46949@SL0.7/T1.5 | 2601/0.239 | 1929/0.181 | - | - | reject | weak FIT/VAL / drop |
| 792 | sweep | x_range_vs_avg20>=1.46949@SL1.0/T2.0 | 1844/0.262 | 1369/0.228 | - | - | reject | weak FIT/VAL / drop |
| 793 | sweep | x_range_vs_avg20<=1.46949@SL0.7/T1.5 | 2269/0.256 | 1619/0.195 | - | - | reject | weak FIT/VAL / drop |
| 794 | sweep | x_range_vs_avg20<=1.46949@SL1.0/T2.0 | 1619/0.251 | 1171/0.203 | - | - | reject | weak FIT/VAL / drop |
| 795 | sweep | x_range_vs_avg20>=1.739128@SL0.7/T1.5 | 2222/0.223 | 1593/0.182 | - | - | reject | weak FIT/VAL / drop |
| 796 | sweep | x_range_vs_avg20>=1.739128@SL1.0/T2.0 | 1644/0.267 | 1195/0.234 | - | - | reject | weak FIT/VAL / drop |
| 797 | sweep | x_range_vs_avg20<=1.739128@SL0.7/T1.5 | 2542/0.258 | 1860/0.179 | - | - | reject | weak FIT/VAL / drop |
| 798 | sweep | x_range_vs_avg20<=1.739128@SL1.0/T2.0 | 1752/0.267 | 1302/0.221 | - | - | reject | weak FIT/VAL / drop |
| 799 | sweep | x_range_vs_avg20>=2.093972@SL0.7/T1.5 | 1187/0.242 | 804/0.228 | - | - | reject | weak FIT/VAL / drop |
| 800 | sweep | x_range_vs_avg20>=2.093972@SL1.0/T2.0 | 1085/0.288 | 763/0.25 | - | - | reject | weak FIT/VAL / drop |
| 801 | sweep | x_range_vs_avg20<=2.093972@SL0.7/T1.5 | 2779/0.246 | 2071/0.186 | - | - | reject | weak FIT/VAL / drop |
| 802 | sweep | x_range_vs_avg20<=2.093972@SL1.0/T2.0 | 1859/0.249 | 1399/0.223 | - | - | reject | weak FIT/VAL / drop |
| 803 | sweep | x_roc12>=0.076647@SL0.7/T1.5 | 2968/0.25 | 2208/0.196 | - | - | reject | weak FIT/VAL / drop |
| 804 | sweep | x_roc12>=0.076647@SL1.0/T2.0 | 1987/0.26 | 1480/0.226 | - | - | reject | weak FIT/VAL / drop |
| 805 | sweep | x_roc12<=0.076647@SL0.7/T1.5 | 1059/0.191 | 578/0.179 | - | - | reject | weak FIT/VAL / drop |
| 806 | sweep | x_roc12<=0.076647@SL1.0/T2.0 | 953/0.223 | 536/0.237 | - | - | reject | weak FIT/VAL / drop |
| 807 | sweep | x_roc12>=0.481811@SL0.7/T1.5 | 2942/0.254 | 2280/0.196 | - | - | reject | weak FIT/VAL / drop |
| 808 | sweep | x_roc12>=0.481811@SL1.0/T2.0 | 2047/0.258 | 1525/0.228 | - | - | reject | weak FIT/VAL / drop |
| 809 | sweep | x_roc12<=0.481811@SL0.7/T1.5 | 1665/0.239 | 1072/0.154 | - | - | reject | weak FIT/VAL / drop |
| 810 | sweep | x_roc12<=0.481811@SL1.0/T2.0 | 1258/0.243 | 802/0.16 | - | - | reject | weak FIT/VAL / drop |
| 811 | sweep | x_roc12>=0.877984@SL0.7/T1.5 | 2926/0.248 | 2244/0.191 | - | - | reject | weak FIT/VAL / drop |
| 812 | sweep | x_roc12>=0.877984@SL1.0/T2.0 | 2039/0.268 | 1558/0.224 | - | - | reject | weak FIT/VAL / drop |
| 813 | sweep | x_roc12<=0.877984@SL0.7/T1.5 | 1901/0.243 | 1316/0.172 | - | - | reject | weak FIT/VAL / drop |
| 814 | sweep | x_roc12<=0.877984@SL1.0/T2.0 | 1354/0.267 | 898/0.19 | - | - | reject | weak FIT/VAL / drop |
| 815 | sweep | x_roc12>=1.433582@SL0.7/T1.5 | 2520/0.253 | 1887/0.215 | - | - | reject | weak FIT/VAL / drop |
| 816 | sweep | x_roc12>=1.433582@SL1.0/T2.0 | 1926/0.301 | 1480/0.237 | - | - | reject | weak FIT/VAL / drop |
| 817 | sweep | x_roc12<=1.433582@SL0.7/T1.5 | 2164/0.237 | 1532/0.17 | - | - | reject | weak FIT/VAL / drop |
| 818 | sweep | x_roc12<=1.433582@SL1.0/T2.0 | 1498/0.255 | 1041/0.237 | - | - | reject | weak FIT/VAL / drop |
| 819 | sweep | x_roc12>=2.795961@SL0.7/T1.5 | 992/0.23 | 746/0.216 | - | - | reject | weak FIT/VAL / drop |
| 820 | sweep | x_roc12>=2.795961@SL1.0/T2.0 | 986/0.283 | 746/0.256 | - | - | reject | weak FIT/VAL / drop |
| 821 | sweep | x_roc12<=2.795961@SL0.7/T1.5 | 2524/0.255 | 1810/0.176 | - | - | reject | weak FIT/VAL / drop |
| 822 | sweep | x_roc12<=2.795961@SL1.0/T2.0 | 1667/0.25 | 1227/0.23 | - | - | reject | weak FIT/VAL / drop |
| 823 | sweep | x_roc3>=0.124639@SL0.7/T1.5 | 2984/0.244 | 2243/0.197 | - | - | reject | weak FIT/VAL / drop |
| 824 | sweep | x_roc3>=0.124639@SL1.0/T2.0 | 1992/0.254 | 1490/0.23 | - | - | reject | weak FIT/VAL / drop |
| 825 | sweep | x_roc3<=0.124639@SL0.7/T1.5 | 1027/0.212 | 545/0.179 | - | - | reject | weak FIT/VAL / drop |
| 826 | sweep | x_roc3<=0.124639@SL1.0/T2.0 | 916/0.226 | 503/0.176 | - | - | reject | weak FIT/VAL / drop |
| 827 | sweep | x_roc3>=0.323692@SL0.7/T1.5 | 3028/0.248 | 2274/0.194 | - | - | reject | weak FIT/VAL / drop |
| 828 | sweep | x_roc3>=0.323692@SL1.0/T2.0 | 2058/0.266 | 1503/0.227 | - | - | reject | weak FIT/VAL / drop |
| 829 | sweep | x_roc3<=0.323692@SL0.7/T1.5 | 1584/0.259 | 1060/0.183 | - | - | reject | weak FIT/VAL / drop |
| 830 | sweep | x_roc3<=0.323692@SL1.0/T2.0 | 1203/0.278 | 801/0.217 | - | - | reject | weak FIT/VAL / drop |
| 831 | sweep | x_roc3>=0.515922@SL0.7/T1.5 | 2987/0.253 | 2268/0.191 | - | - | reject | weak FIT/VAL / drop |
| 832 | sweep | x_roc3>=0.515922@SL1.0/T2.0 | 2064/0.29 | 1536/0.226 | - | - | reject | weak FIT/VAL / drop |
| 833 | sweep | x_roc3<=0.515922@SL0.7/T1.5 | 1820/0.246 | 1242/0.16 | - | - | reject | weak FIT/VAL / drop |
| 834 | sweep | x_roc3<=0.515922@SL1.0/T2.0 | 1338/0.268 | 875/0.206 | - | - | reject | weak FIT/VAL / drop |
| 835 | sweep | x_roc3>=0.793308@SL0.7/T1.5 | 2711/0.247 | 1969/0.197 | - | - | reject | weak FIT/VAL / drop |
| 836 | sweep | x_roc3>=0.793308@SL1.0/T2.0 | 2024/0.27 | 1508/0.234 | - | - | reject | weak FIT/VAL / drop |
| 837 | sweep | x_roc3<=0.793308@SL0.7/T1.5 | 2093/0.245 | 1471/0.178 | - | - | reject | weak FIT/VAL / drop |
| 838 | sweep | x_roc3<=0.793308@SL1.0/T2.0 | 1480/0.246 | 1019/0.219 | - | - | reject | weak FIT/VAL / drop |
| 839 | sweep | x_roc3>=1.436323@SL0.7/T1.5 | 1046/0.251 | 769/0.194 | - | - | reject | weak FIT/VAL / drop |
| 840 | sweep | x_roc3>=1.436323@SL1.0/T2.0 | 1044/0.278 | 769/0.225 | - | - | reject | weak FIT/VAL / drop |
| 841 | sweep | x_roc3<=1.436323@SL0.7/T1.5 | 2520/0.247 | 1830/0.197 | - | - | reject | weak FIT/VAL / drop |
| 842 | sweep | x_roc3<=1.436323@SL1.0/T2.0 | 1692/0.251 | 1208/0.242 | - | - | reject | weak FIT/VAL / drop |
| 843 | sweep | x_roc6>=0.097426@SL0.7/T1.5 | 2966/0.251 | 2244/0.193 | - | - | reject | weak FIT/VAL / drop |
| 844 | sweep | x_roc6>=0.097426@SL1.0/T2.0 | 1998/0.254 | 1483/0.232 | - | - | reject | weak FIT/VAL / drop |
| 845 | sweep | x_roc6<=0.097426@SL0.7/T1.5 | 1042/0.215 | 574/0.164 | - | - | reject | weak FIT/VAL / drop |
| 846 | sweep | x_roc6<=0.097426@SL1.0/T2.0 | 924/0.215 | 532/0.171 | - | - | reject | weak FIT/VAL / drop |
| 847 | sweep | x_roc6>=0.389912@SL0.7/T1.5 | 3005/0.259 | 2280/0.193 | - | - | reject | weak FIT/VAL / drop |
| 848 | sweep | x_roc6>=0.389912@SL1.0/T2.0 | 2088/0.278 | 1502/0.234 | - | - | reject | weak FIT/VAL / drop |
| 849 | sweep | x_roc6<=0.389912@SL0.7/T1.5 | 1612/0.233 | 1063/0.163 | - | - | reject | weak FIT/VAL / drop |
| 850 | sweep | x_roc6<=0.389912@SL1.0/T2.0 | 1192/0.25 | 811/0.202 | - | - | reject | weak FIT/VAL / drop |
| 851 | sweep | x_roc6>=0.667306@SL0.7/T1.5 | 2971/0.256 | 2230/0.191 | - | - | reject | weak FIT/VAL / drop |
| 852 | sweep | x_roc6>=0.667306@SL1.0/T2.0 | 2085/0.276 | 1526/0.237 | - | - | reject | weak FIT/VAL / drop |
| 853 | sweep | x_roc6<=0.667306@SL0.7/T1.5 | 1879/0.233 | 1296/0.174 | - | - | reject | weak FIT/VAL / drop |
| 854 | sweep | x_roc6<=0.667306@SL1.0/T2.0 | 1338/0.258 | 909/0.206 | - | - | reject | weak FIT/VAL / drop |
| 855 | sweep | x_roc6>=1.044175@SL0.7/T1.5 | 2628/0.242 | 1951/0.207 | - | - | reject | weak FIT/VAL / drop |
| 856 | sweep | x_roc6>=1.044175@SL1.0/T2.0 | 2006/0.269 | 1534/0.245 | - | - | reject | weak FIT/VAL / drop |
| 857 | sweep | x_roc6<=1.044175@SL0.7/T1.5 | 2146/0.243 | 1497/0.184 | - | - | reject | weak FIT/VAL / drop |
| 858 | sweep | x_roc6<=1.044175@SL1.0/T2.0 | 1486/0.275 | 1022/0.193 | - | - | reject | weak FIT/VAL / drop |
| 859 | sweep | x_roc6>=1.948039@SL0.7/T1.5 | 993/0.225 | 770/0.202 | - | - | reject | weak FIT/VAL / drop |
| 860 | sweep | x_roc6>=1.948039@SL1.0/T2.0 | 985/0.28 | 770/0.25 | - | - | reject | weak FIT/VAL / drop |
| 861 | sweep | x_roc6<=1.948039@SL0.7/T1.5 | 2540/0.25 | 1853/0.199 | - | - | reject | weak FIT/VAL / drop |
| 862 | sweep | x_roc6<=1.948039@SL1.0/T2.0 | 1716/0.241 | 1216/0.221 | - | - | reject | weak FIT/VAL / drop |
| 863 | sweep | x_rsi>=55.429169@SL0.7/T1.5 | 2917/0.243 | 2220/0.204 | - | - | reject | weak FIT/VAL / drop |
| 864 | sweep | x_rsi>=55.429169@SL1.0/T2.0 | 1979/0.253 | 1487/0.232 | - | - | reject | weak FIT/VAL / drop |
| 865 | sweep | x_rsi<=55.429169@SL0.7/T1.5 | 1056/0.216 | 523/0.163 | - | - | reject | weak FIT/VAL / drop |
| 866 | sweep | x_rsi<=55.429169@SL1.0/T2.0 | 905/0.222 | 484/0.198 | - | - | reject | weak FIT/VAL / drop |
| 867 | sweep | x_rsi>=61.964547@SL0.7/T1.5 | 2787/0.239 | 2182/0.2 | - | - | reject | weak FIT/VAL / drop |
| 868 | sweep | x_rsi>=61.964547@SL1.0/T2.0 | 1979/0.254 | 1497/0.234 | - | - | reject | weak FIT/VAL / drop |
| 869 | sweep | x_rsi<=61.964547@SL0.7/T1.5 | 1816/0.248 | 1116/0.147 | - | - | reject | weak FIT/VAL / drop |
| 870 | sweep | x_rsi<=61.964547@SL1.0/T2.0 | 1348/0.238 | 863/0.171 | - | - | reject | weak FIT/VAL / drop |
| 871 | sweep | x_rsi>=66.643372@SL0.7/T1.5 | 2580/0.239 | 2056/0.19 | - | - | reject | weak FIT/VAL / drop |
| 872 | sweep | x_rsi>=66.643372@SL1.0/T2.0 | 1872/0.259 | 1448/0.231 | - | - | reject | weak FIT/VAL / drop |
| 873 | sweep | x_rsi<=66.643372@SL0.7/T1.5 | 2169/0.261 | 1433/0.176 | - | - | reject | weak FIT/VAL / drop |
| 874 | sweep | x_rsi<=66.643372@SL1.0/T2.0 | 1522/0.266 | 1011/0.199 | - | - | reject | weak FIT/VAL / drop |
| 875 | sweep | x_rsi>=71.552615@SL0.7/T1.5 | 2145/0.232 | 1739/0.194 | - | - | reject | weak FIT/VAL / drop |
| 876 | sweep | x_rsi>=71.552615@SL1.0/T2.0 | 1644/0.264 | 1345/0.225 | - | - | reject | weak FIT/VAL / drop |
| 877 | sweep | x_rsi<=71.552615@SL0.7/T1.5 | 2430/0.258 | 1675/0.189 | - | - | reject | weak FIT/VAL / drop |
| 878 | sweep | x_rsi<=71.552615@SL1.0/T2.0 | 1646/0.28 | 1145/0.228 | - | - | reject | weak FIT/VAL / drop |
| 879 | sweep | x_rsi>=78.587126@SL0.7/T1.5 | 955/0.195 | 721/0.186 | - | - | reject | weak FIT/VAL / drop |
| 880 | sweep | x_rsi>=78.587126@SL1.0/T2.0 | 894/0.255 | 698/0.199 | - | - | reject | weak FIT/VAL / drop |
| 881 | sweep | x_rsi<=78.587126@SL0.7/T1.5 | 2741/0.258 | 1966/0.199 | - | - | reject | weak FIT/VAL / drop |
| 882 | sweep | x_rsi<=78.587126@SL1.0/T2.0 | 1782/0.263 | 1339/0.229 | - | - | reject | weak FIT/VAL / drop |
| 883 | sweep | x_rsi_slope3>=0.630971@SL0.7/T1.5 | 2920/0.251 | 2154/0.195 | - | - | reject | weak FIT/VAL / drop |
| 884 | sweep | x_rsi_slope3>=0.630971@SL1.0/T2.0 | 1917/0.27 | 1463/0.235 | - | - | reject | weak FIT/VAL / drop |
| 885 | sweep | x_rsi_slope3<=0.630971@SL0.7/T1.5 | 1044/0.217 | 617/0.208 | - | - | reject | weak FIT/VAL / drop |
| 886 | sweep | x_rsi_slope3<=0.630971@SL1.0/T2.0 | 944/0.241 | 593/0.193 | - | - | reject | weak FIT/VAL / drop |
| 887 | sweep | x_rsi_slope3>=4.528482@SL0.7/T1.5 | 2749/0.24 | 1989/0.186 | - | - | reject | weak FIT/VAL / drop |
| 888 | sweep | x_rsi_slope3>=4.528482@SL1.0/T2.0 | 1874/0.269 | 1389/0.223 | - | - | reject | weak FIT/VAL / drop |
| 889 | sweep | x_rsi_slope3<=4.528482@SL0.7/T1.5 | 1951/0.248 | 1373/0.19 | - | - | reject | weak FIT/VAL / drop |
| 890 | sweep | x_rsi_slope3<=4.528482@SL1.0/T2.0 | 1495/0.263 | 1059/0.23 | - | - | reject | weak FIT/VAL / drop |
| 891 | sweep | x_rsi_slope3>=7.159397@SL0.7/T1.5 | 2540/0.256 | 1836/0.18 | - | - | reject | weak FIT/VAL / drop |
| 892 | sweep | x_rsi_slope3>=7.159397@SL1.0/T2.0 | 1809/0.282 | 1305/0.215 | - | - | reject | weak FIT/VAL / drop |
| 893 | sweep | x_rsi_slope3<=7.159397@SL0.7/T1.5 | 2333/0.237 | 1711/0.182 | - | - | reject | weak FIT/VAL / drop |
| 894 | sweep | x_rsi_slope3<=7.159397@SL1.0/T2.0 | 1704/0.248 | 1206/0.199 | - | - | reject | weak FIT/VAL / drop |
| 895 | sweep | x_rsi_slope3>=10.076545@SL0.7/T1.5 | 2169/0.255 | 1575/0.159 | - | - | reject | weak FIT/VAL / drop |
| 896 | sweep | x_rsi_slope3>=10.076545@SL1.0/T2.0 | 1629/0.276 | 1157/0.19 | - | - | reject | weak FIT/VAL / drop |
| 897 | sweep | x_rsi_slope3<=10.076545@SL0.7/T1.5 | 2609/0.233 | 1914/0.194 | - | - | reject | weak FIT/VAL / drop |
| 898 | sweep | x_rsi_slope3<=10.076545@SL1.0/T2.0 | 1831/0.256 | 1364/0.228 | - | - | reject | weak FIT/VAL / drop |
| 899 | sweep | x_rsi_slope3>=15.283638@SL0.7/T1.5 | 1109/0.267 | 768/0.208 | - | - | reject | weak FIT/VAL / drop |
| 900 | sweep | x_rsi_slope3>=15.283638@SL1.0/T2.0 | 1033/0.268 | 725/0.247 | - | - | reject | weak FIT/VAL / drop |
| 901 | sweep | x_rsi_slope3<=15.283638@SL0.7/T1.5 | 2872/0.243 | 2156/0.185 | - | - | reject | weak FIT/VAL / drop |
| 902 | sweep | x_rsi_slope3<=15.283638@SL1.0/T2.0 | 1911/0.251 | 1452/0.218 | - | - | reject | weak FIT/VAL / drop |
| 903 | sweep | x_stoch_d>=46.327684@SL0.7/T1.5 | 2942/0.246 | 2233/0.197 | - | - | reject | weak FIT/VAL / drop |
| 904 | sweep | x_stoch_d>=46.327684@SL1.0/T2.0 | 1977/0.254 | 1488/0.231 | - | - | reject | weak FIT/VAL / drop |
| 905 | sweep | x_stoch_d<=46.327684@SL0.7/T1.5 | 1083/0.218 | 590/0.164 | - | - | reject | weak FIT/VAL / drop |
| 906 | sweep | x_stoch_d<=46.327684@SL1.0/T2.0 | 964/0.223 | 542/0.191 | - | - | reject | weak FIT/VAL / drop |
| 907 | sweep | x_stoch_d>=72.661691@SL0.7/T1.5 | 2821/0.252 | 2167/0.189 | - | - | reject | weak FIT/VAL / drop |
| 908 | sweep | x_stoch_d>=72.661691@SL1.0/T2.0 | 1951/0.245 | 1457/0.225 | - | - | reject | weak FIT/VAL / drop |
| 909 | sweep | x_stoch_d<=72.661691@SL0.7/T1.5 | 1878/0.241 | 1285/0.191 | - | - | reject | weak FIT/VAL / drop |
| 910 | sweep | x_stoch_d<=72.661691@SL1.0/T2.0 | 1394/0.251 | 975/0.2 | - | - | reject | weak FIT/VAL / drop |
| 911 | sweep | x_stoch_d>=84.469348@SL0.7/T1.5 | 2517/0.241 | 1863/0.187 | - | - | reject | weak FIT/VAL / drop |
| 912 | sweep | x_stoch_d>=84.469348@SL1.0/T2.0 | 1800/0.272 | 1354/0.248 | - | - | reject | weak FIT/VAL / drop |
| 913 | sweep | x_stoch_d<=84.469348@SL0.7/T1.5 | 2277/0.26 | 1665/0.192 | - | - | reject | weak FIT/VAL / drop |
| 914 | sweep | x_stoch_d<=84.469348@SL1.0/T2.0 | 1607/0.255 | 1141/0.229 | - | - | reject | weak FIT/VAL / drop |
| 915 | sweep | x_stoch_d>=91.221231@SL0.7/T1.5 | 2059/0.235 | 1503/0.176 | - | - | reject | weak FIT/VAL / drop |
| 916 | sweep | x_stoch_d>=91.221231@SL1.0/T2.0 | 1581/0.273 | 1169/0.224 | - | - | reject | weak FIT/VAL / drop |
| 917 | sweep | x_stoch_d<=91.221231@SL0.7/T1.5 | 2609/0.246 | 1862/0.193 | - | - | reject | weak FIT/VAL / drop |
| 918 | sweep | x_stoch_d<=91.221231@SL1.0/T2.0 | 1760/0.272 | 1280/0.215 | - | - | reject | weak FIT/VAL / drop |
| 919 | sweep | x_stoch_d>=96.081686@SL0.7/T1.5 | 986/0.225 | 529/0.2 | - | - | reject | weak FIT/VAL / drop |
| 920 | sweep | x_stoch_d>=96.081686@SL1.0/T2.0 | 926/0.246 | 520/0.228 | - | - | reject | weak FIT/VAL / drop |
| 921 | sweep | x_stoch_d<=96.081686@SL0.7/T1.5 | 2863/0.24 | 2137/0.195 | - | - | reject | weak FIT/VAL / drop |
| 922 | sweep | x_stoch_d<=96.081686@SL1.0/T2.0 | 1902/0.249 | 1440/0.22 | - | - | reject | weak FIT/VAL / drop |
| 923 | sweep | x_stoch_k>=68.21665@SL0.7/T1.5 | 2961/0.242 | 2236/0.196 | - | - | reject | weak FIT/VAL / drop |
| 924 | sweep | x_stoch_k>=68.21665@SL1.0/T2.0 | 1994/0.244 | 1494/0.227 | - | - | reject | weak FIT/VAL / drop |
| 925 | sweep | x_stoch_k<=68.21665@SL0.7/T1.5 | 1100/0.215 | 572/0.159 | - | - | reject | weak FIT/VAL / drop |
| 926 | sweep | x_stoch_k<=68.21665@SL1.0/T2.0 | 974/0.246 | 537/0.179 | - | - | reject | weak FIT/VAL / drop |
| 927 | sweep | x_stoch_k>=91.836735@SL0.7/T1.5 | 2860/0.252 | 2118/0.185 | - | - | reject | weak FIT/VAL / drop |
| 928 | sweep | x_stoch_k>=91.836735@SL1.0/T2.0 | 1953/0.264 | 1428/0.226 | - | - | reject | weak FIT/VAL / drop |
| 929 | sweep | x_stoch_k<=91.836735@SL0.7/T1.5 | 1870/0.253 | 1326/0.18 | - | - | reject | weak FIT/VAL / drop |
| 930 | sweep | x_stoch_k<=91.836735@SL1.0/T2.0 | 1419/0.27 | 1011/0.203 | - | - | reject | weak FIT/VAL / drop |
| 931 | sweep | x_stoch_k>=95.683636@SL0.7/T1.5 | 2520/0.239 | 1826/0.194 | - | - | reject | weak FIT/VAL / drop |
| 932 | sweep | x_stoch_k>=95.683636@SL1.0/T2.0 | 1817/0.25 | 1309/0.245 | - | - | reject | weak FIT/VAL / drop |
| 933 | sweep | x_stoch_k<=95.683636@SL0.7/T1.5 | 2350/0.258 | 1707/0.177 | - | - | reject | weak FIT/VAL / drop |
| 934 | sweep | x_stoch_k<=95.683636@SL1.0/T2.0 | 1653/0.275 | 1194/0.218 | - | - | reject | weak FIT/VAL / drop |
| 935 | sweep | x_stoch_k>=98.223257@SL0.7/T1.5 | 2013/0.252 | 1452/0.179 | - | - | reject | weak FIT/VAL / drop |
| 936 | sweep | x_stoch_k>=98.223257@SL1.0/T2.0 | 1528/0.244 | 1127/0.235 | - | - | reject | weak FIT/VAL / drop |
| 937 | sweep | x_stoch_k<=98.223257@SL0.7/T1.5 | 2688/0.242 | 1914/0.192 | - | - | reject | weak FIT/VAL / drop |
| 938 | sweep | x_stoch_k<=98.223257@SL1.0/T2.0 | 1858/0.253 | 1338/0.239 | - | - | reject | weak FIT/VAL / drop |
| 939 | sweep | x_stoch_k>=100.0@SL0.7/T1.5 | 1621/0.252 | 1096/0.19 | - | - | reject | weak FIT/VAL / drop |
| 940 | sweep | x_stoch_k>=100.0@SL1.0/T2.0 | 1299/0.244 | 914/0.238 | - | - | reject | weak FIT/VAL / drop |
| 941 | sweep | x_stoch_k<=100.0@SL0.7/T1.5 | 2980/0.244 | 2208/0.196 | - | - | reject | weak FIT/VAL / drop |
| 942 | sweep | x_stoch_k<=100.0@SL1.0/T2.0 | 1963/0.25 | 1488/0.228 | - | - | reject | weak FIT/VAL / drop |
| 943 | sweep | x_svwap_dist_atr>=0.83474@SL0.7/T1.5 | 2953/0.237 | 2194/0.204 | - | - | reject | weak FIT/VAL / drop |
| 944 | sweep | x_svwap_dist_atr>=0.83474@SL1.0/T2.0 | 2023/0.26 | 1478/0.234 | - | - | reject | weak FIT/VAL / drop |
| 945 | sweep | x_svwap_dist_atr<=0.83474@SL0.7/T1.5 | 1120/0.204 | 629/0.163 | - | - | reject | weak FIT/VAL / drop |
| 946 | sweep | x_svwap_dist_atr<=0.83474@SL1.0/T2.0 | 974/0.224 | 583/0.186 | - | - | reject | weak FIT/VAL / drop |
| 947 | sweep | x_svwap_dist_atr>=1.865662@SL0.7/T1.5 | 2619/0.251 | 2005/0.197 | - | - | reject | weak FIT/VAL / drop |
| 948 | sweep | x_svwap_dist_atr>=1.865662@SL1.0/T2.0 | 1878/0.248 | 1390/0.259 | - | - | reject | weak FIT/VAL / drop |
| 949 | sweep | x_svwap_dist_atr<=1.865662@SL0.7/T1.5 | 2104/0.231 | 1442/0.165 | - | - | reject | weak FIT/VAL / drop |
| 950 | sweep | x_svwap_dist_atr<=1.865662@SL1.0/T2.0 | 1534/0.259 | 1102/0.197 | - | - | reject | weak FIT/VAL / drop |
| 951 | sweep | x_svwap_dist_atr>=2.767613@SL0.7/T1.5 | 2173/0.253 | 1646/0.197 | - | - | reject | weak FIT/VAL / drop |
| 952 | sweep | x_svwap_dist_atr>=2.767613@SL1.0/T2.0 | 1570/0.273 | 1173/0.241 | - | - | reject | weak FIT/VAL / drop |
| 953 | sweep | x_svwap_dist_atr<=2.767613@SL0.7/T1.5 | 2514/0.256 | 1798/0.184 | - | - | reject | weak FIT/VAL / drop |
| 954 | sweep | x_svwap_dist_atr<=2.767613@SL1.0/T2.0 | 1768/0.266 | 1283/0.196 | - | - | reject | weak FIT/VAL / drop |
| 955 | sweep | x_svwap_dist_atr>=3.818101@SL0.7/T1.5 | 1612/0.265 | 1194/0.196 | - | - | reject | weak FIT/VAL / drop |
| 956 | sweep | x_svwap_dist_atr>=3.818101@SL1.0/T2.0 | 1252/0.3 | 896/0.25 | - | - | reject | weak FIT/VAL / drop |
| 957 | sweep | x_svwap_dist_atr<=3.818101@SL0.7/T1.5 | 2754/0.247 | 2009/0.198 | - | - | reject | weak FIT/VAL / drop |
| 958 | sweep | x_svwap_dist_atr<=3.818101@SL1.0/T2.0 | 1882/0.25 | 1403/0.214 | - | - | reject | weak FIT/VAL / drop |
| 959 | sweep | x_svwap_dist_atr>=5.55542@SL0.7/T1.5 | 854/0.265 | 484/0.197 | - | - | reject | weak FIT/VAL / drop |
| 960 | sweep | x_svwap_dist_atr>=5.55542@SL1.0/T2.0 | 718/0.32 | 439/0.262 | - | - | reject | weak FIT/VAL / drop |
| 961 | sweep | x_svwap_dist_atr<=5.55542@SL0.7/T1.5 | 2944/0.241 | 2193/0.196 | - | - | reject | weak FIT/VAL / drop |
| 962 | sweep | x_svwap_dist_atr<=5.55542@SL1.0/T2.0 | 1943/0.252 | 1463/0.223 | - | - | reject | weak FIT/VAL / drop |
| 963 | sweep | x_svwap_dist_pct>=0.236267@SL0.7/T1.5 | 3006/0.238 | 2241/0.2 | - | - | reject | weak FIT/VAL / drop |
| 964 | sweep | x_svwap_dist_pct>=0.236267@SL1.0/T2.0 | 2031/0.255 | 1493/0.235 | - | - | reject | weak FIT/VAL / drop |
| 965 | sweep | x_svwap_dist_pct<=0.236267@SL0.7/T1.5 | 1036/0.202 | 598/0.144 | - | - | reject | weak FIT/VAL / drop |
| 966 | sweep | x_svwap_dist_pct<=0.236267@SL1.0/T2.0 | 883/0.222 | 541/0.185 | - | - | reject | weak FIT/VAL / drop |
| 967 | sweep | x_svwap_dist_pct>=0.609154@SL0.7/T1.5 | 2989/0.246 | 2308/0.202 | - | - | reject | weak FIT/VAL / drop |
| 968 | sweep | x_svwap_dist_pct>=0.609154@SL1.0/T2.0 | 2081/0.262 | 1543/0.247 | - | - | reject | weak FIT/VAL / drop |
| 969 | sweep | x_svwap_dist_pct<=0.609154@SL0.7/T1.5 | 1656/0.246 | 1122/0.141 | - | - | reject | weak FIT/VAL / drop |
| 970 | sweep | x_svwap_dist_pct<=0.609154@SL1.0/T2.0 | 1231/0.26 | 840/0.161 | - | - | reject | weak FIT/VAL / drop |
| 971 | sweep | x_svwap_dist_pct>=0.968169@SL0.7/T1.5 | 2823/0.242 | 2198/0.196 | - | - | reject | weak FIT/VAL / drop |
| 972 | sweep | x_svwap_dist_pct>=0.968169@SL1.0/T2.0 | 2049/0.267 | 1587/0.234 | - | - | reject | weak FIT/VAL / drop |
| 973 | sweep | x_svwap_dist_pct<=0.968169@SL0.7/T1.5 | 1978/0.255 | 1410/0.171 | - | - | reject | weak FIT/VAL / drop |
| 974 | sweep | x_svwap_dist_pct<=0.968169@SL1.0/T2.0 | 1401/0.279 | 982/0.197 | - | - | reject | weak FIT/VAL / drop |
| 975 | sweep | x_svwap_dist_pct>=1.437949@SL0.7/T1.5 | 2277/0.243 | 1753/0.197 | - | - | reject | weak FIT/VAL / drop |
| 976 | sweep | x_svwap_dist_pct>=1.437949@SL1.0/T2.0 | 1818/0.284 | 1474/0.242 | - | - | reject | weak FIT/VAL / drop |
| 977 | sweep | x_svwap_dist_pct<=1.437949@SL0.7/T1.5 | 2281/0.241 | 1626/0.196 | - | - | reject | weak FIT/VAL / drop |
| 978 | sweep | x_svwap_dist_pct<=1.437949@SL1.0/T2.0 | 1564/0.267 | 1128/0.217 | - | - | reject | weak FIT/VAL / drop |
| 979 | sweep | x_svwap_dist_pct>=2.347151@SL0.7/T1.5 | 1051/0.233 | 675/0.227 | - | - | reject | weak FIT/VAL / drop |
| 980 | sweep | x_svwap_dist_pct>=2.347151@SL1.0/T2.0 | 970/0.295 | 675/0.262 | - | - | reject | weak FIT/VAL / drop |
| 981 | sweep | x_svwap_dist_pct<=2.347151@SL0.7/T1.5 | 2649/0.244 | 1889/0.193 | - | - | reject | weak FIT/VAL / drop |
| 982 | sweep | x_svwap_dist_pct<=2.347151@SL1.0/T2.0 | 1749/0.259 | 1281/0.217 | - | - | reject | weak FIT/VAL / drop |
| 983 | sweep | x_vol_vs_avg20>=1.545458@SL0.7/T1.5 | 2921/0.242 | 2174/0.195 | - | - | reject | weak FIT/VAL / drop |
| 984 | sweep | x_vol_vs_avg20>=1.545458@SL1.0/T2.0 | 1940/0.251 | 1472/0.239 | - | - | reject | weak FIT/VAL / drop |
| 985 | sweep | x_vol_vs_avg20<=1.545458@SL0.7/T1.5 | 909/0.23 | 503/0.131 | - | - | reject | weak FIT/VAL / drop |
| 986 | sweep | x_vol_vs_avg20<=1.545458@SL1.0/T2.0 | 864/0.235 | 501/0.132 | - | - | reject | weak FIT/VAL / drop |
| 987 | sweep | x_vol_vs_avg20>=1.824573@SL0.7/T1.5 | 2786/0.245 | 2047/0.192 | - | - | reject | weak FIT/VAL / drop |
| 988 | sweep | x_vol_vs_avg20>=1.824573@SL1.0/T2.0 | 1897/0.267 | 1417/0.234 | - | - | reject | weak FIT/VAL / drop |
| 989 | sweep | x_vol_vs_avg20<=1.824573@SL0.7/T1.5 | 1877/0.22 | 1312/0.156 | - | - | reject | weak FIT/VAL / drop |
| 990 | sweep | x_vol_vs_avg20<=1.824573@SL1.0/T2.0 | 1463/0.22 | 1041/0.182 | - | - | reject | weak FIT/VAL / drop |
| 991 | sweep | x_vol_vs_avg20>=2.183946@SL0.7/T1.5 | 2595/0.248 | 1889/0.184 | - | - | reject | weak FIT/VAL / drop |
| 992 | sweep | x_vol_vs_avg20>=2.183946@SL1.0/T2.0 | 1818/0.272 | 1342/0.23 | - | - | reject | weak FIT/VAL / drop |
| 993 | sweep | x_vol_vs_avg20<=2.183946@SL0.7/T1.5 | 2294/0.237 | 1679/0.173 | - | - | reject | weak FIT/VAL / drop |
| 994 | sweep | x_vol_vs_avg20<=2.183946@SL1.0/T2.0 | 1646/0.243 | 1184/0.21 | - | - | reject | weak FIT/VAL / drop |
| 995 | sweep | x_vol_vs_avg20>=2.730915@SL0.7/T1.5 | 2199/0.281 | 1621/0.208 | - | - | reject | weak FIT/VAL / drop |
| 996 | sweep | x_vol_vs_avg20>=2.730915@SL1.0/T2.0 | 1648/0.302 | 1196/0.221 | - | - | reject | weak FIT/VAL / drop |
| 997 | sweep | x_vol_vs_avg20<=2.730915@SL0.7/T1.5 | 2605/0.23 | 1877/0.165 | - | - | reject | weak FIT/VAL / drop |
| 998 | sweep | x_vol_vs_avg20<=2.730915@SL1.0/T2.0 | 1771/0.249 | 1310/0.204 | - | - | reject | weak FIT/VAL / drop |
| 999 | sweep | x_vol_vs_avg20>=3.905312@SL0.7/T1.5 | 1339/0.309 | 911/0.191 | - | - | reject | weak FIT/VAL / drop |
| 1000 | sweep | x_vol_vs_avg20>=3.905312@SL1.0/T2.0 | 1158/0.354 | 803/0.199 | - | - | reject | weak FIT/VAL / drop |
| 1001 | sweep | x_vol_vs_avg20<=3.905312@SL0.7/T1.5 | 2848/0.231 | 2081/0.192 | - | - | reject | weak FIT/VAL / drop |
| 1002 | sweep | x_vol_vs_avg20<=3.905312@SL1.0/T2.0 | 1887/0.243 | 1410/0.222 | - | - | reject | weak FIT/VAL / drop |
| 1003 | sweep | x_willr>=-31.78335@SL0.7/T1.5 | 2961/0.242 | 2236/0.196 | - | - | reject | weak FIT/VAL / drop |
| 1004 | sweep | x_willr>=-31.78335@SL1.0/T2.0 | 1994/0.244 | 1494/0.227 | - | - | reject | weak FIT/VAL / drop |
| 1005 | sweep | x_willr<=-31.78335@SL0.7/T1.5 | 1100/0.215 | 572/0.159 | - | - | reject | weak FIT/VAL / drop |
| 1006 | sweep | x_willr<=-31.78335@SL1.0/T2.0 | 974/0.246 | 537/0.179 | - | - | reject | weak FIT/VAL / drop |
| 1007 | sweep | x_willr>=-8.163265@SL0.7/T1.5 | 2860/0.252 | 2118/0.185 | - | - | reject | weak FIT/VAL / drop |
| 1008 | sweep | x_willr>=-8.163265@SL1.0/T2.0 | 1953/0.264 | 1428/0.226 | - | - | reject | weak FIT/VAL / drop |
| 1009 | sweep | x_willr<=-8.163265@SL0.7/T1.5 | 1870/0.253 | 1326/0.18 | - | - | reject | weak FIT/VAL / drop |
| 1010 | sweep | x_willr<=-8.163265@SL1.0/T2.0 | 1419/0.27 | 1011/0.203 | - | - | reject | weak FIT/VAL / drop |
| 1011 | sweep | x_willr>=-4.316364@SL0.7/T1.5 | 2520/0.239 | 1826/0.194 | - | - | reject | weak FIT/VAL / drop |
| 1012 | sweep | x_willr>=-4.316364@SL1.0/T2.0 | 1817/0.25 | 1309/0.245 | - | - | reject | weak FIT/VAL / drop |
| 1013 | sweep | x_willr<=-4.316364@SL0.7/T1.5 | 2350/0.258 | 1707/0.177 | - | - | reject | weak FIT/VAL / drop |
| 1014 | sweep | x_willr<=-4.316364@SL1.0/T2.0 | 1653/0.275 | 1194/0.218 | - | - | reject | weak FIT/VAL / drop |
| 1015 | sweep | x_willr>=-1.776743@SL0.7/T1.5 | 2013/0.252 | 1452/0.179 | - | - | reject | weak FIT/VAL / drop |
| 1016 | sweep | x_willr>=-1.776743@SL1.0/T2.0 | 1528/0.244 | 1127/0.235 | - | - | reject | weak FIT/VAL / drop |
| 1017 | sweep | x_willr<=-1.776743@SL0.7/T1.5 | 2688/0.242 | 1914/0.192 | - | - | reject | weak FIT/VAL / drop |
| 1018 | sweep | x_willr<=-1.776743@SL1.0/T2.0 | 1858/0.253 | 1338/0.239 | - | - | reject | weak FIT/VAL / drop |
| 1019 | sweep | x_willr>=-0.0@SL0.7/T1.5 | 1621/0.252 | 1096/0.19 | - | - | reject | weak FIT/VAL / drop |
| 1020 | sweep | x_willr>=-0.0@SL1.0/T2.0 | 1299/0.244 | 914/0.238 | - | - | reject | weak FIT/VAL / drop |
| 1021 | sweep | x_willr<=-0.0@SL0.7/T1.5 | 2980/0.244 | 2208/0.196 | - | - | reject | weak FIT/VAL / drop |
| 1022 | sweep | x_willr<=-0.0@SL1.0/T2.0 | 1963/0.25 | 1488/0.228 | - | - | reject | weak FIT/VAL / drop |
| 1023 | sweep | x_ema20_gt50>=0.5@SL0.7/T1.5 | 2740/0.229 | 2158/0.198 | - | - | reject | weak FIT/VAL / drop |
| 1024 | sweep | x_ema20_gt50>=0.5@SL1.0/T2.0 | 1927/0.244 | 1505/0.229 | - | - | reject | weak FIT/VAL / drop |
| 1025 | sweep | x_ema20_gt50<=0.5@SL0.7/T1.5 | 1263/0.277 | 732/0.178 | - | - | reject | weak FIT/VAL / drop |
| 1026 | sweep | x_ema20_gt50<=0.5@SL1.0/T2.0 | 990/0.282 | 632/0.181 | - | - | reject | weak FIT/VAL / drop |
| 1027 | sweep | x_ema_stack>=0.5@SL0.7/T1.5 | 2461/0.212 | 2031/0.202 | - | - | reject | weak FIT/VAL / drop |
| 1028 | sweep | x_ema_stack>=0.5@SL1.0/T2.0 | 1769/0.225 | 1451/0.237 | - | - | reject | weak FIT/VAL / drop |
| 1029 | sweep | x_ema_stack<=0.5@SL0.7/T1.5 | 1855/0.281 | 1316/0.161 | - | - | reject | weak FIT/VAL / drop |
| 1030 | sweep | x_ema_stack<=0.5@SL1.0/T2.0 | 1375/0.292 | 971/0.187 | - | - | reject | weak FIT/VAL / drop |
| 1031 | sweep | x_macd_above_sig>=0.5@SL0.7/T1.5 | 2898/0.257 | 2179/0.194 | - | - | reject | weak FIT/VAL / drop |
| 1032 | sweep | x_macd_above_sig>=0.5@SL1.0/T2.0 | 1989/0.256 | 1427/0.22 | - | - | reject | weak FIT/VAL / drop |
| 1033 | sweep | x_macd_above_sig<=0.5@SL0.7/T1.5 | 1494/0.21 | 1033/0.198 | - | - | reject | weak FIT/VAL / drop |
| 1034 | sweep | x_macd_above_sig<=0.5@SL1.0/T2.0 | 1162/0.237 | 817/0.225 | - | - | reject | weak FIT/VAL / drop |
| 1035 | sweep | x_above_pdh>=0.5@SL0.7/T1.5 | 2356/0.199 | 1988/0.193 | - | - | reject | weak FIT/VAL / drop |
| 1036 | sweep | x_above_pdh>=0.5@SL1.0/T2.0 | 1763/0.219 | 1464/0.223 | - | - | reject | weak FIT/VAL / drop |
| 1037 | sweep | x_above_pdh<=0.5@SL0.7/T1.5 | 2106/0.273 | 1458/0.181 | - | - | reject | weak FIT/VAL / drop |
| 1038 | sweep | x_above_pdh<=0.5@SL1.0/T2.0 | 1454/0.292 | 1026/0.225 | - | - | reject | weak FIT/VAL / drop |
| 1039 | sweep | x_fresh_break>=0.5@SL0.7/T1.5 | 2816/0.242 | 2065/0.187 | - | - | reject | weak FIT/VAL / drop |
| 1040 | sweep | x_fresh_break>=0.5@SL1.0/T2.0 | 1891/0.246 | 1441/0.244 | - | - | reject | weak FIT/VAL / drop |
| 1041 | sweep | x_fresh_break<=0.5@SL0.7/T1.5 | 1726/0.226 | 1187/0.191 | - | - | reject | weak FIT/VAL / drop |
| 1042 | sweep | x_fresh_break<=0.5@SL1.0/T2.0 | 1375/0.261 | 968/0.204 | - | - | reject | weak FIT/VAL / drop |
| 1043 | sweep | x_prev_pullback>=0.5@SL0.7/T1.5 | 2579/0.243 | 1865/0.195 | - | - | reject | weak FIT/VAL / drop |
| 1044 | sweep | x_prev_pullback>=0.5@SL1.0/T2.0 | 1809/0.243 | 1293/0.221 | - | - | reject | weak FIT/VAL / drop |
| 1045 | sweep | x_prev_pullback<=0.5@SL0.7/T1.5 | 2316/0.249 | 1665/0.176 | - | - | reject | weak FIT/VAL / drop |
| 1046 | sweep | x_prev_pullback<=0.5@SL1.0/T2.0 | 1686/0.298 | 1227/0.216 | - | - | reject | weak FIT/VAL / drop |
| 1047 | sweep | x_first_break_of_day>=0.5@SL0.7/T1.5 | 2704/0.244 | 1946/0.185 | - | - | reject | weak FIT/VAL / drop |
| 1048 | sweep | x_first_break_of_day>=0.5@SL1.0/T2.0 | 1786/0.265 | 1306/0.226 | - | - | reject | weak FIT/VAL / drop |
| 1049 | sweep | x_first_break_of_day<=0.5@SL0.7/T1.5 | 2012/0.242 | 1449/0.18 | - | - | reject | weak FIT/VAL / drop |
| 1050 | sweep | x_first_break_of_day<=0.5@SL1.0/T2.0 | 1515/0.259 | 1126/0.196 | - | - | reject | weak FIT/VAL / drop |
| 1051 | sweep | x_reg_bull>=0.5@SL0.7/T1.5 | 1152/0.206 | 961/0.175 | - | - | reject | weak FIT/VAL / drop |
| 1052 | sweep | x_reg_bull>=0.5@SL1.0/T2.0 | 800/0.257 | 689/0.208 | - | - | reject | weak FIT/VAL / drop |
| 1053 | sweep | x_reg_bull<=0.5@SL0.7/T1.5 | 2245/0.258 | 1663/0.202 | - | - | reject | weak FIT/VAL / drop |
| 1054 | sweep | x_reg_bull<=0.5@SL1.0/T2.0 | 1559/0.257 | 1216/0.233 | - | - | reject | weak FIT/VAL / drop |
| 1055 | sweep | x_reg_notbear>=0.5@SL0.7/T1.5 | 2111/0.239 | 1773/0.196 | - | - | reject | weak FIT/VAL / drop |
| 1056 | sweep | x_reg_notbear>=0.5@SL1.0/T2.0 | 1435/0.26 | 1173/0.236 | - | - | reject | weak FIT/VAL / drop |
| 1057 | sweep | x_reg_notbear<=0.5@SL0.7/T1.5 | 1369/0.223 | 766/0.22 | - | - | reject | weak FIT/VAL / drop |
| 1058 | sweep | x_reg_notbear<=0.5@SL1.0/T2.0 | 1016/0.218 | 594/0.266 | - | - | reject | weak FIT/VAL / drop |
| 1059 | sweep | x_reg_bulltrend>=0.5@SL0.7/T1.5 | 1591/0.223 | 961/0.175 | - | - | reject | weak FIT/VAL / drop |
| 1060 | sweep | x_reg_bulltrend>=0.5@SL1.0/T2.0 | 1128/0.26 | 689/0.208 | - | - | reject | weak FIT/VAL / drop |
| 1061 | sweep | x_reg_bulltrend<=0.5@SL0.7/T1.5 | 2095/0.24 | 1663/0.202 | - | - | reject | weak FIT/VAL / drop |
| 1062 | sweep | x_reg_bulltrend<=0.5@SL1.0/T2.0 | 1518/0.238 | 1216/0.233 | - | - | reject | weak FIT/VAL / drop |
| 1063 | combo-tpe | SL0.85/T1.5 mask[lower_wick_pct>=0.226032;x_reg_notbear>=0.5] g{"min_slot": "12:00", "max_slot": "13:30", "top_n": 1} | 23/0.881 | 30/0.873 | - | - | reject | band score <= 0.9 / drop |
| 1064 | combo-tpe | SL0.7/T1.5 mask[lower_wick_pct>=0.226032;x_reg_notbear>=0.5] g{"min_slot": "12:00", "max_slot": "13:30", "top_n": 1} | 23/1.008 | 30/0.844 | - | - | reject | band score <= 0.9 / drop |
| 1065 | combo-tpe | SL1.2/T1.5 mask[lower_wick_pct>=0.226032;x_reg_notbear>=0.5] g{"min_slot": "12:00", "max_slot": "13:30", "top_n": 1} | 23/0.933 | 30/0.798 | - | - | reject | band score <= 0.9 / drop |
| 1066 | combo-tpe | SL1.2/T2.0 mask[lower_wick_pct>=0.226032] g{"min_slot": "10:30", "max_slot": "13:30", "top_n": 1} | 113/0.675 | 87/0.683 | - | - | reject | band score <= 0.9 / drop |
| 1067 | combo-tpe | SL1.2/T2.0 mask[lower_wick_pct>=0.226032;x_prev_pullback>=0.5] g{"min_slot": "10:30", "max_slot": "13:30", "top_n": 1} | 74/0.67 | 50/0.674 | - | - | reject | band score <= 0.9 / drop |
| 1068 | combo-tpe | SL0.85/T1.5 mask[lower_wick_pct>=0.226032;x_fresh_break>=0.5] g{"min_slot": "12:00", "max_slot": "13:30", "top_n": 1} | 42/0.685 | 23/0.719 | - | - | reject | band score <= 0.9 / drop |
| 1069 | combo-tpe | SL1.2/T1.5 mask[lower_wick_pct>=0.226032;x_prev_pullback>=0.5] g{"min_slot": "12:00", "max_slot": "13:30", "top_n": 1} | 34/0.69 | 17/0.671 | - | - | reject | band score <= 0.9 / drop |
| 1070 | combo-tpe | SL0.85/T1.5 mask[lower_wick_pct>=0.226032;x_macd_above_sig>=0.5] g{"min_slot": "12:00", "max_slot": "13:30", "top_n": 1} | 36/0.711 | 29/0.802 | - | - | reject | band score <= 0.9 / drop |
| 1071 | combo-tpe | SL1.2/T2.0 mask[x_gap_pct>=1.588235] g{"min_slot": "10:30", "max_slot": "13:30", "top_n": 1} | 126/0.723 | 49/0.671 | - | - | reject | band score <= 0.9 / drop |
| 1072 | combo-tpe | SL1.2/T2.0 mask[lower_wick_pct>=0.226032;x_prev_pullback>=0.5] g{"min_slot": "10:30", "max_slot": "13:30", "top_n": 2} | 124/0.626 | 86/0.647 | - | - | reject | band score <= 0.9 / drop |
| 1073 | combo-tpe | SL0.7/T1.5 mask[lower_wick_pct>=0.226032;x_prev_pullback>=0.5] g{"min_slot": "12:00", "max_slot": "13:30", "top_n": 1} | 34/0.661 | 17/0.743 | - | - | reject | band score <= 0.9 / drop |
| 1074 | combo-tpe | SL0.4/T1.5 mask[lower_wick_pct>=0.226032;x_reg_notbear>=0.5] g{"min_slot": "12:00", "max_slot": "13:30", "top_n": 1} | 23/0.598 | 30/0.609 | - | - | reject | band score <= 0.9 / drop |
| 1075 | combo-tpe | SL1.2/T2.0 mask[x_gap_pct>=1.588235] g{"min_slot": "10:30", "top_n": 1} | 158/0.637 | 57/0.61 | - | - | reject | band score <= 0.9 / drop |
| 1076 | combo-tpe | SL1.2/T2.0 mask[x_cci20>=161.592765] g{"min_slot": "10:30", "max_slot": "12:30", "top_n": 1} | 144/0.64 | 131/0.706 | - | - | reject | band score <= 0.9 / drop |
| 1077 | combo-tpe | SL1.2/T2.0 mask[x_gap_pct>=1.588235] g{"min_slot": "11:00", "top_n": 1} | 136/0.618 | 48/0.662 | - | - | reject | band score <= 0.9 / drop |
| 1078 | combo-tpe | SL1.2/T2.0 mask[x_cci20>=136.810781] g{"min_slot": "10:30", "max_slot": "12:30", "top_n": 1} | 219/0.592 | 189/0.585 | - | - | reject | band score <= 0.9 / drop |
| 1079 | combo-tpe | SL1.2/T1.5 mask[x_cci20>=161.592765] g{"min_slot": "10:30", "max_slot": "12:30", "top_n": 1} | 144/0.586 | 131/0.594 | - | - | reject | band score <= 0.9 / drop |
| 1080 | combo-tpe | SL1.2/T2.0 mask[upper_wick_pct>=0.094678] g{"max_slot": "12:30", "top_n": 1} | 273/0.579 | 193/0.583 | - | - | reject | band score <= 0.9 / drop |
| 1081 | combo-tpe | SL1.2/T2.5 mask[x_cci20>=161.592765] g{"min_slot": "10:30", "max_slot": "12:30", "top_n": 1} | 144/0.7 | 131/0.627 | - | - | reject | band score <= 0.9 / drop |
| 1082 | combo-tpe | SL1.0/T2.5 mask[x_obv_slope5>=6.219594;atr_pct<=0.001887] g{"max_slot": "12:30", "top_n": 1} | 14/0.659 | 6/0.608 | - | - | reject | band score <= 0.9 / drop |
| 1083 | combo-tpe | SL1.2/T2.0 mask[signal_range_pct>=0.786818] g{"min_slot": "10:30", "max_slot": "12:30", "top_n": 1} | 206/0.612 | 166/0.585 | - | - | reject | band score <= 0.9 / drop |
| 1084 | combo-tpe | SL1.0/T2.5 mask[x_obv_slope5>=6.219594;x_break_rank_day<=0.0;x_svwap_dist_pct>=0.968169] g{"max_slot": "14:30", "top_n": 1} | 148/0.662 | 127/0.601 | - | - | reject | band score <= 0.9 / drop |
| 1085 | combo-tpe | SL1.2/T2.5 mask[x_obv_slope5>=6.219594] g{"max_slot": "12:30", "top_n": 1} | 259/0.554 | 201/0.551 | - | - | reject | band score <= 0.9 / drop |
| 1086 | combo-tpe | SL1.2/T2.0 mask[x_roc12>=1.891449] g{"min_slot": "10:30", "max_slot": "12:30", "top_n": 1} | 291/0.562 | 223/0.549 | - | - | reject | band score <= 0.9 / drop |
| 1087 | combo-tpe | SL1.2/T2.0 mask[upper_wick_pct>=0.094678] g{"min_slot": "10:30", "max_slot": "13:30", "top_n": 1} | 227/0.591 | 180/0.56 | - | - | reject | band score <= 0.9 / drop |
| 1088 | combo-tpe | SL1.2/T2.0 mask[x_cci20>=161.592765] g{"min_slot": "10:30", "max_slot": "12:30", "top_n": 2} | 245/0.543 | 245/0.553 | - | - | reject | band score <= 0.9 / drop |
| 1089 | combo-tpe | SL1.2/T2.5 mask[x_obv_slope5>=8.001018] g{"max_slot": "12:30", "top_n": 1} | 126/0.602 | 112/0.563 | - | - | reject | band score <= 0.9 / drop |
| 1090 | combo-tpe | SL1.2/T2.0 mask[rs_pct>=3.59726] g{"min_slot": "10:30", "max_slot": "12:30", "top_n": 1} | 296/0.534 | 154/0.538 | - | - | reject | band score <= 0.9 / drop |
| 1091 | combo-tpe | SL1.2/T2.0 mask[x_svwap_dist_pct>=1.437949] g{"min_slot": "10:30", "max_slot": "12:30", "top_n": 1} | 431/0.549 | 319/0.538 | - | - | reject | band score <= 0.9 / drop |
| 1092 | combo-tpe | SL1.2/T2.0 mask[x_svwap_dist_pct>=1.779258] g{"min_slot": "10:30", "max_slot": "12:30", "top_n": 1} | 319/0.553 | 226/0.584 | - | - | reject | band score <= 0.9 / drop |
| 1093 | combo-tpe | SL1.2/T2.0 mask[x_gap_pct>=1.588235] g{"min_slot": "10:30", "max_slot": "13:30", "top_n": 2} | 208/0.687 | 83/0.597 | - | - | reject | band score <= 0.9 / drop |
| 1094 | combo-tpe | SL1.2/T1.5 mask[x_cci20>=136.810781] g{"min_slot": "10:30", "max_slot": "12:30", "top_n": 1} | 219/0.531 | 189/0.539 | - | - | reject | band score <= 0.9 / drop |
| 1095 | combo-tpe | SL1.2/T1.5 mask[x_gap_pct>=1.588235] g{"min_slot": "10:30", "top_n": 1} | 159/0.558 | 57/0.539 | - | - | reject | band score <= 0.9 / drop |
| 1096 | combo-tpe | SL1.2/T2.0 mask[x_atr_pct>=0.005181] g{"min_slot": "10:30", "max_slot": "12:30", "top_n": 2} | 334/0.528 | 278/0.535 | - | - | reject | band score <= 0.9 / drop |
| 1097 | combo-tpe | SL1.2/T2.0 mask[atr_pct>=0.005181] g{"min_slot": "10:30", "max_slot": "12:30", "top_n": 1} | 188/0.525 | 142/0.528 | - | - | reject | band score <= 0.9 / drop |
| 1098 | combo-tpe | SL1.2/T2.0 mask[x_atr_pct>=0.005181] g{"min_slot": "10:30", "max_slot": "12:30", "top_n": 1} | 188/0.525 | 142/0.528 | - | - | reject | band score <= 0.9 / drop |
| 1099 | combo-tpe | SL1.2/T2.5 mask[lower_wick_pct>=0.108905] g{"min_slot": "10:30", "max_slot": "12:30", "top_n": 1} | 240/0.619 | 161/0.744 | - | - | reject | band score <= 0.9 / drop |
| 1100 | combo-tpe | SL1.2/T2.0 mask[x_rsi_slope3>=12.064732] g{"min_slot": "10:30", "max_slot": "12:30", "top_n": 1} | 138/0.594 | 121/0.688 | - | - | reject | band score <= 0.9 / drop |
| 1101 | combo-tpe | SL1.2/T2.0 mask[x_cci20>=161.592765;x_above_pdh>=0.5] g{"min_slot": "10:30", "max_slot": "12:30", "top_n": 1} | 101/0.537 | 79/0.564 | - | - | reject | band score <= 0.9 / drop |
| 1102 | combo-tpe | SL1.2/T2.0 mask[x_gap_pct>=0.973331] g{"min_slot": "10:00", "max_slot": "13:30", "top_n": 1} | 270/0.523 | 133/0.518 | - | - | reject | band score <= 0.9 / drop |
| 1103 | combo-tpe | SL1.2/T2.0 mask[x_gap_pct>=1.588235] g{"min_slot": "10:00", "max_slot": "13:30", "top_n": 1} | 144/0.638 | 55/0.795 | - | - | reject | band score <= 0.9 / drop |
| 1104 | combo-tpe | SL1.2/T2.0 mask[lower_wick_pct>=0.14902] g{"min_slot": "10:30", "max_slot": "13:30", "top_n": 1} | 211/0.556 | 151/0.612 | - | - | reject | band score <= 0.9 / drop |
| 1105 | combo-tpe | SL1.2/T2.5 mask[lower_wick_pct>=0.108905] g{"max_slot": "12:30", "top_n": 1} | 293/0.57 | 201/0.643 | - | - | reject | band score <= 0.9 / drop |
| 1106 | combo-tpe | SL1.2/T2.0 mask[x_macd_hist_atr>=0.283827] g{"min_slot": "10:30", "max_slot": "12:30", "top_n": 1} | 374/0.534 | 274/0.563 | - | - | reject | band score <= 0.9 / drop |
| 1107 | combo-tpe | SL1.0/T2.5 mask[x_obv_slope5>=6.219594] g{"max_slot": "12:30", "top_n": 1} | 259/0.553 | 201/0.529 | - | - | reject | band score <= 0.9 / drop |
| 1108 | combo-tpe | SL1.2/T2.5 mask[x_bb_width_pct>=0.030927] g{"max_slot": "12:30", "top_n": 1} | 412/0.551 | 284/0.528 | - | - | reject | band score <= 0.9 / drop |
| 1109 | combo-tpe | SL0.7/T2.0 mask[x_gap_pct>=1.588235] g{"min_slot": "11:00", "top_n": 1} | 136/0.512 | 48/0.515 | - | - | reject | band score <= 0.9 / drop |
| 1110 | combo-tpe | SL1.2/T2.5 mask[rs_pct>=3.59726] g{"max_slot": "12:30", "top_n": 1} | 372/0.55 | 205/0.528 | - | - | reject | band score <= 0.9 / drop |
| 1111 | combo-tpe | SL1.0/T2.5 mask[upper_wick_pct>=0.094678] g{"max_slot": "12:30", "top_n": 1} | 273/0.558 | 193/0.531 | - | - | reject | band score <= 0.9 / drop |
| 1112 | combo-tpe | SL0.85/T2.0 mask[x_gap_pct>=1.588235] g{"min_slot": "10:30", "max_slot": "13:30", "top_n": 1} | 126/0.599 | 49/0.713 | - | - | reject | band score <= 0.9 / drop |
| 1113 | combo-tpe | SL1.2/T2.0 mask[x_mfi14>=89.910371] g{"min_slot": "10:00", "max_slot": "12:30", "top_n": 1} | 170/0.524 | 144/0.546 | - | - | reject | band score <= 0.9 / drop |
| 1114 | combo-tpe | SL1.0/T2.5 mask[x_gap_pct>=0.973331;x_macd_hist_delta_atr<=0.136315;x_vol_vs_avg20>=1.545458] g{"max_slot": "12:30", "top_n": 1} | 155/0.511 | 78/0.518 | - | - | reject | band score <= 0.9 / drop |
| 1115 | combo-tpe | SL0.5/T1.5 mask[lower_wick_pct>=0.226032;x_prev_pullback>=0.5] g{"min_slot": "12:00", "max_slot": "13:30", "top_n": 1} | 34/0.568 | 17/0.533 | - | - | reject | band score <= 0.9 / drop |
| 1116 | combo-tpe | SL1.2/T2.5 mask[x_cci20>=161.592765;x_mfi14<=89.910371] g{"min_slot": "10:30", "max_slot": "12:30", "top_n": 1} | 92/0.597 | 81/0.545 | - | - | reject | band score <= 0.9 / drop |
| 1117 | combo-tpe | SL0.85/T2.0 mask[x_cci20>=161.592765] g{"min_slot": "10:30", "max_slot": "12:30", "top_n": 1} | 144/0.626 | 131/0.557 | - | - | reject | band score <= 0.9 / drop |
| 1118 | combo-tpe | SL1.2/T1.25 mask[x_gap_pct>=1.588235] g{"min_slot": "10:30", "max_slot": "13:30", "top_n": 1} | 126/0.652 | 49/0.569 | - | - | reject | band score <= 0.9 / drop |
| 1119 | combo-tpe | SL1.2/T2.0 mask[x_cci20>=161.592765] g{"min_slot": "10:30", "max_slot": "13:30", "top_n": 1} | 204/0.571 | 202/0.532 | - | - | reject | band score <= 0.9 / drop |
| 1120 | combo-tpe | SL1.2/T2.5 mask[upper_wick_pct>=0.094678] g{"max_slot": "12:30", "top_n": 1} | 273/0.61 | 193/0.549 | - | - | reject | band score <= 0.9 / drop |
| 1121 | combo-tpe | SL1.2/T2.5 mask[x_roc12>=1.891449] g{"max_slot": "12:30", "top_n": 1} | 408/0.552 | 321/0.523 | - | - | reject | band score <= 0.9 / drop |
| 1122 | combo-tpe | SL1.2/T2.0 mask[signal_range_pct>=0.786818] g{"min_slot": "10:30", "top_n": 1} | 244/0.51 | 218/0.503 | - | - | reject | band score <= 0.9 / drop |
| 1123 | confirm | SL0.85/T1.5 mask[lower_wick_pct>=0.226032;x_reg_notbear>=0.5] g{"min_slot": "12:00", "max_slot": "13:30", "top_n": 1} | 23/0.881 | 30/0.873 | n=53 PF=0.876 | - | reject | REJECT: TRAIN PF outside [1.30,1.80] / next candidate |
| 1124 | confirm | SL1.2/T2.0 mask[lower_wick_pct>=0.226032] g{"min_slot": "10:30", "max_slot": "13:30", "top_n": 1} | 113/0.675 | 87/0.683 | n=200 PF=0.679 | - | reject | REJECT: TRAIN PF outside [1.30,1.80] / next candidate |
| 1125 | confirm | SL1.2/T2.0 mask[lower_wick_pct>=0.226032;x_prev_pullback>=0.5] g{"min_slot": "10:30", "max_slot": "13:30", "top_n": 1} | 74/0.67 | 50/0.674 | n=124 PF=0.672 | - | reject | REJECT: TRAIN PF outside [1.30,1.80] / next candidate |
| 1126 | confirm | SL0.85/T1.5 mask[lower_wick_pct>=0.226032;x_fresh_break>=0.5] g{"min_slot": "12:00", "max_slot": "13:30", "top_n": 1} | 42/0.685 | 23/0.719 | n=65 PF=0.697 | - | reject | REJECT: TRAIN PF outside [1.30,1.80] / next candidate |
| 1127 | confirm | SL1.2/T1.5 mask[lower_wick_pct>=0.226032;x_prev_pullback>=0.5] g{"min_slot": "12:00", "max_slot": "13:30", "top_n": 1} | 34/0.69 | 17/0.671 | n=51 PF=0.683 | - | reject | REJECT: TRAIN PF outside [1.30,1.80] / next candidate |
| 1128 | confirm | SL0.85/T1.5 mask[lower_wick_pct>=0.226032;x_macd_above_sig>=0.5] g{"min_slot": "12:00", "max_slot": "13:30", "top_n": 1} | 36/0.711 | 29/0.802 | n=65 PF=0.75 | - | reject | REJECT: TRAIN PF outside [1.30,1.80] / next candidate |
| 1129 | confirm | SL1.2/T2.0 mask[x_gap_pct>=1.588235] g{"min_slot": "10:30", "max_slot": "13:30", "top_n": 1} | 126/0.723 | 49/0.671 | n=175 PF=0.707 | - | reject | REJECT: TRAIN PF outside [1.30,1.80] / next candidate |
| 1130 | confirm | SL1.2/T2.0 mask[lower_wick_pct>=0.226032;x_prev_pullback>=0.5] g{"min_slot": "10:30", "max_slot": "13:30", "top_n": 2} | 124/0.626 | 86/0.647 | n=210 PF=0.635 | - | reject | REJECT: TRAIN PF outside [1.30,1.80] / next candidate |
| 1131 | confirm | SL1.2/T2.0 mask[x_gap_pct>=1.588235] g{"min_slot": "10:30", "top_n": 1} | 158/0.637 | 57/0.61 | n=215 PF=0.629 | - | reject | REJECT: TRAIN PF outside [1.30,1.80] / next candidate |
| 1132 | confirm | SL1.2/T2.0 mask[x_cci20>=161.592765] g{"min_slot": "10:30", "max_slot": "12:30", "top_n": 1} | 144/0.64 | 131/0.706 | n=275 PF=0.67 | - | reject | REJECT: TRAIN PF outside [1.30,1.80] / next candidate |
| 1133 | confirm | SL1.2/T2.0 mask[x_gap_pct>=1.588235] g{"min_slot": "11:00", "top_n": 1} | 136/0.618 | 48/0.662 | n=184 PF=0.63 | - | reject | REJECT: TRAIN PF outside [1.30,1.80] / next candidate |
| 1134 | confirm | SL1.2/T2.0 mask[x_cci20>=136.810781] g{"min_slot": "10:30", "max_slot": "12:30", "top_n": 1} | 219/0.592 | 189/0.585 | n=408 PF=0.589 | - | reject | REJECT: TRAIN PF outside [1.30,1.80] / next candidate |
| 1135 | confirm | SL1.2/T2.0 mask[upper_wick_pct>=0.094678] g{"max_slot": "12:30", "top_n": 1} | 273/0.579 | 193/0.583 | n=466 PF=0.581 | - | reject | REJECT: TRAIN PF outside [1.30,1.80] / next candidate |
| 1136 | confirm | SL1.0/T2.5 mask[x_obv_slope5>=6.219594;atr_pct<=0.001887] g{"max_slot": "12:30", "top_n": 1} | 14/0.659 | 6/0.608 | n=20 PF=0.64 | - | reject | REJECT: TRAIN PF outside [1.30,1.80] / next candidate |
| 1137 | confirm | SL1.2/T2.0 mask[signal_range_pct>=0.786818] g{"min_slot": "10:30", "max_slot": "12:30", "top_n": 1} | 206/0.612 | 166/0.585 | n=372 PF=0.6 | - | reject | REJECT: TRAIN PF outside [1.30,1.80] / next candidate |
| 1138 | confirm | SL1.0/T2.5 mask[x_obv_slope5>=6.219594;x_break_rank_day<=0.0;x_svwap_dist_pct>=0.968169] g{"max_slot": "14:30", "top_n": 1} | 148/0.662 | 127/0.601 | n=275 PF=0.632 | - | reject | REJECT: TRAIN PF outside [1.30,1.80] / next candidate |
| 1139 | confirm | SL1.2/T2.5 mask[x_obv_slope5>=6.219594] g{"max_slot": "12:30", "top_n": 1} | 259/0.554 | 201/0.551 | n=460 PF=0.553 | - | reject | REJECT: TRAIN PF outside [1.30,1.80] / next candidate |
| 1140 | confirm | SL1.2/T2.0 mask[x_roc12>=1.891449] g{"min_slot": "10:30", "max_slot": "12:30", "top_n": 1} | 291/0.562 | 223/0.549 | n=514 PF=0.556 | - | reject | REJECT: TRAIN PF outside [1.30,1.80] / next candidate |
| 1141 | confirm | SL1.2/T2.0 mask[upper_wick_pct>=0.094678] g{"min_slot": "10:30", "max_slot": "13:30", "top_n": 1} | 227/0.591 | 180/0.56 | n=407 PF=0.577 | - | reject | REJECT: TRAIN PF outside [1.30,1.80] / next candidate |
| 1142 | confirm | SL1.2/T2.0 mask[x_cci20>=161.592765] g{"min_slot": "10:30", "max_slot": "12:30", "top_n": 2} | 245/0.543 | 245/0.553 | n=490 PF=0.548 | - | reject | REJECT: TRAIN PF outside [1.30,1.80] / next candidate |
| 1143 | combo-tpe | SL0.85/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;wick_skew_pct>=0.0] g{"min_slot": "11:00", "max_slot": "11:30", "top_n": 1} | 35/0.931 | 28/0.908 | - | - | reject | band score <= 0.9 / drop |
| 1144 | combo-tpe | SL0.85/T2.5 mask[x_first_break_of_day>=0.5;x_macd_hist_delta_atr<=0.033329;x_prev_pullback>=0.5] g{"min_slot": "10:00", "max_slot": "11:30", "top_n": 1} | 37/0.895 | 20/0.927 | - | - | reject | band score <= 0.9 / drop |
| 1145 | combo-tpe | SL0.85/T2.5 mask[x_ema200_dist_atr<=6.126011;x_prev_pullback>=0.5;wick_skew_pct>=0.0] g{"min_slot": "11:00", "max_slot": "11:30", "top_n": 1} | 26/0.976 | 17/1.186 | - | - | reject | band score <= 0.9 / drop |
| 1146 | combo-tpe | SL0.85/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;x_dayrange_atr>=10.129536] g{"max_slot": "11:30", "top_n": 1} | 30/0.798 | 19/0.861 | - | - | reject | band score <= 0.9 / drop |
| 1147 | combo-tpe | SL0.85/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;x_dayrange_atr>=10.129536] g{"min_slot": "10:30", "max_slot": "11:30", "top_n": 1} | 30/0.798 | 19/0.861 | - | - | reject | band score <= 0.9 / drop |
| 1148 | combo-tpe | SL0.85/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;x_dayrange_atr>=10.129536] g{"min_slot": "10:00", "max_slot": "11:30", "top_n": 1} | 30/0.798 | 19/0.861 | - | - | reject | band score <= 0.9 / drop |
| 1149 | combo-tpe | SL0.85/T2.5 mask[x_macd_above_sig>=0.5;quality_score<=37.8186] g{"min_slot": "10:00", "max_slot": "11:30", "top_n": 1} | 14/0.866 | 17/0.8 | - | - | reject | band score <= 0.9 / drop |
| 1150 | combo-tpe | SL0.85/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;x_dist_dayhigh_atr>=0.108645] g{"min_slot": "10:00", "max_slot": "11:30", "top_n": 1} | 127/0.767 | 81/0.822 | - | - | reject | band score <= 0.9 / drop |
| 1151 | combo-tpe | SL0.85/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;upper_wick_pct>=0.050107] g{"max_slot": "11:30", "top_n": 1} | 116/0.832 | 78/0.765 | - | - | reject | band score <= 0.9 / drop |
| 1152 | combo-tpe | SL0.85/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;x_obv_slope5>=4.307972] g{"min_slot": "10:00", "max_slot": "11:30", "top_n": 1} | 86/0.716 | 68/0.731 | - | - | reject | band score <= 0.9 / drop |
| 1153 | combo-tpe | SL0.6/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;upper_wick_pct>=0.094678] g{"max_slot": "12:30", "top_n": 1} | 91/0.744 | 67/0.718 | - | - | reject | band score <= 0.9 / drop |
| 1154 | combo-tpe | SL0.85/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;x_adx_slope3<=-1.734748] g{"min_slot": "10:00", "max_slot": "11:30", "top_n": 1} | 41/1.048 | 11/0.841 | - | - | reject | band score <= 0.9 / drop |
| 1155 | combo-tpe | SL0.6/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;upper_wick_pct>=0.050107] g{"max_slot": "12:30", "top_n": 1} | 140/0.68 | 106/0.674 | - | - | reject | band score <= 0.9 / drop |
| 1156 | combo-tpe | SL0.85/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;x_obv_slope5>=2.301614] g{"min_slot": "10:00", "max_slot": "11:30", "top_n": 1} | 165/0.647 | 114/0.656 | - | - | reject | band score <= 0.9 / drop |
| 1157 | combo-tpe | SL1.0/T2.0 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;atr_pct>=0.003703] g{"max_slot": "11:30", "top_n": 1} | 174/0.626 | 109/0.635 | - | - | reject | band score <= 0.9 / drop |
| 1158 | combo-tpe | SL1.0/T2.0 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;x_svwap_dist_pct>=1.177599] g{"max_slot": "11:30", "top_n": 1} | 163/0.659 | 108/0.71 | - | - | reject | band score <= 0.9 / drop |
| 1159 | combo-tpe | SL1.0/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;atr_pct>=0.001887] g{"max_slot": "11:30", "top_n": 1} | 223/0.643 | 136/0.676 | - | - | reject | band score <= 0.9 / drop |
| 1160 | combo-tpe | SL0.85/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5] g{"min_slot": "10:00", "max_slot": "11:30", "top_n": 1} | 223/0.626 | 138/0.619 | - | - | reject | band score <= 0.9 / drop |
| 1161 | combo-tpe | SL0.85/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;x_ema200_dist_atr>=6.126011] g{"min_slot": "11:00", "max_slot": "11:30", "top_n": 1} | 35/0.615 | 27/0.617 | - | - | reject | band score <= 0.9 / drop |
| 1162 | combo-tpe | SL0.85/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;x_pdh_dist_atr>=-10.008219] g{"min_slot": "10:00", "max_slot": "11:30", "top_n": 1} | 216/0.619 | 137/0.626 | - | - | reject | band score <= 0.9 / drop |
| 1163 | combo-tpe | SL0.85/T2.5 mask[x_break_rank_day<=0.0;x_prev_pullback>=0.5;upper_wick_pct>=0.069044] g{"max_slot": "12:30", "top_n": 1} | 117/0.698 | 93/0.81 | - | - | reject | band score <= 0.9 / drop |
| 1164 | combo-tpe | SL0.85/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;x_ema50_dist_atr>=3.735859] g{"max_slot": "11:30", "top_n": 1} | 118/0.622 | 90/0.647 | - | - | reject | band score <= 0.9 / drop |
| 1165 | combo-tpe | SL1.0/T2.0 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;x_macd_hist_delta_atr>=0.015327] g{"max_slot": "11:30", "top_n": 1} | 212/0.607 | 134/0.618 | - | - | reject | band score <= 0.9 / drop |
| 1166 | combo-tpe | SL0.85/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;x_roc12>=0.296902] g{"min_slot": "10:00", "max_slot": "11:30", "top_n": 1} | 201/0.644 | 136/0.618 | - | - | reject | band score <= 0.9 / drop |
| 1167 | combo-tpe | SL0.7/T2.5 mask[x_macd_above_sig>=0.5;x_prev_pullback>=0.5;wick_skew_pct>=0.0] g{"min_slot": "11:00", "max_slot": "11:30", "top_n": 1} | 47/0.604 | 35/0.616 | - | - | reject | band score <= 0.9 / drop |
| 1168 | combo-tpe | SL0.85/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5] g{"max_slot": "11:30", "top_n": 1} | 228/0.616 | 143/0.644 | - | - | reject | band score <= 0.9 / drop |
| 1169 | combo-tpe | SL0.85/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;x_break_rank_day<=1.0] g{"max_slot": "11:30", "top_n": 1} | 228/0.616 | 143/0.644 | - | - | reject | band score <= 0.9 / drop |
| 1170 | combo-tpe | SL0.85/T2.5 mask[x_macd_above_sig>=0.5;x_first_break_of_day>=0.5;x_prev_pullback>=0.5] g{"min_slot": "10:00", "max_slot": "11:30", "top_n": 1} | 202/0.595 | 130/0.597 | - | - | reject | band score <= 0.9 / drop |
| 1171 | combo-tpe | SL0.85/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;x_dayrange_atr<=9.092502] g{"min_slot": "10:00", "max_slot": "11:30", "top_n": 1} | 179/0.603 | 106/0.618 | - | - | reject | band score <= 0.9 / drop |
| 1172 | combo-tpe | SL0.85/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;x_svwap_dist_pct>=0.968169] g{"min_slot": "10:00", "max_slot": "11:30", "top_n": 1} | 185/0.628 | 115/0.606 | - | - | reject | band score <= 0.9 / drop |
| 1173 | combo-tpe | SL0.85/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;x_dayrange_atr>=7.2487] g{"min_slot": "10:00", "max_slot": "11:30", "top_n": 1} | 98/0.711 | 64/0.641 | - | - | reject | band score <= 0.9 / drop |
| 1174 | combo-tpe | SL0.85/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;x_ema20_gt50>=0.5] g{"min_slot": "10:00", "max_slot": "11:30", "top_n": 1} | 173/0.587 | 125/0.585 | - | - | reject | band score <= 0.9 / drop |
| 1175 | combo-tpe | SL0.85/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;rs_pct>=2.26412] g{"min_slot": "10:00", "max_slot": "11:30", "top_n": 1} | 149/0.656 | 75/0.614 | - | - | reject | band score <= 0.9 / drop |
| 1176 | combo-tpe | SL0.85/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;x_dayrange_atr<=10.129536] g{"min_slot": "10:00", "max_slot": "11:30", "top_n": 1} | 193/0.602 | 119/0.588 | - | - | reject | band score <= 0.9 / drop |
| 1177 | combo-tpe | SL0.85/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;x_ema200_dist_atr>=3.87152] g{"min_slot": "10:00", "max_slot": "11:30", "top_n": 1} | 125/0.615 | 89/0.665 | - | - | reject | band score <= 0.9 / drop |
| 1178 | combo-tpe | SL0.85/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;x_roc6>=0.827972] g{"min_slot": "10:00", "max_slot": "11:30", "top_n": 1} | 170/0.678 | 113/0.612 | - | - | reject | band score <= 0.9 / drop |
| 1179 | combo-tpe | SL0.6/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;wick_skew_pct>=0.045633] g{"max_slot": "12:30", "top_n": 1} | 71/0.669 | 63/0.608 | - | - | reject | band score <= 0.9 / drop |
| 1180 | combo-tpe | SL0.85/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;signal_range_pct>=0.545031] g{"min_slot": "10:00", "max_slot": "11:30", "top_n": 1} | 175/0.728 | 103/0.634 | - | - | reject | band score <= 0.9 / drop |
| 1181 | combo-tpe | SL0.6/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;quality_score>=123.9634] g{"max_slot": "12:30", "top_n": 1} | 102/0.593 | 59/0.635 | - | - | reject | band score <= 0.9 / drop |
| 1182 | combo-tpe | SL0.85/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;x_atr_pct>=0.003703] g{"min_slot": "10:00", "max_slot": "11:30", "top_n": 1} | 169/0.706 | 104/0.624 | - | - | reject | band score <= 0.9 / drop |
| 1183 | combo-tpe | SL0.6/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;x_pdh_dist_atr>=-10.008219] g{"max_slot": "11:30", "top_n": 1} | 221/0.568 | 142/0.58 | - | - | reject | band score <= 0.9 / drop |
| 1184 | combo-tpe | SL0.85/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;lower_wick_pct>=0.079316] g{"min_slot": "10:00", "max_slot": "11:30", "top_n": 1} | 95/0.566 | 59/0.579 | - | - | reject | band score <= 0.9 / drop |
| 1185 | combo-tpe | SL0.85/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;x_ema20_slope3_atr>=0.175318] g{"min_slot": "11:00", "max_slot": "11:30", "top_n": 1} | 70/0.632 | 51/0.729 | - | - | reject | band score <= 0.9 / drop |
| 1186 | combo-tpe | SL1.0/T2.0 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;atr_pct>=0.001887] g{"max_slot": "11:30", "top_n": 1} | 223/0.595 | 136/0.649 | - | - | reject | band score <= 0.9 / drop |
| 1187 | combo-tpe | SL0.85/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;x_dayrange_atr>=10.129536] g{"min_slot": "11:00", "max_slot": "11:30", "top_n": 1} | 24/0.731 | 14/0.959 | - | - | reject | band score <= 0.9 / drop |
| 1188 | combo-tpe | SL0.85/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;x_dayrange_atr>=10.129536] g{"min_slot": "10:00", "max_slot": "11:30"} | 176/0.653 | 57/0.595 | - | - | reject | band score <= 0.9 / drop |
| 1189 | combo-tpe | SL0.85/T2.5 mask[x_first_break_of_day>=0.5;x_reg_notbear>=0.5;x_dayrange_atr>=10.129536] g{"min_slot": "10:00", "max_slot": "11:30", "top_n": 1} | 44/0.589 | 26/0.566 | - | - | reject | band score <= 0.9 / drop |
| 1190 | combo-tpe | SL0.85/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;vol_ratio<=2.592] g{"min_slot": "10:30", "max_slot": "11:30", "top_n": 2} | 206/0.64 | 123/0.588 | - | - | reject | band score <= 0.9 / drop |
| 1191 | combo-tpe | SL0.6/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;atr_pct>=0.005181] g{"max_slot": "11:30", "top_n": 1} | 125/0.627 | 72/0.581 | - | - | reject | band score <= 0.9 / drop |
| 1192 | combo-tpe | SL0.6/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;atr_pct>=0.005181] g{"max_slot": "12:30", "top_n": 1} | 127/0.582 | 84/0.628 | - | - | reject | band score <= 0.9 / drop |
| 1193 | combo-tpe | SL0.85/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;x_roc3>=0.636459] g{"min_slot": "10:00", "max_slot": "11:30", "top_n": 1} | 172/0.681 | 105/0.605 | - | - | reject | band score <= 0.9 / drop |
| 1194 | combo-tpe | SL0.85/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;x_atr_pct<=0.004278] g{"min_slot": "10:00", "max_slot": "11:30", "top_n": 1} | 73/0.55 | 50/0.564 | - | - | reject | band score <= 0.9 / drop |
| 1195 | combo-tpe | SL0.6/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;quality_score>=37.8186] g{"max_slot": "11:30", "top_n": 1} | 219/0.548 | 134/0.562 | - | - | reject | band score <= 0.9 / drop |
| 1196 | combo-tpe | SL1.0/T2.0 mask[x_first_break_of_day>=0.5;x_break_rank_day>=0.0;x_svwap_dist_pct>=1.177599] g{"max_slot": "11:30", "top_n": 1} | 286/0.613 | 179/0.57 | - | - | reject | band score <= 0.9 / drop |
| 1197 | combo-tpe | SL0.6/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;atr_pct>=0.001887] g{"max_slot": "11:30", "top_n": 1} | 223/0.562 | 136/0.594 | - | - | reject | band score <= 0.9 / drop |
| 1198 | combo-tpe | SL0.85/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;x_dayrange_atr>=9.092502] g{"min_slot": "10:00", "max_slot": "11:30", "top_n": 1} | 44/0.737 | 32/0.626 | - | - | reject | band score <= 0.9 / drop |
| 1199 | combo-tpe | SL0.85/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;x_adx>=31.595207] g{"min_slot": "10:00", "max_slot": "11:30", "top_n": 1} | 129/0.552 | 92/0.578 | - | - | reject | band score <= 0.9 / drop |
| 1200 | combo-tpe | SL1.0/T2.0 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;x_svwap_dist_pct>=0.236267] g{"max_slot": "11:30", "top_n": 1} | 224/0.569 | 142/0.617 | - | - | reject | band score <= 0.9 / drop |
| 1201 | combo-tpe | SL0.6/T2.5 mask[x_first_break_of_day>=0.5;x_fresh_break>=0.5;upper_wick_pct>=0.094678] g{"max_slot": "12:30", "top_n": 1} | 137/0.643 | 91/0.577 | - | - | reject | band score <= 0.9 / drop |
| 1202 | combo-tpe | SL0.85/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;lower_wick_pct>=0.079316] g{"max_slot": "11:30", "top_n": 1} | 97/0.571 | 60/0.632 | - | - | reject | band score <= 0.9 / drop |
| 1203 | confirm | SL0.85/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;wick_skew_pct>=0.0] g{"min_slot": "11:00", "max_slot": "11:30", "top_n": 1} | 35/0.931 | 28/0.908 | n=63 PF=0.921 | - | reject | REJECT: TRAIN PF outside [1.30,1.80] / next candidate |
| 1204 | confirm | SL0.85/T2.5 mask[x_first_break_of_day>=0.5;x_macd_hist_delta_atr<=0.033329;x_prev_pullback>=0.5] g{"min_slot": "10:00", "max_slot": "11:30", "top_n": 1} | 37/0.895 | 20/0.927 | n=57 PF=0.905 | - | reject | REJECT: TRAIN PF outside [1.30,1.80] / next candidate |
| 1205 | confirm | SL0.85/T2.5 mask[x_ema200_dist_atr<=6.126011;x_prev_pullback>=0.5;wick_skew_pct>=0.0] g{"min_slot": "11:00", "max_slot": "11:30", "top_n": 1} | 26/0.976 | 17/1.186 | n=43 PF=1.048 | - | reject | REJECT: TRAIN PF outside [1.30,1.80] / next candidate |
| 1206 | confirm | SL0.85/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;x_dayrange_atr>=10.129536] g{"max_slot": "11:30", "top_n": 1} | 30/0.798 | 19/0.861 | n=49 PF=0.82 | - | reject | REJECT: TRAIN PF outside [1.30,1.80] / next candidate |
| 1207 | confirm | SL0.85/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;x_dayrange_atr>=10.129536] g{"min_slot": "10:30", "max_slot": "11:30", "top_n": 1} | 30/0.798 | 19/0.861 | n=49 PF=0.82 | - | reject | REJECT: TRAIN PF outside [1.30,1.80] / next candidate |
| 1208 | confirm | SL0.85/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;x_dayrange_atr>=10.129536] g{"min_slot": "10:00", "max_slot": "11:30", "top_n": 1} | 30/0.798 | 19/0.861 | n=49 PF=0.82 | - | reject | REJECT: TRAIN PF outside [1.30,1.80] / next candidate |
| 1209 | confirm | SL0.85/T2.5 mask[x_macd_above_sig>=0.5;quality_score<=37.8186] g{"min_slot": "10:00", "max_slot": "11:30", "top_n": 1} | 14/0.866 | 17/0.8 | n=31 PF=0.829 | - | reject | REJECT: TRAIN PF outside [1.30,1.80] / next candidate |
| 1210 | confirm | SL0.85/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;x_dist_dayhigh_atr>=0.108645] g{"min_slot": "10:00", "max_slot": "11:30", "top_n": 1} | 127/0.767 | 81/0.822 | n=208 PF=0.787 | - | reject | REJECT: TRAIN PF outside [1.30,1.80] / next candidate |
| 1211 | confirm | SL0.85/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;upper_wick_pct>=0.050107] g{"max_slot": "11:30", "top_n": 1} | 116/0.832 | 78/0.765 | n=194 PF=0.805 | - | reject | REJECT: TRAIN PF outside [1.30,1.80] / next candidate |
| 1212 | confirm | SL0.85/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;x_obv_slope5>=4.307972] g{"min_slot": "10:00", "max_slot": "11:30", "top_n": 1} | 86/0.716 | 68/0.731 | n=154 PF=0.722 | - | reject | REJECT: TRAIN PF outside [1.30,1.80] / next candidate |
| 1213 | confirm | SL0.6/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;upper_wick_pct>=0.094678] g{"max_slot": "12:30", "top_n": 1} | 91/0.744 | 67/0.718 | n=158 PF=0.733 | - | reject | REJECT: TRAIN PF outside [1.30,1.80] / next candidate |
| 1214 | confirm | SL0.85/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;x_adx_slope3<=-1.734748] g{"min_slot": "10:00", "max_slot": "11:30", "top_n": 1} | 41/1.048 | 11/0.841 | n=52 PF=1.013 | - | reject | REJECT: TRAIN PF outside [1.30,1.80] / next candidate |
| 1215 | confirm | SL0.6/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;upper_wick_pct>=0.050107] g{"max_slot": "12:30", "top_n": 1} | 140/0.68 | 106/0.674 | n=246 PF=0.678 | - | reject | REJECT: TRAIN PF outside [1.30,1.80] / next candidate |
| 1216 | confirm | SL0.85/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;x_obv_slope5>=2.301614] g{"min_slot": "10:00", "max_slot": "11:30", "top_n": 1} | 165/0.647 | 114/0.656 | n=279 PF=0.651 | - | reject | REJECT: TRAIN PF outside [1.30,1.80] / next candidate |
| 1217 | confirm | SL1.0/T2.0 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;atr_pct>=0.003703] g{"max_slot": "11:30", "top_n": 1} | 174/0.626 | 109/0.635 | n=283 PF=0.63 | - | reject | REJECT: TRAIN PF outside [1.30,1.80] / next candidate |
| 1218 | confirm | SL1.0/T2.0 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;x_svwap_dist_pct>=1.177599] g{"max_slot": "11:30", "top_n": 1} | 163/0.659 | 108/0.71 | n=271 PF=0.679 | - | reject | REJECT: TRAIN PF outside [1.30,1.80] / next candidate |
| 1219 | confirm | SL1.0/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;atr_pct>=0.001887] g{"max_slot": "11:30", "top_n": 1} | 223/0.643 | 136/0.676 | n=359 PF=0.655 | - | reject | REJECT: TRAIN PF outside [1.30,1.80] / next candidate |
| 1220 | confirm | SL0.85/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5] g{"min_slot": "10:00", "max_slot": "11:30", "top_n": 1} | 223/0.626 | 138/0.619 | n=361 PF=0.624 | - | reject | REJECT: TRAIN PF outside [1.30,1.80] / next candidate |
| 1221 | confirm | SL0.85/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;x_ema200_dist_atr>=6.126011] g{"min_slot": "11:00", "max_slot": "11:30", "top_n": 1} | 35/0.615 | 27/0.617 | n=62 PF=0.616 | - | reject | REJECT: TRAIN PF outside [1.30,1.80] / next candidate |
| 1222 | confirm | SL0.85/T2.5 mask[x_first_break_of_day>=0.5;x_prev_pullback>=0.5;x_pdh_dist_atr>=-10.008219] g{"min_slot": "10:00", "max_slot": "11:30", "top_n": 1} | 216/0.619 | 137/0.626 | n=353 PF=0.622 | - | reject | REJECT: TRAIN PF outside [1.30,1.80] / next candidate |