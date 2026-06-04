# v11 historical full backtesting report: 2025-06-02 to 2026-05-29

## Method

- Mode: `historical_all_available`.
- 5-minute source: `C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2`.
- 1-minute source: `C:\TradingData\eqidv2\stocks_indicators_1min_eq`.
- Entry flow: regenerated historical 5-minute candidates, v7 live scanner gates/research filters, v7 1-minute entry engine, PAPER_TRUE-style `ltp_on_signal_1m_open` fill.
- Exit flow: setup-specific 1-minute SL/target/EOD resolver.
- PnL model: v7 signal quantity, price-only, no backtest costs.

- Output folder: `C:\TradingData\eqidv2\outputs_ID_v11_5min`
- Trades CSV: `C:\TradingData\eqidv2\outputs_ID_v11_5min\trades.csv`

## Overall Summary

| Metric | Value |
| --- | --- |
| Calendar dates indexed | 246 |
| Trading days with trades | 245 |
| No-trade indexed dates | 2025-10-21 |
| Trades | 11,067 |
| Avg trades/day | 45.17 |
| Wins / Losses / Flats | 4999 / 6068 / 0 |
| Win rate | 45.17% |
| Target / SL / EOD | 3126 / 4327 / 3614 |
| Target / SL / EOD rate | 28.25% / 39.10% / 32.66% |
| Profit factor | 0.850 |
| Gross PnL Rs | -534,856.86 |
| Costs Rs | 0.00 |
| Net PnL Rs | -534,856.86 |
| Avg / Median PnL per trade | Rs -48.33 / Rs -107.55 |
| Day win rate | 39.59% |
| Max drawdown Rs | -543,225.78 |
| Final cumulative PnL Rs | -534,856.86 |

## Pipeline Funnel

| Stage | Count |
| --- | --- |
| Dates indexed | 246 |
| Slots scanned | 17,220 |
| Slots with processed candidates | 11,149 |
| Raw candidates | 256,426 |
| Ranked raw candidates | 256,426 |
| V8/live gated candidates | 12,949 |
| Research rejected | 1,247 |
| Pre-dedupe live candidates | 11,702 |
| Live-like candidates | 11,070 |
| Live-like duplicates | 632 |
| Entry-engine raw entries | 11,699 |
| Entry-engine rejects | 3 |
| Entry-engine signals | 11,067 |
| Resolved trades | 11,067 |

## Side Summary

| Side | Trades | Net PnL | Win rate | PF | T/SL/EOD | Avg PnL |
| --- | --- | --- | --- | --- | --- | --- |
| LONG | 2144 | Rs -127,083.39 | 39.79% | 0.827 | 339/861/944 | Rs -59.27 |
| SHORT | 8923 | Rs -407,773.46 | 46.46% | 0.855 | 2787/3466/2670 | Rs -45.70 |

## Setup Summary

| Side Setup | Trades | Net PnL | Win rate | PF | T/SL/EOD | Avg PnL |
| --- | --- | --- | --- | --- | --- | --- |
| SHORT E_ORB_BREAKOUT_SHORT | 339 | Rs 35,153.03 | 49.26% | 1.287 | 106/143/90 | Rs 103.70 |
| LONG C_OR_BREAKOUT | 139 | Rs 31,967.55 | 57.55% | 1.792 | 40/22/77 | Rs 229.98 |
| SHORT S_BB_SQUEEZE_SHORT | 545 | Rs 8,542.61 | 46.06% | 1.033 | 138/237/170 | Rs 15.67 |
| SHORT E_VWAP_LOSE_EARLY_SHORT | 74 | Rs 7,791.43 | 48.65% | 1.306 | 31/35/8 | Rs 105.29 |
| LONG L_BB_SQUEEZE_LONG | 231 | Rs -2,978.11 | 49.78% | 0.965 | 106/112/13 | Rs -12.89 |
| LONG G_HIGHER_HIGH_BREAK | 136 | Rs -3,025.04 | 44.85% | 0.932 | 17/36/83 | Rs -22.24 |
| LONG E_ORB_BREAKOUT_LONG | 124 | Rs -21,163.96 | 33.06% | 0.680 | 37/83/4 | Rs -170.68 |
| SHORT D_EMA20_REJECTION | 1408 | Rs -28,416.24 | 44.32% | 0.937 | 188/527/693 | Rs -20.18 |
| LONG D_EMA20_BOUNCE | 1514 | Rs -131,883.83 | 36.72% | 0.737 | 139/608/767 | Rs -87.11 |
| SHORT E_VWAP_BAND_FADE | 6557 | Rs -430,844.29 | 46.79% | 0.781 | 2324/2524/1709 | Rs -65.71 |

## Monthly Summary

| Month | Trades | Net PnL | Win rate | PF | T/SL/EOD | Avg PnL |
| --- | --- | --- | --- | --- | --- | --- |
| 2025-06 | 632 | Rs -40,224.04 | 41.93% | 0.814 | 151/257/224 | Rs -63.65 |
| 2025-07 | 754 | Rs -31,478.80 | 45.89% | 0.865 | 185/280/289 | Rs -41.75 |
| 2025-08 | 723 | Rs -25,597.94 | 46.89% | 0.887 | 224/285/214 | Rs -35.41 |
| 2025-09 | 962 | Rs 21,859.92 | 50.52% | 1.081 | 285/308/369 | Rs 22.72 |
| 2025-10 | 861 | Rs -78,983.16 | 40.42% | 0.711 | 188/307/366 | Rs -91.73 |
| 2025-11 | 938 | Rs -14,933.09 | 47.55% | 0.945 | 254/313/371 | Rs -15.92 |
| 2025-12 | 1076 | Rs -39,640.79 | 44.42% | 0.878 | 279/383/414 | Rs -36.84 |
| 2026-01 | 844 | Rs -74,598.03 | 42.06% | 0.745 | 247/360/237 | Rs -88.39 |
| 2026-02 | 1166 | Rs -76,836.96 | 45.03% | 0.800 | 319/476/371 | Rs -65.90 |
| 2026-03 | 1095 | Rs -47,517.75 | 47.40% | 0.873 | 426/494/175 | Rs -43.40 |
| 2026-04 | 1238 | Rs -72,275.67 | 45.48% | 0.824 | 381/521/336 | Rs -58.38 |
| 2026-05 | 778 | Rs -54,630.55 | 42.29% | 0.802 | 187/343/248 | Rs -70.22 |

## Entry Window Summary

| Window | Trades | Net PnL | Win rate | PF | T/SL/EOD | Avg PnL |
| --- | --- | --- | --- | --- | --- | --- |
| 09:15-10:00 | 205 | Rs -12,767.71 | 38.05% | 0.863 | 63/115/27 | Rs -62.28 |
| 10:01-11:00 | 325 | Rs 35,486.84 | 50.46% | 1.303 | 110/142/73 | Rs 109.19 |
| 11:01-12:00 | 2150 | Rs -48,638.27 | 48.14% | 0.933 | 776/956/418 | Rs -22.62 |
| 12:01-13:00 | 2936 | Rs -196,606.18 | 44.48% | 0.805 | 878/1261/797 | Rs -66.96 |
| 13:01-14:00 | 3194 | Rs -154,360.07 | 45.30% | 0.841 | 835/1157/1202 | Rs -48.33 |
| 14:01-15:00 | 2257 | Rs -157,971.46 | 42.93% | 0.752 | 464/696/1097 | Rs -69.99 |

## Weekday Summary

| Weekday | Trades | Net PnL | Win rate | PF | T/SL/EOD | Avg PnL |
| --- | --- | --- | --- | --- | --- | --- |
| Monday | 2551 | Rs -199,486.98 | 42.77% | 0.766 | 660/1028/863 | Rs -78.20 |
| Tuesday | 2101 | Rs -61,981.34 | 46.31% | 0.905 | 583/799/719 | Rs -29.50 |
| Wednesday | 2328 | Rs -145,537.62 | 44.16% | 0.806 | 618/916/794 | Rs -62.52 |
| Thursday | 1911 | Rs -77,823.98 | 46.47% | 0.874 | 592/766/553 | Rs -40.72 |
| Friday | 2140 | Rs -49,480.05 | 46.73% | 0.926 | 655/801/684 | Rs -23.12 |
| Sunday | 36 | Rs -546.89 | 52.78% | 0.957 | 18/17/1 | Rs -15.19 |

## Best Days

| Date | Net PnL | Cum PnL | Drawdown |
| --- | --- | --- | --- |
| 2026-03-20 | Rs 16,374.38 | Rs -396,613.44 | Rs -398,685.07 |
| 2026-05-12 | Rs 13,680.72 | Rs -501,522.02 | Rs -503,593.65 |
| 2025-09-19 | Rs 10,479.00 | Rs -93,746.69 | Rs -95,818.31 |
| 2025-06-12 | Rs 10,214.16 | Rs 1,386.11 | Rs 0.00 |
| 2025-07-31 | Rs 10,180.36 | Rs -71,702.84 | Rs -73,774.46 |
| 2025-10-08 | Rs 10,121.44 | Rs -98,153.33 | Rs -100,224.95 |
| 2025-09-08 | Rs 9,969.13 | Rs -100,582.61 | Rs -102,654.23 |
| 2026-04-29 | Rs 9,887.99 | Rs -476,935.24 | Rs -479,006.86 |
| 2026-02-13 | Rs 9,657.09 | Rs -349,163.25 | Rs -351,234.87 |
| 2025-07-25 | Rs 9,307.74 | Rs -64,491.84 | Rs -66,563.46 |

## Worst Days

| Date | Net PnL | Cum PnL | Drawdown |
| --- | --- | --- | --- |
| 2025-07-29 | Rs -25,320.49 | Rs -84,487.68 | Rs -86,559.30 |
| 2026-04-02 | Rs -24,807.42 | Rs -441,054.26 | Rs -443,125.89 |
| 2025-08-04 | Rs -24,381.81 | Rs -92,892.90 | Rs -94,964.52 |
| 2025-06-16 | Rs -23,665.26 | Rs -21,593.64 | Rs -23,665.26 |
| 2026-04-06 | Rs -22,316.22 | Rs -463,370.49 | Rs -465,442.11 |
| 2026-02-06 | Rs -22,306.72 | Rs -341,200.81 | Rs -343,272.43 |
| 2026-05-06 | Rs -20,809.81 | Rs -510,636.20 | Rs -512,707.82 |
| 2025-10-01 | Rs -20,603.33 | Rs -96,044.19 | Rs -98,115.81 |
| 2026-03-16 | Rs -18,352.63 | Rs -415,912.99 | Rs -417,984.61 |
| 2026-01-28 | Rs -17,062.16 | Rs -262,119.63 | Rs -264,191.25 |

## Best Tickers

| Ticker | Trades | Net PnL | Win rate | PF | T/SL/EOD | Avg PnL |
| --- | --- | --- | --- | --- | --- | --- |
| SPMLINFRA | 10 | Rs 7,404.72 | 90.00% | 65.391 | 8/0/2 | Rs 740.47 |
| POONAWALLA | 18 | Rs 6,708.28 | 66.67% | 2.756 | 10/5/3 | Rs 372.68 |
| SUBROS | 14 | Rs 6,501.06 | 78.57% | 5.432 | 8/2/4 | Rs 464.36 |
| ADANIGREEN | 13 | Rs 6,064.89 | 84.62% | 5.362 | 9/2/2 | Rs 466.53 |
| JYOTICNC | 25 | Rs 5,686.94 | 64.00% | 2.412 | 13/4/8 | Rs 227.48 |
| KIRLOSENG | 17 | Rs 5,207.06 | 64.71% | 2.414 | 8/5/4 | Rs 306.30 |
| GESHIP | 14 | Rs 5,091.13 | 71.43% | 2.808 | 6/3/5 | Rs 363.65 |
| CHENNPETRO | 6 | Rs 5,048.74 | 100.00% | inf | 2/0/4 | Rs 841.46 |
| BLUESTONE | 16 | Rs 4,980.49 | 62.50% | 2.423 | 10/5/1 | Rs 311.28 |
| SWIGGY | 18 | Rs 4,931.67 | 66.67% | 2.307 | 10/5/3 | Rs 273.98 |
| GICRE | 15 | Rs 4,882.21 | 60.00% | 4.017 | 5/0/10 | Rs 325.48 |
| NAM-INDIA | 15 | Rs 4,733.39 | 53.33% | 2.267 | 6/4/5 | Rs 315.56 |
| AIIL | 12 | Rs 4,603.13 | 75.00% | 3.198 | 8/3/1 | Rs 383.59 |
| JARO | 9 | Rs 4,534.09 | 77.78% | 4.247 | 7/2/0 | Rs 503.79 |
| AVALON | 8 | Rs 4,488.92 | 87.50% | 7.434 | 5/1/2 | Rs 561.12 |

## Worst Tickers

| Ticker | Trades | Net PnL | Win rate | PF | T/SL/EOD | Avg PnL |
| --- | --- | --- | --- | --- | --- | --- |
| V2RETAIL | 19 | Rs -9,159.83 | 26.32% | 0.151 | 2/14/3 | Rs -482.10 |
| ZENTEC | 23 | Rs -7,972.91 | 26.09% | 0.225 | 3/13/7 | Rs -346.65 |
| CAPLIPOINT | 25 | Rs -7,760.44 | 28.00% | 0.312 | 2/14/9 | Rs -310.42 |
| EIDPARRY | 17 | Rs -7,714.52 | 11.76% | 0.081 | 1/9/7 | Rs -453.80 |
| GENESYS | 23 | Rs -7,664.47 | 34.78% | 0.326 | 4/14/5 | Rs -333.24 |
| NAUKRI | 20 | Rs -7,419.10 | 25.00% | 0.125 | 1/10/9 | Rs -370.96 |
| KROSS | 16 | Rs -7,412.93 | 18.75% | 0.195 | 3/11/2 | Rs -463.31 |
| MANORAMA | 17 | Rs -7,245.76 | 17.65% | 0.158 | 2/12/3 | Rs -426.22 |
| RITES | 29 | Rs -7,218.91 | 31.03% | 0.327 | 5/12/12 | Rs -248.93 |
| CONCORDBIO | 21 | Rs -7,208.20 | 23.81% | 0.292 | 5/14/2 | Rs -343.25 |
| IPCALAB | 15 | Rs -7,011.31 | 20.00% | 0.155 | 1/9/5 | Rs -467.42 |
| KAYNES | 16 | Rs -6,450.12 | 12.50% | 0.154 | 2/10/4 | Rs -403.13 |
| INDGN | 19 | Rs -6,241.05 | 21.05% | 0.257 | 1/11/7 | Rs -328.48 |
| PATANJALI | 13 | Rs -6,234.72 | 15.38% | 0.161 | 2/10/1 | Rs -479.59 |
| RCF | 18 | Rs -6,211.44 | 22.22% | 0.210 | 2/10/6 | Rs -345.08 |

## Best Trades

| Date | Entry | Ticker | Side | Setup | Outcome | Exit | Net PnL |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 2026-01-27 | 13:46 | HIMATSEIDE | SHORT | S_BB_SQUEEZE_SHORT | TARGET | 14:00 | Rs 1,503.99 |
| 2025-09-19 | 11:36 | EDELWEISS | LONG | C_OR_BREAKOUT | TARGET | 11:57 | Rs 1,502.73 |
| 2026-03-05 | 11:31 | YATRA | SHORT | S_BB_SQUEEZE_SHORT | TARGET | 13:00 | Rs 1,502.73 |
| 2026-02-10 | 14:31 | MUFTI | SHORT | S_BB_SQUEEZE_SHORT | TARGET | 14:44 | Rs 1,502.45 |
| 2025-06-25 | 13:26 | MANINFRA | LONG | G_HIGHER_HIGH_BREAK | TARGET | 13:33 | Rs 1,502.16 |
| 2026-02-04 | 11:21 | JMFINANCIL | LONG | D_EMA20_BOUNCE | TARGET | 15:17 | Rs 1,501.50 |
| 2025-08-29 | 12:36 | GOKULAGRO | SHORT | S_BB_SQUEEZE_SHORT | TARGET | 15:15 | Rs 1,501.47 |
| 2025-11-06 | 12:31 | TOLINS | SHORT | S_BB_SQUEEZE_SHORT | TARGET | 15:07 | Rs 1,501.44 |
| 2025-07-03 | 11:26 | PARADEEP | LONG | D_EMA20_BOUNCE | TARGET | 14:56 | Rs 1,501.43 |
| 2025-10-24 | 11:11 | SCI | LONG | C_OR_BREAKOUT | TARGET | 11:23 | Rs 1,501.26 |
| 2025-11-20 | 11:41 | PWL | SHORT | S_BB_SQUEEZE_SHORT | TARGET | 12:11 | Rs 1,501.14 |
| 2025-11-17 | 12:06 | GROWW | LONG | C_OR_BREAKOUT | TARGET | 12:09 | Rs 1,500.98 |
| 2026-02-09 | 13:06 | IGIL | LONG | D_EMA20_BOUNCE | TARGET | 13:21 | Rs 1,500.94 |
| 2025-11-26 | 13:51 | IIFLCAPS | LONG | D_EMA20_BOUNCE | TARGET | 14:43 | Rs 1,500.93 |
| 2025-10-30 | 11:36 | WELCORP | LONG | G_HIGHER_HIGH_BREAK | TARGET | 14:22 | Rs 1,500.93 |

## Worst Trades

| Date | Entry | Ticker | Side | Setup | Outcome | Exit | Net PnL |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 2025-09-19 | 13:21 | DIAMONDYD | LONG | C_OR_BREAKOUT | SL | 13:44 | Rs -1,200.38 |
| 2025-09-19 | 13:06 | AFFLE | LONG | C_OR_BREAKOUT | SL | 15:04 | Rs -1,199.22 |
| 2025-11-17 | 13:26 | GPIL | LONG | C_OR_BREAKOUT | SL | 15:08 | Rs -1,199.06 |
| 2025-12-09 | 12:41 | WCIL | LONG | C_OR_BREAKOUT | SL | 12:59 | Rs -1,198.94 |
| 2025-10-14 | 13:11 | MANAKCOAT | LONG | C_OR_BREAKOUT | SL | 14:24 | Rs -1,198.92 |
| 2025-12-09 | 12:01 | V2RETAIL | LONG | C_OR_BREAKOUT | SL | 13:44 | Rs -1,198.80 |
| 2025-12-09 | 12:21 | CGCL | LONG | C_OR_BREAKOUT | SL | 13:48 | Rs -1,198.40 |
| 2025-11-17 | 12:21 | SKYGOLD | LONG | C_OR_BREAKOUT | SL | 12:46 | Rs -1,198.18 |
| 2025-11-17 | 14:01 | GOKULAGRO | LONG | C_OR_BREAKOUT | SL | 14:01 | Rs -1,197.99 |
| 2025-11-17 | 11:21 | ASTERDM | LONG | C_OR_BREAKOUT | SL | 11:30 | Rs -1,197.20 |
| 2025-09-19 | 11:06 | SANDUMA | LONG | C_OR_BREAKOUT | SL | 14:32 | Rs -1,196.60 |
| 2025-10-24 | 11:06 | KITEX | LONG | C_OR_BREAKOUT | SL | 11:50 | Rs -1,196.25 |
| 2025-10-24 | 14:11 | EPACKPEB | LONG | C_OR_BREAKOUT | SL | 14:18 | Rs -1,195.56 |
| 2025-12-05 | 13:06 | PHOENIXLTD | LONG | C_OR_BREAKOUT | SL | 14:35 | Rs -1,194.72 |
| 2025-12-09 | 12:26 | PVRINOX | LONG | C_OR_BREAKOUT | SL | 12:41 | Rs -1,193.01 |
