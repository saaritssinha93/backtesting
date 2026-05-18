# Latest v17b vs v17c Result Comparison

Comparison date: 2026-04-18

Note: the latest `v17b` and `v17c` runs both save their CSV filenames with a `v16_5min` suffix, but the output folders and console logs confirm the actual strategy version:

- `v17b`: `C:\TradingData\eqidv2\outputs_v17b_5min\...` with `[V17B_FILTER]`
- `v17c`: `C:\TradingData\eqidv2\outputs_v17c_5min\...` with `[V17C_FILTER]`

## 1. Latest artifacts used

| Version | Run timestamp | Console log | Trades CSV | Daywise CSV |
|---|---:|---|---|---|
| v17b | 2026-04-18 20:28:05 | `C:\TradingData\eqidv2\outputs_v17b_5min\avwap_combined_runner_20260418_202805.txt` | `C:\TradingData\eqidv2\outputs_v17b_5min\avwap_longshort_trades_v16_5min_ALL_DAYS_20260418_202805.csv` | `C:\TradingData\eqidv2\outputs_v17b_5min\avwap_daywise_breakdown_v16_5min_ALL_DAYS_20260418_202805.csv` |
| v17c | 2026-04-18 21:02:36 | `C:\TradingData\eqidv2\outputs_v17c_5min\avwap_combined_runner_20260418_210236.txt` | `C:\TradingData\eqidv2\outputs_v17c_5min\avwap_longshort_trades_v16_5min_ALL_DAYS_20260418_210236.csv` | `C:\TradingData\eqidv2\outputs_v17c_5min\avwap_daywise_breakdown_v16_5min_ALL_DAYS_20260418_210236.csv` |

## 2. Filter flow and runtime

| Stage | Metric | v17b | v17c | Delta |
|---|---|---:|---:|---:|
| Pre-version filter | V16 SHORT survivors | 371 | 371 | 0 |
| Pre-version filter | V16 LONG survivors | 990 | 990 | 0 |
| Version-specific filter | SHORT survivors | 164 | 259 | +95 |
| Version-specific filter | LONG survivors | 914 | 960 | +46 |
| Version-specific filter | SHORT acceptance vs V16 survivors | 44.20% | 69.81% | +25.61 pts |
| Version-specific filter | LONG acceptance vs V16 survivors | 92.32% | 96.97% | +4.65 pts |
| Timing | Phase 1 runtime | 1603.9s | 1407.9s | -196.0s |
| Timing | Phase 2 runtime | 37.8s | 31.1s | -6.7s |
| Timing | Total runtime | 1649.5s | 1445.4s | -204.1s |
| Intrabar re-resolve | SHORT via 1-min | 86 | 146 | +60 |
| Intrabar re-resolve | SHORT via 5-min fallback | 78 | 113 | +35 |
| Intrabar re-resolve | LONG via 1-min | 563 | 590 | +27 |
| Intrabar re-resolve | LONG via 5-min fallback | 351 | 370 | +19 |

## 3. Combined headline performance

| Metric | v17b | v17c | Delta | Delta % vs v17b |
|---|---:|---:|---:|---:|
| Total trades | 1078 | 1219 | +141 | +13.08% |
| Unique trade days | 183 | 192 | +9 | +4.92% |
| Trades per active day | 5.891 | 6.349 | +0.458 | +7.78% |
| TARGET hits | 763 | 846 | +83 | +10.88% |
| SL hits | 264 | 311 | +47 | +17.80% |
| EOD exits | 51 | 62 | +11 | +21.57% |
| Hit rate | 70.78% | 69.40% | -1.38 pts | -1.95% |
| SL rate | 24.49% | 25.51% | +1.02 pts | +4.16% |
| EOD rate | 4.73% | 5.09% | +0.36 pts | +7.61% |
| Avg PnL % per trade, net | 1.0741% | 0.9859% | -0.0882 pts | -8.21% |
| Sum PnL %, net | 1157.8342% | 1201.8242% | +43.9900 pts | +3.80% |
| Avg PnL % per day, net | 6.3270% | 6.2595% | -0.0675 pts | -1.07% |
| Profit factor | 1.892 | 1.786 | -0.106 | -5.60% |
| Max drawdown | 43.5182% | 58.9204% | +15.4022 pts | +35.39% |
| Sharpe | 4.975 | 4.511 | -0.464 | -9.33% |
| Sortino | 15.219 | 14.734 | -0.485 | -3.19% |
| Calmar | 26.606 | 20.397 | -6.209 | -23.34% |
| Notional P&L | Rs.231,566.84 | Rs.240,364.84 | Rs.8,798.00 | +3.80% |
| Daily win rate, pessimistic path | 73.77% | 72.40% | -1.37 pts | -1.86% |

## 4. Side-wise detailed performance

| Side | Metric | v17b | v17c | Delta |
|---|---|---:|---:|---:|
| SHORT | Trades | 164 | 259 | +95 |
| SHORT | Unique days | 77 | 93 | +16 |
| SHORT | TARGET hits | 122 | 173 | +51 |
| SHORT | SL hits | 33 | 68 | +35 |
| SHORT | EOD exits | 9 | 18 | +9 |
| SHORT | Hit rate | 74.39% | 66.80% | -7.59 pts |
| SHORT | Avg net PnL / trade | 1.4301% | 0.9171% | -0.5130 pts |
| SHORT | Sum net PnL | 234.5346% | 237.5269% | +2.9923 pts |
| SHORT | Profit factor | 2.463 | 1.719 | -0.744 |
| SHORT | Max drawdown | 20.3056% | 31.2090% | +10.9034 pts |
| SHORT | Sharpe | 7.093 | 4.179 | -2.914 |
| SHORT | Sortino | 17.599 | 14.726 | -2.873 |
| SHORT | Calmar | 11.550 | 7.611 | -3.939 |
| SHORT | Notional P&L | Rs.46,906.92 | Rs.47,505.39 | Rs.598.47 |
| LONG | Trades | 914 | 960 | +46 |
| LONG | Unique days | 132 | 134 | +2 |
| LONG | TARGET hits | 641 | 673 | +32 |
| LONG | SL hits | 231 | 243 | +12 |
| LONG | EOD exits | 42 | 44 | +2 |
| LONG | Hit rate | 70.13% | 70.10% | -0.03 pts |
| LONG | Avg net PnL / trade | 1.0102% | 1.0045% | -0.0057 pts |
| LONG | Sum net PnL | 923.2996% | 964.2972% | +40.9976 pts |
| LONG | Profit factor | 1.812 | 1.804 | -0.008 |
| LONG | Max drawdown | 44.1979% | 48.8968% | +4.6989 pts |
| LONG | Sharpe | 4.630 | 4.599 | -0.031 |
| LONG | Sortino | 14.630 | 14.735 | +0.105 |
| LONG | Calmar | 20.890 | 19.721 | -1.169 |
| LONG | Notional P&L | Rs.184,659.92 | Rs.192,859.45 | Rs.8,199.53 |

## 5. Trade mix and day mix

| Metric | v17b | v17c | Delta |
|---|---:|---:|---:|
| LONG trades share | 84.79% | 78.75% | -6.04 pts |
| SHORT trades share | 15.21% | 21.25% | +6.04 pts |
| Days with both sides | 26 | 35 | +9 |
| Days with short only | 51 | 58 | +7 |
| Days with long only | 106 | 99 | -7 |
| Total active days | 183 | 192 | +9 |

## 6. Setup-level contribution

| Side | Setup | v17b trades | v17c trades | Trade delta | v17b sum PnL % | v17c sum PnL % | PnL delta |
|---|---|---:|---:|---:|---:|---:|---:|
| LONG | A_MOD_BREAK_C1_HIGH | 639 | 676 | +37 | 596.2928 | 653.8584 | +57.5656 |
| LONG | A_MOD_CLOSE_CONTINUATION_BREAK | 117 | 123 | +6 | 102.9995 | 90.6040 | -12.3955 |
| LONG | B_HUGE_C1_CLOSE_RECLAIM_BREAK | 158 | 161 | +3 | 224.0073 | 219.8349 | -4.1724 |
| SHORT | A_MOD_BREAK_C1_LOW | 162 | 257 | +95 | 228.1346 | 231.1269 | +2.9923 |
| SHORT | B_HUGE_RED_FAILED_BOUNCE | 2 | 2 | 0 | 6.4000 | 6.4000 | 0.0000 |

## 7. Shared vs added vs removed trades

Matching key used: `trade_date + ticker + side + setup + entry_time_ist`

| Bucket | Count | Comment |
|---|---:|---|
| Shared trades | 1067 | Same trade key in both runs |
| Shared trades with changed outcome | 0 | No outcome change on shared rows |
| Shared trades with changed PnL | 0 | No PnL change on shared rows |
| v17c-only trades | 152 | New trades present only in latest v17c |
| v17b-only trades | 11 | Trades present only in latest v17b |
| Net trade delta | +141 | 152 added minus 11 removed |

### 7.1 v17c-only trade breakdown

| Side | Setup | Count | Sum PnL % | Avg PnL % |
|---|---|---:|---:|---:|
| LONG | A_MOD_BREAK_C1_HIGH | 48 | 21.6757 | 0.4516 |
| LONG | A_MOD_CLOSE_CONTINUATION_BREAK | 6 | -12.3955 | -2.0659 |
| LONG | B_HUGE_C1_CLOSE_RECLAIM_BREAK | 3 | -4.1724 | -1.3908 |
| SHORT | A_MOD_BREAK_C1_LOW | 95 | 2.9924 | 0.0315 |

| Side | TARGET | SL | EOD |
|---|---:|---:|---:|
| LONG | 34 | 21 | 2 |
| SHORT | 51 | 35 | 9 |

### 7.2 v17b-only trade breakdown

All 11 v17b-only trades are `LONG / A_MOD_BREAK_C1_HIGH`.

| Outcome | Count | Sum PnL % |
|---|---:|---:|
| TARGET | 2 | 6.4000 |
| SL | 9 | -42.2899 |
| Net | 11 | -35.8899 |

### 7.3 Exact v17b-only trades missing in v17c

| Trade date | Ticker | Side | Setup | Entry time IST | Outcome | PnL % |
|---|---|---|---|---|---|---:|
| 2025-06-19 | IDEAFORGE | LONG | A_MOD_BREAK_C1_HIGH | 2025-06-19 10:25:00+05:30 | SL | -4.6989 |
| 2025-06-25 | AARTIIND | LONG | A_MOD_BREAK_C1_HIGH | 2025-06-25 10:50:00+05:30 | SL | -4.6989 |
| 2025-06-27 | GHCLTEXTIL | LONG | A_MOD_BREAK_C1_HIGH | 2025-06-27 11:40:00+05:30 | SL | -4.6989 |
| 2025-07-09 | VIJAYA | LONG | A_MOD_BREAK_C1_HIGH | 2025-07-09 11:00:00+05:30 | SL | -4.6989 |
| 2025-09-11 | NELCO | LONG | A_MOD_BREAK_C1_HIGH | 2025-09-11 10:40:00+05:30 | SL | -4.6989 |
| 2025-10-07 | INDRAMEDCO | LONG | A_MOD_BREAK_C1_HIGH | 2025-10-07 10:55:00+05:30 | TARGET | 3.2000 |
| 2025-10-07 | PNGJL | LONG | A_MOD_BREAK_C1_HIGH | 2025-10-07 10:30:00+05:30 | SL | -4.6989 |
| 2025-11-28 | GMRAIRPORT | LONG | A_MOD_BREAK_C1_HIGH | 2025-11-28 10:55:00+05:30 | SL | -4.6989 |
| 2026-01-02 | GIPCL | LONG | A_MOD_BREAK_C1_HIGH | 2026-01-02 11:20:00+05:30 | TARGET | 3.2000 |
| 2026-01-29 | MIDHANI | LONG | A_MOD_BREAK_C1_HIGH | 2026-01-29 11:25:00+05:30 | SL | -4.6989 |
| 2026-03-17 | SBCL | LONG | A_MOD_BREAK_C1_HIGH | 2026-03-17 10:25:00+05:30 | SL | -4.6989 |

## 8. Monthly performance comparison

| Month | v17b trades | v17c trades | Trade delta | v17b sum PnL % | v17c sum PnL % | PnL delta | Rs. delta |
|---|---:|---:|---:|---:|---:|---:|---:|
| 2025-06 | 117 | 129 | +12 | 72.1087 | 102.4431 | +30.3344 | +6066.8758 |
| 2025-07 | 69 | 77 | +8 | 82.7050 | 80.7176 | -1.9874 | -397.4862 |
| 2025-08 | 81 | 104 | +23 | 143.8300 | 167.9318 | +24.1017 | +4820.3488 |
| 2025-09 | 153 | 162 | +9 | 136.8506 | 141.9472 | +5.0966 | +1019.3250 |
| 2025-10 | 136 | 140 | +4 | 104.8603 | 101.8625 | -2.9977 | -599.5500 |
| 2025-11 | 75 | 84 | +9 | 69.0266 | 66.2266 | -2.8000 | -560.0000 |
| 2025-12 | 80 | 83 | +3 | 97.8092 | 99.5081 | +1.6989 | +339.7750 |
| 2026-01 | 76 | 98 | +22 | 122.3804 | 90.0748 | -32.3056 | -6461.1250 |
| 2026-02 | 107 | 126 | +19 | 164.7465 | 175.1315 | +10.3850 | +2076.9989 |
| 2026-03 | 137 | 161 | +24 | 141.0098 | 167.3774 | +26.3676 | +5273.5115 |
| 2026-04 | 47 | 55 | +8 | 22.5071 | 8.6038 | -13.9034 | -2780.6750 |

## 9. Biggest positive and negative daily deltas from v17c additions/removals

### 9.1 Best daily deltas for v17c vs v17b

| Trade date | v17b trades | v17c trades | Trade delta | PnL delta % | Rs. delta |
|---|---:|---:|---:|---:|---:|
| 2025-08-26 | 1 | 15 | +14 | +11.7792 | +2355.8448 |
| 2026-01-29 | 4 | 5 | +1 | +11.0989 | +2219.7750 |
| 2026-03-23 | 7 | 11 | +4 | +10.4816 | +2096.3293 |
| 2026-03-19 | 1 | 4 | +3 | +9.6000 | +1920.0000 |
| 2025-06-18 | 5 | 8 | +3 | +9.6000 | +1920.0000 |

### 9.2 Worst daily deltas for v17c vs v17b

| Trade date | v17b trades | v17c trades | Trade delta | PnL delta % | Rs. delta |
|---|---:|---:|---:|---:|---:|
| 2026-01-09 | 3 | 14 | +11 | -20.1079 | -4021.5750 |
| 2026-01-16 | 10 | 13 | +3 | -14.0966 | -2819.3250 |
| 2026-04-06 | 2 | 4 | +2 | -9.4023 | -1880.4500 |
| 2026-03-16 | 5 | 7 | +2 | -9.4023 | -1880.4500 |
| 2026-01-08 | 3 | 5 | +2 | -9.4022 | -1880.4500 |

## 10. Last 10 trading days comparison

| Date | v17b trades | v17c trades | Trade delta | v17b sum PnL % | v17c sum PnL % | PnL delta | v17b L/S | v17c L/S |
|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-03-27 | 6 | 7 | +1 | 11.2989 | 14.4989 | +3.2000 | 0 / 6 | 0 / 7 |
| 2026-03-30 | 3 | 4 | +1 | 1.6989 | 4.8989 | +3.2000 | 1 / 2 | 1 / 3 |
| 2026-04-01 | 18 | 18 | 0 | -0.5777 | -0.5777 | 0.0000 | 18 / 0 | 18 / 0 |
| 2026-04-02 | 1 | 3 | +2 | -4.7011 | -6.2022 | -1.5011 | 0 / 1 | 0 / 3 |
| 2026-04-06 | 2 | 4 | +2 | -1.5011 | -10.9034 | -9.4023 | 1 / 1 | 1 / 3 |
| 2026-04-07 | 1 | 1 | 0 | -4.7011 | -4.7011 | 0.0000 | 0 / 1 | 0 / 1 |
| 2026-04-08 | 1 | 2 | +1 | -4.6989 | -9.3977 | -4.6989 | 1 / 0 | 2 / 0 |
| 2026-04-09 | 0 | 2 | +2 | 0.0000 | -1.5011 | -1.5011 | 0 / 0 | 0 / 2 |
| 2026-04-10 | 7 | 7 | 0 | 12.5698 | 12.5698 | 0.0000 | 7 / 0 | 7 / 0 |
| 2026-04-17 | 17 | 18 | +1 | 26.1173 | 29.3173 | +3.2000 | 17 / 0 | 18 / 0 |

## 11. Accepted-trade profile

| Side | Metric | v17b mean | v17b median | v17c mean | v17c median | Read |
|---|---|---:|---:|---:|---:|---|
| LONG | Quality score | 6.9485 | 6.8312 | 6.8728 | 6.7394 | v17c accepts slightly lower-QS longs on average |
| SHORT | Quality score | 0.5125 | 0.5586 | 0.5084 | 0.5497 | almost unchanged |
| LONG | RSI at signal | 75.4016 | 74.7896 | 74.6800 | 74.2560 | v17c is slightly less extended on longs |
| SHORT | RSI at signal | 36.6652 | 29.7643 | 35.5457 | 29.7267 | v17c pulls in a slightly lower-RSI short set |
| LONG | ADX at signal | 38.5131 | 36.5357 | 38.1122 | 36.2316 | nearly unchanged |
| SHORT | ADX at signal | 35.5034 | 35.2598 | 34.8229 | 34.7090 | slightly lower ADX on v17c shorts |
| LONG | AVWAP dist ATR | 2.0126 | 1.9823 | 2.0186 | 1.9823 | effectively unchanged |
| SHORT | AVWAP dist ATR | 0.9452 | 1.0758 | 0.9618 | 0.9315 | broader short distance acceptance |
| LONG | Bars from open | 7.7079 | 6.0 | 7.5615 | 6.0 | v17c is a touch earlier on long entries |
| LONG | Entry-bar vol ratio | 2.7747 | 2.3970 | 2.7592 | 2.3650 | almost unchanged |

## 12. Bottom-line reading

| Question | Answer |
|---|---|
| Did v17c make more money than v17b? | Yes. Combined net PnL rose from `1157.8342%` to `1201.8242%` and notional P&L rose by `Rs.8,798.00`. |
| Did v17c improve efficiency per trade? | No. Avg net PnL per trade, hit rate, PF, Sharpe, Sortino, Calmar, and daily win rate all fell. |
| Where did the extra PnL come from? | Mostly from extra long trades (`+40.9976%`) and secondarily from extra short trades (`+2.9923%`). |
| What was the main cost of v17c? | Much heavier risk on shorts: short PF fell from `2.463` to `1.719`, and short max drawdown rose from `20.3056%` to `31.2090%`. |
| How stable is the shared core between runs? | Very stable. `1067` shared trades had zero outcome changes and zero PnL changes. |
| Practical interpretation | `v17c` is a higher-throughput variant: more trades and slightly more total PnL, but materially weaker risk-adjusted quality than latest `v17b`. |
