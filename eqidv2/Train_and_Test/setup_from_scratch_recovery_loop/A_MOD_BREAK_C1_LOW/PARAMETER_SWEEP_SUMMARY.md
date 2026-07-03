# A_MOD_BREAK_C1_LOW (SHORT) — PARAMETER_SWEEP_SUMMARY (recovery loop)

_Generated 2026-07-03. Research-only; NO live trades; NO final_setup_conf.py edits._

Total scored configs: 1835 across variants ['RETEST', 'RX2_ALL', 'RX2_CONFIRM2', 'RX2_DEEP', 'RX2_FIRST_MORN', 'RX2_FRESHLOW', 'RX2_MKT'].

Stage A swept exits (grid + MFE/MAE-derived pairs); stage B swept every feature at q0.2/q0.5/q0.8 both directions; stage C ran TPE combinations (<=2 mask + regime + guards + <=1 premom).

## RETEST — top 10 configs

| SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|---|---|---|---|---|---|---|
| 1.5 | 2.5 | gap_pct<=0.868141 | (none) | - | 1148/0.53 | 718/0.536 | 0.5255 |
| 1.5 | 2.5 | signal_range_pct>=0.311218 | (none) | - | 1285/0.536 | 791/0.529 | 0.523 |
| 1.5 | 2.5 | adx_slope3>=-2.0725 | (none) | - | 1227/0.521 | 742/0.523 | 0.5184 |
| 1.5 | 2.5 | atr_pct>=0.004455 | (none) | - | 1134/0.552 | 740/0.595 | 0.5173 |
| 1.5 | 2.5 | day_ret_pct>=0.267937 | (none) | - | 847/0.538 | 605/0.564 | 0.5173 |
| 1.5 | 2.5 | bars_since_day_low<=17.0 | (none) | - | 1227/0.517 | 760/0.517 | 0.5162 |
| 1.5 | 2.5 | break_depth_atr>=0.0 | (none) | - | 1179/0.525 | 717/0.52 | 0.5154 |
| 1.5 | 2.5 | signal_range_pct>=0.538437 | (none) | - | 1117/0.534 | 747/0.559 | 0.5152 |
| 1.5 | 2.5 | day_low_dist_atr<=0.423307 | (none) | - | 850/0.521 | 656/0.515 | 0.5099 |
| 1.5 | 2.5 | red_streak<=3.0 | (none) | - | 1277/0.516 | 744/0.512 | 0.5093 |

## RX2_ALL — top 10 configs

| SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|---|---|---|---|---|---|---|
| 1.0 | 1.5 | break_depth_atr>=0.971224 | (none) | - | 1862/0.442 | 1200/0.455 | 0.4314 |
| 1.0 | 1.5 | quality_score>=97.122405 | (none) | - | 1862/0.442 | 1200/0.455 | 0.4314 |
| 1.0 | 1.5 | quality_score>=140.748452 | (none) | - | 1367/0.44 | 969/0.452 | 0.4314 |
| 1.0 | 1.5 | break_depth_atr>=1.407485 | (none) | - | 1367/0.44 | 969/0.452 | 0.4314 |
| 1.0 | 1.5 | signal_range_pct>=0.714955 | (none) | - | 1903/0.41 | 1182/0.426 | 0.398 |
| 1.0 | 1.5 | bb_pos<=-1.229603 | (none) | - | 1264/0.397 | 950/0.399 | 0.3945 |
| 1.0 | 1.5 | atr_pct>=0.004763 | (none) | - | 1718/0.401 | 1128/0.418 | 0.387 |
| 1.0 | 1.5 | quality_score>=54.262821 | (none) | - | 2088/0.4 | 1302/0.392 | 0.3858 |
| 1.0 | 1.5 | break_depth_atr>=0.542628 | (none) | - | 2088/0.4 | 1302/0.392 | 0.3858 |
| 1.0 | 1.5 | rsi<=44.874553 | (none) | - | 1975/0.391 | 1261/0.399 | 0.3855 |

## RX2_CONFIRM2 — top 10 configs

| SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|---|---|---|---|---|---|---|
| 1.0 | 1.5 | quality_score>=101.674886 | (none) | - | 1336/0.452 | 850/0.461 | 0.444 |
| 1.0 | 1.5 | break_depth_atr>=1.016749 | (none) | - | 1336/0.452 | 850/0.461 | 0.444 |
| 1.0 | 1.5 | range_expansion>=2.198606 | (none) | - | 807/0.443 | 489/0.44 | 0.4382 |
| 1.0 | 1.5 | break_depth_atr>=1.425854 | (none) | - | 785/0.442 | 486/0.464 | 0.4244 |
| 1.0 | 1.5 | quality_score>=142.585422 | (none) | - | 785/0.442 | 486/0.464 | 0.4244 |
| 1.0 | 1.5 | atr_pct>=0.004773 | (none) | - | 852/0.448 | 421/0.479 | 0.4235 |
| 1.0 | 1.5 | adx5>=35.223949 | (none) | - | 796/0.397 | 452/0.393 | 0.389 |
| 1.0 | 1.5 | body_pct>=0.777778 | (none) | - | 1409/0.406 | 843/0.429 | 0.3885 |
| 1.0 | 1.5 | gap_pct<=0.064172 | (none) | - | 1191/0.409 | 734/0.436 | 0.3876 |
| 1.0 | 1.5 | signal_range_pct>=0.708888 | (none) | - | 880/0.438 | 456/0.503 | 0.3851 |

## RX2_DEEP — top 10 configs

| SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|---|---|---|---|---|---|---|
| 1.0 | 1.5 | quality_score>=144.942527 | (none) | - | 1300/0.453 | 943/0.459 | 0.4486 |
| 1.0 | 1.5 | break_depth_atr>=1.449425 | (none) | - | 1300/0.453 | 943/0.459 | 0.4486 |
| 1.0 | 1.5 | gap_pct<=-0.741525 | (none) | - | 1063/0.432 | 658/0.428 | 0.4254 |
| 1.0 | 1.5 | break_depth_atr>=1.039781 | (none) | - | 1788/0.443 | 1172/0.465 | 0.4246 |
| 1.0 | 1.5 | quality_score>=103.978101 | (none) | - | 1788/0.443 | 1172/0.465 | 0.4246 |
| 1.0 | 1.5 | ema20_dist_atr<=-1.826149 | (none) | - | 1622/0.425 | 1065/0.423 | 0.4225 |
| 1.0 | 1.5 | signal_range_pct>=0.722388 | (none) | - | 1779/0.417 | 1103/0.425 | 0.4105 |
| 1.0 | 1.5 | atr_pct>=0.004686 | (none) | - | 1619/0.417 | 1047/0.426 | 0.4096 |
| 1.0 | 1.5 | body_pct>=0.817384 | (none) | - | 1890/0.401 | 1197/0.402 | 0.4003 |
| 1.0 | 1.5 | vwap_dist_atr>=0.934843 | (none) | - | 1958/0.399 | 1240/0.399 | 0.3995 |

## RX2_FIRST_MORN — top 10 configs

| SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|---|---|---|---|---|---|---|
| 1.2 | 2.0 | break_depth_atr>=1.315204 | (none) | {"max_slot": "12:00"} | 642/0.537 | 509/0.556 | 0.5223 |
| 1.2 | 2.0 | quality_score>=131.520446 | (none) | {"max_slot": "12:00"} | 642/0.537 | 509/0.556 | 0.5223 |
| 1.2 | 2.0 | break_depth_atr>=0.855245 | (none) | {"max_slot": "12:00"} | 1085/0.503 | 736/0.494 | 0.4873 |
| 1.2 | 2.0 | quality_score>=85.524497 | (none) | {"max_slot": "12:00"} | 1085/0.503 | 736/0.494 | 0.4873 |
| 1.2 | 2.0 | vwap_dist_atr>=1.611091 | (none) | {"max_slot": "12:00"} | 839/0.453 | 609/0.455 | 0.4517 |
| 1.2 | 2.0 | sess_vwap_dist_atr<=-1.611091 | (none) | {"max_slot": "12:00"} | 839/0.453 | 609/0.455 | 0.4517 |
| 0.7 | 2.5 | break_depth_atr>=1.147541; regime!=BULL | (none) | {"max_slot": "12:00", "top_n": 3} | 417/0.46 | 318/0.452 | 0.4458 |
| 1.0 | 2.5 | break_depth_atr>=1.147541; regime!=BULL | (none) | {"max_slot": "12:00", "top_n": 2} | 347/0.455 | 247/0.473 | 0.4405 |
| 1.2 | 2.0 | ema20_dist_atr<=-1.581693 | (none) | {"max_slot": "12:00"} | 892/0.462 | 636/0.439 | 0.4196 |
| 1.2 | 2.0 | bb_pos<=-1.197198 | (none) | {"max_slot": "12:00"} | 507/0.431 | 450/0.421 | 0.4134 |

## RX2_FRESHLOW — top 10 configs

| SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|---|---|---|---|---|---|---|
| 1.2 | 2.0 | quality_score>=145.964469 | (none) | - | 564/0.556 | 469/0.504 | 0.4615 |
| 1.2 | 2.0 | break_depth_atr>=1.459645 | (none) | - | 564/0.556 | 469/0.504 | 0.4615 |
| 1.2 | 2.0 | bb_pos<=-1.341864 | (none) | - | 527/0.448 | 505/0.45 | 0.4474 |
| 1.2 | 2.0 | body_pct>=0.786514 | (none) | - | 1050/0.437 | 749/0.439 | 0.4355 |
| 1.2 | 2.0 | sess_vwap_dist_atr<=-1.59565 | (none) | - | 1135/0.435 | 838/0.438 | 0.4322 |
| 1.2 | 2.0 | vwap_dist_atr>=1.59565 | (none) | - | 1135/0.435 | 838/0.438 | 0.4322 |
| 1.2 | 2.0 | break_depth_atr>=0.664302 | (none) | - | 1245/0.428 | 889/0.429 | 0.4277 |
| 1.2 | 2.0 | quality_score>=66.430247 | (none) | - | 1245/0.428 | 889/0.429 | 0.4277 |
| 1.2 | 2.0 | ema20_dist_atr<=-1.738035 | (none) | - | 1159/0.423 | 823/0.42 | 0.418 |
| 1.2 | 2.0 | obv_slope6<=-1.167831 | (none) | - | 640/0.43 | 434/0.416 | 0.4045 |

## RX2_MKT — top 10 configs

| SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|---|---|---|---|---|---|---|
| 1.2 | 2.0 | atr_pct>=0.004539 | (none) | - | 973/0.483 | 556/0.484 | 0.4812 |
| 1.2 | 2.0 | break_depth_atr>=1.424393 | (none) | - | 838/0.476 | 615/0.483 | 0.4714 |
| 1.2 | 2.0 | quality_score>=142.439331 | (none) | - | 838/0.476 | 615/0.483 | 0.4714 |
| 1.2 | 2.0 | break_depth_atr>=0.997453 | (none) | - | 1102/0.481 | 765/0.474 | 0.4682 |
| 1.2 | 2.0 | quality_score>=99.745294 | (none) | - | 1102/0.481 | 765/0.474 | 0.4682 |
| 1.2 | 2.0 | signal_range_pct>=0.684104 | (none) | - | 1027/0.469 | 600/0.472 | 0.4678 |
| 1.5 | 1.5 | upper_wick_pct<=0.0; regime==BEAR | (none) | {"min_slot": "11:00", "max_slot": "14:30", "top_n": 3} | 192/0.447 | 109/0.455 | 0.4406 |
| 1.2 | 2.0 | body_pct>=0.621803 | (none) | - | 1276/0.432 | 799/0.442 | 0.4245 |
| 1.2 | 2.0 | bars_since_day_low<=17.0 | (none) | - | 1147/0.424 | 738/0.424 | 0.4232 |
| 1.2 | 2.0 | adx5>=16.61911 | (none) | - | 1302/0.425 | 803/0.43 | 0.4212 |

## Best band score per variant (tent peaks at PF 1.80; ~1.30+ means both FIT and VAL PF >= 1.30)

- RETEST: 0.525
- RX2_FIRST_MORN: 0.522
- RX2_MKT: 0.481
- RX2_FRESHLOW: 0.462
- RX2_DEEP: 0.449
- RX2_CONFIRM2: 0.444
- RX2_ALL: 0.431