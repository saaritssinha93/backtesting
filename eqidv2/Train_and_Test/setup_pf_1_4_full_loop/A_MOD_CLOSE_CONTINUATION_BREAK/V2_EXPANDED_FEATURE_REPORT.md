# A_MOD_CLOSE_CONTINUATION_BREAK (LONG) — V2 EXPANDED-FEATURE CAMPAIGN REPORT

_Generated 2026-07-03. Research-only; NO live trades; NO final_setup_conf.py edits._

Pool: `pools/pool_enriched` — every signal row enriched with ~41 causal indicator/price-action/day-context features recomputed from OHLCV (uniform TRAIN/TEST coverage; stored MACD/BB/CCI/MFI/OBV/VWAP columns were 0% populated in June and NOT used) plus the 8 pre-momentum engine features as `x_pm_*` columns.

## Structural hypothesis packs (all explainable rule sets)

| hypothesis | exit | FIT n/PF | VAL n/PF | score | decision |
|---|---|---|---|---|---|
| or_breakout_early | SL1.0/T2.0 | 355/0.445 | 252/0.503 | 0.3988 | reject |
| day_range_fresh | SL1.0/T2.0 | 244/0.42 | 205/0.407 | 0.3964 | reject |
| day_range_fresh | SL0.7/T1.5 | 246/0.401 | 215/0.393 | 0.3859 | reject |
| trend_alignment | SL1.0/T2.0 | 458/0.387 | 339/0.37 | 0.3562 | reject |
| late_reject | SL0.7/T1.5 | 859/0.36 | 498/0.353 | 0.3476 | reject |
| rsi_momentum_zone | SL1.0/T2.0 | 766/0.371 | 434/0.406 | 0.3428 | reject |
| fresh_at_day_high | SL0.7/T1.5 | 139/0.446 | 113/0.387 | 0.3403 | reject |
| pdh_break_fresh | SL1.0/T2.0 | 407/0.364 | 293/0.395 | 0.3382 | reject |
| kelt_breakout | SL1.0/T2.0 | 813/0.392 | 508/0.349 | 0.3141 | reject |
| volume_thrust | SL0.7/T1.5 | 306/0.375 | 201/0.34 | 0.3121 | reject |
| or_breakout_early | SL0.7/T1.5 | 380/0.358 | 281/0.418 | 0.3095 | reject |
| macd_turn_up | SL1.0/T2.0 | 820/0.347 | 501/0.32 | 0.2979 | reject |
| pm_trend_confirm | SL0.7/T1.5 | 302/0.389 | 190/0.339 | 0.2979 | reject |
| kelt_breakout | SL0.7/T1.5 | 1025/0.343 | 653/0.315 | 0.2936 | reject |
| low_vol_name | SL1.0/T2.0 | 230/0.315 | 156/0.303 | 0.2935 | reject |
| pm_trend_confirm | SL1.0/T2.0 | 301/0.493 | 181/0.38 | 0.2894 | reject |
| rsi_momentum_zone | SL0.7/T1.5 | 948/0.351 | 552/0.315 | 0.2865 | reject |
| late_reject | SL1.0/T2.0 | 674/0.477 | 374/0.365 | 0.2748 | reject |
| fresh_at_day_high | SL1.0/T2.0 | 139/0.483 | 113/0.367 | 0.2741 | reject |
| volume_thrust | SL1.0/T2.0 | 303/0.498 | 190/0.372 | 0.2713 | reject |
| not_exhausted | SL1.0/T2.0 | 327/0.339 | 165/0.444 | 0.2547 | reject |
| low_vol_name | SL0.7/T1.5 | 233/0.293 | 168/0.34 | 0.2547 | reject |
| trend_alignment | SL0.7/T1.5 | 489/0.288 | 408/0.34 | 0.2463 | reject |
| gap_up_continuation | SL1.0/T2.0 | 511/0.441 | 426/0.333 | 0.2458 | reject |
| macd_turn_up | SL0.7/T1.5 | 1050/0.321 | 670/0.277 | 0.242 | reject |
| not_exhausted | SL0.7/T1.5 | 327/0.296 | 169/0.38 | 0.2289 | reject |
| gap_up_continuation | SL0.7/T1.5 | 634/0.348 | 564/0.281 | 0.2271 | reject |
| pdh_break_fresh | SL0.7/T1.5 | 441/0.268 | 333/0.323 | 0.2241 | reject |
| stoch_not_overbought | SL1.0/T2.0 | 521/0.353 | 266/0.269 | 0.2013 | reject |
| obv_confirm | SL0.7/T1.5 | 663/0.351 | 390/0.261 | 0.1878 | reject |
| stoch_not_overbought | SL0.7/T1.5 | 580/0.282 | 285/0.229 | 0.1867 | reject |
| obv_confirm | SL1.0/T2.0 | 573/0.425 | 341/0.27 | 0.1458 | reject |
| squeeze_expansion | SL0.7/T1.5 | 232/0.257 | 129/0.521 | 0.045 | reject |
| squeeze_expansion | SL1.0/T2.0 | 232/0.253 | 126/0.569 | 0.0002 | reject |
| early_bird | SL0.7/T1.5 | 0/0.0 | 0/0.0 | -5.0 | reject |
| early_bird | SL1.0/T2.0 | 0/0.0 | 0/0.0 | -5.0 | reject |

## Single-term sweeps over the expanded space: 0 keeps / 1116 tested

Top 15 by band score:

| term | FIT n/PF | VAL n/PF | score |
|---|---|---|---|
| x_range_vs_avg20<=0.909095(q0.1)@SL1.0/T2.0 | 233/0.552 | 167/0.595 | 0.5178 |
| x_adx_slope3<=-3.572352(q0.1)@SL1.0/T2.0 | 248/0.47 | 126/0.471 | 0.4696 |
| wick_skew_pct>=0.077361(q0.9)@SL1.0/T2.0 | 254/0.506 | 146/0.484 | 0.4665 |
| x_range_vs_avg20<=0.909095(q0.1)@SL0.7/T1.5 | 234/0.546 | 170/0.492 | 0.4493 |
| x_dist_dayhigh_atr<=0.077958(q0.1)@SL1.0/T2.0 | 224/0.467 | 170/0.446 | 0.4284 |
| x_pm_pre_entry_momentum_score<=57.264588(q0.1)@SL1.0/T2.0 | 145/0.502 | 196/0.45 | 0.4084 |
| x_bb_pos>=1.055329(q0.7)@SL1.0/T2.0 | 575/0.405 | 327/0.405 | 0.4042 |
| upper_wick_pct>=0.121118(q0.9)@SL1.0/T2.0 | 240/0.424 | 183/0.41 | 0.3987 |
| x_adx<=19.84037(q0.3)@SL1.0/T2.0 | 591/0.421 | 309/0.404 | 0.39 |
| x_pm_sig5_adx_calc<=19.84037(q0.3)@SL1.0/T2.0 | 591/0.421 | 309/0.404 | 0.39 |
| x_roc6>=0.888804(q0.7)@SL1.0/T2.0 | 641/0.398 | 388/0.394 | 0.3899 |
| x_ema20_dist_atr>=2.431498(q0.7)@SL1.0/T2.0 | 580/0.39 | 350/0.395 | 0.3856 |
| x_kelt_pos>=1.310499(q0.7)@SL1.0/T2.0 | 580/0.39 | 350/0.395 | 0.3856 |
| atr_pct>=0.005701(q0.9)@SL1.0/T2.0 | 210/0.396 | 191/0.409 | 0.3846 |
| x_atr_pct>=0.0057(q0.9)@SL1.0/T2.0 | 210/0.396 | 191/0.409 | 0.3846 |

## Top TPE combinations (3,000 trials)

| score | FIT n/PF | VAL n/PF | config |
|---|---|---|---|
| 1.0216 | 12/1.733 | 8/1.325 | SL0.7/T2.0 [x_pm_sig5_adx_calc<=22.348337] g={'min_slot': '10:00', 'max_slot': '11:00', 'top_n': 3} |
| 1.0216 | 12/1.733 | 8/1.325 | SL0.7/T2.0 [x_pm_sig5_adx_calc<=22.348337] g={'max_slot': '11:00', 'top_n': 3} |
| 1.0112 | 12/1.994 | 8/1.435 | SL1.0/T2.5 [x_pm_sig5_adx_calc<=22.348337] g={'min_slot': '10:00', 'max_slot': '11:00', 'top_n': 3} |
| 1.0081 | 22/1.092 | 11/1.197 | SL0.7/T2.0 [x_pm_sig5_adx_calc<=28.034374] g={'max_slot': '11:00', 'top_n': 3} |
| 0.9852 | 17/1.01 | 11/0.996 | SL1.0/T2.5 [close_loc<=0.827585] g={'min_slot': '10:30', 'max_slot': '11:00'} |
| 0.94 | 25/1.025 | 16/0.978 | SL1.0/T2.5 [x_adx<=22.348337] g={'min_slot': '10:00', 'max_slot': '11:00'} |
| 0.8386 | 35/1.022 | 18/0.92 | SL1.0/T2.5 [x_adx<=25.045184] g={'min_slot': '10:30', 'max_slot': '11:00'} |
| 0.8386 | 35/1.022 | 18/0.92 | SL1.0/T2.5 [x_adx<=25.045184] g={'min_slot': '10:00', 'max_slot': '11:00'} |
| 0.8386 | 35/1.022 | 18/0.92 | SL1.0/T2.5 [x_pm_sig5_adx_calc<=25.045184] g={'min_slot': '10:00', 'max_slot': '11:00'} |
| 0.8386 | 35/1.022 | 18/0.92 | SL1.0/T2.5 [x_adx<=25.045184] g={'max_slot': '11:00'} |
| 0.8054 | 38/0.851 | 18/0.909 | SL1.0/T2.5 [x_pm_pre_entry_momentum_score<=71.979113] g={'min_slot': '10:00', 'max_slot': '11:00'} |
| 0.7917 | 9/0.892 | 6/1.017 | SL0.7/T2.0 [x_roc12<=0.530157] g={'max_slot': '11:00', 'top_n': 3} |

## Confirmations (full TRAIN; TEST scored once if in band)

- SL0.7/T2.0 [x_pm_sig5_adx_calc<=22.348337] g={'min_slot': '10:00', 'max_slot': '11:00', 'top_n': 3}: TRAIN n=20 PF=1.564 net=Rs4,459 | TEST n=3 PF=0.313 net=Rs-1,278
  - verdict: **REJECT: test_pf_gt_1.40;test_net_pos;test_n_ge_5;train_dom_ok;test_dom_ok**
- SL0.7/T2.0 [x_pm_sig5_adx_calc<=22.348337] g={'max_slot': '11:00', 'top_n': 3}: TRAIN n=20 PF=1.564 net=Rs4,459 | TEST n=3 PF=0.313 net=Rs-1,278
  - verdict: **REJECT: test_pf_gt_1.40;test_net_pos;test_n_ge_5;train_dom_ok;test_dom_ok**
- SL0.7/T2.0 [x_pm_sig5_adx_calc<=28.034374] g={'max_slot': '11:00', 'top_n': 3}: TRAIN n=33 PF=1.126 net=Rs1,967
  - verdict: **REJECT: TRAIN PF outside [1.30,1.80]**
- SL1.0/T2.5 [close_loc<=0.827585] g={'min_slot': '10:30', 'max_slot': '11:00'}: TRAIN n=28 PF=1.004 net=Rs68
  - verdict: **REJECT: TRAIN PF outside [1.30,1.80]**
- SL1.0/T2.5 [x_adx<=22.348337] g={'min_slot': '10:00', 'max_slot': '11:00'}: TRAIN n=41 PF=1.005 net=Rs102
  - verdict: **REJECT: TRAIN PF outside [1.30,1.80]**
- SL1.0/T2.5 [x_adx<=25.045184] g={'min_slot': '10:30', 'max_slot': '11:00'}: TRAIN n=53 PF=0.985 net=Rs-430
  - verdict: **REJECT: TRAIN PF outside [1.30,1.80]**
- SL1.0/T2.5 [x_adx<=25.045184] g={'min_slot': '10:00', 'max_slot': '11:00'}: TRAIN n=53 PF=0.985 net=Rs-430
  - verdict: **REJECT: TRAIN PF outside [1.30,1.80]**
- SL1.0/T2.5 [x_pm_sig5_adx_calc<=25.045184] g={'min_slot': '10:00', 'max_slot': '11:00'}: TRAIN n=53 PF=0.985 net=Rs-430
  - verdict: **REJECT: TRAIN PF outside [1.30,1.80]**
- SL1.0/T2.5 [x_adx<=25.045184] g={'max_slot': '11:00'}: TRAIN n=53 PF=0.985 net=Rs-430
  - verdict: **REJECT: TRAIN PF outside [1.30,1.80]**
- SL1.0/T2.5 [x_pm_pre_entry_momentum_score<=71.979113] g={'min_slot': '10:00', 'max_slot': '11:00'}: TRAIN n=56 PF=0.871 net=Rs-4,204
  - verdict: **REJECT: TRAIN PF outside [1.30,1.80]**
- SL0.7/T2.0 [x_roc12<=0.530157] g={'max_slot': '11:00', 'top_n': 3}: TRAIN n=15 PF=0.944 net=Rs-440
  - verdict: **REJECT: TRAIN PF outside [1.30,1.80]**
- SL1.0/T2.5 [x_adx<=28.034374] g={'min_slot': '10:00', 'max_slot': '11:00'}: TRAIN n=67 PF=0.883 net=Rs-4,348
  - verdict: **REJECT: TRAIN PF outside [1.30,1.80]**
- SL1.0/T2.5 [x_adx<=22.348337;x_pm_sig5_vol_ratio20>=1.745488] g={'min_slot': '10:00', 'max_slot': '11:00'}: TRAIN n=30 PF=1.254 net=Rs3,660
  - verdict: **REJECT: TRAIN PF outside [1.30,1.80]**
- SL0.5/T2.5 [x_roc12<=2.218881] g={'min_slot': '10:00', 'max_slot': '11:00', 'top_n': 1}: TRAIN n=21 PF=0.799 net=Rs-1,968
  - verdict: **REJECT: TRAIN PF outside [1.30,1.80]**
- SL1.0/T1.25 [x_adx<=17.263214] g={'min_slot': '10:00', 'max_slot': '11:00'}: TRAIN n=18 PF=1.078 net=Rs572
  - verdict: **REJECT: TRAIN PF outside [1.30,1.80]**
- SL0.7/T2.0 [x_pm_pre_entry_momentum_score<=69.356538] g={'max_slot': '11:00', 'top_n': 3}: TRAIN n=29 PF=0.79 net=Rs-3,257
  - verdict: **REJECT: TRAIN PF outside [1.30,1.80]**
- SL1.0/T2.5 [x_adx<=31.528786] g={'min_slot': '10:00', 'max_slot': '11:00'}: TRAIN n=84 PF=0.757 net=Rs-12,411
  - verdict: **REJECT: TRAIN PF outside [1.30,1.80]**
- SL1.0/T2.5 [x_above_pdh>=0.5] g={'min_slot': '10:30', 'max_slot': '11:00'}: TRAIN n=60 PF=0.727 net=Rs-12,601
  - verdict: **REJECT: TRAIN PF outside [1.30,1.80]**