# A_MOD_BREAK_C1_LOW (SHORT) — PARAMETER_SWEEP_SUMMARY

_Generated 2026-07-03. Research-only; NO live trades; NO final_setup_conf.py edits._

Stage-3 one-knob-at-a-time sweeps from the baseline config, scored on FIT+VAL with the band objective (tent at PF 1.8, gap penalty 0.80). Baseline FIT/VAL band score is the reference; `improve` = higher score.

Total sweeps: **167** | improve: 65 | worse: 84

## exit

| knob | old | new | FIT n/PF | VAL n/PF | score | vs baseline |
|---|---|---|---|---|---|---|
| sl_pct | 1.1 | 1.5 | 54/0.683 | 19/0.783 | 0.603 | improve |
| sl_pct | 1.1 | 0.7 | 180/0.343 | 93/0.471 | 0.2406 | improve |
| tgt_pct | 1.0 | 0.6 | 109/0.339 | 55/0.565 | 0.1582 | improve |
| sl_pct | 1.1 | 0.85 | 174/0.447 | 84/0.819 | 0.1494 | improve |
| sl_pct | 1.1 | 1.0 | 128/0.363 | 64/0.668 | 0.119 | improve |
| sl_pct | 1.1 | 0.5 | 151/0.252 | 90/0.419 | 0.1184 | improve |
| tgt_pct | 1.0 | 0.8 | 109/0.378 | 55/0.761 | 0.0716 | worse |
| tgt_pct | 1.0 | 1.25 | 109/0.433 | 55/0.922 | 0.0418 | worse |
| tgt_pct | 1.0 | 1.5 | 109/0.422 | 55/1.036 | -0.0692 | worse |
| tgt_pct | 1.0 | 2.0 | 109/0.449 | 55/1.172 | -0.1294 | worse |
| sl_pct | 1.1 | 1.2 | 89/0.372 | 46/1.0 | -0.1304 | worse |
| tgt_pct | 1.0 | 2.5 | 109/0.432 | 55/1.24 | -0.2144 | worse |

## filter

| knob | old | new | FIT n/PF | VAL n/PF | score | vs baseline |
|---|---|---|---|---|---|---|
| mask vol_ratio>= | 1.955814 | 4.449577 (q0.9) | 29/0.381 | 13/0.356 | 0.336 | improve |
| mask vol_ratio>= | 1.955814 | 1.862731 (q0.3) | 115/0.459 | 57/0.779 | 0.203 | improve |
| mask vol_ratio>= | 1.955814 | 1.599691 (q0.1) | 140/0.427 | 65/0.78 | 0.1446 | improve |
| mask vol_ratio>= | 1.955814 | 2.880785 (q0.7) | 66/0.519 | 31/1.007 | 0.1286 | improve |
| drop mask vol_ratio>=1.955814 | vol_ratio>=1.955814 | dropped | 149/0.405 | 72/0.777 | 0.1074 | improve |
| mask vol_ratio>= | 1.955814 | 2.245648 (q0.5) | 91/0.44 | 45/0.876 | 0.0912 | worse |

## guard

| knob | old | new | FIT n/PF | VAL n/PF | score | vs baseline |
|---|---|---|---|---|---|---|
| max_slot | - | 13:00 | 60/0.59 | 31/0.572 | 0.5576 | improve |
| top_n | - | 2 | 22/0.535 | 10/0.749 | 0.3638 | improve |
| max_slot | - | 12:00 | 29/0.569 | 10/0.86 | 0.3362 | improve |
| max_slot | - | 12:30 | 45/0.498 | 22/0.821 | 0.2396 | improve |
| max_slot | - | 14:00 | 91/0.465 | 47/0.758 | 0.2306 | improve |
| max_positions | 20 | 5 | 101/0.458 | 50/0.758 | 0.218 | improve |
| min_slot | - | 09:30 | 109/0.428 | 55/0.842 | 0.0968 | flat |
| min_slot | - | 10:00 | 109/0.428 | 55/0.842 | 0.0968 | flat |
| min_slot | - | 09:45 | 109/0.428 | 55/0.842 | 0.0968 | flat |
| min_slot | - | 11:00 | 109/0.428 | 55/0.842 | 0.0968 | flat |
| min_slot | - | 10:30 | 109/0.428 | 55/0.842 | 0.0968 | flat |
| max_positions | 20 | 10 | 109/0.428 | 55/0.842 | 0.0968 | flat |
| max_slot | - | 14:30 | 109/0.428 | 55/0.842 | 0.0968 | flat |
| daily_loss_rs | 0.0 | 4000.0 | 109/0.428 | 55/0.842 | 0.0968 | flat |
| daily_loss_rs | 0.0 | 2000.0 | 104/0.441 | 51/0.895 | 0.0778 | worse |
| min_slot | - | 12:00 | 82/0.41 | 47/0.914 | 0.0068 | worse |
| top_n | - | 3 | 34/0.502 | 15/1.244 | -0.0916 | worse |
| top_n | - | 1 | 10/1.937 | 7/1.003 | -4.3 | worse |
| max_slot | - | 11:30 | 15/0.52 | 5/0.836 | -4.5 | worse |

## indicator/price-action

| knob | old | new | FIT n/PF | VAL n/PF | score | vs baseline |
|---|---|---|---|---|---|---|
| +mask rs_pct>= | - | 0.846421 (q0.8) | 26/0.486 | 10/0.559 | 0.4276 | improve |
| +mask vol_ratio>= | - | 3.442125 (q0.8) | 54/0.474 | 20/0.564 | 0.402 | improve |
| +mask wick_skew_pct<= | - | -0.011269 (q0.5) | 72/0.543 | 28/0.778 | 0.355 | improve |
| +mask close_loc>= | - | 0.125005 (q0.5) | 38/0.536 | 21/0.827 | 0.3032 | improve |
| +mask body_pct<= | - | 0.902928 (q0.8) | 59/0.544 | 28/0.878 | 0.2768 | improve |
| +mask rs_pct>= | - | -0.633462 (q0.5) | 51/0.409 | 26/0.581 | 0.2714 | improve |
| +mask signal_range_pct<= | - | 0.54102 (q0.8) | 29/0.425 | 21/0.619 | 0.2698 | improve |
| +mask vwap_dist_atr>= | - | -3.592093 (q0.5) | 50/0.396 | 24/0.557 | 0.2672 | improve |
| +mask upper_wick_pct>= | - | 0.021064 (q0.5) | 21/0.344 | 12/0.472 | 0.2416 | improve |
| +mask lower_wick_pct>= | - | 0.043702 (q0.5) | 58/0.541 | 25/0.961 | 0.205 | improve |
| +mask quality_score<= | - | 85.832373 (q0.8) | 69/0.431 | 34/0.742 | 0.1822 | improve |
| +mask lower_wick_pct<= | - | 0.103666 (q0.8) | 77/0.426 | 44/0.742 | 0.1732 | improve |
| +mask rs_pct>= | - | -1.966975 (q0.2) | 77/0.399 | 33/0.696 | 0.1614 | improve |
| +mask quality_score>= | - | 55.626711 (q0.5) | 79/0.468 | 36/0.864 | 0.1512 | improve |
| +mask atr_pct>= | - | 0.002013 (q0.2) | 108/0.431 | 54/0.811 | 0.127 | improve |
| +mask close_loc<= | - | 0.265315 (q0.8) | 98/0.417 | 50/0.781 | 0.1258 | improve |
| +mask signal_range_pct>= | - | 0.37312 (q0.5) | 104/0.45 | 53/0.857 | 0.1244 | improve |
| +mask atr_pct<= | - | 0.00356 (q0.8) | 57/0.414 | 35/0.776 | 0.1244 | improve |
| +mask signal_range_pct>= | - | 0.25378 (q0.2) | 109/0.428 | 54/0.811 | 0.1216 | improve |
| +mask wick_skew_pct<= | - | -0.079093 (q0.2) | 35/0.582 | 16/1.16 | 0.1196 | improve |
| +mask vwap_dist_atr>= | - | -5.420678 (q0.2) | 84/0.383 | 41/0.725 | 0.1094 | improve |
| +mask body_pct>= | - | 0.745475 (q0.5) | 95/0.435 | 48/0.844 | 0.1078 | improve |
| +mask close_loc>= | - | 0.0 (q0.2) | 109/0.428 | 55/0.842 | 0.0968 | flat |
| +mask vol_ratio<= | - | 2.245648 (q0.5) | 18/0.372 | 10/0.716 | 0.0968 | flat |
| +mask upper_wick_pct>= | - | 0.0 (q0.2) | 109/0.428 | 55/0.842 | 0.0968 | flat |
| +mask vol_ratio>= | - | 1.72358 (q0.2) | 109/0.428 | 55/0.842 | 0.0968 | flat |
| +mask lower_wick_pct>= | - | 0.0 (q0.2) | 109/0.428 | 55/0.842 | 0.0968 | flat |
| +mask vol_ratio>= | - | 2.245648 (q0.5) | 91/0.44 | 45/0.876 | 0.0912 | worse |
| +mask wick_skew_pct<= | - | 0.04382 (q0.8) | 99/0.437 | 53/0.883 | 0.0802 | worse |
| +mask vwap_dist_atr<= | - | -1.882604 (q0.8) | 81/0.416 | 46/0.836 | 0.08 | worse |
| +mask upper_wick_pct<= | - | 0.078846 (q0.8) | 99/0.437 | 54/0.89 | 0.0746 | worse |
| +mask upper_wick_pct<= | - | 0.0 (q0.2) | 80/0.462 | 40/0.957 | 0.066 | worse |
| +mask quality_score>= | - | 36.055657 (q0.2) | 106/0.434 | 47/0.896 | 0.0644 | worse |
| +mask wick_skew_pct>= | - | -0.079093 (q0.2) | 74/0.362 | 39/0.753 | 0.0492 | worse |
| +mask body_pct>= | - | 0.611111 (q0.2) | 106/0.419 | 54/0.89 | 0.0422 | worse |
| +mask upper_wick_pct<= | - | 0.021064 (q0.5) | 88/0.448 | 43/0.974 | 0.0272 | worse |
| +mask atr_pct>= | - | 0.00356 (q0.8) | 52/0.442 | 20/0.97 | 0.0196 | worse |
| +mask vwap_dist_atr<= | - | -5.420678 (q0.2) | 25/0.606 | 14/1.346 | 0.014 | worse |
| +mask atr_pct>= | - | 0.002646 (q0.5) | 97/0.446 | 48/1.003 | 0.0004 | worse |
| +mask rs_pct<= | - | -1.966975 (q0.2) | 32/0.507 | 22/1.149 | -0.0066 | worse |
| +mask rs_pct<= | - | 0.846421 (q0.8) | 83/0.408 | 45/0.927 | -0.0072 | worse |
| +mask close_loc<= | - | 0.125005 (q0.5) | 71/0.373 | 34/0.851 | -0.0094 | worse |
| +mask lower_wick_pct<= | - | 0.043702 (q0.5) | 51/0.316 | 30/0.759 | -0.0384 | worse |
| +mask quality_score<= | - | 55.626711 (q0.5) | 30/0.333 | 19/0.802 | -0.0422 | worse |
| +mask signal_range_pct>= | - | 0.54102 (q0.8) | 80/0.428 | 34/1.05 | -0.0696 | worse |
| +mask body_pct>= | - | 0.902928 (q0.8) | 50/0.314 | 27/0.81 | -0.0828 | worse |
| +mask quality_score>= | - | 85.832373 (q0.8) | 40/0.422 | 21/1.053 | -0.0828 | worse |
| +mask vol_ratio<= | - | 3.442125 (q0.8) | 55/0.386 | 35/1.046 | -0.142 | worse |
| +mask vwap_dist_atr<= | - | -3.592093 (q0.5) | 59/0.455 | 31/1.22 | -0.157 | worse |
| +mask close_loc<= | - | 0.0 (q0.2) | 28/0.26 | 24/0.79 | -0.164 | worse |
| +mask lower_wick_pct<= | - | 0.0 (q0.2) | 28/0.26 | 24/0.79 | -0.164 | worse |
| +mask rs_pct<= | - | -0.633462 (q0.5) | 58/0.444 | 29/1.207 | -0.1664 | worse |
| +mask wick_skew_pct>= | - | -0.011269 (q0.5) | 37/0.249 | 27/0.911 | -0.2806 | worse |
| +mask lower_wick_pct>= | - | 0.103666 (q0.8) | 32/0.43 | 11/1.549 | -0.4652 | worse |
| +mask vwap_dist_atr>= | - | -1.882604 (q0.8) | 28/0.465 | 9/0.873 | -4.1 | worse |
| +mask atr_pct<= | - | 0.002646 (q0.5) | 12/0.291 | 7/0.279 | -4.3 | worse |
| +mask body_pct<= | - | 0.745475 (q0.5) | 14/0.38 | 7/0.829 | -4.3 | worse |
| +mask close_loc>= | - | 0.265315 (q0.8) | 11/0.523 | 5/1.903 | -4.5 | worse |
| +mask quality_score<= | - | 36.055657 (q0.2) | 3/0.285 | 8/0.566 | -4.7 | worse |
| +mask wick_skew_pct>= | - | 0.04382 (q0.8) | 10/0.348 | 2/0.117 | -4.8 | worse |
| +mask signal_range_pct<= | - | 0.37312 (q0.5) | 5/0.0 | 2/0.576 | -4.8 | worse |
| +mask atr_pct<= | - | 0.002013 (q0.2) | 1/0.0 | 1/inf | -4.9 | worse |
| +mask body_pct<= | - | 0.611111 (q0.2) | 3/0.835 | 1/0.0 | -4.9 | worse |
| +mask upper_wick_pct>= | - | 0.078846 (q0.8) | 10/0.348 | 1/0.0 | -4.9 | worse |
| +mask vol_ratio<= | - | 1.72358 (q0.2) | 0/0.0 | 0/0.0 | -5.0 | worse |
| +mask signal_range_pct<= | - | 0.25378 (q0.2) | 0/0.0 | 1/inf | -5.0 | worse |

## pre-momentum

| knob | old | new | FIT n/PF | VAL n/PF | score | vs baseline |
|---|---|---|---|---|---|---|
| +premom pre1_adx>= | - | 42.929242 (q0.8) | 17/0.702 | 12/0.667 | 0.639 | improve |
| drop premom pre3_range_r<=0.202087 | pre3_range_r<=0.202087 | dropped | 746/0.426 | 374/0.465 | 0.3948 | improve |
| premom pre3_range_r<= | 0.202087 | 0.555485 (q0.9) | 611/0.449 | 286/0.529 | 0.385 | improve |
| premom pre5_mom_r>= | 0.425861 | -0.017491 (q0.1) | 832/0.353 | 560/0.393 | 0.321 | improve |
| +premom sig5_vol_ratio20>= | - | 3.47122 (q0.8) | 53/0.449 | 20/0.619 | 0.313 | improve |
| +premom sig5_rsi_dir<= | - | 59.08515 (q0.2) | 24/0.426 | 13/0.576 | 0.306 | improve |
| drop premom pre5_mom_r>=0.425861 | pre5_mom_r>=0.425861 | dropped | 841/0.343 | 602/0.39 | 0.3054 | improve |
| premom pre3_range_r<= | 0.202087 | 0.358687 (q0.7) | 384/0.495 | 158/0.757 | 0.2854 | improve |
| premom pre5_mom_r>= | 0.425861 | 0.176027 (q0.3) | 723/0.384 | 398/0.53 | 0.2672 | improve |
| +premom sig5_rsi_dir>= | - | 71.286547 (q0.8) | 26/0.446 | 13/0.69 | 0.2508 | improve |
| +premom sig5_adx_calc>= | - | 27.694227 (q0.5) | 58/0.41 | 24/0.63 | 0.234 | improve |
| +premom sig5_vol_ratio20<= | - | 2.279791 (q0.5) | 20/0.392 | 14/0.592 | 0.232 | improve |
| +premom sig5_rsi_dir<= | - | 65.688599 (q0.5) | 55/0.449 | 27/0.727 | 0.2266 | improve |
| premom pre5_mom_r>= | 0.425861 | 0.268289 (q0.5) | 491/0.389 | 256/0.593 | 0.2258 | improve |
| +premom pre3_close_pos>= | - | 0.799988 (q0.5) | 48/0.413 | 30/0.652 | 0.2218 | improve |
| +premom pre1_adx>= | - | 30.073226 (q0.5) | 56/0.401 | 34/0.653 | 0.1994 | improve |
| +premom pre_entry_momentum_score>= | - | 65.08872 (q0.5) | 49/0.419 | 27/0.7 | 0.1942 | improve |
| +premom pre3_close_pos<= | - | 0.5 (q0.2) | 37/0.757 | 15/1.464 | 0.1914 | improve |
| +premom sig5_adx_calc<= | - | 36.90615 (q0.8) | 80/0.465 | 47/0.828 | 0.1746 | improve |
| +premom sig5_adx_calc>= | - | 22.103036 (q0.2) | 85/0.453 | 41/0.811 | 0.1666 | improve |
| +premom pre_entry_momentum_score<= | - | 73.12409 (q0.8) | 101/0.444 | 50/0.804 | 0.156 | improve |
| premom pre3_range_r<= | 0.202087 | 0.192758 (q0.3) | 97/0.396 | 46/0.712 | 0.1432 | improve |
| +premom pre_entry_momentum_score>= | - | 55.072385 (q0.2) | 92/0.427 | 51/0.79 | 0.1366 | improve |
| premom pre5_mom_r>= | 0.425861 | 0.366698 (q0.7) | 203/0.414 | 106/0.773 | 0.1268 | improve |
| +premom pre3_range_r<= | - | 0.435414 (q0.8) | 109/0.428 | 55/0.842 | 0.0968 | flat |
| +premom pre3_close_pos<= | - | 1.0 (q0.8) | 109/0.428 | 55/0.842 | 0.0968 | flat |
| +premom pre3_range_r<= | - | 0.263527 (q0.5) | 109/0.428 | 55/0.842 | 0.0968 | flat |
| +premom pre5_mom_r>= | - | 0.268289 (q0.5) | 109/0.428 | 55/0.842 | 0.0968 | flat |
| +premom pre5_mom_r>= | - | 0.112638 (q0.2) | 109/0.428 | 55/0.842 | 0.0968 | flat |
| +premom pre3_range_r>= | - | 0.152829 (q0.2) | 50/0.374 | 28/0.725 | 0.0932 | worse |
| +premom pre1_adx>= | - | 20.92282 (q0.2) | 93/0.393 | 49/0.781 | 0.0826 | worse |
| +premom sig5_vol_ratio20>= | - | 1.708072 (q0.2) | 109/0.428 | 52/0.86 | 0.0824 | worse |
| +premom pre5_mom_r>= | - | 0.43791 (q0.8) | 99/0.444 | 49/0.907 | 0.0736 | worse |
| +premom pre3_range_r<= | - | 0.152829 (q0.2) | 59/0.474 | 27/0.99 | 0.0612 | worse |
| +premom sig5_rsi_dir<= | - | 71.286547 (q0.8) | 83/0.422 | 42/0.896 | 0.0428 | worse |
| +premom sig5_vol_ratio20>= | - | 2.279791 (q0.5) | 89/0.437 | 41/0.955 | 0.0226 | worse |
| +premom sig5_rsi_dir>= | - | 59.08515 (q0.2) | 85/0.428 | 42/0.944 | 0.0152 | worse |
| premom pre3_range_r<= | 0.202087 | 0.116751 (q0.1) | 26/0.416 | 10/0.927 | 0.0072 | worse |
| +premom pre3_close_pos>= | - | 1.0 (q0.8) | 32/0.363 | 22/0.818 | -0.001 | worse |
| +premom pre3_close_pos>= | - | 0.5 (q0.2) | 72/0.31 | 41/0.726 | -0.0228 | worse |
| premom pre3_range_r<= | 0.202087 | 0.263527 (q0.5) | 223/0.423 | 94/0.988 | -0.029 | worse |
| +premom pre1_adx<= | - | 42.929242 (q0.8) | 92/0.385 | 43/0.906 | -0.0318 | worse |
| +premom sig5_adx_calc<= | - | 27.694227 (q0.5) | 51/0.449 | 31/1.051 | -0.0326 | worse |
| +premom sig5_rsi_dir>= | - | 65.688599 (q0.5) | 54/0.409 | 28/0.969 | -0.039 | worse |
| +premom pre_entry_momentum_score<= | - | 65.08872 (q0.5) | 60/0.435 | 28/1.042 | -0.0506 | worse |
| +premom sig5_vol_ratio20<= | - | 3.47122 (q0.8) | 56/0.408 | 35/0.982 | -0.0512 | worse |
| +premom sig5_adx_calc<= | - | 22.103036 (q0.2) | 24/0.349 | 14/0.94 | -0.1238 | worse |
| +premom pre3_close_pos<= | - | 0.799988 (q0.5) | 61/0.44 | 25/1.184 | -0.1552 | worse |
| +premom pre1_adx<= | - | 30.073226 (q0.5) | 53/0.46 | 21/1.267 | -0.1856 | worse |
| premom pre5_mom_r>= | 0.425861 | 0.557985 (q0.9) | 26/0.557 | 9/0.582 | -4.1 | worse |
| +premom sig5_adx_calc>= | - | 36.90615 (q0.8) | 29/0.349 | 8/0.96 | -4.2 | worse |
| +premom pre1_adx<= | - | 20.92282 (q0.2) | 16/0.676 | 6/1.908 | -4.4 | worse |
| +premom pre5_mom_r<= | - | 0.43791 (q0.8) | 10/0.299 | 6/0.5 | -4.4 | worse |
| +premom pre_entry_momentum_score>= | - | 73.12409 (q0.8) | 8/0.265 | 5/1.344 | -4.5 | worse |
| +premom pre_entry_momentum_score<= | - | 55.072385 (q0.2) | 17/0.433 | 4/3.991 | -4.6 | worse |
| +premom pre3_range_r>= | - | 0.263527 (q0.5) | 0/0.0 | 0/0.0 | -5.0 | worse |
| +premom pre5_mom_r<= | - | 0.112638 (q0.2) | 0/0.0 | 0/0.0 | -5.0 | worse |
| +premom pre3_range_r>= | - | 0.435414 (q0.8) | 0/0.0 | 0/0.0 | -5.0 | worse |
| +premom pre5_mom_r<= | - | 0.268289 (q0.5) | 0/0.0 | 0/0.0 | -5.0 | worse |
| +premom sig5_vol_ratio20<= | - | 1.708072 (q0.2) | 0/0.0 | 3/0.541 | -5.0 | worse |

## regime

| knob | old | new | FIT n/PF | VAL n/PF | score | vs baseline |
|---|---|---|---|---|---|---|
| +mask regime==BEAR | - | BEAR | 75/0.504 | 35/0.783 | 0.2808 | improve |
| +mask regime!=TREND | - | TREND | 99/0.457 | 55/0.842 | 0.149 | improve |
| +mask regime==NEUTRAL | - | NEUTRAL | 24/0.306 | 20/0.968 | -0.2236 | worse |
| +mask regime!=BEAR | - | BEAR | 34/0.27 | 20/0.968 | -0.2884 | worse |

## Best stable knobs (score-improving, FIT and VAL both alive)

- **pre-momentum / +premom pre1_adx>=** -> 42.929242 (q0.8) (FIT 17/0.702, VAL 12/0.667, score 0.639)
- **exit / sl_pct** -> 1.5 (FIT 54/0.683, VAL 19/0.783, score 0.603)
- **guard / max_slot** -> 13:00 (FIT 60/0.59, VAL 31/0.572, score 0.5576)
- **indicator/price-action / +mask rs_pct>=** -> 0.846421 (q0.8) (FIT 26/0.486, VAL 10/0.559, score 0.4276)
- **indicator/price-action / +mask vol_ratio>=** -> 3.442125 (q0.8) (FIT 54/0.474, VAL 20/0.564, score 0.402)
- **pre-momentum / drop premom pre3_range_r<=0.202087** -> dropped (FIT 746/0.426, VAL 374/0.465, score 0.3948)
- **pre-momentum / premom pre3_range_r<=** -> 0.555485 (q0.9) (FIT 611/0.449, VAL 286/0.529, score 0.385)
- **guard / top_n** -> 2 (FIT 22/0.535, VAL 10/0.749, score 0.3638)
- **indicator/price-action / +mask wick_skew_pct<=** -> -0.011269 (q0.5) (FIT 72/0.543, VAL 28/0.778, score 0.355)
- **guard / max_slot** -> 12:00 (FIT 29/0.569, VAL 10/0.86, score 0.3362)
- **filter / mask vol_ratio>=** -> 4.449577 (q0.9) (FIT 29/0.381, VAL 13/0.356, score 0.336)
- **pre-momentum / premom pre5_mom_r>=** -> -0.017491 (q0.1) (FIT 832/0.353, VAL 560/0.393, score 0.321)
- **pre-momentum / +premom sig5_vol_ratio20>=** -> 3.47122 (q0.8) (FIT 53/0.449, VAL 20/0.619, score 0.313)
- **pre-momentum / +premom sig5_rsi_dir<=** -> 59.08515 (q0.2) (FIT 24/0.426, VAL 13/0.576, score 0.306)
- **pre-momentum / drop premom pre5_mom_r>=0.425861** -> dropped (FIT 841/0.343, VAL 602/0.39, score 0.3054)

## Overfit-risk notes

- Any knob whose FIT PF explodes while VAL PF collapses is a knife-edge; the band objective already penalises the gap, and stage-5 adds neighborhood + dropout checks.
- Sweeps that push PF far above 1.80 are treated as overshoot, not success.

## PHASE 2 — standalone single-feature scan, 47 features x 9 quantiles x 2 ops (846 scans, added 2026-07-03)

Baseline exits 1.10/1.00, no pre-momentum, scored on FIT+VAL.

| feat | op | thr (q) | FIT n/PF | VAL n/PF | score |
|---|---|---|---|---|---|
| gap_pct | <= | -1.82609 (q0.1) | 304/0.482 | 85/0.55 | 0.4271 |
| rs_pct | <= | -2.712063 (q0.1) | 512/0.398 | 488/0.397 | 0.3955 |
| gap_pct | <= | -0.535148 (q0.3) | 724/0.411 | 403/0.435 | 0.3915 |
| vol_ratio | >= | 4.449577 (q0.9) | 722/0.396 | 503/0.403 | 0.3908 |
| gap_pct | <= | -0.228693 (q0.4) | 801/0.408 | 524/0.43 | 0.3899 |
| vol_ratio | >= | 3.442125 (q0.8) | 914/0.393 | 588/0.403 | 0.3851 |
| wick_skew_pct | <= | -0.119699 (q0.1) | 690/0.393 | 425/0.41 | 0.3792 |
| obv_slope6 | <= | -0.480717 (q0.6) | 1085/0.394 | 654/0.383 | 0.3748 |
| mfi14 | <= | 20.494592 (q0.3) | 916/0.379 | 563/0.376 | 0.374 |
| ema50_dist_atr | <= | -5.786527 (q0.1) | 440/0.409 | 236/0.389 | 0.3739 |
| rs_pct | >= | 0.846421 (q0.8) | 777/0.375 | 461/0.379 | 0.3717 |
| obv_slope6 | <= | -1.227447 (q0.1) | 618/0.402 | 415/0.384 | 0.3693 |
| vol_ratio | >= | 2.526091 (q0.6) | 1015/0.379 | 658/0.373 | 0.368 |
| ema20_dist_atr | >= | -1.582567 (q0.7) | 942/0.374 | 613/0.383 | 0.3674 |
| macd_hist_atr | >= | 0.21577 (q0.9) | 476/0.375 | 288/0.385 | 0.3664 |
| atr_pct | >= | 0.004166 (q0.9) | 733/0.364 | 449/0.366 | 0.3636 |
| obv_slope6 | <= | -0.977552 (q0.2) | 852/0.366 | 549/0.37 | 0.363 |
| day_high_dist_atr | <= | 6.350321 (q0.2) | 912/0.365 | 581/0.368 | 0.3628 |
| obv_slope6 | <= | -0.586313 (q0.5) | 1056/0.37 | 660/0.379 | 0.3624 |
| lower_wick_pct | >= | 0.103666 (q0.8) | 961/0.374 | 627/0.366 | 0.3604 |

**Key finding:** 0 of 846 single-feature slices reach min(FIT,VAL) PF >= 1.0 (best: gap_pct<=-1.83 at PF 0.48/0.55). The losing population is homogeneous across every indicator, price-action, volume, session-context and day-context dimension — there is no structural sub-population where this breakdown pattern is net-profitable at realistic costs.