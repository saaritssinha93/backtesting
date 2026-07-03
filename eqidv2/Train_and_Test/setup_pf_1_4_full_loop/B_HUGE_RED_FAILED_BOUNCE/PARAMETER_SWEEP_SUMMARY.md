# B_HUGE_RED_FAILED_BOUNCE (SHORT) — PARAMETER_SWEEP_SUMMARY

_Generated 2026-07-03. Research-only; NO live trades; NO final_setup_conf.py edits._

Stage-3 one-knob-at-a-time sweeps from the baseline config, scored on FIT+VAL with the band objective (tent at PF 1.8, gap penalty 0.80). Baseline FIT/VAL band score is the reference; `improve` = higher score.

Total sweeps: **168** | improve: 26 | worse: 116

## exit

| knob | old | new | FIT n/PF | VAL n/PF | score | vs baseline |
|---|---|---|---|---|---|---|
| sl_pct | 0.9 | 1.5 | 26/0.821 | 32/0.868 | 0.7834 | improve |
| tgt_pct | 1.25 | 1.5 | 22/0.773 | 26/0.616 | 0.4904 | improve |
| tgt_pct | 1.25 | 0.8 | 22/0.577 | 26/0.504 | 0.4456 | improve |
| sl_pct | 0.9 | 0.85 | 22/0.878 | 26/0.599 | 0.3758 | worse |
| tgt_pct | 1.25 | 1.0 | 22/0.764 | 26/0.548 | 0.3752 | worse |
| sl_pct | 0.9 | 0.7 | 20/0.592 | 25/0.449 | 0.3346 | worse |
| tgt_pct | 1.25 | 2.0 | 22/0.753 | 26/0.511 | 0.3174 | worse |
| sl_pct | 0.9 | 1.2 | 24/0.906 | 29/0.571 | 0.303 | worse |
| tgt_pct | 1.25 | 0.6 | 22/0.473 | 26/0.372 | 0.2912 | worse |
| tgt_pct | 1.25 | 2.5 | 22/0.84 | 26/0.491 | 0.2118 | worse |
| sl_pct | 0.9 | 1.1 | 23/1.048 | 29/0.56 | 0.1696 | worse |
| sl_pct | 0.9 | 0.5 | 15/0.786 | 23/0.368 | 0.0336 | worse |
| sl_pct | 0.9 | 1.0 | 22/1.236 | 27/0.546 | -0.006 | worse |

## guard

| knob | old | new | FIT n/PF | VAL n/PF | score | vs baseline |
|---|---|---|---|---|---|---|
| max_slot | - | 14:00 | 19/0.831 | 20/0.908 | 0.7694 | improve |
| daily_loss_rs | 0.0 | 2000.0 | 22/0.848 | 25/0.686 | 0.5564 | improve |
| top_n | - | 2 | 22/0.848 | 25/0.685 | 0.5546 | improve |
| top_n | - | 1 | 17/0.775 | 20/0.609 | 0.4762 | improve |
| min_slot | - | 10:00 | 22/0.848 | 26/0.621 | 0.4394 | flat |
| min_slot | - | 09:30 | 22/0.848 | 26/0.621 | 0.4394 | flat |
| min_slot | - | 09:45 | 22/0.848 | 26/0.621 | 0.4394 | flat |
| max_slot | - | 14:30 | 22/0.848 | 26/0.621 | 0.4394 | flat |
| min_slot | - | 10:30 | 22/0.848 | 26/0.621 | 0.4394 | flat |
| min_slot | - | 11:00 | 22/0.848 | 26/0.621 | 0.4394 | flat |
| top_n | - | 3 | 22/0.848 | 26/0.621 | 0.4394 | flat |
| max_positions | 20 | 5 | 22/0.848 | 26/0.621 | 0.4394 | flat |
| daily_loss_rs | 0.0 | 4000.0 | 22/0.848 | 26/0.621 | 0.4394 | flat |
| max_positions | 20 | 10 | 22/0.848 | 26/0.621 | 0.4394 | flat |
| min_slot | - | 12:00 | 17/1.051 | 23/0.46 | -0.0128 | worse |
| max_slot | - | 13:00 | 12/0.611 | 8/1.022 | -4.2 | worse |
| max_slot | - | 12:30 | 7/0.351 | 3/8.583 | -4.7 | worse |
| max_slot | - | 12:00 | 5/0.465 | 3/8.583 | -4.7 | worse |
| max_slot | - | 11:30 | 3/1.228 | 1/inf | -4.9 | worse |

## indicator/price-action

| knob | old | new | FIT n/PF | VAL n/PF | score | vs baseline |
|---|---|---|---|---|---|---|
| +mask rs_pct>= | - | -0.85198 (q0.5) | 18/0.897 | 15/0.847 | 0.807 | improve |
| +mask signal_range_pct<= | - | 0.536963 (q0.5) | 15/0.987 | 19/0.84 | 0.7224 | improve |
| +mask vwap_dist_atr>= | - | -2.60068 (q0.8) | 19/0.703 | 15/0.705 | 0.7014 | improve |
| +mask close_loc<= | - | 0.264187 (q0.8) | 11/0.699 | 19/0.716 | 0.6854 | improve |
| +mask wick_skew_pct>= | - | -0.120104 (q0.2) | 17/0.94 | 21/0.74 | 0.58 | improve |
| +mask wick_skew_pct<= | - | 0.066302 (q0.8) | 19/0.704 | 22/0.873 | 0.5688 | improve |
| +mask signal_range_pct<= | - | 0.897446 (q0.8) | 17/0.816 | 23/0.66 | 0.5352 | improve |
| +mask upper_wick_pct<= | - | 0.130377 (q0.8) | 20/0.611 | 22/0.707 | 0.5342 | improve |
| +mask body_pct>= | - | 0.604701 (q0.2) | 18/0.599 | 21/0.682 | 0.5326 | improve |
| +mask upper_wick_pct<= | - | 0.035822 (q0.5) | 12/0.91 | 14/0.677 | 0.4906 | improve |
| +mask atr_pct<= | - | 0.002778 (q0.5) | 16/0.992 | 17/0.704 | 0.4736 | improve |
| +mask quality_score<= | - | 105.834628 (q0.8) | 22/0.848 | 26/0.621 | 0.4394 | flat |
| +mask lower_wick_pct>= | - | 0.0 (q0.2) | 22/0.848 | 26/0.621 | 0.4394 | flat |
| +mask upper_wick_pct>= | - | 0.0 (q0.2) | 22/0.848 | 26/0.621 | 0.4394 | flat |
| +mask close_loc>= | - | 0.0 (q0.2) | 22/0.848 | 26/0.621 | 0.4394 | flat |
| +mask vwap_dist_atr>= | - | -6.454222 (q0.2) | 22/0.848 | 26/0.621 | 0.4394 | flat |
| +mask lower_wick_pct<= | - | 0.163242 (q0.8) | 20/0.965 | 21/0.667 | 0.4286 | worse |
| +mask upper_wick_pct>= | - | 0.035822 (q0.5) | 10/0.778 | 12/0.574 | 0.4108 | worse |
| +mask atr_pct<= | - | 0.004157 (q0.8) | 16/0.992 | 23/0.66 | 0.3944 | worse |
| +mask vwap_dist_atr>= | - | -4.480017 (q0.5) | 22/0.848 | 24/0.595 | 0.3926 | worse |
| +mask rs_pct<= | - | 0.754688 (q0.8) | 15/0.86 | 21/0.598 | 0.3884 | worse |
| +mask rs_pct>= | - | -2.445118 (q0.2) | 22/0.848 | 24/0.592 | 0.3872 | worse |
| +mask wick_skew_pct<= | - | -0.016883 (q0.5) | 16/0.77 | 14/0.496 | 0.2768 | worse |
| +mask close_loc>= | - | 0.130429 (q0.5) | 15/1.043 | 14/0.559 | 0.1718 | worse |
| +mask vol_ratio<= | - | 3.014613 (q0.5) | 13/1.264 | 21/0.629 | 0.121 | worse |
| +mask atr_pct>= | - | 0.00205 (q0.2) | 15/0.98 | 19/0.486 | 0.0908 | worse |
| +mask vol_ratio>= | - | 2.016355 (q0.2) | 16/0.844 | 16/0.407 | 0.0574 | worse |
| +mask quality_score<= | - | 64.105917 (q0.5) | 18/1.04 | 20/0.491 | 0.0518 | worse |
| +mask body_pct<= | - | 0.888895 (q0.8) | 20/1.15 | 21/0.514 | 0.0052 | worse |
| +mask body_pct<= | - | 0.739914 (q0.5) | 17/0.82 | 17/0.314 | -0.0908 | worse |
| +mask quality_score>= | - | 38.389287 (q0.2) | 15/1.019 | 16/0.396 | -0.1024 | worse |
| +mask lower_wick_pct>= | - | 0.066828 (q0.5) | 14/1.004 | 13/0.38 | -0.1192 | worse |
| +mask signal_range_pct<= | - | 0.334351 (q0.2) | 11/0.612 | 10/1.556 | -0.1432 | worse |
| +mask vol_ratio<= | - | 4.841589 (q0.8) | 18/1.779 | 23/0.636 | -0.2784 | worse |
| +mask signal_range_pct>= | - | 0.334351 (q0.2) | 11/1.188 | 16/0.372 | -0.2808 | worse |
| +mask lower_wick_pct<= | - | 0.066828 (q0.5) | 8/0.674 | 13/1.139 | -4.2 | worse |
| +mask atr_pct<= | - | 0.00205 (q0.2) | 7/0.63 | 7/2.111 | -4.3 | worse |
| +mask upper_wick_pct<= | - | 0.0 (q0.2) | 8/0.47 | 7/0.59 | -4.3 | worse |
| +mask close_loc<= | - | 0.130429 (q0.5) | 7/0.579 | 12/0.71 | -4.3 | worse |
| +mask close_loc>= | - | 0.264187 (q0.8) | 11/0.982 | 7/0.464 | -4.3 | worse |
| +mask quality_score<= | - | 38.389287 (q0.2) | 7/0.435 | 10/1.141 | -4.3 | worse |
| +mask signal_range_pct>= | - | 0.536963 (q0.5) | 7/0.63 | 7/0.329 | -4.3 | worse |
| +mask wick_skew_pct>= | - | -0.016883 (q0.5) | 6/1.142 | 12/0.869 | -4.4 | worse |
| +mask vol_ratio<= | - | 2.016355 (q0.2) | 6/0.86 | 10/1.082 | -4.4 | worse |
| +mask atr_pct>= | - | 0.002778 (q0.5) | 6/0.623 | 9/0.515 | -4.4 | worse |
| +mask body_pct>= | - | 0.739914 (q0.5) | 5/0.926 | 9/2.815 | -4.5 | worse |
| +mask vol_ratio>= | - | 3.014613 (q0.5) | 9/0.583 | 5/0.569 | -4.5 | worse |
| +mask wick_skew_pct<= | - | -0.120104 (q0.2) | 5/0.542 | 5/0.315 | -4.5 | worse |
| +mask rs_pct>= | - | 0.754688 (q0.8) | 7/0.813 | 5/0.72 | -4.5 | worse |
| +mask rs_pct<= | - | -0.85198 (q0.5) | 4/0.637 | 11/0.419 | -4.6 | worse |
| +mask body_pct<= | - | 0.604701 (q0.2) | 4/5.322 | 5/0.399 | -4.6 | worse |
| +mask quality_score>= | - | 64.105917 (q0.5) | 4/0.397 | 6/1.193 | -4.6 | worse |
| +mask vwap_dist_atr<= | - | -2.60068 (q0.8) | 3/1.798 | 11/0.532 | -4.7 | worse |
| +mask signal_range_pct>= | - | 0.897446 (q0.8) | 5/0.939 | 3/0.454 | -4.7 | worse |
| +mask wick_skew_pct>= | - | 0.066302 (q0.8) | 3/1.796 | 4/0.0 | -4.7 | worse |
| +mask vol_ratio>= | - | 4.841589 (q0.8) | 4/0.0 | 3/0.504 | -4.7 | worse |
| +mask atr_pct>= | - | 0.004157 (q0.8) | 6/0.623 | 3/0.454 | -4.7 | worse |
| +mask body_pct>= | - | 0.888895 (q0.8) | 2/0.0 | 5/4.366 | -4.8 | worse |
| +mask close_loc<= | - | 0.0 (q0.2) | 2/0.897 | 6/1.467 | -4.8 | worse |
| +mask upper_wick_pct>= | - | 0.130377 (q0.8) | 2/inf | 4/0.353 | -4.8 | worse |
| +mask lower_wick_pct<= | - | 0.0 (q0.2) | 2/0.897 | 6/1.467 | -4.8 | worse |
| +mask lower_wick_pct>= | - | 0.163242 (q0.8) | 2/0.047 | 5/0.503 | -4.8 | worse |
| +mask rs_pct<= | - | -2.445118 (q0.2) | 0/0.0 | 2/0.904 | -5.0 | worse |
| +mask quality_score>= | - | 105.834628 (q0.8) | 0/0.0 | 0/0.0 | -5.0 | worse |
| +mask vwap_dist_atr<= | - | -4.480017 (q0.5) | 0/0.0 | 2/2.683 | -5.0 | worse |
| +mask vwap_dist_atr<= | - | -6.454222 (q0.2) | 0/0.0 | 0/0.0 | -5.0 | worse |

## pre-momentum

| knob | old | new | FIT n/PF | VAL n/PF | score | vs baseline |
|---|---|---|---|---|---|---|
| +premom pre3_range_r<= | - | 0.667405 (q0.8) | 21/0.729 | 24/0.766 | 0.6994 | improve |
| +premom pre3_range_r<= | - | 0.364407 (q0.5) | 19/0.7 | 22/0.707 | 0.6944 | improve |
| +premom pre1_adx<= | - | 41.453928 (q0.5) | 19/0.992 | 19/0.799 | 0.6446 | improve |
| +premom sig5_adx_calc>= | - | 15.694835 (q0.2) | 18/0.925 | 12/0.694 | 0.5092 | improve |
| +premom sig5_adx_calc<= | - | 21.887561 (q0.5) | 16/0.812 | 21/0.635 | 0.4934 | improve |
| premom pre3_close_pos<= | 0.581797 | 0.772728 (q0.5) | 35/0.501 | 37/0.522 | 0.4842 | improve |
| +premom pre1_adx<= | - | 54.183743 (q0.8) | 21/0.865 | 25/0.631 | 0.4438 | improve |
| +premom sig5_adx_calc<= | - | 37.125226 (q0.8) | 22/0.848 | 26/0.621 | 0.4394 | flat |
| +premom sig5_rsi_dir<= | - | 71.177146 (q0.5) | 22/0.848 | 26/0.621 | 0.4394 | flat |
| +premom pre3_close_pos<= | - | 0.772728 (q0.5) | 22/0.848 | 26/0.621 | 0.4394 | flat |
| +premom pre5_mom_r<= | - | 0.364744 (q0.5) | 22/0.848 | 26/0.621 | 0.4394 | flat |
| premom pre5_mom_r<= | 0.284145 | 0.263298 (q0.35) | 22/0.848 | 26/0.621 | 0.4394 | flat |
| +premom pre_entry_momentum_score<= | - | 75.366309 (q0.8) | 22/0.848 | 26/0.621 | 0.4394 | flat |
| +premom pre_entry_momentum_score<= | - | 66.585088 (q0.5) | 22/0.848 | 26/0.621 | 0.4394 | flat |
| +premom pre5_mom_r<= | - | 0.695 (q0.8) | 22/0.848 | 26/0.621 | 0.4394 | flat |
| +premom sig5_rsi_dir<= | - | 79.110554 (q0.8) | 22/0.848 | 26/0.621 | 0.4394 | flat |
| +premom pre3_close_pos<= | - | 1.0 (q0.8) | 22/0.848 | 26/0.621 | 0.4394 | flat |
| +premom pre3_range_r<= | - | 0.199134 (q0.2) | 16/0.925 | 13/0.633 | 0.3994 | worse |
| premom pre3_close_pos<= | 0.581797 | 0.652177 (q0.35) | 28/0.741 | 30/0.551 | 0.399 | worse |
| premom pre5_mom_r<= | 0.284145 | 0.695 (q0.8) | 29/0.715 | 38/0.533 | 0.3874 | worse |
| +premom pre_entry_momentum_score<= | - | 55.874998 (q0.2) | 18/0.922 | 24/0.618 | 0.3748 | worse |
| premom sig5_rsi_dir<= | 64.104659 | 67.950924 (q0.35) | 38/0.473 | 44/0.597 | 0.3738 | worse |
| drop premom sig5_rsi_dir<=64.104659 | sig5_rsi_dir<=64.104659 | dropped | 68/0.491 | 126/0.639 | 0.3726 | worse |
| premom sig5_rsi_dir<= | 64.104659 | 76.066894 (q0.65) | 58/0.475 | 76/0.632 | 0.3494 | worse |
| premom pre3_close_pos<= | 0.581797 | 0.888877 (q0.65) | 45/0.373 | 45/0.422 | 0.3338 | worse |
| +premom sig5_vol_ratio20>= | - | 1.989253 (q0.2) | 16/0.844 | 17/0.553 | 0.3202 | worse |
| +premom sig5_rsi_dir<= | - | 63.60046 (q0.2) | 22/0.848 | 25/0.536 | 0.2864 | worse |
| premom sig5_rsi_dir<= | 64.104659 | 63.60046 (q0.2) | 22/0.848 | 25/0.536 | 0.2864 | worse |
| premom sig5_rsi_dir<= | 64.104659 | 79.110554 (q0.8) | 61/0.442 | 96/0.659 | 0.2684 | worse |
| premom pre5_mom_r<= | 0.284145 | 0.364744 (q0.5) | 23/0.775 | 29/0.492 | 0.2656 | worse |
| premom sig5_rsi_dir<= | 64.104659 | 71.177146 (q0.5) | 50/0.415 | 61/0.612 | 0.2574 | worse |
| drop premom pre5_mom_r<=0.284145 | pre5_mom_r<=0.284145 | dropped | 37/0.75 | 43/0.475 | 0.255 | worse |
| premom pre5_mom_r<= | 0.284145 | 0.50175 (q0.65) | 28/0.784 | 32/0.483 | 0.2422 | worse |
| drop premom pre3_close_pos<=0.581797 | pre3_close_pos<=0.581797 | dropped | 79/0.315 | 57/0.409 | 0.2398 | worse |
| premom pre3_close_pos<= | 0.581797 | 1.0 (q0.8) | 79/0.315 | 57/0.409 | 0.2398 | worse |
| premom pre5_mom_r<= | 0.284145 | 0.144913 (q0.2) | 14/1.123 | 23/0.626 | 0.2284 | worse |
| +premom pre5_mom_r<= | - | 0.144913 (q0.2) | 14/1.123 | 23/0.626 | 0.2284 | worse |
| premom pre3_close_pos<= | 0.581797 | 0.5 (q0.2) | 18/0.876 | 22/0.472 | 0.1488 | worse |
| +premom pre3_close_pos<= | - | 0.5 (q0.2) | 18/0.876 | 22/0.472 | 0.1488 | worse |
| +premom sig5_vol_ratio20<= | - | 3.010349 (q0.5) | 13/1.264 | 21/0.629 | 0.121 | worse |
| +premom sig5_vol_ratio20<= | - | 4.849113 (q0.8) | 18/1.779 | 23/0.636 | -0.2784 | worse |
| +premom pre1_adx>= | - | 30.634255 (q0.2) | 9/0.012 | 17/0.511 | -4.1 | worse |
| +premom pre1_adx<= | - | 30.634255 (q0.2) | 13/3.432 | 9/0.899 | -4.1 | worse |
| +premom sig5_vol_ratio20<= | - | 1.989253 (q0.2) | 6/0.86 | 9/0.723 | -4.4 | worse |
| +premom pre3_range_r>= | - | 0.199134 (q0.2) | 6/0.701 | 13/0.613 | -4.4 | worse |
| +premom sig5_vol_ratio20>= | - | 3.010349 (q0.5) | 9/0.583 | 5/0.569 | -4.5 | worse |
| +premom sig5_adx_calc>= | - | 21.887561 (q0.5) | 6/0.95 | 5/0.579 | -4.5 | worse |
| +premom pre3_close_pos>= | - | 0.5 (q0.2) | 7/1.444 | 4/12.175 | -4.6 | worse |
| +premom sig5_adx_calc<= | - | 15.694835 (q0.2) | 4/0.566 | 14/0.55 | -4.6 | worse |
| +premom pre1_adx>= | - | 41.453928 (q0.5) | 3/0.04 | 7/0.259 | -4.7 | worse |
| +premom pre5_mom_r>= | - | 0.144913 (q0.2) | 8/0.603 | 3/0.479 | -4.7 | worse |
| +premom sig5_vol_ratio20>= | - | 4.849113 (q0.8) | 4/0.0 | 3/0.504 | -4.7 | worse |
| +premom pre3_range_r>= | - | 0.364407 (q0.5) | 3/1.858 | 4/0.353 | -4.7 | worse |
| +premom pre_entry_momentum_score>= | - | 55.874998 (q0.2) | 4/0.577 | 2/0.786 | -4.8 | worse |
| +premom pre1_adx>= | - | 54.183743 (q0.8) | 1/0.0 | 1/0.0 | -4.9 | worse |
| +premom pre3_range_r>= | - | 0.667405 (q0.8) | 1/inf | 2/0.0 | -4.9 | worse |
| +premom pre3_close_pos>= | - | 0.772728 (q0.5) | 0/0.0 | 0/0.0 | -5.0 | worse |
| +premom pre3_close_pos>= | - | 1.0 (q0.8) | 0/0.0 | 0/0.0 | -5.0 | worse |
| +premom pre_entry_momentum_score>= | - | 75.366309 (q0.8) | 0/0.0 | 0/0.0 | -5.0 | worse |
| +premom pre_entry_momentum_score>= | - | 66.585088 (q0.5) | 0/0.0 | 0/0.0 | -5.0 | worse |
| +premom pre5_mom_r>= | - | 0.364744 (q0.5) | 0/0.0 | 0/0.0 | -5.0 | worse |
| +premom pre5_mom_r>= | - | 0.695 (q0.8) | 0/0.0 | 0/0.0 | -5.0 | worse |
| +premom sig5_rsi_dir>= | - | 71.177146 (q0.5) | 0/0.0 | 0/0.0 | -5.0 | worse |
| +premom sig5_rsi_dir>= | - | 63.60046 (q0.2) | 0/0.0 | 1/inf | -5.0 | worse |
| +premom sig5_adx_calc>= | - | 37.125226 (q0.8) | 0/0.0 | 0/0.0 | -5.0 | worse |
| +premom sig5_rsi_dir>= | - | 79.110554 (q0.8) | 0/0.0 | 0/0.0 | -5.0 | worse |

## regime

| knob | old | new | FIT n/PF | VAL n/PF | score | vs baseline |
|---|---|---|---|---|---|---|
| +mask regime!=TREND | - | TREND | 21/0.806 | 26/0.621 | 0.473 | improve |
| +mask regime!=BULL | - | BULL | 22/0.848 | 26/0.621 | 0.4394 | flat |
| +mask regime==NEUTRAL | - | NEUTRAL | 16/0.555 | 22/0.822 | 0.3414 | worse |
| +mask regime==BEAR | - | BEAR | 5/2.483 | 4/0.0 | -4.6 | worse |

## Best stable knobs (score-improving, FIT and VAL both alive)

- **indicator/price-action / +mask rs_pct>=** -> -0.85198 (q0.5) (FIT 18/0.897, VAL 15/0.847, score 0.807)
- **exit / sl_pct** -> 1.5 (FIT 26/0.821, VAL 32/0.868, score 0.7834)
- **guard / max_slot** -> 14:00 (FIT 19/0.831, VAL 20/0.908, score 0.7694)
- **indicator/price-action / +mask signal_range_pct<=** -> 0.536963 (q0.5) (FIT 15/0.987, VAL 19/0.84, score 0.7224)
- **indicator/price-action / +mask vwap_dist_atr>=** -> -2.60068 (q0.8) (FIT 19/0.703, VAL 15/0.705, score 0.7014)
- **pre-momentum / +premom pre3_range_r<=** -> 0.667405 (q0.8) (FIT 21/0.729, VAL 24/0.766, score 0.6994)
- **pre-momentum / +premom pre3_range_r<=** -> 0.364407 (q0.5) (FIT 19/0.7, VAL 22/0.707, score 0.6944)
- **indicator/price-action / +mask close_loc<=** -> 0.264187 (q0.8) (FIT 11/0.699, VAL 19/0.716, score 0.6854)
- **pre-momentum / +premom pre1_adx<=** -> 41.453928 (q0.5) (FIT 19/0.992, VAL 19/0.799, score 0.6446)
- **indicator/price-action / +mask wick_skew_pct>=** -> -0.120104 (q0.2) (FIT 17/0.94, VAL 21/0.74, score 0.58)
- **indicator/price-action / +mask wick_skew_pct<=** -> 0.066302 (q0.8) (FIT 19/0.704, VAL 22/0.873, score 0.5688)
- **guard / daily_loss_rs** -> 2000.0 (FIT 22/0.848, VAL 25/0.686, score 0.5564)
- **guard / top_n** -> 2 (FIT 22/0.848, VAL 25/0.685, score 0.5546)
- **indicator/price-action / +mask signal_range_pct<=** -> 0.897446 (q0.8) (FIT 17/0.816, VAL 23/0.66, score 0.5352)
- **indicator/price-action / +mask upper_wick_pct<=** -> 0.130377 (q0.8) (FIT 20/0.611, VAL 22/0.707, score 0.5342)

## Overfit-risk notes

- Any knob whose FIT PF explodes while VAL PF collapses is a knife-edge; the band objective already penalises the gap, and stage-5 adds neighborhood + dropout checks.
- Sweeps that push PF far above 1.80 are treated as overshoot, not success.