# D_AVWAP_LOSE_REVERSAL Iteration Log

Each row changes one logical group where possible; combination/Optuna rows are marked separately and still use FIT/VAL objective only.

| # | Stage | Group | FIT | VAL | TRAIN | TEST | Decision |
|---|---|---|---|---|---|---|---|
| 1 | stage1_baseline | baseline | n=770 PF=0.7843 net=-81420.28 | n=595 PF=0.9853 net=-3433.45 | n=1365 PF=0.8612 net=-84853.73 | n=550 PF=0.9186 net=-17755.33 | TRAIN_OUT_OF_BAND_BASELINE |
| 2 | stage4_exit_sweep | exit | n=1258 PF=0.6023 net=-136288.56 | n=1075 PF=0.5908 net=-118326.87 | not run | not run | REJECT_FIT_VAL |
| 3 | stage4_exit_sweep | exit | n=1231 PF=0.622 net=-134355.04 | n=1044 PF=0.6274 net=-109348.83 | not run | not run | REJECT_FIT_VAL |
| 4 | stage4_exit_sweep | exit | n=1179 PF=0.6635 net=-121712.93 | n=974 PF=0.6465 net=-104150.59 | not run | not run | REJECT_FIT_VAL |
| 5 | stage4_exit_sweep | exit | n=1140 PF=0.6923 net=-111805.61 | n=942 PF=0.7113 net=-83884.42 | not run | not run | REJECT_FIT_VAL |
| 6 | stage4_exit_sweep | exit | n=1093 PF=0.7538 net=-86838.3 | n=917 PF=0.7325 net=-77514.15 | not run | not run | REJECT_FIT_VAL |
| 7 | stage4_exit_sweep | exit | n=1077 PF=0.7772 net=-77767.02 | n=903 PF=0.7346 net=-76268.09 | not run | not run | REJECT_FIT_VAL |
| 8 | stage4_exit_sweep | exit | n=1054 PF=0.7997 net=-69213.03 | n=873 PF=0.7794 net=-61616.67 | not run | not run | REJECT_FIT_VAL |
| 9 | stage4_exit_sweep | exit | n=1012 PF=0.7865 net=-71939.06 | n=847 PF=0.7888 net=-58167.29 | not run | not run | REJECT_FIT_VAL |
| 10 | stage4_exit_sweep | exit | n=990 PF=0.7766 net=-74223.37 | n=832 PF=0.818 net=-49252.39 | not run | not run | REJECT_FIT_VAL |
| 11 | stage4_exit_sweep | exit | n=972 PF=0.7797 net=-71949.54 | n=826 PF=0.8305 net=-45502.41 | not run | not run | REJECT_FIT_VAL |
| 12 | stage4_exit_sweep | exit | n=1216 PF=0.6403 net=-127651.46 | n=1027 PF=0.6042 net=-118648.7 | not run | not run | REJECT_FIT_VAL |
| 13 | stage4_exit_sweep | exit | n=1187 PF=0.646 net=-131994.42 | n=983 PF=0.6224 net=-114403.0 | not run | not run | REJECT_FIT_VAL |
| 14 | stage4_exit_sweep | exit | n=1110 PF=0.6946 net=-113569.05 | n=920 PF=0.6509 net=-107498.93 | not run | not run | REJECT_FIT_VAL |
| 15 | stage4_exit_sweep | exit | n=1062 PF=0.7552 net=-90140.44 | n=885 PF=0.7288 net=-82011.98 | not run | not run | REJECT_FIT_VAL |
| 16 | stage4_exit_sweep | exit | n=1021 PF=0.8111 net=-68016.23 | n=845 PF=0.7535 net=-72536.73 | not run | not run | REJECT_FIT_VAL |
| 17 | stage4_exit_sweep | exit | n=1003 PF=0.8381 net=-57653.55 | n=830 PF=0.77 net=-66960.34 | not run | not run | REJECT_FIT_VAL |
| 18 | stage4_exit_sweep | exit | n=981 PF=0.84 net=-56934.42 | n=800 PF=0.8121 net=-52998.09 | not run | not run | REJECT_FIT_VAL |
| 19 | stage4_exit_sweep | exit | n=946 PF=0.8197 net=-63053.75 | n=772 PF=0.8197 net=-49927.44 | not run | not run | REJECT_FIT_VAL |
| 20 | stage3_time_rank_redesign | entry_guard | n=770 PF=0.7843 net=-81420.28 | n=595 PF=0.9853 net=-3433.45 | not run | not run | REJECT_FIT_VAL |
| 21 | stage3_time_rank_redesign | entry_guard | n=770 PF=0.7843 net=-81420.28 | n=595 PF=0.9853 net=-3433.45 | not run | not run | REJECT_FIT_VAL |
| 22 | stage3_time_rank_redesign | entry_guard | n=770 PF=0.7843 net=-81420.28 | n=595 PF=0.9853 net=-3433.45 | not run | not run | REJECT_FIT_VAL |
| 23 | stage3_time_rank_redesign | entry_guard | n=769 PF=0.7864 net=-80413.12 | n=592 PF=0.9819 net=-4202.79 | not run | not run | REJECT_FIT_VAL |
| 24 | stage3_time_rank_redesign | entry_guard | n=212 PF=0.746 net=-29009.26 | n=162 PF=1.5144 net=29203.99 | not run | not run | REJECT_FIT_VAL |
| 25 | stage3_time_rank_redesign | entry_guard | n=468 PF=0.8055 net=-48059.76 | n=423 PF=1.0045 net=783.25 | not run | not run | REJECT_FIT_VAL |
| 26 | stage3_time_rank_redesign | entry_guard | n=619 PF=0.792 net=-66191.16 | n=497 PF=0.9399 net=-12516.4 | not run | not run | REJECT_FIT_VAL |
| 27 | stage3_time_rank_redesign | entry_guard | n=691 PF=0.8012 net=-68978.14 | n=526 PF=0.946 net=-11832.47 | not run | not run | REJECT_FIT_VAL |
| 28 | stage3_time_rank_redesign | entry_guard | n=619 PF=0.792 net=-66191.16 | n=497 PF=0.9399 net=-12516.4 | not run | not run | REJECT_FIT_VAL |
| 29 | stage3_time_rank_redesign | entry_guard | n=543 PF=0.8327 net=-41018.42 | n=497 PF=0.9154 net=-16726.44 | not run | not run | REJECT_FIT_VAL |
| 30 | stage3_time_rank_redesign | entry_guard | n=713 PF=0.7723 net=-77402.74 | n=570 PF=0.9041 net=-22881.07 | not run | not run | REJECT_FIT_VAL |
| 31 | stage3_time_rank_redesign | entry_guard | n=763 PF=0.7811 net=-81191.98 | n=585 PF=0.9434 net=-13465.43 | not run | not run | REJECT_FIT_VAL |
| 32 | stage4_signal_filter_sweep | signal_filter | n=603 PF=0.662 net=-96044.11 | n=492 PF=0.8852 net=-20997.32 | not run | not run | REJECT_FIT_VAL |
| 33 | stage4_signal_filter_sweep | signal_filter | n=659 PF=0.7539 net=-78287.64 | n=524 PF=1.0992 net=19156.68 | not run | not run | REJECT_FIT_VAL |
| 34 | stage4_signal_filter_sweep | signal_filter | n=410 PF=0.6164 net=-77127.32 | n=435 PF=0.9063 net=-15430.6 | not run | not run | REJECT_FIT_VAL |
| 35 | stage4_signal_filter_sweep | signal_filter | n=254 PF=1.1105 net=10397.88 | n=277 PF=0.9143 net=-9044.36 | not run | not run | REJECT_FIT_VAL |
| 36 | stage4_signal_filter_sweep | signal_filter | n=594 PF=0.7576 net=-76724.07 | n=489 PF=0.9778 net=-4519.51 | not run | not run | REJECT_FIT_VAL |
| 37 | stage4_signal_filter_sweep | signal_filter | n=582 PF=0.8239 net=-48911.42 | n=483 PF=1.1483 net=25734.15 | not run | not run | REJECT_FIT_VAL |
| 38 | stage4_signal_filter_sweep | signal_filter | n=476 PF=0.8382 net=-37218.42 | n=364 PF=1.2677 net=33026.45 | not run | not run | REJECT_FIT_VAL |
| 39 | stage4_signal_filter_sweep | signal_filter | n=719 PF=0.777 net=-77928.34 | n=583 PF=0.9648 net=-8397.05 | not run | not run | REJECT_FIT_VAL |
| 40 | stage4_signal_filter_sweep | signal_filter | n=387 PF=0.5572 net=-80745.4 | n=268 PF=0.9733 net=-2709.99 | not run | not run | REJECT_FIT_VAL |
| 41 | stage4_signal_filter_sweep | signal_filter | n=556 PF=0.77 net=-61274.19 | n=463 PF=1.0222 net=4214.5 | not run | not run | REJECT_FIT_VAL |
| 42 | stage4_signal_filter_sweep | signal_filter | n=741 PF=0.7782 net=-79535.34 | n=572 PF=1.0006 net=121.5 | not run | not run | REJECT_FIT_VAL |
| 43 | stage4_signal_filter_sweep | signal_filter | n=753 PF=0.7864 net=-78902.87 | n=583 PF=1.0021 net=474.8 | not run | not run | REJECT_FIT_VAL |
| 44 | stage4_signal_filter_sweep | signal_filter | n=563 PF=0.8478 net=-41289.72 | n=299 PF=0.8021 net=-26931.82 | not run | not run | REJECT_FIT_VAL |
| 45 | stage4_signal_filter_sweep | signal_filter | n=658 PF=0.752 net=-80761.84 | n=543 PF=0.9983 net=-358.35 | not run | not run | REJECT_FIT_VAL |
| 46 | stage3_premomentum_redesign | pre_momentum | n=462 PF=0.8134 net=-38299.8 | n=389 PF=1.0531 net=6879.44 | not run | not run | REJECT_FIT_VAL |
| 47 | stage3_premomentum_redesign | pre_momentum | n=476 PF=0.7872 net=-48976.55 | n=356 PF=1.0902 net=12309.06 | not run | not run | REJECT_FIT_VAL |
| 48 | stage3_premomentum_redesign | pre_momentum | n=733 PF=0.7388 net=-93812.36 | n=564 PF=1.0513 net=10964.92 | not run | not run | REJECT_FIT_VAL |
| 49 | stage3_premomentum_redesign | pre_momentum | n=448 PF=0.7997 net=-39193.15 | n=364 PF=0.9851 net=-1814.19 | not run | not run | REJECT_FIT_VAL |
| 50 | stage3_premomentum_redesign | pre_momentum | n=698 PF=0.7824 net=-72776.94 | n=567 PF=0.9368 net=-13914.16 | not run | not run | REJECT_FIT_VAL |
| 51 | stage3_premomentum_redesign | pre_momentum | n=687 PF=0.7759 net=-72397.37 | n=561 PF=0.8931 net=-23590.78 | not run | not run | REJECT_FIT_VAL |
| 52 | stage3_premomentum_redesign | pre_momentum | n=178 PF=0.8757 net=-11641.19 | n=104 PF=1.1148 net=5468.42 | not run | not run | REJECT_FIT_VAL |
| 53 | stage3_premomentum_redesign | pre_momentum | n=109 PF=0.605 net=-15477.18 | n=168 PF=0.7176 net=-16874.97 | not run | not run | REJECT_FIT_VAL |
| 54 | stage3_premomentum_redesign | pre_momentum | n=136 PF=0.8227 net=-13700.24 | n=126 PF=1.09 net=5191.19 | not run | not run | REJECT_FIT_VAL |
| 55 | stage3_premomentum_redesign | pre_momentum | n=747 PF=0.7894 net=-75807.23 | n=579 PF=1.009 net=1993.18 | not run | not run | REJECT_FIT_VAL |
| 56 | stage3_premomentum_redesign | pre_momentum | n=188 PF=0.9463 net=-3604.57 | n=281 PF=0.8793 net=-11484.11 | not run | not run | REJECT_FIT_VAL |
| 57 | stage3_premomentum_redesign | pre_momentum | n=298 PF=0.6847 net=-46567.07 | n=241 PF=0.9771 net=-2070.83 | not run | not run | REJECT_FIT_VAL |
| 58 | stage3_premomentum_redesign | pre_momentum | n=619 PF=0.8508 net=-41621.16 | n=536 PF=1.051 net=10509.16 | not run | not run | REJECT_FIT_VAL |
| 59 | stage3_premomentum_redesign | pre_momentum | n=139 PF=0.9147 net=-6630.71 | n=129 PF=1.2733 net=14935.62 | not run | not run | REJECT_FIT_VAL |
| 60 | stage3_premomentum_redesign | pre_momentum | n=728 PF=0.7665 net=-81084.54 | n=574 PF=0.9742 net=-5719.59 | not run | not run | REJECT_FIT_VAL |
| 61 | stage5_optuna_combo | combination | n=64 PF=1.2227 net=4553.96 | n=43 PF=0.8444 net=-2411.22 | not run | not run | REJECT_FIT_VAL |
| 62 | stage5_optuna_combo | combination | n=154 PF=0.8367 net=-11399.79 | n=77 PF=0.8682 net=-4279.32 | not run | not run | REJECT_FIT_VAL |
| 63 | stage5_optuna_combo | combination | n=232 PF=0.5423 net=-43794.7 | n=202 PF=0.6453 net=-25281.69 | not run | not run | REJECT_FIT_VAL |
| 64 | stage5_optuna_combo | combination | n=456 PF=0.7128 net=-61569.69 | n=350 PF=0.8738 net=-18091.99 | not run | not run | REJECT_FIT_VAL |
| 65 | stage5_seeded_random_combo | combination | n=75 PF=0.803 net=-4192.28 | n=142 PF=0.6281 net=-16281.35 | not run | not run | REJECT_FIT_VAL |
| 66 | stage5_seeded_random_combo | combination | n=79 PF=0.8916 net=-2787.04 | n=66 PF=0.8148 net=-4001.22 | not run | not run | REJECT_FIT_VAL |
| 67 | stage5_seeded_random_combo | combination | n=167 PF=0.7165 net=-24320.57 | n=112 PF=0.7865 net=-10707.36 | not run | not run | REJECT_FIT_VAL |
| 68 | stage5_seeded_random_combo | combination | n=54 PF=0.4685 net=-14431.01 | n=82 PF=0.6947 net=-9335.49 | not run | not run | REJECT_FIT_VAL |
| 69 | stage5_seeded_random_combo | combination | n=411 PF=0.768 net=-53189.94 | n=360 PF=1.0754 net=11534.91 | not run | not run | REJECT_FIT_VAL |
| 70 | stage5_seeded_random_combo | combination | n=197 PF=0.7973 net=-13345.42 | n=176 PF=0.9832 net=-934.95 | not run | not run | REJECT_FIT_VAL |
| 71 | stage5_seeded_random_combo | combination | n=5 PF=0.2522 net=-1675.9 | n=17 PF=0.5427 net=-3863.38 | not run | not run | REJECT_FIT_VAL |
| 72 | stage5_seeded_random_combo | combination | n=635 PF=0.6761 net=-95878.34 | n=589 PF=0.8118 net=-43474.92 | not run | not run | REJECT_FIT_VAL |
| 73 | stage5_seeded_random_combo | combination | n=39 PF=0.946 net=-1068.92 | n=35 PF=1.8499 net=7988.23 | not run | not run | REJECT_FIT_VAL |
| 74 | stage5_seeded_random_combo | combination | n=55 PF=0.6867 net=-6093.03 | n=80 PF=0.6599 net=-9034.68 | not run | not run | REJECT_FIT_VAL |
| 75 | stage5_seeded_random_combo | combination | n=386 PF=0.806 net=-43434.74 | n=364 PF=1.0447 net=7327.24 | not run | not run | REJECT_FIT_VAL |

Full CSV: `C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\Train_and_Test\setup_recovery_full_loop_0bps_trainpf_ge_1p3\D_AVWAP_LOSE_REVERSAL\iteration_log.csv`
