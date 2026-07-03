# D_EMA20_REJECTION Iteration Log

Each row changes one logical group where possible; combination/Optuna rows are marked separately and still use FIT/VAL objective only.

| # | Stage | Group | FIT | VAL | TRAIN | TEST | Decision |
|---|---|---|---|---|---|---|---|
| 1 | stage1_baseline | baseline | n=76 PF=0.997 net=-76.34 | n=42 PF=0.6818 net=-5262.06 | n=118 PF=0.8733 net=-5338.4 | n=20 PF=0.585 net=-2430.37 | TRAIN_OUT_OF_BAND_BASELINE |
| 2 | stage4_exit_sweep | exit | n=62 PF=0.7097 net=-4343.81 | n=42 PF=0.6393 net=-3586.85 | not run | not run | REJECT_FIT_VAL |
| 3 | stage4_exit_sweep | exit | n=62 PF=0.6645 net=-5572.93 | n=42 PF=0.6689 net=-3452.22 | not run | not run | REJECT_FIT_VAL |
| 4 | stage4_exit_sweep | exit | n=62 PF=0.8326 net=-2861.03 | n=42 PF=0.7299 net=-2947.18 | not run | not run | REJECT_FIT_VAL |
| 5 | stage4_exit_sweep | exit | n=62 PF=0.8378 net=-2849.17 | n=42 PF=0.6899 net=-3532.97 | not run | not run | REJECT_FIT_VAL |
| 6 | stage4_exit_sweep | exit | n=62 PF=0.878 net=-2158.61 | n=42 PF=0.6425 net=-4244.9 | not run | not run | REJECT_FIT_VAL |
| 7 | stage4_exit_sweep | exit | n=62 PF=0.9397 net=-1067.5 | n=42 PF=0.6759 net=-3848.17 | not run | not run | REJECT_FIT_VAL |
| 8 | stage4_exit_sweep | exit | n=62 PF=0.9406 net=-1079.62 | n=42 PF=0.7101 net=-3443.0 | not run | not run | REJECT_FIT_VAL |
| 9 | stage4_exit_sweep | exit | n=62 PF=0.9618 net=-712.35 | n=42 PF=0.7611 net=-2837.01 | not run | not run | REJECT_FIT_VAL |
| 10 | stage4_exit_sweep | exit | n=62 PF=0.9533 net=-871.84 | n=42 PF=0.7223 net=-3297.47 | not run | not run | REJECT_FIT_VAL |
| 11 | stage4_exit_sweep | exit | n=62 PF=0.9801 net=-371.81 | n=42 PF=0.7223 net=-3297.47 | not run | not run | REJECT_FIT_VAL |
| 12 | stage4_exit_sweep | exit | n=67 PF=0.7184 net=-4906.81 | n=42 PF=0.6127 net=-4284.61 | not run | not run | REJECT_FIT_VAL |
| 13 | stage4_exit_sweep | exit | n=67 PF=0.6322 net=-7339.14 | n=42 PF=0.6437 net=-4149.52 | not run | not run | REJECT_FIT_VAL |
| 14 | stage4_exit_sweep | exit | n=67 PF=0.7747 net=-4625.98 | n=42 PF=0.7102 net=-3543.56 | not run | not run | REJECT_FIT_VAL |
| 15 | stage4_exit_sweep | exit | n=67 PF=0.7329 net=-5713.57 | n=42 PF=0.6214 net=-4850.35 | not run | not run | REJECT_FIT_VAL |
| 16 | stage4_exit_sweep | exit | n=67 PF=0.7666 net=-5023.86 | n=42 PF=0.5773 net=-5661.08 | not run | not run | REJECT_FIT_VAL |
| 17 | stage4_exit_sweep | exit | n=67 PF=0.8172 net=-3933.17 | n=42 PF=0.5723 net=-5727.74 | not run | not run | REJECT_FIT_VAL |
| 18 | stage4_exit_sweep | exit | n=67 PF=0.7982 net=-4460.54 | n=42 PF=0.5877 net=-5521.1 | not run | not run | REJECT_FIT_VAL |
| 19 | stage4_exit_sweep | exit | n=67 PF=0.7931 net=-4693.02 | n=42 PF=0.6619 net=-4527.27 | not run | not run | REJECT_FIT_VAL |
| 20 | stage3_time_rank_redesign | entry_guard | n=76 PF=0.997 net=-76.34 | n=42 PF=0.6818 net=-5262.06 | not run | not run | REJECT_FIT_VAL |
| 21 | stage3_time_rank_redesign | entry_guard | n=76 PF=0.997 net=-76.34 | n=42 PF=0.6818 net=-5262.06 | not run | not run | REJECT_FIT_VAL |
| 22 | stage3_time_rank_redesign | entry_guard | n=76 PF=0.997 net=-76.34 | n=42 PF=0.6818 net=-5262.06 | not run | not run | REJECT_FIT_VAL |
| 23 | stage3_time_rank_redesign | entry_guard | n=76 PF=0.997 net=-76.34 | n=42 PF=0.6818 net=-5262.06 | not run | not run | REJECT_FIT_VAL |
| 24 | stage3_time_rank_redesign | entry_guard | n=0 PF=0.0 net=0.0 | n=2 PF=1.0646 net=53.51 | not run | not run | REJECT_FIT_VAL |
| 25 | stage3_time_rank_redesign | entry_guard | n=22 PF=1.6444 net=4122.27 | n=14 PF=0.731 net=-1787.89 | not run | not run | REJECT_FIT_VAL |
| 26 | stage3_time_rank_redesign | entry_guard | n=47 PF=1.1896 net=3191.11 | n=26 PF=0.7539 net=-2871.69 | not run | not run | REJECT_FIT_VAL |
| 27 | stage3_time_rank_redesign | entry_guard | n=62 PF=0.9515 net=-1175.78 | n=33 PF=0.7395 net=-3482.69 | not run | not run | REJECT_FIT_VAL |
| 28 | stage3_time_rank_redesign | entry_guard | n=47 PF=1.1896 net=3191.11 | n=26 PF=0.7539 net=-2871.69 | not run | not run | REJECT_FIT_VAL |
| 29 | stage3_time_rank_redesign | entry_guard | n=41 PF=1.0283 net=384.05 | n=27 PF=0.602 net=-4438.05 | not run | not run | REJECT_FIT_VAL |
| 30 | stage3_time_rank_redesign | entry_guard | n=60 PF=1.0822 net=1632.64 | n=35 PF=0.7387 net=-3529.62 | not run | not run | REJECT_FIT_VAL |
| 31 | stage3_time_rank_redesign | entry_guard | n=66 PF=1.0513 net=1110.04 | n=40 PF=0.7244 net=-4290.02 | not run | not run | REJECT_FIT_VAL |
| 32 | stage4_signal_filter_sweep | signal_filter | n=41 PF=0.9185 net=-1227.65 | n=21 PF=0.6138 net=-3341.76 | not run | not run | REJECT_FIT_VAL |
| 33 | stage4_signal_filter_sweep | signal_filter | n=76 PF=0.997 net=-76.34 | n=42 PF=0.6818 net=-5262.06 | not run | not run | REJECT_FIT_VAL |
| 34 | stage4_signal_filter_sweep | signal_filter | n=73 PF=0.9009 net=-2535.18 | n=39 PF=0.6262 net=-5872.03 | not run | not run | REJECT_FIT_VAL |
| 35 | stage4_signal_filter_sweep | signal_filter | n=9 PF=1.4834 net=1609.29 | n=5 PF=0.0369 net=-3206.71 | not run | not run | REJECT_FIT_VAL |
| 36 | stage4_signal_filter_sweep | signal_filter | n=68 PF=1.0774 net=1659.13 | n=37 PF=0.6427 net=-5785.82 | not run | not run | REJECT_FIT_VAL |
| 37 | stage4_signal_filter_sweep | signal_filter | n=53 PF=0.985 net=-253.3 | n=42 PF=0.6818 net=-5262.06 | not run | not run | REJECT_FIT_VAL |
| 38 | stage4_signal_filter_sweep | signal_filter | n=52 PF=1.0307 net=524.55 | n=25 PF=0.8013 net=-1964.63 | not run | not run | REJECT_FIT_VAL |
| 39 | stage4_signal_filter_sweep | signal_filter | n=42 PF=0.6389 net=-6440.57 | n=31 PF=0.6641 net=-3924.81 | not run | not run | REJECT_FIT_VAL |
| 40 | stage4_signal_filter_sweep | signal_filter | n=71 PF=0.8789 net=-3072.52 | n=37 PF=0.6866 net=-4401.08 | not run | not run | REJECT_FIT_VAL |
| 41 | stage4_signal_filter_sweep | signal_filter | n=63 PF=0.8118 net=-4258.12 | n=29 PF=0.775 net=-2431.17 | not run | not run | REJECT_FIT_VAL |
| 42 | stage4_signal_filter_sweep | signal_filter | n=73 PF=0.9922 net=-198.32 | n=36 PF=0.5455 net=-7394.49 | not run | not run | REJECT_FIT_VAL |
| 43 | stage4_signal_filter_sweep | signal_filter | n=41 PF=0.9214 net=-1153.37 | n=23 PF=0.8075 net=-1549.29 | not run | not run | REJECT_FIT_VAL |
| 44 | stage4_signal_filter_sweep | signal_filter | n=25 PF=0.917 net=-733.98 | n=16 PF=0.3697 net=-4305.94 | not run | not run | REJECT_FIT_VAL |
| 45 | stage4_signal_filter_sweep | signal_filter | n=35 PF=1.1094 net=1151.31 | n=21 PF=0.7564 net=-1920.3 | not run | not run | REJECT_FIT_VAL |
| 46 | stage3_premomentum_redesign | pre_momentum | n=249 PF=0.7244 net=-28941.07 | n=160 PF=0.7852 net=-13348.91 | not run | not run | REJECT_FIT_VAL |
| 47 | stage3_premomentum_redesign | pre_momentum | n=279 PF=1.0442 net=4270.13 | n=251 PF=0.9051 net=-8084.45 | not run | not run | REJECT_FIT_VAL |
| 48 | stage3_premomentum_redesign | pre_momentum | n=308 PF=0.883 net=-13574.9 | n=219 PF=0.8583 net=-10456.4 | not run | not run | REJECT_FIT_VAL |
| 49 | stage3_premomentum_redesign | pre_momentum | n=98 PF=0.651 net=-12818.28 | n=189 PF=1.0405 net=2465.05 | not run | not run | REJECT_FIT_VAL |
| 50 | stage3_premomentum_redesign | pre_momentum | n=359 PF=0.9526 net=-5866.01 | n=343 PF=0.8678 net=-15507.23 | not run | not run | REJECT_FIT_VAL |
| 51 | stage3_premomentum_redesign | pre_momentum | n=126 PF=1.1284 net=5684.78 | n=96 PF=1.2647 net=6976.46 | n=222 PF=1.1792 net=12661.24 | not run | TRAIN_OUT_OF_BAND |
| 52 | stage3_premomentum_redesign | pre_momentum | n=265 PF=0.761 net=-23884.71 | n=264 PF=0.7517 net=-23915.15 | not run | not run | REJECT_FIT_VAL |
| 53 | stage3_premomentum_redesign | pre_momentum | n=338 PF=0.8536 net=-17921.1 | n=303 PF=0.8634 net=-14116.89 | not run | not run | REJECT_FIT_VAL |
| 54 | stage3_premomentum_redesign | pre_momentum | n=162 PF=1.0201 net=1159.8 | n=132 PF=1.0582 net=2550.4 | n=294 PF=1.0365 net=3710.2 | not run | TRAIN_OUT_OF_BAND |
| 55 | stage3_premomentum_redesign | pre_momentum | n=48 PF=0.868 net=-1955.78 | n=119 PF=1.0389 net=1528.01 | not run | not run | REJECT_FIT_VAL |
| 56 | stage3_premomentum_redesign | pre_momentum | n=300 PF=0.7379 net=-31163.11 | n=228 PF=0.8575 net=-10700.83 | not run | not run | REJECT_FIT_VAL |
| 57 | stage3_premomentum_redesign | pre_momentum | n=139 PF=0.879 net=-6485.65 | n=107 PF=0.8059 net=-7490.39 | not run | not run | REJECT_FIT_VAL |
| 58 | stage3_premomentum_redesign | pre_momentum | n=115 PF=0.5856 net=-22877.53 | n=54 PF=0.8022 net=-4052.24 | not run | not run | REJECT_FIT_VAL |
| 59 | stage3_premomentum_redesign | pre_momentum | n=115 PF=0.5539 net=-25824.28 | n=54 PF=0.7226 net=-5834.16 | not run | not run | REJECT_FIT_VAL |
| 60 | stage3_premomentum_redesign | pre_momentum | n=284 PF=0.9103 net=-9264.73 | n=250 PF=0.946 net=-4355.37 | not run | not run | REJECT_FIT_VAL |
| 61 | stage5_optuna_combo | combination | n=26 PF=1.5099 net=4311.56 | n=25 PF=0.9219 net=-518.73 | not run | not run | REJECT_FIT_VAL |
| 62 | stage5_optuna_combo | combination | n=42 PF=0.8581 net=-2023.47 | n=29 PF=0.6134 net=-4477.62 | not run | not run | REJECT_FIT_VAL |
| 63 | stage5_optuna_combo | combination | n=56 PF=0.7623 net=-4191.88 | n=57 PF=0.5614 net=-8742.68 | not run | not run | REJECT_FIT_VAL |
| 64 | stage5_optuna_combo | combination | n=197 PF=0.9178 net=-6177.16 | n=159 PF=1.0005 net=24.31 | not run | not run | REJECT_FIT_VAL |
| 65 | stage5_optuna_combo | combination | n=60 PF=1.5082 net=10505.4 | n=59 PF=1.581 net=10065.64 | n=119 PF=1.5413 net=20571.04 | n=43 PF=0.7389 net=-4705.65 | TEST_FAIL_OR_DOMINATED |
| 66 | stage5_optuna_combo | combination | n=333 PF=0.7258 net=-30753.25 | n=277 PF=0.9235 net=-6541.83 | not run | not run | REJECT_FIT_VAL |
| 67 | stage5_optuna_combo | combination | n=48 PF=1.0898 net=1629.59 | n=0 PF=0.0 net=0.0 | not run | not run | REJECT_FIT_VAL |
| 68 | stage5_optuna_combo | combination | n=80 PF=1.2427 net=3680.0 | n=120 PF=0.79 net=-5867.8 | not run | not run | REJECT_FIT_VAL |
| 69 | stage5_optuna_combo | combination | n=248 PF=0.8841 net=-11349.1 | n=211 PF=1.0016 net=124.06 | not run | not run | REJECT_FIT_VAL |
| 70 | stage5_optuna_combo | combination | n=190 PF=0.7539 net=-19810.07 | n=145 PF=0.7974 net=-11870.96 | not run | not run | REJECT_FIT_VAL |
| 71 | stage5_optuna_combo | combination | n=11 PF=1.7351 net=2929.38 | n=10 PF=6.1511 net=6037.38 | n=21 PF=2.7387 net=8966.76 | n=9 PF=1.0312 net=138.88 | TEST_FAIL_OR_DOMINATED |
| 72 | stage5_optuna_combo | combination | n=180 PF=0.6944 net=-22578.42 | n=131 PF=0.9991 net=-38.58 | not run | not run | REJECT_FIT_VAL |
| 73 | stage5_optuna_combo | combination | n=179 PF=0.6077 net=-44914.74 | n=126 PF=1.0743 net=3941.74 | not run | not run | REJECT_FIT_VAL |
| 74 | stage5_optuna_combo | combination | n=62 PF=0.6509 net=-12473.56 | n=21 PF=0.7639 net=-2522.76 | not run | not run | REJECT_FIT_VAL |
| 75 | stage5_optuna_combo | combination | n=4 PF=0.2568 net=-2074.74 | n=0 PF=0.0 net=0.0 | not run | not run | REJECT_FIT_VAL |

Full CSV: `C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\Train_and_Test\setup_recovery_full_loop_0bps_trainpf_ge_1p3\D_EMA20_REJECTION\iteration_log.csv`
