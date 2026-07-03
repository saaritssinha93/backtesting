# D_EMA20_REJECTION Iteration Log

Each row changes one logical group where possible; combination/Optuna rows are marked separately and still use FIT/VAL objective only.

| # | Stage | Group | FIT | VAL | TRAIN | TEST | Decision |
|---|---|---|---|---|---|---|---|
| 1 | stage1_baseline | baseline | n=76 PF=0.799 net=-5764.39 | n=42 PF=0.5387 net=-8395.31 | n=118 PF=0.6979 net=-14159.71 | n=20 PF=0.3719 net=-4792.93 | TRAIN_OUT_OF_BAND_BASELINE |
| 2 | stage4_exit_sweep | exit | n=62 PF=0.3497 net=-13163.34 | n=42 PF=0.3652 net=-7901.14 | not run | not run | REJECT_FIT_VAL |
| 3 | stage4_exit_sweep | exit | n=62 PF=0.3714 net=-13392.37 | n=42 PF=0.4061 net=-7706.94 | not run | not run | REJECT_FIT_VAL |
| 4 | stage4_exit_sweep | exit | n=62 PF=0.399 net=-13178.23 | n=42 PF=0.4131 net=-7963.84 | not run | not run | REJECT_FIT_VAL |
| 5 | stage4_exit_sweep | exit | n=62 PF=0.4479 net=-12395.87 | n=42 PF=0.4056 net=-8381.94 | not run | not run | REJECT_FIT_VAL |
| 6 | stage4_exit_sweep | exit | n=62 PF=0.5127 net=-10940.46 | n=42 PF=0.3859 net=-8985.71 | not run | not run | REJECT_FIT_VAL |
| 7 | stage4_exit_sweep | exit | n=62 PF=0.5524 net=-10049.17 | n=42 PF=0.413 net=-8588.99 | not run | not run | REJECT_FIT_VAL |
| 8 | stage4_exit_sweep | exit | n=62 PF=0.5364 net=-10655.36 | n=42 PF=0.4374 net=-8232.7 | not run | not run | REJECT_FIT_VAL |
| 9 | stage4_exit_sweep | exit | n=62 PF=0.5318 net=-11011.33 | n=42 PF=0.4755 net=-7675.48 | not run | not run | REJECT_FIT_VAL |
| 10 | stage4_exit_sweep | exit | n=62 PF=0.4973 net=-11822.77 | n=42 PF=0.4373 net=-8234.41 | not run | not run | REJECT_FIT_VAL |
| 11 | stage4_exit_sweep | exit | n=62 PF=0.4973 net=-11822.77 | n=42 PF=0.4373 net=-8234.41 | not run | not run | REJECT_FIT_VAL |
| 12 | stage4_exit_sweep | exit | n=67 PF=0.4259 net=-12475.98 | n=42 PF=0.3794 net=-8300.95 | not run | not run | REJECT_FIT_VAL |
| 13 | stage4_exit_sweep | exit | n=67 PF=0.4605 net=-12405.86 | n=42 PF=0.4759 net=-7010.07 | not run | not run | REJECT_FIT_VAL |
| 14 | stage4_exit_sweep | exit | n=67 PF=0.5092 net=-11641.05 | n=42 PF=0.4572 net=-7639.25 | not run | not run | REJECT_FIT_VAL |
| 15 | stage4_exit_sweep | exit | n=67 PF=0.5386 net=-11408.51 | n=42 PF=0.4589 net=-7957.09 | not run | not run | REJECT_FIT_VAL |
| 16 | stage4_exit_sweep | exit | n=67 PF=0.5565 net=-11067.17 | n=42 PF=0.3981 net=-9230.73 | not run | not run | REJECT_FIT_VAL |
| 17 | stage4_exit_sweep | exit | n=67 PF=0.5865 net=-10320.05 | n=42 PF=0.4175 net=-8933.27 | not run | not run | REJECT_FIT_VAL |
| 18 | stage4_exit_sweep | exit | n=67 PF=0.5826 net=-10680.66 | n=42 PF=0.4278 net=-8775.52 | not run | not run | REJECT_FIT_VAL |
| 19 | stage4_exit_sweep | exit | n=67 PF=0.582 net=-10961.3 | n=42 PF=0.4926 net=-7781.69 | not run | not run | REJECT_FIT_VAL |
| 20 | stage3_time_rank_redesign | entry_guard | n=76 PF=0.799 net=-5764.39 | n=42 PF=0.5387 net=-8395.31 | not run | not run | REJECT_FIT_VAL |
| 21 | stage3_time_rank_redesign | entry_guard | n=76 PF=0.799 net=-5764.39 | n=42 PF=0.5387 net=-8395.31 | not run | not run | REJECT_FIT_VAL |
| 22 | stage3_time_rank_redesign | entry_guard | n=76 PF=0.799 net=-5764.39 | n=42 PF=0.5387 net=-8395.31 | not run | not run | REJECT_FIT_VAL |
| 23 | stage3_time_rank_redesign | entry_guard | n=76 PF=0.799 net=-5764.39 | n=42 PF=0.5387 net=-8395.31 | not run | not run | REJECT_FIT_VAL |
| 24 | stage3_time_rank_redesign | entry_guard | n=0 PF=0.0 net=0.0 | n=2 PF=0.8922 net=-94.66 | not run | not run | REJECT_FIT_VAL |
| 25 | stage3_time_rank_redesign | entry_guard | n=22 PF=1.3779 net=2682.62 | n=14 PF=0.6192 net=-2682.97 | not run | not run | REJECT_FIT_VAL |
| 26 | stage3_time_rank_redesign | entry_guard | n=47 PF=0.9927 net=-133.16 | n=26 PF=0.6368 net=-4543.17 | not run | not run | REJECT_FIT_VAL |
| 27 | stage3_time_rank_redesign | entry_guard | n=62 PF=0.7906 net=-5542.45 | n=33 PF=0.6024 net=-5748.63 | not run | not run | REJECT_FIT_VAL |
| 28 | stage3_time_rank_redesign | entry_guard | n=47 PF=0.9927 net=-133.16 | n=26 PF=0.6368 net=-4543.17 | not run | not run | REJECT_FIT_VAL |
| 29 | stage3_time_rank_redesign | entry_guard | n=41 PF=0.8323 net=-2572.95 | n=27 PF=0.4763 net=-6357.96 | not run | not run | REJECT_FIT_VAL |
| 30 | stage3_time_rank_redesign | entry_guard | n=60 PF=0.8718 net=-2860.27 | n=35 PF=0.5839 net=-6165.69 | not run | not run | REJECT_FIT_VAL |
| 31 | stage3_time_rank_redesign | entry_guard | n=66 PF=0.8432 net=-3832.75 | n=40 PF=0.574 net=-7276.22 | not run | not run | REJECT_FIT_VAL |
| 32 | stage4_signal_filter_sweep | signal_filter | n=41 PF=0.7365 net=-4355.73 | n=21 PF=0.4891 net=-4828.06 | not run | not run | REJECT_FIT_VAL |
| 33 | stage4_signal_filter_sweep | signal_filter | n=76 PF=0.799 net=-5764.39 | n=42 PF=0.5387 net=-8395.31 | not run | not run | REJECT_FIT_VAL |
| 34 | stage4_signal_filter_sweep | signal_filter | n=73 PF=0.72 net=-8028.69 | n=39 PF=0.4944 net=-8757.87 | not run | not run | REJECT_FIT_VAL |
| 35 | stage4_signal_filter_sweep | signal_filter | n=9 PF=1.3131 net=1113.05 | n=5 PF=0.0068 net=-3505.27 | not run | not run | REJECT_FIT_VAL |
| 36 | stage4_signal_filter_sweep | signal_filter | n=68 PF=0.8646 net=-3287.27 | n=37 PF=0.5202 net=-8425.24 | not run | not run | REJECT_FIT_VAL |
| 37 | stage4_signal_filter_sweep | signal_filter | n=53 PF=0.7683 net=-4479.55 | n=42 PF=0.5387 net=-8395.31 | not run | not run | REJECT_FIT_VAL |
| 38 | stage4_signal_filter_sweep | signal_filter | n=52 PF=0.8148 net=-3547.12 | n=25 PF=0.6461 net=-3807.57 | not run | not run | REJECT_FIT_VAL |
| 39 | stage4_signal_filter_sweep | signal_filter | n=42 PF=0.5169 net=-9564.28 | n=31 PF=0.5211 net=-6188.34 | not run | not run | REJECT_FIT_VAL |
| 40 | stage4_signal_filter_sweep | signal_filter | n=71 PF=0.7047 net=-8360.96 | n=37 PF=0.538 net=-7186.85 | not run | not run | REJECT_FIT_VAL |
| 41 | stage4_signal_filter_sweep | signal_filter | n=63 PF=0.641 net=-9052.53 | n=29 PF=0.6219 net=-4497.42 | not run | not run | REJECT_FIT_VAL |
| 42 | stage4_signal_filter_sweep | signal_filter | n=73 PF=0.803 net=-5586.12 | n=36 PF=0.4407 net=-9951.07 | not run | not run | REJECT_FIT_VAL |
| 43 | stage4_signal_filter_sweep | signal_filter | n=41 PF=0.7376 net=-4278.36 | n=23 PF=0.6178 net=-3443.85 | not run | not run | REJECT_FIT_VAL |
| 44 | stage4_signal_filter_sweep | signal_filter | n=25 PF=0.7423 net=-2574.3 | n=16 PF=0.2564 net=-5653.93 | not run | not run | REJECT_FIT_VAL |
| 45 | stage4_signal_filter_sweep | signal_filter | n=35 PF=0.884 net=-1408.67 | n=21 PF=0.5923 net=-3567.26 | not run | not run | REJECT_FIT_VAL |
| 46 | stage3_premomentum_redesign | pre_momentum | n=249 PF=0.5628 net=-51597.02 | n=161 PF=0.5535 net=-32553.49 | not run | not run | REJECT_FIT_VAL |
| 47 | stage3_premomentum_redesign | pre_momentum | n=279 PF=0.7993 net=-22328.02 | n=251 PF=0.6639 net=-32961.41 | not run | not run | REJECT_FIT_VAL |
| 48 | stage3_premomentum_redesign | pre_momentum | n=308 PF=0.7065 net=-38016.58 | n=219 PF=0.6118 net=-33382.26 | not run | not run | REJECT_FIT_VAL |
| 49 | stage3_premomentum_redesign | pre_momentum | n=98 PF=0.4511 net=-23623.07 | n=189 PF=0.8098 net=-13122.84 | not run | not run | REJECT_FIT_VAL |
| 50 | stage3_premomentum_redesign | pre_momentum | n=359 PF=0.7307 net=-38038.62 | n=343 PF=0.6482 net=-47294.32 | not run | not run | REJECT_FIT_VAL |
| 51 | stage3_premomentum_redesign | pre_momentum | n=126 PF=0.9126 net=-4305.05 | n=96 PF=0.859 net=-4472.18 | not run | not run | REJECT_FIT_VAL |
| 52 | stage3_premomentum_redesign | pre_momentum | n=265 PF=0.5747 net=-48706.95 | n=265 PF=0.5676 net=-47734.52 | not run | not run | REJECT_FIT_VAL |
| 53 | stage3_premomentum_redesign | pre_momentum | n=338 PF=0.6564 net=-47990.02 | n=304 PF=0.6618 net=-39445.36 | not run | not run | REJECT_FIT_VAL |
| 54 | stage3_premomentum_redesign | pre_momentum | n=162 PF=0.7821 net=-14316.86 | n=132 PF=0.8093 net=-9614.46 | not run | not run | REJECT_FIT_VAL |
| 55 | stage3_premomentum_redesign | pre_momentum | n=48 PF=0.6252 net=-6440.64 | n=119 PF=0.8227 net=-7717.74 | not run | not run | REJECT_FIT_VAL |
| 56 | stage3_premomentum_redesign | pre_momentum | n=300 PF=0.5876 net=-54869.08 | n=228 PF=0.5879 net=-36742.68 | not run | not run | REJECT_FIT_VAL |
| 57 | stage3_premomentum_redesign | pre_momentum | n=139 PF=0.7058 net=-17544.08 | n=107 PF=0.5317 net=-22150.39 | not run | not run | REJECT_FIT_VAL |
| 58 | stage3_premomentum_redesign | pre_momentum | n=115 PF=0.4813 net=-31455.3 | n=54 PF=0.5318 net=-11784.76 | not run | not run | REJECT_FIT_VAL |
| 59 | stage3_premomentum_redesign | pre_momentum | n=115 PF=0.4572 net=-34345.24 | n=54 PF=0.4783 net=-12910.69 | not run | not run | REJECT_FIT_VAL |
| 60 | stage3_premomentum_redesign | pre_momentum | n=284 PF=0.721 net=-32345.82 | n=250 PF=0.6772 net=-30237.63 | not run | not run | REJECT_FIT_VAL |
| 61 | stage5_optuna_combo | combination | n=26 PF=1.0899 net=905.91 | n=25 PF=0.6555 net=-2730.65 | not run | not run | REJECT_FIT_VAL |
| 62 | stage5_optuna_combo | combination | n=42 PF=0.6883 net=-5051.18 | n=29 PF=0.4883 net=-6661.67 | not run | not run | REJECT_FIT_VAL |
| 63 | stage5_optuna_combo | combination | n=56 PF=0.4766 net=-11296.2 | n=57 PF=0.3641 net=-15387.1 | not run | not run | REJECT_FIT_VAL |
| 64 | stage5_optuna_combo | combination | n=197 PF=0.7296 net=-23023.43 | n=159 PF=0.7422 net=-15245.78 | not run | not run | REJECT_FIT_VAL |
| 65 | stage5_optuna_combo | combination | n=60 PF=1.1992 net=4525.91 | n=59 PF=1.2328 net=4534.03 | n=119 PF=1.2147 net=9059.95 | not run | TRAIN_OUT_OF_BAND |
| 66 | stage5_optuna_combo | combination | n=333 PF=0.5194 net=-62136.6 | n=277 PF=0.653 net=-34428.56 | not run | not run | REJECT_FIT_VAL |
| 67 | stage5_optuna_combo | combination | n=48 PF=0.9078 net=-1881.36 | n=0 PF=0.0 net=0.0 | not run | not run | REJECT_FIT_VAL |
| 68 | stage5_optuna_combo | combination | n=80 PF=0.8647 net=-2412.18 | n=120 PF=0.448 net=-20214.1 | not run | not run | REJECT_FIT_VAL |
| 69 | stage5_optuna_combo | combination | n=248 PF=0.6792 net=-35456.09 | n=211 PF=0.7245 net=-24510.63 | not run | not run | REJECT_FIT_VAL |
| 70 | stage5_optuna_combo | combination | n=190 PF=0.5231 net=-44183.49 | n=145 PF=0.594 net=-26917.78 | not run | not run | REJECT_FIT_VAL |
| 71 | stage5_optuna_combo | combination | n=11 PF=1.462 net=2045.13 | n=10 PF=4.79 net=5143.81 | n=21 PF=2.243 net=7188.94 | not run | TRAIN_OUT_OF_BAND |
| 72 | stage5_optuna_combo | combination | n=180 PF=0.5259 net=-39738.41 | n=131 PF=0.7007 net=-14815.98 | not run | not run | REJECT_FIT_VAL |
| 73 | stage5_optuna_combo | combination | n=179 PF=0.5017 net=-62975.61 | n=126 PF=0.8892 net=-6409.13 | not run | not run | REJECT_FIT_VAL |
| 74 | stage5_optuna_combo | combination | n=62 PF=0.5651 net=-16449.87 | n=21 PF=0.6596 net=-3906.13 | not run | not run | REJECT_FIT_VAL |
| 75 | stage5_seeded_random_combo | combination | n=80 PF=0.5388 net=-11889.36 | n=118 PF=0.4351 net=-22565.98 | not run | not run | REJECT_FIT_VAL |

Full CSV: `C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\Train_and_Test\setup_recovery_full_loop\D_EMA20_REJECTION\iteration_log.csv`
