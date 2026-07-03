# D_EMA20_BOUNCE Iteration Log

Each row changes one logical group where possible; combination/Optuna rows are marked separately and still use FIT/VAL objective only.

| # | Stage | Group | FIT | VAL | TRAIN | TEST | Decision |
|---|---|---|---|---|---|---|---|
| 1 | stage1_baseline | baseline | n=486 PF=0.5551 net=-99646.29 | n=324 PF=0.4251 net=-86398.27 | n=810 PF=0.5029 net=-186044.56 | n=296 PF=0.5242 net=-60440.79 | TRAIN_OUT_OF_BAND_BASELINE |
| 2 | stage4_exit_sweep | exit | n=540 PF=0.3641 net=-113891.05 | n=335 PF=0.3082 net=-78199.37 | not run | not run | REJECT_FIT_VAL |
| 3 | stage4_exit_sweep | exit | n=540 PF=0.3938 net=-113336.98 | n=335 PF=0.3255 net=-79330.75 | not run | not run | REJECT_FIT_VAL |
| 4 | stage4_exit_sweep | exit | n=540 PF=0.4185 net=-114136.75 | n=335 PF=0.3447 net=-80083.35 | not run | not run | REJECT_FIT_VAL |
| 5 | stage4_exit_sweep | exit | n=539 PF=0.4597 net=-107564.35 | n=334 PF=0.3414 net=-82725.62 | not run | not run | REJECT_FIT_VAL |
| 6 | stage4_exit_sweep | exit | n=536 PF=0.4858 net=-103051.15 | n=334 PF=0.3593 net=-81208.4 | not run | not run | REJECT_FIT_VAL |
| 7 | stage4_exit_sweep | exit | n=533 PF=0.505 net=-98941.15 | n=334 PF=0.3655 net=-80420.4 | not run | not run | REJECT_FIT_VAL |
| 8 | stage4_exit_sweep | exit | n=527 PF=0.5355 net=-91914.5 | n=334 PF=0.3348 net=-85363.03 | not run | not run | REJECT_FIT_VAL |
| 9 | stage4_exit_sweep | exit | n=522 PF=0.5071 net=-97530.01 | n=334 PF=0.3314 net=-86169.98 | not run | not run | REJECT_FIT_VAL |
| 10 | stage4_exit_sweep | exit | n=518 PF=0.5183 net=-94506.28 | n=334 PF=0.3272 net=-87072.15 | not run | not run | REJECT_FIT_VAL |
| 11 | stage4_exit_sweep | exit | n=518 PF=0.5173 net=-94710.09 | n=334 PF=0.3463 net=-84596.51 | not run | not run | REJECT_FIT_VAL |
| 12 | stage4_exit_sweep | exit | n=539 PF=0.3789 net=-119576.72 | n=335 PF=0.3463 net=-76929.62 | not run | not run | REJECT_FIT_VAL |
| 13 | stage4_exit_sweep | exit | n=536 PF=0.415 net=-117003.41 | n=334 PF=0.3663 net=-77521.24 | not run | not run | REJECT_FIT_VAL |
| 14 | stage4_exit_sweep | exit | n=533 PF=0.452 net=-114105.29 | n=333 PF=0.3867 net=-78908.2 | not run | not run | REJECT_FIT_VAL |
| 15 | stage4_exit_sweep | exit | n=527 PF=0.504 net=-104014.63 | n=332 PF=0.3966 net=-80131.5 | not run | not run | REJECT_FIT_VAL |
| 16 | stage4_exit_sweep | exit | n=521 PF=0.5208 net=-101241.99 | n=332 PF=0.4157 net=-78455.67 | not run | not run | REJECT_FIT_VAL |
| 17 | stage4_exit_sweep | exit | n=517 PF=0.5372 net=-97046.63 | n=332 PF=0.4171 net=-78440.21 | not run | not run | REJECT_FIT_VAL |
| 18 | stage4_exit_sweep | exit | n=513 PF=0.5695 net=-90174.05 | n=332 PF=0.3955 net=-82493.81 | not run | not run | REJECT_FIT_VAL |
| 19 | stage4_exit_sweep | exit | n=500 PF=0.5483 net=-93267.97 | n=332 PF=0.4134 net=-80423.69 | not run | not run | REJECT_FIT_VAL |
| 20 | stage3_time_rank_redesign | entry_guard | n=486 PF=0.5551 net=-99646.29 | n=324 PF=0.4251 net=-86398.27 | not run | not run | REJECT_FIT_VAL |
| 21 | stage3_time_rank_redesign | entry_guard | n=486 PF=0.5551 net=-99646.29 | n=324 PF=0.4251 net=-86398.27 | not run | not run | REJECT_FIT_VAL |
| 22 | stage3_time_rank_redesign | entry_guard | n=486 PF=0.5551 net=-99646.29 | n=324 PF=0.4251 net=-86398.27 | not run | not run | REJECT_FIT_VAL |
| 23 | stage3_time_rank_redesign | entry_guard | n=485 PF=0.5572 net=-98815.88 | n=324 PF=0.4251 net=-86398.27 | not run | not run | REJECT_FIT_VAL |
| 24 | stage3_time_rank_redesign | entry_guard | n=36 PF=0.8846 net=-2095.99 | n=23 PF=0.5699 net=-4928.82 | not run | not run | REJECT_FIT_VAL |
| 25 | stage3_time_rank_redesign | entry_guard | n=165 PF=0.545 net=-40124.06 | n=110 PF=0.4127 net=-33660.47 | not run | not run | REJECT_FIT_VAL |
| 26 | stage3_time_rank_redesign | entry_guard | n=312 PF=0.5844 net=-61773.78 | n=224 PF=0.4752 net=-54873.63 | not run | not run | REJECT_FIT_VAL |
| 27 | stage3_time_rank_redesign | entry_guard | n=396 PF=0.5954 net=-73776.83 | n=274 PF=0.4612 net=-68767.75 | not run | not run | REJECT_FIT_VAL |
| 28 | stage3_time_rank_redesign | entry_guard | n=312 PF=0.5844 net=-61773.78 | n=224 PF=0.4752 net=-54873.63 | not run | not run | REJECT_FIT_VAL |
| 29 | stage3_time_rank_redesign | entry_guard | n=366 PF=0.5553 net=-76588.45 | n=248 PF=0.3715 net=-71749.96 | not run | not run | REJECT_FIT_VAL |
| 30 | stage3_time_rank_redesign | entry_guard | n=454 PF=0.5615 net=-92249.37 | n=312 PF=0.4312 net=-81794.31 | not run | not run | REJECT_FIT_VAL |
| 31 | stage3_time_rank_redesign | entry_guard | n=476 PF=0.564 net=-95133.32 | n=323 PF=0.416 net=-87762.75 | not run | not run | REJECT_FIT_VAL |
| 32 | stage4_signal_filter_sweep | signal_filter | n=99 PF=0.5937 net=-18964.2 | n=73 PF=0.3719 net=-20807.21 | not run | not run | REJECT_FIT_VAL |
| 33 | stage4_signal_filter_sweep | signal_filter | n=116 PF=0.6202 net=-20474.0 | n=60 PF=0.2961 net=-22736.21 | not run | not run | REJECT_FIT_VAL |
| 34 | stage4_signal_filter_sweep | signal_filter | n=63 PF=0.3537 net=-18834.22 | n=33 PF=0.1394 net=-12123.05 | not run | not run | REJECT_FIT_VAL |
| 35 | stage4_signal_filter_sweep | signal_filter | n=113 PF=0.4413 net=-34200.35 | n=63 PF=0.4252 net=-17867.5 | not run | not run | REJECT_FIT_VAL |
| 36 | stage4_signal_filter_sweep | signal_filter | n=444 PF=0.5851 net=-82480.41 | n=300 PF=0.4382 net=-76014.99 | not run | not run | REJECT_FIT_VAL |
| 37 | stage4_signal_filter_sweep | signal_filter | n=416 PF=0.5257 net=-91063.32 | n=255 PF=0.4228 net=-62801.05 | not run | not run | REJECT_FIT_VAL |
| 38 | stage4_signal_filter_sweep | signal_filter | n=193 PF=0.4602 net=-54539.71 | n=116 PF=0.354 net=-37294.89 | not run | not run | REJECT_FIT_VAL |
| 39 | stage4_signal_filter_sweep | signal_filter | n=341 PF=0.5666 net=-69275.6 | n=212 PF=0.4313 net=-55696.66 | not run | not run | REJECT_FIT_VAL |
| 40 | stage4_signal_filter_sweep | signal_filter | n=115 PF=0.5505 net=-25732.92 | n=59 PF=0.549 net=-12026.84 | not run | not run | REJECT_FIT_VAL |
| 41 | stage4_signal_filter_sweep | signal_filter | n=284 PF=0.5332 net=-61840.71 | n=78 PF=0.4835 net=-18847.3 | not run | not run | REJECT_FIT_VAL |
| 42 | stage4_signal_filter_sweep | signal_filter | n=412 PF=0.611 net=-71261.1 | n=256 PF=0.4347 net=-65638.14 | not run | not run | REJECT_FIT_VAL |
| 43 | stage4_signal_filter_sweep | signal_filter | n=48 PF=1.1443 net=3098.42 | n=36 PF=0.3868 net=-12618.99 | not run | not run | REJECT_FIT_VAL |
| 44 | stage4_signal_filter_sweep | signal_filter | n=338 PF=0.5421 net=-68606.05 | n=223 PF=0.4207 net=-58169.36 | not run | not run | REJECT_FIT_VAL |
| 45 | stage4_signal_filter_sweep | signal_filter | n=459 PF=0.5395 net=-98752.97 | n=288 PF=0.4651 net=-69378.71 | not run | not run | REJECT_FIT_VAL |
| 46 | stage3_premomentum_redesign | pre_momentum | n=182 PF=0.6223 net=-28584.42 | n=128 PF=0.3175 net=-35289.97 | not run | not run | REJECT_FIT_VAL |
| 47 | stage3_premomentum_redesign | pre_momentum | n=408 PF=0.5488 net=-84316.4 | n=263 PF=0.4576 net=-62506.44 | not run | not run | REJECT_FIT_VAL |
| 48 | stage3_premomentum_redesign | pre_momentum | n=404 PF=0.5286 net=-87594.89 | n=269 PF=0.4648 net=-63617.87 | not run | not run | REJECT_FIT_VAL |
| 49 | stage3_premomentum_redesign | pre_momentum | n=45 PF=0.4079 net=-15127.16 | n=45 PF=0.3866 net=-16763.52 | not run | not run | REJECT_FIT_VAL |
| 50 | stage3_premomentum_redesign | pre_momentum | n=60 PF=0.4287 net=-15871.66 | n=32 PF=0.3033 net=-10631.0 | not run | not run | REJECT_FIT_VAL |
| 51 | stage3_premomentum_redesign | pre_momentum | n=108 PF=0.4789 net=-26459.68 | n=71 PF=0.4262 net=-19277.07 | not run | not run | REJECT_FIT_VAL |
| 52 | stage3_premomentum_redesign | pre_momentum | n=260 PF=0.6342 net=-40884.2 | n=182 PF=0.3525 net=-49344.7 | not run | not run | REJECT_FIT_VAL |
| 53 | stage3_premomentum_redesign | pre_momentum | n=168 PF=0.5458 net=-32489.27 | n=141 PF=0.3339 net=-37691.63 | not run | not run | REJECT_FIT_VAL |
| 54 | stage3_premomentum_redesign | pre_momentum | n=32 PF=0.4313 net=-8479.62 | n=60 PF=0.2925 net=-20382.43 | not run | not run | REJECT_FIT_VAL |
| 55 | stage3_premomentum_redesign | pre_momentum | n=459 PF=0.5446 net=-96554.57 | n=290 PF=0.418 net=-76824.71 | not run | not run | REJECT_FIT_VAL |
| 56 | stage3_premomentum_redesign | pre_momentum | n=147 PF=0.5106 net=-34473.88 | n=86 PF=0.3917 net=-23923.65 | not run | not run | REJECT_FIT_VAL |
| 57 | stage3_premomentum_redesign | pre_momentum | n=322 PF=0.5706 net=-62247.73 | n=245 PF=0.4228 net=-63619.5 | not run | not run | REJECT_FIT_VAL |
| 58 | stage3_premomentum_redesign | pre_momentum | n=416 PF=0.5435 net=-88341.78 | n=257 PF=0.4604 net=-65115.94 | not run | not run | REJECT_FIT_VAL |
| 59 | stage3_premomentum_redesign | pre_momentum | n=239 PF=0.6111 net=-40115.54 | n=206 PF=0.3843 net=-57062.11 | not run | not run | REJECT_FIT_VAL |
| 60 | stage3_premomentum_redesign | pre_momentum | n=445 PF=0.5772 net=-84408.93 | n=297 PF=0.4222 net=-77848.23 | not run | not run | REJECT_FIT_VAL |
| 61 | stage5_optuna_combo | combination | n=38 PF=0.5135 net=-7035.56 | n=20 PF=0.3568 net=-5399.98 | not run | not run | REJECT_FIT_VAL |
| 62 | stage5_optuna_combo | combination | n=81 PF=0.8335 net=-4566.25 | n=30 PF=0.4597 net=-8460.45 | not run | not run | REJECT_FIT_VAL |
| 63 | stage5_optuna_combo | combination | n=46 PF=0.5357 net=-7410.99 | n=35 PF=0.5199 net=-6111.76 | not run | not run | REJECT_FIT_VAL |
| 64 | stage5_optuna_combo | combination | n=260 PF=0.5225 net=-61589.14 | n=134 PF=0.4443 net=-37271.41 | not run | not run | REJECT_FIT_VAL |
| 65 | stage5_optuna_combo | combination | n=86 PF=0.7361 net=-10947.55 | n=40 PF=0.1605 net=-19623.42 | not run | not run | REJECT_FIT_VAL |
| 66 | stage5_optuna_combo | combination | n=431 PF=0.5415 net=-76446.82 | n=241 PF=0.3386 net=-64384.38 | not run | not run | REJECT_FIT_VAL |
| 67 | stage5_optuna_combo | combination | n=55 PF=0.7801 net=-4773.46 | n=9 PF=0.2326 net=-4181.62 | not run | not run | REJECT_FIT_VAL |
| 68 | stage5_optuna_combo | combination | n=151 PF=0.3828 net=-36513.96 | n=106 PF=0.2821 net=-28880.08 | not run | not run | REJECT_FIT_VAL |
| 69 | stage5_optuna_combo | combination | n=306 PF=0.5825 net=-63239.62 | n=186 PF=0.448 net=-50626.83 | not run | not run | REJECT_FIT_VAL |
| 70 | stage5_optuna_combo | combination | n=242 PF=0.4794 net=-70460.42 | n=143 PF=0.4164 net=-44978.97 | not run | not run | REJECT_FIT_VAL |
| 71 | stage5_seeded_random_combo | combination | n=387 PF=0.5909 net=-75045.62 | n=272 PF=0.4677 net=-70772.11 | not run | not run | REJECT_FIT_VAL |
| 72 | stage5_seeded_random_combo | combination | n=102 PF=0.768 net=-9015.77 | n=82 PF=0.3916 net=-24885.55 | not run | not run | REJECT_FIT_VAL |
| 73 | stage5_seeded_random_combo | combination | n=62 PF=0.4576 net=-15663.43 | n=50 PF=0.446 net=-9620.06 | not run | not run | REJECT_FIT_VAL |
| 74 | stage5_seeded_random_combo | combination | n=160 PF=0.554 net=-34425.79 | n=81 PF=0.4961 net=-22326.99 | not run | not run | REJECT_FIT_VAL |
| 75 | stage5_seeded_random_combo | combination | n=130 PF=0.386 net=-37977.91 | n=111 PF=0.2945 net=-36421.02 | not run | not run | REJECT_FIT_VAL |

Full CSV: `C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\Train_and_Test\setup_recovery_full_loop\D_EMA20_BOUNCE\iteration_log.csv`
