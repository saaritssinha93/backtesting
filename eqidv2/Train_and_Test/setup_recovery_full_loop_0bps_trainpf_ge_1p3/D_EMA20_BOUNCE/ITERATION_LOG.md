# D_EMA20_BOUNCE Iteration Log

Each row changes one logical group where possible; combination/Optuna rows are marked separately and still use FIT/VAL objective only.

| # | Stage | Group | FIT | VAL | TRAIN | TEST | Decision |
|---|---|---|---|---|---|---|---|
| 1 | stage1_baseline | baseline | n=476 PF=0.7379 net=-49973.74 | n=323 PF=0.59 net=-52840.45 | n=799 PF=0.6783 net=-102814.2 | n=296 PF=0.731 net=-28691.63 | TRAIN_OUT_OF_BAND_BASELINE |
| 2 | stage4_exit_sweep | exit | n=540 PF=0.5698 net=-64251.51 | n=335 PF=0.5182 net=-44589.7 | not run | not run | REJECT_FIT_VAL |
| 3 | stage4_exit_sweep | exit | n=540 PF=0.5916 net=-64104.53 | n=335 PF=0.5564 net=-42225.04 | not run | not run | REJECT_FIT_VAL |
| 4 | stage4_exit_sweep | exit | n=536 PF=0.6047 net=-65532.45 | n=333 PF=0.5869 net=-41135.88 | not run | not run | REJECT_FIT_VAL |
| 5 | stage4_exit_sweep | exit | n=531 PF=0.6722 net=-54645.31 | n=332 PF=0.5684 net=-44823.54 | not run | not run | REJECT_FIT_VAL |
| 6 | stage4_exit_sweep | exit | n=527 PF=0.7104 net=-48339.45 | n=332 PF=0.5651 net=-46021.48 | not run | not run | REJECT_FIT_VAL |
| 7 | stage4_exit_sweep | exit | n=523 PF=0.7309 net=-44709.12 | n=332 PF=0.5771 net=-44753.15 | not run | not run | REJECT_FIT_VAL |
| 8 | stage4_exit_sweep | exit | n=518 PF=0.7605 net=-39767.31 | n=332 PF=0.5495 net=-48199.93 | not run | not run | REJECT_FIT_VAL |
| 9 | stage4_exit_sweep | exit | n=511 PF=0.7691 net=-38242.71 | n=332 PF=0.5427 net=-49365.07 | not run | not run | REJECT_FIT_VAL |
| 10 | stage4_exit_sweep | exit | n=507 PF=0.7222 net=-45739.44 | n=332 PF=0.5512 net=-48662.34 | not run | not run | REJECT_FIT_VAL |
| 11 | stage4_exit_sweep | exit | n=507 PF=0.7404 net=-42743.18 | n=332 PF=0.5831 net=-45207.62 | not run | not run | REJECT_FIT_VAL |
| 12 | stage4_exit_sweep | exit | n=540 PF=0.5725 net=-69233.62 | n=335 PF=0.5308 net=-46465.73 | not run | not run | REJECT_FIT_VAL |
| 13 | stage4_exit_sweep | exit | n=538 PF=0.586 net=-71126.75 | n=335 PF=0.5549 net=-45749.26 | not run | not run | REJECT_FIT_VAL |
| 14 | stage4_exit_sweep | exit | n=533 PF=0.6109 net=-70022.12 | n=333 PF=0.5898 net=-44162.47 | not run | not run | REJECT_FIT_VAL |
| 15 | stage4_exit_sweep | exit | n=526 PF=0.686 net=-56739.03 | n=332 PF=0.5799 net=-47328.61 | not run | not run | REJECT_FIT_VAL |
| 16 | stage4_exit_sweep | exit | n=522 PF=0.6903 net=-57014.87 | n=332 PF=0.5678 net=-49957.72 | not run | not run | REJECT_FIT_VAL |
| 17 | stage4_exit_sweep | exit | n=516 PF=0.7193 net=-50938.41 | n=332 PF=0.5814 net=-48394.98 | not run | not run | REJECT_FIT_VAL |
| 18 | stage4_exit_sweep | exit | n=509 PF=0.7498 net=-45132.53 | n=332 PF=0.5601 net=-51451.23 | not run | not run | REJECT_FIT_VAL |
| 19 | stage4_exit_sweep | exit | n=496 PF=0.7617 net=-42417.53 | n=332 PF=0.5656 net=-51285.15 | not run | not run | REJECT_FIT_VAL |
| 20 | stage3_time_rank_redesign | entry_guard | n=476 PF=0.7379 net=-49973.74 | n=323 PF=0.59 net=-52840.45 | not run | not run | REJECT_FIT_VAL |
| 21 | stage3_time_rank_redesign | entry_guard | n=476 PF=0.7379 net=-49973.74 | n=323 PF=0.59 net=-52840.45 | not run | not run | REJECT_FIT_VAL |
| 22 | stage3_time_rank_redesign | entry_guard | n=476 PF=0.7379 net=-49973.74 | n=323 PF=0.59 net=-52840.45 | not run | not run | REJECT_FIT_VAL |
| 23 | stage3_time_rank_redesign | entry_guard | n=475 PF=0.741 net=-49193.23 | n=323 PF=0.59 net=-52840.45 | not run | not run | REJECT_FIT_VAL |
| 24 | stage3_time_rank_redesign | entry_guard | n=36 PF=1.1529 net=2425.01 | n=23 PF=0.7899 net=-2036.95 | not run | not run | REJECT_FIT_VAL |
| 25 | stage3_time_rank_redesign | entry_guard | n=164 PF=0.7278 net=-20576.98 | n=110 PF=0.5448 net=-23115.48 | not run | not run | REJECT_FIT_VAL |
| 26 | stage3_time_rank_redesign | entry_guard | n=311 PF=0.7553 net=-31738.36 | n=224 PF=0.6388 net=-33080.15 | not run | not run | REJECT_FIT_VAL |
| 27 | stage3_time_rank_redesign | entry_guard | n=392 PF=0.7724 net=-35822.54 | n=274 PF=0.6341 net=-40282.7 | not run | not run | REJECT_FIT_VAL |
| 28 | stage3_time_rank_redesign | entry_guard | n=311 PF=0.7553 net=-31738.36 | n=224 PF=0.6388 net=-33080.15 | not run | not run | REJECT_FIT_VAL |
| 29 | stage3_time_rank_redesign | entry_guard | n=366 PF=0.7286 net=-40610.98 | n=248 PF=0.5386 net=-44934.09 | not run | not run | REJECT_FIT_VAL |
| 30 | stage3_time_rank_redesign | entry_guard | n=450 PF=0.7413 net=-46714.76 | n=311 PF=0.5995 net=-49486.25 | not run | not run | REJECT_FIT_VAL |
| 31 | stage3_time_rank_redesign | entry_guard | n=468 PF=0.7582 net=-44976.3 | n=322 PF=0.579 net=-54254.89 | not run | not run | REJECT_FIT_VAL |
| 32 | stage4_signal_filter_sweep | signal_filter | n=99 PF=0.758 net=-10000.34 | n=73 PF=0.5046 net=-13782.27 | not run | not run | REJECT_FIT_VAL |
| 33 | stage4_signal_filter_sweep | signal_filter | n=116 PF=0.8252 net=-8290.54 | n=60 PF=0.4141 net=-16298.23 | not run | not run | REJECT_FIT_VAL |
| 34 | stage4_signal_filter_sweep | signal_filter | n=63 PF=0.51 net=-12202.03 | n=33 PF=0.2227 net=-9132.2 | not run | not run | REJECT_FIT_VAL |
| 35 | stage4_signal_filter_sweep | signal_filter | n=113 PF=0.5256 net=-26581.21 | n=63 PF=0.5202 net=-13532.7 | not run | not run | REJECT_FIT_VAL |
| 36 | stage4_signal_filter_sweep | signal_filter | n=436 PF=0.7928 net=-34673.27 | n=299 PF=0.6107 net=-45023.5 | not run | not run | REJECT_FIT_VAL |
| 37 | stage4_signal_filter_sweep | signal_filter | n=409 PF=0.7091 net=-47162.15 | n=254 PF=0.5945 net=-37561.44 | not run | not run | REJECT_FIT_VAL |
| 38 | stage4_signal_filter_sweep | signal_filter | n=193 PF=0.5572 net=-40302.69 | n=116 PF=0.4655 net=-27320.15 | not run | not run | REJECT_FIT_VAL |
| 39 | stage4_signal_filter_sweep | signal_filter | n=341 PF=0.7321 net=-37217.59 | n=212 PF=0.5843 net=-34979.45 | not run | not run | REJECT_FIT_VAL |
| 40 | stage4_signal_filter_sweep | signal_filter | n=115 PF=0.7135 net=-13808.54 | n=59 PF=0.6668 net=-7917.32 | not run | not run | REJECT_FIT_VAL |
| 41 | stage4_signal_filter_sweep | signal_filter | n=284 PF=0.6886 net=-36208.64 | n=78 PF=0.6162 net=-12160.42 | not run | not run | REJECT_FIT_VAL |
| 42 | stage4_signal_filter_sweep | signal_filter | n=410 PF=0.7855 net=-34090.0 | n=256 PF=0.6011 net=-39597.11 | not run | not run | REJECT_FIT_VAL |
| 43 | stage4_signal_filter_sweep | signal_filter | n=48 PF=1.5542 net=9803.86 | n=36 PF=0.5056 net=-9029.76 | not run | not run | REJECT_FIT_VAL |
| 44 | stage4_signal_filter_sweep | signal_filter | n=336 PF=0.7097 net=-38037.77 | n=223 PF=0.5925 net=-34957.2 | not run | not run | REJECT_FIT_VAL |
| 45 | stage4_signal_filter_sweep | signal_filter | n=449 PF=0.7075 net=-53532.33 | n=287 PF=0.6335 net=-41231.97 | not run | not run | REJECT_FIT_VAL |
| 46 | stage3_premomentum_redesign | pre_momentum | n=182 PF=0.8485 net=-9673.44 | n=128 PF=0.4446 net=-24198.04 | not run | not run | REJECT_FIT_VAL |
| 47 | stage3_premomentum_redesign | pre_momentum | n=403 PF=0.7213 net=-44773.49 | n=263 PF=0.6389 net=-35543.9 | not run | not run | REJECT_FIT_VAL |
| 48 | stage3_premomentum_redesign | pre_momentum | n=398 PF=0.6922 net=-48791.97 | n=268 PF=0.6455 net=-36034.95 | not run | not run | REJECT_FIT_VAL |
| 49 | stage3_premomentum_redesign | pre_momentum | n=45 PF=0.5083 net=-11626.15 | n=45 PF=0.5036 net=-11814.16 | not run | not run | REJECT_FIT_VAL |
| 50 | stage3_premomentum_redesign | pre_momentum | n=60 PF=0.5729 net=-10266.19 | n=32 PF=0.4619 net=-6837.67 | not run | not run | REJECT_FIT_VAL |
| 51 | stage3_premomentum_redesign | pre_momentum | n=108 PF=0.625 net=-16270.69 | n=71 PF=0.5379 net=-13590.47 | not run | not run | REJECT_FIT_VAL |
| 52 | stage3_premomentum_redesign | pre_momentum | n=260 PF=0.8296 net=-16271.29 | n=182 PF=0.5293 net=-29505.9 | not run | not run | REJECT_FIT_VAL |
| 53 | stage3_premomentum_redesign | pre_momentum | n=168 PF=0.753 net=-14743.72 | n=141 PF=0.5092 net=-22625.43 | not run | not run | REJECT_FIT_VAL |
| 54 | stage3_premomentum_redesign | pre_momentum | n=32 PF=0.5802 net=-5256.63 | n=60 PF=0.4798 net=-12359.86 | not run | not run | REJECT_FIT_VAL |
| 55 | stage3_premomentum_redesign | pre_momentum | n=448 PF=0.7254 net=-49188.05 | n=290 PF=0.5718 net=-48603.9 | not run | not run | REJECT_FIT_VAL |
| 56 | stage3_premomentum_redesign | pre_momentum | n=147 PF=0.6667 net=-20216.09 | n=86 PF=0.5471 net=-15429.06 | not run | not run | REJECT_FIT_VAL |
| 57 | stage3_premomentum_redesign | pre_momentum | n=321 PF=0.7426 net=-32518.53 | n=245 PF=0.6167 net=-35662.83 | not run | not run | REJECT_FIT_VAL |
| 58 | stage3_premomentum_redesign | pre_momentum | n=411 PF=0.6893 net=-52556.39 | n=257 PF=0.6455 net=-36505.58 | not run | not run | REJECT_FIT_VAL |
| 59 | stage3_premomentum_redesign | pre_momentum | n=239 PF=0.7939 net=-18553.98 | n=206 PF=0.5669 net=-34045.11 | not run | not run | REJECT_FIT_VAL |
| 60 | stage3_premomentum_redesign | pre_momentum | n=438 PF=0.7719 net=-38853.34 | n=296 PF=0.5837 net=-48288.73 | not run | not run | REJECT_FIT_VAL |
| 61 | stage5_optuna_combo | combination | n=38 PF=0.678 net=-4088.6 | n=20 PF=0.4607 net=-4012.33 | not run | not run | REJECT_FIT_VAL |
| 62 | stage5_optuna_combo | combination | n=81 PF=1.0923 net=2188.31 | n=30 PF=0.5462 net=-6472.22 | not run | not run | REJECT_FIT_VAL |
| 63 | stage5_optuna_combo | combination | n=46 PF=0.8135 net=-2366.56 | n=35 PF=0.6896 net=-3492.54 | not run | not run | REJECT_FIT_VAL |
| 64 | stage5_optuna_combo | combination | n=260 PF=0.6685 net=-37554.08 | n=134 PF=0.5566 net=-26330.68 | not run | not run | REJECT_FIT_VAL |
| 65 | stage5_optuna_combo | combination | n=86 PF=0.9137 net=-3159.49 | n=40 PF=0.2117 net=-16444.17 | not run | not run | REJECT_FIT_VAL |
| 66 | stage5_optuna_combo | combination | n=428 PF=0.806 net=-27316.27 | n=241 PF=0.5689 net=-35604.85 | not run | not run | REJECT_FIT_VAL |
| 67 | stage5_optuna_combo | combination | n=55 PF=1.0052 net=100.22 | n=9 PF=0.3009 net=-3294.23 | not run | not run | REJECT_FIT_VAL |
| 68 | stage5_optuna_combo | combination | n=151 PF=0.5638 net=-21433.53 | n=106 PF=0.4138 net=-20320.71 | not run | not run | REJECT_FIT_VAL |
| 69 | stage5_optuna_combo | combination | n=304 PF=0.7385 net=-34792.02 | n=186 PF=0.5703 net=-35206.34 | not run | not run | REJECT_FIT_VAL |
| 70 | stage5_optuna_combo | combination | n=241 PF=0.7014 net=-33871.09 | n=143 PF=0.5188 net=-33011.31 | not run | not run | REJECT_FIT_VAL |
| 71 | stage5_optuna_combo | combination | n=2 PF=0.6936 net=-227.92 | n=1 PF=0.0 net=-827.07 | not run | not run | REJECT_FIT_VAL |
| 72 | stage5_optuna_combo | combination | n=33 PF=0.5737 net=-6554.47 | n=21 PF=0.6115 net=-4505.65 | not run | not run | REJECT_FIT_VAL |
| 73 | stage5_optuna_combo | combination | n=30 PF=0.6578 net=-5537.82 | n=25 PF=0.4128 net=-8062.28 | not run | not run | REJECT_FIT_VAL |
| 74 | stage5_optuna_combo | combination | n=27 PF=0.4782 net=-4032.88 | n=17 PF=1.0457 net=195.76 | not run | not run | REJECT_FIT_VAL |
| 75 | stage5_optuna_combo | combination | n=50 PF=0.9771 net=-462.49 | n=20 PF=0.7967 net=-2005.86 | not run | not run | REJECT_FIT_VAL |

Full CSV: `C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\Train_and_Test\setup_recovery_full_loop_0bps_trainpf_ge_1p3\D_EMA20_BOUNCE\iteration_log.csv`
