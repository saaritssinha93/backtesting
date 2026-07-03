# D_AVWAP_LOSE_REVERSAL Iteration Log

Each row changes one logical group where possible; combination/Optuna rows are marked separately and still use FIT/VAL objective only.

| # | Stage | Group | FIT | VAL | TRAIN | TEST | Decision |
|---|---|---|---|---|---|---|---|
| 1 | stage1_baseline | baseline | n=778 PF=0.6263 net=-156659.63 | n=603 PF=0.752 net=-67404.87 | n=1381 PF=0.6757 net=-224064.5 | n=542 PF=0.7522 net=-57725.0 | TRAIN_OUT_OF_BAND_BASELINE |
| 2 | stage4_exit_sweep | exit | n=1268 PF=0.3716 net=-263979.99 | n=1096 PF=0.3858 net=-217683.06 | not run | not run | REJECT_FIT_VAL |
| 3 | stage4_exit_sweep | exit | n=1252 PF=0.4007 net=-260062.96 | n=1057 PF=0.4125 net=-209011.49 | not run | not run | REJECT_FIT_VAL |
| 4 | stage4_exit_sweep | exit | n=1207 PF=0.4384 net=-248029.27 | n=1019 PF=0.4498 net=-199064.11 | not run | not run | REJECT_FIT_VAL |
| 5 | stage4_exit_sweep | exit | n=1167 PF=0.4739 net=-231277.82 | n=985 PF=0.5261 net=-167480.49 | not run | not run | REJECT_FIT_VAL |
| 6 | stage4_exit_sweep | exit | n=1132 PF=0.5228 net=-204741.51 | n=958 PF=0.537 net=-162912.2 | not run | not run | REJECT_FIT_VAL |
| 7 | stage4_exit_sweep | exit | n=1124 PF=0.5517 net=-191595.82 | n=932 PF=0.55 net=-154382.83 | not run | not run | REJECT_FIT_VAL |
| 8 | stage4_exit_sweep | exit | n=1098 PF=0.5663 net=-182609.69 | n=916 PF=0.5863 net=-140501.39 | not run | not run | REJECT_FIT_VAL |
| 9 | stage4_exit_sweep | exit | n=1065 PF=0.5588 net=-182828.88 | n=893 PF=0.6101 net=-130427.24 | not run | not run | REJECT_FIT_VAL |
| 10 | stage4_exit_sweep | exit | n=1035 PF=0.5544 net=-179453.81 | n=883 PF=0.6274 net=-123479.22 | not run | not run | REJECT_FIT_VAL |
| 11 | stage4_exit_sweep | exit | n=1021 PF=0.5476 net=-179588.33 | n=870 PF=0.6373 net=-118561.65 | not run | not run | REJECT_FIT_VAL |
| 12 | stage4_exit_sweep | exit | n=1225 PF=0.4121 net=-251919.94 | n=1027 PF=0.3915 net=-217885.33 | not run | not run | REJECT_FIT_VAL |
| 13 | stage4_exit_sweep | exit | n=1195 PF=0.4467 net=-242222.32 | n=999 PF=0.4271 net=-207367.71 | not run | not run | REJECT_FIT_VAL |
| 14 | stage4_exit_sweep | exit | n=1130 PF=0.4971 net=-219930.74 | n=946 PF=0.4688 net=-194309.75 | not run | not run | REJECT_FIT_VAL |
| 15 | stage4_exit_sweep | exit | n=1082 PF=0.533 net=-203512.98 | n=913 PF=0.5366 net=-165684.05 | not run | not run | REJECT_FIT_VAL |
| 16 | stage4_exit_sweep | exit | n=1044 PF=0.5814 net=-178640.73 | n=881 PF=0.5434 net=-161911.38 | not run | not run | REJECT_FIT_VAL |
| 17 | stage4_exit_sweep | exit | n=1031 PF=0.6153 net=-162746.48 | n=865 PF=0.5476 net=-158914.28 | not run | not run | REJECT_FIT_VAL |
| 18 | stage4_exit_sweep | exit | n=1013 PF=0.6068 net=-165943.82 | n=837 PF=0.5792 net=-144102.31 | not run | not run | REJECT_FIT_VAL |
| 19 | stage4_exit_sweep | exit | n=981 PF=0.6032 net=-165127.46 | n=807 PF=0.6032 net=-131676.27 | not run | not run | REJECT_FIT_VAL |
| 20 | stage3_time_rank_redesign | entry_guard | n=778 PF=0.6263 net=-156659.63 | n=603 PF=0.752 net=-67404.87 | not run | not run | REJECT_FIT_VAL |
| 21 | stage3_time_rank_redesign | entry_guard | n=778 PF=0.6263 net=-156659.63 | n=603 PF=0.752 net=-67404.87 | not run | not run | REJECT_FIT_VAL |
| 22 | stage3_time_rank_redesign | entry_guard | n=778 PF=0.6263 net=-156659.63 | n=603 PF=0.752 net=-67404.87 | not run | not run | REJECT_FIT_VAL |
| 23 | stage3_time_rank_redesign | entry_guard | n=777 PF=0.6279 net=-155606.16 | n=600 PF=0.7486 net=-67970.57 | not run | not run | REJECT_FIT_VAL |
| 24 | stage3_time_rank_redesign | entry_guard | n=212 PF=0.6154 net=-47371.86 | n=162 PF=1.187 net=12300.94 | not run | not run | REJECT_FIT_VAL |
| 25 | stage3_time_rank_redesign | entry_guard | n=469 PF=0.6707 net=-88028.25 | n=422 PF=0.7978 net=-40059.48 | not run | not run | REJECT_FIT_VAL |
| 26 | stage3_time_rank_redesign | entry_guard | n=623 PF=0.6601 net=-118368.45 | n=505 PF=0.7347 net=-63454.27 | not run | not run | REJECT_FIT_VAL |
| 27 | stage3_time_rank_redesign | entry_guard | n=700 PF=0.6598 net=-130419.34 | n=537 PF=0.7394 net=-65865.84 | not run | not run | REJECT_FIT_VAL |
| 28 | stage3_time_rank_redesign | entry_guard | n=623 PF=0.6601 net=-118368.45 | n=505 PF=0.7347 net=-63454.27 | not run | not run | REJECT_FIT_VAL |
| 29 | stage3_time_rank_redesign | entry_guard | n=544 PF=0.6723 net=-89043.85 | n=498 PF=0.7087 net=-65795.64 | not run | not run | REJECT_FIT_VAL |
| 30 | stage3_time_rank_redesign | entry_guard | n=716 PF=0.6154 net=-144726.72 | n=578 PF=0.717 net=-76676.81 | not run | not run | REJECT_FIT_VAL |
| 31 | stage3_time_rank_redesign | entry_guard | n=762 PF=0.6358 net=-147126.25 | n=593 PF=0.7463 net=-68761.81 | not run | not run | REJECT_FIT_VAL |
| 32 | stage4_signal_filter_sweep | signal_filter | n=611 PF=0.5156 net=-155263.76 | n=502 PF=0.6592 net=-73753.98 | not run | not run | REJECT_FIT_VAL |
| 33 | stage4_signal_filter_sweep | signal_filter | n=663 PF=0.5977 net=-142612.0 | n=533 PF=0.8475 net=-34189.25 | not run | not run | REJECT_FIT_VAL |
| 34 | stage4_signal_filter_sweep | signal_filter | n=416 PF=0.4832 net=-116094.24 | n=442 PF=0.6626 net=-65840.74 | not run | not run | REJECT_FIT_VAL |
| 35 | stage4_signal_filter_sweep | signal_filter | n=254 PF=0.8863 net=-11817.99 | n=278 PF=0.6573 net=-43404.72 | not run | not run | REJECT_FIT_VAL |
| 36 | stage4_signal_filter_sweep | signal_filter | n=594 PF=0.6271 net=-128416.06 | n=493 PF=0.7699 net=-53556.11 | not run | not run | REJECT_FIT_VAL |
| 37 | stage4_signal_filter_sweep | signal_filter | n=580 PF=0.6751 net=-98038.58 | n=484 PF=0.8784 net=-24514.91 | not run | not run | REJECT_FIT_VAL |
| 38 | stage4_signal_filter_sweep | signal_filter | n=477 PF=0.6909 net=-77563.06 | n=367 PF=0.9654 net=-4994.72 | not run | not run | REJECT_FIT_VAL |
| 39 | stage4_signal_filter_sweep | signal_filter | n=724 PF=0.6141 net=-150489.39 | n=592 PF=0.757 net=-66322.84 | not run | not run | REJECT_FIT_VAL |
| 40 | stage4_signal_filter_sweep | signal_filter | n=387 PF=0.4266 net=-116649.01 | n=268 PF=0.712 net=-34623.65 | not run | not run | REJECT_FIT_VAL |
| 41 | stage4_signal_filter_sweep | signal_filter | n=557 PF=0.6246 net=-109911.06 | n=465 PF=0.8031 net=-42331.46 | not run | not run | REJECT_FIT_VAL |
| 42 | stage4_signal_filter_sweep | signal_filter | n=750 PF=0.6162 net=-154401.9 | n=580 PF=0.7781 net=-56403.17 | not run | not run | REJECT_FIT_VAL |
| 43 | stage4_signal_filter_sweep | signal_filter | n=758 PF=0.6192 net=-156625.19 | n=590 PF=0.7734 net=-59514.86 | not run | not run | REJECT_FIT_VAL |
| 44 | stage4_signal_filter_sweep | signal_filter | n=569 PF=0.6752 net=-98425.08 | n=303 PF=0.6512 net=-53499.5 | not run | not run | REJECT_FIT_VAL |
| 45 | stage4_signal_filter_sweep | signal_filter | n=657 PF=0.6065 net=-139364.15 | n=553 PF=0.7536 net=-61603.31 | not run | not run | REJECT_FIT_VAL |
| 46 | stage3_premomentum_redesign | pre_momentum | n=466 PF=0.6446 net=-81814.9 | n=391 PF=0.7983 net=-30176.54 | not run | not run | REJECT_FIT_VAL |
| 47 | stage3_premomentum_redesign | pre_momentum | n=474 PF=0.631 net=-92994.37 | n=356 PF=0.8446 net=-24420.88 | not run | not run | REJECT_FIT_VAL |
| 48 | stage3_premomentum_redesign | pre_momentum | n=738 PF=0.6022 net=-157074.84 | n=575 PF=0.8195 net=-45057.89 | not run | not run | REJECT_FIT_VAL |
| 49 | stage3_premomentum_redesign | pre_momentum | n=450 PF=0.6393 net=-78274.61 | n=366 PF=0.7286 net=-39002.97 | not run | not run | REJECT_FIT_VAL |
| 50 | stage3_premomentum_redesign | pre_momentum | n=705 PF=0.631 net=-136802.66 | n=581 PF=0.6891 net=-82169.32 | not run | not run | REJECT_FIT_VAL |
| 51 | stage3_premomentum_redesign | pre_momentum | n=694 PF=0.6143 net=-139363.15 | n=571 PF=0.6675 net=-86001.2 | not run | not run | REJECT_FIT_VAL |
| 52 | stage3_premomentum_redesign | pre_momentum | n=178 PF=0.7147 net=-29448.32 | n=104 PF=0.8385 net=-8765.64 | not run | not run | REJECT_FIT_VAL |
| 53 | stage3_premomentum_redesign | pre_momentum | n=109 PF=0.4494 net=-24657.21 | n=168 PF=0.5048 net=-35463.78 | not run | not run | REJECT_FIT_VAL |
| 54 | stage3_premomentum_redesign | pre_momentum | n=136 PF=0.6883 net=-26151.58 | n=126 PF=0.8517 net=-9620.14 | not run | not run | REJECT_FIT_VAL |
| 55 | stage3_premomentum_redesign | pre_momentum | n=755 PF=0.6277 net=-150003.87 | n=589 PF=0.7598 net=-62827.86 | not run | not run | REJECT_FIT_VAL |
| 56 | stage3_premomentum_redesign | pre_momentum | n=187 PF=0.7163 net=-21255.67 | n=280 PF=0.633 net=-40653.12 | not run | not run | REJECT_FIT_VAL |
| 57 | stage3_premomentum_redesign | pre_momentum | n=298 PF=0.5586 net=-71440.65 | n=241 PF=0.7533 net=-25497.02 | not run | not run | REJECT_FIT_VAL |
| 58 | stage3_premomentum_redesign | pre_momentum | n=623 PF=0.68 net=-99299.65 | n=543 PF=0.8037 net=-47133.07 | not run | not run | REJECT_FIT_VAL |
| 59 | stage3_premomentum_redesign | pre_momentum | n=139 PF=0.7749 net=-18840.34 | n=129 PF=0.9631 net=-2335.51 | not run | not run | REJECT_FIT_VAL |
| 60 | stage3_premomentum_redesign | pre_momentum | n=733 PF=0.6015 net=-156061.34 | n=584 PF=0.7323 net=-69616.95 | not run | not run | REJECT_FIT_VAL |
| 61 | stage5_optuna_combo | combination | n=64 PF=0.907 net=-2171.63 | n=43 PF=0.5325 net=-9327.88 | not run | not run | REJECT_FIT_VAL |
| 62 | stage5_optuna_combo | combination | n=154 PF=0.6765 net=-24899.34 | n=77 PF=0.6703 net=-12022.96 | not run | not run | REJECT_FIT_VAL |
| 63 | stage5_optuna_combo | combination | n=232 PF=0.4046 net=-63848.91 | n=202 PF=0.4442 net=-46593.31 | not run | not run | REJECT_FIT_VAL |
| 64 | stage5_seeded_random_combo | combination | n=75 PF=0.4932 net=-13563.21 | n=142 PF=0.4588 net=-28169.57 | not run | not run | REJECT_FIT_VAL |
| 65 | stage5_seeded_random_combo | combination | n=79 PF=0.708 net=-8491.12 | n=66 PF=0.6079 net=-9839.65 | not run | not run | REJECT_FIT_VAL |
| 66 | stage5_seeded_random_combo | combination | n=167 PF=0.5517 net=-43174.41 | n=112 PF=0.6126 net=-22155.75 | not run | not run | REJECT_FIT_VAL |
| 67 | stage5_seeded_random_combo | combination | n=54 PF=0.3427 net=-19611.66 | n=82 PF=0.4998 net=-17377.4 | not run | not run | REJECT_FIT_VAL |
| 68 | stage5_seeded_random_combo | combination | n=413 PF=0.6405 net=-90230.24 | n=360 PF=0.8724 net=-21817.64 | not run | not run | REJECT_FIT_VAL |
| 69 | stage5_seeded_random_combo | combination | n=197 PF=0.551 net=-34731.63 | n=176 PF=0.7699 net=-14754.83 | not run | not run | REJECT_FIT_VAL |
| 70 | stage5_seeded_random_combo | combination | n=5 PF=0.0539 net=-3285.01 | n=17 PF=0.3727 net=-7028.05 | not run | not run | REJECT_FIT_VAL |
| 71 | stage5_seeded_random_combo | combination | n=642 PF=0.5208 net=-160992.88 | n=587 PF=0.6294 net=-96190.88 | not run | not run | REJECT_FIT_VAL |
| 72 | stage5_seeded_random_combo | combination | n=39 PF=0.7774 net=-4931.22 | n=35 PF=1.4978 net=5260.68 | not run | not run | REJECT_FIT_VAL |
| 73 | stage5_seeded_random_combo | combination | n=55 PF=0.4954 net=-11075.91 | n=80 PF=0.4318 net=-19196.42 | not run | not run | REJECT_FIT_VAL |
| 74 | stage5_seeded_random_combo | combination | n=388 PF=0.6705 net=-80413.26 | n=364 PF=0.8374 net=-29964.16 | not run | not run | REJECT_FIT_VAL |
| 75 | stage5_seeded_random_combo | combination | n=246 PF=0.4676 net=-57926.83 | n=219 PF=0.5573 net=-37948.99 | not run | not run | REJECT_FIT_VAL |

Full CSV: `C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\Train_and_Test\setup_recovery_full_loop\D_AVWAP_LOSE_REVERSAL\iteration_log.csv`
