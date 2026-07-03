# B_HUGE_GREEN_PULLBACK_HOLD_THEN_BREAK (LONG) — PARAMETER_SWEEP_SUMMARY

_Generated 2026-07-03. Research-only; NO live trades; NO final_setup_conf.py edits._

Stage-3 one-knob-at-a-time sweeps from the baseline config, scored on FIT+VAL with the band objective (tent at PF 1.8, gap penalty 0.80). Baseline FIT/VAL band score is the reference; `improve` = higher score.

Total sweeps: **149** | improve: 69 | worse: 72

## exit

| knob | old | new | FIT n/PF | VAL n/PF | score | vs baseline |
|---|---|---|---|---|---|---|
| tgt_pct | 1.25 | 2.5 | 348/0.654 | 201/0.488 | 0.3552 | improve |
| sl_pct | 0.7 | 0.85 | 349/0.542 | 201/0.432 | 0.344 | improve |
| sl_pct | 0.7 | 1.5 | 340/0.657 | 189/0.475 | 0.3294 | improve |
| tgt_pct | 1.25 | 2.0 | 349/0.613 | 201/0.444 | 0.3088 | worse |
| sl_pct | 0.7 | 1.1 | 347/0.651 | 195/0.459 | 0.3054 | worse |
| tgt_pct | 1.25 | 1.5 | 349/0.559 | 201/0.404 | 0.28 | worse |
| sl_pct | 0.7 | 1.2 | 346/0.684 | 194/0.448 | 0.2592 | worse |
| sl_pct | 0.7 | 1.0 | 349/0.638 | 198/0.414 | 0.2348 | worse |
| tgt_pct | 1.25 | 1.0 | 350/0.53 | 201/0.363 | 0.2294 | worse |
| tgt_pct | 1.25 | 0.8 | 351/0.425 | 201/0.303 | 0.2054 | worse |
| sl_pct | 0.7 | 0.5 | 353/0.446 | 201/0.293 | 0.1706 | worse |
| tgt_pct | 1.25 | 0.6 | 351/0.328 | 201/0.229 | 0.1498 | worse |

## guard

| knob | old | new | FIT n/PF | VAL n/PF | score | vs baseline |
|---|---|---|---|---|---|---|
| daily_loss_rs | 0.0 | 2000.0 | 219/0.565 | 92/0.529 | 0.5002 | improve |
| max_slot | - | 11:30 | 32/0.772 | 35/0.59 | 0.4444 | improve |
| daily_loss_rs | 0.0 | 4000.0 | 249/0.536 | 119/0.465 | 0.4082 | improve |
| top_n | - | 1 | 229/0.489 | 131/0.443 | 0.4062 | improve |
| max_positions | 20 | 5 | 239/0.559 | 145/0.452 | 0.3664 | improve |
| max_slot | - | 13:00 | 172/0.57 | 118/0.449 | 0.3522 | improve |
| top_n | - | 3 | 331/0.534 | 196/0.426 | 0.3396 | improve |
| max_slot | - | 12:30 | 131/0.557 | 106/0.433 | 0.3338 | improve |
| max_slot | - | 12:00 | 73/0.572 | 75/0.436 | 0.3272 | improve |
| max_slot | - | 14:00 | 274/0.573 | 179/0.435 | 0.3246 | improve |
| max_positions | 20 | 10 | 318/0.532 | 193/0.412 | 0.316 | improve |
| min_slot | - | 11:00 | 348/0.546 | 200/0.418 | 0.3156 | improve |
| min_slot | - | 10:00 | 349/0.544 | 201/0.415 | 0.3118 | flat |
| min_slot | - | 09:30 | 349/0.544 | 201/0.415 | 0.3118 | flat |
| min_slot | - | 09:45 | 349/0.544 | 201/0.415 | 0.3118 | flat |
| max_slot | - | 14:30 | 349/0.544 | 201/0.415 | 0.3118 | flat |
| min_slot | - | 10:30 | 349/0.544 | 201/0.415 | 0.3118 | flat |
| top_n | - | 2 | 303/0.565 | 182/0.414 | 0.2932 | worse |
| min_slot | - | 12:00 | 282/0.536 | 139/0.383 | 0.2606 | worse |

## indicator/price-action

| knob | old | new | FIT n/PF | VAL n/PF | score | vs baseline |
|---|---|---|---|---|---|---|
| +mask quality_score<= | - | 29.864302 (q0.2) | 67/0.498 | 44/0.502 | 0.4948 | improve |
| +mask vwap_dist_atr>= | - | 4.48344 (q0.8) | 75/0.543 | 33/0.606 | 0.4926 | improve |
| +mask close_loc>= | - | 0.96775 (q0.8) | 78/0.482 | 32/0.482 | 0.482 | improve |
| +mask signal_range_pct<= | - | 0.745187 (q0.5) | 185/0.5 | 94/0.487 | 0.4766 | improve |
| +mask lower_wick_pct<= | - | 0.152001 (q0.8) | 279/0.559 | 161/0.503 | 0.4582 | improve |
| +mask close_loc>= | - | 0.714289 (q0.2) | 286/0.457 | 155/0.454 | 0.4516 | improve |
| +mask quality_score>= | - | 131.577221 (q0.8) | 74/0.609 | 37/0.521 | 0.4506 | improve |
| +mask wick_skew_pct<= | - | -0.046198 (q0.2) | 78/0.471 | 32/0.5 | 0.4478 | improve |
| +mask upper_wick_pct<= | - | 0.018387 (q0.2) | 81/0.459 | 29/0.451 | 0.4446 | improve |
| +mask vol_ratio<= | - | 3.404217 (q0.5) | 176/0.551 | 100/0.485 | 0.4322 | improve |
| +mask body_pct>= | - | 0.597709 (q0.2) | 277/0.504 | 162/0.461 | 0.4266 | improve |
| +mask body_pct>= | - | 0.71576 (q0.5) | 180/0.467 | 96/0.521 | 0.4238 | improve |
| +mask upper_wick_pct<= | - | 0.221804 (q0.8) | 288/0.513 | 154/0.456 | 0.4104 | improve |
| +mask vol_ratio<= | - | 2.169739 (q0.2) | 78/0.461 | 36/0.556 | 0.385 | improve |
| +mask atr_pct<= | - | 0.003409 (q0.5) | 180/0.419 | 101/0.485 | 0.3662 | improve |
| +mask rs_pct<= | - | 1.819572 (q0.5) | 173/0.542 | 108/0.442 | 0.362 | improve |
| +mask lower_wick_pct>= | - | 0.045193 (q0.5) | 177/0.523 | 100/0.433 | 0.361 | improve |
| +mask rs_pct<= | - | 4.086475 (q0.8) | 282/0.502 | 163/0.422 | 0.358 | improve |
| +mask quality_score>= | - | 74.793058 (q0.5) | 174/0.54 | 99/0.438 | 0.3564 | improve |
| +mask rs_pct<= | - | -0.239824 (q0.2) | 71/0.447 | 42/0.393 | 0.3498 | improve |
| +mask close_loc>= | - | 0.833333 (q0.5) | 184/0.439 | 93/0.558 | 0.3438 | improve |
| +mask body_pct>= | - | 0.873237 (q0.8) | 79/0.456 | 34/0.603 | 0.3384 | improve |
| +mask atr_pct<= | - | 0.004892 (q0.8) | 286/0.574 | 153/0.442 | 0.3364 | improve |
| +mask wick_skew_pct<= | - | 0.143623 (q0.8) | 293/0.519 | 150/0.417 | 0.3354 | improve |
| +mask signal_range_pct<= | - | 1.131096 (q0.8) | 290/0.554 | 148/0.428 | 0.3272 | improve |
| +mask wick_skew_pct<= | - | 0.050171 (q0.5) | 188/0.404 | 89/0.504 | 0.324 | improve |
| +mask vol_ratio<= | - | 5.69666 (q0.8) | 286/0.539 | 154/0.417 | 0.3194 | improve |
| +mask vwap_dist_atr<= | - | 3.090952 (q0.5) | 182/0.5 | 99/0.399 | 0.3182 | improve |
| +mask vwap_dist_atr>= | - | 1.301105 (q0.2) | 271/0.553 | 170/0.421 | 0.3154 | improve |
| +mask lower_wick_pct>= | - | 0.0 (q0.2) | 349/0.544 | 201/0.415 | 0.3118 | flat |
| +mask upper_wick_pct<= | - | 0.1065 (q0.5) | 190/0.418 | 91/0.559 | 0.3052 | worse |
| +mask vwap_dist_atr>= | - | 3.090952 (q0.5) | 170/0.589 | 102/0.43 | 0.3028 | worse |
| +mask signal_range_pct>= | - | 1.131096 (q0.8) | 60/0.486 | 53/0.382 | 0.2988 | worse |
| +mask rs_pct>= | - | -0.239824 (q0.2) | 278/0.572 | 159/0.42 | 0.2984 | worse |
| +mask vol_ratio>= | - | 5.69666 (q0.8) | 64/0.549 | 47/0.407 | 0.2934 | worse |
| +mask vwap_dist_atr<= | - | 1.301105 (q0.2) | 83/0.505 | 31/0.384 | 0.2872 | worse |
| +mask quality_score<= | - | 131.577221 (q0.8) | 280/0.523 | 164/0.39 | 0.2836 | worse |
| +mask upper_wick_pct>= | - | 0.018387 (q0.2) | 271/0.567 | 172/0.409 | 0.2826 | worse |
| +mask close_loc<= | - | 0.96775 (q0.8) | 274/0.559 | 169/0.402 | 0.2764 | worse |
| +mask atr_pct<= | - | 0.002421 (q0.2) | 75/0.373 | 37/0.494 | 0.2762 | worse |
| +mask atr_pct>= | - | 0.004892 (q0.8) | 64/0.423 | 48/0.34 | 0.2736 | worse |
| +mask quality_score<= | - | 74.793058 (q0.5) | 176/0.543 | 102/0.39 | 0.2676 | worse |
| +mask wick_skew_pct>= | - | -0.046198 (q0.2) | 273/0.564 | 169/0.399 | 0.267 | worse |
| +mask quality_score>= | - | 29.864302 (q0.2) | 282/0.554 | 157/0.394 | 0.266 | worse |
| +mask lower_wick_pct<= | - | 0.045193 (q0.5) | 177/0.561 | 101/0.397 | 0.2658 | worse |
| +mask rs_pct>= | - | 1.819572 (q0.5) | 177/0.54 | 93/0.387 | 0.2646 | worse |
| +mask vwap_dist_atr<= | - | 4.48344 (q0.8) | 275/0.541 | 168/0.381 | 0.253 | worse |
| +mask atr_pct>= | - | 0.002421 (q0.2) | 279/0.584 | 164/0.4 | 0.2528 | worse |
| +mask vol_ratio>= | - | 2.169739 (q0.2) | 276/0.562 | 165/0.386 | 0.2452 | worse |
| +mask signal_range_pct>= | - | 0.452932 (q0.2) | 277/0.592 | 161/0.394 | 0.2356 | worse |
| +mask body_pct<= | - | 0.873237 (q0.8) | 275/0.566 | 167/0.382 | 0.2348 | worse |
| +mask signal_range_pct<= | - | 0.452932 (q0.2) | 77/0.355 | 40/0.516 | 0.2262 | worse |
| +mask vol_ratio>= | - | 3.404217 (q0.5) | 178/0.532 | 101/0.355 | 0.2134 | worse |
| +mask wick_skew_pct>= | - | 0.143623 (q0.8) | 61/0.653 | 51/0.407 | 0.2102 | worse |
| +mask signal_range_pct>= | - | 0.745187 (q0.5) | 169/0.584 | 107/0.363 | 0.1862 | worse |
| +mask rs_pct>= | - | 4.086475 (q0.8) | 72/0.707 | 38/0.387 | 0.131 | worse |
| +mask atr_pct>= | - | 0.003409 (q0.5) | 174/0.667 | 100/0.357 | 0.109 | worse |
| +mask body_pct<= | - | 0.71576 (q0.5) | 174/0.624 | 105/0.327 | 0.0894 | worse |
| +mask wick_skew_pct>= | - | 0.050171 (q0.5) | 166/0.725 | 112/0.353 | 0.0554 | worse |
| +mask close_loc<= | - | 0.833333 (q0.5) | 170/0.672 | 108/0.309 | 0.0186 | worse |
| +mask upper_wick_pct>= | - | 0.1065 (q0.5) | 164/0.702 | 110/0.319 | 0.0126 | worse |
| +mask upper_wick_pct>= | - | 0.221804 (q0.8) | 66/0.664 | 47/0.301 | 0.0106 | worse |
| +mask lower_wick_pct<= | - | 0.0 (q0.2) | 112/0.673 | 59/0.263 | -0.065 | worse |
| +mask body_pct<= | - | 0.597709 (q0.2) | 76/0.683 | 39/0.257 | -0.0838 | worse |
| +mask lower_wick_pct>= | - | 0.152001 (q0.8) | 72/0.48 | 40/0.158 | -0.0996 | worse |
| +mask close_loc<= | - | 0.714289 (q0.2) | 68/1.04 | 46/0.296 | -0.2992 | worse |

## pre-momentum

| knob | old | new | FIT n/PF | VAL n/PF | score | vs baseline |
|---|---|---|---|---|---|---|
| +premom sig5_adx_calc<= | - | 19.277018 (q0.2) | 71/0.646 | 42/0.62 | 0.5992 | improve |
| +premom sig5_adx_calc<= | - | 26.805832 (q0.5) | 183/0.607 | 97/0.594 | 0.5836 | improve |
| +premom sig5_rsi_dir<= | - | 69.352257 (q0.5) | 192/0.526 | 96/0.512 | 0.5008 | improve |
| +premom pre1_adx<= | - | 40.17202 (q0.5) | 188/0.571 | 101/0.492 | 0.4288 | improve |
| +premom pre3_close_pos>= | - | 0.780858 (q0.5) | 186/0.471 | 94/0.537 | 0.4182 | improve |
| +premom pre5_mom_r>= | - | 0.931647 (q0.8) | 124/0.509 | 67/0.456 | 0.4136 | improve |
| +premom pre_entry_momentum_score>= | - | 71.286952 (q0.5) | 211/0.472 | 105/0.438 | 0.4108 | improve |
| +premom pre3_close_pos>= | - | 0.541682 (q0.2) | 287/0.496 | 159/0.446 | 0.406 | improve |
| +premom pre3_range_r<= | - | 0.921729 (q0.8) | 255/0.576 | 132/0.479 | 0.4014 | improve |
| +premom pre5_mom_r<= | - | 0.273359 (q0.2) | 42/0.402 | 46/0.404 | 0.4004 | improve |
| +premom sig5_adx_calc<= | - | 38.519627 (q0.8) | 290/0.525 | 157/0.455 | 0.399 | improve |
| +premom sig5_vol_ratio20<= | - | 3.404217 (q0.5) | 185/0.551 | 104/0.466 | 0.398 | improve |
| +premom sig5_vol_ratio20<= | - | 2.169739 (q0.2) | 82/0.455 | 37/0.533 | 0.3926 | improve |
| +premom sig5_vol_ratio20>= | - | 5.69666 (q0.8) | 65/0.577 | 50/0.47 | 0.3844 | improve |
| +premom sig5_rsi_dir<= | - | 62.814507 (q0.2) | 79/0.496 | 38/0.433 | 0.3826 | improve |
| +premom pre1_adx<= | - | 51.258694 (q0.8) | 290/0.546 | 161/0.453 | 0.3786 | improve |
| +premom pre3_range_r>= | - | 0.276081 (q0.2) | 310/0.567 | 178/0.46 | 0.3744 | improve |
| +premom sig5_rsi_dir<= | - | 76.045624 (q0.8) | 291/0.546 | 159/0.444 | 0.3624 | improve |
| +premom pre5_mom_r>= | - | 0.548493 (q0.5) | 239/0.54 | 116/0.44 | 0.36 | improve |
| +premom pre_entry_momentum_score>= | - | 59.630823 (q0.2) | 308/0.538 | 162/0.432 | 0.3472 | improve |
| +premom pre1_adx>= | - | 31.595497 (q0.2) | 286/0.554 | 159/0.439 | 0.347 | improve |
| +premom pre_entry_momentum_score<= | - | 80.237597 (q0.8) | 265/0.574 | 144/0.437 | 0.3274 | improve |
| +premom pre3_close_pos>= | - | 0.958325 (q0.8) | 80/0.519 | 35/0.764 | 0.323 | improve |
| +premom pre3_range_r<= | - | 0.508816 (q0.5) | 143/0.488 | 77/0.394 | 0.3188 | improve |
| +premom sig5_vol_ratio20<= | - | 5.69666 (q0.8) | 292/0.54 | 160/0.414 | 0.3132 | improve |
| +premom pre_entry_momentum_score>= | - | 80.237597 (q0.8) | 102/0.586 | 62/0.432 | 0.3088 | worse |
| +premom pre5_mom_r>= | - | 0.273359 (q0.2) | 311/0.569 | 158/0.42 | 0.3008 | worse |
| +premom sig5_rsi_dir>= | - | 62.814507 (q0.2) | 277/0.559 | 166/0.413 | 0.2962 | worse |
| +premom pre3_range_r>= | - | 0.508816 (q0.5) | 224/0.584 | 126/0.424 | 0.296 | worse |
| +premom sig5_adx_calc>= | - | 19.277018 (q0.2) | 284/0.52 | 162/0.377 | 0.2626 | worse |
| +premom sig5_vol_ratio20>= | - | 2.169739 (q0.2) | 276/0.562 | 165/0.386 | 0.2452 | worse |
| +premom pre5_mom_r<= | - | 0.931647 (q0.8) | 240/0.597 | 140/0.401 | 0.2442 | worse |
| +premom pre1_adx<= | - | 31.595497 (q0.2) | 72/0.483 | 45/0.349 | 0.2418 | worse |
| +premom sig5_vol_ratio20>= | - | 3.404217 (q0.5) | 179/0.542 | 102/0.37 | 0.2324 | worse |
| +premom pre3_close_pos<= | - | 0.958325 (q0.8) | 276/0.573 | 170/0.383 | 0.231 | worse |
| +premom pre5_mom_r<= | - | 0.548493 (q0.5) | 121/0.579 | 90/0.379 | 0.219 | worse |
| +premom pre3_range_r>= | - | 0.921729 (q0.8) | 107/0.487 | 74/0.338 | 0.2188 | worse |
| +premom pre1_adx>= | - | 40.17202 (q0.5) | 175/0.556 | 102/0.358 | 0.1996 | worse |
| +premom sig5_rsi_dir>= | - | 69.352257 (q0.5) | 167/0.554 | 108/0.343 | 0.1742 | worse |
| +premom sig5_adx_calc>= | - | 26.805832 (q0.5) | 173/0.499 | 107/0.317 | 0.1714 | worse |
| +premom pre_entry_momentum_score<= | - | 71.286952 (q0.5) | 156/0.7 | 99/0.403 | 0.1654 | worse |
| +premom pre1_adx>= | - | 51.258694 (q0.8) | 68/0.656 | 45/0.383 | 0.1646 | worse |
| +premom sig5_rsi_dir>= | - | 76.045624 (q0.8) | 62/0.613 | 43/0.354 | 0.1468 | worse |
| +premom pre_entry_momentum_score<= | - | 59.630823 (q0.2) | 52/0.633 | 42/0.358 | 0.138 | worse |
| +premom pre3_close_pos<= | - | 0.780858 (q0.5) | 179/0.647 | 111/0.361 | 0.1322 | worse |
| +premom sig5_adx_calc>= | - | 38.519627 (q0.8) | 66/0.681 | 46/0.359 | 0.1014 | worse |
| +premom pre3_close_pos<= | - | 0.541682 (q0.2) | 73/0.818 | 43/0.348 | -0.028 | worse |
| +premom pre3_range_r<= | - | 0.276081 (q0.2) | 46/0.493 | 23/0.121 | -0.1766 | worse |

## regime

| knob | old | new | FIT n/PF | VAL n/PF | score | vs baseline |
|---|---|---|---|---|---|---|
| +mask regime!=BULL | - | BULL | 349/0.544 | 201/0.415 | 0.3118 | flat |
| +mask regime!=TREND | - | TREND | 349/0.544 | 201/0.415 | 0.3118 | flat |
| +mask regime==NEUTRAL | - | NEUTRAL | 13/0.241 | 4/0.479 | -4.6 | worse |
| +mask regime!=BEAR | - | BEAR | 13/0.241 | 4/0.479 | -4.6 | worse |

## Best stable knobs (score-improving, FIT and VAL both alive)

- **pre-momentum / +premom sig5_adx_calc<=** -> 19.277018 (q0.2) (FIT 71/0.646, VAL 42/0.62, score 0.5992)
- **pre-momentum / +premom sig5_adx_calc<=** -> 26.805832 (q0.5) (FIT 183/0.607, VAL 97/0.594, score 0.5836)
- **pre-momentum / +premom sig5_rsi_dir<=** -> 69.352257 (q0.5) (FIT 192/0.526, VAL 96/0.512, score 0.5008)
- **guard / daily_loss_rs** -> 2000.0 (FIT 219/0.565, VAL 92/0.529, score 0.5002)
- **indicator/price-action / +mask quality_score<=** -> 29.864302 (q0.2) (FIT 67/0.498, VAL 44/0.502, score 0.4948)
- **indicator/price-action / +mask vwap_dist_atr>=** -> 4.48344 (q0.8) (FIT 75/0.543, VAL 33/0.606, score 0.4926)
- **indicator/price-action / +mask close_loc>=** -> 0.96775 (q0.8) (FIT 78/0.482, VAL 32/0.482, score 0.482)
- **indicator/price-action / +mask signal_range_pct<=** -> 0.745187 (q0.5) (FIT 185/0.5, VAL 94/0.487, score 0.4766)
- **indicator/price-action / +mask lower_wick_pct<=** -> 0.152001 (q0.8) (FIT 279/0.559, VAL 161/0.503, score 0.4582)
- **indicator/price-action / +mask close_loc>=** -> 0.714289 (q0.2) (FIT 286/0.457, VAL 155/0.454, score 0.4516)
- **indicator/price-action / +mask quality_score>=** -> 131.577221 (q0.8) (FIT 74/0.609, VAL 37/0.521, score 0.4506)
- **indicator/price-action / +mask wick_skew_pct<=** -> -0.046198 (q0.2) (FIT 78/0.471, VAL 32/0.5, score 0.4478)
- **indicator/price-action / +mask upper_wick_pct<=** -> 0.018387 (q0.2) (FIT 81/0.459, VAL 29/0.451, score 0.4446)
- **guard / max_slot** -> 11:30 (FIT 32/0.772, VAL 35/0.59, score 0.4444)
- **indicator/price-action / +mask vol_ratio<=** -> 3.404217 (q0.5) (FIT 176/0.551, VAL 100/0.485, score 0.4322)

## Overfit-risk notes

- Any knob whose FIT PF explodes while VAL PF collapses is a knife-edge; the band objective already penalises the gap, and stage-5 adds neighborhood + dropout checks.
- Sweeps that push PF far above 1.80 are treated as overshoot, not success.