# B_HUGE_C1_CLOSE_RECLAIM_BREAK (LONG) — PARAMETER_SWEEP_SUMMARY

_Generated 2026-07-03. Research-only; NO live trades; NO final_setup_conf.py edits._

Stage-3 one-knob-at-a-time sweeps from the baseline config, scored on FIT+VAL with the band objective (tent at PF 1.8, gap penalty 0.80). Baseline FIT/VAL band score is the reference; `improve` = higher score.

Total sweeps: **150** | improve: 56 | worse: 85

## exit

| knob | old | new | FIT n/PF | VAL n/PF | score | vs baseline |
|---|---|---|---|---|---|---|
| sl_pct | 1.0 | 0.85 | 416/0.458 | 336/0.452 | 0.4472 | improve |
| sl_pct | 1.0 | 0.7 | 416/0.446 | 339/0.473 | 0.4244 | improve |
| sl_pct | 1.0 | 1.2 | 412/0.534 | 327/0.471 | 0.4206 | improve |
| sl_pct | 1.0 | 1.1 | 415/0.523 | 325/0.462 | 0.4132 | improve |
| tgt_pct | 1.5 | 2.0 | 415/0.592 | 327/0.492 | 0.412 | improve |
| sl_pct | 1.0 | 1.5 | 408/0.547 | 321/0.469 | 0.4066 | worse |
| tgt_pct | 1.5 | 2.5 | 413/0.627 | 324/0.501 | 0.4002 | worse |
| tgt_pct | 1.5 | 1.25 | 416/0.483 | 328/0.437 | 0.4002 | worse |
| tgt_pct | 1.5 | 1.0 | 416/0.433 | 332/0.411 | 0.3934 | worse |
| sl_pct | 1.0 | 0.5 | 416/0.383 | 339/0.404 | 0.3662 | worse |
| tgt_pct | 1.5 | 0.8 | 416/0.389 | 332/0.358 | 0.3332 | worse |
| tgt_pct | 1.5 | 0.6 | 416/0.314 | 336/0.28 | 0.2528 | worse |

## filter

| knob | old | new | FIT n/PF | VAL n/PF | score | vs baseline |
|---|---|---|---|---|---|---|
| drop mask regime!=BULL | regime!=BULL | dropped | 837/0.443 | 566/0.412 | 0.3872 | worse |

## guard

| knob | old | new | FIT n/PF | VAL n/PF | score | vs baseline |
|---|---|---|---|---|---|---|
| top_n | - | 1 | 256/0.497 | 213/0.49 | 0.4844 | improve |
| max_positions | 20 | 5 | 233/0.483 | 159/0.481 | 0.4794 | improve |
| max_slot | - | 13:00 | 245/0.553 | 210/0.505 | 0.4666 | improve |
| daily_loss_rs | 0.0 | 4000.0 | 340/0.524 | 276/0.473 | 0.4322 | improve |
| min_slot | - | 12:00 | 289/0.559 | 256/0.484 | 0.424 | improve |
| top_n | - | 2 | 363/0.533 | 296/0.472 | 0.4232 | improve |
| top_n | - | 3 | 402/0.525 | 319/0.466 | 0.4188 | improve |
| min_slot | - | 11:00 | 415/0.514 | 327/0.458 | 0.4132 | improve |
| max_slot | - | 14:00 | 354/0.554 | 277/0.475 | 0.4118 | improve |
| max_slot | - | 12:00 | 143/0.525 | 96/0.461 | 0.4098 | improve |
| min_slot | - | 09:30 | 416/0.519 | 327/0.458 | 0.4092 | flat |
| min_slot | - | 10:00 | 416/0.519 | 327/0.458 | 0.4092 | flat |
| min_slot | - | 09:45 | 416/0.519 | 327/0.458 | 0.4092 | flat |
| min_slot | - | 10:30 | 416/0.519 | 327/0.458 | 0.4092 | flat |
| max_slot | - | 14:30 | 416/0.519 | 327/0.458 | 0.4092 | flat |
| max_positions | 20 | 10 | 344/0.519 | 248/0.458 | 0.4092 | flat |
| max_slot | - | 12:30 | 183/0.589 | 163/0.473 | 0.3802 | worse |
| daily_loss_rs | 0.0 | 2000.0 | 228/0.435 | 206/0.523 | 0.3646 | worse |
| max_slot | - | 11:30 | 74/0.353 | 34/0.427 | 0.2938 | worse |

## indicator/price-action

| knob | old | new | FIT n/PF | VAL n/PF | score | vs baseline |
|---|---|---|---|---|---|---|
| +mask atr_pct>= | - | 0.003142 (q0.5) | 243/0.565 | 162/0.578 | 0.5546 | improve |
| +mask wick_skew_pct>= | - | 0.022096 (q0.5) | 214/0.525 | 182/0.53 | 0.521 | improve |
| +mask quality_score>= | - | 41.402619 (q0.2) | 313/0.501 | 221/0.503 | 0.4994 | improve |
| +mask upper_wick_pct>= | - | 0.079827 (q0.5) | 230/0.543 | 187/0.517 | 0.4962 | improve |
| +mask body_pct>= | - | 0.607298 (q0.2) | 326/0.499 | 262/0.506 | 0.4934 | improve |
| +mask upper_wick_pct>= | - | 0.202498 (q0.8) | 107/0.575 | 74/0.527 | 0.4886 | improve |
| +mask vol_ratio>= | - | 3.564622 (q0.5) | 221/0.572 | 190/0.525 | 0.4874 | improve |
| +mask rs_pct>= | - | 0.047621 (q0.5) | 272/0.51 | 191/0.541 | 0.4852 | improve |
| +mask lower_wick_pct<= | - | 0.0 (q0.2) | 119/0.525 | 98/0.575 | 0.485 | improve |
| +mask close_loc>= | - | 0.731404 (q0.2) | 328/0.48 | 271/0.479 | 0.4782 | improve |
| +mask signal_range_pct>= | - | 0.407011 (q0.2) | 364/0.504 | 279/0.487 | 0.4734 | improve |
| +mask atr_pct>= | - | 0.002269 (q0.2) | 358/0.566 | 266/0.513 | 0.4706 | improve |
| +mask signal_range_pct>= | - | 0.68784 (q0.5) | 253/0.495 | 180/0.526 | 0.4702 | improve |
| +mask lower_wick_pct<= | - | 0.045548 (q0.5) | 181/0.499 | 162/0.481 | 0.4666 | improve |
| +mask rs_pct>= | - | -0.956105 (q0.2) | 371/0.521 | 283/0.488 | 0.4616 | improve |
| +mask vwap_dist_atr>= | - | 1.638384 (q0.2) | 351/0.534 | 266/0.489 | 0.453 | improve |
| +mask lower_wick_pct<= | - | 0.143311 (q0.8) | 318/0.512 | 267/0.475 | 0.4454 | improve |
| +mask upper_wick_pct>= | - | 0.004202 (q0.2) | 341/0.538 | 270/0.481 | 0.4354 | improve |
| +mask close_loc<= | - | 0.994484 (q0.8) | 341/0.538 | 270/0.481 | 0.4354 | improve |
| +mask wick_skew_pct<= | - | 0.145691 (q0.8) | 318/0.491 | 260/0.452 | 0.4208 | improve |
| +mask quality_score>= | - | 65.946217 (q0.5) | 221/0.511 | 135/0.626 | 0.419 | improve |
| +mask vwap_dist_atr>= | - | 3.389904 (q0.5) | 222/0.481 | 162/0.568 | 0.4114 | improve |
| +mask wick_skew_pct>= | - | -0.063872 (q0.2) | 323/0.499 | 270/0.45 | 0.4108 | improve |
| +mask wick_skew_pct<= | - | -0.063872 (q0.2) | 93/0.586 | 68/0.488 | 0.4096 | improve |
| +mask lower_wick_pct>= | - | 0.0 (q0.2) | 416/0.519 | 327/0.458 | 0.4092 | flat |
| +mask wick_skew_pct>= | - | 0.145691 (q0.8) | 98/0.598 | 71/0.478 | 0.382 | worse |
| +mask upper_wick_pct<= | - | 0.202498 (q0.8) | 309/0.496 | 260/0.429 | 0.3754 | worse |
| +mask body_pct>= | - | 0.746211 (q0.5) | 195/0.433 | 165/0.509 | 0.3722 | worse |
| +mask close_loc>= | - | 0.870305 (q0.5) | 206/0.435 | 160/0.523 | 0.3646 | worse |
| +mask vol_ratio>= | - | 2.247093 (q0.2) | 339/0.545 | 278/0.442 | 0.3596 | worse |
| +mask lower_wick_pct>= | - | 0.045548 (q0.5) | 235/0.534 | 177/0.434 | 0.354 | worse |
| +mask vwap_dist_atr<= | - | 5.204721 (q0.8) | 322/0.566 | 276/0.429 | 0.3194 | worse |
| +mask body_pct<= | - | 0.893412 (q0.8) | 345/0.563 | 266/0.426 | 0.3164 | worse |
| +mask vol_ratio<= | - | 3.564622 (q0.5) | 195/0.458 | 149/0.374 | 0.3068 | worse |
| +mask lower_wick_pct>= | - | 0.143311 (q0.8) | 98/0.538 | 72/0.4 | 0.2896 | worse |
| +mask upper_wick_pct<= | - | 0.079827 (q0.5) | 186/0.487 | 152/0.377 | 0.289 | worse |
| +mask vol_ratio<= | - | 2.247093 (q0.2) | 77/0.399 | 58/0.542 | 0.2846 | worse |
| +mask wick_skew_pct<= | - | 0.022096 (q0.5) | 202/0.511 | 157/0.376 | 0.268 | worse |
| +mask close_loc>= | - | 0.994484 (q0.8) | 75/0.432 | 65/0.338 | 0.2628 | worse |
| +mask upper_wick_pct<= | - | 0.004202 (q0.2) | 75/0.432 | 65/0.338 | 0.2628 | worse |
| +mask signal_range_pct>= | - | 1.161384 (q0.8) | 92/0.474 | 80/0.741 | 0.2604 | worse |
| +mask body_pct<= | - | 0.746211 (q0.5) | 221/0.605 | 174/0.411 | 0.2558 | worse |
| +mask vwap_dist_atr<= | - | 1.638384 (q0.2) | 65/0.434 | 73/0.331 | 0.2486 | worse |
| +mask vol_ratio>= | - | 5.74686 (q0.8) | 92/0.48 | 92/0.772 | 0.2464 | worse |
| +mask atr_pct<= | - | 0.003142 (q0.5) | 173/0.442 | 177/0.331 | 0.2422 | worse |
| +mask close_loc<= | - | 0.870305 (q0.5) | 210/0.612 | 179/0.402 | 0.234 | worse |
| +mask atr_pct<= | - | 0.004537 (q0.8) | 314/0.565 | 258/0.379 | 0.2302 | worse |
| +mask vol_ratio<= | - | 5.74686 (q0.8) | 324/0.531 | 245/0.363 | 0.2286 | worse |
| +mask signal_range_pct<= | - | 1.161384 (q0.8) | 324/0.535 | 251/0.361 | 0.2218 | worse |
| +mask atr_pct<= | - | 0.002269 (q0.2) | 58/0.224 | 73/0.219 | 0.215 | worse |
| +mask vwap_dist_atr>= | - | 5.204721 (q0.8) | 94/0.375 | 62/0.587 | 0.2054 | worse |
| +mask quality_score<= | - | 65.946217 (q0.5) | 195/0.529 | 204/0.346 | 0.1996 | worse |
| +mask signal_range_pct<= | - | 0.68784 (q0.5) | 163/0.567 | 159/0.362 | 0.198 | worse |
| +mask quality_score<= | - | 122.065599 (q0.8) | 293/0.563 | 277/0.359 | 0.1958 | worse |
| +mask quality_score<= | - | 41.402619 (q0.2) | 103/0.582 | 118/0.364 | 0.1896 | worse |
| +mask rs_pct<= | - | 0.047621 (q0.5) | 144/0.538 | 148/0.341 | 0.1834 | worse |
| +mask vwap_dist_atr<= | - | 3.389904 (q0.5) | 194/0.568 | 177/0.353 | 0.181 | worse |
| +mask atr_pct>= | - | 0.004537 (q0.8) | 102/0.412 | 71/0.736 | 0.1528 | worse |
| +mask rs_pct<= | - | 2.867723 (q0.8) | 285/0.583 | 262/0.344 | 0.1528 | worse |
| +mask rs_pct<= | - | -0.956105 (q0.2) | 45/0.503 | 56/0.307 | 0.1502 | worse |
| +mask body_pct>= | - | 0.893412 (q0.8) | 71/0.333 | 68/0.574 | 0.1402 | worse |
| +mask close_loc<= | - | 0.731404 (q0.2) | 88/0.686 | 67/0.373 | 0.1226 | worse |
| +mask body_pct<= | - | 0.607298 (q0.2) | 90/0.596 | 77/0.309 | 0.0794 | worse |
| +mask rs_pct>= | - | 2.867723 (q0.8) | 131/0.413 | 72/0.913 | 0.013 | worse |
| +mask signal_range_pct<= | - | 0.407011 (q0.2) | 52/0.672 | 59/0.294 | -0.0084 | worse |
| +mask quality_score>= | - | 122.065599 (q0.8) | 123/0.435 | 54/1.093 | -0.0914 | worse |

## pre-momentum

| knob | old | new | FIT n/PF | VAL n/PF | score | vs baseline |
|---|---|---|---|---|---|---|
| +premom pre3_range_r>= | - | 0.448233 (q0.5) | 223/0.547 | 139/0.542 | 0.538 | improve |
| +premom sig5_vol_ratio20>= | - | 3.496134 (q0.5) | 225/0.556 | 190/0.542 | 0.5308 | improve |
| +premom pre1_adx>= | - | 31.180706 (q0.2) | 350/0.528 | 277/0.527 | 0.5262 | improve |
| +premom pre5_mom_r>= | - | 0.047074 (q0.2) | 375/0.554 | 239/0.523 | 0.4982 | improve |
| +premom sig5_rsi_dir>= | - | 70.827164 (q0.5) | 240/0.535 | 174/0.511 | 0.4918 | improve |
| +premom sig5_adx_calc>= | - | 18.39577 (q0.2) | 349/0.48 | 278/0.475 | 0.471 | improve |
| +premom pre3_close_pos>= | - | 0.757576 (q0.5) | 212/0.511 | 153/0.486 | 0.466 | improve |
| +premom pre_entry_momentum_score>= | - | 67.057674 (q0.5) | 234/0.486 | 141/0.522 | 0.4572 | improve |
| +premom pre_entry_momentum_score>= | - | 50.998995 (q0.2) | 364/0.549 | 246/0.493 | 0.4482 | improve |
| +premom pre3_close_pos>= | - | 0.971534 (q0.8) | 91/0.483 | 60/0.53 | 0.4454 | improve |
| +premom sig5_rsi_dir>= | - | 63.975048 (q0.2) | 344/0.524 | 258/0.478 | 0.4412 | improve |
| +premom pre3_close_pos>= | - | 0.39351 (q0.2) | 354/0.555 | 262/0.483 | 0.4254 | improve |
| +premom pre3_close_pos<= | - | 0.757576 (q0.5) | 211/0.518 | 195/0.465 | 0.4226 | improve |
| +premom pre3_close_pos<= | - | 0.971534 (q0.8) | 331/0.526 | 276/0.467 | 0.4198 | improve |
| +premom pre1_adx>= | - | 40.376865 (q0.5) | 221/0.482 | 181/0.564 | 0.4164 | improve |
| +premom pre_entry_momentum_score<= | - | 78.656451 (q0.8) | 324/0.552 | 282/0.471 | 0.4062 | worse |
| +premom pre3_range_r>= | - | 0.228685 (q0.2) | 338/0.544 | 257/0.467 | 0.4054 | worse |
| +premom sig5_vol_ratio20>= | - | 2.227133 (q0.2) | 342/0.54 | 273/0.465 | 0.405 | worse |
| +premom sig5_rsi_dir<= | - | 70.827164 (q0.5) | 186/0.475 | 173/0.436 | 0.4048 | worse |
| +premom sig5_vol_ratio20<= | - | 2.227133 (q0.2) | 82/0.431 | 71/0.473 | 0.3974 | worse |
| +premom pre_entry_momentum_score>= | - | 78.656451 (q0.8) | 98/0.49 | 55/0.61 | 0.394 | worse |
| +premom pre5_mom_r>= | - | 0.434068 (q0.5) | 235/0.477 | 140/0.595 | 0.3826 | worse |
| +premom pre3_range_r<= | - | 0.228685 (q0.2) | 79/0.384 | 90/0.391 | 0.3784 | worse |
| +premom pre1_adx<= | - | 52.052438 (q0.8) | 334/0.606 | 266/0.467 | 0.3558 | worse |
| +premom sig5_rsi_dir<= | - | 63.975048 (q0.2) | 74/0.483 | 88/0.412 | 0.3552 | worse |
| +premom sig5_vol_ratio20<= | - | 3.496134 (q0.5) | 198/0.468 | 155/0.402 | 0.3492 | worse |
| +premom pre_entry_momentum_score<= | - | 67.057674 (q0.5) | 189/0.551 | 200/0.438 | 0.3476 | worse |
| +premom pre3_range_r<= | - | 0.842867 (q0.8) | 335/0.556 | 271/0.433 | 0.3346 | worse |
| +premom pre3_range_r<= | - | 0.448233 (q0.5) | 198/0.502 | 202/0.406 | 0.3292 | worse |
| +premom sig5_rsi_dir<= | - | 78.017801 (q0.8) | 315/0.59 | 275/0.436 | 0.3128 | worse |
| +premom sig5_adx_calc>= | - | 25.607559 (q0.5) | 225/0.397 | 192/0.516 | 0.3018 | worse |
| +premom sig5_adx_calc<= | - | 36.533118 (q0.8) | 316/0.575 | 267/0.419 | 0.2942 | worse |
| +premom pre3_range_r>= | - | 0.842867 (q0.8) | 90/0.449 | 67/0.68 | 0.2642 | worse |
| +premom sig5_vol_ratio20>= | - | 5.785476 (q0.8) | 91/0.489 | 89/0.777 | 0.2586 | worse |
| +premom sig5_vol_ratio20<= | - | 5.785476 (q0.8) | 329/0.535 | 252/0.379 | 0.2542 | worse |
| +premom pre5_mom_r<= | - | 0.84693 (q0.8) | 340/0.542 | 276/0.378 | 0.2468 | worse |
| +premom pre5_mom_r<= | - | 0.434068 (q0.5) | 186/0.566 | 200/0.384 | 0.2384 | worse |
| +premom pre1_adx<= | - | 40.376865 (q0.5) | 202/0.54 | 164/0.37 | 0.234 | worse |
| +premom pre_entry_momentum_score<= | - | 50.998995 (q0.2) | 55/0.318 | 102/0.44 | 0.2204 | worse |
| +premom sig5_adx_calc>= | - | 36.533118 (q0.8) | 103/0.404 | 74/0.66 | 0.1992 | worse |
| +premom sig5_rsi_dir>= | - | 78.017801 (q0.8) | 108/0.371 | 62/0.613 | 0.1774 | worse |
| +premom pre3_close_pos<= | - | 0.39351 (q0.2) | 65/0.313 | 89/0.491 | 0.1706 | worse |
| +premom pre1_adx>= | - | 52.052438 (q0.8) | 88/0.301 | 75/0.506 | 0.137 | worse |
| +premom pre5_mom_r<= | - | 0.047074 (q0.2) | 43/0.223 | 107/0.332 | 0.1358 | worse |
| +premom sig5_adx_calc<= | - | 25.607559 (q0.5) | 193/0.712 | 151/0.385 | 0.1234 | worse |
| +premom pre5_mom_r>= | - | 0.84693 (q0.8) | 81/0.49 | 56/1.025 | 0.062 | worse |
| +premom sig5_adx_calc<= | - | 18.39577 (q0.2) | 68/0.754 | 56/0.362 | 0.0484 | worse |
| +premom pre1_adx<= | - | 31.180706 (q0.2) | 69/0.449 | 65/0.183 | -0.0298 | worse |

## regime

| knob | old | new | FIT n/PF | VAL n/PF | score | vs baseline |
|---|---|---|---|---|---|---|
| +mask regime==NEUTRAL | - | NEUTRAL | 295/0.489 | 327/0.458 | 0.4332 | improve |
| +mask regime!=TREND | - | TREND | 295/0.489 | 327/0.458 | 0.4332 | improve |
| +mask regime!=BEAR | - | BEAR | 416/0.519 | 327/0.458 | 0.4092 | flat |
| +mask regime!=BULL | - | BULL | 416/0.519 | 327/0.458 | 0.4092 | flat |

## Best stable knobs (score-improving, FIT and VAL both alive)

- **indicator/price-action / +mask atr_pct>=** -> 0.003142 (q0.5) (FIT 243/0.565, VAL 162/0.578, score 0.5546)
- **pre-momentum / +premom pre3_range_r>=** -> 0.448233 (q0.5) (FIT 223/0.547, VAL 139/0.542, score 0.538)
- **pre-momentum / +premom sig5_vol_ratio20>=** -> 3.496134 (q0.5) (FIT 225/0.556, VAL 190/0.542, score 0.5308)
- **pre-momentum / +premom pre1_adx>=** -> 31.180706 (q0.2) (FIT 350/0.528, VAL 277/0.527, score 0.5262)
- **indicator/price-action / +mask wick_skew_pct>=** -> 0.022096 (q0.5) (FIT 214/0.525, VAL 182/0.53, score 0.521)
- **indicator/price-action / +mask quality_score>=** -> 41.402619 (q0.2) (FIT 313/0.501, VAL 221/0.503, score 0.4994)
- **pre-momentum / +premom pre5_mom_r>=** -> 0.047074 (q0.2) (FIT 375/0.554, VAL 239/0.523, score 0.4982)
- **indicator/price-action / +mask upper_wick_pct>=** -> 0.079827 (q0.5) (FIT 230/0.543, VAL 187/0.517, score 0.4962)
- **indicator/price-action / +mask body_pct>=** -> 0.607298 (q0.2) (FIT 326/0.499, VAL 262/0.506, score 0.4934)
- **pre-momentum / +premom sig5_rsi_dir>=** -> 70.827164 (q0.5) (FIT 240/0.535, VAL 174/0.511, score 0.4918)
- **indicator/price-action / +mask upper_wick_pct>=** -> 0.202498 (q0.8) (FIT 107/0.575, VAL 74/0.527, score 0.4886)
- **indicator/price-action / +mask vol_ratio>=** -> 3.564622 (q0.5) (FIT 221/0.572, VAL 190/0.525, score 0.4874)
- **indicator/price-action / +mask rs_pct>=** -> 0.047621 (q0.5) (FIT 272/0.51, VAL 191/0.541, score 0.4852)
- **indicator/price-action / +mask lower_wick_pct<=** -> 0.0 (q0.2) (FIT 119/0.525, VAL 98/0.575, score 0.485)
- **guard / top_n** -> 1 (FIT 256/0.497, VAL 213/0.49, score 0.4844)

## Overfit-risk notes

- Any knob whose FIT PF explodes while VAL PF collapses is a knife-edge; the band objective already penalises the gap, and stage-5 adds neighborhood + dropout checks.
- Sweeps that push PF far above 1.80 are treated as overshoot, not success.