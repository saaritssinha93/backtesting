# B_HUGE_FAILED_BOUNCE (SHORT) — PARAMETER_SWEEP_SUMMARY

_Generated 2026-07-03. Research-only; NO live trades; NO final_setup_conf.py edits._

Stage-3 one-knob-at-a-time sweeps from the baseline config, scored on FIT+VAL with the band objective (tent at PF 1.8, gap penalty 0.80). Baseline FIT/VAL band score is the reference; `improve` = higher score.

Total sweeps: **149** | improve: 59 | worse: 82

## exit

| knob | old | new | FIT n/PF | VAL n/PF | score | vs baseline |
|---|---|---|---|---|---|---|
| sl_pct | 0.7 | 1.0 | 886/0.414 | 612/0.487 | 0.3556 | improve |
| sl_pct | 0.7 | 0.85 | 917/0.389 | 626/0.446 | 0.3434 | improve |
| sl_pct | 0.7 | 1.1 | 864/0.418 | 598/0.518 | 0.338 | improve |
| sl_pct | 0.7 | 1.5 | 814/0.433 | 571/0.566 | 0.3266 | improve |
| tgt_pct | 1.25 | 1.5 | 955/0.35 | 642/0.411 | 0.3012 | improve |
| sl_pct | 0.7 | 1.2 | 853/0.409 | 591/0.548 | 0.2978 | improve |
| tgt_pct | 1.25 | 1.0 | 989/0.347 | 673/0.41 | 0.2966 | improve |
| tgt_pct | 1.25 | 0.8 | 1010/0.314 | 683/0.369 | 0.27 | worse |
| sl_pct | 0.7 | 0.5 | 1030/0.288 | 696/0.343 | 0.244 | worse |
| tgt_pct | 1.25 | 0.6 | 1034/0.265 | 706/0.298 | 0.2386 | worse |
| tgt_pct | 1.25 | 2.5 | 930/0.318 | 626/0.419 | 0.2372 | worse |
| tgt_pct | 1.25 | 2.0 | 935/0.309 | 635/0.408 | 0.2298 | worse |

## guard

| knob | old | new | FIT n/PF | VAL n/PF | score | vs baseline |
|---|---|---|---|---|---|---|
| max_positions | 20 | 5 | 413/0.365 | 280/0.378 | 0.3546 | improve |
| top_n | - | 3 | 937/0.342 | 624/0.409 | 0.2884 | improve |
| max_slot | - | 14:00 | 828/0.352 | 540/0.437 | 0.284 | improve |
| min_slot | - | 10:00 | 972/0.344 | 656/0.422 | 0.2816 | flat |
| min_slot | - | 09:45 | 972/0.344 | 656/0.422 | 0.2816 | flat |
| min_slot | - | 09:30 | 972/0.344 | 656/0.422 | 0.2816 | flat |
| min_slot | - | 10:30 | 972/0.344 | 656/0.422 | 0.2816 | flat |
| max_slot | - | 14:30 | 972/0.344 | 656/0.422 | 0.2816 | flat |
| top_n | - | 2 | 871/0.338 | 579/0.41 | 0.2804 | worse |
| min_slot | - | 12:00 | 807/0.344 | 564/0.425 | 0.2792 | worse |
| min_slot | - | 11:00 | 970/0.343 | 655/0.423 | 0.279 | worse |
| max_slot | - | 11:30 | 110/0.352 | 66/0.478 | 0.2512 | worse |
| max_slot | - | 12:30 | 375/0.34 | 237/0.458 | 0.2456 | worse |
| top_n | - | 1 | 650/0.33 | 448/0.437 | 0.2444 | worse |
| max_positions | 20 | 10 | 707/0.333 | 464/0.454 | 0.2362 | worse |
| max_slot | - | 12:00 | 235/0.37 | 136/0.539 | 0.2348 | worse |
| daily_loss_rs | 0.0 | 4000.0 | 562/0.332 | 415/0.454 | 0.2344 | worse |
| daily_loss_rs | 0.0 | 2000.0 | 468/0.337 | 303/0.468 | 0.2322 | worse |
| max_slot | - | 13:00 | 514/0.31 | 351/0.478 | 0.1756 | worse |

## indicator/price-action

| knob | old | new | FIT n/PF | VAL n/PF | score | vs baseline |
|---|---|---|---|---|---|---|
| +mask lower_wick_pct>= | - | 0.156235 (q0.8) | 210/0.39 | 153/0.404 | 0.3788 | improve |
| +mask upper_wick_pct<= | - | 0.0 (q0.2) | 362/0.37 | 225/0.389 | 0.3548 | improve |
| +mask upper_wick_pct<= | - | 0.033413 (q0.5) | 536/0.368 | 342/0.359 | 0.3518 | improve |
| +mask wick_skew_pct<= | - | -0.017923 (q0.5) | 536/0.376 | 352/0.41 | 0.3488 | improve |
| +mask close_loc>= | - | 0.264435 (q0.8) | 214/0.341 | 157/0.347 | 0.3362 | improve |
| +mask body_pct<= | - | 0.605268 (q0.2) | 222/0.393 | 146/0.465 | 0.3354 | improve |
| +mask rs_pct>= | - | 0.579646 (q0.8) | 275/0.396 | 103/0.475 | 0.3328 | improve |
| +mask quality_score<= | - | 100.236424 (q0.8) | 809/0.36 | 521/0.405 | 0.324 | improve |
| +mask vwap_dist_atr>= | - | -4.175895 (q0.5) | 581/0.35 | 358/0.384 | 0.3228 | improve |
| +mask vwap_dist_atr>= | - | -6.220735 (q0.2) | 828/0.364 | 568/0.417 | 0.3216 | improve |
| +mask atr_pct>= | - | 0.002761 (q0.5) | 558/0.389 | 366/0.476 | 0.3194 | improve |
| +mask rs_pct>= | - | -0.975455 (q0.5) | 599/0.353 | 315/0.397 | 0.3178 | improve |
| +mask signal_range_pct>= | - | 0.859773 (q0.8) | 206/0.4 | 159/0.503 | 0.3176 | improve |
| +mask wick_skew_pct>= | - | 0.059542 (q0.8) | 226/0.38 | 149/0.459 | 0.3168 | improve |
| +mask atr_pct>= | - | 0.002 (q0.2) | 849/0.369 | 572/0.439 | 0.313 | improve |
| +mask rs_pct>= | - | -2.553996 (q0.2) | 829/0.352 | 527/0.403 | 0.3112 | improve |
| +mask lower_wick_pct<= | - | 0.0 (q0.2) | 237/0.341 | 180/0.38 | 0.3098 | improve |
| +mask close_loc<= | - | 0.0 (q0.2) | 237/0.341 | 180/0.38 | 0.3098 | improve |
| +mask lower_wick_pct>= | - | 0.063864 (q0.5) | 532/0.355 | 364/0.413 | 0.3086 | improve |
| +mask vol_ratio<= | - | 4.705811 (q0.8) | 808/0.354 | 557/0.42 | 0.3012 | improve |
| +mask quality_score<= | - | 60.208617 (q0.5) | 563/0.338 | 327/0.385 | 0.3004 | improve |
| +mask atr_pct>= | - | 0.004081 (q0.8) | 209/0.353 | 159/0.419 | 0.3002 | improve |
| +mask signal_range_pct>= | - | 0.321717 (q0.2) | 829/0.359 | 571/0.434 | 0.299 | improve |
| +mask body_pct>= | - | 0.893233 (q0.8) | 209/0.37 | 160/0.459 | 0.2988 | improve |
| +mask close_loc>= | - | 0.130038 (q0.5) | 541/0.332 | 356/0.375 | 0.2976 | improve |
| +mask signal_range_pct>= | - | 0.518038 (q0.5) | 531/0.356 | 381/0.438 | 0.2904 | improve |
| +mask quality_score<= | - | 33.032392 (q0.2) | 230/0.356 | 147/0.443 | 0.2864 | improve |
| +mask upper_wick_pct>= | - | 0.0 (q0.2) | 972/0.344 | 656/0.422 | 0.2816 | flat |
| +mask lower_wick_pct>= | - | 0.0 (q0.2) | 972/0.344 | 656/0.422 | 0.2816 | flat |
| +mask close_loc>= | - | 0.0 (q0.2) | 972/0.344 | 656/0.422 | 0.2816 | flat |
| +mask wick_skew_pct<= | - | -0.114359 (q0.2) | 216/0.417 | 148/0.34 | 0.2784 | worse |
| +mask signal_range_pct<= | - | 0.859773 (q0.8) | 791/0.328 | 519/0.391 | 0.2776 | worse |
| +mask body_pct<= | - | 0.740738 (q0.5) | 540/0.318 | 357/0.37 | 0.2764 | worse |
| +mask vwap_dist_atr<= | - | -2.082345 (q0.8) | 789/0.352 | 544/0.447 | 0.276 | worse |
| +mask vol_ratio>= | - | 2.9299 (q0.5) | 510/0.334 | 393/0.41 | 0.2732 | worse |
| +mask vol_ratio>= | - | 1.956323 (q0.2) | 800/0.34 | 565/0.425 | 0.272 | worse |
| +mask upper_wick_pct<= | - | 0.121775 (q0.8) | 807/0.335 | 528/0.414 | 0.2718 | worse |
| +mask atr_pct<= | - | 0.004081 (q0.8) | 793/0.336 | 514/0.418 | 0.2704 | worse |
| +mask close_loc<= | - | 0.264435 (q0.8) | 817/0.345 | 553/0.439 | 0.2698 | worse |
| +mask body_pct>= | - | 0.740738 (q0.5) | 532/0.359 | 362/0.476 | 0.2654 | worse |
| +mask body_pct<= | - | 0.893233 (q0.8) | 819/0.334 | 550/0.421 | 0.2644 | worse |
| +mask quality_score>= | - | 33.032392 (q0.2) | 812/0.329 | 556/0.41 | 0.2642 | worse |
| +mask body_pct>= | - | 0.605268 (q0.2) | 818/0.331 | 557/0.419 | 0.2606 | worse |
| +mask vol_ratio<= | - | 2.9299 (q0.5) | 560/0.339 | 330/0.441 | 0.2574 | worse |
| +mask signal_range_pct<= | - | 0.518038 (q0.5) | 534/0.32 | 341/0.399 | 0.2568 | worse |
| +mask rs_pct<= | - | 0.579646 (q0.8) | 755/0.323 | 578/0.417 | 0.2478 | worse |
| +mask wick_skew_pct<= | - | 0.059542 (q0.8) | 803/0.323 | 550/0.423 | 0.243 | worse |
| +mask lower_wick_pct<= | - | 0.156235 (q0.8) | 809/0.324 | 535/0.427 | 0.2416 | worse |
| +mask quality_score>= | - | 60.208617 (q0.5) | 495/0.336 | 391/0.457 | 0.2392 | worse |
| +mask vol_ratio<= | - | 1.956323 (q0.2) | 233/0.33 | 135/0.444 | 0.2388 | worse |
| +mask lower_wick_pct<= | - | 0.063864 (q0.5) | 537/0.323 | 357/0.43 | 0.2374 | worse |
| +mask close_loc<= | - | 0.130038 (q0.5) | 532/0.339 | 365/0.481 | 0.2254 | worse |
| +mask wick_skew_pct>= | - | -0.114359 (q0.2) | 800/0.323 | 543/0.448 | 0.223 | worse |
| +mask atr_pct<= | - | 0.002761 (q0.5) | 500/0.281 | 351/0.365 | 0.2138 | worse |
| +mask rs_pct<= | - | -0.975455 (q0.5) | 451/0.316 | 406/0.445 | 0.2128 | worse |
| +mask vwap_dist_atr<= | - | -4.175895 (q0.5) | 490/0.322 | 365/0.463 | 0.2092 | worse |
| +mask vwap_dist_atr>= | - | -2.082345 (q0.8) | 245/0.282 | 146/0.376 | 0.2068 | worse |
| +mask wick_skew_pct>= | - | -0.017923 (q0.5) | 534/0.303 | 369/0.433 | 0.199 | worse |
| +mask upper_wick_pct>= | - | 0.121775 (q0.8) | 206/0.319 | 160/0.476 | 0.1934 | worse |
| +mask signal_range_pct<= | - | 0.321717 (q0.2) | 221/0.268 | 145/0.365 | 0.1904 | worse |
| +mask vol_ratio>= | - | 4.705811 (q0.8) | 228/0.287 | 152/0.415 | 0.1846 | worse |
| +mask vwap_dist_atr<= | - | -6.220735 (q0.2) | 198/0.287 | 153/0.432 | 0.171 | worse |
| +mask upper_wick_pct>= | - | 0.033413 (q0.5) | 533/0.309 | 378/0.484 | 0.169 | worse |
| +mask rs_pct<= | - | -2.553996 (q0.2) | 173/0.279 | 188/0.473 | 0.1238 | worse |
| +mask quality_score>= | - | 100.236424 (q0.8) | 186/0.267 | 180/0.512 | 0.071 | worse |
| +mask atr_pct<= | - | 0.002 (q0.2) | 205/0.187 | 144/0.354 | 0.0534 | worse |

## pre-momentum

| knob | old | new | FIT n/PF | VAL n/PF | score | vs baseline |
|---|---|---|---|---|---|---|
| +premom pre5_mom_r<= | - | 0.058804 (q0.2) | 108/0.442 | 222/0.434 | 0.4276 | improve |
| +premom pre3_close_pos<= | - | 0.439027 (q0.2) | 162/0.454 | 200/0.506 | 0.4124 | improve |
| +premom pre_entry_momentum_score>= | - | 74.553242 (q0.8) | 342/0.393 | 188/0.424 | 0.3682 | improve |
| +premom pre1_adx<= | - | 52.95204 (q0.8) | 843/0.383 | 560/0.433 | 0.343 | improve |
| +premom sig5_adx_calc<= | - | 22.805449 (q0.5) | 558/0.382 | 362/0.434 | 0.3404 | improve |
| +premom sig5_rsi_dir<= | - | 77.746125 (q0.8) | 819/0.384 | 560/0.439 | 0.34 | improve |
| +premom pre3_range_r>= | - | 0.629434 (q0.8) | 351/0.383 | 220/0.447 | 0.3318 | improve |
| +premom pre_entry_momentum_score<= | - | 52.083765 (q0.2) | 130/0.403 | 206/0.495 | 0.3294 | improve |
| +premom pre_entry_momentum_score<= | - | 64.881115 (q0.5) | 404/0.374 | 352/0.445 | 0.3172 | improve |
| +premom sig5_rsi_dir<= | - | 69.873615 (q0.5) | 577/0.382 | 381/0.47 | 0.3116 | improve |
| +premom pre3_close_pos<= | - | 0.757733 (q0.5) | 501/0.372 | 401/0.449 | 0.3104 | improve |
| +premom sig5_adx_calc<= | - | 34.747086 (q0.8) | 814/0.361 | 551/0.425 | 0.3098 | improve |
| +premom sig5_vol_ratio20<= | - | 4.66893 (q0.8) | 821/0.359 | 566/0.429 | 0.303 | improve |
| +premom pre5_mom_r>= | - | 0.639367 (q0.8) | 375/0.365 | 211/0.452 | 0.2954 | improve |
| +premom pre1_adx<= | - | 29.925525 (q0.2) | 219/0.417 | 171/0.571 | 0.2938 | improve |
| +premom pre3_close_pos>= | - | 0.979654 (q0.8) | 265/0.329 | 145/0.308 | 0.2912 | improve |
| +premom pre5_mom_r>= | - | 0.323626 (q0.5) | 715/0.336 | 396/0.399 | 0.2856 | improve |
| +premom pre3_range_r>= | - | 0.182953 (q0.2) | 900/0.349 | 608/0.429 | 0.285 | improve |
| +premom sig5_vol_ratio20>= | - | 1.95388 (q0.2) | 807/0.349 | 567/0.432 | 0.2826 | improve |
| +premom sig5_rsi_dir>= | - | 62.555346 (q0.2) | 787/0.339 | 544/0.41 | 0.2822 | improve |
| +premom pre3_range_r>= | - | 0.336432 (q0.5) | 697/0.347 | 457/0.432 | 0.279 | worse |
| +premom pre_entry_momentum_score>= | - | 64.881115 (q0.5) | 677/0.331 | 374/0.401 | 0.275 | worse |
| +premom pre5_mom_r<= | - | 0.639367 (q0.8) | 682/0.34 | 489/0.422 | 0.2744 | worse |
| +premom pre1_adx>= | - | 40.450022 (q0.5) | 513/0.328 | 357/0.396 | 0.2736 | worse |
| +premom sig5_vol_ratio20>= | - | 2.935553 (q0.5) | 513/0.339 | 392/0.421 | 0.2734 | worse |
| +premom pre5_mom_r<= | - | 0.323626 (q0.5) | 356/0.361 | 329/0.472 | 0.2722 | worse |
| +premom pre3_range_r<= | - | 0.629434 (q0.8) | 719/0.336 | 486/0.42 | 0.2688 | worse |
| +premom pre3_close_pos>= | - | 0.757733 (q0.5) | 597/0.333 | 345/0.414 | 0.2682 | worse |
| +premom pre_entry_momentum_score>= | - | 52.083765 (q0.2) | 872/0.332 | 518/0.416 | 0.2648 | worse |
| +premom pre3_close_pos<= | - | 0.979654 (q0.8) | 803/0.346 | 564/0.462 | 0.2532 | worse |
| +premom pre5_mom_r>= | - | 0.058804 (q0.2) | 888/0.327 | 500/0.421 | 0.2518 | worse |
| +premom pre3_range_r<= | - | 0.336432 (q0.5) | 389/0.317 | 286/0.4 | 0.2506 | worse |
| +premom pre1_adx<= | - | 40.450022 (q0.5) | 566/0.355 | 377/0.487 | 0.2494 | worse |
| +premom pre1_adx>= | - | 29.925525 (q0.2) | 823/0.317 | 545/0.404 | 0.2474 | worse |
| +premom sig5_vol_ratio20<= | - | 2.935553 (q0.5) | 581/0.347 | 350/0.473 | 0.2462 | worse |
| +premom pre3_close_pos>= | - | 0.439027 (q0.2) | 858/0.32 | 531/0.418 | 0.2416 | worse |
| +premom pre_entry_momentum_score<= | - | 74.553242 (q0.8) | 736/0.33 | 507/0.441 | 0.2412 | worse |
| +premom sig5_rsi_dir>= | - | 69.873615 (q0.5) | 499/0.301 | 352/0.401 | 0.221 | worse |
| +premom sig5_rsi_dir<= | - | 62.555346 (q0.2) | 260/0.335 | 153/0.478 | 0.2206 | worse |
| +premom sig5_vol_ratio20<= | - | 1.95388 (q0.2) | 249/0.34 | 143/0.491 | 0.2192 | worse |
| +premom sig5_vol_ratio20>= | - | 4.66893 (q0.8) | 233/0.3 | 161/0.406 | 0.2152 | worse |
| +premom sig5_adx_calc>= | - | 22.805449 (q0.5) | 532/0.316 | 372/0.443 | 0.2144 | worse |
| +premom sig5_rsi_dir>= | - | 77.746125 (q0.8) | 215/0.293 | 152/0.406 | 0.2026 | worse |
| +premom sig5_adx_calc>= | - | 16.058216 (q0.2) | 820/0.323 | 552/0.48 | 0.1974 | worse |
| +premom sig5_adx_calc>= | - | 34.747086 (q0.8) | 216/0.313 | 160/0.464 | 0.1922 | worse |
| +premom pre3_range_r<= | - | 0.182953 (q0.2) | 112/0.274 | 104/0.39 | 0.1812 | worse |
| +premom pre1_adx>= | - | 52.95204 (q0.8) | 204/0.271 | 166/0.395 | 0.1718 | worse |
| +premom sig5_adx_calc<= | - | 16.058216 (q0.2) | 217/0.408 | 157/0.256 | 0.1344 | worse |

## regime

| knob | old | new | FIT n/PF | VAL n/PF | score | vs baseline |
|---|---|---|---|---|---|---|
| +mask regime==BEAR | - | BEAR | 389/0.349 | 186/0.382 | 0.3226 | improve |
| +mask regime!=BULL | - | BULL | 640/0.325 | 464/0.373 | 0.2866 | improve |
| +mask regime!=TREND | - | TREND | 932/0.338 | 656/0.422 | 0.2708 | worse |
| +mask regime==NEUTRAL | - | NEUTRAL | 212/0.254 | 280/0.365 | 0.1652 | worse |

## Best stable knobs (score-improving, FIT and VAL both alive)

- **pre-momentum / +premom pre5_mom_r<=** -> 0.058804 (q0.2) (FIT 108/0.442, VAL 222/0.434, score 0.4276)
- **pre-momentum / +premom pre3_close_pos<=** -> 0.439027 (q0.2) (FIT 162/0.454, VAL 200/0.506, score 0.4124)
- **indicator/price-action / +mask lower_wick_pct>=** -> 0.156235 (q0.8) (FIT 210/0.39, VAL 153/0.404, score 0.3788)
- **pre-momentum / +premom pre_entry_momentum_score>=** -> 74.553242 (q0.8) (FIT 342/0.393, VAL 188/0.424, score 0.3682)
- **exit / sl_pct** -> 1.0 (FIT 886/0.414, VAL 612/0.487, score 0.3556)
- **indicator/price-action / +mask upper_wick_pct<=** -> 0.0 (q0.2) (FIT 362/0.37, VAL 225/0.389, score 0.3548)
- **guard / max_positions** -> 5 (FIT 413/0.365, VAL 280/0.378, score 0.3546)
- **indicator/price-action / +mask upper_wick_pct<=** -> 0.033413 (q0.5) (FIT 536/0.368, VAL 342/0.359, score 0.3518)
- **indicator/price-action / +mask wick_skew_pct<=** -> -0.017923 (q0.5) (FIT 536/0.376, VAL 352/0.41, score 0.3488)
- **exit / sl_pct** -> 0.85 (FIT 917/0.389, VAL 626/0.446, score 0.3434)
- **pre-momentum / +premom pre1_adx<=** -> 52.95204 (q0.8) (FIT 843/0.383, VAL 560/0.433, score 0.343)
- **pre-momentum / +premom sig5_adx_calc<=** -> 22.805449 (q0.5) (FIT 558/0.382, VAL 362/0.434, score 0.3404)
- **pre-momentum / +premom sig5_rsi_dir<=** -> 77.746125 (q0.8) (FIT 819/0.384, VAL 560/0.439, score 0.34)
- **exit / sl_pct** -> 1.1 (FIT 864/0.418, VAL 598/0.518, score 0.338)
- **indicator/price-action / +mask close_loc>=** -> 0.264435 (q0.8) (FIT 214/0.341, VAL 157/0.347, score 0.3362)

## Overfit-risk notes

- Any knob whose FIT PF explodes while VAL PF collapses is a knife-edge; the band objective already penalises the gap, and stage-5 adds neighborhood + dropout checks.
- Sweeps that push PF far above 1.80 are treated as overshoot, not success.