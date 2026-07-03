# B_HUGE_FAILED_BOUNCE (SHORT) — ITERATION_LOG

_Generated 2026-07-03. Research-only; NO live trades; NO final_setup_conf.py edits._

Optimizer: Optuna TPE. Protocol: search ONLY on FIT/VAL (band objective, tent at PF 1.80, gap penalty); confirm on full TRAIN; TEST scored ONCE per finalist whose TRAIN lands in [1.30,1.80]; TEST evaluations budget-capped (0 used).

- Stage 1 baseline: 1 iteration
- Stage 3 single-knob sweeps: 149 iterations (see PARAMETER_SWEEP_SUMMARY.md)
- Stage 4 combination search: 500 trials (327 unique configs; full list in trials.csv)
- Stage 5/6 finalist + rescue confirmations: 51 iterations

## Full per-iteration log (baseline, sweeps, finalists, rescues)

Complete row-level log: `iteration_log.csv` (every iteration: stage, group, change, old/new, FIT/VAL/TRAIN/TEST metrics, exit counts, keep/reject + why + next action). Key iterations below.

| # | stage | group | change | old -> new | SL/Tgt | FIT n/PF | VAL n/PF | TRAIN n/PF | TEST n/PF | keep | why |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 1-baseline | baseline | current conf config | - -> - | 0.7/1.25 | 972.0/0.344 | 656.0/0.422 | 1628.0/0.373 | 673.0/0.503 | baseline | no_train_edge,sl_too_tight_or_bad_entries,overtrading,train_concentration,no_edge_anywhere |
| 3 | 3-sweep | exit | sl_pct | 0.7 -> 0.85 | 0.85/1.25 | 917.0/0.389 | 626.0/0.446 | -/- | -/- | improve | band score 0.343 vs baseline 0.282 |
| 4 | 3-sweep | exit | sl_pct | 0.7 -> 1.0 | 1.0/1.25 | 886.0/0.414 | 612.0/0.487 | -/- | -/- | improve | band score 0.356 vs baseline 0.282 |
| 5 | 3-sweep | exit | sl_pct | 0.7 -> 1.1 | 1.1/1.25 | 864.0/0.418 | 598.0/0.518 | -/- | -/- | improve | band score 0.338 vs baseline 0.282 |
| 6 | 3-sweep | exit | sl_pct | 0.7 -> 1.2 | 1.2/1.25 | 853.0/0.409 | 591.0/0.548 | -/- | -/- | improve | band score 0.298 vs baseline 0.282 |
| 7 | 3-sweep | exit | sl_pct | 0.7 -> 1.5 | 1.5/1.25 | 814.0/0.433 | 571.0/0.566 | -/- | -/- | improve | band score 0.327 vs baseline 0.282 |
| 10 | 3-sweep | exit | tgt_pct | 1.25 -> 1.0 | 0.7/1.0 | 989.0/0.347 | 673.0/0.41 | -/- | -/- | improve | band score 0.297 vs baseline 0.282 |
| 11 | 3-sweep | exit | tgt_pct | 1.25 -> 1.5 | 0.7/1.5 | 955.0/0.35 | 642.0/0.411 | -/- | -/- | improve | band score 0.301 vs baseline 0.282 |
| 14 | 3-sweep | indicator/price-action | +mask atr_pct>= | - -> 0.002 (q0.2) | 0.7/1.25 | 849.0/0.369 | 572.0/0.439 | -/- | -/- | improve | band score 0.313 vs baseline 0.282 |
| 16 | 3-sweep | indicator/price-action | +mask atr_pct>= | - -> 0.002761 (q0.5) | 0.7/1.25 | 558.0/0.389 | 366.0/0.476 | -/- | -/- | improve | band score 0.319 vs baseline 0.282 |
| 18 | 3-sweep | indicator/price-action | +mask atr_pct>= | - -> 0.004081 (q0.8) | 0.7/1.25 | 209.0/0.353 | 159.0/0.419 | -/- | -/- | improve | band score 0.300 vs baseline 0.282 |
| 21 | 3-sweep | indicator/price-action | +mask body_pct<= | - -> 0.605268 (q0.2) | 0.7/1.25 | 222.0/0.393 | 146.0/0.465 | -/- | -/- | improve | band score 0.335 vs baseline 0.282 |
| 24 | 3-sweep | indicator/price-action | +mask body_pct>= | - -> 0.893233 (q0.8) | 0.7/1.25 | 209.0/0.37 | 160.0/0.459 | -/- | -/- | improve | band score 0.299 vs baseline 0.282 |
| 27 | 3-sweep | indicator/price-action | +mask close_loc<= | - -> 0.0 (q0.2) | 0.7/1.25 | 237.0/0.341 | 180.0/0.38 | -/- | -/- | improve | band score 0.310 vs baseline 0.282 |
| 28 | 3-sweep | indicator/price-action | +mask close_loc>= | - -> 0.130038 (q0.5) | 0.7/1.25 | 541.0/0.332 | 356.0/0.375 | -/- | -/- | improve | band score 0.298 vs baseline 0.282 |
| 30 | 3-sweep | indicator/price-action | +mask close_loc>= | - -> 0.264435 (q0.8) | 0.7/1.25 | 214.0/0.341 | 157.0/0.347 | -/- | -/- | improve | band score 0.336 vs baseline 0.282 |
| 33 | 3-sweep | indicator/price-action | +mask lower_wick_pct<= | - -> 0.0 (q0.2) | 0.7/1.25 | 237.0/0.341 | 180.0/0.38 | -/- | -/- | improve | band score 0.310 vs baseline 0.282 |
| 34 | 3-sweep | indicator/price-action | +mask lower_wick_pct>= | - -> 0.063864 (q0.5) | 0.7/1.25 | 532.0/0.355 | 364.0/0.413 | -/- | -/- | improve | band score 0.309 vs baseline 0.282 |
| 36 | 3-sweep | indicator/price-action | +mask lower_wick_pct>= | - -> 0.156235 (q0.8) | 0.7/1.25 | 210.0/0.39 | 153.0/0.404 | -/- | -/- | improve | band score 0.379 vs baseline 0.282 |
| 39 | 3-sweep | indicator/price-action | +mask quality_score<= | - -> 33.032392 (q0.2) | 0.7/1.25 | 230.0/0.356 | 147.0/0.443 | -/- | -/- | improve | band score 0.286 vs baseline 0.282 |
| 41 | 3-sweep | indicator/price-action | +mask quality_score<= | - -> 60.208617 (q0.5) | 0.7/1.25 | 563.0/0.338 | 327.0/0.385 | -/- | -/- | improve | band score 0.300 vs baseline 0.282 |
| 43 | 3-sweep | indicator/price-action | +mask quality_score<= | - -> 100.236424 (q0.8) | 0.7/1.25 | 809.0/0.36 | 521.0/0.405 | -/- | -/- | improve | band score 0.324 vs baseline 0.282 |
| 44 | 3-sweep | indicator/price-action | +mask rs_pct>= | - -> -2.553996 (q0.2) | 0.7/1.25 | 829.0/0.352 | 527.0/0.403 | -/- | -/- | improve | band score 0.311 vs baseline 0.282 |
| 46 | 3-sweep | indicator/price-action | +mask rs_pct>= | - -> -0.975455 (q0.5) | 0.7/1.25 | 599.0/0.353 | 315.0/0.397 | -/- | -/- | improve | band score 0.318 vs baseline 0.282 |
| 48 | 3-sweep | indicator/price-action | +mask rs_pct>= | - -> 0.579646 (q0.8) | 0.7/1.25 | 275.0/0.396 | 103.0/0.475 | -/- | -/- | improve | band score 0.333 vs baseline 0.282 |
| 50 | 3-sweep | indicator/price-action | +mask signal_range_pct>= | - -> 0.321717 (q0.2) | 0.7/1.25 | 829.0/0.359 | 571.0/0.434 | -/- | -/- | improve | band score 0.299 vs baseline 0.282 |
| 52 | 3-sweep | indicator/price-action | +mask signal_range_pct>= | - -> 0.518038 (q0.5) | 0.7/1.25 | 531.0/0.356 | 381.0/0.438 | -/- | -/- | improve | band score 0.290 vs baseline 0.282 |
| 54 | 3-sweep | indicator/price-action | +mask signal_range_pct>= | - -> 0.859773 (q0.8) | 0.7/1.25 | 206.0/0.4 | 159.0/0.503 | -/- | -/- | improve | band score 0.318 vs baseline 0.282 |
| 57 | 3-sweep | indicator/price-action | +mask upper_wick_pct<= | - -> 0.0 (q0.2) | 0.7/1.25 | 362.0/0.37 | 225.0/0.389 | -/- | -/- | improve | band score 0.355 vs baseline 0.282 |
| 59 | 3-sweep | indicator/price-action | +mask upper_wick_pct<= | - -> 0.033413 (q0.5) | 0.7/1.25 | 536.0/0.368 | 342.0/0.359 | -/- | -/- | improve | band score 0.352 vs baseline 0.282 |
| 67 | 3-sweep | indicator/price-action | +mask vol_ratio<= | - -> 4.705811 (q0.8) | 0.7/1.25 | 808.0/0.354 | 557.0/0.42 | -/- | -/- | improve | band score 0.301 vs baseline 0.282 |
| 68 | 3-sweep | indicator/price-action | +mask vwap_dist_atr>= | - -> -6.220735 (q0.2) | 0.7/1.25 | 828.0/0.364 | 568.0/0.417 | -/- | -/- | improve | band score 0.322 vs baseline 0.282 |
| 70 | 3-sweep | indicator/price-action | +mask vwap_dist_atr>= | - -> -4.175895 (q0.5) | 0.7/1.25 | 581.0/0.35 | 358.0/0.384 | -/- | -/- | improve | band score 0.323 vs baseline 0.282 |
| 77 | 3-sweep | indicator/price-action | +mask wick_skew_pct<= | - -> -0.017923 (q0.5) | 0.7/1.25 | 536.0/0.376 | 352.0/0.41 | -/- | -/- | improve | band score 0.349 vs baseline 0.282 |
| 78 | 3-sweep | indicator/price-action | +mask wick_skew_pct>= | - -> 0.059542 (q0.8) | 0.7/1.25 | 226.0/0.38 | 149.0/0.459 | -/- | -/- | improve | band score 0.317 vs baseline 0.282 |
| 80 | 3-sweep | regime | +mask regime!=BULL | - -> BULL | 0.7/1.25 | 640.0/0.325 | 464.0/0.373 | -/- | -/- | improve | band score 0.287 vs baseline 0.282 |
| 83 | 3-sweep | regime | +mask regime==BEAR | - -> BEAR | 0.7/1.25 | 389.0/0.349 | 186.0/0.382 | -/- | -/- | improve | band score 0.323 vs baseline 0.282 |
| 85 | 3-sweep | pre-momentum | +premom pre1_adx<= | - -> 29.925525 (q0.2) | 0.7/1.25 | 219.0/0.417 | 171.0/0.571 | -/- | -/- | improve | band score 0.294 vs baseline 0.282 |
| 89 | 3-sweep | pre-momentum | +premom pre1_adx<= | - -> 52.95204 (q0.8) | 0.7/1.25 | 843.0/0.383 | 560.0/0.433 | -/- | -/- | improve | band score 0.343 vs baseline 0.282 |
| 91 | 3-sweep | pre-momentum | +premom pre3_close_pos<= | - -> 0.439027 (q0.2) | 0.7/1.25 | 162.0/0.454 | 200.0/0.506 | -/- | -/- | improve | band score 0.412 vs baseline 0.282 |
| 93 | 3-sweep | pre-momentum | +premom pre3_close_pos<= | - -> 0.757733 (q0.5) | 0.7/1.25 | 501.0/0.372 | 401.0/0.449 | -/- | -/- | improve | band score 0.310 vs baseline 0.282 |
| 94 | 3-sweep | pre-momentum | +premom pre3_close_pos>= | - -> 0.979654 (q0.8) | 0.7/1.25 | 265.0/0.329 | 145.0/0.308 | -/- | -/- | improve | band score 0.291 vs baseline 0.282 |
| 96 | 3-sweep | pre-momentum | +premom pre3_range_r>= | - -> 0.182953 (q0.2) | 0.7/1.25 | 900.0/0.349 | 608.0/0.429 | -/- | -/- | improve | band score 0.285 vs baseline 0.282 |
| 100 | 3-sweep | pre-momentum | +premom pre3_range_r>= | - -> 0.629434 (q0.8) | 0.7/1.25 | 351.0/0.383 | 220.0/0.447 | -/- | -/- | improve | band score 0.332 vs baseline 0.282 |
| 103 | 3-sweep | pre-momentum | +premom pre5_mom_r<= | - -> 0.058804 (q0.2) | 0.7/1.25 | 108.0/0.442 | 222.0/0.434 | -/- | -/- | improve | band score 0.428 vs baseline 0.282 |
| 104 | 3-sweep | pre-momentum | +premom pre5_mom_r>= | - -> 0.323626 (q0.5) | 0.7/1.25 | 715.0/0.336 | 396.0/0.399 | -/- | -/- | improve | band score 0.286 vs baseline 0.282 |
| 106 | 3-sweep | pre-momentum | +premom pre5_mom_r>= | - -> 0.639367 (q0.8) | 0.7/1.25 | 375.0/0.365 | 211.0/0.452 | -/- | -/- | improve | band score 0.295 vs baseline 0.282 |
| 109 | 3-sweep | pre-momentum | +premom pre_entry_momentum_score<= | - -> 52.083765 (q0.2) | 0.7/1.25 | 130.0/0.403 | 206.0/0.495 | -/- | -/- | improve | band score 0.329 vs baseline 0.282 |
| 111 | 3-sweep | pre-momentum | +premom pre_entry_momentum_score<= | - -> 64.881115 (q0.5) | 0.7/1.25 | 404.0/0.374 | 352.0/0.445 | -/- | -/- | improve | band score 0.317 vs baseline 0.282 |
| 112 | 3-sweep | pre-momentum | +premom pre_entry_momentum_score>= | - -> 74.553242 (q0.8) | 0.7/1.25 | 342.0/0.393 | 188.0/0.424 | -/- | -/- | improve | band score 0.368 vs baseline 0.282 |
| 117 | 3-sweep | pre-momentum | +premom sig5_adx_calc<= | - -> 22.805449 (q0.5) | 0.7/1.25 | 558.0/0.382 | 362.0/0.434 | -/- | -/- | improve | band score 0.340 vs baseline 0.282 |
| 119 | 3-sweep | pre-momentum | +premom sig5_adx_calc<= | - -> 34.747086 (q0.8) | 0.7/1.25 | 814.0/0.361 | 551.0/0.425 | -/- | -/- | improve | band score 0.310 vs baseline 0.282 |
| 120 | 3-sweep | pre-momentum | +premom sig5_rsi_dir>= | - -> 62.555346 (q0.2) | 0.7/1.25 | 787.0/0.339 | 544.0/0.41 | -/- | -/- | improve | band score 0.282 vs baseline 0.282 |
| 123 | 3-sweep | pre-momentum | +premom sig5_rsi_dir<= | - -> 69.873615 (q0.5) | 0.7/1.25 | 577.0/0.382 | 381.0/0.47 | -/- | -/- | improve | band score 0.312 vs baseline 0.282 |
| 125 | 3-sweep | pre-momentum | +premom sig5_rsi_dir<= | - -> 77.746125 (q0.8) | 0.7/1.25 | 819.0/0.384 | 560.0/0.439 | -/- | -/- | improve | band score 0.340 vs baseline 0.282 |
| 126 | 3-sweep | pre-momentum | +premom sig5_vol_ratio20>= | - -> 1.95388 (q0.2) | 0.7/1.25 | 807.0/0.349 | 567.0/0.432 | -/- | -/- | improve | band score 0.283 vs baseline 0.282 |
| 131 | 3-sweep | pre-momentum | +premom sig5_vol_ratio20<= | - -> 4.66893 (q0.8) | 0.7/1.25 | 821.0/0.359 | 566.0/0.429 | -/- | -/- | improve | band score 0.303 vs baseline 0.282 |
| 142 | 3-sweep | guard | max_slot | - -> 14:00 | 0.7/1.25 | 828.0/0.352 | 540.0/0.437 | -/- | -/- | improve | band score 0.284 vs baseline 0.282 |
| 146 | 3-sweep | guard | top_n | - -> 3 | 0.7/1.25 | 937.0/0.342 | 624.0/0.409 | -/- | -/- | improve | band score 0.288 vs baseline 0.282 |
| 149 | 3-sweep | guard | max_positions | 20 -> 5 | 0.7/1.25 | 413.0/0.365 | 280.0/0.378 | -/- | -/- | improve | band score 0.355 vs baseline 0.282 |
| 151 | 5-finalist | combination | finalist #1 | - -> - | 1.2/1.5 | -/- | -/- | 26.0/1.043 | -/- | reject | TRAIN not in band or too thin (PF 1.043, n 26) |
| 152 | 5-finalist | combination | finalist #2 | - -> - | 1.2/1.25 | -/- | -/- | 25.0/0.861 | -/- | reject | TRAIN not in band or too thin (PF 0.861, n 25) |
| 153 | 5-finalist | combination | finalist #3 | - -> - | 1.2/1.25 | -/- | -/- | 27.0/0.811 | -/- | reject | TRAIN not in band or too thin (PF 0.811, n 27) |
| 154 | 5-finalist | combination | finalist #4 | - -> - | 1.2/1.5 | -/- | -/- | 27.0/0.951 | -/- | reject | TRAIN not in band or too thin (PF 0.951, n 27) |
| 155 | 5-finalist | combination | finalist #5 | - -> - | 1.2/1.5 | -/- | -/- | 27.0/0.951 | -/- | reject | TRAIN not in band or too thin (PF 0.951, n 27) |
| 156 | 5-finalist | combination | finalist #6 | - -> - | 1.2/1.25 | -/- | -/- | 48.0/0.733 | -/- | reject | TRAIN not in band or too thin (PF 0.733, n 48) |
| 157 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.0/1.5 | 361.0/0.422 | 164.0/0.423 | -/- | -/- | - | score 0.422 |
| 158 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.0/1.5 | 361.0/0.422 | 164.0/0.423 | -/- | -/- | - | score 0.422 |
| 159 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.0/1.25 | 367.0/0.42 | 166.0/0.438 | -/- | -/- | - | score 0.406 |
| 160 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.0/1.5 | 209.0/0.44 | 108.0/0.491 | -/- | -/- | - | score 0.400 |
| 161 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.0/1.0 | 371.0/0.433 | 167.0/0.414 | -/- | -/- | - | score 0.399 |
| 162 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 0.85/1.5 | 365.0/0.399 | 165.0/0.403 | -/- | -/- | - | score 0.395 |
| 163 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.0/0.8 | 661.0/0.394 | 457.0/0.4 | -/- | -/- | - | score 0.390 |
| 164 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.0/1.25 | 239.0/0.428 | 122.0/0.484 | -/- | -/- | - | score 0.384 |
| 165 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.0/1.5 | 372.0/0.417 | 174.0/0.398 | -/- | -/- | - | score 0.383 |
| 166 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.0/1.5 | 141.0/0.407 | 60.0/0.442 | -/- | -/- | - | score 0.379 |
| 167 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.0/2.5 | 239.0/0.403 | 122.0/0.433 | -/- | -/- | - | score 0.378 |
| 168 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 0.85/1.5 | 779.0/0.393 | 555.0/0.416 | -/- | -/- | - | score 0.375 |
| 169 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.2/1.5 | 354.0/0.439 | 162.0/0.524 | -/- | -/- | - | score 0.371 |
| 170 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.1/1.5 | 355.0/0.427 | 162.0/0.5 | -/- | -/- | - | score 0.370 |
| 171 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.1/1.5 | 355.0/0.427 | 162.0/0.5 | -/- | -/- | - | score 0.370 |
| 172 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.0/0.8 | 288.0/0.383 | 128.0/0.402 | -/- | -/- | - | score 0.369 |
| 173 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.0/1.5 | 284.0/0.419 | 135.0/0.483 | -/- | -/- | - | score 0.367 |
| 174 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.0/1.5 | 333.0/0.404 | 150.0/0.455 | -/- | -/- | - | score 0.363 |
| 175 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.2/0.8 | 372.0/0.389 | 167.0/0.423 | -/- | -/- | - | score 0.362 |
| 176 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 0.85/1.5 | 880.0/0.385 | 594.0/0.418 | -/- | -/- | - | score 0.358 |
| 177 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 0.85/1.5 | 880.0/0.385 | 594.0/0.418 | -/- | -/- | - | score 0.358 |
| 178 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 0.7/1.5 | 368.0/0.366 | 169.0/0.361 | -/- | -/- | - | score 0.357 |
| 179 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.5/1.5 | 347.0/0.424 | 158.0/0.523 | -/- | -/- | - | score 0.346 |
| 180 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 0.85/1.5 | 566.0/0.373 | 411.0/0.409 | -/- | -/- | - | score 0.344 |
| 181 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.1/1.0 | 886.0/0.404 | 601.0/0.481 | -/- | -/- | - | score 0.342 |
| 182 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.1/1.0 | 884.0/0.404 | 601.0/0.481 | -/- | -/- | - | score 0.342 |
| 183 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.0/1.25 | 828.0/0.402 | 567.0/0.484 | -/- | -/- | - | score 0.337 |
| 184 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.0/1.5 | 498.0/0.39 | 335.0/0.464 | -/- | -/- | - | score 0.331 |
| 185 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 0.85/1.5 | 521.0/0.347 | 363.0/0.368 | -/- | -/- | - | score 0.331 |
| 186 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 0.7/1.5 | 595.0/0.336 | 433.0/0.344 | -/- | -/- | - | score 0.329 |
| 187 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.0/0.8 | 373.0/0.388 | 169.0/0.352 | -/- | -/- | - | score 0.324 |
| 188 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 0.85/1.25 | 577.0/0.376 | 374.0/0.442 | -/- | -/- | - | score 0.324 |
| 189 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 0.85/1.5 | 832.0/0.373 | 564.0/0.44 | -/- | -/- | - | score 0.319 |
| 190 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.0/2.5 | 353.0/0.368 | 160.0/0.429 | -/- | -/- | - | score 0.319 |
| 191 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 0.85/1.5 | 809.0/0.37 | 524.0/0.436 | -/- | -/- | - | score 0.317 |
| 192 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.0/1.0 | 49.0/0.355 | 32.0/0.408 | -/- | -/- | - | score 0.312 |
| 193 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.0/1.5 | 135.0/0.399 | 67.0/0.512 | -/- | -/- | - | score 0.309 |
| 194 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 0.85/1.5 | 199.0/0.341 | 149.0/0.384 | -/- | -/- | - | score 0.307 |
| 195 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 0.85/1.5 | 642.0/0.364 | 445.0/0.442 | -/- | -/- | - | score 0.303 |
| 196 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.0/2.0 | 239.0/0.363 | 122.0/0.452 | -/- | -/- | - | score 0.292 |
| 197 | 6-rescue | R3-window-{"min_slot": "10:00"} | rescue variant | - -> - | 1.2/1.5 | 13.0/1.052 | 13.0/1.036 | 26.0/1.043 | -/- | reject | TRAIN out of band (PF 1.043, n 26) |
| 198 | 6-rescue | R3-window-{"min_slot": "10:00", "max_slot": "14:00"} | rescue variant | - -> - | 1.2/1.5 | 34.0/0.663 | 34.0/0.662 | 68.0/0.662 | -/- | reject | TRAIN out of band (PF 0.662, n 68) |
| 199 | 6-rescue | R1-premom-off | rescue variant | - -> - | 1.0/1.5 | 361.0/0.422 | 164.0/0.423 | 525.0/0.422 | -/- | reject | TRAIN out of band (PF 0.422, n 525) |
| 200 | 6-rescue | R2-drop-premom-0 | rescue variant | - -> - | 1.2/1.5 | 346.0/0.404 | 211.0/0.639 | 557.0/0.484 | -/- | reject | TRAIN out of band (PF 0.484, n 557) |
| 201 | 6-rescue | R3-window-{"max_slot": "12:00"} | rescue variant | - -> - | 1.2/1.5 | 11.0/0.786 | 7.0/1.187 | 18.0/0.948 | -/- | reject | TRAIN out of band (PF 0.948, n 18) |

## Top 40 stage-4 trials (by FIT/VAL band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|----|------|--------|-------|----------|----------|-------|
| 1 | 1.2 | 1.5 | (none) | pre3_range_r>=0.852981 | {"max_slot": "12:30", "top_n": 3} | 13/1.052 | 13/1.036 | 1.0235 |
| 2 | 1.2 | 1.5 | (none) | pre3_range_r>=0.852981 | {"max_slot": "12:30", "top_n": 3} | 13/1.052 | 13/1.036 | 1.0235 |
| 3 | 1.2 | 1.25 | (none) | pre3_range_r>=0.852981 | {"max_slot": "12:30", "top_n": 2} | 12/0.901 | 13/0.832 | 0.7761 |
| 4 | 1.2 | 1.25 | (none) | pre3_range_r>=0.852981 | {"max_slot": "12:30"} | 14/0.787 | 13/0.832 | 0.7516 |
| 5 | 1.2 | 1.5 | (none) | pre3_range_r>=0.852981 | {"min_slot": "10:00", "max_slot": "12:30"} | 14/0.857 | 13/1.036 | 0.7142 |
| 6 | 1.2 | 1.5 | (none) | pre3_range_r>=0.852981 | {"min_slot": "09:30", "max_slot": "12:30"} | 14/0.857 | 13/1.036 | 0.7142 |
| 7 | 1.2 | 1.5 | (none) | pre3_range_r>=0.852981 | {"min_slot": "09:45", "max_slot": "12:30"} | 14/0.857 | 13/1.036 | 0.7142 |
| 8 | 1.2 | 1.5 | (none) | pre3_range_r>=0.852981 | {"max_slot": "12:30"} | 14/0.857 | 13/1.036 | 0.7142 |
| 9 | 1.2 | 1.5 | (none) | pre3_range_r>=0.852981 | {"min_slot": "09:30", "max_slot": "12:30"} | 14/0.857 | 13/1.036 | 0.7142 |
| 10 | 1.2 | 1.25 | (none) | pre3_range_r>=0.629434 | {"min_slot": "11:00", "max_slot": "12:30", "top_n": 1} | 30/0.725 | 18/0.746 | 0.7091 |
| 11 | 1.2 | 1.5 | regime!=TREND | pre3_range_r>=0.852981 | {"max_slot": "12:30", "top_n": 3} | 12/0.851 | 13/1.036 | 0.7033 |
| 12 | 1.2 | 1.25 | (none) | pre3_range_r>=0.629434 | {"min_slot": "10:30", "max_slot": "12:00", "top_n": 1} | 21/0.842 | 12/0.764 | 0.7018 |
| 13 | 1.2 | 1.0 | lower_wick_pct<=0.118072; regime!=BULL | pre5_mom_r>=0.639367 | - | 51/0.766 | 20/0.716 | 0.676 |
| 14 | 1.2 | 1.25 | (none) | pre3_range_r>=0.852981 | {"max_slot": "12:30", "top_n": 1} | 11/0.74 | 13/0.832 | 0.6666 |
| 15 | 1.2 | 1.25 | (none) | pre3_range_r>=0.852981 | {"max_slot": "12:30", "top_n": 1} | 11/0.74 | 13/0.832 | 0.6666 |
| 16 | 1.2 | 1.25 | (none) | pre3_range_r>=0.852981 | {"min_slot": "10:30", "max_slot": "12:30", "top_n": 1} | 11/0.74 | 13/0.832 | 0.6666 |
| 17 | 1.2 | 1.5 | (none) | pre3_range_r>=0.852981 | {"min_slot": "09:30", "max_slot": "14:00"} | 37/0.639 | 35/0.628 | 0.62 |
| 18 | 1.2 | 1.5 | (none) | pre3_range_r>=0.852981 | {"max_slot": "14:00"} | 37/0.639 | 35/0.628 | 0.62 |
| 19 | 1.2 | 1.25 | (none) | pre3_range_r>=0.629434 | {"min_slot": "10:30", "max_slot": "12:30", "top_n": 1} | 31/0.673 | 18/0.746 | 0.6152 |
| 20 | 1.2 | 1.0 | (none) | pre3_range_r>=0.852981 | {"max_slot": "12:30", "top_n": 1} | 11/0.678 | 13/0.627 | 0.5871 |
| 21 | 1.2 | 1.5 | regime!=TREND | pre3_range_r>=0.852981 | {"min_slot": "09:30", "max_slot": "12:30", "top_n": 2} | 11/0.787 | 13/1.036 | 0.5869 |
| 22 | 1.2 | 1.5 | (none) | pre3_range_r>=0.852981 | {"max_slot": "12:30", "top_n": 1} | 11/0.786 | 13/1.036 | 0.5865 |
| 23 | 1.2 | 1.5 | (none) | pre5_mom_r>=0.85886 | {"max_slot": "12:30", "top_n": 3} | 22/0.657 | 13/0.761 | 0.5733 |
| 24 | 1.2 | 1.25 | (none) | pre3_range_r>=0.629434 | {"min_slot": "10:00", "max_slot": "13:00", "top_n": 1} | 37/0.621 | 27/0.682 | 0.5719 |
| 25 | 1.2 | 1.25 | (none) | pre3_range_r>=0.629434 | {"min_slot": "10:30", "max_slot": "13:00", "top_n": 1} | 37/0.621 | 27/0.682 | 0.5719 |
| 26 | 1.2 | 1.25 | regime==BEAR | pre_entry_momentum_score>=74.553242 | {"max_slot": "14:30"} | 71/0.577 | 33/0.585 | 0.571 |
| 27 | 1.2 | 1.25 | (none) | pre_entry_momentum_score>=74.553242 | {"min_slot": "11:00", "max_slot": "14:00"} | 129/0.576 | 63/0.606 | 0.552 |
| 28 | 1.2 | 0.8 | (none) | pre3_range_r>=0.852981 | {"max_slot": "12:30"} | 14/0.586 | 13/0.636 | 0.5462 |
| 29 | 1.2 | 0.8 | (none) | pre3_range_r>=0.852981 | {"min_slot": "09:30", "max_slot": "12:30"} | 14/0.586 | 13/0.636 | 0.5462 |
| 30 | 1.2 | 1.0 | (none) | pre3_range_r>=0.852981 | {"max_slot": "12:30"} | 14/0.751 | 13/0.627 | 0.5289 |
| 31 | 1.2 | 1.5 | (none) | pre3_range_r>=0.485661 | {"min_slot": "11:00", "max_slot": "12:30"} | 79/0.529 | 45/0.542 | 0.518 |
| 32 | 1.2 | 1.25 | vwap_dist_atr>=-5.41656 | pre3_range_r>=0.629434 | {"top_n": 1} | 67/0.576 | 45/0.65 | 0.517 |
| 33 | 1.2 | 1.0 | signal_range_pct>=0.321717 | pre3_range_r>=0.629434 | {"max_slot": "13:00", "top_n": 1} | 37/0.682 | 27/0.577 | 0.4932 |
| 34 | 1.2 | 1.25 | (none) | pre5_mom_r>=0.85886 | {"max_slot": "12:30", "top_n": 1} | 18/0.538 | 13/0.611 | 0.4798 |
| 35 | 0.85 | 1.25 | (none) | pre3_range_r>=0.852981 | {"max_slot": "14:00", "top_n": 1} | 74/0.54 | 42/0.506 | 0.4786 |
| 36 | 1.2 | 1.25 | regime==NEUTRAL | pre_entry_momentum_score>=67.967082 | {"min_slot": "09:45", "max_slot": "13:00", "top_n": 1} | 25/0.666 | 18/0.561 | 0.4761 |
| 37 | 1.2 | 1.5 | (none) | pre3_range_r>=0.485661 | {"max_slot": "12:30", "top_n": 3} | 77/0.519 | 42/0.584 | 0.4677 |
| 38 | 1.2 | 1.25 | (none) | pre3_range_r>=0.629434 | {"min_slot": "10:30", "max_slot": "13:00", "top_n": 2} | 46/0.69 | 33/0.564 | 0.4643 |
| 39 | 1.0 | 1.5 | (none) | pre3_range_r>=0.852981 | {"max_slot": "12:30", "top_n": 3} | 27/0.829 | 19/0.62 | 0.4533 |
| 40 | 1.2 | 1.25 | (none) | sig5_adx_calc>=42.037582 | {"max_slot": "12:30"} | 50/0.49 | 31/0.544 | 0.4468 |