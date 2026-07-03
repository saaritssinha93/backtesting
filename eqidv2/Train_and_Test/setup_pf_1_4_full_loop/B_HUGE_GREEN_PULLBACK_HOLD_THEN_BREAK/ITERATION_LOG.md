# B_HUGE_GREEN_PULLBACK_HOLD_THEN_BREAK (LONG) — ITERATION_LOG

_Generated 2026-07-03. Research-only; NO live trades; NO final_setup_conf.py edits._

Optimizer: Optuna TPE. Protocol: search ONLY on FIT/VAL (band objective, tent at PF 1.80, gap penalty); confirm on full TRAIN; TEST scored ONCE per finalist whose TRAIN lands in [1.30,1.80]; TEST evaluations budget-capped (5 used).

- Stage 1 baseline: 1 iteration
- Stage 3 single-knob sweeps: 149 iterations (see PARAMETER_SWEEP_SUMMARY.md)
- Stage 4 combination search: 500 trials (299 unique configs; full list in trials.csv)
- Stage 5/6 finalist + rescue confirmations: 52 iterations

## Full per-iteration log (baseline, sweeps, finalists, rescues)

Complete row-level log: `iteration_log.csv` (every iteration: stage, group, change, old/new, FIT/VAL/TRAIN/TEST metrics, exit counts, keep/reject + why + next action). Key iterations below.

| # | stage | group | change | old -> new | SL/Tgt | FIT n/PF | VAL n/PF | TRAIN n/PF | TEST n/PF | keep | why |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 1-baseline | baseline | current conf config | - -> - | 0.7/1.25 | 349.0/0.544 | 201.0/0.415 | 550.0/0.493 | 164.0/0.355 | baseline | no_train_edge,sl_too_tight_or_bad_entries,overtrading,train_concentration,no_edge_anywhere |
| 3 | 3-sweep | exit | sl_pct | 0.7 -> 0.85 | 0.85/1.25 | 349.0/0.542 | 201.0/0.432 | -/- | -/- | improve | band score 0.344 vs baseline 0.312 |
| 7 | 3-sweep | exit | sl_pct | 0.7 -> 1.5 | 1.5/1.25 | 340.0/0.657 | 189.0/0.475 | -/- | -/- | improve | band score 0.329 vs baseline 0.312 |
| 13 | 3-sweep | exit | tgt_pct | 1.25 -> 2.5 | 0.7/2.5 | 348.0/0.654 | 201.0/0.488 | -/- | -/- | improve | band score 0.355 vs baseline 0.312 |
| 17 | 3-sweep | indicator/price-action | +mask atr_pct<= | - -> 0.003409 (q0.5) | 0.7/1.25 | 180.0/0.419 | 101.0/0.485 | -/- | -/- | improve | band score 0.366 vs baseline 0.312 |
| 19 | 3-sweep | indicator/price-action | +mask atr_pct<= | - -> 0.004892 (q0.8) | 0.7/1.25 | 286.0/0.574 | 153.0/0.442 | -/- | -/- | improve | band score 0.336 vs baseline 0.312 |
| 20 | 3-sweep | indicator/price-action | +mask body_pct>= | - -> 0.597709 (q0.2) | 0.7/1.25 | 277.0/0.504 | 162.0/0.461 | -/- | -/- | improve | band score 0.427 vs baseline 0.312 |
| 22 | 3-sweep | indicator/price-action | +mask body_pct>= | - -> 0.71576 (q0.5) | 0.7/1.25 | 180.0/0.467 | 96.0/0.521 | -/- | -/- | improve | band score 0.424 vs baseline 0.312 |
| 24 | 3-sweep | indicator/price-action | +mask body_pct>= | - -> 0.873237 (q0.8) | 0.7/1.25 | 79.0/0.456 | 34.0/0.603 | -/- | -/- | improve | band score 0.338 vs baseline 0.312 |
| 26 | 3-sweep | indicator/price-action | +mask close_loc>= | - -> 0.714289 (q0.2) | 0.7/1.25 | 286.0/0.457 | 155.0/0.454 | -/- | -/- | improve | band score 0.452 vs baseline 0.312 |
| 28 | 3-sweep | indicator/price-action | +mask close_loc>= | - -> 0.833333 (q0.5) | 0.7/1.25 | 184.0/0.439 | 93.0/0.558 | -/- | -/- | improve | band score 0.344 vs baseline 0.312 |
| 30 | 3-sweep | indicator/price-action | +mask close_loc>= | - -> 0.96775 (q0.8) | 0.7/1.25 | 78.0/0.482 | 32.0/0.482 | -/- | -/- | improve | band score 0.482 vs baseline 0.312 |
| 34 | 3-sweep | indicator/price-action | +mask lower_wick_pct>= | - -> 0.045193 (q0.5) | 0.7/1.25 | 177.0/0.523 | 100.0/0.433 | -/- | -/- | improve | band score 0.361 vs baseline 0.312 |
| 37 | 3-sweep | indicator/price-action | +mask lower_wick_pct<= | - -> 0.152001 (q0.8) | 0.7/1.25 | 279.0/0.559 | 161.0/0.503 | -/- | -/- | improve | band score 0.458 vs baseline 0.312 |
| 39 | 3-sweep | indicator/price-action | +mask quality_score<= | - -> 29.864302 (q0.2) | 0.7/1.25 | 67.0/0.498 | 44.0/0.502 | -/- | -/- | improve | band score 0.495 vs baseline 0.312 |
| 40 | 3-sweep | indicator/price-action | +mask quality_score>= | - -> 74.793058 (q0.5) | 0.7/1.25 | 174.0/0.54 | 99.0/0.438 | -/- | -/- | improve | band score 0.356 vs baseline 0.312 |
| 42 | 3-sweep | indicator/price-action | +mask quality_score>= | - -> 131.577221 (q0.8) | 0.7/1.25 | 74.0/0.609 | 37.0/0.521 | -/- | -/- | improve | band score 0.451 vs baseline 0.312 |
| 45 | 3-sweep | indicator/price-action | +mask rs_pct<= | - -> -0.239824 (q0.2) | 0.7/1.25 | 71.0/0.447 | 42.0/0.393 | -/- | -/- | improve | band score 0.350 vs baseline 0.312 |
| 47 | 3-sweep | indicator/price-action | +mask rs_pct<= | - -> 1.819572 (q0.5) | 0.7/1.25 | 173.0/0.542 | 108.0/0.442 | -/- | -/- | improve | band score 0.362 vs baseline 0.312 |
| 49 | 3-sweep | indicator/price-action | +mask rs_pct<= | - -> 4.086475 (q0.8) | 0.7/1.25 | 282.0/0.502 | 163.0/0.422 | -/- | -/- | improve | band score 0.358 vs baseline 0.312 |
| 53 | 3-sweep | indicator/price-action | +mask signal_range_pct<= | - -> 0.745187 (q0.5) | 0.7/1.25 | 185.0/0.5 | 94.0/0.487 | -/- | -/- | improve | band score 0.477 vs baseline 0.312 |
| 55 | 3-sweep | indicator/price-action | +mask signal_range_pct<= | - -> 1.131096 (q0.8) | 0.7/1.25 | 290.0/0.554 | 148.0/0.428 | -/- | -/- | improve | band score 0.327 vs baseline 0.312 |
| 57 | 3-sweep | indicator/price-action | +mask upper_wick_pct<= | - -> 0.018387 (q0.2) | 0.7/1.25 | 81.0/0.459 | 29.0/0.451 | -/- | -/- | improve | band score 0.445 vs baseline 0.312 |
| 61 | 3-sweep | indicator/price-action | +mask upper_wick_pct<= | - -> 0.221804 (q0.8) | 0.7/1.25 | 288.0/0.513 | 154.0/0.456 | -/- | -/- | improve | band score 0.410 vs baseline 0.312 |
| 63 | 3-sweep | indicator/price-action | +mask vol_ratio<= | - -> 2.169739 (q0.2) | 0.7/1.25 | 78.0/0.461 | 36.0/0.556 | -/- | -/- | improve | band score 0.385 vs baseline 0.312 |
| 65 | 3-sweep | indicator/price-action | +mask vol_ratio<= | - -> 3.404217 (q0.5) | 0.7/1.25 | 176.0/0.551 | 100.0/0.485 | -/- | -/- | improve | band score 0.432 vs baseline 0.312 |
| 67 | 3-sweep | indicator/price-action | +mask vol_ratio<= | - -> 5.69666 (q0.8) | 0.7/1.25 | 286.0/0.539 | 154.0/0.417 | -/- | -/- | improve | band score 0.319 vs baseline 0.312 |
| 68 | 3-sweep | indicator/price-action | +mask vwap_dist_atr>= | - -> 1.301105 (q0.2) | 0.7/1.25 | 271.0/0.553 | 170.0/0.421 | -/- | -/- | improve | band score 0.315 vs baseline 0.312 |
| 71 | 3-sweep | indicator/price-action | +mask vwap_dist_atr<= | - -> 3.090952 (q0.5) | 0.7/1.25 | 182.0/0.5 | 99.0/0.399 | -/- | -/- | improve | band score 0.318 vs baseline 0.312 |
| 72 | 3-sweep | indicator/price-action | +mask vwap_dist_atr>= | - -> 4.48344 (q0.8) | 0.7/1.25 | 75.0/0.543 | 33.0/0.606 | -/- | -/- | improve | band score 0.493 vs baseline 0.312 |
| 75 | 3-sweep | indicator/price-action | +mask wick_skew_pct<= | - -> -0.046198 (q0.2) | 0.7/1.25 | 78.0/0.471 | 32.0/0.5 | -/- | -/- | improve | band score 0.448 vs baseline 0.312 |
| 77 | 3-sweep | indicator/price-action | +mask wick_skew_pct<= | - -> 0.050171 (q0.5) | 0.7/1.25 | 188.0/0.404 | 89.0/0.504 | -/- | -/- | improve | band score 0.324 vs baseline 0.312 |
| 79 | 3-sweep | indicator/price-action | +mask wick_skew_pct<= | - -> 0.143623 (q0.8) | 0.7/1.25 | 293.0/0.519 | 150.0/0.417 | -/- | -/- | improve | band score 0.335 vs baseline 0.312 |
| 84 | 3-sweep | pre-momentum | +premom pre1_adx>= | - -> 31.595497 (q0.2) | 0.7/1.25 | 286.0/0.554 | 159.0/0.439 | -/- | -/- | improve | band score 0.347 vs baseline 0.312 |
| 87 | 3-sweep | pre-momentum | +premom pre1_adx<= | - -> 40.17202 (q0.5) | 0.7/1.25 | 188.0/0.571 | 101.0/0.492 | -/- | -/- | improve | band score 0.429 vs baseline 0.312 |
| 89 | 3-sweep | pre-momentum | +premom pre1_adx<= | - -> 51.258694 (q0.8) | 0.7/1.25 | 290.0/0.546 | 161.0/0.453 | -/- | -/- | improve | band score 0.379 vs baseline 0.312 |
| 90 | 3-sweep | pre-momentum | +premom pre3_close_pos>= | - -> 0.541682 (q0.2) | 0.7/1.25 | 287.0/0.496 | 159.0/0.446 | -/- | -/- | improve | band score 0.406 vs baseline 0.312 |
| 92 | 3-sweep | pre-momentum | +premom pre3_close_pos>= | - -> 0.780858 (q0.5) | 0.7/1.25 | 186.0/0.471 | 94.0/0.537 | -/- | -/- | improve | band score 0.418 vs baseline 0.312 |
| 94 | 3-sweep | pre-momentum | +premom pre3_close_pos>= | - -> 0.958325 (q0.8) | 0.7/1.25 | 80.0/0.519 | 35.0/0.764 | -/- | -/- | improve | band score 0.323 vs baseline 0.312 |
| 96 | 3-sweep | pre-momentum | +premom pre3_range_r>= | - -> 0.276081 (q0.2) | 0.7/1.25 | 310.0/0.567 | 178.0/0.46 | -/- | -/- | improve | band score 0.374 vs baseline 0.312 |
| 99 | 3-sweep | pre-momentum | +premom pre3_range_r<= | - -> 0.508816 (q0.5) | 0.7/1.25 | 143.0/0.488 | 77.0/0.394 | -/- | -/- | improve | band score 0.319 vs baseline 0.312 |
| 101 | 3-sweep | pre-momentum | +premom pre3_range_r<= | - -> 0.921729 (q0.8) | 0.7/1.25 | 255.0/0.576 | 132.0/0.479 | -/- | -/- | improve | band score 0.401 vs baseline 0.312 |
| 103 | 3-sweep | pre-momentum | +premom pre5_mom_r<= | - -> 0.273359 (q0.2) | 0.7/1.25 | 42.0/0.402 | 46.0/0.404 | -/- | -/- | improve | band score 0.400 vs baseline 0.312 |
| 104 | 3-sweep | pre-momentum | +premom pre5_mom_r>= | - -> 0.548493 (q0.5) | 0.7/1.25 | 239.0/0.54 | 116.0/0.44 | -/- | -/- | improve | band score 0.360 vs baseline 0.312 |
| 106 | 3-sweep | pre-momentum | +premom pre5_mom_r>= | - -> 0.931647 (q0.8) | 0.7/1.25 | 124.0/0.509 | 67.0/0.456 | -/- | -/- | improve | band score 0.414 vs baseline 0.312 |
| 108 | 3-sweep | pre-momentum | +premom pre_entry_momentum_score>= | - -> 59.630823 (q0.2) | 0.7/1.25 | 308.0/0.538 | 162.0/0.432 | -/- | -/- | improve | band score 0.347 vs baseline 0.312 |
| 110 | 3-sweep | pre-momentum | +premom pre_entry_momentum_score>= | - -> 71.286952 (q0.5) | 0.7/1.25 | 211.0/0.472 | 105.0/0.438 | -/- | -/- | improve | band score 0.411 vs baseline 0.312 |
| 113 | 3-sweep | pre-momentum | +premom pre_entry_momentum_score<= | - -> 80.237597 (q0.8) | 0.7/1.25 | 265.0/0.574 | 144.0/0.437 | -/- | -/- | improve | band score 0.327 vs baseline 0.312 |
| 115 | 3-sweep | pre-momentum | +premom sig5_adx_calc<= | - -> 19.277018 (q0.2) | 0.7/1.25 | 71.0/0.646 | 42.0/0.62 | -/- | -/- | improve | band score 0.599 vs baseline 0.312 |
| 117 | 3-sweep | pre-momentum | +premom sig5_adx_calc<= | - -> 26.805832 (q0.5) | 0.7/1.25 | 183.0/0.607 | 97.0/0.594 | -/- | -/- | improve | band score 0.584 vs baseline 0.312 |
| 119 | 3-sweep | pre-momentum | +premom sig5_adx_calc<= | - -> 38.519627 (q0.8) | 0.7/1.25 | 290.0/0.525 | 157.0/0.455 | -/- | -/- | improve | band score 0.399 vs baseline 0.312 |
| 121 | 3-sweep | pre-momentum | +premom sig5_rsi_dir<= | - -> 62.814507 (q0.2) | 0.7/1.25 | 79.0/0.496 | 38.0/0.433 | -/- | -/- | improve | band score 0.383 vs baseline 0.312 |
| 123 | 3-sweep | pre-momentum | +premom sig5_rsi_dir<= | - -> 69.352257 (q0.5) | 0.7/1.25 | 192.0/0.526 | 96.0/0.512 | -/- | -/- | improve | band score 0.501 vs baseline 0.312 |
| 125 | 3-sweep | pre-momentum | +premom sig5_rsi_dir<= | - -> 76.045624 (q0.8) | 0.7/1.25 | 291.0/0.546 | 159.0/0.444 | -/- | -/- | improve | band score 0.362 vs baseline 0.312 |
| 127 | 3-sweep | pre-momentum | +premom sig5_vol_ratio20<= | - -> 2.169739 (q0.2) | 0.7/1.25 | 82.0/0.455 | 37.0/0.533 | -/- | -/- | improve | band score 0.393 vs baseline 0.312 |
| 129 | 3-sweep | pre-momentum | +premom sig5_vol_ratio20<= | - -> 3.404217 (q0.5) | 0.7/1.25 | 185.0/0.551 | 104.0/0.466 | -/- | -/- | improve | band score 0.398 vs baseline 0.312 |
| 130 | 3-sweep | pre-momentum | +premom sig5_vol_ratio20>= | - -> 5.69666 (q0.8) | 0.7/1.25 | 65.0/0.577 | 50.0/0.47 | -/- | -/- | improve | band score 0.384 vs baseline 0.312 |
| 131 | 3-sweep | pre-momentum | +premom sig5_vol_ratio20<= | - -> 5.69666 (q0.8) | 0.7/1.25 | 292.0/0.54 | 160.0/0.414 | -/- | -/- | improve | band score 0.313 vs baseline 0.312 |
| 136 | 3-sweep | guard | min_slot | - -> 11:00 | 0.7/1.25 | 348.0/0.546 | 200.0/0.418 | -/- | -/- | improve | band score 0.316 vs baseline 0.312 |
| 138 | 3-sweep | guard | max_slot | - -> 11:30 | 0.7/1.25 | 32.0/0.772 | 35.0/0.59 | -/- | -/- | improve | band score 0.444 vs baseline 0.312 |
| 139 | 3-sweep | guard | max_slot | - -> 12:00 | 0.7/1.25 | 73.0/0.572 | 75.0/0.436 | -/- | -/- | improve | band score 0.327 vs baseline 0.312 |
| 140 | 3-sweep | guard | max_slot | - -> 12:30 | 0.7/1.25 | 131.0/0.557 | 106.0/0.433 | -/- | -/- | improve | band score 0.334 vs baseline 0.312 |
| 141 | 3-sweep | guard | max_slot | - -> 13:00 | 0.7/1.25 | 172.0/0.57 | 118.0/0.449 | -/- | -/- | improve | band score 0.352 vs baseline 0.312 |
| 142 | 3-sweep | guard | max_slot | - -> 14:00 | 0.7/1.25 | 274.0/0.573 | 179.0/0.435 | -/- | -/- | improve | band score 0.325 vs baseline 0.312 |
| 144 | 3-sweep | guard | top_n | - -> 1 | 0.7/1.25 | 229.0/0.489 | 131.0/0.443 | -/- | -/- | improve | band score 0.406 vs baseline 0.312 |
| 146 | 3-sweep | guard | top_n | - -> 3 | 0.7/1.25 | 331.0/0.534 | 196.0/0.426 | -/- | -/- | improve | band score 0.340 vs baseline 0.312 |
| 147 | 3-sweep | guard | daily_loss_rs | 0.0 -> 2000.0 | 0.7/1.25 | 219.0/0.565 | 92.0/0.529 | -/- | -/- | improve | band score 0.500 vs baseline 0.312 |
| 148 | 3-sweep | guard | daily_loss_rs | 0.0 -> 4000.0 | 0.7/1.25 | 249.0/0.536 | 119.0/0.465 | -/- | -/- | improve | band score 0.408 vs baseline 0.312 |
| 149 | 3-sweep | guard | max_positions | 20 -> 5 | 0.7/1.25 | 239.0/0.559 | 145.0/0.452 | -/- | -/- | improve | band score 0.366 vs baseline 0.312 |
| 150 | 3-sweep | guard | max_positions | 20 -> 10 | 0.7/1.25 | 318.0/0.532 | 193.0/0.412 | -/- | -/- | improve | band score 0.316 vs baseline 0.312 |
| 151 | 5-finalist | combination | finalist #1 | - -> - | 0.85/2.0 | -/- | -/- | 26.0/2.104 | -/- | reject | TRAIN not in band or too thin (PF 2.104, n 26) |
| 152 | 5-finalist | combination | finalist #2 | - -> - | 0.85/0.8 | -/- | -/- | 26.0/1.53 | 8.0/0.524 | reject | TEST PF 0.524 <= 1.4; TEST net PnL not positive; TEST domination (trade>35% gross or day/s |
| 153 | 5-finalist | combination | finalist #3 | - -> - | 0.85/0.8 | -/- | -/- | 26.0/1.53 | 8.0/0.524 | reject | TEST PF 0.524 <= 1.4; TEST net PnL not positive; TEST domination (trade>35% gross or day/s |
| 154 | 5-finalist | combination | finalist #4 | - -> - | 0.85/2.0 | -/- | -/- | 37.0/1.279 | -/- | reject | TRAIN not in band or too thin (PF 1.279, n 37) |
| 155 | 5-finalist | combination | finalist #5 | - -> - | 0.85/2.0 | -/- | -/- | 36.0/1.411 | 11.0/0.249 | reject | TRAIN domination (trade>35% gross or day/sym>40% net); TEST PF 0.249 <= 1.4; TEST net PnL  |
| 156 | 5-finalist | combination | finalist #6 | - -> - | 0.85/2.0 | -/- | -/- | 36.0/1.411 | 11.0/0.249 | reject | TRAIN domination (trade>35% gross or day/sym>40% net); TEST PF 0.249 <= 1.4; TEST net PnL  |
| 157 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.1/2.5 | 30.0/1.34 | 24.0/1.26 | -/- | -/- | - | score 1.195 |
| 158 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.1/2.5 | 30.0/1.34 | 24.0/1.26 | -/- | -/- | - | score 1.195 |
| 159 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.1/2.5 | 30.0/1.34 | 24.0/1.26 | -/- | -/- | - | score 1.195 |
| 160 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.1/2.5 | 39.0/1.143 | 36.0/1.05 | -/- | -/- | - | score 0.976 |
| 161 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.1/2.5 | 39.0/1.143 | 36.0/1.05 | -/- | -/- | - | score 0.976 |
| 162 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.1/2.5 | 44.0/1.7 | 50.0/1.219 | -/- | -/- | - | score 0.834 |
| 163 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.1/2.5 | 38.0/1.064 | 34.0/0.929 | -/- | -/- | - | score 0.821 |
| 164 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.1/2.5 | 59.0/1.293 | 56.0/0.959 | -/- | -/- | - | score 0.692 |
| 165 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.1/2.5 | 38.0/0.923 | 38.0/0.783 | -/- | -/- | - | score 0.671 |
| 166 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.1/2.5 | 75.0/0.721 | 39.0/0.786 | -/- | -/- | - | score 0.668 |
| 167 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.1/2.5 | 69.0/1.337 | 68.0/0.963 | -/- | -/- | - | score 0.664 |
| 168 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.1/2.5 | 118.0/0.731 | 58.0/0.833 | -/- | -/- | - | score 0.650 |
| 169 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.1/2.5 | 72.0/0.656 | 32.0/0.632 | -/- | -/- | - | score 0.612 |
| 170 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.1/2.5 | 31.0/1.37 | 27.0/0.946 | -/- | -/- | - | score 0.607 |
| 171 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 0.85/2.5 | 198.0/0.486 | 113.0/0.487 | -/- | -/- | - | score 0.484 |
| 172 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 0.85/1.5 | 199.0/0.482 | 113.0/0.477 | -/- | -/- | - | score 0.472 |
| 173 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 0.85/2.5 | 195.0/0.534 | 116.0/0.492 | -/- | -/- | - | score 0.458 |
| 174 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 0.85/1.25 | 195.0/0.459 | 118.0/0.476 | -/- | -/- | - | score 0.445 |
| 175 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 0.7/1.5 | 199.0/0.5 | 113.0/0.467 | -/- | -/- | - | score 0.441 |
| 176 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.1/2.5 | 38.0/1.513 | 43.0/0.91 | -/- | -/- | - | score 0.429 |
| 177 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.1/2.5 | 132.0/0.935 | 83.0/0.648 | -/- | -/- | - | score 0.418 |
| 178 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 0.85/1.5 | 209.0/0.473 | 109.0/0.55 | -/- | -/- | - | score 0.412 |
| 179 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 0.85/1.5 | 215.0/0.512 | 104.0/0.456 | -/- | -/- | - | score 0.412 |
| 180 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.5/1.5 | 199.0/0.594 | 112.0/0.492 | -/- | -/- | - | score 0.410 |
| 181 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 0.85/1.5 | 137.0/0.429 | 67.0/0.467 | -/- | -/- | - | score 0.398 |
| 182 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 0.85/2.5 | 306.0/0.571 | 167.0/0.469 | -/- | -/- | - | score 0.388 |
| 183 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 0.5/1.0 | 37.0/0.426 | 19.0/0.398 | -/- | -/- | - | score 0.376 |
| 184 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.2/2.0 | 252.0/0.782 | 147.0/0.554 | -/- | -/- | - | score 0.371 |
| 185 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 0.85/1.5 | 181.0/0.508 | 102.0/0.429 | -/- | -/- | - | score 0.365 |
| 186 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 0.85/1.5 | 192.0/0.534 | 116.0/0.44 | -/- | -/- | - | score 0.365 |
| 187 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 0.85/2.5 | 158.0/0.402 | 75.0/0.38 | -/- | -/- | - | score 0.362 |
| 188 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 0.85/1.5 | 170.0/0.418 | 91.0/0.491 | -/- | -/- | - | score 0.359 |
| 189 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 0.85/1.5 | 130.0/0.399 | 81.0/0.461 | -/- | -/- | - | score 0.349 |
| 190 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.0/1.5 | 130.0/0.529 | 81.0/0.429 | -/- | -/- | - | score 0.349 |
| 191 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.2/1.5 | 199.0/0.595 | 112.0/0.457 | -/- | -/- | - | score 0.347 |
| 192 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.1/2.5 | 33.0/0.931 | 25.0/1.675 | -/- | -/- | - | score 0.335 |
| 193 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 0.85/1.5 | 330.0/0.542 | 196.0/0.422 | -/- | -/- | - | score 0.326 |
| 194 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 0.85/1.5 | 330.0/0.542 | 196.0/0.422 | -/- | -/- | - | score 0.326 |
| 195 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 0.85/1.5 | 78.0/0.61 | 51.0/0.451 | -/- | -/- | - | score 0.323 |
| 196 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 0.85/0.8 | 170.0/0.362 | 91.0/0.34 | -/- | -/- | - | score 0.322 |
| 197 | 6-rescue | R3-window-{"min_slot": "10:00"} | rescue variant | - -> - | 0.85/2.0 | 15.0/2.208 | 11.0/1.945 | 26.0/2.104 | -/- | reject | TRAIN out of band (PF 2.104, n 26) |
| 198 | 6-rescue | R3-window-{"min_slot": "10:00", "max_slot": "14:00"} | rescue variant | - -> - | 0.85/2.0 | 15.0/2.208 | 11.0/1.945 | 26.0/2.104 | -/- | reject | TRAIN out of band (PF 2.104, n 26) |
| 199 | 6-rescue | R1-premom-off | rescue variant | - -> - | 1.1/2.5 | 30.0/1.34 | 24.0/1.26 | 54.0/1.303 | 23.0/0.812 | reject | TRAIN domination (trade>35% gross or day/sym>40% net); TEST PF 0.812 <= 1.4; TEST net PnL  |
| 200 | 6-rescue | R2-drop-premom-0 | rescue variant | - -> - | 0.85/2.0 | 39.0/0.985 | 22.0/0.913 | 61.0/0.96 | -/- | reject | TRAIN out of band (PF 0.96, n 61) |
| 201 | 6-rescue | R2-drop-premom-1 | rescue variant | - -> - | 0.85/2.0 | 99.0/0.708 | 59.0/0.447 | 158.0/0.604 | -/- | reject | TRAIN out of band (PF 0.604, n 158) |
| 202 | 6-rescue | R3-window-{"max_slot": "12:00"} | rescue variant | - -> - | 0.85/2.0 | 2.0/0.0 | 5.0/5.191 | 7.0/1.735 | -/- | reject | TRAIN out of band (PF 1.735, n 7) |

## Top 40 stage-4 trials (by FIT/VAL band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|----|------|--------|-------|----------|----------|-------|
| 1 | 0.85 | 2.0 | (none) | sig5_vol_ratio20>=4.025157; pre3_close_pos>=1.0 | {"top_n": 1} | 15/2.208 | 11/1.945 | 1.4053 |
| 2 | 0.85 | 0.8 | (none) | sig5_vol_ratio20>=4.025157; pre3_close_pos>=1.0 | {"top_n": 1} | 15/1.436 | 11/1.683 | 1.2711 |
| 3 | 0.85 | 0.8 | (none) | sig5_vol_ratio20>=4.025157; pre3_close_pos>=1.0 | {"min_slot": "10:00", "top_n": 1} | 15/1.436 | 11/1.683 | 1.2711 |
| 4 | 0.85 | 0.8 | (none) | sig5_vol_ratio20>=4.025157; pre3_close_pos>=1.0 | {"min_slot": "09:30", "top_n": 1} | 15/1.436 | 11/1.683 | 1.2711 |
| 5 | 0.85 | 0.8 | (none) | sig5_vol_ratio20>=2.575953; pre3_close_pos>=1.0 | {"top_n": 1} | 27/1.491 | 15/1.34 | 1.2635 |
| 6 | 0.85 | 2.0 | (none) | sig5_vol_ratio20>=4.025157; pre3_close_pos>=0.958325 | {"min_slot": "11:00", "top_n": 1} | 24/1.289 | 13/1.259 | 1.2349 |
| 7 | 0.85 | 2.0 | (none) | sig5_vol_ratio20>=4.025157; pre3_close_pos>=1.0 | - | 22/1.339 | 14/1.546 | 1.2145 |
| 8 | 0.85 | 2.0 | regime!=BULL | sig5_vol_ratio20>=4.025157; pre3_close_pos>=1.0 | - | 22/1.339 | 14/1.546 | 1.2145 |
| 9 | 1.1 | 2.0 | regime!=BULL | sig5_vol_ratio20>=4.025157; pre3_close_pos>=1.0 | - | 22/1.292 | 14/1.414 | 1.1947 |
| 10 | 0.85 | 2.0 | regime!=BULL | sig5_vol_ratio20>=3.404217; pre3_close_pos>=1.0 | {"max_slot": "14:00"} | 28/1.302 | 14/1.546 | 1.1492 |
| 11 | 1.5 | 2.0 | regime!=BULL | sig5_vol_ratio20>=3.404217; pre3_close_pos>=1.0 | {"max_slot": "14:00"} | 28/1.172 | 14/1.156 | 1.1438 |
| 12 | 1.5 | 2.0 | regime!=BULL | sig5_vol_ratio20>=3.404217; pre3_close_pos>=1.0 | {"max_slot": "14:30"} | 30/1.257 | 14/1.156 | 1.0757 |
| 13 | 0.85 | 2.5 | (none) | pre3_close_pos>=0.653; pre3_close_pos>=1.0 | {"top_n": 1} | 39/1.083 | 22/1.074 | 1.0663 |
| 14 | 1.1 | 2.0 | regime!=BULL | sig5_vol_ratio20>=3.404217; pre3_close_pos>=1.0 | {"max_slot": "14:00"} | 28/1.208 | 14/1.414 | 1.0427 |
| 15 | 0.85 | 2.5 | (none) | sig5_vol_ratio20>=4.025157; pre3_close_pos>=1.0 | {"top_n": 1} | 15/2.239 | 11/2.453 | 1.0032 |
| 16 | 1.2 | 0.8 | (none) | sig5_vol_ratio20>=4.025157; pre3_close_pos>=1.0 | {"top_n": 1} | 15/1.862 | 11/1.366 | 1.0019 |
| 17 | 1.1 | 2.0 | regime!=BULL | sig5_vol_ratio20>=3.404217; pre3_close_pos>=1.0 | - | 30/1.181 | 14/1.414 | 0.9936 |
| 18 | 1.5 | 2.0 | regime!=BULL | pre1_adx>=40.17202; pre3_close_pos>=1.0 | {"max_slot": "14:30"} | 31/1.157 | 12/1.389 | 0.971 |
| 19 | 1.5 | 2.0 | regime!=BULL | pre1_adx>=40.17202; pre3_close_pos>=1.0 | {"max_slot": "14:30"} | 31/1.157 | 12/1.389 | 0.971 |
| 20 | 1.5 | 1.0 | regime!=BULL | pre1_adx>=40.17202; pre3_close_pos>=1.0 | {"max_slot": "14:00", "top_n": 2} | 26/1.04 | 12/1.001 | 0.9695 |
| 21 | 0.85 | 2.0 | (none) | sig5_vol_ratio20>=4.025157; pre3_close_pos>=0.848306 | {"top_n": 1} | 45/0.985 | 22/0.962 | 0.9436 |
| 22 | 1.5 | 1.0 | regime!=BULL | pre1_adx>=40.17202; pre3_close_pos>=1.0 | {"max_slot": "14:30"} | 31/1.093 | 12/1.001 | 0.9268 |
| 23 | 1.5 | 1.0 | regime!=BULL | pre1_adx>=40.17202; pre3_close_pos>=1.0 | {"max_slot": "14:30", "top_n": 2} | 29/1.094 | 12/1.001 | 0.9264 |
| 24 | 1.5 | 1.0 | regime!=TREND | pre1_adx>=40.17202; pre3_close_pos>=1.0 | {"max_slot": "14:30", "top_n": 2} | 29/1.094 | 12/1.001 | 0.9264 |
| 25 | 1.1 | 2.0 | regime!=BULL | sig5_vol_ratio20>=3.404217; pre3_close_pos>=0.848306 | - | 77/0.953 | 35/0.915 | 0.8856 |
| 26 | 0.85 | 0.6 | (none) | sig5_vol_ratio20>=4.025157; pre3_close_pos>=1.0 | {"top_n": 1} | 15/1.351 | 11/1.089 | 0.8795 |
| 27 | 1.2 | 2.0 | regime!=BULL | pre3_close_pos>=0.780858; pre3_close_pos>=1.0 | {"max_slot": "14:30"} | 63/0.967 | 29/0.911 | 0.8663 |
| 28 | 1.1 | 2.0 | regime!=BULL | sig5_vol_ratio20>=1.818637; pre3_close_pos>=1.0 | - | 59/0.852 | 27/0.904 | 0.8104 |
| 29 | 1.5 | 2.5 | regime!=BULL | pre1_adx>=37.388081; pre5_mom_r>=0.461821 | {"max_slot": "12:30"} | 35/1.045 | 27/1.34 | 0.8094 |
| 30 | 1.0 | 1.0 | (none) | pre1_adx>=40.17202; pre3_close_pos>=1.0 | {"max_slot": "14:30", "top_n": 2} | 29/1.046 | 12/1.348 | 0.8038 |
| 31 | 1.0 | 1.0 | regime!=TREND | pre1_adx>=40.17202; pre3_close_pos>=1.0 | {"max_slot": "14:30", "top_n": 2} | 29/1.046 | 12/1.348 | 0.8038 |
| 32 | 0.85 | 2.0 | regime!=BULL | sig5_vol_ratio20>=4.025157; pre3_close_pos>=0.903104 | - | 45/0.792 | 27/0.788 | 0.7843 |
| 33 | 0.85 | 0.8 | (none) | pre3_close_pos>=0.848306; pre3_close_pos>=1.0 | {"top_n": 1} | 39/0.808 | 22/0.771 | 0.7408 |
| 34 | 1.1 | 1.0 | regime!=BULL | pre1_adx>=40.17202; pre3_close_pos>=1.0 | {"max_slot": "14:30", "top_n": 2} | 29/0.967 | 12/1.261 | 0.7327 |
| 35 | 0.85 | 0.8 | (none) | sig5_vol_ratio20>=2.169739; pre3_close_pos>=1.0 | {"top_n": 1} | 30/1.225 | 19/0.936 | 0.7059 |
| 36 | 1.5 | 2.0 | regime!=BULL | sig5_vol_ratio20>=3.404217; pre3_close_pos>=0.903104 | {"max_slot": "14:00"} | 50/1.134 | 26/0.894 | 0.7021 |
| 37 | 1.5 | 2.0 | regime!=BULL | pre1_adx>=34.509932; pre3_close_pos>=1.0 | {"max_slot": "13:00"} | 20/0.969 | 12/1.319 | 0.689 |
| 38 | 0.85 | 2.0 | vol_ratio>=2.936993; regime!=TREND | (none) | {"max_slot": "12:00", "top_n": 2} | 44/0.977 | 42/0.816 | 0.688 |
| 39 | 1.0 | 1.0 | regime!=TREND | pre_entry_momentum_score>=71.286952; pre3_close_pos>=1.0 | {"max_slot": "14:30", "top_n": 2} | 43/0.712 | 18/0.699 | 0.6878 |
| 40 | 0.85 | 0.8 | (none) | pre_entry_momentum_score>=74.055416; pre3_close_pos>=1.0 | {"top_n": 1} | 32/0.731 | 14/0.815 | 0.664 |