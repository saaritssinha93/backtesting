# A_MOD_BREAK_C1_LOW (SHORT) — ITERATION_LOG

_Generated 2026-07-03. Research-only; NO live trades; NO final_setup_conf.py edits._

Optimizer: Optuna TPE. Protocol: search ONLY on FIT/VAL (band objective, tent at PF 1.80, gap penalty); confirm on full TRAIN; TEST scored ONCE per finalist whose TRAIN lands in [1.30,1.80]; TEST evaluations budget-capped (0 used).

- Stage 1 baseline: 1 iteration
- Stage 3 single-knob sweeps: 167 iterations (see PARAMETER_SWEEP_SUMMARY.md)
- Stage 4 combination search: 600 trials (344 unique configs; full list in trials.csv)
- Stage 5/6 finalist + rescue confirmations: 52 iterations

## Full per-iteration log (baseline, sweeps, finalists, rescues)

Complete row-level log: `iteration_log.csv` (every iteration: stage, group, change, old/new, FIT/VAL/TRAIN/TEST metrics, exit counts, keep/reject + why + next action). Key iterations below.

| # | stage | group | change | old -> new | SL/Tgt | FIT n/PF | VAL n/PF | TRAIN n/PF | TEST n/PF | keep | why |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 1-baseline | baseline | current conf config | - -> - | 1.1/1.0 | 109.0/0.428 | 55.0/0.842 | 164.0/0.542 | 36.0/0.337 | baseline | no_train_edge,train_concentration,no_edge_anywhere,test_concentration,test_day_block_insig |
| 2 | 3-sweep | exit | sl_pct | 1.1 -> 0.5 | 0.5/1.0 | 151.0/0.252 | 90.0/0.419 | -/- | -/- | improve | band score 0.118 vs baseline 0.097 |
| 3 | 3-sweep | exit | sl_pct | 1.1 -> 0.7 | 0.7/1.0 | 180.0/0.343 | 93.0/0.471 | -/- | -/- | improve | band score 0.241 vs baseline 0.097 |
| 4 | 3-sweep | exit | sl_pct | 1.1 -> 0.85 | 0.85/1.0 | 174.0/0.447 | 84.0/0.819 | -/- | -/- | improve | band score 0.149 vs baseline 0.097 |
| 5 | 3-sweep | exit | sl_pct | 1.1 -> 1.0 | 1.0/1.0 | 128.0/0.363 | 64.0/0.668 | -/- | -/- | improve | band score 0.119 vs baseline 0.097 |
| 7 | 3-sweep | exit | sl_pct | 1.1 -> 1.5 | 1.5/1.0 | 54.0/0.683 | 19.0/0.783 | -/- | -/- | improve | band score 0.603 vs baseline 0.097 |
| 8 | 3-sweep | exit | tgt_pct | 1.0 -> 0.6 | 1.1/0.6 | 109.0/0.339 | 55.0/0.565 | -/- | -/- | improve | band score 0.158 vs baseline 0.097 |
| 14 | 3-sweep | filter | mask vol_ratio>= | 1.955814 -> 1.599691 (q0.1) | 1.1/1.0 | 140.0/0.427 | 65.0/0.78 | -/- | -/- | improve | band score 0.145 vs baseline 0.097 |
| 15 | 3-sweep | filter | mask vol_ratio>= | 1.955814 -> 1.862731 (q0.3) | 1.1/1.0 | 115.0/0.459 | 57.0/0.779 | -/- | -/- | improve | band score 0.203 vs baseline 0.097 |
| 17 | 3-sweep | filter | mask vol_ratio>= | 1.955814 -> 2.880785 (q0.7) | 1.1/1.0 | 66.0/0.519 | 31.0/1.007 | -/- | -/- | improve | band score 0.129 vs baseline 0.097 |
| 18 | 3-sweep | filter | mask vol_ratio>= | 1.955814 -> 4.449577 (q0.9) | 1.1/1.0 | 29.0/0.381 | 13.0/0.356 | -/- | -/- | improve | band score 0.336 vs baseline 0.097 |
| 19 | 3-sweep | filter | drop mask vol_ratio>=1.955814 | vol_ratio>=1.955814 -> dropped | 1.1/1.0 | 149.0/0.405 | 72.0/0.777 | -/- | -/- | improve | band score 0.107 vs baseline 0.097 |
| 20 | 3-sweep | indicator/price-action | +mask atr_pct>= | - -> 0.002013 (q0.2) | 1.1/1.0 | 108.0/0.431 | 54.0/0.811 | -/- | -/- | improve | band score 0.127 vs baseline 0.097 |
| 25 | 3-sweep | indicator/price-action | +mask atr_pct<= | - -> 0.00356 (q0.8) | 1.1/1.0 | 57.0/0.414 | 35.0/0.776 | -/- | -/- | improve | band score 0.124 vs baseline 0.097 |
| 28 | 3-sweep | indicator/price-action | +mask body_pct>= | - -> 0.745475 (q0.5) | 1.1/1.0 | 95.0/0.435 | 48.0/0.844 | -/- | -/- | improve | band score 0.108 vs baseline 0.097 |
| 31 | 3-sweep | indicator/price-action | +mask body_pct<= | - -> 0.902928 (q0.8) | 1.1/1.0 | 59.0/0.544 | 28.0/0.878 | -/- | -/- | improve | band score 0.277 vs baseline 0.097 |
| 34 | 3-sweep | indicator/price-action | +mask close_loc>= | - -> 0.125005 (q0.5) | 1.1/1.0 | 38.0/0.536 | 21.0/0.827 | -/- | -/- | improve | band score 0.303 vs baseline 0.097 |
| 37 | 3-sweep | indicator/price-action | +mask close_loc<= | - -> 0.265315 (q0.8) | 1.1/1.0 | 98.0/0.417 | 50.0/0.781 | -/- | -/- | improve | band score 0.126 vs baseline 0.097 |
| 40 | 3-sweep | indicator/price-action | +mask lower_wick_pct>= | - -> 0.043702 (q0.5) | 1.1/1.0 | 58.0/0.541 | 25.0/0.961 | -/- | -/- | improve | band score 0.205 vs baseline 0.097 |
| 43 | 3-sweep | indicator/price-action | +mask lower_wick_pct<= | - -> 0.103666 (q0.8) | 1.1/1.0 | 77.0/0.426 | 44.0/0.742 | -/- | -/- | improve | band score 0.173 vs baseline 0.097 |
| 46 | 3-sweep | indicator/price-action | +mask quality_score>= | - -> 55.626711 (q0.5) | 1.1/1.0 | 79.0/0.468 | 36.0/0.864 | -/- | -/- | improve | band score 0.151 vs baseline 0.097 |
| 49 | 3-sweep | indicator/price-action | +mask quality_score<= | - -> 85.832373 (q0.8) | 1.1/1.0 | 69.0/0.431 | 34.0/0.742 | -/- | -/- | improve | band score 0.182 vs baseline 0.097 |
| 50 | 3-sweep | indicator/price-action | +mask rs_pct>= | - -> -1.966975 (q0.2) | 1.1/1.0 | 77.0/0.399 | 33.0/0.696 | -/- | -/- | improve | band score 0.161 vs baseline 0.097 |
| 52 | 3-sweep | indicator/price-action | +mask rs_pct>= | - -> -0.633462 (q0.5) | 1.1/1.0 | 51.0/0.409 | 26.0/0.581 | -/- | -/- | improve | band score 0.271 vs baseline 0.097 |
| 54 | 3-sweep | indicator/price-action | +mask rs_pct>= | - -> 0.846421 (q0.8) | 1.1/1.0 | 26.0/0.486 | 10.0/0.559 | -/- | -/- | improve | band score 0.428 vs baseline 0.097 |
| 56 | 3-sweep | indicator/price-action | +mask signal_range_pct>= | - -> 0.25378 (q0.2) | 1.1/1.0 | 109.0/0.428 | 54.0/0.811 | -/- | -/- | improve | band score 0.122 vs baseline 0.097 |
| 58 | 3-sweep | indicator/price-action | +mask signal_range_pct>= | - -> 0.37312 (q0.5) | 1.1/1.0 | 104.0/0.45 | 53.0/0.857 | -/- | -/- | improve | band score 0.124 vs baseline 0.097 |
| 61 | 3-sweep | indicator/price-action | +mask signal_range_pct<= | - -> 0.54102 (q0.8) | 1.1/1.0 | 29.0/0.425 | 21.0/0.619 | -/- | -/- | improve | band score 0.270 vs baseline 0.097 |
| 64 | 3-sweep | indicator/price-action | +mask upper_wick_pct>= | - -> 0.021064 (q0.5) | 1.1/1.0 | 21.0/0.344 | 12.0/0.472 | -/- | -/- | improve | band score 0.242 vs baseline 0.097 |
| 72 | 3-sweep | indicator/price-action | +mask vol_ratio>= | - -> 3.442125 (q0.8) | 1.1/1.0 | 54.0/0.474 | 20.0/0.564 | -/- | -/- | improve | band score 0.402 vs baseline 0.097 |
| 74 | 3-sweep | indicator/price-action | +mask vwap_dist_atr>= | - -> -5.420678 (q0.2) | 1.1/1.0 | 84.0/0.383 | 41.0/0.725 | -/- | -/- | improve | band score 0.109 vs baseline 0.097 |
| 76 | 3-sweep | indicator/price-action | +mask vwap_dist_atr>= | - -> -3.592093 (q0.5) | 1.1/1.0 | 50.0/0.396 | 24.0/0.557 | -/- | -/- | improve | band score 0.267 vs baseline 0.097 |
| 81 | 3-sweep | indicator/price-action | +mask wick_skew_pct<= | - -> -0.079093 (q0.2) | 1.1/1.0 | 35.0/0.582 | 16.0/1.16 | -/- | -/- | improve | band score 0.120 vs baseline 0.097 |
| 83 | 3-sweep | indicator/price-action | +mask wick_skew_pct<= | - -> -0.011269 (q0.5) | 1.1/1.0 | 72.0/0.543 | 28.0/0.778 | -/- | -/- | improve | band score 0.355 vs baseline 0.097 |
| 86 | 3-sweep | regime | +mask regime==BEAR | - -> BEAR | 1.1/1.0 | 75.0/0.504 | 35.0/0.783 | -/- | -/- | improve | band score 0.281 vs baseline 0.097 |
| 89 | 3-sweep | regime | +mask regime!=TREND | - -> TREND | 1.1/1.0 | 99.0/0.457 | 55.0/0.842 | -/- | -/- | improve | band score 0.149 vs baseline 0.097 |
| 90 | 3-sweep | pre-momentum | premom pre5_mom_r>= | 0.425861 -> -0.017491 (q0.1) | 1.1/1.0 | 832.0/0.353 | 560.0/0.393 | -/- | -/- | improve | band score 0.321 vs baseline 0.097 |
| 91 | 3-sweep | pre-momentum | premom pre5_mom_r>= | 0.425861 -> 0.176027 (q0.3) | 1.1/1.0 | 723.0/0.384 | 398.0/0.53 | -/- | -/- | improve | band score 0.267 vs baseline 0.097 |
| 92 | 3-sweep | pre-momentum | premom pre5_mom_r>= | 0.425861 -> 0.268289 (q0.5) | 1.1/1.0 | 491.0/0.389 | 256.0/0.593 | -/- | -/- | improve | band score 0.226 vs baseline 0.097 |
| 93 | 3-sweep | pre-momentum | premom pre5_mom_r>= | 0.425861 -> 0.366698 (q0.7) | 1.1/1.0 | 203.0/0.414 | 106.0/0.773 | -/- | -/- | improve | band score 0.127 vs baseline 0.097 |
| 95 | 3-sweep | pre-momentum | drop premom pre5_mom_r>=0.425861 | pre5_mom_r>=0.425861 -> dropped | 1.1/1.0 | 841.0/0.343 | 602.0/0.39 | -/- | -/- | improve | band score 0.305 vs baseline 0.097 |
| 97 | 3-sweep | pre-momentum | premom pre3_range_r<= | 0.202087 -> 0.192758 (q0.3) | 1.1/1.0 | 97.0/0.396 | 46.0/0.712 | -/- | -/- | improve | band score 0.143 vs baseline 0.097 |
| 99 | 3-sweep | pre-momentum | premom pre3_range_r<= | 0.202087 -> 0.358687 (q0.7) | 1.1/1.0 | 384.0/0.495 | 158.0/0.757 | -/- | -/- | improve | band score 0.285 vs baseline 0.097 |
| 100 | 3-sweep | pre-momentum | premom pre3_range_r<= | 0.202087 -> 0.555485 (q0.9) | 1.1/1.0 | 611.0/0.449 | 286.0/0.529 | -/- | -/- | improve | band score 0.385 vs baseline 0.097 |
| 101 | 3-sweep | pre-momentum | drop premom pre3_range_r<=0.202087 | pre3_range_r<=0.202087 -> dropped | 1.1/1.0 | 746.0/0.426 | 374.0/0.465 | -/- | -/- | improve | band score 0.395 vs baseline 0.097 |
| 104 | 3-sweep | pre-momentum | +premom pre1_adx>= | - -> 30.073226 (q0.5) | 1.1/1.0 | 56.0/0.401 | 34.0/0.653 | -/- | -/- | improve | band score 0.199 vs baseline 0.097 |
| 106 | 3-sweep | pre-momentum | +premom pre1_adx>= | - -> 42.929242 (q0.8) | 1.1/1.0 | 17.0/0.702 | 12.0/0.667 | -/- | -/- | improve | band score 0.639 vs baseline 0.097 |
| 109 | 3-sweep | pre-momentum | +premom pre3_close_pos<= | - -> 0.5 (q0.2) | 1.1/1.0 | 37.0/0.757 | 15.0/1.464 | -/- | -/- | improve | band score 0.191 vs baseline 0.097 |
| 110 | 3-sweep | pre-momentum | +premom pre3_close_pos>= | - -> 0.799988 (q0.5) | 1.1/1.0 | 48.0/0.413 | 30.0/0.652 | -/- | -/- | improve | band score 0.222 vs baseline 0.097 |
| 126 | 3-sweep | pre-momentum | +premom pre_entry_momentum_score>= | - -> 55.072385 (q0.2) | 1.1/1.0 | 92.0/0.427 | 51.0/0.79 | -/- | -/- | improve | band score 0.137 vs baseline 0.097 |
| 128 | 3-sweep | pre-momentum | +premom pre_entry_momentum_score>= | - -> 65.08872 (q0.5) | 1.1/1.0 | 49.0/0.419 | 27.0/0.7 | -/- | -/- | improve | band score 0.194 vs baseline 0.097 |
| 131 | 3-sweep | pre-momentum | +premom pre_entry_momentum_score<= | - -> 73.12409 (q0.8) | 1.1/1.0 | 101.0/0.444 | 50.0/0.804 | -/- | -/- | improve | band score 0.156 vs baseline 0.097 |
| 132 | 3-sweep | pre-momentum | +premom sig5_adx_calc>= | - -> 22.103036 (q0.2) | 1.1/1.0 | 85.0/0.453 | 41.0/0.811 | -/- | -/- | improve | band score 0.167 vs baseline 0.097 |
| 134 | 3-sweep | pre-momentum | +premom sig5_adx_calc>= | - -> 27.694227 (q0.5) | 1.1/1.0 | 58.0/0.41 | 24.0/0.63 | -/- | -/- | improve | band score 0.234 vs baseline 0.097 |
| 137 | 3-sweep | pre-momentum | +premom sig5_adx_calc<= | - -> 36.90615 (q0.8) | 1.1/1.0 | 80.0/0.465 | 47.0/0.828 | -/- | -/- | improve | band score 0.175 vs baseline 0.097 |
| 139 | 3-sweep | pre-momentum | +premom sig5_rsi_dir<= | - -> 59.08515 (q0.2) | 1.1/1.0 | 24.0/0.426 | 13.0/0.576 | -/- | -/- | improve | band score 0.306 vs baseline 0.097 |
| 141 | 3-sweep | pre-momentum | +premom sig5_rsi_dir<= | - -> 65.688599 (q0.5) | 1.1/1.0 | 55.0/0.449 | 27.0/0.727 | -/- | -/- | improve | band score 0.227 vs baseline 0.097 |
| 142 | 3-sweep | pre-momentum | +premom sig5_rsi_dir>= | - -> 71.286547 (q0.8) | 1.1/1.0 | 26.0/0.446 | 13.0/0.69 | -/- | -/- | improve | band score 0.251 vs baseline 0.097 |
| 147 | 3-sweep | pre-momentum | +premom sig5_vol_ratio20<= | - -> 2.279791 (q0.5) | 1.1/1.0 | 20.0/0.392 | 14.0/0.592 | -/- | -/- | improve | band score 0.232 vs baseline 0.097 |
| 148 | 3-sweep | pre-momentum | +premom sig5_vol_ratio20>= | - -> 3.47122 (q0.8) | 1.1/1.0 | 53.0/0.449 | 20.0/0.619 | -/- | -/- | improve | band score 0.313 vs baseline 0.097 |
| 157 | 3-sweep | guard | max_slot | - -> 12:00 | 1.1/1.0 | 29.0/0.569 | 10.0/0.86 | -/- | -/- | improve | band score 0.336 vs baseline 0.097 |
| 158 | 3-sweep | guard | max_slot | - -> 12:30 | 1.1/1.0 | 45.0/0.498 | 22.0/0.821 | -/- | -/- | improve | band score 0.240 vs baseline 0.097 |
| 159 | 3-sweep | guard | max_slot | - -> 13:00 | 1.1/1.0 | 60.0/0.59 | 31.0/0.572 | -/- | -/- | improve | band score 0.558 vs baseline 0.097 |
| 160 | 3-sweep | guard | max_slot | - -> 14:00 | 1.1/1.0 | 91.0/0.465 | 47.0/0.758 | -/- | -/- | improve | band score 0.231 vs baseline 0.097 |
| 163 | 3-sweep | guard | top_n | - -> 2 | 1.1/1.0 | 22.0/0.535 | 10.0/0.749 | -/- | -/- | improve | band score 0.364 vs baseline 0.097 |
| 167 | 3-sweep | guard | max_positions | 20 -> 5 | 1.1/1.0 | 101.0/0.458 | 50.0/0.758 | -/- | -/- | improve | band score 0.218 vs baseline 0.097 |
| 169 | 5-finalist | combination | finalist #1 | - -> - | 1.5/2.5 | -/- | -/- | 85.0/0.93 | -/- | reject | TRAIN not in band or too thin (PF 0.93, n 85) |
| 170 | 5-finalist | combination | finalist #2 | - -> - | 1.5/2.5 | -/- | -/- | 86.0/0.924 | -/- | reject | TRAIN not in band or too thin (PF 0.924, n 86) |
| 171 | 5-finalist | combination | finalist #3 | - -> - | 1.5/2.0 | -/- | -/- | 86.0/0.883 | -/- | reject | TRAIN not in band or too thin (PF 0.883, n 86) |
| 172 | 5-finalist | combination | finalist #4 | - -> - | 1.0/2.5 | -/- | -/- | 86.0/0.838 | -/- | reject | TRAIN not in band or too thin (PF 0.838, n 86) |
| 173 | 5-finalist | combination | finalist #5 | - -> - | 1.0/2.5 | -/- | -/- | 85.0/0.843 | -/- | reject | TRAIN not in band or too thin (PF 0.843, n 85) |
| 174 | 5-finalist | combination | finalist #6 | - -> - | 1.0/2.5 | -/- | -/- | 84.0/0.92 | -/- | reject | TRAIN not in band or too thin (PF 0.92, n 84) |
| 175 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.2/2.0 | 38.0/0.743 | 36.0/0.792 | -/- | -/- | - | score 0.704 |
| 176 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.5/2.5 | 110.0/0.759 | 59.0/0.853 | -/- | -/- | - | score 0.684 |
| 177 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.0/1.5 | 38.0/0.752 | 36.0/0.699 | -/- | -/- | - | score 0.656 |
| 178 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.0/1.5 | 110.0/0.729 | 59.0/0.682 | -/- | -/- | - | score 0.644 |
| 179 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.0/1.5 | 32.0/0.669 | 42.0/0.786 | -/- | -/- | - | score 0.576 |
| 180 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.2/2.0 | 151.0/0.595 | 55.0/0.579 | -/- | -/- | - | score 0.566 |
| 181 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.2/2.0 | 72.0/0.723 | 23.0/0.633 | -/- | -/- | - | score 0.562 |
| 182 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.2/2.0 | 72.0/0.723 | 23.0/0.633 | -/- | -/- | - | score 0.562 |
| 183 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.5/2.0 | 200.0/0.568 | 142.0/0.597 | -/- | -/- | - | score 0.544 |
| 184 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.0/1.5 | 60.0/0.593 | 64.0/0.666 | -/- | -/- | - | score 0.535 |
| 185 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.0/1.5 | 60.0/0.593 | 64.0/0.666 | -/- | -/- | - | score 0.535 |
| 186 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.0/1.5 | 70.0/0.566 | 74.0/0.612 | -/- | -/- | - | score 0.530 |
| 187 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 0.85/1.5 | 44.0/0.545 | 41.0/0.575 | -/- | -/- | - | score 0.521 |
| 188 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.5/2.5 | 200.0/0.546 | 142.0/0.584 | -/- | -/- | - | score 0.516 |
| 189 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 0.85/1.0 | 60.0/0.526 | 64.0/0.515 | -/- | -/- | - | score 0.506 |
| 190 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.2/2.0 | 201.0/0.536 | 142.0/0.581 | -/- | -/- | - | score 0.500 |
| 191 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.0/1.5 | 324.0/0.543 | 193.0/0.513 | -/- | -/- | - | score 0.490 |
| 192 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.0/2.5 | 311.0/0.512 | 210.0/0.556 | -/- | -/- | - | score 0.477 |
| 193 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.5/2.5 | 210.0/0.477 | 182.0/0.475 | -/- | -/- | - | score 0.473 |
| 194 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.5/2.5 | 42.0/0.582 | 40.0/0.518 | -/- | -/- | - | score 0.467 |
| 195 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.2/2.0 | 249.0/0.47 | 171.0/0.475 | -/- | -/- | - | score 0.465 |
| 196 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.2/2.0 | 589.0/0.483 | 407.0/0.512 | -/- | -/- | - | score 0.459 |
| 197 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.0/1.5 | 61.0/1.031 | 37.0/0.707 | -/- | -/- | - | score 0.447 |
| 198 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.0/1.5 | 307.0/0.541 | 185.0/0.484 | -/- | -/- | - | score 0.439 |
| 199 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.2/2.0 | 218.0/0.525 | 100.0/0.639 | -/- | -/- | - | score 0.434 |
| 200 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.2/2.0 | 389.0/0.464 | 292.0/0.503 | -/- | -/- | - | score 0.432 |
| 201 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.2/2.0 | 288.0/0.509 | 200.0/0.621 | -/- | -/- | - | score 0.419 |
| 202 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.5/2.5 | 256.0/0.536 | 157.0/0.691 | -/- | -/- | - | score 0.413 |
| 203 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.5/2.5 | 163.0/0.489 | 102.0/0.584 | -/- | -/- | - | score 0.412 |
| 204 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.5/1.0 | 342.0/0.423 | 228.0/0.416 | -/- | -/- | - | score 0.411 |
| 205 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.2/2.0 | 662.0/0.445 | 425.0/0.507 | -/- | -/- | - | score 0.396 |
| 206 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.0/1.5 | 112.0/0.501 | 104.0/0.635 | -/- | -/- | - | score 0.394 |
| 207 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.0/1.0 | 110.0/0.68 | 59.0/0.52 | -/- | -/- | - | score 0.393 |
| 208 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 0.7/1.5 | 108.0/0.396 | 50.0/0.404 | -/- | -/- | - | score 0.389 |
| 209 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.0/1.0 | 307.0/0.402 | 210.0/0.418 | -/- | -/- | - | score 0.389 |
| 210 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.0/1.5 | 97.0/0.414 | 72.0/0.398 | -/- | -/- | - | score 0.385 |
| 211 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.5/2.5 | 262.0/0.451 | 196.0/0.534 | -/- | -/- | - | score 0.385 |
| 212 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 0.85/1.5 | 1123.0/0.412 | 698.0/0.451 | -/- | -/- | - | score 0.381 |
| 213 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 0.85/1.5 | 1123.0/0.412 | 698.0/0.451 | -/- | -/- | - | score 0.381 |
| 214 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.5/2.5 | 379.0/0.403 | 163.0/0.432 | -/- | -/- | - | score 0.380 |
| 215 | 6-rescue | R3-window-{"max_slot": "12:00"} | rescue variant | - -> - | 1.5/2.5 | 61.0/0.934 | 24.0/0.919 | 85.0/0.93 | -/- | reject | TRAIN out of band (PF 0.93, n 85) |
| 216 | 6-rescue | R3-window-{"min_slot": "10:00"} | rescue variant | - -> - | 1.5/2.5 | 61.0/0.934 | 25.0/0.901 | 86.0/0.924 | -/- | reject | TRAIN out of band (PF 0.924, n 86) |
| 217 | 6-rescue | R2-drop-mask-0 | rescue variant | - -> - | 1.5/2.5 | 106.0/0.747 | 42.0/0.745 | 148.0/0.746 | -/- | reject | TRAIN out of band (PF 0.746, n 148) |
| 218 | 6-rescue | R1-premom-off | rescue variant | - -> - | 1.2/2.0 | 38.0/0.743 | 36.0/0.792 | 74.0/0.765 | -/- | reject | TRAIN out of band (PF 0.765, n 74) |
| 219 | 6-rescue | R3-window-{"min_slot": "10:00", "max_slot": "14:00"} | rescue variant | - -> - | 1.5/2.5 | 124.0/0.645 | 45.0/0.671 | 169.0/0.652 | -/- | reject | TRAIN out of band (PF 0.652, n 169) |
| 220 | 6-rescue | R2-drop-premom-0 | rescue variant | - -> - | 1.5/2.5 | 187.0/0.46 | 78.0/0.365 | 265.0/0.429 | -/- | reject | TRAIN out of band (PF 0.429, n 265) |

## Top 40 stage-4 trials (by FIT/VAL band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|----|------|--------|-------|----------|----------|-------|
| 1 | 1.5 | 2.5 | regime==BEAR | pre1_adx<=17.217686 | {"min_slot": "11:00", "max_slot": "12:00", "top_n": 2} | 61/0.934 | 24/0.919 | 0.9078 |
| 2 | 1.5 | 2.5 | regime==BEAR | pre1_adx<=17.217686 | {"max_slot": "12:00", "top_n": 2} | 61/0.934 | 25/0.901 | 0.8745 |
| 3 | 1.5 | 2.0 | regime==BEAR | pre1_adx<=17.217686 | {"max_slot": "12:00", "top_n": 2} | 61/0.87 | 25/0.915 | 0.8346 |
| 4 | 1.5 | 2.0 | regime==BEAR | pre1_adx<=17.217686 | {"max_slot": "12:00", "top_n": 2} | 61/0.87 | 25/0.915 | 0.8346 |
| 5 | 1.0 | 2.5 | regime==BEAR | pre1_adx<=17.217686 | {"max_slot": "12:00", "top_n": 2} | 61/0.836 | 25/0.842 | 0.8313 |
| 6 | 1.0 | 2.5 | regime==BEAR | pre1_adx<=17.217686 | {"min_slot": "11:00", "max_slot": "12:00", "top_n": 2} | 61/0.836 | 24/0.859 | 0.8177 |
| 7 | 1.0 | 2.5 | regime==NEUTRAL | pre3_close_pos<=0.285725 | {"min_slot": "11:00", "max_slot": "12:00", "top_n": 2} | 40/0.966 | 44/0.875 | 0.8016 |
| 8 | 1.5 | 2.0 | regime==BEAR | pre1_adx<=17.217686 | {"max_slot": "12:30", "top_n": 2} | 76/0.937 | 31/0.851 | 0.7825 |
| 9 | 1.1 | 2.5 | regime==BEAR | pre1_adx<=17.217686 | {"min_slot": "11:00", "max_slot": "12:00", "top_n": 2} | 61/0.831 | 24/0.797 | 0.7697 |
| 10 | 1.0 | 2.5 | regime!=BEAR | pre_entry_momentum_score<=48.765382 | {"max_slot": "12:00", "top_n": 2} | 38/0.764 | 52/0.785 | 0.7468 |
| 11 | 1.1 | 2.5 | regime==BEAR | pre1_adx<=17.217686 | {"max_slot": "12:00", "top_n": 2} | 61/0.831 | 25/0.782 | 0.7434 |
| 12 | 1.5 | 1.5 | regime==BEAR | pre1_adx<=17.217686 | {"max_slot": "12:00", "top_n": 2} | 61/0.744 | 25/0.739 | 0.7356 |
| 13 | 0.7 | 2.0 | regime==BEAR | pre1_adx<=17.217686 | {"max_slot": "12:00", "top_n": 2} | 61/0.796 | 25/0.879 | 0.7303 |
| 14 | 1.5 | 2.5 | regime==BEAR | pre1_adx<=17.217686 | {"max_slot": "12:30", "top_n": 2} | 76/0.966 | 31/0.834 | 0.7292 |
| 15 | 1.0 | 2.5 | regime!=BEAR | pre_entry_momentum_score<=48.765382 | {"min_slot": "11:00", "max_slot": "12:00", "top_n": 2} | 38/0.764 | 52/0.816 | 0.7219 |
| 16 | 1.0 | 2.5 | regime!=TREND | pre1_adx<=17.217686 | {"max_slot": "12:00", "top_n": 2} | 96/0.688 | 45/0.688 | 0.6881 |
| 17 | 0.7 | 2.5 | regime==BEAR | pre1_adx<=17.217686 | {"max_slot": "12:00", "top_n": 2} | 61/0.84 | 25/1.037 | 0.6823 |
| 18 | 1.0 | 2.5 | (none) | pre1_adx<=17.217686 | {"max_slot": "12:00", "top_n": 2} | 106/0.704 | 45/0.688 | 0.6757 |
| 19 | 1.5 | 2.0 | regime==BEAR | pre1_adx<=17.217686 | {"max_slot": "14:00", "top_n": 2} | 124/0.694 | 45/0.679 | 0.667 |
| 20 | 0.7 | 2.5 | regime==BEAR | pre1_adx<=17.217686 | {"min_slot": "11:00", "max_slot": "12:00", "top_n": 2} | 61/0.84 | 24/1.062 | 0.6616 |
| 21 | 1.0 | 2.5 | (none) | pre1_adx<=17.217686 | {"min_slot": "11:00", "max_slot": "12:00", "top_n": 2} | 106/0.704 | 42/0.764 | 0.6566 |
| 22 | 1.5 | 1.25 | regime==BEAR | pre1_adx<=17.217686 | {"max_slot": "12:00", "top_n": 2} | 61/0.722 | 25/0.686 | 0.6565 |
| 23 | 1.1 | 2.5 | (none) | pre1_adx<=17.217686 | {"min_slot": "11:00", "max_slot": "12:00", "top_n": 2} | 106/0.682 | 42/0.714 | 0.6553 |
| 24 | 1.1 | 2.5 | regime==BEAR | pre1_adx<=17.217686 | {"min_slot": "11:00", "max_slot": "13:00", "top_n": 2} | 91/0.647 | 32/0.67 | 0.6289 |
| 25 | 1.5 | 2.5 | regime==BEAR | pre1_adx<=42.929242; pre3_close_pos>=1.0 | {"max_slot": "12:00", "top_n": 2} | 89/0.811 | 35/0.709 | 0.6272 |
| 26 | 1.5 | 2.5 | regime==BEAR | pre1_adx<=17.217686 | {"max_slot": "14:00", "top_n": 2} | 124/0.645 | 45/0.671 | 0.6252 |
| 27 | 1.5 | 2.0 | regime!=TREND | pre1_adx<=17.217686 | {"max_slot": "12:00", "top_n": 2} | 96/0.728 | 45/0.671 | 0.6244 |
| 28 | 1.5 | 2.0 | (none) | pre1_adx<=17.217686 | {"max_slot": "12:00", "top_n": 2} | 106/0.742 | 45/0.671 | 0.6136 |
| 29 | 1.2 | 1.5 | regime==BEAR | sig5_adx_calc<=22.103036 | {"min_slot": "09:45", "max_slot": "13:00", "top_n": 2} | 139/0.635 | 52/0.623 | 0.6127 |
| 30 | 1.5 | 2.0 | vwap_dist_atr>=-4.147842; wick_skew_pct<=0.015125; regime==BEAR | sig5_vol_ratio20<=1.576713 | {"max_slot": "14:00", "top_n": 2} | 43/0.624 | 39/0.603 | 0.5873 |
| 31 | 1.5 | 2.0 | regime==BEAR | pre1_adx<=17.217686 | {"max_slot": "14:30", "top_n": 2} | 137/0.657 | 55/0.618 | 0.5867 |
| 32 | 1.5 | 2.0 | regime==BEAR | pre1_adx<=17.217686 | {"top_n": 2} | 137/0.657 | 55/0.618 | 0.5867 |
| 33 | 1.5 | 2.0 | regime==BEAR | pre1_adx<=23.581755 | {"max_slot": "11:30", "top_n": 2} | 77/0.601 | 43/0.624 | 0.5824 |
| 34 | 0.85 | 2.5 | regime==BEAR | pre1_adx<=20.92282 | {"min_slot": "11:00", "max_slot": "13:00", "top_n": 2} | 177/0.592 | 79/0.586 | 0.5809 |
| 35 | 0.85 | 2.5 | regime==BEAR | pre1_adx<=20.92282 | {"min_slot": "09:45", "max_slot": "13:00", "top_n": 2} | 177/0.592 | 80/0.582 | 0.5741 |
| 36 | 1.2 | 2.5 | regime==BEAR | sig5_adx_calc<=22.103036 | {"min_slot": "09:45", "max_slot": "12:30", "top_n": 2} | 101/0.602 | 44/0.642 | 0.5694 |
| 37 | 1.2 | 2.5 | regime==BEAR | sig5_adx_calc<=22.103036 | {"min_slot": "09:45", "max_slot": "12:30", "top_n": 2} | 101/0.602 | 44/0.642 | 0.5694 |
| 38 | 1.5 | 2.0 | (none) | pre1_adx<=17.217686 | {"max_slot": "11:30", "top_n": 2} | 63/0.692 | 24/0.848 | 0.5684 |
| 39 | 1.0 | 2.5 | regime==BEAR | pre1_adx<=23.581755 | {"min_slot": "11:00", "max_slot": "12:00", "top_n": 2} | 129/0.568 | 65/0.575 | 0.5632 |
| 40 | 1.5 | 2.5 | regime==BEAR | pre1_adx<=23.581755 | {"max_slot": "12:00", "top_n": 2} | 126/0.563 | 65/0.569 | 0.5585 |

# PHASE 2 — enriched search (added 2026-07-03)

- Stage E1 single-feature scan: 846 iterations (sweeps_enriched.csv)
- Stage E2 Optuna TPE: 1200 trials / 934 unique configs (trials_enriched.csv), best FIT/VAL band score 1.729
- Stage E3/E4 confirmations + rescue: see iteration_log_enriched.csv (8 TEST evaluations spent)

## Phase-2 finalists (TRAIN-band configs, TEST scored once each)

| # | TRAIN | TEST | verdict |
|---|---|---|---|
| 1 | n=42 PF=1.681 net=Rs11,436 win%=59.5 tgt%=9.5 dayDom=0.55 dbp=0.1082 | n=18 PF=0.195 net=Rs-12,648 win%=22.2 tgt%=0.0 dayDom=9.99 dbp=1.0 | REJECT: TRAIN target-fill 9.5% < 12.0%; TRAIN domination; TEST PF 0.195 <= 1.4 |
| 2 | n=29 PF=1.496 net=Rs6,949 win%=55.2 tgt%=13.8 dayDom=0.784 dbp=0.2284 | n=12 PF=0.154 net=Rs-10,932 win%=8.3 tgt%=0.0 dayDom=9.99 dbp=0.9937 | REJECT: TRAIN domination; TEST PF 0.154 <= 1.4; TEST net PnL not positive |
| 3 | n=38 PF=1.752 net=Rs12,533 win%=60.5 tgt%=10.5 dayDom=0.502 dbp=0.0978 | n=12 PF=0.177 net=Rs-9,317 win%=8.3 tgt%=0.0 dayDom=9.99 dbp=1.0 | REJECT: TRAIN target-fill 10.5% < 12.0%; TRAIN domination; TEST PF 0.177 <= 1.4 |
| 4 | n=22 PF=1.736 net=Rs6,704 win%=45.5 tgt%=13.6 dayDom=0.812 dbp=0.1832 | n=6 PF=0.095 net=Rs-4,202 win%=16.7 tgt%=0.0 dayDom=9.99 dbp=0.9807 | REJECT: TRAIN domination; TEST PF 0.095 <= 1.4; TEST net PnL not positive |
| 5 | n=38 PF=1.406 net=Rs6,748 win%=57.9 tgt%=26.3 dayDom=0.629 dbp=0.2219 | n=15 PF=0.049 net=Rs-15,001 win%=13.3 tgt%=0.0 dayDom=9.99 dbp=1.0 | REJECT: TRAIN domination; TEST PF 0.049 <= 1.4; TEST net PnL not positive |
| 6 | n=42 PF=1.468 net=Rs9,005 win%=59.5 tgt%=9.5 dayDom=0.698 dbp=0.182 | n=18 PF=0.199 net=Rs-12,940 win%=27.8 tgt%=0.0 dayDom=9.99 dbp=1.0 | REJECT: TRAIN target-fill 9.5% < 12.0%; TRAIN domination; TEST PF 0.199 <= 1.4 |
| 7 | n=41 PF=1.586 net=Rs9,846 win%=58.5 tgt%=9.8 dayDom=0.638 dbp=0.1457 | n=18 PF=0.195 net=Rs-12,648 win%=22.2 tgt%=0.0 dayDom=9.99 dbp=1.0 | REJECT: TRAIN target-fill 9.8% < 12.0%; TRAIN domination; TEST PF 0.195 <= 1.4 |
| 8 | n=35 PF=1.798 net=Rs10,660 win%=54.3 tgt%=11.4 dayDom=0.59 dbp=0.1372 | n=10 PF=0.283 net=Rs-6,177 win%=20.0 tgt%=0.0 dayDom=9.99 dbp=0.9553 | REJECT: TRAIN target-fill 11.4% < 12.0%; TRAIN domination; TEST PF 0.283 <= 1.4 |

**Every TRAIN-band finalist collapsed out-of-sample (TEST PF 0.05-0.28, zero target exits, all net-negative, all day-concentrated). The in-band TRAIN pockets are noise, not edge.**