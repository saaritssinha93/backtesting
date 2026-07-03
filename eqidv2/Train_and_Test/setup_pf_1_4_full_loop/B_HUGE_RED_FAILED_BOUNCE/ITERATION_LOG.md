# B_HUGE_RED_FAILED_BOUNCE (SHORT) — ITERATION_LOG

_Generated 2026-07-03. Research-only; NO live trades; NO final_setup_conf.py edits._

Optimizer: Optuna TPE. Protocol: search ONLY on FIT/VAL (band objective, tent at PF 1.80, gap penalty); confirm on full TRAIN; TEST scored ONCE per finalist whose TRAIN lands in [1.30,1.80]; TEST evaluations budget-capped (0 used).

- Stage 1 baseline: 1 iteration
- Stage 3 single-knob sweeps: 168 iterations (see PARAMETER_SWEEP_SUMMARY.md)
- Stage 4 combination search: 500 trials (377 unique configs; full list in trials.csv)
- Stage 5/6 finalist + rescue confirmations: 53 iterations

## Full per-iteration log (baseline, sweeps, finalists, rescues)

Complete row-level log: `iteration_log.csv` (every iteration: stage, group, change, old/new, FIT/VAL/TRAIN/TEST metrics, exit counts, keep/reject + why + next action). Key iterations below.

| # | stage | group | change | old -> new | SL/Tgt | FIT n/PF | VAL n/PF | TRAIN n/PF | TEST n/PF | keep | why |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 1-baseline | baseline | current conf config | - -> - | 0.9/1.25 | 22.0/0.848 | 26.0/0.621 | 48.0/0.716 | 41.0/0.72 | baseline | no_train_edge,too_many_time_exits,train_concentration,no_edge_anywhere,test_concentration, |
| 8 | 3-sweep | exit | sl_pct | 0.9 -> 1.5 | 1.5/1.25 | 26.0/0.821 | 32.0/0.868 | -/- | -/- | improve | band score 0.783 vs baseline 0.439 |
| 10 | 3-sweep | exit | tgt_pct | 1.25 -> 0.8 | 0.9/0.8 | 22.0/0.577 | 26.0/0.504 | -/- | -/- | improve | band score 0.446 vs baseline 0.439 |
| 12 | 3-sweep | exit | tgt_pct | 1.25 -> 1.5 | 0.9/1.5 | 22.0/0.773 | 26.0/0.616 | -/- | -/- | improve | band score 0.490 vs baseline 0.439 |
| 18 | 3-sweep | indicator/price-action | +mask atr_pct<= | - -> 0.002778 (q0.5) | 0.9/1.25 | 16.0/0.992 | 17.0/0.704 | -/- | -/- | improve | band score 0.474 vs baseline 0.439 |
| 21 | 3-sweep | indicator/price-action | +mask body_pct>= | - -> 0.604701 (q0.2) | 0.9/1.25 | 18.0/0.599 | 21.0/0.682 | -/- | -/- | improve | band score 0.533 vs baseline 0.439 |
| 32 | 3-sweep | indicator/price-action | +mask close_loc<= | - -> 0.264187 (q0.8) | 0.9/1.25 | 11.0/0.699 | 19.0/0.716 | -/- | -/- | improve | band score 0.685 vs baseline 0.439 |
| 47 | 3-sweep | indicator/price-action | +mask rs_pct>= | - -> -0.85198 (q0.5) | 0.9/1.25 | 18.0/0.897 | 15.0/0.847 | -/- | -/- | improve | band score 0.807 vs baseline 0.439 |
| 54 | 3-sweep | indicator/price-action | +mask signal_range_pct<= | - -> 0.536963 (q0.5) | 0.9/1.25 | 15.0/0.987 | 19.0/0.84 | -/- | -/- | improve | band score 0.722 vs baseline 0.439 |
| 56 | 3-sweep | indicator/price-action | +mask signal_range_pct<= | - -> 0.897446 (q0.8) | 0.9/1.25 | 17.0/0.816 | 23.0/0.66 | -/- | -/- | improve | band score 0.535 vs baseline 0.439 |
| 60 | 3-sweep | indicator/price-action | +mask upper_wick_pct<= | - -> 0.035822 (q0.5) | 0.9/1.25 | 12.0/0.91 | 14.0/0.677 | -/- | -/- | improve | band score 0.491 vs baseline 0.439 |
| 62 | 3-sweep | indicator/price-action | +mask upper_wick_pct<= | - -> 0.130377 (q0.8) | 0.9/1.25 | 20.0/0.611 | 22.0/0.707 | -/- | -/- | improve | band score 0.534 vs baseline 0.439 |
| 73 | 3-sweep | indicator/price-action | +mask vwap_dist_atr>= | - -> -2.60068 (q0.8) | 0.9/1.25 | 19.0/0.703 | 15.0/0.705 | -/- | -/- | improve | band score 0.701 vs baseline 0.439 |
| 75 | 3-sweep | indicator/price-action | +mask wick_skew_pct>= | - -> -0.120104 (q0.2) | 0.9/1.25 | 17.0/0.94 | 21.0/0.74 | -/- | -/- | improve | band score 0.580 vs baseline 0.439 |
| 80 | 3-sweep | indicator/price-action | +mask wick_skew_pct<= | - -> 0.066302 (q0.8) | 0.9/1.25 | 19.0/0.704 | 22.0/0.873 | -/- | -/- | improve | band score 0.569 vs baseline 0.439 |
| 83 | 3-sweep | regime | +mask regime!=TREND | - -> TREND | 0.9/1.25 | 21.0/0.806 | 26.0/0.621 | -/- | -/- | improve | band score 0.473 vs baseline 0.439 |
| 87 | 3-sweep | pre-momentum | premom pre3_close_pos<= | 0.581797 -> 0.772728 (q0.5) | 0.9/1.25 | 35.0/0.501 | 37.0/0.522 | -/- | -/- | improve | band score 0.484 vs baseline 0.439 |
| 106 | 3-sweep | pre-momentum | +premom pre1_adx<= | - -> 41.453928 (q0.5) | 0.9/1.25 | 19.0/0.992 | 19.0/0.799 | -/- | -/- | improve | band score 0.645 vs baseline 0.439 |
| 108 | 3-sweep | pre-momentum | +premom pre1_adx<= | - -> 54.183743 (q0.8) | 0.9/1.25 | 21.0/0.865 | 25.0/0.631 | -/- | -/- | improve | band score 0.444 vs baseline 0.439 |
| 118 | 3-sweep | pre-momentum | +premom pre3_range_r<= | - -> 0.364407 (q0.5) | 0.9/1.25 | 19.0/0.7 | 22.0/0.707 | -/- | -/- | improve | band score 0.694 vs baseline 0.439 |
| 120 | 3-sweep | pre-momentum | +premom pre3_range_r<= | - -> 0.667405 (q0.8) | 0.9/1.25 | 21.0/0.729 | 24.0/0.766 | -/- | -/- | improve | band score 0.699 vs baseline 0.439 |
| 133 | 3-sweep | pre-momentum | +premom sig5_adx_calc>= | - -> 15.694835 (q0.2) | 0.9/1.25 | 18.0/0.925 | 12.0/0.694 | -/- | -/- | improve | band score 0.509 vs baseline 0.439 |
| 136 | 3-sweep | pre-momentum | +premom sig5_adx_calc<= | - -> 21.887561 (q0.5) | 0.9/1.25 | 16.0/0.812 | 21.0/0.635 | -/- | -/- | improve | band score 0.493 vs baseline 0.439 |
| 161 | 3-sweep | guard | max_slot | - -> 14:00 | 0.9/1.25 | 19.0/0.831 | 20.0/0.908 | -/- | -/- | improve | band score 0.769 vs baseline 0.439 |
| 163 | 3-sweep | guard | top_n | - -> 1 | 0.9/1.25 | 17.0/0.775 | 20.0/0.609 | -/- | -/- | improve | band score 0.476 vs baseline 0.439 |
| 164 | 3-sweep | guard | top_n | - -> 2 | 0.9/1.25 | 22.0/0.848 | 25.0/0.685 | -/- | -/- | improve | band score 0.555 vs baseline 0.439 |
| 166 | 3-sweep | guard | daily_loss_rs | 0.0 -> 2000.0 | 0.9/1.25 | 22.0/0.848 | 25.0/0.686 | -/- | -/- | improve | band score 0.556 vs baseline 0.439 |
| 170 | 5-finalist | combination | finalist #1 | - -> - | 0.7/1.5 | -/- | -/- | 23.0/0.799 | -/- | reject | TRAIN not in band or too thin (PF 0.799, n 23) |
| 171 | 5-finalist | combination | finalist #2 | - -> - | 0.7/1.5 | -/- | -/- | 23.0/0.799 | -/- | reject | TRAIN not in band or too thin (PF 0.799, n 23) |
| 172 | 5-finalist | combination | finalist #3 | - -> - | 1.0/1.5 | -/- | -/- | 90.0/0.692 | -/- | reject | TRAIN not in band or too thin (PF 0.692, n 90) |
| 173 | 5-finalist | combination | finalist #4 | - -> - | 0.7/1.5 | -/- | -/- | 29.0/0.611 | -/- | reject | TRAIN not in band or too thin (PF 0.611, n 29) |
| 174 | 5-finalist | combination | finalist #5 | - -> - | 1.2/0.8 | -/- | -/- | 79.0/0.58 | -/- | reject | TRAIN not in band or too thin (PF 0.58, n 79) |
| 175 | 5-finalist | combination | finalist #6 | - -> - | 0.7/1.5 | -/- | -/- | 34.0/0.559 | -/- | reject | TRAIN not in band or too thin (PF 0.559, n 34) |
| 176 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.1/1.5 | 49.0/0.518 | 38.0/0.495 | -/- | -/- | - | score 0.476 |
| 177 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.1/1.5 | 71.0/0.468 | 37.0/0.467 | -/- | -/- | - | score 0.465 |
| 178 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.1/1.5 | 66.0/0.476 | 48.0/0.492 | -/- | -/- | - | score 0.463 |
| 179 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.0/1.0 | 45.0/0.531 | 21.0/0.491 | -/- | -/- | - | score 0.459 |
| 180 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.1/1.5 | 18.0/0.611 | 12.0/0.809 | -/- | -/- | - | score 0.452 |
| 181 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.5/1.0 | 270.0/0.491 | 131.0/0.542 | -/- | -/- | - | score 0.450 |
| 182 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.1/1.5 | 137.0/0.474 | 103.0/0.506 | -/- | -/- | - | score 0.449 |
| 183 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 0.85/1.5 | 49.0/0.481 | 38.0/0.459 | -/- | -/- | - | score 0.441 |
| 184 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.1/1.5 | 95.0/0.488 | 65.0/0.56 | -/- | -/- | - | score 0.429 |
| 185 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.2/1.5 | 49.0/0.624 | 38.0/0.515 | -/- | -/- | - | score 0.428 |
| 186 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.1/1.5 | 142.0/0.496 | 121.0/0.588 | -/- | -/- | - | score 0.423 |
| 187 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.0/1.0 | 279.0/0.451 | 137.0/0.489 | -/- | -/- | - | score 0.420 |
| 188 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.0/1.0 | 254.0/0.471 | 130.0/0.537 | -/- | -/- | - | score 0.418 |
| 189 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.1/1.5 | 71.0/0.488 | 49.0/0.579 | -/- | -/- | - | score 0.415 |
| 190 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.1/2.5 | 37.0/0.495 | 29.0/0.451 | -/- | -/- | - | score 0.415 |
| 191 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.1/1.5 | 57.0/0.494 | 41.0/0.6 | -/- | -/- | - | score 0.410 |
| 192 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.1/1.0 | 71.0/0.416 | 37.0/0.424 | -/- | -/- | - | score 0.410 |
| 193 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.1/1.5 | 146.0/0.469 | 103.0/0.548 | -/- | -/- | - | score 0.406 |
| 194 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.1/1.5 | 155.0/0.432 | 81.0/0.482 | -/- | -/- | - | score 0.393 |
| 195 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.1/1.5 | 67.0/0.443 | 45.0/0.512 | -/- | -/- | - | score 0.387 |
| 196 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 0.7/1.0 | 237.0/0.418 | 117.0/0.464 | -/- | -/- | - | score 0.382 |
| 197 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.0/1.0 | 299.0/0.444 | 149.0/0.405 | -/- | -/- | - | score 0.373 |
| 198 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.0/1.0 | 299.0/0.444 | 149.0/0.405 | -/- | -/- | - | score 0.373 |
| 199 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.1/1.5 | 68.0/0.581 | 49.0/0.46 | -/- | -/- | - | score 0.363 |
| 200 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.2/1.25 | 183.0/0.433 | 110.0/0.529 | -/- | -/- | - | score 0.356 |
| 201 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 0.85/1.5 | 530.0/0.375 | 412.0/0.364 | -/- | -/- | - | score 0.355 |
| 202 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.1/0.8 | 71.0/0.359 | 37.0/0.37 | -/- | -/- | - | score 0.350 |
| 203 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.0/1.0 | 38.0/0.387 | 27.0/0.437 | -/- | -/- | - | score 0.347 |
| 204 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.0/1.0 | 38.0/0.387 | 27.0/0.437 | -/- | -/- | - | score 0.347 |
| 205 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.1/1.5 | 86.0/0.462 | 60.0/0.605 | -/- | -/- | - | score 0.347 |
| 206 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.0/1.0 | 213.0/0.431 | 129.0/0.538 | -/- | -/- | - | score 0.345 |
| 207 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.0/1.0 | 142.0/0.373 | 83.0/0.409 | -/- | -/- | - | score 0.345 |
| 208 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 0.85/1.5 | 418.0/0.374 | 309.0/0.413 | -/- | -/- | - | score 0.342 |
| 209 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.1/1.5 | 80.0/0.427 | 63.0/0.533 | -/- | -/- | - | score 0.342 |
| 210 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 0.85/1.5 | 601.0/0.36 | 441.0/0.384 | -/- | -/- | - | score 0.342 |
| 211 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.1/1.5 | 72.0/0.477 | 58.0/0.4 | -/- | -/- | - | score 0.339 |
| 212 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.0/1.0 | 57.0/0.383 | 18.0/0.442 | -/- | -/- | - | score 0.337 |
| 213 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.0/1.0 | 307.0/0.426 | 164.0/0.375 | -/- | -/- | - | score 0.334 |
| 214 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.1/1.5 | 103.0/0.537 | 75.0/0.424 | -/- | -/- | - | score 0.334 |
| 215 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.1/1.0 | 308.0/0.41 | 165.0/0.508 | -/- | -/- | - | score 0.332 |
| 216 | 6-rescue | R3-window-{"min_slot": "10:00"} | rescue variant | - -> - | 0.7/1.5 | 13.0/0.778 | 10.0/0.822 | 23.0/0.799 | -/- | reject | TRAIN out of band (PF 0.799, n 23) |
| 217 | 6-rescue | R2-drop-premom-1 | rescue variant | - -> - | 0.7/1.5 | 14.0/0.681 | 11.0/0.704 | 25.0/0.692 | -/- | reject | TRAIN out of band (PF 0.692, n 25) |
| 218 | 6-rescue | R3-window-{"min_slot": "10:00", "max_slot": "14:00"} | rescue variant | - -> - | 0.7/1.5 | 29.0/0.703 | 21.0/0.676 | 50.0/0.691 | -/- | reject | TRAIN out of band (PF 0.691, n 50) |
| 219 | 6-rescue | R1-premom-off | rescue variant | - -> - | 1.1/1.5 | 49.0/0.518 | 38.0/0.495 | 87.0/0.509 | -/- | reject | TRAIN out of band (PF 0.509, n 87) |
| 220 | 6-rescue | R2-drop-mask-0 | rescue variant | - -> - | 0.7/1.5 | 29.0/0.622 | 26.0/0.495 | 55.0/0.555 | -/- | reject | TRAIN out of band (PF 0.555, n 55) |
| 221 | 6-rescue | R2-drop-premom-0 | rescue variant | - -> - | 0.7/1.5 | 54.0/0.367 | 30.0/0.527 | 84.0/0.421 | -/- | reject | TRAIN out of band (PF 0.421, n 84) |
| 222 | 6-rescue | R3-window-{"max_slot": "12:00"} | rescue variant | - -> - | 0.7/1.5 | 9.0/0.537 | 7.0/0.227 | 16.0/0.369 | -/- | reject | TRAIN out of band (PF 0.369, n 16) |

## Top 40 stage-4 trials (by FIT/VAL band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|----|------|--------|-------|----------|----------|-------|
| 1 | 0.7 | 1.5 | regime==BEAR | pre3_close_pos<=0.5; pre_entry_momentum_score<=72.191184 | {"max_slot": "12:30", "top_n": 1} | 13/0.778 | 10/0.822 | 0.7432 |
| 2 | 0.7 | 1.5 | regime==BEAR | pre3_close_pos<=0.5; pre_entry_momentum_score<=72.191184 | {"max_slot": "12:30", "top_n": 1} | 13/0.778 | 10/0.822 | 0.7432 |
| 3 | 0.7 | 1.5 | regime==BEAR | pre3_close_pos<=0.5; pre_entry_momentum_score<=72.191184 | {"min_slot": "09:45", "max_slot": "12:30", "top_n": 1} | 13/0.778 | 10/0.822 | 0.7432 |
| 4 | 0.7 | 1.5 | regime==BEAR | pre3_close_pos<=0.5; pre_entry_momentum_score<=72.191184 | {"min_slot": "09:45", "max_slot": "12:30", "top_n": 1} | 13/0.778 | 10/0.822 | 0.7432 |
| 5 | 1.0 | 1.5 | regime==BEAR | pre3_close_pos<=0.5; pre3_range_r<=0.916927 | {"min_slot": "11:00", "top_n": 3} | 50/0.686 | 40/0.699 | 0.6761 |
| 6 | 0.7 | 1.5 | regime==BEAR | pre3_close_pos<=0.5; pre_entry_momentum_score<=72.191184 | {"max_slot": "12:30", "top_n": 3} | 15/0.619 | 17/0.612 | 0.607 |
| 7 | 0.7 | 1.5 | regime==BEAR | pre3_close_pos<=0.5; pre_entry_momentum_score<=72.191184 | {"min_slot": "10:30", "max_slot": "12:30", "top_n": 3} | 15/0.619 | 17/0.612 | 0.607 |
| 8 | 0.7 | 1.5 | regime==BEAR | pre3_close_pos<=0.5; pre_entry_momentum_score<=69.208887 | {"max_slot": "12:30", "top_n": 3} | 14/0.625 | 17/0.612 | 0.6024 |
| 9 | 0.7 | 1.5 | regime==BEAR | pre3_close_pos<=0.5; sig5_adx_calc<=44.580836 | {"max_slot": "12:30"} | 15/0.619 | 14/0.604 | 0.5914 |
| 10 | 1.2 | 0.8 | regime==BEAR | pre3_close_pos<=0.5; pre1_adx<=59.79734 | {"top_n": 3} | 45/0.577 | 34/0.583 | 0.573 |
| 11 | 0.7 | 1.5 | regime==BEAR | pre3_close_pos<=0.5; pre3_close_pos<=0.846175 | {"max_slot": "12:30", "top_n": 3} | 16/0.556 | 18/0.561 | 0.5519 |
| 12 | 0.7 | 1.5 | regime==BEAR | pre3_close_pos<=0.5; pre_entry_momentum_score<=79.425561 | {"max_slot": "12:30", "top_n": 3} | 16/0.556 | 18/0.561 | 0.5519 |
| 13 | 0.7 | 1.5 | regime==BEAR | pre3_close_pos<=0.5; pre_entry_momentum_score<=79.425561 | {"max_slot": "12:30", "top_n": 3} | 16/0.556 | 18/0.561 | 0.5519 |
| 14 | 1.5 | 1.5 | atr_pct>=0.00205; regime==BEAR | pre3_close_pos<=0.5; pre_entry_momentum_score<=72.191184 | {"max_slot": "12:30", "top_n": 3} | 13/0.644 | 17/0.763 | 0.5495 |
| 15 | 0.7 | 1.5 | regime==BEAR | pre3_close_pos<=0.5; sig5_adx_calc<=30.819969 | {"top_n": 1} | 28/0.631 | 15/0.734 | 0.5482 |
| 16 | 1.2 | 1.5 | regime==BEAR | pre3_close_pos<=0.5; pre3_range_r<=0.916927 | {"min_slot": "11:00", "top_n": 3} | 51/0.621 | 40/0.718 | 0.5443 |
| 17 | 1.2 | 2.5 | regime==BEAR | pre3_close_pos<=0.5; pre3_range_r<=0.364407 | {"min_slot": "11:00", "top_n": 3} | 41/0.587 | 32/0.65 | 0.5364 |
| 18 | 0.7 | 1.0 | regime==BEAR | pre1_adx<=30.634255; pre_entry_momentum_score<=69.208887 | {"max_slot": "14:00", "top_n": 3} | 24/0.539 | 14/0.549 | 0.5311 |
| 19 | 1.2 | 0.8 | regime==BEAR | pre3_close_pos<=0.5; pre3_range_r<=0.916927 | {"top_n": 3} | 51/0.536 | 41/0.554 | 0.5221 |
| 20 | 1.2 | 2.5 | regime==BEAR | pre3_close_pos<=0.5; pre3_range_r<=0.364407 | {"min_slot": "10:00", "top_n": 3} | 41/0.587 | 33/0.67 | 0.521 |
| 21 | 1.2 | 2.5 | regime==BEAR | pre3_close_pos<=0.5; pre3_range_r<=0.364407 | {"min_slot": "09:30", "top_n": 3} | 41/0.587 | 33/0.67 | 0.521 |
| 22 | 0.7 | 2.5 | regime==BEAR | pre3_close_pos<=0.5; pre_entry_momentum_score<=72.191184 | {"top_n": 3} | 49/0.538 | 41/0.576 | 0.5064 |
| 23 | 0.7 | 1.0 | regime==BEAR | pre3_close_pos<=0.5; sig5_adx_calc<=37.125226 | {"max_slot": "12:30"} | 14/0.532 | 14/0.57 | 0.5025 |
| 24 | 0.5 | 1.5 | regime==BEAR | pre3_close_pos<=0.5; pre_entry_momentum_score<=69.208887 | {"max_slot": "12:30"} | 13/0.786 | 16/0.628 | 0.5014 |
| 25 | 1.1 | 1.5 | regime==BEAR | pre3_close_pos<=0.5; pre1_adx<=59.79734 | {"top_n": 1} | 32/0.679 | 24/0.574 | 0.4901 |
| 26 | 0.5 | 1.5 | regime==BEAR | pre3_close_pos<=0.5; pre3_close_pos<=0.926834 | {"min_slot": "09:30", "max_slot": "12:30", "top_n": 3} | 16/0.587 | 18/0.532 | 0.4882 |
| 27 | 1.1 | 1.5 | regime==BEAR | pre3_close_pos<=0.600013; pre1_adx>=37.594019 | {"max_slot": "12:30", "top_n": 1} | 17/0.514 | 10/0.549 | 0.4866 |
| 28 | 1.2 | 0.8 | regime==BEAR | pre3_close_pos<=0.5; pre3_close_pos<=1.0 | {"top_n": 3} | 52/0.56 | 42/0.518 | 0.4847 |
| 29 | 1.0 | 1.5 | regime==BEAR | pre3_close_pos<=0.5; pre_entry_momentum_score<=72.191184 | {"min_slot": "09:45", "max_slot": "12:30", "top_n": 1} | 14/0.765 | 10/1.119 | 0.4812 |
| 30 | 1.2 | 2.0 | regime==BEAR | pre3_close_pos<=0.5; pre3_range_r<=0.364407 | {"min_slot": "10:30", "top_n": 3} | 41/0.536 | 33/0.608 | 0.478 |
| 31 | 0.7 | 1.5 | (none) | pre3_close_pos<=0.5; pre_entry_momentum_score<=69.208887 | {"max_slot": "12:30", "top_n": 3} | 33/0.484 | 40/0.502 | 0.4708 |
| 32 | 0.7 | 1.5 | regime!=BULL | pre3_close_pos<=0.5; pre_entry_momentum_score<=72.191184 | {"max_slot": "12:30", "top_n": 3} | 34/0.483 | 40/0.502 | 0.4675 |
| 33 | 0.7 | 1.5 | (none) | pre3_close_pos<=0.5; pre_entry_momentum_score<=72.191184 | {"max_slot": "12:30", "top_n": 3} | 34/0.483 | 40/0.502 | 0.4675 |
| 34 | 0.5 | 1.5 | regime!=BULL | pre3_close_pos<=0.5; pre3_range_r<=0.524174 | {"max_slot": "12:30", "top_n": 3} | 23/0.486 | 20/0.475 | 0.4657 |
| 35 | 0.7 | 1.5 | regime!=TREND | pre3_close_pos<=0.5; pre_entry_momentum_score<=72.191184 | {"max_slot": "12:30", "top_n": 1} | 28/0.539 | 26/0.495 | 0.4604 |
| 36 | 0.7 | 1.0 | regime==BEAR | pre3_close_pos<=0.5; pre_entry_momentum_score<=72.191184 | {"max_slot": "12:30", "top_n": 2} | 15/0.471 | 16/0.494 | 0.453 |
| 37 | 1.0 | 1.5 | vol_ratio>=4.104685; regime!=BULL | pre5_mom_r<=0.45218 | {"min_slot": "11:00", "top_n": 2} | 66/0.491 | 85/0.541 | 0.451 |
| 38 | 1.0 | 0.6 | (none) | pre3_close_pos<=0.5; pre_entry_momentum_score<=72.191184 | {"min_slot": "09:45", "max_slot": "12:30", "top_n": 1} | 30/0.582 | 26/0.506 | 0.4443 |
| 39 | 1.0 | 1.5 | regime!=BULL | pre5_mom_r<=0.144913 | {"min_slot": "12:00", "top_n": 2} | 67/0.483 | 101/0.46 | 0.4415 |
| 40 | 1.0 | 1.5 | regime!=BULL | pre3_close_pos<=0.5; pre3_range_r<=0.916927 | {"min_slot": "11:00", "top_n": 3} | 95/0.549 | 112/0.489 | 0.4413 |