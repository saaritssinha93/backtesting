# B_HUGE_C1_CLOSE_RECLAIM_BREAK (LONG) — ITERATION_LOG

_Generated 2026-07-03. Research-only; NO live trades; NO final_setup_conf.py edits._

Optimizer: Optuna TPE. Protocol: search ONLY on FIT/VAL (band objective, tent at PF 1.80, gap penalty); confirm on full TRAIN; TEST scored ONCE per finalist whose TRAIN lands in [1.30,1.80]; TEST evaluations budget-capped (3 used).

- Stage 1 baseline: 1 iteration
- Stage 3 single-knob sweeps: 150 iterations (see PARAMETER_SWEEP_SUMMARY.md)
- Stage 4 combination search: 500 trials (258 unique configs; full list in trials.csv)
- Stage 5/6 finalist + rescue confirmations: 52 iterations

## Full per-iteration log (baseline, sweeps, finalists, rescues)

Complete row-level log: `iteration_log.csv` (every iteration: stage, group, change, old/new, FIT/VAL/TRAIN/TEST metrics, exit counts, keep/reject + why + next action). Key iterations below.

| # | stage | group | change | old -> new | SL/Tgt | FIT n/PF | VAL n/PF | TRAIN n/PF | TEST n/PF | keep | why |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 1-baseline | baseline | current conf config | - -> - | 1.0/1.5 | 416.0/0.519 | 327.0/0.458 | 743.0/0.492 | 293.0/0.475 | baseline | no_train_edge,overtrading,train_concentration,no_edge_anywhere,test_concentration,test_day |
| 3 | 3-sweep | exit | sl_pct | 1.0 -> 0.7 | 0.7/1.5 | 416.0/0.446 | 339.0/0.473 | -/- | -/- | improve | band score 0.424 vs baseline 0.409 |
| 4 | 3-sweep | exit | sl_pct | 1.0 -> 0.85 | 0.85/1.5 | 416.0/0.458 | 336.0/0.452 | -/- | -/- | improve | band score 0.447 vs baseline 0.409 |
| 5 | 3-sweep | exit | sl_pct | 1.0 -> 1.1 | 1.1/1.5 | 415.0/0.523 | 325.0/0.462 | -/- | -/- | improve | band score 0.413 vs baseline 0.409 |
| 6 | 3-sweep | exit | sl_pct | 1.0 -> 1.2 | 1.2/1.5 | 412.0/0.534 | 327.0/0.471 | -/- | -/- | improve | band score 0.421 vs baseline 0.409 |
| 12 | 3-sweep | exit | tgt_pct | 1.5 -> 2.0 | 1.0/2.0 | 415.0/0.592 | 327.0/0.492 | -/- | -/- | improve | band score 0.412 vs baseline 0.409 |
| 15 | 3-sweep | indicator/price-action | +mask atr_pct>= | - -> 0.002269 (q0.2) | 1.0/1.5 | 358.0/0.566 | 266.0/0.513 | -/- | -/- | improve | band score 0.471 vs baseline 0.409 |
| 17 | 3-sweep | indicator/price-action | +mask atr_pct>= | - -> 0.003142 (q0.5) | 1.0/1.5 | 243.0/0.565 | 162.0/0.578 | -/- | -/- | improve | band score 0.555 vs baseline 0.409 |
| 21 | 3-sweep | indicator/price-action | +mask body_pct>= | - -> 0.607298 (q0.2) | 1.0/1.5 | 326.0/0.499 | 262.0/0.506 | -/- | -/- | improve | band score 0.493 vs baseline 0.409 |
| 27 | 3-sweep | indicator/price-action | +mask close_loc>= | - -> 0.731404 (q0.2) | 1.0/1.5 | 328.0/0.48 | 271.0/0.479 | -/- | -/- | improve | band score 0.478 vs baseline 0.409 |
| 32 | 3-sweep | indicator/price-action | +mask close_loc<= | - -> 0.994484 (q0.8) | 1.0/1.5 | 341.0/0.538 | 270.0/0.481 | -/- | -/- | improve | band score 0.435 vs baseline 0.409 |
| 34 | 3-sweep | indicator/price-action | +mask lower_wick_pct<= | - -> 0.0 (q0.2) | 1.0/1.5 | 119.0/0.525 | 98.0/0.575 | -/- | -/- | improve | band score 0.485 vs baseline 0.409 |
| 36 | 3-sweep | indicator/price-action | +mask lower_wick_pct<= | - -> 0.045548 (q0.5) | 1.0/1.5 | 181.0/0.499 | 162.0/0.481 | -/- | -/- | improve | band score 0.467 vs baseline 0.409 |
| 38 | 3-sweep | indicator/price-action | +mask lower_wick_pct<= | - -> 0.143311 (q0.8) | 1.0/1.5 | 318.0/0.512 | 267.0/0.475 | -/- | -/- | improve | band score 0.445 vs baseline 0.409 |
| 39 | 3-sweep | indicator/price-action | +mask quality_score>= | - -> 41.402619 (q0.2) | 1.0/1.5 | 313.0/0.501 | 221.0/0.503 | -/- | -/- | improve | band score 0.499 vs baseline 0.409 |
| 41 | 3-sweep | indicator/price-action | +mask quality_score>= | - -> 65.946217 (q0.5) | 1.0/1.5 | 221.0/0.511 | 135.0/0.626 | -/- | -/- | improve | band score 0.419 vs baseline 0.409 |
| 45 | 3-sweep | indicator/price-action | +mask rs_pct>= | - -> -0.956105 (q0.2) | 1.0/1.5 | 371.0/0.521 | 283.0/0.488 | -/- | -/- | improve | band score 0.462 vs baseline 0.409 |
| 47 | 3-sweep | indicator/price-action | +mask rs_pct>= | - -> 0.047621 (q0.5) | 1.0/1.5 | 272.0/0.51 | 191.0/0.541 | -/- | -/- | improve | band score 0.485 vs baseline 0.409 |
| 51 | 3-sweep | indicator/price-action | +mask signal_range_pct>= | - -> 0.407011 (q0.2) | 1.0/1.5 | 364.0/0.504 | 279.0/0.487 | -/- | -/- | improve | band score 0.473 vs baseline 0.409 |
| 53 | 3-sweep | indicator/price-action | +mask signal_range_pct>= | - -> 0.68784 (q0.5) | 1.0/1.5 | 253.0/0.495 | 180.0/0.526 | -/- | -/- | improve | band score 0.470 vs baseline 0.409 |
| 57 | 3-sweep | indicator/price-action | +mask upper_wick_pct>= | - -> 0.004202 (q0.2) | 1.0/1.5 | 341.0/0.538 | 270.0/0.481 | -/- | -/- | improve | band score 0.435 vs baseline 0.409 |
| 59 | 3-sweep | indicator/price-action | +mask upper_wick_pct>= | - -> 0.079827 (q0.5) | 1.0/1.5 | 230.0/0.543 | 187.0/0.517 | -/- | -/- | improve | band score 0.496 vs baseline 0.409 |
| 61 | 3-sweep | indicator/price-action | +mask upper_wick_pct>= | - -> 0.202498 (q0.8) | 1.0/1.5 | 107.0/0.575 | 74.0/0.527 | -/- | -/- | improve | band score 0.489 vs baseline 0.409 |
| 65 | 3-sweep | indicator/price-action | +mask vol_ratio>= | - -> 3.564622 (q0.5) | 1.0/1.5 | 221.0/0.572 | 190.0/0.525 | -/- | -/- | improve | band score 0.487 vs baseline 0.409 |
| 69 | 3-sweep | indicator/price-action | +mask vwap_dist_atr>= | - -> 1.638384 (q0.2) | 1.0/1.5 | 351.0/0.534 | 266.0/0.489 | -/- | -/- | improve | band score 0.453 vs baseline 0.409 |
| 71 | 3-sweep | indicator/price-action | +mask vwap_dist_atr>= | - -> 3.389904 (q0.5) | 1.0/1.5 | 222.0/0.481 | 162.0/0.568 | -/- | -/- | improve | band score 0.411 vs baseline 0.409 |
| 75 | 3-sweep | indicator/price-action | +mask wick_skew_pct>= | - -> -0.063872 (q0.2) | 1.0/1.5 | 323.0/0.499 | 270.0/0.45 | -/- | -/- | improve | band score 0.411 vs baseline 0.409 |
| 76 | 3-sweep | indicator/price-action | +mask wick_skew_pct<= | - -> -0.063872 (q0.2) | 1.0/1.5 | 93.0/0.586 | 68.0/0.488 | -/- | -/- | improve | band score 0.410 vs baseline 0.409 |
| 77 | 3-sweep | indicator/price-action | +mask wick_skew_pct>= | - -> 0.022096 (q0.5) | 1.0/1.5 | 214.0/0.525 | 182.0/0.53 | -/- | -/- | improve | band score 0.521 vs baseline 0.409 |
| 80 | 3-sweep | indicator/price-action | +mask wick_skew_pct<= | - -> 0.145691 (q0.8) | 1.0/1.5 | 318.0/0.491 | 260.0/0.452 | -/- | -/- | improve | band score 0.421 vs baseline 0.409 |
| 82 | 3-sweep | regime | +mask regime==NEUTRAL | - -> NEUTRAL | 1.0/1.5 | 295.0/0.489 | 327.0/0.458 | -/- | -/- | improve | band score 0.433 vs baseline 0.409 |
| 83 | 3-sweep | regime | +mask regime!=TREND | - -> TREND | 1.0/1.5 | 295.0/0.489 | 327.0/0.458 | -/- | -/- | improve | band score 0.433 vs baseline 0.409 |
| 85 | 3-sweep | pre-momentum | +premom pre1_adx>= | - -> 31.180706 (q0.2) | 1.0/1.5 | 350.0/0.528 | 277.0/0.527 | -/- | -/- | improve | band score 0.526 vs baseline 0.409 |
| 87 | 3-sweep | pre-momentum | +premom pre1_adx>= | - -> 40.376865 (q0.5) | 1.0/1.5 | 221.0/0.482 | 181.0/0.564 | -/- | -/- | improve | band score 0.416 vs baseline 0.409 |
| 91 | 3-sweep | pre-momentum | +premom pre3_close_pos>= | - -> 0.39351 (q0.2) | 1.0/1.5 | 354.0/0.555 | 262.0/0.483 | -/- | -/- | improve | band score 0.425 vs baseline 0.409 |
| 93 | 3-sweep | pre-momentum | +premom pre3_close_pos>= | - -> 0.757576 (q0.5) | 1.0/1.5 | 212.0/0.511 | 153.0/0.486 | -/- | -/- | improve | band score 0.466 vs baseline 0.409 |
| 94 | 3-sweep | pre-momentum | +premom pre3_close_pos<= | - -> 0.757576 (q0.5) | 1.0/1.5 | 211.0/0.518 | 195.0/0.465 | -/- | -/- | improve | band score 0.423 vs baseline 0.409 |
| 95 | 3-sweep | pre-momentum | +premom pre3_close_pos>= | - -> 0.971534 (q0.8) | 1.0/1.5 | 91.0/0.483 | 60.0/0.53 | -/- | -/- | improve | band score 0.445 vs baseline 0.409 |
| 96 | 3-sweep | pre-momentum | +premom pre3_close_pos<= | - -> 0.971534 (q0.8) | 1.0/1.5 | 331.0/0.526 | 276.0/0.467 | -/- | -/- | improve | band score 0.420 vs baseline 0.409 |
| 99 | 3-sweep | pre-momentum | +premom pre3_range_r>= | - -> 0.448233 (q0.5) | 1.0/1.5 | 223.0/0.547 | 139.0/0.542 | -/- | -/- | improve | band score 0.538 vs baseline 0.409 |
| 103 | 3-sweep | pre-momentum | +premom pre5_mom_r>= | - -> 0.047074 (q0.2) | 1.0/1.5 | 375.0/0.554 | 239.0/0.523 | -/- | -/- | improve | band score 0.498 vs baseline 0.409 |
| 109 | 3-sweep | pre-momentum | +premom pre_entry_momentum_score>= | - -> 50.998995 (q0.2) | 1.0/1.5 | 364.0/0.549 | 246.0/0.493 | -/- | -/- | improve | band score 0.448 vs baseline 0.409 |
| 111 | 3-sweep | pre-momentum | +premom pre_entry_momentum_score>= | - -> 67.057674 (q0.5) | 1.0/1.5 | 234.0/0.486 | 141.0/0.522 | -/- | -/- | improve | band score 0.457 vs baseline 0.409 |
| 115 | 3-sweep | pre-momentum | +premom sig5_adx_calc>= | - -> 18.39577 (q0.2) | 1.0/1.5 | 349.0/0.48 | 278.0/0.475 | -/- | -/- | improve | band score 0.471 vs baseline 0.409 |
| 121 | 3-sweep | pre-momentum | +premom sig5_rsi_dir>= | - -> 63.975048 (q0.2) | 1.0/1.5 | 344.0/0.524 | 258.0/0.478 | -/- | -/- | improve | band score 0.441 vs baseline 0.409 |
| 123 | 3-sweep | pre-momentum | +premom sig5_rsi_dir>= | - -> 70.827164 (q0.5) | 1.0/1.5 | 240.0/0.535 | 174.0/0.511 | -/- | -/- | improve | band score 0.492 vs baseline 0.409 |
| 129 | 3-sweep | pre-momentum | +premom sig5_vol_ratio20>= | - -> 3.496134 (q0.5) | 1.0/1.5 | 225.0/0.556 | 190.0/0.542 | -/- | -/- | improve | band score 0.531 vs baseline 0.409 |
| 137 | 3-sweep | guard | min_slot | - -> 11:00 | 1.0/1.5 | 415.0/0.514 | 327.0/0.458 | -/- | -/- | improve | band score 0.413 vs baseline 0.409 |
| 138 | 3-sweep | guard | min_slot | - -> 12:00 | 1.0/1.5 | 289.0/0.559 | 256.0/0.484 | -/- | -/- | improve | band score 0.424 vs baseline 0.409 |
| 140 | 3-sweep | guard | max_slot | - -> 12:00 | 1.0/1.5 | 143.0/0.525 | 96.0/0.461 | -/- | -/- | improve | band score 0.410 vs baseline 0.409 |
| 142 | 3-sweep | guard | max_slot | - -> 13:00 | 1.0/1.5 | 245.0/0.553 | 210.0/0.505 | -/- | -/- | improve | band score 0.467 vs baseline 0.409 |
| 143 | 3-sweep | guard | max_slot | - -> 14:00 | 1.0/1.5 | 354.0/0.554 | 277.0/0.475 | -/- | -/- | improve | band score 0.412 vs baseline 0.409 |
| 145 | 3-sweep | guard | top_n | - -> 1 | 1.0/1.5 | 256.0/0.497 | 213.0/0.49 | -/- | -/- | improve | band score 0.484 vs baseline 0.409 |
| 146 | 3-sweep | guard | top_n | - -> 2 | 1.0/1.5 | 363.0/0.533 | 296.0/0.472 | -/- | -/- | improve | band score 0.423 vs baseline 0.409 |
| 147 | 3-sweep | guard | top_n | - -> 3 | 1.0/1.5 | 402.0/0.525 | 319.0/0.466 | -/- | -/- | improve | band score 0.419 vs baseline 0.409 |
| 149 | 3-sweep | guard | daily_loss_rs | 0.0 -> 4000.0 | 1.0/1.5 | 340.0/0.524 | 276.0/0.473 | -/- | -/- | improve | band score 0.432 vs baseline 0.409 |
| 150 | 3-sweep | guard | max_positions | 20 -> 5 | 1.0/1.5 | 233.0/0.483 | 159.0/0.481 | -/- | -/- | improve | band score 0.479 vs baseline 0.409 |
| 152 | 5-finalist | combination | finalist #1 | - -> - | 0.7/1.5 | -/- | -/- | 21.0/1.698 | 3.0/0.166 | reject | TRAIN domination (trade>35% gross or day/sym>40% net); TEST n 3 < 5 |
| 153 | 5-finalist | combination | finalist #2 | - -> - | 0.7/1.25 | -/- | -/- | 21.0/1.371 | 3.0/0.166 | reject | TRAIN domination (trade>35% gross or day/sym>40% net); TEST n 3 < 5; threshold-neighborhoo |
| 154 | 5-finalist | combination | finalist #3 | - -> - | 1.1/1.25 | -/- | -/- | 21.0/1.287 | -/- | reject | TRAIN not in band or too thin (PF 1.287, n 21) |
| 155 | 5-finalist | combination | finalist #4 | - -> - | 1.2/1.25 | -/- | -/- | 21.0/1.201 | -/- | reject | TRAIN not in band or too thin (PF 1.201, n 21) |
| 156 | 5-finalist | combination | finalist #5 | - -> - | 0.7/1.5 | -/- | -/- | 28.0/1.122 | -/- | reject | TRAIN not in band or too thin (PF 1.122, n 28) |
| 157 | 5-finalist | combination | finalist #6 | - -> - | 1.0/1.5 | -/- | -/- | 28.0/1.158 | -/- | reject | TRAIN not in band or too thin (PF 1.158, n 28) |
| 158 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.5/2.5 | 15.0/0.724 | 21.0/0.724 | -/- | -/- | - | score 0.724 |
| 159 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.5/2.5 | 17.0/0.641 | 31.0/0.642 | -/- | -/- | - | score 0.641 |
| 160 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.5/2.5 | 17.0/0.641 | 31.0/0.642 | -/- | -/- | - | score 0.641 |
| 161 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.5/2.5 | 17.0/0.641 | 31.0/0.642 | -/- | -/- | - | score 0.641 |
| 162 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.5/2.5 | 84.0/0.597 | 78.0/0.588 | -/- | -/- | - | score 0.581 |
| 163 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.5/2.5 | 114.0/0.622 | 127.0/0.597 | -/- | -/- | - | score 0.578 |
| 164 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.5/2.5 | 17.0/0.641 | 31.0/0.605 | -/- | -/- | - | score 0.576 |
| 165 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.5/2.5 | 32.0/0.61 | 33.0/0.677 | -/- | -/- | - | score 0.557 |
| 166 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.2/2.5 | 17.0/0.72 | 31.0/0.623 | -/- | -/- | - | score 0.546 |
| 167 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.5/2.5 | 136.0/0.59 | 155.0/0.56 | -/- | -/- | - | score 0.536 |
| 168 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.5/2.5 | 141.0/0.56 | 91.0/0.544 | -/- | -/- | - | score 0.531 |
| 169 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.5/2.0 | 192.0/0.534 | 211.0/0.55 | -/- | -/- | - | score 0.521 |
| 170 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.0/1.25 | 138.0/0.53 | 155.0/0.544 | -/- | -/- | - | score 0.518 |
| 171 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.5/2.0 | 17.0/0.558 | 31.0/0.609 | -/- | -/- | - | score 0.517 |
| 172 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.5/2.5 | 105.0/0.525 | 88.0/0.512 | -/- | -/- | - | score 0.502 |
| 173 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.0/2.5 | 17.0/0.553 | 31.0/0.635 | -/- | -/- | - | score 0.488 |
| 174 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.5/2.5 | 141.0/0.607 | 155.0/0.54 | -/- | -/- | - | score 0.487 |
| 175 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 0.85/2.0 | 221.0/0.512 | 192.0/0.497 | -/- | -/- | - | score 0.485 |
| 176 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.5/2.5 | 125.0/0.493 | 77.0/0.51 | -/- | -/- | - | score 0.479 |
| 177 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 0.85/1.5 | 141.0/0.498 | 152.0/0.534 | -/- | -/- | - | score 0.469 |
| 178 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.5/2.0 | 97.0/0.558 | 63.0/0.675 | -/- | -/- | - | score 0.464 |
| 179 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 0.5/1.0 | 14.0/0.617 | 33.0/0.53 | -/- | -/- | - | score 0.460 |
| 180 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 0.85/1.5 | 285.0/0.452 | 327.0/0.451 | -/- | -/- | - | score 0.450 |
| 181 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 0.7/2.5 | 264.0/0.458 | 200.0/0.47 | -/- | -/- | - | score 0.448 |
| 182 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.2/1.5 | 318.0/0.527 | 242.0/0.482 | -/- | -/- | - | score 0.447 |
| 183 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.5/2.5 | 112.0/0.509 | 75.0/0.474 | -/- | -/- | - | score 0.446 |
| 184 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.0/1.5 | 224.0/0.536 | 236.0/0.486 | -/- | -/- | - | score 0.445 |
| 185 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.1/1.0 | 142.0/0.537 | 167.0/0.486 | -/- | -/- | - | score 0.445 |
| 186 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.5/2.5 | 36.0/0.598 | 38.0/0.511 | -/- | -/- | - | score 0.441 |
| 187 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.5/2.5 | 163.0/0.542 | 117.0/0.486 | -/- | -/- | - | score 0.441 |
| 188 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.5/2.5 | 176.0/0.466 | 166.0/0.499 | -/- | -/- | - | score 0.438 |
| 189 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.5/1.25 | 84.0/0.482 | 85.0/0.456 | -/- | -/- | - | score 0.435 |
| 190 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.5/2.5 | 116.0/0.463 | 94.0/0.447 | -/- | -/- | - | score 0.435 |
| 191 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 0.7/2.0 | 246.0/0.526 | 150.0/0.474 | -/- | -/- | - | score 0.433 |
| 192 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.5/2.5 | 151.0/0.63 | 166.0/0.519 | -/- | -/- | - | score 0.431 |
| 193 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.0/1.0 | 94.0/0.493 | 85.0/0.571 | -/- | -/- | - | score 0.430 |
| 194 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.5/2.5 | 54.0/0.466 | 48.0/0.514 | -/- | -/- | - | score 0.427 |
| 195 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.1/2.5 | 104.0/0.451 | 74.0/0.436 | -/- | -/- | - | score 0.424 |
| 196 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.5/2.5 | 286.0/0.588 | 232.0/0.496 | -/- | -/- | - | score 0.423 |
| 197 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.5/2.5 | 76.0/0.779 | 81.0/0.58 | -/- | -/- | - | score 0.421 |
| 198 | 6-rescue | R3-window-{"max_slot": "12:00"} | rescue variant | - -> - | 0.7/1.5 | 11.0/1.629 | 10.0/1.785 | 21.0/1.698 | 3.0/0.166 | reject | TRAIN domination (trade>35% gross or day/sym>40% net); TEST n 3 < 5 |
| 199 | 6-rescue | R2-drop-mask-0 | rescue variant | - -> - | 0.7/1.5 | 14.0/1.041 | 14.0/1.213 | 28.0/1.122 | -/- | reject | TRAIN out of band (PF 1.122, n 28) |
| 200 | 6-rescue | R2-drop-mask-1 | rescue variant | - -> - | 0.7/1.5 | 20.0/1.22 | 18.0/0.982 | 38.0/1.101 | -/- | reject | TRAIN out of band (PF 1.101, n 38) |
| 201 | 6-rescue | R1-premom-off | rescue variant | - -> - | 1.5/2.5 | 15.0/0.724 | 21.0/0.724 | 36.0/0.724 | -/- | reject | TRAIN out of band (PF 0.724, n 36) |
| 202 | 6-rescue | R3-window-{"min_slot": "10:00", "max_slot": "14:00"} | rescue variant | - -> - | 0.7/1.5 | 249.0/0.541 | 203.0/0.552 | 452.0/0.546 | -/- | reject | TRAIN out of band (PF 0.546, n 452) |
| 203 | 6-rescue | R3-window-{"min_slot": "10:00"} | rescue variant | - -> - | 0.7/1.5 | 100.0/0.561 | 65.0/0.659 | 165.0/0.598 | -/- | reject | TRAIN out of band (PF 0.598, n 165) |

## Top 40 stage-4 trials (by FIT/VAL band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|----|------|--------|-------|----------|----------|-------|
| 1 | 0.7 | 1.5 | vol_ratio>=2.63937; regime!=BULL | (none) | {"min_slot": "12:00", "max_slot": "12:00"} | 11/1.629 | 10/1.785 | 1.5336 |
| 2 | 0.7 | 1.25 | vol_ratio>=2.63937; regime!=BULL | (none) | {"min_slot": "12:00", "max_slot": "12:00"} | 11/1.307 | 10/1.451 | 1.2224 |
| 3 | 0.7 | 1.25 | vol_ratio>=2.63937; regime!=BULL | (none) | {"min_slot": "12:00", "max_slot": "12:00"} | 11/1.307 | 10/1.451 | 1.2224 |
| 4 | 1.1 | 1.25 | vol_ratio>=2.63937; regime!=BULL | (none) | {"min_slot": "12:00", "max_slot": "12:00"} | 11/1.332 | 10/1.233 | 1.1545 |
| 5 | 1.2 | 1.25 | vol_ratio>=2.63937; regime!=BULL | (none) | {"min_slot": "12:00", "max_slot": "12:00"} | 11/1.239 | 10/1.154 | 1.087 |
| 6 | 1.2 | 1.25 | vol_ratio>=2.63937; regime!=BULL | (none) | {"min_slot": "12:00", "max_slot": "12:00"} | 11/1.239 | 10/1.154 | 1.087 |
| 7 | 0.7 | 1.5 | regime!=BULL | (none) | {"min_slot": "12:00", "max_slot": "12:00"} | 14/1.041 | 14/1.213 | 0.9037 |
| 8 | 1.0 | 1.5 | regime!=BULL | (none) | {"min_slot": "12:00", "max_slot": "12:00"} | 14/1.264 | 14/1.058 | 0.8934 |
| 9 | 0.7 | 1.0 | vol_ratio>=2.63937; regime!=BULL | (none) | {"min_slot": "12:00", "max_slot": "12:00"} | 11/0.986 | 10/1.117 | 0.8811 |
| 10 | 0.5 | 1.5 | regime!=BULL | (none) | {"min_slot": "12:00", "max_slot": "12:00"} | 14/0.978 | 14/1.136 | 0.8521 |
| 11 | 1.1 | 1.5 | regime!=BULL | (none) | {"min_slot": "12:00", "max_slot": "12:00"} | 14/1.18 | 14/0.992 | 0.8415 |
| 12 | 0.7 | 2.0 | regime!=BULL | (none) | {"min_slot": "12:00", "max_slot": "12:00"} | 14/0.841 | 14/0.88 | 0.8097 |
| 13 | 0.7 | 1.25 | vol_ratio>=2.247093; regime!=BULL | (none) | {"min_slot": "12:00", "max_slot": "12:00"} | 13/0.958 | 13/1.148 | 0.8052 |
| 14 | 0.7 | 1.25 | vwap_dist_atr>=2.256335; regime!=BULL | (none) | {"min_slot": "12:00", "max_slot": "12:00"} | 13/0.958 | 11/1.16 | 0.796 |
| 15 | 0.7 | 1.25 | vwap_dist_atr>=2.256335; regime!=BULL | (none) | {"min_slot": "12:00", "max_slot": "12:00"} | 13/0.958 | 11/1.16 | 0.796 |
| 16 | 1.2 | 1.5 | regime!=BULL | (none) | {"min_slot": "12:00", "max_slot": "12:00"} | 14/1.106 | 14/0.933 | 0.7953 |
| 17 | 0.7 | 1.5 | (none) | (none) | {"min_slot": "12:00", "max_slot": "12:00"} | 28/0.828 | 24/0.878 | 0.7884 |
| 18 | 0.7 | 1.5 | regime!=BEAR | (none) | {"min_slot": "12:00", "max_slot": "12:00"} | 28/0.828 | 24/0.878 | 0.7884 |
| 19 | 1.0 | 1.5 | regime!=BULL | (none) | {"min_slot": "12:00", "max_slot": "12:00", "top_n": 3} | 12/0.904 | 14/1.058 | 0.7809 |
| 20 | 0.7 | 1.5 | regime!=TREND | (none) | {"min_slot": "12:00", "max_slot": "12:00"} | 25/1.001 | 24/0.878 | 0.7795 |
| 21 | 0.85 | 2.5 | regime!=BULL | (none) | {"min_slot": "12:00", "max_slot": "12:00", "top_n": 3} | 12/0.774 | 14/0.783 | 0.7678 |
| 22 | 0.85 | 2.0 | regime!=BULL | (none) | {"min_slot": "12:00", "max_slot": "12:00"} | 14/0.788 | 14/0.816 | 0.766 |
| 23 | 1.1 | 1.5 | (none) | (none) | {"min_slot": "12:00", "max_slot": "12:00"} | 28/1.088 | 24/0.898 | 0.7453 |
| 24 | 0.7 | 1.0 | regime!=BULL | (none) | {"min_slot": "12:00", "max_slot": "12:00"} | 14/0.802 | 14/0.755 | 0.7178 |
| 25 | 0.85 | 2.5 | regime==NEUTRAL | (none) | {"min_slot": "12:00", "max_slot": "13:00", "top_n": 2} | 62/0.744 | 120/0.726 | 0.7112 |
| 26 | 1.2 | 1.25 | vwap_dist_atr>=2.256335; regime!=BULL | (none) | {"min_slot": "12:00", "max_slot": "12:00"} | 13/1.11 | 11/0.884 | 0.7029 |
| 27 | 1.5 | 1.5 | regime!=BULL | (none) | {"min_slot": "12:00", "max_slot": "12:00"} | 14/0.932 | 14/0.793 | 0.6826 |
| 28 | 0.7 | 2.0 | regime!=BEAR | (none) | {"min_slot": "12:00", "max_slot": "12:00"} | 28/0.711 | 24/0.684 | 0.6621 |
| 29 | 0.7 | 1.25 | vol_ratio>=2.63937 | (none) | {"min_slot": "12:00", "max_slot": "12:00"} | 20/0.981 | 18/0.796 | 0.6468 |
| 30 | 0.85 | 2.5 | regime!=BULL | (none) | {"min_slot": "12:00", "max_slot": "14:00"} | 204/0.642 | 165/0.633 | 0.6257 |
| 31 | 1.0 | 1.5 | (none) | (none) | {"min_slot": "12:00", "max_slot": "12:00"} | 28/0.983 | 24/0.781 | 0.6198 |
| 32 | 1.0 | 1.5 | regime!=BEAR | (none) | {"min_slot": "12:00", "max_slot": "12:00"} | 28/0.983 | 24/0.781 | 0.6198 |
| 33 | 1.0 | 0.8 | regime!=BULL | (none) | {"min_slot": "12:00", "max_slot": "12:00"} | 14/0.645 | 14/0.689 | 0.609 |
| 34 | 0.85 | 2.5 | regime!=BULL | (none) | {"min_slot": "12:00", "max_slot": "12:00"} | 14/1.011 | 14/0.783 | 0.5998 |
| 35 | 1.0 | 1.5 | atr_pct>=0.002269; regime!=BULL | (none) | {"min_slot": "12:00", "max_slot": "12:00"} | 13/1.534 | 13/1.012 | 0.5943 |
| 36 | 0.7 | 0.8 | vol_ratio>=2.63937; regime!=BULL | (none) | {"min_slot": "12:00", "max_slot": "12:00"} | 11/0.729 | 10/0.907 | 0.5862 |
| 37 | 0.7 | 2.0 | regime!=BULL | (none) | {"min_slot": "12:00", "max_slot": "12:30"} | 54/0.787 | 81/0.674 | 0.5831 |
| 38 | 0.7 | 0.8 | regime!=BULL | (none) | {"min_slot": "12:00", "max_slot": "12:00"} | 14/0.592 | 14/0.605 | 0.5825 |
| 39 | 0.7 | 1.25 | signal_range_pct>=0.513274; regime!=BULL | (none) | {"min_slot": "12:00", "max_slot": "12:00"} | 12/0.799 | 12/1.087 | 0.5679 |
| 40 | 0.7 | 2.0 | regime!=TREND | (none) | {"min_slot": "12:00", "max_slot": "12:00"} | 25/0.839 | 24/0.684 | 0.5597 |