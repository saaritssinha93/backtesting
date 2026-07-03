# B_AVWAP_RECLAIM_REVERSAL (LONG) — ITERATION_LOG

_Generated 2026-07-03. Research-only; NO live trades; NO final_setup_conf.py edits._

Optimizer: Optuna TPE. Protocol: search ONLY on FIT/VAL (band objective, tent at PF 1.80, gap penalty); confirm on full TRAIN; TEST scored ONCE per finalist whose TRAIN lands in [1.30,1.80]; TEST evaluations budget-capped (0 used).

- Stage 1 baseline: 1 iteration
- Stage 3 single-knob sweeps: 155 iterations (see PARAMETER_SWEEP_SUMMARY.md)
- Stage 4 combination search: 500 trials (284 unique configs; full list in trials.csv)
- Stage 5/6 finalist + rescue confirmations: 54 iterations

## Full per-iteration log (baseline, sweeps, finalists, rescues)

Complete row-level log: `iteration_log.csv` (every iteration: stage, group, change, old/new, FIT/VAL/TRAIN/TEST metrics, exit counts, keep/reject + why + next action). Key iterations below.

| # | stage | group | change | old -> new | SL/Tgt | FIT n/PF | VAL n/PF | TRAIN n/PF | TEST n/PF | keep | why |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 1-baseline | baseline | current conf config | - -> - | 0.7/1.5 | 1008.0/0.399 | 763.0/0.297 | 1771.0/0.354 | 730.0/0.334 | baseline | no_train_edge,sl_too_tight_or_bad_entries,overtrading,train_concentration,no_edge_anywhere |
| 4 | 3-sweep | exit | sl_pct | 0.7 -> 1.0 | 1.0/1.5 | 847.0/0.443 | 625.0/0.338 | -/- | -/- | improve | band score 0.254 vs baseline 0.215 |
| 5 | 3-sweep | exit | sl_pct | 0.7 -> 1.1 | 1.1/1.5 | 808.0/0.452 | 591.0/0.334 | -/- | -/- | improve | band score 0.240 vs baseline 0.215 |
| 6 | 3-sweep | exit | sl_pct | 0.7 -> 1.2 | 1.2/1.5 | 780.0/0.452 | 569.0/0.324 | -/- | -/- | improve | band score 0.222 vs baseline 0.215 |
| 7 | 3-sweep | exit | sl_pct | 0.7 -> 1.5 | 1.5/1.5 | 721.0/0.449 | 522.0/0.32 | -/- | -/- | improve | band score 0.217 vs baseline 0.215 |
| 12 | 3-sweep | exit | tgt_pct | 1.5 -> 2.0 | 0.7/2.0 | 959.0/0.416 | 738.0/0.329 | -/- | -/- | improve | band score 0.259 vs baseline 0.215 |
| 13 | 3-sweep | exit | tgt_pct | 1.5 -> 2.5 | 0.7/2.5 | 940.0/0.438 | 721.0/0.336 | -/- | -/- | improve | band score 0.254 vs baseline 0.215 |
| 19 | 3-sweep | filter | drop mask vwap_dist_atr<=1.0 | vwap_dist_atr<=1.0 -> dropped | 0.7/1.5 | 1228.0/0.411 | 908.0/0.308 | -/- | -/- | improve | band score 0.226 vs baseline 0.215 |
| 20 | 3-sweep | indicator/price-action | +mask atr_pct>= | - -> 0.002049 (q0.2) | 0.7/1.5 | 961.0/0.375 | 694.0/0.314 | -/- | -/- | improve | band score 0.265 vs baseline 0.215 |
| 22 | 3-sweep | indicator/price-action | +mask atr_pct>= | - -> 0.002795 (q0.5) | 0.7/1.5 | 680.0/0.376 | 449.0/0.341 | -/- | -/- | improve | band score 0.313 vs baseline 0.215 |
| 24 | 3-sweep | indicator/price-action | +mask atr_pct>= | - -> 0.004015 (q0.8) | 0.7/1.5 | 264.0/0.362 | 176.0/0.481 | -/- | -/- | improve | band score 0.267 vs baseline 0.215 |
| 27 | 3-sweep | indicator/price-action | +mask body_pct<= | - -> 0.637705 (q0.2) | 0.7/1.5 | 353.0/0.315 | 295.0/0.299 | -/- | -/- | improve | band score 0.286 vs baseline 0.215 |
| 28 | 3-sweep | indicator/price-action | +mask body_pct>= | - -> 0.791667 (q0.5) | 0.7/1.5 | 600.0/0.336 | 419.0/0.291 | -/- | -/- | improve | band score 0.255 vs baseline 0.215 |
| 32 | 3-sweep | indicator/price-action | +mask close_loc>= | - -> 0.764709 (q0.2) | 0.7/1.5 | 889.0/0.35 | 678.0/0.291 | -/- | -/- | improve | band score 0.244 vs baseline 0.215 |
| 34 | 3-sweep | indicator/price-action | +mask close_loc>= | - -> 0.90196 (q0.5) | 0.7/1.5 | 662.0/0.297 | 448.0/0.309 | -/- | -/- | improve | band score 0.287 vs baseline 0.215 |
| 36 | 3-sweep | indicator/price-action | +mask close_loc>= | - -> 1.0 (q0.8) | 0.7/1.5 | 440.0/0.271 | 270.0/0.338 | -/- | -/- | improve | band score 0.217 vs baseline 0.215 |
| 39 | 3-sweep | indicator/price-action | +mask lower_wick_pct<= | - -> 0.0 (q0.2) | 0.7/1.5 | 486.0/0.372 | 359.0/0.298 | -/- | -/- | improve | band score 0.239 vs baseline 0.215 |
| 40 | 3-sweep | indicator/price-action | +mask lower_wick_pct>= | - -> 0.027228 (q0.5) | 0.7/1.5 | 701.0/0.343 | 533.0/0.299 | -/- | -/- | improve | band score 0.264 vs baseline 0.215 |
| 42 | 3-sweep | indicator/price-action | +mask lower_wick_pct>= | - -> 0.094123 (q0.8) | 0.7/1.5 | 302.0/0.347 | 217.0/0.357 | -/- | -/- | improve | band score 0.339 vs baseline 0.215 |
| 44 | 3-sweep | indicator/price-action | +mask quality_score>= | - -> 50.89676 (q0.2) | 0.7/1.5 | 901.0/0.366 | 664.0/0.307 | -/- | -/- | improve | band score 0.260 vs baseline 0.215 |
| 46 | 3-sweep | indicator/price-action | +mask quality_score>= | - -> 75.262373 (q0.5) | 0.7/1.5 | 677.0/0.375 | 382.0/0.34 | -/- | -/- | improve | band score 0.312 vs baseline 0.215 |
| 50 | 3-sweep | indicator/price-action | +mask rs_pct>= | - -> 0.245521 (q0.2) | 0.7/1.5 | 884.0/0.386 | 663.0/0.32 | -/- | -/- | improve | band score 0.267 vs baseline 0.215 |
| 52 | 3-sweep | indicator/price-action | +mask rs_pct>= | - -> 0.85419 (q0.5) | 0.7/1.5 | 649.0/0.338 | 420.0/0.329 | -/- | -/- | improve | band score 0.322 vs baseline 0.215 |
| 54 | 3-sweep | indicator/price-action | +mask rs_pct>= | - -> 1.936181 (q0.8) | 0.7/1.5 | 278.0/0.358 | 161.0/0.458 | -/- | -/- | improve | band score 0.278 vs baseline 0.215 |
| 56 | 3-sweep | indicator/price-action | +mask signal_range_pct>= | - -> 0.324713 (q0.2) | 0.7/1.5 | 902.0/0.409 | 674.0/0.326 | -/- | -/- | improve | band score 0.260 vs baseline 0.215 |
| 58 | 3-sweep | indicator/price-action | +mask signal_range_pct>= | - -> 0.540726 (q0.5) | 0.7/1.5 | 558.0/0.394 | 410.0/0.342 | -/- | -/- | improve | band score 0.300 vs baseline 0.215 |
| 60 | 3-sweep | indicator/price-action | +mask signal_range_pct>= | - -> 0.887972 (q0.8) | 0.7/1.5 | 187.0/0.392 | 148.0/0.47 | -/- | -/- | improve | band score 0.330 vs baseline 0.215 |
| 63 | 3-sweep | indicator/price-action | +mask upper_wick_pct<= | - -> 0.0 (q0.2) | 0.7/1.5 | 440.0/0.271 | 270.0/0.338 | -/- | -/- | improve | band score 0.217 vs baseline 0.215 |
| 65 | 3-sweep | indicator/price-action | +mask upper_wick_pct<= | - -> 0.047654 (q0.5) | 0.7/1.5 | 692.0/0.307 | 470.0/0.31 | -/- | -/- | improve | band score 0.305 vs baseline 0.215 |
| 66 | 3-sweep | indicator/price-action | +mask upper_wick_pct>= | - -> 0.135302 (q0.8) | 0.7/1.5 | 271.0/0.371 | 211.0/0.429 | -/- | -/- | improve | band score 0.325 vs baseline 0.215 |
| 68 | 3-sweep | indicator/price-action | +mask vol_ratio>= | - -> 1.795083 (q0.2) | 0.7/1.5 | 893.0/0.389 | 676.0/0.312 | -/- | -/- | improve | band score 0.250 vs baseline 0.215 |
| 70 | 3-sweep | indicator/price-action | +mask vol_ratio>= | - -> 2.55782 (q0.5) | 0.7/1.5 | 653.0/0.373 | 507.0/0.345 | -/- | -/- | improve | band score 0.323 vs baseline 0.215 |
| 72 | 3-sweep | indicator/price-action | +mask vol_ratio>= | - -> 4.263719 (q0.8) | 0.7/1.5 | 251.0/0.361 | 226.0/0.315 | -/- | -/- | improve | band score 0.278 vs baseline 0.215 |
| 76 | 3-sweep | indicator/price-action | +mask vwap_dist_atr>= | - -> 0.761686 (q0.5) | 0.7/1.5 | 351.0/0.395 | 254.0/0.325 | -/- | -/- | improve | band score 0.269 vs baseline 0.215 |
| 81 | 3-sweep | indicator/price-action | +mask wick_skew_pct<= | - -> -0.052753 (q0.2) | 0.7/1.5 | 328.0/0.293 | 208.0/0.325 | -/- | -/- | improve | band score 0.267 vs baseline 0.215 |
| 83 | 3-sweep | indicator/price-action | +mask wick_skew_pct<= | - -> 0.008237 (q0.5) | 0.7/1.5 | 693.0/0.284 | 486.0/0.28 | -/- | -/- | improve | band score 0.277 vs baseline 0.215 |
| 84 | 3-sweep | indicator/price-action | +mask wick_skew_pct>= | - -> 0.101075 (q0.8) | 0.7/1.5 | 275.0/0.437 | 209.0/0.397 | -/- | -/- | improve | band score 0.365 vs baseline 0.215 |
| 85 | 3-sweep | indicator/price-action | +mask wick_skew_pct<= | - -> 0.101075 (q0.8) | 0.7/1.5 | 907.0/0.341 | 687.0/0.274 | -/- | -/- | improve | band score 0.220 vs baseline 0.215 |
| 87 | 3-sweep | regime | +mask regime==NEUTRAL | - -> NEUTRAL | 0.7/1.5 | 465.0/0.371 | 555.0/0.317 | -/- | -/- | improve | band score 0.274 vs baseline 0.215 |
| 88 | 3-sweep | regime | +mask regime!=TREND | - -> TREND | 0.7/1.5 | 789.0/0.354 | 763.0/0.297 | -/- | -/- | improve | band score 0.251 vs baseline 0.215 |
| 89 | 3-sweep | regime | +mask regime!=BULL | - -> BULL | 0.7/1.5 | 684.0/0.433 | 555.0/0.317 | -/- | -/- | improve | band score 0.224 vs baseline 0.215 |
| 90 | 3-sweep | pre-momentum | +premom pre1_adx>= | - -> 20.231494 (q0.2) | 0.7/1.5 | 877.0/0.379 | 704.0/0.301 | -/- | -/- | improve | band score 0.239 vs baseline 0.215 |
| 93 | 3-sweep | pre-momentum | +premom pre1_adx<= | - -> 27.066949 (q0.5) | 0.7/1.5 | 746.0/0.342 | 551.0/0.307 | -/- | -/- | improve | band score 0.279 vs baseline 0.215 |
| 94 | 3-sweep | pre-momentum | +premom pre1_adx>= | - -> 35.777423 (q0.8) | 0.7/1.5 | 286.0/0.28 | 235.0/0.247 | -/- | -/- | improve | band score 0.221 vs baseline 0.215 |
| 95 | 3-sweep | pre-momentum | +premom pre1_adx<= | - -> 35.777423 (q0.8) | 0.7/1.5 | 907.0/0.403 | 695.0/0.3 | -/- | -/- | improve | band score 0.218 vs baseline 0.215 |
| 97 | 3-sweep | pre-momentum | +premom pre3_close_pos<= | - -> 0.441252 (q0.2) | 0.7/1.5 | 241.0/0.31 | 352.0/0.267 | -/- | -/- | improve | band score 0.233 vs baseline 0.215 |
| 98 | 3-sweep | pre-momentum | +premom pre3_close_pos>= | - -> 0.799384 (q0.5) | 0.7/1.5 | 749.0/0.306 | 466.0/0.309 | -/- | -/- | improve | band score 0.304 vs baseline 0.215 |
| 100 | 3-sweep | pre-momentum | +premom pre3_close_pos>= | - -> 1.0 (q0.8) | 0.7/1.5 | 447.0/0.293 | 246.0/0.337 | -/- | -/- | improve | band score 0.258 vs baseline 0.215 |
| 102 | 3-sweep | pre-momentum | +premom pre3_range_r>= | - -> 0.167283 (q0.2) | 0.7/1.5 | 973.0/0.393 | 729.0/0.326 | -/- | -/- | improve | band score 0.272 vs baseline 0.215 |
| 104 | 3-sweep | pre-momentum | +premom pre3_range_r>= | - -> 0.328206 (q0.5) | 0.7/1.5 | 824.0/0.384 | 546.0/0.361 | -/- | -/- | improve | band score 0.343 vs baseline 0.215 |
| 106 | 3-sweep | pre-momentum | +premom pre3_range_r>= | - -> 0.626941 (q0.8) | 0.7/1.5 | 417.0/0.414 | 220.0/0.478 | -/- | -/- | improve | band score 0.363 vs baseline 0.215 |
| 108 | 3-sweep | pre-momentum | +premom pre5_mom_r>= | - -> 0.040312 (q0.2) | 0.7/1.5 | 949.0/0.393 | 642.0/0.33 | -/- | -/- | improve | band score 0.280 vs baseline 0.215 |
| 110 | 3-sweep | pre-momentum | +premom pre5_mom_r>= | - -> 0.342895 (q0.5) | 0.7/1.5 | 804.0/0.387 | 447.0/0.383 | -/- | -/- | improve | band score 0.380 vs baseline 0.215 |
| 112 | 3-sweep | pre-momentum | +premom pre5_mom_r>= | - -> 0.691398 (q0.8) | 0.7/1.5 | 395.0/0.413 | 161.0/0.473 | -/- | -/- | improve | band score 0.365 vs baseline 0.215 |
| 114 | 3-sweep | pre-momentum | +premom pre_entry_momentum_score>= | - -> 52.165367 (q0.2) | 0.7/1.5 | 946.0/0.396 | 639.0/0.315 | -/- | -/- | improve | band score 0.250 vs baseline 0.215 |
| 116 | 3-sweep | pre-momentum | +premom pre_entry_momentum_score>= | - -> 66.395361 (q0.5) | 0.7/1.5 | 774.0/0.373 | 442.0/0.374 | -/- | -/- | improve | band score 0.372 vs baseline 0.215 |
| 118 | 3-sweep | pre-momentum | +premom pre_entry_momentum_score>= | - -> 77.956891 (q0.8) | 0.7/1.5 | 376.0/0.388 | 168.0/0.466 | -/- | -/- | improve | band score 0.326 vs baseline 0.215 |
| 120 | 3-sweep | pre-momentum | +premom sig5_adx_calc>= | - -> 14.919849 (q0.2) | 0.7/1.5 | 915.0/0.38 | 700.0/0.297 | -/- | -/- | improve | band score 0.231 vs baseline 0.215 |
| 121 | 3-sweep | pre-momentum | +premom sig5_adx_calc<= | - -> 14.919849 (q0.2) | 0.7/1.5 | 326.0/0.375 | 261.0/0.289 | -/- | -/- | improve | band score 0.220 vs baseline 0.215 |
| 123 | 3-sweep | pre-momentum | +premom sig5_adx_calc<= | - -> 19.7609 (q0.5) | 0.7/1.5 | 642.0/0.347 | 515.0/0.29 | -/- | -/- | improve | band score 0.244 vs baseline 0.215 |
| 124 | 3-sweep | pre-momentum | +premom sig5_adx_calc>= | - -> 27.289658 (q0.8) | 0.7/1.5 | 335.0/0.339 | 192.0/0.295 | -/- | -/- | improve | band score 0.260 vs baseline 0.215 |
| 126 | 3-sweep | pre-momentum | +premom sig5_rsi_dir>= | - -> 53.138073 (q0.2) | 0.7/1.5 | 847.0/0.371 | 666.0/0.321 | -/- | -/- | improve | band score 0.281 vs baseline 0.215 |
| 128 | 3-sweep | pre-momentum | +premom sig5_rsi_dir>= | - -> 57.986759 (q0.5) | 0.7/1.5 | 560.0/0.377 | 355.0/0.37 | -/- | -/- | improve | band score 0.364 vs baseline 0.215 |
| 130 | 3-sweep | pre-momentum | +premom sig5_rsi_dir>= | - -> 62.727852 (q0.8) | 0.7/1.5 | 189.0/0.361 | 118.0/0.406 | -/- | -/- | improve | band score 0.325 vs baseline 0.215 |
| 132 | 3-sweep | pre-momentum | +premom sig5_vol_ratio20>= | - -> 1.759121 (q0.2) | 0.7/1.5 | 915.0/0.396 | 682.0/0.314 | -/- | -/- | improve | band score 0.248 vs baseline 0.215 |
| 134 | 3-sweep | pre-momentum | +premom sig5_vol_ratio20>= | - -> 2.495784 (q0.5) | 0.7/1.5 | 675.0/0.368 | 503.0/0.355 | -/- | -/- | improve | band score 0.345 vs baseline 0.215 |
| 136 | 3-sweep | pre-momentum | +premom sig5_vol_ratio20>= | - -> 4.180733 (q0.8) | 0.7/1.5 | 269.0/0.388 | 221.0/0.326 | -/- | -/- | improve | band score 0.276 vs baseline 0.215 |
| 143 | 3-sweep | guard | min_slot | - -> 12:00 | 0.7/1.5 | 763.0/0.354 | 641.0/0.316 | -/- | -/- | improve | band score 0.286 vs baseline 0.215 |
| 146 | 3-sweep | guard | max_slot | - -> 12:30 | 0.7/1.5 | 585.0/0.376 | 435.0/0.287 | -/- | -/- | improve | band score 0.216 vs baseline 0.215 |
| 150 | 3-sweep | guard | top_n | - -> 1 | 0.7/1.5 | 239.0/0.305 | 202.0/0.296 | -/- | -/- | improve | band score 0.289 vs baseline 0.215 |
| 151 | 3-sweep | guard | top_n | - -> 2 | 0.7/1.5 | 555.0/0.349 | 459.0/0.305 | -/- | -/- | improve | band score 0.270 vs baseline 0.215 |
| 152 | 3-sweep | guard | top_n | - -> 3 | 0.7/1.5 | 723.0/0.363 | 626.0/0.297 | -/- | -/- | improve | band score 0.244 vs baseline 0.215 |
| 153 | 3-sweep | guard | daily_loss_rs | 0.0 -> 2000.0 | 0.7/1.5 | 567.0/0.434 | 377.0/0.327 | -/- | -/- | improve | band score 0.241 vs baseline 0.215 |
| 156 | 3-sweep | guard | max_positions | 20 -> 10 | 0.7/1.5 | 610.0/0.422 | 446.0/0.311 | -/- | -/- | improve | band score 0.222 vs baseline 0.215 |
| 157 | 5-finalist | combination | finalist #1 | - -> - | 0.85/1.5 | -/- | -/- | 167.0/0.553 | -/- | reject | TRAIN not in band or too thin (PF 0.553, n 167) |
| 158 | 5-finalist | combination | finalist #2 | - -> - | 0.7/1.5 | -/- | -/- | 24.0/0.531 | -/- | reject | TRAIN not in band or too thin (PF 0.531, n 24) |
| 159 | 5-finalist | combination | finalist #3 | - -> - | 0.7/1.5 | -/- | -/- | 28.0/0.515 | -/- | reject | TRAIN not in band or too thin (PF 0.515, n 28) |
| 160 | 5-finalist | combination | finalist #4 | - -> - | 0.7/1.5 | -/- | -/- | 200.0/0.478 | -/- | reject | TRAIN not in band or too thin (PF 0.478, n 200) |
| 161 | 5-finalist | combination | finalist #5 | - -> - | 0.7/1.0 | -/- | -/- | 24.0/0.489 | -/- | reject | TRAIN not in band or too thin (PF 0.489, n 24) |
| 162 | 5-finalist | combination | finalist #6 | - -> - | 0.85/1.5 | -/- | -/- | 205.0/0.496 | -/- | reject | TRAIN not in band or too thin (PF 0.496, n 205) |
| 163 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.2/1.5 | 21.0/0.994 | 34.0/1.076 | -/- | -/- | - | score 0.929 |
| 164 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.2/1.5 | 253.0/0.506 | 195.0/0.51 | -/- | -/- | - | score 0.503 |
| 165 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.2/1.5 | 255.0/0.509 | 197.0/0.528 | -/- | -/- | - | score 0.494 |
| 166 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.2/1.5 | 169.0/0.556 | 150.0/0.518 | -/- | -/- | - | score 0.488 |
| 167 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.2/1.5 | 75.0/0.611 | 121.0/0.536 | -/- | -/- | - | score 0.477 |
| 168 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.2/1.5 | 275.0/0.495 | 221.0/0.483 | -/- | -/- | - | score 0.474 |
| 169 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.5/2.5 | 291.0/0.503 | 200.0/0.485 | -/- | -/- | - | score 0.471 |
| 170 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.2/1.5 | 317.0/0.48 | 255.0/0.473 | -/- | -/- | - | score 0.467 |
| 171 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.2/1.5 | 221.0/0.488 | 181.0/0.531 | -/- | -/- | - | score 0.454 |
| 172 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.2/1.5 | 325.0/0.468 | 261.0/0.457 | -/- | -/- | - | score 0.448 |
| 173 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.2/1.5 | 294.0/0.453 | 194.0/0.461 | -/- | -/- | - | score 0.447 |
| 174 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 0.85/1.5 | 255.0/0.47 | 197.0/0.456 | -/- | -/- | - | score 0.446 |
| 175 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.2/1.5 | 315.0/0.478 | 253.0/0.459 | -/- | -/- | - | score 0.445 |
| 176 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.2/1.5 | 267.0/0.465 | 218.0/0.507 | -/- | -/- | - | score 0.431 |
| 177 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.2/1.5 | 264.0/0.548 | 220.0/0.48 | -/- | -/- | - | score 0.426 |
| 178 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.2/1.5 | 374.0/0.444 | 278.0/0.468 | -/- | -/- | - | score 0.425 |
| 179 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.5/1.5 | 293.0/0.46 | 225.0/0.504 | -/- | -/- | - | score 0.424 |
| 180 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.2/1.5 | 490.0/0.449 | 379.0/0.432 | -/- | -/- | - | score 0.418 |
| 181 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.2/1.5 | 66.0/0.485 | 70.0/0.57 | -/- | -/- | - | score 0.416 |
| 182 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.0/2.0 | 351.0/0.441 | 263.0/0.425 | -/- | -/- | - | score 0.412 |
| 183 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.2/1.5 | 145.0/0.625 | 109.0/0.505 | -/- | -/- | - | score 0.408 |
| 184 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 0.7/1.5 | 253.0/0.416 | 195.0/0.409 | -/- | -/- | - | score 0.403 |
| 185 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.0/1.5 | 497.0/0.432 | 379.0/0.416 | -/- | -/- | - | score 0.403 |
| 186 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.5/2.5 | 451.0/0.546 | 355.0/0.466 | -/- | -/- | - | score 0.402 |
| 187 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.2/2.5 | 287.0/0.466 | 202.0/0.43 | -/- | -/- | - | score 0.400 |
| 188 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.2/1.5 | 259.0/0.478 | 201.0/0.434 | -/- | -/- | - | score 0.398 |
| 189 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.2/1.5 | 252.0/0.477 | 215.0/0.429 | -/- | -/- | - | score 0.390 |
| 190 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.5/2.5 | 116.0/0.477 | 102.0/0.592 | -/- | -/- | - | score 0.384 |
| 191 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.2/0.8 | 200.0/0.43 | 163.0/0.4 | -/- | -/- | - | score 0.376 |
| 192 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.2/1.5 | 296.0/0.475 | 194.0/0.418 | -/- | -/- | - | score 0.373 |
| 193 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 0.85/1.5 | 199.0/0.389 | 155.0/0.377 | -/- | -/- | - | score 0.367 |
| 194 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.2/1.5 | 378.0/0.45 | 447.0/0.402 | -/- | -/- | - | score 0.364 |
| 195 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.2/1.5 | 32.0/0.406 | 27.0/0.46 | -/- | -/- | - | score 0.363 |
| 196 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.2/1.5 | 94.0/0.483 | 81.0/0.414 | -/- | -/- | - | score 0.358 |
| 197 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.2/1.5 | 250.0/0.474 | 211.0/0.41 | -/- | -/- | - | score 0.358 |
| 198 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.2/1.5 | 193.0/0.448 | 176.0/0.398 | -/- | -/- | - | score 0.358 |
| 199 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.2/1.5 | 608.0/0.475 | 464.0/0.407 | -/- | -/- | - | score 0.353 |
| 200 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.2/0.8 | 253.0/0.378 | 195.0/0.417 | -/- | -/- | - | score 0.347 |
| 201 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.2/1.5 | 710.0/0.46 | 557.0/0.397 | -/- | -/- | - | score 0.346 |
| 202 | 6-rescue-R1 | premom-off search | premom removed | - -> - | 1.2/1.5 | 611.0/0.393 | 482.0/0.366 | -/- | -/- | - | score 0.344 |
| 203 | 6-rescue | R1-premom-off | rescue variant | - -> - | 1.2/1.5 | 21.0/0.994 | 34.0/1.076 | 55.0/1.043 | -/- | reject | TRAIN out of band (PF 1.043, n 55) |
| 204 | 6-rescue | R2-drop-mask-0 | rescue variant | - -> - | 0.85/1.5 | 90.0/0.543 | 77.0/0.566 | 167.0/0.553 | -/- | reject | TRAIN out of band (PF 0.553, n 167) |
| 205 | 6-rescue | R3-window-{"min_slot": "10:00"} | rescue variant | - -> - | 0.85/1.5 | 90.0/0.543 | 77.0/0.566 | 167.0/0.553 | -/- | reject | TRAIN out of band (PF 0.553, n 167) |
| 206 | 6-rescue | R2-drop-mask-1 | rescue variant | - -> - | 0.85/1.5 | 181.0/0.502 | 143.0/0.472 | 324.0/0.489 | -/- | reject | TRAIN out of band (PF 0.489, n 324) |
| 207 | 6-rescue | R3-window-{"min_slot": "10:00", "max_slot": "14:00"} | rescue variant | - -> - | 0.85/1.5 | 84.0/0.555 | 63.0/0.493 | 147.0/0.528 | -/- | reject | TRAIN out of band (PF 0.528, n 147) |
| 208 | 6-rescue | R2-drop-mask-2 | rescue variant | - -> - | 0.85/1.5 | 167.0/0.412 | 116.0/0.502 | 283.0/0.448 | -/- | reject | TRAIN out of band (PF 0.448, n 283) |
| 209 | 6-rescue | R2-drop-premom-0 | rescue variant | - -> - | 0.85/1.5 | 291.0/0.574 | 247.0/0.341 | 538.0/0.46 | -/- | reject | TRAIN out of band (PF 0.46, n 538) |
| 210 | 6-rescue | R3-window-{"max_slot": "12:00"} | rescue variant | - -> - | 0.85/1.5 | 36.0/0.214 | 20.0/0.336 | 56.0/0.257 | -/- | reject | TRAIN out of band (PF 0.257, n 56) |

## Top 40 stage-4 trials (by FIT/VAL band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|----|------|--------|-------|----------|----------|-------|
| 1 | 0.85 | 1.5 | lower_wick_pct>=0.0; wick_skew_pct>=-0.020108; regime!=BULL | pre3_close_pos>=1.0 | {"min_slot": "10:30", "top_n": 2} | 90/0.543 | 77/0.566 | 0.5237 |
| 2 | 0.7 | 1.5 | vol_ratio>=5.485687; regime==NEUTRAL | pre3_close_pos>=1.0 | {"min_slot": "09:30", "top_n": 1} | 11/0.536 | 13/0.526 | 0.518 |
| 3 | 0.7 | 1.5 | vol_ratio>=5.485687; regime!=BULL | pre3_close_pos>=1.0 | {"min_slot": "09:30", "top_n": 1} | 15/0.506 | 13/0.526 | 0.4905 |
| 4 | 0.7 | 1.5 | vol_ratio>=5.485687; regime!=BULL | pre3_close_pos>=1.0 | {"min_slot": "11:00", "top_n": 1} | 15/0.506 | 13/0.526 | 0.4905 |
| 5 | 0.7 | 1.5 | regime!=BULL | pre3_close_pos>=1.0 | {"min_slot": "11:00", "top_n": 1} | 121/0.475 | 79/0.482 | 0.4688 |
| 6 | 0.7 | 1.0 | vol_ratio>=5.485687; regime==NEUTRAL | pre3_close_pos>=1.0 | {"min_slot": "09:30", "top_n": 1} | 11/0.498 | 13/0.481 | 0.4675 |
| 7 | 0.7 | 1.5 | regime!=BULL | pre3_close_pos>=1.0 | {"min_slot": "11:00", "top_n": 1} | 125/0.467 | 85/0.464 | 0.462 |
| 8 | 0.7 | 1.5 | regime!=BULL | pre3_close_pos>=1.0 | {"min_slot": "10:30", "top_n": 1} | 122/0.469 | 79/0.482 | 0.4578 |
| 9 | 0.7 | 1.5 | regime!=BULL | pre3_close_pos>=1.0 | {"min_slot": "10:30", "top_n": 1} | 122/0.469 | 79/0.482 | 0.4578 |
| 10 | 0.85 | 1.5 | regime!=BULL | pre3_close_pos>=1.0 | {"min_slot": "10:30", "top_n": 1} | 121/0.512 | 84/0.473 | 0.4427 |
| 11 | 0.85 | 1.5 | regime!=BEAR | pre3_close_pos>=1.0 | {"min_slot": "10:30", "top_n": 1} | 196/0.438 | 130/0.447 | 0.4306 |
| 12 | 0.7 | 1.25 | vol_ratio>=5.485687; regime!=BULL | pre3_close_pos>=1.0 | {"min_slot": "11:00", "top_n": 1} | 15/0.562 | 13/0.487 | 0.4269 |
| 13 | 0.85 | 1.5 | (none) | pre3_close_pos>=1.0 | {"min_slot": "10:30", "top_n": 2} | 291/0.437 | 208/0.451 | 0.4248 |
| 14 | 0.85 | 1.5 | regime!=BEAR | pre3_close_pos>=1.0 | {"top_n": 2} | 291/0.437 | 208/0.451 | 0.4248 |
| 15 | 0.85 | 1.5 | regime!=BEAR | pre3_close_pos>=1.0 | {"min_slot": "09:45", "top_n": 2} | 291/0.437 | 208/0.451 | 0.4248 |
| 16 | 0.85 | 1.5 | regime!=BEAR | pre3_close_pos>=1.0 | {"min_slot": "10:30", "top_n": 2} | 291/0.437 | 208/0.451 | 0.4248 |
| 17 | 0.7 | 1.25 | regime!=BULL | pre3_close_pos>=1.0 | {"min_slot": "11:00", "top_n": 1} | 125/0.434 | 85/0.428 | 0.423 |
| 18 | 0.85 | 2.0 | regime!=BEAR | pre3_close_pos>=1.0 | {"min_slot": "09:30", "top_n": 2} | 287/0.438 | 206/0.429 | 0.4221 |
| 19 | 0.85 | 2.0 | regime!=BEAR | pre3_close_pos>=1.0 | {"min_slot": "10:30", "max_slot": "14:30", "top_n": 2} | 287/0.438 | 206/0.429 | 0.4221 |
| 20 | 0.85 | 2.0 | regime!=BEAR | pre3_close_pos>=1.0 | {"min_slot": "10:30", "top_n": 2} | 287/0.438 | 206/0.429 | 0.4221 |
| 21 | 0.85 | 2.0 | regime!=BEAR | pre3_close_pos>=1.0 | {"min_slot": "10:30", "max_slot": "14:00", "top_n": 2} | 269/0.446 | 183/0.431 | 0.4194 |
| 22 | 0.85 | 1.5 | regime!=BEAR | pre3_close_pos>=1.0 | {"min_slot": "10:30", "max_slot": "14:00", "top_n": 2} | 269/0.46 | 185/0.437 | 0.4182 |
| 23 | 0.85 | 2.0 | regime!=BEAR | pre3_close_pos>=1.0 | {"min_slot": "10:30", "max_slot": "14:00", "top_n": 2} | 295/0.42 | 188/0.417 | 0.4144 |
| 24 | 0.85 | 1.5 | regime!=BEAR | pre5_mom_r>=0.538391 | {"min_slot": "10:30", "top_n": 2} | 324/0.414 | 212/0.413 | 0.4133 |
| 25 | 0.85 | 1.25 | regime!=BEAR | pre3_close_pos>=1.0 | {"min_slot": "10:30", "max_slot": "14:00", "top_n": 2} | 277/0.413 | 185/0.415 | 0.4114 |
| 26 | 0.85 | 1.5 | regime!=BULL | pre3_close_pos>=0.942943 | {"min_slot": "09:30", "top_n": 2} | 211/0.412 | 171/0.414 | 0.4112 |
| 27 | 0.7 | 2.5 | vol_ratio>=5.485687; regime!=BULL | pre3_close_pos>=1.0 | {"min_slot": "11:00", "top_n": 1} | 15/0.535 | 13/0.69 | 0.4109 |
| 28 | 0.7 | 1.5 | body_pct>=0.567242; wick_skew_pct<=0.0; regime!=BULL | pre3_close_pos>=1.0 | {"min_slot": "10:30", "top_n": 1} | 110/0.431 | 67/0.419 | 0.4096 |
| 29 | 0.85 | 1.5 | regime!=TREND | pre3_close_pos>=1.0 | {"top_n": 3} | 262/0.415 | 235/0.423 | 0.4084 |
| 30 | 0.85 | 1.5 | regime!=BEAR | pre_entry_momentum_score>=82.236413 | {"min_slot": "09:45", "top_n": 2} | 184/0.411 | 110/0.409 | 0.4078 |
| 31 | 0.85 | 1.5 | regime!=BEAR | pre3_close_pos>=1.0 | {"top_n": 3} | 323/0.443 | 235/0.423 | 0.4061 |
| 32 | 0.85 | 1.5 | regime!=BEAR | pre3_close_pos>=1.0 | {"min_slot": "11:00", "top_n": 1} | 210/0.424 | 133/0.454 | 0.3995 |
| 33 | 1.0 | 1.5 | regime!=TREND | pre3_close_pos<=1.0 | {"top_n": 3} | 347/0.403 | 334/0.407 | 0.3993 |
| 34 | 1.0 | 1.5 | regime!=TREND | pre1_adx<=35.777423 | {"top_n": 3} | 334/0.42 | 317/0.407 | 0.3961 |
| 35 | 1.0 | 1.5 | regime!=TREND | pre1_adx<=35.777423 | - | 341/0.43 | 309/0.411 | 0.3958 |
| 36 | 0.85 | 1.5 | rs_pct>=0.85419; signal_range_pct>=0.887972; regime==NEUTRAL | pre3_close_pos>=0.717001 | {"min_slot": "10:30", "top_n": 1} | 36/0.401 | 41/0.398 | 0.3954 |
| 37 | 1.0 | 1.5 | regime!=TREND | pre1_adx<=35.777423 | {"min_slot": "11:00"} | 344/0.437 | 308/0.414 | 0.3953 |
| 38 | 0.85 | 1.5 | regime!=BEAR | pre3_close_pos>=1.0 | {"min_slot": "10:30", "top_n": 2} | 325/0.406 | 224/0.422 | 0.3922 |
| 39 | 0.7 | 1.5 | regime!=BULL | pre3_close_pos>=0.942943 | {"min_slot": "11:00", "top_n": 1} | 166/0.394 | 109/0.393 | 0.3918 |
| 40 | 0.85 | 1.5 | wick_skew_pct<=-0.052753; atr_pct>=0.002298; regime!=BEAR | pre3_close_pos>=1.0 | {"min_slot": "10:30", "top_n": 2} | 105/0.425 | 58/0.467 | 0.3911 |