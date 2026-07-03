# C_OR_BREAKOUT — PARAMETER_SWEEP_SUMMARY (Stage 4)

_Generated 2026-07-03._

One-knob-at-a-time sweeps from each version base: 1477 iterations across 12 versions. FIT quantile grids; VAL as check.

## Top 30 sweep iterations

| iter | version | change | FIT n/PF | VAL n/PF | score |
|---|---|---|---|---|---|
| 551 | V4_rs_leader | mask +vol_ratio<=1.651211 | 34.0/0.596 | 28.0/0.59 | 0.5852 |
| 337 | V2_fresh_break | mask +signal_range_pct>=1.377075 | 276.0/0.548 | 177.0/0.544 | 0.5408 |
| 1445 | V12_fresh_vol | mask +signal_range_pct>=1.377075 | 255.0/0.522 | 165.0/0.528 | 0.5172 |
| 1352 | V11_broad_gate | pm +pre_entry_momentum_score>=83.969937 | 40.0/0.49 | 16.0/0.502 | 0.4804 |
| 328 | V2_fresh_break | mask +vwap_dist_atr>=6.499371 | 242.0/0.464 | 99.0/0.47 | 0.4592 |
| 1344 | V11_broad_gate | pm +pre3_range_r<=0.121513 | 48.0/0.493 | 47.0/0.543 | 0.453 |
| 244 | V1_mid_exit | pm +pre_entry_momentum_score>=83.969937 | 326.0/0.438 | 118.0/0.448 | 0.43 |
| 952 | V7_candle_quality | mask +signal_range_pct>=1.377075 | 150.0/0.575 | 83.0/0.493 | 0.4274 |
| 319 | V2_fresh_break | mask +atr_pct>=0.006069 | 250.0/0.435 | 191.0/0.446 | 0.4262 |
| 602 | V4_rs_leader | pm +pre5_mom_r<=-0.07115 | 73.0/0.425 | 128.0/0.427 | 0.4234 |
| 982 | V7_candle_quality | pm +pre_entry_momentum_score>=83.969937 | 287.0/0.43 | 98.0/0.44 | 0.422 |
| 364 | V2_fresh_break | pm +pre3_range_r>=1.096076 | 273.0/0.432 | 172.0/0.445 | 0.4216 |
| 367 | V2_fresh_break | pm +pre_entry_momentum_score>=83.969937 | 296.0/0.425 | 105.0/0.431 | 0.4202 |
| 1472 | V12_fresh_vol | pm +pre3_range_r>=1.096076 | 252.0/0.419 | 149.0/0.417 | 0.4154 |
| 1181 | V10_late_drift | mask +atr_pct>=0.006069 | 125.0/0.509 | 77.0/0.456 | 0.4136 |
| 490 | V3_fresh_morning | pm +pre_entry_momentum_score>=83.969937 | 138.0/0.468 | 59.0/0.538 | 0.412 |
| 598 | V4_rs_leader | pm +pre1_adx<=52.210207 | 575.0/0.421 | 333.0/0.415 | 0.4102 |
| 1226 | V10_late_drift | pm +pre3_range_r>=1.096076 | 146.0/0.45 | 66.0/0.501 | 0.4092 |
| 531 | V4_rs_leader | exit 1.1/2.0 | 615.0/0.409 | 399.0/0.409 | 0.409 |
| 605 | V4_rs_leader | pm +pre3_range_r<=0.121513 | 33.0/0.444 | 18.0/0.488 | 0.4088 |
| 543 | V4_rs_leader | guard {'min_slot': '12:30'} | 435.0/0.419 | 256.0/0.433 | 0.4078 |
| 442 | V3_fresh_morning | mask +atr_pct>=0.006069 | 146.0/0.409 | 119.0/0.419 | 0.401 |
| 604 | V4_rs_leader | pm +pre5_mom_r<=1.121735 | 507.0/0.414 | 340.0/0.432 | 0.3996 |
| 428 | V3_fresh_morning | mask +vol_ratio<=1.651211 | 94.0/0.401 | 85.0/0.404 | 0.3986 |
| 460 | V3_fresh_morning | mask +signal_range_pct>=1.377075 | 173.0/0.551 | 116.0/0.463 | 0.3926 |
| 536 | V4_rs_leader | exit 1.5/1.25 | 593.0/0.396 | 400.0/0.401 | 0.392 |
| 532 | V4_rs_leader | exit 1.1/2.5 | 604.0/0.399 | 398.0/0.408 | 0.3918 |
| 542 | V4_rs_leader | guard {'min_slot': '11:30'} | 565.0/0.398 | 351.0/0.407 | 0.3908 |
| 1475 | V12_fresh_vol | pm +pre_entry_momentum_score>=83.969937 | 275.0/0.418 | 93.0/0.401 | 0.3874 |
| 569 | V4_rs_leader | mask +close_loc>=0.675604 | 596.0/0.391 | 359.0/0.401 | 0.383 |

## Best score by knob family (top 25)

| change                                  |   best_score |   n |
|:----------------------------------------|-------------:|----:|
| mask +vol_ratio<=1.651211               |       0.5852 |  12 |
| mask +signal_range_pct>=1.377075        |       0.5408 |  12 |
| pm +pre_entry_momentum_score>=83.969937 |       0.4804 |  12 |
| mask +vwap_dist_atr>=6.499371           |       0.4592 |  12 |
| pm +pre3_range_r<=0.121513              |       0.453  |  12 |
| mask +atr_pct>=0.006069                 |       0.4262 |  12 |
| pm +pre5_mom_r<=-0.07115                |       0.4234 |  12 |
| pm +pre3_range_r>=1.096076              |       0.4216 |  12 |
| pm +pre1_adx<=52.210207                 |       0.4102 |  12 |
| exit 1.1                                |       0.409  |  84 |
| guard {'min_slot': '12:30'}             |       0.4078 |  12 |
| pm +pre5_mom_r<=1.121735                |       0.3996 |  12 |
| exit 1.5                                |       0.392  |  84 |
| guard {'min_slot': '11:30'}             |       0.3908 |  12 |
| mask +close_loc>=0.675604               |       0.383  |  12 |
| pm +pre2_mom_r>=0.680634                |       0.3828 |  12 |
| pm +sig5_rsi_dir>=55.767938             |       0.3822 |  12 |
| mask +fresh_age_bars<=0.0               |       0.3822 |  12 |
| mask +quality_score>=185.806093         |       0.3808 |  12 |
| mask +vwap_dist_atr<=1.955414           |       0.3794 |  12 |
| pm +pre3_range_r<=1.096076              |       0.3792 |  12 |
| mask +signal_range_pct>=0.188061        |       0.3788 |  12 |
| mask +atr_pct>=0.001919                 |       0.3786 |  12 |
| mask +signal_range_pct>=0.614093        |       0.3782 |  12 |
| exit 0.9                                |       0.3782 |  72 |
