# B_HUGE_C1_CLOSE_RECLAIM_BREAK (LONG) — WINNER_LOSER_STUDY

_Generated 2026-07-03. Broad detection book (family dedupe, exit 1.0/1.5, statutory @15bps), FIT window ONLY (thresholds learned here are validated on untouched VAL); n=835, win rate 32.0%._

## Feature separation (winner mean vs loser mean, top |t|-like gaps)

| feature | win mean | loss mean | gap/std |
|---|---|---|---|
| pdh_dist_atr | -3.443 | -0.806 | 0.272 |
| ema200_dist_atr | 3.578 | 5.139 | 0.246 |
| or15_lose_atr | 6.688 | 7.791 | 0.218 |
| or15_break_atr | -0.038 | 1.072 | 0.203 |
| day_ret_pct | 1.054 | 1.599 | 0.198 |
| ema_stack_atr | 0.379 | 0.680 | 0.194 |
| dist_day_low_atr | 8.408 | 9.150 | 0.181 |
| quality_score | 84.065 | 94.840 | 0.168 |
| pdl_dist_atr | 9.985 | 11.572 | 0.165 |
| rs_pct | 1.131 | 1.569 | 0.158 |
| macd_hist_atr | 0.360 | 0.325 | 0.158 |
| macd_sig_atr | 0.360 | 0.325 | 0.158 |
| close_loc | 0.841 | 0.859 | 0.150 |
| ema50_dist_atr | 3.789 | 4.091 | 0.126 |
| body_pct | 0.734 | 0.751 | 0.118 |
| adx5 | 28.428 | 29.782 | 0.115 |
| gap_pct | 0.470 | 0.646 | 0.111 |
| candle_range_atr | 2.243 | 2.310 | 0.100 |
| prev_body_pct | 0.733 | 0.711 | 0.098 |
| rsi_slope3 | 17.487 | 16.886 | 0.092 |

## Net by signal hour

| hour | n | net Rs | win% |
|---|---|---|---|
| 10:00 | 8 | 1,305 | 50 |
| 11:00 | 238 | -90,990 | 31 |
| 12:00 | 237 | -70,619 | 34 |
| 13:00 | 225 | -95,957 | 30 |
| 14:00 | 127 | -38,507 | 31 |

## Worst days
- 2026-04-20: Rs-35,139
- 2026-04-23: Rs-26,304
- 2026-04-01: Rs-24,209
- 2026-03-25: Rs-23,843
- 2026-03-18: Rs-21,092

## Best days
- 2026-03-24: Rs6,365
- 2026-03-16: Rs6,426
- 2026-04-02: Rs12,555

## Worst symbols
- MUFIN: Rs-4,320
- CARTRADE: Rs-4,109
- TI: Rs-3,791
- AWL: Rs-3,695
- CHOLAHLDNG: Rs-3,669

## Exit mix

- {'SL': 384, 'EOD': 261, 'TARGET': 190}