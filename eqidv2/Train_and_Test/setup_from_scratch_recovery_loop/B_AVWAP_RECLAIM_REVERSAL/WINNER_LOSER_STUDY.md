# B_AVWAP_RECLAIM_REVERSAL (LONG) — WINNER_LOSER_STUDY

_Generated 2026-07-03. Broad detection book (family dedupe, exit 0.7/1.5, statutory @15bps), FIT window ONLY (thresholds learned here are validated on untouched VAL); n=1214, win rate 27.6%._

## Feature separation (winner mean vs loser mean, top |t|-like gaps)

| feature | win mean | loss mean | gap/std |
|---|---|---|---|
| day_ret_pct | 0.747 | 1.055 | 0.202 |
| rs_pct | 1.074 | 1.308 | 0.165 |
| or15_lose_atr | 4.807 | 5.385 | 0.165 |
| quality_score | 78.829 | 85.476 | 0.164 |
| mfi | 59.112 | 56.880 | 0.140 |
| dist_day_low_atr | 5.711 | 6.109 | 0.134 |
| close_loc | 0.877 | 0.891 | 0.119 |
| or15_break_atr | -1.875 | -1.507 | 0.117 |
| pdh_dist_atr | -4.327 | -3.447 | 0.107 |
| signal_range_pct | 0.644 | 0.685 | 0.098 |
| bb_width_pct | 1.308 | 1.402 | 0.096 |
| adx5 | 24.121 | 23.228 | 0.093 |
| ema200_dist_atr | 1.950 | 2.513 | 0.092 |
| atr_pct | 0.003 | 0.003 | 0.087 |
| gap_pct | 0.278 | 0.154 | 0.079 |
| ema20_slope3_atr | 0.119 | 0.100 | 0.078 |
| obv_slope10_norm | 0.255 | 0.229 | 0.071 |
| candle_range_atr | 1.926 | 1.972 | 0.067 |
| body_pct | 0.788 | 0.796 | 0.058 |
| cci | 87.323 | 81.370 | 0.055 |

## Net by signal hour

| hour | n | net Rs | win% |
|---|---|---|---|
| 10:00 | 6 | -3,372 | 17 |
| 11:00 | 539 | -212,534 | 26 |
| 12:00 | 306 | -92,484 | 31 |
| 13:00 | 226 | -62,513 | 30 |
| 14:00 | 137 | -56,036 | 23 |

## Worst days
- 2026-04-20: Rs-38,719
- 2026-03-25: Rs-32,649
- 2026-03-30: Rs-29,060
- 2026-04-21: Rs-27,566
- 2026-04-23: Rs-26,861

## Best days
- 2026-04-06: Rs2,982
- 2026-04-02: Rs12,406
- 2026-03-24: Rs14,971

## Worst symbols
- VIYASH: Rs-4,658
- ANURAS: Rs-4,642
- LINDEINDIA: Rs-4,524
- MARINE: Rs-3,726
- EBGNG: Rs-3,725

## Exit mix

- {'SL': 729, 'EOD': 289, 'TARGET': 196}