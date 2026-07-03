# B_HUGE_RED_FAILED_BOUNCE (SHORT) — WINNER_LOSER_STUDY

_Generated 2026-07-03. Broad detection book (family dedupe, exit 0.9/1.25, statutory @15bps), FIT window ONLY (thresholds learned here are validated on untouched VAL); n=645, win rate 32.6%._

## Feature separation (winner mean vs loser mean, top |t|-like gaps)

| feature | win mean | loss mean | gap/std |
|---|---|---|---|
| ema200_dist_atr | -3.992 | -5.238 | 0.168 |
| rs_pct | -0.181 | -0.573 | 0.159 |
| pressure5 | 0.144 | 0.191 | 0.155 |
| gap_pct | -0.219 | -0.014 | 0.124 |
| or15_lose_atr | -0.875 | -1.589 | 0.122 |
| vol_ratio | 3.316 | 3.520 | 0.122 |
| dist_day_high_atr | 10.278 | 10.876 | 0.121 |
| quality_score | 66.004 | 70.965 | 0.115 |
| pdl_dist_atr | 2.748 | 1.672 | 0.109 |
| day_ret_pct | -1.179 | -1.442 | 0.106 |
| roc5_pct | -1.176 | -1.089 | 0.106 |
| mfi | 27.378 | 26.061 | 0.083 |
| vwap_dist_atr | -4.238 | -4.409 | 0.076 |
| atr_pct | 0.003 | 0.003 | 0.074 |
| rechigh_dist_atr | -4.225 | -4.152 | 0.066 |
| or15_break_atr | -8.914 | -9.291 | 0.063 |
| macd_hist_atr | -0.287 | -0.273 | 0.062 |
| macd_sig_atr | -0.287 | -0.273 | 0.062 |
| signal_range_pct | 0.677 | 0.649 | 0.058 |
| ema_stack_atr | -0.773 | -0.863 | 0.056 |

## Net by signal hour

| hour | n | net Rs | win% |
|---|---|---|---|
| 10:00 | 2 | -112 | 50 |
| 11:00 | 137 | -55,409 | 31 |
| 12:00 | 165 | -72,376 | 30 |
| 13:00 | 219 | -54,453 | 40 |
| 14:00 | 122 | -50,185 | 25 |

## Worst days
- 2026-03-13: Rs-24,995
- 2026-04-24: Rs-21,365
- 2026-04-16: Rs-18,287
- 2026-03-17: Rs-17,611
- 2026-03-05: Rs-16,793

## Best days
- 2026-03-30: Rs1,538
- 2026-04-09: Rs5,765
- 2026-03-19: Rs9,242

## Worst symbols
- PTCIL: Rs-4,229
- CARRARO: Rs-3,390
- AXISCADES: Rs-3,381
- PACEDIGITK: Rs-3,067
- ANANDRATHI: Rs-2,935

## Exit mix

- {'SL': 307, 'EOD': 199, 'TARGET': 139}