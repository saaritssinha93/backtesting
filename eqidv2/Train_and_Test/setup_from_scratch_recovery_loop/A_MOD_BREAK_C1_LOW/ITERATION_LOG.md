# A_MOD_BREAK_C1_LOW (SHORT) — ITERATION_LOG (from-scratch recovery)

_Generated 2026-07-03. Research-only; NO live trades; NO final_setup_conf.py edits._

Optimizer: Optuna TPE. Iterations logged: 140 rows in `iteration_log_recovery.csv` + 1835 scored configs in `trials_recovery.csv` (stage A exits, stage B feature scans, stage C TPE). TEST evaluations spent: 0 (budget-capped; only TRAIN-band finalists).

| # | stage | variant | change | SL/Tgt | mask | FIT n/PF | VAL n/PF | TRAIN n/PF | TEST n/PF | keep | why |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 2-baseline | BASELINE | - | 1.1/1.0 | vol_ratio>=1.955814 | 108.0/0.43 | 56.0/0.832 | 164.0/0.542 | 36.0/0.337 | anchor | production config on recreated pool |
| 128 | D-finalist | RETEST | finalist #1 | 1.5/2.5 | gap_pct<=0.868141 | -/- | -/- | 1866.0/0.532 | -/- | reject | TRAIN not in band or thin (PF 0.532, n 1866) |
| 129 | D-finalist | RETEST | finalist #2 | 1.5/2.5 | signal_range_pct>=0.311218 | -/- | -/- | 2076.0/0.533 | -/- | reject | TRAIN not in band or thin (PF 0.533, n 2076) |
| 130 | D-finalist | RX2_FIRST_MORN | finalist #3 | 1.2/2.0 | break_depth_atr>=1.315204 | -/- | -/- | 1151.0/0.545 | -/- | reject | TRAIN not in band or thin (PF 0.545, n 1151) |
| 131 | D-finalist | RX2_FIRST_MORN | finalist #4 | 1.2/2.0 | quality_score>=131.520446 | -/- | -/- | 1151.0/0.545 | -/- | reject | TRAIN not in band or thin (PF 0.545, n 1151) |
| 132 | D-finalist | RETEST | finalist #5 | 1.5/2.5 | adx_slope3>=-2.0725 | -/- | -/- | 1969.0/0.522 | -/- | reject | TRAIN not in band or thin (PF 0.522, n 1969) |
| 133 | D-finalist | RETEST | finalist #6 | 1.5/2.5 | atr_pct>=0.004455 | -/- | -/- | 1874.0/0.568 | -/- | reject | TRAIN not in band or thin (PF 0.568, n 1874) |
| 134 | D-finalist | RETEST | finalist #7 | 1.5/2.5 | day_ret_pct>=0.267937 | -/- | -/- | 1452.0/0.549 | -/- | reject | TRAIN not in band or thin (PF 0.549, n 1452) |
| 135 | D-finalist | RETEST | finalist #8 | 1.5/2.5 | bars_since_day_low<=17.0 | -/- | -/- | 1987.0/0.517 | -/- | reject | TRAIN not in band or thin (PF 0.517, n 1987) |
| 136 | D-finalist | RETEST | finalist #9 | 1.5/2.5 | break_depth_atr>=0.0 | -/- | -/- | 1896.0/0.523 | -/- | reject | TRAIN not in band or thin (PF 0.523, n 1896) |
| 137 | D-finalist | RETEST | finalist #10 | 1.5/2.5 | day_low_dist_atr<=0.423307 | -/- | -/- | 1506.0/0.518 | -/- | reject | TRAIN not in band or thin (PF 0.518, n 1506) |
| 138 | E-rescue | RETEST | rescue | 1.5/2.5 | gap_pct<=0.868141 | -/- | -/- | 1440.0/0.574 | -/- | reject | TRAIN out of band (PF 0.574, n 1440) |
| 139 | E-rescue | RETEST | rescue | 1.5/2.5 | (none) | -/- | -/- | 2073.0/0.511 | -/- | reject | TRAIN out of band (PF 0.511, n 2073) |
| 140 | E-rescue | RETEST | rescue | 1.5/2.5 | gap_pct<=0.868141 | -/- | -/- | 1680.0/0.442 | -/- | reject | TRAIN out of band (PF 0.442, n 1680) |