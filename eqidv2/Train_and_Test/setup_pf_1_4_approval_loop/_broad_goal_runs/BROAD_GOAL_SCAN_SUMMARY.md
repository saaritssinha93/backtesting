# Broad PF-Band Scan Summary

_Generated 2026-07-01. Research-only. No `final_setup_conf.py` edits._

## Gate Used

- TRAIN PF must be controlled inside 1.30-1.70.
- TRAIN trade count must be meaningful, with at least 20 trades for the strict rescoring checks.
- TEST PF must be greater than 1.40, with at least 6 TEST trades.
- Reject if dominated by one trade, one day, or one symbol.
- Reject if neighborhood or term-dropout robustness fails.
- TEST thresholds are not used to tune filters.

## Main Result

No setup cleared the approval gate.

DOC5B_MOMO_BREAKOUT_LONG was the requested setup. The base breakout, RS/breadth v2 rescan, and retest v3 rescan all failed. The strict TRAIN-first rescoring also found zero meaningful DOC5B configs in the 1.30-1.70 TRAIN PF band.

The broader dated-pool probes also failed. These pools generally had only two true sessions on or after 2026-06-20, so the optimizer fell back to the last five available sessions for TEST where needed. Treat those broader probes as extra evidence, not clean long-holdout approval.

## Best Selected Configs From Runs

| run | verdict | train n | train PF | test n | test PF | key reject |
|---|---:|---:|---:|---:|---:|---|
| DOC5B base | REJECT | 125 | 0.488 | 48 | 0.211 | TRAIN PF too low; target-fill too low |
| DOC5B RS/breadth v2 | REJECT | 29 | 0.653 | 10 | 0.329 | TRAIN PF too low; target-fill too low |
| DOC5B retest v3 | REJECT | 12 | 1.816 | 4 | 0.000 | too few trades; TRAIN PF above band |
| A_MOD_BREAK_C1_LOW | REJECT | 43 | 1.067 | 10 | 0.002 | TRAIN PF too low; TEST failed |
| G_HIGHER_HIGH_BREAK | REJECT | 20 | 1.494 | 12 | 1.194 | TEST PF below 1.40; robustness failed |
| L_PRESSURE_BURST_VWAP | REJECT | 103 | 0.528 | 29 | 0.510 | TRAIN PF too low |
| B_AVWAP_RECLAIM_REVERSAL | REJECT | 17 | 1.203 | 2 | 0.000 | too few TRAIN/TEST trades |
| L_DOUBLE_BOTTOM_VWAP | REJECT | 34 | 0.929 | 4 | 0.000 | TRAIN PF too low; TEST too few |
| B_HUGE_C1_CLOSE_RECLAIM_BREAK | REJECT | 21 | 1.779 | 0 | 0.000 | TRAIN PF above band; zero TEST trades |
| B_HUGE_RED_FAILED_BOUNCE | REJECT | 18 | 1.113 | 6 | 0.039 | TRAIN too few/too low; TEST failed |
| G_LOWER_LOW_BREAK | REJECT | 50 | 1.077 | 0 | 0.000 | TRAIN PF too low; zero TEST trades |

## TRAIN-First Rescore Checks

| rescore | unique configs | full train-band configs | TEST PF > 1.40 configs | best confirmed |
|---|---:|---:|---:|---|
| DOC5B base tried configs | 453 | 0 | 0 | none |
| DOC5B RS/breadth v2 strict | 736 | 0 | 0 | none |
| DOC5B retest v3 strict | 444 | 0 | 0 | none |
| A_MOD tried configs | 443 | 1 | 0 | TRAIN 36 PF 1.360; TEST 1 PF inf, but too few TEST trades |
| G_HIGHER tried configs | 501 | 29 | 0 | TRAIN 25 PF 1.374; TEST 7 PF 0.992 |
| B_HUGE_C1 tried configs | 467 | 1 | 0 | TRAIN 25 PF 1.321; TEST 1 PF 0.000 |

## Conclusion

Do not promote any config from this scan.

For DOC5B specifically, the raw momentum breakout has a persistent negative expectancy, and the more selective variants either remain below the TRAIN PF band or become too thin and fail OOS. The best practical next step is not tighter parameter fitting; it is either more out-of-sample data after 2026-06-20 or a structural redesign of the detector before another approval loop.
