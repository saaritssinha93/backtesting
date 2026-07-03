# B_HUGE_RED_FAILED_BOUNCE (SHORT) — PARAMETER_SWEEP_SUMMARY (recovery loop)

_Generated 2026-07-03. From-scratch recovery loop. Research-only; NO live trades; NO final_setup_conf.py edits._

Per-family FIT/VAL outcomes (band objective; higher = closer to a stable PF 1.30-1.80). Rejected ranges are visible as low scores in iteration_log.csv.

## F1_exit_engineering

- configs 54 | best score 1.005 | median 0.541
- best: `{"entry": "market", "mask_terms": [["gap_pct", "<=", -0.412302], ["regime", "==", "BEAR"]], "premom_terms": [["sig5_adx_calc", "<=", 25.661066]], "min_slot": "0`

## F2_retest_entry

- configs 13 | best score 0.923 | median 0.676
- best: `{"entry": ["retest", 0.5, 30], "mask_terms": [["gap_pct", "<=", -0.412302], ["regime", "==", "BEAR"]], "premom_terms": [["sig5_adx_calc", "<=", 25.661066]], "mi`

## F3_fit_mined_filters

- configs 14 | best score 0.874 | median 0.284
- best: `{"entry": "market", "mask_terms": [["gap_pct", "<=", -0.412302], ["regime", "==", "BEAR"], ["gap_pct", "<=", -0.089619]], "premom_terms": [["sig5_adx_calc", "<=`

## F4_time_topn

- configs 8 | best score 0.896 | median -1.813
- best: `{"entry": "market", "mask_terms": [["gap_pct", "<=", -0.412302], ["regime", "==", "BEAR"]], "premom_terms": [["sig5_adx_calc", "<=", 25.661066]], "min_slot": nu`
