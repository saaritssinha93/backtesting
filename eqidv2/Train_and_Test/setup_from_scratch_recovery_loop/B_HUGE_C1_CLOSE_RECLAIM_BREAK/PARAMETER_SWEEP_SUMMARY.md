# B_HUGE_C1_CLOSE_RECLAIM_BREAK (LONG) — PARAMETER_SWEEP_SUMMARY (recovery loop)

_Generated 2026-07-03. From-scratch recovery loop. Research-only; NO live trades; NO final_setup_conf.py edits._

Per-family FIT/VAL outcomes (band objective; higher = closer to a stable PF 1.30-1.80). Rejected ranges are visible as low scores in iteration_log.csv.

## F1_exit_engineering

- configs 54 | best score 1.023 | median 0.102
- best: `{"entry": "market", "mask_terms": [["regime", "!=", "BULL"]], "premom_terms": [["pre5_mom_r", ">=", 0.546221]], "min_slot": "12:00", "max_slot": "14:30", "top_n`

## F2_retest_entry

- configs 13 | best score 0.965 | median 0.433
- best: `{"entry": "market", "mask_terms": [["regime", "!=", "BULL"]], "premom_terms": [["pre5_mom_r", ">=", 0.546221]], "min_slot": "12:00", "max_slot": "14:30", "top_n`

## F3_fit_mined_filters

- configs 14 | best score 0.595 | median 0.171
- best: `{"entry": "market", "mask_terms": [["regime", "!=", "BULL"], ["ema200_dist_atr", "<=", 3.119249]], "premom_terms": [["pre5_mom_r", ">=", 0.546221]], "min_slot":`

## F4_time_topn

- configs 8 | best score 0.981 | median 0.784
- best: `{"entry": "market", "mask_terms": [["regime", "!=", "BULL"]], "premom_terms": [["pre5_mom_r", ">=", 0.546221]], "min_slot": "12:00", "max_slot": null, "top_n": `
