# B_HUGE_FAILED_BOUNCE (SHORT) — PARAMETER_SWEEP_SUMMARY (recovery loop)

_Generated 2026-07-03. From-scratch recovery loop. Research-only; NO live trades; NO final_setup_conf.py edits._

Per-family FIT/VAL outcomes (band objective; higher = closer to a stable PF 1.30-1.80). Rejected ranges are visible as low scores in iteration_log.csv.

## F1_exit_engineering

- configs 54 | best score 1.082 | median 0.477
- best: `{"entry": "market", "mask_terms": [["regime", "!=", "BULL"]], "premom_terms": [["pre3_close_pos", "<=", 0.564802], ["pre3_range_r", ">=", 0.279969]], "min_slot"`

## F2_retest_entry

- configs 13 | best score 1.082 | median 0.545
- best: `{"entry": "market", "mask_terms": [["regime", "!=", "BULL"]], "premom_terms": [["pre3_close_pos", "<=", 0.564802], ["pre3_range_r", ">=", 0.279969]], "min_slot"`

## F3_fit_mined_filters

- configs 14 | best score 1.032 | median 0.409
- best: `{"entry": "market", "mask_terms": [["regime", "!=", "BULL"], ["willr14", ">=", -94.765764]], "premom_terms": [["pre3_close_pos", "<=", 0.564802], ["pre3_range_r`

## F4_time_topn

- configs 8 | best score 1.192 | median 0.662
- best: `{"entry": "market", "mask_terms": [["regime", "!=", "BULL"]], "premom_terms": [["pre3_close_pos", "<=", 0.564802], ["pre3_range_r", ">=", 0.279969]], "min_slot"`
