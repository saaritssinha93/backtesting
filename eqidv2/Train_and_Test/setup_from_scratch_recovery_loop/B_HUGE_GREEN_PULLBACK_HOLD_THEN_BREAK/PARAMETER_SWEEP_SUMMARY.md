# B_HUGE_GREEN_PULLBACK_HOLD_THEN_BREAK (LONG) — PARAMETER_SWEEP_SUMMARY (recovery loop)

_Generated 2026-07-03. From-scratch recovery loop. Research-only; NO live trades; NO final_setup_conf.py edits._

Per-family FIT/VAL outcomes (band objective; higher = closer to a stable PF 1.30-1.80). Rejected ranges are visible as low scores in iteration_log.csv.

## F1_exit_engineering

- configs 54 | best score 1.684 | median 0.443
- best: `{"entry": "market", "mask_terms": [], "premom_terms": [["pre3_close_pos", ">=", 0.541682], ["sig5_vol_ratio20", ">=", 2.575953]], "min_slot": null, "max_slot": `

## F2_retest_entry

- configs 13 | best score 1.337 | median 0.693
- best: `{"entry": "market", "mask_terms": [], "premom_terms": [["pre3_close_pos", ">=", 0.541682], ["sig5_vol_ratio20", ">=", 2.575953]], "min_slot": null, "max_slot": `

## F3_fit_mined_filters

- configs 14 | best score 0.937 | median 0.285
- best: `{"entry": "market", "mask_terms": [["day_ret_pct", ">=", 0.582194], ["mfi", ">=", 77.51043]], "premom_terms": [], "min_slot": null, "max_slot": null, "top_n": 1`

## F4_time_topn

- configs 8 | best score 1.337 | median 0.491
- best: `{"entry": "market", "mask_terms": [], "premom_terms": [["pre3_close_pos", ">=", 0.541682], ["sig5_vol_ratio20", ">=", 2.575953]], "min_slot": null, "max_slot": `
