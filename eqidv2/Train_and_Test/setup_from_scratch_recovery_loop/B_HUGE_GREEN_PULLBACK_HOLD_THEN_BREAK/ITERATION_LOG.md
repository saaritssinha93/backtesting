# B_HUGE_GREEN_PULLBACK_HOLD_THEN_BREAK (LONG) — ITERATION_LOG (recovery loop)

_Generated 2026-07-03. From-scratch recovery loop. Research-only; NO live trades; NO final_setup_conf.py edits._

95 iterations; 0 TEST evaluations (budget-capped). Full row-level log: `iteration_log.csv`.

## Stage results (TRAIN confirms + TEST-once rows)

| iter | family | stage | FIT n/PF | VAL n/PF | TRAIN n/PF | TEST n/PF | keep | why |
|---|---|---|---|---|---|---|---|---|
| 55 | F1_exit_engineering | train_confirm | 16/1.701 | 14/1.775 | 30.0/1.735 | -/- | reject | TRAIN domination |
| 56 | F1_exit_engineering | train_confirm | 16/1.513 | 14/1.392 | 30.0/1.458 | -/- | reject | TRAIN domination |
| 57 | F1_exit_engineering | train_confirm | 16/1.437 | 14/1.301 | 30.0/1.372 | -/- | reject | TRAIN domination |
| 71 | F2_retest_entry | train_confirm | 16/1.513 | 14/1.392 | 30.0/1.458 | -/- | reject | TRAIN domination |
| 94 | F4_time_topn | train_confirm | 16/1.513 | 14/1.392 | 30.0/1.458 | -/- | reject | TRAIN domination |
| 95 | F4_time_topn | train_confirm | 17/1.699 | 20/1.382 | 37.0/1.532 | -/- | reject | TRAIN domination |

## Top 20 FIT/VAL configs overall

| family | FIT n/PF | VAL n/PF | score | spec |
|---|---|---|---|---|
| F1_exit_engineering | 16/1.701 | 14/1.775 | 1.6838 | `{"entry": "market", "mask_terms": [], "premom_terms": [["pre3_close_pos", ">=", 0.541682], ["sig5_vol_ratio20", ">=", 2.575953]], ` |
| F1_exit_engineering | 16/1.513 | 14/1.392 | 1.3372 | `{"entry": "market", "mask_terms": [], "premom_terms": [["pre3_close_pos", ">=", 0.541682], ["sig5_vol_ratio20", ">=", 2.575953]], ` |
| F2_retest_entry | 16/1.513 | 14/1.392 | 1.3372 | `{"entry": "market", "mask_terms": [], "premom_terms": [["pre3_close_pos", ">=", 0.541682], ["sig5_vol_ratio20", ">=", 2.575953]], ` |
| F4_time_topn | 16/1.513 | 14/1.392 | 1.3372 | `{"entry": "market", "mask_terms": [], "premom_terms": [["pre3_close_pos", ">=", 0.541682], ["sig5_vol_ratio20", ">=", 2.575953]], ` |
| F1_exit_engineering | 16/1.437 | 14/1.301 | 1.2342 | `{"entry": "market", "mask_terms": [], "premom_terms": [["pre3_close_pos", ">=", 0.541682], ["sig5_vol_ratio20", ">=", 2.575953]], ` |
| F4_time_topn | 17/1.699 | 20/1.382 | 1.1794 | `{"entry": "market", "mask_terms": [], "premom_terms": [["pre3_close_pos", ">=", 0.541682], ["sig5_vol_ratio20", ">=", 2.575953]], ` |
| F2_retest_entry | 13/1.167 | 13/1.28 | 1.0766 | `{"entry": ["retest", 0.3, 15], "mask_terms": [], "premom_terms": [["pre3_close_pos", ">=", 0.541682], ["sig5_vol_ratio20", ">=", 2` |
| F2_retest_entry | 13/1.167 | 13/1.28 | 1.0766 | `{"entry": ["retest", 0.3, 30], "mask_terms": [], "premom_terms": [["pre3_close_pos", ">=", 0.541682], ["sig5_vol_ratio20", ">=", 2` |
| F1_exit_engineering | 16/1.054 | 14/1.093 | 1.0228 | `{"entry": "market", "mask_terms": [], "premom_terms": [["pre3_close_pos", ">=", 0.541682], ["sig5_vol_ratio20", ">=", 2.575953]], ` |
| F1_exit_engineering | 16/1.444 | 14/2.066 | 0.9884 | `{"entry": "market", "mask_terms": [], "premom_terms": [["pre3_close_pos", ">=", 0.541682], ["sig5_vol_ratio20", ">=", 2.575953]], ` |
| F1_exit_engineering | 16/0.963 | 14/0.976 | 0.9526 | `{"entry": "market", "mask_terms": [], "premom_terms": [["pre3_close_pos", ">=", 0.541682], ["sig5_vol_ratio20", ">=", 2.575953]], ` |
| F3_fit_mined_filters | 82/0.99 | 37/1.056 | 0.9372 | `{"entry": "market", "mask_terms": [["day_ret_pct", ">=", 0.582194], ["mfi", ">=", 77.51043]], "premom_terms": [], "min_slot": null` |
| F1_exit_engineering | 16/1.031 | 14/0.961 | 0.905 | `{"entry": "market", "mask_terms": [], "premom_terms": [["pre3_close_pos", ">=", 0.541682], ["sig5_vol_ratio20", ">=", 2.575953]], ` |
| F2_retest_entry | 13/0.982 | 13/1.096 | 0.8908 | `{"entry": ["retest", 0.3, 30], "mask_terms": [], "premom_terms": [["pre3_close_pos", ">=", 0.541682], ["sig5_vol_ratio20", ">=", 2` |
| F2_retest_entry | 13/0.982 | 13/1.096 | 0.8908 | `{"entry": ["retest", 0.3, 15], "mask_terms": [], "premom_terms": [["pre3_close_pos", ">=", 0.541682], ["sig5_vol_ratio20", ">=", 2` |
| F1_exit_engineering | 16/1.221 | 14/1.634 | 0.8906 | `{"entry": "market", "mask_terms": [], "premom_terms": [["pre3_close_pos", ">=", 0.541682], ["sig5_vol_ratio20", ">=", 2.575953]], ` |
| F1_exit_engineering | 16/1.106 | 14/0.981 | 0.881 | `{"entry": "market", "mask_terms": [], "premom_terms": [["pre3_close_pos", ">=", 0.541682], ["sig5_vol_ratio20", ">=", 2.575953]], ` |
| F1_exit_engineering | 16/1.251 | 14/1.035 | 0.8622 | `{"entry": "market", "mask_terms": [], "premom_terms": [["pre3_close_pos", ">=", 0.541682], ["sig5_vol_ratio20", ">=", 2.575953]], ` |
| F1_exit_engineering | 16/1.473 | 14/1.118 | 0.834 | `{"entry": "market", "mask_terms": [], "premom_terms": [["pre3_close_pos", ">=", 0.541682], ["sig5_vol_ratio20", ">=", 2.575953]], ` |
| F1_exit_engineering | 16/0.941 | 14/1.077 | 0.8322 | `{"entry": "market", "mask_terms": [], "premom_terms": [["pre3_close_pos", ">=", 0.541682], ["sig5_vol_ratio20", ">=", 2.575953]], ` |