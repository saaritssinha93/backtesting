# B_HUGE_C1_CLOSE_RECLAIM_BREAK (LONG) — ITERATION_LOG (recovery loop)

_Generated 2026-07-03. From-scratch recovery loop. Research-only; NO live trades; NO final_setup_conf.py edits._

89 iterations; 0 TEST evaluations (budget-capped). Full row-level log: `iteration_log.csv`.

## Stage results (TRAIN confirms + TEST-once rows)

| iter | family | stage | FIT n/PF | VAL n/PF | TRAIN n/PF | TEST n/PF | keep | why |
|---|---|---|---|---|---|---|---|---|

## Top 20 FIT/VAL configs overall

| family | FIT n/PF | VAL n/PF | score | spec |
|---|---|---|---|---|
| F1_exit_engineering | 87/1.057 | 60/1.1 | 1.0226 | `{"entry": "market", "mask_terms": [["regime", "!=", "BULL"]], "premom_terms": [["pre5_mom_r", ">=", 0.546221]], "min_slot": "12:00` |
| F4_time_topn | 110/0.993 | 78/1.008 | 0.981 | `{"entry": "market", "mask_terms": [["regime", "!=", "BULL"]], "premom_terms": [["pre5_mom_r", ">=", 0.546221]], "min_slot": "12:00` |
| F1_exit_engineering | 87/1.046 | 60/1.147 | 0.9652 | `{"entry": "market", "mask_terms": [["regime", "!=", "BULL"]], "premom_terms": [["pre5_mom_r", ">=", 0.546221]], "min_slot": "12:00` |
| F2_retest_entry | 87/1.046 | 60/1.147 | 0.9652 | `{"entry": "market", "mask_terms": [["regime", "!=", "BULL"]], "premom_terms": [["pre5_mom_r", ">=", 0.546221]], "min_slot": "12:00` |
| F4_time_topn | 87/1.046 | 60/1.147 | 0.9652 | `{"entry": "market", "mask_terms": [["regime", "!=", "BULL"]], "premom_terms": [["pre5_mom_r", ">=", 0.546221]], "min_slot": "12:00` |
| F1_exit_engineering | 87/0.98 | 60/1.166 | 0.8312 | `{"entry": "market", "mask_terms": [["regime", "!=", "BULL"]], "premom_terms": [["pre5_mom_r", ">=", 0.546221]], "min_slot": "12:00` |
| F4_time_topn | 122/0.827 | 68/0.813 | 0.8018 | `{"entry": "market", "mask_terms": [["regime", "!=", "BULL"]], "premom_terms": [["pre5_mom_r", ">=", 0.546221]], "min_slot": "10:00` |
| F2_retest_entry | 63/0.883 | 42/0.989 | 0.7982 | `{"entry": ["retest", 0.5, 15], "mask_terms": [["regime", "!=", "BULL"]], "premom_terms": [["pre5_mom_r", ">=", 0.546221]], "min_sl` |
| F4_time_topn | 155/0.801 | 88/0.807 | 0.7962 | `{"entry": "market", "mask_terms": [["regime", "!=", "BULL"]], "premom_terms": [["pre5_mom_r", ">=", 0.546221]], "min_slot": "10:00` |
| F2_retest_entry | 73/0.956 | 46/1.159 | 0.7936 | `{"entry": ["retest", 0.3, 15], "mask_terms": [["regime", "!=", "BULL"]], "premom_terms": [["pre5_mom_r", ">=", 0.546221]], "min_sl` |
| F4_time_topn | 136/0.804 | 77/0.843 | 0.7728 | `{"entry": "market", "mask_terms": [["regime", "!=", "BULL"]], "premom_terms": [["pre5_mom_r", ">=", 0.546221]], "min_slot": null, ` |
| F4_time_topn | 176/0.774 | 100/0.819 | 0.738 | `{"entry": "market", "mask_terms": [["regime", "!=", "BULL"]], "premom_terms": [["pre5_mom_r", ">=", 0.546221]], "min_slot": null, ` |
| F1_exit_engineering | 87/0.951 | 60/1.221 | 0.735 | `{"entry": "market", "mask_terms": [["regime", "!=", "BULL"]], "premom_terms": [["pre5_mom_r", ">=", 0.546221]], "min_slot": "12:00` |
| F2_retest_entry | 77/0.892 | 48/1.097 | 0.728 | `{"entry": ["retest", 0.3, 30], "mask_terms": [["regime", "!=", "BULL"]], "premom_terms": [["pre5_mom_r", ">=", 0.546221]], "min_sl` |
| F2_retest_entry | 69/0.787 | 43/0.985 | 0.6286 | `{"entry": ["retest", 0.5, 30], "mask_terms": [["regime", "!=", "BULL"]], "premom_terms": [["pre5_mom_r", ">=", 0.546221]], "min_sl` |
| F3_fit_mined_filters | 38/1.329 | 23/0.921 | 0.5946 | `{"entry": "market", "mask_terms": [["regime", "!=", "BULL"], ["ema200_dist_atr", "<=", 3.119249]], "premom_terms": [["pre5_mom_r",` |
| F1_exit_engineering | 87/0.78 | 60/1.048 | 0.5656 | `{"entry": "market", "mask_terms": [["regime", "!=", "BULL"]], "premom_terms": [["pre5_mom_r", ">=", 0.546221]], "min_slot": "12:00` |
| F1_exit_engineering | 87/0.799 | 60/1.167 | 0.5046 | `{"entry": "market", "mask_terms": [["regime", "!=", "BULL"]], "premom_terms": [["pre5_mom_r", ">=", 0.546221]], "min_slot": "12:00` |
| F3_fit_mined_filters | 203/0.566 | 183/0.513 | 0.4706 | `{"entry": "market", "mask_terms": [["ema200_dist_atr", "<=", 3.119249], ["or15_lose_atr", "<=", 6.435996]], "premom_terms": [], "m` |
| F1_exit_engineering | 87/0.74 | 60/1.093 | 0.4576 | `{"entry": "market", "mask_terms": [["regime", "!=", "BULL"]], "premom_terms": [["pre5_mom_r", ">=", 0.546221]], "min_slot": "12:00` |