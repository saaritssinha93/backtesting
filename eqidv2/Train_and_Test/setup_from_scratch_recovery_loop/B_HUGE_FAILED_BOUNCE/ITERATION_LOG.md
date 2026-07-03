# B_HUGE_FAILED_BOUNCE (SHORT) — ITERATION_LOG (recovery loop)

_Generated 2026-07-03. From-scratch recovery loop. Research-only; NO live trades; NO final_setup_conf.py edits._

89 iterations; 0 TEST evaluations (budget-capped). Full row-level log: `iteration_log.csv`.

## Stage results (TRAIN confirms + TEST-once rows)

| iter | family | stage | FIT n/PF | VAL n/PF | TRAIN n/PF | TEST n/PF | keep | why |
|---|---|---|---|---|---|---|---|---|

## Top 20 FIT/VAL configs overall

| family | FIT n/PF | VAL n/PF | score | spec |
|---|---|---|---|---|
| F4_time_topn | 25/1.208 | 31/1.228 | 1.192 | `{"entry": "market", "mask_terms": [["regime", "!=", "BULL"]], "premom_terms": [["pre3_close_pos", "<=", 0.564802], ["pre3_range_r"` |
| F2_retest_entry | 22/1.343 | 21/1.198 | 1.082 | `{"entry": "market", "mask_terms": [["regime", "!=", "BULL"]], "premom_terms": [["pre3_close_pos", "<=", 0.564802], ["pre3_range_r"` |
| F1_exit_engineering | 22/1.343 | 21/1.198 | 1.082 | `{"entry": "market", "mask_terms": [["regime", "!=", "BULL"]], "premom_terms": [["pre3_close_pos", "<=", 0.564802], ["pre3_range_r"` |
| F3_fit_mined_filters | 24/1.078 | 20/1.135 | 1.0324 | `{"entry": "market", "mask_terms": [["regime", "!=", "BULL"], ["willr14", ">=", -94.765764]], "premom_terms": [["pre3_close_pos", "` |
| F4_time_topn | 29/1.089 | 32/1.224 | 0.981 | `{"entry": "market", "mask_terms": [["regime", "!=", "BULL"]], "premom_terms": [["pre3_close_pos", "<=", 0.564802], ["pre3_range_r"` |
| F4_time_topn | 32/1.029 | 42/1.242 | 0.8586 | `{"entry": "market", "mask_terms": [["regime", "!=", "BULL"]], "premom_terms": [["pre3_close_pos", "<=", 0.564802], ["pre3_range_r"` |
| F1_exit_engineering | 22/1.046 | 21/0.933 | 0.8426 | `{"entry": "market", "mask_terms": [["regime", "!=", "BULL"]], "premom_terms": [["pre3_close_pos", "<=", 0.564802], ["pre3_range_r"` |
| F1_exit_engineering | 22/1.065 | 21/0.933 | 0.8274 | `{"entry": "market", "mask_terms": [["regime", "!=", "BULL"]], "premom_terms": [["pre3_close_pos", "<=", 0.564802], ["pre3_range_r"` |
| F1_exit_engineering | 22/1.031 | 21/0.914 | 0.8204 | `{"entry": "market", "mask_terms": [["regime", "!=", "BULL"]], "premom_terms": [["pre3_close_pos", "<=", 0.564802], ["pre3_range_r"` |
| F1_exit_engineering | 22/1.05 | 21/0.914 | 0.8052 | `{"entry": "market", "mask_terms": [["regime", "!=", "BULL"]], "premom_terms": [["pre3_close_pos", "<=", 0.564802], ["pre3_range_r"` |
| F1_exit_engineering | 22/0.817 | 21/0.832 | 0.805 | `{"entry": "market", "mask_terms": [["regime", "!=", "BULL"]], "premom_terms": [["pre3_close_pos", "<=", 0.564802], ["pre3_range_r"` |
| F4_time_topn | 33/0.879 | 40/0.987 | 0.7926 | `{"entry": "market", "mask_terms": [["regime", "!=", "BULL"]], "premom_terms": [["pre3_close_pos", "<=", 0.564802], ["pre3_range_r"` |
| F2_retest_entry | 16/1.437 | 18/1.049 | 0.7386 | `{"entry": ["retest", 0.3, 15], "mask_terms": [["regime", "!=", "BULL"]], "premom_terms": [["pre3_close_pos", "<=", 0.564802], ["pr` |
| F2_retest_entry | 14/0.814 | 15/0.913 | 0.7348 | `{"entry": ["retest", 0.8, 30], "mask_terms": [["regime", "!=", "BULL"]], "premom_terms": [["pre3_close_pos", "<=", 0.564802], ["pr` |
| F1_exit_engineering | 22/0.798 | 21/0.893 | 0.722 | `{"entry": "market", "mask_terms": [["regime", "!=", "BULL"]], "premom_terms": [["pre3_close_pos", "<=", 0.564802], ["pre3_range_r"` |
| F1_exit_engineering | 22/0.698 | 21/0.688 | 0.68 | `{"entry": "market", "mask_terms": [["regime", "!=", "BULL"]], "premom_terms": [["pre3_close_pos", "<=", 0.564802], ["pre3_range_r"` |
| F1_exit_engineering | 22/1.334 | 21/0.97 | 0.6788 | `{"entry": "market", "mask_terms": [["regime", "!=", "BULL"]], "premom_terms": [["pre3_close_pos", "<=", 0.564802], ["pre3_range_r"` |
| F1_exit_engineering | 22/0.729 | 21/0.8 | 0.6722 | `{"entry": "market", "mask_terms": [["regime", "!=", "BULL"]], "premom_terms": [["pre3_close_pos", "<=", 0.564802], ["pre3_range_r"` |
| F2_retest_entry | 10/0.779 | 13/0.924 | 0.663 | `{"entry": ["retest", 0.8, 15], "mask_terms": [["regime", "!=", "BULL"]], "premom_terms": [["pre3_close_pos", "<=", 0.564802], ["pr` |
| F1_exit_engineering | 22/0.666 | 21/0.688 | 0.6484 | `{"entry": "market", "mask_terms": [["regime", "!=", "BULL"]], "premom_terms": [["pre3_close_pos", "<=", 0.564802], ["pre3_range_r"` |