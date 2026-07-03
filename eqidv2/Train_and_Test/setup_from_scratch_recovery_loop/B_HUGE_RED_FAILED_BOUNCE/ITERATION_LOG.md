# B_HUGE_RED_FAILED_BOUNCE (SHORT) — ITERATION_LOG (recovery loop)

_Generated 2026-07-03. From-scratch recovery loop. Research-only; NO live trades; NO final_setup_conf.py edits._

89 iterations; 0 TEST evaluations (budget-capped). Full row-level log: `iteration_log.csv`.

## Stage results (TRAIN confirms + TEST-once rows)

| iter | family | stage | FIT n/PF | VAL n/PF | TRAIN n/PF | TEST n/PF | keep | why |
|---|---|---|---|---|---|---|---|---|

## Top 20 FIT/VAL configs overall

| family | FIT n/PF | VAL n/PF | score | spec |
|---|---|---|---|---|
| F1_exit_engineering | 68/1.006 | 16/1.007 | 1.0052 | `{"entry": "market", "mask_terms": [["gap_pct", "<=", -0.412302], ["regime", "==", "BEAR"]], "premom_terms": [["sig5_adx_calc", "<=` |
| F2_retest_entry | 49/0.981 | 15/1.054 | 0.9226 | `{"entry": ["retest", 0.5, 30], "mask_terms": [["gap_pct", "<=", -0.412302], ["regime", "==", "BEAR"]], "premom_terms": [["sig5_adx` |
| F1_exit_engineering | 68/0.979 | 16/0.937 | 0.9034 | `{"entry": "market", "mask_terms": [["gap_pct", "<=", -0.412302], ["regime", "==", "BEAR"]], "premom_terms": [["sig5_adx_calc", "<=` |
| F4_time_topn | 63/0.97 | 15/1.062 | 0.8964 | `{"entry": "market", "mask_terms": [["gap_pct", "<=", -0.412302], ["regime", "==", "BEAR"]], "premom_terms": [["sig5_adx_calc", "<=` |
| F1_exit_engineering | 68/0.97 | 16/0.921 | 0.8818 | `{"entry": "market", "mask_terms": [["gap_pct", "<=", -0.412302], ["regime", "==", "BEAR"]], "premom_terms": [["sig5_adx_calc", "<=` |
| F1_exit_engineering | 68/0.945 | 16/0.908 | 0.8784 | `{"entry": "market", "mask_terms": [["gap_pct", "<=", -0.412302], ["regime", "==", "BEAR"]], "premom_terms": [["sig5_adx_calc", "<=` |
| F1_exit_engineering | 68/0.941 | 16/0.904 | 0.8744 | `{"entry": "market", "mask_terms": [["gap_pct", "<=", -0.412302], ["regime", "==", "BEAR"]], "premom_terms": [["sig5_adx_calc", "<=` |
| F3_fit_mined_filters | 68/0.941 | 16/0.904 | 0.8744 | `{"entry": "market", "mask_terms": [["gap_pct", "<=", -0.412302], ["regime", "==", "BEAR"], ["gap_pct", "<=", -0.089619]], "premom_` |
| F2_retest_entry | 68/0.941 | 16/0.904 | 0.8744 | `{"entry": "market", "mask_terms": [["gap_pct", "<=", -0.412302], ["regime", "==", "BEAR"]], "premom_terms": [["sig5_adx_calc", "<=` |
| F1_exit_engineering | 68/0.883 | 16/0.92 | 0.8534 | `{"entry": "market", "mask_terms": [["gap_pct", "<=", -0.412302], ["regime", "==", "BEAR"]], "premom_terms": [["sig5_adx_calc", "<=` |
| F1_exit_engineering | 68/0.871 | 16/0.896 | 0.851 | `{"entry": "market", "mask_terms": [["gap_pct", "<=", -0.412302], ["regime", "==", "BEAR"]], "premom_terms": [["sig5_adx_calc", "<=` |
| F3_fit_mined_filters | 56/1.043 | 10/1.285 | 0.8494 | `{"entry": "market", "mask_terms": [["gap_pct", "<=", -0.412302], ["regime", "==", "BEAR"], ["rs_pct", ">=", -0.382957]], "premom_t` |
| F1_exit_engineering | 68/0.91 | 16/0.869 | 0.8362 | `{"entry": "market", "mask_terms": [["gap_pct", "<=", -0.412302], ["regime", "==", "BEAR"]], "premom_terms": [["sig5_adx_calc", "<=` |
| F1_exit_engineering | 68/1.077 | 16/0.938 | 0.8268 | `{"entry": "market", "mask_terms": [["gap_pct", "<=", -0.412302], ["regime", "==", "BEAR"]], "premom_terms": [["sig5_adx_calc", "<=` |
| F1_exit_engineering | 68/0.867 | 16/0.92 | 0.8246 | `{"entry": "market", "mask_terms": [["gap_pct", "<=", -0.412302], ["regime", "==", "BEAR"]], "premom_terms": [["sig5_adx_calc", "<=` |
| F1_exit_engineering | 68/0.908 | 16/0.861 | 0.8234 | `{"entry": "market", "mask_terms": [["gap_pct", "<=", -0.412302], ["regime", "==", "BEAR"]], "premom_terms": [["sig5_adx_calc", "<=` |
| F2_retest_entry | 52/0.849 | 15/0.908 | 0.8018 | `{"entry": ["retest", 0.3, 15], "mask_terms": [["gap_pct", "<=", -0.412302], ["regime", "==", "BEAR"]], "premom_terms": [["sig5_adx` |
| F1_exit_engineering | 68/0.921 | 16/1.08 | 0.7938 | `{"entry": "market", "mask_terms": [["gap_pct", "<=", -0.412302], ["regime", "==", "BEAR"]], "premom_terms": [["sig5_adx_calc", "<=` |
| F2_retest_entry | 49/1.005 | 15/1.309 | 0.7618 | `{"entry": ["retest", 0.5, 30], "mask_terms": [["gap_pct", "<=", -0.412302], ["regime", "==", "BEAR"]], "premom_terms": [["sig5_adx` |
| F4_time_topn | 50/0.858 | 12/0.981 | 0.7596 | `{"entry": "market", "mask_terms": [["gap_pct", "<=", -0.412302], ["regime", "==", "BEAR"]], "premom_terms": [["sig5_adx_calc", "<=` |