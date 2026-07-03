# B_AVWAP_RECLAIM_REVERSAL (LONG) — ITERATION_LOG (recovery loop)

_Generated 2026-07-03. From-scratch recovery loop. Research-only; NO live trades; NO final_setup_conf.py edits._

89 iterations; 0 TEST evaluations (budget-capped). Full row-level log: `iteration_log.csv`.

## Stage results (TRAIN confirms + TEST-once rows)

| iter | family | stage | FIT n/PF | VAL n/PF | TRAIN n/PF | TEST n/PF | keep | why |
|---|---|---|---|---|---|---|---|---|

## Top 20 FIT/VAL configs overall

| family | FIT n/PF | VAL n/PF | score | spec |
|---|---|---|---|---|
| F4_time_topn | 18/1.266 | 17/1.105 | 0.9762 | `{"entry": "market", "mask_terms": [["macd_atr", ">=", 0.314566], ["regime", "!=", "BULL"], ["signal_range_pct", ">=", 1.149468]], ` |
| F4_time_topn | 15/1.416 | 17/1.105 | 0.8562 | `{"entry": "market", "mask_terms": [["macd_atr", ">=", 0.314566], ["regime", "!=", "BULL"], ["signal_range_pct", ">=", 1.149468]], ` |
| F1_exit_engineering | 44/0.743 | 27/0.758 | 0.731 | `{"entry": "market", "mask_terms": [["macd_atr", ">=", 0.314566], ["regime", "!=", "BULL"], ["signal_range_pct", ">=", 1.149468]], ` |
| F2_retest_entry | 37/0.798 | 24/0.756 | 0.7224 | `{"entry": ["retest", 0.3, 15], "mask_terms": [["macd_atr", ">=", 0.314566], ["regime", "!=", "BULL"], ["signal_range_pct", ">=", 1` |
| F1_exit_engineering | 44/0.733 | 27/0.878 | 0.617 | `{"entry": "market", "mask_terms": [["macd_atr", ">=", 0.314566], ["regime", "!=", "BULL"], ["signal_range_pct", ">=", 1.149468]], ` |
| F2_retest_entry | 44/0.733 | 27/0.878 | 0.617 | `{"entry": "market", "mask_terms": [["macd_atr", ">=", 0.314566], ["regime", "!=", "BULL"], ["signal_range_pct", ">=", 1.149468]], ` |
| F4_time_topn | 44/0.733 | 27/0.878 | 0.617 | `{"entry": "market", "mask_terms": [["macd_atr", ">=", 0.314566], ["regime", "!=", "BULL"], ["signal_range_pct", ">=", 1.149468]], ` |
| F1_exit_engineering | 44/0.716 | 27/0.841 | 0.616 | `{"entry": "market", "mask_terms": [["macd_atr", ">=", 0.314566], ["regime", "!=", "BULL"], ["signal_range_pct", ">=", 1.149468]], ` |
| F4_time_topn | 42/0.748 | 26/0.94 | 0.5944 | `{"entry": "market", "mask_terms": [["macd_atr", ">=", 0.314566], ["regime", "!=", "BULL"], ["signal_range_pct", ">=", 1.149468]], ` |
| F2_retest_entry | 40/0.832 | 26/0.699 | 0.5926 | `{"entry": ["retest", 0.3, 30], "mask_terms": [["macd_atr", ">=", 0.314566], ["regime", "!=", "BULL"], ["signal_range_pct", ">=", 1` |
| F4_time_topn | 39/0.691 | 27/0.878 | 0.5414 | `{"entry": "market", "mask_terms": [["macd_atr", ">=", 0.314566], ["regime", "!=", "BULL"], ["signal_range_pct", ">=", 1.149468]], ` |
| F2_retest_entry | 40/0.594 | 26/0.67 | 0.5332 | `{"entry": ["retest", 0.3, 30], "mask_terms": [["macd_atr", ">=", 0.314566], ["regime", "!=", "BULL"], ["signal_range_pct", ">=", 1` |
| F4_time_topn | 37/0.705 | 26/0.94 | 0.517 | `{"entry": "market", "mask_terms": [["macd_atr", ">=", 0.314566], ["regime", "!=", "BULL"], ["signal_range_pct", ">=", 1.149468]], ` |
| F1_exit_engineering | 44/0.689 | 27/0.955 | 0.4762 | `{"entry": "market", "mask_terms": [["macd_atr", ">=", 0.314566], ["regime", "!=", "BULL"], ["signal_range_pct", ">=", 1.149468]], ` |
| F1_exit_engineering | 44/0.638 | 27/0.545 | 0.4706 | `{"entry": "market", "mask_terms": [["macd_atr", ">=", 0.314566], ["regime", "!=", "BULL"], ["signal_range_pct", ">=", 1.149468]], ` |
| F2_retest_entry | 31/0.737 | 21/0.588 | 0.4688 | `{"entry": ["retest", 0.5, 15], "mask_terms": [["macd_atr", ">=", 0.314566], ["regime", "!=", "BULL"], ["signal_range_pct", ">=", 1` |
| F2_retest_entry | 31/0.617 | 21/0.505 | 0.4154 | `{"entry": ["retest", 0.5, 15], "mask_terms": [["macd_atr", ">=", 0.314566], ["regime", "!=", "BULL"], ["signal_range_pct", ">=", 1` |
| F2_retest_entry | 37/0.562 | 24/0.758 | 0.4052 | `{"entry": ["retest", 0.3, 15], "mask_terms": [["macd_atr", ">=", 0.314566], ["regime", "!=", "BULL"], ["signal_range_pct", ">=", 1` |
| F1_exit_engineering | 44/0.632 | 27/0.486 | 0.3692 | `{"entry": "market", "mask_terms": [["macd_atr", ">=", 0.314566], ["regime", "!=", "BULL"], ["signal_range_pct", ">=", 1.149468]], ` |
| F2_retest_entry | 36/0.811 | 23/0.542 | 0.3268 | `{"entry": ["retest", 0.5, 30], "mask_terms": [["macd_atr", ">=", 0.314566], ["regime", "!=", "BULL"], ["signal_range_pct", ">=", 1` |