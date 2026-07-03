# B_AVWAP_RECLAIM_REVERSAL (LONG) — PARAMETER_SWEEP_SUMMARY (recovery loop)

_Generated 2026-07-03. From-scratch recovery loop. Research-only; NO live trades; NO final_setup_conf.py edits._

Per-family FIT/VAL outcomes (band objective; higher = closer to a stable PF 1.30-1.80). Rejected ranges are visible as low scores in iteration_log.csv.

## F1_exit_engineering

- configs 54 | best score 0.731 | median 0.150
- best: `{"entry": "market", "mask_terms": [["macd_atr", ">=", 0.314566], ["regime", "!=", "BULL"], ["signal_range_pct", ">=", 1.149468]], "premom_terms": [], "min_slot"`

## F2_retest_entry

- configs 13 | best score 0.722 | median 0.405
- best: `{"entry": ["retest", 0.3, 15], "mask_terms": [["macd_atr", ">=", 0.314566], ["regime", "!=", "BULL"], ["signal_range_pct", ">=", 1.149468]], "premom_terms": [],`

## F3_fit_mined_filters

- configs 14 | best score 0.217 | median 0.058
- best: `{"entry": "market", "mask_terms": [["or15_lose_atr", "<=", 4.417483]], "premom_terms": [], "min_slot": null, "max_slot": null, "top_n": 1, "be": null, "trail": `

## F4_time_topn

- configs 8 | best score 0.976 | median 0.568
- best: `{"entry": "market", "mask_terms": [["macd_atr", ">=", 0.314566], ["regime", "!=", "BULL"], ["signal_range_pct", ">=", 1.149468]], "premom_terms": [], "min_slot"`
