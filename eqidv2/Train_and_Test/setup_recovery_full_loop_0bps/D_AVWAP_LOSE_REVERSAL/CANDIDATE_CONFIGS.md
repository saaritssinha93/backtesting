# D_AVWAP_LOSE_REVERSAL Candidate Configs

| Rank | Iteration | Decision | TRAIN | TEST | JSON |
|---|---|---|---|---|---|
| 1 | 1 | TRAIN_OUT_OF_BAND_BASELINE | n=1365 PF=0.8612 net=-84853.73 | n=550 PF=0.9186 net=-17755.33 | candidate_01_train_out_of_band_baseline.json |
| 2 | 75 | REJECT_FIT_VAL | not run | not run | candidate_02_reject_fit_val.json |
| 3 | 71 | REJECT_FIT_VAL | not run | not run | candidate_03_reject_fit_val.json |
| 4 | 59 | REJECT_FIT_VAL | not run | not run | candidate_04_reject_fit_val.json |
| 5 | 35 | REJECT_FIT_VAL | not run | not run | candidate_05_reject_fit_val.json |
| 6 | 56 | REJECT_FIT_VAL | not run | not run | candidate_06_reject_fit_val.json |
| 7 | 52 | REJECT_FIT_VAL | not run | not run | candidate_07_reject_fit_val.json |
| 8 | 58 | REJECT_FIT_VAL | not run | not run | candidate_08_reject_fit_val.json |

Selected config:

```json
{
  "exit": {
    "sl_pct": 0.5,
    "tgt_pct": 3.0
  },
  "mask_terms": [
    [
      "market_abs_ret_pct",
      ">=",
      0.058395
    ]
  ],
  "pre_momentum_terms": [
    [
      "pre5_range_r",
      ">=",
      0.161068
    ]
  ],
  "entry_guards": {
    "max_slot": "11:30"
  }
}
```
