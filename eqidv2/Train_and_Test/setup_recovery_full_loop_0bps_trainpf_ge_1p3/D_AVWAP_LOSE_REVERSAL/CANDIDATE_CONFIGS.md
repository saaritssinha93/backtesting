# D_AVWAP_LOSE_REVERSAL Candidate Configs

| Rank | Iteration | Decision | TRAIN | TEST | JSON |
|---|---|---|---|---|---|
| 1 | 1 | TRAIN_OUT_OF_BAND_BASELINE | n=1365 PF=0.8612 net=-84853.73 | n=550 PF=0.9186 net=-17755.33 | candidate_01_train_out_of_band_baseline.json |
| 2 | 73 | REJECT_FIT_VAL | not run | not run | candidate_02_reject_fit_val.json |
| 3 | 59 | REJECT_FIT_VAL | not run | not run | candidate_03_reject_fit_val.json |
| 4 | 35 | REJECT_FIT_VAL | not run | not run | candidate_04_reject_fit_val.json |
| 5 | 56 | REJECT_FIT_VAL | not run | not run | candidate_05_reject_fit_val.json |
| 6 | 52 | REJECT_FIT_VAL | not run | not run | candidate_06_reject_fit_val.json |
| 7 | 58 | REJECT_FIT_VAL | not run | not run | candidate_07_reject_fit_val.json |
| 8 | 61 | REJECT_FIT_VAL | not run | not run | candidate_08_reject_fit_val.json |

Selected config:

```json
{
  "exit": {
    "sl_pct": 0.9,
    "tgt_pct": 1.5
  },
  "mask_terms": [
    [
      "EMA_50",
      "<=",
      394.997394
    ]
  ],
  "pre_momentum_terms": [
    [
      "pre1_rsi_dir",
      "<=",
      49.395157
    ]
  ],
  "entry_guards": {
    "max_slot": "13:30"
  }
}
```
