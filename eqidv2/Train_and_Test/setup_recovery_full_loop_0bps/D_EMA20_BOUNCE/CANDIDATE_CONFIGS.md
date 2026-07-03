# D_EMA20_BOUNCE Candidate Configs

| Rank | Iteration | Decision | TRAIN | TEST | JSON |
|---|---|---|---|---|---|
| 1 | 1 | TRAIN_OUT_OF_BAND_BASELINE | n=799 PF=0.6783 net=-102814.2 | n=296 PF=0.731 net=-28691.63 | candidate_01_train_out_of_band_baseline.json |
| 2 | 24 | REJECT_FIT_VAL | not run | not run | candidate_02_reject_fit_val.json |
| 3 | 63 | REJECT_FIT_VAL | not run | not run | candidate_03_reject_fit_val.json |
| 4 | 40 | REJECT_FIT_VAL | not run | not run | candidate_04_reject_fit_val.json |
| 5 | 48 | REJECT_FIT_VAL | not run | not run | candidate_05_reject_fit_val.json |
| 6 | 58 | REJECT_FIT_VAL | not run | not run | candidate_06_reject_fit_val.json |
| 7 | 47 | REJECT_FIT_VAL | not run | not run | candidate_07_reject_fit_val.json |
| 8 | 26 | REJECT_FIT_VAL | not run | not run | candidate_08_reject_fit_val.json |

Selected config:

```json
{
  "exit": {
    "sl_pct": 0.7,
    "tgt_pct": 1.5
  },
  "mask_terms": [],
  "pre_momentum_terms": [],
  "entry_guards": {
    "max_slot": "11:30"
  }
}
```
