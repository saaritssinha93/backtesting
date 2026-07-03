# D_EMA20_REJECTION Candidate Configs

| Rank | Iteration | Decision | TRAIN | TEST | JSON |
|---|---|---|---|---|---|
| 1 | 1 | TRAIN_OUT_OF_BAND_BASELINE | n=118 PF=0.6979 net=-14159.71 | n=20 PF=0.3719 net=-4792.93 | candidate_01_train_out_of_band_baseline.json |
| 2 | 71 | TRAIN_OUT_OF_BAND | n=21 PF=2.243 net=7188.94 | not run | candidate_02_train_out_of_band.json |
| 3 | 65 | TRAIN_OUT_OF_BAND | n=119 PF=1.2147 net=9059.95 | not run | candidate_03_train_out_of_band.json |
| 4 | 51 | REJECT_FIT_VAL | not run | not run | candidate_04_reject_fit_val.json |
| 5 | 54 | REJECT_FIT_VAL | not run | not run | candidate_05_reject_fit_val.json |
| 6 | 64 | REJECT_FIT_VAL | not run | not run | candidate_06_reject_fit_val.json |
| 7 | 69 | REJECT_FIT_VAL | not run | not run | candidate_07_reject_fit_val.json |
| 8 | 60 | REJECT_FIT_VAL | not run | not run | candidate_08_reject_fit_val.json |

Selected config:

```json
{
  "exit": {
    "sl_pct": 1.0,
    "tgt_pct": 3.0
  },
  "mask_terms": [
    [
      "ema20_dist_atr",
      "<=",
      -0.325336
    ]
  ],
  "pre_momentum_terms": [],
  "entry_guards": {
    "max_slot": "12:30"
  }
}
```
