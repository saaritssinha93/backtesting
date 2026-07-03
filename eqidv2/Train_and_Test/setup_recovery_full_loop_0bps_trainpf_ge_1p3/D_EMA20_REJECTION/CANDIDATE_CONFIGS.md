# D_EMA20_REJECTION Candidate Configs

| Rank | Iteration | Decision | TRAIN | TEST | JSON |
|---|---|---|---|---|---|
| 1 | 71 | TEST_FAIL_OR_DOMINATED | n=21 PF=2.7387 net=8966.76 | n=9 PF=1.0312 net=138.88 | candidate_01_test_fail_or_dominated.json |
| 2 | 65 | TEST_FAIL_OR_DOMINATED | n=119 PF=1.5413 net=20571.04 | n=43 PF=0.7389 net=-4705.65 | candidate_02_test_fail_or_dominated.json |
| 3 | 1 | TRAIN_OUT_OF_BAND_BASELINE | n=118 PF=0.8733 net=-5338.4 | n=20 PF=0.585 net=-2430.37 | candidate_03_train_out_of_band_baseline.json |
| 4 | 51 | TRAIN_OUT_OF_BAND | n=222 PF=1.1792 net=12661.24 | not run | candidate_04_train_out_of_band.json |
| 5 | 54 | TRAIN_OUT_OF_BAND | n=294 PF=1.0365 net=3710.2 | not run | candidate_05_train_out_of_band.json |
| 6 | 61 | REJECT_FIT_VAL | not run | not run | candidate_06_reject_fit_val.json |
| 7 | 64 | REJECT_FIT_VAL | not run | not run | candidate_07_reject_fit_val.json |
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
