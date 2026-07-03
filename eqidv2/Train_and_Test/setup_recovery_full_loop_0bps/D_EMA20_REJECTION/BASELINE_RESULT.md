# D_EMA20_REJECTION Baseline Result

Setup-card gate of record: pre10 <= 0.156614, pre5 >= 0.12493, sig5 ADX >= 20.

| Window | Metrics |
|---|---|
| FIT | n=76 PF=0.997 net=-76.34 |
| VAL | n=42 PF=0.6818 net=-5262.06 |
| TRAIN | n=118 PF=0.8733 net=-5338.4 |
| TEST | n=20 PF=0.585 net=-2430.37 |

```json
{
  "exit": {
    "sl_pct": 0.75,
    "tgt_pct": 1.3
  },
  "mask_terms": [],
  "pre_momentum_terms": [
    [
      "pre10_mom_r",
      "<=",
      0.156614
    ],
    [
      "pre5_mom_r",
      ">=",
      0.12493
    ],
    [
      "sig5_adx_calc",
      ">=",
      20.0
    ]
  ],
  "entry_guards": {}
}
```

Decision: TRAIN_OUT_OF_BAND_BASELINE. Baseline TEST is run because Stage 1 explicitly requires baseline TRAIN/TEST.
