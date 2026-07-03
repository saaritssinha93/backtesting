# D_EMA20_REJECTION Baseline Result

Setup-card gate of record: pre10 <= 0.156614, pre5 >= 0.12493, sig5 ADX >= 20.

| Window | Metrics |
|---|---|
| FIT | n=76 PF=0.799 net=-5764.39 |
| VAL | n=42 PF=0.5387 net=-8395.31 |
| TRAIN | n=118 PF=0.6979 net=-14159.71 |
| TEST | n=20 PF=0.3719 net=-4792.93 |

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
