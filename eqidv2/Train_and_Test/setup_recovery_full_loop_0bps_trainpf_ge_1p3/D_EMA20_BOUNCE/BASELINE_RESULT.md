# D_EMA20_BOUNCE Baseline Result

Raw v2 EMA20-bounce with v11/v6 exit rule.

| Window | Metrics |
|---|---|
| FIT | n=476 PF=0.7379 net=-49973.74 |
| VAL | n=323 PF=0.59 net=-52840.45 |
| TRAIN | n=799 PF=0.6783 net=-102814.2 |
| TEST | n=296 PF=0.731 net=-28691.63 |

```json
{
  "exit": {
    "sl_pct": 0.7,
    "tgt_pct": 1.5
  },
  "mask_terms": [],
  "pre_momentum_terms": [],
  "entry_guards": {}
}
```

Decision: TRAIN_OUT_OF_BAND_BASELINE. Baseline TEST is run because Stage 1 explicitly requires baseline TRAIN/TEST.
