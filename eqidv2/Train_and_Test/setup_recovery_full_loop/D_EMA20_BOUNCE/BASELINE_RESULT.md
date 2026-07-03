# D_EMA20_BOUNCE Baseline Result

Raw v2 EMA20-bounce with v11/v6 exit rule.

| Window | Metrics |
|---|---|
| FIT | n=486 PF=0.5551 net=-99646.29 |
| VAL | n=324 PF=0.4251 net=-86398.27 |
| TRAIN | n=810 PF=0.5029 net=-186044.56 |
| TEST | n=296 PF=0.5242 net=-60440.79 |

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
