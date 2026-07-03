# D_AVWAP_LOSE_REVERSAL Baseline Result

Raw v2 VWAP-loss reversal with v11/v6 exit rule.

| Window | Metrics |
|---|---|
| FIT | n=778 PF=0.6263 net=-156659.63 |
| VAL | n=603 PF=0.752 net=-67404.87 |
| TRAIN | n=1381 PF=0.6757 net=-224064.5 |
| TEST | n=542 PF=0.7522 net=-57725.0 |

```json
{
  "exit": {
    "sl_pct": 1.0,
    "tgt_pct": 1.5
  },
  "mask_terms": [],
  "pre_momentum_terms": [],
  "entry_guards": {}
}
```

Decision: TRAIN_OUT_OF_BAND_BASELINE. Baseline TEST is run because Stage 1 explicitly requires baseline TRAIN/TEST.
