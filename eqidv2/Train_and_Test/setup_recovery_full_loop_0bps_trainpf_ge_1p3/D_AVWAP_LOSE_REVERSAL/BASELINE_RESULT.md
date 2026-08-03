# D_AVWAP_LOSE_REVERSAL Baseline Result

Raw v2 VWAP-loss reversal with v11/v6 exit rule.

| Window | Metrics |
|---|---|
| FIT | n=770 PF=0.7843 net=-81420.28 |
| VAL | n=595 PF=0.9853 net=-3433.45 |
| TRAIN | n=1365 PF=0.8612 net=-84853.73 |
| TEST | n=550 PF=0.9186 net=-17755.33 |

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
