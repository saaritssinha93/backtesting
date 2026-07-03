# D_EMA20_REJECTION Failure Analysis

- No candidate met the full approval gate.
- The closest train-side candidate was not evaluated on TEST because full TRAIN PF did not land in 1.30..1.80.

Closest robust train-side candidate:

- Iteration: 71
- FIT: n=11 PF=1.462 net=2045.13
- VAL: n=10 PF=4.79 net=5143.81
- TRAIN: n=21 PF=2.243 net=7188.94
- TEST: not run

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
