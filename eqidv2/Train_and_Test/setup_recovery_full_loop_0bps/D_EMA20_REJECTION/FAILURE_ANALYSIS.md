# D_EMA20_REJECTION Failure Analysis

- No candidate met the full approval gate.
- Best eligible TEST PF did not clear 1.40.

Closest robust train-side candidate:

- Iteration: 71
- FIT: n=11 PF=1.7351 net=2929.38
- VAL: n=10 PF=6.1511 net=6037.38
- TRAIN: n=21 PF=2.7387 net=8966.76
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
