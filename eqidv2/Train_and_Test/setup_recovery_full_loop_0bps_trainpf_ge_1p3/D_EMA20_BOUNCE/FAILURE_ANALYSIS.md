# D_EMA20_BOUNCE Failure Analysis

- No candidate met the full approval gate.
- The closest train-side candidate was not evaluated on TEST because full TRAIN PF did not reach 1.30 with positive net.

Closest robust train-side candidate:

- Iteration: 75
- FIT: n=50 PF=0.9771 net=-462.49
- VAL: n=20 PF=0.7967 net=-2005.86
- TRAIN: not run
- TEST: not run

```json
{
  "exit": {
    "sl_pct": 1.2,
    "tgt_pct": 1.2
  },
  "mask_terms": [
    [
      "CCI",
      "<=",
      -70.127937
    ]
  ],
  "pre_momentum_terms": [
    [
      "pre2_mom_r",
      ">=",
      0.010152
    ],
    [
      "pre10_close_pos",
      ">=",
      0.375
    ]
  ],
  "entry_guards": {
    "max_slot": "13:30"
  }
}
```
