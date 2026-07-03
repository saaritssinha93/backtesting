# D_AVWAP_LOSE_REVERSAL Failure Analysis

- No candidate met the full approval gate.
- The closest train-side candidate was not evaluated on TEST because full TRAIN PF did not land in 1.30..1.80.

Closest robust train-side candidate:

- Iteration: 75
- FIT: n=192 PF=0.9675 net=-2424.6
- VAL: n=134 PF=1.5061 net=21757.26
- TRAIN: not run
- TEST: not run

```json
{
  "exit": {
    "sl_pct": 0.5,
    "tgt_pct": 3.0
  },
  "mask_terms": [
    [
      "market_abs_ret_pct",
      ">=",
      0.058395
    ]
  ],
  "pre_momentum_terms": [
    [
      "pre5_range_r",
      ">=",
      0.161068
    ]
  ],
  "entry_guards": {
    "max_slot": "11:30"
  }
}
```
