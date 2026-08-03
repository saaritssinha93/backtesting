# D_AVWAP_LOSE_REVERSAL Failure Analysis

- No candidate met the full approval gate.
- The closest train-side candidate was not evaluated on TEST because full TRAIN PF did not reach 1.30 with positive net.

Closest robust train-side candidate:

- Iteration: 73
- FIT: n=39 PF=0.946 net=-1068.92
- VAL: n=35 PF=1.8499 net=7988.23
- TRAIN: not run
- TEST: not run

```json
{
  "exit": {
    "sl_pct": 0.9,
    "tgt_pct": 1.5
  },
  "mask_terms": [
    [
      "EMA_50",
      "<=",
      394.997394
    ]
  ],
  "pre_momentum_terms": [
    [
      "pre1_rsi_dir",
      "<=",
      49.395157
    ]
  ],
  "entry_guards": {
    "max_slot": "13:30"
  }
}
```
