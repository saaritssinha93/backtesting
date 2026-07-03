# D_AVWAP_LOSE_REVERSAL Failure Analysis

- No candidate met the full approval gate.
- The closest train-side candidate was not evaluated on TEST because full TRAIN PF did not land in 1.30..1.80.

Closest robust train-side candidate:

- Iteration: 72
- FIT: n=39 PF=0.7774 net=-4931.22
- VAL: n=35 PF=1.4978 net=5260.68
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
