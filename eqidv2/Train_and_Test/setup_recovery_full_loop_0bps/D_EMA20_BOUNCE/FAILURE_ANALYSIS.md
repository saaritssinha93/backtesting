# D_EMA20_BOUNCE Failure Analysis

- No candidate met the full approval gate.
- The closest train-side candidate was not evaluated on TEST because full TRAIN PF did not land in 1.30..1.80.

Closest robust train-side candidate:

- Iteration: 24
- FIT: n=36 PF=1.1529 net=2425.01
- VAL: n=23 PF=0.7899 net=-2036.95
- TRAIN: not run
- TEST: not run

```json
{
  "exit": {
    "sl_pct": 0.7,
    "tgt_pct": 1.5
  },
  "mask_terms": [],
  "pre_momentum_terms": [],
  "entry_guards": {
    "max_slot": "11:30"
  }
}
```
