# DOC5B_MOMO_BREAKOUT_LONG - Tried Config Rescore

Rescored every unique config logged by the 1x1 and 2x2 FIT/VAL searches on full TRAIN first.
TEST was evaluated only for configs whose full TRAIN PF remained inside 1.30-1.70 with at least 20 trades.

- Trial rows read: 800
- Unique configs TRAIN-rescored: 453
- Fast TRAIN-band configs: 0
- Full-confirmed TRAIN-band configs tested on TEST: 0
- TEST PF > 1.40 configs: 0

## Best TRAIN Rescore

- TRAIN fast PF 3.433 over 7 trades, net Rs2,872.
- Config: `{"daily_loss_rs": 4000.0, "entry_guards": {"max_slot": "14:00", "min_slot": "11:00", "top_n": 1}, "exit": {"sl_pct": 0.7, "tgt_pct": 2.5}, "mask_terms": [], "max_positions": 10, "pre_momentum_terms": [["pre3_range_r", "<=", 0.145999], ["sig5_vol_ratio20", "<=", 1.77545]], "side": "LONG"}`

## Best Confirmed Train-Band Candidate

- None.
