# DOC5B_MOMO_BREAKOUT_LONG - Tried Config Rescore

Rescored every unique config logged by the 1x1 and 2x2 FIT/VAL searches on full TRAIN first.
TEST was evaluated only for configs whose full TRAIN PF remained inside 1.30-1.70 with at least 20 trades.

- Trial rows read: 700
- Unique configs TRAIN-rescored: 444
- Fast TRAIN-band configs: 0
- Full-confirmed TRAIN-band configs tested on TEST: 0
- TEST PF > 1.40 configs: 0

## Best TRAIN Rescore

- TRAIN fast PF inf over 8 trades, net Rs4,371.
- Config: `{"daily_loss_rs": 0.0, "entry_guards": {"max_slot": "12:30", "min_slot": "10:00"}, "exit": {"sl_pct": 1.5, "tgt_pct": 0.8}, "mask_terms": [["retest_depth_atr", ">=", 0.350946]], "max_positions": 20, "pre_momentum_terms": [["pre5_mom_r", ">=", -0.191487]], "side": "LONG"}`

## Best Confirmed Train-Band Candidate

- None.
