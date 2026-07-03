# DOC5B_MOMO_BREAKOUT_LONG - Tried Config Rescore

Rescored every unique config logged by the 1x1 and 2x2 FIT/VAL searches on full TRAIN first.
TEST was evaluated only for configs whose full TRAIN PF remained inside 1.30-1.70 with at least 20 trades.

- Trial rows read: 700
- Unique configs TRAIN-rescored: 487
- Fast TRAIN-band configs: 1
- Full-confirmed TRAIN-band configs tested on TEST: 1
- TEST PF > 1.40 configs: 0

## Best TRAIN Rescore

- TRAIN fast PF inf over 1 trades, net Rs516.
- Config: `{"daily_loss_rs": 0.0, "entry_guards": {"max_slot": "12:00", "min_slot": "10:00"}, "exit": {"sl_pct": 1.5, "tgt_pct": 0.8}, "mask_terms": [], "max_positions": 20, "pre_momentum_terms": [["pre1_adx", ">=", 45.21998], ["sig5_vol_ratio20", "<=", 1.19053]], "side": "LONG"}`

## Best Confirmed Train-Band Candidate

- TRAIN PF 1.402 over 23 trades, net Rs3,765.
- TEST PF 1.388 over 5 trades, net Rs766.
- Config: `{"daily_loss_rs": 4000.0, "entry_guards": {"max_slot": "12:00", "top_n": 2}, "exit": {"sl_pct": 1.5, "tgt_pct": 2.0}, "mask_terms": [["wick_skew_pct", "<=", 0.041153], ["vol_ratio", ">=", 1.359411]], "max_positions": 20, "pre_momentum_terms": [], "side": "LONG"}`
