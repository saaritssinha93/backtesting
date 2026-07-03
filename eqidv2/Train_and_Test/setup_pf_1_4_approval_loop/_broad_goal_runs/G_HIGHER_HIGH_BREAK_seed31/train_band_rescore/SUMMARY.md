# G_HIGHER_HIGH_BREAK - Tried Config Rescore

Rescored every unique config logged by 1 trial file(s) on TRAIN first.
TEST was evaluated only for configs whose full TRAIN PF remained inside 1.30-1.70 with at least 20 trades.

- Trial rows read: 700
- Unique configs TRAIN-rescored: 501
- Fast TRAIN-band configs: 29
- Full-confirmed TRAIN-band configs tested on TEST: 29
- TEST PF > 1.40 configs: 0

## Best TRAIN Rescore

- TRAIN fast PF inf over 5 trades, net Rs5,686.
- Config: `{"daily_loss_rs": 0.0, "entry_guards": {"min_slot": "11:00", "top_n": 2}, "exit": {"sl_pct": 1.2, "tgt_pct": 1.5}, "mask_terms": [["body_pct", "<=", 0.893093], ["rs_pct", "<=", 1.017429]], "max_positions": 20, "pre_momentum_terms": [["pre3_range_r", "<=", 0.401292], ["sig5_vol_ratio20", "<=", 0.676769]], "side": "LONG"}`

## Best Confirmed Train-Band Candidate

- TRAIN PF 1.374 over 25 trades, net Rs3,297.
- TEST PF 0.992 over 7 trades, net Rs-28.
- Config: `{"daily_loss_rs": 4000.0, "entry_guards": {"max_slot": "14:30", "min_slot": "11:00", "top_n": 2}, "exit": {"sl_pct": 1.2, "tgt_pct": 1.5}, "mask_terms": [["close_loc", "<=", 0.953488], ["vwap_dist_atr", "<=", 3.401347]], "max_positions": 20, "pre_momentum_terms": [["pre1_adx", "<=", 40.170062]], "side": "LONG"}`
