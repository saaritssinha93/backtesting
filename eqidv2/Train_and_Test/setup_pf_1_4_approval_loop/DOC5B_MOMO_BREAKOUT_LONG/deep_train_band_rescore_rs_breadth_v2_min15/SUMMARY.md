# DOC5B_MOMO_BREAKOUT_LONG - Tried Config Rescore

Rescored every unique config logged by the 1x1 and 2x2 FIT/VAL searches on full TRAIN first.
TEST was evaluated only for configs whose full TRAIN PF remained inside 1.30-1.70 with at least 15 trades.

- Trial rows read: 800
- Unique configs TRAIN-rescored: 736
- Fast TRAIN-band configs: 1
- Full-confirmed TRAIN-band configs tested on TEST: 1
- TEST PF > 1.40 configs: 0

## Best TRAIN Rescore

- TRAIN fast PF inf over 2 trades, net Rs727.
- Config: `{"daily_loss_rs": 0.0, "entry_guards": {"min_slot": "09:30", "top_n": 1}, "exit": {"sl_pct": 1.1, "tgt_pct": 0.6}, "mask_terms": [["ranker_score", ">=", 109.099605], ["wick_skew_pct", "<=", -0.08808]], "max_positions": 20, "pre_momentum_terms": [["pre_entry_momentum_score", "<=", 41.157639], ["pre1_adx", "<=", 46.14275]], "side": "LONG"}`

## Best Confirmed Train-Band Candidate

- TRAIN PF 1.636 over 18 trades, net Rs4,441.
- TEST PF 0.0 over 5 trades, net Rs-4,163.
- Config: `{"daily_loss_rs": 0.0, "entry_guards": {"max_slot": "13:00", "min_slot": "10:30", "top_n": 2}, "exit": {"sl_pct": 1.2, "tgt_pct": 2.0}, "mask_terms": [["stock_ret_pct", ">=", 1.540024], ["close_loc", ">=", 0.831579]], "max_positions": 20, "pre_momentum_terms": [["sig5_vol_ratio20", ">=", 1.959559], ["sig5_rsi_dir", "<=", 76.419963]], "side": "LONG"}`
