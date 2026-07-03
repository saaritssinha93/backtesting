# A_MOD_BREAK_C1_LOW - Tried Config Rescore

Rescored every unique config logged by 1 trial file(s) on TRAIN first.
TEST was evaluated only for configs whose full TRAIN PF remained inside 1.30-1.70 with at least 20 trades.

- Trial rows read: 700
- Unique configs TRAIN-rescored: 443
- Fast TRAIN-band configs: 1
- Full-confirmed TRAIN-band configs tested on TEST: 1
- TEST PF > 1.40 configs: 0

## Best TRAIN Rescore

- TRAIN fast PF 2.359 over 8 trades, net Rs3,105.
- Config: `{"daily_loss_rs": 0.0, "entry_guards": {"top_n": 1}, "exit": {"sl_pct": 1.1, "tgt_pct": 2.5}, "mask_terms": [["body_pct", ">=", 0.976744], ["upper_wick_pct", ">=", 0.0]], "max_positions": 20, "pre_momentum_terms": [["sig5_vol_ratio20", "<=", 1.49538], ["sig5_adx_calc", ">=", 25.610071]], "side": "LONG"}`

## Best Confirmed Train-Band Candidate

- TRAIN PF 1.36 over 36 trades, net Rs5,169.
- TEST PF inf over 1 trades, net Rs2,252.
- Config: `{"daily_loss_rs": 0.0, "entry_guards": {"top_n": 1}, "exit": {"sl_pct": 1.1, "tgt_pct": 2.5}, "mask_terms": [["close_loc", ">=", 0.324324], ["signal_range_pct", ">=", 0.344568]], "max_positions": 20, "pre_momentum_terms": [], "side": "LONG"}`
