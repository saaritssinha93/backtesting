# B_HUGE_C1_CLOSE_RECLAIM_BREAK - Tried Config Rescore

Rescored every unique config logged by 1 trial file(s) on TRAIN first.
TEST was evaluated only for configs whose full TRAIN PF remained inside 1.30-1.70 with at least 20 trades.

- Trial rows read: 700
- Unique configs TRAIN-rescored: 467
- Fast TRAIN-band configs: 1
- Full-confirmed TRAIN-band configs tested on TEST: 1
- TEST PF > 1.40 configs: 0

## Best TRAIN Rescore

- TRAIN fast PF inf over 2 trades, net Rs2,026.
- Config: `{"daily_loss_rs": 4000.0, "entry_guards": {"max_slot": "12:00", "top_n": 3}, "exit": {"sl_pct": 1.1, "tgt_pct": 1.25}, "mask_terms": [["close_loc", ">=", 0.853261]], "max_positions": 10, "pre_momentum_terms": [["sig5_adx_calc", "<=", 15.526113]], "side": "LONG"}`

## Best Confirmed Train-Band Candidate

- TRAIN PF 1.321 over 25 trades, net Rs3,629.
- TEST PF 0.0 over 1 trades, net Rs-1,428.
- Config: `{"daily_loss_rs": 4000.0, "entry_guards": {"max_slot": "12:00", "min_slot": "09:30", "top_n": 3}, "exit": {"sl_pct": 1.2, "tgt_pct": 2.5}, "mask_terms": [["close_loc", ">=", 0.929858]], "max_positions": 10, "pre_momentum_terms": [["pre5_mom_r", "<=", 0.507041]], "side": "LONG"}`
