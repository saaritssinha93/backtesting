# ITERATION_LOG - L_RS_LEADER_VWAP_HOLD

Command: `py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\L_RS_LEADER_VWAP_HOLD\scripts\parameter_sweep_again.py --pool Train_and_Test\setup_pf_1_4_approval_loop\L_RS_LEADER_VWAP_HOLD\pool --max_iterations 100 --slippage_bps 15`

Each iteration changes one logical group or one staged combination. TEST is shown only when full TRAIN PF is inside [1.30, 1.70].

## Iter 1 - Stage 1 / baseline - too few trades
- changed parameter: baseline_card
- reason: original card
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["quality_score", ">=", 97.121022], ["vol_ratio", ">=", 2.164331], ["vwap_dist_atr", "<=", 1.49336], ["signal_minute", "<=", 660.0]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.5, "tgt": 1.25}`
- FIT: n=1 PF=0.0 net=Rs-733 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=1 PF=inf net=Rs1,009 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=2 PF=1.3775 net=Rs277 win=50.0% t/s/e=1/1/0 dom=1.0/3.649/3.649
- TEST: n=1 PF=0.0 net=Rs-732 win=0.0% t/s/e=0/1/0 dom=9.99/9.99/9.99
- keep/reject: REJECT
- next action: continue train-side search

## Iter 2 - Stage 2 / SL/target - too few trades
- changed parameter: exit_baseline_0.4_0.6
- reason: exit grid around baseline filters
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["quality_score", ">=", 97.121022], ["vol_ratio", ">=", 2.164331], ["vwap_dist_atr", "<=", 1.49336], ["signal_minute", "<=", 660.0]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.4, "tgt": 0.6}`
- FIT: n=1 PF=0.0 net=Rs-633 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=1 PF=inf net=Rs364 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=2 PF=0.5753 net=Rs-269 win=50.0% t/s/e=1/1/0 dom=1.0/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 3 - Stage 2 / SL/target - too few trades
- changed parameter: exit_baseline_0.4_0.8
- reason: exit grid around baseline filters
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["quality_score", ">=", 97.121022], ["vol_ratio", ">=", 2.164331], ["vwap_dist_atr", "<=", 1.49336], ["signal_minute", "<=", 660.0]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.4, "tgt": 0.8}`
- FIT: n=1 PF=0.0 net=Rs-633 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=1 PF=inf net=Rs563 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=2 PF=0.8891 net=Rs-70 win=50.0% t/s/e=1/1/0 dom=1.0/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 4 - Stage 2 / SL/target - too few trades
- changed parameter: exit_baseline_0.4_1.0
- reason: exit grid around baseline filters
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["quality_score", ">=", 97.121022], ["vol_ratio", ">=", 2.164331], ["vwap_dist_atr", "<=", 1.49336], ["signal_minute", "<=", 660.0]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.4, "tgt": 1.0}`
- FIT: n=1 PF=0.0 net=Rs-633 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=1 PF=inf net=Rs761 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=2 PF=1.2029 net=Rs128 win=50.0% t/s/e=1/1/0 dom=1.0/5.929/5.929
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 5 - Stage 2 / SL/target - too few trades
- changed parameter: exit_baseline_0.4_1.25
- reason: exit grid around baseline filters
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["quality_score", ">=", 97.121022], ["vol_ratio", ">=", 2.164331], ["vwap_dist_atr", "<=", 1.49336], ["signal_minute", "<=", 660.0]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.4, "tgt": 1.25}`
- FIT: n=1 PF=0.0 net=Rs-633 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=1 PF=inf net=Rs1,009 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=2 PF=1.5951 net=Rs377 win=50.0% t/s/e=1/1/0 dom=1.0/2.68/2.68
- TEST: n=1 PF=0.0 net=Rs-632 win=0.0% t/s/e=0/1/0 dom=9.99/9.99/9.99
- keep/reject: REJECT
- next action: continue train-side search

## Iter 6 - Stage 2 / SL/target - too few trades
- changed parameter: exit_baseline_0.4_1.5
- reason: exit grid around baseline filters
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["quality_score", ">=", 97.121022], ["vol_ratio", ">=", 2.164331], ["vwap_dist_atr", "<=", 1.49336], ["signal_minute", "<=", 660.0]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.4, "tgt": 1.5}`
- FIT: n=1 PF=0.0 net=Rs-633 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=1 PF=inf net=Rs577 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=2 PF=0.9121 net=Rs-56 win=50.0% t/s/e=0/1/1 dom=1.0/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 7 - Stage 2 / SL/target - too few trades
- changed parameter: exit_baseline_0.4_2.0
- reason: exit grid around baseline filters
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["quality_score", ">=", 97.121022], ["vol_ratio", ">=", 2.164331], ["vwap_dist_atr", "<=", 1.49336], ["signal_minute", "<=", 660.0]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.4, "tgt": 2.0}`
- FIT: n=1 PF=0.0 net=Rs-633 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=1 PF=inf net=Rs577 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=2 PF=0.9121 net=Rs-56 win=50.0% t/s/e=0/1/1 dom=1.0/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 8 - Stage 2 / SL/target - too few trades
- changed parameter: exit_baseline_0.4_2.5
- reason: exit grid around baseline filters
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["quality_score", ">=", 97.121022], ["vol_ratio", ">=", 2.164331], ["vwap_dist_atr", "<=", 1.49336], ["signal_minute", "<=", 660.0]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.4, "tgt": 2.5}`
- FIT: n=1 PF=0.0 net=Rs-633 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=1 PF=inf net=Rs577 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=2 PF=0.9121 net=Rs-56 win=50.0% t/s/e=0/1/1 dom=1.0/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 9 - Stage 2 / SL/target - too few trades
- changed parameter: exit_baseline_0.4_3.0
- reason: exit grid around baseline filters
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["quality_score", ">=", 97.121022], ["vol_ratio", ">=", 2.164331], ["vwap_dist_atr", "<=", 1.49336], ["signal_minute", "<=", 660.0]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.4, "tgt": 3.0}`
- FIT: n=1 PF=0.0 net=Rs-633 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=1 PF=inf net=Rs577 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=2 PF=0.9121 net=Rs-56 win=50.0% t/s/e=0/1/1 dom=1.0/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 10 - Stage 2 / SL/target - too few trades
- changed parameter: exit_baseline_0.5_0.6
- reason: exit grid around baseline filters
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["quality_score", ">=", 97.121022], ["vol_ratio", ">=", 2.164331], ["vwap_dist_atr", "<=", 1.49336], ["signal_minute", "<=", 660.0]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.5, "tgt": 0.6}`
- FIT: n=1 PF=0.0 net=Rs-733 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=1 PF=inf net=Rs364 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=2 PF=0.4968 net=Rs-369 win=50.0% t/s/e=1/1/0 dom=1.0/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 11 - Stage 2 / SL/target - too few trades
- changed parameter: exit_baseline_0.5_0.8
- reason: exit grid around baseline filters
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["quality_score", ">=", 97.121022], ["vol_ratio", ">=", 2.164331], ["vwap_dist_atr", "<=", 1.49336], ["signal_minute", "<=", 660.0]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.5, "tgt": 0.8}`
- FIT: n=1 PF=0.0 net=Rs-733 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=1 PF=inf net=Rs563 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=2 PF=0.7678 net=Rs-170 win=50.0% t/s/e=1/1/0 dom=1.0/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 12 - Stage 2 / SL/target - too few trades
- changed parameter: exit_baseline_0.5_1.0
- reason: exit grid around baseline filters
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["quality_score", ">=", 97.121022], ["vol_ratio", ">=", 2.164331], ["vwap_dist_atr", "<=", 1.49336], ["signal_minute", "<=", 660.0]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.5, "tgt": 1.0}`
- FIT: n=1 PF=0.0 net=Rs-733 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=1 PF=inf net=Rs761 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=2 PF=1.0388 net=Rs28 win=50.0% t/s/e=1/1/0 dom=1.0/26.793/26.793
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 13 - Stage 2 / SL/target - too few trades
- changed parameter: exit_baseline_0.5_1.5
- reason: exit grid around baseline filters
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["quality_score", ">=", 97.121022], ["vol_ratio", ">=", 2.164331], ["vwap_dist_atr", "<=", 1.49336], ["signal_minute", "<=", 660.0]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.5, "tgt": 1.5}`
- FIT: n=1 PF=0.0 net=Rs-733 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=1 PF=inf net=Rs577 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=2 PF=0.7877 net=Rs-156 win=50.0% t/s/e=0/1/1 dom=1.0/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 14 - Stage 2 / SL/target - too few trades
- changed parameter: exit_baseline_0.5_2.0
- reason: exit grid around baseline filters
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["quality_score", ">=", 97.121022], ["vol_ratio", ">=", 2.164331], ["vwap_dist_atr", "<=", 1.49336], ["signal_minute", "<=", 660.0]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.5, "tgt": 2.0}`
- FIT: n=1 PF=0.0 net=Rs-733 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=1 PF=inf net=Rs577 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=2 PF=0.7877 net=Rs-156 win=50.0% t/s/e=0/1/1 dom=1.0/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 15 - Stage 2 / SL/target - too few trades
- changed parameter: exit_baseline_0.5_2.5
- reason: exit grid around baseline filters
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["quality_score", ">=", 97.121022], ["vol_ratio", ">=", 2.164331], ["vwap_dist_atr", "<=", 1.49336], ["signal_minute", "<=", 660.0]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.5, "tgt": 2.5}`
- FIT: n=1 PF=0.0 net=Rs-733 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=1 PF=inf net=Rs577 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=2 PF=0.7877 net=Rs-156 win=50.0% t/s/e=0/1/1 dom=1.0/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 16 - Stage 2 / SL/target - too few trades
- changed parameter: exit_baseline_0.5_3.0
- reason: exit grid around baseline filters
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["quality_score", ">=", 97.121022], ["vol_ratio", ">=", 2.164331], ["vwap_dist_atr", "<=", 1.49336], ["signal_minute", "<=", 660.0]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.5, "tgt": 3.0}`
- FIT: n=1 PF=0.0 net=Rs-733 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=1 PF=inf net=Rs577 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=2 PF=0.7877 net=Rs-156 win=50.0% t/s/e=0/1/1 dom=1.0/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 17 - Stage 2 / SL/target - too few trades
- changed parameter: exit_baseline_0.6_0.6
- reason: exit grid around baseline filters
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["quality_score", ">=", 97.121022], ["vol_ratio", ">=", 2.164331], ["vwap_dist_atr", "<=", 1.49336], ["signal_minute", "<=", 660.0]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.6, "tgt": 0.6}`
- FIT: n=1 PF=0.0 net=Rs-833 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=1 PF=inf net=Rs364 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=2 PF=0.4372 net=Rs-469 win=50.0% t/s/e=1/1/0 dom=1.0/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 18 - Stage 2 / SL/target - too few trades
- changed parameter: exit_baseline_0.6_0.8
- reason: exit grid around baseline filters
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["quality_score", ">=", 97.121022], ["vol_ratio", ">=", 2.164331], ["vwap_dist_atr", "<=", 1.49336], ["signal_minute", "<=", 660.0]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.6, "tgt": 0.8}`
- FIT: n=1 PF=0.0 net=Rs-833 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=1 PF=inf net=Rs563 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=2 PF=0.6756 net=Rs-270 win=50.0% t/s/e=1/1/0 dom=1.0/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 19 - Stage 2 / SL/target - too few trades
- changed parameter: exit_baseline_0.6_1.0
- reason: exit grid around baseline filters
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["quality_score", ">=", 97.121022], ["vol_ratio", ">=", 2.164331], ["vwap_dist_atr", "<=", 1.49336], ["signal_minute", "<=", 660.0]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.6, "tgt": 1.0}`
- FIT: n=1 PF=0.0 net=Rs-833 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=1 PF=inf net=Rs761 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=2 PF=0.9141 net=Rs-72 win=50.0% t/s/e=1/1/0 dom=1.0/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 20 - Stage 2 / SL/target - too few trades
- changed parameter: exit_baseline_0.6_1.25
- reason: exit grid around baseline filters
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["quality_score", ">=", 97.121022], ["vol_ratio", ">=", 2.164331], ["vwap_dist_atr", "<=", 1.49336], ["signal_minute", "<=", 660.0]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.6, "tgt": 1.25}`
- FIT: n=1 PF=0.0 net=Rs-833 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=1 PF=inf net=Rs1,009 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=2 PF=1.2121 net=Rs177 win=50.0% t/s/e=1/1/0 dom=1.0/5.714/5.714
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 21 - Stage 2 / SL/target - too few trades
- changed parameter: exit_baseline_0.6_1.5
- reason: exit grid around baseline filters
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["quality_score", ">=", 97.121022], ["vol_ratio", ">=", 2.164331], ["vwap_dist_atr", "<=", 1.49336], ["signal_minute", "<=", 660.0]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.6, "tgt": 1.5}`
- FIT: n=1 PF=0.0 net=Rs-833 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=1 PF=inf net=Rs577 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=2 PF=0.6931 net=Rs-256 win=50.0% t/s/e=0/1/1 dom=1.0/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 22 - Stage 2 / SL/target - too few trades
- changed parameter: exit_baseline_0.6_2.0
- reason: exit grid around baseline filters
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["quality_score", ">=", 97.121022], ["vol_ratio", ">=", 2.164331], ["vwap_dist_atr", "<=", 1.49336], ["signal_minute", "<=", 660.0]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.6, "tgt": 2.0}`
- FIT: n=1 PF=0.0 net=Rs-833 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=1 PF=inf net=Rs577 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=2 PF=0.6931 net=Rs-256 win=50.0% t/s/e=0/1/1 dom=1.0/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 23 - Stage 2 / SL/target - too few trades
- changed parameter: exit_baseline_0.6_2.5
- reason: exit grid around baseline filters
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["quality_score", ">=", 97.121022], ["vol_ratio", ">=", 2.164331], ["vwap_dist_atr", "<=", 1.49336], ["signal_minute", "<=", 660.0]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.6, "tgt": 2.5}`
- FIT: n=1 PF=0.0 net=Rs-833 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=1 PF=inf net=Rs577 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=2 PF=0.6931 net=Rs-256 win=50.0% t/s/e=0/1/1 dom=1.0/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 24 - Stage 2 / SL/target - too few trades
- changed parameter: exit_baseline_0.6_3.0
- reason: exit grid around baseline filters
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["quality_score", ">=", 97.121022], ["vol_ratio", ">=", 2.164331], ["vwap_dist_atr", "<=", 1.49336], ["signal_minute", "<=", 660.0]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.6, "tgt": 3.0}`
- FIT: n=1 PF=0.0 net=Rs-833 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=1 PF=inf net=Rs577 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=2 PF=0.6931 net=Rs-256 win=50.0% t/s/e=0/1/1 dom=1.0/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 25 - Stage 2 / SL/target - too few trades
- changed parameter: exit_baseline_0.7_0.6
- reason: exit grid around baseline filters
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["quality_score", ">=", 97.121022], ["vol_ratio", ">=", 2.164331], ["vwap_dist_atr", "<=", 1.49336], ["signal_minute", "<=", 660.0]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 0.6}`
- FIT: n=1 PF=0.0 net=Rs-933 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=1 PF=inf net=Rs364 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=2 PF=0.3903 net=Rs-569 win=50.0% t/s/e=1/1/0 dom=1.0/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 26 - Stage 2 / SL/target - too few trades
- changed parameter: exit_baseline_0.7_0.8
- reason: exit grid around baseline filters
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["quality_score", ">=", 97.121022], ["vol_ratio", ">=", 2.164331], ["vwap_dist_atr", "<=", 1.49336], ["signal_minute", "<=", 660.0]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 0.8}`
- FIT: n=1 PF=0.0 net=Rs-933 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=1 PF=inf net=Rs563 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=2 PF=0.6032 net=Rs-370 win=50.0% t/s/e=1/1/0 dom=1.0/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 27 - Stage 2 / indicator/filter - TRAIN PF too low
- changed parameter: signal_rsi_>=_0.2
- reason: single-column range sweep for rsi
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["rsi", ">=", 60.664555]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=90 PF=0.4897 net=Rs-25,241 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=63 PF=0.317 net=Rs-26,601 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=153 PF=0.4136 net=Rs-51,843 win=31.37% t/s/e=20/85/48 dom=0.033/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 28 - Stage 2 / indicator/filter - TRAIN PF too low
- changed parameter: signal_rsi_<=_0.2
- reason: single-column range sweep for rsi
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["rsi", "<=", 60.664555]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=23 PF=0.4475 net=Rs-6,123 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=11 PF=0.0367 net=Rs-6,604 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=34 PF=0.2905 net=Rs-12,727 win=23.53% t/s/e=2/15/17 dom=0.224/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 29 - Stage 2 / indicator/filter - TRAIN PF too low
- changed parameter: signal_rsi_>=_0.4
- reason: single-column range sweep for rsi
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["rsi", ">=", 63.057346]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=69 PF=0.4524 net=Rs-21,731 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=46 PF=0.2884 net=Rs-20,598 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=115 PF=0.3832 net=Rs-42,329 win=30.43% t/s/e=15/68/32 dom=0.046/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 30 - Stage 2 / indicator/filter - TRAIN PF too low
- changed parameter: signal_rsi_<=_0.4
- reason: single-column range sweep for rsi
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["rsi", "<=", 63.057346]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=44 PF=0.5383 net=Rs-9,633 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=28 PF=0.2521 net=Rs-12,608 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=72 PF=0.4104 net=Rs-22,241 win=29.17% t/s/e=7/32/33 dom=0.077/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 31 - Stage 2 / indicator/filter - TRAIN PF too low
- changed parameter: signal_rsi_>=_0.6
- reason: single-column range sweep for rsi
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["rsi", ">=", 65.370003]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=48 PF=0.5113 net=Rs-12,861 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=30 PF=0.3036 net=Rs-12,800 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=78 PF=0.4259 net=Rs-25,661 win=33.33% t/s/e=12/45/21 dom=0.064/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 32 - Stage 2 / indicator/filter - TRAIN PF too low
- changed parameter: signal_rsi_<=_0.6
- reason: single-column range sweep for rsi
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["rsi", "<=", 65.370003]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=65 PF=0.4594 net=Rs-18,503 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=44 PF=0.2559 net=Rs-20,405 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=109 PF=0.3689 net=Rs-38,908 win=27.52% t/s/e=10/55/44 dom=0.053/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 33 - Stage 2 / indicator/filter - TRAIN PF too low
- changed parameter: signal_rsi_>=_0.8
- reason: single-column range sweep for rsi
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["rsi", ">=", 67.73262]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=22 PF=0.4151 net=Rs-8,138 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=17 PF=0.1732 net=Rs-9,884 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=39 PF=0.3033 net=Rs-18,022 win=25.64% t/s/e=5/27/7 dom=0.156/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 34 - Stage 2 / indicator/filter - TRAIN PF too low
- changed parameter: signal_rsi_<=_0.8
- reason: single-column range sweep for rsi
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["rsi", "<=", 67.73262]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=91 PF=0.5019 net=Rs-23,226 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=57 PF=0.311 net=Rs-23,322 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=148 PF=0.4216 net=Rs-46,548 win=31.08% t/s/e=17/73/58 dom=0.036/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 35 - Stage 2 / indicator/filter - TRAIN PF too low
- changed parameter: signal_rsi3max_>=_0.2
- reason: single-column range sweep for rsi3max
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["rsi3max", ">=", 60.762584]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=90 PF=0.4792 net=Rs-26,155 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=63 PF=0.317 net=Rs-26,601 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=153 PF=0.4084 net=Rs-52,756 win=30.72% t/s/e=20/85/48 dom=0.033/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 36 - Stage 2 / indicator/filter - TRAIN PF too low
- changed parameter: signal_rsi3max_<=_0.2
- reason: single-column range sweep for rsi3max
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["rsi3max", "<=", 60.762584]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=23 PF=0.4954 net=Rs-5,209 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=11 PF=0.0367 net=Rs-6,604 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=34 PF=0.3123 net=Rs-11,813 win=26.47% t/s/e=2/15/17 dom=0.216/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 37 - Stage 2 / indicator/filter - TRAIN PF too low
- changed parameter: signal_rsi3max_>=_0.4
- reason: single-column range sweep for rsi3max
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["rsi3max", ">=", 63.086761]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=69 PF=0.4606 net=Rs-21,025 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=46 PF=0.2884 net=Rs-20,598 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=115 PF=0.3872 net=Rs-41,623 win=30.43% t/s/e=15/67/33 dom=0.046/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 38 - Stage 2 / indicator/filter - TRAIN PF too low
- changed parameter: signal_rsi3max_<=_0.4
- reason: single-column range sweep for rsi3max
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["rsi3max", "<=", 63.086761]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=44 PF=0.5207 net=Rs-10,339 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=28 PF=0.2521 net=Rs-12,608 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=72 PF=0.4028 net=Rs-22,946 win=29.17% t/s/e=7/33/32 dom=0.077/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 39 - Stage 2 / indicator/filter - TRAIN PF too low
- changed parameter: signal_rsi3max_>=_0.6
- reason: single-column range sweep for rsi3max
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["rsi3max", ">=", 65.411252]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=47 PF=0.4655 net=Rs-14,067 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=30 PF=0.3036 net=Rs-12,800 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=77 PF=0.3989 net=Rs-26,867 win=32.47% t/s/e=11/45/21 dom=0.068/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 40 - Stage 2 / indicator/filter - TRAIN PF too low
- changed parameter: signal_rsi3max_<=_0.6
- reason: single-column range sweep for rsi3max
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["rsi3max", "<=", 65.411252]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=66 PF=0.4947 net=Rs-17,297 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=44 PF=0.2559 net=Rs-20,405 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=110 PF=0.3884 net=Rs-37,703 win=28.18% t/s/e=11/55/44 dom=0.051/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 41 - Stage 2 / indicator/filter - TRAIN PF too low
- changed parameter: signal_rsi3max_>=_0.8
- reason: single-column range sweep for rsi3max
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["rsi3max", ">=", 67.73262]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=22 PF=0.4151 net=Rs-8,138 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=17 PF=0.1732 net=Rs-9,884 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=39 PF=0.3033 net=Rs-18,022 win=25.64% t/s/e=5/27/7 dom=0.156/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 42 - Stage 2 / indicator/filter - TRAIN PF too low
- changed parameter: signal_rsi3max_<=_0.8
- reason: single-column range sweep for rsi3max
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["rsi3max", "<=", 67.73262]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=91 PF=0.5019 net=Rs-23,226 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=57 PF=0.311 net=Rs-23,322 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=148 PF=0.4216 net=Rs-46,548 win=31.08% t/s/e=17/73/58 dom=0.036/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 43 - Stage 2 / indicator/filter - TRAIN PF too low
- changed parameter: signal_adx_>=_0.2
- reason: single-column range sweep for adx
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["adx", ">=", 21.924144]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=91 PF=0.3022 net=Rs-37,609 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=58 PF=0.2947 net=Rs-25,098 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=149 PF=0.2992 net=Rs-62,707 win=25.5% t/s/e=14/87/48 dom=0.045/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 44 - Stage 2 / indicator/filter - TRAIN PF too low
- changed parameter: signal_adx_<=_0.2
- reason: single-column range sweep for adx
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["adx", "<=", 21.924144]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=22 PF=1.9399 net=Rs6,245 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=16 PF=0.2067 net=Rs-8,108 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=38 PF=0.8896 net=Rs-1,862 win=47.37% t/s/e=8/13/17 dom=0.082/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 45 - Stage 2 / indicator/filter - TRAIN PF too low
- changed parameter: signal_adx_>=_0.4
- reason: single-column range sweep for adx
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["adx", ">=", 25.018684]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=67 PF=0.2841 net=Rs-28,353 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=44 PF=0.2201 net=Rs-22,391 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=111 PF=0.2572 net=Rs-50,744 win=24.32% t/s/e=9/69/33 dom=0.068/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 46 - Stage 2 / indicator/filter - TRAIN PF too low
- changed parameter: signal_adx_<=_0.4
- reason: single-column range sweep for adx
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["adx", "<=", 25.018684]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=46 PF=0.8562 net=Rs-3,011 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=30 PF=0.3673 net=Rs-10,815 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=76 PF=0.6365 net=Rs-13,826 win=38.16% t/s/e=13/31/32 dom=0.051/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 47 - Stage 2 / indicator/filter - TRAIN PF too low
- changed parameter: signal_adx_>=_0.6
- reason: single-column range sweep for adx
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["adx", ">=", 29.342195]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=42 PF=0.2805 net=Rs-16,844 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=34 PF=0.2131 net=Rs-18,583 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=76 PF=0.2467 net=Rs-35,427 win=26.32% t/s/e=5/47/24 dom=0.101/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 48 - Stage 2 / indicator/filter - TRAIN PF too low
- changed parameter: signal_adx_<=_0.6
- reason: single-column range sweep for adx
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["adx", "<=", 29.342195]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=71 PF=0.609 net=Rs-14,520 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=40 PF=0.341 net=Rs-14,622 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=111 PF=0.5087 net=Rs-29,142 win=32.43% t/s/e=17/53/41 dom=0.041/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 49 - Stage 2 / indicator/filter - TRAIN PF too low
- changed parameter: signal_adx_>=_0.8
- reason: single-column range sweep for adx
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["adx", ">=", 34.649849]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=20 PF=0.3036 net=Rs-7,673 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=20 PF=0.2802 net=Rs-9,495 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=40 PF=0.2909 net=Rs-17,168 win=25.0% t/s/e=4/23/13 dom=0.17/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 50 - Stage 2 / indicator/filter - TRAIN PF too low
- changed parameter: signal_adx_<=_0.8
- reason: single-column range sweep for adx
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["adx", "<=", 34.649849]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=93 PF=0.5216 net=Rs-23,691 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=54 PF=0.273 net=Rs-23,711 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=147 PF=0.4229 net=Rs-47,401 win=31.29% t/s/e=18/77/52 dom=0.035/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 51 - Stage 2 / indicator/filter - TRAIN PF too low
- changed parameter: signal_macd_hist_>=_0.2
- reason: single-column range sweep for macd_hist
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["macd_hist", ">=", -0.365052]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=94 PF=0.4248 net=Rs-30,971 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=58 PF=0.2747 net=Rs-26,658 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=152 PF=0.3639 net=Rs-57,629 win=28.29% t/s/e=18/85/49 dom=0.037/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 52 - Stage 2 / indicator/filter - TRAIN PF too low
- changed parameter: signal_macd_hist_<=_0.2
- reason: single-column range sweep for macd_hist
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["macd_hist", "<=", -0.365052]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=19 PF=0.9413 net=Rs-393 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=14 PF=0.3396 net=Rs-4,861 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=33 PF=0.6264 net=Rs-5,254 win=39.39% t/s/e=4/14/15 dom=0.134/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 53 - Stage 2 / indicator/filter - TRAIN PF too low
- changed parameter: signal_macd_hist_>=_0.4
- reason: single-column range sweep for macd_hist
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["macd_hist", ">=", -0.06565]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=72 PF=0.4214 net=Rs-24,767 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=44 PF=0.259 net=Rs-20,447 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=116 PF=0.3578 net=Rs-45,214 win=26.72% t/s/e=14/66/36 dom=0.049/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 54 - Stage 2 / indicator/filter - TRAIN PF too low
- changed parameter: signal_macd_hist_<=_0.4
- reason: single-column range sweep for macd_hist
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["macd_hist", "<=", -0.06565]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=41 PF=0.628 net=Rs-6,597 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=28 PF=0.3298 net=Rs-11,072 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=69 PF=0.4842 net=Rs-17,669 win=36.23% t/s/e=8/33/28 dom=0.072/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 55 - Stage 2 / indicator/filter - TRAIN PF too low
- changed parameter: signal_macd_hist_>=_0.6
- reason: single-column range sweep for macd_hist
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["macd_hist", ">=", 0.109629]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=48 PF=0.4861 net=Rs-13,952 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=29 PF=0.1661 net=Rs-16,712 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=77 PF=0.3502 net=Rs-30,664 win=27.27% t/s/e=8/44/25 dom=0.074/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 56 - Stage 2 / indicator/filter - TRAIN PF too low
- changed parameter: signal_macd_hist_<=_0.6
- reason: single-column range sweep for macd_hist
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["macd_hist", "<=", 0.109629]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=65 PF=0.4786 net=Rs-17,412 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=43 PF=0.385 net=Rs-14,807 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=108 PF=0.4394 net=Rs-32,219 win=32.41% t/s/e=14/55/39 dom=0.048/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 57 - Stage 2 / indicator/filter - TRAIN PF too low
- changed parameter: signal_macd_hist_>=_0.8
- reason: single-column range sweep for macd_hist
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["macd_hist", ">=", 0.526484]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=23 PF=0.4951 net=Rs-6,802 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=15 PF=0.0341 net=Rs-10,710 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=38 PF=0.287 net=Rs-17,513 win=23.68% t/s/e=2/24/12 dom=0.172/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 58 - Stage 2 / indicator/filter - TRAIN PF too low
- changed parameter: signal_macd_hist_<=_0.8
- reason: single-column range sweep for macd_hist
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["macd_hist", "<=", 0.526484]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=90 PF=0.4782 net=Rs-24,561 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=57 PF=0.37 net=Rs-20,808 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=147 PF=0.4336 net=Rs-45,370 win=31.97% t/s/e=20/75/52 dom=0.035/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 59 - Stage 2 / indicator/filter - TRAIN PF too low
- changed parameter: signal_macd_hist_delta_>=_0.2
- reason: single-column range sweep for macd_hist_delta
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["macd_hist_delta", ">=", 0.060931]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=86 PF=0.5714 net=Rs-18,906 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=60 PF=0.2598 net=Rs-27,958 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=146 PF=0.4277 net=Rs-46,864 win=30.82% t/s/e=18/79/49 dom=0.035/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 60 - Stage 2 / indicator/filter - TRAIN PF too low
- changed parameter: signal_macd_hist_delta_<=_0.2
- reason: single-column range sweep for macd_hist_delta
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["macd_hist_delta", "<=", 0.060931]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=27 PF=0.242 net=Rs-12,458 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=12 PF=0.4388 net=Rs-3,561 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=39 PF=0.2968 net=Rs-16,019 win=28.21% t/s/e=4/20/15 dom=0.176/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 61 - Stage 2 / indicator/filter - TRAIN PF too low
- changed parameter: signal_macd_hist_delta_>=_0.4
- reason: single-column range sweep for macd_hist_delta
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["macd_hist_delta", ">=", 0.14695]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=65 PF=0.5928 net=Rs-13,463 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=45 PF=0.1914 net=Rs-23,579 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=110 PF=0.4047 net=Rs-37,042 win=30.0% t/s/e=11/59/40 dom=0.048/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 62 - Stage 2 / pre-momentum - TRAIN PF too low
- changed parameter: premom_pre_entry_momentum_score_>=_0.2
- reason: single pre-momentum range sweep for pre_entry_momentum_score
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [], "max_positions": 20, "premom_terms": [["pre_entry_momentum_score", ">=", 56.82847]], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=93 PF=0.4703 net=Rs-25,803 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=57 PF=0.2631 net=Rs-26,140 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=150 PF=0.3829 net=Rs-51,943 win=30.0% t/s/e=18/80/52 dom=0.037/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 63 - Stage 2 / pre-momentum - TRAIN PF too low
- changed parameter: premom_pre_entry_momentum_score_<=_0.2
- reason: single pre-momentum range sweep for pre_entry_momentum_score
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [], "max_positions": 20, "premom_terms": [["pre_entry_momentum_score", "<=", 56.82847]], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=23 PF=0.756 net=Rs-2,888 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=18 PF=0.2908 net=Rs-7,969 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=41 PF=0.5294 net=Rs-10,857 win=34.15% t/s/e=6/21/14 dom=0.1/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 64 - Stage 2 / pre-momentum - TRAIN PF too low
- changed parameter: premom_pre_entry_momentum_score_>=_0.4
- reason: single pre-momentum range sweep for pre_entry_momentum_score
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [], "max_positions": 20, "premom_terms": [["pre_entry_momentum_score", ">=", 66.401967]], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=74 PF=0.47 net=Rs-20,158 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=46 PF=0.2017 net=Rs-25,041 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=120 PF=0.3487 net=Rs-45,200 win=29.17% t/s/e=14/66/40 dom=0.05/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 65 - Stage 2 / pre-momentum - TRAIN PF too low
- changed parameter: premom_pre_entry_momentum_score_<=_0.4
- reason: single pre-momentum range sweep for pre_entry_momentum_score
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [], "max_positions": 20, "premom_terms": [["pre_entry_momentum_score", "<=", 66.401967]], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=47 PF=0.5808 net=Rs-10,442 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=31 PF=0.3659 net=Rs-10,864 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=78 PF=0.4932 net=Rs-21,306 win=33.33% t/s/e=10/38/30 dom=0.059/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 66 - Stage 2 / pre-momentum - TRAIN PF too low
- changed parameter: premom_pre_entry_momentum_score_>=_0.6
- reason: single pre-momentum range sweep for pre_entry_momentum_score
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [], "max_positions": 20, "premom_terms": [["pre_entry_momentum_score", ">=", 73.38725]], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=48 PF=0.4619 net=Rs-14,126 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=32 PF=0.1755 net=Rs-20,262 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=80 PF=0.3234 net=Rs-34,388 win=25.0% t/s/e=10/50/20 dom=0.074/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 67 - Stage 2 / pre-momentum - TRAIN PF too low
- changed parameter: premom_pre_entry_momentum_score_<=_0.6
- reason: single pre-momentum range sweep for pre_entry_momentum_score
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [], "max_positions": 20, "premom_terms": [["pre_entry_momentum_score", "<=", 73.38725]], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=71 PF=0.5348 net=Rs-16,486 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=45 PF=0.3463 net=Rs-15,643 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=116 PF=0.4588 net=Rs-32,128 win=34.48% t/s/e=13/53/50 dom=0.044/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 68 - Stage 2 / pre-momentum - TRAIN PF too low
- changed parameter: premom_pre_entry_momentum_score_>=_0.8
- reason: single pre-momentum range sweep for pre_entry_momentum_score
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [], "max_positions": 20, "premom_terms": [["pre_entry_momentum_score", ">=", 79.196579]], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=24 PF=0.3257 net=Rs-9,975 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=17 PF=0.2549 net=Rs-8,916 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=41 PF=0.294 net=Rs-18,891 win=24.39% t/s/e=5/27/9 dom=0.155/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 69 - Stage 2 / pre-momentum - TRAIN PF too low
- changed parameter: premom_pre_entry_momentum_score_<=_0.8
- reason: single pre-momentum range sweep for pre_entry_momentum_score
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [], "max_positions": 20, "premom_terms": [["pre_entry_momentum_score", "<=", 79.196579]], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=94 PF=0.5206 net=Rs-22,923 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=58 PF=0.2751 net=Rs-25,153 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=152 PF=0.4174 net=Rs-48,077 win=31.58% t/s/e=17/75/60 dom=0.035/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 70 - Stage 2 / pre-momentum - TRAIN PF too low
- changed parameter: premom_sig5_adx_calc_>=_0.2
- reason: single pre-momentum range sweep for sig5_adx_calc
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [], "max_positions": 20, "premom_terms": [["sig5_adx_calc", ">=", 21.924144]], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=93 PF=0.3201 net=Rs-37,275 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=58 PF=0.2947 net=Rs-25,098 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=151 PF=0.3101 net=Rs-62,373 win=25.83% t/s/e=15/88/48 dom=0.043/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 71 - Stage 2 / pre-momentum - TRAIN PF too low
- changed parameter: premom_sig5_adx_calc_<=_0.2
- reason: single pre-momentum range sweep for sig5_adx_calc
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [], "max_positions": 20, "premom_terms": [["sig5_adx_calc", "<=", 21.924144]], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=23 PF=2.1301 net=Rs7,509 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=16 PF=0.2067 net=Rs-8,108 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=39 PF=0.9645 net=Rs-598 win=48.72% t/s/e=9/13/17 dom=0.076/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 72 - Stage 2 / pre-momentum - TRAIN PF too low
- changed parameter: premom_sig5_adx_calc_>=_0.4
- reason: single pre-momentum range sweep for sig5_adx_calc
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [], "max_positions": 20, "premom_terms": [["sig5_adx_calc", ">=", 25.018684]], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=69 PF=0.3087 net=Rs-28,018 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=45 PF=0.2137 net=Rs-23,255 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=114 PF=0.2686 net=Rs-51,274 win=24.56% t/s/e=10/70/34 dom=0.063/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 73 - Stage 2 / pre-momentum - TRAIN PF too low
- changed parameter: premom_sig5_adx_calc_<=_0.4
- reason: single pre-momentum range sweep for sig5_adx_calc
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [], "max_positions": 20, "premom_terms": [["sig5_adx_calc", "<=", 25.018684]], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=49 PF=0.9117 net=Rs-1,869 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=30 PF=0.3673 net=Rs-10,815 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=79 PF=0.6685 net=Rs-12,683 win=39.24% t/s/e=14/31/34 dom=0.048/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 74 - Stage 2 / pre-momentum - TRAIN PF too low
- changed parameter: premom_sig5_adx_calc_>=_0.6
- reason: single pre-momentum range sweep for sig5_adx_calc
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [], "max_positions": 20, "premom_terms": [["sig5_adx_calc", ">=", 29.342195]], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=44 PF=0.3217 net=Rs-16,509 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=35 PF=0.205 net=Rs-19,516 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=79 PF=0.2631 net=Rs-36,025 win=26.58% t/s/e=6/49/24 dom=0.092/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 75 - Stage 2 / pre-momentum - TRAIN PF too low
- changed parameter: premom_sig5_adx_calc_<=_0.6
- reason: single pre-momentum range sweep for sig5_adx_calc
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [], "max_positions": 20, "premom_terms": [["sig5_adx_calc", "<=", 29.342195]], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=74 PF=0.6088 net=Rs-14,884 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=41 PF=0.3277 net=Rs-15,525 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=115 PF=0.5026 net=Rs-30,409 win=33.04% t/s/e=17/55/43 dom=0.04/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 76 - Stage 2 / pre-momentum - TRAIN PF too low
- changed parameter: premom_sig5_adx_calc_>=_0.8
- reason: single pre-momentum range sweep for sig5_adx_calc
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [], "max_positions": 20, "premom_terms": [["sig5_adx_calc", ">=", 34.649849]], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=20 PF=0.3036 net=Rs-7,673 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=20 PF=0.2802 net=Rs-9,495 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=40 PF=0.2909 net=Rs-17,168 win=25.0% t/s/e=4/23/13 dom=0.17/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 77 - Stage 2 / pre-momentum - TRAIN PF too low
- changed parameter: premom_sig5_adx_calc_<=_0.8
- reason: single pre-momentum range sweep for sig5_adx_calc
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [], "max_positions": 20, "premom_terms": [["sig5_adx_calc", "<=", 34.649849]], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=94 PF=0.5159 net=Rs-24,242 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=54 PF=0.273 net=Rs-23,711 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=148 PF=0.4201 net=Rs-47,952 win=31.08% t/s/e=18/77/53 dom=0.035/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 78 - Stage 2 / guard - TRAIN PF too low
- changed parameter: guard_{"min_slot": "09:45"}
- reason: time/top_n guard sweep
- config: `{"daily_loss_rs": 0.0, "entry_guards": {"min_slot": "09:45"}, "mask_terms": [], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=113 PF=0.482 net=Rs-31,364 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=74 PF=0.275 net=Rs-33,206 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=187 PF=0.3928 net=Rs-64,570 win=29.95% t/s/e=22/100/65 dom=0.029/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 79 - Stage 2 / guard - TRAIN PF too low
- changed parameter: guard_{"min_slot": "10:00"}
- reason: time/top_n guard sweep
- config: `{"daily_loss_rs": 0.0, "entry_guards": {"min_slot": "10:00"}, "mask_terms": [], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=113 PF=0.482 net=Rs-31,364 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=74 PF=0.275 net=Rs-33,206 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=187 PF=0.3928 net=Rs-64,570 win=29.95% t/s/e=22/100/65 dom=0.029/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 80 - Stage 2 / guard - TRAIN PF too low
- changed parameter: guard_{"min_slot": "10:30"}
- reason: time/top_n guard sweep
- config: `{"daily_loss_rs": 0.0, "entry_guards": {"min_slot": "10:30"}, "mask_terms": [], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=100 PF=0.541 net=Rs-23,952 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=67 PF=0.3045 net=Rs-28,268 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=167 PF=0.4374 net=Rs-52,221 win=30.54% t/s/e=22/87/58 dom=0.03/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 81 - Stage 2 / guard - TRAIN PF too low
- changed parameter: guard_{"max_slot": "11:30"}
- reason: time/top_n guard sweep
- config: `{"daily_loss_rs": 0.0, "entry_guards": {"max_slot": "11:30"}, "mask_terms": [], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=52 PF=0.4302 net=Rs-16,399 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=37 PF=0.2567 net=Rs-17,445 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=89 PF=0.3523 net=Rs-33,844 win=28.09% t/s/e=10/51/28 dom=0.065/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 82 - Stage 2 / guard - TRAIN PF too low
- changed parameter: guard_{"max_slot": "12:30"}
- reason: time/top_n guard sweep
- config: `{"daily_loss_rs": 0.0, "entry_guards": {"max_slot": "12:30"}, "mask_terms": [], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=92 PF=0.4175 net=Rs-29,819 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=54 PF=0.2487 net=Rs-25,436 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=146 PF=0.3503 net=Rs-55,255 win=28.77% t/s/e=16/82/48 dom=0.04/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 83 - Stage 2 / guard - TRAIN PF too low
- changed parameter: guard_{"max_slot": "14:00"}
- reason: time/top_n guard sweep
- config: `{"daily_loss_rs": 0.0, "entry_guards": {"max_slot": "14:00"}, "mask_terms": [], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=113 PF=0.482 net=Rs-31,364 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=74 PF=0.275 net=Rs-33,206 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=187 PF=0.3928 net=Rs-64,570 win=29.95% t/s/e=22/100/65 dom=0.029/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 84 - Stage 2 / guard - TRAIN PF too low
- changed parameter: guard_{"max_slot": "11:30", "min_slot": "09:45"}
- reason: time/top_n guard sweep
- config: `{"daily_loss_rs": 0.0, "entry_guards": {"max_slot": "11:30", "min_slot": "09:45"}, "mask_terms": [], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=52 PF=0.4302 net=Rs-16,399 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=37 PF=0.2567 net=Rs-17,445 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=89 PF=0.3523 net=Rs-33,844 win=28.09% t/s/e=10/51/28 dom=0.065/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 85 - Stage 2 / guard - TRAIN PF too low
- changed parameter: guard_{"max_slot": "12:30", "min_slot": "10:30"}
- reason: time/top_n guard sweep
- config: `{"daily_loss_rs": 0.0, "entry_guards": {"max_slot": "12:30", "min_slot": "10:30"}, "mask_terms": [], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=78 PF=0.4873 net=Rs-21,481 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=47 PF=0.2857 net=Rs-20,499 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=125 PF=0.4054 net=Rs-41,980 win=29.6% t/s/e=16/68/41 dom=0.042/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 86 - Stage 2 / guard - TRAIN PF too low
- changed parameter: guard_{"top_n": 1}
- reason: time/top_n guard sweep
- config: `{"daily_loss_rs": 0.0, "entry_guards": {"top_n": 1}, "mask_terms": [], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.7, "tgt": 1.5}`
- FIT: n=91 PF=0.5029 net=Rs-23,918 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=64 PF=0.2594 net=Rs-30,075 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=155 PF=0.3914 net=Rs-53,993 win=29.68% t/s/e=17/84/54 dom=0.035/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 87 - Stage 3 / best train-side combination - TRAIN PF too low
- changed parameter: combo_signal_0.5_1.25
- reason: combine two stable signal terms from Stage 2
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["macd_hist", "<=", 0.109629], ["macd_hist", "<=", 0.526484]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.5, "tgt": 1.25}`
- FIT: n=65 PF=0.4988 net=Rs-15,679 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=43 PF=0.3058 net=Rs-15,902 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=108 PF=0.4172 net=Rs-31,581 win=26.85% t/s/e=20/73/15 dom=0.044/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 88 - Stage 3 / best train-side combination - TRAIN PF too low
- changed parameter: combo_signal_0.5_1.25
- reason: combine two stable signal terms from Stage 2
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["macd_hist", "<=", 0.109629], ["adx", ">=", 21.924144]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.5, "tgt": 1.25}`
- FIT: n=56 PF=0.3312 net=Rs-19,454 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=37 PF=0.3507 net=Rs-12,502 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=93 PF=0.339 net=Rs-31,956 win=23.66% t/s/e=14/65/14 dom=0.06/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 89 - Stage 3 / best train-side combination - TRAIN PF too low
- changed parameter: combo_signal_0.5_1.25
- reason: combine two stable signal terms from Stage 2
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["macd_hist", "<=", 0.109629], ["adx", ">=", 34.649849]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.5, "tgt": 1.25}`
- FIT: n=14 PF=0.5476 net=Rs-2,966 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=14 PF=0.4187 net=Rs-4,224 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=28 PF=0.4799 net=Rs-7,190 win=28.57% t/s/e=6/19/3 dom=0.149/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 90 - Stage 3 / best train-side combination - TRAIN PF too low
- changed parameter: combo_signal_0.5_1.25
- reason: combine two stable signal terms from Stage 2
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["macd_hist", "<=", 0.109629], ["rsi3max", ">=", 60.762584]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.5, "tgt": 1.25}`
- FIT: n=49 PF=0.4964 net=Rs-12,121 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=33 PF=0.3981 net=Rs-10,211 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=82 PF=0.4557 net=Rs-22,332 win=29.27% t/s/e=17/56/9 dom=0.053/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 91 - Stage 3 / best train-side combination - TRAIN PF too low
- changed parameter: combo_signal_0.5_1.25
- reason: combine two stable signal terms from Stage 2
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["macd_hist", "<=", 0.109629], ["rsi", ">=", 60.664555]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.5, "tgt": 1.25}`
- FIT: n=49 PF=0.4964 net=Rs-12,121 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=33 PF=0.3981 net=Rs-10,211 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=82 PF=0.4557 net=Rs-22,332 win=29.27% t/s/e=17/56/9 dom=0.053/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 92 - Stage 3 / best train-side combination - TRAIN PF too low
- changed parameter: combo_signal_0.5_1.25
- reason: combine two stable signal terms from Stage 2
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["macd_hist", "<=", 0.109629], ["rsi3max", ">=", 65.411252]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.5, "tgt": 1.25}`
- FIT: n=22 PF=0.7075 net=Rs-2,776 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=13 PF=0.6692 net=Rs-2,006 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=35 PF=0.6926 net=Rs-4,782 win=37.14% t/s/e=10/21/4 dom=0.093/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 93 - Stage 3 / best train-side combination - TRAIN PF too low
- changed parameter: combo_signal_0.5_1.25
- reason: combine two stable signal terms from Stage 2
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["macd_hist", "<=", 0.109629], ["rsi", "<=", 67.73262]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.5, "tgt": 1.25}`
- FIT: n=57 PF=0.4285 net=Rs-16,208 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=37 PF=0.3033 net=Rs-13,758 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=94 PF=0.3771 net=Rs-29,966 win=25.53% t/s/e=16/65/13 dom=0.054/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 94 - Stage 3 / best train-side combination - TRAIN PF too low
- changed parameter: combo_signal_0.5_1.25
- reason: combine two stable signal terms from Stage 2
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["macd_hist", "<=", 0.526484], ["adx", ">=", 21.924144]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.5, "tgt": 1.25}`
- FIT: n=73 PF=0.2889 net=Rs-27,931 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=45 PF=0.3183 net=Rs-16,607 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=118 PF=0.3001 net=Rs-44,539 win=22.03% t/s/e=16/86/16 dom=0.051/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 95 - Stage 3 / best train-side combination - TRAIN PF too low
- changed parameter: combo_signal_0.5_1.25
- reason: combine two stable signal terms from Stage 2
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["macd_hist", "<=", 0.526484], ["adx", ">=", 34.649849]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.5, "tgt": 1.25}`
- FIT: n=16 PF=0.4478 net=Rs-4,427 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=15 PF=0.3804 net=Rs-4,955 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=31 PF=0.4142 net=Rs-9,382 win=25.81% t/s/e=6/22/3 dom=0.149/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 96 - Stage 3 / best train-side combination - TRAIN PF too low
- changed parameter: combo_signal_0.5_1.25
- reason: combine two stable signal terms from Stage 2
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["macd_hist", "<=", 0.526484], ["rsi3max", ">=", 60.762584]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.5, "tgt": 1.25}`
- FIT: n=69 PF=0.4502 net=Rs-19,156 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=47 PF=0.3597 net=Rs-16,001 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=116 PF=0.4124 net=Rs-35,157 win=27.59% t/s/e=22/81/13 dom=0.04/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 97 - Stage 3 / best train-side combination - TRAIN PF too low
- changed parameter: combo_signal_0.5_1.25
- reason: combine two stable signal terms from Stage 2
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["macd_hist", "<=", 0.526484], ["rsi", ">=", 60.664555]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.5, "tgt": 1.25}`
- FIT: n=69 PF=0.4644 net=Rs-18,269 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=47 PF=0.3597 net=Rs-16,001 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=116 PF=0.4201 net=Rs-34,270 win=28.45% t/s/e=22/80/14 dom=0.04/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 98 - Stage 3 / best train-side combination - TRAIN PF too low
- changed parameter: combo_signal_0.5_1.25
- reason: combine two stable signal terms from Stage 2
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["macd_hist", "<=", 0.526484], ["rsi3max", ">=", 65.411252]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.5, "tgt": 1.25}`
- FIT: n=36 PF=0.5312 net=Rs-8,211 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=19 PF=0.3884 net=Rs-6,392 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=55 PF=0.4778 net=Rs-14,603 win=29.09% t/s/e=12/38/5 dom=0.075/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 99 - Stage 3 / best train-side combination - TRAIN PF too low
- changed parameter: combo_signal_0.5_1.25
- reason: combine two stable signal terms from Stage 2
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["macd_hist", "<=", 0.526484], ["rsi", "<=", 67.73262]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.5, "tgt": 1.25}`
- FIT: n=75 PF=0.424 net=Rs-21,268 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=46 PF=0.341 net=Rs-15,891 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=121 PF=0.3912 net=Rs-37,160 win=27.27% t/s/e=20/82/19 dom=0.041/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search

## Iter 100 - Stage 3 / best train-side combination - TRAIN PF too low
- changed parameter: combo_signal_0.5_1.25
- reason: combine two stable signal terms from Stage 2
- config: `{"daily_loss_rs": 0.0, "entry_guards": {}, "mask_terms": [["adx", ">=", 21.924144], ["adx", ">=", 34.649849]], "max_positions": 20, "premom_terms": [], "regime_align": false, "regime_band": 0.0, "sl": 0.5, "tgt": 1.25}`
- FIT: n=20 PF=0.5929 net=Rs-3,858 win=0% t/s/e=0/0/0 dom=None/None/None
- VAL: n=20 PF=0.262 net=Rs-8,572 win=0% t/s/e=0/0/0 dom=None/None/None
- TRAIN: n=40 PF=0.4106 net=Rs-12,430 win=25.0% t/s/e=8/29/3 dom=0.115/9.99/9.99
- TEST: not run (TRAIN PF not in band)
- keep/reject: REJECT
- next action: continue train-side search
