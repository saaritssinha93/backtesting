# A_PULLBACK_C2_THEN_BREAK_C2_LOW - PARAMETER_SWEEP_SUMMARY

- Optimizer: Optuna TPE
- Iterations: 200
- Search rule: FIT/VAL only; full TRAIN confirmation only for reasonable FIT/VAL; TEST only for full TRAIN PF 1.3-1.8.

## Value Families Tested

- Indicator/filter values: FIT quantiles for atr_pct, body_pct, close_loc, lower_wick_pct, market_abs_ret_pct, market_ret_pct, notional, quality_score, rs_pct, signal_minute, signal_range_pct, upper_wick_pct, vol_ratio, vwap_dist_atr, wick_skew_pct.
- Pre-momentum values: FIT quantiles for pre1_adx, pre3_close_pos, pre3_range_r, pre5_mom_r, pre_entry_momentum_score, sig5_adx_calc, sig5_rsi_dir, sig5_vol_ratio20.
- Exit values: SL [0.5, 0.7, 0.85, 0.9, 1.0, 1.1, 1.2, 1.5]; target [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5].
- Guard values: min slots ['09:30', '09:45', '10:00', '10:30', '11:00']; max slots ['11:30', '12:00', '12:30', '13:00', '14:00', '14:30']; top_n 1/2/3.
- Portfolio values: max_positions [10, 20]; daily_loss_rs [0.0, 4000.0].

## Top 20 FIT/VAL Trials

| iter | group | config | FIT n/PF | VAL n/PF | TRAIN n/PF | TEST n/PF | keep/reject |
|---|---|---|---|---|---|---|---|
| 58 | exit+pre_momentum+guard | SL/Tgt=1.5/1.5 mask=[(none)] premom=[sig5_rsi_dir<=52.555584] guard={'max_slot': '11:30', 'top_n': 1} maxpos=20 dloss=0.0 | 32/0.526 | 14/1.029 | None/None | None/None | REJECT |
| 63 | exit+pre_momentum+guard | SL/Tgt=1.5/1.5 mask=[(none)] premom=[sig5_rsi_dir<=52.555584] guard={'max_slot': '11:30', 'top_n': 1} maxpos=20 dloss=0.0 | 32/0.526 | 14/1.029 | None/None | None/None | REJECT |
| 64 | exit+pre_momentum+guard | SL/Tgt=1.5/1.5 mask=[(none)] premom=[sig5_rsi_dir<=52.555584] guard={'max_slot': '11:30', 'top_n': 1} maxpos=20 dloss=0.0 | 32/0.526 | 14/1.029 | None/None | None/None | REJECT |
| 62 | exit+pre_momentum+guard | SL/Tgt=1.5/1.5 mask=[(none)] premom=[sig5_rsi_dir<=52.555584] guard={'max_slot': '11:30', 'top_n': 1} maxpos=20 dloss=0.0 | 32/0.526 | 14/1.029 | None/None | None/None | REJECT |
| 72 | exit+pre_momentum+guard | SL/Tgt=1.5/1.5 mask=[(none)] premom=[sig5_rsi_dir<=52.555584] guard={'max_slot': '11:30', 'top_n': 1} maxpos=20 dloss=0.0 | 32/0.526 | 14/1.029 | None/None | None/None | REJECT |
| 83 | exit+pre_momentum+guard | SL/Tgt=1.5/1.5 mask=[(none)] premom=[sig5_rsi_dir<=52.555584] guard={'max_slot': '11:30', 'top_n': 1} maxpos=20 dloss=0.0 | 32/0.526 | 14/1.029 | None/None | None/None | REJECT |
| 74 | exit+pre_momentum+guard | SL/Tgt=1.5/1.5 mask=[(none)] premom=[sig5_rsi_dir<=52.555584] guard={'max_slot': '11:30', 'top_n': 1} maxpos=20 dloss=0.0 | 32/0.526 | 14/1.029 | None/None | None/None | REJECT |
| 73 | exit+pre_momentum+guard | SL/Tgt=1.5/1.5 mask=[(none)] premom=[sig5_rsi_dir<=52.555584] guard={'max_slot': '11:30', 'top_n': 1} maxpos=20 dloss=0.0 | 32/0.526 | 14/1.029 | None/None | None/None | REJECT |
| 143 | exit+pre_momentum+guard | SL/Tgt=1.5/1.5 mask=[(none)] premom=[sig5_rsi_dir<=52.555584] guard={'max_slot': '11:30', 'top_n': 1} maxpos=20 dloss=0.0 | 32/0.526 | 14/1.029 | None/None | None/None | REJECT |
| 144 | exit+pre_momentum+guard | SL/Tgt=1.5/1.5 mask=[(none)] premom=[sig5_rsi_dir<=52.555584] guard={'max_slot': '11:30', 'top_n': 1} maxpos=20 dloss=0.0 | 32/0.526 | 14/1.029 | None/None | None/None | REJECT |
| 142 | exit+pre_momentum+guard | SL/Tgt=1.5/1.5 mask=[(none)] premom=[sig5_rsi_dir<=52.555584] guard={'max_slot': '11:30', 'top_n': 1} maxpos=20 dloss=0.0 | 32/0.526 | 14/1.029 | None/None | None/None | REJECT |
| 145 | exit+pre_momentum+guard | SL/Tgt=1.5/1.5 mask=[(none)] premom=[sig5_rsi_dir<=52.555584] guard={'max_slot': '11:30', 'top_n': 1} maxpos=20 dloss=0.0 | 32/0.526 | 14/1.029 | None/None | None/None | REJECT |
| 152 | exit+pre_momentum+guard | SL/Tgt=1.5/1.5 mask=[(none)] premom=[sig5_rsi_dir<=52.555584] guard={'max_slot': '11:30', 'top_n': 1} maxpos=20 dloss=0.0 | 32/0.526 | 14/1.029 | None/None | None/None | REJECT |
| 151 | exit+pre_momentum+guard | SL/Tgt=1.5/1.5 mask=[(none)] premom=[sig5_rsi_dir<=52.555584] guard={'max_slot': '11:30', 'top_n': 1} maxpos=20 dloss=0.0 | 32/0.526 | 14/1.029 | None/None | None/None | REJECT |
| 147 | exit+pre_momentum+guard | SL/Tgt=1.5/1.5 mask=[(none)] premom=[sig5_rsi_dir<=52.555584] guard={'max_slot': '11:30', 'top_n': 1} maxpos=10 dloss=0.0 | 32/0.526 | 14/1.029 | None/None | None/None | REJECT |
| 166 | exit+pre_momentum+guard | SL/Tgt=1.5/1.5 mask=[(none)] premom=[sig5_rsi_dir<=52.555584] guard={'max_slot': '11:30', 'top_n': 1} maxpos=20 dloss=0.0 | 32/0.526 | 14/1.029 | None/None | None/None | REJECT |
| 162 | exit+pre_momentum+guard | SL/Tgt=1.5/1.5 mask=[(none)] premom=[sig5_rsi_dir<=52.555584] guard={'max_slot': '11:30', 'top_n': 1} maxpos=20 dloss=0.0 | 32/0.526 | 14/1.029 | None/None | None/None | REJECT |
| 163 | exit+pre_momentum+guard | SL/Tgt=1.5/1.5 mask=[(none)] premom=[sig5_rsi_dir<=52.555584] guard={'max_slot': '11:30', 'top_n': 1} maxpos=20 dloss=0.0 | 32/0.526 | 14/1.029 | None/None | None/None | REJECT |
| 134 | exit+pre_momentum+guard | SL/Tgt=1.5/1.5 mask=[(none)] premom=[sig5_rsi_dir<=52.555584] guard={'max_slot': '11:30', 'top_n': 1} maxpos=20 dloss=0.0 | 32/0.526 | 14/1.029 | None/None | None/None | REJECT |
| 136 | exit+pre_momentum+guard | SL/Tgt=1.5/1.5 mask=[(none)] premom=[sig5_rsi_dir<=52.555584] guard={'max_slot': '11:30', 'top_n': 1} maxpos=20 dloss=0.0 | 32/0.526 | 14/1.029 | None/None | None/None | REJECT |

## Stable/Rejected Ranges

- exit+filter+guard: best FIT/VAL examples: iter 11 score -1.5831 REJECT | iter 12 score -1.5831 REJECT
- exit+filter+pre_momentum+guard: best FIT/VAL examples: iter 19 score -1.7444 REJECT
- exit+guard: best FIT/VAL examples: iter 37 score -1.6906 REJECT | iter 28 score -1.6966 REJECT | iter 40 score -1.7003 REJECT
- exit+indicator/filter+filter: best FIT/VAL examples: iter 5 score -1.7995 REJECT
- exit+indicator/filter+filter+pre_momentum+guard: best FIT/VAL examples: iter 1 score -1.5955 REJECT
- exit+indicator/filter+pre_momentum+guard: best FIT/VAL examples: iter 49 score -1.6458 REJECT | iter 21 score -1.7749 REJECT | iter 60 score -2.1792 REJECT
- exit+indicator/filter+price_action+filter+pre_momentum+guard: best FIT/VAL examples: iter 10 score -2.2445 REJECT
- exit+indicator/filter+price_action+pre_momentum+guard: best FIT/VAL examples: iter 2 score -2.1956 REJECT | iter 7 score -4.0 REJECT | iter 9 score -4.0 REJECT
- exit+pre_momentum: best FIT/VAL examples: iter 149 score -1.6313 REJECT
- exit+pre_momentum+guard: best FIT/VAL examples: iter 63 score -0.5761 REJECT | iter 64 score -0.5761 REJECT | iter 58 score -0.5761 REJECT
- exit+price_action+filter+pre_momentum+guard: best FIT/VAL examples: iter 66 score -3.6 REJECT | iter 120 score -4.0 REJECT
- exit+price_action+guard: best FIT/VAL examples: iter 13 score -1.5818 REJECT | iter 51 score -1.8733 REJECT
- exit+price_action+pre_momentum+guard: best FIT/VAL examples: iter 27 score -1.7149 REJECT | iter 6 score -1.7941 REJECT | iter 29 score -2.0218 REJECT
- pre_momentum+guard: best FIT/VAL examples: iter 170 score -1.4445 REJECT | iter 138 score -1.6872 REJECT | iter 88 score -1.7298 REJECT

- Overfit-risk rows flagged: 0 (TRAIN PF>1.8 or FIT high/VAL weak).
## Staged 5m-Enriched Rescue Sweep
- Engine: deterministic staged 5m-enriched rescue sweeps
- Configs evaluated: 6000
- Outcome counts: {'REJECT_FITVAL': 5800, 'REJECT_FULL_TRAIN': 189, 'REJECT_TEST_OR_STABILITY': 11}
- Passing candidates: 0
- Full TRAIN-band rows tested on TEST: 11
- Detailed results: `C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_LOW\staged_rescue_results.csv` and `C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_LOW\staged_rescue_summary.md`.
