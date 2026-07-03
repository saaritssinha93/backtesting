# A_MOD_BREAK_C1_HIGH (LONG) — ITERATION_LOG

_Generated 2026-07-01. Optimizer: Optuna TPE. 250 FIT/VAL trials (each trial = one logical config: exit SL/Tgt + <=1 mask + <=1 pre-momentum + guards)._

Search protocol: optimise ONLY on FIT/VAL with the band objective `reward(min(FIT_PF,VAL_PF)) - 0.80*|FIT_PF-VAL_PF|`, where reward tents at PF 1.70 and penalises overshoot (anti-overfit). The single best FIT/VAL config is then confirmed on full TRAIN and scored ONCE on TEST. Quantile thresholds drawn from TRAIN only (never TEST). The robust gate then checks TEST PF+day-block p, TRAIN target-fill, threshold-neighborhood stability, and term-dropout stability.

Exit grid searched: SL [0.5, 0.7, 0.85, 1.0, 1.1, 1.2, 1.5] x Tgt [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5] (small/large SL x small/large target, asymmetric combos). Mask feats: ['atr_pct<=0.012052', 'atr_pct<=0.01439', 'body_pct<=0.940337', 'body_pct>=0.5625', 'body_pct>=0.611111', 'body_pct>=0.753247', 'body_pct>=0.803483', 'body_pct>=0.940337', 'close_loc<=0.666667', 'close_loc>=0.773869', 'lower_wick_pct>=0.024605', 'lower_wick_pct>=0.18003'].

## Top 20 FIT/VAL trials (by band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|-----|------|--------|-------|----------|----------|-------|
| 1 | 0.85 | 1.25 | - | pre5_mom_r>=1.580359 | {"min_slot": "10:30", "top_n": 3} | 6/0.939 | 9/1.173 | 0.7518 |
| 2 | 0.85 | 2.5 | - | sig5_adx_calc<=32.50516 | {"max_slot": "14:30", "top_n": 3} | 9/0.799 | 6/1.044 | 0.6022 |
| 3 | 0.85 | 2.5 | - | sig5_adx_calc<=32.50516 | {"max_slot": "14:30", "top_n": 3} | 9/0.799 | 6/1.044 | 0.6022 |
| 4 | 0.85 | 2.5 | - | sig5_adx_calc<=32.50516 | {"max_slot": "14:30", "top_n": 3} | 9/0.799 | 6/1.044 | 0.6022 |
| 5 | 0.85 | 2.5 | - | sig5_adx_calc<=32.50516 | {"max_slot": "14:30", "top_n": 3} | 9/0.799 | 6/1.044 | 0.6022 |
| 6 | 0.85 | 2.5 | - | sig5_adx_calc<=32.50516 | {"max_slot": "14:30", "top_n": 3} | 9/0.799 | 6/1.044 | 0.6022 |
| 7 | 0.85 | 2.5 | - | sig5_adx_calc<=32.50516 | {"max_slot": "14:30", "top_n": 3} | 9/0.799 | 6/1.044 | 0.6022 |
| 8 | 0.85 | 2.5 | - | sig5_adx_calc<=32.50516 | {"max_slot": "14:30", "top_n": 3} | 9/0.799 | 6/1.044 | 0.6022 |
| 9 | 0.85 | 2.5 | - | sig5_adx_calc<=32.50516 | {"max_slot": "14:30", "top_n": 3} | 9/0.799 | 6/1.044 | 0.6022 |
| 10 | 0.85 | 2.5 | - | sig5_adx_calc<=32.50516 | {"max_slot": "14:30", "top_n": 3} | 9/0.799 | 6/1.044 | 0.6022 |
| 11 | 0.85 | 2.5 | - | sig5_adx_calc<=32.50516 | {"min_slot": "09:45", "max_slot": "14:30", "top_n": 3} | 9/0.799 | 6/1.044 | 0.6022 |
| 12 | 0.85 | 2.5 | - | sig5_adx_calc<=32.50516 | {"max_slot": "14:30", "top_n": 3} | 9/0.799 | 6/1.044 | 0.6022 |
| 13 | 0.85 | 2.5 | - | sig5_adx_calc<=32.50516 | {"max_slot": "14:30", "top_n": 3} | 9/0.799 | 6/1.044 | 0.6022 |
| 14 | 0.85 | 2.5 | - | sig5_adx_calc<=32.50516 | {"max_slot": "14:30", "top_n": 3} | 9/0.799 | 6/1.044 | 0.6022 |
| 15 | 0.85 | 2.5 | - | sig5_adx_calc<=32.50516 | {"max_slot": "14:30", "top_n": 3} | 9/0.799 | 6/1.044 | 0.6022 |
| 16 | 0.85 | 2.5 | - | sig5_adx_calc<=32.50516 | {"max_slot": "14:30", "top_n": 3} | 9/0.799 | 6/1.044 | 0.6022 |
| 17 | 0.85 | 2.5 | - | sig5_adx_calc<=32.50516 | {"max_slot": "14:30", "top_n": 3} | 9/0.799 | 6/1.044 | 0.6022 |
| 18 | 0.85 | 2.5 | - | sig5_adx_calc<=32.50516 | {"max_slot": "14:30", "top_n": 3} | 9/0.799 | 6/1.044 | 0.6022 |
| 19 | 0.85 | 2.5 | - | sig5_adx_calc<=32.50516 | {"max_slot": "14:30", "top_n": 3} | 9/0.799 | 6/1.044 | 0.6022 |
| 20 | 0.85 | 2.5 | - | sig5_adx_calc<=32.50516 | {"max_slot": "14:30", "top_n": 3} | 9/0.799 | 6/1.044 | 0.6022 |

## Best config — confirmation runs

- **Best (FIT/VAL):** SL 0.85/Tgt 1.25 | mask [(none)] | premom [pre5_mom_r>=1.580359] | guard {'min_slot': '10:30', 'top_n': 3} | maxpos 10 | dloss 4000.0
- **TRAIN @15bps:** n=15 PF=1.073 net=Rs549 win%=53.3 avgW=Rs1,013 avgL=Rs-1,079 maxDD=Rs-4,309 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.67 tradeDom=0.125 dayDom=3.695 symDom=1.85 dbp=0.4541
- **TEST  @15bps:** n=3 PF=0.47 net=Rs-1,144 win%=33.3 avgW=Rs1,014 avgL=Rs-1,079 maxDD=Rs-1,082 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.5 tradeDom=1.0 dayDom=9.99 symDom=9.99 dbp=None
- **TRAIN @5bps:**  n=15 PF=1.073 net=Rs549 win%=53.3 avgW=Rs1,013 avgL=Rs-1,079 maxDD=Rs-4,309 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.67 tradeDom=0.125 dayDom=3.695 symDom=1.85 dbp=0.4541
- **TEST  @5bps:**  n=3 PF=0.47 net=Rs-1,144 win%=33.3 avgW=Rs1,014 avgL=Rs-1,079 maxDD=Rs-1,082 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.5 tradeDom=1.0 dayDom=9.99 symDom=9.99 dbp=None

## Robustness diagnostics

- neighborhood pass: `False` (PF floor 1.15)
- term-dropout pass: `False` (PF floor 1.0)
- hard reasons: `TRAIN too few trades (train_n<20); TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed`
- insufficient reasons: `TEST too few trades (test_n<6)`
- warnings: `-`

- **Keep/reject:** REJECT  — TRAIN too few trades (train_n<20); TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST too few trades (test_n<6)

## Command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup A_MOD_BREAK_C1_HIGH --pool C:/TradingData/eqidv2/outputs_ID_v11_conf_fresh_20260629 --trials 250 --time_budget_min 14.0 --seed 7 --gap_lambda 0.8 --max_mask_terms 1 --max_pm_terms 1 --test_pf_min 1.3 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```