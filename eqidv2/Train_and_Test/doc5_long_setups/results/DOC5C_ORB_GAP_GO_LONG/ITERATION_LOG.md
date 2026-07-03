# DOC5C_ORB_GAP_GO_LONG (LONG) — ITERATION_LOG

_Generated 2026-07-01. Optimizer: Optuna TPE. 200 FIT/VAL trials (each trial = one logical config: exit SL/Tgt + <=1 mask + <=1 pre-momentum + guards)._

Search protocol: optimise ONLY on FIT/VAL with the band objective `reward(min(FIT_PF,VAL_PF)) - 0.80*|FIT_PF-VAL_PF|`, where reward tents at PF 1.70 and penalises overshoot (anti-overfit). The single best FIT/VAL config is then confirmed on full TRAIN and scored ONCE on TEST. Quantile thresholds drawn from TRAIN only (never TEST). The robust gate then checks TEST PF+day-block p, TRAIN target-fill, threshold-neighborhood stability, and term-dropout stability.

Exit grid searched: SL [0.5, 0.7, 0.85, 1.0, 1.1, 1.2, 1.5] x Tgt [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5] (small/large SL x small/large target, asymmetric combos). Mask feats: ['atr_pct<=0.005154', 'atr_pct>=0.00387', 'atr_pct>=0.005154', 'body_pct>=0.634444', 'body_pct>=0.826896', 'close_loc>=0.664452', 'close_loc>=0.867257', 'close_loc>=0.936439', 'lower_wick_pct<=0.0', 'lower_wick_pct<=0.041357', 'lower_wick_pct<=0.073091', 'lower_wick_pct>=0.0'].

## Top 20 FIT/VAL trials (by band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|-----|------|--------|-------|----------|----------|-------|
| 1 | 1.0 | 1.5 | vol_ratio>=2.637798 | pre3_range_r>=0.377377 | {"min_slot": "09:45", "max_slot": "12:30", "top_n": 1} | 9/0.571 | 6/0.594 | 0.5528 |
| 2 | 1.0 | 1.5 | vol_ratio>=2.637798 | pre3_range_r>=0.377377 | {"max_slot": "12:30", "top_n": 1} | 9/0.571 | 6/0.594 | 0.5528 |
| 3 | 1.0 | 1.5 | vol_ratio>=2.637798 | pre3_range_r>=0.377377 | {"max_slot": "12:30", "top_n": 1} | 9/0.571 | 6/0.594 | 0.5528 |
| 4 | 1.0 | 1.5 | vol_ratio>=2.637798 | pre3_range_r>=0.377377 | {"max_slot": "12:30", "top_n": 1} | 9/0.571 | 6/0.594 | 0.5528 |
| 5 | 1.0 | 1.5 | vol_ratio>=2.637798 | pre3_range_r>=0.377377 | {"max_slot": "12:30", "top_n": 1} | 9/0.571 | 6/0.594 | 0.5528 |
| 6 | 1.0 | 1.5 | vol_ratio>=2.637798 | pre3_range_r>=0.377377 | {"max_slot": "12:30", "top_n": 1} | 9/0.571 | 6/0.594 | 0.5528 |
| 7 | 1.0 | 1.5 | vol_ratio>=2.637798 | pre3_range_r>=0.377377 | {"max_slot": "12:30", "top_n": 1} | 9/0.571 | 6/0.594 | 0.5528 |
| 8 | 1.0 | 1.5 | vol_ratio>=2.637798 | pre3_range_r>=0.377377 | {"max_slot": "12:30", "top_n": 1} | 9/0.571 | 6/0.594 | 0.5528 |
| 9 | 1.0 | 1.5 | vol_ratio>=2.637798 | pre3_range_r>=0.377377 | {"max_slot": "12:30", "top_n": 1} | 9/0.571 | 6/0.594 | 0.5528 |
| 10 | 1.0 | 1.5 | vol_ratio>=2.637798 | pre3_range_r>=0.377377 | {"max_slot": "12:30", "top_n": 1} | 9/0.571 | 6/0.594 | 0.5528 |
| 11 | 1.0 | 1.5 | vol_ratio>=2.637798 | pre3_range_r>=0.377377 | {"max_slot": "12:30", "top_n": 1} | 9/0.571 | 6/0.594 | 0.5528 |
| 12 | 1.0 | 1.5 | vol_ratio>=2.637798 | pre3_range_r>=0.377377 | {"max_slot": "12:30", "top_n": 1} | 9/0.571 | 6/0.594 | 0.5528 |
| 13 | 1.0 | 1.5 | vol_ratio>=2.637798 | pre3_range_r>=0.377377 | {"max_slot": "12:30", "top_n": 1} | 9/0.571 | 6/0.594 | 0.5528 |
| 14 | 1.0 | 1.5 | vol_ratio>=2.637798 | pre3_range_r>=0.377377 | {"max_slot": "12:30", "top_n": 1} | 9/0.571 | 6/0.594 | 0.5528 |
| 15 | 1.0 | 1.5 | vol_ratio>=2.637798 | pre3_range_r>=0.377377 | {"max_slot": "12:30", "top_n": 1} | 9/0.571 | 6/0.594 | 0.5528 |
| 16 | 1.0 | 1.5 | vol_ratio>=2.637798 | pre3_range_r>=0.377377 | {"max_slot": "12:30", "top_n": 1} | 9/0.571 | 6/0.594 | 0.5528 |
| 17 | 1.0 | 1.5 | vol_ratio>=2.637798 | pre3_range_r>=0.377377 | {"max_slot": "12:30", "top_n": 1} | 9/0.571 | 6/0.594 | 0.5528 |
| 18 | 1.0 | 1.5 | vol_ratio>=2.637798 | pre3_range_r>=0.377377 | {"max_slot": "12:30", "top_n": 1} | 9/0.571 | 6/0.594 | 0.5528 |
| 19 | 1.0 | 1.5 | vol_ratio>=2.637798 | pre3_range_r>=0.377377 | {"max_slot": "12:30", "top_n": 1} | 9/0.571 | 6/0.594 | 0.5528 |
| 20 | 1.0 | 1.5 | vol_ratio>=2.637798 | pre3_range_r>=0.377377 | {"min_slot": "09:45", "max_slot": "12:30", "top_n": 1} | 9/0.571 | 6/0.594 | 0.5528 |

## Best config — confirmation runs

- **Best (FIT/VAL):** SL 1.0/Tgt 1.5 | mask [vol_ratio>=2.637798] | premom [pre3_range_r>=0.377377] | guard {'max_slot': '12:30', 'top_n': 1} | maxpos 10 | dloss 0.0
- **TRAIN @15bps:** n=15 PF=0.581 net=Rs-4,209 win%=33.3 avgW=Rs1,168 avgL=Rs-1,005 maxDD=Rs-5,466 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.5 tradeDom=0.217 dayDom=9.99 symDom=9.99 dbp=0.8108
- **TEST  @15bps:** n=8 PF=0.258 net=Rs-4,253 win%=25.0 avgW=Rs739 avgL=Rs-955 maxDD=Rs-3,244 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.33 tradeDom=0.855 dayDom=9.99 symDom=9.99 dbp=0.9986
- **TRAIN @5bps:**  n=15 PF=0.581 net=Rs-4,209 win%=33.3 avgW=Rs1,168 avgL=Rs-1,005 maxDD=Rs-5,466 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.5 tradeDom=0.217 dayDom=9.99 symDom=9.99 dbp=0.8108
- **TEST  @5bps:**  n=8 PF=0.258 net=Rs-4,253 win%=25.0 avgW=Rs739 avgL=Rs-955 maxDD=Rs-3,244 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.33 tradeDom=0.855 dayDom=9.99 symDom=9.99 dbp=0.9986

## Robustness diagnostics

- neighborhood pass: `False` (PF floor 1.15)
- term-dropout pass: `False` (PF floor 1.0)
- hard reasons: `TRAIN too few trades (train_n<20); TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.30; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates)`
- insufficient reasons: `-`
- warnings: `-`

- **Keep/reject:** REJECT  — TRAIN too few trades (train_n<20); TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.30; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates)

## Command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup DOC5C_ORB_GAP_GO_LONG --pool C:/Users/Saarit/OneDrive/Desktop/Trading/backtesting/eqidv2/backtesting/eqidv2/Train_and_Test/doc5_long_setups/pool --trials 200 --time_budget_min 10.0 --seed 7 --gap_lambda 0.8 --max_mask_terms 1 --max_pm_terms 1 --test_pf_min 1.3 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```