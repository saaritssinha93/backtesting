# DOC5D_AVWAP_RECLAIM_LONG (LONG) — ITERATION_LOG

_Generated 2026-07-01. Optimizer: Optuna TPE. 500 FIT/VAL trials (each trial = one logical config: exit SL/Tgt + <=2 mask + <=1 pre-momentum + guards)._

Search protocol: optimise ONLY on FIT/VAL with the band objective `reward(min(FIT_PF,VAL_PF)) - 0.40*|FIT_PF-VAL_PF|`, where reward tents at PF 1.70 and penalises overshoot (anti-overfit). The single best FIT/VAL config is then confirmed on full TRAIN and scored ONCE on TEST. Quantile thresholds drawn from TRAIN only (never TEST). The robust gate then checks TEST PF+day-block p, TRAIN target-fill, threshold-neighborhood stability, and term-dropout stability.

Exit grid searched: SL [0.5, 0.7, 0.85, 1.0, 1.1, 1.2, 1.5] x Tgt [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5] (small/large SL x small/large target, asymmetric combos). Mask feats: ['atr_pct<=0.003223', 'atr_pct>=0.001589', 'atr_pct>=0.001777', 'atr_pct>=0.002872', 'atr_pct>=0.00483', 'body_pct<=0.612903', 'body_pct<=0.882353', 'body_pct>=0.612903', 'body_pct>=0.75', 'body_pct>=0.780138', 'body_pct>=0.846154', 'body_pct>=0.882353'].

## Top 20 FIT/VAL trials (by band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|-----|------|--------|-------|----------|----------|-------|
| 1 | 0.85 | 1.5 | close_loc>=0.894691 | sig5_vol_ratio20>=2.650909 | {"min_slot": "09:45", "top_n": 2} | 7/1.13 | 9/1.052 | 1.0211 |
| 2 | 0.85 | 1.5 | close_loc>=0.894691 | sig5_vol_ratio20>=2.650909 | {"min_slot": "09:45", "top_n": 2} | 7/1.13 | 9/1.052 | 1.0211 |
| 3 | 0.85 | 1.5 | close_loc>=0.894691 | sig5_vol_ratio20>=2.650909 | {"min_slot": "09:45", "top_n": 2} | 7/1.13 | 9/1.052 | 1.0211 |
| 4 | 0.85 | 1.5 | close_loc>=0.894691 | sig5_vol_ratio20>=2.650909 | {"top_n": 2} | 7/1.13 | 9/1.052 | 1.0211 |
| 5 | 0.85 | 1.5 | close_loc>=0.894691 | sig5_vol_ratio20>=2.650909 | {"min_slot": "09:45", "top_n": 2} | 7/1.13 | 9/1.052 | 1.0211 |
| 6 | 0.85 | 1.5 | close_loc>=0.894691 | sig5_vol_ratio20>=2.650909 | {"min_slot": "09:45", "top_n": 2} | 7/1.13 | 9/1.052 | 1.0211 |
| 7 | 0.85 | 1.5 | close_loc>=0.894691 | sig5_vol_ratio20>=2.650909 | {"min_slot": "09:45", "top_n": 2} | 7/1.13 | 9/1.052 | 1.0211 |
| 8 | 0.85 | 1.5 | close_loc>=0.894691 | sig5_vol_ratio20>=2.650909 | {"min_slot": "09:45", "top_n": 2} | 7/1.13 | 9/1.052 | 1.0211 |
| 9 | 0.85 | 1.5 | close_loc>=0.894691 | sig5_vol_ratio20>=2.650909 | {"min_slot": "09:45", "top_n": 2} | 7/1.13 | 9/1.052 | 1.0211 |
| 10 | 0.85 | 1.5 | close_loc>=0.894691 | sig5_vol_ratio20>=2.650909 | {"min_slot": "09:45", "top_n": 2} | 7/1.13 | 9/1.052 | 1.0211 |
| 11 | 0.85 | 1.5 | close_loc>=0.894691 | sig5_vol_ratio20>=2.650909 | {"top_n": 2} | 7/1.13 | 9/1.052 | 1.0211 |
| 12 | 0.85 | 1.5 | close_loc>=0.894691 | sig5_vol_ratio20>=2.650909 | {"min_slot": "09:45", "top_n": 2} | 7/1.13 | 9/1.052 | 1.0211 |
| 13 | 0.85 | 1.5 | close_loc>=0.894691 | sig5_vol_ratio20>=2.650909 | {"min_slot": "09:45", "top_n": 2} | 7/1.13 | 9/1.052 | 1.0211 |
| 14 | 0.85 | 1.5 | close_loc>=0.894691 | sig5_vol_ratio20>=2.650909 | {"top_n": 2} | 7/1.13 | 9/1.052 | 1.0211 |
| 15 | 0.85 | 1.5 | close_loc>=0.894691 | sig5_vol_ratio20>=2.650909 | {"min_slot": "09:45", "top_n": 2} | 7/1.13 | 9/1.052 | 1.0211 |
| 16 | 0.85 | 1.5 | close_loc>=0.894691 | sig5_vol_ratio20>=2.650909 | {"min_slot": "09:45", "top_n": 2} | 7/1.13 | 9/1.052 | 1.0211 |
| 17 | 0.85 | 1.5 | close_loc>=0.894691 | sig5_vol_ratio20>=2.650909 | {"min_slot": "09:45", "top_n": 2} | 7/1.13 | 9/1.052 | 1.0211 |
| 18 | 0.85 | 1.5 | close_loc>=0.894691 | sig5_vol_ratio20>=2.650909 | {"min_slot": "09:45", "top_n": 2} | 7/1.13 | 9/1.052 | 1.0211 |
| 19 | 0.85 | 1.5 | close_loc>=0.894691 | sig5_vol_ratio20>=2.650909 | {"min_slot": "09:45", "top_n": 2} | 7/1.13 | 9/1.052 | 1.0211 |
| 20 | 0.85 | 1.5 | close_loc>=0.894691 | sig5_vol_ratio20>=2.650909 | {"min_slot": "09:45", "top_n": 2} | 7/1.13 | 9/1.052 | 1.0211 |

## Best config — confirmation runs

- **Best (FIT/VAL):** SL 0.85/Tgt 1.5 | mask [close_loc>=0.894691] | premom [sig5_vol_ratio20>=2.650909] | guard {'top_n': 2} | maxpos 20 | dloss 0.0
- **TRAIN @15bps:** n=16 PF=1.086 net=Rs513 win%=50.0 avgW=Rs806 avgL=Rs-742 maxDD=Rs-3,180 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.6 tradeDom=0.196 dayDom=3.381 symDom=2.466 dbp=0.4413
- **TEST  @15bps:** n=3 PF=0.0 net=Rs-2,576 win%=0.0 avgW=Rs0 avgL=Rs-859 maxDD=Rs-1,798 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.0 tradeDom=9.99 dayDom=9.99 symDom=9.99 dbp=1.0
- **TRAIN @5bps:**  n=16 PF=1.086 net=Rs513 win%=50.0 avgW=Rs806 avgL=Rs-742 maxDD=Rs-3,180 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.6 tradeDom=0.196 dayDom=3.381 symDom=2.466 dbp=0.4413
- **TEST  @5bps:**  n=3 PF=0.0 net=Rs-2,576 win%=0.0 avgW=Rs0 avgL=Rs-859 maxDD=Rs-1,798 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.0 tradeDom=9.99 dayDom=9.99 symDom=9.99 dbp=1.0

## Robustness diagnostics

- neighborhood pass: `False` (PF floor 1.15)
- term-dropout pass: `False` (PF floor 1.0)
- hard reasons: `TRAIN too few trades (train_n<20); TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed`
- insufficient reasons: `TEST too few trades (test_n<6)`
- warnings: `-`

- **Keep/reject:** REJECT  — TRAIN too few trades (train_n<20); TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST too few trades (test_n<6)

## Command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup DOC5D_AVWAP_RECLAIM_LONG --pool Train_and_Test/doc5_long_setups/pool --trials 500 --time_budget_min 10.0 --seed 41 --gap_lambda 0.4 --max_mask_terms 2 --max_pm_terms 1 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```