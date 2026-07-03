# DOC5D_AVWAP_RECLAIM_LONG (LONG) — ITERATION_LOG

_Generated 2026-07-01. Optimizer: Optuna TPE. 500 FIT/VAL trials (each trial = one logical config: exit SL/Tgt + <=2 mask + <=2 pre-momentum + guards)._

Search protocol: optimise ONLY on FIT/VAL with the band objective `reward(min(FIT_PF,VAL_PF)) - 0.80*|FIT_PF-VAL_PF|`, where reward tents at PF 1.70 and penalises overshoot (anti-overfit). The single best FIT/VAL config is then confirmed on full TRAIN and scored ONCE on TEST. Quantile thresholds drawn from TRAIN only (never TEST). The robust gate then checks TEST PF+day-block p, TRAIN target-fill, threshold-neighborhood stability, and term-dropout stability.

Exit grid searched: SL [0.5, 0.7, 0.85, 1.0, 1.1, 1.2, 1.5] x Tgt [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5] (small/large SL x small/large target, asymmetric combos). Mask feats: ['atr_pct<=0.001589', 'atr_pct<=0.002392', 'atr_pct<=0.002872', 'atr_pct<=0.003498', 'atr_pct<=0.00384', 'atr_pct>=0.002005', 'body_pct<=0.5432', 'body_pct<=0.742217', 'body_pct<=0.780138', 'body_pct<=0.846154', 'body_pct<=0.882353', 'body_pct<=0.943067'].

## Top 20 FIT/VAL trials (by band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|-----|------|--------|-------|----------|----------|-------|
| 1 | 1.0 | 0.8 | body_pct>=0.780138 | sig5_vol_ratio20>=1.682633;pre5_mom_r>=-0.026161 | {"min_slot": "10:00", "max_slot": "12:00", "top_n": 2} | 7/0.539 | 9/0.611 | 0.4813 |
| 2 | 1.0 | 0.8 | close_loc<=0.894691 | sig5_rsi_dir>=56.402892;pre5_mom_r>=-0.026161 | {"min_slot": "09:30", "max_slot": "12:00", "top_n": 2} | 9/0.467 | 6/0.463 | 0.4601 |
| 3 | 1.0 | 0.8 | close_loc<=0.894691 | sig5_rsi_dir>=56.402892;pre5_mom_r>=-0.026161 | {"min_slot": "09:30", "max_slot": "12:00", "top_n": 2} | 9/0.467 | 6/0.463 | 0.4601 |
| 4 | 1.0 | 0.8 | close_loc<=0.894691 | sig5_rsi_dir>=56.402892;pre5_mom_r>=-0.026161 | {"min_slot": "09:30", "max_slot": "12:00", "top_n": 2} | 9/0.467 | 6/0.463 | 0.4601 |
| 5 | 1.0 | 0.8 | close_loc<=0.894691 | sig5_rsi_dir>=56.402892;pre5_mom_r>=-0.026161 | {"min_slot": "09:45", "max_slot": "12:00", "top_n": 2} | 9/0.467 | 6/0.463 | 0.4601 |
| 6 | 1.0 | 0.8 | close_loc<=0.894691 | sig5_rsi_dir>=56.402892;pre5_mom_r>=-0.026161 | {"min_slot": "09:30", "max_slot": "12:00", "top_n": 2} | 9/0.467 | 6/0.463 | 0.4601 |
| 7 | 1.0 | 0.8 | close_loc<=0.894691 | sig5_rsi_dir>=56.402892;pre5_mom_r>=-0.026161 | {"min_slot": "09:30", "max_slot": "12:00", "top_n": 2} | 9/0.467 | 6/0.463 | 0.4601 |
| 8 | 1.0 | 0.8 | close_loc<=0.894691 | sig5_rsi_dir>=56.402892;pre5_mom_r>=-0.026161 | {"max_slot": "12:00", "top_n": 2} | 9/0.467 | 6/0.463 | 0.4601 |
| 9 | 1.0 | 0.8 | close_loc<=0.894691 | sig5_rsi_dir>=56.402892;pre5_mom_r>=-0.026161 | {"min_slot": "09:30", "max_slot": "12:00", "top_n": 2} | 9/0.467 | 6/0.463 | 0.4601 |
| 10 | 1.0 | 0.8 | close_loc<=0.894691 | sig5_rsi_dir>=56.402892;pre5_mom_r>=-0.026161 | {"min_slot": "09:30", "max_slot": "12:00", "top_n": 2} | 9/0.467 | 6/0.463 | 0.4601 |
| 11 | 1.0 | 0.8 | close_loc<=0.894691 | sig5_rsi_dir>=56.402892;pre5_mom_r>=-0.026161 | {"min_slot": "09:30", "max_slot": "12:00", "top_n": 2} | 9/0.467 | 6/0.463 | 0.4601 |
| 12 | 1.0 | 0.8 | close_loc<=0.894691 | sig5_rsi_dir>=56.402892;pre5_mom_r>=-0.026161 | {"min_slot": "09:30", "max_slot": "12:00", "top_n": 2} | 9/0.467 | 6/0.463 | 0.4601 |
| 13 | 1.0 | 0.8 | close_loc<=0.894691 | sig5_rsi_dir>=56.402892;pre5_mom_r>=-0.026161 | {"min_slot": "09:30", "max_slot": "12:00", "top_n": 2} | 9/0.467 | 6/0.463 | 0.4601 |
| 14 | 1.0 | 0.8 | close_loc<=0.894691 | sig5_rsi_dir>=56.402892;pre5_mom_r>=-0.026161 | {"min_slot": "09:30", "max_slot": "12:00", "top_n": 2} | 9/0.467 | 6/0.463 | 0.4601 |
| 15 | 1.0 | 0.8 | close_loc<=0.894691 | sig5_rsi_dir>=56.402892;pre5_mom_r>=-0.026161 | {"min_slot": "09:30", "max_slot": "12:00", "top_n": 2} | 9/0.467 | 6/0.463 | 0.4601 |
| 16 | 1.0 | 0.8 | close_loc<=0.894691 | sig5_rsi_dir>=56.402892;pre5_mom_r>=-0.026161 | {"min_slot": "09:30", "max_slot": "12:00", "top_n": 2} | 9/0.467 | 6/0.463 | 0.4601 |
| 17 | 1.0 | 0.8 | close_loc<=0.894691 | sig5_rsi_dir>=56.402892;pre5_mom_r>=-0.026161 | {"min_slot": "09:30", "max_slot": "12:00", "top_n": 2} | 9/0.467 | 6/0.463 | 0.4601 |
| 18 | 1.0 | 0.8 | close_loc<=0.894691 | sig5_rsi_dir>=56.402892;pre5_mom_r>=-0.026161 | {"min_slot": "09:45", "max_slot": "12:00", "top_n": 2} | 9/0.467 | 6/0.463 | 0.4601 |
| 19 | 1.0 | 0.8 | close_loc<=0.894691 | sig5_rsi_dir>=56.402892;pre5_mom_r>=-0.026161 | {"min_slot": "09:45", "max_slot": "12:00", "top_n": 2} | 9/0.467 | 6/0.463 | 0.4601 |
| 20 | 1.0 | 0.8 | close_loc<=0.894691 | sig5_rsi_dir>=56.402892;pre5_mom_r>=-0.026161 | {"min_slot": "09:45", "max_slot": "12:00", "top_n": 2} | 9/0.467 | 6/0.463 | 0.4601 |

## Best config — confirmation runs

- **Best (FIT/VAL):** SL 1.0/Tgt 0.8 | mask [body_pct>=0.780138] | premom [sig5_vol_ratio20>=1.682633; pre5_mom_r>=-0.026161] | guard {'min_slot': '10:00', 'max_slot': '12:00', 'top_n': 2} | maxpos 20 | dloss 4000.0
- **TRAIN @15bps:** n=16 PF=0.581 net=Rs-3,138 win%=50.0 avgW=Rs543 avgL=Rs-935 maxDD=Rs-4,118 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.6 tradeDom=0.13 dayDom=9.99 symDom=9.99 dbp=0.8754
- **TEST  @15bps:** n=3 PF=0.303 net=Rs-1,288 win%=33.3 avgW=Rs559 avgL=Rs-923 maxDD=Rs-614 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.0 tradeDom=1.0 dayDom=9.99 symDom=9.99 dbp=0.8465
- **TRAIN @5bps:**  n=16 PF=0.581 net=Rs-3,138 win%=50.0 avgW=Rs543 avgL=Rs-935 maxDD=Rs-4,118 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.6 tradeDom=0.13 dayDom=9.99 symDom=9.99 dbp=0.8754
- **TEST  @5bps:**  n=3 PF=0.303 net=Rs-1,288 win%=33.3 avgW=Rs559 avgL=Rs-923 maxDD=Rs-614 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.0 tradeDom=1.0 dayDom=9.99 symDom=9.99 dbp=0.8465

## Robustness diagnostics

- neighborhood pass: `False` (PF floor 1.15)
- term-dropout pass: `False` (PF floor 1.0)
- hard reasons: `TRAIN too few trades (train_n<20); TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed`
- insufficient reasons: `TEST too few trades (test_n<6)`
- warnings: `-`

- **Keep/reject:** REJECT  — TRAIN too few trades (train_n<20); TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST too few trades (test_n<6)

## Command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup DOC5D_AVWAP_RECLAIM_LONG --pool Train_and_Test/doc5_long_setups/pool --trials 500 --time_budget_min 10.0 --seed 23 --gap_lambda 0.8 --max_mask_terms 2 --max_pm_terms 2 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```