# DOC5D_AVWAP_RECLAIM_LONG (LONG) — ITERATION_LOG

_Generated 2026-07-01. Optimizer: Optuna TPE. 450 FIT/VAL trials (each trial = one logical config: exit SL/Tgt + <=2 mask + <=1 pre-momentum + guards)._

Search protocol: optimise ONLY on FIT/VAL with the band objective `reward(min(FIT_PF,VAL_PF)) - 0.80*|FIT_PF-VAL_PF|`, where reward tents at PF 1.70 and penalises overshoot (anti-overfit). The single best FIT/VAL config is then confirmed on full TRAIN and scored ONCE on TEST. Quantile thresholds drawn from TRAIN only (never TEST). The robust gate then checks TEST PF+day-block p, TRAIN target-fill, threshold-neighborhood stability, and term-dropout stability.

Exit grid searched: SL [0.5, 0.7, 0.85, 1.0, 1.1, 1.2, 1.5] x Tgt [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5] (small/large SL x small/large target, asymmetric combos). Mask feats: ['atr_pct<=0.002005', 'atr_pct>=0.004011', 'body_pct<=0.754479', 'body_pct<=0.783482', 'close_loc>=0.658621', 'close_loc>=0.702222', 'close_loc>=1.0', 'lower_wick_pct>=0.090029', 'quality_score<=89.159734', 'ranker_score<=77.56233', 'ranker_score>=61.241227', 'rs_pct<=0.544533'].

## Top 20 FIT/VAL trials (by band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|-----|------|--------|-------|----------|----------|-------|
| 1 | 1.0 | 1.5 | - | pre1_adx<=20.16048 | {"top_n": 1} | 11/1.041 | 27/0.982 | 0.9353 |
| 2 | 1.0 | 1.5 | - | pre1_adx<=20.16048 | {"top_n": 1} | 11/1.041 | 27/0.982 | 0.9353 |
| 3 | 1.0 | 1.5 | - | pre1_adx<=20.16048 | {"top_n": 1} | 11/1.041 | 27/0.982 | 0.9353 |
| 4 | 1.0 | 1.5 | - | pre1_adx<=20.16048 | {"top_n": 1} | 11/1.041 | 27/0.982 | 0.9353 |
| 5 | 1.0 | 1.5 | - | pre1_adx<=20.16048 | {"top_n": 1} | 11/1.041 | 27/0.982 | 0.9353 |
| 6 | 1.0 | 1.5 | - | pre1_adx<=20.16048 | {"top_n": 1} | 11/1.041 | 27/0.982 | 0.9353 |
| 7 | 1.0 | 1.5 | - | pre1_adx<=20.16048 | {"top_n": 1} | 11/1.041 | 27/0.982 | 0.9353 |
| 8 | 1.0 | 1.5 | - | pre1_adx<=20.16048 | {"top_n": 1} | 11/1.041 | 27/0.982 | 0.9353 |
| 9 | 1.0 | 1.5 | - | pre1_adx<=20.16048 | {"top_n": 1} | 11/1.041 | 27/0.982 | 0.9353 |
| 10 | 1.0 | 1.5 | - | pre1_adx<=20.16048 | {"top_n": 1} | 11/1.041 | 27/0.982 | 0.9353 |
| 11 | 1.0 | 1.5 | - | pre1_adx<=20.16048 | {"top_n": 1} | 11/1.041 | 27/0.982 | 0.9353 |
| 12 | 1.0 | 1.5 | - | pre1_adx<=20.16048 | {"top_n": 1} | 11/1.041 | 27/0.982 | 0.9353 |
| 13 | 1.0 | 1.5 | - | pre1_adx<=20.16048 | {"top_n": 1} | 11/1.041 | 27/0.982 | 0.9353 |
| 14 | 1.0 | 1.5 | - | pre1_adx<=20.16048 | {"top_n": 1} | 11/1.041 | 27/0.982 | 0.9353 |
| 15 | 1.0 | 1.5 | - | pre1_adx<=20.16048 | {"top_n": 1} | 11/1.041 | 27/0.982 | 0.9353 |
| 16 | 1.0 | 1.5 | - | pre1_adx<=20.16048 | {"top_n": 1} | 11/1.041 | 27/0.982 | 0.9353 |
| 17 | 1.0 | 1.5 | - | pre1_adx<=20.16048 | {"top_n": 1} | 11/1.041 | 27/0.982 | 0.9353 |
| 18 | 1.0 | 1.5 | - | pre1_adx<=20.16048 | {"top_n": 1} | 11/1.041 | 27/0.982 | 0.9353 |
| 19 | 1.0 | 1.5 | - | pre1_adx<=20.16048 | {"top_n": 1} | 11/1.041 | 27/0.982 | 0.9353 |
| 20 | 1.0 | 1.5 | - | pre1_adx<=20.16048 | {"top_n": 1} | 11/1.041 | 27/0.982 | 0.9353 |

## Best config — confirmation runs

- **Best (FIT/VAL):** SL 1.0/Tgt 1.5 | mask [(none)] | premom [pre1_adx<=20.16048] | guard {'top_n': 1} | maxpos 20 | dloss 4000.0
- **TRAIN @15bps:** n=38 PF=1.0 net=Rs-3 win%=47.4 avgW=Rs1,112 avgL=Rs-1,001 maxDD=Rs-6,375 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.71 tradeDom=0.068 dayDom=9.99 symDom=9.99 dbp=0.5084
- **TEST  @15bps:** n=5 PF=0.0 net=Rs-3,293 win%=0.0 avgW=Rs0 avgL=Rs-659 maxDD=Rs-2,542 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.5 tradeDom=9.99 dayDom=9.99 symDom=9.99 dbp=None
- **TRAIN @5bps:**  n=38 PF=1.0 net=Rs-3 win%=47.4 avgW=Rs1,112 avgL=Rs-1,001 maxDD=Rs-6,375 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.71 tradeDom=0.068 dayDom=9.99 symDom=9.99 dbp=0.5084
- **TEST  @5bps:**  n=5 PF=0.0 net=Rs-3,293 win%=0.0 avgW=Rs0 avgL=Rs-659 maxDD=Rs-2,542 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.5 tradeDom=9.99 dayDom=9.99 symDom=9.99 dbp=None

## Robustness diagnostics

- neighborhood pass: `False` (PF floor 1.15)
- term-dropout pass: `False` (PF floor 1.0)
- hard reasons: `TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed`
- insufficient reasons: `TEST too few trades (test_n<6)`
- warnings: `-`

- **Keep/reject:** REJECT  — TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST too few trades (test_n<6)

## Command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup DOC5D_AVWAP_RECLAIM_LONG --pool Train_and_Test/setup_pf_1_4_approval_loop/DOC5D_AVWAP_RECLAIM_LONG/pool_vC --trials 450 --time_budget_min 6.0 --seed 7 --gap_lambda 0.8 --max_mask_terms 2 --max_pm_terms 1 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```