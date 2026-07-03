# DOC5D_AVWAP_RECLAIM_LONG (LONG) — ITERATION_LOG

_Generated 2026-07-01. Optimizer: Optuna TPE. 450 FIT/VAL trials (each trial = one logical config: exit SL/Tgt + <=2 mask + <=1 pre-momentum + guards)._

Search protocol: optimise ONLY on FIT/VAL with the band objective `reward(min(FIT_PF,VAL_PF)) - 0.80*|FIT_PF-VAL_PF|`, where reward tents at PF 1.70 and penalises overshoot (anti-overfit). The single best FIT/VAL config is then confirmed on full TRAIN and scored ONCE on TEST. Quantile thresholds drawn from TRAIN only (never TEST). The robust gate then checks TEST PF+day-block p, TRAIN target-fill, threshold-neighborhood stability, and term-dropout stability.

Exit grid searched: SL [0.5, 0.7, 0.85, 1.0, 1.1, 1.2, 1.5] x Tgt [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5] (small/large SL x small/large target, asymmetric combos). Mask feats: ['atr_pct<=0.002357', 'atr_pct<=0.002703', 'atr_pct<=0.003051', 'atr_pct<=0.004075', 'atr_pct>=0.001951', 'atr_pct>=0.002357', 'atr_pct>=0.002703', 'atr_pct>=0.003051', 'atr_pct>=0.004842', 'atr_pct>=0.005738', 'atr_pct>=0.007624', 'body_pct<=0.632947'].

## Top 20 FIT/VAL trials (by band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|-----|------|--------|-------|----------|----------|-------|
| 1 | 1.1 | 2.5 | ranker_score<=64.444809 | - | {"min_slot": "10:30", "top_n": 2} | 13/1.184 | 25/1.255 | 1.1262 |
| 2 | 1.1 | 2.5 | ranker_score<=64.444809 | - | {"min_slot": "10:30", "top_n": 2} | 13/1.184 | 25/1.255 | 1.1262 |
| 3 | 1.1 | 2.5 | quality_score<=64.444809 | - | {"min_slot": "10:30", "top_n": 2} | 13/1.184 | 25/1.255 | 1.1262 |
| 4 | 1.1 | 2.5 | quality_score<=64.444809 | - | {"min_slot": "10:30", "top_n": 2} | 13/1.184 | 25/1.255 | 1.1262 |
| 5 | 1.1 | 2.5 | ranker_score<=64.444809 | - | {"min_slot": "10:30", "top_n": 2} | 13/1.184 | 25/1.255 | 1.1262 |
| 6 | 1.1 | 2.5 | ranker_score<=64.444809 | - | {"min_slot": "10:30", "top_n": 2} | 13/1.184 | 25/1.255 | 1.1262 |
| 7 | 1.1 | 2.5 | ranker_score<=64.444809 | - | {"min_slot": "10:30", "top_n": 2} | 13/1.184 | 25/1.255 | 1.1262 |
| 8 | 1.1 | 2.5 | ranker_score<=64.444809 | - | {"min_slot": "10:30", "top_n": 2} | 13/1.184 | 25/1.255 | 1.1262 |
| 9 | 1.1 | 2.5 | ranker_score<=64.444809 | - | {"min_slot": "10:30", "top_n": 2} | 13/1.184 | 25/1.255 | 1.1262 |
| 10 | 1.1 | 2.5 | ranker_score<=64.444809 | - | {"min_slot": "10:30", "top_n": 2} | 13/1.184 | 25/1.255 | 1.1262 |
| 11 | 1.1 | 2.5 | quality_score<=64.444809 | - | {"min_slot": "10:30", "top_n": 2} | 13/1.184 | 25/1.255 | 1.1262 |
| 12 | 1.1 | 2.5 | ranker_score<=64.444809 | - | {"min_slot": "10:30", "top_n": 2} | 13/1.184 | 25/1.255 | 1.1262 |
| 13 | 1.1 | 2.5 | ranker_score<=64.444809 | - | {"min_slot": "10:30", "top_n": 2} | 13/1.184 | 25/1.255 | 1.1262 |
| 14 | 1.1 | 2.5 | ranker_score<=64.444809 | - | {"min_slot": "10:30", "top_n": 2} | 13/1.184 | 25/1.255 | 1.1262 |
| 15 | 1.1 | 2.5 | ranker_score<=64.444809 | - | {"min_slot": "10:30", "top_n": 2} | 13/1.184 | 25/1.255 | 1.1262 |
| 16 | 1.1 | 2.5 | quality_score<=64.444809 | - | {"min_slot": "10:30", "top_n": 2} | 13/1.184 | 25/1.255 | 1.1262 |
| 17 | 1.1 | 2.5 | ranker_score<=64.444809 | - | {"min_slot": "10:30", "top_n": 2} | 13/1.184 | 25/1.255 | 1.1262 |
| 18 | 1.1 | 2.5 | quality_score<=64.444809 | - | {"min_slot": "10:30", "top_n": 2} | 13/1.184 | 25/1.255 | 1.1262 |
| 19 | 1.1 | 2.5 | ranker_score<=64.444809 | - | {"min_slot": "10:30", "top_n": 2} | 13/1.184 | 25/1.255 | 1.1262 |
| 20 | 1.1 | 2.5 | ranker_score<=64.444809 | - | {"min_slot": "10:30", "top_n": 2} | 13/1.184 | 25/1.255 | 1.1262 |

## Best config — confirmation runs

- **Best (FIT/VAL):** SL 1.1/Tgt 2.5 | mask [ranker_score<=64.444809] | premom [(none)] | guard {'min_slot': '10:30', 'top_n': 2} | maxpos 10 | dloss 4000.0
- **TRAIN @15bps:** n=38 PF=1.233 net=Rs3,926 win%=47.4 avgW=Rs1,152 avgL=Rs-841 maxDD=Rs-7,085 SL/TGT/EOD=11/6/21 tgt%=15.8 tpd=2.11 tradeDom=0.114 dayDom=1.391 symDom=0.602 dbp=0.3406
- **TEST  @15bps:** n=24 PF=0.728 net=Rs-3,116 win%=45.8 avgW=Rs758 avgL=Rs-881 maxDD=Rs-7,573 SL/TGT/EOD=7/2/15 tgt%=8.3 tpd=4.0 tradeDom=0.284 dayDom=9.99 symDom=9.99 dbp=0.6945
- **TRAIN @5bps:**  n=38 PF=1.233 net=Rs3,926 win%=47.4 avgW=Rs1,152 avgL=Rs-841 maxDD=Rs-7,085 SL/TGT/EOD=11/6/21 tgt%=15.8 tpd=2.11 tradeDom=0.114 dayDom=1.391 symDom=0.602 dbp=0.3406
- **TEST  @5bps:**  n=24 PF=0.728 net=Rs-3,116 win%=45.8 avgW=Rs758 avgL=Rs-881 maxDD=Rs-7,573 SL/TGT/EOD=7/2/15 tgt%=8.3 tpd=4.0 tradeDom=0.284 dayDom=9.99 symDom=9.99 dbp=0.6945

## Robustness diagnostics

- neighborhood pass: `False` (PF floor 1.15)
- term-dropout pass: `False` (PF floor 1.0)
- hard reasons: `TRAIN PF too low (<1.30); TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates)`
- insufficient reasons: `-`
- warnings: `-`

- **Keep/reject:** REJECT  — TRAIN PF too low (<1.30); TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates)

## Command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup DOC5D_AVWAP_RECLAIM_LONG --pool Train_and_Test/setup_pf_1_4_approval_loop/DOC5D_AVWAP_RECLAIM_LONG/pool_vA --trials 450 --time_budget_min 6.0 --seed 7 --gap_lambda 0.8 --max_mask_terms 2 --max_pm_terms 1 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```