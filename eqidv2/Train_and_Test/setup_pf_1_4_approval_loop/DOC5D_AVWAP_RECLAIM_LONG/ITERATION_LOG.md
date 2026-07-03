# DOC5D_AVWAP_RECLAIM_LONG (LONG) — ITERATION_LOG

_Generated 2026-07-01. Optimizer: Optuna TPE. 500 FIT/VAL trials (each trial = one logical config: exit SL/Tgt + <=1 mask + <=1 pre-momentum + guards)._

Search protocol: optimise ONLY on FIT/VAL with the band objective `reward(min(FIT_PF,VAL_PF)) - 0.80*|FIT_PF-VAL_PF|`, where reward tents at PF 1.70 and penalises overshoot (anti-overfit). The single best FIT/VAL config is then confirmed on full TRAIN and scored ONCE on TEST. Quantile thresholds drawn from TRAIN only (never TEST). The robust gate then checks TEST PF+day-block p, TRAIN target-fill, threshold-neighborhood stability, and term-dropout stability.

Exit grid searched: SL [0.5, 0.7, 0.85, 1.0, 1.1, 1.2, 1.5] x Tgt [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5] (small/large SL x small/large target, asymmetric combos). Mask feats: ['atr_pct<=0.00384', 'atr_pct>=0.001777', 'atr_pct>=0.002872', 'atr_pct>=0.003223', 'atr_pct>=0.00384', 'body_pct<=0.742217', 'body_pct<=0.846154', 'body_pct<=0.882353', 'body_pct>=0.5432', 'body_pct>=0.612903', 'body_pct>=0.742217', 'body_pct>=0.75'].

## Top 20 FIT/VAL trials (by band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|-----|------|--------|-------|----------|----------|-------|
| 1 | 1.1 | 2.5 | quality_score>=78.052591 | - | {"min_slot": "09:30", "max_slot": "13:00", "top_n": 2} | 13/0.774 | 13/0.76 | 0.7482 |
| 2 | 1.1 | 2.5 | quality_score>=78.052591 | - | {"min_slot": "09:30", "max_slot": "13:00", "top_n": 2} | 13/0.774 | 13/0.76 | 0.7482 |
| 3 | 1.1 | 2.5 | quality_score>=78.052591 | - | {"min_slot": "09:30", "max_slot": "13:00", "top_n": 2} | 13/0.774 | 13/0.76 | 0.7482 |
| 4 | 1.1 | 2.5 | quality_score>=78.052591 | - | {"min_slot": "09:30", "max_slot": "13:00", "top_n": 2} | 13/0.774 | 13/0.76 | 0.7482 |
| 5 | 1.1 | 2.5 | quality_score>=78.052591 | - | {"min_slot": "09:30", "max_slot": "13:00", "top_n": 2} | 13/0.774 | 13/0.76 | 0.7482 |
| 6 | 1.1 | 2.5 | quality_score>=78.052591 | - | {"min_slot": "09:30", "max_slot": "13:00", "top_n": 2} | 13/0.774 | 13/0.76 | 0.7482 |
| 7 | 1.1 | 2.5 | quality_score>=78.052591 | - | {"min_slot": "09:30", "max_slot": "14:00", "top_n": 2} | 13/0.774 | 13/0.76 | 0.7482 |
| 8 | 1.1 | 2.5 | quality_score>=78.052591 | - | {"min_slot": "09:30", "max_slot": "13:00", "top_n": 2} | 13/0.774 | 13/0.76 | 0.7482 |
| 9 | 1.1 | 2.5 | quality_score>=78.052591 | - | {"min_slot": "09:30", "max_slot": "13:00", "top_n": 2} | 13/0.774 | 13/0.76 | 0.7482 |
| 10 | 1.1 | 2.5 | quality_score>=78.052591 | - | {"min_slot": "09:30", "max_slot": "13:00", "top_n": 2} | 13/0.774 | 13/0.76 | 0.7482 |
| 11 | 1.1 | 2.5 | quality_score>=78.052591 | - | {"min_slot": "09:30", "max_slot": "13:00", "top_n": 2} | 13/0.774 | 13/0.76 | 0.7482 |
| 12 | 1.1 | 2.5 | quality_score>=78.052591 | - | {"min_slot": "09:30", "max_slot": "13:00", "top_n": 2} | 13/0.774 | 13/0.76 | 0.7482 |
| 13 | 1.1 | 2.5 | ranker_score>=78.052591 | - | {"min_slot": "09:30", "max_slot": "13:00", "top_n": 2} | 13/0.774 | 13/0.76 | 0.7482 |
| 14 | 1.1 | 2.5 | quality_score>=78.052591 | - | {"min_slot": "09:30", "max_slot": "13:00", "top_n": 2} | 13/0.774 | 13/0.76 | 0.7482 |
| 15 | 1.1 | 2.5 | quality_score>=78.052591 | - | {"min_slot": "09:30", "max_slot": "13:00", "top_n": 2} | 13/0.774 | 13/0.76 | 0.7482 |
| 16 | 1.1 | 2.5 | quality_score>=78.052591 | - | {"min_slot": "09:30", "max_slot": "13:00", "top_n": 3} | 13/0.774 | 13/0.76 | 0.7482 |
| 17 | 1.1 | 2.5 | quality_score>=78.052591 | - | {"min_slot": "09:30", "max_slot": "13:00", "top_n": 2} | 13/0.774 | 13/0.76 | 0.7482 |
| 18 | 1.1 | 2.5 | quality_score>=78.052591 | - | {"min_slot": "09:30", "max_slot": "13:00", "top_n": 2} | 13/0.774 | 13/0.76 | 0.7482 |
| 19 | 1.1 | 2.5 | quality_score>=78.052591 | - | {"min_slot": "09:30", "max_slot": "13:00", "top_n": 2} | 13/0.774 | 13/0.76 | 0.7482 |
| 20 | 1.1 | 2.5 | quality_score>=78.052591 | - | {"min_slot": "09:30", "max_slot": "13:00", "top_n": 2} | 13/0.774 | 13/0.76 | 0.7482 |

## Best config — confirmation runs

- **Best (FIT/VAL):** SL 1.1/Tgt 2.5 | mask [quality_score>=78.052591] | premom [(none)] | guard {'min_slot': '09:30', 'max_slot': '13:00', 'top_n': 2} | maxpos 10 | dloss 4000.0
- **TRAIN @15bps:** n=26 PF=0.767 net=Rs-2,654 win%=46.2 avgW=Rs727 avgL=Rs-812 maxDD=Rs-7,504 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.86 tradeDom=0.26 dayDom=9.99 symDom=9.99 dbp=0.7054
- **TEST  @15bps:** n=5 PF=0.0 net=Rs-4,543 win%=0.0 avgW=Rs0 avgL=Rs-909 maxDD=Rs-3,220 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.25 tradeDom=9.99 dayDom=9.99 symDom=9.99 dbp=1.0
- **TRAIN @5bps:**  n=26 PF=0.767 net=Rs-2,654 win%=46.2 avgW=Rs727 avgL=Rs-812 maxDD=Rs-7,504 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.86 tradeDom=0.26 dayDom=9.99 symDom=9.99 dbp=0.7054
- **TEST  @5bps:**  n=5 PF=0.0 net=Rs-4,543 win%=0.0 avgW=Rs0 avgL=Rs-909 maxDD=Rs-3,220 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.25 tradeDom=9.99 dayDom=9.99 symDom=9.99 dbp=1.0

## Robustness diagnostics

- neighborhood pass: `False` (PF floor 1.15)
- term-dropout pass: `False` (PF floor 1.0)
- hard reasons: `TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed`
- insufficient reasons: `TEST too few trades (test_n<6)`
- warnings: `-`

- **Keep/reject:** REJECT  — TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST too few trades (test_n<6)

## Command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup DOC5D_AVWAP_RECLAIM_LONG --pool Train_and_Test/doc5_long_setups/pool --trials 500 --time_budget_min 15.0 --seed 7 --gap_lambda 0.8 --max_mask_terms 1 --max_pm_terms 1 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```