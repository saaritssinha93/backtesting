# DOC5D_AVWAP_RECLAIM_LONG (LONG) — ITERATION_LOG

_Generated 2026-07-01. Optimizer: Optuna TPE. 500 FIT/VAL trials (each trial = one logical config: exit SL/Tgt + <=1 mask + <=2 pre-momentum + guards)._

Search protocol: optimise ONLY on FIT/VAL with the band objective `reward(min(FIT_PF,VAL_PF)) - 0.80*|FIT_PF-VAL_PF|`, where reward tents at PF 1.70 and penalises overshoot (anti-overfit). The single best FIT/VAL config is then confirmed on full TRAIN and scored ONCE on TEST. Quantile thresholds drawn from TRAIN only (never TEST). The robust gate then checks TEST PF+day-block p, TRAIN target-fill, threshold-neighborhood stability, and term-dropout stability.

Exit grid searched: SL [0.5, 0.7, 0.85, 1.0, 1.1, 1.2, 1.5] x Tgt [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5] (small/large SL x small/large target, asymmetric combos). Mask feats: ['atr_pct<=0.001589', 'atr_pct>=0.003498', 'atr_pct>=0.00384', 'atr_pct>=0.00483', 'body_pct>=0.780138', 'body_pct>=0.846154', 'close_loc<=0.839286', 'close_loc<=1.0', 'close_loc>=0.75', 'lower_wick_pct<=0.017219', 'lower_wick_pct>=0.038486', 'lower_wick_pct>=0.069606'].

## Top 20 FIT/VAL trials (by band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|-----|------|--------|-------|----------|----------|-------|
| 1 | 1.1 | 0.8 | ranker_score>=84.792783 | - | {"max_slot": "12:30", "top_n": 3} | 7/0.778 | 6/0.759 | 0.7432 |
| 2 | 1.1 | 0.8 | ranker_score>=84.792783 | - | {"max_slot": "14:00", "top_n": 3} | 8/0.973 | 6/0.759 | 0.5872 |
| 3 | 1.1 | 0.8 | ranker_score>=84.792783 | - | {"max_slot": "14:00", "top_n": 3} | 8/0.973 | 6/0.759 | 0.5872 |
| 4 | 1.1 | 0.8 | ranker_score>=84.792783 | - | {"max_slot": "14:00", "top_n": 3} | 8/0.973 | 6/0.759 | 0.5872 |
| 5 | 1.1 | 0.8 | ranker_score>=84.792783 | - | {"max_slot": "14:00", "top_n": 3} | 8/0.973 | 6/0.759 | 0.5872 |
| 6 | 1.1 | 0.8 | ranker_score>=84.792783 | - | {"max_slot": "14:00", "top_n": 3} | 8/0.973 | 6/0.759 | 0.5872 |
| 7 | 1.1 | 0.8 | ranker_score>=84.792783 | - | {"max_slot": "14:00", "top_n": 3} | 8/0.973 | 6/0.759 | 0.5872 |
| 8 | 1.1 | 0.8 | ranker_score>=84.792783 | - | {"max_slot": "14:00", "top_n": 3} | 8/0.973 | 6/0.759 | 0.5872 |
| 9 | 1.1 | 0.8 | ranker_score>=84.792783 | - | {"max_slot": "14:00", "top_n": 3} | 8/0.973 | 6/0.759 | 0.5872 |
| 10 | 1.1 | 0.8 | ranker_score>=84.792783 | - | {"max_slot": "14:00", "top_n": 3} | 8/0.973 | 6/0.759 | 0.5872 |
| 11 | 1.1 | 0.8 | ranker_score>=84.792783 | - | {"max_slot": "14:00", "top_n": 3} | 8/0.973 | 6/0.759 | 0.5872 |
| 12 | 1.1 | 0.8 | ranker_score>=84.792783 | - | {"max_slot": "14:00", "top_n": 3} | 8/0.973 | 6/0.759 | 0.5872 |
| 13 | 1.1 | 0.8 | ranker_score>=84.792783 | - | {"max_slot": "14:00", "top_n": 3} | 8/0.973 | 6/0.759 | 0.5872 |
| 14 | 1.1 | 0.8 | ranker_score>=84.792783 | - | {"max_slot": "14:00", "top_n": 3} | 8/0.973 | 6/0.759 | 0.5872 |
| 15 | 1.1 | 0.8 | ranker_score>=84.792783 | - | {"max_slot": "14:00", "top_n": 3} | 8/0.973 | 6/0.759 | 0.5872 |
| 16 | 0.85 | 2.0 | body_pct>=0.846154 | - | {"min_slot": "09:45", "top_n": 2} | 11/0.548 | 15/0.654 | 0.464 |
| 17 | 0.85 | 2.0 | body_pct>=0.846154 | - | {"top_n": 3} | 11/0.548 | 15/0.654 | 0.464 |
| 18 | 1.1 | 2.5 | ranker_score>=84.792783 | - | {"max_slot": "14:00", "top_n": 3} | 8/0.947 | 6/1.577 | 0.4426 |
| 19 | 1.1 | 2.5 | ranker_score>=84.792783 | - | {"max_slot": "14:00", "top_n": 3} | 8/0.947 | 6/1.577 | 0.4426 |
| 20 | 1.1 | 2.5 | ranker_score>=84.792783 | - | {"max_slot": "14:00", "top_n": 3} | 8/0.947 | 6/1.577 | 0.4426 |

## Best config — confirmation runs

- **Best (FIT/VAL):** SL 1.1/Tgt 0.8 | mask [ranker_score>=84.792783] | premom [(none)] | guard {'max_slot': '12:30', 'top_n': 3} | maxpos 20 | dloss 4000.0
- **TRAIN @15bps:** n=13 PF=0.769 net=Rs-1,225 win%=61.5 avgW=Rs510 avgL=Rs-1,062 maxDD=Rs-4,097 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.18 tradeDom=0.139 dayDom=9.99 symDom=9.99 dbp=0.6442
- **TEST  @15bps:** n=2 PF=0.423 net=Rs-766 win%=50.0 avgW=Rs561 avgL=Rs-1,327 maxDD=Rs0 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.0 tradeDom=1.0 dayDom=9.99 symDom=9.99 dbp=None
- **TRAIN @5bps:**  n=13 PF=0.769 net=Rs-1,225 win%=61.5 avgW=Rs510 avgL=Rs-1,062 maxDD=Rs-4,097 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.18 tradeDom=0.139 dayDom=9.99 symDom=9.99 dbp=0.6442
- **TEST  @5bps:**  n=2 PF=0.423 net=Rs-766 win%=50.0 avgW=Rs561 avgL=Rs-1,327 maxDD=Rs0 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.0 tradeDom=1.0 dayDom=9.99 symDom=9.99 dbp=None

## Robustness diagnostics

- neighborhood pass: `False` (PF floor 1.15)
- term-dropout pass: `False` (PF floor 1.0)
- hard reasons: `TRAIN too few trades (train_n<20); TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed`
- insufficient reasons: `TEST too few trades (test_n<6)`
- warnings: `-`

- **Keep/reject:** REJECT  — TRAIN too few trades (train_n<20); TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST too few trades (test_n<6)

## Command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup DOC5D_AVWAP_RECLAIM_LONG --pool Train_and_Test/doc5_long_setups/pool --trials 500 --time_budget_min 10.0 --seed 11 --gap_lambda 0.8 --max_mask_terms 1 --max_pm_terms 2 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```