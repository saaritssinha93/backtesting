# DOC5B_MOMO_BREAKOUT_LONG (LONG) — ITERATION_LOG

_Generated 2026-07-01. Optimizer: Optuna TPE. 200 FIT/VAL trials (each trial = one logical config: exit SL/Tgt + <=1 mask + <=1 pre-momentum + guards)._

Search protocol: optimise ONLY on FIT/VAL with the band objective `reward(min(FIT_PF,VAL_PF)) - 0.80*|FIT_PF-VAL_PF|`, where reward tents at PF 1.70 and penalises overshoot (anti-overfit). The single best FIT/VAL config is then confirmed on full TRAIN and scored ONCE on TEST. Quantile thresholds drawn from TRAIN only (never TEST). The robust gate then checks TEST PF+day-block p, TRAIN target-fill, threshold-neighborhood stability, and term-dropout stability.

Exit grid searched: SL [0.5, 0.7, 0.85, 1.0, 1.1, 1.2, 1.5] x Tgt [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5] (small/large SL x small/large target, asymmetric combos). Mask feats: ['atr_pct<=0.001902', 'atr_pct<=0.002875', 'atr_pct<=0.005922', 'body_pct<=0.33333', 'body_pct<=0.616657', 'body_pct<=0.722222', 'body_pct<=0.818693', 'body_pct<=0.933218', 'close_loc<=0.751849', 'close_loc<=0.830126', 'close_loc<=0.932992', 'close_loc>=0.900239'].

## Top 20 FIT/VAL trials (by band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|-----|------|--------|-------|----------|----------|-------|
| 1 | 1.1 | 1.0 | ranker_score<=72.064624 | - | {"min_slot": "11:00", "max_slot": "12:30"} | 20/0.703 | 21/0.771 | 0.649 |
| 2 | 1.1 | 1.0 | ranker_score<=72.064624 | - | {"min_slot": "11:00", "max_slot": "12:30"} | 20/0.703 | 21/0.771 | 0.649 |
| 3 | 1.1 | 1.0 | ranker_score<=72.064624 | - | {"min_slot": "11:00", "max_slot": "12:30"} | 20/0.703 | 21/0.771 | 0.649 |
| 4 | 1.1 | 1.0 | ranker_score<=72.064624 | - | {"min_slot": "11:00", "max_slot": "12:30"} | 20/0.703 | 21/0.771 | 0.649 |
| 5 | 1.1 | 1.0 | ranker_score<=72.064624 | - | {"min_slot": "11:00", "max_slot": "12:30"} | 20/0.703 | 21/0.771 | 0.649 |
| 6 | 1.1 | 1.0 | ranker_score<=72.064624 | - | {"min_slot": "11:00", "max_slot": "12:30"} | 20/0.703 | 21/0.771 | 0.649 |
| 7 | 1.1 | 1.0 | ranker_score<=72.064624 | - | {"min_slot": "11:00", "max_slot": "12:30"} | 20/0.703 | 21/0.771 | 0.649 |
| 8 | 1.1 | 1.25 | ranker_score<=72.064624 | - | {"min_slot": "09:45", "max_slot": "12:30"} | 32/0.577 | 29/0.637 | 0.5298 |
| 9 | 1.1 | 1.25 | ranker_score<=72.064624 | - | {"min_slot": "09:45", "max_slot": "12:30"} | 32/0.577 | 29/0.637 | 0.5298 |
| 10 | 1.1 | 1.25 | ranker_score<=72.064624 | - | {"min_slot": "09:45", "max_slot": "12:30"} | 32/0.577 | 29/0.637 | 0.5298 |
| 11 | 1.1 | 1.0 | upper_wick_pct<=0.0 | - | {"min_slot": "11:00", "max_slot": "12:30"} | 19/0.514 | 17/0.54 | 0.4925 |
| 12 | 1.1 | 1.0 | upper_wick_pct<=0.0 | - | {"min_slot": "11:00", "max_slot": "12:30"} | 19/0.514 | 17/0.54 | 0.4925 |
| 13 | 1.1 | 1.0 | upper_wick_pct<=0.0 | - | {"min_slot": "11:00", "max_slot": "12:30"} | 19/0.514 | 17/0.54 | 0.4925 |
| 14 | 1.1 | 1.0 | upper_wick_pct<=0.0 | - | {"min_slot": "11:00", "max_slot": "12:30"} | 19/0.514 | 17/0.54 | 0.4925 |
| 15 | 1.1 | 1.0 | upper_wick_pct<=0.0 | - | {"min_slot": "11:00", "max_slot": "12:30"} | 19/0.514 | 17/0.54 | 0.4925 |
| 16 | 1.1 | 1.0 | upper_wick_pct<=0.0 | - | {"min_slot": "11:00", "max_slot": "12:30"} | 19/0.514 | 17/0.54 | 0.4925 |
| 17 | 1.1 | 1.0 | upper_wick_pct<=0.0 | - | {"min_slot": "11:00", "max_slot": "12:30"} | 19/0.514 | 17/0.54 | 0.4925 |
| 18 | 1.1 | 1.0 | upper_wick_pct<=0.0 | - | {"min_slot": "11:00", "max_slot": "12:30"} | 19/0.514 | 17/0.54 | 0.4925 |
| 19 | 1.1 | 1.0 | upper_wick_pct<=0.0 | - | {"min_slot": "11:00", "max_slot": "12:30"} | 19/0.514 | 17/0.54 | 0.4925 |
| 20 | 1.1 | 1.0 | upper_wick_pct<=0.0 | - | {"min_slot": "11:00", "max_slot": "12:30"} | 19/0.514 | 17/0.54 | 0.4925 |

## Best config — confirmation runs

- **Best (FIT/VAL):** SL 1.1/Tgt 1.0 | mask [ranker_score<=72.064624] | premom [(none)] | guard {'min_slot': '11:00', 'max_slot': '12:30'} | maxpos 20 | dloss 4000.0
- **TRAIN @15bps:** n=41 PF=0.743 net=Rs-3,854 win%=48.8 avgW=Rs557 avgL=Rs-714 maxDD=Rs-5,870 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.16 tradeDom=0.069 dayDom=9.99 symDom=9.99 dbp=0.8099
- **TEST  @15bps:** n=21 PF=0.553 net=Rs-5,483 win%=52.4 avgW=Rs617 avgL=Rs-1,228 maxDD=Rs-8,542 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.62 tradeDom=0.113 dayDom=9.99 symDom=9.99 dbp=0.8973
- **TRAIN @5bps:**  n=41 PF=0.743 net=Rs-3,854 win%=48.8 avgW=Rs557 avgL=Rs-714 maxDD=Rs-5,870 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.16 tradeDom=0.069 dayDom=9.99 symDom=9.99 dbp=0.8099
- **TEST  @5bps:**  n=21 PF=0.553 net=Rs-5,483 win%=52.4 avgW=Rs617 avgL=Rs-1,228 maxDD=Rs-8,542 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.62 tradeDom=0.113 dayDom=9.99 symDom=9.99 dbp=0.8973

## Robustness diagnostics

- neighborhood pass: `False` (PF floor 1.15)
- term-dropout pass: `False` (PF floor 1.0)
- hard reasons: `TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.30; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates)`
- insufficient reasons: `-`
- warnings: `-`

- **Keep/reject:** REJECT  — TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.30; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates)

## Command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup DOC5B_MOMO_BREAKOUT_LONG --pool C:/Users/Saarit/OneDrive/Desktop/Trading/backtesting/eqidv2/backtesting/eqidv2/Train_and_Test/doc5_long_setups/pool --trials 200 --time_budget_min 10.0 --seed 7 --gap_lambda 0.8 --max_mask_terms 1 --max_pm_terms 1 --test_pf_min 1.3 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```