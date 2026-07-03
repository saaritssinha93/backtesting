# DOC5B_MOMO_BREAKOUT_LONG (LONG) — ITERATION_LOG

_Generated 2026-07-01. Optimizer: Optuna TPE. 700 FIT/VAL trials (each trial = one logical config: exit SL/Tgt + <=2 mask + <=2 pre-momentum + guards)._

Search protocol: optimise ONLY on FIT/VAL with the band objective `reward(min(FIT_PF,VAL_PF)) - 0.80*|FIT_PF-VAL_PF|`, where reward tents at PF 1.70 and penalises overshoot (anti-overfit). The single best FIT/VAL config is then confirmed on full TRAIN and scored ONCE on TEST. Quantile thresholds drawn from TRAIN only (never TEST). The robust gate then checks TEST PF+day-block p, TRAIN target-fill, threshold-neighborhood stability, and term-dropout stability.

Exit grid searched: SL [0.5, 0.7, 0.85, 1.0, 1.1, 1.2, 1.5] x Tgt [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5] (small/large SL x small/large target, asymmetric combos). Mask feats: ['atr_pct<=0.001988', 'atr_pct<=0.003038', 'atr_pct<=0.003301', 'atr_pct<=0.004311', 'atr_pct>=0.002366', 'atr_pct>=0.002778', 'atr_pct>=0.003038', 'atr_pct>=0.003301', 'atr_pct>=0.003968', 'atr_pct>=0.006225', 'body_pct<=0.766417', 'body_pct>=0.466912'].

## Top 20 FIT/VAL trials (by band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|-----|------|--------|-------|----------|----------|-------|
| 1 | 1.5 | 0.8 | retest_depth_atr>=0.350946 | sig5_adx_calc>=25.422361 | {"min_slot": "10:00", "max_slot": "12:30"} | 6/2.057 | 6/1.647 | 1.3366 |
| 2 | 1.5 | 0.8 | retest_depth_atr>=0.350946 | sig5_adx_calc>=25.422361 | {"min_slot": "10:00", "max_slot": "12:30"} | 6/2.057 | 6/1.647 | 1.3366 |
| 3 | 1.5 | 0.8 | retest_depth_atr>=0.350946 | sig5_adx_calc>=25.422361 | {"min_slot": "10:00", "max_slot": "12:30"} | 6/2.057 | 6/1.647 | 1.3366 |
| 4 | 1.5 | 0.8 | retest_depth_atr>=0.350946 | sig5_adx_calc>=25.422361 | {"min_slot": "10:00", "max_slot": "12:00"} | 6/2.057 | 6/1.647 | 1.3366 |
| 5 | 1.5 | 0.8 | retest_depth_atr>=0.350946 | sig5_adx_calc>=25.422361 | {"min_slot": "10:00", "max_slot": "12:30"} | 6/2.057 | 6/1.647 | 1.3366 |
| 6 | 1.5 | 0.8 | retest_depth_atr>=0.350946 | sig5_adx_calc>=25.422361 | {"min_slot": "10:00", "max_slot": "12:30"} | 6/2.057 | 6/1.647 | 1.3366 |
| 7 | 1.5 | 0.8 | retest_depth_atr>=0.350946 | sig5_adx_calc>=25.422361 | {"min_slot": "10:00", "max_slot": "12:30"} | 6/2.057 | 6/1.647 | 1.3366 |
| 8 | 1.5 | 0.8 | retest_depth_atr>=0.350946 | sig5_adx_calc>=25.422361 | {"min_slot": "10:00", "max_slot": "12:30"} | 6/2.057 | 6/1.647 | 1.3366 |
| 9 | 1.5 | 0.8 | retest_depth_atr>=0.350946 | sig5_adx_calc>=25.422361 | {"min_slot": "10:00", "max_slot": "12:30"} | 6/2.057 | 6/1.647 | 1.3366 |
| 10 | 1.5 | 0.8 | retest_depth_atr>=0.350946 | sig5_adx_calc>=25.422361 | {"min_slot": "10:00", "max_slot": "12:30"} | 6/2.057 | 6/1.647 | 1.3366 |
| 11 | 1.5 | 0.8 | retest_depth_atr>=0.350946 | sig5_adx_calc>=25.422361 | {"min_slot": "10:00", "max_slot": "12:30"} | 6/2.057 | 6/1.647 | 1.3366 |
| 12 | 1.5 | 0.8 | retest_depth_atr>=0.350946 | sig5_adx_calc>=25.422361 | {"min_slot": "10:00", "max_slot": "12:30"} | 6/2.057 | 6/1.647 | 1.3366 |
| 13 | 1.5 | 0.8 | retest_depth_atr>=0.350946 | sig5_adx_calc>=25.422361 | {"min_slot": "10:00", "max_slot": "12:30"} | 6/2.057 | 6/1.647 | 1.3366 |
| 14 | 1.5 | 0.8 | retest_depth_atr>=0.350946 | sig5_adx_calc>=25.422361 | {"min_slot": "10:00", "max_slot": "12:30"} | 6/2.057 | 6/1.647 | 1.3366 |
| 15 | 1.5 | 0.8 | retest_depth_atr>=0.350946 | sig5_adx_calc>=25.422361 | {"min_slot": "10:00", "max_slot": "12:30"} | 6/2.057 | 6/1.647 | 1.3366 |
| 16 | 1.5 | 0.8 | retest_depth_atr>=0.350946 | sig5_adx_calc>=25.422361 | {"min_slot": "10:00", "max_slot": "12:30"} | 6/2.057 | 6/1.647 | 1.3366 |
| 17 | 1.5 | 0.8 | retest_depth_atr>=0.350946 | sig5_adx_calc>=21.266639 | {"min_slot": "10:00", "max_slot": "12:30"} | 6/2.057 | 6/1.647 | 1.3366 |
| 18 | 1.5 | 0.8 | retest_depth_atr>=0.350946 | sig5_adx_calc>=25.422361 | {"min_slot": "10:00", "max_slot": "12:30"} | 6/2.057 | 6/1.647 | 1.3366 |
| 19 | 1.5 | 0.8 | retest_depth_atr>=0.350946 | sig5_adx_calc>=25.422361 | {"min_slot": "10:00", "max_slot": "12:30"} | 6/2.057 | 6/1.647 | 1.3366 |
| 20 | 1.5 | 0.8 | retest_depth_atr>=0.350946 | sig5_adx_calc>=25.422361 | {"min_slot": "10:00", "max_slot": "12:30"} | 6/2.057 | 6/1.647 | 1.3366 |

## Best config — confirmation runs

- **Best (FIT/VAL):** SL 1.5/Tgt 0.8 | mask [retest_depth_atr>=0.350946] | premom [sig5_adx_calc>=21.266639] | guard {'min_slot': '10:00', 'max_slot': '12:30'} | maxpos 20 | dloss 0.0
- **TRAIN @15bps:** n=12 PF=1.816 net=Rs2,384 win%=83.3 avgW=Rs531 avgL=Rs-1,461 maxDD=Rs-1,719 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.5 tradeDom=0.107 dayDom=0.713 symDom=0.238 dbp=0.1455
- **TEST  @15bps:** n=4 PF=0.0 net=Rs-5,857 win%=0.0 avgW=Rs0 avgL=Rs-1,464 maxDD=Rs-4,142 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.0 tradeDom=9.99 dayDom=9.99 symDom=9.99 dbp=None
- **TRAIN @5bps:**  n=12 PF=1.816 net=Rs2,384 win%=83.3 avgW=Rs531 avgL=Rs-1,461 maxDD=Rs-1,719 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.5 tradeDom=0.107 dayDom=0.713 symDom=0.238 dbp=0.1455
- **TEST  @5bps:**  n=4 PF=0.0 net=Rs-5,857 win%=0.0 avgW=Rs0 avgL=Rs-1,464 maxDD=Rs-4,142 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.0 tradeDom=9.99 dayDom=9.99 symDom=9.99 dbp=None

## Robustness diagnostics

- neighborhood pass: `False` (PF floor 1.15)
- term-dropout pass: `False` (PF floor 1.0)
- hard reasons: `TRAIN too few trades (train_n<20); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed`
- insufficient reasons: `TEST too few trades (test_n<6)`
- warnings: `TRAIN PF above preferred band (>1.70)`

- **Keep/reject:** REJECT  — TRAIN too few trades (train_n<20); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST too few trades (test_n<6); TRAIN PF above preferred band (>1.70)

## Command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup DOC5B_MOMO_BREAKOUT_LONG --pool Train_and_Test\doc5_long_setups\pool_retest_v3 --trials 700 --time_budget_min 10.0 --seed 23 --gap_lambda 0.8 --max_mask_terms 2 --max_pm_terms 2 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```