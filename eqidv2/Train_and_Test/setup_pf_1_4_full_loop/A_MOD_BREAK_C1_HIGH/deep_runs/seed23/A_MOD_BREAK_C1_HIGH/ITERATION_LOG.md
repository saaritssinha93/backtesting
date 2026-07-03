# A_MOD_BREAK_C1_HIGH (LONG) — ITERATION_LOG

_Generated 2026-07-02. Optimizer: Optuna TPE. 3 FIT/VAL trials (each trial = one logical config: exit SL/Tgt + <=2 mask + <=2 pre-momentum + guards)._

Search protocol: optimise ONLY on FIT/VAL with the band objective `reward(min(FIT_PF,VAL_PF)) - 0.80*|FIT_PF-VAL_PF|`, where reward tents at PF 1.70 and penalises overshoot (anti-overfit). The single best FIT/VAL config is then confirmed on full TRAIN and scored ONCE on TEST. Quantile thresholds drawn from TRAIN only (never TEST). The robust gate then checks TEST PF+day-block p, TRAIN target-fill, threshold-neighborhood stability, and term-dropout stability.

Exit grid searched: SL [0.5, 0.7, 0.85, 1.0, 1.1, 1.2, 1.5] x Tgt [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5] (small/large SL x small/large target, asymmetric combos). Mask feats: ['lower_wick_pct>=0.0', 'lower_wick_pct>=0.03585', 'rs_pct>=0.767875'].

## Top 20 FIT/VAL trials (by band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|-----|------|--------|-------|----------|----------|-------|
| 1 | 0.85 | 0.6 | lower_wick_pct>=0.03585;quality_score>=61.927397 | - | {"min_slot": "09:30", "top_n": 2} | 341/0.245 | 380/0.249 | 0.2421 |
| 2 | 1.1 | 0.8 | lower_wick_pct>=0.0;body_pct>=0.781247 | pre1_adx<=31.872126 | - | 495/0.358 | 546/0.272 | 0.2039 |
| 3 | 1.1 | 1.0 | rs_pct>=0.767875 | - | - | 426/0.245 | 402/0.208 | 0.1777 |

## Best config — confirmation runs

- **Best (FIT/VAL):** SL 0.85/Tgt 0.6 | mask [lower_wick_pct>=0.03585; quality_score>=61.927397] | premom [(none)] | guard {'min_slot': '09:30', 'top_n': 2} | maxpos 10 | dloss 4000.0
- **TRAIN @15bps:** n=721 PF=0.247 net=Rs-315,347 win%=40.4 avgW=Rs356 avgL=Rs-974 maxDD=Rs-314,457 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=13.87 tradeDom=0.004 dayDom=9.99 symDom=9.99 dbp=1.0
- **TEST  @15bps:** n=219 PF=0.149 net=Rs-132,093 win%=29.7 avgW=Rs356 avgL=Rs-1,008 maxDD=Rs-132,459 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=9.95 tradeDom=0.016 dayDom=9.99 symDom=9.99 dbp=1.0
- **TRAIN @5bps:**  n=721 PF=0.247 net=Rs-315,347 win%=40.4 avgW=Rs356 avgL=Rs-974 maxDD=Rs-314,457 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=13.87 tradeDom=0.004 dayDom=9.99 symDom=9.99 dbp=1.0
- **TEST  @5bps:**  n=219 PF=0.149 net=Rs-132,093 win%=29.7 avgW=Rs356 avgL=Rs-1,008 maxDD=Rs-132,459 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=9.95 tradeDom=0.016 dayDom=9.99 symDom=9.99 dbp=1.0

## Robustness diagnostics

- neighborhood pass: `False` (PF floor 1.15)
- term-dropout pass: `False` (PF floor 1.0)
- hard reasons: `TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); TRAIN too many trades/day; neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates); TEST too many trades/day`
- insufficient reasons: `-`
- warnings: `-`

- **Keep/reject:** REJECT  — TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); TRAIN too many trades/day; neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates); TEST too many trades/day

## Command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup A_MOD_BREAK_C1_HIGH --pool Train_and_Test/setup_pf_1_4_full_loop/A_MOD_BREAK_C1_HIGH/pools/pool_full --trials 700 --time_budget_min 12.0 --seed 23 --gap_lambda 0.8 --max_mask_terms 2 --max_pm_terms 2 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```