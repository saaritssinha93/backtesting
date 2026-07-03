# G_LOWER_LOW_BREAK (SHORT) — ITERATION_LOG

_Generated 2026-07-01. Optimizer: Optuna TPE. 700 FIT/VAL trials (each trial = one logical config: exit SL/Tgt + <=2 mask + <=2 pre-momentum + guards)._

Search protocol: optimise ONLY on FIT/VAL with the band objective `reward(min(FIT_PF,VAL_PF)) - 0.80*|FIT_PF-VAL_PF|`, where reward tents at PF 1.70 and penalises overshoot (anti-overfit). The single best FIT/VAL config is then confirmed on full TRAIN and scored ONCE on TEST. Quantile thresholds drawn from TRAIN only (never TEST). The robust gate then checks TEST PF+day-block p, TRAIN target-fill, threshold-neighborhood stability, and term-dropout stability.

Exit grid searched: SL [0.5, 0.7, 0.85, 1.0, 1.1, 1.2, 1.5] x Tgt [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5] (small/large SL x small/large target, asymmetric combos). Mask feats: ['atr_pct<=0.001461', 'atr_pct>=0.001637', 'atr_pct>=0.002145', 'atr_pct>=0.00345', 'body_pct<=0.781911', 'body_pct<=0.850556', 'body_pct<=0.906818', 'body_pct>=0.638771', 'body_pct>=0.68', 'body_pct>=0.719276', 'body_pct>=0.781911', 'body_pct>=0.850556'].

## Top 20 FIT/VAL trials (by band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|-----|------|--------|-------|----------|----------|-------|
| 1 | 1.0 | 1.0 | - | pre_entry_momentum_score<=46.220727 | {"top_n": 3} | 43/1.075 | 7/1.089 | 1.0638 |
| 2 | 1.0 | 1.0 | - | pre_entry_momentum_score<=46.220727 | {"top_n": 3} | 43/1.075 | 7/1.089 | 1.0638 |
| 3 | 1.0 | 1.0 | - | pre_entry_momentum_score<=46.220727 | {"top_n": 3} | 43/1.075 | 7/1.089 | 1.0638 |
| 4 | 1.0 | 1.0 | - | pre_entry_momentum_score<=46.220727 | {"top_n": 3} | 43/1.075 | 7/1.089 | 1.0638 |
| 5 | 1.0 | 1.0 | - | pre_entry_momentum_score<=46.220727 | {"top_n": 3} | 43/1.075 | 7/1.089 | 1.0638 |
| 6 | 1.0 | 1.0 | - | pre_entry_momentum_score<=46.220727 | {"top_n": 3} | 43/1.075 | 7/1.089 | 1.0638 |
| 7 | 1.0 | 1.5 | - | pre_entry_momentum_score<=46.220727 | {"min_slot": "09:30", "top_n": 3} | 39/1.157 | 7/1.297 | 1.046 |
| 8 | 1.0 | 1.5 | - | pre_entry_momentum_score<=46.220727 | {"min_slot": "09:30", "top_n": 3} | 39/1.157 | 7/1.297 | 1.046 |
| 9 | 1.0 | 1.5 | - | pre_entry_momentum_score<=46.220727 | {"min_slot": "09:30", "top_n": 3} | 39/1.157 | 7/1.297 | 1.046 |
| 10 | 1.0 | 1.5 | - | pre_entry_momentum_score<=46.220727 | {"min_slot": "09:30", "top_n": 3} | 39/1.157 | 7/1.297 | 1.046 |
| 11 | 1.0 | 1.5 | - | pre_entry_momentum_score<=46.220727 | {"min_slot": "09:30", "top_n": 3} | 39/1.157 | 7/1.297 | 1.046 |
| 12 | 1.0 | 1.5 | - | pre_entry_momentum_score<=46.220727 | {"min_slot": "09:30", "top_n": 3} | 39/1.157 | 7/1.297 | 1.046 |
| 13 | 1.0 | 1.5 | - | pre_entry_momentum_score<=46.220727 | {"min_slot": "10:00", "top_n": 3} | 39/1.157 | 7/1.297 | 1.046 |
| 14 | 1.0 | 1.0 | - | pre_entry_momentum_score<=46.220727 | {"min_slot": "09:30", "top_n": 3} | 40/1.196 | 7/1.089 | 1.0028 |
| 15 | 1.0 | 1.0 | - | pre_entry_momentum_score<=46.220727 | {"min_slot": "09:30", "top_n": 3} | 40/1.196 | 7/1.089 | 1.0028 |
| 16 | 1.0 | 1.0 | - | pre_entry_momentum_score<=46.220727 | {"min_slot": "09:30", "top_n": 3} | 40/1.196 | 7/1.089 | 1.0028 |
| 17 | 1.0 | 1.0 | - | pre_entry_momentum_score<=46.220727 | {"top_n": 3} | 40/1.196 | 7/1.089 | 1.0028 |
| 18 | 1.0 | 1.0 | - | pre_entry_momentum_score<=46.220727 | {"top_n": 3} | 40/1.196 | 7/1.089 | 1.0028 |
| 19 | 1.0 | 1.0 | - | pre_entry_momentum_score<=46.220727 | {"min_slot": "10:00", "top_n": 3} | 40/1.196 | 7/1.089 | 1.0028 |
| 20 | 1.0 | 1.0 | - | pre_entry_momentum_score<=46.220727 | {"min_slot": "10:00", "top_n": 3} | 40/1.196 | 7/1.089 | 1.0028 |

## Best config — confirmation runs

- **Best (FIT/VAL):** SL 1.0/Tgt 1.0 | mask [(none)] | premom [pre_entry_momentum_score<=46.220727] | guard {'top_n': 3} | maxpos 20 | dloss 4000.0
- **TRAIN @15bps:** n=50 PF=1.077 net=Rs1,027 win%=56.0 avgW=Rs512 avgL=Rs-605 maxDD=Rs-4,253 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=6.25 tradeDom=0.054 dayDom=2.647 symDom=0.747 dbp=0.4037
- **TEST  @15bps:** n=0 PF=0.0 net=Rs0 win%=0.0 avgW=Rs0 avgL=Rs0 maxDD=Rs0 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=0.0 tradeDom=9.99 dayDom=9.99 symDom=9.99 dbp=None
- **TRAIN @5bps:**  n=50 PF=1.077 net=Rs1,027 win%=56.0 avgW=Rs512 avgL=Rs-605 maxDD=Rs-4,253 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=6.25 tradeDom=0.054 dayDom=2.647 symDom=0.747 dbp=0.4037
- **TEST  @5bps:**  n=0 PF=0.0 net=Rs0 win%=0.0 avgW=Rs0 avgL=Rs0 maxDD=Rs0 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=0.0 tradeDom=9.99 dayDom=9.99 symDom=9.99 dbp=None

## Robustness diagnostics

- neighborhood pass: `False` (PF floor 1.15)
- term-dropout pass: `False` (PF floor 1.0)
- hard reasons: `TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); TRAIN too many trades/day; neighborhood robustness failed; term-dropout robustness failed`
- insufficient reasons: `TEST too few trades (test_n<6)`
- warnings: `-`

- **Keep/reject:** REJECT  — TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); TRAIN too many trades/day; neighborhood robustness failed; term-dropout robustness failed; TEST too few trades (test_n<6)

## Command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup G_LOWER_LOW_BREAK --pool C:\TradingData\eqidv2\setup_pools_2026_06_29\G_LOWER_LOW_BREAK --trials 700 --time_budget_min 10.0 --seed 59 --gap_lambda 0.8 --max_mask_terms 2 --max_pm_terms 2 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```