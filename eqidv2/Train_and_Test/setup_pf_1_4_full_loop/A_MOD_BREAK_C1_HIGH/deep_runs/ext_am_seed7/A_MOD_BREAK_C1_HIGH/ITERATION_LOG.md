# A_MOD_BREAK_C1_HIGH (LONG) — ITERATION_LOG

_Generated 2026-07-03. Optimizer: Optuna TPE. 600 FIT/VAL trials (each trial = one logical config: exit SL/Tgt + <=2 mask + <=2 pre-momentum + guards)._

Search protocol: optimise ONLY on FIT/VAL with the band objective `reward(min(FIT_PF,VAL_PF)) - 0.80*|FIT_PF-VAL_PF|`, where reward tents at PF 1.70 and penalises overshoot (anti-overfit). The single best FIT/VAL config is then confirmed on full TRAIN and scored ONCE on TEST. Quantile thresholds drawn from TRAIN only (never TEST). The robust gate then checks TEST PF+day-block p, TRAIN target-fill, threshold-neighborhood stability, and term-dropout stability.

Exit grid searched: SL [0.5, 0.7, 0.85, 1.0, 1.1, 1.2, 1.5] x Tgt [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5] (small/large SL x small/large target, asymmetric combos). Mask feats: ['above_or_high<=1.0', 'above_or_high>=1.0', 'above_pdh<=0.0', 'above_pdh>=0.0', 'above_pdh>=1.0', 'adx_x>=23.379833', 'adx_x>=31.680925', 'adx_x>=35.203762', 'adx_x>=43.734341', 'adx_x>=48.602838', 'atr_pct<=0.003665', 'atr_pct>=0.002492'].

## Top 20 FIT/VAL trials (by band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|-----|------|--------|-------|----------|----------|-------|
| 1 | 0.5 | 2.0 | vol_ratio>=3.244737;wick_skew_pct>=0.005197 | pre_entry_momentum_score>=63.297377 | {"min_slot": "10:00", "max_slot": "13:00", "top_n": 3} | 7/0.967 | 14/0.968 | 0.9663 |
| 2 | 0.5 | 2.0 | vol_ratio>=3.244737 | pre_entry_momentum_score>=70.80124 | {"min_slot": "10:00", "max_slot": "11:05", "top_n": 3} | 14/1.0 | 14/0.969 | 0.9435 |
| 3 | 0.7 | 2.0 | vol_ratio>=3.244737 | pre_entry_momentum_score>=70.80124 | {"min_slot": "10:00", "max_slot": "11:05", "top_n": 3} | 13/1.28 | 11/1.091 | 0.9393 |
| 4 | 0.7 | 2.0 | vol_ratio>=3.244737 | pre_entry_momentum_score>=70.80124 | {"min_slot": "09:45", "max_slot": "11:05", "top_n": 3} | 13/1.28 | 11/1.091 | 0.9393 |
| 5 | 0.7 | 2.0 | vol_ratio>=3.244737 | pre_entry_momentum_score>=70.80124 | {"min_slot": "09:45", "max_slot": "11:05", "top_n": 3} | 13/1.28 | 11/1.091 | 0.9393 |
| 6 | 0.7 | 2.0 | vol_ratio>=3.244737 | pre_entry_momentum_score>=70.80124 | {"min_slot": "09:45", "max_slot": "11:05", "top_n": 3} | 13/1.28 | 11/1.091 | 0.9393 |
| 7 | 0.7 | 2.0 | vol_ratio>=3.244737 | pre_entry_momentum_score>=70.80124 | {"min_slot": "09:45", "max_slot": "11:05", "top_n": 3} | 13/1.28 | 11/1.091 | 0.9393 |
| 8 | 0.7 | 2.0 | vol_ratio>=3.244737 | pre_entry_momentum_score>=70.80124 | {"min_slot": "09:45", "max_slot": "11:05", "top_n": 3} | 13/1.28 | 11/1.091 | 0.9393 |
| 9 | 0.7 | 2.0 | vol_ratio>=3.244737 | pre_entry_momentum_score>=70.80124 | {"min_slot": "10:00", "max_slot": "11:05", "top_n": 3} | 13/1.28 | 11/1.091 | 0.9393 |
| 10 | 0.7 | 2.0 | vol_ratio>=3.244737 | pre_entry_momentum_score>=70.80124 | {"min_slot": "09:45", "max_slot": "11:05", "top_n": 3} | 13/1.28 | 11/1.091 | 0.9393 |
| 11 | 0.5 | 2.0 | vol_ratio>=3.244737 | pre_entry_momentum_score>=74.186759 | {"min_slot": "09:45", "max_slot": "12:00", "top_n": 3} | 13/1.124 | 11/1.388 | 0.9125 |
| 12 | 0.5 | 2.0 | vol_ratio>=3.244737 | pre_entry_momentum_score>=74.186759 | {"min_slot": "09:45", "max_slot": "12:00", "top_n": 3} | 13/1.124 | 11/1.388 | 0.9125 |
| 13 | 0.5 | 2.0 | vol_ratio>=3.244737 | pre_entry_momentum_score>=74.186759 | {"min_slot": "09:45", "max_slot": "12:00", "top_n": 3} | 13/1.124 | 11/1.388 | 0.9125 |
| 14 | 0.7 | 1.25 | vol_ratio>=3.244737 | pre_entry_momentum_score>=70.80124 | {"min_slot": "09:45", "max_slot": "11:05", "top_n": 3} | 13/0.934 | 11/0.917 | 0.9037 |
| 15 | 0.7 | 1.25 | vol_ratio>=3.244737 | pre_entry_momentum_score>=70.80124 | {"min_slot": "09:45", "max_slot": "11:05", "top_n": 3} | 13/0.934 | 11/0.917 | 0.9037 |
| 16 | 0.7 | 1.25 | vol_ratio>=3.244737 | pre_entry_momentum_score>=70.80124 | {"min_slot": "09:45", "max_slot": "11:05", "top_n": 3} | 13/0.934 | 11/0.917 | 0.9037 |
| 17 | 0.7 | 1.25 | vol_ratio>=3.244737 | pre_entry_momentum_score>=70.80124 | {"min_slot": "09:45", "max_slot": "11:05", "top_n": 3} | 13/0.934 | 11/0.917 | 0.9037 |
| 18 | 0.7 | 1.25 | vol_ratio>=3.244737 | pre_entry_momentum_score>=70.80124 | {"min_slot": "09:45", "max_slot": "11:05", "top_n": 3} | 13/0.934 | 11/0.917 | 0.9037 |
| 19 | 0.7 | 1.25 | vol_ratio>=3.244737 | pre_entry_momentum_score>=70.80124 | {"min_slot": "09:45", "max_slot": "11:05", "top_n": 3} | 13/0.934 | 11/0.917 | 0.9037 |
| 20 | 0.7 | 1.25 | vol_ratio>=3.244737 | pre_entry_momentum_score>=70.80124 | {"min_slot": "09:45", "max_slot": "11:05", "top_n": 3} | 13/0.934 | 11/0.917 | 0.9037 |

## Best config — confirmation runs

- **Best (FIT/VAL):** SL 0.5/Tgt 2.0 | mask [vol_ratio>=3.244737; wick_skew_pct>=0.005197] | premom [pre_entry_momentum_score>=63.297377] | guard {'min_slot': '10:00', 'max_slot': '13:00', 'top_n': 3} | maxpos 20 | dloss 0.0
- **TRAIN @15bps:** n=21 PF=0.968 net=Rs-351 win%=28.6 avgW=Rs1,762 avgL=Rs-728 maxDD=Rs-7,272 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.5 tradeDom=0.167 dayDom=9.99 symDom=9.99 dbp=0.5336
- **TEST  @15bps:** n=12 PF=0.0 net=Rs-8,746 win%=0.0 avgW=Rs0 avgL=Rs-729 maxDD=Rs-8,019 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.5 tradeDom=9.99 dayDom=9.99 symDom=9.99 dbp=1.0
- **TRAIN @5bps:**  n=21 PF=0.968 net=Rs-351 win%=28.6 avgW=Rs1,762 avgL=Rs-728 maxDD=Rs-7,272 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.5 tradeDom=0.167 dayDom=9.99 symDom=9.99 dbp=0.5336
- **TEST  @5bps:**  n=12 PF=0.0 net=Rs-8,746 win%=0.0 avgW=Rs0 avgL=Rs-729 maxDD=Rs-8,019 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.5 tradeDom=9.99 dayDom=9.99 symDom=9.99 dbp=1.0

## Robustness diagnostics

- neighborhood pass: `False` (PF floor 1.15)
- term-dropout pass: `False` (PF floor 1.0)
- hard reasons: `TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates)`
- insufficient reasons: `-`
- warnings: `-`

- **Keep/reject:** REJECT  — TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates)

## Command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup A_MOD_BREAK_C1_HIGH --pool Train_and_Test/setup_pf_1_4_full_loop/A_MOD_BREAK_C1_HIGH/pools/pool_enriched_first_am --trials 600 --time_budget_min 12.0 --seed 7 --gap_lambda 0.8 --max_mask_terms 2 --max_pm_terms 2 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```