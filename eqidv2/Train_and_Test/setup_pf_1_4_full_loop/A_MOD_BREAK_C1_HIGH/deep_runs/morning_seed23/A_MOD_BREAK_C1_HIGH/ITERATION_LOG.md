# A_MOD_BREAK_C1_HIGH (LONG) — ITERATION_LOG

_Generated 2026-07-03. Optimizer: Optuna TPE. 500 FIT/VAL trials (each trial = one logical config: exit SL/Tgt + <=2 mask + <=2 pre-momentum + guards)._

Search protocol: optimise ONLY on FIT/VAL with the band objective `reward(min(FIT_PF,VAL_PF)) - 0.80*|FIT_PF-VAL_PF|`, where reward tents at PF 1.70 and penalises overshoot (anti-overfit). The single best FIT/VAL config is then confirmed on full TRAIN and scored ONCE on TEST. Quantile thresholds drawn from TRAIN only (never TEST). The robust gate then checks TEST PF+day-block p, TRAIN target-fill, threshold-neighborhood stability, and term-dropout stability.

Exit grid searched: SL [0.5, 0.7, 0.85, 1.0, 1.1, 1.2, 1.5] x Tgt [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5] (small/large SL x small/large target, asymmetric combos). Mask feats: ['atr_pct<=0.003693', 'atr_pct<=0.00511', 'atr_pct>=0.003333', 'atr_pct>=0.00404', 'atr_pct>=0.00511', 'body_pct<=0.794859', 'body_pct>=0.557253', 'body_pct>=0.657744', 'body_pct>=0.794859', 'body_pct>=0.837845', 'close_loc<=0.716429', 'close_loc<=1.0'].

## Top 20 FIT/VAL trials (by band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|-----|------|--------|-------|----------|----------|-------|
| 1 | 1.5 | 2.0 | vol_ratio>=3.283571 | - | {"min_slot": "11:00", "max_slot": "12:30", "top_n": 2} | 10/1.51 | 13/1.768 | 1.3338 |
| 2 | 1.5 | 2.0 | vol_ratio>=3.283571 | - | {"min_slot": "11:00", "max_slot": "12:30", "top_n": 2} | 10/1.51 | 13/1.768 | 1.3338 |
| 3 | 1.5 | 1.25 | vol_ratio>=3.283571;close_loc>=0.716429 | - | {"min_slot": "11:00", "max_slot": "12:30", "top_n": 2} | 9/1.221 | 11/1.233 | 1.2116 |
| 4 | 1.5 | 1.25 | vol_ratio>=3.283571;close_loc>=0.716429 | - | {"min_slot": "11:00", "max_slot": "12:30", "top_n": 2} | 9/1.221 | 11/1.233 | 1.2116 |
| 5 | 1.5 | 1.25 | vol_ratio>=3.283571;close_loc>=0.716429 | - | {"min_slot": "11:00", "max_slot": "12:30", "top_n": 2} | 9/1.221 | 11/1.233 | 1.2116 |
| 6 | 1.5 | 1.25 | vol_ratio>=3.283571;close_loc>=0.716429 | - | {"min_slot": "11:00", "max_slot": "12:30", "top_n": 2} | 9/1.221 | 11/1.233 | 1.2116 |
| 7 | 1.5 | 1.25 | vol_ratio>=3.283571;signal_range_pct>=0.389648 | - | {"min_slot": "11:00", "max_slot": "12:30", "top_n": 2} | 9/1.222 | 11/1.581 | 0.9342 |
| 8 | 1.5 | 1.25 | vol_ratio>=3.283571;signal_range_pct>=0.389648 | - | {"min_slot": "11:00", "max_slot": "12:30", "top_n": 2} | 9/1.222 | 11/1.581 | 0.9342 |
| 9 | 1.5 | 1.25 | vol_ratio>=3.283571;signal_range_pct>=0.389648 | - | {"min_slot": "11:00", "max_slot": "12:30", "top_n": 2} | 9/1.222 | 11/1.581 | 0.9342 |
| 10 | 1.5 | 1.25 | vol_ratio>=3.283571;signal_range_pct>=0.389648 | - | {"min_slot": "11:00", "max_slot": "12:30", "top_n": 2} | 9/1.222 | 11/1.581 | 0.9342 |
| 11 | 1.5 | 1.25 | vol_ratio>=3.283571;signal_range_pct>=0.389648 | - | {"min_slot": "11:00", "max_slot": "12:30", "top_n": 2} | 9/1.222 | 11/1.581 | 0.9342 |
| 12 | 1.5 | 1.25 | vol_ratio>=3.283571;signal_range_pct>=0.389648 | - | {"min_slot": "11:00", "max_slot": "12:30", "top_n": 2} | 9/1.222 | 11/1.581 | 0.9342 |
| 13 | 1.5 | 1.25 | vol_ratio>=3.283571;signal_range_pct>=0.389648 | - | {"min_slot": "11:00", "max_slot": "12:30", "top_n": 2} | 9/1.222 | 11/1.581 | 0.9342 |
| 14 | 1.5 | 1.25 | vol_ratio>=3.283571;signal_range_pct>=0.389648 | - | {"min_slot": "11:00", "max_slot": "12:30", "top_n": 2} | 9/1.222 | 11/1.581 | 0.9342 |
| 15 | 1.5 | 1.25 | vol_ratio>=3.283571;signal_range_pct>=0.389648 | - | {"min_slot": "11:00", "max_slot": "12:30", "top_n": 2} | 9/1.222 | 11/1.581 | 0.9342 |
| 16 | 1.5 | 1.25 | vol_ratio>=3.283571;signal_range_pct>=0.389648 | - | {"min_slot": "11:00", "max_slot": "12:30", "top_n": 2} | 9/1.222 | 11/1.581 | 0.9342 |
| 17 | 1.5 | 1.25 | vol_ratio>=3.283571;signal_range_pct>=0.389648 | - | {"min_slot": "11:00", "max_slot": "12:30", "top_n": 2} | 9/1.222 | 11/1.581 | 0.9342 |
| 18 | 1.5 | 1.25 | vol_ratio>=3.283571;signal_range_pct>=0.389648 | - | {"min_slot": "11:00", "max_slot": "12:30", "top_n": 2} | 9/1.222 | 11/1.581 | 0.9342 |
| 19 | 1.5 | 1.25 | vol_ratio>=3.283571;signal_range_pct>=0.389648 | - | {"min_slot": "11:00", "max_slot": "12:30", "top_n": 2} | 9/1.222 | 11/1.581 | 0.9342 |
| 20 | 1.5 | 1.25 | vol_ratio>=3.283571;signal_range_pct>=0.389648 | - | {"min_slot": "11:00", "max_slot": "12:30", "top_n": 2} | 9/1.222 | 11/1.581 | 0.9342 |

## Best config — confirmation runs

- **Best (FIT/VAL):** SL 1.5/Tgt 2.0 | mask [vol_ratio>=3.283571] | premom [(none)] | guard {'min_slot': '11:00', 'max_slot': '12:30', 'top_n': 2} | maxpos 20 | dloss 0.0
- **TRAIN @15bps:** n=23 PF=1.66 net=Rs8,474 win%=60.9 avgW=Rs1,522 avgL=Rs-1,426 maxDD=Rs-3,453 SL/TGT/EOD=5/11/7 tgt%=47.8 tpd=1.28 tradeDom=0.083 dayDom=0.304 symDom=0.208 dbp=0.0851
- **TEST  @15bps:** n=17 PF=0.277 net=Rs-15,311 win%=23.5 avgW=Rs1,469 avgL=Rs-1,630 maxDD=Rs-15,989 SL/TGT/EOD=11/2/4 tgt%=11.8 tpd=1.55 tradeDom=0.301 dayDom=9.99 symDom=9.99 dbp=0.9901
- **TRAIN @5bps:**  n=23 PF=1.903 net=Rs10,784 win%=60.9 avgW=Rs1,624 avgL=Rs-1,327 maxDD=Rs-3,256 SL/TGT/EOD=5/11/7 tgt%=47.8 tpd=1.28 tradeDom=0.082 dayDom=0.257 symDom=0.173 dbp=0.0411
- **TEST  @5bps:**  n=17 PF=0.316 net=Rs-13,626 win%=23.5 avgW=Rs1,571 avgL=Rs-1,531 maxDD=Rs-14,805 SL/TGT/EOD=11/2/4 tgt%=11.8 tpd=1.55 tradeDom=0.297 dayDom=9.99 symDom=9.99 dbp=0.9837

## Robustness diagnostics

- neighborhood pass: `False` (PF floor 1.15)
- term-dropout pass: `False` (PF floor 1.0)
- hard reasons: `neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates)`
- insufficient reasons: `-`
- warnings: `-`

- **Keep/reject:** REJECT  — neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates)

## Command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup A_MOD_BREAK_C1_HIGH --pool Train_and_Test/setup_pf_1_4_full_loop/A_MOD_BREAK_C1_HIGH/pools/pool_morning --trials 500 --time_budget_min 12.0 --seed 23 --gap_lambda 0.8 --max_mask_terms 2 --max_pm_terms 2 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```