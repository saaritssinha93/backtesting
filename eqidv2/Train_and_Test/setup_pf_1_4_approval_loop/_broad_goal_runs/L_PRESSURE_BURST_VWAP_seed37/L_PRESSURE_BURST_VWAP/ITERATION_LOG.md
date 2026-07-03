# L_PRESSURE_BURST_VWAP (LONG) — ITERATION_LOG

_Generated 2026-07-01. Optimizer: Optuna TPE. 700 FIT/VAL trials (each trial = one logical config: exit SL/Tgt + <=2 mask + <=2 pre-momentum + guards)._

Search protocol: optimise ONLY on FIT/VAL with the band objective `reward(min(FIT_PF,VAL_PF)) - 0.80*|FIT_PF-VAL_PF|`, where reward tents at PF 1.70 and penalises overshoot (anti-overfit). The single best FIT/VAL config is then confirmed on full TRAIN and scored ONCE on TEST. Quantile thresholds drawn from TRAIN only (never TEST). The robust gate then checks TEST PF+day-block p, TRAIN target-fill, threshold-neighborhood stability, and term-dropout stability.

Exit grid searched: SL [0.5, 0.7, 0.85, 1.0, 1.1, 1.2, 1.5] x Tgt [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5] (small/large SL x small/large target, asymmetric combos). Mask feats: ['atr_pct>=0.001727', 'atr_pct>=0.00202', 'atr_pct>=0.002534', 'body_pct>=0.521157', 'body_pct>=0.560423', 'body_pct>=0.662486', 'close_loc<=0.14824', 'close_loc<=0.765548', 'close_loc>=0.464646', 'lower_wick_pct<=0.0', 'lower_wick_pct<=0.03916', 'lower_wick_pct<=0.140333'].

## Top 20 FIT/VAL trials (by band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|-----|------|--------|-------|----------|----------|-------|
| 1 | 0.85 | 2.0 | - | pre3_range_r>=0.326861;sig5_adx_calc<=19.549259 | {"top_n": 2} | 45/0.532 | 58/0.525 | 0.5192 |
| 2 | 0.85 | 2.0 | - | pre3_range_r>=0.326861;sig5_adx_calc<=19.549259 | {"top_n": 2} | 45/0.532 | 58/0.525 | 0.5192 |
| 3 | 0.85 | 2.0 | - | pre3_range_r>=0.326861;sig5_adx_calc<=19.549259 | {"top_n": 2} | 45/0.532 | 58/0.525 | 0.5192 |
| 4 | 0.85 | 2.0 | - | pre3_range_r>=0.326861;sig5_adx_calc<=19.549259 | {"min_slot": "10:00", "top_n": 2} | 45/0.532 | 58/0.525 | 0.5192 |
| 5 | 0.85 | 2.0 | - | pre3_range_r>=0.326861;sig5_adx_calc<=19.549259 | {"top_n": 2} | 45/0.532 | 58/0.525 | 0.5192 |
| 6 | 0.85 | 2.0 | - | pre3_range_r>=0.326861;sig5_adx_calc<=19.549259 | {"top_n": 2} | 45/0.532 | 58/0.525 | 0.5192 |
| 7 | 0.85 | 2.0 | - | pre3_range_r>=0.326861;sig5_adx_calc<=19.549259 | {"top_n": 2} | 45/0.532 | 58/0.525 | 0.5192 |
| 8 | 0.85 | 2.0 | - | pre3_range_r>=0.326861;sig5_adx_calc<=19.549259 | {"top_n": 2} | 45/0.532 | 58/0.525 | 0.5192 |
| 9 | 0.85 | 2.0 | - | pre3_range_r>=0.326861;sig5_adx_calc<=19.549259 | {"top_n": 2} | 45/0.532 | 58/0.525 | 0.5192 |
| 10 | 0.85 | 2.0 | - | pre3_range_r>=0.326861;sig5_adx_calc<=19.549259 | {"min_slot": "10:00", "top_n": 2} | 45/0.532 | 58/0.525 | 0.5192 |
| 11 | 0.85 | 2.0 | - | pre3_range_r>=0.326861;sig5_adx_calc<=19.549259 | {"top_n": 2} | 45/0.532 | 58/0.525 | 0.5192 |
| 12 | 0.85 | 2.0 | - | pre3_range_r>=0.326861;sig5_adx_calc<=19.549259 | {"top_n": 2} | 45/0.532 | 58/0.525 | 0.5192 |
| 13 | 0.85 | 2.0 | - | pre3_range_r>=0.326861;sig5_adx_calc<=19.549259 | {"min_slot": "10:00", "top_n": 2} | 45/0.532 | 58/0.525 | 0.5192 |
| 14 | 0.85 | 2.0 | - | pre3_range_r>=0.326861;sig5_adx_calc<=19.549259 | {"top_n": 2} | 45/0.532 | 58/0.525 | 0.5192 |
| 15 | 0.85 | 2.0 | - | pre3_range_r>=0.326861;sig5_adx_calc<=19.549259 | {"top_n": 2} | 45/0.532 | 58/0.525 | 0.5192 |
| 16 | 0.85 | 2.0 | - | pre3_range_r>=0.326861;sig5_adx_calc<=19.549259 | {"top_n": 2} | 45/0.532 | 58/0.525 | 0.5192 |
| 17 | 0.85 | 2.0 | - | pre3_range_r>=0.326861;sig5_adx_calc<=19.549259 | {"top_n": 2} | 45/0.532 | 58/0.525 | 0.5192 |
| 18 | 0.85 | 2.0 | - | pre3_range_r>=0.326861;sig5_adx_calc<=19.549259 | {"top_n": 2} | 45/0.532 | 58/0.525 | 0.5192 |
| 19 | 0.85 | 2.0 | - | pre3_range_r>=0.326861;sig5_adx_calc<=19.549259 | {"min_slot": "10:00", "top_n": 2} | 45/0.532 | 58/0.525 | 0.5192 |
| 20 | 0.85 | 2.0 | - | pre3_range_r>=0.326861;sig5_adx_calc<=19.549259 | {"top_n": 2} | 45/0.532 | 58/0.525 | 0.5192 |

## Best config — confirmation runs

- **Best (FIT/VAL):** SL 0.85/Tgt 2.0 | mask [(none)] | premom [pre3_range_r>=0.326861; sig5_adx_calc<=19.549259] | guard {'top_n': 2} | maxpos 20 | dloss 4000.0
- **TRAIN @15bps:** n=103 PF=0.528 net=Rs-28,832 win%=33.0 avgW=Rs947 avgL=Rs-884 maxDD=Rs-31,791 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=5.72 tradeDom=0.055 dayDom=9.99 symDom=9.99 dbp=0.982
- **TEST  @15bps:** n=29 PF=0.51 net=Rs-10,051 win%=24.1 avgW=Rs1,497 avgL=Rs-933 maxDD=Rs-14,909 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=5.8 tradeDom=0.168 dayDom=9.99 symDom=9.99 dbp=0.8569
- **TRAIN @5bps:**  n=103 PF=0.528 net=Rs-28,832 win%=33.0 avgW=Rs947 avgL=Rs-884 maxDD=Rs-31,791 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=5.72 tradeDom=0.055 dayDom=9.99 symDom=9.99 dbp=0.982
- **TEST  @5bps:**  n=29 PF=0.51 net=Rs-10,051 win%=24.1 avgW=Rs1,497 avgL=Rs-933 maxDD=Rs-14,909 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=5.8 tradeDom=0.168 dayDom=9.99 symDom=9.99 dbp=0.8569

## Robustness diagnostics

- neighborhood pass: `False` (PF floor 1.15)
- term-dropout pass: `False` (PF floor 1.0)
- hard reasons: `TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates)`
- insufficient reasons: `-`
- warnings: `-`

- **Keep/reject:** REJECT  — TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates)

## Command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup L_PRESSURE_BURST_VWAP --pool C:\TradingData\eqidv2\setup_pools_2026_06_29\L_PRESSURE_BURST_VWAP --trials 700 --time_budget_min 10.0 --seed 37 --gap_lambda 0.8 --max_mask_terms 2 --max_pm_terms 2 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```