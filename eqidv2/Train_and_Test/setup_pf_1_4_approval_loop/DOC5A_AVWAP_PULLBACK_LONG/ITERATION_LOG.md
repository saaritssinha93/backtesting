# DOC5A_AVWAP_PULLBACK_LONG (LONG) — ITERATION_LOG

_Generated 2026-07-01. Optimizer: Optuna TPE. 700 FIT/VAL trials (each trial = one logical config: exit SL/Tgt + <=2 mask + <=2 pre-momentum + guards)._

Search protocol: optimise ONLY on FIT/VAL with the band objective `reward(min(FIT_PF,VAL_PF)) - 0.80*|FIT_PF-VAL_PF|`, where reward tents at PF 1.70 and penalises overshoot (anti-overfit). The single best FIT/VAL config is then confirmed on full TRAIN and scored ONCE on TEST. Quantile thresholds drawn from TRAIN only (never TEST). The robust gate then checks TEST PF+day-block p, TRAIN target-fill, threshold-neighborhood stability, and term-dropout stability.

Exit grid searched: SL [0.5, 0.7, 0.85, 1.0, 1.1, 1.2, 1.5] x Tgt [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5] (small/large SL x small/large target, asymmetric combos). Mask feats: ['atr_pct>=0.004169', 'atr_pct>=0.004712', 'body_pct>=0.265063', 'close_loc<=0.695963', 'close_loc<=0.934368', 'close_loc>=0.934368', 'lower_wick_pct<=0.015217', 'lower_wick_pct<=0.038426', 'lower_wick_pct<=0.054063', 'quality_score<=79.757212', 'quality_score>=60.257405', 'quality_score>=75.40072'].

## Top 20 FIT/VAL trials (by band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|-----|------|--------|-------|----------|----------|-------|
| 1 | 0.85 | 1.25 | - | pre3_range_r>=0.308943 | {"max_slot": "12:30", "top_n": 3} | 25/0.438 | 39/0.422 | 0.4094 |
| 2 | 0.85 | 1.25 | - | pre3_range_r>=0.308943 | {"max_slot": "12:30", "top_n": 3} | 25/0.438 | 39/0.422 | 0.4094 |
| 3 | 0.7 | 1.25 | - | pre3_range_r>=0.308943 | {"min_slot": "10:30", "top_n": 1} | 12/0.449 | 24/0.506 | 0.4024 |
| 4 | 0.7 | 1.25 | - | pre3_range_r>=0.308943 | {"min_slot": "10:30", "top_n": 1} | 12/0.449 | 24/0.506 | 0.4024 |
| 5 | 0.7 | 1.25 | - | pre3_range_r>=0.308943 | {"min_slot": "10:30", "top_n": 1} | 12/0.449 | 24/0.506 | 0.4024 |
| 6 | 0.7 | 1.25 | - | pre3_range_r>=0.308943 | {"min_slot": "10:30", "top_n": 1} | 12/0.449 | 24/0.506 | 0.4024 |
| 7 | 0.7 | 1.25 | - | pre3_range_r>=0.308943 | {"min_slot": "10:30", "top_n": 1} | 12/0.449 | 24/0.506 | 0.4024 |
| 8 | 0.7 | 1.25 | - | pre3_range_r>=0.308943 | {"min_slot": "10:30", "top_n": 1} | 12/0.449 | 24/0.506 | 0.4024 |
| 9 | 0.7 | 1.25 | - | pre3_range_r>=0.308943 | {"min_slot": "10:30", "top_n": 1} | 12/0.449 | 24/0.506 | 0.4024 |
| 10 | 0.7 | 1.25 | - | pre3_range_r>=0.308943 | {"min_slot": "10:30", "top_n": 1} | 12/0.449 | 24/0.506 | 0.4024 |
| 11 | 0.7 | 1.25 | - | pre3_range_r>=0.308943 | {"min_slot": "10:30", "top_n": 1} | 12/0.449 | 24/0.506 | 0.4024 |
| 12 | 0.7 | 1.25 | - | pre3_range_r>=0.308943 | {"min_slot": "10:30", "top_n": 1} | 12/0.449 | 24/0.506 | 0.4024 |
| 13 | 0.7 | 1.25 | - | pre3_range_r>=0.308943 | {"min_slot": "10:30", "top_n": 1} | 12/0.449 | 24/0.506 | 0.4024 |
| 14 | 0.7 | 1.25 | - | pre3_range_r>=0.308943 | {"min_slot": "10:30", "top_n": 1} | 12/0.449 | 24/0.506 | 0.4024 |
| 15 | 0.7 | 1.25 | - | pre3_range_r>=0.308943 | {"min_slot": "10:30", "top_n": 1} | 12/0.449 | 24/0.506 | 0.4024 |
| 16 | 0.7 | 1.25 | - | pre3_range_r>=0.308943 | {"min_slot": "10:30", "top_n": 1} | 12/0.449 | 24/0.506 | 0.4024 |
| 17 | 0.7 | 1.25 | - | pre3_range_r>=0.308943 | {"min_slot": "10:30", "top_n": 1} | 12/0.449 | 24/0.506 | 0.4024 |
| 18 | 0.7 | 1.25 | - | pre3_range_r>=0.308943 | {"min_slot": "10:30", "top_n": 1} | 12/0.449 | 24/0.506 | 0.4024 |
| 19 | 0.7 | 1.25 | - | pre3_range_r>=0.308943 | {"min_slot": "10:30", "top_n": 1} | 12/0.449 | 24/0.506 | 0.4024 |
| 20 | 0.7 | 1.25 | - | pre3_range_r>=0.308943 | {"min_slot": "10:30", "top_n": 1} | 12/0.449 | 24/0.506 | 0.4024 |

## Best config — confirmation runs

- **Best (FIT/VAL):** SL 0.85/Tgt 1.25 | mask [(none)] | premom [pre3_range_r>=0.308943] | guard {'max_slot': '12:30', 'top_n': 3} | maxpos 20 | dloss 0.0
- **TRAIN @15bps:** n=64 PF=0.428 net=Rs-22,294 win%=35.9 avgW=Rs726 avgL=Rs-951 maxDD=Rs-25,687 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=3.2 tradeDom=0.061 dayDom=9.99 symDom=9.99 dbp=0.9977
- **TEST  @15bps:** n=12 PF=0.499 net=Rs-4,066 win%=33.3 avgW=Rs1,012 avgL=Rs-1,014 maxDD=Rs-5,457 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.4 tradeDom=0.251 dayDom=9.99 symDom=9.99 dbp=0.9561
- **TRAIN @5bps:**  n=64 PF=0.428 net=Rs-22,294 win%=35.9 avgW=Rs726 avgL=Rs-951 maxDD=Rs-25,687 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=3.2 tradeDom=0.061 dayDom=9.99 symDom=9.99 dbp=0.9977
- **TEST  @5bps:**  n=12 PF=0.499 net=Rs-4,066 win%=33.3 avgW=Rs1,012 avgL=Rs-1,014 maxDD=Rs-5,457 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.4 tradeDom=0.251 dayDom=9.99 symDom=9.99 dbp=0.9561

## Robustness diagnostics

- neighborhood pass: `False` (PF floor 1.15)
- term-dropout pass: `False` (PF floor 1.0)
- hard reasons: `TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates)`
- insufficient reasons: `-`
- warnings: `-`

- **Keep/reject:** REJECT  — TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates)

## Command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup DOC5A_AVWAP_PULLBACK_LONG --pool C:/Users/Saarit/OneDrive/Desktop/Trading/backtesting/eqidv2/backtesting/eqidv2/Train_and_Test/doc5_long_setups/pool --trials 700 --time_budget_min 22.0 --seed 7 --gap_lambda 0.8 --max_mask_terms 2 --max_pm_terms 2 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```