# G_HIGHER_HIGH_BREAK (LONG) — ITERATION_LOG

_Generated 2026-07-01. Optimizer: Optuna TPE. 700 FIT/VAL trials (each trial = one logical config: exit SL/Tgt + <=2 mask + <=2 pre-momentum + guards)._

Search protocol: optimise ONLY on FIT/VAL with the band objective `reward(min(FIT_PF,VAL_PF)) - 0.80*|FIT_PF-VAL_PF|`, where reward tents at PF 1.70 and penalises overshoot (anti-overfit). The single best FIT/VAL config is then confirmed on full TRAIN and scored ONCE on TEST. Quantile thresholds drawn from TRAIN only (never TEST). The robust gate then checks TEST PF+day-block p, TRAIN target-fill, threshold-neighborhood stability, and term-dropout stability.

Exit grid searched: SL [0.5, 0.7, 0.85, 1.0, 1.1, 1.2, 1.5] x Tgt [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5] (small/large SL x small/large target, asymmetric combos). Mask feats: ['atr_pct<=0.001655', 'atr_pct<=0.001882', 'atr_pct<=0.002908', 'atr_pct>=0.00357', 'body_pct<=0.732981', 'body_pct<=0.845619', 'body_pct<=0.893093', 'body_pct>=0.623033', 'close_loc<=0.643734', 'close_loc<=0.712955', 'close_loc<=0.773314', 'close_loc<=0.834242'].

## Top 20 FIT/VAL trials (by band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|-----|------|--------|-------|----------|----------|-------|
| 1 | 1.2 | 1.5 | close_loc<=0.953488;vwap_dist_atr<=3.401347 | pre3_range_r<=0.401292 | {"max_slot": "14:30", "top_n": 2} | 11/1.475 | 9/1.517 | 1.4674 |
| 2 | 1.2 | 1.5 | close_loc<=0.953488;vwap_dist_atr<=3.401347 | pre3_range_r<=0.401292 | {"min_slot": "11:00", "top_n": 2} | 11/1.475 | 9/1.517 | 1.4674 |
| 3 | 1.2 | 1.5 | close_loc<=0.953488;vwap_dist_atr<=3.401347 | pre3_range_r<=0.401292 | {"min_slot": "11:00", "top_n": 2} | 11/1.475 | 9/1.517 | 1.4674 |
| 4 | 1.2 | 1.5 | close_loc<=0.953488;vwap_dist_atr<=3.401347 | pre3_range_r<=0.401292 | {"min_slot": "11:00", "max_slot": "14:30", "top_n": 2} | 11/1.475 | 9/1.517 | 1.4674 |
| 5 | 1.2 | 1.5 | close_loc<=0.953488;vwap_dist_atr<=3.401347 | pre3_range_r<=0.401292 | {"min_slot": "11:00", "top_n": 2} | 11/1.475 | 9/1.517 | 1.4674 |
| 6 | 1.2 | 1.5 | close_loc<=0.953488;vwap_dist_atr<=3.401347 | pre3_range_r<=0.401292 | {"max_slot": "14:30", "top_n": 2} | 11/1.475 | 9/1.517 | 1.4674 |
| 7 | 1.2 | 1.5 | close_loc<=0.953488;vwap_dist_atr<=3.401347 | pre3_range_r<=0.401292 | {"min_slot": "11:00", "top_n": 2} | 11/1.475 | 9/1.517 | 1.4674 |
| 8 | 1.2 | 1.5 | close_loc<=0.953488;vwap_dist_atr<=3.401347 | pre3_range_r<=0.401292 | {"max_slot": "14:30", "top_n": 2} | 11/1.475 | 9/1.517 | 1.4674 |
| 9 | 1.2 | 1.5 | close_loc<=0.953488;vwap_dist_atr<=3.401347 | pre3_range_r<=0.401292 | {"max_slot": "14:30"} | 11/1.475 | 9/1.517 | 1.4674 |
| 10 | 1.2 | 1.5 | close_loc<=0.953488;vwap_dist_atr<=3.401347 | pre3_range_r<=0.401292 | {"max_slot": "14:30"} | 11/1.475 | 9/1.517 | 1.4674 |
| 11 | 1.2 | 1.5 | close_loc<=0.953488;vwap_dist_atr<=3.401347 | pre3_range_r<=0.401292 | {"max_slot": "14:30"} | 11/1.475 | 9/1.517 | 1.4674 |
| 12 | 1.2 | 1.5 | close_loc<=0.953488;vwap_dist_atr<=3.401347 | pre3_range_r<=0.401292 | {"max_slot": "14:30"} | 11/1.475 | 9/1.517 | 1.4674 |
| 13 | 1.2 | 1.5 | close_loc<=0.953488;vwap_dist_atr<=3.401347 | pre3_range_r<=0.401292 | {"min_slot": "11:00", "max_slot": "14:30"} | 11/1.475 | 9/1.517 | 1.4674 |
| 14 | 1.2 | 1.5 | close_loc<=0.953488;vwap_dist_atr<=3.401347 | pre3_range_r<=0.401292 | {"min_slot": "11:00", "max_slot": "14:30"} | 11/1.475 | 9/1.517 | 1.4674 |
| 15 | 1.2 | 1.5 | close_loc<=0.953488;vwap_dist_atr<=3.401347 | pre3_range_r<=0.401292 | {"min_slot": "11:00", "max_slot": "14:30"} | 11/1.475 | 9/1.517 | 1.4674 |
| 16 | 1.2 | 1.5 | close_loc<=0.953488;vwap_dist_atr<=3.401347 | pre3_range_r<=0.401292 | {"max_slot": "14:30"} | 11/1.475 | 9/1.517 | 1.4674 |
| 17 | 1.2 | 1.5 | close_loc<=0.953488;vwap_dist_atr<=3.401347 | pre3_range_r<=0.401292 | {"max_slot": "14:30", "top_n": 2} | 11/1.475 | 9/1.517 | 1.4674 |
| 18 | 1.2 | 1.5 | close_loc<=0.953488;vwap_dist_atr<=3.401347 | pre3_range_r<=0.401292 | {"min_slot": "11:00", "top_n": 2} | 11/1.475 | 9/1.517 | 1.4674 |
| 19 | 1.2 | 1.5 | close_loc<=0.953488;vwap_dist_atr<=3.401347 | pre3_range_r<=0.401292 | {"min_slot": "11:00", "top_n": 2} | 11/1.475 | 9/1.517 | 1.4674 |
| 20 | 1.2 | 1.5 | close_loc<=0.953488;vwap_dist_atr<=3.401347 | pre3_range_r<=0.401292 | {"min_slot": "11:00", "top_n": 2} | 11/1.475 | 9/1.517 | 1.4674 |

## Best config — confirmation runs

- **Best (FIT/VAL):** SL 1.2/Tgt 1.5 | mask [close_loc<=0.953488; vwap_dist_atr<=3.401347] | premom [pre3_range_r<=0.401292] | guard {'min_slot': '11:00', 'top_n': 2} | maxpos 20 | dloss 0.0
- **TRAIN @15bps:** n=20 PF=1.494 net=Rs3,671 win%=55.0 avgW=Rs1,009 avgL=Rs-825 maxDD=Rs-3,679 SL/TGT/EOD=2/8/10 tgt%=40.0 tpd=2.86 tradeDom=0.114 dayDom=0.682 symDom=0.345 dbp=0.1778
- **TEST  @15bps:** n=12 PF=1.194 net=Rs914 win%=50.0 avgW=Rs937 avgL=Rs-784 maxDD=Rs-3,239 SL/TGT/EOD=1/3/8 tgt%=25.0 tpd=3.0 tradeDom=0.225 dayDom=3.71 symDom=1.386 dbp=0.427
- **TRAIN @5bps:**  n=20 PF=1.859 net=Rs5,619 win%=60.0 avgW=Rs1,014 avgL=Rs-818 maxDD=Rs-3,284 SL/TGT/EOD=2/8/10 tgt%=40.0 tpd=2.86 tradeDom=0.112 dayDom=0.517 symDom=0.243 dbp=0.0821
- **TEST  @5bps:**  n=12 PF=1.503 net=Rs2,115 win%=58.3 avgW=Rs903 avgL=Rs-842 maxDD=Rs-2,767 SL/TGT/EOD=1/3/8 tgt%=25.0 tpd=3.0 tradeDom=0.216 dayDom=1.842 symDom=0.647 dbp=0.2777

## Robustness diagnostics

- neighborhood pass: `False` (PF floor 1.15)
- term-dropout pass: `False` (PF floor 1.0)
- hard reasons: `TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates)`
- insufficient reasons: `-`
- warnings: `-`

- **Keep/reject:** REJECT  — TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates)

## Command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup G_HIGHER_HIGH_BREAK --pool C:\TradingData\eqidv2\setup_pools_2026_06_29\G_HIGHER_HIGH_BREAK --trials 700 --time_budget_min 10.0 --seed 31 --gap_lambda 0.8 --max_mask_terms 2 --max_pm_terms 2 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```