# L_DOUBLE_BOTTOM_VWAP (LONG) — ITERATION_LOG

_Generated 2026-07-01. Optimizer: Optuna TPE. 700 FIT/VAL trials (each trial = one logical config: exit SL/Tgt + <=2 mask + <=2 pre-momentum + guards)._

Search protocol: optimise ONLY on FIT/VAL with the band objective `reward(min(FIT_PF,VAL_PF)) - 0.80*|FIT_PF-VAL_PF|`, where reward tents at PF 1.70 and penalises overshoot (anti-overfit). The single best FIT/VAL config is then confirmed on full TRAIN and scored ONCE on TEST. Quantile thresholds drawn from TRAIN only (never TEST). The robust gate then checks TEST PF+day-block p, TRAIN target-fill, threshold-neighborhood stability, and term-dropout stability.

Exit grid searched: SL [0.5, 0.7, 0.85, 1.0, 1.1, 1.2, 1.5] x Tgt [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5] (small/large SL x small/large target, asymmetric combos). Mask feats: ['atr_pct<=0.001606', 'atr_pct<=0.001914', 'atr_pct<=0.002604', 'atr_pct<=0.003071', 'atr_pct<=0.004211', 'atr_pct<=0.01226', 'atr_pct<=0.017892', 'atr_pct<=0.026736', 'atr_pct>=0.002261', 'atr_pct>=0.003071', 'body_pct<=0.70181', 'body_pct<=0.746269'].

## Top 20 FIT/VAL trials (by band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|-----|------|--------|-------|----------|----------|-------|
| 1 | 0.85 | 2.5 | close_loc<=1.0;vwap_dist_atr<=0.180032 | - | {"min_slot": "09:30", "max_slot": "13:00"} | 22/0.919 | 12/0.946 | 0.8971 |
| 2 | 0.85 | 2.5 | close_loc<=1.0;vwap_dist_atr<=0.180032 | - | {"min_slot": "09:30", "max_slot": "13:00"} | 22/0.919 | 12/0.946 | 0.8971 |
| 3 | 0.85 | 2.5 | close_loc<=1.0;vwap_dist_atr<=0.180032 | - | {"min_slot": "09:30", "max_slot": "13:00"} | 22/0.919 | 12/0.946 | 0.8971 |
| 4 | 0.85 | 2.5 | close_loc<=1.0;vwap_dist_atr<=0.180032 | - | {"min_slot": "09:30", "max_slot": "13:00"} | 22/0.919 | 12/0.946 | 0.8971 |
| 5 | 0.85 | 2.5 | close_loc<=1.0;vwap_dist_atr<=0.180032 | - | {"min_slot": "09:30", "max_slot": "13:00"} | 22/0.919 | 12/0.946 | 0.8971 |
| 6 | 1.2 | 2.5 | signal_range_pct<=2.777335;vwap_dist_atr<=0.180032 | - | {"min_slot": "10:30", "top_n": 3} | 26/0.837 | 23/0.838 | 0.836 |
| 7 | 1.2 | 2.5 | signal_range_pct<=2.777335;vwap_dist_atr<=0.180032 | - | {"min_slot": "09:30", "top_n": 3} | 26/0.837 | 23/0.838 | 0.836 |
| 8 | 1.2 | 2.5 | signal_range_pct<=2.777335;vwap_dist_atr<=0.180032 | - | {"min_slot": "09:30", "top_n": 3} | 26/0.837 | 23/0.838 | 0.836 |
| 9 | 1.2 | 2.5 | signal_range_pct<=2.777335;vwap_dist_atr<=0.180032 | - | {"min_slot": "10:30", "top_n": 3} | 26/0.837 | 23/0.838 | 0.836 |
| 10 | 1.2 | 2.5 | signal_range_pct<=2.777335;vwap_dist_atr<=0.180032 | - | {"min_slot": "09:45", "top_n": 3} | 26/0.837 | 23/0.838 | 0.836 |
| 11 | 1.5 | 2.5 | signal_range_pct<=2.777335;vwap_dist_atr<=0.180032 | - | {"min_slot": "10:30", "top_n": 3} | 26/0.814 | 23/0.832 | 0.8002 |
| 12 | 1.5 | 2.5 | signal_range_pct<=2.777335;vwap_dist_atr<=0.180032 | - | {"min_slot": "10:30", "top_n": 3} | 26/0.814 | 23/0.832 | 0.8002 |
| 13 | 1.5 | 2.5 | signal_range_pct<=2.777335;vwap_dist_atr<=0.180032 | - | {"min_slot": "10:30", "top_n": 3} | 26/0.814 | 23/0.832 | 0.8002 |
| 14 | 1.5 | 2.5 | signal_range_pct<=2.777335;vwap_dist_atr<=0.180032 | - | {"min_slot": "10:30", "top_n": 3} | 26/0.814 | 23/0.832 | 0.8002 |
| 15 | 1.5 | 2.5 | signal_range_pct<=2.777335;vwap_dist_atr<=0.180032 | - | {"min_slot": "10:30", "top_n": 3} | 26/0.814 | 23/0.832 | 0.8002 |
| 16 | 1.5 | 2.5 | signal_range_pct<=2.777335;vwap_dist_atr<=0.180032 | - | {"min_slot": "10:30", "top_n": 3} | 26/0.814 | 23/0.832 | 0.8002 |
| 17 | 1.5 | 2.5 | signal_range_pct<=2.777335;vwap_dist_atr<=0.180032 | - | {"min_slot": "10:30", "top_n": 3} | 26/0.814 | 23/0.832 | 0.8002 |
| 18 | 1.5 | 2.5 | signal_range_pct<=2.777335;vwap_dist_atr<=0.180032 | - | {"min_slot": "10:30", "top_n": 3} | 26/0.814 | 23/0.832 | 0.8002 |
| 19 | 1.5 | 2.5 | signal_range_pct<=2.777335;vwap_dist_atr<=0.180032 | - | {"min_slot": "10:30", "top_n": 3} | 26/0.814 | 23/0.832 | 0.8002 |
| 20 | 1.5 | 2.5 | signal_range_pct<=2.777335;vwap_dist_atr<=0.180032 | - | {"min_slot": "10:30", "top_n": 3} | 26/0.814 | 23/0.832 | 0.8002 |

## Best config — confirmation runs

- **Best (FIT/VAL):** SL 0.85/Tgt 2.5 | mask [close_loc<=1.0; vwap_dist_atr<=0.180032] | premom [(none)] | guard {'min_slot': '09:30', 'max_slot': '13:00'} | maxpos 10 | dloss 0.0
- **TRAIN @15bps:** n=34 PF=0.929 net=Rs-1,312 win%=38.2 avgW=Rs1,320 avgL=Rs-880 maxDD=Rs-5,270 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.62 tradeDom=0.132 dayDom=9.99 symDom=9.99 dbp=0.6483
- **TEST  @15bps:** n=4 PF=0.0 net=Rs-4,094 win%=0.0 avgW=Rs0 avgL=Rs-1,024 maxDD=Rs-3,015 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.33 tradeDom=9.99 dayDom=9.99 symDom=9.99 dbp=1.0
- **TRAIN @5bps:**  n=34 PF=0.929 net=Rs-1,312 win%=38.2 avgW=Rs1,320 avgL=Rs-880 maxDD=Rs-5,270 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.62 tradeDom=0.132 dayDom=9.99 symDom=9.99 dbp=0.6483
- **TEST  @5bps:**  n=4 PF=0.0 net=Rs-4,094 win%=0.0 avgW=Rs0 avgL=Rs-1,024 maxDD=Rs-3,015 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.33 tradeDom=9.99 dayDom=9.99 symDom=9.99 dbp=1.0

## Robustness diagnostics

- neighborhood pass: `False` (PF floor 1.15)
- term-dropout pass: `False` (PF floor 1.0)
- hard reasons: `TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed`
- insufficient reasons: `TEST too few trades (test_n<6)`
- warnings: `-`

- **Keep/reject:** REJECT  — TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST too few trades (test_n<6)

## Command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup L_DOUBLE_BOTTOM_VWAP --pool C:\TradingData\eqidv2\setup_pools_2026_06_29\L_DOUBLE_BOTTOM_VWAP --trials 700 --time_budget_min 10.0 --seed 43 --gap_lambda 0.8 --max_mask_terms 2 --max_pm_terms 2 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```