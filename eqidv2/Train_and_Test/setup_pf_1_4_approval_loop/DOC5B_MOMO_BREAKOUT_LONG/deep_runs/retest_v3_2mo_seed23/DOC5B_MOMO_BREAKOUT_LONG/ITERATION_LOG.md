# DOC5B_MOMO_BREAKOUT_LONG (LONG) — ITERATION_LOG

_Generated 2026-07-01. Optimizer: Optuna TPE. 700 FIT/VAL trials (each trial = one logical config: exit SL/Tgt + <=2 mask + <=2 pre-momentum + guards)._

Search protocol: optimise ONLY on FIT/VAL with the band objective `reward(min(FIT_PF,VAL_PF)) - 0.80*|FIT_PF-VAL_PF|`, where reward tents at PF 1.70 and penalises overshoot (anti-overfit). The single best FIT/VAL config is then confirmed on full TRAIN and scored ONCE on TEST. Quantile thresholds drawn from TRAIN only (never TEST). The robust gate then checks TEST PF+day-block p, TRAIN target-fill, threshold-neighborhood stability, and term-dropout stability.

Exit grid searched: SL [0.5, 0.7, 0.85, 1.0, 1.1, 1.2, 1.5] x Tgt [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5] (small/large SL x small/large target, asymmetric combos). Mask feats: ['atr_pct<=0.003109', 'atr_pct>=0.002851', 'atr_pct>=0.003109', 'atr_pct>=0.005535', 'body_pct<=0.771906', 'body_pct<=0.862282', 'body_pct>=0.32572', 'body_pct>=0.587829', 'body_pct>=0.688797', 'body_pct>=0.771906', 'body_pct>=0.862282', 'breadth_above_vwap<=0.520098'].

## Top 20 FIT/VAL trials (by band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|-----|------|--------|-------|----------|----------|-------|
| 1 | 1.5 | 0.8 | breakout_strength_atr<=0.774244;ranker_score>=112.177566 | - | {"min_slot": "09:30", "top_n": 2} | 9/0.502 | 15/0.541 | 0.471 |
| 2 | 1.5 | 0.8 | retest_close_reclaim_atr>=0.669584;atr_pct>=0.002851 | - | {"max_slot": "14:00", "top_n": 2} | 19/0.524 | 23/0.609 | 0.4557 |
| 3 | 1.5 | 0.8 | retest_close_reclaim_atr>=0.669584;atr_pct>=0.002851 | - | {"min_slot": "09:45", "max_slot": "14:00", "top_n": 2} | 19/0.524 | 23/0.609 | 0.4557 |
| 4 | 1.5 | 0.8 | retest_close_reclaim_atr>=0.669584;atr_pct>=0.002851 | - | {"min_slot": "09:45", "max_slot": "14:00", "top_n": 2} | 19/0.524 | 23/0.609 | 0.4557 |
| 5 | 1.5 | 0.8 | retest_close_reclaim_atr>=0.669584;atr_pct>=0.002851 | - | {"min_slot": "09:45", "max_slot": "14:00", "top_n": 2} | 19/0.524 | 23/0.609 | 0.4557 |
| 6 | 1.5 | 0.8 | retest_close_reclaim_atr>=0.669584;atr_pct>=0.002851 | - | {"min_slot": "09:45", "max_slot": "14:00", "top_n": 2} | 19/0.524 | 23/0.609 | 0.4557 |
| 7 | 1.5 | 0.8 | retest_close_reclaim_atr>=0.669584;atr_pct>=0.002851 | - | {"min_slot": "09:45", "max_slot": "14:00", "top_n": 2} | 19/0.524 | 23/0.609 | 0.4557 |
| 8 | 1.5 | 0.8 | retest_close_reclaim_atr>=0.669584;atr_pct>=0.002851 | - | {"min_slot": "09:45", "top_n": 2} | 19/0.524 | 23/0.609 | 0.4557 |
| 9 | 1.0 | 0.8 | vwap_slope_atr>=0.299632;atr_pct>=0.002851 | - | {"min_slot": "09:45", "top_n": 1} | 15/0.414 | 24/0.402 | 0.393 |
| 10 | 1.0 | 0.8 | vwap_slope_atr>=0.299632;atr_pct>=0.002851 | - | {"min_slot": "09:45", "top_n": 1} | 15/0.414 | 24/0.402 | 0.393 |
| 11 | 1.0 | 0.8 | vwap_slope_atr>=0.299632;atr_pct>=0.002851 | - | {"min_slot": "09:45", "top_n": 1} | 15/0.414 | 24/0.402 | 0.393 |
| 12 | 1.0 | 0.8 | vwap_slope_atr>=0.299632;atr_pct>=0.002851 | - | {"min_slot": "09:45", "top_n": 1} | 15/0.414 | 24/0.402 | 0.393 |
| 13 | 1.0 | 0.8 | vwap_slope_atr>=0.299632;atr_pct>=0.002851 | - | {"min_slot": "09:45", "top_n": 1} | 15/0.414 | 24/0.402 | 0.393 |
| 14 | 1.2 | 0.8 | vwap_slope_atr>=0.539717;signal_range_pct>=0.25703 | - | {"min_slot": "09:45", "top_n": 2} | 11/0.516 | 21/0.715 | 0.3568 |
| 15 | 0.7 | 1.0 | lower_wick_pct>=0.055758;atr_pct>=0.002362 | - | {"min_slot": "10:00", "top_n": 1} | 13/0.352 | 19/0.341 | 0.3319 |
| 16 | 0.7 | 1.0 | lower_wick_pct>=0.055758;atr_pct>=0.002362 | - | {"min_slot": "10:00", "top_n": 1} | 13/0.352 | 19/0.341 | 0.3319 |
| 17 | 0.7 | 1.0 | lower_wick_pct>=0.055758;atr_pct>=0.002362 | - | {"min_slot": "10:00", "top_n": 1} | 13/0.352 | 19/0.341 | 0.3319 |
| 18 | 0.7 | 1.0 | lower_wick_pct>=0.055758;atr_pct>=0.002362 | - | {"min_slot": "10:00", "top_n": 1} | 13/0.352 | 19/0.341 | 0.3319 |
| 19 | 1.0 | 0.8 | retest_close_reclaim_atr>=0.669584;atr_pct>=0.002851 | - | {"min_slot": "09:45", "top_n": 2} | 19/0.343 | 23/0.361 | 0.3284 |
| 20 | 1.0 | 0.8 | retest_close_reclaim_atr>=0.669584;atr_pct>=0.002851 | - | {"min_slot": "09:45", "top_n": 2} | 19/0.343 | 23/0.361 | 0.3284 |

## Best config — confirmation runs

- **Best (FIT/VAL):** SL 1.5/Tgt 0.8 | mask [breakout_strength_atr<=0.774244; ranker_score>=112.177566] | premom [(none)] | guard {'min_slot': '09:30', 'top_n': 2} | maxpos 20 | dloss 4000.0
- **TRAIN @15bps:** n=24 PF=0.527 net=Rs-5,898 win%=50.0 avgW=Rs547 avgL=Rs-1,039 maxDD=Rs-4,358 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.71 tradeDom=0.086 dayDom=9.99 symDom=9.99 dbp=0.8707
- **TEST  @15bps:** n=7 PF=0.434 net=Rs-2,937 win%=57.1 avgW=Rs564 avgL=Rs-1,730 maxDD=Rs-4,630 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.75 tradeDom=0.251 dayDom=9.99 symDom=9.99 dbp=0.937
- **TRAIN @5bps:**  n=24 PF=0.527 net=Rs-5,898 win%=50.0 avgW=Rs547 avgL=Rs-1,039 maxDD=Rs-4,358 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.71 tradeDom=0.086 dayDom=9.99 symDom=9.99 dbp=0.8707
- **TEST  @5bps:**  n=7 PF=0.434 net=Rs-2,937 win%=57.1 avgW=Rs564 avgL=Rs-1,730 maxDD=Rs-4,630 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.75 tradeDom=0.251 dayDom=9.99 symDom=9.99 dbp=0.937

## Robustness diagnostics

- neighborhood pass: `False` (PF floor 1.15)
- term-dropout pass: `False` (PF floor 1.0)
- hard reasons: `TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates)`
- insufficient reasons: `-`
- warnings: `-`

- **Keep/reject:** REJECT  — TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates)

## Command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup DOC5B_MOMO_BREAKOUT_LONG --pool Train_and_Test/doc5_long_setups/pool_retest_v3_2mo --trials 700 --time_budget_min 10.0 --seed 23 --gap_lambda 0.8 --max_mask_terms 2 --max_pm_terms 2 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```