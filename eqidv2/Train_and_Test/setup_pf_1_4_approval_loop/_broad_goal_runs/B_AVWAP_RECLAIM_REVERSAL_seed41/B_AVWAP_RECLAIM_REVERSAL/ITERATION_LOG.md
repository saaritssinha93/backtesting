# B_AVWAP_RECLAIM_REVERSAL (LONG) — ITERATION_LOG

_Generated 2026-07-01. Optimizer: Optuna TPE. 700 FIT/VAL trials (each trial = one logical config: exit SL/Tgt + <=2 mask + <=2 pre-momentum + guards)._

Search protocol: optimise ONLY on FIT/VAL with the band objective `reward(min(FIT_PF,VAL_PF)) - 0.80*|FIT_PF-VAL_PF|`, where reward tents at PF 1.70 and penalises overshoot (anti-overfit). The single best FIT/VAL config is then confirmed on full TRAIN and scored ONCE on TEST. Quantile thresholds drawn from TRAIN only (never TEST). The robust gate then checks TEST PF+day-block p, TRAIN target-fill, threshold-neighborhood stability, and term-dropout stability.

Exit grid searched: SL [0.5, 0.7, 0.85, 1.0, 1.1, 1.2, 1.5] x Tgt [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5] (small/large SL x small/large target, asymmetric combos). Mask feats: ['atr_pct<=0.002166', 'atr_pct<=0.004021', 'atr_pct<=0.005333', 'atr_pct>=0.00169', 'atr_pct>=0.002166', 'atr_pct>=0.002413', 'atr_pct>=0.002673', 'atr_pct>=0.002958', 'atr_pct>=0.005333', 'body_pct<=0.69454', 'body_pct<=0.9375', 'body_pct>=0.565217'].

## Top 20 FIT/VAL trials (by band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|-----|------|--------|-------|----------|----------|-------|
| 1 | 0.7 | 1.5 | signal_range_pct>=1.209043;body_pct<=0.9375 | sig5_adx_calc<=20.175403 | {"min_slot": "10:30", "max_slot": "13:00", "top_n": 2} | 11/1.128 | 6/1.353 | 0.949 |
| 2 | 1.1 | 1.5 | signal_range_pct>=1.209043;body_pct<=0.833981 | pre3_close_pos<=0.775312 | {"max_slot": "13:00", "top_n": 2} | 14/0.954 | 6/0.95 | 0.9473 |
| 3 | 1.1 | 1.5 | signal_range_pct>=1.209043;body_pct<=0.833981 | pre3_close_pos<=0.775312 | {"max_slot": "13:00", "top_n": 2} | 14/0.954 | 6/0.95 | 0.9473 |
| 4 | 1.1 | 1.5 | signal_range_pct>=1.209043;body_pct<=0.833981 | pre3_close_pos<=0.775312 | {"max_slot": "13:00", "top_n": 2} | 14/0.954 | 6/0.95 | 0.9473 |
| 5 | 1.1 | 1.5 | signal_range_pct>=1.209043;body_pct<=0.833981 | pre3_close_pos<=0.775312 | {"max_slot": "13:00", "top_n": 2} | 14/0.954 | 6/0.95 | 0.9473 |
| 6 | 1.1 | 1.5 | signal_range_pct>=1.209043;body_pct<=0.833981 | pre3_close_pos<=0.775312 | {"max_slot": "13:00", "top_n": 2} | 14/0.954 | 6/0.95 | 0.9473 |
| 7 | 1.1 | 1.5 | signal_range_pct>=1.209043;body_pct<=0.833981 | pre3_close_pos<=0.775312 | {"max_slot": "13:00", "top_n": 2} | 14/0.954 | 6/0.95 | 0.9473 |
| 8 | 1.1 | 1.5 | signal_range_pct>=1.209043;body_pct<=0.9375 | sig5_adx_calc<=20.175403 | {"min_slot": "10:30", "max_slot": "13:00", "top_n": 2} | 11/0.928 | 6/0.947 | 0.9123 |
| 9 | 1.1 | 1.5 | signal_range_pct>=1.209043;body_pct<=0.9375 | sig5_adx_calc<=20.175403 | {"min_slot": "09:30", "max_slot": "13:00", "top_n": 2} | 11/0.928 | 6/0.947 | 0.9123 |
| 10 | 1.1 | 1.5 | signal_range_pct>=1.209043;body_pct<=0.9375 | sig5_adx_calc<=20.175403 | {"min_slot": "10:30", "max_slot": "13:00", "top_n": 2} | 11/0.928 | 6/0.947 | 0.9123 |
| 11 | 1.1 | 1.5 | signal_range_pct>=1.209043;body_pct<=0.9375 | sig5_adx_calc<=20.175403 | {"max_slot": "13:00", "top_n": 2} | 11/0.928 | 6/0.947 | 0.9123 |
| 12 | 1.1 | 1.5 | signal_range_pct>=1.209043;body_pct<=0.9375 | sig5_adx_calc<=20.175403 | {"min_slot": "10:30", "max_slot": "13:00", "top_n": 2} | 11/0.928 | 6/0.947 | 0.9123 |
| 13 | 1.1 | 1.5 | signal_range_pct>=1.209043;body_pct<=0.9375 | sig5_adx_calc<=20.175403 | {"max_slot": "13:00", "top_n": 2} | 11/0.928 | 6/0.947 | 0.9123 |
| 14 | 1.1 | 1.5 | signal_range_pct>=1.209043;body_pct<=0.9375 | sig5_adx_calc<=20.175403 | {"max_slot": "13:00", "top_n": 2} | 11/0.928 | 6/0.947 | 0.9123 |
| 15 | 1.1 | 1.5 | signal_range_pct>=1.209043;body_pct<=0.9375 | sig5_adx_calc<=20.175403 | {"max_slot": "13:00", "top_n": 2} | 11/0.928 | 6/0.947 | 0.9123 |
| 16 | 1.1 | 1.5 | signal_range_pct>=1.209043;body_pct<=0.9375 | sig5_adx_calc<=20.175403 | {"max_slot": "13:00", "top_n": 2} | 11/0.928 | 6/0.947 | 0.9123 |
| 17 | 1.1 | 1.5 | signal_range_pct>=1.209043;body_pct<=0.9375 | sig5_adx_calc<=20.175403 | {"max_slot": "13:00", "top_n": 2} | 11/0.928 | 6/0.947 | 0.9123 |
| 18 | 1.1 | 1.5 | signal_range_pct>=1.209043;body_pct<=0.9375 | sig5_adx_calc<=20.175403 | {"min_slot": "10:30", "max_slot": "13:00", "top_n": 2} | 11/0.928 | 6/0.947 | 0.9123 |
| 19 | 1.1 | 1.5 | signal_range_pct>=1.209043;body_pct<=0.9375 | sig5_adx_calc<=20.175403 | {"max_slot": "13:00", "top_n": 2} | 11/0.928 | 6/0.947 | 0.9123 |
| 20 | 1.1 | 1.5 | signal_range_pct>=1.209043;body_pct<=0.9375 | sig5_adx_calc<=20.175403 | {"min_slot": "10:30", "max_slot": "13:00", "top_n": 2} | 11/0.928 | 6/0.947 | 0.9123 |

## Best config — confirmation runs

- **Best (FIT/VAL):** SL 0.7/Tgt 1.5 | mask [signal_range_pct>=1.209043; body_pct<=0.9375] | premom [sig5_adx_calc<=20.175403] | guard {'min_slot': '10:30', 'max_slot': '13:00', 'top_n': 2} | maxpos 10 | dloss 4000.0
- **TRAIN @15bps:** n=17 PF=1.203 net=Rs1,704 win%=47.1 avgW=Rs1,261 avgL=Rs-931 maxDD=Rs-2,455 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.7 tradeDom=0.126 dayDom=0.743 symDom=0.743 dbp=0.3004
- **TEST  @15bps:** n=2 PF=0.0 net=Rs-1,856 win%=0.0 avgW=Rs0 avgL=Rs-928 maxDD=Rs-930 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.0 tradeDom=9.99 dayDom=9.99 symDom=9.99 dbp=None
- **TRAIN @5bps:**  n=17 PF=1.203 net=Rs1,704 win%=47.1 avgW=Rs1,261 avgL=Rs-931 maxDD=Rs-2,455 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.7 tradeDom=0.126 dayDom=0.743 symDom=0.743 dbp=0.3004
- **TEST  @5bps:**  n=2 PF=0.0 net=Rs-1,856 win%=0.0 avgW=Rs0 avgL=Rs-928 maxDD=Rs-930 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.0 tradeDom=9.99 dayDom=9.99 symDom=9.99 dbp=None

## Robustness diagnostics

- neighborhood pass: `False` (PF floor 1.15)
- term-dropout pass: `False` (PF floor 1.0)
- hard reasons: `TRAIN too few trades (train_n<20); TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed`
- insufficient reasons: `TEST too few trades (test_n<6)`
- warnings: `-`

- **Keep/reject:** REJECT  — TRAIN too few trades (train_n<20); TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST too few trades (test_n<6)

## Command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup B_AVWAP_RECLAIM_REVERSAL --pool C:\TradingData\eqidv2\setup_pools_2026_06_29\B_AVWAP_RECLAIM_REVERSAL --trials 700 --time_budget_min 10.0 --seed 41 --gap_lambda 0.8 --max_mask_terms 2 --max_pm_terms 2 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```