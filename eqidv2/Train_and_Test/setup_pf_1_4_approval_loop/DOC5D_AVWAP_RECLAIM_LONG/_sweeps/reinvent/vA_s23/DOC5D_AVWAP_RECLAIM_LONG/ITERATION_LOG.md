# DOC5D_AVWAP_RECLAIM_LONG (LONG) — ITERATION_LOG

_Generated 2026-07-01. Optimizer: Optuna TPE. 400 FIT/VAL trials (each trial = one logical config: exit SL/Tgt + <=1 mask + <=1 pre-momentum + guards)._

Search protocol: optimise ONLY on FIT/VAL with the band objective `reward(min(FIT_PF,VAL_PF)) - 0.80*|FIT_PF-VAL_PF|`, where reward tents at PF 1.70 and penalises overshoot (anti-overfit). The single best FIT/VAL config is then confirmed on full TRAIN and scored ONCE on TEST. Quantile thresholds drawn from TRAIN only (never TEST). The robust gate then checks TEST PF+day-block p, TRAIN target-fill, threshold-neighborhood stability, and term-dropout stability.

Exit grid searched: SL [0.5, 0.7, 0.85, 1.0, 1.1, 1.2, 1.5] x Tgt [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5] (small/large SL x small/large target, asymmetric combos). Mask feats: ['atr_pct>=0.005738', 'body_pct<=0.752426', 'close_loc<=0.916667', 'lower_wick_pct>=0.0', 'quality_score>=102.968597', 'ranker_score<=77.558394', 'rs_pct<=2.36242', 'signal_range_pct<=0.410322', 'upper_wick_pct>=0.146542', 'vol_ratio>=1.527604', 'vwap_dist_atr>=0.710311', 'vwap_slope_atr<=-0.052319'].

## Top 20 FIT/VAL trials (by band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|-----|------|--------|-------|----------|----------|-------|
| 1 | 1.0 | 2.5 | - | sig5_adx_calc<=20.8676 | {"min_slot": "11:00", "max_slot": "13:00", "top_n": 3} | 37/0.983 | 58/0.994 | 0.9739 |
| 2 | 1.0 | 2.5 | - | sig5_adx_calc<=20.8676 | {"min_slot": "11:00", "max_slot": "13:00", "top_n": 3} | 37/0.983 | 58/0.994 | 0.9739 |
| 3 | 1.0 | 2.5 | - | sig5_adx_calc<=20.8676 | {"min_slot": "11:00", "max_slot": "13:00", "top_n": 3} | 37/0.983 | 58/0.994 | 0.9739 |
| 4 | 1.0 | 2.5 | - | sig5_adx_calc<=20.8676 | {"min_slot": "11:00", "max_slot": "14:00", "top_n": 3} | 37/0.983 | 58/0.994 | 0.9739 |
| 5 | 1.0 | 2.5 | - | sig5_adx_calc<=20.8676 | {"min_slot": "11:00", "max_slot": "13:00", "top_n": 3} | 37/0.983 | 58/0.994 | 0.9739 |
| 6 | 1.0 | 2.5 | - | sig5_adx_calc<=20.8676 | {"min_slot": "11:00", "max_slot": "13:00", "top_n": 3} | 37/0.983 | 58/0.994 | 0.9739 |
| 7 | 1.0 | 2.5 | - | sig5_adx_calc<=20.8676 | {"min_slot": "11:00", "max_slot": "13:00", "top_n": 3} | 37/0.983 | 58/0.994 | 0.9739 |
| 8 | 1.0 | 2.5 | - | sig5_adx_calc<=20.8676 | {"min_slot": "11:00", "max_slot": "13:00", "top_n": 3} | 37/0.983 | 58/0.994 | 0.9739 |
| 9 | 1.0 | 2.5 | - | sig5_adx_calc<=20.8676 | {"min_slot": "11:00", "max_slot": "13:00", "top_n": 3} | 37/0.983 | 58/0.994 | 0.9739 |
| 10 | 1.0 | 2.5 | - | sig5_adx_calc<=20.8676 | {"min_slot": "11:00", "max_slot": "13:00", "top_n": 3} | 37/0.983 | 58/0.994 | 0.9739 |
| 11 | 1.0 | 2.5 | - | sig5_adx_calc<=20.8676 | {"min_slot": "11:00", "max_slot": "13:00", "top_n": 3} | 37/0.983 | 58/0.994 | 0.9739 |
| 12 | 1.0 | 2.5 | - | sig5_adx_calc<=20.8676 | {"min_slot": "11:00", "max_slot": "13:00", "top_n": 3} | 37/0.983 | 58/0.994 | 0.9739 |
| 13 | 1.0 | 2.5 | - | sig5_adx_calc<=20.8676 | {"min_slot": "11:00", "max_slot": "13:00", "top_n": 3} | 37/0.983 | 58/0.994 | 0.9739 |
| 14 | 1.0 | 2.5 | - | sig5_adx_calc<=20.8676 | {"min_slot": "11:00", "max_slot": "13:00", "top_n": 3} | 37/0.983 | 58/0.994 | 0.9739 |
| 15 | 1.0 | 2.5 | - | sig5_adx_calc<=20.8676 | {"min_slot": "11:00", "top_n": 3} | 37/0.983 | 58/0.994 | 0.9739 |
| 16 | 1.0 | 2.5 | - | sig5_adx_calc<=20.8676 | {"min_slot": "11:00", "max_slot": "14:30", "top_n": 3} | 37/0.983 | 58/0.994 | 0.9739 |
| 17 | 1.0 | 2.5 | - | sig5_adx_calc<=20.8676 | {"min_slot": "11:00", "top_n": 3} | 37/0.983 | 58/0.994 | 0.9739 |
| 18 | 1.0 | 2.5 | - | sig5_adx_calc<=20.8676 | {"min_slot": "11:00", "top_n": 3} | 37/0.983 | 58/0.994 | 0.9739 |
| 19 | 1.0 | 2.5 | - | sig5_adx_calc<=20.8676 | {"min_slot": "11:00", "top_n": 3} | 37/0.983 | 58/0.994 | 0.9739 |
| 20 | 1.0 | 2.5 | - | sig5_adx_calc<=20.8676 | {"min_slot": "11:00", "top_n": 3} | 37/0.983 | 58/0.994 | 0.9739 |

## Best config — confirmation runs

- **Best (FIT/VAL):** SL 1.0/Tgt 2.5 | mask [(none)] | premom [sig5_adx_calc<=20.8676] | guard {'min_slot': '11:00', 'top_n': 3} | maxpos 20 | dloss 0.0
- **TRAIN @15bps:** n=95 PF=0.99 net=Rs-469 win%=42.1 avgW=Rs1,134 avgL=Rs-833 maxDD=Rs-10,805 SL/TGT/EOD=33/11/51 tgt%=11.6 tpd=5.0 tradeDom=0.052 dayDom=9.99 symDom=9.99 dbp=0.5304
- **TEST  @15bps:** n=30 PF=1.448 net=Rs4,847 win%=50.0 avgW=Rs1,044 avgL=Rs-721 maxDD=Rs-4,078 SL/TGT/EOD=7/4/19 tgt%=13.3 tpd=5.0 tradeDom=0.151 dayDom=0.687 symDom=0.488 dbp=0.1602
- **TRAIN @5bps:**  n=95 PF=0.99 net=Rs-469 win%=42.1 avgW=Rs1,134 avgL=Rs-833 maxDD=Rs-10,805 SL/TGT/EOD=33/11/51 tgt%=11.6 tpd=5.0 tradeDom=0.052 dayDom=9.99 symDom=9.99 dbp=0.5304
- **TEST  @5bps:**  n=30 PF=1.448 net=Rs4,847 win%=50.0 avgW=Rs1,044 avgL=Rs-721 maxDD=Rs-4,078 SL/TGT/EOD=7/4/19 tgt%=13.3 tpd=5.0 tradeDom=0.151 dayDom=0.687 symDom=0.488 dbp=0.1602

## Robustness diagnostics

- neighborhood pass: `False` (PF floor 1.15)
- term-dropout pass: `False` (PF floor 1.0)
- hard reasons: `TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates)`
- insufficient reasons: `-`
- warnings: `-`

- **Keep/reject:** REJECT  — TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates)

## Command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup DOC5D_AVWAP_RECLAIM_LONG --pool Train_and_Test/setup_pf_1_4_approval_loop/DOC5D_AVWAP_RECLAIM_LONG/pool_vA --trials 400 --time_budget_min 5.0 --seed 23 --gap_lambda 0.8 --max_mask_terms 1 --max_pm_terms 1 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```