# DOC5D_AVWAP_RECLAIM_LONG (LONG) — ITERATION_LOG

_Generated 2026-07-01. Optimizer: Optuna TPE. 450 FIT/VAL trials (each trial = one logical config: exit SL/Tgt + <=2 mask + <=1 pre-momentum + guards)._

Search protocol: optimise ONLY on FIT/VAL with the band objective `reward(min(FIT_PF,VAL_PF)) - 0.80*|FIT_PF-VAL_PF|`, where reward tents at PF 1.70 and penalises overshoot (anti-overfit). The single best FIT/VAL config is then confirmed on full TRAIN and scored ONCE on TEST. Quantile thresholds drawn from TRAIN only (never TEST). The robust gate then checks TEST PF+day-block p, TRAIN target-fill, threshold-neighborhood stability, and term-dropout stability.

Exit grid searched: SL [0.5, 0.7, 0.85, 1.0, 1.1, 1.2, 1.5] x Tgt [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5] (small/large SL x small/large target, asymmetric combos). Mask feats: ['atr_pct<=0.007983', 'atr_pct>=0.003545', 'body_pct<=0.783946', 'body_pct>=0.638545', 'close_loc>=0.702222', 'close_loc>=0.93375', 'lower_wick_pct<=0.0', 'lower_wick_pct<=0.006597', 'lower_wick_pct<=0.063162', 'lower_wick_pct<=0.090029', 'lower_wick_pct>=0.041682', 'quality_score<=88.874882'].

## Top 20 FIT/VAL trials (by band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|-----|------|--------|-------|----------|----------|-------|
| 1 | 0.85 | 2.0 | - | sig5_adx_calc<=15.935959 | {"min_slot": "10:30", "max_slot": "14:00", "top_n": 3} | 6/0.962 | 14/0.904 | 0.8585 |
| 2 | 1.1 | 1.25 | - | - | {"max_slot": "14:00", "top_n": 2} | 60/0.713 | 82/0.712 | 0.7118 |
| 3 | 1.1 | 1.25 | - | - | {"max_slot": "14:00", "top_n": 2} | 60/0.713 | 82/0.712 | 0.7118 |
| 4 | 1.1 | 1.25 | - | - | {"max_slot": "14:30", "top_n": 2} | 60/0.713 | 82/0.712 | 0.7118 |
| 5 | 1.1 | 1.25 | - | - | {"max_slot": "14:00", "top_n": 2} | 60/0.713 | 82/0.712 | 0.7118 |
| 6 | 1.1 | 1.25 | - | - | {"max_slot": "14:00", "top_n": 2} | 60/0.713 | 82/0.712 | 0.7118 |
| 7 | 1.1 | 1.25 | - | - | {"min_slot": "09:30", "max_slot": "14:00", "top_n": 2} | 60/0.713 | 82/0.712 | 0.7118 |
| 8 | 1.1 | 1.25 | - | - | {"max_slot": "13:00", "top_n": 2} | 60/0.713 | 82/0.712 | 0.7118 |
| 9 | 1.1 | 1.25 | - | - | {"max_slot": "13:00", "top_n": 2} | 60/0.713 | 82/0.712 | 0.7118 |
| 10 | 1.1 | 1.25 | - | - | {"max_slot": "14:30", "top_n": 2} | 60/0.713 | 82/0.712 | 0.7118 |
| 11 | 1.1 | 1.25 | - | - | {"max_slot": "14:00", "top_n": 2} | 60/0.713 | 82/0.712 | 0.7118 |
| 12 | 1.1 | 1.25 | - | - | {"max_slot": "14:00", "top_n": 2} | 60/0.713 | 82/0.712 | 0.7118 |
| 13 | 1.1 | 1.25 | - | - | {"max_slot": "14:00", "top_n": 2} | 60/0.713 | 82/0.712 | 0.7118 |
| 14 | 1.1 | 1.25 | - | - | {"max_slot": "13:00", "top_n": 2} | 60/0.713 | 82/0.712 | 0.7118 |
| 15 | 1.1 | 1.25 | - | - | {"max_slot": "14:00", "top_n": 2} | 60/0.713 | 82/0.712 | 0.7118 |
| 16 | 1.1 | 1.25 | - | - | {"max_slot": "14:00", "top_n": 2} | 60/0.713 | 82/0.712 | 0.7118 |
| 17 | 1.1 | 1.25 | - | - | {"max_slot": "14:00", "top_n": 2} | 60/0.713 | 82/0.712 | 0.7118 |
| 18 | 1.1 | 1.25 | - | - | {"max_slot": "14:00", "top_n": 2} | 60/0.713 | 82/0.712 | 0.7118 |
| 19 | 1.1 | 1.25 | - | - | {"max_slot": "14:00", "top_n": 2} | 60/0.713 | 82/0.712 | 0.7118 |
| 20 | 1.1 | 1.25 | - | - | {"max_slot": "14:30", "top_n": 2} | 60/0.713 | 82/0.712 | 0.7118 |

## Best config — confirmation runs

- **Best (FIT/VAL):** SL 0.85/Tgt 2.0 | mask [(none)] | premom [sig5_adx_calc<=15.935959] | guard {'min_slot': '10:30', 'max_slot': '14:00', 'top_n': 3} | maxpos 20 | dloss 4000.0
- **TRAIN @15bps:** n=20 PF=0.92 net=Rs-659 win%=45.0 avgW=Rs842 avgL=Rs-749 maxDD=Rs-3,042 SL/TGT/EOD=7/2/11 tgt%=10.0 tpd=2.5 tradeDom=0.246 dayDom=9.99 symDom=9.99 dbp=0.5994
- **TEST  @15bps:** n=6 PF=1.825 net=Rs1,340 win%=50.0 avgW=Rs988 avgL=Rs-541 maxDD=Rs-982 SL/TGT/EOD=1/1/4 tgt%=16.7 tpd=1.5 tradeDom=0.63 dayDom=1.356 symDom=1.394 dbp=0.2968
- **TRAIN @5bps:**  n=20 PF=0.92 net=Rs-659 win%=45.0 avgW=Rs842 avgL=Rs-749 maxDD=Rs-3,042 SL/TGT/EOD=7/2/11 tgt%=10.0 tpd=2.5 tradeDom=0.246 dayDom=9.99 symDom=9.99 dbp=0.5994
- **TEST  @5bps:**  n=6 PF=1.825 net=Rs1,340 win%=50.0 avgW=Rs988 avgL=Rs-541 maxDD=Rs-982 SL/TGT/EOD=1/1/4 tgt%=16.7 tpd=1.5 tradeDom=0.63 dayDom=1.356 symDom=1.394 dbp=0.2968

## Robustness diagnostics

- neighborhood pass: `False` (PF floor 1.15)
- term-dropout pass: `False` (PF floor 1.0)
- hard reasons: `TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates)`
- insufficient reasons: `-`
- warnings: `-`

- **Keep/reject:** REJECT  — TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates)

## Command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup DOC5D_AVWAP_RECLAIM_LONG --pool Train_and_Test/setup_pf_1_4_approval_loop/DOC5D_AVWAP_RECLAIM_LONG/pool_vB --trials 450 --time_budget_min 6.0 --seed 7 --gap_lambda 0.8 --max_mask_terms 2 --max_pm_terms 1 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```