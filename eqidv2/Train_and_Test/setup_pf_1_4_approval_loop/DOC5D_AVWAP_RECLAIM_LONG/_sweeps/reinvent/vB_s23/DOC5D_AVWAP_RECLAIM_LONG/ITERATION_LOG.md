# DOC5D_AVWAP_RECLAIM_LONG (LONG) — ITERATION_LOG

_Generated 2026-07-01. Optimizer: Optuna TPE. 400 FIT/VAL trials (each trial = one logical config: exit SL/Tgt + <=1 mask + <=1 pre-momentum + guards)._

Search protocol: optimise ONLY on FIT/VAL with the band objective `reward(min(FIT_PF,VAL_PF)) - 0.80*|FIT_PF-VAL_PF|`, where reward tents at PF 1.70 and penalises overshoot (anti-overfit). The single best FIT/VAL config is then confirmed on full TRAIN and scored ONCE on TEST. Quantile thresholds drawn from TRAIN only (never TEST). The robust gate then checks TEST PF+day-block p, TRAIN target-fill, threshold-neighborhood stability, and term-dropout stability.

Exit grid searched: SL [0.5, 0.7, 0.85, 1.0, 1.1, 1.2, 1.5] x Tgt [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5] (small/large SL x small/large target, asymmetric combos). Mask feats: ['atr_pct<=0.003545', 'atr_pct>=0.002916', 'atr_pct>=0.003545', 'atr_pct>=0.00597', 'body_pct>=0.716767', 'body_pct>=0.751953', 'body_pct>=0.861364', 'body_pct>=0.90813', 'close_loc<=1.0', 'close_loc>=0.75', 'close_loc>=0.891624', 'close_loc>=1.0'].

## Top 20 FIT/VAL trials (by band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|-----|------|--------|-------|----------|----------|-------|
| 1 | 0.7 | 2.5 | vwap_dist_atr>=1.027686 | - | {"min_slot": "10:00", "max_slot": "14:00"} | 16/1.341 | 13/1.316 | 1.3344 |
| 2 | 0.7 | 2.5 | vwap_dist_atr>=1.027686 | - | {"min_slot": "10:00", "max_slot": "13:00"} | 16/1.341 | 13/1.316 | 1.3344 |
| 3 | 0.7 | 2.5 | vwap_dist_atr>=1.027686 | - | {"min_slot": "10:00", "max_slot": "14:00"} | 16/1.341 | 13/1.316 | 1.3344 |
| 4 | 0.7 | 2.5 | vwap_dist_atr>=1.027686 | - | {"min_slot": "10:00", "max_slot": "14:00"} | 16/1.341 | 13/1.316 | 1.3344 |
| 5 | 0.7 | 2.5 | vwap_dist_atr>=1.027686 | - | {"min_slot": "10:00", "max_slot": "14:00"} | 16/1.341 | 13/1.316 | 1.3344 |
| 6 | 0.7 | 2.5 | vwap_dist_atr>=1.027686 | - | {"min_slot": "10:00", "max_slot": "14:00"} | 16/1.341 | 13/1.316 | 1.3344 |
| 7 | 0.7 | 2.5 | vwap_dist_atr>=1.027686 | - | {"min_slot": "10:00", "max_slot": "14:00"} | 16/1.341 | 13/1.316 | 1.3344 |
| 8 | 0.7 | 2.5 | vwap_dist_atr>=1.027686 | - | {"min_slot": "10:00", "max_slot": "14:00", "top_n": 2} | 16/1.341 | 13/1.316 | 1.3344 |
| 9 | 0.7 | 2.5 | vwap_dist_atr>=1.027686 | - | {"min_slot": "10:00", "max_slot": "13:00"} | 16/1.341 | 13/1.316 | 1.3344 |
| 10 | 0.7 | 2.5 | vwap_dist_atr>=1.027686 | - | {"min_slot": "10:00", "max_slot": "14:30", "top_n": 2} | 16/1.341 | 13/1.316 | 1.3344 |
| 11 | 0.7 | 2.5 | vwap_dist_atr>=1.027686 | - | {"min_slot": "10:00", "max_slot": "14:00", "top_n": 2} | 16/1.341 | 13/1.316 | 1.3344 |
| 12 | 0.7 | 2.5 | vwap_dist_atr>=1.027686 | - | {"min_slot": "10:00", "max_slot": "14:00"} | 16/1.341 | 13/1.316 | 1.3344 |
| 13 | 0.7 | 2.5 | vwap_dist_atr>=1.027686 | - | {"min_slot": "10:00", "max_slot": "14:00"} | 16/1.341 | 13/1.316 | 1.3344 |
| 14 | 0.7 | 2.5 | vwap_dist_atr>=1.027686 | - | {"min_slot": "10:00", "max_slot": "14:00"} | 16/1.341 | 13/1.316 | 1.3344 |
| 15 | 0.7 | 2.5 | vwap_dist_atr>=1.027686 | - | {"min_slot": "10:00", "max_slot": "14:00"} | 16/1.341 | 13/1.316 | 1.3344 |
| 16 | 0.7 | 2.5 | vwap_dist_atr>=1.027686 | - | {"min_slot": "10:00", "max_slot": "14:00"} | 16/1.341 | 13/1.316 | 1.3344 |
| 17 | 0.7 | 2.5 | vwap_dist_atr>=1.027686 | - | {"min_slot": "10:00", "max_slot": "14:00"} | 16/1.341 | 13/1.316 | 1.3344 |
| 18 | 0.7 | 2.5 | vwap_dist_atr>=1.027686 | - | {"min_slot": "10:00", "max_slot": "14:00"} | 16/1.341 | 13/1.316 | 1.3344 |
| 19 | 0.7 | 2.5 | vwap_dist_atr>=1.027686 | - | {"min_slot": "10:00", "max_slot": "14:00"} | 16/1.341 | 13/1.316 | 1.3344 |
| 20 | 0.7 | 2.5 | vwap_dist_atr>=1.027686 | - | {"min_slot": "10:00", "max_slot": "14:00"} | 16/1.341 | 13/1.316 | 1.3344 |

## Best config — confirmation runs

- **Best (FIT/VAL):** SL 0.7/Tgt 2.5 | mask [vwap_dist_atr>=1.027686] | premom [(none)] | guard {'min_slot': '10:00', 'top_n': 2} | maxpos 20 | dloss 4000.0
- **TRAIN @15bps:** n=29 PF=1.329 net=Rs4,540 win%=34.5 avgW=Rs1,833 avgL=Rs-726 maxDD=Rs-5,735 SL/TGT/EOD=14/5/10 tgt%=17.2 tpd=1.81 tradeDom=0.129 dayDom=0.626 symDom=0.521 dbp=0.2666
- **TEST  @15bps:** n=9 PF=0.544 net=Rs-2,378 win%=22.2 avgW=Rs1,421 avgL=Rs-746 maxDD=Rs-2,488 SL/TGT/EOD=5/1/3 tgt%=11.1 tpd=2.25 tradeDom=0.832 dayDom=9.99 symDom=9.99 dbp=0.9483
- **TRAIN @5bps:**  n=29 PF=1.329 net=Rs4,540 win%=34.5 avgW=Rs1,833 avgL=Rs-726 maxDD=Rs-5,735 SL/TGT/EOD=14/5/10 tgt%=17.2 tpd=1.81 tradeDom=0.129 dayDom=0.626 symDom=0.521 dbp=0.2666
- **TEST  @5bps:**  n=9 PF=0.544 net=Rs-2,378 win%=22.2 avgW=Rs1,421 avgL=Rs-746 maxDD=Rs-2,488 SL/TGT/EOD=5/1/3 tgt%=11.1 tpd=2.25 tradeDom=0.832 dayDom=9.99 symDom=9.99 dbp=0.9483

## Robustness diagnostics

- neighborhood pass: `False` (PF floor 1.15)
- term-dropout pass: `False` (PF floor 1.0)
- hard reasons: `TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates)`
- insufficient reasons: `-`
- warnings: `-`

- **Keep/reject:** REJECT  — TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates)

## Command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup DOC5D_AVWAP_RECLAIM_LONG --pool Train_and_Test/setup_pf_1_4_approval_loop/DOC5D_AVWAP_RECLAIM_LONG/pool_vB --trials 400 --time_budget_min 5.0 --seed 23 --gap_lambda 0.8 --max_mask_terms 1 --max_pm_terms 1 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```