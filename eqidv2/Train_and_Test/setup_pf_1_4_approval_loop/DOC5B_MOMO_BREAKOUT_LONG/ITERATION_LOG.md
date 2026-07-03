# DOC5B_MOMO_BREAKOUT_LONG (LONG) — ITERATION_LOG

_Generated 2026-07-01. Optimizer: Optuna TPE. 200 FIT/VAL trials (each trial = one logical config: exit SL/Tgt + <=1 mask + <=1 pre-momentum + guards)._

Search protocol: optimise ONLY on FIT/VAL with the band objective `reward(min(FIT_PF,VAL_PF)) - 0.80*|FIT_PF-VAL_PF|`, where reward tents at PF 1.70 and penalises overshoot (anti-overfit). The single best FIT/VAL config is then confirmed on full TRAIN and scored ONCE on TEST. Quantile thresholds drawn from TRAIN only (never TEST). The robust gate then checks TEST PF+day-block p, TRAIN target-fill, threshold-neighborhood stability, and term-dropout stability.

Exit grid searched: SL [0.5, 0.7, 0.85, 1.0, 1.1, 1.2, 1.5] x Tgt [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5] (small/large SL x small/large target, asymmetric combos). Mask feats: ['atr_pct<=0.002187', 'atr_pct<=0.002527', 'atr_pct<=0.002873', 'atr_pct<=0.003378', 'body_pct<=0.67256', 'body_pct<=0.815931', 'body_pct>=0.61087', 'body_pct>=0.868499', 'close_loc<=0.712729', 'close_loc<=0.754585', 'close_loc<=0.833333', 'close_loc<=1.0'].

## Top 20 FIT/VAL trials (by band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|-----|------|--------|-------|----------|----------|-------|
| 1 | 1.5 | 2.0 | signal_range_pct<=0.388188 | - | {"max_slot": "12:30", "top_n": 2} | 64/0.477 | 61/0.501 | 0.4573 |
| 2 | 1.5 | 2.0 | signal_range_pct<=0.388188 | - | {"top_n": 2} | 86/0.457 | 79/0.47 | 0.4476 |
| 3 | 1.5 | 2.0 | signal_range_pct<=0.388188 | - | {"max_slot": "14:30", "top_n": 2} | 86/0.457 | 79/0.47 | 0.4476 |
| 4 | 1.5 | 2.0 | signal_range_pct<=0.388188 | - | {"max_slot": "14:30", "top_n": 2} | 86/0.457 | 79/0.47 | 0.4476 |
| 5 | 1.5 | 2.0 | signal_range_pct<=0.388188 | - | {"max_slot": "14:30", "top_n": 2} | 86/0.457 | 79/0.47 | 0.4476 |
| 6 | 1.5 | 2.0 | signal_range_pct<=0.388188 | - | {"max_slot": "14:30", "top_n": 2} | 86/0.457 | 79/0.47 | 0.4476 |
| 7 | 1.5 | 2.0 | signal_range_pct<=0.388188 | - | {"max_slot": "14:30", "top_n": 2} | 86/0.457 | 79/0.47 | 0.4476 |
| 8 | 1.5 | 2.0 | signal_range_pct<=0.388188 | - | {"max_slot": "14:30", "top_n": 2} | 86/0.457 | 79/0.47 | 0.4476 |
| 9 | 1.5 | 2.0 | signal_range_pct<=0.388188 | - | {"max_slot": "14:30", "top_n": 2} | 86/0.457 | 79/0.47 | 0.4476 |
| 10 | 1.5 | 2.0 | signal_range_pct<=0.388188 | - | {"max_slot": "14:30", "top_n": 2} | 86/0.457 | 79/0.47 | 0.4476 |
| 11 | 1.5 | 2.0 | signal_range_pct<=0.388188 | - | {"max_slot": "14:30", "top_n": 2} | 86/0.457 | 79/0.47 | 0.4476 |
| 12 | 1.5 | 2.0 | signal_range_pct<=0.388188 | - | {"max_slot": "14:30", "top_n": 2} | 86/0.457 | 79/0.47 | 0.4476 |
| 13 | 1.5 | 2.0 | signal_range_pct<=0.388188 | - | {"max_slot": "14:30", "top_n": 2} | 86/0.457 | 79/0.47 | 0.4476 |
| 14 | 1.5 | 2.0 | signal_range_pct<=0.388188 | - | {"max_slot": "14:30", "top_n": 2} | 86/0.457 | 79/0.47 | 0.4476 |
| 15 | 1.5 | 2.0 | signal_range_pct<=0.388188 | - | {"max_slot": "14:30", "top_n": 2} | 86/0.457 | 79/0.47 | 0.4476 |
| 16 | 1.5 | 2.0 | signal_range_pct<=0.388188 | - | {"max_slot": "14:30", "top_n": 2} | 86/0.457 | 79/0.47 | 0.4476 |
| 17 | 1.5 | 2.0 | signal_range_pct<=0.388188 | - | {"max_slot": "14:30", "top_n": 2} | 86/0.457 | 79/0.47 | 0.4476 |
| 18 | 1.5 | 2.0 | signal_range_pct<=0.388188 | - | {"max_slot": "14:30", "top_n": 2} | 86/0.457 | 79/0.47 | 0.4476 |
| 19 | 1.5 | 2.0 | signal_range_pct<=0.388188 | - | {"max_slot": "14:30", "top_n": 2} | 86/0.457 | 79/0.47 | 0.4476 |
| 20 | 1.5 | 2.0 | signal_range_pct<=0.388188 | - | {"max_slot": "14:30", "top_n": 2} | 86/0.457 | 79/0.47 | 0.4476 |

## Best config — confirmation runs

- **Best (FIT/VAL):** SL 1.5/Tgt 2.0 | mask [signal_range_pct<=0.388188] | premom [(none)] | guard {'max_slot': '12:30', 'top_n': 2} | maxpos 20 | dloss 4000.0
- **TRAIN @15bps:** n=125 PF=0.488 net=Rs-37,044 win%=36.0 avgW=Rs786 avgL=Rs-905 maxDD=Rs-39,690 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=6.25 tradeDom=0.05 dayDom=9.99 symDom=9.99 dbp=0.9818
- **TEST  @15bps:** n=48 PF=0.211 net=Rs-38,729 win%=22.9 avgW=Rs941 avgL=Rs-1,326 maxDD=Rs-37,509 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=8.0 tradeDom=0.171 dayDom=9.99 symDom=9.99 dbp=0.9864
- **TRAIN @5bps:**  n=125 PF=0.488 net=Rs-37,044 win%=36.0 avgW=Rs786 avgL=Rs-905 maxDD=Rs-39,690 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=6.25 tradeDom=0.05 dayDom=9.99 symDom=9.99 dbp=0.9818
- **TEST  @5bps:**  n=48 PF=0.211 net=Rs-38,729 win%=22.9 avgW=Rs941 avgL=Rs-1,326 maxDD=Rs-37,509 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=8.0 tradeDom=0.171 dayDom=9.99 symDom=9.99 dbp=0.9864

## Robustness diagnostics

- neighborhood pass: `False` (PF floor 1.15)
- term-dropout pass: `False` (PF floor 1.0)
- hard reasons: `TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); TRAIN too many trades/day; neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates); TEST too many trades/day`
- insufficient reasons: `-`
- warnings: `-`

- **Keep/reject:** REJECT  — TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); TRAIN too many trades/day; neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates); TEST too many trades/day

## Command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup DOC5B_MOMO_BREAKOUT_LONG --pool Train_and_Test\doc5_long_setups\pool --trials 200 --time_budget_min 10.0 --seed 7 --gap_lambda 0.8 --max_mask_terms 1 --max_pm_terms 1 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```