# A_MOD_BREAK_C1_HIGH (LONG) — ITERATION_LOG

_Generated 2026-07-03. Optimizer: Optuna TPE. 500 FIT/VAL trials (each trial = one logical config: exit SL/Tgt + <=2 mask + <=2 pre-momentum + guards)._

Search protocol: optimise ONLY on FIT/VAL with the band objective `reward(min(FIT_PF,VAL_PF)) - 0.80*|FIT_PF-VAL_PF|`, where reward tents at PF 1.70 and penalises overshoot (anti-overfit). The single best FIT/VAL config is then confirmed on full TRAIN and scored ONCE on TEST. Quantile thresholds drawn from TRAIN only (never TEST). The robust gate then checks TEST PF+day-block p, TRAIN target-fill, threshold-neighborhood stability, and term-dropout stability.

Exit grid searched: SL [0.5, 0.7, 0.85, 1.0, 1.1, 1.2, 1.5] x Tgt [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5] (small/large SL x small/large target, asymmetric combos). Mask feats: ['atr_pct<=0.00404', 'body_pct<=0.886746', 'body_pct>=0.94838', 'close_loc<=0.673464', 'close_loc>=0.896692', 'lower_wick_pct>=0.01529', 'quality_score<=116.884676', 'quality_score>=136.519949', 'rs_pct<=3.937139', 'rs_pct>=5.575286', 'signal_range_pct<=0.621083', 'signal_range_pct<=0.810784'].

## Top 20 FIT/VAL trials (by band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|-----|------|--------|-------|----------|----------|-------|
| 1 | 1.5 | 1.5 | - | pre3_close_pos<=0.666632 | {"min_slot": "11:00", "max_slot": "13:00", "top_n": 2} | 17/0.929 | 31/0.908 | 0.8911 |
| 2 | 1.5 | 1.5 | - | pre3_close_pos<=0.666632 | {"min_slot": "11:00", "max_slot": "13:00", "top_n": 2} | 17/0.929 | 31/0.908 | 0.8911 |
| 3 | 1.5 | 1.5 | - | pre3_close_pos<=0.666632 | {"max_slot": "14:30", "top_n": 2} | 19/0.817 | 39/0.77 | 0.7317 |
| 4 | 1.5 | 1.5 | - | pre3_close_pos<=0.666632 | {"max_slot": "14:30", "top_n": 2} | 19/0.817 | 39/0.77 | 0.7317 |
| 5 | 1.5 | 1.5 | - | pre3_close_pos<=0.666632 | {"max_slot": "14:30", "top_n": 2} | 19/0.817 | 39/0.77 | 0.7317 |
| 6 | 1.5 | 1.5 | - | pre3_close_pos<=0.666632 | {"max_slot": "14:30", "top_n": 2} | 19/0.817 | 39/0.77 | 0.7317 |
| 7 | 1.5 | 1.5 | - | pre3_close_pos<=0.666632 | {"max_slot": "14:30", "top_n": 2} | 19/0.817 | 39/0.77 | 0.7317 |
| 8 | 1.5 | 1.5 | - | pre3_close_pos<=0.666632 | {"max_slot": "14:30", "top_n": 2} | 19/0.817 | 39/0.77 | 0.7317 |
| 9 | 1.5 | 1.5 | - | pre3_close_pos<=0.666632 | {"max_slot": "14:30", "top_n": 2} | 19/0.817 | 39/0.77 | 0.7317 |
| 10 | 1.5 | 1.5 | - | pre3_close_pos<=0.666632 | {"max_slot": "14:30", "top_n": 2} | 19/0.817 | 39/0.77 | 0.7317 |
| 11 | 1.5 | 1.5 | - | pre3_close_pos<=0.666632 | {"max_slot": "14:30", "top_n": 2} | 19/0.817 | 39/0.77 | 0.7317 |
| 12 | 1.5 | 1.5 | - | pre3_close_pos<=0.666632 | {"max_slot": "14:30", "top_n": 2} | 19/0.817 | 39/0.77 | 0.7317 |
| 13 | 1.5 | 1.5 | - | pre3_close_pos<=0.666632 | {"max_slot": "14:30", "top_n": 2} | 19/0.817 | 39/0.77 | 0.7317 |
| 14 | 1.5 | 1.5 | - | pre3_close_pos<=0.666632 | {"max_slot": "14:30", "top_n": 2} | 19/0.817 | 39/0.77 | 0.7317 |
| 15 | 1.5 | 1.5 | - | pre3_close_pos<=0.666632 | {"max_slot": "14:30", "top_n": 2} | 19/0.817 | 39/0.77 | 0.7317 |
| 16 | 1.5 | 1.5 | - | pre3_close_pos<=0.666632 | {"max_slot": "14:30", "top_n": 2} | 19/0.817 | 39/0.77 | 0.7317 |
| 17 | 1.5 | 1.5 | - | pre3_close_pos<=0.666632 | {"max_slot": "14:30", "top_n": 2} | 19/0.817 | 39/0.77 | 0.7317 |
| 18 | 1.5 | 1.5 | - | pre3_close_pos<=0.666632 | {"max_slot": "14:30", "top_n": 2} | 19/0.817 | 39/0.77 | 0.7317 |
| 19 | 1.5 | 1.5 | - | pre3_close_pos<=0.666632 | {"max_slot": "14:30", "top_n": 2} | 19/0.817 | 39/0.77 | 0.7317 |
| 20 | 1.5 | 1.5 | - | pre3_close_pos<=0.666632 | {"max_slot": "14:30", "top_n": 2} | 19/0.817 | 39/0.77 | 0.7317 |

## Best config — confirmation runs

- **Best (FIT/VAL):** SL 1.5/Tgt 1.5 | mask [(none)] | premom [pre3_close_pos<=0.666632] | guard {'min_slot': '11:00', 'max_slot': '13:00', 'top_n': 2} | maxpos 20 | dloss 4000.0
- **TRAIN @15bps:** n=48 PF=0.916 net=Rs-2,791 win%=52.1 avgW=Rs1,214 avgL=Rs-1,441 maxDD=Rs-8,966 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.78 tradeDom=0.042 dayDom=9.99 symDom=9.99 dbp=0.6239
- **TEST  @15bps:** n=31 PF=0.208 net=Rs-26,801 win%=22.6 avgW=Rs1,004 avgL=Rs-1,409 maxDD=Rs-27,343 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.82 tradeDom=0.18 dayDom=9.99 symDom=9.99 dbp=0.9969
- **TRAIN @5bps:**  n=48 PF=0.916 net=Rs-2,791 win%=52.1 avgW=Rs1,214 avgL=Rs-1,441 maxDD=Rs-8,966 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.78 tradeDom=0.042 dayDom=9.99 symDom=9.99 dbp=0.6239
- **TEST  @5bps:**  n=31 PF=0.208 net=Rs-26,801 win%=22.6 avgW=Rs1,004 avgL=Rs-1,409 maxDD=Rs-27,343 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.82 tradeDom=0.18 dayDom=9.99 symDom=9.99 dbp=0.9969

## Robustness diagnostics

- neighborhood pass: `False` (PF floor 1.15)
- term-dropout pass: `False` (PF floor 1.0)
- hard reasons: `TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates)`
- insufficient reasons: `-`
- warnings: `-`

- **Keep/reject:** REJECT  — TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates)

## Command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup A_MOD_BREAK_C1_HIGH --pool Train_and_Test/setup_pf_1_4_full_loop/A_MOD_BREAK_C1_HIGH/pools/pool_morning --trials 500 --time_budget_min 12.0 --seed 7 --gap_lambda 0.8 --max_mask_terms 2 --max_pm_terms 2 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```