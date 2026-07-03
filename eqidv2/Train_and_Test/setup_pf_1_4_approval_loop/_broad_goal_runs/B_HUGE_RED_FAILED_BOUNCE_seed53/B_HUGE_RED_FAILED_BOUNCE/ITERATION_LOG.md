# B_HUGE_RED_FAILED_BOUNCE (SHORT) — ITERATION_LOG

_Generated 2026-07-01. Optimizer: Optuna TPE. 700 FIT/VAL trials (each trial = one logical config: exit SL/Tgt + <=2 mask + <=2 pre-momentum + guards)._

Search protocol: optimise ONLY on FIT/VAL with the band objective `reward(min(FIT_PF,VAL_PF)) - 0.80*|FIT_PF-VAL_PF|`, where reward tents at PF 1.70 and penalises overshoot (anti-overfit). The single best FIT/VAL config is then confirmed on full TRAIN and scored ONCE on TEST. Quantile thresholds drawn from TRAIN only (never TEST). The robust gate then checks TEST PF+day-block p, TRAIN target-fill, threshold-neighborhood stability, and term-dropout stability.

Exit grid searched: SL [0.5, 0.7, 0.85, 1.0, 1.1, 1.2, 1.5] x Tgt [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5] (small/large SL x small/large target, asymmetric combos). Mask feats: ['atr_pct<=0.002094', 'atr_pct<=0.002574', 'atr_pct<=0.002824', 'atr_pct<=0.00572', 'body_pct<=0.615385', 'body_pct<=0.655408', 'body_pct<=0.75', 'body_pct<=0.784169', 'body_pct<=0.957647', 'close_loc<=0.135749', 'close_loc<=0.167232', 'close_loc<=0.316291'].

## Top 20 FIT/VAL trials (by band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|-----|------|--------|-------|----------|----------|-------|
| 1 | 1.0 | 2.5 | signal_range_pct<=0.523252 | pre_entry_momentum_score>=69.370728 | {"min_slot": "10:30", "max_slot": "14:00", "top_n": 3} | 7/1.195 | 11/1.08 | 0.9876 |
| 2 | 1.0 | 2.5 | signal_range_pct<=0.523252 | pre_entry_momentum_score>=69.370728 | {"min_slot": "10:30", "max_slot": "14:00", "top_n": 3} | 7/1.195 | 11/1.08 | 0.9876 |
| 3 | 1.0 | 2.5 | signal_range_pct<=0.523252 | pre_entry_momentum_score>=69.370728 | {"min_slot": "10:30", "max_slot": "14:00", "top_n": 3} | 7/1.195 | 11/1.08 | 0.9876 |
| 4 | 1.0 | 2.5 | signal_range_pct<=0.523252 | pre_entry_momentum_score>=69.370728 | {"min_slot": "10:30", "max_slot": "14:00", "top_n": 3} | 7/1.195 | 11/1.08 | 0.9876 |
| 5 | 1.0 | 2.5 | signal_range_pct<=0.523252 | pre_entry_momentum_score>=69.370728 | {"min_slot": "10:30", "max_slot": "14:00", "top_n": 3} | 7/1.195 | 11/1.08 | 0.9876 |
| 6 | 1.0 | 2.5 | signal_range_pct<=0.523252 | pre_entry_momentum_score>=69.370728 | {"min_slot": "10:30", "max_slot": "14:00", "top_n": 3} | 7/1.195 | 11/1.08 | 0.9876 |
| 7 | 1.0 | 2.5 | signal_range_pct<=0.523252 | pre_entry_momentum_score>=69.370728 | {"min_slot": "10:30", "max_slot": "14:00", "top_n": 3} | 7/1.195 | 11/1.08 | 0.9876 |
| 8 | 1.0 | 2.5 | signal_range_pct<=0.523252 | pre_entry_momentum_score>=69.370728 | {"min_slot": "10:30", "max_slot": "14:00", "top_n": 3} | 7/1.195 | 11/1.08 | 0.9876 |
| 9 | 1.0 | 2.5 | signal_range_pct<=0.523252 | pre_entry_momentum_score>=69.370728 | {"min_slot": "10:30", "max_slot": "14:00", "top_n": 3} | 7/1.195 | 11/1.08 | 0.9876 |
| 10 | 1.0 | 2.5 | signal_range_pct<=0.523252 | pre_entry_momentum_score>=69.370728 | {"min_slot": "10:30", "max_slot": "14:00", "top_n": 3} | 7/1.195 | 11/1.08 | 0.9876 |
| 11 | 1.0 | 2.5 | signal_range_pct<=0.523252 | pre_entry_momentum_score>=69.370728 | {"min_slot": "10:30", "max_slot": "14:00", "top_n": 3} | 7/1.195 | 11/1.08 | 0.9876 |
| 12 | 1.0 | 2.5 | signal_range_pct<=0.523252 | pre_entry_momentum_score>=69.370728 | {"min_slot": "10:00", "max_slot": "14:00", "top_n": 3} | 7/1.195 | 11/1.08 | 0.9876 |
| 13 | 1.0 | 2.5 | signal_range_pct<=0.523252 | pre_entry_momentum_score>=69.370728 | {"min_slot": "10:00", "max_slot": "14:00", "top_n": 3} | 7/1.195 | 11/1.08 | 0.9876 |
| 14 | 1.0 | 2.5 | signal_range_pct<=0.523252 | pre_entry_momentum_score>=69.370728 | {"min_slot": "10:30", "max_slot": "14:00", "top_n": 3} | 7/1.195 | 11/1.08 | 0.9876 |
| 15 | 1.0 | 2.5 | signal_range_pct<=0.523252 | pre_entry_momentum_score>=69.370728 | {"min_slot": "10:30", "max_slot": "14:00", "top_n": 3} | 7/1.195 | 11/1.08 | 0.9876 |
| 16 | 1.0 | 2.5 | signal_range_pct<=0.523252 | pre_entry_momentum_score>=69.370728 | {"min_slot": "10:30", "max_slot": "14:00", "top_n": 3} | 7/1.195 | 11/1.08 | 0.9876 |
| 17 | 1.0 | 2.5 | signal_range_pct<=0.523252 | pre_entry_momentum_score>=69.370728 | {"max_slot": "14:00", "top_n": 3} | 7/1.195 | 11/1.08 | 0.9876 |
| 18 | 1.0 | 2.5 | signal_range_pct<=0.523252 | pre_entry_momentum_score>=69.370728 | {"min_slot": "10:30", "max_slot": "14:00", "top_n": 3} | 7/1.195 | 11/1.08 | 0.9876 |
| 19 | 1.0 | 2.5 | signal_range_pct<=0.523252 | pre_entry_momentum_score>=69.370728 | {"min_slot": "10:30", "max_slot": "14:00", "top_n": 3} | 7/1.195 | 11/1.08 | 0.9876 |
| 20 | 1.0 | 2.5 | signal_range_pct<=0.523252 | pre_entry_momentum_score>=69.370728 | {"min_slot": "10:30", "max_slot": "14:00", "top_n": 3} | 7/1.195 | 11/1.08 | 0.9876 |

## Best config — confirmation runs

- **Best (FIT/VAL):** SL 1.0/Tgt 2.5 | mask [signal_range_pct<=0.523252] | premom [pre_entry_momentum_score>=69.370728] | guard {'min_slot': '10:00', 'max_slot': '14:00', 'top_n': 3} | maxpos 20 | dloss 0.0
- **TRAIN @15bps:** n=18 PF=1.113 net=Rs970 win%=44.4 avgW=Rs1,192 avgL=Rs-857 maxDD=Rs-4,379 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.64 tradeDom=0.238 dayDom=3.737 symDom=2.338 dbp=0.4466
- **TEST  @15bps:** n=6 PF=0.039 net=Rs-4,251 win%=33.3 avgW=Rs87 avgL=Rs-1,106 maxDD=Rs-3,149 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.0 tradeDom=0.739 dayDom=9.99 symDom=9.99 dbp=0.9624
- **TRAIN @5bps:**  n=18 PF=1.113 net=Rs970 win%=44.4 avgW=Rs1,192 avgL=Rs-857 maxDD=Rs-4,379 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.64 tradeDom=0.238 dayDom=3.737 symDom=2.338 dbp=0.4466
- **TEST  @5bps:**  n=6 PF=0.039 net=Rs-4,251 win%=33.3 avgW=Rs87 avgL=Rs-1,106 maxDD=Rs-3,149 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.0 tradeDom=0.739 dayDom=9.99 symDom=9.99 dbp=0.9624

## Robustness diagnostics

- neighborhood pass: `False` (PF floor 1.15)
- term-dropout pass: `False` (PF floor 1.0)
- hard reasons: `TRAIN too few trades (train_n<20); TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates)`
- insufficient reasons: `-`
- warnings: `-`

- **Keep/reject:** REJECT  — TRAIN too few trades (train_n<20); TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates)

## Command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup B_HUGE_RED_FAILED_BOUNCE --pool C:\TradingData\eqidv2\setup_pools_2026_06_29\B_HUGE_RED_FAILED_BOUNCE --trials 700 --time_budget_min 10.0 --seed 53 --gap_lambda 0.8 --max_mask_terms 2 --max_pm_terms 2 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```