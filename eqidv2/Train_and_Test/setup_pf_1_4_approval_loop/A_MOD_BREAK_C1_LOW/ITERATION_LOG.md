# A_MOD_BREAK_C1_LOW (SHORT) — ITERATION_LOG

_Generated 2026-07-01. Optimizer: Optuna TPE. 220 FIT/VAL trials (each trial = one logical config: exit SL/Tgt + <=2 mask + <=2 pre-momentum + guards)._

Search protocol: optimise ONLY on FIT/VAL with the band objective `reward(min(FIT_PF,VAL_PF)) - 0.40*|FIT_PF-VAL_PF|`, where reward tents at PF 1.70 and penalises overshoot (anti-overfit). The single best FIT/VAL config is then confirmed on full TRAIN and scored ONCE on TEST. Quantile thresholds drawn from TRAIN only (never TEST).

Exit grid searched: SL [0.5, 0.7, 0.85, 1.0, 1.1, 1.2, 1.5] x Tgt [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5] (small/large SL x small/large target, asymmetric combos). Mask feats: ['atr_pct<=0.001522', 'atr_pct<=0.002381', 'atr_pct<=0.002614', 'body_pct<=0.9', 'body_pct>=0.540541', 'body_pct>=0.744186', 'body_pct>=0.9', 'body_pct>=0.976744', 'close_loc<=0.324324', 'close_loc>=0.21978', 'lower_wick_pct>=0.008422', 'lower_wick_pct>=0.024205'].

## Top 20 FIT/VAL trials (by band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|-----|------|--------|-------|----------|----------|-------|
| 1 | 0.7 | 2.5 | - | pre_entry_momentum_score>=38.397393;pre3_close_pos<=0.300039 | {"max_slot": "13:00", "top_n": 3} | 40/1.396 | 18/1.292 | 1.2509 |
| 2 | 0.7 | 2.5 | - | pre_entry_momentum_score>=38.397393;pre3_close_pos<=0.300039 | {"max_slot": "13:00", "top_n": 3} | 40/1.396 | 18/1.292 | 1.2509 |
| 3 | 0.7 | 2.5 | - | pre_entry_momentum_score>=38.397393;pre3_close_pos<=0.300039 | {"max_slot": "13:00", "top_n": 3} | 40/1.396 | 18/1.292 | 1.2509 |
| 4 | 0.7 | 2.5 | - | pre_entry_momentum_score>=38.397393;pre3_close_pos<=0.300039 | {"max_slot": "13:00", "top_n": 3} | 40/1.396 | 18/1.292 | 1.2509 |
| 5 | 0.7 | 2.5 | - | pre_entry_momentum_score>=38.397393;pre3_close_pos<=0.300039 | {"max_slot": "13:00", "top_n": 3} | 40/1.396 | 18/1.292 | 1.2509 |
| 6 | 0.7 | 2.5 | - | pre_entry_momentum_score>=38.397393;pre3_close_pos<=0.300039 | {"max_slot": "13:00", "top_n": 3} | 40/1.396 | 18/1.292 | 1.2509 |
| 7 | 0.7 | 2.5 | - | pre_entry_momentum_score>=38.397393;pre3_close_pos<=0.300039 | {"max_slot": "13:00", "top_n": 3} | 40/1.396 | 18/1.292 | 1.2509 |
| 8 | 0.7 | 2.5 | - | pre_entry_momentum_score>=38.397393;pre3_close_pos<=0.300039 | {"max_slot": "13:00", "top_n": 3} | 40/1.396 | 18/1.292 | 1.2509 |
| 9 | 0.7 | 2.5 | - | pre_entry_momentum_score>=38.397393;pre3_close_pos<=0.300039 | {"max_slot": "13:00", "top_n": 3} | 40/1.396 | 18/1.292 | 1.2509 |
| 10 | 0.7 | 2.5 | - | pre_entry_momentum_score>=38.397393;pre3_close_pos<=0.300039 | {"max_slot": "13:00", "top_n": 3} | 40/1.396 | 18/1.292 | 1.2509 |
| 11 | 0.7 | 2.5 | - | pre_entry_momentum_score>=38.397393;pre3_close_pos<=0.300039 | {"max_slot": "13:00", "top_n": 3} | 40/1.396 | 18/1.292 | 1.2509 |
| 12 | 0.7 | 2.5 | - | pre_entry_momentum_score>=38.397393;pre3_close_pos<=0.300039 | {"max_slot": "13:00", "top_n": 3} | 40/1.396 | 18/1.292 | 1.2509 |
| 13 | 0.7 | 2.5 | - | pre_entry_momentum_score>=38.397393;pre3_close_pos<=0.300039 | {"max_slot": "13:00", "top_n": 3} | 40/1.396 | 18/1.292 | 1.2509 |
| 14 | 0.7 | 2.5 | - | pre_entry_momentum_score>=38.397393;pre3_close_pos<=0.300039 | {"max_slot": "13:00", "top_n": 3} | 40/1.396 | 18/1.292 | 1.2509 |
| 15 | 0.7 | 2.5 | - | pre_entry_momentum_score>=38.397393;pre3_close_pos<=0.300039 | {"max_slot": "13:00", "top_n": 3} | 40/1.396 | 18/1.292 | 1.2509 |
| 16 | 0.7 | 2.5 | - | pre_entry_momentum_score>=38.397393;pre3_close_pos<=0.300039 | {"max_slot": "13:00", "top_n": 3} | 40/1.396 | 18/1.292 | 1.2509 |
| 17 | 0.7 | 2.5 | - | pre_entry_momentum_score>=38.397393;pre3_close_pos<=0.300039 | {"max_slot": "13:00", "top_n": 3} | 40/1.396 | 18/1.292 | 1.2509 |
| 18 | 0.7 | 2.5 | - | pre_entry_momentum_score>=38.397393;pre3_close_pos<=0.300039 | {"max_slot": "13:00", "top_n": 3} | 40/1.396 | 18/1.292 | 1.2509 |
| 19 | 0.7 | 2.5 | - | pre_entry_momentum_score>=38.397393;pre3_close_pos<=0.300039 | {"max_slot": "13:00", "top_n": 3} | 40/1.396 | 18/1.292 | 1.2509 |
| 20 | 0.7 | 2.5 | - | pre_entry_momentum_score>=38.397393;pre3_close_pos<=0.300039 | {"max_slot": "13:00", "top_n": 3} | 40/1.396 | 18/1.292 | 1.2509 |

## Best config — confirmation runs

- **Best (FIT/VAL):** SL 0.7/Tgt 2.5 | mask [(none)] | premom [pre_entry_momentum_score>=38.397393; pre3_close_pos<=0.300039] | guard {'max_slot': '13:00', 'top_n': 3} | maxpos 10 | dloss 4000.0
- **TRAIN @15bps:** n=58 PF=1.36 net=Rs7,119 win%=53.4 avgW=Rs867 avgL=Rs-732 maxDD=Rs-4,546 SL/TGT/EOD=19/4/35 tpd=4.83 tradeDom=0.084 dayDom=0.498 symDom=0.318 dbp=0.1884
- **TEST  @15bps:** n=13 PF=0.542 net=Rs-2,713 win%=38.5 avgW=Rs642 avgL=Rs-740 maxDD=Rs-2,061 SL/TGT/EOD=6/0/7 tpd=2.6 tradeDom=0.347 dayDom=9.99 symDom=9.99 dbp=0.9886
- **TRAIN @5bps:**  n=58 PF=1.747 net=Rs12,838 win%=56.9 avgW=Rs910 avgL=Rs-688 maxDD=Rs-4,056 SL/TGT/EOD=19/4/35 tpd=4.83 tradeDom=0.079 dayDom=0.315 symDom=0.184 dbp=0.0662
- **TEST  @5bps:**  n=13 PF=0.718 net=Rs-1,455 win%=38.5 avgW=Rs740 avgL=Rs-644 maxDD=Rs-1,411 SL/TGT/EOD=6/0/7 tpd=2.6 tradeDom=0.327 dayDom=9.99 symDom=9.99 dbp=0.8737

- **Keep/reject:** REJECT  — TEST PF below 1.40; TEST concentrated (one trade/day/symbol dominates)

## Command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup A_MOD_BREAK_C1_LOW --pool C:\TradingData\eqidv2\setup_pools_2026_06_29\A_MOD_BREAK_C1_LOW --trials 220 --time_budget_min 9.0 --seed 7
```