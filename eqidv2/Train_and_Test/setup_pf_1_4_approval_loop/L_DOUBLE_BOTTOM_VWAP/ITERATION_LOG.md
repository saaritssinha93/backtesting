# L_DOUBLE_BOTTOM_VWAP (LONG) — ITERATION_LOG

_Generated 2026-07-01. Optimizer: Optuna TPE. 220 FIT/VAL trials (each trial = one logical config: exit SL/Tgt + <=2 mask + <=2 pre-momentum + guards)._

Search protocol: optimise ONLY on FIT/VAL with the band objective `reward(min(FIT_PF,VAL_PF)) - 0.40*|FIT_PF-VAL_PF|`, where reward tents at PF 1.70 and penalises overshoot (anti-overfit). The single best FIT/VAL config is then confirmed on full TRAIN and scored ONCE on TEST. Quantile thresholds drawn from TRAIN only (never TEST).

Exit grid searched: SL [0.5, 0.7, 0.85, 1.0, 1.1, 1.2, 1.5] x Tgt [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5] (small/large SL x small/large target, asymmetric combos). Mask feats: ['atr_pct<=0.003071', 'body_pct<=0.804023', 'body_pct<=0.857143', 'body_pct>=0.661972', 'body_pct>=0.923077', 'close_loc<=0.928814', 'close_loc>=0.993228', 'lower_wick_pct>=0.016626', 'quality_score>=37.061385', 'rs_pct>=2.946849', 'signal_range_pct>=0.488095', 'upper_wick_pct>=0.004428'].

## Top 20 FIT/VAL trials (by band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|-----|------|--------|-------|----------|----------|-------|
| 1 | 1.0 | 1.5 | - | - | {"min_slot": "11:00", "max_slot": "14:30", "top_n": 1} | 120/0.416 | 131/0.419 | 0.415 |
| 2 | 1.0 | 1.5 | - | - | {"min_slot": "11:00", "max_slot": "14:30", "top_n": 1} | 120/0.416 | 131/0.419 | 0.415 |
| 3 | 1.0 | 1.5 | - | - | {"min_slot": "11:00", "max_slot": "14:30", "top_n": 1} | 120/0.416 | 131/0.419 | 0.415 |
| 4 | 1.0 | 1.5 | - | - | {"min_slot": "11:00", "max_slot": "14:30", "top_n": 1} | 120/0.416 | 131/0.419 | 0.415 |
| 5 | 1.0 | 1.5 | - | - | {"min_slot": "11:00", "top_n": 1} | 120/0.416 | 131/0.419 | 0.415 |
| 6 | 1.0 | 1.5 | - | - | {"min_slot": "11:00", "top_n": 1} | 120/0.416 | 131/0.419 | 0.415 |
| 7 | 1.0 | 1.5 | - | - | {"min_slot": "11:00", "top_n": 1} | 120/0.416 | 131/0.419 | 0.415 |
| 8 | 1.0 | 1.5 | - | - | {"min_slot": "11:00", "max_slot": "14:30", "top_n": 1} | 120/0.416 | 131/0.419 | 0.415 |
| 9 | 1.0 | 1.5 | - | - | {"min_slot": "11:00", "top_n": 1} | 120/0.416 | 131/0.419 | 0.415 |
| 10 | 1.0 | 1.5 | - | - | {"min_slot": "11:00", "top_n": 1} | 120/0.416 | 131/0.419 | 0.415 |
| 11 | 1.0 | 1.5 | - | - | {"min_slot": "11:00", "top_n": 1} | 120/0.416 | 131/0.419 | 0.415 |
| 12 | 1.0 | 1.5 | - | - | {"min_slot": "11:00", "top_n": 1} | 120/0.416 | 131/0.419 | 0.415 |
| 13 | 1.0 | 1.5 | - | - | {"min_slot": "11:00", "top_n": 1} | 120/0.416 | 131/0.419 | 0.415 |
| 14 | 1.0 | 1.5 | - | - | {"min_slot": "11:00", "top_n": 1} | 120/0.416 | 131/0.419 | 0.415 |
| 15 | 1.0 | 1.5 | - | - | {"min_slot": "11:00", "top_n": 1} | 120/0.416 | 131/0.419 | 0.415 |
| 16 | 1.0 | 1.5 | - | - | {"min_slot": "11:00", "top_n": 1} | 120/0.416 | 131/0.419 | 0.415 |
| 17 | 1.0 | 1.5 | - | - | {"min_slot": "11:00", "top_n": 1} | 120/0.416 | 131/0.419 | 0.415 |
| 18 | 1.0 | 1.5 | - | - | {"min_slot": "11:00", "top_n": 1} | 120/0.416 | 131/0.419 | 0.415 |
| 19 | 1.0 | 1.5 | - | - | {"min_slot": "11:00", "top_n": 1} | 120/0.416 | 131/0.419 | 0.415 |
| 20 | 1.0 | 1.5 | - | - | {"min_slot": "11:00", "top_n": 1} | 120/0.416 | 131/0.419 | 0.415 |

## Best config — confirmation runs

- **Best (FIT/VAL):** SL 1.0/Tgt 1.5 | mask [(none)] | premom [(none)] | guard {'min_slot': '11:00', 'max_slot': '14:30', 'top_n': 1} | maxpos 10 | dloss 0.0
- **TRAIN @15bps:** n=251 PF=0.418 net=Rs-88,802 win%=31.1 avgW=Rs817 avgL=Rs-882 maxDD=Rs-92,535 SL/TGT/EOD=94/41/116 tpd=13.94 tradeDom=0.02 dayDom=9.99 symDom=9.99 dbp=0.9997
- **TEST  @15bps:** n=42 PF=0.536 net=Rs-11,984 win%=35.7 avgW=Rs921 avgL=Rs-956 maxDD=Rs-15,975 SL/TGT/EOD=19/9/14 tpd=8.4 tradeDom=0.092 dayDom=9.99 symDom=9.99 dbp=0.9279
- **TRAIN @5bps:**  n=251 PF=0.529 net=Rs-63,880 win%=32.3 avgW=Rs886 avgL=Rs-798 maxDD=Rs-68,462 SL/TGT/EOD=94/41/116 tpd=13.94 tradeDom=0.019 dayDom=9.99 symDom=9.99 dbp=0.995
- **TEST  @5bps:**  n=42 PF=0.662 net=Rs-7,850 win%=40.5 avgW=Rs903 avgL=Rs-928 maxDD=Rs-14,335 SL/TGT/EOD=19/9/14 tpd=8.4 tradeDom=0.089 dayDom=9.99 symDom=9.99 dbp=0.815

- **Keep/reject:** REJECT  — TRAIN PF too low (<1.30); TEST PF below 1.40; TRAIN concentrated (one trade/day/symbol dominates); TEST concentrated (one trade/day/symbol dominates); too many trades/day

## Command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup L_DOUBLE_BOTTOM_VWAP --pool C:\TradingData\eqidv2\setup_pools_2026_06_29\L_DOUBLE_BOTTOM_VWAP --trials 220 --time_budget_min 9.0 --seed 7
```