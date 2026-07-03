# B_HUGE_RED_FAILED_BOUNCE (SHORT) — ITERATION_LOG

_Generated 2026-07-01. Optimizer: Optuna TPE. 220 FIT/VAL trials (each trial = one logical config: exit SL/Tgt + <=2 mask + <=2 pre-momentum + guards)._

Search protocol: optimise ONLY on FIT/VAL with the band objective `reward(min(FIT_PF,VAL_PF)) - 0.40*|FIT_PF-VAL_PF|`, where reward tents at PF 1.70 and penalises overshoot (anti-overfit). The single best FIT/VAL config is then confirmed on full TRAIN and scored ONCE on TEST. Quantile thresholds drawn from TRAIN only (never TEST).

Exit grid searched: SL [0.5, 0.7, 0.85, 1.0, 1.1, 1.2, 1.5] x Tgt [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5] (small/large SL x small/large target, asymmetric combos). Mask feats: ['atr_pct<=0.002325', 'atr_pct<=0.002574', 'body_pct>=0.957647', 'close_loc>=0.039314', 'lower_wick_pct>=0.043134', 'quality_score<=48.596914', 'quality_score<=73.447609', 'rs_pct>=1.029341', 'signal_range_pct<=0.734841', 'signal_range_pct>=0.734841', 'upper_wick_pct>=0.0', 'upper_wick_pct>=0.05282'].

## Top 20 FIT/VAL trials (by band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|-----|------|--------|-------|----------|----------|-------|
| 1 | 1.0 | 1.5 | - | - | {"min_slot": "10:30", "top_n": 1} | 87/0.658 | 74/0.693 | 0.6436 |
| 2 | 1.0 | 1.5 | - | - | {"min_slot": "10:30", "top_n": 1} | 87/0.658 | 74/0.693 | 0.6436 |
| 3 | 1.0 | 1.5 | - | - | {"min_slot": "10:30", "top_n": 1} | 87/0.658 | 74/0.693 | 0.6436 |
| 4 | 1.0 | 1.5 | - | - | {"min_slot": "10:30", "top_n": 1} | 87/0.658 | 74/0.693 | 0.6436 |
| 5 | 1.0 | 1.5 | - | - | {"min_slot": "10:30", "top_n": 1} | 87/0.658 | 74/0.693 | 0.6436 |
| 6 | 1.0 | 1.5 | - | - | {"min_slot": "10:30", "top_n": 1} | 87/0.658 | 74/0.693 | 0.6436 |
| 7 | 1.0 | 1.5 | - | - | {"min_slot": "10:30", "top_n": 1} | 87/0.658 | 74/0.693 | 0.6436 |
| 8 | 1.0 | 1.5 | - | - | {"min_slot": "10:30", "top_n": 1} | 87/0.658 | 74/0.693 | 0.6436 |
| 9 | 1.0 | 1.5 | - | - | {"min_slot": "11:00", "top_n": 1} | 87/0.658 | 74/0.695 | 0.6429 |
| 10 | 1.0 | 2.5 | - | - | {"min_slot": "10:30", "top_n": 1} | 77/0.63 | 67/0.664 | 0.6161 |
| 11 | 1.0 | 2.5 | - | - | {"min_slot": "10:30", "top_n": 1} | 77/0.63 | 67/0.664 | 0.6161 |
| 12 | 1.0 | 2.5 | - | - | {"min_slot": "10:30", "top_n": 1} | 77/0.63 | 67/0.664 | 0.6161 |
| 13 | 1.0 | 2.5 | - | - | {"min_slot": "10:30", "top_n": 1} | 77/0.63 | 67/0.664 | 0.6161 |
| 14 | 1.0 | 2.5 | - | - | {"min_slot": "10:30", "top_n": 1} | 77/0.63 | 67/0.664 | 0.6161 |
| 15 | 1.0 | 2.5 | - | - | {"min_slot": "10:30", "top_n": 1} | 77/0.63 | 67/0.664 | 0.6161 |
| 16 | 1.0 | 2.5 | - | - | {"min_slot": "10:30", "top_n": 1} | 77/0.63 | 67/0.664 | 0.6161 |
| 17 | 1.0 | 2.5 | - | - | {"min_slot": "10:30", "top_n": 1} | 77/0.63 | 67/0.664 | 0.6161 |
| 18 | 1.0 | 2.5 | - | - | {"min_slot": "10:30", "top_n": 1} | 77/0.63 | 67/0.664 | 0.6161 |
| 19 | 1.0 | 2.5 | - | - | {"min_slot": "10:30", "top_n": 1} | 77/0.63 | 67/0.664 | 0.6161 |
| 20 | 1.0 | 2.5 | - | - | {"min_slot": "10:30", "top_n": 1} | 77/0.63 | 67/0.664 | 0.6161 |

## Best config — confirmation runs

- **Best (FIT/VAL):** SL 1.0/Tgt 1.5 | mask [(none)] | premom [(none)] | guard {'min_slot': '10:30', 'top_n': 1} | maxpos 10 | dloss 4000.0
- **TRAIN @15bps:** n=161 PF=0.674 net=Rs-27,584 win%=40.4 avgW=Rs876 avgL=Rs-880 maxDD=Rs-31,499 SL/TGT/EOD=56/37/68 tpd=10.06 tradeDom=0.022 dayDom=9.99 symDom=9.99 dbp=0.985
- **TEST  @15bps:** n=42 PF=0.378 net=Rs-17,551 win%=26.2 avgW=Rs969 avgL=Rs-910 maxDD=Rs-19,475 SL/TGT/EOD=20/7/15 tpd=8.4 tradeDom=0.119 dayDom=9.99 symDom=9.99 dbp=1.0
- **TRAIN @5bps:**  n=161 PF=0.845 net=Rs-11,645 win%=43.5 avgW=Rs908 avgL=Rs-826 maxDD=Rs-16,523 SL/TGT/EOD=56/37/68 tpd=10.06 tradeDom=0.021 dayDom=9.99 symDom=9.99 dbp=0.8174
- **TEST  @5bps:**  n=42 PF=0.467 net=Rs-13,395 win%=26.2 avgW=Rs1,067 avgL=Rs-811 maxDD=Rs-15,913 SL/TGT/EOD=20/7/15 tpd=8.4 tradeDom=0.116 dayDom=9.99 symDom=9.99 dbp=1.0

- **Keep/reject:** REJECT  — TRAIN PF too low (<1.30); TEST PF below 1.40; TRAIN concentrated (one trade/day/symbol dominates); TEST concentrated (one trade/day/symbol dominates); too many trades/day

## Command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup B_HUGE_RED_FAILED_BOUNCE --pool C:\TradingData\eqidv2\setup_pools_2026_06_29\B_HUGE_RED_FAILED_BOUNCE --trials 220 --time_budget_min 9.0 --seed 7
```