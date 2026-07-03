# L_PRESSURE_BURST_VWAP (LONG) — ITERATION_LOG

_Generated 2026-07-01. Optimizer: Optuna TPE. 220 FIT/VAL trials (each trial = one logical config: exit SL/Tgt + <=2 mask + <=2 pre-momentum + guards)._

Search protocol: optimise ONLY on FIT/VAL with the band objective `reward(min(FIT_PF,VAL_PF)) - 0.40*|FIT_PF-VAL_PF|`, where reward tents at PF 1.70 and penalises overshoot (anti-overfit). The single best FIT/VAL config is then confirmed on full TRAIN and scored ONCE on TEST. Quantile thresholds drawn from TRAIN only (never TEST).

Exit grid searched: SL [0.5, 0.7, 0.85, 1.0, 1.1, 1.2, 1.5] x Tgt [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5] (small/large SL x small/large target, asymmetric combos). Mask feats: ['atr_pct<=0.002813', 'body_pct>=0.72', 'body_pct>=0.935065', 'close_loc>=0.765548', 'lower_wick_pct<=0.0', 'lower_wick_pct>=0.005996', 'quality_score<=70.221012', 'rs_pct<=2.161921', 'rs_pct>=3.554105', 'signal_range_pct>=0.334562', 'upper_wick_pct>=0.007401', 'upper_wick_pct>=0.113675'].

## Top 20 FIT/VAL trials (by band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|-----|------|--------|-------|----------|----------|-------|
| 1 | 1.0 | 2.0 | - | sig5_vol_ratio20>=3.84695;pre3_close_pos>=0.392861 | {"max_slot": "13:00"} | 77/0.534 | 76/0.566 | 0.5208 |
| 2 | 1.0 | 2.0 | - | sig5_vol_ratio20>=3.84695;pre3_close_pos>=0.392861 | {"max_slot": "13:00"} | 77/0.534 | 76/0.566 | 0.5208 |
| 3 | 1.5 | 1.25 | - | pre_entry_momentum_score>=67.124616;sig5_adx_calc>=34.739741 | {"min_slot": "11:00", "max_slot": "13:00"} | 29/0.805 | 28/0.576 | 0.4848 |
| 4 | 1.5 | 2.5 | - | sig5_vol_ratio20>=3.84695;sig5_adx_calc>=22.052374 | {"max_slot": "14:00"} | 97/0.487 | 103/0.534 | 0.4683 |
| 5 | 1.5 | 1.25 | - | pre3_close_pos>=0.885707;sig5_adx_calc<=24.628204 | {"max_slot": "14:00"} | 132/0.613 | 125/0.498 | 0.4514 |
| 6 | 1.5 | 1.25 | - | sig5_vol_ratio20>=3.84695;sig5_adx_calc>=30.868886 | {"max_slot": "14:00"} | 66/0.46 | 66/0.453 | 0.4505 |
| 7 | 1.0 | 1.25 | - | sig5_vol_ratio20>=3.84695;pre3_close_pos>=0.392861 | {"max_slot": "13:00"} | 77/0.479 | 76/0.447 | 0.4348 |
| 8 | 1.5 | 1.25 | - | sig5_vol_ratio20>=3.84695;sig5_adx_calc>=22.052374 | {"max_slot": "13:00"} | 85/0.424 | 87/0.456 | 0.4109 |
| 9 | 1.5 | 1.25 | - | sig5_vol_ratio20>=3.84695;sig5_adx_calc>=22.052374 | {"max_slot": "14:00"} | 107/0.427 | 114/0.484 | 0.4034 |
| 10 | 1.5 | 1.25 | - | sig5_vol_ratio20>=3.84695;sig5_adx_calc>=22.052374 | {"max_slot": "14:00"} | 107/0.427 | 114/0.484 | 0.4034 |
| 11 | 1.5 | 1.25 | - | sig5_vol_ratio20>=3.84695;sig5_adx_calc>=22.052374 | {"max_slot": "14:00"} | 107/0.427 | 114/0.484 | 0.4034 |
| 12 | 1.5 | 1.25 | - | sig5_vol_ratio20>=3.84695;sig5_adx_calc>=22.052374 | {"max_slot": "14:00"} | 107/0.427 | 114/0.484 | 0.4034 |
| 13 | 1.5 | 1.25 | - | sig5_vol_ratio20>=3.84695;sig5_adx_calc>=22.052374 | {"max_slot": "14:00"} | 107/0.427 | 114/0.484 | 0.4034 |
| 14 | 1.5 | 1.25 | - | sig5_vol_ratio20>=3.84695;sig5_adx_calc>=22.052374 | {"max_slot": "14:00"} | 107/0.427 | 114/0.484 | 0.4034 |
| 15 | 1.5 | 1.25 | - | sig5_vol_ratio20>=3.84695;sig5_adx_calc>=22.052374 | {"max_slot": "14:00"} | 107/0.427 | 114/0.484 | 0.4034 |
| 16 | 1.5 | 1.25 | - | sig5_vol_ratio20>=3.84695;sig5_adx_calc>=22.052374 | {"max_slot": "14:00"} | 107/0.427 | 114/0.484 | 0.4034 |
| 17 | 1.5 | 1.25 | - | sig5_vol_ratio20>=3.84695;sig5_adx_calc>=22.052374 | {"max_slot": "14:00"} | 107/0.427 | 114/0.484 | 0.4034 |
| 18 | 1.5 | 1.25 | - | sig5_vol_ratio20>=3.84695;sig5_adx_calc>=22.052374 | {"max_slot": "14:00"} | 107/0.427 | 114/0.484 | 0.4034 |
| 19 | 1.5 | 1.25 | - | sig5_vol_ratio20>=3.84695;sig5_adx_calc>=22.052374 | {"max_slot": "14:00"} | 107/0.427 | 114/0.484 | 0.4034 |
| 20 | 1.0 | 1.25 | - | sig5_vol_ratio20>=3.84695;sig5_adx_calc>=22.052374 | {"max_slot": "13:00"} | 88/0.425 | 89/0.481 | 0.4029 |

## Best config — confirmation runs

- **Best (FIT/VAL):** SL 1.0/Tgt 2.0 | mask [(none)] | premom [sig5_vol_ratio20>=3.84695; pre3_close_pos>=0.392861] | guard {'max_slot': '13:00'} | maxpos 10 | dloss 0.0
- **TRAIN @15bps:** n=153 PF=0.549 net=Rs-44,645 win%=31.4 avgW=Rs1,134 avgL=Rs-944 maxDD=Rs-48,783 SL/TGT/EOD=68/23/62 tpd=8.5 tradeDom=0.032 dayDom=9.99 symDom=9.99 dbp=0.9997
- **TEST  @15bps:** n=39 PF=0.537 net=Rs-13,010 win%=30.8 avgW=Rs1,260 avgL=Rs-1,042 maxDD=Rs-12,096 SL/TGT/EOD=20/7/12 tpd=9.75 tradeDom=0.117 dayDom=9.99 symDom=9.99 dbp=1.0
- **TRAIN @5bps:**  n=153 PF=0.669 net=Rs-29,455 win%=34.6 avgW=Rs1,123 avgL=Rs-890 maxDD=Rs-34,997 SL/TGT/EOD=68/23/62 tpd=8.5 tradeDom=0.031 dayDom=9.99 symDom=9.99 dbp=0.984
- **TEST  @5bps:**  n=39 PF=0.642 net=Rs-9,140 win%=35.9 avgW=Rs1,172 avgL=Rs-1,022 maxDD=Rs-8,684 SL/TGT/EOD=20/7/12 tpd=9.75 tradeDom=0.114 dayDom=9.99 symDom=9.99 dbp=1.0

- **Keep/reject:** REJECT  — TRAIN PF too low (<1.30); TEST PF below 1.40; TRAIN concentrated (one trade/day/symbol dominates); TEST concentrated (one trade/day/symbol dominates); too many trades/day

## Command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup L_PRESSURE_BURST_VWAP --pool C:\TradingData\eqidv2\setup_pools_2026_06_29\L_PRESSURE_BURST_VWAP --trials 220 --time_budget_min 9.0 --seed 7
```