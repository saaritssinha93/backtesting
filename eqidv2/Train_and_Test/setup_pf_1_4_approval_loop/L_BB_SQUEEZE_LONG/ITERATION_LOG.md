# L_BB_SQUEEZE_LONG (LONG) — ITERATION_LOG

_Generated 2026-07-01. Optimizer: Optuna TPE. 200 FIT/VAL trials (each trial = one logical config: exit SL/Tgt + <=2 mask + <=2 pre-momentum + guards)._

Search protocol: optimise ONLY on FIT/VAL with the band objective `reward(min(FIT_PF,VAL_PF)) - 0.40*|FIT_PF-VAL_PF|`, where reward tents at PF 1.70 and penalises overshoot (anti-overfit). The single best FIT/VAL config is then confirmed on full TRAIN and scored ONCE on TEST. Quantile thresholds drawn from TRAIN only (never TEST).

Exit grid searched: SL [0.5, 0.7, 0.85, 1.0, 1.1, 1.2, 1.5] x Tgt [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5] (small/large SL x small/large target, asymmetric combos). Mask feats: ['atr_pct<=0.003822', 'atr_pct>=0.003527', 'atr_pct>=0.003822', 'atr_pct>=0.004109', 'body_pct>=0.747611', 'body_pct>=0.851843', 'body_pct>=0.909742', 'body_pct>=0.990851', 'close_loc>=0.92233', 'close_loc>=0.953526', 'close_loc>=1.0', 'lower_wick_pct>=0.0'].

## Top 20 FIT/VAL trials (by band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|-----|------|--------|-------|----------|----------|-------|
| 1 | 1.1 | 1.5 | vol_ratio>=3.627005 | - | {"top_n": 3} | 80/0.646 | 101/0.648 | 0.6447 |
| 2 | 1.1 | 1.5 | vol_ratio>=3.627005 | - | {"top_n": 3} | 80/0.646 | 101/0.648 | 0.6447 |
| 3 | 1.1 | 1.5 | vol_ratio>=3.627005 | - | {"top_n": 3} | 80/0.646 | 101/0.648 | 0.6447 |
| 4 | 1.1 | 1.5 | vol_ratio>=3.627005 | - | {"top_n": 3} | 80/0.646 | 101/0.648 | 0.6447 |
| 5 | 1.1 | 1.5 | vol_ratio>=3.627005 | - | {"top_n": 3} | 80/0.646 | 101/0.648 | 0.6447 |
| 6 | 1.1 | 1.5 | vol_ratio>=3.627005 | - | {"top_n": 3} | 80/0.646 | 101/0.648 | 0.6447 |
| 7 | 1.1 | 1.5 | vol_ratio>=3.627005 | - | {"top_n": 3} | 80/0.646 | 101/0.648 | 0.6447 |
| 8 | 1.1 | 1.5 | vol_ratio>=3.627005 | - | {"top_n": 3} | 80/0.646 | 101/0.648 | 0.6447 |
| 9 | 1.1 | 1.5 | vol_ratio>=3.627005 | - | {"top_n": 3} | 80/0.646 | 101/0.648 | 0.6447 |
| 10 | 1.1 | 1.5 | vol_ratio>=3.627005 | - | {"top_n": 3} | 80/0.646 | 101/0.648 | 0.6447 |
| 11 | 1.1 | 1.5 | vol_ratio>=3.627005 | - | {"top_n": 3} | 80/0.646 | 101/0.648 | 0.6447 |
| 12 | 1.1 | 1.5 | vol_ratio>=3.627005 | - | {"top_n": 3} | 80/0.646 | 101/0.648 | 0.6447 |
| 13 | 1.1 | 1.5 | vol_ratio>=3.627005 | - | {"top_n": 3} | 80/0.646 | 101/0.648 | 0.6447 |
| 14 | 1.1 | 1.5 | vol_ratio>=3.627005 | - | {"top_n": 3} | 80/0.646 | 101/0.648 | 0.6447 |
| 15 | 1.1 | 1.5 | vol_ratio>=3.627005 | - | {"top_n": 3} | 80/0.646 | 101/0.648 | 0.6447 |
| 16 | 1.1 | 1.5 | vol_ratio>=3.627005 | - | {"top_n": 3} | 80/0.646 | 101/0.648 | 0.6447 |
| 17 | 1.1 | 1.5 | vol_ratio>=3.627005 | - | {"top_n": 3} | 80/0.646 | 101/0.648 | 0.6447 |
| 18 | 1.1 | 1.5 | vol_ratio>=3.627005 | - | {"top_n": 3} | 80/0.646 | 101/0.648 | 0.6447 |
| 19 | 1.1 | 1.5 | vol_ratio>=3.627005 | - | {"top_n": 3} | 80/0.646 | 101/0.648 | 0.6447 |
| 20 | 1.1 | 1.5 | vol_ratio>=3.627005 | - | {"top_n": 3} | 80/0.646 | 101/0.648 | 0.6447 |

## Best config — confirmation runs

- **Best (FIT/VAL):** SL 1.1/Tgt 1.5 | mask [vol_ratio>=3.627005] | premom [(none)] | guard {'top_n': 3} | maxpos 20 | dloss 0.0
- **TRAIN @15bps:** n=181 PF=0.647 net=Rs-43,303 win%=42.5 avgW=Rs1,030 avgL=Rs-1,179 maxDD=Rs-46,487 SL/TGT/EOD=84/57/40 tpd=3.55 tradeDom=0.016 dayDom=9.99 symDom=9.99 dbp=0.9882
- **TEST  @15bps:** n=7 PF=1.82 net=Rs2,276 win%=57.1 avgW=Rs1,263 avgL=Rs-926 maxDD=Rs-1,332 SL/TGT/EOD=2/4/1 tpd=1.75 tradeDom=0.251 dayDom=0.556 symDom=0.556 dbp=0.2554
- **TRAIN @5bps:**  n=181 PF=0.775 net=Rs-25,301 win%=44.2 avgW=Rs1,090 avgL=Rs-1,114 maxDD=Rs-30,122 SL/TGT/EOD=84/57/40 tpd=3.55 tradeDom=0.016 dayDom=9.99 symDom=9.99 dbp=0.9085
- **TEST  @5bps:**  n=7 PF=2.2 net=Rs2,977 win%=57.1 avgW=Rs1,365 avgL=Rs-827 maxDD=Rs-1,233 SL/TGT/EOD=2/4/1 tpd=1.75 tradeDom=0.251 dayDom=0.506 symDom=0.459 dbp=0.0515

- **Keep/reject:** REJECT  — TRAIN PF too low (<1.30); TRAIN concentrated (one trade/day/symbol dominates); TEST concentrated (one trade/day/symbol dominates)

## Command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup L_BB_SQUEEZE_LONG --pool C:\TradingData\eqidv2\setup_pools_2026_06_29\L_BB_SQUEEZE_LONG --trials 200 --time_budget_min 8.0 --seed 7
```