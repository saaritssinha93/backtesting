# D_EMA20_BOUNCE (LONG) — ITERATION_LOG

_Generated 2026-07-01. Optimizer: Optuna TPE. 250 FIT/VAL trials (each trial = one logical config: exit SL/Tgt + <=1 mask + <=1 pre-momentum + guards)._

Search protocol: optimise ONLY on FIT/VAL with the band objective `reward(min(FIT_PF,VAL_PF)) - 0.80*|FIT_PF-VAL_PF|`, where reward tents at PF 1.70 and penalises overshoot (anti-overfit). The single best FIT/VAL config is then confirmed on full TRAIN and scored ONCE on TEST. Quantile thresholds drawn from TRAIN only (never TEST). The robust gate then checks TEST PF+day-block p, TRAIN target-fill, threshold-neighborhood stability, and term-dropout stability.

Exit grid searched: SL [0.5, 0.7, 0.85, 1.0, 1.1, 1.2, 1.5] x Tgt [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5] (small/large SL x small/large target, asymmetric combos). Mask feats: ['atr_pct<=0.002197', 'atr_pct>=0.002374', 'atr_pct>=0.002882', 'atr_pct>=0.003263', 'body_pct<=0.660163', 'body_pct>=0.606298', 'body_pct>=0.698485', 'body_pct>=0.878969', 'body_pct>=1.0', 'close_loc<=0.821395', 'close_loc<=0.989749', 'close_loc>=0.771692'].

## Top 20 FIT/VAL trials (by band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|-----|------|--------|-------|----------|----------|-------|
| 1 | 0.85 | 1.0 | body_pct>=0.878969 | - | {"min_slot": "09:30", "max_slot": "14:00", "top_n": 3} | 12/0.515 | 7/0.586 | 0.4572 |
| 2 | 0.85 | 1.0 | body_pct>=0.878969 | - | {"max_slot": "14:00", "top_n": 2} | 12/0.515 | 7/0.586 | 0.4572 |
| 3 | 0.85 | 1.0 | body_pct>=0.878969 | - | {"max_slot": "14:00", "top_n": 3} | 12/0.515 | 7/0.586 | 0.4572 |
| 4 | 0.85 | 1.0 | body_pct>=0.878969 | - | {"max_slot": "14:00", "top_n": 3} | 12/0.515 | 7/0.586 | 0.4572 |
| 5 | 0.85 | 1.0 | lower_wick_pct>=0.069239 | - | {"max_slot": "14:30", "top_n": 2} | 14/0.405 | 8/0.426 | 0.3879 |
| 6 | 0.85 | 1.0 | lower_wick_pct>=0.069239 | - | {"max_slot": "14:30", "top_n": 2} | 14/0.405 | 8/0.426 | 0.3879 |
| 7 | 0.85 | 1.0 | lower_wick_pct>=0.069239 | - | {"max_slot": "14:30", "top_n": 2} | 14/0.405 | 8/0.426 | 0.3879 |
| 8 | 0.85 | 1.0 | lower_wick_pct>=0.069239 | - | {"max_slot": "14:30", "top_n": 2} | 14/0.405 | 8/0.426 | 0.3879 |
| 9 | 0.85 | 1.0 | lower_wick_pct>=0.069239 | - | {"max_slot": "14:30", "top_n": 2} | 14/0.405 | 8/0.426 | 0.3879 |
| 10 | 0.85 | 1.0 | lower_wick_pct>=0.069239 | - | {"max_slot": "14:30", "top_n": 2} | 14/0.405 | 8/0.426 | 0.3879 |
| 11 | 0.85 | 1.0 | lower_wick_pct>=0.069239 | - | {"max_slot": "14:30", "top_n": 2} | 14/0.405 | 8/0.426 | 0.3879 |
| 12 | 0.85 | 1.0 | lower_wick_pct>=0.069239 | - | {"max_slot": "14:30", "top_n": 2} | 14/0.405 | 8/0.426 | 0.3879 |
| 13 | 0.85 | 1.0 | lower_wick_pct>=0.069239 | - | {"max_slot": "14:30", "top_n": 2} | 14/0.405 | 8/0.426 | 0.3879 |
| 14 | 0.85 | 1.0 | lower_wick_pct>=0.069239 | - | {"max_slot": "14:30", "top_n": 2} | 14/0.405 | 8/0.426 | 0.3879 |
| 15 | 0.85 | 1.0 | lower_wick_pct>=0.069239 | - | {"max_slot": "14:30", "top_n": 3} | 14/0.405 | 8/0.426 | 0.3879 |
| 16 | 0.85 | 1.0 | lower_wick_pct>=0.069239 | - | {"max_slot": "14:30", "top_n": 2} | 14/0.405 | 8/0.426 | 0.3879 |
| 17 | 0.85 | 1.0 | lower_wick_pct>=0.069239 | - | {"max_slot": "14:30", "top_n": 3} | 14/0.405 | 8/0.426 | 0.3879 |
| 18 | 0.85 | 1.0 | lower_wick_pct>=0.069239 | - | {"max_slot": "14:30", "top_n": 3} | 14/0.405 | 8/0.426 | 0.3879 |
| 19 | 0.85 | 1.0 | lower_wick_pct>=0.069239 | - | {"max_slot": "14:30", "top_n": 2} | 14/0.405 | 8/0.426 | 0.3879 |
| 20 | 0.85 | 1.0 | lower_wick_pct>=0.069239 | - | {"max_slot": "14:30", "top_n": 2} | 14/0.405 | 8/0.426 | 0.3879 |

## Best config — confirmation runs

- **Best (FIT/VAL):** SL 0.85/Tgt 1.0 | mask [body_pct>=0.878969] | premom [(none)] | guard {'max_slot': '14:00', 'top_n': 2} | maxpos 20 | dloss 0.0
- **TRAIN @15bps:** n=19 PF=0.537 net=Rs-4,228 win%=42.1 avgW=Rs612 avgL=Rs-829 maxDD=Rs-4,993 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.9 tradeDom=0.157 dayDom=9.99 symDom=9.99 dbp=0.9383
- **TEST  @15bps:** n=7 PF=0.323 net=Rs-2,963 win%=28.6 avgW=Rs708 avgL=Rs-876 maxDD=Rs-4,380 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.75 tradeDom=0.541 dayDom=9.99 symDom=9.99 dbp=0.9506
- **TRAIN @5bps:**  n=19 PF=0.537 net=Rs-4,228 win%=42.1 avgW=Rs612 avgL=Rs-829 maxDD=Rs-4,993 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.9 tradeDom=0.157 dayDom=9.99 symDom=9.99 dbp=0.9383
- **TEST  @5bps:**  n=7 PF=0.323 net=Rs-2,963 win%=28.6 avgW=Rs708 avgL=Rs-876 maxDD=Rs-4,380 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.75 tradeDom=0.541 dayDom=9.99 symDom=9.99 dbp=0.9506

## Robustness diagnostics

- neighborhood pass: `False` (PF floor 1.15)
- term-dropout pass: `False` (PF floor 1.0)
- hard reasons: `TRAIN too few trades (train_n<20); TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.30; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates)`
- insufficient reasons: `-`
- warnings: `-`

- **Keep/reject:** REJECT  — TRAIN too few trades (train_n<20); TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.30; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates)

## Command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup D_EMA20_BOUNCE --pool C:/TradingData/eqidv2/outputs_ID_v11_conf_fresh_20260629 --trials 250 --time_budget_min 12.0 --seed 7 --gap_lambda 0.8 --max_mask_terms 1 --max_pm_terms 1 --test_pf_min 1.3 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```