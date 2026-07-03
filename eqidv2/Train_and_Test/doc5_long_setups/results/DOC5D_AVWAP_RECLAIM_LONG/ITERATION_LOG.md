# DOC5D_AVWAP_RECLAIM_LONG (LONG) — ITERATION_LOG

_Generated 2026-07-01. Optimizer: Optuna TPE. 200 FIT/VAL trials (each trial = one logical config: exit SL/Tgt + <=1 mask + <=1 pre-momentum + guards)._

Search protocol: optimise ONLY on FIT/VAL with the band objective `reward(min(FIT_PF,VAL_PF)) - 0.80*|FIT_PF-VAL_PF|`, where reward tents at PF 1.70 and penalises overshoot (anti-overfit). The single best FIT/VAL config is then confirmed on full TRAIN and scored ONCE on TEST. Quantile thresholds drawn from TRAIN only (never TEST). The robust gate then checks TEST PF+day-block p, TRAIN target-fill, threshold-neighborhood stability, and term-dropout stability.

Exit grid searched: SL [0.5, 0.7, 0.85, 1.0, 1.1, 1.2, 1.5] x Tgt [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5] (small/large SL x small/large target, asymmetric combos). Mask feats: ['atr_pct<=0.00286', 'atr_pct>=0.001702', 'atr_pct>=0.002028', 'atr_pct>=0.002539', 'atr_pct>=0.00286', 'atr_pct>=0.003824', 'body_pct<=0.784485', 'body_pct>=0.671266', 'body_pct>=0.905165', 'close_loc<=0.976562', 'close_loc>=0.828286', 'close_loc>=0.87068'].

## Top 20 FIT/VAL trials (by band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|-----|------|--------|-------|----------|----------|-------|
| 1 | 1.1 | 1.25 | ranker_score>=87.752247 | pre1_adx>=16.925369 | {"min_slot": "09:45", "top_n": 2} | 8/1.194 | 14/1.281 | 1.1252 |
| 2 | 1.1 | 1.25 | ranker_score>=87.752247 | pre1_adx>=19.614683 | {"min_slot": "09:45", "top_n": 2} | 8/1.194 | 13/1.097 | 1.0191 |
| 3 | 1.1 | 1.25 | ranker_score>=87.752247 | pre1_adx>=19.614683 | {"min_slot": "09:45", "top_n": 2} | 8/1.194 | 13/1.097 | 1.0191 |
| 4 | 1.1 | 1.25 | ranker_score>=87.752247 | pre1_adx>=19.614683 | {"min_slot": "09:45", "top_n": 2} | 8/1.194 | 13/1.097 | 1.0191 |
| 5 | 1.1 | 1.25 | ranker_score>=87.752247 | pre1_adx>=19.614683 | {"min_slot": "09:45", "top_n": 2} | 8/1.194 | 13/1.097 | 1.0191 |
| 6 | 1.1 | 1.25 | ranker_score>=87.752247 | pre1_adx>=19.614683 | {"min_slot": "09:45", "top_n": 2} | 8/1.194 | 13/1.097 | 1.0191 |
| 7 | 1.1 | 1.25 | ranker_score>=87.752247 | pre1_adx>=19.614683 | {"min_slot": "09:45", "top_n": 2} | 8/1.194 | 13/1.097 | 1.0191 |
| 8 | 1.1 | 1.25 | ranker_score>=87.752247 | pre1_adx>=19.614683 | {"min_slot": "09:45", "top_n": 2} | 8/1.194 | 13/1.097 | 1.0191 |
| 9 | 1.1 | 1.25 | ranker_score>=87.752247 | pre1_adx>=19.614683 | {"min_slot": "09:45", "top_n": 2} | 8/1.194 | 13/1.097 | 1.0191 |
| 10 | 1.1 | 1.25 | ranker_score>=87.752247 | pre1_adx>=19.614683 | {"min_slot": "09:45", "top_n": 2} | 8/1.194 | 13/1.097 | 1.0191 |
| 11 | 1.1 | 1.25 | ranker_score>=87.752247 | pre1_adx>=19.614683 | {"min_slot": "09:45", "top_n": 2} | 8/1.194 | 13/1.097 | 1.0191 |
| 12 | 1.1 | 1.25 | ranker_score>=87.752247 | pre1_adx>=19.614683 | {"min_slot": "09:45", "top_n": 2} | 8/1.194 | 13/1.097 | 1.0191 |
| 13 | 1.1 | 1.25 | ranker_score>=87.752247 | pre1_adx>=19.614683 | {"min_slot": "09:45", "top_n": 2} | 8/1.194 | 13/1.097 | 1.0191 |
| 14 | 1.1 | 1.25 | ranker_score>=87.752247 | pre1_adx>=19.614683 | {"min_slot": "09:45", "top_n": 2} | 8/1.194 | 13/1.097 | 1.0191 |
| 15 | 0.85 | 1.25 | ranker_score>=87.752247 | pre1_adx>=19.614683 | {"min_slot": "09:45", "top_n": 2} | 8/1.047 | 13/0.956 | 0.8834 |
| 16 | 0.85 | 1.25 | ranker_score>=87.752247 | pre1_adx>=19.614683 | {"min_slot": "09:45", "top_n": 2} | 8/1.047 | 13/0.956 | 0.8834 |
| 17 | 0.85 | 1.25 | ranker_score>=87.752247 | pre1_adx>=19.614683 | {"min_slot": "09:45", "top_n": 2} | 8/1.047 | 13/0.956 | 0.8834 |
| 18 | 0.85 | 1.25 | ranker_score>=87.752247 | pre1_adx>=19.614683 | {"min_slot": "09:45", "top_n": 2} | 8/1.047 | 13/0.956 | 0.8834 |
| 19 | 0.85 | 1.25 | ranker_score>=87.752247 | pre1_adx>=19.614683 | {"min_slot": "09:45", "top_n": 2} | 8/1.047 | 13/0.956 | 0.8834 |
| 20 | 0.85 | 1.25 | ranker_score>=87.752247 | pre1_adx>=19.614683 | {"min_slot": "09:45", "top_n": 2} | 8/1.047 | 13/0.956 | 0.8834 |

## Best config — confirmation runs

- **Best (FIT/VAL):** SL 1.1/Tgt 1.25 | mask [ranker_score>=87.752247] | premom [pre1_adx>=16.925369] | guard {'min_slot': '09:45', 'top_n': 2} | maxpos 20 | dloss 4000.0
- **TRAIN @15bps:** n=22 PF=1.252 net=Rs2,086 win%=59.1 avgW=Rs798 avgL=Rs-922 maxDD=Rs-1,950 SL/TGT/EOD=5/8/9 tgt%=36.4 tpd=1.47 tradeDom=0.098 dayDom=0.575 symDom=0.487 dbp=0.2765
- **TEST  @15bps:** n=8 PF=0.467 net=Rs-2,467 win%=37.5 avgW=Rs719 avgL=Rs-925 maxDD=Rs-3,306 SL/TGT/EOD=3/2/3 tgt%=25.0 tpd=1.0 tradeDom=0.471 dayDom=9.99 symDom=9.99 dbp=0.8288
- **TRAIN @5bps:**  n=22 PF=1.578 net=Rs4,281 win%=59.1 avgW=Rs899 avgL=Rs-823 maxDD=Rs-1,655 SL/TGT/EOD=5/8/9 tgt%=36.4 tpd=1.47 tradeDom=0.096 dayDom=0.327 symDom=0.261 dbp=0.1148
- **TEST  @5bps:**  n=8 PF=0.592 net=Rs-1,695 win%=37.5 avgW=Rs818 avgL=Rs-830 maxDD=Rs-2,929 SL/TGT/EOD=3/2/3 tgt%=25.0 tpd=1.0 tradeDom=0.455 dayDom=9.99 symDom=9.99 dbp=0.7436

## Robustness diagnostics

- neighborhood pass: `False` (PF floor 1.15)
- term-dropout pass: `False` (PF floor 1.0)
- hard reasons: `TRAIN PF too low (<1.30); TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.30; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates)`
- insufficient reasons: `-`
- warnings: `-`

- **Keep/reject:** REJECT  — TRAIN PF too low (<1.30); TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.30; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates)

## Command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup DOC5D_AVWAP_RECLAIM_LONG --pool C:/Users/Saarit/OneDrive/Desktop/Trading/backtesting/eqidv2/backtesting/eqidv2/Train_and_Test/doc5_long_setups/pool --trials 200 --time_budget_min 10.0 --seed 7 --gap_lambda 0.8 --max_mask_terms 1 --max_pm_terms 1 --test_pf_min 1.3 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```