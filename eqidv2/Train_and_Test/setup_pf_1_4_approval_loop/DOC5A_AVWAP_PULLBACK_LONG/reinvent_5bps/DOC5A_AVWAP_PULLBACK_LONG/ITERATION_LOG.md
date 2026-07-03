# DOC5A_AVWAP_PULLBACK_LONG (LONG) — ITERATION_LOG

_Generated 2026-07-01. Optimizer: Optuna TPE. 900 FIT/VAL trials (each trial = one logical config: exit SL/Tgt + <=2 mask + <=1 pre-momentum + guards)._

Search protocol: optimise ONLY on FIT/VAL with the band objective `reward(min(FIT_PF,VAL_PF)) - 0.80*|FIT_PF-VAL_PF|`, where reward tents at PF 1.70 and penalises overshoot (anti-overfit). The single best FIT/VAL config is then confirmed on full TRAIN and scored ONCE on TEST. Quantile thresholds drawn from TRAIN only (never TEST). The robust gate then checks TEST PF+day-block p, TRAIN target-fill, threshold-neighborhood stability, and term-dropout stability.

Exit grid searched: SL [0.5, 0.7, 0.85, 1.0, 1.1, 1.2, 1.5] x Tgt [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5] (small/large SL x small/large target, asymmetric combos). Mask feats: ['adx_sig>=14.506755', 'adx_sig>=16.931765', 'adx_sig>=19.206162', 'adx_sig>=24.206914', 'adx_sig>=26.595559', 'atr_pct<=0.002715', 'atr_pct>=0.001539', 'atr_pct>=0.001773', 'atr_pct>=0.001977', 'atr_pct>=0.002187', 'atr_pct>=0.002427', 'atr_pct>=0.003079'].

## Top 20 FIT/VAL trials (by band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|-----|------|--------|-------|----------|----------|-------|
| 1 | 1.2 | 2.5 | pullback_depth_atr>=0.27969;signal_range_pct<=0.207383 | pre3_close_pos<=0.25 | {"max_slot": "12:00", "top_n": 3} | 7/1.662 | 6/1.552 | 1.4832 |
| 2 | 1.2 | 2.5 | pullback_depth_atr>=0.27969;signal_range_pct<=0.207383 | pre3_close_pos<=0.25 | {"max_slot": "12:00", "top_n": 3} | 7/1.662 | 6/1.552 | 1.4832 |
| 3 | 1.2 | 2.5 | pullback_depth_atr>=0.27969;signal_range_pct<=0.207383 | pre3_close_pos<=0.25 | {"max_slot": "12:00", "top_n": 3} | 7/1.662 | 6/1.552 | 1.4832 |
| 4 | 1.2 | 2.5 | pullback_depth_atr>=0.27969;signal_range_pct<=0.207383 | pre3_close_pos<=0.25 | {"max_slot": "12:00", "top_n": 3} | 7/1.662 | 6/1.552 | 1.4832 |
| 5 | 1.2 | 2.5 | pullback_depth_atr>=0.27969;signal_range_pct<=0.207383 | pre3_close_pos<=0.25 | {"max_slot": "12:00", "top_n": 3} | 7/1.662 | 6/1.552 | 1.4832 |
| 6 | 1.2 | 2.5 | pullback_depth_atr>=0.27969;signal_range_pct<=0.207383 | pre3_close_pos<=0.25 | {"max_slot": "12:00", "top_n": 3} | 7/1.662 | 6/1.552 | 1.4832 |
| 7 | 1.1 | 2.5 | quality_score>=58.555742;signal_range_pct<=0.207383 | pre3_close_pos<=0.388898 | {"max_slot": "12:00", "top_n": 3} | 14/1.13 | 7/1.221 | 1.0584 |
| 8 | 1.1 | 2.5 | quality_score>=58.555742;signal_range_pct<=0.207383 | pre3_close_pos<=0.388898 | {"max_slot": "12:00", "top_n": 3} | 14/1.13 | 7/1.221 | 1.0584 |
| 9 | 1.1 | 2.5 | ranker_score>=58.555742;signal_range_pct<=0.207383 | pre3_close_pos<=0.388898 | {"max_slot": "12:00", "top_n": 3} | 14/1.13 | 7/1.221 | 1.0584 |
| 10 | 1.1 | 2.5 | ranker_score>=58.555742;signal_range_pct<=0.207383 | pre3_close_pos<=0.388898 | {"max_slot": "12:00", "top_n": 3} | 14/1.13 | 7/1.221 | 1.0584 |
| 11 | 1.1 | 2.5 | ranker_score>=58.555742;signal_range_pct<=0.207383 | pre3_close_pos<=0.388898 | {"max_slot": "12:00", "top_n": 3} | 14/1.13 | 7/1.221 | 1.0584 |
| 12 | 1.1 | 2.5 | ranker_score>=58.555742;signal_range_pct<=0.207383 | pre3_close_pos<=0.388898 | {"max_slot": "12:00", "top_n": 3} | 14/1.13 | 7/1.221 | 1.0584 |
| 13 | 1.1 | 2.5 | ranker_score>=58.555742;signal_range_pct<=0.207383 | pre3_close_pos<=0.388898 | {"max_slot": "12:00", "top_n": 3} | 14/1.13 | 7/1.221 | 1.0584 |
| 14 | 1.1 | 2.5 | ranker_score>=58.555742;signal_range_pct<=0.207383 | pre3_close_pos<=0.388898 | {"max_slot": "12:00", "top_n": 3} | 14/1.13 | 7/1.221 | 1.0584 |
| 15 | 1.1 | 2.5 | ranker_score>=58.555742;signal_range_pct<=0.207383 | pre3_close_pos<=0.388898 | {"max_slot": "12:00", "top_n": 3} | 14/1.13 | 7/1.221 | 1.0584 |
| 16 | 1.1 | 2.5 | ranker_score>=58.555742;signal_range_pct<=0.207383 | pre3_close_pos<=0.388898 | {"max_slot": "12:00", "top_n": 3} | 14/1.13 | 7/1.221 | 1.0584 |
| 17 | 1.1 | 2.5 | ranker_score>=58.555742;signal_range_pct<=0.207383 | pre3_close_pos<=0.388898 | {"max_slot": "12:00", "top_n": 3} | 14/1.13 | 7/1.221 | 1.0584 |
| 18 | 1.1 | 2.5 | ranker_score>=58.555742;signal_range_pct<=0.207383 | pre3_close_pos<=0.388898 | {"max_slot": "12:00", "top_n": 3} | 14/1.13 | 7/1.221 | 1.0584 |
| 19 | 1.1 | 2.5 | ranker_score>=58.555742;signal_range_pct<=0.207383 | pre3_close_pos<=0.388898 | {"max_slot": "12:00", "top_n": 3} | 14/1.13 | 7/1.221 | 1.0584 |
| 20 | 1.1 | 2.5 | ranker_score>=58.555742;signal_range_pct<=0.207383 | pre3_close_pos<=0.388898 | {"max_slot": "12:00", "top_n": 3} | 14/1.13 | 7/1.221 | 1.0584 |

## Best config — confirmation runs

- **Best (FIT/VAL):** SL 1.2/Tgt 2.5 | mask [pullback_depth_atr>=0.27969; signal_range_pct<=0.207383] | premom [pre3_close_pos<=0.25] | guard {'max_slot': '12:00', 'top_n': 3} | maxpos 20 | dloss 4000.0
- **TRAIN @15bps:** n=13 PF=1.619 net=Rs2,306 win%=46.2 avgW=Rs1,005 avgL=Rs-532 maxDD=Rs-2,297 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.18 tradeDom=0.348 dayDom=1.159 symDom=0.909 dbp=0.2611
- **TEST  @15bps:** n=8 PF=0.002 net=Rs-7,410 win%=12.5 avgW=Rs12 avgL=Rs-1,060 maxDD=Rs-6,086 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.67 tradeDom=1.0 dayDom=9.99 symDom=9.99 dbp=1.0
- **TRAIN @5bps:**  n=13 PF=1.619 net=Rs2,306 win%=46.2 avgW=Rs1,005 avgL=Rs-532 maxDD=Rs-2,297 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.18 tradeDom=0.348 dayDom=1.159 symDom=0.909 dbp=0.2611
- **TEST  @5bps:**  n=8 PF=0.002 net=Rs-7,410 win%=12.5 avgW=Rs12 avgL=Rs-1,060 maxDD=Rs-6,086 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.67 tradeDom=1.0 dayDom=9.99 symDom=9.99 dbp=1.0

## Robustness diagnostics

- neighborhood pass: `False` (PF floor 1.15)
- term-dropout pass: `False` (PF floor 1.0)
- hard reasons: `TRAIN too few trades (train_n<20); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates)`
- insufficient reasons: `-`
- warnings: `-`

- **Keep/reject:** REJECT  — TRAIN too few trades (train_n<20); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates)

## Command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup DOC5A_AVWAP_PULLBACK_LONG --pool C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\Train_and_Test\setup_pf_1_4_approval_loop\DOC5A_AVWAP_PULLBACK_LONG\variant_pool --trials 900 --time_budget_min 26.0 --seed 7 --gap_lambda 0.8 --max_mask_terms 2 --max_pm_terms 1 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```