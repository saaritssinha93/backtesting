# A_MOD_BREAK_C1_HIGH (LONG) — ITERATION_LOG

_Generated 2026-07-03. Optimizer: Optuna TPE. 10 FIT/VAL trials (each trial = one logical config: exit SL/Tgt + <=2 mask + <=2 pre-momentum + guards)._

Search protocol: optimise ONLY on FIT/VAL with the band objective `reward(min(FIT_PF,VAL_PF)) - 0.80*|FIT_PF-VAL_PF|`, where reward tents at PF 1.70 and penalises overshoot (anti-overfit). The single best FIT/VAL config is then confirmed on full TRAIN and scored ONCE on TEST. Quantile thresholds drawn from TRAIN only (never TEST). The robust gate then checks TEST PF+day-block p, TRAIN target-fill, threshold-neighborhood stability, and term-dropout stability.

Exit grid searched: SL [0.5, 0.7, 0.85, 1.0, 1.1, 1.2, 1.5] x Tgt [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5] (small/large SL x small/large target, asymmetric combos). Mask feats: ['body_pct>=0.781247', 'quality_score<=71.610911', 'rs_pct<=1.84328', 'rs_pct>=0.767875', 'upper_wick_pct>=0.050922', 'vol_ratio<=2.640825', 'vol_ratio>=1.62096'].

## Top 20 FIT/VAL trials (by band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|-----|------|--------|-------|----------|----------|-------|
| 1 | 0.85 | 2.5 | rs_pct>=0.767875;wick_skew_pct>=-0.026424 | pre3_close_pos>=0.451301;pre_entry_momentum_score>=68.227952 | {"min_slot": "09:45", "top_n": 1} | 149/0.434 | 153/0.377 | 0.3311 |
| 2 | 1.0 | 2.0 | - | - | {"max_slot": "13:00", "top_n": 3} | 332/0.387 | 353/0.329 | 0.2821 |
| 3 | 1.2 | 0.8 | vol_ratio>=1.62096 | - | {"max_slot": "13:00", "top_n": 3} | 426/0.356 | 493/0.302 | 0.2589 |
| 4 | 1.0 | 0.8 | rs_pct<=1.84328 | - | {"min_slot": "10:00", "max_slot": "13:00", "top_n": 2} | 177/0.368 | 276/0.298 | 0.2432 |
| 5 | 1.2 | 1.0 | quality_score<=71.610911;wick_skew_pct>=0.032453 | sig5_vol_ratio20<=1.734656;sig5_vol_ratio20<=1.581535 | {"min_slot": "10:00", "max_slot": "12:30", "top_n": 2} | 6/0.226 | 15/0.22 | 0.2147 |
| 6 | 1.0 | 1.0 | vol_ratio<=2.640825 | pre3_close_pos>=1.0;pre3_close_pos>=0.921655 | {"top_n": 2} | 191/0.362 | 176/0.267 | 0.1902 |
| 7 | 0.85 | 0.6 | upper_wick_pct>=0.050922;vol_ratio>=3.619864 | pre1_adx>=27.789069;pre1_adx<=39.116776 | {"min_slot": "09:45", "top_n": 3} | 44/0.234 | 59/0.205 | 0.182 |
| 8 | 1.1 | 1.25 | - | sig5_adx_calc>=25.559892 | {"top_n": 2} | 497/0.369 | 554/0.244 | 0.1448 |
| 9 | 1.1 | 2.5 | - | - | {"min_slot": "09:45", "max_slot": "14:00", "top_n": 1} | 413/0.501 | 457/0.296 | 0.1326 |
| 10 | 1.5 | 2.5 | body_pct>=0.781247;upper_wick_pct>=0.128679 | pre1_adx>=34.851118;sig5_rsi_dir<=67.723919 | {"top_n": 2} | 0/0.0 | 1/0.0 | -5.0 |

## Best config — confirmation runs

- **Best (FIT/VAL):** SL 0.85/Tgt 2.5 | mask [rs_pct>=0.767875; wick_skew_pct>=-0.026424] | premom [pre3_close_pos>=0.451301; pre_entry_momentum_score>=68.227952] | guard {'min_slot': '09:45', 'top_n': 1} | maxpos 10 | dloss 4000.0
- **TRAIN @15bps:** n=302 PF=0.404 net=Rs-131,397 win%=23.2 avgW=Rs1,272 avgL=Rs-950 maxDD=Rs-136,740 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=6.16 tradeDom=0.025 dayDom=9.99 symDom=9.99 dbp=1.0
- **TEST  @15bps:** n=116 PF=0.281 net=Rs-62,010 win%=19.8 avgW=Rs1,055 avgL=Rs-928 maxDD=Rs-62,122 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=5.8 tradeDom=0.093 dayDom=9.99 symDom=9.99 dbp=1.0
- **TRAIN @5bps:**  n=302 PF=0.404 net=Rs-131,397 win%=23.2 avgW=Rs1,272 avgL=Rs-950 maxDD=Rs-136,740 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=6.16 tradeDom=0.025 dayDom=9.99 symDom=9.99 dbp=1.0
- **TEST  @5bps:**  n=116 PF=0.281 net=Rs-62,010 win%=19.8 avgW=Rs1,055 avgL=Rs-928 maxDD=Rs-62,122 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=5.8 tradeDom=0.093 dayDom=9.99 symDom=9.99 dbp=1.0

## Robustness diagnostics

- neighborhood pass: `False` (PF floor 1.15)
- term-dropout pass: `False` (PF floor 1.0)
- hard reasons: `TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); TRAIN too many trades/day; neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates)`
- insufficient reasons: `-`
- warnings: `-`

- **Keep/reject:** REJECT  — TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); TRAIN too many trades/day; neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates)

## Command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup A_MOD_BREAK_C1_HIGH --pool Train_and_Test/setup_pf_1_4_full_loop/A_MOD_BREAK_C1_HIGH/pools/pool_full --trials 700 --time_budget_min 12.0 --seed 41 --gap_lambda 0.8 --max_mask_terms 2 --max_pm_terms 2 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```