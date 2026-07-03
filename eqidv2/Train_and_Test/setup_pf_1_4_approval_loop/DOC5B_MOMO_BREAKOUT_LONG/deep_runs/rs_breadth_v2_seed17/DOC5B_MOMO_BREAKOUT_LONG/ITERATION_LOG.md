# DOC5B_MOMO_BREAKOUT_LONG (LONG) — ITERATION_LOG

_Generated 2026-07-01. Optimizer: Optuna TPE. 800 FIT/VAL trials (each trial = one logical config: exit SL/Tgt + <=2 mask + <=2 pre-momentum + guards)._

Search protocol: optimise ONLY on FIT/VAL with the band objective `reward(min(FIT_PF,VAL_PF)) - 0.80*|FIT_PF-VAL_PF|`, where reward tents at PF 1.70 and penalises overshoot (anti-overfit). The single best FIT/VAL config is then confirmed on full TRAIN and scored ONCE on TEST. Quantile thresholds drawn from TRAIN only (never TEST). The robust gate then checks TEST PF+day-block p, TRAIN target-fill, threshold-neighborhood stability, and term-dropout stability.

Exit grid searched: SL [0.5, 0.7, 0.85, 1.0, 1.1, 1.2, 1.5] x Tgt [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5] (small/large SL x small/large target, asymmetric combos). Mask feats: ['atr_pct<=0.002734', 'atr_pct<=0.003823', 'atr_pct>=0.00236', 'atr_pct>=0.002734', 'atr_pct>=0.003353', 'atr_pct>=0.003823', 'atr_pct>=0.004344', 'atr_pct>=0.004961', 'body_pct<=0.8', 'body_pct>=0.457143', 'body_pct>=0.56962', 'body_pct>=0.636364'].

## Top 20 FIT/VAL trials (by band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|-----|------|--------|-------|----------|----------|-------|
| 1 | 1.2 | 2.0 | quality_score>=109.099605;lower_wick_pct>=0.022843 | pre_entry_momentum_score<=68.3712;sig5_vol_ratio20<=2.777304 | {"max_slot": "13:00", "top_n": 2} | 16/0.648 | 13/0.658 | 0.6396 |
| 2 | 1.2 | 2.0 | quality_score>=109.099605;lower_wick_pct>=0.022843 | pre_entry_momentum_score<=68.3712;sig5_vol_ratio20<=2.777304 | {"max_slot": "13:00", "top_n": 2} | 16/0.648 | 13/0.658 | 0.6396 |
| 3 | 1.2 | 2.0 | quality_score>=109.099605;lower_wick_pct>=0.022843 | pre_entry_momentum_score<=68.3712;sig5_vol_ratio20<=2.777304 | {"max_slot": "13:00", "top_n": 2} | 16/0.648 | 13/0.658 | 0.6396 |
| 4 | 1.2 | 2.0 | quality_score>=109.099605;lower_wick_pct>=0.022843 | pre_entry_momentum_score<=68.3712;sig5_vol_ratio20<=2.777304 | {"max_slot": "13:00", "top_n": 2} | 16/0.648 | 13/0.658 | 0.6396 |
| 5 | 1.2 | 2.0 | quality_score>=109.099605;lower_wick_pct>=0.022843 | pre_entry_momentum_score<=68.3712;sig5_vol_ratio20<=2.777304 | {"top_n": 1} | 13/0.645 | 9/0.654 | 0.6366 |
| 6 | 1.2 | 2.0 | quality_score>=109.099605;lower_wick_pct>=0.022843 | pre_entry_momentum_score<=68.3712;sig5_vol_ratio20<=2.777304 | {"top_n": 1} | 13/0.645 | 9/0.654 | 0.6366 |
| 7 | 1.2 | 2.0 | ranker_score>=109.099605;close_loc>=0.831579 | pre_entry_momentum_score>=48.20169;sig5_vol_ratio20<=2.777304 | {"min_slot": "09:30", "max_slot": "13:00", "top_n": 1} | 10/0.809 | 9/0.712 | 0.635 |
| 8 | 1.2 | 2.0 | ranker_score>=109.099605;close_loc>=0.831579 | pre_entry_momentum_score>=48.20169;sig5_vol_ratio20<=2.777304 | {"top_n": 1} | 10/0.809 | 9/0.712 | 0.635 |
| 9 | 1.2 | 2.0 | ranker_score>=109.099605;close_loc>=0.831579 | pre_entry_momentum_score>=48.20169;sig5_vol_ratio20<=2.777304 | {"top_n": 1} | 10/0.809 | 9/0.712 | 0.635 |
| 10 | 1.1 | 2.0 | ranker_score>=109.099605;wick_skew_pct>=0.015076 | pre_entry_momentum_score<=68.3712;sig5_vol_ratio20<=2.777304 | {"max_slot": "14:30", "top_n": 2} | 15/0.701 | 11/0.655 | 0.6187 |
| 11 | 1.2 | 2.0 | ranker_score>=109.099605;close_loc>=0.831579 | sig5_vol_ratio20>=1.34483;sig5_vol_ratio20<=2.777304 | {"min_slot": "09:30", "max_slot": "13:00"} | 23/0.612 | 17/0.621 | 0.605 |
| 12 | 1.1 | 2.0 | quality_score>=109.099605;breakout_strength_atr>=0.218263 | pre_entry_momentum_score<=68.3712;sig5_vol_ratio20<=2.777304 | {"top_n": 1} | 17/0.582 | 14/0.582 | 0.5816 |
| 13 | 1.1 | 2.0 | quality_score>=109.099605;wick_skew_pct>=0.0 | pre_entry_momentum_score<=68.3712;sig5_vol_ratio20<=2.777304 | {"top_n": 1} | 14/0.628 | 10/0.601 | 0.5798 |
| 14 | 1.1 | 2.0 | ranker_score>=109.099605;wick_skew_pct>=0.0 | pre_entry_momentum_score<=68.3712;sig5_vol_ratio20<=2.777304 | {"top_n": 1} | 14/0.628 | 10/0.601 | 0.5798 |
| 15 | 1.1 | 2.0 | ranker_score>=109.099605;wick_skew_pct>=0.0 | pre_entry_momentum_score<=68.3712;sig5_vol_ratio20<=2.777304 | {"top_n": 1} | 14/0.628 | 10/0.601 | 0.5798 |
| 16 | 1.1 | 2.0 | ranker_score>=109.099605;wick_skew_pct>=0.0 | pre_entry_momentum_score<=68.3712;sig5_vol_ratio20<=2.777304 | {"top_n": 1} | 14/0.628 | 10/0.601 | 0.5798 |
| 17 | 1.1 | 2.0 | quality_score>=109.099605;wick_skew_pct>=0.0 | pre_entry_momentum_score<=68.3712;sig5_vol_ratio20<=2.777304 | {"top_n": 1} | 14/0.628 | 10/0.601 | 0.5798 |
| 18 | 1.1 | 2.0 | quality_score>=109.099605;wick_skew_pct>=0.0 | pre_entry_momentum_score<=68.3712;sig5_vol_ratio20<=2.777304 | {"top_n": 1} | 14/0.628 | 10/0.601 | 0.5798 |
| 19 | 1.1 | 2.0 | quality_score>=109.099605;wick_skew_pct>=0.0 | pre_entry_momentum_score<=68.3712;sig5_vol_ratio20<=2.777304 | {"top_n": 1} | 14/0.628 | 10/0.601 | 0.5798 |
| 20 | 1.1 | 2.0 | quality_score>=109.099605;wick_skew_pct>=0.0 | pre_entry_momentum_score<=68.3712;sig5_vol_ratio20<=2.777304 | {"top_n": 1} | 14/0.628 | 10/0.601 | 0.5798 |

## Best config — confirmation runs

- **Best (FIT/VAL):** SL 1.2/Tgt 2.0 | mask [quality_score>=109.099605; lower_wick_pct>=0.022843] | premom [pre_entry_momentum_score<=68.3712; sig5_vol_ratio20<=2.777304] | guard {'max_slot': '13:00', 'top_n': 2} | maxpos 20 | dloss 0.0
- **TRAIN @15bps:** n=29 PF=0.653 net=Rs-5,767 win%=41.4 avgW=Rs905 avgL=Rs-978 maxDD=Rs-6,157 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.23 tradeDom=0.163 dayDom=9.99 symDom=9.99 dbp=0.8792
- **TEST  @15bps:** n=10 PF=0.329 net=Rs-7,157 win%=20.0 avgW=Rs1,758 avgL=Rs-1,334 maxDD=Rs-5,738 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=3.33 tradeDom=0.502 dayDom=9.99 symDom=9.99 dbp=1.0
- **TRAIN @5bps:**  n=29 PF=0.653 net=Rs-5,767 win%=41.4 avgW=Rs905 avgL=Rs-978 maxDD=Rs-6,157 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.23 tradeDom=0.163 dayDom=9.99 symDom=9.99 dbp=0.8792
- **TEST  @5bps:**  n=10 PF=0.329 net=Rs-7,157 win%=20.0 avgW=Rs1,758 avgL=Rs-1,334 maxDD=Rs-5,738 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=3.33 tradeDom=0.502 dayDom=9.99 symDom=9.99 dbp=1.0

## Robustness diagnostics

- neighborhood pass: `False` (PF floor 1.15)
- term-dropout pass: `False` (PF floor 1.0)
- hard reasons: `TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates)`
- insufficient reasons: `-`
- warnings: `-`

- **Keep/reject:** REJECT  — TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates)

## Command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup DOC5B_MOMO_BREAKOUT_LONG --pool Train_and_Test\doc5_long_setups\pool_rs_breadth_v2 --trials 800 --time_budget_min 12.0 --seed 17 --gap_lambda 0.8 --max_mask_terms 2 --max_pm_terms 2 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```