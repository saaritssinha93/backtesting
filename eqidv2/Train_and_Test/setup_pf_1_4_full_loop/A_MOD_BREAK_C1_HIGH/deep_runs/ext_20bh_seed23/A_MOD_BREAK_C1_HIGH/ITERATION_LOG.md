# A_MOD_BREAK_C1_HIGH (LONG) — ITERATION_LOG

_Generated 2026-07-03. Optimizer: Optuna TPE. 27 FIT/VAL trials (each trial = one logical config: exit SL/Tgt + <=2 mask + <=2 pre-momentum + guards)._

Search protocol: optimise ONLY on FIT/VAL with the band objective `reward(min(FIT_PF,VAL_PF)) - 0.80*|FIT_PF-VAL_PF|`, where reward tents at PF 1.70 and penalises overshoot (anti-overfit). The single best FIT/VAL config is then confirmed on full TRAIN and scored ONCE on TEST. Quantile thresholds drawn from TRAIN only (never TEST). The robust gate then checks TEST PF+day-block p, TRAIN target-fill, threshold-neighborhood stability, and term-dropout stability.

Exit grid searched: SL [0.5, 0.7, 0.85, 1.0, 1.1, 1.2, 1.5] x Tgt [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5] (small/large SL x small/large target, asymmetric combos). Mask feats: ['above_or_high>=0.0', 'above_pdh>=0.0', 'adx_x<=40.479081', 'bb_pos>=0.993159', 'break_margin_atr<=1.524902', 'lower_wick_pct<=0.03243', 'pdh_dist_atr<=-3.267963', 'price_level<=456.320007', 'rsi_slope3<=5.600627', 'signal_range_pct>=0.544426', 'wick_skew_pct>=0.034068'].

## Top 20 FIT/VAL trials (by band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|-----|------|--------|-------|----------|----------|-------|
| 1 | 1.1 | 2.5 | - | - | {"min_slot": "09:45", "max_slot": "13:00"} | 618/0.538 | 657/0.541 | 0.5363 |
| 2 | 1.1 | 2.5 | - | - | {"min_slot": "09:45", "max_slot": "13:00"} | 618/0.538 | 657/0.541 | 0.5363 |
| 3 | 1.1 | 2.5 | - | - | {"min_slot": "09:45", "max_slot": "13:00"} | 618/0.538 | 657/0.541 | 0.5363 |
| 4 | 1.1 | 2.5 | - | - | {"min_slot": "09:45", "max_slot": "13:00"} | 618/0.538 | 657/0.541 | 0.5363 |
| 5 | 1.1 | 2.5 | - | - | {"min_slot": "09:45", "max_slot": "13:00"} | 618/0.538 | 657/0.541 | 0.5363 |
| 6 | 1.1 | 2.5 | - | - | {"min_slot": "09:45", "max_slot": "13:00"} | 618/0.538 | 657/0.541 | 0.5363 |
| 7 | 1.2 | 2.5 | - | - | {"min_slot": "10:00", "max_slot": "14:30"} | 684/0.541 | 707/0.563 | 0.5233 |
| 8 | 1.1 | 2.5 | - | - | {"min_slot": "10:00", "max_slot": "14:30"} | 693/0.533 | 720/0.548 | 0.5218 |
| 9 | 1.5 | 2.5 | - | - | {"min_slot": "10:30", "max_slot": "11:05"} | 193/0.564 | 242/0.635 | 0.508 |
| 10 | 1.1 | 2.5 | - | - | {"min_slot": "11:00", "max_slot": "11:05"} | 185/0.552 | 206/0.628 | 0.4909 |
| 11 | 1.0 | 2.0 | price_level<=456.320007 | - | {"min_slot": "10:30", "max_slot": "12:30", "top_n": 1} | 127/0.493 | 113/0.478 | 0.4669 |
| 12 | 1.1 | 2.5 | - | pre_entry_momentum_score>=53.796199 | {"min_slot": "09:45", "max_slot": "13:00"} | 730/0.504 | 735/0.481 | 0.4629 |
| 13 | 1.1 | 2.0 | - | - | {"min_slot": "09:45", "max_slot": "13:00"} | 627/0.503 | 694/0.559 | 0.4578 |
| 14 | 1.1 | 1.0 | - | - | {"min_slot": "09:45", "max_slot": "13:00"} | 749/0.445 | 843/0.48 | 0.4182 |
| 15 | 1.1 | 1.0 | - | - | {"min_slot": "09:45", "max_slot": "13:00"} | 749/0.445 | 843/0.48 | 0.4182 |
| 16 | 1.5 | 2.5 | break_margin_atr<=1.524902 | sig5_vol_ratio20<=2.003947 | {"min_slot": "09:45", "max_slot": "13:00"} | 487/0.498 | 499/0.44 | 0.393 |
| 17 | 1.1 | 0.8 | - | - | {"min_slot": "09:45", "max_slot": "12:00"} | 637/0.449 | 676/0.409 | 0.3778 |
| 18 | 0.85 | 1.0 | bb_pos>=0.993159;above_or_high>=1.0 | - | {"max_slot": "14:00", "top_n": 2} | 507/0.42 | 372/0.367 | 0.3249 |
| 19 | 1.2 | 0.6 | wick_skew_pct>=0.034068 | pre1_adx<=31.176932 | {"top_n": 2} | 164/0.329 | 172/0.312 | 0.2986 |
| 20 | 1.0 | 1.25 | - | - | {"min_slot": "11:00", "max_slot": "14:00", "top_n": 2} | 758/0.538 | 973/0.389 | 0.2692 |

## Best config — confirmation runs

- **Best (FIT/VAL):** SL 1.1/Tgt 2.5 | mask [(none)] | premom [(none)] | guard {'min_slot': '09:45', 'max_slot': '13:00'} | maxpos 20 | dloss 4000.0
- **TRAIN @15bps:** n=1275 PF=0.539 net=Rs-448,920 win%=30.4 avgW=Rs1,359 avgL=Rs-1,098 maxDD=Rs-448,605 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=25.5 tradeDom=0.004 dayDom=9.99 symDom=9.99 dbp=1.0
- **TEST  @15bps:** n=398 PF=0.395 net=Rs-200,953 win%=25.6 avgW=Rs1,287 avgL=Rs-1,123 maxDD=Rs-204,709 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=18.95 tradeDom=0.017 dayDom=9.99 symDom=9.99 dbp=1.0
- **TRAIN @5bps:**  n=1275 PF=0.539 net=Rs-448,920 win%=30.4 avgW=Rs1,359 avgL=Rs-1,098 maxDD=Rs-448,605 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=25.5 tradeDom=0.004 dayDom=9.99 symDom=9.99 dbp=1.0
- **TEST  @5bps:**  n=398 PF=0.395 net=Rs-200,953 win%=25.6 avgW=Rs1,287 avgL=Rs-1,123 maxDD=Rs-204,709 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=18.95 tradeDom=0.017 dayDom=9.99 symDom=9.99 dbp=1.0

## Robustness diagnostics

- neighborhood pass: `False` (PF floor 1.15)
- term-dropout pass: `False` (PF floor 1.0)
- hard reasons: `TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); TRAIN too many trades/day; neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates); TEST too many trades/day`
- insufficient reasons: `-`
- warnings: `-`

- **Keep/reject:** REJECT  — TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); TRAIN too many trades/day; neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates); TEST too many trades/day

## Command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup A_MOD_BREAK_C1_HIGH --pool Train_and_Test/setup_pf_1_4_full_loop/A_MOD_BREAK_C1_HIGH/pools/pool_enriched_first_20bh --trials 600 --time_budget_min 14.0 --seed 23 --gap_lambda 0.8 --max_mask_terms 2 --max_pm_terms 2 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```