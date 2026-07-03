# A_MOD_BREAK_C1_HIGH (LONG) — ITERATION_LOG

_Generated 2026-07-03. Optimizer: Optuna TPE. 600 FIT/VAL trials (each trial = one logical config: exit SL/Tgt + <=2 mask + <=2 pre-momentum + guards)._

Search protocol: optimise ONLY on FIT/VAL with the band objective `reward(min(FIT_PF,VAL_PF)) - 0.80*|FIT_PF-VAL_PF|`, where reward tents at PF 1.70 and penalises overshoot (anti-overfit). The single best FIT/VAL config is then confirmed on full TRAIN and scored ONCE on TEST. Quantile thresholds drawn from TRAIN only (never TEST). The robust gate then checks TEST PF+day-block p, TRAIN target-fill, threshold-neighborhood stability, and term-dropout stability.

Exit grid searched: SL [0.5, 0.7, 0.85, 1.0, 1.1, 1.2, 1.5] x Tgt [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5] (small/large SL x small/large target, asymmetric combos). Mask feats: ['above_or_high>=1.0', 'above_pdh<=1.0', 'above_pdh>=0.0', 'above_pdh>=1.0', 'adx_x<=43.734341', 'adx_x>=23.379833', 'atr_pct<=0.004425', 'atr_pct>=0.002492', 'atr_pct>=0.003312', 'bar_of_day>=20.0', 'bb_pos<=0.877426', 'bb_pos>=0.988304'].

## Top 20 FIT/VAL trials (by band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|-----|------|--------|-------|----------|----------|-------|
| 1 | 1.5 | 1.5 | close_loc>=0.858228 | sig5_adx_calc<=55.099678;pre1_adx<=31.132763 | {"min_slot": "09:30", "max_slot": "12:00", "top_n": 2} | 17/0.862 | 7/0.92 | 0.8161 |
| 2 | 1.5 | 1.5 | close_loc>=0.858228 | sig5_rsi_dir<=81.079407;pre1_adx<=31.132763 | {"max_slot": "12:00", "top_n": 2} | 17/0.862 | 7/0.92 | 0.8161 |
| 3 | 1.5 | 1.5 | ema20_slope5_atr>=0.261616;rsi_slope3>=4.238083 | sig5_rsi_dir<=81.079407;pre3_range_r>=0.163286 | {"max_slot": "13:00", "top_n": 2} | 33/0.787 | 31/0.777 | 0.7691 |
| 4 | 1.5 | 1.5 | ema20_slope5_atr>=0.261616;rsi_slope3>=4.238083 | sig5_rsi_dir<=81.079407;pre3_range_r>=0.163286 | {"max_slot": "13:00", "top_n": 2} | 33/0.787 | 31/0.777 | 0.7691 |
| 5 | 1.5 | 1.5 | ema20_slope5_atr>=0.261616 | sig5_rsi_dir<=76.944656;pre3_range_r>=0.163286 | {"max_slot": "13:00", "top_n": 2} | 34/0.704 | 33/0.698 | 0.6941 |
| 6 | 1.5 | 1.5 | cci_x<=198.466888 | sig5_vol_ratio20<=3.204469;pre3_range_r<=0.163286 | {"max_slot": "12:00", "top_n": 2} | 12/0.656 | 9/0.647 | 0.6406 |
| 7 | 1.5 | 1.5 | cci_x>=110.532584 | sig5_rsi_dir<=81.079407;pre3_close_pos<=0.891426 | {"max_slot": "12:00", "top_n": 2} | 26/0.665 | 22/0.717 | 0.6224 |
| 8 | 1.5 | 1.5 | pre5_ret_atr<=2.45614 | sig5_vol_ratio20<=4.008314;pre3_range_r<=0.163286 | {"max_slot": "12:00", "top_n": 2} | 11/0.816 | 16/0.705 | 0.6168 |
| 9 | 1.5 | 1.5 | close_loc>=0.985189 | pre3_range_r<=0.948951;sig5_rsi_dir<=74.769672 | {"max_slot": "12:00", "top_n": 2} | 12/0.595 | 10/0.597 | 0.5937 |
| 10 | 0.7 | 2.0 | atr_pct>=0.002492 | sig5_rsi_dir<=76.944656;pre1_adx<=38.788589 | {"min_slot": "09:30", "max_slot": "12:30", "top_n": 2} | 29/0.635 | 22/0.707 | 0.5773 |
| 11 | 0.7 | 1.5 | atr_pct>=0.002492 | sig5_rsi_dir<=76.944656;pre1_adx<=38.788589 | {"min_slot": "09:30", "max_slot": "12:30", "top_n": 2} | 29/0.581 | 22/0.577 | 0.5746 |
| 12 | 1.5 | 1.25 | - | sig5_rsi_dir<=76.944656 | {"top_n": 2} | 43/0.578 | 51/0.572 | 0.568 |
| 13 | 1.5 | 1.25 | - | sig5_rsi_dir<=76.944656 | {"top_n": 2} | 43/0.578 | 51/0.572 | 0.568 |
| 14 | 1.5 | 1.5 | - | sig5_rsi_dir<=76.944656 | {"max_slot": "12:00", "top_n": 2} | 43/0.599 | 51/0.64 | 0.5663 |
| 15 | 1.5 | 1.5 | - | sig5_rsi_dir<=76.944656 | {"max_slot": "12:00", "top_n": 2} | 43/0.599 | 51/0.64 | 0.5663 |
| 16 | 1.5 | 1.5 | - | sig5_rsi_dir<=76.944656 | {"max_slot": "12:00", "top_n": 2} | 43/0.599 | 51/0.64 | 0.5663 |
| 17 | 1.5 | 1.5 | - | sig5_rsi_dir<=76.944656 | {"max_slot": "12:00", "top_n": 2} | 43/0.599 | 51/0.64 | 0.5663 |
| 18 | 1.5 | 1.5 | - | sig5_rsi_dir<=76.944656 | {"max_slot": "12:00", "top_n": 2} | 43/0.599 | 51/0.64 | 0.5663 |
| 19 | 1.5 | 1.5 | - | sig5_rsi_dir<=76.944656 | {"max_slot": "12:00", "top_n": 2} | 43/0.599 | 51/0.64 | 0.5663 |
| 20 | 1.5 | 1.5 | - | sig5_rsi_dir<=76.944656 | {"max_slot": "12:00", "top_n": 2} | 43/0.599 | 51/0.64 | 0.5663 |

## Best config — confirmation runs

- **Best (FIT/VAL):** SL 1.5/Tgt 1.5 | mask [close_loc>=0.858228] | premom [sig5_rsi_dir<=81.079407; pre1_adx<=31.132763] | guard {'max_slot': '12:00', 'top_n': 2} | maxpos 20 | dloss 4000.0
- **TRAIN @15bps:** n=24 PF=0.878 net=Rs-1,556 win%=50.0 avgW=Rs931 avgL=Rs-1,061 maxDD=Rs-4,455 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.41 tradeDom=0.113 dayDom=9.99 symDom=9.99 dbp=0.6119
- **TEST  @15bps:** n=15 PF=0.401 net=Rs-7,090 win%=33.3 avgW=Rs948 avgL=Rs-1,183 maxDD=Rs-8,838 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.15 tradeDom=0.267 dayDom=9.99 symDom=9.99 dbp=0.9704
- **TRAIN @5bps:**  n=24 PF=0.878 net=Rs-1,556 win%=50.0 avgW=Rs931 avgL=Rs-1,061 maxDD=Rs-4,455 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.41 tradeDom=0.113 dayDom=9.99 symDom=9.99 dbp=0.6119
- **TEST  @5bps:**  n=15 PF=0.401 net=Rs-7,090 win%=33.3 avgW=Rs948 avgL=Rs-1,183 maxDD=Rs-8,838 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.15 tradeDom=0.267 dayDom=9.99 symDom=9.99 dbp=0.9704

## Robustness diagnostics

- neighborhood pass: `False` (PF floor 1.15)
- term-dropout pass: `False` (PF floor 1.0)
- hard reasons: `TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates)`
- insufficient reasons: `-`
- warnings: `-`

- **Keep/reject:** REJECT  — TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates)

## Command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup A_MOD_BREAK_C1_HIGH --pool Train_and_Test/setup_pf_1_4_full_loop/A_MOD_BREAK_C1_HIGH/pools/pool_enriched_first_am --trials 600 --time_budget_min 12.0 --seed 23 --gap_lambda 0.8 --max_mask_terms 2 --max_pm_terms 2 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```