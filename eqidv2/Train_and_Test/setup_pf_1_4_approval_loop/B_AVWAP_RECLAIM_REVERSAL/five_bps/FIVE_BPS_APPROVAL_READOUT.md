# B_AVWAP_RECLAIM_REVERSAL - 5 bps Approval Readout

No config was promoted. No edits were made to `final_setup_conf.py` or
`Train_and_Test/final_setup_conf.py`.

## Window

- TRAIN: 2026-05-18 through 2026-06-16, 20 sessions
- FIT/VAL: first 10 TRAIN sessions / last 10 TRAIN sessions
- TEST: 2026-06-22 and 2026-06-24, 2 sessions
- Cost assumption: 5 bps/leg slippage, normal 1-minute resolver/EOD behavior

## Baseline Card Config

- Rules: `vwap_dist_atr <= 1.0`
- Exit: SL 0.70%, target 1.50%
- TRAIN: 573 trades, PF 0.5374, net Rs -131,275, win 29.49%, maxDD Rs -138,558
- TEST: 60 trades, PF 0.4798, net Rs -15,844, win 26.67%, maxDD Rs -16,904

## Approval Result

- Passing configs: 0
- Required gate: TRAIN PF 1.30-1.70, TEST PF > 1.40, minimum counts, stable trade/day/symbol dominance
- Recommendation: NO, do not promote

## Best FIT/VAL Score, But Not Approvable

- Config: SL 0.80%, target 1.50%
- Rules: `vwap_dist_atr <= 1.0`, `vol_ratio >= 4.410413`, `atr_pct >= 0.002151`
- Pre-momentum: `pre1_adx <= 17.56403`
- TRAIN: 14 trades, PF 1.7154, net Rs 2,663
- Failure: TRAIN PF above band, trade count below 15, unstable day/symbol dominance; TEST not scored by anti-overfit rule

## Closest OOS Near-Misses

1. Trial 259
   - Config: SL 0.50%, target 2.00%; `close_loc >= 0.983273`, `rs_pct <= 0.525739`, `regime != BULL`; guard 11:00-14:30, top_n 2
   - TRAIN: 45 trades, PF 1.4077, net Rs 5,226
   - TEST: 6 trades, PF 1.0483, net Rs 98
   - Failure: TEST PF below 1.40 and dominance unstable

2. Trial 458
   - Config: SL 0.50%, target 2.00%; `close_loc >= 0.983273`, `rs_pct <= 0.525739`, `regime == NEUTRAL`; guard 09:45-14:00, top_n 2
   - TRAIN: 38 trades, PF 1.5711, net Rs 5,878
   - TEST: 3 trades, PF 1.9093, net Rs 887
   - Failure: TEST trade count below 5 and dominance unstable

## Rerun

```powershell
python Train_and_Test\setup_pf_1_4_approval_loop\B_AVWAP_RECLAIM_REVERSAL\five_bps\scripts\pf_band_search_5bps.py --setup B_AVWAP_RECLAIM_REVERSAL --pool C:\TradingData\eqidv2\setup_pools_2026_06_29\B_AVWAP_RECLAIM_REVERSAL --train_start 2026-05-18 --test_start 2026-06-20 --trials 500 --time_budget_min 25 --seed 7 --slippage_bps 5 --out Train_and_Test\setup_pf_1_4_approval_loop\B_AVWAP_RECLAIM_REVERSAL\five_bps
```
