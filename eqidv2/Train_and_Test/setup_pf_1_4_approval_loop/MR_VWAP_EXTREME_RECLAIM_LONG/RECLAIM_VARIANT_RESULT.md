# MR_VWAP_EXTREME_RECLAIM_LONG - Research Variant Result

Purpose:
- Improve `MR_CONTROLLED_VWAP_EXTREME_FADE_LONG` by waiting for reclaim confirmation instead of buying the raw VWAP extreme.

Research-only scanner:
- `Train_and_Test/setup_pf_1_4_approval_loop/MR_CONTROLLED_VWAP_EXTREME_FADE_LONG/scripts/scan_mr_vwap_extreme_reclaim_long.py`
- Output pool: `C:\TradingData\eqidv2\setup_pools_2026_06_29\MR_VWAP_EXTREME_RECLAIM_LONG`
- Full futures-universe scan: 204 tickers
- Final pool: 1,150 signals, 128 TRAIN sessions and 33 TEST sessions

Variant signal:
- Prior 30-minute VWAP washout: recent `vwap_dist_atr <= -1.75`
- Reclaim toward VWAP: current `-1.50 <= vwap_dist_atr <= 0.75`
- Current bar must close above previous high, green, strong close location, small upper wick
- Avoid BEAR market regime and materially weak market tape
- Require roughly non-negative relative strength

Baseline variant config:
- SL 0.70%, target 1.00%, no extra masks/pre-momentum
- TRAIN `2025-06-02..2026-03-25`: 955 trades, PF 0.257, net Rs -337,651
- TEST `2026-04-01..2026-05-29`: 195 trades, PF 0.215, net Rs -79,573

Approval-loop result:
- 500 Optuna trials plus coordinate loop
- Passing candidates: 0
- Approval recommendation: NO

Best Optuna near-miss:
- SL 0.70%, target 1.50%
- Mask: `vwap_dist_atr >= -0.819846`
- Pre-momentum: `sig5_adx_calc >= 30.794868`, `pre_entry_momentum_score >= 64.694989`
- Guard: `max_slot=12:30`, `top_n=1`
- TRAIN: 21 trades, PF 1.515, net Rs 3,538
- TEST: 0 trades
- Reject reason: no holdout participation.

Best looser tested configs with TEST participation:
- `ranker_score >= 50.172314` plus ADX pre-momentum: TRAIN 33 trades, PF 1.119, net Rs 1,320; TEST 6 trades, PF 0.257, net Rs -3,645
- `regime != BULL`, `atr_pct >= 0.00202`, `rs_pct >= 0.430791`, `close_loc >= 1.0`, `pre3_range_r <= 0.210408`: TRAIN 16-18 trades, PF 1.161-1.268; TEST 5-6 trades, PF 0.122-0.211, negative net

Conclusion:
- The reclaim idea is better than the raw fade in that it can create an in-sample TRAIN-band pocket.
- It still does not generalize into the Apr-May holdout.
- Do not promote to `final_setup_conf.py`.
- Keep both MR fade and MR reclaim parked unless new June+ data creates a materially different forward sample.
