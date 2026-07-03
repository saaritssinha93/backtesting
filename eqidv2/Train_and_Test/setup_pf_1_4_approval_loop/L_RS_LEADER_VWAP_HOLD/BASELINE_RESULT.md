# BASELINE_RESULT - L_RS_LEADER_VWAP_HOLD (LONG)

## Current Rules
- Source: `Train_and_Test/SETUP_CARDS_AND_LIVE_CROSSCHECK.md` and demoted config block in `final_setup_conf.py`.
- Indicator values: RSI 50-72, ADX >=20, EMA20 above EMA50, EMA20 slope >0, close above VWAP/EMA20.
- Non-indicator rules: strong green close, close_loc >=0.60, close > previous bar high, low tests VWAP + 0.30*ATR.
- Filters: quality_score>=97.121022; vol_ratio>=2.164331; vwap_dist_atr<=1.49336; signal_minute<=660.0
- Pre-momentum: none.
- Guards: none.
- Exit: fixed SL 0.50%, target 1.25%, 1-minute SL/target/EOD resolver.

## Exact Sessions
- FIT: 2026-03-16, 2026-03-17, 2026-03-18, 2026-03-20, 2026-03-24, 2026-03-25, 2026-04-01, 2026-04-06, 2026-04-08, 2026-04-09, 2026-04-10, 2026-04-15, 2026-04-16
- VAL: 2026-04-17, 2026-04-20, 2026-04-21, 2026-04-22, 2026-04-23, 2026-04-27, 2026-04-28, 2026-04-29, 2026-05-04, 2026-05-05, 2026-05-07, 2026-05-08, 2026-05-11, 2026-05-13
- TRAIN: 2026-03-16..2026-05-13 (27 sessions)
- TEST: 2026-05-14, 2026-05-15, 2026-05-18, 2026-05-19, 2026-05-20, 2026-05-22, 2026-05-25, 2026-05-26, 2026-05-27
- Note: requested TEST start 2026-06-20 unavailable for L_RS_LEADER_VWAP_HOLD; pool sessions end 2026-05-27. Used nearest available rolling split: TRAIN last 27 sessions before TEST, TEST last 9 sessions.

## Baseline Metrics
- FIT: n=1 PF=0.0 net=Rs-733 win=0.0% t/s/e=0/1/0 dom=9.99/9.99/9.99
- VAL: n=1 PF=inf net=Rs1,009 win=100.0% t/s/e=1/0/0 dom=1.0/1.0/1.0
- TRAIN: n=2 PF=1.3775 net=Rs277 win=50.0% t/s/e=1/1/0 dom=1.0/3.649/3.649
- TEST: n=1 PF=0.0 net=Rs-732 win=0.0% t/s/e=0/1/0 dom=9.99/9.99/9.99

## Initial Diagnosis
- The current card lands in the TRAIN PF band only because it leaves 2 TRAIN trades; this is not meaningful.
- TEST is one losing trade in the nearest available holdout.
- Meaningful-trade variants in the second pass remain well below TRAIN PF 1.30.
