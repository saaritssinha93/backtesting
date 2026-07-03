# C_OR_BREAKDOWN — REDESIGNED_SETUP_IDEAS (Stage 3 versions)

_Generated 2026-07-03._

Each version is one logical redesign of the setup; scored on FIT/VAL before any sweeps.

| version | config | FIT n/PF | VAL n/PF | band score | verdict |
|---|---|---|---|---|---|
| V0_raw | `SL0.9/T2.0` | 1066.0/0.277 | 666.0/0.312 | 0.249 | keep |
| V1_conf_gate | `SL0.9/T2.0 pm[sig5_adx_calc>=39.670518;pre1_adx<=21.368044]` | 119.0/0.581 | 94.0/0.712 | 0.4762 | keep |
| V2_fresh_break | `SL0.9/T2.0 pre[fire_seq<=1.0]` | 1031.0/0.352 | 659.0/0.407 | 0.308 | keep |
| V3_fresh_morning | `SL0.9/T2.0 pre[fire_seq<=1.0] g{"max_slot": "12:30"}` | 698.0/0.375 | 481.0/0.414 | 0.3438 | keep |
| V4_trend_struct | `SL0.9/T2.0 pre[ema20_slope<=0.0;adx>=25.0]` | 0.0/0.0 | 0.0/0.0 | -5.0 | weak |
| V5_not_overext | `SL0.9/T2.0 pre[rsi>=25.0] mask[vwap_dist_atr>=-5.692257]` | 0.0/0.0 | 0.0/0.0 | -5.0 | weak |
| V6_vol_band | `SL0.9/T2.0 pre[vol_ratio>=1.8;vol_ratio<=3.2]` | 896.0/0.342 | 611.0/0.347 | 0.338 | keep |
| V7_candle_quality | `SL0.9/T2.0 pre[body_pct>=0.6;lower_wick_pct<=0.25]` | 999.0/0.294 | 655.0/0.352 | 0.2476 | keep |
| V8_bear_tape | `SL0.9/T2.0 mask[market_ret_pct<=0.0]` | 911.0/0.264 | 529.0/0.325 | 0.2152 | keep |
| V9_ranked_top2 | `SL0.9/T2.0 g{"top_n": 2}` | 930.0/0.346 | 580.0/0.365 | 0.3308 | keep |
| V10_late_drift | `SL0.9/T2.0 g{"min_slot": "13:00"}` | 699.0/0.324 | 477.0/0.444 | 0.228 | keep |
| V11_broad_gate | `SL0.9/T2.0 pm[sig5_adx_calc>=30.0;pre1_adx<=25.0]` | 313.0/0.543 | 239.0/0.507 | 0.4782 | keep |
| V12_fresh_vol | `SL0.9/T2.0 pre[fire_seq<=1.0;vol_ratio>=2.0]` | 957.0/0.376 | 642.0/0.372 | 0.3688 | keep |