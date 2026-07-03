# C_OR_BREAKOUT — REDESIGNED_SETUP_IDEAS (Stage 3 versions)

_Generated 2026-07-03._

Each version is one logical redesign of the setup; scored on FIT/VAL before any sweeps.

| version | config | FIT n/PF | VAL n/PF | band score | verdict |
|---|---|---|---|---|---|
| V0_raw | `SL0.9/T2.0` | 1085.0/0.421 | 840.0/0.289 | 0.1834 | keep |
| V1_mid_exit | `SL0.9/T1.25` | 1188.0/0.387 | 935.0/0.3 | 0.2304 | keep |
| V2_fresh_break | `SL0.9/T2.0 pre[fire_seq<=1.0]` | 1107.0/0.554 | 828.0/0.385 | 0.2498 | keep |
| V3_fresh_morning | `SL0.9/T2.0 pre[fire_seq<=1.0] g{"max_slot": "12:30"}` | 711.0/0.572 | 572.0/0.371 | 0.2102 | keep |
| V4_rs_leader | `SL0.9/T2.0 pre[rs_pct>=3.792928]` | 635.0/0.386 | 401.0/0.379 | 0.3734 | keep |
| V5_not_overext | `SL0.9/T2.0 mask[vwap_dist_atr<=5.231769]` | 1036.0/0.395 | 843.0/0.288 | 0.2024 | keep |
| V6_vol_band | `SL0.9/T2.0 pre[vol_ratio>=1.8;vol_ratio<=3.2]` | 817.0/0.387 | 696.0/0.309 | 0.2466 | keep |
| V7_candle_quality | `SL0.9/T2.0 pre[body_pct>=0.6;upper_wick_pct<=0.25]` | 955.0/0.447 | 786.0/0.312 | 0.204 | keep |
| V8_bull_tape | `SL0.9/T2.0 mask[market_ret_pct>=0.0]` | 740.0/0.341 | 629.0/0.257 | 0.1898 | keep |
| V9_ranked_top2 | `SL0.9/T2.0 g{"top_n": 2}` | 975.0/0.481 | 776.0/0.242 | 0.0508 | keep |
| V10_late_drift | `SL0.9/T2.0 g{"min_slot": "13:00"}` | 680.0/0.404 | 535.0/0.305 | 0.2258 | keep |
| V11_broad_gate | `SL0.9/T2.0 pm[sig5_adx_calc>=30.0;pre1_adx<=25.0]` | 399.0/0.363 | 246.0/0.307 | 0.2622 | keep |
| V12_fresh_vol | `SL0.9/T2.0 pre[fire_seq<=1.0;vol_ratio>=2.0]` | 1058.0/0.502 | 820.0/0.36 | 0.2464 | keep |