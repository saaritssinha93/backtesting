# B_HUGE_C1_CLOSE_RECLAIM_BREAK (LONG) — ROUND2_RESULTS (enriched feature space)

_Generated 2026-07-03. ROUND 2 (enriched indicator/price-action feature space). Research-only; NO live trades; NO final_setup_conf.py edits._

- Optimizer: Optuna TPE | trials 800 (522 unique) | sweeps 384 | TEST evals used 0
- Windows: TRAIN 2026-03-04..2026-05-29 (51 sess) | TEST 2026-06-01..2026-07-01 (22 sess)
- Search space: 3 mask terms over base + enriched features (RSI/ADX/MACD/EMA/BB/Stoch/MFI/CCI/OBV/pressure/vol-z/ROC/W%R + day/gap/OR/prev-day geometry + prev-candle structure), 2 pre-momentum terms, regime, slots, top_n, max_positions, daily_loss, exit grid.
- **Passing candidates: 0**

## Baseline (round-1 conf/default config on this pool)

- FIT: n=416 PF=0.519 net=Rs-132,941 win%=34.1 avgW=Rs1,009 avgL=Rs-1,008 maxDD=Rs-133,463 SL/TGT/EOD=190/103/123 tpd=15.41 tradeDom=0.011 dayDom=9.99 symDom=9.99 dbp=1.0
- VAL: n=327 PF=0.458 net=Rs-114,650 win%=31.8 avgW=Rs932 avgL=Rs-949 maxDD=Rs-115,271 SL/TGT/EOD=136/67/124 tpd=17.21 tradeDom=0.013 dayDom=9.99 symDom=9.99 dbp=1.0
- TRAIN: n=743 PF=0.492 net=Rs-247,592 win%=33.1 avgW=Rs976 avgL=Rs-981 maxDD=Rs-249,855 SL/TGT/EOD=326/170/247 tpd=16.15 tradeDom=0.007 dayDom=9.99 symDom=9.99 dbp=1.0
- TEST: n=293 PF=0.475 net=Rs-91,606 win%=34.5 avgW=Rs820 avgL=Rs-908 maxDD=Rs-99,086 SL/TGT/EOD=113/53/127 tpd=14.65 tradeDom=0.015 dayDom=9.99 symDom=9.99 dbp=0.9998

## Finalists / rescue results

### finalist #1 — reject

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 1.0,
    "tgt_pct": 2.5
  },
  "mask_terms": [
    [
      "regime",
      "!=",
      "BULL"
    ]
  ],
  "pre_momentum_terms": [
    [
      "pre5_mom_r",
      ">=",
      0.21998
    ],
    [
      "pre5_mom_r",
      ">=",
      0.546221
    ]
  ],
  "entry_guards": {
    "min_slot": "12:00",
    "max_slot": "14:30",
    "top_n": 1
  },
  "max_positions": 10,
  "daily_loss_rs": 4000.0
}
```
- TRAIN: n=146 PF=0.973 net=Rs-2,486 win%=36.3 avgW=Rs1,714 avgL=Rs-1,003 maxDD=Rs-21,060 SL/TGT/EOD=67/35/44 tpd=4.06 tradeDom=0.025 dayDom=9.99 symDom=9.99 dbp=0.5685
- reasons: TRAIN not in band or too thin (PF 0.973, n 146)

### finalist #2 — reject

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 1.0,
    "tgt_pct": 2.5
  },
  "mask_terms": [
    [
      "regime",
      "!=",
      "BULL"
    ]
  ],
  "pre_momentum_terms": [
    [
      "pre3_close_pos",
      ">=",
      0.554175
    ],
    [
      "pre5_mom_r",
      ">=",
      0.546221
    ]
  ],
  "entry_guards": {
    "min_slot": "12:00",
    "max_slot": "14:30",
    "top_n": 1
  },
  "max_positions": 10,
  "daily_loss_rs": 4000.0
}
```
- TRAIN: n=131 PF=0.986 net=Rs-1,201 win%=37.4 avgW=Rs1,669 avgL=Rs-1,012 maxDD=Rs-18,253 SL/TGT/EOD=59/31/41 tpd=3.64 tradeDom=0.028 dayDom=9.99 symDom=9.99 dbp=0.5341
- reasons: TRAIN not in band or too thin (PF 0.986, n 131)

### finalist #3 — reject

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 1.1,
    "tgt_pct": 2.5
  },
  "mask_terms": [
    [
      "regime",
      "!=",
      "BULL"
    ]
  ],
  "pre_momentum_terms": [
    [
      "pre3_close_pos",
      ">=",
      0.554175
    ],
    [
      "pre5_mom_r",
      ">=",
      0.546221
    ]
  ],
  "entry_guards": {
    "min_slot": "10:00",
    "max_slot": "14:30",
    "top_n": 1
  },
  "max_positions": 10,
  "daily_loss_rs": 4000.0
}
```
- TRAIN: n=167 PF=0.756 net=Rs-30,175 win%=34.7 avgW=Rs1,612 avgL=Rs-1,135 maxDD=Rs-36,847 SL/TGT/EOD=83/34/50 tpd=4.51 tradeDom=0.024 dayDom=9.99 symDom=9.99 dbp=0.9705
- reasons: TRAIN not in band or too thin (PF 0.756, n 167)

### finalist #4 — reject

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 1.0,
    "tgt_pct": 2.5
  },
  "mask_terms": [
    [
      "regime",
      "!=",
      "BULL"
    ]
  ],
  "pre_momentum_terms": [
    [
      "pre3_close_pos",
      ">=",
      0.554175
    ],
    [
      "pre3_range_r",
      ">=",
      0.534926
    ]
  ],
  "entry_guards": {
    "min_slot": "11:00",
    "max_slot": "14:00",
    "top_n": 1
  },
  "max_positions": 10,
  "daily_loss_rs": 4000.0
}
```
- TRAIN: n=145 PF=0.745 net=Rs-27,376 win%=31.7 avgW=Rs1,736 avgL=Rs-1,083 maxDD=Rs-32,309 SL/TGT/EOD=81/29/35 tpd=3.92 tradeDom=0.028 dayDom=9.99 symDom=9.99 dbp=0.9609
- reasons: TRAIN not in band or too thin (PF 0.745, n 145)

### finalist #5 — reject

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 1.2,
    "tgt_pct": 2.5
  },
  "mask_terms": [
    [
      "regime",
      "!=",
      "BULL"
    ]
  ],
  "pre_momentum_terms": [
    [
      "pre3_close_pos",
      ">=",
      0.554175
    ],
    [
      "pre3_range_r",
      ">=",
      0.165674
    ]
  ],
  "entry_guards": {
    "min_slot": "11:00",
    "max_slot": "14:00",
    "top_n": 1
  },
  "max_positions": 10,
  "daily_loss_rs": 4000.0
}
```
- TRAIN: n=220 PF=0.735 net=Rs-42,299 win%=35.0 avgW=Rs1,523 avgL=Rs-1,116 maxDD=Rs-48,043 SL/TGT/EOD=91/39/90 tpd=5.37 tradeDom=0.019 dayDom=9.99 symDom=9.99 dbp=0.9934
- reasons: TRAIN not in band or too thin (PF 0.735, n 220)

### finalist #6 — reject

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 1.2,
    "tgt_pct": 2.5
  },
  "mask_terms": [
    [
      "regime",
      "!=",
      "BULL"
    ]
  ],
  "pre_momentum_terms": [
    [
      "pre1_adx",
      ">=",
      31.180706
    ],
    [
      "pre3_range_r",
      ">=",
      0.448233
    ]
  ],
  "entry_guards": {
    "top_n": 2
  },
  "max_positions": 20,
  "daily_loss_rs": 4000.0
}
```
- TRAIN: n=219 PF=0.734 net=Rs-46,266 win%=34.7 avgW=Rs1,682 avgL=Rs-1,217 maxDD=Rs-55,587 SL/TGT/EOD=109/48/62 tpd=4.98 tradeDom=0.018 dayDom=9.99 symDom=9.99 dbp=0.9844
- reasons: TRAIN not in band or too thin (PF 0.734, n 219)

## Top 25 FIT/VAL trials

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|----|------|--------|-------|----------|----------|-------|
| 1 | 1.0 | 2.5 | regime!=BULL | pre5_mom_r>=0.546221; pre5_mom_r>=0.21998 | {"min_slot": "12:00", "max_slot": "14:30", "top_n": 1} | 86/0.926 | 60/1.047 | 0.8303 |
| 2 | 1.0 | 2.5 | regime!=BULL | pre5_mom_r>=0.546221; pre3_close_pos>=0.554175 | {"min_slot": "12:00", "max_slot": "14:30", "top_n": 1} | 79/0.93 | 52/1.078 | 0.8116 |
| 3 | 1.0 | 2.5 | regime!=BULL | pre5_mom_r>=0.546221; pre3_close_pos>=0.554175 | {"min_slot": "09:45", "max_slot": "14:30", "top_n": 1} | 112/0.786 | 66/0.807 | 0.7695 |
| 4 | 1.0 | 2.5 | regime!=BULL | pre5_mom_r>=0.546221; pre3_close_pos>=0.554175 | {"min_slot": "10:00", "max_slot": "14:30", "top_n": 1} | 112/0.786 | 66/0.807 | 0.7695 |
| 5 | 1.0 | 2.5 | regime!=BULL | pre5_mom_r>=0.546221; pre3_close_pos>=0.554175 | {"min_slot": "10:30", "max_slot": "14:30", "top_n": 1} | 112/0.786 | 66/0.807 | 0.7695 |
| 6 | 1.1 | 2.5 | regime!=BULL | pre5_mom_r>=0.546221; pre3_close_pos>=0.554175 | {"min_slot": "10:00", "max_slot": "14:30", "top_n": 1} | 105/0.753 | 62/0.76 | 0.748 |
| 7 | 1.0 | 2.5 | regime!=BULL | pre3_range_r>=0.534926; pre3_close_pos>=0.554175 | {"min_slot": "11:00", "max_slot": "14:00", "top_n": 1} | 94/0.743 | 51/0.747 | 0.7399 |
| 8 | 1.2 | 2.5 | regime!=BULL | pre3_range_r>=0.165674; pre3_close_pos>=0.554175 | {"min_slot": "11:00", "max_slot": "14:00", "top_n": 1} | 134/0.735 | 86/0.734 | 0.733 |
| 9 | 1.2 | 2.5 | regime!=BULL | pre3_range_r>=0.448233; pre1_adx>=31.180706 | {"top_n": 2} | 135/0.732 | 84/0.737 | 0.7285 |
| 10 | 1.2 | 2.5 | regime!=BULL | pre3_range_r>=0.448233; pre1_adx>=31.180706 | {"top_n": 2} | 135/0.732 | 84/0.737 | 0.7285 |
| 11 | 1.0 | 2.5 | regime!=BULL | pre3_range_r>=0.534926; sig5_rsi_dir>=66.40603 | {"min_slot": "11:00", "max_slot": "14:00", "top_n": 1} | 93/0.76 | 52/0.802 | 0.7259 |
| 12 | 1.0 | 2.5 | regime!=BULL | pre5_mom_r>=0.546221; pre3_close_pos>=0.554175 | {"min_slot": "09:45", "max_slot": "13:00", "top_n": 3} | 100/0.809 | 59/0.914 | 0.7249 |
| 13 | 1.0 | 2.5 | regime!=BULL | pre5_mom_r>=0.684274; pre3_close_pos>=0.554175 | {"min_slot": "09:45", "max_slot": "14:30", "top_n": 1} | 86/0.76 | 54/0.808 | 0.722 |
| 14 | 1.0 | 2.5 | regime==NEUTRAL | pre5_mom_r>=0.546221; pre3_close_pos>=0.554175 | {"min_slot": "09:45", "max_slot": "14:30", "top_n": 1} | 75/0.76 | 66/0.807 | 0.7217 |
| 15 | 1.1 | 2.5 | regime!=BULL | pre5_mom_r>=0.546221; pre3_close_pos>=0.656306 | {"min_slot": "12:00", "max_slot": "14:30", "top_n": 1} | 63/0.845 | 44/1.005 | 0.7172 |
| 16 | 1.2 | 2.5 | regime!=BULL | pre3_range_r>=0.448233; pre_entry_momentum_score>=58.335135 | {"min_slot": "11:00", "max_slot": "14:00", "top_n": 1} | 97/0.728 | 49/0.715 | 0.705 |
| 17 | 1.2 | 2.5 | regime!=BULL | pre3_range_r>=0.448233; pre_entry_momentum_score>=50.998995 | {"min_slot": "09:30", "max_slot": "14:00", "top_n": 2} | 128/0.713 | 69/0.727 | 0.7013 |
| 18 | 1.2 | 2.5 | regime!=BULL | pre5_mom_r>=0.84693; pre3_close_pos>=0.554175 | {"min_slot": "12:00", "max_slot": "14:00", "top_n": 1} | 20/0.949 | 19/1.259 | 0.7008 |
| 19 | 1.0 | 2.5 | regime!=BULL | pre5_mom_r>=0.546221; sig5_rsi_dir>=66.40603 | {"min_slot": "09:45", "max_slot": "14:30", "top_n": 1} | 114/0.758 | 65/0.83 | 0.6995 |
| 20 | 1.0 | 2.5 | regime!=BULL | pre5_mom_r>=0.546221; pre3_close_pos>=0.163408 | {"min_slot": "09:45", "max_slot": "14:30", "top_n": 1} | 125/0.721 | 76/0.755 | 0.694 |
| 21 | 1.0 | 2.5 | regime!=BULL | pre5_mom_r>=0.546221; pre3_close_pos>=0.554175 | {"min_slot": "09:45", "max_slot": "14:30"} | 143/0.755 | 81/0.72 | 0.6923 |
| 22 | 1.0 | 2.5 | regime!=BULL | pre5_mom_r>=0.546221; pre3_close_pos>=0.554175 | {"min_slot": "10:00", "max_slot": "14:30"} | 143/0.755 | 81/0.72 | 0.6923 |
| 23 | 1.2 | 2.5 | regime!=BULL | pre3_range_r>=0.448233; pre_entry_momentum_score>=58.335135 | {"min_slot": "09:30", "max_slot": "14:00", "top_n": 2} | 126/0.733 | 68/0.709 | 0.6904 |
| 24 | 1.0 | 2.5 | regime!=BULL | pre5_mom_r>=0.684274; pre3_close_pos>=0.554175 | {"min_slot": "09:30", "max_slot": "14:30", "top_n": 3} | 114/0.714 | 69/0.744 | 0.6904 |
| 25 | 1.0 | 2.5 | regime!=BULL | pre5_mom_r>=0.546221; pre3_close_pos>=0.554175 | {"min_slot": "09:45", "max_slot": "14:30", "top_n": 3} | 140/0.76 | 81/0.72 | 0.6887 |

## Best round-2 single-knob improvements

- **indicator/price-action / +mask vol_z20>=** -> 3.075749 (q0.8) (FIT 96/0.66, VAL 71/0.693, score 0.6336)
- **indicator/price-action / +mask rechigh_dist_atr<=** -> -0.589474 (q0.2) (FIT 96/0.596, VAL 45/0.624, score 0.5736)
- **indicator/price-action / +mask pressure5>=** -> 50.0 (q0.8) (FIT 120/0.567, VAL 94/0.581, score 0.5558)
- **indicator/price-action / +mask atr_pct>=** -> 0.003142 (q0.5) (FIT 243/0.565, VAL 162/0.578, score 0.5546)
- **indicator/price-action / +mask ema_stack_atr<=** -> -0.827178 (q0.2) (FIT 86/0.553, VAL 72/0.556, score 0.5506)
- **indicator/price-action / +mask pressure5>=** -> 12.367113 (q0.5) (FIT 227/0.606, VAL 161/0.574, score 0.5484)
- **indicator/price-action / +mask ema20_dist_atr>=** -> 3.325169 (q0.5) (FIT 237/0.558, VAL 170/0.549, score 0.5418)
- **indicator/price-action / +mask ema20_slope3_atr>=** -> 0.619123 (q0.5) (FIT 235/0.56, VAL 160/0.586, score 0.5392)
- **indicator/price-action / +mask bb_width_pct>=** -> 1.027563 (q0.2) (FIT 357/0.54, VAL 161/0.542, score 0.5384)
- **pre-momentum / +premom pre3_range_r>=** -> 0.448233 (q0.5) (FIT 223/0.547, VAL 139/0.542, score 0.538)
- **pre-momentum / +premom sig5_vol_ratio20>=** -> 3.496134 (q0.5) (FIT 225/0.556, VAL 190/0.542, score 0.5308)
- **indicator/price-action / +mask macd_sig_atr<=** -> 0.53066 (q0.8) (FIT 324/0.547, VAL 144/0.569, score 0.5294)
- **indicator/price-action / +mask macd_hist_atr<=** -> 0.53066 (q0.8) (FIT 324/0.547, VAL 144/0.569, score 0.5294)
- **pre-momentum / +premom pre1_adx>=** -> 31.180706 (q0.2) (FIT 350/0.528, VAL 277/0.527, score 0.5262)
- **indicator/price-action / +mask obv_slope10_norm>=** -> 1.062147 (q0.8) (FIT 90/0.761, VAL 47/0.629, score 0.5234)
- **indicator/price-action / +mask cci>=** -> 191.973284 (q0.2) (FIT 332/0.524, VAL 171/0.526, score 0.5224)
- **indicator/price-action / +mask wick_skew_pct>=** -> 0.022096 (q0.5) (FIT 214/0.525, VAL 182/0.53, score 0.521)
- **indicator/price-action / +mask gap_pct>=** -> 0.260242 (q0.5) (FIT 197/0.593, VAL 68/0.686, score 0.5186)
- **indicator/price-action / +mask sma20_dist_atr>=** -> 2.553816 (q0.2) (FIT 345/0.549, VAL 168/0.532, score 0.5184)
- **indicator/price-action / +mask roc5_pct>=** -> 0.698779 (q0.2) (FIT 365/0.548, VAL 264/0.531, score 0.5174)

## Live-parity caveat for enriched features

- Enriched mask features are computed from the SAME live 5-min indicator feed, but the current conf gate only reads scanner-emitted candidate fields. Promoting an enriched-mask candidate requires a small gate extension (look up the indicator columns at apply time). Flag this at approval.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**