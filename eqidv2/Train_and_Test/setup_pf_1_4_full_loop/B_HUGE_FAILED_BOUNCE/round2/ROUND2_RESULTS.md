# B_HUGE_FAILED_BOUNCE (SHORT) — ROUND2_RESULTS (enriched feature space)

_Generated 2026-07-03. ROUND 2 (enriched indicator/price-action feature space). Research-only; NO live trades; NO final_setup_conf.py edits._

- Optimizer: Optuna TPE | trials 800 (492 unique) | sweeps 377 | TEST evals used 0
- Windows: TRAIN 2026-03-02..2026-05-29 (58 sess) | TEST 2026-06-01..2026-07-01 (22 sess)
- Search space: 3 mask terms over base + enriched features (RSI/ADX/MACD/EMA/BB/Stoch/MFI/CCI/OBV/pressure/vol-z/ROC/W%R + day/gap/OR/prev-day geometry + prev-candle structure), 2 pre-momentum terms, regime, slots, top_n, max_positions, daily_loss, exit grid.
- **Passing candidates: 0**

## Baseline (round-1 conf/default config on this pool)

- FIT: n=972 PF=0.344 net=Rs-379,145 win%=27.3 avgW=Rs749 avgL=Rs-817 maxDD=Rs-380,419 SL/TGT/EOD=570/159/243 tpd=27.77 tradeDom=0.005 dayDom=9.99 symDom=9.99 dbp=1.0
- VAL: n=656 PF=0.422 net=Rs-203,928 win%=31.1 avgW=Rs729 avgL=Rs-780 maxDD=Rs-203,777 SL/TGT/EOD=335/114/207 tpd=28.52 tradeDom=0.007 dayDom=9.99 symDom=9.99 dbp=1.0
- TRAIN: n=1628 PF=0.373 net=Rs-583,073 win%=28.8 avgW=Rs740 avgL=Rs-803 maxDD=Rs-584,347 SL/TGT/EOD=905/273/450 tpd=28.07 tradeDom=0.003 dayDom=9.99 symDom=9.99 dbp=1.0
- TEST: n=673 PF=0.503 net=Rs-174,139 win%=34.5 avgW=Rs760 avgL=Rs-795 maxDD=Rs-179,538 SL/TGT/EOD=344/146/183 tpd=30.59 tradeDom=0.006 dayDom=9.99 symDom=9.99 dbp=1.0

## Finalists / rescue results

### finalist #1 — reject

```json
{
  "side": "SHORT",
  "exit": {
    "sl_pct": 1.2,
    "tgt_pct": 1.5
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
      "<=",
      0.564802
    ],
    [
      "pre3_range_r",
      ">=",
      0.279969
    ]
  ],
  "entry_guards": {
    "min_slot": "12:00",
    "max_slot": "14:00",
    "top_n": 1
  },
  "max_positions": 10,
  "daily_loss_rs": 0.0
}
```
- TRAIN: n=43 PF=1.263 net=Rs5,616 win%=55.8 avgW=Rs1,125 avgL=Rs-1,125 maxDD=Rs-3,827 SL/TGT/EOD=14/21/8 tpd=1.79 tradeDom=0.047 dayDom=0.467 symDom=0.226 dbp=0.194
- reasons: TRAIN not in band or too thin (PF 1.263, n 43)

### finalist #2 — reject

```json
{
  "side": "SHORT",
  "exit": {
    "sl_pct": 1.2,
    "tgt_pct": 1.5
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
      "pre3_range_r",
      ">=",
      0.279969
    ]
  ],
  "entry_guards": {
    "min_slot": "12:00",
    "max_slot": "14:00",
    "top_n": 1
  },
  "max_positions": 10,
  "daily_loss_rs": 0.0
}
```
- TRAIN: n=202 PF=0.666 net=Rs-43,738 win%=45.0 avgW=Rs958 avgL=Rs-1,179 maxDD=Rs-51,883 SL/TGT/EOD=80/60/62 tpd=4.7 tradeDom=0.015 dayDom=9.99 symDom=9.99 dbp=0.9795
- reasons: TRAIN not in band or too thin (PF 0.666, n 202)

### finalist #3 — reject

```json
{
  "side": "SHORT",
  "exit": {
    "sl_pct": 1.2,
    "tgt_pct": 1.5
  },
  "mask_terms": [
    [
      "regime",
      "==",
      "BEAR"
    ]
  ],
  "pre_momentum_terms": [
    [
      "pre3_range_r",
      ">=",
      0.279969
    ]
  ],
  "entry_guards": {
    "min_slot": "12:00",
    "max_slot": "14:00",
    "top_n": 1
  },
  "max_positions": 10,
  "daily_loss_rs": 4000.0
}
```
- TRAIN: n=115 PF=0.709 net=Rs-21,574 win%=47.0 avgW=Rs972 avgL=Rs-1,214 maxDD=Rs-28,523 SL/TGT/EOD=47/35/33 tpd=5.0 tradeDom=0.024 dayDom=9.99 symDom=9.99 dbp=0.9113
- reasons: TRAIN not in band or too thin (PF 0.709, n 115)

### finalist #4 — reject

```json
{
  "side": "SHORT",
  "exit": {
    "sl_pct": 1.2,
    "tgt_pct": 1.5
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
      "<=",
      58.927886
    ],
    [
      "pre3_range_r",
      ">=",
      0.279969
    ]
  ],
  "entry_guards": {
    "min_slot": "12:00",
    "max_slot": "14:00",
    "top_n": 1
  },
  "max_positions": 10,
  "daily_loss_rs": 0.0
}
```
- TRAIN: n=183 PF=0.68 net=Rs-37,495 win%=46.4 avgW=Rs936 avgL=Rs-1,195 maxDD=Rs-47,596 SL/TGT/EOD=71/54/58 tpd=4.26 tradeDom=0.016 dayDom=9.99 symDom=9.99 dbp=0.9689
- reasons: TRAIN not in band or too thin (PF 0.68, n 183)

### finalist #5 — reject

```json
{
  "side": "SHORT",
  "exit": {
    "sl_pct": 1.2,
    "tgt_pct": 1.0
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
      "pre3_range_r",
      ">=",
      0.279969
    ],
    [
      "pre_entry_momentum_score",
      ">=",
      70.809776
    ]
  ],
  "entry_guards": {
    "min_slot": "12:00",
    "max_slot": "14:00",
    "top_n": 1
  },
  "max_positions": 10,
  "daily_loss_rs": 0.0
}
```
- TRAIN: n=92 PF=0.627 net=Rs-20,505 win%=51.1 avgW=Rs735 avgL=Rs-1,223 maxDD=Rs-27,663 SL/TGT/EOD=35/44/13 tpd=2.36 tradeDom=0.022 dayDom=9.99 symDom=9.99 dbp=0.9869
- reasons: TRAIN not in band or too thin (PF 0.627, n 92)

### finalist #6 — reject

```json
{
  "side": "SHORT",
  "exit": {
    "sl_pct": 1.2,
    "tgt_pct": 1.5
  },
  "mask_terms": [],
  "pre_momentum_terms": [
    [
      "pre3_range_r",
      ">=",
      0.279969
    ]
  ],
  "entry_guards": {
    "min_slot": "12:00",
    "max_slot": "14:00",
    "top_n": 1
  },
  "max_positions": 10,
  "daily_loss_rs": 4000.0
}
```
- TRAIN: n=297 PF=0.638 net=Rs-68,356 win%=43.8 avgW=Rs927 avgL=Rs-1,131 maxDD=Rs-70,767 SL/TGT/EOD=111/79/107 tpd=5.3 tradeDom=0.011 dayDom=9.99 symDom=9.99 dbp=0.9983
- reasons: TRAIN not in band or too thin (PF 0.638, n 297)

## Top 25 FIT/VAL trials

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|----|------|--------|-------|----------|----------|-------|
| 1 | 1.2 | 1.5 | regime!=BULL | pre3_range_r>=0.279969; pre3_close_pos<=0.564802 | {"min_slot": "12:00", "max_slot": "14:00", "top_n": 1} | 22/1.343 | 21/1.198 | 1.0819 |
| 2 | 1.2 | 1.5 | regime!=BULL | pre3_range_r>=0.279969 | {"min_slot": "12:00", "max_slot": "14:00", "top_n": 1} | 115/0.668 | 87/0.663 | 0.6596 |
| 3 | 1.2 | 1.5 | regime!=BULL | pre3_range_r>=0.279969 | {"min_slot": "12:00", "max_slot": "14:00", "top_n": 1} | 115/0.668 | 86/0.68 | 0.6585 |
| 4 | 1.2 | 1.5 | regime==BEAR | pre3_range_r>=0.279969 | {"min_slot": "12:00", "max_slot": "14:00", "top_n": 1} | 76/0.693 | 39/0.739 | 0.6566 |
| 5 | 1.2 | 1.5 | regime!=BULL | pre3_range_r>=0.279969; pre1_adx<=58.927886 | {"min_slot": "12:00", "max_slot": "14:00", "top_n": 1} | 104/0.658 | 79/0.707 | 0.6197 |
| 6 | 1.2 | 1.0 | regime!=BULL | pre3_range_r>=0.279969; pre_entry_momentum_score>=70.809776 | {"min_slot": "12:00", "max_slot": "14:00", "top_n": 1} | 56/0.635 | 36/0.616 | 0.6011 |
| 7 | 1.2 | 1.5 | (none) | pre3_range_r>=0.279969 | {"min_slot": "12:00", "max_slot": "14:00", "top_n": 1} | 177/0.625 | 120/0.657 | 0.5993 |
| 8 | 1.2 | 1.5 | regime==NEUTRAL | pre3_range_r>=0.279969 | {"min_slot": "12:00", "max_slot": "14:00", "top_n": 1} | 27/0.597 | 48/0.607 | 0.5886 |
| 9 | 1.2 | 1.25 | regime!=BULL | pre3_range_r>=0.279969 | {"min_slot": "12:00", "max_slot": "14:00", "top_n": 1} | 115/0.631 | 86/0.726 | 0.5541 |
| 10 | 1.2 | 1.5 | regime==BEAR | sig5_adx_calc>=25.634333 | {"min_slot": "12:00", "max_slot": "14:00", "top_n": 1} | 57/0.571 | 30/0.6 | 0.5482 |
| 11 | 1.2 | 1.5 | regime==BEAR | sig5_adx_calc>=25.634333 | {"min_slot": "12:00", "max_slot": "14:00", "top_n": 1} | 57/0.571 | 30/0.6 | 0.5482 |
| 12 | 1.5 | 1.5 | regime!=BULL | pre3_range_r>=0.279969; sig5_vol_ratio20>=2.210555 | {"min_slot": "12:00", "max_slot": "14:00", "top_n": 1} | 71/0.561 | 45/0.553 | 0.5474 |
| 13 | 1.2 | 1.5 | regime==BEAR | sig5_adx_calc>=19.620646 | {"min_slot": "12:00", "max_slot": "14:00", "top_n": 1} | 64/0.607 | 41/0.704 | 0.53 |
| 14 | 1.2 | 2.0 | regime!=BULL | pre3_range_r>=0.279969; pre3_close_pos<=0.564802 | {"min_slot": "12:00", "max_slot": "14:00", "top_n": 1} | 22/0.723 | 21/0.973 | 0.5228 |
| 15 | 1.0 | 1.5 | regime==BEAR | sig5_adx_calc>=25.634333 | {"min_slot": "12:00", "max_slot": "14:00", "top_n": 1} | 57/0.583 | 30/0.55 | 0.5227 |
| 16 | 1.0 | 1.5 | regime==BEAR | sig5_adx_calc>=25.634333 | {"min_slot": "12:00", "max_slot": "14:00", "top_n": 1} | 57/0.583 | 30/0.55 | 0.5227 |
| 17 | 1.0 | 1.5 | regime==BEAR | sig5_adx_calc>=25.634333 | {"min_slot": "12:00", "max_slot": "14:00", "top_n": 1} | 57/0.583 | 30/0.55 | 0.5227 |
| 18 | 1.0 | 1.5 | regime==BEAR | sig5_adx_calc>=25.634333 | {"min_slot": "12:00", "max_slot": "14:00", "top_n": 1} | 57/0.583 | 30/0.55 | 0.5227 |
| 19 | 1.0 | 1.5 | regime==BEAR | sig5_adx_calc<=25.634333 | {"min_slot": "12:00", "max_slot": "14:00", "top_n": 1} | 91/0.558 | 40/0.605 | 0.5208 |
| 20 | 1.1 | 1.5 | regime==BEAR | pre3_close_pos<=0.833325 | {"min_slot": "12:00", "max_slot": "14:00", "top_n": 1} | 71/0.578 | 43/0.651 | 0.5184 |
| 21 | 1.5 | 1.5 | regime!=BULL | sig5_rsi_dir>=67.705688 | {"min_slot": "12:00", "max_slot": "14:00", "top_n": 1} | 151/0.548 | 109/0.529 | 0.5134 |
| 22 | 1.2 | 1.0 | regime!=TREND | pre3_range_r>=0.279969 | {"min_slot": "12:00", "max_slot": "14:00", "top_n": 1} | 165/0.654 | 120/0.574 | 0.5098 |
| 23 | 1.2 | 0.8 | (none) | pre3_range_r>=0.279969; pre_entry_momentum_score>=67.967082 | {"min_slot": "12:00", "max_slot": "14:00", "top_n": 1} | 111/0.666 | 62/0.57 | 0.4937 |
| 24 | 1.2 | 1.0 | (none) | pre3_range_r>=0.279969 | {"min_slot": "12:00", "max_slot": "14:00", "top_n": 1} | 177/0.676 | 120/0.574 | 0.4921 |
| 25 | 1.2 | 1.5 | regime!=BULL | sig5_rsi_dir>=67.705688 | {"min_slot": "12:00", "max_slot": "14:00", "top_n": 1} | 152/0.521 | 109/0.56 | 0.4904 |

## Best round-2 single-knob improvements

- **pre-momentum / +premom pre5_mom_r<=** -> 0.058804 (q0.2) (FIT 108/0.442, VAL 222/0.434, score 0.4276)
- **pre-momentum / +premom pre3_close_pos<=** -> 0.439027 (q0.2) (FIT 162/0.454, VAL 200/0.506, score 0.4124)
- **indicator/price-action / +mask candle_range_atr>=** -> 2.545456 (q0.8) (FIT 209/0.411, VAL 156/0.398, score 0.3876)
- **indicator/price-action / +mask lower_wick_pct>=** -> 0.156235 (q0.8) (FIT 210/0.39, VAL 153/0.404, score 0.3788)
- **indicator/price-action / +mask reclow_dist_atr>=** -> 0.518948 (q0.8) (FIT 215/0.398, VAL 95/0.386, score 0.3764)
- **indicator/price-action / +mask ema20_slope3_atr>=** -> -0.600485 (q0.5) (FIT 570/0.389, VAL 358/0.406, score 0.3754)
- **pre-momentum / +premom pre_entry_momentum_score>=** -> 74.553242 (q0.8) (FIT 342/0.393, VAL 188/0.424, score 0.3682)
- **indicator/price-action / +mask vol_z20>=** -> 2.780298 (q0.8) (FIT 213/0.387, VAL 169/0.42, score 0.3606)
- **exit / sl_pct** -> 1.0 (FIT 886/0.414, VAL 612/0.487, score 0.3556)
- **indicator/price-action / +mask upper_wick_pct<=** -> 0.0 (q0.2) (FIT 362/0.37, VAL 225/0.389, score 0.3548)
- **guard / max_positions** -> 5 (FIT 413/0.365, VAL 280/0.378, score 0.3546)
- **indicator/price-action / +mask rsi_slope3<=** -> -15.08852 (q0.5) (FIT 543/0.359, VAL 344/0.366, score 0.3534)
- **indicator/price-action / +mask ema200_dist_atr>=** -> -2.996664 (q0.5) (FIT 551/0.369, VAL 314/0.39, score 0.3522)
- **indicator/price-action / +mask upper_wick_pct<=** -> 0.033413 (q0.5) (FIT 536/0.368, VAL 342/0.359, score 0.3518)
- **indicator/price-action / +mask wick_skew_pct<=** -> -0.017923 (q0.5) (FIT 536/0.376, VAL 352/0.41, score 0.3488)
- **indicator/price-action / +mask bb_pos<=** -> -0.174363 (q0.5) (FIT 521/0.349, VAL 220/0.35, score 0.3482)
- **indicator/price-action / +mask roc5_pct<=** -> -1.524506 (q0.2) (FIT 224/0.382, VAL 136/0.426, score 0.3468)
- **exit / sl_pct** -> 0.85 (FIT 917/0.389, VAL 626/0.446, score 0.3434)
- **pre-momentum / +premom pre1_adx<=** -> 52.95204 (q0.8) (FIT 843/0.383, VAL 560/0.433, score 0.343)
- **indicator/price-action / +mask macd_hist_atr>=** -> -0.120501 (q0.8) (FIT 213/0.439, VAL 113/0.559, score 0.343)

## Live-parity caveat for enriched features

- Enriched mask features are computed from the SAME live 5-min indicator feed, but the current conf gate only reads scanner-emitted candidate fields. Promoting an enriched-mask candidate requires a small gate extension (look up the indicator columns at apply time). Flag this at approval.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**