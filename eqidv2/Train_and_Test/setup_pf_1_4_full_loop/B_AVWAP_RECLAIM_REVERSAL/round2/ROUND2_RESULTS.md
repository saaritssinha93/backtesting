# B_AVWAP_RECLAIM_REVERSAL (LONG) — ROUND2_RESULTS (enriched feature space)

_Generated 2026-07-03. ROUND 2 (enriched indicator/price-action feature space). Research-only; NO live trades; NO final_setup_conf.py edits._

- Optimizer: Optuna TPE | trials 800 (631 unique) | sweeps 389 | TEST evals used 0
- Windows: TRAIN 2026-03-04..2026-05-29 (52 sess) | TEST 2026-06-01..2026-07-01 (22 sess)
- Search space: 3 mask terms over base + enriched features (RSI/ADX/MACD/EMA/BB/Stoch/MFI/CCI/OBV/pressure/vol-z/ROC/W%R + day/gap/OR/prev-day geometry + prev-candle structure), 2 pre-momentum terms, regime, slots, top_n, max_positions, daily_loss, exit grid.
- **Passing candidates: 0**

## Baseline (round-1 conf/default config on this pool)

- FIT: n=1008 PF=0.399 net=Rs-362,847 win%=26.3 avgW=Rs908 avgL=Rs-812 maxDD=Rs-361,915 SL/TGT/EOD=588/157/263 tpd=32.52 tradeDom=0.005 dayDom=9.99 symDom=9.99 dbp=1.0
- VAL: n=763 PF=0.297 net=Rs-331,541 win%=21.8 avgW=Rs844 avgL=Rs-790 maxDD=Rs-330,610 SL/TGT/EOD=447/88/228 tpd=36.33 tradeDom=0.009 dayDom=9.99 symDom=9.99 dbp=1.0
- TRAIN: n=1771 PF=0.354 net=Rs-694,388 win%=24.3 avgW=Rs883 avgL=Rs-802 maxDD=Rs-693,456 SL/TGT/EOD=1035/245/491 tpd=34.06 tradeDom=0.003 dayDom=9.99 symDom=9.99 dbp=1.0
- TEST: n=730 PF=0.334 net=Rs-298,849 win%=24.1 avgW=Rs850 avgL=Rs-809 maxDD=Rs-304,605 SL/TGT/EOD=431/92/207 tpd=33.18 tradeDom=0.008 dayDom=9.99 symDom=9.99 dbp=1.0

## Finalists / rescue results

### finalist #1 — reject

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 1.2,
    "tgt_pct": 2.5
  },
  "mask_terms": [
    [
      "macd_atr",
      ">=",
      0.314566
    ],
    [
      "regime",
      "!=",
      "BULL"
    ],
    [
      "signal_range_pct",
      ">=",
      1.149468
    ]
  ],
  "pre_momentum_terms": [],
  "entry_guards": {
    "top_n": 2
  },
  "max_positions": 20,
  "daily_loss_rs": 4000.0
}
```
- TRAIN: n=46 PF=1.101 net=Rs3,278 win%=43.5 avgW=Rs1,790 avgL=Rs-1,251 maxDD=Rs-6,446 SL/TGT/EOD=22/14/10 tpd=2.0 tradeDom=0.063 dayDom=1.369 symDom=0.691 dbp=0.3773
- reasons: TRAIN not in band or too thin (PF 1.101, n 46)

### finalist #2 — reject

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 1.2,
    "tgt_pct": 2.5
  },
  "mask_terms": [
    [
      "day_ret_pct",
      ">=",
      -0.120017
    ],
    [
      "regime",
      "!=",
      "BULL"
    ],
    [
      "signal_range_pct",
      ">=",
      1.149468
    ]
  ],
  "pre_momentum_terms": [],
  "entry_guards": {
    "top_n": 2
  },
  "max_positions": 20,
  "daily_loss_rs": 4000.0
}
```
- TRAIN: n=124 PF=0.667 net=Rs-34,275 win%=33.1 avgW=Rs1,678 avgL=Rs-1,242 maxDD=Rs-35,548 SL/TGT/EOD=66/26/32 tpd=3.1 tradeDom=0.033 dayDom=9.99 symDom=9.99 dbp=0.9822
- reasons: TRAIN not in band or too thin (PF 0.667, n 124)

### finalist #3 — reject

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 1.2,
    "tgt_pct": 2.5
  },
  "mask_terms": [
    [
      "macd_sig_atr",
      ">=",
      -0.252813
    ],
    [
      "regime",
      "!=",
      "BULL"
    ],
    [
      "signal_range_pct",
      ">=",
      1.149468
    ]
  ],
  "pre_momentum_terms": [],
  "entry_guards": {
    "top_n": 2
  },
  "max_positions": 20,
  "daily_loss_rs": 4000.0
}
```
- TRAIN: n=107 PF=0.665 net=Rs-29,272 win%=33.6 avgW=Rs1,616 avgL=Rs-1,232 maxDD=Rs-33,321 SL/TGT/EOD=55/21/31 tpd=3.24 tradeDom=0.039 dayDom=9.99 symDom=9.99 dbp=0.9641
- reasons: TRAIN not in band or too thin (PF 0.665, n 107)

### finalist #4 — reject

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 1.2,
    "tgt_pct": 2.5
  },
  "mask_terms": [
    [
      "gap_pct",
      ">=",
      0.680882
    ],
    [
      "lower_wick_pct",
      ">=",
      0.066654
    ],
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
      27.066949
    ]
  ],
  "entry_guards": {
    "top_n": 2
  },
  "max_positions": 20,
  "daily_loss_rs": 4000.0
}
```
- TRAIN: n=46 PF=0.634 net=Rs-10,894 win%=32.6 avgW=Rs1,261 avgL=Rs-961 maxDD=Rs-13,142 SL/TGT/EOD=15/6/25 tpd=2.42 tradeDom=0.12 dayDom=9.99 symDom=9.99 dbp=0.9394
- reasons: TRAIN not in band or too thin (PF 0.634, n 46)

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
      "mfi",
      ">=",
      36.519892
    ],
    [
      "signal_range_pct",
      ">=",
      1.149468
    ]
  ],
  "pre_momentum_terms": [],
  "entry_guards": {
    "top_n": 2
  },
  "max_positions": 20,
  "daily_loss_rs": 4000.0
}
```
- TRAIN: n=163 PF=0.623 net=Rs-50,915 win%=33.7 avgW=Rs1,530 avgL=Rs-1,250 maxDD=Rs-55,783 SL/TGT/EOD=86/29/48 tpd=3.98 tradeDom=0.027 dayDom=9.99 symDom=9.99 dbp=0.9938
- reasons: TRAIN not in band or too thin (PF 0.623, n 163)

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
    ],
    [
      "signal_range_pct",
      ">=",
      1.149468
    ],
    [
      "upper_wick_pct",
      ">=",
      0.0
    ]
  ],
  "pre_momentum_terms": [],
  "entry_guards": {
    "top_n": 2
  },
  "max_positions": 20,
  "daily_loss_rs": 4000.0
}
```
- TRAIN: n=138 PF=0.629 net=Rs-42,656 win%=32.6 avgW=Rs1,610 avgL=Rs-1,238 maxDD=Rs-43,022 SL/TGT/EOD=73/26/39 tpd=3.37 tradeDom=0.031 dayDom=9.99 symDom=9.99 dbp=0.9918
- reasons: TRAIN not in band or too thin (PF 0.629, n 138)

## Top 25 FIT/VAL trials

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|----|------|--------|-------|----------|----------|-------|
| 1 | 1.2 | 2.5 | signal_range_pct>=1.149468; macd_atr>=0.314566; regime!=BULL | (none) | {"top_n": 2} | 32/1.126 | 14/1.048 | 0.9864 |
| 2 | 1.2 | 2.5 | signal_range_pct>=1.149468; day_ret_pct>=-0.120017; regime!=BULL | (none) | {"top_n": 2} | 59/0.677 | 65/0.659 | 0.6445 |
| 3 | 1.2 | 2.5 | signal_range_pct>=1.149468; macd_sig_atr>=-0.252813; regime!=BULL | (none) | {"top_n": 2} | 69/0.673 | 38/0.652 | 0.6345 |
| 4 | 1.2 | 2.5 | gap_pct>=0.680882; lower_wick_pct>=0.066654; regime!=BULL | pre1_adx>=27.066949 | {"top_n": 2} | 31/0.63 | 15/0.643 | 0.6188 |
| 5 | 1.2 | 2.5 | signal_range_pct>=1.149468; mfi>=36.519892 | (none) | {"top_n": 2} | 112/0.627 | 51/0.615 | 0.6049 |
| 6 | 1.2 | 2.5 | signal_range_pct>=1.149468; upper_wick_pct>=0.0; regime!=BULL | (none) | {"top_n": 2} | 71/0.64 | 67/0.618 | 0.6005 |
| 7 | 1.2 | 2.5 | signal_range_pct>=1.149468; ema50_dist_atr>=1.570781; regime!=BULL | (none) | {"top_n": 2} | 49/0.633 | 47/0.697 | 0.582 |
| 8 | 1.2 | 2.0 | signal_range_pct>=1.149468; macd_atr>=-0.40078 | (none) | {"top_n": 2} | 116/0.587 | 50/0.582 | 0.5775 |
| 9 | 1.2 | 2.5 | signal_range_pct>=1.149468; atr_pct>=0.003483; regime!=BULL | (none) | {"top_n": 2} | 69/0.673 | 67/0.618 | 0.5742 |
| 10 | 1.2 | 2.5 | gap_pct>=0.680882; lower_wick_pct>=0.066654; regime!=BULL | (none) | {"top_n": 2} | 68/0.605 | 22/0.585 | 0.5686 |
| 11 | 1.2 | 2.5 | signal_range_pct>=1.149468; body_pct>=0.567242 | (none) | {"top_n": 2} | 118/0.572 | 102/0.57 | 0.568 |
| 12 | 1.2 | 2.0 | signal_range_pct>=1.149468; regime==NEUTRAL | (none) | - | 73/0.64 | 95/0.598 | 0.5647 |
| 13 | 1.2 | 2.5 | signal_range_pct>=1.149468; macd_atr>=-0.40078 | (none) | {"top_n": 2} | 116/0.608 | 49/0.67 | 0.5594 |
| 14 | 1.5 | 2.5 | gap_pct>=0.680882; lower_wick_pct>=0.066654; regime!=BULL | (none) | {"top_n": 2} | 68/0.705 | 22/0.623 | 0.5562 |
| 15 | 1.2 | 2.5 | signal_range_pct>=1.149468; obv_slope10_norm>=-0.21586 | (none) | {"top_n": 2} | 108/0.617 | 46/0.695 | 0.5556 |
| 16 | 1.0 | 1.5 | macd_sig_atr>=-0.0668; rsi>=50.166668; atr_pct>=0.004958; regime==NEUTRAL | (none) | {"min_slot": "10:00"} | 41/0.572 | 42/0.599 | 0.5503 |
| 17 | 1.2 | 2.5 | signal_range_pct>=1.149468; adx5_slope3>=-3.77233; regime!=BULL | (none) | {"top_n": 3} | 76/0.618 | 72/0.579 | 0.5471 |
| 18 | 1.2 | 2.5 | signal_range_pct>=1.149468; sma20_dist_atr>=-0.112434; regime!=BULL | (none) | {"top_n": 1} | 46/0.592 | 25/0.649 | 0.5463 |
| 19 | 1.2 | 2.5 | gap_pct>=1.649795; or15_lose_atr>=6.7433; regime!=BULL | (none) | {"top_n": 2} | 23/0.556 | 13/0.549 | 0.5436 |
| 20 | 1.2 | 2.5 | signal_range_pct>=1.149468; prev3_up>=0.0 | (none) | {"top_n": 2} | 120/0.587 | 104/0.56 | 0.5391 |
| 21 | 1.2 | 2.5 | signal_range_pct>=1.149468; atr_pct>=0.00176; regime!=BULL | (none) | {"top_n": 1} | 46/0.592 | 43/0.561 | 0.5367 |
| 22 | 1.2 | 2.5 | signal_range_pct>=1.149468; atr_pct>=0.002534; regime!=BULL | (none) | {"top_n": 1} | 46/0.592 | 43/0.561 | 0.5367 |
| 23 | 1.2 | 2.0 | signal_range_pct>=1.149468 | (none) | - | 186/0.539 | 139/0.542 | 0.5365 |
| 24 | 1.2 | 2.0 | signal_range_pct>=1.149468; vwap_dist_atr>=0.17051 | (none) | {"top_n": 2} | 119/0.575 | 105/0.551 | 0.532 |
| 25 | 1.2 | 2.0 | signal_range_pct>=1.149468; rechigh_dist_atr>=-0.931484 | (none) | {"top_n": 2} | 113/0.61 | 50/0.563 | 0.5259 |

## Best round-2 single-knob improvements

- **indicator/price-action / +mask gap_pct>=** -> 1.018121 (q0.8) (FIT 316/0.434, VAL 74/0.455, score 0.4172)
- **indicator/price-action / +mask ema20_slope3_atr>=** -> 0.291984 (q0.8) (FIT 235/0.421, VAL 158/0.444, score 0.4026)
- **pre-momentum / +premom pre5_mom_r>=** -> 0.342895 (q0.5) (FIT 804/0.387, VAL 447/0.383, score 0.3798)
- **indicator/price-action / +mask roc5_pct>=** -> 0.404725 (q0.5) (FIT 600/0.381, VAL 415/0.385, score 0.3778)
- **indicator/price-action / +mask rsi>=** -> 57.930462 (q0.5) (FIT 558/0.376, VAL 348/0.378, score 0.3744)
- **indicator/price-action / +mask rsi>=** -> 63.000595 (q0.8) (FIT 168/0.394, VAL 106/0.383, score 0.3742)
- **pre-momentum / +premom pre_entry_momentum_score>=** -> 66.395361 (q0.5) (FIT 774/0.373, VAL 442/0.374, score 0.3722)
- **indicator/price-action / +mask wick_skew_pct>=** -> 0.101075 (q0.8) (FIT 275/0.437, VAL 209/0.397, score 0.365)
- **pre-momentum / +premom pre5_mom_r>=** -> 0.691398 (q0.8) (FIT 395/0.413, VAL 161/0.473, score 0.365)
- **pre-momentum / +premom sig5_rsi_dir>=** -> 57.986759 (q0.5) (FIT 560/0.377, VAL 355/0.37, score 0.3644)
- **pre-momentum / +premom pre3_range_r>=** -> 0.626941 (q0.8) (FIT 417/0.414, VAL 220/0.478, score 0.3628)
- **indicator/price-action / +mask gap_pct<=** -> -0.584373 (q0.2) (FIT 280/0.403, VAL 88/0.377, score 0.3562)
- **indicator/price-action / +mask ema20_slope3_atr>=** -> 0.095516 (q0.5) (FIT 607/0.354, VAL 390/0.352, score 0.3504)
- **pre-momentum / +premom sig5_vol_ratio20>=** -> 2.495784 (q0.5) (FIT 675/0.368, VAL 503/0.355, score 0.3446)
- **indicator/price-action / +mask reclow_dist_atr>=** -> 3.272706 (q0.8) (FIT 175/0.4, VAL 91/0.369, score 0.3442)
- **pre-momentum / +premom pre3_range_r>=** -> 0.328206 (q0.5) (FIT 824/0.384, VAL 546/0.361, score 0.3426)
- **indicator/price-action / +mask ema50_dist_atr>=** -> 1.570781 (q0.5) (FIT 560/0.365, VAL 351/0.394, score 0.3418)
- **indicator/price-action / +mask lower_wick_pct>=** -> 0.094123 (q0.8) (FIT 302/0.347, VAL 217/0.357, score 0.339)
- **indicator/price-action / +mask sma20_dist_atr>=** -> 1.311757 (q0.5) (FIT 527/0.385, VAL 230/0.357, score 0.3346)
- **indicator/price-action / +mask ema20_dist_atr>=** -> 1.232183 (q0.5) (FIT 567/0.355, VAL 353/0.382, score 0.3334)

## Live-parity caveat for enriched features

- Enriched mask features are computed from the SAME live 5-min indicator feed, but the current conf gate only reads scanner-emitted candidate fields. Promoting an enriched-mask candidate requires a small gate extension (look up the indicator columns at apply time). Flag this at approval.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**