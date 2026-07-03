# B_HUGE_GREEN_PULLBACK_HOLD_THEN_BREAK (LONG) — ROUND2_RESULTS (enriched feature space)

_Generated 2026-07-03. ROUND 2 (enriched indicator/price-action feature space). Research-only; NO live trades; NO final_setup_conf.py edits._

- Optimizer: Optuna TPE | trials 800 (516 unique) | sweeps 377 | TEST evals used 8
- Windows: TRAIN 2026-03-02..2026-05-29 (38 sess) | TEST 2026-06-01..2026-06-30 (13 sess)
- Search space: 3 mask terms over base + enriched features (RSI/ADX/MACD/EMA/BB/Stoch/MFI/CCI/OBV/pressure/vol-z/ROC/W%R + day/gap/OR/prev-day geometry + prev-candle structure), 2 pre-momentum terms, regime, slots, top_n, max_positions, daily_loss, exit grid.
- **Passing candidates: 0**

## Baseline (round-1 conf/default config on this pool)

- FIT: n=349 PF=0.544 net=Rs-88,219 win%=35.5 avgW=Rs848 avgL=Rs-859 maxDD=Rs-91,654 SL/TGT/EOD=201/91/57 tpd=15.17 tradeDom=0.012 dayDom=9.99 symDom=9.99 dbp=1.0
- VAL: n=201 PF=0.415 net=Rs-72,983 win%=28.9 avgW=Rs891 avgL=Rs-872 maxDD=Rs-72,292 SL/TGT/EOD=130/49/22 tpd=13.4 tradeDom=0.02 dayDom=9.99 symDom=9.99 dbp=0.9998
- TRAIN: n=550 PF=0.493 net=Rs-161,202 win%=33.1 avgW=Rs862 avgL=Rs-864 maxDD=Rs-160,273 SL/TGT/EOD=331/140/79 tpd=14.47 tradeDom=0.008 dayDom=9.99 symDom=9.99 dbp=1.0
- TEST: n=164 PF=0.355 net=Rs-63,249 win%=26.8 avgW=Rs793 avgL=Rs-818 maxDD=Rs-62,317 SL/TGT/EOD=93/31/40 tpd=12.62 tradeDom=0.029 dayDom=9.99 symDom=9.99 dbp=0.9999

## Finalists / rescue results

### finalist #1 — reject

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 1.5,
    "tgt_pct": 2.0
  },
  "mask_terms": [],
  "pre_momentum_terms": [
    [
      "pre3_close_pos",
      ">=",
      0.541682
    ],
    [
      "sig5_vol_ratio20",
      ">=",
      2.575953
    ]
  ],
  "entry_guards": {
    "max_slot": "11:30"
  },
  "max_positions": 10,
  "daily_loss_rs": 4000.0
}
```
- TRAIN: n=39 PF=1.704 net=Rs14,093 win%=61.5 avgW=Rs1,421 avgL=Rs-1,334 maxDD=Rs-8,581 SL/TGT/EOD=10/19/10 tpd=2.17 tradeDom=0.052 dayDom=0.712 symDom=0.125 dbp=0.1425
- TEST:  n=14 PF=1.084 net=Rs758 win%=57.1 avgW=Rs1,224 avgL=Rs-1,506 maxDD=Rs-3,442 SL/TGT/EOD=5/5/4 tpd=1.75 tradeDom=0.18 dayDom=2.327 symDom=2.329 dbp=0.4205
- robustness: neighbor=True dropout=True
- reasons: TRAIN domination (trade>35% gross or day/sym>40% net); TEST PF 1.084 <= 1.4; TEST domination (trade>35% gross or day/sym>40% net); TEST day-block p 0.4205 > 0.1; TEST n 14 < 20 (thin, sample-limited); TRAIN PF in upper band (1.70-1.80) — watch for overfit

### finalist #2 — reject

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 1.5,
    "tgt_pct": 2.0
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
      0.541682
    ],
    [
      "sig5_vol_ratio20",
      ">=",
      2.575953
    ]
  ],
  "entry_guards": {
    "max_slot": "11:30"
  },
  "max_positions": 10,
  "daily_loss_rs": 0.0
}
```
- TRAIN: n=39 PF=1.704 net=Rs14,093 win%=61.5 avgW=Rs1,421 avgL=Rs-1,334 maxDD=Rs-8,581 SL/TGT/EOD=10/19/10 tpd=2.17 tradeDom=0.052 dayDom=0.712 symDom=0.125 dbp=0.1425
- TEST:  n=14 PF=1.084 net=Rs758 win%=57.1 avgW=Rs1,224 avgL=Rs-1,506 maxDD=Rs-3,442 SL/TGT/EOD=5/5/4 tpd=1.75 tradeDom=0.18 dayDom=2.327 symDom=2.329 dbp=0.4205
- robustness: neighbor=True dropout=True
- reasons: TRAIN domination (trade>35% gross or day/sym>40% net); TEST PF 1.084 <= 1.4; TEST domination (trade>35% gross or day/sym>40% net); TEST day-block p 0.4205 > 0.1; TEST n 14 < 20 (thin, sample-limited); TRAIN PF in upper band (1.70-1.80) — watch for overfit

### finalist #3 — reject

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 1.5,
    "tgt_pct": 2.0
  },
  "mask_terms": [],
  "pre_momentum_terms": [
    [
      "pre3_close_pos",
      ">=",
      0.541682
    ],
    [
      "sig5_vol_ratio20",
      ">=",
      2.575953
    ]
  ],
  "entry_guards": {
    "min_slot": "09:30",
    "max_slot": "11:30"
  },
  "max_positions": 10,
  "daily_loss_rs": 0.0
}
```
- TRAIN: n=39 PF=1.704 net=Rs14,093 win%=61.5 avgW=Rs1,421 avgL=Rs-1,334 maxDD=Rs-8,581 SL/TGT/EOD=10/19/10 tpd=2.17 tradeDom=0.052 dayDom=0.712 symDom=0.125 dbp=0.1425
- TEST:  n=14 PF=1.084 net=Rs758 win%=57.1 avgW=Rs1,224 avgL=Rs-1,506 maxDD=Rs-3,442 SL/TGT/EOD=5/5/4 tpd=1.75 tradeDom=0.18 dayDom=2.327 symDom=2.329 dbp=0.4205
- robustness: neighbor=True dropout=True
- reasons: TRAIN domination (trade>35% gross or day/sym>40% net); TEST PF 1.084 <= 1.4; TEST domination (trade>35% gross or day/sym>40% net); TEST day-block p 0.4205 > 0.1; TEST n 14 < 20 (thin, sample-limited); TRAIN PF in upper band (1.70-1.80) — watch for overfit

### finalist #4 — reject

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 1.5,
    "tgt_pct": 2.0
  },
  "mask_terms": [],
  "pre_momentum_terms": [
    [
      "pre3_close_pos",
      ">=",
      0.541682
    ],
    [
      "sig5_vol_ratio20",
      ">=",
      2.575953
    ]
  ],
  "entry_guards": {
    "max_slot": "11:30",
    "top_n": 3
  },
  "max_positions": 10,
  "daily_loss_rs": 0.0
}
```
- TRAIN: n=39 PF=1.704 net=Rs14,093 win%=61.5 avgW=Rs1,421 avgL=Rs-1,334 maxDD=Rs-8,581 SL/TGT/EOD=10/19/10 tpd=2.17 tradeDom=0.052 dayDom=0.712 symDom=0.125 dbp=0.1425
- TEST:  n=14 PF=1.084 net=Rs758 win%=57.1 avgW=Rs1,224 avgL=Rs-1,506 maxDD=Rs-3,442 SL/TGT/EOD=5/5/4 tpd=1.75 tradeDom=0.18 dayDom=2.327 symDom=2.329 dbp=0.4205
- robustness: neighbor=True dropout=True
- reasons: TRAIN domination (trade>35% gross or day/sym>40% net); TEST PF 1.084 <= 1.4; TEST domination (trade>35% gross or day/sym>40% net); TEST day-block p 0.4205 > 0.1; TEST n 14 < 20 (thin, sample-limited); TRAIN PF in upper band (1.70-1.80) — watch for overfit

### finalist #5 — reject

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 1.5,
    "tgt_pct": 2.5
  },
  "mask_terms": [],
  "pre_momentum_terms": [
    [
      "pre3_close_pos",
      ">=",
      0.541682
    ],
    [
      "pre_entry_momentum_score",
      ">=",
      63.289967
    ]
  ],
  "entry_guards": {
    "max_slot": "11:30"
  },
  "max_positions": 10,
  "daily_loss_rs": 0.0
}
```
- TRAIN: n=41 PF=1.801 net=Rs16,616 win%=53.7 avgW=Rs1,699 avgL=Rs-1,092 maxDD=Rs-8,093 SL/TGT/EOD=10/16/15 tpd=2.41 tradeDom=0.061 dayDom=0.638 symDom=0.136 dbp=0.1144
- reasons: TRAIN not in band or too thin (PF 1.801, n 41)

### finalist #6 — reject

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 1.5,
    "tgt_pct": 2.0
  },
  "mask_terms": [],
  "pre_momentum_terms": [
    [
      "pre3_close_pos",
      ">=",
      0.541682
    ],
    [
      "pre3_close_pos",
      ">=",
      0.848306
    ]
  ],
  "entry_guards": {
    "max_slot": "11:30"
  },
  "max_positions": 10,
  "daily_loss_rs": 0.0
}
```
- TRAIN: n=24 PF=1.946 net=Rs9,907 win%=58.3 avgW=Rs1,456 avgL=Rs-1,047 maxDD=Rs-4,100 SL/TGT/EOD=5/11/8 tpd=1.71 tradeDom=0.087 dayDom=0.382 symDom=0.178 dbp=0.1188
- reasons: TRAIN not in band or too thin (PF 1.946, n 24)

### R3-window-{"min_slot": "10:00"} — reject

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 1.5,
    "tgt_pct": 2.0
  },
  "mask_terms": [],
  "pre_momentum_terms": [
    [
      "sig5_vol_ratio20",
      ">=",
      2.575953
    ],
    [
      "pre3_close_pos",
      ">=",
      0.541682
    ]
  ],
  "entry_guards": {
    "max_slot": "11:30",
    "min_slot": "10:00"
  },
  "max_positions": 10,
  "daily_loss_rs": 0.0
}
```
- TRAIN: n=39 PF=1.704 net=Rs14,093 win%=61.5 avgW=Rs1,421 avgL=Rs-1,334 maxDD=Rs-8,581 SL/TGT/EOD=10/19/10 tpd=2.17 tradeDom=0.052 dayDom=0.712 symDom=0.125 dbp=0.1425
- TEST:  n=14 PF=1.084 net=Rs758 win%=57.1 avgW=Rs1,224 avgL=Rs-1,506 maxDD=Rs-3,442 SL/TGT/EOD=5/5/4 tpd=1.75 tradeDom=0.18 dayDom=2.327 symDom=2.329 dbp=0.4205
- robustness: neighbor=True dropout=True
- reasons: TRAIN domination (trade>35% gross or day/sym>40% net); TEST PF 1.084 <= 1.4; TEST domination (trade>35% gross or day/sym>40% net); TEST day-block p 0.4205 > 0.1; TEST n 14 < 20 (thin, sample-limited); TRAIN PF in upper band (1.70-1.80) — watch for overfit

### R2-drop-premom-0 — reject

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 1.5,
    "tgt_pct": 2.0
  },
  "mask_terms": [],
  "pre_momentum_terms": [
    [
      "pre3_close_pos",
      ">=",
      0.541682
    ]
  ],
  "entry_guards": {
    "max_slot": "11:30"
  },
  "max_positions": 10,
  "daily_loss_rs": 0.0
}
```
- TRAIN: n=52 PF=1.545 net=Rs14,768 win%=55.8 avgW=Rs1,444 avgL=Rs-1,179 maxDD=Rs-6,595 SL/TGT/EOD=13/23/16 tpd=2.74 tradeDom=0.042 dayDom=0.569 symDom=0.12 dbp=0.0889
- TEST:  n=17 PF=0.872 net=Rs-1,528 win%=52.9 avgW=Rs1,155 avgL=Rs-1,491 maxDD=Rs-4,290 SL/TGT/EOD=6/5/6 tpd=2.12 tradeDom=0.17 dayDom=9.99 symDom=9.99 dbp=0.6281
- robustness: neighbor=True dropout=True
- reasons: TRAIN domination (trade>35% gross or day/sym>40% net); TEST PF 0.872 <= 1.4; TEST net PnL not positive; TEST domination (trade>35% gross or day/sym>40% net); TEST day-block p 0.6281 > 0.1; TEST n 17 < 20 (thin, sample-limited)

### R3-window-{"max_slot": "12:00"} — reject

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 1.5,
    "tgt_pct": 2.0
  },
  "mask_terms": [],
  "pre_momentum_terms": [
    [
      "sig5_vol_ratio20",
      ">=",
      2.575953
    ],
    [
      "pre3_close_pos",
      ">=",
      0.541682
    ]
  ],
  "entry_guards": {
    "max_slot": "12:00"
  },
  "max_positions": 10,
  "daily_loss_rs": 0.0
}
```
- TRAIN: n=88 PF=1.307 net=Rs14,538 win%=54.5 avgW=Rs1,290 avgL=Rs-1,184 maxDD=Rs-10,570 SL/TGT/EOD=21/33/34 tpd=3.26 tradeDom=0.029 dayDom=0.522 symDom=0.127 dbp=0.1359
- TEST:  n=26 PF=0.977 net=Rs-371 win%=46.2 avgW=Rs1,304 avgL=Rs-1,144 maxDD=Rs-3,442 SL/TGT/EOD=8/8/10 tpd=3.25 tradeDom=0.113 dayDom=9.99 symDom=9.99 dbp=0.5318
- robustness: neighbor=True dropout=True
- reasons: TRAIN domination (trade>35% gross or day/sym>40% net); TEST PF 0.977 <= 1.4; TEST net PnL not positive; TEST domination (trade>35% gross or day/sym>40% net); TEST day-block p 0.5318 > 0.1

### R2-drop-premom-1 — reject

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 1.5,
    "tgt_pct": 2.0
  },
  "mask_terms": [],
  "pre_momentum_terms": [
    [
      "sig5_vol_ratio20",
      ">=",
      2.575953
    ]
  ],
  "entry_guards": {
    "max_slot": "11:30"
  },
  "max_positions": 10,
  "daily_loss_rs": 0.0
}
```
- TRAIN: n=49 PF=1.688 net=Rs16,714 win%=57.1 avgW=Rs1,464 avgL=Rs-1,156 maxDD=Rs-10,987 SL/TGT/EOD=11/23/15 tpd=2.45 tradeDom=0.043 dayDom=0.6 symDom=0.106 dbp=0.1164
- TEST:  n=18 PF=1.111 net=Rs1,239 win%=55.6 avgW=Rs1,239 avgL=Rs-1,394 maxDD=Rs-3,841 SL/TGT/EOD=6/6/6 tpd=2.25 tradeDom=0.142 dayDom=1.424 symDom=1.425 dbp=0.3774
- robustness: neighbor=True dropout=True
- reasons: TRAIN domination (trade>35% gross or day/sym>40% net); TEST PF 1.111 <= 1.4; TEST domination (trade>35% gross or day/sym>40% net); TEST day-block p 0.3774 > 0.1; TEST n 18 < 20 (thin, sample-limited)

## Top 25 FIT/VAL trials

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|----|------|--------|-------|----------|----------|-------|
| 1 | 1.5 | 2.0 | (none) | sig5_vol_ratio20>=2.575953; pre3_close_pos>=0.541682 | {"max_slot": "11:30"} | 17/1.699 | 22/1.709 | 1.741 |
| 2 | 1.5 | 2.0 | regime!=BULL | sig5_vol_ratio20>=2.575953; pre3_close_pos>=0.541682 | {"max_slot": "11:30"} | 17/1.699 | 22/1.709 | 1.741 |
| 3 | 1.5 | 2.0 | (none) | sig5_vol_ratio20>=2.575953; pre3_close_pos>=0.541682 | {"min_slot": "09:30", "max_slot": "11:30"} | 17/1.699 | 22/1.709 | 1.741 |
| 4 | 1.5 | 2.0 | (none) | sig5_vol_ratio20>=2.575953; pre3_close_pos>=0.541682 | {"max_slot": "11:30", "top_n": 3} | 17/1.699 | 22/1.709 | 1.741 |
| 5 | 1.5 | 2.0 | (none) | sig5_vol_ratio20>=2.575953; pre3_close_pos>=0.541682 | {"max_slot": "11:30"} | 17/1.699 | 22/1.709 | 1.741 |
| 6 | 1.5 | 2.0 | (none) | sig5_vol_ratio20>=2.575953; pre3_close_pos>=0.541682 | {"max_slot": "11:30"} | 17/1.699 | 22/1.709 | 1.741 |
| 7 | 1.5 | 2.0 | (none) | sig5_vol_ratio20>=2.575953; pre3_close_pos>=0.541682 | {"min_slot": "10:30", "max_slot": "11:30"} | 17/1.699 | 22/1.709 | 1.741 |
| 8 | 1.5 | 2.5 | (none) | pre_entry_momentum_score>=63.289967; pre3_close_pos>=0.541682 | {"max_slot": "11:30"} | 18/1.887 | 23/1.74 | 1.6757 |
| 9 | 1.5 | 2.0 | regime!=BULL | sig5_vol_ratio20>=2.936993; pre3_close_pos>=0.541682 | {"max_slot": "11:30"} | 14/1.623 | 20/1.608 | 1.6383 |
| 10 | 1.5 | 2.0 | (none) | pre3_close_pos>=0.848306; pre3_close_pos>=0.541682 | {"max_slot": "11:30"} | 12/1.955 | 12/1.938 | 1.615 |
| 11 | 1.1 | 2.0 | (none) | pre1_adx>=34.509932; pre1_adx>=31.595497 | {"max_slot": "11:30"} | 23/1.549 | 26/1.54 | 1.6026 |
| 12 | 1.5 | 2.0 | (none) | sig5_vol_ratio20>=2.575953; pre1_adx>=31.595497 | {"min_slot": "09:30", "max_slot": "11:30"} | 20/2.025 | 24/1.733 | 1.5587 |
| 13 | 1.5 | 2.0 | (none) | sig5_vol_ratio20>=2.575953; pre1_adx>=31.595497 | {"max_slot": "11:30"} | 20/2.025 | 24/1.733 | 1.5587 |
| 14 | 1.5 | 2.0 | (none) | pre3_close_pos>=0.365156; pre3_close_pos>=0.541682 | {"max_slot": "11:30"} | 24/1.508 | 28/1.579 | 1.5227 |
| 15 | 1.5 | 2.5 | (none) | sig5_vol_ratio20>=2.575953; pre3_close_pos>=0.541682 | {"min_slot": "11:00", "max_slot": "11:30"} | 17/1.758 | 21/2.123 | 1.5171 |
| 16 | 1.5 | 2.0 | (none) | pre3_close_pos>=0.653; pre1_adx>=31.595497 | {"max_slot": "11:30"} | 16/2.082 | 19/1.741 | 1.5159 |
| 17 | 1.5 | 2.0 | (none) | pre_entry_momentum_score>=63.289967 | {"min_slot": "11:00", "max_slot": "11:30"} | 18/1.678 | 23/1.961 | 1.5052 |
| 18 | 1.5 | 2.5 | rsi_slope3>=10.092007; regime!=BULL | sig5_vol_ratio20>=1.818637; sig5_rsi_dir>=62.814507 | {"max_slot": "11:30", "top_n": 1} | 17/1.583 | 17/1.499 | 1.4823 |
| 19 | 1.5 | 2.0 | (none) | pre_entry_momentum_score>=63.289967; pre3_close_pos>=0.541682 | {"max_slot": "11:30"} | 18/1.678 | 23/1.534 | 1.4733 |
| 20 | 1.5 | 2.0 | (none) | sig5_vol_ratio20>=2.575953; sig5_rsi_dir>=65.903938 | {"max_slot": "11:30"} | 11/1.571 | 24/1.738 | 1.4703 |
| 21 | 1.5 | 2.0 | regime!=TREND | sig5_vol_ratio20>=2.575953; sig5_rsi_dir>=65.903938 | {"max_slot": "11:30"} | 11/1.571 | 24/1.738 | 1.4703 |
| 22 | 1.2 | 2.0 | (none) | pre1_adx>=34.509932; sig5_rsi_dir>=62.814507 | {"max_slot": "11:30"} | 15/1.657 | 22/1.523 | 1.46 |
| 23 | 1.2 | 2.0 | (none) | sig5_vol_ratio20>=2.169739; pre3_close_pos>=0.541682 | {"max_slot": "11:30"} | 20/1.474 | 24/1.569 | 1.4576 |
| 24 | 1.5 | 2.0 | (none) | sig5_rsi_dir>=67.983871; pre3_close_pos>=0.541682 | {"max_slot": "11:30"} | 13/1.461 | 19/1.524 | 1.4487 |
| 25 | 1.5 | 2.0 | regime!=TREND | sig5_vol_ratio20>=2.575953; pre5_mom_r>=0.273359 | {"max_slot": "11:30"} | 15/1.597 | 20/1.481 | 1.4333 |

## Best round-2 single-knob improvements

- **indicator/price-action / +mask cci<=** -> 178.637948 (q0.2) (FIT 68/0.692, VAL 31/0.708, score 0.6792)
- **indicator/price-action / +mask reclow_dist_atr>=** -> 5.362058 (q0.8) (FIT 73/0.647, VAL 23/0.638, score 0.6308)
- **indicator/price-action / +mask bb_pos<=** -> 1.060001 (q0.2) (FIT 70/0.605, VAL 31/0.61, score 0.601)
- **pre-momentum / +premom sig5_adx_calc<=** -> 19.277018 (q0.2) (FIT 71/0.646, VAL 42/0.62, score 0.5992)
- **pre-momentum / +premom sig5_adx_calc<=** -> 26.805832 (q0.5) (FIT 183/0.607, VAL 97/0.594, score 0.5836)
- **indicator/price-action / +mask adx5<=** -> 19.277018 (q0.2) (FIT 69/0.634, VAL 40/0.592, score 0.5584)
- **indicator/price-action / +mask dist_day_low_atr<=** -> 5.213765 (q0.2) (FIT 67/0.556, VAL 46/0.556, score 0.556)
- **indicator/price-action / +mask macd_atr<=** -> -0.206149 (q0.2) (FIT 81/0.545, VAL 21/0.552, score 0.5394)
- **indicator/price-action / +mask adx5<=** -> 26.805832 (q0.5) (FIT 181/0.585, VAL 94/0.556, score 0.5328)
- **indicator/price-action / +mask bb_width_pct<=** -> 2.760943 (q0.8) (FIT 282/0.534, VAL 116/0.524, score 0.516)
- **indicator/price-action / +mask reclow_dist_atr>=** -> 3.475713 (q0.2) (FIT 284/0.51, VAL 113/0.517, score 0.5044)
- **pre-momentum / +premom sig5_rsi_dir<=** -> 69.352257 (q0.5) (FIT 192/0.526, VAL 96/0.512, score 0.5008)
- **guard / daily_loss_rs** -> 2000.0 (FIT 219/0.565, VAL 92/0.529, score 0.5002)
- **indicator/price-action / +mask quality_score<=** -> 29.864302 (q0.2) (FIT 67/0.498, VAL 44/0.502, score 0.4948)
- **indicator/price-action / +mask vwap_dist_atr>=** -> 4.48344 (q0.8) (FIT 75/0.543, VAL 33/0.606, score 0.4926)
- **indicator/price-action / +mask obv_slope10_norm<=** -> 1.048299 (q0.8) (FIT 285/0.508, VAL 113/0.499, score 0.4918)
- **indicator/price-action / +mask ema50_dist_atr<=** -> 3.282719 (q0.5) (FIT 189/0.499, VAL 88/0.51, score 0.4902)
- **indicator/price-action / +mask mfi>=** -> 86.17028 (q0.8) (FIT 68/0.567, VAL 27/0.522, score 0.486)
- **indicator/price-action / +mask macd_atr<=** -> 0.99774 (q0.8) (FIT 288/0.535, VAL 110/0.506, score 0.4828)
- **indicator/price-action / +mask close_loc>=** -> 0.96775 (q0.8) (FIT 78/0.482, VAL 32/0.482, score 0.482)

## Live-parity caveat for enriched features

- Enriched mask features are computed from the SAME live 5-min indicator feed, but the current conf gate only reads scanner-emitted candidate fields. Promoting an enriched-mask candidate requires a small gate extension (look up the indicator columns at apply time). Flag this at approval.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**