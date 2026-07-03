# B_HUGE_RED_FAILED_BOUNCE (SHORT) — ROUND2_RESULTS (enriched feature space)

_Generated 2026-07-03. ROUND 2 (enriched indicator/price-action feature space). Research-only; NO live trades; NO final_setup_conf.py edits._

- Optimizer: Optuna TPE | trials 800 (630 unique) | sweeps 402 | TEST evals used 0
- Windows: TRAIN 2026-03-02..2026-05-29 (53 sess) | TEST 2026-06-01..2026-06-30 (20 sess)
- Search space: 3 mask terms over base + enriched features (RSI/ADX/MACD/EMA/BB/Stoch/MFI/CCI/OBV/pressure/vol-z/ROC/W%R + day/gap/OR/prev-day geometry + prev-candle structure), 2 pre-momentum terms, regime, slots, top_n, max_positions, daily_loss, exit grid.
- **Passing candidates: 0**

## Baseline (round-1 conf/default config on this pool)

- FIT: n=22 PF=0.848 net=Rs-1,301 win%=50.0 avgW=Rs660 avgL=Rs-778 maxDD=Rs-3,993 SL/TGT/EOD=6/6/10 tpd=2.2 tradeDom=0.14 dayDom=9.99 symDom=9.99 dbp=0.6214
- VAL: n=26 PF=0.621 net=Rs-4,499 win%=42.3 avgW=Rs671 avgL=Rs-792 maxDD=Rs-7,326 SL/TGT/EOD=8/5/13 tpd=2.36 tradeDom=0.138 dayDom=9.99 symDom=9.99 dbp=0.8506
- TRAIN: n=48 PF=0.716 net=Rs-5,800 win%=45.8 avgW=Rs666 avgL=Rs-786 maxDD=Rs-8,899 SL/TGT/EOD=14/11/23 tpd=2.29 tradeDom=0.07 dayDom=9.99 symDom=9.99 dbp=0.8313
- TEST: n=41 PF=0.72 net=Rs-4,573 win%=34.1 avgW=Rs841 avgL=Rs-605 maxDD=Rs-6,342 SL/TGT/EOD=11/11/19 tpd=2.28 tradeDom=0.086 dayDom=9.99 symDom=9.99 dbp=0.8738

## Finalists / rescue results

### finalist #1 — reject

```json
{
  "side": "SHORT",
  "exit": {
    "sl_pct": 1.0,
    "tgt_pct": 1.5
  },
  "mask_terms": [
    [
      "gap_pct",
      "<=",
      -0.412302
    ],
    [
      "regime",
      "==",
      "BEAR"
    ]
  ],
  "pre_momentum_terms": [
    [
      "sig5_adx_calc",
      "<=",
      25.661066
    ]
  ],
  "entry_guards": {
    "min_slot": "09:45",
    "top_n": 3
  },
  "max_positions": 20,
  "daily_loss_rs": 0.0
}
```
- TRAIN: n=79 PF=0.99 net=Rs-357 win%=46.8 avgW=Rs980 avgL=Rs-872 maxDD=Rs-10,082 SL/TGT/EOD=24/24/31 tpd=5.64 tradeDom=0.035 dayDom=9.99 symDom=9.99 dbp=0.5389
- reasons: TRAIN not in band or too thin (PF 0.99, n 79)

### finalist #2 — reject

```json
{
  "side": "SHORT",
  "exit": {
    "sl_pct": 0.85,
    "tgt_pct": 1.0
  },
  "mask_terms": [
    [
      "gap_pct",
      "<=",
      -0.412302
    ],
    [
      "regime",
      "==",
      "BEAR"
    ]
  ],
  "pre_momentum_terms": [
    [
      "sig5_adx_calc",
      "<=",
      30.819969
    ]
  ],
  "entry_guards": {
    "min_slot": "09:45",
    "top_n": 2
  },
  "max_positions": 10,
  "daily_loss_rs": 4000.0
}
```
- TRAIN: n=86 PF=0.929 net=Rs-2,460 win%=51.2 avgW=Rs737 avgL=Rs-831 maxDD=Rs-10,527 SL/TGT/EOD=29/42/15 tpd=5.73 tradeDom=0.024 dayDom=9.99 symDom=9.99 dbp=0.6079
- reasons: TRAIN not in band or too thin (PF 0.929, n 86)

### finalist #3 — reject

```json
{
  "side": "SHORT",
  "exit": {
    "sl_pct": 0.85,
    "tgt_pct": 1.5
  },
  "mask_terms": [
    [
      "gap_pct",
      "<=",
      -0.412302
    ],
    [
      "regime",
      "==",
      "BEAR"
    ]
  ],
  "pre_momentum_terms": [
    [
      "sig5_adx_calc",
      "<=",
      30.819969
    ]
  ],
  "entry_guards": {
    "min_slot": "09:45",
    "top_n": 2
  },
  "max_positions": 20,
  "daily_loss_rs": 4000.0
}
```
- TRAIN: n=89 PF=0.971 net=Rs-1,147 win%=46.1 avgW=Rs953 avgL=Rs-838 maxDD=Rs-13,735 SL/TGT/EOD=33/26/30 tpd=5.93 tradeDom=0.032 dayDom=9.99 symDom=9.99 dbp=0.5402
- reasons: TRAIN not in band or too thin (PF 0.971, n 89)

### finalist #4 — reject

```json
{
  "side": "SHORT",
  "exit": {
    "sl_pct": 0.85,
    "tgt_pct": 1.0
  },
  "mask_terms": [
    [
      "gap_pct",
      "<=",
      -0.412302
    ],
    [
      "regime",
      "==",
      "BEAR"
    ]
  ],
  "pre_momentum_terms": [
    [
      "pre5_mom_r",
      "<=",
      0.547391
    ]
  ],
  "entry_guards": {
    "min_slot": "09:45",
    "top_n": 2
  },
  "max_positions": 10,
  "daily_loss_rs": 4000.0
}
```
- TRAIN: n=79 PF=0.827 net=Rs-5,536 win%=51.9 avgW=Rs644 avgL=Rs-841 maxDD=Rs-11,011 SL/TGT/EOD=27/33/19 tpd=5.64 tradeDom=0.029 dayDom=9.99 symDom=9.99 dbp=0.7198
- reasons: TRAIN not in band or too thin (PF 0.827, n 79)

### finalist #5 — reject

```json
{
  "side": "SHORT",
  "exit": {
    "sl_pct": 1.0,
    "tgt_pct": 1.5
  },
  "mask_terms": [
    [
      "gap_pct",
      "<=",
      -0.412302
    ],
    [
      "regime",
      "==",
      "BEAR"
    ]
  ],
  "pre_momentum_terms": [
    [
      "sig5_rsi_dir",
      "<=",
      73.934093
    ]
  ],
  "entry_guards": {
    "min_slot": "10:00",
    "top_n": 3
  },
  "max_positions": 20,
  "daily_loss_rs": 0.0
}
```
- TRAIN: n=84 PF=0.874 net=Rs-5,210 win%=47.6 avgW=Rs902 avgL=Rs-939 maxDD=Rs-13,306 SL/TGT/EOD=28/23/33 tpd=5.25 tradeDom=0.035 dayDom=9.99 symDom=9.99 dbp=0.688
- reasons: TRAIN not in band or too thin (PF 0.874, n 84)

### finalist #6 — reject

```json
{
  "side": "SHORT",
  "exit": {
    "sl_pct": 0.85,
    "tgt_pct": 0.8
  },
  "mask_terms": [
    [
      "gap_pct",
      "<=",
      -0.412302
    ],
    [
      "regime",
      "==",
      "BEAR"
    ]
  ],
  "pre_momentum_terms": [
    [
      "sig5_adx_calc",
      "<=",
      30.819969
    ]
  ],
  "entry_guards": {
    "min_slot": "09:45",
    "top_n": 2
  },
  "max_positions": 10,
  "daily_loss_rs": 4000.0
}
```
- TRAIN: n=86 PF=0.856 net=Rs-4,543 win%=55.8 avgW=Rs564 avgL=Rs-832 maxDD=Rs-8,827 SL/TGT/EOD=26/48/12 tpd=5.73 tradeDom=0.021 dayDom=9.99 symDom=9.99 dbp=0.6978
- reasons: TRAIN not in band or too thin (PF 0.856, n 86)

## Top 25 FIT/VAL trials

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|----|------|--------|-------|----------|----------|-------|
| 1 | 1.0 | 1.5 | gap_pct<=-0.412302; regime==BEAR | sig5_adx_calc<=25.661066 | {"min_slot": "09:45", "top_n": 3} | 66/0.992 | 13/0.981 | 0.9725 |
| 2 | 0.85 | 1.0 | gap_pct<=-0.412302; regime==BEAR | sig5_adx_calc<=30.819969 | {"min_slot": "09:45", "top_n": 2} | 73/0.923 | 13/0.965 | 0.8892 |
| 3 | 0.85 | 1.0 | gap_pct<=-0.412302; regime==BEAR | sig5_adx_calc<=30.819969 | {"min_slot": "10:30", "top_n": 2} | 73/0.923 | 13/0.965 | 0.8892 |
| 4 | 0.85 | 1.5 | gap_pct<=-0.412302; regime==BEAR | sig5_adx_calc<=30.819969 | {"min_slot": "09:45", "top_n": 2} | 76/0.949 | 13/1.115 | 0.8161 |
| 5 | 0.85 | 1.0 | gap_pct<=-0.412302; regime==BEAR | pre5_mom_r<=0.547391 | {"min_slot": "09:45", "top_n": 2} | 63/0.839 | 16/0.783 | 0.7377 |
| 6 | 1.0 | 1.5 | gap_pct<=-0.412302; regime==BEAR | sig5_adx_calc<=37.125226 | {"min_slot": "10:00", "top_n": 3} | 100/0.782 | 16/0.85 | 0.7267 |
| 7 | 1.0 | 1.5 | gap_pct<=-0.412302; regime==BEAR | sig5_rsi_dir<=73.934093 | {"min_slot": "10:00", "top_n": 3} | 73/0.888 | 11/0.792 | 0.716 |
| 8 | 1.0 | 1.5 | gap_pct<=-0.412302; regime==BEAR | sig5_adx_calc<=30.819969 | {"min_slot": "09:45", "top_n": 3} | 83/0.917 | 14/1.184 | 0.7041 |
| 9 | 0.85 | 0.8 | gap_pct<=-0.412302; regime==BEAR | sig5_adx_calc<=30.819969 | {"min_slot": "09:45", "top_n": 2} | 73/0.83 | 13/1.016 | 0.6817 |
| 10 | 1.0 | 1.5 | gap_pct<=-0.412302; regime==BEAR | sig5_adx_calc<=44.580836 | {"min_slot": "10:00", "top_n": 3} | 97/0.716 | 18/0.776 | 0.6685 |
| 11 | 1.0 | 1.5 | gap_pct<=-0.412302; regime==BEAR | sig5_vol_ratio20<=6.093677 | {"min_slot": "10:00", "top_n": 3} | 105/0.666 | 23/0.661 | 0.657 |
| 12 | 1.0 | 1.5 | gap_pct<=-0.412302; regime==BEAR | sig5_vol_ratio20<=6.093677 | {"min_slot": "09:45", "top_n": 3} | 105/0.666 | 23/0.661 | 0.657 |
| 13 | 1.1 | 1.5 | gap_pct<=-0.173452 | sig5_vol_ratio20<=6.093677 | {"max_slot": "12:00"} | 57/0.697 | 19/0.763 | 0.6442 |
| 14 | 1.0 | 1.5 | gap_pct<=-0.412302; regime==BEAR | sig5_vol_ratio20<=4.849113 | {"min_slot": "10:00", "top_n": 3} | 119/0.61 | 22/0.615 | 0.6069 |
| 15 | 1.0 | 1.5 | gap_pct<=-0.412302; regime==BEAR | sig5_vol_ratio20<=4.849113 | {"min_slot": "09:45", "top_n": 3} | 119/0.61 | 22/0.615 | 0.6069 |
| 16 | 1.0 | 1.5 | gap_pct<=-0.412302; regime==BEAR | sig5_vol_ratio20<=4.849113 | {"min_slot": "09:30", "top_n": 3} | 119/0.61 | 22/0.615 | 0.6069 |
| 17 | 1.5 | 1.25 | gap_pct<=-0.173452; regime==BEAR | sig5_vol_ratio20<=6.093677 | - | 137/0.616 | 28/0.635 | 0.6016 |
| 18 | 1.0 | 1.5 | gap_pct<=-0.412302; regime==BEAR | sig5_rsi_dir<=79.110554 | {"min_slot": "10:00", "top_n": 3} | 109/0.641 | 19/0.691 | 0.601 |
| 19 | 1.0 | 1.5 | gap_pct<=-0.412302; regime==BEAR | sig5_rsi_dir<=79.110554 | {"min_slot": "09:30", "top_n": 3} | 109/0.641 | 19/0.691 | 0.601 |
| 20 | 1.0 | 1.5 | gap_pct<=-0.412302; regime==BEAR | sig5_rsi_dir<=79.110554 | {"min_slot": "09:45", "top_n": 3} | 109/0.641 | 19/0.691 | 0.601 |
| 21 | 0.85 | 1.0 | gap_pct<=-0.412302; regime!=TREND | sig5_adx_calc<=30.819969 | {"min_slot": "09:45", "top_n": 2} | 97/0.586 | 31/0.59 | 0.5834 |
| 22 | 1.0 | 1.5 | gap_pct<=-0.412302; regime==BEAR | pre5_mom_r<=0.547391 | {"min_slot": "09:30", "top_n": 1} | 48/0.695 | 12/0.628 | 0.5747 |
| 23 | 1.5 | 1.5 | gap_pct<=0.023171; regime==BEAR | sig5_vol_ratio20<=6.093677 | {"min_slot": "09:45", "top_n": 3} | 152/0.626 | 31/0.596 | 0.5723 |
| 24 | 1.5 | 1.5 | gap_pct<=0.023171; regime==BEAR | sig5_vol_ratio20<=6.093677 | {"top_n": 3} | 152/0.626 | 31/0.596 | 0.5723 |
| 25 | 0.85 | 1.0 | gap_pct<=-0.412302; regime==BEAR | sig5_adx_calc<=30.819969 | {"min_slot": "12:00", "top_n": 2} | 66/0.92 | 10/1.359 | 0.5683 |

## Best round-2 single-knob improvements

- **indicator/price-action / +mask prev3_up<=** -> 1.0 (q0.8) (FIT 18/1.231, VAL 17/1.008, score 0.8296)
- **indicator/price-action / +mask prev3_up<=** -> 1.0 (q0.5) (FIT 18/1.231, VAL 17/1.008, score 0.8296)
- **indicator/price-action / +mask rs_pct>=** -> -0.85198 (q0.5) (FIT 18/0.897, VAL 15/0.847, score 0.807)
- **exit / sl_pct** -> 1.5 (FIT 26/0.821, VAL 32/0.868, score 0.7834)
- **indicator/price-action / +mask bb_pos>=** -> -0.182075 (q0.5) (FIT 19/0.942, VAL 10/0.849, score 0.7746)
- **indicator/price-action / +mask candle_range_atr<=** -> 1.963765 (q0.5) (FIT 18/1.133, VAL 20/0.932, score 0.7712)
- **guard / max_slot** -> 14:00 (FIT 19/0.831, VAL 20/0.908, score 0.7694)
- **indicator/price-action / +mask signal_range_pct<=** -> 0.536963 (q0.5) (FIT 15/0.987, VAL 19/0.84, score 0.7224)
- **indicator/price-action / +mask mfi>=** -> 22.415659 (q0.5) (FIT 21/0.729, VAL 11/0.756, score 0.7074)
- **indicator/price-action / +mask vwap_dist_atr>=** -> -2.60068 (q0.8) (FIT 19/0.703, VAL 15/0.705, score 0.7014)
- **pre-momentum / +premom pre3_range_r<=** -> 0.667405 (q0.8) (FIT 21/0.729, VAL 24/0.766, score 0.6994)
- **indicator/price-action / +mask or15_break_atr>=** -> -8.962519 (q0.5) (FIT 19/0.86, VAL 18/0.77, score 0.698)
- **pre-momentum / +premom pre3_range_r<=** -> 0.364407 (q0.5) (FIT 19/0.7, VAL 22/0.707, score 0.6944)
- **indicator/price-action / +mask ema_stack_atr>=** -> -0.784238 (q0.5) (FIT 21/0.729, VAL 17/0.708, score 0.6912)
- **indicator/price-action / +mask close_loc<=** -> 0.264187 (q0.8) (FIT 11/0.699, VAL 19/0.716, score 0.6854)
- **indicator/price-action / +mask macd_hist_atr>=** -> -0.284971 (q0.5) (FIT 21/0.845, VAL 11/0.756, score 0.6848)
- **indicator/price-action / +mask macd_sig_atr>=** -> -0.284971 (q0.5) (FIT 21/0.845, VAL 11/0.756, score 0.6848)
- **indicator/price-action / +mask obv_slope10_norm>=** -> -1.065512 (q0.2) (FIT 22/0.848, VAL 11/0.756, score 0.6824)
- **indicator/price-action / +mask pdh_dist_atr<=** -> -4.536651 (q0.8) (FIT 13/1.091, VAL 19/0.863, score 0.6806)
- **indicator/price-action / +mask bb_width_pct<=** -> 2.534401 (q0.8) (FIT 18/0.696, VAL 12/0.729, score 0.6696)

## Live-parity caveat for enriched features

- Enriched mask features are computed from the SAME live 5-min indicator feed, but the current conf gate only reads scanner-emitted candidate fields. Promoting an enriched-mask candidate requires a small gate extension (look up the indicator columns at apply time). Flag this at approval.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**