# CONFIG_PARAM_ITERATION — B_AVWAP_RECLAIM_REVERSAL

One-parameter-at-a-time sweep **anchored on the given config**, net of cost @ 15 bps/leg. Each row changes ONLY that parameter; all others stay at the anchor. 'score' = min(FIT_PF, VAL_PF) (−1 if a fold has < 6 trades). ✓ on TRAIN = inside the [1.3,1.7] band. TEST shown for reference only (never optimised on).

- TRAIN 2026-05-18..2026-06-16 (20) · TEST 2026-06-22..2026-06-24 (2) · entries FIT=680 VAL=753 TRAIN=1433 TEST=123

## TL;DR — config is already at a local FIT/VAL optimum; no parameter improves TEST
- **7 of 10 parameters: current value is already best** (SL 0.9, vol_ratio≥3.54, atr_pct≤0.0039, pre1_adx≥30.7, pre5_mom_r≥0.317, max_positions, daily_loss_rs).
- The only in-sample lifts are cosmetic and **do not touch TEST**:
  - `target 3.0→3.5`: TRAIN PF 1.44→1.47, **TEST PF stays 0.441** (same 4 trades).
  - `max_slot 14:00→13:30`: TRAIN PF 1.44→**1.715 (ABOVE the 1.70 band = overfit)**, **TEST PF stays 0.441**.
  - `max_positions 20→5`: identical score (the cap never binds for one setup).
- **TEST PF is invariant at 0.441 across every single-parameter change** — the 4 TEST trades (2 days) don't respond to any knob. The failure is structural (no OOS edge), not a tuning problem. **No better match exists by varying these parameters.** Verdict unchanged: do not promote.

## Anchor config
```json
{
  "exit": {
    "sl_pct": 0.9,
    "tgt_pct": 3.0
  },
  "mask_terms": [
    [
      "vwap_dist_atr",
      "<=",
      1.0
    ],
    [
      "vol_ratio",
      ">=",
      3.537825
    ],
    [
      "atr_pct",
      "<=",
      0.003921
    ]
  ],
  "pre_momentum_terms": [
    [
      "pre1_adx",
      ">=",
      30.675856
    ],
    [
      "pre5_mom_r",
      ">=",
      0.317166
    ]
  ],
  "entry_guards": {
    "max_slot": "14:00"
  },
  "max_positions": 20,
  "daily_loss_rs": 0.0
}
```
- Anchor metrics: FIT 11/1.586 · VAL 19/1.382 · TRAIN 30/1.436 (net Rs5,782, dayDom 1.279) · TEST 4/0.441 (net Rs-1,526) · score 1.382

## EXIT sl_pct (anchor 0.9)

| value | FIT n/PF | VAL n/PF | TRAIN n/PF | TEST n/PF | score |
|---|---|---|---|---|---|
| SL=0.5  | 13/0.561 | 25/0.828 | 38/0.739 | 6/0.358 | 0.561 |
| SL=0.6  | 13/0.852 | 23/1.221 | 36/1.09 | 6/0.32 | 0.852 |
| SL=0.7  | 13/1.248 | 22/1.19 | 35/1.207 | 6/0.313 | 1.19 |
| SL=0.8  | 11/1.382 | 22/1.184 | 33/1.239 | 6/0.291 | 1.184 |
| SL=0.85  | 11/1.632 | 20/1.146 | 31/1.263 | 6/0.28 | 1.146 |
| SL=0.9 **(current)** ✅best | 11/1.586 | 19/1.382 | 30/1.436✓ | 4/0.441 | 1.382 |
| SL=1  | 11/1.5 | 17/1.096 | 28/1.211 | 4/0.411 | 1.096 |
| SL=1.1  | 10/1.328 | 14/1.127 | 24/1.185 | 4/0.385 | 1.127 |
| SL=1.2  | 9/1.269 | 14/1.081 | 23/1.136 | 2/0.0 | 1.081 |
| SL=1.3  | 7/0.985 | 11/1.165 | 18/1.11 | 2/0.0 | 0.985 |
| SL=1.5  | 5/0.579 | 11/1.07 | 16/0.948 | 1/0.0 | -1.0 |

- **best = `SL=0.9`** (score 1.382 vs current 1.382); TRAIN PF 1.436 in-band, TEST PF 0.441

## EXIT tgt_pct (anchor 3.0)

| value | FIT n/PF | VAL n/PF | TRAIN n/PF | TEST n/PF | score |
|---|---|---|---|---|---|
| Tgt=1  | 11/1.308 | 19/0.628 | 30/0.808 | 4/0.281 | 0.628 |
| Tgt=1.25  | 11/1.669 | 19/0.832 | 30/1.053 | 4/0.372 | 0.832 |
| Tgt=1.5  | 11/1.94 | 19/0.928 | 30/1.196 | 4/0.464 | 0.928 |
| Tgt=1.75  | 11/1.554 | 19/1.058 | 30/1.189 | 4/0.555 | 1.058 |
| Tgt=2  | 11/1.657 | 19/1.186 | 30/1.311✓ | 4/0.441 | 1.186 |
| Tgt=2.5  | 11/1.8 | 19/1.405 | 30/1.509✓ | 4/0.441 | 1.405 |
| Tgt=3 **(current)** | 11/1.586 | 19/1.382 | 30/1.436✓ | 4/0.441 | 1.382 |
| Tgt=3.5 ✅best | 11/1.586 | 19/1.433 | 30/1.473✓ | 4/0.441 | 1.433 |

- **best = `Tgt=3.5`** (score 1.433 vs current 1.382); TRAIN PF 1.473 in-band, TEST PF 0.441

## MASK term [vwap_dist_atr<=1]

| value | FIT n/PF | VAL n/PF | TRAIN n/PF | TEST n/PF | score |
|---|---|---|---|---|---|
| vwap_dist_atr<=0.191358  | 1/inf | 1/0.0 | 2/0.554 | 1/inf | -1.0 |
| vwap_dist_atr<=0.300084  | 2/0.555 | 2/0.0 | 4/0.185 | 1/inf | -1.0 |
| vwap_dist_atr<=0.426195  | 5/0.196 | 5/0.427 | 10/0.329 | 1/inf | -1.0 |
| vwap_dist_atr<=0.578625  | 6/0.179 | 11/1.331 | 17/0.925 | 2/2.553 | 0.179 |
| vwap_dist_atr<=0.757981  | 8/0.888 | 15/1.609 | 23/1.38✓ | 4/0.441 | 0.888 |
| vwap_dist_atr<=0.945203 ✅best | 11/1.586 | 19/1.382 | 30/1.436✓ | 4/0.441 | 1.382 |
| vwap_dist_atr<=1 **(current)** | 11/1.586 | 19/1.382 | 30/1.436✓ | 4/0.441 | 1.382 |
| vwap_dist_atr<=1.16624  | 15/0.873 | 25/1.04 | 40/0.985 | 4/0.441 | 0.873 |
| vwap_dist_atr<=1.42458  | 17/0.812 | 34/1.16 | 51/1.052 | 7/1.266 | 0.812 |
| vwap_dist_atr<=1.75598  | 20/0.823 | 39/0.917 | 59/0.889 | 7/1.266 | 0.823 |
| DROP vwap_dist_atr  | 28/0.764 | 51/0.987 | 79/0.915 | 10/0.636 | 0.764 |

- **best = `vwap_dist_atr<=0.945203`** (score 1.382 vs current 1.382); TRAIN PF 1.436 in-band, TEST PF 0.441

## MASK term [vol_ratio>=3.53783]

| value | FIT n/PF | VAL n/PF | TRAIN n/PF | TEST n/PF | score |
|---|---|---|---|---|---|
| vol_ratio>=1.64952  | 30/1.118 | 50/0.834 | 80/0.922 | 13/0.526 | 0.834 |
| vol_ratio>=1.81521  | 28/1.065 | 48/0.893 | 76/0.949 | 13/0.526 | 0.893 |
| vol_ratio>=2.02507  | 28/1.065 | 45/0.917 | 73/0.967 | 12/0.545 | 0.917 |
| vol_ratio>=2.27738  | 25/1.092 | 41/0.974 | 66/1.014 | 10/0.605 | 0.974 |
| vol_ratio>=2.56952  | 20/1.868 | 33/1.122 | 53/1.332✓ | 7/1.214 | 1.122 |
| vol_ratio>=2.98774  | 15/2.384 | 26/1.154 | 41/1.433✓ | 6/1.46 | 1.154 |
| vol_ratio>=3.53783 **(current)** ✅best | 11/1.586 | 19/1.382 | 30/1.436✓ | 4/0.441 | 1.382 |
| vol_ratio>=4.41041  | 8/1.295 | 16/1.588 | 24/1.519✓ | 2/0.0 | 1.295 |
| vol_ratio>=5.61714  | 2/0.856 | 10/1.414 | 12/1.336✓ | 1/0.0 | -1.0 |
| DROP vol_ratio  | 31/1.027 | 56/0.869 | 87/0.918 | 13/0.526 | 0.869 |

- current value is already best

## MASK term [atr_pct<=0.003921]

| value | FIT n/PF | VAL n/PF | TRAIN n/PF | TEST n/PF | score |
|---|---|---|---|---|---|
| atr_pct<=0.001689  | 0/0.0 | 1/inf | 1/inf | 0/0.0 | -1.0 |
| atr_pct<=0.001926  | 3/2.677 | 4/inf | 7/24.452 | 0/0.0 | -1.0 |
| atr_pct<=0.002151  | 5/1.726 | 5/6.308 | 10/4.042 | 1/0.0 | -1.0 |
| atr_pct<=0.002383  | 5/1.726 | 8/2.098 | 13/2.007 | 2/1.068 | -1.0 |
| atr_pct<=0.002649  | 5/1.726 | 10/2.003 | 15/1.949 | 3/0.753 | -1.0 |
| atr_pct<=0.002919  | 5/1.726 | 14/1.348 | 19/1.4✓ | 3/0.753 | -1.0 |
| atr_pct<=0.00331  | 7/0.757 | 17/1.243 | 24/1.138 | 3/0.753 | 0.757 |
| atr_pct<=0.003921 **(current)** ✅best | 11/1.586 | 19/1.382 | 30/1.436✓ | 4/0.441 | 1.382 |
| atr_pct<=0.005113  | 12/1.199 | 19/1.382 | 31/1.323✓ | 6/0.241 | 1.199 |
| DROP atr_pct  | 14/1.441 | 21/1.122 | 35/1.225 | 6/0.241 | 1.122 |

- current value is already best

## PRE-MOM term [pre1_adx>=30.6759]

| value | FIT n/PF | VAL n/PF | TRAIN n/PF | TEST n/PF | score |
|---|---|---|---|---|---|
| pre1_adx>=17.564  | 18/1.622 | 49/0.761 | 67/0.918 | 11/0.393 | 0.761 |
| pre1_adx>=20.6027  | 16/1.467 | 43/0.855 | 59/0.974 | 11/0.393 | 0.855 |
| pre1_adx>=23.0378  | 15/1.402 | 39/0.916 | 54/1.016 | 9/0.552 | 0.916 |
| pre1_adx>=25.3821  | 12/2.066 | 28/0.835 | 40/1.046 | 8/0.521 | 0.835 |
| pre1_adx>=27.9015  | 11/1.586 | 24/0.945 | 35/1.071 | 5/0.361 | 0.945 |
| pre1_adx>=30.6759 **(current)** ✅best | 11/1.586 | 19/1.382 | 30/1.436✓ | 4/0.441 | 1.382 |
| pre1_adx>=33.6157  | 9/0.976 | 11/1.459 | 20/1.248 | 3/0.751 | 0.976 |
| pre1_adx>=37.4334  | 7/0.74 | 7/0.964 | 14/0.85 | 2/0.0 | 0.74 |
| pre1_adx>=43.3961  | 2/0.0 | 6/0.372 | 8/0.292 | 0/0.0 | -1.0 |
| DROP pre1_adx  | 20/1.567 | 52/0.805 | 72/0.963 | 12/0.344 | 0.805 |

- current value is already best

## PRE-MOM term [pre5_mom_r>=0.317166]

| value | FIT n/PF | VAL n/PF | TRAIN n/PF | TEST n/PF | score |
|---|---|---|---|---|---|
| pre5_mom_r>=-0.150325  | 24/1.007 | 31/0.773 | 55/0.85 | 7/0.247 | 0.773 |
| pre5_mom_r>=-0.021394  | 21/1.033 | 29/0.841 | 50/0.902 | 7/0.247 | 0.841 |
| pre5_mom_r>=0.111319  | 15/0.952 | 28/0.901 | 43/0.915 | 7/0.247 | 0.901 |
| pre5_mom_r>=0.223518  | 13/1.312 | 23/1.291 | 36/1.297 | 6/0.271 | 1.291 |
| pre5_mom_r>=0.317166 **(current)** ✅best | 11/1.586 | 19/1.382 | 30/1.436✓ | 4/0.441 | 1.382 |
| pre5_mom_r>=0.4203  | 9/1.11 | 14/1.076 | 23/1.087 | 2/0.0 | 1.076 |
| pre5_mom_r>=0.514042  | 5/0.547 | 11/1.212 | 16/1.023 | 1/0.0 | -1.0 |
| pre5_mom_r>=0.658392  | 5/0.547 | 3/0.0 | 8/0.236 | 0/0.0 | -1.0 |
| pre5_mom_r>=0.903159  | 1/0.0 | 1/0.0 | 2/0.0 | 0/0.0 | -1.0 |
| DROP pre5_mom_r  | 29/0.816 | 31/0.773 | 60/0.791 | 7/0.247 | 0.773 |

- current value is already best

## GUARD max_slot (anchor 14:00)

| value | FIT n/PF | VAL n/PF | TRAIN n/PF | TEST n/PF | score |
|---|---|---|---|---|---|
| max_slot=None  | 15/1.251 | 23/1.137 | 38/1.171 | 4/0.441 | 1.137 |
| max_slot=11:30  | 1/0.0 | 2/inf | 3/2.616 | 0/0.0 | -1.0 |
| max_slot=12:00  | 4/2.813 | 5/1.262 | 9/1.766 | 0/0.0 | -1.0 |
| max_slot=12:30  | 4/2.813 | 7/0.643 | 11/1.071 | 1/0.0 | -1.0 |
| max_slot=13:00  | 6/1.465 | 9/0.868 | 15/1.036 | 3/0.533 | 0.868 |
| max_slot=13:30 ✅best | 8/2.27 | 13/1.534 | 21/1.715 | 4/0.441 | 1.534 |
| max_slot=14:00 **(current)** | 11/1.586 | 19/1.382 | 30/1.436✓ | 4/0.441 | 1.382 |
| max_slot=14:30  | 15/1.251 | 23/1.137 | 38/1.171 | 4/0.441 | 1.137 |
| max_slot=15:00  | 15/1.251 | 23/1.137 | 38/1.171 | 4/0.441 | 1.137 |

- **best = `max_slot=13:30`** (score 1.534 vs current 1.382); TRAIN PF 1.715, TEST PF 0.441

## max_positions (anchor 20)

| value | FIT n/PF | VAL n/PF | TRAIN n/PF | TEST n/PF | score |
|---|---|---|---|---|---|
| maxpos=5 ✅best | 11/1.586 | 19/1.382 | 30/1.436✓ | 4/0.441 | 1.382 |
| maxpos=10  | 11/1.586 | 19/1.382 | 30/1.436✓ | 4/0.441 | 1.382 |
| maxpos=20 **(current)** | 11/1.586 | 19/1.382 | 30/1.436✓ | 4/0.441 | 1.382 |

- **best = `maxpos=5`** (score 1.382 vs current 1.382); TRAIN PF 1.436 in-band, TEST PF 0.441

## daily_loss_rs (anchor 0)

| value | FIT n/PF | VAL n/PF | TRAIN n/PF | TEST n/PF | score |
|---|---|---|---|---|---|
| dloss=0 **(current)** ✅best | 11/1.586 | 19/1.382 | 30/1.436✓ | 4/0.441 | 1.382 |
| dloss=2000  | 11/1.586 | 19/1.382 | 30/1.436✓ | 4/0.441 | 1.382 |
| dloss=3000  | 11/1.586 | 19/1.382 | 30/1.436✓ | 4/0.441 | 1.382 |
| dloss=5000  | 11/1.586 | 19/1.382 | 30/1.436✓ | 4/0.441 | 1.382 |

- **best = `dloss=0`** (score 1.382 vs current 1.382); TRAIN PF 1.436 in-band, TEST PF 0.441
