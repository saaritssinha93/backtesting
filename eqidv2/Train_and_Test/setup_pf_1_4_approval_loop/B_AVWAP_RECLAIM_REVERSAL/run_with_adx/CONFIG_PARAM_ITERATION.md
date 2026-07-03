# CONFIG_PARAM_ITERATION — B_AVWAP_RECLAIM_REVERSAL

One-parameter-at-a-time sweep **anchored on the given config**, net of cost @ 15 bps/leg. Each row changes ONLY that parameter; all others stay at the anchor. 'score' = min(FIT_PF, VAL_PF) (−1 if a fold has < 6 trades). ✓ on TRAIN = inside the [1.3,1.7] band. TEST shown for reference only (never optimised on).

- TRAIN 2026-05-18..2026-06-16 (20) · TEST 2026-06-22..2026-06-24 (2) · entries FIT=680 VAL=753 TRAIN=1433 TEST=123

## TL;DR — anchor = the ADX-added config; the sweep says DROP ADX
This anchor is the 6-term config (anchor + `sig5_adx_calc≥12`). Coordinate sweep result:
- **The ADX term's own best value is `DROP sig5_adx_calc`** (score 1.382 vs 1.351 with it) — the sweep literally recommends removing ADX, i.e. revert to the original 5-term config.
- Every other parameter is at/near its best again; the only in-sample lifts (`target→2.5` score 1.564, `max_slot→13:30` 1.534) **leave TEST at 0.441** (n=4).
- **TEST PF is invariant at 0.441 across every single-parameter change** (same as the no-ADX run). Adding ADX changed nothing out-of-sample. Verdict: do not promote; ADX is not the missing lever.

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
    ],
    [
      "sig5_adx_calc",
      ">=",
      12.0
    ]
  ],
  "entry_guards": {
    "max_slot": "14:00"
  },
  "max_positions": 20,
  "daily_loss_rs": 0.0
}
```
- Anchor metrics: FIT 9/1.351 · VAL 18/1.563 · TRAIN 27/1.501 (net Rs6,088, dayDom 1.215) · TEST 4/0.441 (net Rs-1,526) · score 1.351

## EXIT sl_pct (anchor 0.9)

| value | FIT n/PF | VAL n/PF | TRAIN n/PF | TEST n/PF | score |
|---|---|---|---|---|---|
| SL=0.5  | 11/0.632 | 24/0.883 | 35/0.804 | 6/0.358 | 0.632 |
| SL=0.6  | 11/0.989 | 22/1.33 | 33/1.214 | 6/0.32 | 0.989 |
| SL=0.7  | 11/1.063 | 21/1.306 | 32/1.228 | 6/0.313 | 1.063 |
| SL=0.8  | 9/1.177 | 21/1.313 | 30/1.273 | 6/0.291 | 1.177 |
| SL=0.85  | 9/1.39 | 19/1.273 | 28/1.304✓ | 6/0.28 | 1.273 |
| SL=0.9 **(current)** ✅best | 9/1.351 | 18/1.563 | 27/1.501✓ | 4/0.441 | 1.351 |
| SL=1  | 9/1.278 | 16/1.262 | 25/1.267 | 4/0.411 | 1.262 |
| SL=1.1  | 8/1.049 | 13/1.383 | 21/1.271 | 4/0.385 | 1.049 |
| SL=1.2  | 8/1.015 | 13/1.338 | 21/1.229 | 2/0.0 | 1.015 |
| SL=1.3  | 6/0.722 | 10/1.506 | 16/1.222 | 2/0.0 | 0.722 |
| SL=1.5  | 4/0.258 | 10/1.399 | 14/1.055 | 1/0.0 | -1.0 |

- **best = `SL=0.9`** (score 1.351 vs current 1.351); TRAIN PF 1.501 in-band, TEST PF 0.441

## EXIT tgt_pct (anchor 3.0)

| value | FIT n/PF | VAL n/PF | TRAIN n/PF | TEST n/PF | score |
|---|---|---|---|---|---|
| Tgt=1  | 9/0.873 | 18/0.71 | 27/0.757 | 4/0.281 | 0.71 |
| Tgt=1.25  | 9/1.157 | 18/0.942 | 27/1.004 | 4/0.372 | 0.942 |
| Tgt=1.5  | 9/1.358 | 18/1.049 | 27/1.139 | 4/0.464 | 1.049 |
| Tgt=1.75  | 9/1.319 | 18/1.197 | 27/1.232 | 4/0.555 | 1.197 |
| Tgt=2  | 9/1.422 | 18/1.341 | 27/1.365✓ | 4/0.441 | 1.341 |
| Tgt=2.5 ✅best | 9/1.564 | 18/1.589 | 27/1.582✓ | 4/0.441 | 1.564 |
| Tgt=3 **(current)** | 9/1.351 | 18/1.563 | 27/1.501✓ | 4/0.441 | 1.351 |
| Tgt=3.5  | 9/1.351 | 18/1.621 | 27/1.543✓ | 4/0.441 | 1.351 |

- **best = `Tgt=2.5`** (score 1.564 vs current 1.351); TRAIN PF 1.582 in-band, TEST PF 0.441

## MASK term [vwap_dist_atr<=1]

| value | FIT n/PF | VAL n/PF | TRAIN n/PF | TEST n/PF | score |
|---|---|---|---|---|---|
| vwap_dist_atr<=0.191358  | 1/inf | 1/0.0 | 2/0.554 | 1/inf | -1.0 |
| vwap_dist_atr<=0.300084  | 2/0.555 | 2/0.0 | 4/0.185 | 1/inf | -1.0 |
| vwap_dist_atr<=0.426195  | 5/0.196 | 5/0.427 | 10/0.329 | 1/inf | -1.0 |
| vwap_dist_atr<=0.578625  | 6/0.179 | 10/1.615 | 16/1.044 | 2/2.553 | 0.179 |
| vwap_dist_atr<=0.757981  | 8/0.888 | 14/1.891 | 22/1.537✓ | 4/0.441 | 0.888 |
| vwap_dist_atr<=0.945203 ✅best | 9/1.351 | 18/1.563 | 27/1.501✓ | 4/0.441 | 1.351 |
| vwap_dist_atr<=1 **(current)** | 9/1.351 | 18/1.563 | 27/1.501✓ | 4/0.441 | 1.351 |
| vwap_dist_atr<=1.16624  | 12/0.826 | 24/1.138 | 36/1.037 | 4/0.441 | 0.826 |
| vwap_dist_atr<=1.42458  | 14/0.767 | 31/1.281 | 45/1.12 | 7/1.266 | 0.767 |
| vwap_dist_atr<=1.75598  | 17/0.787 | 36/0.989 | 53/0.928 | 7/1.266 | 0.787 |
| DROP vwap_dist_atr  | 24/0.801 | 48/1.046 | 72/0.971 | 10/0.636 | 0.801 |

- **best = `vwap_dist_atr<=0.945203`** (score 1.351 vs current 1.351); TRAIN PF 1.501 in-band, TEST PF 0.441

## MASK term [vol_ratio>=3.53783]

| value | FIT n/PF | VAL n/PF | TRAIN n/PF | TEST n/PF | score |
|---|---|---|---|---|---|
| vol_ratio>=1.64952  | 27/1.087 | 49/0.869 | 76/0.937 | 13/0.526 | 0.869 |
| vol_ratio>=1.81521  | 25/1.032 | 47/0.933 | 72/0.966 | 13/0.526 | 0.933 |
| vol_ratio>=2.02507  | 25/1.032 | 44/0.96 | 69/0.985 | 12/0.545 | 0.96 |
| vol_ratio>=2.27738  | 22/1.057 | 40/1.025 | 62/1.036 | 10/0.605 | 1.025 |
| vol_ratio>=2.56952  | 17/1.855 | 32/1.202 | 49/1.386✓ | 7/1.214 | 1.202 |
| vol_ratio>=2.98774  | 13/2.18 | 25/1.257 | 38/1.48✓ | 6/1.46 | 1.257 |
| vol_ratio>=3.53783 **(current)** ✅best | 9/1.351 | 18/1.563 | 27/1.501✓ | 4/0.441 | 1.351 |
| vol_ratio>=4.41041  | 6/0.948 | 15/1.861 | 21/1.619✓ | 2/0.0 | 0.948 |
| vol_ratio>=5.61714  | 2/0.856 | 9/1.883 | 11/1.701 | 1/0.0 | -1.0 |
| DROP vol_ratio  | 28/0.996 | 55/0.903 | 83/0.932 | 13/0.526 | 0.903 |

- current value is already best

## MASK term [atr_pct<=0.003921]

| value | FIT n/PF | VAL n/PF | TRAIN n/PF | TEST n/PF | score |
|---|---|---|---|---|---|
| atr_pct<=0.001689  | 0/0.0 | 1/inf | 1/inf | 0/0.0 | -1.0 |
| atr_pct<=0.001926  | 1/0.0 | 4/inf | 5/21.775 | 0/0.0 | -1.0 |
| atr_pct<=0.002151  | 3/0.933 | 5/6.308 | 8/3.65 | 1/0.0 | -1.0 |
| atr_pct<=0.002383  | 3/0.933 | 8/2.098 | 11/1.812 | 2/1.068 | -1.0 |
| atr_pct<=0.002649  | 3/0.933 | 10/2.003 | 13/1.795 | 3/0.753 | -1.0 |
| atr_pct<=0.002919  | 3/0.933 | 14/1.348 | 17/1.292 | 3/0.753 | -1.0 |
| atr_pct<=0.00331  | 5/0.409 | 17/1.243 | 22/1.063 | 3/0.753 | -1.0 |
| atr_pct<=0.003921 **(current)** ✅best | 9/1.351 | 18/1.563 | 27/1.501✓ | 4/0.441 | 1.351 |
| atr_pct<=0.005113  | 10/1.021 | 18/1.563 | 28/1.373✓ | 6/0.241 | 1.021 |
| DROP atr_pct  | 12/1.298 | 20/1.238 | 32/1.259 | 6/0.241 | 1.238 |

- current value is already best

## PRE-MOM term [pre1_adx>=30.6759]

| value | FIT n/PF | VAL n/PF | TRAIN n/PF | TEST n/PF | score |
|---|---|---|---|---|---|
| pre1_adx>=17.564  | 15/1.216 | 47/0.828 | 62/0.903 | 9/0.552 | 0.828 |
| pre1_adx>=20.6027  | 13/1.032 | 42/0.898 | 55/0.925 | 9/0.552 | 0.898 |
| pre1_adx>=23.0378  | 12/0.967 | 38/0.966 | 50/0.966 | 9/0.552 | 0.966 |
| pre1_adx>=25.3821  | 9/1.351 | 27/0.895 | 36/0.978 | 8/0.521 | 0.895 |
| pre1_adx>=27.9015  | 9/1.351 | 23/1.026 | 32/1.095 | 5/0.361 | 1.026 |
| pre1_adx>=30.6759 **(current)** ✅best | 9/1.351 | 18/1.563 | 27/1.501✓ | 4/0.441 | 1.351 |
| pre1_adx>=33.6157  | 7/0.74 | 10/1.943 | 17/1.333✓ | 3/0.751 | 0.74 |
| pre1_adx>=37.4334  | 7/0.74 | 7/0.964 | 14/0.85 | 2/0.0 | 0.74 |
| pre1_adx>=43.3961  | 2/0.0 | 6/0.372 | 8/0.292 | 0/0.0 | -1.0 |
| DROP pre1_adx  | 17/1.222 | 50/0.875 | 67/0.952 | 10/0.459 | 0.875 |

- current value is already best

## PRE-MOM term [pre5_mom_r>=0.317166]

| value | FIT n/PF | VAL n/PF | TRAIN n/PF | TEST n/PF | score |
|---|---|---|---|---|---|
| pre5_mom_r>=-0.150325  | 22/0.916 | 30/0.823 | 52/0.855 | 7/0.247 | 0.823 |
| pre5_mom_r>=-0.021394  | 19/0.929 | 28/0.901 | 47/0.91 | 7/0.247 | 0.901 |
| pre5_mom_r>=0.111319  | 13/0.811 | 27/0.97 | 40/0.925 | 7/0.247 | 0.811 |
| pre5_mom_r>=0.223518  | 11/1.117 | 22/1.437 | 33/1.342✓ | 6/0.271 | 1.117 |
| pre5_mom_r>=0.317166 **(current)** ✅best | 9/1.351 | 18/1.563 | 27/1.501✓ | 4/0.441 | 1.351 |
| pre5_mom_r>=0.4203  | 8/0.888 | 13/1.267 | 21/1.132 | 2/0.0 | 0.888 |
| pre5_mom_r>=0.514042  | 4/0.244 | 10/1.468 | 14/1.071 | 1/0.0 | -1.0 |
| pre5_mom_r>=0.658392  | 4/0.244 | 2/0.0 | 6/0.13 | 0/0.0 | -1.0 |
| pre5_mom_r>=0.903159  | 1/0.0 | 0/0.0 | 1/0.0 | 0/0.0 | -1.0 |
| DROP pre5_mom_r  | 27/0.753 | 30/0.823 | 57/0.793 | 7/0.247 | 0.753 |

- current value is already best

## PRE-MOM term [sig5_adx_calc>=12]

| value | FIT n/PF | VAL n/PF | TRAIN n/PF | TEST n/PF | score |
|---|---|---|---|---|---|
| sig5_adx_calc>=12 **(current)** | 9/1.351 | 18/1.563 | 27/1.501✓ | 4/0.441 | 1.351 |
| sig5_adx_calc>=12.3534  | 9/1.351 | 18/1.563 | 27/1.501✓ | 4/0.441 | 1.351 |
| sig5_adx_calc>=14.853  | 9/1.351 | 15/1.234 | 24/1.271 | 4/0.441 | 1.234 |
| sig5_adx_calc>=16.5331  | 7/1.357 | 12/0.97 | 19/1.073 | 3/0.0 | 0.97 |
| sig5_adx_calc>=18.1111  | 7/1.357 | 12/0.97 | 19/1.073 | 3/0.0 | 0.97 |
| sig5_adx_calc>=20.2498  | 7/1.357 | 9/0.458 | 16/0.699 | 1/0.0 | 0.458 |
| sig5_adx_calc>=22.3463  | 7/1.357 | 9/0.458 | 16/0.699 | 1/0.0 | 0.458 |
| sig5_adx_calc>=24.6429  | 6/1.56 | 8/0.554 | 14/0.834 | 1/0.0 | 0.554 |
| sig5_adx_calc>=27.7712  | 4/1.199 | 3/0.19 | 7/0.736 | 1/0.0 | -1.0 |
| sig5_adx_calc>=32.7251  | 1/0.0 | 1/0.0 | 2/0.0 | 0/0.0 | -1.0 |
| DROP sig5_adx_calc ✅best | 11/1.586 | 19/1.382 | 30/1.436✓ | 4/0.441 | 1.382 |

- **best = `DROP sig5_adx_calc`** (score 1.382 vs current 1.351); TRAIN PF 1.436 in-band, TEST PF 0.441

## GUARD max_slot (anchor 14:00)

| value | FIT n/PF | VAL n/PF | TRAIN n/PF | TEST n/PF | score |
|---|---|---|---|---|---|
| max_slot=None  | 13/1.087 | 22/1.257 | 35/1.203 | 4/0.441 | 1.087 |
| max_slot=11:30  | 1/0.0 | 2/inf | 3/2.616 | 0/0.0 | -1.0 |
| max_slot=12:00  | 3/2.772 | 5/1.262 | 8/1.753 | 0/0.0 | -1.0 |
| max_slot=12:30  | 3/2.772 | 7/0.643 | 10/1.063 | 1/0.0 | -1.0 |
| max_slot=13:00  | 5/1.443 | 9/0.868 | 14/1.03 | 3/0.533 | -1.0 |
| max_slot=13:30 ✅best | 6/1.891 | 13/1.534 | 19/1.622✓ | 4/0.441 | 1.534 |
| max_slot=14:00 **(current)** | 9/1.351 | 18/1.563 | 27/1.501✓ | 4/0.441 | 1.351 |
| max_slot=14:30  | 13/1.087 | 22/1.257 | 35/1.203 | 4/0.441 | 1.087 |
| max_slot=15:00  | 13/1.087 | 22/1.257 | 35/1.203 | 4/0.441 | 1.087 |

- **best = `max_slot=13:30`** (score 1.534 vs current 1.351); TRAIN PF 1.622 in-band, TEST PF 0.441

## max_positions (anchor 20)

| value | FIT n/PF | VAL n/PF | TRAIN n/PF | TEST n/PF | score |
|---|---|---|---|---|---|
| maxpos=5 ✅best | 9/1.351 | 18/1.563 | 27/1.501✓ | 4/0.441 | 1.351 |
| maxpos=10  | 9/1.351 | 18/1.563 | 27/1.501✓ | 4/0.441 | 1.351 |
| maxpos=20 **(current)** | 9/1.351 | 18/1.563 | 27/1.501✓ | 4/0.441 | 1.351 |

- **best = `maxpos=5`** (score 1.351 vs current 1.351); TRAIN PF 1.501 in-band, TEST PF 0.441

## daily_loss_rs (anchor 0)

| value | FIT n/PF | VAL n/PF | TRAIN n/PF | TEST n/PF | score |
|---|---|---|---|---|---|
| dloss=0 **(current)** ✅best | 9/1.351 | 18/1.563 | 27/1.501✓ | 4/0.441 | 1.351 |
| dloss=2000  | 9/1.351 | 18/1.563 | 27/1.501✓ | 4/0.441 | 1.351 |
| dloss=3000  | 9/1.351 | 18/1.563 | 27/1.501✓ | 4/0.441 | 1.351 |
| dloss=5000  | 9/1.351 | 18/1.563 | 27/1.501✓ | 4/0.441 | 1.351 |

- **best = `dloss=0`** (score 1.351 vs current 1.351); TRAIN PF 1.501 in-band, TEST PF 0.441
