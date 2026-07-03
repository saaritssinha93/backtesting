# PARAMETER_SWEEP_SUMMARY — B_AVWAP_RECLAIM_REVERSAL

Stage-2 individual-knob range sweep, net of cost @ 15 bps/leg. Each knob is varied ONE at a time from a clean base (raw detection + card exit SL 0.7/Tgt 1.5); everything else fixed. Optimised on FIT/VAL only. 'Stable' = both folds keep ≥ 6 trades AND min(FIT_PF, VAL_PF) ≥ 1.3.

- FIT 2026-05-18..2026-06-02 (10) · VAL 2026-06-03..2026-06-16 (10) · TRAIN 2026-05-18..2026-06-16 (20)
- entries @ 15bps: FIT=680 VAL=753
- searchable mask=['atr_pct', 'body_pct', 'close_loc', 'lower_wick_pct', 'quality_score', 'ranker_score', 'rs_pct', 'signal_range_pct', 'upper_wick_pct', 'vol_ratio', 'vwap_dist_atr', 'wick_skew_pct'] | premom=['pre1_adx', 'pre3_close_pos', 'pre3_range_r', 'pre5_mom_r', 'pre_entry_momentum_score', 'sig5_adx_calc', 'sig5_rsi_dir', 'sig5_vol_ratio20']

## EXIT — stop-loss % (target fixed at 1.5)
_smaller↔wider SL; reject SL that bleeds VAL_

**best stable range:** _none reaches min(FIT,VAL) PF ≥ 1.3_ ; closest = `SL=1.2` (min-PF 0.357)

| value | FIT n | FIT PF | VAL n | VAL PF | min(FIT,VAL) |
|---|---:|---:|---:|---:|---:|
| SL=0.4 | 502 | 0.235 | 572 | 0.219 | 0.219 |
| SL=0.5 | 425 | 0.269 | 501 | 0.253 | 0.253 |
| SL=0.6 | 378 | 0.314 | 449 | 0.286 | 0.286 |
| SL=0.7 | 344 | 0.356 | 404 | 0.294 | 0.294 |
| SL=0.8 | 325 | 0.361 | 375 | 0.323 | 0.323 |
| SL=0.9 | 300 | 0.374 | 342 | 0.308 | 0.308 |
| SL=1 | 278 | 0.394 | 325 | 0.325 | 0.325 |
| SL=1.1 | 264 | 0.396 | 313 | 0.32 | 0.32 |
| SL=1.2 | 247 | 0.391 | 301 | 0.357 | 0.357 |
| SL=1.3 | 240 | 0.378 | 294 | 0.332 | 0.332 |
| SL=1.5 | 230 | 0.386 | 269 | 0.315 | 0.315 |

## EXIT — target % (SL fixed at 0.7)
_smaller↔larger target; reject too-ambitious targets_

**best stable range:** _none reaches min(FIT,VAL) PF ≥ 1.3_ ; closest = `Tgt=3` (min-PF 0.319)

| value | FIT n | FIT PF | VAL n | VAL PF | min(FIT,VAL) |
|---|---:|---:|---:|---:|---:|
| Tgt=0.6 | 422 | 0.247 | 497 | 0.231 | 0.231 |
| Tgt=0.8 | 399 | 0.307 | 472 | 0.258 | 0.258 |
| Tgt=1 | 377 | 0.327 | 443 | 0.287 | 0.287 |
| Tgt=1.25 | 352 | 0.339 | 422 | 0.297 | 0.297 |
| Tgt=1.5 | 344 | 0.356 | 404 | 0.294 | 0.294 |
| Tgt=1.75 | 334 | 0.365 | 396 | 0.29 | 0.29 |
| Tgt=2 | 326 | 0.366 | 392 | 0.307 | 0.307 |
| Tgt=2.5 | 319 | 0.405 | 382 | 0.308 | 0.308 |
| Tgt=3 | 319 | 0.389 | 379 | 0.319 | 0.319 |

## FILTER (mask) — rs_pct
_indicator/price-action filter; range across train quantiles_

**best stable range:** _none reaches min(FIT,VAL) PF ≥ 1.3_ ; closest = `rs_pct>=0.191639 (q0.2)` (min-PF 0.306)

| value | FIT n | FIT PF | VAL n | VAL PF | min(FIT,VAL) |
|---|---:|---:|---:|---:|---:|
| rs_pct>=0.191639 (q0.2) | 318 | 0.391 | 383 | 0.306 | 0.306 |
| rs_pct>=0.525739 (q0.4) | 274 | 0.406 | 342 | 0.288 | 0.288 |
| rs_pct>=0.700859 (q0.5) | 262 | 0.413 | 294 | 0.277 | 0.277 |
| rs_pct>=0.929255 (q0.6) | 231 | 0.374 | 261 | 0.265 | 0.265 |
| rs_pct>=1.699459 (q0.8) | 130 | 0.336 | 131 | 0.214 | 0.214 |
| rs_pct<=0.191639 (q0.2) | 122 | 0.248 | 145 | 0.379 | 0.248 |
| rs_pct<=0.525739 (q0.4) | 220 | 0.238 | 247 | 0.342 | 0.238 |
| rs_pct<=0.700859 (q0.5) | 246 | 0.26 | 287 | 0.346 | 0.26 |
| rs_pct<=0.929255 (q0.6) | 267 | 0.29 | 309 | 0.312 | 0.29 |
| rs_pct<=1.699459 (q0.8) | 304 | 0.329 | 358 | 0.284 | 0.284 |

## FILTER (mask) — vol_ratio
_indicator/price-action filter; range across train quantiles_

**best stable range:** _none reaches min(FIT,VAL) PF ≥ 1.3_ ; closest = `vol_ratio>=4.410413 (q0.8)` (min-PF 0.334)

| value | FIT n | FIT PF | VAL n | VAL PF | min(FIT,VAL) |
|---|---:|---:|---:|---:|---:|
| vol_ratio>=1.815209 (q0.2) | 319 | 0.372 | 387 | 0.284 | 0.284 |
| vol_ratio>=2.277376 (q0.4) | 294 | 0.401 | 334 | 0.317 | 0.317 |
| vol_ratio>=2.569518 (q0.5) | 263 | 0.354 | 307 | 0.315 | 0.315 |
| vol_ratio>=2.987742 (q0.6) | 232 | 0.32 | 264 | 0.31 | 0.31 |
| vol_ratio>=4.410413 (q0.8) | 136 | 0.334 | 141 | 0.35 | 0.334 |
| vol_ratio<=1.815209 (q0.2) | 114 | 0.189 | 123 | 0.443 | 0.189 |
| vol_ratio<=2.277376 (q0.4) | 208 | 0.211 | 238 | 0.315 | 0.211 |
| vol_ratio<=2.569518 (q0.5) | 247 | 0.293 | 285 | 0.307 | 0.293 |
| vol_ratio<=2.987742 (q0.6) | 270 | 0.323 | 325 | 0.305 | 0.305 |
| vol_ratio<=4.410413 (q0.8) | 311 | 0.311 | 371 | 0.314 | 0.311 |

## FILTER (mask) — atr_pct
_indicator/price-action filter; range across train quantiles_

**best stable range:** _none reaches min(FIT,VAL) PF ≥ 1.3_ ; closest = `atr_pct<=0.003921 (q0.8)` (min-PF 0.32)

| value | FIT n | FIT PF | VAL n | VAL PF | min(FIT,VAL) |
|---|---:|---:|---:|---:|---:|
| atr_pct>=0.001926 (q0.2) | 330 | 0.366 | 419 | 0.299 | 0.299 |
| atr_pct>=0.002383 (q0.4) | 313 | 0.408 | 389 | 0.294 | 0.294 |
| atr_pct>=0.002649 (q0.5) | 280 | 0.423 | 349 | 0.271 | 0.271 |
| atr_pct>=0.002919 (q0.6) | 231 | 0.403 | 279 | 0.255 | 0.255 |
| atr_pct>=0.003921 (q0.8) | 123 | 0.441 | 132 | 0.229 | 0.229 |
| atr_pct<=0.001926 (q0.2) | 125 | 0.169 | 111 | 0.496 | 0.169 |
| atr_pct<=0.002383 (q0.4) | 184 | 0.182 | 187 | 0.37 | 0.182 |
| atr_pct<=0.002649 (q0.5) | 215 | 0.236 | 224 | 0.376 | 0.236 |
| atr_pct<=0.002919 (q0.6) | 239 | 0.307 | 262 | 0.334 | 0.307 |
| atr_pct<=0.003921 (q0.8) | 290 | 0.32 | 333 | 0.321 | 0.32 |

## FILTER (mask) — body_pct
_indicator/price-action filter; range across train quantiles_

**best stable range:** _none reaches min(FIT,VAL) PF ≥ 1.3_ ; closest = `body_pct>=0.75 (q0.4)` (min-PF 0.331)

| value | FIT n | FIT PF | VAL n | VAL PF | min(FIT,VAL) |
|---|---:|---:|---:|---:|---:|
| body_pct>=0.634557 (q0.2) | 319 | 0.334 | 382 | 0.315 | 0.315 |
| body_pct>=0.75 (q0.4) | 289 | 0.344 | 345 | 0.331 | 0.331 |
| body_pct>=0.789474 (q0.5) | 268 | 0.326 | 302 | 0.362 | 0.326 |
| body_pct>=0.836066 (q0.6) | 231 | 0.323 | 264 | 0.36 | 0.323 |
| body_pct>=0.937499 (q0.8) | 118 | 0.279 | 145 | 0.256 | 0.256 |
| body_pct<=0.634557 (q0.2) | 133 | 0.329 | 126 | 0.289 | 0.289 |
| body_pct<=0.75 (q0.4) | 220 | 0.293 | 236 | 0.288 | 0.288 |
| body_pct<=0.789474 (q0.5) | 246 | 0.337 | 281 | 0.263 | 0.263 |
| body_pct<=0.836066 (q0.6) | 269 | 0.325 | 325 | 0.274 | 0.274 |
| body_pct<=0.937499 (q0.8) | 311 | 0.325 | 368 | 0.291 | 0.291 |

## FILTER (mask) — close_loc
_indicator/price-action filter; range across train quantiles_

**best stable range:** _none reaches min(FIT,VAL) PF ≥ 1.3_ ; closest = `close_loc>=0.769231 (q0.2)` (min-PF 0.334)

| value | FIT n | FIT PF | VAL n | VAL PF | min(FIT,VAL) |
|---|---:|---:|---:|---:|---:|
| close_loc>=0.769231 (q0.2) | 323 | 0.352 | 372 | 0.334 | 0.334 |
| close_loc>=0.862696 (q0.4) | 292 | 0.308 | 319 | 0.381 | 0.308 |
| close_loc>=0.903846 (q0.5) | 262 | 0.344 | 287 | 0.333 | 0.333 |
| close_loc>=0.944567 (q0.6) | 223 | 0.318 | 263 | 0.299 | 0.299 |
| close_loc>=1.0 (q0.8) | 174 | 0.337 | 190 | 0.333 | 0.333 |
| close_loc<=0.769231 (q0.2) | 132 | 0.246 | 130 | 0.222 | 0.222 |
| close_loc<=0.862696 (q0.4) | 222 | 0.334 | 255 | 0.281 | 0.281 |
| close_loc<=0.903846 (q0.5) | 257 | 0.328 | 294 | 0.29 | 0.29 |
| close_loc<=0.944567 (q0.6) | 279 | 0.338 | 326 | 0.32 | 0.32 |
| close_loc<=1.0 (q0.8) | 344 | 0.356 | 404 | 0.294 | 0.294 |

## FILTER (mask) — vwap_dist_atr
_indicator/price-action filter; range across train quantiles_

**best stable range:** _none reaches min(FIT,VAL) PF ≥ 1.3_ ; closest = `vwap_dist_atr>=0.757981 (q0.5)` (min-PF 0.326)

| value | FIT n | FIT PF | VAL n | VAL PF | min(FIT,VAL) |
|---|---:|---:|---:|---:|---:|
| vwap_dist_atr>=0.300084 (q0.2) | 326 | 0.338 | 396 | 0.297 | 0.297 |
| vwap_dist_atr>=0.578625 (q0.4) | 292 | 0.352 | 363 | 0.319 | 0.319 |
| vwap_dist_atr>=0.757981 (q0.5) | 261 | 0.326 | 316 | 0.353 | 0.326 |
| vwap_dist_atr>=0.945203 (q0.6) | 232 | 0.296 | 277 | 0.32 | 0.296 |
| vwap_dist_atr>=1.424577 (q0.8) | 131 | 0.351 | 144 | 0.323 | 0.323 |
| vwap_dist_atr<=0.300084 (q0.2) | 124 | 0.284 | 122 | 0.295 | 0.284 |
| vwap_dist_atr<=0.578625 (q0.4) | 225 | 0.284 | 223 | 0.292 | 0.284 |
| vwap_dist_atr<=0.757981 (q0.5) | 250 | 0.33 | 276 | 0.272 | 0.272 |
| vwap_dist_atr<=0.945203 (q0.6) | 276 | 0.372 | 307 | 0.298 | 0.298 |
| vwap_dist_atr<=1.424577 (q0.8) | 310 | 0.333 | 362 | 0.296 | 0.296 |

## FILTER (mask) — quality_score
_indicator/price-action filter; range across train quantiles_

**best stable range:** _none reaches min(FIT,VAL) PF ≥ 1.3_ ; closest = `quality_score<=75.547986 (q0.6)` (min-PF 0.337)

| value | FIT n | FIT PF | VAL n | VAL PF | min(FIT,VAL) |
|---|---:|---:|---:|---:|---:|
| quality_score>=46.240667 (q0.2) | 320 | 0.363 | 400 | 0.268 | 0.268 |
| quality_score>=61.343469 (q0.4) | 308 | 0.354 | 354 | 0.225 | 0.225 |
| quality_score>=67.191524 (q0.5) | 289 | 0.353 | 316 | 0.24 | 0.24 |
| quality_score>=75.547986 (q0.6) | 254 | 0.36 | 267 | 0.245 | 0.245 |
| quality_score>=96.466829 (q0.8) | 130 | 0.465 | 139 | 0.182 | 0.182 |
| quality_score<=46.240667 (q0.2) | 119 | 0.258 | 122 | 0.543 | 0.258 |
| quality_score<=61.343469 (q0.4) | 191 | 0.291 | 201 | 0.523 | 0.291 |
| quality_score<=67.191524 (q0.5) | 220 | 0.299 | 243 | 0.439 | 0.299 |
| quality_score<=75.547986 (q0.6) | 248 | 0.337 | 278 | 0.356 | 0.337 |
| quality_score<=96.466829 (q0.8) | 298 | 0.294 | 349 | 0.318 | 0.294 |

## FILTER (mask) — ranker_score
_indicator/price-action filter; range across train quantiles_

**best stable range:** _none — no value kept >= min_fold trades on both folds_

| value | FIT n | FIT PF | VAL n | VAL PF | min(FIT,VAL) |
|---|---:|---:|---:|---:|---:|
| ranker_score>=0.435086 (q0.2) | 0 | 0.0 | 52 | 0.323 | 0.0 |
| ranker_score>=0.466628 (q0.4) | 0 | 0.0 | 46 | 0.342 | 0.0 |
| ranker_score>=0.485658 (q0.5) | 0 | 0.0 | 40 | 0.357 | 0.0 |
| ranker_score>=0.513578 (q0.6) | 0 | 0.0 | 32 | 0.288 | 0.0 |
| ranker_score>=0.549127 (q0.8) | 0 | 0.0 | 16 | 0.284 | 0.0 |
| ranker_score<=0.435086 (q0.2) | 0 | 0.0 | 15 | 0.647 | 0.0 |
| ranker_score<=0.466628 (q0.4) | 0 | 0.0 | 29 | 0.345 | 0.0 |
| ranker_score<=0.485658 (q0.5) | 0 | 0.0 | 32 | 0.286 | 0.0 |
| ranker_score<=0.513578 (q0.6) | 0 | 0.0 | 37 | 0.401 | 0.0 |
| ranker_score<=0.549127 (q0.8) | 0 | 0.0 | 45 | 0.378 | 0.0 |

## FILTER (mask) — signal_range_pct
_indicator/price-action filter; range across train quantiles_

**best stable range:** _none reaches min(FIT,VAL) PF ≥ 1.3_ ; closest = `signal_range_pct<=0.856704 (q0.8)` (min-PF 0.332)

| value | FIT n | FIT PF | VAL n | VAL PF | min(FIT,VAL) |
|---|---:|---:|---:|---:|---:|
| signal_range_pct>=0.324917 (q0.2) | 340 | 0.368 | 415 | 0.296 | 0.296 |
| signal_range_pct>=0.466781 (q0.4) | 317 | 0.383 | 371 | 0.293 | 0.293 |
| signal_range_pct>=0.543166 (q0.5) | 290 | 0.381 | 329 | 0.3 | 0.3 |
| signal_range_pct>=0.624411 (q0.6) | 236 | 0.407 | 289 | 0.266 | 0.266 |
| signal_range_pct>=0.856704 (q0.8) | 129 | 0.417 | 134 | 0.219 | 0.219 |
| signal_range_pct<=0.324917 (q0.2) | 122 | 0.148 | 119 | 0.257 | 0.148 |
| signal_range_pct<=0.466781 (q0.4) | 186 | 0.223 | 215 | 0.338 | 0.223 |
| signal_range_pct<=0.543166 (q0.5) | 218 | 0.287 | 239 | 0.299 | 0.287 |
| signal_range_pct<=0.624411 (q0.6) | 254 | 0.315 | 265 | 0.357 | 0.315 |
| signal_range_pct<=0.856704 (q0.8) | 298 | 0.332 | 336 | 0.343 | 0.332 |

## FILTER (mask) — upper_wick_pct
_indicator/price-action filter; range across train quantiles_

**best stable range:** _none reaches min(FIT,VAL) PF ≥ 1.3_ ; closest = `upper_wick_pct>=0.047669 (q0.5)` (min-PF 0.337)

| value | FIT n | FIT PF | VAL n | VAL PF | min(FIT,VAL) |
|---|---:|---:|---:|---:|---:|
| upper_wick_pct>=0.0 (q0.2) | 344 | 0.356 | 404 | 0.294 | 0.294 |
| upper_wick_pct>=0.026082 (q0.4) | 281 | 0.345 | 337 | 0.299 | 0.299 |
| upper_wick_pct>=0.047669 (q0.5) | 266 | 0.382 | 307 | 0.337 | 0.337 |
| upper_wick_pct>=0.070681 (q0.6) | 235 | 0.364 | 261 | 0.291 | 0.291 |
| upper_wick_pct>=0.131085 (q0.8) | 125 | 0.453 | 134 | 0.233 | 0.233 |
| upper_wick_pct<=0.0 (q0.2) | 174 | 0.337 | 190 | 0.333 | 0.333 |
| upper_wick_pct<=0.026082 (q0.4) | 221 | 0.308 | 247 | 0.331 | 0.308 |
| upper_wick_pct<=0.047669 (q0.5) | 253 | 0.267 | 278 | 0.297 | 0.267 |
| upper_wick_pct<=0.070681 (q0.6) | 268 | 0.323 | 304 | 0.368 | 0.323 |
| upper_wick_pct<=0.131085 (q0.8) | 308 | 0.325 | 349 | 0.334 | 0.325 |

## FILTER (mask) — lower_wick_pct
_indicator/price-action filter; range across train quantiles_

**best stable range:** _none reaches min(FIT,VAL) PF ≥ 1.3_ ; closest = `lower_wick_pct>=0.044208 (q0.6)` (min-PF 0.364)

| value | FIT n | FIT PF | VAL n | VAL PF | min(FIT,VAL) |
|---|---:|---:|---:|---:|---:|
| lower_wick_pct>=0.0 (q0.2) | 344 | 0.356 | 404 | 0.294 | 0.294 |
| lower_wick_pct>=0.0093 (q0.4) | 284 | 0.354 | 313 | 0.294 | 0.294 |
| lower_wick_pct>=0.026106 (q0.5) | 261 | 0.352 | 284 | 0.324 | 0.324 |
| lower_wick_pct>=0.044208 (q0.6) | 236 | 0.378 | 243 | 0.364 | 0.364 |
| lower_wick_pct>=0.095696 (q0.8) | 126 | 0.377 | 135 | 0.364 | 0.364 |
| lower_wick_pct<=0.0 (q0.2) | 204 | 0.313 | 248 | 0.343 | 0.313 |
| lower_wick_pct<=0.0093 (q0.4) | 220 | 0.326 | 267 | 0.313 | 0.313 |
| lower_wick_pct<=0.026106 (q0.5) | 250 | 0.311 | 300 | 0.278 | 0.278 |
| lower_wick_pct<=0.044208 (q0.6) | 273 | 0.306 | 321 | 0.265 | 0.265 |
| lower_wick_pct<=0.095696 (q0.8) | 315 | 0.35 | 369 | 0.307 | 0.307 |

## FILTER (mask) — wick_skew_pct
_indicator/price-action filter; range across train quantiles_

**best stable range:** _none reaches min(FIT,VAL) PF ≥ 1.3_ ; closest = `wick_skew_pct<=-0.05306 (q0.2)` (min-PF 0.384)

| value | FIT n | FIT PF | VAL n | VAL PF | min(FIT,VAL) |
|---|---:|---:|---:|---:|---:|
| wick_skew_pct>=-0.05306 (q0.2) | 322 | 0.342 | 377 | 0.281 | 0.281 |
| wick_skew_pct>=0.0 (q0.4) | 296 | 0.34 | 350 | 0.267 | 0.267 |
| wick_skew_pct>=0.009294 (q0.5) | 263 | 0.344 | 310 | 0.237 | 0.237 |
| wick_skew_pct>=0.03122 (q0.6) | 236 | 0.376 | 258 | 0.263 | 0.263 |
| wick_skew_pct>=0.098136 (q0.8) | 125 | 0.38 | 135 | 0.275 | 0.275 |
| wick_skew_pct<=-0.05306 (q0.2) | 127 | 0.384 | 131 | 0.449 | 0.384 |
| wick_skew_pct<=0.0 (q0.4) | 250 | 0.334 | 270 | 0.397 | 0.334 |
| wick_skew_pct<=0.009294 (q0.5) | 257 | 0.332 | 276 | 0.402 | 0.332 |
| wick_skew_pct<=0.03122 (q0.6) | 270 | 0.313 | 307 | 0.355 | 0.313 |
| wick_skew_pct<=0.098136 (q0.8) | 313 | 0.35 | 361 | 0.31 | 0.31 |

## PRE-MOMENTUM — pre_entry_momentum_score
_1-min pre-entry confirmation; range across train quantiles_

**best stable range:** _none reaches min(FIT,VAL) PF ≥ 1.3_ ; closest = `pre_entry_momentum_score>=69.458106 (q0.6)` (min-PF 0.36)

| value | FIT n | FIT PF | VAL n | VAL PF | min(FIT,VAL) |
|---|---:|---:|---:|---:|---:|
| pre_entry_momentum_score>=47.311598 (q0.2) | 307 | 0.342 | 390 | 0.31 | 0.31 |
| pre_entry_momentum_score>=60.604868 (q0.4) | 241 | 0.469 | 377 | 0.325 | 0.325 |
| pre_entry_momentum_score>=65.12592 (q0.5) | 212 | 0.533 | 366 | 0.329 | 0.329 |
| pre_entry_momentum_score>=69.458106 (q0.6) | 181 | 0.542 | 346 | 0.36 | 0.36 |
| pre_entry_momentum_score>=77.763079 (q0.8) | 111 | 0.474 | 250 | 0.326 | 0.326 |
| pre_entry_momentum_score<=47.311598 (q0.2) | 184 | 0.23 | 38 | 0.167 | 0.167 |
| pre_entry_momentum_score<=60.604868 (q0.4) | 228 | 0.212 | 132 | 0.186 | 0.186 |
| pre_entry_momentum_score<=65.12592 (q0.5) | 241 | 0.247 | 190 | 0.219 | 0.219 |
| pre_entry_momentum_score<=69.458106 (q0.6) | 268 | 0.286 | 245 | 0.281 | 0.281 |
| pre_entry_momentum_score<=77.763079 (q0.8) | 307 | 0.346 | 314 | 0.287 | 0.287 |

## PRE-MOMENTUM — sig5_adx_calc
_1-min pre-entry confirmation; range across train quantiles_

**best stable range:** _none reaches min(FIT,VAL) PF ≥ 1.3_ ; closest = `sig5_adx_calc<=20.249848 (q0.5)` (min-PF 0.367)

| value | FIT n | FIT PF | VAL n | VAL PF | min(FIT,VAL) |
|---|---:|---:|---:|---:|---:|
| sig5_adx_calc>=14.853013 (q0.2) | 337 | 0.346 | 392 | 0.277 | 0.277 |
| sig5_adx_calc>=18.111066 (q0.4) | 301 | 0.292 | 349 | 0.315 | 0.292 |
| sig5_adx_calc>=20.249848 (q0.5) | 286 | 0.262 | 316 | 0.288 | 0.262 |
| sig5_adx_calc>=22.346297 (q0.6) | 249 | 0.253 | 270 | 0.273 | 0.253 |
| sig5_adx_calc>=27.771238 (q0.8) | 127 | 0.227 | 149 | 0.206 | 0.206 |
| sig5_adx_calc<=14.853013 (q0.2) | 130 | 0.307 | 135 | 0.261 | 0.261 |
| sig5_adx_calc<=18.111066 (q0.4) | 217 | 0.419 | 250 | 0.322 | 0.322 |
| sig5_adx_calc<=20.249848 (q0.5) | 242 | 0.42 | 274 | 0.367 | 0.367 |
| sig5_adx_calc<=22.346297 (q0.6) | 263 | 0.418 | 298 | 0.354 | 0.354 |
| sig5_adx_calc<=27.771238 (q0.8) | 301 | 0.378 | 354 | 0.352 | 0.352 |

## PRE-MOMENTUM — sig5_rsi_dir
_1-min pre-entry confirmation; range across train quantiles_

**best stable range:** _none reaches min(FIT,VAL) PF ≥ 1.3_ ; closest = `sig5_rsi_dir>=54.406233 (q0.5)` (min-PF 0.363)

| value | FIT n | FIT PF | VAL n | VAL PF | min(FIT,VAL) |
|---|---:|---:|---:|---:|---:|
| sig5_rsi_dir>=47.400763 (q0.2) | 346 | 0.362 | 350 | 0.347 | 0.347 |
| sig5_rsi_dir>=52.375991 (q0.4) | 320 | 0.355 | 266 | 0.396 | 0.355 |
| sig5_rsi_dir>=54.406233 (q0.5) | 295 | 0.363 | 239 | 0.443 | 0.363 |
| sig5_rsi_dir>=56.443646 (q0.6) | 271 | 0.346 | 197 | 0.496 | 0.346 |
| sig5_rsi_dir>=60.4835 (q0.8) | 165 | 0.347 | 107 | 0.463 | 0.347 |
| sig5_rsi_dir<=47.400763 (q0.2) | 68 | 0.366 | 187 | 0.188 | 0.188 |
| sig5_rsi_dir<=52.375991 (q0.4) | 157 | 0.279 | 305 | 0.249 | 0.249 |
| sig5_rsi_dir<=54.406233 (q0.5) | 204 | 0.3 | 329 | 0.254 | 0.254 |
| sig5_rsi_dir<=56.443646 (q0.6) | 245 | 0.34 | 352 | 0.248 | 0.248 |
| sig5_rsi_dir<=60.4835 (q0.8) | 296 | 0.38 | 386 | 0.299 | 0.299 |

## PRE-MOMENTUM — sig5_vol_ratio20
_1-min pre-entry confirmation; range across train quantiles_

**best stable range:** _none reaches min(FIT,VAL) PF ≥ 1.3_ ; closest = `sig5_vol_ratio20>=3.593955 (q0.8)` (min-PF 0.392)

| value | FIT n | FIT PF | VAL n | VAL PF | min(FIT,VAL) |
|---|---:|---:|---:|---:|---:|
| sig5_vol_ratio20>=0.617167 (q0.2) | 331 | 0.367 | 318 | 0.341 | 0.341 |
| sig5_vol_ratio20>=1.608719 (q0.4) | 316 | 0.361 | 264 | 0.324 | 0.324 |
| sig5_vol_ratio20>=1.865448 (q0.5) | 291 | 0.414 | 236 | 0.305 | 0.305 |
| sig5_vol_ratio20>=2.237395 (q0.6) | 263 | 0.414 | 203 | 0.311 | 0.311 |
| sig5_vol_ratio20>=3.593955 (q0.8) | 170 | 0.392 | 101 | 0.392 | 0.392 |
| sig5_vol_ratio20<=0.617167 (q0.2) | 48 | 0.239 | 209 | 0.243 | 0.239 |
| sig5_vol_ratio20<=1.608719 (q0.4) | 127 | 0.324 | 314 | 0.303 | 0.303 |
| sig5_vol_ratio20<=1.865448 (q0.5) | 194 | 0.233 | 336 | 0.342 | 0.233 |
| sig5_vol_ratio20<=2.237395 (q0.6) | 251 | 0.25 | 357 | 0.323 | 0.25 |
| sig5_vol_ratio20<=3.593955 (q0.8) | 304 | 0.341 | 390 | 0.311 | 0.311 |

## PRE-MOMENTUM — pre1_adx
_1-min pre-entry confirmation; range across train quantiles_

**best stable range:** _none reaches min(FIT,VAL) PF ≥ 1.3_ ; closest = `pre1_adx>=37.433429 (q0.8)` (min-PF 0.417)

| value | FIT n | FIT PF | VAL n | VAL PF | min(FIT,VAL) |
|---|---:|---:|---:|---:|---:|
| pre1_adx>=20.602746 (q0.2) | 330 | 0.367 | 390 | 0.29 | 0.29 |
| pre1_adx>=25.38209 (q0.4) | 294 | 0.354 | 352 | 0.301 | 0.301 |
| pre1_adx>=27.901485 (q0.5) | 269 | 0.374 | 314 | 0.289 | 0.289 |
| pre1_adx>=30.675856 (q0.6) | 238 | 0.389 | 266 | 0.379 | 0.379 |
| pre1_adx>=37.433429 (q0.8) | 133 | 0.417 | 144 | 0.474 | 0.417 |
| pre1_adx<=20.602746 (q0.2) | 132 | 0.205 | 136 | 0.311 | 0.205 |
| pre1_adx<=25.38209 (q0.4) | 226 | 0.276 | 239 | 0.289 | 0.276 |
| pre1_adx<=27.901485 (q0.5) | 255 | 0.282 | 283 | 0.298 | 0.282 |
| pre1_adx<=30.675856 (q0.6) | 283 | 0.315 | 322 | 0.253 | 0.253 |
| pre1_adx<=37.433429 (q0.8) | 316 | 0.376 | 370 | 0.3 | 0.3 |

## PRE-MOMENTUM — pre3_range_r
_1-min pre-entry confirmation; range across train quantiles_

**best stable range:** _none reaches min(FIT,VAL) PF ≥ 1.3_ ; closest = `pre3_range_r>=0.383276 (q0.6)` (min-PF 0.325)

| value | FIT n | FIT PF | VAL n | VAL PF | min(FIT,VAL) |
|---|---:|---:|---:|---:|---:|
| pre3_range_r>=0.158834 (q0.2) | 338 | 0.365 | 399 | 0.304 | 0.304 |
| pre3_range_r>=0.247625 (q0.4) | 321 | 0.385 | 397 | 0.309 | 0.309 |
| pre3_range_r>=0.302798 (q0.5) | 285 | 0.413 | 389 | 0.306 | 0.306 |
| pre3_range_r>=0.383276 (q0.6) | 249 | 0.44 | 354 | 0.325 | 0.325 |
| pre3_range_r>=0.609938 (q0.8) | 152 | 0.48 | 263 | 0.275 | 0.275 |
| pre3_range_r<=0.158834 (q0.2) | 107 | 0.108 | 50 | 0.212 | 0.108 |
| pre3_range_r<=0.247625 (q0.4) | 180 | 0.227 | 139 | 0.23 | 0.227 |
| pre3_range_r<=0.302798 (q0.5) | 211 | 0.262 | 184 | 0.275 | 0.262 |
| pre3_range_r<=0.383276 (q0.6) | 233 | 0.273 | 237 | 0.307 | 0.273 |
| pre3_range_r<=0.609938 (q0.8) | 286 | 0.324 | 301 | 0.333 | 0.324 |

## PRE-MOMENTUM — pre5_mom_r
_1-min pre-entry confirmation; range across train quantiles_

**best stable range:** _none reaches min(FIT,VAL) PF ≥ 1.3_ ; closest = `pre5_mom_r>=0.658392 (q0.8)` (min-PF 0.332)

| value | FIT n | FIT PF | VAL n | VAL PF | min(FIT,VAL) |
|---|---:|---:|---:|---:|---:|
| pre5_mom_r>=-0.021394 (q0.2) | 308 | 0.381 | 388 | 0.31 | 0.31 |
| pre5_mom_r>=0.223518 (q0.4) | 232 | 0.472 | 391 | 0.318 | 0.318 |
| pre5_mom_r>=0.317166 (q0.5) | 208 | 0.508 | 389 | 0.319 | 0.319 |
| pre5_mom_r>=0.4203 (q0.6) | 187 | 0.521 | 392 | 0.316 | 0.316 |
| pre5_mom_r>=0.658392 (q0.8) | 125 | 0.5 | 283 | 0.332 | 0.332 |
| pre5_mom_r<=-0.021394 (q0.2) | 184 | 0.179 | 35 | 0.07 | 0.07 |
| pre5_mom_r<=0.223518 (q0.4) | 227 | 0.2 | 98 | 0.178 | 0.178 |
| pre5_mom_r<=0.317166 (q0.5) | 239 | 0.213 | 150 | 0.197 | 0.197 |
| pre5_mom_r<=0.4203 (q0.6) | 258 | 0.24 | 199 | 0.308 | 0.24 |
| pre5_mom_r<=0.658392 (q0.8) | 284 | 0.311 | 285 | 0.313 | 0.311 |

## PRE-MOMENTUM — pre3_close_pos
_1-min pre-entry confirmation; range across train quantiles_

**best stable range:** _none reaches min(FIT,VAL) PF ≥ 1.3_ ; closest = `pre3_close_pos>=0.866881 (q0.6)` (min-PF 0.344)

| value | FIT n | FIT PF | VAL n | VAL PF | min(FIT,VAL) |
|---|---:|---:|---:|---:|---:|
| pre3_close_pos>=0.341922 (q0.2) | 317 | 0.344 | 395 | 0.303 | 0.303 |
| pre3_close_pos>=0.689648 (q0.4) | 274 | 0.36 | 352 | 0.338 | 0.338 |
| pre3_close_pos>=0.786414 (q0.5) | 242 | 0.354 | 313 | 0.342 | 0.342 |
| pre3_close_pos>=0.866881 (q0.6) | 200 | 0.347 | 288 | 0.344 | 0.344 |
| pre3_close_pos>=1.0 (q0.8) | 136 | 0.367 | 193 | 0.341 | 0.341 |
| pre3_close_pos<=0.341922 (q0.2) | 173 | 0.25 | 77 | 0.221 | 0.221 |
| pre3_close_pos<=0.689648 (q0.4) | 240 | 0.333 | 201 | 0.286 | 0.286 |
| pre3_close_pos<=0.786414 (q0.5) | 263 | 0.342 | 273 | 0.259 | 0.259 |
| pre3_close_pos<=0.866881 (q0.6) | 285 | 0.358 | 312 | 0.288 | 0.288 |
| pre3_close_pos<=1.0 (q0.8) | 344 | 0.356 | 404 | 0.294 | 0.294 |

## FILTER — regime (categorical)
_don't-fight-the-tape regime filter_

**best stable range:** _none reaches min(FIT,VAL) PF ≥ 1.3_ ; closest = `regime==NEUTRAL` (min-PF 0.396)

| value | FIT n | FIT PF | VAL n | VAL PF | min(FIT,VAL) |
|---|---:|---:|---:|---:|---:|
| regime==NEUTRAL | 215 | 0.396 | 238 | 0.491 | 0.396 |
| regime==TREND | 0 | 0.0 | 9 | 0.0 | 0.0 |
| regime!=BEAR | 344 | 0.356 | 404 | 0.294 | 0.294 |
| regime!=BULL | 215 | 0.396 | 247 | 0.465 | 0.396 |

## GUARD — min_slot (entry not before)
_avoid early-session traps_

**best stable range:** _none reaches min(FIT,VAL) PF ≥ 1.3_ ; closest = `min_slot=11:00` (min-PF 0.298)

| value | FIT n | FIT PF | VAL n | VAL PF | min(FIT,VAL) |
|---|---:|---:|---:|---:|---:|
| min_slot=09:30 | 344 | 0.356 | 404 | 0.294 | 0.294 |
| min_slot=09:45 | 344 | 0.356 | 404 | 0.294 | 0.294 |
| min_slot=10:00 | 344 | 0.356 | 404 | 0.294 | 0.294 |
| min_slot=10:30 | 344 | 0.356 | 404 | 0.294 | 0.294 |
| min_slot=11:00 | 340 | 0.347 | 398 | 0.298 | 0.298 |

## GUARD — max_slot (entry not after)
_avoid late-day low-quality entries_

**best stable range:** _none reaches min(FIT,VAL) PF ≥ 1.3_ ; closest = `max_slot=14:00` (min-PF 0.304)

| value | FIT n | FIT PF | VAL n | VAL PF | min(FIT,VAL) |
|---|---:|---:|---:|---:|---:|
| max_slot=12:00 | 175 | 0.313 | 195 | 0.182 | 0.182 |
| max_slot=12:30 | 236 | 0.335 | 232 | 0.159 | 0.159 |
| max_slot=13:00 | 273 | 0.361 | 285 | 0.252 | 0.252 |
| max_slot=14:00 | 317 | 0.375 | 368 | 0.304 | 0.304 |
| max_slot=14:30 | 344 | 0.356 | 404 | 0.294 | 0.294 |

## GUARD — top_n (best N per slot by vwap_dist_atr)
_selectivity per signal slot_

**best stable range:** _none reaches min(FIT,VAL) PF ≥ 1.3_ ; closest = `top_n=2` (min-PF 0.221)

| value | FIT n | FIT PF | VAL n | VAL PF | min(FIT,VAL) |
|---|---:|---:|---:|---:|---:|
| top_n=1 | 259 | 0.34 | 239 | 0.202 | 0.202 |
| top_n=2 | 323 | 0.346 | 340 | 0.221 | 0.221 |
| top_n=3 | 340 | 0.35 | 376 | 0.221 | 0.221 |
