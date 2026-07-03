# PARAMETER_SWEEP_SUMMARY — B_AVWAP_RECLAIM_REVERSAL

Stage-2 individual-knob range sweep, net of cost @ 5 bps/leg. Each knob is varied ONE at a time from a clean base (raw detection + card exit SL 0.7/Tgt 1.5); everything else fixed. Optimised on FIT/VAL only. 'Stable' = both folds keep ≥ 6 trades AND min(FIT_PF, VAL_PF) ≥ 1.3.

- FIT 2026-05-18..2026-06-02 (10) · VAL 2026-06-03..2026-06-16 (10) · TRAIN 2026-05-18..2026-06-16 (20)
- entries @ 5bps: FIT=680 VAL=753
- searchable mask=['atr_pct', 'body_pct', 'close_loc', 'lower_wick_pct', 'quality_score', 'ranker_score', 'rs_pct', 'signal_range_pct', 'upper_wick_pct', 'vol_ratio', 'vwap_dist_atr', 'wick_skew_pct'] | premom=['pre1_adx', 'pre3_close_pos', 'pre3_range_r', 'pre5_mom_r', 'pre_entry_momentum_score', 'sig5_adx_calc', 'sig5_rsi_dir', 'sig5_vol_ratio20']

## EXIT — stop-loss % (target fixed at 1.5)
_smaller↔wider SL; reject SL that bleeds VAL_

**best stable range:** _none reaches min(FIT,VAL) PF ≥ 1.3_ ; closest = `SL=1.1` (min-PF 0.522)

| value | FIT n | FIT PF | VAL n | VAL PF | min(FIT,VAL) |
|---|---:|---:|---:|---:|---:|
| SL=0.4 | 432 | 0.461 | 507 | 0.433 | 0.433 |
| SL=0.5 | 386 | 0.532 | 456 | 0.456 | 0.456 |
| SL=0.6 | 346 | 0.592 | 412 | 0.464 | 0.464 |
| SL=0.7 | 329 | 0.559 | 377 | 0.489 | 0.489 |
| SL=0.8 | 306 | 0.583 | 345 | 0.465 | 0.465 |
| SL=0.9 | 281 | 0.576 | 327 | 0.489 | 0.489 |
| SL=1 | 264 | 0.58 | 315 | 0.478 | 0.478 |
| SL=1.1 | 250 | 0.6 | 306 | 0.522 | 0.522 |
| SL=1.2 | 244 | 0.569 | 299 | 0.471 | 0.471 |
| SL=1.3 | 235 | 0.585 | 287 | 0.457 | 0.457 |
| SL=1.5 | 229 | 0.57 | 263 | 0.482 | 0.482 |

## EXIT — target % (SL fixed at 0.7)
_smaller↔larger target; reject too-ambitious targets_

**best stable range:** _none reaches min(FIT,VAL) PF ≥ 1.3_ ; closest = `Tgt=1.25` (min-PF 0.543)

| value | FIT n | FIT PF | VAL n | VAL PF | min(FIT,VAL) |
|---|---:|---:|---:|---:|---:|
| Tgt=0.6 | 410 | 0.446 | 486 | 0.446 | 0.446 |
| Tgt=0.8 | 384 | 0.495 | 447 | 0.529 | 0.495 |
| Tgt=1 | 361 | 0.559 | 424 | 0.521 | 0.521 |
| Tgt=1.25 | 344 | 0.578 | 397 | 0.543 | 0.543 |
| Tgt=1.5 | 329 | 0.559 | 377 | 0.489 | 0.489 |
| Tgt=1.75 | 315 | 0.545 | 367 | 0.534 | 0.534 |
| Tgt=2 | 305 | 0.575 | 359 | 0.529 | 0.529 |
| Tgt=2.5 | 301 | 0.643 | 349 | 0.488 | 0.488 |
| Tgt=3 | 295 | 0.631 | 348 | 0.496 | 0.496 |

## FILTER (mask) — rs_pct
_indicator/price-action filter; range across train quantiles_

**best stable range:** _none reaches min(FIT,VAL) PF ≥ 1.3_ ; closest = `rs_pct>=0.525739 (q0.4)` (min-PF 0.527)

| value | FIT n | FIT PF | VAL n | VAL PF | min(FIT,VAL) |
|---|---:|---:|---:|---:|---:|
| rs_pct>=0.191639 (q0.2) | 304 | 0.628 | 368 | 0.507 | 0.507 |
| rs_pct>=0.525739 (q0.4) | 268 | 0.635 | 331 | 0.527 | 0.527 |
| rs_pct>=0.700859 (q0.5) | 253 | 0.639 | 285 | 0.514 | 0.514 |
| rs_pct>=0.929255 (q0.6) | 226 | 0.596 | 252 | 0.492 | 0.492 |
| rs_pct>=1.699459 (q0.8) | 130 | 0.496 | 131 | 0.432 | 0.432 |
| rs_pct<=0.191639 (q0.2) | 122 | 0.44 | 145 | 0.705 | 0.44 |
| rs_pct<=0.525739 (q0.4) | 217 | 0.446 | 238 | 0.609 | 0.446 |
| rs_pct<=0.700859 (q0.5) | 241 | 0.485 | 278 | 0.586 | 0.485 |
| rs_pct<=0.929255 (q0.6) | 259 | 0.503 | 297 | 0.509 | 0.503 |
| rs_pct<=1.699459 (q0.8) | 293 | 0.55 | 334 | 0.499 | 0.499 |

## FILTER (mask) — vol_ratio
_indicator/price-action filter; range across train quantiles_

**best stable range:** _none reaches min(FIT,VAL) PF ≥ 1.3_ ; closest = `vol_ratio>=2.277376 (q0.4)` (min-PF 0.547)

| value | FIT n | FIT PF | VAL n | VAL PF | min(FIT,VAL) |
|---|---:|---:|---:|---:|---:|
| vol_ratio>=1.815209 (q0.2) | 305 | 0.611 | 367 | 0.488 | 0.488 |
| vol_ratio>=2.277376 (q0.4) | 283 | 0.642 | 325 | 0.547 | 0.547 |
| vol_ratio>=2.569518 (q0.5) | 257 | 0.575 | 298 | 0.525 | 0.525 |
| vol_ratio>=2.987742 (q0.6) | 228 | 0.521 | 256 | 0.521 | 0.521 |
| vol_ratio>=4.410413 (q0.8) | 135 | 0.584 | 141 | 0.534 | 0.534 |
| vol_ratio<=1.815209 (q0.2) | 114 | 0.346 | 123 | 0.779 | 0.346 |
| vol_ratio<=2.277376 (q0.4) | 206 | 0.393 | 233 | 0.557 | 0.393 |
| vol_ratio<=2.569518 (q0.5) | 244 | 0.52 | 269 | 0.601 | 0.52 |
| vol_ratio<=2.987742 (q0.6) | 263 | 0.518 | 303 | 0.586 | 0.518 |
| vol_ratio<=4.410413 (q0.8) | 294 | 0.502 | 342 | 0.565 | 0.502 |

## FILTER (mask) — atr_pct
_indicator/price-action filter; range across train quantiles_

**best stable range:** _none reaches min(FIT,VAL) PF ≥ 1.3_ ; closest = `atr_pct<=0.003921 (q0.8)` (min-PF 0.538)

| value | FIT n | FIT PF | VAL n | VAL PF | min(FIT,VAL) |
|---|---:|---:|---:|---:|---:|
| atr_pct>=0.001926 (q0.2) | 323 | 0.557 | 391 | 0.502 | 0.502 |
| atr_pct>=0.002383 (q0.4) | 308 | 0.611 | 376 | 0.505 | 0.505 |
| atr_pct>=0.002649 (q0.5) | 274 | 0.633 | 339 | 0.451 | 0.451 |
| atr_pct>=0.002919 (q0.6) | 229 | 0.598 | 274 | 0.452 | 0.452 |
| atr_pct>=0.003921 (q0.8) | 123 | 0.634 | 132 | 0.428 | 0.428 |
| atr_pct<=0.001926 (q0.2) | 123 | 0.311 | 110 | 0.851 | 0.311 |
| atr_pct<=0.002383 (q0.4) | 177 | 0.346 | 182 | 0.662 | 0.346 |
| atr_pct<=0.002649 (q0.5) | 207 | 0.437 | 218 | 0.665 | 0.437 |
| atr_pct<=0.002919 (q0.6) | 231 | 0.513 | 251 | 0.572 | 0.513 |
| atr_pct<=0.003921 (q0.8) | 274 | 0.553 | 311 | 0.538 | 0.538 |

## FILTER (mask) — body_pct
_indicator/price-action filter; range across train quantiles_

**best stable range:** _none reaches min(FIT,VAL) PF ≥ 1.3_ ; closest = `body_pct>=0.789474 (q0.5)` (min-PF 0.554)

| value | FIT n | FIT PF | VAL n | VAL PF | min(FIT,VAL) |
|---|---:|---:|---:|---:|---:|
| body_pct>=0.634557 (q0.2) | 304 | 0.536 | 359 | 0.529 | 0.529 |
| body_pct>=0.75 (q0.4) | 278 | 0.614 | 324 | 0.551 | 0.551 |
| body_pct>=0.789474 (q0.5) | 260 | 0.554 | 293 | 0.587 | 0.554 |
| body_pct>=0.836066 (q0.6) | 228 | 0.549 | 253 | 0.609 | 0.549 |
| body_pct>=0.937499 (q0.8) | 118 | 0.479 | 145 | 0.457 | 0.457 |
| body_pct<=0.634557 (q0.2) | 133 | 0.545 | 125 | 0.513 | 0.513 |
| body_pct<=0.75 (q0.4) | 210 | 0.51 | 230 | 0.494 | 0.494 |
| body_pct<=0.789474 (q0.5) | 233 | 0.58 | 270 | 0.454 | 0.454 |
| body_pct<=0.836066 (q0.6) | 257 | 0.559 | 312 | 0.486 | 0.486 |
| body_pct<=0.937499 (q0.8) | 295 | 0.544 | 352 | 0.507 | 0.507 |

## FILTER (mask) — close_loc
_indicator/price-action filter; range across train quantiles_

**best stable range:** _none reaches min(FIT,VAL) PF ≥ 1.3_ ; closest = `close_loc>=0.903846 (q0.5)` (min-PF 0.571)

| value | FIT n | FIT PF | VAL n | VAL PF | min(FIT,VAL) |
|---|---:|---:|---:|---:|---:|
| close_loc>=0.769231 (q0.2) | 305 | 0.569 | 348 | 0.546 | 0.546 |
| close_loc>=0.862696 (q0.4) | 281 | 0.515 | 298 | 0.631 | 0.515 |
| close_loc>=0.903846 (q0.5) | 255 | 0.576 | 275 | 0.571 | 0.571 |
| close_loc>=0.944567 (q0.6) | 218 | 0.564 | 251 | 0.529 | 0.529 |
| close_loc>=1.0 (q0.8) | 174 | 0.559 | 186 | 0.582 | 0.559 |
| close_loc<=0.769231 (q0.2) | 132 | 0.411 | 130 | 0.469 | 0.411 |
| close_loc<=0.862696 (q0.4) | 216 | 0.568 | 248 | 0.487 | 0.487 |
| close_loc<=0.903846 (q0.5) | 242 | 0.535 | 287 | 0.5 | 0.5 |
| close_loc<=0.944567 (q0.6) | 272 | 0.541 | 314 | 0.533 | 0.533 |
| close_loc<=1.0 (q0.8) | 329 | 0.559 | 377 | 0.489 | 0.489 |

## FILTER (mask) — vwap_dist_atr
_indicator/price-action filter; range across train quantiles_

**best stable range:** _none reaches min(FIT,VAL) PF ≥ 1.3_ ; closest = `vwap_dist_atr>=0.757981 (q0.5)` (min-PF 0.561)

| value | FIT n | FIT PF | VAL n | VAL PF | min(FIT,VAL) |
|---|---:|---:|---:|---:|---:|
| vwap_dist_atr>=0.300084 (q0.2) | 310 | 0.527 | 368 | 0.546 | 0.527 |
| vwap_dist_atr>=0.578625 (q0.4) | 282 | 0.557 | 346 | 0.528 | 0.528 |
| vwap_dist_atr>=0.757981 (q0.5) | 253 | 0.561 | 310 | 0.607 | 0.561 |
| vwap_dist_atr>=0.945203 (q0.6) | 229 | 0.537 | 270 | 0.539 | 0.537 |
| vwap_dist_atr>=1.424577 (q0.8) | 131 | 0.65 | 144 | 0.521 | 0.521 |
| vwap_dist_atr<=0.300084 (q0.2) | 124 | 0.44 | 120 | 0.473 | 0.44 |
| vwap_dist_atr<=0.578625 (q0.4) | 216 | 0.488 | 215 | 0.488 | 0.488 |
| vwap_dist_atr<=0.757981 (q0.5) | 243 | 0.548 | 264 | 0.479 | 0.479 |
| vwap_dist_atr<=0.945203 (q0.6) | 265 | 0.558 | 296 | 0.524 | 0.524 |
| vwap_dist_atr<=1.424577 (q0.8) | 297 | 0.502 | 342 | 0.517 | 0.502 |

## FILTER (mask) — quality_score
_indicator/price-action filter; range across train quantiles_

**best stable range:** _none reaches min(FIT,VAL) PF ≥ 1.3_ ; closest = `quality_score<=75.547986 (q0.6)` (min-PF 0.548)

| value | FIT n | FIT PF | VAL n | VAL PF | min(FIT,VAL) |
|---|---:|---:|---:|---:|---:|
| quality_score>=46.240667 (q0.2) | 305 | 0.608 | 376 | 0.472 | 0.472 |
| quality_score>=61.343469 (q0.4) | 294 | 0.545 | 338 | 0.399 | 0.399 |
| quality_score>=67.191524 (q0.5) | 278 | 0.561 | 304 | 0.379 | 0.379 |
| quality_score>=75.547986 (q0.6) | 247 | 0.537 | 264 | 0.433 | 0.433 |
| quality_score>=96.466829 (q0.8) | 130 | 0.704 | 139 | 0.387 | 0.387 |
| quality_score<=46.240667 (q0.2) | 118 | 0.505 | 119 | 0.998 | 0.505 |
| quality_score<=61.343469 (q0.4) | 189 | 0.513 | 190 | 0.903 | 0.513 |
| quality_score<=67.191524 (q0.5) | 216 | 0.524 | 227 | 0.765 | 0.524 |
| quality_score<=75.547986 (q0.6) | 239 | 0.548 | 263 | 0.622 | 0.548 |
| quality_score<=96.466829 (q0.8) | 284 | 0.512 | 326 | 0.543 | 0.512 |

## FILTER (mask) — ranker_score
_indicator/price-action filter; range across train quantiles_

**best stable range:** _none — no value kept >= min_fold trades on both folds_

| value | FIT n | FIT PF | VAL n | VAL PF | min(FIT,VAL) |
|---|---:|---:|---:|---:|---:|
| ranker_score>=0.435086 (q0.2) | 0 | 0.0 | 45 | 0.588 | 0.0 |
| ranker_score>=0.466628 (q0.4) | 0 | 0.0 | 42 | 0.779 | 0.0 |
| ranker_score>=0.485658 (q0.5) | 0 | 0.0 | 40 | 0.761 | 0.0 |
| ranker_score>=0.513578 (q0.6) | 0 | 0.0 | 32 | 0.743 | 0.0 |
| ranker_score>=0.549127 (q0.8) | 0 | 0.0 | 16 | 0.651 | 0.0 |
| ranker_score<=0.435086 (q0.2) | 0 | 0.0 | 15 | 1.13 | 0.0 |
| ranker_score<=0.466628 (q0.4) | 0 | 0.0 | 27 | 0.845 | 0.0 |
| ranker_score<=0.485658 (q0.5) | 0 | 0.0 | 29 | 0.721 | 0.0 |
| ranker_score<=0.513578 (q0.6) | 0 | 0.0 | 34 | 0.691 | 0.0 |
| ranker_score<=0.549127 (q0.8) | 0 | 0.0 | 38 | 0.711 | 0.0 |

## FILTER (mask) — signal_range_pct
_indicator/price-action filter; range across train quantiles_

**best stable range:** _none reaches min(FIT,VAL) PF ≥ 1.3_ ; closest = `signal_range_pct<=0.856704 (q0.8)` (min-PF 0.552)

| value | FIT n | FIT PF | VAL n | VAL PF | min(FIT,VAL) |
|---|---:|---:|---:|---:|---:|
| signal_range_pct>=0.324917 (q0.2) | 325 | 0.62 | 389 | 0.522 | 0.522 |
| signal_range_pct>=0.466781 (q0.4) | 308 | 0.624 | 362 | 0.507 | 0.507 |
| signal_range_pct>=0.543166 (q0.5) | 283 | 0.588 | 324 | 0.526 | 0.526 |
| signal_range_pct>=0.624411 (q0.6) | 235 | 0.627 | 286 | 0.449 | 0.449 |
| signal_range_pct>=0.856704 (q0.8) | 127 | 0.595 | 134 | 0.409 | 0.409 |
| signal_range_pct<=0.324917 (q0.2) | 120 | 0.301 | 118 | 0.525 | 0.301 |
| signal_range_pct<=0.466781 (q0.4) | 181 | 0.424 | 203 | 0.625 | 0.424 |
| signal_range_pct<=0.543166 (q0.5) | 213 | 0.515 | 231 | 0.557 | 0.515 |
| signal_range_pct<=0.624411 (q0.6) | 240 | 0.509 | 257 | 0.64 | 0.509 |
| signal_range_pct<=0.856704 (q0.8) | 285 | 0.552 | 314 | 0.584 | 0.552 |

## FILTER (mask) — upper_wick_pct
_indicator/price-action filter; range across train quantiles_

**best stable range:** _none reaches min(FIT,VAL) PF ≥ 1.3_ ; closest = `upper_wick_pct>=0.047669 (q0.5)` (min-PF 0.579)

| value | FIT n | FIT PF | VAL n | VAL PF | min(FIT,VAL) |
|---|---:|---:|---:|---:|---:|
| upper_wick_pct>=0.0 (q0.2) | 329 | 0.559 | 377 | 0.489 | 0.489 |
| upper_wick_pct>=0.026082 (q0.4) | 275 | 0.582 | 327 | 0.508 | 0.508 |
| upper_wick_pct>=0.047669 (q0.5) | 260 | 0.579 | 297 | 0.582 | 0.579 |
| upper_wick_pct>=0.070681 (q0.6) | 235 | 0.565 | 257 | 0.526 | 0.526 |
| upper_wick_pct>=0.131085 (q0.8) | 125 | 0.702 | 134 | 0.443 | 0.443 |
| upper_wick_pct<=0.0 (q0.2) | 174 | 0.559 | 186 | 0.582 | 0.559 |
| upper_wick_pct<=0.026082 (q0.4) | 217 | 0.512 | 235 | 0.566 | 0.512 |
| upper_wick_pct<=0.047669 (q0.5) | 242 | 0.521 | 264 | 0.526 | 0.521 |
| upper_wick_pct<=0.070681 (q0.6) | 259 | 0.529 | 283 | 0.614 | 0.529 |
| upper_wick_pct<=0.131085 (q0.8) | 291 | 0.558 | 326 | 0.542 | 0.542 |

## FILTER (mask) — lower_wick_pct
_indicator/price-action filter; range across train quantiles_

**best stable range:** _none reaches min(FIT,VAL) PF ≥ 1.3_ ; closest = `lower_wick_pct>=0.095696 (q0.8)` (min-PF 0.617)

| value | FIT n | FIT PF | VAL n | VAL PF | min(FIT,VAL) |
|---|---:|---:|---:|---:|---:|
| lower_wick_pct>=0.0 (q0.2) | 329 | 0.559 | 377 | 0.489 | 0.489 |
| lower_wick_pct>=0.0093 (q0.4) | 268 | 0.567 | 301 | 0.529 | 0.529 |
| lower_wick_pct>=0.026106 (q0.5) | 249 | 0.601 | 269 | 0.529 | 0.529 |
| lower_wick_pct>=0.044208 (q0.6) | 227 | 0.63 | 234 | 0.604 | 0.604 |
| lower_wick_pct>=0.095696 (q0.8) | 126 | 0.624 | 135 | 0.617 | 0.617 |
| lower_wick_pct<=0.0 (q0.2) | 203 | 0.515 | 244 | 0.577 | 0.515 |
| lower_wick_pct<=0.0093 (q0.4) | 218 | 0.537 | 262 | 0.531 | 0.531 |
| lower_wick_pct<=0.026106 (q0.5) | 245 | 0.554 | 296 | 0.502 | 0.502 |
| lower_wick_pct<=0.044208 (q0.6) | 267 | 0.514 | 310 | 0.502 | 0.502 |
| lower_wick_pct<=0.095696 (q0.8) | 295 | 0.595 | 347 | 0.513 | 0.513 |

## FILTER (mask) — wick_skew_pct
_indicator/price-action filter; range across train quantiles_

**best stable range:** _none reaches min(FIT,VAL) PF ≥ 1.3_ ; closest = `wick_skew_pct<=-0.05306 (q0.2)` (min-PF 0.684)

| value | FIT n | FIT PF | VAL n | VAL PF | min(FIT,VAL) |
|---|---:|---:|---:|---:|---:|
| wick_skew_pct>=-0.05306 (q0.2) | 307 | 0.541 | 358 | 0.453 | 0.453 |
| wick_skew_pct>=0.0 (q0.4) | 286 | 0.539 | 335 | 0.435 | 0.435 |
| wick_skew_pct>=0.009294 (q0.5) | 253 | 0.505 | 300 | 0.416 | 0.416 |
| wick_skew_pct>=0.03122 (q0.6) | 231 | 0.559 | 255 | 0.477 | 0.477 |
| wick_skew_pct>=0.098136 (q0.8) | 125 | 0.589 | 135 | 0.513 | 0.513 |
| wick_skew_pct<=-0.05306 (q0.2) | 127 | 0.684 | 131 | 0.725 | 0.684 |
| wick_skew_pct<=0.0 (q0.4) | 241 | 0.586 | 259 | 0.658 | 0.586 |
| wick_skew_pct<=0.009294 (q0.5) | 249 | 0.59 | 265 | 0.703 | 0.59 |
| wick_skew_pct<=0.03122 (q0.6) | 262 | 0.57 | 293 | 0.611 | 0.57 |
| wick_skew_pct<=0.098136 (q0.8) | 297 | 0.601 | 329 | 0.517 | 0.517 |

## PRE-MOMENTUM — pre_entry_momentum_score
_1-min pre-entry confirmation; range across train quantiles_

**best stable range:** _none reaches min(FIT,VAL) PF ≥ 1.3_ ; closest = `pre_entry_momentum_score>=69.46667 (q0.6)` (min-PF 0.61)

| value | FIT n | FIT PF | VAL n | VAL PF | min(FIT,VAL) |
|---|---:|---:|---:|---:|---:|
| pre_entry_momentum_score>=47.312196 (q0.2) | 296 | 0.591 | 366 | 0.505 | 0.505 |
| pre_entry_momentum_score>=60.60982 (q0.4) | 230 | 0.781 | 357 | 0.551 | 0.551 |
| pre_entry_momentum_score>=65.136139 (q0.5) | 207 | 0.819 | 348 | 0.575 | 0.575 |
| pre_entry_momentum_score>=69.46667 (q0.6) | 176 | 0.86 | 330 | 0.61 | 0.61 |
| pre_entry_momentum_score>=77.773984 (q0.8) | 111 | 0.826 | 244 | 0.581 | 0.581 |
| pre_entry_momentum_score<=47.312196 (q0.2) | 179 | 0.394 | 38 | 0.383 | 0.383 |
| pre_entry_momentum_score<=60.60982 (q0.4) | 210 | 0.364 | 131 | 0.393 | 0.364 |
| pre_entry_momentum_score<=65.136139 (q0.5) | 227 | 0.396 | 187 | 0.444 | 0.396 |
| pre_entry_momentum_score<=69.46667 (q0.6) | 253 | 0.47 | 239 | 0.473 | 0.47 |
| pre_entry_momentum_score<=77.773984 (q0.8) | 289 | 0.545 | 303 | 0.508 | 0.508 |

## PRE-MOMENTUM — sig5_adx_calc
_1-min pre-entry confirmation; range across train quantiles_

**best stable range:** _none reaches min(FIT,VAL) PF ≥ 1.3_ ; closest = `sig5_adx_calc<=22.346297 (q0.6)` (min-PF 0.602)

| value | FIT n | FIT PF | VAL n | VAL PF | min(FIT,VAL) |
|---|---:|---:|---:|---:|---:|
| sig5_adx_calc>=14.853013 (q0.2) | 321 | 0.553 | 370 | 0.516 | 0.516 |
| sig5_adx_calc>=18.111066 (q0.4) | 289 | 0.482 | 333 | 0.58 | 0.482 |
| sig5_adx_calc>=20.249848 (q0.5) | 273 | 0.422 | 308 | 0.561 | 0.422 |
| sig5_adx_calc>=22.346297 (q0.6) | 245 | 0.411 | 265 | 0.502 | 0.411 |
| sig5_adx_calc>=27.771238 (q0.8) | 127 | 0.395 | 149 | 0.439 | 0.395 |
| sig5_adx_calc<=14.853013 (q0.2) | 128 | 0.531 | 135 | 0.482 | 0.482 |
| sig5_adx_calc<=18.111066 (q0.4) | 212 | 0.742 | 236 | 0.526 | 0.526 |
| sig5_adx_calc<=20.249848 (q0.5) | 228 | 0.683 | 264 | 0.58 | 0.58 |
| sig5_adx_calc<=22.346297 (q0.6) | 249 | 0.692 | 288 | 0.602 | 0.602 |
| sig5_adx_calc<=27.771238 (q0.8) | 289 | 0.58 | 345 | 0.535 | 0.535 |

## PRE-MOMENTUM — sig5_rsi_dir
_1-min pre-entry confirmation; range across train quantiles_

**best stable range:** _none reaches min(FIT,VAL) PF ≥ 1.3_ ; closest = `sig5_rsi_dir>=54.406233 (q0.5)` (min-PF 0.604)

| value | FIT n | FIT PF | VAL n | VAL PF | min(FIT,VAL) |
|---|---:|---:|---:|---:|---:|
| sig5_rsi_dir>=47.400763 (q0.2) | 323 | 0.57 | 335 | 0.58 | 0.57 |
| sig5_rsi_dir>=52.375991 (q0.4) | 300 | 0.597 | 259 | 0.672 | 0.597 |
| sig5_rsi_dir>=54.406233 (q0.5) | 284 | 0.604 | 236 | 0.768 | 0.604 |
| sig5_rsi_dir>=56.443646 (q0.6) | 257 | 0.573 | 197 | 0.849 | 0.573 |
| sig5_rsi_dir>=60.4835 (q0.8) | 163 | 0.547 | 108 | 0.798 | 0.547 |
| sig5_rsi_dir<=47.400763 (q0.2) | 68 | 0.561 | 181 | 0.356 | 0.356 |
| sig5_rsi_dir<=52.375991 (q0.4) | 156 | 0.491 | 290 | 0.436 | 0.436 |
| sig5_rsi_dir<=54.406233 (q0.5) | 205 | 0.521 | 308 | 0.427 | 0.427 |
| sig5_rsi_dir<=56.443646 (q0.6) | 238 | 0.579 | 330 | 0.456 | 0.456 |
| sig5_rsi_dir<=60.4835 (q0.8) | 288 | 0.608 | 360 | 0.54 | 0.54 |

## PRE-MOMENTUM — sig5_vol_ratio20
_1-min pre-entry confirmation; range across train quantiles_

**best stable range:** _none reaches min(FIT,VAL) PF ≥ 1.3_ ; closest = `sig5_vol_ratio20>=3.593955 (q0.8)` (min-PF 0.61)

| value | FIT n | FIT PF | VAL n | VAL PF | min(FIT,VAL) |
|---|---:|---:|---:|---:|---:|
| sig5_vol_ratio20>=0.617167 (q0.2) | 310 | 0.601 | 299 | 0.584 | 0.584 |
| sig5_vol_ratio20>=1.608719 (q0.4) | 293 | 0.613 | 259 | 0.56 | 0.56 |
| sig5_vol_ratio20>=1.865448 (q0.5) | 281 | 0.634 | 234 | 0.504 | 0.504 |
| sig5_vol_ratio20>=2.237395 (q0.6) | 254 | 0.672 | 201 | 0.547 | 0.547 |
| sig5_vol_ratio20>=3.593955 (q0.8) | 166 | 0.63 | 101 | 0.61 | 0.61 |
| sig5_vol_ratio20<=0.617167 (q0.2) | 48 | 0.414 | 201 | 0.434 | 0.414 |
| sig5_vol_ratio20<=1.608719 (q0.4) | 126 | 0.561 | 301 | 0.56 | 0.56 |
| sig5_vol_ratio20<=1.865448 (q0.5) | 192 | 0.444 | 317 | 0.606 | 0.444 |
| sig5_vol_ratio20<=2.237395 (q0.6) | 243 | 0.47 | 334 | 0.559 | 0.47 |
| sig5_vol_ratio20<=3.593955 (q0.8) | 287 | 0.54 | 360 | 0.564 | 0.54 |

## PRE-MOMENTUM — pre1_adx
_1-min pre-entry confirmation; range across train quantiles_

**best stable range:** _none reaches min(FIT,VAL) PF ≥ 1.3_ ; closest = `pre1_adx>=37.433429 (q0.8)` (min-PF 0.751)

| value | FIT n | FIT PF | VAL n | VAL PF | min(FIT,VAL) |
|---|---:|---:|---:|---:|---:|
| pre1_adx>=20.602746 (q0.2) | 317 | 0.617 | 371 | 0.489 | 0.489 |
| pre1_adx>=25.38209 (q0.4) | 282 | 0.565 | 340 | 0.515 | 0.515 |
| pre1_adx>=27.901485 (q0.5) | 256 | 0.601 | 302 | 0.522 | 0.522 |
| pre1_adx>=30.675856 (q0.6) | 232 | 0.648 | 264 | 0.644 | 0.644 |
| pre1_adx>=37.433429 (q0.8) | 133 | 0.751 | 146 | 0.805 | 0.751 |
| pre1_adx<=20.602746 (q0.2) | 132 | 0.341 | 133 | 0.607 | 0.341 |
| pre1_adx<=25.38209 (q0.4) | 217 | 0.508 | 230 | 0.504 | 0.504 |
| pre1_adx<=27.901485 (q0.5) | 245 | 0.499 | 272 | 0.518 | 0.499 |
| pre1_adx<=30.675856 (q0.6) | 269 | 0.533 | 310 | 0.477 | 0.477 |
| pre1_adx<=37.433429 (q0.8) | 304 | 0.581 | 342 | 0.537 | 0.537 |

## PRE-MOMENTUM — pre3_range_r
_1-min pre-entry confirmation; range across train quantiles_

**best stable range:** _none reaches min(FIT,VAL) PF ≥ 1.3_ ; closest = `pre3_range_r>=0.383656 (q0.6)` (min-PF 0.548)

| value | FIT n | FIT PF | VAL n | VAL PF | min(FIT,VAL) |
|---|---:|---:|---:|---:|---:|
| pre3_range_r>=0.158993 (q0.2) | 327 | 0.593 | 376 | 0.513 | 0.513 |
| pre3_range_r>=0.247872 (q0.4) | 311 | 0.588 | 371 | 0.524 | 0.524 |
| pre3_range_r>=0.3031 (q0.5) | 278 | 0.645 | 364 | 0.503 | 0.503 |
| pre3_range_r>=0.383656 (q0.6) | 243 | 0.687 | 340 | 0.548 | 0.548 |
| pre3_range_r>=0.610547 (q0.8) | 152 | 0.75 | 257 | 0.509 | 0.509 |
| pre3_range_r<=0.158993 (q0.2) | 105 | 0.268 | 50 | 0.482 | 0.268 |
| pre3_range_r<=0.247872 (q0.4) | 173 | 0.44 | 138 | 0.505 | 0.44 |
| pre3_range_r<=0.3031 (q0.5) | 203 | 0.468 | 180 | 0.534 | 0.468 |
| pre3_range_r<=0.383656 (q0.6) | 222 | 0.47 | 229 | 0.554 | 0.47 |
| pre3_range_r<=0.610547 (q0.8) | 275 | 0.509 | 288 | 0.573 | 0.509 |

## PRE-MOMENTUM — pre5_mom_r
_1-min pre-entry confirmation; range across train quantiles_

**best stable range:** _none reaches min(FIT,VAL) PF ≥ 1.3_ ; closest = `pre5_mom_r>=0.65905 (q0.8)` (min-PF 0.566)

| value | FIT n | FIT PF | VAL n | VAL PF | min(FIT,VAL) |
|---|---:|---:|---:|---:|---:|
| pre5_mom_r>=-0.021415 (q0.2) | 297 | 0.664 | 363 | 0.511 | 0.511 |
| pre5_mom_r>=0.223742 (q0.4) | 225 | 0.793 | 366 | 0.543 | 0.543 |
| pre5_mom_r>=0.317483 (q0.5) | 200 | 0.864 | 367 | 0.554 | 0.554 |
| pre5_mom_r>=0.420712 (q0.6) | 183 | 0.858 | 370 | 0.541 | 0.541 |
| pre5_mom_r>=0.65905 (q0.8) | 125 | 0.817 | 280 | 0.566 | 0.566 |
| pre5_mom_r<=-0.021415 (q0.2) | 179 | 0.317 | 35 | 0.296 | 0.296 |
| pre5_mom_r<=0.223742 (q0.4) | 213 | 0.314 | 97 | 0.433 | 0.314 |
| pre5_mom_r<=0.317483 (q0.5) | 226 | 0.332 | 146 | 0.458 | 0.332 |
| pre5_mom_r<=0.420712 (q0.6) | 240 | 0.39 | 194 | 0.636 | 0.39 |
| pre5_mom_r<=0.65905 (q0.8) | 266 | 0.516 | 271 | 0.55 | 0.516 |

## PRE-MOMENTUM — pre3_close_pos
_1-min pre-entry confirmation; range across train quantiles_

**best stable range:** _none reaches min(FIT,VAL) PF ≥ 1.3_ ; closest = `pre3_close_pos>=1.0 (q0.8)` (min-PF 0.646)

| value | FIT n | FIT PF | VAL n | VAL PF | min(FIT,VAL) |
|---|---:|---:|---:|---:|---:|
| pre3_close_pos>=0.341922 (q0.2) | 300 | 0.598 | 372 | 0.512 | 0.512 |
| pre3_close_pos>=0.689648 (q0.4) | 264 | 0.589 | 331 | 0.557 | 0.557 |
| pre3_close_pos>=0.786414 (q0.5) | 238 | 0.635 | 300 | 0.591 | 0.591 |
| pre3_close_pos>=0.866881 (q0.6) | 201 | 0.593 | 271 | 0.58 | 0.58 |
| pre3_close_pos>=1.0 (q0.8) | 136 | 0.66 | 186 | 0.646 | 0.646 |
| pre3_close_pos<=0.341922 (q0.2) | 172 | 0.432 | 77 | 0.415 | 0.415 |
| pre3_close_pos<=0.689648 (q0.4) | 228 | 0.56 | 201 | 0.459 | 0.459 |
| pre3_close_pos<=0.786414 (q0.5) | 252 | 0.554 | 265 | 0.465 | 0.465 |
| pre3_close_pos<=0.866881 (q0.6) | 274 | 0.532 | 304 | 0.497 | 0.497 |
| pre3_close_pos<=1.0 (q0.8) | 329 | 0.559 | 377 | 0.489 | 0.489 |

## FILTER — regime (categorical)
_don't-fight-the-tape regime filter_

**best stable range:** _none reaches min(FIT,VAL) PF ≥ 1.3_ ; closest = `regime==NEUTRAL` (min-PF 0.675)

| value | FIT n | FIT PF | VAL n | VAL PF | min(FIT,VAL) |
|---|---:|---:|---:|---:|---:|
| regime==NEUTRAL | 208 | 0.675 | 220 | 0.799 | 0.675 |
| regime==TREND | 0 | 0.0 | 9 | 0.0 | 0.0 |
| regime!=BEAR | 329 | 0.559 | 377 | 0.489 | 0.489 |
| regime!=BULL | 208 | 0.675 | 229 | 0.751 | 0.675 |

## GUARD — min_slot (entry not before)
_avoid early-session traps_

**best stable range:** _none reaches min(FIT,VAL) PF ≥ 1.3_ ; closest = `min_slot=09:30` (min-PF 0.489)

| value | FIT n | FIT PF | VAL n | VAL PF | min(FIT,VAL) |
|---|---:|---:|---:|---:|---:|
| min_slot=09:30 | 329 | 0.559 | 377 | 0.489 | 0.489 |
| min_slot=09:45 | 329 | 0.559 | 377 | 0.489 | 0.489 |
| min_slot=10:00 | 329 | 0.559 | 377 | 0.489 | 0.489 |
| min_slot=10:30 | 329 | 0.559 | 377 | 0.489 | 0.489 |
| min_slot=11:00 | 326 | 0.55 | 371 | 0.487 | 0.487 |

## GUARD — max_slot (entry not after)
_avoid late-day low-quality entries_

**best stable range:** _none reaches min(FIT,VAL) PF ≥ 1.3_ ; closest = `max_slot=14:00` (min-PF 0.497)

| value | FIT n | FIT PF | VAL n | VAL PF | min(FIT,VAL) |
|---|---:|---:|---:|---:|---:|
| max_slot=12:00 | 175 | 0.539 | 192 | 0.326 | 0.326 |
| max_slot=12:30 | 226 | 0.553 | 224 | 0.298 | 0.298 |
| max_slot=13:00 | 261 | 0.593 | 268 | 0.427 | 0.427 |
| max_slot=14:00 | 302 | 0.593 | 345 | 0.497 | 0.497 |
| max_slot=14:30 | 329 | 0.559 | 377 | 0.489 | 0.489 |

## GUARD — top_n (best N per slot by vwap_dist_atr)
_selectivity per signal slot_

**best stable range:** _none reaches min(FIT,VAL) PF ≥ 1.3_ ; closest = `top_n=3` (min-PF 0.383)

| value | FIT n | FIT PF | VAL n | VAL PF | min(FIT,VAL) |
|---|---:|---:|---:|---:|---:|
| top_n=1 | 251 | 0.615 | 236 | 0.351 | 0.351 |
| top_n=2 | 306 | 0.603 | 317 | 0.332 | 0.332 |
| top_n=3 | 328 | 0.605 | 348 | 0.383 | 0.383 |
