# B_AVWAP_RECLAIM_REVERSAL (LONG) — PARAMETER_SWEEP_SUMMARY

_Generated 2026-07-03. Research-only; NO live trades; NO final_setup_conf.py edits._

Stage-3 one-knob-at-a-time sweeps from the baseline config, scored on FIT+VAL with the band objective (tent at PF 1.8, gap penalty 0.80). Baseline FIT/VAL band score is the reference; `improve` = higher score.

Total sweeps: **155** | improve: 73 | worse: 71

## exit

| knob | old | new | FIT n/PF | VAL n/PF | score | vs baseline |
|---|---|---|---|---|---|---|
| tgt_pct | 1.5 | 2.0 | 959/0.416 | 738/0.329 | 0.2594 | improve |
| tgt_pct | 1.5 | 2.5 | 940/0.438 | 721/0.336 | 0.2544 | improve |
| sl_pct | 0.7 | 1.0 | 847/0.443 | 625/0.338 | 0.254 | improve |
| sl_pct | 0.7 | 1.1 | 808/0.452 | 591/0.334 | 0.2396 | improve |
| sl_pct | 0.7 | 1.2 | 780/0.452 | 569/0.324 | 0.2216 | improve |
| sl_pct | 0.7 | 1.5 | 721/0.449 | 522/0.32 | 0.2168 | improve |
| sl_pct | 0.7 | 0.85 | 912/0.431 | 695/0.303 | 0.2006 | worse |
| tgt_pct | 1.5 | 1.25 | 1037/0.371 | 773/0.275 | 0.1982 | worse |
| tgt_pct | 1.5 | 0.8 | 1117/0.305 | 839/0.242 | 0.1916 | worse |
| tgt_pct | 1.5 | 1.0 | 1063/0.335 | 807/0.255 | 0.191 | worse |
| tgt_pct | 1.5 | 0.6 | 1168/0.238 | 889/0.209 | 0.1858 | worse |
| sl_pct | 0.7 | 0.5 | 1180/0.348 | 905/0.249 | 0.1698 | worse |

## filter

| knob | old | new | FIT n/PF | VAL n/PF | score | vs baseline |
|---|---|---|---|---|---|---|
| drop mask vwap_dist_atr<=1.0 | vwap_dist_atr<=1.0 | dropped | 1228/0.411 | 908/0.308 | 0.2256 | improve |
| mask vwap_dist_atr<= | 1.0 | 1.012503 (q0.65) | 1012/0.394 | 765/0.293 | 0.2122 | worse |
| mask vwap_dist_atr<= | 1.0 | 0.309997 (q0.2) | 399/0.318 | 339/0.253 | 0.201 | worse |
| mask vwap_dist_atr<= | 1.0 | 1.371281 (q0.8) | 1109/0.413 | 821/0.291 | 0.1934 | worse |
| mask vwap_dist_atr<= | 1.0 | 0.761686 (q0.5) | 877/0.368 | 665/0.264 | 0.1808 | worse |
| mask vwap_dist_atr<= | 1.0 | 0.523755 (q0.35) | 677/0.354 | 533/0.254 | 0.174 | worse |

## guard

| knob | old | new | FIT n/PF | VAL n/PF | score | vs baseline |
|---|---|---|---|---|---|---|
| top_n | - | 1 | 239/0.305 | 202/0.296 | 0.2888 | improve |
| min_slot | - | 12:00 | 763/0.354 | 641/0.316 | 0.2856 | improve |
| top_n | - | 2 | 555/0.349 | 459/0.305 | 0.2698 | improve |
| top_n | - | 3 | 723/0.363 | 626/0.297 | 0.2442 | improve |
| daily_loss_rs | 0.0 | 2000.0 | 567/0.434 | 377/0.327 | 0.2414 | improve |
| max_positions | 20 | 10 | 610/0.422 | 446/0.311 | 0.2222 | improve |
| max_slot | - | 12:30 | 585/0.376 | 435/0.287 | 0.2158 | improve |
| min_slot | - | 10:00 | 1008/0.399 | 763/0.297 | 0.2154 | flat |
| min_slot | - | 09:45 | 1008/0.399 | 763/0.297 | 0.2154 | flat |
| min_slot | - | 09:30 | 1008/0.399 | 763/0.297 | 0.2154 | flat |
| min_slot | - | 10:30 | 1008/0.399 | 763/0.297 | 0.2154 | flat |
| max_slot | - | 14:30 | 1008/0.399 | 763/0.297 | 0.2154 | flat |
| min_slot | - | 11:00 | 1005/0.401 | 756/0.294 | 0.2084 | worse |
| max_slot | - | 13:00 | 695/0.41 | 556/0.294 | 0.2012 | worse |
| max_slot | - | 14:00 | 901/0.411 | 675/0.285 | 0.1842 | worse |
| daily_loss_rs | 0.0 | 4000.0 | 679/0.438 | 498/0.297 | 0.1842 | worse |
| max_positions | 20 | 5 | 327/0.405 | 230/0.281 | 0.1818 | worse |
| max_slot | - | 12:00 | 445/0.382 | 270/0.253 | 0.1498 | worse |
| max_slot | - | 11:30 | 231/0.427 | 143/0.183 | -0.0122 | worse |

## indicator/price-action

| knob | old | new | FIT n/PF | VAL n/PF | score | vs baseline |
|---|---|---|---|---|---|---|
| +mask wick_skew_pct>= | - | 0.101075 (q0.8) | 275/0.437 | 209/0.397 | 0.365 | improve |
| +mask lower_wick_pct>= | - | 0.094123 (q0.8) | 302/0.347 | 217/0.357 | 0.339 | improve |
| +mask signal_range_pct>= | - | 0.887972 (q0.8) | 187/0.392 | 148/0.47 | 0.3296 | improve |
| +mask upper_wick_pct>= | - | 0.135302 (q0.8) | 271/0.371 | 211/0.429 | 0.3246 | improve |
| +mask vol_ratio>= | - | 2.55782 (q0.5) | 653/0.373 | 507/0.345 | 0.3226 | improve |
| +mask rs_pct>= | - | 0.85419 (q0.5) | 649/0.338 | 420/0.329 | 0.3218 | improve |
| +mask atr_pct>= | - | 0.002795 (q0.5) | 680/0.376 | 449/0.341 | 0.313 | improve |
| +mask quality_score>= | - | 75.262373 (q0.5) | 677/0.375 | 382/0.34 | 0.312 | improve |
| +mask upper_wick_pct<= | - | 0.047654 (q0.5) | 692/0.307 | 470/0.31 | 0.3046 | improve |
| +mask signal_range_pct>= | - | 0.540726 (q0.5) | 558/0.394 | 410/0.342 | 0.3004 | improve |
| +mask close_loc>= | - | 0.90196 (q0.5) | 662/0.297 | 448/0.309 | 0.2874 | improve |
| +mask body_pct<= | - | 0.637705 (q0.2) | 353/0.315 | 295/0.299 | 0.2862 | improve |
| +mask vol_ratio>= | - | 4.263719 (q0.8) | 251/0.361 | 226/0.315 | 0.2782 | improve |
| +mask rs_pct>= | - | 1.936181 (q0.8) | 278/0.358 | 161/0.458 | 0.278 | improve |
| +mask wick_skew_pct<= | - | 0.008237 (q0.5) | 693/0.284 | 486/0.28 | 0.2768 | improve |
| +mask vwap_dist_atr>= | - | 0.761686 (q0.5) | 351/0.395 | 254/0.325 | 0.269 | improve |
| +mask wick_skew_pct<= | - | -0.052753 (q0.2) | 328/0.293 | 208/0.325 | 0.2674 | improve |
| +mask rs_pct>= | - | 0.245521 (q0.2) | 884/0.386 | 663/0.32 | 0.2672 | improve |
| +mask atr_pct>= | - | 0.004015 (q0.8) | 264/0.362 | 176/0.481 | 0.2668 | improve |
| +mask atr_pct>= | - | 0.002049 (q0.2) | 961/0.375 | 694/0.314 | 0.2652 | improve |
| +mask lower_wick_pct>= | - | 0.027228 (q0.5) | 701/0.343 | 533/0.299 | 0.2638 | improve |
| +mask quality_score>= | - | 50.89676 (q0.2) | 901/0.366 | 664/0.307 | 0.2598 | improve |
| +mask signal_range_pct>= | - | 0.324713 (q0.2) | 902/0.409 | 674/0.326 | 0.2596 | improve |
| +mask body_pct>= | - | 0.791667 (q0.5) | 600/0.336 | 419/0.291 | 0.255 | improve |
| +mask vol_ratio>= | - | 1.795083 (q0.2) | 893/0.389 | 676/0.312 | 0.2504 | improve |
| +mask close_loc>= | - | 0.764709 (q0.2) | 889/0.35 | 678/0.291 | 0.2438 | improve |
| +mask lower_wick_pct<= | - | 0.0 (q0.2) | 486/0.372 | 359/0.298 | 0.2388 | improve |
| +mask wick_skew_pct<= | - | 0.101075 (q0.8) | 907/0.341 | 687/0.274 | 0.2204 | improve |
| +mask close_loc>= | - | 1.0 (q0.8) | 440/0.271 | 270/0.338 | 0.2174 | improve |
| +mask upper_wick_pct<= | - | 0.0 (q0.2) | 440/0.271 | 270/0.338 | 0.2174 | improve |
| +mask vwap_dist_atr<= | - | 1.371281 (q0.8) | 1008/0.399 | 763/0.297 | 0.2154 | flat |
| +mask upper_wick_pct>= | - | 0.0 (q0.2) | 1008/0.399 | 763/0.297 | 0.2154 | flat |
| +mask lower_wick_pct>= | - | 0.0 (q0.2) | 1008/0.399 | 763/0.297 | 0.2154 | flat |
| +mask close_loc<= | - | 1.0 (q0.8) | 1008/0.399 | 763/0.297 | 0.2154 | flat |
| +mask vwap_dist_atr>= | - | 0.309997 (q0.2) | 843/0.395 | 649/0.29 | 0.206 | worse |
| +mask vol_ratio<= | - | 4.263719 (q0.8) | 917/0.383 | 688/0.284 | 0.2048 | worse |
| +mask vwap_dist_atr<= | - | 0.309997 (q0.2) | 399/0.318 | 339/0.253 | 0.201 | worse |
| +mask body_pct<= | - | 0.791667 (q0.5) | 738/0.386 | 567/0.279 | 0.1934 | worse |
| +mask quality_score<= | - | 75.262373 (q0.5) | 611/0.334 | 580/0.252 | 0.1864 | worse |
| +mask body_pct>= | - | 0.637705 (q0.2) | 874/0.386 | 663/0.274 | 0.1844 | worse |
| +mask vwap_dist_atr<= | - | 0.761686 (q0.5) | 877/0.368 | 665/0.264 | 0.1808 | worse |
| +mask lower_wick_pct<= | - | 0.027228 (q0.5) | 643/0.368 | 493/0.263 | 0.179 | worse |
| +mask signal_range_pct<= | - | 0.887972 (q0.8) | 910/0.373 | 691/0.261 | 0.1714 | worse |
| +mask body_pct<= | - | 0.931018 (q0.8) | 897/0.405 | 698/0.274 | 0.1692 | worse |
| +mask upper_wick_pct<= | - | 0.135302 (q0.8) | 889/0.361 | 679/0.254 | 0.1684 | worse |
| +mask body_pct>= | - | 0.931018 (q0.8) | 257/0.264 | 156/0.385 | 0.1672 | worse |
| +mask close_loc<= | - | 0.764709 (q0.2) | 331/0.414 | 276/0.276 | 0.1656 | worse |
| +mask atr_pct<= | - | 0.004015 (q0.8) | 851/0.381 | 679/0.259 | 0.1614 | worse |
| +mask lower_wick_pct<= | - | 0.094123 (q0.8) | 882/0.386 | 678/0.259 | 0.1574 | worse |
| +mask wick_skew_pct>= | - | -0.052753 (q0.2) | 880/0.401 | 684/0.265 | 0.1562 | worse |
| +mask signal_range_pct<= | - | 0.540726 (q0.5) | 698/0.341 | 553/0.236 | 0.152 | worse |
| +mask vol_ratio<= | - | 2.55782 (q0.5) | 699/0.328 | 515/0.227 | 0.1462 | worse |
| +mask quality_score<= | - | 104.654687 (q0.8) | 892/0.395 | 707/0.256 | 0.1448 | worse |
| +mask quality_score>= | - | 104.654687 (q0.8) | 297/0.332 | 125/0.567 | 0.144 | worse |
| +mask wick_skew_pct>= | - | 0.008237 (q0.5) | 648/0.433 | 529/0.272 | 0.1432 | worse |
| +mask rs_pct<= | - | 1.936181 (q0.8) | 895/0.389 | 706/0.249 | 0.137 | worse |
| +mask rs_pct<= | - | 0.85419 (q0.5) | 664/0.399 | 573/0.247 | 0.1254 | worse |
| +mask atr_pct<= | - | 0.002795 (q0.5) | 587/0.349 | 520/0.221 | 0.1186 | worse |
| +mask upper_wick_pct>= | - | 0.047654 (q0.5) | 662/0.426 | 540/0.252 | 0.1128 | worse |
| +mask atr_pct<= | - | 0.002049 (q0.2) | 287/0.314 | 262/0.199 | 0.107 | worse |
| +mask rs_pct<= | - | 0.245521 (q0.2) | 289/0.343 | 268/0.211 | 0.1054 | worse |
| +mask quality_score<= | - | 50.89676 (q0.2) | 230/0.43 | 314/0.243 | 0.0934 | worse |
| +mask close_loc<= | - | 0.90196 (q0.5) | 690/0.435 | 563/0.245 | 0.093 | worse |
| +mask signal_range_pct<= | - | 0.324713 (q0.2) | 403/0.269 | 294/0.159 | 0.071 | worse |
| +mask vol_ratio<= | - | 1.795083 (q0.2) | 303/0.333 | 217/0.177 | 0.0522 | worse |
| +mask vwap_dist_atr>= | - | 1.371281 (q0.8) | 0/0.0 | 0/0.0 | -5.0 | worse |

## pre-momentum

| knob | old | new | FIT n/PF | VAL n/PF | score | vs baseline |
|---|---|---|---|---|---|---|
| +premom pre5_mom_r>= | - | 0.342895 (q0.5) | 804/0.387 | 447/0.383 | 0.3798 | improve |
| +premom pre_entry_momentum_score>= | - | 66.395361 (q0.5) | 774/0.373 | 442/0.374 | 0.3722 | improve |
| +premom pre5_mom_r>= | - | 0.691398 (q0.8) | 395/0.413 | 161/0.473 | 0.365 | improve |
| +premom sig5_rsi_dir>= | - | 57.986759 (q0.5) | 560/0.377 | 355/0.37 | 0.3644 | improve |
| +premom pre3_range_r>= | - | 0.626941 (q0.8) | 417/0.414 | 220/0.478 | 0.3628 | improve |
| +premom sig5_vol_ratio20>= | - | 2.495784 (q0.5) | 675/0.368 | 503/0.355 | 0.3446 | improve |
| +premom pre3_range_r>= | - | 0.328206 (q0.5) | 824/0.384 | 546/0.361 | 0.3426 | improve |
| +premom pre_entry_momentum_score>= | - | 77.956891 (q0.8) | 376/0.388 | 168/0.466 | 0.3256 | improve |
| +premom sig5_rsi_dir>= | - | 62.727852 (q0.8) | 189/0.361 | 118/0.406 | 0.325 | improve |
| +premom pre3_close_pos>= | - | 0.799384 (q0.5) | 749/0.306 | 466/0.309 | 0.3036 | improve |
| +premom sig5_rsi_dir>= | - | 53.138073 (q0.2) | 847/0.371 | 666/0.321 | 0.281 | improve |
| +premom pre5_mom_r>= | - | 0.040312 (q0.2) | 949/0.393 | 642/0.33 | 0.2796 | improve |
| +premom pre1_adx<= | - | 27.066949 (q0.5) | 746/0.342 | 551/0.307 | 0.279 | improve |
| +premom sig5_vol_ratio20>= | - | 4.180733 (q0.8) | 269/0.388 | 221/0.326 | 0.2764 | improve |
| +premom pre3_range_r>= | - | 0.167283 (q0.2) | 973/0.393 | 729/0.326 | 0.2724 | improve |
| +premom sig5_adx_calc>= | - | 27.289658 (q0.8) | 335/0.339 | 192/0.295 | 0.2598 | improve |
| +premom pre3_close_pos>= | - | 1.0 (q0.8) | 447/0.293 | 246/0.337 | 0.2578 | improve |
| +premom pre_entry_momentum_score>= | - | 52.165367 (q0.2) | 946/0.396 | 639/0.315 | 0.2502 | improve |
| +premom sig5_vol_ratio20>= | - | 1.759121 (q0.2) | 915/0.396 | 682/0.314 | 0.2484 | improve |
| +premom sig5_adx_calc<= | - | 19.7609 (q0.5) | 642/0.347 | 515/0.29 | 0.2444 | improve |
| +premom pre1_adx>= | - | 20.231494 (q0.2) | 877/0.379 | 704/0.301 | 0.2386 | improve |
| +premom pre3_close_pos<= | - | 0.441252 (q0.2) | 241/0.31 | 352/0.267 | 0.2326 | improve |
| +premom sig5_adx_calc>= | - | 14.919849 (q0.2) | 915/0.38 | 700/0.297 | 0.2306 | improve |
| +premom pre1_adx>= | - | 35.777423 (q0.8) | 286/0.28 | 235/0.247 | 0.2206 | improve |
| +premom sig5_adx_calc<= | - | 14.919849 (q0.2) | 326/0.375 | 261/0.289 | 0.2202 | improve |
| +premom pre1_adx<= | - | 35.777423 (q0.8) | 907/0.403 | 695/0.3 | 0.2176 | improve |
| +premom pre3_close_pos<= | - | 1.0 (q0.8) | 1008/0.399 | 763/0.297 | 0.2154 | flat |
| +premom sig5_rsi_dir<= | - | 62.727852 (q0.8) | 932/0.393 | 713/0.289 | 0.2058 | worse |
| +premom pre3_close_pos>= | - | 0.441252 (q0.2) | 920/0.393 | 659/0.289 | 0.2058 | worse |
| +premom pre3_range_r<= | - | 0.626941 (q0.8) | 827/0.348 | 665/0.263 | 0.195 | worse |
| +premom sig5_adx_calc<= | - | 27.289658 (q0.8) | 859/0.389 | 687/0.281 | 0.1946 | worse |
| +premom sig5_vol_ratio20<= | - | 4.180733 (q0.8) | 919/0.384 | 700/0.278 | 0.1932 | worse |
| +premom sig5_vol_ratio20<= | - | 1.759121 (q0.2) | 313/0.319 | 292/0.248 | 0.1912 | worse |
| +premom pre5_mom_r<= | - | 0.691398 (q0.8) | 833/0.351 | 693/0.261 | 0.189 | worse |
| +premom sig5_adx_calc>= | - | 19.7609 (q0.5) | 713/0.361 | 516/0.264 | 0.1864 | worse |
| +premom pre1_adx>= | - | 27.066949 (q0.5) | 636/0.368 | 519/0.263 | 0.179 | worse |
| +premom pre_entry_momentum_score<= | - | 52.165367 (q0.2) | 155/0.291 | 347/0.226 | 0.174 | worse |
| +premom pre_entry_momentum_score<= | - | 77.956891 (q0.8) | 851/0.361 | 694/0.256 | 0.172 | worse |
| +premom pre5_mom_r<= | - | 0.342895 (q0.5) | 506/0.336 | 522/0.242 | 0.1668 | worse |
| +premom sig5_vol_ratio20<= | - | 2.495784 (q0.5) | 722/0.321 | 556/0.232 | 0.1608 | worse |
| +premom sig5_rsi_dir<= | - | 57.986759 (q0.5) | 767/0.363 | 589/0.245 | 0.1506 | worse |
| +premom pre_entry_momentum_score<= | - | 66.395361 (q0.5) | 542/0.347 | 542/0.236 | 0.1472 | worse |
| +premom pre3_close_pos<= | - | 0.799384 (q0.5) | 652/0.414 | 572/0.254 | 0.126 | worse |
| +premom pre5_mom_r<= | - | 0.040312 (q0.2) | 131/0.354 | 332/0.216 | 0.1056 | worse |
| +premom pre3_range_r<= | - | 0.328206 (q0.5) | 524/0.338 | 499/0.208 | 0.104 | worse |
| +premom pre3_range_r<= | - | 0.167283 (q0.2) | 182/0.225 | 216/0.152 | 0.0936 | worse |
| +premom pre1_adx<= | - | 20.231494 (q0.2) | 387/0.34 | 258/0.2 | 0.088 | worse |
| +premom sig5_rsi_dir<= | - | 53.138073 (q0.2) | 390/0.349 | 334/0.188 | 0.0592 | worse |

## regime

| knob | old | new | FIT n/PF | VAL n/PF | score | vs baseline |
|---|---|---|---|---|---|---|
| +mask regime==NEUTRAL | - | NEUTRAL | 465/0.371 | 555/0.317 | 0.2738 | improve |
| +mask regime!=TREND | - | TREND | 789/0.354 | 763/0.297 | 0.2514 | improve |
| +mask regime!=BULL | - | BULL | 684/0.433 | 555/0.317 | 0.2242 | improve |
| +mask regime!=BEAR | - | BEAR | 1008/0.399 | 763/0.297 | 0.2154 | flat |

## Best stable knobs (score-improving, FIT and VAL both alive)

- **pre-momentum / +premom pre5_mom_r>=** -> 0.342895 (q0.5) (FIT 804/0.387, VAL 447/0.383, score 0.3798)
- **pre-momentum / +premom pre_entry_momentum_score>=** -> 66.395361 (q0.5) (FIT 774/0.373, VAL 442/0.374, score 0.3722)
- **indicator/price-action / +mask wick_skew_pct>=** -> 0.101075 (q0.8) (FIT 275/0.437, VAL 209/0.397, score 0.365)
- **pre-momentum / +premom pre5_mom_r>=** -> 0.691398 (q0.8) (FIT 395/0.413, VAL 161/0.473, score 0.365)
- **pre-momentum / +premom sig5_rsi_dir>=** -> 57.986759 (q0.5) (FIT 560/0.377, VAL 355/0.37, score 0.3644)
- **pre-momentum / +premom pre3_range_r>=** -> 0.626941 (q0.8) (FIT 417/0.414, VAL 220/0.478, score 0.3628)
- **pre-momentum / +premom sig5_vol_ratio20>=** -> 2.495784 (q0.5) (FIT 675/0.368, VAL 503/0.355, score 0.3446)
- **pre-momentum / +premom pre3_range_r>=** -> 0.328206 (q0.5) (FIT 824/0.384, VAL 546/0.361, score 0.3426)
- **indicator/price-action / +mask lower_wick_pct>=** -> 0.094123 (q0.8) (FIT 302/0.347, VAL 217/0.357, score 0.339)
- **indicator/price-action / +mask signal_range_pct>=** -> 0.887972 (q0.8) (FIT 187/0.392, VAL 148/0.47, score 0.3296)
- **pre-momentum / +premom pre_entry_momentum_score>=** -> 77.956891 (q0.8) (FIT 376/0.388, VAL 168/0.466, score 0.3256)
- **pre-momentum / +premom sig5_rsi_dir>=** -> 62.727852 (q0.8) (FIT 189/0.361, VAL 118/0.406, score 0.325)
- **indicator/price-action / +mask upper_wick_pct>=** -> 0.135302 (q0.8) (FIT 271/0.371, VAL 211/0.429, score 0.3246)
- **indicator/price-action / +mask vol_ratio>=** -> 2.55782 (q0.5) (FIT 653/0.373, VAL 507/0.345, score 0.3226)
- **indicator/price-action / +mask rs_pct>=** -> 0.85419 (q0.5) (FIT 649/0.338, VAL 420/0.329, score 0.3218)

## Overfit-risk notes

- Any knob whose FIT PF explodes while VAL PF collapses is a knife-edge; the band objective already penalises the gap, and stage-5 adds neighborhood + dropout checks.
- Sweeps that push PF far above 1.80 are treated as overshoot, not success.