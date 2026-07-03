# A_MOD_CLOSE_CONTINUATION_BREAK (LONG) — FAILURE_ANALYSIS

_Generated 2026-07-03. Research-only; NO live trades; NO final_setup_conf.py edits._

## Baseline book (TRAIN, raw detector @ SL0.70/T1.50, 15 bps/leg)

n=1887 PF=0.315 net=Rs-846,107 win%=22.6 avgW=Rs915 avgL=Rs-846 maxDD=Rs-845,212 SL/TGT/EOD=1259/260/368 tpd=32.53 domT/D/S=0.003/9.99/9.99 dbp=1.0

- winners 426 vs losers 1461

## By outcome

- {'SL': 1259, 'EOD': 368, 'TARGET': 260}
- avg bars held by outcome: {'EOD': 151.8, 'SL': 45.0, 'TARGET': 74.4}

## By hour bucket

- 10-11: n=104 net=Rs-44,381 PF=0.36
- 11-12: n=763 net=Rs-351,155 PF=0.324
- 12-13: n=476 net=Rs-235,067 PF=0.274
- 13-14: n=363 net=Rs-140,495 PF=0.348
- 14+: n=181 net=Rs-75,009 PF=0.302

## By regime

- BEAR: n=1692 net=Rs-778,904 PF=0.309
- BULL: n=120 net=Rs-52,702 PF=0.257
- NEUTRAL: n=57 net=Rs-16,773 PF=0.445
- TREND: n=18 net=Rs2,272 PF=1.326

## Winner vs loser feature medians (signal features)

| feature | winners | losers |
|---|---|---|
| rs_pct | 1.8678 | 2.3558 |
| vol_ratio | 2.558 | 2.5895 |
| atr_pct | 0.0032 | 0.0033 |
| body_pct | 0.7809 | 0.7857 |
| close_loc | 0.9233 | 0.9394 |
| vwap_dist_atr | 2.362 | 2.3996 |
| quality_score | 75.8756 | 84.7584 |
| signal_range_pct | 0.4766 | 0.5039 |
| upper_wick_pct | 0.0331 | 0.027 |

## Worst days

- 2026-05-12: Rs-66,439
- 2026-03-11: Rs-52,388
- 2026-03-13: Rs-52,271
- 2026-05-29: Rs-50,629
- 2026-03-02: Rs-48,178
- 2026-05-21: Rs-47,240
- 2026-03-30: Rs-46,444
- 2026-03-20: Rs-43,688

## Worst symbols

- USHAMART: Rs-6,263
- INDIASHLTR: Rs-5,569
- ACE: Rs-4,963
- SHRIPISTON: Rs-4,723
- EXICOM: Rs-4,661
- MARKSANS: Rs-4,658
- PFOCUS: Rs-4,657
- SGFIN: Rs-4,657

## Worst trades

| date | ticker | outcome | bars | net Rs |
|---|---|---|---|---|
| 2026-03-02 | GOCOLORS | SL | 36 | -933 |
| 2026-03-20 | CHEMPLASTS | SL | 119 | -933 |
| 2026-05-12 | JINDRILL | SL | 6 | -933 |
| 2026-05-12 | NITINSPIN | SL | 15 | -933 |
| 2026-03-20 | KIOCL | SL | 1 | -933 |
| 2026-03-20 | CYIENTDLM | SL | 26 | -933 |
| 2026-05-07 | GOCOLORS | SL | 7 | -933 |
| 2026-05-06 | VIKRAMSOLR | SL | 6 | -933 |
| 2026-05-12 | PARADEEP | SL | 4 | -933 |
| 2026-03-06 | MPSLTD | SL | 116 | -933 |

## Why rejected candidates failed (confirmation stage)

- SL0.85/T2.0 mask[wick_skew_pct>=0.042427;rs_pct<=3.019326] pm[pre3_range_r>=0.628068] guard={'min_slot': '09:45'}: **REJECT: TRAIN PF outside [1.30,1.80]** (TRAIN n=92 PF=0.573 net=Rs-26,897 win%=29.3 avgW=Rs1,339 avgL=Rs-970 maxDD=Rs-25,815 SL/TGT/EOD=54/18/20 tpd=3.17 domT/D/S=0.049/9.99/9.99 dbp=0.9793 | TEST (not run))
- SL0.7/T2.0 mask[close_loc<=1.0;rs_pct<=3.019326] pm[pre1_adx>=36.28313] guard={'min_slot': '09:45'}: **REJECT: TRAIN PF outside [1.30,1.80]** (TRAIN n=408 PF=0.468 net=Rs-129,169 win%=26.2 avgW=Rs1,063 avgL=Rs-807 maxDD=Rs-128,239 SL/TGT/EOD=239/44/125 tpd=8.0 domT/D/S=0.02/9.99/9.99 dbp=1.0 | TEST (not run))
- SL0.6/T2.0 mask[rs_pct>=1.989908;rs_pct<=3.019326] pm[pre5_mom_r>=0.388087] guard={'min_slot': '09:45'}: **REJECT: TRAIN PF outside [1.30,1.80]** (TRAIN n=297 PF=0.427 net=Rs-105,457 win%=20.5 avgW=Rs1,289 avgL=Rs-780 maxDD=Rs-107,302 SL/TGT/EOD=217/36/44 tpd=8.74 domT/D/S=0.022/9.99/9.99 dbp=1.0 | TEST (not run))
- SL0.6/T2.0 mask[rs_pct>=1.989908;rs_pct<=3.019326] pm[pre5_mom_r>=0.388087] guard={'min_slot': '09:30'}: **REJECT: TRAIN PF outside [1.30,1.80]** (TRAIN n=315 PF=0.46 net=Rs-104,649 win%=21.6 avgW=Rs1,311 avgL=Rs-785 maxDD=Rs-106,495 SL/TGT/EOD=228/41/46 tpd=9.26 domT/D/S=0.02/9.99/9.99 dbp=0.9998 | TEST (not run))
- SL0.85/T2.0 mask[close_loc>=0.863641] pm[pre1_adx>=36.28313] guard={'min_slot': '09:45', 'top_n': 3}: **REJECT: TRAIN PF outside [1.30,1.80]** (TRAIN n=329 PF=0.466 net=Rs-116,721 win%=28.0 avgW=Rs1,109 avgL=Rs-923 maxDD=Rs-116,470 SL/TGT/EOD=182/41/106 tpd=7.0 domT/D/S=0.022/9.99/9.99 dbp=1.0 | TEST (not run))
- SL0.7/T2.0 mask[close_loc<=0.863641;upper_wick_pct>=0.082502] pm[pre1_adx>=17.152161] guard={'min_slot': '09:45'}: **REJECT: TRAIN PF outside [1.30,1.80]** (TRAIN n=310 PF=0.436 net=Rs-116,866 win%=23.5 avgW=Rs1,236 avgL=Rs-874 maxDD=Rs-116,050 SL/TGT/EOD=216/42/52 tpd=7.21 domT/D/S=0.02/9.99/9.99 dbp=0.9998 | TEST (not run))
- SL1.2/T2.0 mask[close_loc<=1.0;body_pct>=0.697703] pm[sig5_vol_ratio20<=2.341116] guard={'min_slot': '10:00', 'top_n': 3}: **REJECT: TRAIN PF outside [1.30,1.80]** (TRAIN n=416 PF=0.479 net=Rs-164,895 win%=31.2 avgW=Rs1,165 avgL=Rs-1,106 maxDD=Rs-164,264 SL/TGT/EOD=177/65/174 tpd=8.0 domT/D/S=0.015/9.99/9.99 dbp=1.0 | TEST (not run))
- SL0.85/T2.5 mask[-] pm[pre3_close_pos>=1.0] guard={'top_n': 1}: **REJECT: TRAIN PF outside [1.30,1.80]** (TRAIN n=247 PF=0.403 net=Rs-100,284 win%=24.3 avgW=Rs1,129 avgL=Rs-899 maxDD=Rs-100,943 SL/TGT/EOD=138/20/89 tpd=4.84 domT/D/S=0.033/9.99/9.99 dbp=1.0 | TEST (not run))
- SL0.6/T2.0 mask[rs_pct>=1.989908;rs_pct<=3.019326] pm[pre5_mom_r<=0.388087] guard={'min_slot': '10:00'}: **REJECT: TRAIN PF outside [1.30,1.80]** (TRAIN n=260 PF=0.354 net=Rs-93,244 win%=23.1 avgW=Rs853 avgL=Rs-722 maxDD=Rs-92,412 SL/TGT/EOD=158/18/84 tpd=7.88 domT/D/S=0.035/9.99/9.99 dbp=1.0 | TEST (not run))
- SL1.0/T2.0 mask[-] pm[-] guard=None: **REJECT: TRAIN PF outside [1.30,1.80]** (TRAIN n=1412 PF=0.364 net=Rs-671,000 win%=26.3 avgW=Rs1,035 avgL=Rs-1,015 maxDD=Rs-669,819 SL/TGT/EOD=746/155/511 tpd=24.34 domT/D/S=0.005/9.99/9.99 dbp=1.0 | TEST (not run))
- SL0.7/T2.0 mask[close_loc<=0.863641;upper_wick_pct>=0.0] pm[pre1_adx>=17.152161] guard={'min_slot': '09:45'}: **REJECT: TRAIN PF outside [1.30,1.80]** (TRAIN n=420 PF=0.378 net=Rs-171,546 win%=23.3 avgW=Rs1,065 avgL=Rs-857 maxDD=Rs-170,617 SL/TGT/EOD=284/41/95 tpd=8.08 domT/D/S=0.017/9.99/9.99 dbp=1.0 | TEST (not run))
- SL0.7/T2.0 mask[close_loc<=0.863641;upper_wick_pct>=0.082502] pm[pre5_mom_r>=0.125824] guard={'min_slot': '09:45'}: **REJECT: TRAIN PF outside [1.30,1.80]** (TRAIN n=294 PF=0.422 net=Rs-116,141 win%=22.4 avgW=Rs1,286 avgL=Rs-882 maxDD=Rs-116,139 SL/TGT/EOD=210/42/42 tpd=7.74 domT/D/S=0.021/9.99/9.99 dbp=0.9997 | TEST (not run))
- SL0.6/T1.5 mask[rs_pct>=1.989908;rs_pct<=3.019326] pm[pre5_mom_r>=0.388087] guard=None: **REJECT: TRAIN PF outside [1.30,1.80]** (TRAIN n=294 PF=0.373 net=Rs-113,474 win%=21.8 avgW=Rs1,053 avgL=Rs-786 maxDD=Rs-113,898 SL/TGT/EOD=215/49/30 tpd=8.65 domT/D/S=0.019/9.99/9.99 dbp=1.0 | TEST (not run))
- SL0.6/T2.0 mask[upper_wick_pct>=0.0] pm[-] guard={'min_slot': '11:00', 'max_slot': '14:00', 'top_n': 1}: **REJECT: TRAIN PF outside [1.30,1.80]** (TRAIN n=734 PF=0.329 net=Rs-302,635 win%=19.6 avgW=Rs1,031 avgL=Rs-765 maxDD=Rs-301,811 SL/TGT/EOD=519/58/157 tpd=12.66 domT/D/S=0.012/9.99/9.99 dbp=1.0 | TEST (not run))
- SL0.6/T2.0 mask[body_pct>=0.749998;rs_pct<=3.019326] pm[sig5_vol_ratio20>=1.75039] guard={'min_slot': '09:45'}: **REJECT: TRAIN PF outside [1.30,1.80]** (TRAIN n=476 PF=0.401 net=Rs-167,174 win%=23.3 avgW=Rs1,007 avgL=Rs-764 maxDD=Rs-166,344 SL/TGT/EOD=322/43/111 tpd=8.5 domT/D/S=0.02/9.99/9.99 dbp=1.0 | TEST (not run))
- SL0.6/T2.0 mask[rs_pct>=1.989908;quality_score<=98.795232] pm[pre5_mom_r>=0.388087] guard={'min_slot': '09:30'}: **REJECT: TRAIN PF outside [1.30,1.80]** (TRAIN n=310 PF=0.411 net=Rs-114,339 win%=19.7 avgW=Rs1,306 avgL=Rs-779 maxDD=Rs-113,507 SL/TGT/EOD=228/37/45 tpd=8.86 domT/D/S=0.022/9.99/9.99 dbp=0.9996 | TEST (not run))
- SL0.85/T2.5 mask[-] pm[-] guard={'max_slot': '12:00', 'top_n': 3}: **REJECT: TRAIN PF outside [1.30,1.80]** (TRAIN n=644 PF=0.504 net=Rs-227,324 win%=27.8 avgW=Rs1,289 avgL=Rs-985 maxDD=Rs-226,249 SL/TGT/EOD=399/72/173 tpd=13.14 domT/D/S=0.01/9.99/9.99 dbp=0.9995 | TEST (not run))
- SL0.85/T2.5 mask[-] pm[pre3_close_pos<=1.0] guard={'max_slot': '12:00', 'top_n': 3}: **REJECT: TRAIN PF outside [1.30,1.80]** (TRAIN n=644 PF=0.504 net=Rs-227,324 win%=27.8 avgW=Rs1,289 avgL=Rs-985 maxDD=Rs-226,249 SL/TGT/EOD=399/72/173 tpd=13.14 domT/D/S=0.01/9.99/9.99 dbp=0.9995 | TEST (not run))
- SL1.2/T1.5 mask[-] pm[-] guard={'min_slot': '10:30', 'max_slot': '12:00', 'top_n': 3}: **REJECT: TRAIN PF outside [1.30,1.80]** (TRAIN n=406 PF=0.479 net=Rs-158,069 win%=35.7 avgW=Rs1,004 avgL=Rs-1,163 maxDD=Rs-156,647 SL/TGT/EOD=186/101/119 tpd=8.29 domT/D/S=0.009/9.99/9.99 dbp=1.0 | TEST (not run))
- SL0.85/T2.5 mask[body_pct>=0.639376] pm[-] guard={'max_slot': '12:00', 'top_n': 3}: **REJECT: TRAIN PF outside [1.30,1.80]** (TRAIN n=546 PF=0.506 net=Rs-191,050 win%=28.2 avgW=Rs1,271 avgL=Rs-987 maxDD=Rs-189,976 SL/TGT/EOD=337/61/148 tpd=11.38 domT/D/S=0.012/9.99/9.99 dbp=0.9994 | TEST (not run))
- SL0.85/T2.5 mask[-] pm[-] guard={'max_slot': '12:00'}: **REJECT: TRAIN PF outside [1.30,1.80]** (TRAIN n=813 PF=0.447 net=Rs-327,106 win%=25.8 avgW=Rs1,257 avgL=Rs-980 maxDD=Rs-326,067 SL/TGT/EOD=514/78/221 tpd=16.59 domT/D/S=0.009/9.99/9.99 dbp=1.0 | TEST (not run))
- SL1.2/T2.0 mask[atr_pct>=0.003807] pm[-] guard={'max_slot': '12:00', 'top_n': 3}: **REJECT: TRAIN PF outside [1.30,1.80]** (TRAIN n=297 PF=0.478 net=Rs-138,485 win%=32.0 avgW=Rs1,335 avgL=Rs-1,313 maxDD=Rs-139,518 SL/TGT/EOD=175/64/58 tpd=8.74 domT/D/S=0.014/9.99/9.99 dbp=1.0 | TEST (not run))
- SL1.2/T1.5 mask[-] pm[-] guard={'min_slot': '09:45', 'max_slot': '14:00', 'top_n': 2}: **REJECT: TRAIN PF outside [1.30,1.80]** (TRAIN n=678 PF=0.411 net=Rs-300,333 win%=32.3 avgW=Rs959 avgL=Rs-1,112 maxDD=Rs-299,702 SL/TGT/EOD=291/142/245 tpd=11.69 domT/D/S=0.006/9.99/9.99 dbp=1.0 | TEST (not run))
- SL1.2/T1.5 mask[-] pm[-] guard={'min_slot': '10:30', 'max_slot': '14:00', 'top_n': 2}: **REJECT: TRAIN PF outside [1.30,1.80]** (TRAIN n=678 PF=0.411 net=Rs-300,333 win%=32.3 avgW=Rs959 avgL=Rs-1,112 maxDD=Rs-299,702 SL/TGT/EOD=291/142/245 tpd=11.69 domT/D/S=0.006/9.99/9.99 dbp=1.0 | TEST (not run))
- SL1.0/T1.5 mask[-] pm[-] guard={'min_slot': '11:00', 'max_slot': '14:00', 'top_n': 2}: **REJECT: TRAIN PF outside [1.30,1.80]** (TRAIN n=558 PF=0.388 net=Rs-249,518 win%=29.0 avgW=Rs976 avgL=Rs-1,029 maxDD=Rs-248,336 SL/TGT/EOD=289/107/162 tpd=9.62 domT/D/S=0.008/9.99/9.99 dbp=1.0 | TEST (not run))
- SL1.0/T2.5 mask[-] pm[sig5_adx_calc>=22.28014] guard={'min_slot': '10:00', 'top_n': 2}: **REJECT: TRAIN PF outside [1.30,1.80]** (TRAIN n=672 PF=0.394 net=Rs-320,035 win%=24.7 avgW=Rs1,256 avgL=Rs-1,045 maxDD=Rs-318,853 SL/TGT/EOD=381/65/226 tpd=12.68 domT/D/S=0.011/9.99/9.99 dbp=1.0 | TEST (not run))
- SL1.2/T1.5 mask[-] pm[-] guard={'min_slot': '09:45', 'max_slot': '13:00', 'top_n': 2}: **REJECT: TRAIN PF outside [1.30,1.80]** (TRAIN n=803 PF=0.428 net=Rs-349,843 win%=33.5 avgW=Rs974 avgL=Rs-1,146 maxDD=Rs-349,211 SL/TGT/EOD=361/178/264 tpd=14.6 domT/D/S=0.005/9.99/9.99 dbp=1.0 | TEST (not run))
- SL1.2/T1.5 mask[-] pm[sig5_vol_ratio20>=3.073591] guard={'min_slot': '09:45', 'max_slot': '14:00'}: **REJECT: TRAIN PF outside [1.30,1.80]** (TRAIN n=550 PF=0.384 net=Rs-264,861 win%=31.3 avgW=Rs960 avgL=Rs-1,138 maxDD=Rs-264,697 SL/TGT/EOD=246/113/191 tpd=10.78 domT/D/S=0.008/9.99/9.99 dbp=1.0 | TEST (not run))
- SL1.0/T1.25 mask[-] pm[-] guard={'min_slot': '11:00', 'top_n': 2}: **REJECT: TRAIN PF outside [1.30,1.80]** (TRAIN n=615 PF=0.369 net=Rs-274,840 win%=31.4 avgW=Rs832 avgL=Rs-1,032 maxDD=Rs-273,659 SL/TGT/EOD=307/140/168 tpd=10.6 domT/D/S=0.006/9.99/9.99 dbp=1.0 | TEST (not run))
- SL1.0/T1.25 mask[-] pm[-] guard={'min_slot': '10:00', 'top_n': 2}: **REJECT: TRAIN PF outside [1.30,1.80]** (TRAIN n=620 PF=0.365 net=Rs-280,141 win%=31.1 avgW=Rs833 avgL=Rs-1,032 maxDD=Rs-278,960 SL/TGT/EOD=311/140/169 tpd=10.69 domT/D/S=0.006/9.99/9.99 dbp=1.0 | TEST (not run))
- SL0.85/T2.5 mask[-] pm[-] guard={'max_slot': '13:00', 'top_n': 3}: **REJECT: TRAIN PF outside [1.30,1.80]** (TRAIN n=995 PF=0.469 net=Rs-374,614 win%=26.5 avgW=Rs1,252 avgL=Rs-965 maxDD=Rs-373,539 SL/TGT/EOD=607/103/285 tpd=18.09 domT/D/S=0.007/9.99/9.99 dbp=1.0 | TEST (not run))
- SL1.0/T1.25 mask[-] pm[pre5_mom_r>=0.388087] guard={'min_slot': '11:00', 'top_n': 2}: **REJECT: TRAIN PF outside [1.30,1.80]** (TRAIN n=555 PF=0.386 net=Rs-257,793 win%=32.1 avgW=Rs909 avgL=Rs-1,113 maxDD=Rs-259,605 SL/TGT/EOD=319/152/84 tpd=12.91 domT/D/S=0.006/9.99/9.99 dbp=1.0 | TEST (not run))