# L_TREND_PULLBACK - ITERATION_LOG

Optimizer: Optuna TPE

## Iteration 001 - baseline_v6_raw
- **Changed group:** baseline
- **Parameters:** `SL=0.7 TGT=0.9 mask=[-] premom=[-] guard={}`
- **FIT:** n=47 PF=0.1185 net=Rs-28,449 win=17.02% dbp=1.0
- **VAL:** n=85 PF=0.2509 net=Rs-40,351 win=27.06% dbp=0.9943
- **TRAIN:** not run
- **TEST:** not run
- **Keep/reject:** FIT/VAL not reasonable enough for full TRAIN confirm

## Iteration 002 - baseline_production_premom
- **Changed group:** baseline
- **Parameters:** `SL=0.7 TGT=0.9 mask=[-] premom=[pre_entry_momentum_score>=73.021; pre2_mom_r>=0.233909] guard={}`
- **FIT:** n=7 PF=0.1194 net=Rs-4,916 win=14.29% dbp=0.8519
- **VAL:** n=8 PF=0.1023 net=Rs-5,846 win=12.5% dbp=1.0
- **TRAIN:** not run
- **TEST:** not run
- **Keep/reject:** FIT/VAL not reasonable enough for full TRAIN confirm

## Iteration 003 - research_watch_best
- **Changed group:** baseline
- **Parameters:** `SL=0.5 TGT=2.5 mask=[market_ret_pct>=-0.286] premom=[pre2_mom_r>=0.217] guard={}`
- **FIT:** n=5 PF=0.1595 net=Rs-2,451 win=20.0% dbp=0.8528
- **VAL:** n=11 PF=0.4998 net=Rs-3,291 win=18.18% dbp=0.7398
- **TRAIN:** not run
- **TEST:** not run
- **Keep/reject:** FIT/VAL not reasonable enough for full TRAIN confirm

## Iteration 004 - search
- **Changed group:** search-supported mask/pre-momentum/guard/exit
- **Parameters:** `SL=0.8 TGT=2.5 mask=[-] premom=[pre_entry_momentum_score<=61.042294] guard={"max_slot": "13:00", "min_slot": "11:00", "top_n": 2}`
- **FIT:** n=14 PF=0.0147 net=Rs-12,593 win=7.14% dbp=1.0
- **VAL:** n=15 PF=0.5751 net=Rs-4,465 win=20.0% dbp=0.8017
- **TRAIN:** not run
- **TEST:** not run
- **Keep/reject:** FIT/VAL not reasonable enough for full TRAIN confirm

## Iteration 005 - search
- **Changed group:** search-supported mask/pre-momentum/guard/exit
- **Parameters:** `SL=0.5 TGT=1.5 mask=[regime!=BEAR; quality_score<=14.654428] premom=[-] guard={"max_slot": "12:00", "min_slot": "10:30", "top_n": 2}`
- **FIT:** n=2 PF=0.0 net=Rs-1,461 win=0.0% dbp=None
- **VAL:** n=0 PF=0.0 net=Rs0 win=0.0% dbp=None
- **TRAIN:** not run
- **TEST:** not run
- **Keep/reject:** FIT/VAL not reasonable enough for full TRAIN confirm

## Iteration 006 - search
- **Changed group:** search-supported mask/pre-momentum/guard/exit
- **Parameters:** `SL=1.1 TGT=0.8 mask=[-] premom=[-] guard={"top_n": 3}`
- **FIT:** n=19 PF=0.1176 net=Rs-14,396 win=21.05% dbp=1.0
- **VAL:** n=20 PF=0.0373 net=Rs-19,451 win=10.0% dbp=0.9675
- **TRAIN:** not run
- **TEST:** not run
- **Keep/reject:** FIT/VAL not reasonable enough for full TRAIN confirm

## Iteration 007 - search
- **Changed group:** search-supported mask/pre-momentum/guard/exit
- **Parameters:** `SL=1.0 TGT=1.0 mask=[-] premom=[pre1_adx>=20.121826; sig5_vol_ratio20>=3.562664] guard={"min_slot": "11:00"}`
- **FIT:** n=7 PF=0.002 net=Rs-6,698 win=14.29% dbp=1.0
- **VAL:** n=5 PF=0.5329 net=Rs-947 win=40.0% dbp=0.7124
- **TRAIN:** not run
- **TEST:** not run
- **Keep/reject:** FIT/VAL not reasonable enough for full TRAIN confirm

## Iteration 008 - search
- **Changed group:** search-supported mask/pre-momentum/guard/exit
- **Parameters:** `SL=1.1 TGT=0.6 mask=[-] premom=[pre3_close_pos<=0.473496; pre1_adx<=20.121826] guard={"top_n": 1}`
- **FIT:** n=5 PF=0.0865 net=Rs-3,871 win=20.0% dbp=0.9936
- **VAL:** n=1 PF=0.0 net=Rs-752 win=0.0% dbp=None
- **TRAIN:** not run
- **TEST:** not run
- **Keep/reject:** FIT/VAL not reasonable enough for full TRAIN confirm

## Iteration 009 - search
- **Changed group:** search-supported mask/pre-momentum/guard/exit
- **Parameters:** `SL=0.7 TGT=0.8 mask=[-] premom=[-] guard={"max_slot": "13:00"}`
- **FIT:** n=11 PF=0.1354 net=Rs-7,233 win=18.18% dbp=0.9993
- **VAL:** n=12 PF=0.0 net=Rs-10,567 win=0.0% dbp=1.0
- **TRAIN:** not run
- **TEST:** not run
- **Keep/reject:** FIT/VAL not reasonable enough for full TRAIN confirm

## Iteration 010 - search
- **Changed group:** search-supported mask/pre-momentum/guard/exit
- **Parameters:** `SL=1.1 TGT=1.75 mask=[regime!=NEUTRAL] premom=[pre_entry_momentum_score<=77.840375; pre_entry_momentum_score>=69.605367] guard={"min_slot": "10:00"}`
- **FIT:** n=1 PF=None net=Rs465 win=100.0% dbp=None
- **VAL:** n=4 PF=0.0 net=Rs-4,150 win=0.0% dbp=1.0
- **TRAIN:** not run
- **TEST:** not run
- **Keep/reject:** FIT/VAL not reasonable enough for full TRAIN confirm

## Iteration 011 - search
- **Changed group:** search-supported mask/pre-momentum/guard/exit
- **Parameters:** `SL=1.1 TGT=0.9 mask=[vol_ratio>=1.972569] premom=[-] guard={"min_slot": "10:00", "top_n": 1}`
- **FIT:** n=7 PF=0.0947 net=Rs-6,369 win=14.29% dbp=1.0
- **VAL:** n=9 PF=0.0203 net=Rs-9,380 win=11.11% dbp=0.9624
- **TRAIN:** not run
- **TEST:** not run
- **Keep/reject:** FIT/VAL not reasonable enough for full TRAIN confirm

## Iteration 012 - search
- **Changed group:** search-supported mask/pre-momentum/guard/exit
- **Parameters:** `SL=1.1 TGT=1.75 mask=[regime!=NEUTRAL; quality_score>=14.654428] premom=[sig5_vol_ratio20>=1.336049; sig5_rsi_dir>=38.663] guard={"min_slot": "10:00"}`
- **FIT:** n=25 PF=0.0366 net=Rs-18,360 win=12.0% dbp=1.0
- **VAL:** n=26 PF=0.3447 net=Rs-13,527 win=26.92% dbp=0.9819
- **TRAIN:** not run
- **TEST:** not run
- **Keep/reject:** FIT/VAL not reasonable enough for full TRAIN confirm

## Iteration 013 - search
- **Changed group:** search-supported mask/pre-momentum/guard/exit
- **Parameters:** `SL=1.5 TGT=0.6 mask=[market_ret_pct<=-0.649781; quality_score<=27.52281] premom=[-] guard={"max_slot": "14:00", "top_n": 2}`
- **FIT:** n=0 PF=0.0 net=Rs0 win=0.0% dbp=None
- **VAL:** n=2 PF=0.2116 net=Rs-1,363 win=50.0% dbp=None
- **TRAIN:** not run
- **TEST:** not run
- **Keep/reject:** FIT/VAL not reasonable enough for full TRAIN confirm

## Iteration 014 - search
- **Changed group:** search-supported mask/pre-momentum/guard/exit
- **Parameters:** `SL=1.2 TGT=0.8 mask=[upper_wick_pct>=0.110229] premom=[sig5_rsi_dir>=38.663] guard={"top_n": 3}`
- **FIT:** n=4 PF=0.132 net=Rs-3,724 win=25.0% dbp=None
- **VAL:** n=2 PF=0.0 net=Rs-2,864 win=0.0% dbp=None
- **TRAIN:** not run
- **TEST:** not run
- **Keep/reject:** FIT/VAL not reasonable enough for full TRAIN confirm

## Iteration 015 - search
- **Changed group:** search-supported mask/pre-momentum/guard/exit
- **Parameters:** `SL=0.4 TGT=0.9 mask=[vol_ratio>=1.972569] premom=[-] guard={"top_n": 1}`
- **FIT:** n=7 PF=0.0 net=Rs-4,421 win=0.0% dbp=1.0
- **VAL:** n=9 PF=0.0 net=Rs-5,678 win=0.0% dbp=1.0
- **TRAIN:** not run
- **TEST:** not run
- **Keep/reject:** FIT/VAL not reasonable enough for full TRAIN confirm

## Iteration 016 - search
- **Changed group:** search-supported mask/pre-momentum/guard/exit
- **Parameters:** `SL=0.6 TGT=0.9 mask=[vol_ratio>=2.473232] premom=[pre2_mom_r>=0.201646] guard={"min_slot": "09:30", "top_n": 3}`
- **FIT:** n=2 PF=0.8064 net=Rs-160 win=50.0% dbp=None
- **VAL:** n=4 PF=0.0 net=Rs-3,318 win=0.0% dbp=None
- **TRAIN:** not run
- **TEST:** not run
- **Keep/reject:** FIT/VAL not reasonable enough for full TRAIN confirm

## Iteration 017 - search
- **Changed group:** search-supported mask/pre-momentum/guard/exit
- **Parameters:** `SL=0.9 TGT=2.0 mask=[signal_range_pct<=0.322982] premom=[-] guard={"top_n": 3}`
- **FIT:** n=11 PF=0.125 net=Rs-7,124 win=18.18% dbp=1.0
- **VAL:** n=14 PF=0.0895 net=Rs-11,500 win=7.14% dbp=0.9581
- **TRAIN:** not run
- **TEST:** not run
- **Keep/reject:** FIT/VAL not reasonable enough for full TRAIN confirm

## Iteration 018 - search
- **Changed group:** search-supported mask/pre-momentum/guard/exit
- **Parameters:** `SL=0.9 TGT=2.0 mask=[-] premom=[sig5_adx_calc<=20.112432] guard={"top_n": 3}`
- **FIT:** n=10 PF=0.1431 net=Rs-6,090 win=20.0% dbp=0.9994
- **VAL:** n=13 PF=0.2407 net=Rs-8,502 win=15.38% dbp=0.9023
- **TRAIN:** not run
- **TEST:** not run
- **Keep/reject:** FIT/VAL not reasonable enough for full TRAIN confirm

## Iteration 019 - search
- **Changed group:** search-supported mask/pre-momentum/guard/exit
- **Parameters:** `SL=0.9 TGT=2.0 mask=[signal_range_pct<=0.322982] premom=[sig5_adx_calc<=20.112432] guard={"top_n": 3}`
- **FIT:** n=7 PF=0.2367 net=Rs-3,280 win=28.57% dbp=0.9959
- **VAL:** n=11 PF=0.1123 net=Rs-8,935 win=9.09% dbp=0.9466
- **TRAIN:** not run
- **TEST:** not run
- **Keep/reject:** FIT/VAL not reasonable enough for full TRAIN confirm

## Iteration 020 - search
- **Changed group:** search-supported mask/pre-momentum/guard/exit
- **Parameters:** `SL=0.9 TGT=2.0 mask=[-] premom=[pre5_mom_r<=0.382995] guard={"top_n": 3}`
- **FIT:** n=17 PF=0.1385 net=Rs-10,396 win=23.53% dbp=1.0
- **VAL:** n=20 PF=0.1921 net=Rs-13,861 win=15.0% dbp=0.9293
- **TRAIN:** not run
- **TEST:** not run
- **Keep/reject:** FIT/VAL not reasonable enough for full TRAIN confirm

## Iteration 021 - search
- **Changed group:** search-supported mask/pre-momentum/guard/exit
- **Parameters:** `SL=0.9 TGT=1.2 mask=[-] premom=[pre5_mom_r<=0.382995] guard={"max_slot": "12:30", "top_n": 3}`
- **FIT:** n=8 PF=0.1933 net=Rs-4,817 win=25.0% dbp=0.949
- **VAL:** n=10 PF=0.108 net=Rs-7,887 win=10.0% dbp=0.9777
- **TRAIN:** not run
- **TEST:** not run
- **Keep/reject:** FIT/VAL not reasonable enough for full TRAIN confirm

## Iteration 022 - search
- **Changed group:** search-supported mask/pre-momentum/guard/exit
- **Parameters:** `SL=0.6 TGT=2.0 mask=[-] premom=[pre5_mom_r<=0.382995] guard={"top_n": 3}`
- **FIT:** n=15 PF=0.1326 net=Rs-7,885 win=20.0% dbp=1.0
- **VAL:** n=17 PF=0.3078 net=Rs-7,410 win=17.65% dbp=0.8773
- **TRAIN:** not run
- **TEST:** not run
- **Keep/reject:** FIT/VAL not reasonable enough for full TRAIN confirm

## Iteration 023 - search
- **Changed group:** search-supported mask/pre-momentum/guard/exit
- **Parameters:** `SL=0.9 TGT=2.0 mask=[-] premom=[pre3_range_r<=0.355727] guard={"top_n": 3}`
- **FIT:** n=18 PF=0.1266 net=Rs-11,526 win=22.22% dbp=1.0
- **VAL:** n=18 PF=0.2212 net=Rs-11,600 win=16.67% dbp=0.9097
- **TRAIN:** not run
- **TEST:** not run
- **Keep/reject:** FIT/VAL not reasonable enough for full TRAIN confirm

## Iteration 024 - search
- **Changed group:** search-supported mask/pre-momentum/guard/exit
- **Parameters:** `SL=0.4 TGT=1.2 mask=[-] premom=[pre5_mom_r<=0.382995] guard={"top_n": 3}`
- **FIT:** n=14 PF=0.1629 net=Rs-6,092 win=14.29% dbp=1.0
- **VAL:** n=10 PF=0.0 net=Rs-6,310 win=0.0% dbp=1.0
- **TRAIN:** not run
- **TEST:** not run
- **Keep/reject:** FIT/VAL not reasonable enough for full TRAIN confirm

## Iteration 025 - search
- **Changed group:** search-supported mask/pre-momentum/guard/exit
- **Parameters:** `SL=1.2 TGT=1.0 mask=[-] premom=[pre5_mom_r<=0.382995; sig5_adx_calc<=20.112432] guard={"top_n": 3}`
- **FIT:** n=9 PF=0.1272 net=Rs-6,760 win=22.22% dbp=0.9994
- **VAL:** n=13 PF=0.115 net=Rs-11,654 win=15.38% dbp=0.9426
- **TRAIN:** not run
- **TEST:** not run
- **Keep/reject:** FIT/VAL not reasonable enough for full TRAIN confirm

## Iteration 026 - search
- **Changed group:** search-supported mask/pre-momentum/guard/exit
- **Parameters:** `SL=1.0 TGT=1.5 mask=[-] premom=[pre5_mom_r<=0.382995] guard={"max_slot": "14:30", "top_n": 3}`
- **FIT:** n=34 PF=0.0769 net=Rs-25,685 win=11.76% dbp=1.0
- **VAL:** n=64 PF=0.3554 net=Rs-29,550 win=26.56% dbp=0.9578
- **TRAIN:** not run
- **TEST:** not run
- **Keep/reject:** FIT/VAL not reasonable enough for full TRAIN confirm

## Iteration 027 - search
- **Changed group:** search-supported mask/pre-momentum/guard/exit
- **Parameters:** `SL=0.7 TGT=2.5 mask=[-] premom=[sig5_vol_ratio20<=0.873792] guard={"top_n": 2}`
- **FIT:** n=1 PF=None net=Rs188 win=100.0% dbp=None
- **VAL:** n=10 PF=0.1874 net=Rs-6,786 win=10.0% dbp=0.9367
- **TRAIN:** not run
- **TEST:** not run
- **Keep/reject:** FIT/VAL not reasonable enough for full TRAIN confirm

## Iteration 028 - search
- **Changed group:** search-supported mask/pre-momentum/guard/exit
- **Parameters:** `SL=0.5 TGT=2.0 mask=[-] premom=[pre3_close_pos<=1.0; pre2_mom_r<=0.130511] guard={"top_n": 1}`
- **FIT:** n=15 PF=0.1117 net=Rs-8,094 win=13.33% dbp=1.0
- **VAL:** n=10 PF=0.0912 net=Rs-5,977 win=10.0% dbp=0.98
- **TRAIN:** not run
- **TEST:** not run
- **Keep/reject:** FIT/VAL not reasonable enough for full TRAIN confirm

## Iteration 029 - search
- **Changed group:** search-supported mask/pre-momentum/guard/exit
- **Parameters:** `SL=0.8 TGT=2.5 mask=[-] premom=[pre1_adx>=26.866387] guard={"max_slot": "14:00", "top_n": 2}`
- **FIT:** n=16 PF=0.0138 net=Rs-13,460 win=6.25% dbp=0.9998
- **VAL:** n=25 PF=0.2041 net=Rs-14,281 win=24.0% dbp=0.9961
- **TRAIN:** not run
- **TEST:** not run
- **Keep/reject:** FIT/VAL not reasonable enough for full TRAIN confirm

## Iteration 030 - search
- **Changed group:** search-supported mask/pre-momentum/guard/exit
- **Parameters:** `SL=1.5 TGT=2.0 mask=[regime==BULL] premom=[pre3_range_r<=0.1695] guard={"top_n": 3}`
- **FIT:** n=8 PF=0.1658 net=Rs-4,387 win=37.5% dbp=1.0
- **VAL:** n=9 PF=0.0 net=Rs-12,209 win=0.0% dbp=None
- **TRAIN:** not run
- **TEST:** not run
- **Keep/reject:** FIT/VAL not reasonable enough for full TRAIN confirm

## Iteration 031 - search
- **Changed group:** search-supported mask/pre-momentum/guard/exit
- **Parameters:** `SL=0.8 TGT=1.5 mask=[-] premom=[pre2_mom_r<=-0.109837] guard={"min_slot": "09:45", "top_n": 3}`
- **FIT:** n=1 PF=0.0 net=Rs-1,032 win=0.0% dbp=None
- **VAL:** n=1 PF=0.0 net=Rs-1,030 win=0.0% dbp=None
- **TRAIN:** not run
- **TEST:** not run
- **Keep/reject:** FIT/VAL not reasonable enough for full TRAIN confirm

## Iteration 032 - search
- **Changed group:** search-supported mask/pre-momentum/guard/exit
- **Parameters:** `SL=0.9 TGT=2.0 mask=[-] premom=[sig5_rsi_dir<=50.915141] guard={"top_n": 2}`
- **FIT:** n=8 PF=0.0 net=Rs-7,659 win=0.0% dbp=1.0
- **VAL:** n=15 PF=0.1421 net=Rs-10,617 win=13.33% dbp=0.9124
- **TRAIN:** not run
- **TEST:** not run
- **Keep/reject:** FIT/VAL not reasonable enough for full TRAIN confirm

## Iteration 033 - search
- **Changed group:** search-supported mask/pre-momentum/guard/exit
- **Parameters:** `SL=0.5 TGT=1.0 mask=[-] premom=[pre5_mom_r<=0.487279; pre3_close_pos>=0.716959] guard={"max_slot": "12:00", "top_n": 3}`
- **FIT:** n=6 PF=0.0 net=Rs-4,125 win=0.0% dbp=1.0
- **VAL:** n=7 PF=0.0 net=Rs-5,018 win=0.0% dbp=1.0
- **TRAIN:** not run
- **TEST:** not run
- **Keep/reject:** FIT/VAL not reasonable enough for full TRAIN confirm

## Iteration 034 - search
- **Changed group:** search-supported mask/pre-momentum/guard/exit
- **Parameters:** `SL=0.9 TGT=0.6 mask=[-] premom=[-] guard={}`
- **FIT:** n=20 PF=0.1334 net=Rs-11,910 win=25.0% dbp=1.0
- **VAL:** n=21 PF=0.0509 net=Rs-17,118 win=14.29% dbp=0.9878
- **TRAIN:** not run
- **TEST:** not run
- **Keep/reject:** FIT/VAL not reasonable enough for full TRAIN confirm

## Iteration 035 - search
- **Changed group:** search-supported mask/pre-momentum/guard/exit
- **Parameters:** `SL=1.0 TGT=1.75 mask=[-] premom=[pre_entry_momentum_score<=61.042294] guard={"min_slot": "09:30", "top_n": 1}`
- **FIT:** n=15 PF=0.1591 net=Rs-10,162 win=20.0% dbp=1.0
- **VAL:** n=8 PF=0.2859 net=Rs-4,322 win=25.0% dbp=0.8782
- **TRAIN:** not run
- **TEST:** not run
- **Keep/reject:** FIT/VAL not reasonable enough for full TRAIN confirm

## Iteration 036 - search
- **Changed group:** search-supported mask/pre-momentum/guard/exit
- **Parameters:** `SL=1.0 TGT=1.75 mask=[-] premom=[pre_entry_momentum_score>=61.042294; pre5_mom_r<=0.487279] guard={"max_slot": "14:30", "min_slot": "09:30", "top_n": 1}`
- **FIT:** n=13 PF=0.045 net=Rs-9,867 win=7.69% dbp=0.9948
- **VAL:** n=37 PF=0.2401 net=Rs-21,804 win=27.03% dbp=0.9451
- **TRAIN:** not run
- **TEST:** not run
- **Keep/reject:** FIT/VAL not reasonable enough for full TRAIN confirm

## Iteration 037 - search
- **Changed group:** search-supported mask/pre-momentum/guard/exit
- **Parameters:** `SL=1.0 TGT=1.75 mask=[-] premom=[-] guard={"min_slot": "09:30", "top_n": 1}`
- **FIT:** n=18 PF=0.1695 net=Rs-11,706 win=22.22% dbp=1.0
- **VAL:** n=19 PF=0.0728 net=Rs-16,875 win=10.53% dbp=0.9461
- **TRAIN:** not run
- **TEST:** not run
- **Keep/reject:** FIT/VAL not reasonable enough for full TRAIN confirm

## Iteration 038 - search
- **Changed group:** search-supported mask/pre-momentum/guard/exit
- **Parameters:** `SL=1.0 TGT=1.75 mask=[regime==BULL; regime!=NEUTRAL] premom=[pre_entry_momentum_score<=61.042294] guard={"min_slot": "09:30", "top_n": 1}`
- **FIT:** n=6 PF=0.1254 net=Rs-2,836 win=33.33% dbp=None
- **VAL:** n=5 PF=0.0 net=Rs-5,636 win=0.0% dbp=None
- **TRAIN:** not run
- **TEST:** not run
- **Keep/reject:** FIT/VAL not reasonable enough for full TRAIN confirm

## Iteration 039 - search
- **Changed group:** search-supported mask/pre-momentum/guard/exit
- **Parameters:** `SL=1.0 TGT=1.75 mask=[-] premom=[pre_entry_momentum_score>=61.042294] guard={"min_slot": "09:30", "top_n": 1}`
- **FIT:** n=4 PF=0.1436 net=Rs-2,775 win=25.0% dbp=None
- **VAL:** n=14 PF=0.0138 net=Rs-13,894 win=7.14% dbp=0.9955
- **TRAIN:** not run
- **TEST:** not run
- **Keep/reject:** FIT/VAL not reasonable enough for full TRAIN confirm

## Iteration 040 - search
- **Changed group:** search-supported mask/pre-momentum/guard/exit
- **Parameters:** `SL=1.0 TGT=1.75 mask=[regime==BEAR] premom=[-] guard={"min_slot": "09:30", "top_n": 1}`
- **FIT:** n=25 PF=0.0074 net=Rs-20,893 win=8.0% dbp=1.0
- **VAL:** n=44 PF=0.4277 net=Rs-17,381 win=31.82% dbp=0.8805
- **TRAIN:** not run
- **TEST:** not run
- **Keep/reject:** FIT/VAL not reasonable enough for full TRAIN confirm

## Iteration 041 - search
- **Changed group:** search-supported mask/pre-momentum/guard/exit
- **Parameters:** `SL=1.0 TGT=1.75 mask=[regime==NEUTRAL] premom=[pre_entry_momentum_score<=61.042294] guard={"min_slot": "09:30", "top_n": 1}`
- **FIT:** n=9 PF=0.1715 net=Rs-7,326 win=11.11% dbp=1.0
- **VAL:** n=3 PF=4.1568 net=Rs1,314 win=66.67% dbp=0.1481
- **TRAIN:** not run
- **TEST:** not run
- **Keep/reject:** FIT/VAL not reasonable enough for full TRAIN confirm

## Iteration 042 - search
- **Changed group:** search-supported mask/pre-momentum/guard/exit
- **Parameters:** `SL=1.0 TGT=0.8 mask=[-] premom=[pre_entry_momentum_score<=61.042294] guard={"min_slot": "09:30", "top_n": 1}`
- **FIT:** n=15 PF=0.1118 net=Rs-10,734 win=20.0% dbp=1.0
- **VAL:** n=8 PF=0.1858 net=Rs-4,927 win=25.0% dbp=0.9357
- **TRAIN:** not run
- **TEST:** not run
- **Keep/reject:** FIT/VAL not reasonable enough for full TRAIN confirm

## Iteration 043 - search
- **Changed group:** search-supported mask/pre-momentum/guard/exit
- **Parameters:** `SL=1.0 TGT=1.75 mask=[-] premom=[pre_entry_momentum_score>=61.042294] guard={"min_slot": "09:30", "top_n": 1}`
- **FIT:** n=4 PF=0.1436 net=Rs-2,775 win=25.0% dbp=None
- **VAL:** n=14 PF=0.0138 net=Rs-13,894 win=7.14% dbp=0.9955
- **TRAIN:** not run
- **TEST:** not run
- **Keep/reject:** FIT/VAL not reasonable enough for full TRAIN confirm

## Iteration 044 - search
- **Changed group:** search-supported mask/pre-momentum/guard/exit
- **Parameters:** `SL=1.1 TGT=1.75 mask=[-] premom=[pre_entry_momentum_score<=61.042294] guard={"max_slot": "12:30", "min_slot": "09:45", "top_n": 1}`
- **FIT:** n=7 PF=0.0285 net=Rs-6,408 win=14.29% dbp=1.0
- **VAL:** n=2 PF=0.0 net=Rs-2,662 win=0.0% dbp=None
- **TRAIN:** not run
- **TEST:** not run
- **Keep/reject:** FIT/VAL not reasonable enough for full TRAIN confirm

## Iteration 045 - search
- **Changed group:** search-supported mask/pre-momentum/guard/exit
- **Parameters:** `SL=0.6 TGT=0.9 mask=[-] premom=[pre_entry_momentum_score<=61.042294] guard={"min_slot": "10:30", "top_n": 1}`
- **FIT:** n=12 PF=0.1626 net=Rs-5,527 win=25.0% dbp=1.0
- **VAL:** n=5 PF=0.1803 net=Rs-2,727 win=20.0% dbp=0.8529
- **TRAIN:** not run
- **TEST:** not run
- **Keep/reject:** FIT/VAL not reasonable enough for full TRAIN confirm

## Iteration 046 - search
- **Changed group:** search-supported mask/pre-momentum/guard/exit
- **Parameters:** `SL=0.6 TGT=0.9 mask=[-] premom=[sig5_adx_calc<=20.112432] guard={"min_slot": "10:30"}`
- **FIT:** n=8 PF=0.1978 net=Rs-3,589 win=25.0% dbp=0.984
- **VAL:** n=12 PF=0.0755 net=Rs-8,071 win=8.33% dbp=0.9782
- **TRAIN:** not run
- **TEST:** not run
- **Keep/reject:** FIT/VAL not reasonable enough for full TRAIN confirm

## Iteration 047 - search
- **Changed group:** search-supported mask/pre-momentum/guard/exit
- **Parameters:** `SL=0.6 TGT=0.9 mask=[-] premom=[pre1_adx<=20.121826] guard={"min_slot": "10:30", "top_n": 1}`
- **FIT:** n=9 PF=0.3462 net=Rs-2,931 win=33.33% dbp=0.962
- **VAL:** n=8 PF=0.0 net=Rs-5,723 win=0.0% dbp=1.0
- **TRAIN:** not run
- **TEST:** not run
- **Keep/reject:** FIT/VAL not reasonable enough for full TRAIN confirm

## Iteration 048 - search
- **Changed group:** search-supported mask/pre-momentum/guard/exit
- **Parameters:** `SL=0.7 TGT=0.9 mask=[-] premom=[pre3_close_pos<=0.473496] guard={"min_slot": "10:30", "top_n": 1}`
- **FIT:** n=6 PF=0.0 net=Rs-5,103 win=0.0% dbp=1.0
- **VAL:** n=3 PF=0.322 net=Rs-1,263 win=33.33% dbp=0.7439
- **TRAIN:** not run
- **TEST:** not run
- **Keep/reject:** FIT/VAL not reasonable enough for full TRAIN confirm

## Iteration 049 - search
- **Changed group:** search-supported mask/pre-momentum/guard/exit
- **Parameters:** `SL=0.6 TGT=1.75 mask=[-] premom=[pre_entry_momentum_score<=61.042294] guard={"min_slot": "09:30", "top_n": 1}`
- **FIT:** n=14 PF=0.2327 net=Rs-6,342 win=21.43% dbp=1.0
- **VAL:** n=5 PF=0.1803 net=Rs-2,727 win=20.0% dbp=0.8529
- **TRAIN:** not run
- **TEST:** not run
- **Keep/reject:** FIT/VAL not reasonable enough for full TRAIN confirm

## Iteration 050 - search
- **Changed group:** search-supported mask/pre-momentum/guard/exit
- **Parameters:** `SL=0.6 TGT=1.2 mask=[-] premom=[pre_entry_momentum_score<=61.042294] guard={"min_slot": "11:00", "top_n": 1}`
- **FIT:** n=12 PF=0.2081 net=Rs-5,228 win=25.0% dbp=1.0
- **VAL:** n=5 PF=0.1803 net=Rs-2,727 win=20.0% dbp=0.8529
- **TRAIN:** not run
- **TEST:** not run
- **Keep/reject:** FIT/VAL not reasonable enough for full TRAIN confirm

## Iteration 051 - search
- **Changed group:** search-supported mask/pre-momentum/guard/exit
- **Parameters:** `SL=0.6 TGT=1.2 mask=[-] premom=[sig5_vol_ratio20<=3.562664] guard={"min_slot": "11:00", "top_n": 1}`
- **FIT:** n=13 PF=0.185 net=Rs-6,052 win=23.08% dbp=1.0
- **VAL:** n=20 PF=0.0643 net=Rs-13,891 win=5.0% dbp=0.9827
- **TRAIN:** not run
- **TEST:** not run
- **Keep/reject:** FIT/VAL not reasonable enough for full TRAIN confirm

## Iteration 052 - search
- **Changed group:** search-supported mask/pre-momentum/guard/exit
- **Parameters:** `SL=0.6 TGT=1.2 mask=[-] premom=[sig5_rsi_dir<=38.663] guard={"min_slot": "11:00", "top_n": 1}`
- **FIT:** n=2 PF=0.0 net=Rs-1,662 win=0.0% dbp=None
- **VAL:** n=9 PF=0.0071 net=Rs-6,593 win=11.11% dbp=0.9962
- **TRAIN:** not run
- **TEST:** not run
- **Keep/reject:** FIT/VAL not reasonable enough for full TRAIN confirm

## Iteration 053 - search
- **Changed group:** search-supported mask/pre-momentum/guard/exit
- **Parameters:** `SL=0.6 TGT=1.2 mask=[-] premom=[pre2_mom_r>=0.201646] guard={"min_slot": "11:00", "top_n": 1}`
- **FIT:** n=3 PF=0.5832 net=Rs-690 win=33.33% dbp=None
- **VAL:** n=7 PF=0.0 net=Rs-5,303 win=0.0% dbp=1.0
- **TRAIN:** not run
- **TEST:** not run
- **Keep/reject:** FIT/VAL not reasonable enough for full TRAIN confirm

## Iteration 054 - search
- **Changed group:** search-supported mask/pre-momentum/guard/exit
- **Parameters:** `SL=0.6 TGT=1.2 mask=[-] premom=[pre_entry_momentum_score<=61.042294] guard={"max_slot": "14:30", "min_slot": "11:00"}`
- **FIT:** n=12 PF=0.2081 net=Rs-5,228 win=25.0% dbp=1.0
- **VAL:** n=5 PF=0.1803 net=Rs-2,727 win=20.0% dbp=0.8529
- **TRAIN:** not run
- **TEST:** not run
- **Keep/reject:** FIT/VAL not reasonable enough for full TRAIN confirm

## Iteration 055 - search
- **Changed group:** search-supported mask/pre-momentum/guard/exit
- **Parameters:** `SL=0.6 TGT=1.2 mask=[-] premom=[pre_entry_momentum_score<=67.157686] guard={"max_slot": "14:30", "min_slot": "11:00"}`
- **FIT:** n=12 PF=0.2081 net=Rs-5,228 win=25.0% dbp=1.0
- **VAL:** n=10 PF=0.2498 net=Rs-4,670 win=20.0% dbp=0.8947
- **TRAIN:** not run
- **TEST:** not run
- **Keep/reject:** FIT/VAL not reasonable enough for full TRAIN confirm

## Iteration 056 - search
- **Changed group:** search-supported mask/pre-momentum/guard/exit
- **Parameters:** `SL=0.6 TGT=1.2 mask=[-] premom=[pre3_range_r<=0.355727] guard={"max_slot": "14:30", "min_slot": "11:00"}`
- **FIT:** n=12 PF=0.2081 net=Rs-5,228 win=25.0% dbp=1.0
- **VAL:** n=8 PF=0.0 net=Rs-5,714 win=0.0% dbp=1.0
- **TRAIN:** not run
- **TEST:** not run
- **Keep/reject:** FIT/VAL not reasonable enough for full TRAIN confirm

## Iteration 057 - search
- **Changed group:** search-supported mask/pre-momentum/guard/exit
- **Parameters:** `SL=0.6 TGT=1.2 mask=[-] premom=[sig5_adx_calc<=22.52031] guard={"max_slot": "14:30", "min_slot": "11:00"}`
- **FIT:** n=11 PF=0.2238 net=Rs-4,764 win=27.27% dbp=0.9997
- **VAL:** n=12 PF=0.1094 net=Rs-7,775 win=8.33% dbp=0.9684
- **TRAIN:** not run
- **TEST:** not run
- **Keep/reject:** FIT/VAL not reasonable enough for full TRAIN confirm
