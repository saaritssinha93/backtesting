# L_TREND_PULLBACK - BASELINE_RESULT

- **Side:** LONG
- **Config source:** `final_setup_conf.py` / `Train_and_Test/final_setup_conf.py` `RESEARCH_WATCH_CONF` disabled reject.
- **Current live status:** not traded / not promoted.
- **FIT sessions:** 2026-05-19..2026-05-29 (8 sessions)
- **VAL sessions:** 2026-06-01..2026-06-16 (9 sessions)
- **TRAIN sessions:** 2026-05-19..2026-06-16 (17 sessions)
- **TEST sessions:** 2026-06-22..2026-06-24 (2 sessions)
- **Rows after entry attach:** FIT=54 VAL=91 TRAIN=145 TEST=6
- **Cost/slippage:** repo `setup_train_test.py`, statutory costs, 15 bps/leg slippage.

## Baselines
| config | TRAIN | TEST |
|---|---|---|
| baseline_v6_raw `SL=0.7 TGT=0.9 mask=[-] premom=[-] guard={}` | n=132 PF=0.2013 net=Rs-68,800 win=23.48% dbp=1.0 | n=6 PF=1.3066 net=Rs511 win=66.67% dbp=None |
| baseline_production_premom `SL=0.7 TGT=0.9 mask=[-] premom=[pre_entry_momentum_score>=73.021; pre2_mom_r>=0.233909] guard={}` | n=15 PF=0.1102 net=Rs-10,762 win=13.33% dbp=0.9993 | n=2 PF=0.2205 net=Rs-726 win=50.0% dbp=None |
| research_watch_best `SL=0.5 TGT=2.5 mask=[market_ret_pct>=-0.286] premom=[pre2_mom_r>=0.217] guard={}` | n=16 PF=0.3954 net=Rs-5,742 win=18.75% dbp=0.8578 | n=4 PF=3.6152 net=Rs1,913 win=75.0% dbp=None |

## Initial diagnosis
- Strict TEST has only 2 setup sessions, so all TEST results are thin and day-dominance-sensitive.
- The raw setup remains loss-making; earlier research-watch rescue is re-tested in this split rather than assumed.