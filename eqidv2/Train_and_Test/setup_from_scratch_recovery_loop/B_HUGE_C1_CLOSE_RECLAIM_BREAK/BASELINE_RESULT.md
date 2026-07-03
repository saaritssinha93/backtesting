# B_HUGE_C1_CLOSE_RECLAIM_BREAK (LONG) — BASELINE_RESULT (recovery loop)

_Generated 2026-07-03. From-scratch recovery loop. Research-only; NO live trades; NO final_setup_conf.py edits._

- baseline spec: `{"entry": "market", "mask_terms": [["regime", "!=", "BULL"]], "premom_terms": [], "min_slot": null, "max_slot": null, "top_n": null, "be": null, "trail": null, "tstop": null, "max_positions": 20, "sl": 1.0, "tgt": 1.5}`

- **FIT**: n=423 PF=0.545 net=Rs-125,310 win%=35.2 SL/TGT/BE/TRAIL/TIME/EOD=189/108/0/0/0/126 tpd=15.67 tradeDom=0.011 dayDom=9.99 symDom=9.99 dbp=0.9996
- **VAL**: n=334 PF=0.486 net=Rs-108,129 win%=33.2 SL/TGT/BE/TRAIL/TIME/EOD=135/70/0/0/0/129 tpd=17.58 tradeDom=0.012 dayDom=9.99 symDom=9.99 dbp=1.0
- **TRAIN**: n=757 PF=0.518 net=Rs-234,044 win%=34.3 SL/TGT/BE/TRAIL/TIME/EOD=324/178/0/0/0/255 tpd=16.46 tradeDom=0.006 dayDom=9.99 symDom=9.99 dbp=1.0
- **TEST**: n=306 PF=0.599 net=Rs-67,468 win%=39.2 SL/TGT/BE/TRAIL/TIME/EOD=109/63/0/0/0/134 tpd=15.3 tradeDom=0.013 dayDom=9.99 symDom=9.99 dbp=0.9779