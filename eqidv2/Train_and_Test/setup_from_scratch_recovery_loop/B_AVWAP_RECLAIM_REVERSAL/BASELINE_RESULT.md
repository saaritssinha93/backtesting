# B_AVWAP_RECLAIM_REVERSAL (LONG) — BASELINE_RESULT (recovery loop)

_Generated 2026-07-03. From-scratch recovery loop. Research-only; NO live trades; NO final_setup_conf.py edits._

- baseline spec: `{"entry": "market", "mask_terms": [["vwap_dist_atr", "<=", 1.0]], "premom_terms": [], "min_slot": null, "max_slot": null, "top_n": null, "be": null, "trail": null, "tstop": null, "max_positions": 20, "sl": 0.7, "tgt": 1.5}`

- **FIT**: n=1006 PF=0.379 net=Rs-377,889 win%=25.3 SL/TGT/BE/TRAIL/TIME/EOD=592/147/0/0/0/267 tpd=32.45 tradeDom=0.005 dayDom=9.99 symDom=9.99 dbp=1.0
- **VAL**: n=779 PF=0.291 net=Rs-341,181 win%=22.0 SL/TGT/BE/TRAIL/TIME/EOD=459/87/0/0/0/233 tpd=37.1 tradeDom=0.009 dayDom=9.99 symDom=9.99 dbp=1.0
- **TRAIN**: n=1785 PF=0.347 net=Rs-706,168 win%=24.1 SL/TGT/BE/TRAIL/TIME/EOD=1038/237/0/0/0/510 tpd=34.33 tradeDom=0.003 dayDom=9.99 symDom=9.99 dbp=1.0
- **TEST**: n=744 PF=0.319 net=Rs-311,808 win%=23.7 SL/TGT/BE/TRAIL/TIME/EOD=441/91/0/0/0/212 tpd=33.82 tradeDom=0.009 dayDom=9.99 symDom=9.99 dbp=1.0