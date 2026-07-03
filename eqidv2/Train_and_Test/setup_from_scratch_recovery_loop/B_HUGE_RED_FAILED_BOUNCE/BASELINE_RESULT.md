# B_HUGE_RED_FAILED_BOUNCE (SHORT) — BASELINE_RESULT (recovery loop)

_Generated 2026-07-03. From-scratch recovery loop. Research-only; NO live trades; NO final_setup_conf.py edits._

- baseline spec: `{"entry": "market", "mask_terms": [], "premom_terms": [["pre3_close_pos", "<=", 0.581797], ["sig5_rsi_dir", "<=", 64.104659], ["pre5_mom_r", "<=", 0.284145]], "min_slot": null, "max_slot": null, "top_n": null, "be": null, "trail": null, "tstop": null, "max_positions": 20, "sl": 0.9, "tgt": 1.25}`

- **FIT**: n=22 PF=0.848 net=Rs-1,301 win%=50.0 SL/TGT/BE/TRAIL/TIME/EOD=6/6/0/0/0/10 tpd=2.2 tradeDom=0.14 dayDom=9.99 symDom=9.99 dbp=0.6214
- **VAL**: n=26 PF=0.621 net=Rs-4,499 win%=42.3 SL/TGT/BE/TRAIL/TIME/EOD=8/5/0/0/0/13 tpd=2.36 tradeDom=0.138 dayDom=9.99 symDom=9.99 dbp=0.8506
- **TRAIN**: n=48 PF=0.716 net=Rs-5,800 win%=45.8 SL/TGT/BE/TRAIL/TIME/EOD=14/11/0/0/0/23 tpd=2.29 tradeDom=0.07 dayDom=9.99 symDom=9.99 dbp=0.8313
- **TEST**: n=41 PF=0.72 net=Rs-4,573 win%=34.1 SL/TGT/BE/TRAIL/TIME/EOD=11/11/0/0/0/19 tpd=2.28 tradeDom=0.086 dayDom=9.99 symDom=9.99 dbp=0.8738