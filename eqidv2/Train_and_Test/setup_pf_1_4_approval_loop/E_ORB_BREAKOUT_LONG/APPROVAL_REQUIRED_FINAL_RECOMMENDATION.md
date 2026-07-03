# E_ORB_BREAKOUT_LONG — APPROVAL_REQUIRED_FINAL_RECOMMENDATION

**Approval recommendation: NO**

> DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES.

## Best candidate
- none cleared the band on both TRAIN and TEST with acceptable stability.
- The proposed action is to KEEP the setup PARKED (`enabled=False`); no config edit.

## Rerun command
```
py -3.12 Train_and_Test/setup_pf_1_4_approval_loop/E_ORB_BREAKOUT_LONG/scripts/pf14_approval_loop.py --setup E_ORB_BREAKOUT_LONG --pool C:\TradingData\eqidv2\outputs_ID_v11_conf_fresh_20260629 --train_start 2026-05-18 --test_start 2026-06-20 --trials 500 --seed 7
```

## Risk notes
- TEST window is only 3 sessions (2026-06-22..2026-06-24) — cannot satisfy 'no single day dominates'; any TEST PF is low-confidence.
- Basis: native=SCREENING-ONLY (firehose; v11 conf backtest is the live-faithful arbiter).
- Coordinate loop is greedy (local optima possible); Optuna global search mitigates but does not eliminate this.
- No live trades; no final_setup_conf.py edit performed by this script.