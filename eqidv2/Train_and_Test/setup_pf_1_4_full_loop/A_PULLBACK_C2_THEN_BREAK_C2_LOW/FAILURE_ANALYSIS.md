# A_PULLBACK_C2_THEN_BREAK_C2_LOW - FAILURE_ANALYSIS

## Losing Trade And Weakness Classification

- Iteration outcomes: {'REJECT': 200}
- Common failure classes are FIT/VAL gate failed, full TRAIN PF outside the controlled band, TEST PF below 1.40, thin TEST sample, or domination failure.

## Baseline Exit Behavior

- TRAIN: n=238 PF=0.538 net=Rs-77,654 win%=36.6 avgW=Rs1,039 avgL=Rs-1,113 SL/TGT/EOD=103/62/73 tpd=5.41 domT/D/S=0.014/9.99/9.99
- TEST: n=64 PF=0.897 net=Rs-3,443 win%=50.0 avgW=Rs932 avgL=Rs-1,040 SL/TGT/EOD=19/19/26 tpd=4.27 domT/D/S=0.042/9.99/9.99

## Selected/Best Observed Behavior

- TRAIN: n=238 PF=0.538 net=Rs-77,654 win%=36.6 avgW=Rs1,039 avgL=Rs-1,113 SL/TGT/EOD=103/62/73 tpd=5.41 domT/D/S=0.014/9.99/9.99
- TEST: n=64 PF=0.897 net=Rs-3,443 win%=50.0 avgW=Rs932 avgL=Rs-1,040 SL/TGT/EOD=19/19/26 tpd=4.27 domT/D/S=0.042/9.99/9.99

## Notes

- Fake-breakdown and weak-momentum risk was proxied with close_loc/body/wick/VWAP/volume/ADX/pre-momentum sweeps.
- Bad time-window risk was proxied with min_slot/max_slot/top_n sweeps.
- SL/target behavior was swept over tight, balanced, runner, and wide-stop combinations.
- TEST validation was deliberately restricted to candidates that first landed in the full TRAIN PF band.

## Worst FIT/VAL Rows

| iter | cfg | FIT PF | VAL PF | failure |
|---|---|---|---|---|
| 116 | SL/Tgt=1.5/1.5 mask=[(none)] premom=[sig5_adx_calc<=11.809648] guard={'max_slot': '11:30', 'top_n': 1} maxpos=20 dloss=0.0 | 0.81 | 7.264 | FIT/VAL gate failed |
| 7 | SL/Tgt=1.2/1.0 mask=[wick_skew_pct<=-0.11099; vol_ratio<=6.520122; close_loc<=0.020323] premom=[pre1_adx>=28.592282] guard={'min_slot': '10:00', 'top_n': 1} maxpos=10 dloss=4000.0 | 0.0 | 0.0 | FIT/VAL gate failed |
| 30 | SL/Tgt=0.7/2.5 mask=[notional>=99847.71] premom=[pre_entry_momentum_score<=53.202953] guard={'min_slot': '09:45', 'max_slot': '11:30', 'top_n': 1} maxpos=20 dloss=4000.0 | 0.0 | 0.0 | FIT/VAL gate failed |
| 9 | SL/Tgt=1.0/1.5 mask=[atr_pct<=0.003745; signal_range_pct<=0.371172] premom=[pre3_range_r>=1.26447; sig5_adx_calc<=17.273745] guard={'max_slot': '13:00', 'top_n': 2} maxpos=10 dloss=4000.0 | 0.0 | 0.0 | FIT/VAL gate failed |
| 108 | SL/Tgt=1.0/1.5 mask=[rs_pct>=0.467666; body_pct>=0.791659; rs_pct<=0.049007] premom=[sig5_rsi_dir<=59.298083] guard={'top_n': 1} maxpos=20 dloss=0.0 | 0.0 | 0.0 | FIT/VAL gate failed |
| 120 | SL/Tgt=1.1/2.0 mask=[signal_range_pct>=0.431494; signal_minute>=800.0] premom=[pre1_adx<=15.627146] guard={'max_slot': '11:30', 'top_n': 1} maxpos=20 dloss=0.0 | 0.0 | 0.0 | FIT/VAL gate failed |
| 160 | SL/Tgt=1.5/1.25 mask=[(none)] premom=[sig5_rsi_dir>=80.312389] guard={'max_slot': '11:30', 'top_n': 1} maxpos=20 dloss=0.0 | 0.234 | 0.0 | FIT/VAL gate failed |
| 66 | SL/Tgt=1.5/1.5 mask=[lower_wick_pct<=0.009322; close_loc<=0.31706; quality_score<=18.635755] premom=[sig5_rsi_dir<=52.555584] guard={'max_slot': '14:30', 'top_n': 1} maxpos=20 dloss=0.0 | 0.0 | 0.0 | FIT/VAL gate failed |
| 20 | SL/Tgt=0.7/0.8 mask=[(none)] premom=[sig5_rsi_dir<=55.351855; pre5_mom_r>=1.203222] guard={'min_slot': '09:45', 'max_slot': '12:00', 'top_n': 1} maxpos=20 dloss=4000.0 | 0.302 | 0.609 | FIT/VAL gate failed |
| 39 | SL/Tgt=0.5/2.5 mask=[rs_pct<=-0.964422] premom=[sig5_rsi_dir<=55.351855; sig5_vol_ratio20>=1.857997] guard={'max_slot': '12:00', 'top_n': 1} maxpos=20 dloss=0.0 | 0.0 | 0.385 | FIT/VAL gate failed |
## Staged 5m-Enriched Failure Update
- Staged outcome counts: {'REJECT_FITVAL': 5800, 'REJECT_FULL_TRAIN': 189, 'REJECT_TEST_OR_STABILITY': 11}
- TEST was only evaluated after full TRAIN landed inside the PF band.
- No staged candidate passed TRAIN/TEST/stability gates.
