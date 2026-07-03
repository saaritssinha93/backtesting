# E_ORB_BREAKOUT_LONG — ITERATION_LOG

Guided coordinate loop (one logical group/iteration, greedy keep-best on FIT/VAL band-score) + 500 Optuna global trials. Band target: TRAIN PF [1.3,1.7], TEST PF > 1.4.

- Iteration 0 = BASELINE: TRAIN n=29 PF=0.518 net=Rs-9,402 win=27.6% t/s/e=8/21/0 tpd=1.81 dayDom=9.99 symDom=9.99 trDom=0.125 dd=Rs-11,000 dbp=0.9724 | TEST n=6 PF=0.68 net=Rs-1,189 win=33.3% t/s/e=2/4/0 tpd=2.0 dayDom=9.99 symDom=9.99 trDom=0.501 dd=Rs-2,792 dbp=0.8528

### Iteration 1 — group: exit
- change: sweep SL/Tgt grid -> 1.0/2.5
- FIT n/PF=(15, 0.706)  VAL n/PF=(14, 1.023)  band-score=0.2501  -> **KEPT**
- full TRAIN: n=29 PF=0.852 net=Rs-3,526 win=31.0% t/s/e=9/19/1 tpd=1.81 dayDom=9.99 symDom=9.99 trDom=0.111 dd=Rs-4,421 dbp=0.7108  (in band: False)
- TEST (NOT scored — TRAIN out of band): -
- next action: return to TRAIN-side logic (TRAIN not in band)

### Iteration 2 — group: regime
- change: regime mask -> none
- FIT n/PF=(15, 0.706)  VAL n/PF=(14, 1.023)  band-score=0.2501  -> **REJECT**
- next action: discard; try next logical group

### Iteration 3 — group: volume
- change: vol_ratio >= threshold -> drop
- FIT n/PF=(15, 0.706)  VAL n/PF=(14, 1.023)  band-score=0.2501  -> **REJECT**
- next action: discard; try next logical group

### Iteration 4 — group: volatility
- change: atr_pct >= threshold -> drop
- FIT n/PF=(15, 0.706)  VAL n/PF=(14, 1.023)  band-score=0.2501  -> **REJECT**
- next action: discard; try next logical group

### Iteration 5 — group: trend_rs
- change: rs_pct >= threshold -> >=q50
- FIT n/PF=(10, 1.226)  VAL n/PF=(6, 1.836)  band-score=0.8162  -> **KEPT**
- full TRAIN: n=16 PF=1.429 net=Rs4,756 win=43.8% t/s/e=7/9/0 tpd=1.23 dayDom=0.476 symDom=0.476 trDom=0.143 dd=Rs-2,464 dbp=0.2076  (in band: True)
- TEST (scored, TRAIN in band): n=4 PF=5.536 net=Rs5,552 win=75.0% t/s/e=3/1/0 tpd=1.33 dayDom=0.814 symDom=0.407 trDom=0.333 dd=Rs0 dbp=0.1466
- next action: TEST evaluated once

### Iteration 6 — group: vwap
- change: vwap_dist_atr <= threshold -> <=q20
- FIT n/PF=(9, 1.471)  VAL n/PF=(6, 1.836)  band-score=1.2197  -> **KEPT**
- full TRAIN: n=15 PF=1.608 net=Rs5,983 win=46.7% t/s/e=7/8/0 tpd=1.25 dayDom=0.378 symDom=0.379 trDom=0.143 dd=Rs-2,464 dbp=0.1469  (in band: True)
- TEST (scored, TRAIN in band): n=4 PF=5.536 net=Rs5,552 win=75.0% t/s/e=3/1/0 tpd=1.33 dayDom=0.814 symDom=0.407 trDom=0.333 dd=Rs0 dbp=0.1466
- next action: TEST evaluated once

### Iteration 7 — group: candle_closeloc
- change: close_loc >= threshold -> drop
- FIT n/PF=(9, 1.471)  VAL n/PF=(6, 1.836)  band-score=1.2197  -> **REJECT**
- next action: discard; try next logical group

### Iteration 8 — group: candle_body
- change: body_pct >= threshold -> drop
- FIT n/PF=(9, 1.471)  VAL n/PF=(6, 1.836)  band-score=1.2197  -> **REJECT**
- next action: discard; try next logical group

### Iteration 9 — group: quality
- change: quality_score >= threshold -> drop
- FIT n/PF=(9, 1.471)  VAL n/PF=(6, 1.836)  band-score=1.2197  -> **REJECT**
- next action: discard; try next logical group

### Iteration 10 — group: pre_momentum
- change: single premom gate -> drop
- FIT n/PF=(9, 1.471)  VAL n/PF=(6, 1.836)  band-score=1.2197  -> **REJECT**
- next action: discard; try next logical group

### Iteration 11 — group: time_guard
- change: entry time window -> min=None,max=None
- FIT n/PF=(9, 1.471)  VAL n/PF=(6, 1.836)  band-score=1.2197  -> **REJECT**
- next action: discard; try next logical group

### Iteration 12 — group: portfolio
- change: max_positions/daily_loss -> mp10,dl0.0
- FIT n/PF=(9, 1.471)  VAL n/PF=(6, 1.836)  band-score=1.2197  -> **REJECT**
- next action: discard; try next logical group

### Iteration 13 — group: exit
- change: sweep SL/Tgt grid -> 1.0/2.5
- FIT n/PF=(9, 1.471)  VAL n/PF=(6, 1.836)  band-score=1.2197  -> **REJECT**
- next action: discard; try next logical group

### Iteration 14 — group: regime
- change: regime mask -> none
- FIT n/PF=(9, 1.471)  VAL n/PF=(6, 1.836)  band-score=1.2197  -> **REJECT**
- next action: discard; try next logical group

### Iteration 15 — group: volume
- change: vol_ratio >= threshold -> drop
- FIT n/PF=(9, 1.471)  VAL n/PF=(6, 1.836)  band-score=1.2197  -> **REJECT**
- next action: discard; try next logical group

### Iteration 16 — group: volatility
- change: atr_pct >= threshold -> drop
- FIT n/PF=(9, 1.471)  VAL n/PF=(6, 1.836)  band-score=1.2197  -> **REJECT**
- next action: discard; try next logical group

### Iteration 17 — group: trend_rs
- change: rs_pct >= threshold -> >=q50
- FIT n/PF=(9, 1.471)  VAL n/PF=(6, 1.836)  band-score=1.2197  -> **REJECT**
- next action: discard; try next logical group

### Iteration 18 — group: vwap
- change: vwap_dist_atr <= threshold -> <=q20
- FIT n/PF=(9, 1.471)  VAL n/PF=(6, 1.836)  band-score=1.2197  -> **REJECT**
- next action: discard; try next logical group

### Iteration 19 — group: candle_closeloc
- change: close_loc >= threshold -> drop
- FIT n/PF=(9, 1.471)  VAL n/PF=(6, 1.836)  band-score=1.2197  -> **REJECT**
- next action: discard; try next logical group

### Iteration 20 — group: candle_body
- change: body_pct >= threshold -> drop
- FIT n/PF=(9, 1.471)  VAL n/PF=(6, 1.836)  band-score=1.2197  -> **REJECT**
- next action: discard; try next logical group

### Iteration 21 — group: quality
- change: quality_score >= threshold -> drop
- FIT n/PF=(9, 1.471)  VAL n/PF=(6, 1.836)  band-score=1.2197  -> **REJECT**
- next action: discard; try next logical group

### Iteration 22 — group: pre_momentum
- change: single premom gate -> drop
- FIT n/PF=(9, 1.471)  VAL n/PF=(6, 1.836)  band-score=1.2197  -> **REJECT**
- next action: discard; try next logical group

### Iteration 23 — group: time_guard
- change: entry time window -> min=None,max=None
- FIT n/PF=(9, 1.471)  VAL n/PF=(6, 1.836)  band-score=1.2197  -> **REJECT**
- next action: discard; try next logical group

### Iteration 24 — group: portfolio
- change: max_positions/daily_loss -> mp10,dl0.0
- FIT n/PF=(9, 1.471)  VAL n/PF=(6, 1.836)  band-score=1.2197  -> **REJECT**
- next action: discard; try next logical group

### Iteration 25 — group: exit
- change: sweep SL/Tgt grid -> 1.0/2.5
- FIT n/PF=(9, 1.471)  VAL n/PF=(6, 1.836)  band-score=1.2197  -> **REJECT**
- next action: discard; try next logical group

### Iteration 26 — group: regime
- change: regime mask -> none
- FIT n/PF=(9, 1.471)  VAL n/PF=(6, 1.836)  band-score=1.2197  -> **REJECT**
- next action: discard; try next logical group

### Iteration 27 — group: volume
- change: vol_ratio >= threshold -> drop
- FIT n/PF=(9, 1.471)  VAL n/PF=(6, 1.836)  band-score=1.2197  -> **REJECT**
- next action: discard; try next logical group

### Iteration 28 — group: volatility
- change: atr_pct >= threshold -> drop
- FIT n/PF=(9, 1.471)  VAL n/PF=(6, 1.836)  band-score=1.2197  -> **REJECT**
- next action: discard; try next logical group

### Iteration 29 — group: trend_rs
- change: rs_pct >= threshold -> >=q50
- FIT n/PF=(9, 1.471)  VAL n/PF=(6, 1.836)  band-score=1.2197  -> **REJECT**
- next action: discard; try next logical group

### Iteration 30 — group: vwap
- change: vwap_dist_atr <= threshold -> <=q20
- FIT n/PF=(9, 1.471)  VAL n/PF=(6, 1.836)  band-score=1.2197  -> **REJECT**
- next action: discard; try next logical group

### Iteration 31 — group: candle_closeloc
- change: close_loc >= threshold -> drop
- FIT n/PF=(9, 1.471)  VAL n/PF=(6, 1.836)  band-score=1.2197  -> **REJECT**
- next action: discard; try next logical group

### Iteration 32 — group: candle_body
- change: body_pct >= threshold -> drop
- FIT n/PF=(9, 1.471)  VAL n/PF=(6, 1.836)  band-score=1.2197  -> **REJECT**
- next action: discard; try next logical group

### Iteration 33 — group: quality
- change: quality_score >= threshold -> drop
- FIT n/PF=(9, 1.471)  VAL n/PF=(6, 1.836)  band-score=1.2197  -> **REJECT**
- next action: discard; try next logical group

### Iteration 34 — group: pre_momentum
- change: single premom gate -> drop
- FIT n/PF=(9, 1.471)  VAL n/PF=(6, 1.836)  band-score=1.2197  -> **REJECT**
- next action: discard; try next logical group

### Iteration 35 — group: time_guard
- change: entry time window -> min=None,max=None
- FIT n/PF=(9, 1.471)  VAL n/PF=(6, 1.836)  band-score=1.2197  -> **REJECT**
- next action: discard; try next logical group

### Iteration 36 — group: portfolio
- change: max_positions/daily_loss -> mp10,dl0.0
- FIT n/PF=(9, 1.471)  VAL n/PF=(6, 1.836)  band-score=1.2197  -> **REJECT**
- next action: discard; try next logical group

## Optuna global-best (confirmed)

- cfg: SL/Tgt=0.9/1.5 mask=[['signal_range_pct', '<=', 1.670956]] premom=[] guard={'min_slot': '10:30', 'max_slot': '14:00', 'top_n': 1}
- TRAIN n=15 PF=1.779 net=Rs4,957 win=60.0% t/s/e=9/5/1 tpd=1.36 dayDom=0.508 symDom=0.255 trDom=0.112 dd=Rs-2,969 dbp=0.1414
- TEST  n=0 (no trades)