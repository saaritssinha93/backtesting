# ITERATION_LOG — B_AVWAP_RECLAIM_REVERSAL

Two phases: (A) forward-greedy single-logical-group ablation from the card baseline (change ONE group per iteration, keep only if FIT/VAL band score improves), then (B) global Optuna/seeded search (each row = a new best-FIT/VAL-score improvement). FIT/VAL drives the search; full TRAIN confirms; TEST is scored ONLY when TRAIN PF is inside [1.30,1.70] (anti-overfit).

Rerun command (identical for every iteration — it reruns the whole loop):
```
py -3.12 Train_and_Test/setup_pf_1_4_approval_loop/B_AVWAP_RECLAIM_REVERSAL/scripts/pf_band_search.py --setup B_AVWAP_RECLAIM_REVERSAL --pool C:\TradingData\eqidv2\setup_pools_2026_06_29\B_AVWAP_RECLAIM_REVERSAL --train_start 2026-05-18 --test_start 2026-06-20 --trials 500 --time_budget_min 25.0 --seed 7 --slippage_bps 5.0
```

## Phase A — logical-group ablation
### Iter 1 — group: baseline(card) — **keep(start)**
- change: starting point = card config
- FIT n=268 PF=0.555 | VAL n=305 PF=0.523
- TRAIN n=573 PF=0.5374 net=Rs-131,275 tpd=28.65
- TEST: not scored (TRAIN PF not in band)
- next: sweep exit next

### Iter 2 — group: exit — **keep**
- change: exit SL/Tgt -> 0.8/1.5
- FIT n=253 PF=0.562 | VAL n=295 PF=0.553
- TRAIN n=548 PF=0.5575 net=Rs-125,873 tpd=27.4
- TEST: not scored (TRAIN PF not in band)
- next: lock group, continue

### Iter 3 — group: volume — **keep**
- change: mask vol_ratio>=4.410413
- FIT n=73 PF=0.629 | VAL n=73 PF=0.672
- TRAIN n=146 PF=0.6525 net=Rs-22,819 tpd=8.11
- TEST: not scored (TRAIN PF not in band)
- next: lock group, continue

### Iter 4 — group: volatility — **keep**
- change: mask atr_pct>=0.002151
- FIT n=44 PF=0.66 | VAL n=57 PF=0.647
- TRAIN n=101 PF=0.6523 net=Rs-17,381 tpd=5.61
- TEST: not scored (TRAIN PF not in band)
- next: lock group, continue

### Iter 5 — group: candle_structure — **skip**
- change: skipped (term budget reached)
- FIT n=44 PF=0.66 | VAL n=57 PF=0.647
- next: next group

### Iter 6 — group: vwap_distance — **skip**
- change: skipped (term budget reached)
- FIT n=44 PF=0.66 | VAL n=57 PF=0.647
- next: next group

### Iter 7 — group: relative_strength — **skip**
- change: skipped (term budget reached)
- FIT n=44 PF=0.66 | VAL n=57 PF=0.647
- next: next group

### Iter 8 — group: quality — **skip**
- change: skipped (term budget reached)
- FIT n=44 PF=0.66 | VAL n=57 PF=0.647
- next: next group

### Iter 9 — group: premom_adx — **keep**
- change: premom pre1_adx<=17.56403
- FIT n=7 PF=1.799 | VAL n=7 PF=1.632
- TRAIN n=14 PF=1.7154 net=Rs2,663 tpd=1.56
- TEST: not scored (TRAIN PF not in band)
- next: lock group, continue

### Iter 10 — group: premom_momentum — **reject**
- change: premom pre5_mom_r<=0.904066
- FIT n=7 PF=1.799 | VAL n=7 PF=1.632
- TRAIN n=14 PF=1.7154 net=Rs2,663 tpd=1.56
- TEST: not scored (TRAIN PF not in band)
- next: revert, try next group

### Iter 11 — group: time_guard — **reject**
- change: guard min_slot=None max_slot=14:00
- FIT n=7 PF=1.799 | VAL n=7 PF=1.632
- TRAIN n=14 PF=1.7154 net=Rs2,663 tpd=1.56
- TEST: not scored (TRAIN PF not in band)
- next: revert, try next group

### Iter 12 — group: top_n — **reject**
- change: top_n=3
- FIT n=5 PF=2.138 | VAL n=5 PF=1.372
- TRAIN n=10 PF=1.6277 net=Rs1,754 tpd=1.67
- TEST (gated, TRAIN in band) n=0 PF=0.0 net=Rs0
- next: revert, try next group

## Phase B — global search best-score trajectory
### Iter 1 — trial -1 — score 1.5977
- changed vs prev best: initial best
- FIT n=7 PF=1.799 | VAL n=7 PF=1.632
- cfg: SL/Tgt=0.8/1.5 mask=vwap_dist_atr<=1.0; vol_ratio>=4.410413; atr_pct>=0.002151 premom=pre1_adx<=17.56403 guard={} maxpos=20 dloss=0.0
