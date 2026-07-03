# ITERATION_LOG — B_AVWAP_RECLAIM_PULLBACK_BREAK_LONG

Two phases: (A) forward-greedy single-logical-group ablation from the card baseline (change ONE group per iteration, keep only if FIT/VAL band score improves), then (B) global Optuna/seeded search (each row = a new best-FIT/VAL-score improvement). FIT/VAL drives the search; full TRAIN confirms; TEST is scored ONLY when TRAIN PF is inside [1.30,1.70] (anti-overfit).

Rerun command (identical for every iteration — it reruns the whole loop):
```
py -3.12 Train_and_Test/setup_pf_1_4_approval_loop/B_AVWAP_RECLAIM_PULLBACK_BREAK_LONG/scripts/pf_band_search.py --setup B_AVWAP_RECLAIM_PULLBACK_BREAK_LONG --pool Train_and_Test\setup_pf_1_4_approval_loop\B_AVWAP_RECLAIM_REVERSAL\new_setup_research\family_pool --train_start 2026-05-18 --test_start 2026-06-20 --trials 200 --time_budget_min 8.0 --seed 17 --slippage_bps 5.0
```

## Phase A — logical-group ablation
### Iter 1 — group: baseline(card) — **keep(start)**
- change: starting point = card config
- FIT n=30 PF=0.71 | VAL n=38 PF=1.391
- TRAIN n=68 PF=1.035 net=Rs765 tpd=3.58
- TEST: not scored (TRAIN PF not in band)
- next: sweep exit next

### Iter 2 — group: exit — **keep**
- change: exit SL/Tgt -> 1.3/1.25
- FIT n=30 PF=1.36 | VAL n=38 PF=1.332
- TRAIN n=68 PF=1.3448 net=Rs6,297 tpd=3.58
- TEST (gated, TRAIN in band) n=12 PF=0.4201 net=Rs-3,775
- next: lock group, continue

### Iter 3 — group: volume — **reject**
- change: mask vol_ratio>=1.234686
- FIT n=28 PF=1.601 | VAL n=33 PF=1.321
- TRAIN n=61 PF=1.446 net=Rs6,959 tpd=3.39
- TEST (gated, TRAIN in band) n=11 PF=0.4524 net=Rs-3,310
- next: revert, try next group

### Iter 4 — group: volatility — **keep**
- change: mask atr_pct>=0.00206
- FIT n=15 PF=3.773 | VAL n=19 PF=2.324
- TRAIN n=34 PF=2.8202 net=Rs11,828 tpd=2.12
- TEST: not scored (TRAIN PF not in band)
- next: lock group, continue

### Iter 5 — group: candle_structure — **reject**
- change: mask close_loc>=0.669512
- FIT n=14 PF=3.635 | VAL n=17 PF=2.229
- TRAIN n=31 PF=2.7348 net=Rs10,730 tpd=2.21
- TEST: not scored (TRAIN PF not in band)
- next: revert, try next group

### Iter 6 — group: vwap_distance — **reject**
- change: mask vwap_dist_atr>=0.622207
- FIT n=15 PF=3.773 | VAL n=16 PF=1.843
- TRAIN n=31 PF=2.5138 net=Rs9,700 tpd=2.07
- TEST: not scored (TRAIN PF not in band)
- next: revert, try next group

### Iter 7 — group: relative_strength — **reject**
- change: mask rs_pct>=0.084576
- FIT n=13 PF=3.214 | VAL n=17 PF=2.837
- TRAIN n=30 PF=2.9839 net=Rs11,320 tpd=2.0
- TEST: not scored (TRAIN PF not in band)
- next: revert, try next group

### Iter 8 — group: quality — **reject**
- change: mask quality_score>=53.569663
- FIT n=13 PF=3.214 | VAL n=18 PF=2.311
- TRAIN n=31 PF=2.6202 net=Rs10,528 tpd=2.07
- TEST: not scored (TRAIN PF not in band)
- next: revert, try next group

### Iter 9 — group: premom_adx — **reject**
- change: premom sig5_adx_calc>=13.079268
- FIT n=15 PF=3.773 | VAL n=19 PF=2.324
- TRAIN n=34 PF=2.8202 net=Rs11,828 tpd=2.12
- TEST: not scored (TRAIN PF not in band)
- next: revert, try next group

### Iter 10 — group: premom_momentum — **reject**
- change: premom pre_entry_momentum_score>=44.218821
- FIT n=15 PF=3.773 | VAL n=17 PF=2.361
- TRAIN n=32 PF=2.8515 net=Rs11,865 tpd=2.13
- TEST: not scored (TRAIN PF not in band)
- next: revert, try next group

### Iter 11 — group: time_guard — **reject**
- change: guard min_slot=None max_slot=12:30
- FIT n=7 PF=2.006 | VAL n=14 PF=2.095
- TRAIN n=21 PF=2.0602 net=Rs5,547 tpd=1.5
- TEST: not scored (TRAIN PF not in band)
- next: revert, try next group

### Iter 12 — group: top_n — **reject**
- change: top_n=1
- FIT n=13 PF=3.147 | VAL n=16 PF=2.152
- TRAIN n=29 PF=2.5333 net=Rs8,898 tpd=1.81
- TEST: not scored (TRAIN PF not in band)
- next: revert, try next group

## Phase B — global search best-score trajectory
### Iter 1 — trial -1 — score 1.7
- changed vs prev best: initial best
- FIT n=15 PF=3.773 | VAL n=19 PF=2.324
- cfg: SL/Tgt=1.3/1.25 mask=atr_pct>=0.00206 premom=(none) guard={} maxpos=20 dloss=0.0
