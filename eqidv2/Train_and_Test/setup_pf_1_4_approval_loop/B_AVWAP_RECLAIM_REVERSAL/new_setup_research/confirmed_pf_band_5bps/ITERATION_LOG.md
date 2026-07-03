# ITERATION_LOG — B_AVWAP_CONFIRMED_RECLAIM_LONG

Two phases: (A) forward-greedy single-logical-group ablation from the card baseline (change ONE group per iteration, keep only if FIT/VAL band score improves), then (B) global Optuna/seeded search (each row = a new best-FIT/VAL-score improvement). FIT/VAL drives the search; full TRAIN confirms; TEST is scored ONLY when TRAIN PF is inside [1.30,1.70] (anti-overfit).

Rerun command (identical for every iteration — it reruns the whole loop):
```
py -3.12 Train_and_Test/setup_pf_1_4_approval_loop/B_AVWAP_CONFIRMED_RECLAIM_LONG/scripts/pf_band_search.py --setup B_AVWAP_CONFIRMED_RECLAIM_LONG --pool Train_and_Test\setup_pf_1_4_approval_loop\B_AVWAP_RECLAIM_REVERSAL\new_setup_research\family_pool --train_start 2026-05-18 --test_start 2026-06-20 --trials 300 --time_budget_min 10.0 --seed 13 --slippage_bps 5.0
```

## Phase A — logical-group ablation
### Iter 1 — group: baseline(card) — **keep(start)**
- change: starting point = card config
- FIT n=22 PF=0.619 | VAL n=26 PF=1.736
- TRAIN n=48 PF=1.1166 net=Rs1,875 tpd=3.0
- TEST: not scored (TRAIN PF not in band)
- next: sweep exit next

### Iter 2 — group: exit — **keep**
- change: exit SL/Tgt -> 1.0/1.25
- FIT n=22 PF=0.945 | VAL n=26 PF=1.479
- TRAIN n=48 PF=1.2162 net=Rs3,257 tpd=3.0
- TEST: not scored (TRAIN PF not in band)
- next: lock group, continue

### Iter 3 — group: volume — **keep**
- change: mask vol_ratio>=1.234686
- FIT n=20 PF=1.075 | VAL n=23 PF=1.253
- TRAIN n=43 PF=1.1727 net=Rs2,413 tpd=2.69
- TEST: not scored (TRAIN PF not in band)
- next: lock group, continue

### Iter 4 — group: volatility — **keep**
- change: mask atr_pct>=0.002125
- FIT n=7 PF=4.599 | VAL n=11 PF=2.522
- TRAIN n=18 PF=3.0087 net=Rs6,810 tpd=1.64
- TEST: not scored (TRAIN PF not in band)
- next: lock group, continue

### Iter 5 — group: candle_structure — **reject**
- change: mask close_loc>=0.680167
- FIT n=5 PF=2.807 | VAL n=11 PF=2.522
- TRAIN n=16 PF=2.5886 net=Rs5,386 tpd=1.6
- TEST: not scored (TRAIN PF not in band)
- next: revert, try next group

### Iter 6 — group: vwap_distance — **reject**
- change: mask vwap_dist_atr>=0.579503
- FIT n=7 PF=4.599 | VAL n=10 PF=2.092
- TRAIN n=17 PF=2.6797 net=Rs5,695 tpd=1.55
- TEST: not scored (TRAIN PF not in band)
- next: revert, try next group

### Iter 7 — group: relative_strength — **reject**
- change: mask rs_pct>=0.123765
- FIT n=7 PF=4.599 | VAL n=10 PF=2.091
- TRAIN n=17 PF=2.6794 net=Rs5,694 tpd=1.7
- TEST: not scored (TRAIN PF not in band)
- next: revert, try next group

### Iter 8 — group: quality — **reject**
- change: mask quality_score>=54.71524
- FIT n=5 PF=3.194 | VAL n=11 PF=2.522
- TRAIN n=16 PF=2.6792 net=Rs5,693 tpd=1.45
- TEST: not scored (TRAIN PF not in band)
- next: revert, try next group

### Iter 9 — group: premom_adx — **reject**
- change: premom sig5_adx_calc>=13.070816
- FIT n=7 PF=4.599 | VAL n=11 PF=2.522
- TRAIN n=18 PF=3.0087 net=Rs6,810 tpd=1.64
- TEST: not scored (TRAIN PF not in band)
- next: revert, try next group

### Iter 10 — group: premom_momentum — **reject**
- change: premom pre_entry_momentum_score>=44.218821
- FIT n=7 PF=4.599 | VAL n=10 PF=2.612
- TRAIN n=17 PF=3.091 net=Rs6,901 tpd=1.7
- TEST: not scored (TRAIN PF not in band)
- next: revert, try next group

### Iter 11 — group: time_guard — **reject**
- change: guard min_slot=None max_slot=14:00
- FIT n=7 PF=4.599 | VAL n=11 PF=2.522
- TRAIN n=18 PF=3.0087 net=Rs6,810 tpd=1.64
- TEST: not scored (TRAIN PF not in band)
- next: revert, try next group

### Iter 12 — group: top_n — **reject**
- change: top_n=1
- FIT n=7 PF=4.599 | VAL n=9 PF=2.857
- TRAIN n=16 PF=3.371 net=Rs6,391 tpd=1.45
- TEST: not scored (TRAIN PF not in band)
- next: revert, try next group

## Phase B — global search best-score trajectory
### Iter 1 — trial -1 — score 1.7
- changed vs prev best: initial best
- FIT n=7 PF=4.599 | VAL n=11 PF=2.522
- cfg: SL/Tgt=1.0/1.25 mask=vol_ratio>=1.234686; atr_pct>=0.002125 premom=(none) guard={} maxpos=20 dloss=0.0
