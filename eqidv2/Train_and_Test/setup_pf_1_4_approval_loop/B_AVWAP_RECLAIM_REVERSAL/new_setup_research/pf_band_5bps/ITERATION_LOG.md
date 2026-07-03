# ITERATION_LOG — B_AVWAP_CONFIRMED_RECLAIM_LONG

Two phases: (A) forward-greedy single-logical-group ablation from the card baseline (change ONE group per iteration, keep only if FIT/VAL band score improves), then (B) global Optuna/seeded search (each row = a new best-FIT/VAL-score improvement). FIT/VAL drives the search; full TRAIN confirms; TEST is scored ONLY when TRAIN PF is inside [1.30,1.70] (anti-overfit).

Rerun command (identical for every iteration — it reruns the whole loop):
```
py -3.12 Train_and_Test/setup_pf_1_4_approval_loop/B_AVWAP_CONFIRMED_RECLAIM_LONG/scripts/pf_band_search.py --setup B_AVWAP_CONFIRMED_RECLAIM_LONG --pool Train_and_Test\setup_pf_1_4_approval_loop\B_AVWAP_RECLAIM_REVERSAL\new_setup_research\pool --train_start 2026-05-18 --test_start 2026-06-20 --trials 300 --time_budget_min 10.0 --seed 11 --slippage_bps 5.0
```

## Phase A — logical-group ablation
### Iter 1 — group: baseline(card) — **keep(start)**
- change: starting point = card config
- FIT n=13 PF=0.401 | VAL n=23 PF=1.77
- TRAIN n=36 PF=1.1174 net=Rs1,521 tpd=2.4
- TEST: not scored (TRAIN PF not in band)
- next: sweep exit next

### Iter 2 — group: exit — **keep**
- change: exit SL/Tgt -> 1.0/1.25
- FIT n=13 PF=0.684 | VAL n=23 PF=1.565
- TRAIN n=36 PF=1.1871 net=Rs2,381 tpd=2.4
- TEST: not scored (TRAIN PF not in band)
- next: lock group, continue

### Iter 3 — group: volume — **keep**
- change: mask vol_ratio>=1.61426
- FIT n=9 PF=1.348 | VAL n=19 PF=1.318
- TRAIN n=28 PF=1.3267 net=Rs3,231 tpd=2.0
- TEST (gated, TRAIN in band) n=2 PF=0.887 net=Rs-30
- next: lock group, continue

### Iter 4 — group: volatility — **keep**
- change: mask atr_pct>=0.00214
- FIT n=3 PF=7.058 | VAL n=10 PF=2.614
- TRAIN n=13 PF=2.9453 net=Rs5,266 tpd=1.44
- TEST: not scored (TRAIN PF not in band)
- next: lock group, continue

### Iter 5 — group: candle_structure — **reject**
- change: mask close_loc<=1.0
- FIT n=3 PF=7.058 | VAL n=10 PF=2.614
- TRAIN n=13 PF=2.9453 net=Rs5,266 tpd=1.44
- TEST: not scored (TRAIN PF not in band)
- next: revert, try next group

### Iter 6 — group: vwap_distance — **reject**
- change: mask vwap_dist_atr>=0.575075
- FIT n=3 PF=7.058 | VAL n=9 PF=2.169
- TRAIN n=12 PF=2.5333 net=Rs4,151 tpd=1.33
- TEST: not scored (TRAIN PF not in band)
- next: revert, try next group

### Iter 7 — group: relative_strength — **reject**
- change: mask rs_pct>=0.193806
- FIT n=3 PF=7.058 | VAL n=9 PF=2.168
- TRAIN n=12 PF=2.5329 net=Rs4,150 tpd=1.5
- TEST: not scored (TRAIN PF not in band)
- next: revert, try next group

### Iter 8 — group: quality — **reject**
- change: mask quality_score>=57.040659
- FIT n=3 PF=7.058 | VAL n=10 PF=2.614
- TRAIN n=13 PF=2.9453 net=Rs5,266 tpd=1.44
- TEST: not scored (TRAIN PF not in band)
- next: revert, try next group

### Iter 9 — group: premom_adx — **reject**
- change: premom sig5_adx_calc>=13.336904
- FIT n=3 PF=7.058 | VAL n=9 PF=2.169
- TRAIN n=12 PF=2.5335 net=Rs4,151 tpd=1.5
- TEST: not scored (TRAIN PF not in band)
- next: revert, try next group

### Iter 10 — group: premom_momentum — **reject**
- change: premom pre_entry_momentum_score<=63.681586
- FIT n=3 PF=7.058 | VAL n=9 PF=2.17
- TRAIN n=12 PF=2.5339 net=Rs4,152 tpd=1.33
- TEST: not scored (TRAIN PF not in band)
- next: revert, try next group

### Iter 11 — group: time_guard — **reject**
- change: guard min_slot=None max_slot=14:00
- FIT n=3 PF=7.058 | VAL n=10 PF=2.614
- TRAIN n=13 PF=2.9453 net=Rs5,266 tpd=1.44
- TEST: not scored (TRAIN PF not in band)
- next: revert, try next group

### Iter 12 — group: top_n — **reject**
- change: top_n=1
- FIT n=3 PF=7.058 | VAL n=8 PF=3.002
- TRAIN n=11 PF=3.4087 net=Rs4,847 tpd=1.22
- TEST: not scored (TRAIN PF not in band)
- next: revert, try next group

## Phase B — global search best-score trajectory
### Iter 1 — trial -1 — score 1.7
- changed vs prev best: initial best
- FIT n=3 PF=7.058 | VAL n=10 PF=2.614
- cfg: SL/Tgt=1.0/1.25 mask=vol_ratio>=1.61426; atr_pct>=0.00214 premom=(none) guard={} maxpos=20 dloss=0.0
