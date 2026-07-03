# C_OR_BREAKDOWN — Optuna code-loop report

**Verdict: NOT SELECTED**  |  faithfulness: readmit=LIVE-FAITHFUL

- TRAIN 2026-04-13..2026-05-25  TEST 2026-05-26..2026-06-24
- trials run: 300  | objective = min(trPF,tePF) − 0.5·|gap| @15bps

## Best config
```
exit: SL 1.2 / Tgt 1.0
mask_terms: atr_pct>=0.004322; upper_wick_pct<=0.0
pre_momentum_terms: (none)
entry_guards: {'min_slot': '09:30', 'max_slot': '14:00', 'top_n': 2}
```

## Metrics

| window | 15 bps/leg (deployable) | 5 bps/leg (paper) |
|---|---|---|
| TRAIN | n=42 PF=0.843 net=Rs-3,420 dbp=0.7343 day_dom=9.99 trade_dom=9.99 tpd=2.33 | n=42 PF=1.184 net=Rs3,351 dbp=0.3252 day_dom=1.537 trade_dom=0.259 tpd=2.33 |
| TEST  | n=18 PF=0.843 net=Rs-1,569 dbp=0.657 day_dom=9.99 trade_dom=9.99 tpd=1.38 | n=18 PF=1.297 net=Rs2,369 dbp=0.2805 day_dom=0.732 trade_dom=0.366 tpd=1.38 |

Selection gate @15bps: **FAIL** (PF≥1.3, n_tr≥20, n_te≥8, dom≤0.4, tpd≤6.0, test day_block_p≤0.1).

## Live-faithfulness note
readmit basis → loop is live-faithful.

No final_setup_conf.py change (read-only; promotion requires setup_train_test.py --approve + sign-off).