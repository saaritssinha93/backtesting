# B_HUGE_RED_FAILED_BOUNCE — Optuna code-loop report

**Verdict: NOT SELECTED**  |  faithfulness: readmit=LIVE-FAITHFUL

- TRAIN 2026-04-13..2026-05-25  TEST 2026-05-26..2026-06-24
- trials run: 300  | objective = min(trPF,tePF) − 0.5·|gap| @15bps

## Best config
```
exit: SL 1.2 / Tgt 1.5
mask_terms: rs_pct>=0.982673
pre_momentum_terms: (none)
entry_guards: {}
```

## Metrics

| window | 15 bps/leg (deployable) | 5 bps/leg (paper) |
|---|---|---|
| TRAIN | n=57 PF=0.575 net=Rs-15,145 dbp=0.8919 day_dom=9.99 trade_dom=9.99 tpd=3.0 | n=57 PF=0.822 net=Rs-5,493 dbp=0.6871 day_dom=9.99 trade_dom=9.99 tpd=3.0 |
| TEST  | n=45 PF=0.644 net=Rs-9,188 dbp=0.8493 day_dom=9.99 trade_dom=9.99 tpd=3.0 | n=45 PF=1.076 net=Rs1,488 dbp=0.4426 day_dom=3.177 trade_dom=0.919 tpd=3.0 |

Selection gate @15bps: **FAIL** (PF≥1.3, n_tr≥20, n_te≥8, dom≤0.4, tpd≤6.0, test day_block_p≤0.1).

## Live-faithfulness note
readmit basis → loop is live-faithful.

No final_setup_conf.py change (read-only; promotion requires setup_train_test.py --approve + sign-off).