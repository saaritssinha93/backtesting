# B_AVWAP_RECLAIM_REVERSAL — Optuna code-loop report

**Verdict: INSUFFICIENT_SAMPLE**  |  faithfulness: native=SCREENING-ONLY (firehose; use v11 conf backtest for live-faithful)

- TRAIN 2026-04-13..2026-05-25  TEST 2026-05-26..2026-06-24
- trials run: 300  | objective = min(trPF,tePF) − 0.5·|gap| @15bps

## Best config
```
exit: SL 0.85 / Tgt 2.0
mask_terms: (none)
pre_momentum_terms: (none)
entry_guards: {'min_slot': '10:30'}
```

## Metrics

| window | 15 bps/leg (deployable) | 5 bps/leg (paper) |
|---|---|---|
| TRAIN | n=11 PF=0.237 net=Rs-7,406 dbp=0.9868 day_dom=9.99 trade_dom=9.99 tpd=1.0 | n=11 PF=0.295 net=Rs-6,210 dbp=0.963 day_dom=9.99 trade_dom=9.99 tpd=1.0 |
| TEST  | n=4 PF=0.0 net=Rs-4,325 dbp=None day_dom=9.99 trade_dom=9.99 tpd=2.0 | n=4 PF=0.0 net=Rs-3,925 dbp=None day_dom=9.99 trade_dom=9.99 tpd=2.0 |

Selection gate @15bps: **FAIL** (PF≥1.3, n_tr≥20, n_te≥8, dom≤0.4, tpd≤6.0, test day_block_p≤0.1).

## Live-faithfulness note
native setup → loop is a PESSIMISTIC firehose (live filters through v8/research first). Treat as SCREENING-ONLY; confirm any winner with the v11 conf backtest before trusting.

No final_setup_conf.py change (read-only; promotion requires setup_train_test.py --approve + sign-off).