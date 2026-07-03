# D_EMA20_REJECTION — Optuna code-loop report

**Verdict: WATCH(paper-only)**  |  faithfulness: native=SCREENING-ONLY (firehose; use v11 conf backtest for live-faithful)

- TRAIN 2026-04-13..2026-05-25  TEST 2026-05-26..2026-06-24
- trials run: 300  | objective = min(trPF,tePF) − 0.5·|gap| @15bps

## Best config
```
exit: SL 1.1 / Tgt 2.0
mask_terms: signal_range_pct<=0.477829
pre_momentum_terms: sig5_rsi_dir>=52.862146
entry_guards: {'min_slot': '11:00', 'top_n': 2}
```

## Metrics

| window | 15 bps/leg (deployable) | 5 bps/leg (paper) |
|---|---|---|
| TRAIN | n=20 PF=1.416 net=Rs2,673 dbp=0.2955 day_dom=1.071 trade_dom=0.661 tpd=2.0 | n=20 PF=2.413 net=Rs6,775 dbp=0.1106 day_dom=0.493 trade_dom=0.275 tpd=2.0 |
| TEST  | n=9 PF=1.695 net=Rs2,195 dbp=0.2032 day_dom=0.803 trade_dom=0.804 tpd=1.5 | n=9 PF=2.311 net=Rs3,481 dbp=0.1092 day_dom=0.571 trade_dom=0.535 tpd=1.5 |

Selection gate @15bps: **FAIL** (PF≥1.3, n_tr≥20, n_te≥8, dom≤0.4, tpd≤6.0, test day_block_p≤0.1).

## Live-faithfulness note
native setup → loop is a PESSIMISTIC firehose (live filters through v8/research first). Treat as SCREENING-ONLY; confirm any winner with the v11 conf backtest before trusting.

No final_setup_conf.py change (read-only; promotion requires setup_train_test.py --approve + sign-off).