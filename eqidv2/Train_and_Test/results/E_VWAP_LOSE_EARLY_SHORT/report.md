# E_VWAP_LOSE_EARLY_SHORT — Optuna code-loop report

**Verdict: NOT SELECTED**  |  faithfulness: native=SCREENING-ONLY (firehose; use v11 conf backtest for live-faithful)

- TRAIN 2026-04-13..2026-05-25  TEST 2026-05-26..2026-06-24
- trials run: 300  | objective = min(trPF,tePF) − 0.5·|gap| @15bps

## Best config
```
exit: SL 0.7 / Tgt 2.0
mask_terms: (none)
pre_momentum_terms: sig5_rsi_dir<=49.392265; pre3_close_pos<=0.687499
entry_guards: {'max_slot': '12:00', 'top_n': 2}
```

## Metrics

| window | 15 bps/leg (deployable) | 5 bps/leg (paper) |
|---|---|---|
| TRAIN | n=22 PF=0.749 net=Rs-3,023 dbp=0.7067 day_dom=9.99 trade_dom=9.99 tpd=1.83 | n=22 PF=1.223 net=Rs2,122 dbp=0.379 day_dom=1.756 trade_dom=0.88 tpd=1.83 |
| TEST  | n=24 PF=0.724 net=Rs-3,844 dbp=0.7467 day_dom=9.99 trade_dom=9.99 tpd=2.4 | n=24 PF=0.977 net=Rs-280 dbp=0.5317 day_dom=9.99 trade_dom=9.99 tpd=2.4 |

Selection gate @15bps: **FAIL** (PF≥1.3, n_tr≥20, n_te≥8, dom≤0.4, tpd≤6.0, test day_block_p≤0.1).

## Live-faithfulness note
native setup → loop is a PESSIMISTIC firehose (live filters through v8/research first). Treat as SCREENING-ONLY; confirm any winner with the v11 conf backtest before trusting.

No final_setup_conf.py change (read-only; promotion requires setup_train_test.py --approve + sign-off).