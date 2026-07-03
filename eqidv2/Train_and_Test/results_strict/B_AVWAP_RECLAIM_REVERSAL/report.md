# B_AVWAP_RECLAIM_REVERSAL — FIT/VAL→TRAIN/TEST code-loop report

**Verdict: NOT SELECTED**  |  faithfulness: native=SCREENING-ONLY (firehose; use v11 conf backtest for live-faithful)  |  optimizer: Optuna TPE

- FIT 2026-05-29..2026-06-04 | VAL 2026-06-05..2026-06-11 | TRAIN 2026-05-29..2026-06-11 | TEST 2026-06-12..2026-06-24
- trials: 300  | objective on FIT/VAL only = min(PF) − 0.5·|gap|; TEST scored once

## Best config (only knobs the engine honors)
```
exit: SL 0.9 / Tgt 0.8
mask_terms: lower_wick_pct>=0.098224
pre_momentum_terms: pre_entry_momentum_score>=66.552605; sig5_rsi_dir>=51.905415
entry_guards: {'min_slot': '09:30', 'top_n': 1}  (NB: conf-mask honors only min_slot; top_n/max_slot NOT enforced live)
max_positions: 10  daily_loss_rs: 4000.0
```

## Metrics (best config)

| window | 15 bps/leg | 5 bps/leg |
|---|---|---|
| TRAIN | n=18 PF=0.554 net=Rs-4,087 tradeDomGross=0.111 dayDom=9.99 symDom=9.99 tpd=2.25 dbp=0.953 | n=18 PF=0.723 net=Rs-2,291 tradeDomGross=0.111 dayDom=9.99 symDom=9.99 tpd=2.25 dbp=0.8245 |
| TEST  | n=12 PF=0.365 net=Rs-3,943 tradeDomGross=0.25 dayDom=9.99 symDom=9.99 tpd=3.0 dbp=0.8845 | n=12 PF=0.495 net=Rs-2,749 tradeDomGross=0.248 dayDom=9.99 symDom=9.99 tpd=3.0 dbp=0.731 |

Selection gate (TRAIN & TEST @15bps): **FAIL** (PF≥1.3, train_n≥15, test_n≥5, gross-profit/day/symbol dominance≤0.4, trades/day≤6.0).

## Live-crosscheck
native setup → SCREENING-ONLY firehose; the v11 conf backtest is the live-faithful arbiter. Doc §5 note for B_AVWAP: the v11 live overlay uses the INVERTED vwap_dist_atr≥0.60 mask (conf uses ≤1.0) — any winner must not rely on that inverted gate.

## Command
```
py -3.12 Train_and_Test\optuna_fitval_loop.py --setup B_AVWAP_RECLAIM_REVERSAL --pool C:/Users/Saarit/AppData/Local/Temp/claude/c--Users-Saarit-OneDrive-Desktop-Trading-backtesting-eqidv2-backtesting-eqidv2/41d4e196-2e06-4276-945a-008c377c414d/scratchpad/setup_pools/B_AVWAP_RECLAIM_REVERSAL --trials 300 --time_budget_min 20.0 --seed 7 --out Train_and_Test/results_strict
```

## Files changed
- none to production. Artifacts under this folder. No final_setup_conf.py edit; no live trades.

## Why NOT SELECTED
- best FIT/VAL config does not clear the gate on TRAIN+TEST at realistic cost (see metrics; dominance/PF/sample the binding constraints).