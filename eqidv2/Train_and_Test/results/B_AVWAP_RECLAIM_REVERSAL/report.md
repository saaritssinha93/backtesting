# B_AVWAP_RECLAIM_REVERSAL — FIT/VAL→TRAIN/TEST code-loop report

**Verdict: NOT SELECTED**  |  faithfulness: native=SCREENING-ONLY (firehose; use v11 conf backtest for live-faithful)  |  optimizer: Optuna TPE

- FIT 2026-05-29..2026-06-04 | VAL 2026-06-05..2026-06-11 | TRAIN 2026-05-29..2026-06-11 | TEST 2026-06-12..2026-06-24
- trials: 300  | objective on FIT/VAL only = min(PF) − 0.5·|gap|; TEST scored once

## Best config (only knobs the engine honors)
```
exit: SL 1.1 / Tgt 1.0
mask_terms: quality_score>=89.678598
pre_momentum_terms: pre3_close_pos>=0.785696; sig5_vol_ratio20<=4.421731
entry_guards: {'min_slot': '10:30', 'max_slot': '14:00'}  (NB: conf-mask honors only min_slot; top_n/max_slot NOT enforced live)
max_positions: 20  daily_loss_rs: 4000.0
```

## Baseline (original card config: mask vwap_dist_atr<=1.0, exit 0.70/1.50, no premom/guard) @15bps
- TRAIN (05-29..06-11): n=288, PF **0.28**, net −Rs134,219 (62.8% SL)
- TEST  (06-12..06-24): n=147, PF **0.46**, net −Rs42,054
- (firehose basis — heavy loser; the 300-trial search lifts TRAIN 0.28→0.56 but cannot reach profitability)

## Metrics (best config)

| window | 15 bps/leg | 5 bps/leg |
|---|---|---|
| TRAIN | n=81 PF=0.564 net=Rs-20,513 tradeDomGross=0.029 dayDom=9.99 symDom=9.99 tpd=9.0 dbp=0.9981 | n=81 PF=0.71 net=Rs-12,440 tradeDomGross=0.028 dayDom=9.99 symDom=9.99 tpd=9.0 dbp=0.9531 |
| TEST  | n=25 PF=0.722 net=Rs-3,254 tradeDomGross=0.091 dayDom=9.99 symDom=9.99 tpd=5.0 dbp=0.7742 | n=25 PF=0.928 net=Rs-758 tradeDomGross=0.088 dayDom=9.99 symDom=9.99 tpd=5.0 dbp=0.6081 |

Selection gate (TRAIN & TEST @15bps): **FAIL** (PF≥1.3, train_n≥15, test_n≥5, gross-profit/day/symbol dominance≤0.4, trades/day≤6.0).

## Live-crosscheck
native setup → SCREENING-ONLY firehose; the v11 conf backtest is the live-faithful arbiter. Doc §5 note for B_AVWAP: the v11 live overlay uses the INVERTED vwap_dist_atr≥0.60 mask (conf uses ≤1.0) — any winner must not rely on that inverted gate.

## Command
```
py -3.12 Train_and_Test\optuna_fitval_loop.py --setup B_AVWAP_RECLAIM_REVERSAL --pool C:/Users/Saarit/AppData/Local/Temp/claude/c--Users-Saarit-OneDrive-Desktop-Trading-backtesting-eqidv2-backtesting-eqidv2/41d4e196-2e06-4276-945a-008c377c414d/scratchpad/setup_pools/B_AVWAP_RECLAIM_REVERSAL --trials 300 --time_budget_min 20.0 --seed 7 --out Train_and_Test/results
```

## Files changed
- none to production. Artifacts under this folder. No final_setup_conf.py edit; no live trades.

## Why NOT SELECTED
- best FIT/VAL config does not clear the gate on TRAIN+TEST at realistic cost (see metrics; dominance/PF/sample the binding constraints).