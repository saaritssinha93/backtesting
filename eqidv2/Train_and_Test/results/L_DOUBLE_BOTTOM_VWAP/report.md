# L_DOUBLE_BOTTOM_VWAP — FIT/VAL→TRAIN/TEST code-loop report

**Verdict: NOT SELECTED**  |  faithfulness: readmit=LIVE-FAITHFUL  |  optimizer: Optuna TPE

- FIT 2026-05-29..2026-06-04 | VAL 2026-06-05..2026-06-11 | TRAIN 2026-05-29..2026-06-11 | TEST 2026-06-12..2026-06-24
- trials: 300  | objective on FIT/VAL only = min(PF) − 0.5·|gap|; TEST scored once

## Best config (only knobs the engine honors)
```
exit: SL 0.9 / Tgt 1.5
mask_terms: wick_skew_pct<=-0.010434; upper_wick_pct>=0.033047
pre_momentum_terms: pre_entry_momentum_score>=67.45277; pre_entry_momentum_score>=63.211847
entry_guards: {'min_slot': '09:45'}  (NB: conf-mask honors only min_slot; top_n/max_slot NOT enforced live)
max_positions: 10  daily_loss_rs: 0.0
```

## Metrics (best config)

| window | 15 bps/leg | 5 bps/leg |
|---|---|---|
| TRAIN | n=21 PF=0.56 net=Rs-5,665 tradeDomGross=0.176 dayDom=9.99 symDom=9.99 tpd=2.33 dbp=0.9383 | n=21 PF=0.692 net=Rs-3,570 tradeDomGross=0.17 dayDom=9.99 symDom=9.99 tpd=2.33 dbp=0.8329 |
| TEST  | n=8 PF=0.412 net=Rs-3,609 tradeDomGross=0.5 dayDom=9.99 symDom=9.99 tpd=2.67 dbp=0.8465 | n=8 PF=0.492 net=Rs-2,821 tradeDomGross=0.5 dayDom=9.99 symDom=9.99 tpd=2.67 dbp=0.736 |

Selection gate (TRAIN & TEST @15bps): **FAIL** (PF≥1.3, train_n≥15, test_n≥5, gross-profit/day/symbol dominance≤0.4, trades/day≤6.0). Binding constraint = **PF & net** (every PF is sub-1.0 and net is negative on both windows; dayDom/symDom show the 9.99 sentinel because total net is negative, so dominance is moot).

## Baseline (original card config — same windows, 15 bps/leg)
Card config replayed via `setup_loop_runner.py`: SL 0.90 / Tgt 1.50, mask **none**, pre-mom `pre_entry_momentum_score>=79.0 AND sig5_adx_calc>=28.0`, no guard.

| window | n | PF | net Rs | win% | t/s/e% |
|---|---:|---:|---:|---:|---|
| TRAIN | 15 | 0.20 | −10,823 | 20.0 | 13/80/7 |
| TEST  | 13 | 0.28 | −7,331 | 30.8 | 8/69/23 |

The baseline is *worse* than the tuned best config — ~80% of train trades stop out. The card's "STRONG PROBATION" evidence was an older window (Nov–Apr train / May–Jun test on the RAW pre-gate pool); on the most recent 15 sessions the edge is absent under realistic cost.

## Knobs changed vs original card (best config)
- **exit**: SL 0.90 / Tgt 1.50 → **unchanged** (0.9 / 1.5)
- **mask_terms**: `[]` → `[wick_skew_pct<=-0.010434, upper_wick_pct>=0.033047]`
- **pre_momentum_terms**: `[pre_entry_momentum_score>=79.0, sig5_adx_calc>=28.0]` → `[pre_entry_momentum_score>=67.45277, pre_entry_momentum_score>=63.211847]` (dropped the `sig5_adx_calc` gate, loosened the momentum floor 79→67.45; 2nd term is redundant)
- **entry_guards**: `{}` → `{min_slot: 09:45}`
- **max_positions**: 20 → 10  ·  **daily_loss_rs**: 0 → 0 (unchanged)

## Clean TEST verdict
TEST (2026-06-12..2026-06-24) was scored **exactly once** on the FIT/VAL-chosen config — no tuning on TEST (anti-overfit rule respected). TEST is a clean loss: n=8, PF=0.41, net=−Rs3,609 over 3 days. This is **NO-EDGE in the current regime**, not OVERFIT — OVERFIT would require a strong TRAIN that collapses on TEST, but TRAIN is itself a loser (PF 0.56). Not a lucky-day artifact (loss is broad, not concentrated).

## Live-crosscheck
readmit basis → loop is live-faithful. Card §2 caveat: L_DOUBLE_BOTTOM_VWAP was evaluated historically on the **RAW pre-gate pool** and the live research-layer currently **blocks the L\* family**; this run uses the same readmit (live-faithful) basis. Card §6.3: the setup was **parked on 2026-06-29** — this result corroborates the parking (re-promotion trigger of test PF≥1.30 / test n≥20 is *not* met).

## Command
```
py -3.12 Train_and_Test\optuna_fitval_loop.py --setup L_DOUBLE_BOTTOM_VWAP --pool C:/TradingData/eqidv2/setup_pools_2026_06_29/L_DOUBLE_BOTTOM_VWAP --trials 300 --time_budget_min 20.0 --seed 7 --out Train_and_Test/results
```

## Files changed
- none to production. Artifacts under this folder. No final_setup_conf.py edit; no live trades.

## Why NOT SELECTED
- The **best of 300 TPE trials still loses in-sample**: best FIT/VAL score = 0.5554, i.e. no config reached PF≥1.0 on *both* FIT and VAL. There is no profitable in-sample config to carry out of sample.
- Best config: TRAIN n=21/PF=0.56/−Rs5,665, TEST n=8/PF=0.41/−Rs3,609 — fails PF≥1.30 and net>0 on both windows.
- Baseline card config is worse still (TRAIN PF 0.20, TEST PF 0.28).
- Verdict is **NOT SELECTED** (no edge), not OVERFIT — TRAIN never cleared the band. Consistent with the card's 2026-06-29 parking of L_DOUBLE_BOTTOM_VWAP; keep `enabled=False`. No `final_setup_conf.py` edit, no promotion, no live trades.