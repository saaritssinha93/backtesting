# L_PRESSURE_BURST_VWAP — FIT/VAL→TRAIN/TEST code-loop report

**Verdict: INSUFFICIENT_SAMPLE**  |  faithfulness: readmit=LIVE-FAITHFUL  |  optimizer: Optuna TPE

- FIT 2026-05-29..2026-06-04 | VAL 2026-06-05..2026-06-11 | TRAIN 2026-05-29..2026-06-11 | TEST 2026-06-12..2026-06-24
- trials: 300  | objective on FIT/VAL only = min(PF) − 0.5·|gap|; TEST scored once

## Best config (only knobs the engine honors)
```
exit: SL 0.9 / Tgt 2.0
mask_terms: wick_skew_pct>=0.080548
pre_momentum_terms: pre1_adx<=36.590976; pre3_close_pos<=0.39637
entry_guards: {'max_slot': '12:30', 'top_n': 1}  (NB: conf-mask honors only min_slot; top_n/max_slot NOT enforced live)
max_positions: 10  daily_loss_rs: 4000.0
```

## Metrics (best config)

| window | 15 bps/leg | 5 bps/leg |
|---|---|---|
| TRAIN | n=22 PF=0.939 net=Rs-613 tradeDomGross=0.188 dayDom=9.99 symDom=9.99 tpd=2.44 dbp=0.5831 | n=22 PF=1.179 net=Rs1,583 tradeDomGross=0.18 dayDom=2.2 symDom=1.18 tpd=2.44 dbp=0.3687 |
| TEST  | n=4 PF=0.0 net=Rs-3,092 tradeDomGross=9.99 dayDom=9.99 symDom=9.99 tpd=1.33 dbp=1.0 | n=4 PF=0.022 net=Rs-2,722 tradeDomGross=1.0 dayDom=9.99 symDom=9.99 tpd=1.33 dbp=0.9673 |

Selection gate (TRAIN & TEST @15bps): **FAIL** (PF≥1.3, train_n≥15, test_n≥5, gross-profit/day/symbol dominance≤0.4, trades/day≤6.0). Binding constraints = **test sample (n=4<5)** AND **PF/net** (TRAIN PF 0.939<1.30 & net −Rs613; TEST PF 0.0). At 5 bps the TRAIN is marginally positive (PF 1.179) but it is a loser at the realistic 15 bps/leg, and TEST collapses at both cost levels.

## Baseline (original card config — same windows, 15 bps/leg)
Card config replayed via `setup_loop_runner.py`: SL 0.70 / Tgt 1.25, mask `quality_score<=25.0`, pre-mom `pre1_adx>=44.0`, no guard.

| window | n | PF | net Rs | win% | t/s/e% |
|---|---:|---:|---:|---:|---|
| TRAIN | 35 | 0.41 | −10,561 | 31.4 | 17/51/31 |
| TEST  | 22 | 0.44 | −5,725 | 31.8 | 14/41/45 |

The baseline is sample-*sufficient* (22 test trades) but a clear loser on both windows — confirming there is no edge here in the recent regime, independent of the small-sample best config.

## Knobs changed vs original card (best config)
- **exit**: SL 0.70 / Tgt 1.25 → **SL 0.90 / Tgt 2.00**
- **mask_terms**: `[quality_score<=25.0]` → `[wick_skew_pct>=0.080548]` (dropped the low-quality selector)
- **pre_momentum_terms**: `[pre1_adx>=44.0]` → `[pre1_adx<=36.590976, pre3_close_pos<=0.39637]` — note the **`pre1_adx` gate is INVERTED** (card wanted high pre-entry ADX ≥44; the optimizer wanted low ADX ≤36.6). This directly corroborates the card's "non-monotonic `pre1_adx`" warning: the threshold has no stable sign, a classic no-edge signature.
- **entry_guards**: `{}` → `{max_slot: 12:30, top_n: 1}` (NB: live conf-mask honors only `min_slot`; `top_n`/`max_slot` are NOT enforced live, so even this config is not live-reproducible)
- **max_positions**: 20 → 10  ·  **daily_loss_rs**: 0 → 4000

## Clean TEST verdict
TEST (2026-06-12..2026-06-24) was scored **exactly once** on the FIT/VAL-chosen config — no tuning on TEST (anti-overfit rule respected). The best config leaves only **n=4 test trades (< MIN_TRADES_TEST=5)**, so the honest label is **INSUFFICIENT_SAMPLE**, reinforced by a hard TEST loss (PF 0.0). The sample-sufficient baseline (n=22) is also a clear loser (PF 0.44), so this is **NO-EDGE**, not a tuning artifact.

## Live-crosscheck
readmit basis → loop is live-faithful. Card §2 caveat: L_PRESSURE_BURST_VWAP is **WEAK / USER_APPROVED_OVERRIDE_WEAK**, RAW-POOL, and explicitly fails monotonic-sensitivity + multi-exit checks with a **non-monotonic `pre1_adx`** — all reproduced here. Card §6.3: parked on 2026-06-29; this result corroborates the parking (re-promotion trigger test PF≥1.30 / test n≥20 is *not* met).

## Command
```
py -3.12 Train_and_Test\optuna_fitval_loop.py --setup L_PRESSURE_BURST_VWAP --pool C:/TradingData/eqidv2/setup_pools_2026_06_29/L_PRESSURE_BURST_VWAP --trials 300 --time_budget_min 20.0 --seed 7 --out Train_and_Test/results
```

## Files changed
- none to production. Artifacts under this folder. No final_setup_conf.py edit; no live trades.

## Why INSUFFICIENT_SAMPLE / NOT SELECTED
- Best of 300 TPE trials reaches only FIT/VAL score 0.9064 — no config cleared PF≥1.0 on both FIT and VAL; the chosen config is a marginal TRAIN config (PF 0.939 @15bps) that needs a tight `top_n=1`/`max_slot` guard, leaving just **4 TEST trades** → INSUFFICIENT_SAMPLE.
- The best config's edge depends on an **inverted, non-monotonic `pre1_adx` gate** and on `top_n`/`max_slot` guards that are **not enforced live** — so it is neither robust nor live-reproducible.
- Card baseline is sample-sufficient (n=22 test) and a clear loser (PF 0.44), confirming no edge in the recent regime.
- Verdict: **NOT SELECTED** (INSUFFICIENT_SAMPLE + no edge), not OVERFIT. Consistent with the card's 2026-06-29 parking; keep `enabled=False`. No `final_setup_conf.py` edit, no promotion, no live trades.